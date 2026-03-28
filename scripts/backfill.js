#!/usr/bin/env node
// scripts/backfill.js
// One-time seed: fetches all data from START_DATE to today in weekly chunks
// and writes public/data/feeds.json as the initial dataset.
//
// Usage:
//   node scripts/backfill.js
//   node scripts/backfill.js --start 2026-01-01
//   node scripts/backfill.js --start 2026-01-01 --end 2026-03-01
//
// After backfill, the normal fetch-feeds cron will append from there.

import { writeFileSync, readFileSync, mkdirSync, existsSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT = resolve(__dirname, "../public/data/feeds.json");
mkdirSync(dirname(OUT), { recursive: true });

// ── Config ───────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const startArg = args[args.indexOf("--start") + 1];
const endArg   = args[args.indexOf("--end")   + 1];

const START_DATE = new Date(startArg || "2026-01-01");
const END_DATE   = new Date(endArg   || new Date().toISOString().split("T")[0]);
const CHUNK_DAYS = 7;   // fetch in weekly chunks to avoid timeouts
const SLEEP_MS   = 600; // pause between chunks to be a polite requester

// ── Shared helpers (duplicated from fetch-feeds.js for standalone use) ────────
const SECTIONS = {
  "🚨 Crisis":  ["war","conflict","ceasefire","attack","bomb","fire","flood","storm","houthi","missile","casualties","killed","explosion","earthquake","pandemic","airstrike","siege","displaced"],
  "💼 Economy": ["oil","opec","economy","gdp","inflation","market","investment","trade","startup","fund","aramco","adnoc","property","rent","salary","job","tourism","vision 2030","neom","stock","revenue","ipo"],
  "🏛️ Politics":["government","minister","policy","election","parliament","diplomacy","sanction","treaty","reform","law","decree","summit","president","prime minister","royal","cabinet","nuclear","coup"],
  "🌐 Expat":   ["visa","expat","cost of living","iqama","golden visa","traffic","metro","food","restaurant","transport","immigration","residency","permit","school","healthcare","grocery"],
  "🕌 Culture": ["ramadan","eid","mosque","religion","entertainment","festival","education","university","women","sports","arts","culture","heritage","cinema","music"],
  "💻 Tech":    ["ai","artificial intelligence","startup","tech","innovation","crypto","blockchain","smart city","5g","solar","renewable","fintech","digital","cybersecurity","g42","data center"],
};
const COUNTRY_KW = [
  ["UAE",          ["uae","dubai","abu dhabi","emirati","sharjah","ajman"]],
  ["Saudi Arabia", ["saudi","riyadh","jeddah","aramco","ksa","mecca","neom"]],
  ["Qatar",        ["qatar","doha","qatari"]],
  ["Kuwait",       ["kuwait","kuwaiti"]],
  ["Oman",         ["oman","muscat","omani"]],
  ["Bahrain",      ["bahrain","manama"]],
  ["Jordan",       ["jordan","amman","jordanian"]],
  ["Lebanon",      ["lebanon","beirut","lebanese"]],
  ["Syria",        ["syria","damascus","aleppo","syrian"]],
  ["Iraq",         ["iraq","baghdad","iraqi","basra","mosul"]],
  ["Palestine",    ["palestine","gaza","west bank","hamas","palestinian"]],
  ["Israel",       ["israel","tel aviv","jerusalem","idf"]],
  ["Egypt",        ["egypt","cairo","egyptian","suez","alexandria"]],
  ["Libya",        ["libya","tripoli","benghazi","libyan"]],
  ["Tunisia",      ["tunisia","tunis","tunisian"]],
  ["Algeria",      ["algeria","algiers","algerian"]],
  ["Morocco",      ["morocco","rabat","casablanca","moroccan"]],
  ["Sudan",        ["sudan","khartoum","sudanese"]],
  ["Yemen",        ["yemen","sanaa","houthi","yemeni","aden"]],
  ["Iran",         ["iran","tehran","iranian","irgc"]],
];
const POS_W = ["growth","surge","record","success","deal","agreement","expands","boost","profit","milestone","launch","stable","peace","recovery","invest","improve","achieve","develop","partnership","innovation","win","hope","progress","rise","benefit","support","signed","approved","relief"];
const NEG_W = ["crisis","attack","conflict","warning","risk","decline","concern","tension","threat","sanction","collapse","killed","explosion","flood","fire","war","bomb","strike","missile","casualties","arrest","ban","shortage","debt","failure","violence","terrorism","hostage","dead","wounded","detained","airstrike","siege"];
const NEGS  = ["not","no","never","don't","doesn't","didn't","won't","can't","isn't","aren't","wasn't","without"];

const senti = text => { if(!text)return{label:"NEUTRAL",score:0}; const w=text.toLowerCase().split(/\W+/);let p=0,n=0;w.forEach((x,i)=>{const neg=NEGS.includes(w[i-1]||"");if(POS_W.includes(x))neg?n++:p++;if(NEG_W.includes(x))neg?p++:n++;});if(n>p+1)return{label:"CRITICAL",score:-2};if(n>p)return{label:"WARNING",score:-1};if(p>n+1)return{label:"POSITIVE",score:2};if(p>n)return{label:"STABLE",score:1};return{label:"NEUTRAL",score:0}; };
const classify = text => { const t=(text||"").toLowerCase();let best="Other",bestN=0;for(const[k,kws]of Object.entries(SECTIONS)){const n=kws.filter(kw=>t.includes(kw)).length;if(n>bestN){best=k;bestN=n;}}return best; };
const detectCountry = text => { const t=(text||"").toLowerCase();for(const[country,kws]of COUNTRY_KW){if(kws.some(k=>t.includes(k)))return country;}return"Regional"; };
const dedup = arr => { const s=new Set();return arr.filter(i=>{if(!i.id||s.has(i.id))return false;s.add(i.id);return true;}); };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const toDate = d => d.toISOString().split("T")[0];
const addDays = (d, n) => new Date(d.getTime() + n*24*3600*1000);

// ── Reddit backfill via Arctic Shift ─────────────────────────────────────────
const ME_SUBREDDITS = [
  { sub: "dubai",       country: "UAE" },
  { sub: "UAE",         country: "UAE" },
  { sub: "saudiarabia", country: "Saudi Arabia" },
  { sub: "qatar",       country: "Qatar" },
  { sub: "kuwait",      country: "Kuwait" },
  { sub: "oman",        country: "Oman" },
  { sub: "bahrain",     country: "Bahrain" },
  { sub: "expats",      country: "Regional" },
  { sub: "middleeast",  country: "Regional" },
  { sub: "arabs",       country: "Regional" },
];

async function fetchRedditChunk(sub, afterDate, beforeDate) {
  try {
    const url = `https://arctic-shift.photon-reddit.com/api/posts/search` +
      `?subreddit=${sub}&after=${afterDate}&before=${beforeDate}&limit=100&sort=created_utc&order=asc`;
    const res = await fetch(url, { headers: { "User-Agent": "OpenEye-Backfill/1.0" }, signal: AbortSignal.timeout(20000) });
    if (!res.ok) { console.warn(`  [r/${sub}] HTTP ${res.status}`); return []; }
    const d = await res.json();
    return d.data || d.posts || d || [];
  } catch (err) { console.warn(`  [r/${sub}] ${err.message}`); return []; }
}

async function fetchHNChunk(afterUnix, beforeUnix) {
  const queries = ["middle east","OPEC oil","UAE technology","Saudi Arabia","Gulf geopolitics","Gaza ceasefire","Iran nuclear","Egypt economy","Iraq security","MENA finance","Dubai startup","Qatar investment"];
  const all = []; const seen = new Set();
  await Promise.allSettled(queries.map(async q => {
    try {
      const d = await fetch(`https://hn.algolia.com/api/v1/search_by_date?query=${encodeURIComponent(q)}&tags=story&hitsPerPage=20&numericFilters=created_at_i>${afterUnix},created_at_i<${beforeUnix}`).then(r=>r.json());
      for (const h of (d.hits||[])) {
        if (!h.title||seen.has(h.objectID)) continue; seen.add(h.objectID);
        all.push({ id:`hn-${h.objectID}`, title:h.title, summary:`${h.points||0} pts · ${h.num_comments||0} comments`, url:h.url||`https://news.ycombinator.com/item?id=${h.objectID}`, timestamp:new Date(h.created_at).toISOString(), source:"Hacker News", sourceType:"HN", tag:"TECH", country:detectCountry(h.title), section:classify(h.title), sentiment:senti(h.title), score:h.points||0, comments:h.num_comments||0 });
      }
    } catch {}
  }));
  return all;
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`\n📦 Open Eye Backfill`);
  console.log(`   Range: ${toDate(START_DATE)} → ${toDate(END_DATE)}`);
  console.log(`   Chunk: ${CHUNK_DAYS} days\n`);

  // Load any existing data so we don't lose it
  let existing = [];
  if (existsSync(OUT)) {
    try {
      const saved = JSON.parse(readFileSync(OUT, "utf8"));
      existing = saved.articles || [];
      console.log(`Loaded ${existing.length} existing articles from feeds.json`);
    } catch {}
  }

  const all = [...existing];
  const existingIds = new Set(existing.map(a => a.id));

  // Iterate in weekly chunks from START to END
  let chunkStart = new Date(START_DATE);
  let chunkNum = 0;
  const totalChunks = Math.ceil((END_DATE - START_DATE) / (CHUNK_DAYS * 24 * 3600 * 1000));

  while (chunkStart < END_DATE) {
    const chunkEnd = new Date(Math.min(addDays(chunkStart, CHUNK_DAYS).getTime(), END_DATE.getTime()));
    const afterStr  = toDate(chunkStart);
    const beforeStr = toDate(chunkEnd);
    const afterUnix  = Math.floor(chunkStart.getTime() / 1000);
    const beforeUnix = Math.floor(chunkEnd.getTime()   / 1000);
    chunkNum++;

    console.log(`[${chunkNum}/${totalChunks}] ${afterStr} → ${beforeStr}`);

    // Reddit — all subs in parallel for this chunk
    const redditBatch = await Promise.all(
      ME_SUBREDDITS.map(entry => fetchRedditChunk(entry.sub, afterStr, beforeStr).then(posts =>
        posts.filter(p => p.title && !p.removed_by_category).map(p => {
          const txt = p.title + " " + (p.selftext || "");
          return { id:`reddit-${p.id}`, title:p.title, summary:(p.selftext||"").replace(/\n+/g," ").trim().slice(0,220)||`↑${p.score||0}`, url:`https://reddit.com${p.permalink||""}`, timestamp:new Date((p.created_utc||0)*1000).toISOString(), source:`r/${entry.sub}`, sourceType:"Reddit", tag:"SOCIAL", country:entry.country==="Regional"?detectCountry(txt):entry.country, section:classify(txt), sentiment:senti(txt), score:p.score||0, comments:p.num_comments||0 };
        })
      ))
    );
    const redditItems = redditBatch.flat().filter(a => !existingIds.has(a.id));

    // HN — for this chunk
    const hnItems = (await fetchHNChunk(afterUnix, beforeUnix)).filter(a => !existingIds.has(a.id));

    const chunkTotal = redditItems.length + hnItems.length;
    console.log(`  → Reddit: ${redditItems.length}  HN: ${hnItems.length}  (${chunkTotal} new)`);

    all.push(...redditItems, ...hnItems);
    redditItems.forEach(a => existingIds.add(a.id));
    hnItems.forEach(a => existingIds.add(a.id));

    // Save incrementally every 4 chunks so progress isn't lost on error
    if (chunkNum % 4 === 0 || chunkEnd >= END_DATE) {
      const sorted = dedup(all).sort((a,b) => new Date(b.timestamp) - new Date(a.timestamp));
      writeFileSync(OUT, JSON.stringify({ generated_at: new Date().toISOString(), count: sorted.length, articles: sorted }, null, 2));
      console.log(`  💾 Saved ${sorted.length} total articles`);
    }

    chunkStart = chunkEnd;
    await sleep(SLEEP_MS);
  }

  // Final save
  const final = dedup(all).sort((a,b) => new Date(b.timestamp) - new Date(a.timestamp));
  writeFileSync(OUT, JSON.stringify({ generated_at: new Date().toISOString(), count: final.length, articles: final }, null, 2));

  console.log(`\n✅ Backfill complete!`);
  console.log(`   Total articles: ${final.length}`);
  console.log(`   Date range: ${final[final.length-1]?.timestamp?.split("T")[0]} → ${final[0]?.timestamp?.split("T")[0]}`);
  console.log(`\nNow push to GitHub — Vercel will deploy, and the cron will append from here.`);
}

main().catch(err => { console.error("Backfill failed:", err); process.exit(1); });
