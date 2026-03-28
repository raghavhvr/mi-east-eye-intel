#!/usr/bin/env node
// scripts/fetch-feeds.js
// Fetches RSS + Reddit (Pullpush) + HN Algolia → public/data/feeds.json
// Run by GitHub Actions cron every 30 min. No Vercel needed.

import { writeFileSync, mkdirSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT = resolve(__dirname, "../public/data/feeds.json");
mkdirSync(dirname(OUT), { recursive: true });

// ── Constants ────────────────────────────────────────────────────────────────
const RSS_SOURCES = [
  { id: "bbc-me",    label: "BBC Middle East",  url: "https://feeds.bbci.co.uk/news/world/middle_east/rss.xml" },
  { id: "aljazeera", label: "Al Jazeera",        url: "https://www.aljazeera.com/xml/rss/all.xml" },
  { id: "arabnews",  label: "Arab News",          url: "https://www.arabnews.com/rss.xml" },
  { id: "mee",       label: "Middle East Eye",    url: "https://www.middleeasteye.net/rss" },
  { id: "national",  label: "The National UAE",   url: "https://www.thenationalnews.com/arc/outboundfeeds/rss/?outputType=xml" },
  { id: "aa",        label: "Anadolu Agency",     url: "https://www.aa.com.tr/en/rss/default?cat=world" },
  { id: "guardian",  label: "The Guardian",       url: "https://www.theguardian.com/world/rss" },
];

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

const SECTIONS = {
  "🚨 Crisis":  ["war","conflict","ceasefire","attack","bomb","fire","flood","storm","houthi","missile","casualties","killed","explosion","earthquake","pandemic","airstrike","siege","displaced"],
  "💼 Economy": ["oil","opec","economy","gdp","inflation","market","investment","trade","startup","fund","aramco","adnoc","property","rent","salary","job","tourism","vision 2030","neom","stock","revenue","ipo"],
  "🏛️ Politics":["government","minister","policy","election","parliament","diplomacy","sanction","treaty","reform","law","decree","summit","president","prime minister","royal","cabinet","nuclear","coup"],
  "🌐 Expat":   ["visa","expat","cost of living","iqama","golden visa","traffic","metro","food","restaurant","transport","immigration","residency","permit","school","healthcare","grocery"],
  "🕌 Culture": ["ramadan","eid","mosque","religion","entertainment","festival","education","university","women","sports","arts","culture","social","marriage","family","heritage","cinema","music"],
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

// ── Helpers ──────────────────────────────────────────────────────────────────
function senti(text) {
  if (!text) return { label: "NEUTRAL", score: 0 };
  const w = text.toLowerCase().split(/\W+/); let p = 0, n = 0;
  w.forEach((x,i) => { const neg = NEGS.includes(w[i-1]||""); if(POS_W.includes(x)) neg?n++:p++; if(NEG_W.includes(x)) neg?p++:n++; });
  if (n>p+1) return { label:"CRITICAL", score:-2 }; if (n>p) return { label:"WARNING", score:-1 };
  if (p>n+1) return { label:"POSITIVE", score:2 };  if (p>n) return { label:"STABLE", score:1 };
  return { label:"NEUTRAL", score:0 };
}

function classify(text) {
  const t = (text||"").toLowerCase(); let best = "Other", bestN = 0;
  for (const [k, kws] of Object.entries(SECTIONS)) { const n = kws.filter(kw=>t.includes(kw)).length; if(n>bestN){best=k;bestN=n;} }
  return best;
}

function detectCountry(text) {
  const t = (text||"").toLowerCase();
  for (const [country, kws] of COUNTRY_KW) { if (kws.some(k=>t.includes(k))) return country; }
  return "Regional";
}

function clean(str) {
  return (str||"").replace(/&amp;/g,"&").replace(/&lt;/g,"<").replace(/&gt;/g,">").replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/&nbsp;/g," ").replace(/&#(\d+);/g,(_,n)=>String.fromCharCode(Number(n))).trim();
}

function stripTags(html) { return (html||"").replace(/<[^>]+>/g," ").replace(/\s+/g," ").trim(); }

function dedup(arr) { const s = new Set(); return arr.filter(i => { if(!i.id||s.has(i.id))return false; s.add(i.id); return true; }); }

// ── RSS Parser ───────────────────────────────────────────────────────────────
function parseRSS(xml) {
  const items = [];
  const isAtom = /<feed[\s>]/i.test(xml) && /xmlns.*atom/i.test(xml);
  const extractTag = (xml, tag) => { const re = new RegExp(`<${tag}(?:[^>]*)>\\s*(?:<!\\[CDATA\\[([\\s\\S]*?)\\]\\]>|([\\s\\S]*?))\\s*<\\/${tag}>`, "i"); const m = xml.match(re); return m ? (m[1]??m[2]??"").trim() : ""; };
  const extractAttr = (xml, tag, attr) => { const re = new RegExp(`<${tag}[^>]*${attr}="([^"]*)"`, "i"); const m = xml.match(re); return m ? m[1] : ""; };

  if (isAtom) {
    const entries = xml.match(/<entry[\s\S]*?<\/entry>/g) || [];
    for (const e of entries.slice(0, 10)) {
      const title = extractTag(e, "title"); if (!title) continue;
      items.push({ title: clean(title), summary: clean(stripTags(extractTag(e,"summary")||extractTag(e,"content")||"")).slice(0,240), link: extractAttr(e,"link","href")||extractTag(e,"link")||"", pubDate: extractTag(e,"updated")||extractTag(e,"published")||"", guid: extractTag(e,"id")||"" });
    }
  } else {
    const channelEnd = xml.indexOf("<item"); const itemsXml = channelEnd>=0 ? xml.slice(channelEnd) : xml;
    const entries = itemsXml.match(/<item[\s\S]*?<\/item>/g) || [];
    for (const e of entries.slice(0, 10)) {
      const title = extractTag(e, "title"); if (!title) continue;
      items.push({ title: clean(title), summary: clean(stripTags(extractTag(e,"description")||extractTag(e,"content:encoded")||"")).slice(0,240), link: extractTag(e,"link")||"", pubDate: extractTag(e,"pubDate")||extractTag(e,"dc:date")||"", guid: extractTag(e,"guid")||extractTag(e,"link")||"" });
    }
  }
  return items;
}

// ── Fetchers ─────────────────────────────────────────────────────────────────
async function fetchRSS(src) {
  try {
    const res = await fetch(src.url, { headers: { "User-Agent": "Mozilla/5.0 (compatible; OpenEye-OSINT/1.0)", "Accept": "application/rss+xml, application/xml, text/xml, */*" }, signal: AbortSignal.timeout(14000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const xml = await res.text();
    const raw = parseRSS(xml);
    return raw.map(item => {
      const txt = item.title + " " + item.summary;
      return { id: item.guid||item.link, title: item.title, summary: item.summary, url: item.link, timestamp: new Date(item.pubDate||Date.now()).toISOString(), source: src.label, sourceType: "RSS", tag: "NEWS", country: detectCountry(txt), section: classify(txt), sentiment: senti(txt), score: 0, comments: 0 };
    });
  } catch (err) {
    console.error(`[rss:${src.id}] failed:`, err.message);
    return [];
  }
}

async function fetchReddit(entry) {
  try {
    // after = 7 days ago as Unix timestamp — Pullpush requires this to avoid returning old all-time top posts
    const after = Math.floor(Date.now()/1000) - 7*24*3600;
    const url = `https://api.pullpush.io/reddit/search/submission/?subreddit=${entry.sub}&size=25&sort=desc&sort_type=created_utc&after=${after}`;
    const res = await fetch(url, { headers: { "User-Agent": "OpenEye/1.0" }, signal: AbortSignal.timeout(12000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const d = await res.json();
    return (d.data||[]).filter(p=>p.title&&!p.removed_by_category).slice(0,15).map(p => {
      const txt = p.title + " " + (p.selftext||"");
      return { id: `reddit-${p.id}`, title: p.title, summary: (p.selftext||"").replace(/\n+/g," ").trim().slice(0,220)||`↑${p.score||0} · 💬${p.num_comments||0}`, url: `https://reddit.com${p.permalink||""}`, timestamp: new Date((p.created_utc||0)*1000).toISOString(), source: `r/${entry.sub}`, sourceType: "Reddit", tag: "SOCIAL", country: entry.country==="Regional" ? detectCountry(txt) : entry.country, section: classify(txt), sentiment: senti(txt), score: p.score||0, comments: p.num_comments||0 };
    });
  } catch (err) {
    console.error(`[reddit:${entry.sub}] failed:`, err.message);
    return [];
  }
}

async function fetchHN() {
  const since = Math.floor(Date.now()/1000) - 7*24*3600;
  const queries = ["middle east","OPEC oil","UAE technology","Saudi Arabia","Gulf geopolitics","Gaza ceasefire","Iran nuclear","Egypt economy","Iraq security","MENA finance","Dubai startup","Qatar investment"];
  const all = []; const seen = new Set();
  await Promise.allSettled(queries.map(async q => {
    try {
      const d = await fetch(`https://hn.algolia.com/api/v1/search?query=${encodeURIComponent(q)}&tags=story&hitsPerPage=5&numericFilters=created_at_i>${since}`).then(r=>r.json());
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
  console.log("[fetch-feeds] Starting…");
  const all = [];

  // RSS — parallel
  const rssResults = await Promise.allSettled(RSS_SOURCES.map(fetchRSS));
  for (const r of rssResults) if (r.status==="fulfilled") all.push(...r.value);
  console.log(`[fetch-feeds] RSS: ${all.length} articles`);

  // Reddit — parallel
  const redditResults = await Promise.allSettled(ME_SUBREDDITS.map(fetchReddit));
  for (const r of redditResults) if (r.status==="fulfilled") all.push(...r.value);
  console.log(`[fetch-feeds] +Reddit: ${all.length} total`);

  // HN
  const hn = await fetchHN();
  all.push(...hn);
  console.log(`[fetch-feeds] +HN: ${all.length} total`);

  const cutoff = Date.now() - 7*24*3600*1000; // 7 days ago
  const sorted = dedup(
    all
      .filter(a => new Date(a.timestamp).getTime() > cutoff)
      .sort((a,b) => new Date(b.timestamp) - new Date(a.timestamp))
  );
  const output = { generated_at: new Date().toISOString(), count: sorted.length, articles: sorted };

  writeFileSync(OUT, JSON.stringify(output, null, 2));
  console.log(`[fetch-feeds] Done → ${sorted.length} articles → ${OUT}`);
}

main().catch(err => { console.error("[fetch-feeds] Fatal:", err); process.exit(0); });
