#!/usr/bin/env node
// scripts/fetch-feeds.js
// Fetches RSS + Reddit (Arctic Shift) + HN → public/data/feeds.json
// APPEND mode: loads existing feeds.json, fetches last 48h of new data,
// merges, deduplicates, and keeps a rolling 90-day window.
// Run by GitHub Actions cron every 30 min.

import { writeFileSync, readFileSync, mkdirSync, existsSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT = resolve(__dirname, "../public/data/feeds.json");
mkdirSync(dirname(OUT), { recursive: true });

const ROLLING_WINDOW_DAYS = 90; // keep 90 days of history

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

// ── Source Fetchers ───────────────────────────────────────────────────────────
async function fetchRSS(src) {
  try {
    const res = await fetch(src.url, { headers: { "User-Agent": "Mozilla/5.0 (compatible; OpenEye-OSINT/1.0)", "Accept": "application/rss+xml, application/xml, text/xml, */*" }, signal: AbortSignal.timeout(14000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const xml = await res.text();
    return parseRSS(xml).map(item => {
      const txt = item.title + " " + item.summary;
      return { id: item.guid||item.link, title: item.title, summary: item.summary, url: item.link, timestamp: new Date(item.pubDate||Date.now()).toISOString(), source: src.label, sourceType: "RSS", tag: "NEWS", country: detectCountry(txt), section: classify(txt), sentiment: senti(txt), score: 0, comments: 0 };
    });
  } catch (err) { console.error(`[rss:${src.id}] failed:`, err.message); return []; }
}

export async function fetchRedditRange(sub, afterDate, beforeDate = null) {
  // afterDate / beforeDate: ISO date strings "YYYY-MM-DD"
  try {
    let url = `https://arctic-shift.photon-reddit.com/api/posts/search?subreddit=${sub}&after=${afterDate}&limit=100&sort=asc`;
    if (beforeDate) url += `&before=${beforeDate}`;
    const res = await fetch(url, { headers: { "User-Agent": "OpenEye/1.0" }, signal: AbortSignal.timeout(20000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const d = await res.json();
    return d.data || d.posts || d || [];
  } catch (err) { console.error(`[reddit:${sub}] range fetch failed:`, err.message); return []; }
}

async function fetchReddit(entry) {
  // Normal cron: last 48h (generous window to not miss anything between 30-min runs)
  const after = new Date(Date.now() - 48*3600*1000).toISOString().split("T")[0];
  try {
    const url = `https://arctic-shift.photon-reddit.com/api/posts/search?subreddit=${entry.sub}&after=${after}&limit=50&sort=desc`;
    const res = await fetch(url, { headers: { "User-Agent": "OpenEye/1.0" }, signal: AbortSignal.timeout(12000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const d = await res.json();
    const posts = d.data || d.posts || d || [];
    return posts.filter(p => p.title && !p.removed_by_category).map(p => {
      const txt = p.title + " " + (p.selftext || "");
      return { id: `reddit-${p.id}`, title: p.title, summary: (p.selftext||"").replace(/\n+/g," ").trim().slice(0,220)||`↑${p.score||0} · 💬${p.num_comments||0}`, url: `https://reddit.com${p.permalink||""}`, timestamp: new Date((p.created_utc||0)*1000).toISOString(), source: `r/${entry.sub}`, sourceType: "Reddit", tag: "SOCIAL", country: entry.country==="Regional" ? detectCountry(txt) : entry.country, section: classify(txt), sentiment: senti(txt), score: p.score||0, comments: p.num_comments||0 };
    });
  } catch (err) { console.error(`[reddit:${entry.sub}] failed:`, err.message); return []; }
}

async function fetchHN(afterUnix = null) {
  const since = afterUnix || Math.floor(Date.now()/1000) - 48*3600;
  const queries = ["middle east","OPEC oil","UAE technology","Saudi Arabia","Gulf geopolitics","Gaza ceasefire","Iran nuclear","Egypt economy","Iraq security","MENA finance","Dubai startup","Qatar investment"];
  const all = []; const seen = new Set();
  await Promise.allSettled(queries.map(async q => {
    try {
      const d = await fetch(`https://hn.algolia.com/api/v1/search?query=${encodeURIComponent(q)}&tags=story&hitsPerPage=10&numericFilters=created_at_i>${since}`).then(r=>r.json());
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
  console.log("[fetch-feeds] Starting (append mode)…");

  // Load existing data
  let existing = [];
  if (existsSync(OUT)) {
    try {
      const saved = JSON.parse(readFileSync(OUT, "utf8"));
      existing = saved.articles || [];
      console.log(`[fetch-feeds] Loaded ${existing.length} existing articles`);
    } catch { console.warn("[fetch-feeds] Could not parse existing feeds.json — starting fresh"); }
  }

  // Fetch fresh data (last 48h)
  const fresh = [];
  const rssResults = await Promise.allSettled(RSS_SOURCES.map(fetchRSS));
  for (const r of rssResults) if (r.status==="fulfilled") fresh.push(...r.value);

  const redditResults = await Promise.allSettled(ME_SUBREDDITS.map(fetchReddit));
  for (const r of redditResults) if (r.status==="fulfilled") fresh.push(...r.value);

  const hn = await fetchHN();
  fresh.push(...hn);
  console.log(`[fetch-feeds] Fetched ${fresh.length} fresh articles`);

  // Merge: new on top, then existing, deduplicate, enforce 90-day rolling window
  const cutoff = Date.now() - ROLLING_WINDOW_DAYS * 24 * 3600 * 1000;
  const merged = dedup([...fresh, ...existing])
    .filter(a => new Date(a.timestamp).getTime() > cutoff)
    .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

  const newCount = merged.length - existing.filter(a => new Date(a.timestamp).getTime() > cutoff).length;
  console.log(`[fetch-feeds] Merged: ${merged.length} total (${Math.max(0,newCount)} new, window: ${ROLLING_WINDOW_DAYS}d)`);

  writeFileSync(OUT, JSON.stringify({ generated_at: new Date().toISOString(), count: merged.length, articles: merged }, null, 2));
  console.log(`[fetch-feeds] Done → ${OUT}`);
}

main().catch(err => { console.error("[fetch-feeds] Fatal:", err); process.exit(0); });
