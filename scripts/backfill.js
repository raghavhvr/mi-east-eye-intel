#!/usr/bin/env node
// scripts/backfill.js — One-time 30-day historical data loader
//
// Usage (run from project root):
//   node scripts/backfill.js
//
// Requires env vars (add to .env or pass inline):
//   UPSTASH_REDIS_REST_URL=https://xxx.upstash.io
//   UPSTASH_REDIS_REST_TOKEN=AXxx...

const UPSTASH_URL   = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

if (!UPSTASH_URL || !UPSTASH_TOKEN) {
  console.error("Set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN first.");
  process.exit(1);
}

// ── Classify / sentiment (mirrors ingest.js) ────────────────────────────────
const SECTIONS = {
  "Crisis & Safety":      ["war","conflict","ceasefire","attack","bomb","fire","flood","houthi","missile","killed","explosion","earthquake"],
  "Economy & Business":   ["oil","opec","economy","gdp","inflation","market","investment","trade","aramco","adnoc","property","salary","tourism"],
  "Politics":             ["government","minister","policy","election","parliament","diplomacy","sanction","treaty","reform","summit","president","royal"],
  "Expat & Daily Life":   ["visa","expat","cost of living","golden visa","traffic","metro","food","transport","immigration","residency","permit","school"],
  "Tech & Innovation":    ["ai","artificial intelligence","startup","tech","blockchain","smart city","5g","solar","renewable","fintech","digital","cybersecurity"],
};
const POS = ["growth","surge","record","success","deal","agreement","boost","profit","launch","stable","peace","recovery","invest","milestone"];
const NEG = ["crisis","attack","conflict","warning","risk","decline","tension","threat","disruption","sanction","collapse","killed","explosion","war","bomb"];
const NEGS = ["not","no","never","don't","doesn't","didn't","won't","can't","isn't","aren't"];

function classifySection(text) {
  const t = (text||"").toLowerCase();
  let best = "Other", bestScore = 0;
  for (const [sec, kws] of Object.entries(SECTIONS)) {
    const score = kws.filter(k=>t.includes(k)).length;
    if (score > bestScore) { best = sec; bestScore = score; }
  }
  return best;
}
function detectCountry(text) {
  const t = (text||"").toLowerCase();
  if (["uae","dubai","abu dhabi","emirati","sharjah"].some(k=>t.includes(k))) return "UAE";
  if (["saudi","riyadh","jeddah","aramco","ksa"].some(k=>t.includes(k))) return "Saudi Arabia";
  if (["qatar","doha"].some(k=>t.includes(k))) return "Qatar";
  if (t.includes("kuwait")) return "Kuwait";
  if (["jordan","amman"].some(k=>t.includes(k))) return "Jordan";
  if (["oman","muscat"].some(k=>t.includes(k))) return "Oman";
  if (t.includes("bahrain")) return "Bahrain";
  if (["lebanon","beirut"].some(k=>t.includes(k))) return "Lebanon";
  if (t.includes("iran")) return "Iran";
  if (t.includes("israel")) return "Israel";
  if (t.includes("yemen")) return "Yemen";
  if (t.includes("egypt")) return "Egypt";
  return "Regional";
}
function analyseSentiment(text) {
  const words = (text||"").toLowerCase().split(/\W+/);
  let pos = 0, neg = 0;
  words.forEach((w,i)=>{
    const negated = NEGS.includes(words[i-1]||"");
    if (POS.includes(w)) negated ? neg++ : pos++;
    if (NEG.includes(w)) negated ? pos++ : neg++;
  });
  if (neg > pos+1) return { label:"CRITICAL", score:-2 };
  if (neg > pos)   return { label:"WARNING",  score:-1 };
  if (pos > neg+1) return { label:"POSITIVE", score: 2 };
  if (pos > neg)   return { label:"STABLE",   score: 1 };
  return             { label:"NEUTRAL",  score: 0 };
}
function normalise(raw) {
  const text = (raw.title||"")+" "+(raw.summary||"");
  return {
    id: raw.id, title: raw.title,
    summary: (raw.summary||"").slice(0,240),
    url: raw.url, timestamp: raw.timestamp,
    source: raw.source, sourceType: raw.sourceType, tag: raw.tag,
    country: raw.country || detectCountry(text),
    section: classifySection(text),
    sentiment: analyseSentiment(text),
    score: raw.score||0, comments: raw.comments||0,
  };
}

// ── RSS Parser ───────────────────────────────────────────────────────────────
function extractTag(xml, tag) {
  const re = new RegExp(`<${tag}[^>]*>(?:<!\\[CDATA\\[([\\s\\S]*?)\\]\\]>|([\\s\\S]*?))<\\/${tag}>`, "i");
  const m = xml.match(re);
  return m ? (m[1]??m[2]??"").trim() : "";
}
function clean(s) {
  return s.replace(/&amp;/g,"&").replace(/&lt;/g,"<").replace(/&gt;/g,">").replace(/&#39;/g,"'").replace(/&quot;/g,'"').replace(/&nbsp;/g," ").trim();
}
function parseRSS(xml, sourceId, sourceLabel) {
  const items = [];
  const channelEnd = xml.indexOf("<item");
  const body = channelEnd >= 0 ? xml.slice(channelEnd) : xml;
  const entries = body.match(/<item[\s\S]*?<\/item>/g) || [];
  for (const e of entries) {
    const title = clean(extractTag(e,"title"));
    const link  = extractTag(e,"link") || "";
    const date  = extractTag(e,"pubDate") || "";
    const desc  = clean(extractTag(e,"description").replace(/<[^>]+>/g," ").replace(/\s+/g," ").trim()).slice(0,240);
    if (!title) continue;
    const ts = date ? new Date(date).toISOString() : new Date().toISOString();
    const idBase = Buffer.from(link||title).toString("base64").slice(0,16);
    items.push({
      id: `rss-${sourceId}-${idBase}`,
      title, summary: desc, url: link, timestamp: ts,
      source: sourceLabel, sourceType: "RSS", tag: "NEWS",
    });
  }
  return items;
}

// ── Sources ──────────────────────────────────────────────────────────────────
const RSS_SOURCES = [
  { id:"bbc-me",   label:"BBC Middle East",  url:"https://feeds.bbci.co.uk/news/world/middle_east/rss.xml" },
  { id:"aljazeera",label:"Al Jazeera",        url:"https://www.aljazeera.com/xml/rss/all.xml" },
  { id:"arabnews", label:"Arab News",         url:"https://www.arabnews.com/rss.xml" },
  { id:"guardian", label:"The Guardian",      url:"https://www.theguardian.com/world/rss" },
  { id:"national", label:"The National UAE",  url:"https://www.thenationalnews.com/arc/outboundfeeds/rss/?outputType=xml" },
  { id:"mee",      label:"Middle East Eye",   url:"https://www.middleeasteye.net/rss" },
  { id:"aa",       label:"Anadolu Agency",    url:"https://www.aa.com.tr/en/rss/default?cat=world" },
];
const MASTODON_TAGS = [
  { tag:"MiddleEast", country:"Regional" },{ tag:"UAE", country:"UAE" },
  { tag:"Dubai", country:"UAE" },{ tag:"SaudiArabia", country:"Saudi Arabia" },
  { tag:"Qatar", country:"Qatar" },{ tag:"Gaza", country:"Palestine" },
  { tag:"Iran", country:"Iran" },{ tag:"OPEC", country:"Regional" },
  { tag:"Israel", country:"Israel" },{ tag:"Yemen", country:"Yemen" },
];
const LEMMY_QUERIES = [
  { q:"UAE Dubai", country:"UAE" },{ q:"Saudi Arabia", country:"Saudi Arabia" },
  { q:"Qatar Doha", country:"Qatar" },{ q:"Middle East", country:"Regional" },
  { q:"OPEC oil", country:"Regional" },{ q:"Iran nuclear", country:"Iran" },
  { q:"Israel Gaza", country:"Palestine" },{ q:"Egypt economy", country:"Egypt" },
];
const HN_QUERIES = [
  "UAE Dubai","Saudi Arabia","Qatar","Gulf OPEC",
  "Middle East geopolitics","Houthi Red Sea","Israel Gaza",
  "Iran nuclear","NEOM Saudi","GCC economy",
];

// ── Fetchers ─────────────────────────────────────────────────────────────────
async function fetchRSS({ id, label, url }) {
  try {
    process.stdout.write(`  RSS ${label}... `);
    const r = await fetch(url, {
      headers:{ "User-Agent":"Mozilla/5.0 (compatible; OpenEye-Backfill/1.0)" },
      redirect:"follow",
      signal: AbortSignal.timeout(15000),
    });
    if (!r.ok) { console.log(`SKIP (${r.status})`); return []; }
    const xml = await r.text();
    const items = parseRSS(xml, id, label);
    console.log(`${items.length} items`);
    return items;
  } catch(e) { console.log(`ERROR: ${e.message}`); return []; }
}

async function fetchMastodon({ tag, country }) {
  try {
    const r = await fetch(
      `https://mastodon.social/api/v1/timelines/tag/${tag}?limit=40`,
      { headers:{"User-Agent":"OpenEye-Backfill/1.0"}, signal:AbortSignal.timeout(10000) }
    );
    if (!r.ok) return [];
    const posts = await r.json();
    return posts.filter(p=>p.content&&!p.reblog).map(p=>{
      const txt = p.content.replace(/<[^>]+>/g," ").replace(/\s+/g," ").trim();
      return {
        id:"masto-"+p.id, title:txt.slice(0,160)||`#${tag} signal`,
        summary:txt.slice(0,240), url:p.url||"",
        timestamp:p.created_at, source:`#${tag} (Mastodon)`,
        sourceType:"Mastodon", tag:tag.toUpperCase(), country,
        score:p.favourites_count||0, comments:p.replies_count||0,
      };
    });
  } catch { return []; }
}

async function fetchLemmy({ q, country }) {
  try {
    const r = await fetch(
      `https://lemmy.world/api/v3/search?q=${encodeURIComponent(q)}&type_=Posts&sort=TopMonth&limit=20`,
      { signal:AbortSignal.timeout(10000) }
    );
    if (!r.ok) return [];
    const d = await r.json();
    return (d.posts||[]).map(p=>{
      const post=p.post; const counts=p.counts||{};
      return {
        id:"lemmy-"+post.id, title:post.name,
        summary:(post.body||"").slice(0,220)||`score:${counts.score||0}`,
        url:post.ap_id||post.url||"", timestamp:post.published,
        source:"Lemmy", sourceType:"Lemmy", tag:"SOCIAL", country,
        score:counts.score||0, comments:counts.comments||0,
      };
    });
  } catch { return []; }
}

async function fetchHN() {
  const since = Math.floor(Date.now()/1000) - 30*24*3600;
  const all = []; const seen = new Set();
  for (const q of HN_QUERIES) {
    try {
      process.stdout.write(`  HN "${q}"... `);
      const d = await fetch(
        `https://hn.algolia.com/api/v1/search?query=${encodeURIComponent(q)}&tags=story&hitsPerPage=40&numericFilters=created_at_i>${since}`
      ).then(r=>r.json());
      let added = 0;
      for (const h of (d.hits||[])) {
        if (!h.title||seen.has(h.objectID)) continue;
        seen.add(h.objectID);
        all.push({
          id:"hn-"+h.objectID, title:h.title,
          summary:`${h.points||0} pts · ${h.num_comments||0} comments`,
          url:h.url||`https://news.ycombinator.com/item?id=${h.objectID}`,
          timestamp:h.created_at, source:"Hacker News",
          sourceType:"HN", tag:"TECH",
          score:h.points||0, comments:h.num_comments||0,
        });
        added++;
      }
      console.log(`${added} stories`);
      await new Promise(r=>setTimeout(r,300));
    } catch(e) { console.log(`ERROR: ${e.message}`); }
  }
  return all;
}

// ── Upstash helpers ───────────────────────────────────────────────────────────
async function upstashGet(key) {
  const res = await fetch(`${UPSTASH_URL}/get/${encodeURIComponent(key)}`, {
    headers:{ "Authorization":`Bearer ${UPSTASH_TOKEN}` },
  });
  const json = await res.json();
  return json.result;
}
async function upstashPipeline(commands) {
  const res = await fetch(`${UPSTASH_URL}/pipeline`, {
    method:"POST",
    headers:{ "Authorization":`Bearer ${UPSTASH_TOKEN}`, "Content-Type":"application/json" },
    body: JSON.stringify(commands),
  });
  return res.json();
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log("Open Eye - 30-day backfill\n");

  console.log("Fetching RSS...");
  const rssItems = (await Promise.all(RSS_SOURCES.map(fetchRSS))).flat();

  console.log("\nFetching Mastodon...");
  const mastoItems = (await Promise.allSettled(MASTODON_TAGS.map(fetchMastodon)))
    .filter(r=>r.status==="fulfilled").flatMap(r=>r.value);
  console.log(`  ${mastoItems.length} posts total`);

  console.log("\nFetching Lemmy...");
  const lemmyItems = (await Promise.allSettled(LEMMY_QUERIES.map(fetchLemmy)))
    .filter(r=>r.status==="fulfilled").flatMap(r=>r.value);
  console.log(`  ${lemmyItems.length} posts total`);

  console.log("\nFetching Hacker News (30 days)...");
  const hnItems = await fetchHN();
  console.log(`  ${hnItems.length} stories total`);

  console.log("\nNormalising...");
  const raw = [...rssItems, ...mastoItems, ...lemmyItems, ...hnItems];
  const seenIds = new Set();
  const unique = raw.filter(i=>{ if(!i.id||seenIds.has(i.id)) return false; seenIds.add(i.id); return true; });
  const cutoff = Date.now() - 30*24*60*60*1000;
  const recent = unique
    .filter(a => new Date(a.timestamp).getTime() > cutoff)
    .map(normalise)
    .sort((a,b) => new Date(b.timestamp)-new Date(a.timestamp))
    .slice(0, 3000);
  console.log(`  Raw: ${raw.length} → Unique: ${unique.length} → Last 30d: ${recent.length}`);

  console.log("\nConnecting to Upstash Redis...");
  const existingRaw = await upstashGet("signals:articles");
  const existing = existingRaw ? JSON.parse(existingRaw) : [];
  console.log(`  Existing records: ${existing.length}`);

  const existingIds = new Set(existing.map(a=>a.id));
  const newItems = recent.filter(a=>!existingIds.has(a.id));
  const merged = [...newItems, ...existing]
    .sort((a,b) => new Date(b.timestamp)-new Date(a.timestamp))
    .slice(0, 3000);

  const meta = {
    lastIngest: new Date().toISOString(),
    totalPushed: merged.length,
    firstIngest: merged[merged.length-1]?.timestamp || new Date().toISOString(),
    backfilledAt: new Date().toISOString(),
  };

  console.log(`  Writing ${merged.length} records (${newItems.length} new)...`);
  const result = await upstashPipeline([
    ["SET", "signals:articles", JSON.stringify(merged)],
    ["SET", "signals:meta",     JSON.stringify(meta)],
  ]);

  if (result[0]?.result === "OK") {
    console.log("\nBackfill complete!");
    console.log(`  Total signals: ${merged.length}`);
    console.log(`  New added:     ${newItems.length}`);
    const bySrc = {};
    merged.forEach(a=>{ bySrc[a.sourceType]=(bySrc[a.sourceType]||0)+1; });
    console.log("\n  Breakdown:");
    Object.entries(bySrc).sort((a,b)=>b[1]-a[1]).forEach(([s,c])=>
      console.log(`    ${s.padEnd(12)} ${c}`)
    );
    console.log("\nDone. Your hourly cron will append new data automatically.");
  } else {
    console.error("Write failed:", JSON.stringify(result));
    process.exit(1);
  }
}

main().catch(e=>{ console.error("Fatal:", e.message); process.exit(1); });
