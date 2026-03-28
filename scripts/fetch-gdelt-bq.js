#!/usr/bin/env node
// scripts/fetch-gdelt-bq.js
// Fetches GDELT 2.0 event data from BigQuery for MENA countries.
// Runs in two modes:
//   node scripts/fetch-gdelt-bq.js              → daily append (yesterday's data)
//   node scripts/fetch-gdelt-bq.js --backfill   → seed from Jan 1 2026 to today
//   node scripts/fetch-gdelt-bq.js --backfill --start 2026-01-01 --end 2026-03-01
//
// Output: public/data/gdelt-events.json (append mode, 90-day rolling window)
// Requires: GCP_SERVICE_ACCOUNT_JSON env var (raw JSON string, not base64)
//
// BigQuery table: gdelt-bq.gdeltv2.events (partitioned by SQLDATE)
// Free tier: 1TB/month — each day of MENA events is ~2-5GB scan

import { writeFileSync, readFileSync, mkdirSync, existsSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT = resolve(__dirname, "../public/data/gdelt-events.json");
mkdirSync(dirname(OUT), { recursive: true });

const ROLLING_WINDOW_DAYS = 90;

// ── Arg parsing ───────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const getArg = flag => { const i = args.indexOf(flag); return i !== -1 ? args[i + 1] : undefined; };
const isBackfill = args.includes("--backfill");
const startArg   = getArg("--start");
const endArg     = getArg("--end");

// ── MENA country codes (CAMEO/FIPS used by GDELT events table) ───────────────
// ActionGeo_CountryCode in GDELT events uses FIPS 10-4
const MENA_FIPS = [
  "AE","SA","QA","KU","BA","MU","JO","LE","SY","IZ","PS","IS","EG","LY","TS","AG","MO","SU","YM","IR"
];

const COUNTRY_MAP = {
  AE:"UAE", SA:"Saudi Arabia", QA:"Qatar", KU:"Kuwait", BA:"Bahrain",
  MU:"Oman", JO:"Jordan", LE:"Lebanon", SY:"Syria", IZ:"Iraq",
  PS:"Palestine", IS:"Israel", EG:"Egypt", LY:"Libya", TS:"Tunisia",
  AG:"Algeria", MO:"Morocco", SU:"Sudan", YM:"Yemen", IR:"Iran",
};

// GDELT QuadClass
const QUAD_TYPE  = { "1":"VERBAL_COOP", "2":"MATERIAL_COOP", "3":"VERBAL_CONF", "4":"MATERIAL_CONF" };
const QUAD_LABEL = { "1":"Verbal Cooperation", "2":"Material Cooperation", "3":"Verbal Conflict", "4":"Material Conflict" };

// CAMEO root code labels (EventRootCode)
const CAMEO_ROOT = {
  "01":"Make Statement",       "02":"Appeal",
  "03":"Express Intent",       "04":"Consult",
  "05":"Diplomatic Coop",      "06":"Material Coop",
  "07":"Provide Aid",          "08":"Yield",
  "09":"Investigate",          "10":"Demand",
  "11":"Disapprove",           "12":"Reject",
  "13":"Threaten",             "14":"Protest",
  "15":"Show Force",           "16":"Reduce Relations",
  "17":"Coerce",               "18":"Assault",
  "19":"Fight",                "20":"Mass Violence",
};

// CAMEO base + specific event code labels (EventCode)
// Covers the ~200 most common codes — unmapped codes fall back to root label
const CAMEO_CODE = {
  // 01 – Statements
  "010":"Make statement","011":"Discuss by phone/in person","012":"Express disagreement",
  "013":"Acknowledge","014":"Deny responsibility","015":"Acknowledge ceasefire",
  "016":"Apologize","017":"Engage in symbolic act","018":"Threaten to punish",
  "019":"Demand",
  // 02 – Appeal
  "020":"Appeal","021":"Appeal for help","022":"Appeal for material support",
  "023":"Appeal for political support","024":"Appeal for diplomatic cooperation",
  "025":"Appeal for settlement","026":"Appeal to yield","027":"Appeal to engage in diplomacy",
  "028":"Appeal to meet","029":"Appeal for investigation",
  // 03 – Express intent
  "030":"Express intent to cooperate","031":"Express intent to meet",
  "032":"Express intent to settle","033":"Express intent to aid",
  "034":"Express intent to release","035":"Express intent to negotiate",
  "036":"Express intent to cooperate militarily","037":"Express intent to reduce conflict",
  "038":"Express intent to impose sanctions",
  // 04 – Consult
  "040":"Consult","041":"Discuss by phone","042":"Make a visit",
  "043":"Host a visit","044":"Meet at a third location","045":"Mediate",
  "046":"Engage in negotiation",
  // 05 – Diplomatic cooperation
  "050":"Engage in diplomatic cooperation","051":"Express diplomatic support",
  "052":"Praise or endorse","053":"Rally public support","054":"Grant diplomatic recognition",
  "055":"Defend, not condemn","056":"Apologize for act","057":"Sign formal agreement",
  "058":"Grant asylum",
  // 06 – Material cooperation
  "060":"Engage in material cooperation","061":"Cooperate economically",
  "062":"Provide military aid","063":"Share intelligence",
  "064":"Share technology","065":"Provide intelligence","066":"Give or loan money",
  // 07 – Provide aid
  "070":"Provide aid","071":"Provide economic aid","072":"Provide military aid",
  "073":"Provide humanitarian aid","074":"Provide military protection",
  "075":"Grant asylum","076":"Release persons","077":"Release property",
  "078":"Return/repatriate","079":"Lift sanctions",
  // 08 – Yield
  "080":"Yield","081":"Ease restrictions","082":"Release persons",
  "083":"Return property","084":"Return territory","085":"Ease blockade",
  "086":"Withdraw troops","087":"Surrender, yield to demands",
  "0871":"Halt military action","0872":"Disarm","0873":"Retreat",
  // 09 – Investigate
  "090":"Investigate","091":"Investigate crime","092":"Investigate human rights abuses",
  "093":"Host fact-finding","094":"Monitor ceasefire",
  // 10 – Demand
  "100":"Demand","101":"Demand political reform","102":"Demand leadership change",
  "103":"Demand rights","104":"Demand military action","105":"Demand sanctions",
  "1052":"Demand ceasefire","1053":"Demand withdrawal","1054":"Demand release",
  "1055":"Demand peace settlement","1056":"Demand end of sanctions",
  // 11 – Disapprove
  "110":"Criticize","111":"Criticize government","112":"Accuse",
  "1121":"Accuse of crime","1122":"Accuse of human rights abuses",
  "1123":"Accuse of aggression","1124":"Accuse of terrorism",
  "113":"Complain","114":"Denounce","115":"Gesture of hostility",
  // 12 – Reject
  "120":"Reject","121":"Reject diplomatic cooperation","122":"Reject request",
  "123":"Reject proposal","124":"Reject request for material aid",
  "1241":"Refuse to allow","1242":"Refuse to allow investigation",
  "1243":"Refuse to allow inspection","1244":"Refuse to allow access",
  "1245":"Refuse to release","1246":"Reject sanctions",
  "125":"Reject accusation","126":"Reject peace proposal",
  "127":"Reject ceasefire","128":"Reject peace settlement",
  // 13 – Threaten
  "130":"Threaten","131":"Threaten political dissent",
  "132":"Threaten military force","133":"Threaten sanctions",
  "134":"Threaten with political/administrative action",
  "135":"Threaten to halt negotiations","136":"Threaten to boycott",
  "137":"Threaten to reduce relations","138":"Threaten with military force",
  "1381":"Threaten nuclear attack","1382":"Threaten aerial attack",
  "1383":"Threaten blockade","1384":"Threaten occupation",
  "1385":"Threaten to seize","1386":"Threaten attack",
  "139":"Threaten to expel",
  // 14 – Protest
  "140":"Engage in political dissent","141":"Demonstrate",
  "142":"Conduct hunger strike","143":"Conduct strike or boycott",
  "144":"Obstruct","145":"Conduct riots","1451":"Riot",
  // 15 – Show force
  "150":"Show of force posture","151":"Increase military alert",
  "152":"Mobilise / increase police","153":"Mobilise military",
  "154":"Conduct military exercises","155":"Raise alert status",
  // 16 – Reduce relations
  "160":"Reduce relations","161":"Reduce or break diplomatic relations",
  "162":"Accuse","163":"Impose embargo / boycott / sanctions",
  "1631":"Impose economic sanctions","1632":"Impose arms embargo",
  "1633":"Impose blockade","1634":"Impose travel restrictions",
  "164":"Halt negotiations","165":"Expel or deport individuals",
  "166":"Expel or recall ambassador","167":"Halt negotiations",
  "168":"Defy court order","169":"Impose administrative action",
  // 17 – Coerce
  "170":"Coerce","171":"Seize or damage","172":"Impose sanctions",
  "173":"Arrest / detain","174":"Expel / deport","175":"Use tactics of harassment",
  "1751":"Assassinate","1752":"Torture","1753":"Kill",
  "176":"Attack","177":"Impose curfew","178":"Detain",
  // 18 – Assault
  "180":"Use conventional military force","181":"Impose blockade",
  "182":"Occupy territory","183":"Fight","184":"Launch strike",
  "1841":"Strike / bomb","1842":"Naval blockade",
  "185":"Assassinate","186":"Use chemical / biological / nuclear weapons",
  "1861":"Use chemical weapons","1862":"Use biological weapons",
  "1863":"Use nuclear weapons","187":"Conduct suicide / car bomb",
  "1871":"Conduct suicide bombing","188":"Conduct aerial bombing",
  "189":"Conduct military strike",
  // 19 – Fight
  "190":"Use unconventional mass violence","191":"Abduct / hijack",
  "192":"Physically assault","193":"Conduct suicide / car bomb",
  "194":"Use suicide / car bomb","1941":"Use suicide bomber",
  "195":"Employ aerial weapons","1951":"Aerial bombing",
  "196":"Violate ceasefire",
  // 20 – Mass violence
  "200":"Use unconventional mass violence","201":"Engage in mass expulsion",
  "202":"Engage in mass killings","2021":"Engage in ethnic cleansing",
  "203":"Engage in ethnic cleansing","204":"Use weapons of mass destruction",
};

function getEventLabel(eventCode, rootCode) {
  return CAMEO_CODE[eventCode] || CAMEO_CODE[rootCode] || CAMEO_ROOT[rootCode] || `Code ${eventCode}`;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));
const toDate = d => d.toISOString().split("T")[0];

// ── GCP Auth ──────────────────────────────────────────────────────────────────
async function getBQToken(saJson) {
  const sa = JSON.parse(saJson);
  const now = Math.floor(Date.now() / 1000);
  const enc = s => Buffer.from(JSON.stringify(s)).toString("base64url");
  const header  = { alg: "RS256", typ: "JWT" };
  const payload = {
    iss: sa.client_email,
    scope: "https://www.googleapis.com/auth/bigquery.readonly",
    aud: "https://oauth2.googleapis.com/token",
    exp: now + 3600,
    iat: now,
  };

  const sigInput = `${enc(header)}.${enc(payload)}`;
  const keyPem = sa.private_key
    .replace(/-----BEGIN PRIVATE KEY-----/, "")
    .replace(/-----END PRIVATE KEY-----/, "")
    .replace(/\n/g, "");
  const keyBuf = Buffer.from(keyPem, "base64");
  const cryptoKey = await crypto.subtle.importKey(
    "pkcs8", keyBuf,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false, ["sign"]
  );
  const sig = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", cryptoKey, Buffer.from(sigInput));
  const jwt = `${sigInput}.${Buffer.from(sig).toString("base64url")}`;

  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion: jwt,
    }),
    signal: AbortSignal.timeout(15000),
  });
  if (!res.ok) throw new Error(`GCP auth failed: ${await res.text()}`);
  const d = await res.json();
  return d.access_token;
}

// ── BigQuery query runner ─────────────────────────────────────────────────────
async function runBQQuery(token, projectId, query) {
  const res = await fetch(
    `https://bigquery.googleapis.com/bigquery/v2/projects/${projectId}/queries`,
    {
      method: "POST",
      headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ query, useLegacySql: false, timeoutMs: 60000, maxResults: 10000 }),
      signal: AbortSignal.timeout(70000),
    }
  );
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`BQ HTTP ${res.status}: ${err.slice(0, 300)}`);
  }
  const result = await res.json();
  if (result.errors?.length) throw new Error(`BQ errors: ${JSON.stringify(result.errors[0])}`);

  // If job didn't complete inline, poll for results
  if (!result.jobComplete) {
    const jobId = result.jobReference?.jobId;
    if (!jobId) throw new Error("BQ job not complete and no jobId returned");
    return await pollBQJob(token, projectId, jobId);
  }
  return result;
}

async function pollBQJob(token, projectId, jobId, maxWaitMs = 120000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    await sleep(3000);
    const res = await fetch(
      `https://bigquery.googleapis.com/bigquery/v2/projects/${projectId}/queries/${jobId}?maxResults=10000&timeoutMs=10000`,
      { headers: { "Authorization": `Bearer ${token}` }, signal: AbortSignal.timeout(15000) }
    );
    if (!res.ok) continue;
    const d = await res.json();
    if (d.jobComplete) return d;
  }
  throw new Error("BQ job timed out after polling");
}

// ── Query GDELT events for one day ────────────────────────────────────────────
async function fetchEventsForDate(token, projectId, dateStr) {
  // dateStr: "YYYY-MM-DD"
  const sqlDate = dateStr.replace(/-/g, ""); // GDELT uses YYYYMMDD integers
  const fipsList = MENA_FIPS.map(f => `'${f}'`).join(",");

  // Using events_partitioned + _PARTITIONTIME → scans only that day's partition (~200MB vs 63GB)
  const query = `
    SELECT
      GLOBALEVENTID,
      SQLDATE,
      Actor1Name,
      Actor1CountryCode,
      Actor2Name,
      Actor2CountryCode,
      IsRootEvent,
      EventCode,
      EventBaseCode,
      EventRootCode,
      QuadClass,
      GoldsteinScale,
      NumMentions,
      NumSources,
      NumArticles,
      AvgTone,
      ActionGeo_CountryCode,
      ActionGeo_FullName,
      ActionGeo_Lat,
      ActionGeo_Long,
      SOURCEURL
    FROM \`gdelt-bq.gdeltv2.events_partitioned\`
    WHERE _PARTITIONTIME = TIMESTAMP('${dateStr}')
      AND SQLDATE = ${sqlDate}
      AND ActionGeo_CountryCode IN (${fipsList})
      AND NumArticles >= 2
    ORDER BY NumArticles DESC, ABS(GoldsteinScale) DESC
    LIMIT 500
  `;

  const result = await runBQQuery(token, projectId, query);
  const rows = result.rows || [];

  return rows.map(row => {
    const f = row.f;
    const fips       = f[16].v;
    const eventCode  = f[7].v  || "";
    const baseCode   = f[8].v  || "";
    const rootCode   = f[9].v  || "";
    const quadClass  = f[10].v || "";
    return {
      id:          `gdelt-${f[0].v}`,
      date:        `${f[1].v}`.replace(/(\d{4})(\d{2})(\d{2})/, "$1-$2-$3"),
      actor1:      f[2].v || "",
      actor1cc:    f[3].v || "",
      actor2:      f[4].v || "",
      actor2cc:    f[5].v || "",
      isRoot:      f[6].v === "1" || f[6].v === true,
      eventCode,
      baseCode,
      rootCode,
      eventLabel:  getEventLabel(eventCode, rootCode),
      rootLabel:   CAMEO_ROOT[rootCode] || rootCode,
      quadClass,
      quadType:    QUAD_TYPE[quadClass]  || "UNKNOWN",
      quadLabel:   QUAD_LABEL[quadClass] || "Unknown",
      goldstein:   parseFloat(f[11].v) || 0,
      mentions:    parseInt(f[12].v)   || 0,
      sources:     parseInt(f[13].v)   || 0,
      articles:    parseInt(f[14].v)   || 0,
      tone:        parseFloat(f[15].v) || 0,
      country:     COUNTRY_MAP[fips]   || fips,
      fips,
      location:    f[17].v || "",
      lat:         parseFloat(f[18].v) || 0,
      lon:         parseFloat(f[19].v) || 0,
      url:         f[20].v || "",
      timestamp:   new Date(`${f[1].v}`.replace(/(\d{4})(\d{2})(\d{2})/, "$1-$2-$3")).toISOString(),
    };
  });
}

// ── Load/save helpers ─────────────────────────────────────────────────────────
function loadExisting() {
  if (!existsSync(OUT)) return [];
  try {
    const d = JSON.parse(readFileSync(OUT, "utf8"));
    return d.events || [];
  } catch { return []; }
}

function save(events) {
  const cutoff = Date.now() - ROLLING_WINDOW_DAYS * 24 * 3600 * 1000;
  const deduped = [...new Map(events.map(e => [e.id, e])).values()];
  const filtered = deduped
    .filter(e => new Date(e.timestamp).getTime() > cutoff)
    .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  writeFileSync(OUT, JSON.stringify({
    generated_at: new Date().toISOString(),
    count: filtered.length,
    date_range: {
      from: filtered[filtered.length - 1]?.date || null,
      to:   filtered[0]?.date || null,
    },
    events: filtered,
  }, null, 2));
  return filtered.length;
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  const GCP_SA = process.env.GCP_SERVICE_ACCOUNT_JSON;
  if (!GCP_SA) {
    console.error("[gdelt-bq] GCP_SERVICE_ACCOUNT_JSON not set — skipping");
    process.exit(0);
  }

  const projectId = JSON.parse(GCP_SA).project_id;
  console.log(`[gdelt-bq] Project: ${projectId}`);

  const token = await getBQToken(GCP_SA);
  console.log("[gdelt-bq] Token obtained");

  const existing = loadExisting();
  const existingIds = new Set(existing.map(e => e.id));
  console.log(`[gdelt-bq] Loaded ${existing.length} existing events`);

  let datesToFetch = [];

  if (isBackfill) {
    const start = new Date(startArg || "2026-01-01");
    const end   = endArg ? new Date(endArg) : new Date(Date.now() - 24*3600*1000); // backfill up to yesterday
    console.log(`[gdelt-bq] Backfill mode: ${toDate(start)} → ${toDate(end)}`);
    let d = new Date(start);
    while (d <= end) {
      datesToFetch.push(toDate(d));
      d = new Date(d.getTime() + 24*3600*1000);
    }
    console.log(`[gdelt-bq] ${datesToFetch.length} days to fetch`);
  } else {
    // Daily mode: fetch yesterday (today's partition may not be complete)
    const yesterday = new Date(Date.now() - 24*3600*1000);
    datesToFetch = [toDate(yesterday)];
    console.log(`[gdelt-bq] Daily mode: fetching ${datesToFetch[0]}`);
  }

  const allEvents = [...existing];
  let totalNew = 0;

  for (let i = 0; i < datesToFetch.length; i++) {
    const dateStr = datesToFetch[i];
    try {
      console.log(`[gdelt-bq] [${i+1}/${datesToFetch.length}] Querying ${dateStr}…`);
      const events = await fetchEventsForDate(token, projectId, dateStr);
      const newEvents = events.filter(e => !existingIds.has(e.id));
      newEvents.forEach(e => existingIds.add(e.id));
      allEvents.push(...newEvents);
      totalNew += newEvents.length;
      console.log(`  → ${newEvents.length} new events (${events.length} total for ${dateStr})`);

      // Save incrementally every 7 days during backfill
      if (isBackfill && (i + 1) % 7 === 0) {
        const saved = save(allEvents);
        console.log(`  💾 Saved ${saved} total events`);
      }
    } catch (err) {
      console.error(`[gdelt-bq] Failed for ${dateStr}:`, err.message);
      // Don't abort — skip this day and continue
    }

    // Small delay between days to be a polite BQ user
    if (datesToFetch.length > 1) await sleep(500);
  }

  const total = save(allEvents);
  console.log(`\n[gdelt-bq] Done — ${total} total events, ${totalNew} new`);
}

main().catch(err => { console.error("[gdelt-bq] Fatal:", err.message); process.exit(1); });
