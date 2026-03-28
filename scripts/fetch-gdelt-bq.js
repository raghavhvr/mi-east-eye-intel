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

// GDELT QuadClass: 1=Verbal Coop, 2=Material Coop, 3=Verbal Conflict, 4=Material Conflict
const QUAD_LABEL = { "1":"Cooperation","2":"Cooperation","3":"Conflict","4":"Conflict" };
const QUAD_TYPE  = { "1":"VERBAL_COOP","2":"MATERIAL_COOP","3":"VERBAL_CONF","4":"MATERIAL_CONF" };

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

  // We query the events table filtered to MENA action geo country codes
  // Using _PARTITIONTIME to avoid scanning the full table (~30GB/day globally)
  // This query scans only the partition for the specific day — very cheap
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
    FROM \`gdelt-bq.gdeltv2.events\`
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
    const fips = f[16].v;
    return {
      id:          `gdelt-${f[0].v}`,
      date:        `${f[1].v}`.replace(/(\d{4})(\d{2})(\d{2})/, "$1-$2-$3"),
      actor1:      f[2].v || "",
      actor1cc:    f[3].v || "",
      actor2:      f[4].v || "",
      actor2cc:    f[5].v || "",
      isRoot:      f[6].v === "1" || f[6].v === true,
      eventCode:   f[7].v || "",
      quadClass:   f[10].v || "",
      quadLabel:   QUAD_LABEL[f[10].v] || "Unknown",
      quadType:    QUAD_TYPE[f[10].v] || "UNKNOWN",
      goldstein:   parseFloat(f[11].v) || 0,
      mentions:    parseInt(f[12].v) || 0,
      sources:     parseInt(f[13].v) || 0,
      articles:    parseInt(f[14].v) || 0,
      tone:        parseFloat(f[15].v) || 0,
      country:     COUNTRY_MAP[fips] || fips,
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
