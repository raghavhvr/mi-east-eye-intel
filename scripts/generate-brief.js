#!/usr/bin/env node
// scripts/generate-brief.js
// AI brief generation: Groq (primary) → Gemini (429 fallback) → keyword-only
// → public/data/brief.json + public/data/lottery-brief.json
// Run by GitHub Actions cron every 6 hours.

import { writeFileSync, readFileSync, existsSync, mkdirSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = resolve(__dirname, "../public/data");
mkdirSync(DATA_DIR, { recursive: true });

const GROQ_KEY   = process.env.GROQ_API_KEY   || "";
const GEMINI_KEY = process.env.GEMINI_API_KEY  || "";

const BRIEF_PROMPT = (headlines) => `You are a senior regional intelligence analyst for the MENA region. Analyse these open-source headlines and produce a concise brief.

Headlines:
${headlines.slice(0,12).map(h=>`- ${h}`).join("\n")}

Return ONLY a JSON object, no markdown, no explanation:
{"threatLevel":"LOW|MODERATE|ELEVATED|CRITICAL","topTheme":"max 5 words","summary":"2 sentences analyst tone","keyTrends":["trend1","trend2","trend3"],"watchItems":["item1","item2"],"userSentiment":"OPTIMISTIC|CAUTIOUS|ANXIOUS|FEARFUL|INDIFFERENT","sentimentDrivers":["driver1","driver2"]}`;

const LOTTERY_PROMPT = (headlines) => `You are a public sentiment analyst specialising in gambling psychology and consumer behaviour for the Middle East. Based on these open-source posts about lottery, gambling, and winning, analyse the public mood.

Signals:
${headlines.slice(0,15).map(h=>`- ${h}`).join("\n")}

Return ONLY a JSON object, no markdown, no explanation:
{"overallMood":"EUPHORIC|HOPEFUL|ANXIOUS|CYNICAL|RESIGNED","sentimentSplit":{"positive":0,"neutral":0,"negative":0},"dominantNarrative":"string max 8 words","keyEmotions":["emotion1","emotion2","emotion3"],"psychInsight":"2 sentences on psychology driving these reactions","riskSignals":["signal1","signal2"],"opportunitySignals":["signal1","signal2"]}
Note: sentimentSplit values must sum to 100.`;

// ── Providers ────────────────────────────────────────────────────────────────
async function callGroq(prompt) {
  const res = await fetch("https://api.groq.com/openai/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${GROQ_KEY}` },
    body: JSON.stringify({ model: "llama-3.3-70b-versatile", messages: [{ role: "user", content: prompt }], temperature: 0.2, max_tokens: 600 }),
    signal: AbortSignal.timeout(30000),
  });
  if (res.status === 429) throw new Error("RATE_LIMITED");
  if (!res.ok) throw new Error(`Groq HTTP ${res.status}`);
  const d = await res.json();
  const text = d.choices?.[0]?.message?.content||"";
  const clean = text.replace(/```json|```/g,"").trim();
  return { parsed: JSON.parse(clean), model: "groq/llama-3.3-70b-versatile" };
}

async function callGemini(prompt) {
  const model = "gemini-2.5-flash-lite-preview-06-17";
  const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${GEMINI_KEY}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }], generationConfig: { temperature: 0.2, maxOutputTokens: 600 } }),
    signal: AbortSignal.timeout(30000),
  });
  if (!res.ok) throw new Error(`Gemini HTTP ${res.status}`);
  const d = await res.json();
  const text = d.candidates?.[0]?.content?.parts?.[0]?.text||"";
  const clean = text.replace(/```json|```/g,"").trim();
  return { parsed: JSON.parse(clean), model: `gemini/${model}` };
}

function keywordBrief(headlines) {
  const all = headlines.join(" ").toLowerCase();
  const crisis = ["war","attack","killed","explosion","airstrike","siege","missile","bomb"].filter(w=>all.includes(w)).length;
  const economic = ["oil","economy","investment","market","gdp"].filter(w=>all.includes(w)).length;
  const political = ["government","sanction","election","summit","diplomacy"].filter(w=>all.includes(w)).length;
  const threatLevel = crisis>4?"CRITICAL":crisis>2?"ELEVATED":crisis>0?"MODERATE":"LOW";
  const topTheme = crisis>economic&&crisis>political?"Regional Security":economic>political?"Economic Activity":"Political Developments";
  return { parsed: { threatLevel, topTheme, summary:`Regional signals indicate ${threatLevel.toLowerCase()} activity with focus on ${topTheme.toLowerCase()}. Monitoring ${headlines.length} open-source data points.`, keyTrends:["Ongoing monitoring","Signal analysis active","Data refresh scheduled"], watchItems:["Regional escalation indicators","Economic sentiment shifts"], userSentiment:"CAUTIOUS", sentimentDrivers:["Geopolitical uncertainty","Economic signals"] }, model: "keyword-fallback" };
}

function keywordLotteryBrief(headlines) {
  const all = headlines.join(" ").toLowerCase();
  const hopeful = ["win","jackpot","lucky","prize","million"].filter(w=>all.includes(w)).length;
  const cynical  = ["scam","fraud","rigged","impossible","fake"].filter(w=>all.includes(w)).length;
  const anxious  = ["debt","desperate","broke","struggling","worry"].filter(w=>all.includes(w)).length;
  const overallMood = anxious>1?"ANXIOUS":cynical>hopeful?"CYNICAL":hopeful>0?"HOPEFUL":"RESIGNED";
  return { parsed: { overallMood, sentimentSplit:{ positive:Math.round(hopeful/(hopeful+cynical+anxious+1)*100), neutral:40, negative:Math.round((cynical+anxious)/(hopeful+cynical+anxious+1)*100) }, dominantNarrative:"Mixed lottery sentiment MENA", keyEmotions:["Hope","Uncertainty","Anticipation"], psychInsight:"Regional lottery sentiment reflects broader economic optimism and aspirational culture common in expatriate communities. Signals suggest persistent interest despite low odds.", riskSignals:["Problem gambling indicators","Financial desperation signals"], opportunitySignals:["High engagement with ME lottery brands","Strong community participation"] }, model: "keyword-fallback" };
}

// ── Generate with fallback chain ─────────────────────────────────────────────
async function generate(prompt, fallbackFn, label) {
  // 1. Try Groq
  if (GROQ_KEY) {
    try {
      console.log(`[brief:${label}] Trying Groq…`);
      const result = await callGroq(prompt);
      console.log(`[brief:${label}] Groq success`);
      return result;
    } catch (err) {
      if (err.message === "RATE_LIMITED") {
        console.warn(`[brief:${label}] Groq rate-limited → falling back to Gemini`);
      } else {
        console.warn(`[brief:${label}] Groq error: ${err.message} → falling back to Gemini`);
      }
    }
  }

  // 2. Try Gemini
  if (GEMINI_KEY) {
    try {
      console.log(`[brief:${label}] Trying Gemini…`);
      const result = await callGemini(prompt);
      console.log(`[brief:${label}] Gemini success`);
      return result;
    } catch (err) {
      console.warn(`[brief:${label}] Gemini error: ${err.message} → falling back to keyword brief`);
    }
  }

  // 3. Keyword fallback
  console.log(`[brief:${label}] Using keyword fallback`);
  return fallbackFn();
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log("[generate-brief] Starting…");

  // Load feeds
  const feedsPath = resolve(DATA_DIR, "feeds.json");
  const signalsPath = resolve(DATA_DIR, "signals.json");

  let headlines = [];
  let lotteryHeadlines = [];

  if (existsSync(feedsPath)) {
    const feeds = JSON.parse(readFileSync(feedsPath,"utf8"));
    headlines = (feeds.articles||[]).slice(0,12).map(a=>a.title).filter(Boolean);
  } else {
    console.warn("[generate-brief] feeds.json not found — using placeholder headlines");
    headlines = ["MENA intelligence signals active","Regional monitoring in progress"];
  }

  if (existsSync(signalsPath)) {
    const signals = JSON.parse(readFileSync(signalsPath,"utf8"));
    lotteryHeadlines = (signals.lottery?.items||[]).slice(0,15).map(i=>i.title).filter(Boolean);
  }

  // Generate intelligence brief
  const { parsed: brief, model: briefModel } = await generate(
    BRIEF_PROMPT(headlines),
    () => keywordBrief(headlines),
    "intelligence"
  );
  const briefOut = { generated_at: new Date().toISOString(), model: briefModel, ...brief };
  writeFileSync(resolve(DATA_DIR,"brief.json"), JSON.stringify(briefOut, null, 2));
  console.log(`[brief] Wrote brief.json (model: ${briefModel})`);

  // Generate lottery brief (if signals available)
  if (lotteryHeadlines.length > 0) {
    const { parsed: lotBrief, model: lotModel } = await generate(
      LOTTERY_PROMPT(lotteryHeadlines),
      () => keywordLotteryBrief(lotteryHeadlines),
      "lottery"
    );
    const lotBriefOut = { generated_at: new Date().toISOString(), model: lotModel, ...lotBrief };
    writeFileSync(resolve(DATA_DIR,"lottery-brief.json"), JSON.stringify(lotBriefOut, null, 2));
    console.log(`[brief] Wrote lottery-brief.json (model: ${lotModel})`);
  } else {
    console.log("[brief] No lottery signals — skipping lottery brief");
  }

  console.log("[generate-brief] Done");
}

main().catch(err => { console.error("[generate-brief] Fatal:", err); process.exit(0); });
