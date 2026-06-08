/**
 * geminiLiveBase.js — Shared Gemini Live infrastructure for all Vani verticals
 *
 * All 4 agents (salon, restaurant, supermarket, EV) call runLiveCall() from
 * this module. Only the system prompt, tools, and tool handler differ per vertical.
 *
 * Audio bridge:
 *   Vobiz  → 8kHz LINEAR16 → upsample to 16kHz → Gemini Live input
 *   Gemini → 24kHz LINEAR16 → downsample to 8kHz → Vobiz playAudio
 */

'use strict';

const WebSocket = require('ws');
const { GoogleGenAI, Modality } = require('@google/genai');
const { getBusinessByDID, getBusinessConfig, getCatalogue, logCall, markBookingWhatsAppSent } = require('./supabase');
const { sendWhatsAppConfirmation } = require('./whatsapp');
// 2026-06-07: μ-law codec helper for Vobiz outbound bidirectional Stream fix.
// Env-gated via STREAM_CODEC=mulaw; default linear16 preserves inbound behavior.
const ulaw = require('./ulaw');
const STREAM_CODEC = (process.env.STREAM_CODEC || 'linear16').toLowerCase();

// ─── Model config ─────────────────────────────────────────────────────────────

const LIVE_ALLOWED_MODELS = [
  'gemini-3.1-flash-live-preview',                 // PRIMARY — verified working in prod 2026-05-24 16:07 IST
  'gemini-2.5-flash-native-audio-preview-12-2025', // FALLBACK — older stable Live model
  // NOTE: gemini-2.0-flash-live-001, gemini-live-2.5-flash-preview, and
  // gemini-2.5-flash-preview-native-audio-dialog were removed from v1beta
  // in May 2026 — DO NOT add them back
];

// 2026-05-24 incident timeline (resolved):
//   ~04:00 UTC: gemini-3.1-flash-live-preview started returning inlineData.data="AAA="
//               (2 zero bytes) instead of real PCM audio. Every D&V Ather call went
//               silent. Diagnostic captured on call 95029fcb stats.firstServerContentSamples.
//   ~10:25 UTC: swapped to 2.5 (commit 04e1eda2). 2.5 also returned no audio (call
//               f44042ba audio_sent=false on build 667409e6) — likely a broader
//               Google Live API hiccup, not just 3.1.
//   ~10:37 UTC: Vineeth set Railway GEMINI_MODEL back to 3.1, calls came back —
//               Google fixed the upstream regression. Confirmed full conversation on
//               call 55fae1d9 at 16:07 IST.
// 2026-05-24 evening incident (resolved by this commit):
//   ~22:24 IST: D&V Ather calls silent again. Stats showed firstBotAudioSent=false
//               but firstServerContentSamples captured 8360-char real PCM in
//               modelTurn.parts[0].inlineData. Root cause: 3200-byte flush gate
//               held short greetings ("Hello, D" = ~2090 bytes after 24k→8k
//               resample) in the buffer, and turnComplete on Gemini 3.1's bundled
//               event shape wasn't firing the flush reliably. Fix: removed the
//               byte gate (flush every chunk) + added per-turn telemetry +
//               TRANSCRIPT_WITHOUT_AUDIO / TURN_COMPLETE_WITHOUT_AUDIO anomaly
//               counters so the next regression surfaces in stats not in calls.
const PRIMARY_MODEL  = 'gemini-3.1-flash-live-preview';
const FALLBACK_MODEL = 'gemini-2.5-flash-native-audio-preview-12-2025';
// Back-compat alias — older imports and ops docs reference STABLE_MODEL.
const STABLE_MODEL   = PRIMARY_MODEL;

// Quarantine list: models we've observed returning bad data in production.
// Code-level baked-in list ∪ env-driven GEMINI_QUARANTINED_MODELS (comma
// separated). Either source forces a fallback. This lets ops quarantine a
// model from Railway env without a deploy when Google regresses upstream.
const CODE_QUARANTINED = new Set([
  // (formerly) 'gemini-3.1-flash-live-preview' — quarantined 2026-05-24 morning
  // after Google returned 2-byte empty audio; cleared at 16:07 IST same day
  // when Google fixed upstream and call 55fae1d9 had a full conversation.
]);
const ENV_QUARANTINED = new Set(
  (process.env.GEMINI_QUARANTINED_MODELS || '')
    .split(',').map((s) => s.trim()).filter(Boolean)
);
const QUARANTINED_MODELS = new Set([...CODE_QUARANTINED, ...ENV_QUARANTINED]);

function resolveModel() {
  const requested = process.env.GEMINI_MODEL || PRIMARY_MODEL;
  if (QUARANTINED_MODELS.has(requested)) {
    console.warn(`[LiveBase] GEMINI_MODEL "${requested}" is QUARANTINED → falling back to ${FALLBACK_MODEL}`);
    return FALLBACK_MODEL;
  }
  if (LIVE_ALLOWED_MODELS.includes(requested)) return requested;
  console.warn(`[LiveBase] GEMINI_MODEL "${requested}" not in allowlist → using ${PRIMARY_MODEL}`);
  return PRIMARY_MODEL;
}

const RESOLVED_MODEL = resolveModel();

// ─── IST date helpers ─────────────────────────────────────────────────────────

function getIstDates() {
  const opts = { timeZone: 'Asia/Kolkata', weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' };
  const isoOpts = { timeZone: 'Asia/Kolkata', year: 'numeric', month: '2-digit', day: '2-digit' };
  const fmt  = (d) => new Intl.DateTimeFormat('en-IN', opts).format(d);
  const iso  = (d) => new Intl.DateTimeFormat('en-CA', isoOpts).format(d);
  const now  = new Date();
  const tmrw = new Date(now.getTime() + 86400000);
  return {
    todayLong:    fmt(now),  todayISO:    iso(now),
    tomorrowLong: fmt(tmrw), tomorrowISO: iso(tmrw),
  };
}

// ─── Audio resampler ──────────────────────────────────────────────────────────

function resample(buffer, srcHz, dstHz) {
  if (srcHz === dstHz) return buffer;
  const samples    = buffer.length / 2;
  const ratio      = dstHz / srcHz;
  const outSamples = Math.round(samples * ratio);
  const out        = Buffer.alloc(outSamples * 2);
  for (let i = 0; i < outSamples; i++) {
    const srcPos = i / ratio;
    const srcIdx = Math.floor(srcPos);
    const frac   = srcPos - srcIdx;
    const s0     = buffer.readInt16LE(Math.min(srcIdx,     samples - 1) * 2);
    const s1     = buffer.readInt16LE(Math.min(srcIdx + 1, samples - 1) * 2);
    out.writeInt16LE(Math.max(-32768, Math.min(32767, Math.round(s0 + frac * (s1 - s0)))), i * 2);
  }
  return out;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function isWsOpen(ws) { return ws?.readyState === WebSocket.OPEN; }
function safeJson(str) { try { return JSON.parse(str); } catch { return null; } }

// ─── Catalogue → system-prompt block ──────────────────────────────────────────
//
// Pulls the business's menu / inventory / model list from `business_catalogue`
// (dashboard-side ingest writes it) and renders a compact, vertical-tailored
// text block that we append to Gemini Live's system_instruction.
//
// This is the only place the catalogue touches the voice path. We do NOT
// register a tool, do NOT change the model, do NOT add STT/TTS — Gemini Live
// just sees richer context in its single cached system prompt for the call.
//
// Salon vertical skips this — services already live on the business row.

const CATALOGUE_HEADINGS = {
  restaurant:  '# MENU (current prices)',
  supermarket: '# INVENTORY (current prices)',
  ev:          '# AVAILABLE MODELS (on-road prices)',
};

const CATALOGUE_RULES = {
  restaurant:
    'These are the items and prices CURRENTLY on the menu. When a caller asks "how much" or places an order, quote these numbers EXACTLY. Half/full and small/large are listed as separate items. If a caller asks for something NOT on this list, say "Let me check — can I take your number and call you back?" and capture the number. NEVER invent a price or guess. If the same dish appears twice with different prices, the cheaper one is half-portion.',
  supermarket:
    'These are the items and prices CURRENTLY in stock. Quote them EXACTLY when the caller asks. Prices are per unit (kg, packet, litre, dozen, etc.). For quantities, multiply: e.g. 2 kg atta at Rs 65/kg = Rs 130. If a caller asks for an item NOT on this list, say "We may not have that today — let me check and call you back" and capture their number. NEVER invent a price.',
  ev:
    'These are the models in stock at the dealership. If a model lists an explicit price in Rupees, quote it EXACTLY. If a model shows "price on enquiry — call sales team", do NOT improvise a number — instead say "Our sales team will share the exact on-road price on WhatsApp" and call capture_lead with query_type=price_enquiry. For EMI / financing / insurance / exchange offers, also escalate via capture_lead — never invent numbers. For test rides, use book_test_ride with the model name written here. The technical specs (range, top speed, battery) listed below ARE accurate and you should quote them confidently.',
};

function formatCatalogueForPrompt(catalogue, verticalName) {
  if (!catalogue || !Array.isArray(catalogue.items) || catalogue.items.length === 0) return '';

  // Drop disabled items (owner toggled off in /dashboard — out of stock, EOL etc.)
  const live = catalogue.items.filter((it) => !it.disabled);
  if (live.length === 0) return '';

  // Group by category for readability inside the prompt
  const byCat = new Map();
  for (const it of live) {
    const key = (it.category && String(it.category).trim()) || 'Other';
    if (!byCat.has(key)) byCat.set(key, []);
    byCat.get(key).push(it);
  }

  const lines = [];
  for (const [category, items] of byCat) {
    lines.push(`\n## ${category}`);
    for (const it of items) {
      const veg  = it.veg === true ? ' (veg)' : it.veg === false ? ' (non-veg)' : '';
      const unit = it.unit ? ` per ${it.unit}` : '';
      // Price may be null on EV dealerships (on-road price quoted by sales team).
      // Render a human-readable placeholder instead of "Rs null", which confuses
      // Gemini's safety layer and triggers the "I'm just a language model"
      // refusal — observed on all 3 client-dv-ather calls on 2026-05-20.
      const hasPrice = it.price !== null && it.price !== undefined && it.price !== '';
      const priceStr = hasPrice ? `Rs ${it.price}` : 'price on enquiry — call sales team';
      // For EV, append the technical attrs the agent can quote confidently
      // (range, top speed, battery) PLUS high-level features (Magic Twist,
      // Warp+ mode, AtherStack Pro etc.) so the model can talk fluently
      // about what makes each variant different — not just numbers.
      let attrStr = '';
      let featStr = '';
      if (verticalName === 'ev' && it.attrs && typeof it.attrs === 'object') {
        const a = it.attrs;
        const parts = [];
        if (a.range_km_idc)   parts.push(`${a.range_km_idc} km IDC range`);
        if (a.top_speed_kmh)  parts.push(`${a.top_speed_kmh} km/h top speed`);
        if (a.battery_kwh)    parts.push(`${a.battery_kwh} kWh battery`);
        if (a.acceleration_0_40) parts.push(`0-40 in ${a.acceleration_0_40}`);
        if (a.display)        parts.push(a.display);
        if (parts.length) attrStr = ` — ${parts.join(', ')}`;

        // High-level technical features — what the salesperson tells you in
        // the first 30 seconds. Rendered as a second line, indented, so the
        // model treats it as a "highlights" bullet attached to the variant.
        if (Array.isArray(a.features) && a.features.length) {
          featStr = `\n    Highlights: ${a.features.join(' · ')}`;
        }
        // Pro Pack upgrade note (so the agent knows what the upgrade buys
        // without making it up).
        if (a.pro_pack_on_road_inr && a.pro_pack_extra_inr) {
          featStr += `\n    Pro Pack: +Rs ${a.pro_pack_extra_inr} on-road (extends range, longer warranty)`;
        }
      }
      lines.push(`- ${it.name} — ${priceStr}${attrStr}${unit}${veg}${featStr}`);
    }
  }

  const heading = CATALOGUE_HEADINGS[verticalName] || '# CATALOGUE (current prices)';
  const rules   = CATALOGUE_RULES[verticalName]
    || 'Quote prices EXACTLY. NEVER invent a price. If the caller asks for something not on this list, offer to call them back with details.';

  return `${heading}\n${rules}\n${lines.join('\n')}`;
}

const DEFAULT_BUSINESS_ID = process.env.DEFAULT_BUSINESS_ID || 'test-salon-01';
// Reduced 10min → 5min default. A single 10-min stuck call burns ~151 credits
// (~Rs 22.65) and an unattended timeout costs more than 50 normal calls. The
// silence watchdog below will end most idle calls in 30s anyway; this cap is
// the last-resort upper bound.
const MAX_CALL_MS         = Number(process.env.MAX_CALL_DURATION_MS || 5 * 60 * 1000);
// Idle-silence watchdog: if no caller speech (no `inputTranscription` event)
// is received within this window after Gemini setupComplete, end the call
// with `end_reason=silence_timeout`. Caller still hears bot greeting before
// the line drops, and credit waste is capped at ~5 vs the previous 151.
// Reset on every transcribed caller utterance — so as long as the caller is
// actually talking, the watchdog keeps deferring.
const SILENCE_TIMEOUT_MS  = Number(process.env.SILENCE_TIMEOUT_MS    || 30 * 1000);

// ─── runLiveCall — core engine used by every vertical ────────────────────────

/**
 * @param {WebSocket}  ws            Vobiz WebSocket
 * @param {string}     callId
 * @param {string|null} callerPhone
 * @param {string|null} did
 * @param {Object|null} business     Pre-resolved business config (optional — router passes it)
 * @param {Object}     vertical      { buildPrompt(business, dates), tools[], onToolCall(fc, session) → { response, booking } }
 * @param {Object}     [opts]
 * @param {Object}     [opts.prewarmEntry]      LEGACY (pre-2026-06-07): a fully-
 *                                              attached warm entry from
 *                                              warmSessionRegistry. Kept for
 *                                              backward compat. New callers
 *                                              should use prewarmSessionId.
 * @param {string|null}[opts.prewarmSessionId]  NEW (2026-06-07 ChatGPT QA fix):
 *                                              the sessionId of a warm entry
 *                                              that has NOT YET been attached.
 *                                              ensureGeminiConnected will claim
 *                                              it lazily, AFTER Vobiz event:start
 *                                              fires (spans.mediaUpAt set).
 *                                              Eliminates the prewarm-attach
 *                                              race that closed Vobiz WS in 1-4ms.
 */
async function runLiveCall(ws, callId, callerPhone, did, business, vertical, opts = {}) {
  const { buildPrompt, tools, onToolCall } = vertical;
  // prewarmEntry is now `let` so claimWarmSessionAfterStart() can set it.
  let prewarmEntry        = opts.prewarmEntry || null;
  const prewarmSessionId  = opts.prewarmSessionId || null;

  // Resolve business if not pre-loaded
  if (!business) {
    const bizId = did ? ((await getBusinessByDID(did)) || DEFAULT_BUSINESS_ID) : DEFAULT_BUSINESS_ID;
    business    = await getBusinessConfig(bizId);
  }

  const session = {
    callId,
    businessId:    business?.id || DEFAULT_BUSINESS_ID,
    business,
    callerPhone:   callerPhone || null,
    startTime:     Date.now(),
    endReason:     null,
    bookingResult: null,
    streamId:      null,
  };

  const conversationLog = [];
  let callEnded         = false;
  let geminiWs          = null;
  let geminiConnectPromise = null;  // dedup in-flight connect so 2 callers share 1 session
  let setupComplete     = false;
  let setupCompleteAt   = 0;
  let greetingPending   = false;
  let pendingBooking    = null;   // in-flight tool call promise
  let outputBuffer      = Buffer.alloc(0);
  let timeoutTimer      = null;
  let silenceTimer      = null;   // idle-silence watchdog (see SILENCE_TIMEOUT_MS)
  let firstBotAudioSent = false;  // diagnostic — flips on first sendToVobiz()
  let serverMsgsLogged  = 0;      // diagnostic — first 3 serverContent messages get dumped
  let serverContentSamples = [];  // diagnostic — first 2 raw serverContent shapes (persisted to calls.stats)
  // ── Per-turn + per-call audio telemetry (added 2026-05-24 evening). ─────
  // Per-turn counters reset on turnComplete; per-call totals roll up into
  // calls.stats. The whole point: separate "Gemini sent us audio" from
  // "we sent audio to telephony" so the next silent-call regression is
  // diagnosable from SQL without needing Railway log access.
  let turnAudioChunks       = 0;
  let turnAudioBytes        = 0;
  let turnAudioMimeTypes    = new Set();
  let turnHasTranscript     = false;
  let totalGeminiAudioChunks   = 0;
  let totalGeminiAudioBytes    = 0;
  let totalTelephonyChunksSent = 0;
  let totalTelephonyBytesSent  = 0;
  let firstAudioChunkMs        = null;  // ms from setupComplete to first inlineData chunk
  // ── Per-turn reply telemetry (2026-06-04 monitor agent ask) ────────────
  // The user's wording: "delays in speaking during the calls" — measure
  // callerStop → botFirstAudio ms for EVERY turn after turn 1. Distinct
  // from pickupToFirstAudioMs which is only the greeting turn.
  let callerLastTranscriptAt = 0;   // wall-clock when caller's last transcript chunk arrived this turn
  let perTurnReplyMsArr      = [];  // [{ turn: 2, replyMs: 850 }, ...] — capped at 20 entries
  let turnIndex              = 0;   // 0 = greeting turn, increments on each turnComplete
  // ── 2026-06-07 caller-first kickoff mode (Adukkala-pattern outbound) ─────
  // When vertical sets kickoffMode='caller-first', mirror inbound behavior:
  // do NOT send a synthetic greeting trigger on setupComplete. Instead let
  // VAD detect the caller's "Hello?" and Gemini will respond naturally to
  // their voice (same as inbound restaurant flow that hits 819ms). If the
  // caller stays silent for KICKOFF_FALLBACK_MS, force-send the trigger as
  // a safety net so the call doesn't dead-air forever.
  let kickoffFallbackTimer   = null;
  // 2026-06-08 (ChatGPT review): drop default from 1500ms to 1200ms,
  // documented Google sweet spot for telephony. Allow env override.
  const KICKOFF_FALLBACK_MS  = Number(process.env.KICKOFF_FALLBACK_MS || 1200);

  // ── 2026-06-08 SPAN TELEMETRY (per ChatGPT review) ──────────────────────
  // Stamp wall-clock at each milestone so we can prove WHERE latency comes
  // from across a call. Persisted as stats.spans for SQL diagnosis.
  const spans = {};                    // { mediaUpAt, setupCompleteAt, localVADFirstHitAt, ... }
  spans.runLiveCallStartAt = Date.now();

  // ── 2026-06-08 LOCAL VAD (per ChatGPT review) ───────────────────────────
  // Cancel kickoffFallbackTimer based on RMS energy of incoming PCM, NOT on
  // the first inputTranscription chunk (which is downstream of Gemini's own
  // VAD + ASR and arrives ~300-500ms late). This shaves the perceived
  // dead-air on caller-first calls when the customer says "Hello?".
  // Threshold tuned for 8 kHz LINEAR16 telephony PCM (signed 16-bit).
  let localVADTriggered = false;
  const VAD_RMS_THRESHOLD = Number(process.env.LOCAL_VAD_RMS_THRESHOLD || 600);  // signed 16-bit RMS
  const VAD_MIN_FRAMES    = Number(process.env.LOCAL_VAD_MIN_FRAMES || 2);       // need N consecutive hot frames
  let vadHotFrameStreak = 0;

  // ── 2026-06-08 (ChatGPT review P0 #1) — explicit fallback-vs-setup gate ─
  // The old code re-armed `greetingPending` if the fallback fired before
  // Gemini was ready, but `setupComplete` was unconditionally a no-op in
  // caller-first mode → silent dead-air. Use a dedicated flag instead.
  let fallbackExpiredBeforeSetup = false;

  // ── 2026-06-08 (ChatGPT review P0 #2) — pre-setup audio buffer ──────────
  // Customer's "Hello?" may arrive BEFORE Gemini setupComplete on cold-
  // connect calls. Old code dropped that audio. Now: buffer up to ~2s of
  // pre-setup linear PCM 8kHz, replay after setupComplete so Gemini sees
  // the actual utterance instead of an empty turn.
  const PRESETUP_AUDIO_MAX_BYTES = 8000 * 2 * 2;  // 2 sec @ 8kHz 16-bit
  let preSetupAudioBuffer = Buffer.alloc(0);

  function appendPreSetupAudio(pcm8k) {
    preSetupAudioBuffer = Buffer.concat([preSetupAudioBuffer, pcm8k]);
    if (preSetupAudioBuffer.length > PRESETUP_AUDIO_MAX_BYTES) {
      // Keep the most recent 2s — drop the oldest. Caller's "Hello?" almost
      // always arrives in the last second pre-setup.
      preSetupAudioBuffer = preSetupAudioBuffer.slice(-PRESETUP_AUDIO_MAX_BYTES);
    }
  }

  function flushPreSetupAudioToGemini() {
    if (!geminiWs || !setupComplete || preSetupAudioBuffer.length === 0) return;
    const ms = Math.round(preSetupAudioBuffer.length / 2 / 8);  // 8 bytes per ms at 8kHz 16-bit
    const up = resample(preSetupAudioBuffer, 8000, 16000);
    geminiWs.sendRealtimeInput({
      audio: { data: up.toString('base64'), mimeType: 'audio/pcm;rate=16000' },
    });
    console.log(`[Live:${vertical.name}] ${callId} — replayed ${ms}ms of pre-setup caller audio (${preSetupAudioBuffer.length} bytes pcm)`);
    spans.preSetupAudioReplayedAt = Date.now();
    spans.preSetupAudioReplayedMs = ms;
    preSetupAudioBuffer = Buffer.alloc(0);
  }

  function computePcmRms(pcmBuf) {
    if (!pcmBuf || pcmBuf.length < 2) return 0;
    let sumSq = 0;
    const n = pcmBuf.length / 2;
    for (let i = 0; i < n; i++) {
      const s = pcmBuf.readInt16LE(i * 2);
      sumSq += s * s;
    }
    return Math.sqrt(sumSq / n);
  }

  function onLocalVadSpeechOnset() {
    if (localVADTriggered || callEnded) return;
    localVADTriggered = true;
    spans.localVADFirstHitAt = Date.now();
    const sinceMediaUp = spans.mediaUpAt ? spans.localVADFirstHitAt - spans.mediaUpAt : null;
    console.log(`[Live:${vertical.name}] ${callId} — LOCAL VAD detected speech onset (${sinceMediaUp}ms after media-up)`);
    if (kickoffFallbackTimer) {
      clearTimeout(kickoffFallbackTimer);
      kickoffFallbackTimer = null;
      greetingPending = false;
      console.log(`[Live:${vertical.name}] ${callId} — fallback cancelled by local VAD (no synthetic kickoff)`);
    }
  }

  // ── 2026-06-08 ANSWER-ANCHORED WATCHDOG ─────────────────────────────────
  // Schedule the kickoff fallback timer at media-up (Vobiz 'start' event) NOT
  // at Gemini setupComplete. Per ChatGPT review: anchoring on setupComplete
  // means a silent callee can hear setup_time + 1.2s of dead air. Anchoring
  // on media-up bounds total dead-air to KICKOFF_FALLBACK_MS regardless of
  // Gemini setup time.
  //
  // The timer fires sendGreeting() which checks greetingPending + geminiWs —
  // if setupComplete hasn't fired yet, greetingPending is still true and the
  // existing setupComplete handler will fire the greeting then. So firing
  // the timer "early" is safe — sendGreeting is idempotent / no-op without
  // a live geminiWs.
  function armKickoffFallbackTimer() {
    if (kickoffFallbackTimer) return;
    if (vertical.kickoffMode !== 'caller-first') return;
    spans.fallbackArmedAt = Date.now();
    console.log(`[Live:${vertical.name}] ${callId} — kickoff fallback armed (${KICKOFF_FALLBACK_MS}ms) — anchored at media-up`);
    kickoffFallbackTimer = setTimeout(() => {
      if (!greetingPending || callEnded || localVADTriggered) return;
      spans.fallbackFiredAt = Date.now();
      const fromMediaUp = spans.mediaUpAt ? spans.fallbackFiredAt - spans.mediaUpAt : null;
      console.log(`[Live:${vertical.name}] ${callId} — KICKOFF FALLBACK fired (${fromMediaUp}ms after media-up, caller never spoke)`);
      // 2026-06-08 (ChatGPT review P0 #1): use the explicit flag, NOT a
      // re-armed greetingPending. The setupComplete handler reads this
      // flag and fires the greeting deterministically.
      if (geminiWs && setupComplete) {
        greetingPending = false;
        sendGreeting();
      } else {
        fallbackExpiredBeforeSetup = true;
        console.log(`[Live:${vertical.name}] ${callId} — fallback expired but Gemini not ready (setupComplete=${setupComplete}); setupComplete handler will fire greeting`);
      }
    }, KICKOFF_FALLBACK_MS);
  }
  // ── Sub-1s Voice Bridge telemetry (2026-06-04, Vineeth target) ──────────
  // Wall-clock from runLiveCall start (≈ Vobiz WSS open ≈ pickup) to the
  // first audio chunk emitted by Gemini. THIS is the number that maps to
  // caller-perceived silence. Target on warm hits: ≤ 1000 ms (ideal ≤ 800).
  let pickupToFirstAudioMs     = null;
  const anomalyCounts = { transcriptWithoutAudio: 0, turnCompleteWithoutAudio: 0 };

  // ── Single-flight Gemini connector ──────────────────────────────────────
  // The early-connect path (WS open) AND the Vobiz `start` handler AND the 3s
  // fallback all want a Gemini session. Without dedup, two of them firing
  // concurrently would open two parallel sessions, neither of which completes
  // cleanly, and Gemini stays silent. This helper guarantees ONE session
  // regardless of how many callers ask for it — late callers await the same
  // in-flight Promise.
  // ── Lazy warm-session claim (2026-06-07 ChatGPT QA fix) ────────────────
  // Called only after spans.mediaUpAt is set (event:start fired). Attaches
  // the warm entry by sessionId, marks prewarmEntry so the next call to
  // ensureGeminiConnected takes the PREWARM ATTACH branch below.
  function claimWarmSessionAfterStart() {
    if (prewarmEntry) return prewarmEntry;
    if (!prewarmSessionId) return null;
    const warmRegistry = require('./warmSessionRegistry');
    const entry = warmRegistry.attach(prewarmSessionId);
    if (entry && entry.gemini && !entry.geminiClosedPreAttach) {
      prewarmEntry = entry;
      const setupAge = entry.setupCompleteAt
        ? `${Date.now() - entry.setupCompleteAt}ms after setupComplete`
        : 'setupComplete NOT YET';
      console.log(`[Live:${vertical.name}] ${callId} — PREWARM CLAIMED AFTER START sessionId=${prewarmSessionId} (${setupAge})`);
      return entry;
    }
    if (entry) {
      console.warn(`[Live:${vertical.name}] ${callId} — PREWARM ENTRY DEAD sessionId=${prewarmSessionId} (geminiClosedPreAttach=${!!entry.geminiClosedPreAttach}) — cold connect`);
      try { entry.gemini?.close?.(); } catch (_) {}
    } else {
      console.log(`[Live:${vertical.name}] ${callId} — no warm entry for sessionId=${prewarmSessionId} — cold connect`);
    }
    return null;
  }

  function ensureGeminiConnected() {
    if (geminiWs)              return Promise.resolve(geminiWs);
    if (geminiConnectPromise)  return geminiConnectPromise;

    // 2026-06-07 (ChatGPT QA — defer-warm-attach fix):
    // If the caller passed a prewarmSessionId (NEW path) but Vobiz has not
    // yet sent event:start (spans.mediaUpAt unset), DEFER. Attaching during
    // the pre-event:start window races with Vobiz's stream handshake and
    // gets the WS torn down in 1-4ms. The event:start handler in the
    // Vobiz event loop calls ensureGeminiConnected() AGAIN after stamping
    // mediaUpAt — that's when we actually attach.
    if (prewarmSessionId && !prewarmEntry && !spans.mediaUpAt) {
      console.log(`[Live:${vertical.name}] ${callId} — ensureGeminiConnected DEFERRED (prewarm sessionId=${prewarmSessionId}, awaiting Vobiz event:start)`);
      return Promise.resolve(null);
    }

    // Lazily claim warm session — only safe after event:start armed mediaUpAt.
    if (prewarmSessionId && !prewarmEntry && spans.mediaUpAt) {
      claimWarmSessionAfterStart();
    }

    // ── PREWARM ATTACH PATH (Sub-1s Voice Bridge, gated on LIVE_PREWARM) ──
    // If the dial path opened a Gemini Live session during the ringing
    // window, claim it here instead of opening a cold one. The warm entry
    // already has its message buffer; installListener replays it through
    // onGeminiMessage, which will surface the buffered `setupComplete`
    // event and fire sendGreeting() immediately — without the ~750 ms
    // cold-connect handshake.
    //
    // Fallback rule (Vineeth guard-rail #4): if the warm entry is missing
    // its gemini handle (connect failed) OR the underlying socket closed
    // pre-attach, drop to cold connect. Correctness never depends on warm.
    if (prewarmEntry && prewarmEntry.gemini && !prewarmEntry.geminiClosedPreAttach) {
      try {
        const { installListener } = require('./warmSessionRegistry');
        geminiWs = prewarmEntry.gemini;
        greetingPending = true;
        const replayed = installListener(prewarmEntry, onGeminiMessage);
        const setupAge = prewarmEntry.setupCompleteAt
          ? (session.startTime - prewarmEntry.setupCompleteAt)
          : null;
        console.log(`[Live:${vertical.name}] ${callId} — PREWARM ATTACH replayed=${replayed} setupComplete=${prewarmEntry.setupComplete} setupAgeMs=${setupAge}`);
        return Promise.resolve(geminiWs);
      } catch (err) {
        console.warn(`[Live:${vertical.name}] ${callId} — prewarm attach failed (${err.message}); falling back to cold connect`);
        geminiWs = null;
      }
    }

    // ── COLD CONNECT PATH (default + fallback) ───────────────────────────
    geminiConnectPromise = connectGemini()
      .then((ws) => { geminiWs = ws; greetingPending = true; return ws; })
      .catch((err) => { geminiConnectPromise = null; throw err; });
    return geminiConnectPromise;
  }

  const ai    = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
  const dates = getIstDates();

  // ── Idle-silence watchdog ───────────────────────────────────────────────
  // Started when Gemini setupComplete fires (caller is past greeting). Reset
  // on every caller transcription event. If no caller speech in the window,
  // end the call cleanly instead of letting the 5-min hard timeout fire.

  function resetSilenceWatchdog() {
    if (callEnded) return;
    if (silenceTimer) clearTimeout(silenceTimer);
    silenceTimer = setTimeout(() => {
      if (callEnded) return;
      console.warn(`[Live:${vertical.name}] ${callId} — silence watchdog fired (no caller speech for ${SILENCE_TIMEOUT_MS}ms)`);
      endCallOnce('silence_timeout').catch(() => {});
    }, SILENCE_TIMEOUT_MS);
  }

  // ── Send audio back to Vobiz ────────────────────────────────────────────

  function sendToVobiz(buf) {
    if (!isWsOpen(ws) || !buf?.length) return;
    // 2026-06-07: when STREAM_CODEC=mulaw, encode 8 kHz linear PCM to μ-law
    // (halves bytes on the wire) and tag the contentType so Vobiz transcodes
    // for the carrier. This matches the codec Vobiz already uses on outbound
    // PSTN legs (PCMU per call f96f2401 logs), unblocking the bidirectional
    // Stream that was silently failing on the customer→bot direction.
    let payload, contentType;
    if (STREAM_CODEC === 'mulaw') {
      const u = ulaw.encodeLinearToMuLawBuffer(buf);
      payload = u.toString('base64');
      contentType = 'audio/x-mulaw';
    } else {
      payload = buf.toString('base64');
      contentType = 'audio/x-l16';
    }
    if (!firstBotAudioSent) {
      firstBotAudioSent = true;
      const tts = setupCompleteAt ? `${Date.now() - setupCompleteAt}ms after setupComplete` : 'pre-setup';
      console.log(`[Live:${vertical.name}] ${callId} — first bot audio sent to Vobiz (${buf.length} bytes pcm → ${contentType}, ${tts})`);
    }
    totalTelephonyChunksSent += 1;
    totalTelephonyBytesSent  += buf.length;
    ws.send(JSON.stringify({
      event: 'playAudio',
      media: { contentType, sampleRate: 8000, payload },
      ...(session.streamId ? { streamId: session.streamId } : {}),
    }));
  }

  function flushAudio() {
    if (!outputBuffer.length) return;
    sendToVobiz(outputBuffer);
    outputBuffer = Buffer.alloc(0);
  }

  // ── Send greeting trigger once Gemini is ready ──────────────────────────

  function sendGreeting() {
    if (!geminiWs || callEnded) return;
    try {
      // ChatGPT-recommended trigger pattern (2026-06-04): instead of the
      // generic '[call started]' which forces Gemini to plan a full first
      // turn from the system prompt, the vertical can provide an exact
      // first-utterance trigger ("SAY_NOW_EXACTLY: ..."). Gemini's planning
      // cost drops from ~2s to ~300-500ms because there's nothing to compose.
      // Falls back to '[call started]' for verticals that don't define one.
      const triggerText =
        (typeof vertical.kickoffTrigger === 'function'
          ? vertical.kickoffTrigger(business)
          : null) || '[call started]';
      geminiWs.sendRealtimeInput({ text: triggerText });
      console.log(`[Live:${vertical.name}] ${callId} — greeting sent (${triggerText.startsWith('SAY_NOW_EXACTLY') ? 'forced' : 'generic'})`);
    } catch (err) {
      console.error(`[Live:${vertical.name}] ${callId} — greeting error: ${err.message}`);
    }
  }

  // ── Handle messages from Gemini ─────────────────────────────────────────

  function onGeminiMessage(msg) {
    // Setup complete — safe to send greeting
    if (msg.setupComplete !== undefined) {
      setupComplete = true;
      setupCompleteAt = Date.now();
      spans.setupCompleteAt = setupCompleteAt;
      console.log(`[Live:${vertical.name}] ${callId} — setup complete`);
      // ── caller-first kickoff (per ChatGPT review 2026-06-08) ─────────────
      // The fallback timer is NOT armed here anymore — anchored at media-up.
      // Three branches to handle on setupComplete in caller-first mode:
      //
      //   (a) Local VAD already fired → caller spoke first. If we buffered
      //       their audio, REPLAY it now so Gemini sees the actual "Hello?"
      //       turn instead of an empty one. VAD already cancelled the
      //       fallback, so we don't greet — Gemini's reply IS the response
      //       to the replayed caller turn.
      //
      //   (b) Fallback timer already expired (caller stayed silent) →
      //       fire the greeting NOW. fallbackExpiredBeforeSetup flag, not
      //       a re-armed greetingPending (which was the previous race bug).
      //
      //   (c) Neither happened yet → wait. VAD or armed fallback will
      //       resolve this. setupComplete is just "Gemini ready" half.
      if (vertical.kickoffMode === 'caller-first') {
        if (localVADTriggered && preSetupAudioBuffer.length) {
          // (a) replay buffered audio so Gemini hears the caller
          flushPreSetupAudioToGemini();
          greetingPending = false;
        } else if (fallbackExpiredBeforeSetup && !localVADTriggered) {
          // (b) silent caller, timer already expired pre-setup → greet now
          fallbackExpiredBeforeSetup = false;
          greetingPending = false;
          console.log(`[Live:${vertical.name}] ${callId} — firing deferred fallback greeting (timer expired before Gemini was ready)`);
          sendGreeting();
        } else {
          // (c) wait for VAD or the still-running timer
          console.log(`[Live:${vertical.name}] ${callId} — caller-first mode: greeting deferred to VAD or armed fallback`);
        }
      } else if (greetingPending) {
        // Immediate-kickoff (legacy) verticals — fire as before.
        greetingPending = false;
        sendGreeting();
      }
      // Start silence watchdog. Greeting (~3-5s) + caller think time fits in 30s.
      // Watchdog is reset on each caller transcription event below.
      resetSilenceWatchdog();
      return;
    }

    // ── Diagnostic (2026-05-24 P0): audio_sent=false on 100% of today's calls.
    // Dump the first 3 serverContent messages so we can see the wire format
    // Gemini is actually returning. Cheap to keep in production.
    // Also persist the first 2 dumps into stats so we can read them via SQL
    // when Railway logs aren't available.
    if (msg.serverContent && serverMsgsLogged < 3) {
      serverMsgsLogged++;
      const sample = JSON.stringify(msg, (_, v) =>
        typeof v === 'string' && v.length > 80 ? `<${v.length}-char-str>` : v
      ).slice(0, 800);
      console.log(`[Live:${vertical.name}] ${callId} serverContent#${serverMsgsLogged}: ${sample}`);
      if (serverContentSamples.length < 2) serverContentSamples.push(sample);
    }

    // Audio chunks. Gemini Live normally returns audio under
    // serverContent.modelTurn.parts[].inlineData.{data,mimeType}. But the
    // wire format on the `-preview` models has shifted before. We scan a
    // few candidate locations defensively, and log when we find audio in a
    // non-canonical place so we know to fix the parser.
    let audioBytes = 0;
    const harvestAudio = (b64, mime, where) => {
      if (!b64) return;
      const raw     = Buffer.from(b64, 'base64');
      const srcRate = parseInt(((mime || 'audio/pcm;rate=24000').match(/rate=(\d+)/) || [])[1] || '24000', 10);
      outputBuffer  = Buffer.concat([outputBuffer, resample(raw, srcRate, 8000)]);
      audioBytes   += raw.length;
      // Telemetry — count Gemini-side audio receipt independently of telephony delivery
      turnAudioChunks        += 1;
      turnAudioBytes         += raw.length;
      totalGeminiAudioChunks += 1;
      totalGeminiAudioBytes  += raw.length;
      if (mime) turnAudioMimeTypes.add(mime);
      if (firstAudioChunkMs === null && setupCompleteAt) {
        firstAudioChunkMs = Date.now() - setupCompleteAt;
        // Wall-clock pickup→first-audio (Sub-1s Voice Bridge metric).
        // session.startTime ≈ Vobiz WSS open, which is the earliest moment
        // we can know about pickup. Real caller-perceived silence may be
        // slightly larger because Vobiz takes another ~100 ms to send the
        // playAudio frame onward, but this is the right comparator across
        // warm vs cold runs.
        pickupToFirstAudioMs = Date.now() - session.startTime;
      }
      // Per-turn caller-stop → bot-first-audio (2026-06-04 monitor ask).
      // Only fires from turn 1 onwards (turn 0 is the greeting, covered by
      // pickupToFirstAudioMs). callerLastTranscriptAt is set when the
      // caller's transcription chunk arrives, and re-armed to 0 after we
      // capture the reply. turnAudioChunks === 1 right now means this is
      // the first audio chunk of the current bot turn.
      if (callerLastTranscriptAt && turnIndex > 0 && turnAudioChunks === 1) {
        const replyMs = Date.now() - callerLastTranscriptAt;
        if (perTurnReplyMsArr.length < 20) {
          perTurnReplyMsArr.push({ turn: turnIndex, replyMs });
        }
        callerLastTranscriptAt = 0;
      }
      if (!firstBotAudioSent && where !== 'inlineData') {
        console.warn(`[Live:${vertical.name}] ${callId} — audio arrived via ${where} (non-canonical); parser may need updating`);
      }
    };

    const parts = msg.serverContent?.modelTurn?.parts;
    if (parts) {
      for (const part of parts) {
        // Canonical Gemini Live audio path
        if (part.inlineData?.data) {
          harvestAudio(part.inlineData.data, part.inlineData.mimeType, 'inlineData');
        }
        // Snake-case variant (raw API payload occasionally arrives unsmoothed)
        if (part.inline_data?.data) {
          harvestAudio(part.inline_data.data, part.inline_data.mime_type, 'inline_data');
        }
        // Hypothetical newer shapes — harmless if absent
        if (part.fileData?.fileUri && /^data:audio/.test(part.fileData.fileUri)) {
          const b64 = part.fileData.fileUri.split(',')[1];
          harvestAudio(b64, part.fileData.mimeType, 'fileData.dataUri');
        }
      }
      // 2026-05-24 evening fix: removed `if (outputBuffer.length >= 3200)` gate.
      // The gate held short greetings (~2090 bytes after 24k→8k resample) in
      // the buffer when Gemini 3.1's bundled event shape didn't trigger our
      // turnComplete flush. Flushing every audio event guarantees the caller
      // hears the bot even on single-chunk turns. Vobiz tolerates small frames
      // (it has its own jitter buffer); the latency cost is negligible.
      if (outputBuffer.length) flushAudio();
    }

    // Top-level audio fields some Live API surfaces have used in past
    // releases. Cheap to probe.
    const topAudio = msg.serverContent?.outputAudio || msg.outputAudio || msg.realtimeOutput?.audio;
    if (topAudio?.data) {
      harvestAudio(topAudio.data, topAudio.mimeType, 'top-level.outputAudio');
      if (outputBuffer.length) flushAudio();
    }

    // Transcription logging.
    // Per-utterance lines are gated behind LOG_TRANSCRIPTS=true to keep Railway
    // logs lean at scale (transcripts are already persisted to calls.transcript
    // in Supabase — Railway stdout would just be a duplicate). Flip the env
    // var on temporarily when you need to live-debug a single deploy.
    const LOG_TRANSCRIPTS = process.env.LOG_TRANSCRIPTS === 'true';
    const callerText = msg.serverContent?.inputTranscription?.text;
    const botText    = msg.serverContent?.outputTranscription?.text;
    if (callerText) {
      conversationLog.push({ ts: Date.now(), role: 'caller', text: callerText });
      if (LOG_TRANSCRIPTS) console.log(`[Live:${vertical.name}] ${callId} CALLER: "${callerText}"`);
      // Span telemetry: first Gemini inputTranscription chunk. Compare to
      // localVADFirstHitAt in SQL to validate how much earlier local VAD
      // fires vs Gemini's own transcription (typically 200-500ms).
      if (!spans.firstInputTranscriptionAt) spans.firstInputTranscriptionAt = Date.now();
      // ── caller-first kickoff: caller spoke first, cancel the fallback ────
      // (Backup path — local VAD usually catches it earlier per the 2026-06-08
      // refactor. Keeping this as a belt-and-suspenders cancel.)
      if (kickoffFallbackTimer) {
        clearTimeout(kickoffFallbackTimer);
        kickoffFallbackTimer = null;
        greetingPending = false;
        console.log(`[Live:${vertical.name}] ${callId} — fallback cancelled by inputTranscription (local VAD didn't catch it first)`);
      }
      // Caller is actually talking → defer the silence watchdog.
      resetSilenceWatchdog();
      // Stamp wall-clock of the latest caller transcript chunk this turn.
      // The harvestAudio path reads this on the next bot audio chunk and
      // records callerStop → botFirstAudio ms into perTurnReplyMsArr.
      // Overwrites on every chunk so the final value lines up with the
      // caller's actual end-of-speech (VAD silenceDurationMs after).
      callerLastTranscriptAt = Date.now();
    }
    if (botText) {
      conversationLog.push({ ts: Date.now(), role: 'bot', text: botText });
      if (LOG_TRANSCRIPTS) console.log(`[Live:${vertical.name}] ${callId}   BOT: "${botText}"`);
      turnHasTranscript = true;
    }

    // turnComplete — run AFTER transcription so anomaly detection sees both
    // audio counters and transcript-presence for the just-completed turn.
    if (msg.serverContent?.turnComplete) {
      flushAudio();
      // Silent-turn anomalies — the patterns that cost us 2026-05-24:
      //   TRANSCRIPT_WITHOUT_AUDIO  → Gemini sent text but no audio (parser or model regression)
      //   TURN_COMPLETE_WITHOUT_AUDIO → empty turn (no text, no audio) — usually setup glitch
      if (turnHasTranscript && turnAudioBytes === 0) {
        anomalyCounts.transcriptWithoutAudio += 1;
        console.warn(`[Live:${vertical.name}] ${callId} ⚠ TRANSCRIPT_WITHOUT_AUDIO — text="${botText?.slice(0, 60) || ''}" (turn #${anomalyCounts.transcriptWithoutAudio})`);
      } else if (!turnHasTranscript && turnAudioBytes === 0) {
        anomalyCounts.turnCompleteWithoutAudio += 1;
        console.warn(`[Live:${vertical.name}] ${callId} ⚠ TURN_COMPLETE_WITHOUT_AUDIO — empty turn (no text, no audio)`);
      }
      // Reset per-turn counters
      turnAudioChunks    = 0;
      turnAudioBytes     = 0;
      turnHasTranscript  = false;
      turnAudioMimeTypes = new Set();
      // Advance turn counter. turnIndex===0 is the greeting (no caller
      // transcript before it); turnIndex===1 is the first caller→bot turn.
      // perTurnReplyMs logic in harvestAudio gates on turnIndex > 0.
      turnIndex += 1;
    }

    // Tool calls
    if (msg.toolCall) {
      for (const fc of (msg.toolCall.functionCalls || [])) {
        console.log(`[Live:${vertical.name}] ${callId} tool: ${fc.name}`, fc.args);
        pendingBooking = executeToolCall(fc).catch((err) =>
          console.error(`[Live:${vertical.name}] ${callId} tool error: ${err.message}`)
        );
      }
    }
    if (msg.toolCallCancellation) {
      console.warn(`[Live:${vertical.name}] ${callId} tool cancelled: ${(msg.toolCallCancellation.ids || []).join(',')}`);
    }
  }

  async function executeToolCall(fc) {
    const { response, booking, endCall } = await onToolCall(fc, session);
    if (booking) session.bookingResult = booking;

    if (geminiWs && !callEnded) {
      try {
        geminiWs.sendToolResponse({
          functionResponses: [{ id: fc.id, name: fc.name, response: { output: response } }],
        });
      } catch (err) {
        console.error(`[Live:${vertical.name}] ${callId} sendToolResponse error: ${err.message}`);
      }
    }

    // ── Graceful end-of-call signal ────────────────────────────────────
    // A tool handler can return endCall: true (or a string reason) to ask
    // the engine to wrap up the call after the agent's final audio plays.
    // Used by end_call_politely / mark_opted_out for outbound, and any
    // tool that knows "this conversation is done". 3-second delay lets
    // the goodbye line finish streaming to the caller before we hang up.
    if (endCall && !callEnded) {
      const reason = typeof endCall === 'string' ? endCall : 'agent_ended';
      console.log(`[Live:${vertical.name}] ${callId} — tool ${fc.name} requested end_call → closing in 3s (reason=${reason})`);
      setTimeout(() => endCallOnce(reason).catch(() => {}), 3000);
    }
  }

  // ── Connect to Gemini Live ──────────────────────────────────────────────

  async function connectGemini() {
    const baseSystemPrompt = buildPrompt(business, dates);

    // Catalogue is fetched only for verticals that have one (restaurant /
    // supermarket / ev). Salon services already live on the business row.
    // Failure here is non-fatal — Gemini falls back to the persona prompt
    // and answers generically. We log so it's visible if the catalogue
    // is silently going missing.
    let catalogueBlock = '';
    if (vertical.name !== 'salon' && business?.id) {
      try {
        const cat = await getCatalogue(business.id);
        catalogueBlock = formatCatalogueForPrompt(cat, vertical.name);
        if (catalogueBlock) {
          const liveItems = cat.items.filter((it) => !it.disabled).length;
          console.log(`[Live:${vertical.name}] ${callId} — catalogue loaded (${liveItems}/${cat.item_count} live items from ${cat.source_type || 'manual'})`);
        } else {
          console.log(`[Live:${vertical.name}] ${callId} — no catalogue for business ${business.id} (owner hasn't uploaded one yet)`);
        }
      } catch (err) {
        console.warn(`[Live:${vertical.name}] ${callId} — catalogue fetch failed (continuing without it): ${err.message}`);
      }
    }

    const systemPrompt = catalogueBlock ? `${baseSystemPrompt}\n\n${catalogueBlock}` : baseSystemPrompt;

    // Voice — per-business via businesses.voice_name (migration 010, see
    // [[project_vani_voice_per_business]]). Falls back to Aoede if the column
    // is null or the value isn't in the @google/genai allowlist. Keep the
    // fallback identical to historical default so existing salons don't change.
    const voiceName = business?.voice_name || 'Aoede';

    // ── ROLLBACK 2026-05-24 ─────────────────────────────────────────────────
    // Tweak 1/4 of Divya's lag plan (commit 6e77b5d4 — per-vertical
    // realtimeInputConfig + maxOutputTokens) caused 4/4 silent calls after
    // deploy. Same failure mode as this morning: setupComplete fires,
    // outputTranscription text arrives, but inlineData stays empty/2-byte.
    //
    // This proves the morning "Google regression" was almost certainly the
    // same config interaction, not a Google-side bug. Some combination of
    // generationConfig.maxOutputTokens AND/OR realtimeInputConfig provokes
    // the Live API to emit transcription-only mode on at least the
    // 3.1-flash-live-preview model.
    //
    // Reverting to the minimal-known-working config. Next attempt will add
    // exactly ONE knob at a time and test it live before adding the next,
    // so we can isolate the offender. Tarun's static QA passed everything,
    // so the failure is purely runtime/API behaviour — has to be diagnosed
    // empirically.
    // ───────────────────────────────────────────────────────────────────────

    // ── VAD tuning (2026-06-05 update — inter-turn lag research) ───────────
    // Background research (full notes in CHATGPT-LATENCY-BRIEF.md):
    //   * Google's official SDK example for AAD uses silenceDurationMs=100,
    //     prefixPaddingMs=20. The "500-800ms recommended" band in the docs
    //     is for desktop mic users with multi-clause utterances. For Indian
    //     English telephony short replies ("haan", "yes", "tomorrow 5pm"),
    //     anything > 400ms is perceptible dead air.
    //   * `endOfSpeechSensitivity: END_SENSITIVITY_HIGH` is the
    //     under-utilized lever — Google's docs call it out as the way to
    //     "end the turn sooner". Current config leaves it at default.
    //   * `startOfSpeechSensitivity: START_SENSITIVITY_HIGH` for the same
    //     reason on the caller-starts-speaking side.
    //
    // SAFETY: defaults are UNCHANGED (silence=600, sensitivities unset)
    // so inbound services running without env overrides are untouched.
    // Outbound service flips Railway env to claim the latency win.
    //
    // Env knobs:
    //   LIVE_VAD_DISABLED=true          → drop realtimeInputConfig entirely
    //   LIVE_SILENCE_MS=300             → faster end-of-speech detection
    //                                     (research recommended for telephony)
    //   LIVE_PREFIX_PAD_MS=100          → leave at 100
    //   LIVE_END_SENS=END_SENSITIVITY_HIGH   → end turn sooner (recommended)
    //   LIVE_END_SENS=END_SENSITIVITY_LOW    → wait longer (noisy env)
    //   LIVE_START_SENS=START_SENSITIVITY_HIGH → faster caller-onset detection
    //
    // Rollback to today's behavior: unset all four LIVE_* env vars.
    const SILENCE_MS    = Number(process.env.LIVE_SILENCE_MS    || 600);
    const PREFIX_PAD_MS = Number(process.env.LIVE_PREFIX_PAD_MS || 100);
    const END_SENS      = process.env.LIVE_END_SENS   || null;  // unset = SDK default
    const START_SENS    = process.env.LIVE_START_SENS || null;  // unset = SDK default
    const aad = {
      silenceDurationMs: SILENCE_MS,
      prefixPaddingMs:   PREFIX_PAD_MS,
    };
    // Only add sensitivity fields if env opts in — keeps the wire payload
    // identical to historical config on services that haven't migrated.
    if (END_SENS)   aad.endOfSpeechSensitivity   = END_SENS;
    if (START_SENS) aad.startOfSpeechSensitivity = START_SENS;
    const vadConfig = process.env.LIVE_VAD_DISABLED === 'true' ? {} : {
      realtimeInputConfig: { automaticActivityDetection: aad },
    };

    const liveSession  = await ai.live.connect({
      model: RESOLVED_MODEL,
      config: {
        responseModalities:       [Modality.AUDIO],
        systemInstruction:        { parts: [{ text: systemPrompt }] },
        speechConfig:             { voiceConfig: { prebuiltVoiceConfig: { voiceName } } },
        inputAudioTranscription:  {},
        outputAudioTranscription: {},
        tools:                    [{ functionDeclarations: tools }],
        // 2026-06-04: kill the 5-7 second "thinking" pause before first audio.
        // gemini-2.5-flash-native-audio-preview-12-2025 has reasoning enabled
        // by default — it emits a thought {"thought":true} chunk before any
        // audio. Telemetry from call 37b1fcf9: firstAudioChunkMs=7421 with a
        // 428-char thought block in firstServerContentSamples[0]. For a phone
        // call this is unusable. thinkingBudget=0 disables the reasoning step
        // so audio starts within ~300-500ms (Gemini's native first-chunk latency).
        // Override via env LIVE_THINKING_BUDGET if a vertical ever needs it.
        thinkingConfig: { thinkingBudget: Number(process.env.LIVE_THINKING_BUDGET || 0) },
        ...vadConfig,
      },
      callbacks: {
        onopen()    { console.log(`[Live:${vertical.name}] ${callId} — Gemini open`); },
        onmessage(msg) { if (!callEnded) { try { onGeminiMessage(msg); } catch (e) { console.error(`[Live:${vertical.name}] msg error: ${e.message}`); } } },
        onerror(err)   { console.error(`[Live:${vertical.name}] ${callId} Gemini error: ${err.message || JSON.stringify(err)}`); },
        onclose(ev)    {
          // Distinguish a hard handshake/auth failure (no audio ever sent) from
          // a normal mid-call close. The former is the silence pattern flagged
          // in the 2026-05-12 19:07Z health check: 2/3 calls dying at ~3.1s with
          // empty transcripts. Surface it as a distinct end_reason so the
          // health monitor can alert specifically on it.
          const code = ev?.code;
          const sinceSetup = setupCompleteAt ? Date.now() - setupCompleteAt : null;
          const silent = !firstBotAudioSent;
          const reason = silent ? 'gemini_closed_silent' : 'gemini_closed';
          console.log(`[Live:${vertical.name}] ${callId} Gemini closed (code=${code}, sinceSetup=${sinceSetup}ms, silent=${silent})`);
          if (silent) {
            console.error(`[Live:${vertical.name}] ${callId} — Gemini closed BEFORE first bot audio. Caller heard silence. Likely model/auth/quota error — check Railway logs for GEMINI_API_KEY, model "${RESOLVED_MODEL}", and quota.`);
          }
          if (!callEnded) endCallOnce(reason);
        },
      },
    });
    return liveSession;
  }

  // ── End call ────────────────────────────────────────────────────────────

  async function endCallOnce(reason) {
    if (callEnded) return;
    callEnded = true;
    if (timeoutTimer) { clearTimeout(timeoutTimer); timeoutTimer = null; }
    if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null; }
    session.endReason = reason;

    // Wait for any in-flight booking to finish (up to 5s) before logging
    if (pendingBooking) {
      await Promise.race([pendingBooking, new Promise(r => setTimeout(r, 5000))]).catch(() => {});
    }

    const durationMs = Date.now() - session.startTime;
    console.log(`[Live:${vertical.name}] ${callId} ended — ${reason}, ${Math.round(durationMs / 1000)}s`);

    try { geminiWs?.close?.(); } catch (_) {}

    try {
      // direction is inferred from the vertical name. Outbound verticals
      // are suffixed -outbound (ev-outbound, sales-outbound). Everything else
      // is inbound. 2026-06-05: was previously left implicit, which meant
      // the DB default ('inbound') was applied to every row including
      // outbound — telemetry queries by direction were useless.
      // 2026-06-08 (ChatGPT review): 'ev-outbound-fast' didn't match .endsWith('-outbound')
      // and was being logged as direction='inbound', corrupting A/B telemetry.
      // Prefer vertical.direction if set; else any name containing 'outbound'.
      const dir = vertical?.direction
        || (((vertical?.name || '').toLowerCase().includes('outbound')) ? 'outbound' : 'inbound');
      await logCall({
        callId,
        businessId:  session.businessId,
        callerPhone: session.callerPhone,
        durationMs,
        endReason:   reason,
        bookingId:   session.bookingResult?.id || null,
        transcript:  conversationLog,
        direction:   dir,
        stats: {
          vertical:                  vertical.name,
          model:                     RESOLVED_MODEL,
          setupCompleted:            setupComplete,
          firstBotAudioSent,
          msToSetup:                 setupCompleteAt ? setupCompleteAt - session.startTime : null,
          // Diagnostic (2026-05-24): captures first 2 raw serverContent shapes
          // so we can reverse-engineer Gemini's current wire format via SQL,
          // without needing Railway log access. Remove once audio is restored.
          firstServerContentSamples: serverContentSamples,
          // Audio pipeline telemetry (added 2026-05-24 evening). These are the
          // counters that make the next silent-call regression diagnosable
          // from a single SQL query rather than needing a fresh diagnostic
          // commit. Compare geminiAudioBytes (what Google gave us) to
          // telephonyBytesSent (what Vobiz received). Divergence = our bug.
          geminiAudioChunks:         totalGeminiAudioChunks,
          geminiAudioBytes:          totalGeminiAudioBytes,
          telephonyChunksSent:       totalTelephonyChunksSent,
          telephonyBytesSent:        totalTelephonyBytesSent,
          firstAudioChunkMs,
          // Sub-1s Voice Bridge KPI: wall-clock from WSS open → first audio.
          // Compare across calls with prewarmHit=true vs false to validate
          // the pre-warm path. Target: pickupToFirstAudioMs ≤ 1000 ms on hits.
          pickupToFirstAudioMs,
          prewarmHit:                !!(prewarmEntry && prewarmEntry.gemini && !prewarmEntry.geminiClosedPreAttach),
          prewarmSetupAgeMs:         (prewarmEntry && prewarmEntry.setupCompleteAt)
                                       ? (session.startTime - prewarmEntry.setupCompleteAt)
                                       : null,
          prewarmUnheardAudioBytes:  prewarmEntry ? prewarmEntry.unheardAudioBytes : null,
          // 2026-06-07 (ChatGPT QA — defer-warm-attach fix) telemetry.
          // prewarmSessionId   = sessionId we were holding (deferred attach mode)
          // prewarmClaimedAfterStart = true if claimWarmSessionAfterStart() succeeded
          prewarmSessionId:          prewarmSessionId,
          prewarmDeferralMode:       !!prewarmSessionId,
          prewarmClaimedAfterStart:  !!prewarmSessionId && !!prewarmEntry,
          anomalies:                 anomalyCounts,
          // Per-turn caller-stop → bot-first-audio (2026-06-04 monitor agent).
          // Distinct from pickupToFirstAudioMs (greeting only). Capped at 20
          // entries to keep calls.stats from bloating on a 10-minute call.
          // SQL: SELECT avg((t->>'replyMs')::int) FROM calls,
          //      jsonb_array_elements(stats->'perTurnReplyMs') t WHERE ...
          perTurnReplyMs:            perTurnReplyMsArr,
          turnCount:                 turnIndex,
          // 2026-06-08 span-level telemetry (per ChatGPT review). Each value
          // is a wall-clock ms timestamp. Subtract pairs in SQL to get
          // per-stage latency. Missing keys = the milestone didn't fire.
          // Keys: runLiveCallStartAt, mediaUpAt, setupCompleteAt,
          //       localVADFirstHitAt, fallbackArmedAt, fallbackFiredAt,
          //       firstInputTranscriptionAt
          spans,
          localVADTriggered,
          fallbackExpiredBeforeSetup,
          preSetupAudioBytesBuffered: preSetupAudioBuffer.length,
          buildSha:                  process.env.RAILWAY_GIT_COMMIT_SHA || process.env.GITHUB_SHA || 'unknown',
        },
      });
    } catch (err) { console.error(`[Live:${vertical.name}] logCall error: ${err.message}`); }

    // Booking-confirmation notifications (WhatsApp / SMS) are DISABLED by
    // default (Vineeth direction 2026-05-25 — "we are not providing SMS or
    // WhatsApp confirmation as of now"). The dealer's sales team calls the
    // customer back instead. To re-enable later, flip env var
    // NOTIFICATIONS_ENABLED=true on Railway (no code change needed).
    const notificationsEnabled = process.env.NOTIFICATIONS_ENABLED === 'true';
    if (session.bookingResult && notificationsEnabled) {
      if (!session.callerPhone) {
        console.warn(`[Live:${vertical.name}] ${callId} — booking ${session.bookingResult.id} confirmed but caller_phone is null; WhatsApp skipped.`);
      } else {
        try {
          const result = await sendWhatsAppConfirmation(session.callerPhone, session.bookingResult, session.business);
          if (result?.status === 'sent') {
            await markBookingWhatsAppSent(session.bookingResult.id);
            console.log(`[Live:${vertical.name}] ${callId} — WhatsApp sent (msgId=${result.messageId}); booking ${session.bookingResult.id} marked delivered`);
          } else {
            console.warn(`[Live:${vertical.name}] ${callId} — WhatsApp ${result?.status || 'unknown'} for booking ${session.bookingResult.id}: ${result?.reason || 'no reason'} (booking NOT marked delivered)`);
          }
        } catch (err) {
          console.error(`[Live:${vertical.name}] ${callId} — WhatsApp error for booking ${session.bookingResult.id}: ${err.message}`);
        }
      }
    } else if (session.bookingResult) {
      console.log(`[Live:${vertical.name}] ${callId} — booking ${session.bookingResult.id} saved; customer notification skipped (NOTIFICATIONS_ENABLED=false).`);
    }

    if (isWsOpen(ws)) ws.close();
  }

  // ── Vobiz WebSocket events ──────────────────────────────────────────────

  ws.on('message', async (data, isBinary) => {
    if (callEnded) return;
    try {
      if (isBinary) {
        if (geminiWs && setupComplete) {
          const up = resample(data, 8000, 16000);
          geminiWs.sendRealtimeInput({ audio: { data: up.toString('base64'), mimeType: 'audio/pcm;rate=16000' } });
        }
        return;
      }
      const msg = safeJson(data.toString());
      if (!msg?.event) return;

      if (msg.event === 'start') {
        if (msg.streamId)        session.streamId = msg.streamId;
        if (msg.start?.streamId) session.streamId = msg.start.streamId;
        session.callerPhone = msg.start?.from || msg.start?.caller || msg.start?.ani || msg.from || session.callerPhone || null;
        spans.mediaUpAt = Date.now();
        console.log(`[Live:${vertical.name}] ${callId} start — caller=${session.callerPhone}, streamId=${session.streamId} (mediaUp) prewarmSession=${prewarmSessionId || '(none)'}`);
        // 2026-06-08 (ChatGPT review): arm the kickoff fallback timer at
        // media-up, NOT at setupComplete. This bounds dead-air to
        // KICKOFF_FALLBACK_MS regardless of how long Gemini takes to come up.
        armKickoffFallbackTimer();
        // 2026-06-07 (ChatGPT QA — defer-warm-attach fix):
        // event:start is the green light to attach the warm session. The
        // ensureGeminiConnected() call below will now take the lazy-claim
        // path inside the function: it sees spans.mediaUpAt is set, runs
        // claimWarmSessionAfterStart(), and then the PREWARM ATTACH branch
        // installs the listener + replays the buffered setupComplete.
        //
        // For cold path (no prewarmSessionId), this is the same code path
        // as before: ensureGeminiConnected awaits the early-connect Promise.
        await ensureGeminiConnected().catch((err) =>
          console.error(`[Live:${vertical.name}] ${callId} — start connect failed: ${err.message}`)
        );
      }

      else if (msg.event === 'media' && msg.media?.payload) {
        // 2026-06-08 (ChatGPT review): even BEFORE Gemini is ready, decode +
        // RMS-check the caller's audio for speech onset. This is the earliest
        // signal we have ("Hello?" arrives ~500-1500ms after pickup, often
        // BEFORE Gemini's setupComplete on cold-connect calls). Use it to
        // cancel the kickoff fallback timer.
        const raw = Buffer.from(msg.media.payload, 'base64');
        const pcm8k = (STREAM_CODEC === 'mulaw') ? ulaw.decodeMuLawBuffer(raw) : raw;
        if (!localVADTriggered && pcm8k.length >= 2) {
          const rms = computePcmRms(pcm8k);
          if (rms > VAD_RMS_THRESHOLD) {
            vadHotFrameStreak += 1;
            if (vadHotFrameStreak >= VAD_MIN_FRAMES) {
              onLocalVadSpeechOnset();
            }
          } else {
            vadHotFrameStreak = 0;
          }
        }
        // 2026-06-08 (ChatGPT review P0 #2): if Gemini isn't ready, buffer
        // the audio rather than dropping it. flushPreSetupAudioToGemini()
        // replays it after setupComplete (only when localVADTriggered, so
        // we don't waste bandwidth on silent pickups).
        if (geminiWs && setupComplete) {
          const up = resample(pcm8k, 8000, 16000);
          geminiWs.sendRealtimeInput({ audio: { data: up.toString('base64'), mimeType: 'audio/pcm;rate=16000' } });
        } else if (vertical.kickoffMode === 'caller-first') {
          appendPreSetupAudio(pcm8k);
        }
        // Non-caller-first verticals: keep historical "drop pre-setup" behavior
        // (the synthetic [call started] trigger is the first turn anyway).
      }

      else if (msg.event === 'stop') {
        endCallOnce('caller_hangup').catch(() => {});
      }
    } catch (err) {
      console.error(`[Live:${vertical.name}] ${callId} WS msg error: ${err.message}`);
    }
  });

  ws.on('close', (code, reason) => {
    // 2026-06-07 (ChatGPT QA — defer-warm-attach fix):
    // If WS closed BEFORE Vobiz event:start (spans.mediaUpAt unset), and
    // we were holding a deferred warm sessionId, release it back to the
    // registry's cancel path so the underlying Gemini socket gets closed.
    if (prewarmSessionId && !prewarmEntry && !spans.mediaUpAt) {
      try {
        require('./warmSessionRegistry').cancel(prewarmSessionId, 'ws_closed_before_start');
      } catch (_) {}
    }
    const reasonStr = (reason && typeof reason.toString === 'function') ? reason.toString() : '';
    console.warn(`[Live:${vertical.name}] ${callId} Vobiz WS close code=${code} reason="${reasonStr}" sawStart=${!!spans.mediaUpAt} prewarmDeferred=${!!prewarmSessionId && !prewarmEntry}`);
    endCallOnce('caller_hangup').catch(() => {});
  });
  ws.on('error', (err) => console.error(`[Live:${vertical.name}] ${callId} Vobiz error: ${err.message}`));

  // Auto-timeout
  timeoutTimer = setTimeout(() => endCallOnce('timeout').catch(() => {}), MAX_CALL_MS);

  // ── EARLY CONNECT (latency reduction, single-flight-safe) ──────────────
  // Fire-and-forget kick the Gemini connect the moment Vobiz WS opens. This
  // parallelises ~1-2s of model warmup with Vobiz's signalling handshake. When
  // the `start` event later arrives, the handler `await`s ensureGeminiConnected
  // — if the early-connect is still in flight, it waits on the SAME Promise
  // (no second session is opened). If the early-connect already finished,
  // the start handler returns immediately and audio starts flowing.
  //
  // 2026-06-07 (ChatGPT QA — defer-warm-attach fix):
  // SKIP early-connect when a deferred warm session is present. The whole
  // point of the deferral is to wait for event:start before doing anything
  // Gemini-side. ensureGeminiConnected will short-circuit if invoked
  // pre-event:start, but better to not invoke it at all.
  if (!prewarmSessionId && !prewarmEntry) {
    ensureGeminiConnected().catch((err) =>
      console.error(`[Live:${vertical.name}] ${callId} — early connect failed: ${err.message}`)
    );
  } else {
    console.log(`[Live:${vertical.name}] ${callId} — early connect SKIPPED (prewarm path: sessionId=${prewarmSessionId || '(none)'}, entry=${!!prewarmEntry})`);
  }

  // 3s belt-and-braces: in case neither early-connect nor `start` triggered
  // a successful connect (e.g. transient SDK error), retry from a clean slate.
  setTimeout(async () => {
    if (!geminiWs && !callEnded) {
      console.log(`[Live:${vertical.name}] ${callId} — no Gemini yet, retrying (fallback)`);
      // Reset the cached failed promise so we can try a fresh connect.
      geminiConnectPromise = null;
      await ensureGeminiConnected().catch((err) =>
        console.error(`[Live:${vertical.name}] fallback connect failed: ${err.message}`)
      );
    }
  }, 3000);
}

// ─── Standalone connect-config builder (Sub-1s Voice Bridge) ────────────────
// Exported so warmSessionRegistry can build the SAME live config that
// runLiveCall would build, without instantiating the full closure. Mirrors
// the prompt + voice + VAD logic inside connectGemini(). If you ever change
// the connect config in connectGemini, MIRROR IT HERE — drift will cause
// warm and cold sessions to behave differently.
//
// Returns { model, config } ready to pass to ai.live.connect(...).
async function buildLiveConnectConfig(business, vertical) {
  if (!business || !vertical) throw new Error('buildLiveConnectConfig: business + vertical required');
  const { buildPrompt, tools } = vertical;
  const dates = getIstDates();

  const baseSystemPrompt = buildPrompt(business, dates);

  let catalogueBlock = '';
  if (vertical.name !== 'salon' && business?.id) {
    try {
      const cat = await getCatalogue(business.id);
      catalogueBlock = formatCatalogueForPrompt(cat, vertical.name);
    } catch (_) { /* non-fatal; cold path logs the same way */ }
  }
  const systemPrompt = catalogueBlock ? `${baseSystemPrompt}\n\n${catalogueBlock}` : baseSystemPrompt;

  const voiceName = business?.voice_name || 'Aoede';

  const SILENCE_MS    = Number(process.env.LIVE_SILENCE_MS    || 600);
  const PREFIX_PAD_MS = Number(process.env.LIVE_PREFIX_PAD_MS || 100);
  // 2026-06-08 (ChatGPT QA Stage B1):
  // Mirror the cold-path connectGemini() VAD env reads here so that warm-
  // session calls (the prewarm path) also pick up LIVE_END_SENS /
  // LIVE_START_SENS overrides. Without this, warm-attached outbound calls
  // were ignoring sensitivity env vars and paying the full ~1.0-1.6s per-
  // turn lag observed in call 5926a3e1 (perTurnReplyMs=[1580,1202,1017]),
  // while the inbound vani-server service (cold-path only) was already
  // benefitting from END_SENSITIVITY_HIGH.
  //
  // Behavior preserved when env vars unset: payload bytes identical to
  // pre-patch (no default sensitivity field added to AAD).
  const END_SENS      = process.env.LIVE_END_SENS   || null;  // unset = SDK default
  const START_SENS    = process.env.LIVE_START_SENS || null;  // unset = SDK default
  const aad = {
    silenceDurationMs: SILENCE_MS,
    prefixPaddingMs:   PREFIX_PAD_MS,
  };
  if (END_SENS)   aad.endOfSpeechSensitivity   = END_SENS;
  if (START_SENS) aad.startOfSpeechSensitivity = START_SENS;
  const vadConfig = process.env.LIVE_VAD_DISABLED === 'true' ? {} : {
    realtimeInputConfig: { automaticActivityDetection: aad },
  };

  return {
    model: RESOLVED_MODEL,
    live: {
      responseModalities:       [Modality.AUDIO],
      systemInstruction:        { parts: [{ text: systemPrompt }] },
      speechConfig:             { voiceConfig: { prebuiltVoiceConfig: { voiceName } } },
      inputAudioTranscription:  {},
      outputAudioTranscription: {},
      tools:                    [{ functionDeclarations: tools }],
      thinkingConfig:           { thinkingBudget: Number(process.env.LIVE_THINKING_BUDGET || 0) },
      ...vadConfig,
    },
  };
}

module.exports = { runLiveCall, getIstDates, buildLiveConnectConfig, RESOLVED_MODEL };
