# Vani — Architecture (2026-06-08)

Inbound + outbound voice receptionist for Indian SMBs. Built by Navamitra (Vineeth S, Bangalore).

This document captures the architecture as locked on 2026-06-08 after a 12-hour debug marathon. The configuration is **frozen**; changing any of the items in §11 requires the owner's explicit go-ahead.

---

## 1. Two-service split

Vani runs as **two Railway services** that share Supabase + Vobiz + Google credentials but mount different code entry points.

```
                    ┌────────────────────────────────────────┐
                    │  Supabase (project: AiVani, Mumbai)    │
                    │  businesses, calls, bookings,          │
                    │  reminder_queue, customers, ...        │
                    └────────────────────────────────────────┘
                                       ▲
                                       │ jsonb writes
              ┌────────────────────────┼───────────────────────────┐
              │                        │                           │
   ┌──────────┴─────────┐   ┌──────────┴──────────────────────────┐
   │  vani-server       │   │  vani-outbound                       │
   │  Railway, SG       │   │  Railway, SG                         │
   │  inbound only      │   │  outbound + service-reminders        │
   │  node index.js     │   │  node index-outbound.js              │
   └────────┬───────────┘   └───────┬──────────────────────────────┘
            │                       │
            ▼                       ▼
   ┌────────────────────────────────────────────────────────────┐
   │  Vobiz (Indian PSTN)                                        │
   │  • inbound DIDs: 080-6548-0620 (hotel/restaurant)           │
   │                  080-4026-0668 (EV dealer / D&V Ather)      │
   │  • outbound caller-IDs: +91 92621 02419, +91 80715 78513    │
   └────────────────────────────────────────────────────────────┘
```

* **Inbound** (`vin3ss/vani` repo): customer dials a Vobiz DID → Vobiz hits `/vobiz/xml` on vani-server → server returns `<Stream>` → Vobiz upgrades to `/call` WSS → audio bridges to Gemini Live → server speaks back over the same WS.
* **Outbound** (`vin3ss/vani-outbound` repo): dashboard or cron triggers `/api/demo/outbound-call` or the reminder dispatcher → server hits Vobiz API to dial customer → Vobiz hits `/vobiz/xml-outbound` → returns `<Stream>` → upgrades `/call/outbound` → audio bridge.

The split exists because outbound caller-ID assignment + service-reminder queues + dealer dashboards live on a different release cadence than the inbound voice loop; locking one shouldn't gate the other.

---

## 2. Outbound call lifecycle (LOCKED)

```
t=0          POST /api/demo/outbound-call    (dashboard or dispatcher → vani-outbound)
                │
                ├─ mint ctx_id, ctx → outboundCtx
                ├─ LIVE_PREWARM=true → warmRegistry.startWarm(sessionId)
                │     │
                │     └─ connectGemini() → ai.live.connect()  // ~1300ms — happens DURING ring
                │
                └─ vobizOutbound.dial(answer_url=/vobiz/xml-outbound?ctx=..., caller_id=…)
                      │
                      ▼
t≈5s        Vobiz rings customer phone +91 95266 97779 (or whoever)

t≈20s       Customer picks up
                │
                ├─ Vobiz GETs /vobiz/xml-outbound?ctx=ctx-...
                │     server returns:
                │     <Response>
                │       <Stream bidirectional="true" keepCallAlive="true"
                │               contentType="audio/x-l16;rate=8000">
                │         wss://vani-outbound-production.up.railway.app/call/outbound?ctx=...&callId=...&sessionId=...
                │       </Stream>
                │     </Response>
                │
                ├─ Vobiz upgrades WS → /call/outbound
                │     │
                │     ├─ server.on('upgrade') router dispatches by pathname  ← critical fix from 2026-06-08
                │     │     (see §8 "WSS multi-instance trap")
                │     │
                │     └─ wssOutbound.handleUpgrade(req, socket, head, …)
                │           │
                │           ├─ getOutboundCtx(ctx_id) → business + customer  // bail if expired
                │           ├─ DO NOT call warmRegistry.attach() here ← caused the prewarm race
                │           └─ handleOutboundCall(ws, …, { prewarmSessionId })
                │                 │
                │                 ▼
                │           geminiLiveBase.runLiveCall(...)
                │                 │
                │                 ├─ early connect SKIPPED (prewarm path is set)
                │                 ├─ on Vobiz event:'start' (= mediaUp):
                │                 │     spans.mediaUpAt = now()
                │                 │     armKickoffFallbackTimer()
                │                 │     ensureGeminiConnected()
                │                 │           │
                │                 │           └─ claimWarmSessionAfterStart()  ← LAZY claim
                │                 │                 │
                │                 │                 └─ wraps prewarm session → installListener
                │                 │                                            (replays buffered setupComplete)
                │                 ├─ on Vobiz event:'media':
                │                 │     decode base64 → 8kHz PCM
                │                 │     local-RMS VAD energy check (turn-onset hint)
                │                 │     resample 8k→16k → geminiWs.sendRealtimeInput({audio:…})
                │                 │
                │                 ├─ on Gemini serverContent (audio):
                │                 │     harvestAudio() → outputBuffer
                │                 │     flushAudio() → μ-law encode → base64 → sendToVobiz({event:'playAudio'})
                │                 │
                │                 ├─ on Gemini serverContent (tool call):
                │                 │     onToolCall(fc, session) → response back to Gemini
                │                 │     (tools: end_call_politely, reschedule_request, mark_opted_out)
                │                 │
                │                 └─ on tool returns endCall reason:
                │                       endCallOnce(reason)
                │                       Supabase logCall(stats, transcript, …)

t≈55s       Caller hangs up OR end_call_politely fires
                │
                ├─ ws.on('close') → endCallOnce('caller_hangup')
                ├─ Gemini WS close
                └─ supabase.logCall → calls table row
```

**Observed latencies (locked state, 2026-06-08 17:20 UTC, call_uuid `8939b141`):**

| Metric | Value | Notes |
|---|---|---|
| `ms_to_setup` | 291 ms | from runLiveCallStart → setupComplete (prewarm hit) |
| `pickup_to_first_audio_ms` | **735 ms** | from event:start → first bot audio sent to Vobiz |
| `per_turn_replyMs` | 0–1 ms | telemetry quirk; conversation feels natural |
| `end_reason` | `customer_interested` | full call-flow completion (yes branch) |
| Model | `gemini-3.1-flash-live-preview` | |
| Build | `5c8371ce` | |

---

## 3. Inbound call lifecycle (LOCKED — for reference)

Mirror image of outbound, three differences:

1. Vobiz hits us first (the DID is a Vobiz number routed to our `/vobiz/xml`).
2. Default `kickoffMode` is **NOT** `caller-first`; instead the engine sends a synthetic `[call started]` trigger when setupComplete fires, so Vani greets the caller as soon as media is up.
3. No prewarm — pickup is a few seconds before media-up, so the cold-connect runs during ringing.

Gold standard: **Adukkala restaurant (DID 080-6548-0620)** consistently hits **819 ms pickup-to-first-audio**. That's the bar for everything else.

Source files (in `vin3ss/vani` repo, not this one):

* `index.js` — entry point
* `geminiLiveBase.js` — shared engine
* `geminiLive-restaurant.js` (Adukkala) / `geminiLive-salon.js` / `geminiLive-supermarket.js` / `geminiLive-ev.js` — per-vertical config

**Inbound is LOCKED — see `feedback_inbound_locked` memory; do not touch without explicit go-ahead.**

---

## 4. Vobiz integration

### 4.1 XML for outbound (`/vobiz/xml-outbound`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Stream bidirectional="true" keepCallAlive="true"
          contentType="audio/x-l16;rate=8000">
    wss://vani-outbound-production.up.railway.app/call/outbound?ctx=...&amp;callId=...&amp;sessionId=...
  </Stream>
</Response>
```

**Locked attributes:**

* `bidirectional="true"` — required for two-way audio
* `keepCallAlive="true"` — required, otherwise Vobiz hits "End Of XML Instructions" (HangupCauseCode 4010) and tears the call within seconds
* `contentType="audio/x-l16;rate=8000"` — DO NOT switch to `audio/x-mulaw;rate=8000`; Vobiz tears the WS at TCP level (close code 1006) before sending event:start. This was the 2026-06-07 12-hour false trail.

### 4.2 Vobiz WebSocket event stream

The bidirectional WS speaks JSON events. Sequence the server handles:

| Event | Payload | Server action |
|---|---|---|
| `start` | `{streamId, from, caller, ani}` | `spans.mediaUpAt = now()`, arm 1200 ms kickoff fallback timer, call `ensureGeminiConnected()` |
| `media` | `{media:{payload:base64}}` | decode → 8 kHz PCM → upsample 16 kHz → `geminiWs.sendRealtimeInput({audio:…})` |
| `stop` | `{}` | `endCallOnce('caller_hangup')` |

Server → Vobiz: `{ event: 'playAudio', media: { payload: base64-μ-law-or-L16 } }` (encoded to match the negotiated `contentType`).

### 4.3 Vobiz CDR fields used for diagnosis

When something goes wrong, the authoritative source is the Vobiz dashboard CDR, not our logs. Key fields:

* `HangupCauseCode` — e.g. `4010 "End Of XML Instructions"` means Vobiz reached end of XML with no media plane open.
* `HangupSource` — `Vobiz` vs `caller` vs `network` vs `system`.
* `Ring Duration`, `Bill Sec` — sanity-check whether the customer actually picked up.

**Lesson from 2026-06-07:** pull the Vobiz CDR FIRST when calls fail before chasing code hypotheses.

---

## 5. Gemini Live integration

### 5.1 Locked configuration

| Setting | Value | Why |
|---|---|---|
| Model | `gemini-3.1-flash-live-preview` | Google's "step change in latency"; ~50% lower pickup-to-first-audio vs 2.5-native-audio. Same audio cost rate ($12/1M output tokens). |
| `responseModalities` | `[Modality.AUDIO]` | native audio in/out |
| `voiceConfig.prebuiltVoiceConfig.voiceName` | per-business (`businesses.voice_name`); default `'Aoede'` | D&V Ather is `'Leda'`. |
| `inputAudioTranscription` | `{}` enabled | for transcript persistence |
| `outputAudioTranscription` | `{}` enabled | for transcript persistence |
| `thinkingConfig.thinkingBudget` | `0` | latency-critical |
| `realtimeInputConfig.automaticActivityDetection.silenceDurationMs` | `300` | how long to wait for silence after caller speech before declaring turn end |
| `realtimeInputConfig.automaticActivityDetection.prefixPaddingMs` | `100` | how much audio context before turn-start |
| `realtimeInputConfig.automaticActivityDetection.endOfSpeechSensitivity` | `END_SENSITIVITY_HIGH` | turn-detection faster |
| `realtimeInputConfig.automaticActivityDetection.startOfSpeechSensitivity` | `START_SENSITIVITY_HIGH` | turn-onset faster |
| `maxOutputTokens` | UNSET | setting it causes silent-call regression — see rollback note `b81e2f4` in source |

### 5.2 Pre-warm + defer-attach pattern

The dial endpoint opens the Gemini session **during the ring window** so setupComplete is already buffered by the time the customer picks up. Three guard-rails:

1. `warmRegistry.startWarm(sessionId, …)` mints a sessionId and opens the WS to Google. Cancellable if dial fails.
2. The WSS handler **does NOT attach** the warm session. It just passes `prewarmSessionId` through to `runLiveCall`.
3. `runLiveCall.ensureGeminiConnected()` short-circuits to `Promise.resolve(null)` until `spans.mediaUpAt` is set (i.e. Vobiz `event:start` has fired). Only then does it lazily call `warmRegistry.attach(sessionId)`.

This defers all Gemini-side side effects until the telephony stream is established. Without it, attaching the warm session synchronously replays buffered `setupComplete` through `onGeminiMessage`, which Vobiz interprets as protocol violation and the WS dies in 1–5 ms.

### 5.3 Two builder paths

Live config is built in two places that must stay in sync:

| Path | Function | Used by |
|---|---|---|
| Cold | `connectGemini()` inside `runLiveCall` | non-prewarm callers |
| Warm | `buildLiveConnectConfig(business, vertical)` (exported) | `warmSessionRegistry.startWarm` |

Both must read the same env vars and produce structurally identical config. Drift between the two caused a real bug on 2026-06-08 (warm path was ignoring `LIVE_END_SENS` env). Mirror logic is now in both.

---

## 6. Audio bridge

### 6.1 Caller → Gemini

```
Vobiz event:media (base64 L16 8 kHz)
   → Buffer.from(payload, 'base64')
   → resample 8 → 16 kHz (linear interpolation)
   → geminiWs.sendRealtimeInput({ audio: { data: base64(pcm16k), mimeType: 'audio/pcm;rate=16000' } })
```

If Gemini is not yet ready (caller-first mode + setupComplete pending), the audio is buffered into a 2-second cap (`PRESETUP_AUDIO_MAX_BYTES`) and replayed after setupComplete.

### 6.2 Gemini → Vobiz

```
Gemini serverContent (modelTurn.parts[].inlineData base64 24 kHz PCM)
   → Buffer.from(data, 'base64')
   → downsample 24 → 8 kHz
   → match Vobiz contentType:
        - audio/x-l16;rate=8000 → send raw PCM
        - audio/x-mulaw;rate=8000 → μ-law encode via ulaw.js
   → ws.send(JSON.stringify({ event: 'playAudio', media: { payload: base64(out) } }))
```

`ulaw.js` is the ITU-T G.711 reference μ-law table (byte-identical to Twilio/Plivo PCMU). Kept in repo even though `STREAM_CODEC=linear16` is locked — used during diagnostic-only mulaw experiments.

---

## 7. Verticals (outbound-fast vs ev-outbound vs adukkala-style)

### 7.1 Vertical shape

```js
const VERTICAL = {
  name:          'ev-outbound',
  direction:     'outbound',   // optional; defaults to name suffix heuristic
  buildPrompt:   (business, dates) => '...',
  tools:         [...],          // function declarations
  onToolCall:    async (fc, session) => ({ response, booking, endCall? }),
  kickoffMode:   'caller-first',  // optional; default is server-greeting
  kickoffTrigger:(business) => 'SAY_NOW_EXACTLY: "..."', // optional fallback line
};
```

### 7.2 Available verticals on `vani-outbound`

| File | Vertical name | Direction | Notes |
|---|---|---|---|
| `geminiLive-ev-outbound.js` | `ev-outbound` | outbound | **The locked production vertical.** Adukkala-style flow, kickoff trigger sends a fixed greeting, branches on customer response (yes / no / busy / wrong number / opt-out). |
| `geminiLive-ev-outbound-fast.js` | `ev-outbound-fast` | outbound | Alternative caller-first vertical. Built during the debug sprint; kept in repo but `/api/demo/outbound-fast` is unused — the production endpoint is `/api/demo/outbound-call`. |

---

## 8. WSS multi-instance trap (2026-06-08 root-cause)

This was the actual root cause of a 12-hour debug marathon. Documenting in detail so it doesn't reoccur.

**The trap:** when you create two `WebSocketServer` instances bound to the same http server with `path:` option, each auto-installs its own `server.on('upgrade')` listener. The first listener to fire on any incoming upgrade calls `wss.shouldHandle(req)`. If the path doesn't match its own, it calls `abortHandshake(socket, 400)` — which writes `HTTP/1.1 400 Bad Request` and destroys the socket **before** the second WSS listener gets to see the request.

Result: 100% of WS upgrades that target the second-registered path die at TCP level with HTTP 400. Vobiz sees this as `HangupCauseCode 4010` ("End Of XML Instructions" — because no media plane ever opened).

**The fix:** use `noServer: true` on every WSS, then register a single `server.on('upgrade')` router that parses the path and dispatches manually via `wss.handleUpgrade()`.

```js
const wssOutbound     = new WebSocketServer({ noServer: true });
const wssOutboundFast = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  let pathname;
  try {
    pathname = new URL(req.url, `http://${req.headers.host}`).pathname;
  } catch (_) {
    socket.destroy();
    return;
  }
  if (pathname === '/call/outbound') {
    wssOutbound.handleUpgrade(req, socket, head, (ws) => wssOutbound.emit('connection', ws, req));
  } else if (pathname === '/call/outbound-fast') {
    wssOutboundFast.handleUpgrade(req, socket, head, (ws) => wssOutboundFast.emit('connection', ws, req));
  } else {
    socket.destroy();   // TCP RST, not malformed HTTP 400
  }
});
```

**Heuristic for next time:** if Vobiz CDR shows `HangupCauseCode 4010 source=Vobiz` AND Railway HTTP logs show `GET /call/... → 400` with NO matching app-side log line, the `ws` library aborted the upgrade. Check for multiple `new WebSocketServer({ server, path })` calls on the same http server.

---

## 9. Environment variables

### 9.1 vani-outbound (locked)

| Var | Value | Locked? | Purpose |
|---|---|---|---|
| `GEMINI_MODEL` | `gemini-3.1-flash-live-preview` | 🔒 | THE latency-critical knob |
| `GEMINI_API_KEY` | (Google) | 🔒 | API key |
| `GEMINI_PROJECT_NUMBER` | (Google) | | unused by SDK; informational |
| `GCP_PROJECT_ID` | (Google) | | informational |
| `GOOGLE_CREDENTIALS_BASE64` | (Google) | | service account for Vertex AI fallback |
| `LIVE_PREWARM` | `true` | 🔒 | enables warm session during ring |
| `LIVE_SILENCE_MS` | `300` | 🔒 | Gemini VAD silenceDurationMs |
| `LIVE_END_SENS` | `END_SENSITIVITY_HIGH` | 🔒 | end-of-speech sensitivity |
| `LIVE_START_SENS` | `START_SENSITIVITY_HIGH` | 🔒 | start-of-speech sensitivity |
| `LIVE_PREFIX_PAD_MS` | (unset → 100) | | turn-onset context |
| `LIVE_THINKING_BUDGET` | (unset → 0) | | thinking off for latency |
| `STREAM_CODEC` | `linear16` | 🔒 | DO NOT switch to mulaw (Vobiz tears WS) |
| `VOBIZ_ACCOUNT_SID` | (Vobiz) | | API auth |
| `VOBIZ_AUTH_ID` | (Vobiz) | | API auth |
| `VOBIZ_AUTH_TOKEN` | (Vobiz) | | API auth |
| `VOBIZ_API_BASE` | (Vobiz URL) | | API endpoint |
| `VOBIZ_PHONE_NUMBER` | `+918040260668` | | default caller-ID (D&V Ather DID) |
| `VOBIZ_CHANNEL_ID` | (Vobiz) | | channel ID |
| `SUPABASE_URL`, `*_ANON_KEY`, `*_SERVICE_ROLE_KEY` | (Supabase) | | DB |
| `CRON_SECRET` | (random) | | bearer auth on cron endpoints |
| `DEFAULT_BUSINESS_ID` | `client-dv-ather` | | fallback business config |
| `SERVER_HOST` | `vani-outbound-production.up.railway.app` | | streamUrl host (no protocol prefix) |
| `NODE_ENV` | `production` | | |
| `NAVAMITRA_SUPPORT_EMAIL` | `support@navamitra.in` | | branding |

🔒 = locked per 2026-06-08 working state; ask before changing.

---

## 10. Telemetry — calls.stats schema

Every call writes a row to `calls` with a rich JSONB `stats` field. Key fields useful for latency diagnosis:

```jsonc
{
  "model":                      "gemini-3.1-flash-live-preview",
  "vertical":                   "ev-outbound",
  "buildSha":                   "5c8371ce…",
  "setupCompleted":             true,
  "msToSetup":                  291,
  "pickupToFirstAudioMs":       735,
  "firstAudioChunkMs":          735,
  "firstBotAudioSent":          true,
  "perTurnReplyMs":             [{"turn": 1, "replyMs": …}, …],
  "turnCount":                  2,
  "geminiAudioBytes":           719584,
  "geminiAudioChunks":          …,
  "telephonyBytesSent":         239860,
  "telephonyChunksSent":        …,
  "prewarmHit":                 true,
  "prewarmSessionId":           "…",
  "prewarmDeferralMode":        true,
  "prewarmClaimedAfterStart":   true,
  "prewarmSetupAgeMs":          …,
  "prewarmUnheardAudioBytes":   0,
  "localVADTriggered":          …,
  "preSetupAudioBytesBuffered": 0,
  "fallbackExpiredBeforeSetup": false,
  "anomalies":                  { "transcriptWithoutAudio": 0, "turnCompleteWithoutAudio": 0 },
  "spans": {
    "runLiveCallStartAt":         <epoch_ms>,
    "mediaUpAt":                  <epoch_ms>,
    "setupCompleteAt":            <epoch_ms>,
    "localVADFirstHitAt":         <epoch_ms>,
    "firstInputTranscriptionAt":  <epoch_ms>,
    "fallbackArmedAt":            <epoch_ms>,
    "fallbackFiredAt":            <epoch_ms>
  },
  "firstServerContentSamples":  [...]  // dev-only first 3 Gemini wire shapes
}
```

SQL for the lag-investigation pattern:

```sql
SELECT id, end_reason, duration_seconds,
       stats->>'pickupToFirstAudioMs'    AS p2fa,
       stats->>'msToSetup'               AS ms_to_setup,
       stats->'perTurnReplyMs'           AS per_turn,
       stats->>'geminiAudioBytes'        AS gem_bytes,
       stats->>'telephonyBytesSent'      AS tele_bytes,
       stats->>'buildSha'                AS build,
       stats->>'model'                   AS model,
       created_at
  FROM calls
 WHERE caller_phone = '+91…'
   AND created_at > now() - interval '1 hour'
 ORDER BY created_at DESC
 LIMIT 5;
```

---

## 11. Lock rules

Per the owner's instruction on 2026-06-08 ("yes nice lets lock this and dont change anything"):

1. **Do not change `GEMINI_MODEL`** on `vani-outbound` Railway env without explicit go-ahead.
2. **Do not change `LIVE_PREWARM`, `LIVE_SILENCE_MS`, `LIVE_END_SENS`, `LIVE_START_SENS`, or `STREAM_CODEC`** without explicit go-ahead.
3. **Do not merge to `vin3ss/vani-outbound:main`** anything that touches `geminiLiveBase.js`, `index-outbound.js`, `warmSessionRegistry.js`, `package.json`, or `railway.json` without explicit go-ahead.
4. **Do not touch the inbound `vin3ss/vani` repo** files `index.js`, `geminiLiveBase.js`, `geminiLive-ev.js`, `geminiLive-restaurant.js`, `geminiLive-salon.js`, `geminiLive-supermarket.js` without explicit go-ahead.
5. **When a problem appears:** pull Supabase calls row + Railway logs + Vobiz CDR FIRST. Diagnose before proposing change.
6. **Already-falsified hypotheses (don't re-attempt without strong new evidence):**
   * μ-law codec (Vobiz tears WS)
   * sessionId in streamUrl (verified to NOT be the killer)
   * defer-warm-attach as primary fix (was correct logic, wrong root cause)
   * prompt trim (prompt is already 485 words)
   * `maxOutputTokens` (causes silent-call regression — see `b81e2f4` rollback note)

---

## 12. Key commits on `vin3ss/vani-outbound:main` (today)

| SHA | Subject |
|---|---|
| `342a7072` | fix(railway): start `node index-outbound.js` (was `index.js` — 388-line stale stub) |
| `f5ed61f8` | fix(package): main + start → `index-outbound.js` |
| `cc7d15a0` | Merge `fix/defer-warm-attach-until-start` — defer warm-session attach until Vobiz event:start |
| `f8860b1d` | **Merge `fix/wss-multi-instance-conflict`** — `noServer:true` + single upgrade router (the actual root cause) |
| `5c8371ce` | Merge `fix/live-vad-sensitivity-warm-path` — `buildLiveConnectConfig` reads `LIVE_END_SENS` / `LIVE_START_SENS` |

Current main HEAD: **`5c8371ce`**.

---

*Authored 2026-06-08 by Vineeth S / Navamitra (with Claude). Update only with explicit go-ahead.*
