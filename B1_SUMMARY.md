# Stage B1 — buildLiveConnectConfig VAD env wiring

## Why
- Cold-path `connectGemini()` (geminiLiveBase.js:942-955) reads `LIVE_END_SENS` + `LIVE_START_SENS` and conditionally adds to AAD.
- Warm-path `buildLiveConnectConfig()` (geminiLiveBase.js:1289-1298 pre-patch) only read `LIVE_SILENCE_MS` + `LIVE_PREFIX_PAD_MS` — sensitivity env vars were silently ignored.
- Result: warm-attached outbound calls had higher per-turn lag than inbound (cold path).

## What changed
- Added 2 env reads (`END_SENS`, `START_SENS`) inside `buildLiveConnectConfig` — mirror of cold path.
- Build `aad` object same way (conditional `if (END_SENS) aad.endOfSpeechSensitivity = END_SENS;`).
- No defaults added when env unset → payload byte-identical to pre-patch.

## What B3 deferred
- maxOutputTokens=100 — NOT applied. Historical rollback notes (b81e2f4) warned this caused transcript-without-audio.

## B2 reassessment
- The QA agent reported EV outbound prompt was 1368 words. Actual word count of the user-visible prompt in `geminiLive-ev-outbound.js` is **485 words, 43 lines** (already lean). The 1368 was counting JS code + comments + template literal markers. **B2 trim cancelled** — nothing meaningful to trim.

## After merge
1. Set Railway env on Vani-outbound:
   - LIVE_END_SENS=END_SENSITIVITY_HIGH
   - LIVE_START_SENS=START_SENSITIVITY_HIGH
2. Dial 3 test calls.
3. Expected perTurnReplyMs: 400-700ms (down from 1000-1500ms).
4. Sanity check: no silent calls, no transcript-without-audio anomaly.
