/**
 * warmSessionRegistry.js — pre-warmed Gemini Live sessions
 *
 * Project: Vani Sub-1 Second Voice Bridge
 *
 * Success criterion (Vineeth 2026-06-04):
 *   pickup_to_first_audio_ms  ≤ 1000 ms on warm-session hits
 *   Ideal target              ≤  800 ms
 *   Cold-fallback baseline    ≈ 2000 ms (current production)
 *
 * Why: even with SAY_NOW_EXACTLY (commit 31be790f) the post-pickup gap is
 * ~750 ms of Gemini setup + ~500 ms of first-chunk planning + ~400 ms of
 * Vobiz↔Railway WSS handshake = ~2 s before the caller hears anything.
 *
 * What this does: during the ringing window (5–15 s of silence before the
 * customer picks up), open the Gemini Live session in parallel. By the time
 * Vobiz bridges the WSS, `setupComplete` has already fired and the session
 * is sitting in standby. We send `SAY_NOW_EXACTLY` ~50 ms after bridge and
 * Gemini ships audio in ~500 ms — sub-1.2 s perceived latency.
 *
 * Gated entirely on env LIVE_PREWARM === 'true'. Default off.
 *
 * Vineeth's mandatory guard-rails (2026-06-04 review):
 *  1. Key by callId / unique outboundSessionId, NOT reminderId (which can
 *     be reused on retry/redial). We use a freshly-minted UUID per dial.
 *  2. NO empty `realtimeInputConfig` pings. If a keep-alive is ever needed,
 *     it must be an explicit `SYSTEM_STANDBY_ONLY` text + a prompt rule
 *     that forbids audio. For now we ship NO keep-alives at all (90 s TTL
 *     is below any plausible Gemini idle timeout). Re-introduce only if
 *     warm sessions start dropping in production.
 *  3. Strict TTL — 90 s default — closes the Gemini session on no-answer,
 *     busy, voicemail, ring-no-answer, dial failure, or just plain timeout.
 *     Prevents both Gemini billing leaks and dangling sockets.
 *  4. Fallback path — if attach() returns null (miss) or the warm entry
 *     hasn't reached `setupComplete` yet, the WSS handler must fall back
 *     to the cold connect path (current behaviour). Pre-warm is best-
 *     effort; correctness never depends on it.
 *  5. Guard log — any audio that arrives BEFORE attach is recorded into
 *     `unheardAudioBytes`. If that counter is ever non-zero, the prompt
 *     rule failed and we need to kill the keep-alive or tighten the
 *     standby prompt.
 *  6. Single-instance constraint — this Map only works inside one Node
 *     process. If Railway scales `vani-outbound` to >1 replica, warm
 *     sessions started on instance A can't be attached on instance B.
 *     Log REPLICA_ID + warm-miss reason so the operator can see this.
 *
 * Public API:
 *   - startWarm({ sessionId, business, vertical, ai, callId, customerPhone })
 *       → starts ai.live.connect() in the background, returns the registry
 *         entry immediately (does not await setupComplete). Caller can ignore
 *         the return value — the entry self-registers under sessionId.
 *
 *   - attach(sessionId) → returns the entry and atomically marks it attached
 *         (so a duplicate WSS event can't double-attach). Returns null if
 *         not found or already attached or already cancelled.
 *
 *   - cancel(sessionId, reason) → close the warm Gemini session and remove.
 *         Safe to call multiple times.
 *
 *   - stats() → diagnostic { size, hits, misses, leaks, cancels }
 *
 * Internal entry shape:
 *   {
 *     sessionId,                // UUID
 *     business,                 // business config (with _outboundCustomer)
 *     vertical,                 // vertical handler (for kickoffTrigger + tools)
 *     callId,                   // logical call id (Vobiz CallUUID once bridged)
 *     gemini,                   // the @google/genai Live session
 *     setupComplete,            // boolean — has setupComplete fired?
 *     setupCompleteAt,          // Date.now() when it fired
 *     warmedAt,                 // Date.now() when registry was populated
 *     attached,                 // boolean — has attach() been called?
 *     attachedAt,               // Date.now() when attach() returned this entry
 *     listener,                 // (msg) => void — set by runLiveCall on attach
 *     buffer,                   // array of messages received pre-attach
 *     unheardAudioBytes,        // bytes of inlineData received pre-attach (should be 0)
 *     ttlTimer,                 // setTimeout handle for auto-cleanup
 *     cancelled,                // boolean — true if cancel() ran
 *     cancelReason,             // string — why
 *   }
 */

'use strict';

const { GoogleGenAI, Modality } = require('@google/genai');

const WARM_TTL_MS    = Number(process.env.LIVE_PREWARM_TTL_MS || 90_000);
const REPLICA_ID     = process.env.RAILWAY_REPLICA_ID || `pid-${process.pid}`;

const registry = new Map();   // sessionId -> entry
const stats    = { warmed: 0, hits: 0, misses: 0, ttlExpired: 0, cancelled: 0, leakedAudio: 0 };

function logStats(why) {
  console.log(`[Prewarm] registry_stats ${why}`, {
    size:     registry.size,
    replica:  REPLICA_ID,
    warmed:   stats.warmed,
    hits:     stats.hits,
    misses:   stats.misses,
    ttl:      stats.ttlExpired,
    cancels:  stats.cancelled,
    leakAudio: stats.leakedAudio,
  });
}

/**
 * Begin warming a Gemini Live session for an outbound dial. Returns the
 * registry entry synchronously; the underlying ai.live.connect() resolves
 * asynchronously and flips entry.setupComplete = true when ready.
 *
 * The registry IS the warm-handle. Callers do not need to await anything.
 * The TTL timer starts immediately — if attach() never fires within
 * WARM_TTL_MS, the session is closed and removed.
 *
 * Throws synchronously if sessionId is missing/already in use.
 */
function startWarm({ sessionId, business, vertical, callId, customerPhone, buildConfig }) {
  if (!sessionId) throw new Error('startWarm: sessionId required');
  if (registry.has(sessionId)) {
    // Collision means a buggy caller is reusing IDs. Hard error so we catch
    // it during development. Per guard-rail #1, sessionId must be a fresh
    // UUID per dial.
    throw new Error(`startWarm: sessionId ${sessionId} already in registry`);
  }
  if (!business || !vertical) throw new Error('startWarm: business + vertical required');
  if (typeof buildConfig !== 'function') throw new Error('startWarm: buildConfig(business) required');

  const entry = {
    sessionId,
    business,
    vertical,
    callId:        callId || null,
    customerPhone: customerPhone || null,
    gemini:        null,
    setupComplete: false,
    setupCompleteAt: 0,
    warmedAt:      Date.now(),
    attached:      false,
    attachedAt:    0,
    listener:      null,
    buffer:        [],
    unheardAudioBytes: 0,
    ttlTimer:      null,
    cancelled:     false,
    cancelReason:  null,
  };

  registry.set(sessionId, entry);
  stats.warmed += 1;

  // Schedule TTL cleanup. If attach() takes the entry first, it MUST clear
  // this timer (see attach()).
  entry.ttlTimer = setTimeout(() => {
    if (!entry.attached && !entry.cancelled) {
      stats.ttlExpired += 1;
      const warmAge = Date.now() - entry.warmedAt;
      console.warn(`[Prewarm] TTL expired for sessionId=${sessionId} (warm for ${warmAge}ms, setupComplete=${entry.setupComplete}) — closing Gemini session`);
      _closeAndRemove(entry, 'ttl_expired');
    }
  }, WARM_TTL_MS);

  // Kick off the actual connect. Errors don't reject; they just mean the
  // attach path will see entry.gemini === null and fall back to cold connect.
  _connectGemini(entry, buildConfig).catch((err) => {
    console.error(`[Prewarm] connect failed for sessionId=${sessionId}: ${err.message}`);
    // Leave the entry in place with gemini=null so attach() still sees it
    // and explicitly logs a warm-fail. TTL will sweep it.
    entry.connectError = err.message;
  });

  console.log(`[Prewarm] WARM started sessionId=${sessionId} callId=${callId || '(pending)'} customer=${customerPhone || '(?)'} replica=${REPLICA_ID}`);
  if (registry.size > 10) {
    console.warn(`[Prewarm] registry size now ${registry.size} on replica ${REPLICA_ID} — possible leak; check no-answer cleanup`);
  }
  return entry;
}

/**
 * Open the Gemini Live session and wire its callbacks into the registry
 * entry. The onmessage callback writes to entry.buffer until entry.listener
 * is set (by attach()), then it forwards directly to the listener.
 */
async function _connectGemini(entry, buildConfig) {
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey) throw new Error('GEMINI_API_KEY not set');
  const ai = new GoogleGenAI({ apiKey });

  // 2026-06-08 (ChatGPT review): buildLiveConnectConfig is async (awaits
  // catalogue fetch). Without await, `config` was a Promise → ai.live.connect
  // received undefined model + undefined config → silent fail.
  const config = await buildConfig(entry.business);

  const liveSession = await ai.live.connect({
    model: config.model,
    config: config.live,
    callbacks: {
      onopen() {
        console.log(`[Prewarm] Gemini open sessionId=${entry.sessionId}`);
      },
      onmessage(msg) {
        // Track setupComplete eagerly so attach() can decide quickly whether
        // the warm session is "ready" or whether to fall back to cold.
        if (msg.setupComplete !== undefined && !entry.setupComplete) {
          entry.setupComplete = true;
          entry.setupCompleteAt = Date.now();
          const setupMs = entry.setupCompleteAt - entry.warmedAt;
          console.log(`[Prewarm] setupComplete sessionId=${entry.sessionId} (${setupMs}ms after warm start)`);
        }

        // Standby leak detection: per guard-rail #5, if Gemini emits ANY
        // audio before attach, something violated the standby contract.
        // (We aren't sending keep-alives so this should never fire — but if
        // it does, the prompt's SYSTEM_STANDBY_ONLY rule needs review.)
        if (!entry.attached) {
          const parts = msg?.serverContent?.modelTurn?.parts || [];
          for (const p of parts) {
            if (p?.inlineData?.data) {
              const bytes = Buffer.from(p.inlineData.data, 'base64').length;
              entry.unheardAudioBytes += bytes;
              stats.leakedAudio += bytes;
            }
          }
          if (entry.unheardAudioBytes > 0 && entry.unheardAudioBytes < 5000) {
            // Log once per entry when we first see a leak.
            if (!entry._leakWarned) {
              entry._leakWarned = true;
              console.warn(`[Prewarm] PRE-BRIDGE AUDIO LEAK sessionId=${entry.sessionId} bytes=${entry.unheardAudioBytes} — prompt SYSTEM_STANDBY_ONLY rule may be failing`);
            }
          }
        }

        if (entry.listener) {
          // Post-attach: forward live messages directly.
          try { entry.listener(msg); }
          catch (e) { console.error(`[Prewarm] listener error sessionId=${entry.sessionId}: ${e.message}`); }
        } else {
          // Pre-attach: buffer for replay at attach time. Cap to prevent
          // unbounded growth (≈200 messages is generous; we shouldn't see
          // more than a handful of setupComplete + heartbeats during a
          // 15-second ring window).
          if (entry.buffer.length < 200) entry.buffer.push(msg);
        }
      },
      onerror(err) {
        console.error(`[Prewarm] Gemini error sessionId=${entry.sessionId}: ${err?.message || JSON.stringify(err)}`);
      },
      onclose(ev) {
        const wasAttached = entry.attached;
        console.log(`[Prewarm] Gemini closed sessionId=${entry.sessionId} code=${ev?.code} attached=${wasAttached}`);
        // If we close pre-attach, mark the entry so attach() falls back to cold.
        if (!wasAttached) {
          entry.geminiClosedPreAttach = true;
        }
      },
    },
  });

  entry.gemini = liveSession;
}

/**
 * Atomically take ownership of a warm session for the bridging WSS handler.
 *
 * Returns the entry on hit, null on miss. Caller MUST check entry.gemini and
 * entry.setupComplete and fall back to a cold connect if either is missing.
 *
 * On hit, the TTL timer is cleared and the entry is removed from the
 * registry — a second attach for the same sessionId returns null.
 */
function attach(sessionId) {
  if (!sessionId) {
    stats.misses += 1;
    return null;
  }
  const entry = registry.get(sessionId);
  if (!entry) {
    stats.misses += 1;
    console.warn(`[Prewarm] MISS sessionId=${sessionId} replica=${REPLICA_ID} — falling back to cold connect`);
    return null;
  }
  if (entry.attached) {
    // Double-attach attempt — Vobiz reconnected the WSS or something. Treat
    // as a miss to force cold connect; the previously-attached one continues.
    stats.misses += 1;
    console.warn(`[Prewarm] DOUBLE ATTACH sessionId=${sessionId} — already taken; returning null`);
    return null;
  }
  if (entry.cancelled) {
    stats.misses += 1;
    console.warn(`[Prewarm] CANCELLED sessionId=${sessionId} (reason=${entry.cancelReason}) — falling back to cold connect`);
    return null;
  }

  // Hit. Mark attached, clear TTL, remove from registry.
  entry.attached = true;
  entry.attachedAt = Date.now();
  if (entry.ttlTimer) { clearTimeout(entry.ttlTimer); entry.ttlTimer = null; }
  registry.delete(sessionId);
  stats.hits += 1;

  const warmAge = entry.attachedAt - entry.warmedAt;
  const setupAge = entry.setupComplete ? (entry.attachedAt - entry.setupCompleteAt) : null;
  console.log(`[Prewarm] HIT sessionId=${sessionId} warmAge=${warmAge}ms setupComplete=${entry.setupComplete} setupAge=${setupAge}ms bufferedMsgs=${entry.buffer.length} unheardAudio=${entry.unheardAudioBytes}b`);
  logStats('after_hit');
  return entry;
}

/**
 * Cancel a warm session without attaching it (no-answer, busy, dial error).
 * Closes the Gemini Live session and removes the entry. Idempotent.
 */
function cancel(sessionId, reason = 'unknown') {
  const entry = registry.get(sessionId);
  if (!entry || entry.cancelled || entry.attached) return false;
  stats.cancelled += 1;
  entry.cancelReason = reason;
  console.log(`[Prewarm] CANCEL sessionId=${sessionId} reason=${reason} warmAge=${Date.now() - entry.warmedAt}ms`);
  _closeAndRemove(entry, reason);
  return true;
}

function _closeAndRemove(entry, reason) {
  entry.cancelled = true;
  entry.cancelReason = reason;
  if (entry.ttlTimer) { clearTimeout(entry.ttlTimer); entry.ttlTimer = null; }
  try { entry.gemini?.close?.(); } catch (e) { /* ignore */ }
  registry.delete(entry.sessionId);
}

/**
 * After attach, the WSS handler calls this to install the live message
 * pump. We drain the buffer (in order) into the listener, then leave the
 * listener wired so future messages flow directly through.
 *
 * Returns the count of messages replayed (useful for telemetry).
 */
function installListener(entry, listener) {
  if (typeof listener !== 'function') throw new Error('installListener: listener must be a function');
  if (entry.listener) throw new Error('installListener: listener already set');
  entry.listener = listener;
  const drained = entry.buffer.length;
  for (const msg of entry.buffer) {
    try { listener(msg); }
    catch (e) { console.error(`[Prewarm] replay error sessionId=${entry.sessionId}: ${e.message}`); }
  }
  entry.buffer.length = 0;
  return drained;
}

function getStats() {
  return { ...stats, size: registry.size, replica: REPLICA_ID };
}

// ── Graceful shutdown: close all warm sessions so Gemini doesn't keep
// billing connections after a redeploy.
function shutdown() {
  console.log(`[Prewarm] shutdown — closing ${registry.size} warm sessions`);
  for (const entry of registry.values()) {
    try { entry.gemini?.close?.(); } catch (_) {}
    if (entry.ttlTimer) clearTimeout(entry.ttlTimer);
  }
  registry.clear();
}

process.on('SIGTERM', shutdown);
process.on('SIGINT',  shutdown);

module.exports = {
  startWarm,
  attach,
  cancel,
  installListener,
  getStats,
  // Surface constants for tests / health endpoints.
  WARM_TTL_MS,
};
