/**
 * reminderDispatcher.js — Dials approved reminder_queue rows via Vobiz.
 *
 * Invoked by:
 *   - POST /api/cron/dispatch-reminders  (external cron every 5 min)
 *   - setInterval in index.js if DISPATCHER_INTERVAL_MS env set (single-instance Railway)
 *
 * Guarantees:
 *   - Time-of-day gate: 11:00–19:00 IST (Divya's pickup windows)
 *   - Day-of-week gate: skip Mon, Sat (Divya's research)
 *   - Concurrency cap: 3 dialing per business at any moment (Vobiz CPS=1, channels=3)
 *   - Atomic claim: markDialing() uses a conditional UPDATE so two workers
 *     never both dial the same row
 *   - Failure isolation: one row failing never blocks others
 *
 * Built 2026-05-23 — Phase 1.
 */

'use strict';

const crypto = require('crypto');
const { dialOutbound, toE164India } = require('./vobizOutbound');
const { putOutboundCtx } = require('./outboundCtx');
const { pickDispatchable, markDialing, recordOutcome } = require('./reminderQueue');
const { supabase } = require('./supabase');
// Sub-1s Voice Bridge — pre-warm imports (no-op when LIVE_PREWARM != true)
const warmRegistry = require('./warmSessionRegistry');
const { buildLiveConnectConfig } = require('./geminiLiveBase');
const { VERTICAL: EV_OUTBOUND_VERTICAL } = require('./geminiLive-ev-outbound');
const PREWARM_ENABLED = process.env.LIVE_PREWARM === 'true';

// ─── Time gates ───────────────────────────────────────────────────────────────
const IST_OFFSET_MIN = 330; // +05:30
function nowIst() {
  const utcMs = Date.now();
  return new Date(utcMs + IST_OFFSET_MIN * 60 * 1000);
}

// 0=Sun 1=Mon 2=Tue 3=Wed 4=Thu 5=Fri 6=Sat in UTC; ISTtoday() reads from the IST clock.
// We skip Monday (1) and Saturday (6) — Divya's data shows worst pickup rates.
const SKIP_DOW = new Set([1, 6]);

// Default window: 11:00–19:00 IST. Override via env (HHmm-HHmm) for late-night testing.
function withinWindow() {
  const w = (process.env.DISPATCH_WINDOW_IST || '1100-1900').match(/^(\d{2})(\d{2})-(\d{2})(\d{2})$/);
  const hStart = w ? parseInt(w[1], 10) : 11;
  const mStart = w ? parseInt(w[2], 10) : 0;
  const hEnd   = w ? parseInt(w[3], 10) : 19;
  const mEnd   = w ? parseInt(w[4], 10) : 0;
  const ist = nowIst();
  // Important: getUTC* on the shifted Date gives us IST clock components.
  const hh = ist.getUTCHours();
  const mm = ist.getUTCMinutes();
  const cur = hh * 60 + mm;
  const start = hStart * 60 + mStart;
  const end   = hEnd * 60 + mEnd;
  return cur >= start && cur < end;
}

function withinDay() {
  if (process.env.DISPATCH_BYPASS_DOW === 'true') return true; // for testing
  const dow = nowIst().getUTCDay();
  return !SKIP_DOW.has(dow);
}

// ─── Concurrency cap ──────────────────────────────────────────────────────────
async function dialingCountForBusiness(businessId) {
  const { count, error } = await supabase
    .from('reminder_queue')
    .select('id', { count: 'exact', head: true })
    .eq('business_id', businessId)
    .eq('status', 'dialing');
  if (error) {
    console.error(`[Dispatcher] concurrency check failed: ${error.message}`);
    return 0;
  }
  return count || 0;
}

// ─── Main tick ────────────────────────────────────────────────────────────────
/**
 * Single dispatch tick. Picks up to N rows, dials each.
 * Returns a summary so the cron caller can log/monitor.
 */
async function dispatchOnce(opts = {}) {
  const startedAt = Date.now();
  const force = opts.force === true;

  if (!force && !withinDay())    return { ok: true, skipped: 'day_of_week_off',    ts: startedAt };
  if (!force && !withinWindow()) return { ok: true, skipped: 'outside_window',      ts: startedAt };

  const MAX_PER_TICK   = Number(opts.maxPerTick   ?? process.env.DISPATCH_MAX_PER_TICK   ?? 10);
  const MAX_CONCURRENT = Number(opts.maxConcurrent ?? process.env.DISPATCH_MAX_CONCURRENT ?? 3);

  const rows = await pickDispatchable(opts.businessId || null, MAX_PER_TICK);
  if (rows.length === 0) return { ok: true, dialed: 0, ts: startedAt };

  const dialed   = [];
  const skipped  = [];
  const failed   = [];

  // Group by business so we can enforce per-business concurrency.
  const byBiz = rows.reduce((acc, r) => {
    (acc[r.business_id] ||= []).push(r);
    return acc;
  }, {});

  for (const [businessId, bizRows] of Object.entries(byBiz)) {
    const inFlight = await dialingCountForBusiness(businessId);
    const headroom = Math.max(0, MAX_CONCURRENT - inFlight);
    if (headroom === 0) {
      bizRows.forEach(r => skipped.push({ id: r.id, reason: 'concurrency_cap', businessId }));
      continue;
    }

    for (const r of bizRows.slice(0, headroom)) {
      // Atomic claim — only one worker wins this row.
      const claimed = await markDialing(r.id, null);
      if (!claimed) { skipped.push({ id: r.id, reason: 'claim_race' }); continue; }

      try {
        await dialOneRow(r);
        dialed.push({ id: r.id, customer: r.customer?.phone });
      } catch (err) {
        console.error(`[Dispatcher] dial failed row=${r.id}: ${err.message}`);
        failed.push({ id: r.id, error: err.message });
        // Record outcome=failed so we don't keep trying forever.
        try { await recordOutcome(r.id, 'failed', err.message); } catch (_) {}
      }
    }
  }

  return {
    ok: true,
    dialed:  dialed.length,
    skipped: skipped.length,
    failed:  failed.length,
    detail:  { dialed, skipped, failed },
    ts:      startedAt,
    durationMs: Date.now() - startedAt,
  };
}

/**
 * Dial one queue row. Builds ctx, stashes in outboundCtx, posts to Vobiz.
 */
async function dialOneRow(row) {
  const phone = toE164India(row.customer?.phone);
  if (!phone) throw new Error(`invalid phone on customer ${row.customer?.id}: ${row.customer?.phone}`);

  // Build customer context exactly as the demo endpoint does. Same WSS handler
  // reads this, same outbound EV agent uses it.
  const ctxId = `ctx-${crypto.randomBytes(8).toString('hex')}`;
  const slots = Array.isArray(row.proposed_slots) && row.proposed_slots.length
    ? row.proposed_slots
    : ['tomorrow 10 AM', 'day-after 4 PM', 'Saturday 11 AM'];

  // Look up business name + city for the prompt. Cached implicitly by Supabase
  // PostgREST; this is one extra round-trip per call but is fine at <100 calls/day.
  let business = { id: row.business_id, name: 'Your dealership', city: 'Bangalore', language: 'kn-IN', voice_name: 'Leda' };
  try {
    const { data: biz } = await supabase
      .from('businesses')
      .select('id, name, city, language, voice_name')
      .eq('id', row.business_id)
      .maybeSingle();
    if (biz) business = { ...business, ...biz };
  } catch (_) { /* fall through with defaults */ }

  putOutboundCtx(ctxId, {
    customer: {
      id:                       row.customer?.id,
      name:                     row.customer?.name || 'Customer',
      phone,
      vehicle_model:            row.vehicle?.model,
      registration_no:          row.vehicle?.registration_no,
      purchase_date:            null,
      last_service_date:        row.vehicle?.last_service_date,
      last_service_km:          row.vehicle?.last_service_km,
      next_service_due_date:    row.service_due_date || row.vehicle?.next_service_due_date,
      preferred_language:       row.customer?.preferred_language || 'Kannada',
      preferred_language_code:  row.customer?.preferred_language_code || 'kn-IN',
      slots_offered:            slots,
    },
    business,
    queueId: row.id,  // dispatcher annotation — WSS handler ignores extra fields
  });

  const serverHost = process.env.SERVER_HOST;
  if (!serverHost) throw new Error('SERVER_HOST env var not set — Vobiz cannot reach answer URL');
  const proto = serverHost.includes('localhost') ? 'http' : 'https';

  // ── Sub-1s Voice Bridge — pre-warm Gemini Live during ring window ──────
  let outboundSessionId = null;
  if (PREWARM_ENABLED) {
    outboundSessionId = crypto.randomUUID();
    try {
      const warmBusiness = {
        ...business,
        _outboundCustomer: {
          name:                  row.customer?.name || 'Customer',
          phone,
          vehicle_model:         row.vehicle?.model,
          last_service_date:     row.vehicle?.last_service_date,
          last_service_km:       row.vehicle?.last_service_km,
          next_service_due_date: row.service_due_date || row.vehicle?.next_service_due_date,
        },
      };
      warmRegistry.startWarm({
        sessionId:     outboundSessionId,
        business:      warmBusiness,
        vertical:      EV_OUTBOUND_VERTICAL,
        callId:        null,
        customerPhone: phone,
        buildConfig:   (biz) => buildLiveConnectConfig(biz, EV_OUTBOUND_VERTICAL),
      });
    } catch (err) {
      console.error(`[Dispatcher] prewarm startWarm failed row=${row.id}: ${err.message}`);
      outboundSessionId = null;
    }
  }

  const answerQs = new URLSearchParams({ ctx: ctxId });
  if (outboundSessionId) answerQs.set('sessionId', outboundSessionId);
  const answerUrl = `${proto}://${serverHost}/vobiz/xml-outbound?${answerQs.toString()}`;

  let result;
  try {
    result = await dialOutbound({
      to:          phone,
      callerId:    process.env.VOBIZ_PHONE_NUMBER,
      answerUrl,
      ringTimeout: 30,
    });
  } catch (err) {
    // Dial rejected — release the warm session immediately so we don't
    // hold an idle Gemini socket open for 90s.
    if (outboundSessionId) warmRegistry.cancel(outboundSessionId, 'dial_failed');
    throw err;
  }

  // Update row with call_uuid so the dashboard can correlate.
  if (result.call_uuid) {
    try {
      await supabase
        .from('reminder_queue')
        .update({ call_id: null /* will be set when calls row exists */, outcome_notes: `vobiz_uuid=${result.call_uuid}` })
        .eq('id', row.id);
    } catch (_) { /* non-fatal */ }
  }

  console.log(`[Dispatcher] dialed queue=${row.id} customer=${phone} call_uuid=${result.call_uuid}`);
  return result;
}

// ─── Optional in-process scheduler ────────────────────────────────────────────
// If DISPATCHER_INTERVAL_MS is set, register a setInterval that runs dispatchOnce.
// Otherwise rely on external cron hitting /api/cron/dispatch-reminders.
let scheduledTimer = null;
function startScheduler() {
  const ms = Number(process.env.DISPATCHER_INTERVAL_MS || 0);
  if (!ms || scheduledTimer) return;
  scheduledTimer = setInterval(() => {
    dispatchOnce().catch(err => console.error('[Dispatcher] tick error:', err.message));
  }, ms);
  scheduledTimer.unref?.();
  console.log(`[Dispatcher] in-process scheduler enabled — every ${ms}ms`);
}

module.exports = { dispatchOnce, startScheduler, withinDay, withinWindow };
