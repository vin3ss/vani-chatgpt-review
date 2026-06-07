/**
 * index-outbound.js — Vani Outbound Service Server
 * aivani.in | Navamitra, Bangalore
 *
 * Runs as a SEPARATE Railway service from index.js (inbound).
 * Same monorepo, same dependencies, same Supabase, same Vobiz API key.
 * Different entry point, different routes, different dyno.
 *
 * What this serves (and ONLY this):
 *   - GET  /health                          → uptime probe + role identifier
 *   - GET  /demo/outbound                   → static demo form HTML
 *   - POST /api/demo/outbound-call          → trigger one outbound call
 *   - POST /vobiz/xml-outbound              → Vobiz webhook on customer pickup
 *   - WSS  /call/outbound                   → audio bridge for outbound calls
 *   - POST /api/dealer/customers/import     → CSV upload for service-reminder roster
 *   - GET  /api/reminder-queue/pending      → manager fetches pending_approval rows
 *   - GET  /api/reminder-queue/by-status    → filter by dialing/completed/no_answer
 *   - POST /api/reminder-queue/approve      → manager approves a batch
 *   - POST /api/reminder-queue/generate     → manual queue regeneration (test)
 *   - POST /api/cron/generate-queues        → daily 9 AM IST queue generation
 *   - POST /api/cron/dispatch-reminders     → every-5-min dispatcher tick
 *
 * What this does NOT serve (lives in index.js / inbound service):
 *   - POST /vobiz/xml                       → inbound call answer XML
 *   - WSS  /call                            → inbound audio bridge
 *   - GET/POST /webhook/whatsapp            → WhatsApp delivery receipts
 *
 * Why split: a runaway dispatcher cron OOM on this service can't kill live
 * inbound calls. Independent scaling, independent rollback, independent
 * crash blast radius. See network specialist report 2026-05-24.
 *
 * Railway service: blissful-stillness-... (renamed: vani-outbound)
 * Start command: node index-outbound.js
 * Required env vars (same as inbound, plus none new):
 *   PORT, SERVER_HOST, GEMINI_API_KEY, GEMINI_MODEL, SUPABASE_URL,
 *   SUPABASE_SERVICE_ROLE_KEY, VOBIZ_AUTH_ID, VOBIZ_AUTH_TOKEN,
 *   VOBIZ_PHONE_NUMBER, CRON_SECRET
 */

'use strict';

require('dotenv').config();

const express = require('express');
const http    = require('http');
const path    = require('path');
const fs      = require('fs');
const { WebSocketServer } = require('ws');

// ─── Anti-regression guard (mirrors index.js) ────────────────────────────────
// Same boot-time guard as the inbound service. If the deleted STT/TTS fallback
// files reappear (botched git pull, stash pop, stale tarball), refuse to boot.
const FORBIDDEN_FALLBACK_FILES = ['callHandler.js', 'googleSTT.js', 'googleTTS.js', 'geminiAgent.js', 'geminiLive.js'];
const resurrected = FORBIDDEN_FALLBACK_FILES.filter((f) => fs.existsSync(path.join(__dirname, f)));
if (resurrected.length > 0) {
  console.error(`[VaniOut] FATAL: STT/TTS fallback files reappeared: ${resurrected.join(', ')}`);
  console.error('[VaniOut] These were intentionally removed 2026-05-13 (Live-only path).');
  process.exit(1);
}

const { dialOutbound, toE164India }       = require('./vobizOutbound');
const { handleCall: handleOutboundCall, VERTICAL: EV_OUTBOUND_VERTICAL } = require('./geminiLive-ev-outbound');
// 2026-06-07 RESET: parallel Adukkala-pattern outbound (caller-first kickoff)
// 2026-06-08 (ChatGPT review P0 #3): import VERTICAL for pre-warm wiring.
const { handleCall: handleOutboundCallFast, VERTICAL: EV_OUTBOUND_FAST_VERTICAL } = require('./geminiLive-ev-outbound-fast');
const { importCustomersCsv }              = require('./csvImporter');
const reminderQueue                       = require('./reminderQueue');
const dispatcher                          = require('./reminderDispatcher');
const { putOutboundCtx, getOutboundCtx, deleteOutboundCtx } = require('./outboundCtx');
const { buildLiveConnectConfig }          = require('./geminiLiveBase');
const warmRegistry                        = require('./warmSessionRegistry');

// ─── Sub-1s Voice Bridge feature flag ─────────────────────────────────────────
// Gated entirely on env LIVE_PREWARM=true. Default off — current production
// behavior is preserved. Flip to true on the warm-session branch deployment
// to enable Gemini Live pre-warming during the dial ringing window.
//
// Target on hits: pickup_to_first_audio_ms ≤ 1000 ms (Vineeth 2026-06-04).
const PREWARM_ENABLED = process.env.LIVE_PREWARM === 'true';
console.log(`[VaniOut] PREWARM: ${PREWARM_ENABLED ? 'ENABLED' : 'disabled'} (set LIVE_PREWARM=true to enable)`);

// ─── Outbound stream codec ────────────────────────────────────────────────────
// 2026-06-07: Vobiz outbound bidirectional Stream silently drops customer
// audio when XML asks for audio/x-l16 (caller PSTN leg is PCMU; Vobiz only
// transcodes bot→customer, not customer→bot). Switching to audio/x-mulaw
// makes Vobiz forward raw PCMU bytes, which we decode on-server via ulaw.js.
//
// Set STREAM_CODEC=mulaw on Railway env to flip. Default linear16 = old.
const STREAM_CONTENT_TYPE = (process.env.STREAM_CODEC || 'linear16').toLowerCase() === 'mulaw'
  ? 'audio/x-mulaw;rate=8000'
  : 'audio/x-l16;rate=8000';
console.log(`[VaniOut] STREAM codec: ${STREAM_CONTENT_TYPE}`);

console.log('[VaniOut] Mode: OUTBOUND-only — service reminders + demo dialer');

const PORT = process.env.PORT || 3000;

const app = express();
app.use(express.json());

// ─── Health (uptime probe + role identifier) ──────────────────────────────────
app.get('/health', (_req, res) => {
  res.json({
    status:  'ok',
    project: 'vani-navamitra',
    role:    'outbound',
    domain:  'aivani.in',
    ts:      new Date().toISOString(),
  });
});

// ─── DEMO / MANUAL TRIGGER ROUTES ─────────────────────────────────────────────
// Build for D&V Ather service-reminder demo (2026-05-23).
//   1. GET  /demo/outbound          → static HTML form to trigger a call
//   2. POST /api/demo/outbound-call → server dials customer via Vobiz
//   3. POST /vobiz/xml-outbound     → Vobiz webhook on customer pickup;
//                                     returns Voice XML bridging to /call/outbound
//   4. WSS  /call/outbound          → loads ctx, dispatches to outbound EV agent

app.get('/demo/outbound', (_req, res) => {
  res.set('Content-Type', 'text/html; charset=utf-8');
  res.sendFile(path.join(__dirname, 'demo-outbound.html'));
});

app.post('/api/demo/outbound-call', async (req, res) => {
  try {
    const b = req.body || {};
    const phone = toE164India(b.customer_phone);
    if (!phone) {
      return res.status(400).json({ error: 'Invalid Indian mobile number (must be +91XXXXXXXXXX or 10 digits starting 6-9).' });
    }

    const ctxId = `ctx-${require('crypto').randomBytes(8).toString('hex')}`;

    putOutboundCtx(ctxId, {
      customer: {
        name:                   b.customer_name || 'Customer',
        phone,
        vehicle_model:          b.vehicle_model || 'Ather 450X',
        registration_no:        b.registration_no || null,
        purchase_date:          b.purchase_date || null,
        last_service_date:      b.last_service_date || null,
        last_service_km:        b.last_service_km ? Number(b.last_service_km) : null,
        next_service_due_date:  b.next_service_due_date || null,
        preferred_language:     b.preferred_language || 'Kannada',
        preferred_language_code:
          /hindi/i.test(b.preferred_language || '')   ? 'hi-IN' :
          /tamil/i.test(b.preferred_language || '')   ? 'ta-IN' :
          /english/i.test(b.preferred_language || '') ? 'en-IN' :
          'kn-IN',
        slots_offered: Array.isArray(b.slots_offered) && b.slots_offered.length
          ? b.slots_offered
          : ['tomorrow 10 AM', 'day-after 4 PM', 'Saturday 11 AM'],
      },
      business: {
        id:        b.business_id || 'client-dv-ather',
        name:      b.business_name || 'D&V Ather Bangalore',
        city:      b.business_city || 'Bangalore',
        language:  'kn-IN',
        type:      'ev_dealership',
        voice_name: b.voice_name || 'Leda',
      },
    });

    const serverHost = process.env.SERVER_HOST || req.headers.host;
    const proto = (serverHost && !serverHost.includes('localhost')) ? 'https' : 'http';

    // ── Sub-1s Voice Bridge: pre-warm Gemini Live during the ring window ──
    // If LIVE_PREWARM=true, mint a UUID per dial (NOT the reminderId — that
    // can be reused across retries; Vineeth guard-rail #1) and start the
    // Gemini Live session immediately so setupComplete fires while the
    // phone is still ringing. The sessionId is embedded in the answer URL
    // so the Vobiz webhook can pass it through to the WSS bridge.
    let outboundSessionId = null;
    if (PREWARM_ENABLED) {
      outboundSessionId = require('crypto').randomUUID();
      try {
        const business = {
          id:        b.business_id || 'client-dv-ather',
          name:      b.business_name || 'D&V Ather Bangalore',
          city:      b.business_city || 'Bangalore',
          language:  'kn-IN',
          type:      'ev_dealership',
          voice_name: b.voice_name || 'Leda',
          _outboundCustomer: {
            name:                   b.customer_name || 'Customer',
            phone,
            vehicle_model:          b.vehicle_model || 'Ather 450X',
            last_service_date:      b.last_service_date || null,
            last_service_km:        b.last_service_km ? Number(b.last_service_km) : null,
            next_service_due_date:  b.next_service_due_date || null,
          },
        };
        warmRegistry.startWarm({
          sessionId:     outboundSessionId,
          business,
          vertical:      EV_OUTBOUND_VERTICAL,
          callId:        null,                  // not known until Vobiz responds
          customerPhone: phone,
          buildConfig:   (biz) => buildLiveConnectConfig(biz, EV_OUTBOUND_VERTICAL),
        });
      } catch (err) {
        // Pre-warm failures must NEVER block the dial. Log + carry on with
        // the cold path; sessionId stays null and the WSS handler will fall
        // back to cold connect.
        console.error(`[DemoOut] prewarm startWarm failed (continuing cold): ${err.message}`);
        outboundSessionId = null;
      }
    }

    const answerQs = new URLSearchParams({ ctx: ctxId });
    if (outboundSessionId) answerQs.set('sessionId', outboundSessionId);
    const answerUrl = `${proto}://${serverHost}/vobiz/xml-outbound?${answerQs.toString()}`;
    const callerId = b.caller_id || process.env.VOBIZ_PHONE_NUMBER || '+918040260668';

    let result;
    try {
      result = await dialOutbound({
        to:          phone,
        callerId,
        answerUrl,
        ringTimeout: 30,
      });
    } catch (err) {
      // Dial failed before Vobiz queued anything. Cancel the warm session
      // immediately so we don't keep paying for an idle Gemini socket.
      if (outboundSessionId) warmRegistry.cancel(outboundSessionId, 'dial_failed');
      throw err;
    }

    console.log(`[DemoOut] Queued ctx=${ctxId} sessionId=${outboundSessionId || '(none)'} to=${phone} caller=${callerId} call_uuid=${result.call_uuid}`);
    return res.json({
      ok:               true,
      ctx_id:           ctxId,
      session_id:       outboundSessionId,
      prewarm_enabled:  PREWARM_ENABLED,
      call_uuid:        result.call_uuid,
      to:               phone,
      caller_id:        callerId,
      message:          result.message,
    });
  } catch (err) {
    console.error('[DemoOut] Trigger failed:', err.message);
    return res.status(500).json({ ok: false, error: err.message, vobiz: err.body || null });
  }
});

// Vobiz webhook — called when the OUTBOUND call is answered by the customer.
app.use('/vobiz/xml-outbound', express.urlencoded({ extended: false }));
app.post('/vobiz/xml-outbound', (req, res) => {
  const ctxId = (req.query.ctx || req.body?.ctx || '').toString();
  // Sub-1s Voice Bridge: sessionId is propagated from the dial endpoint, through
  // Vobiz's answer-URL callback, into the streamUrl query so the WSS bridge can
  // claim the pre-warmed Gemini session.
  const sessionId = (req.query.sessionId || req.body?.sessionId || '').toString() || null;
  const host  = process.env.SERVER_HOST || req.headers.host || 'localhost:3000';
  const protocol = host.includes('localhost') ? 'ws' : 'wss';

  const ctx = getOutboundCtx(ctxId);
  if (!ctx) {
    console.warn(`[VobizXmlOut] Unknown or expired ctx=${ctxId} — hanging up`);
    // Free the warm session if one was opened — caller will never bridge.
    if (sessionId) warmRegistry.cancel(sessionId, 'unknown_ctx');
    res.set('Content-Type', 'text/xml');
    return res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Speak voice="WOMAN" language="en-IN">Sorry, this call could not be completed. Please try again.</Speak>
  <Hangup/>
</Response>`);
  }

  const callId = (req.body?.CallUUID || req.body?.call_uuid || `out-${Date.now()}`).toString();
  console.log(`[VobizXmlOut] Answered ctx=${ctxId} sessionId=${sessionId || '(none)'} CallUUID=${callId} to=${ctx.customer.phone}`);

  const params = new URLSearchParams({ ctx: ctxId, callId });
  if (sessionId) params.set('sessionId', sessionId);
  const rawStreamUrl = `${protocol}://${host}/call/outbound?${params.toString()}`;
  const streamUrl = rawStreamUrl
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');

  // No Vobiz <Speak> — Vineeth confirmed we stay Live-only (no TTS billing
  // surcharge). Latency reduction must come from Gemini-side levers:
  // thinkingBudget=0 (shipped beab33bc), and Gemini session pre-warming
  // (queued as follow-up). The Stream bridges to /call/outbound where
  // runLiveCall() drives the conversation end-to-end.
  res.set('Content-Type', 'text/xml');
  res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Stream bidirectional="true" keepCallAlive="true" contentType="${STREAM_CONTENT_TYPE}">${streamUrl}</Stream>
</Response>`);
});

// ─── PHASE 1: SERVICE REMINDER API ────────────────────────────────────────────
// Endpoints for dealer dashboard + cron jobs. All dealer endpoints require an
// `x-business-id` header for demo. Production: swap for Supabase JWT auth.

function requireBusiness(req, res) {
  const bid = req.header('x-business-id') || req.query.business_id || req.body?.business_id;
  if (!bid) { res.status(400).json({ ok: false, error: 'business_id required (header x-business-id or query param)' }); return null; }
  return String(bid);
}

function requireCronSecret(req, res) {
  const expected = process.env.CRON_SECRET;
  if (!expected) { res.status(503).json({ ok: false, error: 'CRON_SECRET not configured' }); return false; }
  // Accept the secret three ways so any cron platform can talk to us:
  //   1. x-cron-secret header        — what our own scripts use
  //   2. ?secret=... query param     — easy curl from anywhere
  //   3. Authorization: Bearer ...   — Vercel Cron sends this header automatically;
  //                                    we strip the "Bearer " prefix and match.
  // The Vercel cron secret must be set as a Vercel project env var named
  // CRON_SECRET with the SAME value as on the Railway service. Vercel passes
  // it through proxies in the Authorization header by default.
  const bearer = (req.header('authorization') || '').replace(/^Bearer\s+/i, '').trim();
  const given  = req.header('x-cron-secret') || req.query.secret || bearer;
  if (given !== expected) { res.status(401).json({ ok: false, error: 'invalid cron secret' }); return false; }
  return true;
}

// POST /api/dealer/customers/import — bulk + delta CSV upload
app.use('/api/dealer/customers/import', express.text({ type: 'text/csv', limit: '10mb' }));
app.post('/api/dealer/customers/import', async (req, res) => {
  const businessId = requireBusiness(req, res);
  if (!businessId) return;

  let csvText, uploadedBy, source;
  if (typeof req.body === 'string') {
    csvText = req.body;
    uploadedBy = req.header('x-uploaded-by') || 'unknown';
    source = req.query.source || 'csv_import';
  } else {
    csvText    = req.body?.csvText;
    uploadedBy = req.body?.uploadedBy || 'unknown';
    source     = req.body?.source || 'csv_import';
  }
  if (!csvText) return res.status(400).json({ ok: false, error: 'csvText is empty' });

  try {
    const report = await importCustomersCsv({ businessId, csvText, uploadedBy, source });
    console.log(`[CSVImport] biz=${businessId} by=${uploadedBy} ${JSON.stringify(report.summary)}`);
    res.json(report);
  } catch (err) {
    console.error('[CSVImport] error:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/reminder-queue/pending', async (req, res) => {
  const businessId = requireBusiness(req, res);
  if (!businessId) return;
  try {
    const rows = await reminderQueue.listPending(businessId, Number(req.query.limit || 100));
    res.json({ ok: true, rows });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.get('/api/reminder-queue/by-status', async (req, res) => {
  const businessId = requireBusiness(req, res);
  if (!businessId) return;
  const statuses = (req.query.statuses || '').split(',').map(s => s.trim()).filter(Boolean);
  if (statuses.length === 0) return res.status(400).json({ ok: false, error: 'statuses query param required' });
  try {
    const rows = await reminderQueue.listByStatus(businessId, statuses, Number(req.query.limit || 200));
    res.json({ ok: true, rows });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.post('/api/reminder-queue/approve', async (req, res) => {
  const businessId = requireBusiness(req, res);
  if (!businessId) return;
  // Accept BOTH approvedIds (camelCase, original) and approved_ids
  // (snake_case, what the Next.js dashboard sends). 2026-05-26 incident:
  // dashboard was sending snake_case, server rejected with HTTP 400
  // 'approvedIds must be array' because Array.isArray(undefined) === false.
  const body = req.body || {};
  const approvedIds = Array.isArray(body.approvedIds) ? body.approvedIds
                    : Array.isArray(body.approved_ids) ? body.approved_ids
                    : null;
  const approver = body.approver;
  const notes = body.notes;
  if (!Array.isArray(approvedIds)) {
    return res.status(400).json({
      ok: false,
      error: 'approvedIds (or approved_ids) must be an array of UUIDs',
      received_keys: Object.keys(body),
    });
  }
  try {
    const result = await reminderQueue.approveBatch(businessId, approvedIds, {
      ...(approver || {}),
      ipAddress: req.ip,
      userAgent: req.header('user-agent'),
    }, notes);
    console.log(`[Approval] biz=${businessId} approved=${result.approvedCount} skipped=${result.skippedCount}`);
    res.json({ ok: true, ...result });
  } catch (err) {
    console.error('[Approval] error:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/reminder-queue/in-flight — what's currently dispatching + recently done.
// Combined view for the dashboard Dispatch tab so the manager sees the entire
// post-approval lifecycle (approved -> dialing -> completed / no_answer / failed).
app.get('/api/reminder-queue/in-flight', async (req, res) => {
  const businessId = requireBusiness(req, res);
  if (!businessId) return;
  try {
    // approved + dialing = currently in flight
    const inFlight = await reminderQueue.listByStatus(businessId, ['approved','dialing'], 200);
    // outcomes from the last 24 hours
    const recent = await reminderQueue.listByStatus(businessId, ['completed','no_answer','failed','rescheduled','opted_out'], 200);
    res.json({ ok: true, in_flight: inFlight, recent });
  } catch (err) {
    console.error('[InFlight]', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/reminder-queue/retry — flip listed rows back to status='approved'
// so the next dispatcher tick re-dials them. Useful for no_answer/failed rows.
// Body: { ids: string[] } OR { all_failed: true } to retry every no_answer/failed.
app.post('/api/reminder-queue/retry', async (req, res) => {
  const businessId = requireBusiness(req, res);
  if (!businessId) return;
  const body = req.body || {};
  try {
    let q = supabase.from('reminder_queue').update({
      status: 'approved',
      scheduled_for: new Date().toISOString(),
      outcome_notes: null,
    }, { count: 'exact' }).eq('business_id', businessId);
    if (Array.isArray(body.ids) && body.ids.length > 0) {
      q = q.in('id', body.ids);
    } else if (body.all_failed === true) {
      q = q.in('status', ['no_answer','failed']);
    } else {
      return res.status(400).json({ ok: false, error: 'pass { ids: [...] } or { all_failed: true }' });
    }
    const { count, error } = await q;
    if (error) throw error;
    console.log(`[Retry] biz=${businessId} retried=${count}`);
    res.json({ ok: true, retriedCount: count || 0 });
  } catch (err) {
    console.error('[Retry]', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/reminder-queue/reset — bulk move rows back to pending_approval so the
// manager can re-review and re-approve. Body: { scope: 'in_flight' | 'all_active' }
//   - in_flight: only status in (approved, dialing) — wipes a half-finished run
//   - all_active: status in (approved, dialing, no_answer, failed) — full restart
// completed/booked/opted_out are NEVER touched (audit trail preserved).
app.post('/api/reminder-queue/reset', async (req, res) => {
  const businessId = requireBusiness(req, res);
  if (!businessId) return;
  const scope = (req.body?.scope || 'in_flight').toString();
  const targetStatuses = scope === 'all_active'
    ? ['approved','dialing','no_answer','failed']
    : ['approved','dialing'];
  try {
    const { count, error } = await supabase
      .from('reminder_queue')
      .update({
        status: 'pending_approval',
        approved_by: null,
        approved_at: null,
        approval_notes: null,
        scheduled_for: null,
        call_id: null,
        outcome: null,
        outcome_notes: `reset 2026-05-26 via dashboard (scope=${scope})`,
      }, { count: 'exact' })
      .eq('business_id', businessId)
      .in('status', targetStatuses);
    if (error) throw error;
    console.log(`[Reset] biz=${businessId} scope=${scope} reset=${count}`);
    res.json({ ok: true, resetCount: count || 0, scope });
  } catch (err) {
    console.error('[Reset]', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.post('/api/reminder-queue/generate', async (req, res) => {
  const businessId = requireBusiness(req, res);
  if (!businessId) return;
  try {
    const r = await reminderQueue.generateForBusiness(businessId);
    console.log(`[QueueGen] biz=${businessId} inserted=${r.insertedCount}`);
    res.json({ ok: true, ...r });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ─── CRON ENDPOINTS — secured by CRON_SECRET ─────────────────────────────────
// Accept BOTH GET and POST so Vercel Cron (sends GET) and our manual curl
// scripts (use POST) both work. Auth via x-cron-secret header, ?secret= query
// param, or Authorization: Bearer header — see requireCronSecret above.

// Daily 9 AM IST: generate queues for all active businesses
app.all('/api/cron/generate-queues', async (req, res) => {
  if (req.method !== 'GET' && req.method !== 'POST') return res.status(405).json({ ok: false, error: 'method not allowed' });
  if (!requireCronSecret(req, res)) return;
  try {
    const results = await reminderQueue.generateForAll();
    const total = results.reduce((s, r) => s + (r.insertedCount || 0), 0);
    console.log(`[Cron] generate-queues — ${results.length} businesses, ${total} new rows`);
    res.json({ ok: true, total, results });
  } catch (err) {
    console.error('[Cron] generate failed:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Every 5 min: dispatch approved calls
app.all('/api/cron/dispatch-reminders', async (req, res) => {
  if (req.method !== 'GET' && req.method !== 'POST') return res.status(405).json({ ok: false, error: 'method not allowed' });
  if (!requireCronSecret(req, res)) return;
  try {
    const summary = await dispatcher.dispatchOnce({ force: req.query.force === 'true' });
    res.json(summary);
  } catch (err) {
    console.error('[Cron] dispatch failed:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
// ─── ADUKKALA-PATTERN OUTBOUND (2026-06-07 RESET) ─────────────────────────
// ═══════════════════════════════════════════════════════════════════════════
//
// Parallel routes to /api/demo/outbound-call + /vobiz/xml-outbound +
// /call/outbound, EXCEPT they route through the new caller-first vertical
// (kickoffMode='caller-first'). Old paths untouched so we can A/B.
//
//   POST /api/demo/outbound-fast    → dial endpoint (clones old; routes via
//                                     /vobiz/xml-outbound-fast)
//   POST /vobiz/xml-outbound-fast   → Vobiz answers → returns Stream URL
//                                     pointing at /call/outbound-fast
//   WSS  /call/outbound-fast        → dispatches to ev-outbound-fast vertical

app.post('/api/demo/outbound-fast', async (req, res) => {
  try {
    const b = req.body || {};
    const phone = toE164India(b.customer_phone);
    if (!phone) {
      return res.status(400).json({ error: 'Invalid Indian mobile number (must be +91XXXXXXXXXX or 10 digits starting 6-9).' });
    }

    const ctxId = `ctx-${require('crypto').randomBytes(8).toString('hex')}`;

    putOutboundCtx(ctxId, {
      customer: {
        name:                   b.customer_name || 'Customer',
        phone,
        vehicle_model:          b.vehicle_model || 'Ather 450X',
        registration_no:        b.registration_no || null,
        last_service_date:      b.last_service_date || null,
        last_service_km:        b.last_service_km ? Number(b.last_service_km) : null,
        next_service_due_date:  b.next_service_due_date || null,
        preferred_language:     b.preferred_language || 'English',
        preferred_language_code: 'en-IN',
      },
      business: {
        id:         b.business_id || 'client-dv-ather',
        name:       b.business_name || 'D&V Ather Bangalore',
        city:       b.business_city || 'Bangalore',
        language:   'en-IN',
        type:       'ev_dealership',
        voice_name: b.voice_name || 'Leda',
      },
    });

    const serverHost = process.env.SERVER_HOST || req.headers.host;
    const proto = (serverHost && !serverHost.includes('localhost')) ? 'https' : 'http';

    // ── 2026-06-08 (ChatGPT review P0 #3): wire pre-warm into outbound-fast ─
    // Old outbound-fast was cold-only — defeated the purpose of having a
    // warmSessionRegistry. Mint sessionId, startWarm with the FAST vertical,
    // propagate sessionId through xml-outbound-fast → WSS → handleCallFast.
    let outboundSessionId = null;
    if (PREWARM_ENABLED) {
      outboundSessionId = require('crypto').randomUUID();
      try {
        const business = {
          id:         b.business_id || 'client-dv-ather',
          name:       b.business_name || 'D&V Ather Bangalore',
          city:       b.business_city || 'Bangalore',
          language:   'en-IN',
          type:       'ev_dealership',
          voice_name: b.voice_name || 'Leda',
          _outboundCustomer: {
            name:                   b.customer_name || 'Customer',
            phone,
            vehicle_model:          b.vehicle_model || 'Ather 450X',
            last_service_date:      b.last_service_date || null,
            last_service_km:        b.last_service_km ? Number(b.last_service_km) : null,
            next_service_due_date:  b.next_service_due_date || null,
          },
        };
        warmRegistry.startWarm({
          sessionId:     outboundSessionId,
          business,
          vertical:      EV_OUTBOUND_FAST_VERTICAL,
          callId:        null,
          customerPhone: phone,
          buildConfig:   (biz) => buildLiveConnectConfig(biz, EV_OUTBOUND_FAST_VERTICAL),
        });
      } catch (err) {
        console.error(`[DemoOutFast] prewarm startWarm failed (continuing cold): ${err.message}`);
        outboundSessionId = null;
      }
    }

    const answerQs = new URLSearchParams({ ctx: ctxId });
    if (outboundSessionId) answerQs.set('sessionId', outboundSessionId);
    const answerUrl = `${proto}://${serverHost}/vobiz/xml-outbound-fast?${answerQs.toString()}`;
    const callerId = b.caller_id || process.env.VOBIZ_PHONE_NUMBER || '+919262102419';

    let result;
    try {
      result = await dialOutbound({
        to:          phone,
        callerId,
        answerUrl,
        ringTimeout: 30,
      });
    } catch (err) {
      if (outboundSessionId) warmRegistry.cancel(outboundSessionId, 'dial_failed');
      throw err;
    }

    console.log(`[DemoOutFast] Queued ctx=${ctxId} sessionId=${outboundSessionId || '(none)'} to=${phone} caller=${callerId} call_uuid=${result.call_uuid}`);
    return res.json({
      ok:               true,
      mode:             'caller-first',
      ctx_id:           ctxId,
      session_id:       outboundSessionId,
      prewarm_enabled:  PREWARM_ENABLED,
      call_uuid:        result.call_uuid,
      to:               phone,
      caller_id:        callerId,
      message:          result.message,
    });
  } catch (err) {
    console.error('[DemoOutFast] Trigger failed:', err.message);
    return res.status(500).json({ ok: false, error: err.message, vobiz: err.body || null });
  }
});

app.use('/vobiz/xml-outbound-fast', express.urlencoded({ extended: false }));
app.post('/vobiz/xml-outbound-fast', (req, res) => {
  const ctxId = (req.query.ctx || req.body?.ctx || '').toString();
  const sessionId = (req.query.sessionId || req.body?.sessionId || '').toString() || null;
  const host  = process.env.SERVER_HOST || req.headers.host || 'localhost:3000';
  const protocol = host.includes('localhost') ? 'ws' : 'wss';

  const ctx = getOutboundCtx(ctxId);
  if (!ctx) {
    console.warn(`[VobizXmlOutFast] Unknown or expired ctx=${ctxId} — hanging up`);
    if (sessionId) warmRegistry.cancel(sessionId, 'unknown_ctx');
    res.set('Content-Type', 'text/xml');
    return res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Speak voice="WOMAN" language="en-IN">Sorry, this call could not be completed. Please try again.</Speak>
  <Hangup/>
</Response>`);
  }

  const callId = (req.body?.CallUUID || req.body?.call_uuid || `out-fast-${Date.now()}`).toString();
  console.log(`[VobizXmlOutFast] Answered ctx=${ctxId} sessionId=${sessionId || '(none)'} CallUUID=${callId} to=${ctx.customer.phone}`);

  const params = new URLSearchParams({ ctx: ctxId, callId });
  if (sessionId) params.set('sessionId', sessionId);
  const rawStreamUrl = `${protocol}://${host}/call/outbound-fast?${params.toString()}`;
  const streamUrl = rawStreamUrl
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');

  res.set('Content-Type', 'text/xml');
  res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Stream bidirectional="true" keepCallAlive="true" contentType="${STREAM_CONTENT_TYPE}">${streamUrl}</Stream>
</Response>`);
});

// ─── HTTP server + outbound WSS ───────────────────────────────────────────────
const server = http.createServer(app);

// Outbound-fast WSS — caller-first kickoff mode (Adukkala pattern)
const wssOutboundFast = new WebSocketServer({ server, path: '/call/outbound-fast' });
wssOutboundFast.on('connection', (ws, req) => {
  let ctxId = null;
  let callId = null;
  let sessionId = null;
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    ctxId = url.searchParams.get('ctx');
    callId = url.searchParams.get('callId') || `out-fast-${Date.now()}`;
    sessionId = url.searchParams.get('sessionId') || null;
  } catch (_) {
    callId = `out-fast-${Date.now()}`;
  }

  const ctx = ctxId ? getOutboundCtx(ctxId) : null;
  if (!ctx) {
    console.warn(`[VaniOutFast] WS connected without valid ctx=${ctxId} — closing`);
    if (sessionId) warmRegistry.cancel(sessionId, 'ws_missing_ctx');
    try { ws.close(1008, 'missing_or_expired_context'); } catch (_) {}
    return;
  }

  deleteOutboundCtx(ctxId);

  const business = { ...ctx.business, _outboundCustomer: ctx.customer };

  // 2026-06-07 (ChatGPT QA — defer-warm-attach fix):
  //
  // DO NOT call warmRegistry.attach(sessionId) here. Vobiz has connected the
  // WS but has NOT yet sent event:start. Attaching the warm Gemini session
  // synchronously replays the buffered setupComplete through onGeminiMessage
  // BEFORE Vobiz's stream handshake is complete. This makes Vobiz tear the
  // WS down within 1-4ms (observed 2026-06-07: prewarmHit=true →
  // duration_ms=1-4ms, zero audio in either direction, end_reason=
  // caller_hangup; prewarmHit=false → 25-36s healthy call). Cold path
  // accidentally works because Gemini's handshake delay defers the same
  // internal state transition until after Vobiz is ready.
  //
  // Fix: pass the sessionId through to runLiveCall; geminiLiveBase will
  // claim the warm entry lazily, from the event:start handler, once
  // spans.mediaUpAt is set.
  const prewarmSessionId = sessionId || null;
  if (prewarmSessionId) {
    console.log(`[VaniOutFast] PREWARM DEFERRED sessionId=${prewarmSessionId} — will claim after Vobiz event:start`);
  }

  console.log(`[VaniOutFast] WS connected ctx=${ctxId} callId=${callId} sessionId=${sessionId || '(none)'} customer=${ctx.customer.phone} prewarm_session=${!!prewarmSessionId}`);

  handleOutboundCallFast(ws, callId, ctx.customer.phone, business.id, business, { prewarmSessionId })
    .catch((err) => {
      console.error(`[VaniOutFast] outbound-fast agent threw: ${err.message}`);
      try { ws.close(1011, 'agent_error'); } catch (_) {}
    });
});

// Outbound WSS — this is the ONLY WSS this service mounts. Inbound /call WSS
// lives in index.js. Vobiz will only ever upgrade to /call/outbound when the
// answer_url points at THIS service's host.
const wssOutbound = new WebSocketServer({ server, path: '/call/outbound' });
wssOutbound.on('connection', (ws, req) => {
  let ctxId = null;
  let callId = null;
  let sessionId = null;
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    ctxId = url.searchParams.get('ctx');
    callId = url.searchParams.get('callId') || `out-${Date.now()}`;
    sessionId = url.searchParams.get('sessionId') || null;
  } catch (_) {
    callId = `out-${Date.now()}`;
  }

  const ctx = ctxId ? getOutboundCtx(ctxId) : null;
  if (!ctx) {
    console.warn(`[VaniOut] WS connected without valid ctx=${ctxId} — closing`);
    if (sessionId) warmRegistry.cancel(sessionId, 'ws_missing_ctx');
    try { ws.close(1008, 'missing_or_expired_context'); } catch (_) {}
    return;
  }

  // One-shot: delete after read so a replay attack can't reuse a ctx.
  deleteOutboundCtx(ctxId);

  // Inject customer onto a copy of the business object so buildPrompt can read it.
  const business = { ...ctx.business, _outboundCustomer: ctx.customer };

  // 2026-06-07 (ChatGPT QA — defer-warm-attach fix):
  //
  // Same race as /call/outbound-fast above. Pass sessionId through; let
  // geminiLiveBase claim lazily from event:start handler. Do NOT attach here.
  const prewarmSessionId = sessionId || null;
  if (prewarmSessionId) {
    console.log(`[VaniOut] WS bridge — PREWARM DEFERRED sessionId=${prewarmSessionId} — will claim after Vobiz event:start`);
  }

  console.log(`[VaniOut] WS connected ctx=${ctxId} callId=${callId} sessionId=${sessionId || '(none)'} customer=${ctx.customer.phone} vehicle=${ctx.customer.vehicle_model} prewarm_session=${!!prewarmSessionId}`);

  handleOutboundCall(ws, callId, ctx.customer.phone, business.id, business, { prewarmSessionId })
    .catch((err) => {
      console.error(`[VaniOut] outbound agent threw: ${err.message}`);
      try { ws.close(1011, 'agent_error'); } catch (_) {}
    });
});

server.listen(PORT, () => {
  console.log(`[VaniOut] OUTBOUND server running on port ${PORT}`);
  console.log(`[VaniOut] Supabase: ${process.env.SUPABASE_URL ? 'connected' : '(not set)'}`);
  console.log(`[VaniOut] Vobiz: ${process.env.VOBIZ_AUTH_ID ? 'configured' : '(not set)'}`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('[VaniOut] SIGTERM received — shutting down gracefully');
  server.close(() => process.exit(0));
});
