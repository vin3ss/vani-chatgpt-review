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
const { handleCall: handleOutboundCall }  = require('./geminiLive-ev-outbound');
const { importCustomersCsv }              = require('./csvImporter');
const reminderQueue                       = require('./reminderQueue');
const dispatcher                          = require('./reminderDispatcher');
const { putOutboundCtx, getOutboundCtx, deleteOutboundCtx } = require('./outboundCtx');

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
        id:        b.business_id || 'demo-dv-ather',
        name:      b.business_name || 'D&V Ather Bangalore',
        city:      b.business_city || 'Bangalore',
        language:  'kn-IN',
        type:      'ev_dealership',
        voice_name: b.voice_name || 'Leda',
      },
    });

    const serverHost = process.env.SERVER_HOST || req.headers.host;
    const proto = (serverHost && !serverHost.includes('localhost')) ? 'https' : 'http';
    const answerUrl = `${proto}://${serverHost}/vobiz/xml-outbound?ctx=${ctxId}`;
    const callerId = b.caller_id || process.env.VOBIZ_PHONE_NUMBER || '+918040260668';

    const result = await dialOutbound({
      to:          phone,
      callerId,
      answerUrl,
      ringTimeout: 30,
    });

    console.log(`[DemoOut] Queued ctx=${ctxId} to=${phone} caller=${callerId} call_uuid=${result.call_uuid}`);
    return res.json({
      ok:        true,
      ctx_id:    ctxId,
      call_uuid: result.call_uuid,
      to:        phone,
      caller_id: callerId,
      message:   result.message,
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
  const host  = process.env.SERVER_HOST || req.headers.host || 'localhost:3000';
  const protocol = host.includes('localhost') ? 'ws' : 'wss';

  const ctx = getOutboundCtx(ctxId);
  if (!ctx) {
    console.warn(`[VobizXmlOut] Unknown or expired ctx=${ctxId} — hanging up`);
    res.set('Content-Type', 'text/xml');
    return res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Speak voice="WOMAN" language="en-IN">Sorry, this call could not be completed. Please try again.</Speak>
  <Hangup/>
</Response>`);
  }

  const callId = (req.body?.CallUUID || req.body?.call_uuid || `out-${Date.now()}`).toString();
  console.log(`[VobizXmlOut] Answered ctx=${ctxId} CallUUID=${callId} to=${ctx.customer.phone}`);

  const params = new URLSearchParams({ ctx: ctxId, callId });
  const rawStreamUrl = `${protocol}://${host}/call/outbound?${params.toString()}`;
  const streamUrl = rawStreamUrl
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');

  res.set('Content-Type', 'text/xml');
  res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Stream bidirectional="true" keepCallAlive="true" contentType="audio/x-l16;rate=8000">${streamUrl}</Stream>
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
  const { approvedIds, approver, notes } = req.body || {};
  if (!Array.isArray(approvedIds)) return res.status(400).json({ ok: false, error: 'approvedIds must be array' });
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

// ─── HTTP server + outbound WSS ───────────────────────────────────────────────
const server = http.createServer(app);

// Outbound WSS — this is the ONLY WSS this service mounts. Inbound /call WSS
// lives in index.js. Vobiz will only ever upgrade to /call/outbound when the
// answer_url points at THIS service's host.
const wssOutbound = new WebSocketServer({ server, path: '/call/outbound' });
wssOutbound.on('connection', (ws, req) => {
  let ctxId = null;
  let callId = null;
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    ctxId = url.searchParams.get('ctx');
    callId = url.searchParams.get('callId') || `out-${Date.now()}`;
  } catch (_) {
    callId = `out-${Date.now()}`;
  }

  const ctx = ctxId ? getOutboundCtx(ctxId) : null;
  if (!ctx) {
    console.warn(`[VaniOut] WS connected without valid ctx=${ctxId} — closing`);
    try { ws.close(1008, 'missing_or_expired_context'); } catch (_) {}
    return;
  }

  // One-shot: delete after read so a replay attack can't reuse a ctx.
  deleteOutboundCtx(ctxId);

  // Inject customer onto a copy of the business object so buildPrompt can read it.
  const business = { ...ctx.business, _outboundCustomer: ctx.customer };

  console.log(`[VaniOut] WS connected ctx=${ctxId} callId=${callId} customer=${ctx.customer.phone} vehicle=${ctx.customer.vehicle_model}`);

  handleOutboundCall(ws, callId, ctx.customer.phone, business.id, business)
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
