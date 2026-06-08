/**
 * vobizOutbound.js — Vobiz outbound call originator
 *
 * Wraps Vobiz Make-an-Outbound-Call REST API:
 *   POST https://api.vobiz.ai/api/v1/Account/{auth_id}/Call/
 *   Headers: X-Auth-ID, X-Auth-Token
 *   Body:    from, to, answer_url, [machine_detection], [ring_timeout]
 *
 * Docs: https://docs.vobiz.ai/call/make-call
 *
 * Demo usage:
 *   const { dialOutbound } = require('./vobizOutbound');
 *   const { call_uuid } = await dialOutbound({
 *     to:          '+919XXXXXXXXX',
 *     answerUrl:   'https://aivani.in/vobiz/xml-outbound?ctx=abc123',
 *     callerId:    '+918040260668',  // optional; defaults to VOBIZ_PHONE_NUMBER
 *     ringTimeout: 30,                // optional; default 30s
 *   });
 *
 * Built 2026-05-23 for D&V Ather service-reminder demo.
 * Production rollout requires DLT Principal Entity registration and a
 * 1600-series number — see project_vani_dlt_block memory note.
 */

'use strict';

const axios = require('axios');

const VOBIZ_BASE_URL = process.env.VOBIZ_BASE_URL || 'https://api.vobiz.ai/api/v1';

/**
 * Format any Indian phone input into E.164 (+91XXXXXXXXXX).
 * Mirrors normalizeDID in geminiLiveRouter.js but produces +91-prefixed form
 * because Vobiz REST API requires the international form on outbound dial.
 *
 * Rejects (returns null) anything that doesn't normalize to a 10-digit Indian
 * mobile starting with 6-9 — outbound calls to malformed numbers waste credits
 * and trigger spam-flagging on the dialer's reputation score.
 */
function toE164India(raw) {
  if (!raw) return null;
  const digits = String(raw).replace(/\D/g, '');
  let local;
  if (digits.length === 14 && digits.startsWith('0091')) local = digits.slice(4);
  else if (digits.length === 13 && digits.startsWith('091')) local = digits.slice(3);
  else if (digits.length === 12 && digits.startsWith('91')) local = digits.slice(2);
  else if (digits.length === 11 && digits.startsWith('0')) local = digits.slice(1);
  else if (digits.length === 10) local = digits;
  else return null;
  if (!/^[6-9]\d{9}$/.test(local)) return null;
  return `+91${local}`;
}

/**
 * Originate an outbound call via Vobiz.
 *
 * @param {object} opts
 * @param {string} opts.to            Customer phone (E.164 or local 10-digit)
 * @param {string} opts.answerUrl     URL Vobiz hits when call is answered; must return Voice XML
 * @param {string} [opts.callerId]    Override caller ID; defaults to VOBIZ_PHONE_NUMBER
 * @param {number} [opts.ringTimeout] Seconds to ring before giving up (default 30)
 * @param {string} [opts.machineDetection] 'true' | 'hangup' | omit. Default omit — for a
 *                                    demo we want to talk to voicemail too if it picks up,
 *                                    so we can verify the audio path works end-to-end.
 *
 * @returns {Promise<{ call_uuid: string, message: string, api_id: string }>}
 *
 * @throws Error with Vobiz response body attached on .response.data so the
 *         caller can surface specifics in the demo UI (invalid_to, insufficient_balance,
 *         unauthorized_caller_id, etc.) instead of a generic 500.
 */
async function dialOutbound(opts) {
  const authId    = process.env.VOBIZ_ACCOUNT_SID;
  const authToken = process.env.VOBIZ_AUTH_TOKEN;
  if (!authId || !authToken) {
    throw new Error('Vobiz auth missing: set VOBIZ_ACCOUNT_SID and VOBIZ_AUTH_TOKEN');
  }

  const to = toE164India(opts.to);
  if (!to) throw new Error(`Invalid Indian phone number: ${opts.to}`);

  const from = opts.callerId || process.env.VOBIZ_PHONE_NUMBER;
  if (!from) throw new Error('Caller ID missing: set VOBIZ_PHONE_NUMBER or pass opts.callerId');

  if (!opts.answerUrl) throw new Error('answerUrl is required — Vobiz needs a webhook to fetch XML on answer');

  const url = `${VOBIZ_BASE_URL}/Account/${encodeURIComponent(authId)}/Call/`;

  const body = {
    from,
    to,
    answer_url:       opts.answerUrl,
    answer_method:    'POST',
    ring_timeout:     opts.ringTimeout || 30,
    // hangup_url lets us know when the call ends — wire this once we have the route.
    // hangup_url: opts.hangupUrl,
  };
  if (opts.machineDetection) body.machine_detection = opts.machineDetection;

  console.log(`[VobizOut] Dialing ${to} from ${from} → answer=${opts.answerUrl}`);

  try {
    const res = await axios.post(url, body, {
      auth: { username: authId, password: authToken }, // Vobiz also accepts HTTP Basic auth equivalent to X-Auth-ID/X-Auth-Token
      headers: {
        'X-Auth-ID':    authId,
        'X-Auth-Token': authToken,
        'Content-Type': 'application/json',
        'Accept':       'application/json',
      },
      timeout: 15000,
    });
    const data = res.data || {};
    // Vobiz returns either { request_uuid, message, api_id } (queued) or { call_uuid, ... }
    // depending on call category. Normalize the field caller code reads.
    const call_uuid = data.request_uuid || data.call_uuid || data.uuid || null;
    console.log(`[VobizOut] Queued: call_uuid=${call_uuid} message="${data.message || ''}"`);
    return { call_uuid, message: data.message || 'queued', api_id: data.api_id || null, raw: data };
  } catch (err) {
    const status  = err.response?.status;
    const payload = err.response?.data;
    console.error(`[VobizOut] Dial failed status=${status} body=${JSON.stringify(payload)}`);
    const e = new Error(`Vobiz dial failed (${status}): ${payload?.error || payload?.message || err.message}`);
    e.status = status;
    e.body = payload;
    throw e;
  }
}

module.exports = { dialOutbound, toE164India };
