/**
 * whatsapp.js — Meta WhatsApp Cloud API
 *
 * Sends booking confirmations to callers via WhatsApp.
 *
 * Template vs free-text:
 *   - Production: uses approved WhatsApp Business templates (required for
 *     business-initiated messages outside the 24hr customer window).
 *   - Fallback: free-form text (only valid within 24hr of customer contact).
 *   - Set WHATSAPP_USE_TEMPLATES=true to switch to template mode once
 *     your templates are approved on the Meta Business Manager.
 *
 * DLT registration (India TRAI):
 *   Templates must be registered with DLT before use on Indian numbers.
 *
 * Graph API version: configurable via WHATSAPP_API_VERSION env var.
 *   Default: v22.0 (current stable as of May 2025)
 */

'use strict';

const axios = require('axios');

const API_VERSION           = process.env.WHATSAPP_API_VERSION    || 'v22.0';
const WA_BASE               = `https://graph.facebook.com/${API_VERSION}`;
// Platform-level fallback credentials (Navamitra's own WABA)
const PLATFORM_PHONE_ID     = process.env.WHATSAPP_PHONE_NUMBER_ID;
const PLATFORM_ACCESS_TOKEN = process.env.WHATSAPP_ACCESS_TOKEN;
const USE_TEMPLATES         = process.env.WHATSAPP_USE_TEMPLATES === 'true';
const TIMEOUT_MS            = Number(process.env.WHATSAPP_TIMEOUT_MS || 10000);

// Provider selection — 'meta' | 'msg91' | 'vobiz' (BSP wrappers around Meta).
// Default 'meta' for backwards compatibility. Set WHATSAPP_PROVIDER=vobiz on
// Railway to route through Navamitra's Vobiz WABA (single-vendor for voice +
// WhatsApp on the same wallet/invoice — production path as of 2026-05-18).
const WA_PROVIDER           = (process.env.WHATSAPP_PROVIDER || 'meta').toLowerCase();

// — MSG91 BSP (legacy, kept for fallback) —
const MSG91_AUTH_KEY        = process.env.MSG91_AUTH_KEY            || '';
const MSG91_WA_NUMBER       = process.env.MSG91_WHATSAPP_NUMBER     || ''; // your WABA-registered number, digits only e.g. 919876543210
const MSG91_WA_NAMESPACE    = process.env.MSG91_WHATSAPP_NAMESPACE  || ''; // optional, required by some MSG91 template flows

// — Vobiz BSP (current production) —
// Auth/credentials live in Console → Dashboard → API Credentials.
// Channel ID is the Vobiz-side ID for the connected WhatsApp channel
// (NOT the Meta WABA ID — fetch via GET /v1/messaging/channels if unknown).
const VOBIZ_AUTH_ID         = process.env.VOBIZ_AUTH_ID         || '';
const VOBIZ_AUTH_TOKEN      = process.env.VOBIZ_AUTH_TOKEN      || '';
const VOBIZ_CHANNEL_ID      = process.env.VOBIZ_CHANNEL_ID      || '';
const VOBIZ_API_BASE        = process.env.VOBIZ_API_BASE        || 'https://api.vobiz.ai/v1';

// ─── Public API ───────────────────────────────────────────────────────────────

/**
 * Send a booking confirmation after a successful booking.
 *
 * Uses the business's own WhatsApp credentials if configured
 * (wa_phone_number_id + wa_access_token in their businesses row).
 * Falls back to Navamitra's platform WABA if not set.
 *
 * @param {string} toPhone    — caller's phone number (any Indian format)
 * @param {Object} booking    — booking object from geminiAgent / supabase
 * @param {Object} [business] — business config from supabase.getBusinessConfig()
 */
/**
 * Returns a structured result instead of throwing on skips:
 *   { status: 'sent',         messageId }    — message accepted by Meta
 *   { status: 'skipped',      reason }       — bad phone / no credentials configured
 *   { status: 'failed',       reason }       — Meta returned an error
 * Caller (geminiLiveBase) uses status to decide whether to flip
 * bookings.whatsapp_sent=true; skipped/failed must NOT mark the booking
 * as delivered or the dashboard's "delivered" badge becomes a lie.
 */
async function sendWhatsAppConfirmation(toPhone, booking, business = null) {
  const phone = normaliseIndianPhone(toPhone);
  if (!phone) {
    console.warn('[WhatsApp] Invalid phone number, skipping:', toPhone);
    return { status: 'skipped', reason: 'invalid_phone' };
  }

  const message = USE_TEMPLATES
    ? buildTemplateMessage(phone, booking, business)
    : buildTextMessage(phone, formatConfirmationText(booking, business));

  // Route through a BSP wrapper if configured at the platform level. Per-business
  // wa_phone_number_id always wins (B2B2C clients with their own direct-Meta
  // WABA), so BSP routing only kicks in for the Navamitra platform-owned send
  // path. Order matters: vobiz is current production, msg91 is legacy fallback.
  if (!business?.wa_phone_number_id) {
    if (WA_PROVIDER === 'vobiz') return sendViaVobiz(phone, message, business);
    if (WA_PROVIDER === 'msg91') return sendViaMsg91(phone, message, business);
  }

  // ── Direct Meta Cloud API path ─────────────────────────────────────────
  const phoneNumberId = business?.wa_phone_number_id || PLATFORM_PHONE_ID;
  const accessToken   = business?.wa_access_token    || PLATFORM_ACCESS_TOKEN;
  const senderLabel   = business?.wa_phone_number_id ? business.name : 'Navamitra/Vani';

  if (!phoneNumberId || !accessToken) {
    console.log(`[WhatsApp] Not configured (${senderLabel}) — would send to ${phone}:`);
    console.log('[WhatsApp] Message:', formatConfirmationText(booking, business));
    return { status: 'skipped', reason: 'not_configured' };
  }

  try {
    const res = await axios.post(
      `${WA_BASE}/${phoneNumberId}/messages`,
      message,
      {
        headers: {
          Authorization:  `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
        },
        timeout: TIMEOUT_MS,
      }
    );
    const messageId = res.data?.messages?.[0]?.id;
    console.log(`[WhatsApp] Sent via Meta (${senderLabel}) to ${phone}, msgId: ${messageId}`);
    return { status: 'sent', messageId };
  } catch (err) {
    const errMsg = err.response?.data?.error?.message || err.message;
    console.error(`[WhatsApp] Failed via Meta (${senderLabel}) to ${phone}: ${errMsg}`);
    return { status: 'failed', reason: errMsg };
  }
}

/**
 * Send a Meta-shaped payload through MSG91's WhatsApp BSP wrapper.
 *
 * Endpoint:  POST https://control.msg91.com/api/v5/whatsapp/whatsapp-outbound-message/bulk/
 * Headers:   authkey: <MSG91_AUTH_KEY>
 * Body:      { integrated_number, content_type, payload }
 *
 * MSG91 accepts both 'template' and free-text payloads. We translate from the
 * Meta-shaped object we already built so the rest of the code (template
 * builder, text builder) doesn't need to know about MSG91.
 */
async function sendViaMsg91(phone, metaPayload, business = null) {
  if (!MSG91_AUTH_KEY || !MSG91_WA_NUMBER) {
    console.warn('[WhatsApp/MSG91] MSG91_AUTH_KEY or MSG91_WHATSAPP_NUMBER missing — skipping');
    return { status: 'skipped', reason: 'msg91_not_configured' };
  }

  const senderLabel = business?.name || 'Navamitra/Vani';
  const isTemplate  = metaPayload.type === 'template';

  // MSG91's "Outbound Bulk" wraps a Meta-shaped payload.
  // The `to` field inside payload gets replaced by MSG91's per-recipient
  // targeting via the `to_and_components` structure for templates.
  let msg91Body;
  if (isTemplate) {
    const tpl       = metaPayload.template;
    const bodyParts = (tpl.components || []).find(c => c.type === 'body')?.parameters || [];
    // Build "body_1", "body_2", … keys MSG91 expects
    const components = {};
    bodyParts.forEach((p, i) => { components[`body_${i + 1}`] = { type: 'text', value: p.text }; });

    msg91Body = {
      integrated_number: MSG91_WA_NUMBER,
      content_type: 'template',
      payload: {
        messaging_product: 'whatsapp',
        type: 'template',
        template: {
          name: tpl.name,
          language: { code: tpl.language?.code || 'en_US', policy: 'deterministic' },
          ...(MSG91_WA_NAMESPACE ? { namespace: MSG91_WA_NAMESPACE } : {}),
          to_and_components: [{ to: [phone], components }],
        },
      },
    };
  } else {
    // Free-text message (only works inside the 24hr customer-initiated window)
    msg91Body = {
      integrated_number: MSG91_WA_NUMBER,
      content_type: 'text',
      payload: {
        messaging_product: 'whatsapp',
        to: phone,
        type: 'text',
        text: { body: metaPayload.text.body },
      },
    };
  }

  try {
    const res = await axios.post(
      'https://control.msg91.com/api/v5/whatsapp/whatsapp-outbound-message/bulk/',
      msg91Body,
      {
        headers: {
          'Content-Type': 'application/json',
          'Accept':       'application/json',
          'authkey':      MSG91_AUTH_KEY,
        },
        timeout: TIMEOUT_MS,
      }
    );
    const requestId = res.data?.request_id || res.data?.message || 'unknown';
    console.log(`[WhatsApp/MSG91] Sent (${senderLabel}) to ${phone}, requestId: ${requestId}`);
    return { status: 'sent', messageId: requestId };
  } catch (err) {
    const errBody = err.response?.data;
    const errMsg  = errBody?.message || errBody?.error || err.message;
    console.error(`[WhatsApp/MSG91] Failed (${senderLabel}) to ${phone}:`, errMsg, errBody || '');
    return { status: 'failed', reason: errMsg };
  }
}

/**
 * Send a Meta-shaped payload through Vobiz's WhatsApp BSP wrapper.
 *
 * Endpoint:  POST https://api.vobiz.ai/v1/messaging/messages
 * Headers:   X-Auth-ID: <VOBIZ_AUTH_ID>, X-Auth-Token: <VOBIZ_AUTH_TOKEN>
 * Body:      { channel_id, to, type, <type-specific object> }
 *
 * Vobiz proxies directly to Meta, so the template/text sub-objects are
 * Meta-shape compatible — only the top-level envelope differs (channel_id
 * replaces messaging_product). Same wallet as Vobiz voice billing.
 *
 * Common error codes (from docs):
 *   INVALID_NUMBER         — phone not on WhatsApp
 *   INVALID_CHANNEL        — VOBIZ_CHANNEL_ID stale / channel disconnected
 *   TEMPLATE_NOT_FOUND     — template not yet approved by Meta
 *   RATE_LIMIT_EXCEEDED    — Meta tier 1/2/3 cap hit
 */
async function sendViaVobiz(phone, metaPayload, business = null) {
  if (!VOBIZ_AUTH_ID || !VOBIZ_AUTH_TOKEN || !VOBIZ_CHANNEL_ID) {
    console.warn('[WhatsApp/Vobiz] VOBIZ_AUTH_ID / VOBIZ_AUTH_TOKEN / VOBIZ_CHANNEL_ID missing — skipping');
    return { status: 'skipped', reason: 'vobiz_not_configured' };
  }

  const senderLabel = business?.name || 'Navamitra/Vani';
  const isTemplate  = metaPayload.type === 'template';

  // Vobiz envelope: { channel_id, to, type, <subobject> }. The template/text
  // subobjects are byte-identical to Meta Cloud API (Vobiz proxies upstream).
  const vobizBody = isTemplate
    ? {
        channel_id: VOBIZ_CHANNEL_ID,
        to:         phone,
        type:       'template',
        template:   metaPayload.template,
      }
    : {
        channel_id: VOBIZ_CHANNEL_ID,
        to:         phone,
        type:       'text',
        text:       metaPayload.text,
      };

  try {
    const res = await axios.post(
      `${VOBIZ_API_BASE}/messaging/messages`,
      vobizBody,
      {
        headers: {
          'X-Auth-ID':    VOBIZ_AUTH_ID,
          'X-Auth-Token': VOBIZ_AUTH_TOKEN,
          'Content-Type': 'application/json',
          'Accept':       'application/json',
        },
        timeout: TIMEOUT_MS,
      }
    );
    const messageId = res.data?.id || res.data?.message_id || 'unknown';
    const status    = res.data?.status || 'sent';
    console.log(`[WhatsApp/Vobiz] Sent (${senderLabel}) to ${phone}, msgId: ${messageId}, status: ${status}`);
    return { status: 'sent', messageId };
  } catch (err) {
    const errBody = err.response?.data;
    const errCode = errBody?.error?.code || errBody?.code;
    const errMsg  = errBody?.error?.message || errBody?.message || err.message;
    const reason  = errCode ? `${errCode}: ${errMsg}` : errMsg;
    console.error(`[WhatsApp/Vobiz] Failed (${senderLabel}) to ${phone}:`, reason, errBody || '');
    return { status: 'failed', reason };
  }
}

/**
 * Send a free-form text message (owner notifications, follow-ups).
 * Only works within 24hr of customer initiating contact.
 * Routes through Vobiz if WHATSAPP_PROVIDER=vobiz, else MSG91, else direct Meta.
 */
async function sendTextMessage(toPhone, text) {
  const phone = normaliseIndianPhone(toPhone);
  if (!phone) return;

  const metaPayload = buildTextMessage(phone, text);

  if (WA_PROVIDER === 'vobiz') {
    return sendViaVobiz(phone, metaPayload, null);
  }
  if (WA_PROVIDER === 'msg91') {
    return sendViaMsg91(phone, metaPayload, null);
  }

  if (!PLATFORM_PHONE_ID || !PLATFORM_ACCESS_TOKEN) {
    console.warn('[WhatsApp/text] Platform credentials missing — skipping');
    return;
  }

  try {
    await axios.post(
      `${WA_BASE}/${PLATFORM_PHONE_ID}/messages`,
      metaPayload,
      {
        headers: {
          Authorization:  `Bearer ${PLATFORM_ACCESS_TOKEN}`,
          'Content-Type': 'application/json',
        },
        timeout: TIMEOUT_MS,
      }
    );
  } catch (err) {
    const errMsg = err.response?.data?.error?.message || err.message;
    console.error('[WhatsApp/text] Failed:', errMsg);
  }
}

// ─── Message builders ─────────────────────────────────────────────────────────

function buildTextMessage(phone, text) {
  return {
    messaging_product: 'whatsapp',
    to:   phone,
    type: 'text',
    text: { body: text },
  };
}

/**
 * Build a WhatsApp template message payload.
 *
 * Template names + parameter mappings must match what is approved in Meta.
 * Until Vani's own templates clear DLT review we use Meta's pre-approved
 * UTILITY sample template `jaspers_market_order_confirmation_v1`, which
 * has 3 body params: customer_name, order_id, date.
 *
 * Env overrides:
 *   WA_TEMPLATE_SALON / WA_TEMPLATE_FOOD / WA_TEMPLATE_EV / WA_TEMPLATE_SUPERMARKET
 *   WA_TEMPLATE_LANGUAGE     (default 'en_US' — must match the approved
 *                             template's language; 'kn' is rejected by Meta
 *                             until our Kannada templates are approved)
 *
 * Param shape for `jaspers_market_order_confirmation_v1` (all verticals
 * route through it until vertical-specific templates clear approval):
 *   1. customer_name      → booking.caller_name (or "Customer")
 *   2. order/booking id   → short human-friendly ID
 *   3. date               → IST date formatted "May 13, 2026"
 */
const DEFAULT_TEMPLATE   = 'jaspers_market_order_confirmation_v1';
const TEMPLATE_LANGUAGE  = process.env.WA_TEMPLATE_LANGUAGE || 'en_US';

function shortBookingId(booking) {
  // Prefer the last 6 chars of the DB id; fall back to a date-coded id.
  const id = String(booking.id || booking.booking_id || '');
  if (id.length >= 6) return `VANI-${id.slice(-6).toUpperCase()}`;
  return `VANI-${Date.now().toString().slice(-6)}`;
}

function formatBookingDate(booking) {
  // Booking date may arrive as ISO "2026-05-13", a JS Date, or undefined.
  // Render as "May 13, 2026" — Meta's sample template expects a human string.
  const raw = booking.booking_date || booking.date;
  if (!raw) return 'soon';
  const d = raw instanceof Date ? raw : new Date(String(raw));
  if (Number.isNaN(d.getTime())) return String(raw);
  return d.toLocaleDateString('en-IN', {
    year: 'numeric', month: 'long', day: 'numeric', timeZone: 'Asia/Kolkata',
  });
}

function templateNameFor(type) {
  if (type === 'salon_appointment')  return process.env.WA_TEMPLATE_SALON       || DEFAULT_TEMPLATE;
  if (type === 'food_order')         return process.env.WA_TEMPLATE_FOOD        || DEFAULT_TEMPLATE;
  if (type === 'supermarket_order')  return process.env.WA_TEMPLATE_SUPERMARKET || DEFAULT_TEMPLATE;
  if (type === 'ev_appointment')     return process.env.WA_TEMPLATE_EV          || DEFAULT_TEMPLATE;
  return DEFAULT_TEMPLATE;
}

function buildTemplateMessage(phone, booking, business = null) {
  const templateName = templateNameFor(booking.type);
  const parameters   = buildTemplateParams(templateName, booking, business);

  return {
    messaging_product: 'whatsapp',
    to:   phone,
    type: 'template',
    template: {
      name:     templateName,
      language: { code: TEMPLATE_LANGUAGE },
      components: [{ type: 'body', parameters }],
    },
  };
}

/**
 * Per-template parameter mapping. The shape of {{N}} variables must match
 * exactly what was approved by Meta — adding/removing params here without
 * resubmitting the template will cause TEMPLATE_PARAM_COUNT_MISMATCH at send.
 *
 * Add a new branch here for each new approved template.
 */
function buildTemplateParams(templateName, booking, business = null) {
  // order_confirmation_restaurant — 5 params: name, restaurant, items, total, eta
  // Use this for FOOD ORDERS (delivery/takeaway). Set WA_TEMPLATE_FOOD env var
  // to this template name to route restaurant food_order bookings through it.
  if (templateName === 'order_confirmation_restaurant') {
    const items = Array.isArray(booking.items) ? booking.items : [];
    const itemsStr = items.length
      ? items.map((i) => `${i.qty || 1}x ${i.name || 'item'}`).join(', ')
      : 'your items';
    const total = booking.total || booking.order_total ||
      items.reduce((sum, i) => sum + ((i.qty || 1) * (i.price || 0)), 0);
    const totalStr = total ? String(total) : 'TBD';
    const eta = booking.eta ||
      (booking.order_type === 'delivery' ? 'Delivery in 30-45 minutes'
                                         : 'Ready in 20 minutes for takeaway');
    return [
      { type: 'text', text: booking.caller_name || 'Customer'         },
      { type: 'text', text: business?.name      || 'our restaurant'   },
      { type: 'text', text: itemsStr                                  },
      { type: 'text', text: totalStr                                  },
      { type: 'text', text: eta                                       },
    ];
  }

  // booking_confirmation_restaurant — 4 params: name, restaurant, party, datetime
  // Use this for TABLE RESERVATIONS only.
  if (templateName === 'booking_confirmation_restaurant') {
    return [
      { type: 'text', text: booking.caller_name || 'Customer' },
      { type: 'text', text: business?.name || 'our restaurant' },
      { type: 'text', text: String(booking.party_size || booking.guests || 2) },
      { type: 'text', text: formatBookingDateTime(booking) },
    ];
  }
  // Legacy jaspers_market_order_confirmation_v1 — 3 params: name, id, date
  return [
    { type: 'text', text: booking.caller_name || 'Customer' },
    { type: 'text', text: shortBookingId(booking) },
    { type: 'text', text: formatBookingDate(booking) },
  ];
}

/**
 * Format booking date + time into a single human string for templates that
 * combine them into one {{var}} (e.g. "Mon, 18 May at 8:00 PM").
 */
function formatBookingDateTime(booking) {
  const raw = booking.booking_date || booking.date;
  if (!raw) {
    return booking.time ? `today at ${booking.time}` : 'soon';
  }
  const d = raw instanceof Date ? raw : new Date(String(raw));
  if (Number.isNaN(d.getTime())) {
    return [booking.date, booking.time].filter(Boolean).join(' at ') || 'soon';
  }
  const dateStr = d.toLocaleDateString('en-IN', {
    weekday: 'short', day: 'numeric', month: 'short', timeZone: 'Asia/Kolkata',
  });
  return booking.time && /\d/.test(String(booking.time))
    ? `${dateStr} at ${booking.time}`
    : dateStr;
}

// ─── Confirmation text formatter ──────────────────────────────────────────────

function formatConfirmationText(booking, business = null) {
  // Show the business's own name if available, else platform name
  const bizName = business?.name || 'aivani.in';
  const callBackNumber = business?.whatsapp_number || business?.owner_phone || '';
  const callBackLine = callBackNumber
    ? `\nTo reschedule, call ${callBackNumber}.`
    : `\nNeed to reschedule? Call back anytime.`;

  if (booking.type === 'salon_appointment') {
    return (
      `✅ *Booking Confirmed*\n` +
      `🏪 ${bizName}\n\n` +
      `💇 Service: ${booking.service}\n` +
      `📅 Date: ${booking.date}\n` +
      `🕐 Time: ${booking.time}\n` +
      (booking.stylist ? `💁 Stylist: ${booking.stylist}\n` : '') +
      (booking.caller_name ? `👤 Name: ${booking.caller_name}\n` : '') +
      callBackLine
    );
  }

  if (booking.type === 'food_order') {
    const itemList = (booking.items || [])
      .map((i) => `  • ${i.qty}x ${i.name}${i.special_instructions ? ` (${i.special_instructions})` : ''}`)
      .join('\n');
    return (
      `✅ *Order Received*\n` +
      `🏪 ${bizName}\n\n` +
      `🍽️ Items:\n${itemList}\n\n` +
      `📦 Type: ${booking.order_type}\n` +
      (booking.delivery_address ? `📍 Address: ${booking.delivery_address}\n` : '') +
      `\nWe'll call if there are any issues.`
    );
  }

  if (booking.type === 'ev_appointment') {
    return (
      `✅ *Appointment Confirmed*\n` +
      `🏪 ${bizName}\n\n` +
      `🚗 ${booking.appointment_type === 'test_ride' ? 'Test Ride' : 'Service Appointment'}\n` +
      (booking.ev_model ? `🔋 Model: ${booking.ev_model}\n` : '') +
      `📅 Date: ${booking.date}\n` +
      (booking.time ? `🕐 Time: ${booking.time}\n` : '') +
      (booking.caller_name ? `👤 Name: ${booking.caller_name}\n` : '') +
      `\nSee you soon!`
    );
  }

  return `✅ Booking confirmed — ${bizName}\nDetails: ${JSON.stringify(booking)}`;
}

// ─── Phone normalisation ──────────────────────────────────────────────────────

/**
 * Normalise an Indian phone number to E.164 (+91XXXXXXXXXX).
 * Explicitly named to make the India-only scope clear.
 * For multi-country support, pass country code from business config.
 */
function normaliseIndianPhone(phone) {
  if (!phone) return null;
  let p = String(phone).replace(/[\s\-().]/g, '');
  if (p.startsWith('0'))    p = '+91' + p.slice(1);
  if (p.startsWith('91') && !p.startsWith('+')) p = '+' + p;
  if (!p.startsWith('+'))   p = '+91' + p;
  if (!/^\+91\d{10}$/.test(p)) return null; // must be exactly +91 + 10 digits
  return p;
}

module.exports = {
  sendWhatsAppConfirmation,
  sendTextMessage,
  normaliseIndianPhone,
  formatConfirmationText,
};
