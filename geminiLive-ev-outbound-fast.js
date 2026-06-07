/**
 * geminiLive-ev-outbound-fast.js — Vani EV Outbound (Adukkala Pattern)
 *
 * ───────────────────────────────────────────────────────────────────────────
 * BUILT 2026-06-07 — RESET attempt after our incremental fixes never closed
 * the ~3s outbound TTFT gap. Strategy: clone the architecture that ALREADY
 * works on inbound (Adukkala restaurant — 819ms pickup-to-first-audio in
 * production) and adapt it for outbound EV service reminders.
 *
 * KEY DIFFERENCE FROM geminiLive-ev-outbound.js:
 *   This vertical sets kickoffMode='caller-first'. The base engine will NOT
 *   send a synthetic [call started] / SAY_NOW_EXACTLY trigger when the
 *   Gemini session becomes ready. Instead it stays silent and waits for
 *   the CALLER to say "Hello?" — exactly like inbound. Gemini's VAD then
 *   triggers a natural reply.
 *
 *   Most people DO say "Hello?" when they pick up an unexpected call. That
 *   "Hello?" runs in parallel with Gemini's setup time, so the perceived
 *   silence ≈ zero. This is why Adukkala feels instant.
 *
 *   For the ~10-20% of callers who pick up silently, a 1.5s fallback timer
 *   in the base engine (KICKOFF_FALLBACK_MS) force-fires the greeting so
 *   the call never dead-airs.
 *
 * ROUTES:
 *   - WSS  /call/outbound-fast      → uses this vertical
 *   - POST /api/demo/outbound-fast  → demo dial endpoint (mirrors current demo)
 *
 * The OLD outbound flow at /call/outbound + /api/demo/outbound-call is
 * untouched. Both can run side-by-side for A/B comparison.
 */

'use strict';

const crypto = require('crypto');
const { runLiveCall } = require('./geminiLiveBase');
const { upsertBooking } = require('./supabase');

// ─── Language labels (Bangalore-friendly defaults) ────────────────────────────
const LANG_LABELS = {
  'en-IN': { name: 'Indian English', greeting: 'Hello',     region: 'India' },
  'kn-IN': { name: 'Kannada',        greeting: 'Namaskara', region: 'Karnataka' },
  'hi-IN': { name: 'Hindi',          greeting: 'Namaste',   region: 'North India' },
  'ta-IN': { name: 'Tamil',          greeting: 'Vanakkam',  region: 'Tamil Nadu' },
  'ml-IN': { name: 'Malayalam',      greeting: 'Namaskaram', region: 'Kerala' },
};

function renderCustomerBlock(c) {
  if (!c) return '(no customer context — speak generically)';
  const lines = [];
  if (c.name)              lines.push(`Customer name: ${c.name}`);
  if (c.phone)             lines.push(`Phone: ${c.phone}`);
  if (c.vehicle_model)     lines.push(`Vehicle: ${c.vehicle_model}`);
  if (c.registration_no)   lines.push(`Registration: ${c.registration_no}`);
  if (c.last_service_date) lines.push(`Last service: ${c.last_service_date}`);
  if (c.last_service_km)   lines.push(`Last service km: ${c.last_service_km}`);
  if (c.next_service_due_date) lines.push(`Service due: ${c.next_service_due_date}`);
  return lines.join('\n');
}

// ─── System prompt ────────────────────────────────────────────────────────────

function buildPrompt(business, dates) {
  const customer = business?._outboundCustomer || null;
  const rawName  = business?.name || 'D&V Ather';
  const bizName  = rawName.replace(/&/g, 'and').replace(/\s+/g, ' ').trim();
  const city     = business?.city || 'Bangalore';
  const customerName = customer?.name || 'the customer';
  const firstName = String(customer?.name || '')
    .split(/\s+/)[0]
    .replace(/[^a-zA-Z0-9 ]/g, '');
  const vehicle = customer?.vehicle_model || 'your Ather scooter';
  const dueDate = customer?.next_service_due_date || 'this week';

  // ── ADUKKALA PATTERN, ADAPTED FOR OUTBOUND ──────────────────────────────
  //
  // Mirrors geminiLive-restaurant.js: Persona → Language → Call Flow → Style.
  // The CRITICAL difference vs inbound: the caller didn't dial us. They just
  // picked up and said "Hello?" — that "Hello?" is OUR cue to begin.
  return `You are Vani, the AI service assistant for ${bizName}, an Ather electric scooter dealership in ${city}.

You are on an OUTBOUND call. YOU dialed THEM. The customer just answered and you can hear them — likely they said "Hello?" or "Yes?" or just made a sound. THIS IS YOUR CUE. Reply naturally and warmly in Indian English.

TODAY (IST): ${dates.todayLong} (${dates.todayISO})

CUSTOMER YOU ARE CALLING:
${renderCustomerBlock(customer)}

LANGUAGE:
- Primary: SOUTH INDIAN ENGLISH (Bangalore tongue). Warm, melodic, slight lilt on questions. Never American, British, or generic.
- Default greeting (use when the caller's first utterance is just "Hello?" or similar):
    "Hi${firstName ? ' ' + firstName : ''}, this is Vani from ${bizName} — got thirty seconds?"
- Match the caller's language exactly. Kannada → reply Kannada. Tamil → reply Tamil. Hindi → reply Hindi. English → South Indian English.
- If they mix (Kanglish), mirror the mix.
- Never apologise for accent or say "let me switch". Just match.

PERSONA:
Local Bangalorean woman, late 20s. Warm, efficient, respectful of the caller's time. Knows the dealership and Ather scooters by heart. Not pushy.

CALL FLOW:
1. WAIT for the caller. They will say "Hello?" / "Yes?" / "Haan?" or similar. THAT is your cue.
2. Greet them — use the LANGUAGE default opening, or a natural variant in their language.
3. Wait for their response (yes / who is this / busy / etc).
4. Ask ONE casual question: their ${vehicle} is due for service around ${dueDate} — would they like our team to call back and book a slot?
5. Wait for their answer. Branch:
   - YES / interested → "Great, our team will call you back to book the slot. Thanks${firstName ? ' ' + firstName : ''}, have a good one." → call end_call_politely(reason="customer_interested").
   - NO / not now → "No worries — whenever you're ready, just give us a ring. Have a good day." → call end_call_politely(reason="customer_declined").
   - WRONG NUMBER → "Oh sorry, must be the wrong number. We'll check our records. Take care." → call end_call_politely(reason="wrong_number").
   - BUSY / call later → "Of course — when's a good time?" → call reschedule_request + end_call_politely(reason="customer_busy").
   - DON'T CALL ME AGAIN → "Sure, we won't call again. Apologies for the call." → call mark_opted_out + end_call_politely(reason="opted_out").
6. After the closing line, STOP. Do NOT ask "anything else?". The call disconnects automatically right after end_call_politely.

STYLE:
- Warm, quick, natural — like a real service-reminder call.
- Max 2 sentences per turn. Phone call, keep it short.
- Never say you are an AI unless directly asked. If asked: "I'm an AI assistant from ${bizName} — a real person will follow up if you want to book."
- DO NOT propose specific dates or times. The human rep does that on callback.
- DO NOT quote prices. Refer cost questions to the rep.
- Target total call duration: under 45 seconds.`;
}

// ─── Tools ────────────────────────────────────────────────────────────────────

const TOOLS = [
  {
    name: 'reschedule_request',
    description: 'Customer wants to be contacted later (busy now, travelling, etc). Captures preferred timeframe.',
    parameters: {
      type: 'OBJECT',
      properties: {
        preferred_date: { type: 'STRING', description: 'YYYY-MM-DD or descriptive ("next week", "after Diwali")' },
        reason:         { type: 'STRING' },
      },
      required: ['preferred_date'],
    },
  },
  {
    name: 'mark_opted_out',
    description: 'Customer asked to never be called again. Records permanent opt-out. Call IMMEDIATELY when requested — TRAI compliance.',
    parameters: {
      type: 'OBJECT',
      properties: {
        reason: { type: 'STRING', description: 'Verbatim reason if given' },
      },
      required: [],
    },
  },
  {
    name: 'end_call_politely',
    description: 'End the call after the caller has been responded to. ALWAYS pass a reason: customer_interested | customer_declined | wrong_number | customer_busy | opted_out.',
    parameters: {
      type: 'OBJECT',
      properties: {
        reason: { type: 'STRING' },
      },
      required: ['reason'],
    },
  },
];

// ─── Tool handler ─────────────────────────────────────────────────────────────

async function onToolCall(fc, session) {
  const args = fc.args || {};
  const customer = session.business?._outboundCustomer || {};

  if (fc.name === 'reschedule_request') {
    console.log(`[EVOutFast] ${session.callId} — reschedule requested by ${customer.phone}: ${args.preferred_date} (${args.reason || ''})`);
    return {
      response: { success: true, message: 'Got it, will call back then.' },
      booking:  null,
    };
  }

  if (fc.name === 'mark_opted_out') {
    console.log(`[EVOutFast] ${session.callId} — OPT-OUT recorded for ${customer.phone}: ${args.reason || ''}`);
    return {
      response: { success: true, message: 'Recorded. You will not receive further calls from us.' },
      booking:  null,
      endCall:  'opted_out',
    };
  }

  if (fc.name === 'end_call_politely') {
    const reason = String(args.reason || 'task_complete');
    console.log(`[EVOutFast] ${session.callId} — ending call: ${reason}`);
    return {
      response: { success: true, message: 'Goodbye.' },
      booking:  null,
      endCall:  reason,
    };
  }

  return { response: { success: false, reason: `Unknown tool: ${fc.name}` }, booking: null };
}

// ─── Export ───────────────────────────────────────────────────────────────────

// NO kickoffTrigger function. NO SAY_NOW_EXACTLY. NO FIRST_UTTERANCE block.
//
// Instead: kickoffMode='caller-first' tells the base engine NOT to send a
// synthetic [call started] trigger on setupComplete. The engine stays silent
// and lets Gemini's VAD detect the caller's "Hello?", then Gemini replies
// naturally. Fallback: if KICKOFF_FALLBACK_MS (default 1500ms) passes with
// no caller speech, the engine fires '[call started]' as a safety net.
//
// This is the EXACT pattern that makes Adukkala inbound feel instant.

// 2026-06-08 (ChatGPT review P0 #4): when fallback fires (silent caller),
// hand Gemini an EXACT first line to speak — otherwise bare [call started]
// re-introduces the ~2s model planning latency we worked so hard to remove.
function kickoffTrigger(business) {
  const customer = business?._outboundCustomer || {};
  const firstName = String(customer.name || '').split(/\s+/)[0].replace(/[^a-zA-Z0-9 ]/g, '');
  const bizName = (business?.name || 'D and V Ather').replace(/&/g, 'and').replace(/\s+/g, ' ').trim();
  const line = `Hi${firstName ? ' ' + firstName : ''}, this is Vani from ${bizName} — got thirty seconds?`;
  return `SAY_NOW_EXACTLY: "${line}"`;
}

const VERTICAL = {
  name:        'ev-outbound-fast',
  // 2026-06-08 (ChatGPT review P0 #5): explicit direction so logCall can't
  // misclassify outbound-fast as inbound on the name-suffix check.
  direction:   'outbound',
  buildPrompt,
  tools:       TOOLS,
  onToolCall,
  kickoffMode: 'caller-first',
  kickoffTrigger,   // only used when fallback fires (silent caller)
};

async function handleCall(ws, callId, callerPhone, did, business = null, opts = {}) {
  return runLiveCall(ws, callId, callerPhone, did, business, VERTICAL, opts);
}

module.exports = { handleCall, VERTICAL };
