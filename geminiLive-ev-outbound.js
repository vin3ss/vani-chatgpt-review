/**
 * geminiLive-ev-outbound.js — Vani EV Service Reminder Agent (OUTBOUND)
 *
 * Mirrors geminiLive-ev.js but for outbound service-reminder calls:
 * the customer didn't dial us — WE dialed THEM. So the agent must:
 *   1. Speak FIRST on [call started] — caller is silent, waiting.
 *   2. Self-identify as AI (TRAI requirement for outbound automated calls).
 *   3. Verify they're the right person before pitching.
 *   4. Pitch the service-due reminder.
 *   5. Offer 3 concrete slots.
 *   6. Book via the existing book_test_ride tool (appointment_type='service').
 *   7. Wrap in under 3 minutes.
 *
 * Customer context is injected by the dispatcher onto business._outboundCustomer
 * BEFORE runLiveCall() is invoked. buildPrompt reads it there so the existing
 * geminiLiveBase.js (which calls buildPrompt(business, dates)) needs no changes.
 *
 * Built 2026-05-23 for D&V Ather Dealer Boss demo.
 */

'use strict';

const crypto = require('crypto');
const { runLiveCall } = require('./geminiLiveBase');
const { upsertBooking } = require('./supabase');

// ─── Language mapping (same as inbound EV) ────────────────────────────────────
const LANG_LABELS = {
  'kn-IN': { name: 'Kannada',        greeting: 'Namaskara',  region: 'Karnataka' },
  'hi-IN': { name: 'Hindi',          greeting: 'Namaste',    region: 'North India' },
  'ta-IN': { name: 'Tamil',          greeting: 'Vanakkam',   region: 'Tamil Nadu' },
  'te-IN': { name: 'Telugu',         greeting: 'Namaskaram', region: 'Andhra/Telangana' },
  'ml-IN': { name: 'Malayalam',      greeting: 'Namaskaram', region: 'Kerala' },
  'en-IN': { name: 'Indian English', greeting: 'Hello',      region: 'India' },
};

/**
 * Render the customer context block. Only includes fields that are populated
 * so Gemini never speaks an "undefined" or "null" placeholder.
 */
function renderCustomerBlock(c) {
  if (!c) return '(no customer context — speak generically)';
  const lines = [];
  if (c.name)              lines.push(`Customer name: ${c.name}`);
  if (c.phone)             lines.push(`Phone: ${c.phone}`);
  if (c.vehicle_model)     lines.push(`Vehicle: ${c.vehicle_model}`);
  if (c.registration_no)   lines.push(`Registration: ${c.registration_no}`);
  if (c.purchase_date)     lines.push(`Purchased on: ${c.purchase_date}`);
  if (c.last_service_date) lines.push(`Last service: ${c.last_service_date}`);
  if (c.last_service_km)   lines.push(`Last service km: ${c.last_service_km}`);
  if (c.next_service_due_date) lines.push(`Service due: ${c.next_service_due_date}`);
  if (c.preferred_language)    lines.push(`Preferred language: ${c.preferred_language}`);
  if (c.slots_offered?.length) lines.push(`Slots to offer: ${c.slots_offered.join('; ')}`);
  return lines.join('\n');
}

/**
 * Build the outbound system prompt. The crucial difference vs the inbound
 * EV prompt: the agent SPEAKS FIRST and discloses it's an AI.
 */
function buildPrompt(business, dates) {
  const customer = business?._outboundCustomer || null;
  const rawName  = business?.name || 'D&V Ather';
  const bizName  = rawName.replace(/&/g, 'and').replace(/\s+/g, ' ').trim();
  // Use first name only (more natural in the opener). Fallback handled
  // inline in the LANGUAGE section so the prompt reads cleanly either way.
  const firstName = String(customer?.name || '')
    .split(/\s+/)[0]
    .replace(/[^a-zA-Z0-9 ]/g, '');
  const vehicle = customer?.vehicle_model || 'your Ather scooter';
  const dueDate = customer?.next_service_due_date || 'this week';

  // ── ADUKKALA PATTERN (2026-06-05 refactor) ─────────────────────────────
  //
  // Mirrors the working inbound restaurant prompt structure:
  //   Persona → Language (with EXACT opener line baked in) → Call Flow →
  //   Style → Tools
  //
  // No kickoffTrigger user-turn injection. No FIRST_UTTERANCE block. No
  // TRIGGER PROTOCOL section. The base engine sends bare `[call started]`
  // and Gemini speaks the opener that lives inline in LANGUAGE — the same
  // way `geminiLive-restaurant.js` works for Adukkala (which has 600-800ms
  // first audio in production).
  //
  // Why this works for inbound and should work for outbound: the model
  // treats systemInstruction as cached context. The greeting living
  // there alongside the language rule is a single, unambiguous source
  // of truth — no parsing of a user-turn trigger, no improvisation.
  return `You are Vani, the AI service assistant for ${bizName}, an Ather electric scooter dealership in Bangalore. You are calling existing customers to remind them their scooter is due for service.

This is an OUTBOUND call — YOU dialed THEM. The customer just picked up and is silent, waiting to hear who's calling. SPEAK FIRST.

TODAY (IST): ${dates.todayLong} (${dates.todayISO})

CUSTOMER YOU ARE CALLING:
- Name: ${customer?.name || 'unknown'}
- Phone: ${customer?.phone || '(unknown)'}
- Their vehicle: ${vehicle}
- Service due: ${dueDate}

LANGUAGE:
- Primary: SOUTH INDIAN ENGLISH — Bangalore / Karnataka tongue. Warm, melodic intonation. Softer 'r' sound. Slight lilt at the end of questions. NEVER American, British, or generic neutral English.
- Default opening line: "Hi${firstName ? ' ' + firstName : ''}, this is Vani from ${bizName} — got thirty seconds?" (delivered in South Indian English).
- If the caller responds in Kannada, reply in Kannada. If Tamil, reply Tamil. Hindi, reply Hindi. English, stay in South Indian English.
- If they mix languages (Kanglish, Kanglish-Hindi), mirror the mix.
- Pronounce Ather model names naturally — Rizta, four-fifty-S, four-fifty-X, four-fifty Apex. Never anglicize.
- Never apologise for accent or say "let me switch". Just match them.

PERSONA:
Local Bangalorean woman, late 20s. Warm, efficient, respectful of the caller's time. Knows the dealership and the cars by heart. Never pushy.

CALL FLOW:
1. Greet warmly when you hear [call started]. Use the opening line in LANGUAGE above.
2. Wait for their response.
3. Ask ONE casual question: their ${vehicle} is due for service around ${dueDate} — would they like our team to call back and book a slot?
4. Wait for their answer.
5. Branch on what they say:
   - YES / interested → "Great, our team will call you back to book the slot. Thanks${firstName ? ' ' + firstName : ''}, have a good one." → call end_call_politely(reason="customer_interested").
   - NO / not now → "No worries — whenever you're ready, just give us a ring. Have a good day." → call end_call_politely(reason="customer_declined").
   - WRONG NUMBER → "Oh sorry, must be the wrong number. We'll check our records. Take care." → call end_call_politely(reason="wrong_number").
   - BUSY / call later → "Of course — when's a good time?" Wait for their answer, then call reschedule_request with their preferred timeframe, then end_call_politely(reason="customer_busy").
   - DON'T CALL ME AGAIN → "Sure, we won't call again. Apologies for the call." → call mark_opted_out, then end_call_politely(reason="opted_out").
6. After your closing line, STOP. Do NOT ask "anything else?" Do NOT keep talking. The call disconnects automatically right after end_call_politely.

STYLE:
- Warm, quick, natural — like a real person making a service-reminder call.
- Max 2 sentences per turn — phone call, keep it short.
- Never say you are an AI unless directly asked. If asked: "I'm an AI assistant from ${bizName} — a real person will follow up if you want to book."
- DO NOT propose specific dates or times. The human rep does that on callback.
- DO NOT quote prices. Refer cost questions to the rep.
- Total call duration target: under 45 seconds.`;
}

// ─── Tools — service booking + opt-out + reschedule ───────────────────────────
//
// Reuses the same schema as inbound EV's book_test_ride so the booking lands
// in the same Supabase row shape and dealer dashboard. Added two outbound-
// specific tools: reschedule_request and mark_opted_out for TRAI compliance.

const TOOLS = [
  {
    name: 'book_test_ride',
    description: 'Book a service appointment for the customer. Call only after the caller confirms a specific slot. Use appointment_type="service".',
    parameters: {
      type: 'OBJECT',
      properties: {
        ev_model:         { type: 'STRING',  description: 'Their vehicle model (from customer context)' },
        appointment_type: { type: 'STRING',  description: 'Always "service" for this outbound flow' },
        date:             { type: 'STRING',  description: 'YYYY-MM-DD' },
        time:             { type: 'STRING',  description: 'HH:MM 24hr' },
        caller_name:      { type: 'STRING'  },
        caller_phone:     { type: 'STRING'  },
        service_type:     { type: 'STRING',  description: 'general_service, battery_check, tyre, etc.' },
        notes:            { type: 'STRING'  },
      },
      required: ['appointment_type', 'date', 'caller_phone'],
    },
  },
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
    description: 'Politely end the call when wrong number, customer not available, or task complete.',
    parameters: {
      type: 'OBJECT',
      properties: {
        reason: { type: 'STRING' },
      },
      required: [],
    },
  },
];

// ─── Tool handler ─────────────────────────────────────────────────────────────

async function onToolCall(fc, session) {
  const args = fc.args || {};
  const customer = session.business?._outboundCustomer || {};

  if (fc.name === 'book_test_ride') {
    const booking = {
      id:               crypto.randomUUID(),
      type:             'ev_appointment',
      businessId:       session.businessId,
      callerPhone:      args.caller_phone || session.callerPhone || customer.phone,
      caller_name:      args.caller_name  || customer.name || null,
      ev_model:         args.ev_model     || customer.vehicle_model || null,
      appointment_type: args.appointment_type || 'service',
      date:             args.date,
      time:             args.time          || null,
      service_type:     args.service_type  || 'general_service',
      notes:            (args.notes ? args.notes + ' | ' : '') + 'Booked via outbound service reminder call',
      status:           'confirmed',
      createdAt:        new Date().toISOString(),
      // Outbound provenance — useful for dealer dashboard filtering
      meta:             { source: 'outbound_reminder', reminder_call_id: session.callId },
    };

    try {
      const saved = await upsertBooking(booking);
      if (saved?.id) booking.id = saved.id;
      console.log(`[EVOut] ${session.callId} — service booked: ${booking.id}`);
      return {
        response: {
          success:    true,
          booking_id: booking.id,
          message:    `Service confirmed for ${booking.ev_model || 'your scooter'} on ${args.date}${args.time ? ' at ' + args.time : ''}. SMS confirmation in a minute.`,
        },
        booking,
      };
    } catch (err) {
      console.error(`[EVOut] ${session.callId} — upsertBooking error: ${err.message}`);
      return { response: { success: false, message: 'Could not save booking. Apologise and offer to book later.' }, booking: null };
    }
  }

  if (fc.name === 'reschedule_request') {
    // For the demo: log only. Production wires this to push back into reminder_queue.
    console.log(`[EVOut] ${session.callId} — reschedule requested: ${args.preferred_date} (${args.reason || 'no reason given'})`);
    return {
      response: { success: true, message: `Noted. We will call back ${args.preferred_date}.` },
      booking: null,
    };
  }

  if (fc.name === 'mark_opted_out') {
    // For the demo: log only. Production updates customers.opt_in_voice_calls=false + customer_consent_log row.
    console.log(`[EVOut] ${session.callId} — OPT-OUT recorded for ${customer.phone}: ${args.reason || ''}`);
    return {
      response: { success: true, message: 'Recorded. You will not receive further calls from us.' },
      booking: null,
      endCall: 'opted_out',  // hang up after Vani's final line plays (3s grace)
    };
  }

  if (fc.name === 'end_call_politely') {
    // Closes task #112: outbound calls must auto-hangup after the agent's
    // closing line. The engine waits 3s for the goodbye audio to flush,
    // then closes the WSS — Vobiz tears down the phone call.
    // Reason string flows into calls.end_reason for the dashboard.
    const reason = String(args.reason || 'task_complete');
    console.log(`[EVOut] ${session.callId} — ending call: ${reason}`);
    return {
      response: { success: true, message: 'Goodbye.' },
      booking: null,
      endCall: reason,
    };
  }

  return { response: { success: false, reason: `Unknown tool: ${fc.name}` }, booking: null };
}

// ─── Export ───────────────────────────────────────────────────────────────────

// NOTE: NO kickoffTrigger function — by design.
//
// 2026-06-05 Adukkala-pattern refactor: dropped the SAY_NOW_EXACTLY /
// FIRST_UTTERANCE machinery entirely. The opener line lives inline in the
// LANGUAGE section of buildPrompt — exactly how `geminiLive-restaurant.js`
// handles the Adukkala opener (which has 600-800ms first audio in production).
//
// The base engine's `sendGreeting()` falls back to bare `[call started]`
// when vertical.kickoffTrigger is undefined. Gemini reads the opener from
// systemInstruction. No user-turn parse cost. No improvisation surface.
//
// If you ever need to force-script a different first line, prefer adding
// a `Default opening line:` row to the LANGUAGE section of the prompt,
// not a kickoffTrigger function.

const VERTICAL = { name: 'ev-outbound', buildPrompt, tools: TOOLS, onToolCall };

/**
 * Dispatcher entry. Caller (the new /call/outbound WSS handler in index.js)
 * has already loaded the business + customer context and stashed customer
 * onto business._outboundCustomer. We just hand off to runLiveCall.
 */
async function handleCall(ws, callId, callerPhone, did, business = null, opts = {}) {
  return runLiveCall(ws, callId, callerPhone, did, business, VERTICAL, opts);
}

module.exports = { handleCall, VERTICAL };
