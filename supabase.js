/**
 * supabase.js — Supabase Client + DB Helpers
 *
 * All server-side operations use the service_role key — privileged, never
 * exposed to the client. Security must come from server-side validation,
 * not RLS (service_role bypasses RLS by design).
 *
 * Production features:
 *   - Idempotent call logging (unique constraint on calls.id)
 *   - Idempotent credit deduction via Postgres RPC
 *   - Idempotent booking via idempotency_key unique constraint
 *   - upsertBooking returns the DB record (caller gets real UUID)
 *   - checkAvailability validates working hours + booking conflicts
 *   - Business config cache with 5-min TTL
 *
 * Tables: businesses, calls, bookings, credit_ledger, business_hours, services
 */

'use strict';

const { createClient } = require('@supabase/supabase-js');

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  console.warn('[Supabase] Missing credentials — DB operations will be skipped');
}

let wsTransport;
try { wsTransport = require('ws'); } catch { /* Realtime disabled */ }

// Service role client — server-side only
const supabase = createClient(
  process.env.SUPABASE_URL               || 'https://placeholder.supabase.co',
  process.env.SUPABASE_SERVICE_ROLE_KEY  || 'placeholder',
  {
    auth:     { persistSession: false },
    realtime: wsTransport ? { transport: wsTransport } : {},
  }
);

const DB_ENABLED = !!(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY &&
  !process.env.SUPABASE_URL.includes('placeholder'));

// ─── Business config cache ────────────────────────────────────────────────────

const businessCache = new Map();
const CACHE_TTL_MS = 5 * 60 * 1000;

/**
 * Fetch and cache business configuration.
 */
async function getBusinessConfig(businessId) {
  if (!businessId || !DB_ENABLED) return null;

  const cached = businessCache.get(businessId);
  if (cached && Date.now() - cached.fetchedAt < CACHE_TTL_MS) return cached.data;

  try {
    const { data, error } = await supabase
      .from('businesses')
      .select('id, name, type, vertical, language, services, vobiz_did, whatsapp_number, wa_phone_number_id, wa_access_token, owner_name, owner_phone, credit_balance, monthly_minimum, rate_per_minute, gst_rate, status, suspended_at, suspended_reason, city, address, lat, lng, opens_at, closes_at, voice_name, showrooms')
      .eq('id', businessId)
      .single();

    if (error) { console.error(`[Supabase] getBusinessConfig: ${error.message}`); return null; }

    businessCache.set(businessId, { data, fetchedAt: Date.now() });
    return data;
  } catch (err) {
    console.error(`[Supabase] getBusinessConfig exception: ${err.message}`);
    return null;
  }
}

// ─── Catalogue (menu / inventory / model list) ───────────────────────────────
//
// Read by the voice server at Gemini Live session start so Gemini can quote
// real prices on the call. Written by the dashboard's /api/catalogue/ingest
// (Gemini Flash-parsed Zomato/Swiggy/PDF/text).
//
// One catalogue row per business (unique index on business_id).
// Items shape: [{id, name, price, category, veg?, description?, unit?, disabled}]
//
// Cached with the same 5-min TTL as getBusinessConfig — owner edits in the
// dashboard land in Gemini's prompt within at most 5 min, which is fine for
// our menu-doesn't-change-mid-call use case.

const catalogueCache = new Map();
const CATALOGUE_CACHE_TTL_MS = 5 * 60 * 1000;

async function getCatalogue(businessId) {
  if (!businessId || !DB_ENABLED) return null;

  const cached = catalogueCache.get(businessId);
  if (cached && Date.now() - cached.fetchedAt < CATALOGUE_CACHE_TTL_MS) return cached.data;

  try {
    const { data, error } = await supabase
      .from('business_catalogue')
      .select('items, source_type, source_url, item_count, parsed_at, updated_at')
      .eq('business_id', businessId)
      .maybeSingle();

    if (error) {
      // Table missing is OK on a fresh deploy — log once, then cache null so
      // we don't keep hitting the DB on every call.
      console.warn(`[Supabase] getCatalogue(${businessId}): ${error.message}`);
      catalogueCache.set(businessId, { data: null, fetchedAt: Date.now() });
      return null;
    }

    catalogueCache.set(businessId, { data: data || null, fetchedAt: Date.now() });
    return data || null;
  } catch (err) {
    console.error(`[Supabase] getCatalogue exception: ${err.message}`);
    return null;
  }
}

// ─── Availability check ───────────────────────────────────────────────────────

/**
 * Check if a service slot is available.
 * Validates: date format, business hours, existing booking conflicts.
 *
 * @returns {{ available: boolean, reason?: string, alternatives?: string[] }}
 */
async function checkAvailability({ businessId, service, date, time, stylist }) {
  if (!DB_ENABLED) {
    // Graceful fallback when DB not configured: assume available
    return { available: true, note: 'DB not configured — assuming available' };
  }

  try {
    // 1. Check business hours for the day of week
    const dayOfWeek = new Date(date).getDay(); // 0=Sun, 6=Sat
    const { data: hoursData } = await supabase
      .from('business_hours')
      .select('opens_at, closes_at, is_closed')
      .eq('business_id', businessId)
      .eq('day_of_week', dayOfWeek)
      .single();

    if (hoursData?.is_closed) {
      return { available: false, reason: 'Business is closed on that day.' };
    }

    if (hoursData) {
      const requestedMinutes = timeToMinutes(time);
      const opensMinutes     = timeToMinutes(hoursData.opens_at);
      const closesMinutes    = timeToMinutes(hoursData.closes_at);

      if (requestedMinutes < opensMinutes || requestedMinutes >= closesMinutes) {
        return {
          available: false,
          reason: `Business hours are ${hoursData.opens_at} to ${hoursData.closes_at}.`,
        };
      }
    }

    // 2. Check for conflicting existing bookings at same time
    const { data: conflicts } = await supabase
      .from('bookings')
      .select('id')
      .eq('business_id', businessId)
      .eq('booking_date', date)
      .eq('booking_time', time)
      .eq('status', 'confirmed')
      .limit(1);

    if (conflicts && conflicts.length > 0) {
      return {
        available: false,
        reason: 'That slot is already taken.',
        alternatives: suggestAlternativeTimes(time),
      };
    }

    return { available: true };
  } catch (err) {
    console.error(`[Supabase] checkAvailability exception: ${err.message}`);
    // Fail open for MVP — log the error but don't block the booking
    return { available: true, note: 'Availability check failed — proceeding' };
  }
}

function timeToMinutes(timeStr) {
  const [h, m] = timeStr.split(':').map(Number);
  return h * 60 + m;
}

function suggestAlternativeTimes(time) {
  const mins = timeToMinutes(time);
  return [
    minutesToTime(mins + 30),
    minutesToTime(mins + 60),
    minutesToTime(mins - 30),
  ].filter((t) => {
    const m = timeToMinutes(t);
    return m >= 9 * 60 && m <= 20 * 60; // 9am–8pm range
  });
}

function minutesToTime(totalMinutes) {
  const h = Math.floor(totalMinutes / 60) % 24;
  const m = totalMinutes % 60;
  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
}

// ─── Booking ──────────────────────────────────────────────────────────────────

/**
 * Create or return existing booking using idempotency_key.
 * Returns the full DB record so the caller gets the Postgres-generated UUID.
 *
 * Idempotency key: businessId|callerPhone|service|date|time
 * DB unique constraint on bookings.idempotency_key prevents duplicates.
 */
async function createBookingIdempotent(booking) {
  if (!DB_ENABLED) return booking;

  try {
    const { data, error } = await supabase
      .from('bookings')
      .upsert({
        business_id:      booking.businessId,
        type:             booking.type,
        caller_phone:     booking.callerPhone,
        caller_name:      booking.caller_name   || null,
        service:          booking.service        || null,
        stylist:          booking.stylist         || null,
        booking_date:     booking.date            || null,
        booking_time:     booking.time            || null,
        status:           booking.status          || 'confirmed',
        idempotency_key:  booking.idempotencyKey,
        details:          booking,
        created_at:       booking.createdAt       || new Date().toISOString(),
      }, {
        onConflict: 'idempotency_key',   // return existing row, don't error
        ignoreDuplicates: false,
      })
      .select('id, status, idempotency_key')
      .single();

    if (error) {
      console.error(`[Supabase] createBookingIdempotent error: ${error.message}`);
      return booking; // return in-memory booking as fallback
    }

    console.log(`[Supabase] Booking upserted: ${data.id}`);
    return data; // return DB record with real UUID
  } catch (err) {
    console.error(`[Supabase] createBookingIdempotent exception: ${err.message}`);
    return booking;
  }
}

// ─── Booking type inference ──────────────────────────────────────────────────

/**
 * Map business.type to the bookings.type CHECK constraint enum.
 * Allowed values: salon_appointment | food_order | ev_appointment | supermarket_order
 */
function inferBookingType(businessType) {
  const t = (businessType || '').toLowerCase();
  if (t.includes('restaurant') || t.includes('food'))       return 'food_order';
  if (t.includes('ev') || t.includes('electric'))           return 'ev_appointment';
  if (t.includes('supermarket') || t.includes('grocery'))   return 'supermarket_order';
  return 'salon_appointment'; // default
}

/**
 * Upsert any booking type (food orders, EV appointments).
 * Returns DB record.
 */
async function upsertBooking(booking) {
  if (!DB_ENABLED) return booking;

  // Lift typed columns out of the details payload so the dashboard's queries
  // (which select booking_date / booking_time / service / stylist directly)
  // get values, instead of NULL on every row. Until 2026-05-12 these were
  // only stored inside the `details` JSONB and the salon dashboard rendered
  // "—" for every time slot.
  const bookingDate = booking.booking_date || booking.date || null;
  const bookingTime = normalizeBookingTime(booking.booking_time || booking.time);

  try {
    const { data, error } = await supabase
      .from('bookings')
      .upsert({
        id:           booking.id || `bk-${Date.now()}-${Math.random().toString(36).slice(2,7)}`,
        business_id:  booking.businessId,
        type:         booking.type || inferBookingType(booking.businessType),
        caller_phone: booking.callerPhone,
        caller_name:  booking.caller_name  || null,
        service:      booking.service      || null,
        stylist:      booking.stylist      || null,
        booking_date: bookingDate,
        booking_time: bookingTime,
        details:      booking,
        status:       booking.status       || 'confirmed',
        created_at:   booking.createdAt    || new Date().toISOString(),
      }, { onConflict: 'id' })
      .select('id, status')
      .single();

    if (error) {
      console.error(`[Supabase] upsertBooking error: ${error.message}`);
      return null;
    }

    return data;
  } catch (err) {
    console.error(`[Supabase] upsertBooking exception: ${err.message}`);
    return null;
  }
}

/**
 * Normalize a time string into HH:MM:SS (Postgres `time` format).
 * Gemini may return "5pm", "5:00 PM", "17:00", or "17:00:00" — coerce all to "17:00:00".
 * Returns null for unparseable input so we don't write garbage.
 */
function normalizeBookingTime(t) {
  if (!t) return null;
  const s = String(t).trim().toLowerCase();
  // already HH:MM or HH:MM:SS
  let m = s.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (m) {
    const hh = String(Math.min(23, parseInt(m[1], 10))).padStart(2, '0');
    return `${hh}:${m[2]}:${m[3] || '00'}`;
  }
  // 5pm / 5 pm / 5:30pm
  m = s.match(/^(\d{1,2})(?::(\d{2}))?\s*(am|pm)$/);
  if (m) {
    let h = parseInt(m[1], 10) % 12;
    if (m[3] === 'pm') h += 12;
    return `${String(h).padStart(2, '0')}:${m[2] || '00'}:00`;
  }
  return null;
}

/**
 * Flip bookings.whatsapp_sent to true after a confirmation message lands.
 * Lets the dashboard show a "delivered" badge per booking and lets the
 * health monitor distinguish unsendable bookings (caller_phone NULL,
 * WA credentials missing) from genuine delivery failures.
 */
async function markBookingWhatsAppSent(bookingId) {
  if (!DB_ENABLED || !bookingId) return false;
  try {
    const { error } = await supabase
      .from('bookings')
      .update({ whatsapp_sent: true })
      .eq('id', bookingId);
    if (error) {
      console.error(`[Supabase] markBookingWhatsAppSent(${bookingId}) error: ${error.message}`);
      return false;
    }
    return true;
  } catch (err) {
    console.error(`[Supabase] markBookingWhatsAppSent(${bookingId}) exception: ${err.message}`);
    return false;
  }
}

// ─── Call logging + credit deduction (idempotent) ────────────────────────────

/**
 * Log a completed call and deduct credits in one atomic Postgres RPC.
 * The RPC uses INSERT ... ON CONFLICT DO NOTHING on calls.id, so
 * retries / double WS-close events never double-charge credits.
 *
 * Falls back to two separate operations if the RPC doesn't exist yet.
 */
async function logCall({ callId, businessId, callerPhone, language, durationMs, endReason, bookingId, stats, transcript, recordingPath, recordingUrl, direction }) {
  if (!DB_ENABLED) return;

  const durationMin = durationMs / 60000;

  // Per-client rate (rate_per_minute on businesses). Fall back to 15 if the
  // column is missing on legacy rows or the lookup fails. 1 credit = Rs 1,
  // rounded up to the next whole credit so a 47-second call at Rs 15/min
  // bills as Rs 12 (0.78 × 15 = 11.74 → 12). Same rounding rule applies
  // regardless of the per-client rate.
  let ratePerMinute = 15;
  let resolvedBiz   = null;
  try {
    resolvedBiz = await getBusinessConfig(businessId);
    const r = parseFloat(resolvedBiz?.rate_per_minute);
    if (Number.isFinite(r) && r > 0) ratePerMinute = r;
  } catch (_) { /* keep default */ }

  // ── 2026-06-06 fix: businessId-not-in-DB safety net ──────────────────────
  // When the passed businessId is missing from the businesses table (stale
  // demo ctx, mid-deploy race, manual test with wrong id), getBusinessConfig
  // returns null. The OLD logCall would then attempt the RPC with that bad
  // id, hit a foreign-key violation, fall back to an upsert that ALSO failed
  // the same FK, and silently drop the row. That cost us 30+ hours of
  // outbound telemetry blackout today.
  //
  // Behaviour now: stash the original-but-bad id into stats, and rewrite
  // businessId to the env-configured fallback (or 'test-salon-01' if unset).
  // The row lands. The forensics survive. Future SQL queries can find these
  // orphans with `WHERE stats->>'origBusinessIdOrphan' IS NOT NULL`.
  const FALLBACK_BUSINESS_ID = process.env.LOGCALL_FALLBACK_BUSINESS_ID || 'test-salon-01';
  let origBusinessIdOrphan = null;
  if (!resolvedBiz && businessId && businessId !== FALLBACK_BUSINESS_ID) {
    console.warn(`[Supabase] logCall: businessId="${businessId}" not in businesses — rewriting to fallback="${FALLBACK_BUSINESS_ID}" so the row lands. stats.origBusinessIdOrphan will preserve the original for forensics.`);
    origBusinessIdOrphan = businessId;
    businessId = FALLBACK_BUSINESS_ID;
  }
  // Always preserve original-vs-resolved in stats for SQL diagnosis even on
  // the happy path (helpful when the id is right but the FK is mis-configured).
  stats = { ...(stats || {}), origBusinessIdOrphan, resolvedBusinessId: businessId };

  const creditUsed = Math.ceil(durationMin * ratePerMinute);

  try {
    // Prefer atomic RPC (creates call row + deducts credits in one transaction)
    const { error: rpcError } = await supabase.rpc('log_call_and_deduct_credits', {
      p_call_id:    callId,
      p_business_id: businessId,
      p_caller_phone: callerPhone || null,
      p_language:   language     || null,
      p_duration_ms: Math.round(durationMs),
      p_duration_min: parseFloat(durationMin.toFixed(4)),
      p_end_reason: endReason    || null,
      p_booking_id: bookingId    || null,
      p_credits_used: creditUsed,
      p_stats:      stats        || {},
    });

    if (rpcError) {
      // 2026-06-05: any RPC failure (not just "not found") now triggers the
      // fallback insert. Previously a non-404 RPC error was logged-and-dropped,
      // which masked 30 hours of outbound telemetry blackout caused by an
      // FK violation on a bad business_id (demo-dv-ather not in businesses).
      // Better: surface the error AND try the upsert with whatever fields
      // the schema accepts, so we never lose the row silently again.
      if (rpcError.code === 'PGRST202' || rpcError.message?.includes('does not exist')) {
        await logCallFallback({ callId, businessId, callerPhone, language, durationMs, durationMin, endReason, bookingId, creditUsed, stats, transcript, recordingPath, recordingUrl, direction });
      } else {
        console.error(`[Supabase] log_call_and_deduct_credits RPC error (will fallback): ${rpcError.message}`);
        await logCallFallback({ callId, businessId, callerPhone, language, durationMs, durationMin, endReason, bookingId, creditUsed, stats, transcript, recordingPath, recordingUrl, direction })
          .catch((e) => console.error(`[Supabase] logCallFallback after RPC error also failed: ${e.message}`));
      }
    } else {
      // RPC succeeded — patch extra columns the RPC doesn't handle
      if (transcript || recordingPath || recordingUrl || direction) {
        await supabase.from('calls').update({
          transcript:     transcript     || [],
          recording_path: recordingPath  || null,
          recording_url:  recordingUrl   || null,
          ...(direction ? { direction } : {}),
        }).eq('id', callId);
      }
      console.log(`[Supabase] Call logged: ${callId} (${creditUsed} credits, dir=${direction || '?'})`);
    }
  } catch (err) {
    console.error(`[Supabase] logCall exception: ${err.message}`);
  }
}

async function logCallFallback({ callId, businessId, callerPhone, language, durationMs, durationMin, endReason, bookingId, creditUsed, stats, transcript, recordingPath, recordingUrl, direction }) {
  // INSERT ... ON CONFLICT DO NOTHING — idempotent even without the RPC
  const { error } = await supabase.from('calls').upsert({
    id:               callId,
    business_id:      businessId,
    caller_phone:     callerPhone   || null,
    language_detected: language     || null,
    duration_ms:      Math.round(durationMs),
    duration_min:     parseFloat(durationMin.toFixed(4)),
    end_reason:       endReason     || null,
    booking_id:       bookingId     || null,
    credits_used:     creditUsed,
    stats:            stats         || {},
    transcript:       transcript    || [],
    recording_path:   recordingPath || null,
    recording_url:    recordingUrl  || null,
    // 2026-06-05: was implicit (DB default = 'inbound'), now explicit per call.
    // Outbound test calls today were all silently mislabeled because logCall
    // never passed this field.
    direction:        direction     || 'inbound',
    created_at:       new Date().toISOString(),
  }, { onConflict: 'id', ignoreDuplicates: true });

  if (error) {
    console.error(`[Supabase] logCallFallback error: ${error.message}`);
    return;
  }

  if (businessId && creditUsed > 0) {
    await deductCredits(businessId, creditUsed, callId);
  }

  console.log(`[Supabase] Call logged (fallback): ${callId} (${creditUsed} credits, dir=${direction || '?'})`);
}

/**
 * Atomically deduct credits via Postgres RPC.
 * The RPC checks for existing credit_ledger entry by call_id to prevent
 * double deduction.
 */
async function deductCredits(businessId, amount, callId) {
  try {
    const { error } = await supabase.rpc('deduct_credits', {
      p_business_id: businessId,
      p_amount:      amount,
      p_call_id:     callId,
    });
    if (error) console.error(`[Supabase] deductCredits error: ${error.message}`);
  } catch (err) {
    console.error(`[Supabase] deductCredits exception: ${err.message}`);
  }
}

// ─── DID lookup ───────────────────────────────────────────────────────────────

async function getBusinessByDID(did) {
  if (!did || !DB_ENABLED) return null;
  try {
    const { data, error } = await supabase
      .from('businesses')
      .select('id')
      .eq('vobiz_did', did)
      .single();
    if (error) return null;
    return data?.id || null;
  } catch {
    return null;
  }
}

// ─── Call recordings ──────────────────────────────────────────────────────────

/**
 * Upload a call recording (LINEAR16 WAV) to Supabase Storage.
 * Path: call-recordings/{businessId}/{YYYY-MM-DD}/{callId}.wav
 * Returns { path, url } or null on failure.
 *
 * The returned `url` is a signed URL valid for 7 days — use it in the dashboard.
 * Re-generate it on demand rather than storing long-term (signed URLs rotate).
 */
async function uploadRecording({ callId, businessId, audioBuffer }) {
  if (!DB_ENABLED || !audioBuffer || audioBuffer.length === 0) return null;

  try {
    const date = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
    const path = `${businessId || 'default'}/${date}/${callId}.wav`;

    // Prepend a minimal WAV header so the browser can play it natively
    const wavBuffer = wrapLinear16AsWav(audioBuffer, 8000);

    const { error: uploadError } = await supabase.storage
      .from('call-recordings')
      .upload(path, wavBuffer, {
        contentType:  'audio/wav',
        upsert:       true,
      });

    if (uploadError) {
      console.error(`[Supabase] Recording upload error: ${uploadError.message}`);
      return null;
    }

    // Generate a 7-day signed URL for dashboard playback
    const { data: signedData, error: signedError } = await supabase.storage
      .from('call-recordings')
      .createSignedUrl(path, 7 * 24 * 60 * 60); // 7 days in seconds

    if (signedError) {
      console.warn(`[Supabase] Signed URL error: ${signedError.message}`);
      return { path, url: null };
    }

    return { path, url: signedData.signedUrl };
  } catch (err) {
    console.error(`[Supabase] uploadRecording exception: ${err.message}`);
    return null;
  }
}

/**
 * Generate a fresh signed URL for an existing recording path.
 * Call this in the dashboard API route — URLs expire after 7 days.
 */
async function getRecordingSignedUrl(path) {
  if (!DB_ENABLED || !path) return null;
  try {
    const { data, error } = await supabase.storage
      .from('call-recordings')
      .createSignedUrl(path, 7 * 24 * 60 * 60);
    if (error) return null;
    return data.signedUrl;
  } catch {
    return null;
  }
}

/**
 * Wrap raw LINEAR16 PCM bytes in a minimal WAV header.
 * Makes the audio playable in any browser without a codec.
 */
function wrapLinear16AsWav(pcmBuffer, sampleRate = 8000, channels = 1, bitDepth = 16) {
  const byteRate    = sampleRate * channels * (bitDepth / 8);
  const blockAlign  = channels * (bitDepth / 8);
  const dataSize    = pcmBuffer.length;
  const headerSize  = 44;
  const buf         = Buffer.alloc(headerSize + dataSize);

  buf.write('RIFF',          0);
  buf.writeUInt32LE(36 + dataSize, 4);
  buf.write('WAVE',          8);
  buf.write('fmt ',         12);
  buf.writeUInt32LE(16,     16);  // PCM chunk size
  buf.writeUInt16LE(1,      20);  // PCM format
  buf.writeUInt16LE(channels, 22);
  buf.writeUInt32LE(sampleRate, 24);
  buf.writeUInt32LE(byteRate,   28);
  buf.writeUInt16LE(blockAlign, 32);
  buf.writeUInt16LE(bitDepth,   34);
  buf.write('data',          36);
  buf.writeUInt32LE(dataSize,   40);
  pcmBuffer.copy(buf,        44);

  return buf;
}

// ─── Exports ──────────────────────────────────────────────────────────────────

module.exports = {
  supabase,
  getBusinessConfig,
  getCatalogue,
  checkAvailability,
  createBookingIdempotent,
  inferBookingType,
  upsertBooking,
  markBookingWhatsAppSent,
  logCall,
  deductCredits,
  getBusinessByDID,
  uploadRecording,
  getRecordingSignedUrl,
};
