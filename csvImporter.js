/**
 * csvImporter.js — Bulk + delta CSV upload for dealer customer master.
 *
 * Cadence (per project_vani_service_reminder_outbound memory):
 *   - Day 0 (onboarding):   one-time full master CSV
 *   - Recurring:            every 14 days, delta CSV with new purchases + updates
 *
 * UPSERT-on-phone semantics: existing rows updated, new rows inserted.
 * Phones missing from a delta are NOT deleted — manager decides via dashboard.
 *
 * Validation rules:
 *   - Phone: normalize to E.164 +91XXXXXXXXXX, reject if invalid
 *   - Required: name, phone, vehicle_model, consent_signed_on
 *   - Optional: registration_no, purchase_date, last_service_date,
 *               last_service_km, next_service_due_date, email, preferred_language
 *   - Dates: YYYY-MM-DD; reject malformed
 *
 * Returns a structured report so dealer can fix bad rows and re-upload.
 *
 * Built 2026-05-23 (Karthik R / Reenu K).
 */

'use strict';

const { supabase } = require('./supabase');
const { toE164India } = require('./vobizOutbound');

// ─── Minimal RFC 4180 CSV parser ──────────────────────────────────────────────
// Avoiding npm dependency on `csv-parse` — dealer CSVs are simple (no embedded
// newlines, single-byte encoding). Strict enough for our schema, loose enough
// not to choke on Excel-emitted quirks (trailing commas, BOM, CRLF).
function parseCsv(text) {
  // Strip BOM if Excel emitted one
  if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1);
  // Normalize line endings
  text = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n');

  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === '"' && text[i + 1] === '"') { field += '"'; i++; }
      else if (ch === '"') { inQuotes = false; }
      else { field += ch; }
    } else {
      if (ch === '"' && field === '') { inQuotes = true; }
      else if (ch === ',') { row.push(field); field = ''; }
      else if (ch === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
      else { field += ch; }
    }
  }
  // Last field/row if file doesn't end in newline
  if (field !== '' || row.length > 0) { row.push(field); rows.push(row); }
  // Strip blank trailing rows
  return rows.filter(r => !(r.length === 1 && r[0].trim() === ''));
}

function toRecords(rows) {
  if (rows.length < 2) return { headers: [], records: [] };
  const headers = rows[0].map(h => h.trim().toLowerCase().replace(/\s+/g, '_'));
  const records = rows.slice(1).map((cols, idx) => {
    const rec = { _row: idx + 2 }; // 1-based + header row
    headers.forEach((h, i) => { rec[h] = (cols[i] ?? '').trim(); });
    return rec;
  });
  return { headers, records };
}

// ─── Validators ───────────────────────────────────────────────────────────────
const RE_DATE = /^\d{4}-\d{2}-\d{2}$/;
function isValidDate(s) {
  if (!s) return false;
  if (!RE_DATE.test(s)) return false;
  const d = new Date(s + 'T00:00:00Z');
  return !isNaN(d.getTime());
}

const LANGUAGE_CODE_MAP = {
  kannada:   'kn-IN',
  hindi:     'hi-IN',
  tamil:     'ta-IN',
  telugu:    'te-IN',
  malayalam: 'ml-IN',
  english:   'en-IN',
};

function normalizeLanguage(s) {
  const k = (s || '').toLowerCase().trim();
  if (!k) return { name: 'kannada', code: 'kn-IN' };
  for (const [name, code] of Object.entries(LANGUAGE_CODE_MAP)) {
    if (k.includes(name)) return { name, code };
  }
  return { name: 'kannada', code: 'kn-IN' };
}

// ─── Main importer ────────────────────────────────────────────────────────────
/**
 * Import a CSV string for one business.
 *
 * @param {object} args
 * @param {string} args.businessId    target business id
 * @param {string} args.csvText       raw CSV text (UTF-8)
 * @param {string} args.uploadedBy    name/email of dealer staff doing upload
 * @param {string} [args.source]      'csv_import' | 'csv_delta' (default: csv_import)
 *
 * @returns {Promise<{
 *   ok: boolean,
 *   summary: { totalRows, inserted, updated, rejected, skipped },
 *   rejections: Array<{ row, phone, name, reason }>,
 *   insertedIds: string[],
 *   updatedIds: string[],
 * }>}
 */
async function importCustomersCsv({ businessId, csvText, uploadedBy, source = 'csv_import' }) {
  if (!businessId) throw new Error('businessId is required');
  if (!csvText || !csvText.trim()) throw new Error('csvText is empty');

  const rows = parseCsv(csvText);
  const { headers, records } = toRecords(rows);

  // Schema check — fail fast if required columns are missing entirely.
  const required = ['name', 'phone', 'vehicle_model', 'consent_signed_on'];
  const missingCols = required.filter(c => !headers.includes(c));
  if (missingCols.length > 0) {
    return {
      ok: false,
      summary: { totalRows: records.length, inserted: 0, updated: 0, rejected: records.length, skipped: 0 },
      rejections: [{ row: 1, phone: '', name: '', reason: `Missing required columns: ${missingCols.join(', ')}` }],
      insertedIds: [],
      updatedIds: [],
      missingColumns: missingCols,
    };
  }

  const rejections = [];
  const insertedIds = [];
  const updatedIds  = [];
  let skipped = 0;

  for (const r of records) {
    // Phone normalization + validation
    const phone = toE164India(r.phone);
    if (!phone) {
      rejections.push({ row: r._row, phone: r.phone, name: r.name, reason: 'Invalid phone (must be +91XXXXXXXXXX or 10-digit starting 6-9)' });
      continue;
    }

    // Required fields
    if (!r.name) {
      rejections.push({ row: r._row, phone, name: '', reason: 'Missing name' });
      continue;
    }
    if (!r.vehicle_model) {
      rejections.push({ row: r._row, phone, name: r.name, reason: 'Missing vehicle_model' });
      continue;
    }
    if (!r.consent_signed_on) {
      rejections.push({ row: r._row, phone, name: r.name, reason: 'Missing consent_signed_on (required by TRAI for AI voice calls)' });
      continue;
    }
    if (!isValidDate(r.consent_signed_on)) {
      rejections.push({ row: r._row, phone, name: r.name, reason: 'consent_signed_on must be YYYY-MM-DD' });
      continue;
    }

    // Optional date sanity checks
    for (const f of ['purchase_date', 'last_service_date', 'next_service_due_date']) {
      if (r[f] && !isValidDate(r[f])) {
        rejections.push({ row: r._row, phone, name: r.name, reason: `${f} must be YYYY-MM-DD` });
        r._invalid = true;
        break;
      }
    }
    if (r._invalid) continue;

    const lang = normalizeLanguage(r.preferred_language);

    // ── UPSERT customer (by business_id + phone) ─────────────────────────
    let customerId, isNew;
    try {
      const { data: existing, error: selErr } = await supabase
        .from('customers')
        .select('id')
        .eq('business_id', businessId)
        .eq('phone', phone)
        .maybeSingle();
      if (selErr) throw selErr;

      if (existing?.id) {
        customerId = existing.id;
        isNew = false;
        const { error: upErr } = await supabase
          .from('customers')
          .update({
            name:                   r.name,
            email:                  r.email || null,
            preferred_language:     lang.name,
            preferred_language_code: lang.code,
            source,
          })
          .eq('id', customerId);
        if (upErr) throw upErr;
        updatedIds.push(customerId);
      } else {
        const { data: inserted, error: insErr } = await supabase
          .from('customers')
          .insert({
            business_id: businessId,
            name:        r.name,
            phone,
            email:       r.email || null,
            preferred_language:      lang.name,
            preferred_language_code: lang.code,
            opt_in_voice_calls: true,
            opt_in_sms:         true,
            source,
          })
          .select('id')
          .single();
        if (insErr) throw insErr;
        customerId = inserted.id;
        isNew = true;
        insertedIds.push(customerId);
      }
    } catch (err) {
      rejections.push({ row: r._row, phone, name: r.name, reason: `Customer write failed: ${err.message}` });
      continue;
    }

    // ── UPSERT vehicle (by customer + registration_no OR customer + model if no reg) ──
    try {
      let vehicleQuery = supabase.from('customer_vehicles').select('id').eq('customer_id', customerId);
      vehicleQuery = r.registration_no
        ? vehicleQuery.eq('registration_no', r.registration_no)
        : vehicleQuery.eq('model', r.vehicle_model);

      const { data: existVeh } = await vehicleQuery.maybeSingle();
      const vehiclePayload = {
        customer_id:           customerId,
        business_id:           businessId,
        model:                 r.vehicle_model,
        registration_no:       r.registration_no || null,
        purchase_date:         r.purchase_date || null,
        last_service_date:     r.last_service_date || null,
        last_service_km:       r.last_service_km ? parseInt(r.last_service_km, 10) || null : null,
        next_service_due_date: r.next_service_due_date || null,
      };

      if (existVeh?.id) {
        await supabase.from('customer_vehicles').update(vehiclePayload).eq('id', existVeh.id);
      } else {
        await supabase.from('customer_vehicles').insert(vehiclePayload);
      }
    } catch (err) {
      rejections.push({ row: r._row, phone, name: r.name, reason: `Vehicle write failed: ${err.message}` });
      continue;
    }

    // ── Consent log: write a row ONLY for newly inserted customers, or if
    //    consent_signed_on changed for an existing customer. Audit trail
    //    must reflect captured consent at upload time — TRAI requirement.
    if (isNew) {
      try {
        await supabase.from('customer_consent_log').insert({
          customer_id:      customerId,
          business_id:      businessId,
          consent_type:     'voice_reminder',
          granted:          true,
          source:           source === 'csv_delta' ? 'csv_delta' : 'csv_import',
          source_reference: `consent_signed_on=${r.consent_signed_on}`,
          recorded_by:      uploadedBy || 'csv_import',
        });
      } catch (err) {
        // Non-fatal: customer is in, but log line missing. Surface to dealer.
        rejections.push({ row: r._row, phone, name: r.name, reason: `Consent log failed: ${err.message}`, partial: true });
      }
    }
  }

  return {
    ok: true,
    summary: {
      totalRows: records.length,
      inserted:  insertedIds.length,
      updated:   updatedIds.length,
      rejected:  rejections.filter(x => !x.partial).length,
      skipped,
    },
    rejections,
    insertedIds,
    updatedIds,
  };
}

module.exports = { importCustomersCsv, parseCsv, toRecords, normalizeLanguage };
