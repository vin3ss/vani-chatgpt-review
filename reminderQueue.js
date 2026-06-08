/**
 * reminderQueue.js — Service-reminder queue operations.
 *
 * Responsibilities:
 *   - generateForBusiness(businessId): run pg fn generate_daily_reminder_queue
 *   - listPending(businessId):         fetch pending_approval rows w/ customer+vehicle data
 *   - approveBatch(...):                bulk approve, write audit log
 *   - skipBatch(...):                   bulk skip (manager removed before approving)
 *
 * The dispatcher (reminderDispatcher.js) reads status='approved' rows
 * scheduled_for <= now() and dials them. This file owns the manager-facing
 * surface; the dispatcher owns the dialer-facing surface.
 *
 * Built 2026-05-23 — Phase 1.
 */

'use strict';

const { supabase } = require('./supabase');

// Default slots offered to customers. Manager can override per-row via dashboard.
const DEFAULT_SLOTS = ['tomorrow 10 AM', 'day-after 4 PM', 'Saturday 11 AM'];

/**
 * Run the daily queue generator for one business.
 * Calls the pg function generate_daily_reminder_queue.
 *
 * @param {string} businessId
 * @param {object} [opts]
 * @param {number} [opts.daysAhead=7]  How many days ahead to scan
 * @param {number} [opts.dndDays=30]   Days to suppress re-calls after booked/opted-out
 * @returns {Promise<{ insertedCount: number, businessId: string }>}
 */
async function generateForBusiness(businessId, opts = {}) {
  if (!businessId) throw new Error('businessId required');
  const { data, error } = await supabase.rpc('generate_daily_reminder_queue', {
    p_business_id: businessId,
    p_days_ahead:  opts.daysAhead ?? 7,
    p_dnd_days:    opts.dndDays   ?? 30,
  });
  if (error) throw new Error(`generate failed: ${error.message}`);
  const row = Array.isArray(data) ? data[0] : data;

  // totalPending = how many rows are CURRENTLY awaiting approval for this
  // business. The single most useful number for the upload result UX — it
  // answers "how many customers will Vani try to call once you approve?".
  // Independent of whether THIS upload caused changes (re-upload of identical
  // data returns inserted=0, updated=0, but totalPending might be 12).
  let totalPending = 0;
  try {
    const { count } = await supabase
      .from('reminder_queue')
      .select('id', { count: 'exact', head: true })
      .eq('business_id', businessId)
      .eq('status', 'pending_approval');
    totalPending = count || 0;
  } catch (_) { /* swallow — non-fatal for the upload flow */ }

  // updated_count was added 2026-05-26 — the pg function now refreshes
  // service_due_date on existing pending_approval rows when a CSV re-upload
  // moves the vehicle's next_service_due_date. Old function only returned
  // inserted_count, so default to 0 for forward-compat.
  return {
    insertedCount: row?.inserted_count ?? 0,
    updatedCount:  row?.updated_count  ?? 0,
    totalPending,
    businessId,
  };
}

/**
 * Generate queues for ALL active businesses. Returns per-business counts.
 * Called by the daily cron endpoint.
 */
async function generateForAll() {
  const { data: businesses, error } = await supabase
    .from('businesses')
    .select('id, name')
    .eq('status', 'active');
  if (error) throw new Error(`businesses fetch failed: ${error.message}`);

  const results = [];
  for (const b of businesses || []) {
    try {
      const r = await generateForBusiness(b.id);
      results.push({ businessId: b.id, name: b.name, ...r, ok: true });
    } catch (err) {
      results.push({ businessId: b.id, name: b.name, ok: false, error: err.message });
    }
  }
  return results;
}

/**
 * Fetch pending_approval rows for a business with joined customer + vehicle data.
 * Used by the dealer dashboard Outbound tab "Pending Approval" section.
 */
async function listPending(businessId, limit = 100) {
  if (!businessId) throw new Error('businessId required');
  const { data, error } = await supabase
    .from('reminder_queue')
    .select(`
      id, reminder_type, service_due_date, proposed_slots,
      status, scheduled_for, attempts, created_at,
      customer:customers ( id, name, phone, preferred_language ),
      vehicle:customer_vehicles ( id, model, registration_no, last_service_date, last_service_km )
    `)
    .eq('business_id', businessId)
    .eq('status', 'pending_approval')
    .order('service_due_date', { ascending: true })
    .limit(limit);
  if (error) throw new Error(`listPending failed: ${error.message}`);
  return data || [];
}

/**
 * Fetch any queue rows by status with joined data — used for In-flight / History tabs.
 */
async function listByStatus(businessId, statuses, limit = 200) {
  if (!businessId) throw new Error('businessId required');
  if (!Array.isArray(statuses) || statuses.length === 0) throw new Error('statuses required');
  const { data, error } = await supabase
    .from('reminder_queue')
    .select(`
      id, reminder_type, service_due_date, proposed_slots,
      status, outcome, scheduled_for, last_attempt_at, attempts,
      customer:customers ( id, name, phone, preferred_language ),
      vehicle:customer_vehicles ( id, model, registration_no )
    `)
    .eq('business_id', businessId)
    .in('status', statuses)
    .order('updated_at', { ascending: false })
    .limit(limit);
  if (error) throw new Error(`listByStatus failed: ${error.message}`);
  return data || [];
}

/**
 * Approve a batch of pending_approval rows. Two writes:
 *   1. UPDATE reminder_queue → status='approved', approved_by, approved_at
 *   2. INSERT one reminder_approval_log row (the audit artifact)
 *
 * Items NOT in approvedIds but currently pending_approval are marked 'skipped'.
 * (Dealer's intent: "everything not checked = don't call today.")
 *
 * @param {string} businessId
 * @param {string[]} approvedIds        UUIDs of rows to approve
 * @param {object} approver             { id, name, ipAddress, userAgent }
 * @param {string} [notes]              Free-text from manager
 * @returns {Promise<{ approvedCount, skippedCount, auditLogId }>}
 */
async function approveBatch(businessId, approvedIds, approver, notes) {
  if (!businessId) throw new Error('businessId required');
  if (!Array.isArray(approvedIds)) throw new Error('approvedIds must be array');

  const approvedAt = new Date().toISOString();

  // Step 1: fetch all currently pending rows so we know which to mark skipped.
  const { data: currentPending, error: penErr } = await supabase
    .from('reminder_queue')
    .select('id')
    .eq('business_id', businessId)
    .eq('status', 'pending_approval');
  if (penErr) throw new Error(`fetch pending failed: ${penErr.message}`);
  const pendingIds = (currentPending || []).map(r => r.id);

  const approvedSet = new Set(approvedIds);
  const skipIds = pendingIds.filter(id => !approvedSet.has(id));
  const realApproved = pendingIds.filter(id => approvedSet.has(id));

  // Step 2: approve
  //
  // CRITICAL: set scheduled_for = approvedAt at the same time. Without it,
  // pickDispatchable's filter `.lte('scheduled_for', nowIso)` treats NULL
  // as false in PostgREST/SQL — so approved rows with null scheduled_for
  // are invisible to the dispatcher forever. Production incident 2026-05-25:
  // 1 approved row sat undialed for 18 hours because of this. The pending
  // generator sets scheduled_for to 11 AM IST today, but an older generator
  // version may have left it null; the approve step now guarantees it's set.
  let approvedCount = 0;
  if (realApproved.length > 0) {
    const { error: appErr, count } = await supabase
      .from('reminder_queue')
      .update({
        status:         'approved',
        approved_by:    approver?.id || approver?.name || 'unknown',
        approved_at:    approvedAt,
        approval_notes: notes || null,
        scheduled_for:  approvedAt, // dispatch immediately on next tick
      }, { count: 'exact' })
      .in('id', realApproved);
    if (appErr) throw new Error(`approve failed: ${appErr.message}`);
    approvedCount = count ?? realApproved.length;
  }

  // Step 3: skip the rest
  let skippedCount = 0;
  if (skipIds.length > 0) {
    const { error: skipErr, count } = await supabase
      .from('reminder_queue')
      .update({
        status: 'skipped',
        approval_notes: notes || null,
      }, { count: 'exact' })
      .in('id', skipIds);
    if (skipErr) throw new Error(`skip failed: ${skipErr.message}`);
    skippedCount = count ?? skipIds.length;
  }

  // Step 4: audit log row
  const { data: logRow, error: logErr } = await supabase
    .from('reminder_approval_log')
    .insert({
      business_id:         businessId,
      approved_by:         approver?.id || null,
      approved_by_name:    approver?.name || null,
      approved_at:         approvedAt,
      queue_size_total:    pendingIds.length,
      queue_size_approved: approvedCount,
      queue_size_skipped:  skippedCount,
      ip_address:          approver?.ipAddress || null,
      user_agent:          approver?.userAgent || null,
      notes:               notes || null,
    })
    .select('id')
    .single();
  if (logErr) throw new Error(`audit log failed: ${logErr.message}`);

  return { approvedCount, skippedCount, auditLogId: logRow.id };
}

/**
 * Pick approved rows ready to dial right now. Used by the dispatcher.
 *
 * @param {string} businessId   (optional; null = all businesses)
 * @param {number} maxRows      cap per poll
 */
async function pickDispatchable(businessId, maxRows = 10) {
  const nowIso = new Date().toISOString();
  // Two-clause OR: dispatch when scheduled_for has arrived, OR when it's NULL
  // (legacy rows / defensive — approveBatch now always sets it, but this keeps
  // the pipeline working if any path forgets to set scheduled_for in future).
  let q = supabase
    .from('reminder_queue')
    .select(`
      id, business_id, scheduled_for, attempts, max_attempts, proposed_slots,
      reminder_type, service_due_date,
      customer:customers ( id, name, phone, preferred_language, preferred_language_code ),
      vehicle:customer_vehicles ( id, model, registration_no, last_service_date, last_service_km, next_service_due_date )
    `)
    .eq('status', 'approved')
    .or(`scheduled_for.is.null,scheduled_for.lte.${nowIso}`)
    .order('scheduled_for', { ascending: true, nullsFirst: true })
    .limit(maxRows);
  if (businessId) q = q.eq('business_id', businessId);
  const { data, error } = await q;
  if (error) throw new Error(`pickDispatchable failed: ${error.message}`);
  return data || [];
}

/**
 * Mark a row as dialing. Returns true if the optimistic update won the race
 * (status was still 'approved'); false if another worker grabbed it first.
 */
async function markDialing(queueId, callId) {
  const { data, error } = await supabase
    .from('reminder_queue')
    .update({ status: 'dialing', last_attempt_at: new Date().toISOString(), call_id: callId || null })
    .eq('id', queueId)
    .eq('status', 'approved')
    .select('id')
    .maybeSingle();
  if (error) throw new Error(`markDialing failed: ${error.message}`);
  return !!data;
}

/**
 * Record outcome after a call ends. Bumps attempts; flips to retry if applicable.
 */
async function recordOutcome(queueId, outcome, notes, bookedAppointmentId) {
  const terminal = new Set(['booked', 'opted_out', 'wrong_number']);

  // Fetch current attempts to decide retry
  const { data: current, error: getErr } = await supabase
    .from('reminder_queue')
    .select('attempts, max_attempts')
    .eq('id', queueId)
    .single();
  if (getErr) throw new Error(`fetch current failed: ${getErr.message}`);

  const newAttempts = (current.attempts || 0) + 1;
  let newStatus;
  if (terminal.has(outcome)) {
    newStatus = outcome === 'booked' ? 'completed' : (outcome === 'opted_out' ? 'opted_out' : 'completed');
  } else if (outcome === 'no_answer' && newAttempts < (current.max_attempts || 3)) {
    newStatus = 'approved'; // will retry tomorrow per Divya's cadence
  } else if (outcome === 'rescheduled') {
    newStatus = 'rescheduled';
  } else {
    newStatus = 'failed';
  }

  const { error: upErr } = await supabase
    .from('reminder_queue')
    .update({
      status:                newStatus,
      attempts:              newAttempts,
      outcome,
      outcome_notes:         notes || null,
      booked_appointment_id: bookedAppointmentId || null,
    })
    .eq('id', queueId);
  if (upErr) throw new Error(`recordOutcome failed: ${upErr.message}`);
  return { newStatus, attempts: newAttempts };
}

module.exports = {
  DEFAULT_SLOTS,
  generateForBusiness,
  generateForAll,
  listPending,
  listByStatus,
  approveBatch,
  pickDispatchable,
  markDialing,
  recordOutcome,
};
