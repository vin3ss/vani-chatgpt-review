/**
 * outboundCtx.js — Shared in-memory store for per-call outbound context.
 *
 * Used by:
 *   - index.js demo trigger        (POST /api/demo/outbound-call)
 *   - index.js Vobiz XML webhook   (POST /vobiz/xml-outbound)
 *   - index.js outbound WSS        (WSS /call/outbound)
 *   - reminderDispatcher.js        (production queue → dial)
 *
 * Extracted from index.js 2026-05-23 so the dispatcher can hand customer
 * context to the WSS handler the same way the demo endpoint does, without
 * requiring a Supabase round-trip mid-call.
 *
 * Production hardening (later): swap this Map for a Supabase `outbound_calls`
 * row keyed on call_uuid, so multi-instance Railway deploys share state.
 */

'use strict';

const OUTBOUND_CTX_TTL_MS = 10 * 60 * 1000; // 10 min — Vobiz queue + ring + early call
const store = new Map();

function putOutboundCtx(id, value) {
  store.set(id, { ...value, expiresAt: Date.now() + OUTBOUND_CTX_TTL_MS });
}

function getOutboundCtx(id) {
  const v = store.get(id);
  if (!v) return null;
  if (Date.now() > v.expiresAt) { store.delete(id); return null; }
  return v;
}

function deleteOutboundCtx(id) {
  store.delete(id);
}

// Periodic sweep — protects against memory bloat when ctx is created but never consumed
// (e.g. customer never picks up, Vobiz never hits the answer URL).
let sweeper = setInterval(() => {
  const now = Date.now();
  for (const [k, v] of store.entries()) {
    if (now > v.expiresAt) store.delete(k);
  }
}, 5 * 60 * 1000);
sweeper?.unref?.();

module.exports = { putOutboundCtx, getOutboundCtx, deleteOutboundCtx };
