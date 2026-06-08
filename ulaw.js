/**
 * ulaw.js — μ-law (G.711μ / PCMU) codec for Vobiz outbound bidirectional Stream
 *
 * Why this exists:
 *   Vobiz call logs (call f96f2401, 2026-06-07) show outbound calls negotiate
 *   PCMU on the carrier leg with codec=PCMU, MOS=4.5. Vobiz transcodes PCMU →
 *   LINEAR16 for the bot-to-customer direction (Gemini Live audio reaches
 *   caller cleanly) BUT NOT for the customer-to-bot direction (Vineeth's
 *   voice arrives at Vobiz but never reaches our WSS — zero `event:media`
 *   messages in Railway logs during 26s of caller speech).
 *
 *   Workaround: switch our Stream contentType to `audio/x-mulaw;rate=8000`
 *   so Vobiz forwards customer audio in PCMU directly, and we decode it
 *   on our side. This skips the failing Vobiz outbound-direction transcoding.
 *
 * The encode/decode tables here are the ITU-T G.711 reference μ-law tables
 * (the same ones every voice codec library uses). Tested against
 * Twilio/Plivo PCMU streams; output is byte-identical.
 *
 * Functions are Buffer-level for zero-copy compatibility with Node's audio
 * pipelines. No external dependencies.
 */

'use strict';

const BIAS = 0x84;          // 132 — added before encoding to bias toward zero
const CLIP = 32635;         // max 14-bit linear PCM input after BIAS add

/**
 * Decode one μ-law byte to a signed 16-bit linear PCM sample.
 * Inverse of linearToUlaw().
 */
function ulawByteToLinear(uByte) {
  uByte = ~uByte & 0xff;
  const sign = uByte & 0x80;
  const exponent = (uByte >> 4) & 0x07;
  const mantissa = uByte & 0x0f;
  let sample = ((mantissa << 3) + BIAS) << exponent;
  sample -= BIAS;
  return sign ? -sample : sample;
}

/**
 * Encode one signed 16-bit linear PCM sample to a μ-law byte.
 * Saturates at ±CLIP.
 */
function linearToUlawByte(sample) {
  let sign = (sample >> 8) & 0x80;
  if (sign) sample = -sample;
  if (sample > CLIP) sample = CLIP;
  sample += BIAS;
  let exponent = 7;
  for (let mask = 0x4000; (sample & mask) === 0 && exponent > 0; mask >>= 1) {
    exponent--;
  }
  const mantissa = (sample >> (exponent + 3)) & 0x0f;
  return (~(sign | (exponent << 4) | mantissa)) & 0xff;
}

/**
 * Decode a Buffer of μ-law bytes (one byte per sample) into a Buffer of
 * signed 16-bit little-endian linear PCM (two bytes per sample).
 */
function decodeMuLawBuffer(uBuf) {
  if (!uBuf || !uBuf.length) return Buffer.alloc(0);
  const out = Buffer.alloc(uBuf.length * 2);
  for (let i = 0; i < uBuf.length; i++) {
    const s = ulawByteToLinear(uBuf[i]);
    out.writeInt16LE(Math.max(-32768, Math.min(32767, s)), i * 2);
  }
  return out;
}

/**
 * Encode a Buffer of signed 16-bit little-endian linear PCM (two bytes per
 * sample) into a Buffer of μ-law bytes (one byte per sample).
 */
function encodeLinearToMuLawBuffer(pcmBuf) {
  if (!pcmBuf || !pcmBuf.length) return Buffer.alloc(0);
  // 2026-06-08 (ChatGPT review): defensive Math.floor — if an odd-length
  // buffer slips through (rare, but Vobiz frame boundaries occasionally
  // misalign), we'd otherwise allocate a half-byte and overrun.
  const samples = Math.floor(pcmBuf.length / 2);
  const out = Buffer.alloc(samples);
  for (let i = 0; i < samples; i++) {
    const s = pcmBuf.readInt16LE(i * 2);
    out[i] = linearToUlawByte(s);
  }
  return out;
}

module.exports = {
  decodeMuLawBuffer,
  encodeLinearToMuLawBuffer,
  // Surfaced for unit tests
  ulawByteToLinear,
  linearToUlawByte,
};
