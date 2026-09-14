// Phase 2 — computeCompressionVelocity.
//
// Turns two point-in-time samples of Phase 1's (commit ce104918)
// `parseVmStat` cumulative `compressions`/`decompressions` counters into a
// rate (`compressionsPerSec`) and a point-in-time ratio
// (`compressionRatio`), plus a `coldStart` flag the caller can branch on
// before trusting the rate.
//
// Pure, deterministic, no I/O: no `Date.now()`/`Math.random()`, elapsed time
// is derived entirely from the two `timestampMs` fields the caller supplies.
// See `./compression-velocity.jest.spec.mjs` for the full worked rationale
// behind every edge case below.

/**
 * @typedef {{ compressions: number, decompressions: number, timestampMs: number }} CompressionSample
 */

/**
 * A single `vm_stat` sample carries cumulative-since-boot counters, not a
 * per-interval delta — a rate needs two samples spanning a known interval.
 * `compressionRatio`, by contrast, is a point-in-time snapshot (how many
 * decompressions has each compression required so far) and needs only
 * `current`, so it is always computed, cold start or not.
 *
 * Edge cases, each pinned by a dedicated test in
 * `./compression-velocity.jest.spec.mjs`:
 *
 *   - `previous === null` (cold start, e.g. this is the orchestrator's first
 *     beat) — there is no prior sample to diff against, so
 *     `compressionsPerSec` is reported as `0` (not `NaN`, not a fabricated
 *     rate) with `coldStart: true` so the caller can distinguish "genuinely
 *     idle" from "no data yet".
 *   - `current.timestampMs <= previous.timestampMs` (same-timestamp or the
 *     clock stepping backward between beats) — dividing by a zero or
 *     negative elapsed time would produce `Infinity`/`NaN`/a negative rate;
 *     none of those are meaningful, so this is treated as cold-start-
 *     equivalent for the rate (`compressionsPerSec: 0`) *and* reported via
 *     `coldStart: true` — the elapsed-time arithmetic is just as untrustworthy
 *     as having no `previous` sample at all, so a caller can rely on
 *     `coldStart` alone (no need to separately inspect `compressionsPerSec`)
 *     to detect any case where the rate is not a real reading.
 *   - `current.compressions < previous.compressions` (a reboot happened
 *     between beats and the cumulative counter reset) — a negative delta
 *     divided by elapsed time would report a negative rate, which is never
 *     meaningful for a monotonically-cumulative counter; clamp to `0` rather
 *     than let the reboot masquerade as compression pressure reversing.
 *   - `current.decompressions === 0` — `compressions / decompressions` would
 *     be `Infinity`, which silently reads as "very high pressure" to a naive
 *     consumer; `NaN` is the honest answer ("ratio undefined, not
 *     evaluable"), matching Phase 1's `parseVmStat` NaN-sentinel convention.
 *   - any NaN input (`current`'s own fields, or `previous`'s, e.g. a stale
 *     unparseable `vm_stat` sample from Phase 1) — propagates into the
 *     corresponding output field via ordinary NaN-poisoned arithmetic; never
 *     coerced to a `0` that would misrepresent a real reading of "no
 *     compression activity".
 *
 * @param {{ previous: CompressionSample | null, current: CompressionSample }} args
 * @returns {{ compressionsPerSec: number, compressionRatio: number, coldStart: boolean }}
 *   `compressionsPerSec` is windowed (delta over `previous`→`current`);
 *   `compressionRatio` is lifetime-cumulative (from `current` alone) —
 *   the two do not share a time window.
 */
export function computeCompressionVelocity({ previous, current }) {
  const compressionRatio =
    current.decompressions === 0 ? NaN : current.compressions / current.decompressions;

  if (previous === null) {
    return { compressionsPerSec: 0, compressionRatio, coldStart: true };
  }

  const elapsedMs = current.timestampMs - previous.timestampMs;
  const compressionsDelta = current.compressions - previous.compressions;

  // NaN propagates honestly through this comparison chain: any NaN operand
  // makes every relational comparison below `false`, so a NaN elapsed time or
  // delta falls through to the final NaN-poisoned division rather than being
  // silently treated as "elapsed <= 0" or "delta < 0" and clamped to 0.
  const isDegenerateElapsed = elapsedMs <= 0;

  const compressionsPerSec = isDegenerateElapsed
    ? 0
    : compressionsDelta < 0
      ? 0
      : compressionsDelta / (elapsedMs / 1000);

  // Same-timestamp or backward-clock elapsed time is just as untrustworthy as
  // having no `previous` sample at all — fold it into `coldStart` so that
  // flag alone (not `compressionsPerSec`) tells the caller "not a real rate
  // reading". The counter-decrease/reboot case (`compressionsDelta < 0` with
  // a genuinely positive `elapsedMs`) stays `coldStart: false`: `previous`
  // exists and the elapsed-time arithmetic isn't degenerate there, only the
  // counter itself reset.
  return { compressionsPerSec, compressionRatio, coldStart: isDegenerateElapsed };
}
