// Phase 1 — pure leak-velocity slope + sustained-trip circuit breaker.
//
// This module is arithmetic ONLY: `computeLeakVelocity` is a pure function —
// no filesystem access, no locking, no imports beyond plain JS, no I/O. It
// takes the current persisted `consecutiveAboveThreshold` counter (threaded
// through a footprint-state.mjs-shaped per-pid trajectory entry) plus this
// beat's threshold config, and returns the next persisted counter alongside
// the trip decision. A future phase owns reading/writing the persisted
// per-pid state before/after calling this function — this module never
// touches a file itself. Mirrors `aimd-ceiling.mjs`'s `computeAimdCeiling`
// shape deliberately (see that module's doc comment for the house style this
// one follows).
//
// Slope computation: `growthRateMbPerMin` is derived from the FIRST and LAST
// valid (finite, non-null) samples in `state.trajectory`, over an elapsed
// span in minutes computed as `lastPolledAt - windowStartAt` (never an
// assumed fixed poll interval), where `windowStartAt` is `state.windowStartAt`
// when supplied, else `state.firstPolledAt` as a fallback approximation.
// KNOWN LIMITATION: `footprint-state.mjs`'s `FootprintStateEntry#firstPolledAt`
// is set once at cold start and never adjusted, while `trajectory` is capped
// at `TRAJECTORY_CAP` (120, see `footprint-trajectory.mjs`) with the oldest
// entries dropped first. Once a pid has been polled more than
// `TRAJECTORY_CAP` times, `firstPolledAt` no longer corresponds to the oldest
// SURVIVING sample in `trajectory` — it predates it — so falling back to it
// systematically UNDER-estimates the rate (the denominator is inflated by the
// truncated-away history). This module is pure and self-contained (Phase 1
// scope) and does not track per-sample timestamps itself; a caller that has
// computed the true timestamp of the oldest surviving `trajectory` sample can
// pass it as `state.windowStartAt` to get an accurate rate. That wiring is
// left to a later phase, where the truncation-aware timestamp can actually be
// tracked (see `footprint-state.mjs`). `null` entries (failed polls, per
// `footprint-sampler.mjs`/`footprint-state.mjs`) are skipped when locating the
// first/last valid sample; they never corrupt the computed rate, mirroring
// `footprint-trajectory.mjs`'s own "skip nulls without corrupting state"
// contract.
//
// Trip rule (mirrors computeAimdCeiling's AIMD sustained-window shape
// exactly, substituting "above growth threshold" for "Normal pressure"):
//   - When a rate IS computable and `growthRateMbPerMin >=
//     config.thresholdMbPerMin`, the persisted `consecutiveAboveThreshold`
//     counter increments. Only once the incremented counter reaches
//     `config.consecutivePollsRequired` does `trip` become `true` — and the
//     counter resets to 0 on that same call, exactly like
//     `computeAimdCeiling`'s ceiling-increase reset.
//   - When a rate IS computable and below threshold, the counter fully
//     resets to 0 — never a decrement, never partial credit.
//   - When a rate is NOT computable (cold start: fewer than
//     `config.minSamples` valid trajectory samples; or a non-positive elapsed
//     interval), the counter ALSO fully resets to 0, and `trip` is always
//     `false`. An unmeasurable poll is treated the same as a confidently
//     below-threshold poll for reset purposes, but is distinguished by
//     `growthRateMbPerMin: null` in the result.
//   - EXCEPTION — a failed (`null`) individual sample folded into an
//     otherwise-computable trajectory does NOT, by itself, interrupt an
//     in-progress above-threshold streak: it is simply skipped when locating
//     the first/last valid sample, and the trip decision follows whatever
//     rate is computed from the remaining valid samples exactly as it would
//     without the null present. Only a genuinely-computed below-threshold (or
//     genuinely-unmeasurable, e.g. too few valid samples or a degenerate
//     interval) result resets the counter.
//
// Because `consecutiveAboveThreshold` is a separately persisted scalar
// (threaded call-to-call by the caller, not derived from
// `state.trajectory.length`), a `consecutivePollsRequired` at or beyond
// `TRAJECTORY_CAP` (120, see `footprint-trajectory.mjs`) is always reachable
// — the counter has no dependency on, or ceiling from, the trajectory array's
// bounded length.

/**
 * Computes the next leak-velocity slope + sustained-trip state from the
 * current persisted state and this beat's threshold config.
 *
 * @param {{
 *   trajectory: (number|null)[],
 *   firstPolledAt: number,
 *   lastPolledAt: number,
 *   windowStartAt?: number,
 *   consecutiveAboveThreshold: number,
 * }} state
 *   `trajectory`'s `(number|null)[]` shape (with `null` entries skipped as
 *   failed polls) is a defensive superset this module accepts for robustness
 *   / a possible future caller — it is NOT a claim that this mirrors
 *   `footprint-state.mjs`'s `FootprintStateEntry#trajectory` today: that
 *   typedef declares `trajectory: number[]` and `recordFootprintPoll` never
 *   pushes a `null` placeholder for a failed poll (a failed poll is simply
 *   skipped and never appears in the array at all). `firstPolledAt`/
 *   `lastPolledAt` are ms epoch timestamps for this pid's first-ever and most
 *   recent poll. `windowStartAt` (optional) is the ms epoch timestamp of the
 *   oldest sample still SURVIVING in `trajectory` after `TRAJECTORY_CAP`
 *   truncation, when a caller has computed it; when absent, `firstPolledAt`
 *   is used as an approximation that may under-count the elapsed span (and
 *   therefore under-estimate the rate) once `trajectory` has truncated past
 *   `TRAJECTORY_CAP`. `consecutiveAboveThreshold` is the persisted counter
 *   carried in from the previous call's result.
 * @param {{
 *   thresholdMbPerMin: number,
 *   consecutivePollsRequired: number,
 *   minSamples?: number,
 * }} config
 *   `minSamples` (default 2) is the minimum count of valid (non-null,
 *   finite) trajectory samples required before a rate is even attempted —
 *   fewer than this is a cold start: never trips, never throws.
 * @returns {{
 *   trip: boolean,
 *   growthRateMbPerMin: number | null,
 *   consecutiveAboveThreshold: number,
 * }}
 */
export function computeLeakVelocity(state, config) {
  const { trajectory, firstPolledAt, lastPolledAt, windowStartAt } = state;
  const { thresholdMbPerMin, consecutivePollsRequired, minSamples = 2 } = config;

  const windowStart = windowStartAt ?? firstPolledAt;
  const growthRateMbPerMin = computeGrowthRateMbPerMin(trajectory, windowStart, lastPolledAt, minSamples);

  if (growthRateMbPerMin === null || growthRateMbPerMin < thresholdMbPerMin) {
    return {
      trip: false,
      growthRateMbPerMin,
      consecutiveAboveThreshold: 0,
    };
  }

  const nextConsecutiveAboveThreshold = state.consecutiveAboveThreshold + 1;

  if (nextConsecutiveAboveThreshold >= consecutivePollsRequired) {
    return {
      trip: true,
      growthRateMbPerMin,
      consecutiveAboveThreshold: 0,
    };
  }

  return {
    trip: false,
    growthRateMbPerMin,
    consecutiveAboveThreshold: nextConsecutiveAboveThreshold,
  };
}

/**
 * Locates the first and last valid (finite, non-null) samples in
 * `trajectory` and computes MB/min over `lastPolledAt - windowStartAt`.
 * Returns `null` when unmeasurable: fewer than `minSamples` valid samples,
 * or a non-positive elapsed interval (guards against divide-by-zero,
 * Infinity, and NaN on a zero-length or corrupt/negative span).
 *
 * `windowStartAt` is caller-supplied — either the true timestamp of the
 * oldest surviving `trajectory` sample, or (today, always) a fallback to
 * `firstPolledAt`. See this module's header comment for why the fallback is
 * an accepted approximation, not the "actual elapsed wall-clock span" of the
 * surviving samples.
 *
 * @param {(number|null)[]} trajectory
 * @param {number} windowStartAt
 * @param {number} lastPolledAt
 * @param {number} minSamples
 * @returns {number | null}
 */
function computeGrowthRateMbPerMin(trajectory, windowStartAt, lastPolledAt, minSamples) {
  const validSamples = trajectory.filter((sample) => typeof sample === 'number' && Number.isFinite(sample));

  if (validSamples.length < minSamples) return null;

  const elapsedMs = lastPolledAt - windowStartAt;
  if (elapsedMs <= 0) return null;

  const elapsedMin = elapsedMs / 60_000;
  const firstValid = validSamples[0];
  const lastValid = validSamples[validSamples.length - 1];

  return (lastValid - firstValid) / elapsedMin;
}

/**
 * Starting-hypothesis defaults for the leak-velocity circuit breaker — these
 * values are an unvalidated starting hypothesis, not a measured or tuned
 * default (no production telemetry has yet informed them). Callers may
 * override either field; this constant exists so a caller can plug it
 * straight into `computeLeakVelocity`'s `config` parameter without having to
 * invent a starting point of their own.
 *
 * @type {{ thresholdMbPerMin: number, consecutivePollsRequired: number }}
 */
export const DEFAULT_LEAK_VELOCITY_THRESHOLDS = {
  thresholdMbPerMin: 10,
  consecutivePollsRequired: 4,
};
