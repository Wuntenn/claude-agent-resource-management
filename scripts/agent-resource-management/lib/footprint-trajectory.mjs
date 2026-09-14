// Phase 2 — EWMA smoothing + bounded trajectory accumulator.
//
// Per the Build Plan's Phase 2 section, this module owns:
//   - ewmaSmooth(previousSmoothed, newSample, alpha) — pure EWMA smoothing
//     function, no side effects.
//   - createFootprintTrajectory() — a small stateful accumulator that folds
//     successive raw samples (numbers, or `null` for a failed poll) into a
//     smoothed series via ewmaSmooth(), skipping nulls without corrupting
//     state, tracking a running peak over SMOOTHED values (not raw), and
//     capping the retained trajectory array length at a fixed, documented
//     max (TRAJECTORY_CAP), dropping the oldest entries first.

/**
 * Bounds the trajectory array returned by `getTrajectory()` forever,
 * regardless of how many samples fold in over an agent's lifetime — the
 * oldest smoothed values are dropped first once this cap is exceeded.
 */
export const TRAJECTORY_CAP = 120;

/**
 * Default EWMA smoothing factor for both `createFootprintTrajectory` (below)
 * and `lib/footprint-state.mjs`'s `recordFootprintPoll` — the latter
 * reimplements this module's fold step (not its object shape) across
 * separate one-shot CLI processes, so it imports this constant rather than
 * redeclaring its own copy (whole-branch review, Medium finding: two
 * independently-declared `0.3` literals could silently diverge over time).
 */
export const DEFAULT_EWMA_ALPHA = 0.3;

/**
 * Pure exponentially-weighted moving average step. `previousSmoothed ===
 * null` seeds the series: the first sample becomes the smoothed value
 * outright (no division, nothing to blend against yet). Otherwise returns
 * `alpha * newSample + (1 - alpha) * previousSmoothed`.
 *
 * `alpha` must lie in `(0, 1]` — `alpha <= 0` would never move the smoothed
 * value at all (or move it backwards), and `alpha > 1` would overshoot past
 * the new sample. Both are almost certainly caller bugs, so this throws
 * rather than silently clamping.
 *
 * @param {number | null} previousSmoothed
 * @param {number} newSample
 * @param {number} alpha
 * @returns {number}
 */
export function ewmaSmooth(previousSmoothed, newSample, alpha) {
  if (!(alpha > 0 && alpha <= 1)) {
    throw new RangeError(`ewmaSmooth: alpha must be in (0, 1], got ${alpha}`);
  }

  if (previousSmoothed === null) return newSample;

  return alpha * newSample + (1 - alpha) * previousSmoothed;
}

/**
 * Creates a stateful accumulator that folds successive raw footprint samples
 * (a number, or `null` for a failed poll) into an EWMA-smoothed series,
 * tracking the running peak over the SMOOTHED values and retaining at most
 * `TRAJECTORY_CAP` of the most recent smoothed values.
 *
 * @param {{ alpha?: number }} [options]
 * @returns {{
 *   addSample(sample: number | null): void,
 *   getPeakSmoothedMb(): number | null,
 *   getTrajectory(): number[],
 * }}
 */
export function createFootprintTrajectory(options = {}) {
  const alpha = options.alpha ?? DEFAULT_EWMA_ALPHA;

  let smoothed = null;
  let peakSmoothedMb = null;
  const trajectory = [];

  return {
    addSample(sample) {
      if (sample === null) return;

      smoothed = ewmaSmooth(smoothed, sample, alpha);
      peakSmoothedMb = peakSmoothedMb === null ? smoothed : Math.max(peakSmoothedMb, smoothed);

      trajectory.push(smoothed);
      if (trajectory.length > TRAJECTORY_CAP) {
        trajectory.shift();
      }
    },

    getPeakSmoothedMb() {
      return peakSmoothedMb;
    },

    getTrajectory() {
      return [...trajectory];
    },
  };
}
