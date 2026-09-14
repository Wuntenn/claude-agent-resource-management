// Phase 1 — per-agent liveness signal (footprint-trajectory flatness
// + worktree mtime).
//
// `isAgentStalled` is a pure function: every input it needs is threaded in
// explicitly by the caller (mirrors `./threshold.mjs` and `./allowance.mjs`'s
// "plain mapping from explicit arguments to a return value" convention). No
// env-var reads, no `Date.now()`, no `fs.stat`/git shell-out — those seams
// belong to the CLI-level composition Phase 6 builds on top of this
// primitive.
//
// `worktreeMtimeMs` is accepted as an ALREADY-RESOLVED value: this module
// does not walk the worktree or know about noise paths (node_modules,
// `.git/index.lock`, lint/test caches). The caller is responsible for
// scanning only git-tracked, allowlisted paths and resolving the latest
// mtime (or `null` when unresolvable) before calling this function.
//
// Flags stalled only when BOTH signals independently indicate no activity:
//   (a) footprint-trajectory flatness — every sample in `footprintTrajectory`
//       (the EWMA-smoothed `phys_footprint` MB series `./footprint-state.mjs`
//       harvests from prior `--poll-footprint` beats — NOT a CPU-time/CPU-
//       seconds signal; there is no CPU metric fed into this module today)
//       is ~equal across at least `minSampleCount` samples.
//   (b) worktree mtime staleness — `worktreeMtimeMs` is strictly older than
//       `now - boundMs`.
//
// A `worktreeMtimeMs` of `null`/`undefined` (unresolvable worktree path)
// degrades to "unknown", not "stale" — footprint flatness alone is never
// sufficient to flag, since an unknown mtime signal carries no evidence
// either way.

/** Default minimum number of trajectory samples required before judging staleness at all — matches `./threshold.mjs`'s `ROLLING_WINDOW_MAX_LENGTH` convention of a small fixed minimum. */
export const DEFAULT_MIN_SAMPLE_COUNT = 3;

/** Tolerance (in the trajectory's own units) within which consecutive samples are considered "flat" rather than genuinely moving. */
export const FLATNESS_EPSILON = 1e-9;

/**
 * @param {unknown} value
 * @returns {value is number}
 */
function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

/**
 * True when every sample in `trajectory` is within `FLATNESS_EPSILON` of the
 * first sample — i.e. no genuine movement across the whole window. Note that
 * `trajectory` is the EWMA-smoothed `phys_footprint` MB series, not a raw
 * CPU-usage signal — see this module's header comment.
 *
 * A `minSampleCount` that is not a positive finite integer (e.g. `0`,
 * negative, or `NaN`) is treated as "insufficient evidence" and short-circuits
 * to `false` — without this guard, `trajectory.length < minSampleCount` and
 * `samples.length < minSampleCount` both evaluate to `false` for such values,
 * and `[].every(...)` on the resulting empty array vacuously returns `true`,
 * flagging a process with zero footprint samples as "footprint flat".
 *
 * @param {unknown} trajectory
 * @param {number} minSampleCount
 * @returns {boolean}
 */
function isCpuFlat(trajectory, minSampleCount) {
  if (!isFiniteNumber(minSampleCount) || !Number.isInteger(minSampleCount) || minSampleCount < 1) {
    return false;
  }
  if (!Array.isArray(trajectory) || trajectory.length < minSampleCount) return false;

  const samples = trajectory.filter(isFiniteNumber);
  if (samples.length < minSampleCount) return false;

  const first = samples[0];
  return samples.every((sample) => Math.abs(sample - first) <= FLATNESS_EPSILON);
}

/**
 * True only when `worktreeMtimeMs` is a resolved, finite timestamp strictly
 * older than `now - boundMs`. A `null`/`undefined`/non-finite value (an
 * unresolvable worktree path) is "unknown", never treated as stale.
 *
 * @param {unknown} worktreeMtimeMs
 * @param {number} now
 * @param {number} boundMs
 * @returns {boolean}
 */
function isMtimeStale(worktreeMtimeMs, now, boundMs) {
  if (!isFiniteNumber(worktreeMtimeMs)) return false;
  return worktreeMtimeMs < now - boundMs;
}

/**
 * Judges whether a single agent process looks stalled: no genuine CPU
 * movement AND no recent worktree file activity, both observed over at least
 * `minSampleCount` trajectory samples and `boundMs` of elapsed time.
 *
 * @param {{
 *   pid: number,
 *   boundMs: number,
 *   now: number,
 *   footprintTrajectory: number[],
 *   worktreeMtimeMs: number | null | undefined,
 *   minSampleCount?: number,
 * }} params
 * @returns {boolean}
 */
export function isAgentStalled({
  boundMs,
  now,
  footprintTrajectory,
  worktreeMtimeMs,
  minSampleCount = DEFAULT_MIN_SAMPLE_COUNT,
}) {
  if (!isFiniteNumber(boundMs) || !isFiniteNumber(now)) return false;

  const cpuFlat = isCpuFlat(footprintTrajectory, minSampleCount);
  if (!cpuFlat) return false;

  return isMtimeStale(worktreeMtimeMs, now, boundMs);
}
