// Phase 2 — pure AIMD (Additive-Increase/Multiplicative-Decrease)
// concurrency-ceiling adjustment.
//
// This module is arithmetic ONLY: `computeAimdCeiling` is a pure function —
// no filesystem access, no locking, no imports beyond plain JS, no awareness
// of per-agent-class or past-admission-decision concepts. It takes the current
// persisted ceiling state plus this beat's pressure signal, and returns the
// next persisted ceiling state. Phase 3 owns reading the persisted state (via
// this directory's I/O-owning module) before calling this function, and
// writing the result back out afterward — this module never touches a file
// itself.
//
// AIMD shape:
//   - Additive increase: on `pressureLevel === 1` (Normal), a
//     `sustainedNormalCount` counter increments. Only once it reaches
//     `config.sustainedNormalBeatsRequired` CONSECUTIVE Normal beats does the
//     ceiling actually grow, by `config.increaseStep`, clamped at
//     `config.ceilingMax`. The counter then resets to 0 — the next increase
//     requires a fresh run of `sustainedNormalBeatsRequired` consecutive
//     Normal beats, not merely staying above the threshold. This mirrors
//     classic AIMD/TCP congestion-avoidance pacing: growth happens once per
//     sustained-good-behavior window, not on every beat once a threshold is
//     crossed, so the ceiling ramps up cautiously rather than accelerating
//     unboundedly the longer pressure stays Normal.
//   - Multiplicative decrease: on `pressureLevel >= 2` (WARN or CRITICAL —
//     see the design-decision note in the test file this module satisfies),
//     the ceiling drops IMMEDIATELY on that same beat, no delay, by
//     `Math.floor(ceiling * config.decreaseFactor)`, clamped at
//     `config.floor`. WARN and CRITICAL use the exact same `decreaseFactor` —
//     deliberately not a steeper cut for CRITICAL, matching the rest of this
//     codebase's undifferentiated `pressureLevel >= 2` "not Normal" admission
//     band (see `cli.mjs`'s pressure-block logic). Any non-Normal beat also
//     fully resets `sustainedNormalCount` to 0 — a single WARN/CRITICAL beat
//     interrupting an otherwise-long Normal streak discards the whole streak,
//     not merely the most recent beat's contribution to it.
//
// Both the increase and decrease results are always integers:
// `Math.floor` is applied to the multiplicative decrease's result every
// time it runs, not only when the result happens to land past a clamp
// boundary — a ceiling is a count of agents, and a fractional agent count is
// never a meaningful value to persist.

/**
 * Computes the next AIMD ceiling state from the current persisted state and
 * this beat's pressure signal.
 *
 * @param {{ pressureLevel: number, ceiling: number, sustainedNormalCount: number }} state
 *   `pressureLevel` follows the existing codebase convention: 1 = Normal,
 *   2 = WARN, 3 = CRITICAL (>= 2 is "not Normal"). `ceiling` and
 *   `sustainedNormalCount` are the persisted state's current values.
 * @param {{ floor: number, ceilingMax: number, increaseStep: number, decreaseFactor: number, sustainedNormalBeatsRequired: number }} config
 *   All caller-supplied — this function bakes in no defaults of its own.
 * @returns {{ ceiling: number, sustainedNormalCount: number }}
 */
export function computeAimdCeiling(state, config) {
  const { pressureLevel, ceiling, sustainedNormalCount } = state;
  const { floor, ceilingMax, increaseStep, decreaseFactor, sustainedNormalBeatsRequired } = config;

  if (pressureLevel >= 2) {
    const decreased = Math.floor(ceiling * decreaseFactor);
    return {
      ceiling: Math.max(floor, decreased),
      sustainedNormalCount: 0,
    };
  }

  const nextSustainedNormalCount = sustainedNormalCount + 1;

  if (nextSustainedNormalCount >= sustainedNormalBeatsRequired) {
    return {
      ceiling: Math.min(ceilingMax, ceiling + increaseStep),
      sustainedNormalCount: 0,
    };
  }

  return {
    ceiling,
    sustainedNormalCount: nextSustainedNormalCount,
  };
}
