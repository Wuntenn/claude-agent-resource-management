// Phase 4 — idle-probe trajectory classification.
//
// `classifyIdleProbeTrajectory` is a PURE function: window state in, one of
// five named outcomes out, no filesystem, no locking, no `Date.now()` — it
// mirrors `aimd-ceiling.mjs`/`leak-velocity.mjs`'s own "pure decision, caller
// owns I/O" house style exactly (see either file's header comment). It
// decides which of Phase 4's four TERMINAL trajectories an admitted
// idle-probe agent's bounded observation window landed in, plus a fifth,
// non-terminal "keep polling" signal for a mid-window call:
//
//   - 'degrading'    — `computeLeakVelocity` (./leak-velocity.mjs) trips
//                       DURING the window, before `state.windowElapsedMs`
//                       reaches `state.windowBoundMs`. `abortedEarly: true`.
//                       This is the one outcome deliberately allowed to fire
//                       mid-window — an early trip IS the verdict.
//   - 'observing'    — (code-review fix, High 2) no leak-velocity trip YET,
//                       and `state.windowElapsedMs < state.windowBoundMs`:
//                       the window hasn't run its course, so none of the
//                       three outcomes below may be decided yet. NOT one of
//                       the four named terminal outcomes; see the
//                       window-elapsed gate doc on the function itself, and
//                       `applyIdleProbeOutcome`'s guard that refuses this
//                       value.
//   - 'inconclusive' — the window bound elapsed with fewer than
//                       `state.minSamplesForConclusive` valid polls gathered
//                       (e.g. the host slept mid-probe). Checked BEFORE the
//                       flat/new-plateau split so a starved probe is never
//                       silently folded into 'flat' — see the Build Plan's
//                       own rationale, reproduced in this suite's header
//                       comment (`./idle-probe-trajectory.jest.spec.mjs`):
//                       collapsing "genuinely no change" and "not enough
//                       evidence" into one label would let a starved probe
//                       feed `computeAimdCeiling`'s additive-increase path on
//                       zero real evidence.
//   - 'new-plateau'  — the window completes (no leak-velocity trip, enough
//                       valid polls) with a measurable footprint delta (the
//                       trajectory's last valid sample minus its first valid
//                       sample) at or above `config.plateauDeltaThresholdMb`.
//   - 'flat'         — the window completes with no leak-velocity trip, no
//                       trip-worthy or inconclusive edge case, and no
//                       measurable plateau delta.
//
// `applyIdleProbeOutcome` is the IMPURE consequence-applier living alongside
// it in this same file (per the test-writer's documented "simplest starting
// shape" call — see this suite's header comment for why a reviewer should
// not block on splitting it into a second file). It always releases the
// Phase-3 `IDLE_PROBE_CLAIM_TYPE` claim (`./coordination-file.mjs`'s
// `releaseCapacity`, the same primitive `cli.mjs`'s other claim types release
// through — see `IDLE_PROBE_CLAIM_TYPE`'s own doc comment in `cli.mjs`), and
// then does exactly ONE of the following, keyed on `outcome`:
//
//   - 'degrading'/'inconclusive' — nothing further: no AIMD feed, no
//     recorded observation. A fresh explicit `--user-attests-idle` is
//     required to try again either way; the two outcomes are kept distinct
//     purely for the CALLER's own logging/framing (a degrading trajectory
//     implies "don't try yet", an inconclusive one implies "the window
//     itself was compromised, e.g. the host slept — no verdict either way"),
//     not because their side effects differ here.
//   - 'flat' — calls `computeAimdCeiling` (./aimd-ceiling.mjs) with
//     `context.aimdState`/`context.aimdConfig`, UNCHANGED from how an
//     organic (non-probe) sustained-normal beat already calls it — this is
//     the same function/state shape, not a parallel counter that merely
//     resembles it.
//   - 'new-plateau' — calls `recordObservation` (./history.mjs), tagged
//     `operationType: 'idle-probe'` (never `context.probedAgentClass`) and
//     `agentClass: 'idle-probe'`, so this observation can never contaminate
//     the probed agent class's own history / `estimateOperationCost` reads.
//     `context.finalFootprintMb` is written to `peakSmoothedFootprintMb`
//     (with `peakMemorySource: 'phys-footprint-ewma'`), NEVER to
//     `peakMemoryMb` (code-review fix, High 1 — see `history.mjs`'s own
//     `peakMemoryMb` vs. `peakSmoothedFootprintMb` provenance contract,
//     ~lines 402-419, the exact RSS-inversion metric-conflation class).
//     The empirical delta is additionally recorded as `plateauDeltaMb`
//     (code-review fix, Medium). pre-PR review fix (Medium — M3):
//     this value is read straight from `classification.plateauDeltaMb` —
//     the exact quantity `classifyIdleProbeTrajectory` used to decide
//     'new-plateau' over 'flat' — never independently recomputed from
//     caller-supplied context fields; a `context.baselineFootprintMb` field
//     no longer exists on this function's context param for exactly that
//     reason.
//
// COMPRESSION-VELOCITY JUDGMENT CALL (documented per the ticket's own
// instruction): the Build Plan describes classification as triggering on
// "leak-velocity OR compression-velocity trips", but `compression-velocity.mjs`
// (as of this phase) has no threshold/trip concept in production code at all
// — `computeCompressionVelocity` returns a rate and a ratio, never a boolean
// verdict, and nothing in the codebase trips on it today. This module
// therefore classifies using ONLY `computeLeakVelocity`'s existing trip
// mechanism; compression-velocity data is not consulted here at all. Adding
// a compression-velocity threshold/trip and folding it into this
// classification is left to a future ticket. This matches the test suite,
// which only ever exercises the leak-velocity trip path.

import { releaseCapacity, DEFAULT_CLAIM_TTL_MS } from './coordination-file.mjs';
import { computeLeakVelocity } from './leak-velocity.mjs';
import { computeAimdCeiling } from './aimd-ceiling.mjs';
import { recordObservation } from './history.mjs';

/**
 * The dedicated `operationType`/`agentClass` an idle-probe's own
 * `recordObservation` entry is tagged with — always distinct from whatever
 * real agent class was probed, so it can never contaminate that class's own
 * history / `estimateOperationCost` reads.
 *
 * @type {'idle-probe'}
 */
export const IDLE_PROBE_HISTORY_OPERATION_TYPE = 'idle-probe';

/**
 * Classifies one idle-probe's bounded observation window into one of four
 * TERMINAL named outcomes, or a fifth non-terminal 'observing' signal for a
 * mid-window call. Pure: no I/O, no `Date.now()`, no mutation of `state`.
 *
 * WINDOW-ELAPSED GATE (code-review fix, High 2 — matches the
 * "don't lock in a verdict on incomplete data" incident class): the
 * `'degrading'` outcome is deliberately allowed to fire mid-window — that is
 * the whole point of `abortedEarly`, an early-abort SIGNAL, not a race. The
 * other three named outcomes (`'inconclusive'`, `'flat'`, `'new-plateau'`)
 * are terminal verdicts about how the FULL window behaved and must never be
 * produced from partial, mid-window data. If the sampling loop (`cli.mjs`,
 * not yet wired) calls this function once per beat, as `abortedEarly` already
 * implies, a call with no leak-velocity trip and `windowElapsedMs <
 * windowBoundMs` returns the FIFTH, explicitly non-terminal `'observing'`
 * outcome instead — telling the caller "keep polling, no verdict yet". Only
 * once `windowElapsedMs >= windowBoundMs` (or a trip fires) does this
 * function decide between `'inconclusive'`/`'flat'`/`'new-plateau'`.
 * `'observing'` must never be passed to `applyIdleProbeOutcome` — see that
 * function's own guard.
 *
 * @param {{
 *   leakVelocity: {
 *     trajectory: (number|null)[],
 *     firstPolledAt: number,
 *     lastPolledAt: number,
 *     windowStartAt?: number,
 *     consecutiveAboveThreshold: number,
 *   },
 *   windowElapsedMs: number,
 *   windowBoundMs: number,
 *   validPollCount: number,
 *   minSamplesForConclusive: number,
 * }} state
 * @param {{
 *   leakVelocityThresholds: { thresholdMbPerMin: number, consecutivePollsRequired: number, minSamples?: number },
 *   plateauDeltaThresholdMb: number,
 * }} config
 * @returns {{
 *   outcome: 'degrading' | 'flat' | 'new-plateau' | 'inconclusive' | 'observing',
 *   leakVelocityResult: { trip: boolean, growthRateMbPerMin: number | null, consecutiveAboveThreshold: number },
 *   abortedEarly: boolean,
 *   plateauDeltaMb: number | null,
 * }}
 */
export function classifyIdleProbeTrajectory(state, config) {
  const { leakVelocity, windowElapsedMs, windowBoundMs, validPollCount, minSamplesForConclusive } = state;
  const { leakVelocityThresholds, plateauDeltaThresholdMb } = config;

  const leakVelocityResult = computeLeakVelocity(leakVelocity, leakVelocityThresholds);

  if (leakVelocityResult.trip) {
    return {
      outcome: 'degrading',
      leakVelocityResult,
      abortedEarly: windowElapsedMs < windowBoundMs,
      // Not applicable to a degrading verdict — the trip, not a footprint
      // delta, is what decided this outcome.
      plateauDeltaMb: null,
    };
  }

  if (windowElapsedMs < windowBoundMs) {
    // No trip yet, and the window hasn't run its course — there is nothing
    // terminal to say. See the window-elapsed gate doc above this function.
    return {
      outcome: 'observing',
      leakVelocityResult,
      abortedEarly: false,
      plateauDeltaMb: null,
    };
  }

  if (validPollCount < minSamplesForConclusive) {
    return {
      outcome: 'inconclusive',
      leakVelocityResult,
      abortedEarly: false,
      // Not applicable — too few polls to trust any delta this module could
      // compute from the trajectory.
      plateauDeltaMb: null,
    };
  }

  // pre-PR review fix (Medium — M3): this is now the SOLE place
  // `plateauDeltaMb` is computed. It is returned as part of this result so
  // `applyIdleProbeOutcome` can read the exact same value that decided the
  // 'flat' vs 'new-plateau' split below, rather than an uncoupled
  // recomputation from separately-supplied context fields that could
  // disagree with — or even invert the sign of — the value that actually
  // produced the verdict (the metric-provenance-conflation class
  // guards against; see this module's own header comment).
  const plateauDeltaMb = computePlateauDeltaMb(leakVelocity.trajectory);
  const outcome =
    plateauDeltaMb !== null && plateauDeltaMb >= plateauDeltaThresholdMb ? 'new-plateau' : 'flat';

  return {
    outcome,
    leakVelocityResult,
    abortedEarly: false,
    plateauDeltaMb,
  };
}

/**
 * The plateau delta this module measures: the last valid sample minus the
 * first valid sample in `trajectory`. `null` entries (failed polls) are
 * skipped, mirroring `leak-velocity.mjs`'s own "skip nulls without
 * corrupting state" convention. Returns `null` when fewer than two valid
 * samples exist — there is nothing to measure a delta between.
 *
 * @param {(number|null)[]} trajectory
 * @returns {number | null}
 */
function computePlateauDeltaMb(trajectory) {
  const validSamples = trajectory.filter((sample) => typeof sample === 'number' && Number.isFinite(sample));
  if (validSamples.length < 2) return null;
  return validSamples[validSamples.length - 1] - validSamples[0];
}

/**
 * @param {unknown} value
 * @returns {boolean}
 */
function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

/**
 * Applies the consequence of a classified idle-probe outcome: always
 * releases the Phase-3 `IDLE_PROBE_CLAIM_TYPE` claim, then does exactly one
 * of: nothing further ('degrading'/'inconclusive'), feed
 * `computeAimdCeiling` ('flat'), or `recordObservation` ('new-plateau'). See
 * this module's header comment for the full outcome-to-consequence mapping.
 *
 * `classification.outcome === 'observing'` (see
 * `classifyIdleProbeTrajectory`'s window-elapsed gate) is NEVER a valid
 * input here — it means the window hasn't finished, so there is no claim to
 * release yet and no consequence to apply. Passing it throws synchronously
 * rather than silently releasing the claim early.
 *
 * pre-PR review fix (Medium — M3): this now takes the FULL
 * `classifyIdleProbeTrajectory` result (not a bare outcome string) so the
 * `'new-plateau'` branch can read `plateauDeltaMb` from the SAME value that
 * decided the verdict, instead of independently recomputing it from
 * caller-supplied `context.finalFootprintMb`/`context.baselineFootprintMb`
 * — two differently-derived quantities sharing one name is exactly the
 * metric-provenance-conflation class guards against (see this
 * module's own header comment).
 *
 * @param {{
 *   outcome: 'degrading' | 'flat' | 'new-plateau' | 'inconclusive' | 'observing',
 *   plateauDeltaMb?: number | null,
 * }} classification The full return value of `classifyIdleProbeTrajectory`
 *   (or an object shaped like it) — never just the bare `outcome` string.
 * @param {{
 *   coordinationFilePath: string,
 *   historyFilePath?: string,
 *   orchestratorId: string,
 *   probedAgentClass: string,
 *   now: number,
 *   aimdState?: { pressureLevel: number, ceiling: number, sustainedNormalCount: number },
 *   aimdConfig?: { floor: number, ceilingMax: number, increaseStep: number, decreaseFactor: number, sustainedNormalBeatsRequired: number },
 *   finalFootprintMb?: number,
 * }} context `finalFootprintMb` is `--poll-footprint`-derived (EWMA-smoothed
 *   `phys_footprint`), never the legacy caller-supplied `--peak-memory-mb`
 *   flag — see the `'new-plateau'` branch below for how it is recorded, and
 *   `history.mjs`'s own `peakMemoryMb` vs. `peakSmoothedFootprintMb`
 *   provenance contract for why that distinction matters (the
 *   RSS-inversion incident). `baselineFootprintMb` is deliberately NOT read
 *   here any more — `plateauDeltaMb` comes from `classification` instead;
 *   see the M3 fix note above.
 * @returns {Promise<{
 *   released: number,
 *   aimdResult?: { ceiling: number, sustainedNormalCount: number },
 *   recorded?: boolean,
 * }>}
 */
export async function applyIdleProbeOutcome(classification, context) {
  const { outcome } = classification;

  if (outcome === 'observing') {
    throw new Error(
      "applyIdleProbeOutcome received outcome 'observing' — the window has not finished; " +
        'the caller must keep polling and must not release the claim or apply any consequence yet.',
    );
  }

  const { coordinationFilePath, orchestratorId, now } = context;

  // Phase 3's own `IDLE_PROBE_CLAIM_TYPE` claim, from `cli.mjs`, is a
  // single fixed slot (`type: 'idle-probe'`) — see that constant's doc
  // comment in `cli.mjs`. Every outcome releases it; only what happens
  // BESIDE the release differs.
  const releaseResult = await releaseCapacity(
    coordinationFilePath,
    { type: 'idle-probe', releaseCount: 1, orchestratorId, now },
    { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
  );

  const result = { released: releaseResult.released };

  if (outcome === 'flat') {
    result.aimdResult = computeAimdCeiling(context.aimdState, context.aimdConfig);
    return result;
  }

  if (outcome === 'new-plateau') {
    // `finalFootprintMb` is `--poll-footprint`-derived (EWMA-smoothed
    // phys_footprint), never the legacy `--peak-memory-mb` flag — per
    // `history.mjs`'s documented provenance contract (~lines 402-419), that
    // belongs in `peakSmoothedFootprintMb` + `peakMemorySource:
    // 'phys-footprint-ewma'`, NEVER conflated into `peakMemoryMb` (the
    // RSS-inversion incident this distinction exists to prevent).
    //
    // `plateauDeltaMb` additively records the empirical delta the Build Plan
    // asks for (final minus baseline footprint) rather than only the
    // absolute final value — `recordObservation` persists whatever shape
    // it's given via `{ ...observation }`, so this is a documented additive
    // field, not a schema change.
    //
    // pre-PR review fix (Medium — M3): read STRAIGHT from
    // `classification.plateauDeltaMb` — the exact value
    // `classifyIdleProbeTrajectory` used to decide 'new-plateau' over 'flat'
    // — rather than independently recomputing it from
    // `context.finalFootprintMb`/`context.baselineFootprintMb`. Two
    // differently-derived quantities sharing the `plateauDeltaMb` name with
    // nothing coupling them was exactly the defect this fix closes; see the
    // module header comment and `applyIdleProbeOutcome`'s own doc comment.
    const plateauDeltaMb = isFiniteNumber(classification.plateauDeltaMb)
      ? classification.plateauDeltaMb
      : undefined;

    await recordObservation(context.historyFilePath, {
      operationType: IDLE_PROBE_HISTORY_OPERATION_TYPE,
      orchestratorId,
      startedAt: now,
      endedAt: now,
      peakSmoothedFootprintMb: context.finalFootprintMb,
      peakMemorySource: 'phys-footprint-ewma',
      plateauDeltaMb,
      agentClass: IDLE_PROBE_HISTORY_OPERATION_TYPE,
    });
    result.recorded = true;
    return result;
  }

  // 'degrading' / 'inconclusive' — release only, no further consequence.
  return result;
}
