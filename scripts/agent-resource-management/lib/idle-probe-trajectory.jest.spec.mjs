// RED unit tests for the not-yet-implemented
// scripts/agent-resource-management/lib/idle-probe-trajectory.mjs module
// (Phase 4 — "Idle-probe trajectory classification").
//
// Per the Build Plan's Phase 4 section + Convergence Analysis edge cases,
// this module is expected to own the CLASSIFICATION DECISION for an admitted
// idle-probe agent (Phase 3, `IDLE_PROBE_CLAIM_TYPE` in cli.mjs) observed
// over a bounded window (~10 min, configurable): given a window's worth of
// leak-velocity/compression-velocity/footprint state, decide which of the
// ticket's three named trajectories occurred, and apply that outcome's
// consequence (release the Phase-3 claim; feed a positive signal into the
// EXISTING organic AIMD additive-increase path; or record an empirical
// observation tagged so it never contaminates the probed agent class's real
// cost-estimate history).
//
// WHERE THIS LOGIC SHOULD LIVE (documented for the implementer — this file
// only writes tests, per this agent's remit):
//
//   - `classifyIdleProbeTrajectory(state, config)` belongs in a NEW pure
//     module, `./idle-probe-trajectory.mjs` — window state in, one
//     classification out, no I/O, no filesystem, no `Date.now()`. This
//     mirrors `aimd-ceiling.mjs`/`leak-velocity.mjs`'s own "pure decision,
//     caller owns I/O" house style exactly (see either file's own header
//     comment) — the classification decision itself should not know how to
//     write a claim release or a history observation.
//   - `applyIdleProbeOutcome(outcome, context)` — the IMPURE consequence
//     applier — can live in the SAME file (it is Phase 4's own new
//     orchestration surface, not yet living anywhere) or be split into a
//     sibling module; this suite imports it from `./idle-probe-trajectory.mjs`
//     as the simplest starting shape, but a reviewer should not block on the
//     implementer choosing a second file instead.
//   - The per-beat SAMPLING LOOP that gathers ~10 minutes of leak-velocity/
//     compression-velocity/footprint polls and repeatedly calls
//     `classifyIdleProbeTrajectory` belongs in `cli.mjs`'s `main()` (a new
//     `--desired-agents` sub-path or a dedicated flag) — that orchestration
//     is NOT covered by this file. This file's `applyIdleProbeOutcome` tests
//     DO exercise the claim/release fold-back into Phase 3's real
//     coordination-file primitives (`claimCapacity`/`releaseCapacity`)
//     directly, since that fold-back is squarely this phase's own scope; see
//     its Build Plan Phase 4 test-strategy table for the remaining
//     subprocess-driven cli.mjs-level scenarios still owed once this pure
//     layer exists (e.g. wiring `classifyIdleProbeTrajectory` into an actual
//     multi-beat `--desired-agents` loop end to end).
//
// Designed function signatures (documented here since neither function
// exists yet):
//
//   classifyIdleProbeTrajectory(
//     state: {
//       leakVelocity: {
//         trajectory: (number|null)[],
//         firstPolledAt: number,
//         lastPolledAt: number,
//         windowStartAt?: number,
//         consecutiveAboveThreshold: number,
//       },
//       windowElapsedMs: number,
//       windowBoundMs: number,
//       validPollCount: number,
//       minSamplesForConclusive: number,
//     },
//     config: {
//       leakVelocityThresholds: { thresholdMbPerMin: number, consecutivePollsRequired: number, minSamples?: number },
//       plateauDeltaThresholdMb: number,
//     },
//   ): {
//     outcome: 'degrading' | 'flat' | 'new-plateau' | 'inconclusive',
//     leakVelocityResult: { trip: boolean, growthRateMbPerMin: number|null, consecutiveAboveThreshold: number },
//     abortedEarly: boolean,
//   }
//
//   applyIdleProbeOutcome(
//     classification: {
//       outcome: 'degrading' | 'flat' | 'new-plateau' | 'inconclusive' | 'observing',
//       plateauDeltaMb?: number | null,
//     }, // the FULL classifyIdleProbeTrajectory() return value —
//        // pre-PR review fix (Medium — M3): 'new-plateau' reads
//        // plateauDeltaMb from THIS object, never recomputed independently
//        // from context, so the recorded delta can never disagree with the
//        // value that actually produced the verdict.
//     context: {
//       coordinationFilePath: string,
//       historyFilePath?: string,
//       orchestratorId: string,
//       probedAgentClass: string,
//       now: number,
//       aimdState?: { pressureLevel: number, ceiling: number, sustainedNormalCount: number },
//       aimdConfig?: { floor: number, ceilingMax: number, increaseStep: number, decreaseFactor: number, sustainedNormalBeatsRequired: number },
//       finalFootprintMb?: number,
//     },
//   ): Promise<{
//     released: number,
//     aimdResult?: { ceiling: number, sustainedNormalCount: number },
//     recorded?: boolean,
//   }>
//
// Classification rules this suite pins:
//
//   1. DEGRADING — leak-velocity's own EXISTING sustained-trip threshold
//      (`computeLeakVelocity`, `./leak-velocity.mjs`) trips DURING the
//      window (before `windowBoundMs` elapses) -> outcome 'degrading',
//      `abortedEarly: true`. `applyIdleProbeOutcome('degrading', ...)`
//      releases the Phase-3 `IDLE_PROBE_CLAIM_TYPE` claim and does nothing
//      else (no AIMD feed, no recordObservation) — the whole point is a
//      fresh explicit `--user-attests-idle` is required to try again.
//
//   2. FLAT — the window completes (or is inconclusive — see rule 4) with no
//      leak-velocity trip and no measurable plateau delta -> outcome 'flat'.
//      `applyIdleProbeOutcome('flat', ...)` calls `computeAimdCeiling`
//      (the SAME function/state shape the organic non-probe path already
//      uses — see the direct equivalence test below) as a Normal-pressure
//      beat, and ALSO releases the claim.
//
//   3. NEW-PLATEAU — the window completes with a measurable footprint delta
//      (>= `config.plateauDeltaThresholdMb`) and no leak-velocity trip ->
//      outcome 'new-plateau'. `applyIdleProbeOutcome('new-plateau', ...)`
//      calls `recordObservation` (`./history.mjs`) tagged
//      `agentClass: 'idle-probe'` under an `operationType` DISTINCT from
//      `context.probedAgentClass` (see the contamination test below), and
//      releases the claim.
//
//   4. INCONCLUSIVE (this suite's own documented, conservative choice for
//      the Build Plan's "window expires with fewer than minSamples valid
//      polls" edge case — e.g. the host slept mid-probe) — fewer than
//      `state.minSamplesForConclusive` valid polls were gathered by the time
//      `windowBoundMs` elapses -> outcome 'inconclusive', NOT 'flat'. This is
//      deliberately a 4th, HONEST outcome rather than silently reusing
//      'flat': collapsing "we genuinely observed no change" and "we didn't
//      observe enough to know" into the same label would let a starved
//      probe (e.g. the host slept for 9 of its 10 minutes) feed
//      `computeAimdCeiling`'s additive-increase path on ZERO real evidence.
//      `applyIdleProbeOutcome('inconclusive', ...)` releases the claim and
//      does nothing else (same non-consequence as 'degrading', but WITHOUT
//      'degrading''s "no further probes" framing — an inconclusive probe due
//      to e.g. a sleeping host is not evidence of a bad trajectory).
//
// Window-bound configurability: `state.windowBoundMs` is an ordinary,
// caller-supplied field, never a hardcoded ~10-minute literal inside the
// module — this suite asserts a non-default value changes behaviour.

import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { computeAimdCeiling } from './aimd-ceiling.mjs';
import { computeLeakVelocity, DEFAULT_LEAK_VELOCITY_THRESHOLDS } from './leak-velocity.mjs';
import { claimCapacity, DEFAULT_CLAIM_TTL_MS } from './coordination-file.mjs';
import { readHistory } from './history.mjs';

// Import target does not exist yet — this import itself is expected to
// throw a module-not-found error, which is why every test in this file is
// RED for the same underlying reason: the whole orchestration layer is new.
import {
  classifyIdleProbeTrajectory,
  applyIdleProbeOutcome,
} from './idle-probe-trajectory.mjs';

const AIMD_CONFIG = {
  floor: 1,
  ceilingMax: 32,
  increaseStep: 1,
  decreaseFactor: 0.5,
  sustainedNormalBeatsRequired: 5,
};

const PLATEAU_DELTA_THRESHOLD_MB = 50;

let workDir;
let coordinationFilePath;
let historyFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-idle-probe-trajectory-'));
  coordinationFilePath = join(workDir, 'coordination.json');
  historyFilePath = join(workDir, 'history.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

describe('classifyIdleProbeTrajectory — degrading trajectory', () => {
  it('classifies "degrading" and aborts early when leak-velocity trips before windowBoundMs elapses', () => {
    // A runaway trajectory: every sample climbs fast enough (well above
    // DEFAULT_LEAK_VELOCITY_THRESHOLDS.thresholdMbPerMin) to trip
    // computeLeakVelocity's own sustained-above-threshold counter within a
    // handful of polls — long before the ~10 minute window would otherwise
    // elapse.
    const trajectory = [100, 400, 700, 1000]; // steep climb over a short span
    const result = classifyIdleProbeTrajectory(
      {
        leakVelocity: {
          trajectory,
          firstPolledAt: 0,
          lastPolledAt: 60_000, // 1 minute elapsed — far short of a 10 min window
          consecutiveAboveThreshold: DEFAULT_LEAK_VELOCITY_THRESHOLDS.consecutivePollsRequired - 1,
        },
        windowElapsedMs: 60_000,
        windowBoundMs: 10 * 60_000,
        validPollCount: trajectory.length,
        minSamplesForConclusive: 3,
      },
      {
        leakVelocityThresholds: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
        plateauDeltaThresholdMb: PLATEAU_DELTA_THRESHOLD_MB,
      },
    );

    expect(result.outcome).toBe('degrading');
    expect(result.abortedEarly).toBe(true);
    expect(result.leakVelocityResult.trip).toBe(true);
  });

  it('applyIdleProbeOutcome("degrading", ...) releases the IDLE_PROBE_CLAIM_TYPE claim and records nothing', async () => {
    const orchestratorId = 'idle-probe-degrading';
    const now = 1_000_000;

    // First, admit exactly the way Phase 3's cli.mjs block does: claim the
    // single IDLE_PROBE_CLAIM_TYPE slot.
    const grant = await claimCapacity(
      coordinationFilePath,
      {
        type: 'idle-probe',
        requestedCount: 1,
        orchestratorId,
        computeAvailableCapacity: () => 1,
        now,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );
    expect(grant.granted).toBe(1);

    const result = await applyIdleProbeOutcome(
      { outcome: 'degrading', plateauDeltaMb: null },
      {
        coordinationFilePath,
        historyFilePath,
        orchestratorId,
        probedAgentClass: 'typescript-implementer',
        now: now + 60_000,
      },
    );

    expect(result.released).toBe(1);
    expect(result.recorded).not.toBe(true);
    expect(result.aimdResult).toBeUndefined();

    // A fresh probe (same orchestratorId, later beat) can now claim the
    // slot again — proving the release genuinely freed it rather than
    // merely reporting a released count without persisting anything.
    const secondGrant = await claimCapacity(
      coordinationFilePath,
      {
        type: 'idle-probe',
        requestedCount: 1,
        orchestratorId,
        computeAvailableCapacity: () => 1,
        now: now + 120_000,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );
    expect(secondGrant.granted).toBe(1);
  });
});

describe('classifyIdleProbeTrajectory — flat trajectory', () => {
  it('classifies "flat" when the window completes with no leak-velocity trip and no measurable plateau delta', () => {
    const trajectory = [200, 202, 199, 201]; // essentially flat, well under the plateau threshold
    const result = classifyIdleProbeTrajectory(
      {
        leakVelocity: {
          trajectory,
          firstPolledAt: 0,
          lastPolledAt: 10 * 60_000,
          consecutiveAboveThreshold: 0,
        },
        windowElapsedMs: 10 * 60_000,
        windowBoundMs: 10 * 60_000,
        validPollCount: trajectory.length,
        minSamplesForConclusive: 3,
      },
      {
        leakVelocityThresholds: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
        plateauDeltaThresholdMb: PLATEAU_DELTA_THRESHOLD_MB,
      },
    );

    expect(result.outcome).toBe('flat');
    expect(result.abortedEarly).toBe(false);
    expect(result.leakVelocityResult.trip).toBe(false);
  });

  it(
    'applyIdleProbeOutcome("flat", ...) feeds computeAimdCeiling via the SAME function/state shape an organic ' +
      'sustained-normal beat uses — no parallel counter',
    async () => {
      const orchestratorId = 'idle-probe-flat';
      const now = 2_000_000;

      const grant = await claimCapacity(
        coordinationFilePath,
        {
          type: 'idle-probe',
          requestedCount: 1,
          orchestratorId,
          computeAvailableCapacity: () => 1,
          now,
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      );
      expect(grant.granted).toBe(1);

      // The exact same state a Normal-pressure organic beat would carry into
      // computeAimdCeiling — pressureLevel: 1 (Normal), a live ceiling +
      // sustainedNormalCount pair.
      const aimdState = { pressureLevel: 1, ceiling: 4, sustainedNormalCount: 4 };

      // (a) The ORGANIC path's own usual invocation — called directly here,
      // exactly as cli.mjs's normal per-beat AIMD update calls it.
      const organicResult = computeAimdCeiling(aimdState, AIMD_CONFIG);

      // (b) The PROBE's flat-outcome path, via applyIdleProbeOutcome, given
      // the SAME aimdState/aimdConfig.
      const probeResult = await applyIdleProbeOutcome(
        { outcome: 'flat', plateauDeltaMb: 2 },
        {
          coordinationFilePath,
          historyFilePath,
          orchestratorId,
          probedAgentClass: 'typescript-implementer',
          now: now + 60_000,
          aimdState,
          aimdConfig: AIMD_CONFIG,
        },
      );

      // THE assertion this test exists to pin: identical output from
      // identical input, proving the probe's flat outcome is genuinely
      // indistinguishable from an organic sustained-normal beat's
      // contribution to computeAimdCeiling — not a parallel counter that
      // merely resembles it.
      expect(probeResult.aimdResult).toEqual(organicResult);
      expect(probeResult.released).toBe(1);
      expect(probeResult.recorded).not.toBe(true);
    },
  );
});

describe('classifyIdleProbeTrajectory — new-plateau trajectory', () => {
  it('classifies "new-plateau" when the window completes with a measurable footprint delta and no leak-velocity trip', () => {
    const trajectory = [200, 210, 260, 280]; // grows then flattens — a real plateau shift, not a runaway
    const result = classifyIdleProbeTrajectory(
      {
        leakVelocity: {
          trajectory,
          firstPolledAt: 0,
          lastPolledAt: 10 * 60_000,
          consecutiveAboveThreshold: 0,
        },
        windowElapsedMs: 10 * 60_000,
        windowBoundMs: 10 * 60_000,
        validPollCount: trajectory.length,
        minSamplesForConclusive: 3,
      },
      {
        leakVelocityThresholds: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
        plateauDeltaThresholdMb: PLATEAU_DELTA_THRESHOLD_MB,
      },
    );

    expect(result.outcome).toBe('new-plateau');
    expect(result.abortedEarly).toBe(false);
    expect(result.leakVelocityResult.trip).toBe(false);
  });

  it(
    'applyIdleProbeOutcome("new-plateau", ...) records an observation via recordObservation, tagged distinctly, ' +
      'without contaminating the PROBED agent class\'s real history read by estimateOperationCost',
    async () => {
      const orchestratorId = 'idle-probe-plateau';
      const probedAgentClass = 'typescript-implementer';
      const now = 3_000_000;

      // Seed the PROBED agent class's real history first, exactly as
      // ordinary --record-outcome beats would over time — this is the
      // "ground truth" estimateOperationCost must keep reading unpoisoned.
      const { recordObservation } = await import('./history.mjs');
      await recordObservation(historyFilePath, {
        operationType: probedAgentClass,
        orchestratorId: 'real-beat-1',
        startedAt: now - 100_000,
        endedAt: now - 90_000,
        peakMemoryMb: 900,
        agentClass: probedAgentClass,
      });
      const beforeHistory = await readHistory(historyFilePath, probedAgentClass);

      const grant = await claimCapacity(
        coordinationFilePath,
        {
          type: 'idle-probe',
          requestedCount: 1,
          orchestratorId,
          computeAvailableCapacity: () => 1,
          now,
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      );
      expect(grant.granted).toBe(1);

      // pre-PR review fix (Medium — M3): `plateauDeltaMb` now comes
      // from the classification result — the SAME value
      // classifyIdleProbeTrajectory would have used to decide 'new-plateau'
      // over 'flat' — never independently recomputed from
      // context.finalFootprintMb/baselineFootprintMb.
      const result = await applyIdleProbeOutcome(
        { outcome: 'new-plateau', plateauDeltaMb: 80 },
        {
          coordinationFilePath,
          historyFilePath,
          orchestratorId,
          probedAgentClass,
          now: now + 60_000,
          finalFootprintMb: 280,
        },
      );

      expect(result.recorded).toBe(true);
      expect(result.released).toBe(1);

      // The PROBED agent class's own history is untouched — same raw array
      // as before the probe's observation was recorded.
      const afterHistory = await readHistory(historyFilePath, probedAgentClass);
      expect(afterHistory.raw).toEqual(beforeHistory.raw);
      expect(afterHistory.raw).toHaveLength(1);

      // The probe's own observation landed under a DISTINCT operationType —
      // never the probed class's own key — tagged agentClass: 'idle-probe',
      // and is independently retrievable.
      const idleProbeHistory = await readHistory(historyFilePath, 'idle-probe');
      expect(idleProbeHistory.raw).toHaveLength(1);
      expect(idleProbeHistory.raw[0].agentClass).toBe('idle-probe');
      expect(idleProbeHistory.raw[0].operationType).not.toBe(probedAgentClass);

      // Code-review fix, High 1: the footprint-polling-derived value MUST
      // land in `peakSmoothedFootprintMb` (tagged `peakMemorySource:
      // 'phys-footprint-ewma'`), never in `peakMemoryMb` — that field is
      // reserved exclusively for the legacy caller-supplied
      // `--peak-memory-mb` flag (see `history.mjs`'s own provenance
      // contract; this is the exact RSS-inversion metric-conflation
      // class).
      expect(idleProbeHistory.raw[0].peakSmoothedFootprintMb).toBe(280);
      expect(idleProbeHistory.raw[0].peakMemorySource).toBe('phys-footprint-ewma');
      expect(idleProbeHistory.raw[0].peakMemoryMb).toBeUndefined();

      // Code-review fix, Medium: the empirical footprint DELTA
      // (finalFootprintMb - baselineFootprintMb) is captured, not silently
      // discarded.
      expect(idleProbeHistory.raw[0].plateauDeltaMb).toBe(80);
    },
  );

  it(
    ' pre-PR review fix (Medium — M3): the RECORDED plateauDeltaMb is the exact value ' +
      'classifyIdleProbeTrajectory computed to decide the verdict — not an independently recomputed one that ' +
      'could disagree with or invert it',
    async () => {
      const orchestratorId = 'idle-probe-coupling';
      const probedAgentClass = 'typescript-implementer';
      const now = 6_000_000;

      // A real window: classify it for real, then feed the FULL result
      // straight into applyIdleProbeOutcome — end to end, no test-authored
      // shortcut plateauDeltaMb.
      const trajectory = [200, 210, 260, 280];
      const classification = classifyIdleProbeTrajectory(
        {
          leakVelocity: { trajectory, firstPolledAt: 0, lastPolledAt: 10 * 60_000, consecutiveAboveThreshold: 0 },
          windowElapsedMs: 10 * 60_000,
          windowBoundMs: 10 * 60_000,
          validPollCount: trajectory.length,
          minSamplesForConclusive: 3,
        },
        { leakVelocityThresholds: DEFAULT_LEAK_VELOCITY_THRESHOLDS, plateauDeltaThresholdMb: PLATEAU_DELTA_THRESHOLD_MB },
      );
      expect(classification.outcome).toBe('new-plateau');
      // The trajectory's own last-minus-first delta — this is what the
      // recorded observation must match, deriving from nowhere else.
      expect(classification.plateauDeltaMb).toBe(80);

      await claimCapacity(
        coordinationFilePath,
        { type: 'idle-probe', requestedCount: 1, orchestratorId, computeAvailableCapacity: () => 1, now },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      );

      // Deliberately mismatched finalFootprintMb — if applyIdleProbeOutcome
      // ever regresses to recomputing from context instead of reading
      // `classification.plateauDeltaMb`, this test would catch the drift:
      // the recorded delta would silently disagree with the value that
      // actually produced the 'new-plateau' verdict.
      await applyIdleProbeOutcome(classification, {
        coordinationFilePath,
        historyFilePath,
        orchestratorId,
        probedAgentClass,
        now: now + 60_000,
        finalFootprintMb: 9_999, // intentionally NOT trajectory[trajectory.length - 1]
      });

      const idleProbeHistory = await readHistory(historyFilePath, 'idle-probe');
      expect(idleProbeHistory.raw[0].plateauDeltaMb).toBe(classification.plateauDeltaMb);
      expect(idleProbeHistory.raw[0].plateauDeltaMb).toBe(80);
    },
  );
});

describe('classifyIdleProbeTrajectory — window-elapsed gate (code-review fix, High 2)', () => {
  it(
    'returns the non-terminal "observing" outcome — not a premature terminal verdict — when no leak-velocity trip ' +
      'has fired and windowElapsedMs has not yet reached windowBoundMs',
    () => {
      // A flat-looking trajectory with only one valid poll so far — if the
      // window-elapsed gate were missing, this shape (below
      // minSamplesForConclusive) would previously have resolved
      // 'inconclusive' mid-window, locking in a verdict on incomplete data.
      const trajectory = [200];
      const result = classifyIdleProbeTrajectory(
        {
          leakVelocity: {
            trajectory,
            firstPolledAt: 0,
            lastPolledAt: 60_000,
            consecutiveAboveThreshold: 0,
          },
          windowElapsedMs: 60_000, // well short of the window bound below
          windowBoundMs: 10 * 60_000,
          validPollCount: trajectory.length,
          minSamplesForConclusive: 3,
        },
        {
          leakVelocityThresholds: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
          plateauDeltaThresholdMb: PLATEAU_DELTA_THRESHOLD_MB,
        },
      );

      expect(result.outcome).toBe('observing');
      expect(result.outcome).not.toBe('inconclusive');
      expect(result.outcome).not.toBe('flat');
      expect(result.outcome).not.toBe('new-plateau');
      expect(result.abortedEarly).toBe(false);
    },
  );

  it(
    'returns "observing" mid-window even with plenty of valid polls and a flat trajectory — enough evidence does ' +
      'not shortcut the window bound',
    () => {
      const trajectory = [200, 201, 199, 200, 202];
      const result = classifyIdleProbeTrajectory(
        {
          leakVelocity: {
            trajectory,
            firstPolledAt: 0,
            lastPolledAt: 5 * 60_000,
            consecutiveAboveThreshold: 0,
          },
          windowElapsedMs: 5 * 60_000, // halfway through a 10 minute window
          windowBoundMs: 10 * 60_000,
          validPollCount: trajectory.length,
          minSamplesForConclusive: 3,
        },
        {
          leakVelocityThresholds: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
          plateauDeltaThresholdMb: PLATEAU_DELTA_THRESHOLD_MB,
        },
      );

      expect(result.outcome).toBe('observing');
      expect(result.abortedEarly).toBe(false);
    },
  );

  it('a leak-velocity trip still classifies "degrading" mid-window — the gate does not delay a genuine trip', () => {
    const trajectory = [100, 400, 700, 1000];
    const result = classifyIdleProbeTrajectory(
      {
        leakVelocity: {
          trajectory,
          firstPolledAt: 0,
          lastPolledAt: 60_000,
          consecutiveAboveThreshold: DEFAULT_LEAK_VELOCITY_THRESHOLDS.consecutivePollsRequired - 1,
        },
        windowElapsedMs: 60_000,
        windowBoundMs: 10 * 60_000,
        validPollCount: trajectory.length,
        minSamplesForConclusive: 3,
      },
      {
        leakVelocityThresholds: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
        plateauDeltaThresholdMb: PLATEAU_DELTA_THRESHOLD_MB,
      },
    );

    expect(result.outcome).toBe('degrading');
    expect(result.abortedEarly).toBe(true);
  });

  it('applyIdleProbeOutcome rejects "observing" — the window has not finished, so no claim release/consequence may fire', async () => {
    const orchestratorId = 'idle-probe-observing';
    const now = 5_000_000;

    await expect(
      applyIdleProbeOutcome(
        { outcome: 'observing', plateauDeltaMb: null },
        {
          coordinationFilePath,
          historyFilePath,
          orchestratorId,
          probedAgentClass: 'typescript-implementer',
          now,
        },
      ),
    ).rejects.toThrow(/observing/);
  });
});

describe('classifyIdleProbeTrajectory — window bound expiry with too few valid polls (host slept mid-probe)', () => {
  it('resolves to the defined "inconclusive" outcome, never hangs, and is distinct from "flat"', () => {
    const trajectory = [200]; // only one valid poll survived — below minSamplesForConclusive
    const result = classifyIdleProbeTrajectory(
      {
        leakVelocity: {
          trajectory,
          firstPolledAt: 0,
          lastPolledAt: 10 * 60_000, // the window bound elapsed regardless
          consecutiveAboveThreshold: 0,
        },
        windowElapsedMs: 10 * 60_000,
        windowBoundMs: 10 * 60_000,
        validPollCount: trajectory.length,
        minSamplesForConclusive: 3,
      },
      {
        leakVelocityThresholds: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
        plateauDeltaThresholdMb: PLATEAU_DELTA_THRESHOLD_MB,
      },
    );

    expect(result.outcome).toBe('inconclusive');
    expect(result.outcome).not.toBe('flat');
    expect(result.abortedEarly).toBe(false);
  });

  it('applyIdleProbeOutcome("inconclusive", ...) releases the claim and records nothing / feeds no AIMD signal', async () => {
    const orchestratorId = 'idle-probe-inconclusive';
    const now = 4_000_000;

    const grant = await claimCapacity(
      coordinationFilePath,
      {
        type: 'idle-probe',
        requestedCount: 1,
        orchestratorId,
        computeAvailableCapacity: () => 1,
        now,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );
    expect(grant.granted).toBe(1);

    const result = await applyIdleProbeOutcome(
      { outcome: 'inconclusive', plateauDeltaMb: null },
      {
        coordinationFilePath,
        historyFilePath,
        orchestratorId,
        probedAgentClass: 'typescript-implementer',
        now: now + 60_000,
      },
    );

    expect(result.released).toBe(1);
    expect(result.recorded).not.toBe(true);
    expect(result.aimdResult).toBeUndefined();
  });
});

describe('classifyIdleProbeTrajectory — window duration is configurable, not hardcoded to ~10 minutes', () => {
  it('a leak-velocity trip that would land WITHIN a shorter, caller-supplied windowBoundMs still aborts early', () => {
    const trajectory = [100, 400, 700, 1000];
    const shortWindowResult = classifyIdleProbeTrajectory(
      {
        leakVelocity: {
          trajectory,
          firstPolledAt: 0,
          lastPolledAt: 30_000,
          consecutiveAboveThreshold: DEFAULT_LEAK_VELOCITY_THRESHOLDS.consecutivePollsRequired - 1,
        },
        windowElapsedMs: 30_000,
        windowBoundMs: 2 * 60_000, // a deliberately non-default 2 minute window
        validPollCount: trajectory.length,
        minSamplesForConclusive: 3,
      },
      {
        leakVelocityThresholds: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
        plateauDeltaThresholdMb: PLATEAU_DELTA_THRESHOLD_MB,
      },
    );

    expect(shortWindowResult.outcome).toBe('degrading');
    expect(shortWindowResult.abortedEarly).toBe(true);
  });

  it('an identical trajectory below minSamplesForConclusive resolves "inconclusive" at a non-default windowBoundMs too', () => {
    const trajectory = [200];
    const result = classifyIdleProbeTrajectory(
      {
        leakVelocity: {
          trajectory,
          firstPolledAt: 0,
          lastPolledAt: 90_000,
          consecutiveAboveThreshold: 0,
        },
        windowElapsedMs: 90_000,
        windowBoundMs: 90_000, // a deliberately non-default 90 second window
        validPollCount: trajectory.length,
        minSamplesForConclusive: 3,
      },
      {
        leakVelocityThresholds: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
        plateauDeltaThresholdMb: PLATEAU_DELTA_THRESHOLD_MB,
      },
    );

    expect(result.outcome).toBe('inconclusive');
  });
});

describe('classifyIdleProbeTrajectory — leak-velocity consecutiveAboveThreshold does not leak across an aborted probe into a fresh retry', () => {
  it('a fresh retry probe starting with consecutiveAboveThreshold: 0 does NOT inherit a prior aborted probe\'s carried-over count', () => {
    // Simulates the SAME (constant) leak-velocity module directly, the way
    // classifyIdleProbeTrajectory is expected to internally — proving the
    // building block itself has no hidden module-level state a caller could
    // accidentally thread across two independent probe attempts.
    const abortedProbeState = {
      trajectory: [100, 250, 400],
      firstPolledAt: 0,
      lastPolledAt: 60_000,
      consecutiveAboveThreshold: DEFAULT_LEAK_VELOCITY_THRESHOLDS.consecutivePollsRequired - 1,
    };
    const abortedResult = computeLeakVelocity(abortedProbeState, DEFAULT_LEAK_VELOCITY_THRESHOLDS);
    expect(abortedResult.trip).toBe(true); // the aborted probe's own trip

    // A fresh, independently-attested retry probe's cold start MUST pass
    // consecutiveAboveThreshold: 0 into classifyIdleProbeTrajectory — never
    // `abortedResult.consecutiveAboveThreshold` (which, being post-trip, is
    // itself 0 by computeLeakVelocity's own reset-on-trip contract — the
    // risk this test guards is a caller instead threading the PRE-trip
    // in-progress counter, or any other stale carry-over, into the retry).
    const freshRetryTrajectory = [100, 102, 101]; // flat — genuinely different data
    const freshResult = classifyIdleProbeTrajectory(
      {
        leakVelocity: {
          trajectory: freshRetryTrajectory,
          firstPolledAt: 0,
          lastPolledAt: 10 * 60_000,
          consecutiveAboveThreshold: 0, // clean cold start, not carried over
        },
        windowElapsedMs: 10 * 60_000,
        windowBoundMs: 10 * 60_000,
        validPollCount: freshRetryTrajectory.length,
        minSamplesForConclusive: 3,
      },
      {
        leakVelocityThresholds: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
        plateauDeltaThresholdMb: PLATEAU_DELTA_THRESHOLD_MB,
      },
    );

    expect(freshResult.outcome).not.toBe('degrading');
    expect(freshResult.leakVelocityResult.consecutiveAboveThreshold).toBe(0);
  });
});
