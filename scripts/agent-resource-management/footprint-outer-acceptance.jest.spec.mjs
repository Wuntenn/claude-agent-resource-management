// Phase 0 outer acceptance test ("agent-resource-management:
// Phase 1 — per-agent phys_footprint polling + history collection").
//
// RED by construction, right now: `./lib/footprint-sampler.mjs` does not
// exist yet — the top-level `import` below fails module resolution, which
// is the expected red state for this whole file. It turns GREEN only once
// all of the Build Plan's Phases 1-4 land:
//   - Phase 1 ships `lib/footprint-sampler.mjs`'s `sampleFootprint(pid)`,
//     shelling out to `footprint`/`vmmap --summary` behind a fake-seam env
//     var (this file establishes that seam's name: `ARM_FAKE_FOOTPRINT_
//     OUTPUT`, mirroring `cli.mjs`'s existing `ARM_FAKE_PS_OUTPUT` idiom —
//     grep `ARM_FAKE_` in `cli.mjs` for the established convention).
//   - Phase 2 ships EWMA smoothing + a bounded trajectory accumulator that
//     Phase 3's `--poll-footprint` beat uses internally. This outer test
//     does NOT import Phase 2's module directly (its name/shape isn't fixed
//     yet) — it drives `sampleFootprint` across several polls and folds
//     them with a minimal local EWMA helper, matching the OBSERVABLE
//     contract the real accumulator must also satisfy: a peak-smoothed
//     value bounded within the observed sample range, and a
//     length-bounded trajectory array.
//   - Phase 3 extends `recordObservation`'s shape additively (new optional
//     `agentClass` / `footprintTrajectory` / real `startedAt` fields) —
//     `history.mjs` already spreads whatever observation shape it is given
//     (see `recordObservation`'s `{ ...observation }`), so this test can
//     already prove the END-TO-END contract (record → read back) against
//     the REAL, unmodified `history.mjs` without waiting for Phase 3's CLI
//     wiring.
//   - Phase 4 bounds the polling cadence — out of this outer test's scope
//     (covered by its own dedicated unit tests per the Build Plan).
//
// Per the Build Plan's outer-acceptance-test description, a companion
// before/after parity check drives `cli.mjs`'s `--desired-agents` admission
// path via `spawnSync` and asserts byte-identical decision output for a
// fixed input set — proving Phase 1-4's additive changes never regress
// admission. That is `describe('cli.mjs --desired-agents parity ...')`
// below, in the same file per the Build Plan's "same file or a sibling"
// allowance.
//
// Do not add a stub `footprint-sampler.mjs` to make this pass — that
// defeats the point of the outer/phase red-green loop. The builder
// implements `lib/footprint-sampler.mjs` (Phase 1) to turn the import error
// green, then Phases 2-4 turn the remaining assertions green.

import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

// RED: this module does not exist yet (Phase 1). Import failure here is the
// expected, documented red state for this whole file.
import { sampleFootprint } from './lib/footprint-sampler.mjs';

import { recordObservation, readHistory } from './lib/history.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

// ---------------------------------------------------------------------------
// Test 1 — sampleFootprint() + recordObservation()/readHistory() integration
// ---------------------------------------------------------------------------

describe('footprint polling — records agentClass, peak smoothed footprint, real duration, bounded trajectory', () => {
  let workDir;
  let historyFilePath;
  const originalFakeFootprintOutput = process.env.ARM_FAKE_FOOTPRINT_OUTPUT;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), 'arm-footprint-outer-'));
    historyFilePath = join(workDir, 'operation-history.json');
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
    if (originalFakeFootprintOutput === undefined) {
      delete process.env.ARM_FAKE_FOOTPRINT_OUTPUT;
    } else {
      process.env.ARM_FAKE_FOOTPRINT_OUTPUT = originalFakeFootprintOutput;
    }
  });

  /**
   * Minimal local EWMA fold — NOT the production Phase 2 accumulator (whose
   * name/shape is an implementation detail this outer test deliberately does
   * not pin). Only proves the OBSERVABLE contract Phase 2 must also satisfy:
   * a smoothed value seeded from the first sample, a running peak over the
   * smoothed series, and a trajectory capped at `maxTrajectoryLength`.
   */
  function foldFootprintSamples(samples, { alpha = 0.5, maxTrajectoryLength = 3 } = {}) {
    let smoothed = null;
    let peak = -Infinity;
    const trajectory = [];

    for (const sample of samples) {
      if (sample === null || sample === undefined) continue;
      smoothed = smoothed === null ? sample : alpha * sample + (1 - alpha) * smoothed;
      peak = Math.max(peak, smoothed);
      trajectory.push(smoothed);
      if (trajectory.length > maxTrajectoryLength) trajectory.shift();
    }

    return { peakSmoothedMb: peak, trajectory };
  }

  it(
    "records a completed run's agentClass label, peak smoothed phys_footprint, real (non-zero) " +
      'duration, and a bounded trajectory array via recordObservation, readable back via readHistory',
    async () => {
      const pid = 54321;
      const orchestratorId = 'orch-footprint-outer';
      const operationType = 'implementation-agent';
      const agentClass = 'typescript-implementer';

      // Five successive polls of a live PID, fed through the fake shell-out
      // seam Phase 1 must honour (ARM_FAKE_FOOTPRINT_OUTPUT). Each poll is a
      // separate sampleFootprint() call — mirroring how a real orchestrator
      // re-invokes the --poll-footprint CLI beat every 10-30s.
      const fakeOutputsMb = [180, 220, 260, 240, 300];
      const samples = [];
      const startedAt = Date.now();

      for (const mb of fakeOutputsMb) {
        // The exact text format Phase 1's parser expects is Phase 0's spike
        // output (real footprint/vmmap text) — this outer test only pins the
        // NUMERIC contract (`sampleFootprint` resolves to a finite MB
        // number), not the raw text shape, since that shape isn't captured
        // yet. A plausible `footprint`-style single-line summary is used as
        // the fake payload so Phase 1's real parser is free to consume it
        // once its exact grammar is fixed by Phase 0.
        process.env.ARM_FAKE_FOOTPRINT_OUTPUT = `phys_footprint: ${mb} MB`;
        // eslint-disable-next-line no-await-in-loop -- deliberately sequential: proving ordered polling, not concurrency.
        const sample = await sampleFootprint(pid);
        samples.push(sample);
      }
      const endedAt = startedAt + fakeOutputsMb.length * 15_000; // ~15s cadence, realistic 10-30s window

      expect(samples.every((sample) => typeof sample === 'number' && Number.isFinite(sample))).toBe(true);

      const { peakSmoothedMb, trajectory } = foldFootprintSamples(samples, {
        alpha: 0.5,
        maxTrajectoryLength: 3,
      });

      await recordObservation(
        historyFilePath,
        {
          operationType,
          orchestratorId,
          startedAt,
          endedAt,
          peakMemoryMb: peakSmoothedMb,
          agentClass,
          footprintTrajectory: trajectory,
          crashed: false,
        },
        { retentionCap: 5 },
      );

      const history = await readHistory(historyFilePath, operationType);

      expect(history.raw).toHaveLength(1);
      const recorded = history.raw[0];

      // agentClass label survives the round trip.
      expect(recorded.agentClass).toBe(agentClass);

      // Peak smoothed phys_footprint: derived from smoothing, not simply the
      // raw max — proven by being strictly less than the raw max (300) while
      // still within the observed sample range (a wrong implementation that
      // aliased "peak" to the raw max would slip past a looser assertion).
      expect(recorded.peakMemoryMb).toBeGreaterThan(Math.min(...fakeOutputsMb));
      expect(recorded.peakMemoryMb).toBeLessThanOrEqual(Math.max(...fakeOutputsMb));

      // Real, non-zero duration derived from actual poll span — not the
      // startedAt===endedAt===now() degenerate shape --record-outcome falls
      // back to when no polling ever happened for a run.
      const duration = recorded.endedAt - recorded.startedAt;
      expect(duration).toBeGreaterThan(0);
      expect(duration).toBe(endedAt - startedAt);

      // Bounded trajectory: never grows past the documented cap, regardless
      // of how many polls fed into it.
      expect(Array.isArray(recorded.footprintTrajectory)).toBe(true);
      expect(recorded.footprintTrajectory.length).toBeLessThanOrEqual(3);
      expect(recorded.footprintTrajectory.length).toBeGreaterThan(0);
    },
  );

  it('returns null, never throws, when the footprint shell-out seam yields no usable output (dead/unreadable PID)', async () => {
    process.env.ARM_FAKE_FOOTPRINT_OUTPUT = '';

    await expect(sampleFootprint(999_999)).resolves.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Test 1b (whole-branch review, Medium finding #5) — the GENUINE
// end-to-end proof this outer acceptance test was always supposed to be: it
// spawns the REAL --poll-footprint CLI beat (not sampleFootprint/
// recordObservation called directly), the real --record-outcome beat for the
// same pid, then reads back via readHistory. This is exactly the path that
// would have caught the agentClass gap (whole-branch review, High
// finding: parsed by --poll-footprint, echoed into its own stdout, but never
// actually persisted into lib/footprint-state.mjs's per-pid entry) before it
// shipped — the direct-call version above never exercises cli.mjs, footprint-
// state.mjs, or footprint-trajectory.mjs at all.
// ---------------------------------------------------------------------------

describe('cli.mjs --poll-footprint -> --record-outcome end-to-end (whole-branch review, genuine outer acceptance)', () => {
  let workDir;
  let historyFilePath;
  let footprintStateFilePath;
  let coordinationFilePath;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), 'arm-footprint-e2e-'));
    historyFilePath = join(workDir, 'history.json');
    footprintStateFilePath = join(workDir, 'footprint-state.json');
    coordinationFilePath = join(workDir, 'coordination.json');
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  it(
    'a realistic multi-poll --poll-footprint series, followed by --record-outcome, produces a real ' +
      'duration, a smoothed peak field, a bounded trajectory, and a surviving agentClass label — ' +
      'all through the REAL CLI, never a direct function call',
    () => {
      const PID = 45678;
      const ORCHESTRATOR_ID = 'orch-e2e-genuine';
      const OPERATION_TYPE = 'implementation-agent';
      const AGENT_CLASS = 'typescript-implementer';
      const T0 = 1_701_000_000_000;
      const POLL_INTERVAL_MS = 20_000;
      const fakeOutputsMb = [180, 220, 260, 240, 300, 280];

      const baseEnv = {
        ...process.env,
        ARM_HISTORY_FILE: historyFilePath,
        ARM_FOOTPRINT_STATE_FILE: footprintStateFilePath,
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_BEAT_NOW_TEST_MODE: '1',
      };

      fakeOutputsMb.forEach((mb, index) => {
        const poll = spawnSync(
          'node',
          [
            CLI,
            `--poll-footprint=${PID}`,
            `--orchestrator-id=${ORCHESTRATOR_ID}`,
            `--operation-type=${OPERATION_TYPE}`,
            `--agent-class=${AGENT_CLASS}`,
          ],
          {
            encoding: 'utf8',
            env: {
              ...baseEnv,
              ARM_FAKE_NOW_MS: String(T0 + index * POLL_INTERVAL_MS),
              ARM_FAKE_FOOTPRINT_OUTPUT: `phys_footprint: ${mb} MB`,
            },
          },
        );
        expect(poll.status).toBe(0);
        const parsedPoll = JSON.parse(poll.stdout);
        expect(parsedPoll).toMatchObject({
          polled: true,
          pid: PID,
          orchestratorId: ORCHESTRATOR_ID,
          agentClass: AGENT_CLASS,
        });
        expect(typeof parsedPoll.sampleMb).toBe('number');
      });

      const recordAt = T0 + fakeOutputsMb.length * POLL_INTERVAL_MS;
      const record = spawnSync(
        'node',
        [
          CLI,
          '--record-outcome',
          `--pid=${PID}`,
          `--operation-type=${OPERATION_TYPE}`,
          `--orchestrator-id=${ORCHESTRATOR_ID}`,
        ],
        { encoding: 'utf8', env: { ...baseEnv, ARM_FAKE_NOW_MS: String(recordAt) } },
      );
      expect(record.status).toBe(0);

      return readHistory(historyFilePath, OPERATION_TYPE).then((history) => {
        expect(history.raw).toHaveLength(1);
        const recorded = history.raw[0];

        // Real, non-degenerate duration derived from the actual poll span.
        expect(recorded.startedAt).toBe(T0);
        expect(recorded.endedAt).toBe(recordAt);
        expect(recorded.endedAt - recorded.startedAt).toBeGreaterThan(0);

        // A smoothed peak field is present (finding #3's new, distinct
        // field) — never aliased onto peakMemoryMb, which stays absent since
        // no --peak-memory-mb flag was ever passed.
        expect(recorded.peakMemoryMb).toBeUndefined();
        expect(typeof recorded.peakSmoothedFootprintMb).toBe('number');
        expect(Number.isFinite(recorded.peakSmoothedFootprintMb)).toBe(true);
        expect(recorded.peakMemorySource).toBe('phys-footprint-ewma');

        // Bounded trajectory, non-empty.
        expect(Array.isArray(recorded.footprintTrajectory)).toBe(true);
        expect(recorded.footprintTrajectory.length).toBeGreaterThan(0);
        expect(recorded.footprintTrajectory.length).toBeLessThanOrEqual(fakeOutputsMb.length);

        // The genuine proof this test exists for: agentClass survives
        // end-to-end through the REAL CLI, not a direct recordObservation
        // call.
        expect(recorded.agentClass).toBe(AGENT_CLASS);
      });
    },
  );
});

// ---------------------------------------------------------------------------
// Test 2 — cli.mjs --desired-agents admission parity (no-regression golden)
// ---------------------------------------------------------------------------
//
// Golden fixture captured from `origin/master`'s cli.mjs behaviour BEFORE
// any of its phases land (this branch's cli.mjs was byte-identical to
// origin/master's at capture time — confirmed via `git diff origin/master --
// scripts/agent-resource-management/cli.mjs`, which was empty). Captured
// with a fixed, injected GREEN sample and a fixed, injected load-average
// reading (ARM_FAKE_LOAD_AVERAGE_JSON) so the fixture is fully deterministic
// and host-independent — the real os.loadavg() reading is NOT reproducible
// across machines/CI, so leaving it un-faked would make this golden fixture
// flaky by construction.
//
// Each fixture entry was produced by running, for the given --desired-agents
// value, against a fresh coordination file:
//
//   ARM_COORDINATION_FILE=<fresh temp path> \
//   ARM_FAKE_LOAD_AVERAGE_JSON='[1.5,1.2,1.0]' \
//   ARM_FAKE_COLLECT_JSON='{"memory":{"pressureLevel":1,"swapUsedMb":1024,"compressedMb":512},"disk":{"freeDiskGb":60,"declineRateGbPerHour":0.5}}' \
//   node cli.mjs --orchestrator-id=orch-golden --desired-agents=<n>
// ---------------------------------------------------------------------------

const PARITY_MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 };
const PARITY_DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const PARITY_LOAD_AVERAGE = [1.5, 1.2, 1.0];

// Golden decisions, keyed by --desired-agents value, captured against
// origin/master's cli.mjs (see header comment for exact capture command).
//
// Phase 2 — `compressedMb` added additively to every entry below.
// This golden fixture was captured before landed, so on its own it
// would assert byte-identical output forever, including against later,
// separate, intentional schema changes it was never meant to pin.
// deliberately adds a new record-only sibling field (`compressedMb`,
// sourced straight from the same fixed `ARM_FAKE_COLLECT_JSON` sample's
// `memory.compressedMb: 512` used to capture this fixture — see the capture
// command above) to the printed traffic-light JSON, mirroring how
// `loadAverage` was added the same way in an earlier phase. This fixture's
// job — proving the ADMISSION DECISION itself (`type`/`allowance`/
// `liveAgentGrant`) never regresses — is unaffected: only the additive
// `compressedMb` sibling changed.
//
// Phase 3 — `leakBlocks: []` added additively to every entry below the
// same way: no `--poll-footprint` entries exist for this fixture's fresh
// `ARM_FOOTPRINT_STATE_FILE`, so the new per-pid leak-velocity detection echo
// is always the empty array here. Same "additive sibling field, admission
// decision itself unaffected" reasoning as `compressedMb` above.
//
// Phase 3 adds another record-only sibling the same way:
// `memory.compressionVelocity`, sourced from the same fixed
// `ARM_FAKE_COLLECT_JSON` sample (which supplies no `compressions`/
// `decompressions`, hence the deterministic cold-start-with-no-counters
// shape below: `coldStart: true`, `compressionsPerSec: 0`,
// `compressionRatio: null` — 0/0 is NaN, which JSON-serialises as `null`).
const GOLDEN_DECISIONS_BY_DESIRED_AGENTS = {
  0: {
    type: 'spawn-allowed',
    allowance: 0,
    diskTrend: { trend: 'insufficient-data', declineRateGbPerHour: 0 },
    liveAgentGrant: null,
    loadAverage: { oneMinute: 1.5, fiveMinute: 1.2, fifteenMinute: 1 },
    compressedMb: 512,
    leakBlocks: [],
    memory: {
      compressionVelocity: { coldStart: true, compressionsPerSec: 0, compressionRatio: null },
    },
  },
  1: {
    type: 'spawn-allowed',
    allowance: 4,
    diskTrend: { trend: 'insufficient-data', declineRateGbPerHour: 0 },
    liveAgentGrant: 1,
    loadAverage: { oneMinute: 1.5, fiveMinute: 1.2, fifteenMinute: 1 },
    compressedMb: 512,
    leakBlocks: [],
    memory: {
      compressionVelocity: { coldStart: true, compressionsPerSec: 0, compressionRatio: null },
    },
  },
  2: {
    type: 'spawn-allowed',
    allowance: 4,
    diskTrend: { trend: 'insufficient-data', declineRateGbPerHour: 0 },
    liveAgentGrant: 2,
    loadAverage: { oneMinute: 1.5, fiveMinute: 1.2, fifteenMinute: 1 },
    compressedMb: 512,
    leakBlocks: [],
    memory: {
      compressionVelocity: { coldStart: true, compressionsPerSec: 0, compressionRatio: null },
    },
  },
  3: {
    type: 'spawn-allowed',
    allowance: 4,
    diskTrend: { trend: 'insufficient-data', declineRateGbPerHour: 0 },
    liveAgentGrant: 3,
    loadAverage: { oneMinute: 1.5, fiveMinute: 1.2, fifteenMinute: 1 },
    compressedMb: 512,
    leakBlocks: [],
    memory: {
      compressionVelocity: { coldStart: true, compressionsPerSec: 0, compressionRatio: null },
    },
  },
  5: {
    type: 'spawn-allowed',
    allowance: 4,
    diskTrend: { trend: 'insufficient-data', declineRateGbPerHour: 0 },
    liveAgentGrant: 4,
    loadAverage: { oneMinute: 1.5, fiveMinute: 1.2, fifteenMinute: 1 },
    compressedMb: 512,
    leakBlocks: [],
    memory: {
      compressionVelocity: { coldStart: true, compressionsPerSec: 0, compressionRatio: null },
    },
  },
  50: {
    type: 'spawn-allowed',
    allowance: 4,
    diskTrend: { trend: 'insufficient-data', declineRateGbPerHour: 0 },
    liveAgentGrant: 4,
    loadAverage: { oneMinute: 1.5, fiveMinute: 1.2, fifteenMinute: 1 },
    compressedMb: 512,
    leakBlocks: [],
    memory: {
      compressionVelocity: { coldStart: true, compressionsPerSec: 0, compressionRatio: null },
    },
  },
};

describe('cli.mjs --desired-agents admission parity (no-regression golden, outer acceptance)', () => {
  let workDir;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), 'arm-footprint-parity-'));
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  test.each(Object.keys(GOLDEN_DECISIONS_BY_DESIRED_AGENTS))(
    'produces the byte-identical admission decision as origin/master for --desired-agents=%s',
    (desiredAgents) => {
      const coordinationFilePath = join(workDir, `coordination-${desiredAgents}.json`);

      const result = spawnSync(
        'node',
        [CLI, '--orchestrator-id=orch-golden', `--desired-agents=${desiredAgents}`],
        {
          encoding: 'utf8',
          env: {
            ...process.env,
            ARM_COORDINATION_FILE: coordinationFilePath,
            ARM_FAKE_LOAD_AVERAGE_JSON: JSON.stringify(PARITY_LOAD_AVERAGE),
            ARM_FAKE_COLLECT_JSON: JSON.stringify({
              memory: PARITY_MEMORY_GREEN,
              disk: PARITY_DISK_GREEN,
            }),
          },
        },
      );

      expect(result.status).toBe(0);
      const decision = JSON.parse(result.stdout);

      expect(decision).toEqual(GOLDEN_DECISIONS_BY_DESIRED_AGENTS[desiredAgents]);
    },
  );
});
