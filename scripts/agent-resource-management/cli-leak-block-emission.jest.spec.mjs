// Phase 3 — additional integration coverage for cli.mjs's
// `leakBlocks` emission, beyond the Phase 0 outer-acceptance test in
// ./cli-leak-block.jest.spec.mjs (which pins the base contract: a single
// leaking pid is echoed on both a --desired-agents and a --heartbeat beat,
// additive alongside the beat's own admission `type`, without altering that
// admission decision, and never flags a merely-normal/interrupted-growth
// pid).
//
// This file covers the remaining Test Strategy scenarios from the Build
// Plan that the Phase 0 file does not already exercise:
//
//   1. Multiple pids tripping leak-velocity in the same beat — all must be
//      emitted, none silently dropped.
//   2. A beat where 'pressure-block' ALSO fires (host-wide pressure signal)
//      at the same time as a per-pid leak — both must be observable;
//      'pressure-block''s own early-return behaviour is unchanged by this
//      phase.
//   3. No currently-tracked pid trips leak-velocity — beat proceeds exactly
//      as it does today; this file asserts `leakBlocks` is either absent or
//      an empty array in that case (see DESIGN CHOICE note below).
//
// DESIGN CHOICE (test 3): the Phase 0 file's own header states "a pid with
// no leak-velocity concern at all yields `leakBlocks: []`" as its design
// guess, but does not itself exercise the "ZERO tracked pids trip" case end
// to end (its NORMAL_PID assertion only proves that ONE non-tripping pid
// among tracked pids doesn't appear in a non-empty array). This file's test 3
// asserts the Phase 0 header's own stated design guess — `leakBlocks: []`,
// present but empty — as the PRIMARY assertion, since that is the documented
// contract and keeps this test meaningfully RED today (an "absent field" OR
// clause would trivially pass right now, before any implementation exists,
// which defeats the point of a red-first test). A secondary, looser
// `leakBlocksAbsentOrEmpty` check is also asserted below for visibility, in
// case the implementer deliberately chooses "absent" instead of `[]` — if
// so, relax the primary assertion to match rather than treat it as a
// regression.
//
// RED by construction, right now, for the same reason as every assertion in
// ./cli-leak-block.jest.spec.mjs: `cli.mjs` has no concept of "leak
// velocity"/`leak-block` anywhere today, so `body.leakBlocks` is simply
// `undefined` on every beat, and `pressure-block`'s own JSON body has no
// `leakBlocks` field bolted on either.
//
// Coverage/mocking posture: black-box via `spawnSync` of the real `cli.mjs`,
// mirroring every sibling `cli.*.jest.spec.mjs` in this skill — `cli.mjs`
// itself is coverage-exempt and only ever black-box tested via subprocess
// (see e.g. ./cli-evict.jest.spec.mjs's / ./cli-memory-projection-wiring.jest.spec.mjs's
// own "never imports cli.mjs directly" headers, and ./cli-leak-block.jest.spec.mjs's
// identical convention note). `recordFootprintPoll` is imported directly from
// `lib/footprint-state.mjs` only to SEED persisted trajectory state one layer
// below the spawned beat — the same lib-level seeding precedent
// ./cli-leak-block.jest.spec.mjs and ./cli-evict.jest.spec.mjs already use.

import { spawnSync } from 'node:child_process';
import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { TRAJECTORY_CAP } from './lib/footprint-trajectory.mjs';

import { recordFootprintPoll } from './lib/footprint-state.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
// Mirrors ./cli-memory-projection-wiring.jest.spec.mjs's own MEMORY_WARN —
// pressureLevel=2 is WARN-or-above, which trips cli.mjs's pre-admission
// 'pressure-block' short-circuit (see cli.mjs's `DEFAULT_PRESSURE_OVERRIDE_THRESHOLDS.warnAtOrAbove`
// check, grepped in the Build Plan handoff).
const MEMORY_WARN = { pressureLevel: 2, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

// Sustained, uninterrupted growth across every consecutive poll — mirrors
// ./cli-leak-block.jest.spec.mjs's own LEAK_SAMPLES_MB and
// ./leak-velocity-outer-acceptance.jest.spec.mjs's LEAK_TRAJECTORY.
const LEAK_SAMPLES_MB = [200, 260, 330, 410, 500];
// A second, independent leak trajectory — different absolute values, same
// sustained-growth shape — used to prove a SECOND tripping pid is not merely
// a coincidental echo of the first.
const LEAK_SAMPLES_MB_2 = [300, 380, 470, 570, 680];

/** Runs the real cli.mjs as a real child process — never imports it directly (matches every sibling cli.*.jest.spec.mjs). */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

let workDir;
let coordinationFilePath;
let footprintStateFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-leak-block-emission-'));
  coordinationFilePath = join(workDir, 'coordination.json');
  footprintStateFilePath = join(workDir, 'footprint-state.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

function baseEnv(extra = {}) {
  return {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FOOTPRINT_STATE_FILE: footprintStateFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    ...extra,
  };
}

/** Seeds `pid`'s persisted footprint trajectory directly via the real, already-shipped lib primitive — one poll per sample, `now` advancing 10s per poll (a plausible real --poll-footprint cadence). */
async function seedFootprintTrajectory(pid, samplesMb) {
  const baseNow = Date.now() - samplesMb.length * 10_000;
  for (const [index, sampleMb] of samplesMb.entries()) {
    // eslint-disable-next-line no-await-in-loop -- must fold in order; each poll depends on the prior poll's persisted state.
    await recordFootprintPoll(footprintStateFilePath, pid, sampleMb, baseNow + index * 10_000, {
      agentClass: 'typescript-implementer',
    });
  }
}

it(
  'emits a leakBlocks entry for EVERY currently-tracked pid whose trajectory trips leak-velocity in the same beat ' +
    '— none silently dropped when more than one pid is leaking at once',
  async () => {
    const LEAK_PID_A = 9301;
    const LEAK_PID_B = 9302;

    await seedFootprintTrajectory(LEAK_PID_A, LEAK_SAMPLES_MB);
    await seedFootprintTrajectory(LEAK_PID_B, LEAK_SAMPLES_MB_2);

    const { status, stderr, stdout } = runCli(
      ['--orchestrator-id=orch-leak-block-multi', '--desired-agents=1'],
      baseEnv(),
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const body = JSON.parse(stdout);

    expect(body.type).toBe('spawn-allowed');

    // FAILS TODAY: `leakBlocks` is undefined on every beat's JSON body.
    expect(Array.isArray(body.leakBlocks)).toBe(true);

    const pidsReported = body.leakBlocks.map((entry) => entry.pid).sort((a, b) => a - b);
    expect(pidsReported).toEqual([LEAK_PID_A, LEAK_PID_B].sort((a, b) => a - b));

    for (const entry of body.leakBlocks) {
      expect(entry.type).toBe('leak-block');
      expect(typeof entry.reason).toBe('string');
      expect(entry.reason.length).toBeGreaterThan(0);
    }
  },
);

it(
  "still reports a leaking pid's leak-block entry inside the SAME beat where WARN-level host pressure independently " +
    "triggers cli.mjs's existing, unmodified 'pressure-block' early return — both signals must be observable, and " +
    "pressure-block's own short-circuit shape (liveAgentGrant: null, no admission fields) is unchanged by this phase",
  async () => {
    const LEAK_PID = 9303;
    await seedFootprintTrajectory(LEAK_PID, LEAK_SAMPLES_MB);

    const { status, stderr, stdout } = runCli(
      ['--orchestrator-id=orch-leak-block-pressure', '--desired-agents=1'],
      baseEnv({ ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_WARN, disk: DISK_GREEN }) }),
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const body = JSON.parse(stdout);

    // pressure-block's own, pre-existing early-return behaviour is
    // unchanged: the beat still halts before ever reaching an admission
    // verdict, and liveAgentGrant is still null exactly as it is today
    // (./cli-memory-projection-wiring.jest.spec.mjs test 3's own assertion).
    expect(body.type).toBe('pressure-block');
    expect(body.liveAgentGrant).toBeNull();

    // FAILS TODAY: leakBlocks is undefined even once pressure-block's own
    // fields exist, because no code anywhere populates it — on ANY beat
    // shape, blocked or not.
    expect(Array.isArray(body.leakBlocks)).toBe(true);
    const leakEntry = body.leakBlocks.find((entry) => entry.pid === LEAK_PID);
    expect(leakEntry).toBeDefined();
    expect(leakEntry.type).toBe('leak-block');
  },
);

it(
  'proceeds exactly as an ordinary beat does today, with leakBlocks either absent or an empty array, when no ' +
    'currently-tracked pid trips leak-velocity',
  async () => {
    const NORMAL_PID = 9304;
    // Genuine growth, but interrupted (a shrink between polls) — mirrors
    // ./cli-leak-block.jest.spec.mjs's own NORMAL_SAMPLES_MB; must never trip.
    await seedFootprintTrajectory(NORMAL_PID, [200, 260, 210, 400, 220]);

    const { status, stderr, stdout } = runCli(
      ['--orchestrator-id=orch-leak-block-none-trip', '--desired-agents=1'],
      baseEnv(),
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const body = JSON.parse(stdout);

    expect(body.type).toBe('spawn-allowed');
    expect(Number(body.liveAgentGrant)).toBeGreaterThan(0);

    // PRIMARY assertion — matches the Phase 0 file's own stated design guess
    // ("a pid with no leak-velocity concern at all yields `leakBlocks: []`").
    // FAILS TODAY: `leakBlocks` is `undefined`, not an array, on every beat.
    expect(Array.isArray(body.leakBlocks)).toBe(true);
    expect(body.leakBlocks).toEqual([]);

    // Looser, secondary check kept for documentation purposes (see file
    // header DESIGN CHOICE note): also holds if the implementer instead
    // chooses to omit the field entirely rather than emit `[]`.
    const leakBlocksAbsentOrEmpty =
      !('leakBlocks' in body) || (Array.isArray(body.leakBlocks) && body.leakBlocks.length === 0);
    expect(leakBlocksAbsentOrEmpty).toBe(true);

    if (Array.isArray(body.leakBlocks)) {
      const normalEntry = body.leakBlocks.find((entry) => entry.pid === NORMAL_PID);
      expect(normalEntry).toBeUndefined();
    }
  },
);

// Phase 3 review, Critical regression — a pid whose trajectory has
// truncated past `TRAJECTORY_CAP` (120, see `footprint-trajectory.mjs`) must
// still trip when the SURVIVING window shows genuinely sustained,
// above-threshold growth. `computeLeakBlocksForBeat`'s pre-fix formula
// derived its reconstructed per-pair interval from
// `(lastPolledAt - firstPolledAt) / (trajectory.length - 1)` — once a pid
// has been polled more times than `TRAJECTORY_CAP`, `firstPolledAt` (set
// once at cold start, never adjusted by `footprint-state.mjs`) predates the
// oldest SURVIVING sample, so that division silently smuggles the
// truncated-away span into the denominator's numerator, inflating the
// reconstructed interval and diluting the computed growth rate below
// threshold. This test seeds a full, `TRAJECTORY_CAP`-sized trajectory at a
// realistic 10s real cadence, then patches `firstPolledAt` backward by an
// extra ~4800s — simulating a pid that has ACTUALLY been polled roughly 600
// times total (of which only the newest 120 survive) — and asserts the beat
// still reports a leak-block, proving the fix's `windowStartAt`/`intervalMs`
// reconstruction is no longer silently suppressed by the stale
// `firstPolledAt`.
it(
  'still trips leak-velocity for a pid whose trajectory has truncated past TRAJECTORY_CAP, even though its ' +
    'persisted firstPolledAt is stale by hours relative to the surviving window',
  async () => {
    const TRUNCATED_PID = 9306;
    const REAL_POLL_INTERVAL_MS = 10_000; // matches seedFootprintTrajectory's own real-cadence convention
    // Sustained +5MB every real 10s poll across the WHOLE surviving window
    // (rate = 5MB / (10s/60) = 30 MB/min with the true 10s cadence, comfortably
    // above DEFAULT_LEAK_VELOCITY_THRESHOLDS.thresholdMbPerMin=10 and sustained
    // far beyond consecutivePollsRequired=4).
    const truncatedTrajectorySamples = Array.from({ length: TRAJECTORY_CAP }, (_, index) => 200 + index * 5);

    await seedFootprintTrajectory(TRUNCATED_PID, truncatedTrajectorySamples);

    // Simulate truncation: this pid's trajectory is already exactly
    // TRAJECTORY_CAP long (the max `seedFootprintTrajectory` above would
    // have produced without eviction), so patch its persisted
    // `firstPolledAt` backward to simulate ~480 EARLIER real polls that were
    // evicted by `footprint-trajectory.mjs`'s cap before this beat ever
    // runs — exactly what a genuinely long-running leaker's persisted state
    // looks like. Direct low-level patch of the state file (rather than
    // seeding 600 real sequential polls) purely for test speed; the
    // resulting shape — a full TRAJECTORY_CAP trajectory with a firstPolledAt
    // that predates the oldest surviving sample — is indistinguishable from
    // what `footprint-state.mjs`/`footprint-trajectory.mjs` would have
    // produced organically.
    const store = JSON.parse(await readFile(footprintStateFilePath, 'utf8'));
    const entry = store[String(TRUNCATED_PID)];
    const extraTruncatedAwayPolls = 480;
    entry.firstPolledAt -= extraTruncatedAwayPolls * REAL_POLL_INTERVAL_MS;
    await writeFile(footprintStateFilePath, JSON.stringify(store), 'utf8');

    const { status, stderr, stdout } = runCli(
      ['--orchestrator-id=orch-leak-block-truncated', '--desired-agents=1'],
      baseEnv(),
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const body = JSON.parse(stdout);

    expect(Array.isArray(body.leakBlocks)).toBe(true);
    const leakEntry = body.leakBlocks.find((entry_) => entry_.pid === TRUNCATED_PID);
    // Pre-fix, the stale firstPolledAt inflates the reconstructed interval
    // roughly 5x beyond the real 10s cadence, diluting the computed rate to
    // ~6 MB/min — below threshold — so this entry would be silently absent
    // without the fix.
    expect(leakEntry).toBeDefined();
    expect(leakEntry.type).toBe('leak-block');
  },
);

// Phase 3 review, Medium fix regression — the false-positive counterpart to
// the truncation regression test above. A pid can sit at EXACTLY
// `TRAJECTORY_CAP` samples without ever having been truncated (it has simply
// never been polled more than `TRAJECTORY_CAP` times), in which case its
// persisted `firstPolledAt` is still entirely accurate. The pre-fix code
// keyed its fallback purely on `trajectory.length >= TRAJECTORY_CAP`, so it
// wrongly discarded this pid's accurate `firstPolledAt`-derived interval and
// substituted the assumed `DEFAULT_FOOTPRINT_POLL_INTERVAL_MS` (20s) cadence
// instead — even though this pid's real, sustained cadence is a slower
// (but still entirely plausible, within `MIN_FOOTPRINT_POLL_INTERVAL_MS` /
// `MAX_FOOTPRINT_POLL_INTERVAL_MS`) 30s. Recomputed against the WRONG,
// faster assumed 20s cadence, the same real growth reads as a higher MB/min
// rate than it actually is — enough to false-trip a pid whose real,
// accurate-cadence growth rate never crosses the threshold at all. This test
// seeds exactly `TRAJECTORY_CAP` samples at a genuine, uninterrupted 30s
// cadence with a real growth rate that sits BELOW threshold when correctly
// computed against the accurate cadence, but ABOVE threshold if the naive
// interval were wrongly discarded in favour of the faster 20s default —
// asserting no leak-block is ever emitted for this pid.
it(
  'does not false-trip a pid with EXACTLY TRAJECTORY_CAP samples and an accurate, never-truncated firstPolledAt, ' +
    'even though its real, plausible 30s cadence is slower than the assumed default 20s cadence the pre-fix ' +
    'length-keyed fallback would have wrongly substituted',
  async () => {
    const NEVER_TRUNCATED_PID = 9307;
    const REAL_POLL_INTERVAL_MS = 30_000; // plausible real cadence (within the documented 10-30s bound)
    const GROWTH_PER_POLL_MB = 4;
    // Real growth rate at the ACCURATE 30s cadence: 4MB / (30s/60) = 8 MB/min
    // — below DEFAULT_LEAK_VELOCITY_THRESHOLDS.thresholdMbPerMin=10, so this
    // pid must never trip. Recomputed (wrongly) at the assumed 20s default
    // cadence: 4MB / (20s/60) = 12 MB/min — above threshold, which is
    // exactly the false trip the pre-fix length-keyed fallback would have
    // produced for this pid.
    const neverTruncatedSamples = Array.from(
      { length: TRAJECTORY_CAP },
      (_, index) => 200 + index * GROWTH_PER_POLL_MB,
    );

    const baseNow = Date.now() - neverTruncatedSamples.length * REAL_POLL_INTERVAL_MS;
    for (const [index, sampleMb] of neverTruncatedSamples.entries()) {
      // eslint-disable-next-line no-await-in-loop -- must fold in order; each poll depends on the prior poll's persisted state.
      await recordFootprintPoll(
        footprintStateFilePath,
        NEVER_TRUNCATED_PID,
        sampleMb,
        baseNow + index * REAL_POLL_INTERVAL_MS,
        { agentClass: 'typescript-implementer' },
      );
    }

    const { status, stderr, stdout } = runCli(
      ['--orchestrator-id=orch-leak-block-never-truncated', '--desired-agents=1'],
      baseEnv(),
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const body = JSON.parse(stdout);

    expect(body.type).toBe('spawn-allowed');
    expect(Array.isArray(body.leakBlocks)).toBe(true);
    const leakEntry = body.leakBlocks.find((entry) => entry.pid === NEVER_TRUNCATED_PID);
    // Pre-fix, the length-keyed fallback wrongly substitutes the faster 20s
    // assumed cadence for this pid's accurate 30s real cadence, inflating
    // the recomputed rate from 8 to 12 MB/min and false-tripping. The fix
    // must leave this pid's accurate firstPolledAt-derived interval alone.
    expect(leakEntry).toBeUndefined();
  },
);

// pre-PR review, Medium — the symmetric, lower-bound counterpart to the
// truncation regression above. `reconstructLeakVelocityTiming` only checked
// the naive `(lastPolledAt - firstPolledAt) / (trajectory.length - 1)`
// interval against an UPPER implausibility bound (catching TRAJECTORY_CAP
// truncation); an anomalously SMALL naive interval (clock skew, or a
// `firstPolledAt` recorded closer to `lastPolledAt` than any real poll
// cadence could produce) was trusted as-is, understating `intervalMs` and
// inflating the derived MB/min rate enough to false-trip a pid whose real,
// accurate-cadence growth never crosses the threshold. This test seeds a
// trajectory whose real per-pair growth is comfortably below threshold at
// ANY plausible cadence, then patches `firstPolledAt` forward (toward
// `lastPolledAt`) to compress the naive reconstructed interval to ~1s ---
// far below `MIN_FOOTPRINT_POLL_INTERVAL_MS` --- and asserts no leak-block
// is emitted, proving the naive interval was distrusted and the fallback
// cadence used instead, exactly as the upper-bound fallback already does for
// truncation.
it(
  'does not false-trip a pid whose naive reconstructed interval is anomalously SMALL (clock skew), even though ' +
    'the same growth recomputed at that tiny interval would read far above threshold',
  async () => {
    const SKEWED_PID = 9308;
    const GROWTH_PER_POLL_MB = 2;
    // Real growth rate at ANY plausible (10-30s) cadence is well below
    // DEFAULT_LEAK_VELOCITY_THRESHOLDS.thresholdMbPerMin=10 — e.g. at the
    // assumed 20s default fallback cadence: 2MB / (20s/60) = 6 MB/min.
    // Recomputed (wrongly) at a compressed ~1s naive interval: 2MB / (1s/60)
    // = 120 MB/min — far above threshold, which is exactly the false trip a
    // missing lower-bound check would produce for this pid.
    const skewedSamples = [200, 202, 204, 206, 208]; // 4 pairs, +2MB each

    const baseNow = Date.now() - skewedSamples.length * 10_000;
    for (const [index, sampleMb] of skewedSamples.entries()) {
      // eslint-disable-next-line no-await-in-loop -- must fold in order; each poll depends on the prior poll's persisted state.
      await recordFootprintPoll(footprintStateFilePath, SKEWED_PID, sampleMb, baseNow + index * 10_000, {
        agentClass: 'typescript-implementer',
      });
    }

    const store = JSON.parse(await readFile(footprintStateFilePath, 'utf8'));
    const entry = store[String(SKEWED_PID)];
    // Compress firstPolledAt toward lastPolledAt: naive interval becomes
    // (lastPolledAt - firstPolledAt) / (trajectory.length - 1) = 4000ms / 4
    // = 1000ms — far below MIN_FOOTPRINT_POLL_INTERVAL_MS (10s).
    entry.firstPolledAt = entry.lastPolledAt - 4_000;
    await writeFile(footprintStateFilePath, JSON.stringify(store), 'utf8');

    const { status, stderr, stdout } = runCli(
      ['--orchestrator-id=orch-leak-block-clock-skew', '--desired-agents=1'],
      baseEnv(),
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const body = JSON.parse(stdout);

    expect(body.type).toBe('spawn-allowed');
    expect(Array.isArray(body.leakBlocks)).toBe(true);
    const leakEntry = body.leakBlocks.find((entry_) => entry_.pid === SKEWED_PID);
    expect(leakEntry).toBeUndefined();
  },
);
