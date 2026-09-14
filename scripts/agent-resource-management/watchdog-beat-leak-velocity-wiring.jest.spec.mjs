// Phase 2 — integration tests pinning the Test Strategy scenarios from
// the Build Plan: wiring Phase 1's `computeLeakVelocity`
// (./lib/leak-velocity.mjs) into `runWatchdogBeat`'s (./lib/watchdog-beat.mjs)
// `trips` array, alongside the pre-existing `isAgentStalled` signal, with a
// deterministic dedup/precedence rule when both signals fire for the same
// pid in the same beat.
//
// RED by construction, right now: `runWatchdogBeat`'s Step 1 (see
// ./lib/watchdog-beat.mjs) only ever consults `deps.isAgentStalled` per
// candidate and unconditionally stamps `reason: 'stalled'` on every trip —
// there is no leak-velocity signal consulted anywhere in today's
// implementation, and no `reason: 'leak-velocity'` value exists at all. Every
// test below that expects a leak-velocity trip fails because that trip is
// simply absent from `trips` (not a crash — a "field/array has the wrong
// value" failure), which is the correct RED shape per this ticket's Phase 2
// task: "confirm red for a NEW reason (missing wiring), not a naming/import
// error."
//
// Do not add a stub implementation to make this pass — the Phase 2 builder
// implements the real wiring to turn this green.
//
// ---------------------------------------------------------------------------
// DESIGN GUESS THIS FILE BAKES IN — not binding on the eventual implementer;
// update this file's inputs/assertions to match whatever concrete shape
// actually ships, per this skill's own established "integration tests pin
// the observable contract, not internals" convention (see
// ./watchdog-outer-acceptance.jest.spec.mjs's and
// ./leak-velocity-outer-acceptance.jest.spec.mjs's own headers, which
// explicitly invite exactly this kind of later amendment).
//
// `runWatchdogBeat`'s `candidates` entries gain an OPTIONAL new field,
// `leakVelocityState`, shaped exactly like Phase 1's `computeLeakVelocity`
// `state` parameter (`{ trajectory, firstPolledAt, lastPolledAt,
// windowStartAt?, consecutiveAboveThreshold }`) — the per-pid persisted
// footprint-state.mjs-shaped trajectory this beat's CLI-level caller (out of
// this module's own scope; see ./lib/footprint-state.mjs's own header) is
// responsible for reading before invoking this beat. A candidate with NO
// `leakVelocityState` at all (undefined) models "no footprint-state.mjs
// entry exists for this pid" (Test Strategy scenario 4) — this must never
// throw, simply never produce a leak-velocity trip for that pid.
// `runWatchdogBeat` also gains an optional `leakVelocityConfig` parameter,
// defaulting to Phase 1's own `DEFAULT_LEAK_VELOCITY_THRESHOLDS`, and an
// optional `deps.computeLeakVelocity` injection seam (defaulting to the real
// import from ./lib/leak-velocity.mjs), mirroring every other composed
// dependency's own `deps.xxx` seam in this module.
//
// A trip produced by this new signal is expected to carry `reason:
// 'leak-velocity'` — genuinely distinct from the pre-existing `reason:
// 'stalled'` — in `trips`, so a downstream consumer (the liveness NDJSON log,
// a human reading the beat's JSON body) can tell which signal fired.
//
// PRECEDENCE — when a pid trips BOTH `isAgentStalled` and leak-velocity in
// the same beat, this file pins ONE reasonable choice: `stalled` wins
// (it is the pre-existing, more severe/certain signal — a stalled agent is
// already known to be making zero progress at all, whereas a leak-velocity
// trip is a resource-growth heuristic that does not by itself prove the
// agent has stopped doing useful work). The Phase 2 implementer may choose a
// different deterministic order if they can justify it (e.g. `leak-velocity`
// wins because an OOM is imminent and time-sensitive) — if so, adjust ONLY
// the pinned reason string in the "both signals" test below to match; do NOT
// weaken that test's "evicted exactly once" invariant, which is the actual
// requirement from the Build Plan.
// ---------------------------------------------------------------------------

import { jest } from '@jest/globals';

import { spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { runWatchdogBeat, DEFAULT_MAX_LEAK_VELOCITY_EVICTIONS_PER_BEAT } from './lib/watchdog-beat.mjs';
import { DEFAULT_LEAK_VELOCITY_THRESHOLDS } from './lib/leak-velocity.mjs';
import { readLivenessLog } from './lib/liveness-log.mjs';
// — the real `--evict-pid` beat's own fail-closed orchestrator-pid-
// binding check needs a genuinely-recorded, matching pid on the dibs ledger
// BEFORE the recycle attempt; `declareDibsFor`'s real-CLI-beat idiom never
// resolves a `/claude`-rooted ancestor inside Jest, so it never records one
// itself. Mirrors ./watchdog-outer-acceptance.jest.spec.mjs's own
// `declareDibsWithPid` exactly.
import { declareDibs } from './lib/coordination-file.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

// Single claude-rooted agent tree — mirrors ./cli-evict.jest.spec.mjs's own
// PS_AUTHORIZED fixture shape (own-spawned, authorized for eviction under
// --orchestrator-pid=501). Every PID here is SYNTHETIC — never a real process
// on the host running this test.
const PS_AUTHORIZED = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9201   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9202   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9203   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
`;

const ORCHESTRATOR_PID = 501;

/** Runs the real cli.mjs as a real child process — never imports it directly (matches every sibling cli.*.jest.spec.mjs). */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

let workDir;
let queueFilePath;
let coordinationFilePath;
let deadlockStateFilePath;
let liveLogFilePath;
let fakeKillLogPath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-watchdog-leak-wiring-'));
  queueFilePath = join(workDir, 'queue.json');
  coordinationFilePath = join(workDir, 'coordination.json');
  deadlockStateFilePath = join(workDir, 'deadlock-state.json');
  liveLogFilePath = join(workDir, 'liveness-log.json');
  fakeKillLogPath = join(workDir, 'fake-kill-log.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

function baseEnv(extra = {}) {
  return {
    ARM_QUEUE_FILE: queueFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_EVICT_TEST_MODE: '1',
    ARM_FAKE_KILL_LOG: fakeKillLogPath,
    ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
    ARM_FAKE_KILL_RESULT: '',
    ARM_BEAT_NOW_TEST_MODE: '1',
    ARM_FAKE_NOW_MS: String(Date.now()),
    ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 100 MB',
    ARM_EVICT_GRACE_MS: '5',
    ...extra,
  };
}

/** Mirrors ./cli-evict.jest.spec.mjs's own `declareDibsFor` exactly. */
function declareDibsFor(orchestratorId) {
  const result = runCli(
    [`--orchestrator-id=${orchestratorId}`, '--desired-agents=0'],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: undefined,
    }),
  );
  expect(result.status).toBe(0);
}

/** Mirrors ./cli-evict.jest.spec.mjs's own `seedLeasedItem` exactly. */
function seedLeasedItem({
  orchestratorId,
  agentClass = 'typescript-implementer',
  commandRef = 'resumable-workflow --resume=1500-phase-2',
}) {
  declareDibsFor(orchestratorId);

  const enqueueResult = runCli(
    [`--enqueue=${agentClass}:normal:${commandRef}`, `--orchestrator-id=${orchestratorId}`],
    baseEnv({ ARM_FAKE_COLLECT_JSON: undefined, ARM_FAKE_PS_OUTPUT: undefined }),
  );
  expect(enqueueResult.status).toBe(0);

  const dequeueResult = runCli(
    ['--dequeue-if-capacity', `--orchestrator-id=${orchestratorId}`],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: undefined,
    }),
  );
  expect(dequeueResult.status).toBe(0);
  const dequeued = JSON.parse(dequeueResult.stdout);
  expect(dequeued.granted).toBe(true);
  return { leaseId: dequeued.leaseId, orchestratorId };
}

/**
 * Seeds (or overwrites) `orchestratorId`'s dibs-ledger entry with a SPECIFIC
 * `pid`, directly via `declareDibs` — mirrors
 * ./watchdog-outer-acceptance.jest.spec.mjs's own `declareDibsWithPid`
 * exactly. Must run AFTER `seedLeasedItem` for the same `orchestratorId`.
 */
async function declareDibsWithPid(orchestratorId, pid) {
  const now = Date.now();
  await declareDibs(coordinationFilePath, {
    orchestratorId,
    desiredAgents: 0,
    declaredAt: now,
    firstDeclaredAt: now,
    pid,
  });
}

function peekItems(orchestratorId) {
  const result = runCli(
    ['--peek', `--orchestrator-id=${orchestratorId}`],
    baseEnv({ ARM_FAKE_COLLECT_JSON: undefined, ARM_FAKE_PS_OUTPUT: undefined }),
  );
  expect(result.status).toBe(0);
  return JSON.parse(result.stdout).items;
}

/** Wires the real, already-shipped `--evict-pid` beat as handleEvict — never a bare mock. */
function realHandleEvict(nowMs) {
  return function handleEvict(target) {
    const result = runCli(
      [
        `--evict-pid=${target.pid}`,
        `--lease-id=${target.leaseId}`,
        `--orchestrator-id=${target.orchestratorId}`,
        `--orchestrator-pid=${ORCHESTRATOR_PID}`,
      ],
      baseEnv({
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
        ARM_FAKE_NOW_MS: String(nowMs),
      }),
    );
    expect(result.status).toBe(0);
    return JSON.parse(result.stdout);
  };
}

/**
 * A `leakVelocityState` (see this file's header design-guess) that trips on
 * THIS call: growth rate comfortably clears `thresholdMbPerMin`, and
 * `consecutiveAboveThreshold` starts one poll short of
 * `consecutivePollsRequired` so this single call's increment reaches it.
 */
function sustainedLeakState(nowMs) {
  return {
    trajectory: [200, 400], // +200MB over 5 minutes => 40 MB/min, well above the 10 MB/min default threshold
    firstPolledAt: nowMs - 5 * 60_000,
    lastPolledAt: nowMs,
    consecutiveAboveThreshold: DEFAULT_LEAK_VELOCITY_THRESHOLDS.consecutivePollsRequired - 1,
  };
}

/**
 * A `leakVelocityState` (see `sustainedLeakState`) that trips this call with
 * a CONTROLLED growth rate — `growthMb` MB over the same fixed 5-minute
 * window `sustainedLeakState` uses — so throttle-ordering tests can seed
 * multiple pids with distinct, known `growthRateMbPerMin` values.
 */
function leakStateWithGrowth(nowMs, growthMb) {
  return {
    trajectory: [200, 200 + growthMb],
    firstPolledAt: nowMs - 5 * 60_000,
    lastPolledAt: nowMs,
    consecutiveAboveThreshold: DEFAULT_LEAK_VELOCITY_THRESHOLDS.consecutivePollsRequired - 1,
  };
}

/**
 * A `leakVelocityState` representing normal/interrupted growth: the most
 * recent measured rate is comfortably BELOW threshold, so `computeLeakVelocity`
 * resets the streak and never trips this call — regardless of how many
 * consecutive above-threshold polls preceded it (modelling "interrupted").
 */
function normalOrInterruptedLeakState(nowMs, priorStreak = 0) {
  return {
    trajectory: [500, 505], // +5MB over 5 minutes => 1 MB/min, below the 10 MB/min default threshold
    firstPolledAt: nowMs - 5 * 60_000,
    lastPolledAt: nowMs,
    consecutiveAboveThreshold: priorStreak,
  };
}

// ---------------------------------------------------------------------------
// 1. Evicts a pid with sustained above-threshold growth via the real
// resolveEvictionTargets/handleEvict core (ARM_EVICT_TEST_MODE seam, no live
// kill).
// ---------------------------------------------------------------------------

it('evicts a pid with sustained above-threshold footprint growth via the real resolveEvictionTargets/handleEvict core', async () => {
  const LEAK_PID = 9201;
  const seed = seedLeasedItem({ orchestratorId: 'orch-leak-wiring-sustained' });
  await declareDibsWithPid(seed.orchestratorId, ORCHESTRATOR_PID);
  const nowMs = Date.now();

  const result = await runWatchdogBeat({
    now: nowMs,
    boundMs: 10 * 60 * 1000,
    deadlockWindowMs: 10 * 60 * 1000,
    agingBoundMs: 10 * 60 * 1000,
    candidates: [{ pid: LEAK_PID, footprintTrajectory: [], worktreeMtimeMs: nowMs, leakVelocityState: sustainedLeakState(nowMs) }],
    claimLedger: [
      { pid: LEAK_PID, leaseId: seed.leaseId, orchestratorPid: ORCHESTRATOR_PID, orchestratorId: seed.orchestratorId, reentrant: true },
    ],
    coordinationFilePath,
    queueFilePath,
    deadlockStateFilePath,
    liveLogFilePath,
    leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
    handleEvict: realHandleEvict(nowMs),
    deps: { isAgentStalled: () => false },
  });

  expect(result.type).toBe('watchdog');

  const trip = result.trips.find((entry) => entry.pid === LEAK_PID);
  expect(trip).toBeDefined();
  expect(trip.reason).toBe('leak-velocity');
  expect(trip.evicted).toBe(true);

  expect(result.recycled).toContain(LEAK_PID);
  const killLog = JSON.parse(await readFile(fakeKillLogPath, 'utf8'));
  expect(killLog.length).toBeGreaterThan(0);
  expect(killLog.every((entry) => entry.pid === LEAK_PID)).toBe(true);
  expect(killLog[0].signal).toBe('SIGTERM');

  const remaining = peekItems(seed.orchestratorId);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBeUndefined();
});

// ---------------------------------------------------------------------------
// 2. Does not evict a pid with normal or interrupted growth.
// ---------------------------------------------------------------------------

it('does not flag or evict a pid whose footprint growth is normal or was interrupted before reaching a sustained streak', async () => {
  const NORMAL_PID = 9202;
  const seed = seedLeasedItem({ orchestratorId: 'orch-leak-wiring-normal' });
  const nowMs = Date.now();
  const handleEvict = jest.fn(async () => ({ type: 'evict', released: true }));

  const result = await runWatchdogBeat({
    now: nowMs,
    boundMs: 10 * 60 * 1000,
    deadlockWindowMs: 10 * 60 * 1000,
    agingBoundMs: 10 * 60 * 1000,
    candidates: [
      {
        pid: NORMAL_PID,
        footprintTrajectory: [],
        worktreeMtimeMs: nowMs,
        // A streak of 3 prior above-threshold polls is INTERRUPTED by this
        // below-threshold poll — must never trip, per computeLeakVelocity's
        // own documented reset-on-below-threshold contract.
        leakVelocityState: normalOrInterruptedLeakState(nowMs, 3),
      },
    ],
    claimLedger: [
      {
        pid: NORMAL_PID,
        leaseId: seed.leaseId,
        orchestratorPid: ORCHESTRATOR_PID,
        orchestratorId: seed.orchestratorId,
        reentrant: true,
      },
    ],
    coordinationFilePath,
    queueFilePath,
    deadlockStateFilePath,
    liveLogFilePath,
    leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
    handleEvict,
    deps: { isAgentStalled: () => false },
  });

  expect(result.type).toBe('watchdog');
  const trip = result.trips.find((entry) => entry.pid === NORMAL_PID);
  expect(trip).toBeUndefined();
  expect(result.recycled).not.toContain(NORMAL_PID);
  expect(handleEvict).not.toHaveBeenCalled();

  const remaining = peekItems(seed.orchestratorId);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(seed.leaseId);
});

// ---------------------------------------------------------------------------
// 3. A pid that trips BOTH isAgentStalled and leak-velocity in the same beat
// is evicted exactly once, with a documented, deterministic (never
// ordering-dependent) precedence reason.
//
// This test asserts the OBSERVABLE INVARIANT the Build Plan actually
// requires — exactly one eviction, and the stamped reason/outcome must be
// internally consistent — and additionally pins ONE reasonable precedence
// choice (`stalled` wins; see this file's header for the rationale and the
// instruction to the implementer on how to adjust this one string if a
// different, justified precedence order is chosen instead).
// ---------------------------------------------------------------------------

it('evicts a pid tripping both isAgentStalled and leak-velocity in the same beat exactly once, with a deterministic precedence reason', async () => {
  const BOTH_PID = 9203;
  const seed = seedLeasedItem({ orchestratorId: 'orch-leak-wiring-both' });
  await declareDibsWithPid(seed.orchestratorId, ORCHESTRATOR_PID);
  const nowMs = Date.now();

  const result = await runWatchdogBeat({
    now: nowMs,
    boundMs: 10 * 60 * 1000,
    deadlockWindowMs: 10 * 60 * 1000,
    agingBoundMs: 10 * 60 * 1000,
    candidates: [
      {
        pid: BOTH_PID,
        footprintTrajectory: [100, 100, 100], // flat — isAgentStalled's own CPU-flatness half
        worktreeMtimeMs: 0, // no advance at all — isAgentStalled's own mtime-staleness half
        leakVelocityState: sustainedLeakState(nowMs), // ALSO tripping leak-velocity this same beat
      },
    ],
    claimLedger: [
      { pid: BOTH_PID, leaseId: seed.leaseId, orchestratorPid: ORCHESTRATOR_PID, orchestratorId: seed.orchestratorId, reentrant: true },
    ],
    coordinationFilePath,
    queueFilePath,
    deadlockStateFilePath,
    liveLogFilePath,
    leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
    handleEvict: realHandleEvict(nowMs),
    // Force isAgentStalled true for BOTH_PID regardless of the real
    // liveness.mjs bound arithmetic — this test's concern is the DEDUP rule,
    // not liveness.mjs's own stall judgement (already covered elsewhere).
    deps: { isAgentStalled: ({ pid }) => pid === BOTH_PID },
  });

  expect(result.type).toBe('watchdog');

  // Exactly ONE trip entry for this pid — never two (one per signal).
  const tripsForPid = result.trips.filter((entry) => entry.pid === BOTH_PID);
  expect(tripsForPid).toHaveLength(1);
  const trip = tripsForPid[0];

  // Pinned precedence choice — see this file's header. Adjust ONLY this
  // string if the implementer justifies a different deterministic order.
  expect(trip.reason).toBe('stalled');

  // Exactly one eviction — never a double-attempt/double-log for the same pid.
  const recycledOccurrences = result.recycled.filter((pid) => pid === BOTH_PID);
  expect(recycledOccurrences).toHaveLength(1);
  expect(trip.evicted).toBe(true);

  const killLog = JSON.parse(await readFile(fakeKillLogPath, 'utf8'));
  const killsForPid = killLog.filter((entry) => entry.pid === BOTH_PID);
  expect(killsForPid).toHaveLength(1);
});

// ---------------------------------------------------------------------------
// 4. Skips a pid with no footprint-state.mjs entry without throwing.
// ---------------------------------------------------------------------------

it('skips a pid with no footprint-state entry without throwing', async () => {
  const NO_STATE_PID = 9301;

  await expect(
    runWatchdogBeat({
      now: Date.now(),
      boundMs: 10 * 60 * 1000,
      deadlockWindowMs: 10 * 60 * 1000,
      agingBoundMs: 10 * 60 * 1000,
      candidates: [
        {
          pid: NO_STATE_PID,
          footprintTrajectory: [80, 140, 95],
          worktreeMtimeMs: Date.now(),
          // No `leakVelocityState` at all — models "this pid has no
          // footprint-state.mjs entry" (e.g. cold-start, never polled).
        },
      ],
      claimLedger: [],
      coordinationFilePath,
      queueFilePath,
      deadlockStateFilePath,
      liveLogFilePath,
      leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
      deps: { isAgentStalled: () => false },
    }),
  ).resolves.not.toThrow();

  const result = await runWatchdogBeat({
    now: Date.now(),
    boundMs: 10 * 60 * 1000,
    deadlockWindowMs: 10 * 60 * 1000,
    agingBoundMs: 10 * 60 * 1000,
    candidates: [{ pid: NO_STATE_PID, footprintTrajectory: [80, 140, 95], worktreeMtimeMs: Date.now() }],
    claimLedger: [],
    coordinationFilePath,
    queueFilePath,
    deadlockStateFilePath,
    liveLogFilePath,
    leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
    deps: { isAgentStalled: () => false },
  });

  expect(result.type).toBe('watchdog');
  expect(result.trips.find((entry) => entry.pid === NO_STATE_PID)).toBeUndefined();
});

// ---------------------------------------------------------------------------
// 5. A leak-velocity trip for a pid absent from claimLedger is not evicted —
// the ledger remains the trust source (mirrors eviction-targeting.mjs's own
// "no matching claimLedger record" skip path for a watchdog-trip pid).
// ---------------------------------------------------------------------------

it('does not evict a leak-velocity trip for a pid with no matching claimLedger record', async () => {
  const UNLEDGERED_PID = 9401;
  const nowMs = Date.now();
  const handleEvict = jest.fn(async () => ({ type: 'evict', released: true }));

  const result = await runWatchdogBeat({
    now: nowMs,
    boundMs: 10 * 60 * 1000,
    deadlockWindowMs: 10 * 60 * 1000,
    agingBoundMs: 10 * 60 * 1000,
    candidates: [
      { pid: UNLEDGERED_PID, footprintTrajectory: [], worktreeMtimeMs: nowMs, leakVelocityState: sustainedLeakState(nowMs) },
    ],
    claimLedger: [], // deliberately empty — no record for UNLEDGERED_PID
    coordinationFilePath,
    queueFilePath,
    deadlockStateFilePath,
    liveLogFilePath,
    leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
    handleEvict,
    deps: { isAgentStalled: () => false },
  });

  expect(result.type).toBe('watchdog');
  // The trip itself may still be detected/observable (mirrors the existing
  // watchdog-trip "skip" path's own contract: the SIGNAL fires, only the
  // RECYCLE is refused) — what this test actually pins is the eviction
  // refusal, per the Build Plan's own wording ("is not evicted").
  expect(result.recycled).not.toContain(UNLEDGERED_PID);
  expect(handleEvict).not.toHaveBeenCalled();
});

// ---------------------------------------------------------------------------
// 6. Two different pids of the same orchestratorId tripping via different
// mechanisms (stall vs leak-velocity) in one beat: minimum bar per the Build
// Plan pending an stability precedence decision — no crash, and the
// outcome is deterministic and re-run-stable (identical input -> identical
// output). This assertion is INTENTIONALLY loose beyond that (documented
// below) — it does not pin whether both are evicted independently or guarded
// like the deadlock-trip multi-candidate-per-orchestrator case.
// ---------------------------------------------------------------------------

it('produces a deterministic, re-run-stable result (no crash) for two pids of the same orchestratorId tripping via different mechanisms in one beat', async () => {
  const STALLED_PID = 9501;
  const LEAK_PID = 9502;
  const SHARED_ORCHESTRATOR_ID = 'orch-leak-wiring-shared';
  const nowMs = Date.now();
  const handleEvict = jest.fn(async () => ({ type: 'evict', released: true }));

  function buildArgs() {
    return {
      now: nowMs,
      boundMs: 10 * 60 * 1000,
      deadlockWindowMs: 10 * 60 * 1000,
      agingBoundMs: 10 * 60 * 1000,
      candidates: [
        { pid: STALLED_PID, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 },
        { pid: LEAK_PID, footprintTrajectory: [], worktreeMtimeMs: nowMs, leakVelocityState: sustainedLeakState(nowMs) },
      ],
      claimLedger: [
        {
          pid: STALLED_PID,
          leaseId: 'lease-stalled',
          orchestratorPid: ORCHESTRATOR_PID,
          orchestratorId: SHARED_ORCHESTRATOR_ID,
          reentrant: true,
        },
        {
          pid: LEAK_PID,
          leaseId: 'lease-leak',
          orchestratorPid: ORCHESTRATOR_PID,
          orchestratorId: SHARED_ORCHESTRATOR_ID,
          reentrant: true,
        },
      ],
      coordinationFilePath,
      queueFilePath,
      deadlockStateFilePath,
      liveLogFilePath,
      leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
      handleEvict,
      deps: { isAgentStalled: ({ pid }) => pid === STALLED_PID },
    };
  }

  // NOTE: this test's own concern is determinism of runWatchdogBeat's
  // returned VALUE for identical input, not idempotency of its side effects
  // on the shared coordination/queue/liveness-log files — a second call
  // against the SAME on-disk files is not guaranteed side-effect-free (e.g.
  // NDJSON log growth, one-shot escalation stamps) even once this pid is
  // deterministically resolved. Both calls therefore run against their own,
  // freshly seeded temp files so only the INPUT stays identical.
  const firstRunDir = await mkdtemp(join(tmpdir(), 'arm-watchdog-leak-shared-a-'));
  const secondRunDir = await mkdtemp(join(tmpdir(), 'arm-watchdog-leak-shared-b-'));
  try {
    const firstResult = await runWatchdogBeat({
      ...buildArgs(),
      coordinationFilePath: join(firstRunDir, 'coordination.json'),
      queueFilePath: join(firstRunDir, 'queue.json'),
      deadlockStateFilePath: join(firstRunDir, 'deadlock-state.json'),
      liveLogFilePath: join(firstRunDir, 'liveness-log.json'),
    });
    const secondResult = await runWatchdogBeat({
      ...buildArgs(),
      coordinationFilePath: join(secondRunDir, 'coordination.json'),
      queueFilePath: join(secondRunDir, 'queue.json'),
      deadlockStateFilePath: join(secondRunDir, 'deadlock-state.json'),
      liveLogFilePath: join(secondRunDir, 'liveness-log.json'),
    });

    // No crash from either call (an unhandled rejection would already have
    // failed this test above) — and the two runs, given identical logical
    // input, must agree exactly.
    expect(secondResult).toEqual(firstResult);

    // Both candidates' signals are at least observable somewhere in the
    // output — the loose part (per this test's own header note) is HOW
    // multi-pid-per-orchestrator is resolved, not THAT it is resolved
    // without crashing.
    const pidsSeen = new Set([
      ...firstResult.trips.map((entry) => entry.pid),
      ...(firstResult.recycled ?? []),
    ]);
    expect(pidsSeen.size).toBeGreaterThan(0);
  } finally {
    await rm(firstRunDir, { recursive: true, force: true });
    await rm(secondRunDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// stability review, Medium 1 — `growthRateMbPerMin`
// must reach the durable liveness NDJSON log for a leak-velocity trip, not
// just the transient `trips` array.
// ---------------------------------------------------------------------------

it('threads growthRateMbPerMin through to the durable liveness log for a leak-velocity trip', async () => {
  const LEAK_PID = 9201;
  const seed = seedLeasedItem({ orchestratorId: 'orch-leak-observability' });
  await declareDibsWithPid(seed.orchestratorId, ORCHESTRATOR_PID);
  const nowMs = Date.now();

  const result = await runWatchdogBeat({
    now: nowMs,
    boundMs: 10 * 60 * 1000,
    deadlockWindowMs: 10 * 60 * 1000,
    agingBoundMs: 10 * 60 * 1000,
    candidates: [{ pid: LEAK_PID, footprintTrajectory: [], worktreeMtimeMs: nowMs, leakVelocityState: sustainedLeakState(nowMs) }],
    claimLedger: [
      { pid: LEAK_PID, leaseId: seed.leaseId, orchestratorPid: ORCHESTRATOR_PID, orchestratorId: seed.orchestratorId, reentrant: true },
    ],
    coordinationFilePath,
    queueFilePath,
    deadlockStateFilePath,
    liveLogFilePath,
    leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
    handleEvict: realHandleEvict(nowMs),
    deps: { isAgentStalled: () => false },
  });

  const trip = result.trips.find((entry) => entry.pid === LEAK_PID);
  expect(trip.growthRateMbPerMin).toBeGreaterThan(0);

  const logEntries = await readLivenessLog(liveLogFilePath);
  const tripEntry = logEntries.find(
    (entry) => entry.detectionType === 'watchdog-trip' && entry.pid === LEAK_PID,
  );
  expect(tripEntry).toBeDefined();
  expect(tripEntry.detail.growthRateMbPerMin).toBe(trip.growthRateMbPerMin);
});

// ---------------------------------------------------------------------------
// stability review, Medium 2 — a dual-trip pid's
// leak-velocity evidence must survive as `corroboratingSignals` on the
// winning `stalled` trip, both in the returned `trips` array and in the
// durable liveness log, while still being evicted exactly once with
// `reason: 'stalled'`.
// ---------------------------------------------------------------------------

it('records corroboratingSignals for a dual-trip pid while still evicting exactly once as stalled', async () => {
  const BOTH_PID = 9203;
  const seed = seedLeasedItem({ orchestratorId: 'orch-leak-corroboration' });
  await declareDibsWithPid(seed.orchestratorId, ORCHESTRATOR_PID);
  const nowMs = Date.now();

  const result = await runWatchdogBeat({
    now: nowMs,
    boundMs: 10 * 60 * 1000,
    deadlockWindowMs: 10 * 60 * 1000,
    agingBoundMs: 10 * 60 * 1000,
    candidates: [
      {
        pid: BOTH_PID,
        footprintTrajectory: [100, 100, 100],
        worktreeMtimeMs: 0,
        leakVelocityState: sustainedLeakState(nowMs),
      },
    ],
    claimLedger: [
      { pid: BOTH_PID, leaseId: seed.leaseId, orchestratorPid: ORCHESTRATOR_PID, orchestratorId: seed.orchestratorId, reentrant: true },
    ],
    coordinationFilePath,
    queueFilePath,
    deadlockStateFilePath,
    liveLogFilePath,
    leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
    handleEvict: realHandleEvict(nowMs),
    deps: { isAgentStalled: ({ pid }) => pid === BOTH_PID },
  });

  const tripsForPid = result.trips.filter((entry) => entry.pid === BOTH_PID);
  expect(tripsForPid).toHaveLength(1);
  const trip = tripsForPid[0];
  expect(trip.reason).toBe('stalled');
  expect(trip.corroboratingSignals).toEqual([
    { reason: 'leak-velocity', growthRateMbPerMin: expect.any(Number) },
  ]);

  expect(result.recycled.filter((pid) => pid === BOTH_PID)).toHaveLength(1);
  expect(trip.evicted).toBe(true);

  const logEntries = await readLivenessLog(liveLogFilePath);
  const tripEntry = logEntries.find(
    (entry) => entry.detectionType === 'watchdog-trip' && entry.pid === BOTH_PID,
  );
  expect(tripEntry).toBeDefined();
  // `reason` is only persisted for a non-recycled outcome (mirrors the
  // pre-existing `resolveOutcome`/`reason` convention) — this pid WAS
  // recycled, so `corroboratingSignals` is the durable evidence trail here.
  expect(tripEntry.detail.corroboratingSignals).toEqual(trip.corroboratingSignals);
});

// ---------------------------------------------------------------------------
// stability review, Medium 3 — a per-beat cap on
// leak-velocity-attributed evictions: when more leak-velocity trips fire in
// one beat than `maxLeakVelocityEvictionsPerBeat` allows, only the cap's
// worth are evicted (deterministically, by highest growth rate), and the
// rest are recorded as throttled without being evicted.
// ---------------------------------------------------------------------------

it('caps leak-velocity evictions per beat and records the excess as throttled, deterministically', async () => {
  const nowMs = Date.now();
  const pids = [9601, 9602, 9603, 9604]; // 4 pids trip; default cap is 3
  const growthByPid = { 9601: 200, 9602: 175, 9603: 150, 9604: 125 }; // MB over 5 min => descending rates

  function buildArgs(dirs) {
    return {
      now: nowMs,
      boundMs: 10 * 60 * 1000,
      deadlockWindowMs: 10 * 60 * 1000,
      agingBoundMs: 10 * 60 * 1000,
      candidates: pids.map((pid) => ({
        pid,
        footprintTrajectory: [],
        worktreeMtimeMs: nowMs,
        leakVelocityState: leakStateWithGrowth(nowMs, growthByPid[pid]),
      })),
      claimLedger: pids.map((pid) => ({
        pid,
        leaseId: `lease-${pid}`,
        orchestratorPid: ORCHESTRATOR_PID,
        orchestratorId: `orch-throttle-${pid}`,
        reentrant: true,
      })),
      coordinationFilePath: join(dirs, 'coordination.json'),
      queueFilePath: join(dirs, 'queue.json'),
      deadlockStateFilePath: join(dirs, 'deadlock-state.json'),
      liveLogFilePath: join(dirs, 'liveness-log.json'),
      leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
      handleEvict: jest.fn(async () => ({ type: 'evict', released: true })),
      deps: { isAgentStalled: () => false },
    };
  }

  async function runOnce() {
    const dir = await mkdtemp(join(tmpdir(), 'arm-watchdog-leak-throttle-'));
    try {
      return await runWatchdogBeat(buildArgs(dir));
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }

  const firstResult = await runOnce();
  const secondResult = await runOnce();

  // Default cap kept as documented — no cap param passed, so the module's
  // own default applies.
  expect(DEFAULT_MAX_LEAK_VELOCITY_EVICTIONS_PER_BEAT).toBe(3);

  const evictedPids = firstResult.recycled;
  expect(evictedPids).toHaveLength(3);
  // Highest growth rates win: 9601 (40MB/min), 9602 (35), 9603 (30) — 9604
  // (25) is the excess, throttled.
  expect(evictedPids.sort()).toEqual([9601, 9602, 9603]);

  const throttledTrip = firstResult.trips.find((entry) => entry.pid === 9604);
  expect(throttledTrip).toBeDefined();
  expect(throttledTrip.evicted).toBe(false);
  expect(throttledTrip.outcome).toBe('leak-velocity-throttled');
  expect(firstResult.recycled).not.toContain(9604);

  // Re-run stability: identical input -> identical output.
  expect(secondResult.recycled.sort()).toEqual(firstResult.recycled.sort());
  expect(secondResult.trips.find((entry) => entry.pid === 9604).outcome).toBe('leak-velocity-throttled');
});

// ---------------------------------------------------------------------------
// pre-PR review, Low — locks in the documented pid-ASCENDING tie-break
// (see runWatchdogBeat's Step 1d comment: "tie-broken by pid ASCENDING —
// never insertion order alone") for pids with EQUAL growth rates, so a
// future refactor of the sort comparator can't silently reintroduce
// insertion-order-dependent (non-deterministic) throttle selection without a
// test failing.
// ---------------------------------------------------------------------------

it('tie-breaks equal leak-velocity growth rates by pid ASCENDING when throttling, not insertion order', async () => {
  const nowMs = Date.now();
  // All 4 pids share the SAME growth rate (150MB/5min => 30 MB/min), so the
  // sort key that actually decides who is kept/throttled is the pid
  // ascending tie-break, not growth rate. Deliberately seeded out of
  // ascending order to prove the result isn't just insertion order either.
  const pids = [9704, 9701, 9703, 9702]; // default cap is 3 => highest pid (9704) is throttled
  const SAME_GROWTH_MB = 150;

  const dir = await mkdtemp(join(tmpdir(), 'arm-watchdog-leak-throttle-tiebreak-'));
  try {
    const result = await runWatchdogBeat({
      now: nowMs,
      boundMs: 10 * 60 * 1000,
      deadlockWindowMs: 10 * 60 * 1000,
      agingBoundMs: 10 * 60 * 1000,
      candidates: pids.map((pid) => ({
        pid,
        footprintTrajectory: [],
        worktreeMtimeMs: nowMs,
        leakVelocityState: leakStateWithGrowth(nowMs, SAME_GROWTH_MB),
      })),
      claimLedger: pids.map((pid) => ({
        pid,
        leaseId: `lease-${pid}`,
        orchestratorPid: ORCHESTRATOR_PID,
        orchestratorId: `orch-tiebreak-${pid}`,
        reentrant: true,
      })),
      coordinationFilePath: join(dir, 'coordination.json'),
      queueFilePath: join(dir, 'queue.json'),
      deadlockStateFilePath: join(dir, 'deadlock-state.json'),
      liveLogFilePath: join(dir, 'liveness-log.json'),
      leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
      handleEvict: jest.fn(async () => ({ type: 'evict', released: true })),
      deps: { isAgentStalled: () => false },
    });

    // Ascending-pid tie-break keeps the three LOWEST pids and throttles the
    // highest (9704) — the opposite of what array/insertion order (as seeded
    // above) or a descending tie-break would produce.
    expect(result.recycled.sort()).toEqual([9701, 9702, 9703]);
    const throttledTrip = result.trips.find((entry) => entry.pid === 9704);
    expect(throttledTrip).toBeDefined();
    expect(throttledTrip.evicted).toBe(false);
    expect(throttledTrip.outcome).toBe('leak-velocity-throttled');
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// Phase 4 pre-PR review, Medium 3 — a throttled leak-velocity-trip log
// entry must carry a real `orchestratorId` (resolved from `claimLedger`),
// not `undefined`, matching every other `logWatchdogTrip` call site's
// contract in this module.
// ---------------------------------------------------------------------------

it('resolves a real orchestratorId from claimLedger for a leak-velocity-throttled trip log entry', async () => {
  const nowMs = Date.now();
  const pids = [9611, 9612, 9613, 9614]; // 4 pids trip; default cap is 3
  const growthByPid = { 9611: 200, 9612: 175, 9613: 150, 9614: 125 }; // 9614 is the excess, throttled
  const THROTTLED_PID = 9614;
  const THROTTLED_ORCHESTRATOR_ID = 'orch-throttle-medium3';

  const dir = await mkdtemp(join(tmpdir(), 'arm-watchdog-leak-throttle-orch-id-'));
  try {
    const result = await runWatchdogBeat({
      now: nowMs,
      boundMs: 10 * 60 * 1000,
      deadlockWindowMs: 10 * 60 * 1000,
      agingBoundMs: 10 * 60 * 1000,
      candidates: pids.map((pid) => ({
        pid,
        footprintTrajectory: [],
        worktreeMtimeMs: nowMs,
        leakVelocityState: leakStateWithGrowth(nowMs, growthByPid[pid]),
      })),
      claimLedger: pids.map((pid) => ({
        pid,
        leaseId: `lease-${pid}`,
        orchestratorPid: ORCHESTRATOR_PID,
        orchestratorId: pid === THROTTLED_PID ? THROTTLED_ORCHESTRATOR_ID : `orch-throttle-medium3-${pid}`,
        reentrant: true,
      })),
      coordinationFilePath: join(dir, 'coordination.json'),
      queueFilePath: join(dir, 'queue.json'),
      deadlockStateFilePath: join(dir, 'deadlock-state.json'),
      liveLogFilePath: join(dir, 'liveness-log.json'),
      leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
      handleEvict: jest.fn(async () => ({ type: 'evict', released: true })),
      deps: { isAgentStalled: () => false },
    });

    const throttledTrip = result.trips.find((entry) => entry.pid === THROTTLED_PID);
    expect(throttledTrip).toBeDefined();
    expect(throttledTrip.outcome).toBe('leak-velocity-throttled');

    const logEntries = await readLivenessLog(join(dir, 'liveness-log.json'));
    const throttledEntry = logEntries.find(
      (entry) => entry.detectionType === 'watchdog-trip' && entry.pid === THROTTLED_PID,
    );
    expect(throttledEntry).toBeDefined();
    expect(throttledEntry.orchestratorId).toBe(THROTTLED_ORCHESTRATOR_ID);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// stability review, Medium 3 — the leak-velocity
// throttle must NOT apply to `stalled` trips: a beat with many stalled pids
// and no leak-velocity trips evicts all the stalled ones normally, even when
// the stalled count exceeds `maxLeakVelocityEvictionsPerBeat`.
// ---------------------------------------------------------------------------

it('does not throttle stalled trips even when their count exceeds maxLeakVelocityEvictionsPerBeat', async () => {
  const nowMs = Date.now();
  const stalledPids = [9701, 9702, 9703, 9704, 9705]; // 5 stalled pids, cap is 3
  const handleEvict = jest.fn(async () => ({ type: 'evict', released: true }));

  const result = await runWatchdogBeat({
    now: nowMs,
    boundMs: 10 * 60 * 1000,
    deadlockWindowMs: 10 * 60 * 1000,
    agingBoundMs: 10 * 60 * 1000,
    candidates: stalledPids.map((pid) => ({ pid, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 })),
    claimLedger: stalledPids.map((pid) => ({
      pid,
      leaseId: `lease-${pid}`,
      orchestratorPid: ORCHESTRATOR_PID,
      orchestratorId: `orch-stalled-${pid}`,
      reentrant: true,
    })),
    coordinationFilePath,
    queueFilePath,
    deadlockStateFilePath,
    liveLogFilePath,
    leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
    handleEvict,
    deps: { isAgentStalled: () => true },
  });

  expect(result.recycled.sort()).toEqual(stalledPids.sort());
  expect(handleEvict).toHaveBeenCalledTimes(stalledPids.length);
  for (const pid of stalledPids) {
    const trip = result.trips.find((entry) => entry.pid === pid);
    expect(trip.outcome).not.toBe('leak-velocity-throttled');
    expect(trip.evicted).toBe(true);
  }
});

// ---------------------------------------------------------------------------
// Phase 2 pre-PR review, High — the deadlock-tripwire path must not be
// usable as a side door around the leak-velocity throttle: a pid excluded
// from eviction this beat for being over `maxLeakVelocityEvictionsPerBeat`
// must stay excluded even when its orchestratorId is ALSO named in
// `deadlockTrip.stalledPids` this same beat. `resolveEvictionTargets`
// resolves `deadlockTrip.stalledPids` independently of the throttled-trip
// filtering applied to `watchdogTrips`, so without an explicit guard in
// `runWatchdogBeat` this pid would be evicted anyway via the deadlock-trip
// target, and the liveness log would carry two contradictory entries for it
// in one beat (see ./lib/watchdog-beat.mjs's Step 4 comment for the fix).
// ---------------------------------------------------------------------------

it('does not evict a leak-velocity-throttled pid even when its orchestratorId is also named via the deadlock-tripwire path in the same beat', async () => {
  const nowMs = Date.now();
  const pids = [9801, 9802, 9803, 9804]; // 4 pids trip; default cap is 3
  const growthByPid = { 9801: 200, 9802: 175, 9803: 150, 9804: 125 }; // descending rates -> 9804 is the excess, throttled
  const THROTTLED_PID = 9804;
  const THROTTLED_ORCHESTRATOR_ID = `orch-throttle-deadlock-${THROTTLED_PID}`;
  const handleEvict = jest.fn(async () => ({ type: 'evict', released: true }));

  const result = await runWatchdogBeat({
    now: nowMs,
    boundMs: 10 * 60 * 1000,
    deadlockWindowMs: 10 * 60 * 1000,
    agingBoundMs: 10 * 60 * 1000,
    candidates: pids.map((pid) => ({
      pid,
      footprintTrajectory: [],
      worktreeMtimeMs: nowMs,
      leakVelocityState: leakStateWithGrowth(nowMs, growthByPid[pid]),
    })),
    claimLedger: pids.map((pid) => ({
      pid,
      leaseId: `lease-${pid}`,
      orchestratorPid: ORCHESTRATOR_PID,
      orchestratorId: pid === THROTTLED_PID ? THROTTLED_ORCHESTRATOR_ID : `orch-throttle-deadlock-${pid}`,
      reentrant: true,
    })),
    coordinationFilePath,
    queueFilePath,
    deadlockStateFilePath,
    liveLogFilePath,
    leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
    handleEvict,
    deps: {
      isAgentStalled: () => false,
      // Bypasses the real coordination/queue-file-driven deadlock detection —
      // this test's own concern is what `runWatchdogBeat` does with a
      // deadlock trip that names the SAME orchestratorId as a pid already
      // throttled this beat, not how `detectDeadlock` itself decides to trip.
      detectDeadlock: async () => ({ trip: true, stalledPids: [THROTTLED_ORCHESTRATOR_ID] }),
    },
  });

  // Never evicted via the deadlock-tripwire side door.
  expect(result.recycled).not.toContain(THROTTLED_PID);
  expect(handleEvict).not.toHaveBeenCalledWith(expect.objectContaining({ pid: THROTTLED_PID }));

  const throttledTrip = result.trips.find((entry) => entry.pid === THROTTLED_PID);
  expect(throttledTrip).toBeDefined();
  expect(throttledTrip.evicted).toBe(false);
  expect(throttledTrip.outcome).toBe('leak-velocity-throttled');

  // Exactly ONE durable log entry for this pid this beat — no contradictory
  // second write from the deadlock-trip target being processed anyway.
  const logEntries = await readLivenessLog(liveLogFilePath);
  const entriesForPid = logEntries.filter((entry) => entry.pid === THROTTLED_PID);
  expect(entriesForPid).toHaveLength(1);
  expect(entriesForPid[0].outcome).toBe('leak-velocity-throttled');
});

// ---------------------------------------------------------------------------
// pre-PR review round 2, Medium — `buildWatchdogClaimLedger` (cli.mjs)
// only pairs `Math.min(candidates.length, leasedItems.length)` entries, so an
// orchestrator with >= 4 candidate pids (enough to trigger the leak-velocity
// throttle, whose default cap is 3) but FEWER leased queue items produces
// throttled pids with NO `claimLedger` entry at all. `orchestratorId` must
// fall back to the beat's own `orchestratorId` (threaded through
// `runWatchdogBeat`) rather than logging `undefined` in that reachable
// configuration.
// ---------------------------------------------------------------------------

it('falls back to the beat\'s own orchestratorId for a throttled trip with no matching claimLedger entry', async () => {
  const nowMs = Date.now();
  const pids = [9621, 9622, 9623, 9624]; // 4 pids trip; default cap is 3
  const growthByPid = { 9621: 200, 9622: 175, 9623: 150, 9624: 125 }; // 9624 is the excess, throttled
  const THROTTLED_PID = 9624;
  const BEAT_ORCHESTRATOR_ID = 'orch-throttle-ledger-fallback';

  const dir = await mkdtemp(join(tmpdir(), 'arm-watchdog-leak-throttle-fallback-'));
  try {
    const result = await runWatchdogBeat({
      now: nowMs,
      boundMs: 10 * 60 * 1000,
      deadlockWindowMs: 10 * 60 * 1000,
      agingBoundMs: 10 * 60 * 1000,
      candidates: pids.map((pid) => ({
        pid,
        footprintTrajectory: [],
        worktreeMtimeMs: nowMs,
        leakVelocityState: leakStateWithGrowth(nowMs, growthByPid[pid]),
      })),
      // Only 2 leased ledger items for 4 candidates — the throttled pid
      // (9624, the lowest growth rate) has no claimLedger record at all.
      claimLedger: [
        { pid: 9621, leaseId: 'lease-9621', orchestratorPid: ORCHESTRATOR_PID, orchestratorId: BEAT_ORCHESTRATOR_ID, reentrant: true },
        { pid: 9622, leaseId: 'lease-9622', orchestratorPid: ORCHESTRATOR_PID, orchestratorId: BEAT_ORCHESTRATOR_ID, reentrant: true },
      ],
      orchestratorId: BEAT_ORCHESTRATOR_ID,
      coordinationFilePath: join(dir, 'coordination.json'),
      queueFilePath: join(dir, 'queue.json'),
      deadlockStateFilePath: join(dir, 'deadlock-state.json'),
      liveLogFilePath: join(dir, 'liveness-log.json'),
      leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
      handleEvict: jest.fn(async () => ({ type: 'evict', released: true })),
      deps: { isAgentStalled: () => false },
    });

    const throttledTrip = result.trips.find((entry) => entry.pid === THROTTLED_PID);
    expect(throttledTrip).toBeDefined();
    expect(throttledTrip.outcome).toBe('leak-velocity-throttled');

    const logEntries = await readLivenessLog(join(dir, 'liveness-log.json'));
    const throttledEntry = logEntries.find(
      (entry) => entry.detectionType === 'watchdog-trip' && entry.pid === THROTTLED_PID,
    );
    expect(throttledEntry).toBeDefined();
    // Falls back to the beat's own orchestratorId — never undefined.
    expect(throttledEntry.orchestratorId).toBe(BEAT_ORCHESTRATOR_ID);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
