// Phase 0 outer acceptance test ("agent-resource-management:
// per-agent footprint growth-rate ('leak velocity') circuit breaker").
//
// Outer acceptance test (recycle path):
//
//   it("flags a candidate whose footprint trajectory shows sustained
//   above-threshold growth across N consecutive polls with a new
//   'leak-velocity' reason, recycles it via the real
//   resolveEvictionTargets/handleEvict core, and leaves a candidate with
//   normal/interrupted growth completely untouched")
//
//   Layer: integration, lib-level — drives the real `runWatchdogBeat`
//   (./lib/watchdog-beat.mjs) directly (mirrors ./lib/watchdog-beat.jest.spec.mjs's
//   own "drive the composed beat via direct import, inject only the seam
//   under test" idiom), but — unlike that file's fully-mocked
//   `jest.fn(async () => ({ type: 'evict', released: true }))` `handleEvict`
//   stand-ins — wires `handleEvict` to the REAL, already-shipped
//   `--evict-pid` beat (cli.mjs,), spawned as a real
//   child process with `ARM_EVICT_TEST_MODE=1` and the existing
//   `ARM_FAKE_KILL_LOG`/`ARM_FAKE_IS_ALIVE_SEQUENCE`/`ARM_FAKE_KILL_RESULT`
//   seams (mirrors ./cli-evict.jest.spec.mjs's own `baseEnv`/`declareDibsFor`/
//   `seedLeasedItem` idiom) — so this test proves an actual recycle through
//   the real eviction core, never a bare mock, while still never issuing a
//   real `process.kill` syscall against a real host pid.
//
//   `resolveEvictionTargets`/`filterEvictable` (./lib/eviction-targeting.mjs)
//   are exercised via `runWatchdogBeat`'s own REAL default imports — this
//   file does not override either dependency, so Step 4 of `runWatchdogBeat`
//   is the genuine, unmocked targeting core.
//
// ---------------------------------------------------------------------------
// RECONCILED WITH THE SHIPPED CONTRACT (Phase 1-3 landed) — this file's
// original design guess was WRONG on two points, now corrected:
//
// 1. There is no `./lib/leak-velocity.mjs` export named `detectLeakVelocity`.
//    The real, shipped export is `computeLeakVelocity(state, config)` — a
//    pure function taking a persisted `{ trajectory, firstPolledAt,
//    lastPolledAt, windowStartAt?, consecutiveAboveThreshold }` state plus a
//    `{ thresholdMbPerMin, consecutivePollsRequired, minSamples? }` config,
//    returning `{ trip, growthRateMbPerMin, consecutiveAboveThreshold }`. See
//    that module's own header for the full sustained-trip contract (mirrors
//    `computeAimdCeiling`'s AIMD sustained-window shape).
//
// 2. `runWatchdogBeat` was NOT extended to consult `deps.isAgentStalled` for
//    leak velocity (this file's original "TEST-LEVEL stand-in wrapper passed
//    via the isAgentStalled seam" design guess). Instead, Phase 2 gave each
//    `candidates[]` entry an OPTIONAL `leakVelocityState` field (shaped
//    exactly like `computeLeakVelocity`'s own `state` parameter) plus a
//    top-level, optional `leakVelocityConfig` parameter (defaulting to
//    `DEFAULT_LEAK_VELOCITY_THRESHOLDS`) and a `deps.computeLeakVelocity`
//    injection seam (defaulting to the real import) — see
//    ./lib/watchdog-beat.mjs's Step 1b/1c for the real wiring, and
//    ./watchdog-beat-leak-velocity-wiring.jest.spec.mjs for the proven
//    working test idiom against this exact contract, which this file follows
//    at the outer/black-box framing level rather than duplicating its
//    exhaustive scenario list.
//
// A trip produced by this signal carries a genuinely distinct
// `reason: 'leak-velocity'` in `trips` (never overloading the pre-existing
// `reason: 'stalled'`), and `isAgentStalled` is stubbed false for both
// candidates below so this test's own concern — the leak-velocity signal
// alone — is isolated from Phase 1's pre-existing stall signal (already
// covered by ./lib/watchdog-beat.jest.spec.mjs and
// ./watchdog-outer-acceptance.jest.spec.mjs).
//
// Kept to a SINGLE leaking pid (well under
// `DEFAULT_MAX_LEAK_VELOCITY_EVICTIONS_PER_BEAT`'s default of 3, per
// ./lib/watchdog-beat.mjs) so this fixture can never accidentally trip the
// Phase 2 per-beat throttle and get evicted=false/outcome=
// 'leak-velocity-throttled' instead of a genuine eviction — that throttle
// behaviour has its own dedicated coverage in
// ./watchdog-beat-leak-velocity-wiring.jest.spec.mjs and is out of scope for
// this outer acceptance test's single-recycle-path concern.
//
// SAFETY — no real SIGTERM/SIGKILL against an arbitrary host PID. Every
// "running agent" PID in this file's fixtures is SYNTHETIC (mirrors
// ./cli-evict.jest.spec.mjs's own PS_AUTHORIZED fixture verbatim), and the
// `handleEvict` wired in below always spawns the real `cli.mjs --evict-pid`
// beat with `ARM_EVICT_TEST_MODE=1` plus `ARM_FAKE_KILL_LOG`/
// `ARM_FAKE_PS_OUTPUT`/`ARM_FAKE_IS_ALIVE_SEQUENCE` — a real SIGTERM/SIGKILL
// syscall is never reachable from this file.
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { runWatchdogBeat } from './lib/watchdog-beat.mjs';
import { DEFAULT_LEAK_VELOCITY_THRESHOLDS } from './lib/leak-velocity.mjs';
import { declareDibs } from './lib/coordination-file.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

// Single claude-rooted agent tree, PID 501 parented under 1 — mirrors
// ./cli-evict.jest.spec.mjs's own PS_AUTHORIZED fixture shape (own-spawned,
// authorized for eviction under --orchestrator-pid=501). Every PID here is
// SYNTHETIC — never a real process on the host running this test.
const PS_AUTHORIZED = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9101   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9102   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
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
  workDir = await mkdtemp(join(tmpdir(), 'arm-leak-velocity-outer-'));
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
function seedLeasedItem({ orchestratorId, agentClass = 'typescript-implementer', commandRef = 'resumable-workflow --resume=1500' }) {
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
 * ./watchdog-beat-leak-velocity-wiring.jest.spec.mjs's own
 * `declareDibsWithPid` exactly. Needed for the real `--evict-pid` beat's own
 * fail-closed orchestrator-pid-binding check: it requires a
 * genuinely-recorded, matching pid on the dibs ledger before it will
 * authorize a recycle.
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

/**
 * `--peek` prints the FULL live queue (never filtered by `--orchestrator-id`
 * — see `handlePeek`/`peekQueue` in cli.mjs), so this helper filters down to
 * the single entry for `orchestratorId` itself. Both this test's seeded
 * orchestrators share the same `queueFilePath`, so a bare `--peek` call
 * legitimately returns entries for every orchestrator seeded so far.
 */
function peekItemFor(orchestratorId) {
  const result = runCli(
    ['--peek', `--orchestrator-id=${orchestratorId}`],
    baseEnv({ ARM_FAKE_COLLECT_JSON: undefined, ARM_FAKE_PS_OUTPUT: undefined }),
  );
  expect(result.status).toBe(0);
  const items = JSON.parse(result.stdout).items;
  return items.find((item) => item.orchestratorId === orchestratorId);
}

/**
 * A `leakVelocityState` (shaped per `computeLeakVelocity`'s real `state`
 * parameter) that trips on THIS call: growth rate comfortably clears
 * `thresholdMbPerMin`, and `consecutiveAboveThreshold` starts one poll short
 * of `consecutivePollsRequired` so this single call's increment reaches it.
 * Mirrors ./watchdog-beat-leak-velocity-wiring.jest.spec.mjs's own
 * `sustainedLeakState` exactly.
 */
function sustainedLeakState(nowMs) {
  return {
    trajectory: [200, 500], // +300MB over 5 minutes => 60 MB/min, well above the 10 MB/min default threshold
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
function normalOrInterruptedLeakState(nowMs, priorStreak = 3) {
  return {
    trajectory: [500, 505], // +5MB over 5 minutes => 1 MB/min, below the 10 MB/min default threshold
    firstPolledAt: nowMs - 5 * 60_000,
    lastPolledAt: nowMs,
    consecutiveAboveThreshold: priorStreak,
  };
}

it(
  'trips a new leak-velocity reason and recycles a candidate with sustained above-threshold footprint growth via the ' +
    'real resolveEvictionTargets/handleEvict core, while a candidate with normal/interrupted growth is never flagged ' +
    'or touched',
  async () => {
    const LEAK_PID = 9101;
    const NORMAL_PID = 9102;

    const leakSeed = seedLeasedItem({ orchestratorId: 'orch-leak-velocity-leaking' });
    const normalSeed = seedLeasedItem({ orchestratorId: 'orch-leak-velocity-normal' });
    await declareDibsWithPid(leakSeed.orchestratorId, ORCHESTRATOR_PID);

    const nowMs = Date.now();

    const claimLedger = [
      {
        pid: LEAK_PID,
        leaseId: leakSeed.leaseId,
        orchestratorPid: ORCHESTRATOR_PID,
        orchestratorId: leakSeed.orchestratorId,
        reentrant: true,
      },
      {
        pid: NORMAL_PID,
        leaseId: normalSeed.leaseId,
        orchestratorPid: ORCHESTRATOR_PID,
        orchestratorId: normalSeed.orchestratorId,
        reentrant: true,
      },
    ];

    /** Wires the real, already-shipped `--evict-pid` beat as handleEvict — never a bare mock. */
    function handleEvict(target) {
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
    }

    const beatResult = await runWatchdogBeat({
      now: nowMs,
      boundMs: 10 * 60 * 1000,
      deadlockWindowMs: 10 * 60 * 1000,
      agingBoundMs: 10 * 60 * 1000,
      candidates: [
        { pid: LEAK_PID, footprintTrajectory: [], worktreeMtimeMs: nowMs, leakVelocityState: sustainedLeakState(nowMs) },
        { pid: NORMAL_PID, footprintTrajectory: [], worktreeMtimeMs: nowMs, leakVelocityState: normalOrInterruptedLeakState(nowMs) },
      ],
      claimLedger,
      coordinationFilePath,
      queueFilePath,
      deadlockStateFilePath,
      liveLogFilePath,
      leakVelocityConfig: DEFAULT_LEAK_VELOCITY_THRESHOLDS,
      handleEvict,
      // Isolates this test's own concern — the leak-velocity signal alone —
      // from Phase 1's pre-existing stall signal, which has its own dedicated
      // coverage elsewhere (./lib/watchdog-beat.jest.spec.mjs,
      // ./watchdog-outer-acceptance.jest.spec.mjs).
      deps: { isAgentStalled: () => false },
    });

    expect(beatResult.type).toBe('watchdog');

    // The leaking candidate is flagged, under a genuinely distinct reason —
    // now real, shipped behaviour (Phase 2 wiring in ./lib/watchdog-beat.mjs).
    const leakTrip = beatResult.trips.find((entry) => entry.pid === LEAK_PID);
    expect(leakTrip).toBeDefined();
    expect(leakTrip.reason).toBe('leak-velocity');
    expect(leakTrip.evicted).toBe(true);

    // Recycled via the REAL, unmocked resolveEvictionTargets/handleEvict
    // core — only the fake kill log observed a signal, never a real
    // process.kill syscall.
    expect(beatResult.recycled).toContain(LEAK_PID);
    const killLog = JSON.parse(await readFile(fakeKillLogPath, 'utf8'));
    expect(killLog.length).toBeGreaterThan(0);
    expect(killLog.every((entry) => entry.pid === LEAK_PID)).toBe(true);
    expect(killLog[0].signal).toBe('SIGTERM');

    // The leaking candidate's leased work item was released back to the
    // queue as part of the real recycle.
    const leakRemaining = peekItemFor(leakSeed.orchestratorId);
    expect(leakRemaining).toBeDefined();
    expect(leakRemaining.leaseId).toBeUndefined();

    // The normal-growth candidate is never flagged, never evicted, and its
    // leased work item is left completely untouched.
    const normalTrip = beatResult.trips.find((entry) => entry.pid === NORMAL_PID);
    expect(normalTrip).toBeUndefined();
    expect(beatResult.recycled).not.toContain(NORMAL_PID);

    const normalRemaining = peekItemFor(normalSeed.orchestratorId);
    expect(normalRemaining).toBeDefined();
    expect(normalRemaining.leaseId).toBe(normalSeed.leaseId);
  },
);
