// Phase 3 — watchdog exemption pinning test.
//
// This is a PINNING test, not a red-by-construction one. Per the Build
// Plan's Phase 3 objective (see the `## Build Plan` comment on):
// `--watchdog-beat` resolves its own `{pid, leaseId}` pairs via a ps-tree/
// queue zip heuristic each beat (`lib/watchdog-beat.mjs`'s
// `buildWatchdogClaimLedger`), never via `bindPidToLease` — so THIS FILE's
// own scenario, where the lease was never bound by ANY caller, hands
// `performEviction` a `leasedEntry.pid === undefined`, and Phase 2's
// pid-lease-mismatch check skip-not-fails for it (the same "unbound lease"
// backward-compatibility branch `cli-evict.jest.spec.mjs`'s own
// unbound-lease scenario already pins for `--evict-pid`). This is a
// documented, intentional EXEMPTION for this beat's OWN pairing logic, not a
// blanket claim that every watchdog-driven recycle always sees
// `pid === undefined` — a lease some OTHER caller in the orchestrator's own
// dispatch sequence already bound via `--bind-pid` before the watchdog beat
// picks it up WILL carry a real `leasedEntry.pid`, and the watchdog's
// zip-derived pid is checked against it exactly like `--evict-pid`'s is; a
// disagreement there now REFUSES the recycle with `pid-lease-mismatch`
// (fail-closed) — see `watchdog-outer-acceptance.jest.spec.mjs`'s "refuses
// the recycle, and surfaces pid-lease-mismatch by name" test for that
// scenario, and SKILL.md's watchdog-beat section for the operator-facing
// consequence. The watchdog's own wrong-pairing risk (when no prior
// `--bind-pid` call exists for a lease) is instead mitigated by a DIFFERENT,
// already-existing compensating control
// (`countLedgerRecordsForOrchestrator`'s multi-candidate `alert-only`
// routing), not by pid-lease binding.
//
// This test should PASS TODAY — Phase 2's skip-not-fail behaviour for an
// unbound `leasedEntry.pid` already covers it, and this file's own scenario
// never calls `bindPidToLease` at all (mirroring how a real watchdog beat
// never does either). It exists to LOCK IN that unbound-lease behaviour
// explicitly, by name, rather than leaving it as an incidental consequence
// of `watchdog-outer-acceptance.jest.spec.mjs`'s own broader recycle test —
// so a future change that starts requiring a bound pid on every eviction
// path (accidentally closing the documented exemption without updating
// SKILL.md or this test) fails loudly here first.
//
// Layer: integration — real `cli.mjs --watchdog-beat` invocation via
// `spawnSync`, matching every sibling `watchdog-*`/`cli-evict*` spec in
// this skill (never importing `cli.mjs`'s unexported `performEviction`
// directly). Fixtures mirror `watchdog-outer-acceptance.jest.spec.mjs`'s
// own recycle scenario as closely as possible, since this test is
// deliberately "the same real-world path, asserted from the pid-binding
// angle" rather than a new scenario.

import { spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { declareDibs } from './coordination-file.mjs';
import { peekQueue } from './queue.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), '..', 'cli.mjs');

const MEMORY_RED = { pressureLevel: 1, swapUsedMb: 12288, compressedMb: 4096 };
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

// Single claude-rooted agent tree, PID 9001 parented under 501 — mirrors
// ./cli-evict.jest.spec.mjs's/./watchdog-outer-acceptance.jest.spec.mjs's own
// PS_AUTHORIZED fixture verbatim. PID 9001 is SYNTHETIC — never a real
// process on the host running this test.
const PS_AUTHORIZED = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001  51200       58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

/** Runs the real cli.mjs as a real child process — never imports it directly. */
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
let fakeKillLogPath;
let liveLogFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-watchdog-pid-binding-exemption-'));
  queueFilePath = join(workDir, 'queue.json');
  coordinationFilePath = join(workDir, 'coordination.json');
  fakeKillLogPath = join(workDir, 'fake-kill-log.json');
  liveLogFilePath = join(workDir, 'liveness-log.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

function baseEnv(extra = {}) {
  return {
    ARM_QUEUE_FILE: queueFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_LIVENESS_LOG_FILE: liveLogFilePath,
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

// Seeds one queue item and dequeues it — deliberately NEVER calls
// `bindPidToLease` (unlike ./cli-evict.jest.spec.mjs's own pid-lease-mismatch
// scenarios), matching how a real watchdog beat's own claim-ledger resolution
// works today: it discovers `{pid, leaseId}` pairs by zipping ps-tree output
// against the queue's lease records, it never binds a pid onto the lease
// itself.
function seedLeasedItemNeverBound({
  orchestratorId = 'orch-watchdog-exemption',
  agentClass = 'typescript-implementer',
  commandRef = 'resumable-workflow --resume=1316-phase-3',
} = {}) {
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
  return { leaseId: dequeued.leaseId, agentClass, commandRef, orchestratorId };
}

it(
  'watchdog auto-evict via performEviction is unaffected by the new pid check when its own zip-derived ' +
    'entry has no bound pid (documented exemption, not a silent gap)',
  async () => {
    const ORCHESTRATOR_ID = 'orch-watchdog-exemption-outer';
    const { leaseId } = seedLeasedItemNeverBound({ orchestratorId: ORCHESTRATOR_ID });
    await declareDibsWithPid(ORCHESTRATOR_ID, 501);

    // Sanity precondition for this pinning test: the queue entry this beat is
    // about to recycle genuinely has no bound `pid` field on disk — this is
    // exactly the shape a real watchdog beat's own resolution always
    // produces (it never calls bindPidToLease).
    const preBeatQueue = await peekQueue(queueFilePath);
    const preBeatEntry = preBeatQueue.find((item) => item.leaseId === leaseId);
    expect(preBeatEntry).toBeDefined();
    expect(preBeatEntry.pid).toBeUndefined();

    const boundMs = 10 * 60 * 1000;
    const nowMs = Date.now();
    const staleMtimeMs = nowMs - boundMs - 60_000;

    const beatResult = runCli(
      [
        '--watchdog-beat',
        `--orchestrator-id=${ORCHESTRATOR_ID}`,
        '--orchestrator-pid=501',
        `--watchdog-bound-ms=${boundMs}`,
      ],
      baseEnv({
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
        ARM_FAKE_NOW_MS: String(nowMs),
        ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON: JSON.stringify({ '9001': [100, 100, 100, 100, 100] }),
        ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ '9001': staleMtimeMs }),
        ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
        ARM_FAKE_KILL_RESULT: '',
      }),
    );

    expect(beatResult.status).toBe(0);
    const body = JSON.parse(beatResult.stdout);

    // The new pid check (Phase 2) never fires for this entry — it skips,
    // not fails, exactly as it does for any other legacy/never-bound lease.
    // The trip is flagged and genuinely recycled, never refused with
    // `pid-lease-mismatch` or any other pid-related reason.
    const trip = body.trips.find((entry) => entry.pid === 9001);
    expect(trip).toBeDefined();
    expect(trip.reason).toBe('stalled');
    expect(trip.outcome).not.toBe('pid-lease-mismatch');
    expect(body.recycled).toContain(9001);
  },
);
