// Phase 3 — new `--bind-pid=<pid> --lease-id=<leaseId>` CLI flag.
//
// Closes the "does this actually get called in production" gap the
// Convergence Analysis raised on its Build Plan: Phase 1 shipped
// `bindPidToLease` (`lib/queue.mjs`) and Phase 2 shipped the pid-lease
// verification inside `performEviction`, but nothing in `cli.mjs` ever
// called `bindPidToLease` — an orchestrator had no way to actually bind a
// pid to a lease in production. This file mirrors `--ack-lease`/
// `--release-lease`'s own flag-parsing/validation/JSON-output convention
// (`handleAckLease`/`handleReleaseLease`, cli.mjs) — including requiring
// `--orchestrator-id` (the caller's own id, passed through to
// `bindPidToLease` as the ownership-scoping argument).
//
// RED by construction: `cli.mjs` has no `--bind-pid` flag at all today
// (confirmed: `grep -n "bind-pid" cli.mjs` matches nothing). Per this
// skill's own established "unrecognized flag is silently absorbed by
// parseArgs" precedent (see ./cli-dequeue-if-capacity.jest.spec.mjs's
// identical RED-by-construction framing, and ./evict-outer-acceptance.jest
// .spec.mjs's note 2), a `--bind-pid=...` invocation today falls through to
// the nearest recognized beat shape (the default `--desired-agents`/bare
// beat) instead of validating/binding anything — every assertion below
// fails for that reason until Phase 3's production step lands the flag.
//
// Black-box, real-process-spawning idiom — mirrors ./cli-evict.jest.spec.mjs
// / ./cli-dequeue-if-capacity.jest.spec.mjs exactly: never imports cli.mjs
// directly.

import { spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

// Phase 1 — `bindPidToLease`/`peekQueue` are real, shipped
// `lib/queue.mjs` primitives as of this branch. Used here only to seed/
// verify on-disk queue state directly, one layer below `cli.mjs` — mirrors
// ./cli-evict.jest.spec.mjs's own `bindPidToLease` import for the identical
// "lib-level primitive, not cli.mjs itself" rationale.
import { peekQueue } from './lib/queue.mjs';
import { declareDibs } from './lib/coordination-file.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const MEMORY_RED = { pressureLevel: 1, swapUsedMb: 12288, compressedMb: 4096 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

// Orchestrator root PID 501, genuine claude-rooted descendant at PID 9001 —
// mirrors ./cli-evict.jest.spec.mjs's own PS_AUTHORIZED fixture exactly.
// Every PID here is SYNTHETIC — never a real process on the host running
// this test.
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

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-bind-pid-'));
  queueFilePath = join(workDir, 'queue.json');
  coordinationFilePath = join(workDir, 'coordination.json');
  fakeKillLogPath = join(workDir, 'fake-kill-log.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

function baseEnv(extra = {}) {
  return {
    ARM_QUEUE_FILE: queueFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_KILL_LOG: fakeKillLogPath,
    // Every ARM_FAKE_* seam below is a documented no-op unless this master
    // switch is set (cli.mjs's EVICT_FAKE_SEAM_VARS coupling contract) —
    // the mismatch/legitimate-evict scenario below reaches --evict-pid, so
    // this must be present for every call in this file.
    ARM_EVICT_TEST_MODE: '1',
    ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
    ARM_FAKE_KILL_RESULT: '',
    ARM_BEAT_NOW_TEST_MODE: '1',
    ARM_FAKE_NOW_MS: String(Date.now()),
    ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 100 MB',
    ARM_EVICT_GRACE_MS: '5',
    // Deterministic defaults for calls that (today) fall through to the
    // default spawn-decision beat's real `collect()`/ps-tree read (an
    // unrecognized `--bind-pid` today never dispatches into a standalone
    // handler at all — see this file's header note) — without these, a
    // fall-through invocation would shell out to the REAL host's
    // `vm_stat`/`df`/`ps`, which is both non-deterministic and an
    // unnecessary real-system touch for a unit-level CLI test. Overridable
    // per call via `extra` (the mismatch/legitimate-evict scenario below
    // overrides `ARM_FAKE_COLLECT_JSON` to force memory RED).
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
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

/** Seeds one queue item and dequeues it, returning its leaseId — mirrors ./cli-evict.jest.spec.mjs's own `seedLeasedItem`. */
function seedLeasedItem({
  orchestratorId = 'orch-bind-pid',
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

// ---------------------------------------------------------------------------
// Scenario 1 — malformed pid rejected before any write, mirroring
// `--evict-pid`'s own `PID_PATTERN` positive-integer validation.
// ---------------------------------------------------------------------------

it.each([
  ['non-integer', 'abc'],
  ['negative', '-9001'],
  ['zero', '0'],
  ['decimal', '9001.5'],
  ['empty string', ''],
])('new bind-pid flag rejects malformed pid (%s)', (_label, malformedPid) => {
  const { leaseId, orchestratorId } = seedLeasedItem();

  const args = [`--orchestrator-id=${orchestratorId}`, `--lease-id=${leaseId}`];
  args.push(malformedPid === '' ? '--bind-pid=' : `--bind-pid=${malformedPid}`);

  const result = runCli(args, baseEnv());

  // RED today: --bind-pid is not a recognized flag, so this falls through to
  // the default beat and exits 0 instead of rejecting the malformed pid.
  expect(result.status).not.toBe(0);
  expect(result.stderr).toMatch(/--bind-pid/);
});

// ---------------------------------------------------------------------------
// Scenario 2 — missing --lease-id rejected before any write.
// ---------------------------------------------------------------------------

it('new bind-pid flag rejects missing lease-id', () => {
  const { orchestratorId } = seedLeasedItem();

  const result = runCli(
    [`--orchestrator-id=${orchestratorId}`, '--bind-pid=9001'],
    baseEnv(),
  );

  // RED today: falls through to the default beat, which requires no
  // --lease-id at all, so this exits 0 instead of rejecting.
  expect(result.status).not.toBe(0);
  expect(result.stderr).toMatch(/--lease-id/);
});

// ---------------------------------------------------------------------------
// Scenario 3 — missing --orchestrator-id rejected before any write, mirroring
// every other beat-type flag's own required-parameter precedent.
// ---------------------------------------------------------------------------

it('new bind-pid flag rejects missing orchestrator-id', () => {
  const { leaseId } = seedLeasedItem();

  const result = runCli(
    [`--bind-pid=9001`, `--lease-id=${leaseId}`],
    baseEnv(),
  );

  // RED today: falls through to the default beat's OWN --orchestrator-id
  // requirement, whose message never names --bind-pid, so today's exit
  // code/message do not distinguish "missing --orchestrator-id for
  // --bind-pid specifically" from any other beat.
  expect(result.status).not.toBe(0);
  expect(result.stderr).toMatch(/--orchestrator-id/);
  expect(result.stderr).toMatch(/--bind-pid/);
});

// ---------------------------------------------------------------------------
// Scenario 4 — the positive path: calls bindPidToLease with the calling
// --orchestrator-id, exits 0, and the bound pid is genuinely observable on
// disk afterward (not merely a claimed success on stdout).
// ---------------------------------------------------------------------------

it('new bind-pid flag calls bindPidToLease with the calling orchestrator-id and exits 0 on success', async () => {
  const { leaseId, orchestratorId } = seedLeasedItem();

  const result = runCli(
    [`--orchestrator-id=${orchestratorId}`, '--bind-pid=9001', `--lease-id=${leaseId}`],
    baseEnv(),
  );

  // RED today: --bind-pid falls through to the default beat, which prints a
  // traffic-light body, not a bind-result body, and never writes a `pid`
  // onto the queue entry.
  expect(result.status).toBe(0);
  const body = JSON.parse(result.stdout);
  expect(body.bound).toBe(true);

  const entries = await peekQueue(queueFilePath);
  const entry = entries.find((item) => item.leaseId === leaseId);
  expect(entry).toBeDefined();
  expect(entry.pid).toBe(9001);
  expect(entry.orchestratorId).toBe(orchestratorId);
});

// ---------------------------------------------------------------------------
// Scenario 5 — cross-orchestrator bind-pid attempt is a safe no-op (Phase 1's
// ownership-scoped contract: bindPidToLease matches strictly on `leaseId` AND
// `orchestratorId` together), and a subsequent legitimate --evict-pid by the
// TRUE owning orchestrator is completely unaffected by the attempted
// cross-orchestrator bind — it neither poisons the lease with a foreign pid
// nor blocks the real owner's own later bind/evict.
// ---------------------------------------------------------------------------

it('cross-orchestrator bind-pid attempt is a safe no-op, and a subsequent legitimate evict-pid by the true owning orchestrator is unaffected', async () => {
  const TRUE_OWNER = 'orch-bind-pid-true-owner';
  const ATTACKER = 'orch-bind-pid-attacker';
  const { leaseId } = seedLeasedItem({ orchestratorId: TRUE_OWNER });
  declareDibsFor(ATTACKER);

  // The ATTACKER orchestrator does not own this lease — attempts to bind an
  // arbitrary pid onto it anyway.
  const attackerBindResult = runCli(
    [`--orchestrator-id=${ATTACKER}`, '--bind-pid=12345', `--lease-id=${leaseId}`],
    baseEnv(),
  );
  // RED today for a structural reason (no --bind-pid flag exists at all —
  // this falls through to the default beat and exits 0 regardless of
  // ownership), but once the flag lands, a safe no-op must ALSO exit 0 (not
  // an error) — mirroring `ackQueueEntry`/`releaseQueueEntry`'s own
  // "stale/unmatched leaseId is a safe no-op, not an error" convention this
  // ticket's Phase 1 bindPidToLease explicitly reuses.
  expect(attackerBindResult.status).toBe(0);

  const entriesAfterAttack = await peekQueue(queueFilePath);
  const entryAfterAttack = entriesAfterAttack.find((item) => item.leaseId === leaseId);
  expect(entryAfterAttack).toBeDefined();
  // The lease must NOT have been poisoned with the attacker's pid.
  expect(entryAfterAttack.pid).not.toBe(12345);

  // — a genuine PRIOR beat must have recorded the TRUE owner's own pid
  // on the ledger, matching --orchestrator-pid below, or performEviction's
  // fail-closed orchestrator-pid-binding check would reject before this
  // scenario ever reaches the pid-lease-binding machinery under test.
  await declareDibsWithPid(TRUE_OWNER, 501);

  // The TRUE owner now legitimately binds its own real pid.
  const ownerBindResult = runCli(
    [`--orchestrator-id=${TRUE_OWNER}`, '--bind-pid=9001', `--lease-id=${leaseId}`],
    baseEnv(),
  );
  expect(ownerBindResult.status).toBe(0);

  // And a subsequent legitimate --evict-pid, naming the SAME pid it just
  // bound, succeeds — completely unaffected by the earlier attacker attempt.
  const evictResult = runCli(
    [
      `--orchestrator-id=${TRUE_OWNER}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({ ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED, ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }) }),
  );
  expect(evictResult.status).toBe(0);
  const evictBody = JSON.parse(evictResult.stdout);
  expect(evictBody.type).toBe('evict');
});
