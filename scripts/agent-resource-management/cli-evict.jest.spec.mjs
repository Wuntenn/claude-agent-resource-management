// Phase 4 — EVICT wiring + re-enqueue.
//
// Composes Phase 1 (phys_footprint victim ranking, already shipped),
// Phase 2 (`isEvictionAuthorized`, own-spawned-only authorization gate,
// ./lib/recon.mjs, already shipped), Phase 3 (`gracefulStop`, the
// SIGTERM -> grace -> SIGKILL executor, ./lib/graceful-stop.mjs, already
// shipped), and the durable queue lease primitives from
// (./lib/queue.mjs) into a real, opt-in `cli.mjs` beat.
//
// RED by construction: `cli.mjs` has no `--evict-pid` flag at all today
// (confirmed: `grep -n "evict" cli.mjs` matches nothing but unrelated prose
// in an existing atomic-write comment). An invocation carrying
// `--evict-pid=...` today falls all the way through `parseArgs`/`main()`'s
// flag dispatch — exactly like an unrecognized flag on any other beat — and
// runs the default `--desired-agents`/bare beat instead, which never prints
// `{ type: 'evict', ... }` and never touches the queue file. Every
// assertion below fails for that reason until Phase 4 lands.
//
// ---------------------------------------------------------------------------
// DESIGN DECISIONS PINNED BY THIS FILE (read before extending or objecting)
// ---------------------------------------------------------------------------
//
// 1. EXPLICIT, PER-TARGET, OPT-IN ONLY. Per the ticket's resolved design
//    decision (owner interview + Phase 0 outer acceptance test's own
//    precedent, ./evict-outer-acceptance.jest.spec.mjs): a bare RED beat
//    (no extra flags) NEVER evicts anything, no matter how RED memory gets.
//    Eviction only happens when the caller explicitly names a target via
//    `--evict-pid=<pid>`. This file never asserts that omitting
//    `--evict-pid` on a RED beat causes an eviction (that's the *absence*
//    of the feature working correctly — already covered by every existing
//    `pause`-producing test in ./cli.jest.spec.mjs and unaffected by this
//    phase).
//
// 2. CLI FLAG SURFACE — three flags, all required together whenever
//    `--evict-pid` is present:
//
//      --evict-pid=<pid>           candidate OS pid to evaluate for eviction
//      --lease-id=<leaseId>        the lease under which the candidate's
//                                   in-flight work item is currently
//                                   checked out of the durable queue
//      --orchestrator-pid=<pid>    the CALLING orchestrator's own OS pid
//
//    The first two adopt the outer acceptance test's own guessed shape
//    verbatim (its note 2: "a plausible way ... not binding"). The third is
//    THIS file's addition, and is the one genuinely open design question
//    this phase resolves. Rationale: `isEvictionAuthorized` (./lib/
//    recon.mjs, Phase 2) needs a real `orchestratorPid` to walk ps-tree
//    ancestry from. Reading `declareDibs`/`readDibs` (./lib/
//    coordination-file.mjs) end to end shows the dibs ledger entry shape is
//    `{ orchestratorId, desiredAgents, declaredAt, firstDeclaredAt,
//    memoryHistory, diskHistory }` — NO pid field exists anywhere in that
//    schema today (confirmed by reading `declareDibs`'s upsert-by-
//    `orchestratorId` body directly; the only `pid` in the whole file is
//    the *lock* payload's own transient `process.pid`, unrelated to any
//    orchestrator's identity). Retrofitting a pid field onto the dibs
//    schema would be a cross-cutting ledger migration, out of scope for a
//    CLI-wiring phase. The lower-risk option — and the one this file
//    adopts — is for the calling orchestrator to state its own pid
//    explicitly per eviction attempt, exactly as it already states its own
//    `orchestratorId` explicitly on every beat. This is also more honest:
//    `orchestratorPid` is security-sensitive input to an authorization
//    check, and an explicit, opt-in flag is easier to audit than an
//    implicit derivation (e.g. `process.ppid` inside cli.mjs, which would
//    silently be wrong for the extremely common case of a human or test
//    harness invoking cli.mjs directly rather than as a spawned child of
//    the orchestrator process).
//
//    `orchestratorIsLive` (the OTHER input `isEvictionAuthorized` needs) is
//    resolved without any new flag: `handleEvict` calls `declareDibs` for
//    `--orchestrator-id` as its very first step (mirroring the existing
//    default-beat flow's own `declareDibs` call), then treats the calling
//    orchestrator as live iff that orchestratorId is present in the
//    `readDibs` result taken immediately after — i.e. a fresh
//    self-declaration always proves liveness for the beat that just made
//    it, using `readDibs`'s EXISTING staleness-pruning semantics
//    unchanged, not a new liveness concept.
//
// 3. buildTrafficLight VS STANDALONE HANDLER (AC #1) — THIS FILE CHOOSES
//    STANDALONE. `handleEvict` constructs its own `{ type: 'evict', ... }`
//    (and its own `{ type: 'pause'|'hold', evictionAttempted: true,
//    reason }` fallback) directly, WITHOUT routing through
//    `buildTrafficLight` (./lib/allowance.mjs). Rationale:
//      a. Structural safety net for decision 1 above: `buildTrafficLight`
//         is exactly the function the DEFAULT (no extra flags) beat calls
//         on every invocation. Keeping `'evict'` off its reachable-without-
//         explicit-opt-in surface makes "a bare RED beat can never evict"
//         true by construction, not just by convention.
//      b. `buildTrafficLight`'s signature (`memoryState`, `diskState`,
//         `diskReason`, `allowance`, `pauseCandidate`) has no room for
//         per-invocation targeting data (`evictPid`/`leaseId`/
//         `orchestratorPid`) without adding parameters that are meaningless
//         on every OTHER call site of this pure, axis-classification-only
//         function.
//      c. Direct, already-shipped precedent: `handleDequeue`,
//         `handleDequeueIfCapacity`, `handleAckLease`, `handleReleaseLease`
//         are ALL standalone, decision-only beats that already print their
//         own JSON shape without going through `buildTrafficLight`, despite
//         conceptually being "spawn/queue decisions" too (see cli.mjs's own
//         Phase 4 comment ahead of the `--dequeue-if-capacity`
//         dispatch block for this exact framing).
//    FLAGGING FOR REVIEWER: AC #1's literal text says "EVICT is added to
//    buildTrafficLight's discriminated union." The INTENT — a real, backed
//    `'evict'` outcome type instead of a placeholder — is satisfied by the
//    standalone route below (see `type: 'evict'` assertions throughout).
//    The literal mechanism differs from the AC's wording. Reviewer should
//    confirm this interpretation is acceptable before Phase 4's
//    implementer builds against it.
//
// 4. SAFETY — NEVER a real `process.kill` syscall in this file. Every test
//    PID (`9001`, `9002`, `501`, `601`, ...) is SYNTHETIC, matching
//    ./evict-outer-acceptance.jest.spec.mjs's own PID fixture exactly where
//    reused. This file adopts and extends that outer file's own suggested
//    `ARM_FAKE_KILL_LOG` seam (a file path; when set, the real
//    `process.kill` adapter Phase 4 wires into `gracefulStop`'s `kill`
//    callback must append `{ pid, signal }` JSON-lines entries there
//    instead of ever calling real `process.kill`) plus two further seams
//    this file needs and the outer file left undecided:
//      - `ARM_FAKE_KILL_RESULT` — when set to a `gracefulStop`-recognized
//        literal (`'ok' | 'already-gone' | 'permission-denied'`), every
//        injected `kill()` call returns this literal instead of `'ok'`.
//        Models "the process was already gone by the time we tried to
//        signal it" (scenario 5 below) without ever touching a real PID.
//      - `ARM_FAKE_IS_ALIVE_SEQUENCE` — a comma-separated list of
//        `'true'|'false'`, consumed one entry per `isAlive()` call in
//        order (falling back to `'false'` once exhausted). Models
//        "exited before grace" (`false` on the first post-grace check) vs.
//        "still alive, escalate to SIGKILL, confirmed dead after" (`true`
//        then `false`) without a real clock or a real process.
//      - `ARM_EVICT_GRACE_MS` — overrides `gracefulStop`'s default 60s
//        grace period so tests never wait a real 60 seconds. Every test
//        below sets this to a tiny value; production callers (a real
//        orchestrator) would omit it and get the real
//        `DEFAULT_GRACE_PERIOD_MS`.
//    If Phase 4's implementer lands differently-named/shaped seams, update
//    this file's env plumbing to match — the invariant that must survive
//    unchanged is "this file never causes a real kill(2) syscall against a
//    host PID."
//
// 5. RE-ENQUEUE MECHANISM — `releaseQueueEntry(queueFilePath, leaseId)`,
//    NOT `enqueueItem`. Read both functions' JSDoc in ./lib/queue.mjs
//    directly: `enqueueItem` ALWAYS mints a brand-new entry with a
//    server-assigned, current-moment `enqueuedAt` — calling it on eviction
//    would silently reset the item's queue-fairness/aging position and
//    leave the ORIGINAL leased-but-now-orphaned entry sitting in the queue
//    file forever (never cleaned up, since nothing holds its leaseId
//    anymore). `releaseQueueEntry` instead finds the existing on-disk entry
//    whose `leaseId` matches exactly, and atomically clears `leaseId`/
//    `leasedAt` on THAT entry in place — preserving its original
//    `agentClass`/`commandRef`/`orchestratorId`/`enqueuedAt`/priority
//    untouched, and its `leaseId` is already documented as a safe no-op
//    (returns `null`) when the leaseId doesn't match anything on disk. This
//    is exactly the observable contract the outer acceptance test's own
//    Test 2 pins (`remaining[0].leaseId` is `undefined`, every other field
//    intact) and exactly what scenario 4/5 below need.
//
// 6. LEASE-OWNERSHIP CHECK (review, High #1, added after the rest of
//    this file was written). `--evict-pid` and `--lease-id` were originally
//    accepted with no check that the leased queue entry actually belongs to
//    the CALLING `--orchestrator-id`. Since `--peek` is unscoped (design note
//    "handlePeek" in cli.mjs itself — it prints the WHOLE live queue
//    regardless of which `--orchestrator-id` is passed), any orchestrator
//    could see another orchestrator's `leaseId` and pair it with a pid it
//    legitimately owns to release that lease — see scenario 4b below.
//    `handleEvict` now rejects with `reason: 'lease-orchestrator-mismatch'`
//    when `leasedEntry.orchestratorId !== orchestratorId`, checked
//    immediately after the lease lookup and before any authorization/kill
//    work. This does NOT verify that a specific `--evict-pid` and
//    `--lease-id` pairing refers to the SAME in-flight work when both
//    legitimately belong to the SAME orchestrator managing several
//    concurrent agents — see cli.mjs's own comment at this check for why
//    that narrower case is an accepted, tracked limitation rather than
//    fixed here.
//
// Black-box, real-process-spawning idiom — mirrors
// ./cli-dispatcher-drain.jest.spec.mjs / ./cli-dequeue-if-capacity.jest.spec.mjs
// exactly: never imports cli.mjs directly.

import { jest } from '@jest/globals';

import { spawnSync } from 'node:child_process';
import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

// — orchestrator-pid binding tests below seed a dibs entry with a
// SPECIFIC `pid` value directly, bypassing the real ps-tree resolution that
// `declareDibsFor`'s own real-CLI-beat idiom relies on (that idiom never
// resolves a `/claude`-rooted ancestor inside Jest, so every OTHER dibs
// entry this file seeds carries `pid === undefined`). `declareDibs` is a
// lib-level primitive, not `cli.mjs` itself, so importing it directly here
// does not violate this file's "never imports cli.mjs directly" idiom (see
// the header comment above) — it is exactly the seeding mechanism
// `declareDibsFor` itself uses one layer down, just invoked directly instead
// of through a spawned real beat.
import { declareDibs, readDibs } from './lib/coordination-file.mjs';

// Phase 1 — `bindPidToLease` is a real, shipped `lib/queue.mjs`
// primitive as of this branch (see that module's own doc comment). The
// pid-lease-binding scenarios below call it directly, exactly like
// `declareDibsWithPid` already calls `declareDibs` directly one layer below
// `cli.mjs` — this is a lib-level primitive, not `cli.mjs` itself, so
// importing it here does not violate this file's "never imports cli.mjs
// directly" idiom (see the header comment above).
import { bindPidToLease } from './lib/queue.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const MEMORY_RED = { pressureLevel: 1, swapUsedMb: 12288, compressedMb: 4096 };
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

// Orchestrator root PID 501 ("loginwindow" — deliberately NOT itself
// claude-rooted, mirroring a real launching shell/session process), with a
// genuine claude-rooted descendant at PID 9001/9002 — mirrors
// ./evict-outer-acceptance.jest.spec.mjs's PS_ONE_AGENT fixture exactly.
// Every PID here is synthetic (see design note 4 above).
const PS_AUTHORIZED = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001  51200       58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

// Candidate 9001 is a SIBLING branch's descendant (parented under 601, a
// DIFFERENT root than the calling orchestrator's 501) — not reachable from
// orchestratorPid=501 by ancestry, so isEvictionAuthorized must reject it.
const PS_NOT_AUTHORIZED = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
  601     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9001   601 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
`;

// No claude-rooted process anywhere, and no PID 9001 at all.
const PS_NO_AGENT = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
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
  workDir = await mkdtemp(join(tmpdir(), 'arm-evict-'));
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
    // round-4 review — the master `ARM_EVICT_TEST_MODE=1` switch
    // (cli.mjs) must be set before `handleEvict` will honor ANY `ARM_FAKE_*`
    // seam at all; every scenario in this file that reaches `handleEvict`
    // relies on the fake ps-tree/kill/isAlive/now seams below, so this must
    // be present on every call. It is a no-op for the non-evict beats this
    // file's helpers also invoke (declareDibsFor/seedLeasedItem/peekItems)
    // — `ARM_EVICT_TEST_MODE` is read only inside `handleEvict`.
    ARM_EVICT_TEST_MODE: '1',
    // round-3 review, Medium — handleEvict's fake-seam coupling check
    // (cli.mjs) now requires ARM_FAKE_IS_ALIVE_SEQUENCE/ARM_FAKE_KILL_RESULT
    // alongside ARM_FAKE_PS_OUTPUT/ARM_FAKE_KILL_LOG whenever any one of the
    // five is set. Every scenario below that reaches handleEvict sets
    // ARM_FAKE_PS_OUTPUT (directly or via this helper's callers), so these
    // defaults must be present on every such call. All are inert
    // no-ops for scenarios that never reach gracefulStop's isAlive/kill
    // calls (not-authorized / candidate-not-found / lease-not-found /
    // lease-orchestrator-mismatch / no-prior-dibs) and match the existing
    // "first isAlive() call says exited, forced kill result is the
    // ok-otherwise default" behaviour for scenarios that do, unless a
    // scenario overrides them.
    ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
    ARM_FAKE_KILL_RESULT: '',
    // round-4 review — ARM_FAKE_NOW_MS joined the coupled seam set
    // (cli.mjs's EVICT_FAKE_SEAM_VARS) because it reaches
    // isEvictionAuthorized's liveness check via readDibs's pruneStale.
    // Computed fresh per call so it tracks the real clock closely (a few ms
    // of test-harness overhead, well under any TTL this suite cares about)
    // — this is a coupling-satisfying value, not a time-travel seam; no
    // scenario below needs to fake elapsed time.
    ARM_FAKE_NOW_MS: String(Date.now()),
    // Phase 1 — resolveNow() (cli.mjs) has a second, independent
    // master switch alongside ARM_EVICT_TEST_MODE/ARM_COORDINATION_FILE:
    // ARM_FAKE_NOW_MS above is only honoured when this is ALSO exactly '1'.
    // Every scenario in this file that reaches handleEvict relies on that
    // fake-but-effectively-real value above, so this is present by default
    // the same way ARM_EVICT_TEST_MODE is.
    ARM_BEAT_NOW_TEST_MODE: '1',
    // review, Medium — ARM_FAKE_FOOTPRINT_OUTPUT joined the coupled
    // seam set (cli.mjs's EVICT_FAKE_SEAM_VARS) because `buildFallback`'s
    // pause-candidate body now also threads `ARM_EVICT_TEST_MODE` through to
    // its footprint sampling (./lib/pause-candidate.mjs's `honorFakeSeam`).
    // Most scenarios below never reach a `type: 'pause'` fallback body at
    // all, so this default value is never actually consulted by them — it
    // exists purely to satisfy the coupling check, exactly like
    // ARM_FAKE_IS_ALIVE_SEQUENCE/ARM_FAKE_KILL_RESULT above.
    ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 100 MB',
    ARM_EVICT_GRACE_MS: '5',
    ...extra,
  };
}

/**
 * Declares dibs for `orchestratorId` via a normal, real beat — mirroring how
 * a genuine orchestrator establishes liveness in production. `handleEvict`'s
 * `orchestratorIsLive` check (whole-branch review, Medium #3 fix)
 * requires a dibs entry that ALREADY EXISTED before the eviction attempt's
 * own `declareDibs` call, so every scenario below that expects a real
 * eviction to succeed must call this first.
 */
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

/**
 * Seeds one queue item and dequeues it, returning its leaseId (real,
 * already-shipped machinery) — AND declares dibs for `orchestratorId` first
 * (see `declareDibsFor` above), so the returned lease belongs to an
 * orchestrator `handleEvict` can actually authorize against.
 */
function seedLeasedItem({
  orchestratorId = 'orch-evict',
  agentClass = 'typescript-implementer',
  commandRef = 'resumable-workflow --resume=1071-phase-4',
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

/**
 * Seeds (or overwrites) `orchestratorId`'s dibs-ledger entry with a SPECIFIC
 * `pid`, directly via `declareDibs` — bypassing the real ps-tree ancestry
 * walk `declareDibsFor`'s real-CLI-beat idiom depends on (see the import
 * comment above). Models "a genuine PRIOR beat from this orchestrator
 * already recorded its own real root pid on the ledger", which
 * `performEviction`'s new orchestrator-pid-binding check reads via
 * `readDibs` BEFORE this eviction attempt's own `declareDibs` call — i.e.
 * this must run, and complete, strictly before the `runCli` eviction
 * invocation it is set up for.
 *
 * Must run AFTER `seedLeasedItem`/`declareDibsFor` for the same
 * `orchestratorId`, not before: `declareDibs` performs a full-entry REPLACE
 * keyed by `orchestratorId` (./lib/coordination-file.mjs), so an earlier call
 * here would simply be overwritten by `seedLeasedItem`'s own internal
 * `declareDibsFor` call.
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

// ---------------------------------------------------------------------------
// Scenario 1a — authorized candidate, exits cleanly before the grace period
// elapses: SIGTERM sent (via the fake-kill-log seam), no SIGKILL, work item
// re-enqueued with class/command/orchestratorId intact and no leaseId.
// ---------------------------------------------------------------------------

it('evicts an authorized candidate that exits before the grace period, sending only SIGTERM, and re-enqueues its work item', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-clean-exit';
  const { leaseId, agentClass, commandRef } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // — a genuine PRIOR beat must have recorded this orchestrator's own
  // pid on the ledger, matching the --orchestrator-pid supplied below, or the
  // fail-closed binding check would now reject before ever reaching this
  // scenario's own authorization/kill machinery.
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
    }),
  );
  expect(evictResult.status).toBe(0);
  const body = JSON.parse(evictResult.stdout);

  // RED today: no 'evict' literal is ever produced — this beat prints
  // { type: 'pause', pauseCandidate: {...} } unconditionally.
  expect(body.type).toBe('evict');
  expect(body.pid).toBe(9001);
  expect(body.outcome).toBe('exited-before-grace');
  expect(body.signalsSent).toEqual(['SIGTERM']);

  // Never a real syscall: only the fake-kill-log seam observed the signal.
  const killLog = JSON.parse(await readFile(fakeKillLogPath, 'utf8'));
  expect(killLog).toEqual([{ pid: 9001, signal: 'SIGTERM' }]);

  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0]).toMatchObject({ agentClass, commandRef, orchestratorId: ORCHESTRATOR_ID });
  expect(remaining[0].leaseId).toBeUndefined();
});

// ---------------------------------------------------------------------------
// Scenario 1b — authorized candidate, still alive after the grace period:
// escalates to SIGKILL, confirms death, then re-enqueues.
// ---------------------------------------------------------------------------

it('escalates to SIGKILL when the authorized candidate is still alive after the grace period, then re-enqueues its work item', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-sigkill';
  const { leaseId, agentClass, commandRef } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // — see the clean-exit scenario's comment above.
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      // First isAlive() check (post-SIGTERM-grace) says still alive ->
      // escalate; second (post-SIGKILL) says confirmed dead.
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'true,false',
    }),
  );
  expect(evictResult.status).toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).toBe('evict');
  expect(body.outcome).toBe('killed-after-grace');
  expect(body.signalsSent).toEqual(['SIGTERM', 'SIGKILL']);

  const killLog = JSON.parse(await readFile(fakeKillLogPath, 'utf8'));
  expect(killLog).toEqual([
    { pid: 9001, signal: 'SIGTERM' },
    { pid: 9001, signal: 'SIGKILL' },
  ]);

  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0]).toMatchObject({ agentClass, commandRef, orchestratorId: ORCHESTRATOR_ID });
  expect(remaining[0].leaseId).toBeUndefined();
});

// ---------------------------------------------------------------------------
// Scenario 2 — NOT authorized (candidate is not own-spawned: unreachable by
// ancestry from the declared --orchestrator-pid). Must never fabricate an
// eviction, must fall through to the honest pause/hold beat, and must say
// SO OBSERVABLY (AC #6, "no silent masking") rather than printing a plain
// pause result indistinguishable from "nothing was ever attempted".
// ---------------------------------------------------------------------------

it('falls through to honest pause/hold — never fabricating an eviction — when the candidate is not own-spawned', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-not-authorized';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // — this scenario exercises isEvictionAuthorized's own ancestry
  // refusal, not the pid-binding gate; seed a matching recorded pid so the
  // fail-closed binding check passes and the beat actually reaches ancestry
  // authorization instead of being rejected earlier for an unrelated reason.
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_NOT_AUTHORIZED,
    }),
  );
  // review, Medium — a non-`'evict'`-success outcome must exit
  // non-zero (this is CONFIRMED STILL RUNNING territory, not a success),
  // per AC #6's "no silent masking" principle extended to the exit code.
  expect(evictResult.status).not.toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).not.toBe('evict');
  expect(['pause', 'hold']).toContain(body.type);
  // AC #6 — the beat must say, observably, that an eviction was attempted
  // and specifically why it did not proceed, not silently print a bare
  // pause/hold indistinguishable from a beat that never saw --evict-pid.
  expect(body.evictionAttempted).toBe(true);
  expect(body.reason).toBe('not-authorized');

  // No kill signal of any kind was ever sent.
  await expect(readFile(fakeKillLogPath, 'utf8')).rejects.toThrow();

  // The work item was never touched — still leased exactly as it was.
  // peekQueue does not filter by lease status (see
  // cli-dequeue-if-capacity.jest.spec.mjs), so the still-leased item is
  // visible here — its presence, not its absence, is what proves it was
  // never re-enqueued/lost.
  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
});

// ---------------------------------------------------------------------------
// Scenario 3 — no running agent/candidate exists at all (the named
// --evict-pid is not present in the ps-tree snapshot). Same "never
// fabricate, always say why" contract as scenario 2, with a distinct
// `reason` so the two failure modes are told apart.
// ---------------------------------------------------------------------------

it('falls through to honest pause/hold when no running agent matching --evict-pid exists', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-idle';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // — see the "not own-spawned" scenario's comment above: this
  // scenario exercises isEvictionAuthorized's own candidate-lookup refusal,
  // not the pid-binding gate.
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_NO_AGENT,
    }),
  );
  // review, Medium — see the not-authorized scenario's comment above:
  // every non-`'evict'`-success outcome now exits non-zero uniformly.
  expect(evictResult.status).not.toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).not.toBe('evict');
  expect(['pause', 'hold']).toContain(body.type);
  expect(body.evictionAttempted).toBe(true);
  expect(body.reason).toBe('candidate-not-found');

  await expect(readFile(fakeKillLogPath, 'utf8')).rejects.toThrow();

  // Still leased, still visible via --peek (see scenario 2's comment).
  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
});

// ---------------------------------------------------------------------------
// Scenario 4 — an authorized, present candidate, but --lease-id does not
// correspond to any actual queued/leased item (nothing to re-enqueue).
// Chosen behaviour (documented, per design note above): fall through to the
// honest pause/hold beat with reason 'lease-not-found', and — critically —
// never send any kill signal, since there would be nothing to give back to
// the queue if eviction proceeded anyway.
// ---------------------------------------------------------------------------

it('falls through to honest pause/hold, without sending any signal, when --lease-id matches no queued item', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-bad-lease';
  // Seed a real item so the queue is non-empty, but never dequeue it — the
  // lease id below is a plausible-looking string that was never minted by
  // claimQueueEntry/claimAndAdmitQueueEntry.
  const enqueueResult = runCli(
    [`--enqueue=typescript-implementer:normal:resumable-workflow`, `--orchestrator-id=${ORCHESTRATOR_ID}`],
    baseEnv({ ARM_FAKE_COLLECT_JSON: undefined, ARM_FAKE_PS_OUTPUT: undefined }),
  );
  expect(enqueueResult.status).toBe(0);

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      '--lease-id=lease-that-was-never-issued',
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
    }),
  );
  // review, Medium — see scenario 2's comment above: every
  // non-`'evict'`-success outcome now exits non-zero uniformly.
  expect(evictResult.status).not.toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).not.toBe('evict');
  expect(['pause', 'hold']).toContain(body.type);
  expect(body.evictionAttempted).toBe(true);
  expect(body.reason).toBe('lease-not-found');

  await expect(readFile(fakeKillLogPath, 'utf8')).rejects.toThrow();

  // The originally-seeded (still-unleased) item is untouched — exactly one
  // item, never duplicated, never lost.
  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBeUndefined();
});

// ---------------------------------------------------------------------------
// Scenario 4b (review, High #1) — --lease-id resolves to a real,
// currently-leased item, but that item's orchestratorId does NOT match the
// calling --orchestrator-id: queue peeks are not scoped to a single
// orchestrator, so orchestrator A could otherwise see orchestrator B's
// leaseId and pair it with a pid A legitimately owns (per PS_AUTHORIZED,
// authorized under orchestratorPid=501) to release B's still-in-progress
// item — even though A has no legitimate relationship to that lease. This
// must be rejected BEFORE any authorization/kill work happens, with an
// honest, distinguishable reason, and B's lease must be left completely
// untouched.
// ---------------------------------------------------------------------------

it('rejects eviction when --lease-id belongs to a different orchestrator than --orchestrator-id, even with an otherwise-authorized pid, and leaves that lease untouched', async () => {
  const ORCHESTRATOR_B = 'orch-evict-lease-owner';
  const { leaseId, agentClass, commandRef } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_B });

  const ORCHESTRATOR_A = 'orch-evict-different-caller';
  declareDibsFor(ORCHESTRATOR_A);

  const evictResult = runCli(
    [`--orchestrator-id=${ORCHESTRATOR_A}`, '--orchestrator-pid=501', '--evict-pid=9001', `--lease-id=${leaseId}`],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
    }),
  );
  // review, Medium — see scenario 2's comment above: every
  // non-`'evict'`-success outcome now exits non-zero uniformly.
  expect(evictResult.status).not.toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).not.toBe('evict');
  expect(['pause', 'hold']).toContain(body.type);
  expect(body.evictionAttempted).toBe(true);
  expect(body.reason).toBe('lease-orchestrator-mismatch');

  // Never touches the process — this must fail before any kill is attempted.
  await expect(readFile(fakeKillLogPath, 'utf8')).rejects.toThrow();

  // B's lease is completely untouched: still present, still leased, with its
  // original class/command/orchestratorId intact.
  const remaining = peekItems(ORCHESTRATOR_B);
  expect(remaining).toHaveLength(1);
  expect(remaining[0]).toMatchObject({ agentClass, commandRef, orchestratorId: ORCHESTRATOR_B, leaseId });
});

// ---------------------------------------------------------------------------
// Scenario 5 (Convergence Analysis edge case) — the target was ALREADY dead
// by the time eviction was attempted (the injected `kill` reports
// 'already-gone' on the very first SIGTERM). This must NOT be treated as a
// failure: the work item must still be re-enqueued — it must never be lost
// just because the process had already exited before this beat ran.
// ---------------------------------------------------------------------------

it('still re-enqueues the work item when the candidate was already dead at eviction time', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-already-gone';
  const { leaseId, agentClass, commandRef } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // — see the clean-exit scenario's comment above.
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      ARM_FAKE_KILL_RESULT: 'already-gone',
    }),
  );
  expect(evictResult.status).toBe(0);
  const body = JSON.parse(evictResult.stdout);

  // This IS a successful eviction outcome, not a fallback — 'already-gone'
  // is one of gracefulStop's own documented terminal outcomes, not an
  // error condition (see ./lib/graceful-stop.mjs's own @returns union).
  expect(body.type).toBe('evict');
  expect(body.outcome).toBe('already-gone');
  expect(body.signalsSent).toEqual(['SIGTERM']);

  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0]).toMatchObject({ agentClass, commandRef, orchestratorId: ORCHESTRATOR_ID });
  expect(remaining[0].leaseId).toBeUndefined();
});

// ---------------------------------------------------------------------------
// Scenario 6 (AC #6, "no silent masking" — standalone-flag conflict check).
// `--evict-pid` must follow the EXACT existing conflict-check pattern every
// other standalone beat flag uses (see cli.mjs's `--dequeue`/
// `--dequeue-if-capacity` blocks): combined with another standalone
// beat-type flag in the same invocation, it must exit 2 with a clear
// stderr message naming both flags, not silently dispatch into the OTHER
// flag's handler as if --evict-pid weren't present.
// ---------------------------------------------------------------------------

it('rejects --evict-pid combined with another standalone beat-type flag (exit 2, not a silent dispatch)', () => {
  const ORCHESTRATOR_ID = 'orch-evict-conflict';

  const result = runCli(
    [
      '--evict-pid=9001',
      '--lease-id=some-lease',
      '--orchestrator-pid=501',
      '--peek',
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
    ],
    baseEnv({ ARM_FAKE_COLLECT_JSON: undefined, ARM_FAKE_PS_OUTPUT: undefined }),
  );

  // RED today for a DIFFERENT reason than the other scenarios: --evict-pid
  // is unrecognized, so this invocation is dispatched as a plain --peek
  // beat and exits 0 with a peek-shaped body — not the exit-2 conflict
  // rejection this test requires.
  expect(result.status).toBe(2);
  expect(result.stderr).toMatch(/--evict-pid/);
  expect(result.stderr).toMatch(/--peek/);
});

// ---------------------------------------------------------------------------
// Scenario 6b (review, Low) — `--poll-footprint` was missing from
// `--evict-pid`'s own conflict-flag list above, so this exact combination
// silently dispatched into `handleEvict` and ignored `--poll-footprint`
// entirely, contradicting the block's own "full bidirectional coverage"
// comment. Must reject with exit 2, naming both flags, like every other
// standalone-beat-type pairing in scenario 6 above.
// ---------------------------------------------------------------------------

it('rejects --evict-pid combined with --poll-footprint (exit 2, not a silent dispatch)', () => {
  const ORCHESTRATOR_ID = 'orch-evict-poll-footprint-conflict';

  const result = runCli(
    [
      '--evict-pid=9001',
      '--lease-id=some-lease',
      '--orchestrator-pid=501',
      '--poll-footprint=9001',
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
    ],
    baseEnv({ ARM_FAKE_COLLECT_JSON: undefined, ARM_FAKE_PS_OUTPUT: undefined }),
  );

  expect(result.status).toBe(2);
  expect(result.stderr).toMatch(/--evict-pid/);
  expect(result.stderr).toMatch(/--poll-footprint/);
});

// ---------------------------------------------------------------------------
// Scenario 6c (Phase 3 review, Medium) — `--bind-pid` was missing from
// `--watchdog-beat`'s own conflict-flag list (`WATCHDOG_CONFLICTING_BEAT_FLAGS`,
// cli.mjs's `main()`), and that block is checked FIRST in `main()` — ahead of
// both `--evict-pid`'s and `--bind-pid`'s own blocks. Before this fix,
// `--watchdog-beat --bind-pid=<pid> --lease-id=<id> --orchestrator-id=<id>`
// silently fell through to `handleWatchdogBeat`, discarding `--bind-pid`/
// `--lease-id` entirely instead of being rejected as a flag conflict — the
// exact same class of gap `--poll-footprint`'s own belated addition to
// `EVICT_CONFLICTING_BEAT_FLAGS` (Scenario 6b, above) was. Must reject with
// exit 2, naming both flags, like every other standalone-beat-type pairing.
// ---------------------------------------------------------------------------

it('rejects --watchdog-beat combined with --bind-pid (exit 2, not a silent dispatch)', () => {
  const ORCHESTRATOR_ID = 'orch-watchdog-bind-pid-conflict';

  const result = runCli(
    ['--watchdog-beat', '--bind-pid=9001', '--lease-id=some-lease', `--orchestrator-id=${ORCHESTRATOR_ID}`],
    baseEnv({ ARM_FAKE_COLLECT_JSON: undefined, ARM_FAKE_PS_OUTPUT: undefined }),
  );

  expect(result.status).toBe(2);
  expect(result.stderr).toMatch(/--watchdog-beat/);
  expect(result.stderr).toMatch(/--bind-pid/);
});

// ---------------------------------------------------------------------------
// Scenario 7 (post-review fix — TOCTOU, High) — the ps-tree sample fed into
// `isEvictionAuthorized` must be taken AFTER all the awaited queue/dibs I/O
// (`peekQueue`, coordination-dir setup, `readDibs`, `declareDibs`,
// `readDibs` again), not once up front before any of it. `isEvictionAuthorized`'s
// own contract (./lib/recon.mjs) requires a snapshot taken immediately
// before use to be safe against PID-reuse races: an early sample can go
// stale during the awaited I/O window, during which the real target pid
// could exit and an unrelated process reuse the same pid, causing a
// SUBSEQUENT eviction request to be wrongly authorized against a snapshot
// that no longer reflects reality.
//
// This project's existing `ARM_FAKE_PS_OUTPUT` seam returns one static
// string for every `collectPsOutput()` call in a run — by itself it cannot
// distinguish "sampled once, early" from "sampled once, late", because both
// the pre-fix and post-fix code call `collectPsOutput()` exactly once on
// the authorized-candidate path (the bug was WHEN that single call happened
// relative to the I/O, not how many times it happened). Simulating the
// actual race black-box would require the fake ps output to change value
// mid-invocation in lockstep with the real `readDibs`/`declareDibs` file
// I/O inside the same synchronous child-process run — not achievable
// without adding production-only instrumentation whose sole purpose would
// be to make this one test possible.
//
// Per this ticket's own guidance, falling back to a structural regression
// test on the source itself: assert textually, in cli.mjs's actual
// `handleEvict` body, that the `collectPsOutput()` call which feeds
// `isEvictionAuthorized` appears AFTER the last `readDibs` call (the final
// I/O step before authorization) and after the `peekQueue` lease lookup —
// not before either. This directly encodes the fix and fails immediately
// if a future edit reintroduces an early, reused sample.
// ---------------------------------------------------------------------------

it('samples ps-tree for isEvictionAuthorized only after the queue/dibs I/O completes (source-order regression)', async () => {
  const source = await readFile(CLI, 'utf8');

  const fnStart = source.indexOf('async function handleEvict(flags)');
  expect(fnStart).toBeGreaterThan(-1);
  const fnEnd = source.indexOf('\n// ---', fnStart);
  expect(fnEnd).toBeGreaterThan(fnStart);
  const body = source.slice(fnStart, fnEnd);

  const peekQueueCallIndex = body.indexOf('await peekQueue(queueFilePath, now)');
  const declareDibsCallIndex = body.indexOf('await declareDibs(');
  // The LAST readDibs call before authorization — declareDibs is followed
  // immediately by a fresh readDibs in the success path.
  const lastReadDibsCallIndex = body.lastIndexOf('await readDibs(coordinationFilePath, now,');
  const authorizedCallIndex = body.indexOf('isEvictionAuthorized({');
  // The `psTreeOutput` sample that actually feeds `isEvictionAuthorized` —
  // matched specifically (not `buildFallback`'s independent, separately
  // fresh-sampled `collectPsOutput()` calls) via the `const psTreeOutput =`
  // assignment that immediately precedes the authorization call.
  const freshSampleIndex = body.lastIndexOf(
    'const psTreeOutput = collectPsOutput(evictTestModeEnabled);',
    authorizedCallIndex,
  );

  expect(peekQueueCallIndex).toBeGreaterThan(-1);
  expect(declareDibsCallIndex).toBeGreaterThan(-1);
  expect(lastReadDibsCallIndex).toBeGreaterThan(-1);
  expect(authorizedCallIndex).toBeGreaterThan(-1);
  expect(freshSampleIndex).toBeGreaterThan(-1);

  // The fresh sample must sit strictly between the last I/O call and the
  // authorization call it feeds — never before the queue lookup or the
  // dibs declare/read sequence.
  expect(freshSampleIndex).toBeGreaterThan(peekQueueCallIndex);
  expect(freshSampleIndex).toBeGreaterThan(declareDibsCallIndex);
  expect(freshSampleIndex).toBeGreaterThan(lastReadDibsCallIndex);
  expect(freshSampleIndex).toBeLessThan(authorizedCallIndex);
});

// ---------------------------------------------------------------------------
// Scenario 7b (final review — gap found in re-review, High) — the
// SECOND TOCTOU guard, `gracefulStop`'s `verifyStillTarget` callback (see
// ./lib/graceful-stop.mjs's own doc comment and Scenario 15 in
// lib/graceful-stop.jest.spec.mjs for the unit-level proof that
// `verifyStillTarget` resolving `false` correctly aborts before SIGKILL),
// had NO regression coverage at all at the `cli.mjs` wiring level — despite
// Scenario 7 immediately above establishing, for the FIRST TOCTOU guard, that
// a structural source-order regression test is exactly this project's
// existing fallback when a black-box behavioral test is infeasible (the same
// `ARM_FAKE_PS_OUTPUT` "one static string per run" limitation applies here
// verbatim: a black-box test cannot make the ps-tree snapshot change value
// mid-invocation between the pre-`gracefulStop` authorization check and the
// `verifyStillTarget` call `gracefulStop` makes after the grace-period sleep,
// without production-only instrumentation whose sole purpose would be to
// make this one test possible).
//
// Without this, a future edit that dropped the `verifyStillTarget` option
// from the `gracefulStop({...})` call entirely, or that reused the stale
// `psTreeOutput` const (captured before the grace-period wait) instead of
// calling `collectPsOutput(evictTestModeEnabled)` fresh inside the closure,
// would silently regress this beat back to the exact PID-reuse-across-the-
// grace-window vulnerability the `verifyStillTarget` fix (review, High)
// closed — while every existing test in this file (which all use the same
// static `ARM_FAKE_PS_OUTPUT` value for the whole invocation, so a stale vs.
// fresh sample reads identically either way) would keep passing.
// ---------------------------------------------------------------------------

it('wires a fresh-sampling verifyStillTarget into gracefulStop, not the stale pre-grace-period ps sample (source-order regression)', async () => {
  const source = await readFile(CLI, 'utf8');

  const fnStart = source.indexOf('async function handleEvict(flags)');
  expect(fnStart).toBeGreaterThan(-1);
  const fnEnd = source.indexOf('\n// ---', fnStart);
  expect(fnEnd).toBeGreaterThan(fnStart);
  const body = source.slice(fnStart, fnEnd);

  const gracefulStopCallIndex = body.indexOf('await gracefulStop({');
  expect(gracefulStopCallIndex).toBeGreaterThan(-1);

  // The gracefulStop(...) call's own argument object — bounded to just this
  // call site, not the whole rest of the function, so the assertions below
  // can only pass if `verifyStillTarget` is actually an option threaded into
  // THIS call, not merely present somewhere else in the function body.
  const gracefulStopCallEnd = body.indexOf('\n  } catch (error) {\n    stopError = error;', gracefulStopCallIndex);
  expect(gracefulStopCallEnd).toBeGreaterThan(gracefulStopCallIndex);
  const gracefulStopCallArgs = body.slice(gracefulStopCallIndex, gracefulStopCallEnd);

  expect(gracefulStopCallArgs).toMatch(/verifyStillTarget:\s*\(\)\s*=>/);
  // Must call collectPsOutput() fresh INSIDE the callback — never reuse the
  // outer `psTreeOutput` const (the snapshot taken once, before the
  // grace-period sleep, that Scenario 7's own fix exists to keep fresh only
  // up to the point it was taken, not across an arbitrarily long await).
  expect(gracefulStopCallArgs).toMatch(/psTreeOutput:\s*collectPsOutput\(evictTestModeEnabled\)/);
  expect(gracefulStopCallArgs).not.toMatch(/verifyStillTarget:\s*\(\)\s*=>\s*isEvictionAuthorized\(\{\s*psTreeOutput,/);
  // Re-runs the SAME authorization decision the pre-gracefulStop check uses,
  // over the freshly-sampled snapshot — not a different/weaker check.
  expect(gracefulStopCallArgs).toMatch(/isEvictionAuthorized\(\{/);
  expect(gracefulStopCallArgs).toMatch(/orchestratorPid,/);
  expect(gracefulStopCallArgs).toMatch(/orchestratorIsLive,/);
  expect(gracefulStopCallArgs).toMatch(/candidatePid:\s*evictPidRaw,/);
});

// ---------------------------------------------------------------------------
// Scenario 8 (post-review fix — Medium, review superseded the original
// fix) — `gracefulStop` deliberately throws (via `assertKnownKillResult`,
// ./lib/graceful-stop.mjs) on an unrecognized `kill()` result — "fail loud,
// not open" per Phase 3's own design. That must never CRASH this beat to
// `main().catch()`'s generic `exit(1)` (which would give zero
// `evictionAttempted`/`reason` observability): the CLI must still print an
// honest JSON result.
//
// review, Medium — the ORIGINAL fix for this scenario unconditionally
// released the lease on any throw, reasoning "the situation is unknown, and
// holding forever is worse than an honest unknown." That is inconsistent
// with `permission-denied`/`unconfirmed-after-kill` (scenarios 9/10 below),
// where an equally "not confirmed stopped" outcome is deliberately NEVER
// released, for double-execution safety. In practice the realistic way
// `gracefulStop` throws in production is `realEvictKill` propagating an
// unmapped `process.kill` error while sending SIGTERM — i.e. SIGTERM was
// never confirmed delivered, so the target must be presumed STILL RUNNING,
// exactly like `permission-denied`. This test now pins the corrected,
// consistent behaviour: the lease is NOT released, and the queue entry is
// left exactly as it was, still leased.
//
// `ARM_FAKE_KILL_RESULT` set to an UNRECOGNIZED literal (anything outside
// `'ok' | 'already-gone' | 'permission-denied'`) forces the injected
// `kill()` to return that literal, which `assertKnownKillResult` rejects —
// exactly the throw path this test exercises.
// ---------------------------------------------------------------------------

it('never releases the leased work item, and reports an honest failure reason, when gracefulStop throws on an unrecognized kill() result', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-graceful-stop-throws';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // — see the clean-exit scenario's comment above.
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      // Unrecognized by assertKnownKillResult -> gracefulStop throws.
      ARM_FAKE_KILL_RESULT: 'ESOMETHING',
    }),
  );

  // Must NOT crash to the generic top-level exit(1) via an uncaught throw —
  // the thrown error is caught and reported as a well-formed JSON body on a
  // clean CLI exit, not a stack trace. It is NOT a success, though: the
  // target's state is unconfirmed, so (review, Medium) the exit code
  // must be non-zero, exactly like every other non-`'evict'`-success outcome.
  expect(evictResult.status).not.toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).toBe('evict');
  expect(body.pid).toBe(9001);
  expect(body.evictionAttempted).toBe(true);
  expect(body.outcome).toBe('graceful-stop-failed');
  expect(body.reason).toBe('graceful-stop-failed');
  expect(typeof body.error).toBe('string');
  expect(body.error.length).toBeGreaterThan(0);

  // The critical assertion: the target's state is unconfirmed (the throw
  // happened before any confirmed-stopped outcome), so the lease must NOT be
  // released — never risking a second dispatch of the same work while the
  // original may still be running.
  expect(body.released).toBe(false);
  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

// ---------------------------------------------------------------------------
// Scenario 9 (post-review fix — High #1, double-execution risk) — when
// `gracefulStop` reports `'permission-denied'` (SIGTERM was refused —
// never even delivered, the target is CONFIRMED STILL RUNNING), the lease
// must NEVER be released: releasing it would let a dispatcher immediately
// re-dequeue and re-dispatch the SAME work to a second agent while the
// original may still be executing it.
// ---------------------------------------------------------------------------

it("never releases the lease when gracefulStop reports 'permission-denied' (target confirmed still running)", async () => {
  const ORCHESTRATOR_ID = 'orch-evict-permission-denied';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // — see the clean-exit scenario's comment above.
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      ARM_FAKE_KILL_RESULT: 'permission-denied',
    }),
  );
  // review, Medium — the target is CONFIRMED STILL RUNNING here: the
  // operationally worst outcome this beat can report. Must exit non-zero,
  // not 0 — previously this outcome looked identical to success to any
  // caller gating on exit code alone.
  expect(evictResult.status).not.toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).toBe('evict');
  expect(body.pid).toBe(9001);
  expect(body.evictionAttempted).toBe(true);
  expect(body.outcome).toBe('permission-denied');
  // round-2 review, Nit — no separate `reason` field on this path:
  // `outcome` already tells the same story verbatim (see cli.mjs's own
  // comment at this fallback body).
  expect(body.reason).toBeUndefined();
  expect(body.released).toBe(false);

  // The critical assertion: the item is STILL leased, exactly as before —
  // not released back to the pool where a second dispatch could race it.
  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

// ---------------------------------------------------------------------------
// Scenario 10 (post-review fix — High #1, double-execution risk) — when
// `gracefulStop` reports `'unconfirmed-after-kill'` (SIGKILL sent, but
// `isAlive` still reports true afterward — the target is CONFIRMED STILL
// RUNNING), the lease must equally never be released, for the same
// double-execution reason as scenario 9.
// ---------------------------------------------------------------------------

it("never releases the lease when gracefulStop reports 'unconfirmed-after-kill' (target confirmed still running)", async () => {
  const ORCHESTRATOR_ID = 'orch-evict-unconfirmed-after-kill';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // — see the clean-exit scenario's comment above.
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      // Still alive after the grace period -> escalate to SIGKILL; still
      // alive again after SIGKILL -> 'unconfirmed-after-kill'.
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'true,true',
    }),
  );
  // review, Medium — same "confirmed still running -> non-zero exit"
  // fix as the permission-denied scenario above.
  expect(evictResult.status).not.toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).toBe('evict');
  expect(body.evictionAttempted).toBe(true);
  expect(body.outcome).toBe('unconfirmed-after-kill');
  expect(body.signalsSent).toEqual(['SIGTERM', 'SIGKILL']);
  // round-2 review, Nit — see the same note on the permission-denied
  // scenario above: `outcome` already tells the same story as the old
  // `reason`.
  expect(body.reason).toBeUndefined();
  expect(body.released).toBe(false);

  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

// ---------------------------------------------------------------------------
// Scenario 11 (post-review fix — Medium #3, orchestratorIsLive no-op) — an
// orchestrator with NO prior dibs entry must be rejected (fail closed), even
// though it is an otherwise-perfectly-authorized candidate. Contrast this
// against every eviction-succeeds scenario above (1a/1b/5/9/10), all of
// which now go through `seedLeasedItem`'s `declareDibsFor` first — proving
// the distinction is real, not incidental.
// ---------------------------------------------------------------------------

it('rejects --evict-pid from an orchestrator with NO prior dibs entry, even though the candidate is otherwise authorized (fails closed)', () => {
  const ORCHESTRATOR_ID = 'orch-evict-no-prior-dibs';

  // Seed the queue item directly (bypassing seedLeasedItem's declareDibsFor
  // call) so this orchestrator genuinely has no dibs entry at all before the
  // eviction attempt below.
  const enqueueResult = runCli(
    ['--enqueue=typescript-implementer:normal:resumable-workflow', `--orchestrator-id=${ORCHESTRATOR_ID}`],
    baseEnv({ ARM_FAKE_COLLECT_JSON: undefined, ARM_FAKE_PS_OUTPUT: undefined }),
  );
  expect(enqueueResult.status).toBe(0);
  const dequeueResult = runCli(
    ['--dequeue-if-capacity', `--orchestrator-id=${ORCHESTRATOR_ID}`],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: undefined,
    }),
  );
  expect(dequeueResult.status).toBe(0);
  const leaseId = JSON.parse(dequeueResult.stdout).leaseId;

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
    }),
  );
  // review, Medium — see scenario 2's comment: every non-`'evict'`-
  // success outcome now exits non-zero uniformly.
  expect(evictResult.status).not.toBe(0);
  const body = JSON.parse(evictResult.stdout);

  // Never a real eviction — this orchestrator has never made a real prior
  // beat, so it cannot be proven live, regardless of ancestry.
  expect(body.type).not.toBe('evict');
  expect(['pause', 'hold']).toContain(body.type);
  expect(body.evictionAttempted).toBe(true);
  expect(body.reason).toBe('not-authorized');

  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

// ---------------------------------------------------------------------------
// Scenario 12 (post-review fix — Medium #4) — a missing `--orchestrator-pid`
// must hard-reject with exit 2 and a clear stderr message, mirroring
// `--evict-pid`'s own existing validation, rather than silently coercing to
// `"undefined"` internally and surfacing only as a generic 'not-authorized'.
// ---------------------------------------------------------------------------

it('rejects a missing --orchestrator-pid with exit 2 and a clear stderr message', () => {
  const ORCHESTRATOR_ID = 'orch-evict-missing-orchestrator-pid';

  const result = runCli(
    [`--orchestrator-id=${ORCHESTRATOR_ID}`, '--evict-pid=9001', '--lease-id=some-lease'],
    baseEnv({ ARM_FAKE_COLLECT_JSON: undefined, ARM_FAKE_PS_OUTPUT: undefined }),
  );

  expect(result.status).toBe(2);
  expect(result.stderr).toMatch(/--orchestrator-pid/);
});

// ---------------------------------------------------------------------------
// Scenario 13 (post-review fix — Low #8) — when `releaseQueueEntry` itself
// throws on the confirmed-stopped success path, this must be OBSERVABLE: a
// non-zero exit code and an explicit `reason: 'release-failed'`, not a
// silent exit 0 with only `released: false` to go on.
// ---------------------------------------------------------------------------

it("exits non-zero with reason: 'release-failed' when releaseQueueEntry itself throws after a confirmed-stopped outcome", async () => {
  const ORCHESTRATOR_ID = 'orch-evict-release-fails';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // — see the clean-exit scenario's comment above.
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  // The fake-kill-log seam must still be WRITABLE after workDir goes
  // read-only below (buildEvictKillFn's `appendFakeKillLog` needs to create
  // this file the first time `kill()` is called, mid-`gracefulStop`) — move
  // it to a sibling directory outside `workDir` for this test only, so
  // read-only-workDir affects ONLY the queue/coordination files, not this
  // unrelated test seam.
  const outsideDir = await mkdtemp(join(tmpdir(), 'arm-evict-outside-'));
  const outsideKillLogPath = join(outsideDir, 'fake-kill-log.json');

  // Make the queue's directory read-only (not the queue file itself —
  // `writeEntriesAtomic`'s rename-over-existing-file doesn't require write
  // access to the TARGET file on POSIX, only to the directory) so both the
  // release's fencing-lock acquisition and its staged-temp-file write fail
  // with EACCES, AFTER gracefulStop has already succeeded and reported the
  // target confirmed stopped. Reads (peekQueue, the initial lease lookup)
  // still work fine — only creating new directory entries is blocked.
  await chmod(workDir, 0o555);

  try {
    const evictResult = runCli(
      [
        `--orchestrator-id=${ORCHESTRATOR_ID}`,
        '--orchestrator-pid=501',
        '--evict-pid=9001',
        `--lease-id=${leaseId}`,
      ],
      baseEnv({
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
        ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
        ARM_FAKE_KILL_LOG: outsideKillLogPath,
      }),
    );

    expect(evictResult.status).not.toBe(0);
    const body = JSON.parse(evictResult.stdout);
    expect(body.type).toBe('evict');
    expect(body.outcome).toBe('exited-before-grace');
    expect(body.released).toBe(false);
    expect(body.reason).toBe('release-failed');
  } finally {
    // Restore write permission so afterEach's rm(workDir) can clean up.
    await chmod(workDir, 0o755);
    await rm(outsideDir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// Scenario 13b (review round 4, High) — the fake-seam coupling check's
// notion of "set" must match what each seam's own consumer treats as "fake
// path selected". `ARM_FAKE_KILL_LOG=''` alongside every other seam set is
// caught here as PARTIALLY faked (exit 2, refused) rather than silently
// passing the coupling check while `buildEvictKillFn` actually falls back to
// `realEvictKill` — see cli.mjs's `isEvictFakeSeamSet` for the full
// rationale. Before the fix, this combination passed the coupling check
// silently and would have gone on to authorize a real SIGTERM/SIGKILL from
// fabricated ps-tree ancestry.
// ---------------------------------------------------------------------------

it("refuses a fully-'set' seam combination as partially-faked when ARM_FAKE_KILL_LOG is an empty string — empty string is NOT 'set' for the same reason buildEvictKillFn treats it as unset", () => {
  const ORCHESTRATOR_ID = 'orch-evict-empty-kill-log';

  const result = runCli(
    ['--evict-pid=9001', '--lease-id=some-lease', '--orchestrator-pid=501', `--orchestrator-id=${ORCHESTRATOR_ID}`],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      ARM_FAKE_KILL_LOG: '',
      ARM_FAKE_KILL_RESULT: 'ok',
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
      ARM_FAKE_NOW_MS: String(Date.now()),
      // — explicit here (rather than relying solely on baseEnv's
      // default) because this test explicitly re-pins ARM_FAKE_NOW_MS too;
      // redundant with baseEnv's own default post-fix, but self-documenting.
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 1 MB',
    }),
  );

  expect(result.status).toBe(2);
  expect(result.stderr).toMatch(/partially-faked/);
  expect(result.stderr).toMatch(/ARM_FAKE_KILL_LOG/);
});

// ---------------------------------------------------------------------------
// Scenario 14 (review, Low) — `--evict-pid`/`--orchestrator-pid` must
// require a positive-integer pid string, mirroring `--poll-footprint`'s own
// `Number.isInteger(pid) && pid > 0` validation, rather than accepting any
// non-empty string. This value ultimately reaches `process.kill(pid, signal)`
// (via `gracefulStop`) and the ps-tree ancestry walk, so a malformed value
// should fail loudly at the flag boundary, not merely fail to match later.
// ---------------------------------------------------------------------------

it('rejects a non-numeric --evict-pid with exit 2 and a clear stderr message', () => {
  const ORCHESTRATOR_ID = 'orch-evict-malformed-evict-pid';

  const result = runCli(
    [`--orchestrator-id=${ORCHESTRATOR_ID}`, '--evict-pid=not-a-pid', '--orchestrator-pid=501', '--lease-id=some-lease'],
    baseEnv({ ARM_FAKE_COLLECT_JSON: undefined, ARM_FAKE_PS_OUTPUT: undefined }),
  );

  expect(result.status).toBe(2);
  expect(result.stderr).toMatch(/--evict-pid/);
});

it('rejects a non-numeric --orchestrator-pid with exit 2 and a clear stderr message', () => {
  const ORCHESTRATOR_ID = 'orch-evict-malformed-orchestrator-pid';

  const result = runCli(
    [`--orchestrator-id=${ORCHESTRATOR_ID}`, '--evict-pid=9001', '--orchestrator-pid=not-a-pid', '--lease-id=some-lease'],
    baseEnv({ ARM_FAKE_COLLECT_JSON: undefined, ARM_FAKE_PS_OUTPUT: undefined }),
  );

  expect(result.status).toBe(2);
  expect(result.stderr).toMatch(/--orchestrator-pid/);
});

// ---------------------------------------------------------------------------
// Scenario 14b (round-2 review, Low) — the original
// `Number.isInteger(Number(x)) && Number(x) > 0` check also accepted
// non-plain-decimal forms `Number()` happily parses: exponential notation
// (`"1e2"` -> 100) and hex (`"0x10"` -> 16). Neither is a "positive integer
// pid string" per the check's own stated intent, even though both were
// harmless in practice (a real ps-tree line's PID column is always plain
// decimal). The tightened `PID_PATTERN` (`/^\d+$/`) must reject both forms,
// for both `--evict-pid` and `--orchestrator-pid`.
// ---------------------------------------------------------------------------

it.each([
  ['--evict-pid', '1e2'],
  ['--evict-pid', '0x10'],
  ['--orchestrator-pid', '1e2'],
  ['--orchestrator-pid', '0x10'],
])('rejects a previously-accepted-but-wrong %s=%s form with exit 2', (flagName, value) => {
  const ORCHESTRATOR_ID = 'orch-evict-pid-format';
  const args = {
    '--evict-pid': '9001',
    '--orchestrator-pid': '501',
  };
  args[flagName] = value;

  const result = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      `--evict-pid=${args['--evict-pid']}`,
      `--orchestrator-pid=${args['--orchestrator-pid']}`,
      '--lease-id=some-lease',
    ],
    baseEnv({ ARM_FAKE_COLLECT_JSON: undefined, ARM_FAKE_PS_OUTPUT: undefined }),
  );

  expect(result.status).toBe(2);
  expect(result.stderr).toMatch(new RegExp(flagName.replace(/[-]/g, '\\-')));
});

// ---------------------------------------------------------------------------
// Scenario 16 (round-2 review, Medium) — the ps-tree ancestry seam
// (`ARM_FAKE_PS_OUTPUT`) and the real-kill seam (`ARM_FAKE_KILL_LOG`) must be
// coupled: either alone (without the other) is a genuine misconfiguration
// that must refuse to proceed, loudly, rather than silently authorizing
// against fabricated ancestry while a real `process.kill` adapter is still
// wired in (or vice versa).
// ---------------------------------------------------------------------------

it('refuses to proceed when ARM_FAKE_PS_OUTPUT is set without ARM_FAKE_KILL_LOG', () => {
  const ORCHESTRATOR_ID = 'orch-evict-seam-ps-only';

  const result = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      '--lease-id=some-lease',
    ],
    {
      ARM_QUEUE_FILE: queueFilePath,
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_EVICT_TEST_MODE: '1',
      ARM_EVICT_GRACE_MS: '5',
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      ARM_FAKE_KILL_LOG: undefined,
    },
  );

  expect(result.status).toBe(2);
  expect(result.stderr).toMatch(/ARM_FAKE_PS_OUTPUT/);
  expect(result.stderr).toMatch(/ARM_FAKE_KILL_LOG/);
});

it('refuses to proceed when ARM_FAKE_KILL_LOG is set without ARM_FAKE_PS_OUTPUT', () => {
  const ORCHESTRATOR_ID = 'orch-evict-seam-kill-only';

  const result = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      '--lease-id=some-lease',
    ],
    {
      ARM_QUEUE_FILE: queueFilePath,
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_EVICT_TEST_MODE: '1',
      ARM_EVICT_GRACE_MS: '5',
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: undefined,
      ARM_FAKE_KILL_LOG: fakeKillLogPath,
    },
  );

  expect(result.status).toBe(2);
  expect(result.stderr).toMatch(/ARM_FAKE_PS_OUTPUT/);
  expect(result.stderr).toMatch(/ARM_FAKE_KILL_LOG/);
});

// ---------------------------------------------------------------------------
// Scenario 16b (round-3 review, Medium) — round 2's coupling check
// above only paired ARM_FAKE_PS_OUTPUT with ARM_FAKE_KILL_LOG, leaving
// ARM_FAKE_IS_ALIVE_SEQUENCE independently togglable: `buildEvictIsAliveFn`
// (cli.mjs) reads it directly, regardless of ARM_FAKE_KILL_LOG. A leaked
// ARM_FAKE_IS_ALIVE_SEQUENCE (leftover debug env var, shared CI/shell env
// block) with every other fake seam unset would let a REAL SIGTERM/SIGKILL
// go out via the real `realEvictKill` adapter against a real, correctly-
// authorized pid, while the isAlive() checks that decide "confirmed
// stopped" are entirely fabricated — the same class of danger scenario 16
// closes for ARM_FAKE_PS_OUTPUT alone, just via a different variable. This
// must now be rejected the same way, with the same exit code and the same
// "which seams are set" diagnostic naming every required var.
// ---------------------------------------------------------------------------

it('refuses to proceed when ARM_FAKE_IS_ALIVE_SEQUENCE is set alone, without ARM_FAKE_PS_OUTPUT/ARM_FAKE_KILL_LOG/ARM_FAKE_KILL_RESULT', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-seam-is-alive-only';

  const result = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      '--lease-id=some-lease',
    ],
    {
      ARM_QUEUE_FILE: queueFilePath,
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_EVICT_TEST_MODE: '1',
      ARM_EVICT_GRACE_MS: '5',
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: undefined,
      ARM_FAKE_KILL_LOG: undefined,
      ARM_FAKE_KILL_RESULT: undefined,
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
    },
  );

  expect(result.status).toBe(2);
  expect(result.stderr).toMatch(/ARM_FAKE_IS_ALIVE_SEQUENCE/);
  expect(result.stderr).toMatch(/ARM_FAKE_PS_OUTPUT/);
  expect(result.stderr).toMatch(/ARM_FAKE_KILL_LOG/);
  expect(result.stderr).toMatch(/ARM_FAKE_KILL_RESULT/);

  // Never a real syscall, and never even a fake-kill-log write: the beat
  // must exit before gracefulStop is ever reached.
  await expect(readFile(fakeKillLogPath, 'utf8')).rejects.toThrow();
});

// ---------------------------------------------------------------------------
// Scenario 15 (review, High #2) — a never-before-seen orchestrator id
// must NOT be able to manufacture liveness by simply repeating a failed
// eviction attempt. The original `declareDibs` call in `handleEvict` ran
// UNCONDITIONALLY, including on a `not-authorized`-due-to-liveness failure —
// so that failed attempt's own self-declaration persisted a fresh ledger
// entry, and an immediately-following, otherwise-identical SECOND attempt
// would read that entry back as proof of liveness it had never legitimately
// earned. `declareDibs` must only run when `orchestratorIsLive` was already
// true, so a never-live orchestrator id stays not-live no matter how many
// times eviction is attempted.
// ---------------------------------------------------------------------------

it('never manufactures liveness for a never-live orchestrator id by repeating a failed eviction attempt', () => {
  const ORCHESTRATOR_ID = 'orch-evict-repeat-attempt-no-liveness';

  const enqueueResult = runCli(
    ['--enqueue=typescript-implementer:normal:resumable-workflow', `--orchestrator-id=${ORCHESTRATOR_ID}`],
    baseEnv({ ARM_FAKE_COLLECT_JSON: undefined, ARM_FAKE_PS_OUTPUT: undefined }),
  );
  expect(enqueueResult.status).toBe(0);
  const dequeueResult = runCli(
    ['--dequeue-if-capacity', `--orchestrator-id=${ORCHESTRATOR_ID}`],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: undefined,
    }),
  );
  expect(dequeueResult.status).toBe(0);
  const leaseId = JSON.parse(dequeueResult.stdout).leaseId;

  const evictArgs = [
    `--orchestrator-id=${ORCHESTRATOR_ID}`,
    '--orchestrator-pid=501',
    '--evict-pid=9001',
    `--lease-id=${leaseId}`,
  ];
  const evictEnv = baseEnv({
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
    ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
  });

  // review, Medium — every non-`'evict'`-success outcome exits
  // non-zero uniformly; see scenario 2's comment above for the rationale.
  const firstAttempt = runCli(evictArgs, evictEnv);
  expect(firstAttempt.status).not.toBe(0);
  const firstBody = JSON.parse(firstAttempt.stdout);
  expect(firstBody.type).not.toBe('evict');
  expect(firstBody.reason).toBe('not-authorized');

  // A second, otherwise-identical attempt must fail exactly the same way —
  // the first attempt's own declareDibs call (if it ran) must not have
  // primed the ledger with a liveness entry this orchestrator never earned.
  const secondAttempt = runCli(evictArgs, evictEnv);
  expect(secondAttempt.status).not.toBe(0);
  const secondBody = JSON.parse(secondAttempt.stdout);
  expect(secondBody.type).not.toBe('evict');
  expect(secondBody.reason).toBe('not-authorized');

  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

// ---------------------------------------------------------------------------
// Scenario 17 (round-4 review — architectural fix) — the master
// `ARM_EVICT_TEST_MODE` switch. Three straight review rounds each found a
// DIFFERENT `ARM_FAKE_*` var this beat reads that had been left out of an
// enumerated "these must be faked together" list (round 2 missed
// `ARM_FAKE_IS_ALIVE_SEQUENCE`; round 3's own fix for that missed
// `ARM_FAKE_NOW_MS`). An enumerated list is incomplete by construction —
// nothing stops a future seam from being added to this beat without also
// being added to the list. `ARM_EVICT_TEST_MODE=1` closes the whole class of
// gap architecturally: every `ARM_FAKE_*` seam this beat reads
// (`ARM_FAKE_PS_OUTPUT`, `ARM_FAKE_KILL_LOG`, `ARM_FAKE_KILL_RESULT`,
// `ARM_FAKE_IS_ALIVE_SEQUENCE`, `ARM_FAKE_NOW_MS`) must have ZERO effect on
// `handleEvict`'s behavior unless this ONE switch is also set — not "inert
// because every seam happens to be coupled correctly today," but inert BY
// CONSTRUCTION: the real-adapter code paths (`collectPsOutput`,
// `buildEvictKillFn`, `buildEvictIsAliveFn`, `resolveEvictNow`) never even
// read `process.env.ARM_FAKE_*` when the switch is off.
//
// This test genuinely exercises the REAL, non-faked code paths — it does
// NOT mock or stub anything, spawns the real `cli.mjs` exactly like every
// other test in this file, and deliberately leaves `ARM_EVICT_TEST_MODE`
// unset while fully faking EVERY seam this beat reads with values that, if
// honored, would authorize and "cleanly" evict PID 9001 (a fully-coupled,
// would-otherwise-succeed combination, per `PS_AUTHORIZED`'s own fixture
// used successfully by scenario 1a above). The discriminator: `--evict-pid`
// targets THIS TEST'S OWN, REAL, currently-running process (`process.pid`)
// instead of the fixture's synthetic 9001 —
//   - if the master switch genuinely gates every seam (the fix under test),
//     the REAL `ps` shell-out is used, which DOES contain this real process
//     (it is running right now) but whose real `comm` (`node`, never a
//     `/claude`-suffixed path) can never satisfy `isEvictionAuthorized`'s
//     condition 3 — so the beat must report `reason: 'not-authorized'`
//     (candidate FOUND, just not a claude-rooted process);
//   - if any seam leaked through despite the switch being off (the bug this
//     fix closes), `ARM_FAKE_PS_OUTPUT`'s fixed fixture text — containing
//     only the synthetic PIDs 1/501/601/9001/9002, never this test's real,
//     dynamically-assigned pid — would instead be consulted, producing
//     `reason: 'candidate-not-found'` (candidate ABSENT from that fixture)
//     or, if the orchestrator/candidate pairing coincidentally matched, an
//     actual `type: 'evict'` with a real fake-kill-log write.
// `'not-authorized'` vs `'candidate-not-found'` is therefore a precise,
// deterministic proof of which ps data path actually ran — not merely an
// absence of a crash.
// ---------------------------------------------------------------------------

it('ignores every ARM_FAKE_* eviction seam, fully combined, when ARM_EVICT_TEST_MODE is not set — real adapters used regardless', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-test-mode-off';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // — this scenario's own discriminator is which ps-tree data path ran
  // (real vs. fake), not the pid-binding gate; seed a recorded pid matching
  // the `--orchestrator-pid=1` supplied below so the fail-closed binding
  // check passes and the beat actually reaches that discriminator.
  await declareDibsWithPid(ORCHESTRATOR_ID, 1);

  // This test's own real, currently-running process — guaranteed to appear
  // in a genuine `ps` snapshot taken during this test, with a real `comm`
  // (`node`) that can never pass the `/claude$` check.
  const realCandidatePid = String(process.pid);

  const fullyFakedButSwitchOff = baseEnv({
    ARM_EVICT_TEST_MODE: undefined, // the master switch under test — deliberately OFF
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
    ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
    ARM_FAKE_KILL_LOG: fakeKillLogPath,
    ARM_FAKE_KILL_RESULT: 'ok',
    ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
    // An obviously-fabricated, wildly-stale clock value (2000-01-01) — part
    // of the "fully faked" combination; this beat's real clock must be used
    // regardless of what this holds when the switch is off.
    ARM_FAKE_NOW_MS: '946684800000',
    // — deliberately left at baseEnv's default ('1') here: this test's
    // subject is ARM_EVICT_TEST_MODE being OFF, which short-circuits
    // resolveEvictNow() before it ever calls resolveNow() at all (see
    // resolveEvictNow's own header) — the new switch is therefore
    // never consulted on this path regardless of its value, so it neither
    // needs to be forced on nor off for this assertion to hold.
  });

  const result = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=1',
      `--evict-pid=${realCandidatePid}`,
      `--lease-id=${leaseId}`,
    ],
    fullyFakedButSwitchOff,
  );

  // review, Medium — every non-`'evict'`-success outcome exits
  // non-zero uniformly; see scenario 2's comment above for the rationale.
  expect(result.status).not.toBe(0);
  const body = JSON.parse(result.stdout);

  expect(body.type).not.toBe('evict');
  expect(body.evictionAttempted).toBe(true);
  expect(body.reason).toBe('not-authorized');

  // Never reached gracefulStop — the fake-kill-log file must never be
  // created, even though ARM_FAKE_KILL_LOG named a real path and
  // ARM_FAKE_KILL_RESULT/ARM_FAKE_IS_ALIVE_SEQUENCE were set to values that
  // would report a clean, confirmed-stopped eviction if gracefulStop ever
  // ran with them wired in.
  await expect(readFile(fakeKillLogPath, 'utf8')).rejects.toThrow();

  // The lease was never touched — still leased, exactly as before this
  // (refused) attempt.
  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

// ---------------------------------------------------------------------------
// Scenario 17b — direct contrast for scenario 17 above: the IDENTICAL fully-
// faked env, differing only by adding `ARM_EVICT_TEST_MODE: '1'`, DOES flip
// the outcome (to `'candidate-not-found'`, since `PS_AUTHORIZED`'s fixture
// text never contains this test's real pid). This proves scenario 17's
// `'not-authorized'` result is caused by the master switch being off (real ps
// consulted) rather than by some unrelated reason the fake seams would have
// produced anyway.
// ---------------------------------------------------------------------------

it('the same fully-faked env DOES change behavior once ARM_EVICT_TEST_MODE=1 is added (contrast for the test above)', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-test-mode-on-contrast';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // — see the scenario above's comment.
  await declareDibsWithPid(ORCHESTRATOR_ID, 1);
  const realCandidatePid = String(process.pid);

  const fullyFakedSwitchOn = baseEnv({
    ARM_EVICT_TEST_MODE: '1',
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
    ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
    ARM_FAKE_KILL_RESULT: 'ok',
    ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
  });

  const result = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=1',
      `--evict-pid=${realCandidatePid}`,
      `--lease-id=${leaseId}`,
    ],
    fullyFakedSwitchOn,
  );

  // review, Medium — every non-`'evict'`-success outcome exits
  // non-zero uniformly; see scenario 2's comment above for the rationale.
  expect(result.status).not.toBe(0);
  const body = JSON.parse(result.stdout);
  expect(body.type).not.toBe('evict');
  expect(body.reason).toBe('candidate-not-found');
});

// ---------------------------------------------------------------------------
// Scenario 17c/17d — resolveEvictNow(true) defers to
// resolveNow() (see resolveNow's and resolveEvictNow's own header comments),
// so resolveNow()'s own two-layer master switch (ARM_BEAT_NOW_TEST_MODE)
// applies here too, on top of (not instead of) ARM_EVICT_TEST_MODE: a
// leaked/absent ARM_BEAT_NOW_TEST_MODE now falls back to the real clock even
// with ARM_EVICT_TEST_MODE=1 and ARM_COORDINATION_FILE set (every eviction
// call sets the latter, which is why the old two-var gate provided no real
// protection on this path — see resolveEvictNow's header comment).
//
// The discriminator is the SAME liveness mechanism scenario 17/17b already
// established works this way (readDibs's pruneStale, referenced via `now` in
// handleEvict — see cli.mjs around the `readDibs(coordinationFilePath, now,
// ...)` call): a wildly-far-future `now` makes a dibs entry that was
// genuinely just declared (by seedLeasedItem's declareDibsFor, moments ago,
// at the real clock) look catastrophically stale relative to it, flipping
// `orchestratorIsLive` to false and refusing the eviction with
// `reason: 'not-authorized'` — even though the orchestrator is live right
// now. A real, un-faked clock never produces this false negative.
// ---------------------------------------------------------------------------

it('ARM_EVICT_TEST_MODE=1 with the new master switch unset must still yield the real clock, not the leaked far-future ARM_FAKE_NOW_MS', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-1359-switch-unset';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // — this scenario's own discriminator is whether the real (fresh)
  // clock or a leaked far-future fake one governs liveness; seed a matching
  // recorded pid so the fail-closed binding check passes and the beat
  // actually reaches that discriminator instead of an unrelated rejection.
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  // Far enough in the future (2100-01-01T00:00:00Z) that it can never be
  // mistaken for a real Date.now() value, and far enough past
  // DEFAULT_LIVENESS_THRESHOLD_MS (15 minutes) that a just-declared dibs
  // entry compared against it is unambiguously "stale" if honoured.
  const FAR_FUTURE_FAKE_NOW = 4_102_444_800_000;

  const evictResult = runCli(
    [`--orchestrator-id=${ORCHESTRATOR_ID}`, '--orchestrator-pid=501', '--evict-pid=9001', `--lease-id=${leaseId}`],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
      ARM_FAKE_NOW_MS: String(FAR_FUTURE_FAKE_NOW),
      // The new switch under test — deliberately UNSET (overriding
      // baseEnv's own default of '1').
      ARM_BEAT_NOW_TEST_MODE: undefined,
    }),
  );

  expect(evictResult.status).toBe(0);
  const body = JSON.parse(evictResult.stdout);

  // With ARM_BEAT_NOW_TEST_MODE unset, resolveNow() falls back to the real
  // clock despite the leaked far-future ARM_FAKE_NOW_MS: pruneStale judges
  // the just-declared dibs entry genuinely fresh, orchestratorIsLive comes
  // back true, and eviction proceeds — contrast this with the pre-existing
  // gate, under which the leaked value WAS honoured here (ARM_EVICT_TEST_MODE
  // and ARM_COORDINATION_FILE alone satisfied it), wrongly refusing with
  // reason: 'not-authorized' even though the orchestrator was live.
  expect(body.type).toBe('evict');
  expect(body.outcome).toBe('exited-before-grace');
});

it('all four flags together (ARM_EVICT_TEST_MODE, ARM_COORDINATION_FILE, ARM_FAKE_NOW_MS, and the new master switch) honour the fake clock (contrast for the test above)', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-1359-switch-set';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });

  const FAR_FUTURE_FAKE_NOW = 4_102_444_800_000;

  const evictResult = runCli(
    [`--orchestrator-id=${ORCHESTRATOR_ID}`, '--orchestrator-pid=501', '--evict-pid=9001', `--lease-id=${leaseId}`],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
      ARM_FAKE_NOW_MS: String(FAR_FUTURE_FAKE_NOW),
      ARM_BEAT_NOW_TEST_MODE: '1',
    }),
  );

  // Regression pin: with all three legs of resolveNow()'s gate satisfied
  // (ARM_EVICT_TEST_MODE=1, ARM_COORDINATION_FILE set, ARM_BEAT_NOW_TEST_MODE
  // === '1'), the fake clock is still honoured — proving the three-way-AND
  // gate does not accidentally close for the switch-ON case that every
  // pre-existing eviction test in this file already relies on.
  expect(evictResult.status).not.toBe(0);
  const body = JSON.parse(evictResult.stdout);
  expect(body.type).not.toBe('evict');
  expect(body.evictionAttempted).toBe(true);
  expect(body.reason).toBe('not-authorized');
});

// ---------------------------------------------------------------------------
// Scenario 18 (review, Medium — footprint seam gating) — direct
// contrast pair for `ARM_FAKE_FOOTPRINT_OUTPUT`, the one eviction-reachable
// seam Scenario 17/17b's `EVICT_FAKE_SEAM_VARS` enumeration did not yet cover
// at the time those tests were written: `buildFallback`'s pause-candidate
// body (`{ type: 'pause', pauseCandidate: { ..., physFootprintMb } }`) is
// built from `selectPauseCandidateWithFootprint` -> `sampleFootprint`
// (./lib/pause-candidate.mjs -> ./lib/footprint-sampler.mjs), which reads
// `ARM_FAKE_FOOTPRINT_OUTPUT`. Both scenarios below omit `--lease-id`
// entirely so `handleEvict` takes its very first fallback branch
// (`reason: 'lease-not-found'`) before touching the queue at all — the
// simplest possible route to `buildFallback`, isolating this seam from every
// other one already covered by scenario 17/17b.
//
// `ARM_FAKE_PS_OUTPUT` is set to `PS_AUTHORIZED` in both scenarios (a fixture
// containing one synthetic claude-rooted process, PID 9001) purely so the
// switch-ON contrast (18b) has a deterministic, real candidate to sample a
// footprint for. In scenario 18 (switch OFF), `ARM_FAKE_PS_OUTPUT` is itself
// inert too (per scenario 17's own proof) — `collectPsOutput(false)` shells
// out to this HOST's real `ps` instead, which does not contain PID 9001 as a
// claude-rooted process, so `PS_AUTHORIZED`'s fixture text never reaches
// `listAgentProcesses` at all. Whatever real candidates (if any) this host's
// real process tree yields, none of them can have a `physFootprintMb` of
// exactly the sentinel value below (`9999`) — a real `footprint`/`vmmap`
// sample is never that literal round number — so the assertion is
// deterministic across hosts without requiring a specific real candidate to
// exist.
// ---------------------------------------------------------------------------

it('ignores ARM_FAKE_FOOTPRINT_OUTPUT on the fallback pause-candidate body when ARM_EVICT_TEST_MODE is not set — real footprint sampling used instead', () => {
  const ORCHESTRATOR_ID = 'orch-evict-footprint-seam-off';

  const fullyFakedButSwitchOff = baseEnv({
    ARM_EVICT_TEST_MODE: undefined, // the master switch under test — deliberately OFF
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
    ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
    ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 9999 MB',
  });

  const result = runCli(
    [`--orchestrator-id=${ORCHESTRATOR_ID}`, '--orchestrator-pid=1', '--evict-pid=9001'],
    fullyFakedButSwitchOff,
  );

  // review, Medium — every non-`'evict'`-success outcome exits
  // non-zero uniformly; see scenario 2's comment above for the rationale.
  expect(result.status).not.toBe(0);
  const body = JSON.parse(result.stdout);
  expect(body.type).toBe('pause');
  expect(body.reason).toBe('lease-not-found');

  // The sentinel fake value must never appear — proving
  // `ARM_FAKE_FOOTPRINT_OUTPUT` was never consulted at all, not merely that
  // it happened to be overridden by coincidence.
  expect(body.pauseCandidate?.physFootprintMb).not.toBe(9999);
});

it('the same env DOES honor ARM_FAKE_FOOTPRINT_OUTPUT once ARM_EVICT_TEST_MODE=1 is added (contrast for the test above)', () => {
  const ORCHESTRATOR_ID = 'orch-evict-footprint-seam-on';

  const fullyFakedSwitchOn = baseEnv({
    ARM_EVICT_TEST_MODE: '1',
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
    ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
    ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 9999 MB',
  });

  const result = runCli(
    [`--orchestrator-id=${ORCHESTRATOR_ID}`, '--orchestrator-pid=1', '--evict-pid=9001'],
    fullyFakedSwitchOn,
  );

  // review, Medium — every non-`'evict'`-success outcome exits
  // non-zero uniformly; see scenario 2's comment above for the rationale.
  expect(result.status).not.toBe(0);
  const body = JSON.parse(result.stdout);
  expect(body.type).toBe('pause');
  expect(body.reason).toBe('lease-not-found');

  // With the switch on, `ARM_FAKE_PS_OUTPUT`'s synthetic PID 9001 IS
  // discovered as a candidate, and `ARM_FAKE_FOOTPRINT_OUTPUT` IS consulted
  // for its footprint — the exact opposite of the test above, proving the
  // contrast is caused by the master switch and not some unrelated seam.
  expect(body.pauseCandidate).toMatchObject({ agentId: '9001', physFootprintMb: 9999 });
});

// ---------------------------------------------------------------------------
// Scenario 19 (reviewer HIGH finding on — liveness-refresh
// `declareDibs` call wipes `agentClasses`/history via full-entry-replace
// semantics). `declareDibs` (./lib/coordination-file.mjs) performs a full
// REPLACE on upsert, not a field-level merge — only `firstDeclaredAt` is
// carried forward automatically (see its own doc comment). The
// `--running-agent-classes` call site (cli.mjs's default `--desired-agents`
// beat, see ./cli-memory-projection.jest.spec.mjs test 4) already follows
// this contract correctly by supplying the full current `agentClasses` set
// on every beat. This `--evict-pid` liveness-refresh call site did NOT: it
// declared only `{ orchestratorId, desiredAgents, declaredAt,
// firstDeclaredAt }`, silently dropping this orchestrator's `agentClasses`
// (and memory/disk hysteresis history) the moment ANY `--evict-pid` beat ran
// — even one that failed and never touched a real process.
//
// Proven behaviorally (not just via a source-order regression), mirroring
// ./cli-memory-projection.jest.spec.mjs test 4's own technique: an
// orchestrator declares two concurrently-live agent classes (X and Y) whose
// combined historical peak — together with a later, cheap observer
// candidate class — crosses the 16 GiB projected-peak budget. It then issues
// an `--evict-pid` beat that fails with `reason: 'not-authorized'` (no real
// eviction happens, no lease is released — its real, previously-issued
// lease is looked up and matched, but the candidate pid fails the ps-tree
// ancestry check) purely to exercise the liveness-refresh `declareDibs`
// call. `not-authorized` is deliberately chosen over `lease-not-found`
// (which returns EARLIER in `handleEvict`, before ever reaching the
// liveness-refresh `declareDibs` call under test — a lease lookup that
// fails immediately would give this test a false pass regardless of the
// fix). A later, distinct orchestrator's admission request for the cheap
// candidate class must still be refused (`'memory-projection-block'`) —
// proving `agentClasses` (X and Y) survived the eviction beat. Pre-fix, the
// eviction beat's field-dropping `declareDibs` call wipes both classes from
// the ledger, letting the candidate beat wrongly under-count the projected
// sum and admit (verified manually against the pre-fix source: the
// candidate beat returns `'spawn-allowed'` instead).
// ---------------------------------------------------------------------------

it('preserves agentClasses (and hysteresis history) across an --evict-pid liveness-refresh beat', async () => {
  const CLASS_X = 'evict-liveness-class-x';
  const CLASS_Y = 'evict-liveness-class-y';
  const CANDIDATE_CLASS = 'evict-liveness-candidate';
  const ORCHESTRATOR_ID = 'orch-evict-liveness-refresh-preserves-classes';

  const historyFilePath = join(workDir, 'history.json');

  function seedHistory(agentClass, peakMemoryMbValues) {
    peakMemoryMbValues.forEach((peakMemoryMb, index) => {
      const result = runCli(
        [
          '--record-outcome',
          `--operation-type=${agentClass}`,
          `--orchestrator-id=orch-seed-${agentClass}-${index}`,
          `--peak-memory-mb=${peakMemoryMb}`,
        ],
        { ARM_HISTORY_FILE: historyFilePath },
      );
      expect(result.status).toBe(0);
    });
  }

  const projectionEnv = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_HISTORY_FILE: historyFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
  };

  // Seed a REAL, currently-leased work item for this orchestrator FIRST
  // (before any agent-class declaration) — `handleEvict` looks up
  // `--lease-id` before it ever reaches the liveness-refresh `declareDibs`
  // call, so a genuinely-matched lease is required to get past that check
  // and actually exercise the call under test. This intentionally does NOT
  // reuse `seedLeasedItem`/`declareDibsFor`: those declare dibs with NO
  // agent classes, which would itself (harmlessly, since no classes exist
  // yet to lose) full-replace this orchestrator's entry — done here
  // explicitly and first, so the ordering is obvious.
  const enqueueResult = runCli(
    [`--enqueue=typescript-implementer:normal:resumable-workflow`, `--orchestrator-id=${ORCHESTRATOR_ID}`],
    { ARM_QUEUE_FILE: queueFilePath, ARM_COORDINATION_FILE: coordinationFilePath },
  );
  expect(enqueueResult.status).toBe(0);
  const dequeueResult = runCli(['--dequeue-if-capacity', `--orchestrator-id=${ORCHESTRATOR_ID}`], {
    ARM_QUEUE_FILE: queueFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
  });
  expect(dequeueResult.status).toBe(0);
  const dequeued = JSON.parse(dequeueResult.stdout);
  expect(dequeued.granted).toBe(true);
  const leaseId = dequeued.leaseId;

  // X and Y individually fit comfortably; X + Y + the candidate together
  // exceed the 16 GiB budget — exactly ./cli-memory-projection.jest.spec.mjs
  // test 4's own margin shape, reused here so the same assertion proves the
  // same thing: both classes are still counted.
  seedHistory(CLASS_X, [6144, 6144, 6144]);
  seedHistory(CLASS_Y, [6144, 6144, 6144]);
  seedHistory(CANDIDATE_CLASS, [4096, 4096, 4096]);

  // NOW declares both agent classes as concurrently live — this beat's own
  // `declareDibs` call full-replaces the entry seeded above, which is
  // expected and fine: it's the LAST real (non-eviction) beat before the
  // eviction attempt, and is what `existingSelf` must reflect going in.
  const declareResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--desired-agents=1',
      `--agent-class=${CLASS_X}`,
      `--running-agent-classes=${CLASS_Y}`,
    ],
    projectionEnv,
  );
  expect(declareResult.status).toBe(0);

  // — this beat's own real ps-tree walk never resolves a claude-rooted
  // ancestor inside Jest, so the entry `declareResult` just wrote carries no
  // `pid`. Patch ONLY the `pid` field directly (bypassing another real CLI
  // beat, which would perform declareDibs's own full-entry REPLACE and wipe
  // the very agentClasses/history this test exists to prove survive) so the
  // fail-closed binding check below passes and the beat under test actually
  // reaches its own liveness-refresh declareDibs call rather than being
  // rejected earlier for an unrelated reason.
  const priorDibs = await readDibs(coordinationFilePath);
  const priorSelf = priorDibs.find((entry) => entry.orchestratorId === ORCHESTRATOR_ID);
  expect(priorSelf).toBeDefined();
  await declareDibs(coordinationFilePath, { ...priorSelf, pid: 501 });

  // An --evict-pid beat with a REAL, matching lease-id, but a candidate pid
  // that fails ps-tree ancestry authorization (`PS_NOT_AUTHORIZED`) — never
  // touches a real process, never releases the lease, but DOES run the
  // liveness-refresh `declareDibs` call under test, since this orchestrator
  // is already live and the lease lookup succeeds before the authorization
  // check fails.
  const evictResult = runCli(
    [`--orchestrator-id=${ORCHESTRATOR_ID}`, '--orchestrator-pid=501', '--evict-pid=9001', `--lease-id=${leaseId}`],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_NOT_AUTHORIZED,
    }),
  );
  expect(evictResult.status).not.toBe(0);
  const evictBody = JSON.parse(evictResult.stdout);
  expect(evictBody.type).not.toBe('evict');
  expect(evictBody.reason).toBe('not-authorized');

  // A later, distinct orchestrator asks for the cheap candidate class. If
  // the eviction beat above had wiped CLASS_X/CLASS_Y from the ledger (the
  // pre-fix bug), the projected sum here would be the candidate class alone
  // (~4 GiB) — comfortably admitted (`'spawn-allowed'`). With both classes
  // correctly preserved, the projected sum is X + Y + candidate (~16 GiB),
  // which must be refused.
  const candidateResult = runCli(
    [`--orchestrator-id=orch-evict-liveness-observer`, '--desired-agents=1', `--agent-class=${CANDIDATE_CLASS}`],
    projectionEnv,
  );
  expect(candidateResult.status).toBe(0);
  const candidateBody = JSON.parse(candidateResult.stdout);

  expect(candidateBody.type).toBe('memory-projection-block');
  expect(candidateBody.totalProjectedMemoryMb).toBeGreaterThanOrEqual(6144 + 6144 + 4096);
});

// ---------------------------------------------------------------------------
// — LEDGER WRITE CALL SITE 4 IS A DOCUMENTED EXCEPTION TO THE
// ONE-IDENTITY-PER-BEAT RULE, AND THESE TESTS ARE WHY IT HAS TO BE.
//
// The other four call sites take their `pid`/`pidStartedAt` from
// `observeBeatProcesses`' single memoized per-beat identity. `performEviction`
// resolves its own, from `collectPsOutput(evictTestModeEnabled)` — see cli.mjs's
// standing-rule block near `resolveNow`. Reading that block, the obvious
// "cleanup" is to thread the shared identity in here too. These tests exist to
// stop that cleanup, because it would be a security regression rather than a
// tidy-up:
//
//   `collectBeatPsSnapshot` honours `ARM_FAKE_PS_OUTPUT` behind its OWN master
//   switch, `ARM_BEAT_SNAPSHOT_TEST_MODE`. `collectPsOutput(honorFakeSeam)` on
//   this path honours it behind a DIFFERENT one, `ARM_EVICT_TEST_MODE`.
//   Sharing one snapshot would let whichever switch happened to be set decide
//   what identity a real beat of the OTHER type stamps on the ledger — i.e. a
//   test-mode switch for one beat type reaching across into the other, exactly
//   the leakage the two-layer model exists to make impossible by construction.
//
// The exception is safe because it cannot diverge: `main()` dispatches
// `handleEvict`/`handleWatchdogBeat` — `performEviction`'s only two callers —
// and RETURNS from both, ahead of the `observeBeatProcesses()` call that begins
// an ordinary beat, and `--evict-pid`/`--watchdog-beat` are rejected outright
// alongside any other beat-type flag. No single invocation can write through
// both paths, so two ledger writes in one beat can never carry two identities.
// ---------------------------------------------------------------------------

/**
 * A pid genuinely NOT running on this host right now, proven by
 * `process.kill(pid, 0)` raising `ESRCH`. Probed rather than hardcoded — the
 * real-adapter test below runs with `ARM_EVICT_TEST_MODE` UNSET, so
 * `buildEvictKillFn` returns the REAL `kill(2)` adapter, and the fixture's
 * "nothing to kill" premise has to be verified rather than assumed.
 */
function pickConfirmedDeadPid(startAt) {
  for (let candidate = startAt; candidate > startAt - 500; candidate -= 1) {
    try {
      process.kill(candidate, 0);
    } catch (error) {
      if (error.code === 'ESRCH') return candidate;
    }
  }
  throw new Error(`could not find a confirmed-dead pid near ${startAt}`);
}

/** A claude-rooted ancestor sitting above this test process's REAL ppid chain. */
const EVICT_FAKE_CLAUDE_ROOT_PID = 930_001;

/**
 * A fake tree in which the ancestry walk — which starts at the spawned
 * `cli.mjs` child's `process.ppid`, i.e. this test process's own pid — WOULD
 * resolve `EVICT_FAKE_CLAUDE_ROOT_PID` if the eviction beat honoured
 * `ARM_FAKE_PS_OUTPUT`. Generated around the real anchors for that reason.
 */
function buildEvictAncestryPsOutput() {
  return [
    '  PID  PPID    RSS     ELAPSED COMM',
    '    1     0    512 1-03:46:39 /sbin/launchd',
    `${EVICT_FAKE_CLAUDE_ROOT_PID}     1 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude`,
    `${process.ppid} ${EVICT_FAKE_CLAUDE_ROOT_PID}  81920    00:30:00 /usr/local/bin/node`,
    `${process.pid} ${process.ppid}  65536    00:10:00 /usr/local/bin/node`,
    '',
  ].join('\n');
}

it("does not let a leaked ARM_FAKE_PS_OUTPUT decide a real eviction beat's recorded ledger identity", async () => {
  const ORCHESTRATOR_ID = 'orch-evict-identity-seam';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });

  // Nothing here can touch a process: both pids are proven absent from this
  // host, so `isEvictionAuthorized` refuses before `gracefulStop` is reached
  // and the real kill adapter is never invoked. The beat still runs call site
  // 4's `declareDibs` first — that is what is under test.
  const deadCandidatePid = pickConfirmedDeadPid(4_194_301);
  const deadOrchestratorPid = pickConfirmedDeadPid(deadCandidatePid - 1);
  expect(deadCandidatePid).not.toBe(deadOrchestratorPid);

  // — seed a recorded pid matching the --orchestrator-pid supplied
  // below so the fail-closed binding check passes and the beat actually
  // reaches the real-vs-fake ps-tree discriminator this scenario exists to
  // prove, rather than being rejected earlier for an unrelated reason.
  await declareDibsWithPid(ORCHESTRATOR_ID, deadOrchestratorPid);

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      `--orchestrator-pid=${deadOrchestratorPid}`,
      `--evict-pid=${deadCandidatePid}`,
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      // THE POINT OF THIS TEST: the master switch is OFF, as it is for every
      // real orchestrator invocation, while the fake snapshot seam is set —
      // the "leaked/stale var" situation the two-layer model is built for.
      ARM_EVICT_TEST_MODE: undefined,
      ARM_FAKE_PS_OUTPUT: buildEvictAncestryPsOutput(),
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    }),
  );

  // The beat got PAST the lease lookup and the liveness gate — so call site 4's
  // `declareDibs` really did run — and then refused the eviction because the
  // REAL process tree carries no such candidate. Both halves matter: without
  // the first the assertion below would be vacuous, and without the second the
  // fixture would not be provably harmless.
  expect(evictResult.status).not.toBe(0);
  expect(JSON.parse(evictResult.stdout).reason).toBe('candidate-not-found');

  const persisted = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
  const selfEntry = persisted.find((entry) => entry.orchestratorId === ORCHESTRATOR_ID);
  expect(selfEntry).toBeDefined();

  // The identity this beat stamped came from the REAL `ps` tree (or, if no
  // claude-rooted ancestor exists in it, from nothing at all) — never from the
  // fake seam, whose tree was constructed specifically so that honouring it
  // would yield `EVICT_FAKE_CLAUDE_ROOT_PID` here.
  expect(selfEntry.pid).not.toBe(EVICT_FAKE_CLAUDE_ROOT_PID);
});

it("counts the eviction beat's own process-snapshot collections through ARM_FAKE_PS_COLLECTION_LOG", async () => {
  // The counter covers BOTH collection paths — `collectBeatPsSnapshot` and
  // `collectPsOutput` — which is what makes it an honest measure of this
  // beat's collections rather than a measure of one code path. (Collections,
  // not `ps` processes: see the note below the fixture, and note that under
  // this test's own `ARM_FAKE_PS_OUTPUT` no `ps` is spawned at all.) A counter
  // that covered only
  // `collectBeatPsSnapshot` would read "one snapshot this beat" on a beat that
  // took several.
  const ORCHESTRATOR_ID = 'orch-evict-collection-count';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // — seed a recorded pid matching --orchestrator-pid=501 below so the
  // fail-closed binding check passes and the beat's collection count reflects
  // the not-authorized ancestry path this scenario measures, not an earlier
  // rejection that never reaches any of the counted collection sites.
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const collectionLogPath = join(workDir, 'evict-ps-collections.log');
  await writeFile(collectionLogPath, '', 'utf8');

  const evictResult = runCli(
    [`--orchestrator-id=${ORCHESTRATOR_ID}`, '--orchestrator-pid=501', '--evict-pid=9001', `--lease-id=${leaseId}`],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_NOT_AUTHORIZED,
      ARM_FAKE_PS_COLLECTION_LOG: collectionLogPath,
    }),
  );
  expect(evictResult.status).not.toBe(0);
  expect(JSON.parse(evictResult.stdout).reason).toBe('not-authorized');

  const collections = (await readFile(collectionLogPath, 'utf8')).split('\n').filter(Boolean);

  // Non-vacuity first — the seam is wired at all.
  expect(collections.length).toBeGreaterThan(0);

  // Then the exact number, because ">0" is satisfied by a counter wired into
  // only ONE of this path's several `collectPsOutput` call sites, and "an
  // honest measure of every collection" is precisely the claim that would leave
  // untrue.
  //
  // COLLECTIONS, NOT SPAWNS (pre-PR review, Low — this comment said "spawns",
  // and under this test's own `ARM_FAKE_PS_OUTPUT` fixture no `ps` process is
  // spawned at all). `recordProcessSnapshotCollection` is called at the TOP of
  // `collectPsOutput`, before the fake/real branch, so it counts every
  // collection the beat asks for whichever branch serves it. That is the right
  // thing to count — it is the call sites that are being pinned, and in
  // production each one does cost a spawn.
  //
  // This beat — `not-authorized` under a RED memory fixture — collects three
  // times: call site 4's own ancestry-identity snapshot for its
  // `declareDibs`; the authorization snapshot taken immediately before
  // `isEvictionAuthorized` (reused, not re-collected, for the `psTreeHasPid`
  // reason check); and `buildFallback`'s pause-candidate list, which a RED
  // beat reaches and a GREEN one does not. If this number changes, the
  // eviction path's collection count changed — update this comment with it
  // rather than relaxing the bound.
  expect(collections).toHaveLength(3);
});

// ---------------------------------------------------------------------------
// Scenario 12 (— orchestrator-pid binding, Phase 0 outer acceptance).
//
// THE VULNERABILITY: today, NOTHING verifies that the caller-supplied
// `--orchestrator-pid` actually belongs to the calling `--orchestrator-id`.
// `isEvictionAuthorized` only checks (a) the orchestrator id has a live dibs
// entry and (b) `--evict-pid` descends from `--orchestrator-pid` via ps-tree
// ancestry — it never checks that `--orchestrator-pid` itself is the
// CALLER'S own pid. A live orchestrator with a lease it genuinely owns can
// therefore name an arbitrary `--orchestrator-pid` (e.g. a DIFFERENT
// orchestrator's real root pid) and evict a candidate descending from that
// foreign root, as long as the candidate happens to be reachable from it.
//
// This scenario proves it end to end: orchestrator A has a real dibs entry
// recording ITS OWN root pid (501, via `declareDibsWithPid`) and a
// genuinely-owned lease over a work item whose candidate (9001) descends
// from a DIFFERENT root (601 — `PS_NOT_AUTHORIZED`'s second root, standing in
// here for "some other orchestrator's real pid", per this file's own comment
// on that fixture). A supplies `--orchestrator-pid=601` — NOT its own
// recorded 501 — and today's ps-tree-ancestry-only check authorizes it
// anyway, because 9001 genuinely descends from 601. Post-fix, this must be
// rejected with the new `'orchestrator-pid-mismatch'` reason BEFORE any
// ps-tree ancestry work or kill signal happens.
// ---------------------------------------------------------------------------

it('rejects eviction with reason orchestrator-pid-mismatch when --orchestrator-pid is not the calling orchestrator’s own recorded pid, even though ps-tree ancestry alone would authorize it', async () => {
  const ORCHESTRATOR_A = 'orch-pid-binding-mismatch';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_A });

  // A's own real root pid, recorded by a genuine PRIOR beat — read by
  // performEviction's new check via readDibs before its own declareDibs.
  await declareDibsWithPid(ORCHESTRATOR_A, 501);

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_A}`,
      // A foreign pid (NOT A's own recorded 501) that a DIFFERENT
      // orchestrator's candidate genuinely descends from — the vulnerability
      // this scenario proves.
      '--orchestrator-pid=601',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      // 9001 descends from 601, NOT 501 — ps-tree ancestry alone authorizes
      // this today, which is exactly the gap closes.
      ARM_FAKE_PS_OUTPUT: PS_NOT_AUTHORIZED,
    }),
  );

  // RED today: with no orchestrator-pid-binding check, 9001 genuinely
  // descends from the supplied (foreign) --orchestrator-pid=601, so
  // isEvictionAuthorized currently authorizes this and the beat proceeds to
  // a real (fake-seam) eviction — `body.type` comes back `'evict'`, not the
  // rejected pause/hold this test requires.
  expect(evictResult.status).not.toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).not.toBe('evict');
  expect(['pause', 'hold']).toContain(body.type);
  expect(body.evictionAttempted).toBe(true);
  expect(body.reason).toBe('orchestrator-pid-mismatch');

  // Rejected before any ps-tree ancestry/kill work: never a real (or
  // fake-logged) signal.
  await expect(readFile(fakeKillLogPath, 'utf8')).rejects.toThrow();

  // A's lease is left completely untouched — never released, never
  // re-enqueued.
  const remaining = peekItems(ORCHESTRATOR_A);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

// ---------------------------------------------------------------------------
// Scenario 12b (— regression guard). Same setup, but
// `--orchestrator-pid` matches A's own recorded pid AND the candidate
// genuinely descends from it: eviction must still succeed. Pins the
// "still works" baseline the fix must not break.
// ---------------------------------------------------------------------------

it('still authorizes eviction when --orchestrator-pid matches the calling orchestrator’s own recorded pid and the candidate genuinely descends from it', async () => {
  const ORCHESTRATOR_A = 'orch-pid-binding-match';
  const { leaseId, agentClass, commandRef } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_A });

  await declareDibsWithPid(ORCHESTRATOR_A, 501);

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_A}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
    }),
  );

  expect(evictResult.status).toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).toBe('evict');
  expect(body.pid).toBe(9001);
  expect(body.outcome).toBe('exited-before-grace');

  const remaining = peekItems(ORCHESTRATOR_A);
  expect(remaining).toHaveLength(1);
  expect(remaining[0]).toMatchObject({ agentClass, commandRef, orchestratorId: ORCHESTRATOR_A });
  expect(remaining[0].leaseId).toBeUndefined();
});

// ---------------------------------------------------------------------------
// Scenario 12c (— fail-closed on an unrecorded pid, per this ticket's
// own Build Plan acceptance criterion: "existingSelf.pid === undefined is
// treated as a mismatch (fail closed), not as 'skip the check'"). A live
// orchestrator (a real, already-existing dibs entry) whose ledger entry
// carries NO recorded `pid` at all — because its own most recent beat's
// ps-tree identity resolution never succeeded, exactly what happens for
// every `declareDibsFor`/`seedLeasedItem`-seeded entry under Jest — must be
// REJECTED with `'orchestrator-pid-mismatch'`, even though the candidate is
// otherwise genuinely authorized by ps-tree ancestry. Treating an
// unrecorded pid as merely "unverifiable" and falling through to the
// ancestry-only check would reopen the exact vulnerability class
// exists to close for any orchestrator whose ledger entry happens to have no
// recorded pid.
// ---------------------------------------------------------------------------

it('rejects eviction with reason orchestrator-pid-mismatch when the orchestrator has no recorded pid at all on its existing ledger entry (fails closed, does not fall through to ancestry-only authorization)', async () => {
  const ORCHESTRATOR_ID = 'orch-pid-binding-no-recorded-pid';
  // Deliberately does NOT call declareDibsWithPid: this orchestrator is live
  // (seedLeasedItem's declareDibsFor already established a real, existing
  // dibs entry) but that entry carries no `pid` field, exactly as every
  // ordinary real-beat-seeded entry does under Jest's ps-tree resolution.
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      // Would otherwise authorize 9001 as descending from 501 — proving a
      // real ps-tree ancestry pass is not what stops this eviction.
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
    }),
  );

  expect(evictResult.status).not.toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).not.toBe('evict');
  expect(['pause', 'hold']).toContain(body.type);
  expect(body.evictionAttempted).toBe(true);
  expect(body.reason).toBe('orchestrator-pid-mismatch');

  // Rejected before any ps-tree ancestry/kill work: never a real (or
  // fake-logged) signal.
  await expect(readFile(fakeKillLogPath, 'utf8')).rejects.toThrow();

  // The lease is left completely untouched — never released, never
  // re-enqueued.
  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

// ---------------------------------------------------------------------------
// Phase 0 (outer acceptance) — same-orchestrator pid/lease
// pairing is NOT verified today. `performEviction`'s existing checks (see
// `lease-orchestrator-mismatch`/`orchestrator-pid-mismatch` above) close the
// CROSS-orchestrator cases, but never verify that `--evict-pid` and
// `--lease-id` actually name the SAME in-flight unit of work when both
// legitimately belong to the SAME orchestrator managing several concurrent
// agents — see cli.mjs's own comment at the `lease-orchestrator-mismatch`
// check, and the tracked, accepted limitation in
// `.claude/skills/agent-resource-management/SKILL.md` (search
// "same-orchestrator pid/lease pairing").
//
// `lib/queue.mjs` gained the `pid` field and the real `bindPidToLease`
// primitive in Phase 1 — see that module's own doc comment on
// `bindPidToLease`, matching strictly on `leaseId` AND `orchestratorId`
// together. The scenarios below call it directly (imported above), the same
// "lib-level primitive, one layer below cli.mjs" idiom `declareDibsWithPid`
// already uses for `declareDibs`. `performEviction` (cli.mjs) does not yet
// read `leasedEntry.pid` at all as of Phase 1 — that is Phase 2's own
// production change — so:
//   - the MISMATCH scenario is genuinely red until Phase 2 lands: nothing
//     today stops the eviction, so it succeeds when it must be rejected.
//   - the MATCH and UNBOUND scenarios already pass today, for the same
//     underlying reason (the field is inert until Phase 2 reads it) — kept
//     here as the Build Plan's own "positive path"/"skip-not-fail" companion
//     cases and as a pinned regression guard once Phase 2 actually starts
//     reading `leasedEntry.pid`.
// ---------------------------------------------------------------------------

it('handleEvict rejects a same-orchestrator evict-pid/lease-id pairing that does not match the pid bound to that lease', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-pid-lease-mismatch';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);
  // A DIFFERENT pid than --evict-pid=9001 below is bound to this lease — the
  // caller is pairing a genuinely-owned, genuinely-authorized pid (9001, a
  // real claude-rooted descendant of 501 per PS_AUTHORIZED) with a lease
  // that was actually checked out under a DIFFERENT process (12345). This is
  // exactly the same-orchestrator pid/lease pairing bug exists to
  // catch — ps-tree ancestry alone would happily authorize it.
  const bound = await bindPidToLease(queueFilePath, leaseId, 12345, ORCHESTRATOR_ID);
  expect(bound).not.toBeNull();

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
    }),
  );

  // RED today: nothing in performEviction reads leasedEntry.pid at all, so
  // this eviction proceeds and succeeds exactly as scenario 1a's clean-exit
  // case does — this assertion fails until Phase 1+2 land.
  expect(evictResult.status).not.toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).not.toBe('evict');
  expect(['pause', 'hold']).toContain(body.type);
  expect(body.evictionAttempted).toBe(true);
  // Distinguishable from every existing reason string this file already
  // pins (`not-authorized`, `candidate-not-found`, `lease-not-found`,
  // `lease-orchestrator-mismatch`, `orchestrator-pid-mismatch`) — per the
  // Build Plan's own "silent reason collapsing was explicitly called out as
  // a violation" note. Phase 2's implementer owns the exact literal; this
  // is this test file's own naming choice, following the existing
  // `<noun>-mismatch` convention.
  expect(body.reason).toBe('pid-lease-mismatch');
  expect(
    ['not-authorized', 'candidate-not-found', 'lease-not-found', 'lease-orchestrator-mismatch', 'orchestrator-pid-mismatch'],
  ).not.toContain(body.reason);

  // Never touches the process — this must fail before any kill is attempted.
  await expect(readFile(fakeKillLogPath, 'utf8')).rejects.toThrow();

  // The lease is left completely untouched — never released, never
  // re-enqueued.
  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

it('handleEvict succeeds when evict-pid matches the pid bound to the lease', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-pid-lease-match';
  const { leaseId, agentClass, commandRef } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);
  // The SAME pid as --evict-pid=9001 below is bound to this lease — the
  // positive path proving the new check doesn't just fail-closed
  // universally once it lands.
  const bound = await bindPidToLease(queueFilePath, leaseId, 9001, ORCHESTRATOR_ID);
  expect(bound).not.toBeNull();

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
    }),
  );

  expect(evictResult.status).toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).toBe('evict');
  expect(body.pid).toBe(9001);

  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0]).toMatchObject({ agentClass, commandRef, orchestratorId: ORCHESTRATOR_ID });
  expect(remaining[0].leaseId).toBeUndefined();
});

it('handleEvict does not reject an eviction when the leased entry has no bound pid at all (legacy/never-bound lease)', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-pid-lease-unbound';
  // Deliberately does NOT call bindPidToLease — models a lease that was
  // never (or not yet) bound via bindPidToLease, exactly what every
  // pre-existing lease on disk looks like, and what a caller that has not yet
  // adopted the new --bind-pid step still produces after Phase 3 lands. The
  // pid check must be skip-not-fail here, per this ticket's own
  // backward-compatibility Risk Flag.
  const { leaseId, agentClass, commandRef } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
    }),
  );

  expect(evictResult.status).toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).toBe('evict');
  expect(body.pid).toBe(9001);

  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0]).toMatchObject({ agentClass, commandRef, orchestratorId: ORCHESTRATOR_ID });
  expect(remaining[0].leaseId).toBeUndefined();
});

// ---------------------------------------------------------------------------
// Phase 2 — pid-lease binding is ADDITIVE to ps-tree ancestry
// authorization, never a substitute for it. A pid can numerically match the
// one bound to a lease (e.g. an OS pid-reuse coincidence: the bound pid was
// genuinely this orchestrator's own candidate at bind time, but the number
// has since been recycled by an unrelated process not descended from this
// orchestrator's root) — that match alone must not be treated as
// authorization. `isEvictionAuthorized`'s ps-tree ancestry walk must still
// independently pass, exactly as it does for every candidate with no bound
// pid at all.
// ---------------------------------------------------------------------------

it('pid match alone does not authorize eviction when ps-tree ancestry data shows the pid is not a genuine descendant (pid-reuse simulation)', async () => {
  const ORCHESTRATOR_ID = 'orch-evict-pid-lease-ancestry-wins';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);
  // 9001 is bound to this lease AND is exactly the --evict-pid supplied
  // below — the pid-lease check alone would pass. But PS_NOT_AUTHORIZED
  // (unlike PS_AUTHORIZED) parents 9001 under root 601, a DIFFERENT root
  // than the caller's --orchestrator-pid=501 — simulating a recycled OS pid
  // that numerically matches the bound value without being the genuinely
  // authorized descendant process. Ancestry must still refuse it.
  const bound = await bindPidToLease(queueFilePath, leaseId, 9001, ORCHESTRATOR_ID);
  expect(bound).not.toBeNull();

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_NOT_AUTHORIZED,
    }),
  );

  expect(evictResult.status).not.toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).not.toBe('evict');
  expect(['pause', 'hold']).toContain(body.type);
  expect(body.evictionAttempted).toBe(true);
  // The ANCESTRY reason, not the pid-lease-binding reason — proving the pid
  // match never short-circuited straight to a fabricated success, and that
  // the two checks are independent, additive gates rather than one
  // substituting for the other.
  expect(body.reason).toBe('not-authorized');
  expect(body.reason).not.toBe('pid-lease-mismatch');

  // Never touches the process — refused before any kill is attempted.
  await expect(readFile(fakeKillLogPath, 'utf8')).rejects.toThrow();

  // The lease is left completely untouched — never released, never
  // re-enqueued.
  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

// ---------------------------------------------------------------------------
// Scenario 21 (— per-beat evictBeatIdentity hoist). RED by construction
// right now: `performEviction` does not yet accept a `cachedEvictBeatIdentity`
// parameter at all, so every assertion below that looks for its presence in
// the source fails.
//
// Structural/source-text regression tests, not behavioral ones, for the same
// reason Scenario 7 above (the ps-tree-sample-timing TOCTOU fix) is
// structural: this project's `ARM_FAKE_PS_OUTPUT` seam returns one static
// string for the whole invocation, so a black-box test cannot distinguish
// "resolved once per beat" from "resolved once per target" when every
// resolution of a fixed, unchanging fake ps tree necessarily produces the
// same value regardless of how many times it is computed — see Scenario 7's
// own header comment for the identical argument, quoted here rather than
// re-derived: "Simulating the actual race black-box would require the fake
// ps output to change value mid-invocation... not achievable without adding
// production-only instrumentation whose sole purpose would be to make this
// one test possible." The genuinely-behavioral, call-count-based proof that
// a multi-target beat resolves `evictBeatIdentity` exactly once (rather than
// once per target) lives in ./watchdog-outer-acceptance.jest.spec.mjs, which
// drives the real `--watchdog-beat` multi-target loop and counts real
// process-snapshot collections via the already-shipped `ARM_FAKE_PS_COLLECTION_LOG`
// seam (`recordProcessSnapshotCollection`) — a genuine observable side
// channel that does NOT depend on the fake ps VALUE ever changing, only on
// counting how many times a snapshot was collected at all. These structural
// tests below pin the shape `performEviction`'s single-target call site
// (`--evict-pid` via `handleEvict`) must keep, which that behavioral test
// cannot reach (it only exercises the `--watchdog-beat` call site).
// ---------------------------------------------------------------------------

/**
 * Slices `cli.mjs`'s source text from `async function performEviction({`
 * up to (but not including) the next `\n// ---` section divider — the same
 * "no intervening divider" property `performEviction`'s own header comment
 * documents and Scenario 7's `handleEvict`-body slice above already relies
 * on (that slice's own end-of-function marker lands INSIDE this same
 * divider-free run, so it already includes `performEviction`'s full body;
 * this helper slices the narrower, `performEviction`-only span for
 * assertions that must not accidentally match text living in `handleEvict`
 * itself).
 */
async function readPerformEvictionSource() {
  const source = await readFile(CLI, 'utf8');
  const fnStart = source.indexOf('async function performEviction({');
  expect(fnStart).toBeGreaterThan(-1);
  const fnEnd = source.indexOf('\n// ---', fnStart);
  expect(fnEnd).toBeGreaterThan(fnStart);
  return source.slice(fnStart, fnEnd);
}

it('performEviction accepts an optional cachedEvictBeatIdentity parameter, defaulting to internal resolution when the caller supplies none (structural — pins the new signature)', async () => {
  const body = await readPerformEvictionSource();

  // The new parameter is destructured on the function's own params object,
  // alongside the pre-existing `preTakenSample` (its own once-per-beat
  // hoist for the machine-resource sample) — the exact sibling shape this
  // ticket's Investigation names as its precedent.
  const paramsBlockEnd = body.indexOf('}) {');
  expect(paramsBlockEnd).toBeGreaterThan(-1);
  const paramsBlock = body.slice(0, paramsBlockEnd);
  expect(paramsBlock).toMatch(/\bcachedEvictBeatIdentity\b/);
  expect(paramsBlock).toMatch(/\bpreTakenSample\b/);

  // The identity resolution itself must fall back to the SAME internal
  // `resolveNearestClaudeRootIdentity(...)` call this file's existing
  // Scenario 12/12b/12c tests already exercise when no cached value is
  // supplied — i.e. a caller that omits `cachedEvictBeatIdentity` entirely
  // (every call site except the watchdog beat's own) gets byte-for-byte
  // today's behavior. Matched as "the identity resolution line references
  // `cachedEvictBeatIdentity`", not a specific ternary/`??` spelling, so a
  // correct implementation is not penalized for the exact operator chosen.
  const resolveCallIndex = body.indexOf('resolveNearestClaudeRootIdentity(');
  expect(resolveCallIndex).toBeGreaterThan(-1);
  const identityAssignmentStart = body.lastIndexOf('const evictBeatIdentity', resolveCallIndex);
  expect(identityAssignmentStart).toBeGreaterThan(-1);
  const identityAssignmentEnd = body.indexOf(';', resolveCallIndex);
  expect(identityAssignmentEnd).toBeGreaterThan(resolveCallIndex);
  const identityAssignment = body.slice(identityAssignmentStart, identityAssignmentEnd);
  expect(identityAssignment).toMatch(/cachedEvictBeatIdentity/);
});

it('the authorization ps-tree sample that feeds isEvictionAuthorized stays a fresh, per-call collectPsOutput() read — never sourced from cachedEvictBeatIdentity (structural regression guard on the two-snapshot split)', async () => {
  const body = await readPerformEvictionSource();

  // This is the SAME line Scenario 7 above already pins as appearing AFTER
  // all the queue/dibs I/O — re-asserted here, narrowly, as a regression
  // guard against the specific failure mode this ticket's own Investigation
  // and Convergence Analysis flag as highest-risk: conflating the
  // (now possibly cached) IDENTITY snapshot with the (must-stay-per-target)
  // AUTHORIZATION snapshot. If a future edit made this line read from
  // `cachedEvictBeatIdentity` instead of calling `collectPsOutput` fresh, it
  // would reopen the exact PID-reuse race window its authorization-
  // snapshot design closed.
  expect(body).toMatch(/const psTreeOutput = collectPsOutput\(evictTestModeEnabled\);/);

  const authSampleIndex = body.indexOf('const psTreeOutput = collectPsOutput(evictTestModeEnabled);');
  const authSampleLineEnd = body.indexOf('\n', authSampleIndex);
  const authSampleLine = body.slice(authSampleIndex, authSampleLineEnd);
  expect(authSampleLine).not.toMatch(/cachedEvictBeatIdentity/);
});

it('the fail-safe strip-on-null declareDibs write is unchanged in kind by the hoist — still keyed on evictBeatIdentity === null, never on whether the value came from a cache', async () => {
  const body = await readPerformEvictionSource();

  // Pins the exact spread shape `declareDibs`'s call-site-4 write already
  // uses (see that call site's own doc comment, "AND THE STRIP IS STILL
  // REACHABLE ON THIS PATH, ACCEPTED") — a `null`-resolved identity (whether
  // resolved fresh or supplied via `cachedEvictBeatIdentity`) must still omit
  // `pid`/`pidStartedAt` from the write, exactly as today. This is a
  // regression guard, not a new behavior: the hoist changes WHEN/HOW OFTEN
  // `evictBeatIdentity` is computed, never what happens once it is `null`.
  expect(body).toMatch(
    /evictBeatIdentity === null\s*\n\s*\?\s*\{\}\s*\n\s*:\s*\{\s*pid:\s*evictBeatIdentity\.pid,\s*pidStartedAt:\s*evictBeatIdentity\.pidStartedAt\s*\}/,
  );
});

it("handleEvict's own performEviction call site (the --evict-pid path) does not pass cachedEvictBeatIdentity — regression guard proving the single-target call site is unaffected by the hoist", async () => {
  const source = await readFile(CLI, 'utf8');
  const fnStart = source.indexOf('async function handleEvict(flags)');
  expect(fnStart).toBeGreaterThan(-1);
  const callIndex = source.indexOf('await performEviction({', fnStart);
  expect(callIndex).toBeGreaterThan(-1);
  const callEnd = source.indexOf('});', callIndex);
  expect(callEnd).toBeGreaterThan(callIndex);
  const callArgs = source.slice(callIndex, callEnd);

  expect(callArgs).toMatch(/orchestratorId,/);
  expect(callArgs).toMatch(/evictPidRaw,/);
  // The load-bearing negative assertion: `--evict-pid`'s own call site must
  // NOT be touched by this ticket's fix at all — the new parameter's default
  // (fall back to internal resolution when undefined) is what keeps this
  // call site's behavior byte-for-byte identical without any edit here.
  expect(callArgs).not.toMatch(/cachedEvictBeatIdentity/);
});

// Phase 2 — cli.mjs's `buildEvictIsAliveFn` wiring. RED by
// construction: `./lib/reap-aware-is-alive.mjs` does not exist yet, and
// `buildEvictIsAliveFn`'s real-adapter branch (`testModeEnabled === false`)
// today returns the bare `realEvictIsAlive` unwrapped.
//
// Both tests below deliberately do NOT `import` `cli.mjs` directly — this
// file's own header idiom ("Black-box, real-process-spawning ... never
// imports cli.mjs directly") exists because `cli.mjs` unconditionally calls
// `main().catch(...)` at module load (see `./lib/real-evict-adapters.mjs`'s
// own doc comment for why its two adapters were pulled out of `cli.mjs` for
// exactly this reason). `buildEvictIsAliveFn` itself is a local, non-exported
// function inside `cli.mjs` — there is no way to call it directly without
// either importing `cli.mjs` (running the whole beat as a side effect) or
// having the implementer export it, which would need a production-code
// change this test-authoring pass does not make. So:
//
//   - Test A (real path, testModeEnabled=false) tests the exact COMPOSITION
//     `buildEvictIsAliveFn`'s real branch is required to produce per the
//     Build Plan's Phase 2 acceptance criteria ("wires this wrapper around
//     `realEvictIsAlive` only on the real-adapter branch") — composing the
//     real `./lib/real-evict-adapters.mjs`'s `realEvictIsAlive` with
//     `./lib/reap-aware-is-alive.mjs`'s `reapAwareIsAlive`, with
//     `process.kill` mocked to simulate a transient zombie-then-reaped
//     sequence. This is the closest testable proxy to
//     `buildEvictIsAliveFn(false)` without importing `cli.mjs`; the
//     implementer's actual `cli.mjs` change is this exact composition,
//     confirmed by direct code reading rather than by this test importing
//     `cli.mjs`.
//   - Test B (fake path, testModeEnabled=true / `ARM_FAKE_IS_ALIVE_SEQUENCE`)
//     IS run through the real spawned `cli.mjs` process (this file's normal
//     idiom), and is an explicit regression guard: the fake-sequence branch
//     must keep consuming exactly one entry per logical `isAlive()` call,
//     completely unwrapped by the new retry helper, both before AND after
//     Phase 2 lands.
// ---------------------------------------------------------------------------

it("A. buildEvictIsAliveFn(false)'s real-adapter branch: realEvictIsAlive composed with reapAwareIsAlive retries a transient zombie-then-reaped process.kill sequence rather than trusting the first true reading", async () => {
  const { realEvictIsAlive } = await import('./lib/real-evict-adapters.mjs');
  const { reapAwareIsAlive } = await import('./lib/reap-aware-is-alive.mjs');

  // Simulates `process.kill(pid, 0)` succeeding (zombie, unreaped) twice,
  // then throwing ESRCH (reaped) on the third call — exactly the OS-timing
  // scenario its Investigation describes for `realEvictIsAlive`.
  const killSpy = jest
    .spyOn(process, 'kill')
    .mockImplementationOnce(() => {})
    .mockImplementationOnce(() => {})
    .mockImplementationOnce(() => {
      const error = new Error('kill ESRCH');
      error.code = 'ESRCH';
      throw error;
    });

  const sleepCalls = [];
  const sleep = async (ms) => {
    sleepCalls.push(ms);
  };

  // This is the exact composition `buildEvictIsAliveFn`'s real-adapter
  // branch (testModeEnabled === false) is required to wire, per the Build
  // Plan's Phase 2 acceptance criteria.
  const wired = reapAwareIsAlive({ isAlive: realEvictIsAlive, sleep, maxAttempts: 5, delayMs: 5, jitterMs: 5 });

  const result = await wired(12345);

  expect(result).toBe(false); // reaped, not still-alive
  expect(killSpy).toHaveBeenCalledTimes(3);
  // Retried, not busy-waited: a real (injected) sleep occurred between
  // attempts.
  expect(sleepCalls).toHaveLength(2);

  killSpy.mockRestore();
});

it("B. buildEvictIsAliveFn(true) (ARM_FAKE_IS_ALIVE_SEQUENCE) returns the EXACT unwrapped fake-sequence closure, consuming exactly one entry per logical isAlive() call — no extra sleep/retry calls consumed by the new Phase 2 wrapper", async () => {
  const ORCHESTRATOR_ID = 'orch-evict-fake-sequence-exact-consumption';
  const { leaseId, agentClass, commandRef } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const evictResult = runCli(
    [
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      '--evict-pid=9001',
      `--lease-id=${leaseId}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      // Exactly two entries — one per logical isAlive() call this scenario
      // makes (post-grace: still alive; post-SIGKILL: confirmed dead). If
      // the Phase 2 wrapper were mistakenly applied to this fake path, it
      // would consume additional entries per logical call (falling back to
      // the sequence's 'false' default once exhausted) and/or introduce
      // extra `sleep` calls, silently changing this outcome or ballooning
      // wall-clock time.
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'true,false',
      ARM_EVICT_GRACE_MS: '5',
    }),
  );

  expect(evictResult.status).toBe(0);
  const body = JSON.parse(evictResult.stdout);

  expect(body.type).toBe('evict');
  expect(body.outcome).toBe('killed-after-grace');
  expect(body.signalsSent).toEqual(['SIGTERM', 'SIGKILL']);

  const killLog = JSON.parse(await readFile(fakeKillLogPath, 'utf8'));
  expect(killLog).toEqual([
    { pid: 9001, signal: 'SIGTERM' },
    { pid: 9001, signal: 'SIGKILL' },
  ]);

  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0]).toMatchObject({ agentClass, commandRef, orchestratorId: ORCHESTRATOR_ID });
  expect(remaining[0].leaseId).toBeUndefined();
});
