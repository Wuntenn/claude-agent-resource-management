// Phase 3 — failing tests for wiring `getRealWorktreeMtimeMs` into
// `buildWatchdogCandidates` (scripts/agent-resource-management/cli.mjs).
//
// Placement decision: NEW file rather than folding into an existing
// `watchdog-beat*.jest.spec.mjs`. `watchdog-beat-real-worktree-mtime.jest.spec.mjs`
// is Phase 0's own asset (not to be modified — see its header) and is the
// vehicle for case 1 below; `watchdog-outer-acceptance.jest.spec.mjs` and
// `lib/watchdog-beat-pid-lease-binding.jest.spec.mjs` are both scoped to
// OTHER concerns (pid/lease pairing, recycle authorization) and drive
// `worktreeMtimeMs` entirely through the `ARM_FAKE_WORKTREE_MTIME_JSON` seam
// by design — adding real-scanner cases there would blur what each file is
// "the test for". This file is scoped narrowly to the real-scanner wiring
// itself (Phase 3's actual objective), covering the cases the existing red
// Phase 0 test intentionally leaves out (fresh/active worktree, the
// `ARM_EVICT_TEST_MODE=1`-but-no-fake-map fallthrough, seam-wins-over-real
// precedence, and per-candidate resilience to an unexpected scanner throw).
//
// CASE 1 (outer acceptance — genuinely stale worktree, real invocation, trips
// the stall detector and reaches the recycle path) is NOT duplicated here.
// `watchdog-beat-real-worktree-mtime.jest.spec.mjs` is that vehicle; it is
// currently RED for exactly the reason its own header documents
// (`buildWatchdogCandidates` unconditionally stubs `worktreeMtimeMs = null`).
// Nothing in this file re-asserts that scenario.
//
// JUDGMENT CALL — case 4 (`ARM_EVICT_TEST_MODE=1` set, `ARM_FAKE_WORKTREE_MTIME_JSON`
// ABSENT): the ticket asks us to decide, by analogy with
// `ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON`'s existing fallback in this same
// function. Reading `buildWatchdogCandidates` (cli.mjs ~5404-5417): when
// `evictTestModeEnabled` is true but `ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON` is
// UNSET, `fakeTrajectoryMap` resolves to `null` and the code falls through to
// reading the REAL, already-persisted footprint store — test mode does not
// synthesize a fake footprint value out of nothing; it only substitutes when
// the seam is explicitly populated. The worktree-mtime signal should follow
// the identical shape for consistency: `ARM_EVICT_TEST_MODE=1` +
// `ARM_FAKE_WORKTREE_MTIME_JSON` absent should fall through to the REAL
// scanner, not resolve to `null` in test mode. This is also the ONLY
// interpretation under which Phase 0's own outer acceptance test (which sets
// `ARM_EVICT_TEST_MODE=1` and omits `ARM_FAKE_WORKTREE_MTIME_JSON`) can ever
// pass — if test-mode-without-the-seam meant "resolve to null", Phase 0's own
// red test could never go green. We therefore treat cases 1 and 4 as THE SAME
// underlying condition (`evictTestModeEnabled=true`, fake mtime map absent) —
// Phase 0's file already covers the "trips on real stale mtime" direction of
// that condition; case B below covers the complementary "does NOT trip on a
// real ACTIVE mtime" direction, which doubles as this file's case-4 coverage.
//
// JUDGMENT CALL — case 2's literal ticket wording ("ARM_EVICT_TEST_MODE
// unset"): read literally this would require memory/disk/ps-tree collection
// to ALSO be real (those fakes are gated behind the SAME `evictTestModeEnabled`
// switch — see `collectPsOutput`/`collectMemoryAndDisk`'s own honor-fake-seam
// checks), which would make "flat footprint" impossible to arrange
// deterministically (`ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON` is itself gated
// behind `evictTestModeEnabled`) and the whole scenario host-dependent/flaky
// — contradicting Phase 0's own documented simplifications, which keep
// `ARM_EVICT_TEST_MODE=1` and make ONLY the worktree-mtime axis real. We
// follow Phase 0's precedent instead: `ARM_EVICT_TEST_MODE=1` throughout,
// `ARM_FAKE_WORKTREE_MTIME_JSON` omitted — i.e. real end-to-end for the
// worktree-mtime signal specifically, everything else faked exactly like
// Phase 0's file. Flagging this deviation from the literal ticket text
// explicitly, per the ticket's own instruction to flag judgment calls.
//
// Layer: case B is an integration test — real spawned `cli.mjs` via
// `spawnSync`, copying the CLI-invocation harness
// (`runCli`/`baseEnv`/`declareDibsFor`/`declareDibsWithPid`/`seedLeasedItem`)
// from `watchdog-beat-real-worktree-mtime.jest.spec.mjs`/
// `lib/watchdog-beat-pid-lease-binding.jest.spec.mjs` verbatim (file-local
// convenience in every sibling spec, not a shared module). Case C (seam
// precedence) reuses the same harness. Case D (per-candidate scanner-throw
// resilience) is a DIRECT unit test of `buildWatchdogCandidates` itself —
// see that test's own header comment for why the full-CLI-spawn approach
// cannot isolate "one candidate fails, others are unaffected" for this
// specific case, and why a direct import + dependency injection is the only
// way to pin it precisely.

import { jest } from '@jest/globals';
import { spawnSync, spawn, execFileSync } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { declareDibs } from './lib/coordination-file.mjs';
import { getRealWorktreeMtimeMs } from './lib/worktree-mtime-scanner.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const CLI = join(HERE, 'cli.mjs');

const MEMORY_RED = { pressureLevel: 1, swapUsedMb: 12288, compressedMb: 4096 };
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

const ORCHESTRATOR_ANCHOR_PID = 501;

/** Runs the real cli.mjs as a real child process — never imports it directly (matches every sibling spec). */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

let workDir;
let scratchRepoPath;
let queueFilePath;
let coordinationFilePath;
let fakeKillLogPath;
let liveLogFilePath;
let longLivedChild;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-watchdog-real-scanner-'));
  scratchRepoPath = join(workDir, 'scratch-repo');
  queueFilePath = join(workDir, 'queue.json');
  coordinationFilePath = join(workDir, 'coordination.json');
  fakeKillLogPath = join(workDir, 'fake-kill-log.json');
  liveLogFilePath = join(workDir, 'liveness-log.json');
  longLivedChild = undefined;
});

afterEach(async () => {
  if (longLivedChild && longLivedChild.exitCode === null && longLivedChild.signalCode === null) {
    try {
      longLivedChild.kill('SIGKILL');
    } catch {
      // Already gone — nothing to clean up.
    }
  }
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

/** Seeds one queue item and dequeues it, returning its leaseId — mirrors every sibling spec's `seedLeasedItem`. */
function seedLeasedItem({
  orchestratorId,
  agentClass = 'typescript-implementer',
  commandRef = 'resumable-workflow --resume=1501-phase-3',
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
  return { leaseId: dequeued.leaseId, agentClass, commandRef, orchestratorId };
}

/** Creates a disposable git repo, ready for a first commit. */
function seedScratchGitRepo(repoPath) {
  execFileSync('git', ['init', '-q', repoPath]);
  execFileSync('git', ['-C', repoPath, 'config', 'user.email', 'watchdog-mtime-test@example.invalid']);
  execFileSync('git', ['-C', repoPath, 'config', 'user.name', 'Watchdog Mtime Test']);
}

function psTreeWithRealPid(realPid) {
  return (
    `  PID  PPID    RSS     ELAPSED COMM\n` +
    `    1     0    512 1-03:46:39 /sbin/launchd\n` +
    `  ${ORCHESTRATOR_ANCHOR_PID}     1   1024 1-03:30:00 /usr/libexec/loginwindow\n` +
    ` ${realPid}   ${ORCHESTRATOR_ANCHOR_PID} 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude\n`
  );
}

// ---------------------------------------------------------------------------
// Case B — real ACTIVE worktree (fresh tracked-file mtime) does NOT trip the
// stall detector on the mtime axis, even with a flat footprint. Doubles as
// case 4's confirmation that `ARM_EVICT_TEST_MODE=1` + no
// `ARM_FAKE_WORKTREE_MTIME_JSON` falls through to the REAL scanner (if it
// instead resolved to `null` in test mode, this candidate would also not
// trip — but for the WRONG reason; the mtime-axis assertion below on its own
// cannot distinguish "correctly resolved fresh" from "stubbed null", so this
// test is a necessary-but-not-sufficient complement to Phase 0's stale-trips
// case, not a full case-4 proof by itself. Case D's direct-import mock proves
// the real scanner is actually CALLED under this same condition, which is
// the sufficient half).
//
// EXPECTED RED TODAY: it should not be — this scenario should already
// PASS today, because the current unconditional `worktreeMtimeMs = null`
// stub also never trips (null fails `isAgentStalled`'s AND-semantics). This
// case exists to PIN "does not trip" as a real behaviour so a future
// real-scanner implementation that (incorrectly) treats an unresolvable or
// borderline mtime as "stale" is caught, not to prove new red behaviour.
// ---------------------------------------------------------------------------
it(
  'a --watchdog-beat run against a REAL scratch git worktree with a FRESH commit does not trip the stall ' +
    'detector on the mtime axis, even though the footprint is flat (ARM_EVICT_TEST_MODE=1, ' +
    'ARM_FAKE_WORKTREE_MTIME_JSON absent — case 4 fall-through-to-real-scanner condition)',
  async () => {
    seedScratchGitRepo(scratchRepoPath);
    const trackedFilePath = join(scratchRepoPath, 'NOTES.md');
    await writeFile(trackedFilePath, 'fresh content\n');
    execFileSync('git', ['-C', scratchRepoPath, 'add', 'NOTES.md']);
    execFileSync('git', ['-C', scratchRepoPath, 'commit', '-q', '-m', 'fresh commit']);

    longLivedChild = spawn('/bin/sleep', ['30'], { cwd: scratchRepoPath, stdio: 'ignore' });
    const realPid = longLivedChild.pid;
    expect(typeof realPid).toBe('number');

    const ORCHESTRATOR_ID = 'orch-watchdog-active-mtime';
    seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
    await declareDibsWithPid(ORCHESTRATOR_ID, ORCHESTRATOR_ANCHOR_PID);

    const boundMs = 10 * 60 * 1000;
    // "now" stays close to real time (unlike Phase 0's file, which advances
    // it past the bound) — the fresh commit's real mtime is well within the
    // bound, so the mtime axis must resolve as NOT stale.
    const nowMs = Date.now();

    const beatResult = runCli(
      [
        '--watchdog-beat',
        `--orchestrator-id=${ORCHESTRATOR_ID}`,
        `--orchestrator-pid=${ORCHESTRATOR_ANCHOR_PID}`,
        `--watchdog-bound-ms=${boundMs}`,
      ],
      baseEnv({
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: psTreeWithRealPid(realPid),
        ARM_FAKE_NOW_MS: String(nowMs),
        ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON: JSON.stringify({ [String(realPid)]: [100, 100, 100, 100, 100] }),
        // Deliberately OMITTED: ARM_FAKE_WORKTREE_MTIME_JSON.
        ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
        ARM_FAKE_KILL_RESULT: '',
      }),
    );

    expect(beatResult.status).toBe(0);
    const body = JSON.parse(beatResult.stdout);

    expect(body.type).toBe('watchdog');
    expect(Array.isArray(body.trips)).toBe(true);
    const trip = body.trips.find((entry) => entry.pid === realPid);
    expect(trip).toBeUndefined();
    expect(Array.isArray(body.recycled)).toBe(true);
    expect(body.recycled).not.toContain(realPid);
  },
);

// ---------------------------------------------------------------------------
// Case C — ARM_EVICT_TEST_MODE=1 + ARM_FAKE_WORKTREE_MTIME_JSON set: the real
// scanner must NOT be consulted at all. Verified behaviourally: the real
// worktree is genuinely STALE (would trip if the real scanner were
// consulted), but the fake seam reports a FRESH value for the same pid — if
// the beat trips anyway, the seam was bypassed (regression). This is a
// regression guard for EXISTING behaviour, not new Phase 3 behaviour — the
// current code already special-cases `fakeMtimeMap` before falling through
// to the (currently-stubbed) real path, so this case is expected to PASS
// TODAY already, and must keep passing once the real scanner is wired in.
// ---------------------------------------------------------------------------
it(
  'a candidate whose REAL worktree would resolve stale still reports the FAKE (fresh) ' +
    'ARM_FAKE_WORKTREE_MTIME_JSON value and does not trip — the real scanner is never consulted ' +
    'when the seam is set (regression guard, expected green today and after Phase 3)',
  async () => {
    seedScratchGitRepo(scratchRepoPath);
    const trackedFilePath = join(scratchRepoPath, 'NOTES.md');
    await writeFile(trackedFilePath, 'initial content\n');
    execFileSync('git', ['-C', scratchRepoPath, 'add', 'NOTES.md']);
    execFileSync('git', ['-C', scratchRepoPath, 'commit', '-q', '-m', 'initial commit']);

    longLivedChild = spawn('/bin/sleep', ['30'], { cwd: scratchRepoPath, stdio: 'ignore' });
    const realPid = longLivedChild.pid;
    expect(typeof realPid).toBe('number');

    const ORCHESTRATOR_ID = 'orch-watchdog-seam-precedence';
    seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
    await declareDibsWithPid(ORCHESTRATOR_ID, ORCHESTRATOR_ANCHOR_PID);

    const boundMs = 10 * 60 * 1000;
    // "now" is advanced far past the real commit's mtime, exactly like
    // Phase 0's file — if the real scanner were (wrongly) consulted despite
    // the seam being set, this candidate WOULD read as stale and trip.
    const nowMs = Date.now() + boundMs + 5 * 60 * 1000;

    const beatResult = runCli(
      [
        '--watchdog-beat',
        `--orchestrator-id=${ORCHESTRATOR_ID}`,
        `--orchestrator-pid=${ORCHESTRATOR_ANCHOR_PID}`,
        `--watchdog-bound-ms=${boundMs}`,
      ],
      baseEnv({
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: psTreeWithRealPid(realPid),
        ARM_FAKE_NOW_MS: String(nowMs),
        ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON: JSON.stringify({ [String(realPid)]: [100, 100, 100, 100, 100] }),
        // FAKE seam reports a value close to "now" — i.e. NOT stale — for
        // the same pid whose real worktree mtime (seeded above) genuinely
        // IS stale relative to `nowMs`.
        ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ [String(realPid)]: nowMs - 1000 }),
        ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
        ARM_FAKE_KILL_RESULT: '',
      }),
    );

    expect(beatResult.status).toBe(0);
    const body = JSON.parse(beatResult.stdout);

    expect(body.type).toBe('watchdog');
    const trip = body.trips.find((entry) => entry.pid === realPid);
    expect(trip).toBeUndefined();
    expect(body.recycled).not.toContain(realPid);
  },
);

/**
 * Seeds `process.argv`/env with a trivially-valid `--desired-agents=0`
 * invocation BEFORE `cli.mjs` is imported, so `main()`'s synchronous
 * argument-validation prefix (which runs unconditionally as a side effect of
 * module evaluation — see the structural finding in Case D's header comment
 * above) does not `process.exit()` before this test can grab the module's
 * exports. Never awaits `main()` itself — only its synchronous validation
 * prefix runs before the dynamic `import()` this precedes resolves.
 */
async function bootstrapMinimalCliArgv() {
  const bootstrapDir = await mkdtemp(join(tmpdir(), 'arm-case-d-bootstrap-'));
  process.argv = ['node', 'cli.mjs', '--orchestrator-id=case-d-bootstrap', '--desired-agents=0'];
  process.env.ARM_QUEUE_FILE = join(bootstrapDir, 'queue.json');
  process.env.ARM_COORDINATION_FILE = coordinationFilePath;
  process.env.ARM_FAKE_COLLECT_JSON = JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN });
  delete process.env.ARM_FAKE_PS_OUTPUT;
  delete process.env.ARM_EVICT_TEST_MODE;
}

// ---------------------------------------------------------------------------
// Case D — one candidate's real-scanner call throwing unexpectedly must not
// abort the whole beat; that candidate's worktreeMtimeMs degrades to null,
// other candidates in the same beat are unaffected.
//
// WHY THIS IS A DIRECT UNIT TEST, NOT A FULL CLI SPAWN: `getRealWorktreeMtimeMs`'s
// own fake seam, `ARM_FAKE_WORKTREE_SCANNER_JSON` (worktree-mtime-scanner.mjs),
// is a single process-wide env var — it cannot be scoped to make ONE
// candidate pid throw while a SECOND candidate in the same beat resolves
// normally, because whatever value it holds applies uniformly to every
// `getRealWorktreeMtimeMs` call made during that process's lifetime. Proving
// "other candidates are unaffected" therefore requires calling
// `buildWatchdogCandidates` directly with per-candidate control over the
// scanner, via dependency injection — mirroring the SAME `deps = {}`
// convention `getRealWorktreeMtimeMs` itself already uses for
// `resolveProcessCwd` (see worktree-mtime-scanner.mjs's own header comment)
// and `watchdog-beat.mjs` uses elsewhere in this codebase.
//
// `buildWatchdogCandidates` is NOT currently exported from cli.mjs, and does
// not currently accept a `deps` parameter — the assertion below on
// `typeof buildWatchdogCandidates` is expected RED today
// (`cliModule.buildWatchdogCandidates` resolves to `undefined` — confirmed
// empirically; verified NOT to be a hard module-load `SyntaxError` because
// this test uses a namespace-style dynamic `import()` + destructure rather
// than a static named `import { buildWatchdogCandidates } from …`, which
// WOULD throw at parse time for a non-existent export). Making it pass is
// Phase 3 implementation work: export the function, and thread a
// `deps.getRealWorktreeMtimeMs` override through it exactly as
// `getRealWorktreeMtimeMs` itself threads `deps.resolveProcessCwd` — this is
// the interface this test asserts is needed for the defense-in-depth
// requirement to be provable at all, not an existing contract being
// observed.
//
// STRUCTURAL FINDING (not fixed here, flagging for the builder):
// `cli.mjs` calls `main().catch(...)` unconditionally at module scope with
// NO `import.meta.url`/`process.argv[1]` entrypoint guard (verified: no such
// guard exists anywhere in the file, cli.mjs:7578 is the last line). Every
// import of this module — dynamic or static, from a test or otherwise —
// synchronously runs `main()`'s argument-parsing prefix as a side effect of
// module evaluation. A prior version of this test attempted a bare
// `await import('./cli.mjs')` with no bootstrap and got
// `process.exit(2)` (`--orchestrator-id=<id> is required`) surfaced by Jest
// as a test failure, purely from importing the module with no CLI args
// prepared — nothing to do with `buildWatchdogCandidates` itself. This test
// works around it (see `bootstrapMinimalCliArgv()` below) by seeding
// `process.argv`/env with a trivially-valid `--desired-agents=0` invocation
// before importing, exploiting the fact `main()` is invoked without a
// top-level `await` (`main().catch(...)`, not `await main().catch(...)`) so
// module evaluation — and therefore this dynamic `import()` — completes
// synchronously up to `main()`'s first `await`, well before `main()`'s own
// async body finishes running in the background. This works today but is a
// workaround for a structural gap, not a sanctioned test pattern; the
// builder may want an explicit entrypoint guard as follow-up (out of this
// ticket's scope — not actioned here).
//
// SEPARATELY — a genuine bug worth flagging to the Phase 3 implementer, not
// fixed here: `getRealWorktreeMtimeMs`'s own "never throws" contract
// (worktree-mtime-scanner.mjs's header comment, and its per-function doc
// comment) is violated by its OWN fake-seam handling. The `JSON.parse(process.env.ARM_FAKE_WORKTREE_SCANNER_JSON)`
// call at worktree-mtime-scanner.mjs:97 sits OUTSIDE the `try { … } catch`
// block that starts at line 102 — an invalid-JSON value for that env var
// throws synchronously out of `getRealWorktreeMtimeMs` itself, contradicting
// its own documented "Never throws" contract. This is exactly the failure
// mode case 5 asks the CLI-layer wiring to defend against regardless (a
// scanner call throwing "unexpectedly, even though the contract says it
// never throws") — this test's injected-deps mock reproduces that failure
// mode directly rather than relying on the real bug, so it stays valid
// whether or not that separate bug is ever fixed. Flagging file:line for the
// builder: scripts/agent-resource-management/lib/worktree-mtime-scanner.mjs:96-100.
// ---------------------------------------------------------------------------
it(
  'one candidate whose injected getRealWorktreeMtimeMs throws degrades to worktreeMtimeMs: null without ' +
    'affecting a second candidate in the same beat (defense in depth; requires buildWatchdogCandidates to ' +
    'be exported with deps injection — currently RED at import time)',
  async () => {
    await bootstrapMinimalCliArgv();
    const cliModule = await import('./cli.mjs');
    const { buildWatchdogCandidates } = cliModule;
    expect(typeof buildWatchdogCandidates).toBe('function');

    const THROWING_PID = 9101;
    const HEALTHY_PID = 9102;
    const HEALTHY_MTIME_MS = Date.now() - 1000;

    const savedEvictTestMode = process.env.ARM_EVICT_TEST_MODE;
    const savedCoordinationFile = process.env.ARM_COORDINATION_FILE;
    const savedFakePsOutput = process.env.ARM_FAKE_PS_OUTPUT;
    // bot-review fix — `buildWatchdogCandidates`'s own `collectPsOutput()`
    // call is now gated behind `evictTestModeEnabled` (production security
    // posture: an unconditional call would let a leaked `ARM_FAKE_PS_OUTPUT`
    // corrupt real candidate discovery). `ARM_EVICT_TEST_MODE=1` mirrors every
    // other test in this codebase that needs a fake ps-tree in a real
    // `evictTestModeEnabled`-gated call site; the `evictTestModeEnabled: true`
    // passed to `buildWatchdogCandidates` below is what actually satisfies the
    // gate (this direct-import call bypasses `main()`'s own env-var parsing),
    // and has no bearing on this case's actual assertion — the injected
    // `deps.getRealWorktreeMtimeMs` double below ignores its second argument
    // entirely, so the scanner-side behaviour this case pins is unaffected.
    process.env.ARM_EVICT_TEST_MODE = '1';
    process.env.ARM_COORDINATION_FILE = coordinationFilePath;
    process.env.ARM_FAKE_PS_OUTPUT =
      `  PID  PPID    RSS     ELAPSED COMM\n` +
      `    1     0    512 1-03:46:39 /sbin/launchd\n` +
      `  ${ORCHESTRATOR_ANCHOR_PID}     1   1024 1-03:30:00 /usr/libexec/loginwindow\n` +
      ` ${THROWING_PID}   ${ORCHESTRATOR_ANCHOR_PID} 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude\n` +
      ` ${HEALTHY_PID}   ${ORCHESTRATOR_ANCHOR_PID} 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude\n`;

    try {
      const fakeGetRealWorktreeMtimeMs = async (pid) => {
        if (pid === THROWING_PID) {
          throw new Error('simulated unexpected scanner failure');
        }
        if (pid === HEALTHY_PID) {
          return HEALTHY_MTIME_MS;
        }
        throw new Error(`unexpected pid in test double: ${pid}`);
      };

      const candidates = await buildWatchdogCandidates({
        orchestratorPid: ORCHESTRATOR_ANCHOR_PID,
        now: Date.now(),
        evictTestModeEnabled: true,
        footprintStateFilePath: join(workDir, 'footprint-state.json'),
        deps: { getRealWorktreeMtimeMs: fakeGetRealWorktreeMtimeMs },
      });

      expect(Array.isArray(candidates)).toBe(true);

      const throwingCandidate = candidates.find((c) => c.pid === THROWING_PID);
      const healthyCandidate = candidates.find((c) => c.pid === HEALTHY_PID);

      expect(throwingCandidate).toBeDefined();
      expect(throwingCandidate.worktreeMtimeMs).toBeNull();

      expect(healthyCandidate).toBeDefined();
      expect(healthyCandidate.worktreeMtimeMs).toBe(HEALTHY_MTIME_MS);
    } finally {
      if (savedEvictTestMode === undefined) delete process.env.ARM_EVICT_TEST_MODE;
      else process.env.ARM_EVICT_TEST_MODE = savedEvictTestMode;
      if (savedCoordinationFile === undefined) delete process.env.ARM_COORDINATION_FILE;
      else process.env.ARM_COORDINATION_FILE = savedCoordinationFile;
      if (savedFakePsOutput === undefined) delete process.env.ARM_FAKE_PS_OUTPUT;
      else process.env.ARM_FAKE_PS_OUTPUT = savedFakePsOutput;
    }
  },
);

// ---------------------------------------------------------------------------
// Case E (review, High) — `buildWatchdogCandidates` must thread
// `evictTestModeEnabled` through to the REAL scanner's own `honorFakeSeam`
// argument (`getRealWorktreeMtimeMs(pid, honorFakeSeam)`,
// worktree-mtime-scanner.mjs), exactly like the two OTHER fake seams this
// same function already gates (`ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON`,
// `ARM_FAKE_WORKTREE_MTIME_JSON`).
//
// Unlike cases A-D above (which exercise the CLI-layer
// `ARM_FAKE_WORKTREE_MTIME_JSON` seam through a full `runCli` spawn, with no
// `deps` override at all), THIS case is a direct import-and-call unit test of
// `buildWatchdogCandidates` that DOES override `deps.getRealWorktreeMtimeMs`
// — with a `jest.fn()` spy, not a hand-rolled wrapper. The spy's mock
// implementation still delegates to the real, unmocked
// `getRealWorktreeMtimeMs` import (forwarding whatever arguments it is
// actually called with, never a hardcoded value), so the scanner's real
// seam-honouring behaviour is still exercised end-to-end — but the point of
// the spy is to let this test assert on the exact arguments
// `buildWatchdogCandidates` passes at its real call site
// (`resolveRealWorktreeMtimeMs(pid, evictTestModeEnabled)`), which is what
// actually proves the wiring. An earlier version of this test used a wrapper
// that discarded the `evictTestModeEnabled` argument and substituted a
// hardcoded boolean of its own; that could not have caught a regression in
// the wiring itself (see re-review) — the
// `expect(spy).toHaveBeenCalledWith(...)` assertion below is what closes
// that gap.
//
// `ARM_FAKE_WORKTREE_SCANNER_JSON` is that scanner's OWN fake seam (distinct
// from the CLI-layer `ARM_FAKE_WORKTREE_MTIME_JSON` seam exercised by cases
// A-D above).
//
// bot-review fix — `buildWatchdogCandidates`'s own `collectPsOutput()`
// call is gated behind the SAME `evictTestModeEnabled` flag that reaches
// `resolveRealWorktreeMtimeMs` (production security posture: an
// unconditional call would let a leaked `ARM_FAKE_PS_OUTPUT` corrupt real
// candidate discovery). That couples candidate discovery to the very flag
// this case is proving the threading of: `evictTestModeEnabled: false` would
// starve `collectPsOutput` of `ARM_FAKE_PS_OUTPUT`, so `CANDIDATE_PID` would
// never be discovered as owned by the orchestrator and the spy would never
// fire — there is no way, in this direct-call harness, to exercise the
// `evictTestModeEnabled: false` branch of the wiring while still discovering
// a candidate. This case therefore pins `evictTestModeEnabled: true` and
// proves the spy observes exactly that value forwarded, unchanged, to the
// real scanner call; the scanner's own `honorFakeSeam=false` (ignore the
// seam) behaviour is covered directly, independent of
// `buildWatchdogCandidates`'s ps-gate constraints, by
// `worktree-mtime-scanner.jest.spec.ts`.
// ---------------------------------------------------------------------------
it(
  'buildWatchdogCandidates threads its own evictTestModeEnabled argument, unchanged, into ' +
    "getRealWorktreeMtimeMs's honorFakeSeam parameter at the real call site — proven via a jest.fn " +
    "spy that observes the actual call arguments (not a hardcoded stand-in) — and that argument " +
    "correctly gates the real scanner's own ARM_FAKE_WORKTREE_SCANNER_JSON fake seam end-to-end",
  async () => {
    await bootstrapMinimalCliArgv();
    const cliModule = await import('./cli.mjs');
    const { buildWatchdogCandidates } = cliModule;
    expect(typeof buildWatchdogCandidates).toBe('function');

    const CANDIDATE_PID = 9901;
    const FAKE_MTIME_MS = 123456789;

    const savedEvictTestMode = process.env.ARM_EVICT_TEST_MODE;
    const savedCoordinationFile = process.env.ARM_COORDINATION_FILE;
    const savedFakePsOutput = process.env.ARM_FAKE_PS_OUTPUT;
    const savedFakeScannerJson = process.env.ARM_FAKE_WORKTREE_SCANNER_JSON;
    process.env.ARM_EVICT_TEST_MODE = '1';
    process.env.ARM_COORDINATION_FILE = coordinationFilePath;
    process.env.ARM_FAKE_PS_OUTPUT =
      `  PID  PPID    RSS     ELAPSED COMM\n` +
      `    1     0    512 1-03:46:39 /sbin/launchd\n` +
      `  ${ORCHESTRATOR_ANCHOR_PID}     1   1024 1-03:30:00 /usr/libexec/loginwindow\n` +
      ` ${CANDIDATE_PID}   ${ORCHESTRATOR_ANCHOR_PID} 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude\n`;
    process.env.ARM_FAKE_WORKTREE_SCANNER_JSON = JSON.stringify({ [String(CANDIDATE_PID)]: FAKE_MTIME_MS });

    try {
      const params = {
        orchestratorPid: ORCHESTRATOR_ANCHOR_PID,
        now: Date.now(),
        footprintStateFilePath: join(workDir, 'footprint-state.json'),
        // `evictTestModeEnabled: true` is the only value the ps-fake gate
        // (see header comment above) permits in this direct-call harness —
        // `false` here would also starve `collectPsOutput` of
        // `ARM_FAKE_PS_OUTPUT`, so CANDIDATE_PID would never be discovered
        // as owned by the orchestrator and the spy below would never fire.
        evictTestModeEnabled: true,
      };

      // A jest.fn() spy, not a wrapper that discards its second argument:
      // its mock implementation forwards whatever it is actually called
      // with (not a hardcoded value) to the real, unmocked
      // getRealWorktreeMtimeMs, so the seam's honour behaviour is still
      // exercised end-to-end AND the exact argument buildWatchdogCandidates
      // passes is observable for assertion below.
      const getRealWorktreeMtimeMsSpy = jest.fn((pid, honorFakeSeam) =>
        getRealWorktreeMtimeMs(pid, honorFakeSeam),
      );
      const candidates = await buildWatchdogCandidates({
        ...params,
        deps: { getRealWorktreeMtimeMs: getRealWorktreeMtimeMsSpy },
      });
      const candidate = candidates.find((c) => c.pid === CANDIDATE_PID);
      expect(candidate).toBeDefined();
      // The core wiring assertion (re-review) — buildWatchdogCandidates
      // must call the scanner with its OWN evictTestModeEnabled value, not a
      // hardcoded stand-in for it. A regression that hardcodes the call site
      // (e.g. `resolveRealWorktreeMtimeMs(pid, false)` regardless of the
      // caller's evictTestModeEnabled) makes the spy observe (pid, false)
      // instead, failing this assertion.
      expect(getRealWorktreeMtimeMsSpy).toHaveBeenCalledWith(CANDIDATE_PID, true);
      // With honorFakeSeam threaded through as true, the fake seam IS
      // honored end-to-end (no synthetic result is injected — the real,
      // unmocked getRealWorktreeMtimeMs decides). The converse
      // (honorFakeSeam=false ignores the seam) is covered directly, in
      // isolation from buildWatchdogCandidates's ps-gate constraints, by
      // worktree-mtime-scanner.jest.spec.ts.
      expect(candidate.worktreeMtimeMs).toBe(FAKE_MTIME_MS);
    } finally {
      if (savedEvictTestMode === undefined) delete process.env.ARM_EVICT_TEST_MODE;
      else process.env.ARM_EVICT_TEST_MODE = savedEvictTestMode;
      if (savedCoordinationFile === undefined) delete process.env.ARM_COORDINATION_FILE;
      else process.env.ARM_COORDINATION_FILE = savedCoordinationFile;
      if (savedFakePsOutput === undefined) delete process.env.ARM_FAKE_PS_OUTPUT;
      else process.env.ARM_FAKE_PS_OUTPUT = savedFakePsOutput;
      if (savedFakeScannerJson === undefined) delete process.env.ARM_FAKE_WORKTREE_SCANNER_JSON;
      else process.env.ARM_FAKE_WORKTREE_SCANNER_JSON = savedFakeScannerJson;
    }
  },
);
