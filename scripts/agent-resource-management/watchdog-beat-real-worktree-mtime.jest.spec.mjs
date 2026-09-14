// Phase 0 — outer acceptance test for a REAL worktree-mtime scan
// wired into `--watchdog-beat`.
//
// Every OTHER `watchdog-*`/`cli-evict*` spec in this skill drives its
// candidate's `worktreeMtimeMs` entirely through the `ARM_FAKE_WORKTREE_MTIME_JSON`
// test seam (see ./watchdog-outer-acceptance.jest.spec.mjs and
// ./lib/watchdog-beat-pid-lease-binding.jest.spec.mjs) — a synthetic pid that
// never exists on the host, paired with a hand-picked fake mtime. That is
// deliberate for THOSE files: they exist to pin behaviour once the signal is
// already resolved, not to prove the signal gets resolved in the first
// place.
//
// THIS file is the other half: it omits `ARM_FAKE_WORKTREE_MTIME_JSON`
// entirely and requires the beat to resolve `worktreeMtimeMs` for a REAL
// process against a REAL disposable git worktree — i.e. it exercises
// Phase 1 (pid-to-cwd resolver), Phase 2 (git-tracked-mtime scanner), and
// Phase 3 (wiring both into `buildWatchdogCandidates`) end-to-end. Footprint
// stays a test seam (`ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON`) — only the
// worktree-mtime side needs to be real for this ticket.
//
// SIMPLIFICATIONS DOCUMENTED HERE FOR PHASE 3'S IMPLEMENTER (Phase 0 constraint —
// "if you cannot get a fully real end-to-end version working robustly, write
// the most realistic version you can and document the simplification"):
//
//   1. Ownership/ancestry (`isEvictionAuthorized`) is still resolved via the
//      `ARM_FAKE_PS_OUTPUT` seam, exactly like every sibling spec — this
//      test fabricates a ps-tree row whose PID equals the REAL spawned
//      child's pid (so the ownership walk "sees" it as claude-rooted and
//      parented under the given `--orchestrator-pid`), but the ps TEXT
//      itself is still synthetic. The only thing required to be genuinely
//      real is (a) the pid, which must belong to a live OS process for
//      Phase 1's pid-to-cwd resolver to succeed, and (b) that process's
//      actual OS-level cwd, which must resolve to the scratch git repo for
//      Phase 2's scanner to find real tracked files. A fully-real ps-tree
//      snapshot (shelling out to a real `ps -Ao pid,ppid,rss,etime,comm` and
//      hoping a `/…/claude`-suffixed row exists at test time) would be
//      flaky/host-dependent and buys nothing extra here, since ownership
//      resolution is an already-covered, orthogonal concern.
//   2. Staleness is arranged by faking "now" far in the future
//      (`ARM_FAKE_NOW_MS` / `ARM_BEAT_NOW_TEST_MODE`, the same seam every
//      sibling watchdog spec already uses for its own bound arithmetic)
//      rather than by actually waiting `--watchdog-bound-ms` of real wall
//      time. The scratch repo's tracked file's mtime is genuinely real
//      (set by the real `git commit`/filesystem write moments before this
//      beat runs) — only the beat's own notion of "now" is advanced past it
//      by more than the bound, which is exactly what a real long-idle agent
//      looks like from the scanner's point of view.
//   3. The long-lived process standing in for "the agent" is a real `sleep`
//      child spawned with `cwd` set to the scratch repo — not an actual
//      Claude Code process — since this test only needs a real, live OS pid
//      whose cwd the future `resolveProcessCwd` can resolve; it does not
//      need to BE a Claude process for this test's own purposes (the
//      claude-rooted-comm check is satisfied via the fake ps-tree row
//      instead, per simplification 1).
//
// Layer: integration — spawns the real `cli.mjs` via `spawnSync`, mirroring
// ./lib/watchdog-beat-pid-lease-binding.jest.spec.mjs's own CLI-invocation
// harness (`runCli`/`baseEnv`/`declareDibsFor`/`declareDibsWithPid`/
// `seedLeasedItemNeverBound`), copied here rather than imported since these
// helpers are file-local conveniences, not a shared module, in every
// existing sibling spec.
//
// EXPECTED RED TODAY, for exactly one reason: `buildWatchdogCandidates` in
// cli.mjs unconditionally sets `worktreeMtimeMs = null` whenever
// `ARM_FAKE_WORKTREE_MTIME_JSON` is not supplied (see cli.mjs's own doc
// comment on that function: "worktree-mtime scanning has no real
// implementation yet in this phase … always resolves to `null`"). Because
// `isAgentStalled` requires BOTH footprint-flatness AND mtime-staleness
// (AND semantics, never OR), a `null` mtime means this beat can never trip
// on this scenario until Phases 1-3 land — `body.trips` stays empty and the
// candidate pid is never recycled.

import { spawnSync, spawn, execFileSync } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { declareDibs } from './lib/coordination-file.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

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
  workDir = await mkdtemp(join(tmpdir(), 'arm-watchdog-real-mtime-'));
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

/** Creates a disposable git repo with one real, tracked, already-committed file. */
function seedScratchGitRepo(repoPath) {
  execFileSync('git', ['init', '-q', repoPath]);
  execFileSync('git', ['-C', repoPath, 'config', 'user.email', 'watchdog-mtime-test@example.invalid']);
  execFileSync('git', ['-C', repoPath, 'config', 'user.name', 'Watchdog Mtime Test']);
}

it(
  'a --watchdog-beat run against a REAL scratch git worktree (no ARM_FAKE_WORKTREE_MTIME_JSON) resolves ' +
    'a non-null worktreeMtimeMs and trips the stall detector when both footprint is flat and the worktree ' +
    'has had no tracked-file writes for longer than the bound',
  async () => {
    seedScratchGitRepo(scratchRepoPath);
    const trackedFilePath = join(scratchRepoPath, 'NOTES.md');
    await writeFile(trackedFilePath, 'initial content\n');
    execFileSync('git', ['-C', scratchRepoPath, 'add', 'NOTES.md']);
    execFileSync('git', ['-C', scratchRepoPath, 'commit', '-q', '-m', 'initial commit']);

    // A real, long-lived OS process whose OWN cwd (not a stored path) is the
    // scratch repo — Phase 1's pid-to-cwd resolver must resolve this via the
    // OS from the pid alone, exactly as it would for a real orchestrated
    // agent. `spawnSync('sleep', ...)` would not give us a stable pid to
    // interrogate afterwards; a real long-lived `spawn` child is required.
    longLivedChild = spawn('/bin/sleep', ['30'], { cwd: scratchRepoPath, stdio: 'ignore' });
    const realPid = longLivedChild.pid;
    expect(typeof realPid).toBe('number');

    const ORCHESTRATOR_ID = 'orch-watchdog-real-mtime';
    seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
    await declareDibsWithPid(ORCHESTRATOR_ID, ORCHESTRATOR_ANCHOR_PID);

    // Ownership/ancestry is still resolved via the fake ps-tree seam
    // (simplification 1, documented at the top of this file) — the real
    // child's pid stands in for the "claude-rooted" candidate row.
    const psOutputWithRealPid =
      `  PID  PPID    RSS     ELAPSED COMM\n` +
      `    1     0    512 1-03:46:39 /sbin/launchd\n` +
      `  ${ORCHESTRATOR_ANCHOR_PID}     1   1024 1-03:30:00 /usr/libexec/loginwindow\n` +
      ` ${realPid}   ${ORCHESTRATOR_ANCHOR_PID} 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude\n`;

    const boundMs = 10 * 60 * 1000;
    // Simplification 2 (documented at the top of this file): "now" is faked
    // far enough past the scratch file's REAL, just-written mtime that the
    // real scanner (once built) would classify it stale, without this test
    // needing to actually wait `boundMs` of real wall-clock time.
    //
    // The +2min buffer (not +5min) is deliberate: `declareDibsWithPid` above
    // stamps the orchestrator's dibs entry at the REAL wall clock, and
    // `readDibs`'s own liveness check prunes any dibs entry older than
    // `DEFAULT_LIVENESS_THRESHOLD_MS` (15 minutes) as of this same faked
    // `nowMs`. A +5min buffer put `nowMs` at exactly `boundMs + 5min = 15min`
    // past the dibs declaration -- equal to the threshold -- and this test's
    // own real per-call overhead (a few ms between the dibs write and the
    // beat reading it back) was enough to tip the dibs entry's age
    // fractionally PAST 15 minutes, causing `readDibs` to prune it and
    // `orchestratorIsLive` to resolve `false` before ps-tree ancestry was
    // ever consulted -- an unrelated liveness-pruning failure masquerading
    // as a stall-detection failure. +2min keeps `nowMs` comfortably past
    // `boundMs` (still stale for the mtime signal) while leaving a 3-minute
    // margin below the 15-minute liveness threshold.
    const nowMs = Date.now() + boundMs + 2 * 60 * 1000;

    const beatResult = runCli(
      [
        '--watchdog-beat',
        `--orchestrator-id=${ORCHESTRATOR_ID}`,
        `--orchestrator-pid=${ORCHESTRATOR_ANCHOR_PID}`,
        `--watchdog-bound-ms=${boundMs}`,
      ],
      baseEnv({
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: psOutputWithRealPid,
        ARM_FAKE_NOW_MS: String(nowMs),
        ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON: JSON.stringify({ [String(realPid)]: [100, 100, 100, 100, 100] }),
        // Deliberately OMITTED: ARM_FAKE_WORKTREE_MTIME_JSON — the whole
        // point of this test is that the mtime signal must be resolved for
        // real, not faked.
        ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
        ARM_FAKE_KILL_RESULT: '',
      }),
    );

    expect(beatResult.status).toBe(0);
    const body = JSON.parse(beatResult.stdout);

    expect(body.type).toBe('watchdog');
    expect(Array.isArray(body.trips)).toBe(true);
    // RED TODAY: `buildWatchdogCandidates` stubs `worktreeMtimeMs = null`
    // unconditionally when `ARM_FAKE_WORKTREE_MTIME_JSON` is absent, and
    // `isAgentStalled` never trips on a null mtime (AND-semantics with
    // footprint flatness) — so `body.trips` is empty and this fails here,
    // for the RIGHT reason, until Phases 1-3 land.
    const trip = body.trips.find((entry) => entry.pid === realPid);
    expect(trip).toBeDefined();
    expect(trip.reason).toBe('stalled');

    expect(Array.isArray(body.recycled)).toBe(true);
    expect(body.recycled).toContain(realPid);
  },
);
