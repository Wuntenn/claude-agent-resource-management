// Phase 1 unit/integration tests for ("agent-resource-management:
// launchd LaunchAgent as OS-level backstop for the watchdog/EVICT beat").
//
// Companion to the already-committed Phase 0 outer acceptance test,
// ./watchdog-beat-launchd-outer.jest.spec.mjs — read that file first for the
// wrapper's pinned contract:
//
//   node lib/watchdog-beat-launchd.mjs   (no CLI args — launchd's
//                                          ProgramArguments invoke it bare)
//
//   (1) resolves its OWN process pid,
//   (2) invokes the existing `cli.mjs --watchdog-beat` with a fixed
//       synthetic `--orchestrator-id` and `--orchestrator-pid=<its own pid>`,
//   (3) NEVER lets an internal error, or the invoked beat exiting non-zero,
//       propagate as a wrapper crash — always exits 0, catching and logging
//       to stderr with a `watchdog-beat-launchd`-tagged marker.
//
// This file drills into that contract at a more granular level than Phase
// 0's two coarse scenarios. Per this ticket's own Phase 1 test-strategy
// note, each scenario below either targets an angle Phase 0 does NOT cover,
// or is explicitly skipped as a deliberate non-duplication (see the comment
// at each skip point).
//
// RED by construction, right now: `scripts/agent-resource-management/lib/
// watchdog-beat-launchd.mjs` does not exist yet (same premise as Phase 0's
// own file — confirmed via `ls scripts/agent-resource-management/lib/` as
// of this commit). Every scenario below fails for that reason (spawnSync
// reports a Node "Cannot find module"/non-zero exit) until Phase 1's
// implementer lands the wrapper.
//
// Do not add a stub implementation to make this pass — that defeats the
// point of the phase red-green loop. Phase 1's implementer turns this file
// green by actually building the wrapper against the contract Phase 0 (and
// this file) pin.
//
// ---------------------------------------------------------------------------
// A NEW TEST SEAM THIS FILE INTRODUCES, for the "invokes cli.mjs with the
// right argv" scenario only:
//
//   ARM_WATCHDOG_LAUNCHD_CLI_PATH — when set, the wrapper must invoke THIS
//   path instead of its own default sibling `../cli.mjs` resolution, when
//   spawning the beat. This mirrors every other `ARM_*_FILE` override
//   already documented in cli.mjs (`ARM_DEADLOCK_STATE_FILE`,
//   `ARM_WATCHDOG_BACKOFF_FILE`, etc.) — an explicit override always wins,
//   letting this test substitute a tiny stand-in script that dumps its own
//   `process.argv` to a file rather than spawning the real, much heavier
//   `cli.mjs`. Not honoured by any real code yet — Phase 1's implementer
//   adds it as part of landing the wrapper's own spawn call.
//
// The other scenarios below invoke the REAL `cli.mjs` (no override), mirroring
// Phase 0's own posture, and (for the deadlock scenario) use a well-known
// shell `exec` trick — `sh -c 'ANCHOR=$$; export ARM_FAKE_PS_OUTPUT="...
// $ANCHOR..."; exec node <wrapper>'` — so the fake ps-tree fixture can
// embed the wrapper's own about-to-be-resolved pid BEFORE it exists from
// this test's own point of view (`exec` replaces the shell process image
// in place, so the wrapper's `process.pid` is byte-for-byte the same `$$`
// the shell computed for itself moments earlier).
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { readLivenessLog } from './lib/liveness-log.mjs';

const WRAPPER = join(dirname(fileURLToPath(import.meta.url)), 'lib', 'watchdog-beat-launchd.mjs');

const MEMORY_RED = { pressureLevel: 1, swapUsedMb: 12288, compressedMb: 4096 };
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

let workDir;
let livenessLogFilePath;
let coordinationFilePath;
let queueFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-watchdog-launchd-unit-'));
  livenessLogFilePath = join(workDir, 'liveness-log.json');
  coordinationFilePath = join(workDir, 'coordination.json');
  queueFilePath = join(workDir, 'queue.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

function baseEnv(extra = {}) {
  return {
    ...process.env,
    // post-merge review, Medium — every test's env is rooted here, not
    // just the backoff-isolation scenario below, so `homedir()`-derived
    // defaults (`resolveDefaultIsolatedBackoffFilePath` in the wrapper) never
    // resolve against a real developer machine's `~/.claude/agent-state/`.
    // Deliberately does NOT set `ARM_WATCHDOG_BACKOFF_FILE` here — that would
    // flip every test into the wrapper's "caller already set it" branch and
    // stop exercising its own reset-path logic.
    HOME: join(workDir, 'fake-home'),
    ARM_LIVENESS_LOG_FILE: livenessLogFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_QUEUE_FILE: queueFilePath,
    ...extra,
  };
}

/** Runs the real wrapper script as a real child process — mirrors Phase 0's own `runWrapper`. */
function runWrapper(extraEnv) {
  const result = spawnSync('node', [WRAPPER], { encoding: 'utf8', env: baseEnv(extraEnv) });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '', pid: result.pid };
}

/** Runs the real cli.mjs directly (never the wrapper) — this file's own setup helper, mirrors every sibling spec. */
function runCliDirect(args, extraEnv) {
  const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');
  const result = spawnSync('node', [CLI, ...args], { encoding: 'utf8', env: baseEnv(extraEnv) });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

// ---------------------------------------------------------------------------
// 1. argv contract — own pid as --orchestrator-pid, a synthetic
//    --orchestrator-id, and --watchdog-beat itself.
// ---------------------------------------------------------------------------

const FAKE_CLI_SOURCE = `
import { writeFileSync } from 'node:fs';
writeFileSync(process.env.FAKE_CLI_ARGV_DUMP_PATH, JSON.stringify(process.argv.slice(2)));
process.stdout.write(JSON.stringify({ type: 'watchdog', trips: [], deadlockTripped: false, agingEscalations: [], recycled: [] }) + '\\n');
process.exit(0);
`;

it('invokes cli.mjs --watchdog-beat with own pid as --orchestrator-pid and a synthetic --orchestrator-id', async () => {
  const fakeCliPath = join(workDir, 'fake-cli.mjs');
  const argvDumpPath = join(workDir, 'argv-dump.json');
  await writeFile(fakeCliPath, FAKE_CLI_SOURCE);

  const result = runWrapper({
    ARM_WATCHDOG_LAUNCHD_CLI_PATH: fakeCliPath,
    FAKE_CLI_ARGV_DUMP_PATH: argvDumpPath,
  });

  expect(result.status).toBe(0);

  const argv = JSON.parse(await readFile(argvDumpPath, 'utf8'));
  expect(argv).toContain('--watchdog-beat');

  const orchestratorIdArg = argv.find((entry) => entry.startsWith('--orchestrator-id='));
  expect(orchestratorIdArg).toBeDefined();
  const syntheticId = orchestratorIdArg.slice('--orchestrator-id='.length);
  expect(syntheticId.length).toBeGreaterThan(0);
  // "synthetic" per the ticket's own vocabulary — not asserting the exact
  // literal (Phase 1's implementer is free to name it), only that it reads
  // as this wrapper's own identity, mirroring Phase 0's own
  // `watchdog-beat-launchd`-tagged stderr marker convention.
  expect(syntheticId).toMatch(/launchd/i);

  const orchestratorPidArg = argv.find((entry) => entry.startsWith('--orchestrator-pid='));
  expect(orchestratorPidArg).toBeDefined();
  const pid = Number(orchestratorPidArg.slice('--orchestrator-pid='.length));
  expect(Number.isInteger(pid)).toBe(true);
  expect(pid).toBeGreaterThan(0);
  // The load-bearing assertion: the pid passed as --orchestrator-pid is
  // genuinely the WRAPPER's own process pid — `result.pid` is the pid
  // `spawnSync` assigned to the `node <wrapper>` process itself, the exact
  // process whose `process.pid` the wrapper is contracted to resolve.
  expect(pid).toBe(result.pid);
});

// ---------------------------------------------------------------------------
// 1b. The signal-termination failure mode — the one branch of
//     `runWatchdogBeatLaunchd`'s three-way `result.error` /
//     `result.signal` / non-zero-`result.status` triage
//     (lib/watchdog-beat-launchd.mjs) that neither this file's other
//     scenarios nor the Phase 0 outer spec ever exercises. Points
//     `ARM_WATCHDOG_LAUNCHD_CLI_PATH` at a fake CLI that kills ITSELF by
//     signal (mirrors this file's existing `FAKE_CLI_SOURCE` seam/style
//     above) — `spawnSync` then reports `result.signal` rather than
//     `result.status`, exactly like a real launchd-invoked beat process
//     being killed out from under it. The WRAPPER (not the fake CLI) must
//     still exit 0 and log the `watchdog-beat-launchd`-tagged marker
//     mentioning the signal — this is the "never restart-loop" contract
//     the whole wrapper exists to guarantee.
// ---------------------------------------------------------------------------

const SELF_SIGNAL_CLI_SOURCE = `process.kill(process.pid, 'SIGTERM');\n`;

it('still exits 0, with a signal-tagged marker on stderr, when the invoked beat is terminated by a signal', async () => {
  const fakeCliPath = join(workDir, 'fake-cli-self-signal.mjs');
  await writeFile(fakeCliPath, SELF_SIGNAL_CLI_SOURCE);

  const result = runWrapper({
    ARM_WATCHDOG_LAUNCHD_CLI_PATH: fakeCliPath,
  });

  expect(result.status).toBe(0);
  expect(result.stderr).toMatch(/watchdog-beat-launchd/i);
  expect(result.stderr).toMatch(/SIGTERM/);
});

// ---------------------------------------------------------------------------
// 2. "exits 0 on a normal beat with zero candidates" is already covered by
//    Phase 0's first scenario (a bare `runWrapper()` with no seeded state)
//    — deliberately not duplicated here; padding it with an equivalent
//    unit-level assertion would buy no additional coverage.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 3. A DIFFERENT failure-injection path from Phase 0's own ARM_COORDINATION_FILE
//    ENOTDIR scenario: Phase 0 forces `ensureCoordinationDir` to fail on an
//    otherwise-empty beat. This scenario instead seeds a REAL trip (a
//    genuinely stalled synthetic candidate) so the beat's own
//    `logWatchdogTrip` write is attempted, then points
//    `ARM_LIVENESS_LOG_FILE` at a path whose parent is a plain file — which
//    cli.mjs's own `runWatchdogBeat` treats as fatal (refuses to report the
//    beat as successful when a detection can't be durably recorded, per its
//    own message) rather than degrading silently. Confirmed empirically
//    against the real cli.mjs (not the wrapper) before writing this test:
//    `agent-resource-management: --watchdog-beat failed — runWatchdogBeat:
//    failed to write a watchdog-trip entry to the liveness log — ENOTDIR…`,
//    exit code 2 — a genuinely different unhandled-error shape than Phase
//    0's `ensureCoordinationDir` ENOTDIR (which fails before any candidate
//    is ever resolved).
// ---------------------------------------------------------------------------

it(
  'still exits 0, with a caught-and-logged marker on stderr, when a REAL trip is detected but the ' +
    'liveness-log write itself fails (a malformed ARM_LIVENESS_LOG_FILE parent path, distinct from ' +
    "Phase 0's ARM_COORDINATION_FILE case)",
  async () => {
    const blockerFilePath = join(workDir, 'liveness-blocker-not-a-directory');
    await writeFile(blockerFilePath, 'this is a file, not a directory');
    const brokenLivenessLogPath = join(blockerFilePath, 'liveness-log.json');

    const fakeKillLogPath = join(workDir, 'fake-kill-log.json');
    const nowMs = Date.now();
    const staleMtimeMs = nowMs - 20 * 60 * 1000; // older than the 15-minute default bound

    // A genuine trip must fire so the liveness-log write is actually
    // attempted — this needs an authorized, stalled candidate (pid 9001)
    // parented under this wrapper's own real pid, which is only knowable at
    // shell-runtime (the wrapper takes no argv — launchd invokes it bare,
    // per Phase 0's own contract). See this file's header for the `exec`
    // trick this uses to build a matching ps-tree fixture regardless.
    const psTemplateContent =
      `  PID  PPID    RSS     ELAPSED COMM\n` +
      `    1     0    512 1-03:46:39 /sbin/launchd\n` +
      `  __ANCHOR__     1   1024 1-03:30:00 /usr/libexec/loginwindow\n` +
      ` 9001   __ANCHOR__ 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude\n`;
    const psTemplateFilePath = join(workDir, 'ps-template.txt');
    await writeFile(psTemplateFilePath, psTemplateContent);

    // Learn the wrapper's own synthetic orchestrator-id first (via the fake
    // cli seam), so this scenario's own real-cli setup below declares dibs
    // and a leased queue item under the SAME id the real wrapper run will
    // pass to the real cli.mjs.
    const fakeCliPath = join(workDir, 'fake-cli-for-id.mjs');
    const argvDumpPath = join(workDir, 'argv-dump-for-id.json');
    await writeFile(fakeCliPath, FAKE_CLI_SOURCE);
    const idProbe = runWrapper({
      ARM_WATCHDOG_LAUNCHD_CLI_PATH: fakeCliPath,
      FAKE_CLI_ARGV_DUMP_PATH: argvDumpPath,
    });
    expect(idProbe.status).toBe(0);
    const idProbeArgv = JSON.parse(await readFile(argvDumpPath, 'utf8'));
    const syntheticId = idProbeArgv
      .find((entry) => entry.startsWith('--orchestrator-id='))
      .slice('--orchestrator-id='.length);

    // Seed one leased queue item for that same synthetic id, via the real
    // cli.mjs directly (never the wrapper) — mirrors every sibling spec's
    // `seedLeasedItem` idiom.
    const declareResult = runCliDirect(
      [`--orchestrator-id=${syntheticId}`, '--desired-agents=0'],
      { ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }) },
    );
    expect(declareResult.status).toBe(0);
    const enqueueResult = runCliDirect([
      `--enqueue=typescript-implementer:normal:resumable-workflow --resume=1535-liveness-log-fail`,
      `--orchestrator-id=${syntheticId}`,
    ]);
    expect(enqueueResult.status).toBe(0);
    const dequeueResult = runCliDirect(
      ['--dequeue-if-capacity', `--orchestrator-id=${syntheticId}`],
      { ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }) },
    );
    expect(dequeueResult.status).toBe(0);
    expect(JSON.parse(dequeueResult.stdout).granted).toBe(true);

    const script = [
      'ANCHOR=$$',
      // Rebuild the fixture at shell-runtime, substituting the real $$ into
      // the on-disk placeholder template — avoids any shell-quoting hazard
      // from embedding a multi-line template literal directly in the
      // script text (see this file's header for why $$ is used at all).
      `export ARM_FAKE_PS_OUTPUT="$(sed "s/__ANCHOR__/$ANCHOR/g" ${JSON.stringify(psTemplateFilePath)})"`,
      `exec node ${JSON.stringify(WRAPPER)}`,
    ].join('\n');

    const result = spawnSync('sh', ['-c', script], {
      encoding: 'utf8',
      env: baseEnv({
        ARM_LIVENESS_LOG_FILE: brokenLivenessLogPath,
        ARM_EVICT_TEST_MODE: '1',
        ARM_FAKE_KILL_LOG: fakeKillLogPath,
        ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
        ARM_FAKE_KILL_RESULT: '',
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_FAKE_NOW_MS: String(nowMs),
        ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 100 MB',
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
        ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON: JSON.stringify({ '9001': [100, 100, 100, 100, 100] }),
        ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ '9001': staleMtimeMs }),
        ARM_EVICT_GRACE_MS: '5',
      }),
    });

    expect(result.status).toBe(0);
    expect((result.stderr ?? '')).toMatch(/watchdog-beat-launchd/i);
  },
);

// ---------------------------------------------------------------------------
// 4. Does not write a dibs/ledger entry for the synthetic orchestrator-id —
//    `--watchdog-beat` (and therefore this wrapper) is read-only against
//    the coordination file: it never calls `declareDibs` for its own
//    identity, only `readDibs` for the live fleet. A run with nothing
//    pre-seeded must leave no trace of the wrapper's own synthetic id in
//    the coordination file at all (which, per this same read-only
//    contract, may not even exist on disk afterward).
// ---------------------------------------------------------------------------

it('does not write a dibs/ledger entry for the synthetic orchestrator-id', async () => {
  const result = runWrapper();
  expect(result.status).toBe(0);

  let raw;
  try {
    raw = await readFile(coordinationFilePath, 'utf8');
  } catch {
    raw = null; // Never created — the strongest form of "no entry written".
  }

  if (raw === null || raw.trim().length === 0) {
    return;
  }
  const entries = JSON.parse(raw);
  expect(Array.isArray(entries)).toBe(true);
  expect(entries.some((entry) => typeof entry?.orchestratorId === 'string' && /launchd/i.test(entry.orchestratorId))).toBe(
    false,
  );
});

// ---------------------------------------------------------------------------
// 5. Deadlock tripwire still fires normally through the wrapper — a
//    meaningful end-to-end check, not just "doesn't crash". Queue-aging
//    escalation is DELIBERATELY NOT covered here: `handleWatchdogBeat`
//    (cli.mjs) always calls `escalateAgedQueueEntry` with `capacitySamples:
//    []` — "no historical capacity-sample source is wired into this beat
//    yet" per its own inline comment just above that call — and
//    `wasCapacityFreeThroughout` fails closed on an empty sample array by
//    construction (see lib/queue-aging-escalation.mjs's own doc comment).
//    Aging escalation therefore CANNOT trip through `--watchdog-beat` at
//    all in this phase, through the wrapper or otherwise; a test asserting
//    it fires would be asserting dead code, not this wrapper's behaviour.
//    Confirmed empirically against the real cli.mjs before writing this
//    test.
// ---------------------------------------------------------------------------

it('the deadlock tripwire still fires normally through the wrapper, and the trip is recorded in the liveness log', async () => {
  const nowMs = Date.now();
  const boundMs = 15 * 60 * 1000; // cli.mjs's own DEFAULT_WATCHDOG_BOUND_MS — the wrapper passes no override flag
  const deadlockWindowMs = 15 * 60 * 1000; // cli.mjs's own DEFAULT_DEADLOCK_WINDOW_MS
  const staleMtimeMs = nowMs - boundMs - 5 * 60 * 1000; // strictly older than the bound

  // Learn the wrapper's own fixed synthetic orchestrator-id first (a
  // throwaway probe run against a fake cli.mjs stand-in), so every fixture
  // below is seeded under the SAME id the real wrapper run will pass.
  const fakeCliPath = join(workDir, 'fake-cli-for-id.mjs');
  const argvDumpPath = join(workDir, 'argv-dump-for-id.json');
  await writeFile(fakeCliPath, FAKE_CLI_SOURCE);
  const idProbe = runWrapper({
    ARM_WATCHDOG_LAUNCHD_CLI_PATH: fakeCliPath,
    FAKE_CLI_ARGV_DUMP_PATH: argvDumpPath,
  });
  expect(idProbe.status).toBe(0);
  const idProbeArgv = JSON.parse(await readFile(argvDumpPath, 'utf8'));
  const syntheticId = idProbeArgv
    .find((entry) => entry.startsWith('--orchestrator-id='))
    .slice('--orchestrator-id='.length);

  // Live dibs entry for the synthetic id (real cli.mjs, never the wrapper) —
  // deadlock's own liveness sweep reads ALL live dibs entries.
  const declareResult = runCliDirect(
    [`--orchestrator-id=${syntheticId}`, '--desired-agents=0'],
    { ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }) },
  );
  expect(declareResult.status).toBe(0);

  // One leased queue item for the same id — makes the queue non-empty
  // (deadlock's OTHER required condition) and gives `buildWatchdogClaimLedger`
  // something to pair the synthetic candidate pid against, which is what
  // lets `detectDeadlock`'s own `getFootprintTrajectory`/`getWorktreeMtimeMs`
  // resolvers see this id's stall signal at all (see cli.mjs's own
  // `buildFleetSignalResolvers` doc comment: any id with no claimLedger
  // entry degrades to "no evidence, never stalled").
  const enqueueResult = runCliDirect([
    `--enqueue=typescript-implementer:normal:resumable-workflow --resume=1535-deadlock`,
    `--orchestrator-id=${syntheticId}`,
  ]);
  expect(enqueueResult.status).toBe(0);
  const dequeueResult = runCliDirect(
    ['--dequeue-if-capacity', `--orchestrator-id=${syntheticId}`],
    { ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }) },
  );
  expect(dequeueResult.status).toBe(0);
  expect(JSON.parse(dequeueResult.stdout).granted).toBe(true);

  // Pre-seed the deadlock-state file with a `stalledSince` already older
  // than `deadlockWindowMs`, so a SINGLE beat trips immediately rather than
  // needing real sustained-window wall-clock time across multiple beats —
  // `resolveDeadlockStateFilePath` defaults to
  // `join(dirname(coordinationFilePath), 'deadlock-state.json')` when
  // `ARM_DEADLOCK_STATE_FILE` is not overridden (cli.mjs), which is exactly
  // where this writes.
  const deadlockStateFilePath = join(workDir, 'deadlock-state.json');
  await writeFile(
    deadlockStateFilePath,
    JSON.stringify({ stalledSince: nowMs - deadlockWindowMs - 5 * 60 * 1000 }),
  );

  const fakeKillLogPath = join(workDir, 'fake-kill-log.json');

  // The exec trick (see this file's header): the shell's own `$$`, captured
  // at shell-runtime, becomes this wrapper's `process.pid` once `exec`
  // replaces the shell's process image with `node <wrapper>` — so a ps-tree
  // fixture built from that SAME `$$` is guaranteed to match whatever pid
  // the wrapper resolves as its own and passes as `--orchestrator-pid`,
  // without this test needing to know that pid in advance.
  const psTemplateContent =
    `  PID  PPID    RSS     ELAPSED COMM\n` +
    `    1     0    512 1-03:46:39 /sbin/launchd\n` +
    `  __ANCHOR__     1   1024 1-03:30:00 /usr/libexec/loginwindow\n` +
    ` 9001   __ANCHOR__ 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude\n`;
  const psTemplateFilePath = join(workDir, 'ps-template.txt');
  await writeFile(psTemplateFilePath, psTemplateContent);
  const script = [
    'ANCHOR=$$',
    `export ARM_FAKE_PS_OUTPUT="$(sed "s/__ANCHOR__/$ANCHOR/g" ${JSON.stringify(psTemplateFilePath)})"`,
    `exec node ${JSON.stringify(WRAPPER)}`,
  ].join('\n');

  const result = spawnSync('sh', ['-c', script], {
    encoding: 'utf8',
    env: baseEnv({
      ARM_EVICT_TEST_MODE: '1',
      ARM_FAKE_KILL_LOG: fakeKillLogPath,
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
      ARM_FAKE_KILL_RESULT: '',
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_NOW_MS: String(nowMs),
      ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 100 MB',
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON: JSON.stringify({ '9001': [100, 100, 100, 100, 100] }),
      ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ '9001': staleMtimeMs }),
      ARM_EVICT_GRACE_MS: '5',
    }),
  });

  expect(result.status).toBe(0);

  // The beat's own output is opaque from the wrapper's stdout (Phase 0
  // never pins the wrapper as echoing the inner beat's JSON body — only
  // that it exits 0), so this test observes the trip the same
  // report-consumable way Phase 0's own basic-trip test does: via the
  // liveness log, which the deadlock module's recycle path (or the
  // ordinary watchdog-trip path tripping on the SAME synthetic candidate
  // simultaneously) is contracted to write to.
  const liveLog = await readLivenessLog(livenessLogFilePath);
  expect(Array.isArray(liveLog)).toBe(true);
  const logEntry = liveLog.find((entry) => entry.pid === 9001);
  expect(logEntry).toBeDefined();
  expect(['watchdog-trip', 'deadlock-recycle']).toContain(logEntry.detectionType);
});

// ---------------------------------------------------------------------------
// 6. The hang/timeout backstop — a stability finding against this wrapper's
//    Phase 1 `spawnSync` (no `timeout` option): since `launchd` won't start
//    a new `StartInterval` instance of the same `Label` while the current
//    one is still running, a single hung child (e.g. stuck on a file lock)
//    would otherwise silently and PERMANENTLY stop this backstop from ever
//    firing again. `ARM_WATCHDOG_LAUNCHD_TIMEOUT_MS` is this file's own new
//    test seam (see lib/watchdog-beat-launchd.mjs) — pinned short here so
//    this test stays fast rather than waiting out the real
//    `DEFAULT_WATCHDOG_BEAT_TIMEOUT_MS` (120000ms) default.
// ---------------------------------------------------------------------------

const NEVER_EXITS_CLI_SOURCE = `setInterval(() => {}, 1000);\n`;

it(
  'still exits 0, with a timeout-tagged marker on stderr, when the invoked beat hangs past its bound',
  async () => {
    const fakeCliPath = join(workDir, 'fake-cli-hangs.mjs');
    await writeFile(fakeCliPath, NEVER_EXITS_CLI_SOURCE);

    const start = Date.now();
    const result = runWrapper({
      ARM_WATCHDOG_LAUNCHD_CLI_PATH: fakeCliPath,
      ARM_WATCHDOG_LAUNCHD_TIMEOUT_MS: '300',
    });
    const elapsedMs = Date.now() - start;

    expect(result.status).toBe(0);
    expect(result.stderr).toMatch(/watchdog-beat-launchd/i);
    expect(result.stderr).toMatch(/timed out/i);
    // Generous upper bound relative to the 300ms timeout pinned above —
    // this only guards against the timeout mechanism silently not firing
    // at all (which would otherwise hang this test until Jest's own
    // per-test timeout), not a tight timing assertion.
    expect(elapsedMs).toBeLessThan(10_000);
  },
  15_000,
);

// ---------------------------------------------------------------------------
// 7. SIGTERM forwarding — a stability finding against this wrapper's Phase 1
//    shape: on `launchctl bootout` (the documented, routine
//    reinstall/uninstall path — see ../launchd/README.md), launchd signals
//    THIS wrapper process, but the spawned `cli.mjs --watchdog-beat` child
//    is a separate grandchild process not guaranteed to receive that
//    signal, and could be left running orphaned. This scenario sends the
//    wrapper a real SIGTERM while a genuinely long-running child is in
//    flight and confirms the CHILD (not just the wrapper) is torn down —
//    the load-bearing, end-to-end version of the fix, not just "a handler
//    is registered".
// ---------------------------------------------------------------------------

const RECORDS_SIGTERM_CLI_SOURCE = (markerPath) => `
import { writeFileSync } from 'node:fs';
process.on('SIGTERM', () => {
  writeFileSync(${JSON.stringify(markerPath)}, 'received-sigterm');
  process.exit(0);
});
setInterval(() => {}, 1000);
`;

it('forwards SIGTERM to a still-running child before the wrapper itself exits', async () => {
  const markerPath = join(workDir, 'sigterm-marker.txt');
  const fakeCliPath = join(workDir, 'fake-cli-sigterm.mjs');
  await writeFile(fakeCliPath, RECORDS_SIGTERM_CLI_SOURCE(markerPath));

  const { spawn } = await import('node:child_process');
  const wrapperProcess = spawn('node', [WRAPPER], {
    env: baseEnv({ ARM_WATCHDOG_LAUNCHD_CLI_PATH: fakeCliPath }),
  });

  // Give the wrapper a moment to spawn its own child and install the
  // fake CLI's own SIGTERM handler before signalling — polling on the
  // marker file's absence rather than a fixed sleep keeps this hermetic
  // against slow CI machines without over-waiting on fast ones.
  await new Promise((resolve) => setTimeout(resolve, 300));

  wrapperProcess.kill('SIGTERM');

  await new Promise((resolve) => {
    wrapperProcess.on('exit', resolve);
  });

  const markerContent = await readFile(markerPath, 'utf8').catch(() => null);
  expect(markerContent).toBe('received-sigterm');
}, 15_000);

// ---------------------------------------------------------------------------
// 8. pre-PR review, Medium — SIGTERM escalation-to-SIGKILL for an
//    UNCOOPERATIVE child. Scenario 7 above only covers a child that DOES
//    exit on SIGTERM — the whole motivating scenario for this wrapper (a
//    child stuck on a file lock) may not respond to SIGTERM at all. This
//    scenario installs a no-op SIGTERM handler in the fake CLI (so the
//    process survives the forwarded signal) and confirms the wrapper still
//    exits within a short, bounded time via its own escalation-to-SIGKILL
//    timer — not the multi-minute `DEFAULT_WATCHDOG_BEAT_TIMEOUT_MS` bound.
//    `ARM_WATCHDOG_LAUNCHD_SIGTERM_ESCALATION_MS` is pinned short here so
//    this test stays fast rather than waiting out the real
//    `DEFAULT_SIGTERM_ESCALATION_MS` (3000ms) default.
// ---------------------------------------------------------------------------

const IGNORES_SIGTERM_CLI_SOURCE = `
process.on('SIGTERM', () => {
  // Deliberately a no-op — this is the exact "child stuck on a file lock,
  // unresponsive to a graceful SIGTERM" scenario the escalation timer
  // exists to cover.
});
setInterval(() => {}, 1000);
`;

it(
  'still exits within a short, bounded time — via SIGKILL escalation — when the forwarded SIGTERM is ignored by the child',
  async () => {
    const fakeCliPath = join(workDir, 'fake-cli-ignores-sigterm.mjs');
    await writeFile(fakeCliPath, IGNORES_SIGTERM_CLI_SOURCE);

    const { spawn } = await import('node:child_process');
    const wrapperProcess = spawn('node', [WRAPPER], {
      env: baseEnv({
        ARM_WATCHDOG_LAUNCHD_CLI_PATH: fakeCliPath,
        ARM_WATCHDOG_LAUNCHD_SIGTERM_ESCALATION_MS: '300',
      }),
    });

    // Give the wrapper a moment to spawn its own child and install its
    // no-op SIGTERM handler before signalling.
    await new Promise((resolve) => setTimeout(resolve, 300));

    const start = Date.now();
    wrapperProcess.kill('SIGTERM');

    const exitCode = await new Promise((resolve) => {
      wrapperProcess.on('exit', (code) => resolve(code));
    });
    const elapsedMs = Date.now() - start;

    expect(exitCode).toBe(0);
    // Generous upper bound relative to the 300ms escalation window pinned
    // above — this only guards against the escalation mechanism silently
    // not firing at all (which would otherwise hang this test, or fall
    // through to the multi-minute default beat timeout), not a tight
    // timing assertion.
    expect(elapsedMs).toBeLessThan(10_000);
  },
  15_000,
);

// ---------------------------------------------------------------------------
// 9. pre-PR review, High — backoff isolation. Asserts (a) the
//    shared/default backoff file `--watchdog-beat` itself resolves
//    (`resolveWatchdogBackoffFilePath` in cli.mjs, a sibling of
//    `ARM_COORDINATION_FILE`) is never touched by the wrapper, and (b) two
//    consecutive wrapper-driven beats — which would otherwise both be
//    genuine no-ops, tripping nothing — are NOT the second one skipped as
//    `backoff-not-due` by the wrapper's own accumulated backoff state.
//    `HOME` is now set fleet-wide in `baseEnv()` (see post-merge review,
//    Medium, above) — this scenario no longer needs its own local override,
//    it just relies on the shared one.
// ---------------------------------------------------------------------------

it(
  'never touches the shared/default backoff file, and does not skip a second consecutive beat via its own accumulated backoff',
  () => {
    const sharedBackoffFilePath = join(dirname(coordinationFilePath), 'watchdog-backoff.json');

    const runReal = () =>
      runWrapper({
        // Deliberately NOT overriding ARM_WATCHDOG_LAUNCHD_CLI_PATH — this
        // scenario invokes the REAL cli.mjs so its own real backoff-state
        // read/write path is genuinely exercised.
      });

    const firstRun = runReal();
    expect(firstRun.status).toBe(0);
    expect(firstRun.stdout).not.toMatch(/"skipped":true/);

    const secondRun = runReal();
    expect(secondRun.status).toBe(0);
    expect(secondRun.stdout).not.toMatch(/"skipped":true/);

    expect(existsSync(sharedBackoffFilePath)).toBe(false);
  },
);

// ---------------------------------------------------------------------------
// 9b. post-merge review, Medium — an explicit EMPTY-STRING
//     `ARM_WATCHDOG_BACKOFF_FILE` must NOT be treated as "the caller already
//     set it". `cli.mjs`'s own `resolveWatchdogBackoffFilePath` gates on
//     truthiness (`if (process.env.ARM_WATCHDOG_BACKOFF_FILE)`), so `''` is
//     falsy there and falls straight through to the fleet-shared default —
//     a `typeof ... === 'string'` check in the wrapper alone would miss this
//     and pass `''` through unmodified, reinstating the exact shared-backoff
//     failure mode scenario 9 above guards against. Reachable via
//     `launchctl setenv`, a hand-edited plist `EnvironmentVariables` block,
//     or a shell wrapper — not via the shipped plist template, but real.
// ---------------------------------------------------------------------------

it(
  'still isolates its own backoff file (never the shared/default one) when ARM_WATCHDOG_BACKOFF_FILE is explicitly set to the empty string',
  () => {
    const sharedBackoffFilePath = join(dirname(coordinationFilePath), 'watchdog-backoff.json');

    const result = runWrapper({
      // Deliberately NOT overriding ARM_WATCHDOG_LAUNCHD_CLI_PATH — this
      // scenario invokes the REAL cli.mjs so its own real backoff-file
      // resolution is genuinely exercised.
      ARM_WATCHDOG_BACKOFF_FILE: '',
    });

    expect(result.status).toBe(0);
    expect(existsSync(sharedBackoffFilePath)).toBe(false);
  },
);

// ---------------------------------------------------------------------------
// 10. pre-PR review, Medium — entry-point guard. A plain `import` of
//     the wrapper module (for either of its exports) must NOT trigger a real
//     watchdog beat. Run in a real child process (never a same-process
//     `import()` inside this Jest worker) so a guard regression can never
//     spawn a beat, or call `process.exit`, inside the test runner itself.
// ---------------------------------------------------------------------------

it('does not trigger a watchdog beat when the module is merely imported', async () => {
  const importScript = `
import * as wrapper from ${JSON.stringify(WRAPPER)};
process.stdout.write(JSON.stringify({
  hasSyntheticId: typeof wrapper.SYNTHETIC_ORCHESTRATOR_ID === 'string',
  hasTimeout: typeof wrapper.DEFAULT_WATCHDOG_BEAT_TIMEOUT_MS === 'number',
  hasEscalation: typeof wrapper.DEFAULT_SIGTERM_ESCALATION_MS === 'number',
}) + '\\n');
`;
  const importScriptPath = join(workDir, 'import-probe.mjs');
  await writeFile(importScriptPath, importScript);

  const result = spawnSync('node', [importScriptPath], { encoding: 'utf8', env: baseEnv() });

  expect(result.status).toBe(0);
  const parsed = JSON.parse((result.stdout ?? '').trim());
  expect(parsed).toEqual({ hasSyntheticId: true, hasTimeout: true, hasEscalation: true });

  // No coordination/liveness-log activity — the strongest available signal
  // that the module's direct-execution block (which would otherwise spawn
  // `cli.mjs --watchdog-beat` and touch these exact files) never ran.
  expect(existsSync(coordinationFilePath)).toBe(false);
  expect(existsSync(livenessLogFilePath)).toBe(false);
});

// ---------------------------------------------------------------------------
// 11. post-merge review, Medium — the entry-point guard must still
//     recognise direct execution when the wrapper is reached through a
//     symlink (`node <symlink-to-wrapper>`), e.g. a symlinked clone
//     directory, an external-volume mount, or `/tmp` itself (a symlink to
//     `/private/tmp` on macOS). `import.meta.url` is always symlink-resolved
//     by Node, but `process.argv[1]` is the literal invoked path — a naive
//     string-equality guard diverges here and silently no-ops the whole
//     backstop with no stderr output. Run the wrapper via a real symlink and
//     assert the beat actually ran, mirroring scenario 4's "no dibs entry
//     written" shape but for the positive "did produce a real beat result"
//     side.
// ---------------------------------------------------------------------------

it('still runs the beat when invoked through a symlink to the wrapper (not the real path)', async () => {
  const fakeCliPath = join(workDir, 'fake-cli.mjs');
  const argvDumpPath = join(workDir, 'argv-dump.json');
  await writeFile(fakeCliPath, FAKE_CLI_SOURCE);

  const symlinkPath = join(workDir, 'watchdog-beat-launchd-symlink.mjs');
  await symlink(WRAPPER, symlinkPath);

  const result = spawnSync('node', [symlinkPath], {
    encoding: 'utf8',
    env: baseEnv({ ARM_WATCHDOG_LAUNCHD_CLI_PATH: fakeCliPath, FAKE_CLI_ARGV_DUMP_PATH: argvDumpPath }),
  });

  expect(result.status).toBe(0);

  // The load-bearing assertion: the fake CLI (which only ever runs from
  // inside the wrapper's own direct-execution block) actually dumped its
  // argv — proof the beat genuinely ran through the symlinked entry point.
  // Pre-fix, `process.argv[1]` (the literal symlink path) never matched the
  // symlink-resolved `import.meta.url`, the guard evaluated false, the
  // direct-execution block never ran, this file was never written, and this
  // assertion would fail with an ENOENT from `readFile`.
  const argv = JSON.parse(await readFile(argvDumpPath, 'utf8'));
  expect(argv).toContain('--watchdog-beat');
});
