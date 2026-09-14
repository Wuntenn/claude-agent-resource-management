// Phase 0 outer acceptance test ("agent-resource-management:
// unattended liveness — stuck-agent watchdog, queue aging, deadlock
// tripwire").
//
// Outer acceptance test (per the `## Build Plan` comment posted on):
//
//   it('flags a synthetically-stalled agent (flat CPU trajectory + no
//   worktree mtime change for the bound), recycles it via the real
//   --evict-pid path, and records the recycle in the report-consumable log
//   — no real PID or filesystem touched')
//   Layer: outer acceptance, CLI-level (mirrors
//   ./cli-evict.jest.spec.mjs / ./evict-outer-acceptance.jest.spec.mjs's
//   existing shape) — drives the watchdog beat via `spawnSync` with
//   `ARM_EVICT_TEST_MODE=1` and the existing `ARM_FAKE_*` seams
//   (`ARM_FAKE_KILL_LOG`, `ARM_FAKE_IS_ALIVE_SEQUENCE`, `ARM_FAKE_NOW_MS`,
//   `ARM_FAKE_FOOTPRINT_OUTPUT`) plus new fake seams for the mtime/history
//   signal and the queue-aging clock — fully hermetic, no real syscalls.
//   Green when: Phase 1 (liveness signal) + Phase 2 (persisted
//   aging/alert) + Phase 3 (deadlock tripwire) + Phase 4 (recycle wiring) +
//   Phase 5 (report-consumable logging) + Phase 6 (pacing-gated beat) all
//   pass their gates.
//
// RED by construction, right now: NONE of Phases 1-6 have landed.
// `cli.mjs` has no `--watchdog-beat` flag at all today (confirmed: `grep -n
// "watchdog" cli.mjs` matches nothing), `lib/liveness.mjs`,
// `lib/queue-aging-escalation.mjs`, and `lib/deadlock-tripwire.mjs` do not
// exist (confirmed: `ls lib/` has no such files as of this commit — see
// `ls scripts/agent-resource-management/lib/` for the current, complete
// list). Every test below fails for that reason (module-not-found /
// unrecognized-flag-falls-through) until Phase 6 lands.
//
// Do not add a stub implementation to make this pass — that defeats the
// point of the outer/phase red-green loop. Phases 1-6's implementers turn
// this file green by actually building the mechanism.
//
// ---------------------------------------------------------------------------
// ENTRYPOINT / INTERFACE THIS TEST PINS FOR PHASE 6'S IMPLEMENTER
// ---------------------------------------------------------------------------
//
// New standalone, opt-in `cli.mjs` beat flag: `--watchdog-beat`.
//
//   node cli.mjs --watchdog-beat \
//     --orchestrator-id=<id> --orchestrator-pid=<pid> \
//     [--watchdog-bound-ms=<n>] [--watchdog-deadlock-window-ms=<n>] \
//     [--watchdog-aging-bound-ms=<n>]
//
// This mirrors the existing standalone-beat-type family exactly
// (`--evict-pid`, `--dequeue-if-capacity`, `--poll-footprint`,
// `--record-outcome`): one-shot, conflict-checked against every OTHER
// standalone beat-type flag (never silently composed with
// `--desired-agents`/`--heartbeat`/`--evict-pid`/etc. in the same
// invocation — exit 2, naming both flags, per every sibling beat's own
// conflict guard), and it prints exactly one JSON body to stdout before
// exiting.
//
// Required flags (both already-established conventions from
// `--evict-pid`'s own three-flag contract, see ./cli-evict.jest.spec.mjs's
// design note 2):
//   --orchestrator-id=<id>   the calling orchestrator's declared identity
//   --orchestrator-pid=<pid> the calling orchestrator's own OS pid, needed
//                             by Phase 4's recycle wiring to reuse
//                             `isEvictionAuthorized`'s own-spawned-only
//                             ancestry check for every trip this beat
//                             recycles, exactly as `--evict-pid` already
//                             requires it.
//
// Optional bound-override flags (mirroring `ARM_EVICT_GRACE_MS`'s existing
// pattern of "a production caller omits it and gets the real default; tests
// always override it to avoid waiting out a real bound"):
//   --watchdog-bound-ms=<n>              Phase 1's per-agent stall bound
//   --watchdog-deadlock-window-ms=<n>    Phase 3's sustained-window bound
//   --watchdog-aging-bound-ms=<n>        Phase 2's escalation bound
//
// Output shape this test pins (the OBSERVABLE contract, not the internal
// composition):
//
//   {
//     "type": "watchdog",
//     "trips": [ { "pid": <number>, "reason": "stalled", "evicted": <bool>,
//                  "outcome": <gracefulStop outcome string> }, ... ],
//     "deadlockTripped": <bool>,
//     "agingEscalations": [ ... ],
//     "recycled": [ <pid>, ... ]
//   }
//
// This test asserts on the coarse shape above (`type`, `trips` containing
// the synthetically-stalled pid, `recycled` containing that pid) — NOT on
// every field of every sub-object, per this ticket's own outer-acceptance
// guidance to avoid over-pinning internals Phases 1-6 haven't built yet. If
// Phase 6's implementer needs a different field name for equivalent
// information, update this file's assertions to match — the invariant that
// must survive is "the beat's own JSON output makes the trip + the real
// recycle observable without inspecting internals."
//
// ---------------------------------------------------------------------------
// NEW FAKE SEAMS THIS TEST INTRODUCES (for Phase 1's mtime signal and
// Phase 2's queue-aging clock) — read before extending or objecting
// ---------------------------------------------------------------------------
//
// 1. `ARM_FAKE_WORKTREE_MTIME_JSON` — a JSON object, `{ "<pid>":
//    <epochMs> }`, mapping a candidate pid to the latest git-tracked
//    worktree file mtime (in epoch ms) Phase 1's mtime-scanning signal
//    should observe for that pid, bypassing any real `fs.stat`/`git`
//    shell-out entirely when set. Mirrors `ARM_FAKE_PS_OUTPUT`'s own
//    "bypass the real OS surface entirely" shape. Not honoured by any real
//    code yet — Phase 1's implementer is free to land a differently-shaped
//    seam (e.g. per-pid worktree PATH instead of a raw mtime, with a
//    separate fake-fs seam under it) as long as it preserves the two
//    invariants this file needs: (a) no real filesystem stat against a real
//    path in these tests, (b) the fake value is controllable per-pid so a
//    single test run can express "this candidate's worktree looks stale."
//
// 2. `ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON` — a JSON object, `{ "<pid>":
//    [<sampleMb>, <sampleMb>, ...] }`, standing in for Phase 1's CPU/
//    footprint-trajectory-flatness read (built on the ALREADY-EXISTING
//    `lib/footprint-trajectory.mjs` + `lib/footprint-state.mjs` persisted
//    per-pid state — see that module's own header for why one-shot CLI
//    invocations need a persisted home for this accumulator). This test
//    seeds a genuinely flat trajectory (every sample identical) for the
//    synthetic candidate pid via this seam rather than driving N real
//    `--poll-footprint` invocations, to keep this single outer test fast
//    and self-contained; Phase 1's own unit-level spec
//    (`lib/liveness.jest.spec.mjs`, not this file) is expected to also
//    cover the real `--poll-footprint`-seeded path end-to-end. Not honoured
//    by any real code yet.
//
// Both seams are read only when `ARM_EVICT_TEST_MODE=1` is also set —
// consistent with every existing `ARM_FAKE_*` seam's "master switch" gate
// (`EVICT_FAKE_SEAM_VARS` coupling check, `cli.mjs`) so a leaked fake seam
// can never silently redirect a real production beat.
//
// ---------------------------------------------------------------------------
// SAFETY — no real SIGTERM/SIGKILL against an arbitrary host PID. Every
// "running agent" PID in this file's fixtures is SYNTHETIC (mirrors
// ./cli-evict.jest.spec.mjs's own PS_AUTHORIZED fixture verbatim, PID 9001
// parented under 501), and every `process.kill` call Phase 4's recycle
// wiring makes is expected to be redirected through the ALREADY-EXISTING
// `ARM_FAKE_KILL_LOG`/`ARM_FAKE_KILL_RESULT`/`ARM_FAKE_IS_ALIVE_SEQUENCE`
// seams `--evict-pid` already honours — this file asserts the fake kill log
// captured a signal, never that a real syscall fired.
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { readLivenessLog, logWatchdogTrip } from './lib/liveness-log.mjs';
// Phase 4 pre-PR review, High 2 — seeds a REAL, already-persisted
// footprint-state.mjs trajectory directly via this lib-level primitive
// (mirrors ./cli-leak-block.jest.spec.mjs's own `seedFootprintTrajectory`
// idiom), so this file's real-CLI leak-velocity eviction test exercises the
// genuine `buildWatchdogCandidates` read path, never the
// `ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON` fake seam.
import { recordFootprintPoll } from './lib/footprint-state.mjs';
// — this file's own real-CLI-beat `declareDibsFor` idiom never resolves
// a `/claude`-rooted ancestor inside Jest, so the dibs entry it seeds carries
// no `pid`. `performEviction`'s fail-closed orchestrator-pid-binding check
// (shared by --watchdog-beat, which routes through the same function) needs a
// genuinely-recorded, matching pid — see ./cli-evict.jest.spec.mjs's own
// `declareDibsWithPid` for the identical idiom.
import { declareDibs, readDibs } from './lib/coordination-file.mjs';
// Phase 2 review — the watchdog-path equivalent of
// ./cli-evict.jest.spec.mjs's own pid-lease-mismatch scenario: binds a
// DIFFERENT pid than the synthetic candidate's own to the seeded lease, so
// `performEviction`'s pid-lease-binding check (shared by --watchdog-beat)
// refuses the recycle even though ps-tree ancestry alone would authorize it.
import { bindPidToLease } from './lib/queue.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const MEMORY_RED = { pressureLevel: 1, swapUsedMb: 12288, compressedMb: 4096 };
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

// Single claude-rooted agent tree, PID 9001 parented under 501 — mirrors
// ./cli-evict.jest.spec.mjs's own PS_AUTHORIZED fixture exactly (own-spawned,
// authorized for eviction under --orchestrator-pid=501). Every PID here is
// SYNTHETIC — never a real process on the host running this test.
const PS_AUTHORIZED = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001  51200       58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

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
let fakeKillLogPath;
let liveLogFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-watchdog-outer-'));
  queueFilePath = join(workDir, 'queue.json');
  coordinationFilePath = join(workDir, 'coordination.json');
  fakeKillLogPath = join(workDir, 'fake-kill-log.json');
  // Phase 5's own report-consumable log surface — this file guesses a path
  // env var, `ARM_LIVENESS_LOG_FILE`, mirroring `ARM_QUEUE_FILE`/
  // `ARM_COORDINATION_FILE`/`ARM_HISTORY_FILE`'s existing "one env var per
  // persisted store, always overridable for tests" convention. If Phase 5's
  // implementer names this differently, update this file's env plumbing —
  // the invariant that matters is "the recycle is observable in a
  // structured, report-consumable log file this test can read back."
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
    // Master switch — every ARM_FAKE_* seam below (including this file's two
    // new ones) is a documented no-op unless this is set, per cli.mjs's
    // existing EVICT_FAKE_SEAM_VARS coupling contract.
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

/**
 * Declares dibs for `orchestratorId` via a normal, real beat — mirrors
 * ./cli-evict.jest.spec.mjs's `declareDibsFor` exactly: `handleEvict`'s
 * (and, per this ticket's design, the new watchdog beat's own)
 * `orchestratorIsLive` check requires a dibs entry that already existed
 * before the recycle attempt's own internal declaration.
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
 * Seeds (or overwrites) `orchestratorId`'s dibs-ledger entry with a SPECIFIC
 * `pid`, directly via `declareDibs` — bypassing the real ps-tree ancestry walk
 * `declareDibsFor`'s real-CLI-beat idiom depends on. Models "a genuine PRIOR
 * beat from this orchestrator already recorded its own real root pid on the
 * ledger", which `performEviction`'s orchestrator-pid-binding check
 * reads via `readDibs` BEFORE the recycle attempt's own `declareDibs` call —
 * mirrors ./cli-evict.jest.spec.mjs's own `declareDibsWithPid` exactly. Must
 * run AFTER `seedLeasedItem`/`declareDibsFor` for the same `orchestratorId`.
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
 * Seeds one queue item and dequeues it, returning its leaseId — models "this
 * item is currently running as the synthetic agent this test is about to
 * flag as stalled." Mirrors ./cli-evict.jest.spec.mjs's `seedLeasedItem`.
 */
function seedLeasedItem({
  orchestratorId = 'orch-watchdog',
  agentClass = 'typescript-implementer',
  commandRef = 'resumable-workflow --resume=1272-phase-6',
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

function peekItems(orchestratorId) {
  const result = runCli(
    ['--peek', `--orchestrator-id=${orchestratorId}`],
    baseEnv({ ARM_FAKE_COLLECT_JSON: undefined, ARM_FAKE_PS_OUTPUT: undefined }),
  );
  expect(result.status).toBe(0);
  return JSON.parse(result.stdout).items;
}

// ---------------------------------------------------------------------------
// The outer acceptance test itself (Phases 1+2+3+4+5+6 composed):
// end-to-end via the real cli.mjs. A single synthetic candidate (pid 9001,
// own-spawned/authorized under orchestratorPid=501, exactly matching
// ./cli-evict.jest.spec.mjs's own PS_AUTHORIZED fixture) presents BOTH stall
// signals — flat footprint trajectory AND a worktree mtime that has not
// advanced within the configured bound — and currently holds the lease for
// one real, seeded queue item. A single `--watchdog-beat` invocation must:
//
//   (a) flag pid 9001 as stalled (Phase 1),
//   (b) recycle it via the real, already-shipped `--evict-pid`-equivalent
//       eviction mechanism — SIGTERM captured only in the fake kill log,
//       never a real syscall (Phase 4),
//   (c) release/re-enqueue the work item it held, observable via --peek
//       (reusing its already-proven releaseQueueEntry contract),
//   (d) record the recycle in the report-consumable log (Phase 5).
// ---------------------------------------------------------------------------

it(
  'flags a synthetically-stalled agent (flat CPU trajectory + no worktree mtime change for the bound), ' +
    'recycles it via the real --evict-pid path, and records the recycle in the report-consumable log — ' +
    'no real PID or filesystem touched',
  async () => {
    const ORCHESTRATOR_ID = 'orch-watchdog-outer';
    const { leaseId, agentClass, commandRef } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
    // — see `declareDibsWithPid`'s own doc comment above.
    await declareDibsWithPid(ORCHESTRATOR_ID, 501);

    const boundMs = 10 * 60 * 1000; // 10 minutes, matching the existing AGING_THRESHOLD_MS scale
    const nowMs = Date.now();
    const staleMtimeMs = nowMs - boundMs - 60_000; // strictly older than the bound

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
        // Flat trajectory — every sample identical, so Phase 1's CPU-delta
        // signal reads exactly zero movement over the bound.
        ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON: JSON.stringify({ '9001': [100, 100, 100, 100, 100] }),
        // No git-tracked worktree file for pid 9001 has an mtime newer than
        // `staleMtimeMs` — strictly older than `boundMs` before `nowMs`.
        ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ '9001': staleMtimeMs }),
        ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
        ARM_FAKE_KILL_RESULT: '',
        // its own --evict-pid path requires a real lease/orchestrator
        // association to release on a confirmed-stopped outcome — the
        // watchdog beat is expected to resolve this the same way --evict-pid
        // already does today (via the coordination-file's claim ledger, per
        // the Investigation's Edge Case #4), not via a guessed flag on this
        // invocation. leaseId is threaded here only so this test can assert
        // the SAME item is what reappears released below.
      }),
    );

    // RED today, for TWO independent reasons that both must resolve before
    // this line passes: (1) `--watchdog-beat` is not a recognized flag at
    // all yet, so it silently falls through to the default bare-beat
    // dispatch and never touches the queue/kill-log/liveness-log fixtures
    // this test seeds; (2) even once the flag exists, none of
    // lib/liveness.mjs / lib/queue-aging-escalation.mjs /
    // lib/deadlock-tripwire.mjs exist to actually classify pid 9001 as
    // stalled or drive a real recycle.
    expect(beatResult.status).toBe(0);
    const body = JSON.parse(beatResult.stdout);

    // (a) Phase 1 — the beat's own JSON output makes the trip observable.
    expect(body.type).toBe('watchdog');
    expect(Array.isArray(body.trips)).toBe(true);
    const trip = body.trips.find((entry) => entry.pid === 9001);
    expect(trip).toBeDefined();
    expect(trip.reason).toBe('stalled');

    // (b) Phase 4 — recycled via the real eviction mechanism: only the fake
    // kill log observed a signal, never a real process.kill syscall.
    expect(Array.isArray(body.recycled)).toBe(true);
    expect(body.recycled).toContain(9001);
    const killLog = JSON.parse(await readFile(fakeKillLogPath, 'utf8'));
    expect(killLog.length).toBeGreaterThan(0);
    expect(killLog.every((entry) => entry.pid === 9001)).toBe(true);
    expect(killLog[0].signal).toBe('SIGTERM');

    // (c) The work item the stalled agent held is released back to the
    // queue — same observable contract its own outer acceptance test
    // pins (agentClass/commandRef/orchestratorId intact, leaseId cleared).
    const remaining = peekItems(ORCHESTRATOR_ID);
    expect(remaining).toHaveLength(1);
    expect(remaining[0]).toMatchObject({ agentClass, commandRef, orchestratorId: ORCHESTRATOR_ID });
    expect(remaining[0].leaseId).toBeUndefined();
    // Sanity: this is genuinely the SAME item the test seeded (not a fresh
    // enqueue that happens to look similar) — its own leaseId is gone, not
    // a brand-new entry's.
    expect(remaining[0].leaseId).not.toBe(leaseId);

    // (d) Phase 5 — the recycle is recorded in the report-consumable log,
    // in a structured, machine-readable form (not a bare stdout echo).
    // The log is NDJSON on disk (one JSON object per line, per
    // ./lib/liveness-log.mjs's own documented on-disk shape) — read it back
    // via that module's own `readLivenessLog`, not a raw whole-file
    // `JSON.parse`, mirroring ./lib/liveness-log.jest.spec.mjs's own idiom.
    const liveLog = await readLivenessLog(liveLogFilePath);
    expect(Array.isArray(liveLog)).toBe(true);
    const logEntry = liveLog.find((entry) => entry.pid === 9001);
    expect(logEntry).toBeDefined();
    expect(['watchdog-trip', 'deadlock-recycle']).toContain(logEntry.detectionType);
    expect(logEntry.outcome).toBeDefined();
    expect(typeof logEntry.timestamp).toBe('number');
  },
);

// ---------------------------------------------------------------------------
// pre-PR review, Medium — the SAME scenario as the recycle test above,
// minus the `declareDibsWithPid` seeding, so `performEviction`'s fail-closed
// orchestrator-pid-binding check refuses the recycle. Two things must hold:
// the refusal actually stops the kill (the security property), AND the
// refusal is OBSERVABLE by its real name in both the beat's JSON body and
// its liveness NDJSON log. The second half is a regression guard on
// `KNOWN_FAILURE_REASONS` (./lib/eviction-targeting.mjs): the reason shipped
// missing from that set, so `classifyEvictionOutcome` collapsed it to the
// generic `'unknown'` a malformed result would also produce — silently
// masking a security-gate refusal in the one durable delivery mechanism an
// unattended watchdog has.
// ---------------------------------------------------------------------------

it('refuses the recycle, and surfaces orchestrator-pid-mismatch by name (never a collapsed "unknown"), when the orchestrator has no recorded pid matching --orchestrator-pid', async () => {
  const ORCHESTRATOR_ID = 'orch-watchdog-pid-mismatch';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // Deliberately NO `declareDibsWithPid` — the orchestrator is live but its
  // ledger entry carries no recorded pid, so the binding check fails closed.

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
  expect(body.type).toBe('watchdog');

  // The stall itself is still detected — it is the RECYCLE that is refused.
  const trip = body.trips.find((entry) => entry.pid === 9001);
  expect(trip).toBeDefined();
  expect(trip.evicted).toBe(false);
  expect(trip.outcome).toBe('orchestrator-pid-mismatch');
  expect(body.recycled ?? []).not.toContain(9001);

  // No signal ever sent — the refusal fires before any kill work.
  await expect(readFile(fakeKillLogPath, 'utf8')).rejects.toThrow();

  // Observable by name in the durable log, not collapsed to 'unknown'.
  const liveLog = await readLivenessLog(liveLogFilePath);
  const logEntry = liveLog.find((entry) => entry.pid === 9001);
  expect(logEntry).toBeDefined();
  expect(logEntry.outcome).toBe('orchestrator-pid-mismatch');
  // `reason` rides in the entry's `detail` bag, per `logWatchdogTrip`'s own
  // `buildDetail` shape (./lib/liveness-log.mjs) — not as a top-level field.
  expect(logEntry.detail).toMatchObject({ reason: 'orchestrator-pid-mismatch' });

  // The lease is left untouched — never released, never re-enqueued.
  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

// ---------------------------------------------------------------------------
// Phase 2 review, High — the SAME scenario as the recycle test above,
// but with a DIFFERENT pid bound to the seeded lease via `bindPidToLease`,
// so `performEviction`'s pid-lease-binding check refuses the recycle even
// though the candidate pid is genuinely own-spawned/authorized per ps-tree
// ancestry. Mirrors the `orchestrator-pid-mismatch` watchdog-path test above
// exactly: the refusal must both stop the kill (the security property) and
// be OBSERVABLE by its real name in the beat's JSON body and its liveness
// NDJSON log — a regression guard on `KNOWN_FAILURE_REASONS`
// (./lib/eviction-targeting.mjs), which shipped without `pid-lease-mismatch`
// registered, collapsing it to the generic `'unknown'` a malformed result
// would also produce.
// ---------------------------------------------------------------------------

it('refuses the recycle, and surfaces pid-lease-mismatch by name (never a collapsed "unknown"), when the lease is bound to a different pid than the stalled candidate', async () => {
  const ORCHESTRATOR_ID = 'orch-watchdog-pid-lease-mismatch';
  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);
  // A DIFFERENT pid than the synthetic stalled candidate (9001) below is
  // bound to this lease — the candidate is genuinely own-spawned/authorized
  // per ps-tree ancestry, but the lease was actually checked out under a
  // different process (12345). Exactly the same-orchestrator pid/lease
  // pairing bug exists to catch.
  const bound = await bindPidToLease(queueFilePath, leaseId, 12345, ORCHESTRATOR_ID);
  expect(bound).not.toBeNull();

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
  expect(body.type).toBe('watchdog');

  // The stall itself is still detected — it is the RECYCLE that is refused.
  const trip = body.trips.find((entry) => entry.pid === 9001);
  expect(trip).toBeDefined();
  expect(trip.evicted).toBe(false);
  expect(trip.outcome).toBe('pid-lease-mismatch');
  expect(body.recycled ?? []).not.toContain(9001);

  // No signal ever sent — the refusal fires before any kill work.
  await expect(readFile(fakeKillLogPath, 'utf8')).rejects.toThrow();

  // Observable by name in the durable log, not collapsed to 'unknown'.
  const liveLog = await readLivenessLog(liveLogFilePath);
  const logEntry = liveLog.find((entry) => entry.pid === 9001);
  expect(logEntry).toBeDefined();
  expect(logEntry.outcome).toBe('pid-lease-mismatch');
  // `reason` rides in the entry's `detail` bag, per `logWatchdogTrip`'s own
  // `buildDetail` shape (./lib/liveness-log.mjs) — not as a top-level field.
  expect(logEntry.detail).toMatchObject({ reason: 'pid-lease-mismatch' });

  // The lease is left untouched — never released, never re-enqueued.
  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

// ---------------------------------------------------------------------------
// Regression guard (already true of today's real code, kept here as a
// scope-fence for Phase 6): a bare beat WITHOUT `--watchdog-beat` must never
// perform a recycle, no matter how RED memory gets — mirrors its own
// "a bare RED beat can never evict" invariant, extended to the new surface.
// ---------------------------------------------------------------------------

it('never recycles anything on a bare beat that does not opt in via --watchdog-beat', () => {
  const ORCHESTRATOR_ID = 'orch-watchdog-no-opt-in';
  seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });

  const result = runCli(
    [`--orchestrator-id=${ORCHESTRATOR_ID}`],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
    }),
  );
  expect(result.status).toBe(0);
  const body = JSON.parse(result.stdout);

  expect(body.type).not.toBe('watchdog');
  // The seeded item is still leased — untouched by any recycle.
  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBeDefined();
});

// ---------------------------------------------------------------------------
// Regression guard for Phase 1's own documented AND semantics: an agent with
// an ACTIVE footprint trajectory (genuine movement) must never be flagged as
// stalled, even if its worktree mtime looks stale — both signals must agree
// before a trip fires.
// ---------------------------------------------------------------------------

it('does not flag or recycle an agent whose footprint trajectory shows genuine activity, even with a stale worktree mtime', () => {
  const ORCHESTRATOR_ID = 'orch-watchdog-active';
  seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });

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
      // Genuinely moving trajectory — real activity.
      ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON: JSON.stringify({ '9001': [80, 140, 95, 210, 130] }),
      ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ '9001': staleMtimeMs }),
    }),
  );
  expect(beatResult.status).toBe(0);
  const body = JSON.parse(beatResult.stdout);

  expect(body.type).toBe('watchdog');
  const trip = (body.trips ?? []).find((entry) => entry.pid === 9001);
  expect(trip).toBeUndefined();
  expect(body.recycled ?? []).not.toContain(9001);

  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBeDefined();
});

// ---------------------------------------------------------------------------
// Regression guard (review, Medium, cli.mjs commit 7e089a1e) — a
// genuine mid-recycle failure (here, `performEviction`'s own
// `EvictFakeSeamCouplingError` fake-seam-coupling misconfiguration check,
// triggered the same way ./cli-evict.jest.spec.mjs's "refuses to proceed
// when ARM_FAKE_PS_OUTPUT is set without ARM_FAKE_KILL_LOG" scenario
// triggers it for --evict-pid) must fail the WHOLE `--watchdog-beat`
// invocation loudly — exit 2, a stderr message naming the failure — instead
// of resolving 0 with a JSON body that silently downgrades the failure into
// a fabricated per-target `reason: 'unknown'` outcome.
// ---------------------------------------------------------------------------

it('exits 2 with a fail-loud stderr message when a genuine eviction-seam misconfiguration fails the recycle, instead of silently resolving with a swallowed reason: unknown outcome', () => {
  const ORCHESTRATOR_ID = 'orch-watchdog-seam-coupling';
  seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });

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
      // Deliberately drop the paired kill-log seam: ARM_FAKE_PS_OUTPUT stays
      // set (needed so pid 9001 is authorized/flagged stalled at all) but
      // ARM_FAKE_KILL_LOG is not — performEviction's own fake-seam coupling
      // check refuses to proceed with this as a genuine misconfiguration.
      ARM_FAKE_KILL_LOG: undefined,
    }),
  );

  expect(beatResult.status).toBe(2);
  expect(beatResult.stderr).toMatch(/--watchdog-beat failed/);
  expect(beatResult.stderr).toMatch(/partially-faked/);
  expect(beatResult.stderr).toMatch(/ARM_FAKE_PS_OUTPUT/);
  expect(beatResult.stderr).toMatch(/ARM_FAKE_KILL_LOG/);

  // Fails loud, not open — no JSON body at all, never a resolved outcome
  // with a fabricated per-target reason.
  expect(beatResult.stdout).toBe('');

  // The work item the stalled agent held stays leased — a beat that fails
  // loudly must never have gone on to release it as if the recycle had
  // actually completed.
  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBeDefined();
});

// ---------------------------------------------------------------------------
// Regression guard (review, Critical) — a genuine TOTAL machine-probe
// failure (both `realCollect()` attempts inside `collect()`'s own bounded
// retry throwing — see that function's own header comment) reaches
// `handleEvictInProcess` via `getPreTakenSample`'s cached, shared sample
// promise, NOT via `performEviction`'s own `EvictFakeSeamCouplingError`
// check the test above exercises. Before this fix, this rejection carried no
// `armFailBeat` marker at all, so `runWatchdogBeat`'s `isSystemicEvictFailure`
// check silently isolated it into a fabricated per-target `reason: 'unknown'`
// outcome instead of failing the whole beat loudly — for every evict target
// in the beat, since the sample is cached across all of them.
//
// Uses the REAL `ARM_FORCE_COLLECT_FAILURES` test seam — which
// drives actual `collect()`/`realCollect()` retry code, not a hand-built
// marked error object — to reach `collect()`'s own catch block genuinely,
// proving the marker set there (this fix) is what makes the failure
// propagate.
// ---------------------------------------------------------------------------

it('exits 2 with a fail-loud stderr message when a genuine TOTAL collect() failure (both realCollect() attempts) reaches the recycle, instead of silently resolving with a swallowed reason: unknown outcome', () => {
  const ORCHESTRATOR_ID = 'orch-watchdog-total-collect-failure';
  seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });

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
      // Deliberately NOT ARM_FAKE_COLLECT_JSON — that seam short-circuits
      // collect() before realCollect()/totalCollectFailure are ever reached
      // (see cli.jest.spec.mjs's own "bypasses the retry wrapper entirely"
      // tests). Candidate/trip detection here needs no machine sample at all
      // (it's driven by ARM_FAKE_PS_OUTPUT ancestry + footprint trajectory +
      // worktree mtime), so collect() is only ever invoked lazily, once,
      // inside handleEvictInProcess's getPreTakenSample — exactly the call
      // site this regression guard targets.
      ARM_FORCE_COLLECT_FAILURES: '2',
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      ARM_FAKE_NOW_MS: String(nowMs),
      ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON: JSON.stringify({ '9001': [100, 100, 100, 100, 100] }),
      ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ '9001': staleMtimeMs }),
    }),
  );

  expect(beatResult.status).toBe(2);
  expect(beatResult.stderr).toMatch(/--watchdog-beat failed/);
  // The real error message `collect()` throws from its own catch block
  // (ARM_FORCE_COLLECT_FAILURES test seam), proving this is the genuine
  // totalCollectFailure path, not a different failure shape.
  expect(beatResult.stderr).toMatch(/ARM_FORCE_COLLECT_FAILURES test seam/);

  // Fails loud, not open — no JSON body at all, never a resolved outcome
  // with a fabricated per-target reason.
  expect(beatResult.stdout).toBe('');

  // The work item the stalled agent held stays leased — a beat that fails
  // loudly must never have gone on to release it as if the recycle had
  // actually completed.
  const remainingAfterTotalCollectFailure = peekItems(ORCHESTRATOR_ID);
  expect(remainingAfterTotalCollectFailure).toHaveLength(1);
  expect(remainingAfterTotalCollectFailure[0].leaseId).toBeDefined();
});

// ---------------------------------------------------------------------------
// Regression guard (pre-PR review round 2, Medium — pins finding 4's
// fix: `resolveRecentlyEvictedPids` in cli.mjs, previously unit-tested at
// Phase 4 but wired nowhere in the real `--watchdog-beat` path). A pid whose
// eviction attempt FAILED on a prior beat (here, `permission-denied`, forced
// via `ARM_FAKE_KILL_RESULT`) must be held in a cooldown window
// (`DEFAULT_WATCHDOG_EVICT_COOLDOWN_MS`, 5 minutes) rather than re-attempted
// on every subsequent beat — the concrete bug this fix closes was an
// unconditional re-SIGTERM every beat with no backoff. After the cooldown
// window elapses, the same pid must become eligible for eviction again.
//
// Each beat below overrides `ARM_WATCHDOG_BACKOFF_FILE` to a fresh, unique
// path so the (separately-tested, see ./lib/watchdog-backoff.jest.spec.mjs)
// pacing beat-cadence gate never itself blocks a beat from running —
// this test isolates the cooldown behaviour, not the pacing cadence.
// `ARM_FAKE_NOW_MS` (the beat's own logical clock) is advanced across calls
// to move in/out of the cooldown window, while the liveness-log entry
// `resolveRecentlyEvictedPids` reads back is stamped with the REAL wall
// clock (`logWatchdogTrip`'s own `resolveNow` default, since `watchdog-
// beat.mjs` never threads an explicit `now` through to it) — so `nowMs` is
// anchored to `Date.now()` at test start to keep both clocks in the same
// neighbourhood.
// ---------------------------------------------------------------------------

it('holds a pid whose eviction attempt failed in a cooldown (skip, no re-attempt) inside the window, then makes it eligible again once the window elapses', async () => {
  const ORCHESTRATOR_ID = 'orch-watchdog-cooldown';
  seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // — see `declareDibsWithPid`'s own doc comment above.
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const boundMs = 10 * 60 * 1000;
  const nowMs = Date.now();
  const staleMtimeMs = nowMs - boundMs - 10 * 60 * 1000; // comfortably stale across every beat below

  const sharedEnv = {
    ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
    ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON: JSON.stringify({ '9001': [100, 100, 100, 100, 100] }),
    ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ '9001': staleMtimeMs }),
    ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
  };

  // Beat 1 — the pid is stalled and eviction is attempted but FAILS
  // (permission-denied). This is the failed attempt that must start the
  // cooldown clock.
  const beat1 = runCli(
    [
      '--watchdog-beat',
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      `--watchdog-bound-ms=${boundMs}`,
    ],
    baseEnv({
      ...sharedEnv,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_NOW_MS: String(nowMs),
      ARM_FAKE_KILL_RESULT: 'permission-denied',
      ARM_WATCHDOG_BACKOFF_FILE: join(workDir, 'backoff-beat1.json'),
    }),
  );
  expect(beat1.status).toBe(0);
  const body1 = JSON.parse(beat1.stdout);
  const trip1 = body1.trips.find((entry) => entry.pid === 9001);
  expect(trip1).toBeDefined();
  expect(trip1.evicted).toBe(false);
  expect(trip1.outcome).toBe('permission-denied');
  expect(body1.recycled ?? []).not.toContain(9001);

  const killLogAfterBeat1 = JSON.parse(await readFile(fakeKillLogPath, 'utf8'));
  expect(killLogAfterBeat1.length).toBeGreaterThan(0);

  // The failed attempt must not have released the leased item.
  expect(peekItems(ORCHESTRATOR_ID)[0].leaseId).toBeDefined();

  // Beat 2 — one minute later, still well inside the 5-minute cooldown
  // window. Must route to `skip` and must NOT attempt another kill.
  const beat2 = runCli(
    [
      '--watchdog-beat',
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      `--watchdog-bound-ms=${boundMs}`,
    ],
    baseEnv({
      ...sharedEnv,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_NOW_MS: String(nowMs + 60_000),
      ARM_FAKE_KILL_RESULT: 'permission-denied',
      ARM_WATCHDOG_BACKOFF_FILE: join(workDir, 'backoff-beat2.json'),
    }),
  );
  expect(beat2.status).toBe(0);
  const body2 = JSON.parse(beat2.stdout);
  const trip2 = body2.trips.find((entry) => entry.pid === 9001);
  expect(trip2).toBeDefined();
  expect(trip2.evicted).toBe(false);
  expect(trip2.outcome).toBe('skip');
  expect(body2.recycled ?? []).not.toContain(9001);

  const killLogAfterBeat2 = JSON.parse(await readFile(fakeKillLogPath, 'utf8'));
  expect(killLogAfterBeat2.length).toBe(killLogAfterBeat1.length);

  // — beat 1's own genuine recycle attempt refreshed this
  // orchestrator's dibs-ledger identity from `evictBeatIdentity`
  // (cli.mjs's own call-site-4 write), which resolves against THIS FILE'S
  // synthetic `ARM_FAKE_PS_OUTPUT` fixture — a tree that (by construction,
  // see PS_AUTHORIZED's own comment) contains no path from this beat's real
  // spawned-process ancestry to any claude-rooted pid, so that resolution
  // comes back `null` and the refresh legitimately OMITS `pid` entirely
  // (cli.mjs's own documented `evictBeatIdentity === null` degrade). In
  // production this never happens twice in a row for the SAME real
  // orchestrator process (its real ps-tree ancestry is stable beat to beat);
  // here it is purely this fixture's own synthetic-tree artifact. Re-seed
  // the matching pid before beat 3, exactly as a genuine orchestrator's own
  // next real (non-eviction) beat would have re-recorded it in production.
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  // Beat 3 — comfortably past the 5-minute cooldown window (301 seconds
  // later than beat 1). The pid must be eligible for eviction again — a new
  // attempt is made (this time forced to succeed) and the kill log grows.
  const beat3 = runCli(
    [
      '--watchdog-beat',
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      `--watchdog-bound-ms=${boundMs}`,
    ],
    baseEnv({
      ...sharedEnv,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_NOW_MS: String(nowMs + 301_000),
      ARM_FAKE_KILL_RESULT: '',
      ARM_WATCHDOG_BACKOFF_FILE: join(workDir, 'backoff-beat3.json'),
    }),
  );
  expect(beat3.status).toBe(0);
  const body3 = JSON.parse(beat3.stdout);
  const trip3 = body3.trips.find((entry) => entry.pid === 9001);
  expect(trip3).toBeDefined();
  expect(trip3.outcome).not.toBe('skip');

  const killLogAfterBeat3 = JSON.parse(await readFile(fakeKillLogPath, 'utf8'));
  expect(killLogAfterBeat3.length).toBeGreaterThan(killLogAfterBeat2.length);
});

// ---------------------------------------------------------------------------
// Phase 4 pre-PR review, High 2 — the real `--watchdog-beat` CLI path
// must actually reach the leak-velocity eviction wiring, not only
// `runWatchdogBeat` called directly with hand-built candidates (see
// ./watchdog-beat-leak-velocity-wiring.jest.spec.mjs's own tests, all of
// which construct `candidates` by hand and therefore never exercise
// `buildWatchdogCandidates` at all). This test seeds a REAL,
// already-persisted footprint-state.mjs trajectory (via `recordFootprintPoll`
// — never `ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON`, which carries no timestamp
// data and is therefore never wired into `leakVelocityState`, see
// `buildWatchdogCandidates`'s own doc comment in cli.mjs) showing sustained
// above-threshold growth for a pid this beat owns, and confirms that a real
// `cli.mjs --watchdog-beat` invocation (spawned as a real child process)
// evicts it end-to-end via the real `resolveEvictionTargets`/`handleEvict`
// core — closing the gap where the whole Phase 2 eviction-wiring feature was
// previously unreachable from the real production CLI entrypoint.
// ---------------------------------------------------------------------------

it('evicts a pid end-to-end via the real --watchdog-beat CLI path when its REAL, persisted footprint-state.mjs trajectory shows sustained above-threshold growth', async () => {
  const LEAK_PID = 9001;
  const ORCHESTRATOR_ID = 'orch-watchdog-real-leak-cli';
  const footprintStateFilePath = join(workDir, 'footprint-state.json');

  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  // — see `declareDibsWithPid`'s own doc comment above.
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const boundMs = 10 * 60 * 1000;
  const nowMs = Date.now();

  // Seed a REAL footprint-state.mjs trajectory directly via the same
  // already-shipped lib primitive `--poll-footprint` beats use in
  // production — sustained, uninterrupted growth across every consecutive
  // poll, comfortably clearing DEFAULT_LEAK_VELOCITY_THRESHOLDS'
  // 10 MB/min threshold across its 4 consecutivePollsRequired polls, one
  // poll every 10s (a plausible real --poll-footprint cadence).
  const LEAK_SAMPLES_MB = [200, 260, 330, 410, 500];
  const baseNow = nowMs - LEAK_SAMPLES_MB.length * 10_000;
  for (const [index, sampleMb] of LEAK_SAMPLES_MB.entries()) {
    // eslint-disable-next-line no-await-in-loop -- must fold in order; each poll depends on the prior poll's persisted state.
    await recordFootprintPoll(footprintStateFilePath, LEAK_PID, sampleMb, baseNow + index * 10_000, {
      agentClass: 'typescript-implementer',
    });
  }

  // Also flat/non-stalled on Phase 1's own signals, and NOT worktree-stale —
  // this beat must evict LEAK_PID for leak-velocity ALONE, never because it
  // also happens to look stalled.
  const activeMtimeMs = nowMs;

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
      ARM_FOOTPRINT_STATE_FILE: footprintStateFilePath,
      // Deliberately NOT set: ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON — this test's
      // whole point is exercising the REAL footprint-state.mjs read path,
      // not the fake seam.
      ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ '9001': activeMtimeMs }),
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
      ARM_FAKE_KILL_RESULT: '',
    }),
  );

  expect(beatResult.status).toBe(0);
  const body = JSON.parse(beatResult.stdout);

  expect(body.type).toBe('watchdog');
  const trip = body.trips.find((entry) => entry.pid === LEAK_PID);
  expect(trip).toBeDefined();
  expect(trip.reason).toBe('leak-velocity');
  expect(trip.evicted).toBe(true);
  expect(typeof trip.growthRateMbPerMin).toBe('number');
  expect(trip.growthRateMbPerMin).toBeGreaterThan(0);

  // Recycled via the real eviction mechanism — only the fake kill log
  // observed a signal, never a real process.kill syscall.
  expect(body.recycled).toContain(LEAK_PID);
  const killLog = JSON.parse(await readFile(fakeKillLogPath, 'utf8'));
  expect(killLog.length).toBeGreaterThan(0);
  expect(killLog.every((entry) => entry.pid === LEAK_PID)).toBe(true);
  expect(killLog[0].signal).toBe('SIGTERM');

  // The work item the leaking agent held is released back to the queue.
  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBeUndefined();
  expect(remaining[0].leaseId).not.toBe(leaseId);

  // The recycle is durably recorded in the liveness log with the
  // leak-velocity-specific detail field.
  const liveLog = await readLivenessLog(liveLogFilePath);
  const logEntry = liveLog.find((entry) => entry.pid === LEAK_PID);
  expect(logEntry).toBeDefined();
  expect(logEntry.detectionType).toBe('watchdog-trip');
  expect(logEntry.detail.growthRateMbPerMin).toBe(trip.growthRateMbPerMin);
});

// ---------------------------------------------------------------------------
// Phase 4 pre-PR review, High 1 — a `leak-velocity-throttled` trip
// (Step 5b's own `DEFAULT_MAX_LEAK_VELOCITY_EVICTIONS_PER_BEAT` cap) must
// NOT poison `resolveRecentlyEvictedPids`'s cooldown set: since no real
// evict attempt was made for a throttled pid, it must remain eligible for a
// genuine eviction on the VERY NEXT beat, not be suppressed for the full
// `DEFAULT_WATCHDOG_EVICT_COOLDOWN_MS` window as if it had actually been
// evicted (successfully or not).
// ---------------------------------------------------------------------------

it('does not suppress a leak-velocity-throttled pid from eviction on the next beat via the cooldown set', async () => {
  const THROTTLED_PID = 9001;
  const ORCHESTRATOR_ID = 'orch-watchdog-throttle-cooldown';
  const footprintStateFilePath = join(workDir, 'footprint-state.json');

  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const nowMs = Date.now();
  const boundMs = 10 * 60 * 1000;

  const LEAK_SAMPLES_MB = [200, 260, 330, 410, 500];
  const baseNow = nowMs - LEAK_SAMPLES_MB.length * 10_000;
  for (const [index, sampleMb] of LEAK_SAMPLES_MB.entries()) {
    // eslint-disable-next-line no-await-in-loop -- must fold in order; each poll depends on the prior poll's persisted state.
    await recordFootprintPoll(footprintStateFilePath, THROTTLED_PID, sampleMb, baseNow + index * 10_000, {
      agentClass: 'typescript-implementer',
    });
  }

  const sharedEnv = {
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
    ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
    ARM_FOOTPRINT_STATE_FILE: footprintStateFilePath,
    ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ '9001': nowMs }),
    ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
    ARM_FAKE_KILL_RESULT: '',
  };

  // Directly exercise `resolveRecentlyEvictedPids` in isolation: seed a
  // `watchdog-trip` liveness-log entry with `outcome: 'leak-velocity-throttled'`
  // for THROTTLED_PID, well within the cooldown window, then confirm a
  // SUBSEQUENT beat still treats it as eligible (never routed to `skip`
  // purely because of that throttled entry).
  await logWatchdogTrip(
    liveLogFilePath,
    {
      pid: THROTTLED_PID,
      orchestratorId: ORCHESTRATOR_ID,
      outcome: 'leak-velocity-throttled',
      reason: 'leak-velocity',
      growthRateMbPerMin: 40,
    },
    { now: nowMs },
  );

  const beat = runCli(
    [
      '--watchdog-beat',
      `--orchestrator-id=${ORCHESTRATOR_ID}`,
      '--orchestrator-pid=501',
      `--watchdog-bound-ms=${boundMs}`,
    ],
    baseEnv({ ...sharedEnv, ARM_FAKE_NOW_MS: String(nowMs + 60_000) }),
  );
  expect(beat.status).toBe(0);
  const body = JSON.parse(beat.stdout);
  const trip = body.trips.find((entry) => entry.pid === THROTTLED_PID);
  expect(trip).toBeDefined();
  // Must NOT be `skip` — a genuinely throttled-but-never-attempted pid must
  // not be treated as "recently evicted" and suppressed via cooldown.
  expect(trip.outcome).not.toBe('skip');
  expect(trip.evicted).toBe(true);
  expect(body.recycled).toContain(THROTTLED_PID);

  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBeUndefined();
  expect(remaining[0].leaseId).not.toBe(leaseId);
});

// ---------------------------------------------------------------------------
// pre-PR review round 2, High — `buildLeakVelocityStateForCandidate`'s
// "synthetic final pair" replay must trip a STEADILY-leaking pid (constant,
// uninterrupted above-threshold growth every poll) at EVERY trajectory
// length from `consecutivePollsRequired + 1` through
// `2 * consecutivePollsRequired + 4` (5 through 12, given the default
// `consecutivePollsRequired: 4`) via the real `--watchdog-beat` CLI path —
// not only the lengths that happen to land on an exact multiple of
// `consecutivePollsRequired`. Prior to this fix, only lengths 5 and 9
// tripped; 6, 7, 8, 10, 11, 12 wrongly refused to evict. See
// `buildLeakVelocityStateForCandidate`'s own updated doc comment in cli.mjs
// for the corrected "is the trip currently live" semantics this test pins.
// ---------------------------------------------------------------------------

it.each([5, 6, 7, 8, 9, 10, 11, 12])(
  'evicts a steadily-leaking pid via the real --watchdog-beat CLI path at trajectory length %i',
  async (trajectoryLength) => {
    const LEAK_PID = 9001;
    const ORCHESTRATOR_ID = `orch-watchdog-real-leak-length-${trajectoryLength}`;
    const footprintStateFilePath = join(workDir, 'footprint-state.json');

    seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
    await declareDibsWithPid(ORCHESTRATOR_ID, 501);

    const boundMs = 10 * 60 * 1000;
    const nowMs = Date.now();

    // Steady, uninterrupted growth every poll: +100MB every 10s => 600
    // MB/min per pair, comfortably above the 10 MB/min default threshold,
    // never interrupted, for `trajectoryLength` consecutive samples.
    const leakSamplesMb = Array.from({ length: trajectoryLength }, (_, index) => 200 + index * 100);
    const baseNow = nowMs - leakSamplesMb.length * 10_000;
    for (const [index, sampleMb] of leakSamplesMb.entries()) {
      // eslint-disable-next-line no-await-in-loop -- must fold in order; each poll depends on the prior poll's persisted state.
      await recordFootprintPoll(footprintStateFilePath, LEAK_PID, sampleMb, baseNow + index * 10_000, {
        agentClass: 'typescript-implementer',
      });
    }

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
        ARM_FOOTPRINT_STATE_FILE: footprintStateFilePath,
        ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ '9001': nowMs }),
        ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
        ARM_FAKE_KILL_RESULT: '',
      }),
    );

    expect(beatResult.status).toBe(0);
    const body = JSON.parse(beatResult.stdout);
    const trip = body.trips.find((entry) => entry.pid === LEAK_PID);
    expect(trip).toBeDefined();
    expect(trip.reason).toBe('leak-velocity');
    expect(trip.evicted).toBe(true);
    expect(body.recycled).toContain(LEAK_PID);
  },
);

// ---------------------------------------------------------------------------
// pre-PR review round 2, High — a pid that leaked for a while and then
// GENUINELY stopped growing must NOT be treated as currently tripped just
// because it once crossed the sustained-count threshold earlier in its
// trajectory. Growth for the first several polls, then flat (below
// threshold) for the remaining, most-recent polls.
// ---------------------------------------------------------------------------

it('does not evict a pid that leaked for a while and then genuinely stopped growing, even though it tripped earlier in its trajectory', async () => {
  const STOPPED_PID = 9001;
  const ORCHESTRATOR_ID = 'orch-watchdog-leak-stopped';
  const footprintStateFilePath = join(workDir, 'footprint-state.json');

  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const boundMs = 10 * 60 * 1000;
  const nowMs = Date.now();

  // Sustained growth for the first 5 raw samples (trips at least once), then
  // flat (identical raw sample) for many more polls. `recordFootprintPoll`
  // persists an EWMA-SMOOTHED trajectory (alpha 0.3, see footprint-state.mjs),
  // not the raw samples directly, so the smoothed value only asymptotically
  // approaches the flat raw value — enough trailing flat polls are supplied
  // here for the smoothed final-pair delta to genuinely fall below the 10
  // MB/min default threshold (at a 10s poll cadence, a threshold-clearing
  // delta needs < ~1.7MB between the final two smoothed samples).
  const leakSamplesMb = [200, 260, 330, 410, 500, ...Array(18).fill(500)];
  const baseNow = nowMs - leakSamplesMb.length * 10_000;
  for (const [index, sampleMb] of leakSamplesMb.entries()) {
    // eslint-disable-next-line no-await-in-loop -- must fold in order; each poll depends on the prior poll's persisted state.
    await recordFootprintPoll(footprintStateFilePath, STOPPED_PID, sampleMb, baseNow + index * 10_000, {
      agentClass: 'typescript-implementer',
    });
  }

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
      ARM_FOOTPRINT_STATE_FILE: footprintStateFilePath,
      ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ '9001': nowMs }),
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
      ARM_FAKE_KILL_RESULT: '',
    }),
  );

  expect(beatResult.status).toBe(0);
  const body = JSON.parse(beatResult.stdout);
  const trip = body.trips.find((entry) => entry.pid === STOPPED_PID);
  expect(trip).toBeUndefined();
  expect(body.recycled).not.toContain(STOPPED_PID);

  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

// ---------------------------------------------------------------------------
// pre-PR review round 2, High — a pid whose sustained streak is ONE
// poll away from `consecutivePollsRequired` (never yet reached a full
// sustained window) must not trip, even though every poll so far is above
// threshold.
// ---------------------------------------------------------------------------

it('does not evict a pid whose above-threshold streak is one poll away from a sustained trip', async () => {
  const ALMOST_PID = 9001;
  const ORCHESTRATOR_ID = 'orch-watchdog-leak-almost';
  const footprintStateFilePath = join(workDir, 'footprint-state.json');

  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const boundMs = 10 * 60 * 1000;
  const nowMs = Date.now();

  // Exactly 3 pairs (4 samples) of above-threshold growth — one poll short
  // of the default consecutivePollsRequired: 4.
  const leakSamplesMb = [200, 260, 330, 410];
  const baseNow = nowMs - leakSamplesMb.length * 10_000;
  for (const [index, sampleMb] of leakSamplesMb.entries()) {
    // eslint-disable-next-line no-await-in-loop -- must fold in order; each poll depends on the prior poll's persisted state.
    await recordFootprintPoll(footprintStateFilePath, ALMOST_PID, sampleMb, baseNow + index * 10_000, {
      agentClass: 'typescript-implementer',
    });
  }

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
      ARM_FOOTPRINT_STATE_FILE: footprintStateFilePath,
      ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ '9001': nowMs }),
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
      ARM_FAKE_KILL_RESULT: '',
    }),
  );

  expect(beatResult.status).toBe(0);
  const body = JSON.parse(beatResult.stdout);
  const trip = body.trips.find((entry) => entry.pid === ALMOST_PID);
  expect(trip).toBeUndefined();
  expect(body.recycled).not.toContain(ALMOST_PID);

  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

// ---------------------------------------------------------------------------
// pre-PR review round 3, High — the round-2 `everTripped` PERMANENT
// LATCH regression: an early, unrelated trip anywhere in the retained
// trajectory must NOT grant a later, short-lived above-threshold run credit
// toward eviction. This is the exact shape round 2's `everTripped &&
// finalPairAboveThreshold` check got wrong: `everTripped` latches true the
// first time any pair reaches a full sustained count and never resets, so a
// pid that tripped once early, then plateaued for a long stretch (genuinely
// resetting the real, persisted-counter-equivalent streak), then grew again
// for only a SHORT run that has not yet re-reached
// `consecutivePollsRequired`, was wrongly evicted after just one more
// above-threshold poll. The corrected trailing-run count in
// `buildLeakVelocityStateForCandidate` requires an unbroken CURRENT run of
// `consecutivePollsRequired` pairs, with no credit carried over from any
// earlier, already-reset trip.
//
// Raw samples: a steady early ramp (200→600, trips at least once by pair 4),
// then an 18-poll flat plateau at 600 — long enough for the EWMA-smoothed
// (alpha 0.3) pair-to-pair delta to converge to genuinely below the 10
// MB/min default threshold, fully resetting the streak — then exactly two
// more above-threshold polls (600→700→800). Two consecutive above-threshold
// pairs is one short of the default `consecutivePollsRequired: 4`, so the
// trailing run at the end of the trajectory is 2, not 4: this must NOT
// evict, even though an earlier stretch of the same trajectory did trip.
// ---------------------------------------------------------------------------

it('does not evict a pid whose renewed above-threshold run has not yet re-reached the sustained count, even though an earlier stretch of the same trajectory tripped and then genuinely plateaued', async () => {
  const RENEWED_PID = 9001;
  const ORCHESTRATOR_ID = 'orch-watchdog-leak-renewed-short-run';
  const footprintStateFilePath = join(workDir, 'footprint-state.json');

  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const boundMs = 10 * 60 * 1000;
  const nowMs = Date.now();

  const leakSamplesMb = [200, 300, 400, 500, 600, ...Array(18).fill(600), 700, 800];
  const baseNow = nowMs - leakSamplesMb.length * 10_000;
  for (const [index, sampleMb] of leakSamplesMb.entries()) {
    // eslint-disable-next-line no-await-in-loop -- must fold in order; each poll depends on the prior poll's persisted state.
    await recordFootprintPoll(footprintStateFilePath, RENEWED_PID, sampleMb, baseNow + index * 10_000, {
      agentClass: 'typescript-implementer',
    });
  }

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
      ARM_FOOTPRINT_STATE_FILE: footprintStateFilePath,
      ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ '9001': nowMs }),
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
      ARM_FAKE_KILL_RESULT: '',
    }),
  );

  expect(beatResult.status).toBe(0);
  const body = JSON.parse(beatResult.stdout);
  const trip = body.trips.find((entry) => entry.pid === RENEWED_PID);
  expect(trip).toBeUndefined();
  expect(body.recycled).not.toContain(RENEWED_PID);

  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

// ---------------------------------------------------------------------------
// pre-PR review round 2, Medium — the real eviction path
// (`buildLeakVelocityStateForCandidate`/`buildWatchdogCandidates`) must skip
// a footprint-state entry that has not been polled within
// `DEFAULT_LIVENESS_THRESHOLD_MS` (15 minutes) — the same staleness guard
// `computeLeakBlocksForBeat`'s advisory echo already applies. A live
// candidate pid whose footprint polling stopped long ago (or a
// macOS-recycled pid inheriting a stale trajectory from an unrelated earlier
// process) must never be evicted on stale evidence.
// ---------------------------------------------------------------------------

it('excludes a stale footprint-state entry from leak-velocity eviction consideration', async () => {
  const STALE_PID = 9001;
  const ORCHESTRATOR_ID = 'orch-watchdog-leak-stale';
  const footprintStateFilePath = join(workDir, 'footprint-state.json');

  const { leaseId } = seedLeasedItem({ orchestratorId: ORCHESTRATOR_ID });
  await declareDibsWithPid(ORCHESTRATOR_ID, 501);

  const boundMs = 10 * 60 * 1000;
  const nowMs = Date.now();

  // A sustained-looking leak trajectory, but its LAST poll happened 20
  // minutes before `nowMs` — well beyond DEFAULT_LIVENESS_THRESHOLD_MS's own
  // 15-minute bound.
  const staleBaseNow = nowMs - 20 * 60 * 1000;
  const leakSamplesMb = [200, 260, 330, 410, 500];
  for (const [index, sampleMb] of leakSamplesMb.entries()) {
    // eslint-disable-next-line no-await-in-loop -- must fold in order; each poll depends on the prior poll's persisted state.
    await recordFootprintPoll(footprintStateFilePath, STALE_PID, sampleMb, staleBaseNow + index * 10_000, {
      agentClass: 'typescript-implementer',
    });
  }

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
      ARM_FOOTPRINT_STATE_FILE: footprintStateFilePath,
      // Fresh worktree mtime — isAgentStalled's AND-semantics must never
      // fire here either, so this test isolates the leak-velocity staleness
      // guard alone.
      ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ '9001': nowMs }),
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
      ARM_FAKE_KILL_RESULT: '',
    }),
  );

  expect(beatResult.status).toBe(0);
  const body = JSON.parse(beatResult.stdout);
  const trip = body.trips.find((entry) => entry.pid === STALE_PID);
  expect(trip).toBeUndefined();
  expect(body.recycled).not.toContain(STALE_PID);

  const remaining = peekItems(ORCHESTRATOR_ID);
  expect(remaining).toHaveLength(1);
  expect(remaining[0].leaseId).toBe(leaseId);
});

// ---------------------------------------------------------------------------
// — per-beat evictBeatIdentity hoist. See ./cli-evict.jest.spec.mjs's
// own "Scenario 21" header for why the sibling structural tests over there
// (pinning `performEviction`'s new `cachedEvictBeatIdentity` parameter shape
// and the single-target `--evict-pid` call site's regression guard) are
// structural rather than behavioral. The tests below are the genuinely
// BEHAVIORAL half of the same fix, made possible here — but not over
// there — because a `--watchdog-beat` invocation with 2+ evict targets is
// the one shape that lets the ALREADY-SHIPPED `ARM_FAKE_PS_COLLECTION_LOG`
// seam (`recordProcessSnapshotCollection`, cli.mjs; see
// ./cli-evict.jest.spec.mjs's own "counts the eviction beat's own
// process-snapshot collections" test for the precedent this borrows) tell
// "resolved once for the whole beat" apart from "resolved once per target"
// by COUNT alone — no dependence on the fake ps tree's VALUE ever changing
// mid-run, which this project's `ARM_FAKE_PS_OUTPUT` seam cannot do (see
// ./cli-evict.jest.spec.mjs's own Scenario 21 header for why that rules out
// simulating genuine per-call ps-flakiness black-box).
//
// A DELIBERATE, VERIFIED CHOICE OF FIXTURE: `evictBeatIdentity` must resolve
// to a NON-null identity for BOTH targets to reach the code this ticket
// changes at all — `resolveNearestClaudeRootIdentity` walks UP the ps-tree
// from the beat's own REAL `process.ppid` (the Jest worker's real pid, since
// `cli.mjs` runs as a real spawned child of it — see `runCli`), which no
// hardcoded synthetic pid can represent. `PS_TWO_AGENTS_WITH_RESOLVABLE_ROOT`
// below inserts a real ps row keyed on `process.pid` (this test file's own
// pid, i.e. the spawned child's real ppid) chained up to a synthetic
// `/claude`-rooted ancestor whose OWN pid equals `--orchestrator-pid`
// (`501`) — the one combination that makes `resolveNearestClaudeRootIdentity`
// return `{ pid: 501, ... }`, matching the flag exactly, so BOTH targets'
// orchestrator-pid-binding checks pass and BOTH genuinely reach the
// evictBeatIdentity-resolution + fresh-authorization-sample code twice
// each — the precondition for a call-count difference to be observable at
// all. (A DETERMINISTICALLY-`null`-resolving fixture, by contrast, produces
// a *different*, already-existing, NOT-fixed-by-this-ticket cascade — see
// the dedicated regression test further below for why that shape is
// unaffected by this hoist and is not this ticket's concern.)
// ---------------------------------------------------------------------------

/**
 * Two independent, genuinely own-spawned/authorized claude-rooted agent
 * trees under the SAME orchestrator root (501) — extends `PS_AUTHORIZED`
 * (single agent, pid 9001) with a second sibling agent root at pid 9101, so
 * `buildWatchdogCandidates` discovers TWO distinct `agentId`s in one beat.
 * Every candidate PID here is SYNTHETIC (see `PS_AUTHORIZED`'s own doc
 * comment) — only the `<REAL_PPID>` row (substituted at call time with this
 * test process's own real `process.pid`) refers to a real OS entity, and
 * even that row is never itself the target of any authorization/kill
 * decision, only the ancestor `resolveNearestClaudeRootIdentity` walks
 * through to find the synthetic, resolvable `/claude`-rooted pid 501.
 */
function buildTwoAgentPsWithResolvableRoot(realPpid) {
  return `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
${realPpid}   501   1024 1-03:30:00 /usr/libexec/bash
  501     1   1024 1-03:30:00 /Applications/Claude.app/Contents/MacOS/claude
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9101   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
`;
}

it(
  'resolves evictBeatIdentity exactly once for a multi-target watchdog beat, not once per target — proven by a real ARM_FAKE_PS_COLLECTION_LOG call-count, not an inferred value (RED until performEviction/handleWatchdogBeat hoist the resolution)',
  async () => {
    const ORCHESTRATOR_ID = 'orch-watchdog-identity-hoist';
    const PID_A = 9001;
    const PID_B = 9101;

    seedLeasedItem({
      orchestratorId: ORCHESTRATOR_ID,
      agentClass: 'typescript-implementer',
      commandRef: 'resumable-workflow --resume=1373-a',
    });
    seedLeasedItem({
      orchestratorId: ORCHESTRATOR_ID,
      agentClass: 'typescript-implementer',
      commandRef: 'resumable-workflow --resume=1373-b',
    });
    // — a genuine prior beat recorded this orchestrator's own root pid
    // (501) on the ledger, matching `--orchestrator-pid=501` below, so
    // `orchestratorIsLive` is true and the orchestrator-pid-binding check
    // passes for BOTH targets — the precondition for `evictBeatIdentity` to
    // be computed AT ALL (see cli.mjs's own `if (orchestratorIsLive) {
    // ... evictBeatIdentity ... }` gate).
    await declareDibsWithPid(ORCHESTRATOR_ID, 501);

    const boundMs = 10 * 60 * 1000;
    const nowMs = Date.now();
    const staleMtimeMs = nowMs - boundMs - 60_000;

    const collectionLogPath = join(workDir, 'watchdog-ps-collections.log');
    await writeFile(collectionLogPath, '', 'utf8');

    const beatResult = runCli(
      [
        '--watchdog-beat',
        `--orchestrator-id=${ORCHESTRATOR_ID}`,
        '--orchestrator-pid=501',
        `--watchdog-bound-ms=${boundMs}`,
      ],
      baseEnv({
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: buildTwoAgentPsWithResolvableRoot(process.pid),
        ARM_FAKE_PS_COLLECTION_LOG: collectionLogPath,
        ARM_FAKE_NOW_MS: String(nowMs),
        ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON: JSON.stringify({
          [String(PID_A)]: [100, 100, 100, 100, 100],
          [String(PID_B)]: [100, 100, 100, 100, 100],
        }),
        ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({
          [String(PID_A)]: staleMtimeMs,
          [String(PID_B)]: staleMtimeMs,
        }),
        ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
        // 'already-gone' short-circuits gracefulStop BEFORE the grace-period
        // sleep and BEFORE `verifyStillTarget` is ever consulted (see
        // gracefulStop's own `if (termResult === 'already-gone') return
        // ...` — no further ps collection happens on this path), which is
        // what keeps this test's collection-count arithmetic exact rather
        // than dependent on grace-period timing/kill-sequence shape.
        ARM_FAKE_KILL_RESULT: 'already-gone',
      }),
    );

    expect(beatResult.status).toBe(0);
    const body = JSON.parse(beatResult.stdout);
    expect(body.type).toBe('watchdog');

    // Sanity first: both targets were genuinely detected AND genuinely
    // recycled, via the SAME resolvable identity (pid 501, matching
    // `--orchestrator-pid`) — a beat that silently dropped target B (e.g.
    // because the 1:1 candidate/lease pairing broke) would trivially "pass"
    // a once-per-beat collection count for the wrong reason.
    const tripA = body.trips.find((entry) => entry.pid === PID_A);
    const tripB = body.trips.find((entry) => entry.pid === PID_B);
    expect(tripA).toBeDefined();
    expect(tripB).toBeDefined();
    expect(tripA.evicted).toBe(true);
    expect(tripB.evicted).toBe(true);
    expect(tripA.outcome).not.toBe('orchestrator-pid-mismatch');
    expect(tripB.outcome).not.toBe('orchestrator-pid-mismatch');
    expect(body.recycled).toEqual(expect.arrayContaining([PID_A, PID_B]));

    const collections = (await readFile(collectionLogPath, 'utf8')).split('\n').filter(Boolean);

    // The exact count, not merely ">0" — see ./cli-evict.jest.spec.mjs's own
    // "counts the eviction beat's own process-snapshot collections" test for
    // why an exact bound is the honest claim here, not a loose one.
    //
    // Arithmetic (GREEN memory + 'already-gone' kill result, so neither
    // buildFallback's pause-candidate path nor gracefulStop's
    // verifyStillTarget callback ever collects an EXTRA sample; a
    // successfully-resolving identity, so neither target's own
    // orchestrator-pid-binding check refuses it before reaching the counted
    // call sites — see this section's own header for why that resolvability
    // is what this fixture is built to guarantee):
    //   1  — buildWatchdogCandidates' own ownership-discovery collection
    //        (once per beat, unrelated to this fix, unchanged)
    //   1  — evictBeatIdentity, resolved ONCE for the whole beat (the fix)
    //   +1 per target — the authorization sample immediately before
    //        isEvictionAuthorized, which MUST stay per-target (Scenario 4's
    //        own concern) — 2 targets here.
    //   = 1 + 1 + 2 = 4
    //
    // RED today: `performEviction` resolves `evictBeatIdentity` fresh on
    // EVERY call (once per target, not once per beat), so today's actual
    // count is 1 + 2 + 2 = 5 (verified against the current, unfixed code).
    // This assertion fails until `performEviction` accepts a per-beat-cached
    // identity and `handleWatchdogBeat` supplies it once, mirroring the
    // existing `getPreTakenSample()` hoist.
    expect(collections).toHaveLength(4);
  },
);

it(
  'still resolves the authorization ps-tree sample fresh, once per target, in the SAME multi-target beat where evictBeatIdentity is now cached (Scenario 4 — the two-snapshot split survives the hoist)',
  async () => {
    // Reuses the exact fixture/arithmetic from the test immediately above —
    // this is a targeted contrast, not a new scenario: if a future
    // implementation incorrectly hoisted the AUTHORIZATION sample too (an
    // over-fix conflating the two snapshots the Investigation explicitly
    // flags as high-risk to conflate), the collection count would drop to 3
    // (1 discovery + 1 identity + 1 shared auth) instead of the correct 4 —
    // this test's own assertion below distinguishes that over-fix from the
    // correct one by the same count-based mechanism.
    const ORCHESTRATOR_ID = 'orch-watchdog-identity-hoist-auth-split';
    const PID_A = 9001;
    const PID_B = 9101;

    seedLeasedItem({
      orchestratorId: ORCHESTRATOR_ID,
      agentClass: 'typescript-implementer',
      commandRef: 'resumable-workflow --resume=1373-a',
    });
    seedLeasedItem({
      orchestratorId: ORCHESTRATOR_ID,
      agentClass: 'typescript-implementer',
      commandRef: 'resumable-workflow --resume=1373-b',
    });
    await declareDibsWithPid(ORCHESTRATOR_ID, 501);

    const boundMs = 10 * 60 * 1000;
    const nowMs = Date.now();
    const staleMtimeMs = nowMs - boundMs - 60_000;

    const collectionLogPath = join(workDir, 'watchdog-ps-collections-auth-split.log');
    await writeFile(collectionLogPath, '', 'utf8');

    const beatResult = runCli(
      [
        '--watchdog-beat',
        `--orchestrator-id=${ORCHESTRATOR_ID}`,
        '--orchestrator-pid=501',
        `--watchdog-bound-ms=${boundMs}`,
      ],
      baseEnv({
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: buildTwoAgentPsWithResolvableRoot(process.pid),
        ARM_FAKE_PS_COLLECTION_LOG: collectionLogPath,
        ARM_FAKE_NOW_MS: String(nowMs),
        ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON: JSON.stringify({
          [String(PID_A)]: [100, 100, 100, 100, 100],
          [String(PID_B)]: [100, 100, 100, 100, 100],
        }),
        ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({
          [String(PID_A)]: staleMtimeMs,
          [String(PID_B)]: staleMtimeMs,
        }),
        ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
        ARM_FAKE_KILL_RESULT: 'already-gone',
      }),
    );

    expect(beatResult.status).toBe(0);
    const body = JSON.parse(beatResult.stdout);
    const tripA = body.trips.find((entry) => entry.pid === PID_A);
    const tripB = body.trips.find((entry) => entry.pid === PID_B);
    expect(tripA?.evicted).toBe(true);
    expect(tripB?.evicted).toBe(true);

    const collections = (await readFile(collectionLogPath, 'utf8')).split('\n').filter(Boolean);
    // 4, never 3 — see this test's own header for what a count of 3 would
    // mean (the authorization sample wrongly hoisted alongside identity).
    expect(collections).toHaveLength(4);
    expect(collections.length).not.toBe(3);
  },
);

it(
  "the fail-safe strip-on-null declareDibs write, and the pre-existing (NOT this ticket's concern) per-target readDibs re-read it feeds, are BOTH unchanged in kind by the hoist — a deterministically-unresolvable identity still strips the ledger the same way, and a later target in the SAME beat still observes that strip via its own fresh readDibs (Scenario 5 — regression guard, not a new behavior)",
  async () => {
    // Deliberately the OTHER fixture shape from the two tests above: this
    // file's real `ARM_FAKE_PS_OUTPUT` never chains this test process's real
    // ppid up to a resolvable `/claude`-rooted ancestor, so
    // `evictBeatIdentity` resolves `null` — deterministically, on every call,
    // whether resolved once (hoisted) or once per target (today). This is
    // NOT the scenario this ticket's hoist changes the OUTCOME of (only a
    // genuinely per-call-FLAKY resolution — impossible to simulate
    // black-box, see this file's own Scenario 21 cross-reference above —
    // benefits from being resolved once instead of N times); a
    // deterministically-failing resolution fails identically for every
    // target regardless of hoisting, because each target's own `declareDibs`
    // WRITE (not the identity RESOLUTION this ticket hoists) still runs once
    // per target, and target A's own write still strips the ledger entry
    // that target B's own, still-per-target `readDibs` still reads before
    // target B ever reaches its own identity resolution. This test pins
    // that this is UNCHANGED — the fail-safe direction this change chose,
    // preserved in kind, not eliminated by this ticket.
    const ORCHESTRATOR_ID = 'orch-watchdog-identity-hoist-strip';
    const PID_A = 9001;
    const PID_B = 9101;

    seedLeasedItem({
      orchestratorId: ORCHESTRATOR_ID,
      agentClass: 'typescript-implementer',
      commandRef: 'resumable-workflow --resume=1373-a',
    });
    seedLeasedItem({
      orchestratorId: ORCHESTRATOR_ID,
      agentClass: 'typescript-implementer',
      commandRef: 'resumable-workflow --resume=1373-b',
    });
    await declareDibsWithPid(ORCHESTRATOR_ID, 501);

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
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED_TWO_AGENTS,
        ARM_FAKE_NOW_MS: String(nowMs),
        ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON: JSON.stringify({
          [String(PID_A)]: [100, 100, 100, 100, 100],
          [String(PID_B)]: [100, 100, 100, 100, 100],
        }),
        ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({
          [String(PID_A)]: staleMtimeMs,
          [String(PID_B)]: staleMtimeMs,
        }),
        ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
        ARM_FAKE_KILL_RESULT: 'already-gone',
      }),
    );

    expect(beatResult.status).toBe(0);
    const body = JSON.parse(beatResult.stdout);
    const tripA = body.trips.find((entry) => entry.pid === PID_A);
    const tripB = body.trips.find((entry) => entry.pid === PID_B);
    // Target A is processed first, before its own write has stripped
    // anything — it succeeds exactly as the single-target outer acceptance
    // test above does under the same null-resolving fixture.
    expect(tripA?.evicted).toBe(true);
    expect(tripA?.outcome).toBe('already-gone');
    // Target B's own, still-per-target `readDibs` (unaffected by this
    // ticket's identity-only hoist) observes the ledger AFTER target A's own
    // write stripped it — refused, exactly as today, exactly as its own
    // Investigation/Convergence Analysis document as an accepted, unchanged
    // limitation of a DETERMINISTICALLY (not flakily) failing resolution.
    expect(tripB?.evicted).toBe(false);
    expect(tripB?.outcome).toBe('orchestrator-pid-mismatch');

    // The ledger's own entry for this orchestrator, read back directly —
    // stripped of `pid`/`pidStartedAt` by target A's own declareDibs write,
    // exactly as today's per-target resolution already produces for a
    // single-target beat (see `declareDibsWithPid`'s own doc comment on the
    // sibling recycle test above for the pre-existing, single-target version
    // of this same degrade).
    const dibs = await readDibs(coordinationFilePath, Date.now(), { livenessThresholdMs: 15 * 60 * 1000 });
    const entry = dibs.find((candidate) => candidate.orchestratorId === ORCHESTRATOR_ID);
    expect(entry).toBeDefined();
    expect(entry.pid).toBeUndefined();
    expect(entry.pidStartedAt).toBeUndefined();
  },
);

/**
 * Two genuinely own-spawned/authorized claude-rooted agent trees under the
 * same orchestrator root (501), with NO resolvable ancestor for this test
 * process's own real ppid — `evictBeatIdentity` therefore resolves `null`
 * deterministically, mirroring `PS_AUTHORIZED`'s own single-agent fixture
 * and its documented "comes back null... purely this fixture's own
 * synthetic-tree artifact" behavior (see `declareDibsWithPid`'s own doc
 * comment above).
 */
const PS_AUTHORIZED_TWO_AGENTS = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9101   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
`;

// ---------------------------------------------------------------------------
// — structural pin on `handleWatchdogBeat`'s own hoist SHAPE, mirroring
// its existing `cachedSamplePromise`/`getPreTakenSample()` precedent for the
// machine-resource sample (see cli.mjs's own comment ahead of that hoist,
// "Hoisted here to once PER BEAT"). This is additive to, not a substitute
// for, the behavioral call-count tests above: those prove the OBSERVABLE
// effect; this pins the expected IMPLEMENTATION SHAPE for whoever builds
// Phase 1, so a fix that achieves the right call count by some entirely
// different (and harder to review) mechanism is flagged for reviewer
// attention rather than silently accepted.
// ---------------------------------------------------------------------------

it('handleWatchdogBeat resolves evictBeatIdentity via a lazy, once-per-beat cache and threads it into every performEviction call as cachedEvictBeatIdentity (structural — pins the expected implementation shape)', async () => {
  const source = await readFile(CLI, 'utf8');
  const fnStart = source.indexOf('async function handleWatchdogBeat(flags)');
  expect(fnStart).toBeGreaterThan(-1);
  const fnEnd = source.indexOf('\n// ---', fnStart);
  expect(fnEnd).toBeGreaterThan(fnStart);
  const body = source.slice(fnStart, fnEnd);

  // A lazy cache variable exists in this function's own scope — mirroring
  // `getPreTakenSample`'s `cachedSamplePromise` shape (declared with `let`,
  // populated on first use, reused thereafter).
  expect(body).toMatch(/cachedEvictBeatIdentity/);

  // The `performEviction({...})` call inside `handleEvictInProcess` passes
  // it through as `cachedEvictBeatIdentity:` — the SAME call site that
  // already threads `preTakenSample` through via `getPreTakenSample()`.
  const callIndex = body.indexOf('await performEviction({');
  expect(callIndex).toBeGreaterThan(-1);
  const callEnd = body.indexOf('});', callIndex);
  expect(callEnd).toBeGreaterThan(callIndex);
  const callArgs = body.slice(callIndex, callEnd);
  expect(callArgs).toMatch(/preTakenSample,/);
  expect(callArgs).toMatch(/cachedEvictBeatIdentity:/);
});
