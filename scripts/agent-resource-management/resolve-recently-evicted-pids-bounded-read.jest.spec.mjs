// Phase 2 — RED tests for a bounded (backward/tail) read of the
// liveness log inside `resolveRecentlyEvictedPids` (cli.mjs), replacing
// today's `readLivenessLog(liveLogFilePath)` call, which `JSON.parse`s EVERY
// line of the (Phase-1-bounded-to-2000-entries) live NDJSON file on every
// single watchdog beat, purely to find `watchdog-trip`/`deadlock-recycle`
// entries within a trailing `cooldownMs` window (5 minutes in production,
// `DEFAULT_WATCHDOG_EVICT_COOLDOWN_MS` = `DEFAULT_WATCHDOG_CRON_INTERVAL_MS`).
//
// Production code is NOT changed by this phase. An earlier draft of this
// phase added `export` to `resolveRecentlyEvictedPids`'s own `async
// function` keyword in cli.mjs "so a future direct-import white-box test
// remains possible" — that export was reverted (pre-PR review,): it
// had zero importers anywhere in the repo, every test here and in sibling
// files exercises it exclusively black-box via `spawnSync`, and keeping it
// exported was actively dangerous (see below for why directly importing
// cli.mjs is unsafe) rather than merely unused. `resolveRecentlyEvictedPids`
// stays module-private; this file continues to follow the same black-box
// idiom it always has. `cli.mjs`'s own header comment documents that this
// file is "exercised black-box, via real process spawning ... never
// imported directly by a unit test" for a concrete, mechanical reason — `main()` at
// the bottom of the file executes UNCONDITIONALLY on module load
// (`main().catch(...)`, no `import.meta.url === process.argv[1]` guard), so
// `import`-ing cli.mjs inside this Jest worker would parse Jest's own
// `process.argv`, likely fall through to the default bare-beat dispatch,
// and perform REAL `collect()`/coordination-file I/O against whatever
// ambient env this worker process happens to have — exactly the class of
// hazard every sibling `cli-*.jest.spec.mjs` file avoids via `spawnSync`.
// This file follows that same established idiom, and specifically mirrors
// `watchdog-outer-acceptance.jest.spec.mjs`'s own existing cooldown
// regression test (search that file for "holds a pid whose eviction attempt
// failed in a cooldown") — the ALREADY-PROVEN pattern for exercising this
// exact function's observable behaviour through a real `--watchdog-beat`
// CLI invocation.
//
// ---------------------------------------------------------------------------
// HOW EACH REQUIRED SCENARIO IS PROVEN BLACK-BOX
// ---------------------------------------------------------------------------
//
// Every scenario below seeds the liveness-log NDJSON file DIRECTLY (via
// `logWatchdogTrip`/raw appended lines — a lib-level primitive, not cli.mjs
// itself, exactly like `watchdog-outer-acceptance.jest.spec.mjs`'s own
// `readLivenessLog`/`logWatchdogTrip` imports), then runs exactly ONE real
// `--watchdog-beat` invocation against a single synthetic stalled candidate
// (pid 9001, mirroring every sibling file's `PS_AUTHORIZED` fixture) and
// reads the resolved cooldown status off that ONE trip's own
// `outcome` field: `'skip'` means `resolveRecentlyEvictedPids` returned a
// Set containing 9001; any other outcome (`ARM_FAKE_KILL_RESULT` is forced
// to the distinguishable literal `'permission-denied'` throughout, so a
// real attempt always reads back as `outcome === 'permission-denied'`, never
// confusable with `'skip'`) means it did not.
//
// Scenario 6 ("corruption in old history does not sink genuinely recent
// entries") is a DISTINCT property from the "parse cost does not scale with
// total log size" performance claim — the two were previously conflated in
// this file's own comments, which is misleading: a corrupted-file proxy
// proves resilience to corruption, not that the walk is actually BOUNDED in
// the (overwhelmingly common) case where every line parses fine. That
// bounded-cost claim is now proven directly and quantitatively at the
// `lib/liveness-log.mjs` unit level (see `readLivenessLogTail`'s own test in
// `lib/liveness-log.jest.spec.mjs`, which spies on `JSON.parse` to assert
// the walk inspects a small, fixed number of lines regardless of how many
// old, well-formed entries precede the cooldown window) — that test can
// import the primitive directly and count parses; this black-box,
// spawned-child-process file cannot (there is no seam here, short of
// production-only instrumentation, that would let it observe how many lines
// a spawned `cli.mjs` process actually parsed).
//
// Scenario 6 below therefore stays scoped to what a black-box test CAN
// prove: it corrupts the OLDEST line of a log whose newest lines still
// contain a genuine in-cooldown-window entry, and asserts that entry is
// still found. `readLivenessLog`'s eager `Array.prototype.map(JSON.parse)`
// (chronological, oldest-first) makes a single corrupt line ANYWHERE sink
// the whole read, which `resolveRecentlyEvictedPids`'s own
// `try { ... } catch { return new Set() }` then fails open on, discarding
// EVERY entry, including the genuinely-recent, perfectly-valid one. A real
// bounded/tail-first implementation that walks backward from the newest
// entry never reaches that old corrupted line at all, and therefore still
// finds the recent entry correctly.
//
// Scenario 8 (the archive-fallback design question) is written as a
// documented, ACCEPTED-LIMITATION regression guard, not a RED assertion:
// `resolveRecentlyEvictedPids`'s `cooldownMs` is not configurable from the
// real `--watchdog-beat` CLI surface (no flag/env override exists — grepped
// `cli.mjs` for "cooldownMs"/"COOLDOWN" directly; it is always
// `DEFAULT_WATCHDOG_EVICT_COOLDOWN_MS`, 5 minutes), so this file cannot
// misconfigure the WINDOW itself to force the edge case. It instead
// constructs the other side of the same coin the ticket describes: enough
// OTHER evictions occurring inside that same 5-minute window to rotate the
// genuinely-in-window candidate entry out of the live file and into the
// `<path>.1` archive before this beat ever reads it (Phase 1's rotation is
// purely count-based, not time-based, so this is achievable with
// arbitrary, test-chosen timestamps without needing a real 2000-eviction
// production incident). Today's implementation ALREADY only ever reads the
// live file (never the archive), so this specific test currently PASSES —
// it exists to pin the CURRENT documented behaviour (silently not-found,
// treated as "not on cooldown") so that if Phase 3 changes this
// (e.g. by adding an explicit archive-fallback read), that is a conscious,
// visible diff against this test, not a silent regression either way.
//
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { declareDibs } from './lib/coordination-file.mjs';
import { logWatchdogTrip, readLivenessLog, LIVENESS_LOG_MAX_ENTRIES } from './lib/liveness-log.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const MEMORY_RED = { pressureLevel: 1, swapUsedMb: 12288, compressedMb: 4096 };
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

const ORCHESTRATOR_PID = 501;
const CANDIDATE_PID = 9001;
const BOUND_MS = 10 * 60 * 1000;

// Mirrors `cli.mjs`'s own (unexported) `DEFAULT_WATCHDOG_EVICT_COOLDOWN_MS`,
// itself `DEFAULT_WATCHDOG_CRON_INTERVAL_MS` (5 * 60 * 1000) — grepped both
// constants directly in cli.mjs to confirm this value.
const COOLDOWN_MS = 5 * 60 * 1000;

// Same PID fixture every sibling evict/watchdog spec file uses: pid 9001 is
// a genuine claude-rooted descendant of orchestrator root pid 501.
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
let backoffCounter;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-recently-evicted-'));
  queueFilePath = join(workDir, 'queue.json');
  coordinationFilePath = join(workDir, 'coordination.json');
  fakeKillLogPath = join(workDir, 'fake-kill-log.json');
  liveLogFilePath = join(workDir, 'liveness-log.json');
  backoffCounter = 0;
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

/** Mirrors cli-evict.jest.spec.mjs / watchdog-outer-acceptance.jest.spec.mjs's own `declareDibsFor`. */
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

/** Mirrors cli-evict.jest.spec.mjs / watchdog-outer-acceptance.jest.spec.mjs's own `declareDibsWithPid`. */
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
 * Seeds one queue item and dequeues it under `orchestratorId`, returning its
 * leaseId. `commandRef` is deliberately a `resumable-workflow --resume=...`
 * command — the one documented re-entrancy-policy shape
 * (`isReentrantCommand`, cli.mjs) that makes `buildWatchdogClaimLedger`
 * resolve `reentrant: true`, which `resolveEvictionTargets`
 * (./lib/eviction-targeting.mjs's `buildResolvedTarget`) requires for a
 * non-cooldown-suppressed pid to resolve to `action: 'evict'` at all rather
 * than the more conservative `action: 'alert-only'` default — without this,
 * an out-of-cooldown candidate would never actually reach `handleEvict`,
 * and this file's `'permission-denied'` vs `'skip'` outcome distinction
 * would never appear.
 */
function seedLeasedItem(orchestratorId) {
  declareDibsFor(orchestratorId);

  const enqueueResult = runCli(
    [`--enqueue=typescript-implementer:normal:resumable-workflow --resume=1343-phase-2`, `--orchestrator-id=${orchestratorId}`],
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
  return dequeued.leaseId;
}

/**
 * Runs exactly one real `--watchdog-beat` beat against a single synthetic
 * candidate (pid 9001, comfortably stalled — flat footprint trajectory, a
 * worktree mtime strictly older than `BOUND_MS`), and returns that
 * candidate's own resolved trip object from the beat's JSON body.
 * `ARM_FAKE_KILL_RESULT` is always forced to `'permission-denied'` so that
 * every REAL (non-cooldown-suppressed) attempt reads back as
 * `outcome === 'permission-denied'` — a value that can never be confused
 * with the `'skip'` outcome `resolveEvictionTargets` stamps when
 * `resolveRecentlyEvictedPids` reports the pid on cooldown.
 */
function runStalledBeatAndGetTrip(orchestratorId, nowMs) {
  backoffCounter += 1;
  const beatResult = runCli(
    [
      '--watchdog-beat',
      `--orchestrator-id=${orchestratorId}`,
      `--orchestrator-pid=${ORCHESTRATOR_PID}`,
      `--watchdog-bound-ms=${BOUND_MS}`,
    ],
    baseEnv({
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_AUTHORIZED,
      ARM_FAKE_NOW_MS: String(nowMs),
      ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON: JSON.stringify({ [String(CANDIDATE_PID)]: [100, 100, 100, 100, 100] }),
      ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ [String(CANDIDATE_PID)]: nowMs - BOUND_MS - 60_000 }),
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
      ARM_FAKE_KILL_RESULT: 'permission-denied',
      ARM_WATCHDOG_BACKOFF_FILE: join(workDir, `backoff-${backoffCounter}.json`),
    }),
  );
  expect(beatResult.status).toBe(0);
  const body = JSON.parse(beatResult.stdout);
  const trip = body.trips.find((entry) => entry.pid === CANDIDATE_PID);
  expect(trip).toBeDefined();
  return trip;
}

/**
 * Sequential (not concurrent) appends — mirrors
 * ./lib/liveness-log-rotation.jest.spec.mjs's own `appendManyWatchdogTrips`
 * helper exactly (that file already performs `LIVENESS_LOG_MAX_ENTRIES`
 * (2000) sequential appends without issue; this file reuses the same
 * pattern for the same scale). This suite cares about final on-disk shape
 * and the cooldown-scan's own correctness/behaviour, not the lock's
 * concurrency guarantees (already covered elsewhere).
 */
async function appendManyWatchdogTrips(count, { startPid = 20000, startTimestamp } = {}) {
  for (let index = 0; index < count; index += 1) {
    // eslint-disable-next-line no-await-in-loop -- intentionally sequential; see comment above.
    await logWatchdogTrip(
      liveLogFilePath,
      { pid: startPid + index, orchestratorId: `orc-filler-${startPid + index}`, outcome: 'permission-denied' },
      { now: startTimestamp + index },
    );
  }
}

/**
 * Naive full-parse baseline — mirrors `resolveRecentlyEvictedPids`'s own
 * current body verbatim (as of this branch), used ONLY to compute the
 * expected Set for a given seeded `entries` array so scenarios 1/2 can
 * assert the bounded-read implementation (once it lands) agrees with it
 * exactly, at both small and at-cap scale.
 */
const NON_ATTEMPT_OUTCOMES = new Set(['alert-only', 'skip', 'leak-velocity-throttled']);
function naiveResolveRecentlyEvictedPids(entries, now, cooldownMs) {
  const recentlyEvicted = new Set();
  for (const entry of entries) {
    if (!entry || (entry.detectionType !== 'watchdog-trip' && entry.detectionType !== 'deadlock-recycle')) continue;
    if (NON_ATTEMPT_OUTCOMES.has(entry.outcome)) continue;
    if (typeof entry.timestamp !== 'number' || entry.timestamp > now || now - entry.timestamp > cooldownMs) continue;
    const pid = Number(entry.pid);
    if (Number.isFinite(pid)) recentlyEvicted.add(pid);
  }
  return recentlyEvicted;
}

/** Appends a raw, hand-built line directly (bypassing the lib's locking `appendEntry`) — used only where a scenario needs an entry shape `logWatchdogTrip`/`logAgingEscalation` themselves never produce (see Scenario 5's comment). */
async function appendRawLine(path, entry) {
  const existing = await readFile(path, 'utf8').catch((error) => {
    if (error.code === 'ENOENT') return '';
    throw error;
  });
  await writeFile(path, `${existing}${JSON.stringify(entry)}\n`, 'utf8');
}

// ---------------------------------------------------------------------------
// 1. Small log — bounded read must agree with the naive full-parse baseline.
// ---------------------------------------------------------------------------

it('matches the naive full-parse baseline for a small log well under the cooldown window size', async () => {
  const ORCHESTRATOR_ID = 'orch-small-log';
  seedLeasedItem(ORCHESTRATOR_ID);
  await declareDibsWithPid(ORCHESTRATOR_ID, ORCHESTRATOR_PID);

  const nowMs = Date.now();

  // Appended in non-decreasing timestamp order — exactly how every real
  // production caller writes this log (`logWatchdogTrip`/`logDeadlockRecycle`
  // always resolve `now` from wall-clock `Date.now()`, per `resolveNow` in
  // `lib/liveness-log.mjs`; nothing in production ever backfills an older
  // timestamp after a newer one has already been appended). An earlier
  // version of this fixture appended these out of chronological order
  // (oldest-timestamp entry last), which is not a scenario any real caller
  // can produce and, once `readLivenessLogTail` gained a genuine age-based
  // early stop, would incorrectly walk off the end of the file before ever
  // reaching the in-window candidate entry.
  const seededEntries = [
    { pid: 1003, orchestratorId: 'orc-c', outcome: 'permission-denied', timestamp: nowMs - COOLDOWN_MS - 60_000 },
    { pid: 1001, orchestratorId: 'orc-a', outcome: 'permission-denied', timestamp: nowMs - 4 * 60 * 1000 },
    { pid: CANDIDATE_PID, orchestratorId: ORCHESTRATOR_ID, outcome: 'permission-denied', timestamp: nowMs - 60_000 },
    { pid: 1002, orchestratorId: 'orc-b', outcome: 'alert-only', timestamp: nowMs - 1000 },
  ];
  for (const entry of seededEntries) {
    // eslint-disable-next-line no-await-in-loop -- must fold in chronological order.
    await logWatchdogTrip(liveLogFilePath, { pid: entry.pid, orchestratorId: entry.orchestratorId, outcome: entry.outcome }, { now: entry.timestamp });
  }

  const liveEntries = await readLivenessLog(liveLogFilePath);
  const expected = naiveResolveRecentlyEvictedPids(liveEntries, nowMs, COOLDOWN_MS);
  expect(expected.has(CANDIDATE_PID)).toBe(true);

  const trip = runStalledBeatAndGetTrip(ORCHESTRATOR_ID, nowMs);
  // Regression guard today (this correctness assertion may already pass —
  // the OUTPUT hasn't changed, only the internals must, per Phase 3); the
  // real RED assertions for this file live in scenarios 6/7/8 below.
  expect(trip.outcome).toBe(expected.has(CANDIDATE_PID) ? 'skip' : 'permission-denied');
});

// ---------------------------------------------------------------------------
// 2. Log at the Phase-1 cap (2000 entries) — bounded read must still agree
//    with the naive full-parse baseline computed over the WHOLE live file.
// ---------------------------------------------------------------------------

it(
  'matches the naive full-parse baseline for a log sitting at the Phase-1 cap (2000 entries)',
  async () => {
    const ORCHESTRATOR_ID = 'orch-at-cap';
    seedLeasedItem(ORCHESTRATOR_ID);
    await declareDibsWithPid(ORCHESTRATOR_ID, ORCHESTRATOR_PID);

    const nowMs = Date.now();
    const oldStart = nowMs - 10_000_000;

    // 1999 old, out-of-window fillers, then CANDIDATE_PID's own in-window
    // entry as the very last (newest) append — never rotated out, since the
    // live file sits exactly at (not over) the cap.
    await appendManyWatchdogTrips(LIVENESS_LOG_MAX_ENTRIES - 1, { startTimestamp: oldStart });
    await logWatchdogTrip(
      liveLogFilePath,
      { pid: CANDIDATE_PID, orchestratorId: ORCHESTRATOR_ID, outcome: 'permission-denied' },
      { now: nowMs - 60_000 },
    );

    const liveEntries = await readLivenessLog(liveLogFilePath);
    expect(liveEntries).toHaveLength(LIVENESS_LOG_MAX_ENTRIES);
    const expected = naiveResolveRecentlyEvictedPids(liveEntries, nowMs, COOLDOWN_MS);
    expect(expected.has(CANDIDATE_PID)).toBe(true);

    const trip = runStalledBeatAndGetTrip(ORCHESTRATOR_ID, nowMs);
    expect(trip.outcome).toBe('skip');
  },
  30_000,
);

// ---------------------------------------------------------------------------
// 3. Boundary — exactly `now - cooldownMs` is INCLUDED (current semantics:
//    `now - entry.timestamp > cooldownMs` excludes, so `=== cooldownMs`
//    still passes); `cooldownMs + 1` is EXCLUDED.
// ---------------------------------------------------------------------------

it('includes a pid whose timestamp is exactly now - cooldownMs (boundary, inclusive)', async () => {
  const ORCHESTRATOR_ID = 'orch-boundary-included';
  seedLeasedItem(ORCHESTRATOR_ID);
  await declareDibsWithPid(ORCHESTRATOR_ID, ORCHESTRATOR_PID);

  const nowMs = Date.now();
  await logWatchdogTrip(
    liveLogFilePath,
    { pid: CANDIDATE_PID, orchestratorId: ORCHESTRATOR_ID, outcome: 'permission-denied' },
    { now: nowMs - COOLDOWN_MS },
  );

  const trip = runStalledBeatAndGetTrip(ORCHESTRATOR_ID, nowMs);
  expect(trip.outcome).toBe('skip');
});

it('excludes a pid whose timestamp is cooldownMs + 1 ago (boundary, exclusive)', async () => {
  const ORCHESTRATOR_ID = 'orch-boundary-excluded';
  seedLeasedItem(ORCHESTRATOR_ID);
  await declareDibsWithPid(ORCHESTRATOR_ID, ORCHESTRATOR_PID);

  const nowMs = Date.now();
  await logWatchdogTrip(
    liveLogFilePath,
    { pid: CANDIDATE_PID, orchestratorId: ORCHESTRATOR_ID, outcome: 'permission-denied' },
    { now: nowMs - COOLDOWN_MS - 1 },
  );

  const trip = runStalledBeatAndGetTrip(ORCHESTRATOR_ID, nowMs);
  expect(trip.outcome).toBe('permission-denied');
});

// ---------------------------------------------------------------------------
// 4. NON_ATTEMPT_OUTCOMES filtering — 'alert-only' / 'skip' /
//    'leak-velocity-throttled' entries in-window must never count as a real
//    prior attempt, even though their timestamp is well inside the window.
// ---------------------------------------------------------------------------

describe.each(['alert-only', 'skip', 'leak-velocity-throttled'])('NON_ATTEMPT_OUTCOMES: %s', (outcome) => {
  it(`does not treat an in-window '${outcome}' entry as a real prior eviction attempt`, async () => {
    const ORCHESTRATOR_ID = `orch-non-attempt-${outcome}`;
    seedLeasedItem(ORCHESTRATOR_ID);
    await declareDibsWithPid(ORCHESTRATOR_ID, ORCHESTRATOR_PID);

    const nowMs = Date.now();
    await logWatchdogTrip(
      liveLogFilePath,
      { pid: CANDIDATE_PID, orchestratorId: ORCHESTRATOR_ID, outcome },
      { now: nowMs - 60_000 },
    );

    const trip = runStalledBeatAndGetTrip(ORCHESTRATOR_ID, nowMs);
    expect(trip.outcome).not.toBe('skip');
    expect(trip.outcome).toBe('permission-denied');
  });
});

// ---------------------------------------------------------------------------
// 5. detectionType filtering — only 'watchdog-trip'/'deadlock-recycle' count;
//    an 'aging-escalation' entry in-window must never appear in the result,
//    even if (atypically — real logAgingEscalation entries never carry a
//    `pid` at all) it is hand-constructed to carry the candidate's own pid.
//    Constructed via a raw appended line (not `logAgingEscalation`, whose
//    real shape has no `pid` field to begin with — see `appendRawLine`'s own
//    comment) specifically to stress the detectionType branch in isolation.
// ---------------------------------------------------------------------------

it('never treats an in-window aging-escalation entry (even one carrying the candidate pid) as a recent eviction', async () => {
  const ORCHESTRATOR_ID = 'orch-aging-escalation-filter';
  seedLeasedItem(ORCHESTRATOR_ID);
  await declareDibsWithPid(ORCHESTRATOR_ID, ORCHESTRATOR_PID);

  const nowMs = Date.now();
  await appendRawLine(liveLogFilePath, {
    schemaVersion: 1,
    detectionType: 'aging-escalation',
    timestamp: nowMs - 60_000,
    pid: CANDIDATE_PID,
    orchestratorId: ORCHESTRATOR_ID,
    outcome: 'escalated',
  });

  const trip = runStalledBeatAndGetTrip(ORCHESTRATOR_ID, nowMs);
  expect(trip.outcome).not.toBe('skip');
  expect(trip.outcome).toBe('permission-denied');
});

// ---------------------------------------------------------------------------
// 6. Corruption resilience — a distinct property from parse-cost bounding
//    (see this file's own header comment for that split, and
//    `lib/liveness-log.jest.spec.mjs` for the quantitative bounded-cost
//    proof). This scenario proves only that corruption in OLD, unrelated
//    history never sinks a genuinely recent, well-formed entry. FAILS today
//    (full-parse implementation fails open on the corrupt line, losing the
//    genuinely-recent entry); must PASS once the implementation walks
//    backward from the tail and never reaches the old corrupted line.
// ---------------------------------------------------------------------------

it('still finds a genuinely in-window entry when an OLD (out-of-window) line elsewhere in a small log is corrupt', async () => {
  const ORCHESTRATOR_ID = 'orch-corrupt-old-small';
  seedLeasedItem(ORCHESTRATOR_ID);
  await declareDibsWithPid(ORCHESTRATOR_ID, ORCHESTRATOR_PID);

  const nowMs = Date.now();
  // Oldest line: deliberately invalid JSON.
  await writeFile(liveLogFilePath, 'this is not valid json\n', 'utf8');
  // Newer, genuinely-valid, in-window entry for the candidate pid.
  await logWatchdogTrip(
    liveLogFilePath,
    { pid: CANDIDATE_PID, orchestratorId: ORCHESTRATOR_ID, outcome: 'permission-denied' },
    { now: nowMs - 60_000 },
  );

  const trip = runStalledBeatAndGetTrip(ORCHESTRATOR_ID, nowMs);
  // RED today: `readLivenessLog` throws on the corrupt first line,
  // `resolveRecentlyEvictedPids` fails OPEN (empty Set), and the candidate
  // is wrongly treated as never-recently-evicted.
  expect(trip.outcome).toBe('skip');
});

it(
  'still finds a genuinely in-window entry when the OLDEST line of a log at the Phase-1 cap is corrupt (corruption resilience at scale)',
  async () => {
    const ORCHESTRATOR_ID = 'orch-corrupt-old-at-cap';
    seedLeasedItem(ORCHESTRATOR_ID);
    await declareDibsWithPid(ORCHESTRATOR_ID, ORCHESTRATOR_PID);

    const nowMs = Date.now();
    const oldStart = nowMs - 10_000_000;

    await appendManyWatchdogTrips(LIVENESS_LOG_MAX_ENTRIES - 1, { startTimestamp: oldStart });
    await logWatchdogTrip(
      liveLogFilePath,
      { pid: CANDIDATE_PID, orchestratorId: ORCHESTRATOR_ID, outcome: 'permission-denied' },
      { now: nowMs - 60_000 },
    );

    // Corrupt the OLDEST line in place (still well outside the cooldown
    // window — a bounded backward walk would stop long before reaching it).
    const rawText = await readFile(liveLogFilePath, 'utf8');
    const lines = rawText.split('\n').filter((line) => line.trim().length > 0);
    expect(lines).toHaveLength(LIVENESS_LOG_MAX_ENTRIES);
    lines[0] = 'this is not valid json';
    await writeFile(liveLogFilePath, `${lines.join('\n')}\n`, 'utf8');

    const trip = runStalledBeatAndGetTrip(ORCHESTRATOR_ID, nowMs);
    // RED today, at scale: same fail-open collapse as the small-log case
    // above, now proven across a full 2000-entry file — a genuine bounded
    // implementation's cost is dictated by how far back the cooldown window
    // reaches, not by total file size, so it never touches this corrupt line.
    expect(trip.outcome).toBe('skip');
  },
  30_000,
);

// ---------------------------------------------------------------------------
// 7. Post-rotation regression guard — a pid evicted just before Phase 1's
//    rotation fires (still within its own cooldown window) must still be
//    found correctly, because chronological order is preserved within the
//    (post-rotation) live file.
// ---------------------------------------------------------------------------

it(
  'still finds a pid whose cooldown-window entry survives Phase 1 rotation',
  async () => {
    const ORCHESTRATOR_ID = 'orch-post-rotation';
    seedLeasedItem(ORCHESTRATOR_ID);
    await declareDibsWithPid(ORCHESTRATOR_ID, ORCHESTRATOR_PID);

    const nowMs = Date.now();
    const oldStart = nowMs - 10_000_000;

    // Exactly LIVENESS_LOG_MAX_ENTRIES old fillers first...
    await appendManyWatchdogTrips(LIVENESS_LOG_MAX_ENTRIES, { startTimestamp: oldStart });
    // ...then ONE more append (the candidate's own in-window entry) — this
    // is the (LIVENESS_LOG_MAX_ENTRIES + 1)-th append, which trips Phase 1's
    // rotation and moves the single oldest filler out to the `.1` archive,
    // keeping the live file at exactly the cap with the candidate's entry
    // as its newest line.
    const appendResult = await logWatchdogTrip(
      liveLogFilePath,
      { pid: CANDIDATE_PID, orchestratorId: ORCHESTRATOR_ID, outcome: 'permission-denied' },
      { now: nowMs - 60_000 },
    );
    expect(appendResult.written).toBe(true);

    const liveEntries = await readLivenessLog(liveLogFilePath);
    expect(liveEntries).toHaveLength(LIVENESS_LOG_MAX_ENTRIES);
    expect(liveEntries.some((entry) => entry.pid === CANDIDATE_PID)).toBe(true);

    const trip = runStalledBeatAndGetTrip(ORCHESTRATOR_ID, nowMs);
    expect(trip.outcome).toBe('skip');
  },
  30_000,
);

// ---------------------------------------------------------------------------
// 8. Archive-fallback question — documented ACCEPTED-LIMITATION regression
//    guard (see this file's own header comment for the full rationale).
//    `cooldownMs` cannot be misconfigured from the real CLI surface, so this
//    constructs the equivalent scenario from the other side: enough OTHER
//    evictions inside the SAME cooldown window to rotate the candidate's own
//    genuinely-in-window entry out of the live file before this beat reads
//    it. Passes TODAY (current implementation never consults the archive
//    either) — a regression guard, not a RED assertion.
// ---------------------------------------------------------------------------

it(
  'does not find a genuinely in-window entry that has already been rotated into the archive (accepted limitation — no archive fallback)',
  async () => {
    const ORCHESTRATOR_ID = 'orch-archive-fallback';
    seedLeasedItem(ORCHESTRATOR_ID);
    await declareDibsWithPid(ORCHESTRATOR_ID, ORCHESTRATOR_PID);

    const nowMs = Date.now();

    // The candidate's own genuinely in-window entry, appended FIRST (so it
    // is the OLDEST entry once the fillers below are appended after it).
    await logWatchdogTrip(
      liveLogFilePath,
      { pid: CANDIDATE_PID, orchestratorId: ORCHESTRATOR_ID, outcome: 'permission-denied' },
      { now: nowMs - 60_000 },
    );

    // LIVENESS_LOG_MAX_ENTRIES more evictions, all ALSO inside the same
    // 5-minute cooldown window as the candidate's own entry (timestamps
    // packed into the last 59 seconds before `nowMs`) — enough to rotate
    // the candidate's entry (the oldest of the whole set) out to the `.1`
    // archive, even though it never fell outside the cooldown window itself.
    await appendManyWatchdogTrips(LIVENESS_LOG_MAX_ENTRIES, { startTimestamp: nowMs - 59_000 });

    const liveEntries = await readLivenessLog(liveLogFilePath);
    expect(liveEntries).toHaveLength(LIVENESS_LOG_MAX_ENTRIES);
    expect(liveEntries.some((entry) => entry.pid === CANDIDATE_PID)).toBe(false);

    const archiveEntries = await readLivenessLog(`${liveLogFilePath}.1`);
    expect(archiveEntries.some((entry) => entry.pid === CANDIDATE_PID)).toBe(true);

    const trip = runStalledBeatAndGetTrip(ORCHESTRATOR_ID, nowMs);
    // Accepted limitation, pinned explicitly: not found once rotated to the
    // archive, so this beat proceeds with a REAL attempt rather than
    // skipping — exactly today's behaviour, unchanged by Phase 3 unless a
    // future decision deliberately adds archive consultation.
    expect(trip.outcome).toBe('permission-denied');
  },
  30_000,
);
