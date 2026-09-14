// Phase 3 — wire `reconcileLedgers` (and any cli.mjs call site that
// needs it) to Phase 1's `unparseableRows` signal end-to-end, and prove the 3
// non-reconciliation `listAgentProcesses` callers are unaffected.
//
// HONEST STATUS CHECK BEFORE WRITING THESE (see this file's own assertions
// for the receipts): `observeBeatProcesses` (`cli.mjs`)'s `liveProcesses`
// getter calls `listAgentProcesses(collected.output, snapshotAt)` DIRECTLY —
// the exact function Phase 1 changed — and hands its return value straight
// through to `reconcileLedgers`, which passes it straight through to
// `classifyLedgerEntryLiveness` unmodified
// (`classifyLedgerEntryLiveness(record, observation.liveProcesses,
// observation.snapshotAt)`, cli.mjs's `isConfirmedDead`/`isDead`). Phase 1
// attaches `unparseableRows` as a property ON THE ARRAY `listAgentProcesses`
// already returns (`recon.mjs`, `Object.defineProperty(entries,
// 'unparseableRows', ...)`) rather than wrapping it in a new envelope object
// — so `observation.liveProcesses` already carries the signal Phase 2's rule
// 3.5 reads (`liveProcesses.unparseableRows`), with NO further cli.mjs code
// change required. `reconciliation-minority-garble.jest.spec.mjs`
// (Phase 0's outer acceptance test, composed at the function level) is
// ALREADY GREEN today, confirming this.
//
// So THIS file's first two tests are PINNING/REGRESSION tests through the
// real `cli.mjs` entrypoint (subprocess, never imported directly — matches
// every sibling `cli.*.jest.spec.mjs`/`ledger-reconciliation-*.jest.spec.mjs`
// in this directory), proving the wiring holds at the layer Phase 1+2's own
// tests cannot reach: through `main()`'s real dispatch, real
// `observeBeatProcesses`/`reconcileLedgers` composition, and the real
// on-disk coordination file. They are GREEN, not RED — nothing in cli.mjs
// needed to change for Phase 3's core wiring goal.
//
// The remaining tests cover what Phase 3 actually still had to PROVE rather
// than build: the freshness-guard interaction and the 3 non-reconciliation
// `listAgentProcesses` callers' indifference to the new additive field.

import { spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { LIVENESS_REASON } from './lib/reconciliation.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

/**
 * its identity cross-check guard withholds the whole beat's sweep when
 * `resolveNearestClaudeRootIdentity` finds no claude-rooted ancestor AND
 * `listAgentProcesses` is empty — a real, environment-dependent ambiguity
 * this suite's fixtures must not trip by accident. `resolveNearestClaudeRootIdentity`
 * walks from `process.ppid` (of the spawned `cli.mjs` child — i.e. THIS
 * file's own `process.pid`, since `runCli` spawns `cli.mjs` as this test
 * process's direct child) up through `ps` rows looking for a `/claude`-suffixed
 * `comm`. A row keyed at exactly that pid, with a readable `comm` matching
 * that pattern, resolves identity in one hop regardless of the real OS
 * process tree — so tests that don't care about identity (only about a
 * specific OTHER pid's absence) stay independent of whether this suite
 * happens to be running inside a real claude-rooted session.
 */
const SELF_IDENTITY_PID = process.pid;
const selfIdentityRow = () =>
  `${SELF_IDENTITY_PID}     1 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude`;

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };
const MEMORY_RED = { pressureLevel: 1, swapUsedMb: 12288, compressedMb: 4096 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

/** Fixed fake "now" (via `ARM_FAKE_NOW_MS`), so every timestamp below is exact. */
const T0 = 1_700_000_000_000;

/** Runs the real cli.mjs as a child process — never imports it directly. */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

function baseEnv(extra = {}) {
  return {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_NOW_MS: String(T0),
    ARM_BEAT_NOW_TEST_MODE: '1',
    ARM_BEAT_SNAPSHOT_TEST_MODE: '1',
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    ...extra,
  };
}

/** A dibs entry carrying an already-resolved identity pair. */
function dibsEntry({ orchestratorId, pid, pidStartedAt, declaredAt }) {
  return {
    orchestratorId,
    declaredAt,
    firstDeclaredAt: pidStartedAt + 5_000,
    desiredAgents: 1,
    pid,
    pidStartedAt,
  };
}

let workDir;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-phase3-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Test 1 — reconcileLedgers end-to-end: minority-garbled snapshot, one live
// agent's row unparseable (recoverable pid) → NOT reaped.
//
// ALREADY GREEN (pinning, not RED) — see this file's header. The garbled
// row's pid (LIVE_PID) is recoverable (only its `rss` column is corrupted;
// pid/ppid/comm all parse), so `listAgentProcesses`'s Phase-1 signal
// surfaces it via `unparseableRows`, and Phase 2's rule 3.5 — reached
// through cli.mjs's OWN `observation.liveProcesses` value, unmodified —
// classifies the entry UNKNOWN rather than CONFIRMED_DEAD/PID_ABSENT.
// ---------------------------------------------------------------------------
it('reconcileLedgers does not reap a dibs entry whose ps row failed to parse, when the surrounding table is majority-readable (end-to-end via cli.mjs)', async () => {
  const LIVE_PID = 9001;
  const LIVE_STARTED_AT = T0 - 3 * 60 * 60 * 1000; // 3h before snapshot

  const psText = [
    '  PID  PPID    RSS     ELAPSED COMM',
    '    1     0    512 1-03:46:39 /sbin/launchd',
    ' 8000     1  10240    00:05:00 /usr/sbin/some-daemon',
    // The live agent's own root row — rss column garbled, pid/ppid/comm intact.
    ` ${LIVE_PID}     1 XXXXXX    03:00:00 /Applications/Claude.app/Contents/MacOS/claude`,
    ' 8500     1   2048    00:10:00 /usr/bin/some-other-proc',
    // The LIVE_PID row above fails PS_LINE_WITH_ETIME_PATTERN (garbled rss),
    // so it is invisible to resolveNearestClaudeRootIdentity too — this row
    // resolves the beat's own identity independently (see selfIdentityRow's
    // doc comment), so the drift guard doesn't withhold the sweep and
    // this test actually pins Rule 3.5's path rather than the guard's.
    selfIdentityRow(),
    '',
  ].join('\n');

  await writeFile(
    coordinationFilePath,
    JSON.stringify(
      [dibsEntry({ orchestratorId: 'orch-live', pid: LIVE_PID, pidStartedAt: LIVE_STARTED_AT, declaredAt: T0 - 60_000 })],
      null,
      2,
    ),
    'utf8',
  );

  // A DIFFERENT orchestrator-id than the seeded entry's own: this beat's own
  // declareDibs upsert must never overwrite the fixture it is inspecting —
  // matches test 2's `orch-someone-else` observer pattern.
  const beat = runCli(
    ['--orchestrator-id=orch-observer', '--desired-agents=1'],
    baseEnv({ ARM_FAKE_PS_OUTPUT: psText }),
  );

  expect(beat.status).toBe(0);
  // Never reported as reaped.
  expect(beat.stderr).not.toMatch(/reaped dibs record for orch-live/);

  const afterBeat = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
  const survivor = afterBeat.find((entry) => entry.orchestratorId === 'orch-live');
  expect(survivor).toBeDefined();
  expect(survivor.pid).toBe(LIVE_PID);
});

// ---------------------------------------------------------------------------
// Test 2 — reconcileLedgers end-to-end: a genuinely-dead agent (pid absent,
// no garbled row involved) is still reaped as before (regression).
// ---------------------------------------------------------------------------
it('reconcileLedgers still reaps a genuinely-dead dibs entry whose pid is simply absent from the snapshot, no garbled row involved (regression, end-to-end via cli.mjs)', async () => {
  const DEAD_PID = 9501; // Appears nowhere in psText below — not garbled, just gone.
  const DEAD_STARTED_AT = T0 - 3 * 60 * 60 * 1000;

  const psText = [
    '  PID  PPID    RSS     ELAPSED COMM',
    '    1     0    512 1-03:46:39 /sbin/launchd',
    ' 8000     1  10240    00:05:00 /usr/sbin/some-daemon',
    // Resolves this beat's own identity (see selfIdentityRow's doc comment) so
    // the drift guard doesn't withhold the sweep — this fixture is
    // deliberately near-agentless otherwise: DEAD_PID appears nowhere in it.
    selfIdentityRow(),
    '',
  ].join('\n');

  await writeFile(
    coordinationFilePath,
    JSON.stringify(
      [dibsEntry({ orchestratorId: 'orch-dead', pid: DEAD_PID, pidStartedAt: DEAD_STARTED_AT, declaredAt: T0 - 60_000 })],
      null,
      2,
    ),
    'utf8',
  );

  const beat = runCli(
    ['--orchestrator-id=orch-someone-else', '--desired-agents=1'],
    baseEnv({ ARM_FAKE_PS_OUTPUT: psText }),
  );

  expect(beat.status).toBe(0);
  expect(beat.stderr).toMatch(
    new RegExp(`reaped dibs record for orch-dead \\(pid ${DEAD_PID}\\): ${LIVENESS_REASON.PID_ABSENT}`),
  );

  const afterBeat = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
  expect(afterBeat.find((entry) => entry.orchestratorId === 'orch-dead')).toBeUndefined();
});

// ---------------------------------------------------------------------------
// Test 3 — the written-after-snapshot freshness guard still short-circuits
// correctly, and is never overridden by (or double-classified alongside) the
// new unparseable-row signal. Two entries in the SAME beat: one is dead-pid
// (no garble) but was declared AFTER the snapshot was issued — must be
// DEFERRED by the freshness guard, never reaped, and must never be reported
// under the new PID_ROW_UNPARSEABLE reason. The other is the minority-garble
// live entry from test 1, present in the same snapshot, to prove the guard's
// verdict for one entry is not disturbed by the new rule firing for another.
// ---------------------------------------------------------------------------
it('the written-after-snapshot freshness guard defers a record independently of the new unparseable-row rule, and is never overridden by it', async () => {
  const LIVE_PID = 9001;
  const LIVE_STARTED_AT = T0 - 3 * 60 * 60 * 1000;
  const LATE_PID = 9601; // absent from the snapshot — would read CONFIRMED_DEAD/PID_ABSENT on its own.
  const LATE_STARTED_AT = T0 - 60 * 60 * 1000;

  const psText = [
    '  PID  PPID    RSS     ELAPSED COMM',
    '    1     0    512 1-03:46:39 /sbin/launchd',
    ' 8000     1  10240    00:05:00 /usr/sbin/some-daemon',
    ` ${LIVE_PID}     1 XXXXXX    03:00:00 /Applications/Claude.app/Contents/MacOS/claude`,
    ' 8500     1   2048    00:10:00 /usr/bin/some-other-proc',
    // The LIVE_PID row above fails PS_LINE_WITH_ETIME_PATTERN (garbled rss),
    // so it is invisible to resolveNearestClaudeRootIdentity too — this row
    // resolves the beat's own identity independently (see selfIdentityRow's
    // doc comment), so the drift guard doesn't withhold the sweep.
    selfIdentityRow(),
    '',
  ].join('\n');

  // `declaredAt` strictly AFTER this beat's snapshotIssuedAt (~T0, since
  // ARM_FAKE_NOW_MS drives both) — simulates a record written by a beat
  // whose write raced ahead of this beat's own (stale) snapshot.
  const lateDeclaredAt = T0 + 10 * 60 * 1000;

  await writeFile(
    coordinationFilePath,
    JSON.stringify(
      [
        dibsEntry({ orchestratorId: 'orch-live', pid: LIVE_PID, pidStartedAt: LIVE_STARTED_AT, declaredAt: T0 - 60_000 }),
        dibsEntry({ orchestratorId: 'orch-late', pid: LATE_PID, pidStartedAt: LATE_STARTED_AT, declaredAt: lateDeclaredAt }),
      ],
      null,
      2,
    ),
    'utf8',
  );

  const beat = runCli(
    ['--orchestrator-id=orch-observer', '--desired-agents=1'],
    baseEnv({ ARM_FAKE_PS_OUTPUT: psText }),
  );

  expect(beat.status).toBe(0);

  // The late-declared, genuinely-pid-absent entry is DEFERRED by the
  // freshness guard — reported under its own WRITTEN_AFTER_SNAPSHOT reason,
  // never reaped, and never misreported under the new rule's reason.
  expect(beat.stderr).toMatch(
    new RegExp(`deferred dibs record for orch-late \\(pid ${LATE_PID}\\): ${LIVENESS_REASON.WRITTEN_AFTER_SNAPSHOT}`),
  );
  expect(beat.stderr).not.toMatch(/reaped dibs record for orch-late/);
  expect(beat.stderr).not.toMatch(new RegExp(`orch-late.*${LIVENESS_REASON.PID_ROW_UNPARSEABLE}`));

  // The minority-garbled live entry is untouched by the guard firing for its
  // sibling — still spared, still present, exactly as test 1 proved alone.
  expect(beat.stderr).not.toMatch(/reaped dibs record for orch-live/);
  expect(beat.stderr).not.toMatch(/deferred dibs record for orch-live/);

  const afterBeat = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
  expect(afterBeat.find((entry) => entry.orchestratorId === 'orch-live')).toBeDefined();
  expect(afterBeat.find((entry) => entry.orchestratorId === 'orch-late')).toBeDefined();
});

// ---------------------------------------------------------------------------
// Cross-caller regression sweep — the 3 non-reconciliation callers of
// `listAgentProcesses` must be unaffected by the now-additive
// (non-enumerable) `unparseableRows` property on its return array. Each test
// injects an UNRELATED garbled row alongside a clean, well-understood
// fixture and asserts the caller's observable output is identical to what it
// would be without that garbled row.
// ---------------------------------------------------------------------------

const CLAUDE_ROOT_PID = 9001;

/** A clean single-agent snapshot, plus one unrelated garbled row (rss corrupted, unattributable to CLAUDE_ROOT_PID). */
const PS_ONE_AGENT_PLUS_GARBLE = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 ${CLAUDE_ROOT_PID}   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001  51200       58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
 7000     1 XXXXXX    00:02:00 /usr/sbin/unrelated-garbled-daemon
`;

// Test 4 — pause-candidate selection under memory pressure (cli.mjs ~line
// 7115, the main() RED-beat path) is unaffected by the additive field.
it('pause-candidate selection under memory pressure is unaffected by the new unparseableRows field (regression)', () => {
  const beat = runCli(
    ['--orchestrator-id=orch-arm-pause'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: PS_ONE_AGENT_PLUS_GARBLE,
    },
  );

  expect(beat.status).toBe(0);
  expect(beat.stderr).toBe('');
  const parsed = JSON.parse(beat.stdout);

  // Same result as ./cli.jest.spec.mjs's "names a real pauseCandidate" test
  // with its clean PS_ONE_AGENT fixture — the unrelated garbled row changes
  // nothing about which candidate is selected or its computed RSS.
  expect(parsed.type).toBe('pause');
  expect(parsed.pauseCandidate).not.toBeNull();
  expect(parsed.pauseCandidate.agentId).toBe(String(CLAUDE_ROOT_PID));
  expect(parsed.pauseCandidate.rssMb).toBeCloseTo(150, 5);
});

// Test 5 — eviction-fallback pause-candidate selection (cli.mjs ~line 4552,
// inside handleEvict's buildFallback) is unaffected by the additive field.
it('eviction-fallback candidate selection (--evict-pid path) is unaffected by the new unparseableRows field (regression)', () => {
  // No matching --evict-pid target and no --lease-id: falls straight to
  // buildFallback('lease-not-found'), which is a RED-memory fallback that
  // itself calls listAgentProcesses(...) to build a pauseCandidate — see
  // cli.mjs's buildFallback, ~line 4551.
  const beat = runCli(
    ['--evict-pid=99999', '--orchestrator-id=orch-arm-evict', '--orchestrator-pid=501'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_EVICT_TEST_MODE: '1',
      ARM_FAKE_PS_OUTPUT: PS_ONE_AGENT_PLUS_GARBLE,
      // Every ARM_FAKE_* seam --evict-pid reads must be faked together, or
      // it fails closed with a "partially-faked eviction seam" refusal.
      ARM_FAKE_KILL_LOG: join(workDir, 'unused-fake-kill-log.json'),
      ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
      ARM_FAKE_KILL_RESULT: '',
      ARM_FAKE_NOW_MS: String(T0),
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 100 MB',
    },
  );

  const body = JSON.parse(beat.stdout);
  expect(body.evictionAttempted).toBe(true);
  expect(body.reason).toBe('lease-not-found');
  expect(body.type).toBe('pause');
  expect(body.pauseCandidate).not.toBeNull();
  expect(body.pauseCandidate.agentId).toBe(String(CLAUDE_ROOT_PID));
  expect(body.pauseCandidate.rssMb).toBeCloseTo(150, 5);
});

// Test 6 — owned-entries filtering (cli.mjs ~line 5092, buildWatchdogCandidates,
// the --watchdog-beat path) is unaffected by the additive field.
it('owned-entries filtering (--watchdog-beat path) is unaffected by the new unparseableRows field (regression)', () => {
  const beat = runCli(
    [
      '--watchdog-beat',
      '--orchestrator-id=orch-arm-watchdog',
      '--orchestrator-pid=501',
      '--watchdog-bound-ms=600000',
    ],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      ARM_EVICT_TEST_MODE: '1',
      ARM_FAKE_PS_OUTPUT: PS_ONE_AGENT_PLUS_GARBLE,
      ARM_FAKE_NOW_MS: String(T0),
      ARM_BEAT_NOW_TEST_MODE: '1',
      // No stall signals fed in — nothing should trip, garbled row included.
      ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON: JSON.stringify({ [String(CLAUDE_ROOT_PID)]: [100, 90, 80, 70, 60] }),
      ARM_FAKE_WORKTREE_MTIME_JSON: JSON.stringify({ [String(CLAUDE_ROOT_PID)]: T0 }),
    },
  );

  expect(beat.status).toBe(0);
  const body = JSON.parse(beat.stdout);
  expect(body.type).toBe('watchdog');
  expect(Array.isArray(body.trips)).toBe(true);
  // No entry — owned, garble-adjacent, or otherwise — was flagged stalled:
  // the unrelated garbled row surfaced neither a phantom candidate nor a
  // phantom trip.
  expect(body.trips).toHaveLength(0);
});
