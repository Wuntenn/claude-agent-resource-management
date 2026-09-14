// Phase 0 — OUTER ACCEPTANCE TEST (RED): bound the
// agent-recognition-drift total reap with the beat's own identity
// cross-check.
//
// THE BUG CLASS THIS TICKET CLOSES. `reconcileLedgers` (cli.mjs) currently
// reaps a pid-bearing ledger record whenever that pid is absent from
// `observation.liveProcesses` — with NO regard for whether `liveProcesses`
// is itself trustworthy evidence. `observation.liveProcesses: []` is
// ambiguous between two very different worlds:
//
//   (a) a host that is GENUINELY agentless right now — a readable `ps`
//       snapshot that simply contains no claude-rooted process at all,
//       INCLUDING none for the very orchestrator running this beat. There is
//       no live orchestrator to correlate anything against, so `[]` here
//       is not evidence of anything specific dying — it is the absence of
//       any positive identity signal whatsoever.
//   (b) a host that IS running agents, one of which is this beat's own
//       orchestrator (so `observation.identity` resolves to a real,
//       ancestry-derived pid) — where a `[]` reading of "other" agents is a
//       narrower, legitimate claim: nothing else is running, but the sweep
//       still has a live identity to reason from.
//
// The fix (NOT implemented here — later phases do this) adds a guard: when
// `observation.identity` is `null` AND `observation.liveProcesses` is a
// valid empty array on a READABLE snapshot (case (a) above), `reconcileLedgers`
// must refuse to sweep any of the three `RECONCILED_LEDGERS` and warn on
// stderr via the existing `warnCoordinationDegraded` helper — in the same
// vocabulary as the existing `PROCESS_SNAPSHOT_UNREADABLE` path. It must NOT
// become a blanket disarm: when `identity` resolves to a real ancestor pid
// (case (b)), the sweep must keep reaping confirmed-dead records normally,
// including when no OTHER agent happens to be running beside it.
//
// -----------------------------------------------------------------------
// WHY THIS IS A NEW, DEDICATED FILE
// -----------------------------------------------------------------------
// Per the convergence-analysis recommendation already logged on : this
// guard is a narrow, single-purpose behaviour distinct from both
// ./ledger-reconciliation-outer-acceptance.jest.spec.mjs (the whole-
// ticket composition test) and ./ledger-reconciliation-beat-wiring.jest.spec.mjs
// (the beat-wiring unit-of-behaviour suite). It is additive and does
// not modify either file.
//
// NOTABLY: ./ledger-reconciliation-beat-wiring.jest.spec.mjs already contains
// a test named "still reaps against a readable snapshot that simply carries
// no agents" (`buildAgentlessPsOutput`), whose own fixture — a `ps` table
// with NO claude-rooted row anywhere — resolves `identity: null` too (its own
// walk starts at `process.ppid`, finds no claude-rooted ancestor, and returns
// `null`). That existing test currently asserts the ghost record IS reaped.
// Once this ticket's guard lands, that fixture's shape (case (a) above) will
// need `identity` to be resolvable for the "still reaps" claim to remain
// true — that is a fix-phase concern for that file, not this one; it is
// flagged here for whichever later phase touches it, per this skill's
// "surface the bug, don't fix it" stance.
//
// -----------------------------------------------------------------------
// WHY "IDENTITY RESOLVES BUT `liveProcesses` IS EMPTY" IS APPROXIMATED, NOT
// LITERAL, IN THE SECOND HALF OF THIS TEST
// -----------------------------------------------------------------------
// `observation.identity` (`resolveNearestClaudeRootIdentity`) and
// `observation.liveProcesses` (`listAgentProcesses`) are BOTH derived from
// the exact same `ps` text, by the exact same root-detection rule
// (`AGENT_ROOT_COMM_PATTERN` + a parseable `etime`) — see ./lib/recon.mjs.
// Any claude-rooted ancestor that passes `resolveNearestClaudeRootIdentity`'s
// check therefore ALSO produces its own entry in `listAgentProcesses`'
// output. There is no `ps`-text fixture reachable through this CLI's real
// black-box entry point (`reconcileLedgers` is not exported — `cli.mjs` runs
// `main()` at module scope, so it cannot be imported from a spec; this file
// follows the same black-box-child-process convention as every other spec in
// this directory) that produces `identity !== null` together with a
// LITERALLY empty `liveProcesses` array.
//
// What IS reachable, and is the substantively equivalent case the guard's
// "not a blanket disarm" half needs to prove: a snapshot carrying exactly
// ONE claude-rooted process — this beat's own orchestrator, which is what
// `identity` resolves to — and NO other agent-rooted process anywhere. That
// makes `liveProcesses` a single-entry array containing only the identity's
// own root, and — the property that actually matters for the ghost record
// under test — the SEEDED GHOST'S pid is absent from it, exactly as it would
// be from a literal `[]`. The classifier's per-record judgement
// (`classifyLedgerEntryLiveness`) only ever asks "is THIS record's pid found
// in `liveProcesses`?", so a `liveProcesses` array that omits the ghost's pid
// behaves identically, for this ticket's purposes, to an empty one — while
// `identity` is genuinely non-null, proving the guard does not fire.
//
// See ./ledger-reconciliation-outer-acceptance.jest.spec.mjs's own header
// ("HOW THE ANCESTRY FIXTURE IS CONSTRUCTED") for why the ancestry fixture is
// generated around the real `process.pid`/`process.ppid` anchors rather than
// hardcoded.

import { spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { AIMD_CEILING_RESERVED_ID } from './lib/coordination-file.mjs';
import { resolveNearestClaudeRootIdentity } from './lib/recon.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

// Mirrors of cli.mjs's own constants, restated as literals — cli.mjs cannot
// be imported from a spec (it runs `main()` at module scope). Same
// convention as every other black-box spec in this directory.
const LIVE_AGENT_CEILING = 4; // cli.mjs's LIVE_AGENT_CEILING
const LIVE_AGENT_CLAIM_TYPE = 'live-agent';
const LIVE_AGENT_ADMISSION_CLAIM_TYPE = 'live-agent-admission'; // cli.mjs's LIVE_AGENT_ADMISSION_CLAIM_TYPE

const claimLedgerId = (type) => `__claim:${type}__`;

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const FAKE_COLLECT_JSON = JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN });

/** Fixed fake "now", so `pidStartedAt`/`startedAt` arithmetic is exact. */
const T0 = 1_700_000_000_000;
/** Every hand-seeded ghost record is 60s old — far inside every ledger's own TTL. */
const SEEDED_AT = T0 - 60_000;

/** A pid carried by NO row in any fixture below — the sweep's only oracle is the fixture. */
const GHOST_PID = 930_500;
const GHOST_PID_STARTED_AT = T0 - 7_200_000;

/**
 * The real anchors of every fixture tree below. `TEST_PID` is, by
 * construction, the `process.ppid` every `cli.mjs` child spawned by `runCli`
 * sees — exactly where the ancestry walk starts. Following the standing rule
 * from ./ledger-reconciliation-outer-acceptance.jest.spec.mjs: seed a process
 * tree and assert on the identity the code derives from it, never hand a
 * "correct" pid to the fixture directly.
 */
const TEST_PID = process.pid;
const TEST_PPID = process.ppid;

/** A pid for the single claude-rooted ancestor in the "identity resolves" fixture. */
const FAKE_CLAUDE_ROOT_PID = 930_001;
const ANCESTOR_ETIME = '01:00:00';
const EXPECTED_PID_STARTED_AT = T0 - 60 * 60 * 1000;

/**
 * A readable `ps` table with NO claude-rooted process anywhere — not this
 * beat's own orchestrator, not anything else. `resolveNearestClaudeRootIdentity`
 * walks up from `process.ppid` and finds nothing matching
 * `AGENT_ROOT_COMM_PATTERN`, so `identity` is `null`; `listAgentProcesses`
 * likewise finds no root, so `liveProcesses` is `[]`. The table is otherwise
 * entirely parseable (`classifyProcessSnapshotShape` reads it as `'readable'`),
 * so this is case (a) from this file's header: a genuine, fully-legible
 * observation carrying no positive identity signal at all.
 */
function buildNoAgentRootsPsOutput() {
  return [
    '  PID  PPID    RSS     ELAPSED COMM',
    '    1     0    512 1-03:46:39 /sbin/launchd',
    `${TEST_PPID}     1  81920    00:30:00 /usr/local/bin/node`,
    `${TEST_PID} ${TEST_PPID}  65536    00:10:00 /usr/local/bin/node`,
    '',
  ].join('\n');
}

/**
 * A readable `ps` table carrying EXACTLY ONE claude-rooted process: this
 * beat's own orchestrator, ancestor of the real spawned `cli.mjs` child. This
 * is case (b) from this file's header — a legitimately near-agentless host,
 * except that the beat's own identity is resolvable. `listAgentProcesses`
 * returns exactly one entry (`FAKE_CLAUDE_ROOT_PID`'s own), which does NOT
 * carry `GHOST_PID` — see this file's header for why that stands in for a
 * literal `[]` for this ticket's purposes.
 */
function buildResolvableIdentityNoOtherAgentsPsOutput() {
  return [
    '  PID  PPID    RSS     ELAPSED COMM',
    '    1     0    512 1-03:46:39 /sbin/launchd',
    `${FAKE_CLAUDE_ROOT_PID}     1 102400    ${ANCESTOR_ETIME} /Applications/Claude.app/Contents/MacOS/claude`,
    `${TEST_PPID} ${FAKE_CLAUDE_ROOT_PID}  81920    00:30:00 /usr/local/bin/node`,
    `${TEST_PID} ${TEST_PPID}  65536    00:10:00 /usr/local/bin/node`,
    '',
  ].join('\n');
}

/**
 * A snapshot that is genuinely UNREADABLE — CSV-shaped rows that this
 * project's whitespace-column grammar (`PS_LINE_WITH_ETIME_PATTERN`) cannot
 * parse. `classifyProcessSnapshotShape` calls this `'unreadable'`, which
 * `observeBeatProcesses` already treats as no observation at all: it warns
 * `PROCESS_SNAPSHOT_UNREADABLE` and returns `{ liveProcesses: null, identity:
 * null, ... }` BEFORE this ticket's guard is ever reached. Used to prove the
 * new guard does not ALSO fire and double-warn on the same beat.
 */
function buildUnreadablePsOutput() {
  return [
    'PID,PPID,RSS,ELAPSED,COMM',
    '1,0,512,1-03:46:39,/sbin/launchd',
    `${FAKE_CLAUDE_ROOT_PID},1,102400,${ANCESTOR_ETIME},/Users/someone/.local/bin/claude`,
    `${TEST_PPID},${FAKE_CLAUDE_ROOT_PID},81920,00:30:00,/usr/local/bin/node`,
    '',
  ].join('\n');
}

/** Runs the real cli.mjs as a child process — never imports it. See every other spec in this directory. */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/** The AIMD ceiling row, so `LIVE_AGENT_CEILING` is deterministic per fixture. */
function ceilingEntry() {
  return {
    orchestratorId: AIMD_CEILING_RESERVED_ID,
    ceiling: LIVE_AGENT_CEILING,
    sustainedNormalCount: 0,
    declaredAt: SEEDED_AT,
  };
}

/** A dibs entry carrying a pid this fixture's snapshot never mentions. */
function ghostDibsEntry(orchestratorId, desiredAgents) {
  return {
    orchestratorId,
    desiredAgents,
    declaredAt: SEEDED_AT,
    firstDeclaredAt: SEEDED_AT,
    pid: GHOST_PID,
    pidStartedAt: GHOST_PID_STARTED_AT,
  };
}

function ghostClaimLedger(orchestratorId, count) {
  return {
    orchestratorId: claimLedgerId(LIVE_AGENT_CLAIM_TYPE),
    claims: [{ orchestratorId, count, claimedAt: SEEDED_AT, pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }],
    grantedTotal: count,
    declaredAt: SEEDED_AT,
  };
}

async function seedLedger(coordinationFilePath, entries) {
  await writeFile(coordinationFilePath, JSON.stringify(entries, null, 2), 'utf8');
}

async function readLedger(coordinationFilePath) {
  const persisted = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
  return {
    byId: (id) => persisted.find((entry) => entry.orchestratorId === id),
    claimsOf: (type) => persisted.find((entry) => entry.orchestratorId === claimLedgerId(type))?.claims ?? [],
  };
}

let workDir;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-identity-guard-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

describe("refuses to sweep any ledger when the beat's own identity is null and the snapshot yielded zero agent roots, but still reaps when identity resolves", () => {
  it(
    'refuses the sweep and warns when identity is null and liveProcesses is a valid, readable, empty observation',
    async () => {
      // Case (a). The snapshot is fully readable (`classifyProcessSnapshotShape`
      // would call it `'readable'`) — this is NOT the pre-existing
      // `PROCESS_SNAPSHOT_UNREADABLE`/`PROCESS_SNAPSHOT_UNAVAILABLE` path, which
      // already defers correctly. This is the narrower gap: a snapshot
      // that IS trustworthy text, yet carries no positive identity signal
      // whatsoever, because no claude-rooted process — not even this beat's
      // own orchestrator — appears in it anywhere.
      await seedLedger(coordinationFilePath, [
        ceilingEntry(),
        ghostDibsEntry('ghost-orch', 1),
        ghostClaimLedger('ghost-orch', 1),
      ]);

      const beat = runCli(['--orchestrator-id=sweeper', '--desired-agents=1'], {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_NOW_MS: String(T0),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_BEAT_SNAPSHOT_TEST_MODE: '1',
        ARM_FAKE_PS_OUTPUT: buildNoAgentRootsPsOutput(),
        ARM_FAKE_COLLECT_JSON: FAKE_COLLECT_JSON,
      });

      expect(beat.status).toBe(0);

      const { byId, claimsOf } = await readLedger(coordinationFilePath);

      // THE RED ASSERTION. Today `reconcileLedgers` has no identity guard at
      // all: a readable, empty `liveProcesses` is treated as a legitimate
      // "zero agents" observation and `ghost-orch`'s pid-bearing records are
      // reaped `pid-absent`, exactly as
      // ./ledger-reconciliation-beat-wiring.jest.spec.mjs's own "still reaps
      // against a readable snapshot that simply carries no agents" test
      // already pins for a fixture of this exact shape. This ticket's guard
      // must instead defer both records to their own TTL — 60s old, far
      // inside every ledger's window, so nothing here can have aged out
      // independently.
      expect(byId('ghost-orch')).toBeDefined();
      expect(claimsOf(LIVE_AGENT_CLAIM_TYPE).map((claim) => claim.orchestratorId)).toContain('ghost-orch');

      // And the refusal must not be silent — warned via the existing
      // `warnCoordinationDegraded` voice, in the same vocabulary as the
      // `PROCESS_SNAPSHOT_UNREADABLE` path: `agent-resource-management:
      // warning: <what> unavailable (<code>); <consequence>`.
      expect(beat.stderr).toMatch(/warning/i);
      // And, symmetrically with the unreadable-snapshot path, nothing may
      // claim a reap happened on the strength of an observation this guard
      // refused to act on.
      expect(beat.stderr).not.toMatch(/reaped/);
    },
    30_000,
  );

  it(
    'still reaps a confirmed-dead record when identity resolves to a real ancestor, even with no other agents around',
    async () => {
      // Case (b) — the "not a blanket disarm" half. `identity` resolves to
      // `FAKE_CLAUDE_ROOT_PID` (this beat's own orchestrator), so the guard
      // above must not fire, and `ghost-orch`'s pid-bearing records — whose
      // pid (`GHOST_PID`) appears nowhere in this fixture, see this file's
      // header for why that is the reachable equivalent of a literal `[]`
      // here — must be reaped exactly as before this ticket.
      await seedLedger(coordinationFilePath, [
        ceilingEntry(),
        ghostDibsEntry('ghost-orch', 1),
        ghostClaimLedger('ghost-orch', 1),
      ]);

      const beat = runCli(['--orchestrator-id=sweeper', '--desired-agents=1'], {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_NOW_MS: String(T0),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_BEAT_SNAPSHOT_TEST_MODE: '1',
        ARM_FAKE_PS_OUTPUT: buildResolvableIdentityNoOtherAgentsPsOutput(),
        ARM_FAKE_COLLECT_JSON: FAKE_COLLECT_JSON,
      });

      expect(beat.status).toBe(0);

      const { byId, claimsOf } = await readLedger(coordinationFilePath);

      expect(byId('ghost-orch')).toBeUndefined();
      expect(claimsOf(LIVE_AGENT_CLAIM_TYPE).map((claim) => claim.orchestratorId)).not.toContain('ghost-orch');

      // Named on stderr, the same as any other reap.
      expect(beat.stderr).toMatch(/ghost-orch/);

      // Confirms the premise this half of the test rests on: this beat's own
      // ancestry-derived identity really is resolvable under this fixture —
      // otherwise this test would be indistinguishable from the first one.
      const sweeperIdentity = byId('sweeper');
      expect(sweeperIdentity?.pid).toBe(FAKE_CLAUDE_ROOT_PID);
      expect(sweeperIdentity?.pidStartedAt).toBe(EXPECTED_PID_STARTED_AT);
    },
    30_000,
  );
});

// -----------------------------------------------------------------------
// Phase 1 — complementary unit/integration coverage per the Build Plan's
// Test Strategy. These are ADDITIVE to the Phase 0 describe block above and
// deliberately do not repeat its two scenarios; see each `it` for the gap it
// closes.
// -----------------------------------------------------------------------

describe('identity cross-check guard — Phase 1 complementary coverage', () => {
  it(
    'does not reap any ledger when identity is null and the snapshot shows zero agent roots',
    async () => {
      // Restates case (a) at the granularity the Build Plan names explicitly
      // ("does not reap ANY ledger"), independent of the Phase 0 test's own
      // stderr-vocabulary assertions. All three RECONCILED_LEDGERS entries
      // (dibs, live-agent claim, live-agent-admission claim) are seeded with a
      // ghost record each, and every one of them must survive the sweep.
      await seedLedger(coordinationFilePath, [
        ceilingEntry(),
        ghostDibsEntry('ghost-orch', 1),
        ghostClaimLedger('ghost-orch', 1),
        {
          orchestratorId: claimLedgerId(LIVE_AGENT_ADMISSION_CLAIM_TYPE),
          claims: [
            {
              orchestratorId: 'ghost-orch',
              count: 1,
              claimedAt: SEEDED_AT,
              pid: GHOST_PID,
              pidStartedAt: GHOST_PID_STARTED_AT,
            },
          ],
          grantedTotal: 1,
          declaredAt: SEEDED_AT,
        },
      ]);

      const beat = runCli(['--orchestrator-id=sweeper', '--desired-agents=1'], {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_NOW_MS: String(T0),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_BEAT_SNAPSHOT_TEST_MODE: '1',
        ARM_FAKE_PS_OUTPUT: buildNoAgentRootsPsOutput(),
        ARM_FAKE_COLLECT_JSON: FAKE_COLLECT_JSON,
      });

      expect(beat.status).toBe(0);

      const { byId, claimsOf } = await readLedger(coordinationFilePath);
      expect(byId('ghost-orch')).toBeDefined();
      expect(claimsOf(LIVE_AGENT_CLAIM_TYPE).map((claim) => claim.orchestratorId)).toContain('ghost-orch');
      expect(claimsOf(LIVE_AGENT_ADMISSION_CLAIM_TYPE).map((claim) => claim.orchestratorId)).toContain('ghost-orch');
      expect(beat.stderr).not.toMatch(/reaped/);
    },
    30_000,
  );

  it(
    'still reaps a confirmed-dead record when identity resolves and the snapshot legitimately shows zero agents (agentless host, not a blanket disarm)',
    async () => {
      // Restates case (b) at the Build Plan's own wording. Distinct from the
      // Phase 0 "no other agents" fixture in name only — kept here so the
      // Build Plan's exact scenario title has its own pinned assertion,
      // guarding against a future edit to the Phase 0 test silently dropping
      // this coverage.
      await seedLedger(coordinationFilePath, [
        ceilingEntry(),
        ghostDibsEntry('ghost-orch', 1),
        ghostClaimLedger('ghost-orch', 1),
      ]);

      const beat = runCli(['--orchestrator-id=sweeper', '--desired-agents=1'], {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_NOW_MS: String(T0),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_BEAT_SNAPSHOT_TEST_MODE: '1',
        ARM_FAKE_PS_OUTPUT: buildResolvableIdentityNoOtherAgentsPsOutput(),
        ARM_FAKE_COLLECT_JSON: FAKE_COLLECT_JSON,
      });

      expect(beat.status).toBe(0);

      const { byId, claimsOf } = await readLedger(coordinationFilePath);
      expect(byId('ghost-orch')).toBeUndefined();
      expect(claimsOf(LIVE_AGENT_CLAIM_TYPE).map((claim) => claim.orchestratorId)).not.toContain('ghost-orch');
      expect(beat.stderr).toMatch(/reaped/);
    },
    30_000,
  );

  it(
    "writes exactly one stderr warning for the drift case, in warnCoordinationDegraded's vocabulary",
    async () => {
      // The guard is now evaluated PER LEDGER (reusing each ledger's own
      // unlocked pre-pass), but must still surface as exactly ONE warning for
      // the whole beat — not once per ledger it actually suppressed. Seeding
      // a ghost record in all three ledgers maximises the chance a naive
      // per-ledger implementation would warn three times instead of one.
      await seedLedger(coordinationFilePath, [
        ceilingEntry(),
        ghostDibsEntry('ghost-orch', 1),
        ghostClaimLedger('ghost-orch', 1),
        {
          orchestratorId: claimLedgerId(LIVE_AGENT_ADMISSION_CLAIM_TYPE),
          claims: [
            {
              orchestratorId: 'ghost-orch',
              count: 1,
              claimedAt: SEEDED_AT,
              pid: GHOST_PID,
              pidStartedAt: GHOST_PID_STARTED_AT,
            },
          ],
          grantedTotal: 1,
          declaredAt: SEEDED_AT,
        },
      ]);

      const beat = runCli(['--orchestrator-id=sweeper', '--desired-agents=1'], {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_NOW_MS: String(T0),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_BEAT_SNAPSHOT_TEST_MODE: '1',
        ARM_FAKE_PS_OUTPUT: buildNoAgentRootsPsOutput(),
        ARM_FAKE_COLLECT_JSON: FAKE_COLLECT_JSON,
      });

      expect(beat.status).toBe(0);

      // `warnCoordinationDegraded`'s fixed voice:
      // `agent-resource-management: warning: <what> unavailable (<code>); <consequence>`.
      const warningLines = beat.stderr
        .split('\n')
        .filter((line) => /^agent-resource-management: warning:/.test(line));
      expect(warningLines).toHaveLength(1);
    },
    30_000,
  );

  it(
    'applies uniformly to the DIBS ledger too, not just the two claim ledgers',
    async () => {
      // Isolates dibs: no claim-ledger entries are seeded at all, so a guard
      // implementation scoped only to the two claim ledgers (e.g. because
      // `claimType !== null` was used as a filter) would still let this
      // record through and this assertion would fail for the right reason.
      await seedLedger(coordinationFilePath, [ceilingEntry(), ghostDibsEntry('ghost-orch', 1)]);

      const beat = runCli(['--orchestrator-id=sweeper', '--desired-agents=1'], {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_NOW_MS: String(T0),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_BEAT_SNAPSHOT_TEST_MODE: '1',
        ARM_FAKE_PS_OUTPUT: buildNoAgentRootsPsOutput(),
        ARM_FAKE_COLLECT_JSON: FAKE_COLLECT_JSON,
      });

      expect(beat.status).toBe(0);

      const { byId } = await readLedger(coordinationFilePath);
      expect(byId('ghost-orch')).toBeDefined();
      expect(beat.stderr).not.toMatch(/reaped dibs/);
    },
    30_000,
  );

  it(
    '`identity: undefined` never arises from the real identity-resolution path — only `null` or a resolved object',
    () => {
      // Per the convergence-analysis notes already on : the guard's
      // condition, as specified, is `observation.identity === null` — a
      // strict-equality check that does NOT match `undefined`. This test
      // documents, rather than leaves ambiguous, that the distinction is
      // moot in practice: `resolveNearestClaudeRootIdentity` (the ONLY
      // producer of `observation.identity` — see cli.mjs's
      // `observeBeatProcesses`) is exhaustively typed to return either `null`
      // or a `{ pid, pidStartedAt }` object, on every input shape including
      // malformed ones. There is no code path through this beat's real
      // identity resolution that ever yields `undefined`, so a guard written
      // as `=== null` (rather than `== null` / `Object.is(x, undefined)`)
      // cannot be tricked into skipping the sweep, nor into failing to skip
      // it, by a value this codebase can actually produce.
      expect(resolveNearestClaudeRootIdentity('', TEST_PPID, T0)).toBeNull();
      expect(resolveNearestClaudeRootIdentity(buildNoAgentRootsPsOutput(), TEST_PPID, T0)).toBeNull();
      // Malformed/absent inputs also degrade to `null`, never `undefined` —
      // see resolveNearestClaudeRootIdentity's own type guards at the top of
      // its body (lib/recon.mjs).
      expect(resolveNearestClaudeRootIdentity(undefined, TEST_PPID, T0)).toBeNull();
      expect(resolveNearestClaudeRootIdentity(buildNoAgentRootsPsOutput(), undefined, T0)).toBeNull();
      expect(resolveNearestClaudeRootIdentity(buildNoAgentRootsPsOutput(), TEST_PPID, Number.NaN)).toBeNull();

      const resolved = resolveNearestClaudeRootIdentity(
        buildResolvableIdentityNoOtherAgentsPsOutput(),
        TEST_PPID,
        T0,
      );
      expect(resolved).toEqual({ pid: FAKE_CLAUDE_ROOT_PID, pidStartedAt: EXPECTED_PID_STARTED_AT });
    },
  );

  it(
    'is a complete no-op — no warning, no behavior change — when identity is null, liveProcesses is empty, ' +
      'and nothing in any ledger is a candidate to reap (the CI regression: a normal beat with nothing to do)',
    async () => {
      // THE CI REGRESSION THIS TEST PINS. The guard's precondition
      // (`identity === null && liveProcesses` a valid empty array) is the
      // ORDINARY shape of a CI runner: no claude-rooted ancestor in its
      // process tree, and no other live agent-rooted process either. Every
      // one of this repo's countless beat-wiring specs that assert an empty
      // `stderr` after an unremarkable beat hits this exact precondition in
      // CI — and a guard that warns unconditionally on the precondition alone
      // (rather than only when it actually protects a would-be-reaped
      // record) spams stderr on all of them. Only the ceiling row is seeded
      // here — no dibs, no live-agent claim, no live-agent-admission claim —
      // so there is nothing anywhere for the pre-pass to find, and the guard
      // must behave exactly as if had never landed.
      await seedLedger(coordinationFilePath, [ceilingEntry()]);

      const beat = runCli(['--orchestrator-id=sweeper', '--desired-agents=1'], {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_NOW_MS: String(T0),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_BEAT_SNAPSHOT_TEST_MODE: '1',
        ARM_FAKE_PS_OUTPUT: buildNoAgentRootsPsOutput(),
        ARM_FAKE_COLLECT_JSON: FAKE_COLLECT_JSON,
      });

      expect(beat.status).toBe(0);
      expect(beat.stderr).toBe('');
    },
    30_000,
  );

  it(
    'does not double-fire the new guard alongside the pre-existing PROCESS_SNAPSHOT_UNREADABLE warning for the same beat',
    async () => {
      // An unreadable snapshot already makes `observeBeatProcesses` return
      // `{ liveProcesses: null, identity: null, ... }` and warn
      // `PROCESS_SNAPSHOT_UNREADABLE` BEFORE `reconcileLedgers` is ever
      // called (cli.mjs's `main()` calls `observeBeatProcesses` first, then
      // `reconcileLedgers` with its result). `observation.liveProcesses` here
      // is `null`, not `Array.isArray(...) && .length === 0` — so the new
      // guard's own precondition (`Array.isArray(observation.liveProcesses)
      // && observation.liveProcesses.length === 0`) must not match this
      // shape at all. Exactly one warning line should appear on stderr for
      // this beat, carrying PROCESS_SNAPSHOT_UNREADABLE's own vocabulary, and
      // the ghost record must still be spared (deferred by the pre-existing
      // path, independent of this ticket's guard).
      await seedLedger(coordinationFilePath, [
        ceilingEntry(),
        ghostDibsEntry('ghost-orch', 1),
        ghostClaimLedger('ghost-orch', 1),
      ]);

      const beat = runCli(['--orchestrator-id=sweeper', '--desired-agents=1'], {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_NOW_MS: String(T0),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_BEAT_SNAPSHOT_TEST_MODE: '1',
        ARM_FAKE_PS_OUTPUT: buildUnreadablePsOutput(),
        ARM_FAKE_COLLECT_JSON: FAKE_COLLECT_JSON,
      });

      expect(beat.status).toBe(0);

      const { byId, claimsOf } = await readLedger(coordinationFilePath);
      expect(byId('ghost-orch')).toBeDefined();
      expect(claimsOf(LIVE_AGENT_CLAIM_TYPE).map((claim) => claim.orchestratorId)).toContain('ghost-orch');

      const warningLines = beat.stderr
        .split('\n')
        .filter((line) => /^agent-resource-management: warning:/.test(line));
      expect(warningLines).toHaveLength(1);
      expect(warningLines[0]).toMatch(/PROCESS_SNAPSHOT_UNREADABLE/);
    },
    30_000,
  );
});
