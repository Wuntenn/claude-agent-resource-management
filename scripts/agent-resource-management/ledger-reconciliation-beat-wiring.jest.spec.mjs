// Phase 3 — beat wiring: ONE process snapshot per beat, used BOTH to
// resolve this beat's write-side ledger identity AND to classify-and-reap
// every ghost record in the dibs, `live-agent` and `live-agent-admission`
// ledgers, early enough that the freed capacity is visible to that SAME
// invocation's own allowance/admission math.
//
// Black-box throughout: the real `cli.mjs` is driven as a CHILD PROCESS
// against a real temp coordination file, never imported (importing it runs
// `main()` at module scope). Matches ./cli-two-orchestrators.jest.spec.mjs's
// idiom.
//
// The behaviour pinned here: on an ORDINARY beat `cli.mjs` resolves ONE
// ancestry-derived identity and threads it into every ledger write call site
// that beat can reach — sites 1, 2, 3 and 5 of the five marked ` — ledger
// write call site N of 5` in that file — and sweeps all three ledgers against
// that same snapshot before its own allowance and admission math reads the
// file.
//
// Site 4 (`performEviction`) belongs to the eviction/watchdog beat types,
// which `main()` dispatches and returns from before an ordinary beat's
// observation is ever taken, and it resolves from its own
// `ARM_EVICT_TEST_MODE`-gated snapshot by design. That documented exception —
// and the seam rule that motivates it — is pinned in ./cli-evict.jest.spec.mjs,
// not here; cli.mjs's standing-rule block (near `resolveNow`) carries the
// argument.
//
// ADDITIVE. It does not modify ./ledger-reconciliation-outer-acceptance.jest.spec.mjs
// (the ticket's Phase 0 outer test), which owns the whole-ticket composition
// assertion; this file owns Phase 3's own behaviours one at a time.
//
// =========================================================================
// FIXTURE DISCIPLINE — the rule this ticket's Critical defect came from
// breaking
// =========================================================================
// Seed a PROCESS TREE and assert on the identity THE CODE DERIVED FROM IT.
// Never hand a "correct" pid to the fixture and then assert the ledger
// contains it: such a fixture passes identically whether the write path
// records `process.pid` (structurally invisible to the claude-rooted-only
// discovery walk, so every live orchestrator gets reaped on the next sweep)
// or an ancestry-walk-derived pid (the design). See
// ./ledger-reconciliation-outer-acceptance.jest.spec.mjs's header for the
// full argument — this file follows it rather than restating it.
//
// The walk's starting point is knowable at fixture-construction time even
// though the spawned child's own pid is not: `ARM_FAKE_PS_OUTPUT` is an env
// var fixed BEFORE `spawn`, and the child's `process.ppid` is by construction
// this test process's own `process.pid`. So the fake tree below is GENERATED
// around the real anchors, and carries no fabricated row for the `cli.mjs`
// child itself — the walk starts above it and never consults it.
//
//     1 (launchd)
//     ├── FAKE_CLAUDE_ROOT_PID   comm .../claude   <-- the answer
//     │   └── TEST_PPID          comm .../node     (non-claude hop 1)
//     │       └── TEST_PID       comm .../node     (non-claude hop 2; the
//     │                                             real `process.ppid` of
//     │                                             every cli.mjs child)
//     └── DECOY_CLAUDE_ROOT_PID  comm .../claude   (unrelated branch)
//
// GHOST PIDS. The beat's only oracle for liveness is the snapshot it was
// handed, so a "confirmed-dead" pid here means "carried by no row in this
// fixture" — not "not running on the host". `GHOST_PID` is asserted absent
// from the fixture rather than probed against the machine.
//
// =========================================================================
// SEAM CONTRACT — READ BEFORE CHANGING THE WIRING
// =========================================================================
// Every seam below exists in `cli.mjs`. Three predate this file and are reused
// as-is:
//
//   ARM_COORDINATION_FILE   the coordination file path.
//   ARM_FAKE_PS_OUTPUT      `ps -Ao pid,ppid,rss,etime,comm` plaintext, fed
//                           through ./lib/recon.mjs's `listAgentProcesses`.
//                           Setting it MUST NOT, by itself, disable the sweep
//                           or identity resolution — a beat with a fake
//                           snapshot behaves exactly as one with a real
//                           snapshot. It is honoured on the beat path only
//                           alongside the master switch below.
//   ARM_BEAT_SNAPSHOT_TEST_MODE=1
//                           Layer 1 of the beat snapshot's two-layer seam
//                           model (mirroring `ARM_EVICT_TEST_MODE` on the
//                           eviction beat): the master switch that must be set
//                           before `collectBeatPsSnapshot` honours ANY
//                           `ARM_FAKE_PS_*` seam. Unset — the default, and true
//                           of every real orchestrator invocation — the beat
//                           shells out to a real `ps`, so a leaked or stale
//                           `ARM_FAKE_PS_OUTPUT` cannot decide which records
//                           the sweep deletes. `ARM_COORDINATION_FILE` cannot
//                           serve as that gate: it leaks in the same breath as
//                           the seam it would gate, since a stale test
//                           environment exports both together. (It is NOT true
//                           that "every real invocation sets it", which this
//                           line said before pre-PR review —
//                           `resolveCoordinationFilePath` has a real
//                           production default.)
//                           `baseEnv` sets this switch for every env object in
//                           this file, alongside the seams it governs.
//   ARM_FAKE_NOW_MS         fixes `resolveNow()`, so an exact `pidStartedAt`
//                           is expressible. Honoured only alongside
//                           `ARM_COORDINATION_FILE` — every env object
//                           in this file goes through `baseEnv`, which sets
//                           both.
//
// FOUR SEAMS this file requires and `cli.mjs` carries for it. Each follows the
// existing `ARM_FAKE_*` convention and, like `ARM_FAKE_NOW_MS`, is honoured
// ONLY when `ARM_COORDINATION_FILE` is also set, so no production invocation
// can reach any of them. These four and no variants:
//
//   ARM_FAKE_PS_DELAY_MS=<integer ms>
//       Delays the beat's process-snapshot collection by that many
//       milliseconds before it yields output, standing in for a slow `ps` on
//       a loaded host. It models LATENCY ONLY: the snapshot returned is
//       unchanged. Its purpose is to make lock-hold discipline observable —
//       if the snapshot is taken inside a `withLock` hold, a concurrent
//       orchestrator's own write waits out this delay; if it is hoisted
//       outside the hold (as `computeAvailableCapacity`'s sampled-before-lock
//       pattern already does in this file), the concurrent write is
//       unaffected.
//
//   ARM_FAKE_PS_FAILURE=1
//       Makes the beat's process-snapshot collection report FAILURE, exactly
//       as a real `ps` shell-out throwing would. This is deliberately NOT the
//       same as a snapshot containing no rows: an empty snapshot is a valid
//       observation of zero agents and correctly makes every pid-bearing
//       record `confirmed-dead`, whereas a FAILED collection is no
//       observation at all and every record falls to `unknown`/TTL.
//       `collectPsOutput()` cannot express that difference — it swallows the
//       throw and returns `''` — which is why the beat collects through its
//       own `collectBeatPsSnapshot`, passes `null` (not `[]`) to
//       `classifyLedgerEntryLiveness` on failure, omits `pid`/`pidStartedAt`
//       from that beat's writes, and emits one `warnCoordinationDegraded`
//       stderr warning.
//
//   ARM_FAKE_PS_COLLECTION_LOG=<file path>
//       One line appended per process-snapshot collection the invocation
//       performs — from BOTH collection paths, the ordinary beat's memoized
//       `collectBeatPsSnapshot` and `collectPsOutput` (the eviction path, and
//       `main()`'s RED-memory pause-candidate selection) — so "one snapshot
//       feeding the sweep and the identity" is COUNTABLE. Under a fixed
//       snapshot and a fixed clock, two independent resolutions derive
//       byte-identical values, so no assertion on what was WRITTEN can tell one
//       resolution threaded to every call site from several that agreed. The
//       line count can. The count is a count of COLLECTIONS — call sites that
//       asked for a snapshot — not of observations, and not of `ps` processes
//       (the recorder runs before the fake/real branch, so a fixture-served
//       collection is counted like a spawned one; in production the two
//       coincide). On a GREEN beat (every beat in this file) it is exactly one,
//       while a RED-memory beat collects again for the pause-candidate list,
//       which reads no ledger and writes nothing.
//
//   ARM_FAKE_SWEEP_LOCK_LOG=<file path>
//       One line appended per ledger the SWEEP DECIDES to enter under the
//       coordination lock — `dibs`, `live-agent`, `live-agent-admission`.
//       Counted at the decision, before the lock is taken, so a ledger whose
//       lock then times out still logs its line. This is what makes the unlocked
//       pre-pass's benefit is COUNTABLE. That pre-pass changes no outcome by
//       design (the locked pass re-reads and re-judges everything), so no
//       assertion on what the ledger CONTAINS can tell a beat that took three
//       locks from one that took none; only this count can. Scope is the
//       sweep's own critical sections — the beat's ordinary declare/claim
//       writes take their own locks and are not counted.
//
//   ARM_FAKE_SWEEP_WRITE_FAILURES_JSON=<JSON object>
//       Keys are ledger keys — `dibs`, `live-agent`,
//       `live-agent-admission` (the two claim keys are the claim TYPE
//       strings). Values are the `.code` of the error to throw at the moment
//       the sweep commits that ledger's reap write, inside the critical
//       section, before anything lands on disk:
//         'COORDINATION_LOCK_TIMEOUT'  -> `LockTimeoutError`
//         'COORDINATION_LOCK_LOST'     -> `LockLostError`
//       Scope is the SWEEP's own reap writes only — never `declareDibs`,
//       `claimCapacity`, `reserveAdmission` or `claimAndAdmitQueueEntry` when
//       those run as this beat's own ordinary writes. A ledger not named in
//       the object sweeps normally, which is what makes partial-sweep
//       behaviour observable.
//
// WHAT THE SEAMS IMPLY ABOUT THE IMPLEMENTATION, stated so it is not inferred
// from the tests alone: the observation the sweep and the write path share is
// collected ONCE per beat, outside every lock (a RED-memory beat collects
// again for its advisory pause-candidate list, which touches no ledger);
// identity is resolved ONCE from that snapshot and threaded
// unchanged into every call site that beat reaches (1, 2, 3 and 5 — site 4 is
// unreachable from an ordinary beat); each ledger's classify-and-reap is its own
// `withLock`/`writeEntriesAtomic` critical section, so a failure on one leaves
// the others reconciled and leaves the failed one untouched rather than
// half-written.

import { spawn, spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { copyFile, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { AIMD_CEILING_RESERVED_ID } from './lib/coordination-file.mjs';
import { classifyProcessSnapshotShape, listAgentProcesses } from './lib/recon.mjs';
import { LIVENESS_REASON } from './lib/reconciliation.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

// Mirrors of cli.mjs's own constants, restated as literals — cli.mjs cannot
// be imported from a spec (it runs `main()` at module scope). Same convention
// as every other black-box spec in this directory.
const LIVE_AGENT_CEILING = 4; // cli.mjs's LIVE_AGENT_CEILING
/**
 * cli.mjs's `PS_BIN`. Every `ps` this spec shells out to MUST be this one and
 * not `$PATH`'s: the assertions below are about the output of the binary
 * PRODUCTION runs, and production resolves it absolutely (cli.mjs's LOW-10
 * fix) precisely so a foreign `ps` earlier on `$PATH` cannot change what it
 * sees. A probe reading `$PATH` would measure a different binary from the code
 * under test — on a macOS host carrying a nix/Homebrew/wrapper `ps`, a
 * basename-emitting stranger would report this host "unsupported" and trip the
 * darwin hard-throw below, taking the whole suite down over an environment
 * difference that cannot reach production at all.
 */
const PS_BIN = '/bin/ps';
/** The exact argv production's `runPsProbe` uses, so the parse matches too. */
const PS_ARGS = ['-Ao', 'pid,ppid,rss,etime,comm'];
const LIVE_AGENT_CLAIM_TYPE = 'live-agent';
const LIVE_AGENT_ADMISSION_CLAIM_TYPE = 'live-agent-admission';

/** Ledger entry id convention for a per-type claim ledger. */
const claimLedgerId = (type) => `__claim:${type}__`;

/** GREEN, low-pressure host fixture — keeps axis classification out of the way. */
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const FAKE_COLLECT_JSON = JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN });

/** Fixed fake `now`, so `pidStartedAt` arithmetic below is exact. */
const T0 = 1_700_000_000_000;
/** Every hand-seeded ghost record is 60s old — far inside EVERY ledger's own TTL. */
const SEEDED_AT = T0 - 60_000;

const ANCESTOR_ETIME = '01:00:00';
const EXPECTED_PID_STARTED_AT = T0 - 60 * 60 * 1000;

const TEST_PID = process.pid;
const TEST_PPID = process.ppid;

const FAKE_CLAUDE_ROOT_PID = 920_001;
const DECOY_CLAUDE_ROOT_PID = 920_002;
/** Carried by no row in the fixture — the snapshot is the sweep's only oracle. */
const GHOST_PID = 920_500;
const GHOST_PID_STARTED_AT = T0 - 7_200_000;
/**
 * A pid the snapshot DOES carry, paired with a start time nowhere near the
 * one the snapshot reports for it: the PID-REUSE signal, distinct from
 * "absent" and reported with its own reason token.
 */
const REUSED_PID = FAKE_CLAUDE_ROOT_PID;
const REUSED_PID_STARTED_AT = EXPECTED_PID_STARTED_AT - 86_400_000;

/** The fake `ps -Ao pid,ppid,rss,etime,comm` table drawn in this file's header. */
function buildAncestryPsOutput() {
  return [
    '  PID  PPID    RSS     ELAPSED COMM',
    '    1     0    512 1-03:46:39 /sbin/launchd',
    `${FAKE_CLAUDE_ROOT_PID}     1 102400    ${ANCESTOR_ETIME} /Users/someone/.local/bin/claude`,
    `${DECOY_CLAUDE_ROOT_PID}     1  98304    02:00:00 /Users/someone/.local/bin/claude`,
    `${TEST_PPID} ${FAKE_CLAUDE_ROOT_PID}  81920    00:30:00 /usr/local/bin/node`,
    `${TEST_PID} ${TEST_PPID}  65536    00:10:00 /usr/local/bin/node`,
    '',
  ].join('\n');
}

/** The same tree with every claude-rooted row removed — the standalone shape. */
function buildNoClaudeAncestorPsOutput() {
  return [
    '  PID  PPID    RSS     ELAPSED COMM',
    '    1     0    512 1-03:46:39 /sbin/launchd',
    `${TEST_PPID}     1  81920    00:30:00 /usr/local/bin/node`,
    `${TEST_PID} ${TEST_PPID}  65536    00:10:00 /usr/local/bin/node`,
    '',
  ].join('\n');
}

/**
 * A snapshot that SUCCEEDS and is unreadable: the same processes as the
 * ancestry fixture, emitted with comma separators instead of whitespace
 * columns. Stands in for the realistic drift — a column-order, locale, or
 * header change — that leaves `ps` exiting zero while this project's line
 * grammar matches not one row of its output. `listAgentProcesses` returns `[]`
 * for it, indistinguishably from a host running no agents at all.
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

/** A snapshot that is genuinely EMPTY — a well-formed header and no rows. */
function buildEmptyPsOutput() {
  return '  PID  PPID    RSS     ELAPSED COMM\n';
}

/**
 * A snapshot that is small, entirely PARSEABLE, and carries no claude-rooted
 * tree AT ALL — not even one for this beat's own orchestrator — what an
 * agentless host actually looks like to `ps -A`. Since its identity
 * cross-check guard, this is no longer "the case that must still reap": with
 * no claude-rooted row anywhere, `resolveNearestClaudeRootIdentity` also
 * resolves `null` for the beat's own identity, and `identity === null` +
 * `liveProcesses === []` is exactly the drift-suspect shape the guard exists
 * to catch — the snapshot cannot even vouch for the sweeper itself, so a
 * pid-bearing record's absence from it is not trustworthy evidence of death.
 * See the test below for the corrected expectation.
 */
function buildAgentlessPsOutput() {
  return [
    '  PID  PPID    RSS     ELAPSED COMM',
    '    1     0    512 1-03:46:39 /sbin/launchd',
    '  200     1  20480 1-03:46:30 /usr/libexec/logd',
    `${TEST_PPID}     1  81920    00:30:00 /usr/local/bin/node`,
    '',
  ].join('\n');
}

/** Runs the real cli.mjs as a child process — never imports it. */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/**
 * Runs the real cli.mjs CONCURRENTLY — unlike `runCli`/`spawnSync` it does not
 * block this test's event loop, so two invocations genuinely race for the same
 * coordination-file lock. Mirrors ./cli-two-orchestrators.jest.spec.mjs's
 * `runCliAsync`.
 */
function runCliAsync(args, extraEnv) {
  return new Promise((resolvePromise, rejectPromise) => {
    const child = spawn('node', [CLI, ...args], { env: { ...process.env, ...extraEnv } });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => {
      stdout += chunk;
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk;
    });
    child.on('error', rejectPromise);
    child.on('close', (status) => resolvePromise({ status, stdout, stderr }));
  });
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

let workDir;
let coordinationFilePath;
let queueFilePath;

/**
 * Every env object in this file goes through here — it pairs ARM_FAKE_NOW_MS
 * with ARM_COORDINATION_FILE and every ARM_FAKE_PS_* seam with
 * ARM_BEAT_SNAPSHOT_TEST_MODE (the beat snapshot's Layer 1 master switch).
 */
function baseEnv(extra = {}) {
  return {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_QUEUE_FILE: queueFilePath,
    ARM_FAKE_COLLECT_JSON: FAKE_COLLECT_JSON,
    // Layer 1 — without it every ARM_FAKE_PS_* seam below is inert and the
    // beat shells out to the real `ps`. See this file's SEAM CONTRACT header.
    ARM_BEAT_SNAPSHOT_TEST_MODE: '1',
    ARM_FAKE_PS_OUTPUT: buildAncestryPsOutput(),
    ARM_BEAT_NOW_TEST_MODE: '1',
    ARM_FAKE_NOW_MS: String(T0),
    ...extra,
  };
}

function cli(args, extraEnv = {}) {
  // ARM_COORDINATION_FILE — and every other base seam — comes from `baseEnv`,
  // so a caller overriding one seam never has to restate the rest.
  return runCli(args, baseEnv(extraEnv));
}

/** The AIMD ceiling row, so `LIVE_AGENT_CEILING` is deterministic in every fixture. */
function ceilingEntry() {
  return {
    orchestratorId: AIMD_CEILING_RESERVED_ID,
    ceiling: LIVE_AGENT_CEILING,
    sustainedNormalCount: 0,
    declaredAt: SEEDED_AT,
  };
}

function ghostDibs(orchestratorId, desiredAgents, identity = { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }) {
  return {
    orchestratorId,
    desiredAgents,
    declaredAt: SEEDED_AT,
    firstDeclaredAt: SEEDED_AT,
    ...identity,
  };
}

function claimRecord(orchestratorId, count, identity) {
  return { orchestratorId, count, claimedAt: SEEDED_AT, ...identity };
}

function claimLedger(type, claims) {
  return {
    orchestratorId: claimLedgerId(type),
    claims,
    grantedTotal: claims.reduce((sum, claim) => sum + claim.count, 0),
    declaredAt: SEEDED_AT,
  };
}

async function seedLedger(entries) {
  await writeFile(coordinationFilePath, JSON.stringify(entries, null, 2), 'utf8');
}

async function readLedger() {
  const persisted = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
  return {
    all: persisted,
    byId: (id) => persisted.find((entry) => entry.orchestratorId === id),
    claimsOf: (type) => persisted.find((entry) => entry.orchestratorId === claimLedgerId(type))?.claims ?? [],
  };
}

const claimantIds = (claims) => claims.map((claim) => claim.orchestratorId);

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-recon-beat-'));
  coordinationFilePath = join(workDir, 'coordination.json');
  queueFilePath = join(workDir, 'queue.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

it('the fixture tree keeps its synthetic pids clear of the real anchors', () => {
  // The premise every identity assertion below rests on: if a synthetic pid
  // collided with a real one, "the walk resolved the ancestor" would be
  // indistinguishable from "the walk resolved itself".
  expect(new Set([FAKE_CLAUDE_ROOT_PID, DECOY_CLAUDE_ROOT_PID, GHOST_PID, TEST_PID, TEST_PPID]).size).toBe(5);
  expect(buildAncestryPsOutput()).not.toContain(String(GHOST_PID));
});

// ---------------------------------------------------------------------------
// Ancestry-derived identity on the write path.
// ---------------------------------------------------------------------------

describe('write-side identity is resolved by walking ancestry, once per beat', () => {
  it('records the claude-rooted PARENT\'s pid under a realistic parent/child tree, not cli.mjs\'s own pid', async () => {
    await seedLedger([ceilingEntry()]);

    const beat = cli(['--orchestrator-id=ancestry-orch', '--desired-agents=1']);
    expect(beat.status).toBe(0);

    const { byId } = await readLedger();
    const entry = byId('ancestry-orch');

    expect(entry.pid).toBe(FAKE_CLAUDE_ROOT_PID);
    expect(entry.pidStartedAt).toBe(EXPECTED_PID_STARTED_AT);
    // Said the other way round: neither self-referential, nor a "first
    // claude-rooted process I could find" shortcut past the ancestry walk.
    expect(entry.pid).not.toBe(TEST_PID);
    expect(entry.pid).not.toBe(TEST_PPID);
    expect(entry.pid).not.toBe(DECOY_CLAUDE_ROOT_PID);
  }, 30_000);

  it('writes byte-identical pid/pidStartedAt to both ledgers it touches in one beat', async () => {
    // AC 5 — single resolution per beat. `ARM_FAKE_NOW_MS` is deliberately
    // OMITTED here, so `now` is the real clock: two independent resolutions
    // inside one beat would read two different `now` values and derive two
    // different `pidStartedAt`s. Byte-equality is therefore evidence of one
    // resolution threaded to both call sites, not an artifact of a frozen
    // clock.
    await seedLedger([ceilingEntry()]);

    const beat = cli(['--orchestrator-id=one-identity-orch', '--desired-agents=2'], {
      ARM_FAKE_NOW_MS: '',
      ARM_BEAT_NOW_TEST_MODE: '1',
    });
    expect(beat.status).toBe(0);

    const { byId, claimsOf } = await readLedger();
    const dibsEntry = byId('one-identity-orch');
    const admissionRecord = claimsOf(LIVE_AGENT_ADMISSION_CLAIM_TYPE).find(
      (claim) => claim.orchestratorId === 'one-identity-orch',
    );

    expect(dibsEntry.pid).toBe(FAKE_CLAUDE_ROOT_PID);
    expect(admissionRecord).toBeDefined();
    expect(admissionRecord.pid).toBe(dibsEntry.pid);
    expect(admissionRecord.pidStartedAt).toBe(dibsEntry.pidStartedAt);
  }, 30_000);

  it('gives handleDequeueIfCapacity\'s claimAndAdmitQueueEntry the same beat-resolved identity as the dibs call site', async () => {
    // The fifth call site is the only one reaching the ledger through
    // ./lib/dispatch.mjs rather than ./lib/coordination-file.mjs directly,
    // and the one already missed once. `--dequeue-if-capacity` is a
    // standalone beat type (it cannot be combined with `--desired-agents`),
    // so the comparison is across two invocations under one frozen
    // `ARM_FAKE_NOW_MS` and one snapshot. That proves the dispatch path
    // resolves the SAME ancestry identity; the single-resolution-per-beat
    // property is proven separately by the test above.
    await seedLedger([ceilingEntry()]);

    const dibsBeat = cli(['--orchestrator-id=ident-dibs', '--desired-agents=1']);
    expect(dibsBeat.status).toBe(0);

    const enqueued = cli(['--orchestrator-id=ident-dibs', '--enqueue=implementation-agent:normal:echo-hi']);
    expect(enqueued.status).toBe(0);

    const dispatchBeat = cli(['--orchestrator-id=ident-dispatch', '--dequeue-if-capacity']);
    expect(dispatchBeat.status).toBe(0);
    expect(JSON.parse(dispatchBeat.stdout).granted).toBe(true);

    const { byId, claimsOf } = await readLedger();
    const dibsIdentity = byId('ident-dibs');
    const dispatchRecord = claimsOf(LIVE_AGENT_ADMISSION_CLAIM_TYPE).find(
      (claim) => claim.orchestratorId === 'ident-dispatch',
    );

    expect(dibsIdentity.pid).toBe(FAKE_CLAUDE_ROOT_PID);
    expect(dispatchRecord).toBeDefined();
    expect(dispatchRecord.pid).toBe(dibsIdentity.pid);
    expect(dispatchRecord.pidStartedAt).toBe(dibsIdentity.pidStartedAt);
  }, 30_000);

  it('omits pid/pidStartedAt entirely — no sentinel, no crash — when no claude-rooted ancestor exists', async () => {
    // The standalone-invocation degrade path: `cli.mjs` run outside any
    // orchestrator. Distinct from a FAILED snapshot (below): collection
    // succeeds here and simply finds no matching ancestor.
    await seedLedger([ceilingEntry()]);

    // Control first, under the ANCESTRY fixture: this same beat shape records
    // an identity when one is resolvable. Without it, the omission asserted
    // below would be satisfied just as well by a build that never records an
    // identity under any fixture at all.
    const controlBeat = cli(['--orchestrator-id=control-orch', '--desired-agents=1']);
    expect(controlBeat.status).toBe(0);
    expect((await readLedger()).byId('control-orch').pid).toBe(FAKE_CLAUDE_ROOT_PID);

    const beat = cli(['--orchestrator-id=standalone-orch', '--desired-agents=2'], {
      ARM_FAKE_PS_OUTPUT: buildNoClaudeAncestorPsOutput(),
    });
    expect(beat.status).toBe(0);

    const { byId, claimsOf } = await readLedger();
    const dibsEntry = byId('standalone-orch');
    const admissionRecord = claimsOf(LIVE_AGENT_ADMISSION_CLAIM_TYPE).find(
      (claim) => claim.orchestratorId === 'standalone-orch',
    );

    // Absent, not `null`/`0`/`-1`: an entry carrying half an identity, or a
    // sentinel that can never match a snapshot row, reads as `confirmed-dead`
    // on the next sweep — a live agent reaped on the strength of a
    // placeholder.
    expect(Object.hasOwn(dibsEntry, 'pid')).toBe(false);
    expect(Object.hasOwn(dibsEntry, 'pidStartedAt')).toBe(false);
    expect(admissionRecord).toBeDefined();
    expect(Object.hasOwn(admissionRecord, 'pid')).toBe(false);
    expect(Object.hasOwn(admissionRecord, 'pidStartedAt')).toBe(false);
  }, 30_000);
});

// ---------------------------------------------------------------------------
// The sweep itself.
// ---------------------------------------------------------------------------

describe('one beat reconciles every ledger and reports the freed capacity itself', () => {
  it('reaps every ghost dibs/claim/admission record and reports full capacity in that same invocation', async () => {
    await seedLedger([
      ceilingEntry(),
      ghostDibs('ghost-orch', LIVE_AGENT_CEILING),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [
        claimRecord('ghost-orch', 3, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
      ]),
      claimLedger(LIVE_AGENT_ADMISSION_CLAIM_TYPE, [
        claimRecord('ghost-orch', 2, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
      ]),
    ]);

    const beat = cli(['--orchestrator-id=sweeper', '--desired-agents=3']);
    expect(beat.status).toBe(0);

    const { byId, claimsOf } = await readLedger();

    // Every seeded ghost is 60s old — far inside `DEFAULT_CLAIM_TTL_MS` (6h)
    // and the dibs liveness window (15min) — so nothing here can have aged
    // out. If it is gone, the sweep removed it.
    expect(byId('ghost-orch')).toBeUndefined();
    expect(claimantIds(claimsOf(LIVE_AGENT_CLAIM_TYPE))).not.toContain('ghost-orch');
    expect(claimantIds(claimsOf(LIVE_AGENT_ADMISSION_CLAIM_TYPE))).not.toContain('ghost-orch');

    // And the capacity is free in THIS beat's own report, not merely on disk
    // afterwards — the actual functional promise.
    const parsed = JSON.parse(beat.stdout);
    expect(parsed.type).toBe('spawn-allowed');
    expect(parsed.allowance).toBe(LIVE_AGENT_CEILING);
  }, 30_000);

  it('sweeps before this beat\'s own admission decision, freeing exactly the headroom the request needed', async () => {
    // Ceiling 4, three slots held by ghosts, this beat asks for 3. Reaped
    // first: the full ask of 3 is granted. Reaped afterwards (or not at all):
    // only the single genuinely-free slot is available, so the grant is 1 —
    // a beat that satisfies "the sweep runs each beat" while missing the
    // capacity-restoration promise entirely.
    await seedLedger([
      ceilingEntry(),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [
        claimRecord('ghost-orch', 3, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
      ]),
    ]);

    const beat = cli(['--orchestrator-id=ordering-orch', '--desired-agents=3']);
    expect(beat.status).toBe(0);

    expect(JSON.parse(beat.stdout).liveAgentGrant).toBe(3);
  }, 30_000);

  it('leaves a genuinely live record untouched through an otherwise fully ghosted beat', async () => {
    // The Secure AC. The live record's identity is whatever an earlier REAL
    // beat chose to write — never a pid this file invented.
    await seedLedger([ceilingEntry()]);

    const liveBeat = cli(['--orchestrator-id=live-orch', `--claim=${LIVE_AGENT_CLAIM_TYPE}:1`]);
    expect(liveBeat.status).toBe(0);

    const produced = (await readLedger())
      .claimsOf(LIVE_AGENT_CLAIM_TYPE)
      .find((claim) => claim.orchestratorId === 'live-orch');
    expect(produced).toMatchObject({ orchestratorId: 'live-orch', count: 1 });

    await seedLedger([
      ceilingEntry(),
      ghostDibs('ghost-orch', LIVE_AGENT_CEILING),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [
        claimRecord('ghost-orch', 2, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
        produced,
      ]),
    ]);

    const beat = cli(['--orchestrator-id=sweeper', '--desired-agents=1']);
    expect(beat.status).toBe(0);

    const { byId, claimsOf } = await readLedger();
    expect(byId('ghost-orch')).toBeUndefined();
    expect(claimsOf(LIVE_AGENT_CLAIM_TYPE).find((claim) => claim.orchestratorId === 'live-orch')).toMatchObject({
      orchestratorId: 'live-orch',
      count: produced.count,
      pid: produced.pid,
      pidStartedAt: produced.pidStartedAt,
    });
  }, 30_000);

  it('reaps only the ghost record when two records share one orchestratorId (the respawn case)', async () => {
    // AC 4 — record granularity. `claimCapacity` APPENDS rather than
    // upserting, so a crash-and-immediate-respawn leaves two records under
    // one id inside one `claims[]` array. A sweep operating at id
    // granularity takes the live one with the dead one.
    await seedLedger([ceilingEntry()]);

    const liveBeat = cli(['--orchestrator-id=respawn-orch', `--claim=${LIVE_AGENT_CLAIM_TYPE}:1`]);
    expect(liveBeat.status).toBe(0);

    const produced = (await readLedger())
      .claimsOf(LIVE_AGENT_CLAIM_TYPE)
      .find((claim) => claim.orchestratorId === 'respawn-orch');

    await seedLedger([
      ceilingEntry(),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [
        claimRecord('respawn-orch', 2, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
        produced,
      ]),
    ]);

    const beat = cli(['--orchestrator-id=sweeper', '--desired-agents=1']);
    expect(beat.status).toBe(0);

    const afterSweep = await readLedger();
    const survivors = afterSweep.claimsOf(LIVE_AGENT_CLAIM_TYPE).filter(
      (claim) => claim.orchestratorId === 'respawn-orch',
    );

    expect(survivors).toHaveLength(1);
    expect(survivors[0]).toMatchObject({ count: produced.count, pid: produced.pid });
    expect(survivors[0].pid).not.toBe(GHOST_PID);

    // The ledger's aggregate is RECOMPUTED from the survivors, not carried
    // over. This is the partial-reap path — the row survives, so unlike a
    // full reap there is no row-drop to hide a stale total behind. And it is
    // load-bearing: `coordination-file.mjs` reads `grantedTotal` directly for
    // its legacy-shape capacity math, so a total still counting the reaped
    // ghost's 2 slots means the sweep did not actually restore the capacity it
    // reported freeing.
    const remainingClaims = afterSweep.claimsOf(LIVE_AGENT_CLAIM_TYPE);
    const summedCounts = remainingClaims.reduce((sum, claim) => sum + claim.count, 0);
    expect(afterSweep.byId(claimLedgerId(LIVE_AGENT_CLAIM_TYPE)).grantedTotal).toBe(summedCounts);
    // Stated as a literal too, so the assertion above cannot be satisfied by
    // a sweep that leaves both the claims and the total untouched.
    expect(afterSweep.byId(claimLedgerId(LIVE_AGENT_CLAIM_TYPE)).grantedTotal).toBe(produced.count);
  }, 30_000);

  it('names each reaped target and its reason on stderr, without dumping the ledger', async () => {
    await seedLedger([
      ceilingEntry(),
      ghostDibs('absent-orch', 1),
      ghostDibs('reused-orch', 1, { pid: REUSED_PID, pidStartedAt: REUSED_PID_STARTED_AT }),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [
        claimRecord('absent-orch', 1, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
      ]),
    ]);

    const beat = cli(['--orchestrator-id=sweeper', '--desired-agents=1']);
    expect(beat.status).toBe(0);

    // Target: which orchestrator, which ledger, which pid.
    expect(beat.stderr).toMatch(/absent-orch/);
    expect(beat.stderr).toMatch(/reused-orch/);
    expect(beat.stderr).toContain(String(GHOST_PID));
    expect(beat.stderr).toContain(LIVE_AGENT_CLAIM_TYPE);

    // Reason, using `LIVENESS_REASON`'s own stable machine tokens rather
    // than prose — the two verdicts must stay distinguishable to whoever
    // triages the log.
    expect(beat.stderr).toContain(LIVENESS_REASON.PID_ABSENT);
    expect(beat.stderr).toContain(LIVENESS_REASON.PID_REUSE);

    // One line per reaped record, not a file dump: no ledger field that only
    // a wholesale serialization would carry.
    expect(beat.stderr).not.toMatch(/memoryHistory|firstDeclaredAt|grantedTotal/);
  }, 30_000);
});

// ---------------------------------------------------------------------------
// Degraded and failing paths.
// ---------------------------------------------------------------------------

describe('a failed process snapshot degrades the beat rather than breaking it', () => {
  it('warns on stderr and falls back to TTL-only for BOTH the sweep and identity resolution', async () => {
    // A failed collection is no observation at all, so nothing may be reaped
    // on the strength of it — reaping here would turn one flaky `ps` into a
    // fleet-wide reap of live agents. And this beat's own writes carry no
    // identity, exactly as a standalone invocation's would.
    await seedLedger([
      ceilingEntry(),
      ghostDibs('ghost-orch', 1),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [
        claimRecord('ghost-orch', 1, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
      ]),
    ]);

    const beat = cli(['--orchestrator-id=degraded-orch', '--desired-agents=1'], {
      ARM_FAKE_PS_FAILURE: '1',
    });

    expect(beat.status).toBe(0);
    expect(beat.stderr).toMatch(/warning/i);

    const { byId, claimsOf } = await readLedger();

    // Both ghosts survive: 60s old, well inside their own ledgers' TTLs.
    expect(byId('ghost-orch')).toBeDefined();
    expect(claimantIds(claimsOf(LIVE_AGENT_CLAIM_TYPE))).toContain('ghost-orch');

    // And this beat's own record carries no identity to be misread later.
    expect(Object.hasOwn(byId('degraded-orch'), 'pid')).toBe(false);
  }, 30_000);

  it('treats a snapshot that succeeds but carries no parseable row as no observation, not as zero agents', async () => {
    // The mass-reap case. A `ps` that exits ZERO and whose output this
    // project's column grammar cannot read parses to zero rows without
    // throwing, so a beat that checks only for collection FAILURE sees a
    // perfectly ordinary `[]` — a valid observation of an empty host — and
    // condemns every pid-bearing record in every ledger as `pid-absent`. That
    // is the whole fleet, on one column/locale/header drift. shipped
    // exactly one such silent `ps` incompatibility already.
    await seedLedger([
      ceilingEntry(),
      ghostDibs('ghost-orch', 1),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [
        claimRecord('ghost-orch', 1, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
      ]),
    ]);

    const beat = cli(['--orchestrator-id=unreadable-orch', '--desired-agents=1'], {
      ARM_FAKE_PS_OUTPUT: buildUnreadablePsOutput(),
    });

    expect(beat.status).toBe(0);
    expect(beat.stderr).toMatch(/warning/i);
    // Nothing was reaped on the strength of a snapshot nobody could read.
    expect(beat.stderr).not.toMatch(/reaped/);

    const { byId, claimsOf } = await readLedger();
    expect(byId('ghost-orch')).toBeDefined();
    expect(claimantIds(claimsOf(LIVE_AGENT_CLAIM_TYPE))).toContain('ghost-orch');

    // …and, like a failed collection, this beat writes no identity rather than
    // one derived from evidence it could not read.
    expect(Object.hasOwn(byId('unreadable-orch'), 'pid')).toBe(false);
  }, 30_000);

  it('treats a snapshot with NO rows at all as no observation either', async () => {
    // CORRECTED BY PRE-PR REVIEW (Medium). This test used to assert the
    // opposite — that a header-only snapshot is "a valid observation of zero
    // agents" and reaps every pid-bearing record — justified on the ground that
    // deferring it "would disable the sweep on any genuinely idle host".
    //
    // That justification is false, and measurably so. `ps -A` enumerates EVERY
    // process on the host, and any host running this very beat is running at
    // minimum `launchd`, this `node` process, and (on the path that matters)
    // its claude-rooted parent. A real idle macOS host answers with hundreds of
    // rows: `ps -Ao pid,ppid,rss,etime,comm` on the development host answered
    // 667 body rows (668 lines, one of them the header) while idle. Zero body
    // lines is not an idle host — it is a collection that came
    // back broken wearing an exit code of zero, and passing it through means
    // `listAgentProcesses` answers `[]` and every pid-bearing record in all
    // three ledgers is reaped `pid-absent`. That is the same fleet-wide reap the
    // test above exists to prevent, reached through the other door.
    //
    // The agentless-host case the old rationale was reaching for is pinned by
    // the test below — but per the identity cross-check guard, that case
    // now DEFERS rather than reaps: a snapshot that can't even vouch for the
    // beat's own claude-rooted identity isn't trustworthy evidence that
    // anything else in it is dead. The case that must still reap — where the
    // beat's own identity resolves and only the ghosts are truly gone — is
    // covered separately in ledger-reconciliation-identity-guard.jest.spec.mjs.
    await seedLedger([
      ceilingEntry(),
      ghostDibs('ghost-orch', 1),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [
        claimRecord('ghost-orch', 1, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
      ]),
    ]);

    const beat = cli(['--orchestrator-id=empty-snapshot-orch', '--desired-agents=1'], {
      ARM_FAKE_PS_OUTPUT: buildEmptyPsOutput(),
    });

    expect(beat.status).toBe(0);
    expect(beat.stderr).toMatch(/warning/i);
    expect(beat.stderr).not.toMatch(/reaped/);

    const { byId, claimsOf } = await readLedger();
    expect(byId('ghost-orch')).toBeDefined();
    expect(claimantIds(claimsOf(LIVE_AGENT_CLAIM_TYPE))).toContain('ghost-orch');
    expect(Object.hasOwn(byId('empty-snapshot-orch'), 'pid')).toBe(false);
  }, 30_000);

  it('defers against a readable snapshot that carries no claude-rooted process at all, including its own beat (identity cross-check guard)', async () => {
    // UPDATED BY. This test used to assert the opposite — that a small,
    // wholly readable table with no claude-rooted tree in it is a genuine
    // observation of an agentless host, so every pid-bearing record in it
    // should be reaped. PARSEABILITY alone is not enough evidence, though:
    // `buildAgentlessPsOutput` carries no claude-rooted row anywhere, not even
    // one for THIS beat's own orchestrator, so `resolveNearestClaudeRootIdentity`
    // also resolves `null` for the sweep's own identity. A snapshot that cannot
    // even vouch for the sweeper itself is not trustworthy evidence that
    // anything else named in it is dead — that is the "agent-recognition
    // drift" gap, and the accepted tradeoff (see residual limitation
    // 1) is to give up ghost-reaping in this narrow case in favour of the
    // fail-safe direction: the record is deferred and still ages out by its
    // own ledger's TTL. A genuinely near-agentless host where the beat's OWN
    // identity resolves (i.e. its own claude-rooted ancestor is present, just
    // nothing else) is a DIFFERENT case and keeps reaping normally — see
    // ledger-reconciliation-identity-guard.jest.spec.mjs for that half.
    await seedLedger([
      ceilingEntry(),
      ghostDibs('ghost-orch', 1),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [
        claimRecord('ghost-orch', 1, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
      ]),
    ]);

    const beat = cli(['--orchestrator-id=agentless-orch', '--desired-agents=1'], {
      ARM_FAKE_PS_OUTPUT: buildAgentlessPsOutput(),
    });

    expect(beat.status).toBe(0);
    expect(beat.stderr).toMatch(/warning/i);
    expect(beat.stderr).toMatch(/AGENT_RECOGNITION_DRIFT_SUSPECTED/);
    expect(beat.stderr).not.toMatch(/reaped/);

    const { byId, claimsOf } = await readLedger();
    expect(byId('ghost-orch')).toBeDefined();
    expect(claimantIds(claimsOf(LIVE_AGENT_CLAIM_TYPE))).toContain('ghost-orch');
  }, 30_000);
});

describe('one process snapshot per GREEN beat, counted rather than inferred', () => {
  it('collects the snapshot exactly once however many ledger call sites and sweeps read it', async () => {
    // AC 5 — single resolution per beat. Asserting that two call sites wrote
    // equal values cannot pin this: under one fixed snapshot and one fixed
    // clock, two independent resolutions derive byte-identical values and
    // agree by construction. The COUNT of collections can, and it is also the
    // cost this memoization exists to avoid — a beat that re-resolves pays one
    // `ps` spawn per call site on an already-loaded host.
    //
    // The bound is exactly one because this beat's memory fixture is GREEN.
    // The counter measures COLLECTIONS — snapshot call sites, counted before
    // the fake/real branch, so it is not a count of `ps` processes — and a
    // RED-memory beat collects again for `main()`'s pause-candidate list, a
    // reader of no ledger and a writer of nothing, so it is outside what this
    // assertion is about.
    const collectionLogPath = join(workDir, 'ps-collections.log');
    await writeFile(collectionLogPath, '', 'utf8');

    await seedLedger([
      ceilingEntry(),
      ghostDibs('ghost-orch', 1),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [
        claimRecord('ghost-orch', 1, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
      ]),
    ]);

    // A beat that exercises the widest set of readers in one invocation: the
    // three ledger sweeps plus the dibs and admission write call sites.
    const beat = cli(['--orchestrator-id=single-snapshot-orch', '--desired-agents=2'], {
      ARM_FAKE_PS_COLLECTION_LOG: collectionLogPath,
    });
    expect(beat.status).toBe(0);

    const collections = (await readFile(collectionLogPath, 'utf8')).split('\n').filter(Boolean);

    // Non-vacuity first: the seam is wired, so `toHaveLength(1)` below is a
    // real bound rather than the count of a log nothing ever writes to.
    expect(collections.length).toBeGreaterThan(0);
    expect(collections).toHaveLength(1);

    // And the beat really did do the work whose readers are being counted —
    // it swept, and it wrote its own identity.
    expect((await readLedger()).byId('ghost-orch')).toBeUndefined();
    expect((await readLedger()).byId('single-snapshot-orch').pid).toBe(FAKE_CLAUDE_ROOT_PID);
  }, 30_000);
});

describe('the sweep locks only the ledgers that have something to reap', () => {
  // The unlocked pre-pass in `reconcileLedgers` changes NO outcome: with it or
  // without it, the same records are reaped and the same bytes land in the
  // file, because the locked pass re-reads and re-judges everything either
  // way. That is exactly what makes it safe — and exactly what makes it
  // invisible to every other assertion in this file. Its whole benefit is the
  // lock acquisitions it avoids on the overwhelmingly common beat with nothing
  // to reap, and lock-collision surface against the other orchestrators on
  // this host appears in no value the beat writes.
  //
  // So the benefit is pinned by COUNT, via `ARM_FAKE_SWEEP_LOCK_LOG` — one
  // line per ledger the sweep DECIDES to enter under lock (counted before the
  // lock is taken, so a ledger whose lock then fails still logs) — following the
  // `ARM_FAKE_PS_COLLECTION_LOG` precedent above and gated the same way
  // (`armTestModeEnabled()` first, `ARM_COORDINATION_FILE` second).
  //
  // Deleting the pre-pass — replacing its condition with any constant-false
  // test — makes both assertions below fail: the first would see three lines
  // where it demands none, the second three where it demands one.

  /** A ledger identity the beat's fake process tree DOES carry, so it classifies `alive`. */
  const LIVE_IDENTITY = { pid: FAKE_CLAUDE_ROOT_PID, pidStartedAt: EXPECTED_PID_STARTED_AT };

  async function runSweepAndReadLockLog(orchestratorId, seed) {
    const sweepLockLogPath = join(workDir, `sweep-locks-${orchestratorId}.log`);
    await writeFile(sweepLockLogPath, '', 'utf8');

    await seedLedger(seed);

    const beat = cli([`--orchestrator-id=${orchestratorId}`, '--desired-agents=1'], {
      ARM_FAKE_SWEEP_LOCK_LOG: sweepLockLogPath,
    });
    expect(beat.status).toBe(0);

    return (await readFile(sweepLockLogPath, 'utf8')).split('\n').filter(Boolean);
  }

  it('locks the one ledger holding an apparently-dead record, and not the other two', async () => {
    // The non-vacuity half, run FIRST so the empty-log assertion in the next
    // test is a real bound rather than the count of a log nothing ever writes
    // to. A ghost in the dibs array only; both claim ledgers hold records the
    // snapshot confirms alive.
    const locked = await runSweepAndReadLockLog('one-ledger-orch', [
      ceilingEntry(),
      ghostDibs('ghost-orch', 1),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [claimRecord('live-orch', 1, LIVE_IDENTITY)]),
      claimLedger(LIVE_AGENT_ADMISSION_CLAIM_TYPE, [claimRecord('live-orch', 1, LIVE_IDENTITY)]),
    ]);

    expect(locked).toEqual(['dibs']);

    // And the lock it did take did the work: the ghost is gone, the live
    // claims are untouched.
    const { byId, claimsOf } = await readLedger();
    expect(byId('ghost-orch')).toBeUndefined();
    expect(claimantIds(claimsOf(LIVE_AGENT_CLAIM_TYPE))).toContain('live-orch');
    expect(claimantIds(claimsOf(LIVE_AGENT_ADMISSION_CLAIM_TYPE))).toContain('live-orch');
  }, 30_000);

  it('takes NO lock at all on a beat where every record is alive', async () => {
    // The common case, and the one the pre-pass exists for: three ledgers, no
    // candidate anywhere, therefore zero critical sections.
    const locked = await runSweepAndReadLockLog('no-lock-orch', [
      ceilingEntry(),
      ghostDibs('live-dibs-orch', 1, LIVE_IDENTITY),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [claimRecord('live-orch', 1, LIVE_IDENTITY)]),
      claimLedger(LIVE_AGENT_ADMISSION_CLAIM_TYPE, [claimRecord('live-orch', 1, LIVE_IDENTITY)]),
    ]);

    expect(locked).toEqual([]);

    // The beat really ran a sweep-bearing path — it wrote its own dibs record
    // — so the empty log is "nothing looked dead", not "no beat happened".
    expect((await readLedger()).byId('no-lock-orch').pid).toBe(FAKE_CLAUDE_ROOT_PID);
    // Nothing was reaped, which is the outcome the pre-pass must not change.
    expect((await readLedger()).byId('live-dibs-orch')).toBeDefined();
  }, 30_000);

  it('ignores a leaked ARM_FAKE_SWEEP_LOCK_LOG when no master switch is set', async () => {
    // Same safety property `ARM_FAKE_PS_COLLECTION_LOG` carries:
    // `ARM_COORDINATION_FILE` leaks alongside the seam it would gate (see this
    // file's header), so it cannot be this seam's gate on its own — a leaked
    // var must not make a production
    // beat append to an arbitrary env-named path.
    const sweepLockLogPath = join(workDir, 'sweep-locks-leaked.log');
    await writeFile(sweepLockLogPath, '', 'utf8');

    await seedLedger([ceilingEntry(), ghostDibs('ghost-orch', 1)]);

    const beat = runCli(['--orchestrator-id=leaked-seam-orch', '--desired-agents=1'], {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_QUEUE_FILE: queueFilePath,
      ARM_FAKE_COLLECT_JSON: FAKE_COLLECT_JSON,
      // No ARM_BEAT_SNAPSHOT_TEST_MODE and no ARM_EVICT_TEST_MODE.
      ARM_FAKE_SWEEP_LOCK_LOG: sweepLockLogPath,
    });
    expect(beat.status).toBe(0);

    expect(await readFile(sweepLockLogPath, 'utf8')).toBe('');
  }, 30_000);
});

describe('the beat snapshot honours its ARM_FAKE_PS_* seams only behind ARM_BEAT_SNAPSHOT_TEST_MODE', () => {
  it('decides the sweep and the recorded identity from the REAL process tree when the switch is unset', async () => {
    // The whole-ticket hard AC — never reap a live agent — against the
    // leaked/stale-var situation the two-layer model exists for. This snapshot
    // is the sweep's ONLY liveness oracle, so whatever decides its contents
    // decides which records the beat DELETES from the SHARED coordination
    // file. `ARM_COORDINATION_FILE` cannot serve as that gate: every real
    // invocation sets it (`resolveNow`'s header says so outright).
    //
    // THE DISCRIMINATOR, stated because its direction is the counter-intuitive
    // one. The seeded record is alive ACCORDING TO THE FAKE TREE ONLY:
    // `FAKE_CLAUDE_ROOT_PID` is a synthetic pid no real claude-rooted process
    // carries, paired with the exact `startedAt` that tree's `ANCESTOR_ETIME`
    // row yields against the real clock. So
    //   * leak HONOURED (the defect) -> classified `alive`  -> SURVIVES;
    //   * leak IGNORED  (the fix)    -> absent from the real tree, therefore
    //                                   `confirmed-dead`    -> REAPED.
    // Reaping is the evidence that the real `ps` decided it. The mirror test
    // below runs the same record with the switch ON and pins the opposite
    // outcome, so neither test can pass on a fixture that was harmless anyway.
    const seedNow = Date.now();
    await seedLedger([
      ceilingEntry(),
      {
        orchestratorId: 'fake-tree-only-orch',
        desiredAgents: 1,
        declaredAt: seedNow,
        firstDeclaredAt: seedNow,
        pid: FAKE_CLAUDE_ROOT_PID,
        // `now - parseEtimeToSeconds(ANCESTOR_ETIME) * 1000` — the value the
        // fake tree's own row derives, and well inside
        // `PID_IDENTITY_TOLERANCE_MS` of what a beat milliseconds later would.
        pidStartedAt: seedNow - 60 * 60 * 1000,
      },
    ]);

    // THE POINT OF THIS TEST: the master switch is OFF, as it is for every
    // real orchestrator invocation, while the fake snapshot seam (from
    // `baseEnv`) is set. The real clock is used too, so the fake tree's etime
    // and the seeded `pidStartedAt` agree under whichever snapshot wins — the
    // outcome turns on the snapshot alone.
    const leakedBeat = cli(['--orchestrator-id=leak-probe-orch', '--desired-agents=1'], {
      ARM_BEAT_SNAPSHOT_TEST_MODE: undefined,
      ARM_FAKE_NOW_MS: undefined,
      ARM_BEAT_NOW_TEST_MODE: '1',
    });
    expect(leakedBeat.status).toBe(0);

    const { byId } = await readLedger();
    // widens the honest outcome from one shape to two. On a host that
    // resolves a claude-rooted ancestor (most local dev sessions) the real
    // tree condemns the record outright — REAPED. On a host that resolves
    // NONE at all (every CI runner: Linux `ps -o comm` never emits a
    // path-shaped value, so `AGENT_ROOT_COMM_PATTERN` can never match, so
    // `identity` is always `null` and `liveProcesses` is always `[]` there),
    // the drift guard correctly refuses to trust that unvouched empty
    // snapshot and DEFERS the record instead, loudly, via
    // `AGENT_RECOGNITION_DRIFT_SUSPECTED`. Both are proof the real table
    // drove the decision. What must never happen under an honoured seam is a
    // silent third outcome: the record kept with no signal at all — which is
    // exactly what leak-HONOURED (fake tree says `alive`) would produce,
    // since the fake tree's synthetic data never triggers the drift guard
    // (it resolves a non-null identity and a non-empty `liveProcesses`).
    const survived = byId('fake-tree-only-orch') !== undefined;
    expect(!survived || leakedBeat.stderr.includes('AGENT_RECOGNITION_DRIFT_SUSPECTED')).toBe(true);

    // The identity half of the same property, mirroring
    // ./cli-evict.jest.spec.mjs's own leaked-var test: the pid this beat
    // stamped came from the real tree — or, on a host running no claude-rooted
    // ancestor, from nothing at all — never from the leaked fake tree, which is
    // built so that honouring it yields `FAKE_CLAUDE_ROOT_PID` here.
    expect(byId('leak-probe-orch').pid).not.toBe(FAKE_CLAUDE_ROOT_PID);
  }, 30_000);

  it('honours the identical seam and record once the master switch IS set — gated, not removed', async () => {
    // The other direction, and the contract the rest of this file rests on:
    // setting `ARM_FAKE_PS_OUTPUT` must not, by itself, disable the sweep or
    // identity resolution. Same record as above, alive per the fake tree, under
    // the frozen clock and snapshot `baseEnv` supplies.
    await seedLedger([
      ceilingEntry(),
      ghostDibs('fake-tree-only-orch', 1, { pid: FAKE_CLAUDE_ROOT_PID, pidStartedAt: EXPECTED_PID_STARTED_AT }),
      ghostDibs('ghost-orch', 1),
    ]);

    const beat = cli(['--orchestrator-id=gated-seam-orch', '--desired-agents=1']);
    expect(beat.status).toBe(0);

    const { byId } = await readLedger();
    // Alive per the honoured snapshot — kept.
    expect(byId('fake-tree-only-orch')).toBeDefined();
    // …and the sweep genuinely ran against that same snapshot rather than
    // being switched off by it.
    expect(byId('ghost-orch')).toBeUndefined();
    expect(byId('gated-seam-orch').pid).toBe(FAKE_CLAUDE_ROOT_PID);
  }, 30_000);

  // -------------------------------------------------------------------------
  // The CLOCK ANCHOR half of Layer 1, which the two tests above do not reach.
  //
  // `observeBeatProcesses` threads the SAME `testModeEnabled` value into
  // `collectBeatPsSnapshot` AND `resolveProcessSnapshotNow`, so the instant the
  // snapshot's `etime` column is measured against always belongs to the clock
  // the snapshot actually used. Drop the argument — let
  // `resolveProcessSnapshotNow` fall back to its `honorFakeSeam = true` default
  // — and a beat with the master switch UNSET collects the REAL `ps` but
  // anchors it to the FAKE clock, because that default still consults
  // `ARM_FAKE_PS_OUTPUT` directly.
  //
  // That is not a test-only configuration. `ARM_FAKE_NOW_MS` is honoured in
  // production: its only gate is `ARM_COORDINATION_FILE`, which every real
  // invocation sets (`resolveNow`'s own header says so). So a leaked
  // `ARM_FAKE_PS_OUTPUT` and a leaked `ARM_FAKE_NOW_MS` together — a stale
  // shell profile, an inherited CI env — would give every derived `startedAt`
  // an offset the size of the clock skew, miss `PID_IDENTITY_TOLERANCE_MS` on
  // every genuinely live pid, and reap the whole fleet as pid reuse. The
  // whole-ticket hard AC is "never reap a live agent"; nothing above pins this
  // half of it, because both tests above leave `ARM_FAKE_NOW_MS` unset.
  //
  // A SYNTHETIC PID CANNOT PIN THIS. The defect only bites a record whose pid
  // is genuinely present in the real snapshot: an absent pid is `pid-absent`
  // under either anchor, so the anchor makes no difference to it. The fixture
  // therefore starts a REAL, live, claude-rooted process on this host and
  // seeds the ledger with the identity the production code itself derives for
  // it.
  // -------------------------------------------------------------------------

  /**
   * How far the leaked fake clock is skewed from the real one — nearly two
   * orders of magnitude beyond `PID_IDENTITY_TOLERANCE_MS`, so the defect this
   * test pins still moves
   * every row's derived `startedAt` far outside tolerance and reaps the live
   * record.
   *
   * BOUNDED ABOVE, not merely "large". The seeded records have to satisfy two
   * windows at once: young enough under the LEAKED clock to survive
   * `declareDibs`' 15-minute liveness pruning, and stamped at or before the
   * REAL snapshot so the sweep's snapshot-freshness guard
   * (`isWrittenAfterSnapshot`, cli.mjs) does not defer them as records the
   * snapshot predates. A skew at or beyond the liveness threshold makes those
   * two windows disjoint and leaves no stamp that satisfies both — the guard
   * would then spare BOTH records, including the ghost, and the
   * `ghost-orch` non-vacuity control below would fail. The test cannot be made
   * to discriminate anything at such a skew, at any stamp.
   */
  const LEAKED_CLOCK_SKEW_MS = 5 * 60 * 1000;

  /**
   * Asks THIS host's real `ps` whether it prints the `comm` column as a
   * RESOLVED EXECUTABLE PATH — the one host capability the live-process
   * fixture below cannot work without.
   *
   * WHY THIS GATE EXISTS, AND WHY IT IS NOT LAZINESS. Production's
   * `AGENT_ROOT_COMM_PATTERN` (`/\/claude$/`, lib/recon.mjs) requires a tree
   * root's `comm` to END IN `/claude`, i.e. to be a path. That is satisfiable
   * only where `ps -o comm` emits paths:
   *
   *   - macOS/BSD `ps -o comm` prints the resolved executable path
   *     (`/usr/local/bin/node`), so a file literally named `claude` yields a
   *     `comm` ending in `/claude`. ARM is a macOS-host tool by design
   *     (this design's admission signals are macOS-specific), so this is the host
   *     the assertion is actually about.
   *   - Linux/procps `ps -o comm` prints only the executable BASENAME,
   *     truncated to 15 characters. It never STARTS with a `/` (kernel
   *     threads like `kworker/0:1` do contain one — see the predicate note
   *     below), so NO process on such a host can match the production
   *     pattern, `listAgentProcesses` emits no entry for the spawned child,
   *     and the fixture's poll below exhausts. (Demonstrated portably by the
   *     grammar test that follows this block, which runs everywhere.)
   *
   * The assertion is therefore inherently host-shaped: it needs a genuinely
   * live process that production discovery genuinely recognises, and a
   * synthetic pid cannot substitute for THIS route into the defect (an absent
   * pid is `pid-absent` under either clock anchor, so it cannot discriminate
   * the anchor via liveness classification). The OTHER route into the same
   * defect — the sweep's `isWrittenAfterSnapshot` boundary — needs no live
   * process and IS portable; it is covered unconditionally further down this
   * file, so the regression this test guards is not CI-invisible. See
   * for the residual gap and the (owner-owned, cost-bearing) macOS-CI
   * proposal.
   *
   * Widening the production pattern to `/(^|\/)claude$/` would make this test
   * portable at the cost of changing shipped agent-discovery semantics on the
   * real host: any process merely NAMED `claude` would become an agent tree
   * root. That widening is pinned against by the bare-basename test in
   * lib/recon.jest.spec.mjs, added alongside this gate precisely because
   * nothing pinned it before — the widened pattern passed every pre-existing
   * test in lib/recon.jest.spec.mjs and lib/recon-per-agent.jest.spec.mjs
   * (61 of 61) unchanged.
   *
   * Probed from REAL `ps` output rather than from `process.platform`, so the
   * gate tracks the capability the fixture actually depends on rather than a
   * proxy for it. THE SAME BINARY, TOO: this shells out to `PS_BIN`, the
   * absolute path production uses, not to whatever `$PATH` resolves `ps` to.
   * Probing a different binary from the one whose output the assertion is
   * about would make the gate a proxy again — a worse one, because it would
   * answer confidently about a `ps` that never runs in production.
   *
   * THE POPULATION, NOT THIS PROCESS. The probe asks what share of the whole
   * table's `comm` values are path-shaped, deliberately rather than looking at
   * this test process's own row. Measured inside a Jest worker on the macOS
   * dev host: `process.title` is `node` and `ps -o comm` reports `node` to
   * match, while 651 of the same table's 657 rows carry a path. (macOS `ps`
   * reports a process's title once it has one; a forked Node worker's title
   * is the bare `node`, not its execPath.) A self-row probe therefore reports
   * "basename host" ON THE VERY HOST THE TEST IS MEANT TO RUN ON. The
   * fixture's child leaves its title alone, so the population share is both
   * the honest question and the one that predicts it.
   *
   * THE PREDICATE IS `startsWith('/')`, NOT `includes('/')`, AND THAT
   * DISTINCTION IS THE WHOLE GATE. `includes('/')` looks equivalent and is
   * not: LINUX KERNEL THREADS ARE LEGITIMATELY NAMED WITH SLASHES —
   * `kworker/0:1`, `kworker/u8:2`, `ksoftirqd/0`, `migration/0`, `cpuhp/0`,
   * `idle_inject/0`, `irq/24-pciehp`, `jbd2/sda1-8` — and not one of them is
   * a path. On a GitHub Actions ubuntu runner (a VM with a per-CPU kernel
   * thread population and very little userspace) those threads are the
   * MAJORITY of the process table, so `includes('/')` cleared the
   * majority threshold, the probe reported SUPPORTED on the very host this
   * gate exists to skip, and the fixture below threw because production's
   * `/\/claude$/` can never match a procps basename. That was the CI failure
   * on run 33232002395. An ABSOLUTE PATH is the property the pattern actually
   * needs, and it is the property kernel threads do not have: `kworker/0:1`
   * does not start with `/`; `/usr/bin/node` does. Measured on the macOS dev
   * host, 651 of 657 rows start with `/` — the same 651 that contain one.
   *
   * Split into a pure classifier over a supplied table plus a thin shell-out,
   * so the Linux shape can be asserted directly rather than only inferred on
   * a host no developer runs. Three prior reviews "simulated Linux" with
   * hand-written basename-only tables containing no kernel threads, and every
   * one of them missed this.
   *
   * @param {string} stdout raw `ps -Ao pid,ppid,rss,etime,comm` output
   * @returns {{ supported: boolean, reason: string }}
   */
  function classifyCommColumnPathShape(stdout) {
    // Columns are pid, ppid, rss, etime, comm — everything from the 5th field
    // on is the `comm` value (which may itself contain spaces on macOS).
    const comms = String(stdout ?? '')
      .split('\n')
      .map((line) => line.trim())
      .filter((line) => /^\d+\s/.test(line))
      .map((line) => line.split(/\s+/).slice(4).join(' '))
      .filter(Boolean);

    if (comms.length === 0) {
      return { supported: false, reason: 'real `ps` produced no parseable process rows to probe' };
    }

    const pathShaped = comms.filter((comm) => comm.startsWith('/')).length;
    if (pathShaped <= comms.length / 2) {
      return {
        supported: false,
        reason:
          `fewer than half of this host's \`ps\` rows carry an absolute-path \`comm\` (${pathShaped} of ` +
          `${comms.length}), so a process here is not reliably reported by its resolved path and cannot ` +
          'match AGENT_ROOT_COMM_PATTERN (/\\/claude$/). A slash alone does not make a path: Linux ' +
          'kernel threads such as `kworker/0:1` and `ksoftirqd/0` carry one and are still basenames.',
      };
    }

    return { supported: true, reason: '' };
  }

  /** @returns {{ supported: boolean, reason: string }} */
  function probeCommColumnEmitsPaths() {
    const probe = spawnSync(PS_BIN, PS_ARGS, { encoding: 'utf8' });
    if (probe.status !== 0) {
      // `spawnSync` reports a missing/unspawnable `ps` as `status: null` plus
      // an `error`, so the message is the informative half in exactly the case
      // a bare status code says nothing.
      return {
        supported: false,
        reason: `real \`ps\` (${PS_BIN}) could not be run on this host (${probe.error?.message ?? `exit ${probe.status}`})`,
      };
    }

    return classifyCommColumnPathShape(probe.stdout);
  }

  const COMM_PATH_SUPPORT = probeCommColumnEmitsPaths();

  // THE GATE MUST NOT BE ABLE TO DISARM ITSELF ON THE HOST IT EXISTS FOR.
  // macOS is the only host where this test is expected to run at all, so a
  // `darwin` host reporting "unsupported" is not a legitimate skip — it is a
  // broken probe, a broken `ps`, or a `ps` whose output shape has drifted. Any
  // of those is a defect to be seen, and a skip would hide all three behind
  // the same green tick the basename-host skip produces. Failing loudly here
  // is the difference between "this assertion does not apply to this host" and
  // "this assertion silently stopped applying anywhere".
  //
  // This cannot itself fire spuriously on a healthy macOS host: it takes a
  // darwin host on which FEWER THAN HALF of all `ps` rows report an
  // absolute-path `comm` (measured on the dev host: 651 of 657 do), or on which `ps` cannot
  // be spawned at all. Both are genuine faults.
  if (!COMM_PATH_SUPPORT.supported && process.platform === 'darwin') {
    throw new Error(
      '[ ] The clock-anchor gate reported UNSUPPORTED on a darwin host, where it must always be ' +
        `supported. This is a fault, not a skippable host. Probe said: ${COMM_PATH_SUPPORT.reason}. ` +
        'See probeCommColumnEmitsPaths() for the rationale.',
    );
  }

  if (!COMM_PATH_SUPPORT.supported) {
    // Loud on purpose: a silently-skipped test is indistinguishable from a
    // passing one in a CI log.
    // eslint-disable-next-line no-console
    console.warn(
      '[ ] SKIPPING the real-clock-anchor test (the live-process half of the whole-ticket hard ' +
        'AC: a beat with ARM_BEAT_SNAPSHOT_TEST_MODE unset must anchor the REAL snapshot to the ' +
        `REAL clock). Reason: ${COMM_PATH_SUPPORT.reason}. This assertion needs a live process that ` +
        'production agent discovery recognises, which only a path-shaped `comm` can provide; it runs ' +
        'on the macOS host ARM targets. The SAME defect is still covered here by the portable ' +
        '`isWrittenAfterSnapshot` boundary test, which does run on this host. Residual gap and the ' +
        'macOS-CI proposal are tracked separately; see probeCommColumnEmitsPaths() for the rationale.',
    );
  }

  const itWhenCommIsPathShaped = COMM_PATH_SUPPORT.supported ? it : it.skip;

  const CLOCK_ANCHOR_TITLE =
    'anchors the real snapshot to the real clock when the switch is unset, so a leaked fake clock ' +
    'cannot reap a genuinely live agent' +
    (COMM_PATH_SUPPORT.supported ? '' : ` [SKIPPED on this host — see : ${COMM_PATH_SUPPORT.reason}]`);

  it('classifies a REALISTIC Linux `ps` table as unsupported even though most of its `comm` values contain a slash', () => {
    // The table that broke CI run 33232002395. A GitHub Actions ubuntu runner
    // is a VM whose process table is DOMINATED by per-CPU kernel threads, and
    // procps names those with slashes — so the previous `includes('/')`
    // predicate saw a majority of "path-shaped" rows and opened the gate on
    // the one host it exists to close. Every prior hand-written "Linux
    // simulation" in review omitted kernel threads entirely, which is exactly
    // why none of them reproduced it. Kept deliberately kernel-thread-heavy.
    const header = '    PID   PPID    RSS     ELAPSED COMMAND';
    const linuxKernelThreadHeavy = [
      header,
      '      1      0  12000    01:20:14 systemd',
      '      2      0      0    01:20:14 kthreadd',
      '      3      2      0    01:20:14 rcu_gp',
      '     11      2      0    01:20:14 migration/0',
      '     12      2      0    01:20:14 ksoftirqd/0',
      '     13      2      0    01:20:14 kworker/0:0H',
      '     14      2      0    01:20:14 cpuhp/0',
      '     15      2      0    01:20:14 idle_inject/0',
      '     19      2      0    01:20:14 migration/1',
      '     20      2      0    01:20:14 ksoftirqd/1',
      '     21      2      0    01:20:14 kworker/1:0H',
      '     22      2      0    01:20:14 cpuhp/1',
      '     23      2      0    01:20:14 idle_inject/1',
      '     31      2      0    01:20:14 kworker/u8:0',
      '     32      2      0    01:20:14 kworker/u8:1',
      '     44      2      0    01:20:13 irq/24-pciehp',
      '     45      2      0    01:20:13 irq/25-pciehp',
      '    201      2      0    01:20:12 jbd2/sda1-8',
      '    640      1   9000    01:19:58 sshd',
      '   1902    640  48000    00:03:11 bash',
      '   1955   1902 380000    00:02:47 node',
      '   1990   1955  12000    00:00:01 ps',
    ].join('\n');

    // Kernel threads are the majority and they all carry a slash — the
    // condition the old `includes('/')` predicate could not survive. Asserted
    // rather than asserted-in-a-comment, so a later edit that quietly drops
    // the kernel threads fails here instead of silently defanging the test.
    const processRows = linuxKernelThreadHeavy.split('\n').filter((line) => /^\s*\d+\s/.test(line));
    const slashBearing = processRows.filter((line) => line.includes('/'));
    expect(slashBearing.length).toBeGreaterThan(processRows.length / 2);

    // …and yet not one of them is an absolute path, so this host cannot host
    // the gated fixture and the probe must say so.
    expect(classifyCommColumnPathShape(linuxKernelThreadHeavy)).toEqual({
      supported: false,
      reason: expect.stringContaining('fewer than half'),
    });

    // The macOS shape — resolved executable paths — must still open the gate,
    // or the gate would be permanently closed and assert nothing anywhere.
    const macosShaped = [
      header,
      '      1      0  12000    01:20:14 /sbin/launchd',
      '    301      1  40000    01:19:58 /usr/libexec/opendirectoryd',
      '    640      1   9000    01:19:58 /usr/sbin/sshd',
      '   1902    640  48000    00:03:11 /bin/zsh',
      '   1955   1902 380000    00:02:47 /usr/local/bin/node',
      // A forked Jest worker reports the bare title `node` — a minority row
      // the population share is designed to tolerate.
      '   1961   1955 210000    00:02:40 node',
    ].join('\n');
    expect(classifyCommColumnPathShape(macosShaped)).toEqual({ supported: true, reason: '' });

    // Degenerate input is unsupported rather than a divide-by-zero "true".
    expect(classifyCommColumnPathShape('').supported).toBe(false);
  });

  it('pins the host capability the clock-anchor gate turns on: only a path-shaped `comm` yields an agent root', () => {
    // Runs EVERYWHERE, including the hosts where the test below is skipped, so
    // the gate's premise is an assertion rather than a claim in a comment.
    // Same column grammar (`pid ppid rss etime comm`) the beat's real snapshot
    // uses, differing only in the `comm` format the two `ps` families emit.
    const header = '  PID  PPID   RSS     ELAPSED COMM';
    const linuxShaped = [header, '    1     0  1000    10:00:00 systemd', '  900     1 20000    00:05:00 claude'].join(
      '\n',
    );
    const macosShaped = [
      header,
      '    1     0  1000    10:00:00 /sbin/launchd',
      '  900     1 20000    00:05:00 /tmp/arm-real-claude-x/claude',
    ].join('\n');

    // Both are perfectly legible process tables — the difference is not
    // degradation, it is the `comm` format…
    expect(classifyProcessSnapshotShape(linuxShaped)).toBe('readable');
    expect(classifyProcessSnapshotShape(macosShaped)).toBe('readable');

    // …and only the path-shaped one can produce an agent tree root, which is
    // exactly why the fixture below cannot observe its own live process on a
    // basename-only host.
    expect(listAgentProcesses(linuxShaped, 0)).toEqual([]);
    expect(listAgentProcesses(macosShaped, 0)).toEqual([
      expect.objectContaining({ agentId: '900', startedAt: -300_000 }),
    ]);
  });

  /**
   * Starts a genuinely live process that `AGENT_ROOT_COMM_PATTERN`
   * (`/\/claude$/` on the `comm` column — ends in `/claude`, a PATH, not
   * merely in `claude`) recognises as an agent tree root, and answers the
   * identity the production discovery path derives for it.
   *
   * `comm` is the RESOLVED executable path, so neither `argv[0]` nor a symlink
   * named `claude` can fake it — the file itself has to be named `claude`. A
   * copy of `process.execPath` is used because a copied macOS system binary is
   * SIGKILLed for an invalid code signature, while the Node binary's own
   * signature travels with its bytes.
   *
   * `startedAt` is not measured by this fixture: it is read back out of
   * `listAgentProcesses` — the same function `observeBeatProcesses` feeds — so
   * the seeded record carries the value the code under test derives, not a
   * value this test believes it should derive.
   */
  async function startLiveClaudeRootedProcess() {
    const binDir = await mkdtemp(join(tmpdir(), 'arm-real-claude-'));
    const binPath = join(binDir, 'claude');
    await copyFile(process.execPath, binPath);

    const child = spawn(binPath, ['-e', 'setTimeout(() => {}, 120000)'], { stdio: 'ignore' });
    child.unref();

    const stop = async () => {
      try {
        child.kill('SIGKILL');
      } catch {
        // Already gone — nothing to reclaim but the directory.
      }
      await rm(binDir, { recursive: true, force: true });
    };

    // `ps` does not necessarily show a just-spawned process, and its `etime`
    // must be parseable before `listAgentProcesses` will emit an entry at all.
    for (let attempt = 0; attempt < 40; attempt += 1) {
      // ORDER MATTERS, and it mirrors production: `observeBeatProcesses`
      // resolves its `snapshotAt` AFTER `collectBeatPsSnapshot` returns
      // (cli.mjs). Reading the clock BEFORE the shell-out would give the
      // seeded `startedAt` the opposite-sign latency error from the one the
      // CLI derives, so the two would agree only while `ps` stays fast — the
      // gated test would then pass on a quiet dev host and condemn its own
      // live process on a contended one. Same binary as production, too.
      const probe = spawnSync(PS_BIN, PS_ARGS, { encoding: 'utf8' });
      const now = Date.now();
      const entry = listAgentProcesses(probe.stdout ?? '', now).find(
        (candidate) => String(candidate.agentId) === String(child.pid),
      );
      if (entry !== undefined) return { pid: child.pid, startedAt: entry.startedAt, stop };
      await sleep(250);
    }

    await stop();
    // Reaching here on a host that PASSED `probeCommColumnEmitsPaths()` is a
    // genuine failure, not the known basename-`comm` host shape — that host
    // never gets this far, because the test is skipped before it runs.
    throw new Error(
      'fixture could not observe its own live claude-rooted process in `ps`, on a host whose `comm` ' +
        'column IS path-shaped — this is a real failure, not the basename-`comm` host limitation',
    );
  }

  itWhenCommIsPathShaped(CLOCK_ANCHOR_TITLE, async () => {
    const live = await startLiveClaudeRootedProcess();
    try {
      // The leak: a fake clock skewed ahead of the real one. Under the correct
      // wiring the beat's `now` still comes from it (that gate is
      // `ARM_COORDINATION_FILE`'s, unchanged), but the REAL snapshot it
      // collected is anchored to `Date.now()`, so the live process's derived
      // `startedAt` lands within tolerance of the seeded one and the record is
      // `alive`. Under the parameterless-default defect the same real snapshot
      // is anchored to the fake clock, every row's `startedAt` moves by the
      // skew, and this live agent is reaped as `pid-reuse`.
      const leakedNow = Date.now() + LEAKED_CLOCK_SKEW_MS;
      // Stamped against the REAL clock, in the past, so the sweep's
      // snapshot-freshness guard leaves both records decidable — see
      // `LEAKED_CLOCK_SKEW_MS` above for why both windows have to be satisfied
      // at once for this test to discriminate anything.
      const freshlyDeclaredAt = Date.now() - 60_000;

      await seedLedger([
        { ...ceilingEntry(), declaredAt: freshlyDeclaredAt },
        {
          orchestratorId: 'live-agent-orch',
          desiredAgents: 1,
          declaredAt: freshlyDeclaredAt,
          firstDeclaredAt: freshlyDeclaredAt,
          pid: live.pid,
          pidStartedAt: live.startedAt,
        },
        // Non-vacuity control: a pid the real host is not running. It must be
        // reaped, which is how this test distinguishes "the live record
        // survived because the sweep worked" from "the sweep never ran".
        {
          orchestratorId: 'ghost-orch',
          desiredAgents: 1,
          declaredAt: freshlyDeclaredAt,
          firstDeclaredAt: freshlyDeclaredAt,
          pid: GHOST_PID,
          pidStartedAt: leakedNow - 7_200_000,
        },
      ]);

      const beat = cli(['--orchestrator-id=clock-anchor-orch', '--desired-agents=1'], {
        // Layer 1 OFF — as in every real orchestrator invocation.
        ARM_BEAT_SNAPSHOT_TEST_MODE: undefined,
        // …while BOTH seams are leaked. `ARM_FAKE_PS_OUTPUT` comes from
        // `baseEnv`; the fake clock is skewed here.
        ARM_FAKE_NOW_MS: String(leakedNow),
        ARM_BEAT_NOW_TEST_MODE: '1',
      });
      expect(beat.status).toBe(0);

      const { byId } = await readLedger();
      // THE HARD AC. A live agent, correctly identified from the real tree,
      // survives a beat carrying both leaked seams.
      expect(byId('live-agent-orch')).toBeDefined();
      // …and the sweep it survived was a real sweep — reaped BY THE SWEEP,
      // which says so by name, rather than aged out by liveness pruning.
      expect(byId('ghost-orch')).toBeUndefined();
      expect(beat.stderr).toContain(`reaped dibs record for ghost-orch (pid ${GHOST_PID}): ${LIVENESS_REASON.PID_ABSENT}`);
    } finally {
      await live.stop();
    }
  }, 60_000);

  it('anchors the real snapshot to the real clock when the switch is unset, judged at the write-stamp boundary — portable', async () => {
    // THE SAME DEFECT AS THE GATED TEST ABOVE, reached through the sweep's
    // OTHER consumer of the snapshot instant, and therefore WITHOUT needing a
    // live claude-rooted process. This one runs on every host, including the
    // ones where the gated test is skipped.
    //
    // `observeBeatProcesses` publishes its anchor as `observation.snapshotAt`
    // (cli.mjs), and the sweep feeds that value to BOTH
    // `classifyLedgerEntryLiveness` (which needs a live pid to discriminate —
    // hence the gated test) AND `isWrittenAfterSnapshot`, whose whole judgement
    // is `record.declaredAt >= snapshotAt`. That comparison moves with the
    // anchor for an ABSENT pid too, which is what makes it portable:
    //
    //   - correctly anchored: `snapshotAt` is the REAL clock, the record's
    //     stamp is in the future relative to it, so the record is one the
    //     snapshot predates — DEFERRED, and kept, exactly as a record written
    //     by a concurrent beat during the sweep's lock wait must be.
    //   - under the parameterless-default defect: `snapshotAt` is the LEAKED
    //     clock, the same stamp now sits in the snapshot's past, the deferral
    //     never fires, and the record is reaped as `pid-absent`.
    //
    // The stamp is placed strictly BETWEEN the two clocks so that only the
    // anchor can move the verdict. Margins are minutes on both sides, so the
    // sub-second drift between this line and the CLI's own `Date.now()` cannot
    // reach either boundary.
    const realNow = Date.now();
    const leakedNow = realNow + LEAKED_CLOCK_SKEW_MS; // real + 5m
    const stampedBetweenTheClocks = realNow + 60_000; // real + 1m, leaked - 4m

    await seedLedger([
      { ...ceilingEntry(), declaredAt: realNow - 60_000 },
      {
        orchestratorId: 'straddling-orch',
        desiredAgents: 1,
        declaredAt: stampedBetweenTheClocks,
        firstDeclaredAt: stampedBetweenTheClocks,
        pid: GHOST_PID,
        pidStartedAt: leakedNow - 7_200_000,
      },
      // Non-vacuity control: same absent pid, same beat, but stamped in the
      // REAL past — before either candidate anchor. No anchor can defer it, so
      // it must be reaped. That is what separates "the straddling record
      // survived because the deferral fired" from "the sweep never ran".
      {
        orchestratorId: 'ghost-orch',
        desiredAgents: 1,
        declaredAt: realNow - 60_000,
        firstDeclaredAt: realNow - 60_000,
        pid: GHOST_PID,
        pidStartedAt: leakedNow - 7_200_000,
      },
    ]);

    const beat = cli(['--orchestrator-id=portable-clock-anchor-orch', '--desired-agents=1'], {
      // Layer 1 OFF, both seams leaked — the production-shaped leak.
      ARM_BEAT_SNAPSHOT_TEST_MODE: undefined,
      ARM_FAKE_NOW_MS: String(leakedNow),
      ARM_BEAT_NOW_TEST_MODE: '1',
    });
    expect(beat.status).toBe(0);

    const { byId } = await readLedger();

    // ADDS A FOURTH OUTCOME THIS TEST MUST NOT CONFLATE WITH THE THIRD.
    // Both seeded records share one ledger (dibs), and the drift guard's
    // pre-pass is PER-LEDGER, not per-record: on a host that resolves no
    // claude-rooted identity AND sees zero live agent processes (every CI
    // runner — Linux `ps -o comm` never emits a path `AGENT_ROOT_COMM_PATTERN`
    // can match, so `identity` is always `null` and `liveProcesses` is always
    // `[]` there), `ghost-orch` alone is enough to make the pre-pass find a
    // would-be-confirmed-dead candidate, which suppresses the WHOLE ledger's
    // reap for this beat — `straddling-orch` included. On such a host the
    // clock anchor is never even consulted: nothing here can tell "the anchor
    // deferred it" apart from "the guard deferred it", because the guard fires
    // first and short-circuits before either record is individually
    // classified. That is a real, accepted narrowing of what this specific
    // assertion can see in that environment (documented residual limitation 1),
    // not a bug — the property itself is still exercised wherever the guard is
    // inert, which is the common case this repo's tests actually run in
    // locally.
    //
    // The guard's own firing is legitimate evidence in its own right, exactly
    // as in the sibling tests: it can only fire off REAL, empty data — a
    // leak-honoured fake tree resolves a non-null identity and a non-empty
    // `liveProcesses`, so it can never trigger this branch.
    const driftGuardFired = beat.stderr.includes('AGENT_RECOGNITION_DRIFT_SUSPECTED');

    if (driftGuardFired) {
      // The guard is on trial here, not the anchor: both records must be
      // deferred (kept) rather than either one being silently or wrongly
      // reaped off an unvouched snapshot.
      expect(byId('straddling-orch')).toBeDefined();
      expect(byId('ghost-orch')).toBeDefined();
    } else {
      // The guard is inert on this host (a claude-rooted identity resolved,
      // or some other live agent process did), so the write-stamp boundary
      // comparison is genuinely what decided each record, and
      // THE ANCHOR ASSERTION holds: only a REAL-clock `snapshotAt` leaves
      // `straddling-orch` in the snapshot's future, and only that defers it.
      expect(byId('straddling-orch')).toBeDefined();
      // …and the sweep it survived was a real sweep.
      expect(byId('ghost-orch')).toBeUndefined();
      expect(beat.stderr).toContain(
        `reaped dibs record for ghost-orch (pid ${GHOST_PID}): ${LIVENESS_REASON.PID_ABSENT}`,
      );
    }
  }, 30_000);

  it('ignores a leaked ARM_FAKE_PS_FAILURE when the switch is unset: the snapshot is real, so the sweep still runs', async () => {
    // Seam 2 of the three the switch governs. Its regression is fail-safe on
    // the reaping side — a failed collection yields `liveProcesses: null`,
    // every record falls to `unknown`, nothing is reaped — but it silently
    // disables reconciliation AND identity-writing for the whole fleet, which
    // is why "gated" has to be an assertion rather than a comment.
    await seedLedger([ceilingEntry(), ghostDibs('ghost-orch', 1)]);

    const beat = cli(['--orchestrator-id=failure-seam-orch', '--desired-agents=1'], {
      ARM_BEAT_SNAPSHOT_TEST_MODE: undefined,
      ARM_FAKE_PS_FAILURE: '1',
    });
    expect(beat.status).toBe(0);

    // A real snapshot was taken: no degradation was reported. This alone
    // already discriminates the leak: honouring `ARM_FAKE_PS_FAILURE` reports
    // exactly this code, regardless of anything the guard does further
    // down.
    expect(beat.stderr).not.toContain('PROCESS_SNAPSHOT_UNAVAILABLE');

    // …and the real snapshot was used to actually classify the record.
    // `GHOST_PID` is carried by no process on this host, so a real snapshot —
    // whatever it contains — makes the record `confirmed-dead`. Under the
    // leak it would be `unknown` (liveProcesses: null) and kept with no
    // warning at all.
    //
    // widens the honest outcome here too: a host that resolves no
    // claude-rooted identity and sees zero live agent processes (every CI
    // runner — see the sibling tests in this file for why) correctly DEFERS
    // the confirmed-dead record instead of reaping it, loudly, via
    // `AGENT_RECOGNITION_DRIFT_SUSPECTED`, rather than trusting an unvouched
    // empty snapshot. Either REAPED or drift-DEFERRED is proof the real
    // (non-failed) snapshot drove the decision; kept silently, with neither
    // signal, is the only shape a leak could produce and is what the
    // assertion below still refuses.
    const ghostSurvived = (await readLedger()).byId('ghost-orch') !== undefined;
    expect(!ghostSurvived || beat.stderr.includes('AGENT_RECOGNITION_DRIFT_SUSPECTED')).toBe(true);
  }, 30_000);

  it('ignores a leaked ARM_FAKE_PS_DELAY_MS when the switch is unset: the beat does not wait out a fake slow `ps`', async () => {
    // Seam 3 of the three. Latency-only, so its own regression is a stalled
    // beat rather than a wrong reap — but it is enumerated as governed by the
    // switch, and nothing distinguished "governed" from "not" for it.
    const LEAKED_DELAY_MS = 5_000;
    await seedLedger([ceilingEntry()]);

    const startedAt = Date.now();
    const beat = cli(['--orchestrator-id=delay-seam-orch', '--desired-agents=1'], {
      ARM_BEAT_SNAPSHOT_TEST_MODE: undefined,
      ARM_FAKE_PS_DELAY_MS: String(LEAKED_DELAY_MS),
    });
    const elapsed = Date.now() - startedAt;

    expect(beat.status).toBe(0);
    // Generous over a cold `node` start plus a real `ps`, far below the leaked
    // delay a honoured seam would impose.
    expect(elapsed).toBeLessThan(3_000);
    // …and the beat genuinely completed its work, so the timing above is not
    // the timing of a beat that bailed out early.
    expect((await readLedger()).byId('delay-seam-orch')).toBeDefined();
  }, 30_000);

  // -------------------------------------------------------------------------
  // The two seams that are NOT part of the three-seam Layer 2 enumeration but
  // are reached from the same beat, and were shipped gated on
  // `ARM_COORDINATION_FILE` alone — the gate this file's own header states is
  // near-inert, since it leaks together with the seams it would gate. Each is
  // asserted
  // inert in a PRODUCTION-SHAPED env: real `ps`, no master switch, no
  // `ARM_FAKE_PS_*`, only `ARM_COORDINATION_FILE` and the seam's own variable.
  // -------------------------------------------------------------------------

  it('ignores a leaked ARM_FAKE_SWEEP_WRITE_FAILURES_JSON when the switch is unset: the sweep runs and reports no fabricated lock failure', async () => {
    // The worst of the ungated seams. Honoured in production it would both
    // silently disable ghost reaping for the named ledger AND emit a
    // COORDINATION_LOCK_TIMEOUT diagnosis for a lock problem that never
    // happened, sending an operator after a fault that does not exist.
    await seedLedger([ceilingEntry(), ghostDibs('ghost-orch', 1)]);

    const beat = cli(['--orchestrator-id=sweep-failure-seam-orch', '--desired-agents=1'], {
      ARM_BEAT_SNAPSHOT_TEST_MODE: undefined,
      ARM_FAKE_PS_OUTPUT: undefined,
      ARM_FAKE_NOW_MS: undefined,
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_SWEEP_WRITE_FAILURES_JSON: JSON.stringify({ dibs: 'COORDINATION_LOCK_TIMEOUT' }),
    });
    expect(beat.status).toBe(0);

    // No fabricated diagnosis — this alone already discriminates the leak.
    // Honouring `ARM_FAKE_SWEEP_WRITE_FAILURES_JSON` would only ever surface
    // once the sweep actually reaches `reapConfirmedDeadRecords`'
    // `beforeCommit` hook, which is unaffected by anything the guard
    // does (the guard, when it fires, `continue`s BEFORE that call is ever
    // reached — see cli.mjs's `reconcileLedgers`), so these two assertions
    // hold in every environment regardless of which branch this beat
    // takes.
    expect(beat.stderr).not.toContain('COORDINATION_LOCK_TIMEOUT');
    expect(beat.stderr).not.toContain('left unswept');

    // …and the sweep it would have abandoned genuinely ran against real data.
    // `GHOST_PID` is carried by no process on this host, so a real snapshot —
    // whatever it contains — condemns it.
    //
    // widens the honest outcome: a host that resolves no claude-rooted
    // identity and sees zero live agent processes (every CI runner) correctly
    // DEFERS the confirmed-dead candidate for this whole ledger instead of
    // reaping it, loudly, via `AGENT_RECOGNITION_DRIFT_SUSPECTED`. Either
    // REAPED or drift-DEFERRED is proof the real table drove the decision;
    // kept silently with neither signal is the only shape a leak/dishonoured
    // seam could produce.
    const ghostSurvived = (await readLedger()).byId('ghost-orch') !== undefined;
    expect(!ghostSurvived || beat.stderr.includes('AGENT_RECOGNITION_DRIFT_SUSPECTED')).toBe(true);
  }, 30_000);

  it('ignores a leaked ARM_FAKE_PS_COLLECTION_LOG when the switch is unset: no env-directed write to an arbitrary path', async () => {
    // An observability counter, but one that performs an `appendFileSync` to
    // whatever path the environment names, on EVERY process-snapshot
    // collection — including `collectPsOutput`'s on the eviction path.
    const collectionLogPath = join(workDir, 'leaked-ps-collections.log');
    await writeFile(collectionLogPath, '', 'utf8');
    await seedLedger([ceilingEntry()]);

    const beat = cli(['--orchestrator-id=collection-log-seam-orch', '--desired-agents=1'], {
      ARM_BEAT_SNAPSHOT_TEST_MODE: undefined,
      ARM_EVICT_TEST_MODE: undefined,
      ARM_FAKE_PS_OUTPUT: undefined,
      ARM_FAKE_NOW_MS: undefined,
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_PS_COLLECTION_LOG: collectionLogPath,
    });
    expect(beat.status).toBe(0);

    expect(await readFile(collectionLogPath, 'utf8')).toBe('');
    // Non-vacuity: the beat really did take a snapshot and do its work, so
    // "nothing written" is not "nothing happened".
    expect((await readLedger()).byId('collection-log-seam-orch')).toBeDefined();
  }, 30_000);
});

describe('the sweep respects the fencing-lock discipline every other writer in this file follows', () => {
  it('does not extend lock-hold time when the process snapshot is slow', async () => {
    // AC 1 — the bug class: holding the coordination-file lock across
    // an external process spawn. The snapshot must be taken BEFORE any lock
    // is acquired (the shape `computeAvailableCapacity`'s sampled-before-lock
    // pattern already uses in cli.mjs), so a second orchestrator's own
    // `declareDibs` is unaffected by how slow `ps` is.
    // TWO probes, at two different points in the slow beat's life, because
    // one is not enough. A probe fired DURING the delay proves only that the
    // delay that exists today is outside the lock — it passes unchanged if a
    // SECOND, in-lock snapshot spawn is added alongside the hoisted one, since
    // by then the probe has already finished. The second probe is fired once
    // the unlocked delay has elapsed, which is exactly when such an in-lock
    // spawn would be running and holding the lock.
    const DELAY_MS = 2_000;
    await seedLedger([ceilingEntry(), ghostDibs('ghost-orch', 1)]);

    const slowBeatStartedAt = Date.now();
    const slowBeat = runCliAsync(
      ['--orchestrator-id=slow-sweeper', '--desired-agents=1'],
      baseEnv({ ARM_FAKE_PS_DELAY_MS: String(DELAY_MS) }),
    );

    /** Runs one concurrent beat starting at a fixed offset into the slow beat. */
    const probeAt = async (offsetMs, orchestratorId) => {
      const waitMs = slowBeatStartedAt + offsetMs - Date.now();
      if (waitMs > 0) await sleep(waitMs);

      const startedAt = Date.now();
      const result = cli([`--orchestrator-id=${orchestratorId}`, '--desired-agents=1']);
      return { result, elapsed: Date.now() - startedAt };
    };

    // Probe 1 — inside the snapshot collection itself.
    const during = await probeAt(400, 'concurrent-during');
    // Probe 2 — after the unlocked delay has elapsed, so the slow beat is at
    // (or just past) its critical sections. An in-lock snapshot spawn would
    // hold the lock across this whole window.
    const after = await probeAt(DELAY_MS + 250, 'concurrent-after');

    const slow = await slowBeat;
    const slowElapsed = Date.now() - slowBeatStartedAt;

    expect(during.result.status).toBe(0);
    expect(after.result.status).toBe(0);
    expect(slow.status).toBe(0);

    // Non-vacuity: the delay seam was genuinely honoured, so the elapsed
    // bounds below mean something. And it was paid ONCE — a beat holding the
    // lock across a second snapshot would take about twice as long.
    expect(slowElapsed).toBeGreaterThanOrEqual(DELAY_MS);
    expect(slowElapsed).toBeLessThan(2 * DELAY_MS);

    // Neither concurrent orchestrator waits out a snapshot. Generous margin
    // over a cold `node` start; still far below the 2s a lock-held delay
    // would cost either of them.
    expect(during.elapsed).toBeLessThan(1_000);
    expect(after.elapsed).toBeLessThan(1_000);

    // …and they really did write, so the timings above are not the timings of
    // beats that gave up.
    const { byId } = await readLedger();
    expect(byId('concurrent-during')).toBeDefined();
    expect(byId('concurrent-after')).toBeDefined();
  }, 60_000);

  it('leaves the other ledgers reconciled, logs the one it could not reach, and converges on the next beat', async () => {
    // AC 2 — partial-sweep atomicity. Three ledgers are three critical
    // sections, not one transaction. A failure on the second must not leave
    // the beat claiming a clean sweep, and must not be permanent.
    const seeded = () => [
      ceilingEntry(),
      ghostDibs('ghost-orch', LIVE_AGENT_CEILING),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [
        claimRecord('ghost-orch', 2, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
      ]),
      claimLedger(LIVE_AGENT_ADMISSION_CLAIM_TYPE, [
        claimRecord('ghost-orch', 1, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
      ]),
    ];
    await seedLedger(seeded());

    const failedBeat = cli(['--orchestrator-id=sweeper', '--desired-agents=1'], {
      ARM_FAKE_SWEEP_WRITE_FAILURES_JSON: JSON.stringify({
        [LIVE_AGENT_CLAIM_TYPE]: 'COORDINATION_LOCK_TIMEOUT',
      }),
    });
    expect(failedBeat.status).toBe(0);

    const afterFailure = await readLedger();
    expect(afterFailure.byId('ghost-orch')).toBeUndefined();
    expect(claimantIds(afterFailure.claimsOf(LIVE_AGENT_ADMISSION_CLAIM_TYPE))).not.toContain('ghost-orch');
    // The one that failed is untouched — and SAID SO, naming both the ledger
    // and the failure, rather than being folded silently into a beat that
    // reads as fully reconciled.
    expect(claimantIds(afterFailure.claimsOf(LIVE_AGENT_CLAIM_TYPE))).toContain('ghost-orch');
    expect(failedBeat.stderr).toContain(LIVE_AGENT_CLAIM_TYPE);
    expect(failedBeat.stderr).toContain('COORDINATION_LOCK_TIMEOUT');

    // The next ordinary beat picks up the remainder — the failure delayed
    // convergence, it did not prevent it.
    const nextBeat = cli(['--orchestrator-id=sweeper', '--desired-agents=1']);
    expect(nextBeat.status).toBe(0);
    expect(claimantIds((await readLedger()).claimsOf(LIVE_AGENT_CLAIM_TYPE))).not.toContain('ghost-orch');
  }, 60_000);

  it('abandons a ledger\'s sweep whole when the fencing lock is lost mid-write, never writing part of it', async () => {
    // AC 3 — choke-point reuse. The reap must commit through the same
    // `withLock`/`writeEntriesAtomic` path every other writer uses, which is
    // what makes a lost lock an abandoned write rather than an unfenced one.
    // TWO ghost records in the failing ledger, so "half of them reaped"
    // is a distinguishable outcome from "none of them".
    await seedLedger([
      ceilingEntry(),
      ghostDibs('ghost-orch', 1),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [
        claimRecord('ghost-one', 1, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
        claimRecord('ghost-two', 1, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
      ]),
    ]);

    const beat = cli(['--orchestrator-id=sweeper', '--desired-agents=1'], {
      ARM_FAKE_SWEEP_WRITE_FAILURES_JSON: JSON.stringify({
        [LIVE_AGENT_CLAIM_TYPE]: 'COORDINATION_LOCK_LOST',
      }),
    });
    expect(beat.status).toBe(0);
    expect(beat.stderr).toContain('COORDINATION_LOCK_LOST');

    const { byId, claimsOf } = await readLedger();

    // Neither ghost record went — not one of the two.
    const claims = claimsOf(LIVE_AGENT_CLAIM_TYPE);
    expect(claimantIds(claims).sort()).toEqual(['ghost-one', 'ghost-two']);
    // The ledger is intact rather than half-rewritten: its aggregate still
    // describes the records it still holds.
    expect(byId(claimLedgerId(LIVE_AGENT_CLAIM_TYPE)).grantedTotal).toBe(2);
    // The ledger that did not fail still swept — the failure is scoped to
    // one critical section.
    expect(byId('ghost-orch')).toBeUndefined();
  }, 30_000);
});

// ---------------------------------------------------------------------------
// The snapshot-freshness guard: a verdict is a statement about SNAPSHOT TIME,
// so the sweep must not act on a record the snapshot predates.
// ---------------------------------------------------------------------------

describe('the sweep defers every record written at or after the snapshot it is judging against', () => {
  /**
   * A claude root the sweeper's own fixture tree does NOT carry, so a record
   * naming it takes `pid-absent` under the sweeper's snapshot — the direction
   * of the gap with the live-reap consequence.
   */
  const LATE_CLAUDE_ROOT_PID = 920_600;
  const LATE_CLAUDE_ROOT_ETIME = '00:20:00';
  /** How far into the sweeper's own beat the late orchestrator's write lands. */
  const LATE_WRITE_OFFSET_MS = 5_000;

  /** The late orchestrator's own tree: its claude root, then the real anchors. */
  function buildLateAncestryPsOutput() {
    return [
      '  PID  PPID    RSS     ELAPSED COMM',
      '    1     0    512 1-03:46:39 /sbin/launchd',
      `${LATE_CLAUDE_ROOT_PID}     1 102400    ${LATE_CLAUDE_ROOT_ETIME} /Users/someone/.local/bin/claude`,
      `${TEST_PPID} ${LATE_CLAUDE_ROOT_PID}  81920    00:10:00 /usr/local/bin/node`,
      `${TEST_PID} ${TEST_PPID}  65536    00:05:00 /usr/local/bin/node`,
      '',
    ].join('\n');
  }

  it('the late orchestrator\'s claude root is genuinely absent from the sweeper\'s snapshot', () => {
    // The premise the test below rests on. If the sweeper's fixture carried
    // this pid, the record would read `alive` on its own merits and the guard
    // would never be consulted.
    expect(buildAncestryPsOutput()).not.toContain(String(LATE_CLAUDE_ROOT_PID));
  });

  it('spares a dibs record a concurrent orchestrator wrote while the sweeper was still getting to the lock', async () => {
    // THE HARD AC, in the direction that costs a live agent its record.
    // `observeBeatProcesses` fixes the snapshot before any lock is taken (it
    // must — spawning `ps` under a lock hold is the bug class), and the
    // sweep then waits for three locks in turn. An orchestrator whose claude
    // root was born during that wait writes a record naming a pid no row of
    // the older snapshot can carry: `pid-absent`, `confirmed-dead`, reaped
    // while alive. The record's own write stamp is what makes it undecidable,
    // and no second snapshot is taken to establish that.
    //
    // The window is manufactured with the delay seam rather than with real
    // lock contention, so the ordering is deterministic: the sweeper's
    // snapshot is anchored at T0, the late orchestrator writes at
    // T0 + LATE_WRITE_OFFSET_MS on its own clock, and the sweep runs after.
    const SNAPSHOT_DELAY_MS = 2_500;
    await seedLedger([ceilingEntry()]);

    const sweeper = runCliAsync(
      ['--orchestrator-id=sweeper', '--desired-agents=1'],
      baseEnv({ ARM_FAKE_PS_DELAY_MS: String(SNAPSHOT_DELAY_MS) }),
    );

    // Land the late write inside the sweeper's pre-lock snapshot delay.
    await sleep(600);
    const late = cli(['--orchestrator-id=late-but-live-orch', '--desired-agents=1'], {
      ARM_FAKE_PS_OUTPUT: buildLateAncestryPsOutput(),
      ARM_FAKE_NOW_MS: String(T0 + LATE_WRITE_OFFSET_MS),
      ARM_BEAT_NOW_TEST_MODE: '1',
    });
    expect(late.status).toBe(0);

    const swept = await sweeper;
    expect(swept.status).toBe(0);

    const { byId } = await readLedger();
    const survivor = byId('late-but-live-orch');

    // It is still there…
    expect(survivor).toBeDefined();
    // …it really was the record this guard is about — written after the
    // sweeper's snapshot, naming a pid that snapshot could not carry…
    expect(survivor.pid).toBe(LATE_CLAUDE_ROOT_PID);
    expect(survivor.declaredAt).toBeGreaterThanOrEqual(T0);
    // …and the sweeper really did look at it and spare it, rather than never
    // reaching it: the deferral is reported in the same vocabulary as a reap.
    expect(swept.stderr).toContain(
      `deferred dibs record for late-but-live-orch (pid ${LATE_CLAUDE_ROOT_PID}): ` +
        `${LIVENESS_REASON.WRITTEN_AFTER_SNAPSHOT}`,
    );
  }, 60_000);

  it('spares a claim record stamped at or after the snapshot, on both stamps the ledgers use', async () => {
    // The `claimedAt` arm of the same rule. Seeded directly rather than raced,
    // because the stamp — not the concurrency that produces it — is what the
    // guard reads, and a hand-seeded stamp pins the inclusive boundary exactly.
    await seedLedger([
      ceilingEntry(),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [
        // Exactly AT the snapshot: the snapshot may or may not have seen it,
        // and "may not" is the direction that costs a live agent its record.
        { ...claimRecord('boundary-orch', 1, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }), claimedAt: T0 },
        // Strictly after it.
        {
          ...claimRecord('later-orch', 1, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
          claimedAt: T0 + LATE_WRITE_OFFSET_MS,
        },
        // Non-vacuity: same pid, stamped before the snapshot. It IS decidable,
        // and it goes — so the sweep genuinely ran over this ledger.
        claimRecord('older-orch', 1, { pid: GHOST_PID, pidStartedAt: GHOST_PID_STARTED_AT }),
      ]),
    ]);

    const beat = cli(['--orchestrator-id=sweeper', '--desired-agents=1']);
    expect(beat.status).toBe(0);

    const claims = claimantIds((await readLedger()).claimsOf(LIVE_AGENT_CLAIM_TYPE));
    expect(claims.sort()).toEqual(['boundary-orch', 'later-orch']);
    expect(beat.stderr).toContain(`reaped ${LIVE_AGENT_CLAIM_TYPE} record for older-orch`);
    expect(beat.stderr).toContain(
      `deferred ${LIVE_AGENT_CLAIM_TYPE} record for boundary-orch (pid ${GHOST_PID}): ` +
        `${LIVENESS_REASON.WRITTEN_AFTER_SNAPSHOT}`,
    );
  }, 30_000);
});

// ---------------------------------------------------------------------------
// THE FRESHNESS GUARD'S ANCHOR, PINNED STRUCTURALLY.
//
// WHY NOT BEHAVIOURALLY (pre-PR review round 2, Medium — the fix below shipped
// unpinned). The guard defers a record whose write stamp is at or after the
// instant the sweeper's `ps` was ISSUED, which is deliberately EARLIER than
// `snapshotAt` — the latter is read after the collection returns, and the
// snapshot's contents were fixed when the kernel served the `ps`. A record
// stamped inside that collection window is in no row of the snapshot yet
// cleared a `snapshotAt`-anchored guard, and was reaped `pid-absent`: a live
// agent's record deleted through the guard written to stop exactly that.
//
// No test in this file can tell the two anchors apart, for two reasons that
// between them cover every beat here. Most run under `ARM_FAKE_NOW_MS`, which
// COLLAPSES the instants outright: `resolveProcessSnapshotNow` returns the same
// constant on both reads, so `snapshotIssuedAt === snapshotAt` by construction.
// The handful that deliberately blank that variable out (an empty string,
// which `resolveNow` treats as absent) do get two real `Date.now()` readings
// — but
// they assert on the identity a beat WRITES, never on what a sweep reaps, so
// the guard is not on their path at all. A behavioural pin would therefore have
// to be a new test that drops the fake clock AND races a second `cli.mjs` into
// a real `ARM_FAKE_PS_DELAY_MS` window — a timing race, in a suite that must
// not have one, to pin a two-token wiring fact.
//
// So the wiring itself is asserted, in the same idiom
// ./lib/pid-identity-schema.jest.spec.mjs uses to pin the anti-`process.pid`
// rule: read cli.mjs's source and assert the two properties that make the
// anchor correct. Reverting either one fails here.
// ---------------------------------------------------------------------------

describe('the sweep freshness guard is anchored to the ps ISSUE instant', () => {
  const cliSource = readFileSync(CLI, 'utf8');

  /** Strips comments so a prose mention can never satisfy a code assertion. */
  const code = cliSource
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .split('\n')
    .map((line) => line.replace(/(^|\s)\/\/.*$/, '$1'))
    .join('\n');

  it('resolves `snapshotIssuedAt` BEFORE the collection and `snapshotAt` after it', () => {
    const issued = code.indexOf('const snapshotIssuedAt = resolveProcessSnapshotNow(');
    const collect = code.indexOf('await collectBeatPsSnapshot(');
    const taken = code.indexOf('const snapshotAt = resolveProcessSnapshotNow(');

    expect(issued).toBeGreaterThanOrEqual(0);
    expect(collect).toBeGreaterThan(issued);
    expect(taken).toBeGreaterThan(collect);
  });

  it('threads the ISSUE instant, not `snapshotAt`, into `isWrittenAfterSnapshot`', () => {
    expect(code).toContain('isWrittenAfterSnapshot(record, observation.snapshotIssuedAt)');
    expect(code).not.toContain('isWrittenAfterSnapshot(record, observation.snapshotAt)');
  });

  it('still anchors the etime-derived identities to `snapshotAt`', () => {
    // The other half of the same rule, and the reason the fix is two instants
    // rather than one moved instant: `PID_IDENTITY_TOLERANCE_MS` was sized
    // against the post-collection anchor, so the identity side keeps it. That
    // is a "measured against this, so do not move it under the tolerance
    // without re-deriving" constraint, not a claim that the earlier anchor
    // would be unsafe — see `observeBeatProcesses`' own comment, which says so
    // explicitly.
    expect(code).toContain('listAgentProcesses(collected.output, snapshotAt)');
    expect(code).toContain('resolveNearestClaudeRootIdentity(collected.output, process.ppid, snapshotAt)');
  });
});
