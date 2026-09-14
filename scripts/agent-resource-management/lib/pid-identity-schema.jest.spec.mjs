// — the `pid` / `pidStartedAt` ledger schema, pinned at the library
// boundary. FOUR functions accept the pair:
//
//   (a) `declareDibs`              — ./coordination-file.mjs (persists it on
//       the dibs entry)
//   (b) `claimCapacity`            — ./coordination-file.mjs (stamps it on the
//       claim record it appends)
//   (c) `reserveAdmission`         — ./coordination-file.mjs (stamps it on the
//       admission-ledger record it appends)
//   (d) `claimAndAdmitQueueEntry`  — ./dispatch.mjs (forwards it verbatim into
//       its internal `reserveAdmission`; the dispatcher-drain path,
//       reached from `cli.mjs --dequeue-if-capacity`, whose admission holds
//       land in the same ledger as `main()`'s own direct `reserveAdmission`)
//
// The contract all four share: `{pid, pidStartedAt}` is an OPTIONAL,
// ALREADY-RESOLVED pair supplied by the caller. None of the four computes,
// derives, or defaults it — each only persists or forwards it unchanged.
// Resolving what those values should BE requires the beat's own `ps` snapshot
// and lives elsewhere: `resolveNearestClaudeRootIdentity` in ./recon.mjs walks
// from `process.ppid` up to the nearest claude-rooted ancestor, and `cli.mjs`
// threads its result into all four. Nothing about either is asserted here —
// this file owns the library boundary only.
//
// WHAT THE GROUPS BELOW PIN
//   1-2  `declareDibs` persists the pair, `readDibs` surfaces it uncoerced,
//        and a re-declaration REPLACES it (a respawn is a new process).
//   3-5  `claimCapacity` and `reserveAdmission` stamp it onto the appended
//        record, and omit the own-property entirely when it is not supplied.
//   6    `claimAndAdmitQueueEntry` forwards it rather than dropping it.
//   7    STRUCTURAL: no ledger write-path function derives identity from its
//        own process.
//   8    Two records under one `orchestratorId` stay independently
//        identifiable by record.
//   9-11 Backward compatibility: `grantedTotal`'s shape and meaning, a
//        pid-less pre-existing ledger file, and a hand-corrupted pid on disk.
//
// ---------------------------------------------------------------------------
// THE STANDING RULE THIS FILE ENCODES
// ---------------------------------------------------------------------------
// LEDGER IDENTITY MUST NEVER BE THE WRITING PROCESS'S OWN PID. `cli.mjs` is a
// one-shot ~62ms CLI, and `listAgentProcesses` (./recon.mjs) only ever emits
// CLAUDE-ROOTED processes (`comm` matching `/\/claude$/`), so a `node cli.mjs`
// pid can NEVER appear in the snapshot the read side classifies against. A
// self-derived identity would therefore be structurally invisible to the
// read-side classifier, whose "pid absent from snapshot → confirmed-dead" rule
// would then reap every LIVE orchestrator. Caller-supplied, ancestry-resolved
// identity is what makes that classifier correct. (That classifier is
// `classifyLedgerEntryLiveness` in ./reconciliation.mjs, and it owns its own
// tests; references to it below describe the contract this schema serves, not
// assertions made here.)
//
// FIXTURE DISCIPLINE THAT FOLLOWS FROM IT: every test below supplies pid and
// pidStartedAt as LITERAL fixture values (`CALLER_PID = 4242`, etc.), NEVER
// derived from this test process's own `process.pid`/`process.uptime()`. A
// fixture derived from the test process's own identity would let an
// implementation that "helpfully" reads `process.pid` internally pass by
// coincidence.
//
// Both fields are OPTIONAL and purely ADDITIVE. Absent means absent — never
// `NaN`, never a synthesized placeholder, never a throw.
//
// `claimCapacity`'s `orchestratorId` validation, reserved-id rejection, and
// `requestedCount` validation are the only checks it makes; the identity pair
// adds no validation surface, and nothing below asserts one.

import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  DEFAULT_CLAIM_TTL_MS,
  claimCapacity,
  declareDibs,
  getLiveClaimTotal,
  readDibs,
  reserveAdmission,
} from './coordination-file.mjs';
import { claimAndAdmitQueueEntry } from './dispatch.mjs';
import { enqueueItem } from './queue.mjs';

/** Fixed clock, so every `claimedAt`/`declaredAt` below is exact. */
const NOW = 1_700_000_000_000;
/** The dibs liveness window `cli.mjs` uses (`DEFAULT_LIVENESS_THRESHOLD_MS`). */
const LIVENESS_MS = 15 * 60 * 1000;

const LIVE_AGENT_CLAIM_TYPE = 'live-agent';
const LIVE_AGENT_ADMISSION_CLAIM_TYPE = 'live-agent-admission';

/** Ledger entry id convention for a per-type claim ledger (`__claim:<type>__`). */
const claimLedgerId = (type) => `__claim:${type}__`;

/**
 * Deliberately NOT `process.pid` — see this file's header. If an
 * implementation reads `process.pid` internally instead of honouring the
 * caller-supplied value, these fixtures make that visible.
 */
const CALLER_PID = 4242;
const CALLER_PID_STARTED_AT = NOW - 60_000;

let workDir;
let filePath;
let queueFilePath;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'arm-pid-schema-'));
  filePath = join(workDir, 'resource-coordination.json');
  queueFilePath = join(workDir, 'resource-queue.json');
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
});

/** Reads the raw on-disk entry array — no read-path filtering in the way. */
function readPersisted() {
  return JSON.parse(readFileSync(filePath, 'utf8'));
}

function persistedEntry(orchestratorId) {
  return readPersisted().find((entry) => entry.orchestratorId === orchestratorId);
}

function persistedClaims(type) {
  return persistedEntry(claimLedgerId(type))?.claims ?? [];
}

/** Ample capacity — no test here is about scarcity arithmetic. */
const availableCapacity100 = async () => 100;

// ---------------------------------------------------------------------------
// 1-2. declareDibs / readDibs
// ---------------------------------------------------------------------------

describe('declareDibs / readDibs carry the caller-supplied pid identity', () => {
  it('declareDibs persists pid/pidStartedAt on a fresh entry', async () => {
    await declareDibs(
      filePath,
      {
        orchestratorId: 'orch-a',
        desiredAgents: 2,
        declaredAt: NOW,
        firstDeclaredAt: NOW,
        pid: CALLER_PID,
        pidStartedAt: CALLER_PID_STARTED_AT,
      },
      { livenessThresholdMs: LIVENESS_MS },
    );

    expect(persistedEntry('orch-a')).toMatchObject({
      orchestratorId: 'orch-a',
      desiredAgents: 2,
      declaredAt: NOW,
      pid: CALLER_PID,
      pidStartedAt: CALLER_PID_STARTED_AT,
    });
  });

  it('readDibs surfaces pid/pidStartedAt unchanged — no filtering, no stripping, no coercion', async () => {
    await declareDibs(
      filePath,
      {
        orchestratorId: 'orch-a',
        desiredAgents: 1,
        declaredAt: NOW,
        firstDeclaredAt: NOW,
        pid: CALLER_PID,
        pidStartedAt: CALLER_PID_STARTED_AT,
      },
      { livenessThresholdMs: LIVENESS_MS },
    );

    const dibs = await readDibs(filePath, NOW, { livenessThresholdMs: LIVENESS_MS });
    const entry = dibs.find((candidate) => candidate.orchestratorId === 'orch-a');

    expect(entry.pid).toBe(CALLER_PID);
    expect(entry.pidStartedAt).toBe(CALLER_PID_STARTED_AT);
  });

  it('a re-declaration under the same orchestratorId refreshes pid/pidStartedAt (a respawned process is a NEW process)', async () => {
    // The crash-and-respawn case, dibs side: `declareDibs` upserts by
    // `orchestratorId`, so the respawned process's identity must REPLACE the
    // dead one's — the opposite of `firstDeclaredAt`, which is deliberately
    // carried forward. A ghost pid surviving an upsert would make the Phase 2
    // classifier reap a live orchestrator.
    await declareDibs(
      filePath,
      { orchestratorId: 'orch-a', desiredAgents: 1, declaredAt: NOW, pid: 1111, pidStartedAt: NOW - 120_000 },
      { livenessThresholdMs: LIVENESS_MS },
    );
    await declareDibs(
      filePath,
      { orchestratorId: 'orch-a', desiredAgents: 1, declaredAt: NOW + 1_000, pid: 2222, pidStartedAt: NOW - 500 },
      { livenessThresholdMs: LIVENESS_MS },
    );

    expect(persistedEntry('orch-a')).toMatchObject({
      pid: 2222,
      pidStartedAt: NOW - 500,
      // …while first-arrival priority is still preserved, unchanged.
      firstDeclaredAt: NOW,
    });
  });
});

// ---------------------------------------------------------------------------
// 3-5. claimCapacity / reserveAdmission per-claim records
// ---------------------------------------------------------------------------

describe('claimCapacity appends the caller-supplied pid identity onto its claim record', () => {
  it("claimCapacity's appended claim record carries pid/pidStartedAt when the caller supplies them", async () => {
    await claimCapacity(
      filePath,
      {
        type: LIVE_AGENT_CLAIM_TYPE,
        requestedCount: 2,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: availableCapacity100,
        now: NOW,
        pid: CALLER_PID,
        pidStartedAt: CALLER_PID_STARTED_AT,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    const claims = persistedClaims(LIVE_AGENT_CLAIM_TYPE);
    expect(claims).toHaveLength(1);
    expect(claims[0]).toMatchObject({
      orchestratorId: 'orch-a',
      count: 2,
      claimedAt: NOW,
      pid: CALLER_PID,
      pidStartedAt: CALLER_PID_STARTED_AT,
    });
  });

  it("claimCapacity omits pid/pidStartedAt when the caller doesn't supply them (still valid, no crash)", async () => {
    // Absent means ABSENT — not `null`, not `NaN`, not `process.pid` silently
    // substituted. Phase 2 classifies a record with no `pid` as *unknown* and
    // defers it to the pre-existing TTL; a synthesized placeholder would
    // instead route it to a confident (and wrong) liveness verdict.
    const result = await claimCapacity(
      filePath,
      {
        type: LIVE_AGENT_CLAIM_TYPE,
        requestedCount: 2,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: availableCapacity100,
        now: NOW,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    expect(result.granted).toBe(2);
    const claims = persistedClaims(LIVE_AGENT_CLAIM_TYPE);
    expect(claims).toHaveLength(1);
    expect(claims[0]).toEqual({ orchestratorId: 'orch-a', count: 2, claimedAt: NOW });
    expect(Object.prototype.hasOwnProperty.call(claims[0], 'pid')).toBe(false);
    expect(Object.prototype.hasOwnProperty.call(claims[0], 'pidStartedAt')).toBe(false);
  });

  it("reserveAdmission's admission claim record carries pid/pidStartedAt", async () => {
    const result = await reserveAdmission(
      filePath,
      {
        snapshotType: LIVE_AGENT_CLAIM_TYPE,
        admissionType: LIVE_AGENT_ADMISSION_CLAIM_TYPE,
        requestedCount: 2,
        orchestratorId: 'orch-a',
        ceiling: 4,
        now: NOW,
        pid: CALLER_PID,
        pidStartedAt: CALLER_PID_STARTED_AT,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    expect(result.granted).toBe(2);
    const claims = persistedClaims(LIVE_AGENT_ADMISSION_CLAIM_TYPE);
    expect(claims).toHaveLength(1);
    expect(claims[0]).toMatchObject({
      orchestratorId: 'orch-a',
      count: 2,
      claimedAt: NOW,
      pid: CALLER_PID,
      pidStartedAt: CALLER_PID_STARTED_AT,
    });
  });

  it('reserveAdmission omits pid/pidStartedAt when the caller does not supply them', async () => {
    await reserveAdmission(
      filePath,
      {
        snapshotType: LIVE_AGENT_CLAIM_TYPE,
        admissionType: LIVE_AGENT_ADMISSION_CLAIM_TYPE,
        requestedCount: 1,
        orchestratorId: 'orch-a',
        ceiling: 4,
        now: NOW,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    const claims = persistedClaims(LIVE_AGENT_ADMISSION_CLAIM_TYPE);
    expect(claims).toEqual([{ orchestratorId: 'orch-a', count: 1, claimedAt: NOW }]);
  });
});

// ---------------------------------------------------------------------------
// 6. `claimAndAdmitQueueEntry` — the FOURTH call site (./dispatch.mjs)
// ---------------------------------------------------------------------------
// The dispatcher-drain path, reached from `cli.mjs`'s
// `--dequeue-if-capacity` / `handleDequeueIfCapacity`. It composes
// `claimQueueEntry` + `reserveAdmission`, so every admission hold it writes
// lands in the SAME `__claim:live-agent-admission__` ledger as `main()`'s own
// direct `reserveAdmission` call. A dispatcher that dropped the identity pair
// on the floor would make every dispatcher-drained hold permanently
// unclassifiable (TTL-only) while holds written by the direct path stayed
// classifiable — one ledger, two governance regimes. This group pins the
// pass-through that keeps the two paths equivalent.

describe('claimAndAdmitQueueEntry forwards the caller-supplied pid identity (dispatcher-drain path)', () => {
  /** The queue item shape ./dispatch.jest.spec.mjs already uses. */
  function validQueueItem(overrides = {}) {
    return {
      agentClass: 'orchestrator',
      priority: 'normal',
      commandRef: 'echo hello',
      orchestratorId: 'enqueuer',
      ...overrides,
    };
  }

  function dispatchParams(overrides = {}) {
    return {
      orchestratorId: 'dispatcher-a',
      snapshotType: LIVE_AGENT_CLAIM_TYPE,
      admissionType: LIVE_AGENT_ADMISSION_CLAIM_TYPE,
      ceiling: 4,
      now: NOW,
      ...overrides,
    };
  }

  it('forwards caller-supplied pid/pidStartedAt into its internal reserveAdmission call', async () => {
    await enqueueItem(queueFilePath, validQueueItem());

    const result = await claimAndAdmitQueueEntry(
      filePath,
      queueFilePath,
      dispatchParams({ pid: CALLER_PID, pidStartedAt: CALLER_PID_STARTED_AT }),
    );

    expect(result.granted).toBe(true);

    // The only observable proof that the pair was FORWARDED rather than
    // dropped: the admission-ledger record `reserveAdmission` wrote on this
    // call's behalf carries it. Pure pass-through — the literal fixture
    // values, unmodified.
    const claims = persistedClaims(LIVE_AGENT_ADMISSION_CLAIM_TYPE);
    expect(claims).toHaveLength(1);
    expect(claims[0]).toMatchObject({
      orchestratorId: 'dispatcher-a',
      count: 1,
      claimedAt: NOW,
      pid: CALLER_PID,
      pidStartedAt: CALLER_PID_STARTED_AT,
    });
  });

  it("omits pid/pidStartedAt when the caller doesn't supply them (still valid, no crash)", async () => {
    await enqueueItem(queueFilePath, validQueueItem());

    const result = await claimAndAdmitQueueEntry(filePath, queueFilePath, dispatchParams());

    expect(result.granted).toBe(true);

    // Absent means ABSENT. A dispatcher that substituted a placeholder (or,
    // worse, its own `process.pid`) would hand Phase 2's classifier a
    // confident and wrong liveness verdict instead of "unknown".
    const claims = persistedClaims(LIVE_AGENT_ADMISSION_CLAIM_TYPE);
    expect(claims).toEqual([{ orchestratorId: 'dispatcher-a', count: 1, claimedAt: NOW }]);
  });
});

// ---------------------------------------------------------------------------
// 7. STRUCTURAL: none of the four functions — nor the `withPidIdentity`
//    helper they share — derives identity from its OWN process
// ---------------------------------------------------------------------------

describe('no write-path function derives pid/pidStartedAt from its own process', () => {
  /**
   * Strips `//` line comments and block comments so a doc comment that merely
   * TALKS about `process.pid` (both modules have several, deliberately
   * emphatic ones) can't trip this assertion. Executable code only.
   */
  function stripComments(source) {
    return source.replace(/\/\*[\s\S]*?\*\//g, ' ').replace(/(^|[^:])\/\/[^\n]*/g, '$1');
  }

  /**
   * Slices one top-level `[export] [async] function NAME(` declaration out of
   * a module's source, ending at the next top-level declaration — either an
   * `export` or a bare (non-exported) `function` — or EOF.
   *
   * Scoped deliberately: ./coordination-file.mjs has a legitimate
   * `process.pid` in its lock-payload write, inside the exported
   * `acquireLock`, which is about lock OWNERSHIP, not ledger identity. A
   * whole-file grep would false-positive on it; slicing scopes the assertion
   * to the ledger write path, which is what the rule actually governs. (The
   * companion test below pins that lock-payload line's continued existence so
   * this narrow scoping can't rot into a no-op.)
   *
   * The `export ` prefix is OPTIONAL, and the terminator also matches a bare
   * top-level `function`, so NON-EXPORTED helpers are sliceable too. That is
   * required, not incidental: most of the ledger write path is module-private,
   * including `withPidIdentity` — the helper every coordination-file.mjs claim
   * write funnels through, and so the single place where one self-derived
   * identity would leak into all of them at once. A slicer that only matched
   * `export function` would leave every private helper inside no target slice
   * at all, and the assertions below would be vacuous for them.
   */
  function functionBody(source, name) {
    const declaration = new RegExp(`^(?:export )?(?:async )?function ${name}\\b`, 'm');
    const start = source.search(declaration);
    expect(start).toBeGreaterThanOrEqual(0);
    const rest = source.slice(start + 1);
    const nextDeclaration = rest.search(/^(?:export |(?:async )?function )/m);
    return nextDeclaration === -1 ? rest : rest.slice(0, nextDeclaration);
  }

  const libDir = dirname(fileURLToPath(import.meta.url));
  const coordinationSource = stripComments(
    readFileSync(join(libDir, 'coordination-file.mjs'), 'utf8'),
  );
  const dispatchSource = stripComments(readFileSync(join(libDir, 'dispatch.mjs'), 'utf8'));

  it.each([
    ['declareDibs', () => coordinationSource],
    ['claimCapacity', () => coordinationSource],
    ['reserveAdmission', () => coordinationSource],
    ['claimAndAdmitQueueEntry', () => dispatchSource],
    // Non-exported, but the shared helper all three coordination-file.mjs
    // claim writes funnel through — so it is the one place a self-derived
    // identity would leak into ALL of them at once.
    ['withPidIdentity', () => coordinationSource],
    // The rest of the private write path. Every claim record passes through
    // these on its way to disk, so each is a plausible place for someone to
    // "helpfully" stamp identity alongside the timestamp/filtering they
    // already do — `stampMigratedClaims` most of all, since it maps over
    // `claims[]` stamping `claimedAt` immediately before the write in all
    // three coordination-file.mjs paths.
    ['stampMigratedClaims', () => coordinationSource],
    ['filterOutZeroCountClaims', () => coordinationSource],
    ['writeEntriesAtomic', () => coordinationSource],
    // The pruning and release functions in the same claim-array write path.
    // Each rebuilds or filters records immediately before a write — most
    // pointedly `releaseCapacity`, which reconstructs a surviving record as
    // `{ ...claim, count }` and is therefore one spread away from acquiring an
    // identity field it must not have.
    ['pruneExpiredClaims', () => coordinationSource],
    ['pruneStale', () => coordinationSource],
    ['releaseCapacity', () => coordinationSource],
    // Read side: the point where a missing identity could be back-filled
    // from the reading process rather than left absent.
    ['normalizeClaims', () => coordinationSource],
  ])(
    '%s contains no process.pid / process.uptime() identity derivation of its own',
    (name, sourceOf) => {
      const body = functionBody(sourceOf(), name);

      // The rule expressed as a structural invariant rather than a
      // behavioural one: a self-derived identity is not detectable from the
      // outside once a caller happens to pass the same value, so it is pinned
      // at the source level where it can't hide.
      //
      // This is a structural TRIPWIRE, not a proof. It matches only the
      // literal `process.pid` / `process.uptime(` forms; `process['pid']`,
      // `const { pid } = process`, or an `import process from 'node:process'`
      // alias would all evade it. It is here to catch the obvious
      // reintroduction, not to make one impossible.
      expect(body).not.toMatch(/process\s*\.\s*pid\b/);
      expect(body).not.toMatch(/process\s*\.\s*uptime\s*\(/);
    },
  );

  it('the lock-payload process.pid in coordination-file.mjs is untouched by this assertion (it is a different concern)', () => {
    // Guards the scoping above from silently rotting into a no-op: if the
    // lock payload's legitimate `process.pid` ever disappeared, a future
    // reader should know the narrow scoping is what is being relied on, and
    // that it is still narrow for a reason.
    expect(coordinationSource).toMatch(/JSON\.stringify\(\{\s*pid:\s*process\.pid/);
  });
});

// ---------------------------------------------------------------------------
// 8. Respawn under ONE orchestratorId — two records must COEXIST
// ---------------------------------------------------------------------------

describe('respawn under one orchestratorId (Convergence Analysis #6)', () => {
  it('two claim records under ONE orchestratorId coexist in claims[] without being merged or overwritten by the write path', async () => {
    // Unlike dibs (upserted by id), `claimCapacity` APPENDS. A crash and
    // immediate respawn under the same `orchestratorId` therefore leaves two
    // records — one ghost, one live — inside one `claims[]` array under one
    // id. The write path must not coalesce or dedupe them, and each must stay
    // independently identifiable BY RECORD (its own pid), because Phase 2's
    // classifier operates at record granularity, not id granularity. Merging
    // them would make the ghost's slot unreapable without also killing the
    // live respawn's slot.
    const GHOST_PID = 1111;
    const LIVE_PID = 2222;

    await claimCapacity(
      filePath,
      {
        type: LIVE_AGENT_CLAIM_TYPE,
        requestedCount: 1,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: availableCapacity100,
        now: NOW,
        pid: GHOST_PID,
        pidStartedAt: NOW - 120_000,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    await claimCapacity(
      filePath,
      {
        type: LIVE_AGENT_CLAIM_TYPE,
        requestedCount: 1,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: availableCapacity100,
        now: NOW + 1_000,
        pid: LIVE_PID,
        pidStartedAt: NOW + 500,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    const claims = persistedClaims(LIVE_AGENT_CLAIM_TYPE);

    // Append-not-merge: both records survive, under the one id.
    expect(claims).toHaveLength(2);
    expect(claims.map((claim) => claim.orchestratorId)).toEqual(['orch-a', 'orch-a']);

    // …and each remains distinguishable BY RECORD, which is the whole point.
    expect(claims.map((claim) => claim.pid)).toEqual([GHOST_PID, LIVE_PID]);
    expect(claims.map((claim) => claim.pidStartedAt)).toEqual([NOW - 120_000, NOW + 500]);
    expect(claims.map((claim) => claim.claimedAt)).toEqual([NOW, NOW + 1_000]);
  });
});

// ---------------------------------------------------------------------------
// 9. The `grantedTotal` backward-compat aggregate is untouched
// ---------------------------------------------------------------------------

describe('grantedTotal backward-compat aggregate (mixed-version rollout)', () => {
  it("the schema addition does not change grantedTotal's shape or meaning — pid lives ONLY inside the per-claim record", async () => {
    // Pre- code sharing this host-global file reads
    // `ledgerEntry.grantedTotal` directly and has no notion of `claims[]` at
    // all. `pid`/`pidStartedAt` must therefore stay strictly inside the
    // per-claim records — never promoted onto the ledger entry, never allowed
    // to change what `grantedTotal` sums.
    await claimCapacity(
      filePath,
      {
        type: LIVE_AGENT_CLAIM_TYPE,
        requestedCount: 3,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: availableCapacity100,
        now: NOW,
        pid: CALLER_PID,
        pidStartedAt: CALLER_PID_STARTED_AT,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    const ledgerEntry = persistedEntry(claimLedgerId(LIVE_AGENT_CLAIM_TYPE));

    expect(ledgerEntry.grantedTotal).toBe(3);
    expect(ledgerEntry.grantedTotal).toBe(
      ledgerEntry.claims.reduce((total, claim) => total + claim.count, 0),
    );
    // Exactly the pre-existing ledger-entry key set — no new top-level fields.
    expect(Object.keys(ledgerEntry).sort()).toEqual(['claims', 'declaredAt', 'grantedTotal', 'orchestratorId']);
    expect(Object.prototype.hasOwnProperty.call(ledgerEntry, 'pid')).toBe(false);
    expect(Object.prototype.hasOwnProperty.call(ledgerEntry, 'pidStartedAt')).toBe(false);

    // And the aggregate still tracks the per-claim source of truth after a
    // SECOND pid-bearing claim lands.
    await claimCapacity(
      filePath,
      {
        type: LIVE_AGENT_CLAIM_TYPE,
        requestedCount: 2,
        orchestratorId: 'orch-b',
        computeAvailableCapacity: availableCapacity100,
        now: NOW + 1_000,
        pid: 5555,
        pidStartedAt: NOW,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    expect(persistedEntry(claimLedgerId(LIVE_AGENT_CLAIM_TYPE)).grantedTotal).toBe(5);
  });
});

// ---------------------------------------------------------------------------
// 10. Backward-compat regression: a pre-existing ledger file, no `pid` anywhere
// ---------------------------------------------------------------------------

describe('a pre-existing ledger file with NO pid field anywhere (backward-compat regression)', () => {
  /**
   * Exactly the shape pre-existing code wrote: dibs entries with no pid, a
   * per-claim-array ledger with no pid, a still-legacy scalar ledger, one
   * stale dibs entry, and one already-TTL-expired claim record.
   */
  function seedPreTicketLedger() {
    const entries = [
      // Live dibs entry (60s old, well inside the 15min liveness window).
      { orchestratorId: 'orch-live', desiredAgents: 2, declaredAt: NOW - 60_000, firstDeclaredAt: NOW - 60_000 },
      // Stale dibs entry (20min old — past the window, must be pruned).
      { orchestratorId: 'orch-stale', desiredAgents: 1, declaredAt: NOW - 20 * 60 * 1000, firstDeclaredAt: NOW - 20 * 60 * 1000 },
      // New-shape claim ledger: one fresh record + one past `DEFAULT_CLAIM_TTL_MS`.
      {
        orchestratorId: claimLedgerId(LIVE_AGENT_CLAIM_TYPE),
        claims: [
          { orchestratorId: 'orch-live', count: 2, claimedAt: NOW - 60_000 },
          { orchestratorId: 'orch-expired', count: 3, claimedAt: NOW - DEFAULT_CLAIM_TTL_MS - 1 },
        ],
        grantedTotal: 5,
        declaredAt: NOW - 60_000,
      },
      // Pre- legacy SCALAR ledger — no `claims[]` at all.
      { orchestratorId: claimLedgerId('legacy-type'), grantedTotal: 4, declaredAt: NOW - 60_000 },
    ];
    writeFileSync(filePath, JSON.stringify(entries, null, 2), 'utf8');
  }

  it('round-trips through pruneStale (via readDibs) exactly as before — stale pruned, live kept, no pid materialized', async () => {
    seedPreTicketLedger();

    const dibs = await readDibs(filePath, NOW, { livenessThresholdMs: LIVENESS_MS });
    const ids = dibs.map((entry) => entry.orchestratorId);

    expect(ids).toContain('orch-live');
    expect(ids).not.toContain('orch-stale');
    // Reserved `__claim:<type>__` rows are exempt from liveness pruning —
    // unchanged.
    expect(ids).toContain(claimLedgerId(LIVE_AGENT_CLAIM_TYPE));
    expect(ids).toContain(claimLedgerId('legacy-type'));

    // Missing pid is ABSENT, never `NaN` and never a placeholder.
    const live = dibs.find((entry) => entry.orchestratorId === 'orch-live');
    expect(Object.prototype.hasOwnProperty.call(live, 'pid')).toBe(false);
    expect(live.pid).toBeUndefined();
  });

  it('round-trips through normalizeClaims + pruneExpiredClaims + getLiveClaimTotal exactly as before', async () => {
    seedPreTicketLedger();

    // New-shape ledger: the TTL-expired record is excluded, the fresh one counted.
    await expect(
      getLiveClaimTotal(filePath, LIVE_AGENT_CLAIM_TYPE, NOW, DEFAULT_CLAIM_TTL_MS),
    ).resolves.toBe(2);

    // With no TTL set, nothing is excluded — unchanged pre-existing behaviour.
    await expect(getLiveClaimTotal(filePath, LIVE_AGENT_CLAIM_TYPE, NOW, undefined)).resolves.toBe(5);

    // `excludeOrchestratorId` still works against a pid-less ledger.
    await expect(
      getLiveClaimTotal(filePath, LIVE_AGENT_CLAIM_TYPE, NOW, DEFAULT_CLAIM_TTL_MS, 'orch-live'),
    ).resolves.toBe(0);

    // Legacy SCALAR ledger still folds forward into one synthetic record.
    await expect(
      getLiveClaimTotal(filePath, 'legacy-type', NOW, DEFAULT_CLAIM_TTL_MS),
    ).resolves.toBe(4);

    // A type that was never claimed is still 0, not a throw.
    await expect(getLiveClaimTotal(filePath, 'never-claimed', NOW, DEFAULT_CLAIM_TTL_MS)).resolves.toBe(0);
  });

  it('a pid-less pre-existing claim record survives a subsequent pid-BEARING write untouched (mixed-version file)', async () => {
    seedPreTicketLedger();

    await claimCapacity(
      filePath,
      {
        type: LIVE_AGENT_CLAIM_TYPE,
        requestedCount: 1,
        orchestratorId: 'orch-new',
        computeAvailableCapacity: availableCapacity100,
        now: NOW,
        pid: CALLER_PID,
        pidStartedAt: CALLER_PID_STARTED_AT,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    const claims = persistedClaims(LIVE_AGENT_CLAIM_TYPE);

    // The pre-existing pid-less record is carried forward byte-for-byte…
    expect(claims).toContainEqual({ orchestratorId: 'orch-live', count: 2, claimedAt: NOW - 60_000 });
    // …the expired one is physically pruned by this write (unchanged)…
    expect(claims.map((claim) => claim.orchestratorId)).not.toContain('orch-expired');
    // …and only the NEW record carries the pid identity.
    expect(claims).toContainEqual({
      orchestratorId: 'orch-new',
      count: 1,
      claimedAt: NOW,
      pid: CALLER_PID,
      pidStartedAt: CALLER_PID_STARTED_AT,
    });
  });
});

// ---------------------------------------------------------------------------
// 11. Corrupt / non-numeric pid on disk must not throw on read
// ---------------------------------------------------------------------------

describe('corrupt (non-numeric) pid/pidStartedAt on a hand-edited ledger file', () => {
  function seedCorruptLedger() {
    const entries = [
      { orchestratorId: 'orch-corrupt', desiredAgents: 1, declaredAt: NOW - 60_000, pid: 'not-a-number', pidStartedAt: {} },
      { orchestratorId: 'orch-null-pid', desiredAgents: 1, declaredAt: NOW - 60_000, pid: null, pidStartedAt: null },
      {
        orchestratorId: claimLedgerId(LIVE_AGENT_CLAIM_TYPE),
        claims: [
          { orchestratorId: 'orch-corrupt', count: 2, claimedAt: NOW - 60_000, pid: 'nope', pidStartedAt: 'also-nope' },
          { orchestratorId: 'orch-nan-pid', count: 1, claimedAt: NOW - 60_000, pid: Number.NaN, pidStartedAt: -1 },
        ],
        grantedTotal: 3,
        declaredAt: NOW - 60_000,
      },
    ];
    // `JSON.stringify` renders `NaN` as `null` — that IS the on-disk reality a
    // reader has to tolerate, so the fixture is honest about it rather than
    // hand-patching the file to contain invalid JSON.
    writeFileSync(filePath, JSON.stringify(entries, null, 2), 'utf8');
  }

  it('does not throw on read — readDibs and getLiveClaimTotal both resolve normally', async () => {
    seedCorruptLedger();

    await expect(readDibs(filePath, NOW, { livenessThresholdMs: LIVENESS_MS })).resolves.toEqual(
      expect.any(Array),
    );
    await expect(
      getLiveClaimTotal(filePath, LIVE_AGENT_CLAIM_TYPE, NOW, DEFAULT_CLAIM_TTL_MS),
    ).resolves.toBe(3);
  });

  it('surfaces the corrupt values verbatim rather than coercing them — Phase 2 owns the classification, not this module', async () => {
    seedCorruptLedger();

    const dibs = await readDibs(filePath, NOW, { livenessThresholdMs: LIVENESS_MS });
    const corrupt = dibs.find((entry) => entry.orchestratorId === 'orch-corrupt');

    expect(corrupt.pid).toBe('not-a-number');
    expect(corrupt.pidStartedAt).toEqual({});
    // Emphatically NOT silently rewritten to a number, and NOT dropped: the
    // string stays a string. (`Number.isNaN` would be useless here — it
    // returns `false` for every non-`number`, so it would pass whatever the
    // coercion behaviour was.)
    expect(typeof corrupt.pid).toBe('string');
  });

  it('a subsequent write against a corrupt-pid ledger still succeeds and does not crash the beat', async () => {
    seedCorruptLedger();

    const result = await claimCapacity(
      filePath,
      {
        type: LIVE_AGENT_CLAIM_TYPE,
        requestedCount: 1,
        orchestratorId: 'orch-new',
        computeAvailableCapacity: availableCapacity100,
        now: NOW,
        pid: CALLER_PID,
        pidStartedAt: CALLER_PID_STARTED_AT,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    expect(result.granted).toBe(1);
    const claims = persistedClaims(LIVE_AGENT_CLAIM_TYPE);
    // The corrupt records are carried forward untouched (still within TTL) —
    // these functions never sanitize or reap them; Phase 2's classifier routes
    // them to "unknown" and the pre-existing TTL remains their only sweep.
    expect(claims.map((claim) => claim.orchestratorId)).toEqual(
      expect.arrayContaining(['orch-corrupt', 'orch-nan-pid', 'orch-new']),
    );
  });
});
