// RED unit + integration tests for the not-yet-implemented
// scripts/agent-resource-management/lib/dispatch.mjs module (Phase 2 —
// "ARM dispatcher: claim-then-admit composition").
//
// MODULE NAMING CHOICE: `dispatch.mjs` (not `queue-dispatch.mjs`). This
// function composes TWO peer modules (queue.mjs's lease primitives +
// coordination-file.mjs's reserveAdmission) rather than extending either
// one, so it belongs in a new, independently-named module — exactly the
// precedent every other file in this directory already sets
// (`allowance.mjs`, `claim-denial.mjs`, `threshold.mjs`, `recon.mjs`: all
// bare, role-named modules under lib/, none prefixed with the name of a
// module they happen to call). A `queue-dispatch.mjs` name would wrongly
// suggest this lives inside/under queue.mjs's ownership, when it in fact
// depends on coordination-file.mjs's lock just as much.
//
// Do NOT add an implementation to make this pass — that's the next agent's
// job (the builder, implementing scripts/agent-resource-management/lib/
// dispatch.mjs in the next phase). Every test below is expected to fail at
// IMPORT TIME (`Cannot find module './dispatch.mjs'` / "does not provide an
// export named 'claimAndAdmitQueueEntry'") because the module does not
// exist yet — mirrors queue-lease.jest.spec.mjs's own "red because the
// export doesn't exist" framing for Phase 1.
//
// ---------------------------------------------------------------------------
// SNAPSHOT/ADMISSION TYPE REUSE — the load-bearing design decision this
// suite encodes for the implementer and the security reviewer after it:
//
// `claimAndAdmitQueueEntry` MUST be called with the SAME
// `LIVE_AGENT_CLAIM_TYPE` / `LIVE_AGENT_ADMISSION_CLAIM_TYPE` pair cli.mjs's
// existing `--desired-agents` beat already passes to `reserveAdmission`
// (cli.mjs ~line 3009-3037), not a new/separate claim-type pair. Reasoning,
// confirmed by reading `computeAvailableCapacity` (cli.mjs ~line 1774-1841)
// and `reserveAdmission` itself (coordination-file.mjs ~line 2281-2450):
//
//   - `LIVE_AGENT_CEILING` (cli.mjs, = 4) is ONE physical ceiling: the
//     literal number of live agents this single 16 GB host can run at once.
//     It is not scoped per code path — a `--desired-agents` beat and a
//     dequeued/dispatched item both ultimately spawn a real OS process that
//     consumes the exact same RAM/disk headroom.
//   - `computeAvailableCapacity`'s own doc comment (cli.mjs ~1796-1824)
//     is explicit that `LIVE_AGENT_CLAIM_TYPE`'s remaining headroom is
//     computed by subtracting BOTH the live `LIVE_AGENT_CLAIM_TYPE` ledger
//     AND the live `LIVE_AGENT_ADMISSION_CLAIM_TYPE` ledger from the shared
//     ceiling — i.e. the whole point of `LIVE_AGENT_ADMISSION_CLAIM_TYPE` as
//     a distinct ledger from `LIVE_AGENT_CLAIM_TYPE` is to let
//     `reserveAdmission` grants from ANY caller (any orchestratorId, any
//     code path) count against the SAME shared ceiling that a
//     `--desired-agents` beat's own capacity probe also respects.
//   - A dispatcher minting a THIRD claim-type pair here would let a
//     dispatched item's admission escape that shared-ceiling accounting
//     entirely — the exact "silent over-admission" failure mode
//     `reserveAdmission`'s own fail-closed contract (and cli.mjs's
//     `catch` -> `liveAgentGrantedCount = 0` fallback) exists to prevent.
//
// Therefore: this suite's fixtures use the SAME two literal strings
// ('live-agent' / 'live-agent-admission') `coordination-file.jest.spec.mjs`
// itself uses for `reserveAdmission` testing (see that file's own
// `RESERVE_ADMISSION_SNAPSHOT_TYPE`/`RESERVE_ADMISSION_ADMISSION_TYPE`
// constants and comment on why they're copied as literals rather than
// imported from cli.mjs — cli.mjs runs `main()` unconditionally at
// module-evaluation time with no `import.meta.url` guard, so importing it
// from a test file would run the real CLI beat against this process). This
// suite does NOT invoke the public `--claim`/`--release` CLI flags (which
// reject `LIVE_AGENT_ADMISSION_CLAIM_TYPE` as internal-only, per cli.mjs
// ~line 1574-1585/1978-1980) — it calls `reserveAdmission` directly via the
// composed function under test, which is the allowed path; the
// internal-only guard is a CLI-flag-layer restriction, orthogonal to this.
//
// ---------------------------------------------------------------------------
// CONTRACT this suite specifies for the implementer (none of this exists in
// dispatch.mjs today — this file IS the spec):
//
//   claimAndAdmitQueueEntry(coordinationFilePath, queueFilePath, params, options) -> Promise<Result>
//
//   params: {
//     orchestratorId: string,   // identity requesting admission — passed to
//                                // reserveAdmission's `orchestratorId`; the
//                                // queue entry's OWN `orchestratorId` field
//                                // (who originally enqueued it) is untouched
//                                // by this call.
//     snapshotType: string,     // -> reserveAdmission's `snapshotType`
//     admissionType: string,    // -> reserveAdmission's `admissionType`
//     ceiling: number,          // -> reserveAdmission's `ceiling`
//     now?: number,             // ONE shared wall-clock instant used for
//                                // BOTH claimQueueEntry's lease stamp and
//                                // reserveAdmission's own `now` — falls back
//                                // to each primitive's own `Date.now()`
//                                // default when omitted.
//   }
//   `requestedCount` is always exactly 1 when calling reserveAdmission — one
//   queue item is one live-agent-admission unit; not caller-configurable in
//   this phase.
//
//   options: {
//     queue?: { claimTtlMs?, lockStalenessMs?, lockMaxAttempts?, lockRetryDelayMs? },
//       // threaded straight through to claimQueueEntry/releaseQueueEntry —
//       // this is the QUEUE lease's own TTL (default
//       // DEFAULT_QUEUE_CLAIM_TTL_MS, 15 min), semantically distinct from
//       // the admission ledger's freshness window below, so kept in its
//       // own sub-bag rather than a single flat `claimTtlMs` that would
//       // collide the two meanings.
//     admission?: { claimTtlMs?, lockStalenessMs?, lockMaxAttempts?, lockRetryDelayMs? },
//       // threaded straight through to reserveAdmission (its own
//       // `claimTtlMs`, e.g. cli.mjs's real call site's
//       // DEFAULT_FRESHNESS_WINDOW_MS, 90s).
//   }
//
//   Execution order (per the Build Plan — inverting this is explicitly
//   rejected):
//     1. claimQueueEntry(queueFilePath, { now, ...options.queue }) — queue
//        lock, released before step 2 begins (these two locks are NEVER
//        held simultaneously).
//     2. If nothing was eligible to claim (claimQueueEntry returned null):
//        return WITHOUT ever calling reserveAdmission.
//     3. reserveAdmission(coordinationFilePath, { snapshotType, admissionType,
//        requestedCount: 1, orchestratorId, ceiling, now }, options.admission)
//        — coordination-file lock, a SEPARATE critical section from step 1.
//     4. granted >= 1 -> return { granted: true, item: <claimed entry>,
//        leaseId: <claimed entry's leaseId> } WITHOUT acking — acking is the
//        caller's job (Phase 4), only after the caller has actually spawned.
//     5. granted === 0 -> releaseQueueEntry(queueFilePath, leaseId,
//        options.queue) immediately, then return
//        { granted: false, reason: 'capacity-denied', item: <released entry> }.
//     6. This function does NOT catch any error either primitive throws
//        (a queue-lock timeout, a coordination-file lock timeout, a
//        reserveAdmission validation TypeError, a QueueCorruptFileError,
//        …) — it propagates unchanged. In particular, an error thrown by
//        reserveAdmission AFTER claimQueueEntry already succeeded is NOT
//        auto-cleaned-up (no automatic releaseQueueEntry on that path) —
//        the claimed item is left leased and recovers only via the queue
//        lease's own TTL expiry, exactly like any other crashed consumer.
//        Fail-closed-by-propagation is the deliberate contract here,
//        mirroring cli.mjs's own `catch -> liveAgentGrantedCount = 0`
//        fail-closed handling for reserveAdmission's OTHER call site — that
//        fail-closed decision belongs to the CALLER (Phase 4's CLI-layer
//        beat), not to this composed primitive.
//
//   Return shape — DISTINGUISHABLE `reason`, chosen over a flat boolean
//   collapse, because it costs one field and materially helps a future
//   caller/log line tell "nothing was even in the queue" apart from
//   "something was queued but capacity is presently exhausted" (the Build
//   Plan frames this as optional/deferrable at the CLI layer, not mandatory
//   here — this suite exercises the richer shape since it's what
//   dispatch.mjs itself returns; a Phase-4 CLI caller remains free to
//   collapse it):
//     - granted:    { granted: true, item, leaseId }
//     - denied:     { granted: false, reason: 'capacity-denied', item }
//     - empty:      { granted: false, reason: 'queue-empty' }
// ---------------------------------------------------------------------------

import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { enqueueItem, peekQueue, claimQueueEntry } from './queue.mjs';
import { claimCapacity, releaseCapacity, DEFAULT_CLAIM_TTL_MS } from './coordination-file.mjs';
import { claimAndAdmitQueueEntry } from './dispatch.mjs';

// Copied literally rather than imported from cli.mjs — see this file's
// header comment (cli.mjs runs main() unconditionally at module-evaluation
// time) and coordination-file.jest.spec.mjs's identical precedent.
const SNAPSHOT_TYPE = 'live-agent';
const ADMISSION_TYPE = 'live-agent-admission';

let workDir;
let coordinationFilePath;
let queueFilePath;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'dispatch-lib-'));
  coordinationFilePath = join(workDir, 'resource-coordination.json');
  queueFilePath = join(workDir, 'resource-queue.json');
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
});

function validQueueItem(overrides = {}) {
  return {
    agentClass: 'orchestrator',
    priority: 'normal',
    commandRef: 'echo hello',
    orchestratorId: 'enqueuer',
    ...overrides,
  };
}

function baseParams(overrides = {}) {
  return {
    orchestratorId: 'dispatcher-a',
    snapshotType: SNAPSHOT_TYPE,
    admissionType: ADMISSION_TYPE,
    ceiling: 4,
    now: 1_000,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// 1. Claims then admits when capacity is free
// ---------------------------------------------------------------------------

describe('claimAndAdmitQueueEntry claims then admits when coordination capacity is free', () => {
  it('returns { granted: true, item, leaseId } for the top-priority eligible entry', async () => {
    await enqueueItem(queueFilePath, validQueueItem({ agentClass: 'low-one', priority: 'low' }));
    await enqueueItem(queueFilePath, validQueueItem({ agentClass: 'high-one', priority: 'high' }));

    const result = await claimAndAdmitQueueEntry(coordinationFilePath, queueFilePath, baseParams());

    expect(result.granted).toBe(true);
    expect(result.item.agentClass).toBe('high-one');
    expect(typeof result.leaseId).toBe('string');
    expect(result.leaseId.length).toBeGreaterThan(0);
    expect(result.item.leaseId).toBe(result.leaseId);
  });

  it('leaves the claimed item leased (not acked) in the queue afterward — acking is the caller\'s job', async () => {
    await enqueueItem(queueFilePath, validQueueItem({ agentClass: 'solo' }));

    const result = await claimAndAdmitQueueEntry(coordinationFilePath, queueFilePath, baseParams());

    const items = await peekQueue(queueFilePath);
    expect(items).toHaveLength(1);
    expect(items[0].leaseId).toBe(result.leaseId);
  });
});

// ---------------------------------------------------------------------------
// 1b. snapshotType invariant guard (review follow-up) — never even
//     touches the queue or coordination file; throws before any I/O.
// ---------------------------------------------------------------------------

describe('claimAndAdmitQueueEntry snapshotType invariant guard', () => {
  it('throws a TypeError without claiming from the queue when snapshotType is not LIVE_AGENT_CLAIM_TYPE', async () => {
    await enqueueItem(queueFilePath, validQueueItem({ agentClass: 'guard-check' }));

    await expect(
      claimAndAdmitQueueEntry(
        coordinationFilePath,
        queueFilePath,
        baseParams({ snapshotType: 'something-else' }),
      ),
    ).rejects.toThrow(
      new TypeError(
        `claimAndAdmitQueueEntry: snapshotType must be LIVE_AGENT_CLAIM_TYPE ("${SNAPSHOT_TYPE}"), ` +
          'received "something-else" — this function\'s default snapshotClaimTtlMs ' +
          '(DEFAULT_CLAIM_TTL_MS) is only correct for the real, hours-lived live-agent ledger; a ' +
          'different snapshotType needs its own considered TTL default, not this one silently applied',
      ),
    );

    const items = await peekQueue(queueFilePath);
    expect(items).toHaveLength(1);
    expect(items[0].leaseId).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// 2. Empty queue — reserveAdmission never even called
// ---------------------------------------------------------------------------

describe('claimAndAdmitQueueEntry against an empty queue', () => {
  it('returns a distinguishable denied shape without ever touching the coordination file', async () => {
    const result = await claimAndAdmitQueueEntry(coordinationFilePath, queueFilePath, baseParams());

    expect(result.granted).toBe(false);
    expect(result.reason).toBe('queue-empty');

    // No coordination-file lock/ledger activity: a competing claimCapacity
    // against the SAME snapshot type must succeed immediately (uncontended),
    // proving reserveAdmission's lock was never taken by the call above.
    const competitor = await claimCapacity(coordinationFilePath, {
      type: SNAPSHOT_TYPE,
      requestedCount: 1,
      orchestratorId: 'competitor',
      computeAvailableCapacity: async () => 4,
      now: 1_000,
    });
    expect(competitor.granted).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// 3. Claims then releases when admission is denied — FIFO place survives
// ---------------------------------------------------------------------------

describe('claimAndAdmitQueueEntry releases its queue claim back when coordination admission is denied', () => {
  it('returns { granted: false, reason: "capacity-denied" } and the entry stays reclaimable with its ORIGINAL enqueuedAt intact', async () => {
    const enqueued = await enqueueItem(queueFilePath, validQueueItem({ agentClass: 'denied-item' }));

    // Exhaust the shared ceiling via a DIFFERENT orchestrator's admission
    // first, so this call's own reserveAdmission grants 0.
    await claimCapacity(coordinationFilePath, {
      type: ADMISSION_TYPE,
      requestedCount: 4,
      orchestratorId: 'other-holder',
      computeAvailableCapacity: async () => 4,
      now: 500,
    });

    const result = await claimAndAdmitQueueEntry(
      coordinationFilePath,
      queueFilePath,
      baseParams({ ceiling: 4, now: 1_000 }),
    );

    expect(result.granted).toBe(false);
    expect(result.reason).toBe('capacity-denied');
    expect(result.item.agentClass).toBe('denied-item');

    // FIFO place proven, not just "unleased": enqueuedAt survives byte-for-byte.
    const items = await peekQueue(queueFilePath);
    expect(items).toHaveLength(1);
    expect(items[0].leaseId == null).toBe(true);
    expect(items[0].enqueuedAt).toBe(enqueued.enqueuedAt);

    // And it is genuinely reclaimable again via a fresh claimQueueEntry call.
    const reclaimed = await claimQueueEntry(queueFilePath, { now: 1_500 });
    expect(reclaimed).not.toBeNull();
    expect(reclaimed.agentClass).toBe('denied-item');
  });
});

// ---------------------------------------------------------------------------
// 4. Admission always reads fresh ceiling/count — two sequential calls under
// changing capacity conditions reflect current state, never a memoized
// first result. Also serves as the "single choke point" proof: since
// dispatch.mjs has no cache of its own, this can only pass if EVERY call
// re-derives capacity from reserveAdmission's own fresh read (there is no
// other path by which the second call's outcome could differ).
// ---------------------------------------------------------------------------

describe('claimAndAdmitQueueEntry reads coordination capacity fresh on every call, never a cached/memoized result', () => {
  it('first call denied (ceiling full), second call — same params, same process — granted after the holder releases', async () => {
    await enqueueItem(queueFilePath, validQueueItem({ agentClass: 'first-attempt' }));
    await enqueueItem(queueFilePath, validQueueItem({ agentClass: 'second-attempt' }));

    await claimCapacity(coordinationFilePath, {
      type: ADMISSION_TYPE,
      requestedCount: 1,
      orchestratorId: 'blocking-holder',
      computeAvailableCapacity: async () => 1,
      now: 500,
    });

    const firstResult = await claimAndAdmitQueueEntry(
      coordinationFilePath,
      queueFilePath,
      baseParams({ ceiling: 1, now: 1_000 }),
    );
    expect(firstResult.granted).toBe(false);
    expect(firstResult.reason).toBe('capacity-denied');

    // Free up the ceiling the OTHER holder occupied.
    await releaseCapacity(coordinationFilePath, {
      type: ADMISSION_TYPE,
      releaseCount: 1,
      orchestratorId: 'blocking-holder',
    });

    const secondResult = await claimAndAdmitQueueEntry(
      coordinationFilePath,
      queueFilePath,
      baseParams({ ceiling: 1, now: 2_000 }),
    );

    expect(secondResult.granted).toBe(true);
    // NOT 'second-attempt': the first call's `capacity-denied` outcome
    // released 'first-attempt' back with its ORIGINAL `enqueuedAt` intact
    // (test 3, above, locks in this exact invariant — release preserves
    // FIFO place) — so 'first-attempt' is still the oldest eligible entry
    // and legitimately wins the FIFO race again once capacity frees up.
    // What this test actually proves ("fresh, never-memoized capacity read")
    // is that the SAME item, denied on the first call, is granted on the
    // second once the blocking holder releases — a stronger proof of
    // freshness than swapping to a different item would have been.
    expect(secondResult.item.agentClass).toBe('first-attempt');
  });
});

// ---------------------------------------------------------------------------
// 5. Simulated crash between claim and reserveAdmission
// ---------------------------------------------------------------------------
//
// Forced via a REAL rejection from reserveAdmission itself — passing
// `snapshotType === admissionType` triggers reserveAdmission's own
// documented pre-lock validation TypeError ("snapshotType and admissionType
// must be distinct claim ledger types") — rather than a jest module mock.
// This codebase has no established ESM `jest.unstable_mockModule` idiom for
// these lib/ specs (grep confirms zero prior uses across
// scripts/agent-resource-management/); every existing coordination-file.mjs
// test that needs a slow/failing critical section instead drives a REAL
// rejection through the module's own real validation or its documented
// `testInjectedDelayMs` hook. A genuine thrown error exercises this
// function's actual propagation code path more faithfully than a mock
// would, with no new mocking machinery introduced into this suite.
// ---------------------------------------------------------------------------

describe('a rejection from reserveAdmission AFTER claimQueueEntry already succeeded propagates uncaught, and the claim recovers via queue-lease TTL', () => {
  it('rejects the whole call, leaves the item leased (not released), and the item becomes reclaimable once the QUEUE lease TTL elapses', async () => {
    await enqueueItem(queueFilePath, validQueueItem({ agentClass: 'crash-between-claim-and-admit' }));

    const crashParams = baseParams({
      snapshotType: SNAPSHOT_TYPE,
      admissionType: SNAPSHOT_TYPE, // deliberately identical -> real reserveAdmission TypeError
      now: 1_000,
    });

    await expect(
      claimAndAdmitQueueEntry(coordinationFilePath, queueFilePath, crashParams, {
        queue: { claimTtlMs: 1_000 },
      }),
    ).rejects.toThrow(/snapshotType and admissionType must be distinct/);

    // The item is still claimed (leased), not released back — this function
    // does not auto-clean-up on an unexpected throw.
    const itemsRightAfter = await peekQueue(queueFilePath);
    expect(itemsRightAfter).toHaveLength(1);
    expect(itemsRightAfter[0].leaseId != null).toBe(true);

    // Fast-forward the injectable `now` strictly past the QUEUE lease's own
    // TTL (1_000ms, set above) — no real wait required.
    const reclaimed = await claimQueueEntry(queueFilePath, { now: 5_000, claimTtlMs: 1_000 });

    expect(reclaimed).not.toBeNull();
    expect(reclaimed.agentClass).toBe('crash-between-claim-and-admit');
    expect(reclaimed.leaseId).not.toBe(itemsRightAfter[0].leaseId);
  });
});

// ---------------------------------------------------------------------------
// 6. Simulated crash between grant and ack — caller's job, not this
// function's; the queue lease recovers via TTL with no lingering
// side effect from the earlier grant.
// ---------------------------------------------------------------------------

describe('after claimAndAdmitQueueEntry returns granted:true, an un-acked lease recovers via queue-lease TTL with no double-admission side effect', () => {
  it('a fresh claimQueueEntry call reclaims the same item (new leaseId) once the queue lease TTL elapses, with the earlier grant left entirely to the caller to reconcile', async () => {
    await enqueueItem(queueFilePath, validQueueItem({ agentClass: 'never-acked' }));

    const result = await claimAndAdmitQueueEntry(
      coordinationFilePath,
      queueFilePath,
      baseParams({ now: 1_000 }),
      { queue: { claimTtlMs: 1_000 } },
    );
    expect(result.granted).toBe(true);

    // Caller crashes before ever calling ackQueueEntry/releaseQueueEntry.
    // This composed function itself did nothing further — no automatic ack,
    // no automatic release.
    const stillLeased = await peekQueue(queueFilePath);
    expect(stillLeased).toHaveLength(1);
    expect(stillLeased[0].leaseId).toBe(result.leaseId);

    const reclaimed = await claimQueueEntry(queueFilePath, { now: 5_000, claimTtlMs: 1_000 });

    expect(reclaimed).not.toBeNull();
    expect(reclaimed.agentClass).toBe('never-acked');
    expect(reclaimed.leaseId).not.toBe(result.leaseId);
  });
});

// ---------------------------------------------------------------------------
// 7. Two orchestrators racing the same queue+coordination-file pair, small
// ceiling — never both admitted, never exceed the ceiling.
// ---------------------------------------------------------------------------

describe('two orchestrators concurrently racing claimAndAdmitQueueEntry against the same queue+coordination-file pair, ceiling=1', () => {
  it('exactly one of two concurrent callers is granted when two distinct queue items are available but the ceiling only allows one', async () => {
    await enqueueItem(queueFilePath, validQueueItem({ agentClass: 'item-one' }));
    await enqueueItem(queueFilePath, validQueueItem({ agentClass: 'item-two' }));

    const [resultA, resultB] = await Promise.all([
      claimAndAdmitQueueEntry(
        coordinationFilePath,
        queueFilePath,
        baseParams({ orchestratorId: 'orc-a', ceiling: 1, now: 1_000 }),
      ),
      claimAndAdmitQueueEntry(
        coordinationFilePath,
        queueFilePath,
        baseParams({ orchestratorId: 'orc-b', ceiling: 1, now: 1_000 }),
      ),
    ]);

    const results = [resultA, resultB];
    const grantedResults = results.filter((result) => result.granted === true);
    const deniedResults = results.filter((result) => result.granted === false);

    expect(grantedResults).toHaveLength(1);
    expect(deniedResults).toHaveLength(1);
    expect(deniedResults[0].reason).toBe('capacity-denied');

    // Both queue items were legitimately claimed (queue-lock contention is
    // independent of coordination-ceiling contention) — the denied caller's
    // item was released back, so the queue still holds BOTH entries: one
    // leased (the granted caller's, not yet acked) and one unleased (the
    // denied caller's, released back).
    const items = await peekQueue(queueFilePath);
    expect(items).toHaveLength(2);
    const leasedCount = items.filter((item) => item.leaseId != null).length;
    const unleasedCount = items.filter((item) => item.leaseId == null).length;
    expect(leasedCount).toBe(1);
    expect(unleasedCount).toBe(1);
  });

  it('never jointly exceeds the ceiling even when both callers request admission for the same single available item', async () => {
    // Only ONE queue item this time: queue-lock contention alone already
    // guarantees a single winner at the claimQueueEntry step, so this test
    // isolates that the LOSER of the queue race (who gets `null` from
    // claimQueueEntry) never even calls reserveAdmission, and the ceiling
    // is never exceeded by definition of only one winner existing.
    await enqueueItem(queueFilePath, validQueueItem({ agentClass: 'sole-item' }));

    const [resultA, resultB] = await Promise.all([
      claimAndAdmitQueueEntry(
        coordinationFilePath,
        queueFilePath,
        baseParams({ orchestratorId: 'orc-a', ceiling: 1, now: 1_000 }),
      ),
      claimAndAdmitQueueEntry(
        coordinationFilePath,
        queueFilePath,
        baseParams({ orchestratorId: 'orc-b', ceiling: 1, now: 1_000 }),
      ),
    ]);

    const results = [resultA, resultB];
    const grantedResults = results.filter((result) => result.granted === true);
    expect(grantedResults).toHaveLength(1);

    const otherResult = results.find((result) => result.granted === false);
    expect(otherResult).toBeDefined();
    // The loser never claimed a queue item at all (queue-empty), OR did
    // claim but then lost the admission race — either is a valid single-
    // winner outcome; what's asserted is strictly one winner, never two.
    expect(['queue-empty', 'capacity-denied']).toContain(otherResult.reason);
  });
});

// ---------------------------------------------------------------------------
// 8. `releaseQueueEntry` returns null on the denial path — the entry this
// call itself claimed had its lease reclaimed (new leaseId) by a DIFFERENT
// claimQueueEntry call before this call's own release fires (pre-PR review
// finding, fix round). `claimAndAdmitQueueEntry` must fall back to
// its own originally-claimed entry rather than surface `item: null`, per
// this function's own doc comment (dispatch.mjs, "released ?? claimed").
//
// Forced via `reserveAdmission`'s real `testInjectedDelayMs` hook (same
// hook `coordination-file.jest.spec.mjs` already exercises for other
// slow-critical-section races) — NOT a mock: while this call's own
// `reserveAdmission` is deliberately held open (real wall-clock delay), a
// genuinely separate `claimQueueEntry` call reclaims the SAME queue entry
// (its lease already past a short `claimTtlMs`) via the queue file's own
// independent lock, which coordination-file's delay does not hold.
// ---------------------------------------------------------------------------

describe('claimAndAdmitQueueEntry falls back to its own claimed item when releaseQueueEntry finds its leaseId already superseded', () => {
  it("returns the originally-claimed item's identity/content on capacity-denied, even though the underlying release call itself returned null", async () => {
    await enqueueItem(queueFilePath, validQueueItem({ agentClass: 'raced-release' }));

    // Exhaust the shared ceiling so this call's own reserveAdmission denies.
    await claimCapacity(coordinationFilePath, {
      type: ADMISSION_TYPE,
      requestedCount: 4,
      orchestratorId: 'other-holder',
      computeAvailableCapacity: async () => 4,
      now: 500,
    });

    const [result, reclaimed] = await Promise.all([
      claimAndAdmitQueueEntry(
        coordinationFilePath,
        queueFilePath,
        baseParams({ ceiling: 4, now: 1_000 }),
        {
          queue: { claimTtlMs: 50 },
          // Real wall-clock delay inside reserveAdmission's own critical
          // section — long enough for the concurrent reclaim below to run
          // and complete first.
          admission: { testInjectedDelayMs: 150 },
        },
      ),
      (async () => {
        // Give claimAndAdmitQueueEntry's own claimQueueEntry step (and the
        // start of its reserveAdmission call) a moment to run first, so the
        // queue file already holds the FIRST lease before this reclaims it.
        await new Promise((resolve) => setTimeout(resolve, 30));
        // `now: 1_100` is > the first claim's `leasedAt` (1_000) + the 50ms
        // `claimTtlMs` set above, so the existing lease already reads as
        // expired — this call is a genuine reclaim, not a queue-empty read.
        return claimQueueEntry(queueFilePath, { now: 1_100, claimTtlMs: 50 });
      })(),
    ]);

    // The concurrent reclaim genuinely happened and produced a NEW lease.
    expect(reclaimed).not.toBeNull();
    expect(reclaimed.agentClass).toBe('raced-release');

    // claimAndAdmitQueueEntry's own release of its (now-superseded) leaseId
    // found nothing to release — yet still reports the denial with the
    // originally-claimed item's content, not `item: null`.
    expect(result.granted).toBe(false);
    expect(result.reason).toBe('capacity-denied');
    expect(result.item).not.toBeNull();
    expect(result.item.agentClass).toBe('raced-release');
    // The returned item still carries ITS OWN (now-stale) leaseId — proof
    // this came from the `claimed` fallback, not from a fresh read of the
    // (reclaimed) on-disk entry, which now carries a different leaseId.
    expect(result.item.leaseId).not.toBe(reclaimed.leaseId);

    // The queue still shows the RECLAIMED entry (reclaimed.leaseId), proving
    // the stale release call above changed nothing on disk.
    const itemsAfter = await peekQueue(queueFilePath);
    expect(itemsAfter).toHaveLength(1);
    expect(itemsAfter[0].leaseId).toBe(reclaimed.leaseId);
  });
});

// ---------------------------------------------------------------------------
// Phase 2 (FIXED — commit 43063437) — claimAndAdmitQueueEntry now
// defaults `options.admission.snapshotClaimTtlMs` to `DEFAULT_CLAIM_TTL_MS`
// internally when the caller omits it (see this module's own
// `reserveAdmission` call: `admissionOptions` fills in that default before
// being spread with `options.admission`). cli.mjs's `handleDequeueIfCapacity`
// only ever passes `{ admission: { claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS } }`
// (no explicit `snapshotClaimTtlMs`) — mirrored here as
// `DEFAULT_FRESHNESS_WINDOW_MS` literal (90s), matching
// ./live-agent-ttl-reconciliation.jest.spec.mjs's own precedent for not
// importing it from cli.mjs (cli.mjs runs `main()` unconditionally at
// module-evaluation time).
//
// HISTORICAL NOTE: before commit 43063437 landed the default above, this
// test was RED BY CONSTRUCTION and failed for the SAME underlying reason as
// ./live-agent-ttl-reconciliation.jest.spec.mjs's Phase 0 test: without a
// `snapshotClaimTtlMs` reaching `reserveAdmission`, the SNAPSHOT_TYPE ledger
// was pruned against the 90s admission-freshness window instead of the real
// 6h claim TTL, so a live-agent claim aged past 90s (but still well inside
// its real 6h TTL) went invisible to the ceiling check and was
// over-admitted on top of. The test now passes with the default in place.
// ---------------------------------------------------------------------------

const DEFAULT_FRESHNESS_WINDOW_MS = 90 * 1000; // mirrors cli.mjs's real DEFAULT_FRESHNESS_WINDOW_MS export.

describe(
  ' Phase 2 — claimAndAdmitQueueEntry (the --dequeue-if-capacity path) must not over-admit on top of ' +
    'a live-agent claim aged past the 90s admission-freshness window but still inside its real 6h claim TTL',
  () => {
    it(
      'a real live-agent claim (SNAPSHOT_TYPE) aged 91s still counts against the ceiling for a dequeue-and-' +
        'admit decision — must deny, not grant',
      async () => {
        const t0 = 2_000_000;

        // A real live-agent claim filling the whole ceiling (4), claimed
        // against its own real 6h TTL — mirrors handleClaim's real call
        // shape (cli.mjs's `--claim=live-agent:N`).
        const claimResult = await claimCapacity(
          coordinationFilePath,
          {
            type: SNAPSHOT_TYPE,
            requestedCount: 4,
            orchestratorId: 'orc-real-claim-holder',
            computeAvailableCapacity: async () => 4,
            now: t0,
          },
          { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
        );
        expect(claimResult.granted).toBe(4);

        await enqueueItem(queueFilePath, validQueueItem({ agentClass: 'dequeue-if-capacity-item' }));

        // At T + 91s — past the 90s admission-freshness window, nowhere near
        // the 6h claim TTL — a dispatcher attempts to dequeue-and-admit
        // against the SAME shared ceiling, using the EXACT options shape
        // cli.mjs's `handleDequeueIfCapacity` passes today (no
        // `snapshotClaimTtlMs`).
        const result = await claimAndAdmitQueueEntry(
          coordinationFilePath,
          queueFilePath,
          baseParams({ ceiling: 4, now: t0 + 91_000, orchestratorId: 'orc-real-dispatcher' }),
          { admission: { claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS } },
        );

        // THE INVARIANT: the 4 already-live claims are still live under
        // their real 6h TTL at T + 91s, so the ceiling has ZERO genuine
        // headroom left — this dequeue-and-admit decision must be denied.
        //
        // PRE-FIX (commit 43063437) BEHAVIOUR: `options.admission` was
        // forwarded to `reserveAdmission` verbatim with no
        // `snapshotClaimTtlMs`, so SNAPSHOT_TYPE's ledger was pruned against
        // the 90s freshness window at T + 91s, the 4 live claims were pruned
        // away, and this call incorrectly granted. Fixed by defaulting
        // `snapshotClaimTtlMs` to `DEFAULT_CLAIM_TTL_MS` internally — see
        // this suite's file-header note above.
        expect(result.granted).toBe(false);
        expect(result.reason).toBe('capacity-denied');
      },
    );
  },
);
