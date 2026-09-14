// RED unit tests for the not-yet-implemented claim/lease surface on top of
// scripts/agent-resource-management/lib/queue.mjs (Phase 1 —
// "Dispatcher drain: claim/lease state for queue entries").
//
// Direct-import unit tests, mirroring ./queue.jest.spec.mjs's idiom exactly:
// real fs, real temp directories (mkdtempSync under os.tmpdir()), real
// concurrency via Promise.all — never a mocked filesystem, because the whole
// point of this surface is multi-consumer atomicity/lock correctness against
// the REAL filesystem, exactly like `dequeueItem`'s own concurrency proof.
//
// This is a SEPARATE file from ./queue.jest.spec.mjs (rather than an
// extension of it) because that file's own header frames itself as the
// complete, self-contained contract for Phase 1's `enqueueItem`/`peekQueue`/
// `dequeueItem`/`resolveQueueFilePath` surface — adding three more
// exports' worth of contract and ~20 scenarios to an already-1125-line file
// would blur that boundary. This file is the equivalent contract-and-spec
// document for the NEW claim/lease surface only.
//
// `claimQueueEntry`/`ackQueueEntry`/`releaseQueueEntry` do NOT exist yet as
// of this commit — every test below is expected to fail at import time
// (`does not provide an export named 'claimQueueEntry'` or similar) or, once
// added as stubs, with a "<name> is not a function"/behavioral mismatch. Do
// not add an implementation to make this pass — the builder implements these
// three functions in ./queue.mjs in the next phase to turn this green.
//
// ---------------------------------------------------------------------------
// Contract this suite specifies for the implementer (none of this exists in
// queue.mjs today — this file IS the spec). Mirrors
// coordination-file.mjs's `reserveAdmission`/`pruneExpiredClaims` lease-TTL
// idiom (options.claimTtlMs, an injectable `now`, "live if
// now - leasedAt <= claimTtlMs, else expired") rather than inventing a new
// convention — see that module's `pruneExpiredClaims` doc comment.
//
//   DEFAULT_QUEUE_CLAIM_TTL_MS -> number
//     Exported constant backing `claimQueueEntry`'s default `claimTtlMs` when
//     the caller doesn't override it via `options.claimTtlMs`. A queue lease
//     is scoped to "one dispatcher's processing attempt of one work item",
//     not to an admission-ledger's multi-hour hold, so this suite pins it
//     considerably shorter than coordination-file.mjs's
//     `DEFAULT_CLAIM_TTL_MS` (6 hours) — this file pins the EXACT value
//     `claimQueueEntry`/its default must honor at 15 minutes
//     (`15 * 60 * 1000` = 900_000 ms), the chosen bound, exported for the
//     implementer/reviewer to reconcile against or reuse directly rather
//     than restating the number.
//
//   claimQueueEntry(filePath, options?) -> Promise<object|null>
//     Atomically marks the TOP entry that is currently ELIGIBLE — i.e. has no
//     lease at all, OR has a lease whose `leasedAt` is older than
//     `claimTtlMs` (an EXPIRED lease) — as leased, under the SAME
//     `withQueueLock` critical section `enqueueItem`/`dequeueItem` already
//     use, and returns it. Ordering among eligible entries is the exact same
//     `sortQueueEntries` order `peekQueue`/`dequeueItem` use (priority desc,
//     then enqueuedAt asc) — an entry currently under an UNEXPIRED lease is
//     skipped entirely, never returned, even if it would otherwise be top.
//     The returned (and now-persisted) entry carries two lease fields not
//     present on a plain enqueued entry: a freshly-generated, unique
//     `leaseId` (string) and a `leasedAt` timestamp (server-assigned, honors
//     an injectable `options.now` for deterministic testing — mirrors
//     `reserveAdmission`'s own `now` injection convention exactly, just
//     threaded via `options` here since there is no other params bag).
//     Reclaiming an entry whose PRIOR lease has expired assigns a BRAND NEW
//     `leaseId` (the prior leaseId becomes permanently stale — see
//     `ackQueueEntry`'s stale-leaseId contract below) and a fresh
//     `leasedAt`, but the entry's original `enqueuedAt` is NEVER touched —
//     mirrors `releaseQueueEntry`'s own enqueuedAt-preservation contract, for
//     the identical FIFO/aging reason. Returns `null` — never throws — when
//     the queue is empty, missing, or every entry is currently under an
//     unexpired lease (nothing eligible to claim). Propagates
//     `QueueCorruptFileError` for a corrupt on-disk file, identically to
//     `dequeueItem`. Two concurrent `claimQueueEntry` calls racing the same
//     queue never both receive the same entry — same single-winner guarantee
//     `dequeueItem`'s own concurrency test proves for removal.
//
//   ackQueueEntry(filePath, leaseId, options?) -> Promise<object|null>
//     Atomically and PERMANENTLY removes the single entry whose CURRENT
//     on-disk `leaseId` exactly matches the given `leaseId` — the "the
//     leased work item finished successfully" path. Returns the removed
//     entry. If no on-disk entry currently carries this exact `leaseId` —
//     including the documented STALE-LEASE case, where `leaseId` belonged to
//     an entry that has SINCE been reclaimed by a fresh `claimQueueEntry`
//     call (TTL expiry then re-claim, per that function's contract above,
//     assigns a brand new `leaseId`) — this is a SAFE NO-OP: returns `null`,
//     the queue is left completely unmodified, and nothing throws. This
//     mirrors `dequeueItem`'s own "nothing matched, return null rather than
//     throw" convention for the equivalent "no work to do" case, rather than
//     introducing a new throw-on-not-found convention this module has no
//     other precedent for. (Contract choice, made explicit here per this
//     phase's instructions: the alternative — throwing a typed
//     "lease not found" error — was considered and rejected specifically
//     because a late/duplicate ack racing a legitimate reclaim is an
//     expected, non-exceptional occurrence in a crash-recycling lease
//     system, not a caller bug.) Propagates `QueueCorruptFileError` for a
//     corrupt on-disk file, identically to `dequeueItem`/`claimQueueEntry`.
//
//   releaseQueueEntry(filePath, leaseId, options?) -> Promise<object|null>
//     Atomically clears the lease (both `leaseId` and `leasedAt`) on the
//     single entry whose CURRENT on-disk `leaseId` exactly matches the given
//     `leaseId`, returning the entry to normal-claimable state — the
//     "crashed/gave up mid-processing, recycle it" path. Returns the
//     UPDATED entry (lease fields cleared). Critically, the entry's
//     ORIGINAL `enqueuedAt` — stamped once, at the original `enqueueItem`
//     call, long before this lease ever existed — is preserved byte-for-byte
//     and NEVER reset to "now": a crash-recycled item must not lose its
//     FIFO/aging place in the queue by being treated as freshly enqueued.
//     If no on-disk entry currently carries this exact `leaseId` (same
//     stale-leaseId scenario `ackQueueEntry` documents), this is the same
//     safe no-op: returns `null`, queue left unmodified, never throws.
//     Propagates `QueueCorruptFileError` for a corrupt on-disk file,
//     identically to the other two.
// ---------------------------------------------------------------------------

import { mkdtempSync, rmSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  enqueueItem,
  peekQueue,
  dequeueItem,
  claimQueueEntry,
  ackQueueEntry,
  releaseQueueEntry,
  bindPidToLease,
  DEFAULT_QUEUE_CLAIM_TTL_MS,
  QueueCorruptFileError,
} from './queue.mjs';

let workDir;
let queueFilePath;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'queue-lease-lib-'));
  queueFilePath = join(workDir, 'resource-queue.json');
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
});

function validItem(overrides = {}) {
  return {
    agentClass: 'orchestrator',
    priority: 'normal',
    commandRef: 'echo hello',
    orchestratorId: 'test-orchestrator',
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// 0. Pinned default TTL constant
// ---------------------------------------------------------------------------

describe('DEFAULT_QUEUE_CLAIM_TTL_MS', () => {
  it('is exported as 900_000 (15 minutes) — the chosen bound this suite pins', () => {
    expect(DEFAULT_QUEUE_CLAIM_TTL_MS).toBe(15 * 60 * 1000);
  });
});

// ---------------------------------------------------------------------------
// 1. claimQueueEntry claims the top eligible entry and marks it leased
// ---------------------------------------------------------------------------

describe('claimQueueEntry claims the top eligible entry and marks it leased', () => {
  it('returns the highest-priority entry, stamped with a leaseId and leasedAt, and peekQueue reflects the lease', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'low-one', priority: 'low' }));
    await enqueueItem(queueFilePath, validItem({ agentClass: 'high-one', priority: 'high' }));

    const claimed = await claimQueueEntry(queueFilePath, { now: 1_000 });

    expect(claimed).not.toBeNull();
    expect(claimed.agentClass).toBe('high-one');
    expect(typeof claimed.leaseId).toBe('string');
    expect(claimed.leaseId.length).toBeGreaterThan(0);
    expect(claimed.leasedAt).toBe(1_000);

    const items = await peekQueue(queueFilePath);
    const persisted = items.find((item) => item.agentClass === 'high-one');
    expect(persisted.leaseId).toBe(claimed.leaseId);
    expect(persisted.leasedAt).toBe(1_000);
  });

  it('claiming an already-empty queue returns null rather than throwing', async () => {
    await expect(claimQueueEntry(queueFilePath, { now: 1_000 })).resolves.toBeNull();
  });

  it('skips a low-priority entry currently under an unexpired lease and claims the next-eligible entry instead', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'already-leased', priority: 'high' }));
    await enqueueItem(queueFilePath, validItem({ agentClass: 'eligible', priority: 'normal' }));

    await claimQueueEntry(queueFilePath, { now: 1_000 });
    const secondClaim = await claimQueueEntry(queueFilePath, { now: 1_500 });

    expect(secondClaim).not.toBeNull();
    expect(secondClaim.agentClass).toBe('eligible');
  });
});

// ---------------------------------------------------------------------------
// 1b. dequeueItem is lease-aware (pre-PR review finding #1) — two
// consumers sharing one queue file, one using --dequeue (dequeueItem) and
// one using --dequeue-if-capacity (claimQueueEntry under the hood), must
// never both receive the same item.
// ---------------------------------------------------------------------------

describe('dequeueItem is lease-aware, exactly like claimQueueEntry', () => {
  it('skips an entry currently under an unexpired lease and dequeues the next-eligible entry instead', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'already-leased', priority: 'high' }));
    await enqueueItem(queueFilePath, validItem({ agentClass: 'eligible', priority: 'normal' }));

    await claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 60_000 });
    const dequeued = await dequeueItem(queueFilePath, { now: 1_500, claimTtlMs: 60_000 });

    expect(dequeued).not.toBeNull();
    expect(dequeued.agentClass).toBe('eligible');
  });

  it('returns null — never the leased entry — when EVERY entry is currently under an unexpired lease', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'only-item', priority: 'normal' }));
    await claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 60_000 });

    await expect(dequeueItem(queueFilePath, { now: 1_500, claimTtlMs: 60_000 })).resolves.toBeNull();
  });

  it('a concurrent --dequeue (dequeueItem) and --dequeue-if-capacity-style claim (claimQueueEntry) on the same single-item queue never both receive it', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'only-item', priority: 'normal' }));

    const [dequeued, claimed] = await Promise.all([
      dequeueItem(queueFilePath, { now: 1_000, claimTtlMs: 60_000 }),
      claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 60_000 }),
    ]);

    // Exactly one of the two racing calls wins the single item; the other
    // gets null (dequeueItem: nothing eligible; claimQueueEntry: nothing
    // eligible, or the queue is now empty).
    const winners = [dequeued, claimed].filter((result) => result !== null);
    expect(winners).toHaveLength(1);
    expect(winners[0].agentClass).toBe('only-item');
  });

  it('once a lease expires past its TTL, dequeueItem can dequeue the now-eligible (previously leased) entry', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'only-item', priority: 'normal' }));
    await claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 1_000 });

    const dequeued = await dequeueItem(queueFilePath, { now: 5_000, claimTtlMs: 1_000 });

    expect(dequeued).not.toBeNull();
    expect(dequeued.agentClass).toBe('only-item');
  });
});

// ---------------------------------------------------------------------------
// 2. Second claim before TTL expiry does not double-claim
// ---------------------------------------------------------------------------

describe('a second claimQueueEntry call before TTL expiry does not double-claim the same entry', () => {
  it('returns null when the only entry is already under an unexpired lease', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'only-item', priority: 'normal' }));

    const first = await claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 60_000 });
    expect(first.agentClass).toBe('only-item');

    const second = await claimQueueEntry(queueFilePath, { now: 5_000, claimTtlMs: 60_000 });

    expect(second).toBeNull();
  });

  it('the entry remains under the FIRST leaseId in the persisted queue after the skipped second attempt', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'only-item', priority: 'normal' }));

    const first = await claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 60_000 });
    await claimQueueEntry(queueFilePath, { now: 5_000, claimTtlMs: 60_000 });

    const items = await peekQueue(queueFilePath);
    expect(items[0].leaseId).toBe(first.leaseId);
  });
});

// ---------------------------------------------------------------------------
// 3. Claim after TTL expiry re-claims the same (now-expired-lease) entry
// ---------------------------------------------------------------------------

describe('claimQueueEntry re-claims an entry once its prior lease has expired', () => {
  it('returns the same entry (by agentClass/commandRef) with a BRAND NEW leaseId once claimTtlMs has elapsed', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'only-item', priority: 'normal' }));

    const firstLease = await claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 1_000 });
    // now - leasedAt = 1_000 - 1_000 = 0 <= claimTtlMs(1_000): still live at
    // this instant, then advance strictly past the TTL window below.
    const reclaimed = await claimQueueEntry(queueFilePath, { now: 5_000, claimTtlMs: 1_000 });

    expect(reclaimed).not.toBeNull();
    expect(reclaimed.agentClass).toBe('only-item');
    expect(reclaimed.leaseId).not.toBe(firstLease.leaseId);
    expect(reclaimed.leasedAt).toBe(5_000);
  });

  it('preserves the entry\'s original enqueuedAt across a TTL-expiry reclaim — never bumped to the reclaim time', async () => {
    const persisted = await enqueueItem(queueFilePath, validItem({ agentClass: 'only-item', priority: 'normal' }));

    await claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 1_000 });
    const reclaimed = await claimQueueEntry(queueFilePath, { now: 5_000, claimTtlMs: 1_000 });

    expect(reclaimed.enqueuedAt).toBe(persisted.enqueuedAt);
  });
});

// ---------------------------------------------------------------------------
// 4. ackQueueEntry removes a leased entry permanently
// ---------------------------------------------------------------------------

describe('ackQueueEntry removes a leased entry permanently by its lease identity', () => {
  it('removes the entry matching the current leaseId and returns it', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'to-ack', priority: 'normal' }));
    const claimed = await claimQueueEntry(queueFilePath, { now: 1_000 });

    const acked = await ackQueueEntry(queueFilePath, claimed.leaseId);

    expect(acked.agentClass).toBe('to-ack');
    const remaining = await peekQueue(queueFilePath);
    expect(remaining).toHaveLength(0);
  });

  it('does not disturb OTHER entries in the queue', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'to-ack', priority: 'high' }));
    await enqueueItem(queueFilePath, validItem({ agentClass: 'untouched', priority: 'low' }));
    const claimed = await claimQueueEntry(queueFilePath, { now: 1_000 });

    await ackQueueEntry(queueFilePath, claimed.leaseId);

    const remaining = await peekQueue(queueFilePath);
    expect(remaining.map((item) => item.agentClass)).toEqual(['untouched']);
  });
});

// ---------------------------------------------------------------------------
// 5. ack on a stale leaseId (after TTL expiry + reclaim by someone else) —
// EXPLICIT contract: safe no-op, returns null, queue left unmodified.
// ---------------------------------------------------------------------------

describe('ackQueueEntry on a STALE leaseId (already reclaimed by someone else after TTL expiry) is a safe no-op — CONTRACT: returns null, never throws, queue left unmodified', () => {
  it('returns null for a leaseId that has since been superseded by a fresh claim', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'only-item', priority: 'normal' }));

    const staleLease = await claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 1_000 });
    // TTL expires; a different consumer reclaims the same entry, minting a
    // brand new leaseId — staleLease.leaseId is now permanently stale.
    const freshLease = await claimQueueEntry(queueFilePath, { now: 5_000, claimTtlMs: 1_000 });
    expect(freshLease.leaseId).not.toBe(staleLease.leaseId);

    const staleAckResult = await ackQueueEntry(queueFilePath, staleLease.leaseId);

    expect(staleAckResult).toBeNull();
  });

  it('leaves the reclaimed entry (now under the fresh leaseId) fully intact in the queue after the stale ack no-op', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'only-item', priority: 'normal' }));

    const staleLease = await claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 1_000 });
    const freshLease = await claimQueueEntry(queueFilePath, { now: 5_000, claimTtlMs: 1_000 });

    await ackQueueEntry(queueFilePath, staleLease.leaseId);

    const remaining = await peekQueue(queueFilePath);
    expect(remaining).toHaveLength(1);
    expect(remaining[0].agentClass).toBe('only-item');
    expect(remaining[0].leaseId).toBe(freshLease.leaseId);
  });

  it('ackQueueEntry against a leaseId that was never issued at all is also a null no-op', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'untouched', priority: 'normal' }));

    await expect(ackQueueEntry(queueFilePath, 'never-issued-lease-id')).resolves.toBeNull();

    const remaining = await peekQueue(queueFilePath);
    expect(remaining).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// 6. releaseQueueEntry clears the lease and preserves the original enqueuedAt
// ---------------------------------------------------------------------------

describe('releaseQueueEntry clears the lease and returns the entry to claimable state, preserving the ORIGINAL enqueuedAt', () => {
  it('clears leaseId/leasedAt so a subsequent claimQueueEntry can claim it again immediately', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'crash-recycled', priority: 'normal' }));
    const claimed = await claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 60_000 });

    const released = await releaseQueueEntry(queueFilePath, claimed.leaseId);

    expect(released.agentClass).toBe('crash-recycled');
    expect(released.leaseId == null).toBe(true);
    expect(released.leasedAt == null).toBe(true);

    // Immediately re-claimable — proves the lease was genuinely cleared, not
    // merely returned-but-still-persisted-as-leased.
    const reclaimed = await claimQueueEntry(queueFilePath, { now: 1_500, claimTtlMs: 60_000 });
    expect(reclaimed).not.toBeNull();
    expect(reclaimed.agentClass).toBe('crash-recycled');
  });

  it('preserves the entry\'s ORIGINAL enqueuedAt exactly — never reset to "now" — so a crash-recycled item keeps its FIFO/aging place', async () => {
    const persisted = await enqueueItem(
      queueFilePath,
      validItem({ agentClass: 'crash-recycled', priority: 'normal' }),
    );
    const claimed = await claimQueueEntry(queueFilePath, { now: 999_999, claimTtlMs: 60_000 });

    const released = await releaseQueueEntry(queueFilePath, claimed.leaseId);

    expect(released.enqueuedAt).toBe(persisted.enqueuedAt);

    const items = await peekQueue(queueFilePath);
    expect(items[0].enqueuedAt).toBe(persisted.enqueuedAt);
  });

  it('a released (crash-recycled) entry still sorts by its ORIGINAL enqueuedAt ahead of a genuinely newer same-priority entry — proves the FIFO place survives release, not just the raw field value', async () => {
    const original = await enqueueItem(
      queueFilePath,
      validItem({ agentClass: 'original-oldest', priority: 'normal' }),
    );
    const claimed = await claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 60_000 });
    await releaseQueueEntry(queueFilePath, claimed.leaseId);

    // A brand-new item enqueued AFTER the release must still sort behind the
    // recycled one, because the recycled one's enqueuedAt predates it.
    await enqueueItem(queueFilePath, validItem({ agentClass: 'genuinely-newer', priority: 'normal' }));

    const items = await peekQueue(queueFilePath);
    expect(items.map((item) => item.agentClass)).toEqual(['original-oldest', 'genuinely-newer']);
    expect(original.enqueuedAt).toBeDefined();
  });

  it('releaseQueueEntry on a stale/never-issued leaseId is also a safe no-op — returns null, queue left unmodified', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'untouched', priority: 'normal' }));

    await expect(releaseQueueEntry(queueFilePath, 'never-issued-lease-id')).resolves.toBeNull();

    const remaining = await peekQueue(queueFilePath);
    expect(remaining).toHaveLength(1);
    expect(remaining[0].leaseId == null).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 7. Corrupt queue file — claimQueueEntry/ackQueueEntry/releaseQueueEntry all
// raise QueueCorruptFileError, mirroring peekQueue/dequeueItem/enqueueItem.
// ---------------------------------------------------------------------------

describe('a corrupted (non-JSON or non-array) EXISTING queue file surfaces QueueCorruptFileError from all three lease functions', () => {
  it('claimQueueEntry throws QueueCorruptFileError (.code === "QUEUE_CORRUPT_FILE") for non-JSON content', async () => {
    writeFileSync(queueFilePath, 'this is not json{{{', 'utf8');

    await expect(claimQueueEntry(queueFilePath, { now: 1_000 })).rejects.toBeInstanceOf(QueueCorruptFileError);
    await expect(claimQueueEntry(queueFilePath, { now: 1_000 })).rejects.toMatchObject({
      code: 'QUEUE_CORRUPT_FILE',
    });
  });

  it('ackQueueEntry throws QueueCorruptFileError for a corrupt file rather than silently no-oping', async () => {
    writeFileSync(queueFilePath, 'not json at all', 'utf8');

    await expect(ackQueueEntry(queueFilePath, 'some-lease-id')).rejects.toMatchObject({
      code: 'QUEUE_CORRUPT_FILE',
    });
  });

  it('releaseQueueEntry throws QueueCorruptFileError for a corrupt file rather than silently no-oping', async () => {
    writeFileSync(queueFilePath, JSON.stringify({ not: 'an array' }), 'utf8');

    await expect(releaseQueueEntry(queueFilePath, 'some-lease-id')).rejects.toMatchObject({
      code: 'QUEUE_CORRUPT_FILE',
    });
  });

  it('claimQueueEntry against a corrupt file leaves the corrupt bytes on disk untouched (no clobber-with-empty-state)', async () => {
    const corruptContent = 'totally not json';
    writeFileSync(queueFilePath, corruptContent, 'utf8');

    await expect(claimQueueEntry(queueFilePath, { now: 1_000 })).rejects.toMatchObject({
      code: 'QUEUE_CORRUPT_FILE',
    });

    expect(readFileSync(queueFilePath, 'utf8')).toBe(corruptContent);
  });
});

// ---------------------------------------------------------------------------
// 8. Concurrent claimQueueEntry calls never both receive the same entry —
// mirrors ./queue.jest.spec.mjs's concurrent-dequeueItem proof exactly.
// ---------------------------------------------------------------------------

describe('concurrent claimQueueEntry calls never return the same entry twice', () => {
  it('draining a 10-item queue with 10 concurrent claims yields 10 distinct leaseIds over 10 distinct entries, no duplicates, no losses', async () => {
    const agentClasses = Array.from({ length: 10 }, (_, index) => `orc-${index}`);

    for (const agentClass of agentClasses) {
      await enqueueItem(queueFilePath, validItem({ agentClass, priority: 'normal' }));
    }

    const results = await Promise.all(
      agentClasses.map((_, index) => claimQueueEntry(queueFilePath, { now: 1_000 + index })),
    );

    const claimedClasses = results.map((item) => item?.agentClass).filter(Boolean).sort();
    expect(claimedClasses).toEqual([...agentClasses].sort());

    const leaseIds = results.map((item) => item?.leaseId).filter(Boolean);
    expect(new Set(leaseIds).size).toBe(leaseIds.length);
    expect(leaseIds).toHaveLength(10);
  });

  it('when more concurrent claims than eligible items are racing, the excess calls resolve to null rather than throwing or duplicating a lease', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'only-item' }));

    const results = await Promise.all(
      Array.from({ length: 5 }, (_, index) => claimQueueEntry(queueFilePath, { now: 1_000 + index })),
    );

    const nonNull = results.filter((item) => item !== null);
    expect(nonNull).toHaveLength(1);
    expect(nonNull[0].agentClass).toBe('only-item');
    expect(results.filter((item) => item === null)).toHaveLength(4);
  });
});

// ---------------------------------------------------------------------------
// 9. pid-lease field lifecycle — ackQueueEntry/
// releaseQueueEntry must clear a bound `pid` alongside `leaseId`/`leasedAt`,
// and a claimQueueEntry TTL-expiry reclaim must never inherit the prior
// occupant's bound pid. `bindPidToLease` does not exist yet as of this
// commit (see ./queue-bind-pid.jest.spec.mjs's contract header) — every test
// below is expected to fail at import time or, once bindPidToLease exists,
// with a behavioral mismatch against ackQueueEntry/releaseQueueEntry not yet
// clearing `pid`. Do not add an implementation to make this pass.
// ---------------------------------------------------------------------------

describe('ackQueueEntry clears a bound pid alongside leaseId/leasedAt', () => {
  it('the acked (removed) entry snapshot no longer needs to expose pid, but a subsequent lookup of the removed entry proves it is gone entirely', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'to-ack', orchestratorId: 'orc-a' }));
    const claimed = await claimQueueEntry(queueFilePath, { now: 1_000 });
    await bindPidToLease(queueFilePath, claimed.leaseId, 4242, 'orc-a');

    const acked = await ackQueueEntry(queueFilePath, claimed.leaseId);

    expect(acked.agentClass).toBe('to-ack');
    const remaining = await peekQueue(queueFilePath);
    expect(remaining).toHaveLength(0);
  });
});

describe('releaseQueueEntry clears a bound pid alongside leaseId/leasedAt', () => {
  it('the released entry no longer carries the previously bound pid, leaseId, or leasedAt', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'crash-recycled', orchestratorId: 'orc-a' }));
    const claimed = await claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 60_000 });
    await bindPidToLease(queueFilePath, claimed.leaseId, 4242, 'orc-a');

    const released = await releaseQueueEntry(queueFilePath, claimed.leaseId);

    expect(released.leaseId == null).toBe(true);
    expect(released.leasedAt == null).toBe(true);
    expect(released.pid).toBeUndefined();

    const items = await peekQueue(queueFilePath);
    expect(items[0].pid).toBeUndefined();
  });

  it('a fresh claim after release does not inherit the pid bound before the release', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'crash-recycled', orchestratorId: 'orc-a' }));
    const claimed = await claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 60_000 });
    await bindPidToLease(queueFilePath, claimed.leaseId, 4242, 'orc-a');
    await releaseQueueEntry(queueFilePath, claimed.leaseId);

    const reclaimed = await claimQueueEntry(queueFilePath, { now: 2_000, claimTtlMs: 60_000 });

    expect(reclaimed.pid).toBeUndefined();
  });
});

describe('a stale bindPidToLease call against a leaseId superseded by TTL-expiry reclaim is a no-op against the new entry', () => {
  it('does not bind onto the entry now living under the fresh leaseId', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'only-item', orchestratorId: 'orc-a' }));

    const staleLease = await claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 1_000 });
    const freshLease = await claimQueueEntry(queueFilePath, { now: 5_000, claimTtlMs: 1_000 });
    expect(freshLease.leaseId).not.toBe(staleLease.leaseId);

    const staleBindResult = await bindPidToLease(queueFilePath, staleLease.leaseId, 4242, 'orc-a');

    expect(staleBindResult).toBeNull();
    const items = await peekQueue(queueFilePath);
    expect(items[0].leaseId).toBe(freshLease.leaseId);
    expect(items[0].pid).toBeUndefined();
  });
});

describe('claimQueueEntry reclaim never inherits a pid bound to the lease it supersedes', () => {
  it('a fresh reclaim after TTL expiry starts with no pid, even though the prior lease had one bound', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'only-item', orchestratorId: 'orc-a' }));

    const firstLease = await claimQueueEntry(queueFilePath, { now: 1_000, claimTtlMs: 1_000 });
    await bindPidToLease(queueFilePath, firstLease.leaseId, 4242, 'orc-a');

    const reclaimed = await claimQueueEntry(queueFilePath, { now: 5_000, claimTtlMs: 1_000 });

    expect(reclaimed.leaseId).not.toBe(firstLease.leaseId);
    expect(reclaimed.pid).toBeUndefined();
  });
});
