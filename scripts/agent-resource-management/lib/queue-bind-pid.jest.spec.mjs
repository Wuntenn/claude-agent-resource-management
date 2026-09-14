// RED unit tests for the not-yet-implemented `bindPidToLease` primitive on
// top of scripts/agent-resource-management/lib/queue.mjs (Phase 1 —
// "Queue schema: orchestrator-scoped pid-lease binding primitive").
//
// Direct-import unit tests, mirroring ./queue-lease.jest.spec.mjs's idiom
// exactly: real fs, real temp directories (mkdtempSync under os.tmpdir()),
// real concurrency via Promise.all — never a mocked filesystem.
//
// `bindPidToLease` does NOT exist yet as of this commit — every test below is
// expected to fail at import time (`does not provide an export named
// 'bindPidToLease'`) or, once a stub exists, with a behavioral mismatch. Do
// not add an implementation to make this pass — the builder implements this
// function in ./queue.mjs in a later phase to turn this green.
//
// ---------------------------------------------------------------------------
// Contract this suite specifies for the implementer (none of this exists in
// queue.mjs today — this file IS the spec):
//
//   bindPidToLease(filePath, leaseId, pid, orchestratorId, options?)
//     -> Promise<object|null>
//     Atomically stamps `pid` onto the single on-disk entry whose CURRENT
//     `leaseId` AND `orchestratorId` BOTH exactly match the given values —
//     never matching on `leaseId` alone (closes the ownership gap the
//     Convergence Analysis raised: an orchestrator must not be able to bind
//     an arbitrary pid onto another orchestrator's lease). Returns the
//     UPDATED entry. If no on-disk entry matches BOTH `leaseId` AND
//     `orchestratorId` — including a stale/superseded `leaseId`, or a
//     `leaseId` that belongs to a DIFFERENT orchestrator's entry — this is a
//     SAFE NO-OP: returns `null`, the queue is left completely unmodified,
//     nothing throws. Mirrors `ackQueueEntry`/`releaseQueueEntry`'s existing
//     "no match, return null rather than throw" convention.
//
//     Rejects BEFORE writing (no file created/modified as a side effect) for
//     a non-positive-integer `pid` (NaN, negative, zero, non-integer,
//     non-number).
//
//     Idempotent: calling twice with the same leaseId+orchestratorId+pid is
//     a no-op re-write (no error, same end state); calling again with a
//     DIFFERENT pid for the same leaseId+orchestratorId overwrites it.
//
//     Locked via the same `withQueueLock` critical section every other
//     mutating function in this module uses — concurrent
//     `bindPidToLease`/`releaseQueueEntry` calls racing the same file must
//     not corrupt the queue.
//
//     Propagates `QueueCorruptFileError` for a corrupt on-disk file,
//     identically to `ackQueueEntry`/`releaseQueueEntry`.
// ---------------------------------------------------------------------------

import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  enqueueItem,
  peekQueue,
  claimQueueEntry,
  releaseQueueEntry,
  bindPidToLease,
  QueueCorruptFileError,
} from './queue.mjs';

let workDir;
let queueFilePath;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'queue-bind-pid-lib-'));
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

describe('bindPidToLease binds pid to the matching leaseId+orchestratorId entry', () => {
  it('stamps pid onto the entry when both leaseId and orchestratorId match', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'to-bind', orchestratorId: 'orc-a' }));
    const claimed = await claimQueueEntry(queueFilePath, { now: 1_000 });

    const bound = await bindPidToLease(queueFilePath, claimed.leaseId, 4242, 'orc-a');

    expect(bound).not.toBeNull();
    expect(bound.pid).toBe(4242);
    expect(bound.agentClass).toBe('to-bind');

    const items = await peekQueue(queueFilePath);
    const persisted = items.find((item) => item.agentClass === 'to-bind');
    expect(persisted.pid).toBe(4242);
    expect(persisted.leaseId).toBe(claimed.leaseId);
  });
});

describe('bindPidToLease no-op when leaseId does not match any entry', () => {
  it('returns null and leaves the queue unmodified for an unknown leaseId', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'untouched', orchestratorId: 'orc-a' }));
    await claimQueueEntry(queueFilePath, { now: 1_000 });

    const result = await bindPidToLease(queueFilePath, 'never-issued-lease-id', 4242, 'orc-a');

    expect(result).toBeNull();
    const items = await peekQueue(queueFilePath);
    expect(items[0].pid).toBeUndefined();
  });
});

describe('bindPidToLease no-op when leaseId matches but orchestratorId does not (ownership boundary)', () => {
  it('does not bind pid to an entry owned by a different orchestratorId', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'owned-by-a', orchestratorId: 'orc-a' }));
    const claimed = await claimQueueEntry(queueFilePath, { now: 1_000 });

    const result = await bindPidToLease(queueFilePath, claimed.leaseId, 4242, 'orc-b');

    expect(result).toBeNull();
    const items = await peekQueue(queueFilePath);
    expect(items[0].pid).toBeUndefined();
    expect(items[0].leaseId).toBe(claimed.leaseId);
  });
});

describe('bindPidToLease rejects non-positive-integer/NaN/negative/zero pid before writing', () => {
  it.each([
    ['zero', 0],
    ['negative', -1],
    ['NaN', NaN],
    ['non-integer', 1.5],
    ['string', '4242'],
    ['null', null],
    ['undefined', undefined],
  ])('rejects pid=%s (%p) without writing to the queue', async (_label, badPid) => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'to-bind', orchestratorId: 'orc-a' }));
    const claimed = await claimQueueEntry(queueFilePath, { now: 1_000 });

    await expect(bindPidToLease(queueFilePath, claimed.leaseId, badPid, 'orc-a')).rejects.toBeTruthy();

    const items = await peekQueue(queueFilePath);
    expect(items[0].pid).toBeUndefined();
  });
});

describe('bindPidToLease is idempotent when called twice with the same leaseId+orchestratorId+pid', () => {
  it('does not error and leaves the same pid bound after a repeated identical call', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'to-bind', orchestratorId: 'orc-a' }));
    const claimed = await claimQueueEntry(queueFilePath, { now: 1_000 });

    await bindPidToLease(queueFilePath, claimed.leaseId, 4242, 'orc-a');
    const secondCall = await bindPidToLease(queueFilePath, claimed.leaseId, 4242, 'orc-a');

    expect(secondCall.pid).toBe(4242);
    const items = await peekQueue(queueFilePath);
    expect(items[0].pid).toBe(4242);
  });
});

describe('bindPidToLease second bind call with a different pid overwrites the first', () => {
  it('replaces the previously bound pid with the newly supplied one', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'to-bind', orchestratorId: 'orc-a' }));
    const claimed = await claimQueueEntry(queueFilePath, { now: 1_000 });

    await bindPidToLease(queueFilePath, claimed.leaseId, 4242, 'orc-a');
    const overwritten = await bindPidToLease(queueFilePath, claimed.leaseId, 9999, 'orc-a');

    expect(overwritten.pid).toBe(9999);
    const items = await peekQueue(queueFilePath);
    expect(items[0].pid).toBe(9999);
  });
});

describe('two live leases for the same orchestratorId bound to the same pid number do not cross-contaminate lookups', () => {
  it('each lease keeps its own identity — binding is scoped by leaseId, not by a global pid search', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'lease-one', orchestratorId: 'orc-a' }));
    await enqueueItem(queueFilePath, validItem({ agentClass: 'lease-two', orchestratorId: 'orc-a' }));

    const claimedOne = await claimQueueEntry(queueFilePath, { now: 1_000 });
    const claimedTwo = await claimQueueEntry(queueFilePath, { now: 1_100 });
    expect(claimedOne.leaseId).not.toBe(claimedTwo.leaseId);

    await bindPidToLease(queueFilePath, claimedOne.leaseId, 5555, 'orc-a');
    await bindPidToLease(queueFilePath, claimedTwo.leaseId, 5555, 'orc-a');

    const items = await peekQueue(queueFilePath);
    const one = items.find((item) => item.agentClass === 'lease-one');
    const two = items.find((item) => item.agentClass === 'lease-two');
    expect(one.pid).toBe(5555);
    expect(two.pid).toBe(5555);
    expect(one.leaseId).toBe(claimedOne.leaseId);
    expect(two.leaseId).toBe(claimedTwo.leaseId);
  });
});

describe('concurrent bindPidToLease + releaseQueueEntry on the same file do not corrupt the queue (lock contention)', () => {
  it('both operations complete without throwing and the resulting queue is well-formed JSON with exactly one entry', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'contended', orchestratorId: 'orc-a' }));
    const claimed = await claimQueueEntry(queueFilePath, { now: 1_000 });

    await Promise.all([
      bindPidToLease(queueFilePath, claimed.leaseId, 4242, 'orc-a'),
      releaseQueueEntry(queueFilePath, claimed.leaseId),
    ]);

    const items = await peekQueue(queueFilePath);
    expect(items).toHaveLength(1);
    expect(items[0].agentClass).toBe('contended');
  });
});

describe('bindPidToLease propagates QueueCorruptFileError on a corrupted queue file', () => {
  it('throws QueueCorruptFileError (.code === "QUEUE_CORRUPT_FILE") for non-JSON content rather than silently no-oping', async () => {
    writeFileSync(queueFilePath, 'this is not json{{{', 'utf8');

    await expect(bindPidToLease(queueFilePath, 'some-lease-id', 4242, 'orc-a')).rejects.toBeInstanceOf(
      QueueCorruptFileError,
    );
    await expect(bindPidToLease(queueFilePath, 'some-lease-id', 4242, 'orc-a')).rejects.toMatchObject({
      code: 'QUEUE_CORRUPT_FILE',
    });
  });
});
