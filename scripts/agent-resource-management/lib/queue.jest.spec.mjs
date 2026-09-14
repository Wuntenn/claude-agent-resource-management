// RED unit tests for the not-yet-implemented
// scripts/agent-resource-management/lib/queue.mjs module (Phase 1 —
// "Queue module core").
//
// Direct-import unit tests, mirroring ./coordination-file.jest.spec.mjs's
// idiom exactly: real fs, real temp directories (mkdtemp under os.tmpdir()),
// real concurrency via Promise.all — never a mocked filesystem, because the
// whole point of this module is multi-writer atomicity/lock correctness
// against the REAL filesystem. Never touches the real host coordination
// state — every fixture path below is a per-test mkdtemp path, and this
// suite never invokes cli.mjs or reads/writes the production
// `~/.claude/agent-state/*.json` paths directly.
//
// This is deliberately the "lib" layer only — a distinct, narrower spec from
// ../queue.jest.spec.mjs (the Phase 0 outer-acceptance test), which
// drives the CLI black-box via spawned `node cli.mjs` processes and never
// imports a queue module directly. This file is the opposite: it imports
// `./queue.mjs`'s public surface directly and never spawns a process.
//
// `lib/queue.mjs` does NOT exist yet as of this commit — every test below is
// expected to fail at import time (`Cannot find module './queue.mjs'`) or,
// once a stub file exists but the named exports don't, with a
// "<name> is not a function" style failure. Do not add a stub implementation
// to make this pass — the builder implements queue.mjs in the next phase to
// turn this green.
//
// ---------------------------------------------------------------------------
// Contract this suite specifies for the implementer (none of this exists
// today — this file IS the spec):
//
//   enqueueItem(filePath, item, options?) -> Promise<object>
//     item: { agentClass: string, priority: 'low'|'normal'|'high',
//              commandRef: string, enqueuedAt?: number }
//     Persists the entry (stamping `enqueuedAt` with Date.now() if the
//     caller didn't supply one — mirrors declareDibs's own
//     caller-supplies-or-we-stamp convention for its timestamp field,
//     `declaredAt`), atomically, under the SAME lock/atomic-write machinery
//     `./coordination-file.mjs` already exports (`acquireLock`/
//     `releaseLock`/`assertStillHeld`/`writeEntriesAtomic`) — no second
//     locking primitive invented for the queue. Returns the persisted entry.
//     Rejects BEFORE writing (no file created/modified as a side effect) for
//     an out-of-enum `priority` (error `.code === 'QUEUE_INVALID_PRIORITY'`)
//     or an oversized/malformed `commandRef` (error
//     `.code === 'QUEUE_INVALID_COMMAND_REF'`). Chosen bound (no existing
//     precedent elsewhere in this repo for a "command reference string"):
//     `commandRef` must be a non-empty string of at most 4096 characters —
//     `MAX_COMMAND_REF_LENGTH`, exported for the implementer/reviewer to
//     reconcile against or reuse directly rather than restating the number.
//
//   peekQueue(filePath) -> Promise<Array<object>>
//     Read-only — never acquires the queue lock, so takes no `options` (there
//     is no lock-timing parameter for it to thread through). Returns the
//     full LIVE queue, ordered by `priority`
//     DESCENDING (high > normal > low) then `enqueuedAt` ASCENDING
//     (oldest first); a same-priority, same-`enqueuedAt` tie (Date.now()
//     resolution collisions are real on a fast host) breaks by INSERTION
//     ORDER — i.e. stable sort over the on-disk array order, never
//     re-ordered by any other criterion. Never mutates the file (byte-for-
//     byte identical before/after). A missing queue file returns `[]`,
//     never throws — mirrors readEntriesRaw's ENOENT tolerance.
//
//   dequeueItem(filePath, options?) -> Promise<object|null>
//     Atomically removes AND returns the single top entry per the exact same
//     ordering `peekQueue` uses, re-reading the file under its OWN lock
//     acquisition rather than trusting any prior `peekQueue` call (TOCTOU
//     safety). Two concurrent `dequeueItem` calls racing the same queue
//     never both receive the same entry. A missing queue file, or an empty
//     live queue, returns `null`, never throws.
//
//   resolveQueueFilePath() -> string
//     Mirrors cli.mjs's (unexported) `resolveCoordinationFilePath()`
//     precedent exactly: `if (process.env.ARM_QUEUE_FILE) return
//     resolve(process.env.ARM_QUEUE_FILE);`, else defaults to
//     `~/.claude/agent-state/resource-queue.json`. Exported from THIS module
//     (unlike `resolveCoordinationFilePath`, which lives privately in
//     cli.mjs) because Phase 2's CLI wiring (`--enqueue`/`--peek`/
//     `--dequeue`) needs to resolve the SAME path this module's own
//     tests below exercise directly — one resolver, reused, not
//     re-implemented at the CLI layer. Deliberately independent of
//     `ARM_COORDINATION_FILE`: reading/setting one must never affect the
//     other's resolved path.
//
//   MAX_COMMAND_REF_LENGTH -> number (4096)
//     Exported constant backing the `commandRef` length bound above.
// ---------------------------------------------------------------------------

import { mkdtempSync, rmSync, writeFileSync, existsSync, readFileSync, statSync } from 'node:fs';
import { promises as fs } from 'node:fs';
import { tmpdir, homedir } from 'node:os';
import { join } from 'node:path';

import { jest } from '@jest/globals';

import {
  enqueueItem,
  peekQueue,
  dequeueItem,
  resolveQueueFilePath,
  MAX_COMMAND_REF_LENGTH,
  MAX_AGENT_CLASS_LENGTH,
  MAX_ORCHESTRATOR_ID_LENGTH,
  QueueCorruptFileError,
} from './queue.mjs';

let workDir;
let queueFilePath;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'queue-lib-'));
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
// 1. enqueueItem persists the documented fields
// ---------------------------------------------------------------------------

describe('enqueueItem persists agentClass/priority/enqueuedAt/commandRef', () => {
  it('persists an entry that peekQueue can read back with all five fields, including the owning orchestratorId', async () => {
    const persisted = await enqueueItem(
      queueFilePath,
      validItem({
        agentClass: 'engineer-implement',
        priority: 'high',
        commandRef: 'run tests',
        orchestratorId: 'orch-owner',
      }),
    );

    expect(persisted.agentClass).toBe('engineer-implement');
    expect(persisted.priority).toBe('high');
    expect(persisted.commandRef).toBe('run tests');
    expect(persisted.orchestratorId).toBe('orch-owner');
    expect(typeof persisted.enqueuedAt).toBe('string');
    expect(() => new Date(persisted.enqueuedAt).toISOString()).not.toThrow();

    const items = await peekQueue(queueFilePath);
    expect(items).toHaveLength(1);
    expect(items[0]).toMatchObject({
      agentClass: 'engineer-implement',
      priority: 'high',
      commandRef: 'run tests',
      orchestratorId: 'orch-owner',
      enqueuedAt: persisted.enqueuedAt,
    });
  });

  it('stamps enqueuedAt with a server-assigned ISO-8601 string when the caller does not supply one', async () => {
    const before = new Date().toISOString();
    const persisted = await enqueueItem(queueFilePath, validItem());
    const after = new Date().toISOString();

    expect(typeof persisted.enqueuedAt).toBe('string');
    expect(persisted.enqueuedAt).toBe(new Date(persisted.enqueuedAt).toISOString());
    expect(persisted.enqueuedAt >= before).toBe(true);
    expect(persisted.enqueuedAt <= after).toBe(true);
  });

  it('ignores/overwrites a caller-supplied enqueuedAt — never honors it verbatim', async () => {
    const spoofedFutureTimestamp = '2999-01-01T00:00:00.000Z';

    const persisted = await enqueueItem(
      queueFilePath,
      validItem({ enqueuedAt: spoofedFutureTimestamp }),
    );

    expect(persisted.enqueuedAt).not.toBe(spoofedFutureTimestamp);
    expect(typeof persisted.enqueuedAt).toBe('string');
    expect(persisted.enqueuedAt <= new Date().toISOString()).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 2. peekQueue ordering — priority desc, then enqueuedAt asc — read-only
// ---------------------------------------------------------------------------

describe('peekQueue orders by priority (high > normal > low) then enqueuedAt (oldest first), without mutating the file', () => {
  it('orders a mix of priorities highest-first regardless of insertion order', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'a', priority: 'low' }));
    await enqueueItem(queueFilePath, validItem({ agentClass: 'b', priority: 'high' }));
    await enqueueItem(queueFilePath, validItem({ agentClass: 'c', priority: 'normal' }));

    const items = await peekQueue(queueFilePath);

    expect(items.map((item) => item.agentClass)).toEqual(['b', 'c', 'a']);
  });

  it('within the same priority, orders oldest enqueuedAt (i.e. earliest-inserted) first', async () => {
    // enqueuedAt is now server-assigned at persist time, so ordering within
    // a priority is exercised via genuine insertion order — the earlier
    // sequential await necessarily persists with an equal-or-earlier
    // timestamp than the later one, and the sort's stable tiebreak handles
    // any same-millisecond collision identically.
    await enqueueItem(queueFilePath, validItem({ agentClass: 'older', priority: 'normal' }));
    await enqueueItem(queueFilePath, validItem({ agentClass: 'newer', priority: 'normal' }));

    const items = await peekQueue(queueFilePath);

    expect(items.map((item) => item.agentClass)).toEqual(['older', 'newer']);
  });

  it('does not mutate the queue file on disk — byte-for-byte identical before and after peekQueue', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'a', priority: 'low' }));
    await enqueueItem(queueFilePath, validItem({ agentClass: 'b', priority: 'high' }));

    const before = readFileSync(queueFilePath, 'utf8');
    const statBefore = statSync(queueFilePath);

    await peekQueue(queueFilePath);

    const after = readFileSync(queueFilePath, 'utf8');
    const statAfter = statSync(queueFilePath);

    expect(after).toBe(before);
    expect(statAfter.mtimeMs).toBe(statBefore.mtimeMs);
  });
});

// ---------------------------------------------------------------------------
// 3. dequeueItem atomically removes and returns the top entry
// ---------------------------------------------------------------------------

describe('dequeueItem atomically removes and returns the top entry (same ordering as peekQueue)', () => {
  it('returns the highest-priority entry and removes it from the queue', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'low-one', priority: 'low' }));
    await enqueueItem(queueFilePath, validItem({ agentClass: 'high-one', priority: 'high' }));

    const dequeued = await dequeueItem(queueFilePath);

    expect(dequeued.agentClass).toBe('high-one');

    const remaining = await peekQueue(queueFilePath);
    expect(remaining.map((item) => item.agentClass)).toEqual(['low-one']);
  });

  it('repeated dequeues drain the queue in full priority/enqueuedAt order, ending in null', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'a', priority: 'normal' }));
    await enqueueItem(queueFilePath, validItem({ agentClass: 'b', priority: 'high' }));
    await enqueueItem(queueFilePath, validItem({ agentClass: 'c', priority: 'low' }));

    const first = await dequeueItem(queueFilePath);
    const second = await dequeueItem(queueFilePath);
    const third = await dequeueItem(queueFilePath);
    const fourth = await dequeueItem(queueFilePath);

    expect([first.agentClass, second.agentClass, third.agentClass]).toEqual(['b', 'a', 'c']);
    expect(fourth).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// 4. Concurrent enqueueItem — no torn/unparseable file, both entries survive
// ---------------------------------------------------------------------------

describe('concurrent enqueueItem calls never produce a torn/unparseable queue file', () => {
  it('lands both entries when two callers enqueueItem "simultaneously"', async () => {
    await Promise.all([
      enqueueItem(queueFilePath, validItem({ agentClass: 'orc-a' })),
      enqueueItem(queueFilePath, validItem({ agentClass: 'orc-b' })),
    ]);

    const items = await peekQueue(queueFilePath);
    const classes = items.map((item) => item.agentClass).sort();

    expect(classes).toEqual(['orc-a', 'orc-b']);
  });

  it('never throws a JSON parse error under a tight loop of concurrent enqueues and peeks', async () => {
    const iterations = 30;

    const writers = Array.from({ length: iterations }, (_, index) =>
      enqueueItem(queueFilePath, validItem({ agentClass: `orc-${index}` })),
    );
    const readers = Array.from({ length: iterations }, () => peekQueue(queueFilePath));

    await expect(Promise.all([...writers, ...readers])).resolves.toBeDefined();

    const items = await peekQueue(queueFilePath);
    expect(items).toHaveLength(iterations);
  });
});

// ---------------------------------------------------------------------------
// 5. Concurrent dequeueItem — single-winner claim-and-remove
// ---------------------------------------------------------------------------

describe('concurrent dequeueItem calls never return the same entry twice', () => {
  it('draining a 10-item queue with 10 concurrent dequeues yields 10 distinct entries, no duplicates, no losses', async () => {
    const agentClasses = Array.from({ length: 10 }, (_, index) => `orc-${index}`);

    for (const agentClass of agentClasses) {
      await enqueueItem(queueFilePath, validItem({ agentClass, priority: 'normal' }));
    }

    const results = await Promise.all(agentClasses.map(() => dequeueItem(queueFilePath)));

    const dequeuedClasses = results.map((item) => item?.agentClass).filter(Boolean).sort();
    expect(dequeuedClasses).toEqual([...agentClasses].sort());

    const remaining = await peekQueue(queueFilePath);
    expect(remaining).toHaveLength(0);
  });

  it('when more concurrent dequeues than items are racing, the excess calls resolve to null rather than throwing or duplicating', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'only-item' }));

    const results = await Promise.all(Array.from({ length: 5 }, () => dequeueItem(queueFilePath)));

    const nonNull = results.filter((item) => item !== null);
    expect(nonNull).toHaveLength(1);
    expect(nonNull[0].agentClass).toBe('only-item');
    expect(results.filter((item) => item === null)).toHaveLength(4);
  });
});

// ---------------------------------------------------------------------------
// 6. Missing queue file — peekQueue/dequeueItem tolerate ENOENT
// ---------------------------------------------------------------------------

describe('peekQueue/dequeueItem on a missing queue file return empty/null rather than throwing', () => {
  it('peekQueue against a never-created queue file returns []', async () => {
    await expect(peekQueue(queueFilePath)).resolves.toEqual([]);
  });

  it('dequeueItem against a never-created queue file returns null', async () => {
    await expect(dequeueItem(queueFilePath)).resolves.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// 7. enqueueItem rejects an out-of-enum priority before writing
// ---------------------------------------------------------------------------

describe('enqueueItem rejects an out-of-enum priority value before writing', () => {
  it('rejects "urgent" (not in the low|normal|high enum) with a QUEUE_INVALID_PRIORITY error', async () => {
    await expect(
      enqueueItem(queueFilePath, validItem({ priority: 'urgent' })),
    ).rejects.toMatchObject({ code: 'QUEUE_INVALID_PRIORITY' });
  });

  it('does not create the queue file at all as a side effect of a rejected priority', async () => {
    await expect(enqueueItem(queueFilePath, validItem({ priority: 'urgent' }))).rejects.toThrow();
    expect(existsSync(queueFilePath)).toBe(false);
  });

  it('rejects a rejected write without disturbing an EXISTING queue file\'s prior contents', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'already-here', priority: 'normal' }));
    const before = readFileSync(queueFilePath, 'utf8');

    await expect(enqueueItem(queueFilePath, validItem({ priority: 'urgent' }))).rejects.toThrow();

    expect(readFileSync(queueFilePath, 'utf8')).toBe(before);
  });
});

// ---------------------------------------------------------------------------
// 8. enqueueItem rejects an oversized/malformed commandRef before writing
// ---------------------------------------------------------------------------

describe('enqueueItem rejects an oversized or malformed commandRef before writing', () => {
  it('exports MAX_COMMAND_REF_LENGTH as 4096 — the chosen bound this suite pins', () => {
    expect(MAX_COMMAND_REF_LENGTH).toBe(4096);
  });

  it('rejects a commandRef longer than MAX_COMMAND_REF_LENGTH characters', async () => {
    const oversized = 'x'.repeat(MAX_COMMAND_REF_LENGTH + 1);

    await expect(
      enqueueItem(queueFilePath, validItem({ commandRef: oversized })),
    ).rejects.toMatchObject({ code: 'QUEUE_INVALID_COMMAND_REF' });
  });

  it('accepts a commandRef exactly at MAX_COMMAND_REF_LENGTH characters (inclusive boundary)', async () => {
    const boundary = 'x'.repeat(MAX_COMMAND_REF_LENGTH);

    await expect(enqueueItem(queueFilePath, validItem({ commandRef: boundary }))).resolves.toMatchObject({
      commandRef: boundary,
    });
  });

  it('rejects an empty-string commandRef', async () => {
    await expect(
      enqueueItem(queueFilePath, validItem({ commandRef: '' })),
    ).rejects.toMatchObject({ code: 'QUEUE_INVALID_COMMAND_REF' });
  });

  it('rejects a non-string commandRef', async () => {
    await expect(
      enqueueItem(queueFilePath, validItem({ commandRef: 12345 })),
    ).rejects.toMatchObject({ code: 'QUEUE_INVALID_COMMAND_REF' });
  });

  it('does not create the queue file at all as a side effect of a rejected commandRef', async () => {
    await expect(
      enqueueItem(queueFilePath, validItem({ commandRef: 'x'.repeat(MAX_COMMAND_REF_LENGTH + 1) })),
    ).rejects.toThrow();
    expect(existsSync(queueFilePath)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 8b. enqueueItem rejects an oversized/malformed/missing agentClass before
// writing — mirrors section 8's commandRef validation exactly.
// ---------------------------------------------------------------------------

describe('enqueueItem rejects an oversized or malformed agentClass before writing', () => {
  it('exports MAX_AGENT_CLASS_LENGTH as 256 — the chosen bound this suite pins', () => {
    expect(MAX_AGENT_CLASS_LENGTH).toBe(256);
  });

  it('rejects an agentClass longer than MAX_AGENT_CLASS_LENGTH characters', async () => {
    const oversized = 'x'.repeat(MAX_AGENT_CLASS_LENGTH + 1);

    await expect(
      enqueueItem(queueFilePath, validItem({ agentClass: oversized })),
    ).rejects.toMatchObject({ code: 'QUEUE_INVALID_AGENT_CLASS' });
  });

  it('accepts an agentClass exactly at MAX_AGENT_CLASS_LENGTH characters (inclusive boundary)', async () => {
    const boundary = 'x'.repeat(MAX_AGENT_CLASS_LENGTH);

    await expect(enqueueItem(queueFilePath, validItem({ agentClass: boundary }))).resolves.toMatchObject({
      agentClass: boundary,
    });
  });

  it('rejects an empty-string agentClass', async () => {
    await expect(
      enqueueItem(queueFilePath, validItem({ agentClass: '' })),
    ).rejects.toMatchObject({ code: 'QUEUE_INVALID_AGENT_CLASS' });
  });

  it('rejects a non-string agentClass', async () => {
    await expect(
      enqueueItem(queueFilePath, validItem({ agentClass: 12345 })),
    ).rejects.toMatchObject({ code: 'QUEUE_INVALID_AGENT_CLASS' });
  });

  it('does not create the queue file at all as a side effect of a rejected agentClass', async () => {
    await expect(
      enqueueItem(queueFilePath, validItem({ agentClass: 'x'.repeat(MAX_AGENT_CLASS_LENGTH + 1) })),
    ).rejects.toThrow();
    expect(existsSync(queueFilePath)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 8c. enqueueItem rejects an oversized/malformed/missing orchestratorId
// before writing, and persists a valid one — mirrors section 8b's
// agentClass validation exactly. Required at the module level, not just at
// the CLI layer: a future direct caller of enqueueItem can't accidentally
// persist an entry with no owning orchestrator (Scope: "owning
// orchestrator" is a documented required field of the persisted entry).
// ---------------------------------------------------------------------------

describe('enqueueItem persists orchestratorId and rejects an oversized or malformed one before writing', () => {
  it('exports MAX_ORCHESTRATOR_ID_LENGTH as 256 — the chosen bound this suite pins', () => {
    expect(MAX_ORCHESTRATOR_ID_LENGTH).toBe(256);
  });

  it('rejects an orchestratorId longer than MAX_ORCHESTRATOR_ID_LENGTH characters', async () => {
    const oversized = 'x'.repeat(MAX_ORCHESTRATOR_ID_LENGTH + 1);

    await expect(
      enqueueItem(queueFilePath, validItem({ orchestratorId: oversized })),
    ).rejects.toMatchObject({ code: 'QUEUE_INVALID_ORCHESTRATOR_ID' });
  });

  it('accepts an orchestratorId exactly at MAX_ORCHESTRATOR_ID_LENGTH characters (inclusive boundary)', async () => {
    const boundary = 'x'.repeat(MAX_ORCHESTRATOR_ID_LENGTH);

    await expect(enqueueItem(queueFilePath, validItem({ orchestratorId: boundary }))).resolves.toMatchObject({
      orchestratorId: boundary,
    });
  });

  it('rejects an empty-string orchestratorId', async () => {
    await expect(
      enqueueItem(queueFilePath, validItem({ orchestratorId: '' })),
    ).rejects.toMatchObject({ code: 'QUEUE_INVALID_ORCHESTRATOR_ID' });
  });

  it('rejects a missing (undefined) orchestratorId', async () => {
    const { orchestratorId, ...withoutOrchestratorId } = validItem();
    void orchestratorId;

    await expect(
      enqueueItem(queueFilePath, withoutOrchestratorId),
    ).rejects.toMatchObject({ code: 'QUEUE_INVALID_ORCHESTRATOR_ID' });
  });

  it('rejects a non-string orchestratorId', async () => {
    await expect(
      enqueueItem(queueFilePath, validItem({ orchestratorId: 12345 })),
    ).rejects.toMatchObject({ code: 'QUEUE_INVALID_ORCHESTRATOR_ID' });
  });

  it('does not create the queue file at all as a side effect of a rejected orchestratorId', async () => {
    await expect(
      enqueueItem(queueFilePath, validItem({ orchestratorId: 'x'.repeat(MAX_ORCHESTRATOR_ID_LENGTH + 1) })),
    ).rejects.toThrow();
    expect(existsSync(queueFilePath)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 9. ARM_QUEUE_FILE override — independent of ARM_COORDINATION_FILE
// ---------------------------------------------------------------------------

describe('resolveQueueFilePath honors ARM_QUEUE_FILE, independently of ARM_COORDINATION_FILE', () => {
  const originalQueueEnv = process.env.ARM_QUEUE_FILE;
  const originalCoordinationEnv = process.env.ARM_COORDINATION_FILE;

  afterEach(() => {
    if (originalQueueEnv === undefined) delete process.env.ARM_QUEUE_FILE;
    else process.env.ARM_QUEUE_FILE = originalQueueEnv;

    if (originalCoordinationEnv === undefined) delete process.env.ARM_COORDINATION_FILE;
    else process.env.ARM_COORDINATION_FILE = originalCoordinationEnv;
  });

  it('resolves to ARM_QUEUE_FILE when set', () => {
    process.env.ARM_QUEUE_FILE = queueFilePath;
    delete process.env.ARM_COORDINATION_FILE;

    expect(resolveQueueFilePath()).toBe(queueFilePath);
  });

  it('setting ARM_COORDINATION_FILE alone does not affect resolveQueueFilePath\'s resolved path', () => {
    delete process.env.ARM_QUEUE_FILE;
    process.env.ARM_COORDINATION_FILE = join(workDir, 'resource-coordination.json');

    const resolved = resolveQueueFilePath();

    expect(resolved).not.toBe(process.env.ARM_COORDINATION_FILE);
    expect(resolved).toBe(join(homedir(), '.claude/agent-state/resource-queue.json'));
  });

  it('setting BOTH env vars to different paths resolves each independently', () => {
    const coordinationPath = join(workDir, 'resource-coordination.json');
    process.env.ARM_QUEUE_FILE = queueFilePath;
    process.env.ARM_COORDINATION_FILE = coordinationPath;

    expect(resolveQueueFilePath()).toBe(queueFilePath);
    expect(resolveQueueFilePath()).not.toBe(coordinationPath);
  });

  it('defaults to ~/.claude/agent-state/resource-queue.json when ARM_QUEUE_FILE is unset', () => {
    delete process.env.ARM_QUEUE_FILE;
    delete process.env.ARM_COORDINATION_FILE;

    expect(resolveQueueFilePath()).toBe(join(homedir(), '.claude/agent-state/resource-queue.json'));
  });
});

// ---------------------------------------------------------------------------
// 10. Structural — queue.mjs must never reference coordination-file's
// admission-ledger surface (LIVE_AGENT_ADMISSION_CLAIM_TYPE/claimCapacity/
// releaseCapacity/resource-coordination.json) — the durable queue is a fully
// separate concern from ARM's existing admission ledger.
// ---------------------------------------------------------------------------

describe('queue.mjs never references the admission-ledger surface (structural)', () => {
  it('the module source does not mention resource-coordination.json, LIVE_AGENT_ADMISSION_CLAIM_TYPE, claimCapacity, or releaseCapacity', () => {
    // Deliberately no try/catch around this read: if lib/queue.mjs does not
    // exist yet, this throws ENOENT, which is itself a valid RED failure for
    // this suite's purpose (the module doesn't exist yet). Once it exists,
    // this becomes a genuine content assertion.
    const queueSource = readFileSync(new URL('./queue.mjs', import.meta.url), 'utf8');

    expect(queueSource).not.toMatch(/resource-coordination\.json/);
    expect(queueSource).not.toMatch(/LIVE_AGENT_ADMISSION_CLAIM_TYPE/);
    expect(queueSource).not.toMatch(/claimCapacity/);
    expect(queueSource).not.toMatch(/releaseCapacity/);
  });
});

// ---------------------------------------------------------------------------
// 11. Coordination/queue state directory missing (fresh host, first run) —
// the first enqueueItem call creates the directory.
// ---------------------------------------------------------------------------

describe('queue state directory missing (fresh host, first-ever run)', () => {
  it('the first enqueueItem call creates the containing directory when it does not yet exist', async () => {
    const freshDir = join(workDir, 'not-created-yet', 'nested');
    const freshQueuePath = join(freshDir, 'resource-queue.json');

    expect(existsSync(freshDir)).toBe(false);

    await enqueueItem(freshQueuePath, validItem({ agentClass: 'first-ever' }));

    expect(existsSync(freshDir)).toBe(true);
    const items = await peekQueue(freshQueuePath);
    expect(items.map((item) => item.agentClass)).toEqual(['first-ever']);
  });

  // Regression guard for the Medium finding fixed alongside this test: prior
  // to the fix, `dequeueItem` skipped `ensureQueueDir` (unlike `enqueueItem`,
  // above) and called `withQueueLock` -> `acquireLock` directly. On a truly
  // fresh host, `acquireLock`'s `fs.open(lockPath, 'wx')` fails with ENOENT
  // (the containing directory doesn't exist), which `acquireLock`'s catch
  // block only handles for `EEXIST` — the ENOENT propagated straight through
  // `dequeueItem`, contradicting its own doc comment ("A missing queue file,
  // or an empty live queue, returns `null`, never throws"). These tests call
  // `dequeueItem` FIRST, before any `enqueueItem`, so the directory genuinely
  // does not exist yet when the lock is acquired — a plain `mkdtemp`-only
  // `beforeEach` (like section 6's) does NOT reproduce this, because
  // `workDir` itself already exists there; the bug only bites one or more
  // directory levels below it.
  it('dequeueItem called before any enqueueItem, with no queue directory at all, returns null rather than throwing', async () => {
    const freshDir = join(workDir, 'never-created', 'nested');
    const freshQueuePath = join(freshDir, 'resource-queue.json');

    expect(existsSync(freshDir)).toBe(false);

    await expect(dequeueItem(freshQueuePath)).resolves.toBeNull();
  });

  it('dequeueItem creates the full missing directory chain, several levels deep, rather than throwing', async () => {
    const freshDir = join(workDir, 'a', 'b', 'c', 'd');
    const freshQueuePath = join(freshDir, 'resource-queue.json');

    expect(existsSync(freshDir)).toBe(false);

    await expect(dequeueItem(freshQueuePath)).resolves.toBeNull();

    expect(existsSync(freshDir)).toBe(true);
  });

  it('dequeueItem against an ALREADY-EXISTING queue directory remains a no-op on the directory (steady-state, no behavior change)', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'steady-state' }));
    const statBefore = statSync(workDir);

    const dequeued = await dequeueItem(queueFilePath);

    expect(dequeued.agentClass).toBe('steady-state');
    expect(existsSync(workDir)).toBe(true);
    expect(statSync(workDir).mtimeMs).toBeGreaterThanOrEqual(statBefore.mtimeMs);
  });

  it('peekQueue on a completely fresh, never-created queue directory is unaffected by this fix (still returns [] via its own pre-existing ENOENT tolerance, never calls ensureQueueDir)', async () => {
    const freshDir = join(workDir, 'peek-never-created', 'nested');
    const freshQueuePath = join(freshDir, 'resource-queue.json');

    await expect(peekQueue(freshQueuePath)).resolves.toEqual([]);
    // peekQueue never creates the directory — it has no write path at all.
    expect(existsSync(freshDir)).toBe(false);
  });

  // chmod is not a permission barrier for uid 0, so a root runner cannot
  // exhibit this — mirrors coordination-file.jest.spec.mjs's own
  // runningAsRoot guard idiom exactly.
  const runningAsRoot = typeof process.getuid === 'function' && process.getuid() === 0;
  const maybeIt = runningAsRoot ? it.skip : it;

  maybeIt(
    'a genuine permission failure (queue directory exists but is unwritable) still propagates as an error — the fix must not swallow it into a false null',
    async () => {
      const readonlyDir = join(workDir, 'readonly-parent');
      await fs.mkdir(readonlyDir, { recursive: true });
      const lockedQueuePath = join(readonlyDir, 'resource-queue.json');

      // The directory already exists, so `ensureQueueDir`'s
      // `mkdir(..., { recursive: true })` is a no-op here regardless of
      // permissions (mkdir on an already-existing path never needs write
      // access to that path itself) — it's `acquireLock`'s own
      // `fs.open(lockPath, 'wx')`, which needs to CREATE a new `.lock` file
      // inside `readonlyDir`, that hits the real EACCES.
      await fs.chmod(readonlyDir, 0o555);
      try {
        await expect(dequeueItem(lockedQueuePath)).rejects.toMatchObject({ code: 'EACCES' });
      } finally {
        await fs.chmod(readonlyDir, 0o755);
      }
    },
  );
});

// ---------------------------------------------------------------------------
// 12. Partial write / process death mid-write — reuses writeEntriesAtomic's
// tmp-file-then-rename mechanism verbatim; the queue file is never observed
// half-written.
// ---------------------------------------------------------------------------

describe('partial write / process death mid-write never leaves a half-written queue file', () => {
  it('a burst of concurrent enqueues leaves a fully parseable file on disk afterward (proof of atomic rename, not in-place mutation)', async () => {
    const now = Date.now();

    await Promise.all(
      Array.from({ length: 15 }, (_, index) =>
        enqueueItem(queueFilePath, validItem({ agentClass: `orc-${index}`, enqueuedAt: now + index })),
      ),
    );

    const raw = readFileSync(queueFilePath, 'utf8');
    expect(() => JSON.parse(raw)).not.toThrow();
    expect(JSON.parse(raw)).toHaveLength(15);
  });

  it('does not leave an orphaned .tmp-* sibling file behind after a normal enqueueItem call', async () => {
    await enqueueItem(queueFilePath, validItem());

    const siblings = await fs.readdir(workDir);
    const tmpSiblings = siblings.filter((name) => name.startsWith('resource-queue.json.tmp-'));

    expect(tmpSiblings).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// 13. Priority + enqueuedAt tie-break, including same-millisecond collisions
// ---------------------------------------------------------------------------

describe('priority + enqueuedAt tie-break, including same-millisecond collisions', () => {
  // enqueuedAt is now server-assigned via `new Date().toISOString()` inside
  // enqueueItem itself, and any caller-supplied value is ignored — so a
  // genuine same-millisecond collision can no longer be forced by passing
  // `enqueuedAt` in. Instead, mock `Date.prototype.toISOString` to return a
  // fixed value for the duration of these tests, which reliably forces every
  // enqueueItem call in the test to collide, without touching `Date.now()`
  // or any setTimeout-based timing the lock-acquisition machinery relies on.
  let toISOStringSpy;

  beforeEach(() => {
    toISOStringSpy = jest.spyOn(Date.prototype, 'toISOString').mockReturnValue('2024-01-01T00:00:00.000Z');
  });

  afterEach(() => {
    toISOStringSpy.mockRestore();
  });

  it('two entries with identical priority and identical enqueuedAt are ordered by insertion order (documented tiebreak: stable sort over on-disk array order)', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'first-in', priority: 'normal' }));
    await enqueueItem(queueFilePath, validItem({ agentClass: 'second-in', priority: 'normal' }));

    const items = await peekQueue(queueFilePath);

    expect(items.every((item) => item.enqueuedAt === '2024-01-01T00:00:00.000Z')).toBe(true);
    expect(items.map((item) => item.agentClass)).toEqual(['first-in', 'second-in']);
  });

  it('the same tiebreak ordering is stable across repeated peekQueue calls (deterministic, not sort-order-flaky)', async () => {
    const agentClasses = ['a', 'b', 'c', 'd'];

    for (const agentClass of agentClasses) {
      await enqueueItem(queueFilePath, validItem({ agentClass, priority: 'high' }));
    }

    const firstPeek = await peekQueue(queueFilePath);
    const secondPeek = await peekQueue(queueFilePath);

    expect(firstPeek.map((item) => item.agentClass)).toEqual(agentClasses);
    expect(secondPeek.map((item) => item.agentClass)).toEqual(agentClasses);
  });
});

// ---------------------------------------------------------------------------
// 14. Peek-then-dequeue TOCTOU — dequeueItem always re-reads under its own
// lock rather than trusting a prior peekQueue snapshot.
// ---------------------------------------------------------------------------

describe('peek-then-dequeue TOCTOU safety', () => {
  it('a dequeueItem call after a stale peekQueue snapshot returns whatever is CURRENTLY the top entry, never the stale snapshot\'s view', async () => {
    const now = Date.now();
    await enqueueItem(queueFilePath, validItem({ agentClass: 'was-top', priority: 'high', enqueuedAt: now }));
    await enqueueItem(queueFilePath, validItem({ agentClass: 'was-second', priority: 'normal', enqueuedAt: now + 1 }));

    const staleSnapshot = await peekQueue(queueFilePath);
    expect(staleSnapshot[0].agentClass).toBe('was-top');

    // Something else dequeues the true top entry in between — the snapshot
    // above is now stale.
    const somethingElseDequeued = await dequeueItem(queueFilePath);
    expect(somethingElseDequeued.agentClass).toBe('was-top');

    // A subsequent dequeueItem must reflect the CURRENT state, not the stale
    // snapshot — never re-return 'was-top', never throw.
    const nextDequeued = await dequeueItem(queueFilePath);
    expect(nextDequeued.agentClass).toBe('was-second');
  });

  it('never throws when the queue empties out between a peekQueue call and a subsequent dequeueItem call', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'only-one' }));

    const snapshot = await peekQueue(queueFilePath);
    expect(snapshot).toHaveLength(1);

    // Drain it via a separate dequeue before the "real" dequeue below runs.
    await dequeueItem(queueFilePath);

    await expect(dequeueItem(queueFilePath)).resolves.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// 15. Lock-ownership re-verification (assertStillHeld) reused, not reimplemented
// ---------------------------------------------------------------------------

describe('queue.mjs writes go through coordination-file.mjs\'s reused writeEntriesAtomic/assertStillHeld primitives (structural)', () => {
  it('the module source imports writeEntriesAtomic and assertStillHeld from ./coordination-file.mjs rather than calling fs.writeFile directly for the queue file', () => {
    // Deliberately no try/catch: ENOENT (module not yet created) is itself
    // the expected RED failure for this suite.
    const queueSource = readFileSync(new URL('./queue.mjs', import.meta.url), 'utf8');

    expect(queueSource).toMatch(/from ['"]\.\/coordination-file\.mjs['"]/);
    expect(queueSource).toMatch(/writeEntriesAtomic/);
    expect(queueSource).toMatch(/assertStillHeld/);
  });
});

// ---------------------------------------------------------------------------
// 16. Corrupted queue file — a typed error, never silent data loss, and
// never an atomic-overwrite-with-just-the-new-entry.
// ---------------------------------------------------------------------------

describe('a corrupted (non-JSON or non-array) EXISTING queue file surfaces a typed QueueCorruptFileError rather than being silently treated as empty', () => {
  it('peekQueue throws QueueCorruptFileError (.code === "QUEUE_CORRUPT_FILE") for non-JSON content', async () => {
    writeFileSync(queueFilePath, 'this is not json{{{', 'utf8');

    await expect(peekQueue(queueFilePath)).rejects.toBeInstanceOf(QueueCorruptFileError);
    await expect(peekQueue(queueFilePath)).rejects.toMatchObject({ code: 'QUEUE_CORRUPT_FILE' });
  });

  it('peekQueue throws QueueCorruptFileError for valid JSON that is not an array (e.g. an object)', async () => {
    writeFileSync(queueFilePath, JSON.stringify({ not: 'an array' }), 'utf8');

    await expect(peekQueue(queueFilePath)).rejects.toMatchObject({ code: 'QUEUE_CORRUPT_FILE' });
  });

  it('dequeueItem throws QueueCorruptFileError for a corrupted file rather than treating it as an empty queue', async () => {
    writeFileSync(queueFilePath, 'not json at all', 'utf8');

    await expect(dequeueItem(queueFilePath)).rejects.toMatchObject({ code: 'QUEUE_CORRUPT_FILE' });
  });

  it('enqueueItem throws QueueCorruptFileError and does NOT overwrite the corrupted file with just the new entry — the original corrupt bytes are left untouched, preventing silent data loss', async () => {
    const corruptContent = 'totally not json';
    writeFileSync(queueFilePath, corruptContent, 'utf8');

    await expect(
      enqueueItem(queueFilePath, validItem({ agentClass: 'would-clobber' })),
    ).rejects.toMatchObject({ code: 'QUEUE_CORRUPT_FILE' });

    expect(readFileSync(queueFilePath, 'utf8')).toBe(corruptContent);
  });

  it('an empty-string file (0 bytes) is still treated as "nothing here" ([]), not corruption — matches the pre-existing empty-file tolerance', async () => {
    writeFileSync(queueFilePath, '', 'utf8');

    await expect(peekQueue(queueFilePath)).resolves.toEqual([]);
  });

  it('a genuinely MISSING file (never created) is NOT corruption — still returns [] / null, never throws', async () => {
    // Regression guard: distinguishes ENOENT (ok) from corruption (throws) —
    // the two must never be conflated in either direction.
    await expect(peekQueue(queueFilePath)).resolves.toEqual([]);
    await expect(dequeueItem(queueFilePath)).resolves.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// enqueueItem/dequeueItem's `options.lockStalenessMs` actually reaches
// `acquireLock` — Medium review finding: the JSDoc typed `options` but the
// parameter was previously discarded (`void options`) and never threaded
// through to `withQueueLock`, unlike `declareDibs`'s genuine threading of
// the equivalent option in coordination-file.mjs. Mirrors
// coordination-file.jest.spec.mjs's "stale lock reclaim" idiom: seed an
// orphaned `.lock` file with an old `acquiredAt`, then prove a short custom
// `lockStalenessMs` is honored (reclaims immediately) where the module
// default (30s) would not be.
// ---------------------------------------------------------------------------

describe('enqueueItem/dequeueItem honor a caller-supplied options.lockStalenessMs', () => {
  it('enqueueItem reclaims an orphaned .lock file older than a short custom lockStalenessMs, rather than waiting out the 30s default', async () => {
    const lockPath = `${queueFilePath}.lock`;
    await fs.mkdir(workDir, { recursive: true });
    writeFileSync(
      lockPath,
      JSON.stringify({ pid: 999999, acquiredAt: Date.now() - 10_000, token: 'stale-token' }),
    );

    const persisted = await enqueueItem(queueFilePath, validItem({ agentClass: 'after-reclaim' }), {
      lockStalenessMs: 100,
    });

    expect(persisted.agentClass).toBe('after-reclaim');
    const items = await peekQueue(queueFilePath);
    expect(items.map((item) => item.agentClass)).toContain('after-reclaim');
  }, 10_000);

  it('dequeueItem reclaims an orphaned .lock file older than a short custom lockStalenessMs', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'only-item' }));

    const lockPath = `${queueFilePath}.lock`;
    writeFileSync(
      lockPath,
      JSON.stringify({ pid: 999999, acquiredAt: Date.now() - 10_000, token: 'stale-token' }),
    );

    const dequeued = await dequeueItem(queueFilePath, { lockStalenessMs: 100 });

    expect(dequeued.agentClass).toBe('only-item');
  }, 10_000);

  it('a fresh (non-stale) .lock file surfaces COORDINATION_LOCK_TIMEOUT when lockMaxAttempts/lockRetryDelayMs are tightened via options — proving those two are threaded through as well, not just lockStalenessMs', async () => {
    const lockPath = `${queueFilePath}.lock`;
    writeFileSync(
      lockPath,
      JSON.stringify({ pid: process.pid, acquiredAt: Date.now(), token: 'held-by-someone-else' }),
    );

    await expect(
      enqueueItem(queueFilePath, validItem(), {
        lockStalenessMs: 60_000,
        lockMaxAttempts: 3,
        lockRetryDelayMs: 1,
      }),
    ).rejects.toMatchObject({ code: 'COORDINATION_LOCK_TIMEOUT' });
  });
});

// ---------------------------------------------------------------------------
// 17. sortQueueEntries never produces NaN from an out-of-enum on-disk
// `priority` — Medium review finding: `PRIORITY_RANK[a.priority]` is
// `undefined` for an unrecognised value, so the un-guarded subtraction was
// `undefined - n = NaN`, and `Array.prototype.sort`'s behaviour for a
// comparator that ever returns NaN is comparator-implementation-dependent
// (i.e. non-deterministic-in-practice) — verified below to have actually let
// an unknown priority sort AHEAD of "high" prior to the fix. The fix treats
// any priority not in the low|normal|high enum as the lowest rank: it always
// sorts last, never wins a dequeue.
// ---------------------------------------------------------------------------

describe('sortQueueEntries (via peekQueue/dequeueItem) treats an out-of-enum on-disk priority as lowest-rank, never NaN-orders it', () => {
  it('an entry with an unrecognised priority ("urgent") sorts AFTER every valid priority, including "low"', async () => {
    await enqueueItem(queueFilePath, validItem({ agentClass: 'valid-low', priority: 'low' }));
    await enqueueItem(queueFilePath, validItem({ agentClass: 'valid-high', priority: 'high' }));

    // Hand-write a third entry directly to disk with an out-of-enum
    // priority — enqueueItem itself would reject "urgent" (QUEUE_INVALID_
    // PRIORITY), so this simulates a hand-edited file / a queue written by a
    // differently-versioned CLI, per the finding's own scenario.
    const onDisk = JSON.parse(readFileSync(queueFilePath, 'utf8'));
    onDisk.push({
      agentClass: 'unrecognised-priority',
      priority: 'urgent',
      commandRef: 'echo hi',
      orchestratorId: 'test-orchestrator',
      enqueuedAt: new Date().toISOString(),
    });
    writeFileSync(queueFilePath, JSON.stringify(onDisk), 'utf8');

    const items = await peekQueue(queueFilePath);

    expect(items.map((item) => item.agentClass)).toEqual([
      'valid-high',
      'valid-low',
      'unrecognised-priority',
    ]);
  });

  it('dequeueItem never returns the unrecognised-priority entry ahead of a valid "low"-priority entry', async () => {
    const onDisk = [
      {
        agentClass: 'unrecognised-priority',
        priority: 'urgent',
        commandRef: 'echo hi',
        orchestratorId: 'test-orchestrator',
        enqueuedAt: new Date().toISOString(),
      },
      {
        agentClass: 'valid-low',
        priority: 'low',
        commandRef: 'echo hi',
        orchestratorId: 'test-orchestrator',
        enqueuedAt: new Date().toISOString(),
      },
    ];
    writeFileSync(queueFilePath, JSON.stringify(onDisk), 'utf8');

    const dequeued = await dequeueItem(queueFilePath);

    expect(dequeued.agentClass).toBe('valid-low');
  });
});

// ---------------------------------------------------------------------------
// 17b. Prototype-inherited-key priority values — a security-adjacent
// regression guard. `PRIORITY_RANK` is a lookup object; a priority value
// naming an Object.prototype-inherited member (e.g. "toString",
// "constructor", "valueOf", "hasOwnProperty", "__proto__") would resolve to
// an inherited Function via plain-object lookup, which `?? UNKNOWN_
// PRIORITY_RANK` does NOT catch (`??` only fires on null/undefined) —
// reintroducing the NaN-comparator bug section 17 guards against, for this
// specific class of on-disk value. Confirms each such value is treated as
// an ordinary out-of-enum/unknown priority (sorts last), never as an
// inherited member and never producing NaN/inversion.
// ---------------------------------------------------------------------------

describe('sortQueueEntries treats Object.prototype-inherited-key priority values as out-of-enum, never resolving an inherited member', () => {
  const inheritedKeyPriorities = ['toString', '__proto__', 'constructor', 'valueOf', 'hasOwnProperty'];

  it.each(inheritedKeyPriorities)(
    'an entry with priority %j sorts AFTER a genuine "high" entry, never ahead of it',
    async (priority) => {
      const highEntry = {
        agentClass: 'valid-high',
        priority: 'high',
        commandRef: 'echo hi',
        orchestratorId: 'test-orchestrator',
        enqueuedAt: '2024-01-01T00:00:00.000Z',
      };
      const inheritedKeyEntry = {
        agentClass: 'inherited-key-priority',
        priority,
        commandRef: 'echo hi',
        orchestratorId: 'test-orchestrator',
        enqueuedAt: '2024-01-01T00:00:01.000Z',
      };
      writeFileSync(queueFilePath, JSON.stringify([inheritedKeyEntry, highEntry]), 'utf8');

      const items = await peekQueue(queueFilePath);

      expect(items.map((item) => item.agentClass)).toEqual(['valid-high', 'inherited-key-priority']);
    },
  );

  it('dequeueItem never returns an inherited-key-priority entry ahead of a valid "low"-priority entry', async () => {
    const onDisk = [
      {
        agentClass: 'inherited-key-priority',
        priority: 'toString',
        commandRef: 'echo hi',
        orchestratorId: 'test-orchestrator',
        enqueuedAt: '2024-01-01T00:00:00.000Z',
      },
      {
        agentClass: 'valid-low',
        priority: 'low',
        commandRef: 'echo hi',
        orchestratorId: 'test-orchestrator',
        enqueuedAt: '2024-01-01T00:00:01.000Z',
      },
    ];
    writeFileSync(queueFilePath, JSON.stringify(onDisk), 'utf8');

    const dequeued = await dequeueItem(queueFilePath);

    expect(dequeued.agentClass).toBe('valid-low');
  });
});

// ---------------------------------------------------------------------------
// 18. Element-level corruption in the queue file — a malformed array
// element (e.g. `null`) surfaces the typed QueueCorruptFileError rather than
// a raw TypeError, for peekQueue/dequeueItem AND enqueueItem alike (so a
// corrupt queue can never be silently appended on top of, wedging it
// permanently undrainable).
// ---------------------------------------------------------------------------

describe('a malformed (non-object/null) element within an otherwise-valid JSON array surfaces the typed QueueCorruptFileError, never a raw TypeError', () => {
  it('peekQueue throws QueueCorruptFileError (.code === "QUEUE_CORRUPT_FILE"), not a raw TypeError, for [null, validEntry]', async () => {
    const validEntry = {
      agentClass: 'valid',
      priority: 'normal',
      commandRef: 'echo hi',
      orchestratorId: 'test-orchestrator',
      enqueuedAt: new Date().toISOString(),
    };
    writeFileSync(queueFilePath, JSON.stringify([null, validEntry]), 'utf8');

    await expect(peekQueue(queueFilePath)).rejects.toBeInstanceOf(QueueCorruptFileError);
    await expect(peekQueue(queueFilePath)).rejects.toMatchObject({ code: 'QUEUE_CORRUPT_FILE' });
  });

  it('dequeueItem throws QueueCorruptFileError, not a raw TypeError, for [null, validEntry]', async () => {
    const validEntry = {
      agentClass: 'valid',
      priority: 'high',
      commandRef: 'echo hi',
      orchestratorId: 'test-orchestrator',
      enqueuedAt: new Date().toISOString(),
    };
    writeFileSync(queueFilePath, JSON.stringify([null, validEntry]), 'utf8');

    await expect(dequeueItem(queueFilePath)).rejects.toMatchObject({ code: 'QUEUE_CORRUPT_FILE' });
  });

  it('a single-element [null] array (masked by Array.prototype.sort skipping the comparator) still throws via the shape check, not just once a second item is added', async () => {
    writeFileSync(queueFilePath, JSON.stringify([null]), 'utf8');

    await expect(peekQueue(queueFilePath)).rejects.toMatchObject({ code: 'QUEUE_CORRUPT_FILE' });
  });

  it('a non-object primitive element (e.g. a bare string) is also treated as corrupt', async () => {
    writeFileSync(queueFilePath, JSON.stringify(['not-an-entry']), 'utf8');

    await expect(peekQueue(queueFilePath)).rejects.toMatchObject({ code: 'QUEUE_CORRUPT_FILE' });
  });

  it('enqueueItem against a queue file with a malformed element fails cleanly (QUEUE_CORRUPT_FILE) rather than silently appending on top of it, leaving the corrupt bytes untouched', async () => {
    const corruptContent = JSON.stringify([null]);
    writeFileSync(queueFilePath, corruptContent, 'utf8');

    await expect(
      enqueueItem(queueFilePath, validItem({ agentClass: 'would-clobber' })),
    ).rejects.toMatchObject({ code: 'QUEUE_CORRUPT_FILE' });

    expect(readFileSync(queueFilePath, 'utf8')).toBe(corruptContent);
  });
});
