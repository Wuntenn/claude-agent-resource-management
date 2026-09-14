// Phase 2 — persisted queue-aging escalation (priority bump + alert).
//
// RED unit tests for the not-yet-implemented
// scripts/agent-resource-management/lib/queue-aging-escalation.mjs module.
//
// Direct-import unit tests, mirroring ./queue.jest.spec.mjs's and
// ./queue-lease.jest.spec.mjs's idiom exactly: real fs, real temp
// directories (mkdtempSync under os.tmpdir()), real concurrency via
// Promise.all — never a mocked filesystem, because the whole point of this
// module is multi-writer atomicity/lock correctness against the REAL
// filesystem.
//
// This is a SEPARATE, explicitly-named concern from ./queue.mjs's existing
// ORDERING-ONLY aging (`sortQueueEntries`'s optional `now` parameter /
// `AGING_THRESHOLD_MS`, Phase 3): that mechanism NEVER mutates
// `entry.priority` on disk — it recomputes a sort-time-only rank boost fresh
// on every call. This module is the opposite: it PERSISTS a real mutation to
// `entry.priority` (a one-time, idempotent rank bump) once an entry has
// waited past a configured bound *while capacity was free*, and separately
// tracks (and persists) whether an "still stuck after the bump" alert has
// already fired for that entry, so a caller polling this module every beat
// never re-bumps or re-alerts the same entry twice. `queue.mjs`'s own
// `AGING_THRESHOLD_MS` (10 minutes) and this module's own
// `DEFAULT_ESCALATION_BOUND_MS` are deliberately independent constants, not
// aliases of one another.
//
// `lib/queue-aging-escalation.mjs` does NOT exist yet as of this commit —
// every test below is expected to fail at import time (`Cannot find module
// './queue-aging-escalation.mjs'`). Do not add an implementation to make
// this pass — the builder implements it in the next phase to turn this
// green.
//
// ---------------------------------------------------------------------------
// Contract this suite specifies for the implementer (none of this exists
// today — this file IS the spec):
//
//   DEFAULT_ESCALATION_BOUND_MS -> number (1_200_000 = 20 minutes)
//     The chosen default for `options.boundMs` when a caller doesn't
//     override it. Deliberately a DIFFERENT number from queue.mjs's
//     `AGING_THRESHOLD_MS` (10 minutes) — the two mechanisms are not the
//     same policy and must not be conflated by sharing a constant. Exported
//     for the implementer/reviewer to reconcile against or reuse directly
//     rather than restating the number.
//
//   escalateAgedQueueEntry(filePath, options?) ->
//     Promise<{ escalated: Array<object>, alerted: Array<object>, skipped: Array<{ entry: unknown, reason: string }> }>
//
//     A single "beat" over the WHOLE persisted queue file at `filePath`,
//     performed atomically under coordination-file.mjs's existing
//     lock/atomic-write primitives (`acquireLock`/`assertStillHeld`/
//     `releaseLock`/`writeEntriesAtomic`) — reused exactly as queue.mjs's own
//     `withQueueLock` reuses them, NOT a second locking mechanism invented
//     for this module.
//
//     For each well-formed entry currently in the file:
//       - Age is computed as `referenceNow - Date.parse(entry.enqueuedAt)`,
//         where `referenceNow` is EITHER a finite `options.now` (preferred,
//         for deterministic tests) or a single `Date.now()` snapshot taken
//         ONCE per call — never re-read per entry. This single-snapshot
//         discipline is a deliberate, explicitly documented choice (see the
//         "single consistent read" section below): it does not make the
//         overall "waited >= boundMs AND capacity was free throughout"
//         check atomic against a concurrently-changing world (a real
//         capacity-free/full transition could still occur between an
//         earlier caller-side capacity sample and this call's file read),
//         but it does guarantee this module's OWN age computation is
//         self-consistent across every entry considered in the same beat.
//       - An entry already carrying a truthy `escalatedAt` field is treated
//         as "already escalated" — never re-bumped in a later call, even if
//         still aged past `boundMs`.
//       - An entry NOT yet escalated is ELIGIBLE for escalation only when
//         BOTH: (a) its age is `>= options.boundMs` (inclusive boundary,
//         mirroring `queue.mjs`'s own `AGING_THRESHOLD_MS` inclusive
//         convention), AND (b) `options.capacitySamples` (an array of
//         `{ at: number, free: boolean }` observations — the injected
//         "was capacity free at time T" signal this phase's Build Plan
//         calls for) contains AT LEAST ONE `free: true` sample whose `at`
//         falls within the entry's wait window
//         `[Date.parse(entry.enqueuedAt), referenceNow]`, AND contains NO
//         `free: false` sample within that same window. An entry with zero
//         samples in its window, or only `free: false` samples, is NOT
//         escalated — "no evidence of freeness" fails closed, exactly like
//         "evidence of fullness" does. (Comment only, not test-enforced:
//         Phase 5/future work may replace this caller-supplied array with a
//         real read of dispatch.mjs's own admission-check log.)
//       - Escalating an eligible entry: bumps `priority` exactly one tier
//         toward "high" (low -> normal -> high), using the SAME rank
//         direction as queue.mjs's own sort-time aging promotion, but
//         PERSISTED this time — a `priority: 'high'` entry bump is a
//         genuine no-op for the field itself (stays `'high'`), but the
//         entry is still marked `escalatedAt` and evaluated for the alert
//         below, because "already top priority" must not silently exempt a
//         stuck entry from ever alerting. Also stamps `escalatedAt =
//         referenceNow`.
//       - Immediately after escalating (same beat), the entry is evaluated
//         for the alert: since bumping never reduces age, an entry that was
//         just escalated is by definition still waiting `>= boundMs` — so
//         the alert fires in the SAME call that performed the escalation,
//         exactly once, via `console.warn`, and the entry is stamped
//         `escalationAlertedAt = referenceNow`. A later call sees a truthy
//         `escalationAlertedAt` and never alerts again for this entry.
//       - Entries that are not well-formed (missing/mistyped
//         `agentClass`/`commandRef`/`orchestratorId`/`enqueuedAt`, or an
//         out-of-enum `priority` — mirrors `QueueInvalidPriorityError`'s
//         existing enum in queue.mjs: `'low' | 'normal' | 'high'`) are
//         NEVER escalated, NEVER alerted, and NEVER cause the call to
//         throw — they are left byte-for-byte untouched in the persisted
//         file and reported in the returned `skipped` array as
//         `{ entry, reason }`.
//       - The whole file is rewritten atomically in one
//         `writeEntriesAtomic` call per `escalateAgedQueueEntry` call (not
//         once per escalated entry) — an all-or-nothing write. If the
//         fencing lock is lost mid-write (`LockLostError`, per
//         coordination-file.mjs), NOTHING is written: the file on disk is
//         left exactly as it was before the call, never partially bumped.
//
//     `options`: `{ now?: number, boundMs?: number, capacitySamples?:
//     Array<{ at: number, free: boolean }>, lockStalenessMs?: number,
//     lockMaxAttempts?: number, lockRetryDelayMs?: number }`. `boundMs`
//     defaults to `DEFAULT_ESCALATION_BOUND_MS`; `capacitySamples` defaults
//     to `[]` (so, with no samples supplied, nothing is ever escalated —
//     fails closed by default, never escalates "by accident" on a caller
//     that forgot to wire the capacity signal).
// ---------------------------------------------------------------------------

import { mkdtempSync, rmSync, writeFileSync, readFileSync, existsSync, promises as fsPromises } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { jest } from '@jest/globals';

import { enqueueItem } from './queue.mjs';

let workDir;
let queueFilePath;
let warnSpy;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'queue-aging-escalation-lib-'));
  queueFilePath = join(workDir, 'resource-queue.json');
  warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
});

afterEach(() => {
  warnSpy.mockRestore();
  rmSync(workDir, { recursive: true, force: true });
});

const NOW = 1_700_000_000_000;

function entryFixture({ agentClass, priority, ageOffsetMs, overrides = {} }) {
  return {
    agentClass,
    priority,
    commandRef: `echo ${agentClass}`,
    orchestratorId: 'test-orchestrator',
    enqueuedAt: new Date(NOW - ageOffsetMs).toISOString(),
    ...overrides,
  };
}

function writeQueueFile(entries) {
  writeFileSync(queueFilePath, JSON.stringify(entries), 'utf8');
}

function readQueueFile() {
  return JSON.parse(readFileSync(queueFilePath, 'utf8'));
}

function freeSampleWindow(startMs, endMs, count = 3) {
  const step = (endMs - startMs) / Math.max(1, count - 1);
  return Array.from({ length: count }, (_, index) => ({ at: startMs + index * step, free: true }));
}

// ---------------------------------------------------------------------------
// 0. Pinned default bound constant
// ---------------------------------------------------------------------------

describe('DEFAULT_ESCALATION_BOUND_MS', () => {
  it('is exported as 1_200_000 (20 minutes) — the chosen default bound this suite pins, deliberately distinct from queue.mjs AGING_THRESHOLD_MS', async () => {
    const { DEFAULT_ESCALATION_BOUND_MS } = await import('./queue-aging-escalation.mjs');
    expect(DEFAULT_ESCALATION_BOUND_MS).toBe(20 * 60 * 1000);
  });
});

// ---------------------------------------------------------------------------
// 1. Scenario: bumps priority once past bound with capacity free
// ---------------------------------------------------------------------------

describe('scenario: bumps priority once past bound with capacity free', () => {
  it('bumps a low-priority entry to normal, stamps escalatedAt, and reports it in the escalated array', async () => {
    const { escalateAgedQueueEntry, DEFAULT_ESCALATION_BOUND_MS } = await import('./queue-aging-escalation.mjs');
    const ageOffsetMs = DEFAULT_ESCALATION_BOUND_MS + 60_000;
    await enqueueItem(
      queueFilePath,
      { agentClass: 'stuck-low', priority: 'low', commandRef: 'echo stuck-low', orchestratorId: 'orc-1' },
    );
    // Backdate enqueuedAt directly on disk so the fixture is deterministic
    // relative to NOW, mirroring queue-aging.jest.spec.mjs's approach of
    // hand-constructing exact ages rather than sleeping in real time.
    const seeded = readQueueFile();
    seeded[0].enqueuedAt = new Date(NOW - ageOffsetMs).toISOString();
    writeQueueFile(seeded);

    const result = await escalateAgedQueueEntry(queueFilePath, {
      now: NOW,
      boundMs: DEFAULT_ESCALATION_BOUND_MS,
      capacitySamples: freeSampleWindow(NOW - ageOffsetMs, NOW),
    });

    expect(result.escalated).toHaveLength(1);
    expect(result.escalated[0].agentClass).toBe('stuck-low');

    const persisted = readQueueFile();
    expect(persisted[0].priority).toBe('normal');
    expect(persisted[0].escalatedAt).toBe(NOW);
  });

  it('bumps a normal-priority entry to high under the same conditions', async () => {
    const { escalateAgedQueueEntry, DEFAULT_ESCALATION_BOUND_MS } = await import('./queue-aging-escalation.mjs');
    const boundMs = 60_000;
    const ageOffsetMs = boundMs + 5_000;
    writeQueueFile([entryFixture({ agentClass: 'stuck-normal', priority: 'normal', ageOffsetMs })]);

    await escalateAgedQueueEntry(queueFilePath, {
      now: NOW,
      boundMs,
      capacitySamples: freeSampleWindow(NOW - ageOffsetMs, NOW),
    });

    const persisted = readQueueFile();
    expect(persisted[0].priority).toBe('high');
    expect(persisted[0].escalatedAt).toBe(NOW);
  });

  it('an entry that has NOT yet waited boundMs is left untouched, even with capacity free throughout', async () => {
    const { escalateAgedQueueEntry } = await import('./queue-aging-escalation.mjs');
    const boundMs = 60_000;
    const ageOffsetMs = boundMs - 1;
    writeQueueFile([entryFixture({ agentClass: 'not-yet-aged', priority: 'low', ageOffsetMs })]);

    const result = await escalateAgedQueueEntry(queueFilePath, {
      now: NOW,
      boundMs,
      capacitySamples: freeSampleWindow(NOW - ageOffsetMs, NOW),
    });

    expect(result.escalated).toHaveLength(0);
    expect(result.alerted).toHaveLength(0);
    const persisted = readQueueFile();
    expect(persisted[0].priority).toBe('low');
    expect(persisted[0].escalatedAt).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// 2. Scenario: does not escalate when capacity was full throughout wait
// ---------------------------------------------------------------------------

describe('scenario: does not escalate when capacity was full throughout wait', () => {
  it('leaves priority and escalatedAt untouched when every capacity sample in the wait window reports full', async () => {
    const { escalateAgedQueueEntry } = await import('./queue-aging-escalation.mjs');
    const boundMs = 60_000;
    const ageOffsetMs = boundMs + 30_000;
    writeQueueFile([entryFixture({ agentClass: 'genuinely-blocked', priority: 'low', ageOffsetMs })]);

    const fullSamples = Array.from({ length: 4 }, (_, index) => ({
      at: NOW - ageOffsetMs + index * (ageOffsetMs / 3),
      free: false,
    }));

    const result = await escalateAgedQueueEntry(queueFilePath, {
      now: NOW,
      boundMs,
      capacitySamples: fullSamples,
    });

    expect(result.escalated).toHaveLength(0);
    expect(result.alerted).toHaveLength(0);
    const persisted = readQueueFile();
    expect(persisted[0].priority).toBe('low');
    expect(persisted[0].escalatedAt).toBeUndefined();
    expect(warnSpy).not.toHaveBeenCalled();
  });

  it('does not escalate when there are ZERO capacity samples at all — no evidence of freeness fails closed', async () => {
    const { escalateAgedQueueEntry } = await import('./queue-aging-escalation.mjs');
    const boundMs = 60_000;
    writeQueueFile([entryFixture({ agentClass: 'no-samples', priority: 'low', ageOffsetMs: boundMs + 10_000 })]);

    const result = await escalateAgedQueueEntry(queueFilePath, { now: NOW, boundMs });

    expect(result.escalated).toHaveLength(0);
    const persisted = readQueueFile();
    expect(persisted[0].escalatedAt).toBeUndefined();
  });

  it('does not escalate when a SINGLE free:false sample falls inside an otherwise free-looking window', async () => {
    const { escalateAgedQueueEntry } = await import('./queue-aging-escalation.mjs');
    const boundMs = 60_000;
    const ageOffsetMs = boundMs + 20_000;
    writeQueueFile([entryFixture({ agentClass: 'briefly-full', priority: 'low', ageOffsetMs })]);

    const mixedSamples = [
      { at: NOW - ageOffsetMs, free: true },
      { at: NOW - ageOffsetMs / 2, free: false },
      { at: NOW, free: true },
    ];

    const result = await escalateAgedQueueEntry(queueFilePath, { now: NOW, boundMs, capacitySamples: mixedSamples });

    expect(result.escalated).toHaveLength(0);
    const persisted = readQueueFile();
    expect(persisted[0].escalatedAt).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// 3. Scenario: alerts once, not repeatedly, after bump
// ---------------------------------------------------------------------------

describe('scenario: alerts once, not repeatedly, after bump', () => {
  it('emits exactly one console.warn on the beat that performs the escalation', async () => {
    const { escalateAgedQueueEntry } = await import('./queue-aging-escalation.mjs');
    const boundMs = 60_000;
    const ageOffsetMs = boundMs + 15_000;
    writeQueueFile([entryFixture({ agentClass: 'alert-once', priority: 'low', ageOffsetMs })]);

    const result = await escalateAgedQueueEntry(queueFilePath, {
      now: NOW,
      boundMs,
      capacitySamples: freeSampleWindow(NOW - ageOffsetMs, NOW),
    });

    expect(result.alerted).toHaveLength(1);
    expect(result.alerted[0].agentClass).toBe('alert-once');
    expect(warnSpy).toHaveBeenCalledTimes(1);
    const persisted = readQueueFile();
    expect(persisted[0].escalationAlertedAt).toBe(NOW);
  });

  it('does NOT re-alert on a second beat once the entry is already escalated/alerted, even though it is still stuck', async () => {
    const { escalateAgedQueueEntry } = await import('./queue-aging-escalation.mjs');
    const boundMs = 60_000;
    const ageOffsetMs = boundMs + 15_000;
    writeQueueFile([entryFixture({ agentClass: 'still-stuck', priority: 'low', ageOffsetMs })]);

    await escalateAgedQueueEntry(queueFilePath, {
      now: NOW,
      boundMs,
      capacitySamples: freeSampleWindow(NOW - ageOffsetMs, NOW),
    });
    expect(warnSpy).toHaveBeenCalledTimes(1);

    const laterNow = NOW + 30_000;
    const second = await escalateAgedQueueEntry(queueFilePath, {
      now: laterNow,
      boundMs,
      capacitySamples: freeSampleWindow(NOW - ageOffsetMs, laterNow),
    });

    expect(second.escalated).toHaveLength(0);
    expect(second.alerted).toHaveLength(0);
    expect(warnSpy).toHaveBeenCalledTimes(1);

    const persisted = readQueueFile();
    expect(persisted[0].escalationAlertedAt).toBe(NOW);
  });

  it('does not disturb a DIFFERENT, not-yet-escalated entry while skipping the already-alerted one', async () => {
    const { escalateAgedQueueEntry } = await import('./queue-aging-escalation.mjs');
    const boundMs = 60_000;
    const ageOffsetMs = boundMs + 15_000;
    writeQueueFile([
      entryFixture({ agentClass: 'first-round', priority: 'low', ageOffsetMs }),
      entryFixture({ agentClass: 'second-round', priority: 'low', ageOffsetMs: boundMs - 1 }),
    ]);

    await escalateAgedQueueEntry(queueFilePath, {
      now: NOW,
      boundMs,
      capacitySamples: freeSampleWindow(NOW - ageOffsetMs, NOW),
    });

    const laterNow = NOW + boundMs;
    const second = await escalateAgedQueueEntry(queueFilePath, {
      now: laterNow,
      boundMs,
      capacitySamples: freeSampleWindow(NOW - ageOffsetMs, laterNow),
    });

    expect(second.escalated.map((entry) => entry.agentClass)).toEqual(['second-round']);
  });
});

// ---------------------------------------------------------------------------
// 4. Scenario: no-ops the rank-bump but still alerts when already high priority
// ---------------------------------------------------------------------------

describe('scenario: no-ops the rank-bump but still alerts when already high priority', () => {
  it('leaves priority as "high" (no-op) but still stamps escalatedAt/escalationAlertedAt and fires the alert', async () => {
    const { escalateAgedQueueEntry } = await import('./queue-aging-escalation.mjs');
    const boundMs = 60_000;
    const ageOffsetMs = boundMs + 45_000;
    writeQueueFile([entryFixture({ agentClass: 'already-top', priority: 'high', ageOffsetMs })]);

    const result = await escalateAgedQueueEntry(queueFilePath, {
      now: NOW,
      boundMs,
      capacitySamples: freeSampleWindow(NOW - ageOffsetMs, NOW),
    });

    expect(result.escalated).toHaveLength(1);
    expect(result.alerted).toHaveLength(1);
    expect(warnSpy).toHaveBeenCalledTimes(1);

    const persisted = readQueueFile();
    expect(persisted[0].priority).toBe('high');
    expect(persisted[0].escalatedAt).toBe(NOW);
    expect(persisted[0].escalationAlertedAt).toBe(NOW);
  });
});

// ---------------------------------------------------------------------------
// 5. Scenario: skips corrupt entries without throwing
// ---------------------------------------------------------------------------

describe('scenario: skips corrupt entries without throwing', () => {
  it('never throws for an out-of-enum priority, a missing commandRef, or a null element — reports each in skipped, leaves them byte-for-byte untouched, and still processes the well-formed entries', async () => {
    const { escalateAgedQueueEntry } = await import('./queue-aging-escalation.mjs');
    const boundMs = 60_000;
    const ageOffsetMs = boundMs + 20_000;

    const invalidPriorityEntry = entryFixture({ agentClass: 'bad-priority', priority: 'urgent', ageOffsetMs });
    const missingCommandRefEntry = {
      agentClass: 'no-command-ref',
      priority: 'low',
      orchestratorId: 'test-orchestrator',
      enqueuedAt: new Date(NOW - ageOffsetMs).toISOString(),
    };
    const wellFormedEntry = entryFixture({ agentClass: 'well-formed', priority: 'low', ageOffsetMs });

    writeQueueFile([invalidPriorityEntry, missingCommandRefEntry, null, wellFormedEntry]);

    let result;
    await expect(
      (async () => {
        result = await escalateAgedQueueEntry(queueFilePath, {
          now: NOW,
          boundMs,
          capacitySamples: freeSampleWindow(NOW - ageOffsetMs, NOW),
        });
      })(),
    ).resolves.not.toThrow();

    expect(result.skipped).toHaveLength(3);
    expect(result.escalated.map((entry) => entry.agentClass)).toEqual(['well-formed']);

    const persisted = readQueueFile();
    expect(persisted[0]).toEqual(invalidPriorityEntry);
    expect(persisted[1]).toEqual(missingCommandRefEntry);
    expect(persisted[2]).toBeNull();
    expect(persisted[3].priority).toBe('normal');
  });
});

// ---------------------------------------------------------------------------
// 6. Scenario: escalation state persists across process restarts
// ---------------------------------------------------------------------------

describe('scenario: escalation state persists across process restarts (re-reads the queue/coordination file, not in-memory only)', () => {
  it('a fresh call with no shared in-memory state still recognises escalatedAt/escalationAlertedAt persisted by an earlier call and does not re-escalate or re-alert', async () => {
    const { escalateAgedQueueEntry } = await import('./queue-aging-escalation.mjs');
    const boundMs = 60_000;
    const ageOffsetMs = boundMs + 15_000;
    writeQueueFile([entryFixture({ agentClass: 'restart-safe', priority: 'low', ageOffsetMs })]);

    await escalateAgedQueueEntry(queueFilePath, {
      now: NOW,
      boundMs,
      capacitySamples: freeSampleWindow(NOW - ageOffsetMs, NOW),
    });

    // Re-import fresh from disk state only — no reference to any object or
    // closure from the first call is retained; this call constructs its own
    // fresh options bag and reads the queue file from scratch, exactly as a
    // brand-new process invocation would.
    const persistedAfterFirst = readQueueFile();
    expect(persistedAfterFirst[0].escalatedAt).toBe(NOW);

    const laterNow = NOW + 5 * 60_000;
    const second = await escalateAgedQueueEntry(queueFilePath, {
      now: laterNow,
      boundMs,
      capacitySamples: freeSampleWindow(NOW - ageOffsetMs, laterNow),
    });

    expect(second.escalated).toHaveLength(0);
    expect(second.alerted).toHaveLength(0);
    expect(warnSpy).toHaveBeenCalledTimes(1);
  });
});

// ---------------------------------------------------------------------------
// 7. Edge case #4: LockLostError mid-write is all-or-nothing — never a
// partial/stale escalation write.
// ---------------------------------------------------------------------------

/**
 * Polls for `path` to exist, rather than a fixed sleep — mirrors
 * ./history.jest.spec.mjs's `waitForFile` helper exactly.
 *
 * @param {string} path
 * @param {number} [timeoutMs]
 * @returns {Promise<boolean>}
 */
async function waitForFile(path, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (existsSync(path)) return true;
    // eslint-disable-next-line no-await-in-loop -- deliberate poll loop.
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  return false;
}

describe('edge case: a LockLostError mid-write leaves the queue file completely untouched — all-or-nothing, never a partial escalation write', () => {
  it('rejects with LockLostError and persists none of the intended priority bumps/alerts when the lock file is deleted out from under an in-flight call', async () => {
    const { escalateAgedQueueEntry } = await import('./queue-aging-escalation.mjs');
    const boundMs = 60_000;
    const ageOffsetMs = boundMs + 20_000;

    // A large fixture widens the staged-write window so the test can win the
    // race to delete the lock file before the rename lands — mirrors
    // ./history.jest.spec.mjs's seedFillerStore idiom.
    const bulkEntries = Array.from({ length: 5_000 }, (_, index) =>
      entryFixture({ agentClass: `filler-${index}`, priority: 'low', ageOffsetMs: 1_000, overrides: { padding: 'x'.repeat(80) } }),
    );
    writeQueueFile([...bulkEntries, entryFixture({ agentClass: 'target', priority: 'low', ageOffsetMs })]);

    const beforeRaw = readFileSync(queueFilePath, 'utf8');
    const lockPath = `${queueFilePath}.lock`;

    const escalatePromise = escalateAgedQueueEntry(queueFilePath, {
      now: NOW,
      boundMs,
      capacitySamples: freeSampleWindow(NOW - ageOffsetMs, NOW),
      lockStalenessMs: 60_000,
    });

    await expect(waitForFile(lockPath)).resolves.toBe(true);
    await fsPromises.unlink(lockPath);

    await expect(escalatePromise).rejects.toMatchObject({
      name: 'LockLostError',
      code: 'COORDINATION_LOCK_LOST',
    });

    const afterRaw = readFileSync(queueFilePath, 'utf8');
    expect(afterRaw).toBe(beforeRaw);
  }, 15_000);

  it('leaves no orphaned <queueFilePath>.tmp-* sibling behind when the write-side check refuses the write', async () => {
    const { escalateAgedQueueEntry } = await import('./queue-aging-escalation.mjs');
    const boundMs = 60_000;
    const ageOffsetMs = boundMs + 20_000;

    const bulkEntries = Array.from({ length: 5_000 }, (_, index) =>
      entryFixture({ agentClass: `filler-${index}`, priority: 'low', ageOffsetMs: 1_000, overrides: { padding: 'x'.repeat(80) } }),
    );
    writeQueueFile([...bulkEntries, entryFixture({ agentClass: 'target', priority: 'low', ageOffsetMs })]);

    const lockPath = `${queueFilePath}.lock`;
    const escalatePromise = escalateAgedQueueEntry(queueFilePath, {
      now: NOW,
      boundMs,
      capacitySamples: freeSampleWindow(NOW - ageOffsetMs, NOW),
      lockStalenessMs: 60_000,
    });

    await expect(waitForFile(lockPath)).resolves.toBe(true);
    await fsPromises.unlink(lockPath);

    await expect(escalatePromise).rejects.toMatchObject({ code: 'COORDINATION_LOCK_LOST' });

    const siblings = (await fsPromises.readdir(workDir)).filter((name) => name.includes('.tmp-'));
    expect(siblings).toEqual([]);
  }, 15_000);
});

// ---------------------------------------------------------------------------
// 8. Edge case: concurrent mutation of the same queue file goes through
// coordination-file.mjs's lock — two concurrent beats never double-escalate
// or corrupt each other's writes.
// ---------------------------------------------------------------------------

describe('edge case: concurrent escalateAgedQueueEntry calls over the same file never double-escalate or corrupt the write', () => {
  it('two concurrent beats over a queue of 10 equally-eligible entries escalate each entry exactly once between them, with no entry lost or duplicated', async () => {
    const { escalateAgedQueueEntry } = await import('./queue-aging-escalation.mjs');
    const boundMs = 60_000;
    const ageOffsetMs = boundMs + 20_000;
    const entries = Array.from({ length: 10 }, (_, index) =>
      entryFixture({ agentClass: `concurrent-${index}`, priority: 'low', ageOffsetMs }),
    );
    writeQueueFile(entries);

    const options = {
      now: NOW,
      boundMs,
      capacitySamples: freeSampleWindow(NOW - ageOffsetMs, NOW),
    };

    const [first, second] = await Promise.all([
      escalateAgedQueueEntry(queueFilePath, options),
      escalateAgedQueueEntry(queueFilePath, options),
    ]);

    const escalatedClasses = [...first.escalated, ...second.escalated].map((entry) => entry.agentClass).sort();
    expect(escalatedClasses).toEqual(entries.map((entry) => entry.agentClass).sort());
    expect(new Set(escalatedClasses).size).toBe(10);

    const persisted = readQueueFile();
    expect(persisted).toHaveLength(10);
    for (const entry of persisted) {
      expect(entry.priority).toBe('normal');
      expect(entry.escalatedAt).toBe(NOW);
    }

    // Exactly one alert per entry across both racing calls, never two.
    expect(warnSpy).toHaveBeenCalledTimes(10);
  });
});

// ---------------------------------------------------------------------------
// 9. Documented (Convergence Analysis edge case #7): the compound condition
// is evaluated from a single consistent `referenceNow` read per call, not
// re-read per entry — narrows, does not fully close, the non-atomicity
// against a REAL concurrently-changing capacity signal (see this file's own
// header comment and queue-aging-escalation.mjs's own doc comment for the
// full rationale).
// ---------------------------------------------------------------------------

describe('edge case #7: age is computed from a single consistent referenceNow snapshot per call, not re-read per entry', () => {
  it('when options.now is omitted, Date.now() is read at most a small constant number of times for a 20-entry queue — never once per entry', async () => {
    const { escalateAgedQueueEntry, DEFAULT_ESCALATION_BOUND_MS } = await import('./queue-aging-escalation.mjs');
    const ageOffsetMs = DEFAULT_ESCALATION_BOUND_MS + 10_000;
    const entries = Array.from({ length: 20 }, (_, index) =>
      entryFixture({ agentClass: `snapshot-${index}`, priority: 'low', ageOffsetMs }),
    );
    writeQueueFile(entries);

    const dateNowSpy = jest.spyOn(Date, 'now').mockReturnValue(NOW);
    try {
      await escalateAgedQueueEntry(queueFilePath, {
        capacitySamples: freeSampleWindow(NOW - ageOffsetMs, NOW),
      });
    } finally {
      dateNowSpy.mockRestore();
    }

    // A small constant ceiling (well under 20) proves `referenceNow` is
    // snapshotted once per call rather than re-read once per entry — the
    // exact ceiling is deliberately loose (this module's own lock-timing
    // internals may legitimately call Date.now() a handful of times) so this
    // test pins the CONTRACT (single-snapshot-per-call), not an
    // implementation line count.
    expect(dateNowSpy.mock.calls.length).toBeLessThan(10);
  });
});
