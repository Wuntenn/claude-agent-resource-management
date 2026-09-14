// Phase 5 — report-consumable detection logging (watchdog trips,
// aging escalations, deadlock recycles).
//
// RED unit tests for the not-yet-implemented
// scripts/agent-resource-management/lib/liveness-log.mjs module.
//
// ---------------------------------------------------------------------------
// DESIGN DECISION — module choice (documented per the task brief)
// ---------------------------------------------------------------------------
//
// `lib/history.mjs` is NOT extended for this concern. Read in full before
// this file was written: `history.mjs` is scoped, by its own header and
// every doc comment in it, to a single purpose — a per-OPERATION-TYPE
// rolling store of resource-cost OBSERVATIONS (peak memory/swap/disk-delta),
// keyed by `operationType`, whose whole point is bounded-memory statistical
// folding (`foldIntoSummary`/`reservoirInsert`/p90 estimation) once the raw
// log exceeds `retentionCap`. A detection EVENT (a watchdog trip, an aging
// escalation, a deadlock recycle) is not a resource observation and has no
// `operationType`/`peakMemoryMb` shape to fold — coercing detection events
// into `history.mjs`'s `{ raw: [...], summary: {...} }` shape would either
// (a) silently degrade `readHistory`'s exact `count`/mean/max/p90 contract
// with events that were never meant to be statistically folded, or (b)
// require a parallel non-folding code path bolted onto a module whose
// entire design is built around folding. Both are worse than a new module.
// A future report-consumer (Phase 6+) wanting cost stats reads `history.mjs`
// and wanting detection events reads `liveness-log.mjs` — two distinct
// concerns, two distinct files, matching this codebase's one-file-one-
// concern convention (`queue.mjs` vs `queue-aging-escalation.mjs` vs
// `deadlock-tripwire.mjs` are already three separate files for three
// related-but-distinct queue concerns, not one).
//
// ---------------------------------------------------------------------------
// DESIGN DECISION — log entry schema shape
// ---------------------------------------------------------------------------
//
// One JSON object per line (NDJSON), not a single top-level JSON array.
// Convergence Analysis edge case #5 is decisive here: Phase 6 (the beat
// wiring) is not built yet, and there is no dashboard — this log is
// currently the ONLY delivery mechanism for escalations/trips, and calling
// something "alerted" when it only means "written to a file nobody is
// polling yet" would overclaim a guarantee the log alone cannot provide.
// What the log CAN honestly promise is that a human or a future poller can
// find an entry without parsing prose — i.e. it must be genuinely
// greppable. A single growing JSON array is not: `grep detectionType` over
// a minified one-line JSON array matches the whole file, not one event. One
// self-contained JSON object per line, `grep`/`jq -c` friendly out of the
// box, is the queryable shape the edge case calls for. Concurrent-write
// safety is NOT achieved by a bare `fs.appendFile` (two concurrent writers
// can interleave their `write()` syscalls line-internally and corrupt a
// line) — every write instead reuses `coordination-file.mjs`'s
// `acquireLock`/`assertStillHeld`/`releaseLock` fencing-lock primitives
// around a read-whole-file → append-in-memory → atomic-tmp-then-rename
// cycle, exactly mirroring `history.mjs`'s `recordObservation`/
// `writeStoreAtomic` house pattern (JSON-array store there, NDJSON-text
// store here — same lock discipline, different serialisation).
//
// Every entry shares this stable, versioned envelope:
//
//   {
//     schemaVersion: 1,
//     detectionType: 'watchdog-trip' | 'aging-escalation' | 'deadlock-recycle',
//     timestamp: number,       // ms epoch; caller-supplied `options.now` if
//                               // given, else a single Date.now() snapshot
//     pid?: string | number,   // watchdog-trip / deadlock-recycle subject
//                               // (deadlock-recycle substitutes orchestratorId
//                               // for pid per eviction-targeting.mjs's own
//                               // documented pid-substitution convention —
//                               // dibs entries carry no OS pid)
//     orchestratorId?: string, // present on all three detection types
//     queueItemId?: string,    // aging-escalation subject (the queue entry's
//                               // agentClass — the closest thing queue.mjs
//                               // entries have to a stable identifier; there
//                               // is no separate opaque queue-item id field
//                               // in queue.mjs today)
//     outcome: string,         // free-form per detection type, e.g.
//                               // 'trip' | 'escalated' | 'recycled' |
//                               // 'alert-only' | 'skip' — NEVER the literal
//                               // 'alerted' or 'delivered', which this log
//                               // cannot honestly claim (see above)
//     detail?: object,         // additive, detection-type-specific extra
//                               // fields (e.g. { reason }, { priority })
//   }
//
// `schemaVersion` is a plain top-level field on every entry (not a
// file-level header line) precisely so a future consumer can add new fields
// to new entries without needing a file-format migration, and can filter
// out/upgrade-handle old-shape entries by reading `schemaVersion` per line —
// "stable/versioned enough that a future consumer doesn't break on an
// unrelated field addition later," per the Build Plan.
//
// Non-throwing degraded writes mirror the house "fail-open" convention this
// codebase already applies elsewhere: `history.mjs`'s own header describes
// `cli.mjs`'s "existing fail-open catch around [recordObservation] logs the
// rejection and moves on, and nothing retries" (`history.mjs` lines ~55,
// ~293). That existing convention relies on an EXTERNAL caller's catch
// block. This module has no such external caller yet — Phase 6 (the beat
// wiring) does not exist — so `liveness-log.mjs` cannot rely on someone
// else's try/catch to keep a log write failure from crashing the calling
// beat; it must fail open INTERNALLY, in its own implementation, catching
// every error (lock acquisition, read, write, rename) and resolving
// `{ written: false, error }` rather than rejecting. This is the same
// disk-full/permission-error posture the Build Plan calls for, applied one
// layer lower because this module is, for now, its own outermost caller.
//
// `lib/liveness-log.mjs` does NOT exist yet as of this commit — every test
// below is expected to fail at import time (`Cannot find module
// './liveness-log.mjs'`). Do not add an implementation to make this pass —
// the builder implements it in the next phase to turn this green.
//
// ---------------------------------------------------------------------------
// Contract this suite specifies for the implementer (none of this exists
// today — this file IS the spec):
//
//   LIVENESS_LOG_SCHEMA_VERSION -> number (1)
//
//   logWatchdogTrip(logFilePath, { pid, orchestratorId, leaseId?, outcome, reason? }, options?)
//     -> Promise<{ written: boolean, entry?: object, error?: Error }>
//     Appends one `detectionType: 'watchdog-trip'` entry.
//
//   logAgingEscalation(logFilePath, { agentClass, orchestratorId, priority?, outcome }, options?)
//     -> Promise<{ written: boolean, entry?: object, error?: Error }>
//     Appends one `detectionType: 'aging-escalation'` entry. `agentClass` is
//     threaded into `queueItemId`.
//
//   logDeadlockRecycle(logFilePath, { pid, orchestratorId, outcome, reason? }, options?)
//     -> Promise<{ written: boolean, entry?: object, error?: Error }>
//     Appends one `detectionType: 'deadlock-recycle'` entry.
//
//   readLivenessLog(logFilePath) -> Promise<Array<object>>
//     Reads the whole NDJSON file back as an array of parsed entries,
//     tolerating a not-yet-created file (`[]`, never throws).
//
//   `options`: `{ now?: number, lockStalenessMs?: number, lockMaxAttempts?:
//   number, lockRetryDelayMs?: number }`. `now` defaults to a single
//   `Date.now()` snapshot when omitted.
//
//   Every write is best-effort: a lock-acquisition, read, or write/rename
//   failure resolves `{ written: false, error }` — it never rejects/throws
//   out of any of the three `log*` functions.
// ---------------------------------------------------------------------------

import { mkdtempSync, rmSync, writeFileSync, readFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { jest } from '@jest/globals';

let workDir;
let logFilePath;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'liveness-log-lib-'));
  logFilePath = join(workDir, 'liveness-log.ndjson');
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
});

const NOW = 1_700_000_000_000;

function readRawLines() {
  if (!existsSync(logFilePath)) return [];
  const raw = readFileSync(logFilePath, 'utf8');
  return raw.split('\n').filter((line) => line.trim().length > 0);
}

// ---------------------------------------------------------------------------
// 0. Pinned schema version constant
// ---------------------------------------------------------------------------

describe('LIVENESS_LOG_SCHEMA_VERSION', () => {
  it('is exported as 1', async () => {
    const { LIVENESS_LOG_SCHEMA_VERSION } = await import('./liveness-log.mjs');
    expect(LIVENESS_LOG_SCHEMA_VERSION).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// 1. logs one entry per watchdog trip
// ---------------------------------------------------------------------------

describe('scenario: logs one entry per watchdog trip', () => {
  it('appends exactly one detectionType: watchdog-trip entry, carrying pid/orchestratorId/outcome', async () => {
    const { logWatchdogTrip, readLivenessLog, LIVENESS_LOG_SCHEMA_VERSION } = await import('./liveness-log.mjs');

    const result = await logWatchdogTrip(
      logFilePath,
      { pid: 4242, orchestratorId: 'orc-watchdog-1', leaseId: 'lease-abc', outcome: 'recycled', reason: 'graceful-stop-failed' },
      { now: NOW },
    );

    expect(result.written).toBe(true);

    const entries = await readLivenessLog(logFilePath);
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({
      schemaVersion: LIVENESS_LOG_SCHEMA_VERSION,
      detectionType: 'watchdog-trip',
      timestamp: NOW,
      pid: 4242,
      orchestratorId: 'orc-watchdog-1',
      outcome: 'recycled',
    });
  });

  it('a second, independent watchdog trip appends a second entry, not a mutation of the first', async () => {
    const { logWatchdogTrip, readLivenessLog } = await import('./liveness-log.mjs');

    await logWatchdogTrip(logFilePath, { pid: 1, orchestratorId: 'orc-a', outcome: 'recycled' }, { now: NOW });
    await logWatchdogTrip(logFilePath, { pid: 2, orchestratorId: 'orc-b', outcome: 'alert-only' }, { now: NOW + 1000 });

    const entries = await readLivenessLog(logFilePath);
    expect(entries).toHaveLength(2);
    expect(entries.map((entry) => entry.pid)).toEqual([1, 2]);
  });
});

// ---------------------------------------------------------------------------
// 2. logs one entry per aging escalation
// ---------------------------------------------------------------------------

describe('scenario: logs one entry per aging escalation', () => {
  it('appends exactly one detectionType: aging-escalation entry, carrying queueItemId/orchestratorId/outcome', async () => {
    const { logAgingEscalation, readLivenessLog, LIVENESS_LOG_SCHEMA_VERSION } = await import('./liveness-log.mjs');

    const result = await logAgingEscalation(
      logFilePath,
      { agentClass: 'typescript-implementer', orchestratorId: 'orc-1', priority: 'high', outcome: 'escalated' },
      { now: NOW },
    );

    expect(result.written).toBe(true);

    const entries = await readLivenessLog(logFilePath);
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({
      schemaVersion: LIVENESS_LOG_SCHEMA_VERSION,
      detectionType: 'aging-escalation',
      timestamp: NOW,
      queueItemId: 'typescript-implementer',
      orchestratorId: 'orc-1',
      outcome: 'escalated',
    });
  });
});

// ---------------------------------------------------------------------------
// 3. logs one entry per deadlock recycle
// ---------------------------------------------------------------------------

describe('scenario: logs one entry per deadlock recycle', () => {
  it('appends exactly one detectionType: deadlock-recycle entry, substituting orchestratorId for pid per eviction-targeting.mjs convention', async () => {
    const { logDeadlockRecycle, readLivenessLog, LIVENESS_LOG_SCHEMA_VERSION } = await import('./liveness-log.mjs');

    const result = await logDeadlockRecycle(
      logFilePath,
      { pid: 'orc-stalled-1', orchestratorId: 'orc-stalled-1', outcome: 'recycled' },
      { now: NOW },
    );

    expect(result.written).toBe(true);

    const entries = await readLivenessLog(logFilePath);
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({
      schemaVersion: LIVENESS_LOG_SCHEMA_VERSION,
      detectionType: 'deadlock-recycle',
      timestamp: NOW,
      pid: 'orc-stalled-1',
      orchestratorId: 'orc-stalled-1',
      outcome: 'recycled',
    });
  });

  it('a skip outcome (no matching claimLedger record, per resolveEvictionTargets) is still logged, never dropped', async () => {
    const { logDeadlockRecycle, readLivenessLog } = await import('./liveness-log.mjs');

    await logDeadlockRecycle(
      logFilePath,
      { orchestratorId: 'orc-ghost', outcome: 'skip', reason: 'no matching claimLedger record for deadlock-trip orchestratorId' },
      { now: NOW },
    );

    const entries = await readLivenessLog(logFilePath);
    expect(entries).toHaveLength(1);
    expect(entries[0].outcome).toBe('skip');
    expect(entries[0].detail).toMatchObject({ reason: 'no matching claimLedger record for deadlock-trip orchestratorId' });
  });
});

// ---------------------------------------------------------------------------
// 4. log write failure does not throw
// ---------------------------------------------------------------------------

describe('scenario: log write failure does not throw', () => {
  it('resolves { written: false, error } instead of rejecting when the log path cannot be written (parent path is a file, not a directory)', async () => {
    const { logWatchdogTrip } = await import('./liveness-log.mjs');

    // `blocker` is a regular FILE. Using it as the PARENT directory segment
    // of the log path makes every fs op underneath it fail with ENOTDIR —
    // the same "unwritable parent" shape this skill's other modules
    // document as surfacing naturally through the underlying fs call,
    // mirroring queue-aging-escalation.mjs's `ensureQueueDir` comment.
    const blockerPath = join(workDir, 'blocker');
    writeFileSync(blockerPath, 'not a directory', 'utf8');
    const unwritableLogPath = join(blockerPath, 'nested', 'liveness-log.ndjson');

    await expect(
      logWatchdogTrip(unwritableLogPath, { pid: 1, orchestratorId: 'orc-1', outcome: 'recycled' }, { now: NOW }),
    ).resolves.toMatchObject({ written: false });
  });

  it('does not throw for aging-escalation writes either, under the same unwritable-parent condition', async () => {
    const { logAgingEscalation } = await import('./liveness-log.mjs');

    const blockerPath = join(workDir, 'blocker2');
    writeFileSync(blockerPath, 'not a directory', 'utf8');
    const unwritableLogPath = join(blockerPath, 'nested', 'liveness-log.ndjson');

    await expect(
      logAgingEscalation(
        unwritableLogPath,
        { agentClass: 'x', orchestratorId: 'orc-1', outcome: 'escalated' },
        { now: NOW },
      ),
    ).resolves.toMatchObject({ written: false });
  });

  it('does not throw for deadlock-recycle writes either, under the same unwritable-parent condition', async () => {
    const { logDeadlockRecycle } = await import('./liveness-log.mjs');

    const blockerPath = join(workDir, 'blocker3');
    writeFileSync(blockerPath, 'not a directory', 'utf8');
    const unwritableLogPath = join(blockerPath, 'nested', 'liveness-log.ndjson');

    await expect(
      logDeadlockRecycle(unwritableLogPath, { orchestratorId: 'orc-1', outcome: 'recycled' }, { now: NOW }),
    ).resolves.toMatchObject({ written: false });
  });
});

// ---------------------------------------------------------------------------
// 5. concurrent writes from same beat do not corrupt log
// ---------------------------------------------------------------------------

describe('scenario: concurrent writes from same beat do not corrupt log', () => {
  it('a per-agent watchdog check and a deadlock-tripwire check landing in the same beat both persist, and every line parses as valid JSON', async () => {
    const { logWatchdogTrip, logDeadlockRecycle, readLivenessLog } = await import('./liveness-log.mjs');

    const concurrentWrites = [
      ...Array.from({ length: 5 }, (_, index) =>
        logWatchdogTrip(logFilePath, { pid: 1000 + index, orchestratorId: `orc-w-${index}`, outcome: 'recycled' }, { now: NOW }),
      ),
      ...Array.from({ length: 5 }, (_, index) =>
        logDeadlockRecycle(logFilePath, { orchestratorId: `orc-d-${index}`, outcome: 'recycled' }, { now: NOW }),
      ),
    ];

    const results = await Promise.all(concurrentWrites);
    expect(results.every((result) => result.written === true)).toBe(true);

    // Corruption check at the RAW file level, not just via the parsing
    // helper: every non-empty line must independently `JSON.parse` — a torn
    // interleaved write would produce at least one line that doesn't.
    const rawLines = readRawLines();
    expect(rawLines).toHaveLength(10);
    for (const line of rawLines) {
      expect(() => JSON.parse(line)).not.toThrow();
    }

    const entries = await readLivenessLog(logFilePath);
    expect(entries).toHaveLength(10);
  });
});

// ---------------------------------------------------------------------------
// 6. entries from Phases 1/2/3 share a consistent schema
// ---------------------------------------------------------------------------

describe('scenario: entries from Phases 1/2/3 share a consistent schema', () => {
  it('every detection type produces an entry with the same top-level envelope fields (schemaVersion, detectionType, timestamp, orchestratorId, outcome)', async () => {
    const { logWatchdogTrip, logAgingEscalation, logDeadlockRecycle, readLivenessLog, LIVENESS_LOG_SCHEMA_VERSION } =
      await import('./liveness-log.mjs');

    await logWatchdogTrip(logFilePath, { pid: 1, orchestratorId: 'orc-1', outcome: 'recycled' }, { now: NOW });
    await logAgingEscalation(
      logFilePath,
      { agentClass: 'typescript-implementer', orchestratorId: 'orc-2', outcome: 'escalated' },
      { now: NOW + 1 },
    );
    await logDeadlockRecycle(logFilePath, { orchestratorId: 'orc-3', outcome: 'recycled' }, { now: NOW + 2 });

    const entries = await readLivenessLog(logFilePath);
    expect(entries).toHaveLength(3);

    const detectionTypes = entries.map((entry) => entry.detectionType);
    expect(detectionTypes).toEqual(['watchdog-trip', 'aging-escalation', 'deadlock-recycle']);

    for (const entry of entries) {
      expect(entry.schemaVersion).toBe(LIVENESS_LOG_SCHEMA_VERSION);
      expect(typeof entry.timestamp).toBe('number');
      expect(typeof entry.orchestratorId).toBe('string');
      expect(typeof entry.outcome).toBe('string');
      // Never overclaims delivery (Convergence #5) — this log is currently
      // the only detection sink, so no entry may claim to have been
      // "alerted"/"delivered" beyond having been logged.
      expect(entry.outcome).not.toBe('alerted');
      expect(entry.outcome).not.toBe('delivered');
    }
  });

  it('is genuinely greppable: a plain text search for a detectionType value matches only the relevant line(s)', async () => {
    const { logWatchdogTrip, logAgingEscalation } = await import('./liveness-log.mjs');

    await logWatchdogTrip(logFilePath, { pid: 1, orchestratorId: 'orc-1', outcome: 'recycled' }, { now: NOW });
    await logAgingEscalation(
      logFilePath,
      { agentClass: 'typescript-implementer', orchestratorId: 'orc-2', outcome: 'escalated' },
      { now: NOW + 1 },
    );

    const rawLines = readRawLines();
    expect(rawLines).toHaveLength(2);
    const matchingLines = rawLines.filter((line) => line.includes('"detectionType":"watchdog-trip"'));
    expect(matchingLines).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// 7. readLivenessLog tolerates a not-yet-created file
// ---------------------------------------------------------------------------

describe('readLivenessLog', () => {
  it('returns [] for a log file that has never been written, rather than throwing', async () => {
    const { readLivenessLog } = await import('./liveness-log.mjs');
    await expect(readLivenessLog(logFilePath)).resolves.toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// 8. readLivenessLogTail — genuine age-based bounded read (Phase 2
//    pre-PR review, Medium). This is the quantitative proof the black-box
//    `resolve-recently-evicted-pids-bounded-read.jest.spec.mjs` file cannot
//    provide itself (it spawns `cli.mjs` as a real child process and has no
//    seam to count how many lines that process parsed) — this file imports
//    `readLivenessLogTail` directly, so it can spy on `JSON.parse` and count
//    invocations exactly.
// ---------------------------------------------------------------------------

describe('readLivenessLogTail', () => {
  // Mirrors the module's own (unexported) `AGE_STOP_LOOKBACK_MARGIN` —
  // Phase 2 post-review (High) bounded lookback margin defending against
  // concurrent-orchestrator write reordering. Kept in sync by hand rather
  // than exported: it is an internal tuning constant, not part of the
  // module's public contract.
  const AGE_STOP_LOOKBACK_MARGIN = 25;


  it('returns [] for a log file that has never been written, rather than throwing', async () => {
    const { readLivenessLogTail } = await import('./liveness-log.mjs');
    await expect(readLivenessLogTail(logFilePath, { now: NOW, cooldownMs: 5 * 60 * 1000 })).resolves.toEqual([]);
  });

  it(
    'stops the backward walk at the first well-formed entry older than now - cooldownMs, inspecting only a small, bounded number of lines regardless of how much older well-formed history precedes it',
    async () => {
      const { readLivenessLogTail, logWatchdogTrip } = await import('./liveness-log.mjs');

      const cooldownMs = 5 * 60 * 1000;
      const nowMs = NOW + 100_000_000;
      const oldEntryCount = 1500;
      const oldStart = nowMs - 50_000_000;

      // A large number of OLDER, WELL-FORMED (not corrupt), realistically
      // non-decreasing-timestamped entries — all strictly older than the
      // cooldown cutoff — appended BEFORE the in-window entries below, so
      // they sit earlier in file order (further from the tail).
      for (let index = 0; index < oldEntryCount; index += 1) {
        // eslint-disable-next-line no-await-in-loop -- see comment above.
        await logWatchdogTrip(
          logFilePath,
          { pid: 50_000 + index, orchestratorId: `orc-old-${index}`, outcome: 'permission-denied' },
          { now: oldStart + index },
        );
      }

      // A handful of genuinely in-cooldown-window entries, appended last
      // (newest, at the tail).
      const recentEntryCount = 3;
      for (let index = 0; index < recentEntryCount; index += 1) {
        // eslint-disable-next-line no-await-in-loop -- must fold in chronological order.
        await logWatchdogTrip(
          logFilePath,
          { pid: 60_000 + index, orchestratorId: `orc-recent-${index}`, outcome: 'permission-denied' },
          { now: nowMs - 60_000 + index },
        );
      }

      const parseSpy = jest.spyOn(JSON, 'parse');
      try {
        const entries = await readLivenessLogTail(logFilePath, { now: nowMs, cooldownMs });

        // Only the genuinely in-window entries come back...
        expect(entries).toHaveLength(recentEntryCount);

        // ...and the walk inspected only those `recentEntryCount` lines plus
        // the first over-age entry that trips the stop plus the bounded
        // `AGE_STOP_LOOKBACK_MARGIN` lookback beyond it — never anything
        // close to `oldEntryCount + recentEntryCount`. This is the
        // quantitative proof that parse cost is bounded by the cooldown
        // window (plus a small fixed margin), not by total log size.
        expect(parseSpy).toHaveBeenCalledTimes(recentEntryCount + 1 + AGE_STOP_LOOKBACK_MARGIN);
      } finally {
        parseSpy.mockRestore();
      }
    },
    30_000,
  );

  it('does not stop on an entry exactly at the cutoff (now - cooldownMs is inclusive, matching resolveRecentlyEvictedPids own boundary semantics)', async () => {
    const { readLivenessLogTail, logWatchdogTrip } = await import('./liveness-log.mjs');

    const cooldownMs = 5 * 60 * 1000;
    const nowMs = NOW + 100_000_000;

    await logWatchdogTrip(
      logFilePath,
      { pid: 70_000, orchestratorId: 'orc-boundary', outcome: 'permission-denied' },
      { now: nowMs - cooldownMs },
    );

    await expect(readLivenessLogTail(logFilePath, { now: nowMs, cooldownMs })).resolves.toHaveLength(1);
  });

  it('still stops at the first unparseable line even when now/cooldownMs are supplied (corruption stop and age stop coexist)', async () => {
    const { readLivenessLogTail, logWatchdogTrip } = await import('./liveness-log.mjs');

    const cooldownMs = 5 * 60 * 1000;
    const nowMs = NOW + 100_000_000;

    writeFileSync(logFilePath, 'this is not valid json\n', 'utf8');
    await logWatchdogTrip(
      logFilePath,
      { pid: 80_000, orchestratorId: 'orc-corrupt', outcome: 'permission-denied' },
      { now: nowMs - 60_000 },
    );

    const entries = await readLivenessLogTail(logFilePath, { now: nowMs, cooldownMs });
    expect(entries).toHaveLength(1);
    expect(entries[0].pid).toBe(80_000);
  });

  it('falls back to corruption-stop-only behaviour (no age stop) when now/cooldownMs are omitted', async () => {
    const { readLivenessLogTail, logWatchdogTrip } = await import('./liveness-log.mjs');

    const nowMs = NOW + 100_000_000;
    // Deliberately older than any realistic cooldown window, appended first.
    await logWatchdogTrip(
      logFilePath,
      { pid: 90_000, orchestratorId: 'orc-ancient', outcome: 'permission-denied' },
      { now: nowMs - 10_000_000 },
    );
    await logWatchdogTrip(
      logFilePath,
      { pid: 90_001, orchestratorId: 'orc-recent', outcome: 'permission-denied' },
      { now: nowMs - 60_000 },
    );

    await expect(readLivenessLogTail(logFilePath)).resolves.toHaveLength(2);
  });

  it(
    'finds a genuinely in-window entry that sits BEHIND (further from the tail than) a later-file-position over-age entry — the concurrent-orchestrator reordering race (Phase 2 post-review, High)',
    async () => {
      const { readLivenessLogTail, logWatchdogTrip } = await import('./liveness-log.mjs');

      const cooldownMs = 5 * 60 * 1000;
      const nowMs = NOW + 100_000_000;

      // Orchestrator C's genuinely in-window entry lands FIRST (earlier file
      // position) — e.g. a fast beat with no eviction delay.
      await logWatchdogTrip(
        logFilePath,
        { pid: 10_000, orchestratorId: 'orc-c', outcome: 'permission-denied' },
        { now: nowMs - 60_000 },
      );

      // Orchestrator A's entry lands SECOND (later file position, i.e.
      // nearer the tail) despite carrying an OLDER timestamp — simulating
      // `handleEvict`'s real grace-period sleep: A captured `now` well
      // before C's beat even started, but only appends after C has already
      // written and moved on. This timestamp is stamped past the cooldown
      // cutoff, so a naive "stop at the first over-age entry" walking
      // backward from the tail would hit A first and never reach C.
      await logWatchdogTrip(
        logFilePath,
        { pid: 10_001, orchestratorId: 'orc-a', outcome: 'permission-denied' },
        { now: nowMs - cooldownMs - 120_000 },
      );

      const entries = await readLivenessLogTail(logFilePath, { now: nowMs, cooldownMs });

      // C's genuinely in-window entry must still be found via the bounded
      // lookback margin, even though it sits behind the over-age entry that
      // trips the age stop.
      expect(entries.map((entry) => entry.pid)).toEqual(expect.arrayContaining([10_000]));
      // A's over-age entry must never be included.
      expect(entries.map((entry) => entry.pid)).not.toEqual(expect.arrayContaining([10_001]));
    },
  );

  it('does not throw when a line is valid JSON but not an entry object (a bare `null` line), and still returns genuinely in-window entries found before that line is reached', async () => {
    const { readLivenessLogTail, logWatchdogTrip } = await import('./liveness-log.mjs');

    const cooldownMs = 5 * 60 * 1000;
    const nowMs = NOW + 100_000_000;

    // A bare `null` line — valid JSON (so the old `JSON.parse`-failure-only
    // corruption stop would never trigger for it), but not an object, so
    // `entry.timestamp` on it would throw a TypeError without the guard.
    writeFileSync(logFilePath, 'null\n', 'utf8');
    // A genuinely in-window, well-formed entry appended AFTER the null line
    // (i.e. nearer the tail) must still come back.
    await logWatchdogTrip(
      logFilePath,
      { pid: 95_000, orchestratorId: 'orc-after-null', outcome: 'permission-denied' },
      { now: nowMs - 60_000 },
    );

    await expect(readLivenessLogTail(logFilePath, { now: nowMs, cooldownMs })).resolves.toEqual([
      expect.objectContaining({ pid: 95_000 }),
    ]);
  });

  it('does not throw when a line is valid JSON but not an entry object (a bare number line), treating it the same as the corruption stop', async () => {
    const { readLivenessLogTail, logWatchdogTrip } = await import('./liveness-log.mjs');

    const cooldownMs = 5 * 60 * 1000;
    const nowMs = NOW + 100_000_000;

    writeFileSync(logFilePath, '42\n', 'utf8');
    await logWatchdogTrip(
      logFilePath,
      { pid: 95_001, orchestratorId: 'orc-after-number', outcome: 'permission-denied' },
      { now: nowMs - 60_000 },
    );

    await expect(readLivenessLogTail(logFilePath, { now: nowMs, cooldownMs })).resolves.toEqual([
      expect.objectContaining({ pid: 95_001 }),
    ]);
  });

  it('returns entries in NEWEST-FIRST order — the exact reverse of readLivenessLog\'s oldest-first order', async () => {
    const { readLivenessLogTail, logWatchdogTrip } = await import('./liveness-log.mjs');

    await logWatchdogTrip(logFilePath, { pid: 1, orchestratorId: 'orc-1', outcome: 'permission-denied' }, { now: NOW });
    await logWatchdogTrip(logFilePath, { pid: 2, orchestratorId: 'orc-2', outcome: 'permission-denied' }, { now: NOW + 1 });
    await logWatchdogTrip(logFilePath, { pid: 3, orchestratorId: 'orc-3', outcome: 'permission-denied' }, { now: NOW + 2 });

    const entries = await readLivenessLogTail(logFilePath);
    expect(entries.map((entry) => entry.pid)).toEqual([3, 2, 1]);
  });

  it(
    'the lookback margin is NOT infinite: many genuinely-old entries beyond the margin still stop the walk, keeping parse cost bounded rather than degrading back to a whole-file scan',
    async () => {
      const { readLivenessLogTail, logWatchdogTrip } = await import('./liveness-log.mjs');

      const cooldownMs = 5 * 60 * 1000;
      const nowMs = NOW + 100_000_000;
      const oldEntryCount = 500;
      const oldStart = nowMs - 50_000_000;

      // Many well-formed, over-age entries — far more than
      // `AGE_STOP_LOOKBACK_MARGIN` — appended first (earliest file
      // positions).
      for (let index = 0; index < oldEntryCount; index += 1) {
        // eslint-disable-next-line no-await-in-loop -- must fold in chronological order.
        await logWatchdogTrip(
          logFilePath,
          { pid: 20_000 + index, orchestratorId: `orc-old-${index}`, outcome: 'permission-denied' },
          { now: oldStart + index },
        );
      }

      // One genuinely in-window entry at the tail.
      await logWatchdogTrip(
        logFilePath,
        { pid: 30_000, orchestratorId: 'orc-recent', outcome: 'permission-denied' },
        { now: nowMs - 60_000 },
      );

      const parseSpy = jest.spyOn(JSON, 'parse');
      try {
        const entries = await readLivenessLogTail(logFilePath, { now: nowMs, cooldownMs });

        expect(entries).toHaveLength(1);
        expect(entries[0].pid).toBe(30_000);

        // The walk must stop within the bounded margin, never anywhere
        // close to `oldEntryCount` (a whole-file scan). One in-window
        // entry, plus the entry that first trips the age stop, plus at most
        // `AGE_STOP_LOOKBACK_MARGIN` more.
        expect(parseSpy).toHaveBeenCalledTimes(1 + 1 + AGE_STOP_LOOKBACK_MARGIN);
      } finally {
        parseSpy.mockRestore();
      }
    },
    30_000,
  );
});
