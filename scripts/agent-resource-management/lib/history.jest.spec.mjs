// RED unit tests for the not-yet-implemented
// scripts/agent-resource-management/lib/history.mjs module (Phase 1 —
// "Operation-type history store").
//
// Per the Build Plan's Phase 1 section, this module owns:
//   - recordObservation(historyFilePath, observation, options?) — appends a
//     per-operation-type observation to a bounded raw log, folding overflow
//     into a rolling summary once a configurable retention cap is exceeded.
//   - readHistory(historyFilePath, operationType) — returns an explicit
//     `{ raw: [...], summary: {...} | null }` shape for a given operation
//     type, never `undefined`, even cold-start.
//
// Designed function signatures (documented here for the implementer, since
// lib/history.mjs does not exist yet):
//
//   recordObservation(
//     historyFilePath: string,
//     observation: {
//       operationType: string,
//       orchestratorId: string,
//       startedAt: number,
//       endedAt: number,
//       peakMemoryMb: number,
//       peakSwapMb?: number,
//       freeDiskDeltaGb?: number,
//       crashed?: boolean,
//     },
//     options?: { retentionCap?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number },
//   ): Promise<void>
//
//   readHistory(
//     historyFilePath: string,
//     operationType: string,
//   ): Promise<{
//     raw: Array<{ operationType, orchestratorId, startedAt, endedAt, peakMemoryMb, peakSwapMb, freeDiskDeltaGb, crashed }>,
//     summary: { count: number, meanPeakMemoryMb: number, p90PeakMemoryMb: number, maxPeakMemoryMb: number } | null,
//   }>
//
// Retention cap is injectable via `options.retentionCap` (this suite fixes
// it at RETENTION_CAP = 5 throughout, passed explicitly on every
// `recordObservation` call) — this is a documented, explicit test choice,
// not a hidden magic number the implementer has to guess at. Whatever
// default `history.mjs` ships internally, passing `options.retentionCap`
// must override it, per this suite's assertions.
//
// Concurrent-write safety: per SKILL.md's "Configurable thresholds" section
// and lib/coordination-file.mjs's extensive lock doc comments, this module
// is expected to reuse the EXISTING fencing/lock primitives
// (`acquireLock`/`releaseLock`, or an equivalent single-lock-per-file
// critical section) rather than invent a second locking mechanism — this
// suite tests only the PUBLIC contract (two concurrent `recordObservation`
// calls for the same operationType both land), leaving the exact
// lock-reuse mechanism to the implementer.
//
// Do not add a stub implementation to make this pass — that defeats the
// point of the outer/phase red-green loop. The builder implements
// history.mjs to turn this green.

import { mkdtempSync, rmSync, existsSync, readFileSync, promises as fsPromises } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { jest } from '@jest/globals';

import { recordObservation, readHistory } from './history.mjs';
import * as historyModule from './history.mjs';
import { LockLostError } from './coordination-file.mjs';

// Namespace import (as opposed to a named `{ writeStoreAtomic }` import) is
// deliberate: `writeStoreAtomic` is not yet an exported symbol of
// `history.mjs` (its whole point is that it needs to become one, mirroring
// `coordination-file.mjs`'s `writeEntriesAtomic` export-for-tests
// convention). A named import of a not-yet-existing export is a hard ESM
// SyntaxError at module-link time, which would take down every test in this
// file — not the "genuinely red because the feature is missing" failure this
// suite wants. A namespace import never fails to link; `historyModule.
// writeStoreAtomic` is simply `undefined` until the export exists, which the
// tests below assert against directly.

const __dirname = dirname(fileURLToPath(import.meta.url));
const historySource = readFileSync(join(__dirname, 'history.mjs'), 'utf8');

const RETENTION_CAP = 5;

let workDir;
let historyFilePath;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'arm-history-'));
  historyFilePath = join(workDir, 'operation-history.json');
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
});

/**
 * Builds a minimal, valid observation for `operationType`, with every
 * numeric field distinguishable across calls via `seed` so assertions can
 * tell entries apart.
 *
 * @param {string} operationType
 * @param {number} seed
 * @param {object} [overrides]
 */
function makeObservation(operationType, seed, overrides = {}) {
  const startedAt = 1_700_000_000_000 + seed * 1000;
  return {
    operationType,
    orchestratorId: `orchestrator-${seed}`,
    startedAt,
    endedAt: startedAt + 500,
    peakMemoryMb: 100 + seed,
    peakSwapMb: 0,
    freeDiskDeltaGb: 0,
    crashed: false,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Cold start — first observation for a new type
// ---------------------------------------------------------------------------

describe('recordObservation — cold start', () => {
  it('records the first-ever observation for a brand-new operation type', async () => {
    await recordObservation(historyFilePath, makeObservation('test-agent', 1), {
      retentionCap: RETENTION_CAP,
    });

    const history = await readHistory(historyFilePath, 'test-agent');

    expect(history.raw).toHaveLength(1);
    expect(history.raw[0].operationType).toBe('test-agent');
    expect(history.raw[0].peakMemoryMb).toBe(101);
    expect(history.summary).toBeNull();
  });

  it('does not create or pollute history for a different, unrelated operation type', async () => {
    await recordObservation(historyFilePath, makeObservation('test-agent', 1), {
      retentionCap: RETENTION_CAP,
    });

    const unrelated = await readHistory(historyFilePath, 'implementation-agent');

    expect(unrelated.raw).toEqual([]);
    expect(unrelated.summary).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Cold-start read — explicit empty shape, never undefined
// ---------------------------------------------------------------------------

describe('readHistory — cold-start read shape', () => {
  it('returns an explicit empty { raw: [], summary: null } shape for a type with zero observations, not undefined', async () => {
    const history = await readHistory(historyFilePath, 'never-seen-type');

    expect(history).toBeDefined();
    expect(history).not.toBeUndefined();
    expect(history.raw).toEqual([]);
    expect(history.summary).toBeNull();
  });

  it('returns the same explicit empty shape when the history file has not been created yet at all', async () => {
    const history = await readHistory(historyFilePath, 'test-agent');

    expect(history).toEqual({ raw: [], summary: null });
  });
});

// ---------------------------------------------------------------------------
// Retention: raw log grows up to the cap, no folding yet
// ---------------------------------------------------------------------------

describe('recordObservation — retains all raw observations up to the cap', () => {
  it('keeps every observation as raw entries while count <= retentionCap', async () => {
    for (let i = 1; i <= RETENTION_CAP; i += 1) {
      // eslint-disable-next-line no-await-in-loop -- deliberately sequential: proving ordered accumulation, not concurrency (see the dedicated concurrent-writes describe block below).
      await recordObservation(historyFilePath, makeObservation('test-agent', i), {
        retentionCap: RETENTION_CAP,
      });
    }

    const history = await readHistory(historyFilePath, 'test-agent');

    expect(history.raw).toHaveLength(RETENTION_CAP);
    expect(history.summary).toBeNull();
    // Ordering preserved — oldest first, matching declaration order.
    expect(history.raw.map((entry) => entry.peakMemoryMb)).toEqual([101, 102, 103, 104, 105]);
  });
});

// ---------------------------------------------------------------------------
// Fold at cap + 1
// ---------------------------------------------------------------------------

describe('recordObservation — folds the oldest observation into a summary at cap + 1', () => {
  it('drops the oldest raw entry into a rolling summary once the (cap + 1)th observation lands', async () => {
    for (let i = 1; i <= RETENTION_CAP + 1; i += 1) {
      // eslint-disable-next-line no-await-in-loop -- deliberately sequential; see note above.
      await recordObservation(historyFilePath, makeObservation('test-agent', i), {
        retentionCap: RETENTION_CAP,
      });
    }

    const history = await readHistory(historyFilePath, 'test-agent');

    // Raw log stays bounded at the cap — never grows past it.
    expect(history.raw).toHaveLength(RETENTION_CAP);
    // The oldest observation (seed 1, peakMemoryMb 101) is no longer present
    // as a raw entry — it was folded, not silently dropped.
    expect(history.raw.map((entry) => entry.peakMemoryMb)).not.toContain(101);
    expect(history.raw.map((entry) => entry.peakMemoryMb)).toEqual([102, 103, 104, 105, 106]);

    // The folded entry is reflected in the summary, not lost.
    expect(history.summary).not.toBeNull();
    expect(history.summary.count).toBe(1);
    expect(history.summary.meanPeakMemoryMb).toBe(101);
    expect(history.summary.maxPeakMemoryMb).toBe(101);
    expect(history.summary.p90PeakMemoryMb).toBe(101);
  });
});

// ---------------------------------------------------------------------------
// Summary stats stay correct across repeated overflow
// ---------------------------------------------------------------------------

describe('recordObservation — summary stats stay correct across repeated overflow', () => {
  it('keeps a single rolling summary record (not one per fold) and correct count/mean/max as many writes overflow the cap', async () => {
    const totalWrites = RETENTION_CAP + 10; // folds 10 times
    for (let i = 1; i <= totalWrites; i += 1) {
      // eslint-disable-next-line no-await-in-loop -- deliberately sequential; see note above.
      await recordObservation(historyFilePath, makeObservation('test-agent', i), {
        retentionCap: RETENTION_CAP,
      });
    }

    const history = await readHistory(historyFilePath, 'test-agent');

    // Raw log is still exactly the cap size, no unbounded growth.
    expect(history.raw).toHaveLength(RETENTION_CAP);
    // The 10 oldest observations (seeds 1..10, peakMemoryMb 101..110) were
    // folded into ONE rolling summary, not 10 separate summary records.
    expect(history.summary).not.toBeNull();
    expect(history.summary.count).toBe(10);

    const foldedPeaks = Array.from({ length: 10 }, (_, index) => 100 + index + 1); // [101..110]
    const expectedMean = foldedPeaks.reduce((sum, value) => sum + value, 0) / foldedPeaks.length;
    expect(history.summary.meanPeakMemoryMb).toBeCloseTo(expectedMean, 5);
    expect(history.summary.maxPeakMemoryMb).toBe(Math.max(...foldedPeaks));

    // p90 must lie within the observed range and reflect the upper tail —
    // not equal to the mean (a wrong implementation that aliases p90 to mean
    // would still pass a looser assertion, so this pins the two apart).
    expect(history.summary.p90PeakMemoryMb).toBeGreaterThanOrEqual(expectedMean);
    expect(history.summary.p90PeakMemoryMb).toBeLessThanOrEqual(Math.max(...foldedPeaks));
  });

  it('never grows the summary object itself — it stays a single fixed-shape record no matter how many folds have occurred', async () => {
    const totalWrites = RETENTION_CAP * 3; // several fold cycles
    for (let i = 1; i <= totalWrites; i += 1) {
      // eslint-disable-next-line no-await-in-loop -- deliberately sequential; see note above.
      await recordObservation(historyFilePath, makeObservation('test-agent', i), {
        retentionCap: RETENTION_CAP,
      });
    }

    const history = await readHistory(historyFilePath, 'test-agent');

    expect(history.summary).not.toBeNull();
    expect(Object.keys(history.summary).sort()).toEqual(
      ['count', 'meanPeakMemoryMb', 'maxPeakMemoryMb', 'p90PeakMemoryMb'].sort(),
    );
    expect(history.summary.count).toBe(totalWrites - RETENTION_CAP);
  });
});

// ---------------------------------------------------------------------------
// Concurrent writes for the same operation type — no lost update
// ---------------------------------------------------------------------------

describe('recordObservation — concurrent writes for the same type do not lose an update', () => {
  it('reflects both of two near-simultaneous recordObservation calls for the same operationType', async () => {
    await Promise.all([
      recordObservation(historyFilePath, makeObservation('test-agent', 1), { retentionCap: RETENTION_CAP }),
      recordObservation(historyFilePath, makeObservation('test-agent', 2), { retentionCap: RETENTION_CAP }),
    ]);

    const history = await readHistory(historyFilePath, 'test-agent');

    // Both writers' observations must be present — a naive unlocked
    // read-modify-write would silently lose one of them (a torn write races
    // exactly like coordination-file.mjs's declareDibs concurrent-writer
    // test proves for dibs entries).
    expect(history.raw).toHaveLength(2);
    const peaks = history.raw.map((entry) => entry.peakMemoryMb).sort();
    expect(peaks).toEqual([101, 102]);
  });

  it('lands all entries when several orchestrators recordObservation concurrently for the same type (extends beyond the two-writer case)', async () => {
    const writerCount = 5;
    await Promise.all(
      Array.from({ length: writerCount }, (_, index) =>
        recordObservation(historyFilePath, makeObservation('test-agent', index + 1), {
          retentionCap: RETENTION_CAP,
        }),
      ),
    );

    const history = await readHistory(historyFilePath, 'test-agent');

    expect(history.raw).toHaveLength(writerCount);
    const peaks = history.raw.map((entry) => entry.peakMemoryMb).sort((a, b) => a - b);
    expect(peaks).toEqual([101, 102, 103, 104, 105]);
  });
});

// ---------------------------------------------------------------------------
// pre-PR review regression — only CLEAN observations fold into `summary`.
//
// The fold is the one path by which an observation influences the estimate
// for the rest of the file's life (it survives eviction from `raw`), so a
// crashed run's pathological peak folding in silently defeats SKILL.md's
// "crashed: true observations are excluded from the central estimate
// entirely" contract, and a peak-less observation (legal — `--peak-memory-mb`
// is optional on `--record-outcome`) poisons `sum`/`max`/`reservoir` to NaN
// permanently. Neither is visible from within Phase 1 or Phase 3 alone.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Phase 3 — additive optional fields (`agentClass`, `footprintTrajectory`)
// on the observation shape `recordObservation` accepts.
//
// `recordObservation` already spreads whatever observation object it is
// given (`{ ...observation }`) — these tests pin that this remains true (a
// green baseline TODAY, not something Phase 3 needs to newly implement) and
// that back-compat callers (no new fields at all) are entirely unaffected.
// The Build Plan's own scope note: this module's SHAPE is additive-only —
// no existing field, and no existing caller/test written against the old
// shape, may change behaviour.
// ---------------------------------------------------------------------------

describe('recordObservation — Phase 3 additive optional fields (agentClass, footprintTrajectory)', () => {
  it('accepts and persists agentClass and footprintTrajectory when present, round-tripped via readHistory', async () => {
    await recordObservation(
      historyFilePath,
      makeObservation('footprint-agent', 1, {
        agentClass: 'typescript-implementer',
        footprintTrajectory: [180, 205, 240],
      }),
      { retentionCap: RETENTION_CAP },
    );

    const history = await readHistory(historyFilePath, 'footprint-agent');

    expect(history.raw).toHaveLength(1);
    expect(history.raw[0].agentClass).toBe('typescript-implementer');
    expect(history.raw[0].footprintTrajectory).toEqual([180, 205, 240]);
    // Every pre-existing field survives alongside the new ones.
    expect(history.raw[0]).toMatchObject({
      operationType: 'footprint-agent',
      orchestratorId: 'orchestrator-1',
      peakMemoryMb: 101,
    });
  });

  it('still accepts an observation carrying NONE of the new fields — back-compat, unmodified shape', async () => {
    const observation = makeObservation('footprint-agent', 2);
    expect(observation.agentClass).toBeUndefined();
    expect(observation.footprintTrajectory).toBeUndefined();

    await recordObservation(historyFilePath, observation, { retentionCap: RETENTION_CAP });

    const history = await readHistory(historyFilePath, 'footprint-agent');

    expect(history.raw).toHaveLength(1);
    expect(history.raw[0]).not.toHaveProperty('agentClass');
    expect(history.raw[0]).not.toHaveProperty('footprintTrajectory');
    expect(history.raw[0]).toMatchObject({
      operationType: 'footprint-agent',
      orchestratorId: 'orchestrator-2',
      peakMemoryMb: 102,
    });
  });

  it("readHistory's returned shape for an OLD-FORMAT entry (no new fields) is identical to today's documented shape", async () => {
    const oldFormatObservation = makeObservation('legacy-agent', 1);

    await recordObservation(historyFilePath, oldFormatObservation, { retentionCap: RETENTION_CAP });

    const history = await readHistory(historyFilePath, 'legacy-agent');

    expect(history.raw).toHaveLength(1);
    // Exactly the documented old-format field set — no new keys silently
    // introduced by recordObservation/readHistory for an old-format caller
    // that never supplied agentClass/footprintTrajectory in the first place.
    expect(Object.keys(history.raw[0]).sort()).toEqual(
      [
        'operationType',
        'orchestratorId',
        'startedAt',
        'endedAt',
        'peakMemoryMb',
        'peakSwapMb',
        'freeDiskDeltaGb',
        'crashed',
      ].sort(),
    );
    expect(history.summary).toBeNull();
  });
});

describe('recordObservation — summary folding excludes non-evidence observations', () => {
  it('never folds a crashed observation into the rolling summary when it ages past the cap', async () => {
    // First (oldest) observation is a crashed 9000MB outlier; the rest are
    // normal. Overflowing the cap evicts the crashed one first.
    await recordObservation(
      historyFilePath,
      makeObservation('test-agent', 1, { peakMemoryMb: 9000, crashed: true }),
      { retentionCap: 2 },
    );
    for (let seed = 2; seed <= 4; seed += 1) {
      await recordObservation(historyFilePath, makeObservation('test-agent', seed, { peakMemoryMb: 200 }), {
        retentionCap: 2,
      });
    }

    const history = await readHistory(historyFilePath, 'test-agent');

    expect(history.raw).toHaveLength(2);
    expect(history.summary).not.toBeNull();
    expect(history.summary.maxPeakMemoryMb).toBe(200);
    expect(history.summary.p90PeakMemoryMb).toBe(200);
    // Exactly one clean fold (the crashed oldest was evicted without folding).
    expect(history.summary.count).toBe(1);
  });

  it('never folds an observation with no peakMemoryMb, so the accumulator can never go NaN', async () => {
    await recordObservation(
      historyFilePath,
      makeObservation('test-agent', 1, { peakMemoryMb: undefined }),
      { retentionCap: 2 },
    );
    for (let seed = 2; seed <= 4; seed += 1) {
      await recordObservation(historyFilePath, makeObservation('test-agent', seed, { peakMemoryMb: 200 }), {
        retentionCap: 2,
      });
    }

    const history = await readHistory(historyFilePath, 'test-agent');

    expect(history.summary).not.toBeNull();
    expect(Number.isFinite(history.summary.meanPeakMemoryMb)).toBe(true);
    expect(Number.isFinite(history.summary.maxPeakMemoryMb)).toBe(true);
    expect(Number.isFinite(history.summary.p90PeakMemoryMb)).toBe(true);
    // Exactly one clean fold (the peak-less oldest was evicted without folding).
    expect(history.summary.count).toBe(1);
  });

  it('still evicts a skipped observation from raw — the retention cap is a hard bound either way', async () => {
    for (let seed = 1; seed <= 5; seed += 1) {
      await recordObservation(
        historyFilePath,
        makeObservation('test-agent', seed, { peakMemoryMb: 9000, crashed: true }),
        { retentionCap: 2 },
      );
    }

    const history = await readHistory(historyFilePath, 'test-agent');

    expect(history.raw).toHaveLength(2);
    // Nothing clean has ever folded, so there is no summary to speak of.
    expect(history.summary).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Post-reclaim write: the silent-drop failure already closed for
// coordination-file.mjs's `writeEntriesAtomic`, re-opened here for
// history.mjs's `recordObservation`/`writeStoreAtomic`.
//
// `recordObservation` acquires its `<historyFilePath>.lock` sibling via the
// SAME `acquireLock`/`releaseLock` primitives `writeEntriesAtomic` uses, but
// — unlike `writeEntriesAtomic` — never re-proves ownership immediately
// before its final `rename()` (`writeStoreAtomic` just serialises and
// renames, no `assertStillHeld()` call in between). A caller whose
// read-modify-write critical section outlives `lockStalenessMs` can
// therefore have its lock legitimately reclaimed by a contender
// (`reclaimStaleLock` cannot distinguish "stalled but alive" from
// "SIGKILLed"), and then still commit its stale `rename()` over whatever the
// reclaiming holder already wrote — resolving successfully while its
// observation silently vanishes from disk.
//
// RED BY CONSTRUCTION until `recordObservation`/`writeStoreAtomic` thread a
// lock context through to an `assertStillHeld()` call before the final
// `rename()`, mirroring `withLock`/`writeEntriesAtomic`'s shape exactly.
//
// The reproduction widens the critical section with real work, exactly as
// the coordination-file test does: the history store is pre-seeded
// with a bulk of filler raw observations under a reserved operationType, so
// each holder's read-parse-serialise-write cycle genuinely takes long enough
// that a contender's `looksStale` verdict fires against a live holder — the
// same production precondition this ticket names (a stall on a
// swap-thrashing host), expressed as work rather than a mock.
//
// The assertions are written as an INVARIANT meaningful in both states, so
// the builder turns them green without editing them:
//
//     every call either lands its observation on disk, or rejects with
//     LockLostError
//
// Today calls can resolve with their observation nowhere on disk (invariant
// violated) whenever a contender's write lands between this caller's read
// and its stale `rename()`. After the fix, a holder that lost its lock
// rejects instead of silently dropping the write, so absence is always
// accounted for.
// ---------------------------------------------------------------------------

describe('post-reclaim write is refused rather than silently dropped', () => {
  // Enough filler raw observations to make a single readStore/writeStoreAtomic
  // cycle take an order of magnitude longer than
  // `slowCriticalSectionStalenessMs`, which is what lets a contender form a
  // genuine (if premature) staleness verdict against a live holder.
  const FILLER_ENTRY_COUNT = 20_000;
  const slowCriticalSectionStalenessMs = 5;
  const FILLER_TYPE = '__filler-type__';
  const RACE_TYPE = 'race-operation-type';

  async function seedBulkFillerStore(targetPath) {
    const raw = Array.from({ length: FILLER_ENTRY_COUNT }, (_, index) => ({
      operationType: FILLER_TYPE,
      orchestratorId: `filler-${index}`,
      startedAt: 0,
      endedAt: 0,
      peakMemoryMb: 100,
      padding: 'x'.repeat(40),
    }));
    await fsPromises.writeFile(targetPath, JSON.stringify({ [FILLER_TYPE]: { raw, summary: null } }), 'utf8');
  }

  // Every rejection in this race is expected to be a LockLostError.
  // `LockTimeoutError` is reachable in principle under this much contention
  // (`acquireLock` burns many retries first, so it is very unlikely); if it
  // ever fires, this assertion names the mechanism it saw so the failure
  // points at the right place rather than reading as a fencing-check bug.
  function expectLockLost(reason) {
    expect(reason).toMatchObject({ name: 'LockLostError', code: 'COORDINATION_LOCK_LOST' });
    expect(reason).toBeInstanceOf(LockLostError);
  }

  it('never resolves a recordObservation call whose observation did not reach disk — a lost lock rejects instead', async () => {
    await seedBulkFillerStore(historyFilePath);

    const orchestratorIds = Array.from({ length: 8 }, (_, index) => `orc-stall-${index}`);
    const settled = await Promise.allSettled(
      orchestratorIds.map((orchestratorId, index) =>
        recordObservation(
          historyFilePath,
          makeObservation(RACE_TYPE, index, { orchestratorId, peakMemoryMb: 200 }),
          {
            retentionCap: 100_000, // large enough that nothing folds/evicts mid-race
            lockStalenessMs: slowCriticalSectionStalenessMs,
            lockRetryDelayMs: 1,
          },
        ),
      ),
    );

    const persistedStore = JSON.parse(await fsPromises.readFile(historyFilePath, 'utf8'));
    const persistedOrchestratorIds = new Set(
      (persistedStore[RACE_TYPE]?.raw ?? []).map((entry) => entry.orchestratorId),
    );

    // Non-vacuity, stated in a way that does not depend on how the race
    // happened to resolve. Under enough contention EVERY racer can
    // legitimately lose its lock, so "at least one observation landed" is
    // not a safe expectation here — but the store must still be usable
    // afterwards. The mechanism refuses writes; it must never wedge the
    // history file.
    //
    // SAME `lockStalenessMs` AS THE RACERS, for the reason spelled out at
    // length on the twin probe in coordination-file.jest.spec.mjs: the race
    // can legitimately leave an ORPHANED `${historyFilePath}.lock` behind
    // (`releaseLock`'s restore path, documented as a fail-closed residual in
    // coordination-file.mjs), and an orphan is only reclaimable once it is
    // stale BY THE RECLAIMING CALLER'S OWN `lockStalenessMs`. Asking for 60s
    // after a 5ms race made such an orphan un-reclaimable for a full minute,
    // so this call burned LOCK_MAX_ATTEMPTS = 2000 × ~7.5ms of retries and
    // rejected with `LockTimeoutError`. The assertion below is unchanged and
    // still requires the write to land — under the racers' own staleness the
    // orphan is stale on sight, which IS the "never wedges the file" claim.
    await recordObservation(
      historyFilePath,
      makeObservation(RACE_TYPE, 999, { orchestratorId: 'orc-uncontended', peakMemoryMb: 200 }),
      { retentionCap: 100_000, lockStalenessMs: slowCriticalSectionStalenessMs },
    );
    const afterUncontended = JSON.parse(await fsPromises.readFile(historyFilePath, 'utf8'));
    const afterUncontendedIds = new Set((afterUncontended[RACE_TYPE]?.raw ?? []).map((entry) => entry.orchestratorId));
    expect(afterUncontendedIds.has('orc-uncontended')).toBe(true);

    orchestratorIds.forEach((orchestratorId, index) => {
      const outcome = settled[index];
      if (outcome.status === 'fulfilled') {
        // A resolved recordObservation claims its observation is on disk.
        // It must be.
        expect(persistedOrchestratorIds.has(orchestratorId)).toBe(true);
      } else {
        expectLockLost(outcome.reason);
      }
    });
  }, 30_000);

  it('does not let a stalled recordObservation clobber the write of a caller that legitimately reclaimed the lock', async () => {
    await seedBulkFillerStore(historyFilePath);

    // Caller A: driven directly, with a tiny lockStalenessMs so its
    // read-modify-write cycle against the bloated store outlives its own
    // lock's staleness window.
    const callerA = recordObservation(
      historyFilePath,
      makeObservation(RACE_TYPE, 1, { orchestratorId: 'caller-a', peakMemoryMb: 111 }),
      {
        retentionCap: 100_000,
        lockStalenessMs: slowCriticalSectionStalenessMs,
        lockRetryDelayMs: 1,
      },
    );

    // Caller B: races caller A for the same lock. Whichever one legitimately
    // reclaims it and completes its write, that write must be the one on
    // disk afterwards — never a merge with, nor a clobber by, a stale
    // caller-A rename().
    const callerB = recordObservation(
      historyFilePath,
      makeObservation(RACE_TYPE, 2, { orchestratorId: 'caller-b', peakMemoryMb: 222 }),
      {
        retentionCap: 100_000,
        lockStalenessMs: slowCriticalSectionStalenessMs,
        lockRetryDelayMs: 1,
      },
    );

    const [aOutcome, bOutcome] = await Promise.allSettled([callerA, callerB]);

    const persistedStore = JSON.parse(await fsPromises.readFile(historyFilePath, 'utf8'));
    const persistedOrchestratorIds = new Set(
      (persistedStore[RACE_TYPE]?.raw ?? []).map((entry) => entry.orchestratorId),
    );

    [
      ['caller-a', aOutcome],
      ['caller-b', bOutcome],
    ].forEach(([orchestratorId, outcome]) => {
      if (outcome.status === 'fulfilled') {
        expect(persistedOrchestratorIds.has(orchestratorId)).toBe(true);
      } else {
        expectLockLost(outcome.reason);
      }
    });
  }, 30_000);
});

// ---------------------------------------------------------------------------
// Phase 1 — remaining RED unit/integration coverage for the write-side
// ownership recheck. The outer-acceptance reproduction above (Phase 0) proves
// the end-to-end race; the tests below pin the individual mechanics it
// depends on, mirroring the idioms `coordination-file.jest.spec.mjs` already
// uses for `writeEntriesAtomic`/`assertStillHeld` (its "ownership choke
// point" and "read-only ownership re-check" describe blocks).
// ---------------------------------------------------------------------------

/**
 * Seeds `targetPath` with a bulk filler store under a reserved operation
 * type, exactly as the Phase 0 reproduction above does, so a single
 * `recordObservation` call's read-parse-serialise-write cycle takes long
 * enough to reliably win a poll-then-mutate race against this test (delete
 * the `.lock` sibling, or read the tmp sibling) without a fixed sleep.
 *
 * @param {string} targetPath
 */
async function seedFillerStore(targetPath) {
  const count = 20_000;
  const raw = Array.from({ length: count }, (_, index) => ({
    operationType: '__filler-type__',
    orchestratorId: `filler-${index}`,
    startedAt: 0,
    endedAt: 0,
    peakMemoryMb: 100,
    padding: 'x'.repeat(40),
  }));
  await fsPromises.writeFile(targetPath, JSON.stringify({ '__filler-type__': { raw, summary: null } }), 'utf8');
}

/**
 * Polls for `path` to exist, rather than a fixed sleep — lock-file creation
 * is itself async, so a fixed delay would be either too short (flaky) or
 * needlessly long (slow suite).
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

async function tmpSiblingsOf(dir) {
  return (await fsPromises.readdir(dir)).filter((name) => name.includes('.tmp-'));
}

describe('happy path unchanged — no reclaim, on-disk store shape identical', () => {
  it("writes the documented { [operationType]: { raw, summary } } shape, with recordObservation's usual field set, when nothing contends for the lock", async () => {
    await recordObservation(historyFilePath, makeObservation('shape-agent', 1), {
      retentionCap: RETENTION_CAP,
    });

    const persisted = JSON.parse(await fsPromises.readFile(historyFilePath, 'utf8'));

    expect(Object.keys(persisted)).toEqual(['shape-agent']);
    expect(Object.keys(persisted['shape-agent']).sort()).toEqual(['raw', 'summary']);
    expect(persisted['shape-agent'].raw).toHaveLength(1);
    expect(persisted['shape-agent'].summary).toBeNull();
    expect(Object.keys(persisted['shape-agent'].raw[0]).sort()).toEqual(
      [
        'operationType',
        'orchestratorId',
        'startedAt',
        'endedAt',
        'peakMemoryMb',
        'peakSwapMb',
        'freeDiskDeltaGb',
        'crashed',
      ].sort(),
    );

    // The fix must not change the uncontended happy path: no orphaned tmp
    // sibling, and the lock released cleanly.
    expect(await tmpSiblingsOf(workDir)).toEqual([]);
    expect(existsSync(`${historyFilePath}.lock`)).toBe(false);
  });
});

describe('write-side ownership recheck: ENOENT on the lock file is refused, not written', () => {
  it('rejects with LockLostError, rather than completing the write, when the lock file is absent at the write-side check', async () => {
    await seedFillerStore(historyFilePath);
    const lockPath = `${historyFilePath}.lock`;

    const recordPromise = recordObservation(historyFilePath, makeObservation('lock-absent-type', 1), {
      retentionCap: 100_000,
      lockStalenessMs: 60_000,
    });

    // Wait for recordObservation to actually acquire its lock, then delete
    // the lock file out from under it — not a reclaim by a contender (Phase
    // 0 already covers that shape), but the lock file's outright absence,
    // which `coordination-file.mjs`'s `assertStillHeld` documents as
    // "cannot prove ownership, so must not write" (absence-as-loss).
    await expect(waitForFile(lockPath)).resolves.toBe(true);
    await fsPromises.unlink(lockPath);

    await expect(recordPromise).rejects.toMatchObject({
      name: 'LockLostError',
      code: 'COORDINATION_LOCK_LOST',
    });
  }, 15_000);

  it('leaves no orphaned <historyFilePath>.tmp-* sibling behind when the write-side check refuses the write', async () => {
    await seedFillerStore(historyFilePath);
    const lockPath = `${historyFilePath}.lock`;

    const recordPromise = recordObservation(historyFilePath, makeObservation('lock-absent-tmp-type', 1), {
      retentionCap: 100_000,
      lockStalenessMs: 60_000,
    });

    await expect(waitForFile(lockPath)).resolves.toBe(true);
    await fsPromises.unlink(lockPath);

    await expect(recordPromise).rejects.toMatchObject({ code: 'COORDINATION_LOCK_LOST' });

    // Mirrors writeEntriesAtomic's cleanup-on-refusal (`unlink(tmpPath)` in
    // its catch block) — a refused write must not leak its staged sibling.
    expect(await tmpSiblingsOf(workDir)).toEqual([]);
  }, 15_000);

  it("propagates the original LockLostError unmasked through recordObservation's finally — releaseLock's own ENOENT is a silent no-op, not a second error", async () => {
    await seedFillerStore(historyFilePath);
    const lockPath = `${historyFilePath}.lock`;

    const recordPromise = recordObservation(historyFilePath, makeObservation('finally-masking-type', 1), {
      retentionCap: 100_000,
      lockStalenessMs: 60_000,
    });

    await expect(waitForFile(lockPath)).resolves.toBe(true);
    await fsPromises.unlink(lockPath);

    let caught;
    try {
      await recordPromise;
    } catch (error) {
      caught = error;
    }

    // If recordObservation's `finally` block's own `releaseLock(lockPath,
    // token)` call — reaching for a lock file that is now already gone —
    // threw and replaced what the try block threw, `caught` would be some
    // other error shape (or the test above's `waitForFile` timing would
    // instead see an unhandled rejection). `coordination-file.mjs`'s
    // `releaseLock` documents ENOENT-on-an-already-gone-lock as a silent
    // no-op for exactly this reason.
    expect(caught).toBeInstanceOf(LockLostError);
    expect(caught).toMatchObject({ name: 'LockLostError', code: 'COORDINATION_LOCK_LOST' });
  }, 15_000);
});

// ---------------------------------------------------------------------------
// pre-PR review, Medium — the finally-masking test above only drives
// releaseLock's ENOENT branch (a silent no-op that structurally cannot
// throw), which is false assurance: it proves nothing about the
// token-MISMATCH branch releaseLock takes on a genuine reclaim, which routes
// through `restoreOrDiscard` — the one path capable of rethrowing a second,
// unrelated error and masking the original LockLostError. This block drives
// that branch for real, via `reclaimStaleLock`, mirroring how
// `coordination-file.jest.spec.mjs`'s "withLock's finally never masks the
// error fn threw" block drives the analogous case for `claimCapacity`.
// ---------------------------------------------------------------------------
describe('write-side ownership recheck: a genuine reclaim (token mismatch) is not masked by releaseLock in the finally', () => {
  it("propagates the original LockLostError unmasked when a contender has genuinely reclaimed the lock — releaseLock's mismatch branch (restoreOrDiscard) runs but does not replace it", async () => {
    const { acquireLock, releaseLock, reclaimStaleLock } = await import('./coordination-file.mjs');
    await seedFillerStore(historyFilePath);
    const lockPath = `${historyFilePath}.lock`;

    const recordPromise = recordObservation(historyFilePath, makeObservation('token-mismatch-type', 1), {
      retentionCap: 100_000,
      lockStalenessMs: 60_000,
    });

    // Wait for recordObservation to acquire its own lock, then force a real
    // reclaim through the production path (`reclaimStaleLock` with a
    // negative max-age treats any lock as stale regardless of its actual
    // age) and re-acquire — exactly as a genuinely-stalled holder's lock
    // would be reclaimed by a live contender. The token now on disk is
    // provably different from the one recordObservation's own lockContext
    // holds, so its write-side `assertStillHeld()` recheck hits the
    // TOKEN-MISMATCH branch, not absence.
    await expect(waitForFile(lockPath)).resolves.toBe(true);
    expect(await reclaimStaleLock(lockPath, -1)).toBe(true);
    const contenderToken = await acquireLock(lockPath, 60_000);

    let caught;
    try {
      await recordPromise;
    } catch (error) {
      caught = error;
    }

    // recordObservation's finally still calls `releaseLock(lockPath, token)`
    // with ITS OWN (now-stale) token. Because the on-disk token is the
    // contender's, that call takes releaseLock's mismatch branch
    // (`restoreOrDiscard`) rather than the ENOENT no-op branch — and per the
    // fix, a release failure there must never replace what the try
    // block already threw. It did not: `caught` is still the original
    // LockLostError from the write-side recheck.
    expect(caught).toBeInstanceOf(LockLostError);
    expect(caught).toMatchObject({ name: 'LockLostError', code: 'COORDINATION_LOCK_LOST' });

    // The contender's own lock must be untouched by recordObservation's
    // failed release — releaseLock's fencing discipline means a mismatched
    // release is a safe no-op against the current holder, not a corruption.
    await releaseLock(lockPath, contenderToken);
  }, 15_000);
});

describe('writeStoreAtomic requires a lock context, checked before any rename', () => {
  async function callWriteStoreAtomic(...args) {
    return historyModule.writeStoreAtomic(...args);
  }

  it('rejects with a greppable message and creates nothing when called without a lockContext', async () => {
    await expect(
      callWriteStoreAtomic(historyFilePath, { 'some-type': { raw: [], summary: null } }),
    ).rejects.toThrow(/lockContext[\s\S]*assertStillHeld/i);

    expect(existsSync(historyFilePath)).toBe(false);
    expect(await tmpSiblingsOf(workDir)).toEqual([]);
  });

  it('rejects a lockContext whose assertStillHeld is not callable, rather than trusting the shape', async () => {
    await expect(
      callWriteStoreAtomic(historyFilePath, { 'some-type': { raw: [], summary: null } }, { token: 'x' }),
    ).rejects.toThrow(/lockContext[\s\S]*assertStillHeld/i);

    expect(existsSync(historyFilePath)).toBe(false);
    expect(await tmpSiblingsOf(workDir)).toEqual([]);
  });

  it('writes and renames exactly as before when a real lockContext still holds ownership', async () => {
    const { acquireLock, releaseLock, assertStillHeld } = await import('./coordination-file.mjs');
    const lockPath = `${historyFilePath}.lock`;
    const token = await acquireLock(lockPath, 60_000);

    await expect(
      callWriteStoreAtomic(historyFilePath, { 'some-type': { raw: [{ x: 1 }], summary: null } }, {
        lockPath,
        token,
        assertStillHeld: () => assertStillHeld(lockPath, token),
      }),
    ).resolves.toBeUndefined();

    expect(JSON.parse(readFileSync(historyFilePath, 'utf8'))).toEqual({
      'some-type': { raw: [{ x: 1 }], summary: null },
    });
    expect(await tmpSiblingsOf(workDir)).toEqual([]);

    await releaseLock(lockPath, token);
  });
});

describe('write-side ownership recheck staging order: stage tmp, then assert, then rename', () => {
  it('runs assertStillHeld after the tmp file is staged and before the rename — matching writeEntriesAtomic, not the other way round', () => {
    const fnIndex = historySource.indexOf('function writeStoreAtomic(');
    expect(fnIndex).toBeGreaterThan(-1);

    const stagedWriteIndex = historySource.indexOf('fs.writeFile(tmpPath,', fnIndex);
    const assertIndex = historySource.indexOf('lockContext.assertStillHeld()', fnIndex);
    const renameIndex = historySource.indexOf('fs.rename(tmpPath, historyFilePath)', fnIndex);

    expect(stagedWriteIndex).toBeGreaterThan(fnIndex);
    // Asserting before staging would widen the race (the residual window
    // would then include the whole serialise-and-write), which is exactly
    // the ordering mistake convergence-analysis edge case #1 flagged.
    expect(assertIndex).toBeGreaterThan(stagedWriteIndex);
    expect(renameIndex).toBeGreaterThan(assertIndex);
  });
});

// ---------------------------------------------------------------------------
// Phase 2 — the three test-coverage gaps its pre-PR review deferred
// (mirroring coordination-file.jest.spec.mjs's equivalent writeEntriesAtomic
// coverage exactly, per this ticket's own "Suggested fix").
// ---------------------------------------------------------------------------

describe('Phase 2 — writeStoreAtomic rename-failure cleanup (as opposed to an assertStillHeld failure)', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('cleans up the .tmp-* sibling and propagates the original rename error unmasked when the cleanup unlink succeeds', async () => {
    const { acquireLock, releaseLock, assertStillHeld } = await import('./coordination-file.mjs');
    const token = await acquireLock(`${historyFilePath}.lock`, 60_000);
    const lockPath = `${historyFilePath}.lock`;

    const renameError = new Error('simulated ENOSPC on rename');
    const originalRename = fsPromises.rename.bind(fsPromises);
    jest.spyOn(fsPromises, 'rename').mockImplementationOnce(async (src, dest) => {
      if (dest === historyFilePath) throw renameError;
      return originalRename(src, dest);
    });

    await expect(
      historyModule.writeStoreAtomic(historyFilePath, { 'rename-fail-type': { raw: [{ x: 1 }], summary: null } }, {
        lockPath,
        token,
        assertStillHeld: () => assertStillHeld(lockPath, token),
      }),
    ).rejects.toBe(renameError);

    expect(existsSync(historyFilePath)).toBe(false);
    expect(await tmpSiblingsOf(workDir)).toEqual([]);

    await releaseLock(lockPath, token);
  });

  it('swallows a failed cleanup unlink and still propagates the original rename error unmasked', async () => {
    const { acquireLock, releaseLock, assertStillHeld } = await import('./coordination-file.mjs');
    const lockPath = `${historyFilePath}.lock`;
    const token = await acquireLock(lockPath, 60_000);

    const renameError = new Error('simulated EACCES on rename');
    const originalRename = fsPromises.rename.bind(fsPromises);
    jest.spyOn(fsPromises, 'rename').mockImplementationOnce(async (src, dest) => {
      if (dest === historyFilePath) throw renameError;
      return originalRename(src, dest);
    });
    const originalUnlink = fsPromises.unlink.bind(fsPromises);
    jest.spyOn(fsPromises, 'unlink').mockImplementationOnce(async (target) => {
      if (String(target).includes('.tmp-')) {
        throw new Error('simulated unlink failure — races a concurrent cleanup');
      }
      return originalUnlink(target);
    });

    await expect(
      historyModule.writeStoreAtomic(historyFilePath, { 'rename-fail-type-2': { raw: [{ x: 1 }], summary: null } }, {
        lockPath,
        token,
        assertStillHeld: () => assertStillHeld(lockPath, token),
      }),
    ).rejects.toBe(renameError);

    expect(existsSync(historyFilePath)).toBe(false);

    await releaseLock(lockPath, token);
  });
});

describe('Phase 2 — non-LockLostError assertStillHeld / staging failures propagate unmasked', () => {
  it('propagates a non-LockLostError assertStillHeld failure (e.g. EACCES) unchanged, and still cleans up the tmp sibling', async () => {
    const eaccesError = Object.assign(new Error('EACCES: permission denied, open'), { code: 'EACCES' });

    await expect(
      historyModule.writeStoreAtomic(historyFilePath, { 'eacces-type': { raw: [], summary: null } }, {
        assertStillHeld: async () => {
          throw eaccesError;
        },
      }),
    ).rejects.toBe(eaccesError);

    expect(existsSync(historyFilePath)).toBe(false);
    expect(await tmpSiblingsOf(workDir)).toEqual([]);
  });

  it('propagates a failure from the tmp-stage write itself, without ever calling assertStillHeld', async () => {
    const stageError = new Error('simulated ENOSPC on the staged write');
    const originalWriteFile = fsPromises.writeFile.bind(fsPromises);
    jest.spyOn(fsPromises, 'writeFile').mockImplementationOnce(async (target, ...rest) => {
      if (String(target).includes('.tmp-')) throw stageError;
      return originalWriteFile(target, ...rest);
    });
    const assertStillHeldSpy = jest.fn();

    await expect(
      historyModule.writeStoreAtomic(historyFilePath, { 'stage-fail-type': { raw: [], summary: null } }, {
        assertStillHeld: assertStillHeldSpy,
      }),
    ).rejects.toBe(stageError);

    expect(assertStillHeldSpy).not.toHaveBeenCalled();
    expect(existsSync(historyFilePath)).toBe(false);

    jest.restoreAllMocks();
  });
});

// ---------------------------------------------------------------------------
// Phase 2 — retry-idempotency (the Convergence Analysis's critical
// edge case). `readStore`'s process-lifetime cache (see storeReadCache's own
// doc comment at the top of this file) returns the SAME object reference on
// every call for a given historyFilePath, and `recordObservation` mutates
// that SAME reference BEFORE `writeStoreAtomic` runs — the cache is only
// invalidated on a SUCCESSFUL write (`writeStoreAtomic`'s own
// `storeReadCache.delete(...)` call, reached only after `rename()` resolves).
//
// If `writeStoreAtomic` throws (a LockLostError from the write-side recheck,
// or any other failure), the cache-invalidation line is never reached — the
// in-process cache is left holding the UNCOMMITTED mutation forever (until
// this process exits). A LATER, genuinely successful `recordObservation` call
// for the SAME operationType then reads that poisoned cached object,
// silently RESURRECTING the previously-dropped observation into the store it
// writes to disk — even though `recordObservation`'s own documented contract
// says a lock-lost observation "is dropped entirely" and "nothing retries".
//
// RED BY CONSTRUCTION until `writeStoreAtomic` (or `recordObservation`'s own
// catch) invalidates `storeReadCache` for this path on ANY write failure, not
// only on success.
// ---------------------------------------------------------------------------

describe('Phase 2 — retry-idempotency: a failed write must not poison the in-process read cache', () => {
  it('does not resurrect a dropped observation from a failed write into a later, genuinely successful write for the same operationType', async () => {
    const operationType = 'cache-poison-type';
    const lockPath = `${historyFilePath}.lock`;

    // Bulk filler under an unrelated type, exactly like the Phase 0/Phase 1
    // reproductions above — needed so this call's read-parse-serialise cycle
    // takes long enough to reliably observe (and act on) the `.lock`
    // sibling from outside, without a fixed sleep.
    await seedFillerStore(historyFilePath);

    const firstAttempt = recordObservation(
      historyFilePath,
      makeObservation(operationType, 1, { peakMemoryMb: 111 }),
      { retentionCap: 100_000, lockStalenessMs: 60_000 },
    );

    // Force the write-side recheck to see the lock as absent — the same
    // "absence-as-loss" mechanism the Phase 0/Phase 1 tests above already
    // exercise — so this first attempt's mutation of the cached store object
    // is made, but its write never lands on disk.
    await expect(waitForFile(lockPath)).resolves.toBe(true);
    await fsPromises.unlink(lockPath);
    await expect(firstAttempt).rejects.toMatchObject({ code: 'COORDINATION_LOCK_LOST' });

    // The failed attempt genuinely did not persist — only the pre-seeded
    // filler type is on disk, never `operationType`.
    const onDiskAfterFailure = JSON.parse(await fsPromises.readFile(historyFilePath, 'utf8'));
    expect(onDiskAfterFailure[operationType]).toBeUndefined();

    // A later, real observation for the SAME operationType, with nothing
    // contending for the lock this time — this write must succeed cleanly.
    await recordObservation(historyFilePath, makeObservation(operationType, 2, { peakMemoryMb: 222 }), {
      retentionCap: 100_000,
    });

    const history = await readHistory(historyFilePath, operationType);

    // Only the second, genuinely-successful observation may be present.
    // A poisoned cache would resurrect the first (dropped) observation
    // alongside it, producing two raw entries where exactly one write ever
    // actually committed.
    expect(history.raw).toHaveLength(1);
    expect(history.raw.map((entry) => entry.peakMemoryMb)).toEqual([222]);
  }, 15_000);
});
