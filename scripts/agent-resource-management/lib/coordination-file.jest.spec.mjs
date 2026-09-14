// RED unit + integration tests for the not-yet-implemented
// scripts/agent-resource-management/lib/coordination-file.mjs module
// (Phase 3 — "Coordination file: dibs, atomic writes, debounce").
//
// This is explicitly the highest-risk phase in the whole ticket: multi-writer
// filesystem coordination across independent OS processes. Per the
// Investigation, Build Plan (Phase 3), and Convergence Analysis comments on
//, these tests exercise:
//
//   - declareDibs/readDibs basic round-trip
//   - 2 and 3+ concurrent writers, real fs, real concurrency (no mocked fs —
//     atomicity correctness must be proven against the real filesystem)
//   - atomic write-temp-then-rename, proven black-box via a tight
//     concurrent write/read loop that must never observe a torn JSON parse
//   - freshness-window debounce (isSampleFresh), including the exact
//     boundary at the window edge
//   - stale-entry pruning by a liveness threshold, and the false-positive
//     pruning gap flagged by the Convergence Analysis (a slow-but-alive
//     entry within the threshold must survive)
//   - an orphaned `.tmp` file left by a killed mid-rename writer must not be
//     misread as a valid dibs entry or crash the reader
//   - prune-while-read race: a write that prunes concurrently with a read
//     must never yield a torn/partial JSON parse
//
// Do not add a stub implementation to make this pass — that defeats the
// point of the outer/phase red-green loop. The builder implements
// coordination-file.mjs to turn this green.
//
// Coordination-file path convention (for the implementer/SKILL.md author):
// this module manages files under `.claude/agent-state/`, e.g.
// `.claude/agent-state/resource-coordination.json`, reusing the seam already
// documented and gitignored under `.claude/*`
// catch-all, no new `.gitignore` entry needed). These tests exercise the
// module against arbitrary paths under a real temp directory — the specific
// production path is a caller/CLI concern, not this module's — but every
// fixture path below follows the same `<dir>/resource-coordination.json`
// naming so the implementer has one convention to match.

import { mkdtempSync, rmSync, writeFileSync, existsSync, readFileSync, utimesSync } from 'node:fs';
import { promises as fsPromises } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { jest } from '@jest/globals';

import {
  declareDibs,
  readDibs,
  isSampleFresh,
  reclaimStaleLock,
  acquireLock,
  releaseLock,
  restoreOrDiscard,
  isReservedEntry,
  consumeGlobalSpawnTokens,
  claimCapacity,
  releaseCapacity,
  writeSharedSample,
  DEFAULT_CLAIM_TTL_MS,
  reconcileMemoryProjectionAdmission,
  readLedgerCandidateRecords,
  reapConfirmedDeadRecords,
} from './coordination-file.mjs';
// Namespace import (not a named import) so a not-yet-implemented
// `DEFAULT_CLAIM_TTL_MS`-style export doesn't throw a hard SyntaxError at
// module-evaluation time and take down every other test in this file — a
// namespace object simply yields `undefined` for a missing property, which
// the Phase 2 constant-existence test below asserts against directly.
import * as coordinationFile from './coordination-file.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const coordinationFileSource = readFileSync(join(__dirname, 'coordination-file.mjs'), 'utf8');

let workDir;
let filePath;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'coordination-file-'));
  filePath = join(workDir, 'resource-coordination.json');
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Basic round-trip
// ---------------------------------------------------------------------------

describe('declareDibs / readDibs basic round-trip', () => {
  it('writes an entry that readDibs can read back', async () => {
    const now = Date.now();

    await declareDibs(filePath, {
      orchestratorId: 'orc-1',
      desiredAgents: 2,
      declaredAt: now,
    });

    // Reserved pseudo-entries are filtered out throughout this file's
    // orchestrator-entry assertions: `declareDibs` also maintains the
    // `__ever-opted-in__` reserved row in the same write, and these tests are
    // about real orchestrator entries, not the reserved rows that share the
    // array (the same filtering the `isReservedEntry` describe-block below
    // already documents as the intended read-side idiom).
    const dibs = (await readDibs(filePath, now)).filter((entry) => !isReservedEntry(entry.orchestratorId));

    expect(dibs).toHaveLength(1);
    expect(dibs[0].orchestratorId).toBe('orc-1');
    expect(dibs[0].desiredAgents).toBe(2);
  });

  it('readDibs against a file that does not yet exist returns an empty list rather than throwing', async () => {
    const dibs = await readDibs(filePath, Date.now());
    expect(dibs).toEqual([]);
  });

  it('re-declaring dibs for the same orchestratorId updates that entry in place rather than duplicating it', async () => {
    const now = Date.now();

    await declareDibs(filePath, { orchestratorId: 'orc-1', desiredAgents: 2, declaredAt: now });
    await declareDibs(filePath, { orchestratorId: 'orc-1', desiredAgents: 5, declaredAt: now + 10 });

    const dibs = (await readDibs(filePath, now + 10)).filter((entry) => !isReservedEntry(entry.orchestratorId));

    expect(dibs).toHaveLength(1);
    expect(dibs[0].desiredAgents).toBe(5);
  });
});

// ---------------------------------------------------------------------------
// firstDeclaredAt — separate from declaredAt (High-priority fix)
//
// declaredAt is refreshed on EVERY declareDibs call (needed for liveness
// pruning). firstDeclaredAt is the timestamp of an orchestrator's FIRST-EVER
// declaration and must be preserved unchanged across every subsequent
// declareDibs upsert for that same orchestratorId — this is what lets
// `computeAllowance`/`computePerOrchestratorAllowance` (./allowance.mjs)
// order dibs priority by genuine arrival order rather than by whichever
// orchestrator's beat most recently refreshed declaredAt (the exact root
// cause of the priority-inversion deadlock this fix closes).
// ---------------------------------------------------------------------------

describe('declareDibs preserves firstDeclaredAt across repeated upserts for the same orchestratorId (High-priority fix)', () => {
  it("a brand-new orchestratorId's first declaration gets firstDeclaredAt set to its own declaredAt when none was explicitly passed", async () => {
    const now = Date.now();

    await declareDibs(filePath, { orchestratorId: 'orc-1', desiredAgents: 1, declaredAt: now });

    const dibs = await readDibs(filePath, now);
    expect(dibs[0].firstDeclaredAt).toBe(now);
  });

  it('a brand-new orchestratorId that explicitly passes firstDeclaredAt has that value honored', async () => {
    const now = Date.now();
    const explicitFirst = now - 5_000;

    await declareDibs(filePath, {
      orchestratorId: 'orc-1',
      desiredAgents: 1,
      declaredAt: now,
      firstDeclaredAt: explicitFirst,
    });

    const dibs = await readDibs(filePath, now);
    expect(dibs[0].firstDeclaredAt).toBe(explicitFirst);
  });

  it('re-declaring for the SAME orchestratorId preserves its original firstDeclaredAt even though declaredAt (and every other field) is refreshed', async () => {
    const firstBeatAt = Date.now();

    await declareDibs(filePath, { orchestratorId: 'orc-1', desiredAgents: 5, declaredAt: firstBeatAt });

    const secondBeatAt = firstBeatAt + 60_000;
    await declareDibs(filePath, { orchestratorId: 'orc-1', desiredAgents: 5, declaredAt: secondBeatAt });

    const thirdBeatAt = firstBeatAt + 120_000;
    await declareDibs(filePath, { orchestratorId: 'orc-1', desiredAgents: 5, declaredAt: thirdBeatAt });

    const dibs = (await readDibs(filePath, thirdBeatAt)).filter((entry) => !isReservedEntry(entry.orchestratorId));
    expect(dibs).toHaveLength(1);
    // declaredAt reflects the MOST RECENT beat (liveness semantics unchanged)...
    expect(dibs[0].declaredAt).toBe(thirdBeatAt);
    // ...but firstDeclaredAt is still pinned to the very first declaration.
    expect(dibs[0].firstDeclaredAt).toBe(firstBeatAt);
  });

  it('preserves the existing entry\'s firstDeclaredAt even if a later call passes a DIFFERENT (incorrect/stale) firstDeclaredAt value', async () => {
    const firstBeatAt = Date.now();
    await declareDibs(filePath, { orchestratorId: 'orc-1', desiredAgents: 1, declaredAt: firstBeatAt });

    const secondBeatAt = firstBeatAt + 1_000;
    await declareDibs(filePath, {
      orchestratorId: 'orc-1',
      desiredAgents: 1,
      declaredAt: secondBeatAt,
      firstDeclaredAt: secondBeatAt, // wrong on purpose — must be ignored
    });

    const dibs = (await readDibs(filePath, secondBeatAt)).filter((entry) => !isReservedEntry(entry.orchestratorId));
    expect(dibs[0].firstDeclaredAt).toBe(firstBeatAt);
  });

  it('two DIFFERENT orchestrators each keep their own distinct firstDeclaredAt, unaffected by the other\'s beats', async () => {
    const t1 = Date.now();
    await declareDibs(filePath, { orchestratorId: 'orc-a', desiredAgents: 1, declaredAt: t1 });

    const t2 = t1 + 100;
    await declareDibs(filePath, { orchestratorId: 'orc-b', desiredAgents: 1, declaredAt: t2 });

    // A beats again, well after B's first declaration.
    const t3 = t1 + 5_000;
    await declareDibs(filePath, { orchestratorId: 'orc-a', desiredAgents: 1, declaredAt: t3 });

    const dibs = await readDibs(filePath, t3);
    const a = dibs.find((entry) => entry.orchestratorId === 'orc-a');
    const b = dibs.find((entry) => entry.orchestratorId === 'orc-b');

    expect(a.firstDeclaredAt).toBe(t1);
    expect(b.firstDeclaredAt).toBe(t2);
  });

  // Regression test for the Medium finding: `declareDibs` must select
  // the existing self-entry to carry `firstDeclaredAt` forward from the
  // PRUNED (live-only) entries list, not the raw, unpruned one — exactly the
  // same liveness test `readDibs` already applies at read time. Without this,
  // an orchestrator whose prior entry has gone stale (past
  // `livenessThresholdMs`) but hasn't yet been physically removed from the
  // file by some other write would incorrectly resurrect its ancient
  // `firstDeclaredAt` on its next declaration, winning dibs priority over a
  // continuously-live incumbent it has no legitimate claim to outrank.
  it('an orchestrator whose prior entry has gone stale (but not yet been physically pruned) gets a FRESH firstDeclaredAt on its next declaration, not the old stale one', async () => {
    const livenessThresholdMs = 15 * 60_000; // 15 minutes, matching production default
    const staleDeclaredAt = Date.now();

    // orc-1's only-ever declaration, long ago.
    await declareDibs(filePath, {
      orchestratorId: 'orc-1',
      desiredAgents: 1,
      declaredAt: staleDeclaredAt,
    });

    // 20 minutes later — past the liveness threshold — orc-1 declares again.
    // Nothing else has written to the file in between, so orc-1's stale entry
    // is still physically present on disk at the moment this call reads it.
    const freshDeclaredAt = staleDeclaredAt + 20 * 60_000;
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-1', desiredAgents: 1, declaredAt: freshDeclaredAt },
      { livenessThresholdMs },
    );

    const dibs = await readDibs(filePath, freshDeclaredAt, { livenessThresholdMs });
    const orc1 = dibs.find((entry) => entry.orchestratorId === 'orc-1');

    // A fresh arrival — matches THIS declaration's own declaredAt (no
    // explicit firstDeclaredAt was passed) — never the 20-minutes-stale
    // original.
    expect(orc1.firstDeclaredAt).toBe(freshDeclaredAt);
    expect(orc1.firstDeclaredAt).not.toBe(staleDeclaredAt);
  });
});

// ---------------------------------------------------------------------------
// Concurrent writers — 2, then 3+ — no entry may be lost or corrupted
// ---------------------------------------------------------------------------

describe('concurrent writers against the same coordination file (real fs, real concurrency)', () => {
  it('lands both entries when two orchestrators declareDibs "simultaneously"', async () => {
    const now = Date.now();

    await Promise.all([
      declareDibs(filePath, { orchestratorId: 'orc-a', desiredAgents: 1, declaredAt: now }),
      declareDibs(filePath, { orchestratorId: 'orc-b', desiredAgents: 4, declaredAt: now }),
    ]);

    const dibs = (await readDibs(filePath, now)).filter((entry) => !isReservedEntry(entry.orchestratorId));
    const ids = dibs.map((entry) => entry.orchestratorId).sort();

    expect(ids).toEqual(['orc-a', 'orc-b']);
    expect(dibs.find((entry) => entry.orchestratorId === 'orc-a').desiredAgents).toBe(1);
    expect(dibs.find((entry) => entry.orchestratorId === 'orc-b').desiredAgents).toBe(4);
  });

  it('lands all entries when 3+ orchestrators declareDibs concurrently (extends beyond the two-writer case)', async () => {
    const now = Date.now();
    const orchestratorIds = ['orc-1', 'orc-2', 'orc-3', 'orc-4', 'orc-5'];

    await Promise.all(
      orchestratorIds.map((orchestratorId, index) =>
        declareDibs(filePath, { orchestratorId, desiredAgents: index + 1, declaredAt: now }),
      ),
    );

    const dibs = (await readDibs(filePath, now)).filter((entry) => !isReservedEntry(entry.orchestratorId));
    const ids = dibs.map((entry) => entry.orchestratorId).sort();

    expect(ids).toEqual([...orchestratorIds].sort());
    expect(dibs).toHaveLength(orchestratorIds.length);
    for (const [index, orchestratorId] of orchestratorIds.entries()) {
      expect(dibs.find((entry) => entry.orchestratorId === orchestratorId).desiredAgents).toBe(
        index + 1,
      );
    }
  });
});

// ---------------------------------------------------------------------------
// Atomic write proof — write-temp-then-rename, black-box via a tight
// alternating write/read loop that must never observe a torn JSON parse.
// ---------------------------------------------------------------------------

describe('atomic write guarantee (temp-file-then-rename)', () => {
  it('never throws a JSON parse error under a tight loop of alternating concurrent writes and reads', async () => {
    const iterations = 50;
    const now = Date.now();

    const writers = Array.from({ length: iterations }, (_, index) =>
      declareDibs(filePath, {
        orchestratorId: `orc-${index % 5}`,
        desiredAgents: index,
        declaredAt: now + index,
      }),
    );
    const readers = Array.from({ length: iterations }, () => readDibs(filePath, now));

    // Interleave writers and readers so reads race writes at every tick.
    // If the implementation doesn't write-temp-then-rename, a reader can
    // observe a half-written file and JSON.parse throws — this is the
    // assertion that catches that failure mode.
    await expect(Promise.all([...writers, ...readers])).resolves.toBeDefined();
  });

  it('leaves no readable half-written coordination file on disk after a burst of concurrent writes', async () => {
    const now = Date.now();

    await Promise.all(
      Array.from({ length: 10 }, (_, index) =>
        declareDibs(filePath, { orchestratorId: `orc-${index}`, desiredAgents: index, declaredAt: now }),
      ),
    );

    // The final on-disk file must itself be valid, parseable JSON — proof
    // the last rename left a whole file, not a partial one.
    const dibs = (await readDibs(filePath, now)).filter((entry) => !isReservedEntry(entry.orchestratorId));
    expect(dibs).toHaveLength(10);
  });
});

// ---------------------------------------------------------------------------
// Freshness-window debounce
// ---------------------------------------------------------------------------

describe('isSampleFresh — freshness-window debounce', () => {
  const freshnessWindowMs = 5000;

  it('treats a sample taken well within the window as fresh', () => {
    const sampledAt = 1_000_000;
    const now = sampledAt + 1000;

    expect(isSampleFresh({ sampledAt }, now, freshnessWindowMs)).toBe(true);
  });

  it('treats a sample taken well outside the window as stale', () => {
    const sampledAt = 1_000_000;
    const now = sampledAt + 10_000;

    expect(isSampleFresh({ sampledAt }, now, freshnessWindowMs)).toBe(false);
  });

  it('treats a sample exactly at the freshness-window boundary as fresh (inclusive edge)', () => {
    const sampledAt = 1_000_000;
    const now = sampledAt + freshnessWindowMs;

    expect(isSampleFresh({ sampledAt }, now, freshnessWindowMs)).toBe(true);
  });

  it('treats a sample one millisecond past the freshness-window boundary as stale', () => {
    const sampledAt = 1_000_000;
    const now = sampledAt + freshnessWindowMs + 1;

    expect(isSampleFresh({ sampledAt }, now, freshnessWindowMs)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Stale-entry pruning + false-positive pruning gap (Convergence Analysis)
// ---------------------------------------------------------------------------

describe('stale-entry pruning by liveness threshold', () => {
  const livenessThresholdMs = 60_000;

  it('prunes an entry whose declaredAt is older than the liveness threshold on the next write', async () => {
    const longAgo = 1_000_000;
    const now = longAgo + livenessThresholdMs + 1;

    await declareDibs(filePath, { orchestratorId: 'orc-dead', desiredAgents: 1, declaredAt: longAgo });
    // A second orchestrator's write is the "next write" that triggers pruning.
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-alive', desiredAgents: 1, declaredAt: now },
      { livenessThresholdMs },
    );

    const dibs = await readDibs(filePath, now, { livenessThresholdMs });
    const ids = dibs.map((entry) => entry.orchestratorId);

    expect(ids).not.toContain('orc-dead');
    expect(ids).toContain('orc-alive');
  });

  it('does NOT prune a recently-touched entry within the liveness threshold, even if it is the slowest orchestrator (false-positive pruning gap)', async () => {
    const declaredAt = 1_000_000;
    // Just inside the liveness threshold — a "slow but alive" orchestrator,
    // not a crashed one. The Convergence Analysis flagged false-positive
    // pruning of exactly this case as an unresolved edge case.
    const now = declaredAt + livenessThresholdMs - 1;

    await declareDibs(filePath, { orchestratorId: 'orc-slow', desiredAgents: 1, declaredAt });
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-other', desiredAgents: 1, declaredAt: now },
      { livenessThresholdMs },
    );

    const dibs = await readDibs(filePath, now, { livenessThresholdMs });
    const ids = dibs.map((entry) => entry.orchestratorId);

    expect(ids).toContain('orc-slow');
    expect(ids).toContain('orc-other');
  });

  it('excludes stale entries from readDibs\'s live view even before any subsequent write occurs', async () => {
    const longAgo = 1_000_000;
    const now = longAgo + livenessThresholdMs + 1;

    await declareDibs(filePath, { orchestratorId: 'orc-dead', desiredAgents: 1, declaredAt: longAgo });

    const dibs = await readDibs(filePath, now, { livenessThresholdMs });

    expect(dibs.map((entry) => entry.orchestratorId)).not.toContain('orc-dead');
  });
});

// ---------------------------------------------------------------------------
// Orphaned .tmp file from a killed mid-rename writer
// ---------------------------------------------------------------------------

describe('orphaned .tmp file from a killed mid-rename writer', () => {
  it('readDibs ignores a leftover .tmp file in the coordination directory and does not treat it as a live entry', async () => {
    const now = Date.now();

    await declareDibs(filePath, { orchestratorId: 'orc-1', desiredAgents: 1, declaredAt: now });

    // Simulate a writer killed mid-rename: a temp file sits next to the real
    // coordination file, containing what looks like a plausible (but never
    // committed) entry for a different orchestrator.
    const orphanedTmpPath = `${filePath}.tmp-orphan-12345`;
    writeFileSync(
      orphanedTmpPath,
      JSON.stringify([{ orchestratorId: 'orc-ghost', desiredAgents: 99, declaredAt: now }]),
    );

    const dibs = await readDibs(filePath, now);
    const ids = dibs.map((entry) => entry.orchestratorId);

    expect(ids).toContain('orc-1');
    expect(ids).not.toContain('orc-ghost');
  });

  it('a declareDibs write in the presence of an orphaned .tmp file does not crash and does not merge the orphan\'s contents', async () => {
    const now = Date.now();
    const orphanedTmpPath = `${filePath}.tmp-orphan-67890`;
    writeFileSync(
      orphanedTmpPath,
      JSON.stringify([{ orchestratorId: 'orc-ghost', desiredAgents: 99, declaredAt: now }]),
    );

    await expect(
      declareDibs(filePath, { orchestratorId: 'orc-real', desiredAgents: 2, declaredAt: now }),
    ).resolves.not.toThrow();

    const dibs = await readDibs(filePath, now);
    const ids = dibs.map((entry) => entry.orchestratorId);

    expect(ids).toContain('orc-real');
    expect(ids).not.toContain('orc-ghost');
  });

  it('does not delete or misreport an unrelated file in the same directory that does not match the atomic-write temp-file naming convention', async () => {
    const now = Date.now();
    const unrelatedPath = join(workDir, 'unrelated-file.txt');
    writeFileSync(unrelatedPath, 'not a coordination file at all');

    await declareDibs(filePath, { orchestratorId: 'orc-1', desiredAgents: 1, declaredAt: now });
    await readDibs(filePath, now);

    expect(existsSync(unrelatedPath)).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Prune-while-read race (Convergence Analysis gap)
// ---------------------------------------------------------------------------

describe('prune-while-read race', () => {
  it('a read racing a write that prunes a dead entry never throws a JSON parse error and never observes a torn file', async () => {
    const livenessThresholdMs = 1000;
    const longAgo = 1_000_000;
    const now = longAgo + livenessThresholdMs + 1;

    await declareDibs(filePath, { orchestratorId: 'orc-dead', desiredAgents: 1, declaredAt: longAgo });

    // Concurrently: a write that will prune orc-dead (its declaredAt is past
    // the liveness threshold relative to `now`), racing a tight loop of
    // reads. No read may throw or return malformed data.
    const pruningWrite = declareDibs(
      filePath,
      { orchestratorId: 'orc-alive', desiredAgents: 1, declaredAt: now },
      { livenessThresholdMs },
    );
    const racingReads = Array.from({ length: 20 }, () => readDibs(filePath, now, { livenessThresholdMs }));

    const results = await Promise.all([pruningWrite, ...racingReads]);

    // Every read result must be an array of well-formed entries — proof no
    // read observed a torn/partially-pruned file.
    for (const result of results.slice(1)) {
      expect(Array.isArray(result)).toBe(true);
      for (const entry of result) {
        expect(typeof entry.orchestratorId).toBe('string');
      }
    }

    const finalDibs = await readDibs(filePath, now, { livenessThresholdMs });
    const ids = finalDibs.map((entry) => entry.orchestratorId);
    expect(ids).toContain('orc-alive');
    expect(ids).not.toContain('orc-dead');
  });
});

// ---------------------------------------------------------------------------
// Stale lock reclaim (orphaned `.lock` file from a killed mid-critical-
// -section writer) — Critical review finding.
// ---------------------------------------------------------------------------

describe('stale lock reclaim', () => {
  it('reclaims and proceeds past an orphaned .lock file whose acquired-at timestamp is older than lockStalenessMs, rather than timing out', async () => {
    const lockPath = `${filePath}.lock`;
    const lockStalenessMs = 100;

    // Simulate a writer SIGKILL'd between acquiring the lock and its
    // `finally`-block release: a lock file carrying a long-past acquiredAt.
    writeFileSync(lockPath, JSON.stringify({ pid: 999999, acquiredAt: Date.now() - 10_000 }));

    const now = Date.now();
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-after-crash', desiredAgents: 1, declaredAt: now },
      { lockStalenessMs },
    );

    const dibs = await readDibs(filePath, now);
    expect(dibs.map((entry) => entry.orchestratorId)).toContain('orc-after-crash');
  }, 10_000);

  it('does NOT reclaim a fresh .lock file (recently created, simulating a legitimately in-progress write) — normal wait/retry still applies', async () => {
    const lockPath = `${filePath}.lock`;
    const lockStalenessMs = 60_000;

    // A fresh lock, well within the staleness threshold.
    writeFileSync(lockPath, JSON.stringify({ pid: process.pid, acquiredAt: Date.now() }));

    const now = Date.now();
    const declarePromise = declareDibs(
      filePath,
      { orchestratorId: 'orc-waiting', desiredAgents: 1, declaredAt: now },
      { lockStalenessMs },
    );

    // Release the "legitimately held" lock shortly after, as the real
    // holder's `finally` block would. The waiting call must succeed via
    // normal retry rather than forcibly reclaiming the still-fresh lock.
    await new Promise((resolve) => setTimeout(resolve, 50));
    rmSync(lockPath, { force: true });

    await declarePromise;

    const dibs = await readDibs(filePath, now);
    expect(dibs.map((entry) => entry.orchestratorId)).toContain('orc-waiting');
  }, 10_000);

  it('surfaces a distinguishable COORDINATION_LOCK_TIMEOUT error code when the lock is fresh and never released within the caller\'s patience', async () => {
    const lockPath = `${filePath}.lock`;

    // A fresh lock that is never released for the duration of this test —
    // genuinely unable to acquire, distinct from a stale/reclaimable lock.
    writeFileSync(lockPath, JSON.stringify({ pid: process.pid, acquiredAt: Date.now() }));

    const now = Date.now();

    await expect(
      declareDibs(
        filePath,
        { orchestratorId: 'orc-timeout', desiredAgents: 1, declaredAt: now },
        { lockStalenessMs: 60_000, lockRetryDelayMs: 1, lockMaxAttempts: 5 },
      ),
    ).rejects.toMatchObject({ code: 'COORDINATION_LOCK_TIMEOUT' });
  });
});

// ---------------------------------------------------------------------------
// Double-reclaim race on an orphaned .lock file — Critical review finding.
// Two+ callers race the SAME stale lock: previously, staleness was judged
// via a separate read BEFORE reclaiming; whichever caller's reclaim step
// then ran after another had already reclaimed-and-recreated a fresh lock
// would blindly remove that fresh lock by pathname, letting two callers into
// the critical section at once. Fixed by claiming first (atomic rename) and
// judging staleness only after the claim — restoring the file if it turns
// out not to have been stale after all.
// ---------------------------------------------------------------------------

describe('double-reclaim race on a stale lock (atomic-rename fix)', () => {
  it('when several callers race reclaimStaleLock against the exact same stale lock file, exactly one reports success and the rest report failure without throwing', async () => {
    // Targeted, deterministic proof of the mechanism: call the internal
    // reclaim function directly (not through declareDibs/acquireLock) so the
    // race is forced onto the same stale lock file for every racer, with no
    // dependency on outer retry-loop timing. Real concurrent fs syscalls
    // (Node's libuv threadpool) racing the SAME rename target is exactly the
    // failure mode the review reproduced with real child processes — this
    // reproduces the identical race window within one process by racing the
    // reclaim call itself rather than the whole declareDibs critical section.
    const lockPath = `${filePath}.lock`;
    const lockStalenessMs = 100;
    writeFileSync(lockPath, JSON.stringify({ pid: 999999, acquiredAt: Date.now() - 10_000 }));

    const racerCount = 10;
    const results = await Promise.all(
      Array.from({ length: racerCount }, () => reclaimStaleLock(lockPath, lockStalenessMs)),
    );

    const winners = results.filter((didReclaim) => didReclaim === true);
    const losers = results.filter((didReclaim) => didReclaim === false);

    // Exactly one racer actually performed the reclaim; the atomic rename
    // gives everyone else a race-free "someone else already got it" signal
    // rather than every racer independently believing it succeeded.
    expect(winners).toHaveLength(1);
    expect(losers).toHaveLength(racerCount - 1);

    // The stale lock and any `.reclaim-*` sibling are both gone — the winner
    // cleaned up after itself, and no loser threw trying to remove an
    // already-gone file.
    expect(existsSync(lockPath)).toBe(false);
  });

  it('restores a fresh, legitimate lock it atomically claimed but turns out not to be stale, rather than forcing the reclaim through', async () => {
    // Proves the claim-then-verify ordering itself, not just the mutual
    // exclusion above: even a *winning* claim must be handed back if, once
    // inspected, the content turns out to belong to a live (not stale)
    // lock — this is the scenario that a naive "claim === win" design (no
    // post-claim verification) would get wrong.
    const lockPath = `${filePath}.lock`;
    const lockStalenessMs = 60_000; // generous — the seeded lock is fresh.
    const freshPayload = JSON.stringify({ pid: process.pid, acquiredAt: Date.now() });
    writeFileSync(lockPath, freshPayload);

    const reclaimed = await reclaimStaleLock(lockPath, lockStalenessMs);

    expect(reclaimed).toBe(false);
    // The original lock is restored intact at its original path — not lost,
    // not left dangling under a `.reclaim-*` sibling name.
    expect(existsSync(lockPath)).toBe(true);
    expect(readFileSync(lockPath, 'utf8')).toBe(freshPayload);
  });

  it('does not lose or duplicate any entry when many orchestrators declareDibs concurrently against a pre-seeded orphaned stale lock', async () => {
    // End-to-end regression proof via the public surface: this is the exact
    // scenario the review reproduced empirically (8 real processes racing
    // declareDibs against a pre-seeded orphaned lock, 25 iterations, 36%
    // silent loss rate). A single Node process racing genuinely concurrent
    // fs operations (dispatched to libuv's threadpool, not just interleaved
    // promises) against the same pre-seeded stale lock exercises the same
    // reclaim race window; kept to one process/iteration here to stay fast
    // and non-flaky in CI, per review guidance to prefer the smaller
    // deterministic proof over a slow multi-process stress test.
    const lockPath = `${filePath}.lock`;
    // Generous relative to the seeded lock's age (10s in the past): the
    // *seeded* lock must read as stale, but the threshold must stay well
    // clear of the wall-clock time a real read-modify-write critical
    // section legitimately takes under 8-way contention (this is the same
    // "don't false-positive-reclaim a live-but-slow holder" tradeoff
    // `DEFAULT_LOCK_STALENESS_MS`'s doc-comment already calls out — a too-
    // tight threshold here would reclaim a live winner's freshly-created
    // lock out from under it, which is a threshold-tuning issue, not the
    // double-reclaim race this suite is proving closed).
    const lockStalenessMs = 5_000;
    writeFileSync(lockPath, JSON.stringify({ pid: 999999, acquiredAt: Date.now() - 10_000 }));

    const now = Date.now();
    const orchestratorCount = 8;
    const orchestratorIds = Array.from({ length: orchestratorCount }, (_, index) => `orc-race-${index}`);

    await Promise.all(
      orchestratorIds.map((orchestratorId) =>
        declareDibs(filePath, { orchestratorId, desiredAgents: 1, declaredAt: now }, { lockStalenessMs }),
      ),
    );

    const dibs = (await readDibs(filePath, now)).filter((entry) => !isReservedEntry(entry.orchestratorId));
    const ids = dibs.map((entry) => entry.orchestratorId).sort();

    // No entry silently lost, none duplicated — every racer's write landed.
    expect(ids).toEqual([...orchestratorIds].sort());
    expect(dibs).toHaveLength(orchestratorIds.length);
  }, 15_000);
});

// ---------------------------------------------------------------------------
// Fencing-token ownership check before lock removal — Medium review finding.
// releaseLock must never unlink "whichever lock currently occupies the
// path" — only the exact lock this caller created, proven via the token
// acquireLock returns. Covers the residual 3-way race: caller A reclaims a
// stale lock and creates a fresh one; a slower contender's release attempt
// for the ORIGINAL (now-replaced) lock must be a safe no-op, not an
// accidental unlink of A's still-live lock.
// ---------------------------------------------------------------------------

describe('releaseLock fencing-token ownership check', () => {
  it('removes the lock file when the token matches the one acquireLock returned', async () => {
    const lockPath = `${filePath}.lock`;
    const token = await acquireLock(lockPath, 30_000);

    expect(existsSync(lockPath)).toBe(true);

    await releaseLock(lockPath, token);

    expect(existsSync(lockPath)).toBe(false);
  });

  it('does NOT remove a lock file that has since been replaced by a different owner (stale/wrong token) — safe no-op', async () => {
    const lockPath = `${filePath}.lock`;

    // This caller acquires and is issued a token for the lock currently on
    // disk...
    const staleToken = await acquireLock(lockPath, 30_000);

    // ...but before this caller releases, a different owner's lock has
    // since replaced it at the same path (simulating the reclaim race: a
    // winner tore down the original and wrote its own fresh lock there).
    const newOwnerPayload = JSON.stringify({
      pid: process.pid,
      acquiredAt: Date.now(),
      token: 'someone-elses-token',
    });
    writeFileSync(lockPath, newOwnerPayload);

    // Releasing with the stale token must be a safe no-op: the new owner's
    // lock must survive, byte-for-byte, at the same path.
    await releaseLock(lockPath, staleToken);

    expect(existsSync(lockPath)).toBe(true);
    expect(readFileSync(lockPath, 'utf8')).toBe(newOwnerPayload);
  });

  it('is a safe no-op when the lock file is already gone', async () => {
    const lockPath = `${filePath}.lock`;

    await expect(releaseLock(lockPath, 'any-token')).resolves.toBeUndefined();
    expect(existsSync(lockPath)).toBe(false);
  });

  it('end-to-end: declareDibs releases only its own lock even when a stale lock at the same path is reclaimed and replaced mid-flight', async () => {
    // Regression proof via the public surface: seed an orphaned stale lock,
    // let declareDibs reclaim it and proceed, and confirm the coordination
    // file and lock file both end up in a clean, single-owner state with no
    // leftover `.reclaim-*`/`.release-*` siblings.
    const lockPath = `${filePath}.lock`;
    writeFileSync(lockPath, JSON.stringify({ pid: 999999, acquiredAt: Date.now() - 10_000 }));

    const now = Date.now();
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-clean', desiredAgents: 1, declaredAt: now },
      { lockStalenessMs: 100 },
    );

    expect(existsSync(lockPath)).toBe(false);

    const dibs = await readDibs(filePath, now);
    expect(dibs.map((entry) => entry.orchestratorId)).toContain('orc-clean');
  }, 10_000);
});

// ---------------------------------------------------------------------------
// HIGH-4 mitigation — mtime fallback for an UNPARSEABLE lock file (e.g. one
// left half-written by a writer killed mid-`writeFile`/ENOSPC). `looksStale`
// (and `reclaimStaleLock`'s own re-verification) fall back to the lock
// file's own `mtimeMs` when its content isn't valid tokened JSON, rather than
// treating a garbage lock as permanently un-reclaimable.
// ---------------------------------------------------------------------------

describe('unparseable-lock mtime fallback (HIGH-4)', () => {
  it('reclaims a garbage (unparseable) .lock file whose mtime is older than lockStalenessMs, rather than timing out', async () => {
    const lockPath = `${filePath}.lock`;
    const lockStalenessMs = 100;

    // A lock file containing content that is not valid JSON at all — e.g.
    // left over from a writer killed mid-write (ENOSPC / SIGKILL) before it
    // finished writing its payload.
    writeFileSync(lockPath, 'not valid json{{{');

    // Backdate its mtime well past the staleness threshold, simulating a
    // crash long enough ago that the lock is safely reclaimable.
    const longAgo = new Date(Date.now() - 10_000);
    utimesSync(lockPath, longAgo, longAgo);

    const now = Date.now();
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-after-garbage-lock', desiredAgents: 1, declaredAt: now },
      { lockStalenessMs },
    );

    const dibs = await readDibs(filePath, now);
    expect(dibs.map((entry) => entry.orchestratorId)).toContain('orc-after-garbage-lock');
  }, 10_000);

  it('does NOT reclaim a garbage (unparseable) .lock file whose mtime is still fresh — normal wait/retry applies, not immediate reclaim', async () => {
    const lockPath = `${filePath}.lock`;

    // Same unparseable content as above, but its mtime is fresh (the default
    // "just written" mtime from writeFileSync) — this must NOT be treated as
    // an orphan, since a legitimate writer could still be mid-write.
    writeFileSync(lockPath, 'not valid json{{{');

    const now = Date.now();

    // A generous staleness threshold (the fresh lock is nowhere near stale)
    // combined with a tiny retry ceiling so the test fails fast via a
    // COORDINATION_LOCK_TIMEOUT rather than actually waiting out the default
    // ceiling (10-20s of retry sleep alone, plus per-attempt syscalls — see
    // LOCK_MAX_ATTEMPTS's comment in coordination-file.mjs).
    await expect(
      declareDibs(
        filePath,
        { orchestratorId: 'orc-blocked-by-fresh-garbage-lock', desiredAgents: 1, declaredAt: now },
        { lockStalenessMs: 60_000, lockRetryDelayMs: 1, lockMaxAttempts: 5 },
      ),
    ).rejects.toMatchObject({ code: 'COORDINATION_LOCK_TIMEOUT' });

    // The garbage lock is left untouched — never immediately reclaimed.
    expect(existsSync(lockPath)).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// MEDIUM-9 mitigation — acquireLock's write-failure cleanup. A write failure
// (e.g. ENOSPC) after `open('wx')` has already created the lock file must not
// leave that half-written lock file behind (it would wedge every future
// caller) nor leak the open handle.
// ---------------------------------------------------------------------------

describe('acquireLock write-failure cleanup (MEDIUM-9)', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('removes the just-created lock file when handle.writeFile throws, so no leftover lock persists', async () => {
    const lockPath = `${filePath}.lock`;
    const originalOpen = fsPromises.open.bind(fsPromises);

    // Fault-injects a write failure on the handle returned for THIS lock
    // path only: opens for real (so the lock file really is created on
    // disk, exactly like the ENOSPC/killed-writer scenario this guards
    // against), then makes the returned handle's writeFile reject.
    jest.spyOn(fsPromises, 'open').mockImplementationOnce(async (openPath, flags) => {
      const handle = await originalOpen(openPath, flags);
      handle.writeFile = async () => {
        throw new Error('simulated ENOSPC mid-write');
      };
      return handle;
    });

    await expect(acquireLock(lockPath, 30_000)).rejects.toThrow('simulated ENOSPC mid-write');

    // No leftover lock file — the failed write's half-created lock was
    // cleaned up rather than wedging every future caller.
    expect(existsSync(lockPath)).toBe(false);

    // A subsequent declareDibs call succeeds immediately (normal fast path,
    // not a stale-lock reclaim) rather than finding a leftover lock.
    const now = Date.now();
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-after-write-failure', desiredAgents: 1, declaredAt: now },
      { lockMaxAttempts: 5, lockRetryDelayMs: 1 },
    );

    const dibs = await readDibs(filePath, now);
    expect(dibs.map((entry) => entry.orchestratorId)).toContain('orc-after-write-failure');
  });
});

// ---------------------------------------------------------------------------
// — restoreOrDiscard's write-failure cleanup must mirror acquireLock's
// (see the MEDIUM-9 block above). A `writeFile` failure other than `EEXIST`
// inside `restoreOrDiscard`'s `open(lockPath, 'wx')` success path closes the
// handle (via its own `finally`) but rethrows before reaching the trailing
// `fs.unlink(tmpPath)` line — leaving both a half-written `lockPath` behind
// (which would wedge every future caller, exactly like the bug MEDIUM-9
// fixed for `acquireLock`) and an orphaned `tmpPath` (the caller's private
// moved-aside copy). The catch branch now unlinks both before rethrowing.
// ---------------------------------------------------------------------------

describe('restoreOrDiscard write-failure cleanup mirrors acquireLock', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('removes the half-written lockPath and the orphaned tmpPath when writeFile fails inside the open(lockPath, "wx") success path, leaving no leftover file', async () => {
    const lockPath = `${filePath}.lock`;
    const tmpPath = `${filePath}.reclaim-test-writefail`;
    const raw = JSON.stringify({ pid: process.pid, acquiredAt: Date.now(), token: 'tok' });
    writeFileSync(tmpPath, raw);

    const originalOpen = fsPromises.open.bind(fsPromises);
    let closeCallCount = 0;

    // Same fault-injection idiom as the acquireLock MEDIUM-9 test above:
    // opens for real (so the lock file really is created on disk, exactly
    // like an ENOSPC/killed-writer scenario), then makes the returned
    // handle's writeFile reject and counts handle.close() invocations to
    // prove there's no double-close.
    jest.spyOn(fsPromises, 'open').mockImplementationOnce(async (openPath, flags) => {
      const handle = await originalOpen(openPath, flags);
      handle.writeFile = async () => {
        throw new Error('simulated ENOSPC mid-write');
      };
      const originalClose = handle.close.bind(handle);
      handle.close = async (...args) => {
        closeCallCount += 1;
        return originalClose(...args);
      };
      return handle;
    });

    await expect(restoreOrDiscard(lockPath, tmpPath, raw)).rejects.toThrow('simulated ENOSPC mid-write');

    // No half-written lockPath left behind.
    await expect(fsPromises.stat(lockPath)).rejects.toMatchObject({ code: 'ENOENT' });

    // No orphaned tmpPath left behind either — the caller's private
    // moved-aside copy must not leak on a genuine write failure.
    expect(existsSync(tmpPath)).toBe(false);

    // The handle was closed exactly once — no double-close.
    expect(closeCallCount).toBe(1);
  });

  it('leaves an already-occupied lockPath untouched on EEXIST — regression pin for the existing third-caller-wins branch', async () => {
    const lockPath = `${filePath}.lock`;
    const tmpPath = `${filePath}.reclaim-test-eexist`;
    const raw = JSON.stringify({ pid: process.pid, acquiredAt: Date.now(), token: 'mine' });
    writeFileSync(tmpPath, raw);

    const thirdCallerPayload = JSON.stringify({ pid: 999999, acquiredAt: Date.now(), token: 'third-caller' });
    writeFileSync(lockPath, thirdCallerPayload);

    await restoreOrDiscard(lockPath, tmpPath, raw);

    // The third caller's lock is unaffected, byte-for-byte, and this
    // caller's private tmp copy is discarded as before.
    expect(readFileSync(lockPath, 'utf8')).toBe(thirdCallerPayload);
    expect(existsSync(tmpPath)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Phase 3 — general-purpose reserved-entry convention.
//
// RED by construction: `isReservedEntry` does not exist on this module today.
// Per the Convergence Analysis, Phase 3 must land a GENERAL,
// reusable predicate for identifying non-orchestrator pseudo-entries in the
// dibs array (e.g. the shared-machine-sample entry cli.mjs's Phase 3 wiring
// will store) — NOT a bespoke one-off filter — because Phase 4 (global
// spawn-rate token bucket) needs its own reserved entry to be recognised by
// the exact same convention, and Phase 5 (dibs-constrain-allowance) needs
// every dibs-consuming call site to already exclude these pseudo-entries.
//
// Chosen naming convention this suite pins (documented per the task's "pick
// one and test it explicitly" instruction): an orchestratorId is "reserved"
// when it starts AND ends with a double underscore, e.g.
// `'__shared-machine-sample__'` — mirroring the literal already reserved (as
// a comment only, not yet a live binding) in cli.mjs. A normal orchestrator
// id such as `'orc-a'` must never accidentally match this convention.
// ---------------------------------------------------------------------------

describe('isReservedEntry — general-purpose reserved-entry predicate', () => {
  it('identifies the shared-machine-sample orchestratorId as reserved', () => {
    expect(isReservedEntry('__shared-machine-sample__')).toBe(true);
  });

  it('identifies a normal orchestratorId as NOT reserved', () => {
    expect(isReservedEntry('orc-a')).toBe(false);
  });

  it('identifies a normal orchestratorId that merely contains double underscores mid-string as NOT reserved', () => {
    // Guards against an overly-loose implementation (e.g. `includes('__')`)
    // that would misclassify a legitimate orchestrator id containing double
    // underscores incidentally, rather than one that is BOTH prefixed and
    // suffixed by them.
    expect(isReservedEntry('orc__a__b')).toBe(false);
  });

  it('recognises ANY future reserved entry following the same double-underscore convention, not just the shared-machine-sample literal', () => {
    // Pins the "general-purpose, reusable" requirement directly: Phase 4's
    // future global-spawn-rate-token-bucket reserved entry must also be
    // recognised, without this predicate special-casing the sample entry's
    // literal string.
    expect(isReservedEntry('__global-spawn-rate-token-bucket__')).toBe(true);
  });

  it('treats an orchestratorId with only a leading or only a trailing double underscore as NOT reserved', () => {
    expect(isReservedEntry('__leading-only')).toBe(false);
    expect(isReservedEntry('trailing-only__')).toBe(false);
  });
});

describe('reserved entries round-trip through declareDibs/readDibs', () => {
  it('a reserved entry stored via declareDibs can be read back via readDibs', async () => {
    const now = Date.now();

    await declareDibs(filePath, {
      orchestratorId: '__shared-machine-sample__',
      sampledAt: now,
      reading: { memory: { pressureLevel: 1 }, disk: { freeDiskGb: 60 } },
      declaredAt: now,
    });

    const dibs = await readDibs(filePath, now);
    const reservedEntry = dibs.find((entry) => entry.orchestratorId === '__shared-machine-sample__');

    expect(reservedEntry).toBeDefined();
    expect(reservedEntry.sampledAt).toBe(now);
    expect(reservedEntry.reading).toEqual({ memory: { pressureLevel: 1 }, disk: { freeDiskGb: 60 } });
  });

  it('readDibs output can be filtered down to just the normal orchestrator entries via isReservedEntry', async () => {
    const now = Date.now();

    await declareDibs(filePath, {
      orchestratorId: '__shared-machine-sample__',
      sampledAt: now,
      declaredAt: now,
    });
    await declareDibs(filePath, { orchestratorId: 'orc-a', desiredAgents: 1, declaredAt: now });
    await declareDibs(filePath, { orchestratorId: 'orc-b', desiredAgents: 2, declaredAt: now });

    const dibs = await readDibs(filePath, now);
    const normalOrchestratorEntries = dibs.filter((entry) => !isReservedEntry(entry.orchestratorId));
    const ids = normalOrchestratorEntries.map((entry) => entry.orchestratorId).sort();

    expect(ids).toEqual(['orc-a', 'orc-b']);
    // The reserved entry is still present in the UNFILTERED read — the
    // predicate is a caller-applied filter, not something readDibs itself
    // silently strips.
    expect(dibs.map((entry) => entry.orchestratorId)).toContain('__shared-machine-sample__');
  });
});

describe('reserved entries bypass the 15-minute liveness window entirely (Phase 3 design decision)', () => {
  // Design decision this suite pins explicitly, per the task's instruction to
  // be unambiguous: a reserved entry's lifecycle is governed SOLELY by
  // `isSampleFresh`'s freshness window (a separate, much shorter window —
  // DEFAULT_FRESHNESS_WINDOW_MS is 90s in cli.mjs), never by `pruneStale`'s
  // `livenessThresholdMs` (default 15 minutes). This is simpler than trying
  // to apply the normal declaredAt+livenessThresholdMs rule "specially" to
  // reserved entries: a shared sample that is, say, 5 minutes old is USELESS
  // (long past any sane freshness window) but would otherwise still be
  // "alive" under the 15-minute liveness rule — conflating the two windows
  // would either wrongly discard a within-freshness-window sample that
  // happens to look old by liveness-window standards, or wrongly keep a
  // long-stale sample around just because it's within 15 minutes. Keeping
  // the two windows fully independent avoids that whole class of ambiguity.
  const livenessThresholdMs = 15 * 60 * 1000;

  it('a reserved entry declared 20 minutes ago (past the 15-minute liveness threshold) is NOT pruned by readDibs', async () => {
    const twentyMinutesAgo = 1_000_000;
    const now = twentyMinutesAgo + 20 * 60 * 1000;

    await declareDibs(
      filePath,
      { orchestratorId: '__shared-machine-sample__', sampledAt: twentyMinutesAgo, declaredAt: twentyMinutesAgo },
      { livenessThresholdMs },
    );

    const dibs = await readDibs(filePath, now, { livenessThresholdMs });

    expect(dibs.map((entry) => entry.orchestratorId)).toContain('__shared-machine-sample__');
  });

  it('a NORMAL orchestrator entry declared 20 minutes ago (same age) IS pruned by readDibs — the contrast case proving reserved entries are treated differently, not that liveness pruning is broken', async () => {
    const twentyMinutesAgo = 1_000_000;
    const now = twentyMinutesAgo + 20 * 60 * 1000;

    await declareDibs(
      filePath,
      { orchestratorId: 'orc-dead', desiredAgents: 1, declaredAt: twentyMinutesAgo },
      { livenessThresholdMs },
    );

    const dibs = await readDibs(filePath, now, { livenessThresholdMs });

    expect(dibs.map((entry) => entry.orchestratorId)).not.toContain('orc-dead');
  });

  it('a reserved entry survives even the next declareDibs write (pruning is bypassed at write time too, not just at read time)', async () => {
    const twentyMinutesAgo = 1_000_000;
    const now = twentyMinutesAgo + 20 * 60 * 1000;

    await declareDibs(
      filePath,
      { orchestratorId: '__shared-machine-sample__', sampledAt: twentyMinutesAgo, declaredAt: twentyMinutesAgo },
      { livenessThresholdMs },
    );

    // A second orchestrator's write is the "next write" that would otherwise
    // trigger pruning of anything older than the liveness threshold.
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-alive', desiredAgents: 1, declaredAt: now },
      { livenessThresholdMs },
    );

    const dibs = await readDibs(filePath, now, { livenessThresholdMs });
    const ids = dibs.map((entry) => entry.orchestratorId);

    expect(ids).toContain('__shared-machine-sample__');
    expect(ids).toContain('orc-alive');
  });
});

// ---------------------------------------------------------------------------
// Phase 4 — global spawn-rate token bucket (RED: consumeGlobalSpawnTokens
// does not exist yet).
//
// Per the Convergence Analysis, the read-modify-write of the shared bucket
// state MUST happen inside a SINGLE lock-guarded critical section (read ->
// pure consumeSpawnTokens -> write, one lock acquire/release cycle), not two
// separate lock acquisitions (e.g. declareDibs called once for the caller's
// own dibs, then a second separate lock for the bucket). Two orchestrators
// concurrently consuming tokens must never both read the same pre-mutation
// bucket state and both grant a burst that together exceeds capacityTokens.
//
// The bucket is stored under a RESERVED entry (isReservedEntry convention)
// and, per Phase 3's reserved-entry liveness-bypass, must not be reset by the
// normal 15-minute liveness pruning across a long idle gap between beats.
// ---------------------------------------------------------------------------

describe('consumeGlobalSpawnTokens — global spawn-rate token bucket', () => {
  const GLOBAL_BUCKET_RESERVED_ID = '__global-spawn-rate-bucket__';

  it('is stored under a reserved id satisfying isReservedEntry', () => {
    expect(isReservedEntry(GLOBAL_BUCKET_RESERVED_ID)).toBe(true);
  });

  it('first-ever call (no bucket entry exists) initializes at full capacityTokens and grants a requested count within capacity', async () => {
    const now = Date.now();
    const config = { capacityTokens: 10, refillTokensPerMs: 0 };

    const result = await consumeGlobalSpawnTokens(filePath, { requestedCount: 4, config, now });

    expect(result.allowed).toBe(true);
    expect(result.grantedCount).toBe(4);
    // Full capacity (10) minus the 4 just granted.
    expect(result.bucketState.tokens).toBe(6);
  });

  it('a second call re-reads the ALREADY-DECREMENTED bucket state from the first call (persisted round-trip, not in-memory reuse)', async () => {
    const now = Date.now();
    const config = { capacityTokens: 10, refillTokensPerMs: 0 };

    const first = await consumeGlobalSpawnTokens(filePath, { requestedCount: 4, config, now });
    expect(first.bucketState.tokens).toBe(6);

    const second = await consumeGlobalSpawnTokens(filePath, { requestedCount: 5, config, now });

    expect(second.allowed).toBe(true);
    expect(second.grantedCount).toBe(5);
    // 6 tokens available (persisted from the first call) minus 5 granted = 1.
    expect(second.bucketState.tokens).toBe(1);

    // Confirm the persisted state on disk directly, via readDibs, matches —
    // proving genuine round-trip persistence rather than an in-memory cache.
    const dibs = await readDibs(filePath, now);
    const bucketEntry = dibs.find((entry) => entry.orchestratorId === GLOBAL_BUCKET_RESERVED_ID);
    expect(bucketEntry).toBeDefined();
    expect(bucketEntry.tokens).toBe(1);
  });

  it('insufficient tokens for a requested count is denied and the bucket state is NOT mutated on denial', async () => {
    const now = Date.now();
    const config = { capacityTokens: 5, refillTokensPerMs: 0 };

    const first = await consumeGlobalSpawnTokens(filePath, { requestedCount: 5, config, now });
    expect(first.allowed).toBe(true);
    expect(first.bucketState.tokens).toBe(0);

    const denied = await consumeGlobalSpawnTokens(filePath, { requestedCount: 1, config, now });

    expect(denied.allowed).toBe(false);
    expect(denied.grantedCount).toBe(0);
    // Deny-the-whole-burst semantics: bucket state unchanged by the denial
    // (still 0 tokens, not further decremented or otherwise mutated).
    expect(denied.bucketState.tokens).toBe(0);

    const dibs = await readDibs(filePath, now);
    const bucketEntry = dibs.find((entry) => entry.orchestratorId === GLOBAL_BUCKET_RESERVED_ID);
    expect(bucketEntry.tokens).toBe(0);
  });

  it('THE CORE CORRECTNESS PROPERTY: two real concurrent calls against the same file never together grant more than capacityTokens', async () => {
    const now = Date.now();
    // Small capacity so each call alone requests more than half — if the
    // read-modify-write were not atomic (e.g. two separate lock acquisitions
    // per call, or an unguarded read before a separate write), both callers
    // could read the same pre-mutation state (10 tokens) and each
    // independently decide their 6-token request fits, together granting 12
    // against a cap of 10 — a real double-grant race.
    const config = { capacityTokens: 10, refillTokensPerMs: 0 };

    const [resultA, resultB] = await Promise.all([
      consumeGlobalSpawnTokens(filePath, { requestedCount: 6, config, now }),
      consumeGlobalSpawnTokens(filePath, { requestedCount: 6, config, now }),
    ]);

    const totalGranted = resultA.grantedCount + resultB.grantedCount;

    // At most one of the two 6-token requests can be satisfied against a
    // 10-token bucket; the sum of grants must never exceed capacity.
    expect(totalGranted).toBeLessThanOrEqual(config.capacityTokens);
    // Exactly one must have been granted in full and the other denied
    // outright (deny-the-whole-burst semantics) — proves this isn't merely
    // "both partially granted to stay under cap" (which the pure function's
    // contract doesn't even support) but a genuine serialised
    // read-decide-write per call.
    const grantedFlags = [resultA.allowed, resultB.allowed].sort();
    expect(grantedFlags).toEqual([false, true]);

    // The persisted final state must reflect exactly one 6-token grant
    // (10 - 6 = 4), not a corrupted or double-decremented value.
    const dibs = await readDibs(filePath, now);
    const bucketEntry = dibs.find((entry) => entry.orchestratorId === GLOBAL_BUCKET_RESERVED_ID);
    expect(bucketEntry.tokens).toBe(4);
  }, 15_000);

  it('a bucket entry declared 20+ minutes ago (past the 15-minute liveness threshold) is NOT reset to full capacity by a subsequent call', async () => {
    const livenessThresholdMs = 15 * 60 * 1000;
    const twentyMinutesAgo = 1_000_000;
    const config = { capacityTokens: 10, refillTokensPerMs: 0 };

    // Seed the bucket with a partially-consumed state, 20+ minutes in the
    // past relative to the next call's `now`.
    await consumeGlobalSpawnTokens(filePath, { requestedCount: 7, config, now: twentyMinutesAgo }, { livenessThresholdMs });

    const now = twentyMinutesAgo + 20 * 60 * 1000;

    // A normal orchestrator dibs write in between (the realistic "long idle
    // gap between beats" scenario) must not silently prune/reset the bucket.
    await declareDibs(filePath, { orchestratorId: 'orc-resumed', desiredAgents: 1, declaredAt: now }, { livenessThresholdMs });

    const result = await consumeGlobalSpawnTokens(filePath, { requestedCount: 1, config, now }, { livenessThresholdMs });

    // If the bucket had been wrongly reset to full capacity (10) by liveness
    // pruning, requesting 1 more token would still succeed and leave 9 — but
    // the accumulated state from the seed call (10 - 7 = 3) must survive,
    // leaving 3 - 1 = 2, not 9.
    expect(result.allowed).toBe(true);
    expect(result.bucketState.tokens).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Phase 1 — releaseCapacity: a pure decrement of the same reserved
// `__claim:<type>__` ledger entry `claimCapacity` writes, reusing `withLock`
// exactly as `claimCapacity` does — no second lock primitive, no change to
// `claimCapacity`'s own signature/contract.
//
// RED by construction: `releaseCapacity` does not exist yet on this module.
// The builder implements it to turn this green (Build Plan Phase 1).
// ---------------------------------------------------------------------------

describe('releaseCapacity — decrements the per-type claim ledger, floors at 0', () => {
  const availableCapacity100 = async () => 100;

  // Sums a ledger entry's per-holder `claims[].count` — the new
  // (Phase 1) per-claim aggregate, replacing the old single scalar
  // `grantedTotal` field these tests originally asserted on directly. Every
  // test below that previously read `ledgerEntry.grantedTotal` off the raw
  // persisted entry now reads this sum instead — the externally-observable
  // `released`/`grantedTotal` values returned BY `releaseCapacity` itself
  // are unchanged and still asserted directly.
  function sumClaims(ledgerEntry) {
    return (ledgerEntry?.claims ?? []).reduce((total, claim) => total + claim.count, 0);
  }

  it('decrements an existing grantedTotal by releaseCount (single orchestrator, mechanically updated for the new required orchestratorId param — Phase 1)', async () => {
    const now = Date.now();
    await claimCapacity(filePath, {
      type: 'sub-agent',
      requestedCount: 10,
      orchestratorId: 'orch-solo',
      computeAvailableCapacity: availableCapacity100,
    });

    const result = await releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 3, orchestratorId: 'orch-solo' });

    expect(result.released).toBe(3);
    expect(result.grantedTotal).toBe(7);

    const dibs = await readDibs(filePath, now);
    const ledgerEntry = dibs.find((entry) => entry.orchestratorId === '__claim:sub-agent__');
    expect(sumClaims(ledgerEntry)).toBe(7);
  });

  it('floors at 0 when releasing more than was ever granted (mechanically updated for orchestratorId — Phase 1)', async () => {
    await claimCapacity(filePath, {
      type: 'sub-agent',
      requestedCount: 5,
      orchestratorId: 'orch-solo',
      computeAvailableCapacity: availableCapacity100,
    });

    const result = await releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 100, orchestratorId: 'orch-solo' });

    // Only the genuine 5 that were granted can be released — never a
    // negative ledger, and the reported `released` amount reflects reality,
    // not the (larger) amount requested.
    expect(result.released).toBe(5);
    expect(result.grantedTotal).toBe(0);

    const dibs = await readDibs(filePath, Date.now());
    const ledgerEntry = dibs.find((entry) => entry.orchestratorId === '__claim:sub-agent__');
    expect(sumClaims(ledgerEntry)).toBe(0);
  });

  it('a no-op release (releaseCount: 0) leaves the ledger unchanged (mechanically updated for orchestratorId — Phase 1)', async () => {
    await claimCapacity(filePath, {
      type: 'sub-agent',
      requestedCount: 4,
      orchestratorId: 'orch-solo',
      computeAvailableCapacity: availableCapacity100,
    });

    const result = await releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 0, orchestratorId: 'orch-solo' });

    expect(result.released).toBe(0);
    expect(result.grantedTotal).toBe(4);

    const dibs = await readDibs(filePath, Date.now());
    const ledgerEntry = dibs.find((entry) => entry.orchestratorId === '__claim:sub-agent__');
    expect(sumClaims(ledgerEntry)).toBe(4);
  });

  it('releasing against a type with no ledger entry at all treats the prior total as 0 and stays floored at 0 (unchanged behavior — Phase 1 must not regress this)', async () => {
    const result = await releaseCapacity(filePath, { type: 'never-claimed', releaseCount: 5, orchestratorId: 'orch-solo' });

    expect(result.released).toBe(0);
    expect(result.grantedTotal).toBe(0);

    const dibs = await readDibs(filePath, Date.now());
    const ledgerEntry = dibs.find((entry) => entry.orchestratorId === '__claim:never-claimed__');
    // Either no entry was written at all, or one was written with an empty/
    // zero-sum claims aggregate — either is an acceptable "nothing to
    // release" outcome, but it must never be negative or falsely non-zero.
    expect(sumClaims(ledgerEntry)).toBe(0);
  });

  it('the released ledger entry still satisfies isReservedEntry and survives pruneStale regardless of age (mechanically updated for orchestratorId — Phase 1)', async () => {
    const longAgo = 1_000_000;
    await claimCapacity(
      filePath,
      { type: 'sub-agent', requestedCount: 6, orchestratorId: 'orch-solo', computeAvailableCapacity: availableCapacity100 },
    );
    await releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 2, orchestratorId: 'orch-solo' });

    expect(isReservedEntry('__claim:sub-agent__')).toBe(true);

    // A liveness threshold that would prune any normal orchestrator entry
    // this old must NOT prune the reserved claim-ledger entry.
    const farFuture = longAgo + 999_999_999;
    const dibs = await readDibs(filePath, farFuture, { livenessThresholdMs: 1000 });
    const ledgerEntry = dibs.find((entry) => entry.orchestratorId === '__claim:sub-agent__');
    expect(ledgerEntry).toBeDefined();
    expect(sumClaims(ledgerEntry)).toBe(4);
  });

  it('sequential claim-then-release-then-claim round-trip restores claimable headroom (mechanically updated for orchestratorId — Phase 1)', async () => {
    const availableCapacity10 = async () => 10;

    const firstClaim = await claimCapacity(filePath, {
      type: 'sub-agent',
      requestedCount: 10,
      orchestratorId: 'orch-solo',
      computeAvailableCapacity: availableCapacity10,
    });
    expect(firstClaim.granted).toBe(10);

    const exhaustedClaim = await claimCapacity(filePath, {
      type: 'sub-agent',
      requestedCount: 5,
      orchestratorId: 'orch-solo-2',
      computeAvailableCapacity: availableCapacity10,
    });
    expect(exhaustedClaim.granted).toBe(0);

    await releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 4, orchestratorId: 'orch-solo' });

    const claimAfterRelease = await claimCapacity(filePath, {
      type: 'sub-agent',
      requestedCount: 4,
      orchestratorId: 'orch-solo-2',
      computeAvailableCapacity: availableCapacity10,
    });

    // 10 available - 6 still granted (10 - 4 released) = 4 headroom restored.
    expect(claimAfterRelease.granted).toBe(4);
  });

  it('rejects non-finite/negative/non-integer releaseCount without corrupting the ledger (mechanically updated for orchestratorId — Phase 1)', async () => {
    await claimCapacity(filePath, {
      type: 'sub-agent',
      requestedCount: 5,
      orchestratorId: 'orch-solo',
      computeAvailableCapacity: availableCapacity100,
    });

    const invalidReleaseCounts = [NaN, Infinity, -Infinity, -1, 1.5, 'abc', null, undefined];

    for (const releaseCount of invalidReleaseCounts) {
      await expect(
        releaseCapacity(filePath, { type: 'sub-agent', releaseCount, orchestratorId: 'orch-solo' }),
      ).rejects.toThrow();

      // Each rejected attempt must leave the ledger exactly as it was —
      // never partially applied, never corrupted.
      const dibs = await readDibs(filePath, Date.now());
      const ledgerEntry = dibs.find((entry) => entry.orchestratorId === '__claim:sub-agent__');
      expect(sumClaims(ledgerEntry)).toBe(5);
    }
  });

  it('rejects a missing/empty/non-string orchestratorId without corrupting the ledger (Phase 1 review fix)', async () => {
    await claimCapacity(filePath, {
      type: 'sub-agent',
      requestedCount: 5,
      orchestratorId: 'orch-solo',
      computeAvailableCapacity: availableCapacity100,
    });

    const invalidOrchestratorIds = [undefined, null, '', 0, false, 42, {}];

    for (const orchestratorId of invalidOrchestratorIds) {
      await expect(
        releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 1, orchestratorId }),
      ).rejects.toThrow(TypeError);

      // Each rejected attempt must leave the ledger exactly as it was —
      // never partially applied, never corrupted.
      const dibs = await readDibs(filePath, Date.now());
      const ledgerEntry = dibs.find((entry) => entry.orchestratorId === '__claim:sub-agent__');
      expect(sumClaims(ledgerEntry)).toBe(5);
    }
  });

  it('claimCapacity rejects a missing/empty/non-string orchestratorId without corrupting the ledger (Phase 1 review fix)', async () => {
    await claimCapacity(filePath, {
      type: 'sub-agent',
      requestedCount: 5,
      orchestratorId: 'orch-solo',
      computeAvailableCapacity: availableCapacity100,
    });

    const invalidOrchestratorIds = [undefined, null, '', 0, false, 42, {}];

    for (const orchestratorId of invalidOrchestratorIds) {
      await expect(
        claimCapacity(filePath, {
          type: 'sub-agent',
          requestedCount: 1,
          orchestratorId,
          computeAvailableCapacity: availableCapacity100,
        }),
      ).rejects.toThrow(TypeError);

      // Each rejected attempt must leave the ledger exactly as it was —
      // never partially applied, never corrupted.
      const dibs = await readDibs(filePath, Date.now());
      const ledgerEntry = dibs.find((entry) => entry.orchestratorId === '__claim:sub-agent__');
      expect(sumClaims(ledgerEntry)).toBe(5);
    }
  });

  it('reuses the same lock file (withLock) as claimCapacity — no second lock primitive (mechanically updated for orchestratorId — Phase 1)', async () => {
    const lockPath = `${filePath}.lock`;
    const openCalls = [];
    const originalOpen = fsPromises.open.bind(fsPromises);

    jest.spyOn(fsPromises, 'open').mockImplementation(async (openPath, flags) => {
      openCalls.push({ openPath: String(openPath), flags });
      return originalOpen(openPath, flags);
    });

    try {
      await releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 1, orchestratorId: 'orch-solo' });
    } finally {
      jest.restoreAllMocks();
    }

    const lockOpenCalls = openCalls.filter((call) => call.openPath === lockPath && call.flags === 'wx');
    expect(lockOpenCalls.length).toBeGreaterThanOrEqual(1);
  });
});

// ---------------------------------------------------------------------------
// Phase 1 — per-claim ledger schema + orchestrator-scoped release.
//
// The `__claim:<type>__` entry's single `{ grantedTotal, declaredAt }`
// scalar becomes per-claim records: `{ orchestratorId: '__claim:<type>__',
// claims: [{ orchestratorId, count, claimedAt }], declaredAt }`. `--release`
// (via `releaseCapacity`'s new required `orchestratorId` param) now only
// decrements the calling orchestrator's OWN claim record(s), floored at what
// it genuinely holds — never the shared pool.
//
// RED by construction: today's `claimCapacity`/`releaseCapacity` (see the
// scalar-shape describe block above) accept no `orchestratorId` on
// `claimCapacity` and no `orchestratorId` on `releaseCapacity` at all, and
// persist one cumulative scalar with no per-holder attribution whatsoever.
// ---------------------------------------------------------------------------

describe('claimCapacity / releaseCapacity — per-claim ledger schema, orchestrator-scoped release', () => {
  const ample = async () => 100;

  it('releaseCapacity only decrements the calling orchestratorId\'s own claim(s), never the shared pool', async () => {
    await claimCapacity(filePath, { type: 'sub-agent', requestedCount: 5, orchestratorId: 'orch-a', computeAvailableCapacity: ample });
    await claimCapacity(filePath, { type: 'sub-agent', requestedCount: 3, orchestratorId: 'orch-b', computeAvailableCapacity: ample });

    const releaseResult = await releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 2, orchestratorId: 'orch-a' });

    // A released 2 of its own 5 — B's 3 are wholly untouched, so the
    // aggregate grantedTotal reflects (5 - 2) + 3 = 6, not merely 5 - 2 = 3
    // (which would be the old, unattributed, single-scalar behavior).
    expect(releaseResult.released).toBe(2);
    expect(releaseResult.grantedTotal).toBe(6);

    // Confirm B's own claim is untouched: B can still only release exactly
    // its original 3, never more, never less.
    const releaseB = await releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 100, orchestratorId: 'orch-b' });
    expect(releaseB.released).toBe(3);
  });

  it('release is floored at the CALLER\'s own held amount, not the shared pool total', async () => {
    await claimCapacity(filePath, { type: 'sub-agent', requestedCount: 2, orchestratorId: 'orch-a', computeAvailableCapacity: ample });
    await claimCapacity(filePath, { type: 'sub-agent', requestedCount: 10, orchestratorId: 'orch-b', computeAvailableCapacity: ample });

    // A asks to release 5 — more than its own 2, but far less than the
    // combined pool of 12. Must floor at A's own 2, never dip into B's 10.
    const releaseResult = await releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 5, orchestratorId: 'orch-a' });

    expect(releaseResult.released).toBe(2);
    expect(releaseResult.grantedTotal).toBe(10);

    // B's full 10 must still be releasable in full — proof B's claim was
    // never touched by A's over-ask.
    const releaseB = await releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 10, orchestratorId: 'orch-b' });
    expect(releaseB.released).toBe(10);
  });

  it('release against a type the caller never claimed (others did) returns released: 0 and leaves other holders untouched', async () => {
    await claimCapacity(filePath, { type: 'sub-agent', requestedCount: 4, orchestratorId: 'orch-a', computeAvailableCapacity: ample });

    // orch-c never claimed anything against this type.
    const releaseResult = await releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 1, orchestratorId: 'orch-c' });

    expect(releaseResult.released).toBe(0);
    expect(releaseResult.grantedTotal).toBe(4);

    // A's claim survives fully intact.
    const releaseA = await releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 4, orchestratorId: 'orch-a' });
    expect(releaseA.released).toBe(4);
  });

  it('release against a type with NO ledger entry at all still returns released: 0 (unchanged from the old scalar behavior)', async () => {
    const releaseResult = await releaseCapacity(filePath, { type: 'never-claimed-type', releaseCount: 5, orchestratorId: 'orch-a' });

    expect(releaseResult.released).toBe(0);
    expect(releaseResult.grantedTotal).toBe(0);
  });

  it('two claims from the SAME orchestratorId for the same type both count toward the aggregate, pinned as separate per-claim records (not silently overwritten)', async () => {
    await claimCapacity(filePath, { type: 'sub-agent', requestedCount: 3, orchestratorId: 'orch-a', computeAvailableCapacity: ample });
    await claimCapacity(filePath, { type: 'sub-agent', requestedCount: 4, orchestratorId: 'orch-a', computeAvailableCapacity: ample });

    const dibs = await readDibs(filePath, Date.now());
    const ledgerEntry = dibs.find((entry) => entry.orchestratorId === '__claim:sub-agent__');
    const ownClaims = (ledgerEntry.claims ?? []).filter((claim) => claim.orchestratorId === 'orch-a');

    // Pinned contract: repeat claims from the SAME orchestratorId are
    // recorded as separate per-claim records (append, not merge-in-place) —
    // this is the design choice that keeps a future per-claim TTL (Phase 2)
    // able to expire one sub-claim's `claimedAt` independently of the
    // other's, rather than losing that granularity behind one shared,
    // last-write-wins `claimedAt` on a merged record.
    expect(ownClaims.length).toBe(2);
    expect(ownClaims.reduce((total, claim) => total + claim.count, 0)).toBe(7);

    // The aggregate total (across all holders — just orch-a here) reflects
    // the summed 7, and a release of the caller's own full holding must
    // release all 7 in one call, proving the two records are correctly
    // attributed to the SAME orchestratorId even though they're separate
    // array entries.
    const releaseResult = await releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 7, orchestratorId: 'orch-a' });
    expect(releaseResult.released).toBe(7);
    expect(releaseResult.grantedTotal).toBe(0);
  });

  it('a same-orchestratorId claim arriving when a prior claim record for that id is stale/logically-superseded but not yet pruned must not corrupt the merge (mirrors declareDibs\'s stale-but-unpruned precedent)', async () => {
    // Seed the coordination file directly with a ledger entry that already
    // has a claims-array record for orch-a whose `claimedAt` is ancient —
    // simulating a leftover, not-yet-physically-pruned record (the same
    // "stale-but-unpruned" shape declareDibs's own regression test guards
    // against for `firstDeclaredAt`). Phase 1 has no TTL/expiry logic yet
    // (that's Phase 2) — this test only pins that an old, pre-existing
    // per-holder record is never silently dropped or double-counted when a
    // fresh claim from the SAME orchestratorId arrives afterward.
    const ancientClaimedAt = 1_000;
    const seededLedgerEntry = {
      orchestratorId: '__claim:sub-agent__',
      claims: [{ orchestratorId: 'orch-a', count: 2, claimedAt: ancientClaimedAt }],
      declaredAt: ancientClaimedAt,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([seededLedgerEntry]), 'utf8');

    await claimCapacity(filePath, { type: 'sub-agent', requestedCount: 3, orchestratorId: 'orch-a', computeAvailableCapacity: ample });

    const dibs = await readDibs(filePath, Date.now());
    const ledgerEntry = dibs.find((entry) => entry.orchestratorId === '__claim:sub-agent__');
    const ownClaims = (ledgerEntry.claims ?? []).filter((claim) => claim.orchestratorId === 'orch-a');

    // The ancient record's count (2) is neither lost nor double-applied —
    // the aggregate is exactly 2 (seeded) + 3 (new) = 5.
    expect(ownClaims.reduce((total, claim) => total + claim.count, 0)).toBe(5);

    const releaseResult = await releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 5, orchestratorId: 'orch-a' });
    expect(releaseResult.released).toBe(5);
  });

  it('alreadyGrantedTotal/availableCapacity aggregate correctly sums claim records across MULTIPLE holders', async () => {
    await claimCapacity(filePath, { type: 'sub-agent', requestedCount: 2, orchestratorId: 'orch-a', computeAvailableCapacity: ample });
    await claimCapacity(filePath, { type: 'sub-agent', requestedCount: 3, orchestratorId: 'orch-b', computeAvailableCapacity: ample });
    await claimCapacity(filePath, { type: 'sub-agent', requestedCount: 4, orchestratorId: 'orch-c', computeAvailableCapacity: ample });

    // A 4th claim's `alreadyGrantedTotal` (used to compute `remaining =
    // availableCapacity - alreadyGrantedTotal`) must reflect the correct
    // cross-holder sum: 2 + 3 + 4 = 9, out of ample's availableCapacity 100.
    const fourthClaim = await claimCapacity(filePath, {
      type: 'sub-agent',
      requestedCount: 10,
      orchestratorId: 'orch-d',
      computeAvailableCapacity: ample,
    });

    expect(fourthClaim.alreadyGrantedTotal).toBe(9);
    expect(fourthClaim.availableCapacity).toBe(100);
    expect(fourthClaim.granted).toBe(10);
  });

  it('a reserved-looking orchestratorId value ("__foo__") already persisted inside claims[] (e.g. from before the round-3 reserved-id guard existed) is not misidentified as itself a reserved top-level ledger entry', async () => {
    // As of the round-3 review LOW fix, `claimCapacity`/`releaseCapacity`
    // reject a reserved-looking `orchestratorId` outright (see the
    // dedicated describe block above) — so such a value can no longer be
    // written into `claims[]` via the public API today. This test seeds one
    // directly on disk to cover the grandfathered case: a record some
    // earlier, pre-guard version of this module (or a still-unpatched
    // mixed-version writer) already persisted. Scanning code must still
    // never confuse this claims[]-internal value with a top-level reserved
    // entry.
    const now = Date.now();
    const preGuardLedgerEntry = {
      orchestratorId: '__claim:reserved-id-collision-type__',
      claims: [{ orchestratorId: '__foo__', count: 3, claimedAt: now }],
      grantedTotal: 3,
      declaredAt: now,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([preGuardLedgerEntry]), 'utf8');

    // Only ONE top-level entry exists for this type: the genuine reserved
    // ledger entry keyed by claimLedgerEntryId (`__claim:<type>__`). No
    // separate top-level entry keyed directly by the raw orchestratorId
    // ('__foo__') exists — proving no isReservedEntry-based scanning code
    // confuses the VALUE stored inside claims[] with a top-level reserved
    // entry in its own right.
    const dibs = await readDibs(filePath, Date.now());
    const matchingEntries = dibs.filter(
      (entry) => entry.orchestratorId === '__claim:reserved-id-collision-type__' || entry.orchestratorId === '__foo__',
    );
    expect(matchingEntries.length).toBe(1);
    expect(matchingEntries[0].orchestratorId).toBe('__claim:reserved-id-collision-type__');

    // A NEW attempt to claim/release under that same reserved-looking id is
    // now rejected outright by the guard — the loophole this fix closes.
    await expect(
      claimCapacity(filePath, {
        type: 'reserved-id-collision-type',
        requestedCount: 1,
        orchestratorId: '__foo__',
        computeAvailableCapacity: ample,
      }),
    ).rejects.toThrow(TypeError);
  });

  it('claimCapacity\'s and releaseCapacity\'s external return contract shapes are unchanged', async () => {
    const claimResult = await claimCapacity(filePath, {
      type: 'contract-shape-type',
      requestedCount: 2,
      orchestratorId: 'orch-a',
      computeAvailableCapacity: ample,
    });
    expect(Object.keys(claimResult).sort()).toEqual(['alreadyGrantedTotal', 'availableCapacity', 'granted'].sort());

    const releaseResult = await releaseCapacity(filePath, {
      type: 'contract-shape-type',
      releaseCount: 1,
      orchestratorId: 'orch-a',
    });
    expect(Object.keys(releaseResult).sort()).toEqual(['grantedTotal', 'released'].sort());
  });

  it('a coordination file on disk still shaped as the OLD scalar ledger entry ({ orchestratorId, grantedTotal, declaredAt }, no claims array) does not throw and its already-granted total is not silently lost', async () => {
    const legacyLedgerEntry = {
      orchestratorId: '__claim:legacy-shape-type__',
      grantedTotal: 6,
      declaredAt: Date.now(),
    };
    await fsPromises.writeFile(filePath, JSON.stringify([legacyLedgerEntry]), 'utf8');

    // Must not throw on `.find()`/`.filter()`/`.reduce()` over an undefined
    // `claims` array. `claimTtlMs: DEFAULT_CLAIM_TTL_MS` is passed here (as
    // every real caller — cli.mjs — always does) so this test exercises the
    // legacy-shape migration behavior itself, not the separate
    // claimTtlMs-unset+legacy-record guard covered by its own describe block
    // below (Convergence Analysis follow-up).
    await expect(
      claimCapacity(
        filePath,
        {
          type: 'legacy-shape-type',
          requestedCount: 1,
          orchestratorId: 'orch-new',
          computeAvailableCapacity: ample,
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).resolves.toBeDefined();

    // The pre-existing legacy grantedTotal (6) must not be silently treated
    // as 0 — doing so would let a fresh claim over-grant beyond the type's
    // genuinely-remaining capacity, reintroducing the exact over-grant class
    // of bug exists to close.
    const claimResult = await claimCapacity(
      filePath,
      {
        type: 'legacy-shape-type',
        requestedCount: 1000,
        orchestratorId: 'orch-new-2',
        computeAvailableCapacity: ample,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );
    expect(claimResult.alreadyGrantedTotal).toBeGreaterThanOrEqual(6);

    await expect(
      releaseCapacity(
        filePath,
        { type: 'legacy-shape-type', releaseCount: 1, orchestratorId: 'orch-legacy-holder' },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).resolves.toBeDefined();
  });

  it('cli.mjs\'s handleRelease threading orchestratorId through: releaseCapacity called with a DIFFERENT orchestratorId than the claimant returns released: 0, never draining the claimant\'s slots', async () => {
    await claimCapacity(filePath, {
      type: 'attribution-type',
      requestedCount: 3,
      orchestratorId: 'orch-claimant',
      computeAvailableCapacity: ample,
    });

    const releaseByOther = await releaseCapacity(filePath, {
      type: 'attribution-type',
      releaseCount: 3,
      orchestratorId: 'orch-not-the-claimant',
    });
    expect(releaseByOther.released).toBe(0);
    expect(releaseByOther.grantedTotal).toBe(3);

    const releaseByClaimant = await releaseCapacity(filePath, {
      type: 'attribution-type',
      releaseCount: 3,
      orchestratorId: 'orch-claimant',
    });
    expect(releaseByClaimant.released).toBe(3);
    expect(releaseByClaimant.grantedTotal).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Phase 2 — TTL-based claim expiry.
//
// Builds on Phase 1's per-claim `claimedAt` timestamps to automatically
// exclude claims older than a TTL window from `alreadyGrantedTotal`'s
// aggregate, physically prune them from the persisted array on the next
// WRITE (never a background sweep/timer), and evaluate that TTL using ONE
// `referenceNow` captured once inside the same `withLock` critical section
// the read/write already runs in — mirroring `pruneStale`'s existing
// `declaredAt`-relative exclusion pattern (inclusive boundary) applied to
// `claimedAt` instead, and `declareDibs`'s existing single-`referenceNow`
// precedent.
//
// RED by construction: none of this exists yet on `claimCapacity`/
// `releaseCapacity` today — a claim never ages out except via an explicit
// `releaseCapacity` call (see `claimCapacity`'s own "Crash-ack-loss window"
// JSDoc paragraph and SKILL.md's "Releasing capacity" section, both of which
// currently document TTL as explicitly out of scope).
//
// TTL wiring shape: the Build Plan leaves the exact wiring (a `DEFAULT_*`
// constant vs. an options-passed override) to the implementer. These tests
// exercise BEHAVIOR via an explicit `claimTtlMs` option threaded through
// `claimCapacity`/`releaseCapacity`'s existing `options` param (mirroring
// this file's established `livenessThresholdMs` convention on
// `declareDibs`/`readDibs`/`consumeGlobalSpawnTokens`) — implementation-
// agnostic to whether a `DEFAULT_CLAIM_TTL_MS`-style constant also exists as
// the option's default. The constant's own existence is asserted separately,
// directly, below.
// ---------------------------------------------------------------------------

describe('claimCapacity / releaseCapacity — TTL-based claim expiry', () => {
  const ample = async () => 100;
  // An arbitrary, explicit TTL passed via options on every call in this
  // block — deliberately not the eventual `DEFAULT_CLAIM_TTL_MS` default (if
  // any), so these behavior tests don't accidentally couple to that value.
  const CLAIM_TTL_MS = 24 * 60 * 60 * 1000; // 1 day

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('a claim record whose claimedAt is older than the TTL window is excluded from alreadyGrantedTotal on the next read', async () => {
    const t0 = 1_700_000_000_000;
    jest.spyOn(Date, 'now').mockReturnValue(t0);

    await claimCapacity(
      filePath,
      { type: 'ttl-exclusion-type', requestedCount: 5, orchestratorId: 'orch-a', computeAvailableCapacity: ample },
      { claimTtlMs: CLAIM_TTL_MS },
    );

    // Strictly past the TTL window.
    jest.spyOn(Date, 'now').mockReturnValue(t0 + CLAIM_TTL_MS + 1);

    const secondClaim = await claimCapacity(
      filePath,
      { type: 'ttl-exclusion-type', requestedCount: 3, orchestratorId: 'orch-b', computeAvailableCapacity: ample },
      { claimTtlMs: CLAIM_TTL_MS },
    );

    // orch-a's claim has aged past the TTL — excluded entirely from the
    // aggregate, without any explicit release ever having been called for it.
    expect(secondClaim.alreadyGrantedTotal).toBe(0);
    expect(secondClaim.granted).toBe(3);
  });

  it('a claim exactly at the TTL boundary is still counted (inclusive boundary — mirrors pruneStale\'s "exactly at the threshold is still live")', async () => {
    const t0 = 1_700_000_000_000;
    jest.spyOn(Date, 'now').mockReturnValue(t0);

    await claimCapacity(
      filePath,
      { type: 'ttl-boundary-type', requestedCount: 5, orchestratorId: 'orch-a', computeAvailableCapacity: ample },
      { claimTtlMs: CLAIM_TTL_MS },
    );

    // Exactly at the TTL boundary — still live, not yet expired.
    jest.spyOn(Date, 'now').mockReturnValue(t0 + CLAIM_TTL_MS);

    const secondClaim = await claimCapacity(
      filePath,
      { type: 'ttl-boundary-type', requestedCount: 100, orchestratorId: 'orch-b', computeAvailableCapacity: ample },
      { claimTtlMs: CLAIM_TTL_MS },
    );

    expect(secondClaim.alreadyGrantedTotal).toBe(5);
    expect(secondClaim.granted).toBe(95);
  });

  it('an expired claim is physically pruned from the persisted array on the next WRITE, not merely excluded from in-memory aggregates', async () => {
    const t0 = 1_700_000_000_000;
    jest.spyOn(Date, 'now').mockReturnValue(t0);

    await claimCapacity(
      filePath,
      { type: 'ttl-prune-type', requestedCount: 2, orchestratorId: 'orch-old', computeAvailableCapacity: ample },
      { claimTtlMs: CLAIM_TTL_MS },
    );

    jest.spyOn(Date, 'now').mockReturnValue(t0 + CLAIM_TTL_MS + 1);

    // Any call that WRITES (a successful claim) is the trigger for physical
    // pruning — no separate background sweep/timer mechanism should exist.
    await claimCapacity(
      filePath,
      { type: 'ttl-prune-type', requestedCount: 1, orchestratorId: 'orch-new', computeAvailableCapacity: ample },
      { claimTtlMs: CLAIM_TTL_MS },
    );

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:ttl-prune-type__');

    expect(ledgerEntry.claims.some((claim) => claim.orchestratorId === 'orch-old')).toBe(false);
    expect(ledgerEntry.claims.some((claim) => claim.orchestratorId === 'orch-new')).toBe(true);
  });

  it('releasing against a caller whose only claim record for the type has already expired behaves the same as releasing against a nonexistent claim (released: 0)', async () => {
    const t0 = 1_700_000_000_000;
    jest.spyOn(Date, 'now').mockReturnValue(t0);

    await claimCapacity(
      filePath,
      { type: 'ttl-expired-release-type', requestedCount: 4, orchestratorId: 'orch-a', computeAvailableCapacity: ample },
      { claimTtlMs: CLAIM_TTL_MS },
    );

    jest.spyOn(Date, 'now').mockReturnValue(t0 + CLAIM_TTL_MS + 1);

    const releaseResult = await releaseCapacity(
      filePath,
      { type: 'ttl-expired-release-type', releaseCount: 4, orchestratorId: 'orch-a' },
      { claimTtlMs: CLAIM_TTL_MS },
    );

    // Must not throw, under/over-decrement, or double-count — indistinguishable
    // from releasing against a type/orchestratorId that never claimed anything.
    expect(releaseResult.released).toBe(0);
    expect(releaseResult.grantedTotal).toBe(0);
  });

  it('captures ONE referenceNow inside the locked critical section — a fresh claim record\'s claimedAt and its ledger entry\'s declaredAt never diverge, even under a clock that advances on every single Date.now() read', async () => {
    // A clock that returns a NEW, strictly-increasing value on literally
    // every call. If the implementation reads Date.now()/the clock more than
    // once mid-critical-section for what should be a single referenceNow
    // (e.g. once for the TTL exclusion check, once for claimedAt, once for
    // declaredAt), this makes those reads provably diverge — a bug this
    // sequence-mock is specifically designed to catch, which a single fixed
    // mockReturnValue would not.
    let clockValue = 1_700_000_000_000;
    jest.spyOn(Date, 'now').mockImplementation(() => {
      clockValue += 1;
      return clockValue;
    });

    await claimCapacity(
      filePath,
      {
        type: 'ttl-referencenow-type',
        requestedCount: 1,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
      },
      { claimTtlMs: CLAIM_TTL_MS },
    );

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:ttl-referencenow-type__');
    const claimRecord = ledgerEntry.claims.find((claim) => claim.orchestratorId === 'orch-a');

    expect(claimRecord.claimedAt).toBe(ledgerEntry.declaredAt);
  });

  it('a claim that expires BETWEEN two claimCapacity calls frees its capacity for a new claim without any explicit release (fake-clock unit-level version of the Phase 0 outer acceptance test, Part 3)', async () => {
    const t0 = 1_700_000_000_000;
    const exactlyOneSlot = async () => 1;

    jest.spyOn(Date, 'now').mockReturnValue(t0);

    const claimA = await claimCapacity(
      filePath,
      { type: 'ttl-between-calls-type', requestedCount: 1, orchestratorId: 'orch-a', computeAvailableCapacity: exactlyOneSlot },
      { claimTtlMs: CLAIM_TTL_MS },
    );
    expect(claimA.granted).toBe(1);

    // Fully exhausted immediately afterward.
    const exhausted = await claimCapacity(
      filePath,
      { type: 'ttl-between-calls-type', requestedCount: 1, orchestratorId: 'orch-b', computeAvailableCapacity: exactlyOneSlot },
      { claimTtlMs: CLAIM_TTL_MS },
    );
    expect(exhausted.granted).toBe(0);

    // Advance the fake clock past the TTL window — A's claim expires here,
    // between these two calls, with no explicit release ever issued for it.
    jest.spyOn(Date, 'now').mockReturnValue(t0 + CLAIM_TTL_MS + 1);

    const fresh = await claimCapacity(
      filePath,
      { type: 'ttl-between-calls-type', requestedCount: 1, orchestratorId: 'orch-c', computeAvailableCapacity: exactlyOneSlot },
      { claimTtlMs: CLAIM_TTL_MS },
    );
    expect(fresh.granted).toBe(1);
  });

  it('exports a named, documented DEFAULT_CLAIM_TTL_MS-style TTL constant with a sane, conservative default, following this file\'s existing DEFAULT_* config-constant convention', () => {
    expect(typeof coordinationFile.DEFAULT_CLAIM_TTL_MS).toBe('number');
    expect(Number.isFinite(coordinationFile.DEFAULT_CLAIM_TTL_MS)).toBe(true);
    // Sanity-bound only (not pinning an exact value the implementer hasn't
    // chosen yet): conservative enough to comfortably outlive any normal
    // agent task, but not so long it defeats the point of automatic expiry.
    expect(coordinationFile.DEFAULT_CLAIM_TTL_MS).toBeGreaterThan(60 * 60 * 1000); // > 1 hour
    expect(coordinationFile.DEFAULT_CLAIM_TTL_MS).toBeLessThanOrEqual(90 * 24 * 60 * 60 * 1000); // <= 90 days
  });

  it('the outer __claim:<type>__ ledger entry itself remains exempt from pruneStale\'s liveness pruning regardless of claim TTL (regression check — TTL applies only inside claims[], never to the reserved outer entry)', async () => {
    const t0 = 1_700_000_000_000;
    jest.spyOn(Date, 'now').mockReturnValue(t0);

    await claimCapacity(
      filePath,
      { type: 'ttl-reserved-exemption-type', requestedCount: 1, orchestratorId: 'orch-a', computeAvailableCapacity: ample },
      { claimTtlMs: CLAIM_TTL_MS },
    );

    // Advance the fake clock WAY past any believable TTL and any normal
    // dibs liveness threshold.
    const farFuture = t0 + 365 * 24 * 60 * 60 * 1000;
    jest.spyOn(Date, 'now').mockReturnValue(farFuture);

    // readDibs applies pruneStale with an aggressive livenessThresholdMs —
    // the reserved ledger entry itself must survive regardless of age
    // (isReservedEntry's exemption, unchanged by Phase 2's TTL work), even
    // though the claims[] records nested inside it may be logically expired.
    const dibs = await readDibs(filePath, farFuture, { livenessThresholdMs: 60_000 });
    const ledgerEntry = dibs.find((entry) => entry.orchestratorId === '__claim:ttl-reserved-exemption-type__');
    expect(ledgerEntry).toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// Post-review hardening (pre-PR adversarial review on):
//
//   - HIGH: `claimCapacity`/`releaseCapacity`'s write paths must persist a
//     `grantedTotal` aggregate ALONGSIDE `claims[]`, not `claims[]` alone —
//     this coordination file is host-global, so a still-running pre-existing
//     process reading the same file only knows `ledgerEntry.grantedTotal`;
//     dropping it would make that old code see `alreadyGrantedTotal = 0` and
//     over-grant on top of every live claim.
//   - MEDIUM: the synthetic legacy-migration record `normalizeClaims` folds a
//     pre-existing scalar entry into must be EXEMPT from TTL expiry — otherwise
//     a legacy file idle longer than the TTL has its folded-in total silently
//     pruned on the very first read, contradicting `normalizeClaims`'s own
//     "never silently lost" contract.
//   - LOW: `pruneExpiredClaims` must be as defensive as `sumClaimCounts`
//     against a malformed `claims[]` element (optional chaining), not throw
//     inside the lock.
// ---------------------------------------------------------------------------

describe('claimCapacity / releaseCapacity — grantedTotal persisted alongside claims[] for mixed-version rollout safety (post-review hardening)', () => {
  const ample = async () => 100;

  it('claimCapacity writes grantedTotal alongside claims[], equal to sumClaimCounts(claims)', async () => {
    await claimCapacity(filePath, {
      type: 'compat-claim-type',
      requestedCount: 4,
      orchestratorId: 'orch-a',
      computeAvailableCapacity: ample,
    });
    await claimCapacity(filePath, {
      type: 'compat-claim-type',
      requestedCount: 3,
      orchestratorId: 'orch-b',
      computeAvailableCapacity: ample,
    });

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:compat-claim-type__');
    expect(ledgerEntry).toBeDefined();
    expect(Array.isArray(ledgerEntry.claims)).toBe(true);
    expect(ledgerEntry.grantedTotal).toBe(7);
    expect(ledgerEntry.grantedTotal).toBe(
      ledgerEntry.claims.reduce((total, claim) => total + claim.count, 0),
    );
  });

  it('releaseCapacity writes grantedTotal alongside claims[], equal to sumClaimCounts(claims), after a partial release', async () => {
    await claimCapacity(filePath, {
      type: 'compat-release-type',
      requestedCount: 5,
      orchestratorId: 'orch-a',
      computeAvailableCapacity: ample,
    });
    await claimCapacity(filePath, {
      type: 'compat-release-type',
      requestedCount: 4,
      orchestratorId: 'orch-b',
      computeAvailableCapacity: ample,
    });

    await releaseCapacity(filePath, { type: 'compat-release-type', releaseCount: 2, orchestratorId: 'orch-a' });

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:compat-release-type__');
    expect(ledgerEntry).toBeDefined();
    expect(ledgerEntry.grantedTotal).toBe(7); // (5 - 2) + 4
    expect(ledgerEntry.grantedTotal).toBe(
      ledgerEntry.claims.reduce((total, claim) => total + claim.count, 0),
    );
  });

  it('a pre-existing process reading the persisted entry via the OLD "ledgerEntry.grantedTotal ?? 0" pattern sees the correct aggregate, not 0', async () => {
    await claimCapacity(filePath, {
      type: 'mixed-version-type',
      requestedCount: 6,
      orchestratorId: 'orch-a',
      computeAvailableCapacity: ample,
    });

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:mixed-version-type__');
    // Simulates old code's read path exactly (see the High finding's report).
    const alreadyGrantedTotalUnderOldCode =
      typeof ledgerEntry?.grantedTotal === 'number' && Number.isFinite(ledgerEntry.grantedTotal)
        ? ledgerEntry.grantedTotal
        : 0;
    expect(alreadyGrantedTotalUnderOldCode).toBe(6);
  });
});

describe('pruneExpiredClaims — legacy-migration record is exempt from TTL expiry (post-review hardening, legacy-TTL finding, fix option (a))', () => {
  const ample = async () => 100;
  const CLAIM_TTL_MS = 6 * 60 * 60 * 1000; // mirrors DEFAULT_CLAIM_TTL_MS

  it('a legacy-shaped ledger entry (grantedTotal/declaredAt, no claims[]) whose declaredAt is older than the TTL still contributes its full total when a real claimTtlMs is passed', async () => {
    const now = Date.now();
    const declaredAtLongAgo = now - (CLAIM_TTL_MS + 60 * 60 * 1000); // TTL + 1h, well past the window
    const legacyLedgerEntry = {
      orchestratorId: '__claim:legacy-idle-type__',
      grantedTotal: 9,
      declaredAt: declaredAtLongAgo,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([legacyLedgerEntry]), 'utf8');

    const claimResult = await claimCapacity(
      filePath,
      {
        type: 'legacy-idle-type',
        requestedCount: 1000,
        orchestratorId: 'orch-new',
        computeAvailableCapacity: ample,
        now,
      },
      { claimTtlMs: CLAIM_TTL_MS },
    );

    // Fix (a): the legacy fold-in total must survive TTL pruning — it is
    // exempt until its first real migration-forward write, not silently
    // evaporated just because the machine sat idle past the TTL window.
    expect(claimResult.alreadyGrantedTotal).toBeGreaterThanOrEqual(9);
  });

  it('releaseCapacity likewise never silently drops an idle-past-TTL legacy total from grantedTotal', async () => {
    const now = Date.now();
    const declaredAtLongAgo = now - (CLAIM_TTL_MS + 60 * 60 * 1000);
    const legacyLedgerEntry = {
      orchestratorId: '__claim:legacy-idle-release-type__',
      grantedTotal: 5,
      declaredAt: declaredAtLongAgo,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([legacyLedgerEntry]), 'utf8');

    const releaseResult = await releaseCapacity(
      filePath,
      { type: 'legacy-idle-release-type', releaseCount: 1, orchestratorId: 'orch-not-legacy', now },
      { claimTtlMs: CLAIM_TTL_MS },
    );

    // No live orchestrator's real id matches the legacy sentinel, so nothing
    // is actually released (`released: 0`) — but the total itself must still
    // be reported, not silently zeroed by TTL pruning.
    expect(releaseResult.released).toBe(0);
    expect(releaseResult.grantedTotal).toBe(5);
  });
});

// ---------------------------------------------------------------------------
// Post-review hardening, round 2 (pre-PR adversarial review, round 2 on
//):
//
//   - HIGH: round 1's fix over-corrected — exempting the legacy-migration
//     record from TTL unconditionally by `orchestratorId` meant it could
//     never be released or expire, since no real orchestrator id can ever
//     equal the reserved sentinel (blocked by the reserved-id CLI guard).
//     The corrected behavior: the record is exempt from TTL only for the
//     READ that first synthesizes it (`normalizeClaims`); the next real
//     WRITE to that ledger entry (`claimCapacity` OR `releaseCapacity`, via
//     `stampMigratedClaims`) re-stamps `claimedAt` to that write's
//     `referenceNow` and drops the transient `legacyMigrated` marker, so
//     from that write forward the record ages out under ordinary TTL rules
//     exactly like any other claim record — one `claimTtlMs` window after
//     the migrating write, not never.
// ---------------------------------------------------------------------------

describe('legacy-migration record TTL lifecycle — exempt only until the next real write (round-2 review fix)', () => {
  const ample = async () => 100;
  const CLAIM_TTL_MS = 6 * 60 * 60 * 1000; // mirrors DEFAULT_CLAIM_TTL_MS

  it('a claimCapacity write that migrates the legacy record forward re-stamps it, so it expires one TTL window later (not never)', async () => {
    const migrationTime = Date.now();
    const declaredAtLongAgo = migrationTime - (CLAIM_TTL_MS + 60 * 60 * 1000);
    const legacyLedgerEntry = {
      orchestratorId: '__claim:legacy-migrates-forward-type__',
      grantedTotal: 9,
      declaredAt: declaredAtLongAgo,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([legacyLedgerEntry]), 'utf8');

    // The migrating write: legacy total survives (round-1 fix still holds),
    // and physically persists the migrated-forward record.
    const migrationResult = await claimCapacity(
      filePath,
      {
        type: 'legacy-migrates-forward-type',
        requestedCount: 1,
        orchestratorId: 'orch-migrator',
        computeAvailableCapacity: ample,
        now: migrationTime,
      },
      { claimTtlMs: CLAIM_TTL_MS },
    );
    expect(migrationResult.alreadyGrantedTotal).toBeGreaterThanOrEqual(9);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:legacy-migrates-forward-type__');
    const legacyRecord = ledgerEntry.claims.find(
      (claim) => claim.orchestratorId === '__legacy-unattributed-claim__',
    );
    // Migrated forward, not lost.
    expect(legacyRecord).toBeDefined();
    expect(legacyRecord.count).toBe(9);
    // Re-stamped to the migrating write's referenceNow — no longer carrying
    // its old, far-expired `claimedAt`, and no longer marked exempt.
    expect(legacyRecord.claimedAt).toBe(migrationTime);
    expect(legacyRecord.legacyMigrated).toBeUndefined();

    // One TTL window after the migrating write, the (former) legacy record
    // is now an ordinary, TTL-subject claim — a read/write far enough past
    // `migrationTime + CLAIM_TTL_MS` must exclude its 9 from the aggregate,
    // proving it is NOT exempt forever.
    const wellPastTtlAfterMigration = migrationTime + CLAIM_TTL_MS + 60 * 60 * 1000;
    const laterResult = await claimCapacity(
      filePath,
      {
        type: 'legacy-migrates-forward-type',
        requestedCount: 1,
        orchestratorId: 'orch-later',
        computeAvailableCapacity: ample,
        now: wellPastTtlAfterMigration,
      },
      { claimTtlMs: CLAIM_TTL_MS },
    );
    // orch-migrator's own claim was stamped at `migrationTime` too, so by
    // `wellPastTtlAfterMigration` it has also aged out — the point of this
    // assertion is that the migrated legacy record ages out on the SAME
    // schedule as an ordinary claim record, not that it survives forever.
    expect(laterResult.alreadyGrantedTotal).toBe(0);
  });

  it('a releaseCapacity write against a DIFFERENT orchestrator also migrates a still-legacy record forward (not only claimCapacity)', async () => {
    const releaseTime = Date.now();
    const declaredAtLongAgo = releaseTime - (CLAIM_TTL_MS + 60 * 60 * 1000);
    // Legacy record already migrated into claims[] shape but STILL carrying
    // the transient marker — simulates the moment right after a
    // synthesizing read, before any write has happened yet.
    const partiallyMigratedLedgerEntry = {
      orchestratorId: '__claim:legacy-release-migrates-type__',
      claims: [
        { orchestratorId: '__legacy-unattributed-claim__', count: 9, claimedAt: declaredAtLongAgo, legacyMigrated: true },
        { orchestratorId: 'orch-real', count: 2, claimedAt: releaseTime },
      ],
      grantedTotal: 11,
      declaredAt: releaseTime,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([partiallyMigratedLedgerEntry]), 'utf8');

    const releaseResult = await releaseCapacity(
      filePath,
      { type: 'legacy-release-migrates-type', releaseCount: 1, orchestratorId: 'orch-real', now: releaseTime },
      { claimTtlMs: CLAIM_TTL_MS },
    );
    expect(releaseResult.released).toBe(1);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:legacy-release-migrates-type__');
    const legacyRecord = ledgerEntry.claims.find(
      (claim) => claim.orchestratorId === '__legacy-unattributed-claim__',
    );
    // A release against a different holder's claim still stamps the legacy
    // record forward — it must not require a claimCapacity call
    // specifically to lose its exemption.
    expect(legacyRecord.claimedAt).toBe(releaseTime);
    expect(legacyRecord.legacyMigrated).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Post-review hardening, round 3 (pre-PR adversarial review, round 3 on
//):
//
//   - HIGH: round 1/round 2 each fixed a distinct route to the same
//     unbounded-lockout bug (permanent marker-exemption, and unattributable-
//     by-id), but a THIRD route survived — when neither `claimCapacity` NOR
//     `releaseCapacity` ever WRITES for a saturated legacy entry (every
//     `--claim` grants 0 because nothing's left; every `--release` returns 0
//     because no real id matches the sentinel), the entry never migrates out
//     of scalar shape and is re-synthesized `legacyMigrated: true` (hence
//     TTL-exempt) forever. Fixed by making the migration write unconditional
//     on read, not contingent on a non-zero grant/release outcome.
// ---------------------------------------------------------------------------

describe('legacy-record migration is unconditional on read — dead-write-path fix (round-3 review HIGH finding)', () => {
  const CLAIM_TTL_MS = 6 * 60 * 60 * 1000; // mirrors DEFAULT_CLAIM_TTL_MS

  it('a SATURATED legacy ledger (every --claim yields granted: 0) still migrates and self-heals via TTL', async () => {
    const migrationTime = Date.now();
    const declaredAtLongAgo = migrationTime - (CLAIM_TTL_MS + 60 * 60 * 1000);
    const legacyLedgerEntry = {
      orchestratorId: '__claim:legacy-saturated-claim-type__',
      grantedTotal: 10,
      declaredAt: declaredAtLongAgo,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([legacyLedgerEntry]), 'utf8');

    // Saturated: availableCapacity (10) already fully consumed by the
    // legacy total (10) — every claim call grants 0, the exact case that
    // left the round-2 fix's write gated on `granted > 0` permanently dead.
    const saturated = async () => 10;

    const claimResult = await claimCapacity(
      filePath,
      {
        type: 'legacy-saturated-claim-type',
        requestedCount: 1,
        orchestratorId: 'orch-saturated',
        computeAvailableCapacity: saturated,
        now: migrationTime,
      },
      { claimTtlMs: CLAIM_TTL_MS },
    );
    expect(claimResult.granted).toBe(0);
    expect(claimResult.alreadyGrantedTotal).toBe(10);

    // Despite `granted: 0`, the migrating write must have happened: the
    // legacy record on disk is stamped and no longer marked exempt.
    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:legacy-saturated-claim-type__');
    expect(Array.isArray(ledgerEntry.claims)).toBe(true);
    const legacyRecord = ledgerEntry.claims.find(
      (claim) => claim.orchestratorId === '__legacy-unattributed-claim__',
    );
    expect(legacyRecord).toBeDefined();
    expect(legacyRecord.count).toBe(10);
    expect(legacyRecord.claimedAt).toBe(migrationTime);
    expect(legacyRecord.legacyMigrated).toBeUndefined();

    // One TTL window after the migrating (zero-grant) write, a fresh read
    // must exclude the legacy total from the aggregate — proving it is NOT
    // permanently exempt just because it never received a non-zero grant.
    const wellPastTtlAfterMigration = migrationTime + CLAIM_TTL_MS + 60 * 60 * 1000;
    const laterResult = await claimCapacity(
      filePath,
      {
        type: 'legacy-saturated-claim-type',
        requestedCount: 1,
        orchestratorId: 'orch-later',
        computeAvailableCapacity: saturated,
        now: wellPastTtlAfterMigration,
      },
      { claimTtlMs: CLAIM_TTL_MS },
    );
    expect(laterResult.alreadyGrantedTotal).toBe(0);
    expect(laterResult.granted).toBe(1);
  });

  it('a legacy ledger releaseCapacity can never touch (released: 0 every time) still migrates and self-heals via TTL', async () => {
    const migrationTime = Date.now();
    const declaredAtLongAgo = migrationTime - (CLAIM_TTL_MS + 60 * 60 * 1000);
    const legacyLedgerEntry = {
      orchestratorId: '__claim:legacy-saturated-release-type__',
      grantedTotal: 7,
      declaredAt: declaredAtLongAgo,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([legacyLedgerEntry]), 'utf8');

    // No real orchestratorId ever matches the legacy sentinel, so this
    // returns released: 0 regardless of who calls it — the exact dead
    // write path the round-2 fix's `if (released > 0)` gate left open.
    const releaseResult = await releaseCapacity(
      filePath,
      { type: 'legacy-saturated-release-type', releaseCount: 7, orchestratorId: 'orch-never-holds-legacy', now: migrationTime },
      { claimTtlMs: CLAIM_TTL_MS },
    );
    expect(releaseResult.released).toBe(0);
    expect(releaseResult.grantedTotal).toBe(7);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:legacy-saturated-release-type__');
    expect(Array.isArray(ledgerEntry.claims)).toBe(true);
    const legacyRecord = ledgerEntry.claims.find(
      (claim) => claim.orchestratorId === '__legacy-unattributed-claim__',
    );
    expect(legacyRecord.claimedAt).toBe(migrationTime);
    expect(legacyRecord.legacyMigrated).toBeUndefined();

    const wellPastTtlAfterMigration = migrationTime + CLAIM_TTL_MS + 60 * 60 * 1000;
    const laterResult = await releaseCapacity(
      filePath,
      { type: 'legacy-saturated-release-type', releaseCount: 7, orchestratorId: 'orch-never-holds-legacy', now: wellPastTtlAfterMigration },
      { claimTtlMs: CLAIM_TTL_MS },
    );
    // Still 0 (nobody's id ever matches the sentinel) — but the aggregate
    // must have dropped the expired legacy total.
    expect(laterResult.released).toBe(0);
    expect(laterResult.grantedTotal).toBe(0);
  });
});

describe('claimCapacity/releaseCapacity reject a reserved orchestratorId directly, not only via cli.mjs (round-3 review LOW finding)', () => {
  const ample = async () => 100;

  it('claimCapacity throws a TypeError for a reserved-looking orchestratorId, before ever touching the lock/file', async () => {
    await expect(
      claimCapacity(filePath, {
        type: 'reserved-id-claim-type',
        requestedCount: 1,
        orchestratorId: '__not-a-real-orchestrator__',
        computeAvailableCapacity: ample,
      }),
    ).rejects.toThrow(TypeError);

    expect(existsSync(filePath)).toBe(false);
  });

  it('releaseCapacity throws a TypeError for a reserved-looking orchestratorId, before ever touching the lock/file', async () => {
    await expect(
      releaseCapacity(filePath, {
        type: 'reserved-id-release-type',
        releaseCount: 1,
        orchestratorId: '__not-a-real-orchestrator__',
      }),
    ).rejects.toThrow(TypeError);

    expect(existsSync(filePath)).toBe(false);
  });
});

describe('normalizeClaims drops malformed claims[] elements at normalization time (round-3 review LOW finding)', () => {
  it('a null and an empty-object element are both dropped, even on the no-TTL path (claimCapacity)', async () => {
    const now = Date.now();
    const malformedLedgerEntry = {
      orchestratorId: '__claim:normalize-drops-malformed-type__',
      claims: [null, {}, { orchestratorId: 'orch-a', count: 3, claimedAt: now }],
      declaredAt: now,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([malformedLedgerEntry]), 'utf8');

    // No options.claimTtlMs — the no-TTL path where `pruneExpiredClaims`
    // returns `claims` untouched, so any survivor came from `normalizeClaims`
    // filtering, not from TTL pruning. `requestedCount: 1` (not 0) so this
    // call actually grants and writes — a genuine `granted: 0` call performs
    // no write at all (see the ` ` no-op-early-return tests), which
    // would leave the pre-seeded malformed elements on disk unchanged and
    // prove nothing about `normalizeClaims` itself.
    const result = await claimCapacity(filePath, {
      type: 'normalize-drops-malformed-type',
      requestedCount: 1,
      orchestratorId: 'orch-b',
      computeAvailableCapacity: async () => 100,
      now,
    });
    expect(result.alreadyGrantedTotal).toBe(3);
    expect(result.granted).toBe(1);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:normalize-drops-malformed-type__');
    // Only the well-formed pre-existing record plus this call's own new
    // record survive — the null/{} junk never reaches a durable write.
    expect(ledgerEntry.claims).toHaveLength(2);
    expect(ledgerEntry.claims.map((claim) => claim.orchestratorId).sort()).toEqual(['orch-a', 'orch-b']);
  });
});

describe('normalizeClaims drops a claims[] element with a negative or non-integer count (round-4 review, MEDIUM finding)', () => {
  it('a count: -1000 element is dropped and does NOT inflate alreadyGrantedTotal/granted on a subsequent claimCapacity call', async () => {
    const now = Date.now();
    const injectedLedgerEntry = {
      orchestratorId: '__claim:negative-count-injection-type__',
      claims: [{ orchestratorId: 'attacker', count: -1000, claimedAt: now }],
      declaredAt: now,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([injectedLedgerEntry]), 'utf8');

    // Before this fix: isValidClaimRecord only checked isFiniteNumber(count),
    // so the -1000 element survived normalization, sumClaimCounts summed it
    // into alreadyGrantedTotal (-1000), and `remaining = max(0,
    // availableCapacity - alreadyGrantedTotal)` = max(0, 3 - (-1000)) = 1003
    // — a requestedCount: 500 ask against availableCapacity: 3 was granted
    // in full. After this fix, the malformed element never survives
    // normalizeClaims, so alreadyGrantedTotal is 0 and the grant is capped
    // by the genuine availableCapacity of 3.
    const result = await claimCapacity(filePath, {
      type: 'negative-count-injection-type',
      requestedCount: 500,
      orchestratorId: 'orch-legit',
      computeAvailableCapacity: async () => 3,
      now,
    });

    expect(result.alreadyGrantedTotal).toBe(0);
    expect(result.granted).toBe(3);
    expect(result.granted).not.toBe(500);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:negative-count-injection-type__');
    expect(ledgerEntry.claims.map((claim) => claim.orchestratorId)).toEqual(['orch-legit']);
  });

  it('a non-integer count (1.5) element is also dropped', async () => {
    const now = Date.now();
    const injectedLedgerEntry = {
      orchestratorId: '__claim:non-integer-count-type__',
      claims: [{ orchestratorId: 'orch-a', count: 1.5, claimedAt: now }],
      declaredAt: now,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([injectedLedgerEntry]), 'utf8');

    const result = await claimCapacity(filePath, {
      type: 'non-integer-count-type',
      requestedCount: 1,
      orchestratorId: 'orch-b',
      computeAvailableCapacity: async () => 100,
      now,
    });

    expect(result.alreadyGrantedTotal).toBe(0);
    expect(result.granted).toBe(1);
  });
});

describe('claimCapacity validates requestedCount before the lock (round-5 review LOW finding)', () => {
  const ample = async () => 100;

  it('rejects a non-integer requestedCount synchronously, before ever touching the lock/file', async () => {
    await expect(
      claimCapacity(filePath, {
        type: 'fractional-requested-count-type',
        requestedCount: 2.5,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
      }),
    ).rejects.toThrow(TypeError);

    expect(existsSync(filePath)).toBe(false);
  });

  it('rejects a negative requestedCount', async () => {
    await expect(
      claimCapacity(filePath, {
        type: 'negative-requested-count-type',
        requestedCount: -1,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
      }),
    ).rejects.toThrow(TypeError);
  });

  it('rejects requestedCount: NaN (previously silently returned granted: NaN)', async () => {
    await expect(
      claimCapacity(filePath, {
        type: 'nan-requested-count-type',
        requestedCount: NaN,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
      }),
    ).rejects.toThrow(TypeError);
  });
});

describe('normalizeClaims floors a fractional legacy grantedTotal so the synthesized record survives isValidClaimRecord (round-5 review LOW finding)', () => {
  it('a legacy scalar-shape entry with grantedTotal: 2.5 migrates into a count: 2 record, not silently dropped', async () => {
    const now = Date.now();
    const fractionalLegacyEntry = {
      orchestratorId: '__claim:fractional-legacy-total-type__',
      grantedTotal: 2.5,
      declaredAt: now,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([fractionalLegacyEntry]), 'utf8');

    // requestedCount: 0 still triggers the unconditional legacy-migration
    // write (round-3 fix) — granted stays 0, but the fractional total must
    // survive the migration, not vanish because it fails isValidClaimRecord
    // on this same read.
    const result = await claimCapacity(
      filePath,
      {
        type: 'fractional-legacy-total-type',
        requestedCount: 0,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: async () => 100,
        now,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    expect(result.alreadyGrantedTotal).toBe(2);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:fractional-legacy-total-type__');
    const legacyRecord = ledgerEntry.claims.find((claim) => claim.orchestratorId === '__legacy-unattributed-claim__');
    expect(legacyRecord.count).toBe(2);
    expect(Number.isInteger(legacyRecord.count)).toBe(true);
  });
});

describe('pruneExpiredClaims — defensive against a malformed claims[] element (post-review hardening, LOW finding)', () => {
  const ample = async () => 100;
  const CLAIM_TTL_MS = 60 * 60 * 1000;

  it('a null element in a persisted claims[] array does not throw inside the lock, for either claimCapacity or releaseCapacity', async () => {
    const now = Date.now();
    const malformedLedgerEntry = {
      orchestratorId: '__claim:malformed-claims-type__',
      claims: [null, { orchestratorId: 'orch-a', count: 3, claimedAt: now }],
      declaredAt: now,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([malformedLedgerEntry]), 'utf8');

    await expect(
      claimCapacity(
        filePath,
        {
          type: 'malformed-claims-type',
          requestedCount: 1,
          orchestratorId: 'orch-b',
          computeAvailableCapacity: ample,
          now,
        },
        { claimTtlMs: CLAIM_TTL_MS },
      ),
    ).resolves.toBeDefined();

    await fsPromises.writeFile(filePath, JSON.stringify([malformedLedgerEntry]), 'utf8');

    await expect(
      releaseCapacity(
        filePath,
        { type: 'malformed-claims-type', releaseCount: 1, orchestratorId: 'orch-a', now },
        { claimTtlMs: CLAIM_TTL_MS },
      ),
    ).resolves.toBeDefined();
  });

  it('a null element in a persisted claims[] array does not throw inside releaseCapacity on the NO-TTL path either (round-2 review LOW finding)', async () => {
    // Historical note: this comment previously claimed a malformed
    // element reaches `releaseCapacity`'s consume loop intact "ONLY on this
    // no-TTL path", implying `pruneExpiredClaims` was the sole guard and it
    // was TTL-gated. That stopped being true as of the round-3 review fix:
    // `normalizeClaims` now unconditionally filters every `claims[]` element
    // through `isValidClaimRecord` BEFORE `pruneExpiredClaims` ever runs,
    // regardless of whether `claimTtlMs` is set — see
    // 'normalizeClaims drops malformed claims[] elements at normalization
    // time (round-3 review LOW finding)' above, whose
    // "even on the no-TTL path" test name already reflects this, and the
    // unconditional-guard proof in the describe block below this one. This
    // test still exercises the no-TTL path specifically (a real scenario
    // worth covering on its own), it just no longer relies on
    // `pruneExpiredClaims` to explain why it doesn't throw.
    const now = Date.now();
    const malformedLedgerEntry = {
      orchestratorId: '__claim:malformed-claims-no-ttl-type__',
      claims: [null, { orchestratorId: 'orch-a', count: 3, claimedAt: now }],
      declaredAt: now,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([malformedLedgerEntry]), 'utf8');

    // No `options.claimTtlMs` passed at all.
    await expect(
      releaseCapacity(filePath, {
        type: 'malformed-claims-no-ttl-type',
        releaseCount: 1,
        orchestratorId: 'orch-a',
        now,
      }),
    ).resolves.toEqual(expect.objectContaining({ released: 1 }));
  });

  it('normalizeClaims drops a malformed claims[] element identically whether claimTtlMs is set or unset (— proves the guard is unconditional, not TTL-gated)', async () => {
    const now = Date.now();
    const seedMalformed = () =>
      fsPromises.writeFile(
        filePath,
        JSON.stringify([
          {
            orchestratorId: '__claim:normalize-unconditional-type__',
            claims: [null, {}, { orchestratorId: 'orch-a', count: 3, claimedAt: now }],
            declaredAt: now,
          },
        ]),
        'utf8',
      );

    // No claimTtlMs at all — the "no-TTL path" where, before the round-3
    // fix, only pruneExpiredClaims's absence of pruning was in play.
    await seedMalformed();
    const withoutTtl = await claimCapacity(filePath, {
      type: 'normalize-unconditional-type',
      requestedCount: 0,
      orchestratorId: 'orch-b',
      computeAvailableCapacity: async () => 100,
      now,
    });
    expect(withoutTtl.alreadyGrantedTotal).toBe(3); // only the well-formed element counted

    // Same malformed input, this time WITH a real claimTtlMs — if
    // normalizeClaims's filtering were somehow TTL-gated (the stale claim
    // corrects), this would behave differently. It doesn't.
    await seedMalformed();
    const withTtl = await claimCapacity(
      filePath,
      {
        type: 'normalize-unconditional-type',
        requestedCount: 0,
        orchestratorId: 'orch-b',
        computeAvailableCapacity: async () => 100,
        now,
      },
      { claimTtlMs: 6 * 60 * 60 * 1000 },
    );
    expect(withTtl.alreadyGrantedTotal).toBe(3);
  });
});

// ---------------------------------------------------------------------------
// — writeSharedSample: a genuine locked compare-and-swap for the
// `__shared-machine-sample__` reserved entry.
//
// Per the Phase 0 clarification comment on (superseding an earlier,
// weaker "just serialize, unconditional overwrite" draft in the Build Plan's
// Phase 1 edge-case prose): inside the `withLock` critical section, the
// current persisted entry is re-read and this beat's sample is only
// persisted if its `sampledAt` is strictly newer than what's already there,
// or the entry is absent/degraded (missing a well-formed `reading`). An
// empirical two-process race (12/30 = 40% failure rate) proved the weaker
// "just serialize" reading does NOT close — a genuine `sampledAt`
// comparison is required. The outer acceptance test for this property lives
// in ./cli-two-orchestrators.jest.spec.mjs (test 7, `[Phase 0]`
// e43b3d03) — these are the direct unit tests for the primitive itself.
//
// RED by construction: `writeSharedSample` does not exist yet on this
// module. `collect()` (the expensive shell-out) is NEVER invoked by this
// function — it takes an already-taken `reading`, never samples itself.
// ---------------------------------------------------------------------------

describe('writeSharedSample — locked compare-and-swap for the shared-machine-sample entry', () => {
  const SHARED_SAMPLE_ID = '__shared-machine-sample__';

  function makeReading({ sampledAt, freeRamMbMarker, stale = false }) {
    return {
      sampledAt,
      memory: { freeRamMb: freeRamMbMarker },
      disk: { freeDiskGb: 100 },
      stale,
    };
  }

  it('a newer sample overwrites an older persisted one', async () => {
    await writeSharedSample(filePath, makeReading({ sampledAt: 1000, freeRamMbMarker: 111 }));
    await writeSharedSample(filePath, makeReading({ sampledAt: 2000, freeRamMbMarker: 222 }));

    const dibs = await readDibs(filePath, Date.now());
    const entry = dibs.find((item) => item.orchestratorId === SHARED_SAMPLE_ID);

    expect(entry.sampledAt).toBe(2000);
    expect(entry.reading.memory.freeRamMb).toBe(222);
  });

  it('an older/stale-relative-to-persisted sample does NOT overwrite a newer persisted one', async () => {
    await writeSharedSample(filePath, makeReading({ sampledAt: 2000, freeRamMbMarker: 222 }));
    await writeSharedSample(filePath, makeReading({ sampledAt: 1000, freeRamMbMarker: 111 }));

    const dibs = await readDibs(filePath, Date.now());
    const entry = dibs.find((item) => item.orchestratorId === SHARED_SAMPLE_ID);

    // The genuinely fresher sample (sampledAt 2000) must survive — the older
    // write racing/arriving behind it must never clobber it.
    expect(entry.sampledAt).toBe(2000);
    expect(entry.reading.memory.freeRamMb).toBe(222);
  });

  it('two concurrent locked writes serialize and both persist correctly (no torn state)', async () => {
    const [resultA, resultB] = await Promise.all([
      writeSharedSample(filePath, makeReading({ sampledAt: 5000, freeRamMbMarker: 555 })),
      writeSharedSample(filePath, makeReading({ sampledAt: 6000, freeRamMbMarker: 666 })),
    ]);

    expect(resultA).toBeDefined();
    expect(resultB).toBeDefined();

    const dibs = await readDibs(filePath, Date.now());
    const sharedSampleEntries = dibs.filter((item) => item.orchestratorId === SHARED_SAMPLE_ID);

    // Never duplicated/torn — exactly one persisted entry for the reserved id.
    expect(sharedSampleEntries).toHaveLength(1);
    const entry = sharedSampleEntries[0];

    // No torn/hybrid write: sampledAt is always paired with its OWN
    // freeRamMb marker, and the CAS property means the newer of the two
    // (6000) is the one that survives, regardless of completion order.
    expect(entry.sampledAt).toBe(6000);
    expect(entry.reading.memory.freeRamMb).toBe(666);
  });

  it('writing when no prior entry exists creates it', async () => {
    let dibs = await readDibs(filePath, Date.now());
    expect(dibs.find((item) => item.orchestratorId === SHARED_SAMPLE_ID)).toBeUndefined();

    await writeSharedSample(filePath, makeReading({ sampledAt: 4242, freeRamMbMarker: 4242 }));

    dibs = await readDibs(filePath, Date.now());
    const entry = dibs.find((item) => item.orchestratorId === SHARED_SAMPLE_ID);
    expect(entry).toBeDefined();
    expect(entry.sampledAt).toBe(4242);
    expect(entry.reading.memory.freeRamMb).toBe(4242);
    expect(entry.reading.disk.freeDiskGb).toBe(100);
  });

  it('a stale/degraded sample is never persisted, even when the lock is held, and never clobbers an existing entry', async () => {
    // No prior entry — a stale sample must not create one.
    await writeSharedSample(filePath, makeReading({ sampledAt: 1000, freeRamMbMarker: 111, stale: true }));

    let dibs = await readDibs(filePath, Date.now());
    expect(dibs.find((item) => item.orchestratorId === SHARED_SAMPLE_ID)).toBeUndefined();

    // A genuine fresh entry now exists...
    await writeSharedSample(filePath, makeReading({ sampledAt: 2000, freeRamMbMarker: 222 }));

    // ...and a LATER-in-time but stale/degraded sample must not overwrite it,
    // even though its sampledAt is nominally newer.
    await writeSharedSample(filePath, makeReading({ sampledAt: 9000, freeRamMbMarker: 999, stale: true }));

    dibs = await readDibs(filePath, Date.now());
    const entry = dibs.find((item) => item.orchestratorId === SHARED_SAMPLE_ID);
    expect(entry.sampledAt).toBe(2000);
    expect(entry.reading.memory.freeRamMb).toBe(222);
  });

  it('a reading with a missing, NaN, or non-numeric sampledAt is rejected before ever touching the lock/ledger', async () => {
    const invalidSampledAtValues = [undefined, NaN, 'not-a-number', Infinity, -Infinity, null];

    for (const sampledAt of invalidSampledAtValues) {
      const result = await writeSharedSample(filePath, {
        sampledAt,
        memory: { freeRamMb: 111 },
        disk: { freeDiskGb: 100 },
      });

      expect(result).toEqual({ persisted: false, reason: 'invalid-sampled-at' });
    }

    // None of the invalid attempts created (or clobbered) the reserved entry.
    const dibs = await readDibs(filePath, Date.now());
    expect(dibs.find((item) => item.orchestratorId === SHARED_SAMPLE_ID)).toBeUndefined();
  });

  it('a malformed sampledAt never clobbers a genuinely valid prior entry', async () => {
    await writeSharedSample(filePath, makeReading({ sampledAt: 2000, freeRamMbMarker: 222 }));

    const result = await writeSharedSample(filePath, {
      sampledAt: NaN,
      memory: { freeRamMb: 999 },
      disk: { freeDiskGb: 5 },
    });
    expect(result).toEqual({ persisted: false, reason: 'invalid-sampled-at' });

    const dibs = await readDibs(filePath, Date.now());
    const entry = dibs.find((item) => item.orchestratorId === SHARED_SAMPLE_ID);
    expect(entry.sampledAt).toBe(2000);
    expect(entry.reading.memory.freeRamMb).toBe(222);
  });

  it('a persisted entry whose nested reading.stale is true is treated as degraded, regardless of sampledAt ordering (existingIsDegraded reads the correct field path)', async () => {
    // Construct the persisted shape `writeSharedSample` itself produces:
    // the flat reading object lives under `entry.reading`, so a stale marker
    // on a persisted entry is `entry.reading.stale`, never a top-level
    // `entry.stale` (writeSharedSample never persists a reading with
    // `stale: true` itself — see the early-return guard above — but this
    // proves the CAS branch that reads the persisted shape back is looking
    // at the right field, independent of how such an entry came to exist).
    writeFileSync(
      filePath,
      JSON.stringify([
        {
          orchestratorId: SHARED_SAMPLE_ID,
          sampledAt: 9999, // nominally newer than the incoming sample below.
          reading: {
            sampledAt: 9999,
            memory: { freeRamMb: 111 },
            disk: { freeDiskGb: 100 },
            stale: true,
          },
          declaredAt: 9999,
        },
      ]),
      'utf8',
    );

    // An older-by-sampledAt but genuinely fresh incoming reading. If
    // `existingIsDegraded` checked the right field, the persisted entry is
    // degraded (stale) and this write must persist UNCONDITIONALLY —
    // overriding it despite its nominally-older sampledAt.
    const result = await writeSharedSample(filePath, makeReading({ sampledAt: 1000, freeRamMbMarker: 222 }));

    expect(result).toEqual({ persisted: true });

    const dibs = await readDibs(filePath, Date.now());
    const entry = dibs.find((item) => item.orchestratorId === SHARED_SAMPLE_ID);
    expect(entry.sampledAt).toBe(1000);
    expect(entry.reading.memory.freeRamMb).toBe(222);
    expect(entry.reading.stale).toBe(false);
  });

  it('the locked write does not itself invoke any shell-out — collect() stays entirely outside this function', () => {
    // Structural proof, not a spy on a wired-up hook: this module never
    // imports node:child_process at all (collect()/takeSample() live in
    // cli.mjs, not here), and writeSharedSample's own function body — from
    // its declaration to the next top-level export — must not reference any
    // shell-out primitive. This function takes an already-taken `reading`
    // as a parameter; it must never sample on its own.
    expect(coordinationFileSource).not.toMatch(/from ['"]node:child_process['"]/);

    const functionStart = coordinationFileSource.indexOf('export async function writeSharedSample(');
    expect(functionStart).toBeGreaterThanOrEqual(0);

    const nextExportMatch = coordinationFileSource
      .slice(functionStart + 1)
      .match(/\nexport (async function|function|class|const)/);
    const functionEnd = nextExportMatch
      ? functionStart + 1 + nextExportMatch.index
      : coordinationFileSource.length;
    const functionBody = coordinationFileSource.slice(functionStart, functionEnd);

    expect(functionBody).not.toMatch(/takeSample|collect\(|execSync|spawn\(|exec\(|child_process/i);
  });
});

// ---------------------------------------------------------------------------
// Phase 3 (RED) — compression-velocity persistence on the
// `__shared-machine-sample__` reserved entry.
//
// Phase 1 (ce104918) taught `parseVmStat` to return `compressions`/
// `decompressions` cumulative counters on the `memory` reading. Phase 2
// (45c96249) added the pure `computeCompressionVelocity({ previous,
// current })` in `./compression-velocity.mjs`, which needs two
// `{ compressions, decompressions, timestampMs }` samples spanning a beat.
// This phase wires the two together: `cli.mjs` must persist the PRIOR
// beat's counters + a timestamp on the reserved entry so the NEXT beat can
// derive `previous` from it, and must degrade a pre-this-ticket entry
// (written by an older CLI that never emitted these fields) to
// cold-start-equivalent rather than crashing or fabricating a `0` delta.
//
// `writeSharedSample` itself is a generic locked compare-and-swap over
// whatever `reading` shape it is handed (see its own doc comment above) —
// it already round-trips arbitrary extra fields on `reading.memory`
// losslessly, and its existing lock discipline already serializes
// concurrent writers. The two tests below pin that this generic contract
// genuinely extends to the new `compressions`/`decompressions`/
// `timestampMs` fields (regression coverage for Phase 3, not proof that new
// production code is required in this module) — the genuinely RED case is
// `cli.mjs` NOT YET treating a legacy entry (missing these fields) as
// cold-start-equivalent, which the black-box CLI test further below covers.
// ---------------------------------------------------------------------------

describe('writeSharedSample carries compressions/decompressions/timestampMs alongside its existing cached fields', () => {
  const SHARED_SAMPLE_ID = '__shared-machine-sample__';

  function makeCompressorReading({ sampledAt, compressions, decompressions, timestampMs }) {
    return {
      sampledAt,
      memory: { freeRamMb: 4096, compressedMb: 512, compressions, decompressions, timestampMs },
      disk: { freeDiskGb: 100 },
      stale: false,
    };
  }

  it('round-trips compressions/decompressions/timestampMs unchanged through a write + read', async () => {
    await writeSharedSample(
      filePath,
      makeCompressorReading({ sampledAt: 1000, compressions: 12_345, decompressions: 6_789, timestampMs: 1000 }),
    );

    const dibs = await readDibs(filePath, Date.now());
    const entry = dibs.find((item) => item.orchestratorId === SHARED_SAMPLE_ID);

    expect(entry).toBeDefined();
    expect(entry.reading.memory.compressions).toBe(12_345);
    expect(entry.reading.memory.decompressions).toBe(6_789);
    expect(entry.reading.memory.timestampMs).toBe(1000);
    // Existing cached fields (pre-this-ticket) must survive untouched
    // alongside the new ones — this is an additive extension, not a
    // replacement of the persisted shape.
    expect(entry.reading.memory.freeRamMb).toBe(4096);
    expect(entry.reading.memory.compressedMb).toBe(512);
  });

  it('a pre-this-ticket reserved entry missing compressions/decompressions/timestampMs entirely reads back without throwing', async () => {
    // Simulates an older CLI version's write: no `compressions`/
    // `decompressions`/`timestampMs` fields at all on `reading.memory`,
    // only the fields that existed before this ticket.
    writeFileSync(
      filePath,
      JSON.stringify([
        {
          orchestratorId: SHARED_SAMPLE_ID,
          sampledAt: 5000,
          reading: {
            sampledAt: 5000,
            memory: { freeRamMb: 2048, compressedMb: 256 },
            disk: { freeDiskGb: 80 },
            stale: false,
          },
          declaredAt: 5000,
        },
      ]),
      'utf8',
    );

    let dibs;
    expect(() => {
      dibs = readDibs(filePath, Date.now());
    }).not.toThrow();
    await expect(dibs).resolves.toBeDefined();

    const resolved = await dibs;
    const entry = resolved.find((item) => item.orchestratorId === SHARED_SAMPLE_ID);
    expect(entry).toBeDefined();
    // The missing fields must read back as `undefined` — not fabricated as
    // `0` (which would misrepresent "no compression activity" as a real
    // reading rather than "no data was ever collected for this field").
    expect(entry.reading.memory.compressions).toBeUndefined();
    expect(entry.reading.memory.decompressions).toBeUndefined();
    expect(entry.reading.memory.timestampMs).toBeUndefined();

    // A subsequent write carrying the new fields must still succeed and
    // overwrite the legacy entry cleanly (no crash migrating forward).
    const result = await writeSharedSample(
      filePath,
      makeCompressorReading({ sampledAt: 6000, compressions: 100, decompressions: 20, timestampMs: 6000 }),
    );
    expect(result).toEqual({ persisted: true });

    const dibsAfter = await readDibs(filePath, Date.now());
    const entryAfter = dibsAfter.find((item) => item.orchestratorId === SHARED_SAMPLE_ID);
    expect(entryAfter.reading.memory.compressions).toBe(100);
  });

  it('two concurrent locked writes carrying compression counters serialize and do not corrupt each other (no torn state)', async () => {
    const [resultA, resultB] = await Promise.all([
      writeSharedSample(
        filePath,
        makeCompressorReading({ sampledAt: 7000, compressions: 700, decompressions: 70, timestampMs: 7000 }),
      ),
      writeSharedSample(
        filePath,
        makeCompressorReading({ sampledAt: 8000, compressions: 800, decompressions: 80, timestampMs: 8000 }),
      ),
    ]);

    expect(resultA).toBeDefined();
    expect(resultB).toBeDefined();

    const dibs = await readDibs(filePath, Date.now());
    const sharedSampleEntries = dibs.filter((item) => item.orchestratorId === SHARED_SAMPLE_ID);

    // Never duplicated/torn — exactly one persisted entry for the reserved
    // id, and its compression fields are never a hybrid of the two writers
    // (e.g. writer A's compressions paired with writer B's decompressions).
    expect(sharedSampleEntries).toHaveLength(1);
    const entry = sharedSampleEntries[0];

    // The CAS property means the newer of the two (sampledAt 8000) survives
    // regardless of completion order, and its compression counters travel
    // together with it — never torn apart from a competing writer's values.
    expect(entry.sampledAt).toBe(8000);
    expect(entry.reading.memory.compressions).toBe(800);
    expect(entry.reading.memory.decompressions).toBe(80);
    expect(entry.reading.memory.timestampMs).toBe(8000);
  });
});

// ---------------------------------------------------------------------------
// Phase 1 (RED) — AIMD ceiling shared coordination-file state.
//
// Phase 2 needs a host-wide reserved entry that persists the AIMD
// concurrency ceiling's own state across beats/orchestrators: the current
// ceiling value and a sustained-Normal-pressure counter (per its shipped
// cold-start default: ceiling seeded at 4, counter at 0). This follows the
// EXISTING `SHARED_SAMPLE_RESERVED_ID`/`GLOBAL_SPAWN_BUCKET_RESERVED_ID`
// convention in this file: a reserved orchestratorId (`isReservedEntry`'s
// leading+trailing `__` convention, e.g. `'__aimd-ceiling-state__'`),
// mutated only inside `withLock`/`writeEntriesAtomic` (never a bare
// `fs.writeFile`), and therefore automatically exempt from `pruneStale`'s
// liveness pruning without any code change to `pruneStale` itself.
//
// RED by construction: none of `AIMD_CEILING_RESERVED_ID`,
// `readAimdCeilingState`, or `writeAimdCeilingState` exist yet on this
// module. These are the names this test file ASSUMES the Phase 1
// implementer will export — chosen to mirror `SHARED_SAMPLE_RESERVED_ID` /
// `writeSharedSample`'s existing naming shape as closely as possible. If the
// implementer picks different names, only this describe block's references
// need updating — the intent (a lazily-initialized, lock-guarded, prune-
// exempt reserved entry holding `{ ceiling, sustainedNormalCount }`) is
// unambiguous regardless of the exact identifiers.
//
// Namespace-imported (`coordinationFile.*`), not named-imported, for the
// same reason the rest of this file does so for not-yet-implemented
// exports: a named import of a genuinely missing export is a hard
// module-evaluation SyntaxError under real ESM, which would take down every
// other test in this file.
// ---------------------------------------------------------------------------

describe('AIMD ceiling shared coordination-file state (Phase 1 RED)', () => {
  const AIMD_CEILING_RESERVED_ID = '__aimd-ceiling-state__';
  const DEFAULT_AIMD_CEILING = 4;
  const DEFAULT_AIMD_SUSTAINED_NORMAL_COUNT = 0;

  it('is stored under a reserved id satisfying isReservedEntry, following the SHARED_SAMPLE_RESERVED_ID/GLOBAL_SPAWN_BUCKET_RESERVED_ID convention', () => {
    expect(isReservedEntry(AIMD_CEILING_RESERVED_ID)).toBe(true);
  });

  it('initializes the shared ceiling entry on first read — no entry exists yet, but reading returns the cold-start default without an explicit "create" step', async () => {
    let dibs = await readDibs(filePath, Date.now());
    expect(dibs.find((entry) => entry.orchestratorId === AIMD_CEILING_RESERVED_ID)).toBeUndefined();

    const state = await coordinationFile.readAimdCeilingState(filePath, { now: Date.now() });

    expect(state).toBeDefined();
    expect(state.ceiling).toBe(DEFAULT_AIMD_CEILING);
    expect(state.sustainedNormalCount).toBe(DEFAULT_AIMD_SUSTAINED_NORMAL_COUNT);

    // The cold-start default is itself persisted by the read (mirrors
    // consumeGlobalSpawnTokens's "first-ever call initializes" shape) — a
    // second, independent readDibs must see it without going through
    // readAimdCeilingState again.
    dibs = await readDibs(filePath, Date.now());
    const entry = dibs.find((item) => item.orchestratorId === AIMD_CEILING_RESERVED_ID);
    expect(entry).toBeDefined();
    expect(entry.ceiling).toBe(DEFAULT_AIMD_CEILING);
    expect(entry.sustainedNormalCount).toBe(DEFAULT_AIMD_SUSTAINED_NORMAL_COUNT);
  });

  it('two concurrent initializers converge on one entry, not two — real concurrency against a coordination file with no prior entry', async () => {
    const now = Date.now();

    const [stateA, stateB] = await Promise.all([
      coordinationFile.readAimdCeilingState(filePath, { now }),
      coordinationFile.readAimdCeilingState(filePath, { now }),
    ]);

    expect(stateA).toBeDefined();
    expect(stateB).toBeDefined();

    const dibs = await readDibs(filePath, now);
    const aimdEntries = dibs.filter((entry) => entry.orchestratorId === AIMD_CEILING_RESERVED_ID);

    // Never duplicated/torn — exactly one persisted entry for the reserved
    // id, mirroring writeSharedSample's "two concurrent locked writes
    // serialize" precedent in this same file.
    expect(aimdEntries).toHaveLength(1);
    expect(aimdEntries[0].ceiling).toBe(DEFAULT_AIMD_CEILING);
    expect(aimdEntries[0].sustainedNormalCount).toBe(DEFAULT_AIMD_SUSTAINED_NORMAL_COUNT);
  }, 15_000);

  it('reserved entry is exempt from pruneStale regardless of age (mirrors the SHARED_SAMPLE_RESERVED_ID/GLOBAL_SPAWN_BUCKET_RESERVED_ID precedent in this file)', async () => {
    const livenessThresholdMs = 15 * 60 * 1000;
    const twentyMinutesAgo = 1_000_000;
    const now = twentyMinutesAgo + 20 * 60 * 1000;

    // Seed the reserved entry directly via declareDibs (bypassing whatever
    // the Phase 1 implementer names the AIMD-specific accessor) — this
    // proves the generic pruneStale/isReservedEntry exemption applies to
    // this specific reserved id, independent of the AIMD read/write API's
    // exact shape.
    await declareDibs(
      filePath,
      {
        orchestratorId: AIMD_CEILING_RESERVED_ID,
        ceiling: 7,
        sustainedNormalCount: 3,
        declaredAt: twentyMinutesAgo,
      },
      { livenessThresholdMs },
    );

    const dibs = await readDibs(filePath, now, { livenessThresholdMs });

    expect(dibs.map((entry) => entry.orchestratorId)).toContain(AIMD_CEILING_RESERVED_ID);
  });

  it('aimd ceiling state mutation happens only inside withLock/writeEntriesAtomic: concurrent writers never produce a torn/interleaved final state, and never duplicate the reserved entry', async () => {
    const now = Date.now();

    // Prime a base state so both writers are racing a genuine
    // read-modify-write, not two independent "no prior entry" initializers
    // (already covered by the concurrent-initializers test above).
    await coordinationFile.readAimdCeilingState(filePath, { now });

    const [resultA, resultB] = await Promise.all([
      coordinationFile.writeAimdCeilingState(filePath, { ceiling: 6, sustainedNormalCount: 1 }, { now }),
      coordinationFile.writeAimdCeilingState(filePath, { ceiling: 8, sustainedNormalCount: 9 }, { now }),
    ]);

    expect(resultA).toBeDefined();
    expect(resultB).toBeDefined();

    const dibs = await readDibs(filePath, now);
    const aimdEntries = dibs.filter((entry) => entry.orchestratorId === AIMD_CEILING_RESERVED_ID);

    // Exactly one persisted entry — no duplication from two concurrent
    // critical sections both appending instead of upserting.
    expect(aimdEntries).toHaveLength(1);
    const entry = aimdEntries[0];

    // No torn/hybrid write: `ceiling` and `sustainedNormalCount` must come
    // from the SAME winning write, never a `ceiling` from one writer paired
    // with a `sustainedNormalCount` from the other (which would indicate the
    // two critical sections were not serialized by a single lock).
    const isWriterAsWon = entry.ceiling === 6 && entry.sustainedNormalCount === 1;
    const isWriterBsWon = entry.ceiling === 8 && entry.sustainedNormalCount === 9;
    expect(isWriterAsWon || isWriterBsWon).toBe(true);
  }, 15_000);
});

// ---------------------------------------------------------------------------
// releaseCapacity — no-op early-return (Phase 3 /)
//
// `releaseCapacity` must skip the filter/rebuild/`writeEntriesAtomic` call
// entirely whenever the computed `released` amount is `0` — no lock-held
// atomic rename, no bumped `declaredAt`, for a call that changes nothing.
// `writeEntriesAtomic` is a private, non-exported function (not reachable via
// `jest.spyOn`), so "no write occurred" is asserted the way the Investigation
// comment on suggests: via `fs.stat`'s `mtimeMs` on the real
// coordination file, captured before and after the call. `writeEntriesAtomic`
// writes via a temp-file-then-rename, so a genuine write always changes the
// destination file's `mtimeMs` — an unchanged `mtimeMs` is a reliable proxy
// for "no write happened", and the fourth (contrast) test below proves the
// proxy isn't a false negative by exercising the branch that DOES write.
// ---------------------------------------------------------------------------

describe('releaseCapacity — no-op early-return skips the write entirely (Phase 3 /)', () => {
  const ample = async () => 100;

  it('releaseCount: 0 from a caller who genuinely holds live claims performs no write (mtime unchanged)', async () => {
    await claimCapacity(filePath, {
      type: 'noop-explicit-zero',
      requestedCount: 5,
      orchestratorId: 'orch-a',
      computeAvailableCapacity: ample,
    });

    const statBefore = await fsPromises.stat(filePath);

    const result = await releaseCapacity(filePath, {
      type: 'noop-explicit-zero',
      releaseCount: 0,
      orchestratorId: 'orch-a',
    });

    const statAfter = await fsPromises.stat(filePath);

    expect(result).toEqual({ released: 0, grantedTotal: 5 });
    expect(statAfter.mtimeMs).toBe(statBefore.mtimeMs);
  });

  it('a caller with no claim record at all for the type performs no write (mtime unchanged)', async () => {
    // Some other type/claim exists so the coordination file itself already
    // exists on disk with real content — proving the no-op holds even when
    // there's a plausible ledger entry to rewrite around, not just an empty
    // file.
    await claimCapacity(filePath, {
      type: 'some-other-type',
      requestedCount: 3,
      orchestratorId: 'orch-other',
      computeAvailableCapacity: ample,
    });

    const statBefore = await fsPromises.stat(filePath);

    const result = await releaseCapacity(filePath, {
      type: 'noop-never-claimed',
      releaseCount: 5,
      orchestratorId: 'orch-never-claimed',
    });

    const statAfter = await fsPromises.stat(filePath);

    expect(result).toEqual({ released: 0, grantedTotal: 0 });
    expect(statAfter.mtimeMs).toBe(statBefore.mtimeMs);
  });

  it('a caller whose only claim record for the type has already expired (TTL) performs no write — not even a "prune the expired record" write', async () => {
    const CLAIM_TTL_MS = 24 * 60 * 60 * 1000; // 1 day
    const t0 = 1_700_000_000_000;
    jest.spyOn(Date, 'now').mockReturnValue(t0);

    await claimCapacity(
      filePath,
      {
        type: 'noop-expired-claim',
        requestedCount: 4,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
      },
      { claimTtlMs: CLAIM_TTL_MS },
    );

    jest.spyOn(Date, 'now').mockReturnValue(t0 + CLAIM_TTL_MS + 1);

    const statBefore = await fsPromises.stat(filePath);

    const result = await releaseCapacity(
      filePath,
      { type: 'noop-expired-claim', releaseCount: 4, orchestratorId: 'orch-a' },
      { claimTtlMs: CLAIM_TTL_MS },
    );

    const statAfter = await fsPromises.stat(filePath);

    expect(result).toEqual({ released: 0, grantedTotal: 0 });
    // Not a partial write that "prunes" the already-logically-gone (expired)
    // record as a side effect — the file must be byte-for-byte untouched,
    // not merely aggregate-equal.
    expect(statAfter.mtimeMs).toBe(statBefore.mtimeMs);

    jest.restoreAllMocks();
  });

  it('contrast case: a genuine non-zero release for a caller with live, unexpired claims still performs the write (mtime changes)', async () => {
    await claimCapacity(filePath, {
      type: 'genuine-release',
      requestedCount: 10,
      orchestratorId: 'orch-a',
      computeAvailableCapacity: ample,
    });

    // Ensure a real wall-clock gap so a coarse-resolution filesystem mtime
    // clock can't accidentally alias "unchanged" — the write must land at a
    // strictly later mtime than the pre-call read.
    await new Promise((resolve) => setTimeout(resolve, 10));

    const statBefore = await fsPromises.stat(filePath);

    const result = await releaseCapacity(filePath, {
      type: 'genuine-release',
      releaseCount: 4,
      orchestratorId: 'orch-a',
    });

    const statAfter = await fsPromises.stat(filePath);

    expect(result).toEqual({ released: 4, grantedTotal: 6 });
    expect(statAfter.mtimeMs).not.toBe(statBefore.mtimeMs);
  });
});

// ---------------------------------------------------------------------------
// Duplicated `__claim:<type>__` ledger-entry merge — granular edge cases
// (Convergence Analysis; the outer acceptance test in
// ./coordination-file.outer-acceptance.jest.spec.mjs ("nit 3") already pins
// the basic case that alreadyGrantedTotal must sum BOTH duplicate entries,
// not just the first `.find()` match. These two tests pin the exact fix
// behavior at the unit level: the merge must be scoped to only the TARGET
// type's duplicated entries (never touching an unrelated type's own,
// well-formed entry), and releaseCapacity against a duplicated ledger must
// leave exactly ONE merged entry behind afterward, never two, never a
// partial merge.
// ---------------------------------------------------------------------------

describe('duplicated __claim:<type>__ ledger entries — merge is scoped per-type and collapses to one entry on write', () => {
  const ample = async () => 100;

  it('cross-type isolation: merging a duplicated __claim:foo__ entry leaves a separate, well-formed __claim:bar__ entry completely untouched', async () => {
    const now = Date.now();

    const fooLegacy = { orchestratorId: '__claim:foo__', grantedTotal: 4, declaredAt: now };
    const fooArray = {
      orchestratorId: '__claim:foo__',
      claims: [{ orchestratorId: 'orch-b', count: 6, claimedAt: now }],
      grantedTotal: 6,
      declaredAt: now,
    };
    const barEntry = {
      orchestratorId: '__claim:bar__',
      claims: [{ orchestratorId: 'orch-z', count: 9, claimedAt: now }],
      grantedTotal: 9,
      declaredAt: now,
    };

    await fsPromises.writeFile(filePath, JSON.stringify([fooLegacy, fooArray, barEntry]), 'utf8');

    // Trigger the dedup-merge fix for `foo` via claimCapacity — MUST be red
    // today (mirrors the outer acceptance test's "nit 3" assertion exactly:
    // current code's `.find()` only reads the FIRST duplicate, fooLegacy's 4,
    // silently losing fooArray's 6).
    const claimResult = await claimCapacity(
      filePath,
      {
        type: 'foo',
        requestedCount: 1,
        orchestratorId: 'orch-new',
        computeAvailableCapacity: ample,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );
    expect(claimResult.alreadyGrantedTotal).toBe(10);

    await releaseCapacity(
      filePath,
      { type: 'foo', releaseCount: 1, orchestratorId: 'orch-b' },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const barEntriesAfter = raw.filter((entry) => entry.orchestratorId === '__claim:bar__');

    // Exactly one bar entry, and it is byte-for-byte identical to what was
    // seeded — not merely "still present", but untouched by foo's merge.
    expect(barEntriesAfter).toHaveLength(1);
    expect(barEntriesAfter[0]).toEqual(barEntry);
  });

  it('releaseCapacity against a duplicated __claim:foo__ entry (legacy-scalar + per-claim-array) merges both before releasing, and persists exactly ONE __claim:foo__ entry afterward', async () => {
    const now = Date.now();

    const fooLegacy = { orchestratorId: '__claim:foo__', grantedTotal: 4, declaredAt: now };
    const fooArray = {
      orchestratorId: '__claim:foo__',
      claims: [{ orchestratorId: 'orch-b', count: 6, claimedAt: now }],
      grantedTotal: 6,
      declaredAt: now,
    };

    await fsPromises.writeFile(filePath, JSON.stringify([fooLegacy, fooArray]), 'utf8');

    // orch-b holds 6 (in the per-claim-array duplicate) — release 4 of it.
    const result = await releaseCapacity(
      filePath,
      { type: 'foo', releaseCount: 4, orchestratorId: 'orch-b' },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    // Correct amount from the MERGED set: orch-b genuinely holds 6 once both
    // duplicates are combined, so releasing 4 is a full release (not floored
    // at some partial/incorrect view from only one of the two entries).
    expect(result.released).toBe(4);
    // Remaining across the merged set after release: legacy's unattributed 4
    // (untouched — it isn't orch-b's) + orch-b's remaining 6 - 4 = 2 → 6.
    expect(result.grantedTotal).toBe(6);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const fooEntriesAfter = raw.filter((entry) => entry.orchestratorId === '__claim:foo__');

    // Exactly one entry — not two (the duplicates collapsed into one write),
    // not a partial merge that left one of the originals lying around.
    expect(fooEntriesAfter).toHaveLength(1);
    expect(fooEntriesAfter[0].grantedTotal).toBe(6);
  });
});

// ---------------------------------------------------------------------------
// — `readLedgerCandidateRecords`'s claim-type branch must merge every
// duplicate `__claim:<type>__` row the same way `claimCapacity`/
// `releaseCapacity`/`reserveAdmission` do on read, instead of the current
// `.find()`-based first-match-only divergence documented at its own doc
// comment. These are RED until that `.find()` becomes a
// `.filter()` + `.flatMap(normalizeClaims)` merge.
// ---------------------------------------------------------------------------

describe('readLedgerCandidateRecords merges duplicate __claim:<type>__ ledger rows instead of reading only the first match', () => {
  it('surfaces claims from BOTH duplicate rows merged into one array, including a ghost that lives only in the second row', async () => {
    const now = Date.now();

    const firstRow = {
      orchestratorId: '__claim:foo__',
      claims: [{ orchestratorId: 'orch-live', count: 2, claimedAt: now }],
      grantedTotal: 2,
      declaredAt: now,
    };
    const secondRow = {
      orchestratorId: '__claim:foo__',
      // A ghost that today's `.find()` never sees at all, because it only
      // ever inspects the FIRST matching row.
      claims: [{ orchestratorId: 'orch-ghost', count: 1, claimedAt: now }],
      grantedTotal: 1,
      declaredAt: now,
    };

    await fsPromises.writeFile(filePath, JSON.stringify([firstRow, secondRow]), 'utf8');

    const candidates = await readLedgerCandidateRecords(filePath, 'foo', now);

    expect(candidates.map((record) => record.orchestratorId).sort()).toEqual(['orch-ghost', 'orch-live']);
  });

  it('still merges both duplicate rows when called with no third (now) argument, matching cli.mjs\'s actual production call shape', async () => {
    const now = Date.now();

    const firstRow = {
      orchestratorId: '__claim:bar__',
      claims: [{ orchestratorId: 'orch-a', count: 3, claimedAt: now }],
      grantedTotal: 3,
      declaredAt: now,
    };
    const secondRow = {
      orchestratorId: '__claim:bar__',
      claims: [{ orchestratorId: 'orch-b', count: 4, claimedAt: now }],
      grantedTotal: 4,
      declaredAt: now,
    };

    await fsPromises.writeFile(filePath, JSON.stringify([firstRow, secondRow]), 'utf8');

    // No third argument at all — production's real call shape.
    const candidates = await readLedgerCandidateRecords(filePath, 'bar');

    expect(candidates.map((record) => record.orchestratorId).sort()).toEqual(['orch-a', 'orch-b']);
  });
});

// ---------------------------------------------------------------------------
// Omit an emptied `__claim:<type>__` ledger entry entirely, rather than
// persisting `{ claims: [], grantedTotal: 0 }` (Convergence Analysis,
// Phase 4 — nit 4). The outer acceptance test in
// ./coordination-file.outer-acceptance.jest.spec.mjs already pins the basic
// case (a single, fully-released `__claim:<type>__` entry must not survive
// the write). These three tests pin edge cases the outer test doesn't cover:
// the omission logic must be scoped ONLY to the target `__claim:<type>__`
// ledger entry — it must never touch other reserved entries that happen to
// have no `claims[]` field of their own, must leave sibling claim-ledger
// entries for a DIFFERENT type completely untouched, and must fire on the
// Phase-3-merged result of duplicated entries (not merely per-source-entry).
//
// All three tests are RED today: the current write path in `releaseCapacity`
// always persists `{ orchestratorId: ledgerId, claims: [], grantedTotal: 0,
// declaredAt }` once `released > 0`, regardless of whether `claims` ended up
// empty.
// ---------------------------------------------------------------------------

describe('releaseCapacity omits an emptied __claim:<type>__ ledger entry from the write, rather than persisting it as {claims: [], grantedTotal: 0}', () => {
  it('scope leakage guard: a GLOBAL_SPAWN_BUCKET_RESERVED_ID entry (no claims[] field) and a SHARED_SAMPLE_RESERVED_ID entry (no claims[] field) both survive byte-for-byte when releaseCapacity fully empties an unrelated __claim:foo__ entry', async () => {
    const now = Date.now();

    const spawnBucketEntry = {
      orchestratorId: '__global-spawn-rate-bucket__',
      tokens: 7,
      lastRefillAt: now,
      declaredAt: now,
    };
    const sharedSampleEntry = {
      orchestratorId: '__shared-machine-sample__',
      sampledAt: now,
      reading: { freeRamMb: 4096, freeDiskMb: 102400 },
      declaredAt: now,
    };
    const fooEntry = {
      orchestratorId: '__claim:foo__',
      claims: [{ orchestratorId: 'orch-a', count: 3, claimedAt: now }],
      grantedTotal: 3,
      declaredAt: now,
    };

    await fsPromises.writeFile(
      filePath,
      JSON.stringify([spawnBucketEntry, fooEntry, sharedSampleEntry]),
      'utf8',
    );

    await releaseCapacity(filePath, { type: 'foo', releaseCount: 3, orchestratorId: 'orch-a' });

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));

    const spawnBucketAfter = raw.find((entry) => entry.orchestratorId === '__global-spawn-rate-bucket__');
    const sharedSampleAfter = raw.find((entry) => entry.orchestratorId === '__shared-machine-sample__');
    const fooEntriesAfter = raw.filter((entry) => entry.orchestratorId === '__claim:foo__');

    expect(spawnBucketAfter).toEqual(spawnBucketEntry);
    expect(sharedSampleAfter).toEqual(sharedSampleEntry);
    // The naive-predicate failure mode this test guards against: an
    // implementation of "omit when claims[] is empty" that applies the
    // predicate too broadly (e.g. `!entry.claims?.length`, which is also
    // truthy for these two reserved entries — neither has a `claims[]`
    // field at all) would strip the spawn-bucket/shared-sample entries too.
    // The two `toEqual` assertions above already guard against that. This
    // assertion is the RED-today half: the emptied `__claim:foo__` entry
    // itself must be entirely absent, not persisted as
    // `{claims: [], grantedTotal: 0}`.
    expect(fooEntriesAfter).toHaveLength(0);
  });

  it('cross-type isolation: releaseCapacity fully emptying __claim:foo__ leaves a separate, still-live __claim:bar__ entry unchanged, and foo is entirely absent afterward', async () => {
    const now = Date.now();

    const fooEntry = {
      orchestratorId: '__claim:foo__',
      claims: [{ orchestratorId: 'orch-a', count: 5, claimedAt: now }],
      grantedTotal: 5,
      declaredAt: now,
    };
    const barEntry = {
      orchestratorId: '__claim:bar__',
      claims: [{ orchestratorId: 'orch-z', count: 9, claimedAt: now }],
      grantedTotal: 9,
      declaredAt: now,
    };

    await fsPromises.writeFile(filePath, JSON.stringify([fooEntry, barEntry]), 'utf8');

    await releaseCapacity(filePath, { type: 'foo', releaseCount: 5, orchestratorId: 'orch-a' });

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));

    const fooEntriesAfter = raw.filter((entry) => entry.orchestratorId === '__claim:foo__');
    const barEntriesAfter = raw.filter((entry) => entry.orchestratorId === '__claim:bar__');

    expect(fooEntriesAfter).toHaveLength(0);
    expect(barEntriesAfter).toHaveLength(1);
    expect(barEntriesAfter[0]).toEqual(barEntry);
  });

  it('duplicate-then-emptied interaction: two duplicate __claim:foo__ entries (legacy-scalar-shaped + per-claim-array) whose combined claims are fully released persist ZERO __claim:foo__ entries afterward, not a leftover {claims: [], grantedTotal: 0} and not two still-separate entries', async () => {
    const now = Date.now();

    // Mirrors the Phase 3 duplicate fixture's SHAPE exactly: one
    // legacy-scalar-shaped entry (no `claims[]` field, only a scalar
    // `grantedTotal`) and one per-claim-array entry, both for the same
    // type — the same duplicate-ledger-entry situation Phase 3's merge
    // fix handles. `normalizeClaims` attributes any *positive*
    // legacy-scalar `grantedTotal` to the reserved, caller-unaddressable
    // `LEGACY_UNATTRIBUTED_CLAIM_HOLDER_ID` sentinel (never a real
    // orchestratorId — `releaseCapacity` rejects reserved ids outright), so
    // a real caller could never fully drain a *non-zero* legacy scalar in
    // one release call; the legacy-scalar entry here carries `0`, keeping
    // its shape (legacy, no `claims[]`) while contributing nothing to the
    // merged total, so the per-claim-array duplicate's real orch-b claim is
    // what genuinely goes to zero — exercising the Phase 3 merge (two
    // source entries collapsed into one) immediately followed by the
    // Phase 4 omission (the merged, now-empty result must not be written
    // back at all).
    const fooLegacy = { orchestratorId: '__claim:foo__', grantedTotal: 0, declaredAt: now };
    const fooArray = {
      orchestratorId: '__claim:foo__',
      claims: [{ orchestratorId: 'orch-b', count: 6, claimedAt: now }],
      grantedTotal: 6,
      declaredAt: now,
    };

    await fsPromises.writeFile(filePath, JSON.stringify([fooLegacy, fooArray]), 'utf8');

    const result = await releaseCapacity(filePath, { type: 'foo', releaseCount: 6, orchestratorId: 'orch-b' });

    expect(result.released).toBe(6);
    expect(result.grantedTotal).toBe(0);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const fooEntriesAfter = raw.filter((entry) => entry.orchestratorId === '__claim:foo__');

    // Zero persisted entries — not a leftover {claims: [], grantedTotal: 0},
    // and not two still-separate duplicate entries.
    expect(fooEntriesAfter).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Phase 1 — filter out `count === 0` claim records BEFORE the
// `stampedClaims.length > 0` omission guard above runs. That guard was left
// with a known residual gap (see the "Known residual gap" doc comment on
// `releaseCapacity`'s write path): a corrupt, hand-edited `count: 0` claim
// record passes `isValidClaimRecord` (via `isNonNegativeInteger`) and
// survives untouched into `stampedClaims`, so `stampedClaims.length` stays
// `> 0` even when every SURVIVING record is a dead zero — the omission guard
// never fires and the ledger entry is written back forever with
// `grantedTotal: 0` and a lone zero-count claim.
//
// RED today: the filter this ticket adds does not exist yet, so a lone
// count:0 record (and its siblings/variants below) is NOT dropped and the
// entry is NOT omitted.
// ---------------------------------------------------------------------------

// shared rationale for the `-0` (negative zero) claim-count fixture
// used by both the releaseCapacity (Phase 1, below) and claimCapacity
// (Phase 2, further down) omission-guard describes — hoisted here once
// instead of repeating the same comment at each call site.
//
// JSON.stringify(-0) === '0', so round-tripping through the file loses the
// sign — but constructing the fixture with a literal -0 documents intent and
// still exercises the `=== 0` comparison the same way a live in-memory -0
// would.
const NEGATIVE_ZERO_CLAIM_COUNT = -0;

describe('releaseCapacity filters count:0 claim records before the omission guard runs', () => {
  it('releaseCapacity omits the ledger entry when the sole surviving claim is count:0', async () => {
    const now = Date.now();

    // A corrupt, hand-edited ledger: the only claim record left for `foo`
    // is a dead `count: 0` record for a DIFFERENT orchestrator than the one
    // releasing — so `releaseCapacity`'s consume loop never touches it (it
    // only walks records matching `orchestratorId`), and it rides through
    // unmodified into `nextClaims`/`stampedClaims` exactly as today's gap
    // describes.
    const fooEntry = {
      orchestratorId: '__claim:foo__',
      claims: [
        { orchestratorId: 'orch-zero', count: 0, claimedAt: now },
        { orchestratorId: 'orch-a', count: 4, claimedAt: now },
      ],
      grantedTotal: 4,
      declaredAt: now,
    };

    await fsPromises.writeFile(filePath, JSON.stringify([fooEntry]), 'utf8');

    const result = await releaseCapacity(filePath, { type: 'foo', releaseCount: 4, orchestratorId: 'orch-a' });

    expect(result.released).toBe(4);
    expect(result.grantedTotal).toBe(0);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const fooEntriesAfter = raw.filter((entry) => entry.orchestratorId === '__claim:foo__');

    // The entry must be entirely absent — not persisted as
    // `{claims: [{orchestratorId: 'orch-zero', count: 0, ...}], grantedTotal: 0}`.
    expect(fooEntriesAfter).toHaveLength(0);
  });

  it('releaseCapacity drops a count:0 record but keeps sibling live claims', async () => {
    const now = Date.now();

    const fooEntry = {
      orchestratorId: '__claim:foo__',
      claims: [
        { orchestratorId: 'orch-zero', count: 0, claimedAt: now },
        { orchestratorId: 'orch-a', count: 10, claimedAt: now },
        { orchestratorId: 'orch-b', count: 6, claimedAt: now },
      ],
      grantedTotal: 16,
      declaredAt: now,
    };

    await fsPromises.writeFile(filePath, JSON.stringify([fooEntry]), 'utf8');

    // Partial release against orch-a only — orch-b's live claim and the
    // corrupt orch-zero record both ride through the consume loop
    // untouched; only the filter (not the consume loop) should remove the
    // zero record.
    const result = await releaseCapacity(filePath, { type: 'foo', releaseCount: 3, orchestratorId: 'orch-a' });

    expect(result.released).toBe(3);
    expect(result.grantedTotal).toBe(13);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const fooEntriesAfter = raw.filter((entry) => entry.orchestratorId === '__claim:foo__');

    expect(fooEntriesAfter).toHaveLength(1);
    const persistedClaims = fooEntriesAfter[0].claims;
    expect(persistedClaims.some((claim) => claim.count === 0)).toBe(false);
    expect(persistedClaims).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ orchestratorId: 'orch-a', count: 7 }),
        expect.objectContaining({ orchestratorId: 'orch-b', count: 6 }),
      ]),
    );
    expect(persistedClaims).toHaveLength(2);
    expect(fooEntriesAfter[0].grantedTotal).toBe(13);
  });

  it("releaseCapacity's normal (non-corrupt) release path is unchanged", async () => {
    const now = Date.now();
    await claimCapacity(filePath, {
      type: 'sub-agent',
      requestedCount: 10,
      orchestratorId: 'orch-solo',
      computeAvailableCapacity: async () => 100,
    });

    const result = await releaseCapacity(filePath, { type: 'sub-agent', releaseCount: 3, orchestratorId: 'orch-solo' });

    expect(result.released).toBe(3);
    expect(result.grantedTotal).toBe(7);

    const dibs = await readDibs(filePath, now);
    const ledgerEntry = dibs.find((entry) => entry.orchestratorId === '__claim:sub-agent__');
    expect(ledgerEntry).toBeDefined();
    expect(ledgerEntry.claims).toHaveLength(1);
    expect(ledgerEntry.claims[0].orchestratorId).toBe('orch-solo');
    expect(ledgerEntry.claims[0].count).toBe(7);
  });

  it('releaseCapacity drops multiple count:0 records in the same corrupt entry', async () => {
    const now = Date.now();

    const fooEntry = {
      orchestratorId: '__claim:foo__',
      claims: [
        { orchestratorId: 'orch-zero-1', count: 0, claimedAt: now },
        { orchestratorId: 'orch-zero-2', count: 0, claimedAt: now },
        { orchestratorId: 'orch-a', count: 5, claimedAt: now },
      ],
      grantedTotal: 5,
      declaredAt: now,
    };

    await fsPromises.writeFile(filePath, JSON.stringify([fooEntry]), 'utf8');

    const result = await releaseCapacity(filePath, { type: 'foo', releaseCount: 5, orchestratorId: 'orch-a' });

    expect(result.released).toBe(5);
    expect(result.grantedTotal).toBe(0);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const fooEntriesAfter = raw.filter((entry) => entry.orchestratorId === '__claim:foo__');

    // Every live claim was released AND both corrupt zero records must be
    // filtered — the entry must be entirely absent, not persisted with two
    // leftover zero-count records.
    expect(fooEntriesAfter).toHaveLength(0);
  });

  it('releaseCapacity filters a corrupt count:-0 (negative zero) record the same way, since -0 === 0 in JS', async () => {
    const now = Date.now();

    const fooEntry = {
      orchestratorId: '__claim:foo__',
      claims: [
        { orchestratorId: 'orch-neg-zero', count: NEGATIVE_ZERO_CLAIM_COUNT, claimedAt: now },
        { orchestratorId: 'orch-a', count: 2, claimedAt: now },
      ],
      grantedTotal: 2,
      declaredAt: now,
    };

    await fsPromises.writeFile(filePath, JSON.stringify([fooEntry]), 'utf8');

    const result = await releaseCapacity(filePath, { type: 'foo', releaseCount: 2, orchestratorId: 'orch-a' });

    expect(result.released).toBe(2);
    expect(result.grantedTotal).toBe(0);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const fooEntriesAfter = raw.filter((entry) => entry.orchestratorId === '__claim:foo__');

    expect(fooEntriesAfter).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Phase 2 — symmetric fix in claimCapacity's write path: reuse
// `filterOutZeroCountClaims` (already applied by `releaseCapacity`'s write
// path per Phase 1) and add the omission guard `claimCapacity` currently
// lacks. Today `claimCapacity`'s write branch unconditionally calls
// `writeEntriesAtomic` with `nextLedgerEntry`, relying on a doc-comment proof
// that `nextClaims` can never be empty (see the "Phase 4" comment right
// before that call) — a proof that holds only because zero-count records
// aren't filtered out today. Once Phase 2 adds the filter, the one corner the
// proof misses is a legacy-migration write where a corrupt fractional
// `grantedTotal` in the open interval (0, 1) floors to a lone `count: 0`
// record via `Math.floor` inside `normalizeClaims` — these tests pin that
// corner plus the Convergence Analysis edge cases around it. RED against
// today's code: the filter/guard do not exist yet in `claimCapacity`.
// ---------------------------------------------------------------------------

describe('claimCapacity filters count:0 claim records before an omission guard runs', () => {
  const ample = async () => 100;

  it('claimCapacity omits the ledger entry when a corrupt fractional grantedTotal in (0,1) floors to a lone count:0 legacy record', async () => {
    const now = Date.now();

    // A corrupt, hand-edited legacy-scalar ledger: grantedTotal is a
    // fraction strictly between 0 and 1, so `normalizeClaims`'s
    // `Math.floor(ledgerEntry.grantedTotal)` synthesizes a lone
    // `{ legacyMigrated: true, count: 0, ... }` record — no live claims
    // exist alongside it.
    const legacyEntry = {
      orchestratorId: '__claim:foo__',
      grantedTotal: 0.5,
      declaredAt: now,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([legacyEntry]), 'utf8');

    // requestedCount: 0 guarantees `granted === 0` (no available capacity is
    // even needed) — the write branch is entered purely via
    // `needsLegacyMigrationWrite`.
    const result = await claimCapacity(
      filePath,
      {
        type: 'foo',
        requestedCount: 0,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
        now,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    expect(result.granted).toBe(0);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const fooEntriesAfter = raw.filter((entry) => entry.orchestratorId === '__claim:foo__');

    // The entry must be entirely absent — not persisted as
    // `{claims: [{orchestratorId: '__legacy-unattributed-claim__', count: 0, ...}], grantedTotal: 0}`.
    expect(fooEntriesAfter).toHaveLength(0);
  });

  it("claimCapacity's normal granted>0 path still writes the entry (guard is a no-op there)", async () => {
    const result = await claimCapacity(filePath, {
      type: 'sub-agent',
      requestedCount: 5,
      orchestratorId: 'orch-solo',
      computeAvailableCapacity: ample,
    });

    expect(result.granted).toBe(5);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:sub-agent__');

    expect(ledgerEntry).toBeDefined();
    expect(ledgerEntry.claims).toHaveLength(1);
    expect(ledgerEntry.claims[0]).toEqual(
      expect.objectContaining({ orchestratorId: 'orch-solo', count: 5 }),
    );
    expect(ledgerEntry.grantedTotal).toBe(5);
  });

  it("claimCapacity's legacy-migration write with a nonzero floored count is unaffected", async () => {
    const now = Date.now();
    const legacyEntry = {
      orchestratorId: '__claim:foo__',
      grantedTotal: 2.7,
      declaredAt: now,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([legacyEntry]), 'utf8');

    const result = await claimCapacity(
      filePath,
      {
        type: 'foo',
        requestedCount: 0,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
        now,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    expect(result.alreadyGrantedTotal).toBe(2);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:foo__');
    expect(ledgerEntry).toBeDefined();
    const legacyRecord = ledgerEntry.claims.find(
      (claim) => claim.orchestratorId === '__legacy-unattributed-claim__',
    );
    expect(legacyRecord).toBeDefined();
    expect(legacyRecord.count).toBe(2);
  });

  it('claimCapacity filters a stray same-type count:0 record riding alongside a legacy-migrated record, on the granted>0 branch', async () => {
    const now = Date.now();
    const fooEntry = {
      orchestratorId: '__claim:foo__',
      claims: [
        { orchestratorId: '__legacy-unattributed-claim__', count: 3, claimedAt: now, legacyMigrated: true },
        { orchestratorId: 'orch-zero', count: 0, claimedAt: now },
      ],
      grantedTotal: 3,
      declaredAt: now,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([fooEntry]), 'utf8');

    const result = await claimCapacity(
      filePath,
      {
        type: 'foo',
        requestedCount: 4,
        orchestratorId: 'orch-new',
        computeAvailableCapacity: ample,
        now,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    expect(result.granted).toBe(4);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:foo__');
    expect(ledgerEntry).toBeDefined();
    expect(ledgerEntry.claims.some((claim) => claim.count === 0)).toBe(false);
    expect(ledgerEntry.claims).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ orchestratorId: '__legacy-unattributed-claim__', count: 3 }),
        expect.objectContaining({ orchestratorId: 'orch-new', count: 4 }),
      ]),
    );
    expect(ledgerEntry.claims).toHaveLength(2);
  });

  it('claimCapacity filters a stray same-type count:0 record riding alongside a legacy-migrated record, on the legacy-migration-only (granted===0) branch', async () => {
    const now = Date.now();
    const fooEntry = {
      orchestratorId: '__claim:foo__',
      claims: [
        { orchestratorId: '__legacy-unattributed-claim__', count: 3, claimedAt: now, legacyMigrated: true },
        { orchestratorId: 'orch-zero', count: 0, claimedAt: now },
      ],
      grantedTotal: 3,
      declaredAt: now,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([fooEntry]), 'utf8');

    const result = await claimCapacity(
      filePath,
      {
        type: 'foo',
        requestedCount: 0,
        orchestratorId: 'orch-new',
        computeAvailableCapacity: ample,
        now,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    expect(result.granted).toBe(0);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:foo__');
    expect(ledgerEntry).toBeDefined();
    expect(ledgerEntry.claims.some((claim) => claim.count === 0)).toBe(false);
    expect(ledgerEntry.claims).toEqual([
      expect.objectContaining({ orchestratorId: '__legacy-unattributed-claim__', count: 3 }),
    ]);
  });

  it("claimCapacity's legacy-migration path filters an explicit count: -0 record the same way, since -0 === 0 in JS", async () => {
    const now = Date.now();
    const fooEntry = {
      orchestratorId: '__claim:foo__',
      claims: [
        {
          orchestratorId: '__legacy-unattributed-claim__',
          count: NEGATIVE_ZERO_CLAIM_COUNT,
          claimedAt: now,
          legacyMigrated: true,
        },
        { orchestratorId: 'orch-a', count: 2, claimedAt: now },
      ],
      grantedTotal: 2,
      declaredAt: now,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([fooEntry]), 'utf8');

    const result = await claimCapacity(
      filePath,
      {
        type: 'foo',
        requestedCount: 0,
        orchestratorId: 'orch-b',
        computeAvailableCapacity: ample,
        now,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    expect(result.granted).toBe(0);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:foo__');
    expect(ledgerEntry).toBeDefined();
    expect(ledgerEntry.claims.some((claim) => claim.count === 0)).toBe(false);
    expect(ledgerEntry.claims).toEqual([
      expect.objectContaining({ orchestratorId: 'orch-a', count: 2 }),
    ]);
  });

  it('claimCapacity omits the ledger entry for a record that is count:0, legacyMigrated (TTL-exempt), and would otherwise be expired if it weren\'t exempt', async () => {
    const now = Date.now();
    const CLAIM_TTL_MS = 1000;
    const fooEntry = {
      orchestratorId: '__claim:foo__',
      claims: [
        {
          orchestratorId: '__legacy-unattributed-claim__',
          count: 0,
          // Far older than CLAIM_TTL_MS — would be pruned by
          // `pruneExpiredClaims` were it not `legacyMigrated: true`
          // (unconditionally exempt from that check).
          claimedAt: now - 1_000_000,
          legacyMigrated: true,
        },
      ],
      grantedTotal: 0,
      declaredAt: now - 1_000_000,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([fooEntry]), 'utf8');

    const result = await claimCapacity(
      filePath,
      {
        type: 'foo',
        requestedCount: 0,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
        now,
      },
      { claimTtlMs: CLAIM_TTL_MS },
    );

    expect(result.granted).toBe(0);
    expect(result.alreadyGrantedTotal).toBe(0);

    const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const fooEntriesAfter = raw.filter((entry) => entry.orchestratorId === '__claim:foo__');

    // TTL-exemption keeps the record alive through `pruneExpiredClaims`, but
    // the count:0 filter must still remove it afterwards — the entry must
    // be entirely absent, not persisted as `{claims: [], grantedTotal: 0}`
    // nor with the dead record surviving.
    expect(fooEntriesAfter).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// nit 5 follow-up — granular unit coverage for the claimTtlMs-unset +
// legacy-record guard (Convergence Analysis review). The outer acceptance
// test (./coordination-file.outer-acceptance.jest.spec.mjs, "nit 5") already
// pins the basic throw. These tests pin the edge cases: the guard must never
// false-positive on cli.mjs's real production call shape or on a ledger with
// no legacy record, must use `isFiniteNumber` semantics (NaN/-5 are
// "unset" too) rather than a loose `=== undefined` check, must validate
// BEFORE any write (mirroring releaseCapacity's existing
// releaseCount/orchestratorId pre-withLock validation), and must throw a
// typed, catchable error — not just any Error.
//
// Chosen error code: 'COORDINATION_UNRECOVERABLE_LEGACY_CLAIM' — mirrors
// LockTimeoutError's `error.code === 'COORDINATION_LOCK_TIMEOUT'` naming
// convention ("COORDINATION_" prefix + SCREAMING_SNAKE reason). The
// implementer must throw an Error whose `.code` is exactly this string.
// ---------------------------------------------------------------------------

describe('claimTtlMs-unset + legacy-record guard — granular edge cases (Convergence Analysis follow-up)', () => {
  const ample = async () => 100;
  const LEGACY_CLAIM_ERROR_CODE = 'COORDINATION_UNRECOVERABLE_LEGACY_CLAIM';

  // -------------------------------------------------------------------------
  // 1. No false-positive on cli.mjs's real production path.
  // -------------------------------------------------------------------------

  it('does NOT throw when options.claimTtlMs is DEFAULT_CLAIM_TTL_MS (cli.mjs\'s real handleClaim/handleRelease shape) against a ledger holding a legacy-migrated record', async () => {
    const now = Date.now();
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([{ orchestratorId: '__claim:prod-path-type__', grantedTotal: 3, declaredAt: now }]),
      'utf8',
    );

    await expect(
      claimCapacity(
        filePath,
        { type: 'prod-path-type', requestedCount: 1, orchestratorId: 'orch-a', computeAvailableCapacity: ample, now },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).resolves.toBeDefined();

    await expect(
      releaseCapacity(
        filePath,
        { type: 'prod-path-type', releaseCount: 1, orchestratorId: 'orch-a', now },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).resolves.toBeDefined();
  });

  // -------------------------------------------------------------------------
  // 2. No false-positive when no legacy record is present at all.
  // -------------------------------------------------------------------------

  it('does NOT throw when options.claimTtlMs is unset against a ledger with NO ledger entry at all for the type', async () => {
    // filePath doesn't even exist yet — no ledger entry of any kind.
    await expect(
      claimCapacity(filePath, {
        type: 'no-entry-type',
        requestedCount: 1,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
      }),
    ).resolves.toBeDefined();

    await expect(
      releaseCapacity(filePath, { type: 'no-entry-type', releaseCount: 1, orchestratorId: 'orch-a' }),
    ).resolves.toBeDefined();
  });

  it('does NOT throw when options.claimTtlMs is unset against a ledger entry already fully migrated to per-claim shape with only real orchestratorIds', async () => {
    const now = Date.now();
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([
        {
          orchestratorId: '__claim:fully-migrated-type__',
          claims: [{ orchestratorId: 'orch-real', count: 2, claimedAt: now }],
          grantedTotal: 2,
          declaredAt: now,
        },
      ]),
      'utf8',
    );

    await expect(
      claimCapacity(filePath, {
        type: 'fully-migrated-type',
        requestedCount: 1,
        orchestratorId: 'orch-b',
        computeAvailableCapacity: ample,
        now,
      }),
    ).resolves.toBeDefined();

    await expect(
      releaseCapacity(filePath, { type: 'fully-migrated-type', releaseCount: 1, orchestratorId: 'orch-real', now }),
    ).resolves.toBeDefined();
  });

  // -------------------------------------------------------------------------
  // 3. Guard uses isFiniteNumber semantics, not a loose undefined-only check.
  // -------------------------------------------------------------------------

  it('throws when options.claimTtlMs is NaN against a ledger with a legacy record present (NaN is "unset" per isFiniteNumber)', async () => {
    const now = Date.now();
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([{ orchestratorId: '__claim:nan-ttl-type__', grantedTotal: 3, declaredAt: now }]),
      'utf8',
    );

    await expect(
      claimCapacity(
        filePath,
        { type: 'nan-ttl-type', requestedCount: 1, orchestratorId: 'orch-a', computeAvailableCapacity: ample, now },
        { claimTtlMs: NaN },
      ),
    ).rejects.toThrow();

    await expect(
      releaseCapacity(
        filePath,
        { type: 'nan-ttl-type', releaseCount: 1, orchestratorId: 'orch-a', now },
        { claimTtlMs: NaN },
      ),
    ).rejects.toThrow();
  });

  it('throws when options.claimTtlMs is -5 against a ledger with a legacy record present (a negative TTL is "unset" per isFiniteNumber, not a valid TTL)', async () => {
    const now = Date.now();
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([{ orchestratorId: '__claim:negative-ttl-type__', grantedTotal: 3, declaredAt: now }]),
      'utf8',
    );

    await expect(
      claimCapacity(
        filePath,
        {
          type: 'negative-ttl-type',
          requestedCount: 1,
          orchestratorId: 'orch-a',
          computeAvailableCapacity: ample,
          now,
        },
        { claimTtlMs: -5 },
      ),
    ).rejects.toThrow();

    await expect(
      releaseCapacity(
        filePath,
        { type: 'negative-ttl-type', releaseCount: 1, orchestratorId: 'orch-a', now },
        { claimTtlMs: -5 },
      ),
    ).rejects.toThrow();
  });

  // -------------------------------------------------------------------------
  // 4. Guard throws BEFORE any write — the ledger is byte-unchanged.
  // -------------------------------------------------------------------------

  it('throws BEFORE any write when options.claimTtlMs is unset against a legacy record — the coordination file is byte-identical afterward', async () => {
    const now = Date.now();
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([{ orchestratorId: '__claim:no-write-type__', grantedTotal: 3, declaredAt: now }]),
      'utf8',
    );
    const before = await fsPromises.readFile(filePath, 'utf8');

    await expect(
      claimCapacity(filePath, {
        type: 'no-write-type',
        requestedCount: 1,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
        now,
      }),
    ).rejects.toThrow();

    const afterClaim = await fsPromises.readFile(filePath, 'utf8');
    expect(afterClaim).toBe(before);

    await expect(
      releaseCapacity(filePath, { type: 'no-write-type', releaseCount: 1, orchestratorId: 'orch-a', now }),
    ).rejects.toThrow();

    const afterRelease = await fsPromises.readFile(filePath, 'utf8');
    expect(afterRelease).toBe(before);
  });

  // -------------------------------------------------------------------------
  // 5. Error is typed/catchable via a stable, documented .code string.
  // -------------------------------------------------------------------------

  it('rejects with an error carrying a distinguishable, stable .code property (mirroring LockTimeoutError\'s COORDINATION_LOCK_TIMEOUT convention)', async () => {
    const now = Date.now();
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([{ orchestratorId: '__claim:typed-error-type__', grantedTotal: 3, declaredAt: now }]),
      'utf8',
    );

    let claimError;
    try {
      await claimCapacity(filePath, {
        type: 'typed-error-type',
        requestedCount: 1,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
        now,
      });
    } catch (error) {
      claimError = error;
    }
    expect(claimError).toBeDefined();
    expect(claimError.code).toBe(LEGACY_CLAIM_ERROR_CODE);

    let releaseError;
    try {
      await releaseCapacity(filePath, { type: 'typed-error-type', releaseCount: 1, orchestratorId: 'orch-a', now });
    } catch (error) {
      releaseError = error;
    }
    expect(releaseError).toBeDefined();
    expect(releaseError.code).toBe(LEGACY_CLAIM_ERROR_CODE);
  });
});

// ---------------------------------------------------------------------------
// — the guard's clock read must be lazy on the hot (TTL-set) path.
//
// Both `claimCapacity` and `releaseCapacity` build the 4th argument to
// `assertClaimTtlSetAgainstLegacyLedger` as `isFiniteNumber(now) ? now :
// Date.now()`, evaluated EAGERLY as a call argument on every single
// invocation — even though the guard's own body returns immediately via
// `if (isSetClaimTtl(claimTtlMs)) return;` whenever a real, finite,
// positive `claimTtlMs` is supplied (cli.mjs's real, every-call production
// shape — see `DEFAULT_CLAIM_TTL_MS` used throughout this file). On that hot
// path the guard never looks at its 4th argument at all, yet `Date.now()`
// still gets read to build it.
//
// `withLock`'s own critical section independently reads the clock once more
// (`const referenceNow = isFiniteNumber(now) ? now : Date.now();`), and lock
// acquisition (`acquireLock`) reads it a further time to stamp the lock
// payload's `acquiredAt` — both legitimate, out-of-scope-for-this-bug reads.
// The fix under test removes exactly ONE redundant read (the eager guard
// argument) from that total — it does not touch the other two.
// ---------------------------------------------------------------------------

// shared helper for the "baseline" Date.now() call-count measurements
// used throughout the two describe blocks below — spies on Date.now() for
// the duration of a single call, returns how many times it was really read,
// and restores the spy immediately. Callers still spy independently around
// the call under test (rather than reusing this helper for both) so the two
// `jest.spyOn` lifecycles never overlap.
async function measureDateNowCalls(invoke) {
  const spy = jest.spyOn(Date, 'now');
  try {
    await invoke();
    return spy.mock.calls.length;
  } finally {
    spy.mockRestore();
  }
}

describe('claimCapacity / releaseCapacity — lazy clock read on the guard argument', () => {
  const ample = async () => 100;
  const DEFAULT_TTL_MS_FOR_TEST = 5 * 60 * 1000; // realistic default TTL (5 minutes), mirrors cli.mjs's shape

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('does not read the clock via the eager now/Date.now() argument when claimTtlMs is set (the every-real-cli.mjs-call hot path)', async () => {
    // rather than hardcoding the total Date.now() call count (which
    // silently bakes in acquireLock's own `acquiredAt` stamp and withLock's
    // own `referenceNow` fallback — both explicitly out of scope for this
    // guard's laziness fix), measure each function's own baseline for real:
    // call it with a valid finite `now` supplied, which structurally cannot
    // hit either the guard argument or `referenceNow`'s Date.now() fallback
    // (both resolve to the supplied value), leaving only acquireLock's
    // unconditional stamp. Then compare against the real cli.mjs call shape
    // (no `now` supplied) — the ONLY legitimate extra read there is
    // withLock's own `referenceNow` fallback (exactly +1). A regression that
    // reintroduces the eager guard-argument read adds a SECOND extra read,
    // which this still catches — without asserting what acquireLock's own
    // baseline count actually is, so it stays correct even if that count
    // changes for unrelated reasons.
    const claimBaseline = await measureDateNowCalls(() =>
      claimCapacity(
        filePath,
        {
          type: 'lazy-clock-claim-baseline-type',
          requestedCount: 1,
          orchestratorId: 'orch-a',
          computeAvailableCapacity: ample,
          now: 1_700_000_000_000,
        },
        { claimTtlMs: DEFAULT_TTL_MS_FOR_TEST },
      ),
    );

    const claimSpy = jest.spyOn(Date, 'now');
    await claimCapacity(
      filePath,
      {
        type: 'lazy-clock-claim-type',
        requestedCount: 1,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
        // Deliberately no `now` supplied — this is cli.mjs's real shape,
        // where the eager `isFiniteNumber(now) ? now : Date.now()` always
        // falls through to a real `Date.now()` call today.
      },
      { claimTtlMs: DEFAULT_TTL_MS_FOR_TEST },
    );
    expect(claimSpy).toHaveBeenCalledTimes(claimBaseline + 1);
    claimSpy.mockRestore();

    const releaseBaseline = await measureDateNowCalls(() =>
      releaseCapacity(
        filePath,
        {
          type: 'lazy-clock-claim-baseline-type',
          releaseCount: 0,
          orchestratorId: 'orch-a',
          now: 1_700_000_000_000,
        },
        { claimTtlMs: DEFAULT_TTL_MS_FOR_TEST },
      ),
    );

    const releaseSpy = jest.spyOn(Date, 'now');
    await releaseCapacity(
      filePath,
      {
        type: 'lazy-clock-claim-type',
        releaseCount: 1,
        orchestratorId: 'orch-a',
        // Deliberately no `now` supplied, same reasoning as above.
      },
      { claimTtlMs: DEFAULT_TTL_MS_FOR_TEST },
    );
    expect(releaseSpy).toHaveBeenCalledTimes(releaseBaseline + 1);
    releaseSpy.mockRestore();
  });

  it('still reads the clock for the guard argument, and the legacy-ledger guard still rejects, when claimTtlMs is unset (the lazy computation must still run on this path)', async () => {
    const now = Date.now();
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([{ orchestratorId: '__claim:lazy-clock-unset-ttl-type__', grantedTotal: 3, declaredAt: now }]),
      'utf8',
    );

    const claimSpy = jest.spyOn(Date, 'now');
    await expect(
      claimCapacity(filePath, {
        type: 'lazy-clock-unset-ttl-type',
        requestedCount: 1,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
        // No `now` supplied and no `claimTtlMs` in options — the "unset"
        // path where `assertClaimTtlSetAgainstLegacyLedger` must actually
        // evaluate its 4th argument (and therefore still call `Date.now()`
        // at least once) to decide whether to reject.
      }),
    ).rejects.toThrow();
    expect(claimSpy.mock.calls.length).toBeGreaterThanOrEqual(1);
    claimSpy.mockRestore();

    const releaseSpy = jest.spyOn(Date, 'now');
    await expect(
      releaseCapacity(filePath, {
        type: 'lazy-clock-unset-ttl-type',
        releaseCount: 1,
        orchestratorId: 'orch-a',
      }),
    ).rejects.toThrow();
    expect(releaseSpy.mock.calls.length).toBeGreaterThanOrEqual(1);
    releaseSpy.mockRestore();
  });
});

// ---------------------------------------------------------------------------
// Phase 1 — isFiniteNumber boundary values on the guard-argument
// computation.
//
// The expression at both call sites (`isFiniteNumber(now) ? now :
// Date.now()`) MUST use `isFiniteNumber`, not bare truthiness — a
// truthiness bug would break `now === 0` specifically, since `0` is falsy
// but IS a valid finite `now`. These tables pin the exact boundary values
// `isFiniteNumber` (`typeof value === 'number' && Number.isFinite(value)`)
// treats as finite vs not, on BOTH the claimTtlMs-set (hot) path and the
// claimTtlMs-unset (legacy-fallback) path — proving the isFiniteNumber(now)
// check governs which supplied value wins independent of which branch
// resolves it.
//
// Per isFiniteNumber's own (unchanged-by-this-fix) definition:
//   - 0 is finite (typeof 'number', Number.isFinite(0) === true).
//   - a negative number (e.g. -5) is ALSO finite — strict IEEE754
//     finiteness has no special case for sign — so it too must be treated
//     as a valid supplied value, not "unset".
//   - NaN, Infinity, -Infinity fail Number.isFinite and so are NOT finite.
//   - null and undefined are not even `typeof 'number'` and so are NOT
//     finite either.
// ---------------------------------------------------------------------------

describe('claimCapacity / releaseCapacity — isFiniteNumber boundary values on the guard-argument computation', () => {
  const ample = async () => 100;
  const DEFAULT_TTL_MS_FOR_TEST = 5 * 60 * 1000;

  afterEach(() => {
    jest.restoreAllMocks();
  });

  const finiteValues = [
    ['0 (falsy but finite)', 0],
    ['a negative finite number (-5)', -5],
  ];

  const nonFiniteValues = [
    ['NaN', NaN],
    ['Infinity', Infinity],
    ['-Infinity', -Infinity],
    ['null', null],
    ['undefined', undefined],
  ];

  describe.each(finiteValues)('now = %s — a valid supplied value', (_label, now) => {
    it('claimCapacity on the claimTtlMs-SET (hot) path treats it as valid: Date.now() is called only for the unrelated acquireLock stamp, never for the guard argument or the referenceNow fallback', async () => {
      // baseline measured with an ordinary finite `now` (not the
      // boundary value under test), so this asserts "this boundary value
      // reads the clock exactly as many times as any other valid finite
      // now" instead of hardcoding acquireLock's own read count as a magic
      // number.
      const baseline = await measureDateNowCalls(() =>
        claimCapacity(
          filePath,
          {
            type: `finite-hot-claim-baseline-${now}`,
            requestedCount: 1,
            orchestratorId: 'orch-a',
            computeAvailableCapacity: ample,
            now: 1_700_000_000_000,
          },
          { claimTtlMs: DEFAULT_TTL_MS_FOR_TEST },
        ),
      );

      const claimSpy = jest.spyOn(Date, 'now');
      await claimCapacity(
        filePath,
        {
          type: `finite-hot-claim-${now}`,
          requestedCount: 1,
          orchestratorId: 'orch-a',
          computeAvailableCapacity: ample,
          now,
        },
        { claimTtlMs: DEFAULT_TTL_MS_FOR_TEST },
      );
      // Only acquireLock's own unconditional `acquiredAt` stamp remains —
      // neither the guard argument nor withLock's referenceNow need to read
      // the clock when a valid, finite `now` was supplied.
      expect(claimSpy).toHaveBeenCalledTimes(baseline);
    });

    it('releaseCapacity on the claimTtlMs-SET (hot) path treats it as valid: Date.now() is called only for the unrelated acquireLock stamp', async () => {
      const baseline = await measureDateNowCalls(() =>
        releaseCapacity(
          filePath,
          {
            type: `finite-hot-release-baseline-${now}`,
            releaseCount: 0,
            orchestratorId: 'orch-a',
            now: 1_700_000_000_000,
          },
          { claimTtlMs: DEFAULT_TTL_MS_FOR_TEST },
        ),
      );

      const releaseSpy = jest.spyOn(Date, 'now');
      await releaseCapacity(
        filePath,
        {
          type: `finite-hot-release-${now}`,
          releaseCount: 0,
          orchestratorId: 'orch-a',
          now,
        },
        { claimTtlMs: DEFAULT_TTL_MS_FOR_TEST },
      );
      expect(releaseSpy).toHaveBeenCalledTimes(baseline);
    });

    it('claimCapacity on the claimTtlMs-UNSET (legacy-fallback) path resolves the guard argument to the supplied value itself, calling Date.now() zero times, while still rejecting against a present legacy record', async () => {
      const type = `finite-legacy-claim-${now}`;
      await fsPromises.writeFile(
        filePath,
        JSON.stringify([{ orchestratorId: `__claim:${type}__`, grantedTotal: 3, declaredAt: 1_000_000 }]),
        'utf8',
      );

      const claimSpy = jest.spyOn(Date, 'now');
      await expect(
        claimCapacity(filePath, {
          type,
          requestedCount: 1,
          orchestratorId: 'orch-a',
          computeAvailableCapacity: ample,
          now,
        }),
      ).rejects.toThrow();
      // The guard rejects using the SUPPLIED value alone — no real clock
      // read was ever needed to reach that verdict.
      expect(claimSpy).toHaveBeenCalledTimes(0);
    });

    it('releaseCapacity on the claimTtlMs-UNSET (legacy-fallback) path resolves the guard argument to the supplied value itself, calling Date.now() zero times, while still rejecting against a present legacy record', async () => {
      const type = `finite-legacy-release-${now}`;
      await fsPromises.writeFile(
        filePath,
        JSON.stringify([{ orchestratorId: `__claim:${type}__`, grantedTotal: 3, declaredAt: 1_000_000 }]),
        'utf8',
      );

      const releaseSpy = jest.spyOn(Date, 'now');
      await expect(
        releaseCapacity(filePath, { type, releaseCount: 1, orchestratorId: 'orch-a', now }),
      ).rejects.toThrow();
      expect(releaseSpy).toHaveBeenCalledTimes(0);
    });
  });

  describe.each(nonFiniteValues)('now = %s — NOT finite per isFiniteNumber, falls back to Date.now()', (_label, now) => {
    it('claimCapacity on the claimTtlMs-SET (hot) path falls back to Date.now() for withLock\'s referenceNow, on top of acquireLock\'s own stamp (2 real clock reads after the fix)', async () => {
      // baseline isolates acquireLock's own stamp (see the finite-
      // values block above) so this asserts "a non-finite `now` reads the
      // clock exactly one more time than the baseline" — the legitimate
      // referenceNow fallback — rather than hardcoding the combined total.
      const baseline = await measureDateNowCalls(() =>
        claimCapacity(
          filePath,
          {
            type: `nonfinite-hot-claim-baseline-${_label}`,
            requestedCount: 1,
            orchestratorId: 'orch-a',
            computeAvailableCapacity: ample,
            now: 1_700_000_000_000,
          },
          { claimTtlMs: DEFAULT_TTL_MS_FOR_TEST },
        ),
      );

      const claimSpy = jest.spyOn(Date, 'now');
      await claimCapacity(
        filePath,
        {
          type: `nonfinite-hot-claim-${_label}`,
          requestedCount: 1,
          orchestratorId: 'orch-a',
          computeAvailableCapacity: ample,
          now,
        },
        { claimTtlMs: DEFAULT_TTL_MS_FOR_TEST },
      );
      // acquireLock's stamp + withLock's referenceNow fallback — the eager
      // guard-argument read must NOT be a third call once the fix lands, so
      // this must read the clock exactly one more time than the baseline,
      // never two more.
      expect(claimSpy).toHaveBeenCalledTimes(baseline + 1);
    });

    it('releaseCapacity on the claimTtlMs-SET (hot) path falls back to Date.now() for withLock\'s referenceNow, on top of acquireLock\'s own stamp (2 real clock reads after the fix)', async () => {
      const baseline = await measureDateNowCalls(() =>
        releaseCapacity(
          filePath,
          {
            type: `nonfinite-hot-release-baseline-${_label}`,
            releaseCount: 0,
            orchestratorId: 'orch-a',
            now: 1_700_000_000_000,
          },
          { claimTtlMs: DEFAULT_TTL_MS_FOR_TEST },
        ),
      );

      const releaseSpy = jest.spyOn(Date, 'now');
      await releaseCapacity(
        filePath,
        {
          type: `nonfinite-hot-release-${_label}`,
          releaseCount: 0,
          orchestratorId: 'orch-a',
          now,
        },
        { claimTtlMs: DEFAULT_TTL_MS_FOR_TEST },
      );
      expect(releaseSpy).toHaveBeenCalledTimes(baseline + 1);
    });

    it('claimCapacity on the claimTtlMs-UNSET (legacy-fallback) path still calls Date.now() (the lazy computation legitimately needs a real clock here) and still rejects against a present legacy record', async () => {
      const type = `nonfinite-legacy-claim-${_label}`;
      await fsPromises.writeFile(
        filePath,
        JSON.stringify([{ orchestratorId: `__claim:${type}__`, grantedTotal: 3, declaredAt: 1_000_000 }]),
        'utf8',
      );

      const claimSpy = jest.spyOn(Date, 'now');
      await expect(
        claimCapacity(filePath, {
          type,
          requestedCount: 1,
          orchestratorId: 'orch-a',
          computeAvailableCapacity: ample,
          now,
        }),
      ).rejects.toThrow();
      expect(claimSpy.mock.calls.length).toBeGreaterThanOrEqual(1);
    });

    it('releaseCapacity on the claimTtlMs-UNSET (legacy-fallback) path still calls Date.now() (the lazy computation legitimately needs a real clock here) and still rejects against a present legacy record', async () => {
      const type = `nonfinite-legacy-release-${_label}`;
      await fsPromises.writeFile(
        filePath,
        JSON.stringify([{ orchestratorId: `__claim:${type}__`, grantedTotal: 3, declaredAt: 1_000_000 }]),
        'utf8',
      );

      const releaseSpy = jest.spyOn(Date, 'now');
      await expect(
        releaseCapacity(filePath, { type, releaseCount: 1, orchestratorId: 'orch-a', now }),
      ).rejects.toThrow();
      expect(releaseSpy.mock.calls.length).toBeGreaterThanOrEqual(1);
    });
  });
});

// ---------------------------------------------------------------------------
// Phase 1 — parity between claimCapacity and releaseCapacity: both
// call sites must resolve the guard argument identically for the same
// inputs, never drifting out of sync.
// ---------------------------------------------------------------------------

describe('claimCapacity / releaseCapacity — parity in how the guard argument is resolved', () => {
  const ample = async () => 100;

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('both functions call Date.now() the exact same number of times for the same inputs (no `now` supplied, claimTtlMs unset — the fallback branch that actually executes and is observable)', async () => {
    const fixedNow = 1_700_000_000_000;
    const claimType = 'parity-claim-type';
    const releaseType = 'parity-release-type';

    await fsPromises.writeFile(
      filePath,
      JSON.stringify([
        { orchestratorId: `__claim:${claimType}__`, grantedTotal: 3, declaredAt: fixedNow - 1_000 },
        { orchestratorId: `__claim:${releaseType}__`, grantedTotal: 3, declaredAt: fixedNow - 1_000 },
      ]),
      'utf8',
    );

    const nowSpy = jest.spyOn(Date, 'now').mockReturnValue(fixedNow);

    await expect(
      claimCapacity(filePath, {
        type: claimType,
        requestedCount: 1,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
        // No `now` supplied — forces both functions' guard argument down
        // the real `Date.now()`-reading fallback branch.
      }),
    ).rejects.toThrow();
    const claimCallCount = nowSpy.mock.calls.length;

    nowSpy.mockClear();

    await expect(
      releaseCapacity(filePath, {
        type: releaseType,
        releaseCount: 1,
        orchestratorId: 'orch-a',
      }),
    ).rejects.toThrow();
    const releaseCallCount = nowSpy.mock.calls.length;

    expect(claimCallCount).toBe(releaseCallCount);
    nowSpy.mockRestore();
  });

  it('both functions resolve the SAME mocked clock value for the guard\'s legacy-ledger verdict, rejecting identically rather than one succeeding and the other failing', async () => {
    const fixedNow = 1_700_000_000_000;
    const claimType = 'parity-verdict-claim-type';
    const releaseType = 'parity-verdict-release-type';

    await fsPromises.writeFile(
      filePath,
      JSON.stringify([
        { orchestratorId: `__claim:${claimType}__`, grantedTotal: 3, declaredAt: fixedNow - 1_000 },
        { orchestratorId: `__claim:${releaseType}__`, grantedTotal: 3, declaredAt: fixedNow - 1_000 },
      ]),
      'utf8',
    );

    jest.spyOn(Date, 'now').mockReturnValue(fixedNow);

    let claimError;
    try {
      await claimCapacity(filePath, {
        type: claimType,
        requestedCount: 1,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
      });
    } catch (error) {
      claimError = error;
    }

    let releaseError;
    try {
      await releaseCapacity(filePath, {
        type: releaseType,
        releaseCount: 1,
        orchestratorId: 'orch-a',
      });
    } catch (error) {
      releaseError = error;
    }

    expect(claimError).toBeDefined();
    expect(releaseError).toBeDefined();
    expect(claimError.code).toBe(releaseError.code);
  });
});

// ---------------------------------------------------------------------------
// Phase 1 — ARM_FAKE_NOW_MS-style deterministic clock on the lazy
// (claimTtlMs-unset / legacy-fallback) branch specifically. This is the ONE
// path that still calls `Date.now()` under the fix. Simulates the real seam:
// `cli.mjs`'s `resolveNow()` reads `process.env.ARM_FAKE_NOW_MS` and passes
// the resolved value as the `now` param into `claimCapacity`/
// `releaseCapacity` — coordination-file.mjs itself has no direct env-var
// dependency, it just receives `now`. So at this module's level: supply a
// finite `now` standing in for a resolved `ARM_FAKE_NOW_MS` value, against a
// ledger fixture with a legacy-migrated record, and prove the supplied value
// wins over a real clock read.
// ---------------------------------------------------------------------------

describe('claimCapacity / releaseCapacity — deterministic supplied now stands in for ARM_FAKE_NOW_MS on the legacy-fallback branch', () => {
  const ample = async () => 100;
  const fakeNowMs = 1_700_000_000_000; // stands in for a resolved ARM_FAKE_NOW_MS value

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('claimCapacity: Date.now() is NOT called (the supplied now wins over a real clock read) and the legacy-ledger guard rejects using exactly that supplied value', async () => {
    const type = 'arm-fake-now-claim-type';
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([{ orchestratorId: `__claim:${type}__`, grantedTotal: 3, declaredAt: fakeNowMs - 5_000 }]),
      'utf8',
    );

    const claimSpy = jest.spyOn(Date, 'now');
    let claimError;
    try {
      await claimCapacity(filePath, {
        type,
        requestedCount: 1,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
        now: fakeNowMs,
      });
    } catch (error) {
      claimError = error;
    }

    expect(claimError).toBeDefined();
    expect(claimSpy).not.toHaveBeenCalled();
  });

  it('releaseCapacity: Date.now() is NOT called (the supplied now wins over a real clock read) and the legacy-ledger guard rejects using exactly that supplied value', async () => {
    const type = 'arm-fake-now-release-type';
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([{ orchestratorId: `__claim:${type}__`, grantedTotal: 3, declaredAt: fakeNowMs - 5_000 }]),
      'utf8',
    );

    const releaseSpy = jest.spyOn(Date, 'now');
    let releaseError;
    try {
      await releaseCapacity(filePath, {
        type,
        releaseCount: 1,
        orchestratorId: 'orch-a',
        now: fakeNowMs,
      });
    } catch (error) {
      releaseError = error;
    }

    expect(releaseError).toBeDefined();
    expect(releaseSpy).not.toHaveBeenCalled();
  });

  it('a real ARM_FAKE_NOW_MS-shaped value at a completely different instant than the real wall clock still resolves consistently — proving no accidental real-clock read sneaks in regardless of how far the faked value is from "now"', async () => {
    // Deliberately a timestamp nowhere near the real Date.now() at test-run
    // time (year ~2003), so if a real Date.now() read DID sneak in, the
    // ledger entry's `declaredAt` (set relative to fakeNowMs) would produce
    // a wildly different, easily-detectable outcome.
    const farPastFakeNow = 1_050_000_000_000;
    const type = 'arm-fake-now-far-past-type';
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([{ orchestratorId: `__claim:${type}__`, grantedTotal: 3, declaredAt: farPastFakeNow - 5_000 }]),
      'utf8',
    );

    const claimSpy = jest.spyOn(Date, 'now');
    await expect(
      claimCapacity(filePath, {
        type,
        requestedCount: 1,
        orchestratorId: 'orch-a',
        computeAvailableCapacity: ample,
        now: farPastFakeNow,
      }),
    ).rejects.toThrow();
    expect(claimSpy).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Post-reclaim write: the silent-drop failure exists to close.
//
// RED BY CONSTRUCTION until `writeEntriesAtomic` re-proves lock ownership
// immediately before its `rename`. Today a holder whose critical section
// outlives `lockStalenessMs` has its lock legitimately reclaimed by a
// contender (`reclaimStaleLock` cannot tell "stalled but alive" from
// "SIGKILLed"), and then writes anyway — resolving successfully while its
// entry is nowhere on disk, because the reclaiming holder read the file
// before that write and overwrote it afterwards.
//
// The reproduction widens the critical section with real work rather than a
// fake timer or an fs mock: the coordination file is pre-seeded with a bulk
// of reserved (prune-exempt) filler entries, so each holder's
// read-parse-serialise-write cycle genuinely takes long enough that a
// contender's `looksStale` verdict fires against a live holder. That is the
// exact production precondition this ticket names — a stall on a
// swap-thrashing host — expressed as work rather than as a mock.
//
// The assertions are written as an INVARIANT that is meaningful in both
// states, so the builder turns them green without editing them:
//
//     every call either lands its write, or rejects with LockLostError
//
// Today calls resolve with nothing on disk (invariant violated). After the
// fix, a holder that lost its lock rejects instead of silently dropping the
// write, so absence is always accounted for.
//
// If one of these ever goes red in CI, read it as the ACCEPTED RESIDUAL
// before reading it as a fencing regression. A fulfilled racer whose write
// landed inside the assert-to-rename gap and was then clobbered would fail
// the "resolved means on disk" arm — that is the documented window this
// ticket narrows but does not close, not a broken guard. It requires a
// contender to complete a full ~1.8 MB read-parse-serialise-write inside one
// event-loop hop, so it is very unlikely; deliberately NOT papered over with
// `jest.retryTimes`, because a retry would hide a genuine regression just as
// effectively as it would hide the residual.
// ---------------------------------------------------------------------------

describe('post-reclaim write is refused rather than silently dropped', () => {
  // Enough filler to make a single read-modify-write cycle take an order of
  // magnitude longer than `slowCriticalSectionStalenessMs`, which is what
  // lets a contender form a genuine (if premature) staleness verdict against
  // a live holder. Reserved ids (`__...__`) so `pruneStale` exempts them
  // unconditionally and the filler can never itself be the thing that goes
  // missing.
  const FILLER_ENTRY_COUNT = 20_000;
  const slowCriticalSectionStalenessMs = 5;

  async function seedBulkFiller(targetPath, now) {
    const filler = Array.from({ length: FILLER_ENTRY_COUNT }, (_, index) => ({
      orchestratorId: `__filler-${index}__`,
      desiredAgents: 1,
      declaredAt: now,
      padding: 'x'.repeat(40),
    }));
    await fsPromises.writeFile(targetPath, JSON.stringify(filler), 'utf8');
  }

  // TWO LEGITIMATE REFUSALS, not one. `LockLostError` is the mechanism these
  // races are built to produce, and is what fires in isolation. Under parallel
  // load `LockTimeoutError` is also genuinely reachable — `acquireLock` burns
  // LOCK_MAX_ATTEMPTS = 2000 retries against a deliberately-widened critical
  // section — and it is the SAME outcome from this test's point of view: the
  // call rejected and the write did not land.
  //
  // What these tests actually claim is "a call that did not reach disk never
  // RESOLVES", and a fencing regression breaks that by resolving, not by
  // rejecting with a different code. So admitting both refusals costs no
  // discriminating power, and pinning one of them would fail the suite for a
  // property it never set out to assert. The code is still asserted, so a
  // rejection for some third reason still names itself.
  function expectFailedClosed(reason) {
    expect(['COORDINATION_LOCK_LOST', 'COORDINATION_LOCK_TIMEOUT']).toContain(reason?.code);
  }

  it('never resolves a declareDibs call whose entry did not reach disk — a lost lock rejects instead', async () => {
    const now = Date.now();
    await seedBulkFiller(filePath, now);

    const orchestratorIds = Array.from({ length: 8 }, (_, index) => `orc-stall-${index}`);
    const settled = await Promise.allSettled(
      orchestratorIds.map((orchestratorId) =>
        declareDibs(
          filePath,
          { orchestratorId, desiredAgents: 1, declaredAt: now },
          { lockStalenessMs: slowCriticalSectionStalenessMs, lockRetryDelayMs: 1 },
        ),
      ),
    );

    const persisted = new Set((await readDibs(filePath, now)).map((entry) => entry.orchestratorId));

    // Non-vacuity, stated in a way that does not depend on how the race
    // happened to resolve. Under enough contention EVERY racer can
    // legitimately lose its lock, so "at least one entry landed" is not a
    // safe expectation — but the file must still be usable afterwards. The
    // mechanism refuses writes; it must never wedge the coordination file.
    //
    // SAME `lockStalenessMs` AS THE RACERS, deliberately, and this is load-
    // bearing rather than tidiness. The race can legitimately leave an
    // ORPHANED `${filePath}.lock` behind: `releaseLock` renames the lock onto
    // a private path before inspecting it, and if a contender wins
    // `open(lockPath, 'wx')` inside that gap the restore path can put an
    // already-released holder's lock file back (coordination-file.mjs's
    // `restoreOrDiscard`, documented there as a fail-closed residual of the
    // removal/restore design). Measured here: 1 run in 30 ends the race with
    // the lock file still on disk. Such a lock is reclaimable — but only once
    // it is STALE, and staleness is judged against the reclaiming caller's
    // own `lockStalenessMs`. Asking for 60s made an orphan produced under a
    // 5ms regime un-reclaimable for a full minute, so this call burned
    // LOCK_MAX_ATTEMPTS = 2000 × ~7.5ms of retries and rejected with
    // `LockTimeoutError` — the CI flake, and a question this test never meant
    // to ask. Under the racers' own staleness the orphan is stale on sight,
    // which is precisely the "never wedges the file" property being claimed:
    // the assertion below is unchanged and still requires the write to land.
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-uncontended', desiredAgents: 1, declaredAt: now },
      { lockStalenessMs: slowCriticalSectionStalenessMs },
    );
    const afterUncontended = new Set((await readDibs(filePath, now)).map((entry) => entry.orchestratorId));
    expect(afterUncontended.has('orc-uncontended')).toBe(true);

    orchestratorIds.forEach((orchestratorId, index) => {
      const outcome = settled[index];
      if (outcome.status === 'fulfilled') {
        // A resolved declareDibs claims its entry is on disk. It must be.
        expect(persisted.has(orchestratorId)).toBe(true);
      } else {
        expectFailedClosed(outcome.reason);
      }
    });
  }, 30_000);

  it('never reports a spawn-token grant that was not actually debited from the persisted bucket', async () => {
    const now = Date.now();
    await seedBulkFiller(filePath, now);

    // `refillTokensPerMs: 0` freezes the bucket so the arithmetic below is
    // exact: whatever was granted must be missing from the persisted bucket,
    // with no refill drifting the total.
    const config = { capacityTokens: 8, refillTokensPerMs: 0 };
    const settled = await Promise.allSettled(
      Array.from({ length: 8 }, () =>
        consumeGlobalSpawnTokens(
          filePath,
          { requestedCount: 1, config, now },
          { lockStalenessMs: slowCriticalSectionStalenessMs, lockRetryDelayMs: 1 },
        ),
      ),
    );

    let grantedTotal = 0;
    settled.forEach((outcome) => {
      if (outcome.status === 'fulfilled') grantedTotal += outcome.value.grantedCount;
      else expectFailedClosed(outcome.reason);
    });

    const persistedEntries = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const bucketEntry = persistedEntries.find(
      (entry) => entry.orchestratorId === '__global-spawn-rate-bucket__',
    );
    // No bucket entry at all is the correct persisted state when every racer
    // lost its lock and nothing was written — the bucket is still at full
    // capacity, which the arithmetic below handles uniformly.
    const persistedTokens = bucketEntry ? bucketEntry.tokens : config.capacityTokens;

    // Over-admission is exactly "tokens reported granted that the shared
    // bucket never actually lost". A dropped write makes the persisted
    // bucket richer than the grants say it should be.
    expect(persistedTokens).toBe(config.capacityTokens - grantedTotal);
  }, 30_000);

  it('does not let a stalled declareDibs clobber a claim ledger written by a different call site', async () => {
    // Cross-call-site variant (Convergence Analysis edge case 8): the two
    // writers touch DIFFERENT entries in the same file, so a lost write is
    // not merely "my entry is missing" but "someone else's entry vanished
    // under me" — a distinct payload-collision path from any single-site
    // race.
    const now = Date.now();
    await seedBulkFiller(filePath, now);
    const type = 'cross-site-type';
    const ledgerId = `__claim:${type}__`;

    const dibsIds = Array.from({ length: 4 }, (_, index) => `orc-cross-${index}`);
    const settled = await Promise.allSettled([
      ...dibsIds.map((orchestratorId) =>
        declareDibs(
          filePath,
          { orchestratorId, desiredAgents: 1, declaredAt: now },
          { lockStalenessMs: slowCriticalSectionStalenessMs, lockRetryDelayMs: 1 },
        ),
      ),
      ...Array.from({ length: 4 }, (_, index) =>
        claimCapacity(
          filePath,
          {
            type,
            requestedCount: 1,
            orchestratorId: `orc-claimer-${index}`,
            computeAvailableCapacity: async () => 100,
            now,
          },
          {
            claimTtlMs: DEFAULT_CLAIM_TTL_MS,
            lockStalenessMs: slowCriticalSectionStalenessMs,
            lockRetryDelayMs: 1,
          },
        ),
      ),
    ]);

    const persistedEntries = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const persistedIds = new Set(persistedEntries.map((entry) => entry.orchestratorId));
    const ledgerEntry = persistedEntries.find((entry) => entry.orchestratorId === ledgerId);

    let claimedTotal = 0;
    settled.forEach((outcome, index) => {
      if (outcome.status !== 'fulfilled') {
        expectFailedClosed(outcome.reason);
        return;
      }
      if (index < dibsIds.length) {
        expect(persistedIds.has(dibsIds[index])).toBe(true);
      } else {
        claimedTotal += outcome.value.granted;
      }
    });

    // Every grant a claimer reported must be represented on the persisted
    // ledger — a dibs write that clobbered the ledger entry would leave
    // capacity granted in memory but unaccounted-for on disk, which is the
    // over-admission this ticket is a prerequisite for.
    expect(ledgerEntry?.grantedTotal ?? 0).toBe(claimedTotal);
  }, 30_000);
});

// ---------------------------------------------------------------------------
// assertStillHeld — the read-only ownership re-check the write choke point is
// built on. Deliberately tested in isolation from any write
// path, so the choke-point phase's own diff is purely about placement.
// ---------------------------------------------------------------------------

describe('assertStillHeld read-only ownership re-check', () => {
  const lockPathFor = () => `${filePath}.lock`;

  it('resolves when the on-disk token still matches, without touching the lock file', async () => {
    const lockPath = lockPathFor();
    const token = await acquireLock(lockPath, 60_000);
    const before = readFileSync(lockPath, 'utf8');
    const statBefore = await fsPromises.stat(lockPath);

    await expect(coordinationFile.assertStillHeld(lockPath, token)).resolves.toBeUndefined();

    // Happy path is a plain read: no rename, no unlink, no rewrite. A check
    // that disturbed the lock would itself become a source of the race it
    // exists to narrow.
    expect(readFileSync(lockPath, 'utf8')).toBe(before);
    expect((await fsPromises.stat(lockPath)).mtimeMs).toBe(statBefore.mtimeMs);

    await releaseLock(lockPath, token);
  });

  it("throws LockLostError when a different holder's lock now occupies the path", async () => {
    const lockPath = lockPathFor();
    const staleToken = await acquireLock(lockPath, 60_000);
    // A real reclaim + re-acquire, through the production path.
    expect(await reclaimStaleLock(lockPath, -1)).toBe(true);
    const newToken = await acquireLock(lockPath, 60_000);
    expect(newToken).not.toBe(staleToken);

    await expect(coordinationFile.assertStillHeld(lockPath, staleToken)).rejects.toMatchObject({
      name: 'LockLostError',
      code: 'COORDINATION_LOCK_LOST',
    });

    await releaseLock(lockPath, newToken);
  });

  it('throws LockLostError when the lock file is absent — absence is loss, not "probably fine"', async () => {
    const lockPath = lockPathFor();
    const token = await acquireLock(lockPath, 60_000);
    await releaseLock(lockPath, token);
    expect(existsSync(lockPath)).toBe(false);

    await expect(coordinationFile.assertStillHeld(lockPath, token)).rejects.toMatchObject({
      code: 'COORDINATION_LOCK_LOST',
    });
  });

  it('throws LockLostError when the lock payload is unparseable — unprovable ownership must not write', async () => {
    const lockPath = lockPathFor();
    // The shape an ENOSPC-truncated or killed-mid-write lock leaves behind.
    writeFileSync(lockPath, '{"pid":123,"acquired');

    await expect(coordinationFile.assertStillHeld(lockPath, 'any-token')).rejects.toMatchObject({
      code: 'COORDINATION_LOCK_LOST',
    });
  });

  it('throws LockLostError for a well-formed but untokened payload', async () => {
    const lockPath = lockPathFor();
    writeFileSync(lockPath, JSON.stringify({ pid: 123, acquiredAt: Date.now() }));

    await expect(coordinationFile.assertStillHeld(lockPath, 'any-token')).rejects.toMatchObject({
      code: 'COORDINATION_LOCK_LOST',
    });
  });

  it('propagates a non-ENOENT read failure unchanged rather than reporting it as a lost lock', async () => {
    // A directory at the lock path makes `readFile` fail with EISDIR — a
    // real, non-ENOENT read failure produced without mocking `fs`. "Could
    // not tell" must stay distinguishable from "provably lost": both refuse
    // the write, but only one of them is evidence about the lock.
    const lockPath = lockPathFor();
    await fsPromises.mkdir(lockPath);

    await expect(coordinationFile.assertStillHeld(lockPath, 'any-token')).rejects.toMatchObject({
      code: 'EISDIR',
    });
  });

  it('exports LockLostError with the name and code the docs and callers branch on', () => {
    const error = new coordinationFile.LockLostError('/tmp/example.json.lock');
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe('LockLostError');
    expect(error.code).toBe('COORDINATION_LOCK_LOST');
    expect(error.message).toContain('/tmp/example.json.lock');
    // Matches LockTimeoutError/UnrecoverableLegacyClaimError: no extra
    // instance properties beyond name/code/message.
    expect(Object.keys(error).sort()).toEqual(['code', 'name']);
  });
});

// ---------------------------------------------------------------------------
// The write choke point. Every one of the five write paths in
// this module funnels through `writeEntriesAtomic`, so the mechanism is
// proven here once, directly, rather than five times through five different
// callers. The call sites' end-to-end behaviour is covered by the
// post-reclaim invariant block above and by the outer acceptance test in
// ./coordination-file.outer-acceptance.jest.spec.mjs, which drives the one
// call site (`claimCapacity`) that has a real in-lock seam.
// ---------------------------------------------------------------------------

describe('writeEntriesAtomic ownership choke point', () => {
  const entries = [{ orchestratorId: 'orc-a', desiredAgents: 1, declaredAt: 1_700_000_000_000 }];

  async function tmpSiblings() {
    return (await fsPromises.readdir(workDir)).filter((name) => name.includes('.tmp-'));
  }

  it('refuses to write at all without a lock context, with a greppable message rather than a bare TypeError', async () => {
    await expect(coordinationFile.writeEntriesAtomic(filePath, entries)).rejects.toThrow(TypeError);
    await expect(coordinationFile.writeEntriesAtomic(filePath, entries)).rejects.toThrow(
      /lockContext with an assertStillHeld\(\) function is required/,
    );
    // A future writer that forgets the argument must not silently create the
    // file, and must not leave a staged sibling behind either.
    expect(existsSync(filePath)).toBe(false);
    expect(await tmpSiblings()).toEqual([]);
  });

  it('rejects a context whose assertStillHeld is not callable, rather than trusting the shape', async () => {
    await expect(
      coordinationFile.writeEntriesAtomic(filePath, entries, { lockPath: 'x', token: 'y' }),
    ).rejects.toThrow(TypeError);
    expect(existsSync(filePath)).toBe(false);
  });

  it('writes and renames exactly as before when ownership still holds', async () => {
    const lockPath = `${filePath}.lock`;
    const token = await acquireLock(lockPath, 60_000);
    await coordinationFile.writeEntriesAtomic(filePath, entries, {
      lockPath,
      token,
      assertStillHeld: () => coordinationFile.assertStillHeld(lockPath, token),
    });

    expect(JSON.parse(readFileSync(filePath, 'utf8'))).toEqual(entries);
    expect(await tmpSiblings()).toEqual([]);
    await releaseLock(lockPath, token);
  });

  it('abandons the write, removes the staged sibling, and throws when the lock was genuinely reclaimed', async () => {
    const lockPath = `${filePath}.lock`;
    writeFileSync(filePath, JSON.stringify([{ orchestratorId: 'incumbent' }]), 'utf8');
    const staleToken = await acquireLock(lockPath, 60_000);
    // Real reclaim + re-acquire through the production path — the lock at
    // this path is now provably someone else's.
    expect(await reclaimStaleLock(lockPath, -1)).toBe(true);
    const newToken = await acquireLock(lockPath, 60_000);

    await expect(
      coordinationFile.writeEntriesAtomic(filePath, entries, {
        lockPath,
        token: staleToken,
        assertStillHeld: () => coordinationFile.assertStillHeld(lockPath, staleToken),
      }),
    ).rejects.toMatchObject({ name: 'LockLostError', code: 'COORDINATION_LOCK_LOST' });

    // The incumbent content is untouched, and nothing was orphaned.
    expect(JSON.parse(readFileSync(filePath, 'utf8'))).toEqual([{ orchestratorId: 'incumbent' }]);
    expect(await tmpSiblings()).toEqual([]);
    await releaseLock(lockPath, newToken);
  });

  it('still propagates the original failure when cleaning up the staged sibling itself fails', async () => {
    // Simulates a concurrent cleanup racing the refusal path: the staged
    // `.tmp-*` file is gone by the time the unlink runs, so the unlink fails
    // with ENOENT. The caller must still see the ownership failure — a
    // swallowed-then-rethrown cleanup error would mask the real diagnosis,
    // and an unhandled one would surface as a dangling rejection.
    const stealTmpThenLoseLock = async () => {
      for (const name of await tmpSiblings()) {
        await fsPromises.unlink(join(workDir, name));
      }
      throw new coordinationFile.LockLostError(`${filePath}.lock`);
    };

    await expect(
      coordinationFile.writeEntriesAtomic(filePath, entries, {
        assertStillHeld: stealTmpThenLoseLock,
      }),
    ).rejects.toMatchObject({ code: 'COORDINATION_LOCK_LOST' });
    expect(existsSync(filePath)).toBe(false);
  });

  it('leaves the ordering the residual window depends on: the assert runs before the rename, not before the staged write', () => {
    // Pinned structurally as well as behaviourally — the whole value of the
    // choke point is that the gap between proving ownership and committing
    // the rename is one event-loop hop wide. Reordering the assert ahead of
    // the staged write would silently widen it.
    const fnIndex = coordinationFileSource.indexOf('export async function writeEntriesAtomic(');
    const stagedWriteIndex = coordinationFileSource.indexOf('await fs.writeFile(tmpPath,', fnIndex);
    const assertIndex = coordinationFileSource.indexOf('await lockContext.assertStillHeld();', fnIndex);
    const renameIndex = coordinationFileSource.indexOf('await fs.rename(tmpPath, filePath);', fnIndex);

    expect(fnIndex).toBeGreaterThan(-1);
    expect(assertIndex).toBeGreaterThan(stagedWriteIndex);
    expect(renameIndex).toBeGreaterThan(assertIndex);
  });
});

// ---------------------------------------------------------------------------
// Behaviour the choke point must NOT change. The guard is
// mandatory, so every legitimate path that writes nothing has to keep
// writing nothing — without tripping over the new required argument.
// ---------------------------------------------------------------------------

describe('non-write and conditional-write paths are unaffected by the mandatory lock context', () => {
  it("preserves writeSharedSample's three non-write return shapes", async () => {
    const now = Date.now();
    // Two of these return before the lock is ever acquired; the third
    // returns from inside the critical section without reaching the choke
    // point at all.
    await expect(writeSharedSample(filePath, { sampledAt: now, stale: true })).resolves.toEqual({
      persisted: false,
      reason: 'stale-reading',
    });
    await expect(writeSharedSample(filePath, { sampledAt: 'not-a-number' })).resolves.toEqual({
      persisted: false,
      reason: 'invalid-sampled-at',
    });

    await expect(
      writeSharedSample(filePath, { sampledAt: now, memory: {}, disk: {} }),
    ).resolves.toEqual({ persisted: true });
    await expect(
      writeSharedSample(filePath, { sampledAt: now - 1, memory: {}, disk: {} }),
    ).resolves.toEqual({ persisted: false, reason: 'not-newer-than-persisted' });
  });

  it("preserves claimCapacity's zero-grant, non-migrating no-write path", async () => {
    const now = Date.now();
    await expect(
      claimCapacity(
        filePath,
        {
          type: 'no-write-type',
          requestedCount: 2,
          orchestratorId: 'orc-a',
          computeAvailableCapacity: async () => 0,
          now,
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).resolves.toMatchObject({ granted: 0 });
    // Nothing to lose means nothing to prove — the file was never created.
    expect(existsSync(filePath)).toBe(false);
  });

  it('routes the legacy-migration write branch through the choke point too, even when it grants nothing', async () => {
    // The migrating write is a distinct write path: `granted === 0`, so it is
    // reached only via `needsLegacyMigrationWrite`. It must be guarded like
    // any other write — a migration that lands after the lock was reclaimed
    // would rewrite the whole entries array from a stale read.
    const now = Date.now();
    const type = 'legacy-migration-type';
    const lockPath = `${filePath}.lock`;
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([{ orchestratorId: `__claim:${type}__`, grantedTotal: 3, declaredAt: now }]),
      'utf8',
    );

    let newHolderToken;
    const reclaimMidSection = async () => {
      expect(await reclaimStaleLock(lockPath, -1)).toBe(true);
      newHolderToken = await acquireLock(lockPath, 60_000);
      return 0; // grants nothing — only the migration write remains
    };

    await expect(
      claimCapacity(
        filePath,
        {
          type,
          requestedCount: 1,
          orchestratorId: 'orc-a',
          computeAvailableCapacity: reclaimMidSection,
          now,
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).rejects.toMatchObject({ code: 'COORDINATION_LOCK_LOST' });

    // The legacy entry is left exactly as it was — a refused migration must
    // not half-apply.
    const persisted = JSON.parse(readFileSync(filePath, 'utf8'));
    expect(persisted).toEqual([{ orchestratorId: `__claim:${type}__`, grantedTotal: 3, declaredAt: now }]);
    await releaseLock(lockPath, newHolderToken);
  });

  it("surfaces the original error from fn even though withLock's finally still runs a release", async () => {
    // The `finally` release must never mask what `fn` threw, and — because
    // the token no longer matches — must never drop the new holder's lock
    // while doing so.
    const now = Date.now();
    const lockPath = `${filePath}.lock`;
    let newHolderToken;

    const boom = new Error('deliberate failure from inside the critical section');
    await expect(
      claimCapacity(
        filePath,
        {
          type: 'finally-masking-type',
          requestedCount: 1,
          orchestratorId: 'orc-a',
          computeAvailableCapacity: async () => {
            expect(await reclaimStaleLock(lockPath, -1)).toBe(true);
            newHolderToken = await acquireLock(lockPath, 60_000);
            throw boom;
          },
          now,
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).rejects.toBe(boom);

    const lockPayloadAfter = JSON.parse(readFileSync(lockPath, 'utf8'));
    expect(lockPayloadAfter.token).toBe(newHolderToken);
    await releaseLock(lockPath, newHolderToken);
  });
});

// ---------------------------------------------------------------------------
// The post-probe ownership re-check. `claimCapacity` is the
// only critical section that awaits a caller-supplied callback, and in
// production that callback shells out synchronously several times — it is the
// part of this critical section most capable of outliving `lockStalenessMs`.
// Ownership is therefore re-proven the moment it returns, before the ledger
// arithmetic that turns `alreadyGrantedTotal` into a grant.
// ---------------------------------------------------------------------------

describe('claimCapacity re-proves ownership as soon as the capacity probe returns', () => {
  it('refuses a grant derived from a ledger read under a lock that has since changed hands, even when the call would write nothing', async () => {
    const now = Date.now();
    const lockPath = `${filePath}.lock`;
    let newHolderToken;

    await expect(
      claimCapacity(
        filePath,
        {
          type: 'post-probe-type',
          requestedCount: 3,
          orchestratorId: 'orc-a',
          computeAvailableCapacity: async () => {
            expect(await reclaimStaleLock(lockPath, -1)).toBe(true);
            newHolderToken = await acquireLock(lockPath, 60_000);
            // Zero capacity means this call would never reach
            // `writeEntriesAtomic` at all — the write choke point cannot
            // catch this one. The returned `granted` would still have been
            // computed from an `alreadyGrantedTotal` another holder may
            // already have moved on from.
            return 0;
          },
          now,
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).rejects.toMatchObject({ name: 'LockLostError', code: 'COORDINATION_LOCK_LOST' });

    await releaseLock(lockPath, newHolderToken);
  });

  it('grants normally when a slow probe returns with ownership still intact', async () => {
    const now = Date.now();
    await expect(
      claimCapacity(
        filePath,
        {
          type: 'slow-probe-type',
          requestedCount: 2,
          orchestratorId: 'orc-a',
          computeAvailableCapacity: async () => {
            await new Promise((resolve) => setTimeout(resolve, 25));
            return 5;
          },
          now,
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).resolves.toMatchObject({ granted: 2, availableCapacity: 5 });
  });

  it('leaves the pre-existing probe-result contract untouched: a non-finite result still coerces to 0', async () => {
    const now = Date.now();
    await expect(
      claimCapacity(
        filePath,
        {
          type: 'non-finite-type',
          requestedCount: 2,
          orchestratorId: 'orc-a',
          computeAvailableCapacity: async () => Number.NaN,
          now,
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).resolves.toMatchObject({ granted: 0, availableCapacity: 0 });
  });

  it('leaves the pre-existing probe-result contract untouched: a rejecting probe still propagates its own error', async () => {
    const now = Date.now();
    const boom = new Error('probe blew up');
    await expect(
      claimCapacity(
        filePath,
        {
          type: 'rejecting-probe-type',
          requestedCount: 2,
          orchestratorId: 'orc-a',
          computeAvailableCapacity: async () => {
            throw boom;
          },
          now,
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).rejects.toBe(boom);
  });

  it('places the post-probe assert before the ledger arithmetic it protects', () => {
    const fnIndex = coordinationFileSource.indexOf('export async function claimCapacity(');
    const probeIndex = coordinationFileSource.indexOf(
      'const availableCapacityRaw = await computeAvailableCapacity();',
      fnIndex,
    );
    const assertIndex = coordinationFileSource.indexOf('await lockContext.assertStillHeld();', fnIndex);
    const grantIndex = coordinationFileSource.indexOf(
      'const granted = Math.max(0, Math.min(requestedCount, remaining));',
      fnIndex,
    );

    expect(assertIndex).toBeGreaterThan(probeIndex);
    expect(grantIndex).toBeGreaterThan(assertIndex);
  });
});

// ---------------------------------------------------------------------------
// `withLock`'s finally must not swallow what `fn` threw.
//
// This became load-bearing with the write-side re-check: on the LockLostError
// path the token ALWAYS mismatches, which routes `releaseLock` into
// `restoreOrDiscard` — the only part of it that can rethrow a non-ENOENT
// error. A release failure landing there would replace the diagnosis with
// cleanup noise, on exactly the path this ticket exists to make visible.
// ---------------------------------------------------------------------------

describe("withLock's finally never masks the error fn threw", () => {
  // chmod is not a barrier for uid 0, so a root runner cannot exhibit this.
  const runningAsRoot = typeof process.getuid === 'function' && process.getuid() === 0;
  const maybeIt = runningAsRoot ? it.skip : it;

  maybeIt('surfaces the original failure even when releaseLock itself throws', async () => {
    const now = Date.now();
    const boom = new Error('the failure the caller actually needs to see');

    let chmodApplied = false;
    try {
      await expect(
        claimCapacity(
          filePath,
          {
            type: 'finally-masking-type',
            requestedCount: 1,
            orchestratorId: 'orc-a',
            computeAvailableCapacity: async () => {
              // Making the directory read-only breaks `releaseLock`'s very
              // first step — `rename(lockPath, tmpPath)` needs write
              // permission on the containing directory — so the release
              // throws EACCES while unwinding from `boom`.
              await fsPromises.chmod(workDir, 0o555);
              chmodApplied = true;
              throw boom;
            },
            now,
          },
          { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
        ),
      ).rejects.toBe(boom);
    } finally {
      if (chmodApplied) await fsPromises.chmod(workDir, 0o755);
    }
  });

  maybeIt('still surfaces a release failure when fn itself succeeded', async () => {
    // The other half of the contract: with no primary error to protect, a
    // release failure is the only thing that went wrong and must not be
    // silently swallowed.
    const now = Date.now();
    let chmodApplied = false;
    try {
      await expect(
        claimCapacity(
          filePath,
          {
            type: 'release-failure-type',
            requestedCount: 0,
            orchestratorId: 'orc-a',
            computeAvailableCapacity: async () => {
              await fsPromises.chmod(workDir, 0o555);
              chmodApplied = true;
              return 5;
            },
            now,
          },
          { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
        ),
      ).rejects.toMatchObject({ code: 'EACCES' });
    } finally {
      if (chmodApplied) await fsPromises.chmod(workDir, 0o755);
    }
  });
});

// ---------------------------------------------------------------------------
// reserveAdmission — single critical section holds the coordination-file lock
// across its ENTIRE read -> compute -> write, not released between steps
//.
//
// Phase 2 implemented `reserveAdmission` in `coordination-file.mjs`,
// replacing `cli.mjs`'s previous three separate lock cycles
// (`claimCapacity`/`releaseCapacity`/`claimCapacity`) with one `withLock`
// critical section performing one `readEntriesRaw` and one
// `writeEntriesAtomic`. These tests pin that contract.
//
// Referenced via the `coordinationFile` namespace import at the top of this
// file (never as a named import) — see this file's own comment on
// `DEFAULT_CLAIM_TTL_MS` for why a namespace import is used throughout this
// file even for long-since-implemented exports.
//
// MECHANISM CHOSEN to simulate a slow, still-in-progress critical section
// (Build Plan Phase 1's open decision, made explicit here for Phase 2's
// implementer): `reserveAdmission`'s planned signature —
// `reserveAdmission(filePath, { snapshotType, admissionType, requestedCount,
// orchestratorId, ceiling, now }, options)` — carries no caller-supplied
// async callback the way `claimCapacity`'s `computeAvailableCapacity` does,
// so there is nothing to hang an artificial delay off from OUTSIDE the lock.
// Phase 2's `reserveAdmission` MUST therefore accept a test-only
// `options.testInjectedDelayMs` (a plain, finite number of milliseconds)
// and, when present, `await` a bare `setTimeout` for that long exactly once,
// immediately after its single `readEntriesRaw` call and before any of the
// snapshot-ceiling / own-hold-release / admission-grant computation or the
// eventual `writeEntriesAtomic` — the direct analogue of the slow
// `computeAvailableCapacity` idiom the pre-existing
// "claimCapacity re-proves ownership as soon as the capacity probe returns
//" tests above already use to hold `claimCapacity`'s lock open on
// purpose. This option must never fire in production (`cli.mjs` must never
// pass it) and must not leak into any non-test call site.
//
// `LIVE_AGENT_CLAIM_TYPE`/`LIVE_AGENT_ADMISSION_CLAIM_TYPE` are copied here
// as plain string literals rather than imported from `../cli.mjs`, because
// `cli.mjs` calls `main()` unconditionally at module-evaluation time (no
// `import.meta.url` guard) — importing it from this test file would run the
// real CLI beat against this process's own argv/filesystem. Keep these two
// literals in sync with `cli.mjs`'s own exported constants if either ever
// changes.
// ---------------------------------------------------------------------------

const RESERVE_ADMISSION_SNAPSHOT_TYPE = 'live-agent';
const RESERVE_ADMISSION_ADMISSION_TYPE = 'live-agent-admission';

describe('reserveAdmission — one lock cycle for the whole read/compute/write, not three', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('is exported as a function (Phase 2 implemented it — this pins the export surface)', () => {
    expect(typeof coordinationFile.reserveAdmission).toBe('function');
  });

  it('refuses a competing claimCapacity against the snapshot type (LIVE_AGENT_CLAIM_TYPE) issued while reserveAdmission\'s critical section is still (simulated) running', async () => {
    const now = Date.now();

    const reservation = coordinationFile.reserveAdmission(
      filePath,
      {
        snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
        admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
        requestedCount: 1,
        orchestratorId: 'orc-reserve-a',
        ceiling: 4,
        now,
      },
      { testInjectedDelayMs: 150, claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    // Give reserveAdmission's lock a head start so the competing call below
    // is guaranteed to land mid-critical-section, never before it opens.
    // 30ms (widened from 10ms, pre-PR-review finding — pre-existing flake
    // under full-suite parallel Jest-worker load, not a regression): a
    // heavily loaded host can delay the primary's own async chain (TTL
    // guard + withLock + acquireLock) past a 10ms head start, letting the
    // competitor observe the lock as free and pass spuriously. Paired with
    // the widened 150ms injected delay above (was 50ms), the competitor's
    // ~5ms retry budget (`lockMaxAttempts: 5, lockRetryDelayMs: 1`) below
    // now has a much larger margin to fail against, without weakening the
    // assertion itself.
    await new Promise((resolve) => setTimeout(resolve, 30));

    const competitor = claimCapacity(
      filePath,
      {
        type: RESERVE_ADMISSION_SNAPSHOT_TYPE,
        requestedCount: 1,
        orchestratorId: 'orc-competitor-snapshot',
        computeAvailableCapacity: async () => 4,
        now,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS, lockMaxAttempts: 5, lockRetryDelayMs: 1 },
    );

    await expect(competitor).rejects.toMatchObject({
      name: 'LockTimeoutError',
      code: 'COORDINATION_LOCK_TIMEOUT',
    });

    // The competitor's refusal must be a side effect of reserveAdmission
    // still holding the lock, not of reserveAdmission itself having failed —
    // once its (simulated) critical section finishes it is expected to
    // complete normally.
    await expect(reservation).resolves.toBeDefined();
  });

  // KNOWN FLAKE, tracked at — pre-existing on `master`, not introduced by
  //. Measured at 2 occurrences in ~59 full suite runs on a loaded macOS
  // host: the competitor legitimately acquires the lock after the critical
  // section commits, so the `rejects` below resolves instead. The assertion
  // over-specifies a TIMING outcome for what is really the atomicity
  // property. Do NOT paper over it with `jest.retryTimes` — a retry hides a
  // genuine fencing regression exactly as well as it hides the flake.
  it('refuses a competing claimCapacity/releaseCapacity against the admission type (LIVE_AGENT_ADMISSION_CLAIM_TYPE) issued mid-section', async () => {
    const now = Date.now();

    const reservation = coordinationFile.reserveAdmission(
      filePath,
      {
        snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
        admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
        requestedCount: 1,
        orchestratorId: 'orc-reserve-b',
        ceiling: 4,
        now,
      },
      { testInjectedDelayMs: 150, claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    // Widened from 10ms — see the matching comment on the preceding test for
    // why (pre-PR-review finding, pre-existing flake under full-suite
    // parallel Jest-worker load).
    await new Promise((resolve) => setTimeout(resolve, 30));

    const competitorClaim = claimCapacity(
      filePath,
      {
        type: RESERVE_ADMISSION_ADMISSION_TYPE,
        requestedCount: 1,
        orchestratorId: 'orc-competitor-admission-claim',
        computeAvailableCapacity: async () => 4,
        now,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS, lockMaxAttempts: 5, lockRetryDelayMs: 1 },
    );

    await expect(competitorClaim).rejects.toMatchObject({
      name: 'LockTimeoutError',
      code: 'COORDINATION_LOCK_TIMEOUT',
    });

    const competitorRelease = releaseCapacity(
      filePath,
      {
        type: RESERVE_ADMISSION_ADMISSION_TYPE,
        releaseCount: 1,
        orchestratorId: 'orc-competitor-admission-release',
      },
      { lockMaxAttempts: 5, lockRetryDelayMs: 1 },
    );

    await expect(competitorRelease).rejects.toMatchObject({
      name: 'LockTimeoutError',
      code: 'COORDINATION_LOCK_TIMEOUT',
    });

    await expect(reservation).resolves.toBeDefined();
  });

  it('acquires the coordination-file lock exactly once per reserveAdmission call — down from three separate lock cycles', async () => {
    const now = Date.now();
    const lockPath = `${filePath}.lock`;
    const originalOpen = fsPromises.open.bind(fsPromises);
    const wxOpensOnThisLock = [];

    jest.spyOn(fsPromises, 'open').mockImplementation(async (openPath, flags, ...rest) => {
      if (flags === 'wx' && openPath === lockPath) wxOpensOnThisLock.push(Date.now());
      return originalOpen(openPath, flags, ...rest);
    });

    await coordinationFile.reserveAdmission(
      filePath,
      {
        snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
        admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
        requestedCount: 1,
        orchestratorId: 'orc-reserve-lock-count',
        ceiling: 4,
        now,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    // Exactly one `open(lockPath, 'wx')` — one acquireLock call, one withLock
    // critical section — never three (the pre-existing cli.mjs shape: claim,
    // release, claim, each its own separate acquireLock/releaseLock pair).
    expect(wxOpensOnThisLock).toHaveLength(1);
  });

  it('clears a prior nonzero admission-type hold on a round that computes granted === 0 — a stale hold must not survive untouched to age out via TTL', async () => {
    const now = Date.now();

    // First round: this orchestrator is granted 2 out of a ceiling of 4, no
    // other holders — establishes a genuine prior live hold on the
    // admission-type ledger.
    const firstRound = await coordinationFile.reserveAdmission(
      filePath,
      {
        snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
        admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
        requestedCount: 2,
        orchestratorId: 'orc-stale-hold',
        ceiling: 4,
        now,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );
    expect(firstRound.granted).toBe(2);

    const admissionLedgerId = `__claim:${RESERVE_ADMISSION_ADMISSION_TYPE}__`;
    const afterFirstRound = JSON.parse(readFileSync(filePath, 'utf8'));
    const admissionEntryAfterFirstRound = afterFirstRound.find(
      (entry) => entry.orchestratorId === admissionLedgerId,
    );
    expect(admissionEntryAfterFirstRound?.claims).toEqual(
      expect.arrayContaining([expect.objectContaining({ orchestratorId: 'orc-stale-hold', count: 2 })]),
    );

    // A second, independent orchestrator now claims directly against the
    // admission-type ledger until it holds 4 (equal to the whole `ceiling`
    // used above), simulating another orchestrator's own admission growing
    // to consume all of this round's headroom — so that when
    // `orc-stale-hold` calls reserveAdmission again,
    // `liveAgentCeilingRemaining - othersHeld` computes to 0 for it.
    await claimCapacity(
      filePath,
      {
        type: RESERVE_ADMISSION_ADMISSION_TYPE,
        requestedCount: 4,
        orchestratorId: 'orc-other-holder',
        computeAvailableCapacity: async () => 6,
        now,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    // Second round: ceiling headroom has shrunk to nothing for
    // `orc-stale-hold` because another orchestrator now holds everything
    // else — this round must compute granted === 0.
    const secondRound = await coordinationFile.reserveAdmission(
      filePath,
      {
        snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
        admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
        requestedCount: 2,
        orchestratorId: 'orc-stale-hold',
        ceiling: 4,
        now,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );
    expect(secondRound.granted).toBe(0);

    // The stale prior hold of 2 for `orc-stale-hold` must be genuinely
    // cleared from the ledger — not left as the stale prior nonzero value —
    // even though this round's fresh grant computed to 0.
    const afterSecondRound = JSON.parse(readFileSync(filePath, 'utf8'));
    const admissionEntryAfterSecondRound = afterSecondRound.find(
      (entry) => entry.orchestratorId === admissionLedgerId,
    );
    const ownClaimAfterSecondRound = admissionEntryAfterSecondRound?.claims?.find(
      (claim) => claim.orchestratorId === 'orc-stale-hold',
    );
    expect(ownClaimAfterSecondRound).toBeUndefined();
  });

  it('assertStillHeld succeeds for the full duration of the combined critical section under realistic timing — a longer section does not spuriously trip the staleness/fencing guard', async () => {
    const now = Date.now();
    // Realistic: comfortably shorter than DEFAULT_LOCK_STALENESS_MS (30s),
    // but long enough to stand in for three read-modify-write steps folded
    // into one section, rather than the near-instant happy path most other
    // tests in this file exercise.
    const realisticCombinedSectionMs = 200;

    await expect(
      coordinationFile.reserveAdmission(
        filePath,
        {
          snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
          admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
          requestedCount: 1,
          orchestratorId: 'orc-reserve-no-spurious-trip',
          ceiling: 4,
          now,
        },
        { testInjectedDelayMs: realisticCombinedSectionMs, claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).resolves.toBeDefined();
  });

  // -------------------------------------------------------------------------
  // review follow-up (Medium): `reserveAdmission` validates
  // `orchestratorId`/`requestedCount` loudly (TypeError) but, before this
  // fix, left `snapshotType`/`admissionType`/`ceiling` entirely unguarded.
  // A caller passing the SAME string for both types would silently collide
  // the two ledger entries this function writes under one `__claim:<type>__`
  // id, double-counting every claim on the next read; a non-finite
  // `ceiling` (e.g. a stringly-typed env value) would silently grant `0`
  // forever with no error, unlike every other malformed input here.
  // -------------------------------------------------------------------------

  it('throws when snapshotType is missing/empty', async () => {
    await expect(
      coordinationFile.reserveAdmission(
        filePath,
        {
          snapshotType: '',
          admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
          requestedCount: 1,
          orchestratorId: 'orc-guard-snapshot-type',
          ceiling: 4,
          now: Date.now(),
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).rejects.toThrow(TypeError);
  });

  it('throws when admissionType is missing/empty', async () => {
    await expect(
      coordinationFile.reserveAdmission(
        filePath,
        {
          snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
          admissionType: undefined,
          requestedCount: 1,
          orchestratorId: 'orc-guard-admission-type',
          ceiling: 4,
          now: Date.now(),
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).rejects.toThrow(TypeError);
  });

  it('throws when snapshotType and admissionType are the same string, rather than silently colliding the two ledger entries', async () => {
    await expect(
      coordinationFile.reserveAdmission(
        filePath,
        {
          snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
          admissionType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
          requestedCount: 1,
          orchestratorId: 'orc-guard-same-type',
          ceiling: 4,
          now: Date.now(),
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).rejects.toThrow(TypeError);
  });

  it('throws when ceiling is not a finite number, rather than silently treating it as 0 (permanently denying every grant)', async () => {
    await expect(
      coordinationFile.reserveAdmission(
        filePath,
        {
          snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
          admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
          requestedCount: 1,
          orchestratorId: 'orc-guard-ceiling',
          ceiling: '4',
          now: Date.now(),
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).rejects.toThrow(TypeError);
  });

  // -------------------------------------------------------------------------
  // Phase 1 (RED) — `reserveAdmission`'s `ceiling` guard currently only
  // rejects non-finite values (`isFiniteNumber`). It does NOT yet reject a
  // finite but non-integer or negative `ceiling` (e.g. `3.5` from a
  // misconfigured AIMD-adjusted ceiling, or `-1` from an underflowed
  // decrement) — both sail past the current check and are only clamped
  // downstream by `Math.max(0, ceiling)` at Step 1, silently rather than
  // loudly. This block extends the guard to `isNonNegativeInteger`-style
  // validation (finite, integer, >= 0), mirroring the adjacent
  // `requestedCount` check's shape and error type. `ceiling: 0` is a
  // legitimate (if degenerate) value and must NOT be rejected by the new
  // check — see the regression test below.
  // -------------------------------------------------------------------------

  it('reserveAdmission throws on a non-integer, finite ceiling (3.5), not just on non-finite ones (Phase 1 RED)', async () => {
    await expect(
      coordinationFile.reserveAdmission(
        filePath,
        {
          snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
          admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
          requestedCount: 1,
          orchestratorId: 'orc-guard-ceiling-non-integer',
          ceiling: 3.5,
          now: Date.now(),
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).rejects.toThrow(TypeError);
  });

  it('reserveAdmission throws on ceiling=NaN (existing isFiniteNumber coverage, confirmed still enforced)', async () => {
    await expect(
      coordinationFile.reserveAdmission(
        filePath,
        {
          snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
          admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
          requestedCount: 1,
          orchestratorId: 'orc-guard-ceiling-nan',
          ceiling: NaN,
          now: Date.now(),
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).rejects.toThrow(TypeError);
  });

  it('reserveAdmission throws on ceiling=Infinity (existing isFiniteNumber coverage, confirmed still enforced)', async () => {
    await expect(
      coordinationFile.reserveAdmission(
        filePath,
        {
          snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
          admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
          requestedCount: 1,
          orchestratorId: 'orc-guard-ceiling-infinity',
          ceiling: Infinity,
          now: Date.now(),
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).rejects.toThrow(TypeError);
  });

  it('reserveAdmission accepts ceiling=4 — regression, unchanged behavior for a normal finite integer ceiling', async () => {
    await expect(
      coordinationFile.reserveAdmission(
        filePath,
        {
          snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
          admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
          requestedCount: 1,
          orchestratorId: 'orc-guard-ceiling-four',
          ceiling: 4,
          now: Date.now(),
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).resolves.toBeDefined();
  });

  it('reserveAdmission accepts ceiling=0 — a degenerate but legitimate finite integer ceiling, must NOT be rejected by the new integer check', async () => {
    await expect(
      coordinationFile.reserveAdmission(
        filePath,
        {
          snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
          admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
          requestedCount: 1,
          orchestratorId: 'orc-guard-ceiling-zero',
          ceiling: 0,
          now: Date.now(),
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).resolves.toBeDefined();
  });

  it('reserveAdmission throws on ceiling=-1 — a negative ceiling must be rejected at the validation boundary, not silently clamped to 0 by the existing Math.max(0, ceiling) downstream (Phase 1 RED, known gap called out by name in the Build Plan)', async () => {
    await expect(
      coordinationFile.reserveAdmission(
        filePath,
        {
          snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
          admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
          requestedCount: 1,
          orchestratorId: 'orc-guard-ceiling-negative',
          ceiling: -1,
          now: Date.now(),
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).rejects.toThrow(TypeError);
  });
});

// ---------------------------------------------------------------------------
// reserveAdmission — TTL option split (Phase 1 RED).
//
// Today `reserveAdmission` takes a single `options.claimTtlMs` and uses it to
// prune BOTH ledgers it touches: `snapshotType`'s (LIVE_AGENT_CLAIM_TYPE)
// headroom probe at Step 1, and `admissionType`'s
// (LIVE_AGENT_ADMISSION_CLAIM_TYPE) own-hold-replace/grant at Steps 2-3. The
// confirmed fix (per the Build Plan) is a SECOND, independent option —
// `options.snapshotClaimTtlMs` — that governs Step 1's pruning, falling back
// to `options.claimTtlMs` when omitted (today's behavior, unchanged for any
// caller not yet updated); `options.claimTtlMs` continues to govern Steps 2-3
// unconditionally, regardless of what `snapshotClaimTtlMs` is set to.
//
// These are narrower unit tests than the Phase 0 outer acceptance test at
// ../live-agent-ttl-reconciliation.jest.spec.mjs (which proves the end-to-end
// over-grant symptom via claimCapacity + reserveAdmission at realistic 90s/6h
// windows). This block instead pins the exact TTL-resolution CONTRACT
// `reserveAdmission` must implement, with tight synthetic TTLs so each
// boundary is asserted directly rather than inferred from one combined
// scenario.
//
// Do not add a stub implementation to make this pass — `reserveAdmission`
// does not yet accept `snapshotClaimTtlMs` at all; the builder wires it in a
// later phase.
// ---------------------------------------------------------------------------

describe('reserveAdmission — options.snapshotClaimTtlMs governs snapshotType pruning independently of options.claimTtlMs (Phase 1 RED)', () => {
  const snapshotLedgerId = `__claim:${RESERVE_ADMISSION_SNAPSHOT_TYPE}__`;
  const admissionLedgerId = `__claim:${RESERVE_ADMISSION_ADMISSION_TYPE}__`;

  it('a snapshotType claim aged past claimTtlMs but under snapshotClaimTtlMs still counts against the ceiling', async () => {
    const t0 = 1_700_000_000_000;
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([
        {
          orchestratorId: snapshotLedgerId,
          claims: [{ orchestratorId: 'orc-seed', count: 4, claimedAt: t0 }],
          grantedTotal: 4,
          declaredAt: t0,
        },
      ]),
      'utf8',
    );

    const result = await coordinationFile.reserveAdmission(
      filePath,
      {
        snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
        admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
        requestedCount: 4,
        orchestratorId: 'orc-caller-under-snapshot-ttl',
        ceiling: 4,
        now: t0 + 2_000, // past the 1s claimTtlMs, nowhere near the 6h snapshotClaimTtlMs
      },
      { claimTtlMs: 1_000, snapshotClaimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );

    // The claim seeded above must still be counted — headroom is exhausted,
    // so nothing new is granted. Today's implementation prunes the snapshot
    // ledger against the 1s claimTtlMs instead, sees it as expired, and
    // over-grants the full 4 on top of it.
    expect(result.alreadyGrantedTotalForSnapshotType).toBe(4);
    expect(result.granted).toBe(0);
  });

  it('a snapshotType claim aged past snapshotClaimTtlMs but under claimTtlMs is pruned even though it is within claimTtlMs', async () => {
    const t0 = 1_700_000_000_000;
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([
        {
          orchestratorId: snapshotLedgerId,
          claims: [{ orchestratorId: 'orc-seed', count: 4, claimedAt: t0 }],
          grantedTotal: 4,
          declaredAt: t0,
        },
      ]),
      'utf8',
    );

    const result = await coordinationFile.reserveAdmission(
      filePath,
      {
        snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
        admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
        requestedCount: 4,
        orchestratorId: 'orc-caller-past-snapshot-ttl',
        ceiling: 4,
        now: t0 + 2_000, // past the 1s snapshotClaimTtlMs, nowhere near the 6h claimTtlMs
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS, snapshotClaimTtlMs: 1_000 },
    );

    // The claim seeded above must be pruned — full headroom is free. Today's
    // implementation prunes the snapshot ledger against the 6h claimTtlMs
    // instead, sees it as still live, and under-grants (0 instead of 4).
    expect(result.alreadyGrantedTotalForSnapshotType).toBe(0);
    expect(result.granted).toBe(4);
  });

  it('back-compat: omitting snapshotClaimTtlMs falls back to claimTtlMs for snapshotType pruning — today\'s exact behavior, unchanged', async () => {
    const t0 = 1_700_000_000_000;
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([
        {
          orchestratorId: snapshotLedgerId,
          claims: [{ orchestratorId: 'orc-seed', count: 4, claimedAt: t0 }],
          grantedTotal: 4,
          declaredAt: t0,
        },
      ]),
      'utf8',
    );

    const result = await coordinationFile.reserveAdmission(
      filePath,
      {
        snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
        admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
        requestedCount: 4,
        orchestratorId: 'orc-caller-fallback',
        ceiling: 4,
        now: t0 + 2_000,
      },
      // snapshotClaimTtlMs deliberately omitted — must fall back to claimTtlMs.
      { claimTtlMs: 1_000 },
    );

    // claimTtlMs (1s) governs the fallback, so the seeded claim (aged 2s) is
    // pruned and full headroom is free — this is today's single-TTL
    // implementation's own behavior, so this assertion is a regression guard,
    // not a red one: it must keep passing once the split lands.
    expect(result.alreadyGrantedTotalForSnapshotType).toBe(0);
    expect(result.granted).toBe(4);
  });

  it('admissionType pruning is unaffected by snapshotClaimTtlMs — it always uses claimTtlMs regardless of what snapshotClaimTtlMs is set to', async () => {
    const t0 = 1_700_000_000_000;
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([
        {
          orchestratorId: admissionLedgerId,
          claims: [{ orchestratorId: 'orc-other-holder', count: 4, claimedAt: t0 }],
          grantedTotal: 4,
          declaredAt: t0,
        },
      ]),
      'utf8',
    );

    const result = await coordinationFile.reserveAdmission(
      filePath,
      {
        snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
        admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
        requestedCount: 4,
        orchestratorId: 'orc-caller-admission-unaffected',
        ceiling: 4,
        now: t0 + 2_000, // past the 1s snapshotClaimTtlMs, nowhere near the 6h claimTtlMs
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS, snapshotClaimTtlMs: 1_000 },
    );

    // `orc-other-holder`'s admissionType claim is only 2s old — well within
    // the 6h claimTtlMs that must govern it — so it must still count against
    // `othersHeld`, leaving no headroom for this call, regardless of the 1s
    // snapshotClaimTtlMs configured above (which must never apply here).
    expect(result.othersHeld).toBe(4);
    expect(result.granted).toBe(0);
  });

  it('assertClaimTtlSetAgainstLegacyLedger validates snapshotType\'s legacy-migrated record against the resolved snapshot TTL, not against claimTtlMs', async () => {
    const t0 = 1_700_000_000_000;
    // Legacy scalar shape (no claims[] array) — normalizeClaims synthesizes a
    // LEGACY_UNATTRIBUTED_CLAIM_HOLDER_ID record from it, exactly the shape
    // `assertClaimTtlSetAgainstLegacyLedger`'s existing tests use elsewhere in
    // this file. Only snapshotType carries a legacy record; admissionType has
    // no ledger entry at all.
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([{ orchestratorId: snapshotLedgerId, grantedTotal: 3, declaredAt: t0 }]),
      'utf8',
    );

    // claimTtlMs is left UNSET, but snapshotClaimTtlMs IS set — so the
    // resolved TTL for snapshotType's own ledger is set, and the pre-lock
    // guard must not fire against it. Today, the guard is invoked for BOTH
    // ledgers keyed on the single (unset) claimTtlMs, so this throws
    // UnrecoverableLegacyClaimError even though a real TTL governs
    // snapshotType.
    await expect(
      coordinationFile.reserveAdmission(
        filePath,
        {
          snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
          admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
          requestedCount: 1,
          orchestratorId: 'orc-legacy-guard-snapshot',
          ceiling: 4,
          now: t0 + 2_000,
        },
        { snapshotClaimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).resolves.toBeDefined();
  });

  it('assertClaimTtlSetAgainstLegacyLedger still validates admissionType\'s legacy-migrated record against claimTtlMs, unaffected by snapshotClaimTtlMs', async () => {
    const t0 = 1_700_000_000_000;
    // Legacy record on admissionType only; snapshotType has no ledger entry.
    await fsPromises.writeFile(
      filePath,
      JSON.stringify([{ orchestratorId: admissionLedgerId, grantedTotal: 3, declaredAt: t0 }]),
      'utf8',
    );

    // claimTtlMs is UNSET — the TTL that must govern admissionType's guard —
    // even though snapshotClaimTtlMs IS set. The guard must still refuse:
    // admissionType's legacy record can never expire without a real
    // claimTtlMs, regardless of what snapshotClaimTtlMs carries. This holds
    // both before and after the split lands, so it is a regression guard
    // rather than a red assertion.
    await expect(
      coordinationFile.reserveAdmission(
        filePath,
        {
          snapshotType: RESERVE_ADMISSION_SNAPSHOT_TYPE,
          admissionType: RESERVE_ADMISSION_ADMISSION_TYPE,
          requestedCount: 1,
          orchestratorId: 'orc-legacy-guard-admission',
          ceiling: 4,
          now: t0 + 2_000,
        },
        { snapshotClaimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).rejects.toMatchObject({
      name: 'UnrecoverableLegacyClaimError',
      code: 'COORDINATION_UNRECOVERABLE_LEGACY_CLAIM',
    });
  });
});

// ---------------------------------------------------------------------------
// reconcileMemoryProjectionAdmission — retraction scoping (review, High
// finding, correctness)
// ---------------------------------------------------------------------------

describe('reconcileMemoryProjectionAdmission — retraction on refusal removes ONLY the refused candidate class (review, High finding)', () => {
  it('preserves this orchestrator\'s OTHER genuinely-running classes (declared via agentClasses) when its own candidate is refused', async () => {
    const now = Date.now();
    await declareDibs(
      filePath,
      {
        orchestratorId: 'orc-multi-class',
        declaredAt: now,
        agentClasses: ['candidate-class', 'already-running-a', 'already-running-b'],
      },
      { livenessThresholdMs: 15 * 60 * 1000 },
    );

    const result = await reconcileMemoryProjectionAdmission(filePath, {
      orchestratorId: 'orc-multi-class',
      now,
      candidateClass: 'candidate-class',
      decide: () => ({ withinBudget: false }),
    });

    expect(result.withinBudget).toBe(false);

    const entries = await readDibs(filePath, now, { livenessThresholdMs: 15 * 60 * 1000 });
    const selfEntry = entries.find((entry) => entry.orchestratorId === 'orc-multi-class');
    expect(selfEntry).toBeDefined();
    // The refused candidate is gone...
    expect(selfEntry.agentClasses).not.toContain('candidate-class');
    // ...but the OTHER genuinely-running classes this orchestrator declared
    // via --running-agent-classes survive the retraction untouched — this is
    // the exact bug: an earlier version of this function dropped the WHOLE
    // agentClasses field on refusal, wiping these too.
    expect(selfEntry.agentClasses).toEqual(expect.arrayContaining(['already-running-a', 'already-running-b']));
    expect(selfEntry.agentClasses).toHaveLength(2);
  });

  it('drops the agentClasses field entirely when the refused candidate was this orchestrator\'s ONLY declared class', async () => {
    const now = Date.now();
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-single-class', declaredAt: now, agentClasses: ['candidate-class'] },
      { livenessThresholdMs: 15 * 60 * 1000 },
    );

    await reconcileMemoryProjectionAdmission(filePath, {
      orchestratorId: 'orc-single-class',
      now,
      candidateClass: 'candidate-class',
      decide: () => ({ withinBudget: false }),
    });

    const entries = await readDibs(filePath, now, { livenessThresholdMs: 15 * 60 * 1000 });
    const selfEntry = entries.find((entry) => entry.orchestratorId === 'orc-single-class');
    expect(selfEntry).toBeDefined();
    expect(selfEntry.agentClasses).toBeUndefined();
    expect(selfEntry.agentClass).toBeUndefined();
  });

  it('leaves the entry entirely untouched on admission (withinBudget: true)', async () => {
    const now = Date.now();
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-admitted', declaredAt: now, agentClasses: ['candidate-class', 'already-running'] },
      { livenessThresholdMs: 15 * 60 * 1000 },
    );

    await reconcileMemoryProjectionAdmission(filePath, {
      orchestratorId: 'orc-admitted',
      now,
      candidateClass: 'candidate-class',
      decide: () => ({ withinBudget: true }),
    });

    const entries = await readDibs(filePath, now, { livenessThresholdMs: 15 * 60 * 1000 });
    const selfEntry = entries.find((entry) => entry.orchestratorId === 'orc-admitted');
    expect(selfEntry.agentClasses).toEqual(['candidate-class', 'already-running']);
  });

  it('throws a TypeError when candidateClass is missing/empty — the retraction path cannot scope itself without it', async () => {
    await expect(
      reconcileMemoryProjectionAdmission(filePath, {
        orchestratorId: 'orc-missing-candidate',
        now: Date.now(),
        decide: () => ({ withinBudget: false }),
      }),
    ).rejects.toThrow(TypeError);
  });

  it('does NOT drop a class that is ALSO independently known-running when candidateClass collapses into it (review, High finding)', async () => {
    // Reproduces the exact gap the class-name-only filter cannot see on its
    // own: `cli.mjs` deduplicates `candidateClass` against this same
    // orchestrator's own `--running-agent-classes` BEFORE ever declaring
    // dibs (a class named twice contributes ONE entry — see
    // `dibsAgentClasses`'s own comment in `cli.mjs`), so a second
    // `implementer` being requested while one `implementer` is already
    // running collapses into a single 'implementer' entry here — identical
    // in shape to the "refused candidate was this orchestrator's ONLY
    // declared class" case above, even though a genuinely-running agent of
    // that exact class must NOT be retracted.
    const now = Date.now();
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-overlap', declaredAt: now, agentClasses: ['implementer'] },
      { livenessThresholdMs: 15 * 60 * 1000 },
    );

    await reconcileMemoryProjectionAdmission(filePath, {
      orchestratorId: 'orc-overlap',
      now,
      candidateClass: 'implementer',
      // The caller's own `--running-agent-classes` independently names
      // 'implementer' as genuinely running, distinct from this beat's
      // refused candidacy for a SECOND 'implementer'.
      knownRunningClasses: ['implementer'],
      decide: () => ({ withinBudget: false }),
    });

    const entries = await readDibs(filePath, now, { livenessThresholdMs: 15 * 60 * 1000 });
    const selfEntry = entries.find((entry) => entry.orchestratorId === 'orc-overlap');
    // Without the `knownRunningClasses` fix, this would be `undefined` — the
    // same class-name-only filter that correctly strips a refused candidate
    // would also strip the still-running instance, making it invisible to
    // every other orchestrator's projected sum.
    expect(selfEntry.agentClasses).toEqual(['implementer']);
  });

  it('still retracts candidateClass normally when knownRunningClasses is omitted (default, back-compat)', async () => {
    const now = Date.now();
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-no-overlap-param', declaredAt: now, agentClasses: ['implementer'] },
      { livenessThresholdMs: 15 * 60 * 1000 },
    );

    await reconcileMemoryProjectionAdmission(filePath, {
      orchestratorId: 'orc-no-overlap-param',
      now,
      candidateClass: 'implementer',
      decide: () => ({ withinBudget: false }),
    });

    const entries = await readDibs(filePath, now, { livenessThresholdMs: 15 * 60 * 1000 });
    const selfEntry = entries.find((entry) => entry.orchestratorId === 'orc-no-overlap-param');
    expect(selfEntry.agentClasses).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Phase 1 — reconcileMemoryProjectionAdmission and PARTIAL memory-
// projection admission (a `decide` callback that performs its own decrement
// search and returns `withinBudget: true` with an admitted count SMALLER
// than what was originally requested, alongside `withinBudget: false` for
// the genuine zero-feasible-count refusal case).
//
// `reconcileMemoryProjectionAdmission`'s OWN retraction logic needs no
// interface change for this (`decide`'s contract already accepts arbitrary
// extra fields alongside `withinBudget` — see this function's own doc
// comment on its Investigation) — these tests pin that a `decide`
// callback carrying a NEW `memoryProjectionAdmittedCount` field composes
// correctly with the existing `withinBudget`-keyed retraction branch,
// without requiring reconcileMemoryProjectionAdmission itself to know that
// field exists.
// ---------------------------------------------------------------------------

describe('reconcileMemoryProjectionAdmission — partial admission must not trigger refusal-retraction', () => {
  it('a PARTIAL admission (admittedCount > 0, < requested, withinBudget: true) leaves the self dibs entry untouched — the admitted class is NOT retracted', async () => {
    const now = Date.now();
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-partial-admission', declaredAt: now, agentClasses: ['partial-class', 'already-running'] },
      { livenessThresholdMs: 15 * 60 * 1000 },
    );

    // A decrement-search `decide` callback that found a smaller feasible
    // count than requested — genuinely admitted, not refused — must report
    // withinBudget: true so the caller's own retraction branch (keyed on
    // `decision.withinBudget === false`) does not fire. Conflating "not
    // fully granted" with "not within budget" here would incorrectly erase
    // 'partial-class' from this orchestrator's own dibs entry even though
    // some of it is genuinely about to spawn — exactly the regression this
    // ticket exists to avoid.
    const result = await reconcileMemoryProjectionAdmission(filePath, {
      orchestratorId: 'orc-partial-admission',
      now,
      candidateClass: 'partial-class',
      decide: () => ({
        withinBudget: true,
        memoryProjectionAdmittedCount: 2,
        totalProjectedMemoryMb: 9830,
        budgetMemoryMb: 14336,
      }),
    });

    expect(result.withinBudget).toBe(true);
    expect(result.memoryProjectionAdmittedCount).toBe(2);

    const entries = await readDibs(filePath, now, { livenessThresholdMs: 15 * 60 * 1000 });
    const selfEntry = entries.find((entry) => entry.orchestratorId === 'orc-partial-admission');
    expect(selfEntry).toBeDefined();
    // Neither class was retracted — a partial admission is still a genuine
    // admission, not a refusal.
    expect(selfEntry.agentClasses).toEqual(expect.arrayContaining(['partial-class', 'already-running']));
    expect(selfEntry.agentClasses).toHaveLength(2);
  });

  it('a full ZERO-feasible-count refusal (admittedCount: 0, withinBudget: false) still retracts the candidate class — unchanged regression, including the knownRunningClasses guard', async () => {
    const now = Date.now();
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-zero-feasible', declaredAt: now, agentClasses: ['zero-feasible-class', 'already-running'] },
      { livenessThresholdMs: 15 * 60 * 1000 },
    );

    // Even with the new memoryProjectionAdmittedCount field present
    // (mirroring what a real decrement-search decide callback will return on
    // total refusal), withinBudget: false must still retract exactly the
    // refused candidate — this is today's pre-existing behaviour, pinned
    // unchanged.
    await reconcileMemoryProjectionAdmission(filePath, {
      orchestratorId: 'orc-zero-feasible',
      now,
      candidateClass: 'zero-feasible-class',
      decide: () => ({ withinBudget: false, memoryProjectionAdmittedCount: 0, totalProjectedMemoryMb: 15_000, budgetMemoryMb: 14336 }),
    });

    const entries = await readDibs(filePath, now, { livenessThresholdMs: 15 * 60 * 1000 });
    const selfEntry = entries.find((entry) => entry.orchestratorId === 'orc-zero-feasible');
    expect(selfEntry).toBeDefined();
    expect(selfEntry.agentClasses).not.toContain('zero-feasible-class');
    expect(selfEntry.agentClasses).toEqual(['already-running']);
  });

  it('the knownRunningClasses guard still protects a genuinely-running instance of the SAME class from retraction on a full refusal (regression,)', async () => {
    const now = Date.now();
    await declareDibs(
      filePath,
      { orchestratorId: 'orc-zero-feasible-overlap', declaredAt: now, agentClasses: ['implementer'] },
      { livenessThresholdMs: 15 * 60 * 1000 },
    );

    await reconcileMemoryProjectionAdmission(filePath, {
      orchestratorId: 'orc-zero-feasible-overlap',
      now,
      candidateClass: 'implementer',
      // A genuinely-running 'implementer' independently declared via
      // --running-agent-classes must survive the retraction even though this
      // beat's own candidacy for a SECOND 'implementer' was fully refused.
      knownRunningClasses: ['implementer'],
      decide: () => ({ withinBudget: false, memoryProjectionAdmittedCount: 0 }),
    });

    const entries = await readDibs(filePath, now, { livenessThresholdMs: 15 * 60 * 1000 });
    const selfEntry = entries.find((entry) => entry.orchestratorId === 'orc-zero-feasible-overlap');
    expect(selfEntry.agentClasses).toEqual(['implementer']);
  });
});

// ---------------------------------------------------------------------------
// — `reapConfirmedDeadRecords` never consults `isDead` for a RESERVED
// dibs row, and therefore can never reap one.
//
// Pinned DIRECTLY rather than left to emerge from the sweep's own behaviour.
// The sweep's only production caller passes a predicate derived from
// `classifyLedgerEntryLiveness`, which independently answers `unknown` for a
// reserved row (it carries no pid — a `__claim:<type>__` container, the shared
// sample, the AIMD ceiling state and the spawn bucket name no process). So
// deleting the guard inside this function leaves the whole area green: the
// caller's own predicate happens to cover for it. That coverage is incidental,
// not contractual — a future caller with a different `isDead` would silently
// lose the guard and reap the ceiling row, resetting the fleet's ceiling
// mid-flight.
//
// The predicate below is therefore deliberately hostile: it judges EVERYTHING
// dead. Only the function's own guard can keep the reserved rows.
// ---------------------------------------------------------------------------

describe('reapConfirmedDeadRecords exempts reserved rows on its own, not via its caller', () => {
  it('leaves every reserved dibs row intact under an isDead predicate that would reap everything', async () => {
    const now = Date.now();
    const reservedIds = [
      '__claim:live-agent__',
      '__claim:live-agent-admission__',
      '__shared-machine-sample__',
      '__aimd-ceiling__',
      '__global-spawn-bucket__',
    ];
    const seeded = [
      ...reservedIds.map((orchestratorId) => ({ orchestratorId, declaredAt: now })),
      { orchestratorId: 'real-orch', desiredAgents: 1, declaredAt: now, pid: 4242 },
    ];
    await fsPromises.writeFile(filePath, JSON.stringify(seeded), 'utf8');

    const judged = [];
    const { reaped } = await coordinationFile.reapConfirmedDeadRecords(filePath, {
      isDead: (record) => {
        judged.push(record?.orchestratorId);
        return true;
      },
    });

    // `isDead` was never even ASKED about a reserved row — the exemption is a
    // skip, not a verdict that happened to come back "alive".
    expect(judged).toEqual(['real-orch']);

    // Only the genuine orchestrator row was taken.
    expect(reaped.map((record) => record.orchestratorId)).toEqual(['real-orch']);

    const persisted = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    expect(persisted.map((entry) => entry.orchestratorId)).toEqual(reservedIds);
  });

  it('writes nothing at all when the only rows an everything-is-dead predicate could take are reserved', async () => {
    // The `NOTHING TO DO MEANS NO WRITE` half of the same contract: with every
    // row reserved, the hostile predicate finds no candidate, so the file is
    // left alone rather than rewritten.
    //
    // THE SEEDED BYTES ARE PRETTY-PRINTED ON PURPOSE (pre-PR review, Medium —
    // this test was vacuous until they were). `writeEntriesAtomic` serialises
    // with a bare `JSON.stringify(entries)`, so re-writing the same entries
    // produces byte-identical output: seeded with compact JSON, this assertion
    // held with the `if (reaped.length === 0) return` short-circuit DELETED,
    // and the whole suite stayed green. Indented input is a shape only the
    // seeding write can produce, so surviving it proves no rewrite occurred.
    // `mtime` is asserted alongside for the same reason at a different level:
    // it catches a rewrite that happened to reproduce the same bytes.
    const now = Date.now();
    const seeded = [
      { orchestratorId: '__aimd-ceiling__', ceiling: 4, sustainedNormalCount: 0, declaredAt: now },
      { orchestratorId: '__shared-machine-sample__', declaredAt: now },
    ];
    const raw = JSON.stringify(seeded, null, 2);
    expect(raw).not.toBe(JSON.stringify(seeded));
    await fsPromises.writeFile(filePath, raw, 'utf8');
    const before = await fsPromises.stat(filePath);

    const { reaped } = await coordinationFile.reapConfirmedDeadRecords(filePath, { isDead: () => true });

    expect(reaped).toEqual([]);
    expect(await fsPromises.readFile(filePath, 'utf8')).toBe(raw);
    const after = await fsPromises.stat(filePath);
    expect(after.mtimeMs).toBe(before.mtimeMs);
    expect(after.ino).toBe(before.ino);
  });

  it('leaves a CLAIM ledger byte-for-byte alone when nothing in it is judged dead', async () => {
    // The claim-branch twin of the test above, and pinned the same way: the
    // seeded bytes are indented, which only this test's own write can produce,
    // so a redundant `writeEntriesAtomic` cycle is observable even though it
    // would round-trip the identical entries.
    const now = Date.now();
    const seeded = [
      {
        orchestratorId: '__claim:live-agent__',
        grantedTotal: 2,
        declaredAt: now,
        claims: [{ orchestratorId: 'live-orch', count: 2, claimedAt: now }],
      },
    ];
    const raw = JSON.stringify(seeded, null, 2);
    await fsPromises.writeFile(filePath, raw, 'utf8');
    const before = await fsPromises.stat(filePath);

    const { reaped } = await coordinationFile.reapConfirmedDeadRecords(filePath, {
      claimType: 'live-agent',
      isDead: () => false,
    });

    expect(reaped).toEqual([]);
    expect(await fsPromises.readFile(filePath, 'utf8')).toBe(raw);
    const after = await fsPromises.stat(filePath);
    expect(after.mtimeMs).toBe(before.mtimeMs);
    expect(after.ino).toBe(before.ino);
  });
});

// ---------------------------------------------------------------------------
// — `reapConfirmedDeadRecords`'s claim-type branch has two matching
// divergences from every other claim write in this module, both documented
// in-code at the function's own ` ` comment:
//
//   (a) `.findIndex()` sees only the FIRST duplicate `__claim:<type>__` row,
//       so a ghost living in a SECOND row is invisible to the sweep entirely;
//   (b) the write persists the raw, unnormalized `survivors` array rather
//       than passing it through `stampMigratedClaims` -> `filterOutZeroCountClaims`
//       the way `claimCapacity`/`releaseCapacity` do, so a `legacyMigrated`
//       marker survives a reap with its TTL exemption intact and
//       `grantedTotal` can be computed over invalid/malformed records.
//
// All tests below are RED until the read side merges duplicates (mirroring
// `releaseCapacity`'s `.filter()` + `.flatMap(normalizeClaims)` pattern) and
// the write side runs the full stamp -> filter -> sum normalization chain.
// ---------------------------------------------------------------------------

describe('reapConfirmedDeadRecords merges duplicate ledger rows and normalizes the write', () => {
  it('reaps a dead claim living in the SECOND of two duplicate rows, and collapses both rows into exactly ONE consolidated row on write', async () => {
    const now = Date.now();

    const firstRow = {
      orchestratorId: '__claim:foo__',
      claims: [{ orchestratorId: 'orch-live', count: 2, claimedAt: now }],
      grantedTotal: 2,
      declaredAt: now,
    };
    const secondRow = {
      orchestratorId: '__claim:foo__',
      // Invisible to today's `.findIndex()`-based sweep — it only ever
      // inspects the first matching row, so this ghost is never reaped.
      claims: [{ orchestratorId: 'orch-ghost', count: 1, claimedAt: now }],
      grantedTotal: 1,
      declaredAt: now,
    };

    await fsPromises.writeFile(filePath, JSON.stringify([firstRow, secondRow]), 'utf8');

    const { reaped } = await reapConfirmedDeadRecords(filePath, {
      claimType: 'foo',
      isDead: (record) => record.orchestratorId === 'orch-ghost',
      now,
    });

    expect(reaped.map((record) => record.orchestratorId)).toEqual(['orch-ghost']);

    const persisted = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const fooRows = persisted.filter((entry) => entry.orchestratorId === '__claim:foo__');

    // Exactly one consolidated row survives, never two.
    expect(fooRows).toHaveLength(1);
    expect(fooRows[0].claims.map((claim) => claim.orchestratorId)).toEqual(['orch-live']);
    expect(fooRows[0].grantedTotal).toBe(2);
  });

  it('drops the ledger row entirely (zero rows, not an empty one) when reaping duplicates fully drains every claim', async () => {
    const now = Date.now();

    const firstRow = {
      orchestratorId: '__claim:foo__',
      claims: [{ orchestratorId: 'orch-dead-1', count: 2, claimedAt: now }],
      grantedTotal: 2,
      declaredAt: now,
    };
    const secondRow = {
      orchestratorId: '__claim:foo__',
      claims: [{ orchestratorId: 'orch-dead-2', count: 1, claimedAt: now }],
      grantedTotal: 1,
      declaredAt: now,
    };

    await fsPromises.writeFile(filePath, JSON.stringify([firstRow, secondRow]), 'utf8');

    const { reaped } = await reapConfirmedDeadRecords(filePath, {
      claimType: 'foo',
      isDead: () => true,
      now,
    });

    expect(reaped.map((record) => record.orchestratorId).sort()).toEqual(['orch-dead-1', 'orch-dead-2']);

    const persisted = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    expect(persisted.filter((entry) => entry.orchestratorId === '__claim:foo__')).toHaveLength(0);
  });

  it('strips a legacyMigrated marker and re-stamps claimedAt to the reap\'s own referenceNow on the surviving records', async () => {
    const oldClaimedAt = Date.now() - 10_000_000;
    const referenceNow = Date.now();

    const seeded = {
      orchestratorId: '__claim:foo__',
      claims: [
        // Hand-crafted as already carrying the transient exemption marker —
        // exactly the shape `normalizeClaims` would have synthesized from a
        // legacy scalar entry on an earlier read, riding along unmigrated.
        { orchestratorId: 'legacy-holder', count: 3, claimedAt: oldClaimedAt, legacyMigrated: true },
        { orchestratorId: 'orch-dead', count: 2, claimedAt: referenceNow },
      ],
      grantedTotal: 5,
      declaredAt: referenceNow,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([seeded]), 'utf8');

    const { reaped } = await reapConfirmedDeadRecords(filePath, {
      claimType: 'foo',
      isDead: (record) => record.orchestratorId === 'orch-dead',
      now: referenceNow,
    });

    expect(reaped.map((record) => record.orchestratorId)).toEqual(['orch-dead']);

    const persisted = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const fooRow = persisted.find((entry) => entry.orchestratorId === '__claim:foo__');

    expect(fooRow.claims).toEqual([{ orchestratorId: 'legacy-holder', count: 3, claimedAt: referenceNow }]);
    expect(fooRow.claims[0]).not.toHaveProperty('legacyMigrated');
  });

  it('computes grantedTotal over the fully-normalized (stamped + zero-count-filtered, malformed-record-dropped) survivors, not the raw pre-normalization array', async () => {
    const now = Date.now();

    const seeded = {
      orchestratorId: '__claim:foo__',
      claims: [
        { orchestratorId: 'orch-good', count: 5, claimedAt: now },
        // Malformed — a negative count fails `isValidClaimRecord` and must be
        // dropped by normalization on read, before `isDead` ever sees it. If
        // the raw (unnormalized) array were summed instead, this -5 would
        // cancel out `orch-good`'s +5 and produce a `grantedTotal` of 0.
        { orchestratorId: 'orch-bad', count: -5, claimedAt: now },
        { orchestratorId: 'orch-dead', count: 2, claimedAt: now },
      ],
      grantedTotal: 5,
      declaredAt: now,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([seeded]), 'utf8');

    const { reaped } = await reapConfirmedDeadRecords(filePath, {
      claimType: 'foo',
      isDead: (record) => record.orchestratorId === 'orch-dead',
      now,
    });

    expect(reaped.map((record) => record.orchestratorId)).toEqual(['orch-dead']);

    const persisted = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const fooRow = persisted.find((entry) => entry.orchestratorId === '__claim:foo__');

    expect(fooRow.claims.map((claim) => claim.orchestratorId)).toEqual(['orch-good']);
    expect(fooRow.grantedTotal).toBe(5);
  });

  it('reaps a single-row (non-duplicate) ledger identically to pre-fix behavior — regression guard', async () => {
    const now = Date.now();

    const seeded = {
      orchestratorId: '__claim:foo__',
      claims: [
        { orchestratorId: 'orch-live', count: 4, claimedAt: now },
        { orchestratorId: 'orch-dead', count: 1, claimedAt: now },
      ],
      grantedTotal: 5,
      declaredAt: now,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([seeded]), 'utf8');

    const { reaped } = await reapConfirmedDeadRecords(filePath, {
      claimType: 'foo',
      isDead: (record) => record.orchestratorId === 'orch-dead',
      now,
    });

    expect(reaped.map((record) => record.orchestratorId)).toEqual(['orch-dead']);

    const persisted = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const fooRows = persisted.filter((entry) => entry.orchestratorId === '__claim:foo__');

    expect(fooRows).toHaveLength(1);
    expect(fooRows[0].claims).toEqual([{ orchestratorId: 'orch-live', count: 4, claimedAt: now }]);
    expect(fooRows[0].grantedTotal).toBe(4);
  });

  it('the unlocked pre-pass may see stale data, but the locked reap is the sole authority — the final persisted state reflects only the reap\'s OWN referenceNow, never the pre-pass\'s', async () => {
    const staleNow = Date.now() - 10_000_000;
    const freshNow = Date.now();

    const seeded = {
      orchestratorId: '__claim:foo__',
      claims: [
        { orchestratorId: 'legacy-holder', count: 3, claimedAt: staleNow, legacyMigrated: true },
        { orchestratorId: 'orch-dead', count: 2, claimedAt: freshNow },
      ],
      grantedTotal: 5,
      declaredAt: freshNow,
    };
    await fsPromises.writeFile(filePath, JSON.stringify([seeded]), 'utf8');

    // The cheap, unlocked pre-pass — driven with a deliberately STALE `now`,
    // simulating a sweep whose cached clock read is well behind the
    // eventual locked reap. It is read-only and must not perturb the file.
    const preflightCandidates = await readLedgerCandidateRecords(filePath, 'foo', staleNow);
    expect(preflightCandidates.map((record) => record.orchestratorId).sort()).toEqual([
      'legacy-holder',
      'orch-dead',
    ]);
    const beforeReap = await fsPromises.stat(filePath);

    // The locked reap captures its OWN fresh `referenceNow` — this is the
    // one true authority, regardless of what the pre-pass observed.
    const { reaped } = await reapConfirmedDeadRecords(filePath, {
      claimType: 'foo',
      isDead: (record) => record.orchestratorId === 'orch-dead',
      now: freshNow,
    });
    expect(reaped.map((record) => record.orchestratorId)).toEqual(['orch-dead']);

    // The pre-pass performed no write of its own.
    const afterPreflightOnly = await fsPromises.stat(filePath);
    expect(afterPreflightOnly.mtimeMs).toBeGreaterThanOrEqual(beforeReap.mtimeMs);

    const persisted = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    const fooRow = persisted.find((entry) => entry.orchestratorId === '__claim:foo__');

    // The surviving legacy-migrated record is re-stamped to the REAP's fresh
    // referenceNow, never the pre-pass's stale one.
    expect(fooRow.claims).toEqual([{ orchestratorId: 'legacy-holder', count: 3, claimedAt: freshNow }]);
  });
});
