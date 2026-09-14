// Phase 1 — unit/integration tests for the rotation/cap logic inside
// `liveness-log.mjs`'s `appendEntry`. Implemented and green.
//
// ---------------------------------------------------------------------------
// WHAT THIS SPECIFIES (this file IS the contract `appendEntry`'s rotation
// branch satisfies; see the Build Plan and the Phase 0 outer acceptance test
// `liveness-log-rotation-outer.jest.spec.mjs`, which this file complements
// with white-box coverage of the exact rotation mechanics: the boundary, the
// archive-overwrite, the lock discipline, and the fail-open contract on a
// PARTIAL rotation failure).
//
//   LIVENESS_LOG_MAX_ENTRIES -> number (2000), newly exported by
//   `liveness-log.mjs`. This spec hardcodes `2000` as `ASSUMED_CAP` rather
//   than importing the constant for every assertion (dynamic `import()` of a
//   not-yet-existing named export resolves to `undefined`, not a throw — so
//   a stray `LIVENESS_LOG_MAX_ENTRIES: undefined` would silently make
//   `undefined`-based arithmetic pass or fail for the wrong reason). A
//   dedicated test below (`0. exported cap constant`) DOES import it, so
//   there is exactly one place this suite would report a real mismatch
//   between `ASSUMED_CAP` and whatever the implementer actually exports.
//
//   `appendEntry`'s new rotation behaviour (invoked indirectly via
//   `logWatchdogTrip`/`logAgingEscalation`/`logDeadlockRecycle`, all thin
//   wrappers over the same private `appendEntry` per `liveness-log.mjs`'s
//   existing header): after appending, if the live file's entry count
//   exceeds `LIVENESS_LOG_MAX_ENTRIES`, the OLDEST overflowed entries move to
//   a sibling `<logFilePath>.1` file. Every over-cap append folds together
//   whatever is CURRENTLY archived plus the full live text plus the new
//   entry, then re-splits: the newest `LIVENESS_LOG_MAX_ENTRIES` entries go
//   to the live file, and the archive is rewritten to hold the OLDEST
//   overflow — but the archive is ITSELF capped at `LIVENESS_LOG_MAX_ENTRIES`
//   (a bounded "one generation back" ledger, cumulative across every append,
//   not unbounded growth in a second file). This means entries evicted from
//   the archive (once total overflow exceeds the archive's own cap) are
//   permanently gone from both files. `readLivenessLog` is unchanged: it
//   reads ONLY the live file, never merges in the archive.
//
// `lib/liveness-log.mjs` implements this behaviour today; every rotation/cap
// assertion below passes.
// ---------------------------------------------------------------------------

import { mkdtempSync, rmSync, existsSync, readFileSync } from 'node:fs';
import { promises as fsPromises } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { jest } from '@jest/globals';

import { logWatchdogTrip, logDeadlockRecycle, readLivenessLog } from './liveness-log.mjs';

// Most scenarios below append 2000-4000+ entries sequentially (one
// lock-acquire/write/release cycle per append, by design — see the comment
// above `appendManyWatchdogTrips`). That's inherently slower than Jest's
// default 5000ms per-test timeout under full-suite parallel-worker CI load
// (observed flaking intermittently in that environment even though every
// individual run in isolation comfortably finishes well under this bound).
// One global bump covers every test in this file rather than reactively
// patching individual slow tests one at a time as new ones are added.
jest.setTimeout(60000);

let workDir;
let logFilePath;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'liveness-log-rotation-'));
  logFilePath = join(workDir, 'liveness-log.ndjson');
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
  jest.restoreAllMocks();
});

const NOW = 1_700_000_000_000;

// Mirrors the ticket/Build Plan's documented value. See the file header for
// why this is hardcoded rather than imported for every assertion.
const ASSUMED_CAP = 2000;

function archivePathFor(path) {
  return `${path}.1`;
}

function readRawLines(path) {
  if (!existsSync(path)) return [];
  return readFileSync(path, 'utf8')
    .split('\n')
    .filter((line) => line.trim().length > 0);
}

function parseLines(lines) {
  return lines.map((line) => JSON.parse(line));
}

/** Sequential (not concurrent) appends — this suite cares about final
 * on-disk shape, boundary counts, and archive content, not the lock's
 * concurrency guarantees (already covered by `liveness-log.jest.spec.mjs`
 * scenario 5, and by test 5 below for the rotation-specific interaction). */
async function appendManyWatchdogTrips(count, { startPid = 0, startTimestamp = NOW } = {}) {
  const results = [];
  for (let index = 0; index < count; index += 1) {
    // eslint-disable-next-line no-await-in-loop -- intentionally sequential; see comment above.
    const result = await logWatchdogTrip(
      logFilePath,
      { pid: startPid + index, orchestratorId: `orc-${startPid + index}`, outcome: 'recycled' },
      { now: startTimestamp + index },
    );
    results.push(result);
  }
  return results;
}

// ---------------------------------------------------------------------------
// 0. exported cap constant
// ---------------------------------------------------------------------------

describe('LIVENESS_LOG_MAX_ENTRIES', () => {
  it('is exported as 2000, matching the ticket-documented cap', async () => {
    const { LIVENESS_LOG_MAX_ENTRIES } = await import('./liveness-log.mjs');
    expect(LIVENESS_LOG_MAX_ENTRIES).toBe(ASSUMED_CAP);
  });
});

// ---------------------------------------------------------------------------
// 1. does not rotate below the cap
// ---------------------------------------------------------------------------

describe('scenario: appending fewer than the cap never rotates', () => {
  // Regression guard: the implemented cap must never rotate for any total
  // that stays under it.
  it('creates no archive file and the live file holds every entry when the total stays under the cap', async () => {
    const belowCap = ASSUMED_CAP - 5;
    await appendManyWatchdogTrips(belowCap);

    expect(existsSync(archivePathFor(logFilePath))).toBe(false);

    const liveEntries = await readLivenessLog(logFilePath);
    expect(liveEntries).toHaveLength(belowCap);
    expect(liveEntries.map((entry) => entry.pid)).toEqual(
      Array.from({ length: belowCap }, (_, index) => index),
    );
  });
});

// ---------------------------------------------------------------------------
// 2. rotates and archives overflow exactly at the cap
// ---------------------------------------------------------------------------

describe('scenario: rotates and archives overflow exactly at the cap', () => {
  it('live file holds exactly the newest cap entries; archive holds exactly the oldest K, same content and order', async () => {
    const overflow = 7;
    const total = ASSUMED_CAP + overflow;
    await appendManyWatchdogTrips(total);

    const liveEntries = await readLivenessLog(logFilePath);
    expect(liveEntries).toHaveLength(ASSUMED_CAP);
    expect(liveEntries.map((entry) => entry.pid)).toEqual(
      Array.from({ length: ASSUMED_CAP }, (_, index) => overflow + index),
    );

    const archiveEntries = parseLines(readRawLines(archivePathFor(logFilePath)));
    expect(archiveEntries).toHaveLength(overflow);
    expect(archiveEntries.map((entry) => entry.pid)).toEqual(
      Array.from({ length: overflow }, (_, index) => index),
    );
  });
});

// ---------------------------------------------------------------------------
// 3. off-by-one at the boundary
// ---------------------------------------------------------------------------

describe('scenario: off-by-one at the boundary', () => {
  // Regression guard for the lower boundary, paired with the upper
  // boundary case immediately below.
  it('appending exactly the cap (not one more) does not rotate', async () => {
    await appendManyWatchdogTrips(ASSUMED_CAP);

    expect(existsSync(archivePathFor(logFilePath))).toBe(false);

    const liveEntries = await readLivenessLog(logFilePath);
    expect(liveEntries).toHaveLength(ASSUMED_CAP);
  });

  it('appending exactly cap + 1 does rotate (the very next entry over the line)', async () => {
    await appendManyWatchdogTrips(ASSUMED_CAP + 1);

    expect(existsSync(archivePathFor(logFilePath))).toBe(true);

    const liveEntries = await readLivenessLog(logFilePath);
    expect(liveEntries).toHaveLength(ASSUMED_CAP);
    expect(liveEntries.map((entry) => entry.pid)).not.toContain(0);

    const archiveEntries = parseLines(readRawLines(archivePathFor(logFilePath)));
    expect(archiveEntries).toHaveLength(1);
    expect(archiveEntries[0].pid).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// 4. the archive is a bounded, cumulative ledger — it accumulates overflow
//    across separate calls, but is itself capped, so its oldest entries
//    eventually fall out (are permanently evicted from both files) once
//    total overflow exceeds the archive's own cap
// ---------------------------------------------------------------------------

describe('scenario: the archive is bounded and cumulative — old entries eventually fall out once total overflow exceeds its own cap', () => {
  // Exceeds 2 * ASSUMED_CAP (4000) by a modest margin (4010) so that some
  // entries are evicted from the ARCHIVE too, not just the live file — the
  // property this scenario exists to pin down. Split across two separate
  // sequential `appendManyWatchdogTrips` calls (2005 + 2005) rather than one
  // big loop, proving the fold-in-then-resplit accumulation holds across
  // distinct calls (each production detection is its own isolated call,
  // potentially far apart in time), not merely within a single loop.
  it('archive holds the newest cap entries of the cumulative overflow window; entries older than that are permanently evicted from both files', async () => {
    const cap = ASSUMED_CAP;
    const batch1 = cap + 5; // 2005
    const batch2 = cap + 5; // 2005
    const total = batch1 + batch2; // 4010, > 2 * cap (4000)

    await appendManyWatchdogTrips(batch1, { startPid: 0, startTimestamp: NOW });
    await appendManyWatchdogTrips(batch2, { startPid: batch1, startTimestamp: NOW + batch1 });

    // Live file holds the newest `cap` of the `total` appends: pids
    // [total - cap, total).
    const liveEntries = await readLivenessLog(logFilePath);
    expect(liveEntries).toHaveLength(cap);
    expect(liveEntries.map((entry) => entry.pid)).toEqual(
      Array.from({ length: cap }, (_, index) => total - cap + index),
    );

    // The archive holds the newest `cap` of the overflow window (append-index
    // range [0, total - cap - 1]) — i.e. pids in
    // [total - 2*cap, total - cap - 1]. Here that's [10, 2009].
    const archiveEntries = parseLines(readRawLines(archivePathFor(logFilePath)));
    expect(archiveEntries).toHaveLength(cap);
    expect(archiveEntries.map((entry) => entry.pid)).toEqual(
      Array.from({ length: cap }, (_, index) => total - 2 * cap + index),
    );

    // Entries older than `total - 2*cap` (here: pids [0, 9]) exceeded even
    // the archive's own cap and are permanently gone from both files — this
    // is the bounded-ledger property, not an implementation accident.
    const evictedPids = Array.from({ length: total - 2 * cap }, (_, index) => index);
    const allSurvivingPids = new Set([
      ...liveEntries.map((entry) => entry.pid),
      ...archiveEntries.map((entry) => entry.pid),
    ]);
    for (const pid of evictedPids) {
      expect(allSurvivingPids.has(pid)).toBe(false);
    }
  });
});

// ---------------------------------------------------------------------------
// 5. rotation survives a concurrent appender holding the fencing lock
// ---------------------------------------------------------------------------

describe('scenario: rotation survives a concurrent appender holding the fencing lock', () => {
  // Proves the fencing lock's serialisation holds up against the
  // implemented rotation: two concurrent callers that together cross the
  // cap both land their entries (recoverable from live+archive combined)
  // rather than one being lost to a rotation-vs-append race.
  it('two near-simultaneous appendEntry calls that push the file past the cap both persist their entries, even though only one physically triggers rotation', async () => {
    // Build up to exactly cap - 1 sequentially, so the next TWO concurrent
    // appends are the ones that cross the boundary together.
    await appendManyWatchdogTrips(ASSUMED_CAP - 1, { startPid: 0, startTimestamp: NOW });

    const concurrentPidA = 90_001;
    const concurrentPidB = 90_002;
    const [resultA, resultB] = await Promise.all([
      logWatchdogTrip(
        logFilePath,
        { pid: concurrentPidA, orchestratorId: 'orc-concurrent-a', outcome: 'recycled' },
        { now: NOW + ASSUMED_CAP },
      ),
      logWatchdogTrip(
        logFilePath,
        { pid: concurrentPidB, orchestratorId: 'orc-concurrent-b', outcome: 'recycled' },
        { now: NOW + ASSUMED_CAP + 1 },
      ),
    ]);

    expect(resultA.written).toBe(true);
    expect(resultB.written).toBe(true);

    const liveEntries = await readLivenessLog(logFilePath);
    const archiveEntries = parseLines(readRawLines(archivePathFor(logFilePath)));
    const allPids = new Set([
      ...liveEntries.map((entry) => entry.pid),
      ...archiveEntries.map((entry) => entry.pid),
    ]);

    // Both concurrently-appended entries must be recoverable from SOME
    // file — neither was silently dropped by the fencing lock being held by
    // the other caller mid-rotation.
    expect(allPids.has(concurrentPidA)).toBe(true);
    expect(allPids.has(concurrentPidB)).toBe(true);

    // Total entry count across both files must equal exactly what was
    // appended (cap - 1 sequential + 2 concurrent) — no duplication, no loss.
    expect(liveEntries.length + archiveEntries.length).toBe(ASSUMED_CAP - 1 + 2);
  });
});

// ---------------------------------------------------------------------------
// 6. a failure during the archive-file write/rename still fails open, and
//    does not silently lose the overflowed entries from both files
// ---------------------------------------------------------------------------

describe('scenario: archive-write failure fails open without losing entries from both files', () => {
  it('resolves { written: false, error } when the archive file write/rename fails, and the overflowed entries remain recoverable from the live file', async () => {
    // Build up to exactly the cap — the NEXT append is the one that would
    // trigger rotation (and, per this test, the simulated archive failure).
    await appendManyWatchdogTrips(ASSUMED_CAP, { startPid: 0, startTimestamp: NOW });

    const archivePath = archivePathFor(logFilePath);
    const originalRename = fsPromises.rename.bind(fsPromises);

    // Fault-injects a failure ONLY for the rename that lands on the archive
    // sibling path (`<logFilePath>.1`), leaving every other rename (the
    // live-file's own atomic tmp-then-rename, the lock file's rename-based
    // release) unaffected — the narrowest possible fault to isolate "the
    // archive write/rename step specifically failed".
    jest.spyOn(fsPromises, 'rename').mockImplementation(async (src, dest) => {
      if (dest === archivePath) {
        throw new Error('simulated archive rename failure');
      }
      return originalRename(src, dest);
    });

    const overflowPid = ASSUMED_CAP; // the (cap + 1)th entry, id `ASSUMED_CAP`.
    const result = await logWatchdogTrip(
      logFilePath,
      { pid: overflowPid, orchestratorId: 'orc-overflow', outcome: 'recycled' },
      { now: NOW + ASSUMED_CAP },
    );

    expect(result.written).toBe(false);
    expect(result.error).toBeInstanceOf(Error);

    jest.restoreAllMocks();

    // No archive was ever successfully created — the failure fired before
    // any archive content landed on disk.
    expect(existsSync(archivePath)).toBe(false);

    // Critically (the edge case this test exists to pin down): the live
    // file must NOT have already been rewritten down to a rotated/truncated
    // shape before the archive write was confirmed. Every entry that
    // existed prior to this failed call — including the oldest one that
    // WOULD have been archived — must still be readable from the live file
    // alone. A rewrite-live-first, archive-second ordering would lose that
    // oldest entry from BOTH files the moment the archive step failed;
    // this assertion is exactly what rules that ordering out.
    const liveEntries = await readLivenessLog(logFilePath);
    expect(liveEntries).toHaveLength(ASSUMED_CAP);
    expect(liveEntries.map((entry) => entry.pid)).toContain(0);

    // The rejected (cap + 1)th entry was never durably appended anywhere —
    // consistent with the failed append never having taken effect at all,
    // rather than a half-applied state.
    expect(liveEntries.map((entry) => entry.pid)).not.toContain(overflowPid);
  });
});

// ---------------------------------------------------------------------------
// 6b. a failure during the LIVE-file write/rename (i.e. AFTER the archive
//     write has already succeeded) leaves a transient duplicate. Phase 1
//     deliberately does NOT heal this away — three rounds of review
//     on a marker-based healing mechanism each surfaced a new unsound edge
//     case, so the design was simplified to accept the duplicate as
//     harmless (see the doc comment on `appendEntry`'s rotation branch in
//     `liveness-log.mjs`). This test now asserts the two properties that
//     actually matter: (a) no entry is ever LOST, and (b) the log still
//     functions correctly for its real consumer, which folds entries into a
//     `Set<number>` of pids — where a duplicate is inert by construction.
// ---------------------------------------------------------------------------

describe('scenario: live-write failure after the archive write already succeeded is harmless', () => {
  it('produces a transient live+archive duplicate that loses nothing and does not corrupt Set-membership semantics for the real consumer', async () => {
    // Build up to exactly the cap — the NEXT append is the one that
    // triggers rotation (and, per this test, the simulated live-file
    // failure that happens AFTER the archive write has already landed).
    await appendManyWatchdogTrips(ASSUMED_CAP, { startPid: 0, startTimestamp: NOW });

    const archivePath = archivePathFor(logFilePath);
    const originalRename = fsPromises.rename.bind(fsPromises);

    // Fault-injects a failure ONLY for the rename that lands on the live
    // file path itself, leaving the archive write (and the lock file's own
    // rename-based release) unaffected — isolating "the archive write
    // succeeded, but the live write immediately after it did not".
    jest.spyOn(fsPromises, 'rename').mockImplementation(async (src, dest) => {
      if (dest === logFilePath) {
        throw new Error('simulated live-file rename failure');
      }
      return originalRename(src, dest);
    });

    const overflowPid = ASSUMED_CAP; // the (cap + 1)th entry, id `ASSUMED_CAP`.
    const failedResult = await logWatchdogTrip(
      logFilePath,
      { pid: overflowPid, orchestratorId: 'orc-overflow', outcome: 'recycled' },
      { now: NOW + ASSUMED_CAP },
    );

    jest.restoreAllMocks();

    expect(failedResult.written).toBe(false);
    expect(failedResult.error).toBeInstanceOf(Error);

    // The archive WAS durably rewritten to its post-rotation state (the
    // single oldest entry, pid 0) — it landed before the live write failed.
    const archiveAfterFailure = parseLines(readRawLines(archivePath));
    expect(archiveAfterFailure.map((entry) => entry.pid)).toEqual([0]);

    // The live file is untouched (still its FULL pre-rotation content) —
    // pid 0 is therefore present in BOTH files right now: a transient
    // duplicate, not a loss.
    const liveAfterFailure = await readLivenessLog(logFilePath);
    expect(liveAfterFailure).toHaveLength(ASSUMED_CAP);
    expect(liveAfterFailure.map((entry) => entry.pid)).toContain(0);

    // The rejected (cap + 1)th entry was never durably appended anywhere.
    expect(liveAfterFailure.map((entry) => entry.pid)).not.toContain(overflowPid);

    // The NEXT append succeeds normally (unmocked). No healing is expected
    // or asserted here — the duplicate (pid 0, in both live and archive) is
    // allowed to persist; this call only needs to keep working normally.
    const recoveryPid = overflowPid + 1;
    const recoveryResult = await logWatchdogTrip(
      logFilePath,
      { pid: recoveryPid, orchestratorId: 'orc-recovery', outcome: 'recycled' },
      { now: NOW + ASSUMED_CAP + 1 },
    );
    expect(recoveryResult.written).toBe(true);

    const liveEntries = await readLivenessLog(logFilePath);
    const archiveEntries = parseLines(readRawLines(archivePath));
    const livePids = liveEntries.map((entry) => entry.pid);
    const archivePids = archiveEntries.map((entry) => entry.pid);

    // (a) No entry is ever lost: every pid from the pre-existing set
    // (0..ASSUMED_CAP-1) plus the recovery pid is still findable somewhere
    // in the union of live+archive — duplicates permitted, absence is not.
    // The failed append's own pid never existed on disk at all.
    const allPids = new Set([...livePids, ...archivePids]);
    for (let pid = 0; pid < ASSUMED_CAP; pid += 1) {
      expect(allPids.has(pid)).toBe(true);
    }
    expect(allPids.has(recoveryPid)).toBe(true);
    expect(allPids.has(overflowPid)).toBe(false);

    // (b) The log still functions correctly for its real consumer
    // (`resolveRecentlyEvictedPids` in `cli.mjs`), which folds
    // watchdog-trip/deadlock-recycle entries into a `Set<number>` of pids.
    // A duplicated pid (0, present in both live and archive right now) is
    // still found exactly once under Set semantics — `Set.add` on an
    // already-present value is a no-op, so the duplicate cannot cause a
    // pid to be double-counted or otherwise corrupt cooldown-window
    // membership.
    const recentlyEvictedPids = new Set();
    for (const entry of [...archiveEntries, ...liveEntries]) {
      if (entry.detectionType === 'watchdog-trip' || entry.detectionType === 'deadlock-recycle') {
        recentlyEvictedPids.add(entry.pid);
      }
    }
    expect(recentlyEvictedPids.has(0)).toBe(true);
    expect([...recentlyEvictedPids].filter((pid) => pid === 0)).toEqual([0]);
  });
});

// ---------------------------------------------------------------------------
// 6c. two GENUINELY DISTINCT detections that merely happen to serialise
//     identically (per `logDeadlockRecycle`'s documented pid-substitution
//     convention: same `orchestratorId`, same `outcome`, no `reason`, same
//     millisecond `timestamp`) must both survive an ORDINARY, non-failing
//     rotation that happens to split them across the archive/live boundary.
//     This is a PERMANENT regression guard: the rotation fold in
//     `liveness-log.mjs` has no dedup/healing mechanism at all (Phase 1,
//, deliberately removed the marker-based healing an earlier
//     iteration added), so it trivially cannot false-collapse anything —
//     this test exists to keep it that way if anyone is ever tempted to
//     reintroduce a content-equality-based dedup.
// ---------------------------------------------------------------------------

describe('scenario: distinct entries that coincidentally serialise identically are never falsely collapsed', () => {
  it('preserves both of two logDeadlockRecycle entries with identical orchestratorId/outcome/timestamp when an ordinary rotation splits them across the archive/live boundary', async () => {
    const sharedParams = { orchestratorId: 'orc-shared', outcome: 'recycled' };

    // The two oldest entries ever appended are content-identical (no `pid`,
    // no `reason` — same serialised line per the doc-commented
    // pid-substitution convention) but are two REAL, distinct detections.
    const dl1 = await logDeadlockRecycle(logFilePath, sharedParams, { now: NOW });
    const dl2 = await logDeadlockRecycle(logFilePath, sharedParams, { now: NOW });
    expect(dl1.written).toBe(true);
    expect(dl2.written).toBe(true);

    // Fill the live file up to (but not past) the cap with unique watchdog
    // trips — no rotation yet.
    await appendManyWatchdogTrips(ASSUMED_CAP - 2, { startPid: 0, startTimestamp: NOW + 1 });

    // The (ASSUMED_CAP + 1)th append triggers rotation. Because the two
    // identical deadlock-recycle lines are the OLDEST two entries in the
    // fold, this rotation's 1-entry overflow archives exactly the first of
    // them and leaves the second as the new live file's oldest entry —
    // landing them precisely astride the archive/live boundary, with no
    // fault injected anywhere in this test.
    const triggerResult = await logWatchdogTrip(
      logFilePath,
      { pid: 'trigger', orchestratorId: 'orc-trigger', outcome: 'recycled' },
      { now: NOW + ASSUMED_CAP },
    );
    expect(triggerResult.written).toBe(true);

    const archiveEntries = parseLines(readRawLines(archivePathFor(logFilePath)));
    const liveEntries = await readLivenessLog(logFilePath);

    // Both distinct deadlock-recycle detections survive across the two
    // files combined — neither was silently collapsed into the other
    // despite serialising identically and sitting exactly at the boundary.
    const deadlockCountAcrossBoth = [...archiveEntries, ...liveEntries].filter(
      (entry) => entry.detectionType === 'deadlock-recycle',
    ).length;
    expect(deadlockCountAcrossBoth).toBe(2);

    expect(archiveEntries).toHaveLength(1);
    expect(archiveEntries[0].detectionType).toBe('deadlock-recycle');

    expect(liveEntries).toHaveLength(ASSUMED_CAP);
    expect(liveEntries[0].detectionType).toBe('deadlock-recycle');
  });
});

// ---------------------------------------------------------------------------
// 7. readLivenessLog continues to reflect ONLY the live file post-rotation
// ---------------------------------------------------------------------------

describe('scenario: readLivenessLog reflects only the live file after rotation', () => {
  it('does not include archived entries once rotation has occurred', async () => {
    const overflow = 3;
    await appendManyWatchdogTrips(ASSUMED_CAP + overflow);

    const archiveEntries = parseLines(readRawLines(archivePathFor(logFilePath)));
    const archivedPids = new Set(archiveEntries.map((entry) => entry.pid));
    expect(archivedPids.size).toBe(overflow);

    const liveEntries = await readLivenessLog(logFilePath);
    for (const entry of liveEntries) {
      expect(archivedPids.has(entry.pid)).toBe(false);
    }
    expect(liveEntries).toHaveLength(ASSUMED_CAP);
  });
});

// ---------------------------------------------------------------------------
// 8. appending when the file does not yet exist is unaffected by the cap
// ---------------------------------------------------------------------------

describe('scenario: first-ever append (ENOENT path) is unaffected by the new cap logic', () => {
  // Regression guard (mirrors `liveness-log.jest.spec.mjs`'s existing
  // ENOENT coverage) proving the cap logic does not disturb the
  // single-entry-on-a-fresh-file path.
  it('appends exactly one entry, no rotation, no archive, exactly as today', async () => {
    expect(existsSync(logFilePath)).toBe(false);

    const result = await logWatchdogTrip(
      logFilePath,
      { pid: 1, orchestratorId: 'orc-first', outcome: 'recycled' },
      { now: NOW },
    );

    expect(result.written).toBe(true);
    expect(existsSync(archivePathFor(logFilePath))).toBe(false);

    const liveEntries = await readLivenessLog(logFilePath);
    expect(liveEntries).toHaveLength(1);
    expect(liveEntries[0]).toMatchObject({ pid: 1, orchestratorId: 'orc-first', outcome: 'recycled' });
  });
});
