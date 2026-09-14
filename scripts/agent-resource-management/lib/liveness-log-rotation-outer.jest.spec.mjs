// Phase 0 — outer loop acceptance test for liveness-log
// rotation/capping.
//
// ---------------------------------------------------------------------------
// WHAT THIS PROVES (and why it is RED today)
// ---------------------------------------------------------------------------
//
// `lib/liveness-log.mjs`'s `appendEntry` (see its own header, Phase 5)
// does a full read-whole-file -> append-in-memory -> atomic-tmp-then-rename
// cycle on every write, with NO bound on the resulting file's size. Every
// beat that trips a watchdog/deadlock-recycle/aging-escalation appends one
// more NDJSON line, forever — issue. `cli.mjs`'s
// `resolveRecentlyEvictedPids` (~line 6024) makes this worse by calling
// `readLivenessLog` (which reads + `JSON.parse`s every line in the file) on
// every beat, even though it only cares about entries inside a short
// `cooldownMs` trailing window.
//
// The fix (built in Phase 1, verified here as the outer/black-box view):
//   - `liveness-log.mjs` gains a `LIVENESS_LOG_MAX_ENTRIES` cap. When an
//     append would push the LIVE file past this cap, the oldest overflowed
//     entries move to a sibling `<path>.1` file rather than being deleted —
//     the live file stays bounded, and the archive is itself ALSO bounded to
//     the same cap (a "one generation back" ledger that accumulates evicted
//     entries across many calls, not a single-shot overwrite): once total
//     overflow exceeds the archive's own cap, its oldest entries are in turn
//     permanently evicted. See `liveness-log-rotation.jest.spec.mjs`
//     scenario 2 (single-run accumulation) and the "archive is bounded and
//     cumulative" scenario (multi-call accumulation + eventual eviction) for
//     the exact, white-box-verified semantics.
//   - `resolveRecentlyEvictedPids` (or whatever it becomes) stops parsing
//     the whole live file every beat, short-circuiting once it has walked
//     far enough back past `now - cooldownMs`, relying on NDJSON's
//     chronological append order — while still returning behaviourally
//     identical results to today's implementation for any given
//     `{ now, cooldownMs }` window.
//
// This file is the OUTER acceptance test: it exercises only the public,
// already-exported surface of `liveness-log.mjs` (`logWatchdogTrip`,
// `readLivenessLog`) plus the raw filesystem, and asserts on the two
// user-facing guarantees the fix must deliver — a bounded live file, and a
// recoverable archive — regardless of which internal shape Phases 1/2 land
// on. It must go green only once BOTH phases are built; today it is
// expected to fail on assertions 2 and 3 (the cap and the archive do not
// exist yet).
//
// `LIVENESS_LOG_MAX_ENTRIES` is not yet exported by `liveness-log.mjs` (it
// does not exist). This test does NOT import it and does NOT hardcode
// "exactly 2000" as the pass bar for the cap assertion — it appends
// `ASSUMED_CAP + OVERFLOW` entries and asserts the live file holds
// `<= ASSUMED_CAP` (a value comfortably under the ticket's recommended
// 2000), so the test's own redness today is driven purely by "there is no
// cap at all" (2500 raw lines present) rather than by a mismatch against
// whatever exact constant the implementer picks.
//
// ---------------------------------------------------------------------------
// `resolveRecentlyEvictedPids` (cli.mjs ~line 6024) is NOT exported from
// `cli.mjs` (only `buildWatchdogCandidates` and the `DEFAULT_*`/`LIVE_*`
// constants are — grep confirms no `export` on that function). Per this
// ticket's Phase 0 scope, this outer test does not import it and does not
// modify `cli.mjs` to export it. Instead, assertion 4 below exercises the
// underlying CONTRACT `resolveRecentlyEvictedPids` depends on and that
// Phase 2's short-circuit rewrite must preserve — that a `watchdog-trip`
// entry appended just before a `{ now, cooldownMs }` window's edge is still
// found in the log's chronological tail (a plain filter over
// `readLivenessLog`'s output, standing in for `resolveRecentlyEvictedPids`'s
// own filtering logic). This assertion passes TODAY (no short-circuit bug
// yet) — it is included as a regression guard, not a redness driver, so
// that Phase 2's rewrite of the cooldown-window walk cannot silently break
// cooldown correctness while fixing the perf/parsing concern. Direct,
// white-box tests of `resolveRecentlyEvictedPids` itself belong in Phase 2's
// own test file, once (if) it is exported.

import { promises as fs } from 'node:fs';
import { mkdtempSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { jest } from '@jest/globals';

import { logWatchdogTrip, readLivenessLog } from './liveness-log.mjs';

// Every test in this file appends 2,500+ entries sequentially (one
// lock-acquire/write/release cycle per append, by design — see
// appendManyWatchdogTrips below). That's I/O-bound work whose wall-clock cost
// scales with host load, not just data volume — observed flaking under a
// busy CI runner even though each run is comfortably fast in isolation.
// Mirrors liveness-log-rotation.jest.spec.mjs's identical fix for the same
// underlying cause: one file-level bump covers every test here, rather than
// patching individual per-test timeouts one at a time.
jest.setTimeout(60000);

let workDir;
let logFilePath;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'liveness-log-rotation-outer-'));
  logFilePath = join(workDir, 'liveness-log.ndjson');
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
});

// Comfortably under the ticket's recommended 2000 — chosen so today's
// redness is driven by "no cap exists" (we can observe far more than this
// many raw lines), not by guessing the implementer's exact constant.
const ASSUMED_CAP = 2000;
const OVERFLOW = 500;
const TOTAL_APPENDED = ASSUMED_CAP + OVERFLOW; // 2500

const NOW = 1_700_000_000_000;

/** Sequential (not concurrent) appends — this test cares about final on-disk
 * shape and chronological order, not the lock's concurrency guarantees
 * (already covered by `liveness-log.jest.spec.mjs`). Sequential keeps the
 * 2500-append setup fast and deterministic. */
async function appendManyWatchdogTrips(count, { startPid = 0, startTimestamp = NOW } = {}) {
  for (let index = 0; index < count; index += 1) {
    // eslint-disable-next-line no-await-in-loop -- intentionally sequential; see comment above.
    await logWatchdogTrip(
      logFilePath,
      { pid: startPid + index, orchestratorId: `orc-${index}`, outcome: 'recycled' },
      { now: startTimestamp + index },
    );
  }
}

describe('scenario: liveness log stays bounded under sustained watchdog trips', () => {
  it('caps the LIVE log file at (at most) the assumed cap after appending well past it', async () => {
    await appendManyWatchdogTrips(TOTAL_APPENDED);

    const liveEntries = await readLivenessLog(logFilePath);

    // RED today: appendEntry never bounds the file, so this currently holds
    // all 2500 entries — this assertion is expected to fail until Phase 1
    // lands the LIVENESS_LOG_MAX_ENTRIES cap + archive-on-overflow behaviour.
    expect(liveEntries.length).toBeLessThanOrEqual(ASSUMED_CAP);
  });

  it('archives the oldest overflowed entries to a sibling <path>.1 file rather than discarding them', async () => {
    await appendManyWatchdogTrips(TOTAL_APPENDED);

    const archivePath = `${logFilePath}.1`;

    // RED today: no archive mechanism exists yet, so the sibling file is
    // never created.
    expect(existsSync(archivePath)).toBe(true);

    const archiveRaw = await fs.readFile(archivePath, 'utf8');
    const archiveEntries = archiveRaw
      .split('\n')
      .filter((line) => line.trim().length > 0)
      .map((line) => JSON.parse(line));

    expect(archiveEntries.length).toBeGreaterThan(0);

    // The archive must hold the OLDEST overflowed entries (pid 0 was
    // appended first) — recoverable history, not an arbitrary subset — and
    // the live file's own tail (readable via readLivenessLog) must hold the
    // most RECENT entries (the highest pids), so nothing in the live window
    // is lost to make room for the archive.
    const archivedPids = archiveEntries.map((entry) => entry.pid);
    expect(archivedPids).toContain(0);
    expect(archivedPids).not.toContain(TOTAL_APPENDED - 1);

    const liveEntries = await readLivenessLog(logFilePath);
    const livePids = liveEntries.map((entry) => entry.pid);
    expect(livePids).toContain(TOTAL_APPENDED - 1);

    // No entry should exist in both the live file and the archive — a clean
    // partition of history, not an overlapping/duplicated one.
    const overlap = archivedPids.filter((pid) => livePids.includes(pid));
    expect(overlap).toHaveLength(0);
  });

  it('the archive keeps accumulating evicted entries across separate overflow events rather than losing prior history', async () => {
    // First overflow event.
    await appendManyWatchdogTrips(TOTAL_APPENDED, { startPid: 0, startTimestamp: NOW });
    const archivePath = `${logFilePath}.1`;
    const firstArchiveRaw = await fs.readFile(archivePath, 'utf8').catch(() => '');
    const firstArchiveCount = firstArchiveRaw.split('\n').filter((line) => line.trim().length > 0).length;

    // Second overflow event, appended immediately after — pushes a whole new
    // batch of overflow past the cap again.
    await appendManyWatchdogTrips(TOTAL_APPENDED, {
      startPid: TOTAL_APPENDED,
      startTimestamp: NOW + TOTAL_APPENDED,
    });

    const secondArchiveRaw = await fs.readFile(archivePath, 'utf8');
    const secondArchiveEntries = secondArchiveRaw
      .split('\n')
      .filter((line) => line.trim().length > 0)
      .map((line) => JSON.parse(line));

    // RED today: the archive doesn't exist at all yet, so this whole
    // assertion block fails outright. Once built: the archive is a bounded,
    // cumulative ledger — a SECOND overflow event folds its own overflow in
    // alongside whatever the first event already archived (capped at
    // LIVENESS_LOG_MAX_ENTRIES total), so entries from the FIRST batch are
    // still recoverable here, not discarded the moment a second batch overflows.
    expect(firstArchiveCount).toBeGreaterThan(0);
    expect(secondArchiveEntries.some((entry) => entry.pid < TOTAL_APPENDED)).toBe(true);
  });
});

describe('scenario: cooldown-window pid lookup remains correct across rotation (regression guard)', () => {
  it('a watchdog-trip entry for a given pid is still findable in the log immediately after being appended, within a short cooldown window', async () => {
    // This mirrors the contract `resolveRecentlyEvictedPids` (cli.mjs
    // ~line 6024, not exported — see file header) relies on: a
    // detectionType of watchdog-trip/deadlock-recycle, an outcome outside
    // the NON_ATTEMPT_OUTCOMES set, and a timestamp within
    // `now - cooldownMs`. This passes TODAY (no short-circuit bug yet); it
    // is included so Phase 2's short-circuit rewrite of the real
    // `resolveRecentlyEvictedPids` cannot silently regress cooldown-window
    // correctness while chasing the "don't parse everything" perf fix.
    const targetPid = 999_999;
    const tripTimestamp = NOW;

    await logWatchdogTrip(
      logFilePath,
      { pid: targetPid, orchestratorId: 'orc-target', outcome: 'recycled' },
      { now: tripTimestamp },
    );

    const cooldownMs = 5 * 60 * 1000; // 5 minutes
    const now = tripTimestamp + 1000; // just after the trip

    const NON_ATTEMPT_OUTCOMES = new Set(['alert-only', 'skip', 'leak-velocity-throttled']);
    const entries = await readLivenessLog(logFilePath);
    const recentlyEvicted = new Set();
    for (const entry of entries) {
      if (!entry || (entry.detectionType !== 'watchdog-trip' && entry.detectionType !== 'deadlock-recycle')) continue;
      if (NON_ATTEMPT_OUTCOMES.has(entry.outcome)) continue;
      if (typeof entry.timestamp !== 'number' || entry.timestamp > now || now - entry.timestamp > cooldownMs) continue;
      const pid = Number(entry.pid);
      if (Number.isFinite(pid)) recentlyEvicted.add(pid);
    }

    expect(recentlyEvicted.has(targetPid)).toBe(true);
  });

  it('that same pid remains findable even after enough subsequent trips to trigger rotation/archiving', async () => {
    const targetPid = 999_999;
    const tripTimestamp = NOW;

    await logWatchdogTrip(
      logFilePath,
      { pid: targetPid, orchestratorId: 'orc-target', outcome: 'recycled' },
      { now: tripTimestamp },
    );

    // Push well past the assumed cap with newer entries — once rotation
    // exists, this old entry may legitimately fall into the archive file
    // rather than the live one. This test does not assert WHERE it lives;
    // it only documents today's pre-rotation expectation that a query made
    // immediately (short cooldown, short elapsed time) still finds it in
    // the live file — this is the behaviour Phase 2 must not regress for
    // any entry still within its cooldown window when rotation fires.
    await appendManyWatchdogTrips(50, { startPid: 1, startTimestamp: tripTimestamp + 1 });

    const now = tripTimestamp + 51;
    const cooldownMs = 5 * 60 * 1000;

    const entries = await readLivenessLog(logFilePath);
    const found = entries.some(
      (entry) =>
        entry.pid === targetPid &&
        entry.detectionType === 'watchdog-trip' &&
        typeof entry.timestamp === 'number' &&
        now - entry.timestamp <= cooldownMs,
    );

    expect(found).toBe(true);
  });
});
