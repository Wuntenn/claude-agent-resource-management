// Phase 3 — mocked-`node:fs` unit tests for `./cleanup.mjs`'s real fs
// adapter, at the boundary where it composes with the REAL (not mocked)
// Phase 2 decision function (`selectStaleTempDirs` from
// `./cleanup-age-filter.mjs`). RED by construction: `./cleanup.mjs` does not
// exist yet. Do not add a stub implementation to turn this green.
//
// Split out of `./cleanup.jest.spec.mjs` because `jest.unstable_mockModule`
// mocks apply file-wide once registered — mixing them with that sibling
// file's real `fs.mkdtempSync` scratch-dir helpers would break its
// integration tests. Mirrors this same `lib/` directory's existing split
// between `diagnostic-reports-scanner.jest.spec.mjs` (mocked) and
// `diagnostic-reports-scanner-symlink-realfs.jest.spec.mjs` (real fs).
//
// MOCKING IDIOM: `jest.unstable_mockModule` against `node:fs`, matching
// `diagnostic-reports-scanner.jest.spec.mjs`'s established idiom for mocking
// a `node:` builtin under native ESM in this skill's `lib/` specs (that file
// mocks `node:fs/promises`; this one mocks the sync `node:fs` surface
// `cleanup.mjs`'s adapter is expected to use per the Build Plan — readdirSync
// / statSync / rmSync).
//
// SHAPE THESE TESTS ASSUME (pinned by this file, not yet implemented):
//
//   export function listCandidateEntries(dirPath)
//     -> Array<{ path: string, mtimeMs: number }>
//   Real fs adapter: `fs.readdirSync(dirPath, { withFileTypes: true })` +
//   `fs.statSync(entryPath).mtimeMs` for each immediate child. No filtering
//   of its own — that's `selectStaleTempDirs`'s job, composed on top.
//
//   export function directorySizeBytes(dirPath) -> number
//   Real, recursive byte total for a directory tree (files only).
//
//   export function sweepStaleTempDirs({ dir, ageThresholdMs, now })
//     -> { removedCount: number, reclaimedBytes: number, errors: Array<{ path: string, message: string, kind: 'removal-failed' | 'size-read-failed' }> }
//   Composes `listCandidateEntries` + the REAL `selectStaleTempDirs` +
//   `directorySizeBytes` (computed BEFORE removal) + `fs.rmSync(path, {
//   recursive: true, force: true })` per selected entry. A thrown error from
//   `fs.rmSync` for one candidate is caught, recorded in `errors` with
//   `kind: 'removal-failed'`; a thrown error from the size-read step is
//   caught separately and recorded with `kind: 'size-read-failed'` (removal
//   is still attempted afterwards, and on success DOES count toward
//   `removedCount` — see review finding 2,). Either way the sweep
//   CONTINUES to the remaining candidates — `removedCount`/`reclaimedBytes`
//   reflect only entries that were actually removed.

import { jest } from '@jest/globals';

const readdirSyncMock = jest.fn();
const statSyncMock = jest.fn();
const rmSyncMock = jest.fn();

jest.unstable_mockModule('node:fs', () => ({
  readdirSync: readdirSyncMock,
  statSync: statSyncMock,
  rmSync: rmSyncMock,
}));

const { listCandidateEntries, directorySizeBytes, sweepStaleTempDirs } = await import('./cleanup.mjs');
// The REAL Phase 2 decision function — never mocked. This import is only
// used to eyeball the fixture data in scenario 6 below (the actual
// composition happens inside the mocked `sweepStaleTempDirs`/
// `listCandidateEntries` call, which internally imports the same real
// module).
const { selectStaleTempDirs } = await import('./cleanup-age-filter.mjs');

/** Dirent-shaped object matching `fs.Dirent`'s `isDirectory()` surface. */
function direntDir(name) {
  return { name, isDirectory: () => true, isFile: () => false };
}

beforeEach(() => {
  readdirSyncMock.mockReset();
  statSyncMock.mockReset();
  rmSyncMock.mockReset();
});

describe('cleanup.mjs adapter — composed with the REAL selectStaleTempDirs', () => {
  it('6. readdir-derived entries, passed through the REAL selectStaleTempDirs, correctly filter stale candidates', () => {
    const NOW = 1_700_000_000_000;
    const AGE_THRESHOLD_MS = 2 * 60 * 60 * 1000; // 2h
    const staleMtime = NOW - AGE_THRESHOLD_MS - 1;
    const freshMtime = NOW - 1;

    readdirSyncMock.mockReturnValue([
      direntDir('cdk.out-stale'),
      direntDir('bundling-temp-stale'),
      direntDir('cdk.out-fresh'),
      direntDir('unrelated-dir'),
    ]);
    statSyncMock.mockImplementation((entryPath) => {
      if (entryPath.endsWith('cdk.out-fresh')) return { mtimeMs: freshMtime, size: 0 };
      return { mtimeMs: staleMtime, size: 0 };
    });

    const entries = listCandidateEntries('/scratch');
    // Sanity: the adapter itself does no filtering — all four candidates
    // come back, with real mtimes attached.
    expect(entries.map((entry) => entry.path.split('/').pop()).sort()).toEqual(
      ['bundling-temp-stale', 'cdk.out-fresh', 'cdk.out-stale', 'unrelated-dir'].sort(),
    );

    const selected = selectStaleTempDirs({ entries, now: NOW, ageThresholdMs: AGE_THRESHOLD_MS });
    const selectedNames = selected.map((entry) => entry.path.split('/').pop()).sort();

    expect(selectedNames).toEqual(['bundling-temp-stale', 'cdk.out-stale'].sort());
  });

  it('7. the adapter sums real byte sizes correctly for the reclaimed total', () => {
    statSyncMock.mockImplementation((entryPath) => {
      if (entryPath.endsWith('a.bin')) return { size: 1000, isDirectory: () => false, isFile: () => true };
      if (entryPath.endsWith('b.bin')) return { size: 2500, isDirectory: () => false, isFile: () => true };
      return { size: 0, isDirectory: () => true, isFile: () => false };
    });
    readdirSyncMock.mockImplementation((dirPath) => {
      if (dirPath === '/scratch/cdk.out-stale') return [direntFile('a.bin'), direntFile('b.bin')];
      return [];
    });

    const total = directorySizeBytes('/scratch/cdk.out-stale');

    expect(total).toBe(3500);
  });

  it('3. continues the sweep after one candidate throws mid-removal, still removing and reporting the rest', () => {
    const NOW = 1_700_000_000_000;
    const AGE_THRESHOLD_MS = 2 * 60 * 60 * 1000; // 2h
    const staleMtime = NOW - AGE_THRESHOLD_MS - 1;

    readdirSyncMock.mockImplementation((dirPath) => {
      if (dirPath === '/scratch') {
        return [direntDir('cdk.out-forbidden'), direntDir('cdk.out-ok')];
      }
      return []; // no children under either candidate — treat as empty dirs for size purposes
    });
    statSyncMock.mockImplementation((entryPath) => {
      if (entryPath === '/scratch/cdk.out-forbidden') return { mtimeMs: staleMtime, size: 0, isDirectory: () => true };
      if (entryPath === '/scratch/cdk.out-ok') return { mtimeMs: staleMtime, size: 0, isDirectory: () => true };
      return { size: 0, isDirectory: () => false };
    });
    rmSyncMock.mockImplementation((targetPath) => {
      if (targetPath === '/scratch/cdk.out-forbidden') {
        const error = new Error('EACCES: permission denied');
        error.code = 'EACCES';
        throw error;
      }
      // '/scratch/cdk.out-ok' succeeds silently, matching real fs.rmSync.
    });

    const report = sweepStaleTempDirs({ dir: '/scratch', ageThresholdMs: AGE_THRESHOLD_MS, now: NOW });

    // The forbidden candidate never got removed, but the sweep must not
    // abort — the second candidate is still attempted and counted.
    expect(rmSyncMock).toHaveBeenCalledWith('/scratch/cdk.out-forbidden', expect.objectContaining({ recursive: true, force: true }));
    expect(rmSyncMock).toHaveBeenCalledWith('/scratch/cdk.out-ok', expect.objectContaining({ recursive: true, force: true }));
    expect(report.removedCount).toBe(1);
    expect(report.errors).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ path: '/scratch/cdk.out-forbidden', kind: 'removal-failed' }),
      ]),
    );
  });

  it('8. a size-read failure for one candidate does not crash the sweep or discard accounting for the rest (review finding 1)', () => {
    const NOW = 1_700_000_000_000;
    const AGE_THRESHOLD_MS = 2 * 60 * 60 * 1000; // 2h
    const staleMtime = NOW - AGE_THRESHOLD_MS - 1;

    readdirSyncMock.mockImplementation((dirPath) => {
      if (dirPath === '/scratch') {
        return [direntDir('cdk.out-vanishing'), direntDir('cdk.out-ok')];
      }
      if (dirPath === '/scratch/cdk.out-vanishing') {
        // Simulates a file that disappears between the initial readdir
        // listing and the later size-read walk — the exact scenario this
        // review finding describes.
        throw (() => {
          const error = new Error('ENOENT: no such file or directory');
          error.code = 'ENOENT';
          return error;
        })();
      }
      return []; // '/scratch/cdk.out-ok' has no children — 0 bytes.
    });
    statSyncMock.mockImplementation((entryPath) => {
      if (entryPath === '/scratch/cdk.out-vanishing') return { mtimeMs: staleMtime, size: 0, isDirectory: () => true };
      if (entryPath === '/scratch/cdk.out-ok') return { mtimeMs: staleMtime, size: 0, isDirectory: () => true };
      return { size: 0, isDirectory: () => false };
    });
    rmSyncMock.mockImplementation(() => {
      // Both candidates remove successfully — this test is only about the
      // size-read step, not removal.
    });

    const report = sweepStaleTempDirs({ dir: '/scratch', ageThresholdMs: AGE_THRESHOLD_MS, now: NOW });

    // The sweep must not throw (implicit: reaching this line at all), and
    // must still account for BOTH candidates — the size-read failure for
    // the first must not discard the accumulated report for the second.
    expect(rmSyncMock).toHaveBeenCalledWith('/scratch/cdk.out-vanishing', expect.objectContaining({ recursive: true, force: true }));
    expect(rmSyncMock).toHaveBeenCalledWith('/scratch/cdk.out-ok', expect.objectContaining({ recursive: true, force: true }));
    expect(report.removedCount).toBe(2);
    expect(report.reclaimedBytes).toBe(0);
  });

  it('9. an entry that vanishes between the initial readdir listing and the per-entry statSync is omitted, not thrown (review finding 1)', () => {
    // Mirrors test 8's "vanishing entry" shape, but one stage earlier: the
    // race is in `listCandidateEntries` itself (readdirSync lists it, then
    // statSync throws because it vanished before this call), not in the
    // later `directorySizeBytes`/removal stage.
    readdirSyncMock.mockReturnValue([direntDir('cdk.out-vanishing'), direntDir('cdk.out-ok')]);
    statSyncMock.mockImplementation((entryPath) => {
      if (entryPath === '/scratch/cdk.out-vanishing') {
        const error = new Error('ENOENT: no such file or directory');
        error.code = 'ENOENT';
        throw error;
      }
      return { mtimeMs: 0, size: 0 };
    });

    const entries = listCandidateEntries('/scratch');

    // The sweep must not throw (implicit: reaching this line at all), and
    // the vanished entry must simply be omitted — the remaining valid entry
    // is still returned.
    expect(entries.map((entry) => entry.path)).toEqual(['/scratch/cdk.out-ok']);
  });

  it('10. an entry whose size-read genuinely throws is still removed and counted, tagged distinctly from a removal failure (review finding 2)', () => {
    const NOW = 1_700_000_000_000;
    const AGE_THRESHOLD_MS = 2 * 60 * 60 * 1000; // 2h
    const staleMtime = NOW - AGE_THRESHOLD_MS - 1;

    // A dirent whose `isDirectory()` itself throws — an unusual but possible
    // failure mode inside `directorySizeBytes`'s recursive walk, which
    // guards its own `readdirSync`/`statSync` calls but not this dirent
    // method call. This is the one way `directorySizeBytes` can genuinely
    // throw out to `sweepStaleTempDirs`'s own try/catch around it (every
    // fs-level failure inside `directorySizeBytes` is already swallowed
    // internally and returns 0 rather than throwing).
    const explodingDirent = {
      name: 'child',
      isDirectory: () => {
        throw new Error('corrupt dirent');
      },
    };

    readdirSyncMock.mockImplementation((dirPath) => {
      if (dirPath === '/scratch') {
        return [direntDir('cdk.out-throws'), direntDir('cdk.out-ok')];
      }
      if (dirPath === '/scratch/cdk.out-throws') {
        return [explodingDirent];
      }
      return []; // '/scratch/cdk.out-ok' has no children — 0 bytes.
    });
    statSyncMock.mockImplementation((entryPath) => {
      if (entryPath === '/scratch/cdk.out-throws' || entryPath === '/scratch/cdk.out-ok') {
        return { mtimeMs: staleMtime, size: 0 };
      }
      return { size: 0 };
    });
    rmSyncMock.mockImplementation(() => {
      // Both candidates remove successfully — removal is independent of
      // the size-read outcome.
    });

    const report = sweepStaleTempDirs({ dir: '/scratch', ageThresholdMs: AGE_THRESHOLD_MS, now: NOW });

    // Removal still happens and is counted for BOTH candidates, despite the
    // size-read failure for the first.
    expect(rmSyncMock).toHaveBeenCalledWith('/scratch/cdk.out-throws', expect.objectContaining({ recursive: true, force: true }));
    expect(rmSyncMock).toHaveBeenCalledWith('/scratch/cdk.out-ok', expect.objectContaining({ recursive: true, force: true }));
    expect(report.removedCount).toBe(2);

    // The failure is recorded as `size-read-failed`, never `removal-failed`
    // — a caller must be able to tell this apart from a genuine removal
    // failure (the conflation this review finding calls out).
    const recordedError = report.errors.find((error) => error.path === '/scratch/cdk.out-throws');
    expect(recordedError).toBeDefined();
    expect(recordedError.kind).toBe('size-read-failed');
    expect(report.errors.some((error) => error.kind === 'removal-failed')).toBe(false);
  });

  it('11. a target dir that vanishes before listing returns a clean empty report instead of throwing (review finding)', () => {
    // Unlike tests 8/9/10, which simulate a race further into the sweep,
    // this simulates the race in the window between the CLI's own `--dir`
    // validation and this call: `dir` itself is gone by the time
    // `listCandidateEntries` calls `readdirSync` on it.
    readdirSyncMock.mockImplementation(() => {
      const error = new Error('ENOENT: no such file or directory, scandir \'/scratch\'');
      error.code = 'ENOENT';
      throw error;
    });

    const report = sweepStaleTempDirs({ dir: '/scratch', ageThresholdMs: 1000, now: Date.now() });

    // The sweep must not throw (implicit: reaching this line at all) — it
    // returns a clean, empty report instead.
    expect(report.removedCount).toBe(0);
    expect(report.reclaimedBytes).toBe(0);
    expect(report.errors).toEqual([expect.objectContaining({ path: '/scratch', kind: 'listing-failed' })]);
    expect(statSyncMock).not.toHaveBeenCalled();
    expect(rmSyncMock).not.toHaveBeenCalled();
  });
});

/** Dirent-shaped object matching `fs.Dirent`'s `isFile()` surface. */
function direntFile(name) {
  return { name, isDirectory: () => false, isFile: () => true };
}
