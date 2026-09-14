// pre-PR review (High #3) — `reclaimedBytes` MUST NOT COUNT BYTES THE
// DISK KEEPS.
//
// ---------------------------------------------------------------------------
// THE PROPERTY, AND WHY A REAL FILESYSTEM IS THE ONLY HONEST WITNESS
// ---------------------------------------------------------------------------
//
// `reclaimedBytes` is the figure the disk axis's AMBER response loop reads to
// decide whether pressure was actually relieved. Both size walks document the
// invariant it rests on in as many words: it is never a figure the disk did not
// see. The nested-symlink case broke exactly that.
//
// `readdir(..., { withFileTypes: true })` does NOT follow links, so a symlink's
// dirent answers `isDirectory() === false` and both walks fell through to
// `stat(entryPath)` — which DOES follow, adding the TARGET's size. But
// `rm(candidate, { recursive: true })` removes the LINK and never the target.
// A candidate holding one link to a 5 MiB file therefore reported 5 MiB
// reclaimed while freeing nothing at all, and the loop read a filling host as
// relieved.
//
// The top-level rule was already right — a candidate that IS a symlink is
// sized 0 — so this is that same rule, applied one level down where it
// was missing.
//
// A MOCKED `fs` cannot witness this. The whole defect is that the KERNEL
// follows a link the dirent said was not a directory; a mock is free to report
// whatever size the test already believes. So every byte below is a byte a real
// `stat` really returned.
//
// SAFETY: every path here lives under this file's own `mkdtempSync` scratch
// roots, nominated explicitly through `allowedPrefixes` and torn down in
// `afterEach`. The link target deliberately sits OUTSIDE the swept root, which
// is both the realistic shape (a bundler's cache) and the one that makes
// "still there afterwards" a meaningful assertion.

import { mkdirSync, mkdtempSync, realpathSync, rmSync, statSync, symlinkSync, utimesSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { AUTO_TRIGGER_AGE_HOURS, directorySizeBytes, sweepStaleTempDirsAsync } from './cleanup.mjs';

const HOUR_MS = 60 * 60 * 1000;
const STALE_SECONDS = (Date.now() - (AUTO_TRIGGER_AGE_HOURS + 4) * HOUR_MS) / 1000;

const INNER_FILE_BYTES = 1_024;
const LINK_TARGET_BYTES = 5 * 1_024 * 1_024;

let scratchRoot;
let root;
let outsideRoot;

beforeEach(() => {
  scratchRoot = mkdtempSync(join(tmpdir(), 'arm-nested-symlink-'));
  root = realpathSync(scratchRoot);
  outsideRoot = realpathSync(mkdtempSync(join(tmpdir(), 'arm-nested-symlink-target-')));
});

afterEach(() => {
  rmSync(scratchRoot, { recursive: true, force: true });
  rmSync(outsideRoot, { recursive: true, force: true });
});

/** Backdated so neither the staleness filter nor the liveness probe objects. */
function backdate(path) {
  utimesSync(path, STALE_SECONDS, STALE_SECONDS);
}

/**
 * A stale, correctly-named candidate holding one small REAL file and one
 * symlink to a large file outside the swept root.
 *
 * Order matters: contents first, `utimes` last, because creating an entry
 * bumps its parent's mtime.
 */
function seedCandidateWithNestedLink(name) {
  const target = join(outsideRoot, 'big-payload.bin');
  writeFileSync(target, Buffer.alloc(LINK_TARGET_BYTES, 7));
  backdate(target);

  const candidate = join(root, name);
  mkdirSync(candidate, { recursive: true });

  const realFile = join(candidate, 'real.bin');
  writeFileSync(realFile, Buffer.alloc(INNER_FILE_BYTES, 1));
  backdate(realFile);

  symlinkSync(target, join(candidate, 'linked.bin'));

  backdate(candidate);
  return { candidate, target };
}

describe('— the PREMISE these cells rest on is real, not assumed', () => {
  test('the link target really is large, and a following stat really would report its size', () => {
    const { candidate, target } = seedCandidateWithNestedLink('cdk.out-linky');

    expect(statSync(target).size).toBe(LINK_TARGET_BYTES);
    // The following stat, through the link, from inside the candidate — this
    // is the value the old walks added, and it is what makes the cells below
    // non-vacuous rather than "0 happens to equal 0".
    expect(statSync(join(candidate, 'linked.bin')).size).toBe(LINK_TARGET_BYTES);
  });
});

describe('directorySizeBytes (sync) does not count a nested symlink', () => {
  test('a tree holding one link to a 5 MiB file sizes only its REAL bytes', () => {
    const { candidate } = seedCandidateWithNestedLink('cdk.out-linky');

    expect(directorySizeBytes(candidate)).toBe(INNER_FILE_BYTES);
  });
});

describe('the async sweep reports only bytes the disk actually gave back', () => {
  test('a candidate holding a nested symlink reclaims the real file, never the link target', async () => {
    const { candidate, target } = seedCandidateWithNestedLink('cdk.out-linky');

    const report = await sweepStaleTempDirsAsync({
      dir: root,
      ageThresholdMs: AUTO_TRIGGER_AGE_HOURS * HOUR_MS,
      now: Date.now(),
      budgetMs: 5_000,
      allowedPrefixes: [root],
    });

    expect(report.removedCount).toBe(1);
    // THE ASSERTION. `INNER_FILE_BYTES`, not `INNER_FILE_BYTES +
    // LINK_TARGET_BYTES` — the old walk reported the latter.
    expect(report.reclaimedBytes).toBe(INNER_FILE_BYTES);
    expect(report.errors).toEqual([]);

    // The candidate (link included) is gone...
    expect(() => statSync(candidate)).toThrow();
    // ...and the 5 MiB the old figure claimed credit for is still on the disk,
    // which is the whole reason claiming it was wrong.
    expect(statSync(target).size).toBe(LINK_TARGET_BYTES);
  });
});
