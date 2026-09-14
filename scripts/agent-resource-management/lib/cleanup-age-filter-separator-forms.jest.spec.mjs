// Phase 2 — SEPARATOR FORMS at the `selectStaleTempDirs` boundary, and
// the async sweep's obligation to normalise before delegating.
//
// ---------------------------------------------------------------------------
// THE PROPERTY THIS FILE PROTECTS
// ---------------------------------------------------------------------------
//
// `cleanup-age-filter.mjs`'s private `basename` (lines 27-30) is:
//
//     function basename(path) {
//       const segments = path.split('/');
//       return segments[segments.length - 1];
//     }
//
// Two things follow, and only the second is currently dangerous:
//
//   * It splits on a HARD-CODED `'/'`, not `path.sep`. On POSIX those are the
//     same character, so nothing is wrong today — but the equivalence is an
//     unstated PREMISE of the module, and a premise nobody has written down is
//     one nobody will check. The first cell below asserts it aloud, so the day
//     the premise stops holding, a test fails instead of a directory
//     disappearing.
//
//   * A TRAILING SEPARATOR makes it return the EMPTY STRING.
//     `'/tmp/cdk.out-1/'.split('/')` ends in `''`, no name pattern matches `''`,
//     and the entry is silently dropped from the selection.
//
// THE SECOND IS THE FAILURE MODE WORTH A SPEC, AND IT IS NOT THE OBVIOUS ONE.
// A dropped candidate is not a deletion — nothing is destroyed. The harm is
// that the sweep then reports `removedCount: 0, errors: []`: a NO-OP REPORTED
// AS A CLEAN SUCCESS. The AMBER disk-response loop reads that report to decide
// whether pressure was relieved. A clean zero tells it "there was nothing to
// reclaim", so it stops asking — while the backlog it was summoned to drain
// sits there in full. A silent miss on a cleanup primitive is a control-loop
// failure, not a cosmetic one, and it is invisible precisely because every
// field of the report looks healthy.
//
// WHY A NAIVE IMPLEMENTATION PASSES A WEAKER TEST. Every existing spec feeds
// `selectStaleTempDirs` paths built by `path.join`, which never emits a
// trailing separator — so the whole class is untested by construction. On the
// async path the root is now operator-influenced (it derives from the TMPDIR
// variable, which is routinely written as `/tmp/` with the trailing slash the
// shell's tab-completion adds), so a `dir` that ends in a separator is an
// ordinary input, not an exotic one. The cells below therefore pin BOTH ends:
// the filter's behaviour as it actually is, and the async sweep's obligation to
// hand it normalised paths so the behaviour never bites.
//
// ---------------------------------------------------------------------------
// A FINDING, DELIBERATELY NOT FIXED HERE
// ---------------------------------------------------------------------------
//
// The `basename` weakness is LATENT, not live: the only production producer of
// entry paths is `listCandidateEntries`, which builds them with `path.join`,
// and `join` never yields a trailing separator. This file therefore PINS the
// current behaviour rather than asserting a fix, and does not touch
// `cleanup-age-filter.mjs` (contract property 8 — that module stays unmodified
// and is reused as-is). If a future change ever lets an un-normalised path
// reach the filter, the first cell below is what documents what will happen.
//
// ---------------------------------------------------------------------------
// RED BY CONSTRUCTION
// ---------------------------------------------------------------------------
//
// The `selectStaleTempDirs` cells in the first block are GREEN today — they
// characterise an existing, unmodified module, which is the point: they are the
// PREMISE ("an un-normalised path IS silently dropped") that gives the real-fs
// block below its meaning. The real-fs cells are RED, because `./cleanup.mjs`
// exports no `sweepStaleTempDirsAsync`.
//
// `./cleanup.mjs` is imported as a NAMESPACE rather than by name so that red is
// per-cell (`cleanup.sweepStaleTempDirsAsync is not a function`) rather than a
// single ESM link-time `SyntaxError` that would take the green premise cells
// down with it — a premise that never ran is not a premise.
//
// SAFETY: the real-fs block creates its own `fs.mkdtempSync` scratch root,
// nominates it explicitly through `allowedPrefixes`, and tears it down in
// `afterEach`. The process's real `$TMPDIR` is never swept — the sentinel cell
// at the end pins that, and it is the cell most at risk here, since `'/tmp/'`
// with a trailing separator is exactly the kind of string this file is about.

import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  realpathSync,
  rmSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join, sep } from 'node:path';

import { selectStaleTempDirs } from './cleanup-age-filter.mjs';
import * as cleanup from './cleanup.mjs';

const HOUR_MS = 60 * 60 * 1000;
const NOW = 1_700_000_000_000;
const AGE_THRESHOLD_MS = 2 * HOUR_MS;
const STALE_MTIME = NOW - AGE_THRESHOLD_MS - 1;

describe('the filter`s separator premise, stated aloud rather than assumed', () => {
  test('PREMISE: on this platform `path.sep` IS `/`, which is why the hard-coded split is harmless today', () => {
    // `cleanup-age-filter.mjs:28` splits on a literal `'/'`. That is correct
    // only while this holds. Asserted here so the premise is checked rather
    // than believed — on a platform where it fails, every name match in the
    // module silently stops working, and a cleanup sweep that matches nothing
    // reports a clean success (see this file's header).
    expect(sep).toBe('/');
  });

  test('a plain joined path is selected normally — the control', () => {
    const selected = selectStaleTempDirs({
      entries: [{ path: join('/scratch', 'cdk.out-1'), mtimeMs: STALE_MTIME }],
      now: NOW,
      ageThresholdMs: AGE_THRESHOLD_MS,
    });

    expect(selected.map((entry) => entry.path)).toEqual(['/scratch/cdk.out-1']);
  });

  test('a TRAILING-SEPARATOR path is SILENTLY DROPPED — the characterised weakness', () => {
    // Genuinely stale, correctly named, and not selected. No error, no warning:
    // the caller's report would read `removedCount: 0, errors: []`.
    const selected = selectStaleTempDirs({
      entries: [{ path: '/scratch/cdk.out-1/', mtimeMs: STALE_MTIME }],
      now: NOW,
      ageThresholdMs: AGE_THRESHOLD_MS,
    });

    expect(selected).toEqual([]);
  });

  test('a DOUBLED trailing separator is dropped the same way, and a doubled INTERIOR one is not', () => {
    const selected = selectStaleTempDirs({
      entries: [
        { path: '/scratch/cdk.out-doubled//', mtimeMs: STALE_MTIME },
        // An interior double leaves the final segment intact, so this one IS
        // selected — the weakness is specifically about the LAST segment.
        { path: '/scratch//cdk.out-interior', mtimeMs: STALE_MTIME },
      ],
      now: NOW,
      ageThresholdMs: AGE_THRESHOLD_MS,
    });

    expect(selected.map((entry) => entry.path)).toEqual(['/scratch//cdk.out-interior']);
  });

  test('the drop is total, not partial: a mixed batch loses exactly the trailing-separator entries', () => {
    // The shape that makes the miss hardest to notice in the wild — the sweep
    // does SOME work, so the report is not even suspiciously empty.
    const selected = selectStaleTempDirs({
      entries: [
        { path: '/scratch/cdk.out-a', mtimeMs: STALE_MTIME },
        { path: '/scratch/cdk.out-b/', mtimeMs: STALE_MTIME },
        { path: '/scratch/bundling-temp-c', mtimeMs: STALE_MTIME },
        { path: '/scratch/bundling-temp-d/', mtimeMs: STALE_MTIME },
      ],
      now: NOW,
      ageThresholdMs: AGE_THRESHOLD_MS,
    });

    expect(selected.map((entry) => entry.path)).toEqual([
      '/scratch/cdk.out-a',
      '/scratch/bundling-temp-c',
    ]);
  });
});

describe('the async sweep normalises the root BEFORE delegating, so the weakness never bites', () => {
  let scratchRoot;
  let root;

  beforeEach(() => {
    scratchRoot = mkdtempSync(join(tmpdir(), 'arm-separator-'));
    root = realpathSync(scratchRoot);
  });

  afterEach(() => {
    rmSync(scratchRoot, { recursive: true, force: true });
  });

  function seedStaleCandidate(name, bytes = 1024) {
    const candidate = join(root, name);
    mkdirSync(candidate, { recursive: true });
    const payload = join(candidate, 'payload.bin');
    writeFileSync(payload, Buffer.alloc(bytes, 1));
    const seconds = (Date.now() - (cleanup.AUTO_TRIGGER_AGE_HOURS + 4) * HOUR_MS) / 1000;
    utimesSync(payload, seconds, seconds);
    utimesSync(candidate, seconds, seconds);
    return candidate;
  }

  function sweep(dir) {
    return cleanup.sweepStaleTempDirsAsync({
      dir,
      ageThresholdMs: cleanup.AUTO_TRIGGER_AGE_HOURS * HOUR_MS,
      now: Date.now(),
      budgetMs: 5_000,
      allowedPrefixes: [root],
    });
  }

  test.each([
    ['trailing separator', () => `${root}${sep}`],
    ['doubled trailing separator', () => `${root}${sep}${sep}`],
    ['dot segment', () => join(root, '.')],
  ])('a `dir` written with a %s still removes the stale candidate', async (_label, dirOf) => {
    const candidate = seedStaleCandidate('cdk.out-trailing');

    const result = await sweep(dirOf());

    // If the sweep passed its raw `dir` through to child-path construction, the
    // entry paths would carry a doubled separator, `basename` would return `''`,
    // and this would be a clean-looking zero.
    expect(result.removedCount).toBe(1);
    expect(result.reclaimedBytes).toBe(1024);
    expect(result.errors).toEqual([]);
    expect(existsSync(candidate)).toBe(false);
  });

  test('the report`s own paths are normalised — no doubled or trailing separator survives into them', async () => {
    // Read off the report rather than off the disk: even when a doubled
    // separator happens not to break the match, emitting one into the verdict
    // JSON means downstream string comparison against a canonical path fails.
    const candidate = seedStaleCandidate('cdk.out-normalised');
    // Made undeletable so its path is NAMED in the report.
    chmodSync(candidate, 0o555);

    try {
      const result = await sweep(`${root}${sep}${sep}`);

      expect(result.errors).toHaveLength(1);
      expect(result.errors[0].path).toBe(candidate);
      expect(result.errors[0].path).not.toMatch(/\/\//);
      expect(result.errors[0].path.endsWith(sep)).toBe(false);
    } finally {
      chmodSync(candidate, 0o755);
    }
  });

  test('SENTINEL: a trailing-separator form of the REAL host temp root is still refused', async () => {
    // The cell most at risk in this file. `'/tmp/'` is exactly the written form
    // a shell hands you, and a normalisation step applied in the wrong ORDER —
    // after the confinement check rather than before it — is how a refused root
    // becomes an accepted one.
    const candidate = seedStaleCandidate('cdk.out-must-survive');

    const result = await sweep(`${tmpdir()}${sep}`);

    expect(result.removedCount).toBe(0);
    expect(result.errors[0].kind).toBe('confinement-refused');
    expect(existsSync(candidate)).toBe(true);
  });
});
