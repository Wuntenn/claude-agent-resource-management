// Phase 2 — pure age-filter decision logic for cdk.out/bundling-temp
// cleanup. RED by construction: `./lib/cleanup-age-filter.mjs` does not exist
// yet — this file only asserts the contract it must satisfy. Do not add a
// stub implementation to turn this green; that's the next (builder) step.
//
// Mirrors the house convention (see `computeDiskTrend` in
// `scripts/agent-resource-management/cli.mjs`, and `threshold.mjs` +
// `threshold.jest.spec.mjs` in this directory) of keeping a pure decision
// function — no I/O, no `Date.now()`, no `fs`/`glob` calls inside — entirely
// separate from the real filesystem walk/deletion adapter. `now` is always
// threaded through explicitly as an argument, never read internally.
//
// Contract under test:
//
//   selectStaleTempDirs({ entries, now, ageThresholdMs }) -> Array<{ path, mtimeMs }>
//
// - `entries` is an array of `{ path: string, mtimeMs: number }`.
// - Only entries whose basename glob-matches `cdk.out*` or
//   `bundling-temp-*` (prefix match: `cdk.out` followed by anything at all,
//   including nothing; `bundling-temp-` followed by anything) AND whose
//   `mtimeMs` is STRICTLY older than `now - ageThresholdMs` are selected.
// - A non-matching basename is never selected, regardless of age.
// - Boundary: `mtimeMs === now - ageThresholdMs` is treated as NOT stale yet
//   — this function requires strictly-older-than-threshold, not
//   at-or-older-than. (Design choice pinned by this test file; the
//   alternative — treating the boundary as stale — is equally defensible,
//   but "not yet stale" is the safer default for a destructive cleanup: it
//   never deletes a directory before it has been unambiguously past the
///   threshold for at least an instant.)
// - An empty `entries` list returns an empty array.
// - A non-positive (zero or negative) `ageThresholdMs` throws a `TypeError`
//   — 0/negative must never be silently treated as "select everything".
// - Duplicate `path`s in the input never crash the function and never
//   produce duplicate `path`s in the output (deduped).

import { selectStaleTempDirs } from './cleanup-age-filter.mjs';

const NOW = 1_700_000_000_000; // arbitrary fixed epoch ms
const AGE_THRESHOLD_MS = 24 * 60 * 60 * 1000; // 24h, an arbitrary realistic threshold

describe('selectStaleTempDirs — pure age-filter decision logic', () => {
  it('1. selects only stale, correctly-named entries (both cdk.out* and bundling-temp-* prefixes covered)', () => {
    const staleMtime = NOW - AGE_THRESHOLD_MS - 1;

    const entries = [
      { path: '/tmp/cdk.out', mtimeMs: staleMtime },
      { path: '/tmp/cdk.out-12345', mtimeMs: staleMtime },
      { path: '/tmp/bundling-temp-abcdef', mtimeMs: staleMtime },
      // A non-matching name at the exact same stale mtime — proves the name
      // filter and the age filter are both being applied, not just one.
      { path: '/tmp/unrelated-dir', mtimeMs: staleMtime },
    ];

    const selected = selectStaleTempDirs({ entries, now: NOW, ageThresholdMs: AGE_THRESHOLD_MS });
    const selectedPaths = selected.map((entry) => entry.path).sort();

    expect(selectedPaths).toEqual(['/tmp/bundling-temp-abcdef', '/tmp/cdk.out', '/tmp/cdk.out-12345'].sort());
  });

  it('2. excludes entries newer than the threshold, even if correctly named', () => {
    const freshMtime = NOW - 1; // far newer than the threshold requires

    const entries = [
      { path: '/tmp/cdk.out', mtimeMs: freshMtime },
      { path: '/tmp/bundling-temp-xyz', mtimeMs: freshMtime },
    ];

    const selected = selectStaleTempDirs({ entries, now: NOW, ageThresholdMs: AGE_THRESHOLD_MS });

    expect(selected).toEqual([]);
  });

  it('3. excludes non-matching names regardless of age, including a very old mtime', () => {
    const veryStaleMtime = NOW - AGE_THRESHOLD_MS * 100;

    const entries = [
      { path: '/tmp/node_modules', mtimeMs: veryStaleMtime },
      { path: '/tmp/not-cdk.out-either', mtimeMs: veryStaleMtime },
      { path: '/tmp/cdk-out-no-dot', mtimeMs: veryStaleMtime }, // missing the literal dot — not a match
      { path: '/tmp/prefixed-bundling-temp-abc', mtimeMs: veryStaleMtime }, // prefix match requires the name to START with bundling-temp-
    ];

    const selected = selectStaleTempDirs({ entries, now: NOW, ageThresholdMs: AGE_THRESHOLD_MS });

    expect(selected).toEqual([]);
  });

  it('3b. excludes near-miss names that merely start with the literal "cdk.out" characters without a separator boundary (review finding 3)', () => {
    const veryStaleMtime = NOW - AGE_THRESHOLD_MS * 100;

    const entries = [
      // Real, unrelated directory names that happen to begin with the same
      // characters as `cdk.out` but are not `cdk.out` itself nor
      // `cdk.out-<suffix>`/`cdk.out.<suffix>` — must never be swept.
      { path: '/tmp/cdk.outline-backup', mtimeMs: veryStaleMtime },
      { path: '/tmp/cdk.output-notes', mtimeMs: veryStaleMtime },
    ];

    const selected = selectStaleTempDirs({ entries, now: NOW, ageThresholdMs: AGE_THRESHOLD_MS });

    expect(selected).toEqual([]);
  });

  it('3c. still selects real cdk.out variants using "-" and "." separators', () => {
    const staleMtime = NOW - AGE_THRESHOLD_MS - 1;

    const entries = [
      { path: '/tmp/cdk.out', mtimeMs: staleMtime },
      { path: '/tmp/cdk.out-12345', mtimeMs: staleMtime },
      { path: '/tmp/cdk.out.bak', mtimeMs: staleMtime },
    ];

    const selected = selectStaleTempDirs({ entries, now: NOW, ageThresholdMs: AGE_THRESHOLD_MS });
    const selectedPaths = selected.map((entry) => entry.path).sort();

    expect(selectedPaths).toEqual(['/tmp/cdk.out', '/tmp/cdk.out-12345', '/tmp/cdk.out.bak'].sort());
  });

  it('4. boundary case: mtimeMs exactly at now-ageThresholdMs is excluded (not yet stale)', () => {
    const boundaryMtime = NOW - AGE_THRESHOLD_MS;

    const entries = [
      { path: '/tmp/cdk.out', mtimeMs: boundaryMtime },
      { path: '/tmp/bundling-temp-boundary', mtimeMs: boundaryMtime },
    ];

    const selected = selectStaleTempDirs({ entries, now: NOW, ageThresholdMs: AGE_THRESHOLD_MS });

    // Exactly at the threshold is NOT strictly older than it — must not be
    // selected. (See file header: this is a deliberate design choice.)
    expect(selected).toEqual([]);

    // One millisecond older, however, must cross the strict boundary.
    const justOverEntries = [{ path: '/tmp/cdk.out', mtimeMs: boundaryMtime - 1 }];
    const justOverSelected = selectStaleTempDirs({
      entries: justOverEntries,
      now: NOW,
      ageThresholdMs: AGE_THRESHOLD_MS,
    });
    expect(justOverSelected.map((entry) => entry.path)).toEqual(['/tmp/cdk.out']);
  });

  it('5. empty input list returns empty selection', () => {
    const selected = selectStaleTempDirs({ entries: [], now: NOW, ageThresholdMs: AGE_THRESHOLD_MS });

    expect(selected).toEqual([]);
  });

  it('6a. throws a TypeError on a non-positive ageThresholdMs (zero and negative), never silently selecting everything', () => {
    const entries = [{ path: '/tmp/cdk.out', mtimeMs: NOW - AGE_THRESHOLD_MS * 100 }];

    expect(() => selectStaleTempDirs({ entries, now: NOW, ageThresholdMs: 0 })).toThrow(TypeError);
    expect(() => selectStaleTempDirs({ entries, now: NOW, ageThresholdMs: -1000 })).toThrow(TypeError);
  });

  it('6b. duplicate input paths do not produce duplicate output entries and do not crash', () => {
    const staleMtime = NOW - AGE_THRESHOLD_MS - 1;

    const entries = [
      { path: '/tmp/cdk.out', mtimeMs: staleMtime },
      { path: '/tmp/cdk.out', mtimeMs: staleMtime },
      { path: '/tmp/cdk.out', mtimeMs: staleMtime },
    ];

    expect(() => selectStaleTempDirs({ entries, now: NOW, ageThresholdMs: AGE_THRESHOLD_MS })).not.toThrow();

    const selected = selectStaleTempDirs({ entries, now: NOW, ageThresholdMs: AGE_THRESHOLD_MS });
    const selectedPaths = selected.map((entry) => entry.path);

    expect(selectedPaths).toEqual(['/tmp/cdk.out']);
    expect(new Set(selectedPaths).size).toBe(selectedPaths.length);
  });
});
