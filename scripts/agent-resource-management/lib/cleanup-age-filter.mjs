// Phase 2 — pure age-filter decision logic for cdk.out/bundling-temp
// cleanup. No I/O, no `fs`/`glob` calls, no `Date.now()` — `now` is always
// threaded through explicitly as an argument, exactly the house convention
// used by `computeDiskTrend` (`cli.mjs`) and `threshold.mjs`'s classify
// functions in this directory. The real filesystem walk/deletion adapter is
// a separate concern and lives elsewhere.

// Name filters this function selects on, each requiring a separator
// boundary after the literal prefix rather than a raw `startsWith` (review
// finding 3,): `cdk.out` matches itself exactly, or anything it
// prefixes with a `-` or `.` separator (e.g. `cdk.out-12345`, `cdk.out.bak`)
// — but NOT an unrelated real directory that merely starts with the
// characters `cdk.out`, such as `cdk.outline-backup` or `cdk.output-notes`.
// `bundling-temp-` already ends in its own separator (`-`), so a raw
// `startsWith` on it carries no such collision risk and needs no additional
// boundary logic; it matches only names that literally START with that
// prefix — a name that merely contains it as a substring (e.g.
// `prefixed-bundling-temp-abc`) is never selected.
const STALE_NAME_EXACT = ['cdk.out'];
const STALE_NAME_BOUNDARY_PREFIXES = ['cdk.out-', 'cdk.out.'];
const STALE_NAME_PREFIXES = ['bundling-temp-'];

/**
 * @param {string} path
 * @returns {string}
 */
function basename(path) {
  const segments = path.split('/');
  return segments[segments.length - 1];
}

/**
 * Does this bare directory/file NAME match one of the stale temp patterns?
 *
 * EXPORTED (pre-PR review, High #5) so the async listing stage can apply
 * the cheap name test BEFORE spending a `stat`/`lstat` syscall on an entry.
 * It previously stat-ed every dirent in the sweep root and only then handed
 * the lot to `selectStaleTempDirs` — so a root full of unrelated entries could
 * burn the whole budget before the walk ever reached a real candidate, on
 * every beat, since `readdir` order is stable. The two callers share this one
 * predicate rather than keeping a second copy in sync: a filter that drifts
 * from the selector is a silent miss on a destructive primitive.
 *
 * @param {string} name
 * @returns {boolean}
 */
export function matchesStaleNamePattern(name) {
  if (STALE_NAME_EXACT.includes(name)) return true;
  if (STALE_NAME_BOUNDARY_PREFIXES.some((prefix) => name.startsWith(prefix))) return true;
  return STALE_NAME_PREFIXES.some((prefix) => name.startsWith(prefix));
}

/**
 * @typedef {{ path: string, mtimeMs: number }} TempDirEntry
 */

/**
 * Pure decision function: given a list of candidate temp-dir entries, the
 * current time, and an age threshold, selects only those entries that are
 * BOTH name-matched (leading-prefix glob of `cdk.out*` or `bundling-temp-*`)
 * AND strictly older than `now - ageThresholdMs`. Never touches the
 * filesystem — the caller is responsible for turning the result into actual
 * deletions.
 *
 * Boundary: `mtimeMs === now - ageThresholdMs` is NOT stale — this requires
 * strictly-older-than-threshold, not at-or-older-than. That is the safer
 * default for a destructive cleanup: a directory is never deleted before it
 * has been unambiguously past the threshold for at least an instant.
 *
 * Duplicate `path`s in `entries` are deduped in the output (first occurrence
 * wins) rather than crashing or producing duplicate output entries.
 *
 * @param {{ entries: TempDirEntry[], now: number, ageThresholdMs: number }} args
 * @returns {TempDirEntry[]}
 */
export function selectStaleTempDirs({ entries, now, ageThresholdMs }) {
  if (typeof ageThresholdMs !== 'number' || !Number.isFinite(ageThresholdMs) || ageThresholdMs <= 0) {
    throw new TypeError('ageThresholdMs must be a positive, finite number');
  }

  const staleBeforeMs = now - ageThresholdMs;
  const seenPaths = new Set();
  const selected = [];

  for (const entry of entries) {
    if (seenPaths.has(entry.path)) continue;
    seenPaths.add(entry.path);

    if (!matchesStaleNamePattern(basename(entry.path))) continue;
    if (!(entry.mtimeMs < staleBeforeMs)) continue;

    selected.push(entry);
  }

  return selected;
}
