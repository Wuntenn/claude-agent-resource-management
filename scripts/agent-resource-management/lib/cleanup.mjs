// Phase 3 — real fs adapter + CLI entrypoint for the cdk.out*/
// bundling-temp-* cleanup sweep. Composes the Phase 2 pure decision function
// (`selectStaleTempDirs` from `./cleanup-age-filter.mjs`, unmodified) with a
// thin, directly-testable fs adapter.
//
// CLI: `node cleanup.mjs [--age-hours=N] [--dir=<path>]` — no shebang,
// matching `diagnostics/collect.mjs`'s direct-invocation convention. Default
// `--age-hours` is 2; default `--dir` is `process.env.TMPDIR` or `/tmp`.
// Runs only when invoked directly (import.meta.url guard), so importing this
// module from a test never triggers argv parsing or any fs mutation.
//
// `--dir` is confined to the system temp root (`TMPDIR`/`os.tmpdir()`,
// symlink-resolved) — see `isConfinedToTmpRoot` below. This sweep is
// destructive+recursive, and `cdk.out` is this repo's own real, gitignored
// CDK synth output directory (`infra/.gitignore`), not a synthetic
// test-only name; a mistyped/misconfigured `--dir` must never be able to
// reach it.
//
// Namespace import of `node:fs` (not named imports), deliberately: this
// module is imported under `jest.unstable_mockModule('node:fs', ...)` by
// `cleanup-adapter.jest.spec.mjs`, whose mock factory provides only
// `readdirSync`/`statSync`/`rmSync` — a named import of `existsSync` (used
// only by the CLI-validation path below, never exercised by that spec) would
// throw a link-time SyntaxError even though that spec never calls into the
// CLI branch. See `diagnostics/collect.mjs`'s own header comment for the same
// rationale. `node:fs/promises` is imported the same way and for the same
// reason: `cleanup-async-*.jest.spec.mjs` replace that module wholesale with a
// mock factory, and a named import of a member one of those factories happens
// not to provide would fail at link time in a spec that never calls it.
//
// ---------------------------------------------------------------------------
// WHY THIS FILE CARRIES TWO SWEEPS — NOT DRIFT
// ---------------------------------------------------------------------------
//
// `sweepStaleTempDirs` (synchronous) and `sweepStaleTempDirsAsync` (budgeted)
// coexist deliberately. They serve two callers with different contracts, and
// neither contract can be met by the other's implementation:
//
//   * SYNC = the CLI/cron entrypoint below. A human typed `node cleanup.mjs`,
//     is watching stdout, and wants the whole backlog drained in one go. There
//     is no deadline to keep and nothing else sharing the process, so blocking
//     the event loop is free and the simpler code is the better code.
//
//   * ASYNC = the auto-trigger's beat path. It runs INSIDE a PreToolUse
//     hook beat that every agent spawn on this host waits on, and whose entire
//     stdout the hook `JSON.parse`s (failing closed on anything it cannot
//     parse). That caller needs a hard wall-clock ceiling, a bounded report, a
//     candidate cap and a liveness margin — and a deadline is only real if
//     every step can be ABANDONED, which a synchronous fs call structurally
//     cannot be (`readdirSync` on a quiet NFS mount blocks the one thread, so
//     no timer fires and no race resolves).
//
// The DECISION logic is NOT duplicated: both compose the same unmodified
// `selectStaleTempDirs` from `./cleanup-age-filter.mjs`, so the age/name rule —
// which is security-relevant, see `./cleanup-temp-root-anchor.mjs`'s header on
// the allowed-but-CONTAINING root — has exactly one definition. What differs is
// the IO strategy and the bounds, which is the whole point of the split.
// `./cleanup-async-no-sync-fs.jest.spec.mjs` pins the separation mechanically in
// both directions: the async body may not reach the sync helpers, and the sync
// exports must remain present and unchanged in shape.
import * as fs from 'node:fs';
import * as fsPromises from 'node:fs/promises';
import { join, resolve, sep } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { matchesStaleNamePattern, selectStaleTempDirs } from './cleanup-age-filter.mjs';

export const CLEANUP_SCRIPT_PATH = fileURLToPath(import.meta.url);

/**
 * Age threshold for the ATTENDED CLI path (`--age-hours`'s default).
 *
 * Exported rather than module-private so
 * `AUTO_TRIGGER_AGE_HOURS >= DEFAULT_AGE_HOURS` can be asserted against the
 * real constant instead of a duplicated literal — a duplicated literal is a
 * spec that keeps passing while the value it mirrors moves, which is precisely
 * the drift that relation exists to prevent. The promotion changes nothing
 * about the CLI's behaviour.
 */
export const DEFAULT_AGE_HOURS = 2;
const HOUR_MS = 60 * 60 * 1000;

/**
 * Real fs adapter: lists the immediate children of `dirPath` — directories,
 * plain files, AND symlinks — with their `mtimeMs`. No name/age
 * filtering of its own — that is `selectStaleTempDirs`'s job, composed on
 * top by `sweepStaleTempDirs` below.
 *
 * Symlink handling: a symlink candidate is stat'd with
 * `fs.lstatSync`, never `fs.statSync` — `lstatSync` reports the LINK's own
 * `mtimeMs`, not the target's. Using `statSync` here would follow the link
 * and silently judge staleness by the TARGET's age, which is wrong: the
 * link itself is what gets removed (`fs.rmSync` on a symlink path never
 * follows it into the target), so the link's own mtime is what must gate
 * that removal. Each returned symlink entry carries `isSymlink: true` so
 * `sweepStaleTempDirs` can size it as its own (never the target's recursive
 * content — deleting the link never frees the target's space).
 *
 * A plain-file candidate carries `isFile: true` so `sweepStaleTempDirs` can
 * size it via a plain `fs.statSync(path).size` rather than the
 * directory-only `directorySizeBytes` walk.
 *
 * The `fs.lstatSync`/`fs.statSync` call per entry is individually guarded
 * (review finding 1,): an entry that vanishes between the
 * `readdirSync` listing above and this stat call (a concurrent cleanup run,
 * a live process still touching a borderline-stale dir, or plain OS timing
 * against `$TMPDIR` — exactly the race this file's own header comment
 * already anticipates) is simply omitted from the returned list rather than
 * throwing. Mirrors `directorySizeBytes`'s established guard pattern for the
 * same race.
 *
 * @param {string} dirPath
 * @returns {Array<{ path: string, mtimeMs: number, isSymlink?: boolean, isFile?: boolean }>}
 */
export function listCandidateEntries(dirPath) {
  const dirents = fs.readdirSync(dirPath, { withFileTypes: true });
  const entries = [];

  for (const dirent of dirents) {
    const isSymlink = typeof dirent.isSymbolicLink === 'function' && dirent.isSymbolicLink();
    const isFile = !isSymlink && typeof dirent.isFile === 'function' && dirent.isFile();
    if (!dirent.isDirectory() && !isFile && !isSymlink) continue;

    const entryPath = join(dirPath, dirent.name);
    let stats;
    try {
      stats = isSymlink ? fs.lstatSync(entryPath) : fs.statSync(entryPath);
    } catch {
      // Vanished between readdir and stat — omitted, not a throw.
      continue;
    }
    entries.push({ path: entryPath, mtimeMs: stats.mtimeMs, isSymlink, isFile });
  }

  return entries;
}

/**
 * Real, recursive byte total for a directory tree (files only). Computed
 * BEFORE removal by every caller — a size read against an already-removed
 * path is meaningless.
 *
 * Every fs call in here is individually guarded: a file/dir that vanishes
 * between the initial `readdirSync` listing and this walk (a concurrent
 * cleanup run, a live process still touching a borderline-stale dir, or
 * plain OS timing) is treated as contributing 0 bytes rather than throwing
 * — a size-read failure must never escape this function and crash the
 * caller's sweep loop.
 *
 * @param {string} dirPath
 * @returns {number}
 */
export function directorySizeBytes(dirPath) {
  let dirents;
  try {
    dirents = fs.readdirSync(dirPath, { withFileTypes: true });
  } catch {
    return 0;
  }

  let total = 0;
  for (const dirent of dirents) {
    // A SYMLINK NESTED INSIDE THE TREE CONTRIBUTES 0 (pre-PR review,
    // High #3) — the same rule already applied to a symlink that IS the
    // candidate, applied one level down where it was missing.
    //
    // `readdir(..., { withFileTypes: true })` does not follow links, so
    // `isDirectory()` is false for one and it fell through to `statSync`,
    // which DOES follow — adding the TARGET's size. But `rm(candidate,
    // { recursive: true })` removes the LINK, never the target, so those bytes
    // are still on disk afterwards. A candidate holding one link to a 5 MB
    // file reported 5 MB reclaimed while freeing nothing. That breaks the
    // invariant the AMBER response loop reads this figure through —
    // `reclaimedBytes` is never a figure the disk did not see — and it breaks
    // it in the direction that makes a filling host look relieved.
    if (typeof dirent.isSymbolicLink === 'function' && dirent.isSymbolicLink()) continue;

    const entryPath = join(dirPath, dirent.name);
    if (dirent.isDirectory()) {
      total += directorySizeBytes(entryPath);
    } else {
      try {
        total += fs.statSync(entryPath).size;
      } catch {
        // Vanished between readdir and stat — contributes 0, not a throw.
      }
    }
  }
  return total;
}

/**
 * Composes `listCandidateEntries` + the REAL `selectStaleTempDirs` (Phase 2,
 * unmodified) + `directorySizeBytes` (measured before removal) +
 * `fs.rmSync(path, { recursive: true, force: true })` per selected entry.
 *
 * `errors` CONFLATES several different failure kinds, distinguished by the
 * `kind` field on each entry (review finding 2, — do not read this
 * array as "fs.rmSync failures only"):
 *
 *   - `kind: 'removal-failed'` — a thrown error from `fs.rmSync` for this
 *     candidate. The candidate was NOT removed; it does not count toward
 *     `removedCount`/`reclaimedBytes`.
 *   - `kind: 'size-read-failed'` — `directorySizeBytes` threw for this
 *     candidate (e.g. it vanished mid-sweep) BEFORE removal was attempted.
 *     This is informational only: removal is still attempted afterwards and,
 *     on success, the candidate DOES count toward `removedCount` (with 0
 *     contributed to `reclaimedBytes`, since its size could not be read). A
 *     caller must not treat the mere presence of an entry in `errors` as
 *     proof that candidate was left behind — check `kind` first.
 *   - `kind: 'listing-failed'` — `listCandidateEntries(dir)` threw before any
 *     candidate could even be discovered (the target `dir` itself vanished,
 *     was unmounted, or became permission-denied). No candidates exist to
 *     remove; the report is `{ removedCount: 0, reclaimedBytes: 0, errors: [...] }`.
 *
 * In all cases the sweep CONTINUES to the remaining candidates (or, for
 * `listing-failed`, returns a clean empty report) — `removedCount`/
 * `reclaimedBytes` reflect only entries that were actually removed.
 *
 * `listCandidateEntries(dir)` itself is guarded here too (review finding,
 *): the CLI validates `--dir` up front (`existsSync`/`statSync` near
 * the entrypoint), but a TOCTOU race in the window between that validation
 * and this call — `dir` removed, unmounted, or turned permission-denied —
 * would otherwise let the `readdirSync` inside `listCandidateEntries` throw
 * uncaught, crashing the whole process with a raw stack trace instead of a
 * clean report. On failure, a clean, empty report is returned instead, with
 * the failure recorded under `kind: 'listing-failed'`.
 *
 * @param {{ dir: string, ageThresholdMs: number, now: number }} args
 * @returns {{ removedCount: number, reclaimedBytes: number, errors: Array<{ path: string, message: string, kind: 'removal-failed' | 'size-read-failed' | 'listing-failed' }> }}
 */
export function sweepStaleTempDirs({ dir, ageThresholdMs, now }) {
  let entries;
  try {
    entries = listCandidateEntries(dir);
  } catch (error) {
    return { removedCount: 0, reclaimedBytes: 0, errors: [{ path: dir, message: error.message, kind: 'listing-failed' }] };
  }
  const selected = selectStaleTempDirs({ entries, now, ageThresholdMs });

  let removedCount = 0;
  let reclaimedBytes = 0;
  const errors = [];

  for (const entry of selected) {
    // Guarded independently of the `fs.rmSync` try/catch below: a size-read
    // failure for this candidate (e.g. it vanished mid-sweep) must not
    // escape the loop and discard the removedCount/reclaimedBytes/errors
    // already accumulated for prior candidates. Treated as "0 bytes
    // measured, but still attempt removal" — `directorySizeBytes` already
    // guards its own internal fs calls; this is defense-in-depth at the
    // call site itself.
    //
    // Branches on the candidate's own kind: a symlink candidate is
    // always sized as 0 — its own removal never recurses into (or frees)
    // the target's recursive content, so walking the target here would
    // misreport bytes that were never actually reclaimed. A plain-file
    // candidate is sized with a plain `fs.statSync(path).size`, not the
    // directory-only `directorySizeBytes` walk (which expects a directory
    // and would misbehave against a file path). Everything else falls back
    // to the existing recursive directory walk.
    let sizeBytes = 0;
    try {
      if (entry.isSymlink) {
        sizeBytes = 0;
      } else if (entry.isFile) {
        sizeBytes = fs.statSync(entry.path).size;
      } else {
        sizeBytes = directorySizeBytes(entry.path);
      }
    } catch (error) {
      errors.push({ path: entry.path, message: `size read failed: ${error.message}`, kind: 'size-read-failed' });
    }

    try {
      fs.rmSync(entry.path, { recursive: true, force: true });
      removedCount += 1;
      reclaimedBytes += sizeBytes;
    } catch (error) {
      errors.push({ path: entry.path, message: error.message, kind: 'removal-failed' });
    }
  }

  return { removedCount, reclaimedBytes, errors };
}

// ---------------------------------------------------------------------------
// Phase 2 — the BUDGETED, BOUNDED, LIVENESS-AWARE ASYNC SWEEP.
//
// Everything below this line serves `sweepStaleTempDirsAsync` and nothing else.
// See the "WHY THIS FILE CARRIES TWO SWEEPS" block in this file's header for
// why it does not replace, and is not replaced by, the synchronous sweep above.
// ---------------------------------------------------------------------------

/** Hard ceiling on the async sweep's own wall clock, per invocation. */
export const SWEEP_BUDGET_MS = 750;

/**
 * Per-beat candidate cap. A real backlog on this host runs to five figures;
 * draining it across many bounded beats is correct work, draining it in one
 * multi-gigabyte walk on a hot admission path is not.
 */
export const SWEEP_MAX_CANDIDATES = 24;

/**
 * Payload bound on the report's `errors` ring. A mass-EPERM `/private/tmp`
 * (shared between users on macOS; the watchdog's temp root under launchd's
 * minimal environment) produces thousands of identical failures on one beat,
 * and the PreToolUse hook's `spawnSync` sets no `maxBuffer` — Node's default
 * 1 MiB applies, and an overflow surfaces as `ENOBUFS` in `result.error`,
 * which that hook reports as "could not be started" and fails closed on. So an
 * unbounded report denies every agent spawn on the host while presenting as a
 * startup failure. This is the fix.
 */
export const SWEEP_MAX_ERRORS = 8;

/**
 * Age threshold for the UNATTENDED auto-trigger. Asserted to be `>=`
 * `DEFAULT_AGE_HOURS`: nobody typed anything to start this sweep, so it must
 * never be more aggressive than the command an operator types and watches.
 */
export const AUTO_TRIGGER_AGE_HOURS = 6;

/**
 * How long the auto-trigger stands down after a sweep that ran to completion
 * and found NOTHING worth doing (Phase 3 review, Medium).
 *
 * THE CASE THIS EXISTS FOR is a steady state, not an edge case: a host sitting
 * stably just below the GREEN free-space floor, with a `$TMPDIR` that is
 * already clean (nothing left, or only entries younger than
 * `AUTO_TRIGGER_AGE_HOURS`, or all live-writer-skipped), satisfies the trigger
 * on EVERY beat, forever — paying the full `SWEEP_BUDGET_MS` listing-and-
 * probing cost each time to reclaim zero bytes. The trigger has no memory of
 * its own outcome; this constant is that memory's horizon.
 *
 * WHY 30 MINUTES. It is set against `AUTO_TRIGGER_AGE_HOURS` (6h), not against
 * `SWEEP_BUDGET_MS` (750ms): the question a cooldown answers is "how long
 * until the world could plausibly have changed", and the only thing that can
 * make an already-clean root sweepable again is an existing entry crossing the
 * six-hour staleness horizon. 30 minutes is a twelfth of that — noise against
 * a candidate that has had to sit untouched for six hours to qualify, while
 * cutting the futile duty cycle from 750ms per beat (beats can be seconds
 * apart) to 750ms per half hour.
 *
 * Deliberately NOT longer: a cooldown must never be the reason a genuinely
 * filling disk goes unswept for an operationally interesting stretch.
 * Deliberately NOT a beat COUNT either: beat frequency varies by orders of
 * magnitude between a busy orchestrator and the unattended launchd watchdog,
 * so "N beats" would mean wildly different real horizons on one host.
 *
 * It applies to exactly ONE outcome — `completed === true` and
 * `removedCount === 0`, with none of `budgetExhausted`, `candidatesCapped` or
 * `liveWriterSkippedCount` set. See `sweepFoundNothingToDo`, which is the sole
 * arbiter; all three of those mean "there is more work queued", so a sweep
 * that hits any of them re-fires on the very next beat rather than standing
 * down, and a sweep that never `completed` refused before enumerating
 * anything and has established nothing at all to stand down from.
 *
 * Note the relationship with `LIVE_WRITE_MARGIN_MS` below: this horizon is
 * TWICE that margin, which is why a live-writer-skipped sweep must be excluded
 * — suppressing it would out-wait the margin that caused the skip.
 */
export const SWEEP_COOLDOWN_MS = 30 * 60_000;

/**
 * A candidate with a DESCENDANT touched within this margin of `now` is treated
 * as still being written into, and is skipped.
 *
 * Root mtime alone is not a liveness signal: on POSIX a directory's mtime moves
 * only when its OWN entry list changes, so a seven-hour `cdk synth` writing into
 * `cdk.out/asset/` leaves `cdk.out`'s own mtime seven hours old. Writing
 * `cdk.out/asset/x.js` moves `cdk.out/asset`'s mtime — the file's OWN PARENT,
 * and nothing above it. So the depth at which the probe looks is exactly the
 * depth at which it can see anything: see `probeLiveWriter` for the bounded
 * breadth-first walk that follows from this, and for what it does and does not
 * guarantee.
 */
export const LIVE_WRITE_MARGIN_MS = 15 * 60_000;

/**
 * How far the caller-supplied `now` may sit from this process's own
 * `Date.now()` before the sweep refuses to run at all.
 *
 * WHY A FINITENESS CHECK IS NOT ENOUGH. `now` is not one input among several:
 * it is the sole reference point for BOTH safety gates on a recursive-removal
 * primitive, and a single skewed value defeats them TOGETHER, in the same
 * direction.
 *
 *   * Staleness — `selectStaleTempDirs` asks `now - mtimeMs > ageThresholdMs`.
 *     Skew `now` forward and EVERY entry is stale, including one created a
 *     second ago.
 *   * Liveness — `probeLiveWriter` asks `now - mtimeMs < LIVE_WRITE_MARGIN_MS`.
 *     Skew `now` forward by more than that margin and a descendant written
 *     MOMENTS AGO reads as ancient, so the probe reports "no live writer".
 *
 * Both gates therefore fail open at once, and the combined result is `rm -rf`
 * across the whole temp root — actively written `bundling-temp-*` build trees
 * included — reported to the AMBER loop as a clean, successful, unremarkable
 * sweep. The inputs that produce it are ordinary caller bugs, not attacks: a
 * seconds-vs-milliseconds unit slip, `Date.now() * 1000`, a monotonic reading
 * passed where a wall clock was meant, a wrong variable or a long-cached
 * timestamp handed in as `now`.
 *
 * WHAT THIS BAND DOES NOT CATCH, STATED PLAINLY. It measures skew BETWEEN the
 * caller's supplied `now` and THIS process's own clock, so it catches only
 * caller-side mistakes. A host-wide clock change — an NTP step, a manual
 * `date` set, a VM resume — moves both readings together: `nowSkewMs` stays at
 * roughly zero, this gate passes, and the staleness and liveness gates above
 * fail open exactly as described, because both compare a stepped clock against
 * `mtimeMs` values written before the step. Closing that would need a
 * monotonic clock plus a boot-relative mtime source, neither of which this
 * layer has; the residual is accepted and disclosed here rather than papered
 * over, in the same style as the `realpathSync` and un-abortable-`rm`
 * residuals named on `sweepStaleTempDirsAsync`.
 *
 * WHY THIS VALUE. The band must be comfortably SMALLER than
 * `LIVE_WRITE_MARGIN_MS`, because that margin is the point at which the
 * liveness gate starts failing open; a band as wide as the margin would admit
 * exactly the skew it exists to catch. A minute is orders of magnitude more
 * than the sub-millisecond gap between a caller reading `Date.now()` and this
 * function reading it again — even with a loaded event loop in between — and
 * orders of magnitude less than any unit slip or clock step. It is a safety
 * wall, deliberately not a generous tolerance: the cost of a false refusal is
 * one skipped beat, and the cost of a false acceptance is deleted live data.
 */
export const SWEEP_NOW_SKEW_TOLERANCE_MS = 60_000;

/**
 * Entry ceiling on one candidate's liveness probe.
 *
 * The probe walks BREADTH-FIRST, so this cap buys the shallow levels first and
 * runs out of budget (if at all) only in the deep tail. It exists because the
 * probe is otherwise a full-tree walk — precisely the cost the deadline exists
 * to avoid — on every candidate, before deciding whether to touch it at all.
 *
 * It is a PRECISION bound, never a safety veto: a probe that hits the cap
 * reports `truncated` and the candidate is still removed (counted in
 * `probeTruncatedCount`). Treating truncation as "assume live" would be the
 * safer-sounding choice and is in fact the worse one — every large
 * `node_modules` tree exceeds the cap, so it would mean the biggest candidates,
 * the ones this whole feature exists to reclaim, are never removed at all.
 */
export const SWEEP_PROBE_MAX_ENTRIES = 4_096;

/**
 * Slice of the budget the size walk declines to spend, so that the REMOVAL it
 * precedes can still be issued and raced inside the same ceiling.
 *
 * WHY IT EXISTS. Without it, a candidate too big to measure in one beat is
 * abandoned un-removed; the sweep is stateless across beats and sorts
 * oldest-first, so the very same candidate is re-selected, re-probed, re-walked
 * and re-abandoned on every subsequent beat, forever. `removedCount` stays 0
 * permanently for exactly the large `bundling-temp-*`/`node_modules` trees
 * targets — a livelock dressed as a safety property. Reserving a slice
 * for the `rm` converts that into forward progress WITHOUT extending the
 * wall-clock ceiling by a single millisecond.
 */
export const SWEEP_REMOVAL_RESERVE_MS = 100;

/**
 * Per-field character bound on a recorded error. The count bound alone does not
 * bound the PAYLOAD: eight errors carrying a megabyte-long `message` each (a
 * deep-path EPERM, or a native binding that threw a stringified struct) satisfy
 * every count assertion and still produce the document the hook cannot afford.
 * Truncated rather than dropped — an operator still needs to see what kind of
 * failure it was.
 */
const MAX_ERROR_FIELD_CHARS = 256;

/** Sentinel resolved by the deadline race. A `Symbol` so no fs result can impersonate it. */
const BUDGET_EXPIRED = Symbol('sweep-budget-expired');

/** Memoised module record for `./cleanup-temp-root-anchor.mjs` — loaded at most once per process. */
let tempRootAnchorModule;

/**
 * Loads the Phase 1 anchor LAZILY, at first use, rather than with a static
 * `import` at the top of this file.
 *
 * WHY (and it is not a style preference). `./cleanup-temp-root-anchor.mjs`
 * does `import { realpathSync } from 'node:fs'` — a NAMED import.
 * `./cleanup-adapter.jest.spec.mjs` registers
 * `jest.unstable_mockModule('node:fs', ...)` with a factory that provides only
 * `readdirSync`/`statSync`/`rmSync`, and an ESM mock factory REPLACES the
 * module for every importer in the registry. A static import of the anchor here
 * would therefore drag that named import into this module's graph and fail
 * `./cleanup-adapter.jest.spec.mjs` at LINK time —
 * `SyntaxError: The requested module 'node:fs' does not provide an export named
 * 'realpathSync'` — in a spec that never touches the async sweep at all. It is
 * the very hazard this file's header already documents for `existsSync`, one
 * level further out: the named import now lives in a DEPENDENCY rather than in
 * this file, so the namespace-import remedy cannot reach it.
 *
 * Deferring the import to first call keeps the anchor out of the graph for
 * every caller that does not use the async sweep, which is the same lazy-import
 * remedy this codebase already applies to a load-time conflict it cannot
 * restructure away (`FacadeService.getUserFacades`'s `await import(...)`, cited
 * in CLAUDE.md). The module record is memoised, so the cost is paid once per
 * process and never on a subsequent beat.
 *
 * @returns {Promise<typeof import('./cleanup-temp-root-anchor.mjs')>}
 */
async function loadTempRootAnchor() {
  tempRootAnchorModule ??= await import('./cleanup-temp-root-anchor.mjs');
  return tempRootAnchorModule;
}

/**
 * Truncates a string to `MAX_ERROR_FIELD_CHARS`, marking that it was cut.
 *
 * TOTAL BY CONSTRUCTION — it cannot throw, for any input. `String(value)` is
 * not safe on an arbitrary value: an object with a throwing `toString` or
 * `Symbol.toPrimitive`, a `null`-prototype object (which has no `toString` at
 * all), or a bare `Symbol` each make it raise. Those are not hypotheses — this
 * function's callers feed it `error.message ?? error` taken from a REJECTED fs
 * promise, and a native binding or a mocked surface can reject with anything at
 * all. A throw here escapes `sweepStaleTempDirsAsync`'s "never rejects"
 * contract, prints a raw stack trace onto the stdout stream the PreToolUse hook
 * `JSON.parse`s, and fails that hook closed for every agent spawn on the host
 *. A placeholder string is strictly better than that.
 *
 * @param {unknown} value
 * @returns {string}
 */
function boundedField(value) {
  let text;
  if (typeof value === 'string') {
    text = value;
  } else {
    try {
      text = String(value);
    } catch {
      text = '<unstringifiable>';
    }
  }
  return text.length <= MAX_ERROR_FIELD_CHARS ? text : `${text.slice(0, MAX_ERROR_FIELD_CHARS)}…`;
}

/**
 * The async sweep's report, in its zero state. The seven original contract
 * fields — the hook reads every one of them unconditionally, so `truncatedCount`
 * must be a real `0` and never `undefined` even on the cleanest of sweeps —
 * plus two counters added in the Phase 2 review round (both additive; no
 * existing field changed name, type or meaning).
 *
 * FIELD SEMANTICS, stated once here because two of them were ambiguous in the
 * Phase 2 build plan and the ambiguity cost a review round:
 *
 *   * `reclaimedBytes` — bytes the disk ACTUALLY got back, as far as this sweep
 *     measured them. It is EXACT when `sizeUnmeasuredCount === 0` and a strict
 *     LOWER BOUND otherwise. It is never a figure the disk did not see; that
 *     invariant is what the AMBER response loop depends on.
 *   * `sizeUnmeasuredCount` — candidates that WERE REMOVED but whose size walk
 *     was cut short by the budget, so only part of their bytes are counted in
 *     `reclaimedBytes`. It is the flag that says "treat `reclaimedBytes` as a
 *     lower bound". (It is emphatically NOT "candidates left in place": leaving
 *     them in place is what stalled the queue — see `reclaimCandidate`.)
 *   * `liveWriterSkippedCount` — candidates deliberately spared by the liveness
 *     probe. A SKIP, not an alarm. It has its own counter so that a host with
 *     many live candidates cannot fill the bounded `errors` ring with non-errors
 *     and push genuine `removal-failed` entries into `truncatedCount`; the ring
 *     still carries ONE coalesced `live-writer-suspected` entry per sweep so an
 *     operator reading only `errors` still sees that skipping happened.
 *   * `probeTruncatedCount` — candidates removed after a liveness probe that hit
 *     its entry cap, i.e. whose deepest levels were not examined. See
 *     `probeLiveWriter` for exactly what is and is not guaranteed.
 *   * `removalIssuedUnconfirmedCount` — candidates whose `rm` WAS ISSUED but
 *     whose result the budget expired waiting for. Node's `fs.rm` cannot be
 *     aborted once called, so these removals are still in flight and will very
 *     probably complete; this sweep simply never found out. Deliberately NOT
 *     folded into `removedCount` (an unsupportable claim) and deliberately not
 *     left invisible (a bare `removedCount: 0` tells the AMBER loop nothing was
 *     reclaimed when a whole tree may have been). See `reclaimCandidate`.
 *   * `completed` — did this sweep reach its ONE terminal return path, i.e.
 *     actually list, select and process candidates? It defaults to `false`
 *     here and is set to `true` in exactly one place (Phase 3 review
 *     round 2, raised independently by both reviewers). Every early refusal —
 *     `invalid-age-threshold`, `invalid-now`, `now-out-of-band`,
 *     `invalid-max-candidates`, `anchor-load-failed`, `confinement-refused`,
 *     `unresolved-root-refused`, `listing-failed`, and any refusal added in
 *     future — returns before that line and therefore reports `false` WITHOUT
 *     anyone having to remember to enumerate it. That asymmetry is the whole
 *     point: a refusal produces `removedCount: 0` with neither
 *     `budgetExhausted` nor `candidatesCapped` set, which is bit-identical to
 *     a genuinely clean sweep, and `sweepFoundNothingToDo` used to stand the
 *     trigger down on both. Standing down on `confinement-refused` or
 *     `now-out-of-band` would silence a SECURITY-relevant refusal signal for a
 *     full `SWEEP_COOLDOWN_MS` after its first sighting.
 *
 * @returns {{ removedCount: number, reclaimedBytes: number, errors: Array<{ path: string, message: string, kind: string }>, truncatedCount: number, budgetExhausted: boolean, candidatesCapped: boolean, sizeUnmeasuredCount: number, liveWriterSkippedCount: number, probeTruncatedCount: number, removalIssuedUnconfirmedCount: number, completed: boolean }}
 */
function createSweepReport() {
  return {
    removedCount: 0,
    reclaimedBytes: 0,
    errors: [],
    truncatedCount: 0,
    budgetExhausted: false,
    candidatesCapped: false,
    sizeUnmeasuredCount: 0,
    liveWriterSkippedCount: 0,
    probeTruncatedCount: 0,
    removalIssuedUnconfirmedCount: 0,
    completed: false,
  };
}

/**
 * Appends to the bounded errors ring, counting — never silently discarding —
 * anything beyond `SWEEP_MAX_ERRORS`. "8 errors" and "8 errors and 4,992 more"
 * are different operational situations and an operator must be able to tell
 * them apart.
 *
 * The ring TRUNCATES, it does not ABORT: the sweep carries on past the ceiling
 * so a healthy candidate behind a wall of EPERM failures is still reclaimed.
 *
 * @param {ReturnType<typeof createSweepReport>} report
 * @param {{ path: string, kind: string, message: unknown }} entry
 * @returns {void}
 */
function recordSweepError(report, { path, kind, message }) {
  if (report.errors.length >= SWEEP_MAX_ERRORS) {
    report.truncatedCount += 1;
    return;
  }
  report.errors.push({ path: boundedField(path), message: boundedField(message), kind });
}

/**
 * Records budget exhaustion ONCE, however many candidates were abandoned. One
 * `budget-exhausted` entry per abandoned candidate would turn the very
 * condition the bound exists for into a payload of its own.
 *
 * @param {ReturnType<typeof createSweepReport>} report
 * @param {string} path
 * @returns {void}
 */
/**
 * Describes a caught value under a prefix, without ever throwing while doing so.
 *
 * `` `${prefix}: ${error?.message ?? error}` `` looks total and is not: reading
 * `.message` runs a getter (which can throw), and interpolation stringifies
 * (which can throw). Both happen BEFORE `recordSweepError` is entered, so its
 * own `boundedField` guard is too late to help — the throw escapes the very
 * `catch` arm that exists to contain it. This helper is what makes those arms
 * genuinely total.
 *
 * @param {unknown} error
 * @param {string} prefix
 * @returns {string}
 */
function describeError(error, prefix) {
  let detail;
  try {
    detail = boundedField(error?.message ?? error);
  } catch {
    detail = '<undescribable>';
  }
  return prefix ? `${prefix}: ${detail}` : detail;
}

/**
 * Records a liveness SKIP: always on its own counter, and at most ONCE in the
 * bounded `errors` ring.
 *
 * A skip is not a failure — the candidate is still there, on purpose, and
 * nobody should be paged for it. Before this split, every skip consumed one of
 * the eight `SWEEP_MAX_ERRORS` slots, so a busy host with nine live candidates
 * filled the ring with non-events and pushed genuine `removal-failed` entries
 * into the anonymous `truncatedCount` — the bound meant to protect the
 * operator's signal was destroying it instead. The counter carries the
 * magnitude; one coalesced ring entry (the same shape `markBudgetExhausted`
 * already uses) keeps the fact visible to an operator reading only `errors`.
 *
 * @param {ReturnType<typeof createSweepReport>} report
 * @param {string} path
 * @returns {void}
 */
function markLiveWriterSkipped(report, path) {
  const isFirst = report.liveWriterSkippedCount === 0;
  report.liveWriterSkippedCount += 1;
  if (!isFirst) return;
  recordSweepError(report, {
    path,
    kind: 'live-writer-suspected',
    message: 'a descendant was written within the live-write margin; skipped (see liveWriterSkippedCount)',
  });
}

function markBudgetExhausted(report, path) {
  if (report.budgetExhausted) return;
  report.budgetExhausted = true;
  recordSweepError(report, {
    path,
    kind: 'budget-exhausted',
    message: 'sweep budget exhausted; remaining candidates deferred to the next beat',
  });
}

/**
 * Converts a promise into one that always FULFILS, tagged with the outcome.
 *
 * Two jobs. (1) It lets a rejection be raced without the losing branch
 * surfacing as an unhandled rejection once the deadline has already won — an
 * unhandled rejection on this path would print a raw stack trace onto the very
 * stdout stream the PreToolUse hook `JSON.parse`s. (2) It keeps the caller's
 * control flow linear: every fs step is a value, never a throw.
 *
 * @param {unknown} candidate a promise, or any value a mocked fs surface returned
 * @returns {Promise<{ ok: true, value: unknown } | { ok: false, error: unknown }>}
 */
function settleOutcome(candidate) {
  return Promise.resolve(candidate).then(
    (value) => ({ ok: true, value }),
    (error) => ({ ok: false, error }),
  );
}

/**
 * The monotonic deadline, expressed as a RACE rather than a loop-top check.
 *
 * A loop-top `performance.now()` comparison is the tempting implementation and
 * it cannot keep this contract: there is no "next iteration" at which to notice
 * the budget when a single `readdir` never settles, which is exactly what a
 * quiet NFS mount, a spun-down external volume or a stalled FUSE layer produce.
 * Racing every individual fs step against one shared timer is what makes the
 * ceiling real. `isExpired()` remains useful as the cheap pre-check that avoids
 * ISSUING work there is no time left to finish.
 *
 * `performance.now()` and not `Date.now()`: wall clock moves under NTP
 * correction, manual clock changes and DST, so a budget built on it can expire
 * instantly or never. (The sweep's `now` PARAMETER stays wall-clock — it is
 * compared against file mtimes, which are wall-clock too.)
 *
 * The timer is NOT `unref`'d and IS cleared in a `finally`: an unref'd pending
 * timer would let the process exit mid-sweep, and a leaked one would hold the
 * event loop open after the report was returned.
 *
 * `isExpired` takes an optional RESERVE: `isExpired(reserveMs)` answers "is
 * there less than `reserveMs` left?". That is what lets a step stop early while
 * deliberately leaving time on the clock for a LATER step to still complete
 * inside the same ceiling — see `SWEEP_REMOVAL_RESERVE_MS`. The reserve is
 * clamped to a quarter of the budget so a tiny `budgetMs` cannot be consumed
 * entirely by its own reserve.
 *
 * @param {{ deadlineAt: number, budgetMs: number }} args
 * @returns {{ isExpired: (reserveMs?: number) => boolean, within: (work: unknown) => Promise<unknown>, dispose: () => void }}
 */
function createBudgetClock({ deadlineAt, budgetMs }) {
  let timer;
  const expiry = new Promise((resolveExpiry) => {
    timer = setTimeout(() => resolveExpiry(BUDGET_EXPIRED), budgetMs);
  });
  const maxReserveMs = budgetMs / 4;

  return {
    isExpired: (reserveMs = 0) =>
      performance.now() >= deadlineAt - Math.min(Math.max(reserveMs, 0), maxReserveMs),
    within: (work) => Promise.race([settleOutcome(work), expiry]),
    dispose: () => clearTimeout(timer),
  };
}

/**
 * Strips trailing separators from an already-absolute sweep root.
 *
 * `basename()` in `./cleanup-age-filter.mjs` takes the last `'/'`-delimited
 * segment, so a path ending in a separator yields `''`, matches no name pattern,
 * and is SILENTLY DROPPED — a no-op reported as `removedCount: 0, errors: []`,
 * which the AMBER response loop reads as "nothing to reclaim" and stops asking.
 * A silent miss on a cleanup primitive is a control-loop failure, not a cosmetic
 * one.
 *
 * Normalised HERE, at the call site, rather than by fixing `basename`:
 * `cleanup-age-filter.mjs` is reused unmodified (contract property 8), and the
 * weakness is latent for its existing caller, which builds every entry path with
 * `join()`. The anchor's `resolvedDir` already arrives `path.resolve`d, so this
 * is belt-and-braces against a future producer rather than a live fix — kept
 * because the cost is one loop and the failure mode it guards is invisible.
 *
 * @param {string} dirPath
 * @returns {string}
 */
function normaliseSweepRoot(dirPath) {
  let normalised = dirPath;
  while (normalised.length > 1 && normalised.endsWith(sep)) {
    normalised = normalised.slice(0, -1);
  }
  return normalised;
}

/**
 * Async twin of `listCandidateEntries`, with the deadline threaded through.
 *
 * NAME-FILTERED BEFORE IT STATS (pre-PR review, High #5), which is the
 * one place it diverges from the sync adapter. The cheap, pure
 * `matchesStaleNamePattern` test runs on `dirent.name` first, so an entry that
 * `selectStaleTempDirs` would discard anyway never costs a syscall. It used to
 * `stat` every dirent in the sweep root and filter afterwards — so a root
 * holding many unrelated entries could spend the entire budget before reaching
 * the first real candidate, and because `readdir` order is stable it did so on
 * EVERY beat: a livelock in which the candidates past the cutoff were
 * structurally unreachable. Applying the selector's own predicate (rather than
 * a second copy of it) keeps filter and selector from drifting apart.
 *
 * A budget expiry returns the entries gathered SO FAR with `expired: true`,
 * not an empty result. Discarding them meant a beat that had already paid for
 * real candidates reported `removedCount: 0` and threw the evidence away; a
 * partial listing is a lower bound the caller can still act on.
 *
 * Otherwise the same selection rules as the sync adapter — directories, plain
 * files and symlinks, each carrying its own `mtimeMs`, with no AGE filtering of
 * its own — and the same symlink rule: a symlink is `lstat`-ed, never
 * `stat`-ed, because the LINK's own mtime is what gates removal of the LINK
 * (`rm` on a symlink path never follows it into the target). A per-entry stat
 * failure means the entry vanished between listing and stat; it is omitted
 * rather than thrown.
 *
 * @param {string} dirPath
 * @param {ReturnType<typeof createBudgetClock>} clock
 * @returns {Promise<{ status: 'ok', entries: Array<{ path: string, mtimeMs: number, isSymlink: boolean, isFile: boolean }>, expired: boolean } | { status: 'failed', error: unknown }>}
 */
async function listCandidateEntriesAsync(dirPath, clock) {
  const listing = await clock.within(fsPromises.readdir(dirPath, { withFileTypes: true }));
  if (listing === BUDGET_EXPIRED) return { status: 'ok', entries: [], expired: true };
  if (!listing.ok) return { status: 'failed', error: listing.error };

  const dirents = Array.isArray(listing.value) ? listing.value : [];
  const entries = [];

  for (const dirent of dirents) {
    // Reserve-aware, for the same reason the size walk and the liveness probe
    // are: a partial listing is only worth returning if there is still clock
    // left to act on it. Stopping AT the deadline would hand the candidate
    // loop a list it must immediately abandon.
    if (clock.isExpired(SWEEP_REMOVAL_RESERVE_MS)) return { status: 'ok', entries, expired: true };

    // THE CHEAP TEST FIRST — a pure string comparison, before any syscall. See
    // this function's doc block for the livelock this ordering removes.
    if (!matchesStaleNamePattern(dirent.name)) continue;

    const isSymlink = typeof dirent.isSymbolicLink === 'function' && dirent.isSymbolicLink();
    const isFile = !isSymlink && typeof dirent.isFile === 'function' && dirent.isFile();
    if (!dirent.isDirectory() && !isFile && !isSymlink) continue;

    const entryPath = join(dirPath, dirent.name);
    const stats = await clock.within(
      isSymlink ? fsPromises.lstat(entryPath) : fsPromises.stat(entryPath),
    );
    if (stats === BUDGET_EXPIRED) return { status: 'ok', entries, expired: true };
    if (!stats.ok || typeof stats.value?.mtimeMs !== 'number') continue;

    entries.push({ path: entryPath, mtimeMs: stats.value.mtimeMs, isSymlink, isFile });
  }

  return { status: 'ok', entries, expired: false };
}

/**
 * Async, iterative, budget-checked recursive byte total for a directory tree
 * (files only), measured BEFORE removal.
 *
 * The budget is checked PER DIRECTORY LEVEL as well as per candidate: a single
 * candidate can be the whole backlog, and a walk that only yielded between
 * candidates would blow the deadline inside the first one. Every fs call is
 * individually guarded — a path that vanishes mid-walk contributes 0 bytes
 * rather than aborting the measurement, mirroring `directorySizeBytes`.
 *
 * An ABANDONED walk is reported as such (`expired: true`) and the total it
 * carries is then a PARTIAL one. The caller removes the candidate anyway and
 * flags the imprecision via `sizeUnmeasuredCount` — see `reclaimCandidate` and
 * `SWEEP_REMOVAL_RESERVE_MS` for why never-remove-unmeasured, the rule this
 * replaces, livelocked the queue. What is preserved is the invariant that
 * actually mattered: the partial total counts only bytes this walk really
 * measured, so `reclaimedBytes` stays a figure the disk DID see (a lower bound),
 * never one it did not.
 *
 * The walk stops `SWEEP_REMOVAL_RESERVE_MS` BEFORE the deadline rather than at
 * it, so the removal it precedes still fits inside the ceiling.
 *
 * Iterative (an explicit stack) rather than recursive so a pathologically deep
 * tree cannot overflow the stack on an unattended path.
 *
 * @param {string} dirPath
 * @param {ReturnType<typeof createBudgetClock>} clock
 * @returns {Promise<{ total: number, expired: boolean }>}
 */
async function directorySizeBytesAsync(dirPath, clock) {
  const pending = [dirPath];
  let total = 0;

  while (pending.length > 0) {
    if (clock.isExpired(SWEEP_REMOVAL_RESERVE_MS)) return { total, expired: true };

    const current = pending.pop();
    const listing = await clock.within(fsPromises.readdir(current, { withFileTypes: true }));
    if (listing === BUDGET_EXPIRED) return { total, expired: true };
    // Vanished, unreadable, or a mock returning nothing — 0 bytes, not a throw.
    if (!listing.ok || !Array.isArray(listing.value)) continue;

    for (const dirent of listing.value) {
      // Per ENTRY, not only per level: one directory holding 200,000 files is a
      // single level, and a level-only check would spend the whole reserve
      // inside it.
      if (clock.isExpired(SWEEP_REMOVAL_RESERVE_MS)) return { total, expired: true };

      // 0 bytes for a nested symlink — see the identical rule and the full
      // reasoning in `directorySizeBytes`. `rm` removes the link, not its
      // target, so a followed `stat` here would add bytes the disk keeps.
      if (typeof dirent.isSymbolicLink === 'function' && dirent.isSymbolicLink()) continue;

      const entryPath = join(current, dirent.name);
      if (dirent.isDirectory()) {
        pending.push(entryPath);
        continue;
      }
      const stats = await clock.within(fsPromises.stat(entryPath));
      if (stats === BUDGET_EXPIRED) return { total, expired: true };
      if (stats.ok && typeof stats.value?.size === 'number') total += stats.value.size;
    }
  }

  return { total, expired: false };
}

/**
 * The bounded BREADTH-FIRST recency probe (see `LIVE_WRITE_MARGIN_MS`).
 *
 * ---------------------------------------------------------------------------
 * WHY THIS IS NOT A ONE-LEVEL PROBE (it was, and that was a live-data hazard)
 * ---------------------------------------------------------------------------
 *
 * The original probe read one level deep and justified it as sufficient: "a
 * write deep in the tree moves ITS OWN parent's mtime, and that parent is an
 * immediate child of the candidate." The first half is true; the second holds
 * ONLY at depth 2. Writing `cand/child/file.js` moves `cand/child` — an
 * immediate child, seen. Writing `cand/node_modules/pkg/chunk.js` moves
 * `cand/node_modules/pkg` — a GRANDCHILD — and directory mtimes do not
 * propagate upward, so `cand/node_modules`'s own mtime stays exactly as old as
 * the moment `pkg` was created. A one-level probe therefore sees nothing, and
 * the sweep recursively removes a tree that is being written into right now.
 * That is not a corner case: a `bundling-temp-` dir over `node_modules/<pkg>/…` is the
 * literal shape of the esbuild / `cdk synth` bundling output exists to
 * reclaim, and it was reproduced deleting a live directory in review.
 *
 * ---------------------------------------------------------------------------
 * WHAT THIS VERSION ACTUALLY GUARANTEES — stated precisely, not aspirationally
 * ---------------------------------------------------------------------------
 *
 *   * It examines the candidate's descendants breadth-first, so it detects a
 *     write at ANY depth, provided the mutated parent directory is reached
 *     before the walk stops.
 *   * The walk stops on the first of: a live mtime found (`live: true`); the
 *     tree exhausted (`live: false`, `truncated: false` — a COMPLETE answer);
 *     `SWEEP_PROBE_MAX_ENTRIES` entries examined (`truncated: true`); or the
 *     budget (`expired: true`).
 *   * `truncated: true` is an INCOMPLETE answer and the caller removes the
 *     candidate anyway, counting it in `probeTruncatedCount`. See
 *     `SWEEP_PROBE_MAX_ENTRIES` for why the safer-sounding alternative is worse.
 *     RESIDUAL RISK, disclosed rather than papered over (this design's
 *     accepted-gap posture): in a tree larger than the cap, a live write in the
 *     unexamined deep tail is not seen. Breadth-first ordering is what keeps
 *     that residual small — the shallow levels, where a bundler's own working
 *     directories sit, are always covered first.
 *
 * A child whose mtime cannot be read is not evidence of a writer and is
 * skipped; a candidate with no children at all is NOT live (otherwise the empty
 * directories that make up much of a real backlog would never drain).
 *
 * @param {string} dirPath
 * @param {number} now wall-clock, compared against wall-clock mtimes
 * @param {ReturnType<typeof createBudgetClock>} clock
 * @returns {Promise<{ expired: true } | { expired: false, live: boolean, truncated: boolean }>}
 */
async function probeLiveWriter(dirPath, now, clock) {
  // A QUEUE, consumed from the front — breadth-first. A stack would be
  // depth-first, which spends the entry cap plunging down one arbitrary branch
  // and can leave the candidate's own immediate children unexamined.
  const queue = [dirPath];
  let examined = 0;

  while (queue.length > 0) {
    // RESERVE-AWARE, exactly like `directorySizeBytesAsync` (pre-PR
    // review, High #4). A bare `isExpired()` here let one big candidate's
    // probe spend the budget to the last millisecond, leaving nothing for the
    // candidates behind it — the same shape that made the authors give the
    // size walk a reserve in the first place. Stopping
    // `SWEEP_REMOVAL_RESERVE_MS` early is what makes the caller's `continue`
    // (below) buy real forward progress rather than a cosmetic one: the
    // cheap candidates behind this one — plain files and symlinks, which skip
    // the probe entirely — still fit inside what is left.
    if (clock.isExpired(SWEEP_REMOVAL_RESERVE_MS)) return { expired: true };

    const current = queue.shift();
    const listing = await clock.within(fsPromises.readdir(current, { withFileTypes: true }));
    if (listing === BUDGET_EXPIRED) return { expired: true };
    // Vanished, unreadable, or a mock returning nothing — no evidence, not a throw.
    if (!listing.ok || !Array.isArray(listing.value)) continue;

    for (const dirent of listing.value) {
      // Per ENTRY as well as per level, and reserve-aware for the same reason:
      // one directory holding 200,000 children is a single level.
      if (clock.isExpired(SWEEP_REMOVAL_RESERVE_MS)) return { expired: true };
      if (examined >= SWEEP_PROBE_MAX_ENTRIES) return { expired: false, live: false, truncated: true };
      examined += 1;

      const entryPath = join(current, dirent.name);
      const stats = await clock.within(fsPromises.stat(entryPath));
      if (stats === BUDGET_EXPIRED) return { expired: true };
      if (!stats.ok || typeof stats.value?.mtimeMs !== 'number') continue;

      // `<` on the signed difference, so a FUTURE-dated descendant counts as
      // live too — a clock skew that puts one ahead of `now` is not a reason to
      // delete it.
      if (now - stats.value.mtimeMs < LIVE_WRITE_MARGIN_MS) {
        return { expired: false, live: true, truncated: false };
      }

      if (typeof dirent.isDirectory === 'function' && dirent.isDirectory()) queue.push(entryPath);
    }
  }

  return { expired: false, live: false, truncated: false };
}

/**
 * Measures one selected candidate, then removes it, accumulating into `report`.
 *
 * Branches on the candidate's own kind, carrying its rule forward verbatim:
 * a SYMLINK is sized 0 (removing the link frees none of the target's content,
 * so any other figure is bytes the disk never got back); a PLAIN FILE is sized
 * by `stat().size`; a DIRECTORY by the async walk.
 *
 * @param {{ path: string, mtimeMs: number, isSymlink: boolean, isFile: boolean }} entry
 * @param {ReturnType<typeof createSweepReport>} report
 * @param {ReturnType<typeof createBudgetClock>} clock
 * @param {{ probeTruncated?: boolean }} [context] carried from the liveness probe, counted only on a
 *   successful removal so the report never describes a candidate that is still there
 * @returns {Promise<{ expired: boolean }>}
 */
async function reclaimCandidate(entry, report, clock, context = {}) {
  const probeTruncated = context.probeTruncated === true;
  let sizeUnmeasured = false;
  let sizeBytes = 0;

  if (entry.isFile) {
    const stats = await clock.within(fsPromises.stat(entry.path));
    if (stats === BUDGET_EXPIRED) return { expired: true };
    if (stats.ok && typeof stats.value?.size === 'number') {
      sizeBytes = stats.value.size;
    } else if (!stats.ok) {
      // Informational, exactly as on the sync path: removal is still attempted
      // and, on success, the candidate counts toward `removedCount` with 0
      // bytes contributed.
      recordSweepError(report, {
        path: entry.path,
        kind: 'size-read-failed',
        message: describeError(stats.error, 'size read failed'),
      });
    }
  } else if (!entry.isSymlink) {
    const walk = await directorySizeBytesAsync(entry.path, clock);
    // A walk cut short by the budget yields a PARTIAL total, and the candidate
    // is removed anyway. Leaving it in place was the previous rule and it
    // livelocks: the sweep is stateless across beats and sorts oldest-first, so
    // the same too-big candidate is re-selected and re-abandoned on every beat
    // forever, and `removedCount` stays 0 for precisely the trees this feature
    // exists to reclaim. `directorySizeBytesAsync` reserves
    // `SWEEP_REMOVAL_RESERVE_MS` so this `rm` still fits inside the ceiling,
    // and the partial total counts only bytes genuinely measured — so
    // `reclaimedBytes` becomes a LOWER BOUND, never a fiction.
    sizeUnmeasured = walk.expired;
    sizeBytes = walk.total;
  }

  const removal = await clock.within(fsPromises.rm(entry.path, { recursive: true, force: true }));
  // THE REMOVAL WAS ISSUED, AND THE BUDGET RAN OUT WAITING FOR IT. The earlier
  // wording here ("the budget ran out before the removal could even be issued …
  // nothing was removed") was simply false: the `rm` above is CALLED first and
  // raced second, because `fsPromises.rm` offers no cancellation — no
  // `AbortSignal`, no way to withdraw a syscall the kernel already has. The race
  // therefore bounds how long this sweep WAITS, not how long the delete takes,
  // and the recursive delete is still in flight right now. It will very probably
  // complete.
  //
  // So this is an UNKNOWN, not a zero, and it gets its own axis rather than
  // being folded into either neighbour. `removedCount` would be a claim this
  // function cannot support; a silent `0` would tell the AMBER loop nothing was
  // reclaimed while a whole tree drained, so it re-sweeps for pressure that is
  // already relieved. `reclaimedBytes` stays untouched for the same reason it
  // always does — it counts only bytes a completed removal confirmed.
  // `sizeUnmeasuredCount` also stays untouched: it describes CONFIRMED removals
  // whose size walk was cut short, a different fact about a different set.
  if (removal === BUDGET_EXPIRED) {
    report.removalIssuedUnconfirmedCount += 1;
    return { expired: true };
  }

  if (removal.ok) {
    report.removedCount += 1;
    report.reclaimedBytes += sizeBytes;
    if (sizeUnmeasured) report.sizeUnmeasuredCount += 1;
    if (probeTruncated) report.probeTruncatedCount += 1;
  } else {
    recordSweepError(report, {
      path: entry.path,
      kind: 'removal-failed',
      message: describeError(removal.error, ''),
    });
  }

  return { expired: false };
}

/**
 * PURE. Did this sweep run to completion and find NOTHING worth doing?
 *
 * That — and only that — is the outcome the cooldown answers (Phase 3
 * review, Medium). "Ran to completion" is read from a STRUCTURAL fact the
 * sweep sets, and the three "come back next beat" conditions are checked
 * explicitly rather than inferred from `removedCount`:
 *
 *   * `completed` — the sweep reached its single terminal return path, having
 *     actually listed and processed candidates. Checked FIRST, because
 *     without it the rest of this predicate cannot tell a genuinely clean
 *     sweep from an early REFUSAL (Phase 3 review round 2, raised
 *     independently by both reviewers): every refusal branch in
 *     `sweepStaleTempDirsAsync` returns `removedCount: 0` with neither flag
 *     set, which is exactly the shape this function used to suppress on.
 *     Standing down for `SWEEP_COOLDOWN_MS` on `confinement-refused` (the
 *     hostile/misconfigured-`TMPDIR` case Phase 1's anchor exists to catch) or
 *     `now-out-of-band` (the clock-skew safety gate) would take a refusal
 *     signal that is currently emitted on EVERY beat and silence it for half
 *     an hour after its first sighting.
 *   * `budgetExhausted` — the sweep ran out of wall clock with candidates
 *     still unexamined. There IS more work; standing down would be the exact
 *     wrong response.
 *   * `candidatesCapped` — more candidates existed than `SWEEP_MAX_CANDIDATES`
 *     allowed in one beat. Same reasoning: a backlog is being drained across
 *     beats by design, and a cooldown would throttle the drain.
 *   * `liveWriterSkippedCount` — candidates the liveness probe spared. These
 *     are not "nothing to do": they are work DEFERRED, and deferred by
 *     `LIVE_WRITE_MARGIN_MS` (15 min), which is HALF `SWEEP_COOLDOWN_MS` (30
 *     min). Suppressing here would out-wait the very margin that caused the
 *     skip, throttling precisely the case where retrying soon is most likely
 *     to succeed.
 *
 * All are read defensively (`=== true`, and `> 0` on the counter) so a report
 * missing a field — an older shape, or a future one — is treated as "cannot
 * confirm this was a clean, complete, empty-handed sweep", which resolves to
 * NOT suppressing. Every uncertainty here resolves toward sweeping again.
 *
 * Lives here rather than in `cli.mjs` because it is a statement about the
 * REPORT SHAPE this module defines — so it cannot drift from
 * `createSweepReport` the next time a field is added.
 *
 * @param {object | undefined} report
 * @returns {boolean}
 */
export function sweepFoundNothingToDo(report) {
  if (!report) return false;
  if (report.completed !== true) return false;
  if (report.removedCount !== 0) return false;
  if (report.budgetExhausted === true) return false;
  if (report.candidatesCapped === true) return false;
  if (report.liveWriterSkippedCount > 0) return false;
  return true;
}

/**
 * The report a CALLER must attach when `sweepStaleTempDirsAsync` throws at it
 * anyway (Phase 3 review, Medium).
 *
 * `sweepStaleTempDirsAsync` contracts never to throw, and every caller wraps
 * it in a belt-and-braces `catch` against a future edit breaking that. The
 * defect this function fixes is what that `catch` used to DO: return
 * `undefined`, which in `cli.mjs`'s beat JSON means "the trigger never fired"
 * — indistinguishable from "nothing was deleted". But the scenario the `catch`
 * exists for is a throw from PARTWAY THROUGH the sweep, i.e. after removals
 * have already been issued. Reporting an irreversible deletion as if nothing
 * had happened is the one outcome an operator must never be handed.
 *
 * So: a PRESENT, shape-complete report, with the failure named on the bounded
 * errors ring under its own `kind: 'sweep-threw'`. The counters are all zero
 * because the throw destroyed whatever tallies the sweep had accumulated —
 * zero here means "this sweep can no longer account for itself", which is
 * exactly what the `errors` entry beside them says. Deliberately NOT a guess
 * at how much was removed.
 *
 * `budgetExhausted`/`candidatesCapped` stay `false`: both mean "there is more
 * work queued, come back immediately", and a sweep that threw has established
 * no such thing. `completed` stays `false` for the mirror-image reason — a
 * sweep that threw did not run to completion, so it must not stand the trigger
 * down either.
 *
 * Never throws — `describeError` is total by construction, which is why the
 * message is built through it rather than by interpolating `error.message`.
 *
 * @param {unknown} dir The sweep root that was attempted.
 * @param {unknown} error The caught value.
 * @returns {ReturnType<typeof createSweepReport>}
 */
export function sweepThrewReport(dir, error) {
  const report = createSweepReport();
  recordSweepError(report, {
    path: dir,
    kind: 'sweep-threw',
    message: describeError(error, 'sweep threw'),
  });
  return report;
}

/**
 * The budgeted, bounded, liveness-aware sweep the auto-trigger calls from
 * the beat path. NEVER REJECTS, for any input: a rejection here would surface as
 * an unhandled rejection and a raw stack trace on the stdout stream the
 * PreToolUse hook `JSON.parse`s, denying every agent spawn on the host.
 *
 * It also writes NOTHING to stdout, ever, for the same reason — one stray
 * `console.log` is a deny.
 *
 * Order of business, and each step's reason:
 *
 *   0. `now` is validated FIRST-CLASS, alongside `ageThresholdMs`, and in TWO
 *      steps. It is half of every staleness comparison AND half of every
 *      liveness comparison, so an invalid one produces a perfectly clean-looking
 *      empty report that the AMBER loop reads as "nothing to reclaim"
 *      (`invalid-now`), and a merely IMPLAUSIBLE one — finite, but skewed from
 *      the real clock — fails both of those gates open at once and turns the
 *      sweep into an unconditional `rm -rf` of the temp root that still reports
 *      success (`now-out-of-band`). See both gates in the body and
 *      `SWEEP_NOW_SKEW_TOLERANCE_MS`.
 *   1. `ageThresholdMs` is validated HERE, before delegating:
 *      `selectStaleTempDirs` THROWS a `TypeError` on a non-positive or
 *      non-finite value, and a throw must never escape onto the beat path. Every
 *      invalid shape is surfaced under the one `invalid-age-threshold` kind.
 *   2. A non-positive/non-finite `budgetMs` means "there is no time" — a caller
 *      statement, not a degenerate input to normalise away. A sweep that
 *      helpfully did "just one" would be unbudgeted destructive work on a hot
 *      path. (`Infinity` is refused on the same gate: an unbounded budget is not
 *      a thing this function offers, and `setTimeout(fn, Infinity)` silently
 *      degenerates to 1 ms.) `maxCandidates` is refused for the same underlying
 *      reason — "do nothing" must never be quietly upgraded to "do up to 24" on
 *      a recursive-removal primitive — but along TWO distinct gates, because a
 *      numeric integer `0` (or below) is a caller STATEMENT (silent,
 *      error-free, attributed to `candidatesCapped`) while a
 *      non-number/`NaN`/`Infinity`/non-integer is a caller BUG and must be
 *      distinguishable from a healthy capped sweep (`invalid-max-candidates`).
 *      A fractional cap counts here as a bug precisely because `Math.floor` of
 *      a value in `(0, 1)` is `0` — silently the very shape the split closes.
 *   3. CONFINEMENT, via the Phase 1 anchor, BEFORE ANY LISTING. The
 *      module-private `isConfinedToTmpRoot` is deliberately not used: it measures
 *      `--dir` against the very TMPDIR variable the auto-trigger's `dir` derives
 *      from, so it is vacuous on this path (see
 *      `./cleanup-temp-root-anchor.mjs`'s header).
 *   4. `resolvedVia !== 'realpath'` is ALSO refused, under its own distinct
 *      `unresolved-root-refused` kind. The anchor reports `allowed: true` for
 *      that state, so gating on `allowed` alone is not enough: `realpathSync`
 *      resolves wholly or not at all, and the anchor's fallback keeps the
 *      candidate's WRITTEN form with every symlink unresolved — so
 *      `<temp>/link/absent` can be "allowed" while its real location is
 *      elsewhere entirely. Refusing costs no capability (a root with a missing
 *      component has nothing to reclaim) and closes the TOCTOU shape where a
 *      symlink component appears between the check and the walk. The kind is
 *      distinct from `confinement-refused` on purpose: that one is a
 *      configuration error a human must fix, this one is ordinarily transient
 *      and self-healing on a later beat.
 *   5. The walk targets the anchor's `resolvedDir`, never the raw `dir` — the
 *      path that was guarded is the path that gets walked, which is what closes
 *      the symlink divergence.
 *
 * `errors` conflates several kinds, distinguished by `kind` — see
 * `sweepStaleTempDirs`'s doc comment for the shared ones, plus:
 * `confinement-refused`, `unresolved-root-refused`, `anchor-load-failed`,
 * `invalid-age-threshold`, `invalid-now`, `now-out-of-band`,
 * `invalid-max-candidates`, `budget-exhausted`, `sweep-aborted`,
 * and `live-writer-suspected` (a deliberate SKIP, not an alarm: the candidate is
 * still there, on purpose, and the caller must not page anyone for it — it is
 * recorded ONCE per sweep, with the magnitude on `liveWriterSkippedCount`).
 *
 * THE WALL-CLOCK CEILING BOUNDS HOW LONG THIS FUNCTION TAKES TO RESOLVE, WITH
 * TWO NAMED EXCEPTIONS — stated here rather than claimed away, because the
 * unqualified word "hard" was doing work the code does not do. Every fs step is
 * raced against a timer armed before the first of them, so no asynchronous step
 * can make the returned promise overrun. The exceptions are:
 *
 *   1. THE CONFINEMENT CHECK BLOCKS THE THREAD. `classifySweepRoot` calls
 *      `realpathSync`, and a synchronous syscall holds the only thread, so the
 *      timer cannot fire while it runs. A stalled mount can hold that ONE call
 *      for as long as the kernel holds it, and the deadline is simply not
 *      observed until it returns.
 *   2. A RACED `rm` IS NOT AN ABORTED `rm`. `fsPromises.rm` exposes no
 *      cancellation — no `AbortSignal`, no way to withdraw a syscall already
 *      handed to the kernel — so `clock.within(fsPromises.rm(...))` bounds only
 *      how long this sweep WAITS for the removal, never how long the removal
 *      itself takes. When that race resolves as expired, the recursive delete
 *      is still in flight and will very probably complete. The directory is
 *      gone; this beat just never found out.
 *
 * Exception 2 is why `removalIssuedUnconfirmedCount` exists. Counting such a
 * candidate in `removedCount` would be a claim the function cannot support, and
 * reporting a bare `removedCount: 0` would tell the AMBER loop that nothing was
 * reclaimed when a whole tree may have been — so it is counted on its own axis
 * and named for what actually happened. Engineering around it is not available
 * at this layer: aborting the syscall is outside Node's fs API, and declining to
 * race it would hand the ceiling to a hung filesystem. Disclosed, by design's
 * accepted-gap posture.
 *
 * (`SWEEP_REMOVAL_RESERVE_MS` is not an exception in the other direction: it
 * shortens a step so a later one fits INSIDE the ceiling, never past it.)
 *
 * KNOWN IMPRECISION, disclosed rather than papered over (this design's
 * accepted-gap posture): two overlapping beats racing the same candidate both
 * see `rm` succeed under `force: true`, so `reclaimedBytes` can double-count.
 * No lock is introduced; the figure is a signal, not an audit.
 *
 * THE ARGUMENT OBJECT IS DESTRUCTURED IN THE BODY, NOT IN THE PARAMETER LIST,
 * and that is load-bearing rather than stylistic:
 * `./cleanup-async-no-sync-fs.jest.spec.mjs`'s source-scan half locates this
 * function's body by taking the first `{` after the signature and brace-
 * balancing from there, so a destructuring parameter list would hand that scan
 * the PARAMETER OBJECT as if it were the body — and every structural assertion
 * about the async path (no `*Sync` identifier, no sync-helper delegation, a
 * `performance.now()` deadline, no stdout) would then pass vacuously against a
 * few lines of parameter names. Keep the parameter list brace-free.
 *
 * @param {{ dir: string, ageThresholdMs: number, now: number, budgetMs?: number,
 *   maxCandidates?: number, allowedPrefixes?: string[] }} args
 * @returns {Promise<ReturnType<typeof createSweepReport>>}
 */
export async function sweepStaleTempDirsAsync(args) {
  const {
    dir,
    ageThresholdMs,
    now,
    budgetMs = SWEEP_BUDGET_MS,
    maxCandidates = SWEEP_MAX_CANDIDATES,
    allowedPrefixes,
  } = args ?? {};
  const report = createSweepReport();
  // `boundedField`, not `String(dir)`: the latter throws on a hostile `dir`, in
  // the one function whose contract is that it never throws.
  const dirLabel = boundedField(dir);

  if (typeof ageThresholdMs !== 'number' || !Number.isFinite(ageThresholdMs) || ageThresholdMs <= 0) {
    recordSweepError(report, {
      path: dirLabel,
      kind: 'invalid-age-threshold',
      message: `ageThresholdMs must be a positive, finite number — got ${boundedField(ageThresholdMs)}`,
    });
    return report;
  }

  // `now` is validated on exactly the same footing as `ageThresholdMs`, and for
  // a sharper reason. It is not merely an input: it is one half of every
  // staleness comparison. An `undefined`/`NaN`/string `now` makes `now - mtimeMs
  // > ageThresholdMs` false for EVERY entry, so the sweep selects nothing and
  // returns `{ removedCount: 0, errors: [] }` — a report bit-identical to a
  // genuinely healthy, already-clean temp root. The AMBER response loop reads
  // that as "nothing to reclaim" and stops asking, so a caller bug becomes a
  // silent control-loop failure with no operator-visible symptom at all. A
  // distinct refusal kind is the difference between a bug and an outage.
  if (typeof now !== 'number' || !Number.isFinite(now)) {
    recordSweepError(report, {
      path: dirLabel,
      kind: 'invalid-now',
      message: `now must be a finite wall-clock epoch in ms — got ${boundedField(now)}`,
    });
    return report;
  }

  // FINITE IS NOT SANE. The gate above rejects `NaN`/`Infinity`/non-numbers;
  // this one rejects a finite value that cannot plausibly be the current wall
  // clock. See `SWEEP_NOW_SKEW_TOLERANCE_MS` for why a skewed `now` is the
  // worst single input this module accepts: it fails the staleness gate and the
  // liveness gate open SIMULTANEOUSLY, turning the sweep into an unconditional
  // `rm -rf` of the temp root that reports itself as a clean success.
  //
  // `Date.now()` is read HERE rather than trusted from the argument — the whole
  // point is to have a second, independent reading to compare against. The
  // comparison is symmetric (`Math.abs`) because a BACKWARD skew is a caller
  // bug too; it happens to fail safe (nothing looks stale) rather than
  // dangerous, but a sweep that silently does nothing forever is still the
  // silent-control-loop failure the `invalid-now` gate exists to prevent, and
  // it deserves the same distinguishable refusal.
  // PHASE 3 MUST RESOLVE THIS, and deliberately does not here: when the beat
  // runs under a simulated clock (`ARM_FAKE_NOW_MS`), the `now` it hands in is
  // the FAKE reading while this comparison uses the real one, so the band
  // refuses every swept beat in that mode. Whether the async sweep should read
  // the beat's simulated clock or keep an independent real one is a Phase 3
  // design decision, taken when the trigger is wired — not a Phase 2 change.
  const nowSkewMs = Math.abs(now - Date.now());
  if (nowSkewMs > SWEEP_NOW_SKEW_TOLERANCE_MS) {
    recordSweepError(report, {
      path: dirLabel,
      kind: 'now-out-of-band',
      message:
        `now is ${Math.round(nowSkewMs)} ms from this host's clock, beyond the ` +
        `${SWEEP_NOW_SKEW_TOLERANCE_MS} ms safety band — refusing to remove anything`,
    });
    return report;
  }

  if (typeof budgetMs !== 'number' || !Number.isFinite(budgetMs) || budgetMs <= 0) {
    markBudgetExhausted(report, dirLabel);
    return report;
  }

  // TWO DIFFERENT SITUATIONS, DELIBERATELY GIVEN TWO DIFFERENT REPORTS — they
  // were once conflated, and the conflation reintroduced the exact failure the
  // `invalid-now` gate was built to close.
  //
  // (a) A NON-NUMBER, `NaN`, `Infinity`, or a NON-INTEGER is a caller BUG. It
  //     must not fall through to `SWEEP_MAX_CANDIDATES` — turning "remove
  //     nothing" into "remove up to 24" on a recursive-removal primitive is the
  //     worst direction for a misread argument to fail in — but neither may it
  //     be refused SILENTLY. A silent refusal here emits `{ candidatesCapped:
  //     true, errors: [] }`, which is bit-identical to a perfectly healthy
  //     sweep that drained all 24 slots and has more work queued. Phase 3's
  //     AMBER loop reads the flag as "there is more to do, re-arm" and the bug
  //     never surfaces. Distinct kind, same footing as `invalid-now` and
  //     `invalid-age-threshold`.
  //
  //     `Number.isInteger` is part of this gate, not a tidiness nicety. A
  //     FRACTIONAL value in the open interval `(0, 1)` — `0.5`, say, from a
  //     ratio computed against a total — passes both "is a finite number" and
  //     "> 0", and then `Math.floor(0.5)` is `0`: the cap silently becomes
  //     zero and the sweep emits exactly the `{ candidatesCapped: true,
  //     errors: [] }` shape the split above exists to eliminate. A cap is a
  //     count of directories; half a directory is a bug, so it routes here.
  if (typeof maxCandidates !== 'number' || !Number.isInteger(maxCandidates)) {
    recordSweepError(report, {
      path: dirLabel,
      kind: 'invalid-max-candidates',
      message: `maxCandidates must be a finite integer — got ${boundedField(maxCandidates)}`,
    });
    return report;
  }

  // (b) A NUMERIC, non-positive value is the caller SAYING "do no work this
  //     beat" — the same category of explicit statement as `budgetMs <= 0`
  //     above, and honoured the same way. `candidatesCapped` is set because the
  //     cap is, truthfully, the reason nothing happened; no error is recorded
  //     because nothing went wrong, and recording one would page an operator
  //     about a beat that did exactly what it was told.
  if (maxCandidates <= 0) {
    report.candidatesCapped = true;
    return report;
  }
  const candidateCap = Math.floor(maxCandidates);

  // THE CLOCK IS ARMED HERE — BEFORE the confinement step, not after it, and
  // that ordering is the fix for a review finding rather than an accident.
  // Confinement requires loading the anchor module and calling
  // `classifySweepRoot`, and the budget must be running for both: otherwise the
  // time they take is not charged against `budgetMs` (so the documented ceiling
  // is not the real wall clock) and, worse, a stall inside them happens with no
  // timer armed at all — in the function whose entire reason for existing is
  // that it cannot stall.
  const deadlineAt = performance.now() + budgetMs;
  const clock = createBudgetClock({ deadlineAt, budgetMs });

  try {
    // Lazily imported rather than statically — see `loadTempRootAnchor`. Raced,
    // because a dynamic import performs IO (and can reject outright on a link
    // error, which outside a `catch` would be an unhandled rejection printing a
    // stack trace onto the stdout the PreToolUse hook parses).
    const anchor = await clock.within(loadTempRootAnchor());
    if (anchor === BUDGET_EXPIRED) {
      markBudgetExhausted(report, dirLabel);
      return report;
    }
    if (!anchor.ok) {
      recordSweepError(report, {
        path: dirLabel,
        kind: 'anchor-load-failed',
        message: describeError(anchor.error, 'confinement anchor could not be loaded'),
      });
      return report;
    }

    let verdict;
    try {
      // RESIDUAL, DISCLOSED (this design's accepted-gap posture) — the one place
      // this function's ceiling is best-effort rather than absolute.
      // `classifySweepRoot` calls `realpathSync` internally, and a synchronous
      // syscall cannot be abandoned once entered: it blocks the single thread,
      // so the timer armed above cannot fire until it returns. Racing it would
      // be theatre. What the ordering above DOES buy is real: the timer is
      // running, so the moment control returns the deadline is already correct
      // and the remaining budget is charged honestly. A stalled mount can still
      // hold this one call — and therefore the beat — for as long as the kernel
      // holds the syscall. Making that residual go away needs `fsPromises
      // .realpath` inside the anchor, which is Phase 1 surface and out of scope
      // here; it is tracked rather than silently absorbed.
      verdict = anchor.value.classifySweepRoot(dir, { allowedPrefixes });
    } catch (error) {
      recordSweepError(report, {
        path: dirLabel,
        kind: 'anchor-load-failed',
        message: describeError(error, 'confinement check threw'),
      });
      return report;
    }

    if (!verdict.allowed) {
      recordSweepError(report, {
        path: dirLabel,
        kind: 'confinement-refused',
        message: verdict.reason ?? 'refused',
      });
      return report;
    }
    if (verdict.resolvedVia !== 'realpath') {
      recordSweepError(report, {
        path: dirLabel,
        kind: 'unresolved-root-refused',
        message: `sweep root could not be fully resolved (resolvedVia: ${verdict.resolvedVia}); refusing to walk it`,
      });
      return report;
    }

    const sweepRoot = normaliseSweepRoot(verdict.resolvedDir);

    // The confinement step may itself have consumed the budget (see the
    // residual above). Nothing is listed, let alone removed, on a budget that
    // is already spent.
    if (clock.isExpired()) {
      markBudgetExhausted(report, sweepRoot);
      return report;
    }

    const listing = await listCandidateEntriesAsync(sweepRoot, clock);
    if (listing.status === 'failed') {
      // The `dir` itself vanished, was unmounted, or turned permission-denied
      // between the confinement verdict and the listing. A clean, empty report
      // — mirroring the sync path — never an escaping throw.
      recordSweepError(report, {
        path: sweepRoot,
        kind: 'listing-failed',
        message: describeError(listing.error, ''),
      });
      return report;
    }
    // A listing cut short by the budget is RECORDED and then WORKED, not
    // discarded (pre-PR review, High #5). Returning here threw away every
    // candidate the beat had already paid to discover and reported
    // `removedCount: 0` — and because `readdir` order is stable, the next beat
    // paid exactly the same cost and reached exactly the same point. The
    // exhaustion flag is still set, so the cooldown will not stand the sweep
    // down and the caller still knows more work is queued.
    if (listing.expired) markBudgetExhausted(report, sweepRoot);

    // Oldest first, so successive capped beats drain the backlog in a defined
    // order instead of re-picking the same arbitrary slice.
    const selected = [...selectStaleTempDirs({ entries: listing.entries, now, ageThresholdMs })].sort(
      (left, right) => left.mtimeMs - right.mtimeMs,
    );
    for (const entry of selected.slice(0, candidateCap)) {
      if (clock.isExpired()) {
        markBudgetExhausted(report, entry.path);
        break;
      }

      // Symlink and plain-file candidates have no children, so the probe does
      // not apply to them.
      let probeTruncated = false;
      if (!entry.isSymlink && !entry.isFile) {
        const liveness = await probeLiveWriter(entry.path, now, clock);
        if (liveness.expired) {
          // `continue`, NOT `break` (pre-PR review, High #4). An expired
          // probe is a verdict about THIS candidate — "I could not establish
          // that nothing is writing into it, so I will not delete it" — and
          // aborting the whole loop turned that into a verdict about every
          // candidate behind it. One big tree at a tight budget then produced
          // `removed=0, budgetExhausted=true` on every beat forever, with
          // nothing removed and nothing skipped past: a second livelock of the
          // exact shape `SWEEP_REMOVAL_RESERVE_MS` was introduced to kill.
          //
          // The exhaustion is still recorded (once — `markBudgetExhausted` is
          // idempotent), so the caller still learns the beat stopped short and
          // the cooldown still refuses to stand the sweep down.
          //
          // RESIDUAL, DISCLOSED: a DIRECTORY candidate whose probe cannot
          // complete inside one budget is deferred on every beat, because the
          // only alternative is deleting a tree that may be live. What this
          // change recovers is the rest of the queue — the plain files and
          // symlinks that need no probe at all, and which the reserve above
          // deliberately leaves room for.
          markBudgetExhausted(report, entry.path);
          continue;
        }
        if (liveness.live) {
          markLiveWriterSkipped(report, entry.path);
          continue;
        }
        probeTruncated = liveness.truncated;
      }

      const outcome = await reclaimCandidate(entry, report, clock, { probeTruncated });
      if (outcome.expired) {
        markBudgetExhausted(report, entry.path);
        break;
      }
    }

    // SET HERE, AFTER THE LOOP — not eagerly from `selected.length` before it.
    // `candidatesCapped` answers "was the CAP the reason this beat stopped
    // short?", and Phase 3 uses it to decide whether to re-arm the next beat.
    // Set eagerly, a sweep that selected 24 and then died on the budget at
    // candidate 1 reported `candidatesCapped: true`, indistinguishable from one
    // that genuinely drained all 24 slots — a wrong stop reason handed to the
    // component whose whole job is reacting to the stop reason. The budget wins
    // the attribution when both are true, because the budget is what actually
    // ended the iteration.
    report.candidatesCapped = selected.length > candidateCap && !report.budgetExhausted;

    // THE ONE PLACE `completed` IS EVER SET (Phase 3 review round 2).
    // Control only reaches this line having listed the root, selected
    // candidates, and run the processing loop over them — so this, and nothing
    // else, is what "the sweep actually ran" means. It is deliberately a
    // POSITIVE flag set at the single terminal path rather than a denylist of
    // refusal kinds checked elsewhere: a refusal branch added to this function
    // tomorrow returns before this line and is correctly treated as
    // "not completed" without anyone updating a list.
    report.completed = true;

    return report;
  } catch (error) {
    // THE LAST LINE OF THE "NEVER REJECTS" CONTRACT. Every step above is
    // individually guarded, so reaching here means something guarded threw
    // anyway — a mocked fs surface throwing synchronously instead of rejecting,
    // a getter on a dirent, a future edit that forgets a guard. The cost of the
    // alternative is not a lost report: an unhandled rejection prints a raw
    // stack trace onto the stdout stream the PreToolUse hook `JSON.parse`s,
    // which fails that hook closed and denies EVERY agent spawn on the host
    //. A partial report with `kind: 'sweep-aborted'` is always the
    // better outcome, so this arm is deliberately total and deliberately last.
    recordSweepError(report, {
      path: dirLabel,
      kind: 'sweep-aborted',
      message: describeError(error, 'sweep aborted'),
    });
    return report;
  } finally {
    clock.dispose();
  }
}

/**
 * Resolves the system temp root this CLI is confined to: `TMPDIR`/`os.tmpdir()`,
 * symlink-resolved where possible (macOS's `/tmp` is itself a symlink into
 * `/private/tmp`, and `os.tmpdir()`/`TMPDIR` can disagree on the trailing
 * form) so prefix comparison in `isConfinedToTmpRoot` is reliable.
 *
 * @returns {string}
 */
function resolveTmpRoot() {
  const base = process.env.TMPDIR || tmpdir();
  try {
    return fs.realpathSync(base);
  } catch {
    return resolve(base);
  }
}

/**
 * Confinement guard for `--dir`: refuses anything outside the system temp
 * root. `cdk.out` is this repo's own real, gitignored CDK synth output
 * directory (see `infra/.gitignore` / `.gitignore`) — a mistyped or
 * misconfigured `--dir` combined with a real `infra/cdk.out/` that is
 * simply more than the age threshold old (the common case for most dev
 * workflows) must never be eligible for recursive removal. Confining `--dir`
 * to the temp root (rather than only checking existence + directory-ness)
 * makes that class of mistake structurally impossible, independent of git
 * state or working-directory assumptions.
 *
 * @param {string} candidateDir
 * @returns {boolean}
 */
function isConfinedToTmpRoot(candidateDir) {
  let resolvedCandidate;
  try {
    resolvedCandidate = fs.realpathSync(candidateDir);
  } catch {
    resolvedCandidate = resolve(candidateDir);
  }
  const tmpRoot = resolveTmpRoot();
  return resolvedCandidate === tmpRoot || resolvedCandidate.startsWith(tmpRoot + sep);
}

// ---------------------------------------------------------------------------
// parseCliArgs — pure, synchronous CLI flag parsing.
// ---------------------------------------------------------------------------

/**
 * Throws a plain `Error` (never exits/writes stderr itself — that is the
 * CLI entry block's job, matching the existing `--age-hours`/`--dir`
 * validation shape below) for any argument this parser does not recognize
 * a bare `--dir`/`--age-hours` with no `=value`, or any other
 * `--`-prefixed/unrecognized argument. Previously these were silently
 * ignored and the corresponding default was used instead — surprising for a
 * destructive, recursive cleanup sweep, where a typo'd flag (`--age-hour=5`)
 * should never be mistaken for the flag actually taking effect.
 *
 * @param {string[]} argv
 * @returns {{ ageHours: number, dir: string }}
 */
function parseCliArgs(argv) {
  const args = Array.isArray(argv) ? argv : [];
  let ageHours = DEFAULT_AGE_HOURS;
  let dir = process.env.TMPDIR || tmpdir();

  for (const arg of args) {
    if (typeof arg !== 'string') continue;
    if (arg.startsWith('--age-hours=')) {
      ageHours = Number(arg.slice('--age-hours='.length));
    } else if (arg.startsWith('--dir=')) {
      dir = arg.slice('--dir='.length);
    } else if (arg === '--age-hours' || arg === '--dir') {
      throw new Error(`cleanup: "${arg}" requires a value (e.g. "${arg}=...") — got none`);
    } else {
      throw new Error(`cleanup: unrecognized argument "${arg}"`);
    }
  }

  return { ageHours, dir };
}

// ---------------------------------------------------------------------------
// CLI entry point — only runs when this file is executed directly, matching
// `diagnostics/collect.mjs`'s own no-shebang, node-invoked, import.meta.url
// guard convention. Importing this module from a test never triggers this
// block.
// ---------------------------------------------------------------------------

if (process.argv[1] !== undefined && pathToFileURL(process.argv[1]).href === pathToFileURL(CLEANUP_SCRIPT_PATH).href) {
  // Flag parsing/recognition — rejected with the same clean stderr+exit(1)
  // shape as the `--age-hours`/`--dir` value validation below, before any
  // fs mutation is attempted.
  let ageHours;
  let dir;
  try {
    ({ ageHours, dir } = parseCliArgs(process.argv.slice(2)));
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exit(1);
  }

  // `--age-hours` validation — rejected with the same clean stderr+exit(1)
  // shape as an invalid `--dir` below, before any fs mutation is attempted.
  // Without this, a non-numeric value (`--age-hours=abc`) flows through as
  // `NaN`, which `selectStaleTempDirs` rejects by throwing — safe (nothing
  // gets deleted) but surfaces as a raw uncaught stack trace rather than a
  // clean error, since nothing here wraps the `sweepStaleTempDirs` call.
  if (!Number.isFinite(ageHours) || ageHours <= 0) {
    process.stderr.write(`cleanup: --age-hours "${ageHours}" must be a finite positive number\n`);
    process.exit(1);
  }

  // `--dir` validation — a nonexistent/mistyped `--dir`, or one outside the
  // system temp root, is rejected before any fs mutation is attempted. Full
  // shell-injection-style path validation is deliberately out of scope: this
  // CLI is invoked via `spawnSync('node', [...])` (an argv array, never a
  // shell), so classic shell-metacharacter injection does not apply to this
  // invocation shape. The temp-root confinement check (`isConfinedToTmpRoot`)
  // exists for a different reason: this sweep is destructive+recursive, and
  // `--dir` must never be able to reach a real, legitimate directory such as
  // this repo's own `cdk.out/`.
  let dirIsValid = false;
  try {
    dirIsValid = fs.existsSync(dir) && fs.statSync(dir).isDirectory() && isConfinedToTmpRoot(dir);
  } catch {
    dirIsValid = false;
  }

  if (!dirIsValid) {
    process.stderr.write(
      `cleanup: --dir "${dir}" does not exist, is not a directory, or is not confined to the system temp root (${resolveTmpRoot()}); refusing to run\n`,
    );
    process.exit(1);
  } else {
    const ageThresholdMs = ageHours * HOUR_MS;
    const report = sweepStaleTempDirs({ dir, ageThresholdMs, now: Date.now() });
    process.stdout.write(JSON.stringify(report) + '\n');
  }
}
