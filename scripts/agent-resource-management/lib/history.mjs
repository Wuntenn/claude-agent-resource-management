// Phase 1 — operation-type history store.
//
// Per Book Plan Phase 1, this module owns a small per-operation-type history
// of resource observations (peak memory/swap/disk-delta) recorded at the end
// of each sub-agent's task, so a future phase can use it to right-size
// per-operation-type expectations instead of a single flat global cap.
//
// Shape on disk (one JSON object, keyed by `operationType`):
//
//   {
//     "<operationType>": {
//       "raw": [ observation, observation, ... ],   // oldest first, bounded
//                                                     // at `retentionCap`
//       "summary": null | {
//         count: number,
//         sum: number,
//         max: number,
//         reservoir: number[],   // bounded uniform sample of folded peaks,
//                                 // used only to *estimate* p90 — never
//                                 // grows past RESERVOIR_CAPACITY
//       }
//     },
//     ...
//   }
//
// `summary` here is deliberately a richer, PRIVATE on-disk shape than the
// PUBLIC shape `readHistory` returns (`{ count, meanPeakMemoryMb,
// maxPeakMemoryMb, p90PeakMemoryMb }`) — `count`/`sum`/`max` are exact O(1)
// running stats (Welford-style accumulation, no unbounded growth), and
// `reservoir` is a fixed-capacity uniform random sample (reservoir sampling,
// Algorithm R) of every folded peak ever seen, used only to *estimate* p90 at
// read time. This keeps the whole per-type record's memory footprint bounded
// forever, no matter how many observations fold over the life of the file,
// while still producing an exact `count`/mean/`max` and a reasonable p90
// estimate (exact whenever fewer than `RESERVOIR_CAPACITY` observations have
// ever folded, which covers any realistic single-session history).
//
// Concurrent-write safety reuses `coordination-file.mjs`'s existing
// exclusive-create fencing lock (`acquireLock`/`releaseLock`) against a
// `<historyFilePath>.lock` sibling — the same single-lock-per-file critical
// section `withLock` wraps internally for the dibs coordination file. This
// module does not invent a second locking mechanism.
//
// `writeStoreAtomic` additionally re-verifies ownership
// (`lockContext.assertStillHeld()`) immediately before the final `rename`
// that publishes the staged write, throwing `LockLostError` instead of
// writing anyway. That covers the full failure taxonomy `assertStillHeld`
// (`coordination-file.mjs`) itself documents, not only a fencing-token
// mismatch: the lock file being ABSENT (treated as loss even though a
// concurrent `reclaimStaleLock` restore can produce a false refusal against
// a holder that still legitimately owns its lock) and an unparseable lock
// payload both also throw `LockLostError`; any other read failure (e.g.
// `EACCES`, `EMFILE`) propagates unchanged rather than being reported as a
// lost lock. A refusal — true or false — drops the observation entirely:
// `recordObservation` rejects, `cli.mjs`'s existing fail-open catch around
// it logs and moves on, and nothing retries. This narrows the window; it
// does not close it — the exposure shrinks from the whole critical section
// down to the single event-loop hop between the assert resolving and the
// `rename` landing, it does not eliminate that hop.

import { promises as fs } from 'node:fs';
import { appendFileSync } from 'node:fs';
import { randomBytes } from 'node:crypto';

import { acquireLock, releaseLock, assertStillHeld } from './coordination-file.mjs';

// Phase 2 — per-process, per-path memoization of the parsed store.
//
// `main()`'s new memory-projection gate (cli.mjs) may need to look up SEVERAL
// distinct agent classes' history in the course of one beat (every currently
// running class plus the candidate), and each is a separate `readHistory()`
// call. Without this cache, that would mean one genuine `fs.readFile` per
// class, needlessly re-reading the (possibly large) whole-store file N times
// for data that cannot have changed mid-beat — nothing in this file's own
// production caller (`cli.mjs`) ever WRITES to the history file from inside
// the SAME one-shot `--desired-agents` process that also reads it, so a
// process-lifetime cache introduces no staleness a single beat could ever
// observe.
//
// Invalidated on every successful write (`writeStoreAtomic` below), which is
// the only way this module's own store can change — so an in-process caller
// that legitimately writes then reads back the SAME file within one process
// (e.g. `recordObservation` followed by a direct `readHistory` call, as
// several of this directory's own unit tests do) still observes the fresh
// data, never a stale cached copy.
//
// Deliberately NOT a cross-process cache: each `node cli.mjs` invocation is a
// fresh process with an empty `Map`, so no orchestrator ever carries a stale
// view across beats, and a concurrent WRITER process's changes are visible to
// every NEW process's first read exactly as before this cache existed.
const storeReadCache = new Map();

/**
 * Test seam: when `ARM_HISTORY_READ_COUNT_FILE` is set,
 * `readStore` appends one `'1\n'` line to that path per genuine on-disk read
 * attempt of the history file — i.e. once per actual `fs.readFile` call this
 * function makes, INCLUDING an `ENOENT` (the syscall itself still happened;
 * the file merely doesn't exist yet), but NEVER for a cache hit (no syscall
 * was made). This lets a black-box test that only spawns the real CLI as a
 * child process observe how many times the history file was genuinely read
 * from disk during one beat, without needing to spy on an ESM import across a
 * process boundary.
 */
function recordHistoryReadAttempt() {
  const readCountFilePath = process.env.ARM_HISTORY_READ_COUNT_FILE;
  if (readCountFilePath) {
    // review, Medium finding — this is a test-only witness hook, not a
    // load-bearing part of a real beat's admission decision. It must never be
    // able to fail a real history read: an unwritable/undeletable path (a
    // stray or misconfigured env var reaching a real invocation) would
    // otherwise throw here and take the whole beat down with it, failing
    // closed for a reason that has nothing to do with the actual history
    // data being read. Swallow silently — same "a test seam must be inert by
    // construction outside its own test" posture `ARM_EVICT_TEST_MODE`
    // documents elsewhere in this directory.
    try {
      appendFileSync(readCountFilePath, '1\n');
    } catch {
      // Intentionally ignored — see comment above.
    }
  }
}

// Illustrative starting default, not a settled constant. The unit tests in
// this suite pass `options.retentionCap` explicitly (so a fold can be driven
// in a handful of writes); the production caller — `cli.mjs`'s
// `handleRecordOutcome` — deliberately does NOT, and therefore runs on this
// default. Kept generous so an operation type with a modest observation count
// never folds prematurely.
const DEFAULT_RETENTION_CAP = 50;

// Matches `coordination-file.mjs`'s own `DEFAULT_LOCK_STALENESS_MS` — an
// orphaned `.lock` file (writer SIGKILL'd/OOM-killed mid-critical-section)
// is reclaimed after this long rather than wedging every future caller.
const DEFAULT_LOCK_STALENESS_MS = 30_000;

// Bounds the `reservoir` field's size forever, regardless of how many
// observations fold into a given operation type's summary over the life of
// the history file. Comfortably above any realistic single-session fold
// count, so p90 is exact in practice while still being a hard, documented
// memory ceiling rather than an unbounded array.
const RESERVOIR_CAPACITY = 500;

/**
 * @param {unknown} value
 * @returns {value is number}
 */
function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

/**
 * Reads the whole history store (an object keyed by `operationType`),
 * tolerating a not-yet-created file (returns `{}` rather than throwing).
 *
 * The returned object is the SAME reference cached in `storeReadCache` — do
 * not mutate it in place. `recordObservation` is the one exception: it owns
 * the cache-invalidate-on-write contract (mutates, then immediately calls
 * `writeStoreAtomic`, which invalidates the cache for this path before any
 * other caller in this process can observe the pre-durable-write mutation).
 * Any other caller must copy before mutating.
 *
 * @param {string} historyFilePath
 * @returns {Promise<Record<string, { raw: object[], summary: object | null }>>}
 */
async function readStore(historyFilePath) {
  if (storeReadCache.has(historyFilePath)) {
    return storeReadCache.get(historyFilePath);
  }

  let raw;
  try {
    raw = await fs.readFile(historyFilePath, 'utf8');
    recordHistoryReadAttempt();
  } catch (error) {
    recordHistoryReadAttempt();
    if (error.code === 'ENOENT') {
      storeReadCache.set(historyFilePath, {});
      return {};
    }
    throw error;
  }

  if (!raw) {
    storeReadCache.set(historyFilePath, {});
    return {};
  }

  const parsed = JSON.parse(raw);
  const store = parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  storeReadCache.set(historyFilePath, store);
  return store;
}

/**
 * Writes the whole history store atomically: serialise to a
 * `<path>.tmp-<random>` sibling, then `rename()` onto `historyFilePath`, so a
 * concurrent reader only ever observes the whole prior file or the whole new
 * one — matching `coordination-file.mjs`'s `writeEntriesAtomic` convention.
 *
 * THE OWNERSHIP CHOKE POINT (mirroring `writeEntriesAtomic`'s
 * fix exactly). `lockContext` — the `{ lockPath, token, assertStillHeld }`
 * shape `recordObservation` builds inline the same way `withLock` builds it
 * for `coordination-file.mjs` — is mandatory, and this function re-proves
 * ownership through it immediately before the `rename`. A caller whose
 * critical section outlived `lockStalenessMs` can have its lock legitimately
 * reclaimed by a contender; without this recheck it would still `rename()`
 * its stale write over whatever the reclaiming holder already wrote,
 * resolving successfully while its observation silently vanished from disk.
 *
 * Ordering is deliberate, matching `writeEntriesAtomic`: stage the `.tmp-*`
 * file FIRST, then assert, then `rename`. Asserting before staging would
 * widen the residual race window to include the whole serialise-and-write,
 * for no benefit.
 *
 * On any failure — the staged write, the ownership check, or the `rename`
 * itself — the `.tmp-*` sibling is removed (best-effort; a failure of the
 * cleanup itself is swallowed so it can never mask the original error)
 * before the error propagates unmasked.
 *
 * A MISSING `lockContext` (or one whose `assertStillHeld` isn't callable) is
 * a programming error, not a runtime condition, so it throws a `TypeError`
 * (rather than a `LockLostError`) with a greppable message — before any
 * `fs.writeFile` — rather than quietly writing unfenced.
 *
 * Exported (alongside `recordObservation`/`readHistory`) solely so the test
 * suite can drive the refusal path and the staging-order guarantee directly
 * — not part of the documented production API.
 *
 * @param {string} historyFilePath
 * @param {object} store
 * @param {{ assertStillHeld: () => Promise<void> }} lockContext
 * @returns {Promise<void>}
 */
export async function writeStoreAtomic(historyFilePath, store, lockContext) {
  if (typeof lockContext?.assertStillHeld !== 'function') {
    throw new TypeError(
      'writeStoreAtomic: a lockContext with an assertStillHeld() function is required — ' +
        'every write to the history file must re-prove, immediately before its rename, that this ' +
        "caller still holds the fencing lock. Call this from recordObservation's own " +
        'lock-holding critical section and pass the lockContext it built.',
    );
  }

  const tmpPath = `${historyFilePath}.tmp-${randomBytes(8).toString('hex')}`;
  try {
    await fs.writeFile(tmpPath, JSON.stringify(store), 'utf8');
    await lockContext.assertStillHeld();
    await fs.rename(tmpPath, historyFilePath);
    // Phase 2 — invalidate this path's cached store (see
    // `storeReadCache`'s own doc comment above): the file on disk has just
    // genuinely changed, so the next `readStore` call for this path must
    // re-read rather than serve a copy that predates this write.
    storeReadCache.delete(historyFilePath);
  } catch (error) {
    await fs.unlink(tmpPath).catch(() => {});
    throw error;
  }
}

/**
 * Uniform reservoir sampling (Algorithm R): inserts `value` into a
 * fixed-capacity sample of everything seen so far (`countSoFar` includes this
 * value). Below capacity, every value is kept. At/above capacity, `value`
 * replaces a uniformly-random existing slot with probability
 * `RESERVOIR_CAPACITY / countSoFar`, keeping the sample a uniform random
 * subset of the full folded history without ever growing past capacity.
 * Never mutates the input array.
 *
 * @param {number[]} reservoir
 * @param {number} value
 * @param {number} countSoFar
 * @returns {number[]}
 */
function reservoirInsert(reservoir, value, countSoFar) {
  if (reservoir.length < RESERVOIR_CAPACITY) {
    return [...reservoir, value];
  }
  const replaceIndex = Math.floor(Math.random() * countSoFar);
  if (replaceIndex < RESERVOIR_CAPACITY) {
    const next = [...reservoir];
    next[replaceIndex] = value;
    return next;
  }
  return reservoir;
}

/**
 * Folds one more evicted-from-raw peak-memory value into the running summary
 * state, returning a NEW state (never mutates `previous`). `count`/`sum`/
 * `max` are exact O(1) running accumulators; `reservoir` is updated via
 * `reservoirInsert` so it never grows past `RESERVOIR_CAPACITY`.
 *
 * @param {{ count: number, sum: number, max: number, reservoir: number[] } | null} previous
 * @param {number} peakMemoryMb
 * @returns {{ count: number, sum: number, max: number, reservoir: number[] }}
 */
function foldIntoSummary(previous, peakMemoryMb) {
  const state = previous ?? { count: 0, sum: 0, max: -Infinity, reservoir: [] };
  const count = state.count + 1;
  return {
    count,
    sum: state.sum + peakMemoryMb,
    max: Math.max(state.max, peakMemoryMb),
    reservoir: reservoirInsert(state.reservoir, peakMemoryMb, count),
  };
}

/**
 * Linear-interpolation percentile (the common "numpy default" convention)
 * over a copy of `values`, sorted ascending. `p` is a fraction in `[0, 1]`.
 * A single-element array returns that element outright (nothing to
 * interpolate between).
 *
 * @param {number[]} values
 * @param {number} p
 * @returns {number} `NaN` for an empty array.
 */
function computePercentile(values, p) {
  if (values.length === 0) return NaN;
  const sorted = [...values].sort((a, b) => a - b);
  if (sorted.length === 1) return sorted[0];

  const index = p * (sorted.length - 1);
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sorted[lower];

  const fraction = index - lower;
  return sorted[lower] + fraction * (sorted[upper] - sorted[lower]);
}

/**
 * Maps the private on-disk summary accumulator to the public, fixed-shape
 * summary `readHistory` returns — exactly the four documented fields, never
 * more, regardless of how many folds have contributed to it.
 *
 * @param {{ count: number, sum: number, max: number, reservoir: number[] }} state
 * @returns {{ count: number, meanPeakMemoryMb: number, maxPeakMemoryMb: number, p90PeakMemoryMb: number }}
 */
function toPublicSummary(state) {
  return {
    count: state.count,
    meanPeakMemoryMb: state.sum / state.count,
    maxPeakMemoryMb: state.max,
    p90PeakMemoryMb: computePercentile(state.reservoir, 0.9),
  };
}

/**
 * Appends `observation` to the raw log for `observation.operationType`,
 * folding the oldest raw entry into that operation type's rolling summary
 * once the log would exceed `options.retentionCap`. The raw log therefore
 * never grows past the cap; overflow is folded, never silently dropped.
 *
 * The whole read-modify-write cycle for `historyFilePath` is serialised via
 * `coordination-file.mjs`'s exclusive-create fencing lock (a
 * `<historyFilePath>.lock` sibling). That serialises concurrent callers, but
 * — per the standard already established for `coordination-file.mjs`'s
 * `writeEntriesAtomic` — a lock alone does not CLOSE the race: a caller whose
 * critical section outlives `lockStalenessMs` can have its lock legitimately
 * reclaimed by a contender (`reclaimStaleLock` cannot distinguish "stalled
 * but alive" from "SIGKILLed"). `writeStoreAtomic` re-proves ownership
 * (`lockContext.assertStillHeld()`) immediately before its `rename()`, which
 * NARROWS the window in which a stale write could clobber a reclaiming
 * holder down to the gap between that assert resolving and the rename being
 * issued — it does not close it to zero. A caller that loses the race
 * rejects with `LockLostError` instead of silently dropping its observation
 *. That rejection is not limited to a fencing-token mismatch: per
 * `assertStillHeld`'s own documented taxonomy (`coordination-file.mjs`), the
 * lock file being absent (treated as loss even though a concurrent
 * `reclaimStaleLock` restore window can produce a false refusal against a
 * still-legitimate holder) and an unparseable lock payload both also throw
 * `LockLostError`; any other read failure propagates unchanged rather than
 * being reported as a lost lock. Either way — a true reclaim or a false
 * refusal — the observation is dropped entirely: `cli.mjs`'s existing
 * fail-open catch around `recordObservation` logs the rejection and moves
 * on, and nothing retries.
 *
 * @param {string} historyFilePath
 * @param {{
 *   operationType: string,
 *   orchestratorId: string,
 *   startedAt: number,
 *   endedAt: number,
 *   peakMemoryMb?: number,
 *   peakSmoothedFootprintMb?: number,
 *   peakMemorySource?: 'phys-footprint-ewma' | 'caller-supplied',
 *   peakSwapMb?: number,
 *   freeDiskDeltaGb?: number,
 *   crashed?: boolean,
 *   agentClass?: string,
 *   footprintTrajectory?: number[],
 *   plateauDeltaMb?: number,
 * }} observation Additive optional fields: `agentClass` (the
 *   spawning agent's label, e.g. `typescript-implementer`) and
 *   `footprintTrajectory` (the bounded EWMA-smoothed footprint series
 *   `--poll-footprint` accumulated for this run, see
 *   `./footprint-trajectory.mjs`'s `TRAJECTORY_CAP`). This function already
 *   persists whatever shape it is given via `{ ...observation }` — these two
 *   fields need no code change here, only this documented contract addition.
 *
 *   `plateauDeltaMb` (Phase 4, `idle-probe-trajectory.mjs`'s
 *   `'new-plateau'` outcome): the empirical footprint delta (final minus
 *   baseline `--poll-footprint` reading) an idle-probe observed over its
 *   window — recorded alongside `peakSmoothedFootprintMb` rather than
 *   silently discarded. Same "no code change needed here" additive shape as
 *   the two fields above.
 *
 *   `peakMemoryMb` vs. `peakSmoothedFootprintMb` (whole-branch review,
 *   Medium finding — the exact metric-disagreement class that caused the
 * RSS-inversion incident): these are TWO DIFFERENT METRICS with
 *   different provenance, never conflated into the same field.
 *   `peakMemoryMb` is populated ONLY from the legacy `--peak-memory-mb` CLI
 *   flag — a caller-supplied number, historically `ps`-RSS-shaped, and
 *   `estimateOperationCost`'s median/percentile logic reads this field
 *   exclusively. `peakSmoothedFootprintMb` is populated ONLY from
 *   `--poll-footprint`'s accumulated EWMA-smoothed `phys_footprint` peak
 *   (`lib/footprint-state.mjs`'s `peakSmoothedMb`) — `handleRecordOutcome`
 *   (`cli.mjs`) never writes a polling-derived value into `peakMemoryMb`.
 *   `peakMemorySource` is set to `'phys-footprint-ewma'` whenever
 *   `peakSmoothedFootprintMb` is populated from polling; the
 *   `'caller-supplied'` literal exists as a forward-contract slot for a
 *   future consumer that wants to label `peakMemoryMb`'s own provenance, but
 *   is not emitted by any code path today (this fix does not populate
 *   `peakMemoryMb` from polling data at all, so there is nothing there to
 *   label yet).
 * @param {{ retentionCap?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<void>}
 */
export async function recordObservation(historyFilePath, observation, options = {}) {
  const retentionCap = isFiniteNumber(options.retentionCap) ? options.retentionCap : DEFAULT_RETENTION_CAP;
  const lockStalenessMs = isFiniteNumber(options.lockStalenessMs)
    ? options.lockStalenessMs
    : DEFAULT_LOCK_STALENESS_MS;
  const lockPath = `${historyFilePath}.lock`;

  const token = await acquireLock(lockPath, lockStalenessMs, options.lockMaxAttempts, options.lockRetryDelayMs);
  const lockContext = {
    lockPath,
    token,
    assertStillHeld: () => assertStillHeld(lockPath, token),
  };
  let fnThrew = false;
  try {
    const store = await readStore(historyFilePath);
    const typeEntry = store[observation.operationType] ?? { raw: [], summary: null };

    const nextRaw = [...typeEntry.raw, { ...observation }];

    let raw = nextRaw;
    let summary = typeEntry.summary;
    if (nextRaw.length > retentionCap) {
      const [oldest, ...rest] = nextRaw;
      raw = rest;
      // Only CLEAN (non-crashed) observations carrying a genuinely finite
      // `peakMemoryMb` are folded into the rolling summary (pre-PR
      // review, High x2). Two distinct defects this closes:
      //
      //   1. `crashed: true` observations were previously folded in exactly
      //      like clean ones. `estimateOperationCost` (./cost-estimate.mjs)
      //      excludes crashed entries from its RAW median, and SKILL.md
      //      states outright that "crashed: true observations are excluded
      //      from the central estimate entirely" — but once a crashed entry
      //      aged past `retentionCap` it folded into `summary`, and the
      //      summary-derived p90 then drove the estimate with no way to tell
      //      it was crash-contaminated. A crashed run's peak is precisely the
      //      pathological number that must never set future capacity
      //      expectations.
      //   2. A missing/non-finite `peakMemoryMb` (entirely legal —
      //      `--peak-memory-mb` is an OPTIONAL flag on `--record-outcome`)
      //      folded `undefined` into `sum`/`max`/`reservoir`, poisoning the
      //      whole accumulator to `NaN` permanently.
      //
      // A skipped observation is still evicted from `raw` (the cap is a hard
      // bound either way) — it simply contributes no signal to the summary,
      // which is the honest outcome for an observation that carries none.
      if (oldest.crashed !== true && isFiniteNumber(oldest.peakMemoryMb)) {
        summary = foldIntoSummary(summary, oldest.peakMemoryMb);
      }
    }

    store[observation.operationType] = { raw, summary };
    try {
      await writeStoreAtomic(historyFilePath, store, lockContext);
    } catch (writeError) {
      // Phase 2 — cache-poisoning fix. `store` above is the SAME
      // object reference `readStore` cached in `storeReadCache` (see that
      // function's own doc comment: "do not mutate it in place... the one
      // exception [is `recordObservation`]"). The line right above this
      // mutates that shared cached object with an UNCOMMITTED write BEFORE
      // `writeStoreAtomic` has actually landed it on disk. On success,
      // `writeStoreAtomic` invalidates the cache itself, so the next read
      // re-reads the (now-matching) file from disk. But on ANY write
      // failure — most notably `LockLostError` from the ownership recheck
      // — `writeStoreAtomic` never reaches that invalidation, so the cache
      // is left holding this mutated-but-never-persisted object. A later
      // retry (or any other in-process reader) would then read back an
      // observation that was never actually written, silently resurrecting
      // dropped data and corrupting subsequent folds. Deleting the cache
      // entry here forces the next read to go back to disk and see the
      // real, still-unwritten state.
      storeReadCache.delete(historyFilePath);
      throw writeError;
    }
  } catch (error) {
    // A flag, not `primaryError !== undefined`: the try block is free to
    // throw a falsy value, and a truthiness test would let the release error
    // mask it. Mirrors `coordination-file.mjs`'s `withLock` exactly
    // (per review) — see that function's `finally` block for the full
    // rationale (releaseLock is always the standard path here, not the rare
    // one, because a genuine LockLostError always leaves the token
    // mismatched).
    fnThrew = true;
    throw error;
  } finally {
    try {
      await releaseLock(lockPath, token);
    } catch (releaseError) {
      // A release failure must never replace what the try block threw —
      // that error is the diagnosis (e.g. the LockLostError this recheck
      // exists to surface); this one is cleanup noise. With nothing to
      // mask, it propagates normally.
      if (!fnThrew) throw releaseError;
    }
  }
}

/**
 * Reads the raw + summary history for `operationType`, returning an explicit
 * `{ raw: [], summary: null }` shape — never `undefined` — for a type with
 * zero observations, including when the history file hasn't been created at
 * all yet.
 *
 * @param {string} historyFilePath
 * @param {string} operationType
 * @returns {Promise<{
 *   raw: Array<{ operationType: string, orchestratorId: string, startedAt: number, endedAt: number, peakMemoryMb?: number, peakSmoothedFootprintMb?: number, peakMemorySource?: 'phys-footprint-ewma' | 'caller-supplied', peakSwapMb?: number, freeDiskDeltaGb?: number, crashed?: boolean, agentClass?: string, footprintTrajectory?: number[] }>,
 *   summary: { count: number, meanPeakMemoryMb: number, maxPeakMemoryMb: number, p90PeakMemoryMb: number } | null,
 * }>}
 */
export async function readHistory(historyFilePath, operationType) {
  const store = await readStore(historyFilePath);
  const entry = store[operationType];
  if (!entry) return { raw: [], summary: null };

  return {
    raw: entry.raw ?? [],
    summary: entry.summary ? toPublicSummary(entry.summary) : null,
  };
}
