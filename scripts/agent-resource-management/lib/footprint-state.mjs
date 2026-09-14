// Phase 3 — per-PID footprint-polling persisted state.
//
// `cli.mjs`'s `--poll-footprint` beat is a one-shot process (a fresh `node
// cli.mjs` invocation per poll, exactly like `--heartbeat`/`--record-outcome`
// elsewhere in this skill) — there is no in-process home for Phase 2's
// `createFootprintTrajectory()` accumulator to live across separate polls of
// the SAME pid. This module is that home, persisted to disk: a small JSON
// store, keyed by pid, holding exactly the state one more `ewmaSmooth()` step
// needs (`smoothed`, `peakSmoothedMb`, `trajectory`) plus `firstPolledAt` —
// the timestamp of the FIRST poll ever recorded for that pid, which
// `--record-outcome` later threads through as the run's real `startedAt`
// (see cli.mjs's `handleRecordOutcome`).
//
// Concurrent-write safety reuses `coordination-file.mjs`'s existing
// exclusive-create fencing lock (`acquireLock`/`releaseLock`) against a
// `<stateFilePath>.lock` sibling — the exact same pattern `lib/history.mjs`'s
// `recordObservation` already uses for its own store. This module does not
// invent a second locking mechanism.

import { promises as fs } from 'node:fs';
import { randomBytes } from 'node:crypto';

import { acquireLock, releaseLock, assertStillHeld } from './coordination-file.mjs';
import { ewmaSmooth, TRAJECTORY_CAP, DEFAULT_EWMA_ALPHA } from './footprint-trajectory.mjs';

// Matches `coordination-file.mjs`'s/`history.mjs`'s own
// `DEFAULT_LOCK_STALENESS_MS` — an orphaned `.lock` file (writer
// SIGKILL'd/OOM-killed mid-critical-section) is reclaimed after this long
// rather than wedging every future poll of any pid.
const DEFAULT_LOCK_STALENESS_MS = 30_000;

// whole-branch review, Medium finding: this module used to declare its
// own `DEFAULT_ALPHA = 0.3` independently of Phase 2's
// `createFootprintTrajectory` accumulator — two copies of the same constant
// that could silently diverge. Both now derive from the ONE exported default
// in `footprint-trajectory.mjs`.
//
// NOTE on the deeper unification this review also raised: `recordFootprintPoll`
// still reimplements the accumulator's fold step inline (`ewmaSmooth` +
// running-peak + capped trajectory) rather than hydrating a
// `createFootprintTrajectory()` instance from the persisted `{ smoothed,
// peakSmoothedMb, trajectory }` shape and calling its `addSample` once. That
// refactor would require `createFootprintTrajectory` to expose a
// deserialize/rehydrate entry point it doesn't have today, and touches the
// one code path every `--poll-footprint` invocation runs through — riskier
// than the minimal constant-dedup done here. Left as a documented follow-up
// rather than bundled into this fix; the two fold implementations are now at
// least guaranteed to start from the same alpha.
const DEFAULT_ALPHA = DEFAULT_EWMA_ALPHA;

// whole-branch review, Medium finding ("PID reuse silently corrupts
// data"): macOS recycles PIDs. Without a staleness guard, a run that never
// reaches a successful `--record-outcome --pid=<pid>` (killed/OOM'd agent,
// orchestrator that never adopts `--pid`, a crash) leaves its entry in this
// store forever; an unrelated LATER process reusing that pid would then fold
// into the stale entry, corrupting `startedAt`/peak/trajectory.
//
// round-3 review, Medium finding ("threshold too tight"): this used to
// be 3x `MAX_FOOTPRINT_POLL_INTERVAL_MS` (90s) — far shorter than this
// codebase's own settled notion of "how long before we stop trusting a
// beat-driven entry is still live", `cli.mjs`'s `DEFAULT_LIVENESS_THRESHOLD_MS`
// (15 minutes, chosen because orchestrators call this CLI "after each
// sub-agent's task completes", not on a fixed short timer — a beat can
// legitimately be minutes apart: a sub-agent task in progress, a `hold`
// state, a paused agent). A 90s footprint-poll threshold would false-fire on
// exactly those legitimate long gaps, silently resetting `firstPolledAt` and
// recording WRONG (not absent) data for the pressure-affected runs this
// feature exists to measure. This constant now matches
// `DEFAULT_LIVENESS_THRESHOLD_MS`'s value directly — a footprint-poll entry
// and a dibs-liveness entry are both "is this beat-driven record still
// trustworthy" checks, and there's no documented reason for footprint polling
// to need a tighter tolerance than liveness already assumes is safe.
// `cli.mjs` passes this explicitly (derived from its own
// `DEFAULT_LIVENESS_THRESHOLD_MS`) rather than this module importing from
// `cli.mjs`, which would invert the lib/cli.mjs dependency direction every
// other module in this skill maintains; this constant is this module's own
// fallback for callers (including tests) that don't pass `staleAfterMs`
// explicitly.
const DEFAULT_STALE_AFTER_MS = 15 * 60 * 1000;

/**
 * @typedef {{
 *   firstPolledAt: number,
 *   lastPolledAt: number,
 *   smoothed: number | null,
 *   peakSmoothedMb: number | null,
 *   trajectory: number[],
 *   agentClass?: string,
 * }} FootprintStateEntry
 */

/**
 * @param {string} stateFilePath
 * @returns {Promise<Record<string, FootprintStateEntry>>}
 */
async function readStore(stateFilePath) {
  let raw;
  try {
    raw = await fs.readFile(stateFilePath, 'utf8');
  } catch (error) {
    if (error.code === 'ENOENT') return {};
    throw error;
  }

  if (!raw) return {};

  const parsed = JSON.parse(raw);
  return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
}

/**
 * Builds the small `{ lockPath, token, assertStillHeld }` shape
 * `writeStoreAtomic` requires as its `lockContext` — a local helper so this
 * module's three write call sites (`recordFootprintPoll`,
 * `consumeFootprintState`, `clearFootprintState`) build it once each rather
 * than triplicating the inline object literal `history.mjs`'s
 * `recordObservation` builds for itself (Phase 1, Convergence Analysis
 * recommendation).
 *
 * @param {string} lockPath
 * @param {string} token
 * @returns {{ lockPath: string, token: string, assertStillHeld: () => Promise<void> }}
 */
function buildLockContext(lockPath, token) {
  return {
    lockPath,
    token,
    assertStillHeld: () => assertStillHeld(lockPath, token),
  };
}

/**
 * Writes the whole footprint-state store atomically: serialise to a
 * `<path>.tmp-<random>` sibling, then `rename()` onto `stateFilePath` — the
 * same convention `history.mjs`'s `writeStoreAtomic` uses.
 *
 * THE OWNERSHIP CHOKE POINT (Phase 1, mirroring `history.mjs`'s
 * fix exactly). `lockContext` is mandatory, and this function re-proves
 * ownership through it immediately before the `rename`. A caller whose
 * critical section outlived `lockStalenessMs` can have its lock legitimately
 * reclaimed by a contender; without this recheck it would still `rename()`
 * its stale write over whatever the reclaiming holder already wrote,
 * resolving successfully while its update silently vanished from disk.
 *
 * Ordering is deliberate, matching `history.mjs`: stage the `.tmp-*` file
 * FIRST, then assert, then `rename`. Asserting before staging would widen
 * the residual race window to include the whole serialise-and-write, for no
 * benefit.
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
 * Exported (alongside the other public functions in this module) solely so
 * the test suite can drive the refusal path and the staging-order guarantee
 * directly — not part of the documented production API.
 *
 * @param {string} stateFilePath
 * @param {object} store
 * @param {{ assertStillHeld: () => Promise<void> }} lockContext
 * @returns {Promise<void>}
 */
export async function writeStoreAtomic(stateFilePath, store, lockContext) {
  if (typeof lockContext?.assertStillHeld !== 'function') {
    throw new TypeError(
      'writeStoreAtomic: a lockContext with an assertStillHeld() function is required — ' +
        'every write to the footprint-state file must re-prove, immediately before its rename, ' +
        'that this caller still holds the fencing lock (mirroring history.mjs\'s fix). ' +
        "Call this from the holding critical section and pass the lockContext it built.",
    );
  }

  const tmpPath = `${stateFilePath}.tmp-${randomBytes(8).toString('hex')}`;
  try {
    await fs.writeFile(tmpPath, JSON.stringify(store), 'utf8');
    await lockContext.assertStillHeld();
    await fs.rename(tmpPath, stateFilePath);
  } catch (error) {
    await fs.unlink(tmpPath).catch(() => {});
    throw error;
  }
}

/**
 * Prunes every OTHER entry in `store` (i.e. not `excludeKey`, which the
 * caller is about to overwrite anyway) whose `lastPolledAt` is older than
 * `now - staleAfterMs` — opportunistic, defense-in-depth cleanup for the "a
 * run's pid never reaches `--record-outcome`, so its entry never gets
 * evicted via `consumeFootprintState`" leak case (whole-branch review,
 * Medium finding). Runs while `recordFootprintPoll`'s write lock is already
 * held, so it costs nothing beyond an in-memory scan over however many pids
 * currently have entries — no extra lock acquisition. Entries with no
 * `lastPolledAt` at all (written by a pre-this-fix version of this module)
 * are left alone rather than guessed at.
 *
 * @param {Record<string, FootprintStateEntry>} store
 * @param {string} excludeKey
 * @param {number} now
 * @param {number} staleAfterMs
 * @returns {{ store: Record<string, FootprintStateEntry>, pruned: boolean }}
 */
function pruneStaleEntries(store, excludeKey, now, staleAfterMs) {
  let pruned = false;
  const next = {};
  for (const [key, entry] of Object.entries(store)) {
    if (key !== excludeKey && typeof entry?.lastPolledAt === 'number' && now - entry.lastPolledAt > staleAfterMs) {
      pruned = true;
      continue;
    }
    next[key] = entry;
  }
  return { store: next, pruned };
}

/**
 * Folds one more raw footprint sample (in MB, or `null` for a failed poll —
 * mirroring `sampleFootprint`'s own `null`-on-failure contract) into the
 * persisted per-pid smoothing/trajectory state, creating the entry on its
 * first poll. A `null` sample is skipped without corrupting state — same
 * "skip nulls" behaviour as `createFootprintTrajectory`'s `addSample` — but
 * `firstPolledAt`/`lastPolledAt` are still seeded/refreshed on every
 * invocation regardless of whether the sample itself was usable, since a
 * poll genuinely happened at `now` either way.
 *
 * `agentClass` is last-writer-wins across repeated polls of the same pid: a
 * later poll's `--agent-class` value (if provided) overwrites the stored
 * one; a later poll that OMITS `--agent-class` keeps whatever was
 * previously stored rather than clobbering it with `undefined`
 * (whole-branch review, High finding — `agentClass` was parsed by
 * `--poll-footprint` but never threaded into this persisted entry at all).
 *
 * PID-reuse staleness guard (whole-branch review, Medium finding):
 * macOS recycles pids, and a run that never reaches a successful
 * `--record-outcome --pid=<pid>` (killed/OOM'd agent, an orchestrator that
 * never adopts `--pid`, a crash) leaves its entry in this store forever.
 * If `existing.lastPolledAt` is older than `staleAfterMs` (default
 * `DEFAULT_STALE_AFTER_MS`, matching `cli.mjs`'s own
 * `DEFAULT_LIVENESS_THRESHOLD_MS` — 15 minutes, reused from the existing
 * dibs-liveness concept rather than an independently-invented value; see
 * this constant's own definition above for why), this poll is treated as
 * the start of a FRESH run for this pid: `firstPolledAt`
 * resets to `now`, `smoothed`/`peakSmoothedMb`/`trajectory`/`agentClass`
 * reset to their cold-start values, rather than folding this sample into
 * data that almost certainly belongs to a different, earlier process that
 * happened to reuse the same pid. While the write lock is already held,
 * this also opportunistically prunes any OTHER entry in the store that is
 * similarly stale (defense-in-depth against the same leak, for pids this
 * invocation isn't even polling).
 *
 * The whole read-modify-write cycle is serialised via
 * `coordination-file.mjs`'s exclusive-create fencing lock (a
 * `<stateFilePath>.lock` sibling), so two concurrent pollers — even polling
 * DIFFERENT pids, even across independent processes — never lose an update
 * to a torn write.
 *
 * @param {string} stateFilePath
 * @param {number} pid
 * @param {number | null} sampleMb
 * @param {number} now
 * @param {{ alpha?: number, agentClass?: string, staleAfterMs?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<FootprintStateEntry>} the pid's state AFTER this poll folded in.
 */
export async function recordFootprintPoll(stateFilePath, pid, sampleMb, now, options = {}) {
  const alpha = typeof options.alpha === 'number' ? options.alpha : DEFAULT_ALPHA;
  const staleAfterMs = typeof options.staleAfterMs === 'number' ? options.staleAfterMs : DEFAULT_STALE_AFTER_MS;
  const lockStalenessMs =
    typeof options.lockStalenessMs === 'number' ? options.lockStalenessMs : DEFAULT_LOCK_STALENESS_MS;
  const lockPath = `${stateFilePath}.lock`;
  const key = String(pid);

  const token = await acquireLock(lockPath, lockStalenessMs, options.lockMaxAttempts, options.lockRetryDelayMs);
  const lockContext = buildLockContext(lockPath, token);
  let fnThrew = false;
  try {
    const rawStore = await readStore(stateFilePath);
    const { store } = pruneStaleEntries(rawStore, key, now, staleAfterMs);

    const coldStart = { firstPolledAt: now, lastPolledAt: now, smoothed: null, peakSmoothedMb: null, trajectory: [] };
    const storedEntry = store[key];
    const isStale =
      storedEntry !== undefined &&
      typeof storedEntry.lastPolledAt === 'number' &&
      now - storedEntry.lastPolledAt > staleAfterMs;

    const existing = storedEntry === undefined || isStale ? coldStart : storedEntry;

    let { smoothed, peakSmoothedMb, trajectory } = existing;
    if (sampleMb !== null && sampleMb !== undefined) {
      smoothed = ewmaSmooth(smoothed, sampleMb, alpha);
      peakSmoothedMb = peakSmoothedMb === null ? smoothed : Math.max(peakSmoothedMb, smoothed);
      trajectory = [...trajectory, smoothed];
      if (trajectory.length > TRAJECTORY_CAP) {
        trajectory = trajectory.slice(trajectory.length - TRAJECTORY_CAP);
      }
    }

    const nextAgentClass = typeof options.agentClass === 'string' ? options.agentClass : existing.agentClass;

    const nextEntry = {
      firstPolledAt: existing.firstPolledAt,
      lastPolledAt: now,
      smoothed,
      peakSmoothedMb,
      trajectory,
      ...(nextAgentClass !== undefined ? { agentClass: nextAgentClass } : {}),
    };
    store[key] = nextEntry;
    await writeStoreAtomic(stateFilePath, store, lockContext);
    return nextEntry;
  } catch (error) {
    // A flag, not `primaryError !== undefined`: the try block is free to
    // throw a falsy value, and a truthiness test would let the release error
    // mask it. Mirrors `history.mjs`'s `recordObservation` exactly
    // (Phase 1) — see that function's `finally` block for the full rationale.
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
 * Reads the persisted poll state for a single pid, tolerating a not-yet-
 * created store (returns `null`, same as a pid with zero polls) rather than
 * throwing.
 *
 * @param {string} stateFilePath
 * @param {number} pid
 * @returns {Promise<FootprintStateEntry | null>}
 */
export async function readFootprintState(stateFilePath, pid) {
  const store = await readStore(stateFilePath);
  return store[String(pid)] ?? null;
}

/**
 * Reads the WHOLE persisted poll-state store once, tolerating a not-yet-
 * created store (returns `{}`, same as `readStore`'s own contract) rather
 * than throwing. Exists so a caller resolving MANY pids in one pass (e.g.
 * `cli.mjs`'s `buildWatchdogCandidates`, walking every candidate the
 * watchdog beat owns) can read/parse the store file ONCE and look each pid
 * up from the already-parsed object, instead of calling `readFootprintState`
 * per pid — which re-reads and re-parses the entire file on every call
 * (review, Medium finding — N full-file reads where one would do).
 *
 * @param {string} stateFilePath
 * @returns {Promise<Record<string, FootprintStateEntry>>}
 */
export async function readFootprintStore(stateFilePath) {
  return readStore(stateFilePath);
}

/**
 * Atomically reads AND evicts a single pid's persisted poll state under ONE
 * lock acquisition — `cli.mjs`'s `handleRecordOutcome` is this function's
 * only caller, and needs the read (to thread the real `startedAt`/peak/
 * trajectory into the observation it's about to persist) and the delete (so
 * the store doesn't grow unboundedly across pid churn — see
 * `clearFootprintState`'s own doc comment) to happen as a single transaction.
 *
 * Phase 3 review (Medium) — before this function existed,
 * `handleRecordOutcome` called `readFootprintState` then, separately,
 * `clearFootprintState`, each acquiring and releasing the lock independently.
 * A concurrent `--poll-footprint` for the SAME pid landing in the unlocked
 * gap between those two calls would write a newer sample that the subsequent
 * `clearFootprintState` then silently deleted before it was ever recorded —
 * a real data-loss race, since this feature polls every 10-30s per pid while
 * `--record-outcome` can land at roughly the same time. Folding read+delete
 * into one lock acquisition closes that gap: no other process can observe or
 * mutate this pid's entry between the read and the delete.
 *
 * Tolerates a not-yet-created store, or a pid with no entry, returning
 * `null` (not an error) — mirroring `readFootprintState`'s own
 * tolerant-of-absence contract. In that case the store is left untouched
 * (no write is issued) since there is nothing to evict.
 *
 * round-3 review, Medium finding ("asymmetric staleness guard"):
 * `recordFootprintPoll`'s WRITE path has always guarded against PID reuse
 * (see that function's own doc comment), but this READ path did not — a run
 * that calls `--record-outcome --pid=<pid>` without ever having freshly
 * polled that exact pid (legal: `--pid` is optional/additive) would
 * otherwise harvest a PREVIOUS, unrelated process's `firstPolledAt`/peak/
 * trajectory/`agentClass` after a macOS pid recycle — the exact corruption
 * class the write-side guard exists to prevent, just on the read side
 * instead. When the caller passes `staleAfterMs` (and `now`), an entry whose
 * `lastPolledAt` is older than `staleAfterMs` is treated as if it did not
 * exist: this function returns `null` instead of the stale entry. The entry
 * is STILL evicted from the store either way — it is being consumed
 * regardless of whether the caller trusts its contents, and leaving a stale
 * entry behind would just recreate the unbounded-growth problem this
 * function's eviction half already exists to solve.
 *
 * @param {string} stateFilePath
 * @param {number} pid
 * @param {{ now?: number, staleAfterMs?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<FootprintStateEntry | null>}
 *   the pid's state as it stood immediately before eviction, or `null` if
 *   this pid had no persisted entry, or the persisted entry was stale (per
 *   `staleAfterMs`/`now`).
 */
export async function consumeFootprintState(stateFilePath, pid, options = {}) {
  const lockStalenessMs =
    typeof options.lockStalenessMs === 'number' ? options.lockStalenessMs : DEFAULT_LOCK_STALENESS_MS;
  const lockPath = `${stateFilePath}.lock`;
  const key = String(pid);

  const token = await acquireLock(lockPath, lockStalenessMs, options.lockMaxAttempts, options.lockRetryDelayMs);
  const lockContext = buildLockContext(lockPath, token);
  let fnThrew = false;
  try {
    const store = await readStore(stateFilePath);
    const entry = store[key];
    if (entry === undefined) return null;

    delete store[key];
    await writeStoreAtomic(stateFilePath, store, lockContext);

    const isStale =
      typeof options.staleAfterMs === 'number' &&
      typeof options.now === 'number' &&
      typeof entry.lastPolledAt === 'number' &&
      options.now - entry.lastPolledAt > options.staleAfterMs;

    return isStale ? null : entry;
  } catch (error) {
    fnThrew = true;
    throw error;
  } finally {
    try {
      await releaseLock(lockPath, token);
    } catch (releaseError) {
      if (!fnThrew) throw releaseError;
    }
  }
}

/**
 * Evicts a single pid's persisted poll state. This store otherwise has no
 * eviction path — nothing removes an entry on any TTL basis — so a
 * long-lived orchestrator with high pid churn (this feature's exact use
 * case) would grow the file unboundedly.
 *
 * `cli.mjs`'s `handleRecordOutcome` no longer calls this directly — it uses
 * `consumeFootprintState` instead, which reads and evicts a pid's entry
 * under one lock acquisition (see that function's doc comment for the race
 * this replacement closes). This export is kept standalone for any caller
 * that genuinely wants eviction without also needing the entry's prior
 * value.
 *
 * Tolerates a not-yet-created store, or a pid with no entry, as a no-op
 * (not an error) — mirroring `readFootprintState`'s own tolerant-of-absence
 * contract.
 *
 * Uses the same exclusive-create fencing lock as `recordFootprintPoll`, so a
 * concurrent poll of this pid can never lose its write to this delete (or
 * vice versa).
 *
 * @param {string} stateFilePath
 * @param {number} pid
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<void>}
 */
export async function clearFootprintState(stateFilePath, pid, options = {}) {
  const lockStalenessMs =
    typeof options.lockStalenessMs === 'number' ? options.lockStalenessMs : DEFAULT_LOCK_STALENESS_MS;
  const lockPath = `${stateFilePath}.lock`;
  const key = String(pid);

  const token = await acquireLock(lockPath, lockStalenessMs, options.lockMaxAttempts, options.lockRetryDelayMs);
  const lockContext = buildLockContext(lockPath, token);
  let fnThrew = false;
  try {
    const store = await readStore(stateFilePath);
    if (!(key in store)) return;
    delete store[key];
    await writeStoreAtomic(stateFilePath, store, lockContext);
  } catch (error) {
    fnThrew = true;
    throw error;
  } finally {
    try {
      await releaseLock(lockPath, token);
    } catch (releaseError) {
      if (!fnThrew) throw releaseError;
    }
  }
}
