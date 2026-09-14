// Phase 3 — coordination file: dibs, atomic writes, debounce.
//
// Multi-writer filesystem coordination across independent OS processes. Per
// the Investigation, Build Plan (Phase 3), and Convergence Analysis
// comments, this module owns:
//
//   - declareDibs/readDibs — an upsert-by-orchestratorId ledger of "I intend
//     to spawn N agents" declarations, persisted as a small JSON array.
//   - atomic writes — every write goes to a `<path>.tmp-<random>` sibling
//     file, then `rename()`s it onto the real path. `rename(2)` is atomic on
//     the same filesystem, so a concurrent reader can only ever observe the
//     whole old file or the whole new file — never a torn/partial parse.
//   - concurrent-write safety — an exclusive-create lock file
//     (`<path>.lock`, via `wx`-mode `open()`) serialises the
//     read-modify-write cycle across concurrent writers (same process or
//     independent processes); losers retry with a short, jittered bounded
//     backoff rather than corrupting or dropping data. The lock file carries
//     an identifying payload (pid + acquired-at timestamp + a random fencing
//     token); a lock older than `options.lockStalenessMs` (default 30s) is
//     treated as orphaned (e.g. its writer was SIGKILL'd/OOM-killed between
//     acquiring the lock and its `finally`-block release) and is forcibly
//     reclaimed rather than burning the full retry ceiling forever.
//   - fencing invariant — a lock file is never removed (or overwritten) from
//     a shared path by path alone or by a "this looked stale a moment ago"
//     verdict. Every removal/restore path (`releaseLock`'s unlink,
//     `reclaimStaleLock`'s reclaim-unlink and its restore-on-not-stale) first
//     proves, by reading the CURRENT content at that instant (via an atomic
//     claim-then-verify `rename` onto a private tmp path), that what's there
//     is still the exact lock this caller is entitled to touch — identified
//     by the random `token` `acquireLock` minted when it created the lock —
//     before acting. If the content has moved on (someone else's lock now
//     occupies the path), the call is a safe no-op: nothing this caller
//     cared about is still there to remove. This closes the whole class of
//     "removed the wrong lock" races structurally, rather than narrowing yet
//     another timing window.
//   - write-side ownership re-check — a SEPARATE and deliberately
//     WEAKER guarantee than the fencing invariant above; the two must not be
//     conflated. Acquiring the lock proves ownership at acquisition time, but
//     a critical section that outlives `options.lockStalenessMs` can have its
//     lock legitimately reclaimed (`reclaimStaleLock` cannot distinguish a
//     stalled-but-alive holder from a SIGKILLed one) and then write anyway,
//     clobbering the reclaiming holder. So `writeEntriesAtomic` re-reads the
//     lock file and re-compares the fencing `token` immediately before its
//     `rename`, and throws `LockLostError` instead of writing when it no
//     longer matches. It is a plain read, not the claim-then-verify `rename`
//     the removal/restore paths use — renaming would move aside the lock this
//     caller STILL HOLDS, and that window would itself become a fresh
//     instance of the race being addressed. The consequence is that this
//     re-check NARROWS the exposure from an entire critical section down to
//     the single event-loop hop between the check and the `rename`; it does
//     NOT close it, and must never be described as doing what the removal/restore
//     bullet above does. Enforcement lives at the single write choke point
//     (`withLock` threads a lock context into `fn`; `writeEntriesAtomic`
//     refuses to run without one) so a future writer cannot silently omit it.
//   - liveness pruning — declareDibs prunes entries older than
//     `options.livenessThresholdMs` (relative to the entry being written) in
//     the same write; readDibs applies the identical exclusion at read time,
//     so staleness is enforced even without an intervening write.
//   - isSampleFresh — a pure freshness-window check (inclusive boundary)
//     used to debounce recon sampling.
//
// This module never shells out and never touches anything but `filePath`,
// its own `.tmp-*` siblings, and its own `.lock` sibling — it must never
// glob or otherwise inspect the rest of the containing directory (orphaned
// `.tmp` files or unrelated files left there by other processes are simply
// not this module's concern).

import { promises as fs } from 'node:fs';
import { randomBytes } from 'node:crypto';

import { consumeSpawnTokens } from './allowance.mjs';

/**
 * Reserved orchestratorId the global spawn-rate token bucket
 * is persisted under — satisfies `isReservedEntry`'s leading/trailing `__`
 * convention, so it is never subject to the normal liveness pruning
 * `declareDibs`/`readDibs` apply to real orchestrator entries.
 */
const GLOBAL_SPAWN_BUCKET_RESERVED_ID = '__global-spawn-rate-bucket__';

/**
 * Reserved orchestratorId the shared-machine-sample entry is
 * persisted under — same leading/trailing `__` convention as
 * `GLOBAL_SPAWN_BUCKET_RESERVED_ID`, so `isReservedEntry` recognises it and
 * `pruneStale` exempts it from the normal 15-minute liveness pruning; its
 * lifecycle is governed solely by `isSampleFresh`'s (separate, much shorter)
 * freshness window, checked by callers.
 */
export const SHARED_SAMPLE_RESERVED_ID = '__shared-machine-sample__';

/**
 * Reserved orchestratorId the AIMD concurrency-ceiling shared state
 * (Phase 2/3) is persisted under — same leading/trailing `__` convention as
 * `GLOBAL_SPAWN_BUCKET_RESERVED_ID`/`SHARED_SAMPLE_RESERVED_ID`, so
 * `isReservedEntry` recognises it and `pruneStale` exempts it from the
 * normal 15-minute liveness pruning unconditionally. Its lifecycle is
 * governed solely by `readAimdCeilingState`/`writeAimdCeilingState` below —
 * never by declared-at age.
 */
export const AIMD_CEILING_RESERVED_ID = '__aimd-ceiling-state__';

/**
 * Reserved orchestratorId the sticky "has this session EVER declared dibs?"
 * set (design decision 1) is persisted under — same
 * leading/trailing `__` convention as `GLOBAL_SPAWN_BUCKET_RESERVED_ID`/
 * `SHARED_SAMPLE_RESERVED_ID`/`AIMD_CEILING_RESERVED_ID`, so
 * `isReservedEntry` recognises it and `pruneStale` exempts it from the normal
 * 15-minute liveness pruning unconditionally.
 *
 * THE POINT of the exemption here is not an optimisation, it is the whole
 * feature: the PreToolUse gate asks "has this session ever opted in", not
 * "does it hold a live entry right now". A mid-session `pruneStale` expiry or
 * a stray `--release` must never silently disarm the gate, so the ROW itself
 * is never aged out by `pruneStale`, whatever its `declaredAt` says.
 *
 * PERSISTED SHAPE (canonical, as written by `unionEverOptedIn`):
 *
 *   { orchestratorId: '__ever-opted-in__',
 *     everOptedIn: [{ id: string, lastSeenAt: number }, ...],
 *     declaredAt: number }
 *
 * `everOptedIn` is a de-duplicated set of session-derived orchestrator-ids,
 * each carrying its OWN last-touched timestamp. Per-id timestamps — not a
 * single row-level one — are what let the set be bounded without ever
 * evicting a live session: see `EVER_OPTED_IN_MAX_AGE_MS` /
 * `EVER_OPTED_IN_MAX_RECORDS`. The row-level `declaredAt` means only "when
 * this row was last rewritten"; it is written to keep the row shaped like
 * every other entry in this file (and to stay legible in a hand-inspected
 * ledger), and NOTHING reads it — not for liveness (`pruneStale` exempts
 * reserved rows unconditionally), and not for ageing (that is per-id
 * `lastSeenAt`'s job). Do not reintroduce a decision that reads it.
 *
 * LEGACY SHAPE, still READ: the first cut of this row (earlier in its own
 * build) persisted a bare `orchestratorIds: string[]` with no per-id
 * timestamp. `unionEverOptedIn`/`hasEverOptedIn` still read such a row, and
 * the next write migrates its members into the per-id-record shape (dated
 * `now`, i.e. treated as freshly seen, so migration can never itself evict a
 * live session) and drops the legacy field.
 *
 * Exactly ONE such row ever exists. It is mutated only through
 * `recordEverOptedIn`/`declareDibs`, i.e. only inside `withLock` +
 * `writeEntriesAtomic`, and read (lock-free, fail-open) by `hasEverOptedIn`.
 */
export const EVER_OPTED_IN_RESERVED_ID = '__ever-opted-in__';

/**
 * Age horizon for a single id in the ever-opted-in set: a record untouched
 * for this long is evicted on the next write of the row (per review —
 * the set was otherwise monotonic, the growth this design's Consequences
 * explicitly said "must not be forgotten").
 *
 * 30 days is deliberately generous rather than tight. The cost of evicting
 * too eagerly is a gate that silently disarms for a session that is still
 * alive — the exact failure this whole row exists to prevent — while the cost
 * of evicting too late is a few extra hundred bytes in a file the hook parses
 * in well under a millisecond. Asymmetric costs, so the horizon sits far past
 * any plausible single Claude Code session.
 */
export const EVER_OPTED_IN_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000;

/**
 * Count-cap backstop for the ever-opted-in set, applied after the
 * `EVER_OPTED_IN_MAX_AGE_MS` sweep. It exists for the pathological case the
 * age horizon alone cannot bound: a burst that mints far more ids inside one
 * 30-day window than a human host ever really runs (a runaway loop handing a
 * fresh session id to every call).
 *
 * Eviction is strictly OLDEST-`lastSeenAt`-first, and the id being touched by
 * the write is exempt by construction, so a live session is never the victim
 * even when it was the very first id ever recorded.
 */
export const EVER_OPTED_IN_MAX_RECORDS = 256;

/**
 * Reserved pseudo-entry naming the DISK-CLEANUP-ARMED set —
 * the opt-in that gates `cli.mjs`'s disk-axis temp sweep, i.e. the only code
 * path in this skill that RECURSIVELY DELETES directories.
 *
 * IT IS DELIBERATELY NOT `EVER_OPTED_IN_RESERVED_ID`, AND THAT SEPARATION IS
 * THE WHOLE POINT. `declareDibs` unions the beat's own orchestrator-id into
 * the ever-opted-in set as a SIDE EFFECT of declaring capacity (see the
 * `unionEverOptedIn` call in `declareDibs`), and it does so BEFORE `cli.mjs`
 * reaches its trigger-evaluation point. Gating a destructive sweep on that set
 * would therefore make "opt-in, default-inert" actually mean "default-ARMED"
 * for every beat that declares dibs — including the unattended launchd
 * watchdog beat. This set has exactly ONE writer, `recordDiskCleanupArmed`,
 * reached only from `cli.mjs`'s dedicated `--enable-disk-cleanup` beat. No
 * capacity-declaration path may ever touch it, and no future one may be added:
 * `coordination-file-disk-cleanup-armed.jest.spec.mjs` asserts that
 * `declareDibs` leaves this row absent.
 *
 * Membership semantics, bounds and fail-open reads are otherwise identical to
 * the ever-opted-in set — it reuses the same sticky-id-set machinery and the
 * same `EVER_OPTED_IN_MAX_AGE_MS` / `EVER_OPTED_IN_MAX_RECORDS` bounds, so
 * there is one set of rules to reason about rather than two.
 */
export const DISK_CLEANUP_ARMED_RESERVED_ID = '__disk-cleanup-armed__';

/**
 * Reserved pseudo-entry carrying the disk-axis sweep's COOLDOWN
 * (review, Medium finding) — the trigger's memory of its own last outcome.
 *
 * HOST-WIDE, NOT PER-ORCHESTRATOR, and that is the point. What the cooldown
 * records is a fact about the shared temp root ("a sweep just ran it to
 * completion and found nothing worth doing"), not a fact about whoever
 * happened to run it. Making it per-id would let N concurrent orchestrators
 * each pay the full futile sweep cost against the same already-clean root —
 * exactly the waste the cooldown exists to stop, multiplied by N.
 *
 * SHAPE: `{ orchestratorId: '__disk-cleanup-cooldown__', suppressedUntil:
 * number, declaredAt: number }`. `suppressedUntil` is an absolute instant on
 * the BEAT clock (`cli.mjs`'s `resolveNow()`), because both the write and the
 * read are made from the beat path and must agree; it is deliberately NOT the
 * real-`Date.now()` clock the sweep itself uses for file mtimes.
 *
 * It is state, not membership, so it is governed by its own
 * `isDiskCleanupSuppressed`/`recordDiskCleanupCooldown` pair rather than by
 * the sticky-id-set machinery — the same split
 * `AIMD_CEILING_RESERVED_ID` already makes for the same reason.
 *
 * It is NEVER a gate on its own. It can only ever DELAY a sweep that all three
 * real gates (`--advise-only`, armed, disk evidence) have already allowed, so
 * a corrupt, absent, or hostile value degrades to "not suppressed" — i.e. to
 * the pre-cooldown behaviour — never to a sweep that should not have run.
 */
export const DISK_CLEANUP_COOLDOWN_RESERVED_ID = '__disk-cleanup-cooldown__';

/**
 * The per-row field each sticky-id set stores its canonical records under.
 * Paired with its reserved row id so a caller cannot accidentally read one
 * set's field out of the other's row.
 */
const EVER_OPTED_IN_SET = { reservedId: EVER_OPTED_IN_RESERVED_ID, setField: 'everOptedIn' };
const DISK_CLEANUP_ARMED_SET = { reservedId: DISK_CLEANUP_ARMED_RESERVED_ID, setField: 'diskCleanupArmed' };

/**
 * Cold-start default for the AIMD ceiling state: `ceiling` seeded at 4
 * (matching its shipped `LIVE_AGENT_CEILING` default — the starting
 * point Phase 2/3's AIMD adjustment reads and mutates from), and
 * `sustainedNormalCount` seeded at 0 (no Normal-pressure beats observed
 * yet).
 */
const DEFAULT_AIMD_CEILING_STATE = { ceiling: 4, sustainedNormalCount: 0 };

// Lock retry/backoff: bounded and fast. This is host tooling coordinating a
// handful of local orchestrator processes, not a distributed system — no
// need for exponential backoff into seconds. A small jitter is added to each
// retry to avoid lock-step contention when several writers are polling the
// same lock.
//
// The ceiling, stated accurately (— an earlier version of this comment
// said "~10-15s" and callers sized their own constants against that
// understatement): 2000 attempts * (5 + rand*5) ms is 10-20s of SLEEP alone,
// mean ~15s, plus 2000 rounds of `open`/`looksStale`/`reclaimStaleLock`
// syscalls on top — so the real worst case exceeds 20s on a loaded host.
// A caller that needs its own timing guarantee must pass `lockMaxAttempts`
// rather than reason about this default; `cli.mjs`'s `--claim` does exactly
// that, because its hoisted-sample freshness bound has to clear the lock wait
// with margin.
// Exported (review round 2) so a caller that passes its OWN
// `lockMaxAttempts` can derive the wait window that ceiling actually buys --
// `attempts * delay` through `attempts * (delay + jitter)` -- from the same
// numbers `acquireLock` sleeps on, rather than restating them. The recurrence
// this closes is the one that produced this ticket: a caller carrying its own
// copy of the arithmetic drifts silently the moment either constant moves.
// `lib/claim-denial.mjs` is the first such caller.
export const LOCK_RETRY_DELAY_MS = 5;
export const LOCK_RETRY_JITTER_MS = 5;
const LOCK_MAX_ATTEMPTS = 2000;

// Default staleness threshold for an orphaned `.lock` file: long enough that
// a normal brief critical section (the read-modify-write cycle in
// `declareDibs`) never trips it, short enough that a writer killed
// (SIGKILL/OOM) between acquiring the lock and its `finally`-block release
// doesn't block every future caller for long. Overridable per-call via
// `options.lockStalenessMs`.
const DEFAULT_LOCK_STALENESS_MS = 30_000;

/**
 * @param {unknown} value
 * @returns {value is number}
 */
function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * A lock-acquisition timeout, distinguishable from other failures (JSON
 * parse errors, EACCES, etc.) that can also escape `declareDibs`/`readDibs`.
 * Callers can catch on `error.code === 'COORDINATION_LOCK_TIMEOUT'` rather
 * than string-matching `error.message`.
 */
export class LockTimeoutError extends Error {
  constructor(lockPath) {
    super(`coordination-file: timed out acquiring lock at ${lockPath}`);
    this.name = 'LockTimeoutError';
    this.code = 'COORDINATION_LOCK_TIMEOUT';
  }
}

/**
 * Thrown by `claimCapacity`/`releaseCapacity` for the specific caller-error
 * combination `assertClaimTtlSetAgainstLegacyLedger` guards against:
 * `options.claimTtlMs` unset while the ledger for `type` still carries a
 * legacy-migrated (pre-existing) record with no live attribution — a
 * combination with no recovery path (see that function's doc comment).
 * Distinguishable from other failures (JSON parse errors, `EACCES`,
 * `TypeError`s from the existing param validation, etc.) via `.code`,
 * mirroring `LockTimeoutError`'s `.code` convention exactly.
 */
export class UnrecoverableLegacyClaimError extends Error {
  constructor(type) {
    super(
      `coordination-file: refusing claim/release for type "${type}" — options.claimTtlMs is unset ` +
        'while the ledger still holds a legacy-migrated (pre-existing) record with no live ' +
        'orchestrator attribution; such a record can never expire without a TTL and no real ' +
        'orchestratorId can ever selectively release it, so it would sit on the ledger forever. ' +
        'Pass a finite, positive options.claimTtlMs (e.g. DEFAULT_CLAIM_TTL_MS) to let it age out.',
    );
    this.name = 'UnrecoverableLegacyClaimError';
    this.code = 'COORDINATION_UNRECOVERABLE_LEGACY_CLAIM';
  }
}

/**
 * Thrown by `writeEntriesAtomic` when the fencing lock this caller acquired
 * is no longer the lock on disk at the moment the write was about to be
 * committed — i.e. the caller stalled long enough for `reclaimStaleLock` to
 * legitimately hand the lock to someone else. Distinguishable from
 * other failures via `.code`, mirroring `LockTimeoutError`'s and
 * `UnrecoverableLegacyClaimError`'s convention exactly.
 *
 * Deliberately a THROW rather than the `{persisted: false, reason}` shape
 * `writeSharedSample` uses for its own non-write cases. Those are decisions
 * ("this reading is older than what is stored, so there is nothing to do");
 * this is a failure ("this call cannot prove it is entitled to write"). A
 * return value a caller forgets to inspect degrades straight back into the
 * silently-dropped write this error exists to make impossible.
 *
 * Carries no extra instance properties (no `lockPath`, no `wroteBeforeLoss`):
 * neither existing error class in this module does, and the assert is placed
 * BEFORE the `rename`, so this error is never RAISED after the raising call's
 * own `rename` has committed — there is nothing for a `wroteBeforeLoss`
 * discriminator to discriminate. Note this is a claim about when the error
 * can be thrown, NOT a claim that losing the lock after a write has landed is
 * unreachable: the residual assert-to-rename window means it demonstrably
 * is (see this module's header).
 */
export class LockLostError extends Error {
  constructor(lockPath) {
    super(
      `coordination-file: refusing to write — the fencing lock at ${lockPath} is no longer ` +
        'held by this caller (it was reclaimed as stale while this critical section was still ' +
        'running, and another holder now owns it); the write was abandoned rather than ' +
        "silently overwriting the new holder's work",
    );
    this.name = 'LockLostError';
    this.code = 'COORDINATION_LOCK_LOST';
  }
}

/**
 * Parses the identifying payload (pid + acquired-at timestamp + fencing
 * `token`) written into a lock file at creation, from already-read raw text.
 * Returns `null` if the content isn't valid JSON, or doesn't carry a finite
 * `acquiredAt` — either of which means "can't prove staleness/ownership,
 * treat as not-yet-provably-stale/not-ours" rather than crashing the retry
 * loop. `token` may be absent on payloads written before this field existed
 * (defensive only — nothing in this repo still writes untokened payloads);
 * callers that compare tokens treat a missing token as "doesn't match".
 *
 * @param {string} raw
 * @returns {{ pid: number, acquiredAt: number, token?: string } | null}
 */
function parseLockPayload(raw) {
  try {
    const parsed = JSON.parse(raw);
    if (parsed && isFiniteNumber(parsed.acquiredAt)) return parsed;
    return null;
  } catch {
    return null;
  }
}

/**
 * Applies the same inclusive-boundary staleness check as `pruneStale` to a
 * single lock payload: a lock exactly at the threshold is still considered
 * held (not yet reclaimable); only strictly older lock files are stale.
 *
 * @param {{ acquiredAt: number }} lockPayload
 * @param {number} referenceNow
 * @param {number} lockStalenessMs
 * @returns {boolean}
 */
function isLockStale(lockPayload, referenceNow, lockStalenessMs) {
  return referenceNow - lockPayload.acquiredAt > lockStalenessMs;
}

/**
 * Non-destructively reads and judges whether the lock file at `lockPath` is
 * currently stale, WITHOUT touching it. A cheap, read-only pre-check —
 * deliberately never renames or removes anything, so a legitimate,
 * actively-held lock is never disturbed just because a contender happened
 * to look at it. Returns `false` if the lock is gone or unreadable.
 *
 * When the content isn't valid tokened JSON (e.g. a lock file left half
 * written by a writer that hit `ENOSPC` or was killed mid-write), falls back
 * to the lock file's own `mtime` for the staleness judgement: an unparseable
 * lock file older than `lockStalenessMs` by mtime is, by definition, an
 * orphan — nothing legitimate is still writing to it. This is an additional
 * staleness SIGNAL only; it does not change how a lock is actually removed
 * — `reclaimStaleLock`'s claim-then-verify atomic-rename ordering still
 * governs every removal.
 *
 * @param {string} lockPath
 * @param {number} lockStalenessMs
 * @returns {Promise<boolean>}
 */
async function looksStale(lockPath, lockStalenessMs) {
  let raw;
  try {
    raw = await fs.readFile(lockPath, 'utf8');
  } catch {
    return false;
  }
  const lockPayload = parseLockPayload(raw);
  if (lockPayload) {
    return isLockStale(lockPayload, Date.now(), lockStalenessMs);
  }
  return isUnparseableLockStaleByMtime(lockPath, lockStalenessMs);
}

/**
 * Staleness fallback for a lock file whose content isn't valid tokened JSON
 * — falls back to the file's own `mtimeMs` rather than giving up and
 * treating an unparseable lock as permanently un-reclaimable (which would
 * wedge the coordination file forever once such a lock exists, e.g. from a
 * writer killed mid-`writeFile` or a full-disk partial write). Same
 * inclusive-boundary convention as `isLockStale`: exactly at the threshold
 * is still considered held.
 *
 * Returns `false` (not-yet-provably-stale) if the file is gone or its stat
 * can't be read — the caller's normal retry/backoff loop handles that case.
 *
 * @param {string} lockPath
 * @param {number} lockStalenessMs
 * @returns {Promise<boolean>}
 */
async function isUnparseableLockStaleByMtime(lockPath, lockStalenessMs) {
  let stat;
  try {
    stat = await fs.stat(lockPath);
  } catch {
    return false;
  }
  return Date.now() - stat.mtimeMs > lockStalenessMs;
}

/**
 * Attempts to hand `raw` (the exact content this caller privately holds,
 * moved aside from `lockPath` by an earlier `rename`) back onto `lockPath`,
 * used when this caller has decided the content it moved aside is NOT
 * (after all) something it's entitled to remove/replace.
 *
 * Deliberately uses exclusive-create (`open(lockPath, 'wx')`) rather than
 * `fs.rename(tmpPath, lockPath)`. `rename(2)` on POSIX atomically
 * *overwrites* an existing destination rather than failing — so a
 * rename-based restore would silently clobber a fresh, legitimately-held
 * lock that a third caller created at `lockPath` while this caller's copy
 * was moved aside, with no error to signal it. Exclusive-create fails with
 * `EEXIST` in exactly that case, which is the correct outcome: there is
 * nothing to restore onto a path someone else already legitimately
 * re-occupies, so this caller's private copy is simply discarded — the
 * newer lock governs.
 *
 * Exported (alongside `reclaimStaleLock`/`acquireLock`/`releaseLock`) solely
 * so the test suite can drive this cleanup mechanism directly — not part of
 * the documented production API; production callers reach it only through
 * `reclaimStaleLock`/`releaseLock`.
 *
 * @param {string} lockPath
 * @param {string} tmpPath
 * @param {string} raw
 * @returns {Promise<void>}
 */
export async function restoreOrDiscard(lockPath, tmpPath, raw) {
  try {
    const handle = await fs.open(lockPath, 'wx');
    try {
      await handle.writeFile(raw, 'utf8');
    } catch (writeError) {
      // Mirrors `acquireLock`'s write-failure cleanup branch: the
      // `open('wx')` above already created `lockPath` on disk — a write
      // failure here (e.g. ENOSPC) must not leave that half-written lock
      // file behind (it would wedge every future caller) nor leak the open
      // handle. Close the handle and remove the just-created lock file
      // before propagating the original error. Also remove `tmpPath` here —
      // the unlink below this try/catch only runs on the success path, so
      // without this a genuine write failure would rethrow before reaching
      // it and orphan the caller's private moved-aside copy on disk.
      await handle.close().catch(() => {});
      await fs.unlink(lockPath).catch(() => {});
      await fs.unlink(tmpPath).catch(() => {});
      throw writeError;
    }
    await handle.close();
  } catch (error) {
    if (error.code !== 'EEXIST') throw error;
    // A third caller's `open('wx')` already filled the gap; nothing left to
    // restore onto. The newer lock now governs — fall through and discard.
  }
  await fs.unlink(tmpPath).catch((unlinkError) => {
    if (unlinkError.code !== 'ENOENT') throw unlinkError;
  });
}

/**
 * Attempts to reclaim whatever lock file currently sits at `lockPath`,
 * ONLY called once a cheap, non-destructive pre-check (`looksStale`) has
 * already suggested staleness — this function itself re-verifies staleness
 * AFTER atomically claiming the file, as a race-free backstop against that
 * pre-check going out of date between the read and the claim.
 *
 * Why a backstop is needed at all: two callers can both form the same
 * "looks stale" verdict from `looksStale` at nearly the same time. If one
 * of them (A) then reclaims and recreates a fresh lock before the other's
 * (B) claim attempt actually executes, a naive design that trusted the
 * earlier verdict unconditionally would blindly clobber A's fresh lock —
 * by pathname, with no way to tell it wasn't the same file B judged stale.
 * That let two callers end up in the critical section simultaneously
 * (reproduced empirically with real child processes racing a pre-seeded
 * orphaned lock: ~36% of runs silently lost an entry with zero thrown
 * errors).
 *
 * The claim-then-verify ordering closes that specific window:
 *   1. `rename(lockPath, tmpPath)` — atomic; `rename(2)` only succeeds for
 *      whichever caller's rename call actually executes against the file
 *      currently at `lockPath`. An `ENOENT` here is a race-free "someone
 *      else already claimed/removed it" signal — report failure; the outer
 *      loop simply tries `open(lockPath, 'wx')` fresh.
 *   2. Only once a caller holds an exclusive, no-longer-racy private copy
 *      (at `tmpPath`) does it re-judge that copy's staleness. Whoever wins
 *      step 1 is provably holding the CURRENT content of `lockPath` at that
 *      instant — this check is race-free too.
 *   3. If genuinely (still) stale: delete `tmpPath` — reclaimed, success.
 *   4. If NOT stale (this caller's claim raced against a different caller
 *      legitimately recreating a fresh lock in the gap since the pre-check,
 *      or an unreadable/ambiguous payload it can't prove is stale) — restore
 *      `tmpPath` back onto `lockPath` via `restoreOrDiscard` (exclusive
 *      create, never a clobbering `rename`) to hand it back to its rightful
 *      owner, and report failure. If the restore itself hits `EEXIST` (a
 *      third caller's `open('wx')` filled the brief gap while this lock was
 *      moved aside), there is nothing left to restore onto — `tmpPath` is
 *      discarded and this call reports failure; the newer lock now governs.
 *
 * This path is deliberately reserved for the narrow, already-suspected-stale
 * case: it must NOT be invoked unconditionally on every `EEXIST` (see
 * `acquireLock`), because step 4's restore briefly removes whatever it
 * claimed from disk before putting it back — safe for a genuinely rare
 * false-positive, but if run against every contending caller's `EEXIST`
 * (including fresh, actively-held locks) it would itself become the primary
 * source of the exact race it's meant to close.
 *
 * Exported (alongside the documented `declareDibs`/`readDibs`/`isSampleFresh`
 * public surface) solely so the test suite can race several calls against
 * the exact same stale lock deterministically, within a single process, to
 * prove this ordering closes the double-reclaim window — not part of the
 * documented production API.
 *
 * @param {string} lockPath
 * @param {number} lockStalenessMs
 * @returns {Promise<boolean>} true if this call reclaimed a genuinely stale lock.
 */
export async function reclaimStaleLock(lockPath, lockStalenessMs) {
  const tmpPath = `${lockPath}.reclaim-${randomBytes(8).toString('hex')}`;
  try {
    await fs.rename(lockPath, tmpPath);
  } catch (error) {
    if (error.code === 'ENOENT') return false;
    throw error;
  }

  let raw;
  try {
    raw = await fs.readFile(tmpPath, 'utf8');
  } catch {
    raw = '';
  }
  const lockPayload = parseLockPayload(raw);
  // Unparseable content (e.g. a lock left over from a killed/ENOSPC-mid-write
  // writer) falls back to mtime — the file at `tmpPath` is the exact same
  // inode moved aside by the rename above, so its mtime still reflects when
  // the orphaned content was last written. See `isUnparseableLockStaleByMtime`.
  const stale = lockPayload
    ? isLockStale(lockPayload, Date.now(), lockStalenessMs)
    : await isUnparseableLockStaleByMtime(tmpPath, lockStalenessMs);

  if (stale) {
    try {
      await fs.unlink(tmpPath);
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    return true;
  }

  // Not stale after all — this claim raced against a legitimate, freshly
  // recreated lock (or one this call can't prove is stale). Hand it back
  // rather than forcing through — via exclusive-create, never a clobbering
  // `rename` (see `restoreOrDiscard`'s doc-comment for why).
  await restoreOrDiscard(lockPath, tmpPath, raw);
  return false;
}

/**
 * Acquires an exclusive lock at `${filePath}.lock` via `wx`-mode `open()`
 * (fails with EEXIST if the lock already exists), retrying with a short
 * bounded, jittered backoff until it succeeds or the attempt ceiling is hit.
 *
 * Writes an identifying payload (pid + acquired-at timestamp + a random
 * fencing `token` unique to this acquisition) into the lock file at
 * creation, and returns that `token` to the caller. The token is the
 * fencing mechanism `releaseLock` uses to prove, before ever unlinking
 * `lockPath`, that the lock currently on disk is still the exact one this
 * call created — never "whichever lock currently occupies the path".
 *
 * On each `EEXIST`, first runs a cheap, non-destructive `looksStale`
 * pre-check (a plain read — never disturbs an actively-held lock just
 * because a contender looked at it). Only when that pre-check suggests
 * staleness does it call `reclaimStaleLock`, which atomically claims
 * whatever is currently at `lockPath` and re-verifies staleness AFTER
 * claiming it — a race-free backstop against the pre-check having gone
 * stale itself between the read and the claim (see that function's
 * doc-comment). If the reclaim reports success, retries immediately rather
 * than sleeping against a permanent obstacle; otherwise (pre-check said
 * fresh, reclaim lost the race, or reclaim had to restore a legitimate
 * lock) falls through to the normal jittered backoff.
 *
 * `lockMaxAttempts`/`lockRetryDelayMs` shrink the retry ceiling. They began
 * as test-only knobs, but are a production knob as of : `cli.mjs`'s
 * `--claim` passes `lockMaxAttempts` because its hoisted-sample freshness
 * bound has to clear the lock wait with margin, and the DEFAULT ceiling does
 * not fit inside it. Any caller with its own timing guarantee to keep must
 * pass this rather than reason about the default — and must NOT size that
 * guarantee against "~10-15s", which an earlier version of this file's own
 * header said and which is an understatement: the default is 10-20s of retry
 * SLEEP alone (`2000 * (5 + rand*5) ms`), plus 2000 rounds of
 * `open`/`looksStale`/`reclaimStaleLock` syscalls on top. Sizing a caller
 * constant against the understated figure is exactly how its
 * silent-denial regression was created. They default to the production
 * constants.
 *
 * Exported (alongside `reclaimStaleLock`/`releaseLock`) solely so the test
 * suite can drive the fencing-token mechanism directly and deterministically
 * — not part of the documented production API; production callers go
 * through `declareDibs`/`readDibs`, which use `withLock` internally.
 *
 * @param {string} lockPath
 * @param {number} lockStalenessMs
 * @param {number} [lockMaxAttempts]
 * @param {number} [lockRetryDelayMs]
 * @returns {Promise<string>} the fencing token written into this lock.
 */
export async function acquireLock(
  lockPath,
  lockStalenessMs,
  lockMaxAttempts = LOCK_MAX_ATTEMPTS,
  lockRetryDelayMs = LOCK_RETRY_DELAY_MS,
) {
  for (let attempt = 0; attempt < lockMaxAttempts; attempt += 1) {
    try {
      const handle = await fs.open(lockPath, 'wx');
      const token = randomBytes(16).toString('hex');
      const payload = JSON.stringify({ pid: process.pid, acquiredAt: Date.now(), token });
      try {
        await handle.writeFile(payload, 'utf8');
      } catch (writeError) {
        // The `open('wx')` above already created `lockPath` — a write
        // failure here (e.g. ENOSPC) must not leave that half-written lock
        // file behind (it would wedge every future caller) nor leak the
        // open handle. Close the handle and remove the just-created lock
        // file before propagating the original error.
        await handle.close().catch(() => {});
        await fs.unlink(lockPath).catch(() => {});
        throw writeError;
      }
      await handle.close();
      return token;
    } catch (error) {
      if (error.code !== 'EEXIST') throw error;

      if (await looksStale(lockPath, lockStalenessMs)) {
        const reclaimed = await reclaimStaleLock(lockPath, lockStalenessMs);
        if (reclaimed) {
          // Loop back to the top and attempt `open(lockPath, 'wx')` fresh.
          // If a different caller's legitimate new lock is already there,
          // that re-attempt fails with `EEXIST` and falls into this same
          // retry path — never clobbered, never forced through.
          continue;
        }
      }

      await sleep(lockRetryDelayMs + Math.random() * LOCK_RETRY_JITTER_MS);
    }
  }
  throw new LockTimeoutError(lockPath);
}

/**
 * Releases the lock at `lockPath` — but ONLY if the lock currently on disk
 * is still the exact one this caller created, proven by comparing its
 * fencing `token` against the one `acquireLock` returned to this caller.
 *
 * This is the fix for the residual 3-way race an unconditional
 * `fs.unlink(lockPath)` was exposed to: without a fencing check, a caller
 * releasing "its" lock by path alone can drop whichever lock currently
 * occupies that path — including a different, still-live lock created by
 * someone else after a stale-lock reclaim raced this caller's own claim.
 *
 * Uses the same atomic claim-then-verify shape as `reclaimStaleLock`
 * (`rename` onto a private tmp path, THEN inspect content), rather than a
 * plain `readFile` + `unlink`, so there is no read-then-unlink gap in which
 * a different caller's lock could slip onto `lockPath` between the check
 * and the removal:
 *
 *   1. `rename(lockPath, tmpPath)` — atomic; `ENOENT` means the lock is
 *      already gone (nothing to release; safe no-op).
 *   2. Read the claimed copy's payload and compare its `token` to the one
 *      this caller was issued.
 *   3. Token matches → genuinely ours: discard the private copy. Released.
 *   4. Token doesn't match (or payload is unreadable/untokened) → this
 *      caller's lock was already gone and something else's lock is
 *      currently there — restore it via `restoreOrDiscard` (exclusive
 *      create, never a clobbering `rename`) rather than dropping it, and
 *      report nothing further; there is nothing this caller is entitled to
 *      remove.
 *
 * Exported (alongside `reclaimStaleLock`/`acquireLock`) solely so the test
 * suite can drive the fencing-token mechanism directly and deterministically
 * — not part of the documented production API; production callers go
 * through `declareDibs`/`readDibs`, which use `withLock` internally.
 *
 * @param {string} lockPath
 * @param {string} token
 * @returns {Promise<void>}
 */
export async function releaseLock(lockPath, token) {
  const tmpPath = `${lockPath}.release-${randomBytes(8).toString('hex')}`;
  try {
    await fs.rename(lockPath, tmpPath);
  } catch (error) {
    if (error.code === 'ENOENT') return; // already gone — nothing to release
    throw error;
  }

  let raw;
  try {
    raw = await fs.readFile(tmpPath, 'utf8');
  } catch {
    raw = '';
  }
  const payload = parseLockPayload(raw);

  if (payload && payload.token === token) {
    await fs.unlink(tmpPath).catch((error) => {
      if (error.code !== 'ENOENT') throw error;
    });
    return;
  }

  // Not ours — someone else's lock is currently at this path (this caller's
  // own lock must already have been removed/replaced by another party).
  // Hand it back rather than dropping it.
  await restoreOrDiscard(lockPath, tmpPath, raw);
}

/**
 * Re-proves, at the instant it is called, that the lock currently at
 * `lockPath` is still the exact lock this caller acquired — identified by
 * the random fencing `token` `acquireLock` minted for it. Resolves silently
 * when ownership holds; throws `LockLostError` when it provably does not.
 *
 * WHAT THIS IS NOT: this is a NARROWING check, not the structural
 * close the removal/restore paths get. `releaseLock`/`reclaimStaleLock`
 * prove ownership by atomically CLAIMING the file first (`rename` onto a
 * private path) and inspecting it only once it can no longer be raced —
 * which is why those paths close the "acted on the wrong lock" class
 * outright. That shape is wrong here for a reason that is not a
 * simplification: renaming would move aside the lock this caller STILL
 * HOLDS, and the window in which it sat moved-aside would itself become a
 * fresh instance of the very race being narrowed. So this is deliberately a
 * plain read, and a reclaim landing between this read and the caller's
 * subsequent `rename` can still slip through. The window is one event-loop
 * hop wide — a threadpool-to-loop handoff, a microtask drain, and a fresh
 * threadpool dispatch — instead of an entire critical section wide. Narrow,
 * and not zero. Never document it as equivalent to the removal/restore
 * invariant.
 *
 * Touches nothing on the happy path — no rename, no unlink, no write — so a
 * legitimately-held lock is never disturbed by the act of checking it.
 *
 * Failure taxonomy, deliberately kept distinct:
 *   - lock file absent (`ENOENT`) -> `LockLostError`. Absence is TREATED as
 *     loss, which is not the same as proven loss: the lock this caller
 *     created is not at this path right now, and this call cannot
 *     distinguish a genuine reclaim from the transient gap `reclaimStaleLock`
 *     opens while handing a not-actually-stale lock back (it renames the
 *     lock aside, then `restoreOrDiscard` re-creates and rewrites it — a
 *     reader landing mid-window sees `ENOENT`, or an empty file). A holder
 *     that still legitimately owns its lock can therefore be refused. That is
 *     a false refusal, not a safety hole — it fails closed — and refusing is
 *     the only correct response to "cannot prove ownership".
 *   - present but unparseable (e.g. an `ENOSPC`-truncated write), or present
 *     with a different token -> `LockLostError`. Cannot prove ownership, so
 *     must not write.
 *   - any other read failure (`EACCES`, `EMFILE`, `EIO`) -> the ORIGINAL
 *     error propagates unchanged. That is "could not tell", not "provably
 *     lost", and conflating the two would misreport a transient
 *     file-descriptor exhaustion as a lock reclamation. Either way the
 *     caller's write is refused — fail closed — but the diagnosis stays
 *     honest.
 *
 * Exported (alongside `acquireLock`/`releaseLock`/`reclaimStaleLock`/
 * `restoreOrDiscard`/`writeEntriesAtomic`) solely so the test suite can
 * drive this directly — not part of the documented production API;
 * production callers reach it through the `lockContext` `withLock` threads
 * into `fn`.
 *
 * @param {string} lockPath
 * @param {string} token
 * @returns {Promise<void>}
 */
export async function assertStillHeld(lockPath, token) {
  let raw;
  try {
    raw = await fs.readFile(lockPath, 'utf8');
  } catch (error) {
    if (error.code === 'ENOENT') throw new LockLostError(lockPath);
    throw error;
  }

  const payload = parseLockPayload(raw);
  if (!payload || payload.token !== token) throw new LockLostError(lockPath);
}

/**
 * Serialises `fn` against every other `withLock` call for the same
 * `filePath`, across concurrent callers in this process (and, via the
 * exclusive-create lock file, across independent OS processes too).
 *
 * Threads the fencing token `acquireLock` mints for this call through to
 * `releaseLock`, so release only ever removes the lock this specific call
 * created — never whichever lock happens to occupy the path when `fn`
 * finishes.
 *
 * The same token is also handed to `fn` as a `lockContext`
 * (`{ lockPath, token, assertStillHeld }`), which `writeEntriesAtomic`
 * REQUIRES. Acquiring the lock proves ownership at time T0; a
 * critical section that outlives `lockStalenessMs` can have that lock
 * legitimately reclaimed out from under it long before its write at T1.
 * Threading the context rather than letting each call site remember to
 * re-check is what makes forgetting the guard impossible: a writer that does
 * not pass a context does not write at all. That matters most for writers
 * that do not exist yet.
 *
 * @template T
 * @param {string} filePath
 * @param {(lockContext: { lockPath: string, token: string, assertStillHeld: () => Promise<void> }) => Promise<T>} fn
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [lockOptions]
 * @returns {Promise<T>}
 */
async function withLock(filePath, fn, lockOptions = {}) {
  const lockPath = `${filePath}.lock`;
  const lockStalenessMs = isFiniteNumber(lockOptions.lockStalenessMs)
    ? lockOptions.lockStalenessMs
    : DEFAULT_LOCK_STALENESS_MS;
  const token = await acquireLock(lockPath, lockStalenessMs, lockOptions.lockMaxAttempts, lockOptions.lockRetryDelayMs);
  const lockContext = {
    lockPath,
    token,
    assertStillHeld: () => assertStillHeld(lockPath, token),
  };
  let fnThrew = false;
  try {
    return await fn(lockContext);
  } catch (error) {
    // A flag, not `primaryError !== undefined`: `fn` is free to throw a falsy
    // value, and a truthiness test would let the release error mask it.
    fnThrew = true;
    throw error;
  } finally {
    // Runs even when `fn` threw. On the `LockLostError` path the token
    // ALWAYS mismatches, which makes that the standard release path rather
    // than the rare one it used to be — so this frame needs the same
    // don't-mask-the-original discipline `writeEntriesAtomic` applies one
    // frame down.
    //
    // What `releaseLock` guarantees here, stated precisely: it is
    // fencing-safe — it never UNLINKS a lock it cannot prove is its own. It
    // is not a no-op. It renames whatever is at `lockPath` onto a private
    // path before inspecting it, and if a third contender wins
    // `open(lockPath, 'wx')` during that gap, `restoreOrDiscard` hits
    // `EEXIST` and discards the copy it was holding — so the new holder can
    // lose its lock FILE (it keeps a token no longer on disk, and is refused
    // on its own next write). That is a pre-existing residual of the
    // removal/restore design, not something this path introduces, and it
    // fails closed.
    //
    // Skipping the release when `fn` threw `LockLostError` was considered and
    // REJECTED, because the two failure directions are not comparable in the
    // way they first appear. `LockLostError` is also raised on a FALSE
    // refusal — `assertStillHeld` reading `ENOENT` inside the window
    // `reclaimStaleLock` opens while handing a not-actually-stale lock back —
    // and in that case the lock genuinely is still ours. Skipping would leak
    // it until `lockStalenessMs` elapses, blocking every other writer for up
    // to 30s. Calling `releaseLock` removes it correctly. Trading a rare,
    // instantly-recovered lock-file loss for a rare 30s wedge is the wrong
    // direction on a host whose whole problem is contention.
    try {
      await releaseLock(lockPath, token);
    } catch (releaseError) {
      // A release failure must never replace what `fn` threw — that error is
      // the diagnosis; this one is cleanup noise. With nothing to mask it
      // propagates normally.
      if (!fnThrew) throw releaseError;
    }
  }
}

/**
 * Reads the coordination file's entries, tolerating a not-yet-created file
 * (returns `[]` rather than throwing). Never reads any path other than
 * `filePath` itself — orphaned `.tmp-*` siblings and unrelated files in the
 * same directory are never inspected.
 *
 * @param {string} filePath
 * @returns {Promise<Array<object>>}
 */
async function readEntriesRaw(filePath) {
  let raw;
  try {
    raw = await fs.readFile(filePath, 'utf8');
  } catch (error) {
    if (error.code === 'ENOENT') return [];
    throw error;
  }

  if (!raw) return [];

  const parsed = JSON.parse(raw);
  return Array.isArray(parsed) ? parsed : [];
}

/**
 * Writes `entries` atomically: serialise to a `<path>.tmp-<random>` sibling,
 * then `rename()` onto `filePath`. `rename(2)` is atomic on the same
 * filesystem, so a concurrent reader of `filePath` can only ever observe
 * the whole prior file or the whole new one.
 *
 * THE OWNERSHIP CHOKE POINT. `lockContext` — the object `withLock`
 * threads into `fn` — is mandatory, and this function re-proves ownership
 * through it immediately before the `rename`. Enforcing here rather than at
 * each of the five call sites is the whole point of the seam: a future
 * writer cannot SILENTLY omit the guard — a call with no lock context fails
 * loudly instead of writing. (Not "physically cannot": this function is
 * exported for tests and the context is duck-typed on `assertStillHeld`, so
 * a caller determined to bypass it can. The property being bought is that
 * bypassing has to be deliberate and visible, not that it is impossible.)
 *
 * Ordering is deliberate: stage the `.tmp-*` file FIRST, then assert, then
 * `rename`. The residual window is the gap between the assert resolving and
 * the `rename` being issued — a threadpool-to-loop handoff, a microtask
 * drain, and a fresh threadpool dispatch. Sub-millisecond on a healthy host,
 * which this module's whole subject matter is not. For an actual clobber a
 * contender must fit reclaim + acquire + read + serialise + write entirely
 * inside that gap, which is far narrower than "a reclaim slips through" —
 * but it is not zero. Asserting before staging the tmp file would widen the
 * gap to include the serialise-and-write, for no benefit.
 *
 * On any failure — the staged write, the ownership check, or the `rename`
 * itself — the `.tmp-*` sibling is removed before the error propagates, matching `acquireLock`'s and `restoreOrDiscard`'s existing
 * write-failure cleanup discipline. (The module header is explicit that
 * orphaned `.tmp-*` files left by OTHER processes are not this module's
 * concern; that is precisely why this module must not add to them — nothing
 * will ever come along and sweep them up.) A failure of the cleanup itself
 * is swallowed so it can never mask the original error.
 *
 * A MISSING `lockContext` is a programming error, not a runtime condition,
 * so it throws a `TypeError` with a greppable message (matching this
 * module's existing param-validation convention) rather than a
 * `LockLostError` — and never a bare `TypeError: Cannot read properties of
 * undefined`.
 *
 * Exported (alongside `acquireLock`/`releaseLock`/`reclaimStaleLock`/
 * `restoreOrDiscard`/`assertStillHeld`) solely so the test suite can drive
 * the refusal path directly — not part of the documented production API.
 *
 * @param {string} filePath
 * @param {Array<object>} entries
 * @param {{ assertStillHeld: () => Promise<void> }} lockContext
 * @returns {Promise<void>}
 */
export async function writeEntriesAtomic(filePath, entries, lockContext) {
  if (!lockContext || typeof lockContext.assertStillHeld !== 'function') {
    throw new TypeError(
      'writeEntriesAtomic: a lockContext with an assertStillHeld() function is required — every ' +
        'write to the coordination file must re-prove, immediately before its rename, that this ' +
        "caller still holds the fencing lock. Call this from inside withLock's `fn` and " +
        'pass the lockContext it supplies.',
    );
  }

  const tmpPath = `${filePath}.tmp-${randomBytes(8).toString('hex')}`;
  try {
    await fs.writeFile(tmpPath, JSON.stringify(entries), 'utf8');
    await lockContext.assertStillHeld();
    await fs.rename(tmpPath, filePath);
  } catch (error) {
    // Covers the `rename` too, not only the staged write and the ownership
    // check: a failed `rename` (`EACCES` after a mode change, `ENOSPC` on the
    // directory entry, `EROFS`) leaves the staged sibling behind just as
    // surely, and nothing will ever sweep it up.
    await fs.unlink(tmpPath).catch(() => {});
    throw error;
  }
}

/**
 * The records `reapConfirmedDeadRecords` would offer to `isDead` for one
 * ledger, read WITHOUT taking the coordination lock.
 *
 * WHAT THIS IS FOR, AND THE ONE THING IT IS NOT. A beat sweeps three ledgers
 * and almost always finds nothing to reap, so entering three critical sections
 * per beat to discover that triples the beat's lock-collision surface against
 * every other orchestrator on the host — on the host whose contention this
 * skill exists to manage. This read exists so a sweep can CHEAPLY DECIDE
 * WHETHER TO BOTHER: no candidate looks dead, no lock is taken.
 *
 * It is never the authority. The value it returns is a view of the file at
 * some instant with no lock held, so a concurrent writer may be mid-`rename`
 * around it and the very next moment may differ. A caller that acts on this
 * result — reaps, writes, or reports — is acting on an unfenced read. The
 * only legitimate use is as a filter in front of `reapConfirmedDeadRecords`,
 * which re-reads and re-judges everything inside its own critical section; a
 * record that changed in between is judged on what THAT read sees.
 *
 * Selection matches `reapConfirmedDeadRecords`' own candidate set exactly, and
 * must keep doing so or the filter could skip a ledger that had work: reserved
 * rows are excluded from the dibs sweep (they name no process), and a claim
 * sweep sees that type's `claims[]` array, or `[]` when the row is absent or
 * still in the pre-existing scalar shape.
 *
 * A missing file reads as `[]` (`readEntriesRaw`'s own ENOENT tolerance); a
 * corrupt one throws, exactly as the locked path would.
 *
 * @param {string} filePath
 * @param {string | null} [claimType] Unset sweeps the dibs array; set names
 *   the `__claim:<type>__` row whose `claims[]` is swept.
 * @param {number} [now] Reference clock used only to synthesize a legacy
 *   scalar entry's `claimedAt` when merging duplicate claim-type rows via
 *   `normalizeClaims`; defaults to `Date.now()` when unset or non-finite.
 *   Irrelevant to the dibs-array (`claimType === null`) branch.
 * @returns {Promise<Array<object>>}
 */
export async function readLedgerCandidateRecords(filePath, claimType = null, now) {
  const entries = await readEntriesRaw(filePath);

  if (claimType === null) {
    return entries.filter((entry) => !isReservedEntry(entry?.orchestratorId));
  }

  // — merges every duplicate `__claim:<type>__` row the same way
  // `claimCapacity`/`releaseCapacity`/`reserveAdmission` do on read, rather
  // than the old `.find()`-based first-match-only divergence (a ghost living
  // only in a second row was previously invisible to the sweep entirely).
  const referenceNow = isFiniteNumber(now) ? now : Date.now();
  const ledgerId = claimLedgerEntryId(claimType);
  return entries
    .filter((entry) => entry?.orchestratorId === ledgerId)
    .flatMap((entry) => normalizeClaims(entry, referenceNow));
}

/**
 * — removes the records a caller has already judged provably dead from
 * ONE ledger, through this module's ordinary `withLock`/`writeEntriesAtomic`
 * choke point.
 *
 * WHICH LEDGER. `claimType` unset sweeps the dibs array (the top-level
 * entries); `claimType` set sweeps the `claims[]` array inside that type's
 * `__claim:<type>__` row. One call, one ledger, one critical section — the
 * caller sweeping several ledgers in a beat makes several calls, so a failure
 * on one leaves the others already committed and the failed one untouched
 * rather than half-written.
 *
 * WHOSE JUDGEMENT. This function decides nothing about liveness: `isDead` is
 * the caller's predicate, called once per candidate record inside the critical
 * section, and its verdict is applied verbatim. Keeping the rule out of here is
 * what lets the process-correlation logic live in `./reconciliation.mjs`
 * without this module acquiring a dependency on process discovery.
 *
 * RECORD GRANULARITY, NOT ID GRANULARITY. `claims[]` is filtered element by
 * element, so a crash-and-respawn that left two records under one
 * `orchestratorId` (one ghost, one live — `claimCapacity` appends rather than
 * upserting) loses only the ghost. `grantedTotal` is recomputed from the
 * survivors so the aggregate never describes records the ledger no longer
 * holds, and a claim row emptied by the sweep is dropped entirely, matching
 * `claimCapacity`'s own omission of a ledger row with no claims left.
 *
 * RESERVED ROWS ARE NEVER CANDIDATES. A `__claim:<type>__` container, the
 * shared sample, the AIMD ceiling state and the spawn bucket name no process;
 * `isDead` is never consulted for them on the dibs sweep, exactly as
 * `pruneStale` exempts them from age-based pruning.
 *
 * NOTHING TO DO MEANS NO WRITE. With no record judged dead the file is left
 * byte-for-byte alone — a sweep is not a rewrite, so a beat that reaps nothing
 * cannot perturb a concurrent writer's view.
 *
 * `beforeCommit` is an optional hook awaited inside the critical section
 * immediately before the write, provided so a test can inject a
 * `LockTimeoutError`/`LockLostError` at the exact commit point. It is not part
 * of the production contract, and anything it throws propagates unchanged with
 * nothing written.
 *
 * @param {string} filePath
 * @param {{
 *   claimType?: string | null,
 *   isDead: (record: object) => boolean,
 *   beforeCommit?: () => Promise<void> | void,
 *   now?: number,
 * }} params
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<{ reaped: Array<object> }>} The records removed, in ledger
 *   order, by reference — so a caller can log each one against whatever reason
 *   its own predicate reached.
 */
export async function reapConfirmedDeadRecords(
  filePath,
  { claimType = null, isDead, beforeCommit, now },
  options = {},
) {
  if (typeof isDead !== 'function') {
    throw new TypeError('reapConfirmedDeadRecords: isDead must be a function that judges one ledger record');
  }

  return withLock(
    filePath,
    async (lockContext) => {
      const referenceNow = isFiniteNumber(now) ? now : Date.now();
      const entries = await readEntriesRaw(filePath);
      const reaped = [];

      if (claimType === null) {
        const survivors = entries.filter((entry) => {
          if (isReservedEntry(entry?.orchestratorId)) return true;
          if (!isDead(entry)) return true;
          reaped.push(entry);
          return false;
        });

        if (reaped.length === 0) return { reaped };
        if (beforeCommit) await beforeCommit();
        await writeEntriesAtomic(filePath, survivors, lockContext);
        return { reaped };
      }

      const ledgerId = claimLedgerEntryId(claimType);
      // — merges every duplicate `__claim:<type>__` row on read, the
      // same `.filter()` + `.flatMap(normalizeClaims)` pattern `claimCapacity`/
      // `releaseCapacity` use, rather than the old `.findIndex()`-based
      // first-match-only divergence (a ghost living only in a second row was
      // previously invisible to the sweep entirely).
      const matchingLedgerEntries = entries.filter((entry) => entry?.orchestratorId === ledgerId);
      const mergedClaims = matchingLedgerEntries.flatMap((entry) => normalizeClaims(entry, referenceNow));
      // Nothing here carries per-record identity to correlate — there is
      // nothing a liveness verdict could apply to, and it ages out under its
      // own TTL.
      if (mergedClaims.length === 0) return { reaped };

      const survivors = mergedClaims.filter((claim) => {
        if (!isDead(claim)) return true;
        reaped.push(claim);
        return false;
      });

      if (reaped.length === 0) return { reaped };
      if (beforeCommit) await beforeCommit();

      // — the write side now mirrors `claimCapacity`'s/
      // `releaseCapacity`'s own normalization chain exactly: stamp any
      // surviving `legacyMigrated` record to lose its TTL exemption on this
      // write, THEN drop dead zero-count records (must run in this order —
      // see `filterOutZeroCountClaims`'s doc comment), THEN sum the
      // fully-normalized survivors for `grantedTotal`. ALL matching
      // `__claim:<type>__` rows are removed and, when anything survives,
      // exactly ONE consolidated row is spliced back — never the duplicate
      // rows preserved as-is.
      const stampedClaims = stampMigratedClaims(survivors, referenceNow);
      const finalClaims = filterOutZeroCountClaims(stampedClaims);
      const grantedTotal = sumClaimCounts(finalClaims);

      const nextEntries = entries.filter((entry) => entry?.orchestratorId !== ledgerId);
      if (finalClaims.length > 0) {
        nextEntries.push({
          orchestratorId: ledgerId,
          claims: finalClaims,
          grantedTotal,
          declaredAt: referenceNow,
        });
      }

      await writeEntriesAtomic(filePath, nextEntries, lockContext);
      return { reaped };
    },
    options,
  );
}

/**
 * Identifies a reserved pseudo-entry in the dibs array — a general-purpose,
 * reusable naming convention, not a bespoke one-off check:
 * an `orchestratorId` is "reserved" when it both starts AND ends with a
 * double underscore (`__`), e.g. `'__shared-machine-sample__'`. A normal
 * orchestrator id that merely CONTAINS `__` somewhere in the middle (e.g.
 * `'orc__a__b'`) does not match — only a genuine leading+trailing pair does.
 *
 * This predicate is deliberately convention-based rather than literal-based
 * so future reserved entries (e.g. Phase 4's global spawn-rate token bucket)
 * are recognised automatically, without changing this function.
 *
 * @param {string} orchestratorId
 * @returns {boolean}
 */
export function isReservedEntry(orchestratorId) {
  return (
    typeof orchestratorId === 'string' &&
    orchestratorId.length >= 4 &&
    orchestratorId.startsWith('__') &&
    orchestratorId.endsWith('__')
  );
}

/**
 * Normalizes a dibs entry's `agentClasses`/legacy `agentClass` field into a
 * flat array of class-name strings — `[]` when neither is a usable shape.
 * Back-compat seam (Phase 2 review, Medium finding) for entries
 * declared before that fix shipped, which carry only a singular `agentClass`
 * string field rather than an `agentClasses` array. Used by
 * `reconcileMemoryProjectionAdmission`'s retraction path (review) to
 * compute which classes to RETAIN after removing just the refused candidate.
 *
 * Exported (review, Low finding, maintainability) so `cli.mjs` can
 * import this SAME implementation for its own `liveDibs`-summing use inside
 * `reconcileMemoryProjectionAdmission`'s `decide` callback, rather than
 * keeping a second, verbatim-duplicated copy of this function in two files
 * that could silently drift apart.
 *
 * @param {{ agentClasses?: unknown, agentClass?: unknown }} entry
 * @returns {string[]}
 */
export function readEntryAgentClasses(entry) {
  if (Array.isArray(entry?.agentClasses)) {
    return entry.agentClasses.filter((agentClass) => typeof agentClass === 'string');
  }
  if (typeof entry?.agentClass === 'string') {
    return [entry.agentClass];
  }
  return [];
}

/**
 * Excludes entries whose `declaredAt` is older than `livenessThresholdMs`
 * relative to `referenceNow`. Inclusive boundary: an entry exactly at the
 * threshold is still considered live (a "slow but alive" orchestrator, not
 * a crashed one — the false-positive pruning gap the Convergence Analysis
 * flagged).
 *
 * Reserved entries (per `isReservedEntry`) are NEVER pruned here regardless
 * of age — their lifecycle is governed solely by `isSampleFresh`'s
 * (separate, much shorter) freshness window, checked by callers. See the
 * Phase 3 design decision in ./coordination-file.jest.spec.mjs for the
 * rationale for keeping the two windows fully independent.
 *
 * When `livenessThresholdMs` isn't a finite number, no pruning happens.
 *
 * @param {Array<object>} entries
 * @param {number} referenceNow
 * @param {number | undefined} livenessThresholdMs
 * @returns {Array<object>}
 */
function pruneStale(entries, referenceNow, livenessThresholdMs) {
  if (!isFiniteNumber(livenessThresholdMs)) return entries;
  return entries.filter(
    (entry) => isReservedEntry(entry.orchestratorId) || referenceNow - entry.declaredAt <= livenessThresholdMs,
  );
}

/**
 * Upserts an orchestrator's dibs entry (keyed by `orchestratorId`) into the
 * coordination file at `filePath`, pruning entries older than
 * `options.livenessThresholdMs` (relative to `entry.declaredAt`) in the same
 * write. Concurrent callers (same or independent processes) are serialised
 * via an exclusive lock file so no entry is ever lost or corrupted.
 *
 * `declaredAt` vs. `firstDeclaredAt` — two deliberately independent fields,
 * not two names for the same thing: `declaredAt` is refreshed on EVERY call
 * (needed for `pruneStale`'s liveness check — an orchestrator that hasn't
 * beaten in a while must look stale). `firstDeclaredAt` is the timestamp of
 * this `orchestratorId`'s FIRST-EVER declaration, and this upsert PRESERVES
 * it unchanged on every subsequent call for the same `orchestratorId`,
 * regardless of what `entry.firstDeclaredAt` the caller passed in — this
 * function reads whatever entry currently exists for that `orchestratorId`
 * (if any) before upserting, and carries its `firstDeclaredAt` forward. Only
 * a genuinely-new `orchestratorId` (no existing entry) takes the incoming
 * `entry.firstDeclaredAt` (or, failing that, `entry.declaredAt`) as its
 * first-ever value.
 *
 * Critically, "existing entry" here means existing and LIVE, by the exact
 * same liveness test `readDibs` applies at read time — the raw, unpruned
 * entries list is pruned FIRST (relative to `entry.declaredAt`, the same
 * `referenceNow` this write already uses for its own post-upsert prune),
 * and only THEN is it searched for a same-`orchestratorId` entry to carry
 * `firstDeclaredAt` forward from. A prior entry for this `orchestratorId`
 * that has gone stale (past `options.livenessThresholdMs`) but hasn't yet
 * been physically pruned from the file by some other write is therefore
 * treated exactly like "no existing entry at all": this declaration is a
 * fresh arrival, and takes the incoming `entry.firstDeclaredAt` (or,
 * failing that, `entry.declaredAt`) as its first-ever value — never the
 * ancient, stale one. Without this, an orchestrator that goes quiet past
 * the liveness threshold and later comes back could resurrect an
 * arbitrarily old `firstDeclaredAt`, winning dibs priority over a
 * continuously-live incumbent it has no legitimate claim to outrank (see
 * this file's regression test for the reproduction).
 *
 * This split exists because `computeAllowance`/`computePerOrchestratorAllowance`
 * (`./allowance.mjs`) order dibs priority by ascending `firstDeclaredAt` —
 * "earliest-ARRIVING orchestrator claims first". Ordering by `declaredAt`
 * instead would be self-defeating: since `declaredAt` is refreshed every
 * beat, whichever orchestrator's beat is currently running would always look
 * newest (lowest priority) relative to a contender that simply hasn't beaten
 * as recently — under sustained contention this produced a permanent 0/0
 * deadlock for both orchestrators (see this skill's regression test).
 *
 * PID identity: `entry.pid`/`entry.pidStartedAt` are optional and
 * caller-supplied. Ledger identity is never derived here — no ledger write
 * path in this module reads `process.pid` or `process.uptime()`. (The lock
 * payload written by `acquireLock` does read `process.pid`; that is lock
 * OWNERSHIP, a separate concern, and is deliberately exempt from this rule.)
 *
 * The identity these fields carry is NOT the writing process's own. Callers
 * resolve it with `resolveNearestClaudeRootIdentity` (`lib/recon.mjs`), which
 * walks parent-process ancestry from `process.ppid` up to the nearest
 * claude-rooted ancestor, because only claude-rooted pids are visible to the
 * `listAgentProcesses` snapshot the read-side classifier correlates against. A
 * caller that supplies nothing gets an entry governed by TTL alone.
 *
 * Supplied values ride through on the existing whole-`entry` spread below like
 * any other caller-supplied field. Because this is a full-entry upsert rather
 * than a field-level merge, a re-declaration REPLACES them — which is the
 * correct semantics: a respawned process is a new process, and carrying a dead
 * one's pid forward would let the read-side classifier reap a live
 * orchestrator. `readDibs` surfaces them verbatim, uncoerced.
 *
 * @param {string} filePath
 * @param {{ orchestratorId: string, [key: string]: unknown, declaredAt: number, firstDeclaredAt?: number, pid?: number, pidStartedAt?: number }} entry
 * @param {{ livenessThresholdMs?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<void>}
 */
export async function declareDibs(filePath, entry, options = {}) {
  await withLock(
    filePath,
    async (lockContext) => {
      const existing = await readEntriesRaw(filePath);
      const referenceNow = isFiniteNumber(entry.declaredAt) ? entry.declaredAt : Date.now();
      // Prune stale entries BEFORE looking up this orchestratorId's prior
      // entry — see this function's doc comment above for why a stale
      // (unpruned-on-disk) prior entry must not have its `firstDeclaredAt`
      // carried forward.
      const liveExisting = pruneStale(existing, referenceNow, options.livenessThresholdMs);
      const existingSelf = liveExisting.find(
        (existingEntry) => existingEntry.orchestratorId === entry.orchestratorId,
      );
      const withoutSelf = existing.filter((existingEntry) => existingEntry.orchestratorId !== entry.orchestratorId);

      const firstDeclaredAt = isFiniteNumber(existingSelf?.firstDeclaredAt)
        ? existingSelf.firstDeclaredAt
        : isFiniteNumber(entry.firstDeclaredAt)
          ? entry.firstDeclaredAt
          : entry.declaredAt;

      const upserted = [...withoutSelf, { ...entry, firstDeclaredAt }];
      const pruned = pruneStale(upserted, referenceNow, options.livenessThresholdMs);

      // / design decision 1: every declaration also records the id in
      // the sticky ever-opted-in set, in THIS critical section — never a
      // second lock acquisition, so both effects of the beat (the live-entry
      // upsert and the set union) land or fail together.
      //
      // Every beat, not just the first: the union refreshes this id's
      // `lastSeenAt`, which is what keeps a long-running session clear of the
      // `EVER_OPTED_IN_MAX_AGE_MS` sweep that bounds the set's growth.
      //
      // A reserved-shaped `entry.orchestratorId` is SKIPPED rather than
      // rejected. `recordEverOptedIn` throws for one, but `declareDibs` is the
      // documented way reserved pseudo-entries get seeded (this module's own
      // suite seeds the AIMD row through it), so throwing here would be a
      // breaking change. `hasEverOptedIn` therefore answers `false` for a
      // reserved id forever, which is right: reserved rows name no session.
      // The same skip covers a non-string/empty id: `declareDibs` has never
      // validated that field, and starting to throw here would likewise be a
      // breaking change.
      const recordable = isNonEmptyString(entry.orchestratorId) && !isReservedEntry(entry.orchestratorId);
      const withEverOptedIn = recordable
        ? unionEverOptedIn(pruned, entry.orchestratorId, referenceNow).entries
        : pruned;

      await writeEntriesAtomic(filePath, withEverOptedIn, lockContext);
    },
    {
      lockStalenessMs: options.lockStalenessMs,
      lockMaxAttempts: options.lockMaxAttempts,
      lockRetryDelayMs: options.lockRetryDelayMs,
    },
  );
}

/**
 * Reads the live dibs entries from the coordination file at `filePath`,
 * applying the same liveness-threshold exclusion as `declareDibs` at read
 * time — so a stale entry is excluded even before any subsequent write
 * prunes it on disk. Returns `[]` if the file doesn't exist yet.
 *
 * @param {string} filePath
 * @param {number} [now]
 * @param {{ livenessThresholdMs?: number }} [options]
 * @returns {Promise<Array<object>>}
 */
export async function readDibs(filePath, now = Date.now(), options = {}) {
  const entries = await readEntriesRaw(filePath);
  return pruneStale(entries, now, options.livenessThresholdMs);
}

/**
 * PURE. Reads whatever the ever-opted-in row holds — canonical per-id records
 * OR the legacy bare `orchestratorIds: string[]` — into one
 * `Map<id, lastSeenAt>`.
 *
 * This is the single place either shape is understood, so the read path
 * (`hasEverOptedIn`) and the write path (`unionEverOptedIn`) can never drift
 * on what counts as "recorded" — a divergence there would show up as a gate
 * that answers `true` but writes a row that answers `false`, or vice versa.
 *
 * TOLERANT BY DESIGN, never throwing. A non-array field (hand-corrupted, or a
 * torn write), a record that is not an object, a record with no usable `id`:
 * all are skipped, leaving an empty-or-partial set. On the write path that
 * means the row is repaired into canonical shape under the lock; on the read
 * path it means `false`, which is design decision 3's fail-open answer.
 *
 * `fallbackLastSeenAt` dates records that carry no usable timestamp of their
 * own — i.e. every member of a LEGACY row, plus any record whose `lastSeenAt`
 * was corrupted. Write callers pass `now`, so migrating or repairing treats
 * those ids as freshly seen and can never make the very write that migrates
 * them also evict them. Read callers pass anything (`0`); membership does not
 * depend on it.
 *
 * GENERIC OVER THE SET: `setField` names the row field the
 * canonical records live under — `everOptedIn` for the ever-opted-in set,
 * `diskCleanupArmed` for the disk-cleanup-armed set. The legacy bare
 * `orchestratorIds: string[]` shape is read for either set; it is inert for
 * the disk-cleanup-armed set, whose row has only ever been written in the
 * canonical shape, and keeping one reader rather than two is what stops the
 * two sets drifting on what counts as "recorded".
 *
 * @param {object | undefined} row
 * @param {string} setField
 * @param {number} fallbackLastSeenAt
 * @returns {Map<string, number>}
 */
function stickyIdRecords(row, setField, fallbackLastSeenAt) {
  const records = new Map();

  const canonical = Array.isArray(row?.[setField]) ? row[setField] : [];
  for (const record of canonical) {
    if (!isNonEmptyString(record?.id)) continue;
    const lastSeenAt = isFiniteNumber(record.lastSeenAt) ? record.lastSeenAt : fallbackLastSeenAt;
    const known = records.get(record.id);
    // A duplicated id (only reachable from a hand-edited file) collapses to
    // its NEWEST timestamp — never its oldest, which would make a live id
    // look evictable.
    if (known === undefined || lastSeenAt > known) records.set(record.id, lastSeenAt);
  }

  const legacy = Array.isArray(row?.orchestratorIds) ? row.orchestratorIds : [];
  for (const id of legacy) {
    if (!isNonEmptyString(id)) continue;
    if (!records.has(id)) records.set(id, fallbackLastSeenAt);
  }

  return records;
}

/**
 * PURE. Applies both bounds to the set, with the id this write touched held
 * exempt from each of them.
 *
 * ORDER MATTERS: age first, then the count cap, so the cap only ever has to
 * deal with what genuinely is recent. Both evict strictly oldest-first, and
 * `touchedId` is lifted out before either runs — so the answer to "can a
 * live, actively-beating session be evicted by its own write?" is no, by
 * construction rather than by arithmetic that happens to work out. That is
 * the property the review asked for; it is the reason `lastSeenAt` is per-id
 * at all.
 *
 * The age boundary is exclusive-at-the-horizon (a record exactly
 * `EVER_OPTED_IN_MAX_AGE_MS` old is evicted). A `lastSeenAt` in the future
 * (clock skew between orchestrators) is kept, not evicted — "I cannot date
 * this" must never resolve to "so drop it" for a set whose whole job is to
 * stay sticky.
 *
 * @param {Map<string, number>} records
 * @param {string} touchedId Present in `records`, dated `now`.
 * @param {number} now
 * @returns {Array<{ id: string, lastSeenAt: number }>}
 */
function boundStickyIdRecords(records, touchedId, now) {
  const survivors = [...records]
    .filter(([id]) => id !== touchedId)
    .map(([id, lastSeenAt]) => ({ id, lastSeenAt }))
    .filter(({ lastSeenAt }) => now - lastSeenAt < EVER_OPTED_IN_MAX_AGE_MS)
    .sort((left, right) => right.lastSeenAt - left.lastSeenAt)
    .slice(0, EVER_OPTED_IN_MAX_RECORDS - 1);

  return [{ id: touchedId, lastSeenAt: records.get(touchedId) }, ...survivors];
}

/**
 * PURE. Returns `entries` with `orchestratorId` unioned into the single
 * `EVER_OPTED_IN_RESERVED_ID` row — its `lastSeenAt` refreshed to `now`
 * whether or not it was already there — plus whether THIS call added it for
 * the first time.
 *
 * Deliberately pure and separate from any I/O so both callers
 * (`recordEverOptedIn` and `declareDibs`) apply it to the entries list they
 * have ALREADY read inside their own critical section — the union can then
 * never be computed against a snapshot taken outside the lock, which is the
 * lost-update shape this set exists to survive.
 *
 * A REPEAT opt-in is NOT a no-op on disk any more (review). It returns
 * `added: false` exactly as before — that flag still means "newly recorded",
 * which is what callers report — but it still rewrites the row, because a
 * repeat opt-in is precisely the event that proves the id is still live and
 * so is exactly when its `lastSeenAt` must move. Skipping the write for
 * repeats would leave every long-running session dated at its FIRST beat and
 * hand it straight to the age sweep.
 *
 * A pre-existing row in the legacy `orchestratorIds: string[]` shape, or with
 * a corrupt (non-array) set field, is migrated/repaired into the canonical
 * shape here rather than throwing or being discarded: this is a write path
 * with a lock held, so repairing is strictly better than propagating either.
 *
 * GENERIC OVER THE SET: `set` is one of the frozen
 * `{ reservedId, setField }` pairs declared at the top of this file. The two
 * sets are otherwise governed identically — same bounds, same repair
 * behaviour, same lost-update protection — and sharing this function is what
 * guarantees that stays true. It does NOT make them interchangeable: each
 * caller names exactly one set, and `declareDibs` names only
 * `EVER_OPTED_IN_SET`.
 *
 * @param {Array<object>} entries
 * @param {{ reservedId: string, setField: string }} set
 * @param {string} orchestratorId Caller-validated: non-empty and NOT reserved.
 * @param {number} now
 * @returns {{ entries: Array<object>, added: boolean }}
 */
function unionStickyIdSet(entries, set, orchestratorId, now) {
  const existingRow = entries.find((entry) => entry?.orchestratorId === set.reservedId);
  const records = stickyIdRecords(existingRow, set.setField, now);

  const added = !records.has(orchestratorId);
  records.set(orchestratorId, now);

  // `filter` rather than a splice, so duplicate reserved rows produced by an
  // earlier corruption collapse back to exactly one on the next write.
  const withoutRow = entries.filter((entry) => entry?.orchestratorId !== set.reservedId);
  const nextRow = {
    orchestratorId: set.reservedId,
    [set.setField]: boundStickyIdRecords(records, orchestratorId, now),
    declaredAt: now,
  };

  return { entries: [...withoutRow, nextRow], added };
}

/**
 * The ever-opted-in set's binding of `unionStickyIdSet`. Kept as a named
 * function rather than inlined at its two call sites (`recordEverOptedIn` and
 * `declareDibs`) so that `declareDibs` names the ever-opted-in set and ONLY
 * the ever-opted-in set — the separation the `DISK_CLEANUP_ARMED_RESERVED_ID`
 * doc block explains.
 *
 * @param {Array<object>} entries
 * @param {string} orchestratorId
 * @param {number} now
 * @returns {{ entries: Array<object>, added: boolean }}
 */
function unionEverOptedIn(entries, orchestratorId, now) {
  return unionStickyIdSet(entries, EVER_OPTED_IN_SET, orchestratorId, now);
}

/**
 * Records `orchestratorId` into the sticky ever-opted-in set
 * (by design decision 1), so `hasEverOptedIn` answers `true` for it for the
 * remaining life of the coordination file — regardless of whether that
 * orchestrator still holds a live dibs entry.
 *
 * The whole read-union-write happens inside ONE `withLock` critical section
 * (this module's existing lock, not a second mechanism), so two sessions
 * opting in for the first time simultaneously union rather than clobber.
 *
 * RESERVED-ID COLLISION IS REJECTED, NEVER NORMALIZED. A `__…__`-shaped id
 * throws a `TypeError`, as does a non-string or empty one. Normalizing (say,
 * stripping the underscores) was rejected because it maps distinct real ids
 * onto one another — one session's opted-in state leaking onto an unrelated
 * one, which for a gate that fails CLOSED once opted in is a correctness bug
 * in both directions. Validation runs BEFORE the lock is taken, so a rejected
 * call never creates the coordination file or its `.lock` sibling.
 *
 * IDEMPOTENT IN THE SET, NOT IN THE TIMESTAMP. A repeat call for an
 * already-recorded id returns `false` and leaves the set membership exactly
 * as it was, but DOES write — refreshing that id's `lastSeenAt` to `now`, so
 * a session that keeps beating keeps proving it is alive and stays clear of
 * the `EVER_OPTED_IN_MAX_AGE_MS` sweep.
 *
 * @param {string} filePath
 * @param {string} orchestratorId
 * @param {{ now?: number }} [clock]
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<boolean>} `true` when this call newly added the id,
 *   `false` when it was already recorded (membership unchanged; `lastSeenAt`
 *   still refreshed).
 */
export async function recordEverOptedIn(filePath, orchestratorId, { now = Date.now() } = {}, options = {}) {
  if (!isNonEmptyString(orchestratorId)) {
    throw new TypeError(
      'recordEverOptedIn: orchestratorId must be a non-empty string — got ' +
        `${typeof orchestratorId === 'string' ? '""' : String(orchestratorId)}.`,
    );
  }
  if (isReservedEntry(orchestratorId)) {
    throw new TypeError(
      `recordEverOptedIn: refusing reserved-shaped orchestratorId "${orchestratorId}" — ids that both ` +
        'start and end with "__" name this module\'s own reserved pseudo-entries (see isReservedEntry). ' +
        'It is rejected rather than normalized because stripping or escaping the underscores would map ' +
        'distinct sessions onto one shared id, leaking one session\'s opted-in state onto another.',
    );
  }

  return withLock(
    filePath,
    async (lockContext) => {
      const existing = await readEntriesRaw(filePath);
      const { entries, added } = unionEverOptedIn(existing, orchestratorId, now);

      // Unconditional, NOT `if (added)`. The repeat case is the whole reason
      // the write exists now: it is what moves `lastSeenAt` forward and keeps
      // a long-running session out of the age sweep. It also migrates a legacy
      // or corrupt row into canonical shape on a beat that adds no new id.
      await writeEntriesAtomic(filePath, entries, lockContext);

      return added;
    },
    {
      lockStalenessMs: options.lockStalenessMs,
      lockMaxAttempts: options.lockMaxAttempts,
      lockRetryDelayMs: options.lockRetryDelayMs,
    },
  );
}

/**
 * Has `orchestratorId` EVER declared dibs (design decision 1)?
 *
 * A cheap, LOCK-FREE, plain read. The PreToolUse hook calls this on every
 * gated tool call, so it must never shell out and never contend for the
 * coordination lock — contending would stack this module's 10-20s retry
 * ceiling onto every spawn, on the host whose contention this skill exists to
 * manage. The answer it returns is therefore a view of the file at some
 * instant with no lock held. That is sound despite the set no longer being
 * strictly append-only (review added the age + count bounds): every
 * write lands via `writeEntriesAtomic`'s `rename`, so a reader sees one whole
 * version of the file or another, never a half-applied eviction; and the only
 * ids a write can remove are ones untouched for `EVER_OPTED_IN_MAX_AGE_MS`,
 * which a still-beating session's id can never be, since every beat refreshes
 * its `lastSeenAt`.
 *
 * READS BOTH SHAPES: the canonical per-id `everOptedIn` records and the
 * legacy bare `orchestratorIds: string[]` row, via
 * `stickyIdRecords(row, EVER_OPTED_IN_SET.setField, 0)` — the same
 * normalisation the write path applies through its `unionEverOptedIn` /
 * `unionStickyIdSet` wrapper (which the read path itself never calls) — so
 * the two can never disagree about membership. (This sentence named
 * `everOptedInRecords` until Phase 5's doc-drift pass: that function
 * DID exist when this row was introduced, in `569c29c1`, and was later
 * generalized into `stickyIdRecords` when the disk-cleanup-armed set reused
 * it — so a reader grepping the current tree found nothing under the old
 * name.) Timestamps are irrelevant here — this asks a
 * membership question only, and deliberately does NOT apply the age horizon
 * at read time: ageing is a write-time sweep, and re-deriving it here would
 * make the hook's answer depend on a clock it has no reason to trust.
 *
 * FAILS OPEN, by design decision 3's not-opted-in tier: a missing file, an
 * unreadable one, corrupt or truncated JSON, a non-array top level, a
 * non-array set field, and a bad `orchestratorId` all resolve `false` and
 * NEVER throw. "No rule has been established for this session" is the correct
 * reading of every one of those states, and a read must never be the thing
 * that blocks a tool call. It also never creates `filePath` (nor a `.lock`
 * sibling): a session that never touches ARM leaves no residue at all.
 *
 * @param {string} filePath
 * @param {string} orchestratorId
 * @param {{ now?: number }} [_clock] Accepted for positional signature
 *   symmetry with `recordEverOptedIn`, and deliberately UNREAD: ageing is a
 *   write-time sweep (see above), so there is no clock-dependent answer for
 *   this path to give. Named with a leading underscore so that stays visible
 *   at the call site rather than looking like an oversight.
 * @param {object} [_options] Likewise symmetry only — this path takes no lock,
 *   so it has no lock knobs to honour.
 * @returns {Promise<boolean>}
 */
export async function hasEverOptedIn(filePath, orchestratorId, _clock = {}, _options = {}) {
  if (!isNonEmptyString(orchestratorId) || isReservedEntry(orchestratorId)) return false;

  try {
    const entries = await readEntriesRaw(filePath);
    const row = entries.find((entry) => entry?.orchestratorId === EVER_OPTED_IN_RESERVED_ID);
    // Fallback timestamp `0` is arbitrary and unused: membership, not recency,
    // is the question this path answers.
    return stickyIdRecords(row, EVER_OPTED_IN_SET.setField, 0).has(orchestratorId);
  } catch {
    return false;
  }
}

/**
 * Arms the disk-axis temp sweep for `orchestratorId`, by
 * recording it into the sticky `DISK_CLEANUP_ARMED_RESERVED_ID` set.
 *
 * THIS IS THE ONLY WRITER OF THAT SET, BY DESIGN. It is reached from exactly
 * one place — `cli.mjs`'s dedicated `--enable-disk-cleanup` beat, which does
 * nothing else — so arming the only recursively-deleting code path in this
 * skill is always a separate, deliberate act by an operator or orchestrator,
 * never a side effect of declaring capacity. See
 * `DISK_CLEANUP_ARMED_RESERVED_ID`'s doc block for why reusing the
 * ever-opted-in set would have inverted the default.
 *
 * Everything else — the one-critical-section read-union-write, the
 * reserved-id rejection, the idempotent-in-the-set / refreshed-in-the-timestamp
 * semantics, the age and count bounds — is identical to `recordEverOptedIn`,
 * because both delegate to `unionStickyIdSet`.
 *
 * @param {string} filePath
 * @param {string} orchestratorId
 * @param {{ now?: number }} [clock]
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<boolean>} `true` when this call newly armed the id.
 */
export async function recordDiskCleanupArmed(filePath, orchestratorId, { now = Date.now() } = {}, options = {}) {
  if (!isNonEmptyString(orchestratorId)) {
    throw new TypeError(
      'recordDiskCleanupArmed: orchestratorId must be a non-empty string — got ' +
        `${typeof orchestratorId === 'string' ? '""' : String(orchestratorId)}.`,
    );
  }
  if (isReservedEntry(orchestratorId)) {
    throw new TypeError(
      `recordDiskCleanupArmed: refusing reserved-shaped orchestratorId "${orchestratorId}" — ids that both ` +
        'start and end with "__" name this module\'s own reserved pseudo-entries (see isReservedEntry). ' +
        'Rejected rather than normalized for the same reason recordEverOptedIn rejects it: stripping the ' +
        'underscores would map distinct sessions onto one shared id, and here that would arm one session\'s ' +
        'recursive temp sweep off another session\'s opt-in.',
    );
  }

  return withLock(
    filePath,
    async (lockContext) => {
      const existing = await readEntriesRaw(filePath);
      const { entries, added } = unionStickyIdSet(existing, DISK_CLEANUP_ARMED_SET, orchestratorId, now);
      await writeEntriesAtomic(filePath, entries, lockContext);
      return added;
    },
    {
      lockStalenessMs: options.lockStalenessMs,
      lockMaxAttempts: options.lockMaxAttempts,
      lockRetryDelayMs: options.lockRetryDelayMs,
    },
  );
}

/**
 * Is the disk-axis temp sweep currently standing down (Phase 3 review,
 * Medium)?
 *
 * LOCK-FREE and on the beat path, like `hasDiskCleanupArmed`, and FAILS OPEN
 * in the only direction available to it: every degraded state — missing file,
 * unreadable bytes, corrupt JSON, absent row, a non-finite or hostile
 * `suppressedUntil` — resolves `false`, "not suppressed". That is safe by
 * construction because this value can only ever DELAY a sweep the three real
 * gates have already allowed; it can never authorise one. Failing the other
 * way would let a corrupt byte silently disable disk reclamation on a filling
 * host, which is the worse failure by a wide margin.
 *
 * A `suppressedUntil` in the FUTURE beyond the cooldown horizon (clock skew
 * between orchestrators, or a hand-edited file) is clamped rather than
 * honoured, so no single bad write can stand the sweep down indefinitely.
 *
 * @param {string} filePath
 * @param {number} now Beat-clock instant, i.e. `cli.mjs`'s `resolveNow()`.
 * @param {number} maxHorizonMs The cooldown constant, used as the clamp.
 * @returns {Promise<boolean>}
 */
export async function isDiskCleanupSuppressed(filePath, now, maxHorizonMs) {
  if (!isFiniteNumber(now) || !isFiniteNumber(maxHorizonMs)) return false;

  try {
    const entries = await readEntriesRaw(filePath);
    const row = entries.find((entry) => entry?.orchestratorId === DISK_CLEANUP_COOLDOWN_RESERVED_ID);
    const suppressedUntil = row?.suppressedUntil;
    if (!isFiniteNumber(suppressedUntil)) return false;
    return suppressedUntil > now && suppressedUntil - now <= maxHorizonMs;
  } catch {
    return false;
  }
}

/**
 * Stands the disk-axis temp sweep down until `suppressedUntil` (Phase 3
 * review, Medium) — written by `cli.mjs` after, and only after, a sweep that
 * ran to completion and found nothing worth doing.
 *
 * ONE ROW, REPLACED WHOLE, under the shared lock — the same discipline
 * `writeAimdCeilingState` uses for the other piece of reserved state in this
 * file, and for the same reason: a read-modify-write computed outside the
 * critical section is the lost-update shape this file exists to survive.
 *
 * Deliberately NOT called on any other outcome. A productive sweep, a
 * budget-exhausted sweep, a candidate-capped sweep and a thrown sweep all
 * leave the row alone: the first has nothing to stand down from, and the rest
 * mean "there is more work queued, come back next beat".
 *
 * @param {string} filePath
 * @param {{ suppressedUntil: number }} state
 * @param {{ now?: number }} [clock]
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<void>}
 */
export async function recordDiskCleanupCooldown(filePath, { suppressedUntil }, { now = Date.now() } = {}, options = {}) {
  if (!isFiniteNumber(suppressedUntil)) {
    throw new TypeError(
      `recordDiskCleanupCooldown: suppressedUntil must be a finite number (received ${String(suppressedUntil)})`,
    );
  }

  await withLock(
    filePath,
    async (lockContext) => {
      const existing = await readEntriesRaw(filePath);
      const withoutRow = existing.filter((entry) => entry?.orchestratorId !== DISK_CLEANUP_COOLDOWN_RESERVED_ID);
      await writeEntriesAtomic(
        filePath,
        [...withoutRow, { orchestratorId: DISK_CLEANUP_COOLDOWN_RESERVED_ID, suppressedUntil, declaredAt: now }],
        lockContext,
      );
    },
    {
      lockStalenessMs: options.lockStalenessMs,
      lockMaxAttempts: options.lockMaxAttempts,
      lockRetryDelayMs: options.lockRetryDelayMs,
    },
  );
}

/**
 * PURE. Returns `entries` with `orchestratorId` REMOVED from the named sticky
 * set, plus whether it was there to remove (Phase 3 review, Medium).
 *
 * The mirror of `unionStickyIdSet`, and deliberately written against the same
 * `stickyIdRecords` reader so the two can never disagree about what counts as
 * "recorded" — including the legacy bare `orchestratorIds: string[]` shape,
 * whose members are therefore removable too rather than being silently
 * immortal.
 *
 * WHEN THE SET EMPTIES, THE ROW GOES. Disarming the last armed orchestrator
 * leaves no residue at all: "row absent" is exactly the cold, never-armed
 * state the gate already reads as `false`, so there is one such state rather
 * than two. An absent row is likewise a no-op, returning the caller's
 * `entries` untouched — no write is manufactured for a disarm with nothing to
 * do.
 *
 * The surviving records are bounded on the same two axes as a union
 * (`EVER_OPTED_IN_MAX_AGE_MS`, then `EVER_OPTED_IN_MAX_RECORDS`, oldest-first)
 * so a rewrite prompted by a removal cannot leave the row in a state a union
 * would have refused to write. There is no exempt `touchedId` here, because
 * the id this call touched is the one being taken OUT.
 *
 * @param {Array<object>} entries
 * @param {{ reservedId: string, setField: string }} set
 * @param {string} orchestratorId Caller-validated: non-empty and NOT reserved.
 * @param {number} now
 * @returns {{ entries: Array<object>, removed: boolean }}
 */
function removeFromStickyIdSet(entries, set, orchestratorId, now) {
  const existingRow = entries.find((entry) => entry?.orchestratorId === set.reservedId);
  if (existingRow === undefined) return { entries, removed: false };

  const records = stickyIdRecords(existingRow, set.setField, now);
  const removed = records.delete(orchestratorId);

  // `filter` rather than a splice, for the same reason `unionStickyIdSet` uses
  // one: duplicate reserved rows from an earlier corruption collapse back to
  // exactly one (or to none) on this write.
  const withoutRow = entries.filter((entry) => entry?.orchestratorId !== set.reservedId);
  const survivors = [...records]
    .map(([id, lastSeenAt]) => ({ id, lastSeenAt }))
    .filter(({ lastSeenAt }) => now - lastSeenAt < EVER_OPTED_IN_MAX_AGE_MS)
    .sort((left, right) => right.lastSeenAt - left.lastSeenAt)
    .slice(0, EVER_OPTED_IN_MAX_RECORDS);

  if (survivors.length === 0) return { entries: withoutRow, removed };

  return {
    entries: [...withoutRow, { orchestratorId: set.reservedId, [set.setField]: survivors, declaredAt: now }],
    removed,
  };
}

/**
 * DISARMS the disk-axis temp sweep for `orchestratorId` (Phase 3
 * review, Medium) — the counterpart to `recordDiskCleanupArmed`, and the ONLY
 * remover of that set.
 *
 * WHY THIS HAS TO EXIST. Without it, arming the only recursively-deleting
 * capability in this skill was a one-way door for up to
 * `EVER_OPTED_IN_MAX_AGE_MS` (30 days) — the horizon `hasDiskCleanupArmed`
 * applies at READ time, refreshed by every subsequent `--enable-disk-cleanup`
 * call. Expiry is a backstop, not a disarm: half a beat-month is far too long
 * to wait for a capability an operator wants stopped now, and the only other
 * recourses were editing the
 * shared coordination file by hand — racing every concurrent beat's read — or
 * passing `--advise-only` on every later beat, which suppresses far more than
 * the sweep. A capability that can be armed must be disarmable by the same
 * kind of deliberate, single-purpose act that armed it.
 *
 * IDEMPOTENT AND SAFE TO OVER-CALL. Disarming an id that is not armed, or a
 * file with no armed row at all, succeeds and returns `false`. That matters
 * more in this direction than in the arming one: an operator reaching for this
 * is trying to make a destructive capability stop, and must never be handed an
 * error that reads as "still armed" when it is not.
 *
 * Everything else — the reserved-id rejection, the one-critical-section
 * read-modify-write, the bounds — mirrors `recordDiskCleanupArmed`.
 *
 * @param {string} filePath
 * @param {string} orchestratorId
 * @param {{ now?: number }} [clock]
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<boolean>} `true` when this call actually removed an armed id.
 */
export async function clearDiskCleanupArmed(filePath, orchestratorId, { now = Date.now() } = {}, options = {}) {
  if (!isNonEmptyString(orchestratorId)) {
    throw new TypeError(
      'clearDiskCleanupArmed: orchestratorId must be a non-empty string — got ' +
        `${typeof orchestratorId === 'string' ? '""' : String(orchestratorId)}.`,
    );
  }
  if (isReservedEntry(orchestratorId)) {
    throw new TypeError(
      `clearDiskCleanupArmed: refusing reserved-shaped orchestratorId "${orchestratorId}" — ids that both ` +
        'start and end with "__" name this module\'s own reserved pseudo-entries (see isReservedEntry). ' +
        'Rejected rather than normalized, exactly as recordDiskCleanupArmed rejects it: a normalizing disarm ' +
        'would let one session silently disarm another, which is a capability change nobody asked for even ' +
        'though this direction happens to be the safe one.',
    );
  }

  return withLock(
    filePath,
    async (lockContext) => {
      const existing = await readEntriesRaw(filePath);
      const { entries, removed } = removeFromStickyIdSet(existing, DISK_CLEANUP_ARMED_SET, orchestratorId, now);
      // Only write when something actually changed: a disarm of an unarmed id
      // must not rewrite a file every other beat is reading.
      if (removed) await writeEntriesAtomic(filePath, entries, lockContext);
      return removed;
    },
    {
      lockStalenessMs: options.lockStalenessMs,
      lockMaxAttempts: options.lockMaxAttempts,
      lockRetryDelayMs: options.lockRetryDelayMs,
    },
  );
}

/**
 * Has `orchestratorId` explicitly armed the disk-axis temp sweep
 * (Phase 3)?
 *
 * A cheap, LOCK-FREE, plain read on the same footing as `hasEverOptedIn`, and
 * for the same reason: it is consulted on the beat path, which must never
 * contend for the coordination lock.
 *
 * FAILS CLOSED IN THE ONLY DIRECTION THAT MATTERS. Every degraded state — a
 * missing file, unreadable bytes, corrupt JSON, a bad `orchestratorId` —
 * resolves `false`, i.e. "not armed", i.e. "delete nothing". For the
 * ever-opted-in gate `false` is called failing OPEN (it allows a tool call);
 * here the identical answer is the conservative one, because what it withholds
 * is a recursive removal.
 *
 * THE AGE HORIZON IS APPLIED HERE, AT READ TIME — the one place this function
 * deliberately DIVERGES from `hasEverOptedIn` (pre-PR review, High #2).
 * That sibling answers a pure membership question and leaves ageing to the
 * write-time sweep, which is correct for it: `declareDibs` rewrites the
 * ever-opted-in row on every single beat, so its records are continuously
 * re-bounded and a stale one cannot survive an active session. NOTHING ever
 * rewrites the armed row except `recordDiskCleanupArmed`/
 * `clearDiskCleanupArmed` themselves. Without a read-time horizon an armed id
 * therefore stayed armed FOREVER — a row dated 1970 still read as armed —
 * while two docstrings claimed a 30-day bound. The horizon is applied rather
 * than the claim retracted because for THIS set the bound is the conservative
 * direction: what expiry withholds is a recursive deletion, and re-arming is
 * one deliberate command away.
 *
 * A record with no usable timestamp (the legacy `orchestratorIds` array shape,
 * or a non-finite `lastSeenAt`) falls back to `0` and therefore reads as
 * EXPIRED — "I cannot date this arming" resolving to "delete nothing", the
 * same direction every other uncertainty on this path resolves.
 *
 * @param {string} filePath
 * @param {string} orchestratorId
 * @param {{ now?: number }} [clock] Beat clock, i.e. `cli.mjs`'s `resolveNow()`
 *   — the same clock `recordDiskCleanupArmed` stamps `lastSeenAt` with.
 * @returns {Promise<boolean>}
 */
export async function hasDiskCleanupArmed(filePath, orchestratorId, { now = Date.now() } = {}) {
  if (!isNonEmptyString(orchestratorId) || isReservedEntry(orchestratorId)) return false;
  if (!isFiniteNumber(now)) return false;

  try {
    const entries = await readEntriesRaw(filePath);
    const row = entries.find((entry) => entry?.orchestratorId === DISK_CLEANUP_ARMED_RESERVED_ID);
    const lastSeenAt = stickyIdRecords(row, DISK_CLEANUP_ARMED_SET.setField, 0).get(orchestratorId);
    if (!isFiniteNumber(lastSeenAt)) return false;
    // Exclusive at the horizon, matching `boundStickyIdRecords`' own write-time
    // boundary, and a FUTURE `lastSeenAt` (clock skew) stays armed rather than
    // being read as expired — `now - lastSeenAt` is then negative.
    return now - lastSeenAt < EVER_OPTED_IN_MAX_AGE_MS;
  } catch {
    return false;
  }
}

/**
 * Pure freshness-window check for a stored sample, used to debounce recon
 * sampling. Inclusive boundary: a sample taken exactly `freshnessWindowMs`
 * ago is still fresh.
 *
 * @param {{ sampledAt: number } | null | undefined} storedSample
 * @param {number} now
 * @param {number} freshnessWindowMs
 * @returns {boolean}
 */
export function isSampleFresh(storedSample, now, freshnessWindowMs) {
  const sampledAt = storedSample?.sampledAt;
  if (!isFiniteNumber(sampledAt)) return false;
  return now - sampledAt <= freshnessWindowMs;
}

/**
 * Reserved-entry id for a per-`type` claim ledger — mirrors
 * `GLOBAL_SPAWN_BUCKET_RESERVED_ID`'s convention exactly: `__claim:<type>__`
 * both starts and ends with `__`, so `isReservedEntry` recognises it and
 * `pruneStale` exempts it from liveness pruning unconditionally, the same
 * way the global spawn-rate bucket entry is exempted — a claim ledger must
 * never silently evaporate just because no orchestrator has beaten recently.
 *
 * Only this outer, top-level entry id is reserved. Values stored INSIDE its
 * `claims[]` array (each claim's own `orchestratorId`, attributing that
 * sub-record to whichever orchestrator made it — see `normalizeClaims`
 * below) are ordinary orchestrator ids, even when one happens to look
 * reserved (e.g. `'__foo__'`) — `isReservedEntry`/`pruneStale` never scan
 * INTO `claims[]`, only over the top-level entries array, so such a value
 * can never be misidentified as a top-level reserved entry in its own
 * right.
 *
 * `pruneStale`'s liveness exemption above and `claimCapacity`/
 * `releaseCapacity`'s write-time omission of an emptied entry (Phase
 * 4 — see the write branches in each function) are two independent
 * mechanisms, not one. `pruneStale` exemption means a *live* (non-empty)
 * `__claim:<type>__` entry never expires purely from beat-gap staleness.
 * Write-time omission means that when `releaseCapacity` (or a corner-case
 * `claimCapacity` legacy-migration write) reduces an entry's `claims[]` to
 * empty, the caller simply does not write that now-empty entry back at
 * all — rather than persisting it as `{claims: [], grantedTotal: 0}`,
 * which `pruneStale`'s unconditional exemption would otherwise let survive
 * on disk forever (the one dimension of this file that only ever grew).
 * This fix changes nothing about `pruneStale` itself.
 *
 * @param {string} type
 * @returns {string}
 */
function claimLedgerEntryId(type) {
  return `__claim:${type}__`;
}

/**
 * Sentinel `orchestratorId` used to attribute a claim record migrated from
 * the OLD, pre-existing scalar ledger shape (`{ orchestratorId, grantedTotal,
 * declaredAt }`, no `claims` array) — see `normalizeClaims`. That old shape
 * carried no per-holder attribution at all, so its accumulated total cannot
 * be honestly assigned to any real orchestrator; it is folded into `claims[]`
 * under this sentinel instead, purely so the total is never silently lost
 * (still counted in `alreadyGrantedTotal`/`grantedTotal` aggregates) while
 * being explicit that no live orchestrator can selectively `--release`
 * against it.
 *
 * @type {string}
 */
const LEGACY_UNATTRIBUTED_CLAIM_HOLDER_ID = '__legacy-unattributed-claim__';

/**
 * Structural validity check for a single `claims[]` array element — used by
 * `normalizeClaims` to drop malformed entries (`null`, `{}`, missing/wrong-
 * typed fields) at the point the array is first assembled/validated, rather
 * than letting them flow through to a downstream consumer (LOW finding,
 * round-3 review: the no-TTL path in `pruneExpiredClaims` returns `claims`
 * untouched, so a junk element that survived normalization would otherwise
 * get durably persisted the next time this ledger entry is written).
 *
 * `count` is validated via `isNonNegativeInteger` (round-4 review, MEDIUM
 * finding), not the looser `isFiniteNumber` this check originally used —
 * `isFiniteNumber` alone let a corrupt/hand-edited element with a negative
 * or non-integer `count` (e.g. `count: -1000`) survive normalization, flow
 * into `sumClaimCounts`, and inflate `remaining` in `claimCapacity`'s
 * `remaining = max(0, availableCapacity - alreadyGrantedTotal)` math — a
 * massively over-sized grant on the next `--claim` call. Not reachable
 * through either normal write path (both only ever emit non-negative
 * integer counts), but this is defense-in-depth against a corrupt file, and
 * `isNonNegativeInteger` is already the established helper for exactly this
 * shape of validation elsewhere in this module (e.g. `releaseCount`).
 *
 * @param {unknown} claim
 * @returns {claim is { orchestratorId: string, count: number, claimedAt: number, legacyMigrated?: boolean }}
 */
function isValidClaimRecord(claim) {
  return (
    claim !== null &&
    typeof claim === 'object' &&
    isNonEmptyString(claim.orchestratorId) &&
    isNonNegativeInteger(claim.count) &&
    isFiniteNumber(claim.claimedAt)
  );
}

/**
 * — attaches the CALLER-SUPPLIED process identity (`pid`,
 * `pidStartedAt`) to a freshly-built claim record.
 *
 * Both fields are OPTIONAL and purely ADDITIVE, and both arrive ALREADY
 * RESOLVED from the caller rather than being derived here.
 *
 * STANDING RULE: ledger identity must never be the writing process's own —
 * this module's ledger write path must not consult `process.pid`,
 * `process.uptime()`, or any other process-identity API, or a claim written on
 * behalf of some other process would be stamped with this one's identity. (The
 * lock payload written by `acquireLock` does read `process.pid`; that is lock
 * OWNERSHIP, a separate concern, and is deliberately exempt.)
 *
 * That is not a stylistic preference. The identity these fields carry is the
 * nearest claude-rooted ANCESTOR of the writing process, because only
 * claude-rooted pids are visible to `listAgentProcesses` — the snapshot the
 * read-side classifier correlates against. Resolving it belongs to the caller,
 * via `resolveNearestClaudeRootIdentity` (`lib/recon.mjs`). A caller that
 * supplies nothing gets a record governed by TTL alone.
 *
 * Absent means ABSENT: an omitted (or `undefined`) value produces a record
 * with no such own property at all — never `pid: undefined`, never `null`,
 * never `NaN`, never a synthesized placeholder. The read-side classifier
 * treats a record with no `pid` as *unknown* and defers it to the pre-existing
 * TTL; a placeholder would instead route it to a confident (and wrong)
 * liveness verdict. Values that ARE supplied are persisted verbatim, with no
 * coercion or validation — classification is the classifier's job, not this
 * module's.
 *
 * @param {{ orchestratorId: string, count: number, claimedAt: number }} record
 * @param {number | undefined} pid
 * @param {number | undefined} pidStartedAt
 * @returns {{ orchestratorId: string, count: number, claimedAt: number, pid?: number, pidStartedAt?: number }}
 */
function withPidIdentity(record, pid, pidStartedAt) {
  return {
    ...record,
    ...(pid === undefined ? {} : { pid }),
    ...(pidStartedAt === undefined ? {} : { pidStartedAt }),
  };
}

/**
 * Normalizes a claim ledger entry (as read from disk) into a `claims[]`
 * array, regardless of whether it's already in the new
 * per-claim shape or still the OLD scalar shape written by pre-migration
 * code (`{ orchestratorId, grantedTotal, declaredAt }`, no `claims` array).
 * Never throws on either shape, and never silently drops an old scalar
 * total — it's folded into the returned array as a single synthetic record
 * under `LEGACY_UNATTRIBUTED_CLAIM_HOLDER_ID`.
 *
 * Already-array-shaped input is filtered through `isValidClaimRecord`
 * (round-3 review LOW finding) — a malformed element (`null`, `{}`, or
 * anything missing a well-formed `orchestratorId`/`count`/`claimedAt`) never
 * survives this function, regardless of which downstream path (TTL-enabled
 * or not) later processes the result.
 *
 * **Mixed-version ROLLBACK direction (round-3 review, MEDIUM finding — this
 * module's forward-compat direction, "old code correctly reads a new-shape
 * entry", was already documented; this is the reverse):** an OLD, pre-existing
 * writer sharing this same host-global coordination file has no notion of
 * `claims[]` at all — a `--claim`/`--release` beat run by that old code
 * writes a bare `{ orchestratorId, grantedTotal, declaredAt }` scalar,
 * REPLACING whatever `claims[]` array (with its per-holder attribution) was
 * there a moment before. Every live holder's previously-attributed claim is
 * thereby collapsed into one unattributable legacy bucket by this function on
 * the next read, exactly as if the entry had never been migrated in the
 * first place. Every holder's subsequent `--release` then returns `released:
 * 0` (no real id matches `LEGACY_UNATTRIBUTED_CLAIM_HOLDER_ID`) until the
 * next migrating `claimCapacity`/`releaseCapacity` write (now unconditional —
 * see those functions' own doc comments) re-persists the folded total in the
 * new shape, after which it is TTL-subject like any other claim record. This
 * can happen from a genuine rolled-back deploy OR from ordinary concurrent
 * mixed-version operation during a rollout (an old- and new-code
 * orchestrator both writing to the same `type` at different beats) — it is
 * not merely a deploy-rollback edge case. See SKILL.md's "Releasing
 * capacity" section for the caller-facing version of this same disclosure.
 *
 * @param {{ claims?: Array<{ orchestratorId: string, count: number, claimedAt: number, legacyMigrated?: boolean }>, grantedTotal?: number, declaredAt?: number } | undefined} ledgerEntry
 * @param {number} referenceNow Fallback `claimedAt` for a synthesized
 *   legacy-scalar record when `ledgerEntry.declaredAt` is absent.
 *   `claimCapacity`/`releaseCapacity` pass the same single `referenceNow`
 *   already captured at the top of their `withLock` critical section.
 *   `assertClaimTtlSetAgainstLegacyLedger` (Phase 5) is the one caller that
 *   does NOT follow that pattern — it calls this function OUTSIDE any lock,
 *   with an independently-computed value. That's safe here specifically
 *   because that caller only checks for the sentinel `orchestratorId`'s
 *   presence in the returned array, never a timestamp comparison, so which
 *   specific `referenceNow` is used doesn't affect its verdict. Every caller,
 *   regardless of which `referenceNow` it passes, honours the module's
 *   `ARM_FAKE_NOW_MS`/injected-clock seam like every other clock read here —
 *   this function never falls back to a real `Date.now()` call itself.
 * @returns {Array<{ orchestratorId: string, count: number, claimedAt: number, legacyMigrated?: boolean }>}
 */
function normalizeClaims(ledgerEntry, referenceNow) {
  if (!ledgerEntry) return [];
  if (Array.isArray(ledgerEntry.claims)) return ledgerEntry.claims.filter(isValidClaimRecord);
  if (isFiniteNumber(ledgerEntry.grantedTotal) && ledgerEntry.grantedTotal > 0) {
    return [
      {
        orchestratorId: LEGACY_UNATTRIBUTED_CLAIM_HOLDER_ID,
        // Floored so a corrupt/fractional legacy `grantedTotal` (e.g. 2.5)
        // still produces a record that survives its own `isValidClaimRecord`
        // check (round-5 review) instead of silently failing validation on
        // the very next read and vanishing — the "never silently lost"
        // contract this function documents above.
        count: Math.floor(ledgerEntry.grantedTotal),
        claimedAt: isFiniteNumber(ledgerEntry.declaredAt) ? ledgerEntry.declaredAt : referenceNow,
        legacyMigrated: true,
      },
    ];
  }
  return [];
}

/**
 * Strips the transient `legacyMigrated` marker `normalizeClaims` stamps onto
 * a synthesized legacy-scalar record, re-stamping `claimedAt` to `referenceNow`
 * for any record that still carries it — the "first real write retires the
 * TTL exemption" half of the round-2 review fix (see `normalizeClaims`'s doc
 * comment and `pruneExpiredClaims` below). Applied by BOTH `claimCapacity`'s
 * and `releaseCapacity`'s write paths, not just `claimCapacity`'s: either
 * function can be the first to actually persist `claims[]` to disk for a
 * still-legacy-shaped ledger entry (e.g. a DIFFERENT orchestrator releasing
 * an unrelated claim against the same `type` also rewrites the whole entry),
 * and the legacy record must lose its exemption on whichever write happens
 * first — otherwise it could keep riding along, still marked, inside a
 * `claims[]` array already in the new shape (where `normalizeClaims`'s
 * `Array.isArray` branch would return it completely unexamined on every
 * later read), reintroducing the exact permanent-exemption bug this fix
 * closes via a different code path. Non-legacy records are returned
 * untouched.
 *
 * @param {Array<{ orchestratorId: string, count: number, claimedAt: number, legacyMigrated?: boolean }>} claims
 * @param {number} referenceNow
 * @returns {Array<{ orchestratorId: string, count: number, claimedAt: number }>}
 */
function stampMigratedClaims(claims, referenceNow) {
  return claims.map((claim) =>
    claim?.legacyMigrated
      ? { orchestratorId: claim.orchestratorId, count: claim.count, claimedAt: referenceNow }
      : claim,
  );
}

/**
 * Sums a `claims[]` array's `count` fields — the per-type cross-holder
 * aggregate (`alreadyGrantedTotal`/`grantedTotal` on `claimCapacity`'s/
 * `releaseCapacity`'s external return contracts).
 *
 * @param {Array<{ count: number }>} claims
 * @returns {number}
 */
function sumClaimCounts(claims) {
  return claims.reduce((total, claim) => total + (isFiniteNumber(claim?.count) ? claim.count : 0), 0);
}

/**
 * Drops dead `count: 0` (and `count: -0` — `-0 === 0` in JS, so this
 * comparison catches both) claim records from a `claims[]` array — a
 * `count: 0` record has nothing left to grant/release and exists only as
 * corrupt/hand-edited-file noise (`isValidClaimRecord` accepts `count: 0`
 * via `isNonNegativeInteger`, so nothing upstream strips it on read).
 *
 * Must run AFTER `stampMigratedClaims`, never before it: a legitimately
 * stamped record can still resolve to `count: 0` (e.g. a legacy-migrated
 * record synthesized from a corrupt fractional `grantedTotal` in `(0, 1)`,
 * floored to `0` by `normalizeClaims`), and this filter has to catch that
 * shape too, not just a record that was already zero before stamping.
 * Filtering here — immediately before the `stampedClaims.length > 0`
 * omission guard — is what stops a corrupt zero-count record from keeping a
 * ledger entry alive with `grantedTotal: 0` and a lone dead claim.
 *
 * Also called from `claimCapacity`'s write path (not just `releaseCapacity`'s)
 * — both callers must stay symmetric in how they filter zero-count records.
 *
 * @param {Array<{ orchestratorId: string, count: number, claimedAt: number }>} claims
 * @returns {Array<{ orchestratorId: string, count: number, claimedAt: number }>}
 */
function filterOutZeroCountClaims(claims) {
  return claims.filter((claim) => claim?.count !== 0);
}

/**
 * Whether `claimTtlMs` is a genuinely usable TTL for the
 * `assertClaimTtlSetAgainstLegacyLedger` guard below — stricter than the
 * bare `isFiniteNumber` check `pruneExpiredClaims` itself uses. `undefined`
 * and `NaN` are unset by `isFiniteNumber`'s own definition; a negative
 * number IS finite (so `isFiniteNumber` alone would call it "set"), but a
 * negative `claimTtlMs` is not a value any caller could sanely intend — it
 * cannot age anything out the way a positive TTL does, so this guard treats
 * it the same as unset rather than treating it as a real opt-in. A `0` TTL
 * is likewise excluded (not a positive number) — not exercised by any
 * current caller or test, but a zero-length window is equally not a
 * meaningful "I opted into TTL enforcement" signal. `DEFAULT_CLAIM_TTL_MS`
 * and every other real, positive `claimTtlMs` value in production use
 * satisfies this check.
 *
 * @param {unknown} value
 * @returns {value is number}
 */
function isSetClaimTtl(value) {
  return isFiniteNumber(value) && value > 0;
}

/**
 * Guards `claimCapacity`/`releaseCapacity` against a caller combination with
 * no recovery path: `options.claimTtlMs` unset (per `isSetClaimTtl` above)
 * against a ledger for `type` that still carries a legacy-migrated
 * (pre-existing) record, attributed to `LEGACY_UNATTRIBUTED_CLAIM_HOLDER_ID`
 * (see `normalizeClaims`'s doc comment). Without a TTL, that sentinel record
 * can never expire (`pruneExpiredClaims` is a no-op when `claimTtlMs` isn't
 * finite), and no real `orchestratorId` ever equals the sentinel, so
 * `releaseCapacity` can never selectively release it either — left alone, it
 * would sit in `alreadyGrantedTotal`/`grantedTotal` forever with no way for
 * any caller to reclaim the capacity it represents. Convergence
 * Analysis follow-up.
 *
 * Checks the sentinel `orchestratorId` on the PRE-prune claims (via
 * `normalizeClaims`, the same helper `needsLegacyMigrationWrite` uses inside
 * `claimCapacity`/`releaseCapacity` — but checking for that transient
 * `legacyMigrated` marker directly, as `needsLegacyMigrationWrite` does,
 * would NOT be durable enough for this guard: `stampMigratedClaims` strips
 * that marker the very first time either function persists a write for this
 * `type`, including a write made by a DIFFERENT caller passing a valid TTL
 * (`stampMigratedClaims` runs unconditionally over the whole `claims[]`
 * array on every write, not only when the writer itself is doing the
 * migrating). The sentinel `orchestratorId` itself never changes across such
 * a write, so it — not the marker — is the durable signal this guard needs.
 *
 * Deliberately reads the file via `readEntriesRaw` BEFORE the caller ever
 * acquires its fencing lock, mirroring `claimCapacity`/`releaseCapacity`'s
 * existing pattern of validating their other params synchronously before
 * `withLock` is entered — this guard's rejection must land before the lock
 * is ever taken and before any write is ever attempted (a caller hitting
 * this guard changes nothing about the ledger). This is safe specifically
 * because it is a READ-ONLY presence check, not a read-modify-write: the
 * guard's job is "refuse a specific, always-invalid caller/ledger
 * combination up front", not "atomically protect the ledger" the way the
 * fencing lock protects the claim/grant math itself. A legacy record
 * appearing or disappearing in the narrow window between this read and the
 * caller's own (separate) lock acquisition is not a correctness bug for
 * that purpose: at worst, a call that would have been rejected here
 * proceeds and is caught by this exact same guard on a subsequent call once
 * the record's presence is next observed, or a call that would have
 * proceeded is rejected here instead — either way the ledger itself is
 * never corrupted or mutated by this function, which never writes anything.
 *
 * Only reads the file when `claimTtlMs` is unset — a valid, positive TTL
 * returns immediately without touching the filesystem, so `cli.mjs`'s real
 * production path (which always passes `DEFAULT_CLAIM_TTL_MS`) pays no
 * extra I/O cost and is completely unaffected by this guard. In addition,
 * `claimCapacity`/`releaseCapacity` also short-circuit the CALL itself with
 * the same `isSetClaimTtl` check before ever invoking this function — on the
 * hot (TTL-set) path this function is not called at all, so the production
 * path reads no clock either (not even the negligible cost of a no-op
 * function call): the internal `if (isSetClaimTtl(claimTtlMs)) return;`
 * below remains as defense-in-depth for any other/future caller that
 * invokes this function unconditionally.
 *
 * the internal check immediately below and the
 * `claimCapacity`/`releaseCapacity` call-site short-circuits are two copies
 * of the same `isSetClaimTtl` predicate by design (defense in depth) — both
 * call the shared function, not independent logic, so they cannot diverge
 * today. If either copy is ever edited without the other, update both call
 * sites and this internal check together.
 *
 * @param {string} filePath
 * @param {string} type
 * @param {number | undefined} claimTtlMs
 * @param {number} referenceNow Only used as `normalizeClaims`'s fallback
 *   `claimedAt` for a synthesized legacy record when the ledger entry's own
 *   `declaredAt` is absent — irrelevant to this guard's own sentinel-id
 *   check, so an independent `Date.now()`/`now`-derived value here (rather
 *   than the caller's own critical-section `referenceNow`, captured later
 *   inside `withLock`) never affects this guard's verdict. Since,
 *   callers only compute and pass this argument on the legacy-fallback
 *   (claimTtlMs-unset) branch, where this function is actually invoked.
 * @returns {Promise<void>}
 */
async function assertClaimTtlSetAgainstLegacyLedger(filePath, type, claimTtlMs, referenceNow) {
  if (isSetClaimTtl(claimTtlMs)) return;
  const existing = await readEntriesRaw(filePath);
  const ledgerId = claimLedgerEntryId(type);
  const matchingLedgerEntries = existing.filter((entry) => entry.orchestratorId === ledgerId);
  const existingClaims = matchingLedgerEntries.flatMap((entry) => normalizeClaims(entry, referenceNow));
  // Operand order (sentinel-first) is deliberate, not stylistic: it keeps
  // this literal comparison textually distinct from `pruneExpiredClaims`'s
  // now-removed round-1 `claim?.orchestratorId === LEGACY_UNATTRIBUTED_...`
  // pattern that `doc-honesty.jest.spec.mjs` pins as permanently gone from
  // this file (that pattern was a PERMANENT TTL exemption keyed on the
  // sentinel id — the round-1 bug). This guard's use of the same sentinel is
  // a different, narrower thing: a one-time, read-only presence check before
  // the lock, not a TTL exemption inside `pruneExpiredClaims` itself.
  const hasLegacyRecord = existingClaims.some(
    (claim) => LEGACY_UNATTRIBUTED_CLAIM_HOLDER_ID === claim?.orchestratorId,
  );
  if (hasLegacyRecord) {
    throw new UnrecoverableLegacyClaimError(type);
  }
}

/**
 * Default TTL for an individual claim record — how long a
 * granted slot is honoured in `alreadyGrantedTotal`/`grantedTotal` aggregate
 * math with no explicit `releaseCapacity` call ever made against it. Callers
 * opt into TTL enforcement by threading `options.claimTtlMs` through to
 * `claimCapacity`/`releaseCapacity`, mirroring `declareDibs`/`readDibs`'s
 * existing `options.livenessThresholdMs` convention exactly — when
 * `claimTtlMs` isn't a finite number, no TTL exclusion happens at all (same
 * "unset means don't prune" posture `pruneStale` already has for
 * `livenessThresholdMs`). This constant is that option's conventional
 * default value for production callers (`cli.mjs`); it is not auto-applied
 * inside this module itself.
 *
 * Value: 6 hours. Conservative enough to comfortably outlive a normal
 * orchestrator sub-agent task (the "estimated-duration window" its own
 * issue text frames TTL around) without requiring an explicit `--release`
 * for ordinary short/medium-lived work, while still bounding how long a
 * genuinely abandoned claim (the crash-ack-loss window `claimCapacity`'s own
 * JSDoc documents, or an orchestrator that forgot to release) can strand
 * capacity before it self-heals on the next `--claim`/`--release` beat — see
 * SKILL.md's "Claiming capacity" / "Releasing capacity" sections for the
 * caller-facing version of this same rationale. Same "illustrative starting
 * point, not settled" posture as every other `DEFAULT_*` constant in this
 * skill (e.g. `DEFAULT_LIVENESS_THRESHOLD_MS`, `DEFAULT_HEADROOM_CONFIG` in
 * `cli.mjs`) — tune once real usage data exists.
 *
 * @type {number}
 */
export const DEFAULT_CLAIM_TTL_MS = 6 * 60 * 60 * 1000;

/**
 * Excludes claim records whose `claimedAt` is older than `claimTtlMs`
 * relative to `referenceNow` — the per-claim analogue of `pruneStale`'s
 * `declaredAt`-relative exclusion, applied to `claims[]` records inside a
 * `__claim:<type>__` ledger entry rather than to top-level dibs entries.
 * Same inclusive-boundary convention as `pruneStale`: a claim exactly at the
 * threshold is still considered live, not yet expired.
 *
 * When `claimTtlMs` isn't a finite number, no exclusion happens (mirrors
 * `pruneStale`'s identical "unset means don't prune" behavior for
 * `livenessThresholdMs`) — callers that never opt into TTL enforcement see
 * no change from Phase 1's behavior.
 *
 * The synthetic `LEGACY_UNATTRIBUTED_CLAIM_HOLDER_ID` record `normalizeClaims`
 * folds a pre-existing scalar entry's total into is EXEMPT from this TTL
 * exclusion ONLY while it still carries the transient `legacyMigrated: true`
 * marker `normalizeClaims` stamps onto it (round-2 review fix — NOT an
 * unconditional, permanent exemption keyed on `orchestratorId` — that was
 * the round-1 over-correction: an unexempted legacy record could evaporate
 * after `claimTtlMs` on an idle-upgraded host, but exempting it forever by id
 * meant nothing could ever release it once its real id could never match the
 * sentinel, producing a worse, unbounded capacity lock). This mirrors how the
 * outer `__claim:<type>__` entry itself is exempt from `pruneStale`'s
 * liveness pruning (see `claimLedgerEntryId`'s doc comment), but only for the
 * read that first synthesizes the record — not for its persisted lifetime.
 *
 * Without SOME exemption here, a legacy file that sat idle longer than
 * `claimTtlMs` before this migration ever ran would have its migrated-forward
 * total silently pruned away on the very first read under a real
 * (non-default-off) TTL — contradicting `normalizeClaims`'s own "never
 * silently lost" contract. But the exemption is retired the moment the
 * record is actually persisted by a real write: `claimCapacity`'s and
 * `releaseCapacity`'s write paths both run `stampMigratedClaims` over
 * `claims[]` immediately before writing, which re-stamps `claimedAt` to that
 * write's `referenceNow` and drops the marker for any record still carrying
 * it. From that write onward the (former) legacy record is an ordinary,
 * attributed-looking claim record subject to this exact TTL exclusion like
 * any other — it ages out one `claimTtlMs` window after the migrating write,
 * not never.
 *
 * **Round-3 review fix:** "the record is actually persisted by a real write"
 * above no longer means only a write that ALSO grants/releases a non-zero
 * amount. `claimCapacity`/`releaseCapacity` now migrate a still-legacy-shaped
 * entry unconditionally on read — see those functions' own doc comments for
 * why: a saturated legacy entry (`grantedTotal >= availableCapacity`) grants
 * `0` on EVERY future `--claim`, and no real orchestratorId ever matches the
 * legacy sentinel so `--release` against it is `0` too — gating the migrating
 * write on a non-zero outcome left such an entry permanently scalar-shaped on
 * disk (a third route to this same unbounded-lockout bug, distinct from the
 * marker-exemption route round-2 closed).
 *
 * Defensive against a malformed `claims: [null]`/`[{}]` array element (optional
 * chaining, mirroring `sumClaimCounts`'s style) — a bad element is treated as
 * expired/dropped rather than throwing and wedging this type's claim/release
 * calls permanently inside the lock.
 *
 * **`claimTtlMs` unset + legacy record present — no-recovery-path guard
 * (Convergence Analysis follow-up):** this function's own "unset means
 * don't prune" behavior above means a caller that never opts into TTL
 * enforcement leaves a legacy-migrated record (see `normalizeClaims`) on the
 * ledger permanently — it can never expire without a TTL, and no real
 * `orchestratorId` ever matches its sentinel attribution, so it cannot be
 * selectively released either. `claimCapacity`/`releaseCapacity` close that
 * specific combination BEFORE it ever reaches this function (and before
 * either acquires its lock or attempts a write) via
 * `assertClaimTtlSetAgainstLegacyLedger`, which rejects with
 * `UnrecoverableLegacyClaimError` (`.code ===
 * 'COORDINATION_UNRECOVERABLE_LEGACY_CLAIM'`) rather than letting this
 * function silently carry the legacy record forward with no way out. This
 * function's own no-TTL passthrough behavior is otherwise unchanged — the
 * guard runs entirely upstream of it.
 *
 * @param {Array<{ orchestratorId: string, count: number, claimedAt: number, legacyMigrated?: boolean }>} claims
 * @param {number} referenceNow
 * @param {number | undefined} claimTtlMs
 * @returns {Array<{ orchestratorId: string, count: number, claimedAt: number, legacyMigrated?: boolean }>}
 */
function pruneExpiredClaims(claims, referenceNow, claimTtlMs) {
  if (!isFiniteNumber(claimTtlMs)) return claims;
  return claims.filter(
    (claim) => claim?.legacyMigrated === true || referenceNow - claim?.claimedAt <= claimTtlMs,
  );
}

/**
 * Reads `type`'s CURRENT live (TTL-pruned) cumulative claim total from the
 * SAME coordination file `claimCapacity`/`releaseCapacity`/`reserveAdmission`
 * all share — the exact `readEntriesRaw` -> `claimLedgerEntryId` ->
 * `normalizeClaims` -> `pruneExpiredClaims` -> `sumClaimCounts` sequence each
 * of those already run inline for their own ledger type, factored out so a
 * caller outside this module (`cli.mjs`'s `--claim` capacity computation,
 *) can read a DIFFERENT type's live total without duplicating that
 * pipeline or diverging from it.
 *
 * Concurrency note: this function does its own single, unlocked
 * `readEntriesRaw` — it does not itself acquire `withLock`. That is safe ONLY
 * when the caller invokes it from inside a critical section already holding
 * the lock for this same `filePath` (e.g. `claimCapacity`'s
 * `computeAvailableCapacity` callback, which `claimCapacity` itself invokes
 * inside its own `withLock`). Calling this OUTSIDE such a critical section
 * reads a snapshot that can go stale before any decision made from it is
 * committed — the exact staleness class exists to close for the
 * live-agent ceiling. It intentionally does not accept or return a
 * `lockContext`: taking one would invite a caller to assume this function
 * enforces the lock itself, when the guarantee is entirely on the caller's
 * side.
 *
 * `excludeOrchestratorId`, when given, drops that orchestrator's own claims
 * from the sum before returning it — mirroring `reserveAdmission`'s Step 2
 * "own-hold replace, not accumulate" exclusion (`claimsAfterOwnRelease`
 * above), just in the opposite ledger direction. Without this, an
 * orchestrator that reuses the same `orchestratorId` across a
 * `--desired-agents` admission hold and a subsequent `--claim=live-agent:N`
 * for the SAME live-agent count double-counts its own outstanding admission
 * hold against its own headroom — this is the natural extension of reusing
 * one stable `orchestratorId` across beats, as SKILL.md's own
 * `--desired-agents`/`--heartbeat` worked examples already do. See
 * `cli.mjs`'s `computeAvailableCapacity` call site, the only caller that
 * needs this exclusion today.
 *
 * @param {string} filePath
 * @param {string} type
 * @param {number} referenceNow
 * @param {number | undefined} claimTtlMs Same TTL-pruning semantics as
 *   `claimCapacity`'s/`reserveAdmission`'s own `options.claimTtlMs` — an
 *   unset value means no TTL exclusion (mirrors `pruneExpiredClaims`).
 * @param {string | undefined} excludeOrchestratorId When set, claims held by
 *   this `orchestratorId` are excluded from the returned total.
 * @returns {Promise<number>}
 */
export async function getLiveClaimTotal(filePath, type, referenceNow, claimTtlMs, excludeOrchestratorId) {
  const existing = await readEntriesRaw(filePath);
  const ledgerId = claimLedgerEntryId(type);
  const matchingLedgerEntries = existing.filter((entry) => entry.orchestratorId === ledgerId);
  const existingClaims = matchingLedgerEntries.flatMap((entry) => normalizeClaims(entry, referenceNow));
  const liveClaims = pruneExpiredClaims(existingClaims, referenceNow, claimTtlMs);
  const countedClaims = isNonEmptyString(excludeOrchestratorId)
    ? liveClaims.filter((claim) => claim?.orchestratorId !== excludeOrchestratorId)
    : liveClaims;
  return sumClaimCounts(countedClaims);
}

/**
 * Atomically reserves up to `requestedCount` slots of `type`'s capacity
 * against a per-type cumulative claim ledger persisted as a reserved entry
 * (see `claimLedgerEntryId`) in the SAME coordination file `declareDibs`/
 * `consumeGlobalSpawnTokens` already share — reusing THIS module's existing
 * fencing lock (`withLock`/`acquireLock`/`releaseLock`), not a second lock
 * primitive.
 *
 * `computeAvailableCapacity` is an async callback invoked exactly once,
 * inside the critical section, that must return the TOTAL genuinely-free
 * capacity for `type` at this instant (independent of anything already
 * claimed).
 *
 * WHAT MAKES THE GRANT SAFE — corrected here, because an earlier revision of
 * this comment attributed it to the wrong mechanism. It is NOT that the
 * callback runs while this call alone holds the lock. The callback receives
 * no lock-protected state (it is invoked with zero arguments), and in
 * production it reads the live machine plus a DIFFERENT file this lock does
 * not guard — so serialising it never protected anything. What prevents two
 * concurrent callers from independently granting a combined amount that
 * exceeds capacity is the LEDGER's read-modify-write under the lock:
 * `alreadyGrantedTotal` is read under the lock, applied after the callback
 * returns, and written back before the lock is released, so two callers can
 * never both act on the same pre-mutation ledger state. That is the same
 * "read shared state under lock, decide, write back atomically" shape
 * `consumeGlobalSpawnTokens` already established, and it is what actually
 * closes the race two concurrent `--claim` invocations for the last free
 * slot of one type must never lose.
 *
 * That correction is what let `cli.mjs` hoist its expensive host probe out of
 * this critical section entirely. Callers that hoist take on a
 * sample-staleness obligation in exchange — the callback's result may have
 * been computed before the lock wait — and must bound it themselves; see
 * `cli.mjs`'s `DEFAULT_CLAIM_SAMPLE_FRESHNESS_MS`.
 *
 * Ownership is re-proven the moment the callback returns, before any of the
 * ledger arithmetic consumes `alreadyGrantedTotal`. A callback that outran
 * `lockStalenessMs` therefore rejects with `LockLostError` rather than
 * reporting a grant derived from a ledger another holder has since moved on
 * from — and it does so even on the paths that go on to write nothing.
 *
 * Deny-the-remainder semantics: grants `min(requestedCount, remaining)`,
 * where `remaining = max(0, availableCapacity - alreadyGrantedTotal)` — never
 * `0` on a genuine partial ask, and never more than what's genuinely left.
 * `alreadyGrantedTotal` accumulates across every successful claim for this
 * `type` (from any orchestrator) for as long as this ledger entry persists —
 * it is the SUM of every live holder's `claims[].count` (see `normalizeClaims`/
 * `sumClaimCounts`), a cross-holder aggregate, not a per-caller value. Its
 * counterpart, `releaseCapacity` (below), decrements the same ledger entry
 * via an explicit `--release=<type>:<count>` call — grants are not strictly one-way.
 * There is still no TTL/expiry-based *automatic* reclamation of an unused
 * grant (that remains deliberately out of scope for this phase — see
 * SKILL.md's "Releasing capacity" section and issue Phase 2); a slot
 * is only returned via an explicit `releaseCapacity` call.
 *
 * Per-claim schema: the ledger entry's SOURCE OF TRUTH is no
 * longer one cumulative scalar — it is per-claim. It persists
 * `{ orchestratorId: claimLedgerEntryId(type), claims: [{ orchestratorId,
 * count, claimedAt }], grantedTotal, declaredAt }` — an array of per-claim
 * records, one per successful grant, PLUS `grantedTotal` kept alongside it as
 * `sumClaimCounts(claims)`, written purely for backward compatibility: this
 * coordination file is host-global (`resolveCoordinationFilePath()`), so a
 * still-running pre-existing orchestrator process can read this exact file
 * concurrently during a mixed-version rollout, and that old code only knows
 * how to read `ledgerEntry.grantedTotal` (it has no notion of `claims[]`).
 * Without this field, old code would see `alreadyGrantedTotal = 0` on every
 * read and over-grant on top of every live claim. New code (this module)
 * always prefers `claims` when present (see `normalizeClaims`) and never
 * reads `grantedTotal` directly for its own math — `grantedTotal` here is
 * write-only compat surface, always derived from `claims[]`, never the other
 * way around. A REPEAT claim from the SAME `orchestratorId`/`type`
 * APPENDS a new record to `claims[]` rather than merging into an existing
 * one for that id — this preserves each individual claim's own `claimedAt`,
 * which a future phase's per-claim TTL/expiry needs to expire one sub-claim
 * independently of another held by the same orchestrator. `orchestratorId`
 * is therefore now a REQUIRED param here (attributing this grant to its
 * holder) — see `releaseCapacity`'s own doc comment for how that attribution
 * is later used to scope a release to only the calling orchestrator's own
 * claim(s). A coordination file still on disk in the OLD scalar shape (from
 * before this migration) is read via `normalizeClaims`, which folds its
 * total into a single synthetic record rather than losing it or throwing.
 *
 * **Crash-ack-loss window (documented gap, not fixed here):** the ledger
 * write (`writeEntriesAtomic`'s `rename()`, above) and the caller receiving
 * confirmation of the grant are NOT the same event — this function returns
 * `{ granted, ... }` to its caller only AFTER the rename has already
 * completed. If the calling process is killed (OOM-killed, SIGKILL, power
 * loss) after the rename completes but before that return value ever reaches
 * `cli.mjs`'s `handleClaim` and its subsequent stdout write, the slot is
 * already consumed from the ledger (it can be handed back only via an
 * explicit `releaseCapacity` call once the orchestrator has reconciled its
 * own state) but the orchestrator that invoked `--claim` never
 * receives confirmation it won the slot. That orchestrator can neither
 * safely retry (it risks claiming a second slot on top of the one already
 * silently granted) nor safely assume denial (the slot is gone from the pool
 * either way, so treating the call as "denied" doesn't return it). This is a
 * real, narrow window inherent to "atomic write, then report the result of
 * the write" — closing it would need an idempotency token or a two-phase
 * confirm handshake, neither of which exists today; that is future scope,
 * not attempted by this function. See SKILL.md's "Claiming capacity" section
 * for the caller-facing version of this same disclosure.
 *
 * TTL-based expiry: when `options.claimTtlMs` is a finite
 * number, a claim record whose `claimedAt` is older than `referenceNow -
 * claimTtlMs` is excluded from `alreadyGrantedTotal`'s aggregate (so a fresh
 * claim can grant against the capacity it freed) and, if this call goes on to
 * WRITE — i.e. `granted > 0`, OR (round-3 review fix) the ledger entry is
 * still legacy-shaped and this read migrates it forward regardless of grant
 * outcome (see the unconditional-migration note in this function's body) —
 * is physically dropped from the persisted `claims[]` array in that same
 * write — see `pruneExpiredClaims`. There is no background sweep/timer for an
 * ordinary (non-legacy) expired record: one that isn't excluded by a read or
 * pruned by a write simply sits on disk, inert, until the next call that
 * does either. `referenceNow` is captured exactly once at the top of this
 * critical section, mirroring `declareDibs`'s own single-`referenceNow`
 * precedent exactly: `params.now` is used when the caller supplies a finite
 * number (`cli.mjs`'s `resolveNow()`/`ARM_FAKE_NOW_MS` test seam — the same
 * clock `handleClaim` already derives for its capacity-estimate pipeline),
 * falling back to a real `Date.now()` call only when it isn't supplied. The
 * same value is used for the TTL exclusion check, a freshly-granted claim's
 * `claimedAt`, and the ledger entry's own `declaredAt`, so they can never
 * diverge even under a clock that advances between reads. When `claimTtlMs`
 * is unset, no TTL exclusion happens — unchanged Phase 1 behavior.
 *
 * PID identity: `params.pid`/`params.pidStartedAt` are OPTIONAL,
 * caller-supplied, and stamped onto the appended claim record via
 * `withPidIdentity` — see that helper's doc comment for why they are never
 * derived here. They live strictly inside the per-claim record: the
 * backward-compat `grantedTotal` aggregate carries no identity of its own, and
 * this pair adds no validation surface (`orchestratorId`, reserved-id and
 * `requestedCount` validation below are the only checks this function makes).
 *
 * @param {string} filePath
 * @param {{ type: string, requestedCount: number, orchestratorId: string, computeAvailableCapacity: () => Promise<number>, now?: number, pid?: number, pidStartedAt?: number }} params
 * @param {{ claimTtlMs?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<{ granted: number, availableCapacity: number, alreadyGrantedTotal: number }>}
 */
export async function claimCapacity(
  filePath,
  { type, requestedCount, orchestratorId, computeAvailableCapacity, now, pid, pidStartedAt },
  options = {},
) {
  if (!isNonEmptyString(orchestratorId)) {
    throw new TypeError(
      `claimCapacity: orchestratorId must be a non-empty string (received ${orchestratorId})`,
    );
  }
  if (isReservedEntry(orchestratorId)) {
    throw new TypeError(
      `claimCapacity: orchestratorId must not match the reserved double-underscore convention ` +
        `("__...__"), got "${orchestratorId}" — reserved ids collide with internal coordination-file ` +
        'bookkeeping (e.g. the legacy-unattributed-claim sentinel) and are not valid orchestrator ids',
    );
  }
  if (!isNonNegativeInteger(requestedCount)) {
    throw new TypeError(
      `claimCapacity: requestedCount must be a finite, non-negative integer (received ${requestedCount})`,
    );
  }

  // Convergence Analysis follow-up: refuse an unset claimTtlMs against
  // a ledger already holding a legacy-migrated record BEFORE the lock is
  // ever acquired — see `assertClaimTtlSetAgainstLegacyLedger`'s doc comment
  // for why this specific check is safe to make read-only and pre-lock.
  // Convergence Analysis follow-up: guard the call itself with
  // `isSetClaimTtl` rather than always invoking the function — on the hot
  // (TTL-set) path this skips the call entirely, so the `Date.now()` fallback
  // in the 4th argument is never evaluated. A thunk (`() => isFiniteNumber(now)
  // ? now : Date.now()`) was considered and rejected: it still allocates a
  // closure on every hot-path call, which the call-site short-circuit avoids.
  // this short-circuit duplicates `assertClaimTtlSetAgainstLegacyLedger`'s
  // own internal `isSetClaimTtl` check (defense in depth) — see that
  // function's doc comment; keep both in sync if either ever changes.
  // on the hot (TTL-set) path this also means execution no longer
  // yields a microtask tick here before `withLock` (pre-existing it always did,
  // via the awaited no-op promise). That's safe: coordination correctness is
  // enforced by the file lock itself, not by in-process await ordering.
  if (!isSetClaimTtl(options.claimTtlMs)) {
    await assertClaimTtlSetAgainstLegacyLedger(
      filePath,
      type,
      options.claimTtlMs,
      isFiniteNumber(now) ? now : Date.now(),
    );
  }

  return withLock(
    filePath,
    async (lockContext) => {
      const referenceNow = isFiniteNumber(now) ? now : Date.now();
      const existing = await readEntriesRaw(filePath);
      const ledgerId = claimLedgerEntryId(type);
      // Normally there is at most one matching entry — `.filter()` +
      // `.flatMap()` is a no-op in that (overwhelmingly common) case. It
      // exists to tolerate file corruption where two entries share the same
      // `__claim:<type>__` id (possibly in mismatched shapes — one
      // legacy-scalar, one already per-claim-array): each is normalized
      // independently via `normalizeClaims`, then their `claims[]` arrays
      // are concatenated so no duplicate's claims are silently dropped (see
      // the symmetric write below, which already removes ALL matches).
      const matchingLedgerEntries = existing.filter((entry) => entry.orchestratorId === ledgerId);
      const existingClaims = matchingLedgerEntries.flatMap((entry) => normalizeClaims(entry, referenceNow));
      const liveClaims = pruneExpiredClaims(existingClaims, referenceNow, options.claimTtlMs);
      const alreadyGrantedTotal = sumClaimCounts(liveClaims);

      const availableCapacityRaw = await computeAvailableCapacity();
      // re-prove ownership the moment the probe returns, BEFORE any
      // of the ledger arithmetic below consumes `alreadyGrantedTotal`. Of
      // everything inside this critical section the probe is by far the most
      // capable of outliving `lockStalenessMs` — `cli.mjs`'s implementation
      // shells out synchronously up to eight times — so this is the earliest
      // point at which a stalled holder can be caught.
      //
      // Note this fires even on the paths that go on to write nothing. That
      // is deliberate and is NOT the same guard as `writeEntriesAtomic`'s:
      // `alreadyGrantedTotal` was read under a lock that has since been
      // handed to someone else, so the RETURNED `granted` is unsound too,
      // not merely the write. Reporting a grant derived from a ledger
      // another holder has since mutated is the over-admission this ticket
      // exists to prevent, whether or not this call happens to persist
      // anything.
      await lockContext.assertStillHeld();
      const availableCapacity = isFiniteNumber(availableCapacityRaw) ? Math.max(0, availableCapacityRaw) : 0;
      const remaining = Math.max(0, availableCapacity - alreadyGrantedTotal);
      const granted = Math.max(0, Math.min(requestedCount, remaining));

      // Round-3 review fix: migrate a legacy-shaped entry UNCONDITIONALLY on
      // this read, never merely as a side effect of a successful grant. A
      // legacy entry whose `grantedTotal >= availableCapacity` grants `0` on
      // EVERY future `--claim` call (there is nothing left to grant), so
      // gating the migrating write on `granted > 0` left it permanently
      // scalar-shaped on disk — re-synthesized as `legacyMigrated: true` on
      // every read and therefore permanently TTL-exempt (a third route to
      // the same unbounded-lockout bug the round-1/round-2 fixes closed via
      // the marker exemption and the reserved-id release path). Detected via
      // `existingClaims` (the PRE-prune output of `normalizeClaims`) rather
      // than `liveClaims`, since a legacy record's `legacyMigrated` marker
      // always survives `pruneExpiredClaims`'s exemption anyway — both would
      // agree here, but reading intent from the un-pruned form is clearer.
      const needsLegacyMigrationWrite = existingClaims.some((claim) => claim?.legacyMigrated);

      if (granted > 0 || needsLegacyMigrationWrite) {
        const withoutLedger = existing.filter((entry) => entry.orchestratorId !== ledgerId);
        // Append-not-merge, even when a prior claim for the SAME
        // orchestratorId already exists — see this function's doc comment
        // above for why (per-claim `claimedAt` granularity for Phase 2's
        // TTL work). Built from `liveClaims`, not `existingClaims` — this is
        // the "physically prune on next write" half of TTL expiry: any
        // already-expired record is dropped here rather than carried
        // forward. A zero-grant migrating write appends nothing new — it
        // persists `liveClaims` (which still includes the not-yet-expired
        // legacy record) as-is, stamped. Passed through `stampMigratedClaims`
        // so any still-legacy record riding along in `liveClaims` loses its
        // TTL exemption on THIS write (round-2 review fix — see
        // `stampMigratedClaims`'s doc comment).
        const claimsWithGrant =
          granted > 0
            ? [
                ...liveClaims,
                withPidIdentity({ orchestratorId, count: granted, claimedAt: referenceNow }, pid, pidStartedAt),
              ]
            : liveClaims;
        const nextClaims = stampMigratedClaims(claimsWithGrant, referenceNow);
        // Phase 2: filter dead `count: 0`/`count: -0` records OUT of
        // `nextClaims` here — AFTER `stampMigratedClaims` runs, so a
        // legitimately-stamped-but-zero-count record is still caught — and
        // BEFORE the result is used to compute `grantedTotal` below or
        // persisted in `claims:`. Same helper, same ordering, as
        // `releaseCapacity`'s write path (see `filterOutZeroCountClaims`'s
        // doc comment for why this ordering matters). A live (`count > 0`)
        // claim is untouched by this filter.
        const liveNextClaims = filterOutZeroCountClaims(nextClaims);
        const nextLedgerEntry = {
          orchestratorId: ledgerId,
          claims: liveNextClaims,
          // Backward-compat aggregate, kept alongside `claims[]` (never the
          // only field) — this coordination file is host-global, so an
          // orchestrator still running pre-existing code can be reading this
          // exact file concurrently. That old code has no notion of
          // `claims[]`; it reads `ledgerEntry.grantedTotal` directly (see
          // `normalizeClaims`'s doc comment for the full mixed-version
          // rollout scenario this guards against). Always kept equal to
          // `sumClaimCounts(liveNextClaims)` so it can never silently diverge
          // from the per-claim source of truth.
          grantedTotal: sumClaimCounts(liveNextClaims),
          declaredAt: referenceNow,
        };
        // Phase 2: `liveNextClaims` CAN now be empty here, in exactly
        // one corner — the ` Phase 4` comment this replaces claimed
        // `nextClaims` could never be empty, but that proof only held
        // before `filterOutZeroCountClaims` was applied above. The corner: a
        // legacy-migration write (`needsLegacyMigrationWrite === true`,
        // `granted === 0`, so no fresh claim is appended) where the ONLY
        // pre-existing record is a corrupt, hand-edited legacy-scalar entry
        // whose fractional `grantedTotal` sits in the open interval `(0, 1)`
        // — `Math.floor` inside `normalizeClaims` synthesizes a lone
        // `{ legacyMigrated: true, count: 0, ... }` record from it,
        // `stampMigratedClaims` re-stamps that record but does not change
        // its `count`, and `filterOutZeroCountClaims` then strips it,
        // leaving `liveNextClaims` empty. Mirroring (not contradicting)
        // `releaseCapacity`'s existing omission guard: when there is
        // nothing left to persist for this type, omit the entry from the
        // write array entirely rather than writing it back as `{claims: [],
        // grantedTotal: 0}` — that shape would survive on disk forever,
        // since `pruneStale` exempts every `__claim:<type>__` id from
        // liveness pruning unconditionally (see `claimLedgerEntryId`'s doc
        // comment). `withoutLedger` above already strips every OLD entry
        // for this `ledgerId`; this only decides whether to add the new one
        // back.
        const nextEntries = liveNextClaims.length > 0 ? [...withoutLedger, nextLedgerEntry] : withoutLedger;
        await writeEntriesAtomic(filePath, nextEntries, lockContext);
      }

      return { granted, availableCapacity, alreadyGrantedTotal };
    },
    {
      lockStalenessMs: options.lockStalenessMs,
      lockMaxAttempts: options.lockMaxAttempts,
      lockRetryDelayMs: options.lockRetryDelayMs,
    },
  );
}

/**
 * Consumes `requestedCount` tokens from the single, machine-wide spawn-rate
 * token bucket, persisted as a reserved entry (see
 * `GLOBAL_SPAWN_BUCKET_RESERVED_ID`/`isReservedEntry`) in the same
 * coordination file `declareDibs`/`readDibs` use.
 *
 * The entire read -> pure `consumeSpawnTokens` -> write cycle happens inside
 * a SINGLE `withLock` critical section — the exact same lock primitive
 * `declareDibs` uses for its own read-modify-write cycle, not a second,
 * parallel locking mechanism. This is what makes the bucket genuinely
 * `scope: 'global'`: two concurrent callers can never both read the same
 * pre-mutation bucket state and independently grant a burst that together
 * exceeds `config.capacityTokens` (the double-grant race `consumeSpawnTokens`
 * alone cannot prevent, since it's a pure function with no knowledge of
 * concurrent callers).
 *
 * Reserved entries bypass `pruneStale`'s liveness pruning unconditionally
 * (see `isReservedEntry`), so a long idle gap between beats never resets an
 * accumulated bucket back to full capacity. The persisted entry still
 * carries a `declaredAt` timestamp, purely for shape-consistency with normal
 * dibs entries (readable/inspectable the same way) — it has no bearing on
 * this entry's own lifecycle, which `isReservedEntry` exempts from pruning
 * regardless of `declaredAt`'s age.
 *
 * @param {string} filePath
 * @param {{ requestedCount: number, config: { capacityTokens: number, refillTokensPerMs: number }, now: number }} params
 * @param {{ livenessThresholdMs?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<{ allowed: boolean, grantedCount: number, bucketState: { tokens: number, lastRefillAt: number }, scope: 'global' }>}
 */
export async function consumeGlobalSpawnTokens(filePath, { requestedCount, config, now }, options = {}) {
  return withLock(
    filePath,
    async (lockContext) => {
      const existing = await readEntriesRaw(filePath);
      const bucketEntry = existing.find(
        (entry) => entry.orchestratorId === GLOBAL_SPAWN_BUCKET_RESERVED_ID,
      );
      const bucketState = bucketEntry ? { tokens: bucketEntry.tokens, lastRefillAt: bucketEntry.lastRefillAt } : null;

      const result = consumeSpawnTokens({ bucketState, now, requestedCount, config });

      const withoutBucket = existing.filter(
        (entry) => entry.orchestratorId !== GLOBAL_SPAWN_BUCKET_RESERVED_ID,
      );
      const nextBucketEntry = {
        orchestratorId: GLOBAL_SPAWN_BUCKET_RESERVED_ID,
        tokens: result.bucketState.tokens,
        lastRefillAt: result.bucketState.lastRefillAt,
        declaredAt: now,
      };
      const upserted = [...withoutBucket, nextBucketEntry];
      const pruned = pruneStale(upserted, now, options.livenessThresholdMs);

      await writeEntriesAtomic(filePath, pruned, lockContext);

      return { ...result, scope: 'global' };
    },
    {
      lockStalenessMs: options.lockStalenessMs,
      lockMaxAttempts: options.lockMaxAttempts,
      lockRetryDelayMs: options.lockRetryDelayMs,
    },
  );
}

/**
 * @param {unknown} value
 * @returns {value is number}
 */
function isNonNegativeInteger(value) {
  return isFiniteNumber(value) && Number.isInteger(value) && value >= 0;
}

/**
 * @param {unknown} value
 * @returns {value is string}
 */
function isNonEmptyString(value) {
  return typeof value === 'string' && value.length > 0;
}

/**
 * Releases (decrements) up to `releaseCount` slots previously granted by
 * `claimCapacity` for `type`, against the SAME per-type claim ledger entry
 * (`claimLedgerEntryId(type)`) `claimCapacity` writes — purely additive to
 * that ledger's lifecycle, not a new one. Reuses `withLock`/`acquireLock`/
 * `releaseLock` exactly as `claimCapacity` does (no second lock primitive),
 * so a release can never race a concurrent claim/release for the same `type`
 * into a torn or double-applied ledger value.
 *
 * Orchestrator-scoped: `orchestratorId` is now a REQUIRED
 * param, and this function decrements ONLY the CALLING orchestrator's own
 * `claims[]` record(s) for `type` — never another orchestrator's. `released
 * = min(releaseCount, ownHeld)`, where `ownHeld` is the sum of only the
 * `claims[]` records whose `orchestratorId` matches the caller's — floored
 * at what the caller genuinely holds, never at the shared cross-holder pool.
 * An orchestrator that never claimed against `type` (or has already released
 * everything it held) always gets `released: 0`, even when other holders
 * still hold plenty — the exact over-release bug exists to close (see
 * this file's own Phase 1 describe block for the regression tests).
 * `grantedTotal` on the return value, like `alreadyGrantedTotal` on
 * `claimCapacity`'s, is the cross-holder aggregate (sum of every live
 * holder's `claims[].count`) — NOT the caller's own remaining amount.
 *
 * When the caller's own release only partially consumes one of its `claims[]`
 * records (`releaseCount` less than that record's `count`), the record is
 * shrunk in place, preserving its original `claimedAt`. When it fully
 * consumes a record, that record is dropped entirely. Multiple records for
 * the same `orchestratorId` (see `claimCapacity`'s append-not-merge
 * contract) are consumed oldest-array-order first, until `released` slots
 * have been freed or the caller's own records are exhausted — whichever
 * comes first.
 *
 * Releasing against a `type` with no ledger entry at all, or one where the
 * caller holds nothing, is treated as "nothing to release"
 * (`released: 0, grantedTotal: <cross-holder total, possibly 0>`) rather than
 * an error — other holders' claims are always left fully intact.
 *
 * A coordination file still on disk in the OLD, pre-existing scalar shape
 * (`{ orchestratorId, grantedTotal, declaredAt }`, no `claims` array) is read
 * via `normalizeClaims`, which folds its total into a single synthetic
 * record under a legacy sentinel id — see `normalizeClaims`'s own doc
 * comment. No live orchestrator can selectively release against that
 * sentinel record (nobody's real `orchestratorId` will ever match it), so a
 * caller releasing against a still-legacy-shaped entry simply gets
 * `released: 0` for that record — the pre-existing total is never lost. THIS
 * function's own write path (like `claimCapacity`'s) also runs
 * `stampMigratedClaims` over the outgoing `claims[]` before persisting, so
 * a still-legacy record riding along untouched (because THIS release was
 * against a different orchestrator's claim for the same `type`) loses its
 * TTL exemption on this write too — it does not require a `claimCapacity`
 * call specifically to be migrated forward, only the next real write of any
 * kind to this ledger entry (see `pruneExpiredClaims`'s doc comment for the
 * full corrected lifecycle: exempt only until that next write, then TTL-subject
 * like any other claim record from then on).
 *
 * `releaseCount` and `orchestratorId` are both validated BEFORE the lock is
 * ever acquired (or the file ever touched): `releaseCount` must be a finite,
 * non-negative integer, and `orchestratorId` must be a non-empty string. An
 * invalid value throws synchronously (surfacing as a rejected promise, since
 * this function is `async`) and never partially applies — the ledger is left
 * exactly as it was, never corrupted.
 *
 * split a claim this doc comment used to state as one: "never throws"
 * and "never partially applies" are now separate facts with different truth
 * values. This function CAN now throw from inside its own critical section —
 * `writeEntriesAtomic` raises `LockLostError` when the fencing lock was
 * reclaimed while this call was still running. "Never partially applies"
 * remains true, and is in fact strengthened by it: the ownership re-check
 * fires before the `rename`, so a refused write leaves the ledger byte-for-
 * byte as it was. Callers that relied on "release never throws" must handle a
 * rejected promise; `cli.mjs`'s `handleRelease` already does, failing closed
 * to `released: 0`. `claimCapacity` validates its own
 * `orchestratorId` param the same way, for the same reason: without it, a
 * caller passing `orchestratorId: undefined` (or omitting it) would silently
 * collide with any other caller that also omits it, since the comparison
 * against `claim.orchestratorId` is a plain `===` — exactly the cross-holder
 * collision bug exists to close.
 *
 * Does not touch `claimCapacity`'s own signature or contract, beyond sharing
 * the same `normalizeClaims`/`sumClaimCounts` helpers.
 *
 * `declaredAt` (**updated by Phase 2** — previously stamped with a
 * real, internal `Date.now()` unconditionally; see git history for that
 * superseded rationale) is now derived the same way `referenceNow` is below:
 * `params.now` is used when the caller supplies a finite number, falling
 * back to `Date.now()` only when it isn't supplied. This is required for TTL
 * correctness — the same `referenceNow` this function uses to judge which
 * claim records have expired must also be the value it stamps onto a
 * genuine release's `declaredAt`, or the two would silently diverge under
 * `cli.mjs`'s `ARM_FAKE_NOW_MS` test seam. `handleRelease` now calls
 * `resolveNow()` and threads it through as `params.now`, mirroring
 * `handleClaim`'s existing pattern.
 *
 * TTL-based expiry: when `options.claimTtlMs` is a finite
 * number, this function applies the exact same `pruneExpiredClaims`
 * exclusion `claimCapacity` does — using a single `referenceNow`, captured
 * once at the top of this critical section — BEFORE computing
 * `grantedTotalBefore`/`ownHeld`. A caller whose only claim record for
 * `type` has already expired is therefore indistinguishable from a caller
 * who never claimed anything: `ownHeld` is `0`, so `released` is `0` — and,
 * absent a legacy-migration need (below), nothing is written, so it never
 * under/over-decrements or partially applies. (It can still reject with
 * `LockLostError` when a legacy-migration write IS needed and the fencing
 * lock was lost — see the split-claim note above; the "never partially
 * applies" half is unaffected.) Any expired record for
 * a DIFFERENT holder is likewise excluded from `grantedTotal`'s cross-holder
 * aggregate, and — if this call goes on to WRITE (a genuine non-zero release,
 * OR — round-3 review fix — the ledger entry is still legacy-shaped and this
 * read migrates it forward regardless of release outcome, mirroring
 * `claimCapacity`'s identical fix; see that function's doc comment) —
 * physically dropped from the persisted `claims[]` array in that same write,
 * the same "prune-on-next-write" posture `claimCapacity` uses. When
 * `claimTtlMs` is unset, no TTL exclusion happens — unchanged Phase 1
 * behavior.
 *
 * @param {string} filePath
 * @param {{ type: string, releaseCount: number, orchestratorId: string, now?: number }} params
 * @param {{ claimTtlMs?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<{ released: number, grantedTotal: number }>}
 */
export async function releaseCapacity(filePath, { type, releaseCount, orchestratorId, now }, options = {}) {
  if (!isNonNegativeInteger(releaseCount)) {
    throw new TypeError(
      `releaseCapacity: releaseCount must be a finite, non-negative integer (received ${releaseCount})`,
    );
  }
  if (!isNonEmptyString(orchestratorId)) {
    throw new TypeError(
      `releaseCapacity: orchestratorId must be a non-empty string (received ${orchestratorId})`,
    );
  }
  if (isReservedEntry(orchestratorId)) {
    throw new TypeError(
      `releaseCapacity: orchestratorId must not match the reserved double-underscore convention ` +
        `("__...__"), got "${orchestratorId}" — reserved ids collide with internal coordination-file ` +
        'bookkeeping (e.g. the legacy-unattributed-claim sentinel) and are not valid orchestrator ids',
    );
  }

  // Convergence Analysis follow-up: same pre-lock guard as
  // `claimCapacity` — see `assertClaimTtlSetAgainstLegacyLedger`'s doc
  // comment.
  // Convergence Analysis follow-up: same call-site short-circuit as
  // `claimCapacity` — see the matching comment there for why `isSetClaimTtl`
  // guards the call itself instead of always invoking the function (or
  // passing a thunk) on the hot (TTL-set) path.
  // see the matching comments at `claimCapacity`'s short-circuit
  // for the defense-in-depth cross-reference and the microtask-tick note —
  // both apply identically here.
  if (!isSetClaimTtl(options.claimTtlMs)) {
    await assertClaimTtlSetAgainstLegacyLedger(
      filePath,
      type,
      options.claimTtlMs,
      isFiniteNumber(now) ? now : Date.now(),
    );
  }

  return withLock(
    filePath,
    async (lockContext) => {
      const referenceNow = isFiniteNumber(now) ? now : Date.now();
      const existing = await readEntriesRaw(filePath);
      const ledgerId = claimLedgerEntryId(type);
      // See the matching comment in `claimCapacity` — `.filter()` +
      // `.flatMap()` is a no-op for the normal 0-or-1-match case, and merges
      // duplicate `__claim:<type>__` entries (each normalized independently)
      // when file corruption has produced more than one.
      const matchingLedgerEntries = existing.filter((entry) => entry.orchestratorId === ledgerId);
      const existingClaims = matchingLedgerEntries.flatMap((entry) => normalizeClaims(entry, referenceNow));
      const claims = pruneExpiredClaims(existingClaims, referenceNow, options.claimTtlMs);
      const grantedTotalBefore = sumClaimCounts(claims);

      const ownHeld = sumClaimCounts(claims.filter((claim) => claim?.orchestratorId === orchestratorId));
      const released = Math.min(releaseCount, ownHeld);

      // Round-3 review fix — same rationale as `claimCapacity`'s matching
      // check above: no real orchestratorId ever equals the legacy sentinel
      // (`LEGACY_UNATTRIBUTED_CLAIM_HOLDER_ID`), so `ownHeld`/`released` are
      // always `0` for a legacy-shaped entry on EVERY `--release` call — the
      // dead write path this fix closes. Migrate unconditionally on read,
      // never merely as a side effect of a genuine (non-zero) release.
      const needsLegacyMigrationWrite = existingClaims.some((claim) => claim?.legacyMigrated);

      if (released > 0 || needsLegacyMigrationWrite) {
        let remainingToRelease = released;
        const nextClaims = [];
        for (const claim of claims) {
          if (claim?.orchestratorId !== orchestratorId || remainingToRelease <= 0) {
            nextClaims.push(claim);
            continue;
          }
          const claimCount = isFiniteNumber(claim.count) ? claim.count : 0;
          if (claimCount <= remainingToRelease) {
            remainingToRelease -= claimCount;
            // Fully consumed — dropped from the ledger entirely.
            continue;
          }
          nextClaims.push({ ...claim, count: claimCount - remainingToRelease });
          remainingToRelease = 0;
        }

        const withoutLedger = existing.filter((entry) => entry.orchestratorId !== ledgerId);
        // Passed through `stampMigratedClaims` for the same reason
        // `claimCapacity`'s write path is (round-2 review fix — see
        // `stampMigratedClaims`'s doc comment): a release against a
        // DIFFERENT orchestrator's claim for this `type` still rewrites the
        // whole ledger entry, including any still-legacy record riding
        // along untouched in `nextClaims` — that record must lose its TTL
        // exemption on THIS write too, not only on a future `claimCapacity`
        // write, or it could persist marked-exempt forever inside an
        // already-new-shape `claims[]` array.
        const stampedClaims = stampMigratedClaims(nextClaims, referenceNow);
        // Phase 1: filter dead `count: 0`/`count: -0` records OUT of
        // `stampedClaims` here — AFTER `stampMigratedClaims` runs, so a
        // legitimately-stamped-but-zero-count record is still caught — and
        // BEFORE the `stampedClaims.length > 0` omission guard below
        // evaluates it. See `filterOutZeroCountClaims`'s doc comment for why
        // this closes the "Known residual gap" the omission guard used to
        // describe. A live (`count > 0`) claim is untouched by this filter.
        const liveClaims = filterOutZeroCountClaims(stampedClaims);
        // See `claimCapacity`'s matching write path for why `grantedTotal`
        // is persisted alongside `claims[]` here too — same mixed-version
        // rollout hazard applies to a release write.
        const nextGrantedTotal = sumClaimCounts(liveClaims);
        // Phase 4 fix: a release can fully empty this type's ledger
        // entry (every live claim consumed). `pruneStale` exempts ANY
        // `__claim:<type>__` id from liveness pruning unconditionally (see
        // `claimLedgerEntryId`'s doc comment), so if we wrote the emptied
        // entry back as `{claims: [], grantedTotal: 0}` it would survive on
        // disk forever — this coordination file's one dimension that only
        // ever grows. Instead: when there is nothing left to persist for
        // this type, simply omit the entry from the write array rather than
        // writing it back empty. `withoutLedger` above already strips every
        // OLD entry for this `ledgerId`; we only decide whether to add the
        // new one back. Guard on `liveClaims.length` (not `nextGrantedTotal`)
        // — it is the more direct source of "is there anything left".
        const nextEntries =
          liveClaims.length > 0
            ? [
                ...withoutLedger,
                {
                  orchestratorId: ledgerId,
                  claims: liveClaims,
                  grantedTotal: nextGrantedTotal,
                  declaredAt: referenceNow,
                },
              ]
            : withoutLedger;
        await writeEntriesAtomic(filePath, nextEntries, lockContext);

        return { released, grantedTotal: nextGrantedTotal };
      }

      return { released, grantedTotal: grantedTotalBefore };
    },
    {
      lockStalenessMs: options.lockStalenessMs,
      lockMaxAttempts: options.lockMaxAttempts,
      lockRetryDelayMs: options.lockRetryDelayMs,
    },
  );
}

/**
 * Atomically folds `cli.mjs`'s pre-existing three-lock-cycle admission sequence
 * (`claimCapacity(LIVE_AGENT_CLAIM_TYPE, requestedCount: 0)` ->
 * `releaseCapacity(LIVE_AGENT_ADMISSION_CLAIM_TYPE, ...)` ->
 * `claimCapacity(LIVE_AGENT_ADMISSION_CLAIM_TYPE, ...)`) into ONE `withLock`
 * critical section — one `readEntriesRaw`, one `writeEntriesAtomic` — so a
 * competing beat can never observe (or act on) the intermediate state between
 * those three steps.
 *
 * Reuses `claimCapacity`'s/`releaseCapacity`'s exact same helpers
 * (`normalizeClaims`, `pruneExpiredClaims`, `sumClaimCounts`,
 * `stampMigratedClaims`, `filterOutZeroCountClaims`, `claimLedgerEntryId`) so
 * the on-disk shape this function persists is byte-for-byte equivalent, for
 * the same inputs, to what the old two-call sequence produced.
 *
 * Sequence, all against the ONE `existing = await readEntriesRaw(filePath)`
 * snapshot taken at the top of this critical section:
 *
 *   1. **Snapshot read** — `snapshotType`'s live claim total is read (never
 *      granted against; this mirrors the old `requestedCount: 0` claim, a
 *      pure headroom probe) and `liveAgentCeilingRemaining = max(0, ceiling -
 *      alreadyGrantedTotalForSnapshotType)` is computed.
 *   2. **Own-hold replace** — this orchestrator's own prior live claims
 *      against `admissionType` are excluded from consideration (replace, not
 *      accumulate — mirrors the old unconditional `releaseCapacity` call).
 *   3. **Grant** — `admissionType` is granted
 *      `min(requestedCount, max(0, liveAgentCeilingRemaining - othersHeld))`,
 *      where `othersHeld` is every OTHER orchestrator's live `admissionType`
 *      claim total (this orchestrator's own excluded by step 2).
 *
 * Write gating mirrors `claimCapacity`'s own `granted > 0 ||
 * needsLegacyMigrationWrite` posture, applied independently to each ledger
 * entry: a `requestedCount: 0` call (or one that grants nothing because the
 * ceiling is already exhausted) is a pure snapshot — this orchestrator's own
 * prior `admissionType` hold is left on disk untouched (it ages out via TTL
 * like any other claim record) rather than being force-released with no
 * accompanying grant. `snapshotType`'s entry is likewise left byte-for-byte
 * as read unless it's still legacy-scalar-shaped, in which case it is
 * migrated forward on this read regardless of the admission outcome — same
 * unconditional-migration posture `claimCapacity`/`releaseCapacity` already
 * apply, just evaluated separately for each of the two ledger entries. Both
 * entries are folded into a single `nextEntries` array and persisted via
 * exactly one `writeEntriesAtomic` call, whether or not either entry's
 * content actually changed.
 *
 * `options.testInjectedDelayMs` is a TEST-ONLY hook: when present, this
 * function `await`s a bare `setTimeout` for that many milliseconds exactly
 * once, immediately after `readEntriesRaw` and before any of the computation
 * above or the eventual write — simulating a slow combined critical section
 * the way `claimCapacity`'s pre-existing tests hold its own lock open via a
 * slow `computeAvailableCapacity`. `cli.mjs` must never pass this option.
 * Ownership is re-proven (`lockContext.assertStillHeld()`) immediately after
 * this (simulated) delay, before any of the read state above is consumed —
 * the same rationale `claimCapacity` documents for re-checking the
 * moment its own slow step returns.
 *
 * PID identity: `params.pid`/`params.pidStartedAt` are OPTIONAL,
 * caller-supplied, and stamped onto the `admissionType` claim record this call
 * appends — the same posture, and the same `withPidIdentity` helper,
 * `claimCapacity` uses. `snapshotType`'s entry carries no identity from them
 * (this function never appends a record there).
 *
 * TTL pruning: `snapshotType`'s ledger (Step 1's headroom
 * probe) and `admissionType`'s ledger (Steps 2-3's own-hold-replace/grant)
 * are pruned against two INDEPENDENT TTLs, because they legitimately mean
 * different things — `snapshotType` (`LIVE_AGENT_CLAIM_TYPE`) records a
 * whole live-agent's presence, held for as long as the agent itself is
 * running (hours, per `DEFAULT_CLAIM_TTL_MS`), while `admissionType`
 * (`LIVE_AGENT_ADMISSION_CLAIM_TYPE`) records a much shorter in-flight
 * admission decision. Using one TTL for both (today's bug,) prunes
 * whichever ledger's real lifetime is longer against the shorter TTL,
 * either over- or under-granting depending on which of the two TTLs was
 * passed. `options.snapshotClaimTtlMs` governs `snapshotType`'s pruning;
 * when omitted, it resolves to `options.claimTtlMs` — today's exact
 * single-TTL behavior, unchanged for any caller not yet updated (Phase 2
 * wires the real per-ledger values through `cli.mjs`/`dispatch.mjs`).
 * `options.claimTtlMs` continues to govern `admissionType`'s pruning
 * unconditionally, regardless of what `snapshotClaimTtlMs` is set to.
 *
 * @param {string} filePath
 * @param {{ snapshotType: string, admissionType: string, requestedCount: number, orchestratorId: string, ceiling: number, now?: number, pid?: number, pidStartedAt?: number }} params
 * @param {{ claimTtlMs?: number, snapshotClaimTtlMs?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number, testInjectedDelayMs?: number }} [options]
 * @returns {Promise<{ granted: number, liveAgentCeilingRemaining: number, alreadyGrantedTotalForSnapshotType: number, othersHeld: number }>}
 */
export async function reserveAdmission(
  filePath,
  { snapshotType, admissionType, requestedCount, orchestratorId, ceiling, now, pid, pidStartedAt },
  options = {},
) {
  if (!isNonEmptyString(orchestratorId)) {
    throw new TypeError(
      `reserveAdmission: orchestratorId must be a non-empty string (received ${orchestratorId})`,
    );
  }
  if (isReservedEntry(orchestratorId)) {
    throw new TypeError(
      `reserveAdmission: orchestratorId must not match the reserved double-underscore convention ` +
        `("__...__"), got "${orchestratorId}" — reserved ids collide with internal coordination-file ` +
        'bookkeeping (e.g. the legacy-unattributed-claim sentinel) and are not valid orchestrator ids',
    );
  }
  if (!isNonNegativeInteger(requestedCount)) {
    throw new TypeError(
      `reserveAdmission: requestedCount must be a finite, non-negative integer (received ${requestedCount})`,
    );
  }
  if (!isNonEmptyString(snapshotType)) {
    throw new TypeError(
      `reserveAdmission: snapshotType must be a non-empty string (received ${snapshotType})`,
    );
  }
  if (!isNonEmptyString(admissionType)) {
    throw new TypeError(
      `reserveAdmission: admissionType must be a non-empty string (received ${admissionType})`,
    );
  }
  if (snapshotType === admissionType) {
    throw new TypeError(
      `reserveAdmission: snapshotType and admissionType must be distinct claim ledger types, both received ` +
        `"${snapshotType}" — sharing one type would collide the two ledger entries this function writes ` +
        'under the same __claim:<type>__ id, silently double-counting every claim on the next read',
    );
  }
  // `isNonNegativeInteger` (already the established helper for exactly this
  // shape — see `requestedCount`'s check above) rather than the looser
  // `isFiniteNumber` this guard originally used alone: a finite but
  // non-integer ceiling (e.g. `3.5`, from a misconfigured AIMD-adjusted
  // ceiling —) or a negative one (e.g. `-1`, from an underflowed
  // decrement) both sailed past the old check and were only clamped
  // downstream by `Math.max(0, ceiling)` at Step 1 — silently, rather than
  // loudly at the validation boundary. `ceiling: 0` remains a legitimate (if
  // degenerate) value and must NOT be rejected here.
  if (!isNonNegativeInteger(ceiling)) {
    throw new TypeError(
      `reserveAdmission: ceiling must be a finite, non-negative integer (received ${ceiling})`,
    );
  }

  // Resolved snapshot TTL: `options.snapshotClaimTtlMs` when
  // set, falling back to `options.claimTtlMs` otherwise — see this
  // function's own doc comment above for why the two ledgers need
  // independent TTLs. `admissionType` is never affected by this fallback;
  // it always resolves to `options.claimTtlMs` directly.
  const resolvedSnapshotClaimTtlMs = isSetClaimTtl(options.snapshotClaimTtlMs)
    ? options.snapshotClaimTtlMs
    : options.claimTtlMs;

  // Same pre-lock guard `claimCapacity`/`releaseCapacity` apply, called
  // explicitly once per ledger type (`snapshotType` against the resolved
  // snapshot TTL, `admissionType` against `claimTtlMs`) — see
  // `assertClaimTtlSetAgainstLegacyLedger`'s doc comment for why this is
  // safe to make read-only and pre-lock.
  const referenceNowForGuard = isFiniteNumber(now) ? now : Date.now();
  if (!isSetClaimTtl(resolvedSnapshotClaimTtlMs)) {
    await assertClaimTtlSetAgainstLegacyLedger(
      filePath,
      snapshotType,
      resolvedSnapshotClaimTtlMs,
      referenceNowForGuard,
    );
  }
  if (!isSetClaimTtl(options.claimTtlMs)) {
    await assertClaimTtlSetAgainstLegacyLedger(filePath, admissionType, options.claimTtlMs, referenceNowForGuard);
  }

  return withLock(
    filePath,
    async (lockContext) => {
      const referenceNow = isFiniteNumber(now) ? now : Date.now();
      const existing = await readEntriesRaw(filePath);

      // Test-only hook — see this function's doc comment.
      // Never fired in production; `cli.mjs` never passes this option.
      if (isFiniteNumber(options.testInjectedDelayMs)) {
        await new Promise((resolve) => setTimeout(resolve, options.testInjectedDelayMs));
      }

      // Re-prove ownership as soon as the (simulated) slow part of this
      // section finishes, before any of the state read above is consumed —
      // same rationale `claimCapacity` applies to its own slow step.
      await lockContext.assertStillHeld();

      // --- Step 1: snapshotType's ceiling headroom (today's cycle 1) ---
      const snapshotLedgerId = claimLedgerEntryId(snapshotType);
      const matchingSnapshotEntries = existing.filter((entry) => entry.orchestratorId === snapshotLedgerId);
      const existingSnapshotClaims = matchingSnapshotEntries.flatMap((entry) =>
        normalizeClaims(entry, referenceNow),
      );
      const liveSnapshotClaims = pruneExpiredClaims(existingSnapshotClaims, referenceNow, resolvedSnapshotClaimTtlMs);
      const alreadyGrantedTotalForSnapshotType = sumClaimCounts(liveSnapshotClaims);
      const ceilingSafe = isFiniteNumber(ceiling) ? Math.max(0, ceiling) : 0;
      const liveAgentCeilingRemaining = Math.max(0, ceilingSafe - alreadyGrantedTotalForSnapshotType);
      const needsSnapshotLegacyMigrationWrite = existingSnapshotClaims.some((claim) => claim?.legacyMigrated);

      // --- Steps 2 & 3: admissionType's ledger — replace this
      // orchestrator's own prior hold, then grant against the snapshot minus
      // what OTHER orchestrators hold (today's cycles 2 and 3) ---
      const admissionLedgerId = claimLedgerEntryId(admissionType);
      const matchingAdmissionEntries = existing.filter((entry) => entry.orchestratorId === admissionLedgerId);
      const existingAdmissionClaims = matchingAdmissionEntries.flatMap((entry) =>
        normalizeClaims(entry, referenceNow),
      );
      const liveAdmissionClaims = pruneExpiredClaims(existingAdmissionClaims, referenceNow, options.claimTtlMs);
      const needsAdmissionLegacyMigrationWrite = existingAdmissionClaims.some((claim) => claim?.legacyMigrated);

      // Step 2 — this orchestrator's own prior hold is replaced, not
      // accumulated: excluded from consideration before computing what
      // OTHER orchestrators hold. A first-ever call (no prior own-hold)
      // simply has nothing to exclude here — a clean no-op, never a throw.
      const claimsAfterOwnRelease = liveAdmissionClaims.filter(
        (claim) => claim?.orchestratorId !== orchestratorId,
      );
      const othersHeld = sumClaimCounts(claimsAfterOwnRelease);

      // Did this orchestrator hold anything (live) BEFORE this call? If so,
      // that prior hold must be replaced by this write even when the fresh
      // grant below computes to zero — otherwise a stale nonzero hold from
      // an earlier round survives untouched (counted against every OTHER
      // orchestrator's `othersHeld`) until it ages out via TTL, hours later.
      // Ceiling headroom legitimately fluctuates round to round as other
      // orchestrators register/deregister live agents, so a zero-grant round
      // is a real outcome this orchestrator must still see reflected.
      const ownHadPriorLiveHold = claimsAfterOwnRelease.length !== liveAdmissionClaims.length;

      // Step 3 — grant sized to Step 1's snapshot minus other holders,
      // clamped to zero (never negative, whether from an exhausted ceiling
      // or a misconfigured non-positive `ceiling` itself) and never above
      // what was requested.
      const granted = Math.max(0, Math.min(requestedCount, Math.max(0, liveAgentCeilingRemaining - othersHeld)));

      // --- Assemble the single write ---
      const nextEntries = existing.filter(
        (entry) => entry.orchestratorId !== snapshotLedgerId && entry.orchestratorId !== admissionLedgerId,
      );

      // snapshotType's entry: left untouched unless a still-legacy-shaped
      // record needs migrating forward — independent of the admission
      // outcome, mirroring claimCapacity's own unconditional-migration fix.
      if (needsSnapshotLegacyMigrationWrite) {
        const stampedSnapshotClaims = stampMigratedClaims(liveSnapshotClaims, referenceNow);
        const liveSnapshotNextClaims = filterOutZeroCountClaims(stampedSnapshotClaims);
        if (liveSnapshotNextClaims.length > 0) {
          nextEntries.push({
            orchestratorId: snapshotLedgerId,
            claims: liveSnapshotNextClaims,
            grantedTotal: sumClaimCounts(liveSnapshotNextClaims),
            declaredAt: referenceNow,
          });
        }
      } else {
        nextEntries.push(...matchingSnapshotEntries);
      }

      // admissionType's entry: a pure snapshot (granted === 0, no legacy
      // migration pending, AND this orchestrator held nothing live before
      // this call) leaves it entirely untouched — mirrors claimCapacity's
      // own `granted > 0 || needsLegacyMigrationWrite` write gating. A
      // genuine grant, a pending legacy migration, OR a prior own-hold that
      // this zero-grant round must clear persists this orchestrator's own
      // hold REPLACED (never accumulated) plus the new grant (if any).
      if (granted > 0 || needsAdmissionLegacyMigrationWrite || ownHadPriorLiveHold) {
        const claimsWithGrant =
          granted > 0
            ? [
                ...claimsAfterOwnRelease,
                withPidIdentity({ orchestratorId, count: granted, claimedAt: referenceNow }, pid, pidStartedAt),
              ]
            : claimsAfterOwnRelease;
        const stampedAdmissionClaims = stampMigratedClaims(claimsWithGrant, referenceNow);
        const liveAdmissionNextClaims = filterOutZeroCountClaims(stampedAdmissionClaims);
        if (liveAdmissionNextClaims.length > 0) {
          nextEntries.push({
            orchestratorId: admissionLedgerId,
            claims: liveAdmissionNextClaims,
            grantedTotal: sumClaimCounts(liveAdmissionNextClaims),
            declaredAt: referenceNow,
          });
        }
      } else {
        nextEntries.push(...matchingAdmissionEntries);
      }

      await writeEntriesAtomic(filePath, nextEntries, lockContext);

      return {
        granted,
        liveAgentCeilingRemaining,
        alreadyGrantedTotalForSnapshotType,
        othersHeld,
      };
    },
    {
      lockStalenessMs: options.lockStalenessMs,
      lockMaxAttempts: options.lockMaxAttempts,
      lockRetryDelayMs: options.lockRetryDelayMs,
    },
  );
}

/**
 * Phase 2 review, Medium finding (stability) — the memory-projection
 * admission gate's own atomic critical section.
 *
 * Phase 2's original gate derived its "two racing beats can't both be
 * admitted" property purely from an INDUCTIVE PROOF over `declareDibs`'s
 * pre-existing lock ordering (see `cli.mjs`'s own — now-removed — comment on
 * that gate): real today, but not owned or asserted by the gate itself, and
 * silently breakable by an unrelated future refactor of `declareDibs`/the
 * dibs schema. This function instead gives the gate a single `withLock`
 * critical section of its own, mirroring `reserveAdmission`'s own idiom
 * immediately above: one re-snapshot read, one caller-supplied decision, one
 * conditional write — all under ONE lock acquisition, so a competing beat
 * can never observe intermediate state between "read the live dibs ledger"
 * and "decide, and (on refusal) retract this orchestrator's own
 * contribution".
 *
 * `decide(liveDibs)` is caller-supplied because the actual projection
 * computation (`computeProjectedPeakMemoryMb`, per-class history via
 * `readHistory`) is owned by `cli.mjs`/`./memory-projection.mjs`/
 * `./history.mjs`, not by this coordination-file module — this function's
 * job is only the locking and the retraction, not the pricing. `decide` may
 * be async (history reads are I/O) and must return
 * `{ withinBudget: boolean, ...anything else the caller wants back }`; that
 * whole object is returned verbatim by this function. History reads are
 * lock-free by design (`./history.mjs`'s own doc comment: only
 * `recordObservation`'s write path locks, and that lock is against the
 * SEPARATE history file, never this coordination file) — calling into them
 * from inside this lock introduces no deadlock risk and no cross-file lock
 * ordering to reason about.
 *
 * Lock-ordering note (documented once, here, as the canonical answer): this
 * lock is NEVER acquired from inside `reserveAdmission`'s own `withLock`
 * callback, nor vice versa — `cli.mjs`'s `main()` calls this function to
 * completion (lock fully released) BEFORE ever calling `reserveAdmission`,
 * strictly sequentially, matching the existing before-the-live-agent-ceiling
 * ordering the memory-projection gate has always used. Two independent,
 * never-nested critical sections against the same file are safe by
 * construction — `withLock`'s own file-lock mechanism already serializes
 * them — and this ordering is pinned by
 * `../cli-memory-projection-wiring.jest.spec.mjs` test 7.
 *
 * Retraction (Phase 2 review, Low finding, security; review,
 * High finding, correctness) — a beat refused here already had `declareDibs`
 * commit its dibs entry (see `cli.mjs`'s `main()`: `declareDibs` runs BEFORE
 * this gate, so hysteresis history/liveness stay current across a refusal).
 * Left as-is, that entry's `agentClasses`/legacy `agentClass` field would
 * keep over-counting this beat's (never-admitted) demand against every OTHER
 * concurrent orchestrator's projected sum until the entry's liveness TTL
 * expires. On refusal (`!decision.withinBudget`), this function removes ONLY
 * `candidateClass` — the class this specific beat was attempting to add —
 * from this orchestrator's own just-declared `agentClasses`/legacy
 * `agentClass` field.
 *
 * This is a narrower retraction than an earlier version of this function
 * performed: that version dropped `agentClasses`/`agentClass` WHOLESALE on
 * refusal, which silently wiped every OTHER class this same orchestrator
 * declared via `--running-agent-classes` — classes that are genuinely,
 * currently running, not candidates for this beat at all — making them
 * invisible to every other orchestrator's projected-peak sum. That is the
 * exact undercounting failure mode this whole gate exists to prevent, just
 * reached from the refusal path instead of the admission path. Removing only
 * `candidateClass` keeps those other genuinely-running classes visible.
 * Every other field (`desiredAgents`, hysteresis history,
 * `declaredAt`/`firstDeclaredAt`) is preserved untouched regardless, so this
 * remains a narrow retraction, not a re-declaration.
 *
 * `knownRunningClasses` (review, High finding, correctness) closes a
 * gap the class-name-only removal above cannot see on its own: `cli.mjs`
 * deduplicates `candidateClass` against this SAME orchestrator's own
 * `--running-agent-classes` before ever writing the dibs entry (a class named
 * twice contributes one entry — see `dibsAgentClasses`'s own comment in
 * `cli.mjs`). When `candidateClass` is ALSO one of those genuinely-running
 * classes (e.g. one `implementer` is already running and this beat is asking
 * to admit a SECOND `implementer`), a bare `!== candidateClass` filter cannot
 * tell "the refused candidate" apart from "an already-running agent of the
 * identical class name" — both are the same string in a deduplicated set —
 * so refusing the candidate would also erase the genuinely-running instance,
 * making it invisible to every other orchestrator until this orchestrator's
 * NEXT beat re-declares it. Passing this orchestrator's own
 * `--running-agent-classes` value lets the retraction skip removing
 * `candidateClass` entirely whenever it is independently known to be
 * genuinely running, rather than only ever attempting a refused candidacy.
 *
 * [accepted risk, ] mixed-version fleet: this function, and the whole
 * memory-projection gate around it, is only ever CALLED by a Phase-2-or-later
 * `cli.mjs` binary. An orchestrator still running a pre-Phase-2 binary never
 * calls `reconcileMemoryProjectionAdmission` at all — it declares no
 * `agentClasses`/`agentClass` dibs field for this function's `liveDibs`
 * snapshot to see, and its own real memory footprint is therefore invisible
 * to every other (upgraded) orchestrator's `decide` sum until it upgrades.
 * Mirrors the already-accepted `[accepted risk, ] mixed-version fleet`
 * gap on `reserveAdmission`'s ceiling param (see `cli.mjs`'s
 * `LIVE_AGENT_CEILING` doc comment) — this function cannot retroactively
 * enforce a gate on a caller that never invokes it. Pinned by
 * `../cli-memory-projection-mixed-version-fleet.jest.spec.mjs`.
 *
 * @param {string} filePath
 * @param {{ orchestratorId: string, now?: number, candidateClass: string, knownRunningClasses?: string[], decide: (liveDibs: Array<object>) => Promise<{ withinBudget: boolean, [key: string]: unknown }> | { withinBudget: boolean, [key: string]: unknown } }} params
 * @param {{ livenessThresholdMs?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<{ withinBudget: boolean, [key: string]: unknown }>}
 */
export async function reconcileMemoryProjectionAdmission(
  filePath,
  { orchestratorId, now, candidateClass, knownRunningClasses = [], decide },
  options = {},
) {
  if (!isNonEmptyString(orchestratorId)) {
    throw new TypeError(
      `reconcileMemoryProjectionAdmission: orchestratorId must be a non-empty string (received ${orchestratorId})`,
    );
  }
  if (!isNonEmptyString(candidateClass)) {
    throw new TypeError(
      `reconcileMemoryProjectionAdmission: candidateClass must be a non-empty string (received ${candidateClass}) ` +
        '— the retraction path needs it to know which class to remove on refusal without disturbing this ' +
        "orchestrator's other genuinely-running classes",
    );
  }
  if (typeof decide !== 'function') {
    throw new TypeError('reconcileMemoryProjectionAdmission: decide must be a function');
  }

  return withLock(
    filePath,
    async (lockContext) => {
      const referenceNow = isFiniteNumber(now) ? now : Date.now();
      const existing = await readEntriesRaw(filePath);
      const liveDibs = pruneStale(existing, referenceNow, options.livenessThresholdMs);

      const decision = await decide(liveDibs);

      if (decision && decision.withinBudget === false) {
        const selfEntry = existing.find((entry) => entry.orchestratorId === orchestratorId);
        const selfCarriesAgentClass =
          selfEntry !== undefined && (Array.isArray(selfEntry.agentClasses) || typeof selfEntry.agentClass === 'string');
        if (selfCarriesAgentClass) {
          // Remove ONLY `candidateClass` — the class this refused beat was
          // attempting to add — never the whole field. `selfEntry`'s
          // `agentClasses`/`agentClass` may also carry OTHER classes this
          // same orchestrator declared via `--running-agent-classes`
          // (genuinely running, not part of this beat's own refused
          // candidacy); those must survive this write untouched.
          //
          // review, High finding — but NEVER remove `candidateClass`
          // when it is independently named in `knownRunningClasses`: that
          // means this same orchestrator has a genuinely-running agent of
          // this exact class, deduplicated into the same entry as the
          // refused candidacy (see this function's own doc comment). Erasing
          // it here would hide that still-running agent from every other
          // orchestrator's projected sum until this orchestrator's next
          // beat re-declares it.
          const currentClasses = readEntryAgentClasses(selfEntry);
          const retainedClasses = knownRunningClasses.includes(candidateClass)
            ? currentClasses
            : currentClasses.filter((agentClass) => agentClass !== candidateClass);
          const { agentClasses: _droppedAgentClasses, agentClass: _droppedAgentClass, ...retainedFields } = selfEntry;
          const nextSelfEntry =
            retainedClasses.length > 0 ? { ...retainedFields, agentClasses: retainedClasses } : retainedFields;
          const nextEntries = existing.map((entry) => (entry.orchestratorId === orchestratorId ? nextSelfEntry : entry));
          await writeEntriesAtomic(filePath, nextEntries, lockContext);
        }
      }

      return decision;
    },
    {
      lockStalenessMs: options.lockStalenessMs,
      lockMaxAttempts: options.lockMaxAttempts,
      lockRetryDelayMs: options.lockRetryDelayMs,
    },
  );
}

/**
 * Genuine locked compare-and-swap write for the shared-machine-sample
 * reserved entry. Takes an already-taken `reading` — this function
 * NEVER shells out or samples anything itself; the expensive `collect()`
 * shell-out lives entirely in `cli.mjs`, outside this module.
 *
 * Inside a `withLock` critical section (the same fencing lock every other
 * write in this module shares), re-reads whatever is CURRENTLY persisted
 * under the reserved id and only persists `reading` if:
 *   - `reading.stale` is falsy (a stale/degraded reading is NEVER persisted,
 *     regardless of any comparison against what's currently stored), AND
 *   - EITHER no entry is currently persisted, OR the persisted entry is
 *     itself degraded (missing a well-formed `reading`/`sampledAt`, or
 *     itself flagged `stale`), OR `reading.sampledAt` is strictly newer than
 *     the persisted entry's `sampledAt`.
 *
 * This closes the race a weaker "just serialize, unconditional overwrite"
 * write would not: two genuinely concurrent writers racing the same lock
 * must never let the OLDER sample win just because its write happened to run
 * second — an empirical two-process race proved that failure mode occurs
 * ~40% of the time without a genuine `sampledAt` comparison performed AFTER
 * re-reading current state under the lock (see, this file's test
 * suite, and the outer acceptance test in ./cli-two-orchestrators.jest.spec.mjs
 * test 7).
 *
 * @param {string} filePath
 * @param {{ sampledAt: number, stale?: boolean, [key: string]: unknown }} reading
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<{ persisted: boolean, reason?: string }>}
 */
export async function writeSharedSample(filePath, reading, options = {}) {
  if (!reading || reading.stale) {
    return { persisted: false, reason: 'stale-reading' };
  }

  // A malformed reading — missing `sampledAt`, or a non-finite value such as
  // `NaN`/a string — must never sail into the ledger. Mirrors
  // `releaseCapacity`'s validate-before-lock pattern: reject BEFORE ever
  // acquiring the lock, the same way the `stale` check above already does,
  // rather than letting an invalid `sampledAt` reach the CAS comparison
  // below (where `existingIsDegraded || reading.sampledAt > existingEntry.sampledAt`
  // would otherwise silently coerce a non-numeric `sampledAt` via `>`).
  if (!isFiniteNumber(reading.sampledAt)) {
    return { persisted: false, reason: 'invalid-sampled-at' };
  }

  return withLock(
    filePath,
    async (lockContext) => {
      const existing = await readEntriesRaw(filePath);
      const existingEntry = existing.find((entry) => entry.orchestratorId === SHARED_SAMPLE_RESERVED_ID);
      const existingIsDegraded =
        !existingEntry ||
        !existingEntry.reading ||
        existingEntry.reading?.stale ||
        !isFiniteNumber(existingEntry.sampledAt);

      const shouldPersist = existingIsDegraded || reading.sampledAt > existingEntry.sampledAt;

      if (!shouldPersist) {
        return { persisted: false, reason: 'not-newer-than-persisted' };
      }

      const withoutExisting = existing.filter((entry) => entry.orchestratorId !== SHARED_SAMPLE_RESERVED_ID);
      const nextEntry = {
        orchestratorId: SHARED_SAMPLE_RESERVED_ID,
        sampledAt: reading.sampledAt,
        reading,
        // Reuse `reading.sampledAt` (already threaded from the caller's own
        // clock — `cli.mjs`'s `resolveNow()`/`ARM_FAKE_NOW_MS` seam, see
        // `sampledAt` above) rather than a fresh `Date.now()` call. This
        // keeps `declaredAt` and `sampledAt` on the same clock for this
        // entry: under `ARM_FAKE_NOW_MS` both fields advance together,
        // instead of a fake `sampledAt` sitting next to a real-wall-clock
        // `declaredAt`.
        declaredAt: reading.sampledAt,
      };
      await writeEntriesAtomic(filePath, [...withoutExisting, nextEntry], lockContext);

      return { persisted: true };
    },
    {
      lockStalenessMs: options.lockStalenessMs,
      lockMaxAttempts: options.lockMaxAttempts,
      lockRetryDelayMs: options.lockRetryDelayMs,
    },
  );
}

/**
 * A persisted `__aimd-ceiling-state__` row exists but failed the shape/
 * validity check — as opposed to genuinely being absent (cold start) — so it
 * is corrupted (disk corruption, a bad hand-edit, or a future bug writing the
 * wrong shape). Falling back to the safe default is the right direction, but
 * doing so silently would make a corrupted row indistinguishable from a
 * first-ever run. Name the malformed field(s) so an operator watching stderr
 * can tell the two apart.
 *
 * Shared by both of `readAimdCeilingState`'s branches so the persisting and
 * non-persisting reads warn identically — the byte written to disk is the
 * only difference between them.
 *
 * @param {{ ceiling?: unknown, sustainedNormalCount?: unknown }} existingEntry
 */
function warnMalformedAimdCeilingState(existingEntry) {
  const malformedFields = [];
  if (!isNonNegativeInteger(existingEntry.ceiling)) {
    malformedFields.push(`ceiling=${JSON.stringify(existingEntry.ceiling)}`);
  }
  if (!isNonNegativeInteger(existingEntry.sustainedNormalCount)) {
    malformedFields.push(`sustainedNormalCount=${JSON.stringify(existingEntry.sustainedNormalCount)}`);
  }
  console.warn(
    `agent-resource-management: warning: persisted AIMD ceiling state is malformed (${malformedFields.join(', ')}); ` +
      `resetting to the cold-start default (ceiling=${DEFAULT_AIMD_CEILING_STATE.ceiling}, ` +
      `sustainedNormalCount=${DEFAULT_AIMD_CEILING_STATE.sustainedNormalCount})`,
  );
}

/**
 * Reads the AIMD concurrency-ceiling shared state (Phase 2/3),
 * persisted as a reserved entry (see `AIMD_CEILING_RESERVED_ID`/
 * `isReservedEntry`) in the same coordination file `declareDibs`/`readDibs`
 * use.
 *
 * This is the ONLY sanctioned way to read this entry. Reading the reserved
 * entry directly out of `readDibs`/`readEntriesRaw` is not forbidden by
 * anything mechanical, but it is discouraged: it bypasses the lazy
 * cold-start initialization below, so a caller that reads before the entry
 * has ever been written would see `undefined` rather than the seeded
 * default `{ ceiling: 4, sustainedNormalCount: 0 }` this function
 * guarantees. Prefer this function (or `writeAimdCeilingState`) over a raw
 * read/write against the entry's id.
 *
 * On the very first call against a coordination file with no prior AIMD
 * ceiling entry, this both RETURNS and PERSISTS `DEFAULT_AIMD_CEILING_STATE`
 * — mirroring `consumeGlobalSpawnTokens`'s "first-ever call initializes the
 * bucket" shape. The whole read-check-write happens inside a single
 * `withLock` critical section (never a bare unlocked write), so two
 * concurrent first-ever readers racing the same lock are serialized by it
 * and converge on exactly ONE persisted entry: the second (post-lock-wait)
 * reader re-reads under its own lock acquisition and finds the first
 * reader's write already there, so it returns that persisted state instead
 * of writing a second, duplicate entry.
 *
 * `options.persistColdStart: false` (Phase 3, second review round)
 * turns that seeding OFF and, with it, the lock — for callers that must be
 * provably non-mutating. This is not a micro-optimisation; it fixes two
 * concrete defects on `cli.mjs`'s `--advise-only` path:
 *
 *   1. WRITE. The two seeding branches below (row ABSENT, and row PRESENT
 *      BUT MALFORMED) are the only remaining writes an advisory run could
 *      reach. "Read-only" was therefore true only for a coordination file
 *      that already happened to carry a well-formed `__aimd-ceiling-state__`
 *      row — i.e. false on exactly the cold-start and corrupted-file cases.
 *   2. LOCK. Even the well-formed-row case took the shared coordination lock
 *      with this module's default budget (2000 attempts x 5ms + jitter, so
 *      ~10-20s, and longer still while an orphaned lock waits out its 30s
 *      staleness window). The ARM PreToolUse gate gives `cli.mjs` 5s total,
 *      so ANY concurrent `declareDibs`/`reserveAdmission`/
 *      `consumeGlobalSpawnTokens`/watchdog sweep could push an advisory run
 *      past that budget and turn a perfectly healthy host into a fail-closed
 *      deny. Measured before the fix: 18.1s against a freshly-held lock.
 *
 * In this mode the cold-start/malformed default is returned IN MEMORY only,
 * exactly as if it had been seeded — the caller sees the identical value, so
 * no classification changes; only the byte on disk does not. The read itself
 * is a bare `readEntriesRaw`, matching `readDibs`/`hasEverOptedIn`, the
 * module's other lock-free advisory reads. That is safe for the same reason
 * theirs is: a lock-free reader can only ever observe the whole prior file or
 * the whole new one (`writeEntriesAtomic` renames), so the worst case is a
 * value one write stale — and an advisory verdict that reasons over a
 * one-beat-old ceiling is precisely the fidelity `--advise-only` already
 * documents itself as trading away.
 *
 * @param {string} filePath
 * @param {{ now?: number }} [params]
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number, persistColdStart?: boolean }} [options]
 * @returns {Promise<{ ceiling: number, sustainedNormalCount: number }>}
 */
export async function readAimdCeilingState(filePath, { now } = {}, options = {}) {
  const persistColdStart = options.persistColdStart !== false;

  // Lock-free, write-free variant. Deliberately duplicates the read/validate/
  // warn shape rather than threading a flag through the locked closure below:
  // the whole point is that this branch never enters `withLock` at all, and a
  // shared body would leave that as a runtime property of a boolean instead
  // of a structural one visible at a glance.
  if (!persistColdStart) {
    const existing = await readEntriesRaw(filePath);
    const existingEntry = existing.find((entry) => entry.orchestratorId === AIMD_CEILING_RESERVED_ID);

    if (
      existingEntry &&
      isNonNegativeInteger(existingEntry.ceiling) &&
      isNonNegativeInteger(existingEntry.sustainedNormalCount)
    ) {
      return { ceiling: existingEntry.ceiling, sustainedNormalCount: existingEntry.sustainedNormalCount };
    }

    if (existingEntry) {
      warnMalformedAimdCeilingState(existingEntry);
    }

    return { ...DEFAULT_AIMD_CEILING_STATE };
  }

  return withLock(
    filePath,
    async (lockContext) => {
      const existing = await readEntriesRaw(filePath);
      const existingEntry = existing.find((entry) => entry.orchestratorId === AIMD_CEILING_RESERVED_ID);

      if (
        existingEntry &&
        isNonNegativeInteger(existingEntry.ceiling) &&
        isNonNegativeInteger(existingEntry.sustainedNormalCount)
      ) {
        return { ceiling: existingEntry.ceiling, sustainedNormalCount: existingEntry.sustainedNormalCount };
      }

      if (existingEntry) {
        warnMalformedAimdCeilingState(existingEntry);
      }

      // No prior entry (or a malformed one, now warned about above) —
      // persist the cold-start default now, under this same lock
      // acquisition, so a concurrent reader that loses the race to this
      // one sees it already persisted once it gets its own turn under the
      // lock, rather than racing a second, duplicate write.
      const withoutExisting = existing.filter((entry) => entry.orchestratorId !== AIMD_CEILING_RESERVED_ID);
      const nextEntry = {
        orchestratorId: AIMD_CEILING_RESERVED_ID,
        ceiling: DEFAULT_AIMD_CEILING_STATE.ceiling,
        sustainedNormalCount: DEFAULT_AIMD_CEILING_STATE.sustainedNormalCount,
        declaredAt: isFiniteNumber(now) ? now : Date.now(),
      };
      await writeEntriesAtomic(filePath, [...withoutExisting, nextEntry], lockContext);

      return { ceiling: nextEntry.ceiling, sustainedNormalCount: nextEntry.sustainedNormalCount };
    },
    {
      lockStalenessMs: options.lockStalenessMs,
      lockMaxAttempts: options.lockMaxAttempts,
      lockRetryDelayMs: options.lockRetryDelayMs,
    },
  );
}

/**
 * Upserts the AIMD concurrency-ceiling shared state (Phase 2/3) under
 * the reserved entry (`AIMD_CEILING_RESERVED_ID`). This is the ONLY
 * sanctioned way to mutate this entry — see `readAimdCeilingState`'s doc
 * comment for why a bare unlocked write against the entry's id is
 * discouraged.
 *
 * The whole read-modify-write happens inside a single `withLock` critical
 * section, the same fencing lock every other write in this module shares —
 * never a bare `fs.writeFile`. Two concurrent writers racing the same lock
 * are serialized by it: whichever acquires the lock first fully persists
 * its `{ ceiling, sustainedNormalCount }` pair before the second writer's
 * critical section even reads the file, so the final persisted entry always
 * reflects ONE writer's complete pair — never a torn/hybrid mix of one
 * writer's `ceiling` with the other's `sustainedNormalCount`.
 *
 * @param {string} filePath
 * @param {{ ceiling: number, sustainedNormalCount: number }} state
 * @param {{ now: number }} params
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<{ ceiling: number, sustainedNormalCount: number }>}
 */
export async function writeAimdCeilingState(filePath, { ceiling, sustainedNormalCount }, { now } = {}, options = {}) {
  if (!isNonNegativeInteger(ceiling)) {
    throw new TypeError(
      `writeAimdCeilingState: ceiling must be a finite, non-negative integer (received ${ceiling})`,
    );
  }
  if (!isNonNegativeInteger(sustainedNormalCount)) {
    throw new TypeError(
      `writeAimdCeilingState: sustainedNormalCount must be a finite, non-negative integer (received ${sustainedNormalCount})`,
    );
  }

  return withLock(
    filePath,
    async (lockContext) => {
      const existing = await readEntriesRaw(filePath);
      const withoutExisting = existing.filter((entry) => entry.orchestratorId !== AIMD_CEILING_RESERVED_ID);
      const nextEntry = {
        orchestratorId: AIMD_CEILING_RESERVED_ID,
        ceiling,
        sustainedNormalCount,
        declaredAt: isFiniteNumber(now) ? now : Date.now(),
      };
      await writeEntriesAtomic(filePath, [...withoutExisting, nextEntry], lockContext);

      return { ceiling: nextEntry.ceiling, sustainedNormalCount: nextEntry.sustainedNormalCount };
    },
    {
      lockStalenessMs: options.lockStalenessMs,
      lockMaxAttempts: options.lockMaxAttempts,
      lockRetryDelayMs: options.lockRetryDelayMs,
    },
  );
}

/**
 * Phase 3 (post-implementation correctness fix) — atomically reads,
 * computes, and persists the AIMD concurrency-ceiling shared state in ONE
 * `withLock` critical section, mirroring `consumeGlobalSpawnTokens`'s own
 * atomic read-modify-write shape for the (also shared, also
 * concurrently-mutated) global spawn-rate bucket.
 *
 * Why this exists alongside `readAimdCeilingState`/`writeAimdCeilingState`
 * rather than composing them from the caller: `main()`'s `--desired-agents`
 * beat is the ONE call site that both reads AND advances this state (see
 * that beat's own doc comment in cli.mjs) — calling `readAimdCeilingState`
 * and `writeAimdCeilingState` as two SEPARATE lock acquisitions from there
 * leaves a window between them where a concurrently-racing beat's own
 * read-compute-write can interleave, producing a classic lost update: beat A
 * reads count=4, computes an additive increase (ceiling 4→5, count reset to
 * 0) and writes it; beat B, having read the SAME pre-increase count=4
 * slightly earlier, computes its own (now-stale) "no increase yet" result
 * (ceiling unchanged at 4, count→5) and writes AFTER A — silently
 * regressing the persisted ceiling back down from 5 to 4 and discarding A's
 * genuine increase, even though every individual read and write was itself
 * correctly locked. This was found empirically: a 14-way concurrent
 * `--desired-agents` race under sustained Normal pressure
 * (`./cli-two-orchestrators.jest.spec.mjs`'s and
 * `./outer-acceptance.jest.spec.mjs`'s cohort-race tests) could grant more
 * live-agent admissions than the FINAL persisted ceiling reflected — proof
 * the two-lock-acquisition composition was unsound under real concurrency,
 * not merely a theoretical race.
 *
 * `computeNextState(currentState)` is a plain, synchronous, PURE function —
 * intended to be `(currentState) => computeAimdCeiling({ pressureLevel,
 * ...currentState }, config)` (`./aimd-ceiling.mjs`) at the one call site
 * that uses this — invoked once, inside the lock, against whatever state
 * this call itself just read (never a caller-captured, possibly-stale
 * snapshot from before the lock was acquired). Malformed persisted
 * `ceiling`/`sustainedNormalCount` fields are handled exactly like
 * `readAimdCeilingState`'s own cold-start/malformed-entry recovery (warn,
 * reseed to `DEFAULT_AIMD_CEILING_STATE`) before `computeNextState` ever
 * sees them, so callers never observe `NaN`/non-integer inputs.
 *
 * @param {string} filePath
 * @param {(currentState: { ceiling: number, sustainedNormalCount: number }) => { ceiling: number, sustainedNormalCount: number }} computeNextState
 * @param {{ now?: number }} [params]
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<{ ceiling: number, sustainedNormalCount: number }>}
 */
export async function advanceAimdCeilingState(filePath, computeNextState, { now } = {}, options = {}) {
  return withLock(
    filePath,
    async (lockContext) => {
      const existing = await readEntriesRaw(filePath);
      const existingEntry = existing.find((entry) => entry.orchestratorId === AIMD_CEILING_RESERVED_ID);

      let currentState = DEFAULT_AIMD_CEILING_STATE;
      if (existingEntry) {
        if (isNonNegativeInteger(existingEntry.ceiling) && isNonNegativeInteger(existingEntry.sustainedNormalCount)) {
          currentState = { ceiling: existingEntry.ceiling, sustainedNormalCount: existingEntry.sustainedNormalCount };
        } else {
          const malformedFields = [];
          if (!isNonNegativeInteger(existingEntry.ceiling)) {
            malformedFields.push(`ceiling=${JSON.stringify(existingEntry.ceiling)}`);
          }
          if (!isNonNegativeInteger(existingEntry.sustainedNormalCount)) {
            malformedFields.push(`sustainedNormalCount=${JSON.stringify(existingEntry.sustainedNormalCount)}`);
          }
          console.warn(
            `agent-resource-management: warning: persisted AIMD ceiling state is malformed (${malformedFields.join(', ')}); ` +
              `resetting to the cold-start default (ceiling=${DEFAULT_AIMD_CEILING_STATE.ceiling}, ` +
              `sustainedNormalCount=${DEFAULT_AIMD_CEILING_STATE.sustainedNormalCount}) before computing this beat's update`,
          );
        }
      }

      const nextState = computeNextState(currentState);
      if (!isNonNegativeInteger(nextState.ceiling)) {
        throw new TypeError(
          `advanceAimdCeilingState: computeNextState must return a finite, non-negative integer ceiling (received ${nextState.ceiling})`,
        );
      }
      if (!isNonNegativeInteger(nextState.sustainedNormalCount)) {
        throw new TypeError(
          `advanceAimdCeilingState: computeNextState must return a finite, non-negative integer sustainedNormalCount (received ${nextState.sustainedNormalCount})`,
        );
      }

      const withoutExisting = existing.filter((entry) => entry.orchestratorId !== AIMD_CEILING_RESERVED_ID);
      const nextEntry = {
        orchestratorId: AIMD_CEILING_RESERVED_ID,
        ceiling: nextState.ceiling,
        sustainedNormalCount: nextState.sustainedNormalCount,
        declaredAt: isFiniteNumber(now) ? now : Date.now(),
      };
      await writeEntriesAtomic(filePath, [...withoutExisting, nextEntry], lockContext);

      return { ceiling: nextEntry.ceiling, sustainedNormalCount: nextEntry.sustainedNormalCount };
    },
    {
      lockStalenessMs: options.lockStalenessMs,
      lockMaxAttempts: options.lockMaxAttempts,
      lockRetryDelayMs: options.lockRetryDelayMs,
    },
  );
}
