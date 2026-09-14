// Phase 1 — durable work queue: enqueue/peek/dequeue.
//
// A structurally SEPARATE concern from ./coordination-file.mjs's admission
// ledger (declareDibs/readDibs and the capacity-claim surface). When the ARM
// traffic-light signal is "hold", instead of the caller busy-waiting/
// re-polling, it enqueues the deferred work item here for later dequeue by a
// fresh process once capacity frees up. This module never references the
// admission ledger's file or its claim/release surface — see
// ./queue.jest.spec.mjs's structural assertions.
//
// Reuses coordination-file.mjs's existing lock/atomic-write primitives
// (`withLock`'s shape, `writeEntriesAtomic`, `assertStillHeld`) rather than
// inventing a second locking mechanism — the queue file gets its own
// `<path>.lock` sibling, independent of the coordination file's lock, but
// governed by the exact same fencing-token discipline described in that
// module's header.

import { randomBytes } from 'node:crypto';
import { promises as fs } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join, resolve } from 'node:path';

import { acquireLock, assertStillHeld, releaseLock, writeEntriesAtomic } from './coordination-file.mjs';

/**
 * Bound on `commandRef` length. No existing precedent elsewhere in this
 * repo for a "command reference string" — this is the chosen bound,
 * exported so the implementer/reviewer/CLI layer reconcile against or reuse
 * this number directly rather than restating it.
 */
export const MAX_COMMAND_REF_LENGTH = 4096;

/**
 * Bound on `agentClass` length — reuses the same bound family as
 * `commandRef`'s validation but tighter, since an agent-class name is a
 * short identifier, not an arbitrary command string. No existing precedent
 * elsewhere in this repo for this concept either, so this is the chosen
 * bound, exported for the same reconcile-don't-restate reason as
 * `MAX_COMMAND_REF_LENGTH`.
 */
export const MAX_AGENT_CLASS_LENGTH = 256;

/**
 * Bound on `orchestratorId` length — reuses the same bound as
 * `agentClass`'s (both are short identifiers, not arbitrary strings). No
 * existing precedent elsewhere in this repo for a queue-entry orchestrator
 * id length bound, so this is the chosen bound, exported for the same
 * reconcile-don't-restate reason as the other two.
 */
export const MAX_ORCHESTRATOR_ID_LENGTH = 256;

const VALID_PRIORITIES = new Set(['low', 'normal', 'high']);

// Ascending rank — lower rank sorts first ("high" wins). Built via
// `Object.create(null)` (a prototype-less object) rather than an object
// literal so `PRIORITY_RANK[a.priority]` can NEVER resolve an
// Object.prototype-inherited member (e.g. `priority: "toString"` or
// `"constructor"` would otherwise resolve to an inherited Function, not
// `undefined`) — closing the whole class of prototype-inherited-key lookup
// bugs for this table, not just at the one call site below. See
// `sortQueueEntries`'s comment for why that distinction matters.
const PRIORITY_RANK = Object.assign(Object.create(null), { high: 0, normal: 1, low: 2 });

// Rank assigned to any `priority` value NOT present in `PRIORITY_RANK` —
// one past the worst known rank ("low"), so an out-of-enum value (e.g. a
// hand-edited on-disk file, or a queue written by a differently-versioned
// CLI) always sorts LAST, never wins a dequeue, and — critically — never
// produces `undefined - n = NaN` in the comparator (see `sortQueueEntries`).
const UNKNOWN_PRIORITY_RANK = Object.keys(PRIORITY_RANK).length;

const DEFAULT_LOCK_STALENESS_MS = 30_000;

/**
 * Aging threshold (10 minutes, `10 * 60 * 1000`) — the chosen default aging
 * policy for `sortQueueEntries`'s optional `now` parameter.
 * An entry whose age (`now - Date.parse(entry.enqueuedAt)`) is `>=` this
 * value counts as "aged" — the boundary is INCLUSIVE (`>=`, not `>`), so an
 * entry that has waited exactly this long is promoted, not merely one that
 * has waited strictly longer; see `sortQueueEntries`'s rank-promotion logic
 * below for where the comparison itself lives. Exported so a future
 * dispatcher caller (Phase 4/5) and this module's own test file reconcile
 * against or reuse this exact number rather than restating it — same
 * "reconcile-don't-restate" reasoning as `MAX_COMMAND_REF_LENGTH` above.
 */
export const AGING_THRESHOLD_MS = 10 * 60 * 1000;

/**
 * Default `options.claimTtlMs` for `claimQueueEntry` when the caller doesn't
 * override it. Mirrors coordination-file.mjs's `DEFAULT_CLAIM_TTL_MS`/
 * `pruneExpiredClaims` lease-TTL idiom (a lease is "live" while
 * `now - leasedAt <= claimTtlMs`, else expired and reclaimable), but scoped
 * far shorter: a queue lease covers "one dispatcher's processing attempt of
 * one work item", not an admission ledger's multi-hour hold, so this is
 * pinned at 15 minutes (`15 * 60 * 1000`) rather than reusing
 * `DEFAULT_CLAIM_TTL_MS`'s 6 hours.
 */
export const DEFAULT_QUEUE_CLAIM_TTL_MS = 15 * 60 * 1000;

/**
 * Local counterpart to coordination-file.mjs's own (unexported)
 * `isFiniteNumber` — not reused directly because that module doesn't export
 * it, and this module already reuses its lock/atomic-write primitives
 * without reaching into its private helpers.
 *
 * @param {unknown} value
 * @returns {boolean}
 */
function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

/**
 * Validates a candidate `pid` BEFORE any write is attempted (see
 * `bindPidToLease`). Throws `QueueInvalidPidError` for anything that is not
 * a positive integer — `NaN`, negative, zero, non-integer, or non-number
 * (including a numeric-looking string) are all rejected, mirroring
 * `validateItem`'s "never silently coerce" posture.
 *
 * @param {unknown} pid
 */
function validatePid(pid) {
  if (!Number.isInteger(pid) || pid <= 0) {
    throw new QueueInvalidPidError(pid);
  }
}

/**
 * Thrown by `enqueueItem` for an out-of-enum `priority` — distinguishable
 * from other failures via `.code`, mirroring coordination-file.mjs's
 * `LockTimeoutError`/`LockLostError` convention exactly.
 */
export class QueueInvalidPriorityError extends Error {
  constructor(priority) {
    super(
      `queue: invalid priority ${JSON.stringify(priority)} — must be one of ` +
        `${[...VALID_PRIORITIES].join(', ')}`,
    );
    this.name = 'QueueInvalidPriorityError';
    this.code = 'QUEUE_INVALID_PRIORITY';
  }
}

/**
 * Thrown by `enqueueItem` for a missing, non-string, empty, or oversized
 * `commandRef` — distinguishable via `.code`, same convention as above.
 */
export class QueueInvalidCommandRefError extends Error {
  constructor(commandRef) {
    super(
      `queue: invalid commandRef — must be a non-empty string of at most ` +
        `${MAX_COMMAND_REF_LENGTH} characters (got ${typeof commandRef})`,
    );
    this.name = 'QueueInvalidCommandRefError';
    this.code = 'QUEUE_INVALID_COMMAND_REF';
  }
}

/**
 * Thrown by `enqueueItem` for a missing, non-string, empty, or oversized
 * `agentClass` — distinguishable via `.code`, same convention as above.
 */
export class QueueInvalidAgentClassError extends Error {
  constructor(agentClass) {
    super(
      `queue: invalid agentClass — must be a non-empty string of at most ` +
        `${MAX_AGENT_CLASS_LENGTH} characters (got ${typeof agentClass})`,
    );
    this.name = 'QueueInvalidAgentClassError';
    this.code = 'QUEUE_INVALID_AGENT_CLASS';
  }
}

/**
 * Thrown by `enqueueItem` for a missing, non-string, empty, or oversized
 * `orchestratorId` — distinguishable via `.code`, same convention as above.
 * Required at the module level (not just at the CLI layer, which already
 * requires `--orchestrator-id` for `--enqueue`) so a future direct caller of
 * `enqueueItem` can't accidentally persist an entry with no owning
 * orchestrator — the whole reason this field exists (see its Scope) is
 * so a future dispatcher can attribute/filter queued work by owner.
 */
export class QueueInvalidOrchestratorIdError extends Error {
  constructor(orchestratorId) {
    super(
      `queue: invalid orchestratorId — must be a non-empty string of at most ` +
        `${MAX_ORCHESTRATOR_ID_LENGTH} characters (got ${typeof orchestratorId})`,
    );
    this.name = 'QueueInvalidOrchestratorIdError';
    this.code = 'QUEUE_INVALID_ORCHESTRATOR_ID';
  }
}

/**
 * Thrown by `bindPidToLease` for a `pid` that is not a positive integer
 * (non-number, `NaN`, non-integer, zero, or negative) — distinguishable via
 * `.code`, same convention as the other `QueueInvalid*Error` classes above.
 * Thrown BEFORE any lock is acquired or file touched, mirroring
 * `enqueueItem`'s own "validate first, write never happens on a rejected
 * call" ordering.
 */
export class QueueInvalidPidError extends Error {
  constructor(pid) {
    super(`queue: invalid pid ${JSON.stringify(pid)} — must be a positive integer`);
    this.name = 'QueueInvalidPidError';
    this.code = 'QUEUE_INVALID_PID';
  }
}

/**
 * Thrown by `readQueueEntries` (surfaced through `peekQueue`/`dequeueItem`/
 * `enqueueItem`, since all three read the file before acting) when an
 * EXISTING queue file's content is not valid JSON, parses to something
 * other than an array, or parses to an array containing an element that is
 * not a well-formed queue entry (e.g. `null`, a primitive, or an object
 * missing/mistyping one of the required fields — see
 * `isWellFormedQueueEntry`). Distinguishable via `.code`, same convention as
 * above. Deliberately NOT thrown for a missing file (ENOENT) — that is
 * "nothing here yet", not corruption — see `readQueueEntries`'s own doc
 * comment. Surfacing this as a typed throw (rather than silently returning
 * `[]`, the prior behavior, or letting a malformed element reach
 * `sortQueueEntries` and throw a raw `TypeError`) matters specifically
 * because `enqueueItem` would otherwise atomically overwrite a corrupted
 * file with just the new entry, discarding every entry that came before it
 * — an unrecoverable data loss this error exists to prevent by refusing to
 * proceed at all. Also prevents the "wedged queue" failure mode where
 * `--enqueue` keeps silently appending on top of an element-corrupted file
 * that `--peek`/`--dequeue` can no longer read at all.
 *
 * @param {string} filePath
 * @param {unknown} [cause]
 * @param {string} [detail] Overrides the default "not valid JSON, or not a
 *   JSON array" clause — used by the element-level corruption check to name
 *   the specific malformed element instead.
 */
export class QueueCorruptFileError extends Error {
  constructor(filePath, cause, detail = 'not valid JSON, or not a JSON array') {
    super(
      `queue: refusing to proceed — the queue file at ${filePath} is corrupt ` +
        `(${detail}); leaving it untouched rather than ` +
        'silently treating it as empty and overwriting it',
    );
    this.name = 'QueueCorruptFileError';
    this.code = 'QUEUE_CORRUPT_FILE';
    if (cause !== undefined) this.cause = cause;
  }
}

/**
 * Mirrors cli.mjs's (unexported) `resolveCoordinationFilePath()` precedent
 * exactly, but for `ARM_QUEUE_FILE` — deliberately independent of
 * `ARM_COORDINATION_FILE`: reading/setting one must never affect the
 * other's resolved path. Exported (unlike `resolveCoordinationFilePath`,
 * which stays private to cli.mjs) because Phase 2's CLI wiring needs to
 * resolve the SAME path this module's own tests exercise directly — one
 * resolver, reused, not re-implemented at the CLI layer.
 *
 * @returns {string}
 */
export function resolveQueueFilePath() {
  if (process.env.ARM_QUEUE_FILE) {
    return resolve(process.env.ARM_QUEUE_FILE);
  }
  return join(homedir(), '.claude/agent-state/resource-queue.json');
}

/**
 * Validates a candidate queue item BEFORE any write is attempted. Throws
 * `QueueInvalidPriorityError`/`QueueInvalidCommandRefError` — never writes
 * as a side effect of a rejected call.
 *
 * @param {{ priority: unknown, commandRef: unknown }} item
 */
function validateItem(item) {
  if (!VALID_PRIORITIES.has(item?.priority)) {
    throw new QueueInvalidPriorityError(item?.priority);
  }
  if (
    typeof item?.commandRef !== 'string' ||
    item.commandRef.length === 0 ||
    item.commandRef.length > MAX_COMMAND_REF_LENGTH
  ) {
    throw new QueueInvalidCommandRefError(item?.commandRef);
  }
  if (
    typeof item?.agentClass !== 'string' ||
    item.agentClass.length === 0 ||
    item.agentClass.length > MAX_AGENT_CLASS_LENGTH
  ) {
    throw new QueueInvalidAgentClassError(item?.agentClass);
  }
  if (
    typeof item?.orchestratorId !== 'string' ||
    item.orchestratorId.length === 0 ||
    item.orchestratorId.length > MAX_ORCHESTRATOR_ID_LENGTH
  ) {
    throw new QueueInvalidOrchestratorIdError(item?.orchestratorId);
  }
}

/**
 * Structural shape-check for a single queue-file array element — deliberately
 * NOT exhaustive/strict (no enum/format validation, that's `validateItem`'s
 * job on the WRITE path), just enough to catch `null`/non-object/wrong-typed
 * elements before they reach `sortQueueEntries`'s `.priority`/`.enqueuedAt`
 * property access, which would otherwise throw a raw, non-actionable
 * `TypeError` on a `null` element (or worse, silently misorder on some other
 * malformed-but-object-shaped element).
 *
 * @param {unknown} candidate
 * @returns {boolean}
 */
function isWellFormedQueueEntry(candidate) {
  return (
    candidate !== null &&
    typeof candidate === 'object' &&
    !Array.isArray(candidate) &&
    typeof candidate.agentClass === 'string' &&
    typeof candidate.priority === 'string' &&
    typeof candidate.commandRef === 'string' &&
    typeof candidate.orchestratorId === 'string' &&
    typeof candidate.enqueuedAt === 'string'
  );
}

/**
 * Reads the queue file's entries, tolerating a not-yet-created file
 * (returns `[]` rather than throwing — ENOENT means "nothing here yet", not
 * corruption) but THROWING `QueueCorruptFileError` for an EXISTING file
 * whose content is not valid JSON, or that parses to something other than
 * an array. A silent `[]` fallback here would be indistinguishable from
 * real data loss to every caller: `enqueueItem` calls this then atomically
 * overwrites the file with `[...existing, entry]`, so treating "can't
 * parse this" as "empty" would silently discard every prior entry on the
 * very next enqueue. Surfacing a typed error instead lets the caller (and
 * ultimately a human/operator) recover the file rather than lose it.
 *
 * @param {string} filePath
 * @returns {Promise<Array<object>>}
 */
async function readQueueEntries(filePath) {
  let raw;
  try {
    raw = await fs.readFile(filePath, 'utf8');
  } catch (error) {
    if (error.code === 'ENOENT') return [];
    throw error;
  }

  if (!raw) return [];

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (parseError) {
    throw new QueueCorruptFileError(filePath, parseError);
  }
  if (!Array.isArray(parsed)) {
    throw new QueueCorruptFileError(filePath);
  }

  for (const [index, candidate] of parsed.entries()) {
    if (!isWellFormedQueueEntry(candidate)) {
      throw new QueueCorruptFileError(
        filePath,
        undefined,
        `array element at index ${index} is not a well-formed queue entry`,
      );
    }
  }

  return parsed;
}

/**
 * Computes the age (in ms) of `entry` relative to `referenceNow`, from its
 * `enqueuedAt` ISO-8601 string. Returns `0` — never `NaN`, never throws —
 * for a missing/non-string/unparsable `enqueuedAt` (`Date.parse` returns
 * `NaN` for a malformed string; treating that as "age zero" rather than
 * propagating the `NaN` mirrors this module's existing defensive philosophy
 * for malformed `priority` values in `sortQueueEntries` below: never throw,
 * never let a bad on-disk field poison the comparator). Age zero also means
 * "never aged" for rank-promotion purposes, since `0 < AGING_THRESHOLD_MS`
 * always holds — exactly the desired fallback (an entry with a garbled
 * timestamp is left at its literal `priority`, neither promoted nor
 * penalised).
 *
 * @param {object} entry
 * @param {number} referenceNow
 * @returns {number}
 */
function ageMs(entry, referenceNow) {
  const parsed = typeof entry.enqueuedAt === 'string' ? Date.parse(entry.enqueuedAt) : NaN;
  if (!Number.isFinite(parsed)) return 0;
  return referenceNow - parsed;
}

/**
 * Effective sort-time rank for `entry` given `referenceNow` — either its
 * plain `PRIORITY_RANK`-derived rank (aging not in effect / entry not aged),
 * or that rank bumped exactly ONE tier toward "high" when the entry is aged
 * past `AGING_THRESHOLD_MS`. This is a SORT-TIME-ONLY adjustment: it is
 * computed fresh on every call and never written back to `entry.priority` —
 * see `sortQueueEntries`'s own doc comment for why the on-disk field must
 * stay untouched.
 *
 * Promotion is `rank - 1`, floored at `0` (`PRIORITY_RANK.high`) — never
 * negative, never wrapping past "high" — so `low` (rank 2) becomes rank 1
 * (competes at "normal"), `normal` (rank 1) becomes rank 0 (competes at
 * "high"), and `high` (rank 0) stays rank 0 (already top; no
 * double-promotion, no error). `UNKNOWN_PRIORITY_RANK` (an out-of-enum
 * `priority`) is promoted by the same `- 1, floored at 0` rule as any other
 * rank — aging never makes an already-invalid priority MORE invalid, it just
 * follows the same one-tier-toward-high rule uniformly.
 *
 * @param {object} entry
 * @param {number|undefined} referenceNow `undefined` means "aging is not in
 *   effect for this call" (see `sortQueueEntries`) — the plain rank is
 *   returned unconditionally in that case, with no `ageMs` call at all.
 * @returns {number}
 */
function effectiveRank(entry, referenceNow) {
  const rank = PRIORITY_RANK[entry.priority] ?? UNKNOWN_PRIORITY_RANK;
  if (referenceNow === undefined) return rank;
  const aged = ageMs(entry, referenceNow) >= AGING_THRESHOLD_MS;
  return aged ? Math.max(0, rank - 1) : rank;
}

/**
 * Orders `entries` by `priority` DESCENDING (high > normal > low) then
 * `enqueuedAt` ASCENDING (oldest first); any remaining tie (identical
 * priority AND identical enqueuedAt) breaks by insertion order — i.e. a
 * stable sort over the on-disk array order, never re-ordered by any other
 * criterion. `Array.prototype.sort` is stable in Node, so this needs no
 * explicit tiebreaker term. Returns a NEW array — never mutates `entries`.
 *
 * Optional second parameter `now` enables an AGING policy:
 * an entry waiting `>= AGING_THRESHOLD_MS` (inclusive boundary — see that
 * constant's own doc comment) is promoted exactly one priority tier toward
 * "high" for THIS sort only (see `effectiveRank`) — the on-disk `priority`
 * field is never mutated, so this promotion is recomputed fresh from
 * `enqueuedAt` + `now` on every call, never persisted as separate state.
 *
 * Aging is gated STRICTLY on "was `now` explicitly supplied as a finite
 * number" at THIS function's own level — `sortQueueEntries` itself still
 * takes no default; a bare `sortQueueEntries(entries)` zero-arg call is
 * unaffected and produces the pre-aging order, byte-for-byte. Aging becomes
 * the default behavior one layer up, at each of `peekQueue`, `dequeueItem`,
 * and `claimQueueEntry` (Phase 5/post-review fix) — each of those three
 * resolves its own `referenceNow` (a finite `now`/`options.now` override when
 * supplied, else `Date.now()`) and threads it through as this second
 * argument, so aging applies uniformly to entry SELECTION on every real
 * peek/dequeue/claim path, not merely to `claimQueueEntry`'s
 * lease-eligibility filtering — see each of those three functions' own
 * body/doc comments. Keeping `sortQueueEntries` itself strict (no implicit
 * `Date.now()` fallback) means it stays a pure, fully-deterministic
 * comparator function for its own tests — the wall-clock default is a
 * caller-level decision, not baked into the primitive.
 *
 * @param {Array<object>} entries
 * @param {number} [now] Reference "now" for aging, in epoch ms. Aging is
 *   considered ONLY when this is a finite number; omitted/non-finite means
 *   "no aging" — byte-for-byte the pre-aging sort order.
 * @returns {Array<object>}
 */
export function sortQueueEntries(entries, now) {
  const referenceNow = isFiniteNumber(now) ? now : undefined;
  return [...entries].sort((a, b) => {
    // `?? UNKNOWN_PRIORITY_RANK` (inside `effectiveRank`) guards against an
    // out-of-enum `priority` (e.g. a hand-edited on-disk file, or a queue
    // written by an older/newer CLI version) the exact same way the
    // `enqueuedAt` string comparison below is guarded: `PRIORITY_RANK[a.priority]`
    // is `undefined` for an unrecognised value, and `undefined - n` is `NaN`
    // — a comparator that ever returns `NaN` is comparator-implementation-
    // dependent (silently non-deterministic-in-practice) per
    // `Array.prototype.sort`. This guard alone is NOT sufficient for a plain-
    // object lookup table, though: `??` only fires on `null`/`undefined`, so
    // a priority value naming an Object.prototype-inherited member (e.g.
    // `"toString"`, `"constructor"`, `"valueOf"`) would resolve to an
    // inherited Function, not `undefined`, and `??` would never fire —
    // reintroducing the exact NaN comparator bug this guard exists to
    // prevent. `PRIORITY_RANK` is therefore built with `Object.create(null)`
    // (see its declaration above), which has no prototype chain at all, so
    // every out-of-enum `priority` — including those inherited-member names
    // — genuinely misses and falls back here. Falling back to
    // `UNKNOWN_PRIORITY_RANK` (one past the worst known rank) means an
    // unrecognised priority always sorts last — it can never win a dequeue
    // over a valid entry — instead of producing NaN.
    const rankA = effectiveRank(a, referenceNow);
    const rankB = effectiveRank(b, referenceNow);
    const rankDiff = rankA - rankB;
    if (rankDiff !== 0) return rankDiff;
    // `enqueuedAt` is always a server-stamped ISO-8601 string, which sorts
    // correctly lexicographically for the same reason it sorted correctly
    // numerically as an epoch-ms value. Compared as strings via `<`/`>`
    // (never coerced to a Date/number) so a malformed or missing value —
    // e.g. inherited from an entry that predates this format, or from a
    // not-yet-detected corrupt source — can never throw or produce NaN; it
    // just sorts wherever its string value happens to fall, rather than
    // blowing up the whole comparator.
    if (a.enqueuedAt < b.enqueuedAt) return -1;
    if (a.enqueuedAt > b.enqueuedAt) return 1;
    return 0;
  });
}

async function ensureQueueDir(filePath) {
  try {
    await fs.mkdir(dirname(filePath), { recursive: true });
  } catch {
    // Deliberately swallowed, mirroring cli.mjs's ensureCoordinationDir: an
    // unwritable parent dir surfaces naturally when the subsequent lock
    // acquisition/write fails on its own.
  }
}

/**
 * Serialises `fn` against every other `withQueueLock` call for the same
 * `filePath`. Local, queue-scoped counterpart to coordination-file.mjs's
 * (unexported) `withLock` — same fencing-token shape (acquireLock ->
 * lockContext -> releaseLock in a finally), reusing that module's exported
 * `acquireLock`/`assertStillHeld`/`releaseLock` primitives rather than a
 * second locking mechanism. Also mirrors `withLock`'s options threading:
 * `lockOptions.lockStalenessMs`/`lockMaxAttempts`/`lockRetryDelayMs` are
 * passed straight through to `acquireLock`, exactly as `declareDibs` ->
 * `withLock` -> `acquireLock` does in coordination-file.mjs. A caller that
 * passes nothing gets `acquireLock`'s own defaults (this module's
 * `DEFAULT_LOCK_STALENESS_MS` for staleness, `acquireLock`'s built-in
 * defaults for the other two).
 *
 * @template T
 * @param {string} filePath
 * @param {(lockContext: { lockPath: string, token: string, assertStillHeld: () => Promise<void> }) => Promise<T>} fn
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [lockOptions]
 * @returns {Promise<T>}
 */
async function withQueueLock(filePath, fn, lockOptions = {}) {
  const lockPath = `${filePath}.lock`;
  const lockStalenessMs = Number.isFinite(lockOptions.lockStalenessMs)
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
 * Persists `item` to the durable queue at `filePath`, atomically, under the
 * same lock/atomic-write machinery `./coordination-file.mjs` exports.
 * `enqueuedAt` is ALWAYS server-assigned as an ISO-8601 string
 * (`new Date().toISOString()`) at the moment of persistence — any
 * caller-supplied `item.enqueuedAt` is ignored/overwritten, never honored,
 * so two callers can never spoof queue ordering by backdating themselves.
 * Returns the persisted entry.
 *
 * Rejects BEFORE writing (no file created/modified as a side effect) for an
 * out-of-enum `priority`, an oversized/malformed `commandRef`, an
 * invalid `agentClass`, or an invalid `orchestratorId` — validation runs
 * before the directory is even created or the lock acquired. Also propagates `QueueCorruptFileError` (via
 * `readQueueEntries`) if the EXISTING file on disk is corrupt — refusing to
 * silently overwrite it with just this one new entry.
 *
 * @param {string} filePath
 * @param {{ agentClass: string, priority: 'low'|'normal'|'high', commandRef: string, orchestratorId: string }} item
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<object>}
 */
export async function enqueueItem(filePath, item, options = {}) {
  validateItem(item);

  const entry = {
    ...item,
    enqueuedAt: new Date().toISOString(),
  };

  await ensureQueueDir(filePath);

  await withQueueLock(
    filePath,
    async (lockContext) => {
      const existing = await readQueueEntries(filePath);
      await writeEntriesAtomic(filePath, [...existing, entry], lockContext);
    },
    options,
  );

  return entry;
}

/**
 * Read-only. Returns the full live queue in `sortQueueEntries` order,
 * WITH aging applied (see `AGING_THRESHOLD_MS`/`effectiveRank`) — the same
 * ordering `claimQueueEntry`/`dequeueItem` actually select from, so a
 * caller previewing via `--peek` sees exactly what a real claim/dequeue
 * would pick, including any entry promoted for having aged past the
 * threshold. Never mutates the file. A missing queue file returns `[]`,
 * never throws. A CORRUPTED existing queue file throws
 * `QueueCorruptFileError` (see `readQueueEntries`) rather than silently
 * returning `[]`.
 *
 * Still takes no lock-related `options` bag: unlike `enqueueItem`/
 * `dequeueItem`, this function never acquires the queue lock (a plain read
 * needs no fencing token), so there is no `lockStalenessMs`/etc. for an
 * options bag to meaningfully thread through — an earlier revision declared
 * one anyway; dropped rather than kept as a parameter that could never do
 * anything. The optional `now` parameter below exists for a DIFFERENT
 * reason — aging consistency with `claimQueueEntry` — not for lock params.
 *
 * @param {string} filePath
 * @param {number} [now] Reference "now" for aging, in epoch ms. Defaults to
 *   `Date.now()` when omitted/non-finite, so every existing zero-arg caller
 *   (e.g. cli.mjs's `handlePeek`) gets real-clock aging automatically —
 *   aging is ALWAYS active for `peekQueue`, matching `claimQueueEntry`'s
 *   behavior, not opt-in.
 * @returns {Promise<Array<object>>}
 */
export async function peekQueue(filePath, now) {
  const referenceNow = isFiniteNumber(now) ? now : Date.now();
  const entries = await readQueueEntries(filePath);
  return sortQueueEntries(entries, referenceNow);
}

/**
 * Atomically removes AND returns the single top ELIGIBLE entry, per the
 * exact same ordering `peekQueue`/`claimQueueEntry` use — WITH aging applied
 * (see `AGING_THRESHOLD_MS`/`effectiveRank`), so a plain `--dequeue` selects
 * the identical entry `--dequeue-if-capacity`/`claimQueueEntry` would — and
 * re-reading the file under its OWN lock acquisition rather than trusting
 * any prior `peekQueue` call (TOCTOU safety). Two concurrent `dequeueItem`
 * calls racing the same queue never both receive the same entry.
 *
 * **Lease-aware, exactly like `claimQueueEntry` (fixed as part of its
 * pre-PR review — see queue-lease.jest.spec.mjs's "dequeueItem is lease-aware"
 * coverage):** an entry currently under an unexpired lease (see
 * `isEligibleForClaim`) is skipped entirely, not just deprioritised — this
 * function selects the top entry among ELIGIBLE (unleased-or-lease-expired)
 * entries only, never merely the top entry overall. Without this, a plain
 * `--dequeue` consumer and a `--dequeue-if-capacity` consumer sharing one
 * queue file could both receive the SAME item: `claimQueueEntry` stamps a
 * lease and leaves the entry on disk (pending the caller's own external
 * spawn + `--ack-lease`), so an unfiltered `dequeueItem` racing that same
 * file could still see — and unconditionally remove — an entry another
 * consumer has already leased and is actively acting on. Returns `null` —
 * never throws — when the queue is empty, missing, or every entry is
 * currently under an unexpired lease (nothing eligible), mirroring
 * `claimQueueEntry`'s own "nothing eligible" contract. A CORRUPTED existing
 * queue file throws `QueueCorruptFileError` (see `readQueueEntries`) rather
 * than silently treating it as empty.
 *
 * `options.now`/`options.claimTtlMs` resolve exactly as `claimQueueEntry`'s
 * own — a finite `now` override when supplied, else `Date.now()`; a finite
 * `claimTtlMs` override when supplied, else `DEFAULT_QUEUE_CLAIM_TTL_MS` —
 * so aging AND lease-eligibility are ALWAYS active for a real caller by
 * default, not opt-in.
 *
 * @param {string} filePath
 * @param {{ now?: number, claimTtlMs?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<object|null>}
 */
export async function dequeueItem(filePath, options = {}) {
  await ensureQueueDir(filePath);

  return withQueueLock(
    filePath,
    async (lockContext) => {
      const existing = await readQueueEntries(filePath);
      if (existing.length === 0) return null;

      const referenceNow = isFiniteNumber(options.now) ? options.now : Date.now();
      const claimTtlMs = isFiniteNumber(options.claimTtlMs) ? options.claimTtlMs : DEFAULT_QUEUE_CLAIM_TTL_MS;

      const top = sortQueueEntries(existing, referenceNow).find((entry) =>
        isEligibleForClaim(entry, referenceNow, claimTtlMs),
      );
      if (!top) return null;

      const remaining = existing.filter((entry) => entry !== top);

      await writeEntriesAtomic(filePath, remaining, lockContext);
      return top;
    },
    options,
  );
}

/**
 * Whether `entry` is currently eligible to be claimed — either it carries no
 * lease at all (never claimed, or a legacy entry from before this surface
 * existed), or it carries a lease whose `leasedAt` is old enough that
 * `referenceNow - leasedAt` exceeds `claimTtlMs` (an EXPIRED lease, mirroring
 * coordination-file.mjs's `pruneExpiredClaims` "live if
 * `now - leasedAt <= claimTtlMs`" convention exactly — this is that
 * condition's negation). An entry whose `leasedAt` is present but not a
 * finite number (a malformed/hand-edited lease) is treated as expired rather
 * than permanently un-claimable, for the same "never wedge the queue on bad
 * on-disk data" reasoning `sortQueueEntries` documents for `enqueuedAt`.
 *
 * @param {object} entry
 * @param {number} referenceNow
 * @param {number} claimTtlMs
 * @returns {boolean}
 */
function isEligibleForClaim(entry, referenceNow, claimTtlMs) {
  if (entry.leaseId == null) return true;
  if (!isFiniteNumber(entry.leasedAt)) return true;
  return referenceNow - entry.leasedAt > claimTtlMs;
}

/**
 * Atomically claims the TOP currently-ELIGIBLE entry — same
 * `sortQueueEntries` ordering `peekQueue`/`dequeueItem` use (priority desc,
 * then enqueuedAt asc, WITH aging applied — see below), skipping any entry
 * currently under an unexpired lease (see `isEligibleForClaim`) — stamps it
 * with a freshly-generated `leaseId` (a random 16-byte hex string, mirroring
 * `acquireLock`'s own fencing-token generation via
 * `randomBytes(16).toString('hex')`) and a `leasedAt` timestamp, persists it,
 * and returns the now-leased entry. The entry's `enqueuedAt` is never
 * touched — reclaiming an expired lease keeps the original enqueue time (and
 * therefore FIFO/aging place) intact, exactly as `releaseQueueEntry` also
 * preserves it.
 *
 * `leasedAt`/the expiry comparison's "now" are BOTH sourced from
 * `options.now` when it is a finite number, falling back to `Date.now()`
 * otherwise — mirrors `reserveAdmission`'s own `now` injection convention,
 * threaded via `options` here since there is no other params bag. Similarly,
 * `options.claimTtlMs` overrides `DEFAULT_QUEUE_CLAIM_TTL_MS` when it is a
 * finite number. That SAME resolved `referenceNow` is also threaded through
 * as `sortQueueEntries`'s second argument, so aging (see
 * `AGING_THRESHOLD_MS`/`effectiveRank`) applies to which entry is SELECTED
 * for claim, not merely to the `isEligibleForClaim` lease-expiry check — an
 * item that has waited long enough is promoted ahead of newer, lower-priority
 * items on this real claim/dequeue-if-capacity path, matching `sortQueueEntries`'s
 * own aging contract exactly (purely additive; never `NaN`, never throws).
 *
 * Returns `null` — never throws — when the queue is empty, missing, or every
 * entry is currently under an unexpired lease (nothing eligible). Propagates
 * `QueueCorruptFileError` for a corrupt on-disk file, identically to
 * `dequeueItem`. Runs under the SAME `withQueueLock` critical section
 * `enqueueItem`/`dequeueItem` use, re-reading the file under its own lock
 * acquisition (TOCTOU safety) — two concurrent `claimQueueEntry` calls
 * racing the same queue never both receive the same entry.
 *
 * @param {string} filePath
 * @param {{ now?: number, claimTtlMs?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<object|null>}
 */
export async function claimQueueEntry(filePath, options = {}) {
  await ensureQueueDir(filePath);

  return withQueueLock(
    filePath,
    async (lockContext) => {
      const existing = await readQueueEntries(filePath);
      if (existing.length === 0) return null;

      const referenceNow = isFiniteNumber(options.now) ? options.now : Date.now();
      const claimTtlMs = isFiniteNumber(options.claimTtlMs) ? options.claimTtlMs : DEFAULT_QUEUE_CLAIM_TTL_MS;

      const eligible = sortQueueEntries(existing, referenceNow).find((entry) =>
        isEligibleForClaim(entry, referenceNow, claimTtlMs),
      );
      if (!eligible) return null;

      // A reclaim (fresh leaseId minted for an entry whose PRIOR lease
      // expired) must never inherit a `pid` bound to that prior lease via
      // `bindPidToLease` — the new leaseId identifies a
      // brand new processing attempt with no pid of its own yet. Stripped
      // via destructuring (safe whether or not `pid` was ever present),
      // mirroring `releaseQueueEntry`'s own field-stripping idiom below.
      // eslint-disable-next-line no-unused-vars -- destructuring strips
      // `pid` from `eligibleWithoutPid`; the binding itself is unused.
      const { pid: _pid, ...eligibleWithoutPid } = eligible;
      const claimed = {
        ...eligibleWithoutPid,
        leaseId: randomBytes(16).toString('hex'),
        leasedAt: referenceNow,
      };
      const nextEntries = existing.map((entry) => (entry === eligible ? claimed : entry));

      await writeEntriesAtomic(filePath, nextEntries, lockContext);
      return claimed;
    },
    options,
  );
}

/**
 * Atomically and PERMANENTLY removes the single entry whose current on-disk
 * `leaseId` exactly matches `leaseId` — the "the leased work item finished
 * successfully" path — and returns the removed entry.
 *
 * If no on-disk entry currently carries this exact `leaseId` — including a
 * STALE lease that has since been superseded by a fresh `claimQueueEntry`
 * call after TTL expiry (which mints a brand new `leaseId`) — this is a SAFE
 * NO-OP: returns `null`, the queue is left completely unmodified, nothing
 * throws. This mirrors `dequeueItem`'s own "nothing matched, return null
 * rather than throw" convention for the equivalent "no work to do" case,
 * rather than introducing a new throw-on-not-found convention this module
 * has no other precedent for. Deliberate contract choice: the alternative —
 * throwing a typed "lease not found" error — was considered and rejected
 * specifically because a late/duplicate ack racing a legitimate reclaim is
 * an expected, non-exceptional occurrence in a crash-recycling lease system,
 * not a caller bug.
 *
 * Propagates `QueueCorruptFileError` for a corrupt on-disk file, identically
 * to `dequeueItem`/`claimQueueEntry`.
 *
 * @param {string} filePath
 * @param {string} leaseId
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<object|null>}
 */
export async function ackQueueEntry(filePath, leaseId, options = {}) {
  await ensureQueueDir(filePath);

  return withQueueLock(
    filePath,
    async (lockContext) => {
      const existing = await readQueueEntries(filePath);
      const target = existing.find((entry) => entry.leaseId != null && entry.leaseId === leaseId);
      if (!target) return null;

      const remaining = existing.filter((entry) => entry !== target);
      await writeEntriesAtomic(filePath, remaining, lockContext);
      return target;
    },
    options,
  );
}

/**
 * Atomically clears the lease (both `leaseId` and `leasedAt`) on the single
 * entry whose current on-disk `leaseId` exactly matches `leaseId`, returning
 * the entry to normal-claimable state — the "crashed/gave up mid-processing,
 * recycle it" path. Returns the UPDATED entry (lease fields cleared).
 *
 * Critically, the entry's ORIGINAL `enqueuedAt` — stamped once, at the
 * original `enqueueItem` call, long before this lease ever existed — is
 * preserved byte-for-byte and NEVER reset to "now": a crash-recycled item
 * must not lose its FIFO/aging place in the queue by being treated as
 * freshly enqueued.
 *
 * If no on-disk entry currently carries this exact `leaseId` (same
 * stale-leaseId scenario `ackQueueEntry` documents), this is the same safe
 * no-op: returns `null`, queue left unmodified, never throws. Propagates
 * `QueueCorruptFileError` for a corrupt on-disk file, identically to the
 * other two.
 *
 * Also clears a bound `pid` alongside `leaseId`/`leasedAt` —
 * a pid was bound via `bindPidToLease` to THIS specific lease attempt, so it
 * must not survive the entry being returned to normal-claimable state; a
 * future re-claim mints a fresh `leaseId` and starts with no pid of its own
 * (see `claimQueueEntry`'s own pid-stripping-on-reclaim behavior).
 *
 * @param {string} filePath
 * @param {string} leaseId
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<object|null>}
 */
export async function releaseQueueEntry(filePath, leaseId, options = {}) {
  await ensureQueueDir(filePath);

  return withQueueLock(
    filePath,
    async (lockContext) => {
      const existing = await readQueueEntries(filePath);
      const target = existing.find((entry) => entry.leaseId != null && entry.leaseId === leaseId);
      if (!target) return null;

      // eslint-disable-next-line no-unused-vars -- destructuring strips
      // pid/leaseId/leasedAt from `released`; the bindings themselves are unused.
      const { pid: _pid, leaseId: _leaseId, leasedAt: _leasedAt, ...released } = target;
      const nextEntries = existing.map((entry) => (entry === target ? released : entry));

      await writeEntriesAtomic(filePath, nextEntries, lockContext);
      return released;
    },
    options,
  );
}

/**
 * Atomically stamps `pid` onto the single on-disk entry whose CURRENT
 * `leaseId` AND `orchestratorId` BOTH exactly match the given values — never
 * matching on `leaseId` alone, closing the ownership gap where one
 * orchestrator could otherwise bind an arbitrary pid onto another
 * orchestrator's lease. Returns the UPDATED entry.
 *
 * If no on-disk entry matches BOTH `leaseId` AND `orchestratorId` —
 * including a stale/superseded `leaseId` (already reclaimed by a fresh
 * `claimQueueEntry` call after TTL expiry, per that function's contract), or
 * a `leaseId` that belongs to a DIFFERENT orchestrator's entry — this is a
 * SAFE NO-OP: returns `null`, the queue is left completely unmodified,
 * nothing throws. Mirrors `ackQueueEntry`/`releaseQueueEntry`'s existing
 * "no match, return null rather than throw" convention.
 *
 * Rejects BEFORE writing (no file created/modified as a side effect, no lock
 * acquired) for a non-positive-integer `pid` — see `validatePid`.
 *
 * Idempotent: calling twice with the same `leaseId`/`orchestratorId`/`pid` is
 * a no-op re-write (no error, same end state); calling again with a
 * DIFFERENT `pid` for the same `leaseId`/`orchestratorId` overwrites it
 * (last write wins).
 *
 * Runs under the same `withQueueLock` critical section every other mutating
 * function in this module uses — concurrent `bindPidToLease`/
 * `releaseQueueEntry` calls racing the same file never corrupt the queue.
 * Propagates `QueueCorruptFileError` for a corrupt on-disk file, identically
 * to `ackQueueEntry`/`releaseQueueEntry`.
 *
 * @param {string} filePath
 * @param {string} leaseId
 * @param {number} pid
 * @param {string} orchestratorId
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<object|null>}
 */
export async function bindPidToLease(filePath, leaseId, pid, orchestratorId, options = {}) {
  validatePid(pid);

  await ensureQueueDir(filePath);

  return withQueueLock(
    filePath,
    async (lockContext) => {
      const existing = await readQueueEntries(filePath);
      const target = existing.find(
        (entry) => entry.leaseId != null && entry.leaseId === leaseId && entry.orchestratorId === orchestratorId,
      );
      if (!target) return null;

      const bound = { ...target, pid };
      const nextEntries = existing.map((entry) => (entry === target ? bound : entry));

      await writeEntriesAtomic(filePath, nextEntries, lockContext);
      return bound;
    },
    options,
  );
}
