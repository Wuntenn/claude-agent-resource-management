// Phase 2 — persisted queue-aging escalation (priority bump + alert).
//
// A SEPARATE, explicitly-named concern from ./queue.mjs's existing
// ORDERING-ONLY aging (`sortQueueEntries`'s optional `now` parameter /
// `AGING_THRESHOLD_MS`, Phase 3): that mechanism NEVER mutates
// `entry.priority` on disk — it recomputes a sort-time-only rank boost fresh
// on every call. This module is the opposite: it PERSISTS a real mutation to
// `entry.priority` (a one-time, idempotent rank bump) once an entry has
// waited past a configured bound *while capacity was free*, and separately
// tracks (and persists) whether an "still stuck after the bump" alert has
// already fired for that entry, so a caller polling this module every beat
// never re-bumps or re-alerts the same entry twice.
//
// Reuses coordination-file.mjs's existing lock/atomic-write primitives
// (`acquireLock`/`assertStillHeld`/`releaseLock`/`writeEntriesAtomic`)
// exactly as ./queue.mjs's own `withQueueLock` does. Critically, the lock
// path this module derives (`${filePath}.lock` in `withEscalationLock`) is
// the SAME path ./queue.mjs's `withQueueLock` derives for the same
// `filePath` — not a separate/independent lock. When this module is called
// against the live resource-queue file, that path IDENTITY (not
// independence) is what actually buys cross-module mutual exclusion against
// dispatch.mjs's `claimQueueEntry`/`releaseQueueEntry` traffic today. This
// module never imports from ./queue.mjs's own (unexported) lock helper — it
// mirrors the shape (and, when given the same filePath, the exact lock
// path) rather than reaching into another module's private surface. Do not
// "fix" this by giving this module a distinct `.escalation.lock` path — that
// would remove the only thing preventing this module from racing
// destructively against a live dispatcher on the shared queue file.
//
// Single-consistent-read discipline: `referenceNow` is snapshotted ONCE per
// `escalateAgedQueueEntry` call (either a finite `options.now`, or a single
// `Date.now()` read) and threaded through every entry considered in that
// beat — never re-read per entry. This guarantees this module's OWN age
// computation is self-consistent across every entry considered in the same
// beat; it does NOT make the overall "waited >= boundMs AND capacity was
// free throughout" check atomic against a concurrently-changing world (a
// real capacity-free/full transition could still occur between an earlier
// caller-side capacity sample and this call's file read).

import { dirname } from 'node:path';
import { promises as fs } from 'node:fs';

import { acquireLock, assertStillHeld, releaseLock, writeEntriesAtomic } from './coordination-file.mjs';

/**
 * The chosen default for `options.boundMs` when a caller doesn't override
 * it — 20 minutes. Deliberately a DIFFERENT number from queue.mjs's
 * `AGING_THRESHOLD_MS` (10 minutes) — the two mechanisms are not the same
 * policy and must not be conflated by sharing a constant.
 */
export const DEFAULT_ESCALATION_BOUND_MS = 20 * 60 * 1000;

const DEFAULT_LOCK_STALENESS_MS = 30_000;

const VALID_PRIORITIES = new Set(['low', 'normal', 'high']);

// Ascending-toward-high bump table — mirrors queue.mjs's own `PRIORITY_RANK`
// promotion direction (low -> normal -> high), but as a direct persisted
// mutation rather than a sort-time-only rank. `high` maps to itself: bumping
// an already-top-priority entry is a genuine no-op for the field, but the
// entry is still marked `escalatedAt`/evaluated for the alert by the caller
// of this table — see `escalateAgedQueueEntry`.
const PRIORITY_BUMP = Object.assign(Object.create(null), { low: 'normal', normal: 'high', high: 'high' });

/**
 * @param {unknown} value
 * @returns {value is number}
 */
function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

/**
 * Structural shape-check for a single queue-file array element, from this
 * module's own escalation-eligibility point of view — deliberately mirrors
 * queue.mjs's `isWellFormedQueueEntry` field set PLUS an enum check on
 * `priority` (mirroring `QueueInvalidPriorityError`'s enum), since an
 * out-of-enum priority has no defined bump target. Not reused directly from
 * queue.mjs because that module's equivalent throws `QueueCorruptFileError`
 * for a malformed element — this module's contract is the opposite: never
 * throw, report each malformed element in `skipped` instead.
 *
 * @param {unknown} candidate
 * @returns {boolean}
 */
function isWellFormedEscalationEntry(candidate) {
  return (
    candidate !== null &&
    typeof candidate === 'object' &&
    !Array.isArray(candidate) &&
    typeof candidate.agentClass === 'string' &&
    typeof candidate.commandRef === 'string' &&
    typeof candidate.orchestratorId === 'string' &&
    typeof candidate.enqueuedAt === 'string' &&
    VALID_PRIORITIES.has(candidate.priority)
  );
}

/**
 * Whether `capacitySamples` provides sufficient evidence that capacity was
 * free THROUGHOUT `[windowStart, windowEnd]` — at least one `free: true`
 * sample inside the window, and NO `free: false` sample inside it. Zero
 * samples in the window (including an empty `capacitySamples` array
 * entirely) is NOT sufficient evidence — fails closed, exactly like a
 * `free: false` sample does. A non-finite `windowStart` (an unparsable
 * `enqueuedAt`, though `isWellFormedEscalationEntry` already requires a
 * string) also fails closed, never matching any sample.
 *
 * @param {number} windowStart
 * @param {number} windowEnd
 * @param {Array<{ at: number, free: boolean }>} capacitySamples
 * @returns {boolean}
 */
function wasCapacityFreeThroughout(windowStart, windowEnd, capacitySamples) {
  if (!isFiniteNumber(windowStart) || !isFiniteNumber(windowEnd)) return false;

  let sawFreeSample = false;
  for (const sample of capacitySamples) {
    if (!sample || !isFiniteNumber(sample.at) || typeof sample.free !== 'boolean') continue;
    if (sample.at < windowStart || sample.at > windowEnd) continue;
    if (sample.free === false) return false;
    sawFreeSample = true;
  }
  return sawFreeSample;
}

async function ensureQueueDir(filePath) {
  try {
    await fs.mkdir(dirname(filePath), { recursive: true });
  } catch {
    // Deliberately swallowed, mirroring queue.mjs's ensureQueueDir: an
    // unwritable parent dir surfaces naturally when the subsequent lock
    // acquisition/write fails on its own.
  }
}

/**
 * Serialises `fn` against every other `withEscalationLock` call for the same
 * `filePath` — same fencing-token shape as coordination-file.mjs's own
 * (unexported) `withLock` / queue.mjs's own (unexported) `withQueueLock`,
 * reusing `acquireLock`/`assertStillHeld`/`releaseLock` directly rather than
 * a third locking mechanism. Derives the SAME `${filePath}.lock` path that
 * queue.mjs's `withQueueLock` derives for the same `filePath` — this is
 * deliberate, not incidental: when this module is called against the live
 * resource-queue file, that lock-path identity is what provides
 * cross-module mutual exclusion with queue.mjs/dispatch.mjs today. It is not
 * a separate/independent lock.
 *
 * @template T
 * @param {string} filePath
 * @param {(lockContext: { lockPath: string, token: string, assertStillHeld: () => Promise<void> }) => Promise<T>} fn
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [lockOptions]
 * @returns {Promise<T>}
 */
async function withEscalationLock(filePath, fn, lockOptions = {}) {
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
 * Reads the raw queue-file array at `filePath`, tolerating a not-yet-created
 * file (`[]`, never throws). Does NOT validate individual elements — that is
 * this module's own escalation-eligibility concern (`isWellFormedEscalationEntry`),
 * evaluated per-element by the caller, never a whole-file throw.
 *
 * @param {string} filePath
 * @returns {Promise<Array<unknown>>}
 */
async function readRawEntries(filePath) {
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
 * A single atomic "beat" over the WHOLE persisted queue file at `filePath` —
 * see this module's header and `queue-aging-escalation.jest.spec.mjs` (the
 * spec this implementation satisfies) for the full contract. In short: bumps
 * `priority` exactly one tier toward "high" for every well-formed, not-yet-
 * escalated entry that has waited `>= options.boundMs` with corroborating
 * `free: true` evidence (and no `free: false` evidence) in
 * `options.capacitySamples` across its wait window, stamping `escalatedAt`
 * and firing a one-time `console.warn` alert (stamping `escalationAlertedAt`)
 * in the SAME beat. Malformed entries are never touched, never cause a
 * throw, and are reported in the returned `skipped` array. When at least one
 * entry escalates, the whole file is rewritten atomically in one
 * `writeEntriesAtomic` call — an all-or-nothing write; a `LockLostError`
 * mid-write leaves the file completely untouched. When NO entry escalates
 * this beat (`escalated.length === 0`, the common case), the write/rename is
 * skipped entirely — only the lock is taken and released — to avoid
 * unnecessary lock contention against dispatch.mjs's live claim/dequeue
 * traffic on every polling beat.
 *
 * @param {string} filePath
 * @param {{ now?: number, boundMs?: number, capacitySamples?: Array<{ at: number, free: boolean }>, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<{ escalated: Array<object>, alerted: Array<object>, skipped: Array<{ entry: unknown, reason: string }> }>}
 */
export async function escalateAgedQueueEntry(filePath, options = {}) {
  const referenceNow = isFiniteNumber(options.now) ? options.now : Date.now();
  const boundMs = isFiniteNumber(options.boundMs) ? options.boundMs : DEFAULT_ESCALATION_BOUND_MS;
  const capacitySamples = Array.isArray(options.capacitySamples) ? options.capacitySamples : [];

  await ensureQueueDir(filePath);

  return withEscalationLock(
    filePath,
    async (lockContext) => {
      const existing = await readRawEntries(filePath);

      const escalated = [];
      const alerted = [];
      const skipped = [];

      const nextEntries = existing.map((candidate) => {
        if (!isWellFormedEscalationEntry(candidate)) {
          skipped.push({ entry: candidate, reason: 'not a well-formed queue entry' });
          return candidate;
        }

        if (candidate.escalatedAt) {
          // Already escalated in an earlier beat — never re-bumped, never
          // re-alerted, even if still aged past boundMs.
          return candidate;
        }

        const windowStart = Date.parse(candidate.enqueuedAt);
        const ageMs = referenceNow - windowStart;
        const eligible =
          Number.isFinite(windowStart) &&
          ageMs >= boundMs &&
          wasCapacityFreeThroughout(windowStart, referenceNow, capacitySamples);

        if (!eligible) return candidate;

        const bumped = {
          ...candidate,
          priority: PRIORITY_BUMP[candidate.priority],
          escalatedAt: referenceNow,
          escalationAlertedAt: referenceNow,
        };
        escalated.push(bumped);
        alerted.push(bumped);
        console.warn(
          `queue-aging-escalation: entry ${JSON.stringify(bumped.agentClass)} (orchestrator ` +
            `${JSON.stringify(bumped.orchestratorId)}) has waited >= ${boundMs}ms while capacity was free — ` +
            `escalated to priority "${bumped.priority}"`,
        );
        return bumped;
      });

      if (escalated.length > 0) {
        await writeEntriesAtomic(filePath, nextEntries, lockContext);
      }

      return { escalated, alerted, skipped };
    },
    options,
  );
}
