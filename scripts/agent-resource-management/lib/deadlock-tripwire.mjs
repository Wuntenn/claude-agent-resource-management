// Phase 3 — deadlock tripwire (all-stalled + non-empty queue,
// sustained window).
//
// PURE CORE + THIN PERSISTENCE WRAPPER, mirroring ./liveness.mjs's pure-
// composition style and ./queue-aging-escalation.mjs's persisted-state-with-
// lock house pattern. See ./deadlock-tripwire.jest.spec.mjs's header for the
// full design rationale this implementation satisfies — including the
// documented `orchestratorId`-as-`pid` substitution (dibs entries carry no
// OS `pid`; a live entry's `orchestratorId` is threaded through
// `isAgentStalled`'s `pid` param and returned in `stalledPids`), which this
// module implements per the spec's actual assertions, not as an assertion of
// literal fact.
//
// `evaluateDeadlockWindow` is the ENTIRE trip/no-trip/window-tracking
// decision: a plain, synchronous, side-effect-free function — every input
// (including the already-persisted `stalledSince`) is threaded in
// explicitly, no env reads, no `Date.now()`, no fs. `detectDeadlock` is the
// thin I/O wrapper that resolves those inputs from the live coordination
// file, the live queue file, and injected liveness resolvers, then persists
// the resulting `stalledSince` back to `stateFilePath` under the same
// lock/atomic-write primitives `./coordination-file.mjs` exports — reusing
// `./queue-aging-escalation.mjs`'s house pattern for a persisted-state
// module, not a bespoke locking mechanism.
//
// Read-only, best-effort against the coordination file: a read failure
// (including a torn/mid-write file caught mid-race) degrades to "treat as
// zero live agents this beat" rather than throwing — never a thrown error
// out of `detectDeadlock` itself. The queue read gets the identical
// best-effort treatment for the same reason.
//
// COMPOUND-CONDITION ATOMICITY — explicitly NOT fully atomic this phase,
// same posture Phase 2 (`./queue-aging-escalation.mjs`) documented for its
// own non-atomicity: the coordination-file read and the queue read are two
// separate file reads, not one consistent snapshot. What IS closed:
// `evaluateDeadlockWindow`'s own window/debounce math is single-consistent-
// read within itself (one `now`, one `agentLivenessResults` snapshot, one
// `stalledSince` read).

import { dirname } from 'node:path';
import { promises as fs } from 'node:fs';

import {
  acquireLock,
  assertStillHeld,
  releaseLock,
  writeEntriesAtomic,
  readDibs,
  isReservedEntry,
} from './coordination-file.mjs';
import { peekQueue } from './queue.mjs';
import { isAgentStalled } from './liveness.mjs';

/**
 * The chosen default for `options.windowMs` when a caller doesn't override
 * it — 15 minutes. A single noisy beat can never trip the tripwire on its
 * own; the sustained condition must hold continuously for this long.
 */
export const DEFAULT_DEADLOCK_WINDOW_MS = 15 * 60 * 1000;

const DEFAULT_LOCK_STALENESS_MS = 30_000;

/**
 * The chosen default for `options.livenessThresholdMs` when a caller doesn't
 * override it — 15 minutes, matching `cli.mjs`'s own
 * `DEFAULT_LIVENESS_THRESHOLD_MS`. This module defines its own copy rather
 * than importing that constant from `cli.mjs`, preserving the lib/cli.mjs
 * dependency direction every other module in this skill maintains (see
 * `./footprint-state.mjs`'s `DEFAULT_STALE_AFTER_MS` for the same reasoning);
 * `cli.mjs` is expected to pass its own constant explicitly once it wires
 * this module in.
 *
 * Without this, `readDibs` is called with no `livenessThresholdMs`, and
 * `pruneStale` (./coordination-file.mjs) never runs — a dibs entry from an
 * orchestrator that has since crashed stays "live" forever, and because
 * `evaluateDeadlockWindow` requires ALL live agents to be stalled, one such
 * ghost entry permanently defeats the tripwire for the entire fleet.
 */
export const DEFAULT_LIVENESS_THRESHOLD_MS = 15 * 60 * 1000;

/**
 * @param {unknown} value
 * @returns {value is number}
 */
function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

/**
 * Pure core: the entire trip/no-trip/window-tracking decision.
 *
 * The ALL-STALLED + NON-EMPTY-QUEUE condition is judged fresh from
 * `agentLivenessResults`/`queueNonEmpty` on every call — an EMPTY fleet
 * (`agentLivenessResults.length === 0`) is never considered "the condition
 * holds", regardless of `queueNonEmpty`; the debounce clock never starts for
 * an empty fleet.
 *
 * When the condition does NOT hold this beat (any active agent, empty
 * queue, or empty fleet), the debounce clock is reset — `stalledSince: null`
 * — even if a prior `stalledSince` was mid-window; a later beat where the
 * fleet returns to 100% stalled starts a FRESH window, never resurrecting a
 * stale one.
 *
 * When the condition DOES hold, `stalledSince` is either the incoming value
 * (already-running clock, preserved byte-for-byte — this is what makes a
 * persisted restart reconstruct elapsed time correctly, Convergence #6) or,
 * on the first beat the condition is observed holding (`stalledSince` was
 * `null`/not finite), `now` — the clock starting fresh. `trip` fires once
 * `now - stalledSince >= windowMs` (inclusive boundary), and remains `true`
 * for any beat at or beyond that point — a sustained condition, not an
 * edge-triggered pulse.
 *
 * `stalledPids` is the de-duplicated set of `pid`s from `agentLivenessResults`
 * when `trip` is `true`, and `[]` otherwise.
 *
 * @param {{
 *   agentLivenessResults: Array<{ pid: string|number, stalled: boolean }>,
 *   queueNonEmpty: boolean,
 *   now: number,
 *   windowMs: number,
 *   stalledSince: number | null,
 * }} params
 * @returns {{ trip: boolean, stalledPids: Array<string|number>, stalledSince: number | null }}
 */
export function evaluateDeadlockWindow({ agentLivenessResults, queueNonEmpty, now, windowMs, stalledSince }) {
  const liveAgents = Array.isArray(agentLivenessResults) ? agentLivenessResults : [];

  const conditionHolds =
    liveAgents.length > 0 && queueNonEmpty === true && liveAgents.every((agent) => agent && agent.stalled === true);

  if (!conditionHolds) {
    return { trip: false, stalledPids: [], stalledSince: null };
  }

  const effectiveStalledSince = isFiniteNumber(stalledSince) ? stalledSince : now;
  const trip = now - effectiveStalledSince >= windowMs;
  const stalledPids = trip ? [...new Set(liveAgents.map((agent) => agent.pid))] : [];

  return { trip, stalledPids, stalledSince: effectiveStalledSince };
}

async function ensureStateDir(filePath) {
  try {
    await fs.mkdir(dirname(filePath), { recursive: true });
  } catch {
    // Deliberately swallowed, mirroring queue-aging-escalation.mjs's
    // ensureQueueDir: an unwritable parent dir surfaces naturally when the
    // subsequent lock acquisition/write fails on its own.
  }
}

/**
 * Serialises `fn` against every other `withStateLock` call for the same
 * `filePath` — same fencing-token shape as coordination-file.mjs's own
 * `withLock`/queue-aging-escalation.mjs's `withEscalationLock`, reusing
 * `acquireLock`/`assertStillHeld`/`releaseLock` directly rather than a
 * separate locking mechanism.
 *
 * @template T
 * @param {string} filePath
 * @param {(lockContext: { lockPath: string, token: string, assertStillHeld: () => Promise<void> }) => Promise<T>} fn
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [lockOptions]
 * @returns {Promise<T>}
 */
async function withStateLock(filePath, fn, lockOptions = {}) {
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
 * Reads the persisted `{ stalledSince }` debounce state at `stateFilePath`,
 * tolerating a not-yet-created file, an empty file, or a corrupt/unparsable
 * one — all degrade to `null` ("no window currently running") rather than
 * throwing, matching this module's read-only-best-effort posture.
 *
 * @param {string} stateFilePath
 * @returns {Promise<number | null>}
 */
async function readPersistedStalledSince(stateFilePath) {
  let raw;
  try {
    raw = await fs.readFile(stateFilePath, 'utf8');
  } catch {
    return null;
  }
  if (!raw) return null;

  try {
    const parsed = JSON.parse(raw);
    return isFiniteNumber(parsed?.stalledSince) ? parsed.stalledSince : null;
  } catch {
    return null;
  }
}

/**
 * The thin I/O wrapper. Enumerates currently-live agents via `readDibs`
 * (./coordination-file.mjs — reserved pseudo-entries, per `isReservedEntry`,
 * are excluded, since those are internal bookkeeping records, not agents),
 * checks queue non-emptiness via `peekQueue` (./queue.mjs), applies
 * `isAgentStalled` (./liveness.mjs) to each live entry via the caller-
 * injected `options.getFootprintTrajectory`/`options.getWorktreeMtimeMs`
 * resolvers (defaulting to `() => []` / `() => null` — "no evidence, never
 * stalled" — when omitted), and persists `{ stalledSince }` window state to
 * `stateFilePath` under the same lock/atomic-write primitives
 * `./coordination-file.mjs` exports.
 *
 * A coordination-file read failure (including a torn/mid-write file caught
 * mid-race) degrades to "zero live agents this beat" rather than throwing.
 * A missing/corrupt queue file degrades to "queue empty this beat" the same
 * way.
 *
 * @param {string} coordinationFilePath
 * @param {string} queueFilePath
 * @param {string} stateFilePath
 * @param {{
 *   now?: number,
 *   windowMs?: number,
 *   boundMs?: number,
 *   getFootprintTrajectory?: (orchestratorId: string) => number[],
 *   getWorktreeMtimeMs?: (orchestratorId: string) => number | null,
 *   livenessThresholdMs?: number,
 *   lockStalenessMs?: number,
 *   lockMaxAttempts?: number,
 *   lockRetryDelayMs?: number,
 * }} [options]
 * @returns {Promise<{ trip: boolean, stalledPids: Array<string|number>, stalledSince: number | null }>}
 */
export async function detectDeadlock(coordinationFilePath, queueFilePath, stateFilePath, options = {}) {
  const now = isFiniteNumber(options.now) ? options.now : Date.now();
  const windowMs = isFiniteNumber(options.windowMs) ? options.windowMs : DEFAULT_DEADLOCK_WINDOW_MS;
  const boundMs = isFiniteNumber(options.boundMs) ? options.boundMs : undefined;
  const getFootprintTrajectory =
    typeof options.getFootprintTrajectory === 'function' ? options.getFootprintTrajectory : () => [];
  const getWorktreeMtimeMs =
    typeof options.getWorktreeMtimeMs === 'function' ? options.getWorktreeMtimeMs : () => null;
  const livenessThresholdMs = isFiniteNumber(options.livenessThresholdMs)
    ? options.livenessThresholdMs
    : DEFAULT_LIVENESS_THRESHOLD_MS;

  let liveEntries;
  try {
    liveEntries = await readDibs(coordinationFilePath, now, { livenessThresholdMs });
  } catch {
    liveEntries = [];
  }
  if (!Array.isArray(liveEntries)) liveEntries = [];

  let queueEntries;
  try {
    queueEntries = await peekQueue(queueFilePath, now);
  } catch {
    queueEntries = [];
  }
  const queueNonEmpty = Array.isArray(queueEntries) && queueEntries.length > 0;

  const agentLivenessResults = liveEntries
    .filter((entry) => entry && typeof entry.orchestratorId === 'string' && !isReservedEntry(entry.orchestratorId))
    .map((entry) => {
      const orchestratorId = entry.orchestratorId;
      const stalled = isAgentStalled({
        boundMs,
        now,
        footprintTrajectory: getFootprintTrajectory(orchestratorId),
        worktreeMtimeMs: getWorktreeMtimeMs(orchestratorId),
      });
      return { pid: orchestratorId, stalled };
    });

  await ensureStateDir(stateFilePath);

  return withStateLock(
    stateFilePath,
    async (lockContext) => {
      const persistedStalledSince = await readPersistedStalledSince(stateFilePath);

      const result = evaluateDeadlockWindow({
        agentLivenessResults,
        queueNonEmpty,
        now,
        windowMs,
        stalledSince: persistedStalledSince,
      });

      await writeEntriesAtomic(stateFilePath, { stalledSince: result.stalledSince }, lockContext);

      return result;
    },
    options,
  );
}
