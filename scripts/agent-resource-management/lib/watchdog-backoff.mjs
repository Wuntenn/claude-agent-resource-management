// Phase 6 — watchdog beat's own shared-pacing-pattern-shaped backoff-state
// module. See ./watchdog-backoff.jest.spec.mjs's header for the full design
// rationale (in short: same SHAPE as a load-dependent backoff protocol's
// documented `last_full_run`/`turns_to_wait`/`last_outcome` contract, doubling
// capped at 8 — but persisted as plain JSON, not markdown, since this state
// is read/written exclusively by this module's own code, never skimmed by a
// human or another agent at the top of a cron-triggered turn).
//
// Deliberately generic (never watchdog-specific) so a future ARM-family beat
// can reuse this module verbatim, following the same "one implementation,
// referenced not copied" principle.

import { promises as fs } from 'node:fs';
import { randomBytes } from 'node:crypto';

/** "Stay hot" reset value, and the bootstrap default for a never-yet-run state. */
export const DEFAULT_TURNS_TO_WAIT = 1;

/** Doubling never grows `turnsToWait` past this, even from an already-high/corrupted starting value. */
export const MAX_TURNS_TO_WAIT = 8;

/**
 * @param {unknown} value
 * @returns {value is number}
 */
function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

/**
 * PURE, synchronous — no fs, no `Date.now()`. See this module's own header
 * and `./watchdog-backoff.jest.spec.mjs` for the full doubling contract.
 *
 * @param {{ didWork: boolean, now: number, previousTurnsToWait?: number | null }} params
 * @returns {{ lastFullRun: number, turnsToWait: number, lastOutcome: 'worked' | 'no-op' }}
 */
export function computeNextBackoffState({ didWork, now, previousTurnsToWait }) {
  if (didWork) {
    return { lastFullRun: now, turnsToWait: DEFAULT_TURNS_TO_WAIT, lastOutcome: 'worked' };
  }

  const isValidPreviousTurnsToWait =
    isFiniteNumber(previousTurnsToWait) && Number.isInteger(previousTurnsToWait) && previousTurnsToWait > 0;
  const baseline = isValidPreviousTurnsToWait ? previousTurnsToWait : DEFAULT_TURNS_TO_WAIT;
  const turnsToWait = Math.min(baseline * 2, MAX_TURNS_TO_WAIT);

  return { lastFullRun: now, turnsToWait, lastOutcome: 'no-op' };
}

/**
 * PURE. `null`/non-finite `lastFullRun` (never-yet-run) is ALWAYS due,
 * regardless of `turnsToWait` — the same bootstrap rule `readBackoffState`
 * encodes via its own defaults.
 *
 * @param {{ lastFullRun: number | null, turnsToWait: number, now: number, cronIntervalMs: number }} params
 * @returns {boolean}
 */
export function isBackoffDue({ lastFullRun, turnsToWait, now, cronIntervalMs }) {
  if (!isFiniteNumber(lastFullRun)) return true;
  if (!isFiniteNumber(cronIntervalMs) || cronIntervalMs <= 0) return true;

  const turnsElapsed = Math.floor((now - lastFullRun) / cronIntervalMs);
  return turnsElapsed >= turnsToWait;
}

/**
 * Tolerant of a not-yet-created file and of an empty/corrupt file — both
 * degrade to the bootstrap defaults ("treat as immediately due") rather than
 * throwing, matching this whole skill's fail-open-on-read posture (e.g.
 * ./queue-aging-escalation.mjs's `readRawEntries`/./deadlock-tripwire.mjs's
 * `readPersistedStalledSince`).
 *
 * @param {string} filePath
 * @returns {Promise<{ lastFullRun: number | null, turnsToWait: number, lastOutcome: string | null }>}
 */
export async function readBackoffState(filePath) {
  const bootstrap = { lastFullRun: null, turnsToWait: DEFAULT_TURNS_TO_WAIT, lastOutcome: null };

  let raw;
  try {
    raw = await fs.readFile(filePath, 'utf8');
  } catch {
    return bootstrap;
  }
  if (!raw) return bootstrap;

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return bootstrap;
  }

  if (!parsed || typeof parsed !== 'object') return bootstrap;

  return {
    lastFullRun: isFiniteNumber(parsed.lastFullRun) ? parsed.lastFullRun : null,
    turnsToWait:
      isFiniteNumber(parsed.turnsToWait) && Number.isInteger(parsed.turnsToWait) && parsed.turnsToWait > 0
        ? parsed.turnsToWait
        : DEFAULT_TURNS_TO_WAIT,
    lastOutcome: typeof parsed.lastOutcome === 'string' ? parsed.lastOutcome : null,
  };
}

/**
 * Serialises `<path>.tmp-<random>` -> `rename()`, the same atomic-write idiom
 * every other persisted-state module in this directory uses (e.g.
 * ./liveness-log.mjs's `writeLogAtomic`) — a reader can only ever observe the
 * whole prior file or the whole new file, never a torn/partial write, and a
 * crash between the `writeFile` and the `rename` leaves the ORIGINAL file
 * completely untouched.
 *
 * @param {string} filePath
 * @param {{ lastFullRun: number, turnsToWait: number, lastOutcome: string }} state
 * @returns {Promise<void>}
 */
export async function writeBackoffStateAtomic(filePath, state) {
  const tmpPath = `${filePath}.tmp-${randomBytes(8).toString('hex')}`;
  try {
    await fs.writeFile(tmpPath, JSON.stringify(state), 'utf8');
    await fs.rename(tmpPath, filePath);
  } catch (error) {
    await fs.unlink(tmpPath).catch(() => {});
    throw error;
  }
}
