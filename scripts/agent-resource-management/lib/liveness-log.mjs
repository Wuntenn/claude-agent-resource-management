// Phase 5 — report-consumable detection logging (watchdog trips,
// aging escalations, deadlock recycles).
//
// Deliberately its own module, not an extension of `history.mjs` — see this
// module's RED test suite (`liveness-log.jest.spec.mjs`) header for the full
// design-decision writeup (`history.mjs` is scoped to bounded-memory
// statistical folding of resource OBSERVATIONS; a detection EVENT has no
// `operationType`/`peakMemoryMb` shape to fold, and coercing one into the
// other's on-disk shape would either silently degrade `readHistory`'s exact
// contract or require a parallel non-folding path bolted onto a module built
// entirely around folding).
//
// On-disk shape: NDJSON (one self-contained JSON object per line), not a
// single growing JSON array — this log is currently the ONLY delivery
// mechanism for detections (Phase 6/beat-wiring and any dashboard don't
// exist yet), so it must be genuinely `grep`/`jq -c` friendly: a plain-text
// search for a `detectionType` value must match only the relevant line(s),
// which a minified one-line JSON array can never guarantee.
//
// Concurrent-write safety reuses `coordination-file.mjs`'s existing
// exclusive-create fencing lock (`acquireLock`/`assertStillHeld`/
// `releaseLock`) around a read-whole-file -> append-in-memory ->
// atomic-tmp-then-rename cycle — the same lock discipline
// `history.mjs`'s `recordObservation`/`writeStoreAtomic` house pattern
// applies to its JSON-array store, applied here to NDJSON text instead.
//
// Fails OPEN internally: every lock-acquisition, read, write, or rename
// failure is caught inside this module and resolved as
// `{ written: false, error }` rather than thrown. `history.mjs` can rely on
// an external caller's fail-open catch (`cli.mjs`'s existing wrapper around
// `recordObservation`); this module has no such caller yet — Phase 6 (the
// beat wiring) does not exist — so it is its own outermost caller and must
// not let a disk-full/permission error crash whatever eventually calls it.
//
// `outcome` is free-form per detection type but NEVER the literal
// `'alerted'`/`'delivered'` — this log alone cannot honestly claim delivery,
// only that an entry was written (see the spec header for the full
// rationale). Callers pass whatever outcome vocabulary fits
// ('recycled' | 'alert-only' | 'escalated' | 'skip' | ...).

import { promises as fs } from 'node:fs';
import { randomBytes } from 'node:crypto';

import { acquireLock, releaseLock, assertStillHeld } from './coordination-file.mjs';

/** Stable, versioned envelope field every entry carries. */
export const LIVENESS_LOG_SCHEMA_VERSION = 1;

// Phase 1: the live NDJSON file is bounded to this many entries.
// Once an append pushes it past this cap, the oldest overflowed entries are
// rotated out to a sibling `<path>.1` archive file (itself bounded to the
// same cap — a single-generation "one rotation back" ledger, not unbounded
// growth one file over). Implemented and covered green by
// `liveness-log-rotation.jest.spec.mjs` and
// `liveness-log-rotation-outer.jest.spec.mjs`.
export const LIVENESS_LOG_MAX_ENTRIES = 2000;

// Phase 2 post-review (High): `readLivenessLogTail`'s age-based stop
// assumes single-writer non-decreasing timestamp order, but the live log is
// a path shared across every concurrent orchestrator, and a slow append
// (e.g. `handleEvict`'s real grace-period sleep before its trip is logged)
// can land on disk after a faster, later-`now` append from a different
// orchestrator — reordering the tail relative to timestamp. After the walk
// hits its first over-age entry, it keeps scanning this many ADDITIONAL
// entries further back — still collecting any well-formed, in-window entry
// found within that stretch — before actually stopping, as a bounded
// defense-in-depth margin (not a guarantee) against that reordering. 25 is
// chosen generously relative to how many entries a handful of concurrently
// beating orchestrators (5-minute beat cadence) could plausibly interleave
// near the cutoff, while keeping the walk's total cost bounded to a small
// constant (`recentCount + AGE_STOP_LOOKBACK_MARGIN`) rather than the whole
// file. See `readLivenessLogTail`'s own doc comment for the full race
// scenario this guards against.
const AGE_STOP_LOOKBACK_MARGIN = 25;

// Matches `coordination-file.mjs`'s own `DEFAULT_LOCK_STALENESS_MS` — an
// orphaned `.lock` file (writer SIGKILL'd/OOM-killed mid-critical-section) is
// reclaimed after this long rather than wedging every future caller.
const DEFAULT_LOCK_STALENESS_MS = 30_000;

/**
 * @param {unknown} value
 * @returns {value is number}
 */
function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

/**
 * Builds a `detail` object from a set of optional, detection-type-specific
 * fields, dropping any that are `undefined`. Returns `undefined` (never `{}`)
 * when nothing is left, so `JSON.stringify` omits the `detail` key entirely
 * for a detection carrying no extra fields, rather than persisting an empty
 * object nobody asked for.
 *
 * @param {Record<string, unknown>} fields
 * @returns {Record<string, unknown> | undefined}
 */
function buildDetail(fields) {
  const present = Object.entries(fields).filter(([, value]) => value !== undefined);
  return present.length > 0 ? Object.fromEntries(present) : undefined;
}

/**
 * @param {{ now?: number }} [options]
 * @returns {number}
 */
function resolveNow(options) {
  return isFiniteNumber(options?.now) ? options.now : Date.now();
}

/**
 * Reads the whole log file as raw text, tolerating a not-yet-created file
 * (returns `''` rather than throwing). Any other read failure propagates —
 * the caller (`appendEntry`/`readLivenessLog`) decides how to handle it.
 *
 * @param {string} logFilePath
 * @returns {Promise<string>}
 */
async function readRawText(logFilePath) {
  try {
    return await fs.readFile(logFilePath, 'utf8');
  } catch (error) {
    if (error.code === 'ENOENT') return '';
    throw error;
  }
}

/**
 * Writes `text` atomically to `destPath`: serialise to a `<destPath>.tmp-
 * <random>` sibling, re-prove lock ownership, then `rename()` onto
 * `destPath` — mirroring `history.mjs`'s `writeStoreAtomic` exactly
 * (JSON-array store there, NDJSON text here; same lock discipline). Used for
 * both the live log file and its `<path>.1` archive
 * sibling — same atomicity guarantee applies to either destination.
 *
 * @param {string} destPath
 * @param {string} text
 * @param {{ assertStillHeld: () => Promise<void> }} lockContext
 * @returns {Promise<void>}
 */
async function writeLogAtomic(destPath, text, lockContext) {
  if (typeof lockContext?.assertStillHeld !== 'function') {
    throw new TypeError(
      'writeLogAtomic: a lockContext with an assertStillHeld() function is required — every write ' +
        'to the liveness log must re-prove, immediately before its rename, that this caller still ' +
        'holds the fencing lock.',
    );
  }

  const tmpPath = `${destPath}.tmp-${randomBytes(8).toString('hex')}`;
  try {
    await fs.writeFile(tmpPath, text, 'utf8');
    await lockContext.assertStillHeld();
    await fs.rename(tmpPath, destPath);
  } catch (error) {
    await fs.unlink(tmpPath).catch(() => {});
    throw error;
  }
}

/**
 * Appends one entry to the NDJSON log at `logFilePath`, serialised against
 * every other concurrent appender via `coordination-file.mjs`'s fencing lock.
 * Fails OPEN: every failure — lock acquisition, read, write, or rename — is
 * caught here and resolved as `{ written: false, error }`, never thrown.
 *
 * @param {string} logFilePath
 * @param {object} entry
 * @param {{ lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<{ written: boolean, entry?: object, error?: Error }>}
 */
async function appendEntry(logFilePath, entry, options = {}) {
  const lockStalenessMs = isFiniteNumber(options.lockStalenessMs) ? options.lockStalenessMs : DEFAULT_LOCK_STALENESS_MS;
  const lockPath = `${logFilePath}.lock`;

  let token;
  try {
    token = await acquireLock(lockPath, lockStalenessMs, options.lockMaxAttempts, options.lockRetryDelayMs);
  } catch (error) {
    return { written: false, error };
  }

  const lockContext = { lockPath, token, assertStillHeld: () => assertStillHeld(lockPath, token) };
  const archivePath = `${logFilePath}.1`;
  try {
    const rawText = await readRawText(logFilePath);
    const nextText = `${rawText}${JSON.stringify(entry)}\n`;
    const liveLineCount = nextText.split('\n').filter((line) => line.trim().length > 0).length;

    if (liveLineCount > LIVENESS_LOG_MAX_ENTRIES) {
      // Phase 1: the live file alone has crossed the cap. Fold in
      // whatever is already archived (chronologically OLDER than anything
      // still on the live file) so the oldest-overflow/newest-cap split is
      // computed over the FULL known history, not just this single append's
      // own 1-entry excess — otherwise a long-running cap-crossing streak
      // would only ever archive its very last eviction and silently drop
      // every earlier one (see `liveness-log-rotation.jest.spec.mjs`
      // scenario 2's exact multi-entry archive requirement).
      const rawArchiveText = await readRawText(archivePath);
      const archiveLinesFromDisk = rawArchiveText.split('\n').filter((line) => line.trim().length > 0);
      const liveLinesFromDisk = rawText.split('\n').filter((line) => line.trim().length > 0);

      const combinedLines = [...archiveLinesFromDisk, ...liveLinesFromDisk, JSON.stringify(entry)];

      const overflowCount = combinedLines.length - LIVENESS_LOG_MAX_ENTRIES;
      // Bound the archive itself to the same cap — this is a "recoverable
      // one generation back" ledger, not a second unbounded-growth file;
      // once the archive's own oldest-overflow set would exceed the cap,
      // only the newest `LIVENESS_LOG_MAX_ENTRIES` of it are kept.
      const archiveLines = combinedLines.slice(0, overflowCount).slice(-LIVENESS_LOG_MAX_ENTRIES);
      const liveLines = combinedLines.slice(overflowCount);

      // Write the archive FIRST, before touching the live file. If THIS
      // write/rename fails, `appendEntry` fails open with the live file
      // completely untouched — the overflowed (oldest) entries would
      // otherwise be lost from BOTH files the moment this step failed. See
      // scenario 6 in `liveness-log-rotation.jest.spec.mjs` for the exact
      // fault this ordering exists to guard against.
      //
      // Phase 1 design simplification: if instead the archive write
      // succeeds but the SUBSEQUENT live write below fails, the rotated
      // entries are simply left present in both files — a transient,
      // harmless duplicate — rather than a marker-based "heal it on the next
      // call" mechanism. Three review rounds on that mechanism each
      // surfaced a new unsound edge case (a content-equality dedup that
      // could falsely collapse two genuinely-distinct coincidentally-
      // identical entries; a marker file to fix that; a stale-marker/crash-
      // window edge case in the marker fix that could ALSO silently discard
      // real entries) — chasing perfect duplicate-elimination for a rare,
      // non-hot-path, beat-triggered log append is disproportionate to what
      // this log is actually for. This log's only consumer today
      // (`resolveRecentlyEvictedPids` in `cli.mjs`) folds entries into a
      // `Set<number>` of pids — a duplicate entry for the same pid is a
      // complete no-op there (`Set.add` on an already-present value does
      // nothing). The duplicate is not permanent either: the next call's
      // fold-and-resplit treats the duplicated line as one more entry in the
      // combined set, and the live file's own cap keeps shrinking the window
      // in which both copies are visible. No entry is ever silently lost —
      // worst case is an inert duplicate for a bounded window.
      const archiveText = `${archiveLines.join('\n')}\n`;
      await writeLogAtomic(archivePath, archiveText, lockContext);

      const liveText = `${liveLines.join('\n')}\n`;
      await writeLogAtomic(logFilePath, liveText, lockContext);
    } else {
      await writeLogAtomic(logFilePath, nextText, lockContext);
    }

    return { written: true, entry };
  } catch (error) {
    return { written: false, error };
  } finally {
    // Best-effort: a release failure here is cleanup noise, not the outcome
    // this call already resolved above — never let it escape and turn a
    // resolved `{ written: ... }` into a rejection.
    await releaseLock(lockPath, token).catch(() => {});
  }
}

/**
 * Appends one `detectionType: 'watchdog-trip'` entry.
 *
 * `growthRateMbPerMin` (Phase 2, stability review Medium 1)
 * is the leak-velocity signal's own measured growth rate — present only on
 * trips this beat attributed (wholly or partly, see `corroboratingSignals`
 * below) to leak-velocity, absent/omitted for a plain `stalled` trip that
 * never had one. This is the ONLY durable, production-observable window into
 * whether `DEFAULT_LEAK_VELOCITY_THRESHOLDS` (an unvalidated starting
 * hypothesis) is producing false positives — omitting it from this log
 * defeats that purpose entirely.
 *
 * `corroboratingSignals` (Phase 2, stability review Medium 2)
 * records any OTHER detection signal that also fired for this same pid in
 * this same beat but lost the precedence race (e.g. a pid that tripped BOTH
 * `isAgentStalled` and leak-velocity is evicted under `reason: 'stalled'`,
 * but the leak-velocity evidence is still durably recorded here rather than
 * silently discarded) — purely additive metadata, never changes `outcome`/
 * `reason` themselves.
 *
 * @param {string} logFilePath
 * @param {{ pid?: string | number, orchestratorId: string, leaseId?: string, outcome: string, reason?: string, growthRateMbPerMin?: number, corroboratingSignals?: Array<object> }} params
 * @param {{ now?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<{ written: boolean, entry?: object, error?: Error }>}
 */
export async function logWatchdogTrip(logFilePath, params, options = {}) {
  const { pid, orchestratorId, leaseId, outcome, reason, growthRateMbPerMin, corroboratingSignals } = params;
  const entry = {
    schemaVersion: LIVENESS_LOG_SCHEMA_VERSION,
    detectionType: 'watchdog-trip',
    timestamp: resolveNow(options),
    pid,
    orchestratorId,
    outcome,
    detail: buildDetail({ leaseId, reason, growthRateMbPerMin, corroboratingSignals }),
  };
  return appendEntry(logFilePath, entry, options);
}

/**
 * Appends one `detectionType: 'aging-escalation'` entry. `agentClass` is
 * threaded into `queueItemId` — the closest thing `queue.mjs` entries have
 * to a stable identifier today.
 *
 * @param {string} logFilePath
 * @param {{ agentClass: string, orchestratorId: string, priority?: string, outcome: string }} params
 * @param {{ now?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<{ written: boolean, entry?: object, error?: Error }>}
 */
export async function logAgingEscalation(logFilePath, params, options = {}) {
  const { agentClass, orchestratorId, priority, outcome } = params;
  const entry = {
    schemaVersion: LIVENESS_LOG_SCHEMA_VERSION,
    detectionType: 'aging-escalation',
    timestamp: resolveNow(options),
    queueItemId: agentClass,
    orchestratorId,
    outcome,
    detail: buildDetail({ priority }),
  };
  return appendEntry(logFilePath, entry, options);
}

/**
 * Appends one `detectionType: 'deadlock-recycle'` entry. Per
 * `eviction-targeting.mjs`'s own documented pid-substitution convention —
 * dibs entries carry no OS pid — callers substitute `orchestratorId` for
 * `pid` when no real pid exists; `pid` is simply omitted from the persisted
 * entry when not supplied.
 *
 * @param {string} logFilePath
 * @param {{ pid?: string | number, orchestratorId: string, outcome: string, reason?: string }} params
 * @param {{ now?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<{ written: boolean, entry?: object, error?: Error }>}
 */
export async function logDeadlockRecycle(logFilePath, params, options = {}) {
  const { pid, orchestratorId, outcome, reason } = params;
  const entry = {
    schemaVersion: LIVENESS_LOG_SCHEMA_VERSION,
    detectionType: 'deadlock-recycle',
    timestamp: resolveNow(options),
    pid,
    orchestratorId,
    outcome,
    detail: buildDetail({ reason }),
  };
  return appendEntry(logFilePath, entry, options);
}

/**
 * Reads the NDJSON log's TAIL, walking BACKWARD line-by-line from the newest
 * (last) line toward the oldest (first), parsing one line at a time — the
 * bounded counterpart to `readLivenessLog` below, for callers
 * (`resolveRecentlyEvictedPids` in `cli.mjs`) that only care about recent
 * entries and want to stop reading once trustworthy/relevant data runs out,
 * rather than eagerly parsing the entire file up front (Phase 2/3).
 *
 * Two INDEPENDENT stopping conditions, either of which ends the walk —
 * whichever is hit first:
 *
 * 1. **Corruption stop** (always active): the FIRST line that either fails
 *    to parse as JSON, or parses to something that is valid JSON but not the
 *    object shape every entry this module writes actually has (`null`, a
 *    bare number/string/boolean, or an array — all valid JSON, none of them
 *    an entry). `appendEntry` above only ever appends new entries at the
 *    end of the file, in the order callers invoke it — the live file's own
 *    line order is never reshuffled (rotation moves whole lines out to the
 *    `.1` archive, it never reorders what remains) — so a line that fails to
 *    parse walking backward can only be OLDER than everything already
 *    collected. Every entry newer than the corruption is therefore already
 *    safely in hand, and nothing reliable remains further back, so stopping
 *    there can never lose a genuinely newer, well-formed entry.
 *
 * 2. **Age stop, with a bounded lookback margin** (only when
 *    `options.now`/`options.cooldownMs` are both supplied): the walk keeps
 *    collecting well-formed in-window entries until it hits the FIRST
 *    well-formed entry whose `timestamp` is older than `now - cooldownMs` —
 *    but does not stop there. It continues scanning up to
 *    `AGE_STOP_LOOKBACK_MARGIN` further entries past that point, still
 *    collecting anything in that stretch that turns out to be well-formed
 *    and in-window, before actually stopping. This is a heuristic safety
 *    margin, NOT a mathematical guarantee — see below for why one is needed
 *    and why it cannot be perfect.
 *
 *    For a SINGLE writer, appends land in strictly non-decreasing timestamp
 *    order by file position, and the first over-age entry walking backward
 *    would soundly prove every older position is also over-age. But
 *    `ARM_LIVENESS_LOG_FILE` is a single path SHARED across every concurrent
 *    orchestrator (`cli.mjs`); each one's watchdog beat computes `now` once
 *    up front and then may not actually call `appendEntry` (and thus land
 *    its write on disk) until an arbitrary amount of real wall-clock time
 *    later — e.g. `handleEvict`'s SIGTERM -> grace-sleep -> SIGKILL sequence
 *    has a real default 60s grace period before its `logWatchdogTrip` append
 *    lands. Concretely: orchestrator A captures `now = T0`, spends 60s in a
 *    real eviction's grace period, then appends an entry stamped `T0`.
 *    Meanwhile orchestrator B starts and finishes a fast beat at real time
 *    `T0 + 30s` with no delay, appending an entry stamped `T0 + 30000` —
 *    which physically lands on disk BEFORE A's, since A is still asleep in
 *    its grace period when B writes. The live file's file-position order is
 *    now `[..., B(T0+30000), A(T0)]` — decreasing by timestamp at that
 *    point, despite no corruption anywhere. Walking backward, the FIRST
 *    over-age entry encountered could therefore be one like A's, sitting
 *    ahead (in file position) of some other orchestrator C's genuinely
 *    in-window entry that happened to land even earlier on disk. Stopping
 *    at that first over-age entry would wrongly exclude C's pid from
 *    `resolveRecentlyEvictedPids`'s result, allowing a premature re-eviction
 *    attempt against it during its cooldown window — the exact failure mode
 *    `DEFAULT_WATCHDOG_EVICT_COOLDOWN_MS` exists to prevent.
 *
 *    `AGE_STOP_LOOKBACK_MARGIN` defends against exactly this without
 *    reverting to an unbounded scan: it is chosen generously relative to how
 *    many entries a realistic clock/lock skew between a handful of
 *    concurrently-running orchestrators, at a multi-minute beat cadence,
 *    could plausibly interleave near the cutoff, while keeping total parse
 *    cost bounded to a small constant (`recentCount + margin`, not the whole
 *    file). It reduces — it does not eliminate — the risk of missing a
 *    genuinely in-window entry to reordering this severe: a true fix would
 *    need a global ordering token or per-writer sequence numbers, which is
 *    disproportionate complexity for this risk level (the practical blast
 *    radius of missing an entry is bounded to an occasional redundant
 *    SIGTERM against an already-terminating pid, self-correcting on the very
 *    next beat).
 *
 *    Without both `now` and `cooldownMs` supplied, no age-based stop (nor
 *    its lookback margin) is applied and only the corruption stop above is
 *    in effect — this keeps `readLivenessLogTail` a generally useful
 *    primitive rather than one hardcoded to a single caller's cooldown
 *    semantics.
 *
 * Tolerates a not-yet-created file (`[]`, never throws for that case).
 *
 * @param {string} logFilePath
 * @param {{ now?: number, cooldownMs?: number }} [options] Both must be
 *   finite numbers for the age stop to activate; omit either (or both) to
 *   fall back to corruption-stop-only behaviour.
 * @returns {Promise<Array<object>>} Entries in NEWEST-FIRST order — the
 *   walk collects as it goes backward from the tail, so the returned array
 *   is the exact REVERSE of `readLivenessLog`'s oldest-first order below,
 *   despite both functions reading the same underlying file. Today's only
 *   caller (`resolveRecentlyEvictedPids` in `cli.mjs`) folds every entry
 *   into a `Set`, so this is order-independent for it, but a future caller
 *   relying on `entries[0]`/`.find()` for "most recent" or "oldest" must
 *   account for this — do not assume it matches `readLivenessLog`'s shape.
 */
export async function readLivenessLogTail(logFilePath, options = {}) {
  const raw = await readRawText(logFilePath);
  if (!raw) return [];

  const cutoffTimestamp =
    isFiniteNumber(options.now) && isFiniteNumber(options.cooldownMs) ? options.now - options.cooldownMs : undefined;

  const lines = raw.split('\n').filter((line) => line.trim().length > 0);
  const recentEntries = [];
  // `undefined` until the first over-age entry is seen; then counts down
  // from `AGE_STOP_LOOKBACK_MARGIN` to `0`, at which point the walk stops.
  // See the doc comment above for why this margin exists.
  let lookbackRemaining;
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    if (lookbackRemaining !== undefined) {
      if (lookbackRemaining <= 0) break;
      lookbackRemaining -= 1;
    }

    let entry;
    try {
      const parsed = JSON.parse(lines[index]);
      // A line can be VALID JSON (`null`, a bare number/string/boolean, an
      // array) without being the object shape every entry this module writes
      // actually has — `JSON.parse` alone does not fail on those, so the
      // corruption-stop `catch` above would never trigger for them, and the
      // `entry.timestamp` read just below would then throw on a `null`/
      // non-object `entry` (`TypeError: Cannot read properties of null`).
      // That throw would escape `readLivenessLogTail` entirely, which its
      // only production caller (`resolveRecentlyEvictedPids` in `cli.mjs`)
      // catches at a much coarser grain — resolving an EMPTY `Set`, i.e.
      // cooldown suppression silently disabled for the whole beat, the exact
      // opposite of what the corruption-stop exists to guarantee (fail open
      // gracefully, not fail open to "nothing is suppressed"). Treat this
      // the same as an unparseable line: stop the walk here, inside this
      // same protective boundary, rather than let it escape as a thrown
      // exception.
      if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
        break;
      }
      entry = parsed;
    } catch {
      break;
    }

    const isOverAge = cutoffTimestamp !== undefined && isFiniteNumber(entry.timestamp) && entry.timestamp < cutoffTimestamp;
    if (isOverAge) {
      if (lookbackRemaining === undefined) {
        lookbackRemaining = AGE_STOP_LOOKBACK_MARGIN;
      }
      continue;
    }

    recentEntries.push(entry);
  }
  return recentEntries;
}

/**
 * Appends one `detectionType: 'arm-gate-fail-closed'` entry ( 
 * decision 3).
 *
 * A fail-closed trip is the PreToolUse gate denying a spawn for an OPTED-IN
 * session because `cli.mjs` gave no trustworthy answer (crash, non-zero exit,
 * unparseable/empty stdout, timeout, or missing target). It is deliberately
 * NOT the bucket for an ordinary policy denial (`hold`/`pause`): 
 * wants the fail-closed path's real firing rate auditable and greppable, and
 * folding routine policy denials in here would destroy exactly that
 * measurement.
 *
 * Dibs-shaped detections carry no OS pid, so — per the same convention
 * `logDeadlockRecycle` documents — `pid` is simply omitted and
 * `orchestratorId` (the session-derived id from `orchestrator-id.mjs`) is the
 * attribution key.
 *
 * @param {string} logFilePath
 * @param {{ orchestratorId: string, outcome: string, reason?: string, toolName?: string }} params
 * @param {{ now?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number }} [options]
 * @returns {Promise<{ written: boolean, entry?: object, error?: Error }>}
 */
export async function logArmGateFailClosed(logFilePath, params, options = {}) {
  const { orchestratorId, outcome, reason, toolName } = params;
  const entry = {
    schemaVersion: LIVENESS_LOG_SCHEMA_VERSION,
    detectionType: 'arm-gate-fail-closed',
    timestamp: resolveNow(options),
    orchestratorId,
    outcome,
    detail: buildDetail({ reason, toolName }),
  };
  return appendEntry(logFilePath, entry, options);
}

/**
 * Reads the whole NDJSON log back as an array of parsed entries, tolerating
 * a not-yet-created file (`[]`, never throws for the not-yet-created case).
 *
 * @param {string} logFilePath
 * @returns {Promise<Array<object>>}
 */
export async function readLivenessLog(logFilePath) {
  let raw;
  try {
    raw = await fs.readFile(logFilePath, 'utf8');
  } catch (error) {
    if (error.code === 'ENOENT') return [];
    throw error;
  }

  if (!raw) return [];

  return raw
    .split('\n')
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line));
}
