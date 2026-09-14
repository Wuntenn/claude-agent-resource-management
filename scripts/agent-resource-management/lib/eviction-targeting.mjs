// Phase 4 — recycle wiring: resolves watchdog/deadlock trips into
// eviction targets for the real --evict-pid surface.
//
// Two pure/synchronous exports only — no fs, no child_process, no
// `Date.now()`. See the header of ./eviction-targeting.jest.spec.mjs for the
// full design-decision rationale this module implements verbatim.

/**
 * Find a claimLedger record by an exact `pid` match. `claimLedger` is the
 * ONLY trusted source of identifiers — this never reads pid/leaseId/
 * orchestratorPid/orchestratorId off a trip object itself.
 *
 * Uses `Array.prototype.find` (never bracket/property lookup on a plain
 * object keyed by untrusted input) so `__proto__`/`constructor`-shaped or
 * empty-string values can never resolve via the prototype chain.
 */
function findLedgerRecordByPid(claimLedger, pid) {
  if (!Array.isArray(claimLedger)) {
    return undefined;
  }
  return claimLedger.find((record) => record && record.pid === pid);
}

/**
 * Find a claimLedger record by an exact `orchestratorId` match — the
 * documented pid-substitution lookup path for deadlock-trip stalled
 * entries, which carry no real OS pid at all.
 */
function findLedgerRecordByOrchestratorId(claimLedger, orchestratorId) {
  if (!Array.isArray(claimLedger)) {
    return undefined;
  }
  if (typeof orchestratorId !== 'string' || orchestratorId.length === 0) {
    return undefined;
  }
  return claimLedger.find((record) => record && record.orchestratorId === orchestratorId);
}

/**
 * Counts how many claimLedger records share a given `orchestratorId` — an
 * orchestrator can have multiple candidate pids in flight at once (one
 * ledger record per candidate, all carrying the SAME `orchestratorId`; see
 * `cli.mjs`'s `buildWatchdogClaimLedger`). Used ONLY by the deadlock-trip
 * resolution path below to detect that ambiguous, multi-candidate case.
 */
function countLedgerRecordsForOrchestrator(claimLedger, orchestratorId) {
  if (!Array.isArray(claimLedger)) {
    return 0;
  }
  return claimLedger.filter((record) => record && record.orchestratorId === orchestratorId).length;
}

function normalizeRecentlyEvictedPids(recentlyEvictedPids) {
  if (!recentlyEvictedPids) {
    return new Set();
  }
  if (recentlyEvictedPids instanceof Set) {
    return recentlyEvictedPids;
  }
  if (Array.isArray(recentlyEvictedPids)) {
    return new Set(recentlyEvictedPids);
  }
  return new Set();
}

/**
 * Build the final target entry from a resolved claimLedger record — only
 * the ledger record's own fields are ever used for pid/leaseId/
 * orchestratorPid/orchestratorId composition.
 */
function buildResolvedTarget(record, recentlyEvicted) {
  const { pid, leaseId, orchestratorPid, orchestratorId, reentrant } = record;

  if (recentlyEvicted.has(pid)) {
    return {
      pid,
      action: 'skip',
      skipReason: 'recently evicted — cooldown in effect',
    };
  }

  // Unknown/missing reentrant flag defaults to the SAFE direction.
  const action = reentrant === true ? 'evict' : 'alert-only';

  return { pid, leaseId, orchestratorPid, orchestratorId, action };
}

/**
 * resolveEvictionTargets({ watchdogTrips, deadlockTrip, claimLedger, recentlyEvictedPids })
 *   -> Array<{ pid, leaseId, orchestratorPid, orchestratorId,
 *              action: 'evict' | 'alert-only' | 'skip', skipReason? }>
 */
export function resolveEvictionTargets({ watchdogTrips, deadlockTrip, claimLedger, recentlyEvictedPids } = {}) {
  const recentlyEvicted = normalizeRecentlyEvictedPids(recentlyEvictedPids);
  const trips = Array.isArray(watchdogTrips) ? watchdogTrips : [];
  const stalledPids =
    deadlockTrip && deadlockTrip.trip === true && Array.isArray(deadlockTrip.stalledPids)
      ? deadlockTrip.stalledPids
      : [];

  const results = [];
  const seenPids = new Set();

  for (const trip of trips) {
    const tripPid = trip && trip.pid;
    const record = findLedgerRecordByPid(claimLedger, tripPid);

    if (!record) {
      results.push({
        pid: tripPid,
        action: 'skip',
        skipReason: 'no matching claimLedger record for watchdog-trip pid',
      });
      continue;
    }

    if (seenPids.has(record.pid)) {
      continue;
    }
    seenPids.add(record.pid);
    results.push(buildResolvedTarget(record, recentlyEvicted));
  }

  for (const stalledPid of stalledPids) {
    const record = findLedgerRecordByOrchestratorId(claimLedger, stalledPid);

    if (!record) {
      results.push({
        orchestratorId: stalledPid,
        action: 'skip',
        skipReason: 'no matching claimLedger record for deadlock-trip orchestratorId',
      });
      continue;
    }

    if (seenPids.has(record.pid)) {
      continue;
    }
    seenPids.add(record.pid);

    // pre-PR review, High — the deadlock-trip liveness SIGNAL (fed by
    // `buildFleetSignalResolvers`, which resolves ONE candidate pid per
    // `orchestratorId` via last-write-wins over `claimLedger`) and this
    // TARGET resolution (first-match via `findLedgerRecordByOrchestratorId`)
    // can silently disagree about WHICH of an orchestrator's several
    // candidate pids was actually judged stalled, when that orchestrator has
    // more than one candidate in the ledger. There is no reliable way from
    // this module's own inputs to know which pid the stall judgement
    // actually applies to in that case, so auto-eviction is refused — routed
    // to `alert-only` instead of `evict`/`skip` — for any orchestrator with
    // more than one claimLedger candidate. A single-candidate orchestrator
    // has no such ambiguity (the one candidate IS the one judged stalled),
    // so it keeps the normal `reentrant`-driven `evict`/`alert-only` split.
    // Documented as a known limitation in SKILL.md alongside the related
    // pid/lease-pairing gap.
    if (countLedgerRecordsForOrchestrator(claimLedger, stalledPid) > 1) {
      const { pid, leaseId, orchestratorPid, orchestratorId } = record;
      results.push({
        pid,
        leaseId,
        orchestratorPid,
        orchestratorId,
        action: 'alert-only',
      });
      continue;
    }

    results.push(buildResolvedTarget(record, recentlyEvicted));
  }

  return results;
}

/**
 * filterEvictable(results) -> Array<{ pid, leaseId, orchestratorPid,
 *   orchestratorId, action: 'evict' }>
 *
 * Pure filter over `resolveEvictionTargets`'s return value: keeps ONLY the
 * entries with `action === 'evict'`, dropping `alert-only` and `skip`
 * entries entirely. Exists so a Phase 6 caller can enforce "only evict-
 * tagged pids ever reach handleEvict" in code, rather than relying on every
 * future caller to remember to check `action` itself before calling
 * `--evict-pid`.
 *
 * Never throws on a non-array input — returns `[]` instead, matching this
 * module's fail-closed posture elsewhere.
 *
 * @example
 *   const toEvict = filterEvictable(resolveEvictionTargets({
 *     watchdogTrips, deadlockTrip, claimLedger, recentlyEvictedPids,
 *   }));
 *   for (const target of toEvict) {
 *     // call handleEvict(target.pid, target.leaseId, target.orchestratorPid, ...)
 *   }
 */
export function filterEvictable(results) {
  if (!Array.isArray(results)) {
    return [];
  }
  return results.filter((entry) => entry && entry.action === 'evict');
}

// Verified against the full `performEviction` body in ../cli.mjs (the shared
// eviction CORE both `handleEvict` and `handleWatchdogBeat` call). Every
// `reason` string it ever actually assigns:
//   - `buildFallback(reason)` (pre-attempt fallbacks, before any kill is
//     attempted): 'lease-not-found', 'lease-orchestrator-mismatch',
//     'orchestrator-pid-mismatch', 'pid-lease-mismatch', 'not-authorized',
//     'candidate-not-found'.
//   - after `gracefulStop`: 'graceful-stop-failed' (thrown stopError),
//     'release-failed' (releaseQueueEntry threw), 'release-not-found'
//     (releaseQueueEntry returned null — no matching on-disk entry).
//
// KEEP THIS SET IN SYNC with `performEviction`'s `buildFallback` call sites.
// A reason missing here does not merely go unlisted — `classifyEvictionOutcome`
// collapses it to the generic `'unknown'`, which `runWatchdogBeat`
// (./watchdog-beat.mjs) then stamps as BOTH the `outcome` and the `reason` on
// its emitted JSON body and its liveness NDJSON log. That is exactly the
// "silent masking" this module's own contract forbids, and it is worst for a
// security-gate refusal like 'orchestrator-pid-mismatch', whose whole
// point is to be visible when it fires.
const KNOWN_FAILURE_REASONS = new Set([
  'lease-not-found',
  'lease-orchestrator-mismatch',
  // — the orchestrator-pid-binding refusal: the caller-supplied
  // `--orchestrator-pid` did not equal the pid a prior beat recorded on the
  // dibs ledger for this `orchestratorId` (including the fail-closed
  // "no pid recorded at all" case).
  'orchestrator-pid-mismatch',
  // — the pid-lease binding refusal: the caller-supplied `--evict-pid`
  // did not equal the pid `bindPidToLease` (./queue.mjs, Phase 1) recorded on
  // the lease at claim time. Additive to the ps-tree ancestry check, not a
  // substitute for it; see the `performEviction` comment above this reason's
  // call site in ../cli.mjs for the full rationale.
  'pid-lease-mismatch',
  'not-authorized',
  'candidate-not-found',
  'graceful-stop-failed',
  'release-failed',
  'release-not-found',
]);

// The other failure branch `handleEvict` emits (cli.mjs ~3610-3651, the
// `!CONFIRMED_STOPPED_OUTCOMES.has(stopResult.outcome)` block): the target is
// confirmed still running (or its identity may have changed under the grace
// window), so no `reason` field is set at all — only `outcome` carries the
// story. Per that block's own comment, `outcome` here is always one of these
// three literals. Surfaced distinctly (not collapsed to 'unknown') so the
// operationally worst outcomes stay observable — see 'no silent masking'.
const OUTCOME_ONLY_FAILURE_OUTCOMES = new Set(['permission-denied', 'unconfirmed-after-kill', 'target-changed-before-kill']);

/**
 * classifyEvictionOutcome(handleEvictResult) -> { recycled: boolean, reason?: string }
 *
 * A successful evict result carries `type: 'evict'` with no
 * `evictionAttempted` field (i.e. it never entered any of handleEvict's
 * failure-reporting branches). Failure shapes all set `evictionAttempted:
 * true`, but the story is carried differently depending on which branch:
 * most set a `reason` drawn from `KNOWN_FAILURE_REASONS`; the
 * confirmed-still-running branch instead sets only an `outcome` drawn from
 * `OUTCOME_ONLY_FAILURE_OUTCOMES` (no `reason` key at all) — that `outcome`
 * is surfaced here as this function's `reason` so it is never
 * indistinguishable from a malformed/garbage input. Never throws on a
 * malformed/unexpected shape.
 */
export function classifyEvictionOutcome(handleEvictResult) {
  if (!handleEvictResult || typeof handleEvictResult !== 'object') {
    return { recycled: false, reason: 'unknown' };
  }

  if (handleEvictResult.evictionAttempted !== true) {
    if (handleEvictResult.type === 'evict' && handleEvictResult.released === true) {
      return { recycled: true };
    }
    return { recycled: false, reason: 'unknown' };
  }

  if (typeof handleEvictResult.reason === 'string' && KNOWN_FAILURE_REASONS.has(handleEvictResult.reason)) {
    return { recycled: false, reason: handleEvictResult.reason };
  }

  if (
    handleEvictResult.reason === undefined &&
    typeof handleEvictResult.outcome === 'string' &&
    OUTCOME_ONLY_FAILURE_OUTCOMES.has(handleEvictResult.outcome)
  ) {
    return { recycled: false, reason: handleEvictResult.outcome };
  }

  return { recycled: false, reason: 'unknown' };
}
