// Phase 6 — watchdog beat wiring: composes Phase 1 (per-agent
// liveness), Phase 3 (deadlock tripwire), Phase 2 (queue-aging escalation),
// and Phase 4 (recycle wiring: resolveEvictionTargets/filterEvictable) into
// a single beat, logging every detection via Phase 5 (liveness-log).
//
// See ./watchdog-beat.jest.spec.mjs's header for the full contract this
// module implements verbatim — composition order, output shape, and the
// "throws before any log write when an evictable target exists but no
// handleEvict was supplied" safety property.

import { isAgentStalled as defaultIsAgentStalled } from './liveness.mjs';
import {
  computeLeakVelocity as defaultComputeLeakVelocity,
  DEFAULT_LEAK_VELOCITY_THRESHOLDS,
} from './leak-velocity.mjs';
import { detectDeadlock as defaultDetectDeadlock } from './deadlock-tripwire.mjs';
import { escalateAgedQueueEntry as defaultEscalateAgedQueueEntry } from './queue-aging-escalation.mjs';
import {
  resolveEvictionTargets as defaultResolveEvictionTargets,
  filterEvictable as defaultFilterEvictable,
  classifyEvictionOutcome as defaultClassifyEvictionOutcome,
} from './eviction-targeting.mjs';
import {
  logWatchdogTrip as defaultLogWatchdogTrip,
  logAgingEscalation as defaultLogAgingEscalation,
  logDeadlockRecycle as defaultLogDeadlockRecycle,
} from './liveness-log.mjs';

/**
 * Per-orchestrator-per-beat cap on how many pids may be EVICTED via a
 * `leak-velocity`-only trip in a single `runWatchdogBeat` invocation
 * (Phase 2, stability review, Medium finding). This is scoped to ONE
 * orchestrator's own beat: `runWatchdogBeat` receives only the `candidates`
 * the calling orchestrator owns (`cli.mjs`'s `buildWatchdogCandidates` is
 * keyed by that orchestrator's `orchestratorPid`), and the cap is applied to
 * that array in-memory with no shared counter, lock file, or other
 * cross-invocation state — so if N orchestrators are each running their own
 * `--watchdog-beat`, each may independently evict up to this many
 * leak-velocity-only pids in the same wall-clock beat. It is NOT a fleet-wide
 * (host-wide, across-all-orchestrators) cap.
 *
 * Like `DEFAULT_LEAK_VELOCITY_THRESHOLDS` (./leak-velocity.mjs), this is
 * an UNVALIDATED STARTING HYPOTHESIS, not a measured or tuned default — no
 * production telemetry has yet informed it. It exists purely as a mechanical
 * brake: if `DEFAULT_LEAK_VELOCITY_THRESHOLDS` is miscalibrated for a class
 * of legitimately-heavy workloads, every agent running that workload under
 * ONE orchestrator could otherwise trip in the same beat, producing a
 * correlated mass-eviction within that orchestrator with no brake at all. A
 * correlated multi-pid leak-velocity trip in one beat is itself evidence the
 * THRESHOLD, not the agents, is wrong — so beyond this cap, excess trips are
 * recorded (outcome `'leak-velocity-throttled'`) but NOT evicted, leaving
 * them visible for tuning rather than silently acted on.
 *
 * Deliberately scoped to `leak-velocity` trips ONLY — `stalled` trips (from
 * `isAgentStalled`) are the pre-existing, already-trusted-alone signal and
 * are never subject to this cap, no matter how many fire in one beat.
 *
 * @type {number}
 */
export const DEFAULT_MAX_LEAK_VELOCITY_EVICTIONS_PER_BEAT = 3;

/**
 * Cap on how many `handleEvict` calls the bounded-concurrency eviction
 * pool (see the pool loop below, after `processTarget`) may run
 * simultaneously in a single `runWatchdogBeat` invocation.
 *
 * Deliberately a SEPARATE constant from
 * `DEFAULT_MAX_LEAK_VELOCITY_EVICTIONS_PER_BEAT` above, even though both
 * currently hold the same numeric value — the two concepts are unrelated and
 * must be free to diverge independently:
 *   - `DEFAULT_MAX_LEAK_VELOCITY_EVICTIONS_PER_BEAT` throttles how many
 *     `leak-velocity`-only TRIPS are allowed to become eviction targets at
 *     all (a detection-side brake against a miscalibrated threshold; see its
 *     own doc comment above).
 *   - `DEFAULT_MAX_CONCURRENT_EVICTIONS_PER_BEAT` bounds how many already-
 *     resolved `evict` targets (of ANY origin — `stalled`, `leak-velocity`,
 *     or deadlock-recycle alike) may have their `handleEvict` call genuinely
 *     in flight AT ONCE — a pool-sizing concern, not a detection-throttling
 *     one.
 *
 * @type {number}
 */
export const DEFAULT_MAX_CONCURRENT_EVICTIONS_PER_BEAT = 3;

/**
 * Resolves the observable `outcome` string for a single evict attempt: the
 * real `handleEvict` result's own `outcome` field on a genuine success (e.g.
 * `'already-gone'`/`'exited-before-grace'`/`'killed-after-grace'`, per
 * `cli.mjs`'s `handleEvict` shapes), falling back to the literal `'recycled'`
 * when a successful result carries no `outcome` field at all (e.g. a test
 * fake), and to `classified.reason` on any non-recycled outcome — matching
 * `classifyEvictionOutcome`'s own documented shapes.
 *
 * @param {unknown} handleEvictResult
 * @param {{ recycled: boolean, reason?: string }} classified
 * @returns {string}
 */
function resolveOutcome(handleEvictResult, classified) {
  if (classified.recycled) {
    return typeof handleEvictResult?.outcome === 'string' ? handleEvictResult.outcome : 'recycled';
  }
  return typeof classified.reason === 'string' ? classified.reason : 'unknown';
}

/**
 * `liveness-log.mjs`'s writers deliberately fail OPEN — a lock/read/write/
 * rename failure resolves `{ written: false, error }` rather than rejecting,
 * so a transient logging hiccup can never abort an in-flight eviction
 * decision (see `./liveness-log.jest.spec.mjs`'s header for that contract).
 * That contract is right for the *logger*, but wrong left unchecked at this
 * *call site*: this beat's own header calls its liveness log "the ONLY
 * delivery mechanism for detections", and every call site here was silently
 * discarding a `written: false` result — a disk-full/permission/lock failure
 * would print a normal beat body and exit 0 while nothing was actually
 * recorded (and would silently disable the flapping cooldown downstream,
 * since `resolveRecentlyEvictedPids` reads back exactly these entries).
 *
 * pre-PR review, Medium — fail this beat LOUD (throw, matching the
 * genuine-`handleEvict`-rejection contract this module already enforces)
 * rather than silently swallowing an undelivered detection.
 *
 * @param {{ written: boolean, error?: unknown }} logResult
 * @param {string} logKind
 */
function assertLogWritten(logResult, logKind) {
  if (logResult?.written === false) {
    throw new Error(
      `runWatchdogBeat: failed to write ${logKind} to the liveness log — ${
        logResult?.error?.message ?? 'unknown error'
      }. Refusing to report this beat as successful when a detection could not be durably recorded.`,
    );
  }
}

/**
 * @param {{
 *   now: number, boundMs: number, deadlockWindowMs: number, agingBoundMs: number,
 *   candidates?: Array<{ pid: number, footprintTrajectory: number[], worktreeMtimeMs: number|null, leakVelocityState?: object }>,
 *   claimLedger?: Array<{ pid: number, leaseId: string, orchestratorPid: number, orchestratorId: string, reentrant?: boolean }>,
 *   orchestratorId?: string,
 *   recentlyEvictedPids?: Set<number>|Array<number>,
 *   coordinationFilePath: string, queueFilePath: string, deadlockStateFilePath: string, liveLogFilePath: string,
 *   capacitySamples?: Array<{ at: number, free: boolean }>,
 *   getFootprintTrajectory?: (orchestratorId: string) => number[],
 *   getWorktreeMtimeMs?: (orchestratorId: string) => number|null,
 *   leakVelocityConfig?: { thresholdMbPerMin: number, consecutivePollsRequired: number, minSamples?: number },
 *   maxLeakVelocityEvictionsPerBeat?: number,
 *   handleEvict?: (target: object) => Promise<object>,
 *   deps?: object,
 * }} params
 * @returns {Promise<{ type: 'watchdog', trips: Array<object>, deadlockTripped: boolean, agingEscalations: Array<object>, recycled: Array<number|string> }>}
 */
export async function runWatchdogBeat({
  now,
  boundMs,
  deadlockWindowMs,
  agingBoundMs,
  candidates = [],
  claimLedger = [],
  orchestratorId,
  recentlyEvictedPids,
  coordinationFilePath,
  queueFilePath,
  deadlockStateFilePath,
  liveLogFilePath,
  capacitySamples,
  getFootprintTrajectory = () => [],
  getWorktreeMtimeMs = () => null,
  leakVelocityConfig = DEFAULT_LEAK_VELOCITY_THRESHOLDS,
  maxLeakVelocityEvictionsPerBeat = DEFAULT_MAX_LEAK_VELOCITY_EVICTIONS_PER_BEAT,
  handleEvict,
  deps = {},
}) {
  const isAgentStalled = deps.isAgentStalled ?? defaultIsAgentStalled;
  const computeLeakVelocity = deps.computeLeakVelocity ?? defaultComputeLeakVelocity;
  const detectDeadlock = deps.detectDeadlock ?? defaultDetectDeadlock;
  const escalateAgedQueueEntry = deps.escalateAgedQueueEntry ?? defaultEscalateAgedQueueEntry;
  const resolveEvictionTargets = deps.resolveEvictionTargets ?? defaultResolveEvictionTargets;
  const filterEvictable = deps.filterEvictable ?? defaultFilterEvictable;
  const classifyEvictionOutcome = deps.classifyEvictionOutcome ?? defaultClassifyEvictionOutcome;
  const logWatchdogTrip = deps.logWatchdogTrip ?? defaultLogWatchdogTrip;
  const logAgingEscalation = deps.logAgingEscalation ?? defaultLogAgingEscalation;
  const logDeadlockRecycle = deps.logDeadlockRecycle ?? defaultLogDeadlockRecycle;

  // Step 1a — per-candidate stall judgement (Phase 1), pure/sync, no I/O.
  const stalledPids = new Set(
    candidates
      .filter((candidate) =>
        isAgentStalled({
          pid: candidate.pid,
          boundMs,
          now,
          footprintTrajectory: candidate.footprintTrajectory,
          worktreeMtimeMs: candidate.worktreeMtimeMs,
        }),
      )
      .map((candidate) => candidate.pid),
  );

  // Step 1b — per-candidate leak-velocity judgement, pure/
  // sync, no I/O. Only candidates carrying a `leakVelocityState` are even
  // considered — this beat's CLI-level caller is responsible for reading
  // that per-pid trajectory from footprint-state.mjs before invoking this
  // beat (out of this module's own scope; see ./leak-velocity.mjs's header).
  // A candidate with NO `leakVelocityState` at all models "no
  // footprint-state.mjs entry exists for this pid yet" (cold start) — it is
  // simply skipped, never throws, never trips.
  const leakVelocityTripByPid = new Map();
  for (const candidate of candidates) {
    if (!candidate.leakVelocityState) continue;
    const leakResult = computeLeakVelocity(candidate.leakVelocityState, leakVelocityConfig);
    if (leakResult.trip) {
      leakVelocityTripByPid.set(candidate.pid, {
        pid: candidate.pid,
        reason: 'leak-velocity',
        growthRateMbPerMin: leakResult.growthRateMbPerMin,
      });
    }
  }

  // PRECEDENCE RULE: when a pid trips BOTH `isAgentStalled`
  // and leak-velocity in the SAME beat, `stalled` wins — it is the
  // pre-existing, more certain signal (a stalled agent is already known to
  // be making zero progress at all), whereas a leak-velocity trip is a
  // resource-growth heuristic that does not by itself prove the agent has
  // stopped doing useful work. Exactly ONE trip entry is ever produced per
  // pid (never two, one per signal) — this loop only consults
  // `leakVelocityTripByPid` for a pid that did NOT already trip `stalled`.
  // This ordering is also what keeps this module's own `tripByPid` lookup
  // (Step 6 below, for `originatingTrip`) in agreement with
  // `resolveEvictionTargets`'s own `seenPids`-keeps-FIRST dedup
  // (./eviction-targeting.mjs) — since `trips` below only ever contains ONE
  // entry per pid to begin with, there is nothing left for either dedup
  // strategy (Map-overwrite here, seenPids-first there) to disagree about.
  const trips = [];
  for (const candidate of candidates) {
    if (stalledPids.has(candidate.pid)) {
      const stalledTrip = { pid: candidate.pid, reason: 'stalled' };
      // Phase 2 (stability review Medium 2) — a pid that
      // ALSO tripped leak-velocity this same beat loses the precedence race
      // (see the block comment above) but its evidence must not be silently
      // discarded: attach it as purely-additive metadata. Never changes the
      // winning `reason` ('stalled') or the "evicted exactly once" outcome.
      const corroboratingLeakTrip = leakVelocityTripByPid.get(candidate.pid);
      if (corroboratingLeakTrip) {
        stalledTrip.corroboratingSignals = [
          { reason: 'leak-velocity', growthRateMbPerMin: corroboratingLeakTrip.growthRateMbPerMin },
        ];
      }
      trips.push(stalledTrip);
    } else if (leakVelocityTripByPid.has(candidate.pid)) {
      trips.push(leakVelocityTripByPid.get(candidate.pid));
    }
  }

  // Step 1d (Phase 2, stability review Medium 3) — cap how
  // many of THIS beat's leak-velocity-attributed trips are allowed to
  // proceed to eviction. Scoped to trips whose WINNING reason is
  // 'leak-velocity' only — a 'stalled' trip that also corroborates
  // leak-velocity is not touched by this cap at all (isAgentStalled is the
  // pre-existing, already-trusted-alone signal; see
  // DEFAULT_MAX_LEAK_VELOCITY_EVICTIONS_PER_BEAT's own doc comment above).
  //
  // Deterministic, re-run-stable selection: sort by growth rate DESCENDING
  // (the fastest-growing, highest-urgency pids are kept), tie-broken by pid
  // ASCENDING — never insertion order alone, so an identical `candidates`
  // input always throttles the same pids regardless of array construction
  // order.
  const leakVelocityOnlyTrips = trips.filter((trip) => trip.reason === 'leak-velocity');
  const throttledPids = new Set();
  // pre-PR review, Low — clamp to >= 0 before using it as a `slice`
  // boundary below. `maxLeakVelocityEvictionsPerBeat` is not exposed as a
  // CLI flag today (only the positive `DEFAULT_MAX_LEAK_VELOCITY_EVICTIONS_PER_BEAT`
  // reaches this function in practice), but a negative value would otherwise
  // hit JS's negative-index `Array.prototype.slice` semantics (counting from
  // the end) instead of the intended "0 means throttle everything" behavior —
  // clamping keeps this function's contract correct for any caller, present
  // or future.
  const effectiveMaxLeakVelocityEvictionsPerBeat = Math.max(0, maxLeakVelocityEvictionsPerBeat);
  if (leakVelocityOnlyTrips.length > effectiveMaxLeakVelocityEvictionsPerBeat) {
    const sorted = [...leakVelocityOnlyTrips].sort((a, b) => {
      const rateDelta = (b.growthRateMbPerMin ?? 0) - (a.growthRateMbPerMin ?? 0);
      return rateDelta !== 0 ? rateDelta : a.pid - b.pid;
    });
    for (const trip of sorted.slice(effectiveMaxLeakVelocityEvictionsPerBeat)) {
      throttledPids.add(trip.pid);
    }
  }

  const tripByPid = new Map(trips.map((trip) => [trip.pid, trip]));

  // Phase 4 pre-PR review, Medium 3 — resolves the pid -> orchestratorId
  // mapping Step 5b needs below. `claimLedger` is this beat's own ONE
  // trusted identifier source (see its header comment above) pairing each
  // candidate pid with the owning orchestrator; every OTHER `logWatchdogTrip`
  // call site in this module already supplies `orchestratorId` from a
  // resolved target/claimLedger record, and the throttled-trip log entry
  // below must not be the one exception that omits it.
  //
  // pre-PR review round 2, Medium — `buildWatchdogClaimLedger`
  // (cli.mjs) only pairs `Math.min(candidates.length, leasedItems.length)`
  // entries, so an orchestrator with >= 4 candidate pids (enough to trigger
  // the leak-velocity throttle) but FEWER leased queue items produces
  // throttled pids with no `claimLedger` entry at all —
  // `orchestratorIdByPid.get(pid)` is `undefined` in that reachable
  // configuration. This beat's own caller always knows its OWN
  // `orchestratorId` (it is a required top-level flag on `--watchdog-beat`),
  // so it is threaded through as the fallback below rather than left
  // `undefined`.
  const orchestratorIdByPid = new Map(claimLedger.map((record) => [record.pid, record.orchestratorId]));

  // Step 2 — deadlock-window evaluation (Phase 3) against the real
  // coordination/queue files this beat was given.
  const deadlockTrip = await detectDeadlock(coordinationFilePath, queueFilePath, deadlockStateFilePath, {
    now,
    windowMs: deadlockWindowMs,
    boundMs,
    getFootprintTrajectory,
    getWorktreeMtimeMs,
  });
  const deadlockStalledIds = new Set(deadlockTrip?.trip === true ? deadlockTrip.stalledPids ?? [] : []);

  // Step 3 — queue-aging escalation (Phase 2) against the queue file.
  const agingResult = await escalateAgedQueueEntry(queueFilePath, { now, boundMs: agingBoundMs, capacitySamples });
  const agingEscalations = Array.isArray(agingResult?.escalated) ? agingResult.escalated : [];

  // Step 4 — recycle-target resolution (Phase 4) over the Step-1 trips +
  // Step-2 deadlock result + claimLedger. Throttled leak-velocity pids
  // (Step 1d) are deliberately excluded from `watchdogTrips` here — they
  // must never be resolved into an 'evict' target at all, only logged
  // separately below (see the throttled-trips loop after the evict guard).
  const evictionEligibleTrips = trips.filter((trip) => !throttledPids.has(trip.pid));
  const rawTargets = resolveEvictionTargets({
    watchdogTrips: evictionEligibleTrips,
    deadlockTrip,
    claimLedger,
    recentlyEvictedPids,
  });

  // Phase 2 pre-PR review, High — `resolveEvictionTargets` resolves
  // `deadlockTrip.stalledPids` INDEPENDENTLY of `evictionEligibleTrips`
  // (its `seenPids` dedup only ever sees what `watchdogTrips` fed it), so a
  // pid excluded above for being over the per-beat leak-velocity cap can
  // still come back as a SECOND `evict` target here if that same pid's
  // `orchestratorId` also happens to be named in `deadlockTrip.stalledPids`
  // this beat (a wedged, leaking agent is a realistic candidate for both).
  // Left unfiltered, Step 6 below would resolve `tripByPid.get(target.pid)`
  // back to the SAME trip Step 5b already stamped
  // `outcome: 'leak-velocity-throttled'` / `evicted: false`, overwrite it
  // with a real eviction, and write a second, contradictory watchdog-trip
  // log entry for the same pid in the same beat — defeating the throttle
  // entirely via the deadlock-tripwire side door. Drop any such target
  // BEFORE it ever reaches `filterEvictable`/Step 6; Step 5b above has
  // already logged the one, authoritative `leak-velocity-throttled` entry
  // for this pid this beat, so no further log write is needed here.
  const targets = rawTargets.filter((target) => target.pid === undefined || !throttledPids.has(target.pid));
  const evictable = filterEvictable(targets);

  //, corrected post-review — isolation is decided by ERROR TYPE, never
  // by how many `evict` targets this beat happens to have. The original cut
  // of this fix gated isolation on `evictActionCount > 1`, which is wrong on
  // both axes: (1) a multi-target beat hitting a genuinely systemic,
  // whole-process misconfiguration (`cli.mjs`'s `performEviction` throwing
  // `EvictFakeSeamCouplingError` — see its own header comment: a leftover
  // `ARM_FAKE_*` env var, a polluted shared env block, NOT a per-target
  // failure) would have EVERY target throw the identical error, and the
  // count-based gate silently isolated all of them into fabricated
  // `reason: 'unknown'` outcomes instead of failing the beat loudly — exactly
  // the safety regression `watchdog-outer-acceptance.jest.spec.mjs`'s
  // guard exists to catch; and (2) it wrongly forbade a single-target beat
  // from isolating an ordinary per-target failure at all.
  //
  // `error.armFailBeat === true` is the marker `EvictFakeSeamCouplingError`
  // (and any future exception `performEviction`'s own header comment calls
  // out as similarly systemic/deliberately-uncaught) sets on itself — a
  // plain property check, not an `instanceof`/class import, because
  // `watchdog-beat.mjs` takes `handleEvict` purely as an injected function
  // and must never depend on `cli.mjs`'s internals to interpret what it
  // throws (see this module's own header comment on the DI seam). Any error
  // NOT carrying that marker is isolated to its own target, regardless of
  // how many `evict` targets are in this beat; any error carrying it is
  // re-thrown and fails the whole beat loudly, regardless of target count.
  const isSystemicEvictFailure = (error) => Boolean(error) && error.armFailBeat === true;

  // Load-bearing safety property: never silently no-op a real recycle
  // because the caller forgot to wire the injection seam. Checked BEFORE any
  // log write (Step 5/6 below) is attempted.
  if (evictable.length > 0 && typeof handleEvict !== 'function') {
    throw new Error(
      'runWatchdogBeat: at least one evictable target was resolved but no handleEvict was supplied — refusing ' +
        'to silently skip a real recycle. Callers must always inject handleEvict when a beat can produce an ' +
        "'evict' action.",
    );
  }

  const recycled = [];

  // Step 5 (formerly Step 6) — log every aging escalation BEFORE the evict
  // loop below.
  //
  // pre-PR review, High — `escalateAgedQueueEntry` (Step 3, above) has
  // already mutated the queue file on disk (bumping `priority` and stamping
  // `escalatedAt`) by the time we get here, and that stamp makes escalation
  // strictly one-shot (`queue-aging-escalation.mjs`'s `escalatedAt` guard).
  // If this log write ran AFTER the possibly-throwing evict loop below and
  // one of that loop's `handleEvict` calls threw, the beat would abort with
  // the escalation durably applied to the queue but its detection NEVER
  // logged — and no future beat could ever log it, because the one-shot
  // guard would skip it forever. Logging aging escalations first ensures the
  // detection is always durably recorded before any evict-loop failure can
  // abort the beat.
  for (const escalation of agingEscalations) {
    // pre-PR review, Medium — thread this beat's own logical `now`
    // through explicitly rather than letting `logAgingEscalation` fall back
    // to its own `Date.now()` default. Every cooldown/window computation
    // elsewhere in this beat (Steps 1-4) is anchored to the SAME `now` this
    // caller supplies (real or `ARM_FAKE_NOW_MS`-faked); a log entry stamped
    // with the real wall clock instead would introduce a second, independent
    // time source purely by omission, which is exactly what let
    // `watchdog-outer-acceptance.jest.spec.mjs`'s cooldown regression test
    // carry ~1s of unaccounted slack between a spawned beat's real-clock log
    // timestamp and its own faked `now`.
    const logResult = await logAgingEscalation(
      liveLogFilePath,
      {
        agentClass: escalation?.agentClass,
        orchestratorId: escalation?.orchestratorId,
        priority: escalation?.priority,
        outcome: 'escalated',
      },
      { now },
    );
    assertLogWritten(logResult, 'an aging-escalation entry');
  }

  // Step 5b (Phase 2, stability review Medium 3) — log every
  // THROTTLED leak-velocity trip (Step 1d) BEFORE the possibly-throwing evict
  // loop below, for the same reason aging escalations are logged first
  // above: these pids were deliberately excluded from `targets` entirely
  // (Step 4), so nothing in the Step 6 loop will ever log them — without this
  // loop a throttled trip would be silently unrecorded, defeating the whole
  // point of the throttle (visibility for tuning, not silent suppression).
  for (const pid of throttledPids) {
    const throttledTrip = tripByPid.get(pid);
    throttledTrip.evicted = false;
    throttledTrip.outcome = 'leak-velocity-throttled';
    const logResult = await logWatchdogTrip(
      liveLogFilePath,
      {
        pid: throttledTrip.pid,
        orchestratorId: orchestratorIdByPid.get(pid) ?? orchestratorId,
        outcome: 'leak-velocity-throttled',
        reason: 'leak-velocity',
        growthRateMbPerMin: throttledTrip.growthRateMbPerMin,
      },
      { now },
    );
    assertLogWritten(logResult, 'a leak-velocity-throttled watchdog-trip entry');
  }

  // Step 6 — log EVERY resolved target (`evict`, `alert-only`, AND `skip`
  // alike), not only the ones that actually reach `handleEvict`.
  //
  // pre-PR review, High — `alert-only` is the conservative DEFAULT
  // outcome (any agent class other than a `resumable-workflow --resume=` command
  // routes here; see "Re-entrancy policy" in SKILL.md), so the dominant
  // real-world case used to detect a stall and log NOTHING — this beat's
  // liveness NDJSON log is the only durable delivery mechanism an unattended
  // watchdog has, so a silently-unlogged `alert-only`/`skip` outcome is
  // effectively an undelivered detection. Every entry in `targets` (Step 4's
  // FULL `resolveEvictionTargets` output, not just Step 4's `filterEvictable`
  // subset) is now logged here, via the same origin-appropriate logger
  // (watchdog-trip vs deadlock-recycle) `evict` targets already used.
  // — `recycledByIndex` is populated by POSITION (this target's index
  // in `targets`), never by completion order, precisely because the `evict`
  // branch below now runs through a bounded-concurrency POOL: several
  // `handleEvict` calls can be genuinely in flight at once and settle in any
  // order (see the pool loop after this function). Building `recycled` by
  // filtering this array back into position order afterwards is what keeps
  // its "preserves `targets`' original order" contract regardless of
  // resolution order.
  const recycledByIndex = new Array(targets.length);

  async function processTarget(target, index) {
    const originatingTrip = target.pid !== undefined ? tripByPid.get(target.pid) : undefined;
    const isDeadlockOrigin =
      !originatingTrip && target.orchestratorId !== undefined && deadlockStalledIds.has(target.orchestratorId);

    let outcome;
    let reason;
    let recycledIdentifier;

    if (target.action === 'evict') {
      let handleEvictResult;
      try {
        handleEvictResult = await handleEvict(target);
      } catch (error) {
        //, corrected post-review — a marked systemic failure (see
        // `isSystemicEvictFailure`'s own comment above) is NEVER isolated: it
        // must propagate straight out of `runWatchdogBeat` and fail the whole
        // beat loudly, exactly as `watchdog-outer-acceptance.jest.spec.mjs`'s
        // pre-existing regression guard pins, regardless of how many
        // sibling `evict` targets exist in this beat.
        if (isSystemicEvictFailure(error)) throw error;
        // Any other (ordinary, per-target) rejection is isolated to THIS
        // target's own outcome and must never propagate out of
        // `runWatchdogBeat`, taking down every other (possibly already
        // concurrently-settled) target's logging with it. Treated
        // identically to `classifyEvictionOutcome`'s own documented
        // "malformed/no result" fallback (`{ recycled: false, reason:
        // 'unknown' }`) — a real crash and a structurally-invalid result are
        // indistinguishable failure shapes from this beat's point of view,
        // and `resolveOutcome`/`classifyEvictionOutcome` already know how to
        // turn `undefined` into that conservative outcome without any
        // further special-casing here.
        handleEvictResult = undefined;
      }
      const classified = classifyEvictionOutcome(handleEvictResult);
      outcome = resolveOutcome(handleEvictResult, classified);
      reason = classified.recycled ? undefined : classified.reason;
      // pre-PR review, Medium — always the real, killed OS pid
      // (`target.pid`), never `target.orchestratorId`, on EITHER origin: a
      // deadlock-origin `claimLedger` record still carries a genuine `pid`
      // (it is matched BY `orchestratorId`, but the record itself always
      // names a real os pid — see `buildResolvedTarget`), so the process
      // actually killed must always be identifiable from this beat's own
      // JSON output.
      if (classified.recycled === true) recycledIdentifier = target.pid;
    } else {
      // `alert-only` / `skip` — never attempted, so there is no
      // `handleEvict` result to classify; the resolved `action` itself IS
      // the outcome, and any `skipReason` IS the reason.
      outcome = target.action;
      reason = target.skipReason;
    }

    if (originatingTrip) {
      // pre-PR review, Medium — stamp `evicted`/`outcome` on every
      // trip this target resolved from, not only the ones that were
      // actually evicted, so an `alert-only` trip is distinguishable in the
      // `trips` output array from a trip Step 4/5 never got to at all.
      //
      // — this stamps `originatingTrip` (a shared object looked up by
      // PID via `tripByPid`), never by loop position, so it stays correct
      // no matter which order concurrently-pooled targets settle in.
      originatingTrip.evicted = recycledIdentifier !== undefined;
      originatingTrip.outcome = outcome;
      // pre-PR review, Medium — see the aging-escalation log call
      // above for why `now` is threaded through explicitly here too.
      //
      // Phase 2 (stability review Medium 1 + 2) —
      // `growthRateMbPerMin` and `corroboratingSignals` are threaded straight
      // off `originatingTrip` (whichever of the two Step-1c shapes it is:
      // a plain leak-velocity trip carries `growthRateMbPerMin` directly, a
      // dual-trip 'stalled' winner carries `corroboratingSignals` instead —
      // see `liveness-log.mjs`'s own `logWatchdogTrip` doc comment for how
      // `buildDetail` drops whichever of the two is absent). A plain
      // `stalled` trip with no corroboration has neither field, so both are
      // simply `undefined` and omitted from the persisted entry.
      const logResult = await logWatchdogTrip(
        liveLogFilePath,
        {
          pid: target.pid,
          orchestratorId: target.orchestratorId,
          leaseId: target.leaseId,
          outcome,
          reason,
          growthRateMbPerMin: originatingTrip.growthRateMbPerMin,
          corroboratingSignals: originatingTrip.corroboratingSignals,
        },
        { now },
      );
      assertLogWritten(logResult, 'a watchdog-trip entry');
      recycledByIndex[index] = recycledIdentifier;
    } else if (isDeadlockOrigin) {
      const logResult = await logDeadlockRecycle(
        liveLogFilePath,
        {
          pid: target.pid,
          orchestratorId: target.orchestratorId,
          outcome,
          reason,
        },
        { now },
      );
      assertLogWritten(logResult, 'a deadlock-recycle entry');
      recycledByIndex[index] = recycledIdentifier;
    } else {
      // Neither a Step-1 watchdog-trip pid nor a Step-2 deadlock-trip
      // stalledPid — e.g. a `skip` target for which `resolveEvictionTargets`
      // could not resolve ANY claimLedger record at all (no real pid, no
      // real orchestratorId). Should be rare, but fails closed (logged as a
      // watchdog trip, the more conservative of the two shapes) rather than
      // silently dropping the detection.
      const logResult = await logWatchdogTrip(
        liveLogFilePath,
        {
          pid: target.pid,
          orchestratorId: target.orchestratorId,
          leaseId: target.leaseId,
          outcome,
          reason,
        },
        { now },
      );
      assertLogWritten(logResult, 'a watchdog-trip entry');
      recycledByIndex[index] = recycledIdentifier;
    }
  }

  // — genuine bounded-concurrency POOL for `evict` targets, capped at
  // `DEFAULT_MAX_CONCURRENT_EVICTIONS_PER_BEAT` simultaneous in-flight
  // `handleEvict` calls. Deliberately NOT "chunk `targets` into groups of the
  // cap and `Promise.all` each chunk in turn" — that batching disguise would
  // let one permanently-hung target block every later target in its own
  // chunk (and all subsequent chunks) forever. Instead: admit a target the
  // moment a slot is free, by racing only the currently in-flight set. A
  // freed slot is admitted to the NEXT queued evict target immediately, not
  // once a whole batch completes.
  //
  // `alert-only`/`skip` targets never call `handleEvict` at all, so they are
  // simply awaited inline in `targets`' own order — no pool bookkeeping is
  // needed (or observable) on that path, keeping the zero/single-target and
  // no-eviction cases identical to the pre-existing sequential behaviour.
  //
  // post-review, Critical — `performEviction` (`cli.mjs`) is NOT safe
  // to run concurrently for two targets sharing the same `orchestratorId`:
  // it does a read-then-conditionally-write round trip against that
  // orchestrator's OWN dibs-ledger entry (`readDibs` an `existingSelf`, then
  // a full-entry-REPLACE `declareDibs`, gated on what the read observed —
  // see `performEviction`'s own comments on `orchestratorIsLive` and the
  // `orchestrator-pid-mismatch` fail-safe). Two targets for the same
  // orchestrator racing through that read/write independently can each read
  // the SAME pre-write ledger state and then stomp each other's write, or
  // have the second target's read miss the first target's write entirely —
  // both non-deterministic outcomes depending purely on scheduling, and
  // exactly the failure this fix's own regression guard
  // (`watchdog-outer-acceptance.jest.spec.mjs`'s Scenario 5 test) caught: it
  // pins that a later same-orchestrator target's `readDibs` must observe an
  // earlier one's `declareDibs` write, which only holds if they run
  // strictly in order. `chainByOrchestratorId` below serializes same-
  // orchestratorId targets onto one promise chain (each new target's worker
  // waits for its predecessor to fully settle before starting) while still
  // letting DIFFERENT orchestrators' targets run genuinely concurrently in
  // the pool, preserving this fix's actual goal (bounding cross-orchestrator
  // beat latency) without reopening a ledger race within one orchestrator.
  const inFlight = new Set();
  const chainByOrchestratorId = new Map();
  try {
    for (let index = 0; index < targets.length; index += 1) {
      const target = targets[index];
      if (target.action !== 'evict') {
        // eslint-disable-next-line no-await-in-loop -- intentionally
        // sequential for the non-evict path; see comment above.
        await processTarget(target, index);
        continue;
      }

      if (inFlight.size >= DEFAULT_MAX_CONCURRENT_EVICTIONS_PER_BEAT) {
        // eslint-disable-next-line no-await-in-loop -- this IS the pool's
        // admission control: block only until ONE in-flight slot frees up,
        // never until a whole batch does.
        await Promise.race(inFlight);
      }

      const orchestratorKey = target.orchestratorId;
      const priorChain = orchestratorKey !== undefined ? chainByOrchestratorId.get(orchestratorKey) : undefined;
      // `priorChain` is awaited via `.then`, never `.catch`/`.finally`, so a
      // systemic (`armFailBeat`-marked) rejection from an earlier same-
      // orchestrator target propagates straight through without this target
      // ever calling `processTarget` — correct, since a systemic failure
      // must already fail the whole beat (see `isSystemicEvictFailure`
      // above), not attempt further evictions for that orchestrator. An
      // ORDINARY per-target failure never reaches here as a rejection at
      // all — `processTarget`'s own try/catch isolates it into a resolved
      // outcome — so the chain only ever short-circuits on a genuinely
      // systemic error.
      const worker = (priorChain ?? Promise.resolve())
        .then(() => processTarget(target, index))
        .finally(() => inFlight.delete(worker));
      if (orchestratorKey !== undefined) chainByOrchestratorId.set(orchestratorKey, worker);
      inFlight.add(worker);
    }
    await Promise.all(inFlight);
  } catch (error) {
    // post-review, Critical — a systemic `handleEvict` rejection (or
    // any other throw out of `processTarget`) must NOT abandon sibling
    // `evict` targets that are still genuinely in flight in the pool at the
    // moment this one rethrows. `cli.mjs`'s caller responds to a thrown beat
    // by calling `process.exit(2)` synchronously with no drain step of its
    // own, so if we rethrow immediately here, any sibling mid-`gracefulStop`
    // (SIGTERM sent, waiting on exit, about to escalate to SIGKILL) or
    // mid-lease-release gets killed mid-flight and its already-authorized
    // eviction silently never completes. Draining every still-outstanding
    // worker via `Promise.allSettled` — never `Promise.all`, which would
    // itself throw on the first further rejection and defeat the drain —
    // guarantees every concurrently-running kill/release sequence actually
    // finishes before this function's rejection reaches `cli.mjs` and the
    // process exits. The original systemic error is what's surfaced; any
    // sibling rejections uncovered during the drain are intentionally
    // swallowed here, exactly as they would have been by their own
    // `processTarget` try/catch had they settled before this one did.
    await Promise.allSettled(inFlight);
    throw error;
  }

  for (const recycledIdentifier of recycledByIndex) {
    if (recycledIdentifier !== undefined) recycled.push(recycledIdentifier);
  }

  return {
    type: 'watchdog',
    trips,
    deadlockTripped: deadlockTrip?.trip === true,
    agingEscalations,
    recycled,
  };
}
