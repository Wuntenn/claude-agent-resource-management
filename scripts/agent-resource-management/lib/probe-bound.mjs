// — bounding the host-resource probe's blocking shell-outs.
//
// `cli.mjs`'s `realCollect()` samples the host by shelling out SYNCHRONOUSLY
// (`vm_stat`, `sysctl vm.swapusage`, `df -g /`, `sysctl -n
// kern.memorystatus_vm_pressure_level`), and `collect()` retries the whole
// set once on failure — up to eight blocking subprocess spawns per beat. Any
// one of them hanging (a wedged NFS mount under `df`, a kernel stall on a
// swap-thrashing host) stalls the caller for as long as the child takes.
//
// WHY A PROMISE-BASED TIMEOUT IS THE WRONG TOOL HERE, and what is used
// instead. A `Promise.race` between the probe and a `setTimeout` cannot bound
// `execFileSync` at all: while a synchronous child-process call is running,
// the event loop is blocked, so no timer callback can fire until the call has
// already returned. Racing it detects an overrun after the fact; it never
// preempts one.
//
// Node's `execFileSync` does, however, support `timeout`/`killSignal`
// directly (verified empirically: `execFileSync('sleep', ['5'], {timeout:
// 600})` throws after ~608ms with `code: 'ETIMEDOUT'`, `signal: 'SIGTERM'`).
// None of the probe's call sites passed either option. Passing them is a
// REAL bound — the child is signalled and the call returns — rather than a
// post-hoc observation, which is why this module exists instead of a
// `Promise.race` wrapper.
//
// This module bounds the probe in BOTH dimensions the caller has to care
// about: how long a single shell-out may block (`probeExecOptions`), and how
// stale the resulting sample may be by the time a decision is made against it
// (`capacityIfSampleFresh`). The two are complementary, not redundant, and
// since hoisted the probe out of the claim critical section they are
// the only things standing between a hung or long-past reading and a grant.
//
// The decision logic lives here, in a coverage-collected `lib/` module,
// rather than inline in `cli.mjs` — `cli.mjs` is excluded from coverage
// collection (see `jest.skills.config.mjs`) and is black-box tested through
// `child_process` only, so logic left inline there is gated by nothing.

import { isSampleFresh } from './coordination-file.mjs';

/**
 * Per-shell-out wall-clock ceiling for the host-resource probe.
 *
 * Sized against what the caller does NEXT, not against how long these
 * commands normally take (`vm_stat`/`sysctl`/`df` all return in tens of
 * milliseconds on a healthy host). `collect()` may issue the full
 * four-command set twice — its single bounded retry — so the worst case a
 * caller can experience is 8 x this value: ~16s at 2s each.
 *
 * What this is NO LONGER sized against, and why the change matters:
 * `DEFAULT_LOCK_STALENESS_MS`. Before hoisted the probe out of the
 * claim critical section, a slow probe held the lock and could push its own
 * holder past the reclaim threshold, so the two constants were coupled. They
 * are not any more — the probe runs before the lock is requested. Nor is it
 * charged against the caller's sample-freshness window: `capacityIfSampleFresh`
 * measures staleness from when the sample was TAKEN, so a retried, timed-out
 * probe cannot spend the budget reserved for the lock wait.
 *
 * What it is sized against is the BEAT. `--claim` is a one-shot process an
 * orchestrator runs between sub-agent tasks, on a cadence
 * `.claude/skills/agent-resource-management/SKILL.md` puts at ~15-30s. A
 * ~16s absolute worst case for a completely wedged host keeps a pathological
 * probe inside roughly one beat instead of stalling the orchestrator
 * indefinitely — which is the only thing an unbounded `execFileSync` was
 * ever guaranteed to do.
 */
export const DEFAULT_PROBE_TIMEOUT_MS = 2_000;

/**
 * Thrown when a probe shell-out was killed for exceeding its bound. Follows
 * the `.name` + prefixed `.code` convention `coordination-file.mjs`'s error
 * classes already use, so callers can branch on `.code` rather than
 * string-matching a message.
 *
 * Callers must treat this as a FAIL-CLOSED signal: a probe that did not
 * complete produced no evidence about host capacity, and "no evidence" must
 * never be rounded up into a grant.
 */
export class ProbeTimeoutError extends Error {
  constructor(commandLabel, timeoutMs) {
    super(
      `agent-resource-management: host-resource probe "${commandLabel}" exceeded its ${timeoutMs}ms ` +
        'bound and was killed; treating the sample as unavailable rather than proceeding on a ' +
        'partial reading',
    );
    this.name = 'ProbeTimeoutError';
    this.code = 'ARM_PROBE_TIMEOUT';
  }
}

/**
 * The `execFileSync` options every probe shell-out must be issued with.
 *
 * `killSignal` is stated explicitly rather than left to Node's default (also
 * `SIGTERM`) because `isProbeTimeout` reads it back off the thrown error:
 * pinning it here keeps the two halves of the contract in one place, so a
 * future change to one cannot silently desynchronise the other.
 *
 * @param {number} [timeoutMs]
 * @returns {{ encoding: 'utf8', timeout: number, killSignal: 'SIGTERM' }}
 */
export function probeExecOptions(timeoutMs = DEFAULT_PROBE_TIMEOUT_MS) {
  return { encoding: 'utf8', timeout: timeoutMs, killSignal: 'SIGTERM' };
}

/**
 * Recognises the error `execFileSync` throws when it killed a child for
 * exceeding `timeout`.
 *
 * Both signals are checked, and either alone is sufficient. Node sets
 * `error.code = 'ETIMEDOUT'` on the timeout path, but on some platforms an
 * `error.status`/`error.signal` pair arrives without it; conversely a child
 * that this process itself killed with `SIGTERM` is, from the probe's point
 * of view, exactly the same condition — an incomplete reading. Deliberately
 * NOT narrowed to `code === 'ETIMEDOUT'` alone: a missed classification here
 * would surface as an ordinary probe failure and be retried, re-incurring
 * the very stall the bound exists to cut short.
 *
 * @param {unknown} error
 * @returns {boolean}
 */
export function isProbeTimeout(error) {
  if (!error || typeof error !== 'object') return false;
  return error.code === 'ETIMEDOUT' || error.signal === 'SIGTERM';
}

/**
 * Normalises whatever `execFileSync` threw into the error the caller should
 * propagate: a `ProbeTimeoutError` when the child was killed for overrunning
 * its bound, and otherwise the original error, unchanged and unwrapped.
 *
 * Returning (rather than throwing) keeps the call site's own control flow
 * explicit — `throw asProbeFailure(error, label)` reads as a rethrow, which
 * is what it is.
 *
 * @param {unknown} error
 * @param {string} commandLabel
 * @param {number} [timeoutMs]
 * @returns {unknown}
 */
export function asProbeFailure(error, commandLabel, timeoutMs = DEFAULT_PROBE_TIMEOUT_MS) {
  return isProbeTimeout(error) ? new ProbeTimeoutError(commandLabel, timeoutMs) : error;
}

/**
 * The whole in-lock decision for a hoisted sample, as a pure function.
 *
 * Since the host probe runs before the fencing lock is acquired, so by
 * the time the grant is decided the sample is as old as the probe plus the
 * lock wait. That gap fails in the OVER-admitting direction — a sample taken
 * before pressure rose over-states free RAM — so it is bounded here and fails
 * CLOSED rather than being left implicit.
 *
 * `isSampleFresh` is reused rather than reimplemented so this inherits the
 * coordination module's inclusive-boundary convention: a sample exactly at
 * the window is still fresh, only a strictly older one is stale. That is the
 * same rule `isLockStale`, `pruneStale` and `pruneExpiredClaims` all follow,
 * and divergence here would be invisible without a boundary test.
 *
 * `sampledAt` and `referenceNow` must come from the SAME clock — `cli.mjs`'s
 * `resolveNow()`/`ARM_FAKE_NOW_MS` seam. A real-wall-clock `referenceNow`
 * compared against a faked `sampledAt` reads as permanently stale and denies
 * every grant under test.
 *
 * `onStale` (review round 2) exists because this was the QUIETEST of
 * `--claim`'s three denial sources: it throws nothing, so `claimCapacity`
 * resolves normally and the caller prints `{"granted": 0}` with an empty
 * stderr — externally indistinguishable from a host that genuinely had no
 * capacity. The denial is deliberate and unchanged; only its silence is not.
 *
 * The stale branch is reported from HERE rather than reconstructed by the
 * caller, because the caller cannot reconstruct it: `availableCapacity` may
 * legitimately be 0, so "the returned 0 differs from the sampled value" does
 * not identify staleness in the one corner where both are 0. Keeping the
 * branch in this module also keeps it inside the coverage gate, which
 * `cli.mjs` is excluded from.
 *
 * @param {{ availableCapacity: number, sampledAt: number, referenceNow: number, freshnessWindowMs: number, onStale?: (context: { ageMs: number, freshnessWindowMs: number }) => void }} params
 * @returns {number} the capacity to grant against, or 0 when the sample is too old to trust.
 */
export function capacityIfSampleFresh({
  availableCapacity,
  sampledAt,
  referenceNow,
  freshnessWindowMs,
  onStale,
}) {
  if (isSampleFresh({ sampledAt }, referenceNow, freshnessWindowMs)) return availableCapacity;
  if (typeof onStale === 'function') {
    onStale({ ageMs: referenceNow - sampledAt, freshnessWindowMs });
  }
  return 0;
}
