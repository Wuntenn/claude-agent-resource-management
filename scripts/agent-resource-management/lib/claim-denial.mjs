// (review round 2) — making a `--claim` denial NAME ITS OWN CAUSE.
//
// WHAT THIS EXISTS TO PREVENT. `--claim` fails CLOSED: any coordination
// failure yields `{"granted": 0}` and exit 0. That is the deliberate posture
// (staleness fails in the OVER-admission direction, which is the failure mode
// that froze this host), and this module does not soften it. What it fixes is
// the DIAGNOSTIC consequence of that posture: before it, every denial reduced
// to one undifferentiated line, `coordination file unavailable (<code>)`. A
// `DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS` sized too tight for the real concurrency
// would therefore surface to an operator as "claims mysteriously stopped
// working" — indistinguishable from a genuinely busy host, from a permissions
// problem, or from an honest capacity denial, and never pointing at the
// constant that actually caused it.
//
// So each denial cause now names itself, names the constant that governs it,
// and states what to conclude if it RECURS. A mis-sized ceiling announces
// itself instead of hiding inside a generic string.
//
// WHY A LOG LINE AND NOT A PERSISTED COUNTER. The obvious "warned counter" —
// tally denials in the coordination file so recurrence aggregates — is
// structurally impossible on the path that matters most: writing that tally
// requires the very lock whose acquisition just timed out. A counter that
// cannot be incremented in exactly the case it exists to measure is worse
// than none, because its zero reads as "no contention". `--claim` is also a
// ONE-SHOT process, so an in-memory counter dies with the invocation.
//
// The countable artefact is therefore the stable, greppable `error.code`
// token each line carries (`COORDINATION_LOCK_TIMEOUT`,
// `COORDINATION_LOCK_LOST`) — aggregation belongs to whatever collects the
// orchestrator's stderr, which unlike this process survives across beats.
//
// The classification lives HERE, in a coverage-collected `lib/` module,
// rather than inline in `cli.mjs` — `cli.mjs` is excluded from coverage
// collection (see `jest.skills.config.mjs`) and is black-box tested through
// `child_process` only, so branch logic left inline there is gated by
// nothing. Same rule `lib/probe-bound.mjs` was split out under.

import { LOCK_RETRY_DELAY_MS, LOCK_RETRY_JITTER_MS } from './coordination-file.mjs';

/**
 * The wait window a given `acquireLock` attempt ceiling actually buys, in
 * seconds, derived from `acquireLock`'s own retry constants rather than
 * restated. `acquireLock` sleeps `delay + random() * jitter` per attempt, so
 * the sleep total spans `attempts * delay` to `attempts * (delay + jitter)`.
 *
 * Sleep only — the per-attempt `open`/`looksStale` syscalls sit on top, which
 * is why this is reported as the ceiling's NOMINAL window and never as a
 * guarantee. It also assumes the PRODUCTION retry delay: `acquireLock` takes
 * a `lockRetryDelayMs` override, and no production caller passes one (`cli.mjs`
 * passes only `lockMaxAttempts`), so deriving from the module constant is
 * accurate today — a caller that ever overrides the delay would have to pass
 * it here too. It exists so the warning can say "gave up after 800 attempts
 * (~4-8s)" instead of just "800", which is meaningless to whoever is reading
 * stderr at 2am.
 *
 * @param {number} lockMaxAttempts
 * @returns {{ minSeconds: number, maxSeconds: number } | null} null when the ceiling is not a usable number.
 */
export function lockWaitWindowSeconds(lockMaxAttempts) {
  if (!Number.isFinite(lockMaxAttempts) || lockMaxAttempts <= 0) return null;
  return {
    minSeconds: (lockMaxAttempts * LOCK_RETRY_DELAY_MS) / 1000,
    maxSeconds: (lockMaxAttempts * (LOCK_RETRY_DELAY_MS + LOCK_RETRY_JITTER_MS)) / 1000,
  };
}

/** Formats a window as the `~4-8s` fragment the warnings embed. */
function formatWindow(window) {
  if (!window) return null;
  const round = (value) => (Number.isInteger(value) ? String(value) : value.toFixed(1));
  return `~${round(window.minSeconds)}-${round(window.maxSeconds)}s`;
}

/**
 * The default `degradedWhat`/`consequence` pair — byte-for-byte what
 * `cli.mjs`'s `--claim` catch passed before this module existed. Returned
 * unchanged for any error that is NOT one of the two lock outcomes, so this
 * module can only ever ADD specificity, never lose an existing message.
 */
const GENERIC_CLAIM_DENIAL = Object.freeze({
  degradedWhat: 'coordination file',
  consequence: 'granting nothing for this claim',
});

/**
 * Classifies why a `--claim` denied, into the two arguments
 * `warnCoordinationDegraded` already takes. Deliberately shaped to feed that
 * existing house idiom rather than introduce a second warning format — the
 * error code itself is still interpolated by the caller, so every line
 * remains greppable by `error.code` exactly as before.
 *
 * The two lock outcomes are separated because they indict DIFFERENT
 * constants and send a reader to different places:
 *
 *   - `COORDINATION_LOCK_TIMEOUT` — never got the lock. Indicts
 *     `DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS`: real concurrency is outrunning the
 *     ceiling. This is the finding-driving case; see that constant's own
 *     comment for the measured 4-way denial rate the ceiling is sized on.
 *   - `COORDINATION_LOCK_LOST` — HAD the lock and lost it mid-critical-
 *     section. Indicts `DEFAULT_LOCK_STALENESS_MS`: a critical section is
 *     outliving the reclaim horizon, or a contender reclaimed it during
 *     `reclaimStaleLock`'s remove-then-restore window, where
 *     `assertStillHeld` reads a transient ENOENT as loss (an accepted, and
 *     deliberately un-"fixed", false-positive source — the alternative,
 *     treating a missing lock as still-held, fails in the over-admission
 *     direction). Separating it is what makes the two denial sources
 *     countable against each other rather than pooled into one number.
 *
 * @param {{ code?: string, message?: string } | null | undefined} error
 * @param {{ lockMaxAttempts?: number }} [options]
 * @returns {{ degradedWhat: string, consequence: string }} arguments for `warnCoordinationDegraded`.
 */
export function describeClaimDenial(error, { lockMaxAttempts } = {}) {
  const code = error?.code;

  if (code === 'COORDINATION_LOCK_TIMEOUT') {
    const window = formatWindow(lockWaitWindowSeconds(lockMaxAttempts));
    const ceiling = window
      ? `${lockMaxAttempts} attempts, ${window}`
      : `this claim's own attempt ceiling`;
    return {
      degradedWhat: `coordination lock (contended — gave up after ${ceiling})`,
      consequence:
        'granting nothing for this claim; if this recurs, DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS is ' +
        'under-sized for the concurrency actually in use',
    };
  }

  if (code === 'COORDINATION_LOCK_LOST') {
    return {
      degradedWhat: 'coordination lock (acquired, then lost before the write landed)',
      consequence:
        'granting nothing for this claim; if this recurs, a critical section is outliving ' +
        'DEFAULT_LOCK_STALENESS_MS and contenders are reclaiming live locks',
    };
  }

  return GENERIC_CLAIM_DENIAL;
}

/**
 * The `degradedWhat`/`consequence` pair for the THIRD denial source, which
 * until now wrote nothing at all: the hoisted sample going stale between
 * being taken and the grant being decided under the lock
 * (`capacityIfSampleFresh` returning 0).
 *
 * This one is not an error — no exception is thrown, `claimCapacity` resolves
 * normally with `granted: 0` — which is precisely why it was the quietest of
 * the three. Before this, the only externally visible difference between "the
 * host genuinely had no capacity" and "we threw away a perfectly good reading
 * because it aged out" was nothing whatsoever. Both printed `{"granted": 0}`
 * and exited 0 with an empty stderr.
 *
 * The denial itself is deliberate and is NOT being relaxed: a sample that
 * aged past the window over-states free RAM if pressure rose while this
 * caller queued, and over-admission is the failure this whole mechanism
 * exists to prevent. Only its silence is being fixed.
 *
 * Shaped as a NOUN PHRASE with the cause in a parenthetical, like both lock
 * denials above, because `warnCoordinationDegraded` renders it as
 * `${degradedWhat} unavailable (${code})`. Written as a clause instead
 * ("host-resource sample aged out before the lock was acquired") the rendered
 * line reads "... aged out before the lock was acquired unavailable
 * (CLAIM_SAMPLE_STALE)", which is not a sentence — and a diagnostic whose
 * whole justification is being readable at 2am cannot afford that.
 *
 * @param {{ ageMs?: number, freshnessWindowMs?: number }} [context]
 * @returns {{ degradedWhat: string, consequence: string }}
 */
export function describeStaleSampleDenial({ ageMs, freshnessWindowMs } = {}) {
  const measured =
    Number.isFinite(ageMs) && Number.isFinite(freshnessWindowMs)
      ? `, ${ageMs}ms old against a ${freshnessWindowMs}ms window`
      : '';
  return {
    degradedWhat: `host-resource sample (aged out before the lock was acquired${measured})`,
    consequence:
      'granting nothing for this claim; if this recurs, the lock wait is outrunning ' +
      'DEFAULT_CLAIM_SAMPLE_FRESHNESS_MS and the two constants are no longer paired',
  };
}
