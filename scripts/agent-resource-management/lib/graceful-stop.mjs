// Phase 3 — graceful-stop mechanism (SIGTERM -> grace period ->
// SIGKILL-if-still-alive). See graceful-stop.jest.spec.mjs for the full
// contract and the design rationale behind each decision below.

/**
 * Illustrative starting hypothesis for how long to wait after SIGTERM before
 * escalating to SIGKILL, not a measured/tuned value — same "starting
 * hypothesis, not settled" posture as every other DEFAULT_* constant in
 * `cli.mjs` (e.g. `DEFAULT_HYSTERESIS_CONFIG`, `LIVE_AGENT_CEILING`). 60
 * seconds is a round, generous default that gives a well-behaved agent
 * process time to flush and exit cleanly; callers with tighter latency
 * requirements should pass an explicit `graceMs`.
 */
export const DEFAULT_GRACE_PERIOD_MS = 60_000;

const KNOWN_KILL_RESULTS = ['ok', 'already-gone', 'permission-denied'];

/**
 * Assert that a `kill()` result is one of the known literals. Throws rather
 * than letting an unrecognized value silently fall through to being treated
 * as `'ok'` — see the fail-LOUD-not-open rationale on `gracefulStop` below.
 *
 * @param {string} result - The raw value returned by the injected `kill`.
 * @param {number} pid - Target process id, included in the error for
 *   diagnosability.
 * @param {'SIGTERM' | 'SIGKILL'} signal - Which call site produced `result`.
 */
function assertKnownKillResult(result, pid, signal) {
  if (!KNOWN_KILL_RESULTS.includes(result)) {
    throw new Error(
      `gracefulStop: kill(${pid}, '${signal}') returned unrecognized result ` +
        `${JSON.stringify(result)}; expected one of ${JSON.stringify(KNOWN_KILL_RESULTS)}. ` +
        'Refusing to silently treat this as success.',
    );
  }
}

/**
 * Stop a process gracefully: send SIGTERM, wait out a grace period, and only
 * escalate to SIGKILL if the process is still alive afterward.
 *
 * Pure orchestration over fully injected side effects — `kill`, `isAlive`,
 * and `sleep` are supplied by the caller, so this function never itself
 * calls `process.kill`, `setTimeout`, or `Date.now()`. This keeps the
 * decision logic testable without touching a real OS process or a real
 * clock (mirrors `probe-bound.mjs`'s "pure decision + injected side
 * effects" posture and `allowance.mjs`'s `consumeSpawnTokens` convention).
 *
 * No internal serialization across concurrent calls: this function holds no
 * shared state, so two concurrent calls against the same `pid` each run
 * their own independent SIGTERM -> wait -> SIGKILL sequence, with neither
 * call's SIGKILL depending on liveness data from the other call. Callers
 * that need mutual exclusion across concurrent eviction attempts against
 * the same PID must provide it externally — e.g. via the lock primitive in
 * `coordination-file.mjs`.
 *
 * `kill()`'s result is checked exhaustively against the full known-literal
 * union at both call sites (SIGTERM and SIGKILL). This function is the trust
 * boundary Phase 4 wires the real `process.kill` adapter into unchanged — an
 * adapter bug that returns an unmapped/misspelled result must never be
 * silently treated as success (fail LOUD, not open), since that would let
 * `gracefulStop` report `'killed-after-grace'` while the real process is
 * still running. Any `kill()` result outside `'ok' | 'already-gone' |
 * 'permission-denied'` throws immediately rather than falling through.
 *
 * @param {object} opts
 * @param {number} opts.pid - Target process id.
 * @param {(pid: number, signal: 'SIGTERM' | 'SIGKILL') =>
 *   'ok' | 'already-gone' | 'permission-denied'} opts.kill - Injected signal
 *   sender. Returns a literal outcome string rather than throwing, so
 *   ESRCH/EPERM-equivalent conditions are expressed as data. Any other
 *   returned value is treated as an adapter contract violation and throws
 *   (see above).
 * @param {(pid: number) => boolean | Promise<boolean>} opts.isAlive - Injected
 *   liveness check. May be sync or async; both call sites below `await` it.
 * @param {(ms: number) => Promise<void>} opts.sleep - Injected delay; the
 *   only source of "the grace period has elapsed".
 * @param {number} [opts.graceMs] - Milliseconds to wait after SIGTERM before
 *   checking liveness again. Defaults to `DEFAULT_GRACE_PERIOD_MS`.
 * @param {() => boolean | Promise<boolean>} [opts.verifyStillTarget] -
 *   Optional re-authorization check, invoked immediately before the SIGKILL
 *   escalation (i.e. after the grace-period sleep confirms the pid is still
 *   alive). review, High — the authorization proof callers compute
 *   (e.g. `isEvictionAuthorized`) is taken from a ps snapshot BEFORE this
 *   function runs; `graceMs` (default 60s) is then awaited before SIGKILL is
 *   ever sent. If the original target exits and the OS recycles its pid
 *   during that window — realistic on a host that deliberately churns many
 *   short-lived agents — `isAlive(pid)` returning `true` proves only that
 *   SOME process now holds that pid, not that it is still the authorized
 *   target; sending SIGKILL to a bare integer at that point can hit an
 *   unrelated, unauthorized process. When provided, this callback is
 *   consulted right before the SIGKILL call and, if it resolves `false`, the
 *   escalation is aborted (no SIGKILL sent) and the outcome is reported as
 *   `'target-changed-before-kill'` rather than a confirmed-stopped result.
 *   Omitting it preserves the exact prior behavior (no re-check) for
 *   backward compatibility.
 * @returns {Promise<{
 *   outcome: 'exited-before-grace' | 'killed-after-grace' | 'already-gone'
 *          | 'permission-denied' | 'unconfirmed-after-kill'
 *          | 'target-changed-before-kill',
 *   signalsSent: Array<'SIGTERM' | 'SIGKILL'>,
 * }>}
 */
export async function gracefulStop({
  pid,
  kill,
  isAlive,
  sleep,
  graceMs = DEFAULT_GRACE_PERIOD_MS,
  verifyStillTarget,
}) {
  const signalsSent = [];

  const termResult = kill(pid, 'SIGTERM');
  signalsSent.push('SIGTERM');
  assertKnownKillResult(termResult, pid, 'SIGTERM');

  if (termResult === 'already-gone') {
    return { outcome: 'already-gone', signalsSent };
  }
  if (termResult === 'permission-denied') {
    return { outcome: 'permission-denied', signalsSent };
  }

  await sleep(graceMs);

  if (!(await isAlive(pid))) {
    return { outcome: 'exited-before-grace', signalsSent };
  }

  if (verifyStillTarget !== undefined && !(await verifyStillTarget())) {
    return { outcome: 'target-changed-before-kill', signalsSent };
  }

  const killResult = kill(pid, 'SIGKILL');
  signalsSent.push('SIGKILL');
  assertKnownKillResult(killResult, pid, 'SIGKILL');

  if (killResult === 'already-gone') {
    return { outcome: 'already-gone', signalsSent };
  }
  if (killResult === 'permission-denied') {
    return { outcome: 'permission-denied', signalsSent };
  }

  if (await isAlive(pid)) {
    return { outcome: 'unconfirmed-after-kill', signalsSent };
  }

  return { outcome: 'killed-after-grace', signalsSent };
}
