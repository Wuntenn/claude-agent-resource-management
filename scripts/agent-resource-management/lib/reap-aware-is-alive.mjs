// Phase 2 — bounded retry/backoff wrapper that gives the OS a brief
// window to reap a just-exited process before trusting a `true` ("still
// alive") reading from a liveness probe such as `real-evict-adapters.mjs`'s
// `realEvictIsAlive` (its `process.kill(pid, 0)` check cannot distinguish a
// running process from an unreaped zombie — see that module's own doc
// comment and issue its Investigation). See
// `reap-aware-is-alive.jest.spec.mjs` for the full contract.

/**
 * Illustrative starting-hypothesis constants for the reap-retry budget, not
 * measured/tuned — same "starting hypothesis, not settled" posture as
 * `graceful-stop.mjs`'s `DEFAULT_GRACE_PERIOD_MS` and
 * `coordination-file.mjs`'s `LOCK_RETRY_DELAY_MS` / `LOCK_RETRY_JITTER_MS`.
 * Reaping an exited process is typically sub-millisecond-to-a-few-ms on
 * macOS/Linux, so a handful of short probes is expected to cover the vast
 * majority of "zombie momentarily, then reaped" cases while keeping the
 * worst-case total well under `DEFAULT_GRACE_PERIOD_MS` (60s) — this retry
 * window must never materially lengthen `gracefulStop`'s own timing budget.
 */
export const REAP_RETRY_DELAY_MS = 5;
export const REAP_RETRY_JITTER_MS = 5;
export const REAP_MAX_ATTEMPTS = 4;

/**
 * Build a bounded-retry wrapper around a liveness probe (`isAlive`) that
 * gives the OS a brief window to reap a just-exited process before trusting
 * a `true` ("still alive") reading.
 *
 * Retry loop: probe `isAlive(pid)`. If it reports "gone" (`false`), resolve
 * `false` immediately — reaped, no further probes, no trailing sleep. If it
 * reports "still alive" (`true`) and attempts remain, `await sleep(delayMs +
 * jitter)` and probe again. Once `maxAttempts` probes have all reported
 * "still alive", resolve `true` (the last real observation) rather than
 * hanging or retrying unboundedly.
 *
 * Fails LOUD, not open (mirrors `gracefulStop`'s `assertKnownKillResult`
 * posture and this module's sibling `coordination-file.mjs` conventions): an
 * `isAlive` throw or a `sleep` rejection propagates immediately, never
 * swallowed into a false "still alive" retry.
 *
 * @param {object} opts
 * @param {(pid: number) => boolean | Promise<boolean>} opts.isAlive - The
 *   probe to retry. May be sync or async.
 * @param {(ms: number) => Promise<void>} opts.sleep - Injected delay; never
 *   a bare `setTimeout` internally, mirroring `gracefulStop`'s and
 *   `coordination-file.mjs`'s own injected-sleep convention.
 * @param {number} [opts.maxAttempts] - Total number of `isAlive` probes
 *   attempted. Must be >= 1. Defaults to `REAP_MAX_ATTEMPTS`.
 * @param {number} [opts.delayMs] - Fixed delay between probes, in
 *   milliseconds. Must be >= 0. Defaults to `REAP_RETRY_DELAY_MS`.
 * @param {number} [opts.jitterMs] - Additional random jitter added on top of
 *   `delayMs` for each retry (never accumulated/exponential). Defaults to
 *   `REAP_RETRY_JITTER_MS`.
 * @returns {(pid: number) => Promise<boolean>} A liveness probe with the
 *   same external shape as `isAlive`, but retried with backoff.
 */
export function reapAwareIsAlive({
  isAlive,
  sleep,
  maxAttempts = REAP_MAX_ATTEMPTS,
  delayMs = REAP_RETRY_DELAY_MS,
  jitterMs = REAP_RETRY_JITTER_MS,
}) {
  if (maxAttempts <= 0) {
    throw new Error(`reapAwareIsAlive: maxAttempts must be >= 1, got ${maxAttempts}`);
  }
  if (delayMs < 0) {
    throw new Error(`reapAwareIsAlive: delayMs must be >= 0, got ${delayMs}`);
  }

  return async function wrappedIsAlive(pid) {
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      const stillAlive = await isAlive(pid);
      if (!stillAlive) {
        return false;
      }
      if (attempt < maxAttempts) {
        await sleep(delayMs + Math.random() * jitterMs);
      }
    }
    return true;
  };
}
