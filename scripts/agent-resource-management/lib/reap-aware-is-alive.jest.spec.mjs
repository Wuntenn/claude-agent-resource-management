// Phase 2 — failing tests for the bounded retry/backoff wrapper that
// gives the OS a brief window to reap a just-exited process before trusting
// a `true` ("still alive") reading from `realEvictIsAlive`
// (./real-evict-adapters.mjs's `kill(pid, 0)` probe cannot distinguish a
// running process from an unreaped zombie — see that module's own doc
// comment and issue its Investigation).
//
// RED by construction: `./reap-aware-is-alive.mjs` does not exist yet.
//
// Contract this file pins for `typescript-implementer` (per its
// Build Plan Phase 2 + Convergence Analysis edge cases 1-4, 8):
//
//   reapAwareIsAlive({ isAlive, sleep, maxAttempts, delayMs, jitterMs })
//     -> (pid: number) => Promise<boolean>
//
//   - `isAlive(pid)` — the probe to retry. May be sync or async (mirrors
// Phase 1's widened `gracefulStop` `isAlive` type). Composed here
//     around `realEvictIsAlive` in `cli.mjs`'s `buildEvictIsAliveFn`, never
//     around the `ARM_FAKE_IS_ALIVE_SEQUENCE` fake path.
//   - `sleep(ms)` -> Promise<void> — injected delay, exactly like
//     `gracefulStop`'s own `sleep` parameter and `coordination-file.mjs`'s
//     `acquireLock`. Never a bare `setTimeout` internally — a real async
//     sleep is unacceptable to wait out in a unit test.
//   - `maxAttempts` — total number of `isAlive` probes attempted (>= 1).
//   - `delayMs` / `jitterMs` — mirror `coordination-file.mjs`'s
//     `LOCK_RETRY_DELAY_MS` / `LOCK_RETRY_JITTER_MS` naming and shape
//     exactly (a small fixed delay plus a small random jitter added on top,
//     summoned once per retry, never accumulated/exponential — this is host
//     tooling coordinating a handful of local processes, not a distributed
//     system).
//
// Retry loop shape: probe `isAlive(pid)`; if it reports "gone" (`false`),
// resolve `false` immediately (reaped — no further probes, no trailing
// sleep). If it reports "still alive" (`true`) and attempts remain, sleep
// `delayMs (+ jitterMs)` and probe again. Once `maxAttempts` probes have all
// reported "still alive", resolve `true` (the last real observation) rather
// than hanging or retrying unboundedly.
//
// Fail LOUD, not open (mirrors `gracefulStop`'s own `assertKnownKillResult`
// posture and this module's sibling `coordination-file.mjs` conventions):
// an `isAlive` throw or a `sleep` rejection propagates immediately, never
// swallowed into a false "still alive" retry.

import { jest } from '@jest/globals';

import {
  reapAwareIsAlive,
  REAP_RETRY_DELAY_MS,
  REAP_RETRY_JITTER_MS,
  REAP_MAX_ATTEMPTS,
} from './reap-aware-is-alive.mjs';
import { DEFAULT_GRACE_PERIOD_MS } from './graceful-stop.mjs';

/**
 * Builds a fake `sleep` that resolves immediately (no real timer) while
 * recording every requested duration — mirrors
 * `graceful-stop.jest.spec.mjs`'s own `fakeSleep` helper exactly.
 */
function fakeSleep(durations = []) {
  return jest.fn(async (ms) => {
    durations.push(ms);
  });
}

describe('reapAwareIsAlive', () => {
  it('1. a fake liveness sequence reporting zombie (true) then reaped (false) within the retry budget resolves to false', async () => {
    const isAlive = jest
      .fn()
      .mockReturnValueOnce(true)
      .mockReturnValueOnce(true)
      .mockReturnValueOnce(false);
    const sleep = fakeSleep();

    const wrapped = reapAwareIsAlive({ isAlive, sleep, maxAttempts: 5, delayMs: 5, jitterMs: 5 });
    const result = await wrapped(4242);

    expect(result).toBe(false);
    expect(isAlive).toHaveBeenCalledTimes(3);
    expect(isAlive).toHaveBeenNthCalledWith(1, 4242);
    // Reaped — no further probes, no trailing sleep after the final probe.
    expect(sleep).toHaveBeenCalledTimes(2);
  });

  it('2. a fake liveness sequence that never reaps within the retry budget eventually resolves to true (last observed), not hanging', async () => {
    const isAlive = jest.fn(() => true);
    const sleep = fakeSleep();

    const wrapped = reapAwareIsAlive({ isAlive, sleep, maxAttempts: 4, delayMs: 5, jitterMs: 5 });
    const result = await wrapped(5555);

    expect(result).toBe(true);
    // Bounded: exactly `maxAttempts` probes, never more.
    expect(isAlive).toHaveBeenCalledTimes(4);
    // One sleep between each pair of attempts, never a trailing sleep after
    // the last (already-final) probe.
    expect(sleep).toHaveBeenCalledTimes(3);
  });

  it('3. zombie reaping exactly on the LAST permitted attempt (off-by-one boundary) still resolves correctly', async () => {
    const isAlive = jest
      .fn()
      .mockReturnValueOnce(true)
      .mockReturnValueOnce(true)
      .mockReturnValueOnce(false);
    const sleep = fakeSleep();

    // maxAttempts is exactly 3 — the reap happens on the LAST permitted
    // probe, not before the budget is exhausted and not one past it.
    const wrapped = reapAwareIsAlive({ isAlive, sleep, maxAttempts: 3, delayMs: 5, jitterMs: 5 });
    const result = await wrapped(6001);

    expect(result).toBe(false);
    expect(isAlive).toHaveBeenCalledTimes(3);
    expect(sleep).toHaveBeenCalledTimes(2);
  });

  it("4. the injected sleep is called with a bounded, finite delay between attempts (not a busy-wait)", async () => {
    const isAlive = jest.fn(() => true);
    const durations = [];
    const sleep = fakeSleep(durations);

    const wrapped = reapAwareIsAlive({ isAlive, sleep, maxAttempts: 3, delayMs: 10, jitterMs: 10 });
    await wrapped(7001);

    expect(durations).toHaveLength(2);
    for (const ms of durations) {
      expect(Number.isFinite(ms)).toBe(true);
      expect(ms).toBeGreaterThanOrEqual(10);
      expect(ms).toBeLessThanOrEqual(20);
    }
  });

  it('5. if isAlive itself throws (a genuine adapter error, not a zombie/reap signal), the wrapper propagates the throw immediately, not swallowed into a retry', async () => {
    const adapterError = new Error('EPERM-like adapter failure');
    const isAlive = jest.fn(() => {
      throw adapterError;
    });
    const sleep = fakeSleep();

    const wrapped = reapAwareIsAlive({ isAlive, sleep, maxAttempts: 5, delayMs: 5, jitterMs: 5 });

    await expect(wrapped(8001)).rejects.toBe(adapterError);
    // No retry attempted around a genuine adapter error.
    expect(isAlive).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
  });

  it('6. if the injected sleep rejects, the wrapper propagates that rejection rather than treating it as "still alive"', async () => {
    const sleepError = new Error('sleep adapter failure');
    const isAlive = jest.fn(() => true);
    const sleep = jest.fn(async () => {
      throw sleepError;
    });

    const wrapped = reapAwareIsAlive({ isAlive, sleep, maxAttempts: 5, delayMs: 5, jitterMs: 5 });

    await expect(wrapped(9001)).rejects.toBe(sleepError);
    expect(isAlive).toHaveBeenCalledTimes(1);
    expect(sleep).toHaveBeenCalledTimes(1);
  });

  it('7a. maxAttempts <= 0 fails loud rather than silently no-retrying', () => {
    const isAlive = jest.fn(() => true);
    const sleep = fakeSleep();

    expect(() => reapAwareIsAlive({ isAlive, sleep, maxAttempts: 0, delayMs: 5, jitterMs: 5 })).toThrow();
    expect(() =>
      reapAwareIsAlive({ isAlive, sleep, maxAttempts: -1, delayMs: 5, jitterMs: 5 }),
    ).toThrow();
  });

  it('7b. negative delayMs fails loud rather than being silently clamped', () => {
    const isAlive = jest.fn(() => true);
    const sleep = fakeSleep();

    expect(() =>
      reapAwareIsAlive({ isAlive, sleep, maxAttempts: 3, delayMs: -5, jitterMs: 5 }),
    ).toThrow();
  });

  it('8. the total worst-case retry budget (maxAttempts * (delayMs + jitterMs)) stays well under DEFAULT_GRACE_PERIOD_MS', () => {
    // Illustrative starting-hypothesis constants (same "not measured/tuned"
    // posture as `DEFAULT_GRACE_PERIOD_MS` and `LOCK_RETRY_DELAY_MS` /
    // `LOCK_RETRY_JITTER_MS`) — this is a numeric relationship the module
    // must hold, not an eyeballed "short" in prose.
    const worstCaseMs = REAP_MAX_ATTEMPTS * (REAP_RETRY_DELAY_MS + REAP_RETRY_JITTER_MS);

    expect(worstCaseMs).toBeGreaterThan(0);
    // Well under the grace period — a fraction of it, not merely less than
    // it, so the retry window never materially lengthens `gracefulStop`'s
    // own timing budget.
    expect(worstCaseMs).toBeLessThan(DEFAULT_GRACE_PERIOD_MS / 10);
  });
});
