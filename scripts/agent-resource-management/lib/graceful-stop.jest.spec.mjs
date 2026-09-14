// Phase 3 — failing tests for the graceful-stop mechanism (SIGTERM
// -> grace period -> SIGKILL-if-still-alive).
//
// RED by construction: `./lib/graceful-stop.mjs` does not exist yet. This
// file specifies the contract `typescript-implementer` must satisfy for
// its "Graceful-stop mechanism" phase (see the Build Plan's ## Build
// Plan comment on and its Convergence Analysis edge cases).
//
// Design convention this file assumes (per the ticket's own instructions,
// `probe-bound.mjs`'s "pure decision + injected side effects, no real clock"
// posture, and `allowance.mjs`'s `consumeSpawnTokens` "pure function over an
// injected, explicit clock, never a real setTimeout/Date.now()" posture):
// `gracefulStop` is a pure orchestration function over FULLY INJECTED
// side-effect callbacks. It never itself calls `process.kill`, `setTimeout`,
// or `Date.now()`. This lets every test simulate SIGTERM/SIGKILL, process
// liveness, and time passing without touching a real OS process.
//
// Contract under test:
//
//   gracefulStop({ pid, kill, isAlive, sleep, graceMs = DEFAULT_GRACE_PERIOD_MS })
//     -> Promise<{
//          outcome: 'exited-before-grace' | 'killed-after-grace'
//                 | 'already-gone' | 'permission-denied'
//                 | 'unconfirmed-after-kill',
//          signalsSent: Array<'SIGTERM' | 'SIGKILL'>,
//        }>
//
// Injected dependencies (the "no real side effects inside the unit under
// test" requirement):
//   - `kill(pid, signal)` -> one of the literal strings 'ok' | 'already-gone'
//     | 'permission-denied'. Modeled as a return value rather than a thrown
//     error/errno so a test can express "simulate ESRCH" / "simulate EPERM"
//     directly as data, matching this contract's own vocabulary rather than
//     re-deriving Node's `error.code` shape inside the unit under test.
//   - `isAlive(pid)` -> boolean. Queried by the function to decide whether
//     the grace period ended with the process still alive, and again after
//     SIGKILL to attempt to confirm termination.
//   - `sleep(ms)` -> Promise<void>. The function's only source of "the grace
//     period has elapsed" — it must await exactly this, never a real
//     `setTimeout`, so a test's fake `sleep` can resolve instantly while
//     still proving the requested duration was honoured (asserted via the
//     ms argument each call received).
//
// Design decisions for the two Convergence Analysis edge cases (documented
// here, at the point where the tests encode them, per this ticket's
// instructions):
//
//   Edge case 6 (SIGKILL fails / target becomes an unresponsive zombie —
//   `isAlive` keeps reporting true even after SIGKILL was sent): the
//   function does NOT hang or retry SIGKILL forever. It sends SIGKILL
//   exactly once, checks `isAlive` exactly once more afterward, and if that
//   check still reports "alive" it resolves with the explicit outcome
//   `'unconfirmed-after-kill'` rather than hanging, looping, or throwing an
//   unhandled rejection. The caller decides what (if anything) to do next
//   with an unconfirmed target.
//
//   Edge case 7 (two concurrent eviction attempts against the same PID):
//   `gracefulStop` does NOT serialize or de-duplicate concurrent calls
//   against the same `pid` itself — it has no shared state across calls and
//   documents this as the CALLER's responsibility (e.g. via the coordination
//   file's existing lock primitive, `coordination-file.mjs`). What it DOES
//   guarantee on its own, tested here: within a single call, it never sends
//   SIGKILL without having just sent SIGTERM and waited out the grace period
//   FROM THIS CALL's own invocation — it never "reuses" or assumes a SIGTERM
//   sent by a different, concurrent call. Two overlapping calls each run
//   their own independent SIGTERM -> wait -> re-check -> SIGKILL sequence;
//   neither call's SIGKILL depends on liveness data foreign to its own
//   sequence. This is the safer default given the PID-reuse hazard the
//   ticket calls out: a call never sends a bare, unprefaced SIGKILL based on
//   another call's state.

import { jest } from '@jest/globals';

import { gracefulStop, DEFAULT_GRACE_PERIOD_MS } from './graceful-stop.mjs';

/**
 * Builds a fake `sleep` that resolves immediately (no real timer) while
 * recording every requested duration, so tests can assert the grace period
 * was honoured without spending real wall-clock time.
 */
function fakeSleep(durations = []) {
  const fn = jest.fn(async (ms) => {
    durations.push(ms);
  });
  return fn;
}

describe('DEFAULT_GRACE_PERIOD_MS', () => {
  it('is exported as a tunable constant, illustrative default not a settled/measured value', () => {
    expect(DEFAULT_GRACE_PERIOD_MS).toBe(60_000);
  });
});

describe('gracefulStop', () => {
  it('1. sends SIGTERM immediately on invocation', async () => {
    const kill = jest.fn(() => 'ok');
    const isAlive = jest.fn(() => false);
    const sleep = fakeSleep();

    await gracefulStop({ pid: 4242, kill, isAlive, sleep });

    expect(kill).toHaveBeenCalledWith(4242, 'SIGTERM');
    // SIGTERM must be the very first side effect performed.
    expect(kill.mock.invocationCallOrder[0]).toBeLessThan(
      sleep.mock.invocationCallOrder[0] ?? Infinity,
    );
  });

  it('2. does NOT send SIGKILL if the process is reported exited before the grace period elapses', async () => {
    const kill = jest.fn(() => 'ok');
    // Process is already gone by the time the function checks liveness.
    const isAlive = jest.fn(() => false);
    const sleep = fakeSleep();

    const result = await gracefulStop({ pid: 100, kill, isAlive, sleep });

    expect(kill).toHaveBeenCalledTimes(1);
    expect(kill).toHaveBeenCalledWith(100, 'SIGTERM');
    expect(kill).not.toHaveBeenCalledWith(100, 'SIGKILL');
    expect(result.outcome).toBe('exited-before-grace');
    expect(result.signalsSent).toEqual(['SIGTERM']);
  });

  it('3. sends SIGKILL only after the full grace period has elapsed AND the process is confirmed still alive', async () => {
    const kill = jest.fn(() => 'ok');
    // Still alive at the pre-SIGKILL check (justifying escalation), confirmed
    // dead at the post-SIGKILL check (confirming the kill succeeded) — this
    // is the "clean success" path, distinct from test 6's "stays alive even
    // after SIGKILL" (unconfirmed) path, which uses an always-true isAlive.
    const isAlive = jest.fn().mockReturnValueOnce(true).mockReturnValue(false);
    const durations = [];
    const sleep = fakeSleep(durations);

    const result = await gracefulStop({ pid: 200, kill, isAlive, sleep, graceMs: 60_000 });

    expect(kill).toHaveBeenNthCalledWith(1, 200, 'SIGTERM');
    expect(sleep).toHaveBeenCalledWith(60_000);
    expect(kill).toHaveBeenNthCalledWith(2, 200, 'SIGKILL');
    expect(result.outcome).toBe('killed-after-grace');
    expect(result.signalsSent).toEqual(['SIGTERM', 'SIGKILL']);
    // SIGKILL must only occur AFTER sleep resolved.
    expect(sleep.mock.invocationCallOrder[0]).toBeLessThan(kill.mock.invocationCallOrder[1]);
  });

  it('4. treats an injected "already gone" (ESRCH-equivalent) signal as success, not a thrown failure', async () => {
    const kill = jest.fn(() => 'already-gone');
    const isAlive = jest.fn(() => false);
    const sleep = fakeSleep();

    await expect(gracefulStop({ pid: 300, kill, isAlive, sleep })).resolves.toEqual(
      expect.objectContaining({ outcome: 'already-gone' }),
    );
  });

  it('5. surfaces an injected "permission denied" (EPERM-equivalent) signal as an explicit failure', async () => {
    const kill = jest.fn(() => 'permission-denied');
    const isAlive = jest.fn(() => true);
    const sleep = fakeSleep();

    const result = await gracefulStop({ pid: 400, kill, isAlive, sleep });

    // Explicit failure result, never silently swallowed into a
    // success-shaped outcome, and never a thrown/rejected surprise either —
    // this is a well-formed decision the caller can branch on.
    expect(result.outcome).toBe('permission-denied');
  });

  it('5b. permission-denied on the initial SIGTERM never proceeds to wait out the grace period or attempt SIGKILL', async () => {
    const kill = jest.fn(() => 'permission-denied');
    const isAlive = jest.fn(() => true);
    const sleep = fakeSleep();

    await gracefulStop({ pid: 401, kill, isAlive, sleep });

    expect(sleep).not.toHaveBeenCalled();
    expect(kill).toHaveBeenCalledTimes(1);
  });

  it('6. SIGKILL sent but target remains reported-alive resolves with an explicit unconfirmed outcome, never hangs or throws', async () => {
    const kill = jest.fn(() => 'ok');
    const isAlive = jest.fn(() => true); // stays "alive" forever, incl. after SIGKILL
    const sleep = fakeSleep();

    const result = await gracefulStop({ pid: 500, kill, isAlive, sleep });

    expect(kill).toHaveBeenNthCalledWith(1, 500, 'SIGTERM');
    expect(kill).toHaveBeenNthCalledWith(2, 500, 'SIGKILL');
    // Bounded behavior: SIGKILL sent exactly once, not retried in a loop.
    expect(kill).toHaveBeenCalledTimes(2);
    expect(result.outcome).toBe('unconfirmed-after-kill');
    expect(result.signalsSent).toEqual(['SIGTERM', 'SIGKILL']);
  });

  it('6b. never issues a second, unbounded round of SIGKILL/isAlive polling when the target stays alive', async () => {
    const kill = jest.fn(() => 'ok');
    let isAliveCalls = 0;
    const isAlive = jest.fn(() => {
      isAliveCalls += 1;
      return true;
    });
    const sleep = fakeSleep();

    await gracefulStop({ pid: 501, kill, isAlive, sleep });

    // Exactly two liveness checks are expected by this contract: one after
    // the grace period (to decide SIGKILL), one after SIGKILL (to attempt
    // confirmation). Anything higher would indicate an unbounded poll loop.
    expect(isAliveCalls).toBe(2);
  });

  it('7. two concurrent calls against the same PID each run their own independent SIGTERM-wait-SIGKILL sequence', async () => {
    const killLog = [];
    const kill = jest.fn((pid, signal) => {
      killLog.push(signal);
      return 'ok';
    });
    const isAlive = jest.fn(() => true);
    const sleepA = fakeSleep();
    const sleepB = fakeSleep();

    const [resultA, resultB] = await Promise.all([
      gracefulStop({ pid: 600, kill, isAlive, sleep: sleepA }),
      gracefulStop({ pid: 600, kill, isAlive, sleep: sleepB }),
    ]);

    // Each call independently ran its own full SIGTERM -> SIGKILL sequence —
    // neither call's SIGKILL was skipped or short-circuited by the other's
    // state, i.e. no cross-call sharing of "SIGTERM already sent" bookkeeping.
    expect(resultA.signalsSent).toEqual(['SIGTERM', 'SIGKILL']);
    expect(resultB.signalsSent).toEqual(['SIGTERM', 'SIGKILL']);
    // Both calls sent their own SIGTERM before either sent a SIGKILL is NOT
    // required (they are independent, interleaving is unconstrained) — what
    // is required is that overall exactly two SIGTERMs and two SIGKILLs were
    // sent (4 total), never a single shared SIGTERM/SIGKILL pair silently
    // deduplicated across calls, and never MORE than one SIGKILL per call.
    expect(killLog.filter((s) => s === 'SIGTERM')).toHaveLength(2);
    expect(killLog.filter((s) => s === 'SIGKILL')).toHaveLength(2);
    expect(killLog).toHaveLength(4);
  });

  it('8. never sends SIGKILL before the exact configured grace duration has elapsed, even with jittery liveness timing', async () => {
    const kill = jest.fn(() => 'ok');
    // isAlive is jittery/inconsistent across calls but the FINAL check (the
    // one gracefulStop actually acts on after awaiting sleep) reports alive.
    let calls = 0;
    const isAlive = jest.fn(() => {
      calls += 1;
      return true;
    });
    const durations = [];
    const sleep = jest.fn(async (ms) => {
      durations.push(ms);
      // Prove SIGKILL has not yet been sent at the moment sleep is invoked,
      // and is not sent until sleep's promise has actually resolved.
      expect(kill).toHaveBeenCalledTimes(1); // only SIGTERM so far
    });

    await gracefulStop({ pid: 700, kill, isAlive, sleep, graceMs: 12_345 });

    expect(durations).toEqual([12_345]);
    expect(kill).toHaveBeenNthCalledWith(2, 700, 'SIGKILL');
  });

  it('8b. honours a caller-supplied graceMs other than the default', async () => {
    const kill = jest.fn(() => 'ok');
    const isAlive = jest.fn(() => false);
    const sleep = fakeSleep();

    await gracefulStop({ pid: 800, kill, isAlive, sleep, graceMs: 5_000 });

    expect(sleep).toHaveBeenCalledWith(5_000);
  });

  it('8c. falls back to DEFAULT_GRACE_PERIOD_MS when graceMs is not supplied', async () => {
    const kill = jest.fn(() => 'ok');
    const isAlive = jest.fn(() => false);
    const sleep = fakeSleep();

    await gracefulStop({ pid: 900, kill, isAlive, sleep });

    expect(sleep).toHaveBeenCalledWith(DEFAULT_GRACE_PERIOD_MS);
  });

  it('9. treats an injected "already gone" signal on the SIGKILL step as success, not a thrown failure', async () => {
    // SIGTERM succeeds ('ok'), grace period elapses with the target still
    // alive, then the SIGKILL call itself reports "already gone" (ESRCH-
    // equivalent) — mirrors test 4 but for the second `kill()` call site.
    const kill = jest.fn().mockReturnValueOnce('ok').mockReturnValueOnce('already-gone');
    const isAlive = jest.fn(() => true);
    const sleep = fakeSleep();

    const result = await gracefulStop({ pid: 1000, kill, isAlive, sleep });

    expect(kill).toHaveBeenNthCalledWith(1, 1000, 'SIGTERM');
    expect(kill).toHaveBeenNthCalledWith(2, 1000, 'SIGKILL');
    expect(result.outcome).toBe('already-gone');
    expect(result.signalsSent).toEqual(['SIGTERM', 'SIGKILL']);
  });

  it('10. surfaces an injected "permission denied" signal on the SIGKILL step as an explicit failure', async () => {
    // Mirrors test 5 but for the second `kill()` call site: SIGTERM
    // succeeds, the process is still alive after the grace period, and the
    // SIGKILL attempt itself is denied.
    const kill = jest.fn().mockReturnValueOnce('ok').mockReturnValueOnce('permission-denied');
    const isAlive = jest.fn(() => true);
    const sleep = fakeSleep();

    const result = await gracefulStop({ pid: 1001, kill, isAlive, sleep });

    expect(kill).toHaveBeenNthCalledWith(1, 1001, 'SIGTERM');
    expect(kill).toHaveBeenNthCalledWith(2, 1001, 'SIGKILL');
    expect(result.outcome).toBe('permission-denied');
    expect(result.signalsSent).toEqual(['SIGTERM', 'SIGKILL']);
  });

  it('11. throws rather than silently succeeding when kill() returns an unrecognized result on the SIGTERM step', async () => {
    // Fail LOUD, not open: an adapter bug that returns an unmapped/
    // misspelled result must never fall through to being treated as 'ok'.
    const kill = jest.fn(() => 'ESOMETHING');
    const isAlive = jest.fn(() => false);
    const sleep = fakeSleep();

    await expect(gracefulStop({ pid: 1100, kill, isAlive, sleep })).rejects.toThrow(
      /unrecognized result/,
    );
    // Must not proceed past the SIGTERM step into the grace period.
    expect(sleep).not.toHaveBeenCalled();
  });

  it('12. throws rather than silently succeeding when kill() returns an unrecognized result on the SIGKILL step', async () => {
    const kill = jest.fn().mockReturnValueOnce('ok').mockReturnValueOnce('ESOMETHING');
    const isAlive = jest.fn(() => true);
    const sleep = fakeSleep();

    await expect(gracefulStop({ pid: 1101, kill, isAlive, sleep })).rejects.toThrow(
      /unrecognized result/,
    );
    expect(kill).toHaveBeenCalledTimes(2);
  });

  // ---------------------------------------------------------------------------
  // review, High — `verifyStillTarget`: a re-authorization check
  // consulted immediately before the SIGKILL escalation, closing the
  // PID-reuse race the grace-period sleep opens up between the caller's
  // authorization proof and the actual SIGKILL call. See this parameter's
  // own JSDoc above `gracefulStop` for the full scenario.
  // ---------------------------------------------------------------------------

  it("13. omitting verifyStillTarget preserves prior behavior exactly — SIGKILL sent with no re-check", async () => {
    const kill = jest.fn().mockReturnValueOnce('ok').mockReturnValueOnce('ok');
    const isAlive = jest.fn().mockReturnValueOnce(true).mockReturnValueOnce(false);
    const sleep = fakeSleep();

    const result = await gracefulStop({ pid: 2001, kill, isAlive, sleep });

    expect(kill).toHaveBeenCalledTimes(2);
    expect(kill).toHaveBeenNthCalledWith(2, 2001, 'SIGKILL');
    expect(result.outcome).toBe('killed-after-grace');
  });

  it('14. verifyStillTarget resolving true still allows SIGKILL to proceed normally', async () => {
    const kill = jest.fn().mockReturnValueOnce('ok').mockReturnValueOnce('ok');
    const isAlive = jest.fn().mockReturnValueOnce(true).mockReturnValueOnce(false);
    const sleep = fakeSleep();
    const verifyStillTarget = jest.fn(() => true);

    const result = await gracefulStop({ pid: 2002, kill, isAlive, sleep, verifyStillTarget });

    expect(verifyStillTarget).toHaveBeenCalledTimes(1);
    expect(kill).toHaveBeenCalledTimes(2);
    expect(result.outcome).toBe('killed-after-grace');
  });

  it("15. verifyStillTarget resolving false aborts BEFORE SIGKILL and reports 'target-changed-before-kill' — the PID-reuse guard", async () => {
    const kill = jest.fn().mockReturnValueOnce('ok');
    const isAlive = jest.fn(() => true);
    const sleep = fakeSleep();
    const verifyStillTarget = jest.fn(() => false);

    const result = await gracefulStop({ pid: 2003, kill, isAlive, sleep, verifyStillTarget });

    // The critical assertion: SIGKILL must never be sent once re-authorization
    // fails — only the earlier SIGTERM call is present.
    expect(kill).toHaveBeenCalledTimes(1);
    expect(kill).toHaveBeenCalledWith(2003, 'SIGTERM');
    expect(result.outcome).toBe('target-changed-before-kill');
    expect(result.signalsSent).toEqual(['SIGTERM']);
  });

  it('16. verifyStillTarget may resolve asynchronously (a Promise<boolean>)', async () => {
    const kill = jest.fn().mockReturnValueOnce('ok');
    const isAlive = jest.fn(() => true);
    const sleep = fakeSleep();
    const verifyStillTarget = jest.fn(() => Promise.resolve(false));

    const result = await gracefulStop({ pid: 2004, kill, isAlive, sleep, verifyStillTarget });

    expect(kill).toHaveBeenCalledTimes(1);
    expect(result.outcome).toBe('target-changed-before-kill');
  });

  it('17. verifyStillTarget is never consulted when the process already exited before the grace period elapsed — no re-authorization needed when there is no SIGKILL to guard', async () => {
    const kill = jest.fn().mockReturnValueOnce('ok');
    const isAlive = jest.fn(() => false);
    const sleep = fakeSleep();
    const verifyStillTarget = jest.fn(() => false);

    const result = await gracefulStop({ pid: 2005, kill, isAlive, sleep, verifyStillTarget });

    expect(verifyStillTarget).not.toHaveBeenCalled();
    expect(kill).toHaveBeenCalledTimes(1);
    expect(result.outcome).toBe('exited-before-grace');
  });

  // ---------------------------------------------------------------------------
  // Phase 1 — `isAlive`'s two call sites (post-grace, post-SIGKILL) must
  // be `await`ed so a future async-capable (retrying) adapter — e.g.
  // Phase 2's `reapAwareIsAlive` wrapper — can be threaded in without any
  // other change to this function's pure-orchestration contract. Today both
  // call sites are bare `isAlive(pid)` / `!isAlive(pid)`: a Promise is always
  // truthy, so an async `isAlive` currently makes `!isAlive(pid)` always
  // `false` (never `'exited-before-grace'`) and `isAlive(pid)` always truthy
  // (always `'unconfirmed-after-kill'`), regardless of the resolved value.
  // These three tests are RED against that bug and define the fix's exact
  // observable contract: awaiting an async `isAlive` must produce the SAME
  // outcome mapping already pinned above for a synchronous fake.
  // ---------------------------------------------------------------------------

  it("18. an async isAlive resolving false (after a microtask) at the post-grace checkpoint is awaited, producing 'exited-before-grace' — not treated as always-truthy", async () => {
    const kill = jest.fn(() => 'ok');
    // Resolves on a later microtask tick, not synchronously — proving the
    // call site actually awaits rather than treating the returned Promise
    // object itself (always truthy) as the liveness verdict.
    const isAlive = jest.fn(async (targetPid) => {
      await Promise.resolve();
      expect(targetPid).toBe(3001);
      return false;
    });
    const sleep = fakeSleep();

    const result = await gracefulStop({ pid: 3001, kill, isAlive, sleep });

    expect(isAlive).toHaveBeenCalledTimes(1);
    expect(kill).toHaveBeenCalledTimes(1);
    expect(kill).not.toHaveBeenCalledWith(3001, 'SIGKILL');
    expect(result.outcome).toBe('exited-before-grace');
    expect(result.signalsSent).toEqual(['SIGTERM']);
  });

  it("19. an async isAlive resolving true at the post-grace checkpoint, then false (after a microtask) at the post-SIGKILL checkpoint, produces 'killed-after-grace'", async () => {
    const kill = jest.fn(() => 'ok');
    const isAlive = jest
      .fn()
      .mockImplementationOnce(async () => {
        await Promise.resolve();
        return true;
      })
      .mockImplementationOnce(async () => {
        await Promise.resolve();
        return false;
      });
    const sleep = fakeSleep();

    const result = await gracefulStop({ pid: 3002, kill, isAlive, sleep });

    expect(isAlive).toHaveBeenCalledTimes(2);
    expect(kill).toHaveBeenNthCalledWith(1, 3002, 'SIGTERM');
    expect(kill).toHaveBeenNthCalledWith(2, 3002, 'SIGKILL');
    expect(result.outcome).toBe('killed-after-grace');
    expect(result.signalsSent).toEqual(['SIGTERM', 'SIGKILL']);
  });

  it("20. an async isAlive resolving true (after a microtask) at BOTH checkpoints produces 'unconfirmed-after-kill', still bounded to exactly one SIGKILL", async () => {
    const kill = jest.fn(() => 'ok');
    const isAlive = jest.fn(async () => {
      await Promise.resolve();
      return true;
    });
    const sleep = fakeSleep();

    const result = await gracefulStop({ pid: 3003, kill, isAlive, sleep });

    expect(isAlive).toHaveBeenCalledTimes(2);
    expect(kill).toHaveBeenCalledTimes(2);
    expect(result.outcome).toBe('unconfirmed-after-kill');
    expect(result.signalsSent).toEqual(['SIGTERM', 'SIGKILL']);
  });
});
