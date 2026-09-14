// — tests for the host-resource probe's wall-clock bound.
//
// The premise this module rests on was verified empirically rather than
// assumed, because the Convergence Analysis for asserted the opposite
// ("`execFileSync` has no `timeout` option"). It does, and it works. The
// first `describe` below pins that fact against a real child process so the
// module's whole rationale cannot quietly rot if Node's behaviour ever
// changes — everything else here is pure-function unit testing.

import { jest } from '@jest/globals';
import { execFileSync } from 'node:child_process';
import { existsSync } from 'node:fs';

import {
  DEFAULT_PROBE_TIMEOUT_MS,
  ProbeTimeoutError,
  probeExecOptions,
  isProbeTimeout,
  asProbeFailure,
  capacityIfSampleFresh,
} from './probe-bound.mjs';

const SLEEP_BIN = ['/bin/sleep', '/usr/bin/sleep'].find((candidate) => existsSync(candidate));

describe('execFileSync genuinely honours timeout/killSignal (the premise this module rests on)', () => {
  // Skipped rather than failed where no `sleep` binary exists, so a future
  // non-POSIX runner reports "not proven here" instead of a false red.
  const maybeIt = SLEEP_BIN ? it : it.skip;

  maybeIt('kills a child that overruns its bound and throws with ETIMEDOUT/SIGTERM', () => {
    const startedAt = Date.now();
    let thrown;
    try {
      execFileSync(SLEEP_BIN, ['5'], probeExecOptions(300));
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeDefined();
    // The call RETURNED early — this is a real bound, not a post-hoc
    // observation of an overrun that already happened. A `Promise.race`
    // could not have achieved this: `execFileSync` blocks the event loop, so
    // no timer could fire while it ran.
    expect(Date.now() - startedAt).toBeLessThan(4_000);
    expect(isProbeTimeout(thrown)).toBe(true);
  }, 10_000);

  maybeIt('leaves a child that finishes inside its bound completely alone', () => {
    expect(() => execFileSync(SLEEP_BIN, ['0'], probeExecOptions(5_000))).not.toThrow();
  }, 10_000);
});

describe('probeExecOptions', () => {
  it('pins encoding, timeout and killSignal together so the classifier cannot desynchronise', () => {
    expect(probeExecOptions()).toEqual({
      encoding: 'utf8',
      timeout: DEFAULT_PROBE_TIMEOUT_MS,
      killSignal: 'SIGTERM',
    });
  });

  it('accepts an explicit bound', () => {
    expect(probeExecOptions(1_234).timeout).toBe(1_234);
  });

  it("keeps the retried set's absolute worst case inside roughly one beat", () => {
    // `collect()` may issue the four-command set twice (its single bounded
    // retry), so the worst case a caller sees is 8 x the per-command bound.
    // The orchestrator beat cadence SKILL.md documents is ~15-30s; a wedged
    // host must cost roughly one beat, not an unbounded stall. Deliberately
    // NOT asserted against DEFAULT_LOCK_STALENESS_MS: since the probe was
    // hoisted out of the critical section it no longer runs under a lock, and
    // pinning it to a constant it is no longer coupled to would make this
    // test lie about why the number is what it is.
    expect(DEFAULT_PROBE_TIMEOUT_MS * 8).toBeLessThanOrEqual(20_000);
  });
});

describe('isProbeTimeout', () => {
  it('recognises the ETIMEDOUT code', () => {
    expect(isProbeTimeout({ code: 'ETIMEDOUT' })).toBe(true);
  });

  it('recognises a SIGTERM kill even when no code is set', () => {
    expect(isProbeTimeout({ signal: 'SIGTERM' })).toBe(true);
  });

  it('does not misclassify an ordinary command failure', () => {
    expect(isProbeTimeout({ code: 'ENOENT' })).toBe(false);
    expect(isProbeTimeout({ status: 1, signal: null })).toBe(false);
  });

  it('tolerates non-object inputs rather than throwing on them', () => {
    expect(isProbeTimeout(null)).toBe(false);
    expect(isProbeTimeout(undefined)).toBe(false);
    expect(isProbeTimeout('ETIMEDOUT')).toBe(false);
  });
});

describe('asProbeFailure', () => {
  it('converts an overrun into a typed ProbeTimeoutError naming the command and the bound', () => {
    const converted = asProbeFailure({ code: 'ETIMEDOUT' }, 'df -g /', 2_000);
    expect(converted).toBeInstanceOf(ProbeTimeoutError);
    expect(converted.name).toBe('ProbeTimeoutError');
    expect(converted.code).toBe('ARM_PROBE_TIMEOUT');
    expect(converted.message).toContain('df -g /');
    expect(converted.message).toContain('2000ms');
  });

  it('defaults the reported bound to the module default', () => {
    expect(asProbeFailure({ code: 'ETIMEDOUT' }, 'vm_stat').message).toContain(
      `${DEFAULT_PROBE_TIMEOUT_MS}ms`,
    );
  });

  it('passes an unrelated failure through unchanged and unwrapped', () => {
    const original = Object.assign(new Error('command not found'), { code: 'ENOENT' });
    expect(asProbeFailure(original, 'vm_stat')).toBe(original);
  });
});

describe('capacityIfSampleFresh — the fail-closed arm the hoist rests on', () => {
  const base = { availableCapacity: 7, sampledAt: 1_700_000_000_000, freshnessWindowMs: 20_000 };

  it('passes the sampled capacity through while the sample is fresh', () => {
    expect(capacityIfSampleFresh({ ...base, referenceNow: base.sampledAt + 1_000 })).toBe(7);
  });

  it('is inclusive at the boundary — exactly at the window is still fresh', () => {
    // Matches isLockStale/pruneStale/pruneExpiredClaims/isSampleFresh, all of
    // which treat "exactly at the threshold" as not-yet-expired. A silent
    // `>=` divergence here would be invisible without this exact-equality
    // assertion.
    expect(
      capacityIfSampleFresh({ ...base, referenceNow: base.sampledAt + base.freshnessWindowMs }),
    ).toBe(7);
  });

  it('fails closed one millisecond past the boundary', () => {
    expect(
      capacityIfSampleFresh({ ...base, referenceNow: base.sampledAt + base.freshnessWindowMs + 1 }),
    ).toBe(0);
  });

  it('fails closed on a long-past sample, whatever capacity it reported', () => {
    expect(
      capacityIfSampleFresh({ ...base, availableCapacity: 999, referenceNow: base.sampledAt + 120_000 }),
    ).toBe(0);
  });

  it('fails closed rather than throwing when the sample carries no usable stamp', () => {
    // `isSampleFresh` rejects a non-finite `sampledAt` outright. Reaching
    // here at all would mean the probe returned a malformed stamp — deny,
    // do not grant on an unverifiable sample.
    expect(capacityIfSampleFresh({ ...base, sampledAt: Number.NaN, referenceNow: 1 })).toBe(0);
    expect(capacityIfSampleFresh({ ...base, sampledAt: undefined, referenceNow: 1 })).toBe(0);
  });

  it('treats a sample stamped in the future as fresh, not as an error', () => {
    // Clock skew between the stamp and the comparison is not this function's
    // problem to police, and denying on it would be the wrong direction:
    // a future stamp means the sample is newer than the decision, which is
    // never the over-admission case this bound exists for.
    expect(capacityIfSampleFresh({ ...base, referenceNow: base.sampledAt - 5_000 })).toBe(7);
  });
});

// ---------------------------------------------------------------------------
// `onStale` (review round 2) — the staleness arm was the one denial
// source that produced no output at all. These cover the reporting hook that
// gave it a voice, WITHOUT changing what it denies.
// ---------------------------------------------------------------------------

describe('capacityIfSampleFresh reports the stale branch through onStale', () => {
  const base = { availableCapacity: 7, sampledAt: 1_000_000, freshnessWindowMs: 20_000 };

  it('reports the measured age and the window that rejected it', () => {
    const onStale = jest.fn();
    capacityIfSampleFresh({ ...base, referenceNow: base.sampledAt + 25_000, onStale });
    expect(onStale).toHaveBeenCalledTimes(1);
    expect(onStale).toHaveBeenCalledWith({ ageMs: 25_000, freshnessWindowMs: 20_000 });
  });

  it('stays silent on the happy path, including exactly at the inclusive boundary', () => {
    // A warning that fires on every ordinary grant is noise, and noise is how
    // a real warning gets ignored. The boundary is the case most likely to
    // regress, since `isSampleFresh` counts it as fresh.
    const onStale = jest.fn();
    capacityIfSampleFresh({ ...base, referenceNow: base.sampledAt, onStale });
    capacityIfSampleFresh({
      ...base,
      referenceNow: base.sampledAt + base.freshnessWindowMs,
      onStale,
    });
    expect(onStale).not.toHaveBeenCalled();
  });

  it('fires one millisecond past the boundary — the first genuinely stale sample', () => {
    const onStale = jest.fn();
    capacityIfSampleFresh({
      ...base,
      referenceNow: base.sampledAt + base.freshnessWindowMs + 1,
      onStale,
    });
    expect(onStale).toHaveBeenCalledTimes(1);
  });

  it('reports the unstamped-sample denial too, rather than swallowing it', () => {
    // A malformed stamp denies via the same arm. It is at least as worth
    // announcing as an ordinary aged-out sample, and `ageMs` being NaN here
    // is exactly why `describeStaleSampleDenial` omits the measurement
    // instead of printing it.
    const onStale = jest.fn();
    capacityIfSampleFresh({ ...base, sampledAt: Number.NaN, referenceNow: 1, onStale });
    expect(onStale).toHaveBeenCalledTimes(1);
    expect(Number.isNaN(onStale.mock.calls[0][0].ageMs)).toBe(true);
  });

  it('still denies with the same value when no reporter is supplied', () => {
    // The hook is observability only. Its absence must not change a single
    // grant decision — every pre-existing caller passes nothing.
    expect(capacityIfSampleFresh({ ...base, referenceNow: base.sampledAt + 25_000 })).toBe(0);
    expect(
      capacityIfSampleFresh({ ...base, referenceNow: base.sampledAt + 25_000, onStale: undefined }),
    ).toBe(0);
    expect(
      capacityIfSampleFresh({ ...base, referenceNow: base.sampledAt + 25_000, onStale: 'not-a-fn' }),
    ).toBe(0);
  });

  it('never lets the reported value diverge from the returned one', () => {
    // The property that makes the hook trustworthy: it reports the branch
    // that was actually taken, so a reader can never see a warning for a
    // grant that succeeded, or silence for a denial.
    const onStale = jest.fn();
    for (const offset of [0, 1, 19_999, 20_000, 20_001, 60_000]) {
      onStale.mockClear();
      const granted = capacityIfSampleFresh({ ...base, referenceNow: base.sampledAt + offset, onStale });
      expect(onStale.mock.calls.length).toBe(granted === 0 ? 1 : 0);
    }
  });
});
