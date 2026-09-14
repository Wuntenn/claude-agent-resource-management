// Unit coverage for lib/claim-denial.mjs (review round 2).
//
// These are the branches `cli.mjs` cannot be tested for: it is excluded from
// the coverage gate (see jest.skills.config.mjs) and is exercised only as a
// black box through `child_process`, so a mis-classified denial there would
// be caught by nothing. The end-to-end proof that the classification actually
// REACHES stderr lives in ../claim.jest.spec.mjs; this file proves the
// classification itself.

import {
  describeClaimDenial,
  describeStaleSampleDenial,
  lockWaitWindowSeconds,
} from './claim-denial.mjs';
import { LOCK_RETRY_DELAY_MS, LOCK_RETRY_JITTER_MS } from './coordination-file.mjs';

describe('lockWaitWindowSeconds', () => {
  it("derives the window from acquireLock's own retry constants, not a restated copy", () => {
    // The drift this guards: a hardcoded "4-8s" in the warning text stays
    // put while LOCK_RETRY_DELAY_MS moves, and the log then lies about how
    // long the caller actually waited. Asserted against the imported
    // constants so the arithmetic — not a snapshot of today's answer — is
    // what is pinned.
    const attempts = 800;
    expect(lockWaitWindowSeconds(attempts)).toEqual({
      minSeconds: (attempts * LOCK_RETRY_DELAY_MS) / 1000,
      maxSeconds: (attempts * (LOCK_RETRY_DELAY_MS + LOCK_RETRY_JITTER_MS)) / 1000,
    });
  });

  it("resolves to the 4-8s window --claim's own ceiling actually buys", () => {
    // The concrete pairing DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS documents, so a
    // change to either retry constant surfaces here as a named number rather
    // than as an abstract formula mismatch.
    expect(lockWaitWindowSeconds(800)).toEqual({ minSeconds: 4, maxSeconds: 8 });
  });

  it.each([
    ['zero', 0],
    ['negative', -1],
    ['NaN', Number.NaN],
    ['Infinity', Number.POSITIVE_INFINITY],
    ['undefined', undefined],
    ['a string', '800'],
  ])('returns null for an unusable ceiling (%s) rather than formatting nonsense', (_label, value) => {
    expect(lockWaitWindowSeconds(value)).toBeNull();
  });
});

describe('describeClaimDenial', () => {
  it('names the attempt ceiling AND the window it bought on a lock timeout', () => {
    const result = describeClaimDenial(
      { code: 'COORDINATION_LOCK_TIMEOUT' },
      { lockMaxAttempts: 800 },
    );
    // "800" alone is meaningless to whoever is reading stderr; the seconds
    // are what let them judge whether the ceiling was the problem.
    expect(result.degradedWhat).toContain('800 attempts');
    expect(result.degradedWhat).toContain('~4-8s');
    expect(result.degradedWhat).toContain('contended');
  });

  it('indicts DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS by name on a lock timeout', () => {
    // The reviewer's actual complaint: a mis-sized ceiling would be
    // discovered only as "claims mysteriously stopped working". The warning
    // has to point at the constant, and has to say that RECURRENCE — not one
    // occurrence on a momentarily busy host — is the signal.
    const { consequence } = describeClaimDenial(
      { code: 'COORDINATION_LOCK_TIMEOUT' },
      { lockMaxAttempts: 800 },
    );
    expect(consequence).toContain('DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS');
    expect(consequence).toContain('recurs');
    expect(consequence).toContain('under-sized');
  });

  it('renders a fractional window to one decimal rather than a full float', () => {
    // 800 happens to divide into whole seconds, so the integer path is the
    // only one --claim itself exercises today. A future ceiling that does not
    // (750 attempts -> 3.75-7.5s) must still read as a duration a human can
    // scan, not as `~3.75-7.5000000001s`.
    const { degradedWhat } = describeClaimDenial(
      { code: 'COORDINATION_LOCK_TIMEOUT' },
      { lockMaxAttempts: 750 },
    );
    expect(degradedWhat).toContain('750 attempts');
    expect(degradedWhat).toContain('~3.8-7.5s');
  });

  it('still degrades usefully on a timeout when no ceiling was supplied', () => {
    const { degradedWhat, consequence } = describeClaimDenial({
      code: 'COORDINATION_LOCK_TIMEOUT',
    });
    expect(degradedWhat).toContain('contended');
    expect(degradedWhat).not.toContain('undefined');
    expect(degradedWhat).not.toContain('NaN');
    expect(consequence).toContain('DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS');
  });

  it('indicts DEFAULT_LOCK_STALENESS_MS — a different constant — on a lost lock', () => {
    // The two lock outcomes must not pool into one message: a timeout means
    // the ceiling is too tight, a loss means a critical section outlived the
    // reclaim horizon. Pooling them sends a reader to the wrong constant.
    const { degradedWhat, consequence } = describeClaimDenial({ code: 'COORDINATION_LOCK_LOST' });
    expect(degradedWhat).toContain('lost');
    expect(consequence).toContain('DEFAULT_LOCK_STALENESS_MS');
    expect(consequence).not.toContain('DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS');
  });

  it('gives the two lock outcomes genuinely distinct text', () => {
    const timeout = describeClaimDenial(
      { code: 'COORDINATION_LOCK_TIMEOUT' },
      { lockMaxAttempts: 800 },
    );
    const lost = describeClaimDenial({ code: 'COORDINATION_LOCK_LOST' });
    expect(timeout.degradedWhat).not.toBe(lost.degradedWhat);
    expect(timeout.consequence).not.toBe(lost.consequence);
  });

  it.each([
    ['an unrelated coordination code', { code: 'EACCES' }],
    ['a legacy-claim code', { code: 'COORDINATION_UNRECOVERABLE_LEGACY_CLAIM' }],
    ['an error with only a message', { message: 'boom' }],
    ['an empty object', {}],
    ['null', null],
    ['undefined', undefined],
  ])('falls back to the pre-existing generic pair for %s', (_label, error) => {
    // This module may only ever ADD specificity. The generic pair is
    // byte-for-byte what the `--claim` catch passed before it existed, so
    // every previously-working message survives unchanged.
    expect(describeClaimDenial(error, { lockMaxAttempts: 800 })).toEqual({
      degradedWhat: 'coordination file',
      consequence: 'granting nothing for this claim',
    });
  });

  it('starts every consequence with the phrase the existing claim tests assert on', () => {
    // ../claim.jest.spec.mjs asserts `toContain('granting nothing for this
    // claim')` on the contended-lock path. Adding specificity must not break
    // that contract.
    for (const error of [
      { code: 'COORDINATION_LOCK_TIMEOUT' },
      { code: 'COORDINATION_LOCK_LOST' },
      { code: 'EACCES' },
    ]) {
      expect(describeClaimDenial(error, { lockMaxAttempts: 800 }).consequence).toMatch(
        /^granting nothing for this claim/,
      );
    }
  });
});

describe('describeStaleSampleDenial', () => {
  it('reports the measured age against the window that rejected it', () => {
    const { degradedWhat } = describeStaleSampleDenial({ ageMs: 21_000, freshnessWindowMs: 20_000 });
    expect(degradedWhat).toContain('21000ms');
    expect(degradedWhat).toContain('20000ms');
  });

  it('indicts the freshness constant and its pairing, not the lock ceiling alone', () => {
    const { consequence } = describeStaleSampleDenial({ ageMs: 21_000, freshnessWindowMs: 20_000 });
    expect(consequence).toContain('DEFAULT_CLAIM_SAMPLE_FRESHNESS_MS');
    expect(consequence).toContain('paired');
  });

  it('omits the measurement rather than printing NaN when the context is unavailable', () => {
    for (const context of [undefined, {}, { ageMs: Number.NaN, freshnessWindowMs: 20_000 }]) {
      const { degradedWhat } = describeStaleSampleDenial(context);
      expect(degradedWhat).not.toContain('NaN');
      expect(degradedWhat).not.toContain('undefined');
      expect(degradedWhat).toContain('aged out');
    }
  });

  it("reads as a noun phrase in warnCoordinationDegraded's `<what> unavailable (<code>)` template", () => {
    // The caller renders `${degradedWhat} unavailable (${code})`. A
    // degradedWhat written as a clause ("sample aged out before the lock was
    // acquired") renders as "... was acquired unavailable (...)", which is not
    // a sentence — and this line exists solely to be read by a human at 2am.
    // Pinned for all three denial sources, since they share the one template.
    const sources = [
      describeStaleSampleDenial({ ageMs: 21_000, freshnessWindowMs: 20_000 }),
      describeStaleSampleDenial(),
      describeClaimDenial({ code: 'COORDINATION_LOCK_TIMEOUT' }, { lockMaxAttempts: 800 }),
      describeClaimDenial({ code: 'COORDINATION_LOCK_LOST' }),
    ];
    for (const { degradedWhat } of sources) {
      // The cause sits inside a parenthetical, leaving the head noun adjacent
      // to the template's own verb.
      expect(degradedWhat).toMatch(/^[a-z-]+(?: [a-z-]+)* \(.+\)$/);
    }
  });

  it('is distinguishable from both lock denials', () => {
    // The whole point of giving this arm a voice is that it is a THIRD
    // cause. If its text collided with either lock message it would be
    // indistinguishable in a log, which is the state this replaced.
    const stale = describeStaleSampleDenial({ ageMs: 21_000, freshnessWindowMs: 20_000 });
    const timeout = describeClaimDenial(
      { code: 'COORDINATION_LOCK_TIMEOUT' },
      { lockMaxAttempts: 800 },
    );
    const lost = describeClaimDenial({ code: 'COORDINATION_LOCK_LOST' });
    expect(new Set([stale.degradedWhat, timeout.degradedWhat, lost.degradedWhat]).size).toBe(3);
  });
});
