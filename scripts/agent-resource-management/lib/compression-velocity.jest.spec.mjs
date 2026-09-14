// Phase 2 — computeCompressionVelocity.
//
// RED by construction: `./lib/compression-velocity.mjs` does not exist yet.
// This file specifies the contract `typescript-implementer` must
// satisfy, consistent with:
//   - Phase 1 (already merged, commit ce104918) — `parseVmStat`
//     (`./recon.mjs`) now returns `compressions`/`decompressions` cumulative
//     counters, `NaN` when unparseable (never a fabricated `0`).
//   - the Build Plan's Phase 2 edge cases and the convergence-analysis
//     review findings (cold start, same-timestamp, negative elapsed time,
//     counter decrease/reboot, zero-decompressions ratio, NaN propagation,
//     purity).
//
// House style, per `./allowance.mjs`/`./allowance.jest.spec.mjs`: pure
// function, no I/O, no `Date.now()`/`Math.random()`, "propagate NaN, never
// fabricate" convention (see `./recon.mjs`'s NaN-sentinel doc comments).
//
// Do not add a stub implementation to make this pass — Phase 2 turns it
// green for real.

import { computeCompressionVelocity } from './compression-velocity.mjs';

describe('computeCompressionVelocity', () => {
  it('computes the exact compressionsPerSec from a known delta over a known elapsed time', () => {
    const previous = { compressions: 1000, decompressions: 500, timestampMs: 0 };
    const current = { compressions: 1300, decompressions: 520, timestampMs: 3000 };

    const result = computeCompressionVelocity({ previous, current });

    // delta 300 over 3000ms (3s) -> 100 compressions/sec.
    expect(result.compressionsPerSec).toBe(100);
    expect(result.coldStart).toBe(false);
  });

  it('cold start (previous: null) reports coldStart: true, compressionsPerSec: 0, and a point-in-time compressionRatio computed from current alone', () => {
    const current = { compressions: 1000, decompressions: 500, timestampMs: 10_000 };

    const result = computeCompressionVelocity({ previous: null, current });

    expect(result.coldStart).toBe(true);
    expect(result.compressionsPerSec).toBe(0);
    // 1000 / 500 = 2 — no delta/previous sample needed for this field.
    expect(result.compressionRatio).toBe(2);
  });

  it('same timestamp (current.timestampMs === previous.timestampMs) is treated as cold-start-equivalent: compressionsPerSec: 0, coldStart: true, no divide-by-zero/Infinity/NaN surprise', () => {
    const previous = { compressions: 1000, decompressions: 500, timestampMs: 5_000 };
    const current = { compressions: 1300, decompressions: 520, timestampMs: 5_000 };

    const result = computeCompressionVelocity({ previous, current });

    expect(result.compressionsPerSec).toBe(0);
    expect(Number.isFinite(result.compressionsPerSec)).toBe(true);
    expect(Number.isNaN(result.compressionsPerSec)).toBe(false);
    expect(result.coldStart).toBe(true);
  });

  it('negative elapsed time (clock stepped backward, counters increased normally) is treated as cold-start-equivalent: compressionsPerSec: 0, never negative, coldStart: true', () => {
    const previous = { compressions: 1000, decompressions: 500, timestampMs: 10_000 };
    const current = { compressions: 1300, decompressions: 520, timestampMs: 4_000 };

    const result = computeCompressionVelocity({ previous, current });

    expect(result.compressionsPerSec).toBe(0);
    expect(result.compressionsPerSec).toBeGreaterThanOrEqual(0);
    expect(result.coldStart).toBe(true);
  });

  it('counter decrease (reboot between beats: current.compressions < previous.compressions) clamps compressionsPerSec to 0, never negative, never throws', () => {
    const previous = { compressions: 5000, decompressions: 2000, timestampMs: 0 };
    const current = { compressions: 200, decompressions: 100, timestampMs: 3000 };

    expect(() => computeCompressionVelocity({ previous, current })).not.toThrow();

    const result = computeCompressionVelocity({ previous, current });

    expect(result.compressionsPerSec).toBe(0);
    expect(result.compressionsPerSec).toBeGreaterThanOrEqual(0);
    // Unlike the same-timestamp/backward-clock cases, `previous` genuinely
    // exists and elapsedMs is genuinely positive here — only the cumulative
    // counter itself reset, so this stays coldStart: false.
    expect(result.coldStart).toBe(false);
  });

  it('compressionRatio is NaN, not Infinity, when current.decompressions is 0 (explicit convergence-analysis finding: propagate NaN, never fabricate Infinity)', () => {
    const previous = { compressions: 1000, decompressions: 0, timestampMs: 0 };
    const current = { compressions: 1300, decompressions: 0, timestampMs: 3000 };

    const result = computeCompressionVelocity({ previous, current });

    expect(Number.isNaN(result.compressionRatio)).toBe(true);
    expect(result.compressionRatio).not.toBe(Infinity);
  });

  it('cold start with current.decompressions: 0 also yields compressionRatio: NaN, not Infinity', () => {
    const current = { compressions: 1300, decompressions: 0, timestampMs: 3000 };

    const result = computeCompressionVelocity({ previous: null, current });

    expect(Number.isNaN(result.compressionRatio)).toBe(true);
    expect(result.compressionRatio).not.toBe(Infinity);
  });

  it('NaN current.compressions propagates — never silently coerced to a misleading 0 pretending to be a real reading', () => {
    const previous = { compressions: 1000, decompressions: 500, timestampMs: 0 };
    const current = { compressions: NaN, decompressions: 520, timestampMs: 3000 };

    const result = computeCompressionVelocity({ previous, current });

    expect(Number.isNaN(result.compressionsPerSec)).toBe(true);
    expect(Number.isNaN(result.compressionRatio)).toBe(true);
  });

  it('NaN current.decompressions propagates — never silently coerced to a misleading 0 pretending to be a real reading', () => {
    const previous = { compressions: 1000, decompressions: 500, timestampMs: 0 };
    const current = { compressions: 1300, decompressions: NaN, timestampMs: 3000 };

    const result = computeCompressionVelocity({ previous, current });

    // compressionsPerSec is derived from the compressions delta only, but the
    // decompressions counter itself is unparseable this beat (Phase 1's
    // `parseVmStat` returns NaN when a vm_stat sample is missing that line) —
    // the ratio that depends on it must reflect that, not fabricate a number.
    expect(Number.isNaN(result.compressionRatio)).toBe(true);
  });

  it('NaN previous.compressions (a stale/unparseable prior sample) propagates into compressionsPerSec rather than being silently treated as a real 0 baseline', () => {
    const previous = { compressions: NaN, decompressions: 500, timestampMs: 0 };
    const current = { compressions: 1300, decompressions: 520, timestampMs: 3000 };

    const result = computeCompressionVelocity({ previous, current });

    expect(Number.isNaN(result.compressionsPerSec)).toBe(true);
  });

  it('is pure: identical inputs always produce identical (deeply equal) outputs across repeated calls, no hidden Date.now()/Math.random()/I-O dependency', () => {
    const previous = { compressions: 1000, decompressions: 500, timestampMs: 0 };
    const current = { compressions: 1300, decompressions: 520, timestampMs: 3000 };

    const first = computeCompressionVelocity({ previous, current });
    const second = computeCompressionVelocity({ previous, current });

    expect(second).toEqual(first);
  });
});
