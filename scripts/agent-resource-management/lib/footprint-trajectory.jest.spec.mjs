// RED unit tests for the not-yet-implemented
// scripts/agent-resource-management/lib/footprint-trajectory.mjs module
// (Phase 2 — "EWMA smoothing + trajectory accumulator").
//
// Per the Build Plan's Phase 2 section, this module owns:
//   - ewmaSmooth(previousSmoothed, newSample, alpha) — pure EWMA smoothing
//     function, no side effects.
//   - createFootprintTrajectory() — a small stateful accumulator that folds
//     successive raw samples (numbers, or `null` for a failed poll) into a
//     smoothed series via ewmaSmooth(), skipping nulls without corrupting
//     state, tracking a running peak over SMOOTHED values (not raw), and
//     capping the retained trajectory array length at a fixed, documented
//     max (TRAJECTORY_CAP), dropping the oldest entries first.
//
// Designed function/factory signatures (documented here for the implementer,
// since lib/footprint-trajectory.mjs does not exist yet):
//
//   ewmaSmooth(
//     previousSmoothed: number | null,
//     newSample: number,
//     alpha: number,
//   ): number
//     - previousSmoothed === null seeds the series: returns newSample as-is.
//     - otherwise returns alpha * newSample + (1 - alpha) * previousSmoothed.
//     - throws a RangeError when alpha is not in (0, 1] — see the alpha
//       validation tests below for the exact boundary behaviour.
//
//   createFootprintTrajectory(): {
//     addSample(sample: number | null): void,
//     getPeakSmoothedMb(): number | null,
//     getTrajectory(): number[],
//   }
//     - addSample(null) is a documented no-op: it does not fold, does not
//       move the running smoothed value, and does not affect the peak.
//     - getTrajectory() returns (at most) the TRAJECTORY_CAP most recent
//       smoothed values, oldest-first within that window.
//     - getPeakSmoothedMb() returns null until at least one non-null sample
//       has been folded.
//
// Do NOT add a stub implementation to make this pass — that is the builder's
// job in the next phase. Every test below is expected to fail at IMPORT TIME
// (`Cannot find module './footprint-trajectory.mjs'`) because the module does
// not exist yet.

import { ewmaSmooth, createFootprintTrajectory, TRAJECTORY_CAP } from './footprint-trajectory.mjs';

describe('ewmaSmooth', () => {
  it('seeds smoothed value from first sample', () => {
    expect(ewmaSmooth(null, 123.45, 0.3)).toBe(123.45);
  });

  it('smooths a noisy series toward the constant it fluctuates around', () => {
    // Noisy series oscillating around 100: 90, 110, 95, 105, 100, 108, 92.
    const samples = [90, 110, 95, 105, 100, 108, 92];
    let smoothed = null;
    for (const sample of samples) {
      smoothed = ewmaSmooth(smoothed, sample, 0.3);
    }

    // The smoothed value should have converged much closer to the constant
    // (100) than the raw noise amplitude (±10) would allow for any single
    // sample picked at random — and it must not exactly equal any one of the
    // raw noisy samples fed in.
    expect(Math.abs(smoothed - 100)).toBeLessThan(5);
    expect(samples).not.toContain(smoothed);
  });

  it('rejects alpha <= 0', () => {
    expect(() => ewmaSmooth(10, 20, 0)).toThrow(RangeError);
    expect(() => ewmaSmooth(10, 20, -0.1)).toThrow(RangeError);
  });

  it('rejects alpha > 1', () => {
    expect(() => ewmaSmooth(10, 20, 1.1)).toThrow(RangeError);
  });

  it('accepts alpha === 1 as "no smoothing, replace outright"', () => {
    expect(ewmaSmooth(10, 20, 1)).toBe(20);
    expect(ewmaSmooth(500, 1, 1)).toBe(1);
  });

  it('applies the standard EWMA formula for an interior alpha', () => {
    // alpha * newSample + (1 - alpha) * previousSmoothed
    // 0.5 * 20 + 0.5 * 10 = 15
    expect(ewmaSmooth(10, 20, 0.5)).toBe(15);
  });
});

describe('createFootprintTrajectory', () => {
  it('folds successive raw samples into a smoothed series', () => {
    const trajectory = createFootprintTrajectory();
    trajectory.addSample(100);
    trajectory.addSample(200);

    const series = trajectory.getTrajectory();
    expect(series).toHaveLength(2);
    expect(series[0]).toBe(100); // seeded from the first sample
    expect(series[1]).toBeGreaterThan(100);
    expect(series[1]).toBeLessThan(200);
  });

  it('skips null samples without corrupting the smoothed value or peak', () => {
    const trajectory = createFootprintTrajectory();
    trajectory.addSample(100);
    trajectory.addSample(150);

    const smoothedBeforeNull = trajectory.getTrajectory().at(-1);
    const peakBeforeNull = trajectory.getPeakSmoothedMb();

    trajectory.addSample(null);

    expect(trajectory.getTrajectory().at(-1)).toBe(smoothedBeforeNull);
    expect(trajectory.getPeakSmoothedMb()).toBe(peakBeforeNull);
    // A skipped null must not even appear as an extra (undefined/NaN) entry.
    expect(trajectory.getTrajectory()).toHaveLength(2);

    trajectory.addSample(160);
    expect(trajectory.getTrajectory()).toHaveLength(3);
  });

  it('tracks the running peak across smoothed values, not raw', () => {
    const trajectory = createFootprintTrajectory();
    // Mostly-low samples with one huge single-sample spike in the middle.
    const samples = [50, 52, 48, 51, 5000, 49, 53, 50];
    for (const sample of samples) {
      trajectory.addSample(sample);
    }

    const rawMax = Math.max(...samples);
    const smoothedPeak = trajectory.getPeakSmoothedMb();
    const smoothedMax = Math.max(...trajectory.getTrajectory());

    // The raw spike (5000) must be damped by EWMA — the tracked peak must be
    // measurably lower than the raw max, and must equal the actual max of
    // the smoothed series (not the raw series).
    expect(smoothedPeak).toBeLessThan(rawMax);
    expect(smoothedPeak).toBe(smoothedMax);
  });

  it('returns null from getPeakSmoothedMb() until a non-null sample has folded', () => {
    const trajectory = createFootprintTrajectory();
    expect(trajectory.getPeakSmoothedMb()).toBeNull();

    trajectory.addSample(null);
    expect(trajectory.getPeakSmoothedMb()).toBeNull();

    trajectory.addSample(42);
    expect(trajectory.getPeakSmoothedMb()).toBe(42);
  });

  it('caps the trajectory length at the documented max, dropping oldest first', () => {
    const trajectory = createFootprintTrajectory();
    const totalSamples = TRAJECTORY_CAP + 25;

    for (let i = 0; i < totalSamples; i += 1) {
      // Monotonically increasing raw samples make it easy to assert
      // "most-recent window" — each smoothed value is strictly identifiable
      // by which raw sample most recently drove it.
      trajectory.addSample(i);
    }

    const series = trajectory.getTrajectory();
    expect(series).toHaveLength(TRAJECTORY_CAP);

    // The retained window must be the most-recent TRAJECTORY_CAP smoothed
    // values, oldest-first — i.e. the earliest entries from the very start
    // of the run must have been dropped.
    expect(series[series.length - 1]).toBeGreaterThan(series[0]);

    // Feed in a great many more samples than the cap over a long agent
    // lifetime — the array must never grow past TRAJECTORY_CAP regardless.
    for (let i = 0; i < totalSamples * 10; i += 1) {
      trajectory.addSample(i);
    }
    expect(trajectory.getTrajectory()).toHaveLength(TRAJECTORY_CAP);
  });
});
