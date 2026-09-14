// Phase 1 — failing tests for the PURE leak-velocity slope function,
// `computeLeakVelocity` in `./leak-velocity.mjs` (does not exist yet — this
// whole file is RED by construction: the `import` below is expected to fail
// module resolution, the same acceptable "red" form documented in
// `./aimd-ceiling.jest.spec.mjs`).
//
// ---------------------------------------------------------------------------
// PINNED CONTRACT (this is the spec the Phase 1 implementer must build to) —
// mirrors `computeAimdCeiling`'s shape (pure function, sustained-N-
// consecutive-polls-above-threshold trip rule, full-reset-to-0 on any
// below-threshold poll, not a decrement) rather than
// `../leak-velocity-outer-acceptance.jest.spec.mjs`'s own Phase-0 DESIGN
// GUESS (a `detectLeakVelocity({ trajectory, thresholdMbPerPoll,
// sustainedPollsRequired })` boolean over raw trajectory deltas). That
// Phase-0 file explicitly documents its shape as "not binding on the
// eventual implementer" — this file supersedes it per this ticket's Phase 1
// Build Plan, which pins the following shape instead:
//
//   computeLeakVelocity(state, config) -> result
//
//   state: {
//     trajectory: (number|null)[],  // (number|null)[] with null-skipping is
//                                    // a defensive superset this module
//                                    // accepts for robustness / a possible
//                                    // future caller — NOT a claim that this
//                                    // mirrors footprint-state.mjs's
//                                    // FootprintStateEntry#trajectory today
//                                    // (that typedef is `number[]`; a failed
//                                    // poll is skipped and never appears in
//                                    // the array at all).
//     firstPolledAt: number,        // ms epoch of this pid's first poll ever
//     lastPolledAt: number,         // ms epoch of the most recent poll
//     windowStartAt: number,        // OPTIONAL. ms epoch of the oldest
//                                    // surviving trajectory sample; falls
//                                    // back to firstPolledAt when absent
//                                    // (see leak-velocity.mjs header comment
//                                    // for the truncation-drift caveat).
//     consecutiveAboveThreshold: number, // PERSISTED counter, carried in
//                                    // from the previous call's result —
//                                    // same persisted-counter shape as
//                                    // computeAimdCeiling's
//                                    // `sustainedNormalCount`.
//   }
//
//   config: {
//     thresholdMbPerMin: number,
//     consecutivePollsRequired: number,
//     minSamples?: number,          // default 2 — fewer valid (non-null)
//                                    // trajectory samples than this is a
//                                    // cold start: never trips, never
//                                    // throws.
//   }
//
//   returns: {
//     trip: boolean,
//     growthRateMbPerMin: number | null,
//     consecutiveAboveThreshold: number,
//   }
//
// Slope computation: `growthRateMbPerMin` is computed from the FIRST and
// LAST valid (finite, non-null) samples in `trajectory`, over
// `lastPolledAt - windowStartAt` (converted to minutes), where
// `windowStartAt` is `state.windowStartAt` when supplied, else
// `state.firstPolledAt` as a fallback — NOT an assumed fixed poll interval.
// This is what makes the rate correct under non-uniform poll cadence (a beat
// can land seconds or minutes apart — see footprint-state.mjs's own
// `firstPolledAt`/`lastPolledAt` doc comments for why this codebase never
// assumes a fixed cadence). NOTE: the `firstPolledAt` fallback is a KNOWN,
// ACCEPTED APPROXIMATION — once `trajectory` has truncated past
// `TRAJECTORY_CAP` (120), `firstPolledAt` predates the oldest surviving
// sample, so the fallback under-estimates the rate. See
// leak-velocity.mjs's header comment for the full rationale; Phase 2's
// wiring is expected to supply `windowStartAt` once it can track the
// truncation-aware timestamp.
//
// Trip rule (mirrors computeAimdCeiling's AIMD sustained-window shape
// exactly, substituting "above growth threshold" for "Normal pressure"):
//   - When `growthRateMbPerMin >= config.thresholdMbPerMin`, the persisted
//     `consecutiveAboveThreshold` counter increments. Only once the
//     incremented counter reaches `config.consecutivePollsRequired` does
//     `trip` become `true` — and the counter resets to 0 on that same call
//     (mirrors `computeAimdCeiling`'s "the counter then resets to 0" once an
//     AIMD increase fires).
//   - Any call where the growth rate is BELOW threshold (or unmeasurable —
//     cold start, or a degenerate polling interval) fully resets the
//     counter to 0 — never a decrement, never partial credit. A single
//     below-threshold poll erases an entire prior streak, exactly like
//     `computeAimdCeiling`'s non-Normal beat resetting `sustainedNormalCount`.
//
// Failed-poll (`null` sample) handling: a `null` entry in `trajectory` is
// skipped when locating the "first" and "last" valid samples for the slope
// calculation — it never corrupts the computed rate (mirrors
// `footprint-trajectory.mjs`'s `addSample`/`recordFootprintPoll`'s own
// "skip nulls without corrupting state" contract). Per this file's test 5
// below, a failed poll does NOT, by itself, cause a reset of
// `consecutiveAboveThreshold` — the slope is still computed from whatever
// valid samples remain, and the trip decision follows that computed rate
// exactly as it would have without the null in the middle. Only a
// genuinely-computed below-threshold (or unmeasurable) rate resets the
// counter.
// ---------------------------------------------------------------------------

import { TRAJECTORY_CAP } from './footprint-trajectory.mjs';
// RED BY CONSTRUCTION — this module does not exist yet anywhere in the repo.
import { computeLeakVelocity, DEFAULT_LEAK_VELOCITY_THRESHOLDS } from './leak-velocity.mjs';

const BASE_CONFIG = {
  thresholdMbPerMin: 10,
  consecutivePollsRequired: 4,
};

/** Builds a state object with a trajectory whose samples grow linearly by `mbPerPollDelta` every `pollIntervalMs`, starting at `startMb`. */
function buildLinearGrowthState({
  sampleCount,
  startMb,
  mbPerPollDelta,
  pollIntervalMs,
  consecutiveAboveThreshold = 0,
}) {
  const trajectory = [];
  for (let i = 0; i < sampleCount; i += 1) {
    trajectory.push(startMb + i * mbPerPollDelta);
  }
  const firstPolledAt = 0;
  const lastPolledAt = (sampleCount - 1) * pollIntervalMs;
  return { trajectory, firstPolledAt, lastPolledAt, consecutiveAboveThreshold };
}

describe('computeLeakVelocity — pure leak-velocity slope + sustained-trip rule', () => {
  it('does not trip before N (4) consecutive above-threshold polls — the 3rd call (N-1) leaves it untripped', () => {
    // Each call represents one more poll landing above threshold; the
    // persisted counter is threaded through exactly like computeAimdCeiling's
    // sustainedNormalCount.
    let result;
    let consecutiveAboveThreshold = 0;

    for (let call = 0; call < BASE_CONFIG.consecutivePollsRequired - 1; call += 1) {
      const state = buildLinearGrowthState({
        sampleCount: 2,
        startMb: 100,
        mbPerPollDelta: 100, // well above threshold: 100MB over 1 minute = 100 MB/min
        pollIntervalMs: 60_000,
        consecutiveAboveThreshold,
      });
      result = computeLeakVelocity(state, BASE_CONFIG);
      consecutiveAboveThreshold = result.consecutiveAboveThreshold;
    }

    expect(result.trip).toBe(false);
    expect(result.consecutiveAboveThreshold).toBe(BASE_CONFIG.consecutivePollsRequired - 1);
  });

  it('trips at exactly N (4) consecutive above-threshold polls, and resets the counter to 0 on the tripping call', () => {
    let result;
    let consecutiveAboveThreshold = 0;

    for (let call = 0; call < BASE_CONFIG.consecutivePollsRequired; call += 1) {
      const state = buildLinearGrowthState({
        sampleCount: 2,
        startMb: 100,
        mbPerPollDelta: 100,
        pollIntervalMs: 60_000,
        consecutiveAboveThreshold,
      });
      result = computeLeakVelocity(state, BASE_CONFIG);
      consecutiveAboveThreshold = result.consecutiveAboveThreshold;
    }

    expect(result.trip).toBe(true);
    expect(result.consecutiveAboveThreshold).toBe(0);
  });

  it('a single below-threshold poll fully resets the counter to 0 — not a decrement, no partial credit', () => {
    // Build up 3 of the 4 required above-threshold polls first.
    let consecutiveAboveThreshold = 0;
    for (let call = 0; call < 3; call += 1) {
      const state = buildLinearGrowthState({
        sampleCount: 2,
        startMb: 100,
        mbPerPollDelta: 100,
        pollIntervalMs: 60_000,
        consecutiveAboveThreshold,
      });
      consecutiveAboveThreshold = computeLeakVelocity(state, BASE_CONFIG).consecutiveAboveThreshold;
    }
    expect(consecutiveAboveThreshold).toBe(3);

    // One below-threshold poll: 1 MB over 1 minute = 1 MB/min, well under the 10 MB/min threshold.
    const belowThresholdState = buildLinearGrowthState({
      sampleCount: 2,
      startMb: 100,
      mbPerPollDelta: 1,
      pollIntervalMs: 60_000,
      consecutiveAboveThreshold,
    });
    const afterBelow = computeLeakVelocity(belowThresholdState, BASE_CONFIG);
    expect(afterBelow.trip).toBe(false);
    expect(afterBelow.consecutiveAboveThreshold).toBe(0);

    // Resuming above-threshold growth afterwards must count from 0, not from
    // the pre-reset streak (2 + 1 more above-threshold poll must read as 1,
    // never as 4/tripped).
    const resumedState = buildLinearGrowthState({
      sampleCount: 2,
      startMb: 100,
      mbPerPollDelta: 100,
      pollIntervalMs: 60_000,
      consecutiveAboveThreshold: afterBelow.consecutiveAboveThreshold,
    });
    const afterResume = computeLeakVelocity(resumedState, BASE_CONFIG);
    expect(afterResume.trip).toBe(false);
    expect(afterResume.consecutiveAboveThreshold).toBe(1);
  });

  it('returns trip:false and does not throw on a cold-start trajectory with zero samples', () => {
    const state = { trajectory: [], firstPolledAt: 0, lastPolledAt: 0, consecutiveAboveThreshold: 3 };
    let result;
    expect(() => {
      result = computeLeakVelocity(state, BASE_CONFIG);
    }).not.toThrow();

    expect(result.trip).toBe(false);
    expect(result.growthRateMbPerMin).toBeNull();
    // Cold start (unmeasurable) fully resets any inherited counter, same as
    // any other unmeasurable/below-threshold call.
    expect(result.consecutiveAboveThreshold).toBe(0);
  });

  it('returns trip:false and does not throw on a cold-start trajectory with exactly one sample (no delta is computable yet)', () => {
    const state = { trajectory: [250], firstPolledAt: 0, lastPolledAt: 0, consecutiveAboveThreshold: 2 };
    let result;
    expect(() => {
      result = computeLeakVelocity(state, BASE_CONFIG);
    }).not.toThrow();

    expect(result.trip).toBe(false);
    expect(result.growthRateMbPerMin).toBeNull();
    expect(result.consecutiveAboveThreshold).toBe(0);
  });

  it(
    'skips a null/non-finite sample without corrupting the slope — the rate is computed from the first and last VALID ' +
      'samples, ignoring a null in between, and a failed poll does NOT by itself reset an in-progress above-threshold streak',
    () => {
      // 100 -> null (failed poll) -> 500, over 2 minutes total (0, 60s, 120s).
      // The null is skipped entirely: growth is computed from 100 -> 500 over
      // the full 2-minute span = 200 MB/min, comfortably above the 10 MB/min
      // threshold, exactly as if the null were never polled at all.
      const state = {
        trajectory: [100, null, 500],
        firstPolledAt: 0,
        lastPolledAt: 120_000,
        consecutiveAboveThreshold: 2,
      };

      const result = computeLeakVelocity(state, BASE_CONFIG);

      expect(result.growthRateMbPerMin).toBe(200);
      // The failed poll did not reset the pre-existing streak of 2; this
      // computed-above-threshold call increments it to 3, not down to 0 or
      // up from a corrupted base.
      expect(result.consecutiveAboveThreshold).toBe(3);
      expect(result.trip).toBe(false);
    },
  );

  it('a trajectory of ONLY null samples is treated as an unmeasurable cold start — never throws, never trips', () => {
    const state = { trajectory: [null, null, null], firstPolledAt: 0, lastPolledAt: 120_000, consecutiveAboveThreshold: 3 };

    let result;
    expect(() => {
      result = computeLeakVelocity(state, BASE_CONFIG);
    }).not.toThrow();

    expect(result.trip).toBe(false);
    expect(result.growthRateMbPerMin).toBeNull();
    expect(result.consecutiveAboveThreshold).toBe(0);
  });

  it('does not divide by zero or produce Infinity/NaN when firstPolledAt === lastPolledAt (zero-length interval)', () => {
    const state = { trajectory: [100, 500], firstPolledAt: 5_000, lastPolledAt: 5_000, consecutiveAboveThreshold: 1 };

    const result = computeLeakVelocity(state, BASE_CONFIG);

    expect(result.growthRateMbPerMin).not.toBe(Infinity);
    expect(result.growthRateMbPerMin).not.toBe(-Infinity);
    expect(Number.isNaN(result.growthRateMbPerMin)).toBe(false);
    expect(result.trip).toBe(false);
  });

  it('does not divide by zero or produce Infinity/NaN when lastPolledAt < firstPolledAt (a non-positive/corrupt interval)', () => {
    const state = { trajectory: [100, 500], firstPolledAt: 10_000, lastPolledAt: 4_000, consecutiveAboveThreshold: 1 };

    const result = computeLeakVelocity(state, BASE_CONFIG);

    expect(result.growthRateMbPerMin).not.toBe(Infinity);
    expect(result.growthRateMbPerMin).not.toBe(-Infinity);
    expect(Number.isNaN(result.growthRateMbPerMin)).toBe(false);
    expect(result.trip).toBe(false);
    // A degenerate interval is unmeasurable, not "confidently below
    // threshold" — but either way it must fully reset, never trip.
    expect(result.consecutiveAboveThreshold).toBe(0);
  });

  it(
    'growthRateMbPerMin reflects MB/min using the actual elapsed wall-clock span (firstPolledAt/lastPolledAt), not an ' +
      'assumed fixed interval, under non-uniform poll cadence',
    () => {
      // Polls landed at t=0, t=15s, t=45s, t=150s (wildly non-uniform
      // cadence) with samples 100, 130, 160, 400 MB. Only the FIRST (100) and
      // LAST (400) samples and the FULL span (150s = 2.5min) matter for the
      // slope: (400 - 100) / 2.5 = 120 MB/min — not e.g. an assumption of a
      // fixed 10s/poll cadence, which would produce a very different number.
      const state = {
        trajectory: [100, 130, 160, 400],
        firstPolledAt: 0,
        lastPolledAt: 150_000,
        consecutiveAboveThreshold: 0,
      };

      const result = computeLeakVelocity(state, BASE_CONFIG);

      expect(result.growthRateMbPerMin).toBe(120);
    },
  );

  it(
    'a consecutivePollsRequired value at TRAJECTORY_CAP (120) still trips correctly at exactly that count — no silent ' +
      'unreachable state from a large sustained-poll requirement',
    () => {
      const config = { thresholdMbPerMin: 10, consecutivePollsRequired: TRAJECTORY_CAP };
      let result;
      let consecutiveAboveThreshold = 0;

      for (let call = 0; call < TRAJECTORY_CAP; call += 1) {
        const state = buildLinearGrowthState({
          sampleCount: 2,
          startMb: 100,
          mbPerPollDelta: 100,
          pollIntervalMs: 60_000,
          consecutiveAboveThreshold,
        });
        result = computeLeakVelocity(state, config);
        consecutiveAboveThreshold = result.consecutiveAboveThreshold;
      }

      expect(result.trip).toBe(true);
      expect(result.consecutiveAboveThreshold).toBe(0);
    },
  );

  it(
    'a consecutivePollsRequired value BEYOND TRAJECTORY_CAP (121) is still reachable via the persisted counter — the ' +
      'counter is not itself bounded by the trajectory array length cap',
    () => {
      const config = { thresholdMbPerMin: 10, consecutivePollsRequired: TRAJECTORY_CAP + 1 };
      let result;
      let consecutiveAboveThreshold = 0;

      for (let call = 0; call < TRAJECTORY_CAP; call += 1) {
        const state = buildLinearGrowthState({
          sampleCount: 2,
          startMb: 100,
          mbPerPollDelta: 100,
          pollIntervalMs: 60_000,
          consecutiveAboveThreshold,
        });
        result = computeLeakVelocity(state, config);
        consecutiveAboveThreshold = result.consecutiveAboveThreshold;
      }

      // Exactly TRAJECTORY_CAP consecutive above-threshold calls is one short
      // of TRAJECTORY_CAP + 1 — must not have tripped yet.
      expect(result.trip).toBe(false);
      expect(result.consecutiveAboveThreshold).toBe(TRAJECTORY_CAP);

      const finalState = buildLinearGrowthState({
        sampleCount: 2,
        startMb: 100,
        mbPerPollDelta: 100,
        pollIntervalMs: 60_000,
        consecutiveAboveThreshold,
      });
      const finalResult = computeLeakVelocity(finalState, config);
      expect(finalResult.trip).toBe(true);
      expect(finalResult.consecutiveAboveThreshold).toBe(0);
    },
  );

  it(
    'uses windowStartAt (the true start of the surviving trajectory window) over firstPolledAt when a trajectory of ' +
      'exactly TRAJECTORY_CAP entries represents a truncated window whose oldest sample has already been dropped',
    () => {
      // This pid has been polled for a long time: firstPolledAt is far in
      // the past (predates the surviving window by an extra 10 minutes worth
      // of already-truncated-away polls), but trajectory has been capped to
      // exactly TRAJECTORY_CAP entries by footprint-state.mjs, and
      // windowStartAt reflects the timestamp of the oldest sample that
      // actually survived the cap. The surviving window covers 119 minutes
      // (TRAJECTORY_CAP - 1 one-minute intervals): first sample 100 MB, last
      // sample 100 + 119*100 = 12000 MB -> (12000-100)/119 = 100 MB/min.
      // Using firstPolledAt instead would stretch the denominator by the
      // extra 10 minutes of truncated-away history and produce a materially
      // smaller (wrong) rate.
      const sampleCount = TRAJECTORY_CAP;
      const mbPerPollDelta = 100;
      const pollIntervalMs = 60_000;
      const trajectory = [];
      for (let i = 0; i < sampleCount; i += 1) {
        trajectory.push(100 + i * mbPerPollDelta);
      }
      const windowStartAt = 0;
      const lastPolledAt = (sampleCount - 1) * pollIntervalMs;
      const firstPolledAt = windowStartAt - 10 * 60_000; // 10 extra truncated-away minutes

      const state = { trajectory, firstPolledAt, lastPolledAt, windowStartAt, consecutiveAboveThreshold: 0 };
      const result = computeLeakVelocity(state, BASE_CONFIG);

      const expectedRate = (trajectory[trajectory.length - 1] - trajectory[0]) / ((lastPolledAt - windowStartAt) / 60_000);
      expect(result.growthRateMbPerMin).toBeCloseTo(expectedRate, 5);

      // Sanity check that this really would have differed materially had the
      // fallback (firstPolledAt) been used instead of windowStartAt.
      const rateUsingFirstPolledAt =
        (trajectory[trajectory.length - 1] - trajectory[0]) / ((lastPolledAt - firstPolledAt) / 60_000);
      expect(result.growthRateMbPerMin).not.toBeCloseTo(rateUsingFirstPolledAt, 5);
    },
  );

  it(
    'falls back to firstPolledAt when windowStartAt is absent — documenting the accepted approximation that may ' +
      'under-count elapsed rate once trajectory has truncated past TRAJECTORY_CAP',
    () => {
      const state = {
        trajectory: [100, 500],
        firstPolledAt: 0,
        lastPolledAt: 120_000,
        // windowStartAt intentionally omitted
        consecutiveAboveThreshold: 0,
      };

      const result = computeLeakVelocity(state, BASE_CONFIG);

      // (500 - 100) / 2 minutes = 200 MB/min, computed against firstPolledAt.
      expect(result.growthRateMbPerMin).toBe(200);
    },
  );

  it('is a pure function: the same input called twice yields identical output (idempotency check, not a proof of purity)', () => {
    const state = buildLinearGrowthState({
      sampleCount: 5,
      startMb: 200,
      mbPerPollDelta: 60,
      pollIntervalMs: 60_000,
      consecutiveAboveThreshold: 2,
    });

    const first = computeLeakVelocity(state, BASE_CONFIG);
    const second = computeLeakVelocity(state, BASE_CONFIG);

    expect(second).toEqual(first);
  });

  it('is idempotent/pure across a below-threshold input too, and never mutates the input state or config objects', () => {
    const state = buildLinearGrowthState({
      sampleCount: 3,
      startMb: 200,
      mbPerPollDelta: 1,
      pollIntervalMs: 60_000,
      consecutiveAboveThreshold: 3,
    });
    const stateSnapshot = JSON.parse(JSON.stringify(state));
    const configSnapshot = JSON.parse(JSON.stringify(BASE_CONFIG));

    const first = computeLeakVelocity(state, BASE_CONFIG);
    const second = computeLeakVelocity(state, BASE_CONFIG);

    expect(second).toEqual(first);
    expect(state).toEqual(stateSnapshot);
    expect(BASE_CONFIG).toEqual(configSnapshot);
  });
});

describe('DEFAULT_LEAK_VELOCITY_THRESHOLDS', () => {
  it('is exported and shaped as a plain object carrying the pinned default threshold keys', () => {
    expect(DEFAULT_LEAK_VELOCITY_THRESHOLDS).toBeDefined();
    expect(typeof DEFAULT_LEAK_VELOCITY_THRESHOLDS).toBe('object');
    expect(typeof DEFAULT_LEAK_VELOCITY_THRESHOLDS.thresholdMbPerMin).toBe('number');
    expect(typeof DEFAULT_LEAK_VELOCITY_THRESHOLDS.consecutivePollsRequired).toBe('number');
    expect(DEFAULT_LEAK_VELOCITY_THRESHOLDS.thresholdMbPerMin).toBeGreaterThan(0);
    expect(DEFAULT_LEAK_VELOCITY_THRESHOLDS.consecutivePollsRequired).toBeGreaterThan(0);
  });

  it(
    "is usable as computeLeakVelocity's config directly, without a caller having to supply any additional " +
      'field — plugging it straight into a plausible above-threshold state trips exactly as BASE_CONFIG-driven tests above do',
    () => {
      let consecutiveAboveThreshold = 0;
      let result;
      for (let call = 0; call < DEFAULT_LEAK_VELOCITY_THRESHOLDS.consecutivePollsRequired; call += 1) {
        const state = buildLinearGrowthState({
          sampleCount: 2,
          startMb: 100,
          // Comfortably above whatever the default threshold turns out to
          // be: 10x an arbitrarily large multiplier of the configured
          // threshold, computed dynamically so this test does not silently
          // start failing merely because the implementer picks a larger
          // starting-hypothesis default than 10 MB/min.
          mbPerPollDelta: Math.max(1000, DEFAULT_LEAK_VELOCITY_THRESHOLDS.thresholdMbPerMin * 10),
          pollIntervalMs: 60_000,
          consecutiveAboveThreshold,
        });
        result = computeLeakVelocity(state, DEFAULT_LEAK_VELOCITY_THRESHOLDS);
        consecutiveAboveThreshold = result.consecutiveAboveThreshold;
      }

      expect(result.trip).toBe(true);
    },
  );
});

describe('DEFAULT_LEAK_VELOCITY_THRESHOLDS doc-comment convention', () => {
  // Mirrors aimd-ceiling.mjs's own `DEFAULT_AIMD_*`-style doc-comment
  // convention (see this skill's Build Plan instructions for this phase) —
  // the constant's JSDoc must say, in some reasonable phrasing, that these
  // values are a starting hypothesis rather than a measured/tuned default.
  // Favors behavior/shape over exact prose per this ticket's own
  // instructions: this scans for ANY of several plausible phrasings rather
  // than pinning one exact string.
  let source;

  beforeAll(async () => {
    const { readFileSync } = await import('node:fs');
    const { fileURLToPath } = await import('node:url');
    const { dirname, join } = await import('node:path');
    const __dirname = dirname(fileURLToPath(import.meta.url));
    source = readFileSync(join(__dirname, 'leak-velocity.mjs'), 'utf8');
  });

  it('has a JSDoc block immediately above the DEFAULT_LEAK_VELOCITY_THRESHOLDS export', () => {
    expect(source).toMatch(/\/\*\*[\s\S]*?\*\/\s*export const DEFAULT_LEAK_VELOCITY_THRESHOLDS/);
  });

  it(
    'documents, in the comment near the export, that these default values are an unvalidated starting hypothesis ' +
      '— not a measured/tuned default — using any of several plausible phrasings',
    () => {
      const exportIndex = source.indexOf('export const DEFAULT_LEAK_VELOCITY_THRESHOLDS');
      expect(exportIndex).toBeGreaterThan(-1);

      // Look at the whole file up to and including the export — the doc
      // comment could be a block directly above it, matching this file's own
      // convention above and aimd-ceiling.mjs's `DEFAULT_AIMD_*` comments.
      const precedingSource = source.slice(0, exportIndex);

      const STARTING_HYPOTHESIS_PHRASES = [
        /starting hypothesis/i,
        /not (yet )?(measured|validated|tuned)/i,
        /unvalidated/i,
        /has not been (measured|tuned|validated)/i,
      ];

      const matchesAny = STARTING_HYPOTHESIS_PHRASES.some((pattern) => pattern.test(precedingSource));
      expect(matchesAny).toBe(true);
    },
  );
});
