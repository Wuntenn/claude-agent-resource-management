// RED unit tests for the not-yet-implemented
// scripts/agent-resource-management/lib/cost-estimate.mjs module
// (Phase 3 — "Empirical cost estimation").
//
// Per the Build Plan's Phase 3 section, this module owns one pure function:
//
//   estimateOperationCost(operationType, history, options?)
//     -> { estimatedPeakMemoryMb: number, confidence: 'default' | 'empirical', crashedObservationCount: number }
//
// `history` is exactly the shape Phase 1's `readHistory()` (./history.mjs)
// already returns: `{ raw: Observation[], summary: PublicSummary | null }`.
// This module does NOT read from disk itself — the caller (eventually
// cli.mjs's `--query-capacity` beat, Phase 4) is responsible for calling
// `readHistory()` first and passing its result straight through. Kept pure
// (no I/O) for the same reason lib/allowance.mjs and lib/threshold.mjs are
// pure: deterministic, no `Date.now()`/`Math.random()`, trivial to unit test.
//
// Designed function signature (documented here for the implementer, since
// lib/cost-estimate.mjs does not exist yet):
//
//   estimateOperationCost(
//     operationType: string,
//     history: { raw: Array<{ peakMemoryMb: number, crashed?: boolean, ... }>, summary: { count, meanPeakMemoryMb, maxPeakMemoryMb, p90PeakMemoryMb } | null },
//     options?: { defaultPeakMemoryMb?: number, minCleanRawSamplesForEmpirical?: number },
//   ): { estimatedPeakMemoryMb: number, confidence: 'default' | 'empirical', crashedObservationCount: number }
//
// `operationType` is accepted (matching the ticket's stated signature) but
// is not exercised by any assertion in this suite — nothing here depends on
// operationType-keyed behaviour, it is purely a pass-through label the real
// caller already has in hand from its own loop over requested types.
//
// Design decisions this suite locks in (documented here since the module
// does not exist yet — the implementer follows these, doesn't invent their
// own):
//
// 1. COLD-START (`history.summary === null` and `history.raw` contains zero
//    non-crashed observations, OR is empty outright): returns the flat
//    default `{ estimatedPeakMemoryMb: options.defaultPeakMemoryMb ??
//    DEFAULT_ESTIMATE_CONFIG.defaultPeakMemoryMb (350 — mirrors cli.mjs's
//    existing DEFAULT_HEADROOM_CONFIG.perAgentRamMb constant, duplicated
//    here rather than imported to avoid lib/ reaching back up into cli.mjs),
//    confidence: 'default' }`. `options.defaultPeakMemoryMb` lets the real
//    caller (Phase 4's cli.mjs) thread its own DEFAULT_HEADROOM_CONFIG value
//    through explicitly rather than this module silently duplicating that
//    constant's *value* as the source of truth.
//
// 2. LOW-N BOUNDARY (exactly 1 or 2 non-crashed raw observations, no
//    populated summary yet): still `confidence: 'default'`, still the flat
//    constant — deliberately NOT a "low-N empirical" variant. Per the Build
//    Plan's edge case ("single-observation shouldn't overreact to an
//    outlier"), fewer than 3 samples is not enough signal to call the result
//    "empirical" at all, so this module does not fabricate a distinct
//    third confidence tier for it. `minCleanRawSamplesForEmpirical` (default
//    3) is exposed via `options` specifically so this threshold is not a
//    hidden magic number.
//
// 3. WARMED-UP (either `history.summary` is non-null — Phase 1's history.mjs
//    only ever populates `summary` once at least one raw observation has
//    folded past the retention cap, i.e. genuine accumulated signal, however
//    small its `count` — OR `history.raw` contains >= 3 non-crashed
//    observations with no summary yet): returns `confidence: 'empirical'`.
//      - When `summary` is present: `estimatedPeakMemoryMb` is
//        `summary.p90PeakMemoryMb` — reusing the ALREADY p90-estimated,
//        reservoir-sampled statistic `history.mjs` computed at read time
//        (a conservative/safety-margin choice: p90, not the mean, so a
//        capacity estimate errs toward "enough room", matching this
//        function's own "never claim higher confidence/precision than
//        known" mandate) rather than recomputing a different statistic over
//        data this module doesn't have raw access to.
//      - When only `raw` is present (no summary yet): `estimatedPeakMemoryMb`
//        is the MEDIAN of non-crashed `raw[].peakMemoryMb` values — this is
//        the exact smoothing precedent `lib/threshold.mjs`'s
//        `classifyDiskAxis`/`medianOfReadings` already established for
//        disk-decline-rate smoothing, reused here rather than inventing a
//        second robust-statistic convention. (Not a rolling window capped at
//        3 like threshold.mjs's `ROLLING_WINDOW_MAX_LENGTH` — this operates
//        over however many raw observations `history.raw` already holds,
//        since Phase 1 already bounds that array via `retentionCap`.)
//
// 4. CRASHED OBSERVATIONS ARE TREATED DISTINCTLY: `crashed: true` raw
//    observations are EXCLUDED from both (a) the >=3-samples-for-empirical
//    threshold count, and (b) the median/estimate computation itself — a
//    crash-adjacent peak-memory reading is not representative of a normal
//    completion's cost and must not silently pull a "typical cost" estimate
//    toward runaway-crash territory. They ARE still counted and surfaced via
//    a distinct `crashedObservationCount` field on every return value (this
//    module's chosen "flagged separately, not silently averaged in" per the
//    Build Plan's requirement) so a caller can build a future warning signal
//    from it without this function inventing an opaque undocumented field.
//    A history containing ONLY crashed observations (zero clean samples) is
//    therefore cold-start-equivalent: falls through to decision #1 above,
//    `confidence: 'default'`, with `crashedObservationCount` still reporting
//    the true crash count.
//
// Do not add a stub implementation to make this pass — that defeats the
// point of the outer/phase red-green loop. The builder implements
// cost-estimate.mjs to turn this green.

import { estimateOperationCost } from './cost-estimate.mjs';

const DEFAULT_PEAK_MEMORY_MB = 350; // mirrors cli.mjs's DEFAULT_HEADROOM_CONFIG.perAgentRamMb

/**
 * Builds a minimal, valid raw observation. Only `peakMemoryMb` and
 * `crashed` matter to estimateOperationCost's contract; the rest are filled
 * in so fixtures resemble a real Phase 1 `readHistory()` raw entry.
 *
 * @param {number} peakMemoryMb
 * @param {{ crashed?: boolean }} [overrides]
 */
function observation(peakMemoryMb, overrides = {}) {
  return {
    operationType: 'test-op',
    orchestratorId: 'orch-a',
    startedAt: 0,
    endedAt: 1000,
    peakMemoryMb,
    crashed: false,
    ...overrides,
  };
}

describe('estimateOperationCost — cold start', () => {
  it('returns the flat default constant with confidence:default for a type with zero raw observations and no summary', () => {
    const history = { raw: [], summary: null };

    const result = estimateOperationCost('cold-type', history);

    expect(result).toEqual({
      estimatedPeakMemoryMb: DEFAULT_PEAK_MEMORY_MB,
      confidence: 'default',
      crashedObservationCount: 0,
    });
  });

  it('honours an explicit options.defaultPeakMemoryMb override on cold start', () => {
    const history = { raw: [], summary: null };

    const result = estimateOperationCost('cold-type', history, { defaultPeakMemoryMb: 512 });

    expect(result.estimatedPeakMemoryMb).toBe(512);
    expect(result.confidence).toBe('default');
  });
});

describe('estimateOperationCost — low-N boundary (documented: still default, not a third tier)', () => {
  it('exactly 1 clean raw observation stays confidence:default, not empirical', () => {
    const history = { raw: [observation(300)], summary: null };

    const result = estimateOperationCost('low-n-type', history);

    expect(result.confidence).toBe('default');
    expect(result.estimatedPeakMemoryMb).toBe(DEFAULT_PEAK_MEMORY_MB);
  });

  it('exactly 2 clean raw observations stays confidence:default, not empirical', () => {
    const history = { raw: [observation(300), observation(320)], summary: null };

    const result = estimateOperationCost('low-n-type', history);

    expect(result.confidence).toBe('default');
    expect(result.estimatedPeakMemoryMb).toBe(DEFAULT_PEAK_MEMORY_MB);
  });

  it('never claims confidence:empirical while backed by fewer than 3 clean samples and no summary', () => {
    const zero = estimateOperationCost('none', { raw: [], summary: null });
    const one = estimateOperationCost('one', { raw: [observation(300)], summary: null });
    const two = estimateOperationCost('two', { raw: [observation(300), observation(310)], summary: null });

    for (const result of [zero, one, two]) {
      expect(result.confidence).not.toBe('empirical');
    }
  });
});

describe('estimateOperationCost — warmed up (raw-only, >= 3 clean samples)', () => {
  it('returns confidence:empirical once >= 3 clean raw observations exist', () => {
    const history = {
      raw: [observation(280), observation(300), observation(310), observation(295), observation(305)],
      summary: null,
    };

    const result = estimateOperationCost('warm-type', history);

    expect(result.confidence).toBe('empirical');
  });

  it('derives the estimate from the real data (median of clean observations), not the flat default', () => {
    // Median of [280, 295, 300, 305, 310] is 300 — chosen deliberately
    // distinct from the 350 flat default so the two can never accidentally
    // coincide in this fixture.
    const history = {
      raw: [observation(280), observation(300), observation(310), observation(295), observation(305)],
      summary: null,
    };

    const result = estimateOperationCost('warm-type', history);

    expect(result.estimatedPeakMemoryMb).toBe(300);
  });
});

describe('estimateOperationCost — warmed up (populated summary)', () => {
  it('returns confidence:empirical whenever history.summary is non-null, regardless of raw length', () => {
    const history = {
      raw: [],
      summary: { count: 12, meanPeakMemoryMb: 320, maxPeakMemoryMb: 410, p90PeakMemoryMb: 380 },
    };

    const result = estimateOperationCost('summary-type', history);

    expect(result.confidence).toBe('empirical');
  });

  it('uses summary.p90PeakMemoryMb (a conservative, already-robust statistic) as the estimate when a summary is present', () => {
    const history = {
      raw: [],
      summary: { count: 12, meanPeakMemoryMb: 320, maxPeakMemoryMb: 410, p90PeakMemoryMb: 380 },
    };

    const result = estimateOperationCost('summary-type', history);

    expect(result.estimatedPeakMemoryMb).toBe(380);
  });
});

// ---------------------------------------------------------------------------
// pre-PR review regressions — cross-phase interactions no phase-scoped
// review saw, because each involves BOTH Phase 1's history.mjs retention/fold
// and Phase 3's estimate precedence at once.
// ---------------------------------------------------------------------------

describe('estimateOperationCost — recent raw evidence outranks the older folded summary', () => {
  it('prefers the median of >= 3 clean raw samples over a populated summary derived from older, aged-out data', () => {
    // `summary` only ever holds observations that already aged PAST the
    // retention cap, i.e. strictly the older half of the record. If it won,
    // the estimate would freeze at pre-cap history forever.
    const history = {
      raw: [observation(200), observation(200), observation(210)],
      summary: { count: 4, meanPeakMemoryMb: 1400, maxPeakMemoryMb: 5000, p90PeakMemoryMb: 3560 },
    };

    const result = estimateOperationCost('drifted-type', history);

    expect(result.estimatedPeakMemoryMb).toBe(200);
    expect(result.confidence).toBe('empirical');
  });

  it('still falls back to the summary when the retained raw window is too thin to stand on its own', () => {
    const history = {
      raw: [observation(200)],
      summary: { count: 4, meanPeakMemoryMb: 1400, maxPeakMemoryMb: 5000, p90PeakMemoryMb: 3560 },
    };

    expect(estimateOperationCost('thin-type', history).estimatedPeakMemoryMb).toBe(3560);
  });
});

describe('estimateOperationCost — observations carrying no peakMemoryMb are not cost evidence', () => {
  it('does not let three peak-less observations satisfy the empirical threshold with an undefined estimate', () => {
    // `--peak-memory-mb` is OPTIONAL on `--record-outcome`, so this history is
    // entirely legal. Previously it produced
    // `{ estimatedPeakMemoryMb: undefined, confidence: 'empirical' }`, which
    // `computeHeadroomCap` clamps to a permanent freeCapacity of 0.
    const history = {
      raw: [{ orchestratorId: 'o' }, { orchestratorId: 'o' }, { orchestratorId: 'o' }],
      summary: null,
    };

    const result = estimateOperationCost('peakless-type', history);

    expect(result.confidence).toBe('default');
    expect(Number.isFinite(result.estimatedPeakMemoryMb)).toBe(true);
    expect(result.estimatedPeakMemoryMb).toBe(350);
  });

  it('ignores peak-less observations when counting clean samples alongside real ones', () => {
    const history = {
      raw: [observation(300), { orchestratorId: 'o' }, observation(320), { orchestratorId: 'o' }],
      summary: null,
    };

    // Only two REAL peaks — below the >= 3 empirical threshold.
    expect(estimateOperationCost('mixed-type', history).confidence).toBe('default');
  });

  it('never returns a non-finite estimate from a NaN-poisoned summary', () => {
    const history = {
      raw: [],
      summary: { count: 3, meanPeakMemoryMb: NaN, maxPeakMemoryMb: NaN, p90PeakMemoryMb: NaN },
    };

    const result = estimateOperationCost('poisoned-type', history);

    expect(result.confidence).toBe('default');
    expect(result.estimatedPeakMemoryMb).toBe(350);
  });
});

describe('estimateOperationCost — single outlier does not dominate once >= 3 samples exist', () => {
  it('does not let one 5000MB outlier drag the estimate anywhere near it, among 5 normal ~300MB observations', () => {
    const history = {
      raw: [
        observation(290),
        observation(300),
        observation(310),
        observation(295),
        observation(305),
        observation(5000), // the outlier
      ],
      summary: null,
    };

    const result = estimateOperationCost('outlier-type', history);

    expect(result.confidence).toBe('empirical');
    // A mean over these 6 values would be ~1450MB; a correctly
    // median-smoothed estimate stays close to the ~300MB cluster.
    expect(result.estimatedPeakMemoryMb).toBeLessThan(400);
    expect(result.estimatedPeakMemoryMb).toBeGreaterThan(200);
  });
});

describe('estimateOperationCost — crashed observations treated distinctly', () => {
  it('excludes crashed observations from the central estimate, using only clean completions', () => {
    const history = {
      raw: [
        observation(295),
        observation(300),
        observation(305),
        observation(310),
        observation(4000, { crashed: true }), // crash-adjacent runaway reading
      ],
      summary: null,
    };

    const result = estimateOperationCost('crash-mixed-type', history);

    expect(result.confidence).toBe('empirical');
    // Median of the 4 CLEAN values [295, 300, 305, 310] is 302.5 — nowhere
    // near the excluded 4000MB crash reading.
    expect(result.estimatedPeakMemoryMb).toBe(302.5);
  });

  it('surfaces the crash count via a distinct field rather than silently averaging crashes in', () => {
    const history = {
      raw: [observation(300), observation(305), observation(310), observation(1000, { crashed: true })],
      summary: null,
    };

    const result = estimateOperationCost('crash-mixed-type', history);

    expect(result.crashedObservationCount).toBe(1);
  });

  it('a history of ONLY crashed observations is cold-start-equivalent: confidence:default, but still reports the crash count', () => {
    const history = {
      raw: [
        observation(1000, { crashed: true }),
        observation(1200, { crashed: true }),
        observation(900, { crashed: true }),
      ],
      summary: null,
    };

    const result = estimateOperationCost('all-crashed-type', history);

    expect(result.confidence).toBe('default');
    expect(result.estimatedPeakMemoryMb).toBe(DEFAULT_PEAK_MEMORY_MB);
    expect(result.crashedObservationCount).toBe(3);
  });

  it('crashed observations alone never push a type from default to empirical (fewer than 3 clean + several crashed)', () => {
    const history = {
      raw: [
        observation(300), // 1 clean
        observation(2000, { crashed: true }),
        observation(2500, { crashed: true }),
        observation(3000, { crashed: true }),
      ],
      summary: null,
    };

    const result = estimateOperationCost('mostly-crashed-type', history);

    expect(result.confidence).toBe('default');
    expect(result.crashedObservationCount).toBe(3);
  });
});

// ---------------------------------------------------------------------------
// Phase 3 — pinning cost-estimate.mjs's behaviour UNCHANGED in the
// presence of the new additive-only observation fields (`agentClass`,
// `footprintTrajectory`) that `lib/history.mjs`'s `recordObservation` now
// accepts. `estimateOperationCost` is explicitly OUT of scope for (no
// code changes to this file) — it must produce byte-identical output given
// the same `peakMemoryMb`/`crashed` values regardless of whether the newer,
// unrelated fields are present on the raw entries it's fed.
// ---------------------------------------------------------------------------

describe('estimateOperationCost — Phase 3: unaffected by the new agentClass/footprintTrajectory fields', () => {
  it('produces byte-identical output for two observation sets differing ONLY in whether new optional fields are present', () => {
    const peaks = [300, 320, 340];

    const withoutNewFields = {
      raw: peaks.map((peak) => observation(peak)),
      summary: null,
    };

    const withNewFields = {
      raw: peaks.map((peak, index) =>
        observation(peak, {
          agentClass: `builder-${index}`,
          footprintTrajectory: [peak - 20, peak - 10, peak],
        }),
      ),
      summary: null,
    };

    const resultWithout = estimateOperationCost('footprint-type', withoutNewFields);
    const resultWith = estimateOperationCost('footprint-type', withNewFields);

    expect(resultWith).toEqual(resultWithout);
  });
});

describe('estimateOperationCost — never overclaims precision', () => {
  it('confidence is always exactly one of the two documented literals, never a third value', () => {
    const cases = [
      { raw: [], summary: null },
      { raw: [observation(300)], summary: null },
      { raw: [observation(300), observation(310), observation(320)], summary: null },
      { raw: [], summary: { count: 1, meanPeakMemoryMb: 300, maxPeakMemoryMb: 300, p90PeakMemoryMb: 300 } },
    ];

    for (const history of cases) {
      const result = estimateOperationCost('any-type', history);
      expect(['default', 'empirical']).toContain(result.confidence);
    }
  });
});
