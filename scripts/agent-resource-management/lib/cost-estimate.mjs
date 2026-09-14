// Phase 3 — empirical cost estimation.
//
// Pure function: given an operation type's history (exactly the shape Phase
// 1's `readHistory()` in `./history.mjs` returns) and optional overrides,
// estimates the peak memory a future instance of that operation is likely to
// need, along with a `confidence` label so callers never overclaim precision
// they don't have. No I/O, no module-level mutable state — same posture as
// `lib/allowance.mjs` and `lib/threshold.mjs`.

// Mirrors cli.mjs's existing DEFAULT_HEADROOM_CONFIG.perAgentRamMb. Duplicated
// here (rather than imported) so lib/ never reaches back up into cli.mjs;
// callers that want a different source of truth thread it explicitly via
// `options.defaultPeakMemoryMb`.
const DEFAULT_ESTIMATE_CONFIG = {
  defaultPeakMemoryMb: 350,
};

// Fewer than this many clean (non-crashed) raw samples is not enough signal
// to call a result "empirical" — a single observation (or two) shouldn't let
// an outlier dictate the estimate. Exposed via `options` so the threshold is
// not a hidden magic number.
const DEFAULT_MIN_CLEAN_RAW_SAMPLES_FOR_EMPIRICAL = 3;

/**
 * Median of `values` (not percentile-interpolated — the exact "median of N
 * values" smoothing precedent `lib/threshold.mjs`'s `medianOfReadings`
 * already established for disk-decline-rate smoothing, reused here). Never
 * mutates the input array.
 *
 * @param {number[]} values
 * @returns {number} `NaN` for an empty array.
 */
function median(values) {
  if (values.length === 0) return NaN;

  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

/**
 * Estimates the likely peak memory cost of a future instance of
 * `operationType`, given its accumulated history. Pure — does not read from
 * disk itself; the caller is responsible for calling `readHistory()`
 * (`./history.mjs`) first and passing its result straight through.
 *
 * Evidence precedence, most-recent-first:
 *   1. `>= minCleanRawSamplesForEmpirical` clean raw observations (non-crashed,
 *      finite `peakMemoryMb`) -> MEDIAN of those peaks, `'empirical'`.
 *   2. else a populated `summary` with a finite p90 -> `summary.p90PeakMemoryMb`,
 *      `'empirical'` (the older, already-folded half of the record — used only
 *      when the retained raw window is too thin to stand on its own).
 *   3. else the flat `defaultPeakMemoryMb`, `'default'`.
 *
 * @param {string} operationType Accepted for signature parity with the
 *   caller's own per-type loop; not otherwise used by this function.
 * @param {{
 *   raw: Array<{ peakMemoryMb: number, crashed?: boolean }>,
 *   summary: { count: number, meanPeakMemoryMb: number, maxPeakMemoryMb: number, p90PeakMemoryMb: number } | null,
 * }} history Exactly the shape `readHistory()` returns.
 * @param {{ defaultPeakMemoryMb?: number, minCleanRawSamplesForEmpirical?: number }} [options]
 * @returns {{ estimatedPeakMemoryMb: number, confidence: 'default' | 'empirical', crashedObservationCount: number }}
 */
export function estimateOperationCost(operationType, history, options = {}) {
  const defaultPeakMemoryMb = options.defaultPeakMemoryMb ?? DEFAULT_ESTIMATE_CONFIG.defaultPeakMemoryMb;
  const minCleanRawSamplesForEmpirical =
    options.minCleanRawSamplesForEmpirical ?? DEFAULT_MIN_CLEAN_RAW_SAMPLES_FOR_EMPIRICAL;

  const raw = Array.isArray(history?.raw) ? history.raw : [];
  const crashedObservationCount = raw.filter((observation) => observation.crashed === true).length;
  // Non-crashed AND carrying a genuinely finite `peakMemoryMb` (pre-PR
  // review, High). `--peak-memory-mb` is an OPTIONAL flag on
  // `--record-outcome`, so a perfectly legal observation can carry no peak at
  // all; without this filter three such observations satisfied the
  // `>= minCleanRawSamplesForEmpirical` gate and `median([undefined, ...])`
  // returned `undefined` — reported as `confidence: 'empirical'` and then fed
  // to `computeHeadroomCap` as `perAgentRamMb`, whose non-finite guard clamps
  // the whole type's capacity to a permanent `0`. An observation with no peak
  // recorded carries no cost signal and must not be counted as one.
  const cleanPeaks = raw
    .filter((observation) => observation.crashed !== true && Number.isFinite(observation.peakMemoryMb))
    .map((observation) => observation.peakMemoryMb);

  // Recent RAW observations take precedence over the rolling `summary`
  // (pre-PR review, Medium). `summary` accumulates only observations
  // that have already aged OUT of `raw` past `history.mjs`'s `retentionCap`,
  // so it is strictly the OLDER half of the record. Consulting it first meant
  // that from the (retentionCap+1)-th observation onward the estimate was
  // derived exclusively from pre-cap history and never moved again, however
  // much the most recent 50 runs diverged from it — the exact opposite of the
  // evidence-based, keeps-learning behaviour this whole mechanism exists for.
  // The summary is therefore the FALLBACK: it still supplies an empirical
  // estimate when the retained raw window is too thin to stand on its own
  // (fewer than `minCleanRawSamplesForEmpirical` clean samples), which is the
  // only case where it is the best evidence available.
  if (cleanPeaks.length >= minCleanRawSamplesForEmpirical) {
    return {
      estimatedPeakMemoryMb: median(cleanPeaks),
      confidence: 'empirical',
      crashedObservationCount,
    };
  }

  if (history?.summary && Number.isFinite(history.summary.p90PeakMemoryMb)) {
    return {
      estimatedPeakMemoryMb: history.summary.p90PeakMemoryMb,
      confidence: 'empirical',
      crashedObservationCount,
    };
  }

  return {
    estimatedPeakMemoryMb: defaultPeakMemoryMb,
    confidence: 'default',
    crashedObservationCount,
  };
}
