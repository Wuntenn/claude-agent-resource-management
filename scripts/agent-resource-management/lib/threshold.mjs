// Phase 2 — threshold & hysteresis state machine.
//
// Pure classification functions: given a raw per-axis sample (memory or
// disk, straight from Phase 1's `takeSample()` sub-shapes), the configured
// thresholds, and the caller's own hysteresis `history`, decide the current
// state ('GREEN' | 'AMBER' | 'RED') for that axis.
//
// No I/O, no module-level mutable state. Hysteresis is threaded entirely
// through the explicit `history` argument the caller passes in and the
// `history` this module returns — nothing here remembers anything between
// calls on its own. The two axes (memory/disk) never share state; every
// call is independent given its own `history` value.

/**
 * @param {unknown} value
 * @returns {value is number}
 */
function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

/**
 * @typedef {{ value: number, sampledAt: number }} RollingReading
 */

/**
 * @typedef {{ consecutiveGreen: number, tripped: boolean, level: 'GREEN'|'AMBER'|'RED', nonGreenSince?: number, swapReadings?: RollingReading[] }} HysteresisHistory
 */

/**
 * @typedef {{ countsTowardRecovery: boolean }} ClassificationMode
 */

/** @returns {HysteresisHistory} */
function defaultHistory() {
  return { consecutiveGreen: 0, tripped: false, level: 'GREEN', nonGreenSince: undefined };
}

// Phase 5 (Part B/C) — shared rolling-window cap for both the
// memory axis's swapReadings and the disk axis's declineRateReadings. Same
// shape/cap/FIFO-eviction convention for both windows (see this module's
// header comment additions below each axis's section).
const ROLLING_WINDOW_MAX_LENGTH = 3;

// Phase 1 — floor below which `swapTotalMb` is too small a denominator
// to trust a fraction computed against it. macOS grows swap on demand, so a
// healthy host can briefly report a tiny `swapTotalMb` (a few hundred MB, or
// less right after a swapfile rolls over); any nonzero `swapUsedMb` against
// such a total produces a wildly inflated fraction that would otherwise trip
// the fractional AMBER/RED gates despite negligible absolute swap usage
//. Below this floor, `classifyMemoryRaw` skips the fractional gates
// entirely and falls back to the absolute-MB/pressureLevel checks only —
// exactly the existing fallback for `swapTotalMb` of 0/non-finite.
const MIN_CONFIDENT_SWAP_TOTAL_MB = 512;

// Phase 1 (review fix) — noise-tolerance band added to the fractional
// GREEN swap line, kept as its OWN named constant deliberately distinct from
// `MIN_CONFIDENT_SWAP_TOTAL_MB` above. The two constants answer unrelated
// questions — `MIN_CONFIDENT_SWAP_TOTAL_MB` is a floor on `swapTotalMb`'s
// magnitude ("how large must the denominator be to trust a ratio at all?"),
// while this is an absolute-MB slack added on top of a fraction-derived
// ceiling ("how much noise do we tolerate around that ceiling?"). Reusing one
// constant for both was the root cause of a review-caught bug: bumping the
// floor would have silently widened the tolerance band too, with no test
// coupling the two. See `greenSwapFractionOk` below for the clamp that keeps
// this tolerance from ever growing large enough to swallow the fractional
// gate outright.
const GREEN_FRACTION_NOISE_TOLERANCE_MB = 512;

/**
 * Pushes `{ value, sampledAt }` onto a rolling window, evicting the oldest
 * entry once the window exceeds `ROLLING_WINDOW_MAX_LENGTH` (FIFO). A
 * non-finite `value` is a no-op — the window is returned unchanged rather
 * than corrupted with a NaN/undefined entry. Never mutates the input array.
 *
 * @param {RollingReading[] | undefined} previousReadings
 * @param {unknown} value
 * @param {number} sampledAt
 * @returns {RollingReading[]}
 */
function pushRollingReading(previousReadings, value, sampledAt) {
  const readings = Array.isArray(previousReadings) ? previousReadings : [];
  if (!isFiniteNumber(value)) return readings;

  const next = [...readings, { value, sampledAt }];
  return next.length > ROLLING_WINDOW_MAX_LENGTH ? next.slice(next.length - ROLLING_WINDOW_MAX_LENGTH) : next;
}

/**
 * Median of a rolling window's `value`s. With fewer than
 * `ROLLING_WINDOW_MAX_LENGTH` readings, this is simply the median of
 * whatever IS available (a lone reading's "median" is itself, unsmoothed) —
 * not a special case, just what "median of N values" means for small N.
 *
 * @param {RollingReading[] | undefined} readings
 * @returns {number} `NaN` when the window is empty.
 */
function medianOfReadings(readings) {
  if (!Array.isArray(readings) || readings.length === 0) return NaN;

  const sorted = [...readings].map((reading) => reading.value).sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

/**
 * Recognizes only the well-formed `{ countsTowardRecovery: false }` shape as
 * a "heartbeat" (non-counting) call. Anything else — omitted, `undefined`,
 * `{ countsTowardRecovery: true }`, or any malformed/unrecognized value —
 * falls back to the default counting behavior, since a malformed `mode`
 * must never accidentally suppress trip detection or corrupt the counter.
 *
 * @param {unknown} mode
 * @returns {boolean}
 */
function isNonCountingMode(mode) {
  return (
    typeof mode === 'object' &&
    mode !== null &&
    (/** @type {{ countsTowardRecovery?: unknown }} */ (mode)).countsTowardRecovery === false
  );
}

/**
 * Applies the shared hysteresis rule to a freshly computed raw level:
 * worsening (AMBER/RED, including a stale reading forced to AMBER) always
 * takes effect immediately; recovery to GREEN only takes effect once
 * `hysteresisRecoverConsecutiveGreen` consecutive raw-GREEN samples have
 * been seen since the last non-GREEN sample. While recovery is pending, the
 * axis keeps reporting the last tripped (non-GREEN) level rather than
 * flapping to GREEN early.
 *
 * A `mode` of `{ countsTowardRecovery: false }` ("heartbeat") still detects
 * an immediate worsening to AMBER/RED, but never advances the
 * `consecutiveGreen` recovery counter on a raw-GREEN sample — the returned
 * history's counter/tripped/level are left exactly as passed in.
 *
 * @param {{ level: 'GREEN'|'AMBER'|'RED', reason: string }} raw
 * @param {HysteresisHistory | undefined} history
 * @param {{ hysteresisRecoverConsecutiveGreen: number, hysteresisRecoverWindowMs?: number }} hysteresisConfig
 * @param {ClassificationMode | undefined} [mode]
 * @param {number} [now]
 * @param {{ extraHistoryFields?: object, extraRecoveryGate?: (extraHistoryFields: object) => { satisfied: boolean, reason?: string } }} [extras]
 *   Phase 5 extension point, additive-only:
 *   - `extraHistoryFields` are merged into every returned `history` object
 *     (worsening, heartbeat, solo-GREEN, and both recovery branches) — e.g.
 *     the memory axis's `swapReadings` rolling window, maintained by the
 *     caller and threaded through unmodified by this function.
 *   - `extraRecoveryGate`, when provided, is consulted ONLY once the
 *     existing count + wall-clock gates both already pass, immediately
 *     before granting recovery to GREEN. Returning `{ satisfied: false,
 *     reason }` blocks recovery for this call (the axis keeps reporting the
 *     last tripped level, with `reason` folded into the returned message);
 *     never affects worsening detection, which always takes effect
 *     immediately above.
 * @returns {{ state: 'GREEN'|'AMBER'|'RED', reason: string, history: HysteresisHistory }}
 */
function applyHysteresis(raw, history, hysteresisConfig, mode, now, extras) {
  const previous = history ?? defaultHistory();
  const recoverAfter = hysteresisConfig?.hysteresisRecoverConsecutiveGreen ?? 1;
  const recoverWindowMs = hysteresisConfig?.hysteresisRecoverWindowMs;
  const extraHistoryFields = extras?.extraHistoryFields ?? {};

  if (raw.level !== 'GREEN') {
    return {
      state: raw.level,
      reason: raw.reason,
      history: { consecutiveGreen: 0, tripped: true, level: raw.level, nonGreenSince: now, ...extraHistoryFields },
    };
  }

  if (isNonCountingMode(mode)) {
    // Heartbeat: classify the current (GREEN) sample without advancing the
    // recovery counter. If recovery was already pending, keep reporting the
    // last tripped level; otherwise report GREEN — but the history handed
    // back is untouched from what was passed in.
    return {
      state: previous.tripped ? previous.level : 'GREEN',
      reason: previous.tripped
        ? `heartbeat: still ${previous.level} pending recovery (raw sample: ${raw.reason})`
        : raw.reason,
      history: {
        consecutiveGreen: previous.consecutiveGreen,
        tripped: previous.tripped,
        level: previous.level,
        nonGreenSince: previous.nonGreenSince,
        ...extraHistoryFields,
      },
    };
  }

  if (!previous.tripped) {
    return {
      state: 'GREEN',
      reason: raw.reason,
      history: {
        consecutiveGreen: previous.consecutiveGreen + 1,
        tripped: false,
        level: 'GREEN',
        nonGreenSince: previous.nonGreenSince,
        ...extraHistoryFields,
      },
    };
  }

  const consecutiveGreen = previous.consecutiveGreen + 1;
  const countSatisfied = consecutiveGreen >= recoverAfter;
  const windowSatisfied =
    recoverWindowMs == null || previous.nonGreenSince == null || now == null
      ? true
      : now - previous.nonGreenSince >= recoverWindowMs;

  const gateResult =
    countSatisfied && windowSatisfied && typeof extras?.extraRecoveryGate === 'function'
      ? extras.extraRecoveryGate(extraHistoryFields)
      : { satisfied: true };

  if (countSatisfied && windowSatisfied && gateResult.satisfied) {
    return {
      state: 'GREEN',
      reason: raw.reason,
      history: { consecutiveGreen, tripped: false, level: 'GREEN', nonGreenSince: previous.nonGreenSince, ...extraHistoryFields },
    };
  }

  const baseReason = countSatisfied
    ? `recovering: ${consecutiveGreen}/${recoverAfter} consecutive GREEN samples seen since last ${previous.level}, ` +
      'but not enough wall-clock time has elapsed since the trip yet'
    : `recovering: ${consecutiveGreen}/${recoverAfter} consecutive GREEN samples seen since last ${previous.level}`;

  const reason =
    countSatisfied && windowSatisfied && !gateResult.satisfied && gateResult.reason
      ? `recovering: ${consecutiveGreen}/${recoverAfter} consecutive GREEN samples and wall-clock window satisfied, but ${gateResult.reason}`
      : baseReason;

  return {
    state: previous.level,
    reason,
    history: { consecutiveGreen, tripped: true, level: previous.level, nonGreenSince: previous.nonGreenSince, ...extraHistoryFields },
  };
}

// ---------------------------------------------------------------------------
// Memory axis
// ---------------------------------------------------------------------------

/**
 * The noise-tolerant ceiling (in MB) for the fractional GREEN swap check.
 * Only ever consulted when BOTH `thresholds.greenSwapUsedMbBelow` AND
 * `thresholds.redSwapUsedFractionOfTotalAtOrAbove` are configured (see
 * `classifyMemoryRaw` below) — the red fraction line is required as the
 * reference point the tolerance is clamped against; without it there is
 * nothing to bound "how far into danger territory" the added slack is
 * allowed to reach (review, Medium finding). Clamped to the midpoint
 * between the green and red fraction lines so the tolerance can never grow
 * large enough to reach/exceed `swapTotalMb` and silently disable the
 * fractional gate.
 *
 * @param {object} thresholds
 * @param {number} swapTotalMb
 * @returns {number}
 */
function noiseTolerantGreenSwapUsedMbCeiling(thresholds, swapTotalMb) {
  const greenFractionLineMb = thresholds.greenSwapUsedFractionOfTotalBelow * swapTotalMb;
  const redFractionLineMb = thresholds.redSwapUsedFractionOfTotalAtOrAbove * swapTotalMb;
  const midpointFractionLineMb = (greenFractionLineMb + redFractionLineMb) / 2;

  return Math.min(greenFractionLineMb + GREEN_FRACTION_NOISE_TOLERANCE_MB, midpointFractionLineMb);
}

/**
 * @param {{ pressureLevel?: number, swapUsedMb?: number, swapTotalMb?: number, stale?: boolean } | null | undefined} sample
 * @param {object} thresholds
 * @returns {{ level: 'GREEN'|'AMBER'|'RED', reason: string }}
 */
function classifyMemoryRaw(sample, thresholds) {
  const pressureLevel = sample?.pressureLevel;
  const swapUsedMb = sample?.swapUsedMb;
  const swapTotalMb = sample?.swapTotalMb;

  if (!sample || sample.stale === true || !isFiniteNumber(pressureLevel) || !isFiniteNumber(swapUsedMb)) {
    return {
      level: 'AMBER',
      reason: 'memory sample is stale/unknown; treating as degraded (AMBER), never assumed healthy',
    };
  }

  // Phase 1 — relative (fraction-of-swapTotalMb) swap thresholds,
  // additive alongside the existing absolute ones. Guarded: only consulted
  // when swapTotalMb is a finite, positive number — a 0/non-finite total
  // (swap disabled/unparseable) skips the fractional checks entirely rather
  // than dividing by zero into a NaN/Infinity comparison.
  const hasUsableSwapTotal =
    isFiniteNumber(swapTotalMb) && swapTotalMb > 0 && swapTotalMb >= MIN_CONFIDENT_SWAP_TOTAL_MB;
  const swapUsedFractionOfTotal = hasUsableSwapTotal ? swapUsedMb / swapTotalMb : undefined;

  if (
    (isFiniteNumber(thresholds.redPressureAtOrAbove) && pressureLevel >= thresholds.redPressureAtOrAbove) ||
    (isFiniteNumber(thresholds.redSwapUsedMbAtOrAbove) && swapUsedMb >= thresholds.redSwapUsedMbAtOrAbove) ||
    (isFiniteNumber(thresholds.redSwapUsedFractionOfTotalAtOrAbove) &&
      swapUsedFractionOfTotal !== undefined &&
      swapUsedFractionOfTotal >= thresholds.redSwapUsedFractionOfTotalAtOrAbove)
  ) {
    return {
      level: 'RED',
      reason: `memory pressureLevel=${pressureLevel} swapUsedMb=${swapUsedMb} at/above red threshold`,
    };
  }

  const greenPressureOk =
    !isFiniteNumber(thresholds.greenPressureAtOrBelow) || pressureLevel <= thresholds.greenPressureAtOrBelow;
  const greenSwapOk =
    !isFiniteNumber(thresholds.greenSwapUsedMbBelow) || swapUsedMb < thresholds.greenSwapUsedMbBelow;
  // Phase 1 — when a threshold config corroborates the fractional
  // green line with its OWN absolute ceiling (`greenSwapUsedMbBelow`), the
  // fractional GREEN check tolerates up to GREEN_FRACTION_NOISE_TOLERANCE_MB
  // of additional swap usage above that fraction-based line before calling
  // the sample non-green. A demand-grown swapTotalMb (e.g. 3072MB) can put an
  // ordinary, healthy sample's fraction just over a static green ceiling by
  // noise-scale margins even while comfortably under the corroborating
  // absolute ceiling; treating the first GREEN_FRACTION_NOISE_TOLERANCE_MB of
  // "excess" as within that noise band avoids over-triggering AMBER on hosts
  // that were never in real distress. Deliberately does NOT apply when only
  // the fractional threshold is configured (no `greenSwapUsedMbBelow`) —
  // without a second, corroborating absolute signal there's nothing to
  // justify treating the fractional line itself as noisy, and a
  // small-swap-device host relying purely on the relative gate must
  // keep classifying strictly on the fraction, exactly as before this phase.
  //
  // (review fix) Clamped: the raw `greenFractionLineMb +
  // GREEN_FRACTION_NOISE_TOLERANCE_MB` ceiling grows with `swapTotalMb`, so
  // for a small-enough total (e.g. swapTotalMb in [512, 1024] with the
  // default 0.5 fraction + 512MB tolerance) it could reach or exceed
  // `swapTotalMb` itself — silently disabling the fractional GREEN gate
  // entirely rather than merely tolerating noise around it. The ceiling is
  // therefore capped at the midpoint between the green and red fraction
  // lines, which always stays strictly inside the AMBER band and so can
  // never swallow the gate.
  //
  // (review fix, round 2 — Medium finding) The tolerance additionally
  // requires `redSwapUsedFractionOfTotalAtOrAbove` to be configured, not just
  // `greenSwapUsedMbBelow`. The clamp above needs a genuine "how far into
  // danger territory" reference point to bound itself against; absent a
  // configured red fraction line, the previous fallback used `swapTotalMb`
  // itself as a stand-in red line, which produces a midpoint of
  // `0.75 * swapTotalMb` for the default 0.5 green fraction — a FAR larger
  // loosening than the deliberately small, named
  // `GREEN_FRACTION_NOISE_TOLERANCE_MB` (512MB) noise band this mechanism is
  // supposed to represent, and one with zero test coverage (every existing
  // fixture configures both fraction thresholds together). Rather than invent
  // a second synthetic reference point, this falls back to the strict,
  // untolerated fractional check in that configuration — exactly the
  // behaviour when only `greenSwapUsedFractionOfTotalBelow` is set with no
  // `greenSwapUsedMbBelow` corroboration at all (see the paragraph above): no
  // real red-line reference point means no principled amount of slack to add,
  // so the sample is judged on the raw fraction alone.
  const hasNoiseToleranceReferencePoint =
    isFiniteNumber(thresholds.greenSwapUsedMbBelow) &&
    isFiniteNumber(thresholds.redSwapUsedFractionOfTotalAtOrAbove);
  const greenSwapFractionOk =
    !isFiniteNumber(thresholds.greenSwapUsedFractionOfTotalBelow) ||
    swapUsedFractionOfTotal === undefined ||
    (hasNoiseToleranceReferencePoint
      ? swapUsedMb < noiseTolerantGreenSwapUsedMbCeiling(thresholds, swapTotalMb)
      : swapUsedFractionOfTotal < thresholds.greenSwapUsedFractionOfTotalBelow);

  if (greenPressureOk && greenSwapOk && greenSwapFractionOk) {
    return { level: 'GREEN', reason: 'memory pressureLevel and swapUsedMb within green thresholds' };
  }

  return {
    level: 'AMBER',
    reason: `memory pressureLevel=${pressureLevel} swapUsedMb=${swapUsedMb} above green threshold, below red threshold`,
  };
}

/**
 * Phase 5 (Part B) — swap-recovery velocity. GREEN recovery
 * additionally requires the swap window's trend (as it stands after this
 * call's own push) to be non-increasing: `window[last].value <=
 * window[0].value`. Skipped (treated satisfied) whenever the window has
 * fewer than `ROLLING_WINDOW_MAX_LENGTH` (3) entries — including a
 * pre-Phase-5 `history` with no `swapReadings` field at all — so recovery
 * never hangs waiting on trend data that doesn't exist yet. Worsening
 * (AMBER/RED) is entirely unaffected: it always takes effect immediately,
 * exactly as before this phase.
 *
 * @param {{ pressureLevel?: number, swapUsedMb?: number, stale?: boolean } | null | undefined} sample
 * @param {object} thresholds
 * @param {import('./threshold.mjs').HysteresisHistory | undefined} history
 * @param {{ hysteresisRecoverConsecutiveGreen: number, hysteresisRecoverWindowMs?: number }} hysteresisConfig
 * @param {import('./threshold.mjs').ClassificationMode | undefined} [mode]
 * @param {number} [now]
 * @returns {{ state: 'GREEN'|'AMBER'|'RED', reason: string, history: object }}
 */
export function classifyMemoryAxis(sample, thresholds, history, hysteresisConfig, mode, now) {
  const raw = classifyMemoryRaw(sample, thresholds);
  const swapReadings = pushRollingReading(history?.swapReadings, sample?.swapUsedMb, now);

  return applyHysteresis(raw, history, hysteresisConfig, mode, now, {
    extraHistoryFields: { swapReadings },
    extraRecoveryGate: (extraHistoryFields) => {
      const gateSwapReadings = extraHistoryFields.swapReadings;
      if (gateSwapReadings.length < ROLLING_WINDOW_MAX_LENGTH) return { satisfied: true };

      const first = gateSwapReadings[0].value;
      const last = gateSwapReadings[gateSwapReadings.length - 1].value;
      if (last <= first) return { satisfied: true };

      return {
        satisfied: false,
        reason: `swap trend is still rising (window: ${gateSwapReadings.map((r) => r.value).join(' -> ')}), not yet declining`,
      };
    },
  });
}

// ---------------------------------------------------------------------------
// Disk axis
// ---------------------------------------------------------------------------

/**
 * Phase 5 (Part C) — `smoothedDeclineRateGbPerHour`, the MEDIAN
 * of the disk axis's `declineRateReadings` rolling window (robust to a
 * single noisy/reversing outlier sample, unlike a mean), is substituted for
 * the raw `sample.declineRateGbPerHour` ONLY at the `hoursToFull`/
 * `amberHoursToFullBelow` steep-decline check below — the RED
 * `freeDiskGb < redFreeDiskGbBelow` and GREEN
 * `freeDiskGb > greenFreeDiskGbAbove` checks are both driven by
 * instantaneous `freeDiskGb` and are untouched by this substitution.
 *
 * @param {{ freeDiskGb?: number, declineRateGbPerHour?: number, stale?: boolean } | null | undefined} sample
 * @param {object} thresholds
 * @param {number} smoothedDeclineRateGbPerHour
 * @returns {{ level: 'GREEN'|'AMBER'|'RED', reason: string }}
 */
function classifyDiskRaw(sample, thresholds, smoothedDeclineRateGbPerHour) {
  const freeDiskGb = sample?.freeDiskGb;
  const declineRateGbPerHour = sample?.declineRateGbPerHour;

  if (!sample || sample.stale === true || !isFiniteNumber(freeDiskGb) || !isFiniteNumber(declineRateGbPerHour)) {
    return {
      level: 'AMBER',
      reason: 'disk sample is stale/unknown; treating as degraded (AMBER), never assumed healthy',
    };
  }

  if (isFiniteNumber(thresholds.redFreeDiskGbBelow) && freeDiskGb < thresholds.redFreeDiskGbBelow) {
    return { level: 'RED', reason: `disk freeDiskGb=${freeDiskGb} below red threshold ${thresholds.redFreeDiskGbBelow}` };
  }

  const effectiveDeclineRate = isFiniteNumber(smoothedDeclineRateGbPerHour)
    ? smoothedDeclineRateGbPerHour
    : declineRateGbPerHour;
  const hoursToFull = effectiveDeclineRate > 0 ? freeDiskGb / effectiveDeclineRate : Infinity;
  const steepDecline =
    isFiniteNumber(thresholds.amberHoursToFullBelow) && hoursToFull < thresholds.amberHoursToFullBelow;

  if (steepDecline) {
    return {
      level: 'AMBER',
      reason:
        `disk smoothed decline rate (median of last ${ROLLING_WINDOW_MAX_LENGTH}) projects only ` +
        `${hoursToFull.toFixed(2)}h to full, below the ${thresholds.amberHoursToFullBelow}h threshold`,
    };
  }

  const greenFreeOk = !isFiniteNumber(thresholds.greenFreeDiskGbAbove) || freeDiskGb > thresholds.greenFreeDiskGbAbove;

  if (greenFreeOk) {
    return { level: 'GREEN', reason: 'disk freeDiskGb above green threshold and decline rate not steep' };
  }

  return {
    level: 'AMBER',
    reason: `disk freeDiskGb=${freeDiskGb} below green threshold, above red threshold`,
  };
}

/**
 * @param {{ freeDiskGb?: number, declineRateGbPerHour?: number, stale?: boolean } | null | undefined} sample
 * @param {object} thresholds
 * @param {import('./threshold.mjs').HysteresisHistory | undefined} history
 * @param {{ hysteresisRecoverConsecutiveGreen: number, hysteresisRecoverWindowMs?: number }} hysteresisConfig
 * @param {import('./threshold.mjs').ClassificationMode | undefined} [mode]
 * @param {number} [now]
 * @returns {{ state: 'GREEN'|'AMBER'|'RED', reason: string, history: object }}
 */
export function classifyDiskAxis(sample, thresholds, history, hysteresisConfig, mode, now) {
  const declineRateReadings = pushRollingReading(history?.declineRateReadings, sample?.declineRateGbPerHour, now);
  const smoothedDeclineRateGbPerHour = medianOfReadings(declineRateReadings);

  const raw = classifyDiskRaw(sample, thresholds, smoothedDeclineRateGbPerHour);

  return applyHysteresis(raw, history, hysteresisConfig, mode, now, {
    extraHistoryFields: { declineRateReadings },
  });
}
