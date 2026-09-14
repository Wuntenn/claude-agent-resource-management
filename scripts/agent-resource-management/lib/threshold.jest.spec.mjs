// Phase 2 — threshold & hysteresis state machine. RED by construction:
// `./lib/threshold.mjs` does not exist yet — this file only asserts the
// contract it must satisfy. Do not add a stub implementation to turn this
// green; that's the next (builder) step.
//
// Contract under test (per the Build Plan's Phase 2 acceptance criteria):
//
//   classifyMemoryAxis(sample, thresholds, history, config) -> { level, reason, history }
//   classifyDiskAxis(sample, thresholds, history, config)   -> { level, reason, history }
//
// - `level` is one of 'GREEN' | 'AMBER' | 'RED'.
// - `reason` is a short string naming what tripped the level (observability).
// - Hysteresis (consecutive-GREEN recovery counter) is threaded through via
//   the explicit `history` argument in and the returned `history` out — no
//   module-level mutable state. Callers carry `history` forward themselves,
//   exactly as the outer acceptance test's `runBeat()` helper does.
// - All threshold values (memory/disk bands, hysteresis N) come from the
//   `thresholds`/`config` arguments, never hardcoded.
// - A `stale: true` sample must classify as at-least-AMBER on the affected
//   axis, never GREEN — the single most safety-critical property in this
//   phase (Investigation: "must not be silently read as GREEN downstream").
//
// The outer acceptance test (agent-resource-management.jest.spec.mjs) calls
// these two functions with a fourth positional arg
// `{ hysteresisRecoverConsecutiveGreen }` — this file matches that exact call
// shape so Phase 2, once implemented, also turns the outer test green.

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { classifyMemoryAxis, classifyDiskAxis } from './threshold.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const thresholdSource = readFileSync(join(__dirname, 'threshold.mjs'), 'utf8');

// ---------------------------------------------------------------------------
// Suggested default thresholds — Investigation briefing §7, documented here
// as the fixture every test builds from (never inlined as magic numbers in
// the module itself).
// ---------------------------------------------------------------------------

const MEMORY_THRESHOLDS = {
  greenPressureAtOrBelow: 1,
  greenSwapUsedMbBelow: 4096,
  amberPressureAtOrBelow: 2,
  amberSwapUsedMbBelow: 8192,
  redPressureAtOrAbove: 4,
  redSwapUsedMbAtOrAbove: 10240,
};

const DISK_THRESHOLDS = {
  greenFreeDiskGbAbove: 30,
  amberFreeDiskGbBelow: 20,
  amberHoursToFullBelow: 2,
  redFreeDiskGbBelow: 10,
};

const HYSTERESIS_CONFIG = { hysteresisRecoverConsecutiveGreen: 3 };

// ---------------------------------------------------------------------------
// Sample fixtures (mirrors the outer acceptance test's fixtures, and Phase
// 1's `takeSample()` sub-shapes).
// ---------------------------------------------------------------------------

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 };
const MEMORY_AMBER = { pressureLevel: 2, swapUsedMb: 5120, compressedMb: 2048 };
const MEMORY_RED = { pressureLevel: 4, swapUsedMb: 12288, compressedMb: 4096 };
const MEMORY_STALE = { stale: true };

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const DISK_AMBER_LOW_FREE = { freeDiskGb: 15, declineRateGbPerHour: 0.5 };
const DISK_AMBER_STEEP_DECLINE = { freeDiskGb: 50, declineRateGbPerHour: 30 }; // <2h to full despite plenty free
const DISK_RED = { freeDiskGb: 5, declineRateGbPerHour: 1 };
const DISK_STALE = { stale: true };

// ---------------------------------------------------------------------------
// Basic per-axis classification from documented defaults
// ---------------------------------------------------------------------------

describe('classifyMemoryAxis — basic classification from documented default thresholds', () => {
  it('classifies a healthy sample as GREEN', () => {
    const result = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(result.state).toBe('GREEN');
    expect(typeof result.reason).toBe('string');
  });

  it('classifies a moderately pressured sample as AMBER, naming the reason', () => {
    const result = classifyMemoryAxis(MEMORY_AMBER, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(result.state).toBe('AMBER');
    expect(result.reason).toEqual(expect.any(String));
    expect(result.reason.length).toBeGreaterThan(0);
  });

  it('classifies a severely pressured sample as RED', () => {
    const result = classifyMemoryAxis(MEMORY_RED, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(result.state).toBe('RED');
  });
});

describe('classifyDiskAxis — basic classification from documented default thresholds', () => {
  it('classifies plentiful free disk as GREEN', () => {
    const result = classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(result.state).toBe('GREEN');
  });

  it('classifies low absolute free disk as AMBER', () => {
    const result = classifyDiskAxis(DISK_AMBER_LOW_FREE, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(result.state).toBe('AMBER');
  });

  it('classifies critically low free disk as RED', () => {
    const result = classifyDiskAxis(DISK_RED, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(result.state).toBe('RED');
  });

  it('classifies AMBER on a steep decline-rate projecting <2h to full, even with plenty of absolute free disk', () => {
    // freeDiskGb=50 is well above the 30GB GREEN floor on its own, but a
    // decline rate of 30GB/hour projects under 2 hours to zero — this must
    // trip AMBER on the rate-of-change signal alone, not the instantaneous
    // free-space value.
    const result = classifyDiskAxis(DISK_AMBER_STEEP_DECLINE, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(result.state).toBe('AMBER');
    expect(result.reason).toMatch(/hour|decline|rate|full/i);
  });
});

// ---------------------------------------------------------------------------
// Independence of the two axes — no cross-contamination
// ---------------------------------------------------------------------------

describe('axis independence — memory and disk are classified without cross-contamination', () => {
  it('memory RED + disk GREEN classify independently in the same beat', () => {
    const memoryResult = classifyMemoryAxis(MEMORY_RED, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    const diskResult = classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    expect(memoryResult.state).toBe('RED');
    expect(diskResult.state).toBe('GREEN');
  });

  it('memory GREEN + disk RED classify independently in the same beat (the reverse matrix cell)', () => {
    const memoryResult = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    const diskResult = classifyDiskAxis(DISK_RED, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    expect(memoryResult.state).toBe('GREEN');
    expect(diskResult.state).toBe('RED');
  });

  it('memory AMBER + disk AMBER for unrelated reasons do not bleed reasons across axes', () => {
    const memoryResult = classifyMemoryAxis(MEMORY_AMBER, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    const diskResult = classifyDiskAxis(DISK_AMBER_STEEP_DECLINE, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    expect(memoryResult.state).toBe('AMBER');
    expect(diskResult.state).toBe('AMBER');
    // Each axis's reason should speak to its own signal, not the other axis's.
    expect(memoryResult.reason).not.toMatch(/disk|free space/i);
    expect(diskResult.reason).not.toMatch(/swap|pressure/i);
  });
});

// ---------------------------------------------------------------------------
// Stale sample handling — safety-critical: never silently GREEN
// ---------------------------------------------------------------------------

describe('stale sample handling — must never classify as GREEN', () => {
  it('classifies a stale memory sample as AMBER or RED, never GREEN', () => {
    const result = classifyMemoryAxis(MEMORY_STALE, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(['AMBER', 'RED']).toContain(result.state);
    expect(result.reason).toMatch(/stale|unknown/i);
  });

  it('classifies a stale disk sample as AMBER or RED, never GREEN', () => {
    const result = classifyDiskAxis(DISK_STALE, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(['AMBER', 'RED']).toContain(result.state);
    expect(result.reason).toMatch(/stale|unknown/i);
  });

  it('a stale memory sample stays at-least-AMBER even carried forward from a prior GREEN history', () => {
    // Simulates a beat sequence: GREEN, GREEN, then a stale sample — a
    // healthy recent history must not cause the stale reading itself to be
    // interpreted as GREEN.
    const first = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    const second = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, first.history, HYSTERESIS_CONFIG);
    const staleBeat = classifyMemoryAxis(MEMORY_STALE, MEMORY_THRESHOLDS, second.history, HYSTERESIS_CONFIG);

    expect(staleBeat.state).not.toBe('GREEN');
  });

  it('a stale disk sample stays at-least-AMBER even carried forward from a prior RED history', () => {
    const first = classifyDiskAxis(DISK_RED, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    const staleBeat = classifyDiskAxis(DISK_STALE, DISK_THRESHOLDS, first.history, HYSTERESIS_CONFIG);

    expect(staleBeat.state).not.toBe('GREEN');
  });
});

// ---------------------------------------------------------------------------
// Hysteresis — explicit history argument threaded through, no hidden state
// ---------------------------------------------------------------------------

describe('hysteresis — history is threaded explicitly, not held as hidden module state', () => {
  it('does not resume from RED after only a single GREEN sample sandwiched between AMBER samples', () => {
    // RED -> GREEN (1 good sample) -> AMBER: the single good sample must not
    // reset/clear the hysteresis-tripped state — it must not "resume" GREEN
    // just because the very next-next sample looks bad again either. The
    // trip must still require a fresh run of N consecutive GREEN samples.
    const tripped = classifyMemoryAxis(MEMORY_RED, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(tripped.state).toBe('RED');

    const oneGreen = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, tripped.history, HYSTERESIS_CONFIG);
    // A single GREEN sample right after RED is not enough to recover.
    expect(oneGreen.state).not.toBe('GREEN');

    const backToAmber = classifyMemoryAxis(MEMORY_AMBER, MEMORY_THRESHOLDS, oneGreen.history, HYSTERESIS_CONFIG);
    // The dip back to AMBER must not have been treated as a resumed-GREEN
    // state having been silently granted by the one good sample.
    expect(backToAmber.state).not.toBe('GREEN');
    expect(backToAmber.state).toBe('AMBER');
  });

  it('recovers to GREEN only on the 3rd consecutive GREEN sample after RED, not the 1st or 2nd', () => {
    const red = classifyMemoryAxis(MEMORY_RED, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(red.state).toBe('RED');

    const firstGreen = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, red.history, HYSTERESIS_CONFIG);
    expect(firstGreen.state).not.toBe('GREEN');

    const secondGreen = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, firstGreen.history, HYSTERESIS_CONFIG);
    expect(secondGreen.state).not.toBe('GREEN');

    const thirdGreen = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, secondGreen.history, HYSTERESIS_CONFIG);
    expect(thirdGreen.state).toBe('GREEN');
  });

  it('threads hysteresis state through an explicit history argument that the caller carries forward itself (no hidden internal counter)', () => {
    // Prove there is no module-level mutable state: interleave two entirely
    // separate call chains (as if two independent axes/orchestrators) using
    // their own `history` values, and confirm one chain's progress never
    // leaks into the other's classification.
    const redA = classifyMemoryAxis(MEMORY_RED, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    const greenB1 = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    const greenB2 = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, greenB1.history, HYSTERESIS_CONFIG);
    const greenB3 = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, greenB2.history, HYSTERESIS_CONFIG);

    // Chain B recovers to GREEN on its own 3rd consecutive GREEN sample...
    expect(greenB3.state).toBe('GREEN');

    // ...while chain A, evaluated after chain B's calls (interleaved calls,
    // shared process), is still RED from its own independent history — this
    // would only be possible if history is genuinely per-call-chain, not a
    // shared module-level counter.
    const stillRedA = classifyMemoryAxis(MEMORY_RED, MEMORY_THRESHOLDS, redA.history, HYSTERESIS_CONFIG);
    expect(stillRedA.state).toBe('RED');
  });

  it('applies the same N-consecutive-GREEN recovery rule to the disk axis independently of memory', () => {
    const red = classifyDiskAxis(DISK_RED, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(red.state).toBe('RED');

    const firstGreen = classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, red.history, HYSTERESIS_CONFIG);
    expect(firstGreen.state).not.toBe('GREEN');

    const secondGreen = classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, firstGreen.history, HYSTERESIS_CONFIG);
    expect(secondGreen.state).not.toBe('GREEN');

    const thirdGreen = classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, secondGreen.history, HYSTERESIS_CONFIG);
    expect(thirdGreen.state).toBe('GREEN');
  });
});

// ---------------------------------------------------------------------------
// Configurability — thresholds and hysteresis N are parameters, never
// hardcoded magic numbers in the module itself.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Heartbeat / non-counting classification — RED by construction.
//
// classifyMemoryAxis/classifyDiskAxis gain a 5th, optional positional
// argument: `mode`, shaped `{ countsTowardRecovery: boolean }`.
//
//   classifyMemoryAxis(sample, thresholds, history, hysteresisConfig, mode?)
//
// - Omitted (undefined) `mode`, or `{ countsTowardRecovery: true }`, is
//   IDENTICAL to today's behaviour byte-for-byte — a normal "beat" call that
//   both classifies the current sample AND advances the hysteresis
//   `consecutiveGreen` recovery counter on GREEN, exactly as
//   `applyHysteresis` does today.
// - `{ countsTowardRecovery: false }` is a "heartbeat" call: it classifies
//   the current sample (worsening to AMBER/RED still takes effect
//   immediately — trip detection is never suppressed) but must NOT advance
//   `consecutiveGreen` when the raw level is GREEN. The returned history's
//   `consecutiveGreen` (and `tripped`/`level`) must equal the history that
//   was passed in, unchanged, so a caller who discards a heartbeat's
//   returned history (or discards it, since heartbeats do not update the
//   caller's carried-forward history at all) can equally re-derive it from
//   the input history.
// - Any `mode` value that isn't a recognized shape (missing the field,
//   wrong type, or an unrecognized value) falls back to default (counting)
//   behaviour rather than throwing or silently miscounting — this is the
//   safe default because a malformed `mode` must never accidentally
//   *suppress* trip detection or corrupt the counter.
//
// This file only asserts the contract the next (builder) phase must
// satisfy. Do not add a stub implementation of `mode` to turn these green;
// that's the next agent's job.
// ---------------------------------------------------------------------------

describe('heartbeat / non-counting classification — mode: { countsTowardRecovery }', () => {
  it('a non-counting-mode call observing GREEN does NOT increment consecutiveGreen', () => {
    const red = classifyMemoryAxis(MEMORY_RED, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(red.state).toBe('RED');

    const realBeat = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, red.history, HYSTERESIS_CONFIG);
    expect(realBeat.state).not.toBe('GREEN');
    expect(realBeat.history.consecutiveGreen).toBe(1);

    const heartbeat = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, realBeat.history, HYSTERESIS_CONFIG, {
      countsTowardRecovery: false,
    });

    // The heartbeat observes GREEN too, but must not advance the counter.
    expect(heartbeat.history.consecutiveGreen).toBe(realBeat.history.consecutiveGreen);
    expect(heartbeat.history.consecutiveGreen).toBe(1);
    expect(heartbeat.state).not.toBe('GREEN');
  });

  it('a non-counting-mode call immediately reflects a newly-tripped AMBER/RED (trip detection is not suppressed)', () => {
    const green = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(green.state).toBe('GREEN');

    const heartbeatTrip = classifyMemoryAxis(MEMORY_RED, MEMORY_THRESHOLDS, green.history, HYSTERESIS_CONFIG, {
      countsTowardRecovery: false,
    });

    // Worsening takes effect immediately regardless of mode.
    expect(heartbeatTrip.state).toBe('RED');
    expect(heartbeatTrip.history.tripped).toBe(true);
    expect(heartbeatTrip.history.level).toBe('RED');

    // Same guarantee on the disk axis.
    const greenDisk = classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    const heartbeatTripDisk = classifyDiskAxis(DISK_RED, DISK_THRESHOLDS, greenDisk.history, HYSTERESIS_CONFIG, {
      countsTowardRecovery: false,
    });
    expect(heartbeatTripDisk.state).toBe('RED');
  });

  it('omitting the mode parameter preserves exact current behaviour (default/counting path unchanged)', () => {
    // Same call, once with no 5th argument at all, once with the explicit
    // default — both must be indistinguishable from each other, and from
    // today's documented behaviour (asserted throughout this file already).
    const withoutMode = classifyMemoryAxis(MEMORY_AMBER, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    const withExplicitDefault = classifyMemoryAxis(MEMORY_AMBER, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG, {
      countsTowardRecovery: true,
    });

    expect(withExplicitDefault).toEqual(withoutMode);
    expect(withoutMode.state).toBe('AMBER');
  });

  it('interleaves heartbeat (non-counting) and real beats: recovery is reached only via 3 real GREEN beats', () => {
    const red = classifyMemoryAxis(MEMORY_RED, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(red.state).toBe('RED');

    const realBeat1 = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, red.history, HYSTERESIS_CONFIG);
    expect(realBeat1.state).not.toBe('GREEN');
    expect(realBeat1.history.consecutiveGreen).toBe(1);

    const heartbeat1 = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, realBeat1.history, HYSTERESIS_CONFIG, {
      countsTowardRecovery: false,
    });
    expect(heartbeat1.state).not.toBe('GREEN');
    expect(heartbeat1.history.consecutiveGreen).toBe(1);

    const realBeat2 = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, heartbeat1.history, HYSTERESIS_CONFIG);
    expect(realBeat2.state).not.toBe('GREEN');
    expect(realBeat2.history.consecutiveGreen).toBe(2);

    const heartbeat2 = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, realBeat2.history, HYSTERESIS_CONFIG, {
      countsTowardRecovery: false,
    });
    expect(heartbeat2.state).not.toBe('GREEN');
    expect(heartbeat2.history.consecutiveGreen).toBe(2);

    const realBeat3 = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, heartbeat2.history, HYSTERESIS_CONFIG);
    expect(realBeat3.state).toBe('GREEN');
    expect(realBeat3.history.consecutiveGreen).toBe(3);
  });

  it('a non-counting call with undefined prior history (first-ever call) does not throw and mirrors the default first-observation shape', () => {
    expect(() =>
      classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG, {
        countsTowardRecovery: false,
      }),
    ).not.toThrow();

    const heartbeatFirst = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG, {
      countsTowardRecovery: false,
    });

    expect(heartbeatFirst.state).toBe('GREEN');
    expect(heartbeatFirst).toEqual(
      expect.objectContaining({
        state: 'GREEN',
        reason: expect.any(String),
        history: expect.objectContaining({ tripped: false, level: 'GREEN' }),
      }),
    );
    // Non-counting: consecutiveGreen must not have advanced off its
    // starting value of 0, unlike a real (counting) first call which
    // advances it to 1.
    expect(heartbeatFirst.history.consecutiveGreen).toBe(0);
  });

  it('an unrecognized/invalid mode value falls back safely to default (counting) behaviour', () => {
    const red = classifyMemoryAxis(MEMORY_RED, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    // A string, a plain empty object, and null are all malformed `mode`
    // values — none should throw, and none should suppress trip detection
    // or silently behave as non-counting.
    for (const invalidMode of ['bogus', {}, null, { countsTowardRecovery: 'not-a-boolean' }]) {
      expect(() =>
        classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, red.history, HYSTERESIS_CONFIG, invalidMode),
      ).not.toThrow();

      const result = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, red.history, HYSTERESIS_CONFIG, invalidMode);
      // Falls back to counting: behaves exactly like the real (counting)
      // call already asserted above — advances to consecutiveGreen: 1.
      expect(result.history.consecutiveGreen).toBe(1);
      expect(result.state).not.toBe('GREEN'); // still recovering, not silently GREEN
    }
  });

  it('an unrecognized/invalid mode value falls back safely to default (counting) behaviour — disk axis', () => {
    const red = classifyDiskAxis(DISK_RED, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    // Same malformed-mode contract as the memory axis, exercised here for
    // symmetry since both axes share the same applyHysteresis implementation.
    for (const invalidMode of ['bogus', {}, null, { countsTowardRecovery: 'not-a-boolean' }]) {
      expect(() =>
        classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, red.history, HYSTERESIS_CONFIG, invalidMode),
      ).not.toThrow();

      const result = classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, red.history, HYSTERESIS_CONFIG, invalidMode);
      expect(result.history.consecutiveGreen).toBe(1);
      expect(result.state).not.toBe('GREEN'); // still recovering, not silently GREEN
    }
  });

  it('a heartbeat during a tripped recovery-pending window reports a reason consistent with the reported (stale) state, not the raw sample', () => {
    const red = classifyMemoryAxis(MEMORY_RED, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    const heartbeat = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, red.history, HYSTERESIS_CONFIG, {
      countsTowardRecovery: false,
    });

    expect(heartbeat.state).toBe('RED');
    // The reason must describe the reported (stale, tripped) state — not
    // read as if the axis were currently healthy.
    expect(heartbeat.reason).toMatch(/RED/);
    expect(heartbeat.reason).toMatch(/pending recovery/);
    expect(heartbeat.reason).not.toBe(
      'memory pressureLevel and swapUsedMb within green thresholds',
    );
  });
});

// ---------------------------------------------------------------------------
// Wall-clock hysteresis recovery — RED by construction.
//
// Today, `applyHysteresis`'s recovery-to-GREEN rule is PURELY count-based:
// `hysteresisRecoverConsecutiveGreen` consecutive raw-GREEN samples recover
// the axis, regardless of how much real time elapsed between those samples.
// This section pins an ADDITIVE change the next (builder) phase must
// implement, without altering any behaviour asserted above:
//
//   - `HysteresisHistory` gains a new timestamp field, `nonGreenSince`
//     (number | undefined) — the `now` at which the axis most recently
//     transitioned to a non-GREEN raw level. It is (re-)stamped on EVERY
//     worsening transition (raw.level !== 'GREEN'), not just the first.
//   - `classifyMemoryAxis`/`classifyDiskAxis` (and, via them, the module's
//     private `applyHysteresis`) gain a 6th positional argument, `now`
//     (epoch ms), threaded explicitly by the caller — matching this file's
//     existing "no `Date.now()` inside, always take `now` as an argument"
//     convention. It is positioned after the existing `mode` argument:
//
//       classifyMemoryAxis(sample, thresholds, history, hysteresisConfig, mode, now)
//       classifyDiskAxis(sample, thresholds, history, hysteresisConfig, mode, now)
//
//   - `hysteresisConfig` gains a new, OPTIONAL field, `hysteresisRecoverWindowMs`
//     (number), alongside the existing `hysteresisRecoverConsecutiveGreen`.
//     Recovery to GREEN now requires BOTH:
//       1. `consecutiveGreen >= hysteresisRecoverConsecutiveGreen` (existing rule)
//       2. `now - nonGreenSince >= hysteresisRecoverWindowMs` (new rule)
//   - Backward compatibility: when `hysteresisRecoverWindowMs` is omitted
//     from `hysteresisConfig` (every existing test above, and any caller
//     that hasn't opted in), the wall-clock gate is a no-op (always
//     satisfied) — recovery behaves exactly as documented above, unchanged.
//
// `applyHysteresis` itself is not exported, so every scenario below is
// expressed through the exported `classifyMemoryAxis`/`classifyDiskAxis`
// entry points (scenario 6 below confirms both axes thread `now` the same
// way, since both share the same underlying `applyHysteresis`).
//
// This file only asserts the contract the next (builder) phase must
// satisfy. Do not add a stub implementation to turn these green; that's the
// next agent's job.
// ---------------------------------------------------------------------------

describe('wall-clock hysteresis recovery — nonGreenSince + hysteresisRecoverWindowMs', () => {
  it('[1] backward-compat: with no hysteresisRecoverWindowMs in config, 3 rapid-fire GREEN samples at the SAME `now` recover exactly as today (pure count-based)', () => {
    const now = 1_000_000;

    const red = classifyMemoryAxis(MEMORY_RED, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG, undefined, now);
    expect(red.state).toBe('RED');

    // Three GREEN samples fired at literally the same `now` (0ms elapsed) —
    // with no `hysteresisRecoverWindowMs` configured, the wall-clock gate
    // must be a no-op and recovery must complete on the 3rd sample exactly
    // as the pure-count tests above already prove.
    const firstGreen = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, red.history, HYSTERESIS_CONFIG, undefined, now);
    expect(firstGreen.state).not.toBe('GREEN');

    const secondGreen = classifyMemoryAxis(
      MEMORY_GREEN,
      MEMORY_THRESHOLDS,
      firstGreen.history,
      HYSTERESIS_CONFIG,
      undefined,
      now,
    );
    expect(secondGreen.state).not.toBe('GREEN');

    const thirdGreen = classifyMemoryAxis(
      MEMORY_GREEN,
      MEMORY_THRESHOLDS,
      secondGreen.history,
      HYSTERESIS_CONFIG,
      undefined,
      now,
    );
    expect(thirdGreen.state).toBe('GREEN');
  });

  it('[1b] backward-compat: `now` entirely omitted (legacy call shape) behaves identically to the same call with `now` provided', () => {
    const withoutNow = (() => {
      const red = classifyMemoryAxis(MEMORY_RED, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
      const g1 = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, red.history, HYSTERESIS_CONFIG);
      const g2 = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, g1.history, HYSTERESIS_CONFIG);
      return classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, g2.history, HYSTERESIS_CONFIG);
    })();

    expect(withoutNow.state).toBe('GREEN');
  });

  const HYSTERESIS_CONFIG_WITH_WINDOW = {
    hysteresisRecoverConsecutiveGreen: 3,
    hysteresisRecoverWindowMs: 60_000,
  };

  it('[2] wall-clock gate ACTIVE: 3 rapid-fire GREEN samples (1-2ms apart) after a trip do NOT complete recovery, even though the count threshold is reached', () => {
    const t0 = 1_000_000;

    const red = classifyMemoryAxis(
      MEMORY_RED,
      MEMORY_THRESHOLDS,
      undefined,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0,
    );
    expect(red.state).toBe('RED');
    expect(red.history.nonGreenSince).toBe(t0);

    const firstGreen = classifyMemoryAxis(
      MEMORY_GREEN,
      MEMORY_THRESHOLDS,
      red.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0 + 1,
    );
    expect(firstGreen.state).not.toBe('GREEN');

    const secondGreen = classifyMemoryAxis(
      MEMORY_GREEN,
      MEMORY_THRESHOLDS,
      firstGreen.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0 + 2,
    );
    expect(secondGreen.state).not.toBe('GREEN');

    const thirdGreen = classifyMemoryAxis(
      MEMORY_GREEN,
      MEMORY_THRESHOLDS,
      secondGreen.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0 + 3,
    );

    // Count threshold IS reached (3 consecutive GREEN samples), but only 3ms
    // of wall-clock time has elapsed since `nonGreenSince` — far below the
    // configured 60_000ms window — so recovery must still not complete.
    expect(thirdGreen.history.consecutiveGreen).toBe(3);
    expect(thirdGreen.state).toBe('RED');
    expect(thirdGreen.state).not.toBe('GREEN');

    // The reason string must distinguish "count satisfied, still waiting on
    // the wall-clock window" from the plain count-based recovering message —
    // it names the count (3/3), the previous level (RED), and a wall-clock
    // indicator (elapsed/window).
    expect(thirdGreen.reason).toMatch(/3\/3 consecutive GREEN/);
    expect(thirdGreen.reason).toMatch(/RED/);
    expect(thirdGreen.reason).toMatch(/elapsed|wall-clock|window/i);
  });

  it('[3] wall-clock gate ACTIVE: 3 GREEN samples spread far enough apart in real time DO complete recovery once both gates are satisfied', () => {
    const t0 = 1_000_000;

    const red = classifyMemoryAxis(
      MEMORY_RED,
      MEMORY_THRESHOLDS,
      undefined,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0,
    );
    expect(red.state).toBe('RED');

    const t1 = t0 + 30_000;
    const firstGreen = classifyMemoryAxis(
      MEMORY_GREEN,
      MEMORY_THRESHOLDS,
      red.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t1,
    );
    expect(firstGreen.state).not.toBe('GREEN');

    const t2 = t1 + 30_000;
    const secondGreen = classifyMemoryAxis(
      MEMORY_GREEN,
      MEMORY_THRESHOLDS,
      firstGreen.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t2,
    );
    expect(secondGreen.state).not.toBe('GREEN');

    const t3 = t2 + 30_000; // t3 - t0 == 90_000ms >= 60_000ms window
    const thirdGreen = classifyMemoryAxis(
      MEMORY_GREEN,
      MEMORY_THRESHOLDS,
      secondGreen.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t3,
    );

    expect(thirdGreen.history.consecutiveGreen).toBe(3);
    expect(thirdGreen.state).toBe('GREEN');
  });

  it('[4] symmetry: a RED -> AMBER -> RED flap re-stamps nonGreenSince on EACH worsening transition, not just the first', () => {
    const t0 = 1_000_000;
    const t1 = t0 + 5_000;
    const t2 = t1 + 5_000;

    const red1 = classifyMemoryAxis(
      MEMORY_RED,
      MEMORY_THRESHOLDS,
      undefined,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0,
    );
    expect(red1.state).toBe('RED');
    expect(red1.history.nonGreenSince).toBe(t0);

    const amber = classifyMemoryAxis(
      MEMORY_AMBER,
      MEMORY_THRESHOLDS,
      red1.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t1,
    );
    expect(amber.state).toBe('AMBER');
    // Re-stamped to the AMBER transition's own `now`, not left at t0.
    expect(amber.history.nonGreenSince).toBe(t1);
    expect(amber.history.nonGreenSince).not.toBe(t0);

    const red2 = classifyMemoryAxis(
      MEMORY_RED,
      MEMORY_THRESHOLDS,
      amber.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t2,
    );
    expect(red2.state).toBe('RED');
    // Re-stamped again, to the SECOND RED's own `now` — the most recent
    // non-GREEN sample, not the oldest.
    expect(red2.history.nonGreenSince).toBe(t2);
    expect(red2.history.nonGreenSince).not.toBe(t0);
    expect(red2.history.nonGreenSince).not.toBe(t1);
  });

  it('[5] edge case: wall-clock window satisfied but count NOT yet satisfied — recovery must still NOT complete (both gates required)', () => {
    const t0 = 1_000_000;

    const red = classifyMemoryAxis(
      MEMORY_RED,
      MEMORY_THRESHOLDS,
      undefined,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0,
    );
    expect(red.state).toBe('RED');

    // A huge amount of wall-clock time has passed (well past the 60_000ms
    // window) but this is only the FIRST of 3 required consecutive GREEN
    // samples — the count gate alone must still block recovery.
    const t1 = t0 + 10 * 60_000;
    const firstGreen = classifyMemoryAxis(
      MEMORY_GREEN,
      MEMORY_THRESHOLDS,
      red.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t1,
    );

    expect(firstGreen.history.consecutiveGreen).toBe(1);
    expect(firstGreen.state).not.toBe('GREEN');
    expect(firstGreen.state).toBe('RED');
  });

  it('[6] `now` threads through the disk axis identically to the memory axis (both share the same underlying gate)', () => {
    const t0 = 1_000_000;

    const red = classifyDiskAxis(
      DISK_RED,
      DISK_THRESHOLDS,
      undefined,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0,
    );
    expect(red.state).toBe('RED');
    expect(red.history.nonGreenSince).toBe(t0);

    // Rapid-fire recovery attempt (1ms apart) — count reached, window not.
    const g1 = classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, red.history, HYSTERESIS_CONFIG_WITH_WINDOW, undefined, t0 + 1);
    const g2 = classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, g1.history, HYSTERESIS_CONFIG_WITH_WINDOW, undefined, t0 + 2);
    const g3 = classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, g2.history, HYSTERESIS_CONFIG_WITH_WINDOW, undefined, t0 + 3);

    expect(g3.history.consecutiveGreen).toBe(3);
    expect(g3.state).not.toBe('GREEN');
    expect(g3.state).toBe('RED');

    // The same sequence spread over real wall-clock time DOES recover.
    const g1Slow = classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, red.history, HYSTERESIS_CONFIG_WITH_WINDOW, undefined, t0 + 30_000);
    const g2Slow = classifyDiskAxis(
      DISK_GREEN,
      DISK_THRESHOLDS,
      g1Slow.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0 + 60_000,
    );
    const g3Slow = classifyDiskAxis(
      DISK_GREEN,
      DISK_THRESHOLDS,
      g2Slow.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0 + 90_000,
    );

    expect(g3Slow.history.consecutiveGreen).toBe(3);
    expect(g3Slow.state).toBe('GREEN');
  });
});

describe('configurable thresholds — no hardcoded magic numbers', () => {
  it('changing the memory AMBER swap threshold changes the classification of the same sample', () => {
    const sample = { pressureLevel: 1, swapUsedMb: 5000, compressedMb: 512 };

    // With default thresholds, 5000MB swap crosses the AMBER band (below
    // 8192MB but above the 4096MB GREEN ceiling).
    const withDefaults = classifyMemoryAxis(sample, MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(withDefaults.state).toBe('AMBER');

    // Loosen the GREEN ceiling past the sample's swap usage — the same
    // sample must now classify GREEN, proving the band is read from config,
    // not inlined.
    const loosenedThresholds = { ...MEMORY_THRESHOLDS, greenSwapUsedMbBelow: 6000 };
    const withLoosenedConfig = classifyMemoryAxis(sample, loosenedThresholds, undefined, HYSTERESIS_CONFIG);
    expect(withLoosenedConfig.state).toBe('GREEN');
  });

  it('changing the disk RED free-space threshold changes the classification of the same sample', () => {
    const sample = { freeDiskGb: 12, declineRateGbPerHour: 0.5 };

    // With default thresholds (RED below 10GB), 12GB free is AMBER.
    const withDefaults = classifyDiskAxis(sample, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(withDefaults.state).toBe('AMBER');

    // Raise the RED ceiling above the sample's free space — the same sample
    // must now classify RED, proving the band is read from config.
    const tightenedThresholds = { ...DISK_THRESHOLDS, redFreeDiskGbBelow: 15 };
    const withTightenedConfig = classifyDiskAxis(sample, tightenedThresholds, undefined, HYSTERESIS_CONFIG);
    expect(withTightenedConfig.state).toBe('RED');
  });

  it('changing the hysteresis recovery N changes how many consecutive GREEN samples are required to recover', () => {
    const red = classifyMemoryAxis(MEMORY_RED, MEMORY_THRESHOLDS, undefined, { hysteresisRecoverConsecutiveGreen: 1 });
    expect(red.state).toBe('RED');

    // With N=1, a single GREEN sample is enough to recover — a different
    // outcome than the N=3 default asserted in the tests above, proving N
    // is read from config, not hardcoded.
    const firstGreen = classifyMemoryAxis(MEMORY_GREEN, MEMORY_THRESHOLDS, red.history, {
      hysteresisRecoverConsecutiveGreen: 1,
    });
    expect(firstGreen.state).toBe('GREEN');
  });
});

// ---------------------------------------------------------------------------
// Phase 5 (Part B) — swap-recovery velocity, DISTINCT from the
// existing wall-clock hysteresis gate above (`nonGreenSince` +
// `hysteresisRecoverWindowMs`). That gate only asks "has enough real time
// passed since the trip, AND have N consecutive samples read GREEN" — it
// never looks at whether swapUsedMb is actually trending down. A machine
// whose swap stays elevated (compressed pages not evicted) for the whole
// recovery window still recovers today once count+wall-clock pass. This
// section closes that gap with a THIRD, independent recovery gate.
//
// RED by construction: none of this exists in ./lib/threshold.mjs yet.
// `classifyMemoryAxis`/`classifyDiskAxis` themselves are NOT new exports (no
// dynamic-import workaround needed — see allowance.jest.spec.mjs's Part A
// section for why that workaround exists there but not here); calling them
// with the new history/config shapes below simply produces the WRONG
// result today (an assertion mismatch), never a crash, since the current
// implementation just ignores fields it doesn't recognize.
//
// PINNED CONTRACT (design decision surfaced here for the implementer):
//
//   `HysteresisHistory` gains a new field, `swapReadings`: an array of
//   `{ value: number, sampledAt: number }`, capped at
//   SWAP_WINDOW_MAX_LENGTH = 3 entries (oldest evicted first, FIFO).
//
//   Maintained INSIDE `classifyMemoryAxis`'s returned `history` — the same
//   place `consecutiveGreen`/`tripped`/`level`/`nonGreenSince` already
//   live — threaded through by the caller exactly the same way (no new
//   argument; it rides along inside the existing `history` in/out
//   parameter).
//
//   Population rule: on EVERY `classifyMemoryAxis` call (regardless of the
//   sample's classified level — RED, AMBER, or GREEN), if
//   `sample.swapUsedMb` is a finite number, push
//   `{ value: sample.swapUsedMb, sampledAt: now }` onto the window,
//   evicting the oldest entry once the window exceeds
//   SWAP_WINDOW_MAX_LENGTH. A non-finite/missing `swapUsedMb` (e.g. a stale
//   sample) does not push anything — the window is left exactly as it was,
//   never corrupted with a NaN/undefined entry.
//
//   New recovery condition (GREEN recovery ONLY — never affects worsening
//   detection): in addition to the existing count
//   (`consecutiveGreen >= hysteresisRecoverConsecutiveGreen`) and wall-clock
//   (`now - nonGreenSince >= hysteresisRecoverWindowMs`) gates, recovery to
//   GREEN additionally requires the swap window (AS IT STANDS after this
//   call's own push) to show a NON-INCREASING trend: the LAST reading's
//   `value` must be <= the FIRST reading's `value` in the window (a cheap,
//   robust "not still climbing/flat overall" check — not a strict
//   monotonic requirement on every intermediate step).
//
//   Degrades gracefully when there isn't enough data to judge a trend: if
//   the window has FEWER than SWAP_WINDOW_MAX_LENGTH (3) entries, this new
//   gate is skipped entirely (treated as satisfied) — recovery falls back
//   to the existing count+wall-clock gates only, exactly as today. This
//   also covers a pre-Phase-5 persisted `memoryHistory` with no
//   `swapReadings` field at all (undefined/missing is 0 entries, well under
//   the minimum) — it must never crash, and must never wait forever for a
//   trend it has no data to compute.
//
//   Worsening (raw AMBER/RED) is completely unaffected: it always takes
//   effect immediately, exactly as today, regardless of anything in
//   `swapReadings`.
//
// Do not add a stub implementation to make this pass — the next (builder)
// phase turns it green for real.
// ---------------------------------------------------------------------------

describe('swap-recovery velocity — GREEN recovery additionally requires a non-increasing swap trend (Phase 5 Part B)', () => {
  const HYSTERESIS_CONFIG_WITH_WINDOW = {
    hysteresisRecoverConsecutiveGreen: 3,
    hysteresisRecoverWindowMs: 60_000,
  };

  it('[1] swap elevated-but-declining across 3 samples: recovery completes once count + wall-clock + swap-trend ALL pass', () => {
    const t0 = 1_000_000;

    const red = classifyMemoryAxis(
      { pressureLevel: 4, swapUsedMb: 12_000, compressedMb: 4096 },
      MEMORY_THRESHOLDS,
      undefined,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0,
    );
    expect(red.state).toBe('RED');

    // NOTE: every raw sample below must classify raw-GREEN on its own terms
    // (pressureLevel <= 1 AND swapUsedMb < greenSwapUsedMbBelow=4096) —
    // otherwise the *existing* count/wall-clock hysteresis gate would never
    // even reach the new swap-trend gate this test targets. "Elevated but
    // declining" therefore means elevated RELATIVE to the other readings in
    // this same recovering window, not elevated past the raw-GREEN band.
    const g1 = classifyMemoryAxis(
      { pressureLevel: 1, swapUsedMb: 3_900, compressedMb: 1024 },
      MEMORY_THRESHOLDS,
      red.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0 + 40_000,
    );
    expect(g1.state).not.toBe('GREEN');

    const g2 = classifyMemoryAxis(
      { pressureLevel: 1, swapUsedMb: 2_000, compressedMb: 1024 },
      MEMORY_THRESHOLDS,
      g1.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0 + 80_000,
    );
    expect(g2.state).not.toBe('GREEN');

    const g3 = classifyMemoryAxis(
      { pressureLevel: 1, swapUsedMb: 500, compressedMb: 1024 },
      MEMORY_THRESHOLDS,
      g2.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0 + 120_000,
    );

    // Count (3) + wall-clock (120_000ms >= 60_000ms) + swap trend
    // (3900 -> 2000 -> 500, strictly declining) all satisfied.
    expect(g3.history.consecutiveGreen).toBe(3);
    expect(g3.state).toBe('GREEN');
  });

  it('[2] swap flat/rising blocks recovery even after count + wall-clock gates pass', () => {
    const t0 = 1_000_000;

    const red = classifyMemoryAxis(
      { pressureLevel: 4, swapUsedMb: 12_000, compressedMb: 4096 },
      MEMORY_THRESHOLDS,
      undefined,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0,
    );
    expect(red.state).toBe('RED');

    const g1 = classifyMemoryAxis(
      { pressureLevel: 1, swapUsedMb: 500, compressedMb: 1024 },
      MEMORY_THRESHOLDS,
      red.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0 + 40_000,
    );
    const g2 = classifyMemoryAxis(
      { pressureLevel: 1, swapUsedMb: 2_000, compressedMb: 1024 },
      MEMORY_THRESHOLDS,
      g1.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0 + 80_000,
    );
    const g3 = classifyMemoryAxis(
      { pressureLevel: 1, swapUsedMb: 3_900, compressedMb: 1024 },
      MEMORY_THRESHOLDS,
      g2.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0 + 120_000,
    );

    // Count and wall-clock gates are BOTH satisfied at g3 (same timings as
    // test [1]), but swap is rising throughout the window (500 -> 2000 ->
    // 3900, all still individually below the raw-GREEN swap ceiling) — the
    // new trend gate must block recovery despite the other two gates
    // passing.
    expect(g3.history.consecutiveGreen).toBe(3);
    expect(g3.state).not.toBe('GREEN');
    expect(g3.state).toBe('RED');
    expect(g3.reason).toMatch(/swap/i);
  });

  it('[3] fewer than the minimum window samples (< 3 readings) degrades to count+wall-clock-only gating — no trend is invented from insufficient data', () => {
    const t0 = 1_000_000;
    const config = { hysteresisRecoverConsecutiveGreen: 1, hysteresisRecoverWindowMs: 60_000 };

    const red = classifyMemoryAxis(
      { pressureLevel: 4, swapUsedMb: 12_000, compressedMb: 4096 },
      MEMORY_THRESHOLDS,
      undefined,
      config,
      undefined,
      t0,
    );
    expect(red.state).toBe('RED');

    // Only ONE GREEN sample is required to satisfy the count gate here
    // (hysteresisRecoverConsecutiveGreen: 1), and wall-clock elapsed
    // (70_000ms >= 60_000ms) is also satisfied. The swap window after this
    // call holds only 2 readings total (the RED trip's 12_000 plus this
    // GREEN's own value) — well under the 3-reading minimum — so the trend
    // gate must be skipped entirely rather than blocking recovery on data
    // it doesn't have enough of.
    const g1 = classifyMemoryAxis(
      { pressureLevel: 1, swapUsedMb: 3_000, compressedMb: 1024 }, // raw-GREEN, but window still too short for a trend verdict
      MEMORY_THRESHOLDS,
      red.history,
      config,
      undefined,
      t0 + 70_000,
    );

    expect(g1.history.consecutiveGreen).toBe(1);
    expect(g1.state).toBe('GREEN');
  });

  it('[4] the swap window never affects immediate AMBER/RED trip detection — worsening always takes effect instantly', () => {
    const t0 = 1_000_000;

    // Build up a DECLINING swap window during a recovery attempt (as in
    // test [1]) — the window is primed favorably for recovery.
    const red = classifyMemoryAxis(
      { pressureLevel: 4, swapUsedMb: 12_000, compressedMb: 4096 },
      MEMORY_THRESHOLDS,
      undefined,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0,
    );
    const g1 = classifyMemoryAxis(
      { pressureLevel: 1, swapUsedMb: 500, compressedMb: 1024 },
      MEMORY_THRESHOLDS,
      red.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0 + 40_000,
    );

    // A fresh worsening sample must instantly report RED regardless of the
    // favorably-declining swap window built up so far — trip detection is
    // never gated by, or delayed because of, the swap-trend mechanism.
    const worsen = classifyMemoryAxis(
      { pressureLevel: 4, swapUsedMb: 13_000, compressedMb: 4096 },
      MEMORY_THRESHOLDS,
      g1.history,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0 + 40_001,
    );

    expect(worsen.state).toBe('RED');
    expect(worsen.history.tripped).toBe(true);
    expect(worsen.history.nonGreenSince).toBe(t0 + 40_001);
  });

  it('[5] backward-compat regression: a pre-Phase-5 memoryHistory with NO swapReadings field at all does not crash and degrades to count+wall-clock-only recovery', () => {
    const t0 = 1_000_000;

    // Simulates exactly what a pre-Phase-5 cli.mjs would have persisted:
    // consecutiveGreen/tripped/level/nonGreenSince only, no swapReadings key
    // whatsoever (not even `undefined` — the property is simply absent, as
    // JSON.parse of an old coordination-file entry would produce).
    const legacyHistory = { consecutiveGreen: 2, tripped: true, level: 'RED', nonGreenSince: t0 };

    expect(() =>
      classifyMemoryAxis(
        { pressureLevel: 1, swapUsedMb: 500, compressedMb: 256 },
        MEMORY_THRESHOLDS,
        legacyHistory,
        HYSTERESIS_CONFIG_WITH_WINDOW,
        undefined,
        t0 + 120_000,
      ),
    ).not.toThrow();

    const result = classifyMemoryAxis(
      { pressureLevel: 1, swapUsedMb: 500, compressedMb: 256 },
      MEMORY_THRESHOLDS,
      legacyHistory,
      HYSTERESIS_CONFIG_WITH_WINDOW,
      undefined,
      t0 + 120_000,
    );

    // This is the 3rd consecutive GREEN sample (2 carried forward + this
    // one) with wall-clock elapsed (120_000ms >= 60_000ms) — with no swap
    // window data at all to judge a trend from, recovery must still
    // complete via the existing count+wall-clock gates, not hang forever
    // waiting on data that will never arrive.
    expect(result.history.consecutiveGreen).toBe(3);
    expect(result.state).toBe('GREEN');
  });
});

// ---------------------------------------------------------------------------
// Phase 5 (Part C) — disk-decline-rate smoothing.
//
// RED by construction: none of this exists in ./lib/threshold.mjs yet.
// Same non-crashing "wrong result, not a throw" RED shape as Part B above —
// `classifyDiskAxis` is an existing export exercised with new history/config
// shapes the current implementation simply ignores.
//
// PINNED CONTRACT:
//
//   `diskHistory` gains a new field, `declineRateReadings`: an array of
//   `{ value: number, sampledAt: number }`, same shape/cap convention as
//   Part B's `swapReadings` (DECLINE_RATE_WINDOW_MAX_LENGTH = 3, FIFO
//   eviction), maintained inside `classifyDiskAxis`'s returned `history`.
//
//   Population rule: on EVERY `classifyDiskAxis` call, if
//   `sample.declineRateGbPerHour` is a finite number, push
//   `{ value: sample.declineRateGbPerHour, sampledAt: now }` onto the
//   window (evicting the oldest beyond the cap). A non-finite/missing value
//   (e.g. a stale sample) does not push anything.
//
//   Smoothing method: MEDIAN of the window's readings (not a mean/simple
//   moving average) — chosen specifically because a median is robust to a
//   single noisy/reversing outlier sample (e.g. one large transient write)
//   in a way a mean is not; a mean would still be dragged toward the
//   outlier, exactly the false-positive this smoothing exists to prevent.
//   With fewer than 3 readings in the window (including the brand-new,
//   first-ever-call case: 1 reading), the median of however many readings
//   ARE available is used — i.e. a lone reading's "median" is itself,
//   unsmoothed. This is NOT a special case in the implementation, just what
//   "median of N values" naturally means for small N — documented here so
//   the RED tests below don't read as pinning a separate code path.
//
//   This smoothed value is substituted for the raw `declineRateGbPerHour`
//   ONLY at the point `classifyDiskRaw` computes `hoursToFull` for the
//   `amberHoursToFullBelow` steep-decline check — the RED
//   `freeDiskGb < redFreeDiskGbBelow` check and the GREEN
//   `freeDiskGb > greenFreeDiskGbAbove` check are both driven by
//   instantaneous `freeDiskGb` already and are untouched by this change.
//
//   A real, sustained decline trend must still trip AMBER/RED within a
//   bounded number of samples (3, the window's own cap) — smoothing must
//   dampen single-sample noise, not permanently mask a genuine trend.
//
// Do not add a stub implementation to make this pass — the next (builder)
// phase turns it green for real.
// ---------------------------------------------------------------------------

describe('disk-decline-rate smoothing — median-of-last-3 before the amberHoursToFullBelow comparison (Phase 5 Part C)', () => {
  it('[1] a single noisy/reversing transient sample does NOT flip the disk axis when the smoothed (median) value stays outside the AMBER threshold', () => {
    const t0 = 1_000_000;

    // Two stable, low-decline GREEN readings...
    const g1 = classifyDiskAxis({ freeDiskGb: 50, declineRateGbPerHour: 0.5 }, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG, undefined, t0);
    expect(g1.state).toBe('GREEN');

    const g2 = classifyDiskAxis(
      { freeDiskGb: 50, declineRateGbPerHour: 0.5 },
      DISK_THRESHOLDS,
      g1.history,
      HYSTERESIS_CONFIG,
      undefined,
      t0 + 1_000,
    );
    expect(g2.state).toBe('GREEN');

    // ...then one large transient write reverses/spikes the raw
    // declineRateGbPerHour way up (a value that, taken RAW, would project
    // hoursToFull = 50 / 50 = 1h, well under the 2h AMBER threshold).
    const noisySpike = classifyDiskAxis(
      { freeDiskGb: 50, declineRateGbPerHour: 50 },
      DISK_THRESHOLDS,
      g2.history,
      HYSTERESIS_CONFIG,
      undefined,
      t0 + 2_000,
    );

    // median([0.5, 0.5, 50]) === 0.5 -> hoursToFull = 50 / 0.5 = 100h, well
    // above the 2h threshold -> stays GREEN despite the raw spike.
    expect(noisySpike.state).toBe('GREEN');
  });

  it('[2] a genuinely sustained decline trend across 3 samples still trips AMBER within the window\'s own bound (3 samples) — smoothing must not hide a real trend forever', () => {
    const t0 = 1_000_000;

    const g1 = classifyDiskAxis({ freeDiskGb: 50, declineRateGbPerHour: 25 }, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG, undefined, t0);
    const g2 = classifyDiskAxis(
      { freeDiskGb: 50, declineRateGbPerHour: 28 },
      DISK_THRESHOLDS,
      g1.history,
      HYSTERESIS_CONFIG,
      undefined,
      t0 + 1_000,
    );
    const g3 = classifyDiskAxis(
      { freeDiskGb: 50, declineRateGbPerHour: 30 },
      DISK_THRESHOLDS,
      g2.history,
      HYSTERESIS_CONFIG,
      undefined,
      t0 + 2_000,
    );

    // median([25, 28, 30]) === 28 -> hoursToFull = 50 / 28 ≈ 1.79h, below
    // the 2h AMBER threshold -> trips AMBER by (at the latest) the 3rd
    // sample, once the window is fully populated with a genuinely
    // sustained trend.
    expect(g3.state).toBe('AMBER');
  });

  it('[3] fewer than the minimum window samples (first-ever call, 1 reading) uses the single available reading unsmoothed rather than crashing or waiting indefinitely', () => {
    const t0 = 1_000_000;

    expect(() =>
      classifyDiskAxis({ freeDiskGb: 50, declineRateGbPerHour: 30 }, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG, undefined, t0),
    ).not.toThrow();

    // A single 30 GB/hour reading, alone, projects hoursToFull = 50/30 ≈
    // 1.67h — below the 2h AMBER threshold. With only 1 reading ever
    // observed, "smoothing" is a no-op (median of 1 value is itself) — this
    // must trip AMBER on the very first call, not wait for a 3-sample
    // window that will never arrive if the axis never sees 2 more samples.
    const first = classifyDiskAxis(
      { freeDiskGb: 50, declineRateGbPerHour: 30 },
      DISK_THRESHOLDS,
      undefined,
      HYSTERESIS_CONFIG,
      undefined,
      t0,
    );
    expect(first.state).toBe('AMBER');
  });

  it('[4] backward-compat regression: a pre-Phase-5 diskHistory with NO declineRateReadings field at all does not crash and classifies from the current raw reading alone', () => {
    const t0 = 1_000_000;

    const legacyHistory = { consecutiveGreen: 1, tripped: false, level: 'GREEN', nonGreenSince: undefined };

    expect(() =>
      classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, legacyHistory, HYSTERESIS_CONFIG, undefined, t0),
    ).not.toThrow();

    const result = classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, legacyHistory, HYSTERESIS_CONFIG, undefined, t0);
    expect(result.state).toBe('GREEN');
  });
});

// ---------------------------------------------------------------------------
// GitHub issue, Phase 1 — relative (fraction-of-swapTotalMb) memory
// swap thresholds, plus the extraRecoveryGate closure-parameter regression
//.
//
// RED by construction: classifyMemoryRaw (threshold.mjs:227-254) today only
// understands ABSOLUTE swap thresholds (`redSwapUsedMbAtOrAbove`,
// `greenSwapUsedMbBelow`) sized for a large-swap machine (production default
// 10240MB RED / 4096MB GREEN, see cli.mjs's DEFAULT_MEMORY_THRESHOLDS). On
// this host's real 1 GiB (1024MB) total swap, swapUsedMb can NEVER reach
// 10240MB — the RED swap gate is structurally unreachable — and ANY
// swapUsedMb below 4096MB (i.e. always, since total swap is only 1024MB)
// trivially satisfies the GREEN gate regardless of how saturated swap
// actually is. Neither absolute threshold means anything on a small-swap
// host.
//
// PINNED TARGET CONTRACT (design decision surfaced here for the builder,
// following this file's own "RED by construction" documentation convention
// used throughout — e.g. the wall-clock/swap-velocity sections above):
//
//   - The memory sample sub-shape gains a new field, `swapTotalMb` (finite
//     number | absent), sourced from the SAME `sysctl vm.swapusage` call
//     `./lib/recon.mjs`'s `parseSwapUsage` already parses at recon.mjs:196
//     (today dropped before cli.mjs's `realCollect()` forwards its sample at
//     cli.mjs:692 — see the cli.jest.spec.mjs section below for the
//     forwarding regression test).
//   - `thresholds` gains two new, OPTIONAL fields expressed as a FRACTION
//     (0..1) of `swapTotalMb`, replacing the structurally-unreachable
//     absolute swap fields as the mechanism actually driving RED/GREEN on
//     any host regardless of its total swap size:
//       `redSwapUsedFractionOfTotalAtOrAbove`  — RED when
//         `swapUsedMb / swapTotalMb >= redSwapUsedFractionOfTotalAtOrAbove`
//       `greenSwapUsedFractionOfTotalBelow`    — GREEN (swap-wise) only when
//         `swapUsedMb / swapTotalMb < greenSwapUsedFractionOfTotalBelow`
//   - Guarded division: when `swapTotalMb` is 0, non-finite, or absent (swap
//     disabled/unparseable), the fractional checks are skipped entirely
//     (never divide by zero, never throw, never fabricate a false RED/AMBER
//     from a NaN/Infinity result) — classification falls back to whatever
//     other checks (pressure, any still-configured absolute fields) apply.
//   - Existing absolute fields (`redSwapUsedMbAtOrAbove`,
//     `greenSwapUsedMbBelow`) are unaffected when present — this is an
//     ADDITIVE mechanism, not a replacement of the field shape itself (only
//     of the unreachable production DEFAULT_MEMORY_THRESHOLDS *values*,
//     which is a cli.mjs config change, not a threshold.mjs contract change,
//     and therefore out of scope for this test file).
//
// Field names above are this test file's own naming proposal (no prior
// precedent in the codebase) — the builder may rename, but the shape
// (a fraction field per RED/GREEN band, driven by `swapUsedMb/swapTotalMb`)
// is the contract this section pins.
//
// Do not add a stub implementation to make this pass — the next (builder)
// phase turns it green for real.
// ---------------------------------------------------------------------------

describe('relative (fraction-of-swapTotalMb) memory swap thresholds', () => {
  const RELATIVE_SWAP_THRESHOLDS = {
    greenPressureAtOrBelow: 1,
    redPressureAtOrAbove: 4,
    redSwapUsedFractionOfTotalAtOrAbove: 0.9,
    greenSwapUsedFractionOfTotalBelow: 0.5,
  };

  it('swap RED gate reachable at 1 GiB total swap: 950/1024MB (~92.8%) trips RED', () => {
    // Under today's absolute-only thresholds this sample would never reach
    // RED (10240MB is unreachable when total swap is only 1024MB) and would
    // trivially pass any GREEN check that only looks at absolute MB — this
    // test proves the fix makes the RED gate reachable on a small-swap host.
    const sample = { pressureLevel: 1, swapUsedMb: 950, swapTotalMb: 1024, compressedMb: 128 };

    const result = classifyMemoryAxis(sample, RELATIVE_SWAP_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    expect(result.state).toBe('RED');
    expect(result.reason).toMatch(/swap/i);
  });

  it('swap GREEN gate is not always-true once relative: 700/1024MB (~68%) is AMBER, not GREEN, despite being "small" in absolute MB', () => {
    // 700MB is comfortably below the old absolute greenSwapUsedMbBelow
    // default (4096MB) and would have classified GREEN under today's
    // absolute-only logic regardless of how saturated the 1GiB swap device
    // actually is. Once the gate is relative to swapTotalMb, 700/1024
    // (~68%) sits between the green floor (50%) and the red ceiling (90%)
    // and must classify AMBER — proving GREEN is no longer trivially true
    // just because the absolute MB figure looks small.
    const sample = { pressureLevel: 1, swapUsedMb: 700, swapTotalMb: 1024, compressedMb: 96 };

    const result = classifyMemoryAxis(sample, RELATIVE_SWAP_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    expect(result.state).toBe('AMBER');
    expect(result.state).not.toBe('GREEN');
  });

  it('swapTotalMb=0 (swap disabled) does not throw and does not divide by zero into a fabricated RED/AMBER', () => {
    const sample = { pressureLevel: 1, swapUsedMb: 0, swapTotalMb: 0, compressedMb: 0 };

    expect(() => classifyMemoryAxis(sample, RELATIVE_SWAP_THRESHOLDS, undefined, HYSTERESIS_CONFIG)).not.toThrow();

    const result = classifyMemoryAxis(sample, RELATIVE_SWAP_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    // 0/0 is NaN — a naive unguarded `swapUsedMb / swapTotalMb >= threshold`
    // comparison against NaN is always false in JS, which would silently
    // mask this case as "not RED" by accident rather than by a deliberate
    // guard; the important, explicit assertion is that nothing throws and
    // nothing produces a non-finite figure baked into the reason string.
    expect(result.reason).not.toMatch(/NaN|Infinity/);
    expect(['GREEN', 'AMBER', 'RED']).toContain(result.state);
  });

  it('swapTotalMb non-finite (e.g. undefined/unparseable) does not throw and does not divide by zero', () => {
    const sample = { pressureLevel: 1, swapUsedMb: 500, swapTotalMb: undefined, compressedMb: 64 };

    expect(() => classifyMemoryAxis(sample, RELATIVE_SWAP_THRESHOLDS, undefined, HYSTERESIS_CONFIG)).not.toThrow();

    const result = classifyMemoryAxis(sample, RELATIVE_SWAP_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(result.reason).not.toMatch(/NaN|Infinity/);
    expect(['GREEN', 'AMBER', 'RED']).toContain(result.state);
  });

  it('a pre-existing hysteresis history (no swapReadings field, no swapTotalMb ever seen) still classifies a fraction-thresholded sample without throwing', () => {
    // Simulates upgrading a running orchestrator mid-recovery-window: the
    // carried-forward `history` predates both Phase 5's swapReadings AND
    // this phase's swapTotalMb wiring, but the CURRENT sample/thresholds
    // already use the new relative contract.
    const legacyHistory = { consecutiveGreen: 1, tripped: false, level: 'GREEN', nonGreenSince: undefined };
    const sample = { pressureLevel: 1, swapUsedMb: 200, swapTotalMb: 1024, compressedMb: 32 };

    expect(() =>
      classifyMemoryAxis(sample, RELATIVE_SWAP_THRESHOLDS, legacyHistory, HYSTERESIS_CONFIG),
    ).not.toThrow();

    const result = classifyMemoryAxis(sample, RELATIVE_SWAP_THRESHOLDS, legacyHistory, HYSTERESIS_CONFIG);
    expect(result.state).toBe('GREEN');
  });
});

// ---------------------------------------------------------------------------
// GitHub issue — `classifyMemoryAxis`'s `extraRecoveryGate` closure
// (threshold.mjs:288-299) reads the outer, closed-over `swapReadings` local
// variable directly, ignoring the `extraHistoryFields` parameter
// `applyHysteresis` calls it with (threshold.mjs:190:
// `extras.extraRecoveryGate(extraHistoryFields)`) — violating the contract
// `applyHysteresis` documents for itself at threshold.mjs:118-124 ("Returning
// `{ satisfied: false, reason }`... `extraRecoveryGate`, when provided, is
// consulted...").
//
// IMPORTANT — per issue its own text, this is explicitly a NO
// OBSERVABLE BEHAVIOUR DIFFERENCE bug today: `classifyMemoryAxis` builds
// `extraHistoryFields: { swapReadings }` from the exact same local
// `swapReadings` variable the closure independently reads, so the two are
// always the identical array reference by construction — there is no
// black-box (behavioural) input that can make them diverge without editing
// `classifyMemoryAxis` itself. its acceptance criteria confirm this:
// "No behavior change — existing threshold.jest.spec.mjs tests pass
// unmodified." A pure black-box test of classifyMemoryAxis's inputs/outputs
// therefore cannot be made to fail today — this is a real discrepancy from
// this test list's framing of it as a behavioural "regression test that
// would have caught "; flagged in the accompanying report rather than
// silently worked around.
//
// This test instead pins the STRUCTURAL contract directly (parameter usage,
// not closure capture) via the function's own source text, which IS
// observably different today (the closure is declared `() => { ... }`,
// zero params, per the source read at threshold.mjs:288) vs. the fixed
// shape (`(extraHistoryFields) => { ... extraHistoryFields.swapReadings ... }`).
// This is a deliberate, narrow exception to "test behaviour, not
// implementation" — justified only because no behavioural distinction
// exists to test instead, and only for exactly this one documented
// contract violation.
// ---------------------------------------------------------------------------

describe('extraRecoveryGate reads its own extraHistoryFields parameter, not closure-captured outer state', () => {
  it('the extraRecoveryGate closure inside classifyMemoryAxis declares and uses its extraHistoryFields parameter', () => {
    // Reads the ACTUAL FILE from disk rather than `classifyMemoryAxis.toString()`
    // — under `jest --coverage` (istanbul instrumentation, which the real
    // `npm run test:skills -- --coverage` CI gate runs — see
    // ci.yml's "Functional pillar — Jest with coverage" step), the in-memory
    // function's `.toString()` is rewritten with injected coverage-counter
    // statements (`cov_xxxxx().s[0]++;` etc.), which breaks this regex's
    // expectation of clean, unmodified source formatting — a real,
    // CI-reproducible failure this file's own tests must not (re)introduce.
    // The doc-honesty companion tests elsewhere in this directory already
    // establish `readFileSync` against the real file as this project's
    // pattern for exactly this reason.
    const match = thresholdSource.match(/extraRecoveryGate:\s*\(([^)]*)\)\s*=>/);
    expect(match).not.toBeNull();

    const paramName = (match?.[1] ?? '').trim();
    expect(paramName.length).toBeGreaterThan(0);

    // The closure's body (from the arrow up to the matching close of the
    // extras object literal) must actually READ the parameter it declares —
    // e.g. `extraHistoryFields.swapReadings` — not merely accept-and-ignore
    // it while still falling back to the bare, closed-over `swapReadings`
    // identifier.
    const bodyStart = thresholdSource.indexOf(match[0]);
    const body = thresholdSource.slice(bodyStart, bodyStart + 400);
    expect(body).toMatch(new RegExp(`${paramName}\\.swapReadings`));
  });

  it('disk axis\'s declineRateReadings recovery gate is unaffected by the fix (regression guard)', () => {
    // classifyDiskAxis's own extraHistoryFields usage (threshold.mjs:381)
    // takes no extraRecoveryGate at all today — the fix must not
    // introduce one, or otherwise change the disk axis's hysteresis
    // recovery behaviour. Same 3-consecutive-GREEN recovery contract
    // already pinned elsewhere in this file, repeated here specifically as
    // a regression net tied to the fix landing.
    const red = classifyDiskAxis(DISK_RED, DISK_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    expect(red.state).toBe('RED');

    const firstGreen = classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, red.history, HYSTERESIS_CONFIG);
    expect(firstGreen.state).not.toBe('GREEN');

    const secondGreen = classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, firstGreen.history, HYSTERESIS_CONFIG);
    expect(secondGreen.state).not.toBe('GREEN');

    const thirdGreen = classifyDiskAxis(DISK_GREEN, DISK_THRESHOLDS, secondGreen.history, HYSTERESIS_CONFIG);
    expect(thirdGreen.state).toBe('GREEN');

    // Disk axis's extras never carried an extraRecoveryGate, so the
    // fix (scoped to classifyMemoryAxis only) has literally nothing
    // to change here — the disk axis's own extraHistoryFields usage
    // (threshold.mjs:381) is a single-line, no-closure pass-through.
    const diskSource = classifyDiskAxis.toString();
    expect(diskSource).not.toMatch(/extraRecoveryGate/);
  });
});

// ---------------------------------------------------------------------------
// GitHub issue, Phase 1 — small/demand-grown `swapTotalMb` pins the
// memory axis at AMBER even on a genuinely idle host.
//
// `classifyMemoryRaw` (threshold.mjs:227-277) computes
// `swapUsedFractionOfTotal = swapUsedMb / swapTotalMb` guarded only against
// `swapTotalMb` being 0/non-finite (`hasUsableSwapTotal`) — it has NO floor
// on `swapTotalMb`'s MAGNITUDE. macOS grows swap on demand, so a real,
// otherwise-healthy host can report a small `swapTotalMb` (e.g. a few
// hundred MB, or even single-digit MB just after a swapfile rolls over) —
// any nonzero `swapUsedMb` on such a small total produces a wildly inflated
// fraction that trips the AMBER/RED fractional gates even though the
// absolute swap usage is negligible. is the observed real-world
// regression: a host with 3072MB total swap and 2024.25MB used (≈66%, a
// perfectly ordinary demand-grown swap total, not a memory emergency) got
// pinned at AMBER by the fractional gate alone.
//
// PINNED TARGET CONTRACT (design decision surfaced here for the builder):
//
//   - A new floor constant, `MIN_CONFIDENT_SWAP_TOTAL_MB` (proposed value:
//     512), names the point below which `swapTotalMb` is treated as
//     low-confidence: too small a denominator for a fraction computed
//     against it to be a meaningful signal (a single swap event can swing
//     the fraction by tens of percentage points). `hasUsableSwapTotal` must
//     additionally require `swapTotalMb >= MIN_CONFIDENT_SWAP_TOTAL_MB`,
//     alongside its existing finite/positive check — when swapTotalMb is
//     below the floor, the fractional gates (`redSwapUsedFractionOfTotalAtOrAbove`
//     / `greenSwapUsedFractionOfTotalBelow`) are skipped entirely and
//     classification falls back to whatever absolute-MB / pressureLevel
//     checks are still configured, exactly as it already does for
//     `swapTotalMb` of 0/non-finite.
//   - This is a NARROWING of when the fractional gate applies, not a
//     disabling of it — hosts with a large, confidently-sized swap total
//     (at/above the floor) must continue to classify via the fractional
//     gate exactly as today, including tripping AMBER/RED for genuinely
//     high fractional swap usage.
//
// The floor's exact name/value is this test file's own proposal (documented
// here for the implementer to wire, per this repo's existing "RED by
// construction" convention) — 512MB is chosen as a defensible round number
// comfortably above the "a few hundred MB of freshly demand-grown swap"
// noise band and comfortably below the host's real 3072MB total.
//
// Do not add a stub implementation to make this pass — the next (builder)
// phase turns it green for real.
// ---------------------------------------------------------------------------

describe('swap-total floor for the fractional swap gate — MIN_CONFIDENT_SWAP_TOTAL_MB', () => {
  const PRODUCTION_MEMORY_THRESHOLDS = {
    greenPressureAtOrBelow: 1,
    greenSwapUsedMbBelow: 4096,
    redPressureAtOrAbove: 4,
    redSwapUsedMbAtOrAbove: 10240,
    redSwapUsedFractionOfTotalAtOrAbove: 0.9,
    greenSwapUsedFractionOfTotalBelow: 0.5,
  };

  it('[1] pinned regression: swapUsedMb=2024.25/swapTotalMb=3072 (~66%, low pressure) classifies GREEN, not AMBER', () => {
    // Today: fraction ≈0.6589 sits between the green floor (0.5) and the red
    // ceiling (0.9), so the (buggy) fractional gate alone forces AMBER even
    // though 3072MB total swap is an ordinary demand-grown size and 66%
    // utilization at pressureLevel=1 does not reflect real memory distress.
    const sample = { pressureLevel: 1, swapUsedMb: 2024.25, swapTotalMb: 3072, compressedMb: 512 };

    const result = classifyMemoryAxis(sample, PRODUCTION_MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    expect(result.state).toBe('GREEN');
  });

  it('[2] boundary still-correct AMBER (regression guard): swapUsedMb=2400/swapTotalMb=3072 (~78%) still classifies AMBER', () => {
    // Proves the eventual fix narrows the false positive rather than
    // disabling the fractional gate outright for genuinely mid-band
    // pressure on a confidently-sized swap total. This case likely already
    // passes today (the fractional gate already fires AMBER here) — kept as
    // a regression guard so the fix does not over-correct to
    // always-GREEN.
    const sample = { pressureLevel: 1, swapUsedMb: 2400, swapTotalMb: 3072, compressedMb: 512 };

    const result = classifyMemoryAxis(sample, PRODUCTION_MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    expect(result.state).toBe('AMBER');
  });

  it('[3] genuinely pressured large-total host still classifies AMBER/RED (regression guard): swapTotalMb=16384, swapUsedMb=12000 (~73%)', () => {
    // Proves the fix does not blanket-disable the fractional (or absolute)
    // signal for large-total hosts. This sample already trips RED today via
    // the absolute redSwapUsedMbAtOrAbove gate (12000 >= 10240) as well as
    // the fractional gate being reachable (swapTotalMb well above any
    // reasonable floor) — kept as a regression guard on both mechanisms.
    const sample = { pressureLevel: 1, swapUsedMb: 12000, swapTotalMb: 16384, compressedMb: 2048 };

    const result = classifyMemoryAxis(sample, PRODUCTION_MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    expect(['AMBER', 'RED']).toContain(result.state);
  });

  it('[4] tiny nonzero swapTotalMb causing fraction >= 1 from noise: swapUsedMb=8/swapTotalMb=4 (fraction=2.0) classifies GREEN, not RED', () => {
    // Today: hasUsableSwapTotal is satisfied (4 > 0, finite) with no floor,
    // so fraction=2.0 >= the 0.9 red threshold trips RED despite 8MB of
    // absolute swap usage being utterly negligible. The fix must treat a
    // swapTotalMb this small as low-confidence and fall back to the
    // absolute-only thresholds, under which 8MB is nowhere near AMBER/RED.
    const sample = { pressureLevel: 0, swapUsedMb: 8, swapTotalMb: 4, compressedMb: 0 };

    const result = classifyMemoryAxis(sample, PRODUCTION_MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    expect(result.state).toBe('GREEN');
  });

  it('[5] swapTotalMb=0 or undefined (regression guard): never throws, falls back to absolute-only thresholds', () => {
    // Already guarded today via hasUsableSwapTotal's finite/positive check
    // (unrelated to the floor this phase adds) — kept as a regression guard
    // so the fix does not disturb this existing, already-correct path.
    const zeroTotal = { pressureLevel: 1, swapUsedMb: 100, swapTotalMb: 0, compressedMb: 16 };
    const undefinedTotal = { pressureLevel: 1, swapUsedMb: 100, swapTotalMb: undefined, compressedMb: 16 };

    expect(() => classifyMemoryAxis(zeroTotal, PRODUCTION_MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG)).not.toThrow();
    expect(() =>
      classifyMemoryAxis(undefinedTotal, PRODUCTION_MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG),
    ).not.toThrow();

    const zeroResult = classifyMemoryAxis(zeroTotal, PRODUCTION_MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    const undefinedResult = classifyMemoryAxis(undefinedTotal, PRODUCTION_MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    // 100MB absolute swap is well below the 4096MB green ceiling, and
    // pressureLevel=1 is at the green pressure ceiling — with the fractional
    // gate skipped (no usable total), both fall back to GREEN via the
    // absolute/pressure checks alone.
    expect(zeroResult.state).toBe('GREEN');
    expect(undefinedResult.state).toBe('GREEN');
    expect(zeroResult.reason).not.toMatch(/NaN|Infinity/);
    expect(undefinedResult.reason).not.toMatch(/NaN|Infinity/);
  });

  it('[6] MIN_CONFIDENT_SWAP_TOTAL_MB floor boundary (proposed value: 512): just below the floor falls back to absolute-only, just at/above it applies the fractional gate', () => {
    // Just below the proposed 512MB floor: swapTotalMb=511, swapUsedMb=460
    // (fraction ≈0.9002, at/above the 0.9 red threshold) — but because the
    // total is below the low-confidence floor, the fractional gate must be
    // skipped entirely; 460MB absolute is nowhere near the 4096MB green
    // ceiling, so this classifies GREEN via the absolute/pressure checks.
    const justBelowFloor = { pressureLevel: 1, swapUsedMb: 460, swapTotalMb: 511, compressedMb: 32 };

    // Just at/above the proposed 512MB floor: swapTotalMb=512,
    // swapUsedMb=461 (fraction ≈0.9004, at/above the 0.9 red threshold) —
    // the total is now confidently sized, so the fractional gate applies
    // normally and this classifies RED.
    const justAtFloor = { pressureLevel: 1, swapUsedMb: 461, swapTotalMb: 512, compressedMb: 32 };

    const belowResult = classifyMemoryAxis(justBelowFloor, PRODUCTION_MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);
    const atResult = classifyMemoryAxis(justAtFloor, PRODUCTION_MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    expect(belowResult.state).toBe('GREEN');
    expect(atResult.state).toBe('RED');
  });

  it('[7] review regression: swapTotalMb in [512, 1024] with high fractional usage still classifies AMBER, not GREEN', () => {
    // swapTotalMb=700 sits just above MIN_CONFIDENT_SWAP_TOTAL_MB (512), so
    // the fractional gate is reachable. swapUsedMb=600 is ~85.7% of total —
    // well above the 0.5 green fraction line. Before the review fix,
    // the noise-tolerant ceiling (`greenSwapUsedFractionOfTotalBelow *
    // swapTotalMb + 512`) evaluated to 862, which is >= swapTotalMb itself
    // for any total in this band — silently disabling the fractional GREEN
    // gate and misclassifying this sample as GREEN. The clamped ceiling
    // (midpoint between the green and red fraction lines: (350 + 630) / 2 =
    // 490) correctly keeps this sample out of GREEN.
    const sample = { pressureLevel: 1, swapUsedMb: 600, swapTotalMb: 700, compressedMb: 64 };

    const result = classifyMemoryAxis(sample, PRODUCTION_MEMORY_THRESHOLDS, undefined, HYSTERESIS_CONFIG);

    expect(result.state).toBe('AMBER');
  });

  it('[8] review Medium finding: fallback ceiling with NO red fraction threshold configured falls back to the strict (untolerated) fraction line, not a 0.75*swapTotalMb midpoint', () => {
    // `greenSwapUsedMbBelow` (the corroborating absolute ceiling) IS
    // configured here, alongside `greenSwapUsedFractionOfTotalBelow` — but
    // `redSwapUsedFractionOfTotalAtOrAbove` is deliberately OMITTED. Every
    // other fixture in this describe block configures both fraction
    // thresholds together, so this fallback branch had zero coverage before
    // this test (see review, Medium finding).
    //
    // swapTotalMb=1000, greenSwapUsedFractionOfTotalBelow=0.5 -> raw green
    // fraction line = 500MB. Before the fix, the fallback treated
    // `swapTotalMb` itself as a stand-in red line, producing a midpoint
    // ceiling of (500 + 1000) / 2 = 750MB (75% of total) — a much larger
    // loosening than the 512MB `GREEN_FRACTION_NOISE_TOLERANCE_MB` noise band
    // this mechanism is supposed to represent. swapUsedMb=700 (70% of total)
    // sits ABOVE the raw 500MB green line but BELOW that buggy 750MB fallback
    // ceiling, and is also comfortably under `greenSwapUsedMbBelow` (4096MB)
    // — so the pre-fix fallback would misclassify this sample as GREEN at
    // 70% swap-of-total.
    //
    // Fixed behaviour: with no red fraction line configured, the fallback
    // skips the noise tolerance entirely and judges the sample on the raw
    // fraction line (500MB) alone — 700MB is above that line, so this must
    // classify AMBER (or RED via the absolute/pressure gates), never GREEN.
    const thresholdsWithoutRedFraction = {
      greenPressureAtOrBelow: 1,
      greenSwapUsedMbBelow: 4096,
      greenSwapUsedFractionOfTotalBelow: 0.5,
      // redSwapUsedFractionOfTotalAtOrAbove intentionally omitted.
    };
    const sample = { pressureLevel: 1, swapUsedMb: 700, swapTotalMb: 1000, compressedMb: 64 };

    const result = classifyMemoryAxis(sample, thresholdsWithoutRedFraction, undefined, HYSTERESIS_CONFIG);

    expect(result.state).not.toBe('GREEN');
  });
});
