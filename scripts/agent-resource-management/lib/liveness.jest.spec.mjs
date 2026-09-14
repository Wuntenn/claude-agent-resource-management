// Phase 1 — per-agent liveness signal (CPU delta + worktree mtime).
// RED by construction: `./lib/liveness.mjs` does not exist yet — this file
// only asserts the contract it must satisfy. Do not add a stub
// implementation to turn this green; that's the next (builder) step.
//
// Contract under test (per the Build Plan's Phase 1 section):
//
//   isAgentStalled({ pid, boundMs, now, footprintTrajectory, worktreeMtimeMs,
//                     minSampleCount? }) -> boolean
//
// A pure function — no I/O, no real `Date.now()`, no real `fs.stat`/`git`
// shell-out. Every input the function needs to judge staleness is threaded
// in explicitly by the caller (mirrors `lib/threshold.mjs`'s and
// `lib/allowance.mjs`'s own "plain mapping from explicit arguments to a
// return value" convention — this is a pure `lib/*.mjs` unit, not a CLI-level
// module, so it takes injected params directly rather than reading
// `ARM_FAKE_*` env vars; those env-var seams belong to the CLI-level
// composition Phase 6 wires on top of this primitive, exercised instead by
// `../watchdog-outer-acceptance.jest.spec.mjs`).
//
// `isAgentStalled` returns `true` only when BOTH signals are flat/stale for
// the configured `boundMs`:
//   (a) CPU-seconds-delta flatness — every sample in `footprintTrajectory`
//       (an already EWMA-smoothed series per `./footprint-trajectory.mjs`)
//       is ~equal (no genuine movement) across at least `minSampleCount`
//       samples.
//   (b) worktree mtime staleness — `worktreeMtimeMs` (the latest git-tracked
//       worktree file mtime observed for this pid, already resolved/filtered
//       by the caller's own noise-path allowlist) is strictly older than
//       `now - boundMs`.
//
// Insufficient sample history (fewer than `minSampleCount` — default 3,
// matching `lib/threshold.mjs`'s `ROLLING_WINDOW_MAX_LENGTH` convention of a
// small fixed minimum) must never be judged as stalled — a just-spawned
// agent has no track record yet, and treating silence as staleness would
// falsely flag it immediately.
//
// An unresolvable worktree path degrades to CPU-only judgment (the caller
// signals this by passing `worktreeMtimeMs: null`) rather than throwing —
// per the ticket's documented edge case, a missing signal must never crash
// the watchdog beat, and must never manufacture a stale-mtime false
// positive by itself: with no mtime evidence at all, only the CPU signal
// carries weight, and CPU flatness ALONE is not sufficient to flag (both
// signals must still be affirmatively flat/stale — a `null` mtime is
// "unknown", not "stale").

import { isAgentStalled } from './liveness.mjs';

const BOUND_MS = 10 * 60 * 1000; // 10 minutes, matching the outer acceptance test's scale
const NOW_MS = 1_700_000_000_000; // arbitrary fixed instant — never real Date.now()

/** A genuinely flat trajectory: every sample identical (default length 5, matching the outer acceptance fixture). */
function flatTrajectory(length = 5, value = 100) {
  return Array.from({ length }, () => value);
}

/** A trajectory with clear movement between samples. */
function activeTrajectory() {
  return [80, 140, 95, 210, 130];
}

describe('isAgentStalled', () => {
  it('flags stalled when CPU flat and no mtime change (both signals agree, past the bound)', () => {
    const staleMtimeMs = NOW_MS - BOUND_MS - 60_000; // strictly older than the bound

    const result = isAgentStalled({
      pid: 9001,
      boundMs: BOUND_MS,
      now: NOW_MS,
      footprintTrajectory: flatTrajectory(),
      worktreeMtimeMs: staleMtimeMs,
    });

    expect(result).toBe(true);
  });

  it('does not flag when CPU is active, even if the worktree mtime looks stale', () => {
    const staleMtimeMs = NOW_MS - BOUND_MS - 60_000;

    const result = isAgentStalled({
      pid: 9001,
      boundMs: BOUND_MS,
      now: NOW_MS,
      footprintTrajectory: activeTrajectory(),
      worktreeMtimeMs: staleMtimeMs,
    });

    expect(result).toBe(false);
  });

  it('does not flag when the worktree mtime recently advanced, even if CPU is flat', () => {
    const freshMtimeMs = NOW_MS - 30_000; // well within the bound

    const result = isAgentStalled({
      pid: 9001,
      boundMs: BOUND_MS,
      now: NOW_MS,
      footprintTrajectory: flatTrajectory(),
      worktreeMtimeMs: freshMtimeMs,
    });

    expect(result).toBe(false);
  });

  it('does not flag with insufficient sample history (just-spawned agent, no track record yet)', () => {
    const staleMtimeMs = NOW_MS - BOUND_MS - 60_000;

    const result = isAgentStalled({
      pid: 9001,
      boundMs: BOUND_MS,
      now: NOW_MS,
      footprintTrajectory: [100], // fewer than the minimum sample count
      worktreeMtimeMs: staleMtimeMs,
    });

    expect(result).toBe(false);
  });

  it('does not flag with zero samples at all (no footprint history collected yet)', () => {
    const staleMtimeMs = NOW_MS - BOUND_MS - 60_000;

    const result = isAgentStalled({
      pid: 9001,
      boundMs: BOUND_MS,
      now: NOW_MS,
      footprintTrajectory: [],
      worktreeMtimeMs: staleMtimeMs,
    });

    expect(result).toBe(false);
  });

  it('honors an explicit minSampleCount override rather than a hardcoded minimum', () => {
    const staleMtimeMs = NOW_MS - BOUND_MS - 60_000;

    // Two flat samples: insufficient under the default minimum, but this
    // test asserts the threshold is a configurable input, not a hardcoded
    // constant, by requiring only 2.
    const result = isAgentStalled({
      pid: 9001,
      boundMs: BOUND_MS,
      now: NOW_MS,
      footprintTrajectory: flatTrajectory(2),
      worktreeMtimeMs: staleMtimeMs,
      minSampleCount: 2,
    });

    expect(result).toBe(true);
  });

  it('degrades to CPU-only (never throws) when the worktree path is unresolvable, and does not flag on CPU flatness alone', () => {
    expect(() =>
      isAgentStalled({
        pid: 9001,
        boundMs: BOUND_MS,
        now: NOW_MS,
        footprintTrajectory: flatTrajectory(),
        worktreeMtimeMs: null, // unresolvable worktree path — degrade, don't throw
      }),
    ).not.toThrow();

    const result = isAgentStalled({
      pid: 9001,
      boundMs: BOUND_MS,
      now: NOW_MS,
      footprintTrajectory: flatTrajectory(),
      worktreeMtimeMs: null,
    });

    // CPU flat but mtime UNKNOWN (not affirmatively stale) — both signals
    // are not affirmatively flat/stale, so this must not flag. This is the
    // same "both signals must agree" AND semantics the ticket's own
    // "CPU flat but mtime fresh, or vice versa" edge case describes, applied
    // to the degraded/unknown case rather than a genuinely fresh one.
    expect(result).toBe(false);
  });

  it('excludes noise paths from the mtime scan — a stale mtime derived only from allowlisted, git-tracked paths still flags when CPU is also flat', () => {
    // This test documents the CONTRACT `isAgentStalled` relies on: the
    // caller (the mtime-scanning layer, not this pure function) is
    // responsible for excluding noisy paths (node_modules, .git/index.lock,
    // lint/test caches) via an explicit allowlist scoped to git-tracked
    // worktree changes before ever calling `isAgentStalled`. Given an
    // already-correctly-filtered `worktreeMtimeMs` (i.e. noise excluded), a
    // genuinely stale result still flags when CPU is flat too — proving this
    // function does not itself need to know about noise paths to honor the
    // combined signal correctly.
    const staleMtimeMsFromAllowlistedFileOnly = NOW_MS - BOUND_MS - 120_000;

    const result = isAgentStalled({
      pid: 9001,
      boundMs: BOUND_MS,
      now: NOW_MS,
      footprintTrajectory: flatTrajectory(),
      worktreeMtimeMs: staleMtimeMsFromAllowlistedFileOnly,
    });

    expect(result).toBe(true);
  });

  it('honors a fake-now seam for the bound calculation — mtime exactly at the bound boundary is not yet stale, one past it is', () => {
    const exactlyAtBoundMs = NOW_MS - BOUND_MS; // exactly boundMs old — not STRICTLY older
    const oneMsPastBoundMs = NOW_MS - BOUND_MS - 1; // strictly older than the bound

    const atBoundResult = isAgentStalled({
      pid: 9001,
      boundMs: BOUND_MS,
      now: NOW_MS,
      footprintTrajectory: flatTrajectory(),
      worktreeMtimeMs: exactlyAtBoundMs,
    });
    expect(atBoundResult).toBe(false);

    const pastBoundResult = isAgentStalled({
      pid: 9001,
      boundMs: BOUND_MS,
      now: NOW_MS,
      footprintTrajectory: flatTrajectory(),
      worktreeMtimeMs: oneMsPastBoundMs,
    });
    expect(pastBoundResult).toBe(true);

    // Same fixture, evaluated against a DIFFERENT injected `now` (the
    // fake-clock seam) rather than a real elapsed wall-clock wait — proves
    // the bound is computed entirely from the injected `now`, never
    // `Date.now()`. An EARLIER injected `now` means less time has elapsed
    // since the same absolute `worktreeMtimeMs`, so the very same mtime that
    // was stale relative to `NOW_MS` falls back within the bound relative to
    // this earlier `now`.
    const earlierNowMs = NOW_MS - 5 * 60 * 1000;
    const noLongerStaleAtEarlierNow = isAgentStalled({
      pid: 9001,
      boundMs: BOUND_MS,
      now: earlierNowMs,
      footprintTrajectory: flatTrajectory(),
      worktreeMtimeMs: oneMsPastBoundMs, // now within bound of the earlier `now`
    });
    expect(noLongerStaleAtEarlierNow).toBe(false);
  });

  it.each([
    ['zero', 0],
    ['negative', -1],
    ['NaN', NaN],
  ])(
    'never flags stalled when minSampleCount is %s, even with an empty trajectory and a stale mtime (vacuous-truth guard)',
    (_label, minSampleCount) => {
      const staleMtimeMs = NOW_MS - BOUND_MS - 60_000; // strictly older than the bound — proves the CPU-flat guard itself is what's tested, not the mtime side falling through

      const resultWithEmptyTrajectory = isAgentStalled({
        pid: 9001,
        boundMs: BOUND_MS,
        now: NOW_MS,
        footprintTrajectory: [],
        worktreeMtimeMs: staleMtimeMs,
        minSampleCount,
      });
      expect(resultWithEmptyTrajectory).toBe(false);

      const resultWithUnderPopulatedTrajectory = isAgentStalled({
        pid: 9001,
        boundMs: BOUND_MS,
        now: NOW_MS,
        footprintTrajectory: flatTrajectory(1),
        worktreeMtimeMs: staleMtimeMs,
        minSampleCount,
      });
      expect(resultWithUnderPopulatedTrajectory).toBe(false);
    },
  );

  it(
    'KNOWN LIMITATION (Convergence Analysis edge case #8): a non-agent writer touching a tracked file can mask a ' +
      'genuinely-hung agent by mtime alone — since BOTH signals must be flat to flag, a fresh mtime from ANY writer ' +
      '(not necessarily the agent itself) suppresses the flag even when CPU is flat, producing a false negative. ' +
      'This test asserts that documented behaviour explicitly rather than silently relying on it.',
    () => {
      // CPU is flat (the agent itself is genuinely hung), but a background
      // git/IDE/linter-watcher process touched a tracked file moments ago —
      // indistinguishable, from this function's inputs alone, from the agent
      // itself being active.
      const freshMtimeFromUnrelatedWriter = NOW_MS - 1_000;

      const result = isAgentStalled({
        pid: 9001,
        boundMs: BOUND_MS,
        now: NOW_MS,
        footprintTrajectory: flatTrajectory(),
        worktreeMtimeMs: freshMtimeFromUnrelatedWriter,
      });

      // False negative, by design of the "both signals must be flat" AND
      // requirement — not a bug in this function, a known, accepted
      // limitation of the mtime signal's provenance.
      expect(result).toBe(false);
    },
  );
});
