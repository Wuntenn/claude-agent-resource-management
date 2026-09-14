// Phase 0 — outer acceptance test for
// "AIMD adjustment of the concurrency ceiling" (Phase 2 of the larger
// agent-resource-management effort; Phase 1 — per-agent phys_footprint
// polling — already landed, and EVICT is still open as a follow-up).
//
// Outer acceptance test (per the Build Plan):
//
//   it("reproduces its incident cohort, shows the AIMD ceiling backing
//   off as pressure climbs to WARN, and keeps admission refused at WARN
//   independent of the ceiling's current value")
//
//   Layer: integration — black-box via child_process spawning of the real
//   `cli.mjs` (matching every sibling `cli.*.jest.spec.mjs` /
//   `*-outer-acceptance.jest.spec.mjs` in this skill — see
//   ../outer-acceptance.jest.spec.mjs and ../evict-outer-acceptance.jest.spec.mjs
//   for the established beat-driven harness this file follows), reading the
//   persisted AIMD state back off the coordination file the same way
//   pressure-override.jest.spec.mjs reads persisted dibs/hysteresis history,
//   plus one small pure-function probe of the AIMD arithmetic itself.
//
//   Green when: Phase 1 (reserved coordination-file AIMD-state entry,
//   mutated only inside withLock/writeEntriesAtomic) + Phase 2 (the pure
//   `aimd-ceiling.mjs` adjustment function) + Phase 3 (wiring the
//   AIMD-adjusted ceiling into all four of cli.mjs's
//   LIVE_AGENT_CEILING-consuming call sites) all land.
//
// RED BY CONSTRUCTION, right now: none of Phases 1-3 exist yet.
//
//   - `./aimd-ceiling.mjs` does not exist at all — the `import` below is
//     expected to fail module resolution. That is an acceptable, intended
//     form of "red" for this file (per this ticket's own instructions): a
//     future reader sees immediately, from the failing import alone, that
//     Phase 2's module has not landed.
//   - `cli.mjs` exports a single FLAT constant, `LIVE_AGENT_CEILING = 4`
//     (cli.mjs line 227), consumed as-is (never adjusted) at all four call
//     sites this ticket's Build Plan Phase 3 names:
//       1. `probeAvailableCapacity`'s `--claim=live-agent:N` pre-lock probe
//          (~cli.mjs line 2032)
//       2. `--dequeue-if-capacity`'s `claimAndAdmitQueueEntry` call
//          (~cli.mjs line 2557)
//       3. `main()`'s `--desired-agents` beat's `reserveAdmission` call
//          (~cli.mjs line 4240)
//       4. the advisory `allowanceConfig` object feeding
//          `computePerOrchestratorAllowance` (~cli.mjs lines 4076-4077)
//     There is no coordination-file entry, reserved id, or any other
//     on-disk state anywhere in this codebase today that records an
//     AIMD-adjusted ceiling, a sustained-Normal-pressure counter, or any
//     multiplicative-decrease history — so every assertion below that reads
//     such state back off the coordination file necessarily fails today
//     (the entry this test looks for is simply absent).
//   - The ONE piece of behaviour this file exercises that is already true
//     today is the Phase 4 pressure-WARN admission block itself
//     (cli.mjs ~line 4017, `type: 'pressure-block'`) — kept here
//     deliberately, not as this file's RED reason, but as the fixed
//     baseline contract Phase 3's ceiling-wiring must NOT regress: refusal
//     at WARN must stay independent of whatever ceiling value AIMD has
//     computed, including an artificially high one. See the final block of
//     this test for why that assertion is expected to already pass.
//
// Do not add a stub implementation to make this pass — that defeats the
// point of the outer/phase red-green loop. Phases 1-3's implementer turns
// this file green by actually building the mechanism; Phase 4 (the
// dedicated regression test replaying this same incident trajectory) closes
// the loop this file opens.
//
// ---------------------------------------------------------------------------
// Seam-shape decisions — UPDATED for Phase 3 against what Phases 1/2
// actually shipped (this file's guesses, below each note, are what it
// originally guessed at Phase 0; kept for the historical record):
//
// 1. PURE AIMD FUNCTION SHAPE — Phase 2 shipped `computeAimdCeiling(state,
//    config)` as a TWO-ARGUMENT function (`./aimd-ceiling.mjs`) — NOT the
//    single-merged-object call this file originally guessed. `state` is
//    `{ pressureLevel, ceiling, sustainedNormalCount }` (note:
//    `sustainedNormalCount`, not this file's originally-guessed
//    `sustainedNormalBeats`) and `config` is `{ floor, ceilingMax,
//    increaseStep, decreaseFactor, sustainedNormalBeatsRequired }`. Updated
//    below to match exactly.
//
// 2. PERSISTED-STATE READBACK SHAPE — Phase 1 shipped the reserved
//    coordination-file entry id as `AIMD_CEILING_RESERVED_ID` (exported from
//    ./coordination-file.mjs) = `'__aimd-ceiling-state__'` — NOT this file's
//    originally-guessed `'__aimd-ceiling__'` — shaped `{ orchestratorId:
//    AIMD_CEILING_RESERVED_ID, ceiling: <int>, sustainedNormalCount: <int>,
//    declaredAt: <int> }`. Updated below to import and use the real
//    constant rather than a hardcoded guess.
//
// 3. ARTIFICIALLY-HIGH CEILING FOR PART 3 — rather than depending on a
//    guessed, Phase-3-only env-config seam (this file originally guessed
//    `ARM_AIMD_CEILING_CONFIG_JSON`, which is no part of any of the four
//    Build Plan Phase 3 call sites and was never confirmed to exist), Part 3
//    below seeds the artificially-high ceiling directly via Phase 1's own
//    `writeAimdCeilingState` on a fresh coordination file — the same
//    mechanism `../cli-aimd-ceiling.jest.spec.mjs`'s dedicated WARN-override
//    tests use. This removes a speculative, unconfirmed seam dependency
//    entirely rather than merely renaming it.
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

// Phase 2's actual shipped export (see seam-shape note 1 above).
import { computeAimdCeiling } from './aimd-ceiling.mjs';
// Phase 1's actual shipped reserved-entry id + write helper (see seam-shape
// notes 2 and 3 above).
import { AIMD_CEILING_RESERVED_ID, writeAimdCeilingState } from './coordination-file.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const CLI = join(__dirname, '..', 'cli.mjs');

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

// its incident cohort: real per-agent footprint ~2GB, climbing toward a
// ~14-agent cohort — the same shape ../outer-acceptance.jest.spec.mjs's
// " outer acceptance" describe block uses, replayed here across a
// sustained-Normal-then-WARN trajectory rather than a single all-at-once
// race.
const COHORT_SIZE = 14;
const SIMULATED_REAL_PEAK_MEMORY_MB_PER_AGENT = 2048;

// Deliberately wrong / overly-permissive headroom stub (identical rationale
// to ../outer-acceptance.jest.spec.mjs's WRONG_PERMISSIVE_MEMORY_STUB):
// nothing but a real ceiling — flat or AIMD-adjusted — should be able to
// refuse this cohort while pressureLevel stays Normal.
function memoryNormal() {
  return { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };
}

function memoryWarn() {
  return { pressureLevel: 2, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };
}

/** Runs the real cli.mjs as a child process — never imports it directly (matches every sibling cli.*.jest.spec.mjs). */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/**
 * Reads the shared AIMD-ceiling entry back off the coordination file, or
 * `undefined` if no such entry exists (true of every coordination file on
 * this branch today, since Phase 3 hasn't wired it into cli.mjs yet).
 */
function readAimdCeilingEntry(coordinationFilePath) {
  const onDisk = JSON.parse(readFileSync(coordinationFilePath, 'utf8'));
  return onDisk.find((entry) => entry.orchestratorId === AIMD_CEILING_RESERVED_ID);
}

let workDir;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-aimd-outer-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

it(
  "reproduces its incident cohort, shows the AIMD ceiling backing off as pressure climbs to WARN, and " +
    "keeps admission refused at WARN independent of the ceiling's current value",
  async () => {
    expect(SIMULATED_REAL_PEAK_MEMORY_MB_PER_AGENT).toBe(2048);

    // -----------------------------------------------------------------
    // Part 0 — pure-function probe of the AIMD arithmetic itself
    // (Phase 2's contract), independent of any cli.mjs wiring. Proves the
    // additive-increase and multiplicative-decrease shape in isolation
    // before relying on it end-to-end below.
    // -----------------------------------------------------------------
    const aimdConfig = {
      floor: 1,
      ceilingMax: 20,
      increaseStep: 1,
      decreaseFactor: 0.5,
      sustainedNormalBeatsRequired: 3,
    };

    // Sustained Normal pressure (pressureLevel: 1) for
    // `sustainedNormalBeatsRequired` consecutive beats must additively
    // increase the ceiling by `increaseStep` above its starting value.
    // `computeAimdCeiling` is Phase 2's actual TWO-ARGUMENT shape:
    // `(state, config)`, not a single merged object.
    let state = { ceiling: 4, sustainedNormalCount: 0 };
    for (let beat = 0; beat < aimdConfig.sustainedNormalBeatsRequired; beat += 1) {
      state = computeAimdCeiling({ pressureLevel: 1, ...state }, aimdConfig);
    }
    expect(state.ceiling).toBeGreaterThan(4);
    expect(Number.isInteger(state.ceiling)).toBe(true);

    // The instant pressure reaches WARN (pressureLevel: 2), the ceiling
    // must multiplicatively decrease below whatever it had climbed to —
    // not merely fail to increase further.
    const preWarnCeiling = state.ceiling;
    const afterWarn = computeAimdCeiling({ pressureLevel: 2, ...state }, aimdConfig);
    expect(afterWarn.ceiling).toBeLessThan(preWarnCeiling);
    expect(afterWarn.ceiling).toBeGreaterThanOrEqual(aimdConfig.floor);
    expect(Number.isInteger(afterWarn.ceiling)).toBe(true);

    // -----------------------------------------------------------------
    // Part 1 — replay its incident cohort end to end through the real
    // `cli.mjs`, beat by beat, under SUSTAINED Normal pressure: the
    // AIMD-adjusted ceiling must additively climb past the old flat
    // LIVE_AGENT_CEILING (4), admitting more of the 14-agent cohort than a
    // flat ceiling ever could, entirely independent of a deliberately
    // over-permissive freeRamMb reading.
    // -----------------------------------------------------------------
    const normalEnv = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: memoryNormal(), disk: DISK_GREEN }),
    };

    let totalAdmittedUnderSustainedNormal = 0;
    for (let index = 0; index < COHORT_SIZE; index += 1) {
      const beat = runCli(
        [`--orchestrator-id=cohort-agent-${index}`, '--desired-agents=1'],
        normalEnv,
      );
      expect(beat.status).toBe(0);
      const parsed = JSON.parse(beat.stdout);
      totalAdmittedUnderSustainedNormal += parsed.liveAgentGrant ?? 0;
    }

    // FAILS TODAY: with no AIMD adjustment, LIVE_AGENT_CEILING stays flat
    // at 4 across every one of the 14 sequential beats above, so the total
    // admitted across sustained Normal pressure can never exceed 4 — this
    // assertion requires the ceiling to have climbed past that flat value.
    expect(totalAdmittedUnderSustainedNormal).toBeGreaterThan(4);

    // The shared AIMD ceiling itself, read back off the coordination file,
    // must have additively climbed above the old flat LIVE_AGENT_CEILING
    // (4) after this many consecutive Normal-pressure beats. FAILS TODAY:
    // no such entry exists on any coordination file this branch produces.
    const climbedEntry = readAimdCeilingEntry(coordinationFilePath);
    expect(climbedEntry).toBeDefined();
    expect(climbedEntry.ceiling).toBeGreaterThan(4);
    const climbedCeiling = climbedEntry.ceiling;

    // -----------------------------------------------------------------
    // Part 2 — pressure climbs to WARN: the shared AIMD ceiling must back
    // off (multiplicative decrease) from whatever it climbed to in Part 1,
    // proving the AIMD loop actually closes under real pressure, not just
    // in the pure-function probe above.
    // -----------------------------------------------------------------
    const warnEnv = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: memoryWarn(), disk: DISK_GREEN }),
    };

    const warnBeat = runCli(['--orchestrator-id=cohort-agent-warn', '--desired-agents=1'], warnEnv);
    expect(warnBeat.status).toBe(0);

    // FAILS TODAY: no such entry exists, so this necessarily fails before
    // the ceiling-comparison assertion is even reached.
    const backedOffEntry = readAimdCeilingEntry(coordinationFilePath);
    expect(backedOffEntry).toBeDefined();
    expect(backedOffEntry.ceiling).toBeLessThan(climbedCeiling);

    // -----------------------------------------------------------------
    // Part 3 — admission stays refused at WARN independent of the
    // ceiling's current value: seed the AIMD ceiling artificially HIGH
    // directly via Phase 1's `writeAimdCeilingState` (not a guessed env
    // seam) on a FRESH coordination file, and prove pressureLevel >= 2
    // still refuses admission even though the ceiling itself would happily
    // allow it. This is the Phase 0/Phase 4 WARN/CRITICAL override
    // contract this ticket must not regress — it is EXPECTED TO ALREADY
    // PASS today (the pressure-block pre-check, cli.mjs ~line 4131, already
    // runs and refuses independent of any ceiling value), kept here so a
    // future regression in Phase 3's ceiling-wiring is caught by THIS SAME
    // outer test, not silently reintroduced.
    // -----------------------------------------------------------------
    const highCeilingWorkDir = await mkdtemp(join(tmpdir(), 'arm-aimd-outer-high-ceiling-'));
    const highCeilingCoordinationFilePath = join(highCeilingWorkDir, 'coordination.json');

    try {
      await writeAimdCeilingState(
        highCeilingCoordinationFilePath,
        { ceiling: 999, sustainedNormalCount: 0 }, // artificially generous — proves refusal isn't merely "ceiling too low"
        { now: 1_700_000_000_000 },
      );

      const highCeilingEnv = {
        ARM_COORDINATION_FILE: highCeilingCoordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: memoryWarn(), disk: DISK_GREEN }),
      };

      const highCeilingBeat = runCli(
        ['--orchestrator-id=cohort-agent-high-ceiling', '--desired-agents=1'],
        highCeilingEnv,
      );
      expect(highCeilingBeat.status).toBe(0);
      const parsedHighCeiling = JSON.parse(highCeilingBeat.stdout);

      expect(parsedHighCeiling.type).not.toBe('spawn-allowed');
      expect(parsedHighCeiling.liveAgentGrant).toBeNull();
    } finally {
      await rm(highCeilingWorkDir, { recursive: true, force: true });
    }
  },
  30_000,
);
