// Phase 4 — dedicated regression test replaying the incident
// trajectory through the real, already-shipped AIMD ceiling mechanism
// (Phases 1-3, landed clean). This file is NOT a red-to-green driver for new
// production code — every scenario below already passes against today's
// `cli.mjs` + `./aimd-ceiling.mjs` + `./coordination-file.mjs`. Its job is to
// pin, as dedicated regression coverage, two edge cases the outer acceptance
// test (`./aimd-ceiling-outer-acceptance.jest.spec.mjs`) does not cover:
//
//   1. The "backing off" claim, stated in an unambiguous shape: the ceiling
//      is NOT strictly monotonically decreasing across the whole trajectory
//      (additive-increase beats before WARN legitimately raise it — that is
//      correct AIMD behaviour, not a bug) — only the SPECIFIC WARN-triggered
//      beat must show a multiplicative decrease from whatever the ceiling had
//      climbed to immediately before it.
//   2. The recovery leg after a WARN-triggered decrease: pressure returning
//      to Normal must NOT immediately restore the pre-WARN ceiling.
//      `sustainedNormalCount` is reset to 0 by the WARN beat (per
//      `computeAimdCeiling`'s own documented contract, ./aimd-ceiling.mjs) —
//      additive increase only resumes once a FRESH run of
//      `sustainedNormalBeatsRequired` (5, `DEFAULT_AIMD_CEILING_CONFIG`,
//      ../cli.mjs) consecutive Normal beats, counted from AFTER the WARN
//      beat, is observed again.
//
// The single coherent "WARN override refusal is independent of ceiling
// value" scenario the Build Plan's Phase 4 AC also names is ALREADY covered,
// end to end, by `./aimd-ceiling-outer-acceptance.jest.spec.mjs`'s Part 3 —
// not duplicated here. See that file for the artificially-high-ceiling +
// WARN-refusal scenario.
//
// Beat-driven harness (no flakiness): every beat below is one real,
// SEQUENTIAL `spawnSync` child-process invocation of `cli.mjs`, coordinated
// entirely through `ARM_FAKE_COLLECT_JSON` (which pressure/disk reading this
// beat "observes") and a shared, on-disk coordination file — never a real
// sleep or `setTimeout`. This matches every sibling `cli.*.jest.spec.mjs` and
// `*-outer-acceptance.jest.spec.mjs` in this directory (see e.g.
// ./aimd-ceiling-outer-acceptance.jest.spec.mjs and
// ../cli-two-orchestrators.jest.spec.mjs) — the established
// deterministic-without-real-timing idiom this whole skill uses; there is no
// literal Jest fake-timers usage anywhere in this directory because none of
// these tests drive cli.mjs's own internal clock — they drive it beat by
// beat as separate real invocations instead.

import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { AIMD_CEILING_RESERVED_ID } from './coordination-file.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const CLI = join(__dirname, '..', 'cli.mjs');

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const MEMORY_NORMAL = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };
const MEMORY_WARN = { pressureLevel: 2, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };

// `DEFAULT_AIMD_CEILING_CONFIG` (../cli.mjs) — mirrored here as a literal,
// not imported, so this regression test independently pins the exact tuning
// values the real config carries rather than trivially re-asserting whatever
// the constant happens to say today.
const SUSTAINED_NORMAL_BEATS_REQUIRED = 5;
const COLD_START_CEILING = 4;

/** Runs the real cli.mjs as a child process — never imports it directly. */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

function readAimdCeilingEntry(coordinationFilePath) {
  const onDisk = JSON.parse(readFileSync(coordinationFilePath, 'utf8'));
  return onDisk.find((entry) => entry.orchestratorId === AIMD_CEILING_RESERVED_ID);
}

function readCeiling(coordinationFilePath) {
  const entry = readAimdCeilingEntry(coordinationFilePath);
  expect(entry).toBeDefined();
  return entry.ceiling;
}

/** Sends one Normal-pressure `--desired-agents` beat and returns the resulting persisted ceiling. */
function beatNormal(coordinationFilePath, orchestratorId) {
  const env = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_NORMAL, disk: DISK_GREEN }),
  };
  const result = runCli([`--orchestrator-id=${orchestratorId}`, '--desired-agents=1'], env);
  expect(result.status).toBe(0);
  return readCeiling(coordinationFilePath);
}

/** Sends one WARN-pressure `--desired-agents` beat and returns the resulting persisted ceiling. */
function beatWarn(coordinationFilePath, orchestratorId) {
  const env = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_WARN, disk: DISK_GREEN }),
  };
  const result = runCli([`--orchestrator-id=${orchestratorId}`, '--desired-agents=1'], env);
  expect(result.status).toBe(0);
  const body = JSON.parse(result.stdout);
  expect(body.type).toBe('pressure-block'); // WARN still short-circuits admission — unchanged contract.
  return readCeiling(coordinationFilePath);
}

let workDir;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-aimd-incident-replay-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

describe('incident replay — explicit "backing off" semantics (eventually-lower across the WARN leg, not strictly monotonic overall)', () => {
  it(
    'climbs across two full sustained-Normal windows (correct additive-increase, NOT a violation of "backing off"), then shows the multiplicative decrease specifically on the WARN-triggered beat',
    () => {
      const orchestratorId = 'incident-replay-backing-off';
      const trajectory = [];

      // Two full sustained-Normal windows: 2 * SUSTAINED_NORMAL_BEATS_REQUIRED
      // beats. Each window's LAST beat crosses the threshold and additively
      // grows the ceiling by 1 — this is EXPECTED to raise the ceiling twice
      // in a row here, which is correct AIMD behaviour, not a contradiction
      // of "the ceiling backs off". A future reader must not "fix" this
      // climb thinking it violates backing-off — that is exactly what this
      // test's own name and structure exist to make unambiguous.
      for (let beat = 0; beat < SUSTAINED_NORMAL_BEATS_REQUIRED * 2; beat += 1) {
        trajectory.push(beatNormal(coordinationFilePath, orchestratorId));
      }

      // Across the whole climb, the ceiling only ever holds steady or rises
      // — never dips — proving the climbing phase itself is monotonically
      // NON-decreasing (the additive-increase half of AIMD, isolated from
      // the multiplicative-decrease half this test goes on to exercise).
      for (let i = 1; i < trajectory.length; i += 1) {
        expect(trajectory[i]).toBeGreaterThanOrEqual(trajectory[i - 1]);
      }
      expect(trajectory[trajectory.length - 1]).toBe(COLD_START_CEILING + 2);

      const preWarnCeiling = trajectory[trajectory.length - 1];

      // THE assertion this test exists to pin: the WARN-triggered beat's
      // resulting ceiling is LOWER than the immediately-preceding value —
      // not "lower than some earlier point in the climb", specifically the
      // beat-over-beat comparison across the WARN leg itself.
      const postWarnCeiling = beatWarn(coordinationFilePath, orchestratorId);
      expect(postWarnCeiling).toBeLessThan(preWarnCeiling);
      expect(postWarnCeiling).toBe(Math.floor(preWarnCeiling * 0.5));
    },
  );
});

describe('incident replay — recovery leg after a WARN-triggered decrease', () => {
  it(
    'does not restore the pre-WARN ceiling on the very next Normal beats — additive increase only resumes after a FRESH sustained-Normal run counted from after the WARN beat',
    () => {
      const orchestratorId = 'incident-replay-recovery';

      // (a) Climb the ceiling under sustained Normal pressure, one full
      // window, from the cold-start default — then send 3 MORE Normal beats
      // (short of a second full window) so `sustainedNormalCount` is
      // non-zero (3) at the moment WARN hits, the realistic incident shape:
      // WARN can interrupt a partially-built-up streak mid-window, not only
      // land exactly on a window boundary. This is also what gives the
      // upcoming break-it-and-see-it-fail check its teeth — if WARN hit
      // exactly on a boundary (count already 0), a broken "don't reset
      // sustainedNormalCount" would be indistinguishable from the correct
      // reset; a mid-window WARN (count=3) is where the two diverge, since
      // only 2 MORE beats after WARN would then suffice to cross the
      // threshold from a carried-over 3, instead of the full fresh run of 5
      // this test's step (c)/(d) below actually requires.
      let ceiling;
      for (let beat = 0; beat < SUSTAINED_NORMAL_BEATS_REQUIRED; beat += 1) {
        ceiling = beatNormal(coordinationFilePath, orchestratorId);
      }
      expect(ceiling).toBe(COLD_START_CEILING + 1);
      for (let beat = 0; beat < 3; beat += 1) {
        ceiling = beatNormal(coordinationFilePath, orchestratorId);
      }
      expect(ceiling).toBe(COLD_START_CEILING + 1); // still short of a second full window — no further climb yet.
      const preWarnCeiling = ceiling;

      // (b) Trigger a WARN-driven multiplicative decrease.
      const postWarnCeiling = beatWarn(coordinationFilePath, orchestratorId);
      expect(postWarnCeiling).toBeLessThan(preWarnCeiling);
      expect(postWarnCeiling).toBe(Math.floor(preWarnCeiling * 0.5));

      // (c) Pressure returns to Normal, but for FEWER beats than the
      // threshold requires (2 of the 5 needed). The ceiling must NOT have
      // climbed back at all yet — it must stay pinned at the post-WARN
      // value, proving `sustainedNormalCount` was genuinely reset to 0 by
      // the WARN beat rather than merely paused or partially retained.
      let recoveryCeiling;
      for (let beat = 0; beat < SUSTAINED_NORMAL_BEATS_REQUIRED - 3; beat += 1) {
        recoveryCeiling = beatNormal(coordinationFilePath, orchestratorId);
      }
      expect(recoveryCeiling).toBe(postWarnCeiling);
      expect(recoveryCeiling).not.toBe(preWarnCeiling);

      // (d) Send enough ADDITIONAL consecutive Normal beats to reach the
      // threshold, counted fresh from after the WARN beat (2 already sent in
      // (c), 3 more here = 5 total). The ceiling must THEN climb — by
      // exactly `increaseStep` (1) — from the POST-WARN value, not from the
      // original pre-WARN value. Climbing from the pre-WARN value would mean
      // the WARN-triggered decrease was effectively undone rather than
      // genuinely recovered from.
      let climbedAgainCeiling;
      for (let beat = 0; beat < 3; beat += 1) {
        climbedAgainCeiling = beatNormal(coordinationFilePath, orchestratorId);
      }
      expect(climbedAgainCeiling).toBe(postWarnCeiling + 1);
      expect(climbedAgainCeiling).toBeLessThan(preWarnCeiling);
    },
  );
});
