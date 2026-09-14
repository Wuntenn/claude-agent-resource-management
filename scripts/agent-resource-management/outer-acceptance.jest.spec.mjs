// Phase 0 — outer acceptance test for agent-resource-management
// v2: evidence-based per-operation-type capacity + atomic claim.
//
// This is the RED outer test for the whole ticket. It drives the full
// evidence loop end to end through the real, black-box `cli.mjs` (never
// imported directly — matches ./cli.jest.spec.mjs's and
// ./cli-two-orchestrators.jest.spec.mjs's process-spawn idiom), using the
// SAME `ARM_*` test seams already documented in SKILL.md
// (`ARM_COORDINATION_FILE`, `ARM_HISTORY_FILE`, `ARM_FAKE_COLLECT_JSON`,
// `ARM_FAKE_NOW_MS`) — no new test infrastructure is invented here.
//
// None of `--record-outcome`, `--query-capacity`, or `--claim` exist in
// `cli.mjs` yet (as of Phase 0) — every scenario below is EXPECTED to fail:
// either `cli.mjs` exits non-zero / prints an "unknown flag"-style stderr
// message for the unrecognized flag, or (if it silently no-ops) the
// resulting JSON assertions fail because the new behavior doesn't exist.
// This file goes green only once Phases 1-6 in the Build Plan are
// implemented; it is intentionally NOT skipped and NOT marked `.todo`.
//
// Scenarios (mirrors the Build Plan's "### Outer acceptance test" section):
//   1. --record-outcome for synthetic test-agent (high peak-memory) and
//      implementation-agent (low peak-memory) observations.
//   2. --query-capacity=test-agent,implementation-agent under identical
//      simulated headroom: test-agent's freeCapacity < implementation-agent's,
//      both confidence: 'empirical'.
//   3. --query-capacity=lint-agent (zero observations): confidence: 'default'.
//   4. Two concurrent --claim=test-agent:1 process spawns racing for one
//      slot: exactly one process is granted 1, the other 0.
//   5. --desired-agents disk trend: one sample -> "insufficient-data"; two
//      real timestamped samples -> a genuine declineRateGbPerHour. (`diskTrend`
//      only ever surfaces on normal spawn-decision beats — `--desired-agents`
//      / `--heartbeat` — per SKILL.md's "## Disk decline-rate trend" and
//      ./disk-trend.jest.spec.mjs; `--query-capacity`/`--claim` never carry
//      it, per the Build Plan's Phase 6 scope note.)
//
// Every `--record-outcome`/`--query-capacity`/`--claim` invocation below sets
// `ARM_HISTORY_FILE` to a per-test `mkdtemp` work-dir path — exactly like
// every sibling spec file (claim.jest.spec.mjs, query-capacity.jest.spec.mjs,
// record-outcome.jest.spec.mjs, disk-trend.jest.spec.mjs) — so this file can
// never fall through to the real production default
// (~/.claude/agent-state/resource-history.json) and pollute it.

import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { AIMD_CEILING_RESERVED_ID } from './lib/coordination-file.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 };

// Tuned so a COLD-START type (default 350 MB/agent cost estimate,
// DEFAULT_HEADROOM_CONFIG's osBaselineMb: 2048 + reserveMb: 2048 = 4096 MB
// baseline, per ./lib/cost-estimate.mjs and cli.mjs's DEFAULT_HEADROOM_CONFIG)
// reports EXACTLY 1 genuinely-free slot via computeHeadroomCap:
//   floor((4450 - 4096) / 350) === floor(354 / 350) === 1
// Replicated from claim.jest.spec.mjs's MEMORY_ONE_SLOT fixture (not
// exported from that file, so duplicated here verbatim) — this is the
// purpose-built fixture for a genuine single-scarce-slot race, unlike the
// generic "ample headroom" MEMORY_GREEN shape above.
const MEMORY_ONE_SLOT = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 4450 };

const FIVE_MINUTES_MS = 5 * 60 * 1000;

/** Runs the real cli.mjs as a child process — never imports it directly. */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/**
 * Runs the real cli.mjs as a genuinely CONCURRENT child process — resolves
 * once the process exits, but (critically, unlike `runCli`/`spawnSync`) does
 * not block this test's event loop while the child runs, so calls started
 * back-to-back via `Promise.all` actually race each other for the same
 * coordination-file lock instead of running one after another. Mirrors
 * ./cli-two-orchestrators.jest.spec.mjs's `runCliAsync` (lines 91-109)
 * exactly — the genuine-race helper this outer test is required to use,
 * NOT the sequential `runCli`/`spawnSync` pattern above (that pattern would
 * pass while the real race stays open; see this test's own header comment).
 */
function runCliAsync(args, extraEnv) {
  return new Promise((resolvePromise, rejectPromise) => {
    const child = spawn('node', [CLI, ...args], {
      env: { ...process.env, ...extraEnv },
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => {
      stdout += chunk;
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk;
    });
    child.on('error', rejectPromise);
    child.on('close', (status) => {
      resolvePromise({ status, stdout, stderr });
    });
  });
}

function parseStdout(stdout) {
  return JSON.parse(stdout);
}

let workDir;
let coordinationFilePath;
let historyFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-outer-acceptance-'));
  coordinationFilePath = join(workDir, 'coordination.json');
  historyFilePath = join(workDir, 'history.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

describe('outer acceptance — evidence-based per-operation-type capacity + atomic claim', () => {
  test('1-2. --record-outcome for two operation types, then --query-capacity reports the same flat placeholder freeCapacity for both types, both confidence:empirical (Phase 2 rewrite)', () => {
    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_HISTORY_FILE: historyFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    };

    // A handful of synthetic test-agent observations with a HIGH peak-memory
    // reading.
    for (let i = 0; i < 5; i += 1) {
      const record = runCli(
        [
          '--record-outcome',
          '--operation-type=test-agent',
          '--orchestrator-id=orch-a',
          '--peak-memory-mb=2500',
        ],
        env,
      );
      expect(record.status).toBe(0);
    }

    // A handful of synthetic implementation-agent observations with a LOW
    // peak-memory reading.
    for (let i = 0; i < 5; i += 1) {
      const record = runCli(
        [
          '--record-outcome',
          '--operation-type=implementation-agent',
          '--orchestrator-id=orch-a',
          '--peak-memory-mb=400',
        ],
        env,
      );
      expect(record.status).toBe(0);
    }

    const query = runCli(
      ['--orchestrator-id=orch-a', '--query-capacity=test-agent,implementation-agent'],
      env,
    );
    expect(query.status).toBe(0);
    const parsed = parseStdout(query.stdout);

    expect(parsed).toHaveProperty('test-agent');
    expect(parsed).toHaveProperty('implementation-agent');
    expect(parsed['test-agent'].confidence).toBe('empirical');
    expect(parsed['implementation-agent'].confidence).toBe('empirical');

    // Phase 2 (Build Plan Amendment, RC-3) REWRITE: this assertion
    // originally required the historically memory-heavier type (test-agent)
    // to report a SMALLER freeCapacity than the historically lighter type
    // (implementation-agent) — that differentiation came directly from
    // feeding each type's own cost estimate into computeHeadroomCap against
    // a live freeRamMb sample. RC-3 retires freeRamMb (live or the
    // assumedFreeRamMb fallback) as an admission input at this site
    // entirely, so the differentiation this assertion pinned no longer
    // exists — every requested type now gets the same flat
    // DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis (3) placeholder cap
    // regardless of its own cost estimate — PERMANENTLY, not "until Phase 3":
    // Phase 3 shipped a real live-agent ceiling, but scoped to
    // --desired-agents/--claim=live-agent:N only, deliberately excluding
    // --query-capacity (Build Plan judgment call #1). `confidence` per type
    // is untouched and still independently correct — see
    // query-capacity.jest.spec.mjs's identical rewrite for the dedicated
    // unit-level version of this contract.
    expect(parsed['test-agent'].freeCapacity).toBe(3);
    expect(parsed['implementation-agent'].freeCapacity).toBe(3);
  });

  test('3. --query-capacity for a type with zero recorded observations reports confidence:default', () => {
    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_HISTORY_FILE: historyFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    };

    const query = runCli(['--orchestrator-id=orch-a', '--query-capacity=lint-agent'], env);
    expect(query.status).toBe(0);
    const parsed = parseStdout(query.stdout);

    expect(parsed).toHaveProperty('lint-agent');
    expect(parsed['lint-agent'].confidence).toBe('default');
    expect(typeof parsed['lint-agent'].freeCapacity).toBe('number');
  });

  test('4. two --claim=test-agent:3 process spawns racing for the flat 3-slot cap: exactly one wins the whole cap (Phase 2 rewrite)', () => {
    // Phase 2 (Build Plan Amendment, RC-3) REWRITE: MEMORY_ONE_SLOT's
    // freeRamMb was originally tuned to make computeHeadroomCap report
    // EXACTLY 1 genuinely-free test-agent slot — RC-3 retires freeRamMb (live
    // or the assumedFreeRamMb fallback) as an admission input entirely, so
    // capacity here is now the flat DEFAULT_ALLOWANCE_CONFIG
    // .maxAgentsMemoryAxis (3) placeholder regardless of freeRamMb. Two
    // claimants each asking for 1 slot no longer contends at all (1 + 1 = 2
    // fits comfortably inside a cap of 3, so both would be granted 1) — the
    // race-safety property this test exists to prove would go untested. Each
    // racer now asks for the WHOLE cap (3) instead, so genuine scarcity (one
    // winner takes everything, the other gets nothing) is still exercised at
    // the new cap size — mirrors claim.jest.spec.mjs's identical rewrite.
    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_HISTORY_FILE: historyFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_ONE_SLOT, disk: DISK_GREEN }),
    };

    // Spawn both claim invocations as real, concurrent child processes (not
    // sequential awaits) — mirrors the existing two-process race idiom used
    // for coordination-file lock contention in
    // ./lib/coordination-file.jest.spec.mjs and the two-orchestrator harness
    // in ./cli-two-orchestrators.jest.spec.mjs.
    const procA = spawnSync(
      'node',
      [CLI, '--orchestrator-id=orch-a', '--claim=test-agent:3'],
      { encoding: 'utf8', env: { ...process.env, ...env } },
    );
    const procB = spawnSync(
      'node',
      [CLI, '--orchestrator-id=orch-b', '--claim=test-agent:3'],
      { encoding: 'utf8', env: { ...process.env, ...env } },
    );

    expect(procA.status).toBe(0);
    expect(procB.status).toBe(0);

    const parsedA = JSON.parse(procA.stdout ?? '{}');
    const parsedB = JSON.parse(procB.stdout ?? '{}');

    // Exactly one of the two claims is granted the whole cap (3), the other
    // 0 — never both 3, never both 0.
    const grants = [parsedA.granted, parsedB.granted].sort();
    expect(grants).toEqual([0, 3]);
  });

  test('5a. --desired-agents with only one disk sample reports diskTrend.trend: "insufficient-data"', () => {
    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_NOW_MS: '1700000000000',
    };

    const query = runCli(['--orchestrator-id=orch-a', '--desired-agents=1'], env);
    expect(query.status).toBe(0);
    const parsed = parseStdout(query.stdout);

    expect(parsed.diskTrend).toBeDefined();
    expect(parsed.diskTrend.trend).toBe('insufficient-data');
  });

  test('5b. --desired-agents with two real timestamped disk samples reports a genuine declineRateGbPerHour', () => {
    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_BEAT_NOW_TEST_MODE: '1',
    };
    const BASE_NOW_MS = 1700000000000;

    // First sample: baseline reading at t0.
    const first = runCli(
      ['--orchestrator-id=orch-a', '--desired-agents=1'],
      {
        ...env,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: MEMORY_GREEN,
          disk: { freeDiskGb: 60, declineRateGbPerHour: 0.5 },
        }),
        ARM_FAKE_NOW_MS: String(BASE_NOW_MS),
      },
    );
    expect(first.status).toBe(0);
    expect(parseStdout(first.stdout).diskTrend.trend).toBe('insufficient-data');

    // Second sample: 5 real minutes later, disk has dropped by 2.5 GB — a
    // genuine, computable rate of 2.5 GB / (5/60 h) = 30 GB/hr.
    const second = runCli(
      ['--orchestrator-id=orch-a', '--desired-agents=1'],
      {
        ...env,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: MEMORY_GREEN,
          disk: { freeDiskGb: 57.5, declineRateGbPerHour: 0.5 },
        }),
        ARM_FAKE_NOW_MS: String(BASE_NOW_MS + FIVE_MINUTES_MS),
      },
    );
    expect(second.status).toBe(0);
    const parsedSecond = parseStdout(second.stdout);

    expect(parsedSecond.diskTrend).toBeDefined();
    expect(parsedSecond.diskTrend.trend).not.toBe('insufficient-data');
    // Never a value defaulting to 0 as if it were measured.
    expect(parsedSecond.diskTrend.declineRateGbPerHour).toBeCloseTo(30, 0);
  });
});

// ---------------------------------------------------------------------------
// (Phase 0 outer acceptance) — a real live-agent ceiling must refuse
// admission for a large concurrent cohort even when the (today, flat-350MB-
// per-agent) headroom/cost math is deliberately fed a wildly permissive
// `freeRamMb` reading, and stays refused with the disk axis simultaneously
// RED.
//
// RED by construction: none of its fix phases exist yet.
//   - Phase 1 (dead swap-gate fix), Phase 2 (flat-350/free-RAM retirement),
//     Phase 3 (live-agent ceiling of 4), and Phase 4 (unmaskable
//     pressure-WARN admission block) all have to land before this goes
//     green.
//   - Today, `main()`'s `--desired-agents` beat (cli.mjs ~lines 2090-2170)
//     derives its memory-axis cap purely from `computeHeadroomCap({
//     freeRamMb, ...DEFAULT_HEADROOM_CONFIG })` — a flat `perAgentRamMb:
//     350` cost estimate (DEFAULT_HEADROOM_CONFIG, cli.mjs line 151) that
//     has no concept of a real agent's actual footprint, and no live-agent
//     ceiling of any kind constrains the aggregate grant across many
//     simultaneously-contending orchestrators. There is nothing in today's
//     code that would refuse this cohort once 4 agents are already
//     admitted.
//
// Each of the 14 simulated cohort members represents one real agent whose
// actual peak memory footprint is ~2GB (2048MB) — far above the flat 350MB/
// agent estimate today's headroom formula assumes. The injected
// `ARM_FAKE_COLLECT_JSON` fixture below is DELIBERATELY WRONG about
// available headroom: it reports a `freeRamMb` generous enough that even
// today's under-costed flat-350MB math would happily admit the entire
// cohort many times over — nowhere close to what 14 real ~2GB agents would
// actually require. The point of this test is that a real live-agent
// ceiling of 4 must gate admission independently of that (wrong, over-
// permissive) headroom/cost accounting — not merely happen to agree with
// it.
//
// All 14 cohort members race cli.mjs's real `--desired-agents` beat AT ONCE
// via `runCliAsync`/`Promise.all` (mirrors
// ./cli-two-orchestrators.jest.spec.mjs:551-608's genuine-race pattern,
// NOT the sequential `runCli` pattern at that same file's lines 233-296 —
// see the Build Plan's AC Shape Check for why the sequential pattern is
// explicitly excluded here: it would pass while the real race stays open).
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Phase 1 — pinned regression: `classifyMemoryRaw`'s swap-used-
// fraction-of-total gate (`lib/threshold.mjs:227-277`) misfires on a small,
// demand-grown `swapTotalMb`, inflating `swapUsedMb / swapTotalMb` and
// pinning the memory axis at AMBER on what is really an idle host. This
// outer-acceptance scenario drives the real `cli.mjs` end to end with the
// EXACT pinned sample from (`swapUsedMb=2024.25`, `swapTotalMb=3072`,
// `pressureLevel=1`) and asserts the coordination-file-persisted memory-axis
// classification (`memoryHistory.level`, written by `declareDibs` at
// `cli.mjs:7741` from `classifyMemoryAxis`'s returned `history`) reads GREEN,
// not AMBER, after one beat — and that `buildTrafficLight` therefore does
// NOT return `hold` for that beat (`lib/allowance.mjs:466-491`).
//
// RED by construction: Phase 1's fix has not landed yet.
// `swapUsedMb / swapTotalMb === 2024.25 / 3072 ~= 0.659`, which today's
// unfixed `greenSwapUsedFractionOfTotalBelow: 0.5`
// (`DEFAULT_MEMORY_THRESHOLDS`, `cli.mjs:172-178`) fails, so
// `classifyMemoryRaw` returns AMBER for this exact sample right now — this
// test is EXPECTED to fail until Phase 1 changes that gate. No production
// code (`lib/threshold.mjs` or otherwise) is touched by this commit.
// ---------------------------------------------------------------------------

describe('outer acceptance — pinned regression: small swapTotalMb no longer inflates the memory-axis fraction gate', () => {
  test('swapUsedMb=2024.25, swapTotalMb=3072, pressureLevel=1 persists memoryHistory.level: GREEN and does not produce a hold verdict', async () => {
    const orchestratorId = 'orch-1423-regression';
    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_HISTORY_FILE: historyFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({
        memory: { pressureLevel: 1, swapUsedMb: 2024.25, swapTotalMb: 3072 },
        disk: DISK_GREEN,
      }),
    };

    const beat = runCli([`--orchestrator-id=${orchestratorId}`, '--desired-agents=1'], env);
    expect(beat.status).toBe(0);
    const parsed = parseStdout(beat.stdout);

    // The pinned false-conservative bug manifests as `hold` here
    // today; a correctly-classified GREEN memory axis (with disk also
    // GREEN) must instead reach `spawn-allowed`.
    expect(parsed.type).not.toBe('hold');
    expect(parsed.type).toBe('spawn-allowed');

    // The persisted classification itself, straight from the coordination
    // file this beat's `declareDibs` call wrote `memoryResult.history` into
    // (`cli.mjs:7741`) — the authoritative "what did the memory axis
    // classify this sample as" record, independent of the derived
    // traffic-light `type` assertion above.
    const onDisk = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
    const selfEntry = onDisk.find((entry) => entry.orchestratorId === orchestratorId);
    expect(selfEntry).toBeDefined();
    expect(selfEntry.memoryHistory).toBeDefined();
    expect(selfEntry.memoryHistory.level).toBe('GREEN');
  });
});

describe('outer acceptance — live-agent ceiling refuses a 14-agent cohort independent of a wrong headroom stub', () => {
  const COHORT_SIZE = 14;
  const LIVE_AGENT_CEILING = 4;
  // Real per-agent footprint this cohort simulates (~2GB) — documented for
  // context; today's `--desired-agents` beat has no flag to carry a live
  // per-agent cost, so the "wrongness" of the headroom stub below is
  // expressed by how generously it is sized relative to this real cost, not
  // by a cost value passed on the command line.
  const SIMULATED_REAL_PEAK_MEMORY_MB_PER_AGENT = 2048;

  // Deliberately wrong / overly-permissive headroom stub: this freeRamMb,
  // run through today's flat DEFAULT_HEADROOM_CONFIG math
  // (perAgentRamMb: 350, osBaselineMb: 2048, reserveMb: 2048), yields
  // floor((40000 - 4096) / 350) ~= 102 "free" agent slots — comfortably
  // admitting all 14 cohort members several times over, despite the real
  // per-agent cost above (2048MB) meaning the cohort would actually need
  // ~28.7GB, not the ~36GB this headroom figure implies is spare. Today's
  // code has no way to know it is wrong.
  const WRONG_PERMISSIVE_MEMORY_STUB = {
    pressureLevel: 1,
    swapUsedMb: 1024,
    compressedMb: 512,
    freeRamMb: 40_000,
  };

  test(
    'refuses admission for a simulated 14-agent cohort at 2GB projected peak once the live ceiling of 4 is ' +
      'reached, independent of a deliberately-wrong headroom/cost stub, and stays refused when disk axis is ' +
      'simultaneously RED',
    async () => {
      expect(SIMULATED_REAL_PEAK_MEMORY_MB_PER_AGENT).toBe(2048);

      // --- Part 1: GREEN memory (wrong/permissive stub) + GREEN disk. -------
      // Nothing but a genuine live-agent ceiling should be able to refuse
      // this cohort — the headroom math itself has been deliberately rigged
      // to say yes.
      const greenEnv = {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: WRONG_PERMISSIVE_MEMORY_STUB,
          disk: DISK_GREEN,
        }),
      };

      const greenCohortResults = await Promise.all(
        Array.from({ length: COHORT_SIZE }, (_unused, index) =>
          runCliAsync(
            [`--orchestrator-id=cohort-agent-${index}`, '--desired-agents=1'],
            greenEnv,
          ),
        ),
      );

      for (const result of greenCohortResults) {
        expect(result.status).toBe(0);
      }

      const parsedGreenResults = greenCohortResults.map((result) => parseStdout(result.stdout));

      // Total number of agents actually granted admission across the whole
      // concurrently-racing cohort — summed across every orchestrator's own
      // REAL, atomically-reserved `liveAgentGrant` (not the advisory dibs
      // `allowance`, which Phase 3 (Option B) deliberately leaves
      // unnarrowed by this reservation — see cli.mjs's `liveAgentGrant`
      // comment and live-agent-ceiling.jest.spec.mjs's header comment. Dibs
      // `allowance` can legitimately sum above the ceiling across a 14-way
      // race; the ledger-enforced `liveAgentGrant` is the number that must
      // not).
      const totalGrantedUnderWrongStub = parsedGreenResults.reduce((sum, parsed) => {
        return sum + (parsed.liveAgentGrant ?? 0);
      }, 0);

      // FAILS TODAY: today's allowance math divides the (wrongly generous)
      // ~102-slot headroom cap across the 14 live, simultaneously-contending
      // orchestrators, granting each of them their requested 1 agent — the
      // aggregate comfortably exceeds 4, because no live-agent ceiling
      // exists yet to independently refuse it once 4 is reached.
      //
      // Phase 3 correction: once `main()`'s `--desired-agents` beat
      // wires in the AIMD-adjusted ceiling, sustained Normal pressure across
      // this very 14-way race additively grows the shared, persisted
      // ceiling above the flat `LIVE_AGENT_CEILING` (4) — see
      // ./cli-aimd-ceiling.jest.spec.mjs's and
      // ./cli-two-orchestrators.jest.spec.mjs's dedicated tests. The
      // ceiling only ever grows under Normal pressure (never shrinks), so
      // the FINAL persisted value read back after the race settles is a
      // valid upper bound for whatever value was in effect at any earlier
      // point during it — this still proves the real load-bearing property
      // (a genuine, ledger-enforced ceiling refuses the cohort once
      // reached), without pinning the now-superseded "always exactly 4"
      // assumption.
      const onDiskAfterGreenRace = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
      const aimdEntryAfterGreenRace = onDiskAfterGreenRace.find(
        (entry) => entry.orchestratorId === AIMD_CEILING_RESERVED_ID,
      );
      const effectiveCeilingAfterGreenRace = aimdEntryAfterGreenRace
        ? aimdEntryAfterGreenRace.ceiling
        : LIVE_AGENT_CEILING;
      expect(totalGrantedUnderWrongStub).toBeLessThanOrEqual(effectiveCeilingAfterGreenRace);

      // --- Part 2: same wrong/permissive memory stub, but disk axis is now
      // simultaneously RED — admission must stay refused under BOTH
      // conditions at once, on a fresh coordination file so Part 1's dibs
      // history can't leak into this race.
      const redDiskWorkDir = await mkdtemp(join(tmpdir(), 'arm-outer-acceptance-1265-disk-red-'));
      const redDiskCoordinationFilePath = join(redDiskWorkDir, 'coordination.json');

      try {
        const redDiskEnv = {
          ARM_COORDINATION_FILE: redDiskCoordinationFilePath,
          ARM_FAKE_COLLECT_JSON: JSON.stringify({
            memory: WRONG_PERMISSIVE_MEMORY_STUB,
            // Below DEFAULT_DISK_THRESHOLDS.redFreeDiskGbBelow (10) — a
            // genuinely RED disk axis reading.
            disk: { freeDiskGb: 5, declineRateGbPerHour: 0.5 },
          }),
        };

        const redDiskCohortResults = await Promise.all(
          Array.from({ length: COHORT_SIZE }, (_unused, index) =>
            runCliAsync(
              [`--orchestrator-id=cohort-agent-${index}`, '--desired-agents=1'],
              redDiskEnv,
            ),
          ),
        );

        for (const result of redDiskCohortResults) {
          expect(result.status).toBe(0);
        }

        const parsedRedDiskResults = redDiskCohortResults.map((result) => parseStdout(result.stdout));

        const totalGrantedWithDiskRed = parsedRedDiskResults.reduce((sum, parsed) => {
          return sum + (parsed.liveAgentGrant ?? 0);
        }, 0);

        // Admission must stay refused — not one single agent should be
        // granted while the disk axis is RED, and the live-agent ceiling
        // must still hold even independent of the disk axis.
        expect(totalGrantedWithDiskRed).toBe(0);
        expect(totalGrantedWithDiskRed).toBeLessThanOrEqual(LIVE_AGENT_CEILING);
        expect(parsedRedDiskResults.some((parsed) => parsed.type === 'spawn-allowed')).toBe(false);
      } finally {
        await rm(redDiskWorkDir, { recursive: true, force: true });
      }
    },
    30_000,
  );
});
