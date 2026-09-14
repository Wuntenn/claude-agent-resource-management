// Phase 1 — RED tests for partial memory-projection admission via a
// decrement search inside `decideMemoryProjection` (cli.mjs).
//
// Today (pre-fix), `decideMemoryProjection` prices exactly `candidateAgentCount`
// copies of `agentClass` in ONE call to `computeProjectedPeakMemoryMb` and
// returns a single `withinBudget: boolean` — there is no search over smaller
// counts, so a `--desired-agents=N` beat whose full N-candidate projection
// exceeds budget is refused ENTIRELY via `memory-projection-block`, even when
// a smaller count would comfortably fit. This file is intentionally
// black-box (spawns the real `cli.mjs`, never imports it — matches
// ./cli-memory-projection.jest.spec.mjs and
// ./cli-memory-projection-wiring.jest.spec.mjs's existing idiom in this
// directory) and stays red until Phase 1 lands.
//
// ---------------------------------------------------------------------------
// Expected shape this file bakes in, for the implementer to match (per the
// Build Plan comment on):
//
//   - `decideMemoryProjection` searches DOWNWARD from `candidateAgentCount`
//     to `1`, stopping at the largest count whose projection is
//     `withinBudget`. It returns that admitted count alongside
//     `withinBudget: true` (partial or full), or `withinBudget: false` with
//     admitted count `0` when nothing fits (unchanged from today).
//   - The admitted count is surfaced on the beat's JSON output as
//     `memoryProjectionAdmittedCount` — present ONLY when it differs from
//     the originally-requested `candidateAgentCount` (a fully-fitting beat's
//     output must otherwise be byte-for-byte unchanged from today).
//   - `spawnBucketRequestAmount`/the live-agent ceiling claim compose
//     `Math.min(desiredAgents, perOrchestratorAllowance, admittedCount)` —
//     all three clamp sources, not just two.
//   - A beat where even 1 candidate doesn't fit still refuses via
//     `memory-projection-block`, with `totalProjectedMemoryMb`/
//     `budgetMemoryMb` populated from the count=1 attempt's real numbers,
//     never `null`/`undefined` from an aborted search.
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { writeAimdCeilingState } from './lib/coordination-file.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

// Mirrors DEFAULT_PROJECTION_CONFIG in ./lib/memory-projection.mjs:
// totalHostMemoryMb (16 GiB) - memoryBudgetMarginMb (2 GiB).
const EXPECTED_BUDGET_MEMORY_MB = 16 * 1024 - 2048;

/** Runs the real cli.mjs as a child process — never imports it directly. */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

function parseStdout(stdout) {
  return JSON.parse(stdout);
}

/** Seeds `count` --record-outcome observations for `operationType` (== agentClass) via the real CLI. */
function seedHistory(historyFilePath, agentClass, peakMemoryMbValues) {
  peakMemoryMbValues.forEach((peakMemoryMb, index) => {
    const { status, stderr } = runCli(
      [
        '--record-outcome',
        `--operation-type=${agentClass}`,
        `--orchestrator-id=orch-seed-${agentClass}-${index}`,
        `--peak-memory-mb=${peakMemoryMb}`,
      ],
      { ARM_HISTORY_FILE: historyFilePath },
    );
    expect(status).toBe(0);
    expect(stderr).toBe('');
  });
}

let workDir;
let coordinationFilePath;
let historyFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-memory-projection-partial-'));
  coordinationFilePath = join(workDir, 'coordination.json');
  historyFilePath = join(workDir, 'history.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Test 1 — a beat whose full requested count exceeds budget but whose
// smaller count fits is admitted at that smaller (genuinely feasible) count,
// not refused entirely, and not silently truncated to some OTHER count.
// ---------------------------------------------------------------------------

test('1. a --desired-agents=4 beat whose 4-candidate projection exceeds budget but whose 2-candidate projection fits is admitted at exactly 2', async () => {
  const AGENT_CLASS = 'partial-admission-class';
  // Per-instance peak ~4915 MB (median of last two): 2 instances (~9830 MB)
  // comfortably fit the ~14336 MB budget; 3 instances (~14745 MB) exceed it;
  // 4 instances (~19660 MB) exceed it further. A correct decrement search
  // must land on exactly 2 — a naive "stop at the first count below N that
  // ever fits" or an off-by-one search could instead land on 3 or 1.
  seedHistory(historyFilePath, AGENT_CLASS, [4800, 4915, 4915, 4915]);

  // Generous ceiling so perOrchestratorAllowance (solo orchestrator) is 4,
  // matching desiredAgents exactly — isolates the memory-projection search
  // as the only thing that can clamp this beat below 4.
  await writeAimdCeilingState(coordinationFilePath, { ceiling: 4, sustainedNormalCount: 0 }, { now: Date.now() });

  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-partial-admission', '--desired-agents=4', `--agent-class=${AGENT_CLASS}`],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_HISTORY_FILE: historyFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');
  const parsed = parseStdout(stdout);

  // FAILS TODAY: no decrement search exists — decideMemoryProjection prices
  // exactly 4 candidates in one shot, that sum exceeds budget, and the WHOLE
  // beat is refused via 'memory-projection-block' with liveAgentGrant: null,
  // even though 2 candidates would fit comfortably.
  expect(parsed.type).toBe('spawn-allowed');
  expect(parsed.type).not.toBe('memory-projection-block');

  // The admitted count is genuinely the smaller FEASIBLE one (2), not merely
  // a truthy withinBudget — pins against a bug that reuses a stale
  // projectedRunningClasses across iterations and always "succeeds" at the
  // full requested count, or one that stops searching at the wrong count.
  expect(parsed.memoryProjectionAdmittedCount).toBe(2);
  expect(Number(parsed.liveAgentGrant)).toBe(2);
  expect(Number(parsed.liveAgentGrant)).not.toBe(4);
});

// ---------------------------------------------------------------------------
// Test 2 — even 1 candidate doesn't fit: still refuses via
// 'memory-projection-block', with totalProjectedMemoryMb/budgetMemoryMb
// populated from the count=1 attempt's real numbers, not null/undefined from
// an aborted search.
// ---------------------------------------------------------------------------

test('2. a beat where even 1 candidate does not fit still refuses via memory-projection-block, with totals from the count=1 attempt', async () => {
  const AGENT_CLASS = 'zero-feasible-class';
  // A single instance's own p90 peak (15000 MB) already exceeds the ~14336
  // MB budget alone — no count, however small, can ever fit.
  seedHistory(historyFilePath, AGENT_CLASS, [15_000, 15_000, 15_000, 15_000]);

  await writeAimdCeilingState(coordinationFilePath, { ceiling: 3, sustainedNormalCount: 0 }, { now: Date.now() });

  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-zero-feasible', '--desired-agents=3', `--agent-class=${AGENT_CLASS}`],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_HISTORY_FILE: historyFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');
  const parsed = parseStdout(stdout);

  expect(parsed.type).toBe('memory-projection-block');
  expect(parsed.liveAgentGrant).toBeNull();

  // Not null/undefined — and specifically the count=1 attempt's real
  // numbers, not the never-searched, always-3-candidates total (~45000 MB)
  // today's un-searched implementation would report instead.
  expect(parsed.totalProjectedMemoryMb).not.toBeNull();
  expect(parsed.totalProjectedMemoryMb).not.toBeUndefined();
  expect(parsed.budgetMemoryMb).not.toBeNull();
  expect(parsed.budgetMemoryMb).not.toBeUndefined();

  // FAILS TODAY: today's un-searched decideMemoryProjection always prices
  // the full requested count (3) in one shot, so totalProjectedMemoryMb here
  // is ~45000 MB (3 * 15000), not the single-candidate 15000 MB the search's
  // last (smallest, count=1) attempt must report.
  expect(parsed.totalProjectedMemoryMb).toBe(15_000);
  expect(parsed.budgetMemoryMb).toBe(EXPECTED_BUDGET_MEMORY_MB);
});

// ---------------------------------------------------------------------------
// Test 3 — a fully-fitting beat's output is byte-for-byte identical to
// today: the new field must be ABSENT (not just falsy/undefined-when-
// serialized) when the full requested count is admitted.
// ---------------------------------------------------------------------------

test('3. a fully-fitting beat omits memoryProjectionAdmittedCount entirely (present only when admission is genuinely partial)', () => {
  const AGENT_CLASS = 'fully-fitting-class';
  seedHistory(historyFilePath, AGENT_CLASS, [256, 300, 320, 340]);

  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-fully-fitting', '--desired-agents=2', `--agent-class=${AGENT_CLASS}`],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_HISTORY_FILE: historyFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');
  const parsed = parseStdout(stdout);

  expect(parsed.type).toBe('spawn-allowed');
  expect(Number(parsed.liveAgentGrant)).toBe(2);

  // Not merely `toBeUndefined()` (which a `memoryProjectionAdmittedCount:
  // undefined` key — still present, still changing the JSON shape for a
  // strict-key-comparison consumer once JSON.stringify drops it — could
  // accidentally satisfy in some serialization paths). Assert the KEY
  // itself is absent.
  expect(Object.prototype.hasOwnProperty.call(parsed, 'memoryProjectionAdmittedCount')).toBe(false);
});

// ---------------------------------------------------------------------------
// Test 4 — the downstream clamp composes Math.min(desiredAgents,
// perOrchestratorAllowance, admittedCount) — all three, using distinct
// values for each, so a clamp that only checked two of the three would fail
// to catch the bug this test exists to pin.
// ---------------------------------------------------------------------------

test('4. spawnBucketRequestAmount/the live-agent ceiling claim clamp to Math.min(desiredAgents, perOrchestratorAllowance, admittedCount) — all three, not just two', async () => {
  const AGENT_CLASS = 'triple-clamp-class';
  // Per-instance peak ~4000 MB: 3 instances (~12000 MB) fit the ~14336 MB
  // budget; 4 and 5 do not (~16000/~20000 MB).
  seedHistory(historyFilePath, AGENT_CLASS, [4000, 4000, 4000, 4000]);

  // desiredAgents (8) > perOrchestratorAllowance (5, via the seeded AIMD
  // ceiling, solo orchestrator) > admittedCount (3, the genuinely feasible
  // memory-projection count) — three DISTINCT values. An implementation that
  // omitted the admittedCount clamp (Math.min(desiredAgents,
  // perOrchestratorAllowance) = 5) or the perOrchestratorAllowance clamp
  // (Math.min(desiredAgents, admittedCount) would coincidentally also give
  // 3 here, but the 5-omission case is caught) would report the wrong
  // liveAgentGrant.
  await writeAimdCeilingState(coordinationFilePath, { ceiling: 5, sustainedNormalCount: 0 }, { now: Date.now() });

  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-triple-clamp', '--desired-agents=8', `--agent-class=${AGENT_CLASS}`],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_HISTORY_FILE: historyFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');
  const parsed = parseStdout(stdout);

  // FAILS TODAY: candidateAgentCount = min(8, 5) = 5, priced in one shot
  // (~20000 MB), exceeds budget, so the WHOLE beat refuses via
  // 'memory-projection-block' — liveAgentGrant is never a clamped 3.
  expect(parsed.type).toBe('spawn-allowed');
  expect(parsed.memoryProjectionAdmittedCount).toBe(3);
  expect(Number(parsed.liveAgentGrant)).toBe(3);
  expect(Number(parsed.liveAgentGrant)).not.toBe(5);
  expect(Number(parsed.liveAgentGrant)).not.toBe(8);
});

// ---------------------------------------------------------------------------
// Test 5 — the decrement search adds no extra history-file I/O beyond the
// existing per-class dedup: at most one additional history-file read per
// beat, even when the search re-prices several distinct candidate counts.
// Extends ./cli-memory-projection-wiring.jest.spec.mjs test 6's exact
// ARM_HISTORY_READ_COUNT_FILE mechanism and budget.
// ---------------------------------------------------------------------------

test('5. the decrement search across several candidate counts still reads history at most once per beat', async () => {
  const CANDIDATE_CLASS = 'search-read-budget-candidate';
  const RUNNING_CLASS_A = 'search-read-budget-running-a';
  const RUNNING_CLASS_B = 'search-read-budget-running-b';

  [CANDIDATE_CLASS, RUNNING_CLASS_A, RUNNING_CLASS_B].forEach((agentClass) => {
    seedHistory(historyFilePath, agentClass, [4000, 4000, 4000, 4000]);
  });

  const baseEnv = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_HISTORY_FILE: historyFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
  };

  [RUNNING_CLASS_A, RUNNING_CLASS_B].forEach((runningClass, index) => {
    const seedRunning = runCli(
      [`--orchestrator-id=orch-search-running-${index}`, '--desired-agents=1', `--agent-class=${runningClass}`],
      baseEnv,
    );
    expect(seedRunning.status).toBe(0);
  });

  await writeAimdCeilingState(coordinationFilePath, { ceiling: 5, sustainedNormalCount: 0 }, { now: Date.now() });

  const historyReadCountFile = join(workDir, 'search-history-read-count.log');
  await writeFile(historyReadCountFile, '');

  // desiredAgents=5, but the two running classes (8000 MB already) plus
  // candidates leaves room for a search across several counts before
  // landing on a feasible one — this is the shape that forces the decrement
  // loop to run more than once internally.
  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-search-read-budget', '--desired-agents=5', `--agent-class=${CANDIDATE_CLASS}`],
    { ...baseEnv, ARM_HISTORY_READ_COUNT_FILE: historyReadCountFile },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');
  parseStdout(stdout); // still valid JSON — sanity check, not the point of this test.

  const readCountLog = await readFile(historyReadCountFile, 'utf8');
  const readCount = readCountLog.split('\n').filter((line) => line.trim().length > 0).length;

  // FAILS TODAY only in the sense that this pins a NEW contract (a
  // multi-iteration search) against the SAME budget test 6 in
  // ./cli-memory-projection-wiring.jest.spec.mjs already proves for a single,
  // un-searched pricing call — a regression here would mean the decrement
  // search re-reads history once per candidate count considered, instead of
  // reusing the one historyByClass map built before the search begins.
  expect(readCount).toBeGreaterThanOrEqual(1);
  expect(readCount).toBeLessThanOrEqual(1);
});
