// Phase 5 — Load average: record, do not gate.
//
// RED BY CONSTRUCTION: no production code reads/reports load average today.
// `main()` never calls `os.loadavg()` anywhere, and no `loadAverage` field
// exists on the printed traffic-light JSON.
//
// ---------------------------------------------------------------------------
// DESIGN DECISION (documented per this ticket's own pattern of recording
// judgment calls where the Build Plan leaves an implementation detail open):
//
//   The issue is explicit that load-42 was a symptom of compressor thrash,
//   not a cause — agents are network-blocked for much of their life at ~0%
//   CPU while holding ~1 GB, so load average must never become a CPU
//   admission axis (see the outer Build Plan's Phase 5 objective). This
//   phase is purely observability: record the real value, surface it, do
//   NOT let it influence any verdict.
//
//   "Surfaced in logs" is satisfied here as a new top-level sibling field,
//   `loadAverage: { oneMinute, fiveMinute, fifteenMinute }`, on the printed
//   traffic-light JSON — the SAME pattern `diskTrend`/`liveAgentGrant`
//   already use (a fact `cli.mjs` attaches outside whatever verdict-shaped
//   literal it computed, present on every spawn-decision beat regardless of
//   type). This was chosen over an unconditional stderr line specifically
//   because ~30+ existing tests across this directory assert
//   `stderr === ''` on a successful beat — adding an unconditional stderr
//   write would be a much larger, unrelated blast radius for a phase whose
//   own objective is "record, do not gate" (i.e. minimal-footprint
//   observability), not a new logging subsystem. The printed JSON on stdout
//   IS this CLI's log — every existing sibling field (`diskTrend`,
//   `liveAgentGrant`) already treats it that way, and an orchestrator
//   piping/recording that JSON for diagnosis, per this file's test below,
//   gets the load-average reading without any new plumbing.
//
//   Sourced from Node's built-in `os.loadavg()` — no new `sysctl`/shell-out
//   is added (the issue's "no more than one extra sysctl call per beat"
//   budget was already spent by Phase 1's `swapTotalMb` plumbing), and
//   `os.loadavg()` is portable to non-macOS CI runners unlike this file's
//   `vm_stat`/`sysctl`/`df` probes. `ARM_FAKE_LOAD_AVERAGE_JSON` (a JSON
//   3-array `[oneMinute, fiveMinute, fifteenMinute]`) is the new test seam,
//   mirroring this directory's existing `ARM_FAKE_*` pattern — never set in
//   a real orchestrator invocation.
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

let workDir;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-load-average-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

test('load average is recorded in the printed traffic-light JSON as a { oneMinute, fiveMinute, fifteenMinute } sibling field', () => {
  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-a', '--desired-agents=1'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      ARM_FAKE_LOAD_AVERAGE_JSON: JSON.stringify([2.5, 1.75, 1.1]),
    },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');
  const parsed = JSON.parse(stdout);
  expect(parsed.type).toBe('spawn-allowed');
  expect(parsed.loadAverage).toEqual({ oneMinute: 2.5, fiveMinute: 1.75, fifteenMinute: 1.1 });
});

test('load average also appears on a non-spawn-allowed (hold) beat — recorded regardless of verdict', () => {
  const MEMORY_AMBER_VIA_SWAP = { pressureLevel: 1, swapUsedMb: 6144, compressedMb: 2048 };
  const { stdout } = runCli(
    ['--orchestrator-id=orch-a', '--desired-agents=1'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_AMBER_VIA_SWAP, disk: DISK_GREEN }),
      ARM_FAKE_LOAD_AVERAGE_JSON: JSON.stringify([9.9, 8.8, 7.7]),
    },
  );

  const parsed = JSON.parse(stdout);
  expect(parsed.type).toBe('hold');
  expect(parsed.loadAverage).toEqual({ oneMinute: 9.9, fiveMinute: 8.8, fifteenMinute: 7.7 });
});

// ---------------------------------------------------------------------------
// The negative-assertion counterpart to Phases 3-4's positive gates: no
// admission verdict may vary with load average, across its full plausible
// range — near-zero to well above core count — at a FIXED pressure/ceiling
// state. Each run uses its OWN coordination file so dibs contention between
// runs can never confound the comparison.
// ---------------------------------------------------------------------------

test('admission verdict is identical across loadavg values [0, 1, core-count, 10x core-count, 42] at fixed pressure/ceiling state', () => {
  const CORE_COUNT_STAND_IN = 8; // illustrative — this test only cares that the RANGE spans near-zero to well-above-core-count.
  const loadAverageScenarios = [0, 1, CORE_COUNT_STAND_IN, CORE_COUNT_STAND_IN * 10, 42];

  const results = loadAverageScenarios.map((load, index) => {
    const perScenarioCoordinationFile = join(workDir, `coordination-${index}.json`);
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-a', '--desired-agents=1'],
      {
        ARM_COORDINATION_FILE: perScenarioCoordinationFile,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_LOAD_AVERAGE_JSON: JSON.stringify([load, load, load]),
      },
    );
    expect(status).toBe(0);
    expect(stderr).toBe('');
    return JSON.parse(stdout);
  });

  const [first, ...rest] = results;
  for (const result of rest) {
    expect(result.type).toBe(first.type);
    expect(result.allowance).toBe(first.allowance);
    expect(result.liveAgentGrant).toBe(first.liveAgentGrant);
  }

  // Sanity check the seam actually varied what it claims to (otherwise this
  // test would trivially pass even if loadAverage were plumbed nowhere at
  // all): the RECORDED value must have actually followed each scenario's
  // input, even though the VERDICT did not.
  results.forEach((result, index) => {
    expect(result.loadAverage.oneMinute).toBe(loadAverageScenarios[index]);
  });
});

test('with no ARM_FAKE_LOAD_AVERAGE_JSON set, falls back to the real os.loadavg() reading (finite, non-negative numbers, never a crash)', () => {
  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-a', '--desired-agents=1'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');
  const parsed = JSON.parse(stdout);
  expect(Number.isFinite(parsed.loadAverage.oneMinute)).toBe(true);
  expect(Number.isFinite(parsed.loadAverage.fiveMinute)).toBe(true);
  expect(Number.isFinite(parsed.loadAverage.fifteenMinute)).toBe(true);
  expect(parsed.loadAverage.oneMinute).toBeGreaterThanOrEqual(0);
});

test('a --heartbeat beat also carries loadAverage — this is not a real-beat-only field', () => {
  const { stdout } = runCli(
    ['--orchestrator-id=orch-a', '--heartbeat'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      ARM_FAKE_LOAD_AVERAGE_JSON: JSON.stringify([0.5, 0.4, 0.3]),
    },
  );

  const parsed = JSON.parse(stdout);
  expect(parsed.loadAverage).toEqual({ oneMinute: 0.5, fiveMinute: 0.4, fifteenMinute: 0.3 });
});

test('a pressure-blocked beat also carries loadAverage', () => {
  const MEMORY_WARN = { pressureLevel: 2, swapUsedMb: 100, compressedMb: 100, freeRamMb: 40_000 };
  const { stdout } = runCli(
    ['--orchestrator-id=orch-a', '--desired-agents=1'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_WARN, disk: DISK_GREEN }),
      ARM_FAKE_LOAD_AVERAGE_JSON: JSON.stringify([3, 2, 1]),
    },
  );

  const parsed = JSON.parse(stdout);
  expect(parsed.type).toBe('pressure-block');
  expect(parsed.loadAverage).toEqual({ oneMinute: 3, fiveMinute: 2, fifteenMinute: 1 });
});
