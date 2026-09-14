// Phase 4 — `--query-capacity` per-operation-type read.
//
// RED by construction: `--query-capacity` does not exist yet in cli.mjs.
// Only Phase 1 (lib/history.mjs), Phase 2 (--record-outcome CLI wiring), and
// Phase 3 (lib/cost-estimate.mjs's estimateOperationCost) have landed. These
// are black-box, real-process-spawning tests — mirroring the established
// idiom in ./cli.jest.spec.mjs and ./record-outcome.jest.spec.mjs — never
// importing cli.mjs directly (it stays excluded from the unit-coverage gate,
// see jest.skills.config.mjs's `!scripts/agent-resource-management/cli.mjs`).
//
// ---------------------------------------------------------------------------
// CLI contract this file tests AGAINST (for the builder to match in cli.mjs):
//
//   node cli.mjs --query-capacity=<comma-separated-operation-types>
//
//   ARM_FAKE_COLLECT_JSON   (env, required for test hermeticity)
//     Reused exactly as the existing `--desired-agents` beat's seam — a JSON
//     string shaped `{ "memory": {...}, "disk": {...} }`. `memory.freeRamMb`
//     feeds the SAME `computeHeadroomCap` (./lib/allowance.mjs) the existing
//     `--desired-agents` beat already reuses for its own memory-axis GREEN
//     cap — Phase 4 does not duplicate that formula. The per-type
//     `freeCapacity` this flag reports is `computeHeadroomCap` called once
//     PER requested type, with `perAgentRamMb` set to that type's own
//     `estimateOperationCost(...).estimatedPeakMemoryMb` (./lib/cost-
//     estimate.mjs, Phase 3) instead of the flat `DEFAULT_HEADROOM_CONFIG
//     .perAgentRamMb` — this is the "REUSE, not duplicate" instruction from
//     the Build Plan: same function, same osBaselineMb/reserveMb constants,
//     different (type-specific) per-agent divisor. `pressureLevel`/disk
//     state still gate the whole answer via the existing axis classifiers
//     exactly as `--desired-agents` does — see the AMBER/RED behaviour below.
//
//   ARM_HISTORY_FILE   (env, required for test hermeticity)
//     Reused exactly as `--record-outcome`'s existing seam (Phase 2) — the
//     history store `lib/history.mjs`'s `recordObservation`/`readHistory`
//     read/write, so per-type cost estimates come from THIS test's own
//     seeded observations, never the real production history file.
//
//   Output (stdout, exit 0): a single JSON object keyed by exactly the
//   requested operation types, in the order requested, each value shaped
//   `{ freeCapacity: <integer >= 0>, confidence: 'empirical' | 'default' }`
//   — the exact `confidence` vocabulary `estimateOperationCost` already
//   returns (./lib/cost-estimate.mjs), passed straight through.
//
//   Axis AMBER/RED (either axis, exactly as `--desired-agents`'s existing
//   GREEN-only `spawn-allowed` gate): `freeCapacity` is `0` for EVERY
//   requested type in the same response — `confidence` per type is still
//   independently correct (this flag never suppresses/clobbers the
//   confidence read just because the axis is constrained).
//
//   An operation type with zero history is NOT rejected — it is treated as
//   a fresh cold-start type (matching `estimateOperationCost`'s own
//   contract): `confidence: 'default'`, `freeCapacity` a valid non-negative
//   integer derived from the flat default cost, never an error/exit-2.
//
//   Combined with `--desired-agents` (or `--record-outcome`/`--heartbeat`)
//   in the same invocation: REJECTED outright, exit 2, clear stderr naming
//   both flags — mirroring the exact precedent `--record-outcome` already
//   set for flag-combination rejection (Phase 2, see
//   ./record-outcome.jest.spec.mjs's "rejects --record-outcome combined
//   with --desired-agents" test). `--query-capacity` is a standalone
//   advisory-read beat, structurally the same kind of thing as
//   `--record-outcome` (it doesn't declare dibs, doesn't drive the
//   collect -> classify -> dibs -> traffic-light pipeline, and doesn't need
//   `--orchestrator-id` at all) — composing it with a spawn-decision beat in
//   one invocation is rejected for the same reason `--record-outcome` +
//   `--desired-agents` is rejected: two incompatible beat types can't both
//   run inside one process's single stdout JSON payload.
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

// Generous headroom so a heavy-vs-light type comparison has real room to
// differ, and so a cold-start type's flat-default cost estimate still
// yields a comfortably positive freeCapacity under GREEN/GREEN.
const MEMORY_GREEN_WITH_HEADROOM = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const MEMORY_RED = { pressureLevel: 4, swapUsedMb: 12288, compressedMb: 4096, freeRamMb: 8192 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

/** Runs the real cli.mjs as a child process — never imports it directly. */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

let workDir;
let historyFilePath;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-query-capacity-'));
  historyFilePath = join(workDir, 'history.json');
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

/** Seeds `count` --record-outcome observations for `operationType` via the real CLI (Phase 2). */
function seedObservations(operationType, orchestratorId, peakMemoryMbValues) {
  for (const peakMemoryMb of peakMemoryMbValues) {
    const { status, stderr } = runCli(
      [
        '--record-outcome',
        `--operation-type=${operationType}`,
        `--orchestrator-id=${orchestratorId}`,
        `--peak-memory-mb=${peakMemoryMb}`,
      ],
      { ARM_HISTORY_FILE: historyFilePath },
    );
    expect(status).toBe(0);
    expect(stderr).toBe('');
  }
}

function queryCapacity(types, extraEnv = {}) {
  return runCli([`--query-capacity=${types.join(',')}`], {
    ARM_HISTORY_FILE: historyFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN_WITH_HEADROOM, disk: DISK_GREEN }),
    ...extraEnv,
  });
}

// Phase 2 (Build Plan Amendment, RC-3) REWRITE: this test originally
// asserted that a historically memory-heavy type gets a strictly LOWER
// freeCapacity than a light type — that differentiation came directly from
// feeding each type's own cost estimate into computeHeadroomCap against a
// live freeRamMb sample. RC-3 retires freeRamMb (live or the assumedFreeRamMb
// fallback) as an admission input at this site entirely, so the
// differentiation this test pinned no longer exists — every requested type
// now gets the same flat DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis (3)
// placeholder cap regardless of its own cost estimate — PERMANENTLY, not
// "until Phase 3": Phase 3 shipped a real live-agent ceiling for
// `--desired-agents`'s admission decision and `--claim=live-agent:N`'s
// ledger, but `--query-capacity` was deliberately left out of that scope
// (Build Plan judgment call #1 — a standalone, non-composable advisory read,
// not the crash-causing beat) and still reports this flat placeholder for
// every type. `confidence` per type is untouched and still independently
// correct. See the "Phase 2" describe block below for the dedicated
// new-contract tests this differentiation regression is deliberate.
test('a historically memory-heavy type and a light type both get the same flat placeholder freeCapacity under identical simulated headroom (Phase 2 rewrite)', () => {
  seedObservations('test-agent', 'orc-heavy', [3200, 3400, 3300]);
  seedObservations('implementation-agent', 'orc-light', [150, 180, 160]);

  const { status, stdout, stderr } = queryCapacity(['test-agent', 'implementation-agent']);

  expect(status).toBe(0);
  expect(stderr).toBe('');

  let parsed;
  expect(() => {
    parsed = JSON.parse(stdout);
  }).not.toThrow();

  expect(parsed['test-agent']).toMatchObject({ confidence: 'empirical' });
  expect(parsed['implementation-agent']).toMatchObject({ confidence: 'empirical' });
  expect(Number.isInteger(parsed['test-agent'].freeCapacity)).toBe(true);
  expect(Number.isInteger(parsed['implementation-agent'].freeCapacity)).toBe(true);
  expect(parsed['test-agent'].freeCapacity).toBeGreaterThanOrEqual(0);

  // Both types land on the flat placeholder cap — no longer differentiated
  // by cost estimate. This is permanent for --query-capacity, not a gap a
  // later phase fills: Phase 3's real live-agent ceiling applies only to
  // --desired-agents/--claim=live-agent:N, deliberately not this path.
  expect(parsed['test-agent'].freeCapacity).toBe(3);
  expect(parsed['implementation-agent'].freeCapacity).toBe(3);
});

// pre-PR review (High) — end-to-end proof of a cross-phase interaction
// no phase-scoped review saw: Phase 2's `--peak-memory-mb` is OPTIONAL, but
// Phase 3's `estimateOperationCost` counted peak-less observations toward its
// `>= 3 clean samples` empirical threshold, yielding
// `estimatedPeakMemoryMb: undefined` -> `computeHeadroomCap`'s non-finite
// clamp -> a permanent `freeCapacity: 0` for that type, with a confident-
// sounding `confidence: 'empirical'` label on it.
test('recording outcomes WITHOUT --peak-memory-mb never silently zeroes that type\'s freeCapacity', () => {
  for (const orchestratorId of ['orc-np-1', 'orc-np-2', 'orc-np-3']) {
    const { status, stderr } = runCli(
      ['--record-outcome', '--operation-type=peakless-agent', `--orchestrator-id=${orchestratorId}`],
      { ARM_HISTORY_FILE: historyFilePath },
    );
    expect(status).toBe(0);
    expect(stderr).toBe('');
  }

  const { status, stdout, stderr } = queryCapacity(['peakless-agent']);

  expect(status).toBe(0);
  expect(stderr).toBe('');

  const parsed = JSON.parse(stdout);
  // No peak was ever recorded, so there is genuinely no empirical evidence —
  // the honest answer is the cold-start default, not a confident zero.
  expect(parsed['peakless-agent'].confidence).toBe('default');
  expect(parsed['peakless-agent'].freeCapacity).toBeGreaterThan(0);
});

test('a cold-start type queried alongside a warm type gets its own independent confidence', () => {
  seedObservations('test-agent', 'orc-warm', [900, 950, 1000]);
  // lint-agent is never recorded — genuinely zero history.

  const { status, stdout, stderr } = queryCapacity(['test-agent', 'lint-agent']);

  expect(status).toBe(0);
  expect(stderr).toBe('');

  const parsed = JSON.parse(stdout);
  expect(parsed['test-agent'].confidence).toBe('empirical');
  expect(parsed['lint-agent'].confidence).toBe('default');
  expect(Number.isInteger(parsed['lint-agent'].freeCapacity)).toBe(true);
  expect(parsed['lint-agent'].freeCapacity).toBeGreaterThanOrEqual(0);
});

test('a RED memory axis zeroes freeCapacity for every requested type, not just one', () => {
  seedObservations('test-agent', 'orc-warm', [900, 950, 1000]);
  // implementation-agent stays cold-start.

  const { status, stdout, stderr } = queryCapacity(['test-agent', 'implementation-agent'], {
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
  });

  expect(status).toBe(0);
  expect(stderr).toBe('');

  const parsed = JSON.parse(stdout);
  expect(parsed['test-agent'].freeCapacity).toBe(0);
  expect(parsed['implementation-agent'].freeCapacity).toBe(0);

  // Confidence is still independently correct even though capacity is zeroed
  // — a RED axis must not clobber/mask the per-type confidence read.
  expect(parsed['test-agent'].confidence).toBe('empirical');
  expect(parsed['implementation-agent'].confidence).toBe('default');
});

test('--query-capacity combined with --desired-agents in the same invocation is rejected, exit 2', () => {
  const { status, stdout, stderr } = runCli(
    ['--query-capacity=test-agent', '--desired-agents=1', '--orchestrator-id=orc-combo'],
    {
      ARM_HISTORY_FILE: historyFilePath,
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN_WITH_HEADROOM, disk: DISK_GREEN }),
    },
  );

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--query-capacity/);
  expect(stderr).toMatch(/--desired-agents/);
});

test('--query-capacity combined with --record-outcome in the same invocation is rejected, exit 2', () => {
  const { status, stdout, stderr } = runCli(
    [
      '--query-capacity=test-agent',
      '--record-outcome',
      '--operation-type=test-agent',
      '--orchestrator-id=orc-combo-2',
    ],
    { ARM_HISTORY_FILE: historyFilePath, ARM_COORDINATION_FILE: coordinationFilePath },
  );

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--query-capacity/);
  expect(stderr).toMatch(/--record-outcome/);
});

test('an unknown/never-seen operation type is treated as cold-start, never rejected', () => {
  const { status, stdout, stderr } = queryCapacity(['totally-made-up-type']);

  expect(status).toBe(0);
  expect(stderr).toBe('');

  const parsed = JSON.parse(stdout);
  expect(parsed['totally-made-up-type']).toBeDefined();
  expect(parsed['totally-made-up-type'].confidence).toBe('default');
  expect(Number.isInteger(parsed['totally-made-up-type'].freeCapacity)).toBe(true);
  expect(parsed['totally-made-up-type'].freeCapacity).toBeGreaterThanOrEqual(0);
});

// ---------------------------------------------------------------------------
// Phase 2 (Build Plan Amendment, RC-3) — REWRITE of the two tests this
// block replaces.
//
// `freeRamMb`/`ps`-derived RSS inverts under macOS memory-compressor pressure
// (the Investigation's RC-3 finding), so it — and the `assumedFreeRamMb`
// fallback baseline that used to be derived FROM the flat-350 arithmetic
// (`osBaselineMb + reserveMb + maxAgentsMemoryAxis * perAgentRamMb`) — must
// no longer feed `computeHeadroomCap` at this site AT ALL, whether `freeRamMb`
// is live, missing, or non-finite. `--query-capacity` falls back to the flat
// `DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis` (3) as a placeholder cap,
// applied identically to every requested type (per-type in RESPONSE SHAPE —
// one `freeCapacity` entry per type, never collapsed into one flat top-level
// number — but not differentiated by that type's own cost estimate).
// Phase 3 shipped since this comment was first written and did NOT restore
// this differentiation for `--query-capacity` — its real live-agent ceiling
// applies only to `--desired-agents`/`--claim=live-agent:N` (Build Plan
// judgment call #1). This is a real, acknowledged regression in per-type
// differentiation, accepted as the amendment's explicit PERMANENT contract
// for this path — NOT an oversight, and NOT a placeholder awaiting a later
// phase.
// ---------------------------------------------------------------------------

test('a missing freeRamMb sample no longer derives an assumed baseline from flat-350 arithmetic — every type gets the flat maxAgentsMemoryAxis placeholder cap', () => {
  seedObservations('test-agent', 'orc-heavy', [3200, 3400, 3300]);
  seedObservations('implementation-agent', 'orc-light', [150, 180, 160]);

  const { status, stdout, stderr } = queryCapacity(['test-agent', 'implementation-agent'], {
    // Deliberately omit freeRamMb — the exact fixture shape ARM_FAKE_COLLECT_JSON
    // memory readings above never exercise (they all set freeRamMb: 8192).
    ARM_FAKE_COLLECT_JSON: JSON.stringify({
      memory: { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 },
      disk: DISK_GREEN,
    }),
  });

  expect(status).toBe(0);
  expect(stderr).toBe('');

  const parsed = JSON.parse(stdout);
  expect(parsed['test-agent']).toMatchObject({ confidence: 'empirical' });
  expect(parsed['implementation-agent']).toMatchObject({ confidence: 'empirical' });
  expect(Number.isInteger(parsed['test-agent'].freeCapacity)).toBe(true);
  expect(Number.isInteger(parsed['implementation-agent'].freeCapacity)).toBe(true);

  // Both types land on the SAME flat placeholder cap — the heavy-vs-light
  // differentiation the old assumedFreeRamMb formula produced is gone by
  // design (per the amendment). Permanent for --query-capacity, not
  // restored by Phase 3 — see this describe block's header comment.
  expect(parsed['test-agent'].freeCapacity).toBe(3);
  expect(parsed['implementation-agent'].freeCapacity).toBe(3);
});

test('a non-finite (NaN) freeRamMb sample falls back the same way as a missing one — the flat maxAgentsMemoryAxis placeholder cap, not a NaN-derived formula', () => {
  seedObservations('test-agent', 'orc-heavy-2', [3200, 3400, 3300]);
  seedObservations('implementation-agent', 'orc-light-2', [150, 180, 160]);

  const { status, stdout, stderr } = queryCapacity(['test-agent', 'implementation-agent'], {
    ARM_FAKE_COLLECT_JSON: JSON.stringify({
      memory: { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: NaN },
      disk: DISK_GREEN,
    }),
  });

  expect(status).toBe(0);
  expect(stderr).toBe('');

  const parsed = JSON.parse(stdout);
  expect(parsed['test-agent'].freeCapacity).toBe(3);
  expect(parsed['implementation-agent'].freeCapacity).toBe(3);
});

test('`--query-capacity` freeCapacity does not vary with freeRamMb at fixed pressureLevel/type cost', () => {
  seedObservations('test-agent', 'orc-invariant', [900, 950, 1000]);

  const lowFreeRamMb = queryCapacity(['test-agent'], {
    ARM_FAKE_COLLECT_JSON: JSON.stringify({
      memory: { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 4500 },
      disk: DISK_GREEN,
    }),
  });
  const highFreeRamMb = queryCapacity(['test-agent'], {
    ARM_FAKE_COLLECT_JSON: JSON.stringify({
      memory: { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 500_000 },
      disk: DISK_GREEN,
    }),
  });

  expect(lowFreeRamMb.status).toBe(0);
  expect(highFreeRamMb.status).toBe(0);
  expect(lowFreeRamMb.stderr).toBe('');
  expect(highFreeRamMb.stderr).toBe('');

  const parsedLow = JSON.parse(lowFreeRamMb.stdout);
  const parsedHigh = JSON.parse(highFreeRamMb.stdout);
  expect(parsedLow['test-agent'].freeCapacity).toBe(parsedHigh['test-agent'].freeCapacity);
  expect(parsedLow['test-agent'].freeCapacity).toBe(3);
});

// ---------------------------------------------------------------------------
// Phase 5 review (second pass), Medium #2/#3: the per-type try/catch
// around `readHistory` (cli.mjs) had no regression test proving a single
// type's corrupted/unreadable history entry doesn't crash the whole beat,
// and that the catch is narrowly scoped to the I/O read (not the pure
// `estimateOperationCost` call). A structurally-corrupted per-type entry
// (a `summary.reservoir` that is `null` instead of an array) makes
// `readHistory`'s own internal `toPublicSummary`/`computePercentile` throw
// (`[...null]`) while the JSON document as a whole still parses fine — this
// exercises exactly the "one bad type, other types resolve correctly" path
// without needing an unparseable file (which would fail every type
// uniformly, and would prevent seeding a healthy sibling type at all).
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Phase 5 review (third pass), Medium #1: a missing or empty
// `--query-capacity` value previously silently succeeded with `{}` (exit 0,
// no stderr) instead of validating — unlike every sibling required parameter
// in this diff. `--query-capacity`'s value IS the entire payload of this beat
// type, so a missing/malformed value must be a hard validation error: exit 2,
// clear stderr, nothing on stdout.
// ---------------------------------------------------------------------------

test('bare --query-capacity (no value) is rejected, exit 2, empty stdout', () => {
  const { status, stdout, stderr } = runCli(['--query-capacity'], {
    ARM_HISTORY_FILE: historyFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN_WITH_HEADROOM, disk: DISK_GREEN }),
  });

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--query-capacity/);
});

test('--query-capacity= (empty value) is rejected, exit 2, empty stdout', () => {
  const { status, stdout, stderr } = queryCapacity([]);

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--query-capacity/);
});

test('--query-capacity=,, (only commas, no real types) is rejected, exit 2, empty stdout', () => {
  const { status, stdout, stderr } = runCli(['--query-capacity=,,'], {
    ARM_HISTORY_FILE: historyFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN_WITH_HEADROOM, disk: DISK_GREEN }),
  });

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--query-capacity/);
});

test('a single corrupted-history type degrades to a cold-start default without crashing the beat or affecting a healthy sibling type', async () => {
  // Seed a genuinely healthy type through the real CLI first.
  seedObservations('healthy-agent', 'orc-healthy', [900, 950, 1000]);

  // Directly corrupt ONLY the "broken-agent" entry in the shared history
  // file — the JSON document as a whole remains syntactically valid (so
  // seeding above and this corruption can coexist in one file), but
  // `readHistory('broken-agent')`'s internal summary-formatting throws.
  const store = JSON.parse(await readFile(historyFilePath, 'utf8'));
  store['broken-agent'] = {
    raw: [],
    summary: { count: 1, sum: 100, max: 100, reservoir: null },
  };
  await writeFile(historyFilePath, JSON.stringify(store), 'utf8');

  const { status, stdout, stderr } = queryCapacity(['healthy-agent', 'broken-agent']);

  expect(status).toBe(0);
  expect(stderr).toMatch(/broken-agent/);
  expect(stderr).not.toMatch(/healthy-agent/);

  const parsed = JSON.parse(stdout);

  // The healthy type is completely unaffected by its sibling's corruption.
  expect(parsed['healthy-agent'].confidence).toBe('empirical');
  expect(Number.isInteger(parsed['healthy-agent'].freeCapacity)).toBe(true);
  expect(parsed['healthy-agent'].freeCapacity).toBeGreaterThan(0);

  // The broken type degrades gracefully to a cold-start default instead of
  // crashing the whole beat.
  expect(parsed['broken-agent'].confidence).toBe('default');
  expect(Number.isInteger(parsed['broken-agent'].freeCapacity)).toBe(true);
  expect(parsed['broken-agent'].freeCapacity).toBeGreaterThanOrEqual(0);
});
