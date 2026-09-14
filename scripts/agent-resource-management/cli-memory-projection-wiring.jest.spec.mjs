// Phase 2 — RED unit/integration tests for wiring Phase 1's
// `computeProjectedPeakMemoryMb` (./lib/memory-projection.mjs) into `main()`'s
// `--desired-agents` beat as a new, independently-atomic refusal gate.
//
// More granular than Phase 0's outer black-box test (./cli-memory-projection.jest.spec.mjs),
// which only pins two end-to-end scenarios (sequential refusal, concurrent
// race safety). This file targets the SPECIFIC wiring behaviours Phase 2 must
// deliver: the gate must be reachable via source (test 1), must independently
// refuse/admit (test 2), must NOT replace or weaken the two pre-existing
// gates — the pressure-override backstop (test 3) and the AIMD ceiling
// (test 4) — refusal reasons across all three gates must stay distinguishable
// (test 5), the new gate must add at most one history-file read per beat
// (test 6), and its lock must not nest inside `reserveAdmission`'s (test 7).
//
// ---------------------------------------------------------------------------
// Design assumptions this file bakes in, additional to Phase 0's own
// (./cli-memory-projection.jest.spec.mjs's header) — Phase 2's implementer
// must either match these or come back and adjust this file:
//
// 1. `main()`'s body is bounded by the literal `async function main() {` up
//    to (but not including) the top-level `main().catch(` call that follows
//    it in this file today — see the `extractMainBody` helper below. If a
//    future refactor moves `main()` or renames its trailing call, this
//    helper (and ONLY this helper) needs updating, not the intent of test 1.
//
// 2. Test 6's read-count budget requires an instrumentation hook that does
//    NOT exist in `lib/history.mjs` yet: `ARM_HISTORY_READ_COUNT_FILE`. When
//    set, `readStore()` (the function underlying `readHistory`) must
//    `fs.appendFileSync(process.env.ARM_HISTORY_READ_COUNT_FILE, '1\n')`
//    exactly once per genuine on-disk read of the history file it's given
//    (i.e. once per `fs.readFile` call inside `readStore`, mirroring the
//    ENOENT-tolerant contract `readStore` already documents — a read that
//    hits ENOENT still counts, since the file *system call* was made).
//    Phase 2's implementer must add this hook (or an equivalent one; if a
//    different mechanism is chosen, this test needs updating) — it is the
//    simplest way to observe file-read counts from a black-box child-process
//    test without adding a network of counting spies across a process
//    boundary. See the test's own comment for why this hook, specifically,
//    was chosen over alternatives (spying on an ESM import is not reliably
//    interceptable across a spawned child process; counting real syscalls
//    via strace/dtrace is unavailable/unportable in CI).
//
// 3. The gate's own lock is assumed to be a SEPARATE `withLock` critical
//    section over the SAME coordination file, sequenced so it runs and
//    fully releases before `reserveAdmission`'s own `withLock` call begins
//    (mirroring how the pressure-override pre-check and the AIMD ceiling
//    advance/read both already run to completion, sequentially, before
//    `reserveAdmission` is ever reached in today's `main()`). Test 7 only
//    proves this sequencing doesn't hang under real (non-concurrent)
//    execution — genuine concurrent race coverage between this gate's lock
//    and `reserveAdmission`'s lock is explicitly Phase 3's territory (per
//    the Build Plan), not this file's.
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { writeAimdCeilingState } from './lib/coordination-file.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const CLI = join(__dirname, 'cli.mjs');

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };
// Mirrors ./pressure-override.jest.spec.mjs's MEMORY_WARN — pressureLevel=2
// alone, with otherwise-healthy swap/compressed/freeRamMb numbers, so ONLY
// the pressure pre-check is exercised, independent of the AMBER/RED swap
// thresholds and independent of this ticket's own memory-projection gate.
const MEMORY_WARN = { pressureLevel: 2, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

const LIVE_AGENT_CLAIM_TYPE = 'live-agent';

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

/** Seeds `count` --record-outcome observations for `operationType` (== agentClass, Phase 0 assumption #2) via the real CLI. */
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
  workDir = await mkdtemp(join(tmpdir(), 'arm-memory-projection-wiring-'));
  coordinationFilePath = join(workDir, 'coordination.json');
  historyFilePath = join(workDir, 'history.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Test 1 — grep-checkable AC: main()'s body must reference both
// `readHistory` and `estimateOperationCost` (or the memory-projection module
// that wraps them), not merely have them imported at module scope for some
// OTHER, pre-existing beat handler (`handleQueryCapacity`/
// `probeAvailableCapacity` already import and use both today — this test
// must not be satisfied by that unrelated usage).
// ---------------------------------------------------------------------------

/**
 * Extracts the source text of `async function main() {`'s body from cli.mjs,
 * scoped narrowly enough to exclude every other function in the file
 * (including `handleQueryCapacity`/`probeAvailableCapacity`, which already
 * reference `readHistory`/`estimateOperationCost` for unrelated reasons).
 *
 * Deliberately a simple substring/line-range extraction, not full brace
 * matching (see this file's header, design assumption 1): finds the
 * `async function main() {` marker, then the top-level `main().catch(`
 * call that follows it in this file today, and returns everything strictly
 * between the two.
 */
function extractMainBody(source) {
  const startMarker = 'async function main() {';
  const startIndex = source.indexOf(startMarker);
  if (startIndex === -1) {
    throw new Error('extractMainBody: could not find "async function main() {" in cli.mjs');
  }
  const endMarker = '\nmain().catch(';
  const endIndex = source.indexOf(endMarker, startIndex);
  if (endIndex === -1) {
    throw new Error('extractMainBody: could not find the trailing "main().catch(" call after main()');
  }
  return source.slice(startIndex + startMarker.length, endIndex);
}

test('1. main() references both readHistory and estimateOperationCost (or the memory-projection module wrapping them)', async () => {
  const source = await readFile(CLI, 'utf8');
  const mainBody = extractMainBody(source);

  // FAILS TODAY: main() (lines ~3717-5049 as of this ticket's Phase 0/1
  // handoff) calls neither readHistory nor estimateOperationCost anywhere —
  // both are only referenced from handleQueryCapacity/probeAvailableCapacity,
  // functions OUTSIDE main()'s own body, which extractMainBody's scoping
  // deliberately excludes.
  const referencesReadHistory = /readHistory\s*\(/.test(mainBody);
  const referencesCostEstimateOrWrapper =
    /estimateOperationCost\s*\(/.test(mainBody) || /computeProjectedPeakMemoryMb\s*\(/.test(mainBody);

  expect(referencesReadHistory).toBe(true);
  expect(referencesCostEstimateOrWrapper).toBe(true);
});

// ---------------------------------------------------------------------------
// Test 2 — refuses when the projected sum exceeds budget, admits when under.
// ---------------------------------------------------------------------------

test('2a. refuses admission with type "memory-projection-block" when a single class alone would exceed the memory budget', () => {
  const AGENT_CLASS = 'oversized-agent';
  // A single class whose own p90 peak (median of the two most recent
  // recorded peaks — see ./lib/cost-estimate.mjs) is comfortably above any
  // reasonable margined budget under 16 GiB alone.
  seedHistory(historyFilePath, AGENT_CLASS, [15_000, 15_500, 15_800, 16_000]);

  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-oversized', '--desired-agents=1', `--agent-class=${AGENT_CLASS}`],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_HISTORY_FILE: historyFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');
  const parsed = parseStdout(stdout);

  // FAILS TODAY: no --agent-class flag, no memory-projection gate — this
  // beat resolves purely via the existing GREEN/GREEN axis classification
  // and returns 'spawn-allowed', never 'memory-projection-block'.
  expect(parsed.type).toBe('memory-projection-block');
  expect(parsed.type).not.toBe('spawn-allowed');
  // Mirrors 'pressure-block''s own convention (liveAgentGrant: null on any
  // beat that halts admission before ever reaching reserveAdmission) — a
  // blocked beat must never attempt (or report) a real ledger claim.
  expect(parsed.liveAgentGrant).toBeNull();
});

test('2b. admits with type "spawn-allowed" when the projected sum comfortably fits inside the memory budget', () => {
  const AGENT_CLASS = 'lightweight-agent';
  // A single, cheap class whose p90 peak is nowhere close to the 16 GiB
  // host figure, let alone any margined budget under it.
  seedHistory(historyFilePath, AGENT_CLASS, [256, 300, 320, 340]);

  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-lightweight', '--desired-agents=1', `--agent-class=${AGENT_CLASS}`],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_HISTORY_FILE: historyFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');
  const parsed = parseStdout(stdout);

  // Passes vacuously today for the WRONG reason (no gate exists at all, so
  // every GREEN/GREEN beat is 'spawn-allowed' regardless of --agent-class) —
  // paired with 2a (which DOES fail today), this test exists to prove the
  // eventual gate is a genuine two-sided branch, not a refusal-only stub
  // that would make 2a pass by rejecting everything unconditionally.
  expect(parsed.type).toBe('spawn-allowed');
  expect(Number(parsed.liveAgentGrant)).toBeGreaterThan(0);
});

// ---------------------------------------------------------------------------
// Test 2c (review, High finding) — `--desired-agents=N` must price N
// candidates, not always exactly one. A class priced comfortably under
// budget as a SINGLE candidate must still refuse once `desiredAgents` (and
// this uncontended orchestrator's own dibs-bounded share, which the cold-start
// AIMD ceiling of 4 does not constrain here) means FOUR are about to spawn.
// ---------------------------------------------------------------------------

test('2c. --desired-agents=N prices N candidates of the same class, not just one', () => {
  const AGENT_CLASS = 'multi-instance-agent';
  // Each instance's own empirical peak (~4000 MB) is nowhere near the ~14336
  // MB budget alone, but four of them (16000 MB) comfortably exceed it.
  seedHistory(historyFilePath, AGENT_CLASS, [4000, 4000, 4000, 4000]);

  const baseEnv = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_HISTORY_FILE: historyFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
  };

  const singleResult = runCli(
    ['--orchestrator-id=orch-multi-single', '--desired-agents=1', `--agent-class=${AGENT_CLASS}`],
    baseEnv,
  );
  expect(singleResult.status).toBe(0);
  expect(parseStdout(singleResult.stdout).type).toBe('spawn-allowed');

  const quadResult = runCli(
    ['--orchestrator-id=orch-multi-quad', '--desired-agents=4', `--agent-class=${AGENT_CLASS}`],
    baseEnv,
  );
  expect(quadResult.status).toBe(0);
  const quadParsed = parseStdout(quadResult.stdout);

  // FAILS without the fix: the gate always priced exactly ONE candidate
  // regardless of --desired-agents, so this beat would also resolve
  // 'spawn-allowed' with a projected sum of only ~4000 MB instead of ~16000
  // MB — silently admitting a 4-agent fleet the host cannot actually hold.
  //
  // Phase 1 — pricing 4 candidates in one shot (plus the earlier
  // `orch-multi-single` beat's own already-declared, still-live dibs entry
  // for this same class, shared via the same coordination file) exceeds
  // budget, but a smaller count fits comfortably, so the decrement search
  // admits AT that smaller, genuinely feasible count rather than refusing
  // the whole beat outright.
  expect(quadParsed.type).toBe('spawn-allowed');
  expect(quadParsed.memoryProjectionAdmittedCount).toBe(2);
  expect(Number(quadParsed.liveAgentGrant)).toBe(2);
});

// ---------------------------------------------------------------------------
// Test 3 — the pressure-override pre-check (Phase 0 backstop, Phase 4)
// must still short-circuit admission independently of the new memory gate:
// seeded with WARN pressure alongside history that is comfortably UNDER the
// memory budget (so the memory gate, once wired, would itself admit) — the
// pressure pre-check must still be the one that blocks, and must still run
// BEFORE the memory gate ever gets a say.
// ---------------------------------------------------------------------------

test('3. WARN pressure still blocks admission via "pressure-block" even when the candidate class is well under the memory budget', () => {
  const AGENT_CLASS = 'cheap-agent-under-pressure';
  seedHistory(historyFilePath, AGENT_CLASS, [256, 300, 320, 340]);

  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-pressure', '--desired-agents=1', `--agent-class=${AGENT_CLASS}`],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_HISTORY_FILE: historyFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_WARN, disk: DISK_GREEN }),
    },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');
  const parsed = parseStdout(stdout);

  // This assertion already passes today (the pressure pre-check predates
  // this ticket and needs no new wiring) — it is included here as a
  // regression pin: Phase 2 must not reorder the pressure pre-check to run
  // AFTER (or be folded into/replaced by) the new memory-projection gate.
  // If Phase 2's implementation moves the memory gate ahead of the pressure
  // pre-check, or lets a cheap-enough class bypass the pressure block
  // entirely, this assertion is what catches it.
  expect(parsed.type).toBe('pressure-block');
  expect(parsed.type).not.toBe('memory-projection-block');
  expect(parsed.pressureLevel).toBe(2);
  expect(parsed.liveAgentGrant).toBeNull();
});

// ---------------------------------------------------------------------------
// Test 4 — the AIMD ceiling (Phase 3, already shipped) must still
// independently govern count-based admission: seeded with an exhausted AIMD
// ceiling alongside history that is comfortably UNDER the memory budget (so
// the memory gate, once wired, would itself admit) — the AIMD-derived
// liveAgentGrant of 0 must still be what limits this beat, unrelated to and
// unaffected by the new memory gate.
// ---------------------------------------------------------------------------

test('4. an exhausted AIMD ceiling still yields liveAgentGrant: 0 even when the candidate class is well under the memory budget (regression, not new behaviour)', async () => {
  const AGENT_CLASS = 'cheap-agent-under-aimd-ceiling';
  seedHistory(historyFilePath, AGENT_CLASS, [256, 300, 320, 340]);

  const SEEDED_CEILING = 1;
  await writeAimdCeilingState(
    coordinationFilePath,
    { ceiling: SEEDED_CEILING, sustainedNormalCount: 0 },
    { now: 1_700_000_000_000 },
  );

  const baseEnv = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_HISTORY_FILE: historyFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
  };

  // Exhausts the seeded ceiling (1) via a real --claim from a DIFFERENT
  // orchestrator, mirroring ./cli-aimd-ceiling.jest.spec.mjs's own "fill,
  // then observe the next beat is bounded" shape.
  const fill = runCli([`--claim=${LIVE_AGENT_CLAIM_TYPE}:${SEEDED_CEILING}`, '--orchestrator-id=orch-filler'], baseEnv);
  expect(fill.status).toBe(0);
  expect(parseStdout(fill.stdout).granted).toBe(SEEDED_CEILING);

  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-aimd-check', '--desired-agents=1', `--agent-class=${AGENT_CLASS}`],
    baseEnv,
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');
  const parsed = parseStdout(stdout);

  // This assertion already passes today (the AIMD ceiling predates this
  // ticket) — included as a regression pin, same rationale as test 3: Phase
  // 2 must not let the new memory gate mask, replace, or otherwise change
  // this pre-existing count-based ceiling behaviour.
  expect(parsed.type).toBe('spawn-allowed');
  expect(parsed.liveAgentGrant).toBe(0);
});

// ---------------------------------------------------------------------------
// Test 5 — refusal reasons stay distinguishable across all three gates: a
// generic "refused" collapse would make it impossible for an orchestrator
// (or a future debugging session) to tell WHY a beat was denied.
// ---------------------------------------------------------------------------

test('5. memory-projection-block, pressure-block, and an AIMD-driven refusal are all distinguishable from one another', () => {
  const OVERSIZED_CLASS = 'distinguishability-oversized';
  const CHEAP_CLASS = 'distinguishability-cheap';
  seedHistory(historyFilePath, OVERSIZED_CLASS, [15_000, 15_500, 15_800, 16_000]);
  seedHistory(historyFilePath, CHEAP_CLASS, [256, 300, 320, 340]);

  // Scenario A — memory-projection-block.
  const memoryBlockResult = runCli(
    ['--orchestrator-id=orch-dist-memory', '--desired-agents=1', `--agent-class=${OVERSIZED_CLASS}`],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_HISTORY_FILE: historyFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    },
  );
  expect(memoryBlockResult.status).toBe(0);
  const memoryBlockParsed = parseStdout(memoryBlockResult.stdout);

  // Scenario B — pressure-block (fresh coordination file: pressure-blocked
  // beats never touch the live-agent/dibs ledgers, but isolating avoids any
  // cross-scenario ledger interaction from Scenario A muddying this one).
  const pressureBlockResult = runCli(
    ['--orchestrator-id=orch-dist-pressure', '--desired-agents=1', `--agent-class=${CHEAP_CLASS}`],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_HISTORY_FILE: historyFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_WARN, disk: DISK_GREEN }),
    },
  );
  expect(pressureBlockResult.status).toBe(0);
  const pressureBlockParsed = parseStdout(pressureBlockResult.stdout);

  // FAILS TODAY (memoryBlockParsed.type): with no memory-projection gate,
  // Scenario A resolves to 'spawn-allowed', identical to a healthy beat —
  // indistinguishable from an admitted beat, let alone from Scenario B.
  expect(memoryBlockParsed.type).toBe('memory-projection-block');
  expect(pressureBlockParsed.type).toBe('pressure-block');
  expect(memoryBlockParsed.type).not.toBe(pressureBlockParsed.type);

  // Neither block type is the generic 'hold'/'alert'/'pause' literal
  // buildTrafficLight already produces for plain AMBER/RED axis
  // classification — both are new, admission-refusal-specific literals, not
  // a repurposing of an existing ambiguous one.
  expect(['hold', 'alert', 'pause']).not.toContain(memoryBlockParsed.type);
  expect(['hold', 'alert', 'pause']).not.toContain(pressureBlockParsed.type);

  // AIMD's own refusal signal is deliberately NOT a `type` literal — it is
  // the sibling `liveAgentGrant: 0` field alongside `type: 'spawn-allowed'`
  // (Phase 3 Option B — see cli.mjs's own comment on this exact
  // design choice). This is already a THIRD, independently-identifiable
  // shape distinct from both block types above: neither block type is
  // 'spawn-allowed' at all, so a reader checking `type` first and
  // `liveAgentGrant` second can always tell all three apart. Asserted here
  // as a documentation-level pin, not a new runtime check — test 4 above is
  // the behavioural assertion for this shape.
  expect(memoryBlockParsed.type).not.toBe('spawn-allowed');
  expect(pressureBlockParsed.type).not.toBe('spawn-allowed');
});

// ---------------------------------------------------------------------------
// Test 6 — history-read-count budget: the new gate must add no more than one
// additional history-file read per beat, even when several distinct running
// classes' history all need to be summed alongside the candidate's own.
//
// Mechanism chosen (see this file's header, design assumption 2):
// `ARM_HISTORY_READ_COUNT_FILE` — a NEW env-var hook Phase 2's implementer
// must add to `lib/history.mjs`'s `readStore()`, appending one `'1\n'` line
// to the given path per genuine on-disk read attempt of the history file.
// Chosen over alternatives because this file runs the real CLI as a child
// process (spawnSync) throughout, matching every other file in this
// directory's black-box idiom — an in-process `jest.spyOn` cannot observe
// calls made inside a separately-spawned `node` process, and there is no
// portable, CI-safe way to count real `read()`/`open()` syscalls made by an
// arbitrary child process without a platform-specific tracer.
// ---------------------------------------------------------------------------

test('6. adds at most one additional history-file read per beat, even with several distinct running classes', async () => {
  const CANDIDATE_CLASS = 'read-budget-candidate';
  const RUNNING_CLASS_A = 'read-budget-running-a';
  const RUNNING_CLASS_B = 'read-budget-running-b';
  const RUNNING_CLASS_C = 'read-budget-running-c';

  // All four classes cheap enough that nothing here is meant to trip the
  // memory-projection gate itself — this test is purely about I/O count.
  [CANDIDATE_CLASS, RUNNING_CLASS_A, RUNNING_CLASS_B, RUNNING_CLASS_C].forEach((agentClass) => {
    seedHistory(historyFilePath, agentClass, [256, 300, 320, 340]);
  });

  const baseEnv = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_HISTORY_FILE: historyFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
  };

  // Establishes three DISTINCT "currently running" classes via real,
  // successful prior --desired-agents --agent-class calls from distinct
  // orchestrators (Phase 0's own assumption #3 — a live dibs entry per
  // orchestrator declaring an --agent-class stands in for "this class is
  // currently running").
  [RUNNING_CLASS_A, RUNNING_CLASS_B, RUNNING_CLASS_C].forEach((runningClass, index) => {
    const seedRunning = runCli(
      [`--orchestrator-id=orch-running-${index}`, '--desired-agents=1', `--agent-class=${runningClass}`],
      baseEnv,
    );
    expect(seedRunning.status).toBe(0);
  });

  const historyReadCountFile = join(workDir, 'history-read-count.log');
  await writeFile(historyReadCountFile, '');

  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-read-budget-check', '--desired-agents=1', `--agent-class=${CANDIDATE_CLASS}`],
    { ...baseEnv, ARM_HISTORY_READ_COUNT_FILE: historyReadCountFile },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');
  parseStdout(stdout); // still valid JSON — sanity check, not the point of this test.

  const readCountLog = await readFile(historyReadCountFile, 'utf8');
  const readCount = readCountLog.split('\n').filter((line) => line.trim().length > 0).length;

  // Lower bound FAILS TODAY: neither the ARM_HISTORY_READ_COUNT_FILE hook
  // (lib/history.mjs's readStore()) nor any call into
  // readHistory/estimateOperationCost/computeProjectedPeakMemoryMb exists
  // inside main() yet (test 1) — this log file stays empty, readCount === 0,
  // failing the ">= 1" assertion below. That failure is deliberate: a
  // vacuous "0 <= 1" pass would prove nothing about the budget, only that
  // nobody read anything at all. Once Phase 2 wires the gate AND adds the
  // hook, the lower bound proves the gate genuinely consulted history for
  // this beat, and the upper bound is the actual budget this test exists to
  // pin: reading the WHOLE history store once per beat (mirroring
  // readStore's own whole-file-keyed-by-type shape) and extracting all four
  // classes' entries from that single read, rather than calling
  // readHistory once per class (which would each independently re-read the
  // file, yielding 4 reads here, not 1).
  expect(readCount).toBeGreaterThanOrEqual(1);
  expect(readCount).toBeLessThanOrEqual(1);
});

// ---------------------------------------------------------------------------
// Test 7 — lock non-nesting / no deadlock: the new gate's own lock and
// reserveAdmission's lock must both be reachable, sequentially, within one
// beat without hanging. Lightweight per this ticket's own scope (a real
// concurrent race-safety test belongs in Phase 3) — this only proves
// sequential correctness completes in bounded time.
// ---------------------------------------------------------------------------

test('7. a beat that must clear both the memory-projection gate and reserveAdmission completes within a bounded time, sequentially correct', async () => {
  const AGENT_CLASS = 'lock-sequencing-agent';
  // Comfortably under budget, so (once wired) the memory gate admits and
  // reserveAdmission's own lock is also reached in the SAME beat — the
  // sequencing this test is actually about. A refusal earlier in the
  // pipeline would never reach reserveAdmission at all, which would prove
  // nothing about the two locks coexisting.
  seedHistory(historyFilePath, AGENT_CLASS, [256, 300, 320, 340]);

  // Reuses test 6's ARM_HISTORY_READ_COUNT_FILE hook purely as a witness
  // that the memory-projection gate's own lock/read genuinely ran THIS
  // beat, alongside reserveAdmission's — without it, a passing
  // 'spawn-allowed' + non-null liveAgentGrant result would be indistinguishable
  // from today's baseline behaviour (which reaches reserveAdmission for a
  // completely unrelated reason: no gate exists yet to sequence ahead of
  // it).
  const historyReadCountFile = join(workDir, 'lock-sequencing-history-read-count.log');
  await writeFile(historyReadCountFile, '');

  const startedAt = Date.now();
  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-lock-sequencing', '--desired-agents=1', `--agent-class=${AGENT_CLASS}`],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_HISTORY_FILE: historyFilePath,
      ARM_HISTORY_READ_COUNT_FILE: historyReadCountFile,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    },
  );
  const elapsedMs = Date.now() - startedAt;

  expect(status).toBe(0);
  expect(stderr).toBe('');
  // Generous bound (this directory's other lock-heavy tests, e.g.
  // ./cli-two-orchestrators.jest.spec.mjs's race tests, complete in low
  // hundreds of ms even under real concurrent contention) — this is a
  // hang/deadlock detector, not a performance budget.
  expect(elapsedMs).toBeLessThan(5000);

  const parsed = parseStdout(stdout);
  expect(parsed.type).toBe('spawn-allowed');
  expect(Number(parsed.liveAgentGrant)).toBeGreaterThan(0);

  // FAILS TODAY: the ARM_HISTORY_READ_COUNT_FILE hook doesn't exist yet
  // (test 6) and main() never calls into the memory-projection gate at all
  // (test 1), so this log stays empty — proving today's passing
  // type/liveAgentGrant assertions above are, as documented, reaching
  // reserveAdmission for the wrong reason (no gate ever ran ahead of it).
  // Once Phase 2 wires both the gate and the hook, this is the assertion
  // that the gate's own lock genuinely fired in this same beat, sequentially
  // before reserveAdmission's, without the process hanging.
  const readCountLog = await readFile(historyReadCountFile, 'utf8');
  const readCount = readCountLog.split('\n').filter((line) => line.trim().length > 0).length;
  expect(readCount).toBeGreaterThanOrEqual(1);
});
