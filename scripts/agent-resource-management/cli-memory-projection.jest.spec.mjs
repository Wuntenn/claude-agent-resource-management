// Phase 0 — outer acceptance test for the per-agent-class
// projected-peak admission gate.
//
// RED by construction: none of Phase 1/2/3's actual feature exists yet.
// `main()`'s `--desired-agents` beat has no `--agent-class` flag, no
// per-class "sum of currently-live agents' projected peaks vs. the 16 GiB
// host budget" calculation exists anywhere, and no `memory-projection-block`
// traffic-light `type` is ever emitted. This file is intentionally
// black-box (spawns the real `cli.mjs`, never imports it — matches every
// other file in this directory's idiom, e.g. ./cli-two-orchestrators.jest.spec.mjs)
// and stays red until Phases 1-3 of land.
//
// ---------------------------------------------------------------------------
// Design assumptions this file bakes in (Phase 1/2's implementer must either
// match these or come back and adjust this test — that's expected for a
// Phase 0 outer test, not a locked contract):
//
// 1. `--agent-class=<name>` is a NEW flag on `main()`'s `--desired-agents`
//    beat (today it's parsed only inside `--poll-footprint`'s handler — see
//    cli.mjs's `handlePollFootprint`). Assumed shape:
//
//      node cli.mjs --orchestrator-id=<id> --desired-agents=<n> --agent-class=<name>
//
// 2. The projected-peak calculation sums HISTORICAL p90 peaks (exactly
//    `estimateOperationCost`'s existing `estimatedPeakMemoryMb`, keyed by
//    operation type — ./lib/cost-estimate.mjs) of the agent classes that are
//    CURRENTLY LIVE (i.e. have an outstanding, non-stale dibs entry
//    declaring that `--agent-class`), not live per-process footprint
//    sampling (no `ps`/`phys_footprint` polling is exercised here — that's
//    ./poll-footprint.jest.spec.mjs's/./footprint-outer-acceptance.jest.spec.mjs's
//    territory).
//
//    This file therefore ASSUMES `--agent-class` doubles as the
//    `--record-outcome`/`readHistory` `operationType` key (i.e. seeding
//    history via `--record-outcome --operation-type=<agentClass>` is
//    equivalent to seeding "this agent class's" cost history) — there is no
//    separate agentClass-to-operationType mapping layer in the codebase
//    today, and unifying the two keys is the simplest reading of the
//    ticket's "per-agent-class" framing consistent with what already exists
//    (`./lib/history.mjs`'s `readHistory(historyFilePath, operationType)`).
//
// 3. "Currently running agents of class X" is modelled by PRIOR, real
//    `--desired-agents --agent-class=X` calls from DISTINCT orchestrator ids
//    against the SAME shared coordination file — each successful call is
//    assumed to persist a dibs entry the projected-peak gate can later sum
//    over (mirroring how `computePerOrchestratorAllowance` already sums
//    live dibs entries for its own, unrelated per-axis cap — see
//    ./cli-two-orchestrators.jest.spec.mjs test 4). No new dibs-declaring
//    flag is invented for this — reusing the existing `--desired-agents`
//    beat itself as "declare an agent of this class" is the minimal
//    extension consistent with gap #2 in the task brief.
//
// 4. The refusal `type` literal is assumed to be `'memory-projection-block'`
//    — consistent with the existing `'pressure-block'` precedent (cli.mjs,
//    `main()`'s WARN/CRITICAL pressure short-circuit) for a beat that halts
//    admission for a reason OTHER than the plain AMBER/RED axis
//    classification that produces `'hold'`.
//
// 5. The budget is 16 GiB (`TOTAL_HOST_MEMORY_GIB` below) minus a safety
//    margin whose exact value is TBD per the ticket — this file only
//    asserts refusal happens at some point STRICTLY BEFORE the cumulative
//    projected sum reaches the full, un-marginned 16 GiB figure, never
//    pinning an exact margin value Phase 1/2 must match bit-for-bit.
//
// 6. Fixtures use GREEN/GREEN pressure+memory+disk readings throughout
//    (mirrors ./cli-two-orchestrators.jest.spec.mjs's `MEMORY_GREEN`/
//    `DISK_GREEN`) so neither the existing pressure-override pre-check
//    (see ./pressure-override.jest.spec.mjs) nor the existing AIMD ceiling
//    (see ./cli-aimd-ceiling.jest.spec.mjs) is what trips these tests —
//    only the new memory-projection gate is meant to be exercised, once it
//    exists.
// ---------------------------------------------------------------------------

import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { writeAimdCeilingState } from './lib/coordination-file.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

const TOTAL_HOST_MEMORY_GIB = 16;

/** Runs the real cli.mjs as a child process — never imports it directly. */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/**
 * Runs the real cli.mjs as a genuinely CONCURRENT child process — mirrors
 * ./cli-two-orchestrators.jest.spec.mjs's `runCliAsync` exactly.
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

/** Seeds `count` --record-outcome observations for `operationType` (== agentClass, assumption #2 above) via the real CLI. */
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
  workDir = await mkdtemp(join(tmpdir(), 'arm-memory-projection-'));
  coordinationFilePath = join(workDir, 'coordination.json');
  historyFilePath = join(workDir, 'history.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

test('1. sequential refusal: repeated --desired-agents --agent-class calls are refused once the projected-peak sum crosses the budgeted margin, strictly before the raw 16 GiB physical figure', async () => {
  const AGENT_CLASS = 'incident-agent';

  // Seeds the incident's own shape: ~2 GB peaks, climbing at roughly
  // +0.26 GB/min across successive runs (2048 -> 2313 -> 2578 -> 2843 MB).
  // The climb rate itself is not consumed by any gate this test exercises
  // (there is no time-series/trend input to the projected-peak sum, only a
  // per-class point estimate via `estimateOperationCost`'s median-of-recent-
  // peaks) — it's recorded here purely so this fixture is traceably the
  // incident's own numbers, not an arbitrary round figure.
  const INCIDENT_PEAKS_MB = [2048, 2313, 2578, 2843];
  seedHistory(historyFilePath, AGENT_CLASS, INCIDENT_PEAKS_MB);
  // median([2313, 2578]) == 2445.5 MB per agent of this class — the
  // estimated per-instance projected peak `estimateOperationCost` will
  // report once ARM_HISTORY_FILE is read for AGENT_CLASS (assumption #2).
  const ESTIMATED_PEAK_PER_AGENT_GIB = 2445.5 / 1024;

  const baseEnv = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_HISTORY_FILE: historyFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
  };

  // Phase 2 implementer's note: the memory-projection gate is deliberately
  // gated on the SAME `!isHeartbeat && isSpawnEligibleBeat` condition as
  // `reserveAdmission`/the AIMD-advance step (see cli.mjs's own comment on
  // that gate) — a beat whose dibs-divided `perOrchestratorAllowance` is
  // already 0 never reaches the memory gate at all, exactly like it never
  // reaches `reserveAdmission`. With 8 DISTINCT contending orchestrators and
  // the default cold-start AIMD ceiling (4), `perOrchestratorAllowance`
  // itself hits 0 by the 6th orchestrator — before the projected-peak sum
  // crosses the memory budget at these fixture values — which would mask
  // this test's intended `memory-projection-block` behind an unrelated,
  // pre-existing `spawn-allowed`/`liveAgentGrant: null` (dibs-exhausted)
  // beat instead. Seeding a generous ceiling here isolates the property this
  // test actually exists to pin (the memory-projection gate, not the
  // unrelated AIMD/dibs-fairness ceiling this fixture would otherwise
  // collide with) — consistent with this file's own header, which documents
  // every fixture assumption here as adjustable by Phase 2's implementer.
  await writeAimdCeilingState(coordinationFilePath, { ceiling: 64, sustainedNormalCount: 0 }, { now: Date.now() });

  let refused = false;
  let cumulativeProjectedGib = 0;
  let refusalIndex = null;

  // Issue repeated --desired-agents calls, each from a DISTINCT orchestrator
  // (assumption #3 above) declaring one more "currently running" agent of
  // AGENT_CLASS, until the projected sum would cross the 16 GiB host budget.
  // 8 calls * ~2.4 GiB/agent ~= 19 GiB, comfortably past 16 GiB, so a
  // crossing point is guaranteed to occur within this loop.
  for (let callIndex = 1; callIndex <= 8 && !refused; callIndex += 1) {
    cumulativeProjectedGib += ESTIMATED_PEAK_PER_AGENT_GIB;

    const { status, stdout, stderr } = runCli(
      [`--orchestrator-id=orch-incident-${callIndex}`, '--desired-agents=1', `--agent-class=${AGENT_CLASS}`],
      baseEnv,
    );
    expect(status).toBe(0);
    expect(stderr).toBe('');

    const parsed = parseStdout(stdout);

    // FAILS TODAY: `type` is never `'memory-projection-block'` — the flag
    // and the gate both do not exist yet, so every call in this loop
    // currently returns some other `type` (`spawn-allowed`/`hold`/
    // `pressure-block`, never this one) regardless of how many agents of
    // AGENT_CLASS have "accumulated" via this loop's prior calls.
    if (parsed.type === 'memory-projection-block') {
      refused = true;
      refusalIndex = callIndex;
    }
  }

  expect(refused).toBe(true);
  // The core budget-safety property: refusal must land BEFORE the
  // cumulative projected sum reaches the raw, un-marginned 16 GiB physical
  // figure — i.e. there was genuine headroom held back as a safety margin,
  // not a gate that only trips once the host is already exhausted.
  expect(cumulativeProjectedGib).toBeLessThan(TOTAL_HOST_MEMORY_GIB);
  expect(refusalIndex).not.toBeNull();
});

test('2. race safety: two orchestrators of DIFFERENT agent classes admitted concurrently must not together exceed the memory budget', async () => {
  const CLASS_A = 'race-class-a';
  const CLASS_B = 'race-class-b';

  // Each class's own p90 peak individually fits comfortably inside the
  // budgeted margin (assumption #5 — exact margin TBD, but well under
  // 16 GiB alone); only the COMBINED sum (~16 GiB) would exceed any
  // reasonable safety-margined budget below the raw 16 GiB host figure.
  const PER_CLASS_PEAK_MB = [8192, 8192, 8192];
  seedHistory(historyFilePath, CLASS_A, PER_CLASS_PEAK_MB);
  seedHistory(historyFilePath, CLASS_B, PER_CLASS_PEAK_MB);

  const baseEnv = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_HISTORY_FILE: historyFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
  };

  // Genuinely concurrent — Promise.all, real OS scheduling decides ordering,
  // both racing for the same coordination-file lock (mirrors
  // ./cli-two-orchestrators.jest.spec.mjs test 10's race shape).
  const [resultA, resultB] = await Promise.all([
    runCliAsync(
      ['--orchestrator-id=orch-race-class-a', '--desired-agents=1', `--agent-class=${CLASS_A}`],
      baseEnv,
    ),
    runCliAsync(
      ['--orchestrator-id=orch-race-class-b', '--desired-agents=1', `--agent-class=${CLASS_B}`],
      baseEnv,
    ),
  ]);

  expect(resultA.status).toBe(0);
  expect(resultB.status).toBe(0);

  const parsedA = parseStdout(resultA.stdout);
  const parsedB = parseStdout(resultB.stdout);

  // The core race-safety property: not BOTH racers may be granted admission
  // when their combined projected peak (~16 GiB) would exceed the budgeted
  // margin — the ledger-level re-check (mirroring `reserveAdmission`'s
  // existing atomic snapshot+othersHeld arithmetic for the unrelated
  // live-agent ceiling) must be the actual backstop, not a pre-lock read
  // that both racers could independently see as "still under budget".
  //
  // FAILS TODAY: neither `--agent-class` nor any memory-projection
  // admission check exists — both calls are decided purely by the existing,
  // unrelated GREEN/GREEN axis classification and the existing flat
  // live-agent ceiling, so both racers are typically granted
  // `'spawn-allowed'` with a non-null `liveAgentGrant`, regardless of their
  // combined projected peak.
  const bothGrantedLiveAgent =
    parsedA.type === 'spawn-allowed' &&
    parsedB.type === 'spawn-allowed' &&
    Number(parsedA.liveAgentGrant) > 0 &&
    Number(parsedB.liveAgentGrant) > 0;

  expect(bothGrantedLiveAgent).toBe(false);
});

// ---------------------------------------------------------------------------
// Phase 2 review, High finding (security) — an orchestrator that omits
// `--agent-class` entirely must NOT be exempt from this gate, on either side
// of the sum. Before this fix, two concurrent unlabelled orchestrators
// reproduced the original FINDING-5 race unmodified: the gate never ran for
// them (no candidate class to price), and neither was visible to the OTHER's
// `runningClasses` sum (their dibs entries carried no agent-class field at
// all). This mirrors test 2's exact race shape, but both racers omit
// `--agent-class` — proving the catch-all `UNLABELLED_AGENT_CLASS` sentinel
// (cli.mjs) now closes that gap: unlabelled beats are priced and summed
// exactly like any other class.
// ---------------------------------------------------------------------------

test('3. an unlabelled agent class (no --agent-class at all) is no longer exempt from the gate — two concurrent unlabelled beats must not together exceed the memory budget', async () => {
  // Mirrors cli.mjs's own `UNLABELLED_AGENT_CLASS` sentinel — deliberately
  // NOT imported (this file stays black-box, spawning the real CLI, never
  // importing it — see this file's header) so this literal must be kept in
  // sync with cli.mjs's constant by hand if that sentinel's value ever
  // changes.
  const UNLABELLED_AGENT_CLASS = '__unlabelled__';

  // Each individual beat's own p90 peak fits comfortably inside the budgeted
  // margin; only the COMBINED sum (~16 GiB) would exceed any reasonable
  // safety-margined budget below the raw 16 GiB host figure — identical
  // shape to test 2's per-class peaks, but seeded under the ONE shared
  // catch-all class both unlabelled racers are priced under.
  const PER_CLASS_PEAK_MB = [8192, 8192, 8192];
  seedHistory(historyFilePath, UNLABELLED_AGENT_CLASS, PER_CLASS_PEAK_MB);

  const baseEnv = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_HISTORY_FILE: historyFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
  };

  // Genuinely concurrent — neither call passes --agent-class at all.
  const [resultA, resultB] = await Promise.all([
    runCliAsync(['--orchestrator-id=orch-race-unlabelled-a', '--desired-agents=1'], baseEnv),
    runCliAsync(['--orchestrator-id=orch-race-unlabelled-b', '--desired-agents=1'], baseEnv),
  ]);

  expect(resultA.status).toBe(0);
  expect(resultB.status).toBe(0);

  const parsedA = parseStdout(resultA.stdout);
  const parsedB = parseStdout(resultB.stdout);

  // FAILS PRE-FIX: with no --agent-class, `agentClass` degraded to
  // `undefined` and the gate's own `typeof agentClass === 'string'` guard
  // skipped it entirely for BOTH racers — both would be granted
  // 'spawn-allowed' with a real liveAgentGrant regardless of their combined
  // (real) memory consumption.
  const bothGrantedLiveAgent =
    parsedA.type === 'spawn-allowed' &&
    parsedB.type === 'spawn-allowed' &&
    Number(parsedA.liveAgentGrant) > 0 &&
    Number(parsedB.liveAgentGrant) > 0;

  expect(bothGrantedLiveAgent).toBe(false);
});

// ---------------------------------------------------------------------------
// Phase 2 review, Medium finding (stability + typescript) — the dibs
// schema previously stored only ONE `agentClass` per `orchestratorId`,
// so an orchestrator running TWO concurrently-live agent classes had its
// earlier class silently overwritten (undercounted) the moment it declared
// the second. `--running-agent-classes=<a>,<b>,...` (cli.mjs) closes this by
// letting an orchestrator declare every class it currently has live, not
// just the one candidate this beat is requesting.
// ---------------------------------------------------------------------------

test('4. a single orchestrator running two agent classes concurrently contributes BOTH to another orchestrator\'s projected sum', async () => {
  const CLASS_X = 'multi-class-x';
  const CLASS_Y = 'multi-class-y';
  const CANDIDATE_CLASS = 'multi-class-candidate';

  // X and Y individually comfortably fit the budget; X + Y together already
  // consume most of it, so a LATER orchestrator's own (cheap) candidate
  // class only tips the combined projection over budget if BOTH X and Y are
  // genuinely counted — a schema that lost X (or Y) the moment the other was
  // declared would leave enough headroom for the candidate to be admitted
  // wrongly.
  seedHistory(historyFilePath, CLASS_X, [6144, 6144, 6144]);
  seedHistory(historyFilePath, CLASS_Y, [6144, 6144, 6144]);
  seedHistory(historyFilePath, CANDIDATE_CLASS, [4096, 4096, 4096]);

  const baseEnv = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_HISTORY_FILE: historyFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
  };

  // One orchestrator declares BOTH classes as concurrently live in a single
  // beat: `--agent-class` names the one it's requesting admission for THIS
  // beat (X), `--running-agent-classes` names the other one it already has
  // running (Y) — both must land in this orchestrator's dibs entry.
  const multiClassBeat = runCli(
    [
      '--orchestrator-id=orch-multi-class',
      '--desired-agents=1',
      `--agent-class=${CLASS_X}`,
      `--running-agent-classes=${CLASS_Y}`,
    ],
    baseEnv,
  );
  expect(multiClassBeat.status).toBe(0);
  expect(multiClassBeat.stderr).toBe('');

  // A LATER, distinct orchestrator asks for the cheap candidate class. If
  // the dibs schema had lost CLASS_Y (the pre-fix single-`agentClass` upsert
  // shape), the projected sum here would be CLASS_X + CANDIDATE_CLASS only
  // (~10 GiB) — comfortably admitted. With BOTH X and Y correctly counted,
  // the projected sum is X + Y + candidate (~16 GiB), which must be refused.
  const candidateBeat = runCli(
    ['--orchestrator-id=orch-multi-class-observer', '--desired-agents=1', `--agent-class=${CANDIDATE_CLASS}`],
    baseEnv,
  );
  expect(candidateBeat.status).toBe(0);
  const parsedCandidate = parseStdout(candidateBeat.stdout);

  expect(parsedCandidate.type).toBe('memory-projection-block');
  expect(parsedCandidate.totalProjectedMemoryMb).toBeGreaterThanOrEqual(6144 + 6144 + 4096);
});

// ---------------------------------------------------------------------------
// review, Nit — a caller passing `--agent-class=__unlabelled__`
// LITERALLY is indistinguishable from an omitted `--agent-class` once it
// reaches the dibs entry, silently pooling its own history/admission
// decisions into the shared catch-all bucket rather than getting a class of
// its own. `cli.mjs` now rejects this explicitly with exit code 2, the same
// way `isReservedEntry` rejects a colliding `--orchestrator-id`.
// ---------------------------------------------------------------------------

test('6. --agent-class literally equal to the reserved unlabelled sentinel is rejected', () => {
  const UNLABELLED_AGENT_CLASS = '__unlabelled__';

  const result = runCli(
    [`--orchestrator-id=orch-reject-sentinel-collision`, '--desired-agents=1', `--agent-class=${UNLABELLED_AGENT_CLASS}`],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_HISTORY_FILE: historyFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    },
  );

  expect(result.status).toBe(2);
  expect(result.stderr).toContain('--agent-class must not equal the reserved sentinel');
});

// ---------------------------------------------------------------------------
// review (post-verification-pass) — HIGH finding investigated: does a
// `--heartbeat` beat's own `declareDibs` call (main()'s single, SHARED call
// site — the same one a normal `--desired-agents` beat uses; `cli.mjs` has
// only ONE `declareDibs` call inside `main()`, not a heartbeat-specific
// duplicate) silently wipe `agentClasses`/hysteresis history the way the now-
// fixed `--evict-pid` liveness-refresh call site once did (see
// ./cli-evict.jest.spec.mjs scenario 19)? This test proves BEHAVIORIALLY that
// it does not: an orchestrator's `--heartbeat` tick, passing the SAME
// `--agent-class`/`--running-agent-classes` set as its last real beat (the
// calling convention every beat — heartbeat or not — is documented to
// follow, per SKILL.md's "--running-agent-classes" section), must leave both
// classes visible to a later orchestrator's projected-peak sum exactly as
// test 4 above proves for a real beat. A regression that special-cased
// `--heartbeat` into a lighter-weight `declareDibs` call omitting
// `agentClasses` (reproducing the `--evict-pid` bug shape a third time) would
// let CLASS_Y disappear here, under-projecting the sum and wrongly admitting
// the candidate.
// ---------------------------------------------------------------------------

test('5. a --heartbeat tick between beats preserves an orchestrator\'s declared agent classes', async () => {
  const CLASS_X = 'heartbeat-preserve-x';
  const CLASS_Y = 'heartbeat-preserve-y';
  const CANDIDATE_CLASS = 'heartbeat-preserve-candidate';

  seedHistory(historyFilePath, CLASS_X, [6144, 6144, 6144]);
  seedHistory(historyFilePath, CLASS_Y, [6144, 6144, 6144]);
  seedHistory(historyFilePath, CANDIDATE_CLASS, [4096, 4096, 4096]);

  const baseEnv = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_HISTORY_FILE: historyFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
  };

  // A real beat declares both classes as concurrently live, exactly as test
  // 4 does.
  const multiClassBeat = runCli(
    [
      '--orchestrator-id=orch-heartbeat-preserve',
      '--desired-agents=1',
      `--agent-class=${CLASS_X}`,
      `--running-agent-classes=${CLASS_Y}`,
    ],
    baseEnv,
  );
  expect(multiClassBeat.status).toBe(0);
  expect(multiClassBeat.stderr).toBe('');

  // A `--heartbeat` tick from the SAME orchestrator, between real beats,
  // re-declaring the same currently-live classes — the documented calling
  // convention (SKILL.md: "the caller simply supplies the complete current
  // set each time"). Note this beat omits `--desired-agents` (heartbeats
  // never draw from it) but must still pass through main()'s single, shared
  // `declareDibs` call with `agentClasses`/history intact.
  const heartbeatBeat = runCli(
    [
      '--orchestrator-id=orch-heartbeat-preserve',
      '--heartbeat',
      `--agent-class=${CLASS_X}`,
      `--running-agent-classes=${CLASS_Y}`,
    ],
    baseEnv,
  );
  expect(heartbeatBeat.status).toBe(0);
  expect(heartbeatBeat.stderr).toBe('');

  // A LATER, distinct orchestrator asks for the cheap candidate class. If the
  // heartbeat tick above had wiped CLASS_X/CLASS_Y from the ledger, the
  // projected sum here would be CANDIDATE_CLASS alone (~4 GiB) — comfortably
  // admitted. With both classes still counted (surviving the heartbeat), the
  // projected sum is X + Y + candidate (~16 GiB), which must be refused.
  const candidateBeat = runCli(
    ['--orchestrator-id=orch-heartbeat-preserve-observer', '--desired-agents=1', `--agent-class=${CANDIDATE_CLASS}`],
    baseEnv,
  );
  expect(candidateBeat.status).toBe(0);
  const parsedCandidate = parseStdout(candidateBeat.stdout);

  expect(parsedCandidate.type).toBe('memory-projection-block');
  expect(parsedCandidate.totalProjectedMemoryMb).toBeGreaterThanOrEqual(6144 + 6144 + 4096);
});

test('7. a --heartbeat tick that OMITS both --agent-class and --running-agent-classes still preserves an orchestrator\'s declared agent classes (does not collapse to the unlabelled sentinel)', async () => {
  // review round 2, High finding — SKILL.md's own worked heartbeat
  // example passes neither flag (heartbeats are documented as never
  // reaching the memory-projection gate "regardless of --agent-class").
  // Unlike test 5 above (which re-passes both flags on every heartbeat),
  // this test pins the realistic calling convention: a bare
  // `--orchestrator-id=<id> --heartbeat` tick with no class flags at all
  // must NOT overwrite this orchestrator's previously-declared
  // `agentClasses` with `['__unlabelled__']`.
  const CLASS_X = 'heartbeat-omit-x';
  const CLASS_Y = 'heartbeat-omit-y';
  const CANDIDATE_CLASS = 'heartbeat-omit-candidate';

  seedHistory(historyFilePath, CLASS_X, [6144, 6144, 6144]);
  seedHistory(historyFilePath, CLASS_Y, [6144, 6144, 6144]);
  seedHistory(historyFilePath, CANDIDATE_CLASS, [4096, 4096, 4096]);

  const baseEnv = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_HISTORY_FILE: historyFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
  };

  const multiClassBeat = runCli(
    [
      '--orchestrator-id=orch-heartbeat-omit',
      '--desired-agents=1',
      `--agent-class=${CLASS_X}`,
      `--running-agent-classes=${CLASS_Y}`,
    ],
    baseEnv,
  );
  expect(multiClassBeat.status).toBe(0);
  expect(multiClassBeat.stderr).toBe('');

  // The bare heartbeat: no --agent-class, no --running-agent-classes.
  const heartbeatBeat = runCli(['--orchestrator-id=orch-heartbeat-omit', '--heartbeat'], baseEnv);
  expect(heartbeatBeat.status).toBe(0);
  expect(heartbeatBeat.stderr).toBe('');

  // A later, distinct orchestrator asks for the cheap candidate class. If
  // the bare heartbeat had collapsed CLASS_X/CLASS_Y down to the unlabelled
  // sentinel, the projected sum here would be CANDIDATE_CLASS alone (~4
  // GiB) — comfortably admitted. With both real classes still counted, the
  // projected sum is X + Y + candidate (~16 GiB), which must be refused.
  const candidateBeat = runCli(
    ['--orchestrator-id=orch-heartbeat-omit-observer', '--desired-agents=1', `--agent-class=${CANDIDATE_CLASS}`],
    baseEnv,
  );
  expect(candidateBeat.status).toBe(0);
  const parsedCandidate = parseStdout(candidateBeat.stdout);

  expect(parsedCandidate.type).toBe('memory-projection-block');
  expect(parsedCandidate.totalProjectedMemoryMb).toBeGreaterThanOrEqual(6144 + 6144 + 4096);
});
