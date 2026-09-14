// Phase 3 — wiring the AIMD-adjusted concurrency ceiling into cli.mjs's
// four LIVE_AGENT_CEILING call sites.
//
// RED by construction, right now: `cli.mjs` reads the FLAT constant
// `LIVE_AGENT_CEILING = 4` (line ~228) at all four call sites named in the
// Build Plan:
//   1. `probeAvailableCapacity`'s `--claim=live-agent:N` pre-lock probe
//      (~cli.mjs line 2033).
//   2. `--dequeue-if-capacity`'s `claimAndAdmitQueueEntry` call
//      (~cli.mjs line 2558).
//   3. `main()`'s advisory `allowanceConfig` object feeding
//      `computePerOrchestratorAllowance` (~cli.mjs lines 4205-4206) — the
//      4th call site the Build Plan's own text omits, but which is real and
//      load-bearing (see the convergence-analysis finding this ticket cites).
//   4. `main()`'s `--desired-agents` beat's `reserveAdmission` call
//      (~cli.mjs line 4369).
// None of the four reads `readAimdCeilingState` (Phase 1,
// ./lib/coordination-file.mjs) yet — every assertion below that requires a
// seeded, non-default ceiling to actually govern admission fails today.
//
// Black-box, real-process-spawning tests throughout — mirrors the
// established idiom in ./cli-dequeue-if-capacity.jest.spec.mjs and
// ./cli-two-orchestrators.jest.spec.mjs (never importing cli.mjs directly;
// ARM_COORDINATION_FILE/ARM_QUEUE_FILE env-var overrides for hermeticity),
// except where a Phase-1 lib-level function (`reserveAdmission`,
// `readAimdCeilingState`, `writeAimdCeilingState`) is imported and driven
// directly — those already exist and are the right layer to seed/observe
// persisted state and to probe `reserveAdmission`'s own contract.
//
// Cross-ledger isolation note (read before extending): `--claim`,
// `--dequeue-if-capacity`, and the `--desired-agents` beat all share ONE
// coordination file's `live-agent` (real claims) and `live-agent-admission`
// (advisory admission holds) ledgers, plus the dibs ledger the
// `--desired-agents` beat alone declares into. Chaining all three beat types
// against a single coordination file in one scenario means every later
// step's headroom is affected by every earlier step's ledger writes — the
// cross-ledger fix this codebase already relies on. Tests below that
// want to prove "this call site resolves against the seeded ceiling, not the
// flat 4" therefore use a FRESH, identically-seeded coordination file per
// call site rather than chaining all four through one shared file end to
// end: this isolates each assertion from unrelated ledger state left behind
// by a different call site's own check, while still proving every call site
// resolves the exact same persisted value.

import { spawn, spawnSync } from 'node:child_process';
import { mkdirSync, readFileSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import {
  AIMD_CEILING_RESERVED_ID,
  readAimdCeilingState,
  writeAimdCeilingState,
  reserveAdmission,
} from './lib/coordination-file.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const CLI = join(__dirname, 'cli.mjs');

const LIVE_AGENT_CLAIM_TYPE = 'live-agent';
const LIVE_AGENT_ADMISSION_CLAIM_TYPE = 'live-agent-admission';
const FLAT_LIVE_AGENT_CEILING = 4; // cli.mjs's pre-existing constant, LIVE_AGENT_CEILING.

const MEMORY_NORMAL = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };
const MEMORY_WARN = { pressureLevel: 2, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

/** Runs the real cli.mjs as a child process — never imports it directly. */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/** Genuinely concurrent (non-blocking) child-process runner — mirrors cli-two-orchestrators.jest.spec.mjs's runCliAsync. */
function runCliAsync(args, extraEnv) {
  return new Promise((resolvePromise, rejectPromise) => {
    const child = spawn('node', [CLI, ...args], { env: { ...process.env, ...extraEnv } });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => (stdout += chunk));
    child.stderr.on('data', (chunk) => (stderr += chunk));
    child.on('error', rejectPromise);
    child.on('close', (status) => resolvePromise({ status, stdout, stderr }));
  });
}

function parseStdout(stdout) {
  return JSON.parse(stdout);
}

function readRawEntries(coordinationFilePath) {
  return JSON.parse(readFileSync(coordinationFilePath, 'utf8'));
}

function readAimdEntryRaw(coordinationFilePath) {
  return readRawEntries(coordinationFilePath).find((entry) => entry.orchestratorId === AIMD_CEILING_RESERVED_ID);
}

async function makeWorkDir(prefix) {
  return mkdtemp(join(tmpdir(), prefix));
}

let workDirs = [];

afterEach(async () => {
  await Promise.all(workDirs.map((dir) => rm(dir, { recursive: true, force: true })));
  workDirs = [];
});

async function freshCoordinationFile(prefix) {
  const dir = await makeWorkDir(prefix);
  workDirs.push(dir);
  return join(dir, 'coordination.json');
}

// ---------------------------------------------------------------------------
// Scenario 1 — all four call sites resolve to the same, seeded AIMD-adjusted
// ceiling (6), not the flat 4.
// ---------------------------------------------------------------------------

describe('all four call sites resolve to the same AIMD-adjusted ceiling within one beat', () => {
  const SEEDED_CEILING = 6;

  test('--claim=live-agent:N grants up to the seeded ceiling (6), not the flat 4', async () => {
    const coordinationFilePath = await freshCoordinationFile('arm-aimd-claim-');
    await writeAimdCeilingState(coordinationFilePath, { ceiling: SEEDED_CEILING, sustainedNormalCount: 0 }, { now: 1_700_000_000_000 });

    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_NORMAL, disk: DISK_GREEN }),
    };

    const result = runCli([`--claim=${LIVE_AGENT_CLAIM_TYPE}:${SEEDED_CEILING}`, '--orchestrator-id=orch-claim-check'], env);
    expect(result.status).toBe(0);
    // FAILS TODAY: with LIVE_AGENT_CEILING unwired, this grants only 4.
    expect(parseStdout(result.stdout).granted).toBe(SEEDED_CEILING);
  });

  test('--dequeue-if-capacity grants against the seeded ceiling (6) once the flat-4 headroom is already exhausted', async () => {
    const coordinationFilePath = await freshCoordinationFile('arm-aimd-dequeue-');
    await writeAimdCeilingState(coordinationFilePath, { ceiling: SEEDED_CEILING, sustainedNormalCount: 0 }, { now: 1_700_000_000_000 });

    const queueDir = await makeWorkDir('arm-aimd-dequeue-queue-');
    workDirs.push(queueDir);
    const queueFilePath = join(queueDir, 'queue.json');

    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_QUEUE_FILE: queueFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_NORMAL, disk: DISK_GREEN }),
    };

    // Fill the real live-agent ledger to 5 — one MORE than the flat 4, but
    // still one short of the seeded ceiling (6). A flat-ceiling reader would
    // have already denied this fill call at 4; an AIMD-aware one grants it.
    const fill = runCli([`--claim=${LIVE_AGENT_CLAIM_TYPE}:5`, '--orchestrator-id=orch-filler'], env);
    expect(fill.status).toBe(0);
    // FAILS TODAY: a flat-ceiling --claim caps this fill at 4, not 5.
    expect(parseStdout(fill.stdout).granted).toBe(5);

    const enqueue = runCli(['--enqueue=implementation-agent:normal:aimd-dequeue-command', '--orchestrator-id=orch-seed'], env);
    expect(enqueue.status).toBe(0);

    const dequeue = runCli(['--dequeue-if-capacity', '--orchestrator-id=orch-dequeue'], env);
    expect(dequeue.status).toBe(0);
    const dequeueBody = parseStdout(dequeue.stdout);
    // FAILS TODAY: against the flat ceiling (4), 5 already-claimed live
    // agents leave zero headroom — this would be denied ('no-capacity').
    // Against the seeded ceiling (6), exactly 1 slot of headroom remains.
    expect(dequeueBody.granted).toBe(true);
    expect(dequeueBody.item.commandRef).toBe('aimd-dequeue-command');
  });

  test('the --desired-agents beat resolves the seeded ceiling (6) at BOTH the advisory allowanceConfig path and the reserveAdmission call, in the same beat', async () => {
    const coordinationFilePath = await freshCoordinationFile('arm-aimd-desired-agents-');
    await writeAimdCeilingState(coordinationFilePath, { ceiling: SEEDED_CEILING, sustainedNormalCount: 0 }, { now: 1_700_000_000_000 });

    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_NORMAL, disk: DISK_GREEN }),
    };

    // A SOLO (uncontended) orchestrator's advisory `allowance` is the flat
    // per-axis cap unmodified — the cleanest possible readout of what
    // `allowanceConfig.maxAgentsMemoryAxis`/`maxAgentsDiskAxis` were actually
    // set to for this beat. `desiredAgents=5` (above the flat 4, at or below
    // the seeded 6) makes `reserveAdmission`'s own grant a genuine
    // discriminator too — a flat-ceiling reserveAdmission call could never
    // report more than 4.
    const beat = runCli(['--orchestrator-id=orch-desired-agents-solo', '--desired-agents=5'], env);
    expect(beat.status).toBe(0);
    const body = parseStdout(beat.stdout);

    expect(body.type).toBe('spawn-allowed');
    // FAILS TODAY: allowanceConfig is still the flat LIVE_AGENT_CEILING (4).
    expect(body.allowance).toBe(SEEDED_CEILING);
    // FAILS TODAY: reserveAdmission is still called with ceiling: 4, so this
    // beat could never be granted more than 4 regardless of what it asked.
    expect(body.liveAgentGrant).toBe(5);
  });
});

// ---------------------------------------------------------------------------
// Scenario 6 (dedicated) — the advisory allowanceConfig call site, in
// isolation. Per the convergence analysis, this is the single most
// load-bearing test in this phase: it is the 4th call site the Build Plan's
// own text omits, and an implementer following only the Build Plan's literal
// text could land Phases 1-3 with this one site silently left stale.
// ---------------------------------------------------------------------------

describe('the advisory allowanceConfig call site (computePerOrchestratorAllowance) reflects the AIMD ceiling', () => {
  test('a solo orchestrator\'s reported `allowance` equals the seeded AIMD ceiling, not the flat LIVE_AGENT_CEILING', async () => {
    const coordinationFilePath = await freshCoordinationFile('arm-aimd-allowance-');
    const SEEDED_CEILING = 9;
    await writeAimdCeilingState(coordinationFilePath, { ceiling: SEEDED_CEILING, sustainedNormalCount: 0 }, { now: 1_700_000_000_000 });

    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_NORMAL, disk: DISK_GREEN }),
    };

    const beat = runCli(['--orchestrator-id=orch-allowance-solo', '--desired-agents=1'], env);
    expect(beat.status).toBe(0);
    const body = parseStdout(beat.stdout);

    // FAILS TODAY: allowanceConfig.maxAgentsMemoryAxis/maxAgentsDiskAxis are
    // still the flat LIVE_AGENT_CEILING (4), so allowance reads 4 here, not 9.
    //
    // Weaker fallback per this test's own brief: if wiring this 4th call
    // site is judged genuinely infeasible, an explicit accepted-risk code
    // comment must exist at cli.mjs's allowanceConfig object naming why. No
    // such comment exists today (grep of cli.mjs's allowanceConfig block
    // finds none), so the strong assertion below is the operative one.
    expect(body.allowance).toBe(SEEDED_CEILING);
  });
});

// ---------------------------------------------------------------------------
// Scenario 3 — fresh coordination file: all four call sites fall back to the
// default (4), via readAimdCeilingState's lazy cold-start init — not
// three (or four) independently-hardcoded defaults.
// ---------------------------------------------------------------------------

describe('fresh coordination file — all four call sites fall back to the same default (4)', () => {
  test('--claim=live-agent:N cold-starts the persisted AIMD entry at ceiling=4, sustainedNormalCount=0', async () => {
    const coordinationFilePath = await freshCoordinationFile('arm-aimd-coldstart-claim-');
    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_NORMAL, disk: DISK_GREEN }),
    };

    const result = runCli([`--claim=${LIVE_AGENT_CLAIM_TYPE}:5`, '--orchestrator-id=orch-coldstart-claim'], env);
    expect(result.status).toBe(0);
    expect(parseStdout(result.stdout).granted).toBe(FLAT_LIVE_AGENT_CEILING);

    // Distinguishes "genuinely read readAimdCeilingState" from "coincidentally
    // matches because the flat constant is still unwired": if --claim never
    // calls readAimdCeilingState at all, this reserved entry is never
    // created. FAILS TODAY — no such entry exists on any coordination file
    // this branch produces via --claim.
    const entry = readAimdEntryRaw(coordinationFilePath);
    expect(entry).toBeDefined();
    expect(entry.ceiling).toBe(4);
    expect(entry.sustainedNormalCount).toBe(0);
  });

  test('--dequeue-if-capacity cold-starts the persisted AIMD entry at ceiling=4, sustainedNormalCount=0', async () => {
    const coordinationFilePath = await freshCoordinationFile('arm-aimd-coldstart-dequeue-');
    const queueDir = await makeWorkDir('arm-aimd-coldstart-dequeue-queue-');
    workDirs.push(queueDir);
    const queueFilePath = join(queueDir, 'queue.json');

    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_QUEUE_FILE: queueFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_NORMAL, disk: DISK_GREEN }),
    };

    const enqueue = runCli(['--enqueue=implementation-agent:normal:coldstart-command', '--orchestrator-id=orch-seed'], env);
    expect(enqueue.status).toBe(0);

    const result = runCli(['--dequeue-if-capacity', '--orchestrator-id=orch-coldstart-dequeue'], env);
    expect(result.status).toBe(0);
    expect(parseStdout(result.stdout).granted).toBe(true);

    // FAILS TODAY — no such entry exists on any coordination file this
    // branch's --dequeue-if-capacity handler produces.
    const entry = readAimdEntryRaw(coordinationFilePath);
    expect(entry).toBeDefined();
    expect(entry.ceiling).toBe(4);
    expect(entry.sustainedNormalCount).toBe(0);
  });

  test('the --desired-agents beat cold-starts the persisted AIMD entry at ceiling=4, and admits/reports against exactly that default', async () => {
    const coordinationFilePath = await freshCoordinationFile('arm-aimd-coldstart-beat-');
    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_NORMAL, disk: DISK_GREEN }),
    };

    const beat = runCli(['--orchestrator-id=orch-coldstart-beat', '--desired-agents=5'], env);
    expect(beat.status).toBe(0);
    const body = parseStdout(beat.stdout);
    expect(body.allowance).toBe(FLAT_LIVE_AGENT_CEILING);
    expect(body.liveAgentGrant).toBe(FLAT_LIVE_AGENT_CEILING);

    // FAILS TODAY — no such entry exists on any coordination file this
    // branch's --desired-agents beat produces.
    const entry = readAimdEntryRaw(coordinationFilePath);
    expect(entry).toBeDefined();
    expect(entry.ceiling).toBe(4);
  });
});

// ---------------------------------------------------------------------------
// Scenario 4 — WARN pressure refuses admission independent of the AIMD
// ceiling's current value (AC5 / outer-acceptance Part 3's safety property,
// re-proven directly against a freshly-seeded, absurdly-high persisted
// ceiling rather than via a guessed env-config seam).
// ---------------------------------------------------------------------------

describe('WARN pressure refuses admission even when the AIMD ceiling is artificially high', () => {
  test.each([
    ['WARN', 2],
    ['CRITICAL', 3],
  ])('pressureLevel %s (%d) with a persisted ceiling of 999 still refuses admission', async (_label, pressureLevel) => {
    const coordinationFilePath = await freshCoordinationFile('arm-aimd-warn-override-');
    await writeAimdCeilingState(coordinationFilePath, { ceiling: 999, sustainedNormalCount: 0 }, { now: 1_700_000_000_000 });

    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({
        memory: { ...MEMORY_WARN, pressureLevel },
        disk: DISK_GREEN,
      }),
    };

    const beat = runCli(['--orchestrator-id=orch-warn-override', '--desired-agents=1'], env);
    expect(beat.status).toBe(0);
    const body = parseStdout(beat.stdout);

    // This half is EXPECTED TO ALREADY PASS today — the Phase 4
    // pressure-WARN pre-check is unconditional and independent of any
    // ceiling value. Kept here so a future Phase 3 regression (e.g. moving
    // the ceiling read/admission attempt ahead of the pressure pre-check) is
    // caught by THIS test, not silently reintroduced.
    expect(body.type).not.toBe('spawn-allowed');
    expect(body.liveAgentGrant).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Scenario 5 — structural: Phase 3's wiring introduces no NEW shell-out
// surface. Mirrors ./queue-security.jest.spec.mjs's "no exec/spawn surface"
// source-scan idiom. These baselines are captured from THIS branch's cli.mjs
// today (Phase 1/2 landed, Phase 3 not yet wired) and pinned as a ceiling —
// legitimately green today (nothing has changed yet), and a regression guard
// once Phase 3's implementer wires the four call sites: the ceiling read
// must reuse Phase 1's readAimdCeilingState (file/lock I/O only) and the
// beat's EXISTING pressure sample, never a fresh runProbe/execFileSync call.
// ---------------------------------------------------------------------------

describe('no new runProbe/execFileSync/spawnSync call site is introduced by wiring the AIMD ceiling', () => {
  test('cli.mjs\'s execFileSync/runProbe/spawnSync call-site counts are unchanged from the pre-Phase-3 baseline', () => {
    const source = readFileSync(CLI, 'utf8');

    // Baseline, counted directly off this branch's cli.mjs before Phase 3:
    //   execFileSync( -> 2 (inside runProbe, and the ps discovery call)
    //   runProbe(     -> 5 (1 definition + 4 call sites: vm_stat, sysctl
    //                        swapusage, df, sysctl pressure level)
    //   spawnSync(    -> 0 (cli.mjs never spawns a child of its OWN accord —
    //                        see the file's own header comment)
    const execFileSyncCalls = (source.match(/execFileSync\(/g) ?? []).length;
    const runProbeReferences = (source.match(/runProbe\(/g) ?? []).length;
    const spawnSyncCalls = (source.match(/spawnSync\(/g) ?? []).length;

    expect({ execFileSyncCalls, runProbeReferences, spawnSyncCalls }).toEqual({
      execFileSyncCalls: 2,
      runProbeReferences: 5,
      spawnSyncCalls: 0,
    });
  });
});

// ---------------------------------------------------------------------------
// Scenario 2 — the `--claim` pre-lock probe's ceiling read shares the SAME
// bounded-staleness window as the existing pressure-sample staleness check
// (design decision A: reuse DEFAULT_CLAIM_SAMPLE_FRESHNESS_MS's mechanism,
// never a second, independently-invented one).
//
// Genuinely staling the sample by real elapsed wall-clock time inside a
// SINGLE `--claim` invocation is not achievable black-box: `resolveNow()`
// reads a frozen `ARM_FAKE_NOW_MS` when set (so the stamp and the in-lock
// comparison never drift), or the real clock when unset (so the whole
// invocation completes in milliseconds, nowhere near
// DEFAULT_CLAIM_SAMPLE_FRESHNESS_MS's 20s window) — the SAME reason
// claim.jest.spec.mjs's own "the THIRD denial source" comment gives for not
// exercising this arm black-box either. These two tests instead pin (a) the
// ceiling participates in the SAME frozen-clock seam as the pressure sample
// (so a frozen clock never spuriously stales a seeded, non-default ceiling),
// and (b) no second, differently-named freshness mechanism is introduced for
// the ceiling specifically.
// ---------------------------------------------------------------------------

describe('the --claim ceiling read shares the pressure sample\'s bounded-staleness window (design decision A)', () => {
  test('ARM_FAKE_NOW_MS drives both the ceiling read and the pressure-sample freshness comparison — a frozen clock never spuriously stales a seeded, non-default ceiling', async () => {
    const coordinationFilePath = await freshCoordinationFile('arm-aimd-claim-frozen-clock-');
    const FAR_PAST = 1_600_000_000_000;
    await writeAimdCeilingState(coordinationFilePath, { ceiling: 6, sustainedNormalCount: 0 }, { now: FAR_PAST });

    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_NOW_MS: String(FAR_PAST),
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_NORMAL, disk: DISK_GREEN }),
    };

    const result = runCli([`--claim=${LIVE_AGENT_CLAIM_TYPE}:6`, '--orchestrator-id=orch-frozen-clock'], env);
    expect(result.status).toBe(0);
    expect(result.stderr).toBe('');
    // If the ceiling read were staleness-gated by a DIFFERENT, real-wall-clock
    // seam than resolveNow()/ARM_FAKE_NOW_MS, a frozen FAR_PAST timestamp
    // would read as permanently stale and this grant would silently fall
    // back to 0 (mirroring the EXISTING sample-staleness "frozen clock is
    // never spuriously stale" regression this codebase already guards for
    // the pressure sample — claim.jest.spec.mjs's own test of that name).
    // FAILS TODAY regardless, since the ceiling isn't wired at all yet — but
    // once wired, this specifically catches a second, wrongly-real-clock-
    // bound staleness mechanism, not merely "the ceiling isn't read".
    expect(parseStdout(result.stdout).granted).toBe(6);
  });

  test('no second, independently-named freshness constant governs the ceiling read', () => {
    const source = readFileSync(CLI, 'utf8');
    // Every `_FRESHNESS_MS`-suffixed exported constant in cli.mjs today.
    // Baseline captured off THIS branch pre-Phase-3: only
    // DEFAULT_CLAIM_SAMPLE_FRESHNESS_MS. A Phase 3 implementer who invents a
    // SEPARATE ceiling-freshness constant (e.g. `DEFAULT_CEILING_SAMPLE_
    // FRESHNESS_MS`) rather than reusing the existing one changes this count
    // — legitimately green today (nothing added yet), a regression guard
    // once Phase 3 lands.
    const freshnessConstants = (source.match(/export const \w*FRESHNESS_MS\w* =/g) ?? []).map((line) =>
      line.replace('export const ', '').replace(' =', ''),
    );
    expect(freshnessConstants).toEqual(['DEFAULT_CLAIM_SAMPLE_FRESHNESS_MS']);
  });
});

// ---------------------------------------------------------------------------
// Scenario 8 — mixed-version-fleet risk: does an OLD orchestrator hardcoding
// `ceiling: LIVE_AGENT_CEILING` (the literal 4) risk out-admitting a NEW
// orchestrator's AIMD-DECREASED ceiling?
//
// Traced directly against `reserveAdmission` (./lib/coordination-file.mjs,
// already shipped in Phase 1): Step 1 computes
// `liveAgentCeilingRemaining = Math.max(0, ceilingSafe -
// alreadyGrantedTotalForSnapshotType)`, where `ceilingSafe` is taken
// VERBATIM from THIS CALL's own `ceiling` parameter — `reserveAdmission`
// never itself calls `readAimdCeilingState` or cross-checks its caller's
// `ceiling` against whatever is actually currently persisted. This IS a
// real, currently-unclosed gap, not a false alarm: a caller that passes a
// stale (too-permissive) `ceiling` is granted admission computed against
// ITS OWN stale view, regardless of what a concurrently-running, correctly-
// updated caller has persisted.
//
// Recorded here as documentation of a KNOWN, CURRENTLY ACCEPTED risk (per
// this ticket's own instructions) rather than silently having zero coverage
// of the scenario — candidate for a Phase 5 ADR / follow-up ticket, not
// something Phase 3's wiring is expected to close (Phase 3 only wires
// cli.mjs's OWN four call sites to read the current value; it cannot control
// what an out-of-date orchestrator binary on the same host passes in).
//
// This test runs directly against the ALREADY-SHIPPED `reserveAdmission` —
// it does not depend on Phase 3's cli.mjs wiring landing, and is legitimately
// green today.
// ---------------------------------------------------------------------------

describe('[accepted risk, ] mixed-version fleet: reserveAdmission trusts its OWN caller\'s ceiling param, not the currently-persisted AIMD value', () => {
  test('an "old" orchestrator hardcoding ceiling=4 is granted admission computed against 4, even while a "new" orchestrator has already persisted a lower, post-decrease ceiling of 2', async () => {
    const coordinationFilePath = await freshCoordinationFile('arm-aimd-mixed-fleet-');
    const now = 1_700_000_000_000;

    // The "new" (Phase-3-aware) orchestrator has already observed WARN
    // pressure and persisted a decreased ceiling.
    await writeAimdCeilingState(coordinationFilePath, { ceiling: 2, sustainedNormalCount: 0 }, { now });

    // The "old" (pre-existing) orchestrator never reads that persisted state at
    // all — it calls reserveAdmission exactly the way today's shipped
    // cli.mjs does, with the literal flat constant.
    const result = await reserveAdmission(
      coordinationFilePath,
      {
        snapshotType: LIVE_AGENT_CLAIM_TYPE,
        admissionType: LIVE_AGENT_ADMISSION_CLAIM_TYPE,
        requestedCount: 4,
        orchestratorId: 'orch-old-fleet-member',
        ceiling: FLAT_LIVE_AGENT_CEILING, // the accepted-risk gap: NOT read from readAimdCeilingState
        now,
      },
      { claimTtlMs: 60_000 },
    );

    // The old orchestrator is granted up to ITS OWN (stale, too-permissive)
    // ceiling of 4 — TWO more than the true, currently-persisted ceiling of
    // 2 permits. This is the gap, pinned exactly as it exists today: not a
    // hypothetical, not something this test expects to fail once Phase 3
    // lands (Phase 3 cannot retroactively fix an out-of-date binary), and
    // not silently unasserted.
    expect(result.granted).toBe(4);
    expect(result.liveAgentCeilingRemaining).toBe(4);

    // The true, currently-persisted AIMD ceiling is unaffected by this call
    // — reserveAdmission never writes back to the AIMD-ceiling entry, only
    // to the live-agent/live-agent-admission ledgers — so a Phase-3-aware
    // caller reading readAimdCeilingState next would still (correctly) see 2.
    const state = await readAimdCeilingState(coordinationFilePath, { now });
    expect(state.ceiling).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Review finding (Phase 3 fix) — `--dequeue-if-capacity`'s AIMD ceiling read
// must fail closed through THIS handler's own tailored message + exit(2)
// contract, not escape uncaught to the generic top-level `main().catch()`.
// Forced here by pointing ARM_COORDINATION_FILE at a DIRECTORY, not a file —
// `readAimdCeilingState`'s underlying file read then throws (EISDIR), which
// is exactly the "coordination-file read blows up" shape this handler must
// contain, not merely "some code path returns non-zero".
// ---------------------------------------------------------------------------

describe('--dequeue-if-capacity fails closed through its own handler when the AIMD ceiling read throws', () => {
  test('a coordination-file read failure surfaces this handler\'s own "--dequeue-if-capacity failed" message and exit(2), not the generic top-level catch', async () => {
    const workDir = await makeWorkDir('arm-aimd-dequeue-fail-closed-');
    workDirs.push(workDir);
    const coordinationFilePath = join(workDir, 'coordination.json');
    // A directory at the coordination-file path makes any read of it throw
    // (EISDIR) — simulating the kind of coordination-file I/O failure
    // (lock-acquisition exhaustion, disk error) this handler must contain.
    mkdirSync(coordinationFilePath);

    const queueDir = await makeWorkDir('arm-aimd-dequeue-fail-closed-queue-');
    workDirs.push(queueDir);
    const queueFilePath = join(queueDir, 'queue.json');

    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_QUEUE_FILE: queueFilePath,
    };

    const result = runCli(['--dequeue-if-capacity', '--orchestrator-id=orch-fail-closed'], env);

    // The handler's OWN fail-closed contract, not the generic top-level
    // `main().catch()` shape ("agent-resource-management: unexpected error").
    expect(result.status).toBe(2);
    expect(result.stderr).toMatch(/--dequeue-if-capacity failed/);
    expect(result.stderr).not.toMatch(/unexpected error/);
    expect(result.stdout).toBe('');
  });
});

// ---------------------------------------------------------------------------
// Review finding (Phase 3 fix) — heartbeat beats must REPORT the actual
// persisted AIMD ceiling (via the read-only `readAimdCeilingState`), not the
// flat `LIVE_AGENT_CEILING` literal, while still never ADVANCING the AIMD
// state machine (`sustainedNormalCount`/`ceiling` must be left exactly as
// found by a heartbeat).
// ---------------------------------------------------------------------------

describe('a --heartbeat beat reports the real persisted AIMD ceiling, without advancing it', () => {
  test('--heartbeat reports the seeded, non-default ceiling in `allowance`, not the flat LIVE_AGENT_CEILING', async () => {
    const coordinationFilePath = await freshCoordinationFile('arm-aimd-heartbeat-report-');
    const SEEDED_CEILING = 7;
    const now = 1_700_000_000_000;
    await writeAimdCeilingState(coordinationFilePath, { ceiling: SEEDED_CEILING, sustainedNormalCount: 0 }, { now });

    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_NOW_MS: String(now),
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_NORMAL, disk: DISK_GREEN }),
    };

    // `computePerOrchestratorAllowance` treats a co-resident dibs entry with
    // no prior REAL declared spawn intent (`desiredAgents === 0`, a
    // heartbeat's own default) as a non-competitor and reports `allowance: 0`
    // regardless of the ceiling — by design (SKILL.md: heartbeats never add
    // contention). To observe the ceiling actually feeding `allowance` (this
    // fix's own concern) a real `--desired-agents` beat must first establish
    // this orchestrator's declared demand, exactly as a real orchestrator's
    // own poll loop would (declare intent once, heartbeat in between).
    const declare = runCli(['--orchestrator-id=orch-heartbeat-report', '--desired-agents=1'], env);
    expect(declare.status).toBe(0);
    expect(parseStdout(declare.stdout).allowance).toBe(SEEDED_CEILING);

    // The real (non-heartbeat) declare beat above legitimately advances the
    // AIMD state machine under sustained NORMAL pressure — capture its
    // resulting `sustainedNormalCount` as the baseline the heartbeat below
    // must leave untouched, rather than assuming it stayed at the initial
    // seed of 0.
    const stateAfterDeclare = await readAimdCeilingState(coordinationFilePath, { now });

    const beat = runCli(['--orchestrator-id=orch-heartbeat-report', '--heartbeat'], env);
    expect(beat.status).toBe(0);
    const body = parseStdout(beat.stdout);

    expect(body.allowance).toBe(SEEDED_CEILING);

    // The heartbeat must not have mutated the persisted AIMD state any
    // further than the preceding real beat already had.
    const state = await readAimdCeilingState(coordinationFilePath, { now });
    expect(state.ceiling).toBe(SEEDED_CEILING);
    expect(state.sustainedNormalCount).toBe(stateAfterDeclare.sustainedNormalCount);
  });

  test('repeated --heartbeat beats never advance sustainedNormalCount/ceiling, even under sustained NORMAL pressure', async () => {
    const coordinationFilePath = await freshCoordinationFile('arm-aimd-heartbeat-no-advance-');
    const now = 1_700_000_000_000;
    await writeAimdCeilingState(coordinationFilePath, { ceiling: 4, sustainedNormalCount: 0 }, { now });

    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_NOW_MS: String(now),
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_NORMAL, disk: DISK_GREEN }),
    };

    for (let i = 0; i < 3; i += 1) {
      const beat = runCli(['--orchestrator-id=orch-heartbeat-no-advance', '--heartbeat'], env);
      expect(beat.status).toBe(0);
    }

    const state = await readAimdCeilingState(coordinationFilePath, { now });
    expect(state.ceiling).toBe(4);
    expect(state.sustainedNormalCount).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Review finding (High) — a real (non-heartbeat) beat whose `pressureLevel`
// reading comes back stale/unknown (`null`, mirroring
// ./lib/recon.mjs's `staleMemoryMarker()`, which sets `pressureLevel: null`
// specifically so "downstream classifiers can't mistake it for a real
// sample") must NOT advance the AIMD ceiling state machine. Before the fix,
// `computeAimdCeiling`'s `pressureLevel >= 2` check was FALSE for `null` —
// the exact same falsy outcome a confirmed-Normal `pressureLevel: 1` beat
// produces — so a stale reading was silently counted toward the additive
// increase's `sustainedNormalCount`, growing the shared, persisted ceiling
// on nothing but "absence of evidence of WARN". This is the same asymmetric-
// safety property Scenario 4 above proves for the WARN/CRITICAL admission
// check; this test proves it for the AIMD ceiling ADVANCE path specifically.
// ---------------------------------------------------------------------------

describe('a real beat with a stale/unknown pressureLevel reading never advances the AIMD ceiling', () => {
  test('5 consecutive beats with pressureLevel: null leave ceiling/sustainedNormalCount exactly as seeded', async () => {
    const coordinationFilePath = await freshCoordinationFile('arm-aimd-stale-pressure-no-advance-');
    const now = 1_700_000_000_000;
    await writeAimdCeilingState(coordinationFilePath, { ceiling: 4, sustainedNormalCount: 0 }, { now });

    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_NOW_MS: String(now),
      ARM_BEAT_NOW_TEST_MODE: '1',
      // Mirrors staleMemoryMarker()'s own shape: pressureLevel: null, while
      // swapUsedMb/compressedMb/disk stay well-formed so this exercises the
      // AIMD-advance gate specifically, not a broader malformed-sample path.
      ARM_FAKE_COLLECT_JSON: JSON.stringify({
        memory: { pressureLevel: null, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 },
        disk: DISK_GREEN,
      }),
    };

    // A stale/unknown pressureLevel fails OPEN on the WARN/CRITICAL check
    // alone (documented at cli.mjs's pressureLevel read) — these beats are
    // never refused ON THAT CHECK. `body.type` may still legitimately read
    // something other than 'spawn-allowed' for unrelated reasons (e.g. the
    // global spawn-rate bucket across 5 rapid-fire beats), so this test
    // asserts only what the fix is actually about: the persisted AIMD state.
    for (let i = 0; i < 5; i += 1) {
      const beat = runCli(['--orchestrator-id=orch-stale-pressure-no-advance', `--desired-agents=1`], env);
      expect(beat.status).toBe(0);
    }

    const state = await readAimdCeilingState(coordinationFilePath, { now });
    expect(state.ceiling).toBe(4);
    expect(state.sustainedNormalCount).toBe(0);
  });
});
