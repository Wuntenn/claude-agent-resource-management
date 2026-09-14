// Phase 3, third review round — the two `--advise-only` fidelity/cost
// defects, pinned as regressions.
//
// Medium #1 (fidelity): the memory-projection axis silently ignored the gated
// session's OWN already-running agents. An advisory caller (the PreToolUse
// gate) passes neither `--agent-class` nor `--running-agent-classes` — it
// consults on behalf of a session it does not manage and cannot know either
// value. `decideMemoryProjection` excludes the self dibs entry from
// `liveDibs` and substitutes `ownRunningAgentClasses` for it, so the empty
// list priced this session's entire running fleet at ZERO and turned a
// genuine `memory-projection-block` into `spawn-allowed` — failing OPEN on
// the one axis the gate exists to enforce.
//
// Medium #2 (cost): memory-RED candidate selection — a SECOND `ps` collection
// plus a per-running-agent `/usr/bin/footprint` (-> `/usr/bin/vmmap`)
// shell-out chain, each bounded at 2s and effectively serialized — ran on
// `memoryResult.state === 'RED'` alone, with no `!isAdviseOnly` guard, unlike
// every other expensive/side-effecting step on that path. On a memory-RED
// host with a real fleet that can exceed the hook's 5s budget and manufacture
// a fail-closed deny out of a state the advisory path already answers.
//
// Black-box throughout: spawns the real `cli.mjs`, never imports it — the
// idiom every sibling file here uses (e.g. ./cli-memory-projection.jest.spec.mjs).

import { spawnSync } from 'node:child_process';
import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 };
const MEMORY_RED = { pressureLevel: 1, swapUsedMb: 12288, compressedMb: 4096 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

const COLLECT_GREEN = JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN });
const COLLECT_RED = JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN });

// Two DISTINCT heavy classes, each priced at ~6 GB, so the pair alone
// (12 GB) plus one candidate priced at the same figure overshoots the
// 16384 - 2048 = 14336 MB budget, while EITHER class alone plus a candidate
// stays comfortably inside it. That asymmetry is what makes the assertions
// below attributable to the self fleet specifically.
const HEAVY_A = 'heavy-a';
const HEAVY_B = 'heavy-b';
const HEAVY_PEAKS_MB = [6000, 6000, 6000];

/** A ps table in which each pid is its own claude-rooted tree. */
function fakePsOutput(pids) {
  return ['  PID  PPID    RSS ELAPSED COMM', ...pids.map((pid) => `${pid} 1 400000 05:00 /usr/local/bin/claude`)].join(
    '\n',
  );
}

let workDir;
let coordinationFilePath;
let historyFilePath;
let psCollectionLogPath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-advise-only-fidelity-'));
  coordinationFilePath = join(workDir, 'coordination.json');
  historyFilePath = join(workDir, 'history.json');
  psCollectionLogPath = join(workDir, 'ps-collections.log');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

function runCli(args, extraEnv = {}) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

function baseEnv(extra = {}) {
  return {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_HISTORY_FILE: historyFilePath,
    ARM_FAKE_COLLECT_JSON: COLLECT_GREEN,
    ...extra,
  };
}

/** Seeds cost history for `agentClass` through the real `--record-outcome` beat. */
function seedHistory(agentClass, peaks) {
  peaks.forEach((peakMemoryMb, index) => {
    const { status } = runCli(
      [
        '--record-outcome',
        `--operation-type=${agentClass}`,
        `--orchestrator-id=orch-seed-${agentClass}-${index}`,
        `--peak-memory-mb=${peakMemoryMb}`,
      ],
      { ARM_HISTORY_FILE: historyFilePath },
    );
    expect(status).toBe(0);
  });
}

/**
 * Declares a live self entry the honest way — a REAL beat, not a hand-written
 * fixture — carrying two concurrently-running heavy classes.
 */
function declareHeavyFleet(orchestratorId) {
  const { status, stdout } = runCli(
    [
      `--orchestrator-id=${orchestratorId}`,
      '--desired-agents=1',
      `--agent-class=${HEAVY_A}`,
      `--running-agent-classes=${HEAVY_B}`,
    ],
    baseEnv(),
  );
  expect(status).toBe(0);
  return JSON.parse(stdout);
}

async function readEntries() {
  return JSON.parse(await readFile(coordinationFilePath, 'utf8'));
}

async function psCollectionCount() {
  try {
    return (await readFile(psCollectionLogPath, 'utf8')).split('\n').filter(Boolean).length;
  } catch {
    return 0;
  }
}

describe('Medium #1 — the advisory memory projection prices the session\'s OWN running fleet', () => {
  const ORCHESTRATOR_ID = 'orch-advisory-self-fleet';

  beforeEach(() => {
    seedHistory(HEAVY_A, HEAVY_PEAKS_MB);
    seedHistory(HEAVY_B, HEAVY_PEAKS_MB);
  });

  test('blocks with NO class flags at all — the shape the PreToolUse gate actually consults in', async () => {
    // The declaring beat itself is admitted: one running heavy class plus one
    // candidate is ~12 GB, inside the 14336 MB budget. The fleet only
    // overshoots once BOTH declared classes are counted.
    expect(declareHeavyFleet(ORCHESTRATOR_ID).type).not.toBe('memory-projection-block');
    expect(
      (await readEntries()).find((entry) => entry.orchestratorId === ORCHESTRATOR_ID).agentClasses.sort(),
    ).toEqual([HEAVY_A, HEAVY_B]);

    const { status, stdout } = runCli([`--orchestrator-id=${ORCHESTRATOR_ID}`, '--advise-only'], baseEnv());

    expect(status).toBe(0);
    const verdict = JSON.parse(stdout);
    // Pre-fix this was `spawn-allowed` with a non-zero allowance: the self
    // entry was excluded from `liveDibs` and replaced by an empty
    // `ownRunningAgentClasses`, pricing the whole running fleet at zero.
    expect(verdict.type).toBe('memory-projection-block');
    // 6000 (heavy-a) + 6000 (heavy-b) + 6000 (the unlabelled candidate,
    // priced at the most expensive known class) > 14336.
    expect(verdict.totalProjectedMemoryMb).toBe(18000);
    expect(verdict.budgetMemoryMb).toBe(14336);
  });

  test('agrees with the same beat given the class flags explicitly — the fallback is not a different answer', () => {
    declareHeavyFleet(ORCHESTRATOR_ID);

    const implicit = JSON.parse(runCli([`--orchestrator-id=${ORCHESTRATOR_ID}`, '--advise-only'], baseEnv()).stdout);
    const explicit = JSON.parse(
      runCli(
        [`--orchestrator-id=${ORCHESTRATOR_ID}`, '--advise-only', `--running-agent-classes=${HEAVY_A},${HEAVY_B}`],
        baseEnv(),
      ).stdout,
    );

    expect(implicit.type).toBe(explicit.type);
    expect(implicit.totalProjectedMemoryMb).toBe(explicit.totalProjectedMemoryMb);
  });

  test('an EXPLICIT --running-agent-classes still wins over the self entry, including the empty form', () => {
    declareHeavyFleet(ORCHESTRATOR_ID);

    // Explicitly "nothing else of mine is running" — the caller's own
    // statement, not an omission — must NOT be overridden by the ledger.
    const { status, stdout } = runCli(
      [`--orchestrator-id=${ORCHESTRATOR_ID}`, '--advise-only', '--running-agent-classes='],
      baseEnv(),
    );

    expect(status).toBe(0);
    expect(JSON.parse(stdout).type).toBe('spawn-allowed');
  });

  test('a session with no live entry of its own is unaffected — nothing to fall back to', () => {
    declareHeavyFleet(ORCHESTRATOR_ID);

    // A DIFFERENT session: the heavy fleet belongs to someone else's entry,
    // which is summed as an "other orchestrator" (6000 + 6000 + candidate),
    // and its own hypothetical stand-in contributes no running classes.
    const { status, stdout } = runCli(['--orchestrator-id=orch-someone-else', '--advise-only'], baseEnv());

    expect(status).toBe(0);
    // Same arithmetic, reached by the other-orchestrator path — proving the
    // fallback added coverage rather than double-counting anyone.
    expect(JSON.parse(stdout).totalProjectedMemoryMb).toBe(18000);
  });

  test('does not leak into a real beat — a NON-advisory beat still ignores the persisted self classes', async () => {
    declareHeavyFleet(ORCHESTRATOR_ID);

    // The same orchestrator's next REAL beat, declaring only ONE running
    // class: the ledger says two, but a real beat's authority is its own
    // flags (full-replace-on-upsert), so it is admitted and its entry is
    // rewritten to exactly what it declared.
    const { status, stdout } = runCli(
      [`--orchestrator-id=${ORCHESTRATOR_ID}`, '--desired-agents=1', `--agent-class=${HEAVY_A}`],
      baseEnv(),
    );

    expect(status).toBe(0);
    expect(JSON.parse(stdout).type).not.toBe('memory-projection-block');
    expect((await readEntries()).find((entry) => entry.orchestratorId === ORCHESTRATOR_ID).agentClasses).toEqual([
      HEAVY_A,
    ]);
  });

  test('writes nothing — the block is still reached with the coordination file byte-identical', async () => {
    declareHeavyFleet(ORCHESTRATOR_ID);
    const before = await readFile(coordinationFilePath, 'utf8');

    expect(JSON.parse(runCli([`--orchestrator-id=${ORCHESTRATOR_ID}`, '--advise-only'], baseEnv()).stdout).type).toBe(
      'memory-projection-block',
    );

    expect(await readFile(coordinationFilePath, 'utf8')).toBe(before);
  });
});

describe('Medium #2 — memory-RED candidate selection is skipped on the advisory path', () => {
  const ORCHESTRATOR_ID = 'orch-advisory-red';
  const AGENT_PIDS = [910001, 910002, 910003];

  /** Env that makes the RED path fully hermetic AND counts every `ps` collection. */
  function redEnv() {
    return baseEnv({
      ARM_FAKE_COLLECT_JSON: COLLECT_RED,
      ARM_FAKE_PS_OUTPUT: fakePsOutput(AGENT_PIDS),
      ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 4096 MB',
      // Master switch for the `ps`-collection counter (see cli.mjs's
      // `recordProcessSnapshotCollection`).
      ARM_EVICT_TEST_MODE: '1',
      ARM_FAKE_PS_COLLECTION_LOG: psCollectionLogPath,
    });
  }

  beforeEach(() => {
    // A live self entry, declared the honest way, so both runs below are
    // reasoning about the same host state.
    expect(runCli([`--orchestrator-id=${ORCHESTRATOR_ID}`, '--desired-agents=1'], baseEnv()).status).toBe(0);
  });

  test('an advisory memory-RED verdict is still `pause`, but names no candidate and collects no `ps` snapshot', async () => {
    const { status, stdout } = runCli([`--orchestrator-id=${ORCHESTRATOR_ID}`, '--advise-only'], redEnv());

    expect(status).toBe(0);
    const verdict = JSON.parse(stdout);
    // The verdict TYPE is unchanged — the hook reads only `type`/`allowance`/
    // `liveAgentGrant`, and `pause` is a policy deny either way.
    expect(verdict.type).toBe('pause');
    // `buildTrafficLight`'s own documented "nothing to pause" shape.
    expect(verdict.pauseCandidate).toBeNull();
    // The direct cost proof: the whole chain starts with a `ps` collection,
    // and the advisory path takes none at all (it also skips the beat
    // snapshot).
    expect(await psCollectionCount()).toBe(0);
  });

  test('a REAL memory-RED beat is unchanged — it still collects `ps` and names a real candidate', async () => {
    const { status, stdout } = runCli([`--orchestrator-id=${ORCHESTRATOR_ID}`, '--desired-agents=1'], redEnv());

    expect(status).toBe(0);
    const verdict = JSON.parse(stdout);
    expect(verdict.type).toBe('pause');
    expect(verdict.pauseCandidate).not.toBeNull();
    expect(AGENT_PIDS.map(String)).toContain(verdict.pauseCandidate.agentId);
    expect(verdict.pauseCandidate.physFootprintMb).toBe(4096);
    expect(await psCollectionCount()).toBeGreaterThan(0);
  });

  test('an advisory memory-RED consultation is fast — the multi-second shell-out chain is genuinely gone', async () => {
    // Deliberately generous relative to the ~50ms this actually takes, and
    // still far below the hook's 5s `CLI_TIMEOUT_MS`: this pins the ABSENCE
    // of a per-agent, 2s-bounded, serialized shell-out chain rather than a
    // machine-speed figure.
    const startedAt = Date.now();
    const { status } = runCli([`--orchestrator-id=${ORCHESTRATOR_ID}`, '--advise-only'], redEnv());
    const elapsedMs = Date.now() - startedAt;

    expect(status).toBe(0);
    expect(elapsedMs).toBeLessThan(2_000);
  });

  test('a memory-GREEN advisory beat never had a candidate to skip — no behaviour change there', async () => {
    const { stdout } = runCli(
      [`--orchestrator-id=${ORCHESTRATOR_ID}`, '--advise-only'],
      baseEnv({ ARM_EVICT_TEST_MODE: '1', ARM_FAKE_PS_COLLECTION_LOG: psCollectionLogPath }),
    );

    expect(JSON.parse(stdout).type).not.toBe('pause');
    expect(await psCollectionCount()).toBe(0);
  });
});

describe('the self-fleet fallback tolerates a hostile or legacy ledger row', () => {
  const ORCHESTRATOR_ID = 'orch-legacy-row';

  test('reads a LEGACY singular `agentClass` entry through the same back-compat seam', async () => {
    seedHistory(HEAVY_A, [12000, 12000, 12000]);

    const now = Date.now();
    await writeFile(
      coordinationFilePath,
      JSON.stringify([
        // Pre- shape: a singular `agentClass` string, no `agentClasses`.
        { orchestratorId: ORCHESTRATOR_ID, desiredAgents: 1, declaredAt: now, firstDeclaredAt: now, agentClass: HEAVY_A },
      ]),
      'utf8',
    );

    const { status, stdout } = runCli([`--orchestrator-id=${ORCHESTRATOR_ID}`, '--advise-only'], baseEnv());

    expect(status).toBe(0);
    expect(JSON.parse(stdout).type).toBe('memory-projection-block');
  });

  test('a malformed `agentClasses` field degrades to no fallback rather than crashing the beat', async () => {
    const now = Date.now();
    await writeFile(
      coordinationFilePath,
      JSON.stringify([
        {
          orchestratorId: ORCHESTRATOR_ID,
          desiredAgents: 1,
          declaredAt: now,
          firstDeclaredAt: now,
          agentClasses: 'not-an-array',
        },
      ]),
      'utf8',
    );

    const { status, stdout } = runCli([`--orchestrator-id=${ORCHESTRATOR_ID}`, '--advise-only'], baseEnv());

    expect(status).toBe(0);
    expect(typeof JSON.parse(stdout).type).toBe('string');
  });
});

// Phase 6 review (Medium) — `--advise-only`'s non-mutation guarantee was
// enforced against `--heartbeat` ONLY. Both the flag's validation and that one
// conflict check live inside the `--desired-agents` handler, which every other
// beat-type flag dispatches ahead of — so `--advise-only` combined with any
// other beat type was silently accepted and ran that beat's normal, MUTATING
// handler. The first test below is the reviewer's verbatim live repro.
describe('--advise-only rejects every other beat-type flag rather than silently mutating', () => {
  const SESSION_ID = '11111111-2222-3333-4444-555555555555';

  async function historyFileExists() {
    try {
      await readFile(historyFilePath, 'utf8');
      return true;
    } catch {
      return false;
    }
  }

  test("the reviewer's repro: `--advise-only --record-outcome` rejects and writes no history", async () => {
    const { status, stdout, stderr } = runCli(
      [
        `--session-id=${SESSION_ID}`,
        '--advise-only',
        '--record-outcome',
        '--operation-type=test',
        '--peak-memory-mb=100',
      ],
      baseEnv(),
    );

    expect(status).toBe(2);
    expect(stdout).toBe('');
    expect(stderr).toContain('--advise-only cannot be combined with --record-outcome');
    expect(await historyFileExists()).toBe(false);
  });

  test('`--advise-only --claim` rejects and takes no live-agent admission', async () => {
    const { status, stderr } = runCli(
      [`--orchestrator-id=orch-advise-claim`, '--advise-only', '--claim', '--count=1'],
      baseEnv(),
    );

    expect(status).toBe(2);
    expect(stderr).toContain('--advise-only cannot be combined with --claim');
    await expect(readFile(coordinationFilePath, 'utf8')).rejects.toThrow();
  });

  test.each([
    ['--watchdog-beat', ['--watchdog-beat']],
    ['--evict-pid', ['--evict-pid=12345']],
    ['--enqueue', ['--enqueue=task-a']],
    ['--dequeue', ['--dequeue']],
    ['--release', ['--release']],
    ['--query-capacity', ['--query-capacity']],
    ['--poll-footprint', ['--poll-footprint']],
    ['--heartbeat', ['--heartbeat']],
  ])('`--advise-only %s` is rejected', (flagLabel, args) => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-advise-conflict', '--advise-only', ...args],
      baseEnv(),
    );

    expect(status).toBe(2);
    expect(stdout).toBe('');
    expect(stderr).toContain(`--advise-only cannot be combined with ${flagLabel}`);
  });

  test('a bare `--advise-only` beat is unaffected by the new rejection list', () => {
    const { status, stdout } = runCli(['--orchestrator-id=orch-advise-clean', '--advise-only'], baseEnv());

    expect(status).toBe(0);
    expect(typeof JSON.parse(stdout).type).toBe('string');
  });
});
