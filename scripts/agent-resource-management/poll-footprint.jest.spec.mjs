// Phase 3 — `--poll-footprint` CLI beat tests.
//
// RED by construction: `--poll-footprint` does not exist yet in cli.mjs (only
// Phase 1's `lib/footprint-sampler.mjs` and Phase 2's
// `lib/footprint-trajectory.mjs` have landed). These are black-box,
// real-process-spawning tests — mirroring the established idiom in
// ./cli.jest.spec.mjs and ./record-outcome.jest.spec.mjs — never importing
// cli.mjs directly, and never asserting on any NEW internal module's shape
// (per the Build Plan's own preference for the CLI black-box surface over
// forcing a specific internal persisted-state shape on the implementer).
//
// ---------------------------------------------------------------------------
// CLI contract this file tests AGAINST (for the builder to match in
// cli.mjs) — a design proposal, not settled; the implementer may adjust the
// exact env var / flag names below as long as the OBSERVABLE behaviour these
// tests pin (exit codes, stderr shape, conflicting-flag rejection, and the
// downstream --record-outcome duration-threading effect) holds:
//
//   node cli.mjs --poll-footprint=<pid> --orchestrator-id=<id>
//                --operation-type=<type> [--agent-class=<label>]
//
//   ARM_FOOTPRINT_STATE_FILE   (env, required for test hermeticity)
//     Mirrors ARM_HISTORY_FILE's / ARM_QUEUE_FILE's existing "env override
//     falls back to a real `.claude/agent-state/` production default when
//     unset" convention. Absolute path to wherever `--poll-footprint`
//     persists its per-PID EWMA-smoothing/trajectory state across separate
//     one-shot invocations. This suite never reads or parses that file
//     directly — every assertion goes through --poll-footprint's own stdout
//     confirmation JSON, or (for the duration-threading tests) through
//     --record-outcome's own effect, verified via the pre-existing, already
//     -public `readHistory` (./lib/history.mjs).
//
//   ARM_FAKE_FOOTPRINT_OUTPUT   mirrors lib/footprint-sampler.mjs's own test
//     seam exactly (see that module + ./footprint-outer-acceptance.jest.spec.mjs):
//     when set, --poll-footprint's underlying sampleFootprint(pid) call
//     resolves against that fixed text instead of shelling out for real.
//
//   --poll-footprint=<pid>   (one-shot: samples the given PID's footprint
//     ONCE per invocation, folds it into persisted per-PID smoothing state,
//     prints a minimal confirmation JSON to stdout —
//     `{ "polled": true, "pid": <pid>, "orchestratorId": <id>,
//        "operationType": <type> }` — exit 0. NEVER an internal loop/daemon;
//     an external orchestrator re-invokes this every 10-30s per live PID.
//
//   --orchestrator-id=<id> / --operation-type=<type>   (both required)
//     Missing/absent: reject before any file I/O, exit 2, clear stderr
//     message naming --poll-footprint specifically — mirrors
//     --record-outcome's own required-flag validation precedent exactly
//     (see ./record-outcome.jest.spec.mjs).
//
//   --agent-class=<label>   (optional passthrough)
//     A missing/invalid value degrades to undefined (unlabelled) — never
//     crashes this beat.
//
//   Combined with --desired-agents or --heartbeat in the same invocation:
//     Rejected outright, exit 2, clear stderr — mirrors --record-outcome's
//     own conflicting-flag guard exactly (same message shape: "--poll-
//     footprint cannot be combined with <flag> in the same invocation —
//     polling a footprint is a standalone one-shot beat type, not a
//     spawn-decision beat").
//
//   --record-outcome --pid=<pid>   (assumed, additive optional flag on the
//     EXISTING --record-outcome beat)
//     When one or more prior --poll-footprint beats exist for the given
//     `--pid` (matched by PID alone, per the ticket's own "your design
//     call" — PID is this suite's chosen key), --record-outcome threads the
//     REAL `startedAt` (the timestamp of the FIRST such poll) through to the
//     persisted observation, rather than its current `now = now` default.
//     When no prior poll exists for that PID (or --pid is omitted
//     entirely), --record-outcome's existing now=now fallback behaviour is
//     UNCHANGED — pinned by test 9 below as a currently-passing baseline.
// ---------------------------------------------------------------------------

import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { readHistory } from './lib/history.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

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
 * once the process exits without blocking this test's event loop while the
 * child runs, so two calls started back-to-back via `Promise.all` actually
 * race against each other. Mirrors ./cli-two-orchestrators.jest.spec.mjs's
 * `runCliAsync` exactly.
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
      resolvePromise({ status, stdout: stdout ?? '', stderr: stderr ?? '' });
    });
  });
}

let workDir;
let historyFilePath;
let footprintStateFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-poll-footprint-'));
  historyFilePath = join(workDir, 'history.json');
  footprintStateFilePath = join(workDir, 'footprint-state.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

function baseEnv() {
  return {
    ARM_HISTORY_FILE: historyFilePath,
    ARM_FOOTPRINT_STATE_FILE: footprintStateFilePath,
  };
}

// ---------------------------------------------------------------------------
// 5/6. Conflicting-flag guard — mirrors --record-outcome's own precedent.
// ---------------------------------------------------------------------------

test('--poll-footprint rejects combination with --desired-agents in the same invocation, exit 2', () => {
  const { status, stdout, stderr } = runCli(
    [
      '--poll-footprint=12345',
      '--orchestrator-id=orch-poll-a',
      '--operation-type=implementation-agent',
      '--desired-agents=2',
    ],
    baseEnv(),
  );

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--poll-footprint/);
  expect(stderr).toMatch(/--desired-agents/);
});

test('--poll-footprint rejects combination with --heartbeat in the same invocation, exit 2', () => {
  const { status, stdout, stderr } = runCli(
    [
      '--poll-footprint=12345',
      '--orchestrator-id=orch-poll-b',
      '--operation-type=implementation-agent',
      '--heartbeat',
    ],
    baseEnv(),
  );

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--poll-footprint/);
  expect(stderr).toMatch(/--heartbeat/);
});

// ---------------------------------------------------------------------------
// 7. Required-flag validation — mirrors --record-outcome's own precedent.
// ---------------------------------------------------------------------------

test('--poll-footprint requires --orchestrator-id, exit 2, clear stderr naming --poll-footprint', () => {
  const { status, stdout, stderr } = runCli(
    ['--poll-footprint=12345', '--operation-type=implementation-agent'],
    baseEnv(),
  );

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--poll-footprint/);
  expect(stderr).toMatch(/--orchestrator-id/);
  expect(stderr).toMatch(/required/i);
});

test('--poll-footprint requires --operation-type, exit 2, clear stderr naming --poll-footprint', () => {
  const { status, stdout, stderr } = runCli(
    ['--poll-footprint=12345', '--orchestrator-id=orch-poll-c'],
    baseEnv(),
  );

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--poll-footprint/);
  expect(stderr).toMatch(/--operation-type/);
  expect(stderr).toMatch(/required/i);
});

// ---------------------------------------------------------------------------
// 8. --record-outcome derives the REAL duration from prior --poll-footprint
// beats for the same PID, rather than its current now=now default.
// ---------------------------------------------------------------------------

test(
  '--record-outcome derives real startedAt/duration when prior --poll-footprint beats exist for the same PID',
  () => {
    const PID = 54321;
    const T0 = 1_700_200_000_000;
    const POLL_INTERVAL_MS = 15_000;

    const pollEnv = {
      ...baseEnv(),
      ARM_COORDINATION_FILE: join(workDir, 'coordination.json'),
      ARM_BEAT_NOW_TEST_MODE: '1',
    };

    // Three successive polls of the same PID, each a fresh one-shot process
    // (matching how a real orchestrator re-invokes this beat every 10-30s),
    // with an increasing ARM_FAKE_NOW_MS across them — mirrors cli.mjs's
    // established ARM_FAKE_NOW_MS seam (see cli.mjs's own resolveNow() doc
    // comment) for simulating elapsed wall-clock time across separate
    // one-shot child-process invocations without ever waiting in real time.
    for (let i = 0; i < 3; i += 1) {
      process.env.ARM_FAKE_FOOTPRINT_OUTPUT = `phys_footprint: ${200 + i * 10} MB`;
      const poll = runCli(
        [
          `--poll-footprint=${PID}`,
          '--orchestrator-id=orch-poll-duration',
          '--operation-type=implementation-agent',
        ],
        {
          ...pollEnv,
          ARM_FAKE_NOW_MS: String(T0 + i * POLL_INTERVAL_MS),
          ARM_FAKE_FOOTPRINT_OUTPUT: `phys_footprint: ${200 + i * 10} MB`,
        },
      );
      expect(poll.status).toBe(0);
    }
    delete process.env.ARM_FAKE_FOOTPRINT_OUTPUT;

    const recordAt = T0 + 2 * POLL_INTERVAL_MS + 5_000;
    const record = runCli(
      [
        '--record-outcome',
        `--pid=${PID}`,
        '--operation-type=implementation-agent',
        '--orchestrator-id=orch-poll-duration',
      ],
      {
        ...pollEnv,
        ARM_FAKE_NOW_MS: String(recordAt),
      },
    );

    expect(record.status).toBe(0);

    const history = readHistory(historyFilePath, 'implementation-agent');
    return history.then((result) => {
      expect(result.raw).toHaveLength(1);
      const recorded = result.raw[0];

      // The real poll span (first poll at T0 through the record call), NOT
      // the degenerate startedAt===endedAt===now() shape --record-outcome
      // falls back to when no polling ever happened for a PID.
      expect(recorded.startedAt).toBe(T0);
      expect(recorded.endedAt).toBe(recordAt);
      expect(recorded.endedAt - recorded.startedAt).toBe(recordAt - T0);
      expect(recorded.endedAt - recorded.startedAt).toBeGreaterThan(0);

      // whole-branch review, Medium finding — the EWMA-smoothed
      // phys_footprint peak lands in the NEW, distinct
      // `peakSmoothedFootprintMb` field (with `peakMemorySource` naming its
      // provenance), never in `peakMemoryMb` — that field stays absent
      // entirely since no `--peak-memory-mb` flag was ever passed on this
      // path. This is the exact back-compat guarantee `estimateOperationCost`
      // depends on: its median/percentile logic reads only `peakMemoryMb`.
      expect(recorded.peakMemoryMb).toBeUndefined();
      expect(typeof recorded.peakSmoothedFootprintMb).toBe('number');
      expect(Number.isFinite(recorded.peakSmoothedFootprintMb)).toBe(true);
      expect(recorded.peakMemorySource).toBe('phys-footprint-ewma');
    });
  },
);

// ---------------------------------------------------------------------------
// 12 (whole-branch review, High finding). agentClass round-trips
// end-to-end through the REAL CLI: --poll-footprint --agent-class=<label>,
// then --record-outcome --pid=<pid>, then read back via readHistory. Proves
// the persisted state entry actually carries agentClass (it did not before
// this fix — the flag was parsed and echoed into --poll-footprint's own
// stdout confirmation but never written into lib/footprint-state.mjs's
// per-pid entry, so no downstream observation could ever carry it).
// ---------------------------------------------------------------------------

test('agentClass survives --poll-footprint --agent-class=<label> through --record-outcome to readHistory, via the real CLI', async () => {
  const PID = 24680;
  const T0 = 1_700_600_000_000;
  const AGENT_CLASS = 'typescript-implementer';

  const pollEnv = {
    ...baseEnv(),
    ARM_COORDINATION_FILE: join(workDir, 'coordination-agent-class.json'),
    ARM_BEAT_NOW_TEST_MODE: '1',
  };

  const poll = runCli(
    [
      `--poll-footprint=${PID}`,
      '--orchestrator-id=orch-agent-class',
      '--operation-type=implementation-agent',
      `--agent-class=${AGENT_CLASS}`,
    ],
    { ...pollEnv, ARM_FAKE_NOW_MS: String(T0), ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 210 MB' },
  );
  expect(poll.status).toBe(0);
  expect(JSON.parse(poll.stdout)).toMatchObject({ polled: true, pid: PID, agentClass: AGENT_CLASS });

  const recordAt = T0 + 12_000;
  const record = runCli(
    ['--record-outcome', `--pid=${PID}`, '--operation-type=implementation-agent', '--orchestrator-id=orch-agent-class'],
    { ...pollEnv, ARM_FAKE_NOW_MS: String(recordAt) },
  );
  expect(record.status).toBe(0);

  const history = await readHistory(historyFilePath, 'implementation-agent');
  expect(history.raw).toHaveLength(1);
  expect(history.raw[0].agentClass).toBe(AGENT_CLASS);
});

// ---------------------------------------------------------------------------
// 13 (whole-branch review, Medium finding). --poll-footprint's stdout
// confirmation carries the raw sample result (`sampleMb`) so a caller can
// distinguish a successful sample from a failed one, rather than always
// printing `polled: true` with no signal either way.
// ---------------------------------------------------------------------------

test('--poll-footprint reports a finite sampleMb on a successful sample', () => {
  const { status, stdout } = runCli(
    ['--poll-footprint=13579', '--orchestrator-id=orch-sample-ok', '--operation-type=implementation-agent'],
    { ...baseEnv(), ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 175 MB' },
  );

  expect(status).toBe(0);
  const parsed = JSON.parse(stdout);
  expect(parsed.polled).toBe(true);
  expect(typeof parsed.sampleMb).toBe('number');
  expect(Number.isFinite(parsed.sampleMb)).toBe(true);
});

test('--poll-footprint reports sampleMb: null, still exits 0, when the sampler yields no usable output', () => {
  const { status, stdout, stderr } = runCli(
    ['--poll-footprint=97531', '--orchestrator-id=orch-sample-fail', '--operation-type=implementation-agent'],
    { ...baseEnv(), ARM_FAKE_FOOTPRINT_OUTPUT: '' },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');
  const parsed = JSON.parse(stdout);
  expect(parsed.polled).toBe(true);
  expect(parsed.sampleMb).toBeNull();
});

// ---------------------------------------------------------------------------
// 9. --record-outcome falls back to now=now when no prior --poll-footprint
// beats exist for the run — pins TODAY'S existing behaviour unchanged.
// ---------------------------------------------------------------------------

test('--record-outcome falls back to now=now when no prior --poll-footprint beats exist for the PID', async () => {
  const now = 1_700_300_000_000;

  const { status } = runCli(
    [
      '--record-outcome',
      '--pid=99999',
      '--operation-type=implementation-agent',
      '--orchestrator-id=orch-no-poll',
    ],
    {
      ...baseEnv(),
      ARM_COORDINATION_FILE: join(workDir, 'coordination-no-poll.json'),
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_NOW_MS: String(now),
    },
  );

  expect(status).toBe(0);

  const history = await readHistory(historyFilePath, 'implementation-agent');
  expect(history.raw).toHaveLength(1);
  const recorded = history.raw[0];

  // Unchanged degenerate shape: startedAt === endedAt === the resolved now.
  expect(recorded.startedAt).toBe(now);
  expect(recorded.endedAt).toBe(now);
  expect(recorded.endedAt - recorded.startedAt).toBe(0);
});

// ---------------------------------------------------------------------------
// 11 (Phase 3 review, Medium). A pid's footprint state is removed once
// --record-outcome has consumed it, so a SUBSEQUENT --record-outcome for the
// SAME pid (e.g. the pid being reused, or a duplicate/retried call) falls
// back to the degenerate now=now shape rather than re-threading stale poll
// state — proving the store doesn't grow unboundedly across a long-lived
// orchestrator's pid churn.
// ---------------------------------------------------------------------------

test(
  "a pid's footprint state is removed after --record-outcome consumes it, so a subsequent " +
    '--record-outcome for the same pid falls back to now=now',
  async () => {
    const PID = 67890;
    const T0 = 1_700_500_000_000;

    const pollEnv = {
      ...baseEnv(),
      ARM_COORDINATION_FILE: join(workDir, 'coordination-evict.json'),
      ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 200 MB',
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_NOW_MS: String(T0),
    };

    const poll = runCli(
      [`--poll-footprint=${PID}`, '--orchestrator-id=orch-evict', '--operation-type=implementation-agent'],
      pollEnv,
    );
    expect(poll.status).toBe(0);

    const firstRecordAt = T0 + 10_000;
    const firstRecord = runCli(
      ['--record-outcome', `--pid=${PID}`, '--operation-type=implementation-agent', '--orchestrator-id=orch-evict'],
      { ...pollEnv, ARM_FAKE_NOW_MS: String(firstRecordAt) },
    );
    expect(firstRecord.status).toBe(0);

    // Same pid, reused by a new run, with NO --poll-footprint beat in
    // between — the first --record-outcome must have consumed (evicted) the
    // prior poll state, so this second call cannot see it.
    const secondRecordAt = T0 + 60_000;
    const secondRecord = runCli(
      ['--record-outcome', `--pid=${PID}`, '--operation-type=implementation-agent', '--orchestrator-id=orch-evict'],
      { ...pollEnv, ARM_FAKE_NOW_MS: String(secondRecordAt) },
    );
    expect(secondRecord.status).toBe(0);

    const history = await readHistory(historyFilePath, 'implementation-agent');
    expect(history.raw).toHaveLength(2);

    const firstEntry = history.raw[0];
    const secondEntry = history.raw[1];

    expect(firstEntry.startedAt).toBe(T0);
    expect(firstEntry.endedAt).toBe(firstRecordAt);

    // Degenerate now=now shape — proves the second call found no persisted
    // poll state for this pid to thread through.
    expect(secondEntry.startedAt).toBe(secondRecordAt);
    expect(secondEntry.endedAt).toBe(secondRecordAt);
  },
);

// ---------------------------------------------------------------------------
// 10. Concurrent --poll-footprint calls for DIFFERENT PIDs both persist
// without a torn write — the shared per-PID smoothing-state store must
// survive two genuinely concurrent one-shot processes writing to it at once,
// reusing coordination-file.mjs's existing lock (not a new one).
// ---------------------------------------------------------------------------

test(
  'concurrent --poll-footprint calls for two different PIDs both persist without a torn write, ' +
    'proven downstream via two independent --record-outcome calls',
  async () => {
    const PID_A = 11111;
    const PID_B = 22222;
    const T0 = 1_700_400_000_000;

    const sharedEnv = {
      ...baseEnv(),
      ARM_COORDINATION_FILE: join(workDir, 'coordination-concurrent.json'),
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_NOW_MS: String(T0),
    };

    const [resultA, resultB] = await Promise.all([
      runCliAsync(
        [`--poll-footprint=${PID_A}`, '--orchestrator-id=orch-concurrent-a', '--operation-type=implementation-agent'],
        { ...sharedEnv, ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 150 MB' },
      ),
      runCliAsync(
        [`--poll-footprint=${PID_B}`, '--orchestrator-id=orch-concurrent-b', '--operation-type=implementation-agent'],
        { ...sharedEnv, ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 450 MB' },
      ),
    ]);

    expect(resultA.status).toBe(0);
    expect(resultB.status).toBe(0);
    expect(resultA.stderr).toBe('');
    expect(resultB.stderr).toBe('');

    // Each poll's own confirmation JSON must independently name its own PID
    // — proving neither process's write clobbered or merged with the
    // other's in-flight state.
    const parsedA = JSON.parse(resultA.stdout);
    const parsedB = JSON.parse(resultB.stdout);
    expect(parsedA).toMatchObject({ polled: true, pid: PID_A, orchestratorId: 'orch-concurrent-a' });
    expect(parsedB).toMatchObject({ polled: true, pid: PID_B, orchestratorId: 'orch-concurrent-b' });

    // Downstream proof neither PID's persisted state was corrupted by the
    // other's concurrent write: each PID's own subsequent --record-outcome
    // (keyed by --pid) must still see ITS OWN poll — not zero polls (a torn
    // write dropping one), not the other PID's data (a torn write merging
    // the two).
    const recordA = runCli(
      ['--record-outcome', `--pid=${PID_A}`, '--operation-type=implementation-agent', '--orchestrator-id=orch-concurrent-a'],
      { ...sharedEnv, ARM_FAKE_NOW_MS: String(T0 + 20_000) },
    );
    const recordB = runCli(
      ['--record-outcome', `--pid=${PID_B}`, '--operation-type=implementation-agent', '--orchestrator-id=orch-concurrent-b'],
      { ...sharedEnv, ARM_FAKE_NOW_MS: String(T0 + 20_000) },
    );

    expect(recordA.status).toBe(0);
    expect(recordB.status).toBe(0);

    const history = await readHistory(historyFilePath, 'implementation-agent');
    expect(history.raw).toHaveLength(2);

    const entryA = history.raw.find((entry) => entry.orchestratorId === 'orch-concurrent-a');
    const entryB = history.raw.find((entry) => entry.orchestratorId === 'orch-concurrent-b');

    expect(entryA).toBeDefined();
    expect(entryB).toBeDefined();
    // Each PID's real poll-derived startedAt survives distinctly — neither
    // fell back to the degenerate now=now shape (which would signal its own
    // poll's state was lost to the other's concurrent write).
    expect(entryA.startedAt).toBe(T0);
    expect(entryB.startedAt).toBe(T0);
    expect(entryA.endedAt - entryA.startedAt).toBeGreaterThan(0);
    expect(entryB.endedAt - entryB.startedAt).toBeGreaterThan(0);
  },
  20_000,
);
