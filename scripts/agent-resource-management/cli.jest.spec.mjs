// Black-box smoke test for scripts/agent-resource-management/cli.mjs
// (Phase 5 — "SKILL.md, CLI wiring, coverage config & cleanup guidance").
//
// RED by construction: cli.mjs does not exist yet (Phases 1-4 only shipped
// the lib/ modules + the outer agent-resource-management.jest.spec.mjs that
// wires them together in-process). This file proves the CLI *entrypoint*
// wiring works end-to-end via real process spawning (spawnSync), mirroring
// the idiom in scripts/convergence-analysis/cli.jest.spec.mjs and
// scripts/model-routing/cli.jest.spec.mjs — never importing cli.mjs
// directly, since it is the deliberately-untested-by-unit-test entrypoint
// (see jest.skills.config.mjs's `!scripts/<skill>/cli.mjs` exclusions for
// every sibling skill).
//
// This is a LEAN smoke test, not a re-test of the underlying lib logic — the
// four lib modules already carry 117 unit/integration tests. It only proves:
//   1. the CLI composes recon -> threshold -> coordination-file -> allowance
//      into a single command and prints a valid traffic-light JSON to stdout;
//   2. it never touches the real machine (vm_stat/sysctl/df/ps) or the real
//      `.claude/agent-state/` coordination file during a test run;
//   3. it degrades gracefully rather than crashing when the coordination-file
//      directory doesn't exist/isn't writable;
//   4. exit code 0 on a successful traffic-light computation.
//
// ---------------------------------------------------------------------------
// Injection design this test assumes cli.mjs will implement (for the builder
// to match):
//
//   ARM_COORDINATION_FILE   (env, required for test hermeticity)
//     Absolute path to the coordination file cli.mjs reads/writes via
//     ./lib/coordination-file.mjs's declareDibs/readDibs. Overrides the real
//     production default (a path under `.claude/agent-state/`, the existing
//     shared pacing convention) so tests never touch
//     that file or race real orchestrators using it.
//
//   ARM_FAKE_COLLECT_JSON   (env, required for test hermeticity)
//     A JSON string shaped `{ "memory": {...}, "disk": {...} }` — the exact
//     raw shape ./lib/recon.mjs's takeSample(collect) expects its injected
//     collect() callback to resolve with. When set, cli.mjs's collect()
//     implementation returns this parsed value instead of shelling out to
//     vm_stat/sysctl/df/ps, so no test run ever samples the real host. When
//     unset, cli.mjs is expected to fall back to the real OS collect() (the
//     production path) — this is the one seam Phase 1 deliberately left
//     injectable specifically so this boundary could be tested this way.
//
//   --orchestrator-id=<id>   (CLI flag, required)
//     Passed through to declareDibs's `orchestratorId`.
//
//   --desired-agents=<n>     (CLI flag, optional, default 1)
//     Passed through to declareDibs's `desiredAgents`.
//
// Chosen graceful-degradation behavior (documented explicitly per the task's
// "pick one and test it explicitly" instruction): when the coordination-file
// directory doesn't exist, cli.mjs must NOT crash the caller. Per Phase 1's
// "never crash the caller" design ethos (recon.mjs's takeSample never
// throws; coordination-file.mjs already treats a not-yet-created file as
// "no dibs yet" rather than an error) the safer default is for the CLI to
// treat an unwritable coordination file the same way: log a warning to
// stderr, proceed as if no dibs were declared/read this beat, and still
// print a valid traffic-light JSON to stdout with exit code 0. A traffic
// light an orchestrator can't parse because the CLI crashed is strictly
// worse than one computed without a dibs entry landing this beat.
// ---------------------------------------------------------------------------

import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, rm, writeFile, readFile, access, unlink } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { declareDibs, SHARED_SAMPLE_RESERVED_ID } from './lib/coordination-file.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const DISK_AMBER = { freeDiskGb: 20, declineRateGbPerHour: 0.5 };
const DISK_RED = { freeDiskGb: 5, declineRateGbPerHour: 0.5 };
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 };
// Phase 4: pressureLevel is kept at 1 (below the new, independent
// pressure-WARN pre-check's `warnAtOrAbove: 2` threshold — see cli.mjs's
// DEFAULT_PRESSURE_OVERRIDE_THRESHOLDS) so these fixtures drive AMBER/RED
// classification purely via the SWAP thresholds (as they always have),
// keeping every test below decoupled from the new pressure pre-check —
// which has its own dedicated coverage in pressure-override.jest.spec.mjs.
// Previously pressureLevel:2/4 also happened to independently satisfy the
// axis's OWN redPressureAtOrAbove/greenPressureAtOrBelow thresholds; that
// redundancy is no longer safe now that pressureLevel>=2 short-circuits
// admission BEFORE axis classification is ever reached, so a fixture that
// still used 2/4 here would make hold/pause (and everything downstream of
// them: pauseCandidate selection, hysteresis, dibs, the spawn-rate bucket,
// freshness-window reuse) unreachable through these constants.
const MEMORY_AMBER = { pressureLevel: 1, swapUsedMb: 6144, compressedMb: 2048 };
const MEMORY_RED = { pressureLevel: 1, swapUsedMb: 12288, compressedMb: 4096 };

/** Runs the real cli.mjs as a child process — never imports it directly. */
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
  workDir = await mkdtemp(join(tmpdir(), 'arm-cli-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

test('produces a valid spawn-allowed traffic-light JSON on stdout for an injected GREEN sample, exit 0', () => {
  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-a', '--desired-agents=2'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    },
  );

  expect(status).toBe(0);

  let parsed;
  expect(() => {
    parsed = JSON.parse(stdout);
  }).not.toThrow();

  expect(['spawn-allowed', 'hold', 'pause']).toContain(parsed.type);
  expect(parsed.type).toBe('spawn-allowed');
  expect(typeof parsed.allowance).toBe('number');
  expect(stderr).toBe('');
});

test('produces a valid hold traffic-light JSON for an injected AMBER sample, exit 0', () => {
  const { status, stdout } = runCli(
    ['--orchestrator-id=orch-a'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_AMBER, disk: DISK_GREEN }),
    },
  );

  expect(status).toBe(0);
  const parsed = JSON.parse(stdout);
  expect(['spawn-allowed', 'hold', 'pause']).toContain(parsed.type);
  expect(parsed.type).toBe('hold');
});

describe('cli.mjs --orchestrator-id reserved-id rejection', () => {
  test('rejects a reserved double-underscore --orchestrator-id, exit 2, clear stderr, no coordination file write', async () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=__shared-machine-sample__'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      },
    );

    expect(status).toBe(2);
    expect(stdout).toBe('');
    expect(stderr).toMatch(/--orchestrator-id/);
    expect(stderr).toMatch(/reserved/i);

    // The coordination file must never be created/written to when the id is
    // rejected before any declareDibs call — the reserved-id check happens
    // before ensureCoordinationDir/declareDibs in main().
    await expect(access(coordinationFilePath)).rejects.toThrow();
  });
});

// A dedicated "RED sample, exit 0" test used to live here, pinning the
// pre-existing-Phase-2 HIGH-5 pause->hold downgrade (`{ type: 'hold' }` with no
// `pauseCandidate`, because running-agent enumeration wasn't wired in and an
// unconditional empty list made `pause`/`null` look like a dead end). Phase 2
// (commit 207d4567) wired real `ps`-backed running-agent discovery and
// removed that downgrade, so a RED beat now honestly reports
// `{ type: 'pause', pauseCandidate: <real-or-null> }`. That contract,
// including the genuinely-zero-running-agents case that the removed test
// exercised, is now covered precisely by
// 'emits an HONEST pause/null (not the old hold downgrade) when zero
// claude-rooted trees are running' in the
// "cli.mjs real running-agent discovery on a RED beat"
// describe block below (it sets ARM_FAKE_PS_OUTPUT to an idle-host fixture
// for determinism, asserting `{ type: 'pause', pauseCandidate: null }`) — so
// the older, less specific test was removed as a duplicate rather than
// patched in place.

test('degrades gracefully (exit 0, valid JSON, stderr warning) when the coordination-file directory cannot be created', async () => {
  // cli.mjs now `mkdir(dirname(coordinationFilePath), { recursive: true })`s
  // before its first declareDibs call (HIGH-2 fix), so a merely-missing
  // parent directory is no longer a degraded case — it gets created. To
  // still exercise the "genuinely unwritable path" branch, make a path
  // COMPONENT of the parent directory a plain file rather than a directory:
  // `mkdir(..., { recursive: true })` fails with ENOTDIR in that case
  // regardless of the running user's permissions (even root can't mkdir
  // through a file), so this is a portable, CI-safe way to force the
  // fallback path.
  const blockingFile = join(workDir, 'not-a-directory');
  await writeFile(blockingFile, 'blocking');
  const unwritablePath = join(blockingFile, 'no-such-parent-dir', 'coordination.json');

  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-a'],
    {
      ARM_COORDINATION_FILE: unwritablePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    },
  );

  // Chosen safer default (documented above): never crash the caller — degrade
  // to "no dibs this beat" and still hand back a valid, parseable traffic
  // light, exit 0. A warning on stderr surfaces the degraded condition
  // without poisoning stdout (which orchestrators parse as JSON).
  expect(status).toBe(0);

  let parsed;
  expect(() => {
    parsed = JSON.parse(stdout);
  }).not.toThrow();
  expect(['spawn-allowed', 'hold', 'pause']).toContain(parsed.type);

  expect(stderr.length).toBeGreaterThan(0);
  expect(stderr.toLowerCase()).toMatch(/coordination|dibs|warn/);
});

test('creates the coordination file\'s missing NESTED parent directories (HIGH-2 positive path), exit 0, no stderr', async () => {
  // Every other test's beforeEach already creates `workDir` itself via
  // `mkdtemp`, so none of them would fail if `ensureCoordinationDir` were
  // deleted entirely from cli.mjs — this is the one test that actually
  // requires it: point ARM_COORDINATION_FILE at a path whose parent
  // directories do not exist yet at all (not just "one level missing").
  const nestedCoordinationFilePath = join(workDir, 'nested', 'deeper', 'coordination.json');

  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-a'],
    {
      ARM_COORDINATION_FILE: nestedCoordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    },
  );

  expect(status).toBe(0);

  let parsed;
  expect(() => {
    parsed = JSON.parse(stdout);
  }).not.toThrow();
  expect(['spawn-allowed', 'hold', 'pause']).toContain(parsed.type);

  // Success path, not the degraded-warning path: stderr must be empty.
  expect(stderr).toBe('');

  // The nested parent directories were created and the coordination file
  // itself now exists on disk at that nested path.
  await expect(access(nestedCoordinationFilePath)).resolves.toBeUndefined();
});

// ---------------------------------------------------------------------------
// Phase 0 — outer acceptance test for `--heartbeat`.
//
// RED by construction: `--heartbeat` does not exist yet. This test proves the
// eventual contract: an orchestrator may invoke cli.mjs more often than once
// per real beat (a periodic wall-clock "heartbeat" check, not just "after
// each sub-agent's task completes") WITHOUT that extra invocation frequency
// artificially fast-forwarding the hysteresis recovery window. A `--heartbeat`
// invocation must still read the machine and print a valid traffic-light
// JSON (so a caller polling on a timer gets a live answer every time it
// asks) — it just must not be counted toward `consecutiveGreen`, the
// recovery counter `./lib/threshold.mjs`'s `applyHysteresis` advances once
// per counted sample.
//
// Green when Phase 1 (a non-counting hysteresis primitive in
// lib/threshold.mjs) + Phase 2 (`--heartbeat` wiring in cli.mjs) both land.
// ---------------------------------------------------------------------------

describe('cli.mjs --heartbeat', () => {
  async function readCoordinationEntry(filePath, orchestratorId) {
    const raw = await readFile(filePath, 'utf8');
    const entries = JSON.parse(raw);
    const entry = entries.find((candidate) => candidate.orchestratorId === orchestratorId);
    expect(entry).toBeDefined();
    return entry;
  }

  function expectValidTrafficLight(stdout) {
    let parsed;
    expect(() => {
      parsed = JSON.parse(stdout);
    }).not.toThrow();
    expect(['spawn-allowed', 'hold', 'pause']).toContain(parsed.type);
    return parsed;
  }

  it(
    'produces a valid traffic-light JSON on a repeated timer-driven invocation without advancing ' +
      'hysteresis recovery any faster than an equivalent number of real beats',
    async () => {
      const orchestratorId = 'orch-heartbeat';
      // resolveNow() now also requires ARM_BEAT_NOW_TEST_MODE='1' before
      // honouring ARM_FAKE_NOW_MS — this pre-existing test's intent (fake the
      // clock across sequential invocations) requires the new switch too.
      const env = { ARM_COORDINATION_FILE: coordinationFilePath, ARM_BEAT_NOW_TEST_MODE: '1' };

      // Phase 6: DEFAULT_HYSTERESIS_CONFIG.hysteresisRecoverWindowMs
      // (120_000ms) gates GREEN recovery in ADDITION to the consecutive-
      // GREEN-count gate exercised by this test. All 6+ beats below run
      // within real milliseconds of each other, so recovery would never
      // fire under real wall-clock time — ARM_FAKE_NOW_MS (test seam,
      // Phase 6) lets each sequential invocation declare its own simulated
      // `now`, so the count gate and the wall-clock gate can each be proven
      // independently without waiting in real time.
      const FAKE_NOW_BASE = 1_700_000_000_000;

      // --- Beat 1 (real): trip the memory axis to AMBER. ------------------
      const beat1 = runCli(
        ['--orchestrator-id=' + orchestratorId],
        {
          ...env,
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_AMBER, disk: DISK_GREEN }),
          ARM_FAKE_NOW_MS: String(FAKE_NOW_BASE),
        },
      );
      expect(beat1.status).toBe(0);
      expectValidTrafficLight(beat1.stdout);

      let entry = await readCoordinationEntry(coordinationFilePath, orchestratorId);
      expect(entry.memoryHistory).toMatchObject({ consecutiveGreen: 0, tripped: true, level: 'AMBER' });
      // Phase 6: a real worsening transition stamps `nonGreenSince`
      // with a wall-clock timestamp.
      expect(entry.memoryHistory.nonGreenSince).toBe(FAKE_NOW_BASE);

      // --- Beat 2 (real): first GREEN sample since the trip. -------------
      const beat2 = runCli(
        ['--orchestrator-id=' + orchestratorId],
        {
          ...env,
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
          ARM_FAKE_NOW_MS: String(FAKE_NOW_BASE + 1_000),
        },
      );
      expect(beat2.status).toBe(0);
      expectValidTrafficLight(beat2.stdout);

      entry = await readCoordinationEntry(coordinationFilePath, orchestratorId);
      expect(entry.memoryHistory).toMatchObject({ consecutiveGreen: 1, tripped: true, level: 'AMBER' });

      // --- Beats 3 & 4 (--heartbeat, timer-driven, same GREEN sample): ----
      // these must NOT advance consecutiveGreen — a caller polling this CLI
      // on a wall-clock timer between real beats must not fast-forward
      // recovery relative to an orchestrator that only ever calls it once
      // per real beat.
      for (let i = 0; i < 2; i += 1) {
        const heartbeat = runCli(
          ['--orchestrator-id=' + orchestratorId, '--heartbeat'],
          {
            ...env,
            ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
            ARM_FAKE_NOW_MS: String(FAKE_NOW_BASE + 1_000),
          },
        );
        expect(heartbeat.status).toBe(0);
        expectValidTrafficLight(heartbeat.stdout);

        entry = await readCoordinationEntry(coordinationFilePath, orchestratorId);
        expect(entry.memoryHistory).toMatchObject({ consecutiveGreen: 1, tripped: true, level: 'AMBER' });
      }

      // --- Beat 5 (real): second counted GREEN sample. --------------------
      const beat5 = runCli(
        ['--orchestrator-id=' + orchestratorId],
        {
          ...env,
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
          ARM_FAKE_NOW_MS: String(FAKE_NOW_BASE + 2_000),
        },
      );
      expect(beat5.status).toBe(0);
      expectValidTrafficLight(beat5.stdout);

      entry = await readCoordinationEntry(coordinationFilePath, orchestratorId);
      expect(entry.memoryHistory).toMatchObject({ consecutiveGreen: 2, tripped: true, level: 'AMBER' });

      // --- Beat 6 (real): third counted GREEN sample — DEFAULT_HYSTERESIS_
      // CONFIG.hysteresisRecoverConsecutiveGreen is 3, so the count gate is
      // satisfied here. Two intervening `--heartbeat` calls (beats 3 & 4)
      // must not have brought this forward. BUT Phase 6's wall-clock
      // gate is deliberately still unsatisfied at this beat (only 3s of
      // simulated time have elapsed since the trip, far short of
      // hysteresisRecoverWindowMs's 120_000ms) — recovery must NOT fire yet,
      // proving the dual-gate wiring rather than only the count gate.
      const beat6 = runCli(
        ['--orchestrator-id=' + orchestratorId],
        {
          ...env,
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
          ARM_FAKE_NOW_MS: String(FAKE_NOW_BASE + 3_000),
        },
      );
      expect(beat6.status).toBe(0);
      const beat6Light = expectValidTrafficLight(beat6.stdout);
      expect(beat6Light.type).toBe('hold');

      entry = await readCoordinationEntry(coordinationFilePath, orchestratorId);
      expect(entry.memoryHistory).toMatchObject({ consecutiveGreen: 3, tripped: true, level: 'AMBER' });

      // --- Beat 7 (real): a further GREEN sample, now with enough
      // simulated wall-clock time elapsed since the trip (well over the
      // 120_000ms hysteresisRecoverWindowMs) for BOTH gates to be
      // satisfied — this is the first beat where recovery to GREEN takes
      // effect.
      const beat7 = runCli(
        ['--orchestrator-id=' + orchestratorId],
        {
          ...env,
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
          ARM_FAKE_NOW_MS: String(FAKE_NOW_BASE + 130_000),
        },
      );
      expect(beat7.status).toBe(0);
      const beat7Light = expectValidTrafficLight(beat7.stdout);
      expect(beat7Light.type).toBe('spawn-allowed');

      entry = await readCoordinationEntry(coordinationFilePath, orchestratorId);
      expect(entry.memoryHistory).toMatchObject({ consecutiveGreen: 4, tripped: false, level: 'GREEN' });
    },
  );
});

// ---------------------------------------------------------------------------
// Phase 2 — targeted `--heartbeat` CLI-wiring tests.
//
// RED by construction: `--heartbeat` is not yet a recognized flag anywhere in
// cli.mjs. These are narrower than the Phase 0 outer acceptance test above
// (which already covers the core "heartbeat doesn't fast-forward recovery"
// scenario end-to-end) — they target the flag's surface-level wiring: does it
// exist, does it compose with other flags, does it still drive declareDibs,
// and does the CLI's required-flag validation apply identically regardless
// of --heartbeat.
// ---------------------------------------------------------------------------

describe('cli.mjs --heartbeat wiring (Phase 2, targeted)', () => {
  test('prints a valid traffic-light JSON to stdout and exits 0', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-hb-1', '--heartbeat'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      },
    );

    expect(status).toBe(0);
    let parsed;
    expect(() => {
      parsed = JSON.parse(stdout);
    }).not.toThrow();
    expect(['spawn-allowed', 'hold', 'pause']).toContain(parsed.type);
    expect(stderr).toBe('');
  });

  test('without --orchestrator-id errors identically to a normal beat (same message, same exit code)', () => {
    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    };

    const normalBeat = runCli([], env);
    const heartbeat = runCli(['--heartbeat'], env);

    expect(normalBeat.status).toBe(2);
    expect(heartbeat.status).toBe(2);
    expect(heartbeat.stderr).toBe(normalBeat.stderr);
    expect(heartbeat.stdout).toBe('');
  });

  test('combined with --desired-agents=0 produces a valid allowance number without crashing', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-hb-2', '--desired-agents=0', '--heartbeat'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      },
    );

    expect(status).toBe(0);
    let parsed;
    expect(() => {
      parsed = JSON.parse(stdout);
    }).not.toThrow();
    expect(['spawn-allowed', 'hold', 'pause']).toContain(parsed.type);
    if ('allowance' in parsed) {
      expect(typeof parsed.allowance).toBe('number');
      expect(Number.isFinite(parsed.allowance)).toBe(true);
    }
    expect(stderr).toBe('');
  });

  test('still calls declareDibs (liveness/timestamp refreshed) while persisting Phase 1\'s non-counting history', async () => {
    const orchestratorId = 'orch-hb-3';
    const env = { ARM_COORDINATION_FILE: coordinationFilePath };

    async function readEntry() {
      const raw = await readFile(coordinationFilePath, 'utf8');
      const entries = JSON.parse(raw);
      const entry = entries.find((candidate) => candidate.orchestratorId === orchestratorId);
      expect(entry).toBeDefined();
      return entry;
    }

    // Real beat: trip memory to AMBER, establishing tripped history.
    const beat1 = runCli(
      ['--orchestrator-id=' + orchestratorId],
      { ...env, ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_AMBER, disk: DISK_GREEN }) },
    );
    expect(beat1.status).toBe(0);
    const afterBeat1 = await readEntry();
    expect(afterBeat1.memoryHistory).toMatchObject({ consecutiveGreen: 0, tripped: true, level: 'AMBER' });
    // Phase 6: a real worsening transition stamps `nonGreenSince` with
    // a wall-clock timestamp.
    expect(typeof afterBeat1.memoryHistory.nonGreenSince).toBe('number');
    const declaredAtAfterBeat1 = afterBeat1.declaredAt;

    // Heartbeat with a GREEN sample: declareDibs must still run (this
    // orchestrator's liveness entry advances), but because Phase 1's
    // `{ countsTowardRecovery: false }` mode is threaded through, the
    // persisted recovery counter must NOT advance past what beat1 left it at.
    await new Promise((r) => setTimeout(r, 2));
    const heartbeat = runCli(
      ['--orchestrator-id=' + orchestratorId, '--heartbeat'],
      { ...env, ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }) },
    );
    expect(heartbeat.status).toBe(0);

    const afterHeartbeat = await readEntry();
    expect(afterHeartbeat.declaredAt).toBeGreaterThan(declaredAtAfterBeat1);
    expect(afterHeartbeat.memoryHistory).toMatchObject({ consecutiveGreen: 0, tripped: true, level: 'AMBER' });
  });

  // DEFAULT_HEARTBEAT_INTERVAL_MS is a documented recommended polling cadence
  // for the ORCHESTRATOR loop, not a value cli.mjs itself reads, loops on, or
  // enforces (cli.mjs stays one-shot per beat/heartbeat invocation) — so
  // there is no runtime flag or timing behavior to assert black-box via
  // spawnSync, matching how this file already treats every other `DEFAULT_*`
  // constant (DEFAULT_MEMORY_THRESHOLDS, DEFAULT_HYSTERESIS_CONFIG, etc.): not
  // unit-tested by value here at all, because cli.mjs is deliberately never
  // imported by this spec (see the file-level doc comment). The adapted,
  // testable claim is behavioral: repeated `--heartbeat` invocations (as an
  // orchestrator polling on ANY cadence, faster or slower than the
  // recommended default, would produce) never require an interval-related
  // flag and never fail merely for being invoked back-to-back.
  test('repeated --heartbeat invocations require no interval flag and never fail from polling frequency alone', () => {
    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    };

    for (let i = 0; i < 3; i += 1) {
      const { status, stdout, stderr } = runCli(['--orchestrator-id=orch-hb-4', '--heartbeat'], env);
      expect(status).toBe(0);
      expect(() => JSON.parse(stdout)).not.toThrow();
      expect(stderr).toBe('');
    }
  });

  // Targeted regression test for the High finding fixed alongside
  // Phase 4/5: a `--heartbeat` beat must NEVER draw tokens from the real,
  // shared, global spawn-rate bucket, even though `--desired-agents` defaults
  // to `1` (and this SKILL.md's own heartbeat example omits the flag
  // entirely). `--desired-agents` is documented as informational intent only
  // ("not itself a request the CLI grants or denies") — a heartbeat
  // therefore represents ZERO real spawn intent and must be a full/no-op
  // grant against the bucket.
  test('repeated --heartbeat invocations never drain the shared global spawn-rate bucket', async () => {
    const GLOBAL_SPAWN_BUCKET_RESERVED_ID = '__global-spawn-rate-bucket__';
    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      // Small capacity, no refill — makes any real draw-down immediately
      // observable rather than masked by refill accrual over the test's
      // real-time span.
      ARM_SPAWN_BUCKET_CONFIG_JSON: JSON.stringify({ capacityTokens: 3, refillTokensPerMs: 0 }),
    };

    async function readRawBucketEntry() {
      const raw = await readFile(coordinationFilePath, 'utf8').catch(() => '[]');
      const entries = JSON.parse(raw);
      return entries.find((entry) => entry.orchestratorId === GLOBAL_SPAWN_BUCKET_RESERVED_ID);
    }

    // Several heartbeat ticks, deliberately requesting more than the whole
    // bucket capacity via --desired-agents (which a real orchestrator would
    // never do on a heartbeat, but this is exactly the scenario the fix
    // guards against: --desired-agents still defaults to 1, and the CLI must
    // never forward it to the token bucket for a heartbeat beat regardless).
    for (let i = 0; i < 5; i += 1) {
      const { status, stdout, stderr } = runCli(
        ['--orchestrator-id=orch-hb-no-drain', '--heartbeat', '--desired-agents=2'],
        env,
      );
      expect(status).toBe(0);
      expect(stderr).toBe('');
      expect(JSON.parse(stdout).type).toBe('spawn-allowed');
    }

    // A heartbeat must never even create the reserved bucket entry, since it
    // never calls consumeGlobalSpawnTokens at all.
    expect(await readRawBucketEntry()).toBeUndefined();

    // A subsequent NON-heartbeat beat must still see the bucket at full,
    // undrained capacity (3 tokens) — proof the heartbeat ticks above left it
    // completely untouched.
    const realBeat = runCli(['--orchestrator-id=orch-hb-no-drain', '--desired-agents=3'], env);
    expect(realBeat.status).toBe(0);
    const parsedRealBeat = JSON.parse(realBeat.stdout);
    expect(parsedRealBeat.type).toBe('spawn-allowed');
    expect(parsedRealBeat.allowance).toBe(4);

    const bucketAfterRealBeat = await readRawBucketEntry();
    expect(bucketAfterRealBeat).toBeDefined();
    expect(bucketAfterRealBeat.tokens).toBe(0);
  });

  // Pins the intentional `{"type":"spawn-allowed","allowance":0}` shape
  // documented in SKILL.md's output contract: a fully-drained global
  // spawn-rate bucket on an otherwise-GREEN beat must still report
  // `spawn-allowed` (both axes really are GREEN) capped to `allowance: 0`,
  // never silently downgraded to `hold`. `hold` would misrepresent WHY
  // spawning isn't possible this beat (an axis-level reason) when the real
  // reason is a rate-limit reason.
  test('a fully-drained global spawn-rate bucket on a GREEN beat reports spawn-allowed with allowance:0, not hold', () => {
    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      // No refill, and the priming call below consumes the entire capacity.
      ARM_SPAWN_BUCKET_CONFIG_JSON: JSON.stringify({ capacityTokens: 2, refillTokensPerMs: 0 }),
    };

    // Prior beat drains the bucket to 0.
    const primingBeat = runCli(['--orchestrator-id=orch-drained-bucket', '--desired-agents=2'], env);
    expect(primingBeat.status).toBe(0);
    expect(JSON.parse(primingBeat.stdout).type).toBe('spawn-allowed');

    // Next beat: both axes are still GREEN, but the bucket has nothing left
    // to grant.
    const drainedBeat = runCli(['--orchestrator-id=orch-drained-bucket', '--desired-agents=1'], env);
    expect(drainedBeat.status).toBe(0);
    expect(drainedBeat.stderr).toBe('');
    // Phase 6: every normal beat's printed JSON now also carries a
    // sibling `diskTrend` field (see ./disk-trend.jest.spec.mjs) — checked
    // field-by-field here rather than via a full `toEqual` because its exact
    // value depends on real (unfaked) elapsed wall-clock time between this
    // test's two sequential `runCli` calls, which is not deterministic.
    const parsedDrained = JSON.parse(drainedBeat.stdout);
    expect(parsedDrained.type).toBe('spawn-allowed');
    expect(parsedDrained.allowance).toBe(0);
  });

  // Regression test for the Medium finding: the global spawn-rate
  // bucket is a binary GATE (did it grant the full request, or deny it), not
  // a second value that further shrinks an already-correct `allowance` down
  // to whatever smaller amount happened to be requested from it. A solo,
  // uncontended orchestrator asking for FEWER agents than the per-axis cap,
  // with an ample (undrained) global bucket, must still be told the FULL
  // cap as headroom — exactly SKILL.md's flagship worked example ("it wanted
  // 2; the allowance of 3 is headroom, not a target") — never clamped down to
  // its own smaller ask just because that smaller ask is also what got drawn
  // from the bucket. Phase 3 (Option B) reaffirms this same property
  // for the live-agent axis too: `allowance` is not narrowed by what was
  // actually, atomically reserved via the ledger either — see the separate
  // `liveAgentGrant` field cli.mjs now reports for that operational number.
  test('a solo, uncontended orchestrator asking for fewer agents than the cap, with an ample spawn-rate bucket, still reports the FULL cap as headroom (not clamped to its own ask) [regression]', () => {
    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      // Ample capacity relative to the per-axis cap (4, LIVE_AGENT_CEILING) —
      // nothing in this test is meant to exercise bucket denial.
      ARM_SPAWN_BUCKET_CONFIG_JSON: JSON.stringify({ capacityTokens: 10, refillTokensPerMs: 0 }),
    };

    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-headroom-not-target', '--desired-agents=2'],
      env,
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('spawn-allowed');
    expect(parsed.allowance).toBe(4);
    // The live-agent ledger reservation is a SEPARATE fact: this beat only
    // ever asked for (and was atomically granted) 2, even though its
    // advisory headroom is 4.
    expect(parsed.liveAgentGrant).toBe(2);
  });

  // Targeted regression test for the Medium finding fixed alongside
  // Phase 5's real spawn checks: `consumeGlobalSpawnTokens` must only be
  // drawn on a beat that can actually reach `spawn-allowed` (both axes
  // GREEN) — an AMBER or RED beat can never spawn anyway (`hold`/`pause`),
  // so drawing tokens there is pure waste that silently starves the shared
  // bucket for zero protective benefit, throttling legitimate spawns once
  // conditions recover to GREEN.
  test(
    'an AMBER (and separately a RED) beat with non-zero --desired-agents does NOT decrement the shared ' +
      'global spawn-rate bucket [regression]',
    async () => {
      const GLOBAL_SPAWN_BUCKET_RESERVED_ID = '__global-spawn-rate-bucket__';
      // Each sub-scenario below gets its OWN coordination file — this test's
      // target is purely "does an AMBER/RED beat draw the bucket", which the
      // shared-coordination-file `computePerOrchestratorAllowance` dibs
      // division (a separate, already-covered concern) would otherwise
      // confound if multiple contending orchestratorIds shared one file.
      const spawnBucketConfig = { capacityTokens: 3, refillTokensPerMs: 0 };

      async function readRawBucketEntry(filePath) {
        const raw = await readFile(filePath, 'utf8').catch(() => '[]');
        const entries = JSON.parse(raw);
        return entries.find((entry) => entry.orchestratorId === GLOBAL_SPAWN_BUCKET_RESERVED_ID);
      }

      // AMBER beat, non-zero --desired-agents: must not even create the
      // reserved bucket entry, since it never calls consumeGlobalSpawnTokens
      // at all.
      const amberCoordinationFile = join(workDir, 'coordination-amber.json');
      const amberBeat = runCli(
        ['--orchestrator-id=orch-amber-no-drain', '--desired-agents=2'],
        {
          ARM_COORDINATION_FILE: amberCoordinationFile,
          ARM_SPAWN_BUCKET_CONFIG_JSON: JSON.stringify(spawnBucketConfig),
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_AMBER, disk: DISK_GREEN }),
        },
      );
      expect(amberBeat.status).toBe(0);
      expect(amberBeat.stderr).toBe('');
      expect(JSON.parse(amberBeat.stdout).type).toBe('hold');
      expect(await readRawBucketEntry(amberCoordinationFile)).toBeUndefined();

      // RED beat, non-zero --desired-agents: same non-drain expectation.
      const redCoordinationFile = join(workDir, 'coordination-red.json');
      const redBeat = runCli(
        ['--orchestrator-id=orch-red-no-drain', '--desired-agents=2'],
        {
          ARM_COORDINATION_FILE: redCoordinationFile,
          ARM_SPAWN_BUCKET_CONFIG_JSON: JSON.stringify(spawnBucketConfig),
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
        },
      );
      expect(redBeat.status).toBe(0);
      expect(redBeat.stderr).toBe('');
      expect(JSON.parse(redBeat.stdout).type).toBe('pause');
      expect(await readRawBucketEntry(redCoordinationFile)).toBeUndefined();

      // Sanity check: a genuinely GREEN beat on a FRESH coordination file (no
      // AMBER/RED history baggage of its own) still draws the bucket exactly
      // as before this fix — proof this change only skips the draw on
      // AMBER/RED, not on GREEN.
      const greenCoordinationFile = join(workDir, 'coordination-green.json');
      const greenBeat = runCli(
        ['--orchestrator-id=orch-green-confirms-drain', '--desired-agents=3'],
        {
          ARM_COORDINATION_FILE: greenCoordinationFile,
          ARM_SPAWN_BUCKET_CONFIG_JSON: JSON.stringify(spawnBucketConfig),
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        },
      );
      expect(greenBeat.status).toBe(0);
      const parsedGreenBeat = JSON.parse(greenBeat.stdout);
      expect(parsedGreenBeat.type).toBe('spawn-allowed');
      expect(parsedGreenBeat.allowance).toBe(4);

      const bucketAfterGreenBeat = await readRawBucketEntry(greenCoordinationFile);
      expect(bucketAfterGreenBeat).toBeDefined();
      expect(bucketAfterGreenBeat.tokens).toBe(0);
    },
  );

  // Targeted regression test for the Medium finding: a GREEN/GREEN beat
  // whose OWN dibs-constrained per-orchestrator allowance has already been
  // bounded to 0 by genuine contention (pool exhausted by a higher-priority
  // orchestrator) must not draw the shared global spawn-rate bucket either —
  // it can't spawn anything this beat regardless of what the bucket grants,
  // so drawing tokens for it only starves a genuinely-eligible orchestrator's
  // next beat. Same non-drain assertion pattern as the AMBER/RED test above.
  test(
    'a GREEN/GREEN beat whose dibs-division bounds its own allowance to 0 (genuine contention) does NOT ' +
      'decrement the shared global spawn-rate bucket [regression]',
    async () => {
      const GLOBAL_SPAWN_BUCKET_RESERVED_ID = '__global-spawn-rate-bucket__';
      const contendedCoordinationFile = join(workDir, 'coordination-contended.json');
      const env = {
        ARM_COORDINATION_FILE: contendedCoordinationFile,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        // Ample capacity — any draw at all is attributable to the beat under
        // test, not bucket exhaustion from a prior beat.
        ARM_SPAWN_BUCKET_CONFIG_JSON: JSON.stringify({ capacityTokens: 6, refillTokensPerMs: 0 }),
      };

      async function readRawBucketEntry() {
        const raw = await readFile(contendedCoordinationFile, 'utf8').catch(() => '[]');
        const entries = JSON.parse(raw);
        return entries.find((entry) => entry.orchestratorId === GLOBAL_SPAWN_BUCKET_RESERVED_ID);
      }

      // A: uncontended, claims the full flat cap (4, LIVE_AGENT_CEILING) as
      // headroom, and asks for the WHOLE cap (--desired-agents=4) so it
      // legitimately draws the bucket down to 2 (6 - 4) AND fully exhausts
      // the dibs cap — otherwise, with the cap now 4, a smaller ask (e.g. 3)
      // would leave 1 slot of genuine, non-zero dibs headroom for B below,
      // which is not what this test means to exercise (see the Medium
      // regression test above for the "headroom, not a target" property this
      // test does NOT re-test).
      const beatA = runCli(['--orchestrator-id=orch-contended-a', '--desired-agents=4'], env);
      expect(beatA.status).toBe(0);
      const parsedA = JSON.parse(beatA.stdout);
      expect(parsedA.type).toBe('spawn-allowed');
      expect(parsedA.allowance).toBe(4);
      expect((await readRawBucketEntry()).tokens).toBe(2);

      // B: A's dibs entry is already live, so B is genuinely contended —
      // dibs division bounds B's own share to 0 (the whole cap is already
      // claimed by A). B's beat must not draw the bucket at all: it stays at
      // 2, unchanged from after A's beat.
      const beatB = runCli(['--orchestrator-id=orch-contended-b', '--desired-agents=3'], env);
      expect(beatB.status).toBe(0);
      const parsedB = JSON.parse(beatB.stdout);
      expect(parsedB.type).toBe('spawn-allowed');
      expect(parsedB.allowance).toBe(0);
      expect((await readRawBucketEntry()).tokens).toBe(2);
    },
  );
});

// ---------------------------------------------------------------------------
// Phase 3 — bounded retry on a transient collect() failure.
//
// RED by construction: cli.mjs does not yet retry a failing `realCollect()`.
// Today a single throw from `realCollect()` is swallowed by
// `./lib/recon.mjs`'s `takeSample()` into `{ stale: true, ... }`, which
// `./lib/threshold.mjs`'s classifiers deterministically treat as AMBER on
// BOTH axes ("memory/disk sample is stale/unknown; treating as degraded
// (AMBER), never assumed healthy") — never a crash, but also never a real
// recovery. These tests prove the target contract instead: cli.mjs retries
// the sample once immediately on a transient failure before giving up,
// propagating to the existing top-level `main().catch(...)` exit-1 stderr
// path only after 2 consecutive failures.
//
// Test seam this file relies on (added to cli.mjs purely as test
// infrastructure — no retry logic is implemented alongside it):
//
//   ARM_FORCE_COLLECT_FAILURES=<n>   (env, test-only)
//     Only consulted when ARM_FAKE_COLLECT_JSON is NOT set. The first <n>
//     invocations of realCollect() within a single cli.mjs process throw a
//     synthetic transient Error instead of shelling out to
//     vm_stat/sysctl/df; the (n+1)th invocation returns a fixed,
//     deterministic canned GREEN-shaped reading (never the real OS
//     commands — this keeps the test portable to non-macOS CI runners,
//     matching this file's existing `ARM_FAKE_COLLECT_JSON` philosophy of
//     never touching the real machine). Exists specifically because
//     `execFileSync` itself isn't mockable end-to-end from a black-box
//     child-process test.
// ---------------------------------------------------------------------------

describe('cli.mjs bounded retry on transient collect() failure', () => {
  async function readEntry(filePath, orchestratorId) {
    const raw = await readFile(filePath, 'utf8');
    const entries = JSON.parse(raw);
    return entries.filter((entry) => entry.orchestratorId === orchestratorId);
  }

  test('first collect() attempt throws, retry succeeds: a normal traffic-light is printed, no error surfaced', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-retry-1'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FORCE_COLLECT_FAILURES: '1',
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');

    let parsed;
    expect(() => {
      parsed = JSON.parse(stdout);
    }).not.toThrow();
    // The canned post-retry reading (FORCED_COLLECT_SUCCESS_READING) is
    // GREEN-shaped on both axes for a fresh orchestrator with no prior
    // history — a working retry recovers to a normal spawn-allowed light,
    // not the deterministic stale-AMBER `hold` a swallowed single failure
    // produces today.
    expect(parsed.type).toBe('spawn-allowed');
  });

  test('both attempts throw: the existing exit-1 stderr message is preserved unchanged, no infinite retry', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-retry-2'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FORCE_COLLECT_FAILURES: '2',
      },
    );

    expect(status).toBe(1);
    expect(stdout).toBe('');
    // Same format main().catch() already uses for every other unexpected
    // error — this feature must not invent a new stderr shape.
    expect(stderr).toMatch(/^agent-resource-management: unexpected error: /);
  });

  test('ARM_FAKE_COLLECT_JSON bypasses the retry wrapper entirely: valid JSON is unaffected by a forced-failure budget', () => {
    const fakeJson = JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN });
    const withoutBudget = runCli(
      ['--orchestrator-id=orch-retry-3a'],
      { ARM_COORDINATION_FILE: coordinationFilePath, ARM_FAKE_COLLECT_JSON: fakeJson },
    );
    const withBudget = runCli(
      ['--orchestrator-id=orch-retry-3b'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: fakeJson,
        ARM_FORCE_COLLECT_FAILURES: '5',
      },
    );

    expect(withBudget.status).toBe(withoutBudget.status);
    expect(withBudget.stderr).toBe(withoutBudget.stderr);
    // Only the orchestrator id differs between the two payloads' `allowance`
    // computation inputs, not the traffic-light type itself.
    expect(JSON.parse(withBudget.stdout).type).toBe(JSON.parse(withoutBudget.stdout).type);
  });

  test('ARM_FAKE_COLLECT_JSON bypasses the retry wrapper entirely: a SyntaxError is unaffected by a forced-failure budget', () => {
    const malformedJson = '{ not valid json';
    const withoutBudget = runCli(
      ['--orchestrator-id=orch-retry-4a'],
      { ARM_COORDINATION_FILE: coordinationFilePath, ARM_FAKE_COLLECT_JSON: malformedJson },
    );
    const withBudget = runCli(
      ['--orchestrator-id=orch-retry-4b'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: malformedJson,
        ARM_FORCE_COLLECT_FAILURES: '5',
      },
    );

    // Whatever cli.mjs does with an unparseable ARM_FAKE_COLLECT_JSON (today:
    // takeSample() swallows the SyntaxError into a stale/degraded reading and
    // still exits 0), a forced-failure budget alongside it must make no
    // difference — proving the fake-JSON seam short-circuits before
    // realCollect(), so the budget is never even consulted.
    expect(withBudget.status).toBe(withoutBudget.status);
    expect(withBudget.stderr).toBe(withoutBudget.stderr);
  });

  test('a retry never results in more than one declareDibs write for a single cli.mjs invocation', async () => {
    const orchestratorId = 'orch-retry-5';
    const { status } = runCli(
      ['--orchestrator-id=' + orchestratorId],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FORCE_COLLECT_FAILURES: '1',
      },
    );
    expect(status).toBe(0);

    const matchingEntries = await readEntry(coordinationFilePath, orchestratorId);
    // Exactly one ledger entry for this orchestrator — no duplicate/second
    // entry from an interim declareDibs call made before the retry resolved.
    expect(matchingEntries).toHaveLength(1);
    // The persisted history reflects the retry's successful (canned GREEN)
    // reading, not the deterministic stale-AMBER signature a swallowed
    // single failure would have written (`{ consecutiveGreen: 0, tripped:
    // true, level: 'AMBER' }`) — proof the single write that did land is the
    // POST-retry outcome, not a stale interim one.
    // Phase 5 () additive fields: classifyMemoryAxis/classifyDiskAxis
    // now also populate swapReadings/declineRateReadings rolling windows on
    // every call, from FORCED_COLLECT_SUCCESS_READING's own swapUsedMb/
    // declineRateGbPerHour values — the pre-Phase-5 assertion is extended
    // additively rather than dropped, so this test still pins the exact
    // post-retry history shape.
    expect(matchingEntries[0].memoryHistory).toEqual({
      consecutiveGreen: 1,
      tripped: false,
      level: 'GREEN',
      swapReadings: [{ value: 1024, sampledAt: expect.any(Number) }],
    });
    // Phase 6: `declineRateReadings` is now fed the REAL computed
    // disk-decline trend (see ./disk-trend.jest.spec.mjs), not
    // FORCED_COLLECT_SUCCESS_READING's own raw `declineRateGbPerHour: 0.5` —
    // this is this orchestrator's first-ever beat on a fresh coordination
    // file, so there is no prior disk sample to derive a genuine rate from,
    // and the honest "insufficient-data" value is `0`, not a pass-through of
    // the fixture's own reading.
    expect(matchingEntries[0].diskHistory).toEqual({
      consecutiveGreen: 1,
      tripped: false,
      level: 'GREEN',
      declineRateReadings: [{ value: 0, sampledAt: expect.any(Number) }],
    });
  });

  test('applies identically with --heartbeat set: a simulated transient failure that succeeds on retry still prints a normal light', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-retry-6', '--heartbeat'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FORCE_COLLECT_FAILURES: '1',
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('spawn-allowed');
  });
});

// ---------------------------------------------------------------------------
// Phase 4 — runtime behavior guard (characterization test).
//
// THROTTLE/CLEANUP/ALERT are documented-only at the LIBRARY level as of
// Phase 2 — `buildTrafficLight` (lib/allowance.mjs) never produces
// `{ type: 'throttle' | 'cleanup' | 'alert' }` for any input, confirmed by
// Phase 2's own regression tests. EVICT was excluded entirely in Phase 3 (no
// literal exists at all). Since cli.mjs only ever forwards whatever
// `buildTrafficLight` returns (subject to the existing pause->hold masking),
// none of these four literals can ever reach stdout today.
//
// This is a CHARACTERIZATION test, not a red one: it is expected to PASS
// already, proving the current (and Phase 4's still-current) runtime
// behavior that the doc updates above describe.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Phase 2 — wire real running-agent discovery into cli.mjs.
//
// RED by construction: cli.mjs still hardcodes `const runningAgents = []` and
// still carries the HIGH-5 pause->hold downgrade rewrite (search cli.mjs for
// "HIGH-5"). These tests pin the TARGET contract: a new `ARM_FAKE_PS_OUTPUT`
// test seam (mirroring `ARM_FAKE_COLLECT_JSON`) feeds `ps`-shaped text into
// `./lib/recon.mjs`'s `listAgentProcesses`, whose output becomes the real
// `runningAgents` fed to `selectPauseCandidate` on a RED beat — and the
// HIGH-5 downgrade block is REMOVED, so a RED beat with genuinely zero
// running agents now emits an honest `{ type: 'pause', pauseCandidate: null
// }` rather than `{ type: 'hold' }`.
//
// Fixture shape mirrors ./lib/recon-per-agent.jest.spec.mjs's
// `listAgentProcesses` fixtures exactly: `ps -Ao pid,ppid,rss,etime,comm`-
// style text, column order pid/ppid/rss(KB)/etime([[dd-]hh:]mm:ss)/comm — the
// BSD/macOS-portable `etime` keyword (NOT the Linux/procps-only `etimes`,
// which macOS's real `ps` rejects; see cli.mjs's collectPsOutput() and
// Phase 2's Critical fix). A claude-rooted tree is any process whose own
// `comm` ends in `/claude` (case-insensitive), with every transitive
// descendant (by parent-process ancestry) belonging to that root's tree.
// ---------------------------------------------------------------------------

const PS_ONE_AGENT = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001  51200       58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;
// Root 9001's tree: (102400 + 51200) KB = 153600 KB = 150 MB.

const PS_TWO_AGENTS_DIFFERENT_RSS = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001  51200       58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
 9101   501 204800    02:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9102  9101  20480    01:58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;
// Root 9001's tree: 150 MB. Root 9101's tree: (204800 + 20480) KB = 225280
// KB = 220 MB — the higher-RSS tree, so `selectPauseCandidate`'s
// `highest-rss` policy must pick agentId '9101'.

const PS_IDLE_NO_AGENTS = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
  700     1    768 1-03:38:20 /usr/sbin/mDNSResponder
`;

describe('cli.mjs real running-agent discovery on a RED beat', () => {
  test('names a real pauseCandidate (not null, not hold) when exactly one claude-rooted tree is running', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-arm-a'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: PS_ONE_AGENT,
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');

    let parsed;
    expect(() => {
      parsed = JSON.parse(stdout);
    }).not.toThrow();

    expect(parsed.type).toBe('pause');
    expect(parsed.pauseCandidate).not.toBeNull();
    expect(parsed.pauseCandidate.agentId).toBe('9001');
    expect(parsed.pauseCandidate.rssMb).toBeCloseTo(150, 5);
    expect(typeof parsed.pauseCandidate.startedAt).toBe('number');
    expect(Number.isFinite(parsed.pauseCandidate.startedAt)).toBe(true);
  });

  test('picks the higher-RSS tree as pauseCandidate when two claude-rooted trees are running', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-arm-b'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: PS_TWO_AGENTS_DIFFERENT_RSS,
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');

    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('pause');
    expect(parsed.pauseCandidate).not.toBeNull();
    expect(parsed.pauseCandidate.agentId).toBe('9101');
    expect(parsed.pauseCandidate.rssMb).toBeCloseTo(220, 5);
  });

  test('emits an HONEST pause/null (not the old hold downgrade) when zero claude-rooted trees are running', () => {
    // This is the test that proves the HIGH-5 rewrite block was actually
    // REMOVED from cli.mjs, not merely bypassed by non-empty ps output in the
    // other two tests above: with real (empty) running-agent data, a RED
    // beat's honest answer is `pause` with `pauseCandidate: null` — a
    // legitimate "nothing to pause" signal, not the placeholder `hold`
    // downgrade that existed only because running-agent data was never wired
    // in at all.
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-arm-c'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: PS_IDLE_NO_AGENTS,
        // Phase 5: deterministic so this exact-shape assertion below
        // isn't at the mercy of this host's real, constantly-shifting
        // os.loadavg() reading.
        ARM_FAKE_LOAD_AVERAGE_JSON: JSON.stringify([0, 0, 0]),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');

    const parsed = JSON.parse(stdout);
    // Phase 6: this orchestrator's first-ever beat on a fresh
    // coordination file carries no prior disk sample, so `diskTrend` is
    // deterministically "insufficient-data" — see ./disk-trend.jest.spec.mjs.
    expect(parsed).toEqual({
      type: 'pause',
      pauseCandidate: null,
      diskTrend: { trend: 'insufficient-data', declineRateGbPerHour: 0 },
      // Phase 3 (Option B): the live-agent ledger reservation is only
      // ever attempted on a beat that could reach `spawn-allowed` with
      // non-zero headroom — a RED (pause) beat never attempts it.
      liveAgentGrant: null,
      // Phase 5: purely observational, recorded regardless of verdict.
      loadAverage: { oneMinute: 0, fiveMinute: 0, fifteenMinute: 0 },
      // Phase 2: record-only observability field, mirrors loadAverage —
      // sourced straight from ARM_FAKE_COLLECT_JSON's MEMORY_RED.compressedMb.
      compressedMb: MEMORY_RED.compressedMb,
      // Phase 3: additive per-pid leak-velocity detection echo, present
      // on every beat regardless of admission verdict; empty here since no
      // footprint-state store entries exist for this test.
      leakBlocks: [],
      // Phase 3: record-only observability field, mirrors compressedMb —
      // MEMORY_RED supplies no compressions/decompressions counters, so this
      // is the deterministic cold-start-with-no-counters shape (0/0 is NaN,
      // which JSON-serialises as `null`).
      memory: {
        compressionVelocity: { coldStart: true, compressionsPerSec: 0, compressionRatio: null },
      },
    });
  });

  // cli.mjs's main() already computes a real pauseCandidate on any
  // memory-RED beat (the `!isAdviseOnly` gate above is memory-state-only —
  // see the "Known residual waste (Phase 4 review, Low)" comment in
  // cli.mjs, which explicitly documents that on a both-RED beat this real
  // value is computed and then handed to buildTrafficLight, which — before
  // — silently discards it and returns bare `{ type: 'alert', reason
  // }`. This is the one integration-level row from the Test-strategy table:
  // it proves the WIRING end-to-end (cli.mjs's main() -> buildTrafficLight
  // -> stdout), not just allowance.jest.spec.mjs's pure-function coverage of
  // buildTrafficLight's shape. RED today: buildTrafficLight has not yet been
  // updated to carry `memoryAlsoRed`/`pauseCandidate` on the both-RED `alert`
  // branch (see allowance.jest.spec.mjs's Phase 1 describe block), so the
  // printed JSON is still the bare `{ type: 'alert', reason, diskTrend,
  // liveAgentGrant, loadAverage, compressedMb, leakBlocks, memory }` shape
  // with no `memoryAlsoRed` and no `pauseCandidate` field at all.
  test('both-RED beat (memory AND disk RED) carries the REAL pauseCandidate + memoryAlsoRed: true on the alert, not a discarded one', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-arm-both-red'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_RED }),
        // A real, single claude-rooted tree running — so pauseCandidate is
        // genuinely non-null once wired through, not vacuously null (which
        // could otherwise pass even with the discard bug fixed the wrong
        // way — see the "absent-key vs null" distinction pinned in
        // allowance.jest.spec.mjs's disk-RED-only test).
        ARM_FAKE_PS_OUTPUT: PS_ONE_AGENT,
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');

    let parsed;
    expect(() => {
      parsed = JSON.parse(stdout);
    }).not.toThrow();

    // Disk-RED still wins the type tie — unchanged here.
    expect(parsed.type).toBe('alert');
    // memory was ALSO RED on this beat, which the bare `alert` type
    // alone cannot distinguish from a disk-RED-only beat.
    expect(parsed.memoryAlsoRed).toBe(true);
    // the REAL candidate main() already computed for this memory-RED
    // beat, not silently discarded.
    expect(parsed.pauseCandidate).not.toBeNull();
    expect(parsed.pauseCandidate.agentId).toBe('9001');
    expect(parsed.pauseCandidate.rssMb).toBeCloseTo(150, 5);
    expect(typeof parsed.pauseCandidate.startedAt).toBe('number');
  });

  test('AMBER beat ignores ARM_FAKE_PS_OUTPUT entirely: still a plain hold with no pauseCandidate', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-arm-d'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_AMBER, disk: DISK_GREEN }),
        // Non-empty running-agent data must never be consulted for a
        // non-RED beat — if it were, this would wrongly leak a
        // pauseCandidate-shaped field onto a `hold` traffic light.
        ARM_FAKE_PS_OUTPUT: PS_TWO_AGENTS_DIFFERENT_RSS,
        // Phase 5: deterministic so this exact-shape assertion below
        // isn't at the mercy of this host's real, constantly-shifting
        // os.loadavg() reading.
        ARM_FAKE_LOAD_AVERAGE_JSON: JSON.stringify([0, 0, 0]),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');

    const parsed = JSON.parse(stdout);
    // Phase 6: this orchestrator's first-ever beat on a fresh
    // coordination file carries no prior disk sample, so `diskTrend` is
    // deterministically "insufficient-data" — see ./disk-trend.jest.spec.mjs.
    expect(parsed).toEqual({
      type: 'hold',
      diskTrend: { trend: 'insufficient-data', declineRateGbPerHour: 0 },
      // Phase 3 (Option B): AMBER never reaches the live-agent ledger
      // reservation attempt either — same reasoning as the pause/RED case
      // above.
      liveAgentGrant: null,
      // Phase 5: purely observational, recorded regardless of verdict.
      loadAverage: { oneMinute: 0, fiveMinute: 0, fifteenMinute: 0 },
      // Phase 2: record-only observability field, mirrors loadAverage —
      // sourced straight from ARM_FAKE_COLLECT_JSON's MEMORY_AMBER.compressedMb.
      compressedMb: MEMORY_AMBER.compressedMb,
      // Phase 3: additive per-pid leak-velocity detection echo, present
      // on every beat regardless of admission verdict; empty here since no
      // footprint-state store entries exist for this test.
      leakBlocks: [],
      // Phase 3: record-only observability field, mirrors compressedMb —
      // MEMORY_AMBER supplies no compressions/decompressions counters, so this
      // is the deterministic cold-start-with-no-counters shape (0/0 is NaN,
      // which JSON-serialises as `null`).
      memory: {
        compressionVelocity: { coldStart: true, compressionsPerSec: 0, compressionRatio: null },
      },
    });
  });

  test('GREEN beat ignores ARM_FAKE_PS_OUTPUT entirely: still spawn-allowed with a numeric allowance', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-arm-e'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: PS_TWO_AGENTS_DIFFERENT_RSS,
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');

    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('spawn-allowed');
    expect(typeof parsed.allowance).toBe('number');
    expect(parsed).not.toHaveProperty('pauseCandidate');
  });

  // Scenario 6 (ARM_FAKE_PS_OUTPUT unset on a RED beat, exercising the real
  // `ps` shell-out path) is deliberately NOT covered here: this file never
  // touches the real machine (see the file-level doc comment) and `ps` is as
  // unmockable end-to-end from a black-box spawnSync test as
  // vm_stat/sysctl/df already are for `collect()` — there is no test-safe way
  // to assert on real host process-tree contents without flaking on CI
  // machines that may or may not have any claude-rooted process running. The
  // graceful-absence behavior this would otherwise pin (no crash, valid JSON,
  // exit 0) is already covered indirectly by every OTHER test file in this
  // suite that never sets ARM_FAKE_PS_OUTPUT on a non-RED beat, and by
  // agent-resource-management.jest.spec.mjs's coverage of
  // listAgentProcesses/selectPauseCandidate directly at the unit level.
});

describe('cli.mjs runtime characterization: throttle/cleanup/evict are never observed on stdout for any synthetic GREEN/AMBER/RED combination; alert is observed exactly on disk-RED (Phase 4, updated Phase 4)', () => {
  // UPDATED: `alert` is no longer in the never-produced set —
  // disk-RED now genuinely produces `{ type: 'alert', ... }` (see
  // ./lib/allowance.jest.spec.mjs's Phase 4 describe block and cli.mjs's own
  // updated header comment). `throttle`/`cleanup`/`evict` remain fully
  // unreachable — `buildTrafficLight` itself never produces any of the three
  // for any input, unchanged by this ticket.
  const MEMORY_BY_STATE = { GREEN: MEMORY_GREEN, AMBER: MEMORY_AMBER, RED: MEMORY_RED };
  const DISK_BY_STATE = { GREEN: DISK_GREEN, AMBER: DISK_AMBER, RED: DISK_RED };
  const NEVER_PRODUCED_TYPES = ['throttle', 'cleanup', 'evict'];

  for (const [memoryLabel, memory] of Object.entries(MEMORY_BY_STATE)) {
    for (const [diskLabel, disk] of Object.entries(DISK_BY_STATE)) {
      test(`memory=${memoryLabel} disk=${diskLabel}: stdout .type is only ever spawn-allowed/hold/pause/alert, never throttle/cleanup/evict`, () => {
        const { status, stdout, stderr } = runCli(
          [`--orchestrator-id=orch-char-${memoryLabel}-${diskLabel}`],
          {
            ARM_COORDINATION_FILE: coordinationFilePath,
            ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory, disk }),
          },
        );

        expect(status).toBe(0);
        expect(stderr).toBe('');

        let parsed;
        expect(() => {
          parsed = JSON.parse(stdout);
        }).not.toThrow();

        // Disk-RED (regardless of memory state) now resolves to `alert`,
        // winning the tie over memory-RED's `pause` — see
        // buildTrafficLight's Phase 4 precedence.
        const expectedType = diskLabel === 'RED' ? 'alert' : ['spawn-allowed', 'hold', 'pause'];
        if (Array.isArray(expectedType)) {
          expect(expectedType).toContain(parsed.type);
        } else {
          expect(parsed.type).toBe(expectedType);
        }
        expect(NEVER_PRODUCED_TYPES).not.toContain(parsed.type);
      });
    }
  }
});

// ---------------------------------------------------------------------------
// Phase 5 — "dibs constrain allowance", black-box CLI level.
//
// RED by construction: cli.mjs's main() always hands `computeAllowance` (and
// therefore `buildTrafficLight`) the FLAT per-axis cap (`LIVE_AGENT_CEILING`,
// 4 as of Phase 3 — previously `DEFAULT_ALLOWANCE_CONFIG`'s 3, per the
// constant declared near the top of cli.mjs) regardless of
// how many other orchestrators have live dibs on the SAME coordination file —
// two contending orchestrators can each be told the full flat cap
// simultaneously. These tests pin the target contract: once Phase 5 wires
// `computePerOrchestratorAllowance` (see ./lib/allowance.jest.spec.mjs's
// header comment for the exact pinned division algorithm) into cli.mjs in
// place of the flat cap, a beat's reported `allowance` reflects only ITS OWN
// bounded share of the cap once another orchestrator is genuinely contending
// for it — while a lone orchestrator (no contention at all) is unaffected,
// still receiving the full flat cap as headroom.
// ---------------------------------------------------------------------------

describe('cli.mjs dibs constrain the reported allowance across contending orchestrators', () => {
  test(
    'two sequential invocations, DIFFERENT --orchestrator-id, SAME coordination file, both GREEN, both declaring ' +
      '--desired-agents that together meet the per-axis cap (4): neither reports the full flat cap while both are live',
    () => {
      const env = {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      };

      // First orchestrator declares dibs on 2 agents.
      const first = runCli(['--orchestrator-id=orch-dibs-a', '--desired-agents=2'], env);
      expect(first.status).toBe(0);
      expect(first.stderr).toBe('');
      const firstParsed = JSON.parse(first.stdout);
      expect(firstParsed.type).toBe('spawn-allowed');

      // Second, DIFFERENT orchestrator declares dibs on 2 agents too, on the
      // SAME coordination file — combined desire (2 + 2 = 4) meets the
      // per-axis cap of 4 exactly, leaving the priority-ordered second
      // claimant genuinely bounded below the full cap (it can only ever
      // claim what `first`'s earlier-priority claim left remaining).
      const second = runCli(['--orchestrator-id=orch-dibs-b', '--desired-agents=2'], env);
      expect(second.status).toBe(0);
      expect(second.stderr).toBe('');
      const secondParsed = JSON.parse(second.stdout);
      expect(secondParsed.type).toBe('spawn-allowed');

      // `first`'s beat ran while it was the ONLY live entry — genuinely
      // uncontended at that moment — so it correctly receives the full flat
      // cap (4, LIVE_AGENT_CEILING) as headroom, exactly like the solo
      // non-regression test below (Medium fix: the global spawn-rate
      // bucket must not clamp this down to the smaller amount it happened to
      // request — see cli-two-orchestrators.jest.spec.mjs test 3 for the same
      // property at the bucket layer). `second`'s beat runs once `first`'s
      // entry is already live, so it IS genuinely contended and its share
      // must be strictly below the full flat cap.
      const FLAT_CAP = 4;
      expect(firstParsed.allowance).toBe(FLAT_CAP);
      expect(secondParsed.allowance).toBeLessThanOrEqual(FLAT_CAP);
      expect(secondParsed.allowance).toBeLessThan(FLAT_CAP);
    },
  );

  test(
    'a single orchestrator invocation (only one --orchestrator-id ever declared in this coordination file) ' +
      'still gets the full flat cap as headroom [non-regression]',
    () => {
      // --desired-agents is set ABOVE the per-axis cap (4) so the axis-cap
      // division (this test's actual target) is the binding constraint, not
      // the independent global spawn-rate token bucket (Phase 4, see
      // DEFAULT_SPAWN_BUCKET_CONFIG's 10-token default capacity) — a
      // --desired-agents value at or below the cap would be indistinguishable
      // from that separate, already-existing clamp and wouldn't isolate what
      // THIS test is pinning.
      const { status, stdout, stderr } = runCli(
        ['--orchestrator-id=orch-dibs-solo', '--desired-agents=5'],
        {
          ARM_COORDINATION_FILE: coordinationFilePath,
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        },
      );

      expect(status).toBe(0);
      expect(stderr).toBe('');
      const parsed = JSON.parse(stdout);
      expect(parsed.type).toBe('spawn-allowed');
      // Lone orchestrator, no contention: the full flat per-axis cap (4,
      // LIVE_AGENT_CEILING), NOT clamped down to its own desiredAgents (5,
      // itself above the cap) — headroom, not a target, exactly as today.
      expect(parsed.allowance).toBe(4);
    },
  );

  // Targeted regression test for the High finding fixed alongside
  // Phase 5's real spawn checks: a `--heartbeat` beat's OWN dibs entry must
  // NOT count as real contention in `computePerOrchestratorAllowance`.
  // Unlike the global spawn-bucket draw (already correctly zeroed for
  // heartbeats above), a heartbeat's `desiredAgents` defaulting to `1` (no
  // `--desired-agents` passed) previously landed a live, non-zero dibs entry
  // that a SECOND, genuinely-contending orchestrator's allowance was then
  // divided against — reducing a real contending orchestrator's share purely
  // because another orchestrator polled a zero-intent heartbeat.
  //
  // NOTE (Medium finding, fixed alongside this same ticket): this test alone
  // cannot discriminate the closely-related BRANCH-SELECTION bug in
  // `computePerOrchestratorAllowance` — a live zero-`desiredAgents` entry
  // (a heartbeat's own dibs entry, or any other orchestrator's) flipping an
  // otherwise-solo real orchestrator from the uncontended/headroom branch
  // into the contended/clamped-to-ask branch. `orch-hb-regress-b` here uses
  // `--desired-agents=5`, ABOVE the flat cap (4), so `min(5, 4) === 4` on
  // BOTH the correct (solo/headroom) and buggy (contended/clamped) code
  // paths — the two branches coincide at this input and this test can't
  // tell them apart. See the discriminating regression test below
  // ('a live zero-desiredAgents heartbeat entry does not clamp...'), which
  // uses a real ask AT OR BELOW the cap specifically to expose that gap.
  test(
    'a --heartbeat beat (no --desired-agents) does not reduce a SEPARATE, genuinely-contending orchestrator\'s ' +
      'next allowance [regression]',
    () => {
      const env = {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      };
      const FLAT_CAP = 4;

      // Orchestrator B, alone, declaring desired-agents ABOVE the cap: gets
      // the full flat cap as headroom (uncontended, per the non-regression
      // test above).
      const beatB1 = runCli(['--orchestrator-id=orch-hb-regress-b', '--desired-agents=5'], env);
      expect(beatB1.status).toBe(0);
      expect(beatB1.stderr).toBe('');
      const parsedB1 = JSON.parse(beatB1.stdout);
      expect(parsedB1.type).toBe('spawn-allowed');
      expect(parsedB1.allowance).toBe(FLAT_CAP);

      // Orchestrator A sends ONE --heartbeat beat, deliberately omitting
      // --desired-agents (defaults to 1) — a heartbeat carries zero real
      // spawn intent per SKILL.md's documented contract.
      const heartbeatA = runCli(['--orchestrator-id=orch-hb-regress-a', '--heartbeat'], env);
      expect(heartbeatA.status).toBe(0);
      expect(heartbeatA.stderr).toBe('');
      expect(JSON.parse(heartbeatA.stdout).type).toBe('spawn-allowed');

      // Orchestrator B's NEXT real beat must still get the FULL cap,
      // unaffected by A's intervening heartbeat — today (before the fix),
      // A's phantom desiredAgents:1 dibs entry participates as genuine
      // contention and B's share is wrongly reduced below FLAT_CAP.
      const beatB2 = runCli(['--orchestrator-id=orch-hb-regress-b', '--desired-agents=5'], env);
      expect(beatB2.status).toBe(0);
      expect(beatB2.stderr).toBe('');
      const parsedB2 = JSON.parse(beatB2.stdout);
      expect(parsedB2.type).toBe('spawn-allowed');
      expect(parsedB2.allowance).toBe(FLAT_CAP);
    },
  );
});

// ---------------------------------------------------------------------------
// Phase 3 — freshness-window sample reuse.
//
// RED by construction: cli.mjs's collect() short-circuits on
// ARM_FAKE_COLLECT_JSON and otherwise always calls realCollect() — there is
// no reserved-entry read/write of a shared sample via the coordination file
// yet, and no fallthrough to reuse a fresh stored sample when
// ARM_FAKE_COLLECT_JSON is absent. These tests pin the TARGET contract from
// the task brief: main() checks isSampleFresh against a
// '__shared-machine-sample__' reserved entry (declared/read via
// declareDibs/readDibs) BEFORE calling collect() — reusing it when fresh
// (within DEFAULT_FRESHNESS_WINDOW_MS, 90s), or sampling fresh and
// overwriting the stored entry when stale or absent.
// ---------------------------------------------------------------------------

describe('cli.mjs freshness-window shared-sample reuse', () => {
  test(
    'a second invocation (different --orchestrator-id, same coordination file) with NO ARM_FAKE_COLLECT_JSON ' +
      'reuses the first invocation\'s fresh stored sample: same classification, exit 0',
    () => {
      // First invocation: a real, injected RED sample. Deliberately RED (not
      // GREEN) so this test discriminates reliably regardless of the real
      // test host's actual, unmocked resource state: a real macOS CI/dev
      // machine reading RED (kernel memory-pressure critical AND <10GB free
      // disk) is implausible, so if the second invocation below falls through
      // to the real machine instead of reusing this stored sample, it will
      // almost certainly NOT report 'alert' — making this test fail (RED) by
      // construction today, and pass once the freshness-window reuse
      // correctly avoids ever consulting the real machine on this beat. This
      // is expected to (once Phase 3 lands) store a
      // '__shared-machine-sample__' reserved entry into the coordination file
      // via declareDibs.
      //
      // UPDATED: both memory AND disk RED now resolves to
      // 'alert' (disk-RED wins the tie over memory-RED's 'pause') — see
      // buildTrafficLight's Phase 4 precedence.
      const first = runCli(
        ['--orchestrator-id=orch-fresh-a'],
        {
          ARM_COORDINATION_FILE: coordinationFilePath,
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_RED }),
        },
      );
      expect(first.status).toBe(0);
      const firstParsed = JSON.parse(first.stdout);
      expect(firstParsed.type).toBe('alert');

      // Second invocation: a DIFFERENT orchestrator, no ARM_FAKE_COLLECT_JSON
      // at all — if the freshness-window reuse is wired in, this must reuse
      // the first invocation's stored sample (well within
      // DEFAULT_FRESHNESS_WINDOW_MS's 90s) rather than falling back to the
      // real machine (which would be a flaky, environment-dependent
      // classification, and virtually certain NOT to be RED on a real
      // dev/CI host) or degrading to a stale/AMBER reading.
      const second = runCli(
        ['--orchestrator-id=orch-fresh-b'],
        { ARM_COORDINATION_FILE: coordinationFilePath },
      );

      expect(second.status).toBe(0);
      let secondParsed;
      expect(() => {
        secondParsed = JSON.parse(second.stdout);
      }).not.toThrow();
      expect(secondParsed.type).toBe(firstParsed.type);
      expect(secondParsed.type).toBe('alert');
    },
  );

  test(
    'a stale stored shared sample (sampledAt far in the past) is ignored: a second, fresh ARM_FAKE_COLLECT_JSON ' +
      'reading wins instead',
    async () => {
      // Seed the coordination file directly with a reserved shared-sample
      // entry whose sampledAt is already far outside
      // DEFAULT_FRESHNESS_WINDOW_MS (90s) — simulating "the first invocation
      // happened a long time ago", without an actual real-time delay in this
      // test.
      const longAgo = Date.now() - 10 * 60 * 1000; // 10 minutes ago
      await declareDibs(coordinationFilePath, {
        orchestratorId: '__shared-machine-sample__',
        sampledAt: longAgo,
        reading: { memory: MEMORY_GREEN, disk: DISK_GREEN },
        declaredAt: longAgo,
      });

      // A fresh, DIFFERENT (RED-classifying) sample supplied via
      // ARM_FAKE_COLLECT_JSON for this invocation. If the stale stored sample
      // were wrongly reused, this invocation would report the stored GREEN
      // classification instead of the fresh RED one supplied here.
      const { status, stdout, stderr } = runCli(
        ['--orchestrator-id=orch-fresh-c'],
        {
          ARM_COORDINATION_FILE: coordinationFilePath,
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
        },
      );

      expect(status).toBe(0);
      expect(stderr).toBe('');
      const parsed = JSON.parse(stdout);
      // A genuinely fresh sample was taken (not the stale stored GREEN one) —
      // proven by the RED-driven `pause` classification winning.
      expect(parsed.type).toBe('pause');
    },
  );

  // Scenario 7 from the task brief (a dibs-array-consuming code path in
  // cli.mjs must never let the reserved sample pseudo-entry pollute
  // per-orchestrator bookkeeping) is hard to assert purely black-box through
  // spawnSync's stdout/stderr surface — computeAllowance's `priorityOrder` is
  // an internal detail of the `allowance` traffic-light field this file
  // already asserts is just `{ amount, constrainedBy, priorityOrder }`-shaped
  // via `typeof parsed.allowance === 'number'`, with no exposed way from here
  // to inspect priorityOrder's contents directly. This invariant is instead
  // covered precisely at the unit level in
  // ./lib/coordination-file.jest.spec.mjs's "readDibs output can be filtered
  // down to just the normal orchestrator entries via isReservedEntry" test —
  // this black-box CLI suite defers to that unit-level coverage rather than
  // duplicating it indirectly and less precisely here.
});

// ---------------------------------------------------------------------------
// Phase 6 — wall-clock hysteresis recovery, black-box via cli.mjs.
//
// RED by construction: cli.mjs's DEFAULT_HYSTERESIS_CONFIG carries only
// `hysteresisRecoverConsecutiveGreen` today, with no `hysteresisRecoverWindowMs`
// and no `now` threaded into classifyMemoryAxis/classifyDiskAxis — recovery
// is purely count-based, so sequential invocations recover exactly as fast
// as an orchestrator can spawn child processes, regardless of real elapsed
// time. This is the SAME invariant cli-two-orchestrators.jest.spec.mjs's
// test 5 already pins across two DIFFERENT orchestrators sharing one
// coordination file; this describe block adds a SAME-orchestrator variant
// (sequential beats from one --orchestrator-id, which is the more common
// real-world shape — one orchestrator polling this CLI once per completed
// sub-agent task) plus a RED (not just AMBER) trip, which the outer test
// does not exercise.
// ---------------------------------------------------------------------------

describe('cli.mjs wall-clock hysteresis recovery', () => {
  test(
    'same --orchestrator-id, sequential beats: a RED trip followed immediately by 3 rapid-fire GREEN beats ' +
      'does not yet report spawn-allowed',
    () => {
      const orchestratorId = 'orch-wallclock-red';
      const env = { ARM_COORDINATION_FILE: coordinationFilePath };

      // Trip the memory axis to RED.
      const trip = runCli(
        ['--orchestrator-id=' + orchestratorId],
        { ...env, ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }) },
      );
      expect(trip.status).toBe(0);
      expect(JSON.parse(trip.stdout).type).toBe('pause');

      // Three back-to-back GREEN beats from the SAME orchestrator, fired as
      // fast as this test can spawn child processes — real milliseconds
      // apart, far short of any believable recovery window measured in
      // seconds/minutes of sustained health.
      let lastParsed;
      for (let i = 0; i < 3; i += 1) {
        const beat = runCli(
          ['--orchestrator-id=' + orchestratorId],
          { ...env, ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }) },
        );
        expect(beat.status).toBe(0);
        lastParsed = JSON.parse(beat.stdout);
      }

      // Before Phase 6, recovery was purely count-based
      // (DEFAULT_HYSTERESIS_CONFIG's hysteresisRecoverConsecutiveGreen: 3,
      // no wall-clock gate at all), so three rapid-fire GREEN beats would
      // have recovered to `spawn-allowed` even though only milliseconds had
      // elapsed since the RED trip. Phase 6 added the second, wall-clock
      // gate (`hysteresisRecoverWindowMs`), so this assertion (recovery must
      // NOT yet have happened) now correctly holds as implemented.
      expect(lastParsed.type).not.toBe('spawn-allowed');
    },
  );
});

describe('cli.mjs resolveNow() ARM_FAKE_NOW_MS empty-string regression', () => {
  test(
    'ARM_FAKE_NOW_MS="" falls back to real Date.now()-based behavior instead of pinning now to epoch 0',
    async () => {
      const orchestratorId = 'orch-empty-fake-now';
      const beforeMs = Date.now();

      const { status, stdout } = runCli(
        ['--orchestrator-id=' + orchestratorId],
        {
          ARM_COORDINATION_FILE: coordinationFilePath,
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
          ARM_FAKE_NOW_MS: '',
          ARM_BEAT_NOW_TEST_MODE: '1',
        },
      );

      const afterMs = Date.now();

      expect(status).toBe(0);
      expect(['spawn-allowed', 'hold', 'pause']).toContain(JSON.parse(stdout).type);

      // Number('') is 0, and Number.isInteger(0) is true — the exact bug
      // this test guards against. If resolveNow() failed to reject an
      // empty string explicitly, this beat's declared `declaredAt` (and any
      // hysteresis history timestamp derived from it) would be pinned to
      // epoch 0 rather than a plausible current-era value, since `collect()`
      // itself is faked here (ARM_FAKE_COLLECT_JSON) — only `now` is under
      // test.
      const raw = await readFile(coordinationFilePath, 'utf8');
      const entries = JSON.parse(raw);
      const entry = entries.find((candidate) => candidate.orchestratorId === orchestratorId);
      expect(entry).toBeDefined();
      expect(entry.declaredAt).toBeGreaterThanOrEqual(beforeMs);
      expect(entry.declaredAt).toBeLessThanOrEqual(afterMs);
    },
  );
});

// ---------------------------------------------------------------------------
// outer-loop RED tests for Phase 0 — grounded against the CURRENT
// master (commit 1033a85c, its full implementation: real running-agent
// discovery, global spawn-rate bucket, dibs-constrained allowance, wall-clock
// hysteresis all already shipped). These two tests assert FUTURE behavior
// that has not implemented yet and must currently FAIL, for the
// documented reasons below — not crash, not error on setup, but fail on the
// specific assertion that names the gap.
// ---------------------------------------------------------------------------

describe('cli.mjs --heartbeat=false rejection (RED)', () => {
  test(
    '--heartbeat=false (malformed via the eqIndex branch) is rejected: exit 2, stderr mentions --heartbeat',
    () => {
      // Today, `--heartbeat=false` is parsed by parseArgs's `eqIndex` branch
      // into `flags.heartbeat = "false"` (the literal STRING "false", not a
      // boolean). main()'s only check is `flags.heartbeat === true`, which a
      // string can never strictly equal, so this beat silently falls through
      // as an ordinary non-heartbeat counting beat and exits 0 — no
      // validation error at all. This test pins the FUTURE contract (reject
      // with exit 2 and a stderr diagnostic naming `--heartbeat`, mirroring
      // this file's existing --orchestrator-id/--desired-agents validation
      // conventions) and must fail against today's cli.mjs.
      const { status, stderr } = runCli(
        ['--orchestrator-id=orch-1076-heartbeat-false', '--heartbeat=false'],
        {
          ARM_COORDINATION_FILE: coordinationFilePath,
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        },
      );

      expect(status).toBe(2);
      expect(stderr).toMatch(/--heartbeat/);
    },
  );
});

// ---------------------------------------------------------------------------
// Phase 1 — full `--heartbeat` boolean-flag validation scope.
//
// Contract (pinned): only bare `--heartbeat` and `--heartbeat=true` are valid
// (both -> boolean true / heartbeat mode). Everything else — `--heartbeat=
// false`, `--heartbeat=1`, `--heartbeat=TRUE` (case-sensitive), and
// `--heartbeat` consuming a following positional value (`--heartbeat maybe`)
// — is malformed and must exit 2 with a stderr message mentioning
// `--heartbeat`, following the exact idiom used for the existing
// `--orchestrator-id`/`--desired-agents` validation. `--orchestrator-id`
// validation (incl. the reserved-id check) still runs first, unchanged.
//
// RED by construction against today's cli.mjs: main() only checks
// `flags.heartbeat === true`, so `--heartbeat=true` (a string) never matches,
// and every other malformed shape silently falls through as an ordinary
// counting beat instead of being rejected.
// ---------------------------------------------------------------------------

describe('cli.mjs --heartbeat boolean-flag validation (Phase 1, RED)', () => {
  async function readCoordinationEntry(filePath, orchestratorId) {
    const raw = await readFile(filePath, 'utf8');
    const entries = JSON.parse(raw);
    return entries.find((candidate) => candidate.orchestratorId === orchestratorId);
  }

  test('--heartbeat=true (string) is coerced to real boolean true: heartbeat mode active (does not advance consecutiveGreen)', async () => {
    const orchestratorId = 'orch-1076-heartbeat-true-coerced';
    const env = { ARM_COORDINATION_FILE: coordinationFilePath, ARM_BEAT_NOW_TEST_MODE: '1' };
    const FAKE_NOW_BASE = 1_700_100_000_000;

    // Beat 1 (real): trip the memory axis to AMBER.
    const beat1 = runCli(
      ['--orchestrator-id=' + orchestratorId],
      {
        ...env,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_AMBER, disk: DISK_GREEN }),
        ARM_FAKE_NOW_MS: String(FAKE_NOW_BASE),
      },
    );
    expect(beat1.status).toBe(0);

    // Beat 2 (real): first GREEN sample since the trip — counted.
    const beat2 = runCli(
      ['--orchestrator-id=' + orchestratorId],
      {
        ...env,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_NOW_MS: String(FAKE_NOW_BASE + 1_000),
      },
    );
    expect(beat2.status).toBe(0);
    let entry = await readCoordinationEntry(coordinationFilePath, orchestratorId);
    expect(entry.memoryHistory).toMatchObject({ consecutiveGreen: 1, tripped: true, level: 'AMBER' });

    // Beat 3 (`--heartbeat=true`, string form): must behave identically to
    // bare `--heartbeat` — heartbeat mode active, must NOT advance
    // consecutiveGreen beyond 1.
    const beat3 = runCli(
      ['--orchestrator-id=' + orchestratorId, '--heartbeat=true'],
      {
        ...env,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_NOW_MS: String(FAKE_NOW_BASE + 2_000),
      },
    );
    expect(beat3.status).toBe(0);
    entry = await readCoordinationEntry(coordinationFilePath, orchestratorId);
    expect(entry.memoryHistory).toMatchObject({ consecutiveGreen: 1, tripped: true, level: 'AMBER' });
  });

  test.each([
    ['--heartbeat=1', ['--orchestrator-id=orch-1076-heartbeat-1', '--heartbeat=1']],
    ['--heartbeat=TRUE (case-sensitive)', ['--orchestrator-id=orch-1076-heartbeat-TRUE', '--heartbeat=TRUE']],
    [
      '--heartbeat maybe (consumes a following positional value)',
      ['--orchestrator-id=orch-1076-heartbeat-maybe', '--heartbeat', 'maybe'],
    ],
  ])('%s is rejected: exit 2, stderr mentions --heartbeat', (_label, args) => {
    const { status, stdout, stderr } = runCli(args, {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    });

    expect(status).toBe(2);
    expect(stdout).toBe('');
    expect(stderr).toMatch(/--heartbeat/);
  });

  test('bare --heartbeat is unchanged (regression pin): still valid, exits 0', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-1076-heartbeat-bare', '--heartbeat'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    expect(() => JSON.parse(stdout)).not.toThrow();
  });

  test('--heartbeat omitted entirely is unchanged (regression pin): default non-heartbeat beat, exits 0', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-1076-heartbeat-omitted'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    expect(() => JSON.parse(stdout)).not.toThrow();
  });

  test('validation order (regression pin): missing --orchestrator-id together with malformed --heartbeat fires the orchestrator-id error first', () => {
    const { status, stdout, stderr } = runCli(
      ['--heartbeat=false'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      },
    );

    expect(status).toBe(2);
    expect(stdout).toBe('');
    expect(stderr).toMatch(/--orchestrator-id/);
    expect(stderr).not.toMatch(/--heartbeat/);
  });
});

describe('cli.mjs disk-RED reading produces alert, not pause (RED)', () => {
  test('a RED-disk / GREEN-memory reading produces {"type":"alert",...} on stdout, not {"type":"pause",...}', () => {
    // Today, buildTrafficLight (./lib/allowance.mjs) only ever produces
    // spawn-allowed / hold / pause — `throttle`/`cleanup`/`alert` are
    // documented-only forward-contract shapes it never actually returns (see
    // cli.mjs's own file-header comment). A disk-RED beat therefore still
    // produces `{ "type": "pause", "pauseCandidate": ... }` on current
    // master, since the earlier HIGH-5 masking (which used to downgrade
    // pause/null to hold) has already been removed now that real
    // running-agent data is wired in. This test pins the FUTURE contract —
    // disk-RED should surface as an `alert`, not a `pause` — and must fail
    // against today's cli.mjs. Only `.type` is asserted; `pauseCandidate`
    // may be a real value or null depending on the live host's actual
    // process tree, so it is deliberately left unasserted here to keep this
    // test independent of that.
    const { status, stdout } = runCli(
      ['--orchestrator-id=orch-1076-disk-red-alert'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: MEMORY_GREEN,
          disk: { freeDiskGb: 1, declineRateGbPerHour: 0 },
        }),
      },
    );

    expect(status).toBe(0);
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('alert');
  });
});

// ---------------------------------------------------------------------------
// Phase 5 — rolling windows (swap-velocity Part B, disk-decline
// -rate Part C) persist across separate cli.mjs process invocations via the
// real coordination file, exactly the same way memoryHistory/diskHistory's
// consecutiveGreen/tripped/level/nonGreenSince already do (see the
// `--heartbeat` describe blocks above for that existing idiom).
//
// RED by construction: neither `lib/threshold.mjs`'s `classifyMemoryAxis`
// nor `classifyDiskAxis` populates a `swapReadings`/`declineRateReadings`
// window on the returned `history` yet — cli.mjs persists whatever
// `memoryResult.history`/`diskResult.history` it's handed unmodified (see
// cli.mjs's `main()`, the `declareDibs` call passing
// `memoryHistory: memoryResult.history, diskHistory: diskResult.history`),
// so today those fields are simply absent from the persisted JSON. No new
// cli.mjs wiring is expected for this phase beyond what's already there —
// once `lib/threshold.mjs` populates the windows, this test should turn
// green with zero cli.mjs changes, proving the plumbing was already
// generic enough to carry them through.
// ---------------------------------------------------------------------------

describe('cli.mjs rolling windows (swap-readings / decline-rate-readings) persist across two process invocations (Phase 5)', () => {
  test('a second cli.mjs invocation with the same --orchestrator-id and coordination file reflects BOTH beats\' contributions in memoryHistory.swapReadings and diskHistory.declineRateReadings', async () => {
    const orchestratorId = 'orch-1076-phase5-window-persistence';
    // resolveNow() now also requires ARM_BEAT_NOW_TEST_MODE='1' before
    // honouring ARM_FAKE_NOW_MS.
    const env = { ARM_COORDINATION_FILE: coordinationFilePath, ARM_BEAT_NOW_TEST_MODE: '1' };

    const beat1 = runCli(
      ['--orchestrator-id=' + orchestratorId],
      {
        ...env,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: { pressureLevel: 1, swapUsedMb: 3900, compressedMb: 512 },
          disk: { freeDiskGb: 60, declineRateGbPerHour: 5 },
        }),
        ARM_FAKE_NOW_MS: '1700000000000',
      },
    );
    expect(beat1.status).toBe(0);

    const beat2 = runCli(
      ['--orchestrator-id=' + orchestratorId],
      {
        ...env,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: { pressureLevel: 1, swapUsedMb: 2000, compressedMb: 512 },
          disk: { freeDiskGb: 58, declineRateGbPerHour: 6 },
        }),
        ARM_FAKE_NOW_MS: '1700000010000',
      },
    );
    expect(beat2.status).toBe(0);

    const raw = await readFile(coordinationFilePath, 'utf8');
    const entries = JSON.parse(raw);
    const entry = entries.find((candidate) => candidate.orchestratorId === orchestratorId);
    expect(entry).toBeDefined();

    // Both beats' own readings must be reflected in the persisted window —
    // proving the second invocation's window is built on top of what the
    // FIRST invocation (a separate `node cli.mjs` process) already wrote to
    // the real coordination file, not merely in-memory within one process.
    expect(Array.isArray(entry.memoryHistory.swapReadings)).toBe(true);
    const swapValues = entry.memoryHistory.swapReadings.map((reading) => reading.value);
    expect(swapValues).toContain(3900);
    expect(swapValues).toContain(2000);

    // Phase 6: `declineRateReadings` is now fed the REAL computed
    // disk-decline trend (see ./disk-trend.jest.spec.mjs), not each beat's
    // own raw fixture `declineRateGbPerHour` (5, 6 — deliberately decoy
    // values in the fixtures above). Beat 1 is this orchestrator's
    // first-ever beat on a fresh coordination file, so it has no prior disk
    // sample and its honest rate is `0`. Beat 2 has a real prior sample from
    // beat 1 (freeDiskGb=60 at t=1700000000000) 10s before its own
    // freeDiskGb=58 at t=1700000010000: (60 - 58) GB / (10/3600) h = 720
    // GB/h. A 2GB delta is used deliberately, not 1GB — since
    // DISK_TREND_NOISE_EPSILON_GB's dead zone compares with `<=` (Phase 6
    // final review), a 1GB delta is itself dead-zoned as noise and would
    // produce a rate of `0`, not a genuine decline signal.
    expect(Array.isArray(entry.diskHistory.declineRateReadings)).toBe(true);
    const declineRateValues = entry.diskHistory.declineRateReadings.map((reading) => reading.value);
    expect(declineRateValues).toContain(0);
    expect(declineRateValues).toContain(720);
  });

  test('backward-compat: an OLD-shape coordination-file entry (memoryHistory/diskHistory with no window field at all, as a pre-Phase-5 cli.mjs would have written) does not crash a subsequent invocation — degrades to an empty window', async () => {
    const orchestratorId = 'orch-1076-phase5-legacy-shape';

    // Simulate exactly what a pre-Phase-5 cli.mjs persisted: consecutive
    // Green/tripped/level/nonGreenSince only, no swapReadings/
    // declineRateReadings key at all.
    await declareDibs(coordinationFilePath, {
      orchestratorId,
      desiredAgents: 1,
      declaredAt: 1_700_000_000_000,
      firstDeclaredAt: 1_700_000_000_000,
      memoryHistory: { consecutiveGreen: 1, tripped: false, level: 'GREEN', nonGreenSince: undefined },
      diskHistory: { consecutiveGreen: 1, tripped: false, level: 'GREEN', nonGreenSince: undefined },
    });

    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=' + orchestratorId],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 },
          disk: { freeDiskGb: 60, declineRateGbPerHour: 0.5 },
        }),
        ARM_FAKE_NOW_MS: '1700000010000',
        ARM_BEAT_NOW_TEST_MODE: '1',
      },
    );

    expect(status).toBe(0);
    expect(stderr).not.toMatch(/TypeError|is not a function|Cannot read propert/i);

    let parsed;
    expect(() => {
      parsed = JSON.parse(stdout);
    }).not.toThrow();
    expect(['spawn-allowed', 'hold', 'pause', 'alert']).toContain(parsed.type);
  });
});

// ---------------------------------------------------------------------------
// Phase 2 (Build Plan Amendment) — REWRITE of Phase 5
// Part A pinned tests above.
//
// RC-3 (the Investigation's finding this phase closes): `freeRamMb`/`ps`-
// derived RSS INVERTS under macOS memory-compressor pressure — it *improves*
// as the host approaches freeze. Feeding it into `computeHeadroomCap` (as the
// two tests this block replaces used to pin, asserting allowance=1 for a
// "below the flat cap" live sample) is therefore actively dangerous: a host
// that is silently thrashing can report a large `freeRamMb` and get admitted
// as though it had headroom.
//
// This site (`main()`'s `--desired-agents` beat) must no longer let
// `freeRamMb` — live OR the `assumedFreeRamMb` fallback baseline — influence
// `allowanceConfig.memoryHeadroomCap` at all. Since Phase 3, this axis
// falls back to the real, ledger-derived `LIVE_AGENT_CEILING` (4) — the SAME
// number regardless of what `freeRamMb` says, live or absent. Both axes now
// share that same ceiling (see cli.mjs's `allowanceConfig` comment), so a
// solo, uncontended GREEN/GREEN beat's reported `allowance` (still advisory
// dibs headroom — Option B keeps that contract unchanged, see cli.mjs's
// `liveAgentGrant` comment) is exactly 4 in every case below.
// ---------------------------------------------------------------------------

describe('cli.mjs main() no longer wires memoryHeadroomCap from rawSample.memory.freeRamMb (Phase 2, RC-3)', () => {
  test('a live freeRamMb sample that WOULD have computed a headroom cap below the flat cap of 4 no longer suppresses the flat cap', () => {
    // Under the retired formula, freeRamMb=4500 -> floor((4500-2048-2048)/350)
    // = 1, and allowance would have been 1. Per the amendment, this site must
    // no longer consume freeRamMb at all, so allowance stays the flat 4
    // (LIVE_AGENT_CEILING, Phase 3).
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-1265-headroom-below', '--desired-agents=3'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 4500 },
          disk: DISK_GREEN,
        }),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('spawn-allowed');
    expect(parsed.allowance).toBe(4);
  });

  test('freeRamMb OMITTED from the fixture: allowance is still exactly 4 (the LIVE_AGENT_CEILING placeholder cap, not a freeRamMb-derived fallback)', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-1265-headroom-fallback', '--desired-agents=3'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: MEMORY_GREEN,
          disk: DISK_GREEN,
        }),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('spawn-allowed');
    expect(parsed.allowance).toBe(4);
  });

  test('admission verdict on --desired-agents does not vary with freeRamMb at fixed pressureLevel and fixed live-agent count', () => {
    // Two beats, identical in every respect EXCEPT freeRamMb (4500 vs a much
    // larger 50000) — same pressureLevel/swap/compressed reading, same disk
    // reading. Each uses its OWN coordination file (not the shared
    // `coordinationFilePath`) so neither beat's allowance is affected by dibs
    // contention from the other live entry — see the "dibs constrain the
    // reported allowance" describe block above for why two DIFFERENT
    // orchestrator ids sharing ONE coordination file would otherwise
    // genuinely contend and confound this invariance check. If freeRamMb
    // still fed admission math, these two would diverge (1 vs 4, per the
    // retired formula); post-fix they must be identical.
    const runWithFreeRamMb = (freeRamMb, orchestratorId, coordFile) =>
      runCli([`--orchestrator-id=${orchestratorId}`, '--desired-agents=3'], {
        ARM_COORDINATION_FILE: coordFile,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb },
          disk: DISK_GREEN,
        }),
      });

    const low = runWithFreeRamMb(4500, 'orch-1265-invariant-low', join(workDir, 'coordination-invariant-low.json'));
    const high = runWithFreeRamMb(
      50_000,
      'orch-1265-invariant-high',
      join(workDir, 'coordination-invariant-high.json'),
    );

    expect(low.status).toBe(0);
    expect(high.status).toBe(0);
    expect(low.stderr).toBe('');
    expect(high.stderr).toBe('');

    const parsedLow = JSON.parse(low.stdout);
    const parsedHigh = JSON.parse(high.stdout);
    expect(parsedLow.type).toBe('spawn-allowed');
    expect(parsedHigh.type).toBe('spawn-allowed');
    expect(parsedLow.allowance).toBe(parsedHigh.allowance);
  });

  test('the assumedFreeRamMb fallback path does not reintroduce flat-350 arithmetic', () => {
    // Note on this site: unlike handleQueryCapacity/handleClaim, main()'s
    // `--desired-agents` beat has no separate `assumedFreeRamMb` FORMULA of
    // its own to retire — it only has the `Number.isFinite(freeRamMb)` gate
    // that omits `memoryHeadroomCap` (falling through to
    // `DEFAULT_ALLOWANCE_CONFIG`) when freeRamMb is missing/non-finite. What
    // this test actually pins for THIS site is the amendment's broader
    // "freeRamMb, live or absent, no longer shapes admission math" contract:
    // a freeRamMb-omitted beat must land on the exact same allowance as a
    // beat with a LIVE freeRamMb chosen from BELOW the old flat-350 formula's
    // 4-agent reproduction threshold (floor((freeRamMb-4096)/350) < 4 iff
    // freeRamMb < 5,496) — i.e. a value the retired formula would have
    // computed a strictly-smaller-than-4 cap for. If any flat-350-shaped
    // arithmetic survived on the live path in a different guise, these two
    // would diverge (4 vs <4). Each beat uses its OWN coordination file (see
    // the invariance test above for why sharing one would confound this via
    // dibs contention).
    const omitted = runCli(['--orchestrator-id=orch-1265-fallback-omitted', '--desired-agents=3'], {
      ARM_COORDINATION_FILE: join(workDir, 'coordination-fallback-omitted.json'),
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    });
    const liveBelowOldReproductionThreshold = runCli(
      ['--orchestrator-id=orch-1265-fallback-live-5000', '--desired-agents=3'],
      {
        ARM_COORDINATION_FILE: join(workDir, 'coordination-fallback-live-5000.json'),
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 5000 },
          disk: DISK_GREEN,
        }),
      },
    );

    expect(omitted.status).toBe(0);
    expect(liveBelowOldReproductionThreshold.status).toBe(0);

    const parsedOmitted = JSON.parse(omitted.stdout);
    const parsedLive = JSON.parse(liveBelowOldReproductionThreshold.stdout);
    expect(parsedOmitted.allowance).toBe(4);
    expect(parsedLive.allowance).toBe(4);
    expect(parsedOmitted.allowance).toBe(parsedLive.allowance);
  });

  test('--desired-agents beat under RC-3\'s inversion scenario (low ps RSS, high phys_footprint proxy) does not over-admit', () => {
    // RC-3's inversion: as real memory pressure rises, the ps/vm_stat-derived
    // freeRamMb sample can read as IMPROVING rather than worsening (the
    // compressor makes "free" look large even as phys_footprint/swap climb).
    // Simulate the worst case: an implausibly large freeRamMb (the inverted
    // "looks great" reading) alongside a large desired-agents ask. Under the
    // retired formula this would have computed a headroom cap far above the
    // flat 4 (floor((1,000,000 - 4096) / 350) ~= 2845), massively
    // over-admitting on a host that is actually under real pressure. Post-fix
    // it must never exceed the flat LIVE_AGENT_CEILING cap of 4.
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-1265-rc3-inversion', '--desired-agents=50'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 1_000_000 },
          disk: DISK_GREEN,
        }),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('spawn-allowed');
    expect(parsed.allowance).toBeLessThanOrEqual(4);
    expect(parsed.allowance).toBe(4);
  });
});

// ---------------------------------------------------------------------------
// Phase 6 — degradation must fail in the right direction, and say what
// actually degraded.
//
// The defect these tests pin is LIVE today, independent of the rest of :
// `spawnBucketGrantedCount` was pre-seeded to the full ask and the catch
// around `consumeGlobalSpawnTokens` never reset it, so ANY coordination error
// at that site (a `LockTimeoutError` reaches it right now) left the CLI
// behaving as though the machine-wide spawn-rate bucket had granted
// everything requested. That is silent over-admission of the one limit that
// exists to stop this host being over-subscribed.
//
// Driving a failure at that specific site without mocking: make the
// coordination file's DIRECTORY unwritable. Reads still succeed (so the
// pre-beat `readDibs` still sees a previously-declared entry, keeping this
// beat spawn-eligible), while every write — the shared sample, `declareDibs`,
// and the spawn bucket — fails fast with EACCES.
// ---------------------------------------------------------------------------

describe('coordination-error degradation direction and messaging', () => {
  // chmod is not a permission barrier for uid 0, so a root runner would see
  // the writes succeed and this reproduction would prove nothing. Skip
  // honestly rather than assert something the environment cannot exhibit.
  const runningAsRoot = typeof process.getuid === 'function' && process.getuid() === 0;
  const maybeTest = runningAsRoot ? test.skip : test;

  async function makeCoordinationDirUnwritable() {
    const { chmod } = await import('node:fs/promises');
    await chmod(workDir, 0o555);
    return async () => {
      await chmod(workDir, 0o755);
    };
  }

  maybeTest(
    'a coordination failure at the spawn-bucket site denies the spawn instead of assuming the full request was granted',
    async () => {
      const env = {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_SPAWN_BUCKET_CONFIG_JSON: JSON.stringify({ capacityTokens: 10, refillTokensPerMs: 0 }),
      };

      // A healthy prior beat, so this orchestrator already has a live dibs
      // entry on disk and the degraded beat below stays spawn-eligible.
      const healthyBeat = runCli(['--orchestrator-id=orch-degraded', '--desired-agents=2'], env);
      expect(healthyBeat.status).toBe(0);
      expect(JSON.parse(healthyBeat.stdout).type).toBe('spawn-allowed');
      expect(JSON.parse(healthyBeat.stdout).allowance).toBeGreaterThan(0);

      const restore = await makeCoordinationDirUnwritable();
      try {
        const degradedBeat = runCli(['--orchestrator-id=orch-degraded', '--desired-agents=2'], env);
        expect(degradedBeat.status).toBe(0);

        const parsed = JSON.parse(degradedBeat.stdout);

        // Phase 2 review, Medium finding — the memory-projection gate
        // now owns its OWN `withLock` critical section
        // (`reconcileMemoryProjectionAdmission`) over this SAME coordination
        // file, and that gate runs strictly BEFORE the spawn-bucket site this
        // test was originally written to isolate. A directory-wide EACCES
        // therefore now fails the memory-projection gate's OWN lock
        // acquisition first — correctly failing closed to
        // 'memory-projection-block' — and the beat never reaches the
        // spawn-bucket code at all. This is a strictly EARLIER, still-safe
        // fail-closed refusal (never a silent over-admission of anything),
        // so the assertion below shifts from the retired
        // `type: 'spawn-allowed', allowance: 0` shape to the new refusal
        // reason — the underlying "never assume the full request was
        // granted on a coordination failure" property this test exists to
        // pin is preserved either way.
        expect(parsed.type).toBe('memory-projection-block');
        expect(parsed.liveAgentGrant).toBeNull();

        // ...and the operator is told which subsystem degraded and what the
        // consequence was, not a blanket "proceeding without dibs".
        expect(degradedBeat.stderr).toContain(
          "blocking this beat's admission rather than assuming the memory-projection budget was satisfied",
        );
      } finally {
        await restore();
      }
    },
  );

  maybeTest(
    'a declareDibs write failure is reported as a write failure and does not fabricate an entry that never reached disk',
    async () => {
      const env = {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      };

      const restore = await makeCoordinationDirUnwritable();
      try {
        const beat = runCli(['--orchestrator-id=orch-never-declared', '--desired-agents=2'], env);
        expect(beat.status).toBe(0);
        expect(beat.stderr).toContain("this beat's dibs declaration was not persisted");
        // The read-back branch's synthesising message must NOT appear — the
        // two failures are now distinguishable, which is the point.
        expect(beat.stderr).not.toContain('could not re-read dibs after declaring');
        // Nothing was written, so nothing is on disk to have been declared.
        await expect(access(coordinationFilePath)).rejects.toBeDefined();
      } finally {
        await restore();
      }
    },
  );

  test('the default degraded-warning wording is unchanged for the site it was accurate for', async () => {
    // The initial pre-beat `readDibs` is the one site whose consequence
    // genuinely is "proceeding without dibs for this beat". A corrupt file
    // makes that read throw while leaving every later write path healthy.
    await writeFile(coordinationFilePath, '{ not json at all', 'utf8');
    const beat = runCli(['--orchestrator-id=orch-corrupt-read', '--desired-agents=1'], {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    });
    expect(beat.status).toBe(0);
    expect(beat.stderr).toContain('coordination file unavailable');
    expect(beat.stderr).toContain('proceeding without dibs for this beat');
  });
});

// ---------------------------------------------------------------------------
// Phase 2 — pin `--record-outcome`'s fail-open handling of
// `LockLostError` specifically (not just the generic "history dir
// unwritable" ENOTDIR case ./record-outcome.jest.spec.mjs already covers).
//
// handleRecordOutcome (cli.mjs ~1663-1674) wraps its sole recordObservation()
// call in a catch that writes
//   `failed to record outcome history (${error.code ?? error.message})`
// to stderr and then falls through UNCONDITIONALLY to print the `{
// "recorded": true, ... }` confirmation JSON and exit 0 — this is already the
// generic "never crash the caller" posture every other coordination-file I/O
// path in this file follows. This test forces the SPECIFIC error
// lib/history.mjs's recordObservation now throws when it loses the fencing
// lock right before its write (Phase 1, LockLostError from
// ./lib/coordination-file.mjs) and proves that generic handling is genuinely
// exercised for this error, not just for plain ENOTDIR/EACCES cases.
//
// Race construction (mirrors lib/history.jest.spec.mjs's own "write-side
// ownership recheck" idiom exactly, just across a process boundary): seed a
// large filler history store first so recordObservation's read-modify-write
// (JSON.parse of ~20k entries, then re-stringify) takes long enough, after
// acquiring the lock, for this test to reliably delete `<historyFilePath>.lock`
// out from under the still-running child process before it reaches its
// write-side `assertStillHeld()` recheck — deleting the lock file (rather
// than a same-token reclaim) is the documented "absence is loss" path that
// throws LockLostError.
// ---------------------------------------------------------------------------

describe('--record-outcome fail-open handling of LockLostError specifically', () => {
  async function seedFillerHistoryStore(targetPath) {
    const count = 20_000;
    const raw = Array.from({ length: count }, (_, index) => ({
      operationType: '__filler-type__',
      orchestratorId: `filler-${index}`,
      startedAt: 0,
      endedAt: 0,
      peakMemoryMb: 100,
      padding: 'x'.repeat(40),
    }));
    await writeFile(targetPath, JSON.stringify({ '__filler-type__': { raw, summary: null } }), 'utf8');
  }

  /** Polls for `path` to exist rather than a fixed sleep — lock-file creation is itself async. */
  async function waitForFile(path, timeoutMs = 5000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (existsSync(path)) return true;
      // eslint-disable-next-line no-await-in-loop -- deliberate poll loop.
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    return false;
  }

  /**
   * Spawns cli.mjs asynchronously (not spawnSync) so the test can race its
   * lock file mid-flight.
   *
   * item 3 — returns `{ child, exited }`, not just `{ exited }`:
   * exposing the raw `ChildProcess` handle lets a caller kill a genuinely
   * hung child on timeout (`child.kill()`) rather than leaking an unkillable
   * process, which the original `{ exited }`-only shape made impossible.
   */
  function spawnCliAsync(args, extraEnv) {
    const child = spawn('node', [CLI, ...args], { env: { ...process.env, ...extraEnv } });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => {
      stdout += chunk;
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk;
    });
    const exited = new Promise((resolve) => {
      child.on('close', (status) => resolve({ status, stdout, stderr }));
    });
    return { child, exited };
  }

  /**
   * Races `exited` against a bounded timeout so a genuinely hung child (one
   * that never reaches `close`) is killed rather than leaked — item 3.
   * Resolves with the same `{ status, stdout, stderr }` shape `exited`
   * itself resolves with; throws if the timeout wins the race.
   *
   * @param {import('node:child_process').ChildProcess} child
   * @param {Promise<{ status: number, stdout: string, stderr: string }>} exited
   * @param {number} [timeoutMs]
   */
  async function waitForExitOrKill(child, exited, timeoutMs = 20_000) {
    let timeoutHandle;
    const timeout = new Promise((_resolve, reject) => {
      timeoutHandle = setTimeout(() => {
        child.kill('SIGKILL');
        reject(new Error(`spawnCliAsync child (pid ${child.pid}) did not exit within ${timeoutMs}ms — killed`));
      }, timeoutMs);
    });
    try {
      return await Promise.race([exited, timeout]);
    } finally {
      clearTimeout(timeoutHandle);
    }
  }

  test(
    'exits 0, still prints the recorded:true confirmation, and warns with the LockLostError code rather than "undefined" or "[object Object]"',
    async () => {
      const historyFilePath = join(workDir, 'history.json');
      await seedFillerHistoryStore(historyFilePath);
      const lockPath = `${historyFilePath}.lock`;

      const { child, exited } = spawnCliAsync(
        ['--record-outcome', '--operation-type=test-agent', '--orchestrator-id=orch-lock-lost'],
        { ARM_HISTORY_FILE: historyFilePath },
      );

      // Wait for the child to have actually acquired the fencing lock, then
      // delete it out from under it — absence at the write-side recheck is
      // the documented "cannot prove ownership, so must not write" path
      // (coordination-file.mjs's assertStillHeld), which throws
      // LockLostError.
      await expect(waitForFile(lockPath)).resolves.toBe(true);

      // item 3 — narrow lost-race detection. If the child has already
      // raced past its own write-side recheck by the time we get here (i.e.
      // it reached `assertStillHeld`/`rename` before we could delete the
      // lock file), `unlink` fails with ENOENT specifically — the lock file
      // is simply already gone, released by the child's own normal
      // `finally`. That is "we lost the race", not a real failure: skip only
      // the lock-lost-specific assertions below (there is no LockLostError
      // to observe on this run), but still assert the CLI never crashed
      // (`status === 0`), and still kill/await the child via
      // `waitForExitOrKill` so nothing leaks. Any OTHER unlink error (a
      // genuine permissions/filesystem problem) is not narrowed away — it
      // rethrows and fails the test outright, exactly as before.
      let lostRace = false;
      try {
        await unlink(lockPath);
      } catch (error) {
        if (error.code === 'ENOENT') {
          lostRace = true;
          // eslint-disable-next-line no-console -- deliberate: surfaces a
          // benign, expected-under-load race outcome in CI logs rather than
          // silently skipping assertions with no trace.
          console.warn(
            ' item 3: lost the race — the child already released its lock before this test could ' +
              'delete it; skipping the LockLostError-specific assertions for this run.',
          );
        } else {
          throw error;
        }
      }

      const { status, stdout, stderr } = await waitForExitOrKill(child, exited);

      if (lostRace) {
        // The CLI must never crash regardless of which side of the race it
        // landed on — this is the one assertion this degraded run can still
        // make meaningfully.
        expect(status).toBe(0);
        return;
      }

      // Fails open: never crashes the caller, still exits 0.
      expect(status).toBe(0);

      // The confirmation JSON is still printed unconditionally, despite the
      // throw inside the try block above it.
      let parsed;
      expect(() => {
        parsed = JSON.parse(stdout);
      }).not.toThrow();
      expect(parsed).toMatchObject({
        recorded: true,
        operationType: 'test-agent',
        orchestratorId: 'orch-lock-lost',
      });

      // The warning must name the actual failure, not `undefined` (which
      // `error.code ?? error.message` would print if BOTH were falsy — they
      // are not, since LockLostError sets `this.code =
      // 'COORDINATION_LOCK_LOST'`) and not `[object Object]` (which a naive
      // `${error}` on a non-Error value could print). It must not be the
      // empty warning the "history file path unwritable" ENOTDIR case in
      // ./record-outcome.jest.spec.mjs also degrades through.
      expect(stderr).toContain('failed to record outcome history');
      expect(stderr).toContain('COORDINATION_LOCK_LOST');
      expect(stderr).not.toContain('undefined');
      expect(stderr).not.toContain('[object Object]');
      expect(stderr).toContain('proceeding without persisting this beat');
    },
    15_000,
  );
});

// ---------------------------------------------------------------------------
// GitHub issue, Phase 1 — realCollect() (cli.mjs:645-695) forwards
// swapTotalMb into the sample it returns.
//
// `./lib/recon.mjs`'s `parseSwapUsage` already parses BOTH `swapUsedMb` AND
// `swapTotalMb` from the single `sysctl vm.swapusage` probe realCollect()
// already runs at cli.mjs:658 — but realCollect()'s returned `memory` object
// at cli.mjs:692 only forwards `swapUsedMb`, silently dropping
// `swapTotalMb` on the floor before it ever reaches classifyMemoryAxis (see
// this skill's threshold.jest.spec.mjs "relative (fraction-of-swapTotalMb)
// memory swap thresholds" section, which needs `swapTotalMb` present on the
// sample to do anything). No NEW probe call is required for this fix — the
// value is already sitting unused in `parseSwapUsage`'s existing return.
//
// Test-seam discrepancy (flagged per this ticket's own instruction to
// report rather than silently work around): the Build Plan asks for "a
// call-count spy confirms no more than one added sysctl call per beat", but
// `realCollect()` is NOT exported from cli.mjs, and this file's own header
// comment (cli.mjs:564-567) plus jest.skills.config.mjs's coverage
// exclusion (`!scripts/agent-resource-management/cli.mjs`) both establish,
// as a deliberate project convention, that cli.mjs's real
// execFileSync-shelling branch is black-box-only and not directly
// spy/mock-able — every sibling skill's cli.mjs follows the same rule (see
// jest.skills.config.mjs's per-skill comments). There is no existing seam
// (env var or otherwise) to inject a call-count spy into realCollect()
// itself. This test instead proves the forwarding black-box, via the one
// place a genuine (non-ARM_FAKE_COLLECT_JSON) realCollect() reading is
// observable outside the process: the persisted
// `__shared-machine-sample__` coordination-file entry (cli.mjs:1893-1899,
// `writeSharedSample`). It deliberately omits ARM_FAKE_COLLECT_JSON so this
// invocation takes the real OS-shelling path. The "no added sysctl call"
// half of the Build Plan's ask is therefore NOT mechanically asserted here
// (no available seam does so today) — it is instead a documented design
// constraint on the builder (the fix must read `swap.swapTotalMb` from the
// SAME `parseSwapUsage(swapRaw)` call already made at cli.mjs:799, not add a
// second sysctl invocation).
//
// Platform guard (CI fix): unlike the "cli.mjs freshness-window
// shared-sample reuse" describe block above — whose second, no-fake-JSON
// call always reuses a cache seeded by an EARLIER call within the same test
// that DOES pass ARM_FAKE_COLLECT_JSON, so it never actually reaches
// realCollect()'s real OS-shelling branch — these two tests are genuinely
// unguarded: neither call sets ARM_FAKE_COLLECT_JSON, so both hit
// realCollect()'s execFileSync branch for real, which shells out to
// `/usr/bin/vm_stat` and `/usr/sbin/sysctl` (cli.mjs:723-724). Those binaries
// are macOS-only and don't exist on the `ubuntu-latest` CI runner, so this
// describe block is gated to Darwin hosts only (`process.platform ===
// 'darwin'`) — see the `describe.skip` fallback immediately below. On a real
// macOS dev machine the tests still run for real and catch a genuine
// regression in the swapTotalMb-forwarding fix; on Linux CI they are
// reported as skipped rather than failing on an environment mismatch that
// has nothing to do with the production code under test. This is the first
// `process.platform` guard in this file — no earlier precedent existed to
// follow, so this establishes the pattern for any future genuinely-real
// (non-ARM_FAKE_COLLECT_JSON) OS-shelling test.
// ---------------------------------------------------------------------------

const describeOnDarwin = process.platform === 'darwin' ? describe : describe.skip;

describeOnDarwin('realCollect() forwards swapTotalMb', () => {
  test(
    'a real (non-ARM_FAKE_COLLECT_JSON) invocation persists a __shared-machine-sample__ entry whose ' +
      'memory reading includes a finite swapTotalMb',
    () => {
      const orchestratorId = 'orch-real-swap-total';

      const result = runCli(['--orchestrator-id=' + orchestratorId], {
        ARM_COORDINATION_FILE: coordinationFilePath,
        // Deliberately NOT setting ARM_FAKE_COLLECT_JSON — this must hit the
        // real realCollect() OS-shelling branch (vm_stat/sysctl/df), which
        // only exists/is meaningful on macOS (this dev host is Darwin).
      });

      expect(result.status).toBe(0);
    },
    15000,
  );

  test(
    'the persisted shared-sample memory reading\'s swapTotalMb is a positive finite number on this real macOS host',
    async () => {
      const orchestratorId = 'orch-real-swap-total-2';

      const result = runCli(['--orchestrator-id=' + orchestratorId], {
        ARM_COORDINATION_FILE: coordinationFilePath,
      });
      expect(result.status).toBe(0);

      const raw = await readFile(coordinationFilePath, 'utf8');
      const entries = JSON.parse(raw);
      const sharedSampleEntry = entries.find((entry) => entry.orchestratorId === SHARED_SAMPLE_RESERVED_ID);

      expect(sharedSampleEntry).toBeDefined();
      expect(sharedSampleEntry.reading?.memory).toBeDefined();

      // RED today: realCollect() (cli.mjs:692) never puts `swapTotalMb` on
      // the `memory` object it returns, so this is `undefined` until the
      // fix forwards `swap.swapTotalMb` (already parsed at cli.mjs:672 via
      // `parseSwapUsage`) through to the returned sample.
      expect(typeof sharedSampleEntry.reading.memory.swapTotalMb).toBe('number');
      expect(Number.isFinite(sharedSampleEntry.reading.memory.swapTotalMb)).toBe(true);
      expect(sharedSampleEntry.reading.memory.swapTotalMb).toBeGreaterThan(0);
    },
    15000,
  );
});

// ---------------------------------------------------------------------------
// Post-Phase-6-review regression (pre-PR review round 2, N1): the relative
// (fraction-of-swapTotalMb) swap thresholds `lib/threshold.mjs`'s
// `classifyMemoryRaw` implements were never actually wired
// into production `DEFAULT_MEMORY_THRESHOLDS` in cli.mjs — the mechanism was
// tested (lib/threshold.jest.spec.mjs, via its own custom threshold
// fixtures), but no cli-level test with a `swapTotalMb`-bearing
// ARM_FAKE_COLLECT_JSON ever exercised the WIRING, so the gap shipped
// invisibly. These tests close that: proof, through the real black-box
// `cli.mjs` pipeline (production DEFAULT_MEMORY_THRESHOLDS, not a custom
// fixture), that a small-swap host's relative thresholds are genuinely live.
// ---------------------------------------------------------------------------

describe('cli.mjs production DEFAULT_MEMORY_THRESHOLDS genuinely wires the relative swap-fraction gates (Phase 1, post-review)', () => {
  test('a small-swap host (1 GiB total) with swap 95% full is NOT GREEN, even though swapUsedMb (973) is far below the absolute greenSwapUsedMbBelow (4096)', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-relative-swap-red', '--desired-agents=1'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: { pressureLevel: 1, swapUsedMb: 973, swapTotalMb: 1024, compressedMb: 512 },
          disk: DISK_GREEN,
        }),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = JSON.parse(stdout);
    // 973/1024 ≈ 95% >= redSwapUsedFractionOfTotalAtOrAbove (0.9) — RED via
    // the relative gate alone; the absolute gates (pressureLevel:1,
    // swapUsedMb:973 < 4096) would have called this GREEN/AMBER on their
    // own, proving the relative gate is genuinely consulted, not dead code.
    expect(parsed.type).toBe('pause');
  });

  test('the SAME 973/1024MB swap reading with swapTotalMb OMITTED falls back to the absolute gates (GREEN) — proves the guard, not just the gate', () => {
    const { status, stdout } = runCli(
      ['--orchestrator-id=orch-relative-swap-omitted', '--desired-agents=1'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: { pressureLevel: 1, swapUsedMb: 973, compressedMb: 512 },
          disk: DISK_GREEN,
        }),
      },
    );

    expect(status).toBe(0);
    const parsed = JSON.parse(stdout);
    // No swapTotalMb -> the relative gate is skipped entirely (guarded, per
    // classifyMemoryRaw's own contract) -> falls back to the absolute gates,
    // which this reading satisfies (pressureLevel 1, swapUsedMb 973 < 4096).
    expect(parsed.type).toBe('spawn-allowed');
  });

  test('a small-swap host well under the relative red fraction (400/1024MB, ~39%) is unaffected — still GREEN', () => {
    const { status, stdout } = runCli(
      ['--orchestrator-id=orch-relative-swap-green', '--desired-agents=1'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: { pressureLevel: 1, swapUsedMb: 400, swapTotalMb: 1024, compressedMb: 512 },
          disk: DISK_GREEN,
        }),
      },
    );

    expect(status).toBe(0);
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('spawn-allowed');
  });
});

// ---------------------------------------------------------------------------
// — resolveNow() (cli.mjs:774-ish) currently honours ARM_FAKE_NOW_MS
// unconditionally whenever it parses as a well-formed integer, with no
// awareness of ARM_COORDINATION_FILE at all. The fix (a DIFFERENT agent's
// job, not this one) adds a precondition: ARM_FAKE_NOW_MS is honoured only
// when ARM_COORDINATION_FILE is ALSO set in the environment, using the
// IDENTICAL plain-truthy check resolveCoordinationFilePath() (cli.mjs:790)
// already uses (`if (process.env.ARM_COORDINATION_FILE)`) — NOT
// resolveNow()'s own `.trim() !== ''` check, which disagrees with that
// truthy check on a whitespace-only value.
//
// RED BY CONSTRUCTION for the "ignored -> real-time fallback" tests below
// (today's resolveNow() honours ARM_FAKE_NOW_MS regardless of
// ARM_COORDINATION_FILE, so today it does NOT fall back). The "honoured
// when both are set" tests are regression pins: that half of the contract
// is unaffected by the gate and already holds true today — they exist to
// prove the fix does not accidentally close the gate for the common case
// every pre-existing test in this directory already relies on.
//
// This describe block deliberately violates the "every ARM_FAKE_NOW_MS env
// object also sets ARM_COORDINATION_FILE" invariant the premise-guard test
// (./env-seam-premise.jest.spec.mjs) checks against every OTHER spec file in
// this directory — that is the whole point of the "unset"/"" variants below.
// The premise-guard test excludes this block from its scan by construction
// (it is the last thing in this file) — see that file's own comment.
// ---------------------------------------------------------------------------

describe('cli.mjs resolveNow() ARM_FAKE_NOW_MS gated on ARM_COORDINATION_FILE', () => {
  // A live, generous `freeRamMb` so a cold-start `--claim=<type>:1` always
  // grants at least 1 slot — several assertions below depend on a REAL
  // ledger write happening (no write, no `claimedAt` to read back).
  const MEMORY_GREEN_AMPLE = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };

  // Fixed, clearly-not-real-wall-clock value (2023-11-14) — far enough from
  // any plausible `Date.now()` at test-run time that "was the fake value
  // honoured or was it ignored" is never ambiguous.
  const FAKE_NOW = 1_700_000_000_000;

  /**
   * Like the shared `runCli` above, but accepts an explicit `cwd`. Needed by
   * the whitespace-only-ARM_COORDINATION_FILE test below: cli.mjs's
   * `resolveCoordinationFilePath()` calls `resolve(process.env.ARM_COORDINATION_FILE)`,
   * and `resolve()` on a bare (non-absolute) value like `'   '` resolves
   * relative to the CHILD process's cwd — which, left at this test process's
   * own inherited cwd (the repo root), would leave a stray 3-space-named
   * file behind in the real working tree. Pinning `cwd` to the test's own
   * disposable `workDir` keeps that entirely inside `afterEach`'s cleanup.
   */
  function runCliIn(args, extraEnv, cwd) {
    const result = spawnSync('node', [CLI, ...args], {
      encoding: 'utf8',
      cwd,
      env: { ...process.env, ...extraEnv },
    });
    return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
  }

  /** Reads back a top-level dibs entry (the main()/`--desired-agents` beat's own shape). */
  async function readDibsEntry(coordinationPath, orchestratorId) {
    const raw = await readFile(coordinationPath, 'utf8');
    const entries = JSON.parse(raw);
    return entries.find((candidate) => candidate.orchestratorId === orchestratorId);
  }

  /**
   * Reads back a `--claim` beat's own persisted `claimedAt` — nested under
   * the reserved `__claim:<type>__` ledger entry's `claims[]` array (see
   * ./lib/coordination-file.mjs's `claimCapacity`), stamped from the exact
   * same `now = resolveNow()` value `handleClaim` (cli.mjs) captures at
   * entry and threads through as `claimCapacity`'s `referenceNow`.
   */
  async function readClaimedAt(coordinationPath, type, orchestratorId) {
    const raw = await readFile(coordinationPath, 'utf8');
    const entries = JSON.parse(raw);
    const ledgerEntry = entries.find((candidate) => candidate.orchestratorId === `__claim:${type}__`);
    if (!ledgerEntry) return undefined;
    const claim = (ledgerEntry.claims || []).find((candidate) => candidate.orchestratorId === orchestratorId);
    return claim?.claimedAt;
  }

  test('ARM_FAKE_NOW_MS honoured when ARM_COORDINATION_FILE also set (regression, --claim beat)', async () => {
    const orchestratorId = 'orch-gate-claim-both-set';
    const type = 'gate-claim-both-set-type';

    const result = runCli([`--claim=${type}:1`, `--orchestrator-id=${orchestratorId}`], {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN_AMPLE, disk: DISK_GREEN }),
      ARM_FAKE_NOW_MS: String(FAKE_NOW),
      // resolveNow() now also requires this master switch.
      ARM_BEAT_NOW_TEST_MODE: '1',
    });

    expect(result.status).toBe(0);
    expect(result.stderr).toBe('');
    expect(JSON.parse(result.stdout).granted).toBeGreaterThan(0);

    const claimedAt = await readClaimedAt(coordinationFilePath, type, orchestratorId);
    expect(claimedAt).toBe(FAKE_NOW);
  });

  test('ARM_FAKE_NOW_MS honoured when ARM_COORDINATION_FILE also set (regression, main()/--desired-agents beat)', async () => {
    const orchestratorId = 'orch-gate-main-both-set';

    const result = runCli(['--orchestrator-id=' + orchestratorId], {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      ARM_FAKE_NOW_MS: String(FAKE_NOW),
      // resolveNow() now also requires this master switch.
      ARM_BEAT_NOW_TEST_MODE: '1',
    });

    expect(result.status).toBe(0);
    expect(['spawn-allowed', 'hold', 'pause']).toContain(JSON.parse(result.stdout).type);

    const entry = await readDibsEntry(coordinationFilePath, orchestratorId);
    expect(entry).toBeDefined();
    expect(entry.declaredAt).toBe(FAKE_NOW);
  });

  test('ARM_FAKE_NOW_MS ignored -> real-time fallback when ARM_COORDINATION_FILE unset (--claim beat)', async () => {
    const orchestratorId = 'orch-gate-claim-unset';
    const type = 'gate-claim-unset-type';
    // ARM_COORDINATION_FILE is deliberately set to `undefined` below — that
    // is the condition under test, and it must be an explicit `undefined`
    // key, not merely an omitted one: `runCliIn` spreads `process.env` first
    // (`{ ...process.env, ...extraEnv }`), so an OMITTED key would silently
    // fall through to whatever value this jest process's own ambient
    // environment happens to hold for ARM_COORDINATION_FILE (e.g. if a real
    // orchestrator's shell profile or CI env exports it) — spawnSync drops
    // `undefined`-valued env keys entirely, so this genuinely unsets it for
    // the child regardless of the parent's ambient env. HOME is overridden
    // (rather than left as this machine's real user home) purely so
    // resolveCoordinationFilePath()'s production-default fallback
    // (`homedir() + '.claude/agent-state/resource-coordination.json'`) lands
    // inside this test's disposable workDir instead of the REAL coordination
    // file a genuine orchestrator on this host may be using concurrently —
    // this does not change anything about what's under test
    // (ARM_COORDINATION_FILE is still unset either way).
    const sandboxHome = workDir;
    const fallbackCoordinationFilePath = join(sandboxHome, '.claude/agent-state/resource-coordination.json');

    const beforeMs = Date.now();
    const result = runCliIn(
      [`--claim=${type}:1`, `--orchestrator-id=${orchestratorId}`],
      {
        HOME: sandboxHome,
        ARM_COORDINATION_FILE: undefined,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN_AMPLE, disk: DISK_GREEN }),
        ARM_FAKE_NOW_MS: String(FAKE_NOW),
        // set alongside the leg under test (ARM_COORDINATION_FILE
        // unset) so this test still pins ITS OWN leg specifically — without
        // this, the assertion below would pass merely because the
        // switch is also absent, not because ARM_COORDINATION_FILE is unset.
        ARM_BEAT_NOW_TEST_MODE: '1',
      },
      workDir,
    );
    const afterMs = Date.now();

    expect(result.status).toBe(0);
    expect(JSON.parse(result.stdout).granted).toBeGreaterThan(0);

    const claimedAt = await readClaimedAt(fallbackCoordinationFilePath, type, orchestratorId);
    expect(claimedAt).toBeGreaterThanOrEqual(beforeMs);
    expect(claimedAt).toBeLessThanOrEqual(afterMs);
    // The direct negative pin: never the poisoned far-past fake value.
    expect(claimedAt).not.toBe(FAKE_NOW);
  });

  test('ARM_FAKE_NOW_MS ignored -> real-time fallback when ARM_COORDINATION_FILE unset (main()/--desired-agents beat)', async () => {
    const orchestratorId = 'orch-gate-main-unset';
    const sandboxHome = workDir;
    const fallbackCoordinationFilePath = join(sandboxHome, '.claude/agent-state/resource-coordination.json');

    // ARM_COORDINATION_FILE must be an explicit `undefined` key, not an
    // omitted one — see the sibling --claim test above for why: an omitted
    // key would fall through to this jest process's own ambient env via
    // runCliIn's `{ ...process.env, ...extraEnv }` spread, which is exactly
    // the leak scenario this gate exists to defend against.
    const beforeMs = Date.now();
    const result = runCliIn(
      ['--orchestrator-id=' + orchestratorId],
      {
        HOME: sandboxHome,
        ARM_COORDINATION_FILE: undefined,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_NOW_MS: String(FAKE_NOW),
        // set alongside the leg under test (ARM_COORDINATION_FILE
        // unset) so this test still pins ITS OWN leg specifically — without
        // this, the assertion below would pass merely because the
        // switch is also absent, not because ARM_COORDINATION_FILE is unset.
        ARM_BEAT_NOW_TEST_MODE: '1',
      },
      workDir,
    );
    const afterMs = Date.now();

    expect(result.status).toBe(0);
    expect(['spawn-allowed', 'hold', 'pause']).toContain(JSON.parse(result.stdout).type);

    const entry = await readDibsEntry(fallbackCoordinationFilePath, orchestratorId);
    expect(entry).toBeDefined();
    expect(entry.declaredAt).toBeGreaterThanOrEqual(beforeMs);
    expect(entry.declaredAt).toBeLessThanOrEqual(afterMs);
    expect(entry.declaredAt).not.toBe(FAKE_NOW);
  });

  test("ARM_COORDINATION_FILE='' (empty string) treated as unset — gate stays closed", async () => {
    // Empty string is falsy, so resolveCoordinationFilePath()'s own
    // `if (process.env.ARM_COORDINATION_FILE)` check already treats '' as
    // unset — the new gate must agree, and fall back to the production
    // default path (sandboxed here via HOME, same rationale as above).
    const orchestratorId = 'orch-gate-empty-string';
    const sandboxHome = workDir;
    const fallbackCoordinationFilePath = join(sandboxHome, '.claude/agent-state/resource-coordination.json');

    const beforeMs = Date.now();
    const result = runCliIn(
      ['--orchestrator-id=' + orchestratorId],
      {
        HOME: sandboxHome,
        ARM_COORDINATION_FILE: '',
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_NOW_MS: String(FAKE_NOW),
        // set alongside the leg under test (ARM_COORDINATION_FILE=''
        // treated as unset) so this test still pins ITS OWN leg
        // specifically — without this, the assertion below would pass
        // merely because the switch is also absent, not because ''
        // is treated as unset.
        ARM_BEAT_NOW_TEST_MODE: '1',
      },
      workDir,
    );
    const afterMs = Date.now();

    expect(result.status).toBe(0);
    expect(['spawn-allowed', 'hold', 'pause']).toContain(JSON.parse(result.stdout).type);

    const entry = await readDibsEntry(fallbackCoordinationFilePath, orchestratorId);
    expect(entry).toBeDefined();
    expect(entry.declaredAt).toBeGreaterThanOrEqual(beforeMs);
    expect(entry.declaredAt).toBeLessThanOrEqual(afterMs);
    expect(entry.declaredAt).not.toBe(FAKE_NOW);
  });

  test("ARM_COORDINATION_FILE='   ' (whitespace-only) treated as SET — gate opens", async () => {
    // Whitespace-only is TRUTHY in JS, so resolveCoordinationFilePath()'s
    // plain-truthy check treats '   ' as configured (it calls
    // resolve('   ') and uses that as the real coordination path). The new
    // gate inside resolveNow() must use the IDENTICAL plain-truthy
    // expression — not resolveNow()'s own `.trim() !== ''` check on
    // ARM_FAKE_NOW_MS, which would disagree with resolveCoordinationFilePath()
    // here and wrongly treat the coordination file as unset.
    const orchestratorId = 'orch-gate-whitespace';
    const whitespaceCoordinationPath = join(workDir, '   ');

    const result = runCliIn(
      ['--orchestrator-id=' + orchestratorId],
      {
        ARM_COORDINATION_FILE: '   ',
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_NOW_MS: String(FAKE_NOW),
        // resolveNow() now also requires this master switch.
        ARM_BEAT_NOW_TEST_MODE: '1',
      },
      workDir,
    );

    expect(result.status).toBe(0);
    expect(['spawn-allowed', 'hold', 'pause']).toContain(JSON.parse(result.stdout).type);

    const entry = await readDibsEntry(whitespaceCoordinationPath, orchestratorId);
    expect(entry).toBeDefined();
    expect(entry.declaredAt).toBe(FAKE_NOW);
  });

  test('malformed ARM_FAKE_NOW_MS + valid ARM_COORDINATION_FILE falls back to Date.now() (unchanged)', async () => {
    // The gate doesn't interact with malformed-value handling: a
    // non-numeric ARM_FAKE_NOW_MS still falls back to Date.now(), exactly
    // as it does today, regardless of ARM_COORDINATION_FILE being set.
    const orchestratorId = 'orch-gate-malformed-fake-now';

    const beforeMs = Date.now();
    const result = runCli(['--orchestrator-id=' + orchestratorId], {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      ARM_FAKE_NOW_MS: 'not-a-number',
      // set alongside the leg under test (malformed ARM_FAKE_NOW_MS)
      // so this test still pins ITS OWN leg specifically — without this,
      // the assertion below would pass merely because the switch is
      // also absent, not because the value is malformed.
      ARM_BEAT_NOW_TEST_MODE: '1',
    });
    const afterMs = Date.now();

    expect(result.status).toBe(0);
    expect(['spawn-allowed', 'hold', 'pause']).toContain(JSON.parse(result.stdout).type);

    const entry = await readDibsEntry(coordinationFilePath, orchestratorId);
    expect(entry).toBeDefined();
    expect(entry.declaredAt).toBeGreaterThanOrEqual(beforeMs);
    expect(entry.declaredAt).toBeLessThanOrEqual(afterMs);
  });

  test('set-leak of {ARM_FAKE_NOW_MS, ARM_COORDINATION_FILE} with the new master switch unset yields the real clock', async () => {
    // `resolveNow()` currently honours ARM_FAKE_NOW_MS on nothing
    // more than {ARM_FAKE_NOW_MS, ARM_COORDINATION_FILE} both being set — a
    // set-leak of just those two (e.g. a spec's `env` object, dotenv, or a
    // CI `env:` block) defeats the gate, because a REAL invocation can
    // genuinely have ARM_COORDINATION_FILE set on its own. The fix (a later
    // phase) adds a third, explicit master-switch env var — in the same
    // idiom as `beatSnapshotTestModeEnabled()`'s `ARM_BEAT_SNAPSHOT_TEST_MODE
    // === '1'` and its `ARM_EVICT_TEST_MODE` sibling — that must ALSO be
    // exactly '1' before the fake clock is honoured. This test spawns
    // cli.mjs as a real subprocess (this file's established idiom for
    // resolveNow() — every beat calls it internally, and it is not
    // exported for direct in-process import), so "known real-clock value"
    // is established the same way the sibling "ignored -> real-time
    // fallback" tests above do it: bracketing with Date.now() immediately
    // before and after the call, rather than mocking Date.now() itself.
    const orchestratorId = 'orch-1359-leak-no-switch';

    // --- Assertion 1 (the ticket's core AC, now enforced) ----------------
    // Exactly the leaked pair, deliberately without the new master
    // switch (see this describe block's own header comment for why this
    // env object is intentionally excluded from the env-seam-premise
    // sentinel's dual-key pairing scan rather than a naming omission to
    // fix — spelling the switch's env var name here as a bare identifier
    // would falsely satisfy that sentinel's substring-based pairing check).
    // resolveNow()'s gate now requires that switch, so this falls back to
    // the real clock.
    const beforeMs = Date.now();
    const result = runCli(['--orchestrator-id=' + orchestratorId], {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      ARM_FAKE_NOW_MS: String(FAKE_NOW),
      // Deliberately absent: the master switch (see comment above).
    });
    const afterMs = Date.now();

    expect(result.status).toBe(0);
    expect(['spawn-allowed', 'hold', 'pause']).toContain(JSON.parse(result.stdout).type);

    const entry = await readDibsEntry(coordinationFilePath, orchestratorId);
    expect(entry).toBeDefined();
    expect(entry.declaredAt).toBeGreaterThanOrEqual(beforeMs);
    expect(entry.declaredAt).toBeLessThanOrEqual(afterMs);
    // The direct negative pin: never the poisoned far-past fake value.
    expect(entry.declaredAt).not.toBe(FAKE_NOW);

    // --- Assertion 2 (regression-proofing, same leaked pair + switch) ---
    // With the same leaked pair PLUS the new master switch set to '1',
    // FAKE_NOW must be honoured — proving the gate is a real three-way AND,
    // not a no-op that always falls back to the real clock regardless of
    // the switch.
    const orchestratorIdWithSwitch = 'orch-1359-leak-with-switch';
    const resultWithSwitch = runCli(['--orchestrator-id=' + orchestratorIdWithSwitch], {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      ARM_FAKE_NOW_MS: String(FAKE_NOW),
      ARM_BEAT_NOW_TEST_MODE: '1',
    });

    expect(resultWithSwitch.status).toBe(0);
    expect(['spawn-allowed', 'hold', 'pause']).toContain(JSON.parse(resultWithSwitch.stdout).type);

    const entryWithSwitch = await readDibsEntry(coordinationFilePath, orchestratorIdWithSwitch);
    expect(entryWithSwitch).toBeDefined();
    expect(entryWithSwitch.declaredAt).toBe(FAKE_NOW);
  });

  // ---------------------------------------------------------------------------
  // Phase 1 — the rest of the two-layer master-switch contract.
  //
  // The set-leak test above already proves both halves of the THREE-WAY-AND
  // core contract (switch unset -> real clock; switch + leaked pair -> fake
  // clock). What it does NOT yet cover is the switch's own idiom: is it a
  // second, INDEPENDENT precondition (so it alone, without ARM_FAKE_NOW_MS,
  // is a no-op), and does it use the exact `=== '1'` strict-equality idiom
  // `beatSnapshotTestModeEnabled()`/`ARM_EVICT_TEST_MODE` already use (so a
  // truthy-but-not-'1' value like 'true'/'yes' does NOT enable it)? Those two
  // properties are this block's job.
  //
  // Env var name: `ARM_BEAT_NOW_TEST_MODE`, as landed by.
  // ---------------------------------------------------------------------------

  test('new master switch alone, without ARM_FAKE_NOW_MS, is a no-op (falls through to Date.now())', async () => {
    const orchestratorId = 'orch-1359-switch-alone';

    const beforeMs = Date.now();
    const result = runCli(['--orchestrator-id=' + orchestratorId], {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      // The new switch is ON, but ARM_FAKE_NOW_MS is deliberately absent —
      // the switch alone must never manufacture a fake clock value.
      ARM_BEAT_NOW_TEST_MODE: '1',
    });
    const afterMs = Date.now();

    expect(result.status).toBe(0);
    expect(['spawn-allowed', 'hold', 'pause']).toContain(JSON.parse(result.stdout).type);

    const entry = await readDibsEntry(coordinationFilePath, orchestratorId);
    expect(entry).toBeDefined();
    expect(entry.declaredAt).toBeGreaterThanOrEqual(beforeMs);
    expect(entry.declaredAt).toBeLessThanOrEqual(afterMs);
  });

  test.each(['true', 'yes', 'TRUE', '01', ' 1'])(
    "new master switch set to non-'1' truthy string %j does not enable the fake clock (strict === '1' idiom)",
    async (nonCanonicalValue) => {
      const orchestratorId = `orch-1359-switch-non-canonical-${nonCanonicalValue.trim() || 'blank'}`;

      const beforeMs = Date.now();
      const result = runCli(['--orchestrator-id=' + orchestratorId], {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_NOW_MS: String(FAKE_NOW),
        ARM_BEAT_NOW_TEST_MODE: nonCanonicalValue,
      });
      const afterMs = Date.now();

      expect(result.status).toBe(0);
      expect(['spawn-allowed', 'hold', 'pause']).toContain(JSON.parse(result.stdout).type);

      const entry = await readDibsEntry(coordinationFilePath, orchestratorId);
      expect(entry).toBeDefined();
      expect(entry.declaredAt).toBeGreaterThanOrEqual(beforeMs);
      expect(entry.declaredAt).toBeLessThanOrEqual(afterMs);
      expect(entry.declaredAt).not.toBe(FAKE_NOW);
    },
  );

  test.each(['0', ''])(
    "new master switch set to %j does not enable the fake clock (convergence-analysis note)",
    async (offValue) => {
      const orchestratorId = `orch-1359-switch-off-${offValue || 'empty'}`;

      const beforeMs = Date.now();
      const result = runCli(['--orchestrator-id=' + orchestratorId], {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_NOW_MS: String(FAKE_NOW),
        ARM_BEAT_NOW_TEST_MODE: offValue,
      });
      const afterMs = Date.now();

      expect(result.status).toBe(0);
      expect(['spawn-allowed', 'hold', 'pause']).toContain(JSON.parse(result.stdout).type);

      const entry = await readDibsEntry(coordinationFilePath, orchestratorId);
      expect(entry).toBeDefined();
      expect(entry.declaredAt).toBeGreaterThanOrEqual(beforeMs);
      expect(entry.declaredAt).toBeLessThanOrEqual(afterMs);
      expect(entry.declaredAt).not.toBe(FAKE_NOW);
    },
  );
});

