// Phase 7 — outer acceptance test: two orchestrators sharing ONE
// coordination file.
//
// This file simulates two independent orchestrator processes
// (`--orchestrator-id=orch-a` / `--orchestrator-id=orch-b`) invoking the real
// `cli.mjs` as a child process (never imported directly — matches
// ./cli.jest.spec.mjs's black-box idiom) against one shared
// `ARM_COORDINATION_FILE`, and pins the full Definition of Done:
// real running-agent discovery, freshness-window sample reuse, a genuinely
// GLOBAL spawn-rate token bucket, dibs-bounded allowance sharing, and
// wall-clock (not merely count-based) hysteresis recovery.
//
// All five gaps below were closed across Phases 1-6, each individually
// reviewed clean and covered by dedicated unit/integration tests
// (`lib/*.jest.spec.mjs`, `cli.jest.spec.mjs`). This file's job is no longer
// to document what's missing — it's the "Definition of Done" outer test,
// proving the five capabilities work TOGETHER, end-to-end, through the real
// black-box CLI, on one shared coordination file:
//   - real running-agent discovery: cli.mjs feeds `ARM_FAKE_PS_OUTPUT`
//     (real `ps -Ao pid,ppid,rss,etime,comm` plaintext) through
//     `./lib/recon.mjs`'s `listAgentProcesses`, and a RED beat with a
//     genuinely running claude-rooted process tree names a real
//     `pauseCandidate` (not `null`, not downgraded to `hold`).
//   - freshness-window sample reuse: `collect()` reuses another
//     orchestrator's still-fresh sample (`DEFAULT_FRESHNESS_WINDOW_MS`) via a
//     `__shared-machine-sample__` reserved coordination-file entry, instead
//     of re-sampling (or degrading) on every beat.
//   - `consumeSpawnTokens` (./lib/allowance.mjs) is wired into cli.mjs, with
//     bucket state persisted through the coordination file as a genuinely
//     GLOBAL, cross-orchestrator spawn-rate cap.
//   - `computePerOrchestratorAllowance` (./lib/allowance.mjs) divides the
//     flat per-axis cap across orchestrators with genuinely live,
//     simultaneous dibs entries, while a lone (uncontended) orchestrator
//     still gets the full flat cap as headroom.
//   - `applyHysteresis` (./lib/threshold.mjs, via `classifyMemoryAxis` /
//     `classifyDiskAxis`) gates recovery on wall-clock elapsed time, not
//     merely a count of consecutive GREEN samples — back-to-back rapid-fire
//     beats do not recover as fast as beats spread over real time.
//
// Seam names (all implemented in cli.mjs, matching the existing `ARM_*`
// convention):
//
//   ARM_FAKE_PS_OUTPUT        Real `ps -Ao pid,ppid,rss,etime,comm`-style
//                             PLAINTEXT (column order pid/ppid/rss(KB)/
//                             etime/comm) describing a fake running-agent
//                             process tree, standing in for real `ps`-based
//                             discovery. See ./lib/recon.mjs's
//                             `PS_LINE_WITH_ETIME_PATTERN` and
//                             ./cli.jest.spec.mjs's `PS_ONE_AGENT`-style
//                             fixtures for the exact shape.
//   ARM_SPAWN_BUCKET_CONFIG_JSON
//                             JSON `{ capacityTokens, refillTokensPerMs }`
//                             overriding the production default for the
//                             global spawn-rate token bucket persisted
//                             through the coordination file.
//
// This file writes real Jest assertions throughout — none are skipped or
// marked `.todo`/`.skip`.

import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { AIMD_CEILING_RESERVED_ID, writeAimdCeilingState } from './lib/coordination-file.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 };
// Phase 4: pressureLevel is kept at 1 (below the new, independent
// pressure-WARN pre-check's `warnAtOrAbove: 2` threshold — see cli.mjs's
// DEFAULT_PRESSURE_OVERRIDE_THRESHOLDS) so these fixtures drive AMBER/RED
// classification purely via the SWAP thresholds, keeping every test below
// decoupled from the new pressure pre-check (see
// ./pressure-override.jest.spec.mjs for its dedicated coverage) — see
// cli.jest.spec.mjs's identical fixture comment for the full rationale.
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

/**
 * Runs the real cli.mjs as a genuinely CONCURRENT child process — resolves
 * once the process exits, but (critically, unlike `runCli`/`spawnSync`)
 * does not block this test's event loop while the child runs, so two calls
 * started back-to-back via `Promise.all` actually race against each other
 * for the same coordination-file lock, rather than running one after the
 * other. Mirrors ./claim.jest.spec.mjs's `runCliAsync` exactly (see its
 * Build Plan, Phase 0).
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

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-two-orch-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

test('1. real running-agent discovery: a RED beat with a fake process tree names a real agent as pauseCandidate, not null', () => {
  // Real `ps -Ao pid,ppid,rss,etime,comm`-style plaintext (the actually-
  // shipped seam — see ./lib/recon.mjs's `PS_LINE_WITH_ETIME_PATTERN` and
  // ./cli.jest.spec.mjs's `PS_ONE_AGENT` fixture), NOT a JSON array. One
  // claude-rooted process tree: root pid 9001 (102400 KB) + its child pid
  // 9002 (51200 KB) = 153600 KB = 150 MB.
  const fakeProcessTree = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001  51200       58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-a'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
      ARM_FAKE_PS_OUTPUT: fakeProcessTree,
    },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');

  const parsed = parseStdout(stdout);
  // Real running-agent discovery: a RED beat with a
  // genuinely running claude-rooted process tree names an honest
  // `pauseCandidate` keyed by the root process's pid, not `null` and not
  // downgraded to a plain `{ type: 'hold' }`.
  expect(parsed.type).toBe('pause');
  expect(parsed).toHaveProperty('pauseCandidate');
  expect(parsed.pauseCandidate).not.toBeNull();
  expect(parsed.pauseCandidate).toMatchObject({ agentId: '9001' });
  expect(parsed.pauseCandidate.rssMb).toBeCloseTo(150, 5);
});

test('2. freshness-window sample reuse: orchestrator B reuses A\'s fresh sample instead of degrading on invalid collect input', () => {
  const env = { ARM_COORDINATION_FILE: coordinationFilePath };

  // Orchestrator A takes a real (fake-injected) GREEN sample and declares it.
  const beatA = runCli(
    ['--orchestrator-id=orch-a'],
    { ...env, ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }) },
  );
  expect(beatA.status).toBe(0);
  const parsedA = parseStdout(beatA.stdout);
  expect(parsedA.type).toBe('spawn-allowed');

  // Orchestrator B invokes shortly after, with NO valid collect input of its
  // own (deliberately malformed) — it should reuse A's still-fresh sample
  // from the shared coordination file rather than crashing or falling back
  // to a stale/degraded (AMBER) reading.
  const beatB = runCli(
    ['--orchestrator-id=orch-b'],
    { ...env, ARM_FAKE_COLLECT_JSON: '{ not valid json' },
  );
  expect(beatB.status).toBe(0);
  const parsedB = parseStdout(beatB.stdout);

  // Today: an invalid ARM_FAKE_COLLECT_JSON is swallowed by
  // ./lib/recon.mjs's takeSample() into a `{ stale: true, ... }` reading,
  // which both classifiers deterministically treat as AMBER — never
  // `spawn-allowed` — so this equality fails today.
  expect(parsedB.type).toBe(parsedA.type);
  expect(parsedB.type).toBe('spawn-allowed');
});

test('3. global spawn-rate token bucket: B\'s request for more than the SHARED bucket has left is denied/reduced', () => {
  const env = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    // Small shared capacity, no refill within this test's real-time span —
    // assumed seam (see file header); cli.mjs does not recognize this env
    // var at all today.
    ARM_SPAWN_BUCKET_CONFIG_JSON: JSON.stringify({ capacityTokens: 2, refillTokensPerMs: 0 }),
  };

  // Orchestrator A consumes (nearly) the whole shared bucket. A is solo/
  // uncontended at this moment (no other orchestrator has live dibs yet), so
  // its REPORTED allowance is the full flat per-axis cap (4, LIVE_AGENT_
  // CEILING) as headroom — the bucket happily granted A's actual request
  // (`min(desiredAgents=2, perOrchestratorAllowance=4)` = 2 tokens), so the
  // bucket is a pass-through here, not a further clamp down to the smaller
  // requested amount (Medium fix: "allowance is headroom, not a
  // target").
  const beatA = runCli(['--orchestrator-id=orch-a', '--desired-agents=2'], env);
  expect(beatA.status).toBe(0);
  const parsedA = parseStdout(beatA.stdout);
  expect(parsedA.type).toBe('spawn-allowed');
  expect(parsedA.allowance).toBe(4);

  // Orchestrator B immediately asks for 2 more tokens from the SAME global
  // bucket, which has nothing left (0 remaining, no refill elapsed). A true
  // global cap must deny/reduce B's allowance below its request.
  const beatB = runCli(['--orchestrator-id=orch-b', '--desired-agents=2'], env);
  expect(beatB.status).toBe(0);
  const parsedB = parseStdout(beatB.stdout);

  // Today: there is no persisted token-bucket state anywhere — `allowance`
  // is computed purely from each beat's own axis classification
  // (GREEN => the flat per-axis cap, here 4), completely independent of A's
  // prior consumption. This assertion fails today because B is handed the
  // full flat cap rather than a bucket-constrained (here: 0) share.
  expect(parsedB.type === 'spawn-allowed' ? parsedB.allowance : 0).toBeLessThan(2);
});

test('4. dibs constrain allowance: two SIMULTANEOUSLY-contending orchestrators divide the cap by first-arrival priority, not by whoever just beat', () => {
  const env = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
  };
  // LIVE_AGENT_CEILING in cli.mjs (Phase 3 — previously
  // DEFAULT_ALLOWANCE_CONFIG's maxAgentsMemoryAxis: 3 / maxAgentsDiskAxis: 3)
  // — the full uncapped per-axis maximum (and the headroom a genuinely SOLO
  // orchestrator still correctly receives) is 4.
  const FULL_UNCAPPED_PER_AXIS_MAXIMUM = 4;

  // A per-beat allowance computation is a PURE function of the dibs entries
  // that are ALREADY live in the coordination file at the moment that beat
  // runs — it cannot see a future, not-yet-declared dibs entry. So to prove
  // genuine, simultaneous contention correctly bounds the LATER-arriving
  // orchestrator's share while the EARLIER-arriving one keeps its priority
  // claim stable across beats (see ./lib/allowance.jest.spec.mjs's
  // `computePerOrchestratorAllowance` tests and ./cli.jest.spec.mjs's Phase 5
  // describe block for the pinned contract this mirrors), this test declares
  // BOTH orchestrators' dibs first, then re-checks each one's allowance while
  // the other's entry is already live:
  //
  //   Beat 1 (A, --desired-agents=5): only A is live — uncontended — A
  //           correctly receives the full flat cap as headroom.
  //   Beat 2 (B, --desired-agents=5): A's dibs entry is now already live, so
  //           B's beat runs under genuine contention — B's allowance is
  //           correctly bounded below the full cap (A arrived first).
  //   Beat 3 (A again, --desired-agents=5): B's dibs entry is now also live,
  //           but A is still the EARLIER-arriving orchestrator (`firstDeclaredAt`
  //           is preserved across A's own beats, unlike `declaredAt`, which is
  //           refreshed every beat) — A must keep the FULL cap here too. Prior
  //           to the fix, priority ordering was (bugged) sorted on
  //           `declaredAt`, which this very beat refreshes to "now" — making A
  //           look newest/lowest-priority purely because its own beat just
  //           ran, self-demoting it below B on every alternating beat and
  //           producing a permanent 0/0 deadlock under sustained contention.
  //           This assertion is the regression guard for that bug.

  // Beat 1: A alone. Uncontended — full flat cap as headroom [non-regression].
  const beatA1 = runCli(['--orchestrator-id=orch-a', '--desired-agents=5'], env);
  expect(beatA1.status).toBe(0);
  const parsedA1 = parseStdout(beatA1.stdout);
  expect(parsedA1.type).toBe('spawn-allowed');
  expect(parsedA1.allowance).toBe(FULL_UNCAPPED_PER_AXIS_MAXIMUM);

  // Beat 2: B's beat runs while A's dibs entry is already live — genuine,
  // simultaneous contention (5 + 5 = 10 exceeds the per-axis cap of 4) — B's
  // share must be bounded below the full uncapped maximum (A arrived first).
  const beatB = runCli(['--orchestrator-id=orch-b', '--desired-agents=5'], env);
  expect(beatB.status).toBe(0);
  const parsedB = parseStdout(beatB.stdout);
  expect(parsedB.type).toBe('spawn-allowed');
  expect(parsedB.allowance).toBeLessThan(FULL_UNCAPPED_PER_AXIS_MAXIMUM);

  // Beat 3: A runs again, now that B's dibs entry is also live. A is still
  // the earlier-arriving orchestrator (its `firstDeclaredAt` predates B's),
  // so A's priority claim is unaffected by refreshing its own `declaredAt` —
  // it keeps the full cap, proving priority is stable across beats rather
  // than flipping to whoever happened to beat most recently.
  const beatA2 = runCli(['--orchestrator-id=orch-a', '--desired-agents=5'], env);
  expect(beatA2.status).toBe(0);
  const parsedA2 = parseStdout(beatA2.stdout);
  expect(parsedA2.type).toBe('spawn-allowed');
  expect(parsedA2.allowance).toBe(FULL_UNCAPPED_PER_AXIS_MAXIMUM);
});

test('5. wall-clock hysteresis: three rapid-fire GREEN beats immediately after AMBER do not yet report GREEN', () => {
  const orchestratorId = 'orch-a';
  const env = { ARM_COORDINATION_FILE: coordinationFilePath };

  // Trip the memory axis to AMBER.
  const trip = runCli(
    ['--orchestrator-id=' + orchestratorId],
    { ...env, ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_AMBER, disk: DISK_GREEN }) },
  );
  expect(trip.status).toBe(0);
  expect(parseStdout(trip.stdout).type).toBe('hold');

  // Three back-to-back GREEN beats, fired as fast as this process can spawn
  // children — far faster than any believable real-world recovery window
  // (seconds/minutes of sustained healthy readings). A wall-clock hysteresis
  // gate should still report the axis's last non-GREEN state until that
  // window elapses, regardless of how many consecutive GREEN SAMPLES have
  // been counted.
  let lastParsed;
  for (let i = 0; i < 3; i += 1) {
    const beat = runCli(
      ['--orchestrator-id=' + orchestratorId],
      { ...env, ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }) },
    );
    expect(beat.status).toBe(0);
    lastParsed = parseStdout(beat.stdout);
  }

  // Today: recovery is purely count-based (DEFAULT_HYSTERESIS_CONFIG's
  // `hysteresisRecoverConsecutiveGreen: 3`), with no wall-clock gate at all —
  // three rapid-fire GREEN beats DO recover to `spawn-allowed` today
  // (see cli.jest.spec.mjs's `--heartbeat` describe block's beat6 assertion
  // for the equivalent, currently-passing, purely-count-based recovery).
  // This assertion (recovery must NOT yet have happened) fails today.
  expect(lastParsed.type).not.toBe('spawn-allowed');
});

test(
  '6. a --heartbeat tick preserves the orchestrator\'s own prior REAL desiredAgents, it does not clobber it ' +
    'to 0 [regression]',
  () => {
    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    };
    // LIVE_AGENT_CEILING in cli.mjs (Phase 3 — previously
    // DEFAULT_ALLOWANCE_CONFIG's maxAgentsMemoryAxis: 3 / maxAgentsDiskAxis:
    // 3) — cap 4, matching test 4 above.
    const CAP = 4;

    // Beat 1: A declares real intent (desiredAgents=4, the WHOLE cap) —
    // uncontended, gets the full cap as headroom. Asking for the whole cap
    // (rather than 3, as this test used to before the cap became 4) keeps B
    // genuinely bounded to 0 below — a smaller ask would leave 1 slot of
    // real, non-zero dibs headroom for B, which is not what this test means
    // to exercise (see cli-two-orchestrators.jest.spec.mjs test 3 / the
    // Medium test in cli.jest.spec.mjs for the "headroom, not a
    // target" property this test does NOT re-test).
    const beatA1 = runCli(['--orchestrator-id=orch-a', '--desired-agents=4'], env);
    expect(beatA1.status).toBe(0);
    const parsedA1 = parseStdout(beatA1.stdout);
    expect(parsedA1.type).toBe('spawn-allowed');
    expect(parsedA1.allowance).toBe(CAP);

    // Beat 2: B declares real intent (desiredAgents=3) while A's dibs entry
    // (desiredAgents=4) is already live — genuine contention, cap fully
    // claimed by A (priority by earliest declaredAt) — B is correctly
    // bounded to 0.
    const beatB1 = runCli(['--orchestrator-id=orch-b', '--desired-agents=3'], env);
    expect(beatB1.status).toBe(0);
    const parsedB1 = parseStdout(beatB1.stdout);
    expect(parsedB1.type).toBe('spawn-allowed');
    expect(parsedB1.allowance).toBe(0);

    // A sends a `--heartbeat` — a timer-driven poll, not a new real beat.
    // Per the fix: this must PRESERVE A's own prior real desiredAgents (4)
    // in A's dibs entry, not clobber it to 0.
    const heartbeatA = runCli(['--orchestrator-id=orch-a', '--heartbeat'], env);
    expect(heartbeatA.status).toBe(0);

    // B's NEXT real beat must STILL be correctly bounded — not handed the
    // full cap — proving A's real intent survived the heartbeat tick. Before
    // the fix, A's heartbeat would have overwritten A's dibs entry to
    // desiredAgents:0, so B's next beat here would have wrongly seen no real
    // contention from A and received the full cap (4) again.
    const beatB2 = runCli(['--orchestrator-id=orch-b', '--desired-agents=3'], env);
    expect(beatB2.status).toBe(0);
    const parsedB2 = parseStdout(beatB2.stdout);
    expect(parsedB2.type).toBe('spawn-allowed');
    expect(parsedB2.allowance).toBeLessThan(CAP);
  },
);

// ---------------------------------------------------------------------------
// 7. (Phase 0 outer acceptance) — shared-sample compare-and-swap: a
// fresh sample must never lose to a staler one racing behind it.
//
// The `__shared-machine-sample__` reserved entry's read-decide-write today
// (cli.mjs's `main()`, ~lines 1272-1361) reads `existingEntries` UNLOCKED,
// then — once each orchestrator has independently finished taking its own
// sample — persists via `declareDibs` (an unconditional upsert-by-id, no
// freshness comparison against what's on disk at write time). Two
// orchestrators racing a fresh sample in the SAME window therefore persist
// whichever one's `declareDibs` call happens to acquire the coordination
// lock LAST — independent of which sample was actually taken more recently
// (`sampledAt`). This is empirically reproducible today (confirmed via a
// 30-run manual stress test during Build Plan authorship: the numerically
// OLDER `sampledAt` won ~40% of the time), matching the ticket's own framing
// ("no compare-and-swap... races under concurrent orchestrators") and the
// Build Plan's outer acceptance wording verbatim: "the entry always reflects
// whichever sample was most recently taken" / "never silently overwritten by
// a stale write racing behind a fresher one".
//
// `ARM_FAKE_NOW_MS` (the existing Phase 6 seam — see cli.mjs's
// `resolveNow()`) supplies each racer's distinguishable `sampledAt`,
// decoupled from real wall-clock completion order — exactly the seam the
// Build Plan's outer-acceptance text calls for ("distinguishable
// sampledAt/reading values so the winner is identifiable"). Runs several
// iterations (not a single lucky pass, matching Phase 1's own test-strategy
// note) so the assertion fails deterministically rather than flaking green.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 8.  (Phase 0 outer acceptance) — per-holder claim ledger:
// orchestrator B cannot release capacity orchestrator A claimed, and a claim
// past its TTL is excluded from the granted total without an explicit
// release.
//
// RED by construction: neither fix exists yet.
//
//   - Today's ledger (`./lib/coordination-file.mjs`'s `claimCapacity`/
//     `releaseCapacity`) is a single cumulative SCALAR per `type`
//     (`__claim:<type>__` => `{ grantedTotal, declaredAt }`), with no
//     per-holder attribution at all. `releaseCapacity` takes no
//     `orchestratorId` parameter (see its JSDoc's own "Unattributed by
//     design (today)" paragraph, coordination-file.mjs ~lines 964-971) — any
//     caller who knows `type` can decrement the SHARED pool, regardless of
//     who actually claimed those slots. So orchestrator B releasing against a
//     type only orchestrator A ever claimed succeeds today exactly as if B
//     had claimed it itself.
//   - There is also no TTL/expiry of any kind today — a claim only ever
//     leaves the ledger via an explicit `releaseCapacity` call (see the same
//     JSDoc: "There is still no TTL/expiry-based *automatic* reclamation of
//     an unused grant").
//
// This test drives the real `cli.mjs` (`--claim`/`--release`, never imported
// directly — matches every other file in this directory's black-box idiom)
// against one shared `ARM_COORDINATION_FILE`, with a capacity fixture tuned
// (mirrors ./claim.jest.spec.mjs's/./release.jest.spec.mjs's own
// `MEMORY_ONE_SLOT` fixture) so the type under test has EXACTLY one
// genuinely-free slot — any grant beyond that one slot is only possible if
// the ledger has incorrectly freed capacity it shouldn't have.
//
// `ARM_FAKE_NOW_MS` (the existing Phase 6 / seam — see cli.mjs's
// `resolveNow()`) supplies each beat's fake "now", letting this test advance
// past a TTL window entirely in fake time, with no real waiting.
// ---------------------------------------------------------------------------

test(
  '8. orchestrator B cannot release capacity orchestrator A claimed, and a claim past its TTL is excluded from ' +
    'the granted total without an explicit release',
  () => {
    // Phase 2 (Build Plan Amendment, RC-3) REWRITE: this fixture's
    // freeRamMb was originally tuned so a cold-start type had EXACTLY one
    // genuinely-free slot (floor((4450 - 4096) / 350) === 1). RC-3 retires
    // freeRamMb (live or the assumedFreeRamMb fallback) as an admission
    // input entirely — every type now gets the same flat
    // DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis (3) placeholder cap
    // regardless of freeRamMb. The fixture is kept (still a GREEN, low-
    // pressure memory reading — required shape) but "exactly one slot" below
    // is achieved by orchestrator A claiming the WHOLE 3-slot cap, not by
    // this reading alone.
    const MEMORY_ONE_SLOT = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 4450 };
    const TYPE = 'ttl-and-attribution-e2e-type';

    // A conservative window comfortably past ANY believable "conservative
    // estimated-duration" TTL default (per its own issue text) — 30 days
    // in fake-clock time, so this assertion does not depend on guessing the
    // implementer's exact `DEFAULT_CLAIM_TTL_MS` value.
    const T0 = 1_700_000_000_000; // an arbitrary, fixed fake "claim time"
    const THIRTY_DAYS_MS = 30 * 24 * 60 * 60 * 1000;
    const T1_PAST_TTL = T0 + THIRTY_DAYS_MS;

    const baseEnv = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_ONE_SLOT, disk: DISK_GREEN }),
      ARM_BEAT_NOW_TEST_MODE: '1',
    };

    // --- Part 1: orchestrator A claims the type's entire flat 3-slot cap. --
    const claimA = runCli(
      [`--claim=${TYPE}:3`, '--orchestrator-id=orch-a'],
      { ...baseEnv, ARM_FAKE_NOW_MS: String(T0) },
    );
    expect(claimA.status).toBe(0);
    expect(claimA.stderr).toBe('');
    expect(parseStdout(claimA.stdout).granted).toBe(3);

    // Sanity: the type is now fully exhausted — a third orchestrator (C) gets
    // nothing.
    const exhaustedClaim = runCli(
      [`--claim=${TYPE}:1`, '--orchestrator-id=orch-c'],
      { ...baseEnv, ARM_FAKE_NOW_MS: String(T0) },
    );
    expect(exhaustedClaim.status).toBe(0);
    expect(parseStdout(exhaustedClaim.stdout).granted).toBe(0);

    // --- Part 2: orchestrator B — who never claimed anything against this
    // type — attempts to release against it. This must have NO EFFECT on
    // orchestrator A's claimed capacity: B holds nothing of its own to
    // release, so `released` must be 0, and the ledger must be untouched.
    const releaseB = runCli(
      [`--release=${TYPE}:3`, '--orchestrator-id=orch-b'],
      { ...baseEnv, ARM_FAKE_NOW_MS: String(T0) },
    );
    expect(releaseB.status).toBe(0);
    expect(releaseB.stderr).toBe('');
    // Core fix: B never claimed against TYPE, so B's own recorded
    // claim total for it is 0 — releasing must report `released: 0`, not the
    // 3 slots that only A actually holds. FAILS TODAY: today's unattributed
    // ledger lets B decrement the shared scalar regardless of who claimed
    // it, so this returns `released: 3`.
    expect(parseStdout(releaseB.stdout).released).toBe(0);

    // Proof B's no-op release didn't free A's slot: a further claim by C
    // still gets nothing — the type must still read as fully exhausted.
    // FAILS TODAY: B's release above erroneously freed the shared pool, so
    // this grants 1 to C today.
    const stillExhaustedClaim = runCli(
      [`--claim=${TYPE}:1`, '--orchestrator-id=orch-c'],
      { ...baseEnv, ARM_FAKE_NOW_MS: String(T0) },
    );
    expect(stillExhaustedClaim.status).toBe(0);
    expect(parseStdout(stillExhaustedClaim.stdout).granted).toBe(0);

    // --- Part 3: advance the fake clock 30 days past A's original claim,
    // well past any believable TTL window, WITHOUT any explicit --release
    // call anywhere in this test for A's claim. A fresh claim (by
    // orchestrator D) must now succeed — A's expired claim must be excluded
    // from the granted-capacity aggregate automatically.
    // FAILS TODAY: there is no TTL/expiry mechanism at all today — A's claim
    // never ages out, so this grants 0 to D forever.
    const claimD = runCli(
      [`--claim=${TYPE}:1`, '--orchestrator-id=orch-d'],
      { ...baseEnv, ARM_FAKE_NOW_MS: String(T1_PAST_TTL) },
    );
    expect(claimD.status).toBe(0);
    expect(claimD.stderr).toBe('');
    expect(parseStdout(claimD.stdout).granted).toBe(1);
  },
);

// ---------------------------------------------------------------------------
// 9. Phase 1 — multi-holder concurrent release race: two DIFFERENT
// orchestrators releasing their OWN claims concurrently must not corrupt or
// drop each other's entries, and a third orchestrator's untouched claim
// entry must survive both releases.
//
// Distinct from test 8 above (which proves B cannot release A's claim, the
// outer acceptance scenario) — this test proves the inverse
// direction: two orchestrators EACH releasing what is genuinely their OWN
// claim, running as two real concurrent child processes racing for the
// same coordination-file lock, must never lose or corrupt either release,
// nor a third, wholly untouched holder's entry.
// ---------------------------------------------------------------------------

test(
  '9. two DIFFERENT orchestrators releasing their OWN claims concurrently must not corrupt or drop each ' +
    'other\'s entries, and a third orchestrator\'s untouched claim entry survives both releases',
  async () => {
    // Phase 2 (Build Plan Amendment, RC-3) REWRITE: MEMORY_AMPLE's
    // freeRamMb was originally tuned to compute 11 genuinely-free slots
    // (floor((8192-4096)/350) === 11), comfortably covering three
    // orchestrators' combined claims of 3 + 4 + 2 = 9. RC-3 retires
    // freeRamMb as an admission input entirely — every type now gets the
    // same flat DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis (3) placeholder
    // cap regardless of freeRamMb, so a combined ask of 9 no longer fits.
    // The three orchestrators' claims are rescaled to 1 + 1 + 1 = 3 — exactly
    // filling the new flat cap — so the "three distinct concurrent holders,
    // none of whose entries get corrupted or dropped" property this test
    // exists to prove is still exercised at genuine capacity, just at the
    // new cap size.
    const MEMORY_AMPLE = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
    const TYPE = 'concurrent-release-e2e-type';
    const T0 = 1_700_100_000_000;

    const baseEnv = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_AMPLE, disk: DISK_GREEN }),
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_NOW_MS: String(T0),
    };

    // Three distinct orchestrators each claim their own slot against the
    // same type, exactly filling the flat 3-slot cap — sequential setup, not
    // the race under test.
    const claimA = runCli([`--claim=${TYPE}:1`, '--orchestrator-id=orch-race-release-a'], baseEnv);
    expect(claimA.status).toBe(0);
    expect(parseStdout(claimA.stdout).granted).toBe(1);

    const claimB = runCli([`--claim=${TYPE}:1`, '--orchestrator-id=orch-race-release-b'], baseEnv);
    expect(claimB.status).toBe(0);
    expect(parseStdout(claimB.stdout).granted).toBe(1);

    const claimC = runCli([`--claim=${TYPE}:1`, '--orchestrator-id=orch-race-release-c'], baseEnv);
    expect(claimC.status).toBe(0);
    expect(parseStdout(claimC.stdout).granted).toBe(1);

    // A and B release their OWN claims genuinely concurrently — two
    // independent processes racing for the same coordination-file lock.
    const [releaseAResult, releaseBResult] = await Promise.all([
      runCliAsync([`--release=${TYPE}:1`, '--orchestrator-id=orch-race-release-a'], baseEnv),
      runCliAsync([`--release=${TYPE}:1`, '--orchestrator-id=orch-race-release-b'], baseEnv),
    ]);

    expect(releaseAResult.status).toBe(0);
    expect(releaseBResult.status).toBe(0);
    expect(releaseAResult.stderr).toBe('');
    expect(releaseBResult.stderr).toBe('');

    // Neither release is lost or partially applied, regardless of which
    // order the two racers' locked critical sections actually ran in.
    expect(parseStdout(releaseAResult.stdout).released).toBe(1);
    expect(parseStdout(releaseBResult.stdout).released).toBe(1);

    // C's claim, untouched by either race, must survive intact: releasing
    // C's own (unrelated) claim afterwards must still report exactly the 1
    // slot C genuinely holds — not corrupted, not dropped, not merged with
    // A's or B's entries.
    const releaseC = runCli([`--release=${TYPE}:100`, '--orchestrator-id=orch-race-release-c'], baseEnv);
    expect(releaseC.status).toBe(0);
    expect(parseStdout(releaseC.stdout).released).toBe(1);

    // A repeat attempt to release A's or B's already-released claims must
    // now report 0 — proving the concurrent releases genuinely landed
    // (rather than, say, only one of the two racers' writes surviving a
    // lost-update race, which would let a repeat release of A "succeed"
    // again for a nonzero amount).
    const repeatReleaseA = runCli([`--release=${TYPE}:1`, '--orchestrator-id=orch-race-release-a'], baseEnv);
    expect(repeatReleaseA.status).toBe(0);
    expect(parseStdout(repeatReleaseA.stdout).released).toBe(0);
  },
  20_000,
);

test(
  '7. shared-sample compare-and-swap: the freshest of two racing samples is never lost to a staler write',
  async () => {
    const RACE_ITERATIONS = 12;

    for (let iteration = 0; iteration < RACE_ITERATIONS; iteration += 1) {
      const iterationWorkDir = await mkdtemp(join(tmpdir(), 'arm-shared-sample-race-'));
      const iterationCoordinationFilePath = join(iterationWorkDir, 'coordination.json');

      try {
        const OLDER_SAMPLED_AT_MS = 1_000_000;
        const NEWER_SAMPLED_AT_MS = 2_000_000;
        // Distinguishable, out-of-range-of-any-real-fixture marker values so
        // the winning writer is unambiguously identifiable from the
        // persisted `reading.memory.freeRamMb` field alone.
        const OLDER_FREE_RAM_MB_MARKER = 111_111;
        const NEWER_FREE_RAM_MB_MARKER = 222_222;

        const env = {
          ARM_COORDINATION_FILE: iterationCoordinationFilePath,
          ARM_BEAT_NOW_TEST_MODE: '1',
        };

        // Both processes are launched together via Promise.all (a real race,
        // not a scripted ordering) — which one's `declareDibs` call actually
        // wins the coordination-file lock is left to real OS scheduling, NOT
        // to the `ARM_FAKE_NOW_MS` values below (those only fix each
        // process's OWN `sampledAt`, independent of completion order).
        const [olderResult, newerResult] = await Promise.all([
          runCliAsync(
            ['--orchestrator-id=orch-race-older'],
            {
              ...env,
              ARM_FAKE_NOW_MS: String(OLDER_SAMPLED_AT_MS),
              ARM_FAKE_COLLECT_JSON: JSON.stringify({
                memory: { ...MEMORY_GREEN, freeRamMb: OLDER_FREE_RAM_MB_MARKER },
                disk: DISK_GREEN,
              }),
            },
          ),
          runCliAsync(
            ['--orchestrator-id=orch-race-newer'],
            {
              ...env,
              ARM_FAKE_NOW_MS: String(NEWER_SAMPLED_AT_MS),
              ARM_FAKE_COLLECT_JSON: JSON.stringify({
                memory: { ...MEMORY_GREEN, freeRamMb: NEWER_FREE_RAM_MB_MARKER },
                disk: DISK_GREEN,
              }),
            },
          ),
        ]);

        expect(olderResult.status).toBe(0);
        expect(newerResult.status).toBe(0);

        const raw = await readFile(iterationCoordinationFilePath, 'utf8');
        const entries = JSON.parse(raw);
        const sharedSampleEntry = entries.find(
          (entry) => entry.orchestratorId === '__shared-machine-sample__',
        );

        expect(sharedSampleEntry).toBeDefined();

        // Never torn/corrupted: the persisted entry always matches EXACTLY
        // one of the two attempted full writes (sampledAt paired with its
        // OWN freeRamMb marker), never a mismatched hybrid of the two.
        const matchesOlderWriteExactly =
          sharedSampleEntry.sampledAt === OLDER_SAMPLED_AT_MS &&
          sharedSampleEntry.reading?.memory?.freeRamMb === OLDER_FREE_RAM_MB_MARKER;
        const matchesNewerWriteExactly =
          sharedSampleEntry.sampledAt === NEWER_SAMPLED_AT_MS &&
          sharedSampleEntry.reading?.memory?.freeRamMb === NEWER_FREE_RAM_MB_MARKER;
        expect(matchesOlderWriteExactly || matchesNewerWriteExactly).toBe(true);

        // The core property: the FRESHER sample (the higher
        // `sampledAt`) must be the one persisted — a fresh sample must never
        // be silently lost to a staler write racing behind it. Fails today
        // whenever the older writer happens to acquire the lock last.
        expect(sharedSampleEntry.sampledAt).toBe(NEWER_SAMPLED_AT_MS);
      } finally {
        await rm(iterationWorkDir, { recursive: true, force: true });
      }
    }
  },
  60_000,
);

// ---------------------------------------------------------------------------
// 10. two-orchestrator concurrent-ceiling race — flagged by
// the convergence analysis as the highest-risk gap in the whole
// build. Orchestrator A observes WARN pressure and drives an AIMD
// multiplicative-decrease of the shared ceiling (6 -> lower); orchestrator B
// races it, requesting live-agent admission under NORMAL pressure in the
// SAME beat window, against the SAME coordination file.
//
// Design decision A's rationale (see ./cli-aimd-ceiling.jest.spec.mjs's own
// header on this) is that the ledger-level `reserveAdmission` lock-time
// re-check is the actual backstop against over-admission, not any
// pre-lock ceiling read being perfectly fresh. This test proves that
// backstop holds under GENUINE two-process concurrency (not single-process
// mocking): whatever ceiling value each racer's own beat happened to read
// pre-lock, `reserveAdmission`'s own atomic snapshot+othersHeld arithmetic
// (per this file's own `AIMD_CEILING_RESERVED_ID` seed value used by BOTH
// racers going in) prevents the two beats' grants from ever exceeding that
// STARTING ceiling — the property `reserveAdmission`'s existing, already-
// shipped (Phase 1) locking genuinely guarantees.
//
// What this test deliberately does NOT claim: that the combined grant is
// bounded by the LOWER, post-decrease ceiling A's own beat may concurrently
// persist. Per ./cli-aimd-ceiling.jest.spec.mjs's dedicated "mixed-version
// fleet" test (which traces the same property directly against
// reserveAdmission's shipped code), a caller that read the ceiling BEFORE a
// concurrent decrease landed is granted admission computed against ITS OWN
// (now-stale) reading — the tighter bound is not something this design
// provides, even between two same-version orchestrators racing the same
// beat. This test's assertions are scoped to the bound that genuinely DOES
// hold, plus a direct after-the-fact check of whether the tighter bound
// happened to hold or not, so a future change to either direction is
// visible here rather than silently assumed.
// ---------------------------------------------------------------------------

test(
  '10. two orchestrators racing a WARN-driven AIMD ceiling decrease: no admission grant exceeds the STARTING (pre-decrease) ceiling, and the persisted AIMD ceiling genuinely backs off',
  async () => {
    const STARTING_CEILING = 6;
    const T0 = 1_700_200_000_000;

    // Both racers start from the SAME seeded ceiling — the property under
    // test is what happens when their two concurrent beats disagree about
    // whether it has since decreased, not a torn/uninitialized read.
    await writeAimdCeilingState(
      coordinationFilePath,
      { ceiling: STARTING_CEILING, sustainedNormalCount: 0 },
      { now: T0 },
    );

    const MEMORY_WARN = { pressureLevel: 2, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };
    const MEMORY_NORMAL_FOR_RACE = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };

    // Orchestrator A: WARN pressure — pressure-blocked, no admission
    // attempted, but (once Phase 3 lands) drives the AIMD ceiling's
    // multiplicative decrease on this same beat.
    const beatAPromise = runCliAsync(
      ['--orchestrator-id=orch-race-a-warn', '--desired-agents=1'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_WARN, disk: DISK_GREEN }),
      },
    );

    // Orchestrator B: NORMAL pressure, requesting a large ask — races A for
    // the SAME coordination-file lock, genuinely concurrently (Promise.all
    // below, real OS scheduling decides ordering).
    const beatBPromise = runCliAsync(
      ['--orchestrator-id=orch-race-b-normal', '--desired-agents=5'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_NORMAL_FOR_RACE, disk: DISK_GREEN }),
      },
    );

    const [beatA, beatB] = await Promise.all([beatAPromise, beatBPromise]);

    expect(beatA.status).toBe(0);
    expect(beatB.status).toBe(0);

    const parsedA = parseStdout(beatA.stdout);
    const parsedB = parseStdout(beatB.stdout);

    // A's own beat is pressure-blocked — never admitted anything.
    expect(parsedA.type).not.toBe('spawn-allowed');
    expect(parsedA.liveAgentGrant).toBeNull();

    // FAILS TODAY (Phase 3 unwired): the AIMD ceiling entry is never
    // written by ANY beat yet, so this entry does not exist at all.
    const raw = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
    const aimdEntry = raw.find((entry) => entry.orchestratorId === AIMD_CEILING_RESERVED_ID);
    expect(aimdEntry).toBeDefined();
    // The core AIMD-loop-closes-under-real-concurrency property: A's WARN
    // beat genuinely backed the shared ceiling off from its starting value,
    // even while racing B for the same lock.
    expect(aimdEntry.ceiling).toBeLessThan(STARTING_CEILING);

    // The bound reserveAdmission's existing (Phase 1, already-shipped)
    // locking genuinely guarantees: B's grant can never exceed the ceiling
    // BOTH racers started this beat with — reserveAdmission's own
    // snapshot+othersHeld arithmetic, run under one exclusive lock, cannot
    // manufacture headroom beyond whatever `ceiling` value(s) were actually
    // supplied to it, and neither racer's beat can have read a ceiling
    // HIGHER than the STARTING_CEILING seeded above (only equal-or-lower
    // readings are possible once Phase 3 wires the read before the write
    // this same beat may perform).
    expect(parsedB.liveAgentGrant ?? 0).toBeLessThanOrEqual(STARTING_CEILING);

    // The TIGHTER bound — B's grant never exceeding the FINAL, post-decrease
    // ceiling — is NOT asserted as a guarantee here (see this test's own
    // header comment and ./cli-aimd-ceiling.jest.spec.mjs's dedicated
    // "mixed-version fleet" test, which traces why `reserveAdmission` cannot
    // provide it on its own). Recorded, not asserted as safe: if B's beat
    // happened to read the ceiling before A's decrease landed, B may be
    // granted MORE than the final persisted value — this is observed and
    // logged for the human reviewer rather than silently passing either way.
    if ((parsedB.liveAgentGrant ?? 0) > aimdEntry.ceiling) {
      // eslint-disable-next-line no-console
      console.warn(
        `[ accepted-risk observation] orchestrator B was granted ${parsedB.liveAgentGrant} live-agent ` +
          `slot(s) against a ceiling that had already decreased to ${aimdEntry.ceiling} by the time this ` +
          'race settled — consistent with the documented mixed-ceiling-reading gap, not a torn/corrupted ledger.',
      );
    }
  },
  30_000,
);
