// Phase 0 — outer acceptance test for `--release=<type>:<count>`.
//
// RED by construction: `--release` does not exist yet in cli.mjs (grep
// confirms zero handling of the flag today — `releaseCapacity` is not
// exported from ./lib/coordination-file.mjs either). Phase 1/2 of the Build
// Plan land `releaseCapacity` + the CLI wiring; this file pins the ticket's
// outer acceptance scenario end-to-end, black-box, against the real cli.mjs
// — mirroring ./claim.jest.spec.mjs's and ./cli-two-orchestrators.jest.spec.mjs's
// idiom (temp ARM_COORDINATION_FILE, runCli/runCliAsync via spawnSync/spawn
// against the real cli.mjs, ARM_FAKE_COLLECT_JSON for hermetic capacity
// readings). Never imports cli.mjs directly (it stays excluded from the
// unit-coverage gate, see jest.skills.config.mjs's
// `!scripts/agent-resource-management/cli.mjs`).
//
// ---------------------------------------------------------------------------
// CLI contract this file tests AGAINST (for the builder to match in cli.mjs,
// per the Build Plan's Phase 2 objective):
//
//   node cli.mjs --release=<type>:<count> --orchestrator-id=<id>
//
//   Decrements the same cumulative per-type claim ledger `--claim` writes to
//   (`__claim:<type>__`, via a new `releaseCapacity` export in
//   ./lib/coordination-file.mjs, reusing the SAME `withLock` fencing
//   primitive `claimCapacity` already uses — no second lock primitive).
//
//   Output (stdout, exit 0): a single JSON object `{ "released": <n> }` where
//   `n = min(requestedReleaseCount, alreadyGrantedTotal)` — releasing more
//   than was ever granted floors at the true (lesser) amount, never goes
//   negative, never errors.
//
//   `--orchestrator-id` is required (mirrors `--claim`'s own precedent).
//
//   Combined with `--claim`/`--desired-agents`/`--query-capacity`/
//   `--record-outcome`/`--heartbeat` in the same invocation: REJECTED
//   outright, exit 2 — mirroring `--claim`'s own flag-combination-conflict
//   precedent exactly.
// ---------------------------------------------------------------------------

import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

// Tuned so a COLD-START type (default 350 MB/agent cost estimate,
// DEFAULT_HEADROOM_CONFIG's osBaselineMb: 2048 + reserveMb: 2048 = 4096 MB
// baseline, per ./lib/cost-estimate.mjs and cli.mjs's DEFAULT_HEADROOM_CONFIG)
// reports EXACTLY 1 genuinely-free slot via computeHeadroomCap:
//   floor((4450 - 4096) / 350) === floor(354 / 350) === 1
// — matches ./claim.jest.spec.mjs's MEMORY_ONE_SLOT fixture exactly, so a
// single `--claim` exhausts the type's entire available capacity.
const MEMORY_ONE_SLOT = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 4450 };

/** Runs the real cli.mjs as a child process synchronously — never imports it directly. */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/**
 * Runs the real cli.mjs as a genuinely CONCURRENT child process — resolves
 * once the process exits, but (critically, unlike `runCli`/`spawnSync`) does
 * not block this test's event loop while the child runs, so two calls
 * started back-to-back via `Promise.all` actually race against each other
 * for the same coordination-file lock, rather than running one after the
 * other. Mirrors ./claim.jest.spec.mjs's `runCliAsync` exactly.
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

let workDir;
let historyFilePath;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-release-'));
  historyFilePath = join(workDir, 'history.json');
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

function baseEnv(extraEnv = {}) {
  return {
    ARM_HISTORY_FILE: historyFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_ONE_SLOT, disk: DISK_GREEN }),
    ...extraEnv,
  };
}

function cli(args, extraEnv = {}) {
  return runCli(args, baseEnv(extraEnv));
}

function cliAsync(args, extraEnv = {}) {
  return runCliAsync(args, baseEnv(extraEnv));
}

// ---------------------------------------------------------------------------
// Core outer acceptance scenario: claim to exhaustion, release,
// claim again succeeds up to the released amount.
// ---------------------------------------------------------------------------

test('claim-to-exhaustion -> release -> claim-again: a released amount becomes claimable again', () => {
  // Phase 2 (Build Plan Amendment, RC-3) REWRITE: MEMORY_ONE_SLOT's
  // freeRamMb was originally tuned to make computeHeadroomCap report EXACTLY
  // 1 genuinely-free slot, so a single --claim=…:1 exhausted the whole type.
  // RC-3 retires freeRamMb as an admission input entirely — capacity is now
  // the flat DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis (3) placeholder
  // regardless of freeRamMb, so a single 1-slot claim no longer exhausts
  // anything (1 < 3). Exhaustion now requires claiming the whole 3-slot cap.
  const firstClaim = cli(['--claim=release-e2e-type:3', '--orchestrator-id=orch-release-1']);
  expect(firstClaim.status).toBe(0);
  expect(firstClaim.stderr).toBe('');
  expect(JSON.parse(firstClaim.stdout).granted).toBe(3);

  // The type is now fully exhausted — a further claim grants nothing.
  const exhaustedClaim = cli(['--claim=release-e2e-type:1', '--orchestrator-id=orch-release-2']);
  expect(exhaustedClaim.status).toBe(0);
  expect(JSON.parse(exhaustedClaim.stdout).granted).toBe(0);

  // Release the three previously-granted slots.
  const release = cli(['--release=release-e2e-type:3', '--orchestrator-id=orch-release-1']);
  expect(release.status).toBe(0);
  expect(release.stderr).toBe('');
  expect(JSON.parse(release.stdout).released).toBe(3);

  // The released capacity is claimable again.
  const reclaim = cli(['--claim=release-e2e-type:3', '--orchestrator-id=orch-release-3']);
  expect(reclaim.status).toBe(0);
  expect(reclaim.stderr).toBe('');
  expect(JSON.parse(reclaim.stdout).granted).toBe(3);
});

test('releasing more than was ever granted reports the true floored amount, never negative, never an error', () => {
  const firstClaim = cli(['--claim=release-overshoot-type:1', '--orchestrator-id=orch-overshoot-1']);
  expect(firstClaim.status).toBe(0);
  expect(JSON.parse(firstClaim.stdout).granted).toBe(1);

  const release = cli(['--release=release-overshoot-type:99', '--orchestrator-id=orch-overshoot-1']);
  expect(release.status).toBe(0);
  expect(release.stderr).toBe('');
  // Only 1 was ever granted — releasing 99 must report the true floored
  // amount (1), not the requested 99, and must never go negative.
  expect(JSON.parse(release.stdout).released).toBe(1);
});

test('releasing a never-claimed type reports {"released":0}, not an error', () => {
  const release = cli(['--release=release-never-claimed-type:5', '--orchestrator-id=orch-never-claimed']);
  expect(release.status).toBe(0);
  expect(release.stderr).toBe('');
  expect(JSON.parse(release.stdout).released).toBe(0);
});

// ---------------------------------------------------------------------------
// Validation / contract-shape coverage — mirrors --claim's own established
// posture in ./claim.jest.spec.mjs exactly.
// ---------------------------------------------------------------------------

test('--release without --orchestrator-id is rejected, exit 2, empty stdout', () => {
  const { status, stdout, stderr } = cli(['--release=release-validation-type:1']);
  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--orchestrator-id/);
});

test('--release with a missing count segment (no colon) is rejected, exit 2, empty stdout', () => {
  const { status, stdout, stderr } = cli(['--release=release-validation-type', '--orchestrator-id=orch-malformed-1']);
  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--release/);
});

test('--release with a non-integer count is rejected, exit 2, empty stdout', () => {
  const { status, stdout, stderr } = cli([
    '--release=release-validation-type:not-a-number',
    '--orchestrator-id=orch-malformed-2',
  ]);
  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--release/);
});

test('--release with an empty count segment is rejected, exit 2, empty stdout', () => {
  const { status, stdout, stderr } = cli([
    '--release=release-validation-type:',
    '--orchestrator-id=orch-malformed-empty',
  ]);
  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--release/);
});

test('--release with a negative count is rejected, exit 2, empty stdout', () => {
  const { status, stdout, stderr } = cli([
    '--release=release-validation-type:-1',
    '--orchestrator-id=orch-malformed-3',
  ]);
  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--release/);
});

// Security review follow-up (Low finding, agent-resource-management
// worktree) — mirrors `--declare-dibs`'s existing
// `isReservedEntry` rejection (cli.mjs, main()) and --claim's own equivalent
// coverage (./claim.jest.spec.mjs) so a caller cannot release under the
// reserved double-underscore sentinel convention (e.g. colliding with
// `LEGACY_UNATTRIBUTED_CLAIM_HOLDER_ID` in lib/coordination-file.mjs).
test('--release with a reserved-looking --orchestrator-id ("__foo__") is rejected, exit 2, empty stdout', () => {
  const { status, stdout, stderr } = cli(['--release=release-validation-type:1', '--orchestrator-id=__foo__']);
  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/reserved/);
});

test('--release combined with --claim in the same invocation is rejected, exit 2, naming both flags', () => {
  const { status, stdout, stderr } = cli([
    '--release=release-validation-type:1',
    '--claim=release-validation-type:1',
    '--orchestrator-id=orch-combo-1',
  ]);
  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--release/);
  expect(stderr).toMatch(/--claim/);
});

test('--release combined with --desired-agents in the same invocation is rejected, exit 2', () => {
  const { status, stdout, stderr } = cli([
    '--release=release-validation-type:1',
    '--desired-agents=1',
    '--orchestrator-id=orch-combo-2',
  ]);
  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--release/);
  expect(stderr).toMatch(/--desired-agents/);
});

test('--release combined with --query-capacity in the same invocation is rejected, exit 2', () => {
  const { status, stdout, stderr } = cli([
    '--release=release-validation-type:1',
    '--query-capacity=release-validation-type',
    '--orchestrator-id=orch-combo-3',
  ]);
  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--release/);
  expect(stderr).toMatch(/--query-capacity/);
});

test('--release combined with --record-outcome in the same invocation is rejected, exit 2', () => {
  const { status, stdout, stderr } = cli([
    '--release=release-validation-type:1',
    '--record-outcome',
    '--operation-type=release-validation-type',
    '--orchestrator-id=orch-combo-4',
  ]);
  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--release/);
  expect(stderr).toMatch(/--record-outcome/);
});

test('--release combined with --heartbeat in the same invocation is rejected, exit 2', () => {
  const { status, stdout, stderr } = cli([
    '--release=release-validation-type:1',
    '--heartbeat',
    '--orchestrator-id=orch-combo-5',
  ]);
  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--release/);
  expect(stderr).toMatch(/--heartbeat/);
});

// ---------------------------------------------------------------------------
// Phase 1 — cli.mjs's handleRelease threads --orchestrator-id through
// to releaseCapacity's new orchestratorId param (single-process, direct CLI
// confirmation — distinct from the two-process race idiom in
// ./cli-two-orchestrators.jest.spec.mjs's own test #8, which already covers
// the outer "B cannot release A's claim" acceptance scenario end-to-end).
// ---------------------------------------------------------------------------

test('a --release from an orchestrator who never claimed this type reports released:0 and does not drain a DIFFERENT orchestrator\'s claim', () => {
  // Phase 2 (Build Plan Amendment, RC-3) REWRITE: a single 1-slot claim
  // no longer exhausts the type under the flat 3-slot placeholder cap (see
  // the "claim-to-exhaustion" rewrite above) — the claimant now takes the
  // WHOLE cap (3) so "the claimant's slot(s) are still there, untouched" and
  // "a further claim still gets nothing" remain genuinely true below.
  const claimA = cli(['--claim=release-attribution-type:3', '--orchestrator-id=orch-attribution-claimant']);
  expect(claimA.status).toBe(0);
  expect(JSON.parse(claimA.stdout).granted).toBe(3);

  // A different orchestrator, who never claimed against this type, attempts
  // to release — must report released:0, proving --orchestrator-id is
  // genuinely threaded through to releaseCapacity's per-holder scoping, not
  // silently ignored (the pre-existing unattributed behavior would report
  // released:3 here, draining the claimant's slots).
  const releaseOther = cli([
    '--release=release-attribution-type:3',
    '--orchestrator-id=orch-attribution-not-the-claimant',
  ]);
  expect(releaseOther.status).toBe(0);
  expect(releaseOther.stderr).toBe('');
  expect(JSON.parse(releaseOther.stdout).released).toBe(0);

  // The claimant's slots are still there, untouched — a further claim by a
  // third orchestrator still gets nothing (fully exhausted, exactly as
  // before the no-op release attempt).
  const stillExhausted = cli(['--claim=release-attribution-type:1', '--orchestrator-id=orch-attribution-third']);
  expect(stillExhausted.status).toBe(0);
  expect(JSON.parse(stillExhausted.stdout).granted).toBe(0);

  // The genuine claimant CAN still release its own slots.
  const releaseClaimant = cli(['--release=release-attribution-type:3', '--orchestrator-id=orch-attribution-claimant']);
  expect(releaseClaimant.status).toBe(0);
  expect(JSON.parse(releaseClaimant.stdout).released).toBe(3);
});

// ---------------------------------------------------------------------------
// Concurrency correctness (outer scenario): a concurrent --claim
// racing a --release for the SAME type must never oversubscribe (combined
// grants never exceed availableCapacity) or undersubscribe (a release is
// never silently lost) the ledger — mirrors ./claim.jest.spec.mjs's own
// "exactly one wins the last slot" race idiom.
// ---------------------------------------------------------------------------

test(
  'a --release racing a concurrent --claim for the SAME type never oversubscribes or loses the release: ' +
    'the combined effect always matches SOME valid sequential ordering of the two',
  async () => {
    // Phase 2 (Build Plan Amendment, RC-3) REWRITE: MEMORY_ONE_SLOT no
    // longer makes a single --claim=…:1 exhaust the type (capacity is now
    // the flat DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis (3) placeholder,
    // independent of freeRamMb) — the setup claim now takes the WHOLE 3-slot
    // cap so the racing --claim below still has NO genuinely-free capacity
    // available except whatever the racing --release frees up, preserving
    // the property this test exists to prove.
    const setupClaim = cli(['--claim=release-race-type:3', '--orchestrator-id=orch-race-owner']);
    expect(setupClaim.status).toBe(0);
    expect(JSON.parse(setupClaim.stdout).granted).toBe(3);

    // Race a --release of the whole 3-slot cap against a --claim for the
    // whole 3-slot cap, for the SAME type, via two genuinely concurrent
    // child processes.
    const [releaseResult, claimResult] = await Promise.all([
      cliAsync(['--release=release-race-type:3', '--orchestrator-id=orch-race-owner']),
      cliAsync(['--claim=release-race-type:3', '--orchestrator-id=orch-race-challenger']),
    ]);

    expect(releaseResult.status).toBe(0);
    expect(claimResult.status).toBe(0);
    expect(releaseResult.stderr).toBe('');
    expect(claimResult.stderr).toBe('');

    const released = JSON.parse(releaseResult.stdout).released;
    const granted = JSON.parse(claimResult.stdout).granted;

    // The release itself must never be lost regardless of ordering — exactly
    // 3 slots were ever granted, so releasing 3 always reports released:3,
    // whichever order the two racers' locked critical sections run in.
    expect(released).toBe(3);

    // Whatever the --claim raced against, it must land on ONE of the two
    // valid sequential outcomes for "claim capacity:3, release 3, claim
    // capacity:3 more" — either the release-then-claim ordering (granted:3,
    // since the released cap is available again) or the claim-then-release
    // ordering (granted:0, since no capacity was free yet when the claim's
    // lock section ran) — never a third, ledger-corrupting value.
    expect([0, 3]).toContain(granted);

    // The ledger's own final state must reflect EXACTLY the released
    // capacity actually consumed by the race above — never oversubscribed
    // (more granted overall than the type's flat 3-slot cap ever allowed).
    // A trailing claim for one more slot must succeed if and only if the
    // race above left the ledger with headroom (granted:0 above), and must
    // be denied if the race above already consumed the whole cap (granted:3
    // above) — proving the release was neither lost NOR double-spent by the
    // race.
    const trailing = cli(['--claim=release-race-type:1', '--orchestrator-id=orch-race-trailing']);
    expect(trailing.status).toBe(0);
    const trailingGranted = JSON.parse(trailing.stdout).granted;
    expect(trailingGranted).toBe(granted === 3 ? 0 : 1);
  },
  20_000,
);

// ---------------------------------------------------------------------------
// review follow-up (Medium): mirrors --claim's own guard (see
// claim.jest.spec.mjs) — `live-agent-admission` is written and pruned
// exclusively by `reserveAdmission` (the --desired-agents beat); a public
// --release against it would let an orchestrator (or a typo) clear another
// orchestrator's outstanding admission hold out from under it.
// ---------------------------------------------------------------------------

test(
  '--release=live-agent-admission:N is rejected outright (exit 2) — that ledger is internal-only ' +
    '[ regression]',
  () => {
    const result = cli(['--release=live-agent-admission:3', '--orchestrator-id=orc-tries-admission-release']);

    expect(result.status).toBe(2);
    expect(result.stdout).toBe('');
    expect(result.stderr).toContain('live-agent-admission');
    expect(result.stderr).toContain('internal-only');
  },
);

// ---------------------------------------------------------------------------
// Phase 3 — mirrors the `live-agent-admission` guard directly above:
// `idle-probe` (IDLE_PROBE_CLAIM_TYPE) is the same posture, internal-only,
// written exclusively by the `--desired-agents` beat's `--user-attests-idle`
// path — see that constant's own doc comment in cli.mjs. A public
// `--release` against it must be rejected the same way.
// ---------------------------------------------------------------------------

test(
  '--release=idle-probe:N is rejected outright (exit 2) — that ledger is internal-only [ ]',
  () => {
    const result = cli(['--release=idle-probe:3', '--orchestrator-id=orc-tries-idle-probe-release']);

    expect(result.status).toBe(2);
    expect(result.stdout).toBe('');
    expect(result.stderr).toContain('idle-probe');
    expect(result.stderr).toContain('internal-only');
  },
);
