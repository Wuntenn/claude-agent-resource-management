// Phase 5 — `--claim` atomic slot claim (race-safety).
//
// RED by construction: `--claim` does not exist yet in cli.mjs. Phases 1-4
// (lib/history.mjs, --record-outcome, lib/cost-estimate.mjs,
// --query-capacity) have already landed. These are black-box, real-process-
// spawning tests — mirroring the established idiom in ./cli.jest.spec.mjs,
// ./record-outcome.jest.spec.mjs, and ./query-capacity.jest.spec.mjs — never
// importing cli.mjs directly (it stays excluded from the unit-coverage gate,
// see jest.skills.config.mjs's `!scripts/agent-resource-management/cli.mjs`).
// The core race-safety property additionally requires two REAL, concurrently
// -running child processes (not the sequential `spawnSync` idiom
// ./cli-two-orchestrators.jest.spec.mjs otherwise uses for its own six
// tests) — see `runCliAsync`/`raceCli` below.
//
// ---------------------------------------------------------------------------
// CLI contract this file tests AGAINST (for the builder to match in cli.mjs):
//
//   node cli.mjs --claim=<type>:<count> --orchestrator-id=<id>
//
//   `--claim` is the authoritative WRITE half of the query/claim split
//   Phase 4's `--query-capacity` began: it re-validates capacity INSIDE the
//   existing fencing lock from ./lib/coordination-file.mjs (`declareDibs`'s
//   `withLock`/`acquireLock`/`releaseLock` mechanism — no second lock
//   primitive) and atomically reserves whatever is genuinely still available
//   at the moment the lock is held, closing the race window a
//   `--query-capacity` advisory read alone cannot close.
//
//   `<type>` — a single operation type (unlike `--query-capacity`'s
//   comma-separated list — `--claim` claims exactly one type per invocation).
//   `<count>` — a non-negative integer: how many slots this invocation wants.
//
//   ARM_FAKE_COLLECT_JSON   (env, required for test hermeticity)
//     Reused exactly as `--query-capacity`'s own seam — a JSON string shaped
//     `{ "memory": {...}, "disk": {...} }`. `memory.freeRamMb` feeds the SAME
//     `computeHeadroomCap` (./lib/allowance.mjs) `--query-capacity` already
//     reuses for its own per-type `freeCapacity` — `--claim` does not
//     duplicate that formula. The type's own cost estimate (empirical, via
//     `estimateOperationCost`/./lib/cost-estimate.mjs, or the cold-start
//     default when the type has no history) still supplies `perAgentRamMb`.
//
//   ARM_HISTORY_FILE   (env, required for test hermeticity)
//     Reused exactly as `--record-outcome`'s/`--query-capacity`'s existing
//     seam.
//
//   ARM_COORDINATION_FILE   (env, required for test hermeticity)
//     Reused exactly as every spawn-decision beat's existing seam — `--claim`
//     persists its atomically-reserved grant through this same file, under
//     the same lock `declareDibs` already uses, so two independent processes
//     racing for the last slot of one type never both win.
//
//   Output (stdout, exit 0): a single JSON object `{ "granted": <n> }` where
//   `0 <= n <= count`, and `n` never exceeds whatever was genuinely available
//   (computeHeadroomCap's result for this type, using this type's own cost
//   estimate) at the instant the lock was held — deny-the-remainder
//   semantics (a request for more than is available grants exactly what's
//   left, not 0 and not the full ask).
//
//   A `--query-capacity` read taken BEFORE a `--claim` (by this process or
//   another) can legitimately imply a higher grant than a `--claim` actually
//   returns moments later — `--query-capacity` never consults or reserves
//   against the claim ledger `--claim` writes to, by design (it is advisory-
//   only). This is expected, not a bug: a later `--claim` always reflects
//   the CURRENT true state under the lock, never the earlier advisory read.
//
//   `--orchestrator-id` is required (mirrors the existing `--desired-agents`
//   spawn-decision beat's own precedent) — the fencing/attribution identity
//   for this claim.
//
//   Combined with `--desired-agents`, `--record-outcome`, `--heartbeat`, or
//   `--query-capacity` in the same invocation: REJECTED outright, exit 2,
//   clear stderr naming both flags — mirroring the exact precedent
//   `--record-outcome`/`--query-capacity` already established for
//   flag-combination rejection.
// ---------------------------------------------------------------------------

import { spawn, spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdtemp, rm, writeFile, readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

// Phase 2 (Build Plan Amendment, RC-3): this fixture's `freeRamMb:
// 4450` was originally tuned so a COLD-START type's computeHeadroomCap call
// reported EXACTLY 1 genuinely-free slot (floor((4450 - 4096) / 350) === 1).
// That arithmetic no longer applies — `freeRamMb` (live or the
// assumedFreeRamMb-derived fallback) is fully retired as an input to
// --claim's capacity math, per RC-3 (freeRamMb inverts under macOS
// memory-compressor pressure — improves as the host approaches freeze).
// Every test below keyed to this fixture and asserting an EXACT granted
// count now expects a count derived from the flat placeholder cap
// (DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis === 3) instead — see each
// rewritten test's own comment. The fixture is kept (still a GREEN,
// low-pressure memory reading — required shape) but its name/exactness
// claim is now historical, not live.
const MEMORY_ONE_SLOT = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 4450 };

// Generous headroom — comfortably several slots free for a cold-start type
// (floor((8192 - 4096) / 350) === 11), used wherever the test only needs "a
// real, non-hanging, non-zero grant happened", not an exact scarcity bound.
const MEMORY_AMPLE = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };

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
 * once the process exits, but (critically, unlike `runCli`/`spawnSync`)
 * does not block this test's event loop while the child runs, so two calls
 * started back-to-back via `Promise.all` actually race against each other
 * for the same coordination-file lock, rather than running one after the
 * other.
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
  workDir = await mkdtemp(join(tmpdir(), 'arm-claim-'));
  historyFilePath = join(workDir, 'history.json');
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

function claim(args, extraEnv = {}) {
  return runCli(args, {
    ARM_HISTORY_FILE: historyFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_BEAT_NOW_TEST_MODE: '1',
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_AMPLE, disk: DISK_GREEN }),
    ...extraEnv,
  });
}

function claimAsync(args, extraEnv = {}) {
  return runCliAsync(args, {
    ARM_HISTORY_FILE: historyFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_ONE_SLOT, disk: DISK_GREEN }),
    ...extraEnv,
  });
}

// ---------------------------------------------------------------------------
// Core correctness property: two concurrent claims for one slot — exactly
// one wins.
// ---------------------------------------------------------------------------

test('two concurrent --claim invocations racing for the SAME flat 3-slot placeholder cap: exactly one gets granted:3, the other granted:0 (Phase 2 rewrite)', async () => {
  // Phase 2 (RC-3): MEMORY_ONE_SLOT's freeRamMb no longer computes a
  // 1-slot cap — capacity is now the flat DEFAULT_ALLOWANCE_CONFIG
  // .maxAgentsMemoryAxis (3) placeholder, regardless of freeRamMb. Each racer
  // now asks for the WHOLE cap (3) rather than 1, so the race-safety property
  // (exactly one winner, one loser — never both win, never both lose) is
  // still exercised against genuine scarcity, just at the new cap size.
  const [resultA, resultB] = await Promise.all([
    claimAsync(['--claim=test-agent:3', '--orchestrator-id=orch-race-a']),
    claimAsync(['--claim=test-agent:3', '--orchestrator-id=orch-race-b']),
  ]);

  expect(resultA.status).toBe(0);
  expect(resultB.status).toBe(0);
  expect(resultA.stderr).toBe('');
  expect(resultB.stderr).toBe('');

  const parsedA = JSON.parse(resultA.stdout);
  const parsedB = JSON.parse(resultB.stdout);

  const grants = [parsedA.granted, parsedB.granted].sort((a, b) => a - b);

  // Never both 3 (that would mean the same 3-slot cap was double-granted)
  // and never both 0 (that would mean the genuinely free capacity was lost
  // rather than awarded to either racer) — exactly one winner, one loser.
  expect(grants).toEqual([0, 3]);
});

// ---------------------------------------------------------------------------
// Requesting more than available grants only the lesser of requested/
// available.
// ---------------------------------------------------------------------------

test('a claim requesting more than is currently available grants only what is genuinely free, never the full ask (Phase 2 rewrite)', () => {
  // Phase 2 (RC-3): MEMORY_ONE_SLOT's freeRamMb=4450 no longer computes
  // a 1-slot cap — capacity is the flat maxAgentsMemoryAxis placeholder (3),
  // regardless of freeRamMb. A request for 5 must therefore grant exactly 3,
  // not the old formula's 1.
  const { status, stdout, stderr } = claim(
    ['--claim=test-agent:5', '--orchestrator-id=orch-over-ask'],
    { ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_ONE_SLOT, disk: DISK_GREEN }) },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');

  const parsed = JSON.parse(stdout);
  expect(parsed.granted).toBe(3);
});

// ---------------------------------------------------------------------------
// A claim made after an advisory --query-capacity read has gone stale (a
// DIFFERENT claim landed in between) can legitimately return a LOWER grant
// than the earlier read implied — expected, not a bug.
// ---------------------------------------------------------------------------

test('a claim made after the advisory --query-capacity read has gone stale reflects the true current state, not the earlier reading (Phase 2 rewrite)', () => {
  // Phase 2 (RC-3): MEMORY_ONE_SLOT's freeRamMb no longer computes a
  // 1-slot cap — the advisory read now reports the flat maxAgentsMemoryAxis
  // placeholder (3). To still exercise "a later claim reflects reality, not
  // the earlier stale reading", the other orchestrator now claims the WHOLE
  // 3-slot cap before this orchestrator's own claim runs.
  const env = { ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_ONE_SLOT, disk: DISK_GREEN }) };

  // The advisory read: the flat placeholder cap reported free.
  const query = claim(['--query-capacity=test-agent'], env);
  expect(query.status).toBe(0);
  expect(query.stderr).toBe('');
  const parsedQuery = JSON.parse(query.stdout);
  expect(parsedQuery['test-agent'].freeCapacity).toBe(3);

  // A DIFFERENT orchestrator claims the entire cap before this orchestrator's
  // own claim runs.
  const otherClaim = claim(['--claim=test-agent:3', '--orchestrator-id=orch-other'], env);
  expect(otherClaim.status).toBe(0);
  expect(JSON.parse(otherClaim.stdout).granted).toBe(3);

  // This orchestrator's own claim, made after the (now-stale) advisory read
  // implied 3 slots were free, must reflect reality — 0 genuinely left — not
  // the earlier reading's figure.
  const lateClaim = claim(['--claim=test-agent:1', '--orchestrator-id=orch-late'], env);
  expect(lateClaim.status).toBe(0);
  expect(lateClaim.stderr).toBe('');
  expect(JSON.parse(lateClaim.stdout).granted).toBe(0);
});

// ---------------------------------------------------------------------------
// A stale lock left by a "crashed" writer is reclaimed exactly per the
// existing mechanism already proven for coordination-file writes — --claim
// still succeeds rather than hanging/timing out.
// ---------------------------------------------------------------------------

test('a stale .lock file left by a crashed writer is reclaimed, and --claim still succeeds rather than hanging', async () => {
  const lockPath = `${coordinationFilePath}.lock`;
  // Well past ./lib/coordination-file.mjs's DEFAULT_LOCK_STALENESS_MS (30s)
  // — an orphaned lock from a writer that was SIGKILL'd/OOM-killed between
  // acquiring the lock and its release, per that module's own documented
  // reclaim contract.
  const staleLockPayload = JSON.stringify({
    pid: 999999,
    acquiredAt: Date.now() - 60_000,
    token: 'stale-lock-left-by-a-crashed-writer',
  });
  await writeFile(lockPath, staleLockPayload, 'utf8');

  const { status, stdout, stderr } = claim(['--claim=test-agent:1', '--orchestrator-id=orch-stale-lock']);

  expect(status).toBe(0);
  expect(stderr).toBe('');
  const parsed = JSON.parse(stdout);
  expect(Number.isInteger(parsed.granted)).toBe(true);
  expect(parsed.granted).toBeGreaterThanOrEqual(0);
}, 20_000);

// ---------------------------------------------------------------------------
// Phase 2 (Build Plan Amendment, RC-3) — `--claim`'s `granted` count
// must no longer be gated by `freeRamMb` (live or the retired
// `assumedFreeRamMb` fallback formula). Same pressureLevel/type-cost, two
// wildly different freeRamMb readings, same requested count — the grant must
// be identical.
// ---------------------------------------------------------------------------

test('`--claim` `granted` count does not vary with freeRamMb at fixed pressureLevel/type cost', () => {
  // Distinct TYPES (not just distinct orchestrator ids) — the coordination
  // ledger tracks grants per type, so reusing one type across both calls
  // within this test's single shared coordination file would let the second
  // call see the first call's grant already deducted, confounding the
  // freeRamMb-invariance being asserted here with ordinary ledger depletion.
  const claimWithFreeRamMb = (freeRamMb, type, orchestratorId) =>
    claim([`--claim=${type}:3`, `--orchestrator-id=${orchestratorId}`], {
      ARM_FAKE_COLLECT_JSON: JSON.stringify({
        memory: { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb },
        disk: DISK_GREEN,
      }),
    });

  const low = claimWithFreeRamMb(4450, 'invariant-type-low', 'orch-invariant-low');
  const high = claimWithFreeRamMb(500_000, 'invariant-type-high', 'orch-invariant-high');

  expect(low.status).toBe(0);
  expect(high.status).toBe(0);
  expect(low.stderr).toBe('');
  expect(high.stderr).toBe('');

  const parsedLow = JSON.parse(low.stdout);
  const parsedHigh = JSON.parse(high.stdout);
  // Distinct orchestrator/type combinations (invariant-type has no prior
  // grants against it in this test's fresh coordination file for either
  // call), so each independently sees the full flat placeholder cap.
  expect(parsedLow.granted).toBe(3);
  expect(parsedHigh.granted).toBe(3);
  expect(parsedLow.granted).toBe(parsedHigh.granted);
});

// ---------------------------------------------------------------------------
// Claiming a type with zero prior history falls through to the same
// cold-start default cost --query-capacity uses.
// ---------------------------------------------------------------------------

test('claiming a never-seen operation type falls back to the cold-start default cost, not an error or a stuck/zero grant', () => {
  // never-claimed-before-type has no --record-outcome history at all.
  const { status, stdout, stderr } = claim(['--claim=never-claimed-before-type:2', '--orchestrator-id=orch-cold-start']);

  expect(status).toBe(0);
  expect(stderr).toBe('');
  const parsed = JSON.parse(stdout);
  // Ample headroom (MEMORY_AMPLE, floor((8192-4096)/350) === 11) comfortably
  // covers the requested 2 slots under the cold-start default cost estimate
  // — proves the fallback produces a real, usable grant, not 0 and not a
  // hang.
  expect(parsed.granted).toBe(2);
});

// ---------------------------------------------------------------------------
// Validation / contract-shape coverage — not explicitly named in the Build
// Plan's edge-case list, but the same "required parameter / malformed value
// hard-rejects" posture every sibling beat type in this file already
// enforces (--record-outcome, --query-capacity) — a --claim implementation
// that silently accepted a malformed invocation would be a regression
// against that established precedent.
// ---------------------------------------------------------------------------

test('--claim without --orchestrator-id is rejected, exit 2, empty stdout', () => {
  const { status, stdout, stderr } = claim(['--claim=test-agent:1']);

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--orchestrator-id/);
});

test('--claim with a missing count segment (no colon) is rejected, exit 2, empty stdout', () => {
  const { status, stdout, stderr } = claim(['--claim=test-agent', '--orchestrator-id=orch-malformed-1']);

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--claim/);
});

test('--claim with a non-integer count is rejected, exit 2, empty stdout', () => {
  const { status, stdout, stderr } = claim(['--claim=test-agent:not-a-number', '--orchestrator-id=orch-malformed-2']);

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--claim/);
});

// pre-PR review (Low): `Number('')` is `0` and `Number.isInteger(0)` is
// `true`, so a truncated `--claim=<type>:` silently parsed as a well-formed
// "claim zero slots" ask and printed `{"granted":0}` with exit 0 — a malformed
// invocation reported as a legitimate denial.
test('--claim with an empty count segment is rejected, exit 2, empty stdout', () => {
  const { status, stdout, stderr } = claim(['--claim=test-agent:', '--orchestrator-id=orch-malformed-empty']);

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--claim/);
});

test('--claim with a negative count is rejected, exit 2, empty stdout', () => {
  const { status, stdout, stderr } = claim(['--claim=test-agent:-1', '--orchestrator-id=orch-malformed-3']);

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--claim/);
});

// Security review follow-up (Low finding, agent-resource-management
// worktree) — mirrors `--declare-dibs`'s existing
// `isReservedEntry` rejection (cli.mjs, main()) so a caller cannot claim
// under the reserved double-underscore sentinel convention (e.g. colliding
// with `LEGACY_UNATTRIBUTED_CLAIM_HOLDER_ID` in lib/coordination-file.mjs).
test('--claim with a reserved-looking --orchestrator-id ("__foo__") is rejected, exit 2, empty stdout', () => {
  const { status, stdout, stderr } = claim(['--claim=test-agent:1', '--orchestrator-id=__foo__']);

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/reserved/);
});

test('--claim combined with --desired-agents in the same invocation is rejected, exit 2', () => {
  const { status, stdout, stderr } = claim([
    '--claim=test-agent:1',
    '--desired-agents=1',
    '--orchestrator-id=orch-combo-1',
  ]);

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--claim/);
  expect(stderr).toMatch(/--desired-agents/);
});

test('--claim combined with --query-capacity in the same invocation is rejected, exit 2', () => {
  const { status, stdout, stderr } = claim([
    '--claim=test-agent:1',
    '--query-capacity=test-agent',
    '--orchestrator-id=orch-combo-2',
  ]);

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--claim/);
  expect(stderr).toMatch(/--query-capacity/);
});

test('--claim combined with --record-outcome in the same invocation is rejected, exit 2', () => {
  const { status, stdout, stderr } = claim([
    '--claim=test-agent:1',
    '--record-outcome',
    '--operation-type=test-agent',
    '--orchestrator-id=orch-combo-3',
  ]);

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--claim/);
  expect(stderr).toMatch(/--record-outcome/);
});

// ---------------------------------------------------------------------------
// Phase 5 — the host probe now runs BEFORE the fencing lock, not inside
// it. These tests pin the CLI-visible consequences of that move: the output
// contract is unchanged, an unhealthy beat no longer touches the lock at all,
// and `sampledAt`/the freshness comparison stay on the one `resolveNow()`
// clock seam.
// ---------------------------------------------------------------------------

describe('--claim with the host probe hoisted out of the critical section', () => {
  const MEMORY_RED = { pressureLevel: 4, swapUsedMb: 9000, compressedMb: 8000, freeRamMb: 128 };
  const DISK_RED = { freeDiskGb: 2, declineRateGbPerHour: 12 };

  test('still prints the same {"granted":n} contract on a healthy beat', () => {
    const result = claim(['--claim=hoisted-type:2', '--orchestrator-id=orc-a']);
    expect(result.status).toBe(0);
    expect(result.stderr).toBe('');
    expect(JSON.parse(result.stdout)).toEqual({ granted: 2 });
  });

  // A legacy-shaped ledger entry is the discriminator these two tests need.
  // "No lock file afterwards" proves nothing on its own — the lock is
  // released either way — and "no coordination file" proves nothing either,
  // since a zero grant writes nothing. But `claimCapacity` migrates a
  // legacy-shaped entry to the `claims[]` form UNCONDITIONALLY on any read
  // that reaches it, even when it grants 0. So the entry surviving in its
  // original scalar shape is direct evidence that `claimCapacity` was never
  // entered at all.
  async function seedLegacyLedger(type) {
    const raw = JSON.stringify([
      { orchestratorId: `__claim:${type}__`, grantedTotal: 2, declaredAt: 1_700_000_000_000 },
    ]);
    await writeFile(coordinationFilePath, raw, 'utf8');
    return raw;
  }

  test('a non-GREEN memory beat never enters the critical section: no lock file, and a legacy ledger entry is left untouched', async () => {
    const seeded = await seedLegacyLedger('red-mem-type');
    const result = claim(['--claim=red-mem-type:3', '--orchestrator-id=orc-a'], {
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
    });
    expect(result.status).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual({ granted: 0 });
    // The point of hoisting the short-circuit ahead of `claimCapacity`: a
    // beat that cannot possibly grant anything must not make every other
    // orchestrator queue behind its lock acquisition.
    expect(existsSync(`${coordinationFilePath}.lock`)).toBe(false);
    expect(await readFile(coordinationFilePath, 'utf8')).toBe(seeded);
  });

  test('a non-GREEN disk beat behaves identically', async () => {
    const seeded = await seedLegacyLedger('red-disk-type');
    const result = claim(['--claim=red-disk-type:3', '--orchestrator-id=orc-a'], {
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_AMPLE, disk: DISK_RED }),
    });
    expect(result.status).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual({ granted: 0 });
    expect(existsSync(`${coordinationFilePath}.lock`)).toBe(false);
    expect(await readFile(coordinationFilePath, 'utf8')).toBe(seeded);
  });

  test('a GREEN beat DOES enter the critical section — the same legacy entry is migrated', async () => {
    // The other half of the discriminator above: proves the legacy-migration
    // side effect really does fire when `claimCapacity` is reached, so its
    // absence in the two tests above is meaningful and not vacuous.
    const seeded = await seedLegacyLedger('green-migrates-type');
    const result = claim(['--claim=green-migrates-type:1', '--orchestrator-id=orc-a']);
    expect(result.status).toBe(0);
    expect(await readFile(coordinationFilePath, 'utf8')).not.toBe(seeded);
  });

  test('ARM_FAKE_NOW_MS drives both the sample stamp and the in-lock freshness comparison, so a frozen clock is never spuriously stale', () => {
    // If the freshness check read a second, independent clock, a frozen
    // `sampledAt` far in the past would compare against real wall-clock now
    // and deny every grant. It grants, so both sides are on the one seam.
    const farPast = 1_600_000_000_000;
    const result = claim(['--claim=frozen-clock-type:2', '--orchestrator-id=orc-a'], {
      ARM_FAKE_NOW_MS: String(farPast),
    });
    expect(result.status).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual({ granted: 2 });
  });

  test('a fresh claim still writes its grant to the ledger and still denies the remainder (Phase 2 rewrite)', async () => {
    // Regression guard on the property the hoist must not weaken: the grant
    // is bounded by the ledger read under the lock, not by the probe.
    //
    // Phase 2 (RC-3): MEMORY_ONE_SLOT's freeRamMb no longer computes a
    // 1-slot cap — capacity is the flat maxAgentsMemoryAxis placeholder (3).
    // The first claim now takes the WHOLE cap (3) so the second claim still
    // has genuinely nothing left to deny-the-remainder against.
    const first = claim(['--claim=remainder-type:3', '--orchestrator-id=orc-a'], {
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_ONE_SLOT, disk: DISK_GREEN }),
    });
    const second = claim(['--claim=remainder-type:1', '--orchestrator-id=orc-b'], {
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_ONE_SLOT, disk: DISK_GREEN }),
    });
    expect(JSON.parse(first.stdout)).toEqual({ granted: 3 });
    expect(JSON.parse(second.stdout)).toEqual({ granted: 0 });
  });

  test('gives up on a permanently-held lock inside its own attempt ceiling, failing closed WITH a warning', async () => {
    // The pairing between DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS and
    // DEFAULT_CLAIM_SAMPLE_FRESHNESS_MS, exercised end to end. A lock stamped
    // NOW is never judged stale, so it is never reclaimed and this claim must
    // exhaust its retries rather than ever acquiring.
    //
    // What is being proven is that the give-up is bounded and LOUD. Left at
    // acquireLock's default ceiling this call could spend 10-20s waiting,
    // acquire the lock, and then be denied on sample staleness — the same
    // `granted: 0`, but after most of a beat, and (before its review
    // round 2 gave that arm its own warning) with empty stderr.
    await writeFile(
      `${coordinationFilePath}.lock`,
      JSON.stringify({ pid: 999_999, acquiredAt: Date.now(), token: 'held-by-someone-else' }),
      'utf8',
    );

    const startedAt = Date.now();
    const result = claim(['--claim=contended-type:2', '--orchestrator-id=orc-a']);
    const elapsed = Date.now() - startedAt;

    expect(result.status).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual({ granted: 0 });
    expect(result.stderr).toContain('COORDINATION_LOCK_TIMEOUT');
    expect(result.stderr).toContain('granting nothing for this claim');
    // The claim being asserted is exactly the pairing's claim: the lock wait
    // finishes inside the freshness window. Asserted against that window
    // itself rather than a tighter number picked for looking impressive — 800
    // rounds of open/looksStale syscalls are not bounded on a loaded shared
    // runner, and a tighter bound would go red for the runner being slow
    // rather than for the pairing being wrong.
    expect(elapsed).toBeLessThan(20_000);
  }, 40_000);
});

// ---------------------------------------------------------------------------
// The contention benchmark DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS is sized on
// (review round 2).
//
// The review finding this answers: cutting the lock-wait ceiling from ~10-20s
// to ~4-8s "could make `--claim` fail closed far more often under real
// contention". The existing evidence did not settle it — the only multi-way
// contention exercise in the suite (lib/coordination-file.jest.spec.mjs's
// post-reclaim race) runs EIGHT claimants against a DELIBERATELY WIDENED
// critical section: 20,000 filler entries, a 1.8 MB read-parse-serialise-
// write cycle, and a 5 ms staleness threshold, all chosen to force lock loss
// on purpose. That is a fencing-correctness fixture, not this host's
// operating point, and a denial rate measured there says nothing about the
// ceiling's sizing.
//
// This measures the operating point instead: FOUR concurrent claimants — the
// real configured agent ceiling for this host — against a NORMAL-width
// critical section (no filler, default staleness, the same code path an
// orchestrator beat actually takes).
//
// Phase 2 (Build Plan Amendment, RC-3) REWRITE: this test originally
// relied on MEMORY_AMPLE's freeRamMb=8192 computing 11 genuinely-free slots
// (floor((8192-4096)/350) === 11) under the retired formula — with 11 free
// slots and four claimants each asking for one, capacity could never be the
// reason a claim was denied, so any `granted: 0` was attributable to the
// lock and nothing else. RC-3 retires freeRamMb as a capacity input
// entirely: every type now gets the same flat
// DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis (3) placeholder cap
// regardless of freeRamMb, so MEMORY_AMPLE's 11-slot headroom no longer
// means anything here. Four claimants asking for one slot each against a
// cap of 3 now hit a REAL, deterministic capacity denial — exactly one of
// the four every round, since 4 asks > 3 available — that is not lock
// contention at all. The test still proves the thing cares about (the
// lock never costs anyone a slot beyond capacity), but it now has to
// separate the two denial causes explicitly instead of assuming capacity is
// never in play: a lock-contention denial is distinguishable by its
// COORDINATION_LOCK_TIMEOUT stderr (see the "gives up on a permanently-held
// lock" test above); a capacity denial has empty stderr, exactly like every
// other honest `granted: 0` in this file.
// ---------------------------------------------------------------------------

describe('lock-contention denial rate at the real configured agent ceiling', () => {
  const CONFIGURED_AGENT_CEILING = 4;
  const ROUNDS = 6;

  it('4 concurrent --claim invocations for the flat 3-slot cap: exactly one capacity denial per round, and lock contention never costs an additional slot (Phase 2 rewrite)', async () => {
    // Measured across a wider sweep than CI should pay for on every run —
    // 4-way x 30 rounds (120 claims), 8-way x 15 (120), 16-way x 15 (240),
    // and 48-way x 5 (240) — all recorded in DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS's
    // own comment. Zero lock-contention denials at every concurrency; the
    // worst full round took 75 ms at 4-way and 393 ms even at 48-way, against
    // a ~4000 ms floor on the ceiling's budget. This test pins the 4-way row
    // so the claim stays evidence rather than a story about a measurement
    // someone once took.
    let claims = 0;
    let capacityDenials = 0;
    let lockContentionDenials = 0;
    const lockDenialStderr = [];

    for (let round = 0; round < ROUNDS; round++) {
      const type = `contention-round-${round}`;
      const results = await Promise.all(
        Array.from({ length: CONFIGURED_AGENT_CEILING }, (_, index) =>
          claimAsync([`--claim=${type}:1`, `--orchestrator-id=orc-contend-${index}`], {
            ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_AMPLE, disk: DISK_GREEN }),
          }),
        ),
      );

      for (const result of results) {
        expect(result.status).toBe(0);
        claims++;
        if (JSON.parse(result.stdout).granted === 0) {
          if (result.stderr.includes('COORDINATION_LOCK_TIMEOUT')) {
            lockContentionDenials++;
            lockDenialStderr.push(result.stderr.trim());
          } else {
            capacityDenials++;
          }
        }
      }
    }

    // Non-vacuity: prove the sweep actually ran the intended number of real
    // races, so a future refactor that silently skips the loop cannot leave a
    // zero-denial assertion passing for the wrong reason.
    expect(claims).toBe(CONFIGURED_AGENT_CEILING * ROUNDS);

    // Four askers, three slots: exactly one capacity denial every round —
    // deterministic, not probabilistic — while lock contention (identified
    // by its own COORDINATION_LOCK_TIMEOUT stderr) must still never cost
    // anyone a slot on top of that.
    expect({ capacityDenials, lockContentionDenials, lockDenialStderr }).toEqual({
      capacityDenials: ROUNDS,
      lockContentionDenials: 0,
      lockDenialStderr: [],
    });
  }, 120_000);
});

// ---------------------------------------------------------------------------
// Observability: a denial must name its own cause (review round 2).
//
// The reviewer's core complaint was not that the ceiling is wrong — the
// benchmark above shows it is not — but that a wrong ceiling would surface
// only as "claims mysteriously stopped working". These assert the end-to-end
// path: the classification in lib/claim-denial.mjs actually reaches stderr of
// the real process. The classification's own branches are unit-covered in
// lib/claim-denial.jest.spec.mjs; cli.mjs is outside the coverage gate, so
// this is the only place the wiring itself is proven.
// ---------------------------------------------------------------------------

describe('a --claim denial names the constant that governs it', () => {
  it('a contended lock indicts DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS, with the window it bought', async () => {
    // Stamped NOW, so it is never judged stale, never reclaimed, and this
    // claim must exhaust its retries rather than ever acquiring — the exact
    // shape a genuinely under-sized ceiling would produce in production.
    await writeFile(
      `${coordinationFilePath}.lock`,
      JSON.stringify({ pid: 999_999, acquiredAt: Date.now(), token: 'held-by-someone-else' }),
      'utf8',
    );

    const result = claim(['--claim=observable-timeout:1', '--orchestrator-id=orc-a']);

    expect(JSON.parse(result.stdout)).toEqual({ granted: 0 });
    // The code token stays greppable — it is what an stderr collector counts
    // occurrences by, since a persisted counter is impossible on precisely
    // this path (writing it needs the lock that just timed out).
    expect(result.stderr).toContain('COORDINATION_LOCK_TIMEOUT');
    // The ceiling, and the wall-clock window it actually bought. "800" alone
    // tells a reader nothing at 2am.
    expect(result.stderr).toContain('800 attempts');
    expect(result.stderr).toContain('~4-8s');
    // And the conclusion to draw if it RECURS — one occurrence on a busy
    // host is not a mis-sized constant.
    expect(result.stderr).toContain('DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS');
    expect(result.stderr).toContain('recurs');
  }, 40_000);

  // The THIRD denial source — a hoisted sample that aged out before the lock
  // was acquired — is deliberately NOT exercised here. It throws nothing and
  // fires only after DEFAULT_CLAIM_SAMPLE_FRESHNESS_MS (20s) of real elapsed
  // time, and ARM_FAKE_NOW_MS drives the sample stamp and the in-lock
  // comparison from the SAME clock (see the frozen-clock test above), so a
  // frozen clock can never age a sample out. Reaching it black-box would mean
  // adding a production env seam whose only purpose is to observe a warning.
  // It is covered where it can be covered honestly instead: lib/probe-bound's
  // `onStale` fires exactly on the stale branch and never on the happy path,
  // lib/claim-denial owns the message, and doc-honesty.jest.spec.mjs pins the
  // cli.mjs wiring between them structurally.
});

// ---------------------------------------------------------------------------
// review follow-up (Medium): `live-agent-admission` is an internal-only
// ledger written exclusively by the `--desired-agents` beat's `reserveAdmission`
// call — SKILL.md and cli.mjs's own `LIVE_AGENT_ADMISSION_CLAIM_TYPE` doc
// comment assert it is "never seeded, claimed, or released via the public
// `--claim`/`--release` flags." Before this fix, nothing enforced that: a
// stray `--claim=live-agent-admission:N` silently inflated a ledger that
// `handleClaim`'s own `computeAvailableCapacity` (for `live-agent`) subtracts
// from every OTHER orchestrator's headroom, with host-wide blast radius for
// up to `DEFAULT_FRESHNESS_WINDOW_MS`.
// ---------------------------------------------------------------------------

test('--claim=live-agent-admission:N is rejected outright (exit 2) — that ledger is internal-only [ regression]', () => {
  const result = claim(['--claim=live-agent-admission:3', '--orchestrator-id=orc-tries-admission-claim']);

  expect(result.status).toBe(2);
  expect(result.stdout).toBe('');
  expect(result.stderr).toContain('live-agent-admission');
  expect(result.stderr).toContain('internal-only');
});

// ---------------------------------------------------------------------------
// Phase 3 — mirrors the `live-agent-admission` guard directly above:
// `idle-probe` (IDLE_PROBE_CLAIM_TYPE) is the same posture, internal-only,
// written exclusively by the `--desired-agents` beat's `--user-attests-idle`
// path — see that constant's own doc comment in cli.mjs. A stray
// `--claim=idle-probe:N` must be rejected the same way.
// ---------------------------------------------------------------------------

test('--claim=idle-probe:N is rejected outright (exit 2) — that ledger is internal-only [ ]', () => {
  const result = claim(['--claim=idle-probe:3', '--orchestrator-id=orc-tries-idle-probe-claim']);

  expect(result.status).toBe(2);
  expect(result.stdout).toBe('');
  expect(result.stderr).toContain('idle-probe');
  expect(result.stderr).toContain('internal-only');
});
