// Phase 0 outer acceptance test ("arm: wire cdk.out* cleanup
// into the disk axis's AMBER response — follow-up from ").
//
// This file is the outer loop for the rev-2 Build Plan. It drives the REAL
// `cli.mjs` end-to-end as a child process against a REAL, self-constructed
// scratch temp root and asserts the SEVEN green conditions the plan's
// "### Outer acceptance test" section enumerates:
//
//   1. Declining/AMBER disk trend + opt-in  -> the emitted verdict JSON
//      carries `cleanup: { removedCount, reclaimedBytes, errors,
//      truncatedCount, budgetExhausted }` and a real seeded stale
//      `cdk.out-*` dir in the scratch root is GONE.
//   2. The same holds on the `pressure-block` and `memory-projection-block`
//      short-circuit emission literals.
//   3. GREEN/stable trend, no-evidence trends, and any beat WITHOUT the
//      opt-in -> NO `cleanup` key and no sweep.
//   4. `--advise-only` + declining trend + opt-in -> no `cleanup` key, the
//      seeded stale candidate SURVIVES. Unconditionally.
//   5. A sweep that rejects / reports errors / stops on its budget never
//      changes the beat's `type` and never crashes `main()`.
//   6. A sweep root outside the platform temp allowlist — including a
//      repo-RELATIVE `TMPDIR` — yields `cleanup.errors[0].kind ===
//      'confinement-refused'` and ZERO removals.
//   7. The wall clock the sweep adds to the beat never exceeds
//      `SWEEP_BUDGET_MS`.
//
// ---------------------------------------------------------------------------
// RED BY CONSTRUCTION — the expected failure, and why it is the RIGHT failure
// ---------------------------------------------------------------------------
//
// On `origin/master`, all three of the following are true:
//
//   a. `./lib/cleanup-temp-root-anchor.mjs` DOES NOT EXIST (Phase 1 builds
//      it). The top-level import below therefore fails module resolution
//      with `ERR_MODULE_NOT_FOUND` — that is this file's documented red
//      state, not a typo.
//   b. `./lib/cleanup.mjs` exists but exports NEITHER
//      `sweepStaleTempDirsAsync` NOR `SWEEP_BUDGET_MS` /
//      `AUTO_TRIGGER_AGE_HOURS` (Phase 2 adds them). Those named imports
//      fail ESM link-time resolution.
//   c. `cli.mjs` emits no `cleanup` key on ANY path — there is no call site
//      for either sweep export today (Phase 3 wires it).
//
// Do NOT add a stub anchor module, a stub async export, or a fake `cleanup`
// key to turn this file green. Phase 1 lands the anchor, Phase 2 the budgeted
// sweep, Phase 3 the `cli.mjs` wiring — and only then does this file pass.
//
// ---------------------------------------------------------------------------
// SAFETY — the single most important property of this file (rev-2's reason)
// ---------------------------------------------------------------------------
//
// This file NEVER reads or writes the process's real `$TMPDIR`. The host
// `$TMPDIR` currently holds 3,000+ real `cdk.out` dirs that must never be
// touched by any test run (see `lib/cleanup.jest.spec.mjs:86-99`'s doc comment
// and `cleanup-outer-acceptance.jest.spec.mjs`'s note 1).
//
// Every sweep root this file exercises is constructed with `fs.mkdtempSync`
// and handed to the child process explicitly, using the established safe
// per-child idiom:
//
//     spawnSync('node', [CLI, ...args], { env: { ...process.env, TMPDIR: scratchDir } })
//
// `process.env.TMPDIR` is NEVER mutated inside this Jest process. Each scratch
// root is torn down unconditionally in `afterEach`, even when an assertion has
// already failed mid-test.
//
// ---------------------------------------------------------------------------
// SHAPES THIS FILE PINS (test-author decisions — the plan fixes the contract
// but leaves these names to the test author; Phase 3's implementer must match
// them, or update this file and say why in the phase handoff)
// ---------------------------------------------------------------------------
//
// 1. `cleanup` is an ADDITIVE top-level sibling key on the emitted verdict
//    JSON — exactly the `diskTrend` precedent from (see
//    `disk-trend.jest.spec.mjs`'s design note 1). It appears on the normal
//    spread-assembled verdict AND on the `pressure-block` /
//    `memory-projection-block` literals. Its ABSENCE means "the trigger did
//    not fire"; a present `cleanup` with `removedCount: 0` means "fired,
//    found nothing" — the plan's Phase 3 edge case requires those two to be
//    distinguishable, which is why this file asserts key presence/absence
//    (`'cleanup' in parsed`) rather than truthiness.
//
// 2. TEST-MODE EXTRA-REFUSAL FLAG: `ARM_TEST_ALLOW_TEMP_SWEEP=1`. Plan Phase 3
//    suppression 2 requires the sweep to REFUSE whenever
//    `ARM_COORDINATION_FILE` is set (the suite's own test-mode marker)
//    "unless an explicit test-only sweep-root override is also present".
//    This file pins that override as an EXTRA REFUSAL, NOT as the sweep root
//    and NOT as an alternative arming route: the root itself still derives
//    only from `cli.mjs`'s own resolved temp root (which this file controls
//    per-child via `TMPDIR`), which is what keeps Phase 4's sweep-root-
//    provenance fitness assertion ("never argv, never the coordination file,
//    never an `ARM_*` override") true at the same time.
//
// Phase 3 review (High): the flag is ANDed with the real production
//    gate, never substituted for it. It can only ever subtract permission.
//    Belt and braces, per the plan: every spec here ALSO uses the per-child
//    `TMPDIR` idiom, so a regression in the refusal itself still cannot reach
//    the host temp root.
//
// 3. ARMING is seeded through the REAL `recordDiskCleanupArmed` primitive from
//    `./lib/coordination-file.mjs` (Phase 3's reserved, never-pruned
//    `__disk-cleanup-armed__` row) — never a hand-written JSON fixture, and
//    never the ever-opted-in row, so this file exercises exactly the gate
//    production reads.
//
// 4. TREND is driven exactly as `disk-trend.jest.spec.mjs` drives it: two
//    sequential real `node cli.mjs` beats against one coordination file, with
//    `ARM_FAKE_COLLECT_JSON` supplying each beat's `freeDiskGb` and
//    `ARM_FAKE_NOW_MS` (+ its two required master switches) simulating real
//    elapsed wall clock. Fake `now` values here are derived from the REAL
//    clock (`Date.now()`), deliberately: the sweep's own staleness decision
//    compares against real file mtimes, so a 2023-epoch fake `now` would make
//    every seeded candidate look like it came from the future and silently
//    turn every removal assertion into a false negative.
//
// ---------------------------------------------------------------------------
// SURFACED DESIGN OBSERVATION — RESOLVED by the `__disk-cleanup-armed__` row
// ---------------------------------------------------------------------------
//
// `declareDibs` unions the beat's own orchestrator-id into the
// `__ever-opted-in__` set as a side effect of declaring capacity, and it does
// so BEFORE the trend/trigger evaluation point. Had the sweep been gated on
// THAT set, every dibs-declaring beat would have armed itself and "a dibs beat
// without the opt-in" would not have been a reachable state at all.
//
// That is precisely why Phase 3 introduced a SEPARATE row,
// `__disk-cleanup-armed__`, written only by the standalone
// `--enable-disk-cleanup` beat. No capacity path touches it, so an un-armed
// dibs beat IS reachable — and `condition 3` pins it directly (see "a beat
// that declares dibs ... is still not armed"), which is the regression test
// for Phase 3's review round-1 High finding.
//
// ---------------------------------------------------------------------------
// SCOPE BOUNDARY — what this outer file deliberately does NOT do
// ---------------------------------------------------------------------------
//
// Condition 7's "even with an arbitrarily slow fs" cannot be expressed against
// a real child process: there is no seam to slow the child's `fs/promises`
// down from here. That mocked-slow / mocked-hung assertion is Phase 4's
// bounded-latency fitness function (`jest.unstable_mockModule`, asserted
// against the mocked clock and the budget constant, never a wall-clock
// figure). What THIS file pins is the observable end of the same property:
// against a deliberately oversized real tree the sweep reports that it stopped
// on its own bound (`budgetExhausted` / `candidatesCapped`) rather than
// running to completion, and the measured added beat latency stays inside
// `SWEEP_BUDGET_MS` plus one documented process-noise allowance.

import { spawnSync } from 'node:child_process';
import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  statSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

// RED (a): this module does not exist until Phase 1. `classifySweepRoot` is
// imported — not merely the module — so the anchor's real contract, not a
// side-effect import, is what this file depends on.
import { classifySweepRoot } from './lib/cleanup-temp-root-anchor.mjs';

// RED (b): `./lib/cleanup.mjs` exists, but none of these three exports do
// until Phase 2. `SWEEP_BUDGET_MS` and `AUTO_TRIGGER_AGE_HOURS` are imported
// rather than restated as literals precisely so this file cannot drift from
// the real constants (plan Phase 4: "must import both constants rather than
// restating them, or it will pass while drifting").
import {
  AUTO_TRIGGER_AGE_HOURS,
  SWEEP_BUDGET_MS,
  SWEEP_COOLDOWN_MS,
  sweepStaleTempDirsAsync,
} from './lib/cleanup.mjs';

// Real, already-shipped primitive (/ ) — the opt-in seam.
import { hasEverOptedIn, recordDiskCleanupArmed } from './lib/coordination-file.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const CLI = join(__dirname, 'cli.mjs');
// scripts/agent-resource-management -> scripts -> repo root.
const REPO_ROOT = join(__dirname, '..', '..');

const HOUR_MS = 60 * 60 * 1000;
const FIVE_MINUTES_MS = 5 * 60 * 1000;

/** Test-author-pinned arming flag — see shape note 2. */
const TEST_SWEEP_ARM_ENV = 'ARM_TEST_ALLOW_TEMP_SWEEP';

/** Mirrors every sibling spec's MEMORY_GREEN fixture — memory held comfortably GREEN so only the disk axis is in play. */
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };
/** Mirrors pressure-override.jest.spec.mjs's MEMORY_WARN — pressureLevel 2 alone trips main()'s unmaskable pressure pre-check. */
const MEMORY_WARN = { pressureLevel: 2, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };

// Disk fixtures, chosen against DEFAULT_DISK_THRESHOLDS (cli.mjs:182 —
// greenFreeDiskGbAbove: 30, amberHoursToFullBelow: 2, redFreeDiskGbBelow: 10)
// so each test exercises exactly ONE of classifyDiskAxis's routes:
//
//   DECLINING_GREEN: 100 -> 97.5 GB over 5 minutes = 30 GB/h decline.
//     hoursToFull = 97.5/30 = 3.25h (> 2h, so NOT the steep-decline AMBER
//     route) and freeDiskGb > 30 (so the axis stays GREEN). This isolates the
//     plan's `trend === 'declining'` trigger route from the AMBER routes.
//   AMBER_LOW_FREE: 25 GB held flat — trend `stable`, axis AMBER purely via
//     the low-free-space route (10 <= 25 <= 30). The plan's second trigger
//     route.
//   GREEN_STABLE: 60 GB held flat — GREEN, `stable`. Must never sweep.
const DECLINING_FREE_GB_FIRST = 100;
const DECLINING_FREE_GB_SECOND = 97.5;
/**
 * A THIRD point on the same 30 GB/h decline (97.5 -> 95 over the next 5
 * minutes), for the one test that needs TWO measured beats rather than one.
 *
 * `computeDiskTrend` reads exactly the single most-recent persisted sample
 * (`cli.mjs`'s `computeDiskTrend`: `elapsedMs <= 0` is reported as
 * `insufficient-data`), so a beat run at the SAME fake instant as the beat
 * before it has no trend at all — a disarmed "control" beat therefore consumes
 * the very trend the armed beat under test needs. Condition 7 advances the
 * clock and the reading again for its armed beat instead of reusing the
 * control's instant. hoursToFull = 95/30 = 3.17h (> 2h) and 95 > 30, so the
 * axis stays GREEN and the `declining` route stays the only one in play,
 * exactly as for the first two points.
 */
const DECLINING_FREE_GB_THIRD = 95;
const AMBER_LOW_FREE_GB = 25;
const GREEN_STABLE_FREE_GB = 60;

/**
 * Generous, DOCUMENTED allowance for child-process spawn/JIT variance on a
 * loaded 16 GB host. Condition 7's real teeth are the sweep's own
 * self-reported bound (`budgetExhausted`/`candidatesCapped`), asserted
 * alongside; this figure only exists so the latency arm of the assertion is
 * not a flake generator. Deliberately NOT tightened: a tighter number would
 * make CI red for reasons that have nothing to do with the sweep.
 */
const SPAWN_NOISE_ALLOWANCE_MS = 1_500;

/** Runs the real cli.mjs as a child process — never imports it directly (cli.mjs is excluded from the coverage gate by design). */
function runCli(args, extraEnv, spawnOptions = {}) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
    ...spawnOptions,
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/** The single JSON document cli.mjs writes to stdout. Parsing failure here IS a finding — the PreToolUse hook JSON.parses this same stream and fails closed. */
function parseVerdict(stdout) {
  return JSON.parse(stdout);
}

/** Directory containing `fileSizeBytes` of real content, with both the file and the dir backdated to `ageHours` ago (mtime, not process ancestry, is its confirmed attribution rule). */
function seedDir(dirPath, { ageHours, fileSizeBytes = 4096 }) {
  mkdirSync(dirPath, { recursive: true });
  const filePath = join(dirPath, 'payload.bin');
  writeFileSync(filePath, Buffer.alloc(fileSizeBytes, 1));
  const backdatedSeconds = (Date.now() - ageHours * HOUR_MS) / 1000;
  utimesSync(filePath, backdatedSeconds, backdatedSeconds);
  utimesSync(dirPath, backdatedSeconds, backdatedSeconds);
  return dirPath;
}

/** A candidate comfortably past the auto-trigger threshold, with its child ALSO backdated so Phase 2's one-level live-writer probe cannot suspect it. */
function seedStaleCandidate(root, name) {
  return seedDir(join(root, name), { ageHours: AUTO_TRIGGER_AGE_HOURS + 4 });
}

/** A correctly-named but too-young candidate — must survive on every path. */
function seedFreshCandidate(root, name) {
  return seedDir(join(root, name), { ageHours: AUTO_TRIGGER_AGE_HOURS / 2, fileSizeBytes: 512 });
}

/** Recursive real byte total, computed independently of whatever the production code sums, so this file never just echoes the implementation's own arithmetic back at itself. */
function realDirSizeBytes(dirPath) {
  let total = 0;
  for (const entry of readdirSync(dirPath, { withFileTypes: true })) {
    const entryPath = join(dirPath, entry.name);
    total += entry.isDirectory() ? realDirSizeBytes(entryPath) : statSync(entryPath).size;
  }
  return total;
}

let sweepRoot;
let workDir;
let coordinationFilePath;
let historyFilePath;
/** Paths whose mode this test narrowed; restored before teardown so `rmSync` can always complete. */
let modeRestorePaths;
/** A scratch root deliberately OUTSIDE any platform temp prefix (condition 6). Created lazily. */
let nonTempRoot;

beforeEach(() => {
  // The ONLY directories this file ever creates, sweeps or deletes. Never the
  // real host $TMPDIR (see the SAFETY section above).
  sweepRoot = mkdtempSync(join(tmpdir(), 'arm-cleanup-autotrigger-sweep-'));
  workDir = mkdtempSync(join(tmpdir(), 'arm-cleanup-autotrigger-work-'));
  coordinationFilePath = join(workDir, 'coordination.json');
  historyFilePath = join(workDir, 'history.json');
  modeRestorePaths = [];
  nonTempRoot = null;
});

afterEach(() => {
  for (const path of modeRestorePaths) {
    try {
      chmodSync(path, 0o755);
    } catch {
      // Best effort — a path the test already removed is fine.
    }
  }
  rmSync(sweepRoot, { recursive: true, force: true });
  rmSync(workDir, { recursive: true, force: true });
  if (nonTempRoot) rmSync(nonTempRoot, { recursive: true, force: true });
});

/**
 * Builds one beat's env. Every object literal that sets `ARM_FAKE_NOW_MS`
 * also sets `ARM_COORDINATION_FILE` AND `ARM_BEAT_NOW_TEST_MODE` in the same
 * literal — cli.mjs's `resolveNow()` needs the strict three-way AND, and
 * `env-seam-premise.jest.spec.mjs` enforces the pairing across this whole
 * directory's specs.
 *
 * `TMPDIR` is set PER CHILD only. This process's own `process.env.TMPDIR` is
 * never mutated.
 */
function beatEnv({ nowMs, freeDiskGb, memory = MEMORY_GREEN, root, armSweep, extra = {} }) {
  return {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_BEAT_NOW_TEST_MODE: '1',
    ARM_FAKE_NOW_MS: String(nowMs),
    // `declineRateGbPerHour: 999` is a decoy, exactly as disk-trend.jest.spec
    // .mjs uses it: a correct implementation derives its own rate from two
    // persisted timestamped samples and never passes this literal through.
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory, disk: { freeDiskGb, declineRateGbPerHour: 999 } }),
    TMPDIR: root,
    ...(armSweep ? { [TEST_SWEEP_ARM_ENV]: '1' } : {}),
    ...extra,
  };
}

/**
 * Runs the earlier of the two beats that establish a real trend, and returns
 * the fake-clock instant the SECOND beat should use.
 *
 * Deliberately NOT armed: on this beat the trend is still `insufficient-data`,
 * so a correct implementation would not sweep anyway — leaving it disarmed
 * means a regression in the trend gate cannot silently consume the seeded
 * candidates before the beat under test ever runs.
 */
function primeTrend({ freeDiskGb, baseNowMs, orchestratorId = 'orch-1691-prime' }) {
  const primed = runCli(
    [`--orchestrator-id=${orchestratorId}`],
    beatEnv({ nowMs: baseNowMs, freeDiskGb, root: sweepRoot, armSweep: false }),
  );
  expect(primed.status).toBe(0);
  return baseNowMs + FIVE_MINUTES_MS;
}

/** Asserts the exact five-field `cleanup` shape the plan's green condition 1 enumerates. */
function expectCleanupShape(cleanup) {
  expect(cleanup).toBeDefined();
  expect(typeof cleanup.removedCount).toBe('number');
  expect(typeof cleanup.reclaimedBytes).toBe('number');
  expect(Array.isArray(cleanup.errors)).toBe(true);
  expect(typeof cleanup.truncatedCount).toBe('number');
  expect(typeof cleanup.budgetExhausted).toBe('boolean');
}

// ===========================================================================
// Premise guard — the Phase 1/2 seams this whole file leans on actually exist
// and mean what the plan says. Runs first so a red run names the missing
// module, not a downstream assertion.
// ===========================================================================

describe('premise — the Phase 1 anchor and Phase 2 async sweep exist as the plan specifies', () => {
  test('the anchor allows an fs.mkdtempSync scratch root and refuses the repo root', () => {
    expect(typeof classifySweepRoot).toBe('function');
    expect(classifySweepRoot(sweepRoot).allowed).toBe(true);
    expect(classifySweepRoot(REPO_ROOT).allowed).toBe(false);
  });

  test('the async sweep and its budget constant are exported, and the budget is a positive finite number', () => {
    expect(typeof sweepStaleTempDirsAsync).toBe('function');
    expect(Number.isFinite(SWEEP_BUDGET_MS)).toBe(true);
    expect(SWEEP_BUDGET_MS).toBeGreaterThan(0);
    expect(Number.isFinite(AUTO_TRIGGER_AGE_HOURS)).toBe(true);
    expect(AUTO_TRIGGER_AGE_HOURS).toBeGreaterThan(0);
  });
});

// ===========================================================================
// Green condition 1 — declining/AMBER + opt-in sweeps, reports, and the real
// seeded stale dir is gone.
// ===========================================================================

describe('condition 1 — declining/AMBER disk trend with the opt-in present sweeps and reports', () => {
  test('a declining-trend beat emits the five-field cleanup payload and the seeded stale cdk.out-* dir is gone', async () => {
    const orchestratorId = 'orch-1691-declining';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);

    const stale = seedStaleCandidate(sweepRoot, 'cdk.out-declining-stale');
    const fresh = seedFreshCandidate(sweepRoot, 'cdk.out-declining-fresh');
    const unrelated = seedDir(join(sweepRoot, 'some-other-thing'), {
      ageHours: AUTO_TRIGGER_AGE_HOURS + 4,
    });
    const expectedReclaimedBytes = realDirSizeBytes(stale);

    const secondNowMs = primeTrend({
      freeDiskGb: DECLINING_FREE_GB_FIRST,
      baseNowMs: Date.now(),
    });

    const { status, stdout } = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: secondNowMs,
        freeDiskGb: DECLINING_FREE_GB_SECOND,
        root: sweepRoot,
        armSweep: true,
      }),
    );

    expect(status).toBe(0);
    const parsed = parseVerdict(stdout);

    // The trend gate really did see a decline — if this is not `declining`
    // the rest of the test is asserting nothing.
    expect(parsed.diskTrend.trend).toBe('declining');

    expectCleanupShape(parsed.cleanup);
    expect(parsed.cleanup.removedCount).toBe(1);
    expect(parsed.cleanup.reclaimedBytes).toBe(expectedReclaimedBytes);
    expect(parsed.cleanup.errors).toEqual([]);

    // The observable outcome, not just the report.
    expect(existsSync(stale)).toBe(false);
    expect(existsSync(fresh)).toBe(true);
    expect(existsSync(unrelated)).toBe(true);
  });

  test('the low-free-space AMBER route also sweeps, on a stable trend', async () => {
    const orchestratorId = 'orch-1691-amber-low-free';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);
    const stale = seedStaleCandidate(sweepRoot, 'cdk.out-amber-stale');

    const secondNowMs = primeTrend({ freeDiskGb: AMBER_LOW_FREE_GB, baseNowMs: Date.now() });

    const { status, stdout } = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: secondNowMs,
        freeDiskGb: AMBER_LOW_FREE_GB,
        root: sweepRoot,
        armSweep: true,
      }),
    );

    expect(status).toBe(0);
    const parsed = parseVerdict(stdout);
    expectCleanupShape(parsed.cleanup);
    expect(parsed.cleanup.removedCount).toBe(1);
    expect(existsSync(stale)).toBe(false);
  });

  test('a fired-but-found-nothing sweep still emits the cleanup key, so it is distinguishable from not firing', async () => {
    const orchestratorId = 'orch-1691-nothing-to-do';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);
    const fresh = seedFreshCandidate(sweepRoot, 'cdk.out-nothing-fresh');

    const secondNowMs = primeTrend({
      freeDiskGb: DECLINING_FREE_GB_FIRST,
      baseNowMs: Date.now(),
    });

    const { status, stdout } = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: secondNowMs,
        freeDiskGb: DECLINING_FREE_GB_SECOND,
        root: sweepRoot,
        armSweep: true,
      }),
    );

    expect(status).toBe(0);
    const parsed = parseVerdict(stdout);
    expect('cleanup' in parsed).toBe(true);
    expect(parsed.cleanup.removedCount).toBe(0);
    expect(parsed.cleanup.reclaimedBytes).toBe(0);
    expect(existsSync(fresh)).toBe(true);
  });
});

// ===========================================================================
// Green condition 2 — the two short-circuit emission literals.
// ===========================================================================

describe('condition 2 — cleanup attaches to the pressure-block and memory-projection-block literals', () => {
  test('a pressure-blocked beat with a declining trend carries cleanup, and its type is still pressure-block', async () => {
    const orchestratorId = 'orch-1691-pressure-block';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);
    const stale = seedStaleCandidate(sweepRoot, 'cdk.out-pressure-stale');

    const secondNowMs = primeTrend({
      freeDiskGb: DECLINING_FREE_GB_FIRST,
      baseNowMs: Date.now(),
    });

    const { status, stdout } = runCli(
      [`--orchestrator-id=${orchestratorId}`, '--desired-agents=1'],
      beatEnv({
        nowMs: secondNowMs,
        freeDiskGb: DECLINING_FREE_GB_SECOND,
        memory: MEMORY_WARN,
        root: sweepRoot,
        armSweep: true,
      }),
    );

    expect(status).toBe(0);
    const parsed = parseVerdict(stdout);
    // The literal itself is unchanged — the sweep is additive, never a
    // verdict influence.
    expect(parsed.type).toBe('pressure-block');
    expectCleanupShape(parsed.cleanup);
    expect(parsed.cleanup.removedCount).toBe(1);
    expect(existsSync(stale)).toBe(false);
  });

  test('a memory-projection-blocked beat with a declining trend carries cleanup, and its type is still memory-projection-block', async () => {
    const orchestratorId = 'orch-1691-memory-projection-block';
    const agentClass = 'oversized-agent-1691';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);

    // Mirrors cli-memory-projection-wiring.jest.spec.mjs's seedHistory: a
    // single class whose recorded peaks alone blow any margined 16 GiB budget.
    for (const [index, peakMemoryMb] of [15_000, 15_500, 15_800, 16_000].entries()) {
      const seeded = runCli(
        [
          '--record-outcome',
          `--operation-type=${agentClass}`,
          `--orchestrator-id=orch-1691-seed-${index}`,
          `--peak-memory-mb=${peakMemoryMb}`,
        ],
        { ARM_HISTORY_FILE: historyFilePath },
      );
      expect(seeded.status).toBe(0);
    }

    const stale = seedStaleCandidate(sweepRoot, 'cdk.out-projection-stale');
    const secondNowMs = primeTrend({
      freeDiskGb: DECLINING_FREE_GB_FIRST,
      baseNowMs: Date.now(),
    });

    const { status, stdout } = runCli(
      [`--orchestrator-id=${orchestratorId}`, '--desired-agents=1', `--agent-class=${agentClass}`],
      beatEnv({
        nowMs: secondNowMs,
        freeDiskGb: DECLINING_FREE_GB_SECOND,
        root: sweepRoot,
        armSweep: true,
        extra: { ARM_HISTORY_FILE: historyFilePath },
      }),
    );

    expect(status).toBe(0);
    const parsed = parseVerdict(stdout);
    expect(parsed.type).toBe('memory-projection-block');
    expectCleanupShape(parsed.cleanup);
    expect(parsed.cleanup.removedCount).toBe(1);
    expect(existsSync(stale)).toBe(false);
  });
});

// ===========================================================================
// Green condition 3 — no evidence, or no opt-in, means no key and no sweep.
// ===========================================================================

describe('condition 3 — a GREEN/stable trend, a no-evidence trend, and a beat without the opt-in never sweep', () => {
  test('a GREEN, stable beat with the opt-in present emits no cleanup key and the stale candidate survives', async () => {
    const orchestratorId = 'orch-1691-green-stable';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);
    const stale = seedStaleCandidate(sweepRoot, 'cdk.out-green-stable-stale');

    const secondNowMs = primeTrend({ freeDiskGb: GREEN_STABLE_FREE_GB, baseNowMs: Date.now() });

    const { status, stdout } = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: secondNowMs,
        freeDiskGb: GREEN_STABLE_FREE_GB,
        root: sweepRoot,
        armSweep: true,
      }),
    );

    expect(status).toBe(0);
    const parsed = parseVerdict(stdout);
    expect(parsed.diskTrend.trend).toBe('stable');
    expect('cleanup' in parsed).toBe(false);
    expect(existsSync(stale)).toBe(true);
  });

  test('a first-ever beat (insufficient-data) never sweeps on no evidence', async () => {
    const orchestratorId = 'orch-1691-insufficient-data';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);
    const stale = seedStaleCandidate(sweepRoot, 'cdk.out-insufficient-data-stale');

    const { status, stdout } = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: Date.now(),
        freeDiskGb: AMBER_LOW_FREE_GB,
        root: sweepRoot,
        armSweep: true,
      }),
    );

    expect(status).toBe(0);
    const parsed = parseVerdict(stdout);
    expect(parsed.diskTrend.trend).toBe('insufficient-data');
    // Plan Phase 3 suppression 5: `insufficient-data` is not evidence, and a
    // destructive action taken on no evidence is the thing the trend gate
    // exists to prevent. Note this beat's freeDiskGb is in the AMBER band, so
    // an implementation that fires on "the axis said AMBER" rather than on
    // the evidence-bearing routes fails here.
    expect('cleanup' in parsed).toBe(false);
    expect(existsSync(stale)).toBe(true);
  });

  test('a stale/unknown disk sample (the third AMBER route) never sweeps', async () => {
    const orchestratorId = 'orch-1691-stale-sample-amber';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);
    const stale = seedStaleCandidate(sweepRoot, 'cdk.out-stale-sample-stale');

    const secondNowMs = primeTrend({ freeDiskGb: AMBER_LOW_FREE_GB, baseNowMs: Date.now() });

    // `freeDiskGb: null` is an unreadable sample — classifyDiskAxis reaches
    // AMBER via lib/threshold.mjs:441-446, the stale/unknown route. Sweeping
    // because the sampler could not read the disk is exactly the destructive
    // action-on-no-evidence the plan forbids.
    const { status, stdout } = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({ nowMs: secondNowMs, freeDiskGb: null, root: sweepRoot, armSweep: true }),
    );

    expect(status).toBe(0);
    const parsed = parseVerdict(stdout);
    expect('cleanup' in parsed).toBe(false);
    expect(existsSync(stale)).toBe(true);
  });

  test('a beat that declares dibs — and so unions itself into the ever-opted-in set — is still not armed and never sweeps', async () => {
    // Phase 3 review round 1, HIGH — the direct regression pin. This is
    // an ordinary `--desired-agents` beat, so `declareDibs` DOES union this id
    // into `__ever-opted-in__` before the trigger is evaluated. Under the
    // reviewed-out test-mode branch (which read `hasEverOptedIn`) this beat
    // swept. Under the real `__disk-cleanup-armed__` gate it must not: nothing
    // ever called `recordDiskCleanupArmed` for it.
    const orchestratorId = 'orch-1691-dibs-self-arm';
    const stale = seedStaleCandidate(sweepRoot, 'cdk.out-dibs-self-arm-stale');

    const secondNowMs = primeTrend({
      freeDiskGb: DECLINING_FREE_GB_FIRST,
      baseNowMs: Date.now(),
      orchestratorId,
    });

    // Proves the premise rather than assuming it: the beat above really did
    // record this id in the ever-opted-in set.
    expect(await hasEverOptedIn(coordinationFilePath, orchestratorId)).toBe(true);

    const { status, stdout } = runCli(
      [`--orchestrator-id=${orchestratorId}`, '--desired-agents=1'],
      beatEnv({
        nowMs: secondNowMs,
        freeDiskGb: DECLINING_FREE_GB_SECOND,
        root: sweepRoot,
        armSweep: true,
      }),
    );

    expect(status).toBe(0);
    const parsed = parseVerdict(stdout);
    expect('cleanup' in parsed).toBe(false);
    expect(existsSync(stale)).toBe(true);
  });

  test('a beat whose orchestrator-id is not in the disk-cleanup-armed set never sweeps, even on a declining trend', () => {
    // No `recordDiskCleanupArmed` call at all — and, belt and braces, a
    // `--heartbeat` beat declares no dibs either, so this id is absent from
    // BOTH reserved sets.
    const stale = seedStaleCandidate(sweepRoot, 'cdk.out-no-optin-stale');

    const secondNowMs = primeTrend({
      freeDiskGb: DECLINING_FREE_GB_FIRST,
      baseNowMs: Date.now(),
      orchestratorId: 'orch-1691-prime-other-session',
    });

    const { status, stdout } = runCli(
      ['--orchestrator-id=orch-1691-never-opted-in', '--heartbeat'],
      beatEnv({
        nowMs: secondNowMs,
        freeDiskGb: DECLINING_FREE_GB_SECOND,
        root: sweepRoot,
        armSweep: true,
      }),
    );

    expect(status).toBe(0);
    const parsed = parseVerdict(stdout);
    expect('cleanup' in parsed).toBe(false);
    expect(existsSync(stale)).toBe(true);
  });

  test('an opted-in, declining beat WITHOUT the test-mode arming flag never sweeps (suppression 2, the suite-safety net)', async () => {
    const orchestratorId = 'orch-1691-test-mode-refusal';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);
    const stale = seedStaleCandidate(sweepRoot, 'cdk.out-test-mode-stale');

    const secondNowMs = primeTrend({
      freeDiskGb: DECLINING_FREE_GB_FIRST,
      baseNowMs: Date.now(),
    });

    const { status, stdout } = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: secondNowMs,
        freeDiskGb: DECLINING_FREE_GB_SECOND,
        root: sweepRoot,
        armSweep: false,
      }),
    );

    expect(status).toBe(0);
    const parsed = parseVerdict(stdout);
    // This is the assertion that keeps `npm run test:skills` from deleting
    // 3,000+ real cdk.out dirs if a future spec forgets the per-child TMPDIR
    // idiom. It must never be relaxed.
    expect('cleanup' in parsed).toBe(false);
    expect(existsSync(stale)).toBe(true);
  });
});

// ===========================================================================
// Green condition 4 — --advise-only suppresses UNCONDITIONALLY.
// ===========================================================================

describe('condition 4 — --advise-only suppresses the sweep unconditionally', () => {
  test('an advisory beat with a declining trend AND the opt-in AND the arming flag still sweeps nothing', async () => {
    const orchestratorId = 'orch-1691-advise-only';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);
    const stale = seedStaleCandidate(sweepRoot, 'cdk.out-advise-only-stale');
    const staleBytesBefore = realDirSizeBytes(stale);

    const secondNowMs = primeTrend({
      freeDiskGb: DECLINING_FREE_GB_FIRST,
      baseNowMs: Date.now(),
    });

    const { status, stdout } = runCli(
      [`--orchestrator-id=${orchestratorId}`, '--desired-agents=1', '--advise-only'],
      beatEnv({
        nowMs: secondNowMs,
        freeDiskGb: DECLINING_FREE_GB_SECOND,
        root: sweepRoot,
        armSweep: true,
      }),
    );

    expect(status).toBe(0);
    const parsed = parseVerdict(stdout);
    // `--advise-only` is documented (cli.mjs:6588) as "a fully non-mutating
    // advisory read", and the PreToolUse hook's whole trust argument rests on
    // that. A recursive removal would be the most mutating action in the file.
    expect('cleanup' in parsed).toBe(false);
    expect(existsSync(stale)).toBe(true);
    expect(realDirSizeBytes(stale)).toBe(staleBytesBefore);
  });
});

// ===========================================================================
// Green condition 5 — a failing sweep never changes `type` and never crashes.
// ===========================================================================

describe('condition 5 — a sweep that reports errors leaves the verdict intact', () => {
  test('an undeletable candidate yields cleanup.errors, an unchanged type, exit 0, and a surviving candidate', async () => {
    const orchestratorId = 'orch-1691-removal-failure';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);

    // A stale candidate containing a child entry that cannot be unlinked,
    // because the candidate directory itself is read+execute only. Scoped to
    // the candidate — never to `sweepRoot` — so cli.mjs's own use of its temp
    // root is unaffected and the failure is genuinely the sweep's.
    const stale = seedStaleCandidate(sweepRoot, 'cdk.out-undeletable');
    chmodSync(stale, 0o555);
    modeRestorePaths.push(stale);

    const secondNowMs = primeTrend({
      freeDiskGb: DECLINING_FREE_GB_FIRST,
      baseNowMs: Date.now(),
    });

    const { status, stdout } = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: secondNowMs,
        freeDiskGb: DECLINING_FREE_GB_SECOND,
        root: sweepRoot,
        armSweep: true,
      }),
    );

    // main() never crashed, and stdout is still exactly one parseable JSON
    // document (a stray write here would make the PreToolUse hook fail closed
    // and deny legitimate spawns).
    expect(status).toBe(0);
    const parsed = parseVerdict(stdout);
    expectCleanupShape(parsed.cleanup);
    expect(parsed.cleanup.errors.length).toBeGreaterThan(0);
    expect(parsed.cleanup.removedCount).toBe(0);
    // The verdict type is whatever the axes said — the sweep's failure is
    // reported, never promoted into an admission decision.
    expect(parsed.type).not.toBe('cleanup-failed');
    expect(typeof parsed.type).toBe('string');
    expect(existsSync(stale)).toBe(true);
  });
});

// ===========================================================================
// Green condition 6 — confinement refusal, including a repo-RELATIVE TMPDIR.
// ===========================================================================

describe('condition 6 — a sweep root outside the platform temp allowlist is refused with zero removals', () => {
  /** Creates a scratch root under the REPO, i.e. deliberately outside every platform temp prefix, seeded with one stale candidate. */
  function seedNonTempRootWithCandidate(name) {
    nonTempRoot = mkdtempSync(join(REPO_ROOT, '.arm-cleanup-autotrigger-nontemp-'));
    return seedStaleCandidate(nonTempRoot, name);
  }

  test('an absolute repo-path TMPDIR is refused: cleanup.errors[0].kind is confinement-refused and nothing is removed', async () => {
    const orchestratorId = 'orch-1691-confinement-absolute';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);
    const stale = seedNonTempRootWithCandidate('cdk.out-confinement-absolute');

    const secondNowMs = primeTrend({
      freeDiskGb: DECLINING_FREE_GB_FIRST,
      baseNowMs: Date.now(),
    });

    const { status, stdout } = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: secondNowMs,
        freeDiskGb: DECLINING_FREE_GB_SECOND,
        root: nonTempRoot,
        armSweep: true,
      }),
    );

    expect(status).toBe(0);
    const parsed = parseVerdict(stdout);
    expectCleanupShape(parsed.cleanup);
    expect(parsed.cleanup.errors[0].kind).toBe('confinement-refused');
    expect(parsed.cleanup.removedCount).toBe(0);
    expect(parsed.cleanup.reclaimedBytes).toBe(0);
    expect(existsSync(stale)).toBe(true);
  });

  test('a RELATIVE TMPDIR resolving against the REPO_ROOT-pinned cwd is refused too (rev-2 critical finding)', async () => {
    const orchestratorId = 'orch-1691-confinement-relative';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);
    const stale = seedNonTempRootWithCandidate('cdk.out-confinement-relative');
    // The PreToolUse hook pins cwd to REPO_ROOT (hooks/pretooluse-arm-gate
    // .mjs:297), so a relative TMPDIR resolves inside the repo. Reproduced
    // faithfully here by spawning with `cwd: REPO_ROOT`.
    const relativeRoot = relative(REPO_ROOT, nonTempRoot);
    expect(relativeRoot.startsWith('.')).toBe(true);

    const secondNowMs = primeTrend({
      freeDiskGb: DECLINING_FREE_GB_FIRST,
      baseNowMs: Date.now(),
    });

    const { status, stdout } = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: secondNowMs,
        freeDiskGb: DECLINING_FREE_GB_SECOND,
        root: relativeRoot,
        armSweep: true,
      }),
      { cwd: REPO_ROOT },
    );

    expect(status).toBe(0);
    const parsed = parseVerdict(stdout);
    expectCleanupShape(parsed.cleanup);
    expect(parsed.cleanup.errors[0].kind).toBe('confinement-refused');
    expect(parsed.cleanup.removedCount).toBe(0);
    expect(existsSync(stale)).toBe(true);
  });
});

// ===========================================================================
// Green condition 7 — the sweep's added wall clock is bounded by
// SWEEP_BUDGET_MS. See the SCOPE BOUNDARY note: the mocked-slow/hung-fs half
// of this property is Phase 4's fitness function.
// ===========================================================================

describe('condition 7 — the sweep never adds more than SWEEP_BUDGET_MS to the beat', () => {
  test('an oversized real candidate backlog stops on the sweep\'s own bound, and the beat stays within budget', async () => {
    const orchestratorId = 'orch-1691-budget';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);

    // Deliberately far more work than one budgeted sweep can finish: the
    // plan's SWEEP_MAX_CANDIDATES is a per-beat cap and AMBER backlogs are
    // meant to drain across beats, so a correct implementation reports
    // `candidatesCapped` and/or `budgetExhausted` rather than running long.
    const BACKLOG_SIZE = 80;
    for (let index = 0; index < BACKLOG_SIZE; index += 1) {
      const candidate = seedStaleCandidate(sweepRoot, `cdk.out-backlog-${index}`);
      for (let fileIndex = 0; fileIndex < 20; fileIndex += 1) {
        writeFileSync(join(candidate, `blob-${fileIndex}.bin`), Buffer.alloc(64 * 1024, 7));
      }
      const backdatedSeconds = (Date.now() - (AUTO_TRIGGER_AGE_HOURS + 4) * HOUR_MS) / 1000;
      for (const entry of readdirSync(candidate)) {
        utimesSync(join(candidate, entry), backdatedSeconds, backdatedSeconds);
      }
      utimesSync(candidate, backdatedSeconds, backdatedSeconds);
    }

    const baseNowMs = Date.now();
    const secondNowMs = primeTrend({ freeDiskGb: DECLINING_FREE_GB_FIRST, baseNowMs });

    // Control: the identical beat with the sweep disarmed, so the measurement
    // isolates the sweep's own cost from node startup and the rest of the beat.
    //
    // It is a REAL beat, so it persists its own disk sample — which is why the
    // armed beat below advances to a THIRD point on the same decline rather
    // than reusing this one's instant. Two beats at one fake instant give
    // `elapsedMs === 0`, i.e. `insufficient-data`, i.e. no trend and no sweep:
    // the control would silently disarm the test it exists to calibrate.
    const controlStartedAt = Date.now();
    const control = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: secondNowMs,
        freeDiskGb: DECLINING_FREE_GB_SECOND,
        root: sweepRoot,
        armSweep: false,
      }),
    );
    const controlMs = Date.now() - controlStartedAt;
    expect(control.status).toBe(0);
    expect('cleanup' in parseVerdict(control.stdout)).toBe(false);

    const thirdNowMs = secondNowMs + FIVE_MINUTES_MS;
    const armedStartedAt = Date.now();
    const armed = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: thirdNowMs,
        freeDiskGb: DECLINING_FREE_GB_THIRD,
        root: sweepRoot,
        armSweep: true,
      }),
    );
    const armedMs = Date.now() - armedStartedAt;

    expect(armed.status).toBe(0);
    const parsed = parseVerdict(armed.stdout);
    expectCleanupShape(parsed.cleanup);

    // It stopped on its own bound rather than draining an 80-candidate
    // backlog in one beat.
    expect(parsed.cleanup.budgetExhausted || parsed.cleanup.candidatesCapped).toBe(true);
    expect(parsed.cleanup.removedCount).toBeLessThan(BACKLOG_SIZE);

    // And the observable latency it added is inside the budget (plus the one
    // documented spawn-noise allowance — see SPAWN_NOISE_ALLOWANCE_MS).
    expect(armedMs - controlMs).toBeLessThanOrEqual(SWEEP_BUDGET_MS + SPAWN_NOISE_ALLOWANCE_MS);

    // The remaining backlog is untouched and eligible on the next beat.
    expect(existsSync(join(sweepRoot, `cdk.out-backlog-${BACKLOG_SIZE - 1}`))).toBe(true);
  });
});

// ===========================================================================
// Phase 3 review (Medium) — the futile-sweep COOLDOWN, end to end.
//
// Driven on the AMBER low-free-space route deliberately: it holds `freeDiskGb`
// FLAT, so the `stable` + at-or-below-the-floor evidence route is satisfied
// identically on every beat in the sequence. Any change in whether the sweep
// fires is therefore attributable to the cooldown and nothing else.
// ===========================================================================

describe('cooldown — a sweep that found nothing stands the trigger down for a while', () => {
  test('a clean, empty-handed sweep suppresses the NEXT beat, and the beat after the horizon fires again', async () => {
    const orchestratorId = 'orch-1691-cooldown';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);

    // No candidates at all: the sweep will run to completion and reclaim
    // nothing, which is the one suppressible outcome.
    const fresh = seedFreshCandidate(sweepRoot, 'cdk.out-cooldown-fresh');

    const baseNowMs = Date.now();
    const firstSweepAt = primeTrend({ freeDiskGb: AMBER_LOW_FREE_GB, baseNowMs });

    const first = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({ nowMs: firstSweepAt, freeDiskGb: AMBER_LOW_FREE_GB, root: sweepRoot, armSweep: true }),
    );
    expect(first.status).toBe(0);
    const firstParsed = parseVerdict(first.stdout);

    // The premise: it DID fire, and found nothing. Without this the rest of
    // the test would pass vacuously against a trigger that never ran.
    expectCleanupShape(firstParsed.cleanup);
    expect(firstParsed.cleanup.removedCount).toBe(0);
    expect(firstParsed.cleanup.budgetExhausted).toBe(false);
    expect(firstParsed.cleanup.candidatesCapped).toBe(false);

    // (a) Well inside the cooldown: the trigger stands down even though every
    // real gate still says yes.
    const second = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: firstSweepAt + FIVE_MINUTES_MS,
        freeDiskGb: AMBER_LOW_FREE_GB,
        root: sweepRoot,
        armSweep: true,
      }),
    );
    expect(second.status).toBe(0);
    const secondParsed = parseVerdict(second.stdout);
    expect(secondParsed.diskTrend.trend).toBe('stable');
    expect('cleanup' in secondParsed).toBe(false);

    // (a2) Past the horizon: it fires again on its own, with no operator
    // intervention. A cooldown that never lifted would be a disabled feature.
    //
    // The trend is RE-PRIMED first, by an unarmed beat at the horizon.
    // `computeDiskTrend` reads only the most-recent persisted sample and
    // reports `insufficient-data` once the gap between beats grows large, so
    // jumping the whole 30 minutes in one beat would lose the EVIDENCE gate
    // rather than test the cooldown. The priming beat is not armed, so it
    // cannot itself consume the outcome under test.
    const rePrimedAt = primeTrend({
      freeDiskGb: AMBER_LOW_FREE_GB,
      baseNowMs: firstSweepAt + SWEEP_COOLDOWN_MS,
      orchestratorId: 'orch-1691-cooldown-reprime',
    });

    const third = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: rePrimedAt,
        freeDiskGb: AMBER_LOW_FREE_GB,
        root: sweepRoot,
        armSweep: true,
      }),
    );
    expect(third.status).toBe(0);
    expectCleanupShape(parseVerdict(third.stdout).cleanup);

    // Nothing the sweep should not have touched was touched, on any beat.
    expect(existsSync(fresh)).toBe(true);
  });

  test('a sweep that REMOVED something does not suppress the next beat', async () => {
    const orchestratorId = 'orch-1691-cooldown-productive';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);
    const stale = seedStaleCandidate(sweepRoot, 'cdk.out-cooldown-productive-stale');

    const baseNowMs = Date.now();
    const firstSweepAt = primeTrend({ freeDiskGb: AMBER_LOW_FREE_GB, baseNowMs });

    const first = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({ nowMs: firstSweepAt, freeDiskGb: AMBER_LOW_FREE_GB, root: sweepRoot, armSweep: true }),
    );
    expect(first.status).toBe(0);
    expect(parseVerdict(first.stdout).cleanup.removedCount).toBe(1);
    expect(existsSync(stale)).toBe(false);

    // A productive sweep writes no cooldown: "I just reclaimed something" is
    // evidence there may be more, not evidence the root is clean.
    const second = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: firstSweepAt + FIVE_MINUTES_MS,
        freeDiskGb: AMBER_LOW_FREE_GB,
        root: sweepRoot,
        armSweep: true,
      }),
    );
    expect(second.status).toBe(0);
    const secondParsed = parseVerdict(second.stdout);
    expectCleanupShape(secondParsed.cleanup);
    expect(secondParsed.cleanup.removedCount).toBe(0);

    // ...and THAT one, having found nothing, is what finally stands the
    // trigger down — so the mechanism is armed by outcome, not by beat count.
    const third = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: firstSweepAt + 2 * FIVE_MINUTES_MS,
        freeDiskGb: AMBER_LOW_FREE_GB,
        root: sweepRoot,
        armSweep: true,
      }),
    );
    expect(third.status).toBe(0);
    expect('cleanup' in parseVerdict(third.stdout)).toBe(false);
  });

  test('the cooldown never suppresses an UNARMED or advise-only beat into looking armed', async () => {
    // A guard against the cooldown becoming a fourth gate by accident: it must
    // only ever delay, never explain, a refusal.
    const orchestratorId = 'orch-1691-cooldown-advise';
    await recordDiskCleanupArmed(coordinationFilePath, orchestratorId);
    const stale = seedStaleCandidate(sweepRoot, 'cdk.out-cooldown-advise-stale');

    const firstSweepAt = primeTrend({ freeDiskGb: AMBER_LOW_FREE_GB, baseNowMs: Date.now() });

    const advisory = runCli(
      [`--orchestrator-id=${orchestratorId}`, '--advise-only', '--desired-agents=1'],
      beatEnv({ nowMs: firstSweepAt, freeDiskGb: AMBER_LOW_FREE_GB, root: sweepRoot, armSweep: true }),
    );
    expect(advisory.status).toBe(0);
    expect('cleanup' in parseVerdict(advisory.stdout)).toBe(false);
    expect(existsSync(stale)).toBe(true);

    // An advisory beat wrote no cooldown either, so the next real beat still
    // sweeps.
    const real = runCli(
      [`--orchestrator-id=${orchestratorId}`],
      beatEnv({
        nowMs: firstSweepAt + FIVE_MINUTES_MS,
        freeDiskGb: AMBER_LOW_FREE_GB,
        root: sweepRoot,
        armSweep: true,
      }),
    );
    expect(real.status).toBe(0);
    expect(parseVerdict(real.stdout).cleanup.removedCount).toBe(1);
  });
});
