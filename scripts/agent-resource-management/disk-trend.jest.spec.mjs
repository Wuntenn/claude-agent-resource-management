// Phase 6 — real disk-decline trend, replacing the always-`0`
// `declineRateGbPerHour` documented as a known gap in SKILL.md ("every
// invocation still passes the same single `df` reading as both 'old' and
// 'new,' yielding `declineRateGbPerHour: 0`").
//
// RED by construction: none of the machinery this file exercises exists yet
// — `cli.mjs` never persists a prior disk reading, never computes a genuine
// elapsed-time rate, and never prints a `diskTrend` field. These are
// black-box, real-process-spawning tests — mirroring the established idiom
// in ./cli.jest.spec.mjs and ./query-capacity.jest.spec.mjs — never importing
// cli.mjs directly (it stays excluded from the unit-coverage gate, see
// jest.skills.config.mjs's `!scripts/agent-resource-management/cli.mjs`).
//
// ---------------------------------------------------------------------------
// Design this file tests AGAINST (for the builder to match in cli.mjs) —
// these are test-author decisions per the Build Plan's "your call, document
// it" instructions, not yet-implemented facts:
//
// 1. WHERE `trend` surfaces: a new top-level `diskTrend` field, added to
//    EVERY normal spawn-decision beat's printed JSON (`--desired-agents`,
//    including `--heartbeat`) alongside the existing traffic-light shape
//    (`type`/`allowance`/`pauseCandidate`/`reason`) — i.e. the object
//    `main()` already prints via `JSON.stringify(trafficLight)` gains one
//    more sibling key:
//
//      { ...trafficLight, diskTrend: { trend, declineRateGbPerHour } }
//
//    `trend` is one of `"insufficient-data" | "declining" | "stable" |
//    "recovering"`. `declineRateGbPerHover` [sic — see below] is a plain
//    number, NEVER `NaN`/`null`: when `trend` is `"insufficient-data"`,
//    `declineRateGbPerHour` is exactly `0` — not a fabricated guess, and
//    deliberately NOT `NaN`, because `classifyDiskRaw` (./lib/threshold.mjs)
//    already treats a non-finite `declineRateGbPerHour` as reason to force
//    the WHOLE disk axis to stale/AMBER (`!isFiniteNumber(declineRateGbPerHour)`
//    branch) — feeding NaN through on a merely-trend-unknown-but-otherwise-
//    live-and-healthy reading would incorrectly downgrade a genuinely GREEN
//    disk reading to AMBER, which is exactly the kind of regression the
//    "RED floor / GREEN ceiling must stay unaffected" acceptance criterion
//    below guards against. `0` reproduces the exact pre-Phase-6 rate value
//    for a beat with no trend evidence, preserving old behavior for the
//    steep-decline AMBER check while `diskTrend.trend` separately reports
//    the honest "insufficient-data" state.
//
//    `--query-capacity` and `--claim` are NOT in scope for this phase (per
//    the Build Plan: "this substitution applies only to the steep-decline
//    AMBER check") — this file does not assert anything about their output
//    shape.
//
// 2. Recency window: a NEW, distinct constant — `declineRateGbPerHour`
//    reasoning is deliberately never conflated with
//    `DEFAULT_FRESHNESS_WINDOW_MS` (90s), which governs a totally different
//    thing (whether a whole *reading* can be skip-sampled/reused verbatim by
//    a DIFFERENT orchestrator's beat). A prior disk sample can be too STALE
//    to trust for a rate computation while still being usable, if reused,
//    for the ordinary freshness-window debounce. This file's tests assume a
//    15-minute recency window (mirroring `DEFAULT_LIVENESS_THRESHOLD_MS`'s
//    existing "long enough to span a normal beat cadence — orchestrators
//    call this CLI after each sub-agent's task completes, not on a fixed
//    short timer" rationale) — a prior disk sample older than 15 minutes is
//    stale for trend purposes and yields `"insufficient-data"`, never an
//    extrapolated rate across a meaningless gap. Tests use gaps of a few
//    minutes (well inside the window) and gaps over an hour (well outside
//    it) so the exact boundary value is never load-bearing here.
//
// 3. Persistence: the existing reserved `__shared-machine-sample__`
//    coordination-file entry (`SHARED_MACHINE_SAMPLE_ORCHESTRATOR_ID`,
//    cli.mjs) is extended with one more field, `priorDiskSample:
//    { freeDiskGb, sampledAt }` — the disk reading and timestamp that were
//    in that entry immediately BEFORE this beat's own fresh sample
//    overwrites it. A first-ever beat on a brand-new coordination file finds
//    no existing shared-sample entry at all, so `priorDiskSample` is absent
//    — "insufficient-data", not `0`, not a guess. This file drives that
//    persistence via REAL, sequential `node cli.mjs` child-process
//    invocations against the same coordination file (never importing
//    ./lib/coordination-file.mjs to pre-seed the entry directly), using
//    `ARM_FAKE_NOW_MS` to simulate real elapsed wall-clock time across those
//    otherwise-fresh, otherwise-short-lived one-shot processes, and
//    `ARM_FAKE_COLLECT_JSON` to supply each beat's own `freeDiskGb` reading
//    directly (this seam bypasses `parseFreeDisk` entirely, so the
//    trend/rate computation this phase adds must live in `cli.mjs`'s own
//    beat-composition logic, not inside `parseFreeDisk`/`collect()`).
//
//    Per `hasExplicitFakeCollectInput`'s existing, pre-Phase-6 contract, a
//    beat that carries its own well-formed `ARM_FAKE_COLLECT_JSON` never
//    reuses a still-fresh shared sample — it always genuinely re-samples (and
//    therefore always genuinely refreshes the shared-sample entry, including
//    its new `priorDiskSample`). This is what makes two sequential
//    `ARM_FAKE_COLLECT_JSON`-driven invocations a faithful way to test real,
//    distinct, timestamped samples without relying on the freshness-window
//    reuse path at all.
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

// Mirrors cli.jest.spec.mjs's MEMORY_GREEN fixture exactly — this file's
// scenarios are all about the disk axis, so memory is held fixed at a
// comfortably GREEN reading throughout.
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 };

const FIVE_MINUTES_MS = 5 * 60 * 1000;
const OVER_ONE_HOUR_MS = 65 * 60 * 1000;

/** Runs the real cli.mjs as a child process — never imports it directly. */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

function beat(orchestratorId, coordinationFilePath, { freeDiskGb, nowMs, extraDiskFields = {} }) {
  return runCli(
    ['--orchestrator-id=' + orchestratorId],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({
        memory: MEMORY_GREEN,
        // `declineRateGbPerHour` here is deliberately a decoy/arbitrary
        // value distinct from what the real computed rate should be — a
        // correct Phase 6 implementation must derive its own rate from two
        // persisted timestamped `freeDiskGb` samples, NEVER pass this
        // fixture-supplied literal straight through to `diskTrend` or to the
        // steep-decline AMBER check.
        disk: { freeDiskGb, declineRateGbPerHour: 999, ...extraDiskFields },
      }),
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_NOW_MS: String(nowMs),
    },
  );
}

let workDir;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-disk-trend-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

const BASE_NOW_MS = 1_700_000_000_000;

describe('single sample (first-ever beat on a fresh coordination file)', () => {
  test('reports diskTrend.trend "insufficient-data", never a fabricated rate', () => {
    const { status, stdout, stderr } = beat('orch-disk-trend-first-beat', coordinationFilePath, {
      freeDiskGb: 60,
      nowMs: BASE_NOW_MS,
    });

    expect(status).toBe(0);
    expect(stderr).toBe('');

    const parsed = JSON.parse(stdout);
    expect(parsed.diskTrend).toBeDefined();
    expect(parsed.diskTrend.trend).toBe('insufficient-data');
    // Never NaN/null and never the fixture's decoy 999 — an honest "no
    // evidence" zero, not a guessed or pass-through value.
    expect(parsed.diskTrend.declineRateGbPerHour).toBe(0);
  });
});

describe('a second sample exists but the first is older than the recency window', () => {
  test('still reports "insufficient-data", not an extrapolation across a meaningless time gap', () => {
    const orchestratorId = 'orch-disk-trend-stale-prior';

    const first = beat(orchestratorId, coordinationFilePath, { freeDiskGb: 100, nowMs: BASE_NOW_MS });
    expect(first.status).toBe(0);

    const second = beat(orchestratorId, coordinationFilePath, {
      freeDiskGb: 40, // a huge drop — if this were wrongly treated as fresh,
      // it would produce an enormous, clearly-fabricated-looking rate.
      nowMs: BASE_NOW_MS + OVER_ONE_HOUR_MS,
    });

    expect(second.status).toBe(0);
    const parsed = JSON.parse(second.stdout);
    expect(parsed.diskTrend.trend).toBe('insufficient-data');
    expect(parsed.diskTrend.declineRateGbPerHour).toBe(0);
  });
});

describe('two genuinely fresh, timestamped samples with real elapsed time', () => {
  test('computes the correct declineRateGbPerHour from GB delta / hours elapsed', () => {
    const orchestratorId = 'orch-disk-trend-real-rate';

    const first = beat(orchestratorId, coordinationFilePath, { freeDiskGb: 100, nowMs: BASE_NOW_MS });
    expect(first.status).toBe(0);
    // The very first beat has nothing to compare against yet.
    expect(JSON.parse(first.stdout).diskTrend.trend).toBe('insufficient-data');

    // 5 minutes later, 2.5 GB less free: 2.5 GB / (5/60 h) = 30 GB/h.
    const second = beat(orchestratorId, coordinationFilePath, {
      freeDiskGb: 97.5,
      nowMs: BASE_NOW_MS + FIVE_MINUTES_MS,
    });

    expect(second.status).toBe(0);
    const parsed = JSON.parse(second.stdout);
    expect(parsed.diskTrend.trend).toBe('declining');
    expect(parsed.diskTrend.declineRateGbPerHour).toBeCloseTo(30, 5);
  });
});

describe('disk increasing between samples (e.g. cleanup ran)', () => {
  test('reports a correctly-signed "recovering" result, not a nonsensical negative-hours artifact', () => {
    const orchestratorId = 'orch-disk-trend-recovering';

    const first = beat(orchestratorId, coordinationFilePath, { freeDiskGb: 50, nowMs: BASE_NOW_MS });
    expect(first.status).toBe(0);

    // 5 minutes later, 6 GB MORE free (cleanup ran): the disk is recovering,
    // not declining — this must never be reported as a positive "GB/hour
    // decline" figure that would make hoursToFull look catastrophic.
    const second = beat(orchestratorId, coordinationFilePath, {
      freeDiskGb: 56,
      nowMs: BASE_NOW_MS + FIVE_MINUTES_MS,
    });

    expect(second.status).toBe(0);
    const parsed = JSON.parse(second.stdout);
    expect(parsed.diskTrend.trend).toBe('recovering');
    // Signed rate: negative (or non-positive) — never a positive "declining"
    // magnitude for a disk that is genuinely gaining free space.
    expect(parsed.diskTrend.declineRateGbPerHour).toBeLessThanOrEqual(0);

    // The traffic light itself must reflect a healthy machine, not a
    // "hoursToFull" panic derived from a negative-rate artifact — both axes
    // are comfortably GREEN with growing free disk, so this beat must never
    // read AMBER/hold/alert purely from the recovering trend.
    expect(parsed.type).toBe('spawn-allowed');
  });
});

describe('noise-tolerance dead zone around zero (Phase 6 review, Medium #3; epsilon corrected to a raw GB delta in the follow-up High-severity review — see cli.mjs\'s DISK_TREND_NOISE_EPSILON_GB doc comment)', () => {
  test('a below-epsilon RAW GB DELTA (under 1GB) reports "stable", not "declining" — regardless of elapsed time between samples', () => {
    const orchestratorId = 'orch-disk-trend-noise-dead-zone';

    const first = beat(orchestratorId, coordinationFilePath, { freeDiskGb: 100, nowMs: BASE_NOW_MS });
    expect(first.status).toBe(0);

    // Only 1 minute later (a realistic, short beat-to-beat gap — NOT the
    // 15-minute recency window), 0.3 GB less free. The dead zone is a raw GB
    // delta, not a rate: 0.3GB < 1GB (DISK_TREND_NOISE_EPSILON_GB) is noise
    // regardless of how little time elapsed, so this must never flip to
    // "declining" purely from routine ±1GB df quantization rounding — even
    // though 0.3GB over 1 minute would be an alarming-looking 18 GB/h if a
    // rate-based epsilon were (incorrectly) used instead. Contrast with the
    // "two genuinely fresh, timestamped samples" describe block above, whose
    // 2.5GB-over-5-minutes case sits comfortably ABOVE the 1GB delta
    // threshold and correctly stays "declining".
    const second = beat(orchestratorId, coordinationFilePath, {
      freeDiskGb: 99.7,
      nowMs: BASE_NOW_MS + 60_000,
    });

    expect(second.status).toBe(0);
    const parsed = JSON.parse(second.stdout);
    expect(parsed.diskTrend.trend).toBe('stable');
    expect(parsed.diskTrend.declineRateGbPerHour).toBe(0);
  });
});

describe('the real trend genuinely drives the steep-decline AMBER check', () => {
  test('a fast real decline (computed rate, not the fixture decoy) trips AMBER/hold via hoursToFull', () => {
    const orchestratorId = 'orch-disk-trend-steep-decline';

    // freeDiskGb starts comfortably above both the red floor (10) and the
    // green ceiling (30) thresholds (see DEFAULT_DISK_THRESHOLDS in cli.mjs).
    const first = beat(orchestratorId, coordinationFilePath, { freeDiskGb: 13, nowMs: BASE_NOW_MS });
    expect(first.status).toBe(0);
    expect(JSON.parse(first.stdout).type).toBe('hold'); // freeDiskGb=13 is between red(10) and green(30) -> AMBER already, sanity check the fixture is otherwise unremarkable.

    // 1 minute later, 2 GB less free: 2 GB / (1/60 h) = 120 GB/h. A 2GB delta
    // is deliberately used here, not 1GB — since DISK_TREND_NOISE_EPSILON_GB's
    // dead zone now compares with `<=` (Phase 6 final review, Medium — epsilon
    // comparator can't dead-zone real integer-GB noise), a 1GB delta is itself
    // dead-zoned as noise and reports "stable" (see the "noise-tolerance dead
    // zone" describe block above, which now covers exactly that 1GB-at-epsilon
    // case). This test exists to prove a delta that genuinely EXCEEDS the
    // epsilon still drives a real "declining" trend and the steep-decline
    // AMBER check: at freeDiskGb=11, hoursToFull = 11/120 ≈ 0.09h, well below
    // the 2h amberHoursToFullBelow threshold. Before Phase 6, the rate was
    // always 0 in production, so hoursToFull was always Infinity and this
    // branch could never trip from real elapsed samples.
    const second = beat(orchestratorId, coordinationFilePath, {
      freeDiskGb: 11,
      nowMs: BASE_NOW_MS + 60_000,
    });

    expect(second.status).toBe(0);
    const parsed = JSON.parse(second.stdout);
    expect(parsed.diskTrend.trend).toBe('declining');
    expect(parsed.diskTrend.declineRateGbPerHour).toBeCloseTo(120, 5);
    expect(parsed.type).toBe('hold');
  });
});

describe('regression: RED floor and GREEN ceiling disk classification are unaffected by this phase', () => {
  test('a single-beat (insufficient-data) RED freeDiskGb still produces "alert", exactly as before Phase 6', () => {
    const { status, stdout } = beat('orch-disk-trend-red-floor', coordinationFilePath, {
      freeDiskGb: 5, // below DEFAULT_DISK_THRESHOLDS.redFreeDiskGbBelow (10)
      nowMs: BASE_NOW_MS,
    });

    expect(status).toBe(0);
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('alert');
    expect(parsed.diskTrend.trend).toBe('insufficient-data');
  });

  test('a single-beat (insufficient-data) GREEN freeDiskGb still produces "spawn-allowed", exactly as before Phase 6', () => {
    const { status, stdout } = beat('orch-disk-trend-green-ceiling', coordinationFilePath, {
      freeDiskGb: 60, // above DEFAULT_DISK_THRESHOLDS.greenFreeDiskGbAbove (30)
      nowMs: BASE_NOW_MS,
    });

    expect(status).toBe(0);
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('spawn-allowed');
    expect(parsed.diskTrend.trend).toBe('insufficient-data');
  });

  test('two fresh samples that stay comfortably above the green ceiling still produce "spawn-allowed" regardless of a mild real decline', () => {
    const orchestratorId = 'orch-disk-trend-green-ceiling-with-trend';

    const first = beat(orchestratorId, coordinationFilePath, { freeDiskGb: 100, nowMs: BASE_NOW_MS });
    expect(first.status).toBe(0);

    // A slow, mild decline that never gets remotely close to either the RED
    // floor or a steep-decline AMBER hoursToFull trip. A 2GB delta is used
    // deliberately, not 1GB — since DISK_TREND_NOISE_EPSILON_GB's dead zone
    // compares with `<=`, a 1GB delta is itself dead-zoned as noise and would
    // report "stable" here, which is not what this test is checking.
    const second = beat(orchestratorId, coordinationFilePath, {
      freeDiskGb: 98,
      nowMs: BASE_NOW_MS + FIVE_MINUTES_MS,
    });

    expect(second.status).toBe(0);
    const parsed = JSON.parse(second.stdout);
    expect(parsed.diskTrend.trend).toBe('declining');
    expect(parsed.type).toBe('spawn-allowed');
  });
});
