// Phase 4 — Unmaskable pressure-WARN admission block + CRITICAL shed
// signal.
//
// RED BY CONSTRUCTION: no production code implementing this pre-check exists
// yet. `main()`'s only place `pressureLevel` currently reaches is INSIDE
// `classifyMemoryRaw` (./lib/threshold.mjs), where it is one of several
// inputs blended into `memoryState` (GREEN/AMBER/RED) — which then feeds
// `buildTrafficLight`'s existing precedence chain (./lib/allowance.mjs),
// where a simultaneous disk-RED reading short-circuits to `alert` BEFORE the
// memory-RED/pause branch is ever reached. That is the exact masking risk
// this phase closes (Investigation R2 / its residual risk): today,
// `pressureLevel` has no INDEPENDENT voice in the admission decision — it is
// only ever heard through `memoryState`, and `memoryState` can be silently
// out-voted by disk-RED. Every test below fails against the current
// `cli.mjs` because none of the assertions it makes (the new `type` literal,
// the new `pressureLevel`/`shedSignal` fields, the both-RED tie going to
// that new literal instead of `alert`) are produced by any code that exists
// today.
//
// ---------------------------------------------------------------------------
// OUTPUT-SHAPE DECISION (documented per the task's "decide, and write a
// comment explaining the decision" instruction — the implementer must match
// this exactly, or explain in their handoff why they diverged):
//
//   A tripped pressure pre-check (pressureLevel >= 2) reports a NEW,
//   dedicated top-level literal:
//
//     { type: 'pressure-block', pressureLevel: <n>, shedSignal: <boolean>,
//       diskTrend: {...}, liveAgentGrant: null }
//
//   Why NOT reuse `hold`'s exact shape: `hold` already means "AMBER on
//   either axis" (buildTrafficLight, memoryState/diskState AMBER branch).
//   Today, pressureLevel=2 already happens to co-occur with an AMBER
//   `memoryState` under the existing DEFAULT_MEMORY_THRESHOLDS
//   (`greenPressureAtOrBelow: 1` — see lib/threshold.mjs's
//   `classifyMemoryRaw`), so a lazy implementation could satisfy "admission
//   blocked" by doing nothing more than what already happens today. That
//   would NOT satisfy this phase's actual requirement: an INDEPENDENT,
//   unmaskable pre-check that runs before, and is not derived from,
//   `buildTrafficLight`'s precedence chain — the whole point being that it
//   must still fire even when disk-RED would otherwise steal the verdict to
//   `alert` (which `hold` never does — disk-RED beats memory-AMBER's `hold`
//   too). Reusing `hold` verbatim would make a WARN-pressure block
//   indistinguishable, from an orchestrator's point of view, from an
//   ordinary swap-based AMBER hold — which is exactly the ambiguity the
//   issue's "unmaskable" requirement exists to remove. A new literal makes
//   the pre-check's verdict observably distinct and lets a test assert on
//   its presence/absence directly, rather than inferring it from `hold`
//   showing up in a case where `alert` "should" otherwise have won.
//
//   `shedSignal` is a sibling boolean (not a fifth top-level literal),
//   mirroring the existing `diskTrend`/`liveAgentGrant` convention: a fact
//   `cli.mjs` attaches to the printed JSON alongside whatever
//   `buildTrafficLight`-shaped verdict it computed, not a value the verdict
//   itself needs another literal to express. `shedSignal` is `true` only
//   when `pressureLevel >= 4` (CRITICAL) and `false` for a WARN-only
//   (`pressureLevel` 2 or 3) block. This is EMISSION ONLY — per the Build
//   Plan/Investigation R1 (: no executor for `pause`/`pauseCandidate`
//   exists), `shedSignal: true` must never correlate with any observable
//   process-control side effect (no `kill`/`SIGSTOP`/`SIGCONT`/`exec`
//   anywhere in cli.mjs, mirroring doc-honesty.jest.spec.mjs's existing
//   source-scan assertion against lib/allowance.mjs).
//
//   `liveAgentGrant` stays `null` on a pressure-block beat: per cli.mjs's
//   existing contract (see LIVE_AGENT_CLAIM_TYPE/LIVE_AGENT_ADMISSION_
//   CLAIM_TYPE doc comments), the atomic live-agent claim is only ever
//   attempted on a beat that would otherwise reach `spawn-allowed` — a
//   pressure-blocked beat must not attempt it either, exactly like today's
//   AMBER/RED beats.
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const __dirname = dirname(fileURLToPath(import.meta.url));
const CLI = join(__dirname, 'cli.mjs');
const cliSource = readFileSync(CLI, 'utf8');

/**
 * Extracts just the NEW pressure pre-check code block this phase adds to
 * `cli.mjs` — from its opening doc-comment marker to the comment immediately
 * following the `if` branch that closes it (see `endMarker` below; the
 * pre-check was relocated post-Phase-6-review to run after dibs declaration,
 * so the marker bounding its end moved with it — the extracted region still
 * covers exactly the `if (... >= warnAtOrAbove) { ... return; }` block).
 * Deliberately narrower than doc-honesty.jest.spec.mjs's
 * equivalent whole-file scan against lib/allowance.mjs: that file is pure
 * logic with no operational prose, so a whole-file scan for `kill` never
 * false-positives there. cli.mjs is a much larger file that legitimately
 * discusses process-control concepts in COMMENT PROSE elsewhere (e.g. an
 * unrelated existing comment about a probe's own timeout "kill" behavior),
 * so scanning the entire file for the bare word `kill` would false-positive
 * on that pre-existing, unrelated prose — this scopes the assertion to only
 * the code this phase actually introduces, which is the thing under test.
 */
function extractPressureBlockCode(source) {
  const startMarker = ' Phase 4 — the unmaskable pressure-WARN admission pre-check.';
  const startIndex = source.indexOf(startMarker);
  if (startIndex === -1) {
    throw new Error(
      'extractPressureBlockCode: could not find the Phase 4 pressure pre-check marker comment in cli.mjs ' +
        '— has it been renamed? Update startMarker to match.',
    );
  }
  // Post-Phase-6-review fix moved the pressure pre-check to run AFTER axis
  // classification/dibs declaration (so hysteresis history/dibs liveness
  // stay current across a sustained pressure episode) but still strictly
  // BEFORE the spawn-rate bucket/live-agent ceiling/buildTrafficLight — see
  // cli.mjs's own "why AFTER dibs, not before" comment on the pre-check for
  // the full rationale. The comment immediately following the pre-check
  // block moved along with it.
  const endMarker = '  // Per-orchestrator dibs-constrained allowance: reserved';
  const endIndex = source.indexOf(endMarker, startIndex);
  if (endIndex === -1) {
    throw new Error(
      'extractPressureBlockCode: could not find the end-of-block marker after the pressure pre-check — ' +
        'update endMarker to match the current cli.mjs structure.',
    );
  }
  return source.slice(startIndex, endIndex);
}

/** Runs the real cli.mjs as a child process — never imports it directly (it
 * runs main() unconditionally at module scope on import). */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const DISK_RED = { freeDiskGb: 5, declineRateGbPerHour: 0.5 };

// Comfortably-healthy swap/compressed numbers alongside each pressureLevel —
// "headroom/cost accounting stubbed to return unlimited capacity" per
// scenario 1/5: a huge freeRamMb is exactly the fixture
// ./live-agent-ceiling.jest.spec.mjs's own MEMORY_WRONG_PERMISSIVE uses to
// prove a gate is independent of a deliberately-over-generous headroom
// reading.
const MEMORY_CONTROL_GREEN = {
  pressureLevel: 1,
  swapUsedMb: 100,
  compressedMb: 100,
  freeRamMb: 40_000,
};
const MEMORY_WARN = {
  pressureLevel: 2,
  swapUsedMb: 100,
  compressedMb: 100,
  freeRamMb: 40_000,
};
const MEMORY_CRITICAL = {
  pressureLevel: 4,
  swapUsedMb: 100,
  compressedMb: 100,
  freeRamMb: 40_000,
};

let workDir;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-pressure-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Scenario 1 / Build Plan Test Strategy row 1 — "WARN (pressureLevel=2)
// blocks admission with headroom/cost accounting stubbed to return
// unlimited capacity".
// ---------------------------------------------------------------------------

test('WARN (pressureLevel=2) blocks admission even with headroom/cost accounting stubbed maximally permissive', () => {
  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-a', '--desired-agents=3'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_WARN, disk: DISK_GREEN }),
    },
  );

  expect(status).toBe(0);
  const parsed = JSON.parse(stdout);

  // Must NOT be spawn-allowed (or spawn-allowed with any allowance — a
  // grant of literal 0 under the 'spawn-allowed' literal would still be
  // dishonest: it implies "the axes/bucket/ceiling machinery decided to
  // grant nothing", not "an independent hard gate refused to consider this
  // beat for admission at all"). See this file's header comment for the
  // exact literal decision.
  expect(parsed.type).not.toBe('spawn-allowed');
  expect(parsed.type).toBe('pressure-block');
  expect(parsed.pressureLevel).toBe(2);
  // WARN alone (not CRITICAL) never emits the shed signal.
  expect(parsed.shedSignal).toBe(false);
  // The atomic live-agent claim must never be attempted on a blocked beat.
  expect(parsed.liveAgentGrant).toBeNull();
  expect(stderr).toBe('');
});

// ---------------------------------------------------------------------------
// Scenario 5 — the same WARN block, more concretely wired to this
// directory's existing "wrong/permissive headroom stub" idiom
// (MEMORY_WRONG_PERMISSIVE in ./live-agent-ceiling.jest.spec.mjs): a huge
// freeRamMb reading that the (currently-retired) flat headroom formula would
// happily admit against, proving the pressure gate does not depend on that
// accounting being correct, or even consulted, to do its job.
// ---------------------------------------------------------------------------

test('WARN (pressureLevel=2) blocks admission against a deliberately wrong/permissive freeRamMb reading', () => {
  const memoryWarnWrongPermissive = {
    pressureLevel: 2,
    swapUsedMb: 100,
    compressedMb: 100,
    freeRamMb: 100_000, // absurdly large — mirrors MEMORY_WRONG_PERMISSIVE's own intent
  };

  const { status, stdout } = runCli(
    ['--orchestrator-id=orch-a', '--desired-agents=1'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: memoryWarnWrongPermissive, disk: DISK_GREEN }),
    },
  );

  expect(status).toBe(0);
  const parsed = JSON.parse(stdout);
  expect(parsed.type).toBe('pressure-block');
  expect(parsed.pressureLevel).toBe(2);
  expect(parsed.liveAgentGrant).toBeNull();
});

// ---------------------------------------------------------------------------
// Scenario 2 / Build Plan edge case — "Both memory-RED and disk-RED
// simultaneously — the new pre-check must still block admission (this is
// the named regression test for its residual risk)".
//
// Uses pressureLevel=4 (CRITICAL) alongside a genuinely RED disk reading.
// Today, `buildTrafficLight` checks `diskState === 'RED'` FIRST and would
// return `{ type: 'alert', reason }`, discarding the memory signal entirely
// — proving the OLD code's masking bug, not this phase's fix. The pressure
// pre-check must win this tie by running BEFORE buildTrafficLight is even
// reached, so the verdict must be 'pressure-block', never 'alert'.
// ---------------------------------------------------------------------------

test('memory-RED (pressureLevel=4) and disk-RED simultaneously — pressure pre-check still blocks admission, not masked by disk-RED alert (regression)', () => {
  const { status, stdout } = runCli(
    ['--orchestrator-id=orch-a', '--desired-agents=1'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_CRITICAL, disk: DISK_RED }),
    },
  );

  expect(status).toBe(0);
  const parsed = JSON.parse(stdout);

  // The named regression: this must NOT be 'alert' (disk-RED's own signal
  // winning the tie the way it does inside buildTrafficLight today).
  expect(parsed.type).not.toBe('alert');
  expect(parsed.type).toBe('pressure-block');
  expect(parsed.pressureLevel).toBe(4);
  expect(parsed.liveAgentGrant).toBeNull();
});

// ---------------------------------------------------------------------------
// Scenario 3 — "CRITICAL (pressureLevel=4) emits a shed signal but triggers
// no process signal/checkpoint/exit side effect".
//
// Per Investigation R1 / : there is no executor for pause/pauseCandidate
// today, so this is "emits a shed signal" ONLY. Asserted two ways:
//   (a) the observable JSON output DOES carry a shed signal (shedSignal:
//       true) distinct from the WARN-only case (shedSignal: false above);
//   (b) cli.mjs's own source contains none of the process-control primitives
//       that would turn "emit a signal" into "actually reclaim memory" —
//       mirroring doc-honesty.jest.spec.mjs's existing regex assertion
//       against lib/allowance.mjs, applied here to the NEW pressure-check
//       code path this phase adds to cli.mjs.
// ---------------------------------------------------------------------------

test('CRITICAL (pressureLevel=4) emits a shed signal with no process signal/checkpoint/exit side effect', () => {
  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-a', '--desired-agents=1'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_CRITICAL, disk: DISK_GREEN }),
    },
  );

  expect(status).toBe(0);
  const parsed = JSON.parse(stdout);
  expect(parsed.type).toBe('pressure-block');
  expect(parsed.pressureLevel).toBe(4);
  expect(parsed.shedSignal).toBe(true);
  expect(parsed.liveAgentGrant).toBeNull();

  // No side channel: nothing killed/signalled/execed, and exit 0 with clean
  // stderr — a "signal" that is only ever a JSON field, never an action.
  expect(status).toBe(0);
  expect(stderr).toBe('');

  // Source-scan companion to doc-honesty.jest.spec.mjs's existing
  // lib/allowance.mjs assertion (see that file's
  // "confirms the underlying fact being documented" test) — applied to just
  // the NEW pressure pre-check block this phase adds to cli.mjs (see
  // extractPressureBlockCode's own comment for why this is scoped rather
  // than a whole-file scan: cli.mjs, unlike lib/allowance.mjs, has
  // pre-existing, unrelated prose that legitimately mentions "kill").
  expect(extractPressureBlockCode(cliSource)).not.toMatch(/\b(kill|SIGSTOP|SIGCONT|exec)\b/);
});

test('WARN (pressureLevel=2, below CRITICAL) never sets the shed signal', () => {
  const { stdout } = runCli(
    ['--orchestrator-id=orch-a', '--desired-agents=1'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_WARN, disk: DISK_GREEN }),
    },
  );

  const parsed = JSON.parse(stdout);
  expect(parsed.type).toBe('pressure-block');
  expect(parsed.shedSignal).toBe(false);
});

// ---------------------------------------------------------------------------
// Scenario 4 / THE key discriminating test — "pressure pre-check runs before
// buildTrafficLight's precedence chain, not folded into it".
//
// Framing note (documented per this task's instruction to record judgment
// calls): the Build Plan asks for "memoryState and diskState BOTH stubbed
// GREEN" alongside pressureLevel=2. Under this codebase's REAL
// classifyMemoryRaw (./lib/threshold.mjs), `pressureLevel` is one of the
// inputs `memoryState` is derived FROM (DEFAULT_MEMORY_THRESHOLDS.
// greenPressureAtOrBelow = 1) — so a sample with pressureLevel=2 can never
// itself classify as memoryState GREEN through the real pipeline; the two
// are not independently stubbable via ARM_FAKE_COLLECT_JSON (which injects
// the raw sample, not the derived axis state). Rather than assert an
// impossible-under-real-thresholds intermediate state, this test proves the
// SAME discriminating fact a different, black-box-honest way: two beats with
// IDENTICAL freeRamMb/swapUsedMb/compressedMb/disk readings (values that, on
// their own, given only pressureLevel=1, already classify GREEN/GREEN and
// reach spawn-allowed — see the control case below) differ ONLY in
// pressureLevel. If the admission verdict flips from spawn-allowed to
// pressure-block purely because pressureLevel moved from 1 to 2, with every
// other input held fixed, the block cannot be coming from anything
// `buildTrafficLight`'s existing memoryState/diskState-driven precedence
// chain alone would have produced from those OTHER inputs — it must be the
// new, independent pre-check. This is the exact "not derived from, not
// gated behind, the existing GREEN/AMBER/RED/pause/alert branching" proof
// the Build Plan asks for, adapted to what is actually constructible
// through this CLI's real input surface.
// ---------------------------------------------------------------------------

test('pressure pre-check runs before buildTrafficLight precedence chain: flipping pressureLevel alone (all else identical, otherwise-GREEN) flips the verdict', () => {
  const controlRun = runCli(
    ['--orchestrator-id=orch-a', '--desired-agents=1'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_CONTROL_GREEN, disk: DISK_GREEN }),
    },
  );
  expect(controlRun.status).toBe(0);
  const controlParsed = JSON.parse(controlRun.stdout);
  // Control: with pressureLevel=1 and everything else identical to the WARN
  // case below, this beat reaches spawn-allowed today — proving
  // memoryState/diskState alone (via every OTHER field) would not have
  // produced anything but spawn-allowed on their own.
  expect(controlParsed.type).toBe('spawn-allowed');

  const warnRun = runCli(
    ['--orchestrator-id=orch-b', '--desired-agents=1'],
    {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_WARN, disk: DISK_GREEN }),
    },
  );
  expect(warnRun.status).toBe(0);
  const warnParsed = JSON.parse(warnRun.stdout);

  // The ONLY input that changed between the two runs is pressureLevel
  // (1 -> 2); freeRamMb/swapUsedMb/compressedMb/disk are byte-for-byte
  // identical. The verdict must still flip to the new, independent literal
  // — proving the check is a pre-check, not folded inside the existing
  // memoryState/diskState precedence chain (which had nothing else to react
  // to here).
  expect(warnParsed.type).toBe('pressure-block');
  expect(warnParsed.type).not.toBe(controlParsed.type);
});

// ---------------------------------------------------------------------------
// Post-Phase-6-review regression: a pressure-blocked beat still persists
// dibs/hysteresis history (memoryHistory/diskHistory + refreshed liveness).
// The original Phase 4 draft returned BEFORE declareDibs ever ran, which
// would freeze this orchestrator's hysteresis history and let its dibs entry
// age out for the ENTIRE duration of a sustained pressure episode — exactly
// the scenario this ticket targets. This is admission-decision-NEUTRAL
// state (declareDibs never grants anything), so persisting it does not
// reopen the "no wasted admission-granting work on a blocked beat" property
// the spawn-bucket/live-agent-ceiling gates still correctly preserve (see
// the next test).
// ---------------------------------------------------------------------------

test('a pressure-blocked beat still persists dibs/hysteresis history (memoryHistory present, liveness refreshed) [regression]', () => {
  const env = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_WARN, disk: DISK_GREEN }),
  };

  const blocked = runCli(['--orchestrator-id=orch-pressure-history', '--desired-agents=2'], env);
  expect(blocked.status).toBe(0);
  expect(JSON.parse(blocked.stdout).type).toBe('pressure-block');

  const onDisk = JSON.parse(readFileSync(coordinationFilePath, 'utf8'));
  const selfEntry = onDisk.find((entry) => entry.orchestratorId === 'orch-pressure-history');
  expect(selfEntry).toBeDefined();
  expect(selfEntry.memoryHistory).toBeDefined();
  expect(selfEntry.diskHistory).toBeDefined();
  expect(typeof selfEntry.declaredAt).toBe('number');
  expect(typeof selfEntry.firstDeclaredAt).toBe('number');
});

// ---------------------------------------------------------------------------
// Companion regression: a pressure-blocked beat still never draws the
// global spawn-rate bucket or attempts the live-agent ceiling's atomic
// claim — moving the pre-check to run AFTER dibs declaration (the fix
// above) must not accidentally also move it to run after these two
// genuinely admission-GRANTING gates.
// ---------------------------------------------------------------------------

test('a pressure-blocked beat still never draws the spawn-rate bucket or attempts the live-agent ceiling claim [regression]', () => {
  const GLOBAL_SPAWN_BUCKET_RESERVED_ID = '__global-spawn-rate-bucket__';
  // `claimCapacity`/`releaseCapacity`'s reserved per-type ledger entry
  // convention (./lib/coordination-file.mjs: `__claim:<type>__`) — asserting
  // its absence pins the OTHER half of "never attempts an admission-granting
  // claim" that this test's title promises: the bucket check alone would not
  // have caught a future reordering that moved the pre-check past the
  // live-agent `claimCapacity` calls but not past the bucket.
  const LIVE_AGENT_ADMISSION_RESERVED_ID = '__claim:live-agent-admission__';
  const env = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_WARN, disk: DISK_GREEN }),
    ARM_SPAWN_BUCKET_CONFIG_JSON: JSON.stringify({ capacityTokens: 3, refillTokensPerMs: 0 }),
  };

  const blocked = runCli(['--orchestrator-id=orch-pressure-no-bucket-draw', '--desired-agents=2'], env);
  expect(blocked.status).toBe(0);
  expect(JSON.parse(blocked.stdout).type).toBe('pressure-block');

  const onDisk = JSON.parse(readFileSync(coordinationFilePath, 'utf8'));
  expect(onDisk.find((entry) => entry.orchestratorId === GLOBAL_SPAWN_BUCKET_RESERVED_ID)).toBeUndefined();
  expect(onDisk.find((entry) => entry.orchestratorId === LIVE_AGENT_ADMISSION_RESERVED_ID)).toBeUndefined();
});
