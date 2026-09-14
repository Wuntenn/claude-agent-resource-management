// Phase 2 — `--record-outcome` CLI wiring.
//
// RED by construction: `--record-outcome` does not exist yet in cli.mjs
// (only Phase 1's `lib/history.mjs` has landed, commit 3e20b827). These are
// black-box, real-process-spawning tests — mirroring the established idiom
// in ./cli.jest.spec.mjs — never importing cli.mjs directly.
//
// ---------------------------------------------------------------------------
// CLI contract this file tests AGAINST (for the builder to match in cli.mjs):
//
//   node cli.mjs --record-outcome --operation-type=<type> --orchestrator-id=<id>
//                [--peak-memory-mb=<n>] [--crashed]
//
//   ARM_HISTORY_FILE   (env, required for test hermeticity)
//     Absolute path to the history store file `lib/history.mjs`'s
//     `recordObservation`/`readHistory` read/write. Mirrors
//     `ARM_COORDINATION_FILE`'s existing precedent exactly (same naming
//     convention, same "env override falls back to a real
//     `.claude/agent-state/` production default when unset" shape) — tests
//     always set it so no run ever touches the real production history file.
//
//   --record-outcome   (boolean flag, presence-only; matches the existing
//                        `--heartbeat` bare-flag precedent)
//     Switches this invocation into a distinct "recording" beat: it persists
//     one observation via `lib/history.mjs`'s `recordObservation` and prints
//     a minimal confirmation JSON to stdout, `{ "recorded": true,
//     "operationType": <type>, "orchestratorId": <id> }` — it does NOT run
//     the normal collect -> classify -> dibs -> traffic-light pipeline at
//     all (recording is a standalone beat type, not a spawn-decision beat).
//
//   --operation-type=<type>   (required with --record-outcome)
//     Passed through to `recordObservation`'s `observation.operationType`.
//     Missing/absent: reject before any file I/O, exit 2, clear stderr
//     message — mirroring the exact style of the existing `--heartbeat` /
//     `--desired-agents` validation precedent in cli.mjs ("agent-resource-
//     management: --operation-type=<type> is required for --record-outcome").
//
//   --orchestrator-id=<id>   (required with --record-outcome; same flag the
//                              normal beat already requires)
//     Missing/absent under --record-outcome: same treatment as
//     --operation-type — exit 2, clear stderr, no file I/O attempted.
//
//   --peak-memory-mb=<n>   (optional numeric)
//     Passed through to `observation.peakMemoryMb` when present.
//
//   --crashed   (optional boolean flag, presence-only)
//     Passed through as `observation.crashed: true`.
//
//   Combined with --desired-agents or --heartbeat in the same invocation:
//     Rejected outright as an invalid flag combination — recording is a
//     distinct, standalone beat type, not composable with a spawn-decision
//     beat (per the Build Plan's documented Phase 2 edge case). Exit 2,
//     clear stderr, before any file I/O.
//
//   History file path unwritable at record time:
//     Fails open — exactly like every other coordination/history-file I/O
//     path in cli.mjs (ARM_COORDINATION_FILE's own "coordination file
//     unavailable ... proceeding without dibs" precedent). Warn to stderr,
//     still exit 0, still print a (best-effort) confirmation JSON to stdout
//     rather than crashing the caller — a beat that can't persist evidence
//     this time is strictly better than one that kills the orchestrator's
//     calling loop.
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
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

let workDir;
let historyFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-record-outcome-'));
  historyFilePath = join(workDir, 'history.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

test('records a valid outcome and exits 0 with a minimal confirmation JSON on stdout', async () => {
  const { status, stdout, stderr } = runCli(
    [
      '--record-outcome',
      '--operation-type=test-agent',
      '--orchestrator-id=orc-a',
      '--peak-memory-mb=900',
    ],
    { ARM_HISTORY_FILE: historyFilePath },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');

  let parsed;
  expect(() => {
    parsed = JSON.parse(stdout);
  }).not.toThrow();
  expect(parsed).toMatchObject({
    recorded: true,
    operationType: 'test-agent',
    orchestratorId: 'orc-a',
  });

  const history = await readHistory(historyFilePath, 'test-agent');
  expect(history.raw).toHaveLength(1);
  expect(history.raw[0]).toMatchObject({
    operationType: 'test-agent',
    orchestratorId: 'orc-a',
    peakMemoryMb: 900,
  });
});

test('records a valid outcome with --crashed and persists crashed:true', async () => {
  const { status, stdout, stderr } = runCli(
    [
      '--record-outcome',
      '--operation-type=test-agent',
      '--orchestrator-id=orc-b',
      '--peak-memory-mb=2200',
      '--crashed',
    ],
    { ARM_HISTORY_FILE: historyFilePath },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');

  const parsed = JSON.parse(stdout);
  expect(parsed).toMatchObject({
    recorded: true,
    operationType: 'test-agent',
    orchestratorId: 'orc-b',
  });

  const history = await readHistory(historyFilePath, 'test-agent');
  expect(history.raw).toHaveLength(1);
  expect(history.raw[0]).toMatchObject({
    operationType: 'test-agent',
    orchestratorId: 'orc-b',
    peakMemoryMb: 2200,
    crashed: true,
  });
});

test('rejects missing --operation-type with exit 2, clear stderr, no file written', async () => {
  const { status, stdout, stderr } = runCli(
    ['--record-outcome', '--orchestrator-id=orc-c', '--peak-memory-mb=500'],
    { ARM_HISTORY_FILE: historyFilePath },
  );

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--operation-type/);
  expect(stderr).toMatch(/required/i);

  const history = await readHistory(historyFilePath, 'unknown-type');
  expect(history.raw).toHaveLength(0);
});

test('rejects missing --orchestrator-id with exit 2, clear stderr, no file written', async () => {
  const { status, stdout, stderr } = runCli(
    ['--record-outcome', '--operation-type=test-agent', '--peak-memory-mb=500'],
    { ARM_HISTORY_FILE: historyFilePath },
  );

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--orchestrator-id/);
  expect(stderr).toMatch(/required/i);

  const history = await readHistory(historyFilePath, 'test-agent');
  expect(history.raw).toHaveLength(0);
});

test('degrades gracefully (exit 0, valid JSON, stderr warning) when the history file path is unwritable', async () => {
  // Mirrors cli.jest.spec.mjs's own coordination-file-directory-unwritable
  // idiom exactly: make a path COMPONENT of the parent directory a plain
  // file rather than a directory, so `mkdir(..., { recursive: true })` (or
  // any write attempt through it) fails with ENOTDIR regardless of the
  // running user's permissions — a portable, CI-safe way to force the
  // fallback path.
  const blockingFile = join(workDir, 'not-a-directory');
  await writeFile(blockingFile, 'blocking');
  const unwritableHistoryPath = join(blockingFile, 'no-such-parent-dir', 'history.json');

  const { status, stdout, stderr } = runCli(
    ['--record-outcome', '--operation-type=test-agent', '--orchestrator-id=orc-d', '--peak-memory-mb=700'],
    { ARM_HISTORY_FILE: unwritableHistoryPath },
  );

  // Chosen safer default (documented above): fail open, never crash the
  // caller — warn on stderr, still exit 0 with a parseable confirmation.
  expect(status).toBe(0);

  let parsed;
  expect(() => {
    parsed = JSON.parse(stdout);
  }).not.toThrow();
  expect(parsed).toMatchObject({ operationType: 'test-agent', orchestratorId: 'orc-d' });

  expect(stderr.length).toBeGreaterThan(0);
  expect(stderr.toLowerCase()).toMatch(/history|record|warn/);
});

test('rejects --record-outcome combined with --desired-agents in the same invocation, exit 2', async () => {
  const { status, stdout, stderr } = runCli(
    [
      '--record-outcome',
      '--operation-type=test-agent',
      '--orchestrator-id=orc-e',
      '--desired-agents=2',
    ],
    { ARM_HISTORY_FILE: historyFilePath },
  );

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--record-outcome/);
  expect(stderr).toMatch(/--desired-agents/);

  const history = await readHistory(historyFilePath, 'test-agent');
  expect(history.raw).toHaveLength(0);
});

// ---------------------------------------------------------------------------
// Phase 5 review (third pass), Medium #2: `--crashed=false` and
// `--record-outcome=false` were both silently treated as truthy (any
// non-empty string passes an `if (flags.crashed)` / `if
// (flags['record-outcome'])` check) — corrupting the exact crashed/not-crashed
// signal `estimateOperationCost` relies on to exclude crashed observations
// from its clean-sample median. Both flags now reuse the same bare-or-`=true`
// -only validation `--heartbeat` already established: any
// other value hard-rejects, exit 2, clear stderr.
// ---------------------------------------------------------------------------

test('rejects --crashed=false with exit 2, clear stderr, no file written', async () => {
  const { status, stdout, stderr } = runCli(
    [
      '--record-outcome',
      '--operation-type=test-agent',
      '--orchestrator-id=orc-g',
      '--peak-memory-mb=900',
      '--crashed=false',
    ],
    { ARM_HISTORY_FILE: historyFilePath },
  );

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--crashed/);

  const history = await readHistory(historyFilePath, 'test-agent');
  expect(history.raw).toHaveLength(0);
});

test('rejects --record-outcome=false with exit 2, clear stderr, no file written', async () => {
  const { status, stdout, stderr } = runCli(
    ['--record-outcome=false', '--operation-type=test-agent', '--orchestrator-id=orc-h'],
    { ARM_HISTORY_FILE: historyFilePath },
  );

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--record-outcome/);

  const history = await readHistory(historyFilePath, 'test-agent');
  expect(history.raw).toHaveLength(0);
});

test('bare --record-outcome (Phase 2 precedent) still dispatches and works after boolean-flag validation was added', async () => {
  const { status, stdout, stderr } = runCli(
    ['--record-outcome', '--operation-type=test-agent', '--orchestrator-id=orc-i', '--peak-memory-mb=1100'],
    { ARM_HISTORY_FILE: historyFilePath },
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');

  const parsed = JSON.parse(stdout);
  expect(parsed).toMatchObject({
    recorded: true,
    operationType: 'test-agent',
    orchestratorId: 'orc-i',
  });

  const history = await readHistory(historyFilePath, 'test-agent');
  expect(history.raw).toHaveLength(1);
  expect(history.raw[0]).toMatchObject({ orchestratorId: 'orc-i', peakMemoryMb: 1100 });
});

test('rejects --record-outcome combined with --heartbeat in the same invocation, exit 2', async () => {
  const { status, stdout, stderr } = runCli(
    ['--record-outcome', '--operation-type=test-agent', '--orchestrator-id=orc-f', '--heartbeat'],
    { ARM_HISTORY_FILE: historyFilePath },
  );

  expect(status).toBe(2);
  expect(stdout).toBe('');
  expect(stderr).toMatch(/--record-outcome/);
  expect(stderr).toMatch(/--heartbeat/);

  const history = await readHistory(historyFilePath, 'test-agent');
  expect(history.raw).toHaveLength(0);
});
