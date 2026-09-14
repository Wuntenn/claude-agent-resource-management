// Phase 2 — `--enqueue`/`--peek` CLI verb wiring.
//
// RED by construction: cli.mjs's `parseArgs` has no special handling for
// `--enqueue`/`--peek` yet, so both flags silently fall through to the
// pre-existing default `--desired-agents` spawn-decision beat and print a
// `{"type":"spawn-allowed", ...}`-shaped traffic-light object instead of any
// queue-shaped JSON. Every assertion on the shape of stdout below fails for
// that reason until Phase 2 lands.
//
// Black-box, real-process-spawning tests — mirrors the established idiom in
// ./claim.jest.spec.mjs/./cli.jest.spec.mjs (never importing cli.mjs
// directly). This file deliberately does NOT re-cover the "two separate CLI
// processes / process-death survival / concurrent-enqueue-never-tears-the-
// file" scenarios already owned end-to-end by ./queue.jest.spec.mjs (Phase 0
// outer acceptance) — it covers the Phase-2-specific edge cases from the
// Build Plan's Test Strategy table: flag-conflict checking, malformed-spec
// rejection, reserved-id rejection, and --peek's non-mutation guarantee.
//
// CLI contract under test (see ./queue.jest.spec.mjs's header for the full
// contract this file does not repeat):
//
//   node cli.mjs --enqueue=<agentClass>:<priority>:<commandRef> --orchestrator-id=<id>
//   node cli.mjs --peek --orchestrator-id=<id>
//
//   ARM_QUEUE_FILE (env, required for test hermeticity) — mirrors
//   ARM_COORDINATION_FILE's existing env-var-override pattern.

import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, rm, readFile, stat } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const FAKE_COLLECT_JSON = JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN });

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
 * once the child exits, without blocking this test's event loop while it
 * runs, so calls started back-to-back via `Promise.all` actually race each
 * other. Mirrors ./claim.jest.spec.mjs's/./queue.jest.spec.mjs's
 * `runCliAsync`/`raceCli` idiom exactly.
 */
function raceCli(args, extraEnv) {
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
let queueFilePath;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-enqueue-'));
  queueFilePath = join(workDir, 'queue.json');
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

function baseEnv(extra = {}) {
  return {
    ARM_QUEUE_FILE: queueFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: FAKE_COLLECT_JSON,
    ...extra,
  };
}

function enqueue(args, extraEnv = {}) {
  return runCli(args, baseEnv(extraEnv));
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

test('--enqueue with a valid spec exits 0 and the item is visible via a subsequent --peek', () => {
  const enqueued = enqueue([
    '--enqueue=implementation-agent:normal:run-a-command',
    '--orchestrator-id=orch-happy',
  ]);

  expect(enqueued.status).toBe(0);
  expect(enqueued.stderr).toBe('');
  const parsedEnqueue = JSON.parse(enqueued.stdout);
  expect(parsedEnqueue.enqueued).toBe(true);
  expect(parsedEnqueue.item).toMatchObject({
    agentClass: 'implementation-agent',
    priority: 'normal',
    commandRef: 'run-a-command',
    // The validated --orchestrator-id must reach the persisted entry, not
    // just be validated-and-discarded (High review finding) — a future
    // dispatcher needs this to attribute/filter queued work by
    // owning orchestrator.
    orchestratorId: 'orch-happy',
  });
  expect(typeof parsedEnqueue.item.enqueuedAt).toBe('string');

  const peeked = enqueue(['--peek', '--orchestrator-id=orch-happy-peek']);
  expect(peeked.status).toBe(0);
  expect(peeked.stderr).toBe('');
  const parsedPeek = JSON.parse(peeked.stdout);
  expect(Array.isArray(parsedPeek.items)).toBe(true);
  expect(parsedPeek.items).toHaveLength(1);
  expect(parsedPeek.items[0]).toMatchObject({
    agentClass: 'implementation-agent',
    priority: 'normal',
    commandRef: 'run-a-command',
    orchestratorId: 'orch-happy',
  });
});

// ---------------------------------------------------------------------------
// Malformed-spec rejection — must fail closed, before any write.
// ---------------------------------------------------------------------------

test('--enqueue rejects a malformed spec (missing segments) with exit 2 and no file write occurs', () => {
  const result = enqueue([
    '--enqueue=implementation-agent:normal',
    '--orchestrator-id=orch-malformed-missing-segment',
  ]);

  expect(result.status).toBe(2);
  expect(result.stdout).toBe('');
  expect(result.stderr).toMatch(/--enqueue/);

  return expect(stat(queueFilePath)).rejects.toMatchObject({ code: 'ENOENT' });
});

test('--enqueue rejects an out-of-enum priority with exit 2 and no file write occurs', () => {
  const result = enqueue([
    '--enqueue=implementation-agent:urgent:run-a-command',
    '--orchestrator-id=orch-malformed-priority',
  ]);

  expect(result.status).toBe(2);
  expect(result.stdout).toBe('');
  expect(result.stderr).toMatch(/--enqueue/);

  return expect(stat(queueFilePath)).rejects.toMatchObject({ code: 'ENOENT' });
});

test('--enqueue rejects an empty commandRef segment with exit 2 and no file write occurs', () => {
  const result = enqueue([
    '--enqueue=implementation-agent:normal:',
    '--orchestrator-id=orch-malformed-empty-command-ref',
  ]);

  expect(result.status).toBe(2);
  expect(result.stdout).toBe('');
  expect(result.stderr).toMatch(/--enqueue/);

  return expect(stat(queueFilePath)).rejects.toMatchObject({ code: 'ENOENT' });
});

test('--enqueue rejects an empty agentClass segment with exit 2 and no file write occurs', () => {
  const result = enqueue([
    '--enqueue=:normal:run-a-command',
    '--orchestrator-id=orch-malformed-empty-agent-class',
  ]);

  expect(result.status).toBe(2);
  expect(result.stdout).toBe('');
  expect(result.stderr).toMatch(/--enqueue/);

  return expect(stat(queueFilePath)).rejects.toMatchObject({ code: 'ENOENT' });
});

// ---------------------------------------------------------------------------
// --orchestrator-id required / reserved-id rejection — mirrors --claim's own
// established precedent (claim.jest.spec.mjs).
// ---------------------------------------------------------------------------

test('--enqueue without --orchestrator-id is rejected, exit 2, empty stdout', () => {
  const result = enqueue(['--enqueue=implementation-agent:normal:run-a-command']);

  expect(result.status).toBe(2);
  expect(result.stdout).toBe('');
  expect(result.stderr).toMatch(/--orchestrator-id/);
});

test('--enqueue with a reserved __foo__ orchestrator id is rejected, exit 2, empty stdout', () => {
  const result = enqueue([
    '--enqueue=implementation-agent:normal:run-a-command',
    '--orchestrator-id=__foo__',
  ]);

  expect(result.status).toBe(2);
  expect(result.stdout).toBe('');
  expect(result.stderr).toMatch(/reserved/);

  return expect(stat(queueFilePath)).rejects.toMatchObject({ code: 'ENOENT' });
});

// ---------------------------------------------------------------------------
// Flag-conflict checking — mirrors the exact precedent every other beat-type
// flag already established (see cli.mjs's main(), --release/--claim/
// --query-capacity conflict blocks).
// ---------------------------------------------------------------------------

test.each([
  ['--claim=test-agent:1'],
  ['--release=test-agent:1'],
  ['--query-capacity=test-agent'],
  ['--record-outcome'],
  ['--desired-agents=1'],
  ['--heartbeat'],
])('--enqueue combined with %s in the same invocation is rejected, exit 2, naming --enqueue', (conflictingFlagArg) => {
  const result = enqueue([
    '--enqueue=implementation-agent:normal:run-a-command',
    conflictingFlagArg,
    '--orchestrator-id=orch-combo',
  ]);

  expect(result.status).toBe(2);
  expect(result.stdout).toBe('');
  expect(result.stderr).toMatch(/--enqueue/);

  return expect(stat(queueFilePath)).rejects.toMatchObject({ code: 'ENOENT' });
});

test.each([
  ['--claim=test-agent:1'],
  ['--release=test-agent:1'],
  ['--query-capacity=test-agent'],
  ['--record-outcome'],
  ['--desired-agents=1'],
  ['--heartbeat'],
])('--peek combined with %s in the same invocation is rejected, exit 2, naming --peek', (conflictingFlagArg) => {
  const result = enqueue(['--peek', conflictingFlagArg, '--orchestrator-id=orch-combo-peek']);

  expect(result.status).toBe(2);
  expect(result.stdout).toBe('');
  expect(result.stderr).toMatch(/--peek/);
});

// ---------------------------------------------------------------------------
// --peek non-mutation guarantee (Build Plan Phase 2 edge case).
// ---------------------------------------------------------------------------

test('--peek does not mutate the queue file contents or mtime', async () => {
  const seeded = enqueue([
    '--enqueue=implementation-agent:normal:run-a-command',
    '--orchestrator-id=orch-peek-seed',
  ]);
  expect(seeded.status).toBe(0);

  const contentsBefore = await readFile(queueFilePath, 'utf8');
  const statBefore = await stat(queueFilePath);

  // Sleep long enough that an mtime bump (if one wrongly occurred) would be
  // observable at whatever filesystem mtime resolution this host has.
  await new Promise((resolvePromise) => setTimeout(resolvePromise, 50));

  const peeked = enqueue(['--peek', '--orchestrator-id=orch-peek-read']);
  expect(peeked.status).toBe(0);
  expect(peeked.stderr).toBe('');

  const contentsAfter = await readFile(queueFilePath, 'utf8');
  const statAfter = await stat(queueFilePath);

  expect(contentsAfter).toBe(contentsBefore);
  expect(statAfter.mtimeMs).toBe(statBefore.mtimeMs);
});

test('--peek on an empty/nonexistent queue returns an empty items array, exit 0, and does not create the file', () => {
  const result = enqueue(['--peek', '--orchestrator-id=orch-peek-empty']);

  expect(result.status).toBe(0);
  expect(result.stderr).toBe('');
  const parsed = JSON.parse(result.stdout);
  expect(parsed.items).toEqual([]);

  return expect(stat(queueFilePath)).rejects.toMatchObject({ code: 'ENOENT' });
});

// ---------------------------------------------------------------------------
// ARM_QUEUE_FILE isolation from the real host queue path.
// ---------------------------------------------------------------------------

test('ARM_QUEUE_FILE isolates test runs from the real host queue path — a distinct temp path is actually written to', async () => {
  const result = enqueue([
    '--enqueue=implementation-agent:normal:isolation-check-command',
    '--orchestrator-id=orch-isolation',
  ]);
  expect(result.status).toBe(0);

  const contents = await readFile(queueFilePath, 'utf8');
  const parsed = JSON.parse(contents);
  expect(Array.isArray(parsed)).toBe(true);
  expect(parsed.some((item) => item.commandRef === 'isolation-check-command')).toBe(true);
});

// ---------------------------------------------------------------------------
// Concurrent --enqueue calls never construct a shell string / never crash —
// (torn-write/race safety itself is already pinned by ./queue.jest.spec.mjs;
// this asserts the CLI layer's own contribution: two concurrent enqueue
// invocations both succeed rather than one crashing the process).
// ---------------------------------------------------------------------------

test('two concurrent --enqueue invocations against the same queue file both exit 0', async () => {
  const [resultA, resultB] = await Promise.all([
    raceCli(
      ['--enqueue=implementation-agent:normal:race-a', '--orchestrator-id=orch-race-a'],
      baseEnv(),
    ),
    raceCli(
      ['--enqueue=implementation-agent:normal:race-b', '--orchestrator-id=orch-race-b'],
      baseEnv(),
    ),
  ]);

  expect(resultA.status).toBe(0);
  expect(resultB.status).toBe(0);
  expect(resultA.stderr).toBe('');
  expect(resultB.stderr).toBe('');

  const parsedA = JSON.parse(resultA.stdout);
  const parsedB = JSON.parse(resultB.stdout);
  expect(parsedA.enqueued).toBe(true);
  expect(parsedB.enqueued).toBe(true);

  const peeked = enqueue(['--peek', '--orchestrator-id=orch-race-verify']);
  const parsedPeek = JSON.parse(peeked.stdout);
  expect(parsedPeek.items).toHaveLength(2);
  const commandRefs = parsedPeek.items.map((item) => item.commandRef).sort();
  expect(commandRefs).toEqual(['race-a', 'race-b']);
});
