// Phase 2 — `--dequeue` CLI verb wiring.
//
// RED by construction: cli.mjs's `parseArgs` has no special handling for
// `--dequeue` yet, so it silently falls through to the pre-existing default
// `--desired-agents` spawn-decision beat and prints a
// `{"type":"spawn-allowed", ...}`-shaped traffic-light object instead of any
// queue-shaped JSON. Every assertion on the shape of stdout below fails for
// that reason until Phase 2 lands.
//
// Black-box, real-process-spawning tests — mirrors the established idiom in
// ./claim.jest.spec.mjs/./cli.jest.spec.mjs (never importing cli.mjs
// directly). This file deliberately does NOT re-cover the "item enqueued by
// one process is dequeueable by a fresh process / two concurrent --dequeue
// calls racing for the single remaining item" scenarios already owned
// end-to-end by ./queue.jest.spec.mjs (Phase 0 outer acceptance) — it covers
// the Phase-2-specific edge cases from the Build Plan's Test Strategy table:
// flag-conflict checking, reserved-id rejection, the CLI-level empty-queue
// exit-0 marker, atomic-removal (a second immediate --dequeue does not
// re-return the same item), and a THIRD independent raceCli-style
// concurrency check at the CLI layer.
//
// CLI contract under test (see ./queue.jest.spec.mjs's header for the full
// contract this file does not repeat):
//
//   node cli.mjs --dequeue --orchestrator-id=<id>
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
 * other for the same queue-file lock. Mirrors
 * ./claim.jest.spec.mjs's/./queue.jest.spec.mjs's `runCliAsync`/`raceCli`
 * idiom exactly.
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
  workDir = await mkdtemp(join(tmpdir(), 'arm-dequeue-'));
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

function cli(args, extraEnv = {}) {
  return runCli(args, baseEnv(extraEnv));
}

function seedOneItem(commandRef = 'seed-command', orchestratorId = 'orch-seed') {
  const result = cli([
    `--enqueue=implementation-agent:normal:${commandRef}`,
    `--orchestrator-id=${orchestratorId}`,
  ]);
  expect(result.status).toBe(0);
  return result;
}

// ---------------------------------------------------------------------------
// Empty-queue exit-0 marker (Build Plan Phase 2 edge case).
// ---------------------------------------------------------------------------

test('--dequeue on an empty/nonexistent queue exits 0 with an explicit { item: null } marker, not an error', () => {
  const result = cli(['--dequeue', '--orchestrator-id=orch-empty']);

  expect(result.status).toBe(0);
  expect(result.stderr).toBe('');
  const parsed = JSON.parse(result.stdout);
  expect(parsed).toHaveProperty('item', null);
});

// ---------------------------------------------------------------------------
// Atomic removal — a second immediate --dequeue does not return the same
// item again.
// ---------------------------------------------------------------------------

test('--dequeue atomically removes the returned entry — a second immediate --dequeue does not return it again', () => {
  seedOneItem('only-command', 'orch-enqueuer');

  const first = cli(['--dequeue', '--orchestrator-id=orch-first']);
  expect(first.status).toBe(0);
  expect(first.stderr).toBe('');
  const parsedFirst = JSON.parse(first.stdout);
  // The dequeued entry retains the ORIGINAL enqueuing orchestrator's id
  // (`orch-enqueuer`), not the dequeuing caller's own `--orchestrator-id`
  // (`orch-first`) — proving the persisted `orchestratorId` genuinely
  // survives the full enqueue/dequeue round-trip on disk.
  expect(parsedFirst.item).toMatchObject({ commandRef: 'only-command', orchestratorId: 'orch-enqueuer' });

  const second = cli(['--dequeue', '--orchestrator-id=orch-second']);
  expect(second.status).toBe(0);
  expect(second.stderr).toBe('');
  const parsedSecond = JSON.parse(second.stdout);
  expect(parsedSecond.item).toBeNull();
});

// ---------------------------------------------------------------------------
// Concurrency — two concurrent --dequeue child processes racing for the SAME
// single item never both return it (a THIRD, CLI-layer-focused check,
// distinct from ./queue.jest.spec.mjs's own outer-acceptance race test —
// this one additionally asserts stderr is clean on both sides and that the
// loser's response is the well-formed null marker, not a crash/hang).
// ---------------------------------------------------------------------------

test('two concurrent --dequeue child processes never both return the same entry', async () => {
  seedOneItem('contested-command');

  const [resultA, resultB] = await Promise.all([
    raceCli(['--dequeue', '--orchestrator-id=orch-race-dequeue-a'], baseEnv()),
    raceCli(['--dequeue', '--orchestrator-id=orch-race-dequeue-b'], baseEnv()),
  ]);

  expect(resultA.status).toBe(0);
  expect(resultB.status).toBe(0);
  expect(resultA.stderr).toBe('');
  expect(resultB.stderr).toBe('');

  const parsedA = JSON.parse(resultA.stdout);
  const parsedB = JSON.parse(resultB.stdout);

  const commandRefs = [parsedA.item?.commandRef ?? null, parsedB.item?.commandRef ?? null];
  // Exactly one racer gets the real item, the other gets the null marker —
  // never both null (the item lost) and never both non-null (the item
  // double-granted). Order-independent: either racer may win the race.
  expect(commandRefs).toHaveLength(2);
  expect(commandRefs).toEqual(expect.arrayContaining([null, 'contested-command']));
});

// ---------------------------------------------------------------------------
// --orchestrator-id required / reserved-id rejection — mirrors --claim's own
// established precedent (claim.jest.spec.mjs).
// ---------------------------------------------------------------------------

test('--dequeue without --orchestrator-id is rejected, exit 2, empty stdout', () => {
  const result = cli(['--dequeue']);

  expect(result.status).toBe(2);
  expect(result.stdout).toBe('');
  expect(result.stderr).toMatch(/--orchestrator-id/);
});

test('--dequeue with a reserved __foo__ orchestrator id is rejected, exit 2, empty stdout', () => {
  seedOneItem('untouched-command');

  const result = cli(['--dequeue', '--orchestrator-id=__foo__']);

  expect(result.status).toBe(2);
  expect(result.stdout).toBe('');
  expect(result.stderr).toMatch(/reserved/);
});

test('--dequeue rejected with a reserved orchestrator id does not remove the queue item', async () => {
  seedOneItem('untouched-command');

  const rejected = cli(['--dequeue', '--orchestrator-id=__foo__']);
  expect(rejected.status).toBe(2);

  const peeked = cli(['--peek', '--orchestrator-id=orch-verify']);
  expect(peeked.status).toBe(0);
  const parsedPeek = JSON.parse(peeked.stdout);
  expect(parsedPeek.items).toHaveLength(1);
  expect(parsedPeek.items[0]).toMatchObject({ commandRef: 'untouched-command' });
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
])('--dequeue combined with %s in the same invocation is rejected, exit 2, naming --dequeue', (conflictingFlagArg) => {
  seedOneItem('untouched-by-conflict');

  const result = cli(['--dequeue', conflictingFlagArg, '--orchestrator-id=orch-combo']);

  expect(result.status).toBe(2);
  expect(result.stdout).toBe('');
  expect(result.stderr).toMatch(/--dequeue/);
});

test('--dequeue combined with a conflicting flag does not remove the queue item', async () => {
  seedOneItem('untouched-by-conflict');

  const rejected = cli(['--dequeue', '--claim=test-agent:1', '--orchestrator-id=orch-combo-verify']);
  expect(rejected.status).toBe(2);

  const peeked = cli(['--peek', '--orchestrator-id=orch-combo-verify-peek']);
  expect(peeked.status).toBe(0);
  const parsedPeek = JSON.parse(peeked.stdout);
  expect(parsedPeek.items).toHaveLength(1);
  expect(parsedPeek.items[0]).toMatchObject({ commandRef: 'untouched-by-conflict' });
});

// ---------------------------------------------------------------------------
// Priority-then-enqueue-time ordering is honored by --dequeue (not just
// --peek) — the top-priority entry is the one returned, mirroring
// ./queue.jest.spec.mjs's ordering contract from the dequeue side.
// ---------------------------------------------------------------------------

test('--dequeue returns the highest-priority entry first, honoring the same ordering --peek reports', () => {
  const low = cli([
    '--enqueue=implementation-agent:low:low-priority-command',
    '--orchestrator-id=orch-order-low',
  ]);
  expect(low.status).toBe(0);

  const high = cli([
    '--enqueue=implementation-agent:high:high-priority-command',
    '--orchestrator-id=orch-order-high',
  ]);
  expect(high.status).toBe(0);

  const dequeued = cli(['--dequeue', '--orchestrator-id=orch-order-dequeue']);
  expect(dequeued.status).toBe(0);
  const parsed = JSON.parse(dequeued.stdout);
  expect(parsed.item).toMatchObject({ commandRef: 'high-priority-command', priority: 'high' });
});

// ---------------------------------------------------------------------------
// ARM_QUEUE_FILE isolation from the real host queue path.
// ---------------------------------------------------------------------------

test('ARM_QUEUE_FILE isolates test runs from the real host queue path — dequeue operates only on the isolated temp path', async () => {
  seedOneItem('isolation-check-command');

  const dequeued = cli(['--dequeue', '--orchestrator-id=orch-isolation']);
  expect(dequeued.status).toBe(0);
  const parsed = JSON.parse(dequeued.stdout);
  expect(parsed.item).toMatchObject({ commandRef: 'isolation-check-command' });

  const contents = await readFile(queueFilePath, 'utf8');
  const parsedFile = JSON.parse(contents);
  expect(Array.isArray(parsedFile)).toBe(true);
  expect(parsedFile).toHaveLength(0);
});

test('--dequeue never shells out — no shell metacharacter in commandRef survives round-trip unescaped/misinterpreted', () => {
  const commandRefWithMetachars = 'echo hi; rm -rf / && $(whoami) `id`';
  const seeded = cli([
    `--enqueue=implementation-agent:normal:${commandRefWithMetachars}`,
    '--orchestrator-id=orch-shell-safety',
  ]);
  expect(seeded.status).toBe(0);

  const dequeued = cli(['--dequeue', '--orchestrator-id=orch-shell-safety-dequeue']);
  expect(dequeued.status).toBe(0);
  expect(dequeued.stderr).toBe('');
  const parsed = JSON.parse(dequeued.stdout);
  // The raw string comes back byte-for-byte, unexecuted and unmangled — proof
  // it was only ever treated as opaque data, never interpolated into a shell
  // command.
  expect(parsed.item.commandRef).toBe(commandRefWithMetachars);
});
