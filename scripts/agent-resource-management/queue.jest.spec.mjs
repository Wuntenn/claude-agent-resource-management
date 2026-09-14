// Phase 0 — outer acceptance test for
// "agent-resource-management: durable work queue — hold means enqueue, not
// refuse".
//
// This is the RED outer test for the whole ticket. Today, when
// `--desired-agents`/`--claim` deny admission (a `hold`-style decision), the
// caller's work item is simply lost — the orchestrator has to remember to
// retry it itself, and nothing survives across process restarts. This test
// specifies (via a black-box, real-process-spawning CLI contract — never
// importing cli.mjs or a queue module directly, matching every sibling spec
// in this directory) a DURABLE work queue: an item enqueued by one `node
// cli.mjs` process must still be there — peekable and dequeueable — for a
// LATER, entirely separate `node cli.mjs` process, even though the process
// that enqueued it has already exited (i.e. survives "process death").
//
// None of `--enqueue`, `--peek`, `--dequeue`, or `lib/queue.mjs` exist yet
// (as of Phase 0) — every scenario below is EXPECTED to fail: cli.mjs's
// `parseArgs` has no special handling for these flags today, so they
// silently fall through to the pre-existing default `--desired-agents`
// spawn-decision beat (see cli.mjs's `main()`, ~line 2100 onward) and print
// a `{"type":"spawn-allowed", ...}`-shaped traffic-light object instead of
// any queue-shaped JSON — every assertion on the SHAPE of that output below
// fails for that reason. This file goes green only once Phases 1 (lib/
// queue.mjs) and 2 (CLI wiring: --enqueue/--peek/--dequeue) in the Build
// Plan are implemented; it is intentionally NOT skipped and NOT marked
// `.todo`.
//
// ---------------------------------------------------------------------------
// CLI contract this file specifies (for the builder to match in cli.mjs /
// lib/queue.mjs — none of this exists today; matches the issue Build
// Plan's Phase 1/2 acceptance criteria verbatim, this test does not invent
// its own schema):
//
//   node cli.mjs --enqueue=<agentClass>:<priority>:<commandRef> --orchestrator-id=<id>
//
//   Colon-delimited structured flag, modeled directly on `parseClaimSpec`'s
//   `<type>:<count>` precedent (a new `parseEnqueueSpec`, per the Build
//   Plan's Phase 2 objective). Appends one durable work item to the queue
//   file under the SAME lock/atomic-write machinery `./lib/coordination-
//   file.mjs` already uses for `ARM_COORDINATION_FILE` (`withLock`/
//   `writeEntriesAtomic` — no second locking primitive invented for the
//   queue). Fields, per the Build Plan's Phase 1 acceptance criteria:
//     - `agentClass`   (string)
//     - `priority`     (closed enum: `'low' | 'normal' | 'high'` — NOT
//                       numeric; there is no existing ARM concept of
//                       work-item priority to reuse, so the Build Plan pins
//                       this domain explicitly)
//     - `commandRef`   (bounded-length string — the command/skill reference
//                       needed to re-dispatch the item later)
//     - `enqueuedAt`   (ISO timestamp string, server-assigned — not caller-
//                       supplied)
//   Prints `{ "enqueued": true, "item": { agentClass, priority, commandRef,
//   enqueuedAt } }` to stdout, exit 0.
//
//   node cli.mjs --peek --orchestrator-id=<id>
//
//   Read-only: prints `{ "items": [ ... ] }`, the full live queue contents
//   ordered by priority DESCENDING (`high` > `normal` > `low`) then
//   `enqueuedAt` ASCENDING (ties broken oldest-first) — WITHOUT mutating the
//   queue file on disk (byte-for-byte identical before/after; the Build
//   Plan's Phase 2 edge cases explicitly require an mtime/contents check).
//
//   node cli.mjs --dequeue --orchestrator-id=<id>
//
//   Atomically returns AND removes the single highest-priority live queue
//   entry (same ordering as `--peek`) under one locked critical section —
//   two concurrent `--dequeue` calls racing for the same last item must
//   never both receive it. Prints `{ "item": { ... } }`, or an explicit
//   empty-queue marker `{ "item": null }` when the queue is empty (per the
//   Build Plan's Phase 2 edge case: "exits 0 with an explicit ... marker
//   (not an error)"), exit 0 in both cases.
//
//   `--orchestrator-id=<id>` is required on all three verbs (mirrors every
//   other beat-type flag's existing precedent) and is separately subject to
//   the existing `isReservedEntry` (`__foo__`) rejection the Build Plan
//   names for `--enqueue` — it is distinct from `agentClass` (the queue
//   entry's own field), not a substitute for it.
//
//   ARM_QUEUE_FILE   (env, required for test hermeticity)
//     Absolute path to the queue's own JSON file. Mirrors
//     `ARM_COORDINATION_FILE`'s existing env-var-override pattern exactly
//     (cli.mjs ~line 1004: `if (process.env.ARM_COORDINATION_FILE) return
//     resolve(process.env.ARM_COORDINATION_FILE);`) — a distinct file/env
//     var from `ARM_COORDINATION_FILE`, never the same path, so the durable
//     work queue is fully separate from ARM's existing
//     `resource-coordination.json` admission-ledger file and from
//     `LIVE_AGENT_ADMISSION_CLAIM_TYPE`'s claim bookkeeping within it.
//
// Every invocation below ALSO sets `ARM_COORDINATION_FILE` to its own
// per-test `mkdtemp` path (never the production default) purely so that if
// `--enqueue`/`--peek`/`--dequeue` fall through to today's unwired default
// beat (the expected RED failure mode), that fallback beat's own
// coordination-file read/write is hermetically contained too, and to let
// assertion (d) below directly diff the coordination file's contents
// before/after the whole queue scenario runs.
// ---------------------------------------------------------------------------

import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises';
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
 * once the process exits, but (unlike `runCli`/`spawnSync`) does not block
 * this test's event loop while the child runs, so calls started back-to-back
 * via `Promise.all` actually race each other for the same queue-file lock.
 * Mirrors ./claim.jest.spec.mjs's and ./outer-acceptance.jest.spec.mjs's
 * `runCliAsync` idiom exactly (the "raceCli" concurrency pattern named in
 * this ticket's Build Plan).
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

function parseStdout(stdout) {
  return JSON.parse(stdout);
}

let workDir;
let queueFilePath;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-queue-outer-acceptance-'));
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

describe('outer acceptance — durable work queue: hold means enqueue, not refuse', () => {
  test(
    'a work item enqueued by one CLI process is still peekable and dequeueable by a fresh CLI process, ' +
      'surviving process death',
    async () => {
      // -----------------------------------------------------------------
      // Setup: snapshot the coordination file's state BEFORE any queue
      // activity, so assertion (d) below can prove the queue never touches
      // it. It does not exist yet at this point (mkdtemp gave us a fresh
      // workDir) — that absence is itself part of the baseline.
      // -----------------------------------------------------------------
      const coordinationFileBefore = await readFile(coordinationFilePath, 'utf8').catch(
        () => null,
      );
      expect(coordinationFileBefore).toBeNull();

      // -----------------------------------------------------------------
      // Step 1: two SEPARATE CLI processes each enqueue one work item, at
      // different priorities, so ordering has something real to prove.
      // Each process exits fully before the next command runs — the queue
      // must be durable across that process death, not held in any
      // in-memory state.
      // -----------------------------------------------------------------
      const enqueueLow = runCli(
        ['--enqueue=implementation-agent:low:run-low-priority-command', '--orchestrator-id=orch-a'],
        baseEnv(),
      );
      expect(enqueueLow.status).toBe(0);
      expect(enqueueLow.stderr).toBe('');
      const parsedEnqueueLow = parseStdout(enqueueLow.stdout);
      expect(parsedEnqueueLow.enqueued).toBe(true);
      expect(parsedEnqueueLow.item).toMatchObject({
        agentClass: 'implementation-agent',
        priority: 'low',
        commandRef: 'run-low-priority-command',
        orchestratorId: 'orch-a',
      });
      // `enqueuedAt` is a server-assigned ISO timestamp string, never
      // caller-supplied.
      expect(typeof parsedEnqueueLow.item.enqueuedAt).toBe('string');
      expect(() => new Date(parsedEnqueueLow.item.enqueuedAt).toISOString()).not.toThrow();

      const enqueueHigh = runCli(
        ['--enqueue=test-agent:high:run-high-priority-command', '--orchestrator-id=orch-b'],
        baseEnv(),
      );
      expect(enqueueHigh.status).toBe(0);
      expect(enqueueHigh.stderr).toBe('');
      const parsedEnqueueHigh = parseStdout(enqueueHigh.stdout);
      expect(parsedEnqueueHigh.enqueued).toBe(true);
      expect(parsedEnqueueHigh.item).toMatchObject({
        agentClass: 'test-agent',
        priority: 'high',
        commandRef: 'run-high-priority-command',
      });

      // -----------------------------------------------------------------
      // (a) --peek, from a FRESH, entirely separate CLI process (neither
      // enqueuing process is still alive) returns both items ordered by
      // priority-then-enqueue-time (highest priority first), WITHOUT
      // mutating the queue file.
      // -----------------------------------------------------------------
      const queueFileBeforePeek = await readFile(queueFilePath, 'utf8');

      const peek = runCli(['--peek', '--orchestrator-id=orch-c'], baseEnv());
      expect(peek.status).toBe(0);
      expect(peek.stderr).toBe('');
      const parsedPeek = parseStdout(peek.stdout);

      expect(Array.isArray(parsedPeek.items)).toBe(true);
      expect(parsedPeek.items).toHaveLength(2);
      // Highest priority ("high", "run-high-priority-command") ordered
      // first.
      expect(parsedPeek.items[0]).toMatchObject({
        commandRef: 'run-high-priority-command',
        priority: 'high',
      });
      expect(parsedPeek.items[1]).toMatchObject({
        commandRef: 'run-low-priority-command',
        priority: 'low',
      });

      const queueFileAfterPeek = await readFile(queueFilePath, 'utf8');
      expect(queueFileAfterPeek).toBe(queueFileBeforePeek);

      // -----------------------------------------------------------------
      // (b) --dequeue, from yet ANOTHER fresh CLI process, atomically
      // returns and removes the highest-priority live entry.
      // -----------------------------------------------------------------
      const dequeue = runCli(['--dequeue', '--orchestrator-id=orch-d'], baseEnv());
      expect(dequeue.status).toBe(0);
      expect(dequeue.stderr).toBe('');
      const parsedDequeue = parseStdout(dequeue.stdout);

      expect(parsedDequeue.item).toMatchObject({
        commandRef: 'run-high-priority-command',
        priority: 'high',
      });

      // The dequeued item is genuinely gone — a subsequent --peek sees only
      // the remaining lower-priority item.
      const peekAfterDequeue = runCli(['--peek', '--orchestrator-id=orch-e'], baseEnv());
      expect(peekAfterDequeue.status).toBe(0);
      const parsedPeekAfterDequeue = parseStdout(peekAfterDequeue.stdout);
      expect(parsedPeekAfterDequeue.items).toHaveLength(1);
      expect(parsedPeekAfterDequeue.items[0]).toMatchObject({
        commandRef: 'run-low-priority-command',
        priority: 'low',
      });

      // -----------------------------------------------------------------
      // (c) two concurrent --enqueue calls racing against the SAME queue
      // file never tear/corrupt it — the file on disk is well-formed JSON
      // after the race resolves, and both items survive (no lost write).
      // Mirrors claim.jest.spec.mjs's/outer-acceptance.jest.spec.mjs's
      // `raceCli`/Promise.all concurrency idiom exactly.
      // -----------------------------------------------------------------
      const [raceResultA, raceResultB] = await Promise.all([
        raceCli(
          ['--enqueue=implementation-agent:normal:race-command-a', '--orchestrator-id=orch-race-a'],
          baseEnv(),
        ),
        raceCli(
          ['--enqueue=implementation-agent:normal:race-command-b', '--orchestrator-id=orch-race-b'],
          baseEnv(),
        ),
      ]);

      expect(raceResultA.status).toBe(0);
      expect(raceResultB.status).toBe(0);
      expect(raceResultA.stderr).toBe('');
      expect(raceResultB.stderr).toBe('');

      const queueFileAfterRace = await readFile(queueFilePath, 'utf8');
      // Well-formed JSON, no partial/torn write — JSON.parse throws on a
      // torn write and this assertion surfaces that failure directly.
      let parsedQueueFileAfterRace;
      expect(() => {
        parsedQueueFileAfterRace = JSON.parse(queueFileAfterRace);
      }).not.toThrow();
      expect(Array.isArray(parsedQueueFileAfterRace)).toBe(true);

      const peekAfterRace = runCli(['--peek', '--orchestrator-id=orch-f'], baseEnv());
      expect(peekAfterRace.status).toBe(0);
      const parsedPeekAfterRace = parseStdout(peekAfterRace.stdout);
      // The pre-existing low-priority item plus BOTH racing enqueues — no
      // write was lost to the race.
      expect(parsedPeekAfterRace.items).toHaveLength(3);
      const raceCommandRefs = parsedPeekAfterRace.items.map((item) => item.commandRef).sort();
      expect(raceCommandRefs).toEqual([
        'race-command-a',
        'race-command-b',
        'run-low-priority-command',
      ]);

      // -----------------------------------------------------------------
      // (d) none of the above ever touched ARM's existing coordination
      // file (LIVE_AGENT_ADMISSION_CLAIM_TYPE / resource-coordination.json)
      // — the queue file is fully separate. The coordination file must
      // still not exist: every command above went through --enqueue/--peek/
      // --dequeue, never --claim/--desired-agents/--heartbeat, so nothing
      // should have caused ./lib/coordination-file.mjs's declareDibs/
      // readDibs to create or write it.
      // -----------------------------------------------------------------
      const coordinationFileAfter = await readFile(coordinationFilePath, 'utf8').catch(
        () => null,
      );
      expect(coordinationFileAfter).toBeNull();
    },
  );

  // ---------------------------------------------------------------------
  // Supplementary correctness property referenced by (b): --dequeue's
  // "atomically returns and removes ... under a single locked critical
  // section" language is a race-safety claim on its own, independent of the
  // ordering scenario above — two concurrent --dequeue calls racing for the
  // SAME single remaining item must never both receive it (and never both
  // receive null while the item silently vanishes uncollected).
  // ---------------------------------------------------------------------
  test('two concurrent --dequeue calls racing for the single remaining queue item: exactly one receives it, the other receives null', async () => {
    const env = baseEnv();

    const seed = runCli(
      ['--enqueue=implementation-agent:normal:only-command', '--orchestrator-id=orch-seed'],
      env,
    );
    expect(seed.status).toBe(0);

    const [dequeueA, dequeueB] = await Promise.all([
      raceCli(['--dequeue', '--orchestrator-id=orch-race-dequeue-a'], env),
      raceCli(['--dequeue', '--orchestrator-id=orch-race-dequeue-b'], env),
    ]);

    expect(dequeueA.status).toBe(0);
    expect(dequeueB.status).toBe(0);

    const parsedA = JSON.parse(dequeueA.stdout);
    const parsedB = JSON.parse(dequeueB.stdout);

    const commandRefs = [parsedA.item?.commandRef ?? null, parsedB.item?.commandRef ?? null];
    // Exactly one racer gets the real item, the other gets null — never
    // both null (the item lost) and never both non-null (the item
    // double-granted). Order-independent: either racer may win the race.
    expect(commandRefs).toHaveLength(2);
    expect(commandRefs).toEqual(expect.arrayContaining([null, 'only-command']));
  });
});

// Sanity check on this test file's own setup, independent of any queue
// behavior: writeFile/readFile against a plain tmp path work as expected in
// this test environment, so a failure above can be trusted to originate
// from cli.mjs's (missing) queue behavior, not from this file's own
// fs-handling.
test('sanity: this spec file can itself read/write a plain tmp file (setup self-check, not a queue assertion)', async () => {
  const sanityPath = join(workDir, 'sanity.txt');
  await writeFile(sanityPath, 'ok', 'utf8');
  const contents = await readFile(sanityPath, 'utf8');
  expect(contents).toBe('ok');
});
