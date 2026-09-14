// Phase 3 RED unit suite for ("PreToolUse hook rewrite"), by design.
//
// RELATIONSHIP TO THE OUTER TEST
//   `pretooluse-arm-gate.outer-acceptance.jest.spec.mjs` (Phase 0, already
//   committed) is the AUTHORITATIVE integration-level contract. This file is
//   deliberately consistent with it and never contradicts it: same hook path,
//   same `ARM_CLI_PATH` / `ARM_COORDINATION_FILE` / `ARM_LIVENESS_LOG_FILE`
//   seams, same exit-code vocabulary (0 = allow, 2 = deny), same
//   recording-stand-in technique for counting real `cli.mjs` child processes.
//
//   The split follows the `scripts/check-pre-pr-sentinel.mjs` precedent:
//     - OUTER  = end-to-end tiers driven by a REAL `cli.mjs` beat.
//     - HERE   = per-branch behaviour of the hook itself, with the opted-in
//                state seeded through Phase 1's `recordEverOptedIn` primitive
//                and `cli.mjs` replaced by a purpose-built stand-in, so each
//                branch can be isolated and each failure mode forced.
//
//   Every test here still SPAWNS THE REAL HOOK as a real child process. The
//   hook is a process-shaped unit — its contract is (stdin JSON, env) ->
//   (exit code, stderr, files touched). Importing internals instead would pin
//   a module structure deliberately leaves to the implementer ("keep
//   the hook itself extremely small and simple"), and would not be able to
//   assert the process-spawn COUNT that the zero-cost claim rests on.
//
// RED BY CONSTRUCTION: `./pretooluse-arm-gate.mjs` does not exist in this
// worktree. Every `runHook(...)` therefore returns a non-zero, non-2 status
// today and every expectation below fails.
//
// ---------------------------------------------------------------------------
// JUDGMENT CALLS PINNED HERE (the implementer must NOT re-litigate these)
//
//   J1. A `deriveOrchestratorId` THROW (missing / blank / non-string /
//       whitespace-padded `session_id`) is treated exactly like
//       "never opted in": allow, exit 0, ZERO `cli.mjs` spawns, ZERO
//       liveness-log entries, and no crash / no stack trace on stderr.
//       Rationale: design decision 3 tiers on whether a rule has been
//       ESTABLISHED for this session. If we cannot derive the session's
//       identity we cannot have established anything for it, so there is no
//       sticky state to protect and fail-CLOSED would be denying on behalf of
//       a session we cannot name. Phase 2 deliberately provides NO shared
//       fallback id, so the alternative (invent one) is the cross-session
//       state-leak `orchestrator-id.mjs` exists to prevent. Fail open.
//
//   J2. A working `cli.mjs` that returns a legitimate NON-`spawn-allowed`
//       verdict (`hold`, `pause`, …) is a POLICY DENY, not a fail-closed
//       trip: exit 2, but ZERO `arm-gate-fail-closed` liveness entries.
//       Rationale: wants the fail-closed path's "real firing rate
//       … auditable and greppable". Logging ordinary policy denials into the
//       same bucket destroys exactly that measurement.
//
//   J3. `cli.mjs` exiting NON-ZERO is a fail-closed trip even if its stdout
//       happens to contain a parseable `spawn-allowed` verdict. A failed
//       process has not given a trustworthy answer.
//
//   J4. The hook is a pure READER of the coordination file — as a NET EFFECT
//       across the hook process AND every child it spawns, not merely across
//       its own `fs` calls. It must never create the file, and must never
//       modify it: the bytes are identical before and after every hook
//       invocation, on every path. The net-effect scoping is load-bearing —
//       most tests here substitute a stand-in `cli.mjs` (which mutates
//       nothing by construction), so the claim is additionally pinned against
//       the REAL binary by the dedicated test in the "opted in — the hook
//       consults cli.mjs" block below.
//
//   J5. The best-effort liveness-log append must be BOUNDED. With the log's
//       `.lock` already held by someone else, the hook must still resolve its
//       deny well inside `coordination-file.mjs`'s default lock ceiling
//       (`LOCK_MAX_ATTEMPTS` 2000 x `LOCK_RETRY_DELAY_MS` 5ms ~= 10s), which
//       would otherwise stack on top of the 5s `cli.mjs` timeout (Convergence
//       Analysis edge case 5).
// ---------------------------------------------------------------------------

import { spawn, spawnSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync, chmodSync, statSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { homedir, tmpdir } from 'node:os';

import { recordEverOptedIn } from '../lib/coordination-file.mjs';
import { readLivenessLog } from '../lib/liveness-log.mjs';
import { deriveOrchestratorId } from '../lib/orchestrator-id.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const HOOK = join(HERE, 'pretooluse-arm-gate.mjs');
const REAL_CLI = join(HERE, '..', 'cli.mjs');

/**
 * A GREEN host sample, so the one test in this file that drives the REAL
 * `cli.mjs` never touches the actual host. Mirrors the outer suite's fixture.
 */
const COLLECT_GREEN = JSON.stringify({
  memory: { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 },
  disk: { freeDiskGb: 60, declineRateGbPerHour: 0.5 },
});

/**
 * Every tool name the hook's `SPAWN_TOOL_NAMES` matcher claims to gate.
 *
 * Both are pinned SYMMETRICALLY, via `describe.each`, rather than one being
 * "the" constant and the other an afterthought. Before this, both spec files
 * hard-coded `'Task'` alone — and `'Task'` is the dead entry: a census of this
 * host's Claude Code transcripts (see the hook header's dated snapshot) found
 * `Agent` used for every real subagent spawn and `Task` used zero times. So
 * the ONE load-bearing name in production was the one no test exercised, and
 * deleting `'Agent'` from the matcher left the whole suite green. Running the
 * matched-path scenarios against both names is what makes silently dropping
 * either one a red test.
 */
const SPAWN_TOOL_NAMES = ['Task', 'Agent'];

/** The default for helpers that don't care which matched name they use. */
const SPAWN_TOOL_NAME = SPAWN_TOOL_NAMES[0];

/** Tools that are unambiguously NOT the subagent-spawn path. */
const NON_SPAWN_TOOL_NAMES = ['Read', 'Edit', 'Grep', 'Bash'];

/**
 * The SUBSTRING-ADJACENT non-spawn tools: real tool names that share a prefix
 * with a matcher alternative but are NOT subagent spawns.
 *
 * `TaskOutput` (read a running agent's output; the canonical name that
 * `BashOutput` / `AgentOutput` alias to) and `TaskStop` (kill a running agent
 * or shell; the canonical name that `KillShell` / `KillBash` alias to) both
 * begin with `Task` — an alternative in the `"Agent|Task"` settings matcher.
 * If Claude Code compiled that matcher as an UNANCHORED regex, every call to
 * either would fire this hook: a Node child process, a stdin read and a JSON
 * parse on what the header comment above promises is a genuinely free hot
 * path.
 *
 * They are separated from `NON_SPAWN_TOOL_NAMES` rather than folded into it
 * because the risk they pin is different in kind: `Read` / `Bash` share no
 * prefix with anything in the matcher, so they can only ever reach the hook by
 * a gross misconfiguration. These two are the names an over-firing matcher
 * would ACTUALLY leak through, so they get their own explicitly-named block
 * below with the full zero-cost assertion set.
 *
 * The hook's own `SPAWN_TOOL_NAMES.has(toolName)` is an exact-match membership
 * test, so it short-circuits both regardless of what the matcher does — which
 * is precisely why this is the durable place to pin the claim. See
 * `settings-wiring.jest.spec.mjs`'s "MATCHER STRING FORMAT" section for the
 * matcher engine's verified behaviour and why the matcher is NOT anchored.
 */
const SUBSTRING_ADJACENT_TOOL_NAMES = ['TaskOutput', 'TaskStop'];

const SESSION_ID = '7c9a1f42-1b7e-4d2c-9f31-0a6b5c8e4d10';
const OTHER_SESSION_ID = 'de3f0b21-55aa-4c98-8e17-9d4c2a7f6b03';

/** The production paths that MUST stay untouched: every test drives the env seams instead. */
const REAL_HOME_COORDINATION_FILE = join(homedir(), '.claude/agent-state/resource-coordination.json');

let workDir;
let coordinationFilePath;
let livenessLogPath;
let cliInvocationLogPath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-gate-unit-'));
  coordinationFilePath = join(workDir, 'coordination.json');
  livenessLogPath = join(workDir, 'liveness.ndjson');
  cliInvocationLogPath = join(workDir, 'cli-invocations.log');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

function baseEnv(extra = {}) {
  return {
    ...process.env,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_LIVENESS_LOG_FILE: livenessLogPath,
    ...extra,
  };
}

/**
 * Spawns the REAL hook with an arbitrary stdin payload — `rawInput` lets the
 * malformed-JSON cases send bytes that `buildEvent` could never produce.
 */
function runHookRaw(rawInput, env = {}) {
  const result = spawnSync('node', [HOOK], { encoding: 'utf8', input: rawInput, env: baseEnv(env) });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/**
 * Builds a hook_event body. `session_id` / `tool_name` / `hook_event_name` are
 * passed through VERBATIM, including a deliberate `undefined` (which
 * `JSON.stringify` drops, producing a genuinely absent key rather than a null).
 */
const ABSENT = Symbol('absent');

function buildEvent({ sessionId = SESSION_ID, toolName = SPAWN_TOOL_NAME, hookEventName = 'PreToolUse' }) {
  const event = { tool_input: { description: 'spawn a subagent', prompt: 'do the thing' } };
  if (hookEventName !== ABSENT) event.hook_event_name = hookEventName;
  if (toolName !== ABSENT) event.tool_name = toolName;
  if (sessionId !== ABSENT) event.session_id = sessionId;
  return JSON.stringify(event);
}

/** The well-formed-event path. */
function runHook({ env = {}, ...eventFields } = {}) {
  return runHookRaw(buildEvent(eventFields), env);
}

/** Genuinely concurrent spawn (`spawnSync` in a `Promise.all` would still serialise). */
function runHookAsync({ env = {}, ...eventFields } = {}) {
  return new Promise((resolve) => {
    const child = spawn('node', [HOOK], { env: baseEnv(env) });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => {
      stdout += chunk;
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk;
    });
    child.on('close', (status) => resolve({ status, stdout, stderr }));
    child.stdin.end(buildEvent(eventFields));
  });
}

/**
 * Writes a stand-in `cli.mjs` that records one line per invocation (the
 * process-spawn COUNTER) and then behaves as `behaviour` dictates. Mirrors the
 * outer acceptance test's helper, with three extra behaviours this file needs.
 */
function writeStandInCli(behaviour, verdict = null) {
  const standInPath = join(workDir, `cli-stand-in-${behaviour}.mjs`);
  const body = {
    verdict: `process.stdout.write(${JSON.stringify(JSON.stringify(verdict))});\nprocess.exit(0);\n`,
    // Valid stdout, non-zero exit — J3.
    'verdict-but-nonzero-exit':
      `process.stdout.write(${JSON.stringify(JSON.stringify(verdict))});\nprocess.exit(3);\n`,
    // Valid verdict on stdout, unrelated chatter on stderr — must still parse.
    'verdict-with-stderr-noise':
      "process.stderr.write('warning: some unrelated diagnostic\\n');\n" +
      `process.stdout.write(${JSON.stringify(`\n${JSON.stringify(verdict)}\n`)});\nprocess.exit(0);\n`,
    crash: "process.stderr.write('stand-in cli.mjs exploded\\n');\nprocess.exit(1);\n",
    unparseable: "process.stdout.write('this is not json at all');\nprocess.exit(0);\n",
    silent: 'process.exit(0);\n',
    hang: 'setTimeout(() => process.exit(0), 60_000);\n',
  }[behaviour];

  if (!body) throw new Error(`writeStandInCli: unknown behaviour "${behaviour}"`);

  writeFileSync(
    standInPath,
    `import { appendFileSync } from 'node:fs';\n` +
      `appendFileSync(${JSON.stringify(cliInvocationLogPath)}, process.argv.slice(2).join(' ') + '\\n');\n` +
      body,
    'utf8',
  );
  chmodSync(standInPath, 0o755);
  return standInPath;
}

function cliInvocationLines() {
  if (!existsSync(cliInvocationLogPath)) return [];
  return readFileSync(cliInvocationLogPath, 'utf8').split('\n').filter(Boolean);
}

function cliInvocationCount() {
  return cliInvocationLines().length;
}

/** Seeds the sticky opted-in state through Phase 1's own primitive — never a hand-written fixture. */
async function seedOptedIn(sessionId) {
  const orchestratorId = deriveOrchestratorId(sessionId);
  await recordEverOptedIn(coordinationFilePath, orchestratorId);
  return orchestratorId;
}

async function failClosedEntries() {
  const entries = await readLivenessLog(livenessLogPath);
  return entries.filter((entry) => entry.detectionType === 'arm-gate-fail-closed');
}

/** Byte snapshot of the coordination file (J4) — `null` when it does not exist. */
function coordinationFileBytes() {
  return existsSync(coordinationFilePath) ? readFileSync(coordinationFilePath, 'utf8') : null;
}

/** A crash or unhandled rejection leaks a stack trace; an intentional deny never should. */
function expectNoStackTrace(stderr) {
  expect(stderr).not.toMatch(/TypeError|ReferenceError|Cannot read properties|\n\s+at\s|UnhandledPromiseRejection/);
}

describe('Phase 3 — PreToolUse ARM gate hook', () => {
  describe('cheapest path — the event is not a gated spawn at all', () => {
    // Each case below must allow WITHOUT reading the ledger and WITHOUT
    // shelling out. Non-creation of the coordination file is the black-box
    // proof of "zero reads": a hook that consulted the ledger for these events
    // would have to resolve the very path it must not touch.
    const malformedStdin = [
      ['empty stdin', ''],
      ['non-JSON bytes', 'not json at all'],
      ['truncated JSON', '{"hook_event_name":"PreToolUse","tool_name":"Task"'],
      ['JSON that is not an object', '"just a string"'],
      ['JSON null', 'null'],
      ['JSON array', '[]'],
    ];

    test.each(malformedStdin)('allows (exit 0) for %s, with zero cli.mjs spawns and no ledger file created', (_label, raw) => {
      const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });

      const { status, stderr } = runHookRaw(raw, { ARM_CLI_PATH: standIn });

      expect(status).toBe(0);
      expect(cliInvocationCount()).toBe(0);
      expect(existsSync(coordinationFilePath)).toBe(false);
      expectNoStackTrace(stderr);
    });

    test.each([
      ['a different hook_event_name', 'PostToolUse'],
      ['a missing hook_event_name', ABSENT],
      ['a non-string hook_event_name', 42],
    ])('allows for %s even when this session IS opted in, with zero cli.mjs spawns', async (_label, hookEventName) => {
      // Seeded opted-in + a `hold` stand-in: if the hook took the gated path at
      // all it would deny. Allowing with a zero spawn count can only mean it
      // short-circuited before consulting anything.
      await seedOptedIn(SESSION_ID);
      const before = coordinationFileBytes();
      const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });

      const { status, stderr } = runHook({ sessionId: SESSION_ID, hookEventName, env: { ARM_CLI_PATH: standIn } });

      expect(status).toBe(0);
      expect(cliInvocationCount()).toBe(0);
      expect(coordinationFileBytes()).toBe(before);
      expectNoStackTrace(stderr);
    });

    test.each(NON_SPAWN_TOOL_NAMES)(
      'allows tool_name "%s" (not a spawn tool) with zero cli.mjs spawns, even when opted in',
      async (toolName) => {
        await seedOptedIn(SESSION_ID);
        const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });

        const { status } = runHook({ sessionId: SESSION_ID, toolName, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(0);
        expect(cliInvocationCount()).toBe(0);
      },
    );

    // The over-fire guard. Everything above feeds the hook a tool name that
    // could only arrive by gross misconfiguration; these two are names the
    // SETTINGS MATCHER itself could plausibly leak through, so they carry the
    // full zero-cost assertion set rather than just the spawn count.
    test.each(SUBSTRING_ADJACENT_TOOL_NAMES)(
      'allows tool_name "%s" — a Task-PREFIXED tool that is not a spawn — at genuinely zero cost, even when opted in',
      async (toolName) => {
        // Opted in + a `hold` stand-in: reaching the gated path at all would
        // deny (exit 2). Allowing is therefore proof of the short-circuit, not
        // an accident of a permissive verdict.
        await seedOptedIn(SESSION_ID);
        const before = coordinationFileBytes();
        const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });

        const { status, stderr } = runHook({ sessionId: SESSION_ID, toolName, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(0);
        // Zero spawn: no `cli.mjs` child process on this path.
        expect(cliInvocationCount()).toBe(0);
        // Zero write: the ledger's bytes are untouched (J4).
        expect(coordinationFileBytes()).toBe(before);
        // Zero side-effect file: the liveness log is never even created.
        expect(existsSync(livenessLogPath)).toBe(false);
        expectNoStackTrace(stderr);
      },
    );

    test('never confuses a Task-prefixed tool with the Task spawn tool itself (the exact-match is real)', async () => {
      // Guards against a regression to `toolName.startsWith(...)` or a regex
      // membership test inside the hook: `Task` must deny where `TaskStop`
      // allows, from the identical seeded state. Asserting only the allow side
      // would pass vacuously against a hook that gates nothing at all.
      await seedOptedIn(SESSION_ID);
      const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });
      const env = { ARM_CLI_PATH: standIn };

      expect(runHook({ sessionId: SESSION_ID, toolName: 'TaskStop', env }).status).toBe(0);
      expect(cliInvocationCount()).toBe(0);

      expect(runHook({ sessionId: SESSION_ID, toolName: 'Task', env }).status).toBe(2);
      expect(cliInvocationCount()).toBe(1);
    });

    test.each([
      ['missing', ABSENT],
      ['non-string', 42],
      ['null', null],
      ['empty string', ''],
    ])('allows a %s tool_name without crashing and with zero cli.mjs spawns', async (_label, toolName) => {
      await seedOptedIn(SESSION_ID);
      const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });

      const { status, stderr } = runHook({ sessionId: SESSION_ID, toolName, env: { ARM_CLI_PATH: standIn } });

      expect(status).toBe(0);
      expect(cliInvocationCount()).toBe(0);
      expectNoStackTrace(stderr);
    });

    test('never creates the real ~/.claude/agent-state coordination file — the env seam is honoured', () => {
      const before = existsSync(REAL_HOME_COORDINATION_FILE)
        ? statSync(REAL_HOME_COORDINATION_FILE).mtimeMs
        : null;
      const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });

      // The status assertion keeps this test honest: without it, a hook that
      // never ran at all would satisfy the "untouched" claim vacuously.
      const { status } = runHook({ sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

      expect(status).toBe(0);
      const after = existsSync(REAL_HOME_COORDINATION_FILE) ? statSync(REAL_HOME_COORDINATION_FILE).mtimeMs : null;
      expect(after).toBe(before);
    });
  });

  // Every matched-path scenario below runs against BOTH names in
  // `SPAWN_TOOL_NAMES`. See that constant's note: pinning only one left the
  // production-load-bearing name (`Agent`) untested, so removing it from the
  // hook's matcher kept the whole suite green.
  describe.each(SPAWN_TOOL_NAMES)('matched spawn tool_name "%s"', (spawnToolName) => {
    describe('not opted in — fail open, zero cost (design decision 3, tier 1)', () => {
      test('allows a matched spawn tool with zero cli.mjs spawns when the ledger has no ever-opted-in row', async () => {
        // A ledger that exists and is perfectly readable, but records a DIFFERENT
        // session. Distinguishes "read it and correctly found nothing" from
        // "never looked" — both allow, and both must cost zero spawns.
        await seedOptedIn(OTHER_SESSION_ID);
        const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });

        const { status, stderr } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(0);
        expect(cliInvocationCount()).toBe(0);
        expect(stderr).toBe('');
      });

      test('allows when the ledger file is corrupt and this session was never opted in', () => {
        writeFileSync(coordinationFilePath, '{ this is not valid json', 'utf8');
        const standIn = writeStandInCli('crash');

        const { status, stderr } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(0);
        expect(cliInvocationCount()).toBe(0);
        expectNoStackTrace(stderr);
      });

      test('writes no liveness-log entry at all on the not-opted-in path', async () => {
        const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });

        const { status } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(0);
        expect(cliInvocationCount()).toBe(0);
        expect(await readLivenessLog(livenessLogPath)).toEqual([]);
      });
    });

    describe('J1 — an underivable session identity fails OPEN, never closed and never a crash', () => {
      // design decision 3 tiers on whether a rule has been ESTABLISHED for the
      // session. `deriveOrchestratorId` throws (rather than returning a shared
      // fallback) for every input below, so no sticky state can exist for it —
      // there is nothing to protect, and denying would block a session we cannot
      // even name. This is the not-opted-in tier, not the opted-in one.
      const underivable = [
        ['missing session_id', ABSENT],
        ['null session_id', null],
        ['non-string session_id', 42],
        ['empty session_id', ''],
        ['blank session_id', '   '],
        ['leading-whitespace-padded session_id', ` ${SESSION_ID}`],
        ['trailing-whitespace-padded session_id', `${SESSION_ID}\n`],
      ];

      test.each(underivable)('allows (exit 0) for a %s, with zero cli.mjs spawns and no crash', (_label, sessionId) => {
        const standIn = writeStandInCli('crash');

        const { status, stderr } = runHook({ toolName: spawnToolName, sessionId, env: { ARM_CLI_PATH: standIn } });

        // Not exit 1 (crash), not exit 2 (deny) — exit 0.
        expect(status).toBe(0);
        expect(cliInvocationCount()).toBe(0);
        expectNoStackTrace(stderr);
      });

      test('a padded session_id does NOT inherit the unpadded session\'s sticky opted-in state', async () => {
        // The state-leak guard. `session-<id>` for the padded form is underivable,
        // so the hook must not fall back to the unpadded id (or to any shared
        // placeholder) and pick up its gate — nor deny on its behalf.
        await seedOptedIn(SESSION_ID);
        const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });

        const { status } = runHook({ toolName: spawnToolName, sessionId: `  ${SESSION_ID}  `, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(0);
        expect(cliInvocationCount()).toBe(0);
      });

      test('records no fail-closed liveness entry for an underivable session identity', async () => {
        await seedOptedIn(SESSION_ID);
        const standIn = writeStandInCli('crash');

        const { status } = runHook({ toolName: spawnToolName, sessionId: '   ', env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(0);
        expect(await failClosedEntries()).toEqual([]);
      });
    });

    describe('opted in — the hook consults cli.mjs and honours its verdict', () => {
      test('passes the derived orchestrator-id and --advise-only, and never a mutating beat flag', async () => {
        const orchestratorId = await seedOptedIn(SESSION_ID);
        const before = coordinationFileBytes();
        const standIn = writeStandInCli('verdict', { type: 'spawn-allowed', allowance: 1 });

        const { status } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(0);
        expect(cliInvocationCount()).toBe(1);

        const args = cliInvocationLines()[0].split(' ');
        expect(args).toContain(`--orchestrator-id=${orchestratorId}`);
        // The consultation must use the NON-MUTATING advisory mode. Passing
        // `--desired-agents=N` here would make the consultation a full ARM
        // beat: it would re-declare dibs (overwriting the calling session's
        // own declared `desiredAgents`), draw a global spawn-rate token for a
        // spawn it is not performing, and re-size this session's live-agent
        // admission hold. Both halves are asserted — the flag that must be
        // present, and the family of flags that must not.
        expect(args).toContain('--advise-only');
        expect(args.some((arg) => arg.startsWith('--desired-agents'))).toBe(false);
        // The gate check is a consultation, not a state mutation of its own.
        expect(args.some((arg) => arg.startsWith('--release') || arg.startsWith('--record-outcome'))).toBe(false);
        // J4 — the hook is a pure reader of the coordination file.
        expect(coordinationFileBytes()).toBe(before);
      });

      // The only test in this file that drives the REAL `cli.mjs` rather than
      // a stand-in — because the mutation this pins is the CHILD's, and a
      // stand-in by construction has none. J4's byte-equality claim is a claim
      // about NET effect (hook + every process it spawns), not about the
      // hook's own `fs` calls; asserted here against the real binary so it
      // cannot silently become false again.
      test('a REAL cli.mjs consultation leaves the ledger byte-identical, including this session\'s declared desiredAgents', async () => {
        const orchestratorId = deriveOrchestratorId(SESSION_ID);
        const cliEnv = baseEnv({ ARM_FAKE_COLLECT_JSON: COLLECT_GREEN });

        // Opt in through a REAL beat declaring FOUR agents, so a clobber shows
        // up as a value change rather than only a timestamp refresh.
        const optIn = spawnSync('node', [REAL_CLI, `--orchestrator-id=${orchestratorId}`, '--desired-agents=4'], {
          encoding: 'utf8',
          env: cliEnv,
        });
        expect(optIn.status).toBe(0);

        const sessionRow = () =>
          JSON.parse(readFileSync(coordinationFilePath, 'utf8')).find(
            (entry) => entry?.orchestratorId === orchestratorId,
          );
        // Precondition, so neither assertion below can pass vacuously.
        expect(sessionRow().desiredAgents).toBe(4);
        const before = coordinationFileBytes();

        const { status } = runHook({
          toolName: spawnToolName,
          sessionId: SESSION_ID,
          env: { ARM_FAKE_COLLECT_JSON: COLLECT_GREEN },
        });

        expect(status).toBe(0);
        expect(sessionRow().desiredAgents).toBe(4);
        expect(coordinationFileBytes()).toBe(before);
      });

      test('allows on a spawn-allowed verdict and writes no liveness-log entry', async () => {
        await seedOptedIn(SESSION_ID);
        const standIn = writeStandInCli('verdict', { type: 'spawn-allowed', allowance: 1 });

        const { status } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(0);
        expect(await readLivenessLog(livenessLogPath)).toEqual([]);
      });

      test('tolerates surrounding whitespace and unrelated stderr chatter around a valid verdict', async () => {
        await seedOptedIn(SESSION_ID);
        const standIn = writeStandInCli('verdict-with-stderr-noise', { type: 'spawn-allowed', allowance: 1 });

        const { status } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(0);
        expect(cliInvocationCount()).toBe(1);
      });

      // J2 — a real verdict that is not `spawn-allowed` is a POLICY deny.
      test.each([
        ['hold', { type: 'hold', allowance: 0 }],
        ['pause', { type: 'pause', agentId: 'agent-7' }],
        ['some-future-verdict', { type: 'some-future-verdict' }],
      ])('denies (exit 2) on a "%s" verdict, naming it on stderr', async (verdictType, verdict) => {
        await seedOptedIn(SESSION_ID);
        const standIn = writeStandInCli('verdict', verdict);

        const { status, stderr } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(2);
        expect(stderr).toMatch(new RegExp(verdictType));
        expect(stderr).toMatch(/agent-resource-management/i);
      });

      test('a policy deny is NOT recorded as a fail-closed trip (keeps the fail-closed rate auditable)', async () => {
        await seedOptedIn(SESSION_ID);
        const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });

        const { status } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(2);
        expect(await failClosedEntries()).toEqual([]);
      });
    });

    describe('opted in — fail CLOSED when cli.mjs gives no usable answer (design decision 3, tier 2)', () => {
      const unusable = [
        ['crashes (non-zero exit)', 'crash', null],
        ['returns unparseable stdout', 'unparseable', null],
        ['returns empty stdout', 'silent', null],
        ['returns valid JSON that is not a verdict object', 'verdict', 'just a string'],
        ['returns a verdict object with no type', 'verdict', { allowance: 1 }],
        ['returns JSON null', 'verdict', null],
        ['exits non-zero despite a spawn-allowed stdout', 'verdict-but-nonzero-exit', { type: 'spawn-allowed' }],
      ];

      test.each(unusable)('denies (exit 2) when cli.mjs %s', async (_label, behaviour, verdict) => {
        await seedOptedIn(SESSION_ID);
        const standIn = writeStandInCli(behaviour, verdict);

        const { status, stderr } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(2);
        expect(cliInvocationCount()).toBe(1);
        // Actionable, by design's load-bearing mitigation.
        expect(stderr).toMatch(/agent-resource-management/i);
        expect(stderr).toMatch(/fail(ing|ed)?[- ]closed/i);
        expect(stderr).toMatch(/cli\.mjs/);
      });

      test.each(unusable)('records exactly one arm-gate-fail-closed liveness entry when cli.mjs %s', async (_label, behaviour, verdict) => {
        const orchestratorId = await seedOptedIn(SESSION_ID);
        const standIn = writeStandInCli(behaviour, verdict);

        runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        const entries = await failClosedEntries();
        expect(entries).toHaveLength(1);
        // Auditable means attributable: the trip must name the session it denied.
        expect(entries[0].orchestratorId).toBe(orchestratorId);
        expect(entries[0].schemaVersion).toBe(1);
        expect(typeof entries[0].timestamp).toBe('number');
        expect(typeof entries[0].outcome).toBe('string');
        expect(entries[0].outcome.length).toBeGreaterThan(0);
      });

      test('denies for an opted-in session when ARM_CLI_PATH points at a file that does not exist', async () => {
        await seedOptedIn(SESSION_ID);
        const missing = join(workDir, 'does-not-exist.mjs');

        const { status, stderr } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: missing } });

        expect(status).toBe(2);
        expect(stderr).toMatch(/fail(ing|ed)?[- ]closed/i);
        expect(await failClosedEntries()).toHaveLength(1);
      });

      test(
        'denies within the 5s cli.mjs timeout budget when cli.mjs hangs, naming the timeout',
        async () => {
          await seedOptedIn(SESSION_ID);
          const standIn = writeStandInCli('hang');

          const startedAt = Date.now();
          const { status, stderr } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });
          const elapsedMs = Date.now() - startedAt;

          expect(status).toBe(2);
          expect(stderr).toMatch(/fail(ing|ed)?[- ]closed/i);
          // The message must distinguish a hang from a crash — "go fix the hook"
          // is only actionable if a human can tell which failure they hit.
          expect(stderr).toMatch(/time(d)?[- ]?out|timeout/i);
          expect(elapsedMs).toBeLessThan(15_000);
          expect(await failClosedEntries()).toHaveLength(1);
        },
        30_000,
      );

      test('stays gated across repeated calls — every call is consulted, not just the first', async () => {
        await seedOptedIn(SESSION_ID);
        const standIn = writeStandInCli('crash');

        for (let i = 0; i < 3; i += 1) {
          expect(runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } }).status).toBe(2);
        }

        expect(cliInvocationCount()).toBe(3);
        expect(await failClosedEntries()).toHaveLength(3);
      });
    });

    describe('the liveness-log append is best-effort — it never flips or blocks the decision', () => {
      /** An unwritable log path: the parent directory does not exist. */
      function unwritableLogPath() {
        return join(workDir, 'no-such-dir', 'nested', 'liveness.ndjson');
      }

      test('still DENIES when the liveness-log write itself fails', async () => {
        await seedOptedIn(SESSION_ID);
        const standIn = writeStandInCli('crash');

        const { status, stderr } = runHook({
          toolName: spawnToolName,
          sessionId: SESSION_ID,
          env: { ARM_CLI_PATH: standIn, ARM_LIVENESS_LOG_FILE: unwritableLogPath() },
        });

        expect(status).toBe(2);
        expect(stderr).toMatch(/fail(ing|ed)?[- ]closed/i);
        expectNoStackTrace(stderr);
      });

      test('still ALLOWS when the liveness-log path is unwritable and the verdict is spawn-allowed', async () => {
        await seedOptedIn(SESSION_ID);
        const standIn = writeStandInCli('verdict', { type: 'spawn-allowed', allowance: 1 });

        const { status, stderr } = runHook({
          toolName: spawnToolName,
          sessionId: SESSION_ID,
          env: { ARM_CLI_PATH: standIn, ARM_LIVENESS_LOG_FILE: unwritableLogPath() },
        });

        expect(status).toBe(0);
        expectNoStackTrace(stderr);
      });

      // J5 — Convergence Analysis edge case 5.
      test(
        'does not stack the liveness-log lock wait onto the decision when the log lock is held',
        async () => {
          await seedOptedIn(SESSION_ID);
          // A freshly-dated lock file: not stale, so `acquireLock` will retry to
          // its ceiling (2000 x 5ms ~= 10s) unless the hook bounds its own wait.
          writeFileSync(`${livenessLogPath}.lock`, JSON.stringify({ token: 'someone-else', acquiredAt: Date.now() }), 'utf8');
          const standIn = writeStandInCli('crash');

          const startedAt = Date.now();
          const { status } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });
          const elapsedMs = Date.now() - startedAt;

          expect(status).toBe(2);
          expect(elapsedMs).toBeLessThan(8_000);
        },
        30_000,
      );
    });

    describe('concurrency — parallel spawn calls from one session', () => {
      test('N parallel hook processes each resolve independently without deadlocking', async () => {
        await seedOptedIn(SESSION_ID);
        const standIn = writeStandInCli('verdict', { type: 'spawn-allowed', allowance: 1 });

        const startedAt = Date.now();
        const results = await Promise.all(
          Array.from({ length: 5 }, () => runHookAsync({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } })),
        );
        const elapsedMs = Date.now() - startedAt;

        expect(results.map((result) => result.status)).toEqual([0, 0, 0, 0, 0]);
        expect(cliInvocationCount()).toBe(5);
        expect(elapsedMs).toBeLessThan(20_000);
      }, 30_000);
    });
  });
});
