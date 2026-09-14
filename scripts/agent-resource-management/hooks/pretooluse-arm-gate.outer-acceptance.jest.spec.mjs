// Phase 0 outer acceptance test ("agent-resource-management:
// make the PreToolUse gate opt-in-but-sticky — enforced once a session's
// ledger entry exists, zero-cost otherwise"), by design.
//
// Outer acceptance test:
//
//   it('a session that opts into ARM stays gated for the rest of the session
//   even after its live dibs entry is pruned or released, while a session that
//   never opts in pays zero ARM cost')
//
//   Layer: integration, hermetic. Every assertion below drives the REAL hook
//   script as a real child process (`spawnSync(node, [HOOK])`, hook_event JSON
//   on stdin, exit code read back), against a REAL coordination file on a tmp
//   path via the already-shipped `ARM_COORDINATION_FILE` test seam
//   (cli.mjs:1315), and — for the opt-in step and the "verdict fidelity"
//   assertions — a REAL `cli.mjs` invocation. No AWS/Amplify boundary, no
//   mocked module graph, no real host sampling (the existing
//   `ARM_FAKE_COLLECT_JSON` seam injects the memory/disk sample), and no real
//   `.claude/agent-state/` file is ever touched.
//
// RED BY CONSTRUCTION at the time of writing. None of the following exist yet:
//   - `./pretooluse-arm-gate.mjs`                (Phase 3 — hook rewrite)
//   - `../lib/orchestrator-id.mjs`               (Phase 2 — deriveOrchestratorId)
//   - `hasEverOptedIn` + the reserved ever-opted-in entry in
//     `../lib/coordination-file.mjs`             (Phase 1 — sticky primitive)
// The uncommitted draft at `hooks/pretooluse-arm-gate.mjs` in the main
// checkout implements the SUPERSEDED blanket-mandatory design and is NOT the
// contract asserted here — Phase 3 rewrites it in place.
//
// ---------------------------------------------------------------------------
// CONTRACT THIS TEST PINS (for the Phase 1-3 implementers to match)
//
//   HOOK PATH
//     `scripts/agent-resource-management/hooks/pretooluse-arm-gate.mjs`
//     (same path as the superseded draft, rewritten in place).
//
//   HOOK PROTOCOL (Claude Code PreToolUse — see code.claude.com/docs/en/hooks)
//     stdin: the hook_event JSON, `{ hook_event_name: 'PreToolUse', tool_name,
//     session_id, ... }`. Exit 0 (or any non-2 code) allows the tool call;
//     exit 2 blocks it and surfaces stderr as the shown reason.
//
//   LOOKUP KEY — `deriveOrchestratorId(sessionId)`, exported from
//     `../lib/orchestrator-id.mjs` (Phase 2). This test IMPORTS that helper
//     rather than hard-coding a prefix, on purpose: Convergence Analysis edge
//     case 1 ("prefix mismatch is real, not hypothetical" — the draft derives
//     `hook-${sessionId}`, /Phase 2 mandates `session-<sessionId>`)
//     is only catchable by a test where the id the orchestrator declares and
//     the id the hook derives come from the SAME source of truth. A fixture
//     that hard-coded either prefix consistently would pass while the gate
//     silently never fires — the `worktreeMtimeMs`-always-null shape.
//
//   OPT-IN — a session becomes opted in the moment a real, skill-driven
//     `cli.mjs --session-id="$CLAUDE_CODE_SESSION_ID" --desired-agents=N` call
//     succeeds. This test performs exactly that call (never a hand-crafted
//     ledger fixture), which is the project owner's explicit hard AC on.
//
//     STRENGTHENED IN PHASE 6, and the reason matters. This file originally
//     called `deriveOrchestratorId(SESSION_ID)` in the HARNESS and handed the
//     result to `--orchestrator-id=`. That satisfied the AC's letter — a real
//     `cli.mjs` child process really did run — but exercised an acquisition
//     path no orchestrator had: nothing on the producer side could run that
//     helper, SKILL.md described the derivation only in prose, so every real
//     session passed a hand-picked literal, `hasEverOptedIn(<hook's derived
//     id>)` was `false` forever, and the gate fell through its tier-1
//     fail-open path on every gated spawn. A green suite over a permanently
//     dead gate is the shape this ticket exists to prevent, so the
//     harness now obtains the id the only way a real caller can: by handing
//     `cli.mjs` a session id and letting IT derive.
//
//   STICKINESS — "has this session EVER declared dibs", not "does it have a
//     live entry right now". Phase 1's reserved, never-pruned entry
//     (`isReservedEntry`-shaped, `__…__`) carries this; the hook reads it with
//     a plain file read, never a `cli.mjs` shell-out.
//
//   NEW TEST SEAM REQUIRED OF PHASE 3 — `ARM_CLI_PATH` (env): absolute path
//     to the `cli.mjs` the hook consults, defaulting to the real repo-relative
//     one when unset. Mirrors `ARM_COORDINATION_FILE`/`ARM_LIVENESS_LOG_FILE`'s
//     own naming and precedence convention exactly. This seam is what makes
//     two otherwise-untestable requirements real rather than inferred:
//       (a) the ZERO-COST claim is asserted as a genuine process-spawn COUNT
//           (a recording stand-in appends one line per invocation; zero lines
//           == zero spawns), not inferred from timing or from "it allowed, so
//           it probably didn't shell out";
//       (b) the FAIL-CLOSED path is exercised against a genuinely broken and a
//           genuinely hanging real child process, not a mocked verdict object.
//     The hook cannot be instrumented via PATH because it spawns
//     `process.execPath` (an absolute path), so PATH-shimming `node` is not
//     an option — the seam is necessary, not merely convenient.
//
//   FAIL-CLOSED LOGGING — every fail-closed trip appends one
//     `detectionType: 'arm-gate-fail-closed'` entry to the existing append-only
//     NDJSON liveness log (`../lib/liveness-log.mjs`), honouring the existing
//     `ARM_LIVENESS_LOG_FILE` seam. Per that module's own contract the write is
//     best-effort and must never flip the allow/deny decision.
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync, chmodSync } from 'node:fs';
import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { isReservedEntry } from '../lib/coordination-file.mjs';
import { readLivenessLog } from '../lib/liveness-log.mjs';
// Phase 2 — does not exist yet. This import is one of the two expected RED
// failure sources for this file (the other is the hook script itself).
import { deriveOrchestratorId } from '../lib/orchestrator-id.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const HOOK = join(HERE, 'pretooluse-arm-gate.mjs');
const REAL_CLI = join(HERE, '..', 'cli.mjs');

// A GREEN host sample -> the real cli.mjs returns `{ type: 'spawn-allowed' }`;
// an AMBER one -> `{ type: 'hold' }`. Mirrors cli.jest.spec.mjs's own fixtures
// (pressureLevel deliberately held at 1 so classification is driven purely by
// the swap thresholds and never short-circuited by the pressure pre-check).
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 };
const MEMORY_AMBER = { pressureLevel: 1, swapUsedMb: 6144, compressedMb: 2048 };

const COLLECT_GREEN = JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN });
const COLLECT_AMBER = JSON.stringify({ memory: MEMORY_AMBER, disk: DISK_GREEN });

// A realistic Claude Code session id (uuid-shaped, with the hyphens the
// sanitizer has to cope with).
const SESSION_ID = '7c9a1f42-1b7e-4d2c-9f31-0a6b5c8e4d10';
const OTHER_SESSION_ID = 'de3f0b21-55aa-4c98-8e17-9d4c2a7f6b03';

/**
 * Every subagent-spawn tool name the hook's `SPAWN_TOOL_NAMES` matcher claims
 * to gate. These, the hook's matcher set, and the `.claude/settings.json`
 * entry must always name the same tools.
 *
 * BOTH are exercised, symmetrically, by the `describe.each` below. This file
 * originally pinned `'Task'` alone — and a census of this host's Claude Code
 * transcripts (see the hook header's dated snapshot) found `Agent` used for
 * every real subagent spawn and `Task` used zero times. The one name that
 * actually fires in production was therefore the one no test covered, so
 * deleting `'Agent'` from the matcher left the entire suite green.
 */
const SPAWN_TOOL_NAMES = ['Task', 'Agent'];

/** The default for helpers that don't care which matched name they use. */
const SPAWN_TOOL_NAME = SPAWN_TOOL_NAMES[0];

let workDir;
let coordinationFilePath;
let livenessLogPath;
let cliInvocationLogPath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-gate-outer-'));
  coordinationFilePath = join(workDir, 'coordination.json');
  livenessLogPath = join(workDir, 'liveness.ndjson');
  cliInvocationLogPath = join(workDir, 'cli-invocations.log');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

/** Base env every child in this file gets — nothing here touches the real host. */
function baseEnv(extra = {}) {
  return {
    ...process.env,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_LIVENESS_LOG_FILE: livenessLogPath,
    ARM_FAKE_COLLECT_JSON: COLLECT_GREEN,
    ...extra,
  };
}

/** Runs the REAL hook script as a real child process, hook_event JSON on stdin. */
function runHook({ sessionId, toolName = SPAWN_TOOL_NAME, env = {} } = {}) {
  const event = {
    hook_event_name: 'PreToolUse',
    tool_name: toolName,
    session_id: sessionId,
    tool_input: { description: 'spawn a subagent', prompt: 'do the thing' },
  };
  const result = spawnSync('node', [HOOK], {
    encoding: 'utf8',
    input: JSON.stringify(event),
    env: baseEnv(env),
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/**
 * Runs the REAL cli.mjs as a real child process, via the EXACT invocation
 * SKILL.md tells an orchestrator to copy — including the shell expansion.
 *
 * This is the acquisition path under test, not a convenience wrapper. The
 * earlier version of this file called `deriveOrchestratorId(SESSION_ID)` in
 * the harness and handed the result to `--orchestrator-id=`, which satisfied
 * design decision 2's letter ("verified against a real skill-driven
 * `cli.mjs` call") while exercising a path no real orchestrator has: nothing
 * on the producer side could run that helper, so every real session kept
 * passing a hand-picked literal and the gate silently never fired. Only a test
 * that obtains the id the way the docs say to obtain it can catch that.
 *
 * `shell: true` is deliberate: `$CLAUDE_CODE_SESSION_ID` must be expanded by
 * the shell from the CHILD's environment, exactly as it is when a human or an
 * LLM orchestrator pastes the documented command. Passing the uuid as a
 * pre-expanded argv element would skip the half of the mechanism most likely
 * to be wrong.
 *
 * @param {string} sessionId placed in the child's `CLAUDE_CODE_SESSION_ID`.
 * @param {string[]} extraArgs appended verbatim (already shell-safe literals).
 */
function runRealCli(sessionId, env = {}, extraArgs = ['--desired-agents=1']) {
  const command = [
    'node',
    JSON.stringify(REAL_CLI),
    '--session-id="$CLAUDE_CODE_SESSION_ID"',
    ...extraArgs,
  ].join(' ');
  const result = spawnSync(command, {
    shell: true,
    encoding: 'utf8',
    env: baseEnv({ ...env, CLAUDE_CODE_SESSION_ID: sessionId }),
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/**
 * The orchestrator-id the ledger actually ended up keyed by, read back from
 * disk rather than recomputed.
 *
 * Every assertion that both sides "agree on the id" is strongest when neither
 * side of the comparison is the harness's own call to the shared helper: this
 * returns what the PRODUCER (a real, shell-driven `cli.mjs --session-id=…`)
 * wrote, so comparing it against what the hook passes to `cli.mjs` compares
 * two independently-produced observations.
 */
async function ledgerOrchestratorIdAfterOptIn() {
  const entries = await liveEntries();
  const ids = entries.map((entry) => entry.orchestratorId).filter((id) => id.startsWith('session-'));
  expect(ids).toHaveLength(1);
  return ids[0];
}

/**
 * Writes a stand-in `cli.mjs` that appends one line per invocation to
 * `cliInvocationLogPath` — the process-spawn COUNTER — and then behaves as
 * `behaviour` dictates. `verdict` stand-ins are used where the concern is
 * "did the hook consult at all / did it honour the verdict"; the `crash` and
 * `hang` stand-ins are genuinely broken/slow real processes, not mocks.
 */
function writeStandInCli(behaviour, verdict = null) {
  const standInPath = join(workDir, 'cli-stand-in.mjs');
  const body = {
    verdict: `process.stdout.write(${JSON.stringify(JSON.stringify(verdict))});\nprocess.exit(0);\n`,
    crash: "process.stderr.write('stand-in cli.mjs exploded\\n');\nprocess.exit(1);\n",
    unparseable: "process.stdout.write('this is not json at all');\nprocess.exit(0);\n",
    // Longer than the hook's documented 5s timeout, and long enough that a
    // hook which failed to time out would fail the test rather than pass slowly.
    hang: 'setTimeout(() => process.exit(0), 60_000);\n',
  }[behaviour];

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

/** How many times the stand-in cli.mjs was actually spawned. */
function cliInvocationCount() {
  if (!existsSync(cliInvocationLogPath)) return 0;
  return readFileSync(cliInvocationLogPath, 'utf8').split('\n').filter(Boolean).length;
}

/** The `--orchestrator-id=…` argv each recorded invocation carried. */
function recordedOrchestratorIds() {
  if (!existsSync(cliInvocationLogPath)) return [];
  return readFileSync(cliInvocationLogPath, 'utf8')
    .split('\n')
    .filter(Boolean)
    .flatMap((line) => line.split(' ').filter((arg) => arg.startsWith('--orchestrator-id=')))
    .map((arg) => arg.slice('--orchestrator-id='.length));
}

/**
 * Simulates the ledger losing this session's LIVE entry mid-session — the
 * union of the two ways says it can happen (`pruneStale` expiry and
 * an explicit `--release`), by dropping every non-reserved row. Reserved rows
 * (`isReservedEntry`, which is what Phase 1's ever-opted-in entry must be) are
 * left untouched, exactly as `pruneStale` and `--release` leave them.
 */
async function dropAllLiveEntries() {
  const raw = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
  const survivors = raw.filter((entry) => isReservedEntry(entry?.orchestratorId));
  await writeFile(coordinationFilePath, JSON.stringify(survivors, null, 2), 'utf8');
  return survivors;
}

/** The non-reserved (live-dibs) rows currently on disk. */
async function liveEntries() {
  if (!existsSync(coordinationFilePath)) return [];
  const raw = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
  return raw.filter((entry) => !isReservedEntry(entry?.orchestratorId));
}

/** Byte snapshot of the coordination file — `null` when it does not exist. */
function coordinationFileBytes() {
  return existsSync(coordinationFilePath) ? readFileSync(coordinationFilePath, 'utf8') : null;
}

/**
 * The specific rows a mutating consultation was empirically shown to corrupt,
 * pulled out by id so a failure names WHICH invariant broke rather than just
 * "the bytes differ".
 *
 * `session` is this orchestrator's own dibs row (its `desiredAgents` was being
 * silently overwritten to the consultation's own fabricated ask);
 * `globalSpawnRateBucket` is the host-wide rate limiter (a token was being
 * drawn a second time for a single spawn); `aimdCeiling` is the shared
 * additive-increase/multiplicative-decrease ceiling state (its
 * `sustainedNormalCount` was being advanced from a read-only code path); and
 * `liveAgentAdmission` is the live-agent admission ledger (this session's hold
 * was being re-sized down to the consultation's ask, with nothing to release
 * it).
 */
async function ledgerRows(orchestratorId) {
  if (!existsSync(coordinationFilePath)) return null;
  const raw = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
  const byId = (id) => raw.find((entry) => entry?.orchestratorId === id) ?? null;
  return {
    session: byId(orchestratorId),
    globalSpawnRateBucket: byId('__global-spawn-rate-bucket__'),
    aimdCeiling: byId('__aimd-ceiling-state__'),
    liveAgentAdmission: byId('__claim:live-agent-admission__'),
    sharedSample: byId('__shared-machine-sample__'),
    everOptedIn: byId('__ever-opted-in__'),
  };
}

describe('outer acceptance — the ARM PreToolUse gate is opt-in but sticky', () => {
  describe('tool names outside the matcher are not gated at all', () => {
    test('allows an UNMATCHED tool call with zero cli.mjs spawns and no ledger write', async () => {
      const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });

      const { status } = runHook({
        sessionId: SESSION_ID,
        toolName: 'Read',
        env: { ARM_CLI_PATH: standIn },
      });

      expect(status).toBe(0);
      expect(cliInvocationCount()).toBe(0);
      // Cheapest possible path: a non-spawn tool must not even cause the
      // coordination file to be created.
      expect(existsSync(coordinationFilePath)).toBe(false);
    });
  });

  // Every scenario below is a MATCHED-spawn-tool scenario, and each runs
  // against BOTH names in `SPAWN_TOOL_NAMES`. See that constant's note: this
  // file (and the unit suite) previously hard-coded `Task` alone, which is the
  // name production never uses — so deleting the load-bearing `Agent` entry
  // from the hook's matcher left every test green.
  describe.each(SPAWN_TOOL_NAMES)('matched spawn tool_name "%s"', (spawnToolName) => {
    describe('tier 1 — a session that never opts in pays zero ARM cost', () => {
      test('allows a MATCHED spawn tool call with ZERO cli.mjs child processes spawned', () => {
        // The stand-in would return `hold` (i.e. DENY) if it were ever consulted,
        // so an `allow` here cannot be a false pass from a permissive verdict —
        // it can only mean the hook never shelled out at all. The spawn COUNT
        // assertion below proves that directly rather than inferring it.
        const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });

        const { status, stderr } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(0);
        expect(cliInvocationCount()).toBe(0);
        expect(stderr).toBe('');
      });

      test('fails OPEN (allows) for a never-opted-in session even when the ledger file is corrupt', () => {
        // design decision 3, not-opted-in tier: no rule has been established
        // for this session, so an unreadable ledger must still allow.
        writeFileSync(coordinationFilePath, '{ this is not valid json', 'utf8');
        const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });

        const { status } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(0);
        expect(cliInvocationCount()).toBe(0);
      });
    });

    describe('tier 2 — a session with a LIVE entry is gated, matching the real cli.mjs verdict', () => {
      test('opting in via a REAL skill-driven cli.mjs call makes the hook consult, and it honours a real spawn-allowed verdict', async () => {
        // The opt-in: a real, skill-driven beat, invoked exactly as SKILL.md
        // documents it — never a hand-crafted fixture, and never a harness-side
        // call to `deriveOrchestratorId`.
        const optIn = runRealCli(SESSION_ID);
        expect(optIn.status).toBe(0);
        expect(JSON.parse(optIn.stdout).type).toBe('spawn-allowed');

        // The lookup actually fires: the id the PRODUCER wrote (read back off
        // disk) is the id the CONSUMER's independent derivation produces.
        // (Guards the prefix-mismatch gap, and now also the gap where the
        // producer had no derivation mechanism at all.)
        const orchestratorId = await ledgerOrchestratorIdAfterOptIn();
        expect(orchestratorId).toBe(deriveOrchestratorId(SESSION_ID));

        const rowsBefore = await ledgerRows(orchestratorId);
        const bytesBefore = coordinationFileBytes();

        const { status, stderr } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_FAKE_COLLECT_JSON: COLLECT_GREEN } });

        expect(status).toBe(0);
        expect(stderr).toBe('');

        // The consultation is a QUERY, not a beat. Asserting the verdict alone
        // (as this test originally did) is exactly what let a full mutating
        // beat hide behind an `allow`.
        expect(await ledgerRows(orchestratorId)).toEqual(rowsBefore);
        expect(coordinationFileBytes()).toBe(bytesBefore);
      });

      test("denies when the REAL cli.mjs's verdict for this exact state is not spawn-allowed", async () => {
        expect(runRealCli(SESSION_ID).status).toBe(0);
        const orchestratorId = await ledgerOrchestratorIdAfterOptIn();

        // Independently obtain the real verdict for an AMBER host, then assert
        // the hook's decision is the same decision — behaviour parity with the
        // real policy-decision point, not a re-implementation of it.
        const verdict = JSON.parse(runRealCli(SESSION_ID, { ARM_FAKE_COLLECT_JSON: COLLECT_AMBER }).stdout);
        expect(verdict.type).not.toBe('spawn-allowed');

        const rowsBefore = await ledgerRows(orchestratorId);
        const bytesBefore = coordinationFileBytes();

        const { status, stderr } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_FAKE_COLLECT_JSON: COLLECT_AMBER } });

        expect(status).toBe(2);
        expect(stderr).toMatch(new RegExp(verdict.type));

        // A DENYING consultation must be just as non-mutating as an allowing
        // one — this path still runs the full classification pipeline.
        expect(await ledgerRows(orchestratorId)).toEqual(rowsBefore);
        expect(coordinationFileBytes()).toBe(bytesBefore);
      });

      // The reviewer's own repro, promoted to a regression test. Before
      // `--advise-only`, the hook consulted `cli.mjs --desired-agents=1`, which
      // is a full ARM beat: it rewrote this session's declared
      // `desiredAgents: 4` to `1`, drew a second global spawn-rate token for a
      // single spawn, advanced the shared AIMD `sustainedNormalCount`, and
      // re-sized this session's live-agent admission hold from 4 to 1 — all
      // while the hook's own `fs` calls stayed read-only, so nothing in either
      // suite noticed.
      //
      // PARAMETERISED OVER THE AIMD ROW'S THREE STATES, and that is the whole
      // point of the `describe.each` below rather than a nicety. The original
      // single-case version asserted `rowsBefore.aimdCeiling).not.toBeNull()`
      // as a PRECONDITION, so its fixture always pre-seeded
      // `__aimd-ceiling-state__` via the real opt-in beat — which meant the
      // byte-identity assertion could only ever exercise
      // `readAimdCeilingState`'s well-formed-row branch. The second review
      // round then found the mutation on exactly the two branches this test
      // structurally could not reach: an ABSENT row and a MALFORMED one both
      // made the "read-only" advisory run seed the row under the shared lock.
      // Byte-identity has to hold in all three states or it is not a property
      // of the code, only of the fixture.
      describe.each([
        [
          'present and well-formed',
          async () => {
            const rows = await ledgerRows(deriveOrchestratorId(SESSION_ID));
            expect(rows.aimdCeiling).not.toBeNull();
          },
        ],
        [
          'absent (cold start)',
          async () => {
            const raw = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
            await writeFile(
              coordinationFilePath,
              JSON.stringify(raw.filter((entry) => entry?.orchestratorId !== '__aimd-ceiling-state__')),
              'utf8',
            );
            const rows = await ledgerRows(deriveOrchestratorId(SESSION_ID));
            expect(rows.aimdCeiling).toBeNull();
          },
        ],
        [
          'present but MALFORMED',
          async () => {
            const raw = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
            await writeFile(
              coordinationFilePath,
              JSON.stringify(
                raw.map((entry) =>
                  entry?.orchestratorId === '__aimd-ceiling-state__'
                    ? { ...entry, ceiling: 'not-a-number', sustainedNormalCount: null }
                    : entry,
                ),
              ),
              'utf8',
            );
            const rows = await ledgerRows(deriveOrchestratorId(SESSION_ID));
            expect(rows.aimdCeiling.ceiling).toBe('not-a-number');
          },
        ],
      ])('with the AIMD ceiling row %s', (_aimdRowState, prepareAimdRow) => {
        test("a REAL cli.mjs consultation leaves this session's declared desiredAgents and every reserved row untouched", async () => {
          // A genuine multi-agent declaration, so a clobber is visible as a
          // VALUE change (4 -> 1) rather than only as a timestamp refresh.
          const optIn = runRealCli(SESSION_ID, {}, ['--desired-agents=4']);
          expect(optIn.status).toBe(0);
          const orchestratorId = await ledgerOrchestratorIdAfterOptIn();

          // Put the AIMD row into this case's state AFTER the opt-in beat
          // (which always seeds it) and BEFORE the byte snapshot below.
          await prepareAimdRow();

          const rowsBefore = await ledgerRows(orchestratorId);
          // Preconditions — without these the assertions below could pass
          // vacuously against rows that were never written in the first place.
          // Deliberately NOT asserting `aimdCeiling` is non-null here: its
          // state is this case's parameter, already asserted inside
          // `prepareAimdRow`.
          expect(rowsBefore.session.desiredAgents).toBe(4);
          expect(rowsBefore.globalSpawnRateBucket).not.toBeNull();
          expect(rowsBefore.liveAgentAdmission).not.toBeNull();
          const bytesBefore = coordinationFileBytes();

          const { status } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_FAKE_COLLECT_JSON: COLLECT_GREEN } });
          expect(status).toBe(0);

          const rowsAfter = await ledgerRows(orchestratorId);
          // Named individually so a regression says WHICH invariant broke.
          expect(rowsAfter.session.desiredAgents).toBe(4);
          expect(rowsAfter.session).toEqual(rowsBefore.session);
          expect(rowsAfter.globalSpawnRateBucket).toEqual(rowsBefore.globalSpawnRateBucket);
          expect(rowsAfter.aimdCeiling).toEqual(rowsBefore.aimdCeiling);
          expect(rowsAfter.liveAgentAdmission).toEqual(rowsBefore.liveAgentAdmission);
          expect(rowsAfter.sharedSample).toEqual(rowsBefore.sharedSample);
          expect(rowsAfter.everOptedIn).toEqual(rowsBefore.everOptedIn);
          // And the catch-all: nothing else moved either, including row order.
          expect(coordinationFileBytes()).toBe(bytesBefore);
        });
      });

      test('the hook consults cli.mjs under the SAME orchestrator-id the orchestrator declared', async () => {
        const orchestratorId = deriveOrchestratorId(SESSION_ID);
        expect(runRealCli(SESSION_ID).status).toBe(0);

        const standIn = writeStandInCli('verdict', { type: 'spawn-allowed', allowance: 1 });
        const { status } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(0);
        expect(cliInvocationCount()).toBe(1);
        expect(recordedOrchestratorIds()).toEqual([orchestratorId]);
      });

      test("one session's opt-in does not gate a DIFFERENT, never-opted-in session", async () => {
        expect(runRealCli(SESSION_ID).status).toBe(0);

        const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });
        const { status } = runHook({ toolName: spawnToolName, sessionId: OTHER_SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(0);
        expect(cliInvocationCount()).toBe(0);
      });
    });

    describe('tier 3 — STICKY: the gate survives the live entry being pruned or released', () => {
      // The single most important assertion in this file. Without stickiness a
      // mid-session `pruneStale` expiry or a stray `--release` silently disarms
      // the gate and every subsequent spawn goes unchecked — exactly the gap
      // exists to close, moved one layer down.
      test('a session that opted in stays gated after its live dibs entry is pruned/released', async () => {
        const orchestratorId = deriveOrchestratorId(SESSION_ID);
        expect(runRealCli(SESSION_ID).status).toBe(0);

        const survivors = await dropAllLiveEntries();
        // Precondition: the LIVE row really is gone (so a "does it have an entry
        // right now" implementation would read this session as never-opted-in)…
        expect(await liveEntries()).toEqual([]);
        // …while Phase 1's reserved, never-pruned ever-opted-in row survives.
        expect(survivors.some((entry) => isReservedEntry(entry?.orchestratorId))).toBe(true);

        const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });
        const { status, stderr } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(2);
        expect(cliInvocationCount()).toBe(1);
        expect(recordedOrchestratorIds()).toEqual([orchestratorId]);
        expect(stderr).toMatch(/hold/);
      });

      test('stickiness gates rather than blanket-denies — a spawn-allowed verdict still allows after pruning', async () => {
        const orchestratorId = deriveOrchestratorId(SESSION_ID);
        expect(runRealCli(SESSION_ID).status).toBe(0);
        await dropAllLiveEntries();

        const standIn = writeStandInCli('verdict', { type: 'spawn-allowed', allowance: 1 });
        const { status } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(0);
        expect(cliInvocationCount()).toBe(1);
      });

      test('stickiness holds across repeated calls — every subsequent spawn is consulted, not just the first', async () => {
        const orchestratorId = deriveOrchestratorId(SESSION_ID);
        expect(runRealCli(SESSION_ID).status).toBe(0);
        await dropAllLiveEntries();

        const standIn = writeStandInCli('verdict', { type: 'hold', allowance: 0 });
        for (let i = 0; i < 3; i += 1) {
          expect(runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } }).status).toBe(2);
        }

        // Closes the "consults for beat 1, then spawns beats 2-N unchecked" gap.
        expect(cliInvocationCount()).toBe(3);
        expect(recordedOrchestratorIds()).toEqual([orchestratorId, orchestratorId, orchestratorId]);
      });
    });

    // Phase 3, third review round. Both of these exercise the REAL
    // cli.mjs (never a stand-in) because the property under test is what the
    // advisory pipeline itself computes and pays for, end to end, through the
    // hook — the two things the third review round found the hook was
    // silently getting wrong.
    describe('tier 2b — the advisory verdict is faithful, and cheap, on the axes it claims', () => {
      test("denies a session whose OWN declared fleet already overshoots the memory budget", async () => {
        const historyFilePath = join(workDir, 'history.json');
        const projectionEnv = { ARM_HISTORY_FILE: historyFilePath };

        // Two DISTINCT ~6 GB classes: either alone plus a candidate fits the
        // 14336 MB budget, the pair plus a candidate does not. The gate must
        // see BOTH, and it learns them only from this session's own row.
        for (const agentClass of ['gate-heavy-a', 'gate-heavy-b']) {
          for (const [index, peakMemoryMb] of [6000, 6000, 6000].entries()) {
            const seeded = spawnSync(
              'node',
              [
                REAL_CLI,
                '--record-outcome',
                `--operation-type=${agentClass}`,
                `--orchestrator-id=seed-${agentClass}-${index}`,
                `--peak-memory-mb=${peakMemoryMb}`,
              ],
              { encoding: 'utf8', env: baseEnv(projectionEnv) },
            );
            expect(seeded.status).toBe(0);
          }
        }

        // The opt-in is a real, skill-driven beat declaring the fleet — and it
        // is itself admitted, so the deny below cannot be inherited from it.
        const optIn = runRealCli(SESSION_ID, projectionEnv, [
          '--desired-agents=1',
          '--agent-class=gate-heavy-a',
          '--running-agent-classes=gate-heavy-b',
        ]);
        expect(optIn.status).toBe(0);
        expect(JSON.parse(optIn.stdout).type).not.toBe('memory-projection-block');

        const orchestratorId = await ledgerOrchestratorIdAfterOptIn();
        const rowsBefore = await ledgerRows(orchestratorId);
        const bytesBefore = coordinationFileBytes();

        const { status, stderr } = runHook({
          toolName: spawnToolName,
          sessionId: SESSION_ID,
          env: { ...projectionEnv, ARM_FAKE_COLLECT_JSON: COLLECT_GREEN },
        });

        // Pre-fix this ALLOWED: the hook passes no `--running-agent-classes`
        // (it cannot know one), and the empty list that produced replaced this
        // session's own declared classes, pricing its whole running fleet at
        // zero.
        expect(status).toBe(2);
        expect(stderr).toMatch(/memory-projection-block/);

        // A POLICY deny — nothing failed, so nothing lands in the
        // fail-closed bucket — and still a pure read.
        expect(await readLivenessLog(livenessLogPath)).toEqual([]);
        expect(await ledgerRows(orchestratorId)).toEqual(rowsBefore);
        expect(coordinationFileBytes()).toBe(bytesBefore);
      });

      test('denies on a memory-RED host as a POLICY deny, quickly and without a fail-closed trip', async () => {
        const orchestratorId = deriveOrchestratorId(SESSION_ID);
        expect(runRealCli(SESSION_ID).status).toBe(0);

        // A RED host with a real-looking fleet: pre-fix, every one of these
        // agents cost the advisory run a `/usr/bin/footprint` (->
        // `/usr/bin/vmmap`) shell-out, each bounded at 2s and effectively
        // serialized behind a second `ps` collection — the chain that can
        // overrun CLI_TIMEOUT_MS and manufacture a fail-closed deny.
        const redEnv = {
          ARM_FAKE_COLLECT_JSON: JSON.stringify({
            memory: { pressureLevel: 1, swapUsedMb: 12288, compressedMb: 4096 },
            disk: DISK_GREEN,
          }),
          ARM_FAKE_PS_OUTPUT: ['  PID  PPID    RSS ELAPSED COMM', '910001 1 400000 05:00 /usr/local/bin/claude'].join(
            '\n',
          ),
        };

        const startedAt = Date.now();
        const { status, stderr } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: redEnv });
        const elapsedMs = Date.now() - startedAt;

        expect(status).toBe(2);
        expect(stderr).toMatch(/"pause"/);
        // Not a fail-closed trip: ARM answered clearly and the answer was
        // `pause`. The gate reads only `type`, so `pauseCandidate: null` is
        // behaviour-preserving here.
        expect(await readLivenessLog(livenessLogPath)).toEqual([]);
        expect(elapsedMs).toBeLessThan(4_000);
      });
    });

    describe('tier 4 — FAIL CLOSED once opted in when cli.mjs itself fails', () => {
      test('denies with an actionable message when cli.mjs crashes, and records the fail-closed trip', async () => {
        const orchestratorId = deriveOrchestratorId(SESSION_ID);
        expect(runRealCli(SESSION_ID).status).toBe(0);

        const standIn = writeStandInCli('crash');
        const { status, stderr } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(2);
        expect(cliInvocationCount()).toBe(1);
        // Actionable, by design's mitigation: names the mechanism, says it
        // failed closed, and tells a human where to look.
        expect(stderr).toMatch(/agent-resource-management/i);
        expect(stderr).toMatch(/fail(ing|ed)?[- ]closed/i);
        expect(stderr).toMatch(/cli\.mjs/);

        const entries = await readLivenessLog(livenessLogPath);
        expect(entries.filter((entry) => entry.detectionType === 'arm-gate-fail-closed')).toHaveLength(1);
      });

      test('denies when cli.mjs returns unparseable output', async () => {
        const orchestratorId = deriveOrchestratorId(SESSION_ID);
        expect(runRealCli(SESSION_ID).status).toBe(0);

        const standIn = writeStandInCli('unparseable');
        const { status, stderr } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });

        expect(status).toBe(2);
        expect(stderr).toMatch(/fail(ing|ed)?[- ]closed/i);

        const entries = await readLivenessLog(livenessLogPath);
        expect(entries.filter((entry) => entry.detectionType === 'arm-gate-fail-closed')).toHaveLength(1);
      });

      test(
        'denies within the documented timeout budget when cli.mjs hangs, rather than stalling the tool call',
        async () => {
          const orchestratorId = deriveOrchestratorId(SESSION_ID);
          expect(runRealCli(SESSION_ID).status).toBe(0);

          const standIn = writeStandInCli('hang');
          const startedAt = Date.now();
          const { status, stderr } = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: standIn } });
          const elapsedMs = Date.now() - startedAt;

          expect(status).toBe(2);
          expect(stderr).toMatch(/fail(ing|ed)?[- ]closed/i);
          // this design's 5s cli.mjs timeout is load-bearing, not polish; the
          // headroom above it covers the best-effort liveness-log append, which
          // must NOT stack its own multi-second lock-wait on top (Convergence
          // Analysis edge case 5).
          expect(elapsedMs).toBeLessThan(15_000);

          const entries = await readLivenessLog(livenessLogPath);
          expect(entries.filter((entry) => entry.detectionType === 'arm-gate-fail-closed')).toHaveLength(1);
        },
        30_000,
      );

      test('a MISSING cli.mjs denies for an opted-in session but still allows a never-opted-in one', () => {
        const missing = join(workDir, 'does-not-exist.mjs');

        // Never opted in -> fail OPEN (no rule established for this session).
        expect(runHook({ toolName: spawnToolName, sessionId: OTHER_SESSION_ID, env: { ARM_CLI_PATH: missing } }).status).toBe(0);

        // Opted in -> fail CLOSED.
        expect(runRealCli(SESSION_ID).status).toBe(0);
        const denied = runHook({ toolName: spawnToolName, sessionId: SESSION_ID, env: { ARM_CLI_PATH: missing } });
        expect(denied.status).toBe(2);
        expect(denied.stderr).toMatch(/fail(ing|ed)?[- ]closed/i);
      });
    });
  });
});
