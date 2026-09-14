// Phase 6 — the rejection branches of `resolveOrchestratorIdFlag`
// (`cli.mjs`), pinned as executable contract.
//
// WHY THIS FILE EXISTS. Phase 6 shipped `--session-id=<id>` as the producer
// side of design decision 2: `cli.mjs` derives the `--orchestrator-id` from
// the session id itself, so the orchestrator and the PreToolUse hook can only
// ever agree on the lookup key or be wrong together — never drift apart
// independently. It shipped with three rejection branches and ZERO tests over
// any of them. The final security review confirmed the gap by mutation: it
// turned the `--session-id` + `--orchestrator-id` conflict check into a no-op
// (silently letting `--session-id` win) and the entire ~2610-test ARM suite
// stayed green. `cli.mjs` is also excluded from `jest.skills.config.mjs`'s
// `collectCoverageFrom`, so the coverage ratchet could not catch it either.
//
// The mutant matters because this design's second amendment makes the rejection
// itself load-bearing: "`--session-id` combined with `--orchestrator-id` is a
// hard rejection rather than a precedence rule, because silently picking a
// winner would recreate the same undetectable divergence". A precedence rule
// lets a caller believe it declared dibs under one key while it declared under
// the other — `hasEverOptedIn` then returns `false` forever and the gate takes
// its fail-open path on every gated spawn, permanently and silently. That is
// the "silently never fires" shape this whole feature exists to prevent,
// which is exactly the class of invariant that must not rest on a code comment.
//
// Every assertion below therefore pins the EXIT CODE SPECIFICALLY (2, the
// documented usage-error code — not merely "some non-zero exit"), because a
// mutant that crashes for an unrelated reason must not be able to pass as a
// deliberate rejection.
//
// Black-box, real-process-spawning idiom — mirrors ./cli-bind-pid.jest.spec.mjs
// / ./cli-evict.jest.spec.mjs exactly: never imports cli.mjs directly.

import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

// Imported ONLY to express "the derived id a successful run would have used",
// so the happy-path control below compares two independently-produced
// observations rather than a hard-coded prefix (the Convergence Analysis
// edge case 1 rationale the outer acceptance test also follows).
import { deriveOrchestratorId } from './lib/orchestrator-id.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

/** A realistic Claude Code session id — uuid-shaped, hyphens and all. */
const SESSION_ID = '7c9a1f42-1b7e-4d2c-9f31-0a6b5c8e4d10';

/** The kind of hand-picked literal SKILL.md's examples used before Phase 6. */
const HAND_PICKED_ORCHESTRATOR_ID = 'orchestrator-a';

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const COLLECT_GREEN = JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN });

let workDir;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-session-id-flag-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

/**
 * Runs the real cli.mjs as a real child process — never imports it directly.
 *
 * A GREEN host sample is injected via the existing `ARM_FAKE_COLLECT_JSON`
 * seam and the coordination file is redirected to a tmp path, so a REJECTION
 * that failed to reject would proceed to a real, successful, ledger-mutating
 * beat rather than dying on an unrelated environmental error. That is what
 * makes "the coordination file was never created" a meaningful second
 * assertion: under the reviewer's mutant the beat really would have run.
 */
function runCli(args) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: {
      ...process.env,
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: COLLECT_GREEN,
    },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

describe('cli.mjs rejects every malformed or ambiguous --session-id invocation with exit 2 (Phase 6, design decision 2)', () => {
  test('--session-id combined with --orchestrator-id is a hard rejection, not a silent precedence rule', () => {
    const { status, stdout, stderr } = runCli([
      `--session-id=${SESSION_ID}`,
      `--orchestrator-id=${HAND_PICKED_ORCHESTRATOR_ID}`,
      '--desired-agents=1',
    ]);

    // THE MUTATION KILL. The reviewer's mutant made the conflict check a
    // no-op, so `--session-id` silently won and the beat ran normally: exit
    // 0, a `spawn-allowed` verdict on stdout, a coordination file on disk.
    // All four assertions below flip under it.
    expect(status).toBe(2);
    expect(stderr).toContain('--session-id and --orchestrator-id cannot be combined');
    // The message must say WHY it is a conflict rather than a precedence
    // rule, since that is the property this design makes load-bearing.
    expect(stderr).toMatch(/silently discard/);
    expect(stdout).toBe('');

    // No beat ran: nothing was declared under EITHER key. A precedence rule
    // would have written dibs under one of them.
    expect(existsSync(coordinationFilePath)).toBe(false);
  });

  test('the conflict is rejected regardless of flag order, so neither flag can win by appearing last', () => {
    const { status, stderr } = runCli([
      `--orchestrator-id=${HAND_PICKED_ORCHESTRATOR_ID}`,
      `--session-id=${SESSION_ID}`,
      '--desired-agents=1',
    ]);

    expect(status).toBe(2);
    expect(stderr).toContain('--session-id and --orchestrator-id cannot be combined');
    expect(existsSync(coordinationFilePath)).toBe(false);
  });

  test('--session-id= with an empty value is rejected — the shape an unset $CLAUDE_CODE_SESSION_ID produces', () => {
    // This is the realistic failure: the documented invocation is
    // `--session-id="$CLAUDE_CODE_SESSION_ID"`, and an unset variable expands
    // to the empty string, leaving the flag present with an empty value.
    const { status, stdout, stderr } = runCli(['--session-id=', '--desired-agents=1']);

    expect(status).toBe(2);
    expect(stderr).toContain('--session-id=<id> requires a non-empty value');
    // The message must name the acquisition path, not just complain about a
    // session id — a caller who cannot see `$CLAUDE_CODE_SESSION_ID` in the
    // error is the caller most likely to substitute a hand-picked id, which
    // silently disables the gate for the whole session.
    expect(stderr).toContain('CLAUDE_CODE_SESSION_ID');
    expect(stdout).toBe('');
    expect(existsSync(coordinationFilePath)).toBe(false);
  });

  test('a whitespace-only --session-id is rejected too, so a padded expansion cannot derive a blank-keyed id', () => {
    const { status, stderr } = runCli(['--session-id=   ', '--desired-agents=1']);

    expect(status).toBe(2);
    expect(stderr).toContain('--session-id=<id> requires a non-empty value');
    expect(existsSync(coordinationFilePath)).toBe(false);
  });

  test('a bare --session-id with no value at all is rejected (it parses to `true`, not to a string)', () => {
    const { status, stdout, stderr } = runCli(['--session-id', '--desired-agents=1']);

    expect(status).toBe(2);
    expect(stderr).toContain('--session-id=<id> requires a non-empty value');
    expect(stdout).toBe('');
    expect(existsSync(coordinationFilePath)).toBe(false);
  });

  test('a whitespace-PADDED session id is rejected by deriveOrchestratorId, surfaced as exit 2 rather than a stack trace', () => {
    // The third rejection branch, and the only one reachable past cli.mjs's
    // own non-empty check: a padded-but-non-blank value clears
    // `trim() === ''` and then throws inside `deriveOrchestratorId`, which
    // rejects padding rather than normalizing it (a padded and an unpadded
    // capture of the same session must never derive different lookup keys —
    // the shape). `main()` must convert that throw into the same
    // usage-error exit code as the branches above, never an unhandled
    // rejection (exit 1 plus a stack trace on stderr).
    const invalidSessionId = `  ${SESSION_ID}`;
    const { status, stdout, stderr } = runCli([
      `--session-id=${invalidSessionId}`,
      '--desired-agents=1',
    ]);

    // Guard the premise: if this id ever becomes VALID, the test must fail
    // loudly rather than silently asserting nothing.
    expect(() => deriveOrchestratorId(invalidSessionId)).toThrow();

    expect(status).toBe(2);
    expect(stderr).toContain('--session-id is invalid');
    expect(stderr).not.toMatch(/^\s+at /m);
    expect(stdout).toBe('');
    expect(existsSync(coordinationFilePath)).toBe(false);
  });

  test('CONTROL — a well-formed --session-id alone still succeeds and keys the ledger by the DERIVED id', () => {
    // Without this, every assertion above could be satisfied by a cli.mjs
    // that rejects `--session-id` unconditionally.
    const { status, stdout } = runCli([`--session-id=${SESSION_ID}`, '--desired-agents=1']);

    expect(status).toBe(0);
    expect(JSON.parse(stdout).type).toBe('spawn-allowed');
    expect(existsSync(coordinationFilePath)).toBe(true);
    // The id on disk is the DERIVED one, never the raw session id — the
    // property every rejection above exists to protect.
    const ledger = readFileSync(coordinationFilePath, 'utf8');
    expect(ledger).toContain(deriveOrchestratorId(SESSION_ID));

    // Bare `--orchestrator-id` also remains accepted on its own ( : it
    // is still the supported path for non-session tooling) — only COMBINING
    // it with `--session-id` is the rejection.
    const bare = runCli([`--orchestrator-id=${HAND_PICKED_ORCHESTRATOR_ID}`, '--desired-agents=1']);
    expect(bare.status).toBe(0);
  });
});
