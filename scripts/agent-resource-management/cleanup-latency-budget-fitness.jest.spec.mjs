// Phase 4 — LATENCY, PAYLOAD and PROVENANCE fitness functions for the
// disk-axis temp sweep.
//
// ---------------------------------------------------------------------------
// WHY THIS FILE EXISTS, AND WHY IT IS THE INVERSE OF THE TEST IT REPLACES
// ---------------------------------------------------------------------------
//
// AC1 of forbids the sweep from "blocking or slowing the normal
// traffic-light response path". Revision 1 of this ticket's build plan proposed
// asserting that the sweep's call site **is `await`ed** — which would have
// mechanically ENFORCED the very property AC1 forbids: an unbounded `await` on
// a recursive filesystem walk, sitting on the path that gates every agent spawn
// on the host.
//
// Revision 2 inverts it. The `await` stays (the same beat must report the bytes
// it reclaimed), but what gets asserted is that the `await` **provably cannot
// exceed its budget**, and that the budget is a safe fraction of the real
// deadline it is measured against. That is what this file pins.
//
// ---------------------------------------------------------------------------
// THE THREE RELATIONS PINNED HERE, AND WHY EACH IS A *RELATION*
// ---------------------------------------------------------------------------
//
//   1. TIME.    The sweep's own budget (`SWEEP_BUDGET_MS`) plus the worst-case
//               retry sleep of the cooldown write that follows it
//               (`DEFAULT_COOLDOWN_LOCK_MAX_ATTEMPTS` x
//               `LOCK_RETRY_DELAY_MS` + `LOCK_RETRY_JITTER_MS`) must stay a
//               safe fraction of the PreToolUse hook's `CLI_TIMEOUT_MS`.
//
//   2. PAYLOAD. The serialised `cleanup` field the beat prints must stay a
//               small fraction of the buffer the hook reads it through —
//               Node's default 1 MiB `spawnSync` `maxBuffer`, which applies
//               precisely because the hook sets no `maxBuffer` of its own.
//
//   3. PROVENANCE. The sweep root on the verdict path derives ONLY from
//               `os.tmpdir()` — never from argv, the coordination file, or an
//               `ARM_*` override — and the call is the asynchronous, budgeted
//               export, never the unbudgeted synchronous one next door.
//
// EVERY ONE OF THESE IS ASSERTED AS A RELATION BETWEEN TWO REAL VALUES READ
// FROM THEIR OWN SOURCE OF TRUTH, never as a restated literal. That is the
// whole point: a fitness function that hardcodes `750` and `5000` passes
// happily while both constants drift apart underneath it. Lowering
// `CLI_TIMEOUT_MS` must fail this file just as loudly as raising
// `SWEEP_BUDGET_MS` does — the plan's edge case "assert the relation, in both
// directions", which a ratio (rather than two independent bounds) delivers for
// free.
//
// ---------------------------------------------------------------------------
// WHY THE HOOK'S AND THE CLI'S CONSTANTS ARE SOURCE-SCANNED, NOT IMPORTED
// ---------------------------------------------------------------------------
//
// The build plan says "imported from `hooks/pretooluse-arm-gate.mjs`, not
// duplicated as a literal". The intent — read the real value from the real
// source of truth — is honoured here; the mechanism is not `import`, and
// deliberately so:
//
//   * `hooks/pretooluse-arm-gate.mjs` has NO exports at all. It is a hook
//     entrypoint that calls `main()` at module scope, so importing it would
//     read this Jest worker's stdin and write an allow/deny decision to fd 1 —
//     a side effect, in a test, on the process's real file descriptors.
//   * `DEFAULT_COOLDOWN_LOCK_MAX_ATTEMPTS` is module-private to `cli.mjs`,
//     which is likewise an entrypoint (and is deliberately excluded from this
//     directory's coverage collection).
//
// Exporting either constant purely so a spec could import it would widen a
// module's public surface for a test's convenience — the exact shape
// `dead-export-fitness.jest.spec.mjs` exists to discourage. So they are read
// out of the real source text instead, and `readNumericConstant` below THROWS
// rather than returning a default when a declaration cannot be found. That
// matters more than it looks: a source scan that silently yields `undefined`
// on a rename becomes a vacuous test that passes forever. A rename here is a
// loud failure, which is the correct outcome — the relation genuinely can no
// longer be checked.
//
// The constants that ARE properly exported (`SWEEP_BUDGET_MS`,
// `SWEEP_MAX_ERRORS`, `LOCK_RETRY_DELAY_MS`, ...) are imported normally.
//
// ---------------------------------------------------------------------------
// WHAT THIS FILE DOES *NOT* CLAIM — the honest half
// ---------------------------------------------------------------------------
//
// These are disclosed limitations, not gaps to close in this phase:
//
//   * The hook itself can never trigger a sweep at all. It consults the CLI
//     with `--advise-only`, and `maybeSweepStaleTempRoot`'s FIRST and
//     unconditional refusal is `if (isAdviseOnly) return undefined`. The time
//     relation asserted here is therefore DEFENCE IN DEPTH, not the only thing
//     standing between a hung filesystem and a fail-closed deny. Cell 1c pins
//     that primary suppression so the two guarantees cannot silently collapse
//     into one.
//   * Bounded settlement of the sweep itself — a `readdir` that never settles,
//     a size walk cut off mid-level, an `rm` still in flight at the deadline —
//     is asserted by `lib/cleanup-async-deadline-budget.jest.spec.mjs`, and the
//     beat-level "the sweep never adds more than SWEEP_BUDGET_MS to the beat"
//     by `cleanup-auto-trigger-outer-acceptance.jest.spec.mjs` (condition 7).
//     This file asserts that the budget those files enforce is set to a SANE
//     VALUE relative to the deadline it is protecting. Neither is sufficient
//     alone: an enforced-but-absurd budget and a sane-but-unenforced one both
//     deny agent spawns.
//   * A small residual of unbudgeted work precedes the race by construction —
//     the anchor's `realpathSync` on the sweep root, and the lazy
//     `await import()` of the anchor module. Both were reviewed and explicitly
//     ACCEPTED as disclosed limitations in Phases 1-2 (a single stat on the
//     platform temp root, and one module load that is cached after the first
//     beat). Phase 4 does not re-litigate them; it records that they sit
//     outside the relation asserted here.

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { SWEEP_BUDGET_MS, SWEEP_MAX_ERRORS, SWEEP_MAX_CANDIDATES } from './lib/cleanup.mjs';
import { LOCK_RETRY_DELAY_MS, LOCK_RETRY_JITTER_MS } from './lib/coordination-file.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const CLI_PATH = join(HERE, 'cli.mjs');
const HOOK_PATH = join(HERE, 'hooks', 'pretooluse-arm-gate.mjs');

const CLI_SOURCE = readFileSync(CLI_PATH, 'utf8');
const HOOK_SOURCE = readFileSync(HOOK_PATH, 'utf8');

/**
 * Node's default `spawnSync` `maxBuffer`, in bytes.
 *
 * Not a project constant and not settable from here — it is the documented
 * Node default that applies to `consultCli`'s `spawnSync` precisely BECAUSE
 * that call passes no `maxBuffer` of its own. Cell 2a asserts that premise
 * directly against the hook's source, so this figure cannot quietly become
 * the wrong yardstick if a future edit sets an explicit (smaller) buffer.
 */
const NODE_DEFAULT_MAX_BUFFER_BYTES = 1024 * 1024;

/**
 * The share of `CLI_TIMEOUT_MS` the whole sweep-plus-cooldown sequence may
 * occupy.
 *
 * WHY 40% AND NOT 90%. The budget is not the only thing spending that 5s. The
 * beat it sits inside also collects a process snapshot, reads and locks the
 * coordination file, and computes the verdict — and the hook FAILS CLOSED on
 * timeout, denying every agent spawn on the host (design decision 3). A
 * ceiling that leaves the rest of the beat a sliver is not a budget, it is a
 * countdown. 40% leaves the majority of the deadline to the work the beat
 * actually exists to do, while still being loose enough that this file fails
 * for a real regression rather than for routine tuning.
 */
const COMBINED_BUDGET_SHARE_OF_TIMEOUT = 0.4;

/**
 * The share of `CLI_TIMEOUT_MS` the sweep's OWN budget may occupy.
 *
 * Tighter than the combined ceiling on purpose. The sweep is the discretionary
 * half of this pair: the cooldown write is a few hundred milliseconds of
 * bounded lock retry that exists to SAVE future sweeps, whereas the sweep is
 * the thing whose budget a future contributor will be tempted to raise ("it
 * only got through 24 candidates"). This is the assertion that meets them:
 * draining a five-figure backlog is correct work to spread across many
 * bounded beats, and is never a reason to widen the one beat that gates
 * admission.
 */
const SWEEP_BUDGET_SHARE_OF_TIMEOUT = 0.2;

/** The share of the hook's read buffer the serialised `cleanup` field may occupy. */
const PAYLOAD_SHARE_OF_MAX_BUFFER = 0.1;

/**
 * Reads a `const NAME = <number>;` declaration out of real source text.
 *
 * THROWS when the declaration is absent. See this file's header for why that
 * is the desired behaviour and not a robustness failing: a scan that degrades
 * to `undefined` turns every relation below into a vacuous truth.
 *
 * Numeric separators (`5_000`) are stripped, and an arithmetic right-hand side
 * is refused outright rather than guessed at — this helper's contract is a
 * literal, and a silently mis-parsed `30 * 60_000` would be worse than no
 * reading at all.
 *
 * @param {string} source
 * @param {string} name
 * @param {string} whereForMessage
 * @returns {number}
 */
function readNumericConstant(source, name, whereForMessage) {
  const declaration = new RegExp(`\\bconst\\s+${name}\\s*=\\s*([^;]+);`).exec(source);
  if (!declaration) {
    throw new Error(
      `cleanup-latency-budget-fitness: could not find \`const ${name} = ...\` in ` +
        `${whereForMessage}. This fitness function reads that constant from its real ` +
        `source of truth rather than restating it as a literal, so a rename breaks the ` +
        `check outright instead of letting it pass vacuously. If the constant was ` +
        `renamed, update the name here; if it was deleted, the relation it participates ` +
        `in needs rethinking, not deleting.`,
    );
  }

  const literal = declaration[1].trim();
  if (!/^\d[\d_]*$/.test(literal)) {
    throw new Error(
      `cleanup-latency-budget-fitness: \`${name}\` in ${whereForMessage} is no longer a ` +
        `plain numeric literal (found \`${literal}\`). This helper deliberately refuses to ` +
        `evaluate an expression rather than risk mis-parsing the value a safety relation ` +
        `rests on.`,
    );
  }

  return Number(literal.replace(/_/g, ''));
}

// ---------------------------------------------------------------------------
// 0 — THE CHECKERS THEMSELVES
//
// A source-scanning fitness function is only as honest as its scanner. Both
// helpers below had a real failure mode during this phase's own construction —
// `extractFunctionBody` returned a destructured PARAMETER LIST instead of a
// body, which made every `not.toMatch` assertion in section 3 pass vacuously
// while the code they guard was never read. These fixture cells are the
// acceptance evidence that the checkers do what the cells below assume, in the
// same spirit as `dead-export-fitness.jest.spec.mjs`'s own self-tests.
// ---------------------------------------------------------------------------

describe('Phase 4 — the source scanners used below are themselves checked', () => {
  it('0a: extractFunctionBody skips a destructured parameter list and returns the real body', () => {
    const fixture = [
      'async function target({ alpha, beta }) {',
      '  const inner = { nested: true };',
      '  return inner;',
      '}',
    ].join('\n');

    const body = extractFunctionBody(fixture, 'target');
    expect(body).toMatch(/const inner = \{ nested: true \};/);
    // The negative half — this is the exact bug the helper's doc comment names.
    expect(body).not.toBe('{ alpha, beta }');
  });

  it('0b: extractFunctionBody THROWS on a missing function rather than returning an empty string', () => {
    expect(() => extractFunctionBody('const x = 1;', 'noSuchFunction')).toThrow(/could not find/);
  });

  it('0c: readNumericConstant reads a separated literal, and THROWS rather than defaulting', () => {
    expect(readNumericConstant('const WIDGET_MS = 5_000;', 'WIDGET_MS', 'fixture')).toBe(5000);
    expect(() => readNumericConstant('const OTHER = 1;', 'WIDGET_MS', 'fixture')).toThrow(
      /could not find/,
    );
    // An arithmetic right-hand side is refused, not guessed at.
    expect(() => readNumericConstant('const WIDGET_MS = 30 * 60;', 'WIDGET_MS', 'fixture')).toThrow(
      /no longer a plain numeric literal/,
    );
  });

  it('0d: stripComments removes prose without eating the code around it', () => {
    const stripped = stripComments('const a = 1; // mentions process.env\n/* and ARM_FOO */\nconst b = 2;');
    expect(stripped).not.toMatch(/process\.env/);
    expect(stripped).not.toMatch(/ARM_FOO/);
    expect(stripped).toMatch(/const a = 1;/);
    expect(stripped).toMatch(/const b = 2;/);
  });
});

// ---------------------------------------------------------------------------
// 1 — THE TIME RELATION
// ---------------------------------------------------------------------------

describe('Phase 4 — the sweep budget is a safe fraction of the hook deadline it sits inside', () => {
  it('PREMISE: both constants are readable from their own source, and are sane positive numbers', () => {
    const cliTimeoutMs = readNumericConstant(
      HOOK_SOURCE,
      'CLI_TIMEOUT_MS',
      'hooks/pretooluse-arm-gate.mjs',
    );
    const cooldownLockMaxAttempts = readNumericConstant(
      CLI_SOURCE,
      'DEFAULT_COOLDOWN_LOCK_MAX_ATTEMPTS',
      'cli.mjs',
    );

    // Non-vacuity. Every relation below is meaningless if any input is 0, NaN
    // or negative — a ratio against 0 is not a bound, it is an exception or an
    // Infinity, and `expect(x).toBeLessThan(NaN)` fails in a way that reads
    // like a real regression while meaning nothing of the sort.
    for (const [name, value] of [
      ['CLI_TIMEOUT_MS', cliTimeoutMs],
      ['DEFAULT_COOLDOWN_LOCK_MAX_ATTEMPTS', cooldownLockMaxAttempts],
      ['SWEEP_BUDGET_MS', SWEEP_BUDGET_MS],
      ['LOCK_RETRY_DELAY_MS', LOCK_RETRY_DELAY_MS],
      ['LOCK_RETRY_JITTER_MS', LOCK_RETRY_JITTER_MS],
    ]) {
      expect(`${name}=${value}`).toBe(`${name}=${value}`); // keeps the name in the failure text
      expect(Number.isFinite(value)).toBe(true);
      expect(value).toBeGreaterThan(0);
    }
  });

  it("1a: SWEEP_BUDGET_MS stays within its share of the hook's CLI_TIMEOUT_MS", () => {
    const cliTimeoutMs = readNumericConstant(
      HOOK_SOURCE,
      'CLI_TIMEOUT_MS',
      'hooks/pretooluse-arm-gate.mjs',
    );

    // The relation, not two independent bounds. RAISING SWEEP_BUDGET_MS fails
    // this; so does LOWERING CLI_TIMEOUT_MS without revisiting the sweep —
    // which is the direction a reviewer is least likely to catch by eye,
    // because the diff would not touch cleanup.mjs at all.
    expect(SWEEP_BUDGET_MS / cliTimeoutMs).toBeLessThanOrEqual(SWEEP_BUDGET_SHARE_OF_TIMEOUT);
  });

  it('1b: sweep budget PLUS the cooldown write\'s worst-case lock wait stays within the combined share', () => {
    const cliTimeoutMs = readNumericConstant(
      HOOK_SOURCE,
      'CLI_TIMEOUT_MS',
      'hooks/pretooluse-arm-gate.mjs',
    );
    const cooldownLockMaxAttempts = readNumericConstant(
      CLI_SOURCE,
      'DEFAULT_COOLDOWN_LOCK_MAX_ATTEMPTS',
      'cli.mjs',
    );

    // WORST CASE, not typical: every attempt sleeps the full retry delay AND
    // draws the maximum jitter. `acquireLock` sleeps
    // `lockRetryDelayMs + Math.random() * LOCK_RETRY_JITTER_MS` per attempt,
    // so this is the real ceiling on that arm rather than its mean.
    //
    // These two are ADDED because they are SEQUENTIAL on the beat:
    // `maybeSweepStaleTempRoot` awaits the sweep, and only then awaits
    // `maybeRecordSweepCooldown`. A future edit that made the cooldown write
    // concurrent would make this assertion conservative, never unsafe.
    const worstCaseCooldownWaitMs =
      cooldownLockMaxAttempts * (LOCK_RETRY_DELAY_MS + LOCK_RETRY_JITTER_MS);
    const worstCaseSequenceMs = SWEEP_BUDGET_MS + worstCaseCooldownWaitMs;

    expect(worstCaseSequenceMs / cliTimeoutMs).toBeLessThanOrEqual(
      COMBINED_BUDGET_SHARE_OF_TIMEOUT,
    );
  });

  // -------------------------------------------------------------------------
  // 1d-1f — pre-PR review (High #6): THE CONSTANTS ARE ACTUALLY WIRED UP.
  //
  // Cells 1a/1b (and the PREMISE cell) assert that three constants hold a safe
  // RATIO to one another. None of them asserted that any of the three is
  // actually PASSED to the call site it protects — so deleting
  // `{ lockMaxAttempts: DEFAULT_COOLDOWN_LOCK_MAX_ATTEMPTS }` from the cooldown
  // write, or `timeout: CLI_TIMEOUT_MS` from the hook's `spawnSync`, left every
  // ratio cell green while the real worst case moved by 4x (a ~20s default lock
  // wait against a 5s hook deadline) or became unbounded.
  //
  // A relation between two constants is only a fitness function if both
  // constants are load-bearing at run time. These three cells are what make
  // that true, and they are deliberately WIRING assertions — call shape, read
  // from comment-stripped source — not value assertions, which the cells above
  // already own.
  // -------------------------------------------------------------------------

  it('1d: the cooldown write actually PASSES DEFAULT_COOLDOWN_LOCK_MAX_ATTEMPTS to recordDiskCleanupCooldown', () => {
    const cooldownWrite = stripComments(extractFunctionBody(CLI_SOURCE, 'maybeRecordSweepCooldown'));

    expect(cooldownWrite).toMatch(/recordDiskCleanupCooldown\(/);
    // Without this, the write falls back to the module default and cell 1b is
    // measuring a number nothing uses.
    expect(cooldownWrite).toMatch(/lockMaxAttempts:\s*DEFAULT_COOLDOWN_LOCK_MAX_ATTEMPTS/);
  });

  it('1e: the sweep call site is governed by SWEEP_BUDGET_MS — by taking the default, not by naming another number', () => {
    const sweepFunction = stripComments(extractFunctionBody(CLI_SOURCE, 'maybeSweepStaleTempRoot'));

    // The call site deliberately omits `budgetMs` and relies on the export's
    // own default. Both halves of that have to be asserted, or the omission is
    // indistinguishable from a call site that quietly passes 30_000.
    expect(sweepFunction).toMatch(/sweepStaleTempDirsAsync\(/);
    expect(sweepFunction).not.toMatch(/budgetMs/);

    // ...and the default it relies on really is the constant cells 1a/1b
    // measure. Read from `cleanup.mjs`'s own source so a default changed to a
    // literal — `budgetMs = 5_000` — fails here rather than passing silently.
    const cleanupSource = readFileSync(join(HERE, 'lib', 'cleanup.mjs'), 'utf8');
    expect(cleanupSource).toMatch(/budgetMs\s*=\s*SWEEP_BUDGET_MS\b/);
  });

  it('1f: the hook actually PASSES CLI_TIMEOUT_MS as its spawnSync timeout', () => {
    const consultCli = stripComments(extractFunctionBody(HOOK_SOURCE, 'consultCli'));

    expect(consultCli).toMatch(/spawnSync\(/);
    // The deadline every relation in this section is measured against. Drop
    // this option and the consultation is unbounded, while 1a/1b stay green.
    expect(consultCli).toMatch(/timeout:\s*CLI_TIMEOUT_MS/);
  });

  it('1c: the hook can never trigger a sweep at all — it consults with --advise-only, which is the FIRST, unconditional refusal', () => {
    // The primary guarantee, pinned so it cannot silently collapse into the
    // budget relation above. If a future edit dropped `--advise-only` from the
    // hook's consultation, cells 1a/1b would still pass while the hook had
    // quietly become a caller that CAN sweep.
    expect(HOOK_SOURCE).toMatch(/'--advise-only'/);

    const sweepFunction = extractFunctionBody(CLI_SOURCE, 'maybeSweepStaleTempRoot');
    // Greedy `(.*)` deliberately: three of the four refusal conditions contain
    // their own parentheses (`!diskEvidenceWarrantsSweep(...)`,
    // `await isDiskCleanupSuppressed(...)`), and a lazy `([^)]*)` silently
    // matched only the two parenthesis-free ones — under-counting the gates
    // while still "passing" a `>= 3` check had the threshold been lower. Each
    // refusal is a single line, so greedy-to-the-last-`)` is exact here.
    const refusals = [...sweepFunction.matchAll(/^\s*if\s*\((.*)\)\s*return undefined;/gm)].map(
      (match) => match[1].trim(),
    );

    // ORDER IS THE PROPERTY, not mere presence. The three free refusals sit
    // ahead of the one that costs a file read, and `isAdviseOnly` sits ahead of
    // all of them because it is the guarantee the PreToolUse hook's entire
    // trust argument rests on.
    expect(refusals).toEqual([
      'isAdviseOnly',
      '!diskCleanupArmed',
      '!diskEvidenceWarrantsSweep(diskTrend, freeDiskGb)',
      'await isDiskCleanupSuppressed(coordinationFilePath, now, SWEEP_COOLDOWN_MS)',
    ]);
  });
});

// ---------------------------------------------------------------------------
// 2 — THE PAYLOAD RELATION
// ---------------------------------------------------------------------------

describe('Phase 4 — the serialised cleanup field stays far inside the buffer the hook reads it through', () => {
  it("2a: PREMISE — the hook's spawnSync sets no maxBuffer, so Node's 1 MiB default is the real ceiling", () => {
    // Without this cell the ceiling below is folklore. With it, an edit that
    // introduced an explicit (smaller) `maxBuffer` fails here and forces the
    // yardstick in this file to be revisited alongside it.
    const consultCli = extractFunctionBody(HOOK_SOURCE, 'consultCli');
    expect(consultCli).toMatch(/spawnSync\(/);
    expect(consultCli).not.toMatch(/maxBuffer/);
  });

  it('2b: a worst-case cleanup field — every error slot full, every count saturated — serialises to a small fraction of that buffer', () => {
    // Built from the REAL bounds (`SWEEP_MAX_ERRORS`, `SWEEP_MAX_CANDIDATES`),
    // not from a convenient small fixture, and each error slot is filled with
    // a string longer than `cleanup.mjs`'s own per-field truncation so the
    // construction cannot accidentally understate the worst case.
    //
    // This is the RELATION cell. `lib/cleanup-async-bounded-payload.jest.spec
    // .mjs` already proves the real sweep cannot exceed these bounds; what is
    // asserted here is that the bounds themselves, serialised as the beat
    // prints them, are small against the buffer the hook reads.
    const worstCaseCleanupField = {
      dir: 'x'.repeat(4096),
      removedCount: SWEEP_MAX_CANDIDATES,
      reclaimedBytes: Number.MAX_SAFE_INTEGER,
      completed: false,
      budgetExhausted: true,
      candidatesCapped: true,
      truncatedCount: Number.MAX_SAFE_INTEGER,
      liveWriterSkippedCount: Number.MAX_SAFE_INTEGER,
      errors: Array.from({ length: SWEEP_MAX_ERRORS }, (_unused, index) => ({
        kind: 'remove-failed',
        path: `${'p'.repeat(1024)}/${index}`,
        message: 'e'.repeat(1024),
      })),
    };

    const serialisedBytes = Buffer.byteLength(
      JSON.stringify({ verdict: { type: 'spawn-allowed' }, cleanup: worstCaseCleanupField }),
      'utf8',
    );

    expect(serialisedBytes).toBeLessThanOrEqual(
      NODE_DEFAULT_MAX_BUFFER_BYTES * PAYLOAD_SHARE_OF_MAX_BUFFER,
    );
  });

  it('2c: the bounds that make 2b true are real bounds, not formalities', () => {
    // A future edit raising `SWEEP_MAX_ERRORS` to, say, 50_000 would fail 2b.
    // This cell states the weaker, human-readable half of the same property so
    // a failure in 2b is diagnosable without reconstructing the arithmetic.
    expect(Number.isInteger(SWEEP_MAX_ERRORS)).toBe(true);
    expect(SWEEP_MAX_ERRORS).toBeGreaterThan(0);
    expect(SWEEP_MAX_ERRORS).toBeLessThanOrEqual(64);
    expect(Number.isInteger(SWEEP_MAX_CANDIDATES)).toBe(true);
    expect(SWEEP_MAX_CANDIDATES).toBeGreaterThan(0);
  });
});

// ---------------------------------------------------------------------------
// 3 — THE PROVENANCE RELATION
// ---------------------------------------------------------------------------

describe('Phase 4 — the sweep root on the verdict path derives only from the platform temp root', () => {
  it('3a: the sweep root is `tmpdir()` and nothing else — not argv, not the coordination file, not an ARM_* override', () => {
    const sweepFunction = extractFunctionBody(CLI_SOURCE, 'maybeSweepStaleTempRoot');

    // Call SHAPE, not line numbers (the plan's edge case). A harmless
    // refactor that moves this function does not break the assertion; a change
    // that sources the root from anywhere else does.
    expect(sweepFunction).toMatch(/const sweepRoot = tmpdir\(\);/);

    // The `dir` handed to the sweep is that same binding, verbatim.
    expect(sweepFunction).toMatch(/dir:\s*sweepRoot\b/);

    // And nothing in this function reads an environment override or an argv
    // slot to build it. `ARM_FAKE_NOW_MS` is mentioned in a COMMENT here
    // explaining why the sweep does NOT use the beat clock, so the scan runs
    // against comment-stripped source.
    const executable = stripComments(sweepFunction);
    expect(executable).not.toMatch(/process\.env/);
    expect(executable).not.toMatch(/process\.argv/);
    expect(executable).not.toMatch(/ARM_[A-Z_]+/);
  });

  it('3b: the verdict path calls the BUDGETED async export, never the unbudgeted synchronous sweep next door', () => {
    const sweepFunction = extractFunctionBody(CLI_SOURCE, 'maybeSweepStaleTempRoot');
    const executable = stripComments(sweepFunction);

    expect(executable).toMatch(/await sweepStaleTempDirsAsync\(/);
    // `sweepStaleTempDirs` (the synchronous export) is still a live,
    // supported entry point for the standalone `--cleanup` CLI beat. What must
    // never happen is it appearing HERE, on the path the hook waits on. Match
    // the bare name only when it is NOT followed by `Async`.
    expect(executable).not.toMatch(/\bsweepStaleTempDirs\s*\(/);
  });

  it("3c: the anchor's allowlist seam is never populated from process.env", () => {
    // The confinement guard is only a guard if the thing it checks against
    // cannot be widened by the same environment it is defending against. The
    // behavioural half of this lives in
    // `lib/cleanup-async-confinement-anchor.jest.spec.mjs`; this is the
    // source-level half, which catches a widening that a behavioural test
    // would only notice if it happened to exercise the new path.
    const anchorSource = readFileSync(join(HERE, 'lib', 'cleanup-temp-root-anchor.mjs'), 'utf8');
    expect(stripComments(anchorSource)).not.toMatch(/process\.env/);
  });
});

// ---------------------------------------------------------------------------
// Source-scanning helpers.
// ---------------------------------------------------------------------------

/**
 * Removes line and block comments from source text.
 *
 * Deliberately simple: it is used only to keep PROSE out of "this code must
 * not mention X" assertions, and this directory's sources contain no regex
 * literal or string that would confuse it. It is not, and does not claim to
 * be, a lexer.
 *
 * @param {string} source
 * @returns {string}
 */
function stripComments(source) {
  return source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|\s)\/\/.*$/gm, '$1');
}

/**
 * Slices a named function's body out of source text.
 *
 * THE PARAMETER LIST IS SKIPPED BY BALANCING PARENTHESES FIRST, and that is
 * load-bearing rather than fussy. `maybeSweepStaleTempRoot` takes a single
 * DESTRUCTURED options object, so the first `{` after its name opens the
 * PARAMETER pattern, not the body. A naive "first brace wins" slice returns
 * `{ coordinationFilePath, now, ... }` — a string in which every `not.toMatch`
 * assertion in this file passes trivially and every `toMatch` one fails. The
 * first draft of this helper did exactly that, which is why the failure mode
 * is spelled out here.
 *
 * THROWS when the function cannot be found — same reasoning as
 * `readNumericConstant`: a helper that returns `''` on a rename turns every
 * `not.toMatch` assertion above into a guaranteed pass.
 *
 * @param {string} source
 * @param {string} name
 * @returns {string}
 */
function extractFunctionBody(source, name) {
  const signature = new RegExp(`(?:async\\s+)?function\\s+${name}\\s*\\(`).exec(source);
  if (!signature) {
    throw new Error(
      `cleanup-latency-budget-fitness: could not find \`function ${name}\` to scan. ` +
        `If it was renamed, update the name here — do not delete the assertion, which ` +
        `would silently retire the property it protects.`,
    );
  }

  // Walk past the parameter list.
  let cursor = signature.index + signature[0].length - 1; // sits on the opening `(`
  let parenDepth = 0;
  for (; cursor < source.length; cursor += 1) {
    if (source[cursor] === '(') parenDepth += 1;
    else if (source[cursor] === ')') {
      parenDepth -= 1;
      if (parenDepth === 0) break;
    }
  }

  const open = source.indexOf('{', cursor);
  if (open === -1) {
    throw new Error(
      `cleanup-latency-budget-fitness: \`function ${name}\` has no body brace after its ` +
        `parameter list — nothing below it was actually checked.`,
    );
  }

  let depth = 0;
  for (let index = open; index < source.length; index += 1) {
    if (source[index] === '{') depth += 1;
    else if (source[index] === '}') {
      depth -= 1;
      if (depth === 0) return source.slice(open, index + 1);
    }
  }

  throw new Error(
    `cleanup-latency-budget-fitness: \`function ${name}\` has unbalanced braces — the ` +
      `body could not be sliced, so nothing below it was actually checked.`,
  );
}
