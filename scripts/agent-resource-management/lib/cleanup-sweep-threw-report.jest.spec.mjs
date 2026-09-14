// Phase 3 review (Medium) — "fired and deleted things" must never be
// reported as "never fired".
//
// THE DEFECT THIS FILE PINS
//
// `cli.mjs`'s `maybeSweepStaleTempRoot` wraps the sweep in a belt-and-braces
// `catch`. That `catch` used to `return undefined`, and `undefined` is that
// function's word for "the trigger did not fire" — the beat then emits NO
// `cleanup` key at all. But the scenario the `catch` exists for is a throw
// from PARTWAY THROUGH a sweep, after removals have already been issued. An
// irreversible deletion would have been reported as if nothing had happened.
//
// The invariant `maybeSweepStaleTempRoot`'s own docstring states, and which
// this file exists to keep true in every arm:
//
//   absent `cleanup`                      = the trigger never fired
//   present `cleanup`, `removedCount: 0`  = fired, found nothing to do
//
// TWO HALVES, DELIBERATELY
//
// 1. BEHAVIOURAL, over `sweepThrewReport` itself — the real function `cli.mjs`
//    calls, exercised with real thrown values including a hostile one whose
//    `.message` getter throws. This is the half that proves the payload is
//    shape-complete, correctly kinded, and total.
//
// 2. STRUCTURAL, over `cli.mjs`'s catch arm — a source scan, borrowing the
//    `stripComments` idiom from `./cleanup-temp-root-anchor-source-scan
//    .jest.spec.mjs` (same "regex, not a parser" caveat). A source scan rather
//    than a child-process test because `cli.mjs` runs `main()` at import time
//    (so it cannot be imported and stubbed in-process) and
//    `sweepStaleTempDirsAsync` contracts NEVER to throw (so there is no input
//    that makes the real child take this arm). The arm is insurance against a
//    FUTURE edit breaking that contract; the honest way to hold a claim about
//    code that today is unreachable is to read the code. Phase 4's
//    `jest.unstable_mockModule` latency harness is where a live in-process
//    stub of the sweep becomes available.

import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { sweepThrewReport, SWEEP_MAX_ERRORS } from './cleanup.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const CLI_PATH = join(HERE, '..', 'cli.mjs');

/** See the header: preserves length and line structure so tokens never merge. */
function stripComments(source) {
  return source.replace(/\/\*[\s\S]*?\*\/|\/\/.*$/gm, (match) => match.replace(/[^\n]/g, ' '));
}

describe('sweepThrewReport returns a PRESENT, shape-complete report rather than nothing', () => {
  test('it is not undefined, and carries every field the beat payload contract enumerates', () => {
    const report = sweepThrewReport('/tmp/whatever', new Error('boom'));

    // Exactly the assertions `cleanup-auto-trigger-outer-acceptance
    // .jest.spec.mjs`'s `expectCleanupShape` makes, so a report from this path
    // is indistinguishable-in-shape from a report from a real sweep.
    expect(report).toBeDefined();
    expect(typeof report.removedCount).toBe('number');
    expect(typeof report.reclaimedBytes).toBe('number');
    expect(Array.isArray(report.errors)).toBe(true);
    expect(typeof report.truncatedCount).toBe('number');
    expect(typeof report.budgetExhausted).toBe('boolean');
  });

  test('it names the failure under its own `sweep-threw` kind, with the attempted root and the message', () => {
    const report = sweepThrewReport('/tmp/attempted-root', new Error('mid-removal explosion'));

    expect(report.errors).toHaveLength(1);
    expect(report.errors[0].kind).toBe('sweep-threw');
    expect(report.errors[0].path).toBe('/tmp/attempted-root');
    expect(report.errors[0].message).toContain('mid-removal explosion');
  });

  test('it claims nothing it cannot account for: every counter is zero', () => {
    const report = sweepThrewReport('/tmp/whatever', new Error('boom'));

    // A throw destroyed whatever tallies the sweep had. Zero here means "this
    // sweep can no longer account for itself" — which is what the errors entry
    // beside it says — never a guess at how much was removed.
    expect(report.removedCount).toBe(0);
    expect(report.reclaimedBytes).toBe(0);
    expect(report.truncatedCount).toBe(0);
  });

  test('it does NOT claim more work is queued — budgetExhausted and candidatesCapped stay false', () => {
    const report = sweepThrewReport('/tmp/whatever', new Error('boom'));

    // Both flags mean "come back immediately, there is more to do". A sweep
    // that threw has established no such thing — and, via `cli.mjs`'s
    // cooldown, setting either would change the NEXT beat's behaviour.
    expect(report.budgetExhausted).toBe(false);
    expect(report.candidatesCapped).toBe(false);
  });

  test('it is total: a hostile error whose .message getter throws is described, not propagated', () => {
    const hostile = {
      get message() {
        throw new Error('getter exploded');
      },
    };

    let report;
    expect(() => {
      report = sweepThrewReport('/tmp/whatever', hostile);
    }).not.toThrow();
    expect(report.errors[0].kind).toBe('sweep-threw');
    expect(typeof report.errors[0].message).toBe('string');
  });

  test('it is total for a hostile `dir` too, and stays within the bounded errors ring', () => {
    const hostile = {
      toString() {
        throw new Error('dir toString exploded');
      },
    };

    let report;
    expect(() => {
      report = sweepThrewReport(hostile, 'a thrown string, not an Error');
    }).not.toThrow();
    expect(report.errors.length).toBeLessThanOrEqual(SWEEP_MAX_ERRORS);
    expect(report.errors[0].kind).toBe('sweep-threw');
  });
});

describe("cli.mjs's sweep catch arm returns that report instead of undefined", () => {
  let catchArm;

  beforeAll(async () => {
    const code = stripComments(await readFile(CLI_PATH, 'utf8'));
    const fnAt = code.indexOf('async function maybeSweepStaleTempRoot');
    expect(fnAt).toBeGreaterThan(-1);

    // PAREN-balance the parameter list FIRST. `maybeSweepStaleTempRoot` takes
    // a DESTRUCTURING parameter object, so "the first `{` after the name" is
    // the parameter list, not the body — the exact trap `lib/cleanup.mjs`'s
    // own header warns source scans about. Then brace-balance the real body,
    // so the scan can never run past the function and pass against unrelated
    // code.
    const parenOpen = code.indexOf('(', fnAt);
    let parenDepth = 0;
    let parenEnd = parenOpen;
    for (; parenEnd < code.length; parenEnd += 1) {
      if (code[parenEnd] === '(') parenDepth += 1;
      else if (code[parenEnd] === ')') {
        parenDepth -= 1;
        if (parenDepth === 0) break;
      }
    }
    const open = code.indexOf('{', parenEnd);
    let depth = 0;
    let end = open;
    for (; end < code.length; end += 1) {
      if (code[end] === '{') depth += 1;
      else if (code[end] === '}') {
        depth -= 1;
        if (depth === 0) break;
      }
    }
    const body = code.slice(open, end + 1);

    const catchAt = body.indexOf('} catch');
    expect(catchAt).toBeGreaterThan(-1);
    catchArm = body.slice(catchAt);
  });

  test('the catch arm returns sweepThrewReport(...)', () => {
    expect(catchArm).toMatch(/return\s+sweepThrewReport\s*\(/);
  });

  test('the catch arm never returns undefined again (the regression itself)', () => {
    expect(catchArm).not.toMatch(/return\s+undefined\s*;/);
    expect(catchArm).not.toMatch(/return\s*;/);
  });

  test('the catch arm binds the caught value rather than discarding it', () => {
    // A bare `catch {` cannot describe what went wrong, which would leave the
    // report present but mute — half the finding unfixed.
    expect(catchArm).toMatch(/catch\s*\(\s*\w+\s*\)/);
  });

  test('cli.mjs imports sweepThrewReport from the cleanup module rather than re-declaring the shape inline', () => {
    // An inline object literal would drift from `createSweepReport` the first
    // time a field is added to the report.
    return readFile(CLI_PATH, 'utf8').then((raw) => {
      expect(stripComments(raw)).toMatch(/import\s*{[^}]*\bsweepThrewReport\b[^}]*}\s*from\s*'\.\/lib\/cleanup\.mjs'/);
    });
  });
});
