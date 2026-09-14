//  premise-guard — a lightweight structural sentinel, not a
// behavioral test of cli.mjs itself. It reads the SOURCE TEXT of every
// `*.jest.spec.mjs` file in this directory and asserts a pre-existing
// invariant this whole directory's test suite relies on: every place that
// fakes `now` via ARM_FAKE_NOW_MS also configures BOTH ARM_COORDINATION_FILE
// AND ARM_BEAT_NOW_TEST_MODE. That is what makes cli.mjs's `resolveNow()`
// gate (fake `now` honoured only when ALL THREE of ARM_FAKE_NOW_MS,
// ARM_COORDINATION_FILE, and ARM_BEAT_NOW_TEST_MODE are set — a strict
// three-way AND, see cli.mjs's resolveNow()/resolveCoordinationFilePath())
// cost-free against the existing suite: nothing here needs to change for the
// gate to hold.
//
// History: introduced the original two-key invariant
// (ARM_FAKE_NOW_MS + ARM_COORDINATION_FILE). Phase 1 added
// ARM_BEAT_NOW_TEST_MODE as a required third master switch to `resolveNow()`
// and Phase 2 (this file, as it stands now) promoted the three-key pairing
// to be the sentinel's one, permanent, canonical invariant — there is no
// longer a separately-enforced two-key-only check; a spec that pairs
// ARM_FAKE_NOW_MS with ARM_COORDINATION_FILE alone (omitting
// ARM_BEAT_NOW_TEST_MODE) is a violation under the check below.
//
// This is a TEXT scan (readdirSync + readFileSync + regex/brace matching),
// deliberately not an AST parse — precise enough for this directory's actual
// authoring conventions (see the three resolution tiers below), not a
// general-purpose JS analyzer.
//
// Scope note (documented per the task's own instruction to use judgment
// here): cli.jest.spec.mjs's OWN " " describe block (the last thing in
// that file — see its own header comment) deliberately, and intentionally,
// sets ARM_FAKE_NOW_MS WITHOUT its full required pairing in several of its
// own env objects (that unset condition is the very thing those tests are
// pinning). That block is excluded from this scan by cutting the file at
// the block's opening marker — everything before it is still scanned in
// full, and everything after it is exactly the block those tests own.

import { readdirSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const DIR = dirname(fileURLToPath(import.meta.url));

const NEW_GATE_DESCRIBE_MARKER =
  "describe('cli.mjs resolveNow() ARM_FAKE_NOW_MS gated on ARM_COORDINATION_FILE'";

/**
 * Finds the smallest `{ ... }` object literal enclosing `matchIndex`, via
 * brace-depth counting (backward, then forward from the found start).
 * Returns the literal's text (braces included), or null if unbalanced.
 */
function findEnclosingObjectLiteral(source, matchIndex) {
  let depth = 0;
  let start = -1;
  for (let i = matchIndex; i >= 0; i -= 1) {
    const ch = source[i];
    if (ch === '}') depth += 1;
    else if (ch === '{') {
      if (depth === 0) {
        start = i;
        break;
      }
      depth -= 1;
    }
  }
  if (start === -1) return null;

  depth = 0;
  for (let i = start; i < source.length; i += 1) {
    const ch = source[i];
    if (ch === '{') depth += 1;
    else if (ch === '}') {
      depth -= 1;
      if (depth === 0) return { text: source.slice(start, i + 1), start, end: i };
    }
  }
  return null;
}

/**
 * Given the start index of an object literal, walks backward over any
 * preceding sibling arguments/whitespace to find the enclosing function
 * CALL's name, if this literal is itself one argument of a call expression
 * (e.g. `claim(args, { ARM_FAKE_NOW_MS: ... })` -> returns "claim").
 * Tracks paren depth so it correctly skips over other balanced parens
 * between the literal and the call's own opening `(`.
 */
function findEnclosingCallName(source, literalStart) {
  let parenDepth = 0;
  for (let i = literalStart - 1; i >= 0; i -= 1) {
    const ch = source[i];
    if (ch === ')') {
      parenDepth += 1;
    } else if (ch === '(') {
      if (parenDepth === 0) {
        let j = i - 1;
        while (j >= 0 && /\s/.test(source[j])) j -= 1;
        const end = j + 1;
        while (j >= 0 && /[\w$]/.test(source[j])) j -= 1;
        const name = source.slice(j + 1, end);
        return name || null;
      }
      parenDepth -= 1;
    }
  }
  return null;
}

/** Extracts the body of a `function <name>(...) { ... }` declaration, if present. */
function findFunctionBody(source, name) {
  const re = new RegExp(`function\\s+${name}\\s*\\(`);
  const match = re.exec(source);
  if (!match) return null;

  // The regex match ends right after the parameter list's OPENING `(` — walk
  // forward tracking paren depth to find its matching `)` first, rather than
  // naively taking the first `{` after the match. A default-parameter value
  // shaped like an object literal (e.g. `extraEnv = {}`) has its own `{}`
  // INSIDE the parameter list, which would otherwise be mistaken for the
  // function body's opening brace and yield an empty, wrong "body".
  const paramListOpenParen = match.index + match[0].length - 1;
  let parenDepth = 0;
  let paramListCloseParen = -1;
  for (let i = paramListOpenParen; i < source.length; i += 1) {
    const ch = source[i];
    if (ch === '(') parenDepth += 1;
    else if (ch === ')') {
      parenDepth -= 1;
      if (parenDepth === 0) {
        paramListCloseParen = i;
        break;
      }
    }
  }
  if (paramListCloseParen === -1) return null;

  const bodyBraceStart = source.indexOf('{', paramListCloseParen + 1);
  if (bodyBraceStart === -1) return null;
  let depth = 0;
  for (let i = bodyBraceStart; i < source.length; i += 1) {
    const ch = source[i];
    if (ch === '{') depth += 1;
    else if (ch === '}') {
      depth -= 1;
      if (depth === 0) return source.slice(bodyBraceStart, i + 1);
    }
  }
  return null;
}

/**
 * Extracts the object-literal value of a `const <name> = { ... }` /
 * `let <name> = { ... }` declaration. `source` is the text preceding the
 * spread usage being resolved, so multiple same-named declarations can
 * legitimately appear in it (this directory's convention is a fresh, locally
 * scoped `const env = {...}` per test) — takes the LAST (nearest-preceding)
 * match, not the first, so a shadowing declaration in an earlier, unrelated
 * test doesn't get mistaken for the one actually in scope at the spread site.
 */
function findConstObjectLiteral(source, name) {
  const re = new RegExp(`(?:const|let)\\s+${name}\\s*=\\s*\\{`, 'g');
  let match;
  let lastMatch = null;
  while ((match = re.exec(source)) !== null) {
    lastMatch = match;
  }
  if (!lastMatch) return null;
  const braceStart = lastMatch.index + lastMatch[0].length - 1;
  const found = findEnclosingObjectLiteral(source, braceStart);
  return found ? found.text : null;
}

/**
 * Decides whether one `ARM_FAKE_NOW_MS:` occurrence's env object is paired
 * with the given `key`, across three tiers this directory's spec files
 * actually use:
 *
 *   1. Direct — both keys literally in the same object literal
 *      (e.g. the empty-string-regression test's env object).
 *   2. Spread-of-local-const — the literal spreads a `const env = {...}` /
 *      `const baseEnv = {...}` defined earlier in the same file, and THAT
 *      literal sets `key` (e.g. cli-two-orchestrators's `baseEnv` pattern).
 *   3. Wrapper-function-default — the literal is passed as an argument to a
 *      locally-defined helper (e.g. claim.jest.spec.mjs's `claim(args, {...})`,
 *      whose OWN body sets `key` as a default the caller's object can
 *      override but need not repeat).
 *
 * Generalized on the key being checked so the same tiered resolution logic
 * serves every master switch this sentinel enforces (ARM_COORDINATION_FILE,
 * ARM_BEAT_NOW_TEST_MODE, and any future addition) without duplicating the
 * tier-walking logic per key.
 */
function isPairedWithKey(source, matchIndex, key) {
  const literal = findEnclosingObjectLiteral(source, matchIndex);
  if (!literal) return false;
  if (literal.text.includes(key)) return true;

  // Tier 2: spread identifiers within this literal.
  const spreadRe = /\.\.\.\s*([A-Za-z_$][\w$]*)/g;
  let spreadMatch;
  while ((spreadMatch = spreadRe.exec(literal.text)) !== null) {
    const identifierSource = source.slice(0, literal.start);
    const constLiteral = findConstObjectLiteral(identifierSource, spreadMatch[1]);
    if (constLiteral && constLiteral.includes(key)) return true;
  }

  // Tier 3: enclosing call's own helper-function body.
  const callName = findEnclosingCallName(source, literal.start);
  if (callName) {
    const body = findFunctionBody(source, callName);
    if (body && body.includes(key)) return true;
  }

  return false;
}

/** True only when every key in `keys` resolves per `isPairedWithKey`. */
function isPairedWithAllKeys(source, matchIndex, keys) {
  return keys.every((key) => isPairedWithKey(source, matchIndex, key));
}

// The canonical, permanent pairing invariant this sentinel enforces (see
// header comment): every ARM_FAKE_NOW_MS-setting env object must be paired
// with BOTH of these, not either one alone.
const REQUIRED_FAKE_NOW_KEYS = ['ARM_COORDINATION_FILE', 'ARM_BEAT_NOW_TEST_MODE'];

/** Scans `source` for ARM_FAKE_NOW_MS occurrences unpaired with `keys`; returns `"file:line"` violation strings. */
function collectUnpairedViolations(source, file, keys) {
  const violations = [];
  const fakeNowKeyRe = /ARM_FAKE_NOW_MS\s*:/g;
  let match;
  while ((match = fakeNowKeyRe.exec(source)) !== null) {
    if (!isPairedWithAllKeys(source, match.index, keys)) {
      const line = source.slice(0, match.index).split('\n').length;
      violations.push(`${file}:${line}`);
    }
  }
  return violations;
}

test(
  'every ARM_FAKE_NOW_MS-setting env object in every *.jest.spec.mjs file in this directory is paired with BOTH ' +
    'ARM_COORDINATION_FILE AND ARM_BEAT_NOW_TEST_MODE',
  () => {
    // Excludes THIS file: its own source text mentions `ARM_FAKE_NOW_MS:`,
    // `ARM_COORDINATION_FILE`, and `ARM_BEAT_NOW_TEST_MODE` as plain strings
    // (inside its own regexes, comments, and fixtures, above/below), which
    // would otherwise be scanned as if they were real test env objects and
    // produce spurious "violations" against its own helper-function bodies.
    const files = readdirSync(DIR).filter(
      (name) => name.endsWith('.jest.spec.mjs') && name !== 'env-seam-premise.jest.spec.mjs',
    );
    expect(files.length).toBeGreaterThan(0);

    const violations = [];

    for (const file of files) {
      let source = readFileSync(join(DIR, file), 'utf8');

      if (file === 'cli.jest.spec.mjs') {
        const markerIndex = source.indexOf(NEW_GATE_DESCRIBE_MARKER);
        // The gate-coverage block is appended as the LAST describe
        // block in this file (see its own header comment) — cutting the
        // scanned text off at its opening marker excludes it (and only it)
        // from this scan, per this file's own header comment.
        expect(markerIndex).toBeGreaterThan(-1);

        // Guard the "and only it" claim above: slicing at markerIndex
        // silently unscans EVERYTHING after the marker, not just the one
        // block that owns the exemption. If a future spec is appended after
        // this block (the natural authoring move — this very file did it,
        // once), it would fall into that unscanned tail and this guard
        // would pass regardless of whether the new spec violates the
        // pairing invariant. Assert there is exactly one `describe(`
        // occurrence from the marker onward (this block's own opening,
        // with no nested describes of its own) — i.e. the marked block is
        // still genuinely the last thing in the file. A future author who
        // appends a new describe block after it must move the block
        // back to last (or teach this guard to scan the new block too),
        // rather than the addition silently going unscanned.
        const tail = source.slice(markerIndex);
        const describeCallCount = (tail.match(/\bdescribe\(/g) ?? []).length;
        expect(describeCallCount).toBe(1);

        source = source.slice(0, markerIndex);
      }

      violations.push(...collectUnpairedViolations(source, file, REQUIRED_FAKE_NOW_KEYS));
    }

    expect(violations).toEqual([]);
  },
);

test('sentinel still excludes the deliberately-violating describe block when checking for BOTH keys', () => {
  const source = readFileSync(join(DIR, 'cli.jest.spec.mjs'), 'utf8');
  const markerIndex = source.indexOf(NEW_GATE_DESCRIBE_MARKER);
  expect(markerIndex).toBeGreaterThan(-1);

  // Sanity check: the excluded tail block genuinely violates the dual-key
  // pairing invariant on (at least some of) its own env objects — e.g. its
  // " " set-leak test's own "Assertion 1" deliberately omits the switch
  // (see that test's comment for why the switch's name isn't spelled out
  // there as a bare token — doing so would falsely satisfy this very
  // sentinel). Note a naive single-key check (ARM_COORDINATION_FILE only) does NOT
  // catch this block's violations at all: its "ARM_COORDINATION_FILE:
  // undefined," lines still contain the substring "ARM_COORDINATION_FILE"
  // textually, so the naive text-scan would treat them as "paired" even
  // though the value is undefined — this is a known looseness of a
  // text-scan sentinel (not an AST parse), and it's why the exclusion of
  // this block exists in the first place regardless of which invariant is
  // being checked. If the assertion below were false, the exclusion
  // mechanism exercised next would be moot (nothing to exclude).
  const tail = source.slice(markerIndex);
  const tailViolations = collectUnpairedViolations(tail, 'cli.jest.spec.mjs (tail)', REQUIRED_FAKE_NOW_KEYS);
  expect(tailViolations.length).toBeGreaterThan(0);

  // The cut mechanism — slicing the source at markerIndex BEFORE running the
  // dual-key scan — must exclude the tail entirely: none of its occurrences
  // are ever visited, so no violation the dual-key scan reports can
  // originate from a line at or beyond the marker's line.
  const tailStartLine = source.slice(0, markerIndex).split('\n').length;
  const cutSource = source.slice(0, markerIndex);
  const dualKeyViolations = collectUnpairedViolations(cutSource, 'cli.jest.spec.mjs', REQUIRED_FAKE_NOW_KEYS);
  const violationsInOrAfterTail = dualKeyViolations.filter((violation) => {
    const line = Number(violation.split(':').pop());
    return line >= tailStartLine;
  });
  expect(violationsInOrAfterTail).toEqual([]);
});

// ---------------------------------------------------------------------------
// Narrower regression checks below, pinned against small fixtures rather
// than the whole directory. These document the individual tiers and prove
// the dual-key check above is strictly stricter than a single-key check —
// not a different, unrelated check — by exercising `isPairedWithKey`
// (single-key) and `isPairedWithAllKeys` (multi-key) directly.
// ---------------------------------------------------------------------------

test('sentinel flags an env object with ARM_FAKE_NOW_MS and ARM_COORDINATION_FILE but missing ARM_BEAT_NOW_TEST_MODE', () => {
  const fixture = `
    it('does a thing', async () => {
      await run({
        env: { ARM_FAKE_NOW_MS: '1000', ARM_COORDINATION_FILE: coordinationFilePath },
      });
    });
  `;
  const matchIndex = fixture.indexOf('ARM_FAKE_NOW_MS');
  expect(isPairedWithAllKeys(fixture, matchIndex, REQUIRED_FAKE_NOW_KEYS)).toBe(false);
  // But it DOES still satisfy a single-key check against ARM_COORDINATION_FILE
  // alone — proving the dual-key invariant is strictly stricter, not a
  // different, unrelated check.
  expect(isPairedWithKey(fixture, matchIndex, 'ARM_COORDINATION_FILE')).toBe(true);
});

test('sentinel passes an env object with ARM_FAKE_NOW_MS, ARM_COORDINATION_FILE, AND ARM_BEAT_NOW_TEST_MODE', () => {
  const fixture = `
    it('does a thing', async () => {
      await run({
        env: {
          ARM_FAKE_NOW_MS: '1000',
          ARM_COORDINATION_FILE: coordinationFilePath,
          ARM_BEAT_NOW_TEST_MODE: '1',
        },
      });
    });
  `;
  const matchIndex = fixture.indexOf('ARM_FAKE_NOW_MS');
  expect(isPairedWithAllKeys(fixture, matchIndex, REQUIRED_FAKE_NOW_KEYS)).toBe(true);
});

test(
  'sentinel resolves ARM_BEAT_NOW_TEST_MODE pairing across all three tiers: direct, spread-of-local-const, ' +
    'wrapper-function-default',
  () => {
    // Tier 1: direct — all three keys in the same object literal.
    const directFixture = `
      it('direct', async () => {
        await run({ env: { ARM_FAKE_NOW_MS: '1000', ARM_COORDINATION_FILE: p, ARM_BEAT_NOW_TEST_MODE: '1' } });
      });
    `;
    const directMatch = directFixture.indexOf('ARM_FAKE_NOW_MS');
    expect(isPairedWithAllKeys(directFixture, directMatch, REQUIRED_FAKE_NOW_KEYS)).toBe(true);

    // Tier 2: spread-of-local-const — a `const env = {...}` sets the two
    // switches; the call site spreads it and adds only ARM_FAKE_NOW_MS.
    const spreadFixture = `
      const baseEnv = { ARM_COORDINATION_FILE: p, ARM_BEAT_NOW_TEST_MODE: '1' };
      it('spread', async () => {
        await run({ env: { ...baseEnv, ARM_FAKE_NOW_MS: '1000' } });
      });
    `;
    const spreadMatch = spreadFixture.indexOf('ARM_FAKE_NOW_MS');
    expect(isPairedWithAllKeys(spreadFixture, spreadMatch, REQUIRED_FAKE_NOW_KEYS)).toBe(true);

    // Tier 3: wrapper-function-default — a locally-defined helper's own body
    // sets the two switches as defaults; the caller's object only overrides
    // ARM_FAKE_NOW_MS.
    const wrapperFixture = `
      function claim(args, extra = {}) {
        const env = { ARM_COORDINATION_FILE: p, ARM_BEAT_NOW_TEST_MODE: '1', ...extra };
        return run(args, env);
      }
      it('wrapper', async () => {
        await claim(args, { ARM_FAKE_NOW_MS: '1000' });
      });
    `;
    const wrapperMatch = wrapperFixture.indexOf('ARM_FAKE_NOW_MS');
    expect(isPairedWithAllKeys(wrapperFixture, wrapperMatch, REQUIRED_FAKE_NOW_KEYS)).toBe(true);
  },
);
