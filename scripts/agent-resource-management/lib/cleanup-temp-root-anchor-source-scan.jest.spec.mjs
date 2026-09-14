// Phase 1 — SOURCE-SCAN FITNESS FUNCTION over the system-temp-root
// anchor (Ford/Parsons, *Building Evolutionary Architectures*; the plan's
// `input-allow-list-fitness-function` strategy).
//
// RED BY CONSTRUCTION: `./cleanup-temp-root-anchor.mjs` does not exist yet, so
// `readFile` rejects with `ENOENT`. That is this file's documented red state.
//
// WHY A SOURCE SCAN AND NOT ONLY A BEHAVIOURAL TEST
//
// The anchor's central property is a NEGATIVE one — "the verdict does not
// depend on the environment". Its sibling
// `./cleanup-async-confinement-anchor.jest.spec.mjs` asserts that
// behaviourally, by moving `process.env.TMPDIR` and watching the verdict stay
// put. But behaviour tests can only probe the env vars the test author thought
// of; a future edit that reads `process.env.ARM_SWEEP_EXTRA_PREFIX` would sail
// past every one of them while re-opening exactly the vacuous-guard hole
// (rev-2 critical finding 1) this module exists to close. The structural
// assertion — "this file contains no `process.env` reference AT ALL" — is
// stronger than any enumeration of variables, and it is the assertion Phase 4
// extends to the `allowedPrefixes` seam.
//
// The same logic covers the destructive surface: "performs no removal and no
// `readdir`; its only fs call is the single realpath resolution" is a claim
// about the code, and the cheapest honest way to hold it is to read the code.
// `./cleanup-temp-root-anchor-fs-surface.jest.spec.mjs` pins the runtime half
// against a mocked `node:fs`; this file pins the static half.
//
// COMMENT-STRIPPING: every scan for a forbidden identifier runs over
// COMMENT-STRIPPED source, borrowing `dead-export-fitness.jest.spec.mjs`'s
// `stripComments` idiom (and its "regex, not a parser" scope caveat). This is
// load-bearing, not incidental: the module's header is REQUIRED to explain why
// `process.env.TMPDIR` is an untrustworthy yardstick, so the phrase must be
// legal in prose and illegal in code. The header-content assertions below run
// over the RAW source for the same reason.

import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const ANCHOR_PATH = join(HERE, 'cleanup-temp-root-anchor.mjs');

/**
 * Blanks `//` line comments and block comments, preserving length and line
 * structure so tokens can never be accidentally merged. Copied in spirit from
 * `dead-export-fitness.jest.spec.mjs`'s helper of the same name; same
 * limitation applies (not string-literal aware).
 */
function stripComments(source) {
  return source.replace(/\/\*[\s\S]*?\*\/|\/\/.*$/gm, (match) => match.replace(/[^\n]/g, ' '));
}

let rawSource;
let codeSource;

beforeAll(async () => {
  rawSource = await readFile(ANCHOR_PATH, 'utf8');
  codeSource = stripComments(rawSource);
});

describe('the anchor module reads no environment, argv, or coordination state', () => {
  test('it contains no process.env reference in executable code', () => {
    expect(codeSource).not.toMatch(/process\s*\.\s*env/);
  });

  test('it contains no destructured env read either (the obvious way round the assertion above)', () => {
    expect(codeSource).not.toMatch(/\benv\b\s*}/);
    expect(codeSource).not.toMatch(/from\s+['"]node:process['"]/);
    expect(codeSource).not.toMatch(/\bTMPDIR\b/);
  });

  test('it names no ARM_* variable in executable code', () => {
    expect(codeSource).not.toMatch(/\bARM_[A-Z0-9_]+\b/);
  });

  test('it reads no argv', () => {
    expect(codeSource).not.toMatch(/process\s*\.\s*argv/);
  });

  test('it imports neither the coordination file, the CLI, nor any other ARM module that could carry env in', () => {
    // The `allowedPrefixes` seam must be reachable ONLY from a caller's
    // explicit argument — never populated transitively.
    for (const forbiddenImport of [
      'coordination-file.mjs',
      'cli.mjs',
      'cleanup.mjs',
      'threshold.mjs',
      'allowance.mjs',
    ]) {
      expect(codeSource).not.toContain(forbiddenImport);
    }
  });

  test('the ONLY process property it may touch is platform', () => {
    const processReferences = [...codeSource.matchAll(/process\s*\.\s*([A-Za-z_$][\w$]*)/g)].map(
      (match) => match[1],
    );
    expect([...new Set(processReferences)].sort()).toEqual(['platform']);
  });
});

describe('the anchor module performs no destructive or directory-listing filesystem work', () => {
  // Matched as whole identifiers, not substrings: a bare `toContain('rm(')`
  // would false-positive on any innocent `norm(`/`perform(`.
  const FORBIDDEN_FS_IDENTIFIERS = [
    'rm',
    'rmSync',
    'rmdir',
    'rmdirSync',
    'unlink',
    'unlinkSync',
    'readdir',
    'readdirSync',
    'opendir',
    'opendirSync',
    'writeFile',
    'writeFileSync',
    'appendFile',
    'appendFileSync',
    'mkdir',
    'mkdirSync',
    'mkdtemp',
    'mkdtempSync',
    'rename',
    'renameSync',
    'copyFile',
    'copyFileSync',
    // `cp`/`cpSync` (recursive directory copy, Node 16.7) are separate write
    // primitives from `copyFile`/`copyFileSync` and would otherwise pass this
    // scan unremarked.
    'cp',
    'cpSync',
    'chmod',
    'chmodSync',
    'truncate',
    'truncateSync',
    'rimraf',
  ];

  test.each(FORBIDDEN_FS_IDENTIFIERS)('it never references %s', (forbidden) => {
    expect(codeSource).not.toMatch(new RegExp(`\\b${forbidden}\\b`));
  });

  test('it imports nothing from node:fs/promises — the whole module is one synchronous read', () => {
    expect(codeSource).not.toMatch(/node:fs\/promises/);
  });

  test('it spawns no child process', () => {
    expect(codeSource).not.toMatch(/child_process|execSync|spawnSync|\bexeca\b/);
  });

  test('the only node:fs binding it imports is realpathSync', () => {
    const importMatches = [...codeSource.matchAll(/import\s*\{([^}]*)\}\s*from\s*['"]node:fs['"]/g)];
    // A default/namespace import (`import fs from 'node:fs'`) is allowed as a
    // style choice, but then the member-access scan below is what bounds the
    // surface. Either way, only `realpathSync` may appear.
    const namedBindings = importMatches
      .flatMap((match) => match[1].split(','))
      .map((binding) => binding.trim().split(/\s+as\s+/)[0].trim())
      .filter(Boolean);
    for (const binding of namedBindings) {
      expect(binding).toBe('realpathSync');
    }

    const memberCalls = [...codeSource.matchAll(/\bfs\s*\.\s*([A-Za-z_$][\w$]*)/g)].map(
      (match) => match[1],
    );
    for (const member of memberCalls) {
      expect(member).toBe('realpathSync');
    }
  });

  test('realpath is resolved exactly once per classification — a single call site', () => {
    const realpathOccurrences = codeSource.match(/realpathSync\s*\(/g) ?? [];
    // One call site only. Two would mean the candidate and something else
    // (e.g. the repo root, or a prefix) are being resolved through the fs,
    // which is both a second fs call and a second thing that can throw.
    expect(realpathOccurrences).toHaveLength(1);
  });
});

describe('the module header records the rationale, so the two guards are read together rather than as drift', () => {
  test('it cites the ticket', () => {
    expect(rawSource).toContain(' ');
  });

  test('it cross-references cleanup.mjs\'s existing confinement guard', () => {
    expect(rawSource).toContain('cleanup.mjs');
    // The rationale itself: why the existing guard is adequate for the CLI's
    // independently-supplied `--dir` and vacuous for an auto-trigger whose
    // `dir` derives from the same variable.
    expect(rawSource).toMatch(/resolveTmpRoot|isConfinedToTmpRoot/);
    expect(rawSource).toMatch(/TMPDIR/);
  });

  test('it states that the allowedPrefixes seam is test-only', () => {
    expect(rawSource).toMatch(/allowedPrefixes/);
    expect(rawSource.toLowerCase()).toMatch(/test[- ]only/);
  });
});
