// Phase 1 — MOCKED-`node:fs` unit tests pinning the anchor's ENTIRE
// filesystem surface: exactly one `realpathSync` read per classification, and
// nothing else, ever.
//
// RED BY CONSTRUCTION: `./cleanup-temp-root-anchor.mjs` does not exist yet, so
// the dynamic `await import(...)` below rejects with `ERR_MODULE_NOT_FOUND`.
//
// WHY A SEPARATE FILE: `jest.unstable_mockModule` applies file-wide once
// registered, so a mocked `node:fs` cannot coexist with
// `./cleanup-temp-root-anchor-real-fs.jest.spec.mjs`'s real `mkdtempSync`
// scratch roots. Same split, same reason, as
// `./cleanup-adapter.jest.spec.mjs` (mocked) vs `./cleanup.jest.spec.mjs`
// (real fs) — see that file's own doc comment.
//
// WHY THE FS SURFACE IS A CONTRACT AND NOT AN IMPLEMENTATION DETAIL
//
// The anchor is the security control that makes recursive removal safe. Two
// properties follow from that, and both are structural rather than incidental:
//
//   1. NON-DESTRUCTIVE. It must never remove, list, or write anything. A guard
//      that mutates is a guard that can be the bug it exists to prevent. Every
//      destructive and listing `node:fs` binding below is mocked to a spy that
//      THROWS if called, so an accidental call is a loud failure rather than a
//      silent one.
//   2. ONE READ. The single `realpath` resolution is what makes "the path that
//      was guarded" and "the path Phase 2 walks" the same string. A second
//      resolution (of the repo root, say, or of each allowlist prefix) is both
//      a second fs call on the beat path and a second thing that can throw
//      mid-verdict. The repo-root exclusion asserted in
//      `./cleanup-async-confinement-anchor.jest.spec.mjs` must therefore be
//      derived by PATH ARITHMETIC from `import.meta.url`, not by an fs call.
//
// SAFETY: with `node:fs` mocked, this file cannot reach the real filesystem at
// all — the strongest possible form of "no test run touches the host `$TMPDIR`".

import { jest } from '@jest/globals';

const realpathSyncMock = jest.fn();

/** Every fs binding the anchor must NEVER use, mocked to fail loudly. */
const FORBIDDEN_BINDING_NAMES = [
  'readdirSync',
  'opendirSync',
  'statSync',
  'lstatSync',
  'rmSync',
  'rmdirSync',
  'unlinkSync',
  'writeFileSync',
  'appendFileSync',
  'readFileSync',
  'mkdirSync',
  'mkdtempSync',
  'renameSync',
  'copyFileSync',
  // `cpSync` is a real write primitive in its own right (recursive directory
  // copy, added in Node 16.7) and is NOT covered by `copyFileSync` above — a
  // guard that duplicated a tree would still be a guard that mutates.
  'cpSync',
  'chmodSync',
  'truncateSync',
  'existsSync',
  'accessSync',
  'openSync',
];

const forbiddenMocks = Object.fromEntries(
  FORBIDDEN_BINDING_NAMES.map((name) => [
    name,
    jest.fn(() => {
      throw new Error(`anchor called forbidden fs binding: ${name}`);
    }),
  ]),
);

const fsSurface = { realpathSync: realpathSyncMock, ...forbiddenMocks };

jest.unstable_mockModule('node:fs', () => ({
  ...fsSurface,
  // Cover a `import fs from 'node:fs'` style implementation as well as named
  // imports — the contract is about which FUNCTIONS run, not which import form
  // the implementer prefers.
  default: fsSurface,
}));

const { classifySweepRoot } = await import('./cleanup-temp-root-anchor.mjs');

beforeEach(() => {
  jest.clearAllMocks();
  // Default: resolution succeeds and lands inside a real darwin temp prefix.
  realpathSyncMock.mockImplementation((candidate) => candidate);
});

describe('the anchor makes exactly one filesystem call, and it is a realpath read', () => {
  test('an allowed candidate resolves through realpathSync exactly once', () => {
    const verdict = classifySweepRoot('/private/tmp/cdk.out-1', { platform: 'darwin' });

    expect(verdict.allowed).toBe(true);
    expect(realpathSyncMock).toHaveBeenCalledTimes(1);
    expect(realpathSyncMock).toHaveBeenCalledWith('/private/tmp/cdk.out-1');
  });

  test('a REFUSED candidate also costs exactly one realpath read — no extra probing of the refusal', () => {
    classifySweepRoot('/Users/somebody/repo/infra/cdk.out', { platform: 'darwin' });
    expect(realpathSyncMock).toHaveBeenCalledTimes(1);
  });

  test.each([
    ['an allowed candidate', '/private/tmp/cdk.out-1'],
    ['a refused candidate', '/Users/somebody/repo/infra'],
    ['an adjacent-prefix candidate', '/tmpfoo/cdk.out-1'],
  ])('%s touches no destructive or listing fs binding', (_label, candidate) => {
    classifySweepRoot(candidate, { platform: 'darwin' });

    for (const [name, mock] of Object.entries(forbiddenMocks)) {
      expect(mock).not.toHaveBeenCalled();
      // Named in the assertion so a failure says WHICH binding was called.
      expect(`${name}:${mock.mock.calls.length}`).toBe(`${name}:0`);
    }
  });

  test('an unsupported platform short-circuits without any fs call at all', () => {
    const verdict = classifySweepRoot('/private/tmp/cdk.out-1', { platform: 'win32' });

    expect(verdict.allowed).toBe(false);
    expect(verdict.reason).toBe('unsupported-platform');
    // Nothing to resolve if nothing can ever be allowed — the fail-closed path
    // must also be the cheap path, since Phase 3 calls this on a hot beat.
    expect(realpathSyncMock).not.toHaveBeenCalled();
  });

  test('a non-string candidate short-circuits without any fs call at all', () => {
    for (const candidate of [undefined, null, '', 42, {}]) {
      classifySweepRoot(candidate, { platform: 'darwin' });
    }
    expect(realpathSyncMock).not.toHaveBeenCalled();
  });

  test('a throwing realpathSync is caught and the resolve() fallback classifies the candidate', () => {
    realpathSyncMock.mockImplementation(() => {
      const error = new Error('ENOENT: no such file or directory');
      error.code = 'ENOENT';
      throw error;
    });

    const verdict = classifySweepRoot('/private/tmp/never-created/deeper', { platform: 'darwin' });

    expect(realpathSyncMock).toHaveBeenCalledTimes(1);
    expect(verdict.allowed).toBe(true);
    expect(verdict.resolvedDir).toBe('/private/tmp/never-created/deeper');
  });

  test('a realpathSync that resolves OUT of the temp root is refused on the resolved value, not the input', () => {
    realpathSyncMock.mockReturnValue('/Users/somebody/repo');

    const verdict = classifySweepRoot('/private/tmp/link-to-repo', { platform: 'darwin' });

    expect(verdict.allowed).toBe(false);
    expect(verdict.reason).toBe('outside-system-temp-root');
    expect(verdict.resolvedDir).toBe('/Users/somebody/repo');
  });

  test('a realpathSync that throws a non-Error value still does not escape', () => {
    realpathSyncMock.mockImplementation(() => {
      throw 'a bare string, as some native bindings manage to';
    });

    let verdict;
    expect(() => {
      verdict = classifySweepRoot('/private/tmp/cdk.out-1', { platform: 'darwin' });
    }).not.toThrow();
    expect(verdict.allowed).toBe(true);
  });

  test('the injected allowedPrefixes seam does not add fs calls of its own', () => {
    classifySweepRoot('/scratch/cdk.out-1', { allowedPrefixes: ['/scratch'], platform: 'darwin' });
    expect(realpathSyncMock).toHaveBeenCalledTimes(1);
    for (const mock of Object.values(forbiddenMocks)) {
      expect(mock).not.toHaveBeenCalled();
    }
  });
});
