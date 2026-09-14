// Phase 1 — the env-independent system-temp-root anchor.
//
// RED BY CONSTRUCTION: `./cleanup-temp-root-anchor.mjs` DOES NOT EXIST yet.
// The import below fails module resolution with `ERR_MODULE_NOT_FOUND`; that
// is this file's documented red state, not a typo or a config miss. Do not add
// a stub to turn it green — Phase 1's implementer builds the real module.
//
// ---------------------------------------------------------------------------
// WHY THIS MODULE EXISTS (rev-2 Build Plan, critical finding 1)
// ---------------------------------------------------------------------------
//
// `cleanup.mjs`'s existing confinement guard compares `--dir` against
// `resolveTmpRoot()` (`cleanup.mjs:236`), which is itself
// `process.env.TMPDIR || os.tmpdir()`. That is a sound yardstick for the
// standalone CLI, whose `--dir` is supplied INDEPENDENTLY of `TMPDIR`. It is
// VACUOUS for its auto-trigger, whose `dir` derives from the SAME env var:
// with `TMPDIR=/Users/…/example-project/infra`, both sides of the comparison move
// together, "confinement" passes, and the sweep recursively removes the repo's
// real `infra/cdk.out`. A RELATIVE `TMPDIR` compounds it — it resolves against
// `process.cwd()`, which the PreToolUse hook pins to `REPO_ROOT`
// (`hooks/pretooluse-arm-gate.mjs:297`).
//
// So this module's yardstick is compiled from `process.platform` alone.
// `TMPDIR` becomes an INPUT TO BE VALIDATED, never the standard it is
// validated against. This file's central assertion is therefore a NEGATIVE
// one: the verdict does not move when `process.env.TMPDIR` moves.
//
// ---------------------------------------------------------------------------
// CONTRACT UNDER TEST (fixed by the plan; factoring is the implementer's call)
// ---------------------------------------------------------------------------
//
//   export const SYSTEM_TEMP_ROOT_PREFIXES  // frozen, platform-keyed map
//   export function classifySweepRoot(candidateDir, {
//     platform = process.platform,
//     allowedPrefixes,                      // TEST-ONLY injection seam
//   } = {}) -> {
//     allowed: boolean,
//     resolvedDir: string,
//     reason: null | 'outside-system-temp-root' | 'unresolvable'
//           | 'unsupported-platform',
//     resolvedVia: null | 'realpath' | 'fallback',
//   }
//
// `resolvedVia` is an ADDITIVE extension to the plan's three-field shape (the
// plan's `{ allowed, resolvedDir, reason }` keep their exact semantics). It
// exists because `resolvedDir` is only a REAL location on the realpath-success
// branch: `realpathSync` throws ENOENT on a missing final component, and the
// `resolve()` fallback then keeps the candidate's written form with its
// symlinks UNRESOLVED — so `/private/tmp/link/absent`, where `link` points at
// the working tree, is `allowed: true` with a `resolvedDir` that does not say
// where it actually goes. PHASE 2 MUST REQUIRE `resolvedVia === 'realpath'`
// BEFORE WALKING `resolvedDir`; a root that could not be realpath-resolved has
// a missing component and so nothing to reclaim, which is why that costs no
// capability.
//
// ---------------------------------------------------------------------------
// SAFETY — this file never touches the real `$TMPDIR`'s contents
// ---------------------------------------------------------------------------
//
// The host `$TMPDIR` holds 3,000+ real `cdk.out` dirs that must never be
// touched by any test run (see `./cleanup.jest.spec.mjs:86-99`'s doc comment).
// This file is safe on two independent grounds:
//
//   1. The module under test is NON-DESTRUCTIVE by contract — no `rm`, no
//      `readdir`, no writes, one `realpath` read. `./cleanup-temp-root-anchor-
//      fs-surface.jest.spec.mjs` proves that mechanically against a mocked
//      `node:fs`, and `-source-scan` proves it statically.
//   2. Every path this file passes in either does not exist at all
//      (deliberately — so `realpathSync` throws and the `resolve()` fallback
//      is the code path exercised) or is a `fs.mkdtempSync` scratch root this
//      file created and tears down itself.
//
// `process.env.TMPDIR` IS mutated in one describe block below — that is the
// only way to prove env-independence from inside the process — and is restored
// unconditionally in `afterEach`. That is safe here precisely because the unit
// under test cannot delete anything; NO destructive path may ever consume an
// in-process `TMPDIR` override (Phases 2 and 3 use the per-child
// `spawnSync(..., { env: { ...process.env, TMPDIR: scratch } })` idiom instead).

import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  SYSTEM_TEMP_ROOT_PREFIXES,
  classifySweepRoot,
} from './cleanup-temp-root-anchor.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
// lib -> agent-resource-management -> scripts -> repo root.
const REPO_ROOT = resolve(HERE, '..', '..', '..');

/** The exact prefix lists the plan fixes, restated here so a silent widening of the allowlist fails. */
const EXPECTED_DARWIN_PREFIXES = ['/private/tmp', '/private/var/folders', '/tmp', '/var/folders'];
const EXPECTED_LINUX_PREFIXES = ['/tmp', '/var/tmp'];

/**
 * A path that is INSIDE `prefix` but does not exist on any host, so
 * `realpathSync` is guaranteed to throw and the `resolve()` fallback is what
 * classifies it. Used for every platform-parameterised cell: a REAL path would
 * be realpath-resolved by the host's own filesystem (on macOS `/tmp` becomes
 * `/private/tmp`, `/var/tmp` becomes `/private/var/tmp`), which would make a
 * `platform: 'linux'` cell fail for reasons that have nothing to do with the
 * logic under test.
 */
function nonExistentUnder(prefix, label) {
  return `${prefix}/arm-anchor-absent-${label}-${process.pid}`;
}

/** Every scratch root this file created, torn down unconditionally. */
let scratchRoots;
let savedTmpdir;
let savedTmpdirWasSet;

beforeEach(() => {
  scratchRoots = [];
  savedTmpdirWasSet = 'TMPDIR' in process.env;
  savedTmpdir = process.env.TMPDIR;
});

afterEach(() => {
  // Restore the process's real `$TMPDIR` before anything else — a leaked
  // override is the one way this file could endanger a later spec.
  if (savedTmpdirWasSet) {
    process.env.TMPDIR = savedTmpdir;
  } else {
    delete process.env.TMPDIR;
  }
  for (const root of scratchRoots) {
    rmSync(root, { recursive: true, force: true });
  }
});

/** A real, own, throwaway scratch directory — the only real directories this file creates. */
function makeScratchRoot(label) {
  const root = mkdtempSync(join(tmpdir(), `arm-anchor-${label}-`));
  scratchRoots.push(root);
  return realpathSync(root);
}

// ===========================================================================
// SYSTEM_TEMP_ROOT_PREFIXES — the allowlist itself
// ===========================================================================

describe('SYSTEM_TEMP_ROOT_PREFIXES — a frozen, platform-keyed allowlist', () => {
  test('darwin lists exactly the four real macOS temp roots, in both written forms', () => {
    expect([...SYSTEM_TEMP_ROOT_PREFIXES.darwin].sort()).toEqual(
      [...EXPECTED_DARWIN_PREFIXES].sort(),
    );
  });

  test('linux lists exactly /tmp and /var/tmp', () => {
    expect([...SYSTEM_TEMP_ROOT_PREFIXES.linux].sort()).toEqual([...EXPECTED_LINUX_PREFIXES].sort());
  });

  test('the map and each of its lists are frozen, so no caller can widen the allowlist at runtime', () => {
    expect(Object.isFrozen(SYSTEM_TEMP_ROOT_PREFIXES)).toBe(true);
    for (const list of Object.values(SYSTEM_TEMP_ROOT_PREFIXES)) {
      expect(Object.isFrozen(list)).toBe(true);
    }
  });

  test('an attempt to widen the allowlist in place is inert — the classifier still refuses', () => {
    const outsider = '/Users/arm-not-a-temp-root';
    try {
      SYSTEM_TEMP_ROOT_PREFIXES.darwin.push(outsider);
    } catch {
      // A frozen array throws in strict mode (ESM is always strict) — either
      // outcome is fine; what matters is that the verdict below is unchanged.
    }
    expect(classifySweepRoot(`${outsider}/x`, { platform: 'darwin' }).allowed).toBe(false);
  });

  test('an unrecognised platform contributes no prefixes at all — everything is refused (fail-closed on the destructive path)', () => {
    for (const platform of ['win32', 'sunos', 'aix', 'haiku', '']) {
      const prefixes = SYSTEM_TEMP_ROOT_PREFIXES[platform] ?? [];
      expect(prefixes).toHaveLength(0);
    }
  });
});

// ===========================================================================
// THE CENTRAL PROPERTY — the verdict never moves when `TMPDIR` moves
// ===========================================================================

describe('classifySweepRoot reaches its verdict without reading process.env', () => {
  test('a repo path is refused even when process.env.TMPDIR points straight at it', () => {
    const repoPath = join(REPO_ROOT, 'infra');
    // The exact rev-2 critical finding: the vacuous guard would compare this
    // path against a tmp root derived from the same variable and pass.
    process.env.TMPDIR = REPO_ROOT;

    const verdict = classifySweepRoot(repoPath);

    expect(verdict.allowed).toBe(false);
    expect(verdict.reason).toBe('outside-system-temp-root');
  });

  test('the repo root itself is refused when TMPDIR points at it', () => {
    process.env.TMPDIR = REPO_ROOT;
    expect(classifySweepRoot(REPO_ROOT).allowed).toBe(false);
  });

  test('a real temp scratch root stays allowed even when TMPDIR points at the repo', () => {
    const scratch = makeScratchRoot('env-independent-allow');
    process.env.TMPDIR = REPO_ROOT;

    const verdict = classifySweepRoot(scratch);

    expect(verdict.allowed).toBe(true);
    expect(verdict.reason).toBeNull();
  });

  test('deleting TMPDIR entirely changes nothing — the allowlist is not derived from it', () => {
    const scratch = makeScratchRoot('env-unset');
    const withEnv = classifySweepRoot(scratch);
    delete process.env.TMPDIR;
    const withoutEnv = classifySweepRoot(scratch);
    expect(withoutEnv).toEqual(withEnv);
  });

  test('a relative TMPDIR-style candidate resolves against cwd and is refused', () => {
    // The PreToolUse hook pins cwd to REPO_ROOT, so a relative `TMPDIR`
    // resolves INSIDE the repo. Premise assertion first: this test only means
    // something if the cwd it resolves against is itself refused.
    expect(classifySweepRoot(process.cwd()).allowed).toBe(false);
    process.env.TMPDIR = 'tmp';

    for (const relativeCandidate of ['tmp', './tmp', '../tmp', 'infra/cdk.out']) {
      const verdict = classifySweepRoot(relativeCandidate);
      expect(verdict.allowed).toBe(false);
      expect(verdict.reason).toBe('outside-system-temp-root');
      expect(verdict.resolvedDir).toBe(resolve(process.cwd(), relativeCandidate));
    }
  });
});

// ===========================================================================
// THE RETURNED RESOLVED PATH — the caller walks exactly what was guarded
// ===========================================================================

describe('classifySweepRoot returns the single resolved path it guarded', () => {
  test('an allowed real scratch root comes back fully resolved', () => {
    const scratch = makeScratchRoot('resolved-dir');
    expect(classifySweepRoot(scratch).resolvedDir).toBe(realpathSync(scratch));
  });

  test('a refused candidate still carries a string resolvedDir, so the refusal can be reported', () => {
    const verdict = classifySweepRoot(join(REPO_ROOT, 'infra'));
    expect(typeof verdict.resolvedDir).toBe('string');
    expect(verdict.resolvedDir).toBe(join(REPO_ROOT, 'infra'));
  });

  test('trailing-slash and doubled-separator forms normalise to the same allowed resolvedDir', () => {
    const scratch = makeScratchRoot('separator-forms');
    const canonical = classifySweepRoot(scratch);
    expect(canonical.allowed).toBe(true);

    const equivalentForms = [
      `${scratch}/`,
      `${scratch}//`,
      scratch.replace(sep, `${sep}${sep}`),
      join(scratch, '.'),
      join(scratch, 'child', '..'),
    ];

    for (const form of equivalentForms) {
      const verdict = classifySweepRoot(form);
      expect(verdict.allowed).toBe(true);
      expect(verdict.reason).toBeNull();
      // Normalised: no trailing separator, no doubled separator — the value
      // Phase 2 will hand to `selectStaleTempDirs`, whose `basename()` splits
      // on a hard-coded `/` and silently yields `''` for a trailing form.
      expect(verdict.resolvedDir).toBe(canonical.resolvedDir);
      expect(verdict.resolvedDir.endsWith(sep)).toBe(false);
      expect(verdict.resolvedDir).not.toContain(`${sep}${sep}`);
    }
  });

  test('the written prefix forms themselves normalise consistently on the declared platform', () => {
    for (const prefix of ['/private/tmp/', '/private//tmp', '/private/tmp/./x']) {
      const verdict = classifySweepRoot(prefix, { platform: 'darwin' });
      expect(verdict.allowed).toBe(true);
      expect(verdict.resolvedDir.endsWith(sep)).toBe(false);
      expect(verdict.resolvedDir).not.toContain(`${sep}${sep}`);
    }
  });
});

// ===========================================================================
// `resolvedVia` — WHICH BRANCH PRODUCED `resolvedDir`, SO PHASE 2 CAN GATE
// ===========================================================================

describe('resolvedVia distinguishes the realpath branch from the resolve() fallback', () => {
  test('a real, existing scratch root reports realpath — the only value Phase 2 may walk', () => {
    const scratch = makeScratchRoot('resolved-via-realpath');
    const verdict = classifySweepRoot(scratch);

    expect(verdict.allowed).toBe(true);
    expect(verdict.resolvedVia).toBe('realpath');
    expect(verdict.resolvedDir).toBe(realpathSync(scratch));
  });

  test('a candidate with a missing component reports fallback, even when it is allowed', () => {
    const scratch = makeScratchRoot('resolved-via-fallback');
    const absent = join(scratch, 'never-created', 'deeper');

    const verdict = classifySweepRoot(absent);

    // The pairing that matters: `allowed: true` does NOT imply the path is safe
    // to walk. `realpathSync` threw on the missing component, so every symlink
    // in `resolvedDir` is unresolved and Phase 2 must decline to walk it.
    expect(verdict.allowed).toBe(true);
    expect(verdict.resolvedVia).toBe('fallback');
  });

  test('the refusals that never reach the filesystem report a null resolvedVia, matching their empty resolvedDir', () => {
    for (const [candidate, options] of [
      ['/private/tmp/x', { platform: 'win32' }],
      [undefined, {}],
      ['', {}],
      [42, {}],
      ['/tmp/a\u0000b', {}],
    ]) {
      const verdict = classifySweepRoot(candidate, options);
      expect(verdict.allowed).toBe(false);
      expect(verdict.resolvedDir).toBe('');
      // `null`, never `'fallback'`: nothing was resolved at all, and a caller
      // must not be able to read a resolution mode off a verdict that has none.
      expect(verdict.resolvedVia).toBeNull();
    }
  });

  test('a REFUSED but resolvable candidate still reports how it was resolved, so the refusal is diagnosable', () => {
    const verdict = classifySweepRoot(REPO_ROOT);
    expect(verdict.allowed).toBe(false);
    expect(verdict.resolvedVia).toBe('realpath');
  });

  test('every verdict carries the field — it is part of the shape, not an occasional extra', () => {
    for (const candidate of [undefined, '', '/tmpfoo', REPO_ROOT, '/private/tmp']) {
      expect(classifySweepRoot(candidate)).toHaveProperty('resolvedVia');
    }
  });
});

// ===========================================================================
// PREFIX-ADJACENCY — `/tmpfoo` must NEVER match `/tmp`
// ===========================================================================

describe('the boundary comparison is path.sep-aware, not a bare startsWith', () => {
  const ADJACENT_DARWIN = [
    '/tmpfoo',
    '/tmpfoo/cdk.out-1',
    '/tmp-evil/cdk.out-1',
    '/private/tmpfoo',
    '/private/tmpfoo/cdk.out-1',
    '/var/foldersX',
    '/var/foldersX/cdk.out-1',
    '/private/var/foldersX/cdk.out-1',
    '/tmpX',
  ];

  test.each(ADJACENT_DARWIN)('refuses the prefix-adjacent path %s on darwin', (candidate) => {
    const verdict = classifySweepRoot(candidate, { platform: 'darwin' });
    expect(verdict.allowed).toBe(false);
    expect(verdict.reason).toBe('outside-system-temp-root');
  });

  test.each(['/tmpfoo/cdk.out-1', '/var/tmpfoo/cdk.out-1', '/tmp-evil'])(
    'refuses the prefix-adjacent path %s on linux',
    (candidate) => {
      const verdict = classifySweepRoot(candidate, { platform: 'linux' });
      expect(verdict.allowed).toBe(false);
      expect(verdict.reason).toBe('outside-system-temp-root');
    },
  );

  test('the exact prefix itself, and a child of it, ARE allowed — the adjacency rule is not over-broad', () => {
    for (const prefix of EXPECTED_DARWIN_PREFIXES) {
      expect(classifySweepRoot(nonExistentUnder(prefix, 'child'), { platform: 'darwin' }).allowed).toBe(
        true,
      );
    }
    for (const prefix of EXPECTED_LINUX_PREFIXES) {
      expect(classifySweepRoot(nonExistentUnder(prefix, 'child'), { platform: 'linux' }).allowed).toBe(
        true,
      );
    }
  });
});

// ===========================================================================
// NEVER THROWS
// ===========================================================================

describe('classifySweepRoot never throws, for any input', () => {
  const HOSTILE_INPUTS = [
    ['undefined', undefined],
    ['null', null],
    ['the empty string', ''],
    ['a number', 42],
    ['a boolean', true],
    ['a plain object', {}],
    ['an array', []],
    ['a function', () => '/tmp'],
    // A NUL byte is `unresolvable` by the anchor's own DECISION, not because
    // `path.resolve` rejects it. Verified on Node 22.23.0: `path.resolve` is
    // platform-independent string arithmetic and happily RETURNS the
    // NUL-bearing absolute path; it is `fs.realpathSync` — and every other
    // `node:fs` call — that throws `ERR_INVALID_ARG_VALUE` on it. So a
    // NUL-bearing path is a string the anchor could classify but whose verdict
    // would be useless: handing it to Phase 2 as an `allowed` sweep root would
    // hand Phase 2 a path it can only throw on. The anchor rejects it up front
    // instead, which also means it costs no filesystem call.
    //
    // Written as a `\u0000` escape rather than a raw control byte so the file
    // stays greppable.
    ['a NUL-embedded string', '/tmp/a\u0000b'],
    // Whitespace-only IS a legal `path.resolve` argument, so it resolves
    // against cwd and is refused as OUTSIDE, not as unresolvable — which is
    // why it is absent from the reason-specific list below. "Never throws"
    // must still hold for it.
    ['a whitespace-only string', '   '],
  ];

  test.each(HOSTILE_INPUTS)('returns a refusal rather than throwing for %s', (_label, candidate) => {
    let verdict;
    expect(() => {
      verdict = classifySweepRoot(candidate);
    }).not.toThrow();
    expect(verdict.allowed).toBe(false);
    expect(typeof verdict.resolvedDir).toBe('string');
    expect(typeof verdict.reason).toBe('string');
  });

  test.each([
    ['undefined', undefined],
    ['null', null],
    ['the empty string', ''],
    ['a number', 42],
    ['a plain object', {}],
    ['a NUL-embedded string', '/tmp/a\u0000b'],
  ])('reports %s specifically as unresolvable', (_label, candidate) => {
    expect(classifySweepRoot(candidate).reason).toBe('unresolvable');
  });

  test('a hostile options object never throws either', () => {
    expect(() => classifySweepRoot('/tmp/x', {})).not.toThrow();
    expect(() => classifySweepRoot('/tmp/x', { platform: undefined })).not.toThrow();
    expect(() => classifySweepRoot('/tmp/x', { platform: null }).allowed).not.toThrow();
    expect(() => classifySweepRoot('/tmp/x', { allowedPrefixes: undefined })).not.toThrow();
    expect(classifySweepRoot('/tmp/x', { platform: null }).allowed).toBe(false);
  });
});

// ===========================================================================
// A NON-EXISTENT CANDIDATE INSIDE A TEMP PREFIX IS ALLOWED
// ===========================================================================

describe('a candidate that does not exist falls back to resolve() and is classified normally', () => {
  test('a non-existent path inside a real temp prefix is allowed', () => {
    const scratch = makeScratchRoot('absent-child');
    const absent = join(scratch, 'never-created', 'deeper');

    const verdict = classifySweepRoot(absent);

    // Allowed, not `unresolvable`: Phase 2's listing step is what reports
    // `listing-failed` for a path that is not there. Confinement is a
    // question about WHERE the path is, not whether it exists.
    expect(verdict.allowed).toBe(true);
    expect(verdict.reason).toBeNull();
    expect(verdict.resolvedDir).toBe(absent);
  });

  test('a non-existent path outside every temp prefix is refused as outside, not as unresolvable', () => {
    const verdict = classifySweepRoot(join(REPO_ROOT, 'no-such-dir-1691', 'deeper'));
    expect(verdict.allowed).toBe(false);
    expect(verdict.reason).toBe('outside-system-temp-root');
  });
});

// ===========================================================================
// THE `allowedPrefixes` INJECTION SEAM
// ===========================================================================

describe('allowedPrefixes is a test-only injection seam', () => {
  test('an injected prefix allows a path beneath it', () => {
    const scratch = makeScratchRoot('injected-allow');
    const verdict = classifySweepRoot(join(scratch, 'cdk.out-1'), { allowedPrefixes: [scratch] });
    expect(verdict.allowed).toBe(true);
    expect(verdict.reason).toBeNull();
  });

  test('an injected prefix does NOT widen to a sibling adjacent to it', () => {
    const scratch = makeScratchRoot('injected-adjacent');
    const verdict = classifySweepRoot(`${scratch}foo/cdk.out-1`, { allowedPrefixes: [scratch] });
    expect(verdict.allowed).toBe(false);
    expect(verdict.reason).toBe('outside-system-temp-root');
  });

  test('an injected prefix REPLACES the platform list rather than adding to it', () => {
    const scratch = makeScratchRoot('injected-replaces');
    // `/private/tmp` is on the real darwin list; with an explicit injection it
    // must no longer be honoured, or the seam would only ever be a widening
    // mechanism — and a widening seam on a destructive path is a liability.
    const verdict = classifySweepRoot(nonExistentUnder('/private/tmp', 'replaced'), {
      allowedPrefixes: [scratch],
      platform: 'darwin',
    });
    expect(verdict.allowed).toBe(false);
    expect(verdict.reason).toBe('outside-system-temp-root');
  });

  test('an empty injected list refuses everything', () => {
    const scratch = makeScratchRoot('injected-empty');
    const verdict = classifySweepRoot(scratch, { allowedPrefixes: [] });
    expect(verdict.allowed).toBe(false);
    // Both refusal reasons are fail-closed and equally defensible here (the
    // platform IS supported, but no prefix is on offer); the ordering is
    // deliberately NOT pinned, only the refusal.
    expect(['outside-system-temp-root', 'unsupported-platform']).toContain(verdict.reason);
  });

  test('an injected LIST OF JUNK entries never throws and never allows', () => {
    // A list was supplied, so it governs — and none of its entries is a usable
    // prefix, so nothing can match. Refusal, not a silent fall-back to the
    // platform list: a caller that passed a list meant to narrow the seam.
    for (const allowedPrefixes of [[null], [''], [42], [{}], [undefined]]) {
      let verdict;
      expect(() => {
        verdict = classifySweepRoot('/tmp/arm-hostile-seam', { allowedPrefixes });
      }).not.toThrow();
      expect(verdict.allowed).toBe(false);
    }
  });

  test('a nullish seam falls back to the platform list, and a malformed one is never MORE permissive than that', () => {
    // CANDIDATE CHOICE IS LOAD-BEARING — it must be REFUSED by the platform
    // default, or the widening assertion is VACUOUS. An earlier revision used a
    // path under `/tmp`, which the platform list already ALLOWS: every
    // "malformed value did not widen" check then reduced to `true`, and a real
    // widening mutant (malformed -> fall through to a permissive list) would
    // have survived undetected. An invented, non-temp absolute root is outside
    // every platform prefix AND outside the repo, so the only way a malformed
    // seam can make it `allowed` is by widening — the direction under test.
    // Nothing is created here; the module only classifies, it never writes.
    const candidate = `/arm-not-a-temp-root-${process.pid}/seam-fallback`;
    const platformDefault = classifySweepRoot(candidate);

    // Pinned explicitly so this cell cannot silently regress to vacuity if the
    // candidate or the platform list is ever edited.
    expect(platformDefault.allowed).toBe(false);
    expect(platformDefault.reason).toBe('outside-system-temp-root');

    // Nullish is the ordinary production case — the seam is simply absent.
    expect(classifySweepRoot(candidate, { allowedPrefixes: undefined })).toEqual(platformDefault);
    expect(classifySweepRoot(candidate, { allowedPrefixes: null })).toEqual(platformDefault);

    // A malformed non-array value is the implementer's call to reject or to
    // treat as absent; what is NOT their call is to let it widen anything.
    for (const allowedPrefixes of ['not-an-array', 42, true, {}, () => ['/']]) {
      let verdict;
      expect(() => {
        verdict = classifySweepRoot(candidate, { allowedPrefixes });
      }).not.toThrow();
      expect(verdict.allowed).toBe(false);
      expect(verdict.reason).toBe('outside-system-temp-root');
    }
  });

  test('a malformed seam can never rescue a candidate inside the repo either', () => {
    // A SEPARATE DIRECTION from the cell above, kept separate because it proves
    // something narrower: the repo-root exclusion runs BEFORE the prefix check,
    // so this path is refused however the prefix list was chosen — it never
    // reaches the allowlist at all. That makes it useless as a widening probe
    // (hence the invented non-temp root above) but worth pinning on its own.
    for (const allowedPrefixes of ['/', ['/'], 42, {}]) {
      expect(
        classifySweepRoot(join(REPO_ROOT, 'infra', 'cdk.out'), { allowedPrefixes }).allowed,
      ).toBe(false);
    }
  });

  test("a '/' prefix reaches root ITSELF but cannot widen to paths UNDER root", () => {
    // Pins the documented behaviour that the module header and `isWithin`'s doc
    // comment describe, so the prose and the code cannot drift apart again.
    //
    // The `sep`-append in `isWithin` makes the comparison string `'//'`, which
    // no resolved path starts with — so a `'/'` prefix cannot widen the
    // allowlist to cover anything BENEATH root. Root itself, however, matches
    // the `candidatePath === boundary` arm and IS reachable as `resolvedDir`.
    //
    // Unreachable in production: `SYSTEM_TEMP_ROOT_PREFIXES` never contains
    // `'/'` and the seam is test-only. Phase 2 must NOT rely on this module to
    // refuse `/` as a sweep root — confinement plus the `cdk.out-*` candidate
    // NAME filter are what bound the sweep.
    expect(classifySweepRoot('/', { allowedPrefixes: ['/'] })).toEqual({
      allowed: true,
      resolvedDir: '/',
      reason: null,
      resolvedVia: 'realpath',
    });

    // …but nothing under root is widened by that same prefix.
    expect(classifySweepRoot('/etc', { allowedPrefixes: ['/'] }).allowed).toBe(false);

    // And the PLATFORM default — the only list production ever uses — refuses
    // root outright.
    const platformVerdict = classifySweepRoot('/');
    expect(platformVerdict.allowed).toBe(false);
    expect(platformVerdict.reason).toBe('outside-system-temp-root');
  });
});

// ===========================================================================
// THE REPO IS NEVER A SWEEP ROOT — INCLUDING THE CI CASE
// ===========================================================================

describe('the repo root, and any path under it, is refused', () => {
  test('the repo root and representative paths under it are refused on the real platform', () => {
    const repoPaths = [
      REPO_ROOT,
      join(REPO_ROOT, 'infra'),
      join(REPO_ROOT, 'infra', 'cdk.out'),
      join(REPO_ROOT, 'scripts', 'agent-resource-management'),
      HERE,
    ];
    for (const candidate of repoPaths) {
      const verdict = classifySweepRoot(candidate);
      expect(verdict.allowed).toBe(false);
      expect(verdict.reason).toBe('outside-system-temp-root');
    }
  });

  test('the repo is refused EVEN WHEN its parent is nominated as an allowed prefix', () => {
    // A CI runner (or a developer working inside a temp-mounted checkout) can
    // legitimately have the repo underneath a real temp root. Prefix logic
    // alone would then ALLOW recursive removal inside the working tree — the
    // single worst outcome this module exists to prevent.
    //
    // SCOPE OF THIS TEST, STATED HONESTLY. Declaring the repo's parent via the
    // `allowedPrefixes` seam is NOT mechanically identical to "the repo lives
    // under an allowed temp root" — an earlier revision of this comment claimed
    // it was, and that claim hid a real bypass. The seam REPLACES the platform
    // list, so this cell never exercises a PLATFORM prefix interacting with the
    // exclusion, which is exactly where the case-variant bypass lived (a path
    // keeping its literal `/private/tmp` prefix, passing the allowlist, and
    // missing the exclusion on spelling). What this cell does prove is narrower
    // and still worth proving: the exclusion is not merely a side effect of the
    // repo happening to sit outside every prefix.
    //
    // The genuine CI arrangement — a repo-shaped tree under a REAL platform
    // prefix — needs a copy of the module loaded from an `fs.mkdtempSync` tree,
    // and is asserted in `./cleanup-temp-root-anchor-real-fs.jest.spec.mjs`.
    //
    // IMPLEMENTER NOTE: satisfying this needs an explicit repo-root exclusion
    // in addition to the prefix check — derive the repo root from
    // `import.meta.url` by PATH ARITHMETIC ONLY (no extra fs call; the single
    // `realpath` budget is asserted in `-fs-surface`). `outside-system-temp-
    // root` is the right reason code: the repo is not a system temp root, and
    // the plan fixes the reason union at four values.
    const repoParent = dirname(REPO_ROOT);

    for (const candidate of [REPO_ROOT, join(REPO_ROOT, 'infra', 'cdk.out'), HERE]) {
      const verdict = classifySweepRoot(candidate, { allowedPrefixes: [repoParent] });
      expect(verdict.allowed).toBe(false);
      expect(verdict.reason).toBe('outside-system-temp-root');
    }

    // Premise guard: the injection really did make that prefix allowable, so
    // the refusals above are the exclusion doing work, not a dud fixture.
    const siblingOfRepo = join(repoParent, 'arm-not-the-repo', 'cdk.out-1');
    expect(classifySweepRoot(siblingOfRepo, { allowedPrefixes: [repoParent] }).allowed).toBe(true);
  });

  test('a CASE-VARIANT spelling of a repo path is refused — on a case-insensitive volume it names the same directory', () => {
    // THE BYPASS THIS CELL EXISTS FOR. `realpathSync` canonicalises symlinks,
    // not the CASE of components it never had to traverse, and `path.resolve`
    // is byte-faithful. On APFS (case-insensitive by default) and NTFS,
    // `<parent>/EXAMPLE-PROJECT/infra` and `<parent>/example-project/infra` are ONE
    // directory on disk while comparing unequal as strings — so an exact-case
    // exclusion refuses the first spelling and ALLOWS the second, which is a
    // recursive-removal target inside the working tree. Reproduced under a real
    // `/private/tmp` prefix on Node 22.23.0.
    //
    // The candidate derives from the TMPDIR variable, whose spelling the
    // session chooses freely, so this is inside the module's own threat model.
    //
    // Only the repo's LAST component is re-spelled: the parent must stay
    // byte-identical so it still matches the nominated prefix. That is what
    // makes this a test of the EXCLUSION rather than of the allowlist — and the
    // allowlist must stay case-SENSITIVE, since folding it would widen the
    // destructive surface instead of narrowing it.
    const repoParent = dirname(REPO_ROOT);
    const repoName = basename(REPO_ROOT);

    const caseVariants = [repoName.toUpperCase(), repoName.toLowerCase()].filter(
      (name) => name !== repoName,
    );
    // A repo directory whose name has no case variants at all would make this
    // cell vacuous, so say so rather than passing silently.
    expect(caseVariants.length).toBeGreaterThan(0);

    for (const variantName of caseVariants) {
      for (const tail of [[], ['infra'], ['infra', 'cdk.out']]) {
        const candidate = join(repoParent, variantName, ...tail);
        const verdict = classifySweepRoot(candidate, { allowedPrefixes: [repoParent] });
        expect(verdict.allowed).toBe(false);
        expect(verdict.reason).toBe('outside-system-temp-root');
      }
    }
  });
});

// ===========================================================================
// THE FOLD ASYMMETRY — FOLD THE EXCLUSION, NEVER THE ALLOWLIST
// ===========================================================================
//
// `foldForRepoExclusion` (NFC + `toLowerCase`) is applied to the REPO-ROOT
// EXCLUSION only. The cells above prove the exclusion IS folded. These cells
// prove the other half of the asymmetry: the PREFIX ALLOWLIST is NOT.
//
// WHY THE ASYMMETRY IS THE WHOLE POINT. Folding a comparison can only ever make
// MORE paths match. On the exclusion that means more REFUSALS — safe. On the
// allowlist it means more paths matching an allowed prefix, i.e. a WIDENING of
// the recursive-removal surface, in the exact direction this module exists to
// prevent. `foldForRepoExclusion`'s doc comment warns a future editor off it in
// prose; prose is not a gate, so this is the gate.
//
// THE MUTATION THESE CELLS CATCH, stated so a future reader can re-run it:
// change the prefix comparison in `classifySweepRoot` from
//   isWithin(resolvedDir, prefix)
// to
//   isWithin(foldForRepoExclusion(resolvedDir), foldForRepoExclusion(prefix))
// and every candidate below flips to `allowed: true`. Before these cells
// existed the entire ARM suite passed byte-identically under that mutation.
//
// Every candidate is NON-EXISTENT (see `nonExistentUnder`): a case variant that
// really exists on a case-insensitive volume is realpath-resolved to its
// canonical lowercase spelling by the KERNEL, which is correct behaviour and not
// the thing under test. It is the FALLBACK branch — the written form, spelled as
// the session chose — where an unfolded allowlist is load-bearing.

describe('the prefix allowlist is NOT folded — the asymmetry with the repo exclusion is pinned, not just documented', () => {
  test.each([
    ['/TMP', 'fold-darwin-tmp'],
    ['/Tmp', 'fold-darwin-tmp-mixed'],
    ['/Private/Tmp', 'fold-darwin-private-tmp'],
    ['/PRIVATE/VAR/FOLDERS', 'fold-darwin-folders'],
    ['/VAR/FOLDERS', 'fold-darwin-var-folders'],
  ])(
    'darwin: a case variant of the allowed prefix %s is REFUSED (folding it would widen)',
    (prefix, label) => {
      const verdict = classifySweepRoot(nonExistentUnder(prefix, label), { platform: 'darwin' });
      expect(verdict.allowed).toBe(false);
      expect(verdict.reason).toBe('outside-system-temp-root');
    },
  );

  test.each([
    ['/TMP', 'fold-linux-tmp'],
    ['/Tmp', 'fold-linux-tmp-mixed'],
    ['/VAR/TMP', 'fold-linux-var-tmp'],
    ['/Var/Tmp', 'fold-linux-var-tmp-mixed'],
  ])(
    'linux: a case variant of the allowed prefix %s is REFUSED (folding it would widen)',
    (prefix, label) => {
      const verdict = classifySweepRoot(nonExistentUnder(prefix, label), { platform: 'linux' });
      expect(verdict.allowed).toBe(false);
      expect(verdict.reason).toBe('outside-system-temp-root');
    },
  );

  test('premise guard: the exact-case spelling of each of those prefixes IS allowed', () => {
    // Otherwise the refusals above could be passing for some unrelated reason
    // (a typo'd prefix, say) rather than because the allowlist stayed
    // case-sensitive.
    for (const prefix of EXPECTED_DARWIN_PREFIXES) {
      expect(
        classifySweepRoot(nonExistentUnder(prefix, 'fold-premise-darwin'), { platform: 'darwin' })
          .allowed,
      ).toBe(true);
    }
    for (const prefix of EXPECTED_LINUX_PREFIXES) {
      expect(
        classifySweepRoot(nonExistentUnder(prefix, 'fold-premise-linux'), { platform: 'linux' })
          .allowed,
      ).toBe(true);
    }
  });

  test('the allowlist is not NFC-folded either — an NFD candidate does not match its NFC prefix', () => {
    // The fold does Unicode normalisation as well as case, so the asymmetry has
    // two halves and both need pinning. The platform prefixes are all ASCII and
    // so have no NFD variant; the seam is the only way to get a non-ASCII prefix
    // in front of this comparison. Both forms are DERIVED, never typed as
    // literals — an editor or a normalising transport silently rewrites a
    // decomposed literal into its composed form, which would make this cell
    // vacuous without ever failing. The premise assertions say so aloud.
    const nfcPrefix = '/private/tmp/arm-Répo'.normalize('NFC');
    const nfdPrefix = nfcPrefix.normalize('NFD');
    expect(nfdPrefix).not.toBe(nfcPrefix);
    expect(nfdPrefix.normalize('NFC')).toBe(nfcPrefix);

    // Premise: the NFC spelling, against the NFC prefix, IS allowed.
    expect(
      classifySweepRoot(`${nfcPrefix}/cdk.out-1`, { allowedPrefixes: [nfcPrefix] }).allowed,
    ).toBe(true);

    // The property: the NFD spelling is refused, because the allowlist is
    // compared byte-faithfully. Folding it would match, and would widen.
    const verdict = classifySweepRoot(`${nfdPrefix}/cdk.out-1`, { allowedPrefixes: [nfcPrefix] });
    expect(verdict.allowed).toBe(false);
    expect(verdict.reason).toBe('outside-system-temp-root');
  });
});

// ===========================================================================
// EXHAUSTIVE `{platform} x {candidate kind}` MATRIX — every cell explicit
// ===========================================================================

describe('exhaustive platform x candidate-kind matrix — every cell asserted', () => {
  // Every candidate is a NON-EXISTENT absolute path (see `nonExistentUnder`'s
  // doc comment): the host's own symlink layout must not decide a
  // cross-platform cell.
  const CANDIDATES = {
    'darwin allowed prefix (/private/var/folders)': nonExistentUnder('/private/var/folders', 'm1'),
    'darwin allowed prefix (/tmp)': nonExistentUnder('/tmp', 'm2'),
    'linux allowed prefix (/var/tmp)': nonExistentUnder('/var/tmp', 'm3'),
    'adjacent prefix (/tmpfoo)': '/tmpfoo/cdk.out-1',
    'outside (a home-directory path)': '/Users/arm/projects/repo/infra/cdk.out',
    unresolvable: undefined,
  };

  /**
   * The full truth table. `null` reason means allowed. Read as: for this
   * platform, this candidate kind, this is the whole verdict.
   */
  const MATRIX = {
    darwin: {
      'darwin allowed prefix (/private/var/folders)': { allowed: true, reason: null },
      'darwin allowed prefix (/tmp)': { allowed: true, reason: null },
      // `/var/tmp` is NOT a darwin temp root — macOS's `/var/tmp` is not a
      // per-user temp root and is deliberately absent from the darwin list.
      'linux allowed prefix (/var/tmp)': { allowed: false, reason: 'outside-system-temp-root' },
      'adjacent prefix (/tmpfoo)': { allowed: false, reason: 'outside-system-temp-root' },
      'outside (a home-directory path)': { allowed: false, reason: 'outside-system-temp-root' },
      unresolvable: { allowed: false, reason: 'unresolvable' },
    },
    linux: {
      'darwin allowed prefix (/private/var/folders)': {
        allowed: false,
        reason: 'outside-system-temp-root',
      },
      // `/tmp` is on BOTH lists.
      'darwin allowed prefix (/tmp)': { allowed: true, reason: null },
      'linux allowed prefix (/var/tmp)': { allowed: true, reason: null },
      'adjacent prefix (/tmpfoo)': { allowed: false, reason: 'outside-system-temp-root' },
      'outside (a home-directory path)': { allowed: false, reason: 'outside-system-temp-root' },
      unresolvable: { allowed: false, reason: 'unresolvable' },
    },
    // Unsupported platforms refuse EVERY candidate kind. The platform check
    // dominates the prefix check; it does NOT dominate input validation (see
    // the `unresolvable` cell below), which is why that one cell accepts
    // either fail-closed reason.
    win32: {
      'darwin allowed prefix (/private/var/folders)': {
        allowed: false,
        reason: 'unsupported-platform',
      },
      'darwin allowed prefix (/tmp)': { allowed: false, reason: 'unsupported-platform' },
      'linux allowed prefix (/var/tmp)': { allowed: false, reason: 'unsupported-platform' },
      'adjacent prefix (/tmpfoo)': { allowed: false, reason: 'unsupported-platform' },
      'outside (a home-directory path)': { allowed: false, reason: 'unsupported-platform' },
      unresolvable: { allowed: false, reason: ['unresolvable', 'unsupported-platform'] },
    },
    sunos: {
      'darwin allowed prefix (/private/var/folders)': {
        allowed: false,
        reason: 'unsupported-platform',
      },
      'darwin allowed prefix (/tmp)': { allowed: false, reason: 'unsupported-platform' },
      'linux allowed prefix (/var/tmp)': { allowed: false, reason: 'unsupported-platform' },
      'adjacent prefix (/tmpfoo)': { allowed: false, reason: 'unsupported-platform' },
      'outside (a home-directory path)': { allowed: false, reason: 'unsupported-platform' },
      unresolvable: { allowed: false, reason: ['unresolvable', 'unsupported-platform'] },
    },
    // PROTOTYPE-CHAIN KEYS AS PLATFORM NAMES. `SYSTEM_TEMP_ROOT_PREFIXES` is a
    // plain frozen object, so `SYSTEM_TEMP_ROOT_PREFIXES['constructor']` is the
    // `Object` function and `['__proto__']` is `Object.prototype` — both TRUTHY.
    // The production code refuses them only because it gates on
    // `Array.isArray(platformPrefixes)`; swapping that for a truthiness check
    // would make `{ platform: 'constructor' }` reach the prefix loop with a
    // non-array, and `.some` would then throw out of a "never throws" contract.
    // No cell pinned that, so these two rows are the pin.
    constructor: {
      'darwin allowed prefix (/private/var/folders)': {
        allowed: false,
        reason: 'unsupported-platform',
      },
      'darwin allowed prefix (/tmp)': { allowed: false, reason: 'unsupported-platform' },
      'linux allowed prefix (/var/tmp)': { allowed: false, reason: 'unsupported-platform' },
      'adjacent prefix (/tmpfoo)': { allowed: false, reason: 'unsupported-platform' },
      'outside (a home-directory path)': { allowed: false, reason: 'unsupported-platform' },
      unresolvable: { allowed: false, reason: ['unresolvable', 'unsupported-platform'] },
    },
    // COMPUTED KEY, DELIBERATELY. A bare `__proto__:` in an object literal
    // invokes the prototype SETTER instead of creating an own property, so the
    // row would vanish from `Object.keys` and this cell would silently not exist.
    ['__proto__']: {
      'darwin allowed prefix (/private/var/folders)': {
        allowed: false,
        reason: 'unsupported-platform',
      },
      'darwin allowed prefix (/tmp)': { allowed: false, reason: 'unsupported-platform' },
      'linux allowed prefix (/var/tmp)': { allowed: false, reason: 'unsupported-platform' },
      'adjacent prefix (/tmpfoo)': { allowed: false, reason: 'unsupported-platform' },
      'outside (a home-directory path)': { allowed: false, reason: 'unsupported-platform' },
      unresolvable: { allowed: false, reason: ['unresolvable', 'unsupported-platform'] },
    },
  };

  const cells = Object.entries(MATRIX).flatMap(([platform, row]) =>
    Object.entries(row).map(([candidateKind, expected]) => [
      platform,
      candidateKind,
      CANDIDATES[candidateKind],
      expected,
    ]),
  );

  test('the matrix covers every declared platform x candidate-kind pair — no cell left implicit', () => {
    expect(cells).toHaveLength(Object.keys(MATRIX).length * Object.keys(CANDIDATES).length);
    for (const [, row] of Object.entries(MATRIX)) {
      expect(Object.keys(row).sort()).toEqual(Object.keys(CANDIDATES).sort());
    }

    // AND TIE THE PLATFORM AXIS TO PRODUCTION. The two assertions above both
    // derive from the hand-written `MATRIX`, so on their own they say nothing
    // about `SYSTEM_TEMP_ROOT_PREFIXES`. Adding `freebsd: ['/tmp']` to the
    // production allowlist — a WIDENING of the destructive surface — would then
    // ship with zero coverage while a test named "no cell left implicit" still
    // passed. Every supported platform must have a row.
    //
    // Deliberately one-directional: `MATRIX` may carry platforms the allowlist
    // does NOT (`win32`, `sunos` are here precisely to pin the
    // unsupported-platform refusal), so this is `toContain`, not set equality.
    for (const platform of Object.keys(SYSTEM_TEMP_ROOT_PREFIXES)) {
      expect(Object.keys(MATRIX)).toContain(platform);
    }
  });

  test.each(cells)(
    'platform %s + %s -> the declared verdict',
    (platform, _candidateKind, candidate, expected) => {
      const verdict = classifySweepRoot(candidate, { platform });
      expect(verdict.allowed).toBe(expected.allowed);
      if (Array.isArray(expected.reason)) {
        expect(expected.reason).toContain(verdict.reason);
      } else {
        expect(verdict.reason).toBe(expected.reason);
      }
      expect(typeof verdict.resolvedDir).toBe('string');
    },
  );

  test('the default platform is process.platform — an omitted option behaves identically to passing it', () => {
    const scratch = makeScratchRoot('default-platform');
    expect(classifySweepRoot(scratch)).toEqual(
      classifySweepRoot(scratch, { platform: process.platform }),
    );
  });
});
