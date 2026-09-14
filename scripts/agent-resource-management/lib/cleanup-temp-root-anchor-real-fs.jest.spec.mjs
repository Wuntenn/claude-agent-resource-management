// Phase 1 — REAL-FILESYSTEM integration tests for the system-temp-root
// anchor. Split from `./cleanup-async-confinement-anchor.jest.spec.mjs` (unit /
// matrix) and `./cleanup-temp-root-anchor-fs-surface.jest.spec.mjs` (mocked
// `node:fs`) because `jest.unstable_mockModule` applies file-wide once
// registered — mirroring this directory's existing
// `diagnostic-reports-scanner.jest.spec.mjs` (mocked) vs
// `diagnostic-reports-scanner-symlink-realfs.jest.spec.mjs` (real fs) split,
// and `cleanup-adapter.jest.spec.mjs`'s own doc comment on the same reason.
//
// RED BY CONSTRUCTION: `./cleanup-temp-root-anchor.mjs` does not exist yet.
//
// WHAT ONLY A REAL FILESYSTEM CAN PROVE
//
// The anchor resolves its candidate ONCE, via `realpathSync` with a
// `resolve()` fallback, and RETURNS that resolved path so Phase 2 walks
// exactly the path that was guarded. That closes the "confined path ≠ walked
// path" symlink divergence — and a symlink is precisely the thing a mocked fs
// cannot honestly simulate: the whole risk is that the KERNEL resolves a path
// somewhere the guard never looked.
//
// Two directions matter, and both are asserted:
//   - A symlink INSIDE a temp prefix pointing at the REPO must be refused
//     AFTER resolution. A pre-resolution check would allow it, and the sweep
//     would then recursively remove working-tree files through the link.
//   - macOS's real `/tmp` -> `/private/tmp` pair must be ALLOWED in both
//     written forms, or the auto-trigger would refuse the host's own temp root
//     and the feature would be inert on every developer machine.
//
// SAFETY: every directory this file creates lives under a per-test
// `fs.mkdtempSync` scratch root and is removed in `afterEach`. No test writes
// to, or removes anything from, the real `$TMPDIR` itself — the host `$TMPDIR`
// holds 3,000+ real `cdk.out` dirs that must never be touched (see
// `./cleanup.jest.spec.mjs:86-99`). The unit under test is non-destructive by
// contract, and this file additionally asserts that as an observable fact: the
// seeded tree is still intact after every classification.

import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { classifySweepRoot } from './cleanup-temp-root-anchor.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const ANCHOR_PATH = join(HERE, 'cleanup-temp-root-anchor.mjs');
// lib -> agent-resource-management -> scripts -> repo root.
const REPO_ROOT = resolve(HERE, '..', '..', '..');

/**
 * Platform gate, NOT a green-washing skip: the macOS `/tmp` -> `/private/tmp`
 * and `/var/folders` -> `/private/var/folders` symlink pairs only exist on
 * darwin, and asserting them on a Linux CI runner would assert nothing. The
 * platform-independent halves of the same property (an own symlink resolved to
 * its own target, and a symlink into the repo refused after resolution) run
 * EVERYWHERE, so no property is left unasserted on Linux.
 */
const describeDarwin = process.platform === 'darwin' ? describe : describe.skip;

let scratchRoot;

beforeEach(() => {
  scratchRoot = mkdtempSync(join(tmpdir(), 'arm-anchor-realfs-'));
});

afterEach(() => {
  rmSync(scratchRoot, { recursive: true, force: true });
});

/** A real directory with a real file in it, so "nothing was removed" is observable. */
function seedRealTree(dirPath) {
  mkdirSync(dirPath, { recursive: true });
  writeFileSync(join(dirPath, 'payload.bin'), Buffer.alloc(1024, 1));
  return dirPath;
}

describe('the anchor resolves the real filesystem once and returns the path it guarded', () => {
  test('a real mkdtemp scratch root is allowed, and the returned resolvedDir is its realpath', () => {
    const verdict = classifySweepRoot(scratchRoot);

    expect(verdict.allowed).toBe(true);
    expect(verdict.reason).toBeNull();
    expect(verdict.resolvedDir).toBe(realpathSync(scratchRoot));
  });

  test('a symlink inside the temp root pointing at ANOTHER temp dir is allowed, and resolvedDir is the TARGET, not the link', () => {
    const target = seedRealTree(join(scratchRoot, 'real-target', 'nested'));
    const link = join(scratchRoot, 'link-to-temp');
    symlinkSync(target, link);

    const verdict = classifySweepRoot(link);

    expect(verdict.allowed).toBe(true);
    // The caller must walk the TARGET — the divergence Phase 2 depends on
    // being closed.
    expect(verdict.resolvedDir).toBe(realpathSync(target));
    expect(verdict.resolvedDir).not.toBe(link);
  });

  test('a symlink inside the temp root pointing AT THE REPO is refused AFTER resolution', () => {
    const link = join(scratchRoot, 'link-to-repo');
    symlinkSync(REPO_ROOT, link);

    // Premise: the link's OWN path is inside an allowed temp prefix, so an
    // implementation that classified before resolving would allow it.
    expect(classifySweepRoot(scratchRoot).allowed).toBe(true);

    const verdict = classifySweepRoot(link);

    expect(verdict.allowed).toBe(false);
    expect(verdict.reason).toBe('outside-system-temp-root');
    // And it reports where the link actually went, so the refusal is
    // diagnosable rather than mysterious.
    expect(verdict.resolvedDir).toBe(realpathSync(REPO_ROOT));
  });

  test('a symlink inside the temp root pointing at a REPO SUBDIRECTORY is refused too', () => {
    const link = join(scratchRoot, 'link-to-repo-infra');
    symlinkSync(join(REPO_ROOT, 'scripts', 'agent-resource-management'), link);

    const verdict = classifySweepRoot(link);

    expect(verdict.allowed).toBe(false);
    expect(verdict.reason).toBe('outside-system-temp-root');
  });

  test('a DANGLING symlink inside the temp root falls back to resolve() and stays allowed', () => {
    const link = join(scratchRoot, 'dangling-link');
    symlinkSync(join(scratchRoot, 'never-created'), link);

    const verdict = classifySweepRoot(link);

    // `realpathSync` throws for a dangling link; the `resolve()` fallback keeps
    // the link's own path, which is inside the temp root. Phase 2's listing
    // step is what reports `listing-failed` for it.
    expect(verdict.allowed).toBe(true);
    expect(verdict.resolvedDir).toBe(link);
    // And it says so, so Phase 2 can decline to walk it — see the next test for
    // why that matters rather than being a nicety.
    expect(verdict.resolvedVia).toBe('fallback');
  });

  test('a symlink OUT of the temp root with a MISSING LEAF reports fallback — resolvedDir is NOT where it goes', () => {
    // THE DIVERGENCE `resolvedVia` EXISTS FOR, on a real filesystem.
    //
    // `realpathSync` resolves the whole path or nothing: one missing component
    // — here the leaf — makes it throw ENOENT, and the `resolve()` fallback then
    // keeps the candidate's WRITTEN form with `escape-link` still unresolved. So
    // the verdict is `allowed: true` (the written form sits inside a temp root)
    // while the real location of that path is outside it. Verified on Node
    // 22.23.0; this is not a hypothetical.
    //
    // THE ESCAPE TARGET IS THE REPO ITSELF. It is genuinely outside the temp
    // root (which is what makes this test's name true) and it already holds real
    // content, so the survival assertion at the end is about a directory that was
    // actually on the code path. An earlier revision seeded a separate
    // `<scratch>/outside-stand-in` tree for that assertion while the link pointed
    // at `REPO_ROOT` — so the fixture, its variable name and its comment all
    // described a path no classification ever saw.
    const escapeTarget = REPO_ROOT;
    const link = join(scratchRoot, 'escape-link');
    symlinkSync(escapeTarget, link);

    // Control: with the leaf PRESENT, realpath succeeds and the refusal is
    // correct and complete — the guard sees where the link really goes, and
    // refuses it because that is the repo.
    const existingLeaf = classifySweepRoot(link);
    expect(existingLeaf.allowed).toBe(false);
    expect(existingLeaf.reason).toBe('outside-system-temp-root');
    expect(existingLeaf.resolvedVia).toBe('realpath');
    expect(existingLeaf.resolvedDir).toBe(realpathSync(escapeTarget));

    // The divergent case: an absent leaf under the same link.
    const absentLeaf = classifySweepRoot(join(link, 'no-such-leaf-1691'));
    expect(absentLeaf.allowed).toBe(true);
    expect(absentLeaf.resolvedVia).toBe('fallback');
    expect(absentLeaf.resolvedDir.startsWith(scratchRoot + sep)).toBe(true);

    // THE CONTRACT THIS PINS: `allowed` alone is not a licence to walk
    // `resolvedDir`. Phase 2 must require BOTH.
    expect(absentLeaf.allowed && absentLeaf.resolvedVia === 'realpath').toBe(false);

    // Nothing was touched either way — asserted on the escape target the link
    // really points at, not on an unrelated fixture.
    expect(existsSync(join(escapeTarget, 'package.json'))).toBe(true);
    expect(existsSync(link)).toBe(true);
  });

  test('classification is observably non-destructive — the seeded tree is untouched afterwards', () => {
    const staleLookalike = seedRealTree(join(scratchRoot, 'cdk.out-would-be-swept'));
    const payload = join(staleLookalike, 'payload.bin');

    // Classify everything this file classifies elsewhere, including a
    // deliberately refused candidate, in one pass.
    classifySweepRoot(scratchRoot);
    classifySweepRoot(staleLookalike);
    classifySweepRoot(REPO_ROOT);
    classifySweepRoot(join(scratchRoot, 'no-such-child'));

    expect(existsSync(staleLookalike)).toBe(true);
    expect(existsSync(payload)).toBe(true);
    expect(existsSync(scratchRoot)).toBe(true);
    // And the repo it refused is, obviously, still there.
    expect(existsSync(join(REPO_ROOT, 'package.json'))).toBe(true);
  });

  test('trailing-separator and doubled-separator forms of a REAL scratch root all normalise to the same allowed resolvedDir', () => {
    const canonical = classifySweepRoot(scratchRoot);

    for (const form of [`${scratchRoot}/`, `${scratchRoot}//`, join(scratchRoot, '.')]) {
      const verdict = classifySweepRoot(form);
      expect(verdict.allowed).toBe(true);
      expect(verdict.resolvedDir).toBe(canonical.resolvedDir);
      expect(verdict.resolvedDir.endsWith(sep)).toBe(false);
    }
  });
});

describeDarwin('macOS symlinked temp-root pairs (darwin only — the pairs do not exist elsewhere)', () => {
  test('the real /tmp symlink resolves to /private/tmp and is allowed', () => {
    expect(realpathSync('/tmp')).toBe('/private/tmp');

    for (const writtenForm of ['/tmp', '/private/tmp', '/tmp/']) {
      const verdict = classifySweepRoot(writtenForm);
      expect(verdict.allowed).toBe(true);
      expect(verdict.reason).toBeNull();
      expect(verdict.resolvedDir).toBe('/private/tmp');
    }
  });

  test('the /var/folders pair resolves to /private/var/folders and is allowed in both written forms', () => {
    expect(realpathSync('/var/folders')).toBe('/private/var/folders');

    for (const writtenForm of ['/var/folders', '/private/var/folders']) {
      const verdict = classifySweepRoot(writtenForm);
      expect(verdict.allowed).toBe(true);
      expect(verdict.resolvedDir).toBe('/private/var/folders');
    }
  });

  test("the host's own os.tmpdir() is allowed — otherwise the auto-trigger would be inert on every dev machine", () => {
    const verdict = classifySweepRoot(tmpdir());
    expect(verdict.allowed).toBe(true);
    expect(verdict.resolvedDir).toBe(realpathSync(tmpdir()));
  });

  test('/private/tmp is allowed even under launchd\'s minimal env, where TMPDIR is unset', () => {
    // The plan's launchd edge case: with `TMPDIR` unset the watchdog's temp
    // root IS `/private/tmp`, shared across users. Confinement must ALLOW it —
    // the multi-user EPERM consequence is Phase 2's bounded-errors problem, not
    // a confinement failure. Asserted without mutating this process's env by
    // passing the resolved value directly.
    const verdict = classifySweepRoot('/private/tmp');
    expect(verdict.allowed).toBe(true);
    expect(verdict.reason).toBeNull();
  });
});

// ===========================================================================
// THE REAL CI ARRANGEMENT — A REPO-SHAPED TREE UNDER A REAL PLATFORM PREFIX
// ===========================================================================
//
// `./cleanup-async-confinement-anchor.jest.spec.mjs` can only approximate this
// through the `allowedPrefixes` seam, and the approximation is NOT mechanically
// equivalent: the seam REPLACES the platform list, so that spec never exercises
// a PLATFORM prefix interacting with the repo exclusion. That interaction is
// exactly where a real bypass lived — a path keeping its literal `/private/tmp`
// prefix passes the allowlist and can then miss the exclusion on SPELLING:
//
//   exact-case <checkout>/infra : allowed:false, 'outside-system-temp-root'
//   UPPER-case <checkout>/infra : allowed:true,  reason:null   <-- BYPASS
//
// Both name the same directory on a case-insensitive volume (APFS's default).
// Reproducing it needs the module's OWN `import.meta.url` to sit inside a temp
// prefix, because that is what `REPO_ROOT` is derived from — so this block
// copies the module into a repo-shaped `mkdtempSync` tree and imports the copy.
// That is the arrangement a CI runner with a temp-mounted checkout really has.

describe('a COPY of the anchor, loaded from a repo-shaped tree under a real temp prefix', () => {
  /** `<scratch>/<repoName>/scripts/agent-resource-management/lib/` — the depth `REPO_ROOT` assumes. */
  function plantAnchorCopy(repoName) {
    const fakeRepoRoot = join(scratchRoot, repoName);
    const libDir = join(fakeRepoRoot, 'scripts', 'agent-resource-management', 'lib');
    mkdirSync(libDir, { recursive: true });
    copyFileSync(ANCHOR_PATH, join(libDir, basename(ANCHOR_PATH)));
    // A real working-tree-shaped directory with real content in it, so "the
    // sweep would have removed THIS" is concrete rather than notional.
    seedRealTree(join(fakeRepoRoot, 'infra', 'cdk.out'));
    return { fakeRepoRoot, modulePath: join(libDir, basename(ANCHOR_PATH)) };
  }

  test('the planted copy refuses its own repo root, which sits under a REAL platform temp prefix', async () => {
    const { fakeRepoRoot, modulePath } = plantAnchorCopy('MyRepo');
    const planted = await import(pathToFileURL(modulePath).href);

    // Premise: the scratch root really is inside a platform prefix, with NO
    // seam in play — this is the platform allowlist doing the allowing.
    expect(planted.classifySweepRoot(scratchRoot).allowed).toBe(true);

    for (const candidate of [fakeRepoRoot, join(fakeRepoRoot, 'infra', 'cdk.out')]) {
      const verdict = planted.classifySweepRoot(candidate);
      expect(verdict.allowed).toBe(false);
      expect(verdict.reason).toBe('outside-system-temp-root');
    }
  });

  test('the planted copy refuses a CASE-VARIANT spelling of its own repo root — the reproduced bypass', async () => {
    const { fakeRepoRoot, modulePath } = plantAnchorCopy('MyRepo');
    const planted = await import(pathToFileURL(modulePath).href);

    const upper = join(scratchRoot, 'MYREPO');
    const lower = join(scratchRoot, 'myrepo');

    // Premise, asserted rather than assumed: on THIS volume the variant
    // spellings really do name the same directory. On a case-SENSITIVE volume
    // they do not, the bypass does not exist there, and the refusals below are
    // then simply a harmless over-refusal — so the test is meaningful either way.
    const caseInsensitiveVolume = existsSync(join(upper, 'infra', 'cdk.out'));
    if (caseInsensitiveVolume) {
      expect(realpathSync(join(upper, 'infra'))).not.toBe(join(fakeRepoRoot, 'infra'));
    }

    for (const spelling of [upper, lower]) {
      for (const tail of [[], ['infra'], ['infra', 'cdk.out']]) {
        const verdict = planted.classifySweepRoot(join(spelling, ...tail));
        expect(verdict.allowed).toBe(false);
        expect(verdict.reason).toBe('outside-system-temp-root');
      }
    }
  });

  test('the planted copy refuses an NFD spelling of a non-ASCII repo root', async () => {
    // APFS is normalisation-INSENSITIVE as well as case-insensitive, so the
    // composed and decomposed spellings of `Rép` are one directory while
    // comparing unequal as strings — the same class of bypass as case, and
    // likewise reproduced on Node 22.23.0. This repo's own path is ASCII, so
    // only a planted non-ASCII tree can exercise it.
    // Both forms are DERIVED rather than typed as literals: an editor, a diff
    // tool or a copy-paste through a normalising transport silently rewrites a
    // decomposed literal into its composed form, which would make this cell
    // vacuous without ever failing. The premise assertions below say so aloud.
    const nfcName = 'R\u00e9po-1691'.normalize('NFC'); // R + LATIN SMALL E WITH ACUTE
    const nfdName = nfcName.normalize('NFD'); // R + e + COMBINING ACUTE (U+0301)
    expect(nfdName).not.toBe(nfcName);
    expect(nfdName.normalize('NFC')).toBe(nfcName);

    const { modulePath } = plantAnchorCopy(nfcName);
    const planted = await import(pathToFileURL(modulePath).href);

    for (const tail of [[], ['infra'], ['infra', 'cdk.out']]) {
      for (const name of [nfcName, nfdName]) {
        const verdict = planted.classifySweepRoot(join(scratchRoot, name, ...tail));
        expect(verdict.allowed).toBe(false);
        expect(verdict.reason).toBe('outside-system-temp-root');
      }
    }
  });

  test('the planted copy still ALLOWS a sibling that is not its repo — the exclusion is not over-broad', async () => {
    const { modulePath } = plantAnchorCopy('MyRepo');
    const planted = await import(pathToFileURL(modulePath).href);

    const sibling = join(scratchRoot, 'not-the-repo-at-all', 'cdk.out-1');
    const verdict = planted.classifySweepRoot(sibling);
    expect(verdict.allowed).toBe(true);
    expect(verdict.reason).toBeNull();
  });

  test('the planted copy removed nothing — the seeded working tree survives every classification', async () => {
    const { fakeRepoRoot, modulePath } = plantAnchorCopy('MyRepo');
    const planted = await import(pathToFileURL(modulePath).href);
    const payload = join(fakeRepoRoot, 'infra', 'cdk.out', 'payload.bin');

    planted.classifySweepRoot(fakeRepoRoot);
    planted.classifySweepRoot(join(scratchRoot, 'MYREPO', 'infra', 'cdk.out'));
    planted.classifySweepRoot(scratchRoot);

    expect(existsSync(payload)).toBe(true);
  });
});
