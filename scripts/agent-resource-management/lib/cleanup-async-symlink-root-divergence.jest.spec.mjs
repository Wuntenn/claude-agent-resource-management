// Phase 2 — CONFINEMENT-FIRST, and the TOCTOU / symlink-divergence gate.
// REAL filesystem: a symlink is the one thing a mocked fs cannot honestly
// simulate, because the whole risk is that the KERNEL resolves a path somewhere
// the guard never looked.
//
// ---------------------------------------------------------------------------
// THE PROPERTY THIS FILE PROTECTS
// ---------------------------------------------------------------------------
//
// The sweep is a RECURSIVE REMOVAL primitive that runs unattended. Its only
// protection is that the path it WALKS is the same path the guard APPROVED. Two
// distinct ways that equality breaks, and this file pins both:
//
// 1. CONFINEMENT RUNS FIRST, AND ON THE RESOLVED PATH. `classifySweepRoot` is
//    consulted BEFORE any listing, and the walk targets `resolvedDir`, never
//    the raw `dir`. If the walk used the raw `dir`, a symlink sitting inside an
//    allowed temp root and pointing at a working tree would be approved (the
//    LINK's path is confined) and then recursively removed THROUGH the link.
//    The anchor closes that by returning where the link actually goes; the
//    sweep's job is to use the value it was given rather than the one it was
//    handed.
//
// 2. `resolvedVia !== 'realpath'` MUST ALSO REFUSE. This is a PHASE-1
//    CARRY-FORWARD that is easy to miss, because the anchor reports
//    `allowed: true` for it. `realpathSync` resolves a path WHOLLY or not at
//    all: one missing component — including the final one — makes it throw
//    ENOENT, and the anchor's `path.resolve()` fallback then keeps the
//    candidate's WRITTEN form with EVERY SYMLINK IN IT UNRESOLVED. So
//    `<temp>/link/absent`, where `link` points outside the temp root, comes
//    back `allowed: true` with a `resolvedDir` whose real location is somewhere
//    else entirely. The anchor's own header states the requirement in capitals:
//    "PHASE 2 MUST REQUIRE `resolvedVia === 'realpath'` BEFORE LISTING OR
//    REMOVING ANYTHING UNDER `resolvedDir`."
//
//    That is also the TOCTOU shape. A path that could not be resolved at guard
//    time can acquire a symlink component between the check and the walk, and
//    the walk would then leave the guarded region without anything having
//    lied. Refusing costs no capability whatsoever: a root with a missing
//    component has nothing in it to reclaim.
//
// WHY A NAIVE IMPLEMENTATION PASSES A WEAKER TEST. Gating on `allowed` alone is
// the obvious reading of the anchor's contract, and it passes every cell that
// only ever presents EXISTING paths — which is every cell a test author writes
// without having read the anchor's fallback branch. `resolvedVia` is a field
// such an implementation never mentions, and no behavioural test built from
// resolvable fixtures can distinguish it. Hence a cell built specifically on an
// UNRESOLVABLE root, with its premise (`allowed: true`, `resolvedVia:
// 'fallback'`) asserted against the real anchor first so the cell cannot rot
// into a tautology.
//
// THE ERROR KIND FOR (2) IS PINNED HERE AS `'unresolved-root-refused'` — a
// test-author decision the plan leaves open. It is deliberately DISTINCT from
// `'confinement-refused'`: the two refusals have different operational
// meanings. `confinement-refused` says "you aimed this somewhere it may never
// go" — a configuration error a human must fix. `unresolved-root-refused` says
// "the target could not be pinned down THIS TIME" — ordinarily transient, and
// expected to resolve itself on a later beat. Collapsing them would make a
// benign, self-healing condition indistinguishable from a misconfiguration
// aimed at a working tree.
//
// ---------------------------------------------------------------------------
// RED BY CONSTRUCTION
// ---------------------------------------------------------------------------
//
// `./cleanup.mjs` does not export `sweepStaleTempDirsAsync`. The NAMESPACE
// import below keeps that red per-cell — `sweepStaleTempDirsAsync is
// not a function` — rather than one opaque link-time SyntaxError for the whole
// file. `classifySweepRoot` (Phase 1) DOES exist and
// is green — it is imported here to state each cell's premise against the real
// guard rather than against this file's belief about it.
//
// SAFETY: REAL removals happen in this file. Every path it creates lives under
// its own `fs.mkdtempSync` scratch roots, nominated explicitly through
// `allowedPrefixes` and torn down in `afterEach`. The process's real `$TMPDIR`
// is never swept — pinned by the sentinel cell at the end. Note that one cell
// deliberately creates a symlink pointing at the REPO ROOT: it asserts the
// refusal AND that the repo is still intact afterwards, because "the sweep
// refused" and "the working tree survived" are not the same claim.

import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  realpathSync,
  rmSync,
  symlinkSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

import { classifySweepRoot } from './cleanup-temp-root-anchor.mjs';
import { AUTO_TRIGGER_AGE_HOURS, sweepStaleTempDirsAsync } from './cleanup.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
// lib -> agent-resource-management -> scripts -> repo root.
const REPO_ROOT = resolve(HERE, '..', '..', '..');
const HOUR_MS = 60 * 60 * 1000;

let scratchRoot;
let root;
/** A second scratch tree, deliberately OUTSIDE `allowedPrefixes` — the "somewhere else" a link can point. */
let outsideRoot;
/** Paths whose mode a cell narrowed; restored before teardown so `rmSync` can always complete. */
let modeRestorePaths;

beforeEach(() => {
  scratchRoot = mkdtempSync(join(tmpdir(), 'arm-divergence-'));
  root = realpathSync(scratchRoot);
  outsideRoot = realpathSync(mkdtempSync(join(tmpdir(), 'arm-divergence-outside-')));
  modeRestorePaths = [];
});

afterEach(() => {
  for (const path of modeRestorePaths) {
    try {
      chmodSync(path, 0o755);
    } catch {
      // Best effort — a path a cell already removed is fine.
    }
  }
  rmSync(scratchRoot, { recursive: true, force: true });
  rmSync(outsideRoot, { recursive: true, force: true });
});

/** A stale, correctly-named candidate with real content, every level backdated past the auto threshold. */
function seedStaleCandidate(parent, name, bytes = 4096) {
  const candidate = join(parent, name);
  mkdirSync(candidate, { recursive: true });
  const payload = join(candidate, 'payload.bin');
  writeFileSync(payload, Buffer.alloc(bytes, 1));
  const seconds = (Date.now() - (AUTO_TRIGGER_AGE_HOURS + 4) * HOUR_MS) / 1000;
  utimesSync(payload, seconds, seconds);
  utimesSync(candidate, seconds, seconds);
  return candidate;
}

function sweep(dir, allowedPrefixes) {
  return sweepStaleTempDirsAsync({
    dir,
    ageThresholdMs: AUTO_TRIGGER_AGE_HOURS * HOUR_MS,
    now: Date.now(),
    budgetMs: 5_000,
    allowedPrefixes,
  });
}

describe('the walked path is the GUARDED path — confinement runs first, on the resolved form', () => {
  test('a symlinked sweep root is walked at its TARGET: the candidate under the target is removed', async () => {
    // The link sits inside the allowed prefix; its target also does. Both are
    // legitimate, so the sweep proceeds — and the observable proof that it
    // walked `resolvedDir` rather than the raw `dir` is that the candidate
    // living under the TARGET is what disappears.
    const target = join(root, 'real-root');
    mkdirSync(target, { recursive: true });
    const link = join(root, 'link-root');
    symlinkSync(target, link);
    const candidate = seedStaleCandidate(target, 'cdk.out-through-link');

    // Premise, against the real Phase 1 guard.
    const verdict = classifySweepRoot(link, { allowedPrefixes: [root] });
    expect(verdict.allowed).toBe(true);
    expect(verdict.resolvedVia).toBe('realpath');
    expect(verdict.resolvedDir).toBe(realpathSync(target));

    const result = await sweep(link, [root]);

    expect(existsSync(candidate)).toBe(false);
    expect(result.removedCount).toBe(1);
    expect(result.reclaimedBytes).toBe(4096);
  });

  test('the paths the report names are rooted at `resolvedDir`, not at the written `dir`', async () => {
    // The same property read off the REPORT rather than off the disk. A
    // candidate that cannot be removed forces an error whose `path` field is
    // exactly the string the sweep was operating on — the cheapest honest way
    // to observe which root it walked.
    const target = join(root, 'real-root');
    mkdirSync(target, { recursive: true });
    const link = join(root, 'link-root');
    symlinkSync(target, link);
    const candidate = seedStaleCandidate(target, 'cdk.out-undeletable');
    // Read+execute only: the child cannot be unlinked, so `rm` fails.
    chmodSync(candidate, 0o555);
    modeRestorePaths.push(candidate);

    const result = await sweep(link, [root]);

    expect(result.removedCount).toBe(0);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].kind).toBe('removal-failed');
    expect(result.errors[0].path).toBe(join(realpathSync(target), 'cdk.out-undeletable'));
    expect(result.errors[0].path.startsWith(link + sep)).toBe(false);
  });

  test('a sweep root that RESOLVES outside the allowlist is refused, and the candidate beyond it survives', async () => {
    // The link's own path is inside the allowed prefix; its target is not.
    // An implementation that classified before resolving — or that walked the
    // raw `dir` — would recursively remove through it.
    const link = join(root, 'escape-link');
    symlinkSync(outsideRoot, link);
    const beyond = seedStaleCandidate(outsideRoot, 'cdk.out-beyond-the-link');

    // Premise: the link's OWN written path really is inside the allowlist.
    expect(link.startsWith(root + sep)).toBe(true);
    expect(classifySweepRoot(link, { allowedPrefixes: [root] }).allowed).toBe(false);

    const result = await sweep(link, [root]);

    expect(result.removedCount).toBe(0);
    expect(result.reclaimedBytes).toBe(0);
    expect(result.errors).toEqual([
      expect.objectContaining({ path: link, kind: 'confinement-refused' }),
    ]);
    expect(existsSync(beyond)).toBe(true);
    expect(readdirSync(beyond)).toContain('payload.bin');
  });

  test('a symlink pointing at THIS REPOSITORY is refused, and the working tree is intact afterwards', async () => {
    // The single worst outcome this whole subsystem exists to prevent, on a
    // real filesystem. Asserted as TWO claims, because "the report says
    // refused" and "the repo is still there" are different things.
    const link = join(root, 'link-to-repo');
    symlinkSync(REPO_ROOT, link);

    const result = await sweep(link, [root, REPO_ROOT]);

    expect(result.removedCount).toBe(0);
    expect(result.errors[0].kind).toBe('confinement-refused');
    // Note the allowlist above DELIBERATELY nominates `REPO_ROOT` — the seam is
    // a widening primitive (see the anchor's header) and this cell proves the
    // repo exclusion outranks it, rather than relying on the prefix list to do
    // the refusing.
    expect(existsSync(join(REPO_ROOT, 'package.json'))).toBe(true);
    expect(existsSync(join(REPO_ROOT, 'scripts', 'agent-resource-management', 'cli.mjs'))).toBe(true);
  });

  test('confinement is decided BEFORE any listing — a refused root is never even read', async () => {
    // Ordering, asserted through an observable consequence: the refused root is
    // one this process cannot list at all, so an implementation that listed
    // first would produce a `listing-failed` error instead of a
    // `confinement-refused` one.
    const unreadable = join(outsideRoot, 'unreadable');
    mkdirSync(unreadable, { recursive: true });
    seedStaleCandidate(unreadable, 'cdk.out-hidden');
    chmodSync(unreadable, 0o000);
    modeRestorePaths.push(unreadable);

    const result = await sweep(unreadable, [root]);

    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].kind).toBe('confinement-refused');
    expect(result.errors.some((error) => error.kind === 'listing-failed')).toBe(false);
    expect(result.removedCount).toBe(0);
  });
});

describe('THE TOCTOU / SYMLINK-DIVERGENCE GATE — `allowed` alone is not a licence to walk', () => {
  test('an ALLOWED root whose `resolvedVia` is `fallback` is refused, with zero removals', async () => {
    // The Phase-1 carry-forward. `outside-link` points out of the allowlist;
    // the candidate root adds a component that does not exist, so `realpathSync`
    // throws ENOENT and the anchor falls back to the WRITTEN form — which is
    // inside the allowed prefix. The guard therefore says `allowed: true` about
    // a path whose real location is `outsideRoot`.
    const escapeLink = join(root, 'outside-link');
    symlinkSync(outsideRoot, escapeLink);
    const unresolvableRoot = join(escapeLink, 'not-created-yet');

    // PREMISE, against the real Phase 1 guard — this is what makes the cell
    // non-vacuous, and it is the exact state the anchor's header warns about.
    const verdict = classifySweepRoot(unresolvableRoot, { allowedPrefixes: [root] });
    expect(verdict.allowed).toBe(true);
    expect(verdict.resolvedVia).toBe('fallback');
    expect(verdict.resolvedDir).toBe(unresolvableRoot);

    const result = await sweep(unresolvableRoot, [root]);

    expect(result.removedCount).toBe(0);
    expect(result.reclaimedBytes).toBe(0);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].kind).toBe('unresolved-root-refused');
    expect(result.errors[0].path).toBe(unresolvableRoot);
    // Distinct from the configuration-error refusal — see this file's header
    // for why collapsing the two would be a real loss of signal.
    expect(result.errors[0].kind).not.toBe('confinement-refused');
  });

  test('a DANGLING symlink root — allowed, unresolvable — is refused rather than walked', async () => {
    // The anchor's own real-fs spec pins this as `allowed: true`,
    // `resolvedVia: 'fallback'`. It costs nothing to refuse: there is nothing
    // behind a dangling link to reclaim.
    const dangling = join(root, 'dangling-link');
    symlinkSync(join(root, 'never-created'), dangling);

    const verdict = classifySweepRoot(dangling, { allowedPrefixes: [root] });
    expect(verdict.allowed).toBe(true);
    expect(verdict.resolvedVia).toBe('fallback');

    const result = await sweep(dangling, [root]);

    expect(result.removedCount).toBe(0);
    expect(result.errors[0].kind).toBe('unresolved-root-refused');
  });

  test('a simply NONEXISTENT root is refused on the same gate, not reported as a listing failure', async () => {
    const missing = join(root, 'no-such-root-1691');
    expect(classifySweepRoot(missing, { allowedPrefixes: [root] }).resolvedVia).toBe('fallback');

    const result = await sweep(missing, [root]);

    expect(result.removedCount).toBe(0);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].kind).toBe('unresolved-root-refused');
  });

  test('the gate is a REFUSAL, not a crash — the promise resolves and the report is well formed', async () => {
    // The sweep runs inside `cli.mjs`, whose entire stdout the PreToolUse hook
    // `JSON.parse`s and fails closed on. A rejected promise here would surface
    // as an unhandled rejection and a raw stack trace on that stream, denying
    // every agent spawn on the host.
    const result = await sweep(join(root, 'no-such-root-1691'), [root]);

    expect(result).toEqual({
      removedCount: 0,
      reclaimedBytes: 0,
      errors: [expect.objectContaining({ kind: 'unresolved-root-refused' })],
      truncatedCount: 0,
      budgetExhausted: false,
      candidatesCapped: false,
      sizeUnmeasuredCount: 0,
      liveWriterSkippedCount: 0,
      probeTruncatedCount: 0,
      removalIssuedUnconfirmedCount: 0,
      // FALSE, and load-bearing (Phase 3 review round 2). This gate
      // refused before listing anything, so the report must not be readable as
      // "swept everything, found nothing" — otherwise the futile-sweep
      // cooldown stands the trigger down for half an hour and this refusal,
      // currently emitted on every beat, goes silent after its first sighting.
      completed: false,
    });
  });
});

describe('SENTINEL — this file, which performs REAL removals, can never reach the host temp root', () => {
  test('the process`s real os.tmpdir() is refused, while an eligible candidate sits untouched in this file`s own scratch root', async () => {
    const candidate = seedStaleCandidate(root, 'cdk.out-must-survive');

    const result = await sweep(tmpdir(), [root]);

    expect(result.removedCount).toBe(0);
    expect(result.errors[0].kind).toBe('confinement-refused');
    expect(existsSync(candidate)).toBe(true);
  });
});
