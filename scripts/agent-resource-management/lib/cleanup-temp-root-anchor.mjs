// Phase 1 — the env-independent system-temp-root anchor.
//
// ---------------------------------------------------------------------------
// WHY THIS MODULE EXISTS (and why it is NOT a duplicate of the guard next door)
// ---------------------------------------------------------------------------
//
// `cleanup.mjs` already carries a confinement guard: `isConfinedToTmpRoot()`
// compares the CLI's `--dir` against `resolveTmpRoot()`, which is
// `process.env.TMPDIR || os.tmpdir()`. Read its doc comment (the block
// immediately above `isConfinedToTmpRoot` in `cleanup.mjs`) together with this
// header — the two are one argument split across two call paths, not drift:
//
//   * For the STANDALONE CLI that guard is sound. `--dir` arrives from the
//     operator, INDEPENDENTLY of the TMPDIR variable, so comparing the two
//     is a real comparison: a mistyped `--dir` that lands on the repo's own
//     `infra/cdk.out` fails the check and nothing is removed.
//
//   * For its AUTO-TRIGGER that same guard is VACUOUS. The swept `dir`
//     derives from the very variable the guard measures it against, so both
//     sides move together. Point the TMPDIR variable at
//     `/Users/…/example-project/infra` and "confinement" passes while the sweep
//     recursively removes the repo's real, gitignored `infra/cdk.out`. A
//     RELATIVE value compounds it: it resolves against `process.cwd()`, which
//     the PreToolUse hook pins to the repo root.
//
// So this module compiles its yardstick from `process.platform` ALONE. The
// TMPDIR variable becomes an INPUT TO BE VALIDATED, never the standard it is
// validated against. This file consequently reads no environment at all — a
// property held structurally by
// `./cleanup-temp-root-anchor-source-scan.jest.spec.mjs`, because a negative
// ("depends on nothing in the environment") cannot be exhausted by enumerating
// the variables a test author happened to think of.
//
// THE REPO EXCLUSION IS DESCENDANT-ONLY, AND THAT IS NOT THE WHOLE ANSWER. It
// refuses roots INSIDE this repository; it deliberately ALLOWS a root that
// CONTAINS it. In the CI-under-temp arrangement — checkout at
// `/private/tmp/<repo>` — `classifySweepRoot('/private/tmp')` is `allowed:
// true`, because refusing every ancestor of the checkout would refuse the
// host's own temp root and make the auto-trigger inert. So confinement alone
// does NOT bound the sweep to non-repo files: Phase 2's `cdk.out-*` candidate
// NAME filter is what keeps an allowed-but-containing root from walking into
// the working tree, and it is therefore SECURITY-RELEVANT and load-bearing, not
// merely selective. Do not relax it on the grounds that "confinement already
// refuses the repo" — it refuses the repo as a ROOT, not as a descendant of one.
//
// ---------------------------------------------------------------------------
// FILESYSTEM BUDGET: ONE READ, NOTHING ELSE
// ---------------------------------------------------------------------------
//
// The module is non-destructive by contract — it never removes, lists, or
// writes anything. Its only filesystem call is AT MOST ONE `realpathSync` per
// classification (an unsupported platform, or an unusable candidate string,
// refuses before spending it), with a `path.resolve()` fallback when that
// throws. The repo-root exclusion below is derived from `import.meta.url` by
// path arithmetic only: a second `realpath` would be a second call on a hot
// beat path and a second thing that can throw mid-verdict.
//
// THE RESIDUAL THAT BUYS (disclosed, not yet closed). Deriving `REPO_ROOT`
// arithmetically leaves it in whatever form the LOADER handed us, while the
// candidate it is compared against HAS been realpath-resolved. Node's default
// module resolution realpaths the specifier, so the two forms agree and the
// exclusion holds — but that is a PRECONDITION, not an invariant of this file.
// Under `NODE_OPTIONS=--preserve-symlinks` (or any loader that preserves them)
// a symlinked checkout yields an unresolved `REPO_ROOT`, the fold-compare then
// misses a candidate that resolves INTO the working tree, and the exclusion is
// bypassed. Closing it means realpath'ing `REPO_ROOT` at module load, which
// collides with the "exactly one `realpathSync(` call site" assertion in
// `./cleanup-temp-root-anchor-source-scan.jest.spec.mjs` — so it is a
// considered change to that spec and is tracked as follow-up, not smuggled in
// here. Do not deploy this module under `--preserve-symlinks` until it lands.
//
// ---------------------------------------------------------------------------
// `resolvedDir` IS ONLY A REAL LOCATION WHEN `resolvedVia === 'realpath'`
// ---------------------------------------------------------------------------
//
// On the realpath-SUCCESS branch, `resolvedDir` is the candidate's true
// location, so a caller that walks it walks exactly the path that was guarded —
// that is what closes the "confined path ≠ walked path" symlink divergence.
//
// On the FALLBACK branch it is not. `realpathSync` throws ENOENT as soon as any
// component of the path is missing, including the final one, and the
// `path.resolve()` fallback then keeps the candidate's WRITTEN form with every
// symlink in it unresolved. A candidate like `/private/tmp/link/absent`, where
// `link` points outside a temp root, is therefore reported `allowed: true` with
// a `resolvedDir` whose real location is somewhere else entirely.
//
// That is safe for CLASSIFICATION (a path with a missing component has nothing
// to sweep) but it is NOT safe to walk. Hence the `resolvedVia` field:
//
//   PHASE 2 MUST REQUIRE `resolvedVia === 'realpath'` BEFORE LISTING OR
//   REMOVING ANYTHING UNDER `resolvedDir`.
//
// It costs no capability — a root that could not be realpath-resolved has a
// missing component, so there is nothing there to reclaim.
//
// ---------------------------------------------------------------------------
// THE `allowedPrefixes` OPTION IS A TEST-ONLY INJECTION SEAM
// ---------------------------------------------------------------------------
//
// `allowedPrefixes` exists so a spec can nominate its own scratch root as an
// allowable prefix. It is test-only: production callers must never populate it
// — not from an environment variable, not from argv, not from the coordination
// file.
//
// When supplied it REPLACES the platform list. That is emphatically NOT the
// same thing as narrowing it: `classifySweepRoot('/Users/alice/scratch', {
// allowedPrefixes: ['/Users'] })` returns `allowed: true` — a home-directory
// tree, nowhere near any temp root — so the seam can nominate ARBITRARY roots
// anywhere on the filesystem. It is a widening primitive as readily as a
// narrowing one. (Verified return value, not an illustration.)
//
// WHAT `'/'` AS A PREFIX DOES AND DOES NOT DO — stated precisely, because an
// earlier revision of this header overclaimed it. A `'/'` prefix cannot widen
// the allowlist to cover paths UNDER root: the `sep`-append in `isWithin` makes
// the comparison string `'//'`, which no resolved path starts with (see that
// function's doc comment). But root ITSELF is reachable, via `isWithin`'s
// `candidatePath === boundary` arm — `classifySweepRoot('/', {
// allowedPrefixes: ['/'] })` returns `{ allowed: true, resolvedDir: '/',
// reason: null, resolvedVia: 'realpath' }` (verified on Node 22.23.0, macOS).
// Every OTHER absolute prefix widens normally, root included as a single point.
//
// THE IMPLICATION FOR PHASE 2: do not rely on this module to refuse `/` as a
// sweep root. Confinement plus the `cdk.out-*` candidate NAME filter are what
// bound the sweep — that filter is load-bearing here too, not just for the
// allowed-but-containing root discussed above.
//
// It is unreachable in production regardless: `SYSTEM_TEMP_ROOT_PREFIXES` never
// contains `'/'`, and the seam is test-only — a property Phase 4's provenance
// fitness function enforces mechanically. `./cleanup-async-confinement-anchor.
// jest.spec.mjs` pins both halves (root allowed via the seam, root refused by
// the platform default) so this prose and the code cannot drift apart again.
//
// That is precisely WHY it must never be reachable from the environment, argv,
// or the coordination file — anything that plumbs it through from configuration
// hands a session-controlled recursive-removal target to the beat path and
// undoes this whole module. Phase 4's provenance fitness function is therefore
// LOAD-BEARING, not belt-and-braces; do not read it as a formality and do not
// conclude from "it's just a test seam" that config could safely supply it.
//
// The one thing the seam cannot do is rescue a path inside this repository: the
// repo-root exclusion below is evaluated before the prefix check and applies
// however the prefix list was chosen.

import { realpathSync } from 'node:fs';
import { dirname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * Real operating-system temp roots, keyed by `process.platform`. Both written
 * forms of each macOS symlink pair are listed, so a candidate is allowable
 * whether the kernel resolved it or not. An unrecognised platform contributes
 * no prefixes at all, which refuses everything — fail-closed on the
 * DESTRUCTIVE action (the opposite polarity to the admission gate, and the
 * correct one here).
 *
 * Frozen at both levels: neither the map nor any list may be widened at
 * runtime by a later caller.
 *
 * @type {Readonly<Record<string, readonly string[]>>}
 */
export const SYSTEM_TEMP_ROOT_PREFIXES = Object.freeze({
  darwin: Object.freeze(['/private/tmp', '/private/var/folders', '/tmp', '/var/folders']),
  linux: Object.freeze(['/tmp', '/var/tmp']),
});

/** lib -> agent-resource-management -> scripts -> repo root. Path arithmetic only. */
const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..', '..');

/** The NUL byte itself, built via char code so no editor or diff tool can eat it. */
const NUL = String.fromCharCode(0);

/**
 * Whether a value is a path string this module is willing to reason about.
 *
 * A NUL byte is rejected explicitly rather than left to `path.resolve`: the
 * platform-independent `resolve` happily returns a NUL-bearing absolute path,
 * while every `node:fs` call rejects it with `ERR_INVALID_ARG_VALUE`. Treating
 * it as a legal path would mean handing Phase 2 a string it can only throw on.
 *
 * @param {unknown} value
 * @returns {boolean}
 */
function isUsablePathString(value) {
  return typeof value === 'string' && value.length > 0 && !value.includes(NUL);
}

/**
 * Normalises a path into the comparable form: absolute, no trailing separator,
 * no doubled separator. Returns `null` for anything unusable as a path (a
 * NUL-embedded string, a non-string, the empty string).
 *
 * @param {unknown} value
 * @returns {string | null}
 */
function normalisePath(value) {
  if (!isUsablePathString(value)) return null;
  try {
    return resolve(value);
  } catch {
    return null;
  }
}

/**
 * `path.sep`-aware containment. A bare `startsWith` would let `/tmpfoo` match
 * `/tmp`, which is the whole prefix-adjacency attack.
 *
 * NON-OBVIOUS CONSEQUENCE OF THE `sep`-APPEND: a boundary of `'/'` matches
 * NOTHING BUT ITSELF. The comparison string becomes `'//'`, which no resolved
 * path starts with (`resolve` collapses doubled separators), and `'/tmp/foo'`
 * is not `===` `'/'`. So a `'/'` prefix cannot widen the allowlist to cover
 * paths UNDER root — every OTHER absolute prefix widens normally.
 *
 * BUT ROOT ITSELF IS STILL REACHABLE, through the `candidatePath === boundary`
 * arm immediately below: `classifySweepRoot('/', { allowedPrefixes: ['/'] })`
 * is `allowed: true` with `resolvedDir: '/'` and `resolvedVia: 'realpath'`.
 * This function does not, and is not asked to, refuse `/` as a sweep root;
 * Phase 2 must not assume it does (the `cdk.out-*` candidate NAME filter is
 * what bounds the sweep). Unreachable in production — the platform lists never
 * contain `'/'` and the seam is test-only — and pinned by a cell in
 * `./cleanup-async-confinement-anchor.jest.spec.mjs`.
 *
 * @param {string} candidatePath
 * @param {string} boundary
 * @returns {boolean}
 */
function isWithin(candidatePath, boundary) {
  return candidatePath === boundary || candidatePath.startsWith(`${boundary}${sep}`);
}

/**
 * Folds a path into the form used for the REPO-ROOT EXCLUSION ONLY.
 *
 * WHY THIS EXISTS. `resolve()` and `realpathSync` are both byte-faithful about
 * case and Unicode form: `realpathSync` canonicalises symlinks, not the
 * spelling of components it never had to traverse. On a case-insensitive,
 * normalisation-insensitive volume (APFS's default, and NTFS) two different
 * spellings therefore name ONE directory while comparing unequal:
 *
 *   /private/tmp/<checkout>/MyRepo/infra  -> refused (exact match on the repo)
 *   /private/tmp/<checkout>/MYREPO/infra  -> ALLOWED, same directory on disk
 *   .../Rép/infra (NFC) vs .../Re + U+0301 p/infra (NFD)  -> same, ditto
 *
 * Both bypasses were reproduced under a real `/private/tmp` prefix on Node
 * 22.23.0, which is why this is not a theoretical hardening: the candidate
 * derives from the TMPDIR variable, whose spelling the session chooses freely.
 *
 * WHY IT IS APPLIED UNCONDITIONALLY, NOT ONLY ON `darwin`/`win32`. Folding can
 * only ever ADD refusals, never remove one, so there is no widening risk in
 * applying it everywhere — and case-insensitivity is not actually a property of
 * the PLATFORM. Linux ext4/f2fs support per-directory casefolding (`+F`,
 * kernel 5.2), and case-insensitive network/FUSE mounts exist on every
 * platform, so a `platform === 'darwin' || platform === 'win32'` test would
 * still miss a real case-insensitive checkout on Linux. The only cost of
 * folding on a genuinely case-sensitive volume is refusing a sweep root whose
 * spelling is a case variant of the repo root — a path nobody has reason to use
 * as a temp root, and a refusal rather than a removal.
 *
 * WHY IT IS NOT APPLIED TO THE PREFIX ALLOWLIST. Folding the allowlist would
 * make MORE paths match an allowed prefix, i.e. WIDEN the destructive surface.
 * The asymmetry is deliberate and must be preserved: fold the exclusion, never
 * the allowlist.
 *
 * `toLowerCase` (not `toLocaleLowerCase`) is deliberate — it applies the
 * locale-independent Unicode default case mapping, so the verdict cannot move
 * with the host's locale (the Turkish dotless-i hazard).
 *
 * @param {string} pathValue
 * @returns {string}
 */
function foldForRepoExclusion(pathValue) {
  return pathValue.normalize('NFC').toLowerCase();
}

/**
 * Chooses the prefix list that governs this classification.
 *
 * A nullish `allowedPrefixes` is the ordinary production case — the platform
 * list governs. An array REPLACES it (entries that are not usable absolute
 * prefixes are discarded, which can only narrow). Any other malformed value is
 * treated as absent, because the one outcome that must never follow from
 * malformed input is a WIDER allowlist.
 *
 * @param {string[] | undefined} platformPrefixes
 * @param {unknown} allowedPrefixes
 * @returns {string[]}
 */
function selectPrefixes(platformPrefixes, allowedPrefixes) {
  if (allowedPrefixes === undefined || allowedPrefixes === null) return platformPrefixes;
  if (!Array.isArray(allowedPrefixes)) return platformPrefixes;
  return allowedPrefixes.map(normalisePath).filter((prefix) => prefix !== null);
}

/**
 * Decides whether `candidateDir` is inside a real operating-system temp root,
 * using an allowlist compiled from `platform` and never from the environment.
 *
 * Never throws, for any input. Performs at most one filesystem read.
 *
 * CALLERS MUST GATE ON `allowed`. `resolvedDir` is the EMPTY STRING for the
 * refusals that never got as far as resolving anything (`unsupported-platform`,
 * and `unresolvable` for a non-string/empty/NUL-bearing candidate). An empty
 * string is not an inert value downstream, though NOT by the route one might
 * assume: a DIRECT `readdir('')`/`rm('')` throws `ENOENT` immediately — Node
 * does not resolve `''` to the cwd for fs calls. The real hazard is
 * RELATIVE-PATH COMPOSITION. A caller that builds child paths the ordinary way,
 * `join(resolvedDir, name)`, gets `join('', 'cdk.out-1') === 'cdk.out-1'` — a
 * RELATIVE path, which every fs call then resolves against `process.cwd()`.
 * Likewise `path.resolve('')` IS `process.cwd()`, so a caller that resolves
 * before using lands there directly. The PreToolUse hook pins that cwd to the
 * repo root, so either route aims the sweep at the working tree. A caller that
 * reads `resolvedDir` without checking `allowed` therefore walks the repo.
 *
 * `resolvedVia` reports HOW `resolvedDir` was obtained, and is `null` whenever
 * `resolvedDir` is `''`:
 *
 *   'realpath' — fully resolved; `resolvedDir` is the candidate's real location
 *                and is safe to walk.
 *   'fallback' — `realpathSync` threw (a missing path component, typically), so
 *                `resolvedDir` is the candidate's own written form with symlinks
 *                UNRESOLVED. See the header: PHASE 2 MUST REQUIRE
 *                `resolvedVia === 'realpath'` BEFORE WALKING `resolvedDir`.
 *
 * @param {unknown} candidateDir
 * @param {{ platform?: unknown, allowedPrefixes?: unknown }} [options]
 * @returns {{ allowed: boolean, resolvedDir: string,
 *   reason: null | 'outside-system-temp-root' | 'unresolvable' | 'unsupported-platform',
 *   resolvedVia: null | 'realpath' | 'fallback' }}
 */
export function classifySweepRoot(candidateDir, options = {}) {
  const { platform = process.platform, allowedPrefixes } =
    options && typeof options === 'object' ? options : {};

  // The platform check dominates the prefix check and costs no filesystem
  // call: if nothing can ever be allowed here, there is nothing to resolve.
  const platformPrefixes =
    typeof platform === 'string' ? SYSTEM_TEMP_ROOT_PREFIXES[platform] : undefined;
  if (!Array.isArray(platformPrefixes)) {
    return { allowed: false, resolvedDir: '', reason: 'unsupported-platform', resolvedVia: null };
  }

  // Input validation, also before any filesystem call — the SAME predicate
  // `normalisePath` applies, so "unusable input costs no filesystem call" holds
  // uniformly (a NUL-bearing string included) rather than only for the two
  // conditions this branch used to restate.
  if (!isUsablePathString(candidateDir)) {
    return { allowed: false, resolvedDir: '', reason: 'unresolvable', resolvedVia: null };
  }

  // THE SINGLE FILESYSTEM CALL SITE. A throw here (non-existent path, dangling
  // symlink, a native binding throwing a bare string) is contained, and the
  // `path.resolve` fallback classifies the candidate on its own written form.
  let resolvedRaw;
  try {
    resolvedRaw = realpathSync(candidateDir);
  } catch {
    resolvedRaw = undefined;
  }

  const fullyResolved = normalisePath(resolvedRaw);
  const resolvedDir = fullyResolved ?? normalisePath(candidateDir);
  if (resolvedDir === null) {
    return { allowed: false, resolvedDir: '', reason: 'unresolvable', resolvedVia: null };
  }
  // Symlinks in `resolvedDir` are only known to be resolved on this branch; the
  // fallback keeps the written form. Phase 2 gates its walk on this.
  const resolvedVia = fullyResolved === null ? 'fallback' : 'realpath';

  // Repo exclusion, evaluated AFTER resolution so a symlink from inside a temp
  // root into the working tree is caught by where it actually goes. A CI runner
  // may legitimately hold the checkout under a real temp root; prefix logic
  // alone would then allow recursive removal inside the working tree, which is
  // the single worst outcome this module prevents. The repo is not a system temp
  // root, so it earns that reason code.
  //
  // Compared under `foldForRepoExclusion` (see its doc comment) so a case or
  // Unicode-form variant of the repo's own path cannot name the same directory
  // on disk while comparing unequal here. The FOLD IS ON THE EXCLUSION ONLY —
  // the prefix allowlist below is compared unfolded, because folding it would
  // widen rather than narrow.
  if (isWithin(foldForRepoExclusion(resolvedDir), foldForRepoExclusion(REPO_ROOT))) {
    return { allowed: false, resolvedDir, reason: 'outside-system-temp-root', resolvedVia };
  }

  const prefixes = selectPrefixes([...platformPrefixes], allowedPrefixes);
  const allowed = prefixes.some((prefix) => isWithin(resolvedDir, prefix));

  return allowed
    ? { allowed: true, resolvedDir, reason: null, resolvedVia }
    : { allowed: false, resolvedDir, reason: 'outside-system-temp-root', resolvedVia };
}
