// Phase 3 — fitness function: catch future unused-export instances.
//
// WHY THIS EXISTS (Ford/Parsons, *Building Evolutionary Architectures*):
// this ticket retired `coreCount`/`parseResourceCapacity` (Phase 1, deleted
// whole) — a named export from lib/**/*.mjs with ZERO production callers
// anywhere (not inside its own defining file, not from any other
// production file) whose only reference of any kind was its own
// `*.jest.spec.mjs` test block. That shape (production export, consumed by
// nothing except a spec) is a rule a human reviewer will not reliably
// catch by inspection across ~30 lib files and ~60 sibling spec files — so
// it becomes a fitness function instead of a review checklist item. This
// file IS both the "test" and the "implementation" for this phase; there
// is no separate hand-written checker module elsewhere — the checker
// function lives here, and its own fixture-based self-tests double as the
// acceptance evidence that the checker does what it claims.
//
// NOTE on what counts as "production usage": an export referenced only
// WITHIN its own defining file (e.g. a module-private constant exported
// solely so a test can assert against the same literal) is NOT flagged —
// that is a live, in-use export, materially different from
// `parseResourceCapacity`, which had no callers at all, inside its file or
// out. See `findDeadLibExports`'s own doc comment below for the precise
// rule and why `usedInternally` is checked as a distinct signal from
// cross-file production references.
//
// SCOPE — deliberately pinned, not incidental:
//   Glob: exactly `scripts/agent-resource-management/lib/**/*.mjs`
//   (production files only). This explicitly EXCLUDES:
//     - `*.jest.spec.mjs` files themselves (the test-only references this
//       check is hunting for, not export sources to check).
//     - `scripts/agent-resource-management/cli.mjs` — excluded from this
//       directory's coverage config for its own separate reasons (see jest.config's
//       coveragePathIgnorePatterns for this directory, if present, or the
//       Build Plan's own note). A future widening of this glob to sweep in
//       `cli.mjs` (or any other directory) must be an explicit, deliberate
//       code change to the GLOB constant below — never silent. Convergence
//       analysis on this ticket flagged exactly this risk.
//
// WHAT THIS CHECK DOES NOT DO (explicit non-goals for v1):
//   - It does NOT catch field-level dead code — a value assigned to an
//     object property but never destructured downstream (e.g.
//     `compressedMb`'s ORIGINAL bug shape, before Phase 2 of this ticket
//     wired it into the printed JSON). Detecting that requires data-flow
//     tracing through object literals/spreads/destructuring, a materially
//     harder static-analysis problem than import/export graph walking.
//     Out of scope for v1; a future iteration could add it, but this file
//     does not claim to.
//   - It is a lightweight import/export graph walk over regex-extracted
//     names, not a real JS parser/type-checker. It will not understand
//     re-exports (`export { foo } from './other.mjs'`), computed member
//     access, or dynamic `import()` of a bare specifier built at runtime.
//     None of those patterns exist in this directory's lib files today
//     (see the real-world assertion below), but a future author adding one
//     should be aware this checker will not follow it.
//
// RELATIONSHIP TO GENERAL ESLINT COVERAGE (there is currently no ESLint
// coverage for scripts/**/*.mjs at all): this Jest-based check is a
// NARROWER, STANDALONE supplement, not blocked on general lint coverage
// landing. General lint coverage would be about lint concerns (unused
// vars, style, etc.) across ALL `.mjs` scripts in the repo; this check is
// scoped specifically to `lib/**/*.mjs`'s cross-file EXPORT usage, which no
// ESLint rule out of the box detects either (`no-unused-vars` only sees
// unused *local* bindings, not unused *exports* referenced solely by a test
// file). Once general `.mjs` lint coverage lands, this check should be
// reconciled with — or superseded by — an ESLint
// `eslint-plugin-unused-imports`/`no-unused-exports`-style rule that can
// express the same "test-only reference" rule directly in lint config.
// Until then, this file is the mechanism.
//
// MUST NOT filename-match to find "own spec" — this repo already has specs
// that test a differently-named module by convention, e.g.
// lib/eviction-authorization.jest.spec.mjs imports from ./recon.mjs (not
// eviction-authorization.mjs, which does not exist), and
// lib/real-evict-kill.jest.spec.mjs imports from ./real-evict-adapters.mjs.
// A filename-matching heuristic would silently UNDER-enforce exactly this
// shape — the wrong direction for a bug-catching tool. `findDeadLibExports`
// below treats ANY `*.jest.spec.mjs` file's references as "test-only",
// regardless of its name relative to the module being checked. Positive
// case 2 below pins this directly.

import { mkdtemp, mkdir, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const HERE = dirname(fileURLToPath(import.meta.url));

// Deliberately pinned scope — see header comment. Do not widen this without
// an explicit decision recorded here.
const REAL_LIB_DIR = join(HERE, 'lib');

// KNOWN PRE-EXISTING EXCEPTIONS — exports this checker correctly flags as
// having zero production callers, discovered as a side effect of fixing the
// checker's comment-stripping blind spot during its own review (a
// doc-comment self-mention was previously miscounted as "used internally").
// These 4 predate and are NOT its three originally-scoped signals
// (`coreCount`, `compressedMb`, `swapTotalMb`) — deciding whether each is
// deliberate (like `computeHeadroomCap`, which `doc-honesty.jest.spec.mjs`
// already pins as an intentional Phase 2 retirement) or genuinely
// accidental dead code needs its own per-export investigation, out of
// its scope. Tracked in.
//
// This allowlist must SHRINK over time, not grow silently — test 4 below
// asserts the real checker's findings match this list EXACTLY, so fixing
// (or confirming-and-documenting) any entry via requires removing it
// here too. A genuinely new, untracked dead export still fails the test.
const KNOWN_PRE_EXISTING_EXCEPTIONS = [
  { file: 'lib/allowance.mjs', exportName: 'computeHeadroomCap' },
  { file: 'lib/footprint-state.mjs', exportName: 'readFootprintState' },
  { file: 'lib/footprint-trajectory.mjs', exportName: 'createFootprintTrajectory' },
  { file: 'lib/recon.mjs', exportName: 'attributeAgentProcesses' },
  // NOT pre-existing — newly dead AS OF THIS DIFF, not inherited from an
  // earlier ticket (unlike every other entry in this list). Phase 3
  // is what switched `resolveRecentlyEvictedPids` (cli.mjs)'s only
  // production call from `readLivenessLog` (eager, whole-file parse) to the
  // new bounded `readLivenessLogTail` (backward walk, stops at the first
  // unparseable line) — `readLivenessLog` had a live production caller
  // immediately before this ticket's own Phase 3 change removed it.
  // `readLivenessLog` remains a legitimate, actively-used whole-file read
  // primitive for 6 *.jest.spec.mjs files' own verification needs, but has
  // zero production callers left. Tracked in.
  { file: 'lib/liveness-log.mjs', exportName: 'readLivenessLog' },
  // NOT introduced by — `clearFootprintState` had zero production
  // callers (and zero test references at all) on master already; its
  // own Phase 1 test-writing added the first real test coverage for it
  // (footprint-state.jest.spec.mjs now imports and calls it), which is what
  // newly satisfies this checker's "referenced by a test, zero production
  // usage" flag condition — a pre-existing dead-in-production export simply
  // became visible to this fitness function once it was, for the first
  // time, exercised by a test. Mirrors `readFootprintState`'s own entry
  // just above (same file, same shape of gap). Not this ticket's bug to fix
  // — is about write-side fencing, not resurrecting or deleting an
  // unrelated unused function.
  { file: 'lib/footprint-state.mjs', exportName: 'clearFootprintState' },
];

// IN-FLIGHT, NOT PRE-EXISTING — deliberately a SEPARATE list from
// KNOWN_PRE_EXISTING_EXCEPTIONS (which is its baseline and must only
// shrink). These are exports landed by an in-progress multi-phase build whose
// production consumer lands in a LATER phase of the SAME ticket, so between
// those two phases the checker correctly sees "referenced only by a spec".
//
// Every entry must name the ticket and the phase that removes it. An entry
// still here after its ticket closes is exactly the dead export exists
// to catch, and the exact-match assertion below will not let it be forgotten:
// once the consumer lands, the finding disappears and this list must be
// emptied or the test fails.
//
//   (Currently empty, which is the desired steady state.)
//
//   RESOLVED — Phase 1's `hasEverOptedIn` (design decision 1, the read
//   half of the sticky ever-opted-in ledger entry) was listed here until its
//   production consumer, the PreToolUse hook `hooks/pretooluse-arm-gate.mjs`,
//   landed in Phase 3. Its write half, `recordEverOptedIn`, was already wired
//   into `declareDibs`. Entry removed by Phase 3 exactly as this list's own
//   contract requires.
const IN_FLIGHT_PHASED_EXCEPTIONS = [
  // Phase 2's `selectStaleTempDirs` (lib/cleanup-age-filter.mjs) — its
  // deferred production consumer, `lib/cleanup.mjs`, landed in Phase 3
  // (calls it for real from `sweepStaleTempDirs`). Entry removed accordingly.
  // Phase 4 shipped the pure classification module
  // (`classifyIdleProbeTrajectory`) plus its own fixture-based spec; the
  // real production consumer — wiring it into cli.mjs's multi-beat
  // sampling/polling loop in main() — is deliberately deferred to a later
  // phase (documented in this directory's SKILL.md, commit b2426474, as
  // "not yet wired up"). Remove this entry once cli.mjs's sampling loop
  // calls `classifyIdleProbeTrajectory` for real.
  { file: 'lib/idle-probe-trajectory.mjs', exportName: 'classifyIdleProbeTrajectory' },
  // `applyIdleProbeOutcome`, the other export added by the same file, is
  // JUST AS UNUSED in production as `classifyIdleProbeTrajectory` above
  // (cli.mjs's main() calls neither — same deferred sampling-loop wiring).
  // It is deliberately NOT listed as an entry here, and that is a disclosed
  // gap, not an oversight: the real checker's `findDeadLibExports` does not
  // flag it, because its own-file guard clause does
  // `throw new Error('applyIdleProbeOutcome: ...')`, and the checker's
  // `usedInternally` heuristic counts ANY occurrence of the export's name in
  // its own file — including inside a string literal — as a real reference.
  // That is a false-positive "used" signal, not genuine production wiring.
  // Adding a matching allowlist entry here anyway would make test 5's
  // exact-match assertion FAIL (the real findings and the allowlist would
  // diverge), because the checker's actual findings genuinely omit it. This
  // loophole is disclosed here in the comment rather than quietly relied
  // upon or silently patched around, and is tracked separately as follow-up
  // work (fix the heuristic to require a real reference, e.g. a call/import,
  // not a string-literal substring match). Do not add an allowlist entry for
  // `applyIdleProbeOutcome` — it would break test 5's exact-match invariant
  // against the real checker output.
  //
  // --- end of the idle-probe disclosure ---------------------------------
  //
  // Phase 1 shipped the pure, env-independent system-temp-root anchor
  // (`lib/cleanup-temp-root-anchor.mjs`) plus its four specs, and
  // `classifySweepRoot` was listed here while it waited for a consumer. Phase 2
  // is that consumer: `lib/cleanup.mjs`'s `sweepStaleTempDirsAsync` calls it
  // before any listing. Entry removed accordingly, exactly as this list's own
  // contract requires.
  //
  // (It is reached through a lazy `await import('./cleanup-temp-root-anchor.mjs')`
  // rather than a static import — see `loadTempRootAnchor`'s doc comment in
  // `lib/cleanup.mjs` for why — which makes no difference to this checker: the
  // identifier appears in comment-stripped production source outside its own
  // defining file, which is what `referencedByOtherProduction` measures.)
  //
  // POSITION IS LOAD-BEARING: entries sit AFTER the disclosure block ends,
  // because that block's prose says "the other export added by the same file"
  // and refers to `lib/idle-probe-trajectory.mjs`. An earlier revision inserted
  // an entry between the idle-probe entry and its own continuation comment,
  // which made "the same file" misread as pointing at another module. Keep new
  // entries at the end of the array.
  //
  // RESOLVED — Phase 2 landed the budgeted async sweep in
  // `lib/cleanup.mjs`, and `AUTO_TRIGGER_AGE_HOURS` + `sweepStaleTempDirsAsync`
  // were listed here while they waited for their deferred consumer. Phase 3 is
  // that consumer: `cli.mjs`'s `maybeSweepStaleTempRoot` calls
  // `sweepStaleTempDirsAsync({ ageThresholdMs: AUTO_TRIGGER_AGE_HOURS *
  // CLEANUP_HOUR_MS, ... })` for real from the disk axis's AMBER cleanup
  // response. Both entries removed accordingly — not optional bookkeeping: a
  // stale entry fails test 5's exact-match assertion just as a new dead export
  // would (the plan's build-order hazard 1).
  //
  // Only TWO of Phase 2's seven new exports ever needed entries. The other five
  // — `SWEEP_BUDGET_MS`, `SWEEP_MAX_CANDIDATES`, `SWEEP_MAX_ERRORS`,
  // `LIVE_WRITE_MARGIN_MS` and `DEFAULT_AGE_HOURS` — are each read by
  // `lib/cleanup.mjs` itself (as a parameter default, a ring bound, a liveness
  // comparison, or the CLI's own `--age-hours` default), so
  // `findDeadLibExports`'s `usedInternally` signal already covered them.
  //
  // Phase 3's own two new `lib/coordination-file.mjs` exports —
  // `recordDiskCleanupArmed` and `hasDiskCleanupArmed` — need no entries
  // either: `cli.mjs` calls both in the same phase that introduces them
  // (`handleEnableDiskCleanup` and `diskCleanupArmedForBeat`), so there is no
  // between-phases window in which they are spec-only.
  //
  // VERIFIED CLEAN — Phase 4 re-ran this checker as its "dead-export
  // truthfulness" work item and made no allowlist change, which is the outcome
  // the build plan anticipated. Recorded here rather than left implicit,
  // because "we looked and there was nothing to do" and "we never looked" are
  // indistinguishable from a diff.
  //
  // Note WHY no manual audit was needed to establish that: test 5 asserts EXACT
  // EQUALITY between the real checker's findings and this allowlist, not a
  // subset relation. So this file being green IS the proof that the list is
  // accurate in both directions at once — an entry describing an export that
  // has since been wired up fails it just as loudly as a genuinely-dead export
  // missing from the list would. The one acknowledged hole in that guarantee is
  // the `applyIdleProbeOutcome` heuristic false-positive disclosed above
  //, which is a limitation of the CHECKER, not of this list.
];

/**
 * Strips `//` line comments and `/* *\/` block comments (including JSDoc)
 * from a source string before occurrence-counting.
 *
 * Why this exists: without it, an export mentioned only in its own JSDoc
 * (e.g. `/** doOrphanThing exists for future use. *\/`) would register as
 * "used internally" purely from the doc-comment text, even with zero real
 * callers — the exact bug shape (`parseResourceCapacity`) this checker
 * exists to catch, reappearing via a comment mention instead of a test
 * reference. Deliberately simple (not comment-vs-string-literal aware); see
 * header comment for this checker's general "regex, not a parser" scope.
 *
 * KNOWN LIMITATION (not currently active — see real-world assertion below):
 * not string-literal-aware, so a `//` inside a string (e.g. a `https://`
 * URL) on an otherwise-live code line would blank the rest of that line too,
 * potentially hiding a genuine reference and causing a false positive (a
 * live export flagged as dead). No such pattern exists in
 * `lib/**\/*.mjs` today. A real parser would close this gap; out of scope
 * for this "regex, not a parser" checker (see header comment).
 *
 * @param {string} source
 * @returns {string} source with comment text blanked out (same length,
 *   whitespace-preserving, so this can't accidentally merge adjacent tokens)
 */
function stripComments(source) {
  return source.replace(/\/\*[\s\S]*?\*\/|\/\/.*$/gm, (match) =>
    match.replace(/[^\n]/g, ' '),
  );
}

/**
 * Escapes a string for safe interpolation into a `RegExp` pattern.
 *
 * Export identifiers may contain `$` (see `extractNamedExports`'s
 * `[A-Za-z_$][\w$]*` character class), which is a regex metacharacter
 * (end-of-input anchor) outside a character class — an unescaped `\b$\b`
 * pattern would never match correctly.
 *
 * KNOWN LIMITATION (not currently active — no `$`-prefixed/suffixed export
 * exists in `lib/**\/*.mjs` today, per the real-world assertion below):
 * escaping `$` here does not fully solve identifiers that START or END with
 * `$` — `\b` requires a word/non-word transition, and `$` is not a `\w`
 * character, so a leading/trailing `\b` can still fail to match even after
 * escaping. In that case the checker would find zero references for the
 * export, including its own declaration, and silently fail to flag it even
 * if genuinely dead. Would need a lookaround-based boundary (not `\b`) to
 * close fully; out of scope for now.
 *
 * @param {string} value
 * @returns {string}
 */
function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Extract top-level named exports from a .mjs source string.
 *
 * Recognises the export shapes actually used in this directory's lib
 * files: `export function foo(`, `export async function foo(`,
 * `export const foo = `, `export class Foo`. Does not attempt to resolve
 * `export { a, b }` re-export lists or `export default` — neither pattern
 * is present in scripts/agent-resource-management/lib/**\/*.mjs today (see
 * the real-world assertion below), and this checker's job is to catch the
 * bug shape that actually occurred here, not to be a general-purpose export
 * parser.
 *
 * @param {string} source
 * @returns {string[]} exported identifier names
 */
function extractNamedExports(source) {
  const names = new Set();
  const patterns = [
    /^export\s+(?:async\s+)?function\s+([A-Za-z_$][\w$]*)/gm,
    /^export\s+const\s+([A-Za-z_$][\w$]*)/gm,
    /^export\s+class\s+([A-Za-z_$][\w$]*)/gm,
  ];
  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) {
      names.add(match[1]);
    }
  }
  return [...names];
}

/**
 * Recursively list all `.mjs` files under `rootDir`.
 *
 * @param {string} rootDir
 * @returns {Promise<string[]>} absolute file paths
 */
async function listMjsFilesRecursive(rootDir) {
  const entries = await readdir(rootDir, { withFileTypes: true });
  const results = [];
  for (const entry of entries) {
    const fullPath = join(rootDir, entry.name);
    if (entry.isDirectory()) {
      results.push(...(await listMjsFilesRecursive(fullPath)));
    } else if (entry.isFile() && entry.name.endsWith('.mjs')) {
      results.push(fullPath);
    }
  }
  return results;
}

/**
 * The fitness function itself.
 *
 * Flags any named export from a production file under `libDir` that has
 * ZERO real production usage anywhere — neither inside its own defining
 * file (beyond the `export` declaration line itself) nor in any other
 * production `.mjs` file under `searchRoot` — while at least one
 * `*.jest.spec.mjs` file DOES reference it.
 *
 * Why "used inside its own file" matters as a distinct signal from "used
 * by another production file": several legitimate exports in this
 * codebase (e.g. `AIMD_CEILING_RESERVED_ID`) are called/read many times
 * WITHIN the file that defines them, and exported chiefly so tests can
 * assert against the same literal — that is a normal, live export, not
 * dead code. `coreCount`/`parseResourceCapacity` (this ticket's motivating
 * bug, Phase 1) was different in kind: it had ZERO callers anywhere,
 * including inside its own defining file — the function existed, ran
 * nowhere, and only its own test block ever invoked it. This function
 * pins that specific shape: production-dead AND test-referenced.
 *
 * "Reference" here is a simple whole-word substring search for the export's
 * identifier over COMMENT-STRIPPED source (see `stripComments`) —
 * deliberately conservative (a real parser would be more precise about
 * scope/shadowing) but sufficient for this bug shape: a genuinely-used
 * production export will be referenced by its exact identifier somewhere in
 * *executable* production source (a call, an import specifier, an internal
 * use), and this directory's naming conventions do not reuse short generic
 * identifiers across unrelated exports in ways that would produce false
 * negatives in practice (confirmed by the real-world zero-findings
 * assertion below). Comments are stripped before counting specifically so a
 * doc-comment mention of an export's own name (e.g. its JSDoc block, which
 * every export in this directory carries) can never masquerade as a real
 * caller — see fixture case 4 below.
 *
 * @param {{ libDir: string, searchRoot: string }} options
 * @returns {Promise<Array<{ file: string, exportName: string }>>}
 */
async function findDeadLibExports({ libDir, searchRoot }) {
  const libFiles = (await listMjsFilesRecursive(libDir)).filter(
    (file) => !file.endsWith('.jest.spec.mjs'),
  );
  const allSearchFiles = await listMjsFilesRecursive(searchRoot);

  // Pre-read every candidate reference file once, with comment text
  // (including JSDoc) stripped — a mention only inside a doc comment must
  // not count as a real reference (see `stripComments`'s doc comment).
  const fileSources = new Map();
  for (const file of allSearchFiles) {
    fileSources.set(file, stripComments(await readFile(file, 'utf8')));
  }

  const findings = [];

  for (const libFile of libFiles) {
    // `libDir` is always a subdirectory of `searchRoot`, so `libFile` is
    // always already present in `fileSources` — no re-read needed. Export
    // extraction runs on this same comment-stripped source: a real `export`
    // declaration is never inside a comment, and stripping preserves line
    // structure (blanks non-newline chars only), so `extractNamedExports`'s
    // line-anchored patterns are unaffected.
    const source = fileSources.get(libFile);
    const exportNames = extractNamedExports(source);

    for (const exportName of exportNames) {
      const escapedName = escapeRegExp(exportName);
      const globalPattern = new RegExp(`\\b${escapedName}\\b`, 'g');

      // Internal usage: does the identifier appear more than once in its
      // OWN defining file (comments excluded)? The first occurrence is the
      // `export` line itself (declaration); any additional occurrence is a
      // real use (a call, a read, a re-assignment target, etc.).
      const ownFileOccurrences = source.match(globalPattern) ?? [];
      const usedInternally = ownFileOccurrences.length > 1;

      let referencedByOtherProduction = false;
      let referencedByTest = false;

      for (const [otherFile, otherSource] of fileSources) {
        if (otherFile === libFile) continue; // already accounted for above
        const wordBoundaryPattern = new RegExp(`\\b${escapedName}\\b`);
        if (!wordBoundaryPattern.test(otherSource)) continue;

        if (otherFile.endsWith('.jest.spec.mjs')) {
          referencedByTest = true;
        } else {
          referencedByOtherProduction = true;
        }
      }

      const hasAnyProductionUsage = usedInternally || referencedByOtherProduction;

      // Flag only exports with ZERO production usage anywhere (own file or
      // elsewhere) that are nonetheless referenced by at least one test
      // file — the exact bug shape Phase 1 deleted. An export
      // referenced by nothing at all, not even a test (dead to everyone),
      // is a different, arguably worse smell, but is not this ticket's bug
      // shape and is left out of v1 to keep the check's claim precise (see
      // header comment).
      if (referencedByTest && !hasAnyProductionUsage) {
        findings.push({ file: relative(searchRoot, libFile), exportName });
      }
    }
  }

  return findings;
}

/**
 * Build a synthetic fixture tree under a fresh tmpdir: a `lib/` subdirectory
 * plus whatever sibling spec/production files the scenario needs, mirroring
 * this directory's real `lib/` + sibling-spec layout closely enough for
 * `findDeadLibExports` to run against it unmodified.
 *
 * @param {Record<string, string>} files map of path (relative to the fixture
 *   root) -> file contents
 * @returns {Promise<string>} the fixture root directory
 */
async function buildFixtureTree(files) {
  const root = await mkdtemp(join(tmpdir(), 'arm-dead-export-fitness-'));
  for (const [relativePath, contents] of Object.entries(files)) {
    const fullPath = join(root, relativePath);
    await mkdir(dirname(fullPath), { recursive: true });
    await writeFile(fullPath, contents, 'utf8');
  }
  return root;
}

describe('fitness function: no lib/**/*.mjs export may be referenced only from a *.jest.spec.mjs file', () => {
  let fixtureRoot;

  afterEach(async () => {
    if (fixtureRoot) {
      await rm(fixtureRoot, { recursive: true, force: true });
      fixtureRoot = undefined;
    }
  });

  it('1. positive case: an export referenced ONLY by a SAME-named spec is flagged', async () => {
    fixtureRoot = await buildFixtureTree({
      'lib/widget.mjs': `export function parseWidget(raw) {\n  return raw;\n}\n`,
      'lib/widget.jest.spec.mjs': `import { parseWidget } from './widget.mjs';\n\ntest('parses', () => {\n  expect(parseWidget('x')).toBe('x');\n});\n`,
    });

    const findings = await findDeadLibExports({
      libDir: join(fixtureRoot, 'lib'),
      searchRoot: fixtureRoot,
    });

    expect(findings).toContainEqual({ file: 'lib/widget.mjs', exportName: 'parseWidget' });
  });

  it('2. positive case (the critical one): an export referenced ONLY by a DIFFERENTLY-named spec is ALSO flagged — proves no filename-matching', async () => {
    // Mirrors the real repo's eviction-authorization.jest.spec.mjs ->
    // ./recon.mjs shape: the spec's filename shares no stem with the
    // module it tests.
    fixtureRoot = await buildFixtureTree({
      'lib/gadget.mjs': `export function computeGadget(raw) {\n  return raw * 2;\n}\n`,
      'lib/oddly-named.jest.spec.mjs': `import { computeGadget } from './gadget.mjs';\n\ntest('computes', () => {\n  expect(computeGadget(2)).toBe(4);\n});\n`,
    });

    const findings = await findDeadLibExports({
      libDir: join(fixtureRoot, 'lib'),
      searchRoot: fixtureRoot,
    });

    // A filename-matching heuristic (looking for gadget.jest.spec.mjs)
    // would find nothing and MISS this — the check must flag it anyway.
    expect(findings).toContainEqual({ file: 'lib/gadget.mjs', exportName: 'computeGadget' });
  });

  it('3. negative case: an export referenced by a sibling PRODUCTION file is NOT flagged, even with a spec present too', async () => {
    fixtureRoot = await buildFixtureTree({
      'lib/base.mjs': `export function sharedHelper(raw) {\n  return raw;\n}\n`,
      'lib/consumer.mjs': `import { sharedHelper } from './base.mjs';\n\nexport function useIt(x) {\n  return sharedHelper(x);\n}\n`,
      'lib/base.jest.spec.mjs': `import { sharedHelper } from './base.mjs';\n\ntest('works', () => {\n  expect(sharedHelper(1)).toBe(1);\n});\n`,
    });

    const findings = await findDeadLibExports({
      libDir: join(fixtureRoot, 'lib'),
      searchRoot: fixtureRoot,
    });

    expect(findings).not.toContainEqual(
      expect.objectContaining({ exportName: 'sharedHelper' }),
    );
  });

  it('4. positive case: an export mentioned ONLY in its own JSDoc comment (no real caller) is still flagged — proves comment text does not count as usage', async () => {
    fixtureRoot = await buildFixtureTree({
      'lib/orphan.mjs': `/**\n * doOrphanThing exists for future use. doOrphanThing is not called yet.\n */\nexport function doOrphanThing(x) {\n  return x;\n}\n`,
      'lib/orphan.jest.spec.mjs': `import { doOrphanThing } from './orphan.mjs';\n\ntest('placeholder', () => {\n  expect(doOrphanThing(1)).toBe(1);\n});\n`,
    });

    const findings = await findDeadLibExports({
      libDir: join(fixtureRoot, 'lib'),
      searchRoot: fixtureRoot,
    });

    // Without comment-stripping, the doc comment's two extra textual
    // mentions of `doOrphanThing` would register as "used internally" and
    // this would be MISSED — the exact blind spot this case pins.
    expect(findings).toContainEqual({ file: 'lib/orphan.mjs', exportName: 'doOrphanThing' });
  });

  it('5. real-world check: running against the real scripts/agent-resource-management/lib/**/*.mjs (post Phase 1/2 cleanup) returns ONLY the known, tracked pre-existing exceptions — no NEW untracked findings', async () => {
    const findings = await findDeadLibExports({
      libDir: REAL_LIB_DIR,
      searchRoot: HERE,
    });

    const sortKey = (f) => `${f.file}::${f.exportName}`;
    const sortedFindings = [...findings].sort((a, b) => sortKey(a).localeCompare(sortKey(b)));
    const sortedAllowlist = [...KNOWN_PRE_EXISTING_EXCEPTIONS, ...IN_FLIGHT_PHASED_EXCEPTIONS].sort(
      (a, b) => sortKey(a).localeCompare(sortKey(b)),
    );

    // Exact-match, not subset: a NEW untracked finding fails this (the
    // checker's primary purpose), and so does an allowlisted entry that got
    // fixed without being removed here (keeps the allowlist honest — see
    // its own comment above). A self-explanatory failure message here
    // matters: this test can fail from an UNRELATED PR touching a different
    // part of lib/**/*.mjs, so a future contributor hitting it needs enough
    // context to act without reading this file's source first.
    try {
      expect(sortedFindings).toEqual(sortedAllowlist);
    } catch (error) {
      error.message =
        `dead-export-fitness: real lib/**/*.mjs findings diverged from the tracked baseline ` +
        `(KNOWN_PRE_EXISTING_EXCEPTIONS + IN_FLIGHT_PHASED_EXCEPTIONS above).\n\n` +
        `If this is a NEW untracked dead export: wire it into a real consumer or delete it — ` +
        `that is this checker's whole job (see).\n` +
        `If this is a KNOWN_PRE_EXISTING_EXCEPTIONS entry (tracked in) that just got fixed ` +
        `or deleted: remove its entry from the allowlist above.\n` +
        `If this is an IN_FLIGHT_PHASED_EXCEPTIONS entry whose later-phase production consumer has ` +
        `now landed: remove its entry from that list.\n\n${error.message}`;
      throw error;
    }
  });
});
