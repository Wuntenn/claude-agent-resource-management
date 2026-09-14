// Phase 0 outer acceptance test ("Confirmed: $TMPDIR/cdk.out*
// is the disk-exhaustion source (13,463 dirs / 5.5GB) — build the cleanup").
//
// Outer acceptance test (per the ## Build Plan comment posted on):
//
//   it('removes only cdk.out*/bundling-temp-* dirs older than the age
//   threshold from a real temp tree, and reports bytes reclaimed', ...)
//   Layer: integration — creates a real scratch directory (via
//   `fs.mkdtempSync`, never the shared/system `$TMPDIR`) seeded with a mix
//   of `cdk.out<rand>`/`bundling-temp-<rand>`/unrelated dirs at varying real
//   mtimes, runs the real cleanup.mjs entrypoint against that directory (no
//   mocked fs — the fs boundary itself is exactly what's under test here),
//   and asserts: only the old, correctly-named dirs are gone; the untouched/
//   unrelated/fresh dirs remain byte-for-byte; the reported reclaimed total
//   matches the actual bytes removed.
//   Green when: Phase 2 (pure age-filter decision logic) + Phase 3 (fs
//   adapter + CLI entrypoint + reporting) both pass their gates.
//
// RED by construction, right now: `scripts/agent-resource-management/lib/
// cleanup.mjs` does not exist yet (confirmed: no such file in this worktree
// prior to this ticket's Phase 2/3 work). The top-level import below fails
// module resolution — that ModuleNotFoundError is this file's expected red
// state, not a bug in this test.
//
// Do not add a stub `cleanup.mjs` to make this pass — that defeats the
// point of the outer/phase red-green loop. Phase 2's implementer lands the
// pure age-filter function, Phase 3's implementer wires the real fs adapter
// + CLI entrypoint + reporting, and only then does this file turn green.
//
// -----------------------------------------------------------------------
// Scope notes / seam-shape decisions (read before extending this file):
//
// 1. SAFETY — this file NEVER touches the real host `$TMPDIR`. Every
//    directory this test creates or deletes lives under a fresh
//    `fs.mkdtempSync(join(tmpdir(), 'arm-cleanup-outer-'))` scratch root,
//    passed to the real cleanup entrypoint via its own `--dir=` override
//    flag (per the Build Plan's documented CLI shape:
//    `node scripts/agent-resource-management/lib/cleanup.mjs
//    [--age-hours=N] [--dir=$TMPDIR]`). This is the one property the Build
//    Plan calls out as absolute ("the existing 3,000+ real cdk.out* dirs on
//    this host must never be touched by a test run") — this file's
//    `beforeEach`/`afterEach` scaffolding exists specifically to guarantee
//    it even if an assertion fails mid-test.
//
// 2. INVOCATION SHAPE — this file shells out to the real entrypoint via
//    `spawnSync('node', [CLEANUP_SCRIPT, ...args])`, matching the
//    established convention for every `cli.*.jest.spec.mjs` sibling in this
//    directory (see evict-outer-acceptance.jest.spec.mjs's own `runCli`
//    helper) and matching the Build Plan's own stated invocation
//    ("directly-node-invokable script ... matching diagnostics/collect.mjs's
//    existing invocation convention" — that file also runs its CLI body
//    only when invoked as the entrypoint, via the `import.meta.url` guard
//    idiom, so importing it as a module never triggers argv parsing).
//    Reporting shape (reclaimed bytes / removed count on stdout) is not
//    pinned by the Build Plan beyond "reports ... to stdout" — this file
//    assumes a single JSON object on stdout (the established convention
//    every other CLI in this directory already uses — see
//    evict-outer-acceptance.jest.spec.mjs's `JSON.parse(result.stdout)`),
//    with `reclaimedBytes`/`removedCount` fields. If Phase 3's implementer
//    picks different field names or a non-JSON stdout shape, update the
//    `parseReport` helper below to match — the assertion that matters is
//    the OBSERVABLE OUTCOME (accurate reclaimed-bytes/removed-count
//    reporting), not these exact field names.
//
// 3. AGE-THRESHOLD BOUNDARY — the Build Plan's Phase 2 edge cases call for
//    an explicit inclusive/exclusive decision at exactly `mtime == now -
//    thresholdMs`. This outer test does not pin that exact boundary (that
//    is Phase 2's own unit-level test's job, per the Build Plan's test
//    strategy table) — it uses mtimes comfortably on either side of the
//    threshold (well past 2h, and well within 2h) so this file's own
//    assertions are not coupled to Phase 2's inclusive/exclusive choice.
// -----------------------------------------------------------------------

import { mkdtempSync, rmSync, mkdirSync, writeFileSync, utimesSync, existsSync, readdirSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

// RED: this module does not exist yet (Phases 2+3). Import failure here is
// the expected, documented red state for this whole file.
import { CLEANUP_SCRIPT_PATH } from './lib/cleanup.mjs';

const FALLBACK_CLEANUP_SCRIPT = join(
  dirname(fileURLToPath(import.meta.url)),
  'lib',
  'cleanup.mjs',
);

const DEFAULT_AGE_HOURS = 2;
const HOUR_MS = 60 * 60 * 1000;

/** Runs the real cleanup.mjs entrypoint as a real child process — never imports and calls it directly, matching every sibling cli.*.jest.spec.mjs convention in this directory. */
function runCleanup(args) {
  const result = spawnSync(
    'node',
    [CLEANUP_SCRIPT_PATH ?? FALLBACK_CLEANUP_SCRIPT, ...args],
    { encoding: 'utf8' },
  );
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/** Parses the single JSON report object this entrypoint is expected to print to stdout (see note 2 above). */
function parseReport(stdout) {
  return JSON.parse(stdout);
}

/** Creates a directory at `dirPath` containing `fileSizeBytes` of real file content, then backdates its mtime (and its child file's mtime) to `ageHours` hours ago. mtime, not process ancestry, is the attribution rule this ticket confirms applies here (its creating process has already exited by the time cleanup runs). */
function seedDir(dirPath, { ageHours, fileSizeBytes = 1024 }) {
  mkdirSync(dirPath, { recursive: true });
  const filePath = join(dirPath, 'payload.bin');
  writeFileSync(filePath, Buffer.alloc(fileSizeBytes, 1));

  const ageMs = ageHours * HOUR_MS;
  const backdatedSeconds = (Date.now() - ageMs) / 1000;
  utimesSync(filePath, backdatedSeconds, backdatedSeconds);
  utimesSync(dirPath, backdatedSeconds, backdatedSeconds);

  return fileSizeBytes;
}

/** Real, recursive byte total for a directory tree — used to compute the expected reclaimed total independently of whatever internal size-summing Phase 3 implements, so this test doesn't just echo the production code's own arithmetic back at itself. */
function realDirSizeBytes(dirPath) {
  let total = 0;
  for (const entry of readdirSync(dirPath, { withFileTypes: true })) {
    const entryPath = join(dirPath, entry.name);
    if (entry.isDirectory()) {
      total += realDirSizeBytes(entryPath);
    } else {
      total += statSync(entryPath).size;
    }
  }
  return total;
}

describe('cleanup.mjs — real fs sweep of stale cdk.out*/bundling-temp-* dirs (outer acceptance)', () => {
  let scratchDir;

  beforeEach(() => {
    // note 1 — the ONLY directory this test ever creates or deletes.
    // Never the real host $TMPDIR.
    scratchDir = mkdtempSync(join(tmpdir(), 'arm-cleanup-outer-'));
  });

  afterEach(() => {
    rmSync(scratchDir, { recursive: true, force: true });
  });

  it(
    'removes only cdk.out*/bundling-temp-* dirs older than the age threshold, leaves fresh and ' +
      'unrelated dirs byte-for-byte intact, and reports an accurate reclaimed-bytes/removed-count total',
    () => {
      const STALE_HOURS = DEFAULT_AGE_HOURS + 3; // well past the 2h default threshold
      const FRESH_HOURS = 0.1; // well within the 2h default threshold

      // Stale, correctly-named — MUST be removed.
      const staleCdkOut = join(scratchDir, 'cdk.out-aaaa1111');
      const staleBundling = join(scratchDir, 'bundling-temp-bbbb2222');
      const staleCdkOutBytes = seedDir(staleCdkOut, { ageHours: STALE_HOURS, fileSizeBytes: 4096 });
      const staleBundlingBytes = seedDir(staleBundling, { ageHours: STALE_HOURS, fileSizeBytes: 2048 });

      // Fresh, correctly-named — MUST survive despite the matching name.
      const freshCdkOut = join(scratchDir, 'cdk.out-cccc3333');
      seedDir(freshCdkOut, { ageHours: FRESH_HOURS, fileSizeBytes: 512 });

      // Old, but NOT a cdk.out*/bundling-temp-* name — MUST NEVER be
      // removed regardless of age (Build Plan Phase 2 edge case: "A dir
      // whose name doesn't match ... at all — never selected, regardless
      // of age").
      const unrelatedOld = join(scratchDir, 'some-other-thing');
      seedDir(unrelatedOld, { ageHours: STALE_HOURS, fileSizeBytes: 1024 });

      const expectedReclaimedBytes = realDirSizeBytes(staleCdkOut) + realDirSizeBytes(staleBundling);
      void staleCdkOutBytes;
      void staleBundlingBytes;

      const result = runCleanup([`--dir=${scratchDir}`, `--age-hours=${DEFAULT_AGE_HOURS}`]);

      expect(result.status).toBe(0);
      const report = parseReport(result.stdout);

      // Observable outcome 1: only the stale, correctly-named entries are
      // gone.
      expect(existsSync(staleCdkOut)).toBe(false);
      expect(existsSync(staleBundling)).toBe(false);

      // Observable outcome 2: fresh and unrelated entries survive
      // byte-for-byte.
      expect(existsSync(freshCdkOut)).toBe(true);
      expect(realDirSizeBytes(freshCdkOut)).toBe(512);
      expect(existsSync(unrelatedOld)).toBe(true);
      expect(realDirSizeBytes(unrelatedOld)).toBe(1024);

      // Observable outcome 3: accurate reclaimed-bytes/removed-count
      // reporting (see note 2 for the assumed field names).
      expect(report.removedCount).toBe(2);
      expect(report.reclaimedBytes).toBe(expectedReclaimedBytes);
    },
  );

  it('reports zero cleanly, and removes nothing, when every entry is fresh or non-matching', () => {
    const freshCdkOut = join(scratchDir, 'cdk.out-freshonly');
    seedDir(freshCdkOut, { ageHours: 0.1, fileSizeBytes: 256 });
    const unrelated = join(scratchDir, 'not-a-cdk-dir');
    seedDir(unrelated, { ageHours: DEFAULT_AGE_HOURS + 10, fileSizeBytes: 256 });

    const result = runCleanup([`--dir=${scratchDir}`, `--age-hours=${DEFAULT_AGE_HOURS}`]);

    expect(result.status).toBe(0);
    const report = parseReport(result.stdout);

    expect(report.removedCount).toBe(0);
    expect(report.reclaimedBytes).toBe(0);
    expect(existsSync(freshCdkOut)).toBe(true);
    expect(existsSync(unrelated)).toBe(true);
  });
});
