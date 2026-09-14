// Phase 3 — real fs adapter + CLI entrypoint for the cdk.out*/
// bundling-temp-* cleanup sweep. RED by construction: `./cleanup.mjs` does
// not exist yet (Phase 2 landed only the pure `selectStaleTempDirs` decision
// function in `./cleanup-age-filter.mjs`). Do not add a stub implementation
// to turn this green — that is the builder's job in this phase.
//
// SCOPE OF THIS FILE — integration-level, real filesystem, exercised via
// `spawnSync` against the real CLI entrypoint (matching every sibling
// `*-outer-acceptance.jest.spec.mjs` / `cli-*.jest.spec.mjs` convention in
// this directory, and matching `cleanup-outer-acceptance.jest.spec.mjs`'s own
// `runCleanup` helper one directory up). This file is the Phase 3 analogue
// of that Phase 0 outer-acceptance file's Test Strategy row: real fs via
// `fs.mkdtempSync` scratch dirs, NEVER the real host `$TMPDIR` (which
// currently holds 3,000+ real cdk.out dirs that must never be touched by any
// test run — see the acceptance file's note 1 for why every test below
// creates its own scratch root and tears it down in `afterEach`
// unconditionally).
//
// Mocked-fs unit tests for the adapter/decision-logic composition boundary
// (scenarios 3, 6, 7 of the Build Plan's Phase 3 Test Strategy row) live in
// the sibling file `./cleanup-adapter.jest.spec.mjs` instead of here —
// `jest.unstable_mockModule('node:fs', ...)` mocks apply file-wide once
// registered, which would break every real-fs scratch-dir helper in THIS
// file (`mkdtempSync`, `utimesSync`, `rmSync` teardown, etc.). This mirrors
// the established split in this same `lib/` directory between
// `diagnostic-reports-scanner.jest.spec.mjs` (mocked `node:fs/promises`) and
// `diagnostic-reports-scanner-symlink-realfs.jest.spec.mjs` (real fs) for
// exactly the same reason.
//
// SHAPE THIS TEST ASSUMES (pinned by this file, not yet implemented):
//
//   export const CLEANUP_SCRIPT_PATH: string
//     — this module's own absolute path (`fileURLToPath(import.meta.url)`),
//     already consumed by `cleanup-outer-acceptance.jest.spec.mjs`.
//
//   CLI: `node cleanup.mjs [--age-hours=N] [--dir=<path>]`
//     — no shebang (matches `diagnostics/collect.mjs`'s direct-invocation,
//     no-execute-bit convention); default `--age-hours` is 2; default
//     `--dir` is `process.env.TMPDIR` or `/tmp`. Runs only when
//     `import.meta.url` matches the invoked file (collect.mjs's own guard
//     idiom), so importing this module from a test never triggers argv
//     parsing.
//   stdout: a single JSON object `{ removedCount: number, reclaimedBytes:
//     number }` (matching the field names `cleanup-outer-acceptance.jest.
//     spec.mjs` already pins). Exit code 0 on a successful sweep, even when
//     nothing qualifies (scenario 5) or when some entries fail removal
//     (scenario 3, exercised in the adapter-unit sibling file since it needs
//     a simulated permission error).
//
// `--dir` VALIDATION — judgement call (see report): this CLI is invoked
// directly by a human/cron, not fed attacker-controlled input in the common
// case, and `spawnSync('node', [...])` here never goes through a shell, so
// classic shell-metacharacter injection (the threat `ensure-worktree.mjs`'s
// `SAFE_PATH_RE` guards against for its own shell-interpolated inputs) does
// not apply to this argv-array invocation. Full path-injection-style
// validation is therefore judged OVER-SCOPED for this script's threat model.
// What IS worth guarding against operationally is a mistyped/nonexistent
// `--dir` silently no-op'ing (or worse, being created and then swept) —
// so this file pins the LIGHTER guard: a `--dir` that does not exist as a
// real, existing directory is rejected with a non-zero exit and stderr
// message, before any fs mutation is attempted.

import { mkdtempSync, rmSync, mkdirSync, writeFileSync, utimesSync, existsSync, symlinkSync, lutimesSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

// RED: this module does not exist yet. Import failure here is the expected
// red state for this whole file, not a bug in this spec.
import { CLEANUP_SCRIPT_PATH } from './cleanup.mjs';

const FALLBACK_CLEANUP_SCRIPT = join(dirname(fileURLToPath(import.meta.url)), 'cleanup.mjs');

const DEFAULT_AGE_HOURS = 2;
const HOUR_MS = 60 * 60 * 1000;

/** Runs the real cleanup.mjs entrypoint as a real child process. */
function runCleanup(args) {
  const result = spawnSync('node', [CLEANUP_SCRIPT_PATH ?? FALLBACK_CLEANUP_SCRIPT, ...args], {
    encoding: 'utf8',
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/**
 * Runs the real cleanup.mjs entrypoint with `TMPDIR` pinned to `scratchDir`
 * (never the real host `$TMPDIR`) so that any silent fallback-to-default
 * `--dir` behaviour under test — see the "reject unrecognized/bare CLI
 * flags" scenarios below — resolves against the scratch tree instead of the
 * real host temp root, which currently holds 3,000+ real `cdk.out` dirs that
 * must never be touched by any test run.
 */
function runCleanupConfinedToScratch(args, scratchDir) {
  const result = spawnSync('node', [CLEANUP_SCRIPT_PATH ?? FALLBACK_CLEANUP_SCRIPT, ...args], {
    encoding: 'utf8',
    env: { ...process.env, TMPDIR: scratchDir },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

function parseReport(stdout) {
  return JSON.parse(stdout);
}

function toEpochSeconds(ageHours) {
  return (Date.now() - ageHours * HOUR_MS) / 1000;
}

/** Creates a directory containing `fileSizeBytes` of real file content, backdated to `ageHours` ago. */
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

describe('cleanup.mjs — real fs adapter + CLI entrypoint', () => {
  let scratchDir;

  beforeEach(() => {
    // The ONLY directory any test in this file creates or deletes. Never
    // the real host $TMPDIR.
    scratchDir = mkdtempSync(join(tmpdir(), 'arm-cleanup-phase3-'));
  });

  afterEach(() => {
    rmSync(scratchDir, { recursive: true, force: true });
  });

  it('1. removes stale cdk.out*/bundling-temp-* dirs from a real scratch tree, leaves fresh/unrelated dirs untouched', () => {
    const staleCdkOut = join(scratchDir, 'cdk.out-stale');
    const staleBundling = join(scratchDir, 'bundling-temp-stale');
    const freshCdkOut = join(scratchDir, 'cdk.out-fresh');
    const unrelatedOld = join(scratchDir, 'unrelated-old-dir');

    seedDir(staleCdkOut, { ageHours: DEFAULT_AGE_HOURS + 3 });
    seedDir(staleBundling, { ageHours: DEFAULT_AGE_HOURS + 3 });
    seedDir(freshCdkOut, { ageHours: 0.05 });
    seedDir(unrelatedOld, { ageHours: DEFAULT_AGE_HOURS + 3 });

    const result = runCleanup([`--dir=${scratchDir}`, `--age-hours=${DEFAULT_AGE_HOURS}`]);

    expect(result.status).toBe(0);
    expect(existsSync(staleCdkOut)).toBe(false);
    expect(existsSync(staleBundling)).toBe(false);
    expect(existsSync(freshCdkOut)).toBe(true);
    expect(existsSync(unrelatedOld)).toBe(true);
  });

  it('2. reports an accurate reclaimed-byte total and removed-dir count for known file sizes', () => {
    const staleOne = join(scratchDir, 'cdk.out-one');
    const staleTwo = join(scratchDir, 'bundling-temp-two');
    const staleOneBytes = seedDir(staleOne, { ageHours: DEFAULT_AGE_HOURS + 5, fileSizeBytes: 4096 });
    const staleTwoBytes = seedDir(staleTwo, { ageHours: DEFAULT_AGE_HOURS + 5, fileSizeBytes: 8192 });
    // Fresh, must not contribute to the reclaimed total.
    seedDir(join(scratchDir, 'cdk.out-fresh'), { ageHours: 0.05, fileSizeBytes: 999 });

    const result = runCleanup([`--dir=${scratchDir}`, `--age-hours=${DEFAULT_AGE_HOURS}`]);

    expect(result.status).toBe(0);
    const report = parseReport(result.stdout);

    expect(report.removedCount).toBe(2);
    expect(report.reclaimedBytes).toBe(staleOneBytes + staleTwoBytes);
  });

  it('4. rejects a nonexistent --dir override with a non-zero exit and touches nothing', () => {
    const nonexistentDir = join(scratchDir, 'does-not-exist-at-all');

    const result = runCleanup([`--dir=${nonexistentDir}`, `--age-hours=${DEFAULT_AGE_HOURS}`]);

    expect(result.status).not.toBe(0);
    expect(result.stderr.length).toBeGreaterThan(0);
    // Guarding against the CLI ever creating the missing directory itself
    // as a side effect of a naive fs call before validating it.
    expect(existsSync(nonexistentDir)).toBe(false);
  });

  it('9. refuses to run against a --dir outside the system temp root, and touches nothing (review finding 2)', () => {
    // The lib/ directory itself: a real, existing directory that is
    // definitely NOT under the system temp root. Used only to prove the
    // confinement check rejects it — nothing inside it should ever be
    // read, listed, or mutated by this assertion.
    const outsideTmpDir = dirname(fileURLToPath(import.meta.url));
    const sentinelFile = join(outsideTmpDir, 'cleanup.mjs');

    const before = existsSync(sentinelFile);
    const result = runCleanup([`--dir=${outsideTmpDir}`, `--age-hours=${DEFAULT_AGE_HOURS}`]);

    expect(result.status).not.toBe(0);
    expect(result.stderr.length).toBeGreaterThan(0);
    expect(result.stderr).toMatch(/temp root/);
    // Zero fs mutation: the CLI's own source file must still exist exactly
    // as before the run.
    expect(existsSync(sentinelFile)).toBe(before);
    expect(existsSync(sentinelFile)).toBe(true);
  });

  it('10. rejects a non-numeric --age-hours with a clean non-zero exit and stderr message, not a raw stack trace (review finding 3)', () => {
    const result = runCleanup([`--dir=${scratchDir}`, '--age-hours=abc']);

    expect(result.status).not.toBe(0);
    expect(result.stderr.length).toBeGreaterThan(0);
    expect(result.stderr).toMatch(/age-hours/);
    // A raw uncaught-exception stack trace starts with "at " frames and the
    // error constructor name; a clean CLI error message does not.
    expect(result.stderr).not.toMatch(/at .*cleanup-age-filter\.mjs/);
  });

  it('5. reports zero cleanly (0 removed, 0 reclaimed) when nothing in the scratch dir qualifies', () => {
    seedDir(join(scratchDir, 'cdk.out-fresh-only'), { ageHours: 0.05, fileSizeBytes: 256 });
    seedDir(join(scratchDir, 'not-a-cdk-dir'), { ageHours: DEFAULT_AGE_HOURS + 10, fileSizeBytes: 256 });

    const result = runCleanup([`--dir=${scratchDir}`, `--age-hours=${DEFAULT_AGE_HOURS}`]);

    expect(result.status).toBe(0);
    const report = parseReport(result.stdout);

    expect(report.removedCount).toBe(0);
    expect(report.reclaimedBytes).toBe(0);
  });
});

// — ports three edge-case improvements from the closed-duplicate
// into this already-shipped implementation: symlinks are now
// candidates (via `lstatSync` for the link's own mtime, never following into
// the target), unrecognized/bare CLI flags now error instead of silently
// falling back to defaults, and a stale plain file (not just a directory)
// matching the naming pattern is now a candidate too.
describe('cleanup.mjs — edge-case hardening (symlinks, CLI flag rejection, stale files)', () => {
  let scratchDir;

  beforeEach(() => {
    // The ONLY directory any test in this describe block creates or
    // deletes. Never the real host $TMPDIR.
    scratchDir = mkdtempSync(join(tmpdir(), 'arm-cleanup-1700-'));
  });

  afterEach(() => {
    rmSync(scratchDir, { recursive: true, force: true });
  });

  describe('1. symlink-safe stat handling', () => {
    it('removes a stale symlink (own mtime stale) without ever touching its fresh-content target directory', () => {
      // A real directory elsewhere in the scratch tree, with FRESH content —
      // never backdated. If the implementation ever follows the symlink
      // (`statSync` instead of `lstatSync`, or recurses into the target on
      // removal) this directory/content must still be destroyed; asserting
      // it survives untouched is the whole point of this test.
      const targetDir = join(scratchDir, 'real-target-dir');
      mkdirSync(targetDir);
      const targetFile = join(targetDir, 'fresh-payload.bin');
      writeFileSync(targetFile, 'still here');

      const symlinkPath = join(scratchDir, 'cdk.out-symlink-test');
      symlinkSync(targetDir, symlinkPath, 'dir');

      // Backdate the SYMLINK's own mtime (not the target's) via
      // `lutimesSync` — Node 14.5+, confirmed available in this repo's
      // pinned Node 22 runtime. `utimesSync` would follow the link and
      // stamp the target instead, which is exactly the bug this test
      // guards against.
      const staleSeconds = toEpochSeconds(DEFAULT_AGE_HOURS + 3);
      lutimesSync(symlinkPath, staleSeconds, staleSeconds);

      const result = runCleanup([`--dir=${scratchDir}`, `--age-hours=${DEFAULT_AGE_HOURS}`]);

      expect(result.status).toBe(0);
      // The symlink itself is gone...
      expect(existsSync(symlinkPath)).toBe(false);
      // ...but the real directory it pointed to, and its fresh content,
      // are completely untouched.
      expect(existsSync(targetDir)).toBe(true);
      expect(existsSync(targetFile)).toBe(true);
      expect(readFileSync(targetFile, 'utf8')).toBe('still here');
    });

    it('leaves a fresh symlink alone even when the directory it points to is old', () => {
      const targetDir = join(scratchDir, 'real-target-dir-old');
      mkdirSync(targetDir);
      const staleSeconds = toEpochSeconds(DEFAULT_AGE_HOURS + 3);
      utimesSync(targetDir, staleSeconds, staleSeconds);

      const symlinkPath = join(scratchDir, 'cdk.out-symlink-fresh-test');
      symlinkSync(targetDir, symlinkPath, 'dir');
      // Deliberately left at its just-created (fresh) mtime — no
      // `lutimesSync` backdating here, unlike the previous test.

      const result = runCleanup([`--dir=${scratchDir}`, `--age-hours=${DEFAULT_AGE_HOURS}`]);

      expect(result.status).toBe(0);
      expect(existsSync(symlinkPath)).toBe(true);
      expect(existsSync(targetDir)).toBe(true);
    });

    it('removes a stale DANGLING symlink (target already gone) cleanly, with no thrown ENOENT', () => {
      // The scenario that actually motivates this fix: a `cdk.out*` symlink
      // left behind after its target build directory was already cleaned up
      // independently, elsewhere. `lstatSync` doesn't require the target to
      // exist, and a dangling-symlink candidate is sized as 0 without ever
      // `statSync`-ing (and thus never throwing on) the missing target.
      const targetDir = join(scratchDir, 'now-removed-target-dir');
      mkdirSync(targetDir);

      const symlinkPath = join(scratchDir, 'cdk.out-dangling-symlink-test');
      symlinkSync(targetDir, symlinkPath, 'dir');

      const staleSeconds = toEpochSeconds(DEFAULT_AGE_HOURS + 3);
      lutimesSync(symlinkPath, staleSeconds, staleSeconds);

      // Remove the target AFTER creating the symlink, so the link is left
      // dangling — this is the real-world sequence (target cleaned up by
      // some other process; the symlink is orphaned).
      rmSync(targetDir, { recursive: true, force: true });

      const result = runCleanup([`--dir=${scratchDir}`, `--age-hours=${DEFAULT_AGE_HOURS}`]);

      expect(result.status).toBe(0);
      expect(existsSync(symlinkPath)).toBe(false);
      const report = JSON.parse(result.stdout);
      expect(report.errors).toEqual([]);
    });
  });

  describe('2. reject unrecognized/bare CLI flags', () => {
    it('rejects a bare --dir with no value, exiting non-zero with a clear stderr message rather than silently defaulting', () => {
      const result = runCleanupConfinedToScratch([`--age-hours=${DEFAULT_AGE_HOURS}`, '--dir'], scratchDir);

      expect(result.status).not.toBe(0);
      expect(result.stderr.length).toBeGreaterThan(0);
      expect(result.stderr).toMatch(/--dir/);
    });

    it('rejects a bare --age-hours with no value, exiting non-zero with a clear stderr message rather than silently defaulting', () => {
      const result = runCleanupConfinedToScratch([`--dir=${scratchDir}`, '--age-hours'], scratchDir);

      expect(result.status).not.toBe(0);
      expect(result.stderr.length).toBeGreaterThan(0);
      expect(result.stderr).toMatch(/--age-hours/);
    });

    it('rejects a typo\'d flag (--age-hour=5) instead of silently ignoring it and falling back to the default', () => {
      const result = runCleanupConfinedToScratch([`--dir=${scratchDir}`, '--age-hour=5'], scratchDir);

      expect(result.status).not.toBe(0);
      expect(result.stderr.length).toBeGreaterThan(0);
      expect(result.stderr).toMatch(/--age-hour=5|unrecognized|unknown/i);
    });
  });

  describe('3. stale-file candidate support', () => {
    it('removes a stale plain FILE matching the naming pattern, same as a stale directory, and counts it in the report', () => {
      const staleFilePath = join(scratchDir, 'cdk.out-leftover-file');
      const fileContents = Buffer.alloc(2048, 7);
      writeFileSync(staleFilePath, fileContents);
      const staleSeconds = toEpochSeconds(DEFAULT_AGE_HOURS + 3);
      utimesSync(staleFilePath, staleSeconds, staleSeconds);

      // A fresh file with the same naming pattern must NOT be removed —
      // proving the new file-candidate path still respects the age filter.
      const freshFilePath = join(scratchDir, 'cdk.out-fresh-file');
      writeFileSync(freshFilePath, 'fresh');

      const result = runCleanup([`--dir=${scratchDir}`, `--age-hours=${DEFAULT_AGE_HOURS}`]);

      expect(result.status).toBe(0);
      expect(existsSync(staleFilePath)).toBe(false);
      expect(existsSync(freshFilePath)).toBe(true);

      const report = parseReport(result.stdout);
      expect(report.removedCount).toBe(1);
      expect(report.reclaimedBytes).toBe(fileContents.length);
    });
  });
});
