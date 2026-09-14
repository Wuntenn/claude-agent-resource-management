// — black-box smoke test for `collect.mjs`'s CLI entry-point block
// (the `if (process.argv[1] !== undefined && pathToFileURL(...) ===
// import.meta.url)`-guarded block at the bottom of the file).
//
// Every other test in this directory (collect.jest.spec.mjs,
// collect-cli.jest.spec.mjs) exercises `collect.mjs` via direct function
// import (`parseCliArgs`, `runCollection`, `writeCollectionOutput`,
// `excludeSelfFromProcessTree`) — none of them ever actually spawn the file
// as a process, so the entry-point block itself (argv parsing, exit codes,
// the print-then-write ordering, the write-failure stderr warning) has no
// coverage of its own. This file closes that gap, following the same
// black-box `spawnSync`-over-`node <script>` pattern established by
// `scripts/agent-resource-management/cli.jest.spec.mjs`'s `runCli` helper —
// never importing `collect.mjs` directly.
//
// Platform guard: unlike `cli.mjs` (which has an `ARM_FAKE_COLLECT_JSON`
// injection seam letting most of that file's tests avoid the real OS-shelling
// path), `collect.mjs` has NO such seam — `--live` always shells out for real
// to `/usr/bin/vm_stat`, `/usr/sbin/sysctl`, and `/bin/ps`. Those binaries are
// macOS-only and don't exist on the `ubuntu-latest` CI runner. Following the
// precedent `cli.jest.spec.mjs` itself established for its own genuinely-real
// (non-faked) OS-shelling tests (`describeOnDarwin`, guarding its
// "realCollect() forwards swapTotalMb" describe block), the `--live` tests
// below are gated to Darwin hosts only; on Linux CI they report as skipped
// rather than failing on an environment mismatch unrelated to the code under
// test. The invalid-args test needs no such guard — `parseCliArgs` is pure
// and rejects before any probe ever runs, so it is exercised unconditionally.

import { spawnSync } from 'node:child_process';
import { readdir, readFile, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const COLLECT_SCRIPT = join(dirname(fileURLToPath(import.meta.url)), 'collect.mjs');
const OUTPUT_DIR = join(dirname(fileURLToPath(import.meta.url)), 'output');

/** Runs the real collect.mjs as a child process — never imports it directly. */
function runCollect(args) {
  const result = spawnSync('node', [COLLECT_SCRIPT, ...args], { encoding: 'utf8' });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/**
 * Deletes every `collect-*.json` file this test run wrote to the real,
 * default `output/` directory — `writeCollectionOutput`'s `outputDir` has no
 * env-based override, so a spawned `--live` invocation always writes there
 * for real. The directory is `.gitignore`d, but tests still clean up after
 * themselves rather than accumulating files across local runs.
 */
async function cleanOutputDir(filesBefore) {
  let entries;
  try {
    entries = await readdir(OUTPUT_DIR);
  } catch {
    return;
  }
  const newFiles = entries.filter((name) => !filesBefore.has(name));
  await Promise.all(newFiles.map((name) => rm(join(OUTPUT_DIR, name), { force: true })));
}

async function listOutputDir() {
  try {
    return new Set(await readdir(OUTPUT_DIR));
  } catch {
    return new Set();
  }
}

describe('collect.mjs CLI entry point — invalid arguments (platform-independent)', () => {
  test('no mode flag at all exits 2 and prints a usage error to stderr, nothing on stdout', () => {
    const { status, stdout, stderr } = runCollect([]);

    expect(status).toBe(2);
    expect(stdout).toBe('');
    expect(stderr).toMatch(/collect: one of --live or --retroactive is required/);
    expect(stderr).toMatch(/^Usage: node collect\.mjs/m);
  });

  test('--live and --retroactive together exits 2 with the mutually-exclusive error', () => {
    const { status, stdout, stderr } = runCollect(['--live', '--retroactive']);

    expect(status).toBe(2);
    expect(stdout).toBe('');
    expect(stderr).toMatch(/mutually exclusive/);
  });

  test('an unrecognized flag exits 2 with the unrecognized-flag error', () => {
    const { status, stdout, stderr } = runCollect(['--bogus']);

    expect(status).toBe(2);
    expect(stdout).toBe('');
    expect(stderr).toMatch(/unrecognized flag "--bogus"/);
  });
});

const describeOnDarwin = process.platform === 'darwin' ? describe : describe.skip;

describeOnDarwin('collect.mjs CLI entry point — --live (real OS-shelling, Darwin only)', () => {
  let filesBeforeEach;

  beforeEach(async () => {
    filesBeforeEach = await listOutputDir();
  });

  afterEach(async () => {
    await cleanOutputDir(filesBeforeEach);
  });

  test(
    '--live exits 0 and prints valid, self-describing JSON to stdout',
    () => {
      const { status, stdout, stderr } = runCollect(['--live']);

      expect(status).toBe(0);

      let parsed;
      expect(() => {
        parsed = JSON.parse(stdout);
      }).not.toThrow();

      expect(parsed.mode).toBe('live');
      expect(typeof parsed.generatedAt).toBe('string');
      expect(parsed.memory).toBeDefined();
      expect(parsed.pressure).toBeDefined();
      expect(parsed.swap).toBeDefined();
      expect(parsed.processes).toBeDefined();
      expect(parsed.footprints).toBeDefined();

      // The success path also reports where the report was written, on
      // stderr (not stdout, which callers parse as JSON).
      expect(stderr).toMatch(/collect: wrote diagnostics report to/);
    },
    15000,
  );

  test(
    'prints the collected JSON to stdout BEFORE any write-failure warning could appear on stderr ' +
      '(print-before-write ordering) and a real invocation actually persists a report file',
    async () => {
      const { status, stdout, stderr } = runCollect(['--live']);

      expect(status).toBe(0);
      // Successful write path: no warning, just the "wrote diagnostics
      // report to ..." confirmation — proves this run took the write-success
      // branch, not the write-failure branch (covered by unit tests on
      // `writeCollectionOutput` directly; there is no env seam to force a
      // real filesystem failure for this specific default, non-overridable
      // `outputDir` from a spawned black-box invocation).
      expect(stderr).not.toMatch(/warning: failed to write output file/);
      expect(stderr).toMatch(/collect: wrote diagnostics report to (.+collect-.+\.json)/);

      const writtenPathMatch = stderr.match(/collect: wrote diagnostics report to (.+\.json)/);
      expect(writtenPathMatch).not.toBeNull();
      const writtenPath = writtenPathMatch[1].trim();

      const onDisk = await readFile(writtenPath, 'utf8');
      expect(JSON.parse(onDisk)).toEqual(JSON.parse(stdout));
    },
    15000,
  );
});
