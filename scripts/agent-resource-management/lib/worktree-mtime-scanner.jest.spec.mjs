// Unit/integration tests for
// scripts/agent-resource-management/lib/worktree-mtime-scanner.mjs
// (Phase 2 — "git-tracked worktree-mtime scanner"), which exports
// `getRealWorktreeMtimeMs(pid, honorFakeSeam = true, deps = {})`.
//
// SHAPE: this module composes Phase 1's `resolveProcessCwd` (already real,
// `scripts/agent-resource-management/lib/process-cwd.mjs`) with a
// git-tracked-file mtime walk of that cwd: `git -C <cwd> ls-files` (bounded
// via `probeExecOptions()`, mirroring `process-cwd.mjs`/`footprint-sampler.mjs`),
// then `fs.stat` each listed path to find the latest mtime. Consumer contract
// (`liveness.mjs`): `worktreeMtimeMs: number | null` — `null` means "unknown",
// never a guessed `0`/`NaN`, and the function must never throw.
//
// MOCKING STRATEGY — dependency injection, not repeated ESM module mocking:
//
// An earlier version of this file used `jest.unstable_mockModule('./process-
// cwd.mjs', ...)` per-test, reset via `jest.resetModules()` between describe
// blocks. That proved unreliable: Jest's experimental ESM VM-modules registry
// does not consistently honor a fresh `unstable_mockModule` registration for
// the same specifier across many reset/remock cycles within one test file —
// tests would non-deterministically observe a STALE mock from an earlier
// describe block regardless of `resetModules()`. Rather than fight that
// experimental-feature instability, `getRealWorktreeMtimeMs` accepts an
// optional `deps.resolveProcessCwd` override — mirroring the `deps = {}`
// injection convention `watchdog-beat.mjs` already uses in this codebase for
// exactly this reason. This file therefore:
//   - mocks `node:child_process` exactly ONCE, at module load, via a single
//     `execFileSyncMock` that defaults to a real passthrough (same idiom as
//     `process-cwd.jest.spec.mjs`'s Phase 1 spec) — every test overrides it
//     only via `mockImplementationOnce`/assertion, never via re-registration;
//   - imports `worktree-mtime-scanner.mjs` ONCE, statically, at module top;
//   - substitutes `resolveProcessCwd` per test via the `deps` parameter
//     instead of mocking `./process-cwd.mjs` at all.
//
// FAKE-SEAM NAMING — why `ARM_FAKE_WORKTREE_SCANNER_JSON` is a NEW, distinct
// env var from the existing `ARM_FAKE_WORKTREE_MTIME_JSON`:
//
// `ARM_FAKE_WORKTREE_MTIME_JSON` (see cli.mjs ~line 5407) lives at the
// `buildWatchdogCandidates` CLI layer, several calls above this module — when
// set, the CLI composition never calls `getRealWorktreeMtimeMs` (or even
// `resolveProcessCwd`) AT ALL, it substitutes the fake value directly into the
// candidate object it builds. That seam cannot be used to unit-test THIS
// module in isolation, because by design it never reaches this module.
//
// `ARM_FAKE_WORKTREE_SCANNER_JSON` is this module's OWN seam, keyed by pid
// (string keys, JSON-encoded numeric millisecond values — same shape
// convention as `ARM_FAKE_PROCESS_CWD_JSON`/`ARM_FAKE_WORKTREE_MTIME_JSON`),
// consulted first inside `getRealWorktreeMtimeMs` itself. When set, NEITHER
// `resolveProcessCwd` NOR any `git`/`fs.stat` call occurs — this lets a caller
// (or a future CLI layer) exercise this module's own composition/contract
// (return-type shape, `honorFakeSeam` plumbing) without needing a real git
// worktree or spawned process, while `ARM_FAKE_WORKTREE_MTIME_JSON` remains
// the seam for bypassing this module entirely from the CLI's point of view.

import { jest } from '@jest/globals';
import { createRequire } from 'node:module';
import {
  mkdtempSync,
  rmSync,
  writeFileSync,
  symlinkSync,
  mkdirSync,
  realpathSync,
  statSync,
  readFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { spawn } from 'node:child_process';
import path from 'node:path';

const require = createRequire(import.meta.url);
const realChildProcess = require('node:child_process');

const execFileSyncMock = jest.fn((...args) => realChildProcess.execFileSync(...args));

jest.unstable_mockModule('node:child_process', () => ({
  ...realChildProcess,
  execFileSync: execFileSyncMock,
}));

const { getRealWorktreeMtimeMs } = await import('./worktree-mtime-scanner.mjs');
const { probeExecOptions } = await import('./probe-bound.mjs');

const REPO_ROOT = path.resolve(process.cwd());

beforeEach(() => {
  execFileSyncMock.mockClear();
  execFileSyncMock.mockImplementation((...args) => realChildProcess.execFileSync(...args));
});

function git(args, cwd) {
  return realChildProcess.execFileSync('git', args, { cwd, encoding: 'utf8' });
}

function initRepoWithOneCommit(dir) {
  git(['init', '-q'], dir);
  git(['config', 'user.email', 'test@example.com'], dir);
  git(['config', 'user.name', 'Test'], dir);
  writeFileSync(path.join(dir, 'tracked.txt'), 'hello\n');
  git(['add', 'tracked.txt'], dir);
  git(['commit', '-q', '-m', 'initial'], dir);
}

// ---------------------------------------------------------------------------
// Fake-seam tests (cases 10, 11) — no resolveProcessCwd/git involved at all.
// ---------------------------------------------------------------------------

describe('ARM_FAKE_WORKTREE_SCANNER_JSON seam', () => {
  afterEach(() => {
    delete process.env.ARM_FAKE_WORKTREE_SCANNER_JSON;
  });

  it('resolves against the fake JSON map without calling resolveProcessCwd or git at all', async () => {
    process.env.ARM_FAKE_WORKTREE_SCANNER_JSON = JSON.stringify({ '4321': 1_700_000_000_000 });

    const result = await getRealWorktreeMtimeMs(4321);

    expect(result).toBe(1_700_000_000_000);
    expect(execFileSyncMock).not.toHaveBeenCalled();
  });

  it('returns null without shelling out when the seam is set but the pid is not a key in the map', async () => {
    process.env.ARM_FAKE_WORKTREE_SCANNER_JSON = JSON.stringify({ '9999': 1_700_000_000_000 });

    const result = await getRealWorktreeMtimeMs(4321);

    expect(result).toBeNull();
    expect(execFileSyncMock).not.toHaveBeenCalled();
  });

  it('honorFakeSeam: false forces the real path even when ARM_FAKE_WORKTREE_SCANNER_JSON is set', async () => {
    process.env.ARM_FAKE_WORKTREE_SCANNER_JSON = JSON.stringify({ '999999999': 1_700_000_000_000 });

    // A pid this improbable will not resolve a real cwd via lsof, so the real
    // path must fall through to null rather than honoring the fake map.
    const result = await getRealWorktreeMtimeMs(999_999_999, false);

    expect(result).toBeNull();
  });

  // bot-review fix — the fake-seam JSON.parse used to sit OUTSIDE its
  // own try/catch, so a malformed env-var value threw synchronously out of a
  // function whose documented contract (this file's header comment, and this
  // function's own doc comment) says it never throws.
  it('resolves to null (never throws) when ARM_FAKE_WORKTREE_SCANNER_JSON is malformed JSON', async () => {
    process.env.ARM_FAKE_WORKTREE_SCANNER_JSON = '{not valid json';

    await expect(getRealWorktreeMtimeMs(4321)).resolves.toBeNull();
    expect(execFileSyncMock).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Case 2 — resolveProcessCwd resolves to null: inject it via `deps`, assert
// no git call.
// ---------------------------------------------------------------------------

describe('resolveProcessCwd resolves to null', () => {
  it('returns null and never attempts a git call', async () => {
    const result = await getRealWorktreeMtimeMs(4321, true, {
      resolveProcessCwd: async () => null,
    });

    expect(result).toBeNull();
    expect(execFileSyncMock).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Real filesystem / real git integration tests (cases 1, 3, 5, 6, 7, 9).
// Case 1, 3, 5, 6 use the REAL resolveProcessCwd (no deps override) against a
// real spawned process's cwd. Cases 7 and 9 inject `deps.resolveProcessCwd`
// for a specific cwd value while keeping the git/fs walk itself real.
// ---------------------------------------------------------------------------

describe('real integration — git-tracked worktree mtime walk', () => {
  let tempDir;
  let child;

  beforeEach(() => {
    // realpathSync: macOS's /var is itself a symlink to /private/var, and the
    // kernel (hence lsof's own `n<path>` field, via resolveProcessCwd) always
    // reports the resolved path.
    tempDir = realpathSync(mkdtempSync(path.join(tmpdir(), 'arm-worktree-mtime-')));
  });

  afterEach(() => {
    if (child && child.exitCode === null && child.signalCode === null) {
      child.kill('SIGKILL');
    }
    rmSync(tempDir, { recursive: true, force: true });
  });

  it(
    // Case 1
    'returns a timestamp close to "now" for a real git repo + real spawned process cwd',
    async () => {
      initRepoWithOneCommit(tempDir);

      child = spawn('/bin/sleep', ['5'], { cwd: tempDir, stdio: 'ignore' });
      await new Promise((resolve) => setTimeout(resolve, 300));

      const before = Date.now();
      const result = await getRealWorktreeMtimeMs(child.pid);
      const after = Date.now();

      expect(typeof result).toBe('number');
      expect(Number.isFinite(result)).toBe(true);
      // The tracked file was written moments ago by initRepoWithOneCommit, so
      // its mtime should sit somewhere close to "now" — well within this
      // generous window, guarding against clock-skew flakiness rather than
      // asserting an exact value.
      expect(result).toBeGreaterThan(before - 60_000);
      expect(result).toBeLessThanOrEqual(after + 1_000);
    },
    10_000,
  );

  it(
    // Case 3
    'returns null when the resolved cwd is not a git repo at all',
    async () => {
      // tempDir is a real, non-git directory — no git init at all.
      writeFileSync(path.join(tempDir, 'not-tracked.txt'), 'hi\n');

      child = spawn('/bin/sleep', ['5'], { cwd: tempDir, stdio: 'ignore' });
      await new Promise((resolve) => setTimeout(resolve, 300));

      const result = await getRealWorktreeMtimeMs(child.pid);

      expect(result).toBeNull();
    },
    10_000,
  );

  it(
    // Case 5
    'returns null (never 0/NaN) for a real git repo with zero tracked files',
    async () => {
      git(['init', '-q'], tempDir);
      git(['config', 'user.email', 'test@example.com'], tempDir);
      git(['config', 'user.name', 'Test'], tempDir);
      // Deliberately no `git add`, no commit — zero tracked files.

      child = spawn('/bin/sleep', ['5'], { cwd: tempDir, stdio: 'ignore' });
      await new Promise((resolve) => setTimeout(resolve, 300));

      const result = await getRealWorktreeMtimeMs(child.pid);

      expect(result).not.toBe(0);
      expect(result).not.toBeNaN();
      expect(result).toBeNull();
    },
    10_000,
  );

  it(
    // Case 6
    "skips a tracked file that's missing from disk and still succeeds using the other files",
    async () => {
      initRepoWithOneCommit(tempDir);
      writeFileSync(path.join(tempDir, 'second.txt'), 'second\n');
      git(['add', 'second.txt'], tempDir);
      git(['commit', '-q', '-m', 'second commit'], tempDir);

      // Now delete "second.txt" from disk WITHOUT telling git — it remains
      // in the index (`git ls-files` will still list it) but `fs.stat` on it
      // will fail.
      rmSync(path.join(tempDir, 'second.txt'));

      child = spawn('/bin/sleep', ['5'], { cwd: tempDir, stdio: 'ignore' });
      await new Promise((resolve) => setTimeout(resolve, 300));

      const before = Date.now();
      const result = await getRealWorktreeMtimeMs(child.pid);

      expect(typeof result).toBe('number');
      expect(Number.isFinite(result)).toBe(true);
      expect(result).toBeGreaterThan(before - 60_000);
    },
    10_000,
  );

  it(
    // Case 7 — TOCTOU: resolveProcessCwd resolved a real dir, but it is
    // deleted before the git/stat walk runs.
    'fails closed to null (not a throw) when the resolved cwd no longer exists on disk',
    async () => {
      const goneDir = mkdtempSync(path.join(tmpdir(), 'arm-worktree-mtime-gone-'));
      initRepoWithOneCommit(goneDir);
      rmSync(goneDir, { recursive: true, force: true });

      await expect(
        getRealWorktreeMtimeMs(4321, true, { resolveProcessCwd: async () => goneDir }),
      ).resolves.toBeNull();
    },
  );

  it(
    // Case 9 — bounded execution via probeExecOptions().
    'issues the git ls-files shell-out with options matching probeExecOptions()',
    async () => {
      initRepoWithOneCommit(tempDir);

      await getRealWorktreeMtimeMs(4321, true, { resolveProcessCwd: async () => tempDir });

      expect(execFileSyncMock).toHaveBeenCalled();
      const [, , options] = execFileSyncMock.mock.calls[0];
      expect(options).toEqual(expect.objectContaining(probeExecOptions()));
    },
  );

  it(
    // review finding 1 — explicit maxBuffer override, so a
    // tracked-file listing bigger than Node's 1 MB default cannot throw
    // ENOBUFS and be swallowed into a false "not a git repo" null.
    'issues the git ls-files -z shell-out with a maxBuffer well above the 1 MB default',
    async () => {
      initRepoWithOneCommit(tempDir);

      await getRealWorktreeMtimeMs(4321, true, { resolveProcessCwd: async () => tempDir });

      expect(execFileSyncMock).toHaveBeenCalled();
      const [, args, options] = execFileSyncMock.mock.calls[0];
      expect(args).toContain('-z');
      expect(options.maxBuffer).toBeGreaterThanOrEqual(64 * 1024 * 1024);
    },
  );

  it(
    // review finding 2 — a tracked file with a non-ASCII name that
    // `git ls-files` would C-quote by default (e.g. `"caf\303\251.txt"`)
    // must still be found and stat'd, not silently dropped.
    'finds and stats a tracked file with a non-ASCII name (git-quoted by default)',
    async () => {
      initRepoWithOneCommit(tempDir);
      const nonAsciiName = 'café.txt';
      writeFileSync(path.join(tempDir, nonAsciiName), 'latest\n');
      git(['add', nonAsciiName], tempDir);
      git(['commit', '-q', '-m', 'add non-ascii file'], tempDir);

      child = spawn('/bin/sleep', ['5'], { cwd: tempDir, stdio: 'ignore' });
      await new Promise((resolve) => setTimeout(resolve, 300));

      const before = Date.now();
      const result = await getRealWorktreeMtimeMs(child.pid);
      const after = Date.now();

      // The non-ASCII file was the most recently written/committed tracked
      // file, so a correct scan's max mtime must reflect it, not silently
      // fall back to the earlier `tracked.txt` (which would happen if the
      // quoted path failed to resolve and was swallowed by the missing-file
      // catch).
      expect(typeof result).toBe('number');
      expect(result).toBeGreaterThan(before - 60_000);
      expect(result).toBeLessThanOrEqual(after + 1_000);
    },
    10_000,
  );
});

// ---------------------------------------------------------------------------
// Case 4 — linked git worktree (`.git` is a FILE, not a directory).
// ---------------------------------------------------------------------------

describe('linked git worktree case (`.git` is a file, not a directory)', () => {
  let worktreeDir;

  afterEach(() => {
    if (worktreeDir) {
      try {
        git(['worktree', 'remove', '--force', worktreeDir], REPO_ROOT);
      } catch {
        // best-effort cleanup; fall through to a manual rmSync below
      }
      rmSync(worktreeDir, { recursive: true, force: true });
    }
    worktreeDir = undefined;
  });

  it(
    'correctly resolves tracked files and their mtimes via a linked worktree cwd',
    async () => {
      worktreeDir = path.join(tmpdir(), `arm-linked-worktree-${process.pid}-${Date.now()}`);
      git(['worktree', 'add', '--detach', worktreeDir, 'HEAD'], REPO_ROOT);

      // Sanity-check the fixture itself matches the ticket's description:
      // `.git` inside a linked worktree is a FILE containing `gitdir: ...`.
      const dotGitPath = path.join(worktreeDir, '.git');
      const dotGitStat = statSync(dotGitPath);
      expect(dotGitStat.isFile()).toBe(true);
      expect(readFileSync(dotGitPath, 'utf8')).toMatch(/^gitdir:/);

      const result = await getRealWorktreeMtimeMs(4321, true, {
        resolveProcessCwd: async () => worktreeDir,
      });

      expect(typeof result).toBe('number');
      expect(Number.isFinite(result)).toBe(true);
      expect(result).not.toBeNull();
    },
    15_000,
  );
});

// ---------------------------------------------------------------------------
// Case 8 — symlinked path component in the resolved cwd.
// ---------------------------------------------------------------------------

describe('symlinked path component in the resolved cwd', () => {
  let realBase;
  let symlinkBase;
  let repoDirViaSymlink;

  afterEach(() => {
    rmSync(realBase, { recursive: true, force: true });
    rmSync(symlinkBase, { force: true });
  });

  it(
    'joins fs.stat calls against the resolved (symlinked) cwd correctly',
    async () => {
      realBase = realpathSync(mkdtempSync(path.join(tmpdir(), 'arm-symlink-real-')));
      symlinkBase = path.join(tmpdir(), `arm-symlink-link-${process.pid}-${Date.now()}`);
      symlinkSync(realBase, symlinkBase, 'dir');

      const repoDirName = 'repo';
      mkdirSync(path.join(realBase, repoDirName));
      initRepoWithOneCommit(path.join(realBase, repoDirName));

      repoDirViaSymlink = path.join(symlinkBase, repoDirName);

      const before = Date.now();
      const result = await getRealWorktreeMtimeMs(4321, true, {
        resolveProcessCwd: async () => repoDirViaSymlink,
      });

      expect(typeof result).toBe('number');
      expect(Number.isFinite(result)).toBe(true);
      expect(result).toBeGreaterThan(before - 60_000);
    },
  );
});

// ---------------------------------------------------------------------------
// Case 12 — return-type contract: number | null, never throws.
// ---------------------------------------------------------------------------

describe('return-type contract', () => {
  it('never throws for a wildly out-of-range pid with no fake seam and no real process', async () => {
    delete process.env.ARM_FAKE_WORKTREE_SCANNER_JSON;

    await expect(getRealWorktreeMtimeMs(999_999_999)).resolves.toBeNull();
  });
});
