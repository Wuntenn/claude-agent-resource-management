// RED unit tests for the not-yet-implemented
// scripts/agent-resource-management/lib/process-cwd.mjs module (Phase 1
// — "pid-to-cwd resolver"), which will export `resolveProcessCwd(pid,
// honorFakeSeam = true)` following `footprint-sampler.mjs`'s established
// shape: bounded `execFileSync` via `probeExecOptions()`, `stdio: 'pipe'`,
// never throws, resolves to `null` on any failure, an `ARM_FAKE_*` env-var
// seam checked first.
//
// Do NOT add a stub implementation to make this pass — that is the builder's
// job in the next phase. Every test below is expected to fail at IMPORT TIME
// (`Cannot find module './process-cwd.mjs'`) because the module does not
// exist yet — that is an acceptable red state for a not-yet-created module.
//
// MOCKING IDIOM — why this file departs slightly from footprint-sampler's
// pure `jest.unstable_mockModule` idiom:
//
// This suite needs BOTH a real, non-mocked shell-out (resolving a REAL
// running pid's cwd via the real `lsof`, per the ticket's requirement #1 —
// "no fake seam, real lsof shell-out") AND fully-mocked shell-outs (to force
// deterministic parsing/failure fixtures that real `lsof` output cannot
// reliably reproduce byte-for-byte). `jest.unstable_mockModule` on its own
// makes every subsequent import of `node:child_process` — including the one
// inside `process-cwd.mjs` — resolve to the mock, with no way to "opt out"
// per test.
//
// The fix: the mock factory below wraps a REAL reference to
// `execFileSync`, obtained via `createRequire(import.meta.url)` — a
// synchronous CommonJS `require()` call. Jest's ESM mock registry
// (`jest.unstable_mockModule`) only intercepts `import`/dynamic `import()`
// specifiers resolved through Jest's own ESM loader; a `require()` call goes
// through Node's ordinary CJS loader and is untouched by it. That gives this
// suite a genuine, unmocked `execFileSync` to delegate to by default —
// `execFileSyncMock` is a `jest.fn()` whose default implementation forwards
// every call to the real function — while individual tests can still
// `mockImplementationOnce`/`mockReturnValueOnce` to fake a specific
// success/failure/parsing scenario. This is a legitimate extension of the
// pattern, not a departure from its intent: production code still only ever
// sees `execFileSync` imported from `'node:child_process'`, and the fakery is
// confined to this spec file.

import { jest } from '@jest/globals';
import { createRequire } from 'node:module';
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync, realpathSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

const require = createRequire(import.meta.url);
const realChildProcess = require('node:child_process');

const execFileSyncMock = jest.fn((...args) => realChildProcess.execFileSync(...args));

jest.unstable_mockModule('node:child_process', () => ({
  ...realChildProcess,
  execFileSync: execFileSyncMock,
}));

const { resolveProcessCwd } = await import('./process-cwd.mjs');
const { probeExecOptions } = await import('./probe-bound.mjs');

beforeEach(() => {
  execFileSyncMock.mockClear();
  execFileSyncMock.mockImplementation((...args) => realChildProcess.execFileSync(...args));
});

afterEach(() => {
  delete process.env.ARM_FAKE_PROCESS_CWD_JSON;
});

// ---------------------------------------------------------------------------
// 1. Real, non-mocked resolution against a real running pid.
// ---------------------------------------------------------------------------

describe('resolves the real cwd of a real running process (no fake seam, real lsof)', () => {
  let tempDir;
  let child;

  beforeEach(() => {
    // realpathSync: macOS's /var is itself a symlink to /private/var, and the
    // kernel (hence lsof's own `n<path>` field) always reports the resolved
    // path — os.tmpdir()'s raw /var/... value would never match.
    tempDir = realpathSync(mkdtempSync(path.join(tmpdir(), 'arm-process-cwd-')));
  });

  afterEach(() => {
    if (child && child.exitCode === null && child.signalCode === null) {
      child.kill('SIGKILL');
    }
    rmSync(tempDir, { recursive: true, force: true });
  });

  it(
    "resolves the real, absolute cwd for a real running pid spawned with a known `cwd` option",
    async () => {
      child = spawn('/bin/sleep', ['5'], { cwd: tempDir, stdio: 'ignore' });
      // Give the kernel a moment to finish exec'ing before lsof inspects it —
      // spawn() returns synchronously with a pid, but the process's cwd is
      // not guaranteed observable via lsof on the very first tick.
      await new Promise((resolve) => setTimeout(resolve, 300));

      const result = await resolveProcessCwd(child.pid);

      expect(result).toBe(tempDir);
      expect(path.isAbsolute(result)).toBe(true);
    },
    10_000,
  );
});

// ---------------------------------------------------------------------------
// 2. Never throws for a pid that does not exist.
// ---------------------------------------------------------------------------

describe('returns null (never throws) for a pid that does not exist', () => {
  it('returns null for a very large, improbable pid', async () => {
    const result = await resolveProcessCwd(999_999_999);
    expect(result).toBeNull();
  });

  it('returns null for a pid that was just reaped after kill() + wait', async () => {
    const child = spawn('/bin/sleep', ['5'], { stdio: 'ignore' });
    const deadPid = child.pid;
    child.kill('SIGKILL');
    await new Promise((resolve) => child.once('exit', resolve));
    // A brief settle window so the kernel has fully reclaimed the pid table
    // entry before lsof is asked about it.
    await new Promise((resolve) => setTimeout(resolve, 100));

    const result = await resolveProcessCwd(deadPid);

    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// 3. Empty / unparsable shell-out output.
// ---------------------------------------------------------------------------

describe('malformed or empty shell-out output', () => {
  it('returns null when the shell-out succeeds but stdout is empty', async () => {
    execFileSyncMock.mockReturnValueOnce('');

    const result = await resolveProcessCwd(1234);

    expect(result).toBeNull();
  });

  it('returns null when stdout contains no parsable n<path> field line', async () => {
    execFileSyncMock.mockReturnValueOnce('some unexpected garbled output with no n-field\n');

    const result = await resolveProcessCwd(1234);

    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// 4. Correct `lsof -Fn`-style parsing — p<pid> line must not be mistaken for
//    the path.
// ---------------------------------------------------------------------------

describe('parses lsof -Fn-style output correctly', () => {
  it('extracts the path from the n<path> line and ignores a leading p<pid> field line', async () => {
    execFileSyncMock.mockReturnValueOnce('p1234\nn/Users/someone/some/project\n');

    const result = await resolveProcessCwd(1234);

    expect(result).toBe('/Users/someone/some/project');
  });

  it('does not return the p<pid> line itself as a path when no n line is present', async () => {
    execFileSyncMock.mockReturnValueOnce('p1234\n');

    const result = await resolveProcessCwd(1234);

    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// 5. Deterministic rule for multiple n<path> lines: take the first.
// ---------------------------------------------------------------------------

describe('multiple n<path> lines — deterministic "take the first" rule', () => {
  it('returns the first n<path> line when more than one is present', async () => {
    execFileSyncMock.mockReturnValueOnce('p1234\nn/first/path\nn/second/path\n');

    const result = await resolveProcessCwd(1234);

    expect(result).toBe('/first/path');
  });
});

// ---------------------------------------------------------------------------
// 6. ARM_FAKE_PROCESS_CWD_JSON seam.
// ---------------------------------------------------------------------------

describe('ARM_FAKE_PROCESS_CWD_JSON seam', () => {
  it('resolves against the fake JSON map without shelling out at all', async () => {
    process.env.ARM_FAKE_PROCESS_CWD_JSON = JSON.stringify({ '1234': '/some/path' });

    const result = await resolveProcessCwd(1234);

    expect(result).toBe('/some/path');
    expect(execFileSyncMock).not.toHaveBeenCalled();
  });

  it('returns null without shelling out when the seam is set but the pid is not a key in the map', async () => {
    process.env.ARM_FAKE_PROCESS_CWD_JSON = JSON.stringify({ '9999': '/some/other/path' });

    const result = await resolveProcessCwd(1234);

    expect(result).toBeNull();
    expect(execFileSyncMock).not.toHaveBeenCalled();
  });

  // bot-review fix — the fake-seam JSON.parse used to sit OUTSIDE its
  // own try/catch, so a malformed env-var value threw synchronously out of a
  // function whose documented contract (this file's header comment, and this
  // function's own doc comment) says it never throws.
  it('resolves to null (never throws) when ARM_FAKE_PROCESS_CWD_JSON is malformed JSON', async () => {
    process.env.ARM_FAKE_PROCESS_CWD_JSON = '{not valid json';

    await expect(resolveProcessCwd(1234)).resolves.toBeNull();
    expect(execFileSyncMock).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// 7. honorFakeSeam parameter (default true, mirrors sampleFootprint).
// ---------------------------------------------------------------------------

describe('honorFakeSeam parameter', () => {
  it('ignores ARM_FAKE_PROCESS_CWD_JSON and always shells out for real when honorFakeSeam is false', async () => {
    process.env.ARM_FAKE_PROCESS_CWD_JSON = JSON.stringify({ '1234': '/fake/should/not/be/used' });
    execFileSyncMock.mockReturnValueOnce('p1234\nn/real/shelled/out/path\n');

    const result = await resolveProcessCwd(1234, false);

    expect(result).toBe('/real/shelled/out/path');
    expect(execFileSyncMock).toHaveBeenCalledTimes(1);
  });

  it('still honors ARM_FAKE_PROCESS_CWD_JSON when honorFakeSeam is true (explicit, matches the default)', async () => {
    process.env.ARM_FAKE_PROCESS_CWD_JSON = JSON.stringify({ '1234': '/some/path' });

    const result = await resolveProcessCwd(1234, true);

    expect(result).toBe('/some/path');
    expect(execFileSyncMock).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// 8. Bounded execution via probeExecOptions().
// ---------------------------------------------------------------------------

describe('shell-out is time-bounded via probeExecOptions', () => {
  it('passes execFileSync options matching probeExecOptions()', async () => {
    execFileSyncMock.mockReturnValueOnce('p1234\nn/bounded/path\n');

    await resolveProcessCwd(1234);

    expect(execFileSyncMock).toHaveBeenCalledTimes(1);
    const [, , options] = execFileSyncMock.mock.calls[0];
    expect(options).toEqual(expect.objectContaining(probeExecOptions()));
  });

  it('uses stdio: "pipe" so failures do not leak onto this process\'s own stderr', async () => {
    execFileSyncMock.mockReturnValueOnce('p1234\nn/bounded/path\n');

    await resolveProcessCwd(1234);

    const [, , options] = execFileSyncMock.mock.calls[0];
    expect(options.stdio).toBe('pipe');
  });
});

// ---------------------------------------------------------------------------
// 9. Permission-denied / non-zero exit resolves to null, not a throw.
// ---------------------------------------------------------------------------

describe('permission-denied / non-zero exit from the shell-out', () => {
  it('returns null when execFileSync throws (e.g. non-zero exit, permission denied)', async () => {
    execFileSyncMock.mockImplementationOnce(() => {
      const error = new Error('Command failed with exit code 1.');
      error.status = 1;
      error.stdout = '';
      error.stderr = 'lsof: no permission to view command name\n';
      throw error;
    });

    const result = await resolveProcessCwd(1234);

    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// argv shape — only the pid is interpolated, no shell-string concatenation.
// ---------------------------------------------------------------------------

describe('argv contains the pid — no string interpolation', () => {
  it('passes an argv array whose entries include String(pid), issued to a bare (PATH-resolved) bin name', async () => {
    execFileSyncMock.mockReturnValueOnce('p1234\nn/argv/shape/path\n');

    await resolveProcessCwd(1234);

    expect(execFileSyncMock).toHaveBeenCalledTimes(1);
    const [bin, argv] = execFileSyncMock.mock.calls[0];
    // Bare, not absolute — `lsof` is PATH-resolved (unlike footprint-
    // sampler.mjs's macOS-exclusive absolute paths) so this shell-out works
    // on both the macOS production target and the Linux CI runners this
    // module's real-integration tests exercise. No path separator anywhere
    // in `bin` confirms it's a plain command name, not a string-concatenated
    // or otherwise unexpectedly-constructed path.
    expect(typeof bin).toBe('string');
    expect(bin).toBe('lsof');
    expect(bin).not.toContain('/');
    expect(argv).toContain('1234');
  });
});
