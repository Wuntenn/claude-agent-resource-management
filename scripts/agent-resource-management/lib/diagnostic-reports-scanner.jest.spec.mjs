// RED unit tests for the not-yet-implemented
// scripts/agent-resource-management/lib/diagnostic-reports-scanner.mjs module
// (Phase 1 — "diagnostics-parity tooling").
//
// Do NOT add a stub implementation to make this pass — that is the builder's
// job in a later phase. Every test below is expected to fail at IMPORT TIME
// (`Cannot find module './diagnostic-reports-scanner.mjs'`) because the
// module does not exist yet.
//
// SHAPE THIS TEST ASSUMES (chosen by this test's author, not yet
// implemented):
//
//   export async function scanDiagnosticReports({ since, dir } = {})
//     -> Promise<{ ok: true, files: Array<{ name, path, mtimeMs }>, reason? }>
//
// `dir` defaults to `/Library/Logs/DiagnosticReports/` in production but is
// always passed explicitly in this suite (injectable for testability, per
// the task brief). The function NEVER throws/rejects — an unreadable
// directory or an empty directory are both representable as an explicit,
// successful ("we looked") result rather than an error, matching the
// ticket's "self-describing JSON, never throws" outer contract exercised by
// collect.jest.spec.mjs.
//
// MOCKING IDIOM: `jest.unstable_mockModule` against `node:fs/promises`,
// matching footprint-sampler.jest.spec.mjs's established idiom for mocking a
// `node:` builtin under native ESM in this skill's `lib/` specs.

import { jest } from '@jest/globals';

const readdirMock = jest.fn();
const lstatMock = jest.fn();

jest.unstable_mockModule('node:fs/promises', () => ({
  readdir: readdirMock,
  lstat: lstatMock,
}));

const { scanDiagnosticReports } = await import('./diagnostic-reports-scanner.mjs');

const FIXTURE_DIR = '/Library/Logs/DiagnosticReports';

/** Node ENOENT/EACCES-shaped error, matching what `fs/promises` throws. */
function eaccesError(targetPath) {
  const error = new Error(`EACCES: permission denied, scandir '${targetPath}'`);
  error.code = 'EACCES';
  error.errno = -13;
  error.syscall = 'scandir';
  error.path = targetPath;
  return error;
}

/** Builds an `lstat`-shaped stats object for a regular file with the given mtime. */
function regularFileStats(mtimeMs) {
  return {
    isSymbolicLink: () => false,
    mtimeMs,
    mtime: new Date(mtimeMs),
  };
}

/** Builds an `lstat`-shaped stats object for a symlink. */
function symlinkStats(mtimeMs) {
  return {
    isSymbolicLink: () => true,
    mtimeMs,
    mtime: new Date(mtimeMs),
  };
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('scanDiagnosticReports — lists matching files within the since window', () => {
  it('includes files with an mtime at or after `since` and excludes older ones', async () => {
    const since = new Date('2026-08-01T00:00:00.000Z');
    const withinWindow = new Date('2026-08-15T00:00:00.000Z').getTime();
    const outsideWindow = new Date('2026-07-01T00:00:00.000Z').getTime();

    readdirMock.mockResolvedValue([
      'JetsamEvent-2026-08-15-091233.ips',
      'spindump-2026-07-01-010000.ips',
    ]);
    lstatMock.mockImplementation((filePath) => {
      if (String(filePath).includes('JetsamEvent')) return Promise.resolve(regularFileStats(withinWindow));
      return Promise.resolve(regularFileStats(outsideWindow));
    });

    const result = await scanDiagnosticReports({ since, dir: FIXTURE_DIR });

    expect(result.ok).toBe(true);
    const names = result.files.map((file) => file.name);
    expect(names).toContain('JetsamEvent-2026-08-15-091233.ips');
    expect(names).not.toContain('spindump-2026-07-01-010000.ips');
  });
});

describe('scanDiagnosticReports — rejects symlinked entries', () => {
  it('excludes a symlinked report file from the results rather than following it', async () => {
    const since = new Date('2026-08-01T00:00:00.000Z');
    const withinWindow = new Date('2026-08-20T00:00:00.000Z').getTime();

    readdirMock.mockResolvedValue([
      'JetsamEvent-legit.ips',
      'shutdownStall-symlinked.ips',
    ]);
    lstatMock.mockImplementation((filePath) => {
      if (String(filePath).includes('symlinked')) return Promise.resolve(symlinkStats(withinWindow));
      return Promise.resolve(regularFileStats(withinWindow));
    });

    const result = await scanDiagnosticReports({ since, dir: FIXTURE_DIR });

    expect(result.ok).toBe(true);
    const names = result.files.map((file) => file.name);
    expect(names).toContain('JetsamEvent-legit.ips');
    expect(names).not.toContain('shutdownStall-symlinked.ips');

    // The path-traversal defense means a symlink must never even be
    // followed/read — asserting lstat (not a following stat/read) was the
    // only inspection performed on it is out of scope for this pure-listing
    // unit; the important, testable-here guarantee is that it is absent
    // from the returned file list.
  });
});

describe('scanDiagnosticReports — symlinked scan directory', () => {
  it('returns an explicit empty-but-successful result and never calls readdir when `dir` itself is a symlink', async () => {
    lstatMock.mockResolvedValue(symlinkStats(Date.now()));

    const result = await scanDiagnosticReports({ since: new Date('2026-08-01T00:00:00.000Z'), dir: FIXTURE_DIR });

    expect(result.ok).toBe(true);
    expect(result.files).toEqual([]);
    expect(typeof result.reason).toBe('string');
    expect(readdirMock).not.toHaveBeenCalled();
  });
});

describe('scanDiagnosticReports — unreadable directory', () => {
  it('returns an explicit empty-but-successful result (never throws) when readdir rejects with EACCES', async () => {
    lstatMock.mockResolvedValue(regularFileStats(Date.now()));
    readdirMock.mockRejectedValue(eaccesError(FIXTURE_DIR));

    let result;
    await expect(
      (async () => {
        result = await scanDiagnosticReports({ since: new Date('2026-08-01T00:00:00.000Z'), dir: FIXTURE_DIR });
      })(),
    ).resolves.toBeUndefined();

    expect(result.ok).toBe(true);
    expect(result.files).toEqual([]);
    expect(typeof result.reason).toBe('string');
  });
});

describe('scanDiagnosticReports — no matching files', () => {
  it('returns an explicit empty-but-successful result when the directory has no files at all', async () => {
    lstatMock.mockResolvedValue(regularFileStats(Date.now()));
    readdirMock.mockResolvedValue([]);

    const result = await scanDiagnosticReports({ since: new Date('2026-08-01T00:00:00.000Z'), dir: FIXTURE_DIR });

    expect(result.ok).toBe(true);
    expect(result.files).toEqual([]);
  });
});
