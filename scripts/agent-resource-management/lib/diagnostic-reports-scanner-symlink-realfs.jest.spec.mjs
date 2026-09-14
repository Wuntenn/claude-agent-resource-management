// M3 regression — proves the symlinked-scan-directory defense in
// `diagnostic-reports-scanner.mjs` actually holds against REAL filesystem
// trailing-slash semantics, not just a fully-mocked `lstat`.
//
// WHY THIS FILE EXISTS SEPARATELY from `diagnostic-reports-scanner.jest.spec.mjs`:
// that suite mocks `node:fs/promises` wholesale via `jest.unstable_mockModule`,
// so its "symlinked scan directory" case only proves the code calls
// `isSymbolicLink()` and branches on it — it can never catch a bug in how the
// *path itself* is passed to a real `lstat`. The bug this regression targets
// was exactly that: POSIX `lstat` FOLLOWS a symlink when the path carries a
// trailing slash (`lstat('link')` → `isSymbolicLink() === true`, but
// `lstat('link/')` → `false`), and the production default
// (`/Library/Logs/DiagnosticReports/`) carries one. A fully-mocked `lstat`
// cannot exercise that OS-level behaviour at all — only a real filesystem
// call can.
//
// This file therefore does NOT mock `node:fs/promises`. It builds a real
// symlinked directory under `os.tmpdir()`, points `scanDiagnosticReports` at
// it WITH a trailing slash (mirroring the production default's shape), and
// asserts it is still rejected.

import { mkdtemp, mkdir, symlink, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { scanDiagnosticReports } from './diagnostic-reports-scanner.mjs';

describe('scanDiagnosticReports — real filesystem, trailing-slash symlinked scan directory', () => {
  let workDir;
  let realDir;
  let symlinkedDir;

  beforeEach(async () => {
    workDir = await mkdtemp(join(tmpdir(), 'arm-diagnostic-reports-scanner-'));
    realDir = join(workDir, 'real-target');
    symlinkedDir = join(workDir, 'symlinked-dir');
    await mkdir(realDir, { recursive: true });
    await symlink(realDir, symlinkedDir, 'dir');
  });

  afterEach(async () => {
    await rm(workDir, { recursive: true, force: true });
  });

  it('rejects a symlinked scan directory even when the caller passes a trailing slash', async () => {
    const result = await scanDiagnosticReports({
      since: new Date('2026-08-01T00:00:00.000Z'),
      dir: `${symlinkedDir}/`,
    });

    expect(result.ok).toBe(true);
    expect(result.files).toEqual([]);
    expect(result.reason).toMatch(/is a symlink/);
  });

  it('still scans normally when the (non-symlinked) directory is passed with a trailing slash', async () => {
    const result = await scanDiagnosticReports({
      since: new Date('2026-08-01T00:00:00.000Z'),
      dir: `${realDir}/`,
    });

    expect(result.ok).toBe(true);
    expect(result.files).toEqual([]);
    expect(result.reason).toBeUndefined();
  });
});
