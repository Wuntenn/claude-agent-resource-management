// Phase 1 — lists macOS DiagnosticReports files modified at or after a
// given `since` timestamp.
//
// `dir` defaults to `/Library/Logs/DiagnosticReports/` in production but is
// always injectable for testability. This function NEVER throws/rejects — an
// unreadable directory or an empty directory are both representable as an
// explicit, successful ("we looked") result rather than an error.
//
// Rejects symlinked entries: every entry is `lstat`'d (never followed) and
// excluded from the result when `isSymbolicLink()` is true — a path-traversal
// defense against a report directory containing a symlink to an arbitrary
// file elsewhere on disk.
//
// Also rejects a symlinked top-level `dir` itself: `dir` is `lstat`'d before
// `readdir` is ever called, so a scan directory that is itself a symlink is
// refused up front rather than silently resolving every listed entry under
// an attacker's target.

import { readdir, lstat } from 'node:fs/promises';

const DEFAULT_DIR = '/Library/Logs/DiagnosticReports/';

/**
 * @param {{ since?: Date, dir?: string }} [options]
 * @returns {Promise<{ ok: true, files: Array<{ name: string, path: string, mtimeMs: number }>, reason?: string }>}
 */
export async function scanDiagnosticReports({ since, dir = DEFAULT_DIR } = {}) {
  const sinceMs = since instanceof Date ? since.getTime() : 0;

  // POSIX `lstat` FOLLOWS a symlink when the path carries a trailing slash
  // (verified empirically: `lstat('link')` reports `isSymbolicLink() ===
  // true`, but `lstat('link/')` reports `false`, because the trailing slash
  // forces resolution through the link to the directory it targets). Without
  // stripping it here, `DEFAULT_DIR`'s own trailing slash — and any caller
  // that passes one — would make the symlink defense below silently inert on
  // exactly the production default this function exists to guard.
  const scanDir = dir.replace(/\/+$/, '') || '/';

  let dirStats;
  try {
    dirStats = await lstat(scanDir);
  } catch (error) {
    return {
      ok: true,
      files: [],
      reason: `diagnostic-reports-scanner: could not read "${dir}" — ${error.message}`,
    };
  }

  if (dirStats.isSymbolicLink()) {
    return { ok: true, files: [], reason: `diagnostic-reports-scanner: "${dir}" is a symlink` };
  }

  let entries;
  try {
    entries = await readdir(scanDir);
  } catch (error) {
    return {
      ok: true,
      files: [],
      reason: `diagnostic-reports-scanner: could not read "${dir}" — ${error.message}`,
    };
  }

  const files = [];
  for (const name of entries) {
    const entryPath = `${scanDir}/${name}`;

    let stats;
    try {
      stats = await lstat(entryPath);
    } catch {
      // Entry vanished or is unreadable between readdir and lstat — skip it
      // rather than failing the whole scan.
      continue;
    }

    if (stats.isSymbolicLink()) continue;
    if (stats.mtimeMs < sinceMs) continue;

    files.push({ name, path: entryPath, mtimeMs: stats.mtimeMs });
  }

  return { ok: true, files };
}
