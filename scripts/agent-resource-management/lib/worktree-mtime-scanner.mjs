// Phase 2 — git-tracked worktree-mtime scanner.
//
// Composes Phase 1's `resolveProcessCwd` with a git-tracked-file mtime walk
// of that cwd: `git -C <cwd> ls-files -z` (bounded via `probeExecOptions()`
// plus an explicit generous `maxBuffer`, mirroring
// `process-cwd.mjs`/`footprint-sampler.mjs`), then `fs.stat` each listed path
// to find the latest mtime. Consumer contract (`liveness.mjs`):
// `worktreeMtimeMs: number | null` — `null` means "unknown", never a guessed
// `0`/`NaN`, and this function must never throw.
//
// `ARM_FAKE_WORKTREE_SCANNER_JSON` is this module's OWN fake seam, distinct
// from the CLI-layer `ARM_FAKE_WORKTREE_MTIME_JSON` — see the header comment
// in `worktree-mtime-scanner.jest.spec.mjs` for the full rationale. When set
// (and `honorFakeSeam` is true, the default) it is consulted FIRST, before
// `resolveProcessCwd` or any git/fs call, mirroring `resolveProcessCwd`'s own
// `!== undefined` check so an explicit empty string counts as set.

import { execFileSync } from 'node:child_process';
import { stat } from 'node:fs/promises';
import path from 'node:path';
import { resolveProcessCwd as defaultResolveProcessCwd } from './process-cwd.mjs';
import { probeExecOptions } from './probe-bound.mjs';

const GIT_BIN = 'git';

/**
 * `maxBuffer` override for the `git ls-files -z` shell-out.
 *
 * Node's `execFileSync` defaults `maxBuffer` to 1 MB; a repo whose tracked
 * -file listing exceeds that throws `ENOBUFS`, which the call site's `catch`
 * cannot distinguish from "not a git repo" — silently degrading a large
 * worktree to `null` instead of a real scan. This repo's own `git ls-files`
 * output is ~95 KB (verified via `git ls-files | wc -c`); 64 MB is sized well
 * above any realistic tracked-file listing while still bounding worst-case
 * memory use for a single synchronous shell-out.
 */
const LS_FILES_MAX_BUFFER_BYTES = 64 * 1024 * 1024;

/**
 * Parses `git ls-files -z` output into a list of relative tracked-file
 * paths.
 *
 * `-z` NUL-terminates each entry and disables `core.quotePath`'s C-quoting
 * entirely (unlike `-c core.quotePath=false`, which only stops quoting
 * non-ASCII bytes — a filename containing a literal backslash or an embedded
 * double-quote is still C-quoted by git even with `quotePath=false`,
 * verified empirically). `-z` round-trips every tracked path byte-for-byte,
 * so no unquoting step is needed here.
 *
 * @param {string} stdout
 * @returns {string[]}
 */
function parseLsFilesOutput(stdout) {
  return stdout.split('\0').filter((entry) => entry.length > 0);
}

/**
 * Resolves the latest mtime (in milliseconds) across a pid's git-tracked
 * worktree files.
 *
 * When `honorFakeSeam` is `true` (the default) and
 * `ARM_FAKE_WORKTREE_SCANNER_JSON` is set (checked via `!== undefined`), it
 * is parsed as a JSON object keyed by pid string and the mapped millisecond
 * value is returned — or `null` if the pid is not a key in the map. Neither
 * `resolveProcessCwd` nor any git/fs call occurs on this path. When
 * `honorFakeSeam` is `false`, `ARM_FAKE_WORKTREE_SCANNER_JSON` is never read
 * at all.
 *
 * Otherwise: calls `resolveProcessCwd(pid)`; if it resolves to `null`, this
 * short-circuits to `null` with no git call. If a cwd is resolved, shells out
 * to `git -C <cwd> ls-files -z` (bounded via `probeExecOptions()`,
 * `stdio: 'pipe'`, and an explicit `maxBuffer` of
 * `LS_FILES_MAX_BUFFER_BYTES` so a large tracked-file listing cannot be
 * mistaken for "not a git repo") to enumerate tracked files. A non-zero exit
 * (not a git repo, or any other git failure) resolves to `null`. Zero tracked files
 * resolves to `null` (never `0`). Each tracked path is resolved against the
 * cwd and `fs.stat`'d; a file whose stat fails (e.g. present in the index but
 * missing from disk) is skipped rather than aborting the whole walk. The
 * maximum mtime across all successfully-stat'd files is returned in
 * milliseconds. If every stat fails, or the cwd itself no longer exists
 * (TOCTOU), resolves to `null`. Never throws.
 *
 * `deps.resolveProcessCwd` (mirrors `watchdog-beat.mjs`'s own `deps = {}`
 * injection convention) lets a caller/test substitute Phase 1's resolver
 * without touching Jest's experimental ESM module-mocking machinery — the
 * default is the real `resolveProcessCwd` from `./process-cwd.mjs`.
 *
 * @param {number} pid
 * @param {boolean} [honorFakeSeam]
 * @param {{ resolveProcessCwd?: (pid: number, honorFakeSeam?: boolean) => Promise<string | null> }} [deps]
 * @returns {Promise<number | null>}
 */
export async function getRealWorktreeMtimeMs(pid, honorFakeSeam = true, deps = {}) {
  const resolveProcessCwd = deps.resolveProcessCwd ?? defaultResolveProcessCwd;

  if (honorFakeSeam && process.env.ARM_FAKE_WORKTREE_SCANNER_JSON !== undefined) {
    try {
      const fakeMap = JSON.parse(process.env.ARM_FAKE_WORKTREE_SCANNER_JSON);
      const pidKey = String(pid);
      return Object.prototype.hasOwnProperty.call(fakeMap, pidKey) ? fakeMap[pidKey] : null;
    } catch {
      // Malformed fake-seam JSON must degrade like every other failure this
      // function documents — "never throws" — rather than propagate a
      // synchronous SyntaxError out of a test-only code path.
      return null;
    }
  }

  try {
    const cwd = await resolveProcessCwd(pid, honorFakeSeam);
    if (cwd === null) return null;

    let stdout;
    try {
      stdout = execFileSync(GIT_BIN, ['-C', cwd, 'ls-files', '-z'], {
        ...probeExecOptions(),
        stdio: 'pipe',
        maxBuffer: LS_FILES_MAX_BUFFER_BYTES,
      });
    } catch {
      return null;
    }

    const trackedFiles = parseLsFilesOutput(stdout);
    if (trackedFiles.length === 0) return null;

    let maxMtimeMs = null;
    for (const relativePath of trackedFiles) {
      try {
        const absolutePath = path.resolve(cwd, relativePath);
        const fileStat = await stat(absolutePath);
        const mtimeMs = fileStat.mtimeMs;
        if (maxMtimeMs === null || mtimeMs > maxMtimeMs) {
          maxMtimeMs = mtimeMs;
        }
      } catch {
        // Tracked in the index but missing from disk (or otherwise
        // unstat-able) — skip this file, keep walking the rest.
      }
    }

    return maxMtimeMs;
  } catch {
    return null;
  }
}
