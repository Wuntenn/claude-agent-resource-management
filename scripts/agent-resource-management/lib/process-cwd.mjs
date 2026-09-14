// Phase 1 — resolve a pid's current working directory via `lsof`.
//
// Mirrors `footprint-sampler.mjs`'s `sampleFootprint` shape: an `ARM_FAKE_*`
// env-var seam checked first (via `!== undefined`, so an explicit empty
// string counts as set), a bounded `execFileSync` shell-out via
// `probeExecOptions()` on the real path, `stdio: 'pipe'` so failures never
// leak onto this process's own stderr, and a contract that never throws —
// any failure (non-zero exit, thrown error, unparsable/empty output)
// resolves to `null`.
//
// `resolveProcessCwd` is `async` solely so its return type is a Promise for
// callers, exactly as `sampleFootprint` documents for itself; the work
// itself remains synchronous internally via `execFileSync`.

import { execFileSync } from 'node:child_process';
import { probeExecOptions } from './probe-bound.mjs';

// Bare command name (PATH-resolved), NOT a hardcoded absolute path — unlike
// `footprint-sampler.mjs`'s `/usr/bin/footprint`/`/usr/bin/vmmap` (genuinely
// macOS-exclusive tools with no equivalent elsewhere), `lsof` is a standard
// utility present on both this project's macOS production target AND the
// Linux (`ubuntu-latest`) CI runners this module's own tests execute a real,
// unmocked shell-out against. A hardcoded `/usr/sbin/lsof` (macOS's path) is
// absent on Ubuntu (`/usr/bin/lsof` there instead) and silently degrades
// every real-integration test to "pid not found" — this bit CI
// (caught post-push). Mirrors `worktree-mtime-scanner.mjs`'s own bare
// `const GIT_BIN = 'git'` for the same cross-platform reason.
const LSOF_BIN = 'lsof';

/**
 * Parses `lsof -a -p <pid> -d cwd -Fn` output.
 *
 * The format is field-prefixed, one field per line, no other separators —
 * e.g. `p1234\nn/some/path\n`. The leading `p<pid>` line is not a path and
 * must be ignored. When more than one `n<path>` line is present the rule is
 * deterministic: take the first.
 *
 * @param {string} stdout
 * @returns {string | null}
 */
function parseLsofFnOutput(stdout) {
  const lines = stdout.split('\n');
  for (const line of lines) {
    if (line.startsWith('n')) {
      const cwdPath = line.slice(1);
      return cwdPath.length > 0 ? cwdPath : null;
    }
  }
  return null;
}

/**
 * Resolves a process's current working directory.
 *
 * When `honorFakeSeam` is `true` (the default) and `ARM_FAKE_PROCESS_CWD_JSON`
 * is set (checked via `!== undefined`, so an explicit empty string counts as
 * set), it is parsed as a JSON object keyed by pid string and the mapped path
 * is returned — or `null` if the pid is not a key in the map. No shell-out
 * occurs on this path. When `honorFakeSeam` is `false`,
 * `ARM_FAKE_PROCESS_CWD_JSON` is never read at all — this always shells out
 * for real. Otherwise (real path) shells out to
 * `lsof -a -p <pid> -d cwd -Fn`, bounded via `probeExecOptions()`. Never
 * throws; resolves to `null` on any failure or unparsable output.
 *
 * @param {number} pid
 * @param {boolean} [honorFakeSeam]
 * @returns {Promise<string | null>}
 */
export async function resolveProcessCwd(pid, honorFakeSeam = true) {
  if (honorFakeSeam && process.env.ARM_FAKE_PROCESS_CWD_JSON !== undefined) {
    try {
      const fakeMap = JSON.parse(process.env.ARM_FAKE_PROCESS_CWD_JSON);
      const pidKey = String(pid);
      return Object.prototype.hasOwnProperty.call(fakeMap, pidKey) ? fakeMap[pidKey] : null;
    } catch {
      // Malformed fake-seam JSON must degrade like every other failure this
      // function documents — "never throws" — rather than propagate a
      // synchronous SyntaxError out of a test-only code path.
      return null;
    }
  }

  const pidArg = String(pid);

  try {
    const stdout = execFileSync(LSOF_BIN, ['-a', '-p', pidArg, '-d', 'cwd', '-Fn'], {
      ...probeExecOptions(),
      stdio: 'pipe',
    });
    return parseLsofFnOutput(stdout);
  } catch {
    return null;
  }
}
