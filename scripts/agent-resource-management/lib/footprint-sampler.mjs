// Phase 1 — sample a single process's physical memory footprint (MB).
//
// Two macOS-only tools report the same underlying value in two different
// literal formats (see `.footprint-format-spike-1266.md`, Phase 0):
//
//   `footprint <pid>`      -> "    phys_footprint: 848 KB"   (space before unit)
//   `vmmap --summary <pid>` -> "Physical footprint:         848K"  (no space)
//
// `footprint` is tried first; on ANY failure (non-zero exit, thrown error,
// unparsable output) this falls back to `vmmap --summary`. If both fail, or
// the surviving stdout does not contain a well-formed footprint line, this
// returns `null` — never `NaN`, never a throw. `ps` is never shelled out to
// (the RSS values it reports are not the same "footprint" concept and were
// the root cause of the inversion bug this module deliberately avoids
// repeating).
//
// Both shell-outs are bounded via `probeExecOptions()` from `probe-bound.mjs`
// so a wedged `footprint`/`vmmap` cannot block a caller indefinitely.
//
// Test seam: mirrors the SHAPE of `cli.mjs`'s `collectPsOutput()`/
// `ARM_FAKE_PS_OUTPUT` idiom — check the env var first, and bypass the real
// shell-out entirely when it is set — but NOT its exact check. `collectPsOutput()`
// uses a truthy check (an empty string is treated as "not set"), whereas this
// module deliberately checks `!== undefined`, so an empty string IS treated as
// an explicit fake value. When `ARM_FAKE_FOOTPRINT_OUTPUT` is set (including to
// an empty string), it is treated as `footprint`'s raw stdout and parsed with
// the same `phys_footprint:` parser used on the real shell-out path —
// `child_process`/`execFileSync` is never touched in that case. The seam is
// consulted first and is a bypass layered in FRONT of the real shell-out
// logic, not a replacement for it: when the env var is unset, the real
// footprint -> vmmap fallback below runs exactly as before.
//
// review, Medium — `honorFakeSeam` (default `true`, mirrors
// `collectPsOutput()`'s own parameter of the same name in cli.mjs) lets
// `handleEvict`'s fallback path force the REAL footprint/vmmap shell-out
// regardless of `ARM_FAKE_FOOTPRINT_OUTPUT` when its own `ARM_EVICT_TEST_MODE`
// master switch is not set — every other caller (the `--poll-footprint` beat,
// direct unit tests, etc.) keeps honoring the seam unconditionally by omitting
// the argument. This closes the gap where `ARM_FAKE_FOOTPRINT_OUTPUT` was the
// one eviction-reachable seam NOT gated behind `ARM_EVICT_TEST_MODE`, contrary
// to that switch's own "must be set before handleEvict honors ANY of these
// seams" claim (see cli.mjs's `handleEvict` header comment).
//
// `sampleFootprint` is `async` solely so its return type is a Promise for
// callers (`await sampleFootprint(pid)`); the work itself remains
// synchronous internally via `execFileSync`.

import { execFileSync } from 'node:child_process';
import { probeExecOptions } from './probe-bound.mjs';

const FOOTPRINT_BIN = '/usr/bin/footprint';
const VMMAP_BIN = '/usr/bin/vmmap';

// Binary (1024-based) unit multipliers, expressed in MB, per the spike's
// explicit contract: `848 KB -> 848 / 1024 = 0.828125 MB`.
const MB_PER_UNIT = {
  B: 1 / (1024 * 1024),
  KB: 1 / 1024,
  K: 1 / 1024,
  MB: 1,
  M: 1,
  GB: 1024,
  G: 1024,
};

/**
 * Parses `footprint`'s `phys_footprint: <number> <unit>` line (space between
 * number and unit; unit is one of `B`/`KB`/`MB`, not fixed-width).
 *
 * @param {string} stdout
 * @returns {number | null} the physical footprint in MB, or `null` if the
 *   line is absent or its unit token is not recognised.
 */
function parseFootprintOutput(stdout) {
  const match = stdout.match(/phys_footprint:\s*([\d.]+)\s*([A-Za-z]+)/);
  if (!match) return null;
  return toMb(match[1], match[2]);
}

/**
 * Parses `vmmap --summary`'s `Physical footprint:  <number><unit>` line (no
 * space between number and unit; unit is one of `K`/`M`/`G`, per the spike).
 *
 * @param {string} stdout
 * @returns {number | null} the physical footprint in MB, or `null` if the
 *   line is absent or its unit token is not recognised.
 */
function parseVmmapOutput(stdout) {
  const match = stdout.match(/Physical footprint:\s*([\d.]+)([A-Za-z]+)/);
  if (!match) return null;
  return toMb(match[1], match[2]);
}

/**
 * @param {string} rawNumber
 * @param {string} rawUnit
 * @returns {number | null}
 */
function toMb(rawNumber, rawUnit) {
  const multiplier = MB_PER_UNIT[rawUnit.toUpperCase()];
  if (multiplier === undefined) return null;
  const value = Number(rawNumber);
  if (!Number.isFinite(value)) return null;
  return value * multiplier;
}

/**
 * Samples a process's current physical memory footprint, in MB.
 *
 * When `honorFakeSeam` is `true` (the default) and `ARM_FAKE_FOOTPRINT_OUTPUT`
 * is set, resolves against that fixed text instead of shelling out at all
 * (see the test-seam note above). When `honorFakeSeam` is `false`,
 * `ARM_FAKE_FOOTPRINT_OUTPUT` is never read at all — this always shells out
 * for real, regardless of what that env var holds. Otherwise (real path)
 * tries `footprint <pid>` first, falling back to `vmmap --summary <pid>` on
 * any failure of the first (thrown error or unparsable success output).
 * Never throws; resolves to `null` when no reading could be obtained.
 *
 * @param {number} pid
 * @param {boolean} [honorFakeSeam]
 * @returns {Promise<number | null>}
 */
export async function sampleFootprint(pid, honorFakeSeam = true) {
  if (honorFakeSeam && process.env.ARM_FAKE_FOOTPRINT_OUTPUT !== undefined) {
    return parseFootprintOutput(process.env.ARM_FAKE_FOOTPRINT_OUTPUT);
  }

  const pidArg = String(pid);

  try {
    // stdio: 'pipe' (not the execFileSync default of inheriting stderr) so a
    // "PID no longer exists" failure (e.g. `footprint: Unable to find pid for
    // process matching '<pid>'`) is captured and swallowed by the catch below
    // rather than leaking onto this CLI's own stderr — mirrors cli.mjs's
    // `collectPsOutput()`.
    const stdout = execFileSync(FOOTPRINT_BIN, [pidArg], {
      ...probeExecOptions(),
      stdio: 'pipe',
    });
    const footprintMb = parseFootprintOutput(stdout);
    if (footprintMb !== null) return footprintMb;
  } catch {
    // Fall through to the vmmap fallback below.
  }

  try {
    // stdio: 'pipe' for the same reason as above — e.g. vmmap's
    // `[fatal] mach port for process 0 not valid` should degrade silently,
    // not leak onto stderr.
    const stdout = execFileSync(VMMAP_BIN, ['--summary', pidArg], {
      ...probeExecOptions(),
      stdio: 'pipe',
    });
    return parseVmmapOutput(stdout);
  } catch {
    return null;
  }
}
