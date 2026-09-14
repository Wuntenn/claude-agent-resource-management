// Phase 0 — outer acceptance test proving `cli.mjs`'s REAL
// `realCollect()` wiring of `compressions`/`decompressions` from a genuine
// `vm_stat` shell-out, end to end into `memory.compressionVelocity`.
//
// `./compression-velocity-outer.jest.spec.mjs` already
// covers this signal's CONTRACT, but exclusively via the
// `ARM_FAKE_COLLECT_JSON` seam — which bypasses `realCollect()`/`parseVmStat`
// entirely and hands `compressions`/`decompressions` to `cli.mjs` directly in
// fake JSON (see that file's own header comment). That seam can never catch
// a wiring bug INSIDE `realCollect()` itself.
//
// Confirmed root cause (investigation): `realCollect()` (cli.mjs
// ~line 1185) calls `parseVmStat(vmStatRaw)`, which correctly extracts
// `compressions`/`decompressions` from real `vm_stat` output — but
// `realCollect()`'s returned `memory` object (~line 1226-1240) never
// forwards those two fields onward, only `pressureLevel`, `swapUsedMb`,
// `swapTotalMb`, `compressedMb`, `freeRamMb`. Downstream,
// `computeCompressionVelocity`'s `current.compressions`/`current.decompressions`
// (fed from `rawSample.memory?.compressions`/`?.decompressions`) are
// therefore always `undefined` for any REAL beat, which makes
// `memory.compressionVelocity.coldStart` structurally unable to ever clear
// and `compressionRatio` always serialize to `null` (NaN -> JSON -> null).
//
// This test spawns the REAL `cli.mjs` process (mirroring the process-spawn
// idiom in `./compression-velocity-outer.jest.spec.mjs`,
// `./outer-acceptance.jest.spec.mjs`, and
// `./footprint-outer-acceptance.jest.spec.mjs`) WITHOUT setting
// `ARM_FAKE_COLLECT_JSON`, so `collect()` falls through to the real
// `realCollect()` and genuinely shells out to `vm_stat`/`sysctl`/`df` on this
// (macOS) host — the one code path none of the existing compression-velocity
// tests exercise.
//
// RED by construction against the current, unfixed `cli.mjs` — Phase 2 (a
// separate agent) adds the two missing fields to `realCollect()`'s returned
// `memory` object to turn this green. Do not add stub/production fixes here
// — this phase is test-only.

import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const T0 = 1_700_000_000_000;
// Must exceed DEFAULT_FRESHNESS_WINDOW_MS (90s, cli.mjs) so beat 2 does NOT
// reuse beat 1's cached shared sample and instead takes its own genuine
// fresh `realCollect()` sample — i.e. forces `tookFreshSample: true` on beat
// 2 too, per the Build Plan. Chosen well clear of the 90s boundary so this
// isn't sensitive to sub-second scheduling jitter between the two spawns.
const ELAPSED_MS = 120_000;

/**
 * Runs one simulated beat of the REAL `cli.mjs` as a child process, sharing
 * `coordinationFilePath` across calls so beat 2 can see whatever beat 1
 * persisted. Deliberately does NOT set `ARM_FAKE_COLLECT_JSON` — that seam
 * short-circuits `collect()` before `realCollect()` is ever reached, which
 * is exactly the code path this test exists to exercise.
 *
 * @param {object} args
 * @param {string} args.coordinationFilePath
 * @param {string} args.orchestratorId
 * @param {number} args.nowMs
 * @returns {{ status: number | null, stdout: string, stderr: string }}
 */
function runRealBeat({ coordinationFilePath, orchestratorId, nowMs }) {
  const result = spawnSync(
    'node',
    [CLI, `--orchestrator-id=${orchestratorId}`, '--desired-agents=1'],
    {
      encoding: 'utf8',
      env: {
        ...process.env,
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_FAKE_NOW_MS: String(nowMs),
      },
    },
  );
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

// Platform guard (mirrors ./cli.jest.spec.mjs's `describeOnDarwin` precedent,
// CI fix): this test deliberately never sets `ARM_FAKE_COLLECT_JSON`,
// so both real beats hit `realCollect()`'s `execFileSync` branch for real —
// shelling out to `/usr/bin/vm_stat`/`/usr/sbin/sysctl`/`/bin/df`, all
// macOS-only binaries absent on the `ubuntu-latest` CI runner. Gated to
// Darwin hosts so CI reports this suite as skipped (an environment mismatch
// unrelated to the production code under test) rather than failing on every
// PR; on a real macOS dev machine it still runs for real and catches a
// genuine regression.
const describeOnDarwin = process.platform === 'darwin' ? describe : describe.skip;

describeOnDarwin('compression-velocity signal wired from the REAL vm_stat shell-out (Phase 0 outer acceptance)', () => {
  let workDir;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), 'arm-compression-velocity-real-collect-outer-'));
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  it(
    'first real beat: compressionRatio is finite (never null) from the real vm_stat sample alone; ' +
      'second real beat, forced past the freshness window so it takes its own genuine sample: ' +
      'coldStart is false and compressionsPerSec is finite',
    () => {
      const coordinationFilePath = join(workDir, 'coordination.json');
      const orchestratorId = 'orch-compression-velocity-real-collect';

      const beat1 = runRealBeat({ coordinationFilePath, orchestratorId, nowMs: T0 });
      expect(beat1.status).toBe(0);
      const decision1 = JSON.parse(beat1.stdout);

      expect(decision1.memory).toBeDefined();
      expect(decision1.memory.compressionVelocity).toBeDefined();
      // A point-in-time value computed from `current` alone (regardless of
      // `coldStart`) — must already be assertable on the very first real
      // beat once `realCollect()` is fixed. Real vm_stat counters are
      // host-dependent and non-deterministic, so only finiteness is
      // asserted, never a specific number.
      expect(Number.isFinite(decision1.memory.compressionVelocity.compressionRatio)).toBe(true);

      const beat2 = runRealBeat({
        coordinationFilePath,
        orchestratorId,
        nowMs: T0 + ELAPSED_MS,
      });
      expect(beat2.status).toBe(0);
      const decision2 = JSON.parse(beat2.stdout);

      expect(decision2.memory).toBeDefined();
      expect(decision2.memory.compressionVelocity).toBeDefined();
      expect(decision2.memory.compressionVelocity.coldStart).toBe(false);
      // Real cumulative compressor counters may not visibly change between
      // two quick sequential spawns on a throttled CI runner (they're
      // monotonic since-boot counters, not guaranteed to tick within a test
      // run) — asserting finiteness, not a delta value, keeps this
      // deterministic and non-flaky.
      expect(Number.isFinite(decision2.memory.compressionVelocity.compressionsPerSec)).toBe(true);
    },
  );
});
