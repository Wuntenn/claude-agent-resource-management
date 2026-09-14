// Phase 3 — RED outer/integration tests for a `--debug` diagnostic
// flag that does not exist yet in `cli.mjs` (confirmed: `grep -n
// "debug\|verbose" cli.mjs` returns zero matches as of this commit).
//
// Spawns the REAL `cli.mjs` process — mirrors the process-spawn idiom in
// `./compression-velocity-outer-real-collect.jest.spec.mjs` and
// `./outer-acceptance.jest.spec.mjs` (isolated `ARM_COORDINATION_FILE` temp
// dir per test, `ARM_FAKE_COLLECT_JSON` for deterministic raw memory/disk
// input, `ARM_FAKE_NOW_MS`/`ARM_BEAT_NOW_TEST_MODE` for deterministic time).
//
// Intent (per the Build Plan): `--debug` prints the RAW memory/disk inputs
// that feed `classifyMemoryRaw`/`classifyDiskAxis` (`lib/threshold.mjs`) to
// STDERR, without ever changing the stdout JSON body that every existing
// caller does `JSON.parse(stdout)` on.
//
// RED by construction against the current, unfixed `cli.mjs` — a `--debug`
// flag is simply an unrecognized/no-op flag today, so none of the stderr
// assertions below can pass. This phase is test-only: no production code
// (`cli.mjs` or otherwise) is touched by this commit. A later phase (a
// different agent) implements the flag to turn this green.

import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const T0 = 1_700_000_000_000;

// Known fixture reading — deliberately distinctive numeric values (unlikely
// to collide with any other value cli.mjs might incidentally print) so
// substring assertions against stderr are unambiguous.
const MEMORY_FIXTURE = {
  pressureLevel: 2,
  swapUsedMb: 500,
  swapTotalMb: 4000,
  compressedMb: 100,
  freeRamMb: 800,
  compressions: 10,
  decompressions: 5,
};
const DISK_FIXTURE = { freeDiskGb: 50, declineRateGbPerHour: 0 };

/**
 * Runs the real `cli.mjs` as a child process — never imports it directly.
 *
 * @param {string[]} args
 * @param {Record<string, string>} extraEnv
 * @returns {{ status: number | null, stdout: string, stderr: string }}
 */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

describe('Phase 3 outer acceptance — --debug diagnostic flag (RED: flag does not exist yet)', () => {
  let workDir;
  let coordinationFilePath;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), 'arm-debug-flag-'));
    coordinationFilePath = join(workDir, 'coordination.json');
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  test('1. --debug prints raw memory/disk classification inputs to stderr', () => {
    const result = runCli(
      ['--session-id=session-debug-1', '--desired-agents=1', '--debug'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_FIXTURE, disk: DISK_FIXTURE }),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_FAKE_NOW_MS: String(T0),
      },
    );

    expect(result.status).toBe(0);

    // Raw values feeding `classifyMemoryRaw` — at minimum pressureLevel,
    // swapUsedMb, swapTotalMb, and the `stale` marker (false for a
    // well-formed fixture reading) must be surfaced.
    expect(result.stderr).toEqual(expect.stringContaining('2')); // pressureLevel
    expect(result.stderr).toEqual(expect.stringContaining('500')); // swapUsedMb
    expect(result.stderr).toEqual(expect.stringContaining('4000')); // swapTotalMb
    expect(result.stderr.toLowerCase()).toEqual(expect.stringContaining('stale'));

    // Raw values feeding `classifyDiskAxis` — at minimum freeDiskGb and the
    // `stale` marker.
    expect(result.stderr).toEqual(expect.stringContaining('50')); // freeDiskGb
  });

  test('2. stdout JSON is byte-for-byte identical between --debug present and absent', () => {
    const orchestratorId = 'orch-debug-parity';

    // Isolated coordination files per run so the two beats can never
    // interact with (or read hysteresis history persisted by) each other —
    // any difference in stdout must come purely from the presence/absence
    // of `--debug`, not from cross-beat state.
    const withoutDebugCoordinationFile = join(workDir, 'coordination-without-debug.json');
    const withDebugCoordinationFile = join(workDir, 'coordination-with-debug.json');

    // `ARM_COORDINATION_FILE` is set here (env-seam-premise's required
    // pairing with ARM_FAKE_NOW_MS/ARM_BEAT_NOW_TEST_MODE in the same object
    // literal) as a placeholder that both call sites below unconditionally
    // override via `{ ...env, ARM_COORDINATION_FILE: ... }` — the explicit
    // key after a spread always wins, so this placeholder is never actually
    // read; each beat still gets its own isolated coordination file.
    const env = {
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_FIXTURE, disk: DISK_FIXTURE }),
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_NOW_MS: String(T0),
      ARM_COORDINATION_FILE: withoutDebugCoordinationFile,
    };

    const withoutDebug = runCli(
      [`--orchestrator-id=${orchestratorId}`, '--desired-agents=1'],
      { ...env, ARM_COORDINATION_FILE: withoutDebugCoordinationFile },
    );
    const withDebug = runCli(
      [`--orchestrator-id=${orchestratorId}`, '--desired-agents=1', '--debug'],
      { ...env, ARM_COORDINATION_FILE: withDebugCoordinationFile },
    );

    expect(withoutDebug.status).toBe(0);
    expect(withDebug.status).toBe(0);

    // The critical safety property: `--debug` must never leak onto stdout,
    // since every existing caller does `JSON.parse(stdout)` on it.
    expect(withDebug.stdout).toBe(withoutDebug.stdout);

    // Both must still parse as the SAME decision JSON body.
    expect(JSON.parse(withDebug.stdout)).toEqual(JSON.parse(withoutDebug.stdout));
  });

  test('3. --debug combined with --heartbeat degrades gracefully: no throw, no stdout pollution, prints whatever is available to stderr', () => {
    const result = runCli(
      ['--orchestrator-id=orch-debug-heartbeat', '--heartbeat', '--debug'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_FIXTURE, disk: DISK_FIXTURE }),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_FAKE_NOW_MS: String(T0),
      },
    );

    // Must not crash the process.
    expect(result.status).toBe(0);

    // stdout must still be exactly one well-formed JSON decision body — no
    // debug text interleaved into it.
    expect(() => JSON.parse(result.stdout)).not.toThrow();

    // `classifyMemoryAxis`/`classifyDiskAxis` run on a `--heartbeat` beat
    // identically to a `--desired-agents` beat (both execute before the
    // `isHeartbeat` branch in `main()`), so the same raw memory/disk inputs
    // must still be available to print to stderr on this beat type too.
    expect(result.stderr).toEqual(expect.stringContaining('2')); // pressureLevel
    expect(result.stderr).toEqual(expect.stringContaining('500')); // swapUsedMb
    expect(result.stderr).toEqual(expect.stringContaining('50')); // freeDiskGb
  });

  test('4. --debug works under the ARM_FAKE_COLLECT_JSON seam (no real vm_stat/sysctl/df shell-out required)', () => {
    // Named separately per the convergence-analysis review: scenario 1
    // already proves this (it never shells out to a real collector), but
    // this seam-specific guarantee is worth pinning as its own test case so
    // a future refactor that accidentally routes --debug's diagnostic
    // printing through a fresh, un-seamed collect() call gets caught here
    // specifically, independent of the other scenarios' broader assertions.
    const result = runCli(
      ['--orchestrator-id=orch-debug-fake-collect-seam', '--desired-agents=1', '--debug'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_FIXTURE, disk: DISK_FIXTURE }),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_FAKE_NOW_MS: String(T0),
      },
    );

    expect(result.status).toBe(0);
    // The exact fixture values injected via ARM_FAKE_COLLECT_JSON must be
    // what's echoed to stderr — proving the diagnostic output is wired to
    // the fake-collect seam's raw sample, not a genuine vm_stat shell-out.
    expect(result.stderr).toEqual(expect.stringContaining('500')); // swapUsedMb
    expect(result.stderr).toEqual(expect.stringContaining('4000')); // swapTotalMb
    expect(result.stderr).toEqual(expect.stringContaining('50')); // freeDiskGb
  });

  test('5. --debug reports stale:true for a genuinely stale sample (not just always-false)', () => {
    // Pins the fix for a Medium finding caught in this ticket's own
    // pre-pr-review pass: `takeSample()` (`lib/recon.mjs`) puts `stale` at
    // the TOP LEVEL of its return (`{ stale, memory, disk }`), never nested
    // as `memory.stale`/`disk.stale` — `staleMemoryMarker()`/
    // `staleDiskMarker()` degrade to null/NaN fields instead, with no
    // nested `stale` key of their own. An earlier version of
    // `printDebugDiagnostics` read `memory.stale`/`disk.stale` (always
    // `undefined`), so it printed `stale: false` unconditionally — even on
    // a genuinely stale sample. A malformed `ARM_FAKE_COLLECT_JSON` (missing
    // `memory`/`disk`) forces `takeSample()`'s `isWellFormedSample` check to
    // fail, producing a real `{ stale: true, ... }` sample.
    const result = runCli(
      ['--session-id=session-debug-stale', '--desired-agents=1', '--debug'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: '{}',
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_FAKE_NOW_MS: String(T0),
      },
    );

    expect(result.status).toBe(0);
    expect(result.stderr).toEqual(expect.stringContaining('"stale":true'));
    expect(result.stderr).not.toEqual(expect.stringContaining('"stale":false'));
  });
});
