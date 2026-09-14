// Phase 0 — outer acceptance test for the "compression velocity"
// signal: `vm_stat`'s cumulative `Compressions:`/`Decompressions:` counters
// turned into a rate, persisted across beats via the coordination file, and
// attached to the printed traffic-light JSON as a record-only sibling field
// — the same additive-sibling shape `loadAverage`/`diskTrend`/`compressedMb`
// already use at cli.mjs's `process.stdout.write` call site (see that call
// site's own comment, cli.mjs around line 7573).
//
// RED by construction, right now, for three independent reasons — none of
// which exist yet on this branch:
//   - Phase 1: `./lib/recon.mjs`'s `parseVmStat` does not parse
//     `Compressions:`/`Decompressions:` into `compressions`/`decompressions`
//     fields.
//   - Phase 2: `./lib/compression-velocity.mjs` (a new module) does not
//     exist — there is no `computeCompressionVelocity(previous, current)`.
//   - Phase 3: `cli.mjs` never persists a prior beat's `compressions`/
//     `decompressions`+timestamp on `SHARED_SAMPLE_RESERVED_ID`'s reserved
//     coordination-file entry, and never attaches `memory.compressionVelocity`
//     to its printed JSON.
//
// This test drives the REAL, black-box `cli.mjs` (never imported directly —
// matches ./cli.jest.spec.mjs's, ./outer-acceptance.jest.spec.mjs's, and
// ./footprint-outer-acceptance.jest.spec.mjs's process-spawn idiom) across
// TWO sequential simulated beats sharing one coordination file, using the
// SAME `ARM_FAKE_COLLECT_JSON` / `ARM_COORDINATION_FILE` / `ARM_FAKE_NOW_MS`
// / `ARM_BEAT_NOW_TEST_MODE` test seams already documented in SKILL.md — no
// new test infrastructure is invented here.
//
// `ARM_FAKE_COLLECT_JSON` bypasses `parseVmStat` entirely (`collect()` in
// cli.mjs just `JSON.parse`s it straight into the `{ memory, disk }` shape —
// see cli.mjs's `collect()`, and `compressedMb`'s own precedent in
// ./footprint-outer-acceptance.jest.spec.mjs's golden fixture, which supplies
// `compressedMb` directly in the fake JSON without any real `vm_stat` text).
// So this test supplies `compressions`/`decompressions` directly on the fake
// `memory` reading too — Phase 1's real `parseVmStat` parsing is exercised
// by its own dedicated unit tests, not by this outer test, exactly as
// `compressedMb`'s real `vm_stat`-text parsing isn't re-proven here either.
//
// Do not add stub implementations to make this pass — that defeats the
// point of the outer/phase red-green loop. The builder implements Phases
// 1-3 (recon.mjs parsing, lib/compression-velocity.mjs, cli.mjs wiring) to
// turn this green.

import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const MEMORY_GREEN_BASE = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 };

const T0 = 1_700_000_000_000;
// 30s between beats — long enough that a per-second rate is easy to reason
// about by hand, short enough to stay a plausible orchestrator beat cadence.
const ELAPSED_MS = 30_000;

// Synthetic vm_stat-shaped cumulative counters (since-boot, monotonically
// increasing under normal operation — see the ticket's Phase 1 description).
// Beat 1 -> beat 2 delta: +300 compressions, +60 decompressions over 30s.
const BEAT_1_COMPRESSOR_COUNTERS = { compressions: 1_000, decompressions: 200 };
const BEAT_2_COMPRESSOR_COUNTERS = { compressions: 1_300, decompressions: 260 };

const EXPECTED_COMPRESSIONS_PER_SEC =
  (BEAT_2_COMPRESSOR_COUNTERS.compressions - BEAT_1_COMPRESSOR_COUNTERS.compressions) / (ELAPSED_MS / 1000);
const EXPECTED_COMPRESSION_RATIO =
  (BEAT_2_COMPRESSOR_COUNTERS.compressions - BEAT_1_COMPRESSOR_COUNTERS.compressions) /
  (BEAT_2_COMPRESSOR_COUNTERS.decompressions - BEAT_1_COMPRESSOR_COUNTERS.decompressions);

/**
 * Runs one simulated beat of the real `cli.mjs` as a child process, sharing
 * `coordinationFilePath` across calls so beat 2 can see whatever beat 1
 * persisted. Mirrors `./outer-acceptance.jest.spec.mjs`'s `runCli` helper —
 * never imports cli.mjs directly.
 *
 * @param {object} args
 * @param {string} args.coordinationFilePath
 * @param {string} args.orchestratorId
 * @param {number} args.nowMs
 * @param {object} args.memory a full `ARM_FAKE_COLLECT_JSON` `memory` reading
 * @returns {{ status: number | null, stdout: string, stderr: string }}
 */
function runBeat({ coordinationFilePath, orchestratorId, nowMs, memory }) {
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
        ARM_FAKE_LOAD_AVERAGE_JSON: JSON.stringify([1.0, 1.0, 1.0]),
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory, disk: DISK_GREEN }),
      },
    },
  );
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

describe('compression-velocity signal (Phase 0 outer acceptance)', () => {
  let workDir;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), 'arm-compression-velocity-outer-'));
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  it(
    'beat 1 is cold-start (compressionsPerSec: 0), beat 2 derives a real rate from the persisted ' +
      'prior beat, sharing one coordination file across two sequential cli.mjs invocations',
    () => {
      const coordinationFilePath = join(workDir, 'coordination.json');
      const orchestratorId = 'orch-compression-velocity';

      const beat1 = runBeat({
        coordinationFilePath,
        orchestratorId,
        nowMs: T0,
        memory: { ...MEMORY_GREEN_BASE, ...BEAT_1_COMPRESSOR_COUNTERS },
      });
      expect(beat1.status).toBe(0);
      const decision1 = JSON.parse(beat1.stdout);

      expect(decision1.memory).toBeDefined();
      expect(decision1.memory.compressionVelocity).toBeDefined();
      expect(decision1.memory.compressionVelocity.coldStart).toBe(true);
      expect(decision1.memory.compressionVelocity.compressionsPerSec).toBe(0);
      // `compressionRatio` needs no prior-beat delta — it is derived from the
      // CURRENT sample alone (Phase 2's locked contract, see
      // compression-velocity.jest.spec.mjs's "cold start ... compressionRatio
      // computed from current alone" test). Cold start only affects
      // `compressionsPerSec`/`coldStart`, so beat 1's fixture
      // (compressions: 1000, decompressions: 200) correctly yields 5 even
      // though this is the first beat.
      expect(decision1.memory.compressionVelocity.compressionRatio).toBe(5);

      const beat2 = runBeat({
        coordinationFilePath,
        orchestratorId,
        nowMs: T0 + ELAPSED_MS,
        memory: { ...MEMORY_GREEN_BASE, ...BEAT_2_COMPRESSOR_COUNTERS },
      });
      expect(beat2.status).toBe(0);
      const decision2 = JSON.parse(beat2.stdout);

      expect(decision2.memory).toBeDefined();
      expect(decision2.memory.compressionVelocity).toBeDefined();
      expect(decision2.memory.compressionVelocity.coldStart).toBe(false);
      expect(decision2.memory.compressionVelocity.compressionsPerSec).toBeCloseTo(
        EXPECTED_COMPRESSIONS_PER_SEC,
        6,
      );
      expect(decision2.memory.compressionVelocity.compressionRatio).toBeCloseTo(
        EXPECTED_COMPRESSION_RATIO,
        6,
      );
    },
  );

  it(
    'is genuinely record-only: the admission decision (type, allowance) is byte-identical between a ' +
      "control beat whose reading omits compressor counters entirely (today's pre-ticket shape) and " +
      'the same beat with compressor counters added',
    () => {
      const controlCoordinationFilePath = join(workDir, 'coordination-control.json');
      const treatmentCoordinationFilePath = join(workDir, 'coordination-treatment.json');
      const orchestratorId = 'orch-compression-velocity-parity';

      const control = runBeat({
        coordinationFilePath: controlCoordinationFilePath,
        orchestratorId,
        nowMs: T0,
        memory: { ...MEMORY_GREEN_BASE },
      });
      expect(control.status).toBe(0);
      const controlDecision = JSON.parse(control.stdout);

      const treatment = runBeat({
        coordinationFilePath: treatmentCoordinationFilePath,
        orchestratorId,
        nowMs: T0,
        memory: { ...MEMORY_GREEN_BASE, ...BEAT_1_COMPRESSOR_COUNTERS },
      });
      expect(treatment.status).toBe(0);
      const treatmentDecision = JSON.parse(treatment.stdout);

      // The decision itself must never move because compressor counters were
      // present on the reading — only a NEW, additive, record-only sibling
      // field may differ.
      expect(treatmentDecision.type).toBe(controlDecision.type);
      expect(treatmentDecision.allowance).toBe(controlDecision.allowance);

      // The genuine proof of "record-only, additive, never gates admission":
      // the field is present when compressor counters were supplied, and the
      // rest of the decision is otherwise identical to the control run that
      // never supplied them.
      expect(treatmentDecision.memory?.compressionVelocity).toBeDefined();
      expect(treatmentDecision.memory.compressionVelocity.coldStart).toBe(true);
    },
  );
});
