// Phase 3 (RED) — a pre-this-ticket `__shared-machine-sample__`
// reserved entry (written by an older CLI version that never emitted
// `compressions`/`decompressions`/`timestampMs`) must be read back by
// `cli.mjs` WITHOUT throwing, and treated as cold-start-equivalent for the
// compression-velocity computation — never fabricating a `0`-as-real
// `compressionsPerSec` from a delta against fields that were never there.
//
// This is deliberately a SEPARATE black-box CLI test from
// `./compression-velocity-outer.jest.spec.mjs` (Phase 0's two-sequential-
// real-beats acceptance test): that test always drives BOTH beats through
// this same `cli.mjs`, so its "beat 1" prior entry is always well-formed
// (either absent, or written by THIS ticket's own code). This test instead
// hand-seeds a coordination file with the OLDER, pre-this-ticket shape
// directly — the scenario the outer test cannot exercise — mirroring
// `coordination-file.jest.spec.mjs`'s "pre-this-ticket reserved entry"
// fixture at the unit level, but proven end-to-end through the real CLI
// (never importing cli.mjs directly — matches `./outer-acceptance.jest.spec.mjs`'s
// and `./compression-velocity-outer.jest.spec.mjs`'s process-spawn idiom).
//
// RED by construction: `cli.mjs` does not yet read `compressions`/
// `decompressions` off the shared-sample entry, does not yet call
// `computeCompressionVelocity`, and does not yet attach
// `memory.compressionVelocity` to its printed JSON at all.

import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const MEMORY_GREEN_BASE = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 };

const LEGACY_SAMPLED_AT = 1_700_000_000_000;
const CURRENT_NOW_MS = LEGACY_SAMPLED_AT + 30_000;

const SHARED_SAMPLE_RESERVED_ID = '__shared-machine-sample__';

describe('cli.mjs treats a pre-this-ticket shared-sample entry as cold-start-equivalent for compression velocity', () => {
  let workDir;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), 'arm-compression-velocity-legacy-'));
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  it('reads the legacy entry without throwing and reports coldStart: true, compressionsPerSec: 0 — not a fabricated delta', () => {
    const coordinationFilePath = join(workDir, 'coordination.json');
    const orchestratorId = 'orch-compression-velocity-legacy';

    // Hand-seed a reserved entry in the shape an OLDER CLI (pre-existing)
    // would have written: `reading.memory` has no `compressions`,
    // `decompressions`, or `timestampMs` fields whatsoever — only the
    // fields that existed before this ticket.
    writeFileSync(
      coordinationFilePath,
      JSON.stringify([
        {
          orchestratorId: SHARED_SAMPLE_RESERVED_ID,
          sampledAt: LEGACY_SAMPLED_AT,
          reading: {
            sampledAt: LEGACY_SAMPLED_AT,
            memory: { ...MEMORY_GREEN_BASE },
            disk: { ...DISK_GREEN },
            stale: false,
          },
          declaredAt: LEGACY_SAMPLED_AT,
        },
      ]),
      'utf8',
    );

    const result = spawnSync(
      'node',
      [CLI, `--orchestrator-id=${orchestratorId}`, '--desired-agents=1'],
      {
        encoding: 'utf8',
        env: {
          ...process.env,
          ARM_COORDINATION_FILE: coordinationFilePath,
          ARM_BEAT_NOW_TEST_MODE: '1',
          ARM_FAKE_NOW_MS: String(CURRENT_NOW_MS),
          ARM_FAKE_LOAD_AVERAGE_JSON: JSON.stringify([1.0, 1.0, 1.0]),
          ARM_FAKE_COLLECT_JSON: JSON.stringify({
            memory: { ...MEMORY_GREEN_BASE, compressions: 5_000, decompressions: 1_000 },
            disk: DISK_GREEN,
          }),
        },
      },
    );

    // No crash — a malformed/missing-field "previous" sample must degrade
    // gracefully, exactly like every other coordination-file read path in
    // this file (see the `hasWellFormedSharedReading` doc comment).
    expect(result.status).toBe(0);
    expect(result.stderr).toBe('');

    const decision = JSON.parse(result.stdout);

    expect(decision.memory).toBeDefined();
    expect(decision.memory.compressionVelocity).toBeDefined();
    // Cold-start-equivalent: no genuine "previous" counters were available
    // (the legacy entry has none), so this must read exactly like a
    // first-ever beat — never a fabricated delta against `undefined`
    // treated as `0` (which would misrepresent 5,000 compressions over 30s
    // as a real, enormous rate instead of "no prior data").
    expect(decision.memory.compressionVelocity.coldStart).toBe(true);
    expect(decision.memory.compressionVelocity.compressionsPerSec).toBe(0);
  });
});
