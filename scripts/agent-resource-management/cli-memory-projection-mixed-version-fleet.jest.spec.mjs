// review — [accepted risk, ] mixed-version fleet.
//
// Mirrors ./cli-aimd-ceiling.jest.spec.mjs's own "[accepted risk, ]
// mixed-version fleet" describe block, applied to the per-agent-class
// projected-peak memory admission gate instead of the AIMD
// ceiling: an orchestrator still running a pre-Phase-2 `cli.mjs` binary never
// calls `--agent-class`/`--running-agent-classes` at all, so its dibs entry
// carries neither `agentClasses` nor `agentClass` — exactly the shape a raw
// `declareDibs` call with no class field produces, since that flag simply
// didn't exist yet in that binary. This file pins that this gap is KNOWN and
// ACCEPTED (see `cli.mjs`'s `UNLABELLED_AGENT_CLASS` doc comment and
// `lib/coordination-file.mjs`'s `reconcileMemoryProjectionAdmission` doc
// comment for the in-code disclosure this test backs), not silently assumed
// away — this test is expected to stay GREEN across and any future
// phase, exactly like its own pinning test.

import { spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { declareDibs } from './lib/coordination-file.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const CLI = join(__dirname, 'cli.mjs');

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

/** Runs the real cli.mjs as a child process — never imports it directly. */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

function parseStdout(stdout) {
  return JSON.parse(stdout);
}

/** Seeds `count` --record-outcome observations for `operationType` (== agentClass) via the real CLI. */
function seedHistory(historyFilePath, agentClass, peakMemoryMbValues) {
  peakMemoryMbValues.forEach((peakMemoryMb, index) => {
    const { status, stderr } = runCli(
      [
        '--record-outcome',
        `--operation-type=${agentClass}`,
        `--orchestrator-id=orch-seed-${agentClass}-${index}`,
        `--peak-memory-mb=${peakMemoryMb}`,
      ],
      { ARM_HISTORY_FILE: historyFilePath },
    );
    expect(status).toBe(0);
    expect(stderr).toBe('');
  });
}

let workDir;
let coordinationFilePath;
let historyFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-memory-projection-mixed-fleet-'));
  coordinationFilePath = join(workDir, 'coordination.json');
  historyFilePath = join(workDir, 'history.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

describe('[accepted risk, ] mixed-version fleet: a pre-Phase-2 orchestrator\'s footprint is invisible to the memory-projection gate', () => {
  test('a "new" orchestrator is admitted against its own class alone, even while a heavy "old" (pre-Phase-2) orchestrator is already live and would blow the combined budget if it were visible', async () => {
    const HEAVY_CLASS = 'heavy-agent';
    const now = 1_700_000_000_000;

    // Seeded so the candidate's OWN contribution (median of these four clean
    // peaks -> 12_000, see the exact computation below) sits close enough to
    // the ~14_336 MB margined budget (16_384 MB host minus the default 2_048
    // MB margin) that only ~2_336 MB of headroom remains. This is
    // deliberately tighter than the flat `coldStartDefaultMemoryMb` (2_560
    // MB, `lib/memory-projection.mjs`) an unlabelled legacy dibs entry would
    // contribute if a future "partial fix" started folding it into the sum:
    // 12_000 + 2_560 = 14_560 MB, which crosses the budget. A margin loose
    // enough to absorb that cold-start default too (as the original, wider
    // fixture below this comment's predecessor used) would let such a
    // partial fix keep reporting `spawn-allowed` and pass this test anyway —
    // the exact silently-weakened-pin risk this fixture is sized to close.
    seedHistory(historyFilePath, HEAVY_CLASS, [11_900, 11_950, 12_050, 12_100]);

    // The "old" (pre-existing-Phase-2) orchestrator is genuinely live and, in
    // reality, is running a HEAVY_CLASS agent right now — but its binary
    // predates `--agent-class`/`--running-agent-classes` entirely, so its
    // persisted dibs entry carries neither `agentClasses` nor `agentClass`.
    // This is exactly what `declareDibs` with no class field produces; it is
    // not a synthetic malformed entry, it is what that binary has always
    // written.
    await declareDibs(coordinationFilePath, {
      orchestratorId: 'orch-old-pre-phase-2',
      desiredAgents: 1,
      declaredAt: now,
    });

    // The "new" (Phase-2-aware) orchestrator's beat requests admission for
    // the SAME heavy class. If the old orchestrator's real HEAVY_CLASS
    // footprint were visible to the gate, the projected sum (~24,000 MB)
    // would cross the ~14,336 MB budget and this beat would be refused. It
    // is granted instead — pinning the accepted gap, not a hypothetical.
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-new-phase-2-aware', '--desired-agents=1', `--agent-class=${HEAVY_CLASS}`],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_HISTORY_FILE: historyFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_NOW_MS: String(now),
        ARM_BEAT_NOW_TEST_MODE: '1',
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = parseStdout(stdout);

    // The gate only ever saw its OWN candidate class — the old
    // orchestrator's live entry contributed nothing to the sum, exactly the
    // documented accepted risk. This is the known/accepted, TESTED gap, not
    // an assumption that a mixed-version fleet is safe by construction.
    expect(parsed.type).not.toBe('memory-projection-block');
    expect(parsed.type).toBe('spawn-allowed');

    // `spawn-allowed` never carries `totalProjectedMemoryMb` (only
    // `memory-projection-block` does — see `buildTrafficLight`'s literal
    // shape in `lib/allowance.mjs`), so asserting on `parsed` alone cannot
    // pin the exact candidate-only contribution the gate actually summed. A
    // control beat gets there instead: it asks for the SAME candidate class,
    // but with `--running-agent-classes=<HEAVY_CLASS>` explicitly passed —
    // i.e. deliberately reproducing the sum the gate WOULD have computed had
    // the old orchestrator's real HEAVY_CLASS footprint genuinely been
    // visible to it. Because both the "running" and "candidate" slots here
    // are the identical class, `computeProjectedPeakMemoryMb` prices each
    // slot separately from the SAME per-class history and sums them
    // (`lib/memory-projection.mjs`), so this control's
    // `totalProjectedMemoryMb` is exactly double the true single-class
    // contribution — crossing the ~14,336 MB margined budget and blocking.
    //
    // Deliberately run against a FRESH, otherwise-empty coordination file
    // (not `coordinationFilePath`): reusing the main scenario's file would
    // also surface `orch-new-phase-2-aware`'s own still-live HEAVY_CLASS dibs
    // entry via `readAgentClasses()`, inflating this control's sum to THREE
    // HEAVY_CLASS contributions instead of the intended two and corrupting
    // the "exactly double" arithmetic below. This control's whole purpose is
    // isolated per-class pricing, not a repeat of the mixed-fleet exclusion
    // already pinned above.
    const controlCoordinationFilePath = join(workDir, 'coordination-control.json');
    const control = runCli(
      [
        '--orchestrator-id=orch-control-if-visible',
        '--desired-agents=1',
        `--agent-class=${HEAVY_CLASS}`,
        `--running-agent-classes=${HEAVY_CLASS}`,
      ],
      {
        ARM_COORDINATION_FILE: controlCoordinationFilePath,
        ARM_HISTORY_FILE: historyFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_NOW_MS: String(now),
        ARM_BEAT_NOW_TEST_MODE: '1',
      },
    );
    expect(control.status).toBe(0);
    expect(control.stderr).toBe('');
    const parsedControl = parseStdout(control.stdout);
    expect(parsedControl.type).toBe('memory-projection-block');

    // The MEDIAN of the four seeded clean peaks (11_900, 11_950, 12_050,
    // 12_100) is 12_000, per `estimateOperationCost`'s empirical-median rule
    // (`lib/cost-estimate.mjs`) — computed here from the test's own fixtures,
    // not hand-picked. The control's two identical-class slots must each
    // price at exactly that figure, i.e. the candidate's true single-class
    // contribution is exactly half the control's reported sum.
    const expectedSingleClassContributionMb = 12_000;
    expect(parsedControl.totalProjectedMemoryMb).toBe(2 * expectedSingleClassContributionMb);

    // This is the load-bearing assertion: it proves the main scenario above
    // was granted `spawn-allowed` because the gate's sum genuinely contained
    // only ONE HEAVY_CLASS contribution — the candidate's own — not two, and
    // that this single contribution alone (12_000 MB) is what the gate
    // actually admitted against, not merely a lucky low number. The fixture
    // above is deliberately sized so 12_000 MB leaves only ~2_336 MB of
    // headroom under the ~14_336 MB margined budget — LESS than the flat
    // `coldStartDefaultMemoryMb` (2_560 MB) an unlabelled legacy dibs entry
    // would contribute. A future "partial fix" that started folding the old
    // orchestrator's entry into the sum via that cold-start estimate (rather
    // than genuinely reading its real, class-specific footprint) would push
    // the main scenario's sum to 12_000 + 2_560 = 14_560 MB, crossing the
    // budget and flipping its `type` away from `spawn-allowed` — so this
    // fixture, unlike the original wider-margin one, would catch that
    // regression via the existing `parsed.type === 'spawn-allowed'`
    // assertion above, rather than silently continuing to pass it.
  });
});
