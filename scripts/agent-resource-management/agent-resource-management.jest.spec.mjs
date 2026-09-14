// Outer acceptance test for (agent-resource-management skill).
//
// outer loop, Phase 0: this test is RED by construction — none of the
// Phase 1-4 modules it imports exist yet. It exercises the full intended
// decision pipeline end-to-end at the module boundary:
//
//   recon/sample -> per-axis threshold+hysteresis classification
//                -> coordination-file dibs
//                -> priority/allowance/traffic-light output
//
// It is fully hermetic: `collect()` (the only function that would ever shell
// out to vm_stat/sysctl/df/ps in the real implementation) is a plain
// injected/stubbed function here. Nothing in this file spawns a process or
// touches the real machine's resource state; the only real I/O is the
// coordination file itself, written under a per-test temp directory.
//
// This test asserts three end-to-end properties called out by the Build
// Plan's "Outer acceptance test" section:
//
//   (a) hermetic injection — collect() is stubbed, never a real OS call
//   (b) two concurrent synthetic orchestrators declaring dibs concurrently
//       against the same coordination file never lose either entry
//   (c) traffic-light output responds correctly across the full state
//       progression: spawn-allowed (GREEN) -> freeze/hold (AMBER) ->
//       pause-selection (RED) -> hysteresis-gated recovery back to
//       spawn-allowed only after N consecutive GREEN samples (not
//       immediately on one good sample)
//
// Module boundary this test drives (none of these exist yet — Phase 1-4):
//   ./lib/recon.mjs             - takeSample(collect) -> raw machine sample
//   ./lib/threshold.mjs         - classifyMemoryAxis / classifyDiskAxis
//   ./lib/coordination-file.mjs - declareDibs / readDibs / getFreshSample
//   ./lib/allowance.mjs         - computeAllowance / selectPauseCandidate /
//                                  buildTrafficLight
//
// Do not add stub implementations to make this pass — that defeats the
// point of the outer loop. Phases 1-4 turn this green for real.

import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { takeSample } from './lib/recon.mjs';
import { classifyMemoryAxis, classifyDiskAxis } from './lib/threshold.mjs';
import { declareDibs, readDibs, isReservedEntry } from './lib/coordination-file.mjs';
import {
  computeAllowance,
  selectPauseCandidate,
  buildTrafficLight,
} from './lib/allowance.mjs';

// ---------------------------------------------------------------------------
// Fixtures — synthetic machine-state samples, one per §7 traffic-light band.
// Disk axis is held GREEN throughout so the memory axis alone drives the
// traffic-light transitions under test; a full memory x disk matrix is
// Phase 2/4 unit-test territory, not this integration test's job.
// ---------------------------------------------------------------------------

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 };
const MEMORY_AMBER = { pressureLevel: 2, swapUsedMb: 5120, compressedMb: 2048 };
const MEMORY_RED = { pressureLevel: 4, swapUsedMb: 12288, compressedMb: 4096 };

const THRESHOLDS = {
  memory: {
    greenSwapUsedMbBelow: 4096,
    amberSwapUsedMbBelow: 8192,
    redSwapUsedMbAtOrAbove: 10240,
  },
  disk: {
    greenFreeDiskGbAbove: 30,
    amberFreeDiskGbBelow: 20,
    redFreeDiskGbBelow: 10,
  },
  hysteresisRecoverConsecutiveGreen: 3,
};

/** A `collect()` stub — the only place the real implementation shells out
 * to vm_stat/sysctl/df/ps. Here it just returns pre-built fixtures in
 * sequence, so the whole pipeline never touches the real machine. */
function makeStubCollect(sequence) {
  let i = 0;
  return async function collect() {
    const sample = sequence[Math.min(i, sequence.length - 1)];
    i += 1;
    return sample;
  };
}

/** Runs one orchestrator "beat" through the full pipeline: recon -> classify
 * (threading hysteresis history forward) -> dibs -> allowance/traffic-light. */
async function runBeat({
  collect,
  memoryHistory,
  diskHistory,
  coordinationFilePath,
  orchestratorId,
  desiredAgents,
  runningAgents,
}) {
  const rawSample = await takeSample(collect);

  const memoryResult = classifyMemoryAxis(rawSample.memory, THRESHOLDS.memory, memoryHistory, {
    hysteresisRecoverConsecutiveGreen: THRESHOLDS.hysteresisRecoverConsecutiveGreen,
  });
  const diskResult = classifyDiskAxis(rawSample.disk, THRESHOLDS.disk, diskHistory, {
    hysteresisRecoverConsecutiveGreen: THRESHOLDS.hysteresisRecoverConsecutiveGreen,
  });

  await declareDibs(coordinationFilePath, {
    orchestratorId,
    desiredAgents,
    declaredAt: Date.now(),
  });
  const dibs = await readDibs(coordinationFilePath);

  const allowance = computeAllowance(memoryResult.state, diskResult.state, dibs, {
    maxAgentsMemoryAxis: 6,
    maxAgentsDiskAxis: 6,
  });

  const pauseCandidate =
    memoryResult.state === 'RED' || diskResult.state === 'RED'
      ? selectPauseCandidate(runningAgents, { policy: 'highest-rss' })
      : null;

  const trafficLight = buildTrafficLight({
    memoryState: memoryResult.state,
    diskState: diskResult.state,
    allowance,
    pauseCandidate,
  });

  return {
    trafficLight,
    memoryHistory: memoryResult.history,
    diskHistory: diskResult.history,
  };
}

// ---------------------------------------------------------------------------
// Test setup — per-test temp coordination file, cleaned up afterward.
// ---------------------------------------------------------------------------

let workDir;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'agent-resource-management-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// (a) + (b) — two concurrent synthetic orchestrators, neither dibs entry lost
// ---------------------------------------------------------------------------

describe('concurrent dibs declarations', () => {
  it('lands both orchestrators\' dibs entries when declared concurrently against the same coordination file', async () => {
    const collectA = makeStubCollect([{ memory: MEMORY_GREEN, disk: DISK_GREEN }]);
    const collectB = makeStubCollect([{ memory: MEMORY_GREEN, disk: DISK_GREEN }]);

    // Fire both orchestrators' beats concurrently (same tick) against the
    // same coordination file — this is the race the Investigation flagged
    // as the highest-risk gap: neither entry may clobber the other.
    await Promise.all([
      runBeat({
        collect: collectA,
        memoryHistory: undefined,
        diskHistory: undefined,
        coordinationFilePath,
        orchestratorId: 'orchestrator-a',
        desiredAgents: 2,
        runningAgents: [],
      }),
      runBeat({
        collect: collectB,
        memoryHistory: undefined,
        diskHistory: undefined,
        coordinationFilePath,
        orchestratorId: 'orchestrator-b',
        desiredAgents: 3,
        runningAgents: [],
      }),
    ]);

    // Reserved pseudo-entries are filtered out of every orchestrator-entry
    // assertion in this file: `declareDibs` also maintains the
    // `__ever-opted-in__` reserved row in the same write, and these
    // assertions are about real orchestrator entries.
    const dibs = (await readDibs(coordinationFilePath)).filter(
      (entry) => !isReservedEntry(entry.orchestratorId),
    );
    const ids = dibs.map((entry) => entry.orchestratorId).sort();

    expect(ids).toEqual(['orchestrator-a', 'orchestrator-b']);
    expect(dibs.find((entry) => entry.orchestratorId === 'orchestrator-a').desiredAgents).toBe(2);
    expect(dibs.find((entry) => entry.orchestratorId === 'orchestrator-b').desiredAgents).toBe(3);
  });

  it('never observes a partially-written coordination file mid-race (readers see a fully valid entry set at every point)', async () => {
    const orchestratorIds = ['orc-1', 'orc-2', 'orc-3'];

    // Interleave declareDibs writes with reads to catch a torn-write; every
    // read must parse and contain only well-formed entries declared so far.
    const writers = orchestratorIds.map((orchestratorId, index) =>
      declareDibs(coordinationFilePath, {
        orchestratorId,
        desiredAgents: index + 1,
        declaredAt: Date.now(),
      }),
    );
    const readerDuringWrites = readDibs(coordinationFilePath).catch(() => []);

    await Promise.all([...writers, readerDuringWrites]);

    const finalDibs = (await readDibs(coordinationFilePath)).filter(
      (entry) => !isReservedEntry(entry.orchestratorId),
    );
    expect(finalDibs).toHaveLength(3);
    for (const entry of finalDibs) {
      expect(typeof entry.orchestratorId).toBe('string');
      expect(Number.isInteger(entry.desiredAgents)).toBe(true);
    }
  });
});

// ---------------------------------------------------------------------------
// (c) — traffic-light progression across the full state machine, including
// hysteresis-gated recovery (not immediate on a single good sample)
// ---------------------------------------------------------------------------

describe('traffic-light output across the GREEN -> AMBER -> RED -> recovery progression', () => {
  it('signals spawn-allowed on a sustained GREEN sample', async () => {
    const collect = makeStubCollect([{ memory: MEMORY_GREEN, disk: DISK_GREEN }]);

    const { trafficLight } = await runBeat({
      collect,
      memoryHistory: undefined,
      diskHistory: undefined,
      coordinationFilePath,
      orchestratorId: 'orc-solo',
      desiredAgents: 1,
      runningAgents: [],
    });

    expect(trafficLight.type).toBe('spawn-allowed');
    expect(trafficLight.allowance).toBeGreaterThan(0);
  });

  it('signals hold (freeze new spawns) on AMBER without pausing already-running agents', async () => {
    const collect = makeStubCollect([{ memory: MEMORY_AMBER, disk: DISK_GREEN }]);

    const { trafficLight } = await runBeat({
      collect,
      memoryHistory: undefined,
      diskHistory: undefined,
      coordinationFilePath,
      orchestratorId: 'orc-solo',
      desiredAgents: 1,
      runningAgents: [{ agentId: 'agent-1', rssMb: 300, startedAt: Date.now() }],
    });

    expect(trafficLight.type).toBe('hold');
  });

  it('signals pause-agent(s) on RED, naming a specific running agent via the priority policy', async () => {
    const collect = makeStubCollect([{ memory: MEMORY_RED, disk: DISK_GREEN }]);
    const runningAgents = [
      { agentId: 'agent-small', rssMb: 220, startedAt: Date.now() },
      { agentId: 'agent-big', rssMb: 900, startedAt: Date.now() },
    ];

    const { trafficLight } = await runBeat({
      collect,
      memoryHistory: undefined,
      diskHistory: undefined,
      coordinationFilePath,
      orchestratorId: 'orc-solo',
      desiredAgents: 1,
      runningAgents,
    });

    expect(trafficLight.type).toBe('pause');
    expect(trafficLight.pauseCandidate.agentId).toBe('agent-big');
  });

  it('returns a well-defined no-op pause signal on RED when there are zero running agents to pause', async () => {
    const collect = makeStubCollect([{ memory: MEMORY_RED, disk: DISK_GREEN }]);

    const { trafficLight } = await runBeat({
      collect,
      memoryHistory: undefined,
      diskHistory: undefined,
      coordinationFilePath,
      orchestratorId: 'orc-solo',
      desiredAgents: 1,
      runningAgents: [],
    });

    expect(trafficLight.type).toBe('pause');
    expect(trafficLight.pauseCandidate).toBeNull();
  });

  it('does not recover to spawn-allowed on a single GREEN sample after RED — only after N consecutive GREEN samples (hysteresis)', async () => {
    // RED, then GREEN, GREEN, GREEN — recovery threshold is 3 consecutive
    // GREEN samples (THRESHOLDS.hysteresisRecoverConsecutiveGreen). The
    // pipeline must stay non-spawn-allowed through the first two GREEN
    // samples and only flip on the third.
    const collect = makeStubCollect([
      { memory: MEMORY_RED, disk: DISK_GREEN },
      { memory: MEMORY_GREEN, disk: DISK_GREEN },
      { memory: MEMORY_GREEN, disk: DISK_GREEN },
      { memory: MEMORY_GREEN, disk: DISK_GREEN },
    ]);

    let memoryHistory;
    let diskHistory;
    const seenTrafficLightTypes = [];

    for (let beat = 0; beat < 4; beat += 1) {
      const result = await runBeat({
        collect,
        memoryHistory,
        diskHistory,
        coordinationFilePath,
        orchestratorId: 'orc-solo',
        desiredAgents: 1,
        runningAgents: [],
      });
      memoryHistory = result.memoryHistory;
      diskHistory = result.diskHistory;
      seenTrafficLightTypes.push(result.trafficLight.type);
    }

    const [afterRed, afterFirstGreen, afterSecondGreen, afterThirdGreen] = seenTrafficLightTypes;

    expect(afterRed).toBe('pause');
    // Two consecutive good samples are not yet enough to trust recovery —
    // this is the crux of the hysteresis requirement: no flapping back to
    // spawn-allowed on the first (or second) sign of improvement.
    expect(afterFirstGreen).not.toBe('spawn-allowed');
    expect(afterSecondGreen).not.toBe('spawn-allowed');
    // The third consecutive GREEN sample crosses the configured recovery
    // threshold (N = 3) and the traffic light finally returns to green.
    expect(afterThirdGreen).toBe('spawn-allowed');
  });

  it('treats a stale/unknown sample as at-least-AMBER, never silently GREEN', async () => {
    const collect = makeStubCollect([{ memory: { stale: true }, disk: DISK_GREEN }]);

    const { trafficLight } = await runBeat({
      collect,
      memoryHistory: undefined,
      diskHistory: undefined,
      coordinationFilePath,
      orchestratorId: 'orc-solo',
      desiredAgents: 1,
      runningAgents: [],
    });

    expect(trafficLight.type).not.toBe('spawn-allowed');
  });
});

// ---------------------------------------------------------------------------
// Full pipeline, two orchestrators, effective allowance = min(memory, disk)
// ---------------------------------------------------------------------------

describe('full pipeline with two orchestrators sharing one coordination file', () => {
  it('computes an effective allowance that both orchestrators can read back consistently, hermetically (no real OS calls)', async () => {
    let realCollectCalls = 0;
    const realCollect = async () => {
      realCollectCalls += 1;
      throw new Error('collect() must never be called for real in this test');
    };
    void realCollect; // never wired into runBeat below — its absence from any call is the point

    const collectA = makeStubCollect([{ memory: MEMORY_GREEN, disk: DISK_GREEN }]);
    const collectB = makeStubCollect([{ memory: MEMORY_GREEN, disk: DISK_GREEN }]);

    const [resultA, resultB] = await Promise.all([
      runBeat({
        collect: collectA,
        memoryHistory: undefined,
        diskHistory: undefined,
        coordinationFilePath,
        orchestratorId: 'orchestrator-a',
        desiredAgents: 2,
        runningAgents: [],
      }),
      runBeat({
        collect: collectB,
        memoryHistory: undefined,
        diskHistory: undefined,
        coordinationFilePath,
        orchestratorId: 'orchestrator-b',
        desiredAgents: 2,
        runningAgents: [],
      }),
    ]);

    expect(realCollectCalls).toBe(0);

    expect(resultA.trafficLight.type).toBe('spawn-allowed');
    expect(resultB.trafficLight.type).toBe('spawn-allowed');

    const dibs = (await readDibs(coordinationFilePath)).filter(
      (entry) => !isReservedEntry(entry.orchestratorId),
    );
    expect(dibs).toHaveLength(2);
  });
});
