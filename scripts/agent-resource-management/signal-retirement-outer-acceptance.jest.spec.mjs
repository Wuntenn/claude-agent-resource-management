// Phase 0 — outer acceptance test for signal retirement.
//
// This is the RED outer test for the whole ticket: retiring three
// parsed-but-unconsumed signals in scripts/agent-resource-management/.
// It goes green only once Phases 1-3 in the Build Plan land; it is
// intentionally NOT skipped and NOT marked `.todo`.
//
// Scenarios (mirror the Build Plan's three phases exactly):
//   1. `parseResourceCapacity` (lib/recon.mjs:248-277) is entirely dead —
//      all 3 returned fields (coreCount/totalRamMb/totalDiskGb) are
//      unconsumed anywhere except its own test block. Phase 1 deletes the
//      function (and its test block + CAPACITY_RAW fixture in
//      lib/recon.jest.spec.mjs). RED today: the function IS still exported.
//   2. `compressedMb` reaches `realCollect()`'s returned `memory` object in
//      cli.mjs (~line 1076) but is never read downstream. Phase 2 wires it
//      into the final printed traffic-light JSON as a record-only
//      observability field, mirroring how `loadAverage` was added
//      record-only (cli.mjs's final
//      `process.stdout.write(JSON.stringify({ ...trafficLight, diskTrend,
//      liveAgentGrant, loadAverage }) + '\n')` call). RED today: the printed
//      JSON has no `compressedMb` key. Driven the same way the established
//      outer-acceptance specs in this directory do (see
//      ./outer-acceptance.jest.spec.mjs's and ./load-average.jest.spec.mjs's
//      `runCli`/`ARM_FAKE_COLLECT_JSON`/exact-shape-`toEqual` idiom).
//   3. A new Jest-based fitness-function spec exists under
//      scripts/agent-resource-management/ that flags any named export in
//      lib/**/*.mjs whose only reference outside its own file is a
//      *.jest.spec.mjs file (any such file, not just a same-named one).
//      Phase 3 adds that file with fixture-based self-tests, returning only
//      the known, tracked pre-existing exceptions (KNOWN_PRE_EXISTING_EXCEPTIONS,
//) against the real lib/**/*.mjs after Phase 1's cleanup — not an
//      unconditional zero, since fixing the checker's own comment-stripping
//      blind spot (a review finding) surfaced 4 pre-existing dead
//      exports unrelated to this ticket's three original signals. RED
//      today: no such file exists yet.
//
// This file goes green only once Phases 1-3 all land; do not add a stub/
// rewording to make any one assertion pass in isolation — each is pinned to
// the real production behaviour the corresponding phase must produce.

import { spawnSync } from 'node:child_process';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const HERE = dirname(fileURLToPath(import.meta.url));
const CLI = join(HERE, 'cli.mjs');
const RECON_PATH = join(HERE, 'lib/recon.mjs');

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

let workDir;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-signal-retirement-outer-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

describe('outer acceptance — retire three parsed-but-unconsumed signals', () => {
  test('1. parseResourceCapacity is no longer exported from lib/recon.mjs (Phase 1: entirely-dead function deleted)', async () => {
    // Import the real module by URL (not a mock) — this must fail to find
    // the export once Phase 1 deletes it. RED today: the function IS still
    // exported, so `parseResourceCapacity` resolves to a function, not
    // `undefined`.
    const reconModule = await import(RECON_PATH);

    expect(reconModule.parseResourceCapacity).toBeUndefined();
  });

  test('2. the final printed traffic-light JSON includes compressedMb as a record-only observability field (Phase 2: mirrors loadAverage)', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-1269-a', '--desired-agents=1'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        // Deterministic so this exact-shape assertion below isn't at the
        // mercy of this host's real, constantly-shifting os.loadavg()
        // reading — mirrors cli.jest.spec.mjs's own exact-shape tests.
        ARM_FAKE_LOAD_AVERAGE_JSON: JSON.stringify([0, 0, 0]),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');

    const parsed = JSON.parse(stdout);

    // RED today: cli.mjs's final `process.stdout.write` call only spreads
    // `{ ...trafficLight, diskTrend, liveAgentGrant, loadAverage }` —
    // `compressedMb` is parsed into `realCollect()`'s `memory` object
    // (cli.mjs ~line 1076) but never threaded onto this printed JSON, so
    // `parsed.compressedMb` is `undefined` until Phase 2 lands.
    expect(parsed).toHaveProperty('compressedMb');
    // The fixture fed a distinctive, non-default value (512) via
    // ARM_FAKE_COLLECT_JSON's memory.compressedMb above — proves the wiring
    // actually threads the REAL collected value through, not a hardcoded
    // stand-in.
    expect(parsed.compressedMb).toBe(512);
  });

  test('3. a fitness-function spec exists under scripts/agent-resource-management/ that flags dead lib exports only referenced from *.jest.spec.mjs files, and its self-tests pass against the real lib/**/*.mjs after Phase 1 (Phase 3)', async () => {
    const entries = await readdir(HERE);
    const fitnessSpecCandidates = entries.filter(
      (name) =>
        name.endsWith('.jest.spec.mjs') &&
        /dead-export|unused-export|fitness/i.test(name),
    );

    // RED today: no such fitness-function spec file exists yet under
    // scripts/agent-resource-management/ — Phase 3 must add one (name left
    // to the implementer's judgment; this test looks for a plausible
    // naming convention rather than pinning one exact filename, since the
    // Build Plan itself leaves the filename open).
    expect(fitnessSpecCandidates.length).toBeGreaterThan(0);
  });
});
