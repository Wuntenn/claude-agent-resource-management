// Phase 0 outer acceptance test ("agent-resource-management:
// per-agent footprint growth-rate ('leak velocity') circuit breaker").
//
// Outer acceptance test (detection echo):
//
//   it("a --desired-agents/--heartbeat beat emits a 'leak-block' literal
//   for a pid whose persisted footprint trajectory shows sustained
//   above-threshold growth, WITHOUT altering the admission decision
//   ('spawn-allowed'/'pause'/etc.) for any other agent")
//
//   Layer: outer acceptance, CLI-level — black-box via `spawnSync` of the
//   real `cli.mjs` (mirrors every sibling `cli.*.jest.spec.mjs` in this
//   skill; `cli.mjs` itself is coverage-exempt and only ever black-box
//   tested via subprocess, per this skill's own established convention —
//   see e.g. ./cli-evict.jest.spec.mjs's and
//   ./cli-memory-projection-wiring.jest.spec.mjs's own "never imports
//   cli.mjs directly" headers).
//
// RED by construction, right now: `cli.mjs` has no concept of "leak
// velocity" or a `leak-block` literal anywhere today (confirmed: `grep -n
// "leak" cli.mjs` matches nothing). A normal `--desired-agents` beat's JSON
// body carries no field at all reflecting a per-pid leak-velocity verdict —
// the assertion below that such a field exists (and names the leaking pid)
// fails because the field is simply `undefined`, not because of a crash.
//
// Do not add a stub implementation to make this pass — that defeats the
// point of the outer/phase red-green loop. This ticket's own build-plan
// phases turn this file green by actually building the mechanism.
//
// ---------------------------------------------------------------------------
// DESIGN GUESSES THIS FILE BAKES IN — not binding on the eventual
// implementer; update this file's env plumbing/assertions to match whatever
// concrete shape actually ships (mirrors every other outer-acceptance file
// in this skill's own explicit invitation to amend guessed shapes later —
// see e.g. ./watchdog-outer-acceptance.jest.spec.mjs's header).
//
// 1. DETECTION SOURCE — the ALREADY-SHIPPED, persisted per-pid footprint
//    trajectory store `lib/footprint-state.mjs` (`ARM_FOOTPRINT_STATE_FILE`,
//    populated by real `--poll-footprint` beats in production; seeded here
//    directly via `recordFootprintPoll` — a lib-level primitive, not
//    `cli.mjs` itself, exactly like ./cli-evict.jest.spec.mjs's own
//    `declareDibs`/`bindPidToLease` direct-import precedent for seeding
//    state one layer below a spawned beat). A future `--desired-agents`/
//    `--heartbeat` beat is expected to scan this store (independently of,
//    and in addition to, whatever candidate pid THIS beat's own
//    `--orchestrator-id` is requesting admission for) for any pid whose
//    trajectory shows sustained leak velocity.
//
// 2. OBSERVABLE OUTPUT SHAPE — a new top-level array field on the beat's
//    ordinary JSON body, `leakBlocks`, additive metadata alongside (never
//    replacing) the beat's own admission `type`
//    (`spawn-allowed`/`pause`/`hold`/etc.):
//
//      { "type": "spawn-allowed", ..., "leakBlocks": [
//          { "pid": <number>, "type": "leak-block", "reason": <string> }
//        ] }
//
//    This is the crux of the "does NOT halt" requirement distinguishing
//    `leak-block` from the existing, HOST-WIDE, beat-halting
//    `pressure-block` (cli.mjs's pre-`reserveAdmission` short-circuit,
//    ./cli-memory-projection-wiring.jest.spec.mjs test 3): `leak-block` is
//    PER-PID information riding alongside a normal admission verdict, not a
//    verdict of its own. A pid with no leak-velocity concern at all yields
//    `leakBlocks: []`.
//
// SAFETY — no `--poll-footprint`/`--evict-pid` invocation in this file ever
// touches a real PID; every pid below is synthetic, and no eviction is
// attempted at all (this file only proves the DETECTION ECHO, not the
// recycle path — see ./leak-velocity-outer-acceptance.jest.spec.mjs for
// that).
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { recordFootprintPoll } from './lib/footprint-state.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

// Sustained, uninterrupted growth across every consecutive poll — mirrors
// ./leak-velocity-outer-acceptance.jest.spec.mjs's own LEAK_TRAJECTORY.
const LEAK_SAMPLES_MB = [200, 260, 330, 410, 500];
// Genuine growth, but interrupted (a shrink between polls 2 and 3) — must
// never be judged as sustained leak velocity.
const NORMAL_SAMPLES_MB = [200, 260, 210, 400, 220];

/** Runs the real cli.mjs as a real child process — never imports it directly (matches every sibling cli.*.jest.spec.mjs). */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

let workDir;
let coordinationFilePath;
let footprintStateFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-leak-block-'));
  coordinationFilePath = join(workDir, 'coordination.json');
  footprintStateFilePath = join(workDir, 'footprint-state.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

function baseEnv(extra = {}) {
  return {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FOOTPRINT_STATE_FILE: footprintStateFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    ...extra,
  };
}

/** Seeds `pid`'s persisted footprint trajectory directly via the real, already-shipped lib primitive — one poll per sample, `now` advancing 10s per poll (a plausible real --poll-footprint cadence). */
async function seedFootprintTrajectory(pid, samplesMb) {
  const baseNow = Date.now() - samplesMb.length * 10_000;
  for (const [index, sampleMb] of samplesMb.entries()) {
    // eslint-disable-next-line no-await-in-loop -- must fold in order; each poll depends on the prior poll's persisted state.
    await recordFootprintPoll(footprintStateFilePath, pid, sampleMb, baseNow + index * 10_000, {
      agentClass: 'typescript-implementer',
    });
  }
}

it(
  "a --desired-agents beat emits a 'leak-block' literal naming a pid whose persisted footprint trajectory shows " +
    'sustained above-threshold growth, without altering this beat\'s own admission decision for the orchestrator ' +
    'actually requesting capacity, and without flagging a pid whose growth is merely normal/interrupted',
  async () => {
    const LEAK_PID = 9201;
    const NORMAL_PID = 9202;

    await seedFootprintTrajectory(LEAK_PID, LEAK_SAMPLES_MB);
    await seedFootprintTrajectory(NORMAL_PID, NORMAL_SAMPLES_MB);

    // A completely different orchestrator, requesting ordinary admission —
    // proves the leak-velocity detection above (for LEAK_PID/NORMAL_PID,
    // neither of which this orchestrator owns or is asking about) never
    // interferes with an unrelated beat's own admission verdict.
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-leak-block-requester', '--desired-agents=1'],
      baseEnv(),
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const body = JSON.parse(stdout);

    // The admission decision itself is completely unaffected by the
    // leak-velocity detection below — 'leak-block' is additive per-pid
    // metadata, never a beat-halting verdict like the existing, host-wide
    // 'pressure-block' (see this file's header, design guess 2).
    expect(body.type).toBe('spawn-allowed');
    expect(Number(body.liveAgentGrant)).toBeGreaterThan(0);

    // FAILS TODAY: `leakBlocks` is simply undefined on every beat's JSON
    // body — no code anywhere populates it.
    expect(Array.isArray(body.leakBlocks)).toBe(true);

    const leakEntry = body.leakBlocks.find((entry) => entry.pid === LEAK_PID);
    expect(leakEntry).toBeDefined();
    expect(leakEntry.type).toBe('leak-block');

    // The normal/interrupted-growth pid must never appear in leakBlocks at
    // all — a genuine two-sided branch, not a "flag everything polled" stub.
    const normalEntry = body.leakBlocks.find((entry) => entry.pid === NORMAL_PID);
    expect(normalEntry).toBeUndefined();
  },
);

it(
  'never alters a --heartbeat beat\'s own admission-adjacent output for an unrelated orchestrator, even while a ' +
    'leaking pid is echoed via leak-block',
  async () => {
    const LEAK_PID = 9203;
    await seedFootprintTrajectory(LEAK_PID, LEAK_SAMPLES_MB);

    const orchestratorId = 'orch-leak-block-heartbeat';

    // Prime the orchestrator's own dibs entry first, exactly like every
    // other --heartbeat test in ./cli.jest.spec.mjs does.
    const priming = runCli(['--orchestrator-id=' + orchestratorId, '--desired-agents=1'], baseEnv());
    expect(priming.status).toBe(0);
    const primingBody = JSON.parse(priming.stdout);
    expect(primingBody.type).toBe('spawn-allowed');

    const heartbeat = runCli(['--orchestrator-id=' + orchestratorId, '--heartbeat'], baseEnv());
    expect(heartbeat.status).toBe(0);
    const heartbeatBody = JSON.parse(heartbeat.stdout);

    // A heartbeat's own light-classification output is untouched by the
    // leak-velocity echo — same admission-adjacent shape a heartbeat always
    // produces today, regardless of what leakBlocks reports.
    expect(heartbeatBody.type).not.toBe('leak-block');

    // FAILS TODAY: undefined, same reason as the first test in this file.
    expect(Array.isArray(heartbeatBody.leakBlocks)).toBe(true);
    const leakEntry = heartbeatBody.leakBlocks.find((entry) => entry.pid === LEAK_PID);
    expect(leakEntry).toBeDefined();
    expect(leakEntry.type).toBe('leak-block');
  },
);

// ---------------------------------------------------------------------------
// pre-PR review, Medium 1 — `computeLeakBlocksForBeat` must skip a
// footprint-state entry whose `lastPolledAt` predates this beat's own `now`
// by more than the reused `DEFAULT_LIVENESS_THRESHOLD_MS` staleness window
// (15 minutes) — a long-dead/pid-reused process must never keep surfacing a
// `leak-block` echo indefinitely.
// ---------------------------------------------------------------------------

it('excludes a stale footprint-state entry (last polled beyond the staleness window) from leakBlocks', async () => {
  const STALE_LEAK_PID = 9204;
  await seedFootprintTrajectory(STALE_LEAK_PID, LEAK_SAMPLES_MB);

  // Advance this beat's own `now` well past DEFAULT_LIVENESS_THRESHOLD_MS
  // (15 minutes) beyond the seeded trajectory's own `lastPolledAt` (which
  // `seedFootprintTrajectory` anchors at roughly `Date.now()`).
  const farFutureNowMs = Date.now() + 20 * 60 * 1000;

  const { status, stdout, stderr } = runCli(
    ['--orchestrator-id=orch-leak-block-stale', '--desired-agents=1'],
    baseEnv({
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_NOW_MS: String(farFutureNowMs),
    }),
  );

  expect(status).toBe(0);
  expect(stderr).toBe('');
  const body = JSON.parse(stdout);

  expect(Array.isArray(body.leakBlocks)).toBe(true);
  const staleEntry = body.leakBlocks.find((entry) => entry.pid === STALE_LEAK_PID);
  expect(staleEntry).toBeUndefined();
});
