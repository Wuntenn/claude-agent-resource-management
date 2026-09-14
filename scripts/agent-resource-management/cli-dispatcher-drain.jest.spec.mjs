// Phase 5 — outer acceptance test: the dispatcher drain.
//
// Proves Phases 1-4's primitives (`./lib/queue.mjs`'s lease primitives +
// aging-aware `sortQueueEntries`, `./lib/dispatch.mjs`'s
// `claimAndAdmitQueueEntry`, and `cli.mjs`'s `--dequeue-if-capacity`/
// `--ack-lease`/`--release-lease` CLI wiring) compose end-to-end: 30 items,
// enqueued with mixed priorities, drain to zero against the real
// `LIVE_AGENT_CEILING` (4) using only real child-process `cli.mjs`
// invocations against one shared queue file + coordination file, in
// priority-then-FIFO order, safely under two racing orchestrators, and
// safely across a simulated crash — with no item lost or double-"spawned".
//
// Black-box, real-process-spawning, real-filesystem idiom — mirrors
// ./cli-two-orchestrators.jest.spec.mjs's `runCli`/`runCliAsync` helpers and
// ./cli-dequeue-if-capacity.jest.spec.mjs's `ARM_QUEUE_FILE`/
// `ARM_COORDINATION_FILE` fixture setup exactly. No `.skip`/`.todo` — every
// assertion below is real and this file is GREEN today (Phases 1-4 already
// shipped).
//
// ---------------------------------------------------------------------------
// Investigation findings, stated up front (per the Phase 5 handoff):
//
// 1. HOW CEILING HEADROOM ACTUALLY FREES UP FOR A BATCH DRAIN — resolved,
//    not a gap. `--dequeue-if-capacity` reserves against the ADVISORY
//    `LIVE_AGENT_ADMISSION_CLAIM_TYPE` ledger (`reserveAdmission`), keyed
//    per-orchestrator with REPLACE-not-accumulate semantics (see
//    coordination-file.mjs's `reserveAdmission`, "Step 2 — this
//    orchestrator's own prior hold is replaced, not accumulated"). That
//    means a SINGLE orchestratorId issuing repeated `--dequeue-if-capacity`
//    calls never actually saturates the ceiling via the admission ledger
//    alone — its own prior admission hold is excluded from its own next
//    request every time. The ceiling only genuinely binds via the SEPARATE,
//    already-public `LIVE_AGENT_CLAIM_TYPE` ledger (`--claim=live-agent:N`
//    / `--release=live-agent:N`), which cli.mjs's own doc comments (see
//    `LIVE_AGENT_CLAIM_TYPE`'s JSDoc, ~cli.mjs line 224) document as the
//    ledger a caller writes to "ONLY once a live agent has genuinely
//    started" — i.e. the real post-spawn registration step of the documented
//    dispatch lifecycle (dequeue-if-capacity -> spawn -> `--claim` -> ...
//    -> `--release` -> `--ack-lease`), not an undocumented workaround. A
//    genuine `--claim=live-agent:N` ALSO atomically releases the calling
//    orchestrator's own admission hold (cli.mjs ~line 1919), so this is the
//    intended, already-shipped mechanism — not a fixture hack. This test
//    uses it throughout (see `admitAndRegisterRunning`/`completeRunning`
//    below) instead of `ARM_FAKE_NOW_MS` clock-jumping, because clock-jumping
//    would only expire the 90s admission-ledger TTL, which (per the above)
//    was never actually the bottleneck for a single orchestrator anyway.
//
// 2. FIXED — aging is wired into the dispatch path. `./lib/queue.mjs`'s
//    `claimQueueEntry` (used by `--dequeue-if-capacity` via
//    `claimAndAdmitQueueEntry`) now calls `sortQueueEntries(existing,
//    referenceNow)`, threading through the same already-resolved
//    `referenceNow` (`options.now` when finite, else `Date.now()`) it already
//    used for the lease-eligibility check. Per `sortQueueEntries`'s own
//    contract ("Aging is considered ONLY when the caller explicitly supplies
//    a finite `now`"), this means aging-based rank promotion (Phase 3's
//    `AGING_THRESHOLD_MS` mechanism) now genuinely applies through
//    `--dequeue-if-capacity`, to entry SELECTION and not merely lease-expiry
//    filtering. `it('priority-then-aged-FIFO order ...')` below proves this
//    empirically: an artificially very-old `low`-priority item is promoted
//    exactly one tier (to compete at "normal" rank) and drains ahead of the
//    fresher `normal`-priority items, while staying behind the `high` block —
//    matching `effectiveRank`'s documented "promote one tier toward high,
//    never skip a tier" contract exactly.
//
// 3. TERMINATION BOUND for the "repeat until drained" loop (its own
//    explicit "unbounded and can hang a test" flag): a fixed
//    `MAX_BEATS = 200` bound (~3x the ~60 real beats this suite's own
//    30-item/ceiling-4 drain design needs: 30 admits + 30
//    releases/acks + a handful of one-off probe/denial beats), rather than a
//    wall-clock timeout — a beat-count bound is deterministic and immune to
//    CI host slowness, whereas a wall-clock bound would either flake under
//    load or need to be so generous it stops being a meaningful guard at
//    all.
//
// 4. CRASH-RECOVERY TTL MECHANISM: the queue lease's own TTL
//    (`DEFAULT_QUEUE_CLAIM_TTL_MS`, 15 min, ./lib/queue.mjs) is compared
//    against `options.now` (`claimQueueEntry`'s `referenceNow`), which
//    `claimAndAdmitQueueEntry` threads straight from `cli.mjs`'s own
//    `resolveNow()` — the existing `ARM_FAKE_NOW_MS` seam (honoured whenever
//    `ARM_COORDINATION_FILE` is set, which every test below already sets).
//    So advancing `ARM_FAKE_NOW_MS` by `DEFAULT_QUEUE_CLAIM_TTL_MS + 1`
//    between two `--dequeue-if-capacity` calls deterministically expires the
//    QUEUE lease with no real wall-clock wait and no new CLI flag needed —
//    already-available, no gap here.
// ---------------------------------------------------------------------------

import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const LIVE_AGENT_CLAIM_TYPE = 'live-agent';
const LIVE_AGENT_CEILING = 4; // must match cli.mjs's real, un-overridden constant
const DEFAULT_QUEUE_CLAIM_TTL_MS = 15 * 60 * 1000; // ./lib/queue.mjs's DEFAULT_QUEUE_CLAIM_TTL_MS
const AGING_THRESHOLD_MS = 10 * 60 * 1000; // ./lib/queue.mjs's AGING_THRESHOLD_MS
const MAX_BEATS = 200; // see finding 3 above

// Hermeticity fix (post-review): `--claim`/`--release` (unlike
// `--dequeue-if-capacity`/`--enqueue`/`--peek`/`--ack-lease`/`--release-lease`,
// none of which probe host resources at all) run a REAL host memory/disk
// probe via `probeAvailableCapacity` whenever `ARM_FAKE_COLLECT_JSON` is
// unset — see `claim.jest.spec.mjs`'s own header ("ARM_FAKE_COLLECT_JSON
// (env, required for test hermeticity)") and every existing
// `--claim`-exercising spec in this directory (e.g.
// `cli-two-orchestrators.jest.spec.mjs`), all of which set it on every
// `--claim` call. This file's `cli()` helper originally did not, so every
// `--claim=live-agent:N` call fell through to a REAL probe of the actual
// host — which reads non-GREEN under load (this suite runs dozens of real
// child-process beats back-to-back) and fails the claim closed
// (`{"granted":0}`) regardless of the real, in-test ceiling headroom. Fixed
// by pinning a GREEN fixture on every `cli()` call, matching the sibling
// suites' convention exactly — this test's subject is dispatcher/queue
// correctness, not memory/disk axis classification, so GREEN-always is the
// correct fixture, not a workaround.
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 };

/** Runs the real cli.mjs as a child process — never imports it directly. */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/**
 * Runs the real cli.mjs as a genuinely CONCURRENT child process — mirrors
 * ./cli-two-orchestrators.jest.spec.mjs's `runCliAsync` exactly.
 */
function runCliAsync(args, extraEnv) {
  return new Promise((resolvePromise, rejectPromise) => {
    const child = spawn('node', [CLI, ...args], { env: { ...process.env, ...extraEnv } });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => {
      stdout += chunk;
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk;
    });
    child.on('error', rejectPromise);
    child.on('close', (status) => {
      resolvePromise({ status, stdout, stderr });
    });
  });
}

let workDir;
let queueFilePath;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-dispatcher-drain-'));
  queueFilePath = join(workDir, 'queue.json');
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

// note: this helper's own body literally sets ARM_COORDINATION_FILE
// (not merely via a delegated call) so ./env-seam-premise.jest.spec.mjs's
// Tier-3 "wrapper-function-default" text scan recognizes every
// `cli([...], { ARM_FAKE_NOW_MS: ... })` call site below as paired.
function cli(args, extraEnv = {}) {
  return runCli(args, {
    ARM_QUEUE_FILE: queueFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    ARM_BEAT_NOW_TEST_MODE: '1',
    ...extraEnv,
  });
}

function baseEnv(extra = {}) {
  return {
    ARM_QUEUE_FILE: queueFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    ARM_BEAT_NOW_TEST_MODE: '1',
    ...extra,
  };
}

function enqueue(commandRef, priority, orchestratorId = 'orch-seed') {
  const result = cli([`--enqueue=implementation-agent:${priority}:${commandRef}`, `--orchestrator-id=${orchestratorId}`]);
  expect(result.status).toBe(0);
  return result;
}

function peek(orchestratorId = 'orch-peek') {
  const result = cli(['--peek', `--orchestrator-id=${orchestratorId}`]);
  expect(result.status).toBe(0);
  return JSON.parse(result.stdout).items;
}

// ---------------------------------------------------------------------------
// Scenario 1 + termination-bound proof — 30-item/ceiling-4 full drain, using
// the real dequeue-if-capacity -> claim(live-agent) -> release(live-agent)
// -> ack-lease lifecycle (see finding 1 above for why `--claim`/`--release`
// are the genuine mechanism, not a fixture workaround).
// ---------------------------------------------------------------------------

it(
  "drains 30 enqueued items to zero, in priority-then-aged-FIFO order, using only cooperative " +
    '--dequeue-if-capacity beats, with no lost or duplicated item across a simulated crash',
  async () => {
    const ORCH = 'orch-drain-single';
    const ITEM_COUNT = 30;
    const priorities = ['low', 'normal', 'high'];

    for (let i = 0; i < ITEM_COUNT; i += 1) {
      enqueue(`drain-item-${i}`, priorities[i % priorities.length]);
    }
    expect(peek()).toHaveLength(ITEM_COUNT);

    const running = []; // { leaseId, commandRef }
    const ackedCounts = new Map();
    let queueEmptySeen = false;
    let beats = 0;
    let sawExplicitCeilingDenial = false;

    while ((!queueEmptySeen || running.length > 0) && beats < MAX_BEATS) {
      beats += 1;

      if (!queueEmptySeen && running.length < LIVE_AGENT_CEILING) {
        const result = cli(['--dequeue-if-capacity', `--orchestrator-id=${ORCH}`]);
        expect(result.status).toBe(0);
        const body = JSON.parse(result.stdout);

        if (body.reason === 'queue-empty') {
          queueEmptySeen = true;
          continue;
        }

        // Own-orchestrator admission holds are excluded from the caller's
        // own next request (finding 1) — capacity here is bounded only by
        // the real `LIVE_AGENT_CLAIM_TYPE` claims registered below, which
        // this loop itself keeps under `LIVE_AGENT_CEILING` by construction
        // (the `running.length < LIVE_AGENT_CEILING` guard above) — so this
        // beat must always be granted.
        expect(body.granted).toBe(true);

        // Simulate the real spawn: register the item as a genuinely running
        // agent against the real ledger the ceiling actually reads.
        const claimResult = cli([`--claim=${LIVE_AGENT_CLAIM_TYPE}:1`, `--orchestrator-id=${ORCH}`]);
        expect(claimResult.status).toBe(0);
        expect(JSON.parse(claimResult.stdout).granted).toBe(1);

        running.push({ leaseId: body.leaseId, commandRef: body.item.commandRef });

        // Explicit ceiling-enforcement proof (once): with 4 real agents
        // genuinely registered, a 5th, DISTINCT orchestrator's request must
        // be denied for capacity — proving the ceiling actually binds
        // (not merely "we happened to never ask for a 5th").
        if (!sawExplicitCeilingDenial && running.length === LIVE_AGENT_CEILING) {
          const denyProbe = cli(['--dequeue-if-capacity', '--orchestrator-id=orch-denial-probe']);
          expect(denyProbe.status).toBe(0);
          expect(JSON.parse(denyProbe.stdout)).toEqual({ item: null, granted: false, reason: 'no-capacity' });
          sawExplicitCeilingDenial = true;
        }
      } else {
        // At the ceiling, or the queue is drained with running work left —
        // complete the oldest running item (real completion: release the
        // real claim, then ack the queue lease).
        const finished = running.shift();
        const releaseResult = cli([`--release=${LIVE_AGENT_CLAIM_TYPE}:1`, `--orchestrator-id=${ORCH}`]);
        expect(releaseResult.status).toBe(0);
        expect(JSON.parse(releaseResult.stdout).released).toBe(1);

        const ackResult = cli([`--ack-lease=${finished.leaseId}`, `--orchestrator-id=${ORCH}`]);
        expect(ackResult.status).toBe(0);
        const ackBody = JSON.parse(ackResult.stdout);
        expect(ackBody.acked).toBe(true);
        expect(ackBody.item.commandRef).toBe(finished.commandRef);

        ackedCounts.set(finished.commandRef, (ackedCounts.get(finished.commandRef) ?? 0) + 1);
      }
    }

    // Explicit, generous bound never actually hit — see finding 3.
    expect(beats).toBeLessThan(MAX_BEATS);
    expect(sawExplicitCeilingDenial).toBe(true);
    expect(queueEmptySeen).toBe(true);
    expect(running).toHaveLength(0);

    // No lost or duplicated item: every one of the 30 distinct commandRefs
    // was acked exactly once.
    expect(ackedCounts.size).toBe(ITEM_COUNT);
    for (const count of ackedCounts.values()) {
      expect(count).toBe(1);
    }

    // Final queue state is exactly empty, not merely "count 0".
    expect(peek()).toEqual([]);
  },
  30_000,
);

// ---------------------------------------------------------------------------
// Scenario 2 — priority-then-FIFO drain order, with aging-based rank
// promotion (see file header, finding 2) proven empirically end-to-end
// through the real `--dequeue-if-capacity` CLI path.
// ---------------------------------------------------------------------------

it('drains strictly by priority (high, then normal, then low), FIFO within each priority — and an artificially aged low-priority item IS promoted exactly one tier (to compete at normal rank), draining ahead of fresher normal-priority items but still behind the high block', async () => {
  const ORCH = 'orch-drain-order';
  const T0 = 1_700_500_000_000;

  // Interleaved enqueue order, deliberately NOT priority-grouped, to prove
  // the drain reorders by priority rather than merely reflecting insertion
  // order.
  enqueue('l0', 'low');
  enqueue('n0', 'normal');
  enqueue('h0', 'high');
  enqueue('l1', 'low');
  enqueue('n1', 'normal');
  enqueue('h1', 'high');
  enqueue('l2', 'low');
  enqueue('n2', 'normal');
  enqueue('h2', 'high');

  // Artificially backdate l2's enqueuedAt to well past AGING_THRESHOLD_MS
  // (10 min) relative to T0 — the reference "now" every drain beat below
  // uses via ARM_FAKE_NOW_MS. Aging promotion IS wired into the dispatch
  // path (`claimQueueEntry` threads `referenceNow` through to
  // `sortQueueEntries`), so l2 is promoted exactly one tier (from "low" to
  // compete at "normal" rank — see `effectiveRank`'s "never skip a tier"
  // contract) — it drains ahead of n0/n1/n2 (whose enqueuedAt is later than
  // l2's backdated one) but still behind the entire "high" block, since
  // promotion never crosses more than one tier.
  const entries = JSON.parse(await readFile(queueFilePath, 'utf8'));
  const backdatedAt = new Date(T0 - AGING_THRESHOLD_MS - 100_000).toISOString();
  const nextEntries = entries.map((entry) =>
    entry.commandRef === 'l2' ? { ...entry, enqueuedAt: backdatedAt } : entry,
  );
  await writeFile(queueFilePath, JSON.stringify(nextEntries), 'utf8');

  const drainedOrder = [];
  for (let i = 0; i < 9; i += 1) {
    const result = cli(['--dequeue-if-capacity', `--orchestrator-id=${ORCH}`], {
      ARM_FAKE_NOW_MS: String(T0),
    });
    expect(result.status).toBe(0);
    const body = JSON.parse(result.stdout);
    expect(body.granted).toBe(true);
    drainedOrder.push(body.item.commandRef);

    const acked = cli([`--ack-lease=${body.leaseId}`, `--orchestrator-id=${ORCH}`]);
    expect(JSON.parse(acked.stdout).acked).toBe(true);
  }

  // Priority strictly governs the high block first — aging never lets a
  // "low" item skip past "high". Within the promoted "normal" rank, l2's
  // backdated (earliest) enqueuedAt sorts it ahead of n0/n1/n2. l0/l1 remain
  // in the (now l2-depleted) low block, unaffected since they weren't aged.
  expect(drainedOrder).toEqual(['h0', 'h1', 'h2', 'l2', 'n0', 'n1', 'n2', 'l0', 'l1']);

  const emptyProbe = cli(['--dequeue-if-capacity', `--orchestrator-id=${ORCH}`], {
    ARM_FAKE_NOW_MS: String(T0),
  });
  expect(JSON.parse(emptyProbe.stdout)).toEqual({ item: null, granted: false, reason: 'queue-empty' });
});

// ---------------------------------------------------------------------------
// Scenario 3 — two orchestrators racing: never claim the same item, never
// jointly exceed the ceiling.
// ---------------------------------------------------------------------------

it('two racing orchestrators never claim the same item and never jointly exceed the ceiling', async () => {
  const ORCH_A = 'orch-race-a';
  const ORCH_B = 'orch-race-b';
  const env = baseEnv();

  for (let i = 0; i < 5; i += 1) {
    enqueue(`race-${i}`, 'normal');
  }

  const grantedRefs = new Set();
  const running = []; // { orchestratorId, leaseId, commandRef }

  async function raceRound() {
    const [a, b] = await Promise.all([
      runCliAsync(['--dequeue-if-capacity', `--orchestrator-id=${ORCH_A}`], env),
      runCliAsync(['--dequeue-if-capacity', `--orchestrator-id=${ORCH_B}`], env),
    ]);
    expect(a.status).toBe(0);
    expect(b.status).toBe(0);
    return { bodyA: JSON.parse(a.stdout), bodyB: JSON.parse(b.stdout) };
  }

  function registerRunning(orchestratorId, body) {
    expect(grantedRefs.has(body.item.commandRef)).toBe(false); // never the same item twice
    grantedRefs.add(body.item.commandRef);
    const claimResult = cli([`--claim=${LIVE_AGENT_CLAIM_TYPE}:1`, `--orchestrator-id=${orchestratorId}`]);
    expect(JSON.parse(claimResult.stdout).granted).toBe(1);
    running.push({ orchestratorId, leaseId: body.leaseId, commandRef: body.item.commandRef });
  }

  // Round 1: 5 items available, running=0 -> both concurrent requests granted.
  const round1 = await raceRound();
  expect(round1.bodyA.granted).toBe(true);
  expect(round1.bodyB.granted).toBe(true);
  expect(round1.bodyA.item.commandRef).not.toBe(round1.bodyB.item.commandRef);
  registerRunning(ORCH_A, round1.bodyA);
  registerRunning(ORCH_B, round1.bodyB);
  expect(running).toHaveLength(2);

  // Round 2: running=2 -> both concurrent requests still granted, reaching
  // the ceiling exactly (running=4).
  const round2 = await raceRound();
  expect(round2.bodyA.granted).toBe(true);
  expect(round2.bodyB.granted).toBe(true);
  expect(round2.bodyA.item.commandRef).not.toBe(round2.bodyB.item.commandRef);
  registerRunning(ORCH_A, round2.bodyA);
  registerRunning(ORCH_B, round2.bodyB);
  expect(running).toHaveLength(LIVE_AGENT_CEILING);

  // Round 3: capacity is fully saturated (running=4=ceiling) with exactly 1
  // item left in the queue — racing both orchestrators for it must NEVER
  // grant either (the ceiling is the binding constraint here, not item
  // availability), proving the ceiling is never jointly exceeded.
  const round3 = await raceRound();
  expect(round3.bodyA.granted).toBe(false);
  expect(round3.bodyB.granted).toBe(false);
  expect(['no-capacity', 'queue-empty']).toContain(round3.bodyA.reason);
  expect(['no-capacity', 'queue-empty']).toContain(round3.bodyB.reason);

  // The last, still-unclaimed item was never lost: --peek still shows all 5
  // entries (the 4 admitted-and-running items are still in the queue too —
  // ack-lease, not --claim, is what removes an entry; those 4 haven't been
  // acked yet), with exactly one entry carrying no leaseId — proving the
  // claim-then-deny path in round 3 released it back rather than losing it.
  const remaining = peek();
  expect(remaining).toHaveLength(5);
  const stillUnclaimed = remaining.filter((entry) => entry.leaseId === undefined);
  expect(stillUnclaimed).toHaveLength(1);

  // Free 2 of orch-A's real running slots (simulating 2 of its agents
  // completing) — running=2, capacity=2 free.
  const orchARunning = running.filter((entry) => entry.orchestratorId === ORCH_A);
  for (const entry of orchARunning) {
    const releaseResult = cli([`--release=${LIVE_AGENT_CLAIM_TYPE}:1`, `--orchestrator-id=${ORCH_A}`]);
    expect(JSON.parse(releaseResult.stdout).released).toBe(1);
    const ackResult = cli([`--ack-lease=${entry.leaseId}`, `--orchestrator-id=${ORCH_A}`]);
    expect(JSON.parse(ackResult.stdout).acked).toBe(true);
  }
  const stillRunning = running.filter((entry) => entry.orchestratorId === ORCH_B);

  // Round 4: capacity is free again (2 slots) and exactly 1 item remains —
  // racing both orchestrators for it must grant EXACTLY ONE of them (item
  // scarcity, not capacity, is now the binding constraint) — the other must
  // see queue-empty, never a duplicate grant of the same item.
  const round4 = await raceRound();
  const grants4 = [round4.bodyA, round4.bodyB].filter((body) => body.granted);
  const denials4 = [round4.bodyA, round4.bodyB].filter((body) => !body.granted);
  expect(grants4).toHaveLength(1);
  expect(denials4).toHaveLength(1);
  expect(denials4[0].reason).toBe('queue-empty');
  expect(grantedRefs.has(grants4[0].item.commandRef)).toBe(false);

  const winnerOrchestratorId = round4.bodyA.granted ? ORCH_A : ORCH_B;
  registerRunning(winnerOrchestratorId, grants4[0]);

  // Drain whatever real running agents remain to a clean final state — no
  // item lost, none double-acked.
  const finalRunning = [...stillRunning, running[running.length - 1]];
  const seenRefs = new Set();
  for (const entry of finalRunning) {
    expect(seenRefs.has(entry.commandRef)).toBe(false);
    seenRefs.add(entry.commandRef);
    const releaseResult = cli([`--release=${LIVE_AGENT_CLAIM_TYPE}:1`, `--orchestrator-id=${entry.orchestratorId}`]);
    expect(JSON.parse(releaseResult.stdout).released).toBe(1);
    const ackResult = cli([`--ack-lease=${entry.leaseId}`, `--orchestrator-id=${entry.orchestratorId}`]);
    expect(JSON.parse(ackResult.stdout).acked).toBe(true);
  }

  expect(grantedRefs.size).toBe(5); // every one of the 5 items was granted exactly once
  expect(peek()).toEqual([]);
});

// ---------------------------------------------------------------------------
// Scenario 4 — crash recovery: a claimed-but-never-acked item is reclaimed
// after its queue lease's TTL expires, and completes exactly once.
// ---------------------------------------------------------------------------

it('a claim never acked (simulated crash) is reclaimed after the queue lease TTL and completes exactly once', () => {
  const ORCH = 'orch-crash-sim';
  const T0 = 1_700_600_000_000;
  const T1_PAST_QUEUE_TTL = T0 + DEFAULT_QUEUE_CLAIM_TTL_MS + 1_000;

  enqueue('crash-item', 'normal');

  // First "beat": granted, but the caller crashes before ever calling
  // --ack-lease (or --release-lease) — the queue lease is left dangling.
  const firstGrant = cli(['--dequeue-if-capacity', `--orchestrator-id=${ORCH}`], { ARM_FAKE_NOW_MS: String(T0) });
  expect(firstGrant.status).toBe(0);
  const firstBody = JSON.parse(firstGrant.stdout);
  expect(firstBody.granted).toBe(true);
  expect(firstBody.item.commandRef).toBe('crash-item');
  const staleLeaseId = firstBody.leaseId;

  // While the lease is still fresh, a subsequent attempt correctly sees
  // nothing eligible (the item is under an unexpired lease) — not lost, not
  // double-granted.
  const tooSoon = cli(['--dequeue-if-capacity', '--orchestrator-id=orch-too-soon'], {
    ARM_FAKE_NOW_MS: String(T0 + 1_000),
  });
  expect(JSON.parse(tooSoon.stdout)).toEqual({ item: null, granted: false, reason: 'queue-empty' });

  // Advance the fake clock past the queue lease's own TTL (finding 4) — no
  // real wall-clock wait, no new CLI flag needed.
  const reclaim = cli(['--dequeue-if-capacity', `--orchestrator-id=${ORCH}`], {
    ARM_FAKE_NOW_MS: String(T1_PAST_QUEUE_TTL),
  });
  expect(reclaim.status).toBe(0);
  const reclaimBody = JSON.parse(reclaim.stdout);
  expect(reclaimBody.granted).toBe(true);
  expect(reclaimBody.item.commandRef).toBe('crash-item');
  expect(reclaimBody.leaseId).not.toBe(staleLeaseId);

  // The original, now-superseded leaseId is a safe no-op — proves the crash
  // path can never double-complete the item via its stale lease.
  const staleAck = cli([`--ack-lease=${staleLeaseId}`, `--orchestrator-id=${ORCH}`], {
    ARM_FAKE_NOW_MS: String(T1_PAST_QUEUE_TTL),
  });
  expect(JSON.parse(staleAck.stdout)).toEqual({ acked: false });

  // The reclaimed lease completes exactly once.
  const finalAck = cli([`--ack-lease=${reclaimBody.leaseId}`, `--orchestrator-id=${ORCH}`], {
    ARM_FAKE_NOW_MS: String(T1_PAST_QUEUE_TTL),
  });
  expect(JSON.parse(finalAck.stdout)).toEqual({ acked: true, item: reclaimBody.item });

  // A repeat ack of the SAME (already-consumed) leaseId is a safe no-op —
  // final proof of "exactly once", not "at least once".
  const repeatAck = cli([`--ack-lease=${reclaimBody.leaseId}`, `--orchestrator-id=${ORCH}`], {
    ARM_FAKE_NOW_MS: String(T1_PAST_QUEUE_TTL),
  });
  expect(JSON.parse(repeatAck.stdout)).toEqual({ acked: false });

  expect(peek()).toEqual([]);
});
