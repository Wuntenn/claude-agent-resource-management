// Phase 4 — `--dequeue-if-capacity` / `--ack-lease` / `--release-lease`
// CLI wiring.
//
// RED by construction: none of these three flags exist in cli.mjs yet.
// `main()`'s flag-dispatch has no branch that recognizes
// `--dequeue-if-capacity`, `--ack-lease`, or `--release-lease` — an
// invocation carrying ONLY one of them (no other beat-type flag) falls all
// the way through to the bottom of `main()` and runs the default
// `--desired-agents` spawn-decision beat instead, printing a
// `{"type":"spawn-allowed"|"hold"|"pause"|"alert", ...}`-shaped traffic-light
// object rather than any dequeue/ack/release-shaped JSON. An invocation
// combining one of these three with an EXISTING beat-type flag (e.g.
// `--dequeue-if-capacity --claim=...`) is worse: the existing flag's own
// conflict-check block doesn't know about the new flag either, so it
// silently dispatches into ITS OWN handler as if the new flag weren't
// present at all — succeeding (exit 0) where the design requires rejection
// (exit 2). Every assertion below fails for one of these two reasons until
// Phase 4 lands.
//
// Architectural correction (settled, not open — see the Phase 4
// handoff): `cli.mjs` never spawns a real process itself (see the file's own
// header comment, ~line 1-15, and the `LIVE_AGENT_CLAIM_TYPE` doc comment,
// ~line 216-227: "it never claims a slot on a caller's behalf, since
// admission alone is not the same as a spawn having actually happened").
// `--dequeue-if-capacity` is therefore a THIRD standalone, decision-only beat
// type (mirroring `--dequeue`'s existing "atomic queue-file write, print
// JSON, exit 0, never spawn" shape) — it composes `claimAndAdmitQueueEntry`
// (./lib/dispatch.mjs, Phase 2) and prints its verdict; the external
// orchestrator that invoked this CLI is responsible for actually spawning
// `item.commandRef` and then calling `--ack-lease` (success) or
// `--release-lease` (failure) as two FURTHER standalone beats, each a thin
// CLI wrapper around `./lib/queue.mjs`'s `ackQueueEntry`/`releaseQueueEntry`
// (Phase 1) directly against the queue file.
//
// Printed-JSON contract this file locks in (chosen here since cli.mjs does
// not implement it yet):
//
//   node cli.mjs --dequeue-if-capacity --orchestrator-id=<id>
//     queue empty:            { item: null, granted: false, reason: 'queue-empty' }
//     capacity denied:        { item: null, granted: false, reason: 'no-capacity' }
//       (dispatch.mjs's claimAndAdmitQueueEntry itself returns
//       `{ granted: false, reason: 'capacity-denied', item }` — the claimed
//       item, already released back to the queue by claimAndAdmitQueueEntry
//       before returning. The CLI layer remaps `'capacity-denied'` to the
//       caller-facing literal `'no-capacity'` and reports `item: null`
//       rather than the (already-released, no-longer-actionable) item —
//       there is nothing further for the caller to do with it.)
//     granted:                 { item, granted: true, leaseId }
//   Exit 0 in all three cases (never an error condition), exit 2 only for
//   missing/reserved --orchestrator-id or an unexpected thrown error,
//   mirroring --dequeue's own precedent exactly.
//
//   node cli.mjs --ack-lease=<leaseId> --orchestrator-id=<id>
//     removed:                 { acked: true, item }
//     stale/unknown leaseId:   { acked: false } (safe no-op, exit 0 — not
//       an error, mirroring --dequeue's "empty queue is not an error"
//       precedent for ackQueueEntry's own documented "stale leaseId is a
//       safe no-op returning null" contract, ./lib/queue.mjs)
//
//   node cli.mjs --release-lease=<leaseId> --orchestrator-id=<id>
//     released:                 { released: true, item }
//     stale/unknown leaseId:   { released: false } (safe no-op, exit 0,
//       same rationale as --ack-lease above)
//
//   All three:
//     - require --orchestrator-id (exit 2, same stderr-shape precedent as
//       --dequeue/--peek).
//     - reject a reserved ("__...__") --orchestrator-id (exit 2, same
//       precedent).
//     - are standalone atomic beat types: combined with ANY existing
//       beat-type flag (--enqueue, --peek, --dequeue, --claim, --release,
//       --query-capacity, --record-outcome, --heartbeat, --desired-agents)
//       OR with each other (--dequeue-if-capacity + --ack-lease,
//       --dequeue-if-capacity + --release-lease, --ack-lease +
//       --release-lease), exit 2 with a clear stderr message naming both
//       flags — mirroring --dequeue's/--claim's exact conflict-rejection
//       precedent.
//
// Black-box, real-process-spawning tests — mirrors the established idiom in
// ./dequeue.jest.spec.mjs/./claim.jest.spec.mjs (never importing cli.mjs
// directly; ARM_QUEUE_FILE/ARM_COORDINATION_FILE env-var overrides for test
// hermeticity).

import { spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const FAKE_COLLECT_JSON = JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN });

const LIVE_AGENT_CLAIM_TYPE = 'live-agent';
const LIVE_AGENT_CEILING = 4;

/** Runs the real cli.mjs as a child process — never imports it directly. */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

let workDir;
let queueFilePath;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-dequeue-if-capacity-'));
  queueFilePath = join(workDir, 'queue.json');
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

function baseEnv(extra = {}) {
  return {
    ARM_QUEUE_FILE: queueFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: FAKE_COLLECT_JSON,
    ...extra,
  };
}

function cli(args, extraEnv = {}) {
  return runCli(args, baseEnv(extraEnv));
}

function seedOneItem(commandRef = 'seed-command', orchestratorId = 'orch-seed') {
  const result = cli([`--enqueue=implementation-agent:normal:${commandRef}`, `--orchestrator-id=${orchestratorId}`]);
  expect(result.status).toBe(0);
  return result;
}

/** Fills the shared live-agent ceiling to LIVE_AGENT_CEILING via the existing --claim beat. */
function fillLiveAgentCeiling(orchestratorId = 'orch-ceiling-filler') {
  const result = cli([
    `--orchestrator-id=${orchestratorId}`,
    `--claim=${LIVE_AGENT_CLAIM_TYPE}:${LIVE_AGENT_CEILING}`,
  ]);
  expect(result.status).toBe(0);
  return JSON.parse(result.stdout);
}

/** Frees the ceiling filled by fillLiveAgentCeiling. */
function freeLiveAgentCeiling(orchestratorId = 'orch-ceiling-filler') {
  const result = cli([
    `--orchestrator-id=${orchestratorId}`,
    `--release=${LIVE_AGENT_CLAIM_TYPE}:${LIVE_AGENT_CEILING}`,
  ]);
  expect(result.status).toBe(0);
}

// ---------------------------------------------------------------------------
// Mutual exclusivity — --dequeue-if-capacity vs every existing beat-type
// flag.
// ---------------------------------------------------------------------------

describe('--dequeue-if-capacity is a standalone beat type, rejected when combined with any existing flag', () => {
  test.each([
    ['--enqueue=implementation-agent:normal:x'],
    ['--peek'],
    ['--dequeue'],
    [`--claim=${LIVE_AGENT_CLAIM_TYPE}:1`],
    [`--release=${LIVE_AGENT_CLAIM_TYPE}:1`],
    [`--query-capacity=${LIVE_AGENT_CLAIM_TYPE}`],
    ['--record-outcome'],
    ['--heartbeat'],
    ['--desired-agents=1'],
  ])('--dequeue-if-capacity combined with %s is rejected, exit 2', (otherFlag) => {
    const result = cli(['--dequeue-if-capacity', otherFlag, '--orchestrator-id=orch-a']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
  });
});

// ---------------------------------------------------------------------------
// Mutual exclusivity — --ack-lease vs every existing beat-type flag.
// ---------------------------------------------------------------------------

describe('--ack-lease is a standalone beat type, rejected when combined with any existing flag', () => {
  test.each([
    ['--enqueue=implementation-agent:normal:x'],
    ['--peek'],
    ['--dequeue'],
    [`--claim=${LIVE_AGENT_CLAIM_TYPE}:1`],
    [`--release=${LIVE_AGENT_CLAIM_TYPE}:1`],
    [`--query-capacity=${LIVE_AGENT_CLAIM_TYPE}`],
    ['--record-outcome'],
    ['--heartbeat'],
    ['--desired-agents=1'],
  ])('--ack-lease combined with %s is rejected, exit 2', (otherFlag) => {
    const result = cli(['--ack-lease=deadbeef', otherFlag, '--orchestrator-id=orch-a']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
  });
});

// ---------------------------------------------------------------------------
// Mutual exclusivity — --release-lease vs every existing beat-type flag.
// ---------------------------------------------------------------------------

describe('--release-lease is a standalone beat type, rejected when combined with any existing flag', () => {
  test.each([
    ['--enqueue=implementation-agent:normal:x'],
    ['--peek'],
    ['--dequeue'],
    [`--claim=${LIVE_AGENT_CLAIM_TYPE}:1`],
    [`--release=${LIVE_AGENT_CLAIM_TYPE}:1`],
    [`--query-capacity=${LIVE_AGENT_CLAIM_TYPE}`],
    ['--record-outcome'],
    ['--heartbeat'],
    ['--desired-agents=1'],
  ])('--release-lease combined with %s is rejected, exit 2', (otherFlag) => {
    const result = cli(['--release-lease=deadbeef', otherFlag, '--orchestrator-id=orch-a']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
  });
});

// ---------------------------------------------------------------------------
// Mutual exclusivity — pairwise among the three new flags themselves.
// ---------------------------------------------------------------------------

describe('the three new flags reject being combined with each other', () => {
  test('--dequeue-if-capacity + --ack-lease is rejected, exit 2', () => {
    const result = cli(['--dequeue-if-capacity', '--ack-lease=deadbeef', '--orchestrator-id=orch-a']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
  });

  test('--dequeue-if-capacity + --release-lease is rejected, exit 2', () => {
    const result = cli(['--dequeue-if-capacity', '--release-lease=deadbeef', '--orchestrator-id=orch-a']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
  });

  test('--ack-lease + --release-lease is rejected, exit 2', () => {
    const result = cli(['--ack-lease=deadbeef', '--release-lease=deadbeef', '--orchestrator-id=orch-a']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
  });
});

// ---------------------------------------------------------------------------
// --orchestrator-id required / reserved-id rejection, for all three flags.
// ---------------------------------------------------------------------------

describe('--orchestrator-id is required for all three new flags', () => {
  test('--dequeue-if-capacity without --orchestrator-id is rejected, exit 2', () => {
    const result = cli(['--dequeue-if-capacity']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
  });

  test('--ack-lease without --orchestrator-id is rejected, exit 2', () => {
    const result = cli(['--ack-lease=deadbeef']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
  });

  test('--release-lease without --orchestrator-id is rejected, exit 2', () => {
    const result = cli(['--release-lease=deadbeef']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
  });
});

describe('a reserved ("__...__") --orchestrator-id is rejected for all three new flags', () => {
  test('--dequeue-if-capacity with a reserved orchestrator id is rejected, exit 2', () => {
    const result = cli(['--dequeue-if-capacity', '--orchestrator-id=__reserved__']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
  });

  test('--ack-lease with a reserved orchestrator id is rejected, exit 2', () => {
    const result = cli(['--ack-lease=deadbeef', '--orchestrator-id=__reserved__']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
  });

  test('--release-lease with a reserved orchestrator id is rejected, exit 2', () => {
    const result = cli(['--release-lease=deadbeef', '--orchestrator-id=__reserved__']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
  });
});

// ---------------------------------------------------------------------------
// --dequeue-if-capacity: empty queue.
// ---------------------------------------------------------------------------

test('--dequeue-if-capacity on an empty/nonexistent queue exits 0 with { item: null, granted: false, reason: "queue-empty" }', () => {
  const result = cli(['--dequeue-if-capacity', '--orchestrator-id=orch-empty']);
  expect(result.status).toBe(0);
  expect(result.stderr).toBe('');
  expect(JSON.parse(result.stdout)).toEqual({ item: null, granted: false, reason: 'queue-empty' });
});

// ---------------------------------------------------------------------------
// --dequeue-if-capacity: queue non-empty, ceiling fully claimed elsewhere ->
// no-capacity; item remains claimable afterwards.
// ---------------------------------------------------------------------------

describe('--dequeue-if-capacity denies admission when the shared live-agent ceiling is fully claimed', () => {
  test('exits 0 with { item: null, granted: false, reason: "no-capacity" }, and the item remains claimable once capacity frees up', () => {
    seedOneItem('deferred-command', 'orch-seed');
    fillLiveAgentCeiling('orch-ceiling-filler');

    const denied = cli(['--dequeue-if-capacity', '--orchestrator-id=orch-dequeue']);
    expect(denied.status).toBe(0);
    expect(denied.stderr).toBe('');
    expect(JSON.parse(denied.stdout)).toEqual({ item: null, granted: false, reason: 'no-capacity' });

    // The item was claimed-then-released internally by claimAndAdmitQueueEntry
    // — still visible via --peek, no longer carrying an unexpired lease.
    const peeked = cli(['--peek', '--orchestrator-id=orch-peek']);
    expect(peeked.status).toBe(0);
    const peekedItems = JSON.parse(peeked.stdout).items;
    expect(peekedItems).toHaveLength(1);
    expect(peekedItems[0].commandRef).toBe('deferred-command');
    expect(peekedItems[0].leaseId).toBeUndefined();

    freeLiveAgentCeiling('orch-ceiling-filler');

    const granted = cli(['--dequeue-if-capacity', '--orchestrator-id=orch-dequeue-2']);
    expect(granted.status).toBe(0);
    const grantedBody = JSON.parse(granted.stdout);
    expect(grantedBody.granted).toBe(true);
    expect(grantedBody.item.commandRef).toBe('deferred-command');
    expect(typeof grantedBody.leaseId).toBe('string');
  });
});

// ---------------------------------------------------------------------------
// --dequeue-if-capacity: queue non-empty, capacity free -> granted.
// ---------------------------------------------------------------------------

describe('--dequeue-if-capacity grants admission when capacity is free', () => {
  test('exits 0 with { item, granted: true, leaseId }; --peek still shows the item, now carrying that leaseId (peekQueue does not filter by lease status)', () => {
    seedOneItem('ready-command', 'orch-seed');

    const result = cli(['--dequeue-if-capacity', '--orchestrator-id=orch-dequeue']);
    expect(result.status).toBe(0);
    expect(result.stderr).toBe('');
    const body = JSON.parse(result.stdout);
    expect(body.granted).toBe(true);
    expect(body.item.commandRef).toBe('ready-command');
    expect(typeof body.leaseId).toBe('string');
    expect(body.item.leaseId).toBe(body.leaseId);

    // peekQueue/sortQueueEntries carries no lease-status filter — the leased
    // entry is still visible, now with leaseId/leasedAt populated. This
    // reflects peekQueue's ACTUAL (unmodified, Phase 1) behavior, not a
    // "leased entries are hidden" behavior it never implements.
    const peeked = cli(['--peek', '--orchestrator-id=orch-peek']);
    expect(peeked.status).toBe(0);
    const peekedItems = JSON.parse(peeked.stdout).items;
    expect(peekedItems).toHaveLength(1);
    expect(peekedItems[0].commandRef).toBe('ready-command');
    expect(peekedItems[0].leaseId).toBe(body.leaseId);
  });
});

// ---------------------------------------------------------------------------
// --ack-lease: removes the granted item permanently.
// ---------------------------------------------------------------------------

describe('--ack-lease permanently removes the acked item', () => {
  test('a valid leaseId from a just-granted --dequeue-if-capacity is removed; a follow-up --peek shows it gone', () => {
    seedOneItem('to-ack-command', 'orch-seed');
    const granted = JSON.parse(cli(['--dequeue-if-capacity', '--orchestrator-id=orch-dequeue']).stdout);
    expect(granted.granted).toBe(true);

    const acked = cli([`--ack-lease=${granted.leaseId}`, '--orchestrator-id=orch-ack']);
    expect(acked.status).toBe(0);
    expect(acked.stderr).toBe('');
    const ackedBody = JSON.parse(acked.stdout);
    expect(ackedBody.acked).toBe(true);
    expect(ackedBody.item.commandRef).toBe('to-ack-command');

    const peeked = cli(['--peek', '--orchestrator-id=orch-peek']);
    expect(peeked.status).toBe(0);
    expect(JSON.parse(peeked.stdout).items).toHaveLength(0);
  });

  test('an unknown/stale leaseId is a safe no-op: { acked: false }, exit 0, not an error', () => {
    const result = cli(['--ack-lease=stale-lease-id-that-never-existed', '--orchestrator-id=orch-ack']);
    expect(result.status).toBe(0);
    expect(result.stderr).toBe('');
    expect(JSON.parse(result.stdout)).toEqual({ acked: false });
  });
});

// ---------------------------------------------------------------------------
// --ack-lease: a missing or empty leaseId value is rejected BEFORE
// ackQueueEntry ever runs (pre-PR review finding #3) — exit 2, not a
// silent `{ acked: false }` no-op that would be indistinguishable from a
// genuine stale leaseId.
// ---------------------------------------------------------------------------

describe('--ack-lease rejects a missing or empty leaseId value', () => {
  test('bare --ack-lease (no value; parses to boolean true) exits 2 with a clear stderr message', () => {
    const result = cli(['--ack-lease', '--orchestrator-id=orch-ack']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
    expect(result.stderr).toMatch(/--ack-lease=<leaseId> requires a non-empty leaseId/);
  });

  test('--ack-lease= (empty string) exits 2 with a clear stderr message', () => {
    const result = cli(['--ack-lease=', '--orchestrator-id=orch-ack']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
    expect(result.stderr).toMatch(/--ack-lease=<leaseId> requires a non-empty leaseId/);
  });
});

// ---------------------------------------------------------------------------
// --release-lease: returns the item to the claimable queue, preserving
// commandRef/enqueuedAt, re-grantable via a subsequent --dequeue-if-capacity.
// ---------------------------------------------------------------------------

describe('--release-lease returns the leased item to the claimable queue', () => {
  test('a valid leaseId from a just-granted --dequeue-if-capacity is released; a follow-up --dequeue-if-capacity re-grants the SAME item (same commandRef, preserved enqueuedAt)', () => {
    seedOneItem('to-release-command', 'orch-seed');
    const firstGrant = JSON.parse(cli(['--dequeue-if-capacity', '--orchestrator-id=orch-dequeue']).stdout);
    expect(firstGrant.granted).toBe(true);
    const originalEnqueuedAt = firstGrant.item.enqueuedAt;

    const released = cli([`--release-lease=${firstGrant.leaseId}`, '--orchestrator-id=orch-release']);
    expect(released.status).toBe(0);
    expect(released.stderr).toBe('');
    const releasedBody = JSON.parse(released.stdout);
    expect(releasedBody.released).toBe(true);
    expect(releasedBody.item.commandRef).toBe('to-release-command');
    expect(releasedBody.item.leaseId).toBeUndefined();

    const secondGrant = JSON.parse(
      cli(['--dequeue-if-capacity', '--orchestrator-id=orch-dequeue-2']).stdout,
    );
    expect(secondGrant.granted).toBe(true);
    expect(secondGrant.item.commandRef).toBe('to-release-command');
    expect(secondGrant.item.enqueuedAt).toBe(originalEnqueuedAt);
    expect(secondGrant.leaseId).not.toBe(firstGrant.leaseId);
  });

  test('an unknown/stale leaseId is a safe no-op: { released: false }, exit 0, not an error', () => {
    const result = cli(['--release-lease=stale-lease-id-that-never-existed', '--orchestrator-id=orch-release']);
    expect(result.status).toBe(0);
    expect(result.stderr).toBe('');
    expect(JSON.parse(result.stdout)).toEqual({ released: false });
  });
});

// ---------------------------------------------------------------------------
// --release-lease: a missing or empty leaseId value is rejected BEFORE
// releaseQueueEntry ever runs (pre-PR review finding #3) — same
// rationale as --ack-lease's own rejection above.
// ---------------------------------------------------------------------------

describe('--release-lease rejects a missing or empty leaseId value', () => {
  test('bare --release-lease (no value; parses to boolean true) exits 2 with a clear stderr message', () => {
    const result = cli(['--release-lease', '--orchestrator-id=orch-release']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
    expect(result.stderr).toMatch(/--release-lease=<leaseId> requires a non-empty leaseId/);
  });

  test('--release-lease= (empty string) exits 2 with a clear stderr message', () => {
    const result = cli(['--release-lease=', '--orchestrator-id=orch-release']);
    expect(result.status).toBe(2);
    expect(result.stderr).not.toBe('');
    expect(result.stderr).toMatch(/--release-lease=<leaseId> requires a non-empty leaseId/);
  });
});

// ---------------------------------------------------------------------------
// Post-review fix (Medium finding on Phase 5): `--peek`/plain
// `--dequeue` are now aging-aware, matching `--dequeue-if-capacity`'s
// selection exactly — this proves the three beats agree on an aged item's
// promoted position, using REAL wall-clock time (no ARM_FAKE_NOW_MS), by
// directly backdating one entry's on-disk `enqueuedAt` past
// AGING_THRESHOLD_MS relative to the actual current clock.
// ---------------------------------------------------------------------------

const AGING_THRESHOLD_MS = 10 * 60 * 1000;

describe('--peek, --dequeue, and --dequeue-if-capacity all agree on an aged low-priority item, promoted ahead of a fresher normal-priority item', () => {
  test('an item backdated past AGING_THRESHOLD_MS sorts first under --peek, and is the exact item --dequeue-if-capacity grants', async () => {
    // Seed a fresh normal-priority item, then a low-priority item that will
    // be backdated below — normal enqueue order (low seeded second) would
    // otherwise sort it BEHIND the normal item on priority alone; aging is
    // the only thing that can promote it ahead.
    const normalResult = cli([
      '--enqueue=implementation-agent:normal:fresh-normal-command',
      '--orchestrator-id=orch-seed',
    ]);
    expect(normalResult.status).toBe(0);

    const lowResult = cli([
      '--enqueue=implementation-agent:low:aged-low-command',
      '--orchestrator-id=orch-seed',
    ]);
    expect(lowResult.status).toBe(0);

    // Backdate the low-priority entry's on-disk enqueuedAt well past the
    // aging threshold, relative to the REAL current clock — no fake-time
    // seam involved, matching production behavior exactly.
    const entries = JSON.parse(await readFile(queueFilePath, 'utf8'));
    const backdatedAt = new Date(Date.now() - AGING_THRESHOLD_MS - 60_000).toISOString();
    const backdated = entries.map((entry) =>
      entry.commandRef === 'aged-low-command' ? { ...entry, enqueuedAt: backdatedAt } : entry,
    );
    await writeFile(queueFilePath, JSON.stringify(backdated), 'utf8');

    const peeked = JSON.parse(cli(['--peek', '--orchestrator-id=orch-peek']).stdout);
    expect(peeked.items.map((item) => item.commandRef)).toEqual([
      'aged-low-command',
      'fresh-normal-command',
    ]);

    const granted = JSON.parse(cli(['--dequeue-if-capacity', '--orchestrator-id=orch-dequeue']).stdout);
    expect(granted.granted).toBe(true);
    expect(granted.item.commandRef).toBe('aged-low-command');
  });
});
