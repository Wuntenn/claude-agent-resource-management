// Phase 3 — a hard ceiling on concurrent LIVE agents.
//
// RED BY CONSTRUCTION: no production code implementing this ceiling exists
// yet (Phase 3 has not been built). Every test below is expected to fail
// today — either because the real `cli.mjs` `--desired-agents` beat admits
// far more than the ceiling (today it falls back to the flat, axis-gated
// `DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis` placeholder — see cli.mjs's
// own comment at the `probeAvailableCapacity` `availableCapacity` line,
// "the flat DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis (3) stands in until
// Phase 3 installs a real live-agent ceiling"), or because seeding the
// assumed ceiling ledger via the library-level primitives below has no
// effect on `cli.mjs`'s admission decision at all yet.
//
// ---------------------------------------------------------------------------
// DESIGN ASSUMPTIONS (light-touch — the implementer may rename/reshape any
// of this; the BEHAVIOURAL assertions below are what's load-bearing):
//
//   1. The live-agent ceiling is NOT a new lock/TTL primitive. It reuses the
//      already-merged, already-fencing-safe `claimCapacity`/`releaseCapacity`
//      pair (./lib/coordination-file.mjs,) against a
//      dedicated claim `type` (assumed here: `'live-agent'`) with a FIXED
//      `computeAvailableCapacity` that always returns the ceiling constant
//      (assumed default: 4, i.e. `LIVE_AGENT_CEILING` below) — independent of
//      the memory/disk headroom math entirely. This is the "reuse the
//      existing withLock pattern claimCapacity uses, not a new ad hoc lock"
//      requirement, and it is also what already carries its
//      in-critical-section fencing re-check (`writeEntriesAtomic`'s
//      mandatory `lockContext.assertStillHeld()`) — nothing new to wire up
//      there.
//   2. TTL/liveness marker: SUPERSEDED by — corrected below; kept
//      struck-through rather than deleted so the historical reasoning
//      (and why it turned out to be wrong) stays legible.
//
//      ORIGINAL ASSUMPTION, now known incorrect: the ceiling's claim
//      records were assumed to use `claimTtlMs` equal to
//      `DEFAULT_FRESHNESS_WINDOW_MS` (90s) for aging out a crashed agent's
//      slot, on the theory that reusing `DEFAULT_CLAIM_TTL_MS` (6h) — the
//      pre-existing, unrelated per-operation-type `--claim`/`--release`
//      ledger's TTL — "would leave a crashed agent's slot occupied
//      for hours, not within one freshness window as the ticket requires".
//
// CORRECTION: that reasoning inverted which claim outlives which.
//      A real `--claim=live-agent:N` (the same `LIVE_AGENT_CLAIM_TYPE`
//      ledger this ceiling reads) is written with `claimTtlMs:
//      DEFAULT_CLAIM_TTL_MS` (6h) — see `cli.mjs`'s `handleClaim` — because a
//      live agent legitimately runs for hours, not 90 seconds. Pruning that
//      SAME ledger against the 90s `DEFAULT_FRESHNESS_WINDOW_MS` when
//      checking the ceiling (as this file originally assumed) meant a real,
//      still-running agent's slot silently freed up after 90s of the
//      ORCHESTRATOR merely not re-polling — undercounting the ceiling for
//      every agent older than 90s, live-agent-crashed or not. its fix
//      (`reserveAdmission`'s `snapshotClaimTtlMs` option, `lib/
//      coordination-file.mjs`) makes the ceiling check prune
//      `LIVE_AGENT_CLAIM_TYPE` against `DEFAULT_CLAIM_TTL_MS` (6h) — the
//      SAME TTL a real `--claim` already writes with — not
//      `DEFAULT_FRESHNESS_WINDOW_MS`. A crashed agent that never releases
//      therefore now legitimately occupies its ceiling slot until it ages
//      out at the real 6h claim TTL, same as any other live-agent claim; it
//      does NOT age out "within one freshness window" as originally assumed
//      here (Test 3, below, is updated to assert the corrected 6h timescale).
//      `DEFAULT_FRESHNESS_WINDOW_MS` remains the correct TTL for the
//      DIFFERENT, short-lived `LIVE_AGENT_ADMISSION_CLAIM_TYPE` ledger
//      (advisory in-flight admission holds, not a live agent's own
//      presence) — that ledger's use of the 90s window is unaffected by
// and is unchanged throughout this file.
//
//      Neither `DEFAULT_FRESHNESS_WINDOW_MS` nor `DEFAULT_CLAIM_TTL_MS` can
//      be imported directly from cli.mjs in this file (cli.mjs runs `main()`
//      unconditionally at module scope on import — every sibling spec file
//      in this directory spawns it as a child process instead, never
//      imports it), so both are duplicated here as literals, exactly the way
//      this directory's other spec files already duplicate cli.mjs's
//      `DEFAULT_*` fixtures/values.
//   3. Admission wiring: `main()`'s existing `--desired-agents` beat (no new
//      CLI flag) is assumed to internally issue this same `claimCapacity`
//      call as an ADDITIONAL gate alongside today's dibs-allowance/spawn-
//      bucket gates. UPDATE (Option B, owner-decided after review flagged a
//      design conflict — see the PR discussion): it does NOT clamp
//      `trafficLight.allowance` down to whatever it granted — that would
//      silently narrow `allowance`'s pre-existing, pinned "advisory
//      headroom, not a target" meaning (see cli.jest.spec.mjs's Medium
//      regression test and SKILL.md's flagship worked example, both
//      unchanged by this ticket) for just this one axis. Instead, `cli.mjs`
//      reports what was ACTUALLY, atomically reserved via the ledger as a
//      SEPARATE sibling field, `liveAgentGrant` — same shape as the existing
//      `diskTrend` sibling field (a fact `cli.mjs` attaches to the
//      traffic-light JSON, not a value `buildTrafficLight` produces).
//      `liveAgentGrant` is `null` whenever the atomic claim was never
//      attempted this beat (heartbeat, AMBER/RED, or a beat whose own
//      dibs-bounded allowance is already 0) and a finite granted count
//      (which can be `0`) otherwise. So every CLI-level assertion below that
//      cares about the REAL reservation checks `liveAgentGrant`, not
//      `allowance` — only already-existing flags (`--orchestrator-id`,
//      `--desired-agents`, `ARM_COORDINATION_FILE`, `ARM_FAKE_COLLECT_JSON`,
//      `ARM_FAKE_NOW_MS`) and the established `{ type, allowance,
//      liveAgentGrant }` JSON output contract are used (see
//      ./outer-acceptance.jest.spec.mjs).
//   4. Graceful exit (test 2) is assumed to be exposed via the ALREADY
//      GENERIC `--release=<type>:<count>` CLI flag against the same
//      `'live-agent'` type — but is exercised here directly through the
//      `releaseCapacity` library primitive (which that flag is assumed to
//      call), since that primitive is guaranteed to already exist today,
//      unlike any not-yet-invented flag wiring.
//
// If the real Phase 3 implementation uses a different type string, ceiling
// constant name, or TTL constant, only the small "assumed constants" block
// below should need to change — the assertions describe ceiling BEHAVIOUR,
// not these particular names.
// ---------------------------------------------------------------------------

import { spawn, spawnSync } from 'node:child_process';
import { promises as fsPromises } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import {
  claimCapacity,
  releaseCapacity,
  reserveAdmission,
  acquireLock,
  reclaimStaleLock,
  releaseLock,
  AIMD_CEILING_RESERVED_ID,
} from './lib/coordination-file.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

// Assumed ceiling identity — see DESIGN ASSUMPTIONS #1/#2 above.
const LIVE_AGENT_CLAIM_TYPE = 'live-agent';
const LIVE_AGENT_ADMISSION_CLAIM_TYPE = 'live-agent-admission';
const LIVE_AGENT_CEILING = 4;
const DEFAULT_FRESHNESS_WINDOW_MS = 90 * 1000; // mirrors cli.mjs's real export, see assumption #2.
// mirrors cli.mjs's real export (lib/coordination-file.mjs) — the TTL a real
// `--claim=live-agent:N` writes with, and (post-) the TTL the ceiling
// check itself now prunes `LIVE_AGENT_CLAIM_TYPE` against. See assumption #2.
const DEFAULT_CLAIM_TTL_MS = 6 * 60 * 60 * 1000;

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };
// Deliberately over-generous — a wrong/permissive headroom reading, matching
// ./outer-acceptance.jest.spec.mjs's fixture: today's (Phase-2-retired) flat
// headroom math would happily admit far more than the ceiling from this
// alone. Proves the ceiling gates independently of memory/disk accounting.
const MEMORY_WRONG_PERMISSIVE = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };

/** Fixed, always-available capacity — the ceiling is NOT headroom-derived. */
function fixedCeilingCapacity() {
  return LIVE_AGENT_CEILING;
}

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
 * ./cli-two-orchestrators.jest.spec.mjs:551-608 / ./outer-acceptance.jest.spec.mjs
 * exactly. Required (not the sequential `runCli` pattern) for every scenario
 * below that asserts a genuine race outcome, per the Build Plan's explicit
 * instruction.
 */
function runCliAsync(args, extraEnv) {
  return new Promise((resolvePromise, rejectPromise) => {
    const child = spawn('node', [CLI, ...args], {
      env: { ...process.env, ...extraEnv },
    });
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

function parseStdout(stdout) {
  return JSON.parse(stdout);
}

/**
 * Sums the REAL, atomically-reserved live-agent count across a set of
 * parsed traffic-light results — the operational number (`liveAgentGrant`),
 * not the advisory dibs headroom (`allowance`, which Option B keeps
 * unnarrowed by this reservation — see this file's header comment). `null`
 * (claim never attempted) contributes 0.
 */
function sumGrantedLiveAgent(results) {
  return results.reduce((sum, parsed) => sum + (parsed.liveAgentGrant ?? 0), 0);
}

/** Seeds `count` already-"live" agents into the assumed ceiling ledger. */
async function seedLiveAgents(filePath, count, { now, idPrefix = 'seed-agent' } = {}) {
  for (let index = 0; index < count; index += 1) {
    const result = await claimCapacity(
      filePath,
      {
        type: LIVE_AGENT_CLAIM_TYPE,
        requestedCount: 1,
        orchestratorId: `${idPrefix}-${index}`,
        computeAvailableCapacity: fixedCeilingCapacity,
        now,
      },
      { claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS },
    );
    if (result.granted !== 1) {
      throw new Error(`seedLiveAgents: expected to seed 1 slot for ${idPrefix}-${index}, got ${result.granted}`);
    }
  }
}

let workDir;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-live-agent-ceiling-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Test 1 — two orchestrators racing for the 4th/5th slot: exactly one
// admitted at the boundary.
// ---------------------------------------------------------------------------

test(
  '1. two orchestrators racing for the 4th/5th live-agent slot — exactly one is admitted (genuine race)',
  async () => {
    const T0 = 1_700_000_000_000;

    // Pre-fill 3 of the 4 ceiling slots — simulates 3 already-live agents,
    // via the SAME primitive the real ceiling is assumed to use internally.
    await seedLiveAgents(coordinationFilePath, LIVE_AGENT_CEILING - 1, { now: T0 });

    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      ARM_BEAT_NOW_TEST_MODE: '1',
      ARM_FAKE_NOW_MS: String(T0 + 1),
    };

    // Two REAL, genuinely concurrent orchestrator processes race for the
    // one remaining slot (the "4th" from their own perspective, "5th"
    // counting the pre-seeded 3 — i.e. exactly the boundary the ceiling of
    // 4 must enforce).
    const [resultA, resultB] = await Promise.all([
      runCliAsync(['--orchestrator-id=racer-d', '--desired-agents=1'], env),
      runCliAsync(['--orchestrator-id=racer-e', '--desired-agents=1'], env),
    ]);

    expect(resultA.status).toBe(0);
    expect(resultB.status).toBe(0);

    const parsedA = parseStdout(resultA.stdout);
    const parsedB = parseStdout(resultB.stdout);

    // Exactly one of the two racers is granted the single remaining slot —
    // never both (that would breach the ceiling), never neither (the slot
    // genuinely exists and must not be lost to the race). This is the REAL,
    // atomically-reserved count (`liveAgentGrant`) — the advisory dibs
    // `allowance` is a separate fact and is not asserted here (see this
    // file's header comment).
    const totalGranted = sumGrantedLiveAgent([parsedA, parsedB]);
    expect(totalGranted).toBe(1);

    const grantedCount = [parsedA, parsedB].filter((parsed) => parsed.liveAgentGrant === 1).length;
    expect(grantedCount).toBe(1);
  },
  15_000,
);

// ---------------------------------------------------------------------------
// Test 2 — ceiling decrements on graceful exit.
// ---------------------------------------------------------------------------

test('2. ceiling decrements on graceful exit, freeing a slot for the next admission', async () => {
  const T0 = 1_700_100_000_000;

  // Fill the ceiling completely.
  await seedLiveAgents(coordinationFilePath, LIVE_AGENT_CEILING, { now: T0 });

  const env = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
    ARM_BEAT_NOW_TEST_MODE: '1',
    ARM_FAKE_NOW_MS: String(T0 + 1_000),
  };

  const refusedBeforeRelease = runCli(['--orchestrator-id=late-arrival', '--desired-agents=1'], env);
  expect(refusedBeforeRelease.status).toBe(0);
  // The REAL ledger reservation is refused (ceiling full) — asserted on
  // `liveAgentGrant`, not `allowance` (which reports solo dibs headroom,
  // unaffected by the live-agent ceiling under Option B).
  expect(parseStdout(refusedBeforeRelease.stdout)).toMatchObject({ type: 'spawn-allowed', liveAgentGrant: 0 });

  // Graceful exit: the first seeded agent's slot is explicitly released —
  // assumption #4 above, exercised via the underlying primitive.
  const releaseResult = await releaseCapacity(
    coordinationFilePath,
    { type: LIVE_AGENT_CLAIM_TYPE, releaseCount: 1, orchestratorId: 'seed-agent-0', now: T0 + 2_000 },
    { claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS },
  );
  expect(releaseResult.released).toBe(1);

  const admittedAfterRelease = runCli(
    ['--orchestrator-id=late-arrival', '--desired-agents=1'],
    { ...env, ARM_FAKE_NOW_MS: String(T0 + 3_000) },
  );
  expect(admittedAfterRelease.status).toBe(0);
  expect(parseStdout(admittedAfterRelease.stdout)).toMatchObject({ type: 'spawn-allowed', liveAgentGrant: 1 });
});

// ---------------------------------------------------------------------------
// Test 3 — ceiling decrements via TTL on crash / no exit signal, at the real
// claim TTL (correction — was wrongly asserted at one 90s freshness
// window; see this file's header, assumption #2).
// ---------------------------------------------------------------------------

test(
  '3. a crashed agent that never releases still ages out of the ceiling, but only at the real ' +
    '6h claim TTL — NOT within one 90s freshness window',
  async () => {
    const T0 = 1_700_200_000_000;

    // Fill the ceiling — none of these agents ever calls --release, simulating
    // an ungraceful crash / SIGKILL for all four.
    await seedLiveAgents(coordinationFilePath, LIVE_AGENT_CEILING, { now: T0 });

    const baseEnv = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      ARM_BEAT_NOW_TEST_MODE: '1',
    };

    // Still well inside both TTLs — the crashed agents' slots must still be
    // occupied.
    const stillWithinWindow = runCli(
      ['--orchestrator-id=next-agent', '--desired-agents=1'],
      { ...baseEnv, ARM_FAKE_NOW_MS: String(T0 + DEFAULT_FRESHNESS_WINDOW_MS - 1_000) },
    );
    expect(stillWithinWindow.status).toBe(0);
    expect(parseStdout(stillWithinWindow.stdout)).toMatchObject({ type: 'spawn-allowed', liveAgentGrant: 0 });

    // Past the OLD 90s freshness window this test used to assert as the
    // aging-out point (pre-existing) — a live agent legitimately runs for hours,
    // so the crashed agents' claims must still be occupied here. This is the
    // load-bearing regression assertion: before the fix, `main()`'s
    // ceiling check wrongly pruned `LIVE_AGENT_CLAIM_TYPE` against
    // `DEFAULT_FRESHNESS_WINDOW_MS`, so this beat would have wrongly admitted.
    const pastOldFreshnessWindow = runCli(
      ['--orchestrator-id=next-agent', '--desired-agents=1'],
      { ...baseEnv, ARM_FAKE_NOW_MS: String(T0 + DEFAULT_FRESHNESS_WINDOW_MS + 1_000) },
    );
    expect(pastOldFreshnessWindow.status).toBe(0);
    expect(parseStdout(pastOldFreshnessWindow.stdout)).toMatchObject({ type: 'spawn-allowed', liveAgentGrant: 0 });

    // Still well inside the REAL 6h claim TTL — one hour after the crash,
    // the slots remain legitimately occupied (a live agent can easily still
    // be running an hour in).
    const stillWithinClaimTtl = runCli(
      ['--orchestrator-id=next-agent', '--desired-agents=1'],
      { ...baseEnv, ARM_FAKE_NOW_MS: String(T0 + 60 * 60 * 1000) },
    );
    expect(stillWithinClaimTtl.status).toBe(0);
    expect(parseStdout(stillWithinClaimTtl.stdout)).toMatchObject({ type: 'spawn-allowed', liveAgentGrant: 0 });

    // Past the real `DEFAULT_CLAIM_TTL_MS` (6h) since the crashed agents'
    // claims were recorded — every one of them must now be excluded
    // automatically, with no explicit --release ever having been called.
    // The crashed-agent-recovery guarantee this test protects still holds —
    // the ceiling still eventually recovers on its own — just at the
    // genuinely-correct 6h timescale, not the mistaken 90s one.
    const pastClaimTtl = runCli(
      ['--orchestrator-id=next-agent', '--desired-agents=1'],
      { ...baseEnv, ARM_FAKE_NOW_MS: String(T0 + DEFAULT_CLAIM_TTL_MS + 1_000) },
    );
    expect(pastClaimTtl.status).toBe(0);
    const parsedPastClaimTtl = parseStdout(pastClaimTtl.stdout);
    expect(parsedPastClaimTtl.type).toBe('spawn-allowed');
    expect(parsedPastClaimTtl.liveAgentGrant).toBeGreaterThan(0);
  },
);

// ---------------------------------------------------------------------------
// Test 4 — R4 regression: ceiling count must be unaffected by a stale
// cached resource sample (the shared-machine-sample freshness-window reuse
// must never be the thing the live-agent COUNT is judged against).
// ---------------------------------------------------------------------------

test(
  '4. ceiling count is derived fresh from the ledger every beat, not frozen inside a reused, ' +
    'still-fresh cached resource sample (R4 regression)',
  async () => {
    const T0 = 1_700_300_000_000;

    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      ARM_BEAT_NOW_TEST_MODE: '1',
    };

    // A first real beat takes a genuine machine sample and caches it under
    // the reserved shared-sample entry (existing Phase 3
    // behaviour) — at this instant, ZERO agents are live.
    const firstBeat = runCli(
      ['--orchestrator-id=sampler', '--desired-agents=1'],
      { ...env, ARM_FAKE_NOW_MS: String(T0) },
    );
    expect(firstBeat.status).toBe(0);
    expect(parseStdout(firstBeat.stdout)).toMatchObject({ type: 'spawn-allowed' });

    // A burst of agents becomes live AFTER that sample was cached — seeded
    // directly against the ledger (standing in for other orchestrators'
    // concurrent admissions), bringing the ceiling to fully occupied.
    await seedLiveAgents(coordinationFilePath, LIVE_AGENT_CEILING, {
      now: T0 + 5_000,
      idPrefix: 'burst-agent',
    });

    // Still well within DEFAULT_FRESHNESS_WINDOW_MS of the FIRST beat's
    // sample — a beat right now is guaranteed (per the existing, shipped
    // freshness-window mechanism) to reuse that stale-relative-to-the-burst
    // cached machine reading rather than taking a fresh one. If the ceiling
    // count were ever (incorrectly) derived from anything cached alongside
    // that sample instead of a fresh ledger read, this beat would wrongly
    // admit — the burst would be invisible to it.
    const beatDuringStaleSampleReuse = runCli(
      ['--orchestrator-id=post-burst-agent', '--desired-agents=1'],
      { ...env, ARM_FAKE_NOW_MS: String(T0 + 10_000) },
    );
    expect(beatDuringStaleSampleReuse.status).toBe(0);
    expect(parseStdout(beatDuringStaleSampleReuse.stdout)).toMatchObject({
      type: 'spawn-allowed',
      liveAgentGrant: 0,
    });
  },
);

// ---------------------------------------------------------------------------
// Test 5 — regression: 14-agent cohort at 2GB projected peak refused once
// the ceiling of 4 is reached (Phase-3-scoped version of the outer
// acceptance scenario in ./outer-acceptance.jest.spec.mjs).
// ---------------------------------------------------------------------------

test(
  '5. a 14-agent cohort racing simultaneously is refused once the live-agent ceiling of 4 is reached, ' +
    'independent of a deliberately wrong/permissive headroom stub',
  async () => {
    const COHORT_SIZE = 14;

    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      // Deliberately wrong/over-generous — see MEMORY_WRONG_PERMISSIVE's own
      // comment above. Nothing but a genuine ceiling should be able to
      // refuse this cohort.
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_WRONG_PERMISSIVE, disk: DISK_GREEN }),
    };

    const cohortResults = await Promise.all(
      Array.from({ length: COHORT_SIZE }, (_unused, index) =>
        runCliAsync([`--orchestrator-id=cohort-agent-${index}`, '--desired-agents=1'], env),
      ),
    );

    for (const result of cohortResults) {
      expect(result.status).toBe(0);
    }

    const parsedResults = cohortResults.map((result) => parseStdout(result.stdout));
    // Summed on the REAL, atomically-reserved count (`liveAgentGrant`) — the
    // advisory dibs `allowance` is not bounded by the live-agent ceiling
    // under Option B and can legitimately sum above it across a 14-way race
    // (see this file's header comment).
    const totalGranted = sumGrantedLiveAgent(parsedResults);

    // Phase 3 correction: `LIVE_AGENT_CEILING` (4) is no longer the
    // FLAT bound admission is judged against once `main()`'s
    // `--desired-agents` beat wires in the AIMD-adjusted ceiling — sustained
    // Normal pressure across this very 14-way race (every racer's own beat
    // observes Normal pressure) additively grows the shared, persisted AIMD
    // ceiling (see ./cli-aimd-ceiling.jest.spec.mjs's and
    // ./cli-two-orchestrators.jest.spec.mjs's dedicated tests for the
    // mechanism this exercises for real, under genuine multi-process
    // concurrency). This was RIGHT for (a permanently flat ceiling)
    // and is now a stale assumption deliberately supersedes — not a
    // bug in either ticket.
    //
    // The ceiling only ever GROWS under Normal pressure (it never shrinks
    // except on WARN/CRITICAL, which this race never observes), so the
    // FINAL persisted value, read back after every racer has settled, is a
    // valid upper bound for whatever value was actually in effect at any
    // earlier point during the race. This still proves this test's real
    // load-bearing property — that SOME real, ledger-enforced ceiling
    // genuinely bounds total admission, independent of the deliberately
    // wrong/permissive headroom stub — without pinning the now-superseded
    // "always exactly 4" assumption.
    const onDiskAfterRace = JSON.parse(await fsPromises.readFile(coordinationFilePath, 'utf8'));
    const aimdEntryAfterRace = onDiskAfterRace.find(
      (entry) => entry.orchestratorId === AIMD_CEILING_RESERVED_ID,
    );
    const effectiveCeilingAfterRace = aimdEntryAfterRace ? aimdEntryAfterRace.ceiling : LIVE_AGENT_CEILING;

    expect(totalGranted).toBeGreaterThan(0);
    expect(totalGranted).toBeLessThanOrEqual(effectiveCeilingAfterRace);
  },
  30_000,
);

// ---------------------------------------------------------------------------
// Test 6 — the ceiling's shared lock path genuinely exercises its
// in-critical-section fencing re-check, through the already-merged
// production code (claimCapacity/writeEntriesAtomic), not a new mock.
//
// Reproduction idiom lifted directly from
// ./lib/coordination-file.jest.spec.mjs's own "post-reclaim write"
// describe block (its `seedBulkFiller` + tiny `lockStalenessMs` technique):
// real contention, real work, real reclaim — not a fake timer or an fs
// mock.
// ---------------------------------------------------------------------------

describe('6. the live-agent ceiling claim path exercises fencing re-check under genuine lock contention', () => {
  const FILLER_ENTRY_COUNT = 20_000;
  const slowCriticalSectionStalenessMs = 5;

  async function seedBulkFiller(targetPath, now) {
    const filler = Array.from({ length: FILLER_ENTRY_COUNT }, (_, index) => ({
      orchestratorId: `__filler-${index}__`,
      desiredAgents: 1,
      declaredAt: now,
      padding: 'x'.repeat(40),
    }));
    await fsPromises.writeFile(targetPath, JSON.stringify(filler), 'utf8');
  }

  function expectLandedOrLockLost(promise) {
    return promise.then(
      (value) => ({ outcome: 'landed', value }),
      (reason) => {
        expect(reason).toMatchObject({ name: 'LockLostError', code: 'COORDINATION_LOCK_LOST' });
        return { outcome: 'lock-lost', reason };
      },
    );
  }

  test('a live-agent claim whose critical section outlives lockStalenessMs never silently over-grants after reclaim', async () => {
    const now = Date.now();
    await seedBulkFiller(coordinationFilePath, now);
    const raceOptions = {
      claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS,
      lockStalenessMs: slowCriticalSectionStalenessMs,
      lockRetryDelayMs: 1,
    };

    // Two genuinely racing claimCapacity calls against the SAME
    // artificially-tiny lockStalenessMs the coordination file's own
    // test suite uses to force a live holder's lock to be (prematurely, but
    // legitimately, per reclaimStaleLock's documented limits) reclaimed
    // mid-critical-section — exercising the exact fencing re-check
    // `writeEntriesAtomic` performs immediately before its `rename`, via the
    // real `claimCapacity` call path the live-agent ceiling is assumed to
    // reuse (assumption #1 above), not a new mock of that mechanism.
    const [outcomeA, outcomeB] = await Promise.all([
      expectLandedOrLockLost(
        claimCapacity(
          coordinationFilePath,
          {
            type: LIVE_AGENT_CLAIM_TYPE,
            requestedCount: 1,
            orchestratorId: 'fencing-racer-a',
            computeAvailableCapacity: fixedCeilingCapacity,
            now,
          },
          raceOptions,
        ),
      ),
      expectLandedOrLockLost(
        claimCapacity(
          coordinationFilePath,
          {
            type: LIVE_AGENT_CLAIM_TYPE,
            requestedCount: 1,
            orchestratorId: 'fencing-racer-b',
            computeAvailableCapacity: fixedCeilingCapacity,
            now,
          },
          raceOptions,
        ),
      ),
    ]);

    // The invariant that matters (mirrors coordination-file.jest.spec.mjs's
    // own framing): every call either lands a grant that is genuinely
    // reflected on disk, or is refused via LockLostError — never a grant
    // reported to the caller that the ledger does not actually hold. Total
    // on-disk granted slots for this type must never exceed what a landed
    // outcome claims, and must never silently double-grant the same slot to
    // both racers.
    const landedGrants = [outcomeA, outcomeB]
      .filter((outcome) => outcome.outcome === 'landed')
      .map((outcome) => outcome.value.granted);

    const finalLedger = await claimCapacity(
      coordinationFilePath,
      {
        type: LIVE_AGENT_CLAIM_TYPE,
        requestedCount: 0,
        orchestratorId: 'fencing-observer',
        computeAvailableCapacity: fixedCeilingCapacity,
        now: now + 1,
      },
      {
        claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS,
        lockRetryDelayMs: 1,
        lockStalenessMs: slowCriticalSectionStalenessMs,
      },
    );

    const sumOfLandedGrants = landedGrants.reduce((sum, granted) => sum + granted, 0);
    expect(finalLedger.alreadyGrantedTotal).toBe(sumOfLandedGrants);
    expect(finalLedger.alreadyGrantedTotal).toBeLessThanOrEqual(LIVE_AGENT_CEILING);
  }, 60_000);

  test('a lock reclaimed out from under a live-agent claim rejects the write rather than dropping it silently', async () => {
    const lockPath = `${coordinationFilePath}.lock`;
    await fsPromises.writeFile(coordinationFilePath, JSON.stringify([]), 'utf8');

    const staleToken = await acquireLock(lockPath, 60_000);
    // Real reclaim + re-acquire through the production path (identical to
    // coordination-file.jest.spec.mjs's writeEntriesAtomic fencing test) —
    // the lock at this path is now provably someone else's.
    expect(await reclaimStaleLock(lockPath, -1)).toBe(true);
    const newToken = await acquireLock(lockPath, 60_000);

    // A live-agent claim that (somehow) still held `staleToken` must be
    // refused by the SAME fencing mechanism, not a bespoke one for this
    // feature. There is no seam to inject a pre-held token into
    // `claimCapacity` from outside, so this proves the underlying primitive
    // it is assumed to call — `writeEntriesAtomic` — refuses on this
    // caller's behalf.
    const { writeEntriesAtomic, assertStillHeld } = await import('./lib/coordination-file.mjs');
    await expect(
      writeEntriesAtomic(
        coordinationFilePath,
        [{ orchestratorId: `__claim:${LIVE_AGENT_CLAIM_TYPE}__`, claims: [{ orchestratorId: 'ghost', count: 1, claimedAt: Date.now() }] }],
        { lockPath, token: staleToken, assertStillHeld: () => assertStillHeld(lockPath, staleToken) },
      ),
    ).rejects.toMatchObject({ name: 'LockLostError', code: 'COORDINATION_LOCK_LOST' });

    const onDisk = JSON.parse(await fsPromises.readFile(coordinationFilePath, 'utf8'));
    expect(onDisk).toEqual([]);

    await releaseLock(lockPath, newToken);
  });
});

// ---------------------------------------------------------------------------
// Test 7 — pre-PR-review regression: `liveAgentGrant` must never report a
// non-zero reservation on a beat the global spawn-rate bucket already
// denied. Before this fix, main() sized the live-agent ledger claim to the
// PRE-DENIAL `spawnBucketRequestAmount` rather than `Math.min(that,
// spawnBucketGrantedCount)`, so a bucket-denied beat could still report
// `liveAgentGrant` > 0 — silently over-admitting the one gate this ticket
// exists to install, AND burning real ledger capacity for a beat that could
// never actually spawn.
// ---------------------------------------------------------------------------

test(
  '7. a beat the global spawn-rate bucket fully denies reports liveAgentGrant: 0, never the pre-denial ask ' +
    '[regression]',
  () => {
    const env = {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      // Drained, no refill — the bucket has nothing left to grant regardless
      // of how much live-agent-ceiling headroom exists.
      ARM_SPAWN_BUCKET_CONFIG_JSON: JSON.stringify({ capacityTokens: 0, refillTokensPerMs: 0 }),
    };

    const beat = runCli(['--orchestrator-id=orch-bucket-denied', '--desired-agents=3'], env);
    expect(beat.status).toBe(0);
    const parsed = parseStdout(beat.stdout);

    // The bucket's own genuine-denial clamp already zeroes `allowance`
    // (pre-existing Phase 4 behaviour, unaffected by this fix).
    expect(parsed.type).toBe('spawn-allowed');
    expect(parsed.allowance).toBe(0);

    // The regression this test guards: liveAgentGrant must ALSO be 0 — not
    // the full, pre-denial `--desired-agents` ask — since the ledger must
    // never reserve real capacity for a beat that was never going to spawn.
    expect(parsed.liveAgentGrant).toBe(0);
  },
);

// ---------------------------------------------------------------------------
// Test 8 — pre-PR-review regression: `--claim=live-agent:N`'s writer path
// (`handleClaim`) must agree with `main()`'s reader path on the SAME
// ceiling constant (`LIVE_AGENT_CEILING`, 4) — not the unrelated flat-3
// `DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis` placeholder every OTHER
// claim type still (correctly) uses. Before this fix, a caller could never
// register more than 3 genuinely-live agents against this ledger even
// though `main()`'s ceiling check believed 4 slots existed — an
// unreachable 4th real agent, and a ceiling that could never actually
// saturate from `main()`'s point of view (see Test 5 above, which only
// ever seeds via the internal `claimCapacity` primitive directly, not this
// public `--claim` path).
// ---------------------------------------------------------------------------

test(
  "8. --claim=live-agent:4 grants the full LIVE_AGENT_CEILING (4), not the unrelated flat-3 placeholder " +
    '[regression]',
  () => {
    const claim = runCli(
      ['--orchestrator-id=orch-claim-live-agent', `--claim=${LIVE_AGENT_CLAIM_TYPE}:${LIVE_AGENT_CEILING}`],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        // Required for test hermeticity — --claim reads/writes the
        // operation-history file too (see claim.jest.spec.mjs's identical
        // requirement); without this it would fall through to the real
        // production default path.
        ARM_HISTORY_FILE: join(workDir, 'history.json'),
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
      },
    );
    expect(claim.status).toBe(0);
    expect(parseStdout(claim.stdout)).toEqual({ granted: LIVE_AGENT_CEILING });
  },
);

// ---------------------------------------------------------------------------
// Test 9 — (cross-ledger admission atomicity, expanded scope): a real
// `--claim=live-agent:N` must account for outstanding
// `LIVE_AGENT_ADMISSION_CLAIM_TYPE` holds — advisory admission grants
// `main()`'s own `--desired-agents` beat has issued but that no caller has
// yet converted into a genuine spawn. Before this fix, `handleClaim`'s
// `computeAvailableCapacity` for `LIVE_AGENT_CLAIM_TYPE` ignored this ledger
// entirely and always offered the full flat `LIVE_AGENT_CEILING`, so a real
// claim could land on top of an outstanding admission and together exceed
// the ceiling — the reverse of the direction `reserveAdmission` already
// guards (it subtracts `LIVE_AGENT_CLAIM_TYPE`'s live total from ITS own
// headroom).
// ---------------------------------------------------------------------------

test(
  "9. --claim=live-agent:N is denied/reduced when outstanding admission holds would push the combined " +
    'total over LIVE_AGENT_CEILING, even though the real ledger alone has room [ regression]',
  async () => {
    const T0 = 1_700_400_000_000;

    // Two slots are advisorily admitted (via the SAME primitive main()'s own
    // beat uses) but never converted into a genuine --claim=live-agent — the
    // real ledger is still entirely empty at this point.
    const admissionResult = await reserveAdmission(
      coordinationFilePath,
      {
        snapshotType: LIVE_AGENT_CLAIM_TYPE,
        admissionType: LIVE_AGENT_ADMISSION_CLAIM_TYPE,
        requestedCount: 2,
        orchestratorId: 'admission-holder',
        ceiling: LIVE_AGENT_CEILING,
        now: T0,
      },
      { claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS },
    );
    expect(admissionResult.granted).toBe(2);

    // A genuinely real claim for the FULL ceiling arrives shortly after,
    // from a different caller entirely, well within the admission holds'
    // freshness window.
    const claim = runCli(
      ['--orchestrator-id=orch-claim-live-agent-real', `--claim=${LIVE_AGENT_CLAIM_TYPE}:${LIVE_AGENT_CEILING}`],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_HISTORY_FILE: join(workDir, 'history.json'),
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_FAKE_NOW_MS: String(T0 + 1_000),
      },
    );
    expect(claim.status).toBe(0);
    // Only the 2 slots NOT already advisorily admitted are genuinely free —
    // granting the full 4 here would let 2 admitted + 4 claimed = 6 coexist
    // against a ceiling of 4.
    expect(parseStdout(claim.stdout)).toEqual({ granted: LIVE_AGENT_CEILING - 2 });
  },
);

// ---------------------------------------------------------------------------
// Test 10 — : an admission hold whose TTL has already lapsed must NOT
// count against a subsequent `--claim`'s headroom — the subtraction must use
// the SAME TTL-pruned "live" notion `reserveAdmission` itself uses
// (`DEFAULT_FRESHNESS_WINDOW_MS`), not a raw, unpruned count.
// ---------------------------------------------------------------------------

test(
  '10. an EXPIRED admission hold (past DEFAULT_FRESHNESS_WINDOW_MS) does not count against ' +
    "--claim=live-agent:N's headroom [ regression]",
  async () => {
    const T0 = 1_700_500_000_000;

    const admissionResult = await reserveAdmission(
      coordinationFilePath,
      {
        snapshotType: LIVE_AGENT_CLAIM_TYPE,
        admissionType: LIVE_AGENT_ADMISSION_CLAIM_TYPE,
        requestedCount: 2,
        orchestratorId: 'admission-holder-expiring',
        ceiling: LIVE_AGENT_CEILING,
        now: T0,
      },
      { claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS },
    );
    expect(admissionResult.granted).toBe(2);

    // Past exactly one freshness window since the admission hold was
    // recorded — it must be treated as expired, freeing its slots.
    const claim = runCli(
      ['--orchestrator-id=orch-claim-live-agent-after-expiry', `--claim=${LIVE_AGENT_CLAIM_TYPE}:${LIVE_AGENT_CEILING}`],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_HISTORY_FILE: join(workDir, 'history.json'),
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_FAKE_NOW_MS: String(T0 + DEFAULT_FRESHNESS_WINDOW_MS + 1_000),
      },
    );
    expect(claim.status).toBe(0);
    // The expired admission hold no longer counts — the full ceiling is
    // genuinely free.
    expect(parseStdout(claim.stdout)).toEqual({ granted: LIVE_AGENT_CEILING });
  },
);

// ---------------------------------------------------------------------------
// Test 11 — review follow-up (High): the SAME orchestratorId converting
// its OWN outstanding admission hold into a genuine claim must not have that
// hold subtracted from its own headroom. This is the natural extension of
// reusing one stable `--orchestrator-id` across beats — exactly as SKILL.md's
// own `--desired-agents`/`--heartbeat` worked examples already do — so this
// is a supported calling pattern, not an edge case.
// Before this fix, `computeAvailableCapacity` summed
// `LIVE_AGENT_ADMISSION_CLAIM_TYPE` across every orchestrator with no
// exclusion of the caller's own id — self-starving the exact orchestrator
// that is legitimately converting its own hold.
// ---------------------------------------------------------------------------

test(
  '11. converting your OWN outstanding admission hold into a --claim under the SAME orchestrator-id is granted ' +
    'in full, not cut down by your own hold [ regression]',
  async () => {
    const T0 = 1_700_600_000_000;
    const SAME_ORCHESTRATOR_ID = 'orch-self-convert';

    // This orchestrator is granted an admission hold for the full ceiling via
    // --desired-agents (simulated directly through reserveAdmission, the same
    // primitive main()'s beat uses).
    const admissionResult = await reserveAdmission(
      coordinationFilePath,
      {
        snapshotType: LIVE_AGENT_CLAIM_TYPE,
        admissionType: LIVE_AGENT_ADMISSION_CLAIM_TYPE,
        requestedCount: LIVE_AGENT_CEILING,
        orchestratorId: SAME_ORCHESTRATOR_ID,
        ceiling: LIVE_AGENT_CEILING,
        now: T0,
      },
      { claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS },
    );
    expect(admissionResult.granted).toBe(LIVE_AGENT_CEILING);

    // The SAME orchestrator-id now converts that admission into a genuine
    // claim, well within the admission hold's freshness window. Nobody else
    // holds anything, so the full ceiling is genuinely free to THIS caller —
    // its own outstanding admission hold must not count against itself.
    const claim = runCli(
      [`--orchestrator-id=${SAME_ORCHESTRATOR_ID}`, `--claim=${LIVE_AGENT_CLAIM_TYPE}:${LIVE_AGENT_CEILING}`],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_HISTORY_FILE: join(workDir, 'history.json'),
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_FAKE_NOW_MS: String(T0 + 1_000),
      },
    );
    expect(claim.status).toBe(0);
    expect(parseStdout(claim.stdout)).toEqual({ granted: LIVE_AGENT_CEILING });
  },
);

test(
  "12. converting your OWN admission hold still leaves an OTHER orchestrator's outstanding admission " +
    'holds counted against your claim headroom [ regression — exclusion is per-orchestrator, not global]',
  async () => {
    const T0 = 1_700_700_000_000;
    const SELF_ID = 'orch-self-convert-partial';
    const OTHER_ID = 'orch-other-admission-holder';

    // This orchestrator holds an admission for 2 slots.
    const selfAdmission = await reserveAdmission(
      coordinationFilePath,
      {
        snapshotType: LIVE_AGENT_CLAIM_TYPE,
        admissionType: LIVE_AGENT_ADMISSION_CLAIM_TYPE,
        requestedCount: 2,
        orchestratorId: SELF_ID,
        ceiling: LIVE_AGENT_CEILING,
        now: T0,
      },
      { claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS },
    );
    expect(selfAdmission.granted).toBe(2);

    // A DIFFERENT orchestrator also holds an admission for 1 slot.
    const otherAdmission = await reserveAdmission(
      coordinationFilePath,
      {
        snapshotType: LIVE_AGENT_CLAIM_TYPE,
        admissionType: LIVE_AGENT_ADMISSION_CLAIM_TYPE,
        requestedCount: 1,
        orchestratorId: OTHER_ID,
        ceiling: LIVE_AGENT_CEILING,
        now: T0,
      },
      { claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS },
    );
    expect(otherAdmission.granted).toBe(1);

    // SELF_ID now claims the full ceiling. Its own 2-slot hold must be
    // excluded, but OTHER_ID's 1-slot hold must still count against it —
    // only 3 of the 4 ceiling slots are genuinely free to SELF_ID.
    const claim = runCli(
      [`--orchestrator-id=${SELF_ID}`, `--claim=${LIVE_AGENT_CLAIM_TYPE}:${LIVE_AGENT_CEILING}`],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_HISTORY_FILE: join(workDir, 'history.json'),
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_FAKE_NOW_MS: String(T0 + 1_000),
      },
    );
    expect(claim.status).toBe(0);
    expect(parseStdout(claim.stdout)).toEqual({ granted: LIVE_AGENT_CEILING - 1 });
  },
);

// ---------------------------------------------------------------------------
// Test 13 — review follow-up (High): converting an admission hold into
// a genuine `--claim=live-agent:N` must CLEAR that same amount of the
// caller's own outstanding admission hold immediately, not merely exclude it
// from the caller's OWN headroom computation (test 11's fix). Before this
// fix, a converted hold stayed on the admission ledger and was double-
// counted: once as a genuine `live-agent` claim, and again as an
// outstanding `live-agent-admission` hold — starving every OTHER
// orchestrator's headroom for up to `DEFAULT_FRESHNESS_WINDOW_MS` (~90s)
// after a conversion that should have freed those slots immediately.
// ---------------------------------------------------------------------------

test(
  "13. a successful --claim=live-agent:N clears that orchestrator's own admission hold immediately, so a " +
    "DIFFERENT orchestrator's very next claim is not starved by a stale, already-converted hold " +
    '[ regression]',
  async () => {
    const T0 = 1_700_800_000_000;
    const SELF_ID = 'orch-convert-then-clear';
    const OTHER_ID = 'orch-claims-right-after';

    // SELF_ID is advisorily admitted for 2 slots via --desired-agents (the
    // same primitive main()'s own beat uses).
    const admissionResult = await reserveAdmission(
      coordinationFilePath,
      {
        snapshotType: LIVE_AGENT_CLAIM_TYPE,
        admissionType: LIVE_AGENT_ADMISSION_CLAIM_TYPE,
        requestedCount: 2,
        orchestratorId: SELF_ID,
        ceiling: LIVE_AGENT_CEILING,
        now: T0,
      },
      { claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS },
    );
    expect(admissionResult.granted).toBe(2);

    // SELF_ID converts that admission hold into a genuine claim.
    const selfClaim = runCli(
      [`--orchestrator-id=${SELF_ID}`, `--claim=${LIVE_AGENT_CLAIM_TYPE}:2`],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_HISTORY_FILE: join(workDir, 'history.json'),
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_FAKE_NOW_MS: String(T0 + 1_000),
      },
    );
    expect(selfClaim.status).toBe(0);
    expect(parseStdout(selfClaim.stdout)).toEqual({ granted: 2 });

    // A DIFFERENT orchestrator claims the remainder immediately afterward —
    // well within the admission hold's freshness window, so this only
    // passes if the conversion above actually cleared SELF_ID's admission
    // hold rather than leaving it outstanding. Only SELF_ID's 2 now-genuine
    // live-agent slots should count against OTHER_ID's headroom; SELF_ID's
    // admission hold must no longer count a second time.
    const otherClaim = runCli(
      [`--orchestrator-id=${OTHER_ID}`, `--claim=${LIVE_AGENT_CLAIM_TYPE}:${LIVE_AGENT_CEILING}`],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_HISTORY_FILE: join(workDir, 'history.json'),
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_BEAT_NOW_TEST_MODE: '1',
        ARM_FAKE_NOW_MS: String(T0 + 2_000),
      },
    );
    expect(otherClaim.status).toBe(0);
    expect(parseStdout(otherClaim.stdout)).toEqual({ granted: LIVE_AGENT_CEILING - 2 });
  },
);
