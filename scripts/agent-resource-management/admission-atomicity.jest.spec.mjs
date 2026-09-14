// Phase 0 — outer acceptance test: the liveAgentGrant admission
// decision is not atomic across cli.mjs's THREE separate lock cycles.
//
// RED BY CONSTRUCTION: `main()`'s live-agent admission beat (cli.mjs,
// ~lines 2480-2578) today performs its decision across three SEPARATE
// `withLock` critical sections in ./lib/coordination-file.mjs:
//
//   1. `claimCapacity({ type: LIVE_AGENT_CLAIM_TYPE, requestedCount: 0, ... })`
//      — a snapshot read of the ceiling's remaining headroom
//      (`liveAgentCeilingRemaining`).
//   2. `releaseCapacity({ type: LIVE_AGENT_ADMISSION_CLAIM_TYPE,
//      releaseCount: LIVE_AGENT_CEILING, ... })` — releases this
//      orchestrator's own PRIOR admission hold (replace-not-accumulate).
//   3. `claimCapacity({ type: LIVE_AGENT_ADMISSION_CLAIM_TYPE,
//      requestedCount: liveAgentClaimSize, computeAvailableCapacity: () =>
//      liveAgentCeilingRemaining, ... })` — grants against the STALE
//      snapshot captured in step 1.
//
// Because each step is its OWN lock cycle, a concurrent, genuinely
// independent `--claim=live-agent:N` (the public way a real spawned agent
// registers itself as live against the SAME `LIVE_AGENT_CLAIM_TYPE` ledger
// step 1 reads) can land in the WINDOW between steps 1 and 3 — invisible to
// step 3's stale `liveAgentCeilingRemaining` snapshot. The two independent
// actors can then walk away with MORE combined slots than genuinely existed
// free when the race began. The fix (later phases) collapses all three
// steps into one `reserveAdmission()` primitive under a SINGLE `withLock`
// critical section, closing this window entirely (the racer's own
// `--claim` — which needs the same file lock to write — would then be
// forced to either fully precede or fully follow the orchestrator's entire
// atomic decision, never interleave with it).
//
// This file does not touch cli.mjs or ./lib/coordination-file.mjs — it
// exercises the real, already-shipped `--desired-agents` admission beat and
// the real, already-shipped `--claim=live-agent:N` flag as two genuinely
// concurrent black-box child processes (the `runCliAsync` idiom from
// ./live-agent-ceiling.jest.spec.mjs / ./cli-two-orchestrators.jest.spec.mjs),
// racing them against ONE shared coordination file, with NO explicit
// sleep/timing hook (there is none to inject without touching production
// code) — landing "mid-decision" is left to real OS scheduling, matching
// ./cli-two-orchestrators.jest.spec.mjs test 7's own empirically-measured
// ~40%-of-iterations reproduction rate for an analogous unhooked race. A
// single iteration is therefore not a reliable regression guard on its own;
// this file loops the race many times and asserts the invariant on EVERY
// iteration, whether or not that particular iteration's race actually landed
// in the dangerous window.
//
// ---------------------------------------------------------------------------
// The invariant (why it is airtight, not just plausible):
//
// Each iteration seeds the ledger so EXACTLY `FREE_SLOTS_AT_RACE_START` (2)
// slots are genuinely free out of `LIVE_AGENT_CEILING` (4) when the race
// begins, then races two independent, real callers for those same 2 slots:
//   - the orchestrator-under-test's own `--desired-agents=2` admission beat
//     (liveAgentGrant, via the buggy 3-lock-cycle path), and
//   - a second process's `--claim=live-agent:2` (granted, via the
//     already-atomic, single-`claimCapacity`-call `--claim` path).
//
// Both request exactly the 2 slots that exist. If the admission decision
// were genuinely atomic, whichever of the two the shared file lock
// serializes FIRST would exhaust or reduce the true remaining headroom, and
// the second would legitimately see (and be bounded by) that reduction — so
// `orchestratorGrant + racerGrant` could never exceed 2, REGARDLESS of which
// one runs first. It is this "regardless of ordering" property (not a
// specific expected winner) that makes the sum-bounded-by-2 assertion valid
// on every iteration, not just the ones where the race happens to land.
// Today's 3-lock-cycle bug can violate exactly this bound: the orchestrator's
// stale step-1 snapshot can still show 2 free even after the racer's
// `--claim` has already consumed both, so its step-3 grant (sized to that
// stale number) can hand out up to 2 MORE — a combined total above 2, in
// violation of `LIVE_AGENT_CEILING` itself.
// ---------------------------------------------------------------------------

import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { claimCapacity } from './lib/coordination-file.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

// Duplicated as literals rather than imported from cli.mjs — cli.mjs runs
// `main()` unconditionally at module scope on import (every sibling spec
// file in this directory spawns it as a child process instead; see
// ./live-agent-ceiling.jest.spec.mjs's identical header note).
const LIVE_AGENT_CLAIM_TYPE = 'live-agent';
const LIVE_AGENT_CEILING = 4;
const DEFAULT_FRESHNESS_WINDOW_MS = 90 * 1000; // mirrors cli.mjs's real DEFAULT_FRESHNESS_WINDOW_MS export.

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };

const RACE_ITERATIONS = 40;
const FREE_SLOTS_AT_RACE_START = 2;
const SEEDED_LIVE_AGENTS = LIVE_AGENT_CEILING - FREE_SLOTS_AT_RACE_START;

/** Fixed, always-available capacity — mirrors ./live-agent-ceiling.jest.spec.mjs's own helper. */
function fixedCeilingCapacity() {
  return LIVE_AGENT_CEILING;
}

/**
 * Runs the real cli.mjs as a genuinely CONCURRENT child process — mirrors
 * ./live-agent-ceiling.jest.spec.mjs / ./cli-two-orchestrators.jest.spec.mjs's
 * own `runCliAsync` exactly. Required (not the sequential `spawnSync` idiom)
 * so two calls started back-to-back via `Promise.all` actually race for the
 * same coordination-file lock rather than running strictly one after the
 * other.
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

/** Seeds `count` already-"live" agents directly against the real ledger. */
async function seedLiveAgents(filePath, count, { now, idPrefix }) {
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

test(
  'the liveAgentGrant admission decision never over-grants beyond the genuinely-free ledger count, even when ' +
    `a concurrent --claim=live-agent:N lands mid-decision (raced over ${RACE_ITERATIONS} iterations; a single ` +
    'iteration only reproduces the bug probabilistically)',
  async () => {
    let raceLandedIterationCount = 0;

    for (let iteration = 0; iteration < RACE_ITERATIONS; iteration += 1) {
      // Fresh coordination file + history file every iteration — races must
      // not bleed stale claims/locks from one iteration into the next.
      const workDir = await mkdtemp(join(tmpdir(), 'arm-admission-atomicity-'));
      const coordinationFilePath = join(workDir, 'coordination.json');
      const historyFilePath = join(workDir, 'history.json');

      try {
        const now = 1_700_000_000_000 + iteration;

        // Seed the ledger near-full: LIVE_AGENT_CEILING - 2 already-live
        // agents, leaving EXACTLY 2 genuinely-free slots — the near-full
        // case that actually exercises the ceiling-remaining arithmetic.
        await seedLiveAgents(coordinationFilePath, SEEDED_LIVE_AGENTS, {
          now,
          idPrefix: `seed-${iteration}`,
        });

        const sharedEnv = {
          ARM_COORDINATION_FILE: coordinationFilePath,
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
          ARM_BEAT_NOW_TEST_MODE: '1',
          ARM_FAKE_NOW_MS: String(now + 1),
        };

        // The orchestrator-under-test's real admission beat — exercises the
        // production (buggy) 3-lock-cycle decision path in cli.mjs's
        // main(), asking for exactly the 2 slots that are genuinely free.
        const orchestratorStartedAt = Date.now();
        const orchestratorPromise = runCliAsync(
          [
            `--orchestrator-id=admission-under-test-${iteration}`,
            `--desired-agents=${FREE_SLOTS_AT_RACE_START}`,
          ],
          sharedEnv,
        ).then((result) => ({ result, endedAt: Date.now() }));

        // A genuinely SEPARATE process registering itself as a real live
        // agent via the public, already-atomic `--claim=live-agent:N` path
        // — racing for the SAME 2 slots. No explicit timing hook is
        // injected (there is none to add without touching production code);
        // landing mid-decision is left to real OS/event-loop scheduling of
        // the two concurrently spawned child processes.
        const racerStartedAt = Date.now();
        const racerPromise = runCliAsync(
          [
            `--claim=${LIVE_AGENT_CLAIM_TYPE}:${FREE_SLOTS_AT_RACE_START}`,
            `--orchestrator-id=racer-${iteration}`,
          ],
          { ...sharedEnv, ARM_HISTORY_FILE: historyFilePath },
        ).then((result) => ({ result, endedAt: Date.now() }));

        const [
          { result: orchestratorResult, endedAt: orchestratorEndedAt },
          { result: racerResult, endedAt: racerEndedAt },
        ] = await Promise.all([orchestratorPromise, racerPromise]);

        expect(orchestratorResult.status).toBe(0);
        expect(racerResult.status).toBe(0);
        expect(orchestratorResult.stderr).toBe('');
        expect(racerResult.stderr).toBe('');

        const orchestratorParsed = parseStdout(orchestratorResult.stdout);
        const racerParsed = parseStdout(racerResult.stdout);

        // A solo, uncontended, GREEN, non-heartbeat beat with a nonzero
        // dibs-bounded allowance always attempts the live-agent ceiling
        // claim — liveAgentGrant must be a real number here, not null.
        expect(orchestratorParsed.type).toBe('spawn-allowed');
        expect(orchestratorParsed.liveAgentGrant).not.toBeNull();

        const orchestratorGrant = orchestratorParsed.liveAgentGrant ?? 0;
        const racerGrant = racerParsed.granted ?? 0;

        // "Race landed" is measured directly from wall-clock overlap of the
        // two child processes' actual lifetimes, not from the SHAPE of their
        // grants. A "both nonzero" grant heuristic was tried first and
        // discarded: `claimCapacity`'s/`reserveAdmission`'s deny-the-remainder
        // arithmetic (`min(requestedCount, remaining)`) means that once the
        // cross-ledger fix closes the bug, a genuinely
        // atomic decision is ALWAYS winner-take-all for two callers each
        // requesting the full `FREE_SLOTS_AT_RACE_START` — a legitimate
        // (2, 0) or (0, 2) split, never (1, 1) — so "both nonzero" would
        // never fire again post-fix and this diagnostic would spuriously
        // read as "the race never lands," even on iterations where the two
        // processes' lifetimes genuinely overlapped in real wall-clock time.
        // Overlap of `[startedAt, endedAt]` intervals is a fix-independent
        // proxy for "the two processes were genuinely concurrent," which is
        // the only thing this diagnostic needs to confirm — the actual
        // correctness bound is asserted unconditionally below regardless of
        // this tally.
        const intervalsOverlap =
          orchestratorStartedAt <= racerEndedAt && racerStartedAt <= orchestratorEndedAt;
        if (intervalsOverlap) {
          raceLandedIterationCount += 1;
        }

        // THE INVARIANT — unconditional on every iteration, race-landed or
        // not (see this file's header comment for why "regardless of
        // ordering" makes this bound valid either way): the two concurrent,
        // independent, real callers can never walk away with MORE combined
        // slots than genuinely existed free (2) when the race began. This
        // is its bug: the 3-separate-lock-cycle admission decision's
        // stale step-1 snapshot can miss the racer's concurrent
        // registration, letting the combined total exceed
        // FREE_SLOTS_AT_RACE_START (and, transitively, LIVE_AGENT_CEILING
        // itself).
        expect(orchestratorGrant + racerGrant).toBeLessThanOrEqual(FREE_SLOTS_AT_RACE_START);
      } finally {
        await rm(workDir, { recursive: true, force: true });
      }
    }

    // Diagnostic only (not itself a correctness assertion): confirms this
    // run's OS scheduling produced at least some genuinely wall-clock-
    // overlapping races, so the loop above is not silently degenerating into
    // 40 sequential, non-overlapping invocations that could never expose the
    // bug regardless of iteration count.
    expect(raceLandedIterationCount).toBeGreaterThan(0);
  },
  120_000,
);
