// Phase 0 — outer acceptance test: reserveAdmission's single
// options.claimTtlMs prunes the LIVE_AGENT_CLAIM_TYPE snapshot ledger against
// the WRONG TTL.
//
// RED BY CONSTRUCTION: today `reserveAdmission` (./lib/coordination-file.mjs)
// takes exactly one `options.claimTtlMs` and uses it to prune BOTH ledgers it
// reads/writes — `snapshotType`'s (LIVE_AGENT_CLAIM_TYPE) headroom probe at
// Step 1, and `admissionType`'s (LIVE_AGENT_ADMISSION_CLAIM_TYPE) own-hold
// replace/grant at Steps 2-3. Both of `reserveAdmission`'s real call sites —
// cli.mjs's `main()` `--desired-agents` beat, and ./lib/dispatch.mjs's
// `claimAndAdmitQueueEntry` (used by `--dequeue-if-capacity`) — pass
// `DEFAULT_FRESHNESS_WINDOW_MS` (90s) for that single option, because 90s is
// the right freshness window for `LIVE_AGENT_ADMISSION_CLAIM_TYPE`'s OWN
// records (short-lived per-beat admission holds).
//
// But `LIVE_AGENT_CLAIM_TYPE` is a DIFFERENT ledger: it's the same one the
// real `--claim=live-agent:N` handler (`handleClaim` in cli.mjs, via
// `claimCapacity`) prunes using `DEFAULT_CLAIM_TTL_MS` (6h) — a genuinely
// live, currently-spawned agent's claim record is meant to persist for up to
// 6 hours, not 90 seconds. Today's single-TTL `reserveAdmission` prunes THAT
// SAME ledger against the 90s window at Step 1 instead, so any live-agent
// claim older than 90s (but still well inside its real 6h TTL) goes
// invisible to the next admission-ceiling check — `reserveAdmission` then
// computes `liveAgentCeilingRemaining` as if those slots were free, and can
// grant MORE admission than `LIVE_AGENT_CEILING` allows.
//
// The confirmed fix (later phases, NOT implemented by this file): split the
// single option into `options.snapshotClaimTtlMs` (new — governs
// `LIVE_AGENT_CLAIM_TYPE` pruning at Step 1; both call sites will pass
// `DEFAULT_CLAIM_TTL_MS`) and `options.claimTtlMs` (kept — continues to
// govern `LIVE_AGENT_ADMISSION_CLAIM_TYPE` pruning at Steps 2-3, staying
// `DEFAULT_FRESHNESS_WINDOW_MS`).
//
// This test deliberately does NOT probe "does headroom free up after 6h" —
// that would pass whether or not the split is wired correctly, since the
// OLD single 90s TTL already, accidentally, prunes by 6h too (90s < 6h).
// Instead it uses the DIFFERENTIATING window: T + 95s — past the 90s
// freshness window, nowhere near the 6h claim TTL. Only a correctly-wired
// `snapshotClaimTtlMs` (6h) keeps the 4 already-live claims counted against
// the ceiling at that point; the current single-TTL implementation prunes
// them away at 90s and incorrectly reports headroom as if they'd expired.
//
// This file does not touch cli.mjs, ./lib/coordination-file.mjs, or
// ./lib/dispatch.mjs — tests only.

import { mkdtemp, rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { claimCapacity, reserveAdmission, DEFAULT_CLAIM_TTL_MS } from './lib/coordination-file.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

/** Runs the real cli.mjs as a child process — never imports it directly (mirrors ./cli.jest.spec.mjs). */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

// Duplicated as literals rather than imported from cli.mjs — cli.mjs runs
// `main()` unconditionally at module scope on import (every sibling spec
// file in this directory that needs these constants does the same; see
// ./live-agent-ceiling.jest.spec.mjs's and
// ./admission-atomicity.jest.spec.mjs's identical header notes).
const LIVE_AGENT_CLAIM_TYPE = 'live-agent';
const LIVE_AGENT_ADMISSION_CLAIM_TYPE = 'live-agent-admission';
const LIVE_AGENT_CEILING = 4;
const DEFAULT_FRESHNESS_WINDOW_MS = 90 * 1000; // mirrors cli.mjs's real DEFAULT_FRESHNESS_WINDOW_MS export.

/** Fixed, always-available capacity — mirrors ./live-agent-ceiling.jest.spec.mjs's own helper. */
function fixedCeilingCapacity() {
  return LIVE_AGENT_CEILING;
}

describe('reserveAdmission TTL reconciliation — LIVE_AGENT_CLAIM_TYPE snapshot pruning', () => {
  let workDir;
  let coordinationFilePath;

  beforeEach(async () => {
    workDir = await mkdtemp(join(tmpdir(), 'arm-live-agent-ttl-reconciliation-'));
    coordinationFilePath = join(workDir, 'coordination.json');
  });

  afterEach(async () => {
    await rm(workDir, { recursive: true, force: true });
  });

  test(
    'a live-agent claim aged 95s (past the 90s admission-freshness window, but nowhere near the 6h ' +
      'claim TTL) still counts against the ceiling — reserveAdmission must not over-grant admission on ' +
      'top of it',
    async () => {
      const t0 = 1_700_000_000_000;

      // Step 1: a real `--claim=live-agent:4` — mirrors handleClaim's real
      // call shape, filling the entire LIVE_AGENT_CEILING with genuinely
      // live claims at T.
      const claimResult = await claimCapacity(
        coordinationFilePath,
        {
          type: LIVE_AGENT_CLAIM_TYPE,
          requestedCount: 4,
          orchestratorId: 'orc-a',
          computeAvailableCapacity: fixedCeilingCapacity,
          now: t0,
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      );
      expect(claimResult.granted).toBe(4);

      // Step 2: at T + 95s — past the 90s freshness window, nowhere near the
      // 6h claim TTL — a competing orchestrator's admission beat asks for 4
      // more slots against the SAME ceiling.
      const admissionResult = await reserveAdmission(
        coordinationFilePath,
        {
          snapshotType: LIVE_AGENT_CLAIM_TYPE,
          admissionType: LIVE_AGENT_ADMISSION_CLAIM_TYPE,
          requestedCount: 4,
          orchestratorId: 'orc-b',
          ceiling: LIVE_AGENT_CEILING,
          now: t0 + 95_000,
        },
        {
          claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS,
          snapshotClaimTtlMs: DEFAULT_CLAIM_TTL_MS,
        },
      );

      // THE INVARIANT: the 4 already-live claims from Step 1 are still
      // "live" under their real 6h TTL at T + 95s, so the ceiling has ZERO
      // genuine headroom left — reserveAdmission must grant exactly 0.
      //
      // Today's single-options.claimTtlMs implementation instead prunes
      // LIVE_AGENT_CLAIM_TYPE's snapshot against DEFAULT_FRESHNESS_WINDOW_MS
      // (90s) at Step 1 — by T + 95s the 4 claims from Step 1 are already
      // past that 90s window and are pruned away, so
      // alreadyGrantedTotalForSnapshotType reads 0, liveAgentCeilingRemaining
      // reads the full ceiling (4), and this call incorrectly grants 4 more
      // — 8 combined against a ceiling of 4.
      expect(admissionResult.granted).toBe(0);
    },
  );
});

// ---------------------------------------------------------------------------
// Phase 2 (FIXED — commit 43063437) — REAL call-site wiring tests: the
// primitive fixed in Phase 1 (reserveAdmission's split
// `claimTtlMs`/`snapshotClaimTtlMs` options) is now reached from both of its
// real production call sites (cli.mjs's `main()` `--desired-agents` beat,
// and ./lib/dispatch.mjs's `claimAndAdmitQueueEntry`). These two tests
// exercise those call sites directly/end-to-end and now pass.
//
// HISTORICAL NOTE: before commit 43063437 landed the call-site wiring, these
// tests were red by construction and were expected to fail.
// ---------------------------------------------------------------------------

describe('Phase 2 — cli.mjs main()\'s --desired-agents beat must pass snapshotClaimTtlMs through to reserveAdmission', () => {
  let phase2WorkDir;
  let coordinationFilePath;

  beforeEach(async () => {
    phase2WorkDir = await mkdtemp(join(tmpdir(), 'arm-live-agent-ttl-reconciliation-phase2-'));
    coordinationFilePath = join(phase2WorkDir, 'coordination.json');
  });

  afterEach(async () => {
    await rm(phase2WorkDir, { recursive: true, force: true });
  });

  test(
    'a real --claim=live-agent:4 at T, then --desired-agents=4 at T+95s (past the 90s admission-freshness ' +
      'window, nowhere near the 6h claim TTL) must NOT report liveAgentGrant: 4 — the 4 already-live claims ' +
      'are still live under their real 6h TTL and must still count against the ceiling',
    () => {
      const t0 = 1_700_300_000_000;
      const workDirEnv = { ARM_COORDINATION_FILE: coordinationFilePath, ARM_BEAT_NOW_TEST_MODE: '1' };
      const greenCollectEnv = {
        ARM_FAKE_COLLECT_JSON: JSON.stringify({
          memory: { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 },
          disk: { freeDiskGb: 60, declineRateGbPerHour: 0.5 },
        }),
      };

      // Real `--claim=live-agent:4` — fills the entire LIVE_AGENT_CEILING (4)
      // with genuinely live claims at T. `--claim=live-agent:N`'s own
      // pre-lock probe (probeAvailableCapacity) samples the host exactly
      // like the `--desired-agents` beat does — GREEN memory/disk is
      // supplied here so this claim's own grant does not depend on this
      // test-running machine's real, unpredictable resource state.
      const claimResult = runCli(
        ['--claim=live-agent:4', '--orchestrator-id=orc-real-claim'],
        { ...workDirEnv, ...greenCollectEnv, ARM_FAKE_NOW_MS: String(t0) },
      );
      expect(claimResult.status).toBe(0);
      expect(claimResult.stderr).toBe('');
      expect(JSON.parse(claimResult.stdout).granted).toBe(4);

      // At T + 95s, a SEPARATE orchestrator's --desired-agents beat asks for
      // the full ceiling again, under GREEN memory/disk (so the beat would
      // otherwise be spawn-eligible) and a spawn-rate bucket sized never to
      // be the binding constraint.
      const desiredAgentsResult = runCli(
        ['--orchestrator-id=orc-real-desired', '--desired-agents=4'],
        {
          ...workDirEnv,
          ...greenCollectEnv,
          ARM_FAKE_NOW_MS: String(t0 + 95_000),
          ARM_SPAWN_BUCKET_CONFIG_JSON: JSON.stringify({ capacityTokens: 1000, refillTokensPerMs: 0 }),
        },
      );
      expect(desiredAgentsResult.status).toBe(0);
      expect(desiredAgentsResult.stderr).toBe('');

      const parsed = JSON.parse(desiredAgentsResult.stdout);
      // PRE-FIX (commit 43063437) BEHAVIOUR: main() passed only `{
      // claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS }` (90s) to reserveAdmission,
      // with no `snapshotClaimTtlMs` — reserveAdmission pruned
      // LIVE_AGENT_CLAIM_TYPE's snapshot against that same 90s window, so
      // the 4 claims from T were invisible by T+95s and this beat
      // incorrectly reported the full ceiling (4) as granted. Fixed by
      // wiring `snapshotClaimTtlMs: DEFAULT_CLAIM_TTL_MS` through at this
      // call site — see this suite's file-header note above.
      expect(parsed.liveAgentGrant).not.toBe(4);
      expect(parsed.liveAgentGrant ?? 0).toBe(0);
    },
  );
});
