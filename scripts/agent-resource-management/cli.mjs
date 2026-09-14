#!/usr/bin/env node
// Phase 5 — the real cli.mjs entrypoint an orchestrator runs at each
// beat. Composes Phases 1-4's lib modules into a single command:
//
//   collect() -> takeSample() -> classifyMemoryAxis/classifyDiskAxis
//             -> declareDibs/readDibs (coordination file)
//             -> computePerOrchestratorAllowance/selectPauseCandidate/buildTrafficLight
//             -> print traffic-light JSON to stdout, exit 0
//
// This file is DELIBERATELY excluded from the coverage gate (see
// jest.skills.config.mjs's `!scripts/agent-resource-management/cli.mjs`),
// mirroring every sibling skill's cli.mjs/lib split (model-routing,
// convergence-analysis, ticket-post-mortem). It is exercised black-box, via
// real process spawning, by ./cli.jest.spec.mjs — never imported directly by
// a unit test. See that file for the exact CLI contract this implements.
//
// Full current literal picture (Phase 2, updated Phase 4):
// `buildTrafficLight` (in ./lib/allowance.mjs) can itself produce FOUR
// literals today — spawn-allowed / hold / pause / alert — and this file
// prints whatever it returns unmodified. A RED beat now feeds real
// running-agent data (via `listAgentProcesses`/`collectPsOutput` below) into
// `selectPauseCandidate`, so `{ type: 'pause', pauseCandidate: null }` is an
// honest "nothing to pause" signal, not a placeholder needing a downgrade to
// `hold`. This supersedes the earlier HIGH-5 mitigation, which
// downgraded `pause`/null to `hold` only because running-agent data wasn't
// wired in yet — that downgrade block has been removed now that it is.
// `alert` is the fourth: a disk-RED reading now produces
// `{ type: 'alert', reason: diskResult.reason }` (disk-RED wins the tie even
// when memoryState is also RED), threaded through unmodified exactly like
// the other three — there is no masking or downgrade logic for it either.
// throttle/cleanup are the remaining, different kind of gap: `buildTrafficLight`
// itself never produces them for any input — they're documented-only
// forward-contract shapes at the library level (see Phase 2/its JSDoc
// on `buildTrafficLight`), so they never reach this file at all, and
// there is nothing here for cli.mjs to mask or wire for them yet.
//
// -----------------------------------------------------------------------
// Configurable defaults — ONE place a human tuning these later should look.
// All are hypotheses from the Investigation briefing's §7 (illustrative
// starting points for a 16 GB RAM / 245 GB SSD machine), not settled
// constants. `.claude/skills/agent-resource-management/SKILL.md` points
// here rather than restating these numbers.
//
// The two other config defaults this skill depends on live beside the logic
// they configure, not here, to avoid two competing "sources of truth":
//   - lock staleness (`DEFAULT_LOCK_STALENESS_MS`, 30s) — see
//     ./lib/coordination-file.mjs's module-level constant.
//   - lock retry/backoff (`LOCK_RETRY_DELAY_MS`, `LOCK_MAX_ATTEMPTS`) — same
//     file.
// -----------------------------------------------------------------------

import { execFileSync } from 'node:child_process';
import { promises as fs, appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import { homedir, loadavg, tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';

import {
  takeSample,
  parseVmStat,
  parsePressureLevel,
  parseSwapUsage,
  parseFreeDisk,
  listAgentProcesses,
  isEvictionAuthorized,
  resolveNearestClaudeRootIdentity,
  classifyProcessSnapshotShape,
  PS_HEADER_WITH_ETIME_PATTERN,
} from './lib/recon.mjs';
import { computeCompressionVelocity } from './lib/compression-velocity.mjs';
import { classifyLedgerEntryLiveness, LIVENESS_REASON, LIVENESS_STATUS } from './lib/reconciliation.mjs';
import { gracefulStop } from './lib/graceful-stop.mjs';
import { realEvictKill, realEvictIsAlive } from './lib/real-evict-adapters.mjs';
import { reapAwareIsAlive } from './lib/reap-aware-is-alive.mjs';
import { classifyMemoryAxis, classifyDiskAxis } from './lib/threshold.mjs';
import {
  declareDibs,
  readDibs,
  isSampleFresh,
  isReservedEntry,
  consumeGlobalSpawnTokens,
  claimCapacity,
  releaseCapacity,
  reserveAdmission,
  reconcileMemoryProjectionAdmission,
  readEntryAgentClasses as readAgentClasses,
  getLiveClaimTotal,
  writeSharedSample,
  SHARED_SAMPLE_RESERVED_ID,
  DEFAULT_CLAIM_TTL_MS,
  readAimdCeilingState,
  advanceAimdCeilingState,
  reapConfirmedDeadRecords,
  readLedgerCandidateRecords,
  LockTimeoutError,
  LockLostError,
  // Phase 3 — the disk-axis temp sweep's opt-in. DELIBERATELY NOT
  // `hasEverOptedIn` — which this file deliberately does NOT import for the
  // sweep gate. See `DISK_CLEANUP_ARMED_RESERVED_ID`'s doc block and
  // `diskCleanupArmedForBeat` below (Phase 3 review, High).
  recordDiskCleanupArmed,
  clearDiskCleanupArmed,
  hasDiskCleanupArmed,
  // Phase 3 review (Medium) — the futile-sweep cooldown, i.e. the
  // trigger's memory of its own last outcome.
  isDiskCleanupSuppressed,
  recordDiskCleanupCooldown,
} from './lib/coordination-file.mjs';
// Phase 3 — the budgeted, bounded, confinement-checked async temp sweep
// (Phase 2) and the age horizon it is driven at. Imported statically rather
// than lazily because this file already pays for `lib/cleanup.mjs`'s module
// graph on no other path — the sweep itself is what is conditional, not the
// module.
import {
  sweepStaleTempDirsAsync,
  sweepThrewReport,
  AUTO_TRIGGER_AGE_HOURS,
  SWEEP_COOLDOWN_MS,
  sweepFoundNothingToDo,
} from './lib/cleanup.mjs';
import { computeAimdCeiling } from './lib/aimd-ceiling.mjs';
import {
  computePerOrchestratorAllowance,
  buildTrafficLight,
} from './lib/allowance.mjs';
import { recordObservation, readHistory } from './lib/history.mjs';
import { estimateOperationCost } from './lib/cost-estimate.mjs';
import { computeProjectedPeakMemoryMb } from './lib/memory-projection.mjs';
import { sampleFootprint } from './lib/footprint-sampler.mjs';
import { selectPauseCandidateWithFootprint } from './lib/pause-candidate.mjs';
import { recordFootprintPoll, consumeFootprintState, readFootprintStore } from './lib/footprint-state.mjs';
import { computeLeakVelocity, DEFAULT_LEAK_VELOCITY_THRESHOLDS } from './lib/leak-velocity.mjs';
import {
  probeExecOptions,
  asProbeFailure,
  capacityIfSampleFresh,
} from './lib/probe-bound.mjs';
import { describeClaimDenial, describeStaleSampleDenial } from './lib/claim-denial.mjs';
import {
  enqueueItem,
  peekQueue,
  dequeueItem,
  resolveQueueFilePath,
  ackQueueEntry,
  releaseQueueEntry,
  bindPidToLease,
} from './lib/queue.mjs';
import { claimAndAdmitQueueEntry } from './lib/dispatch.mjs';
// Phase 6 — watchdog beat wiring: composes Phases 1-5 (imported
// here) into the new `--watchdog-beat` standalone CLI flag.
import { isAgentStalled } from './lib/liveness.mjs';
import { detectDeadlock, DEFAULT_DEADLOCK_WINDOW_MS } from './lib/deadlock-tripwire.mjs';
import { escalateAgedQueueEntry, DEFAULT_ESCALATION_BOUND_MS } from './lib/queue-aging-escalation.mjs';
import { runWatchdogBeat } from './lib/watchdog-beat.mjs';
import { readLivenessLogTail } from './lib/liveness-log.mjs';
import { getRealWorktreeMtimeMs } from './lib/worktree-mtime-scanner.mjs';
import { deriveOrchestratorId } from './lib/orchestrator-id.mjs';
import {
  computeNextBackoffState,
  readBackoffState,
  isBackoffDue,
  writeBackoffStateAtomic,
} from './lib/watchdog-backoff.mjs';

/**
 * Memory axis thresholds — briefing §7 "Memory axis".
 *
 * `greenSwapUsedMbBelow`/`redSwapUsedMbAtOrAbove` are ABSOLUTE MB figures
 * sized for a large-swap machine — structurally unreachable on a host with
 * ≤4 GB of total swap (this host: 1 GiB): `redSwapUsedMbAtOrAbove` (10240)
 * can never be hit, and ANY `swapUsedMb` trivially satisfies
 * `greenSwapUsedMbBelow` (4096) regardless of real saturation. Phase 1
 * closed this by adding `classifyMemoryRaw` (`lib/threshold.mjs`) support for
 * RELATIVE, fraction-of-`swapTotalMb` thresholds — additive alongside the
 * absolute ones, guarded to skip cleanly when `swapTotalMb` is 0/non-finite
 * (swap disabled/unparseable). `redSwapUsedFractionOfTotalAtOrAbove`/
 * `greenSwapUsedFractionOfTotalBelow` below are THAT wiring — without them
 * here, `lib/threshold.mjs`'s fractional gates exist but are never consulted
 * by any production call site (they were previously only exercised by
 * `lib/threshold.jest.spec.mjs`'s own custom threshold fixtures, never by
 * this constant), leaving every real beat exactly as unreachable-on-small-
 * swap as before Phase 1 — a Post-Phase-6-review fix, not new Phase 1 scope.
 * 0.9/0.5 are the same illustrative starting-hypothesis values
 * `lib/threshold.jest.spec.mjs`'s own pinned contract test proposed.
 *
 * `greenPressureAtOrBelow`/`redPressureAtOrAbove` are effectively
 * `--claim`/`--query-capacity`-only from `main()`'s own `--desired-agents`/
 * `--heartbeat` spawn-decision beat's point of view as of Phase 4: the
 * unmaskable pressure-WARN pre-check (`DEFAULT_PRESSURE_OVERRIDE_THRESHOLDS`
 * below) returns before this axis is ever classified whenever the raw
 * `pressureLevel >= warnAtOrAbove` (2) — strictly below `redPressureAtOrAbove`
 * (4) — so on that beat, `classifyMemoryRaw` never actually SEES a
 * `pressureLevel` at or above 2 in practice; `redPressureAtOrAbove` stays
 * live for `--claim`/`--query-capacity` (which never run the Phase 4
 * pre-check at all) and for direct `lib/threshold.mjs` callers/tests.
 */
export const DEFAULT_MEMORY_THRESHOLDS = {
  greenPressureAtOrBelow: 1,
  greenSwapUsedMbBelow: 4096,
  redPressureAtOrAbove: 4,
  redSwapUsedMbAtOrAbove: 10240,
  redSwapUsedFractionOfTotalAtOrAbove: 0.9,
  greenSwapUsedFractionOfTotalBelow: 0.5,
};

/** Disk axis thresholds — briefing §7 "Disk axis". */
export const DEFAULT_DISK_THRESHOLDS = {
  greenFreeDiskGbAbove: 30,
  amberHoursToFullBelow: 2,
  redFreeDiskGbBelow: 10,
};

/**
 * Phase 4 — thresholds for the UNMASKABLE pressure pre-check `main()`
 * runs on the raw `pressureLevel` reading, independently of (and strictly
 * BEFORE) `memoryState`/`diskState` classification and `buildTrafficLight`'s
 * own precedence chain. This closes the Investigation's R2 / residual
 * risk: today, `pressureLevel` only ever reaches an admission decision
 * indirectly, blended into `memoryState` via `DEFAULT_MEMORY_THRESHOLDS`
 * above (`redPressureAtOrAbove: 4`) — and a simultaneous disk-RED reading
 * can silently out-vote it, since `buildTrafficLight` checks `diskState ===
 * 'RED'` first and short-circuits straight to `alert` before the
 * memory-RED/pause branch (or any future memory-pressure signal) is ever
 * reached. `warnAtOrAbove`/`criticalAtOrAbove` give `pressureLevel` an
 * independent voice that cannot be masked that way.
 *
 * Deliberately does NOT reuse `DEFAULT_MEMORY_THRESHOLDS.redPressureAtOrAbove`
 * for `criticalAtOrAbove`, even though both are `4` today — these are two
 * different concerns (one axis-classification input among several vs. a
 * standalone hard admission gate) that happen to share a starting value; a
 * future re-tune of one must not silently move the other.
 */
export const DEFAULT_PRESSURE_OVERRIDE_THRESHOLDS = {
  warnAtOrAbove: 2,
  criticalAtOrAbove: 4,
};

/**
 * Hysteresis recovery window — briefing §7 ("sustained return to GREEN for N
 * consecutive samples"). The briefing doesn't pin a number; 3 is this
 * skill's chosen starting hypothesis, tunable like everything else here.
 *
 * `hysteresisRecoverWindowMs` adds a SECOND, wall-clock gate
 * alongside the existing count-based one: recovery to GREEN requires both
 * `hysteresisRecoverConsecutiveGreen` consecutive GREEN samples AND at least
 * this many milliseconds elapsed since the axis's last non-GREEN sample
 * (`./lib/threshold.mjs`'s `nonGreenSince`). Without this, an orchestrator
 * that happens to poll/spawn in rapid succession right after a trip can
 * "recover" in milliseconds even though nothing about the underlying
 * resource pressure has actually had time to settle. 120 seconds (2 minutes)
 * is this skill's starting hypothesis — long enough to rule out a
 * momentary blip while still recovering within a normal work session; like
 * every other DEFAULT_* constant here, tune it once real data is in.
 */
export const DEFAULT_HYSTERESIS_CONFIG = {
  hysteresisRecoverConsecutiveGreen: 3,
  hysteresisRecoverWindowMs: 120 * 1000,
};

/**
 * Effective per-orchestrator agent caps per axis — briefing §3/§7 headroom
 * discussion ("the user typically runs ~3 tickets concurrently"). A rough
 * starting hypothesis, not derived from the full headroom formula the
 * briefing sketches (`(usable_RAM - OS_baseline - reserve) / per_agent_RAM`)
 * — that formula is a documented future refinement, not implemented here.
 */
export const DEFAULT_ALLOWANCE_CONFIG = { maxAgentsMemoryAxis: 3, maxAgentsDiskAxis: 3 };

/**
 * Phase 3 — the hard, ledger-derived ceiling on concurrently LIVE
 * agents across every orchestrator on the host. This REPLACES the flat
 * `DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis` (3) placeholder that
 * `main()`'s `--desired-agents` beat fell back to during the Phase 2
 * interim (see `probeAvailableCapacity`/`allowanceConfig`'s own comments
 * below for where the flat placeholder still applies elsewhere, and where
 * it does not any more) — `main()`'s admission path derives its per-axis cap
 * from `<ceiling> - <live count read fresh from the coordination-file ledger
 * every beat>`, not from a fixed constant or a `ps` process walk.
 *
 * Phase 3 UPDATE: `<ceiling>` above is no longer this flat constant —
 * every call site now resolves the AIMD-adjusted ceiling instead (see the
 * `DEFAULT_AIMD_CEILING_CONFIG`/mixed-version-fleet doc comment immediately
 * below). This constant's only remaining live role is the cold-start seed
 * `readAimdCeilingState` writes into a fresh coordination file and the
 * degraded-read fallback every AIMD call site falls back to on a
 * coordination-file error — see that comment for the full picture.
 *
 * 4 is a starting hypothesis (same "illustrative, not settled" posture as
 * every other DEFAULT_ constant in this file), not a measured value.
 */
export const LIVE_AGENT_CEILING = 4;

/**
 * Phase 3 — the mixed-version-fleet accepted risk: `reserveAdmission`
 * (./lib/coordination-file.mjs) trusts the `ceiling` value ITS CALLER passes
 * in verbatim; it never itself calls `readAimdCeilingState` or cross-checks
 * that value against whatever is currently persisted. Every call site in
 * THIS file now passes the freshly-read (or freshly-computed) AIMD-adjusted
 * ceiling, but an old, pre-existing orchestrator binary running concurrently on
 * the same host would still pass the flat `LIVE_AGENT_CEILING` (4) literal —
 * and be granted admission computed against ITS OWN stale, potentially
 * too-permissive view, regardless of what a newer orchestrator has since
 * persisted (e.g. after a WARN-driven multiplicative decrease below 4). This
 * is a KNOWN, CURRENTLY ACCEPTED rollout risk (pinned by
 * `./cli-aimd-ceiling.jest.spec.mjs`'s "[accepted risk, ] mixed-version
 * fleet" test, run directly against `reserveAdmission`) — closing it would
 * mean `reserveAdmission` itself reading `readAimdCeilingState`, which is
 * deliberately out of scope for this phase. See that test file for the full
 * trace.
 *
 * `LIVE_AGENT_CEILING` itself is left untouched by its AIMD wiring — it
 * remains the cold-start default `readAimdCeilingState` seeds a fresh
 * coordination file with (see `DEFAULT_AIMD_CEILING_STATE`,
 * ./lib/coordination-file.mjs) and the historical fallback this comment
 * documents, not a value any of the four call sites below read directly for
 * admission purposes any more.
 */

/**
 * Phase 3 — AIMD (Additive-Increase/Multiplicative-Decrease) tuning
 * for the shared, ledger-persisted live-agent ceiling (see
 * `./lib/aimd-ceiling.mjs`'s `computeAimdCeiling`, which this config is
 * passed to verbatim). Same "illustrative starting hypothesis, not a
 * measured value" posture as every other DEFAULT_* constant in this file.
 *
 * `sustainedNormalBeatsRequired` (5) is deliberately > 1: a single Normal
 * beat must not itself grow the ceiling (see
 * `./cli-aimd-ceiling.jest.spec.mjs`'s cold-start tests, which pin exactly
 * one beat leaving the ceiling at the flat cold-start default). `floor` (1)
 * keeps a multiplicative decrease from ever reaching 0 and permanently
 * wedging admission; `ceilingMax` (64) is a generous, non-binding upper
 * bound — this host's real constraint is memory/disk pressure, which the
 * WARN/CRITICAL pre-check (independent of this ceiling entirely — see
 * `main()`'s pressure-block short-circuit) already refuses regardless of
 * this value.
 */
export const DEFAULT_AIMD_CEILING_CONFIG = {
  floor: 1,
  ceilingMax: 64,
  increaseStep: 1,
  decreaseFactor: 0.5,
  sustainedNormalBeatsRequired: 5,
};

/**
 * Claim `type` this ceiling's ledger entries are recorded under (see
 * `claimCapacity`/`releaseCapacity`, ./lib/coordination-file.mjs) — a plain
 * string, not a new lock/TTL primitive. Any orchestrator (or future CLI
 * caller) that has genuinely spawned or released a live agent claims or
 * releases against this SAME type via the already-generic
 * `--claim=<type>:<count>` / `--release=<type>:<count>` flags; `main()`'s own
 * `--desired-agents` beat only ever READS this ledger (via a zero-count
 * `claimCapacity` call) to derive remaining headroom — it never claims a slot
 * on a caller's behalf, since admission alone is not the same as a spawn
 * having actually happened.
 *
 * this ledger and `LIVE_AGENT_ADMISSION_CLAIM_TYPE` are no longer
 * mutually blind. `reserveAdmission` derives ITS headroom by subtracting
 * this ledger's live total from the ceiling its caller passes in;
 * symmetrically, a genuine `--claim=live-agent:N` against THIS type
 * (`handleClaim`'s `computeAvailableCapacity`) subtracts
 * `LIVE_AGENT_ADMISSION_CLAIM_TYPE`'s live total from that same ceiling
 * before granting, so an outstanding advisory admission and a genuine spawn
 * can never together exceed it.
 *
 * Phase 3 UPDATE: "the ceiling" both sides agree on is, as of this
 * phase, the AIMD-adjusted value (`readAimdCeilingState`/`aimdCeiling`) —
 * `LIVE_AGENT_CEILING` itself is no longer what either side reads for this
 * purpose (see that constant's own doc comment above).
 */
export const LIVE_AGENT_CLAIM_TYPE = 'live-agent';

/**
 * A SECOND claim type — never seeded, claimed, or released via the public
 * `--claim`/`--release` flags (both `handleClaim` and `handleRelease`
 * explicitly reject this type with exit code 2 — see the ` review
 * follow-up` guard at each call site; the invariant is mechanically
 * enforced, not merely documented here), only ever written by `main()`'s own
 * `--desired-agents` beat (through `reserveAdmission`) — that this beat uses
 * to atomically resolve concurrent admission races AGAINST a snapshot of
 * `LIVE_AGENT_CLAIM_TYPE`'s remaining headroom, without itself mutating
 * `LIVE_AGENT_CLAIM_TYPE`'s own ledger entry.
 *
 * while nothing outside `main()` ever WRITES this ledger, it is no
 * longer read-blind from the outside — `handleClaim`'s
 * `computeAvailableCapacity` (this file, `LIVE_AGENT_CLAIM_TYPE`'s branch)
 * reads this type's live total (via `getLiveClaimTotal`, the SAME
 * TTL-pruned "live" notion `reserveAdmission` itself uses) so a real
 * `--claim=live-agent:N` correctly accounts for outstanding admission holds
 * that haven't yet been converted into a genuine spawn. Symmetrically, once
 * that conversion happens — a genuine `granted > 0` from a
 * `--claim=live-agent:N` call — `handleClaim` also RELEASES (up to `granted`
 * of) the calling orchestrator's own hold on THIS ledger via
 * `releaseCapacity`, in a second lock cycle immediately following the claim.
 * Without that release, the same agents would be double-counted (once here,
 * once on the real ledger) for up to a full `DEFAULT_FRESHNESS_WINDOW_MS`,
 * needlessly starving every OTHER orchestrator's `othersHeld` subtraction in
 * the meantime — see that call site's own comment for the full rationale.
 *
 * Why a second type, rather than main() claiming directly against
 * `LIVE_AGENT_CLAIM_TYPE`: `LIVE_AGENT_CLAIM_TYPE` is the ledger a caller
 * writes to ONLY once a live agent has genuinely started (or explicitly
 * released a finished one) — `main()`'s own admission DECISION is not that
 * event, and must not be indistinguishable from it on disk. If `main()`
 * claimed `LIVE_AGENT_CLAIM_TYPE` directly on every `spawn-allowed` verdict,
 * that reservation would persist (subject only to
 * `DEFAULT_FRESHNESS_WINDOW_MS`) regardless of whether the caller ever
 * actually spawned anything, permanently miscounting the ledger every other
 * reader (including a future caller's own `--claim`/`--release` against the
 * SAME type) relies on as ground truth.
 *
 * What this second ledger buys instead: two (or more) beats racing for the
 * SAME last slot(s) both read `LIVE_AGENT_CLAIM_TYPE`'s remaining headroom
 * (a cheap, lock-protected, zero-count `claimCapacity` read — see `main()`),
 * then each atomically claims against THIS type using that snapshot as a
 * FIXED per-race capacity ceiling. `claimCapacity`'s own accumulation
 * (`alreadyGrantedTotal`, read fresh under lock on every call against this
 * SAME type) is what makes the race genuinely exclusive — whichever racer's
 * claim lands first against this ledger correctly starves the other, the
 * exact `writeEntriesAtomic`/ fencing-backed atomicity `--claim`/
 * `--release` already rely on, reused rather than reimplemented. TTL-pruned
 * against the SAME `DEFAULT_FRESHNESS_WINDOW_MS` freshness window as
 * `LIVE_AGENT_CLAIM_TYPE` (see that constant's own doc comment), so a run of
 * successful admissions this type accumulates ages back out on its own —
 * full reconciliation between this bookkeeping and whether those
 * admissions' agents are still genuinely alive is explicitly out of scope
 *; this type only ever needs to outlive one race window, and TTL
 * decay is the documented, deferred answer for anything longer.
 */
export const LIVE_AGENT_ADMISSION_CLAIM_TYPE = 'live-agent-admission';

/**
 * A THIRD, independent claim type — never seeded, claimed,
 * or released via the public `--claim`/`--release` flags (mirrors
 * `LIVE_AGENT_ADMISSION_CLAIM_TYPE`'s own internal-only posture), only ever
 * written by `main()`'s own `--desired-agents` beat when
 * `--user-attests-idle` is set and this beat's verdict would otherwise be
 * `hold`. It backs a fixed, single-slot `claimCapacity` reservation
 * (`computeAvailableCapacity: () => 1`) that gives "admit exactly one
 * idle-probe agent, refuse a second one already in flight" for free, reusing
 * the same atomic deny-the-remainder ledger primitive `--claim`/`--release`
 * already rely on rather than inventing a parallel locking mechanism. Unlike
 * `LIVE_AGENT_ADMISSION_CLAIM_TYPE`, this ledger has no matching
 * `releaseCapacity` call anywhere in this file — an admitted idle-probe's
 * slot is intentionally left outstanding until it ages out under
 * `DEFAULT_CLAIM_TTL_MS`, since there is no signal in this phase's scope for
 * "the probe has finished".
 */
export const IDLE_PROBE_CLAIM_TYPE = 'idle-probe';

/**
 * Headroom-formula constants (Phase 5 Part A) — feed
 * `computeHeadroomCap({ freeRamMb, osBaselineMb, reserveMb, perAgentRamMb })`
 * (./lib/allowance.mjs), which derives the memory axis's GREEN cap from a
 * beat's live `freeRamMb` sample instead of the flat
 * `DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis` constant.
 *
 * Amendment: `computeHeadroomCap` is currently UNUSED by any
 * live admission path in this file — Phase 2 retired all three call sites
 * (main()'s per-beat spawn decision, `--query-capacity`'s per-type
 * `freeCapacity`, and `--claim`'s `availableCapacity`) back onto the flat
 * `DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis` placeholder (see the RC-3
 * describe block in cli.jest.spec.mjs and the Phase 2 commits). Phase 3
 * has since shipped and did NOT restore this formula — it installed a
 * ledger-derived live-agent CEILING (`LIVE_AGENT_CEILING`) instead, an
 * entirely different mechanism (a hard, atomically-reserved count cap, not a
 * live-`freeRamMb`-derived headroom estimate). This constant is retained
 * only as a documented historical value — `cost-estimate.mjs`'s doc-honesty
 * tests are its only remaining consumer.
 * Same "illustrative starting point, not settled" posture as every other
 * DEFAULT_* constant in this file — these three numbers are a documented
 * starting hypothesis (the per-agent footprint, a fixed OS/background-process
 * baseline, and a safety reserve), consistent with its own
 * Investigation/Build Plan comments on the target 16GB-RAM Mac mini M4 host,
 * not a measured/tuned value.
 */
export const DEFAULT_HEADROOM_CONFIG = { perAgentRamMb: 350, osBaselineMb: 2048, reserveMb: 2048 };

/**
 * Global spawn-rate token bucket config — shared across ALL
 * orchestrators on the host via the coordination file's reserved
 * `__global-spawn-rate-bucket__` entry (see
 * `./lib/coordination-file.mjs`'s `consumeGlobalSpawnTokens`). This caps the
 * RATE of new agent spawns host-wide, independent of the per-axis headroom
 * caps above (`DEFAULT_ALLOWANCE_CONFIG`) — a burst of spawns can starve the
 * machine even while memory/disk both read GREEN, because spawn cost
 * (process startup, initial model calls, disk churn) front-loads before
 * steady-state resource use shows up in a sample.
 *
 * 10 tokens, refilling at 1 token/minute, is a starting hypothesis (same
 * "illustrative, not settled" posture as every other DEFAULT_* constant in
 * this file): enough burst capacity for a normal multi-orchestrator session
 * to spawn its usual handful of agents back-to-back, while a sustained flood
 * of spawn requests is throttled back to roughly one every 60s.
 *
 * Overridable via `ARM_SPAWN_BUCKET_CONFIG_JSON` (test seam + operator
 * escape hatch), matching the `ARM_FAKE_*` naming convention — see
 * `resolveSpawnBucketConfig()` below.
 */
export const DEFAULT_SPAWN_BUCKET_CONFIG = { capacityTokens: 10, refillTokensPerMs: 1 / 60_000 };

/**
 * How long a declared dibs entry is trusted before being treated as a dead
 * orchestrator's stale claim (see ./lib/coordination-file.mjs's
 * `livenessThresholdMs` option). 15 minutes is long enough to span a normal
 * beat cadence (orchestrators call this CLI "after each sub-agent's task
 * completes", not on a fixed short timer) while still reclaiming a crashed
 * orchestrator's slot well within a work session.
 */
export const DEFAULT_LIVENESS_THRESHOLD_MS = 15 * 60 * 1000;

/**
 * Phase 2 review, High finding — the catch-all agent class a
 * `--desired-agents` beat is priced/counted under when it omits
 * `--agent-class` entirely. Prior to this fix, an unlabelled beat was
 * EXEMPT from the memory-projection gate on both sides: it never supplied a
 * candidate class to price (so the gate never ran for it), and its dibs
 * entry carried no `agentClasses`/`agentClass` field at all (so no OTHER
 * beat's sum ever counted its real memory consumption either) — this
 * reproduced the original FINDING-5 race, unmodified, for any orchestrator
 * that simply never adopted `--agent-class`.
 *
 * Folding every unlabelled beat into ONE shared catch-all class closes that
 * gap without a breaking mandatory-flag change: an unlabelled beat is now
 * priced exactly like any other class with no history (the same
 * conservative cold-start estimate `computeProjectedPeakMemoryMb` already
 * applies to a genuinely-new class), and its declared dibs entry is visible
 * to every other beat's `runningClasses` sum, whether that other beat is
 * itself labelled or unlabelled. Never used as a REAL `--agent-class` value
 * by any legitimate caller (the double-underscore convention mirrors
 * `isReservedEntry`'s orchestrator-id sentinel shape, though this is an
 * agent-class value, not an orchestratorId, so `isReservedEntry` itself does
 * not — and must not — apply to it).
 *
 * [accepted risk, ] mixed-version fleet: this whole gate — including
 * this catch-all — only runs inside a Phase-2-or-later `cli.mjs` binary. An
 * orchestrator still running a pre-Phase-2 binary never calls the
 * memory-projection gate at all: it has no `--agent-class`/
 * `--running-agent-classes` flags to pass, and its dibs entry carries
 * neither `agentClasses` nor `agentClass` — not even this
 * `UNLABELLED_AGENT_CLASS` sentinel — so its real memory footprint stays
 * invisible to every OTHER (upgraded) orchestrator's projected sum until it
 * upgrades. This is the same shape as the `LIVE_AGENT_CEILING` mixed-version
 * gap already accepted for (see that constant's own doc comment
 * below), applied to this gate specifically: a stale binary that predates a
 * gate is outside that gate's reach by construction, not something the new
 * gate can retroactively enforce on a caller that never invokes it. Pinned
 * by `./cli-memory-projection-mixed-version-fleet.jest.spec.mjs`, mirroring
 * `./cli-aimd-ceiling.jest.spec.mjs`'s own "[accepted risk, ]
 * mixed-version fleet" test.
 */
export const UNLABELLED_AGENT_CLASS = '__unlabelled__';

// Normalizes a raw `agentClasses`/legacy `agentClass` dibs-entry field into a
// flat array of class-name strings — `[]` when neither is a usable shape.
// Back-compat seam (Phase 2 review, Medium finding): entries declared
// before this fix shipped carry only a single `agentClass` string field, not
// an `agentClasses` array — both shapes must keep being read correctly for as
// long as pre-fix entries can still be live on disk.
//
// review, Low finding, maintainability — imported as `readAgentClasses`
// from `./lib/coordination-file.mjs` (as `readEntryAgentClasses`) rather than
// duplicated verbatim in this file: the two copies had identical bodies kept
// in sync only by comment, an easy thing for a future edit to silently drift
// on. See that module's own doc comment for the full contract.

/**
 * Freshness window for reusing another orchestrator's recent machine sample
 * instead of re-sampling (briefing: "debounce sampling ... configurable
 * freshness window"). Wired into `main()`: before calling
 * `collect()`, `main()` checks whether a DIFFERENT orchestrator already
 * sampled the machine within this window (via the coordination file's
 * reserved shared-machine-sample entry, see
 * `SHARED_SAMPLE_RESERVED_ID` (imported from `./lib/coordination-file.mjs`,
 * the single source of truth for this reserved id)) and reuses that reading
 * entirely, skipping this beat's own shell-outs when still fresh.
 */
export const DEFAULT_FRESHNESS_WINDOW_MS = 90 * 1000;

/**
 * How stale `--claim`'s hoisted host sample may be — measured from the
 * instant the sample was TAKEN to the instant the grant decision is actually
 * made inside the fencing lock. Not from handler entry: the probe's
 * own bounded worst case (~16s across `collect()`'s retried set) must not be
 * charged against a budget sized for the lock wait.
 *
 * Before the hoist, the sample was taken INSIDE the lock, so it was always
 * ~0ms old at decision time. Taking it before the lock buys back the
 * blocking shell-outs that every other orchestrator was queued behind — but
 * it introduces a sample-to-decision gap the width of the lock wait, and the
 * direction that gap fails in is OVER-admission: if pressure rose while this
 * caller waited, a stale sample over-states free RAM. So the gap is bounded
 * and fails closed (`granted: 0`) rather than left implicit.
 *
 * 20s is chosen against two hard edges, not picked for roundness:
 *   - It must exceed the lock wait it is measured across, or it would deny
 *     grants under ORDINARY contention — no staleness, no crash, just a busy
 *     file — turning a safety net into a functional regression. Note this
 *     edge is NOT cleared by `acquireLock`'s default ceiling, which is
 *     10-20s of retry sleep alone plus per-attempt syscalls (see
 *     coordination-file.mjs; an earlier version of that comment understated
 *     it as "~10-15s"). That is why `--claim` passes its own, tighter
 *     `DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS` instead of inheriting the default —
 *     the two constants are sized together, and changing one without the
 *     other reintroduces the silent-denial case.
 *   - It must stay under `DEFAULT_LOCK_STALENESS_MS` (30s), so a sample can
 *     never outlive the lock horizon the whole coordination scheme is built
 *     on.
 * With the paired attempt ceiling below, it never fires on the happy path; it
 * exists to make the residual bounded and visible rather than silent.
 */
export const DEFAULT_CLAIM_SAMPLE_FRESHNESS_MS = 20 * 1000;

/**
 * How long `--claim` will wait for the coordination lock before giving up
 *. Paired with `DEFAULT_CLAIM_SAMPLE_FRESHNESS_MS` above — read them
 * together or neither makes sense.
 *
 * `acquireLock`'s default ceiling (2000 attempts) can spend 10-20s of sleep
 * plus syscall time, which does not fit inside a 20s freshness window. Left
 * at the default, a `--claim` that waited out heavy contention and DID
 * acquire the lock could then be denied on sample staleness — and until
 * its review round 2 that denial WAS silent; it now warns via
 * `describeStaleSampleDenial`, but a warning is a diagnostic, not a reason to
 * stop sizing the two constants to make the case unreachable in the first
 * place. 800 attempts caps the retry sleep at
 * ~8s (mean ~6s), which the freshness window clears with real margin.
 *
 * The failure mode this trades INTO is strictly better: a claim that cannot
 * get the lock in ~8s now fails with `LockTimeoutError`, which `handleClaim`
 * already converts to `granted: 0` AND a stderr warning naming what degraded
 * — since its review round 2, one that names THIS constant as the
 * suspect (see `lib/claim-denial.mjs`). Same fail-closed outcome, but
 * observable instead of silent — and giving up after ~8s rather than ~20s is
 * the correct posture for a one-shot command an orchestrator re-runs every
 * beat (~15-30s). Waiting out most of a beat for a lock is how contention is
 * sustained, not relieved.
 *
 * MEASURED, not asserted (review round 2). The concern this ceiling
 * has to answer is whether cutting the wait from ~10-20s to ~4-8s makes
 * `--claim` fail closed more often under real contention. It was measured at
 * this host's real operating point — a configured ceiling of FOUR concurrent
 * agents — against a NORMAL-width critical section, not the deliberately
 * widened one (20k filler entries, 5ms staleness) that
 * `lib/coordination-file.jest.spec.mjs`'s post-reclaim race uses to force
 * lock loss on purpose. Lock-contention denials, by concurrency:
 *
 *     4-way   x 30 rounds (120 claims)  — 0 lock denials, worst round  75ms
 *     8-way   x 15 rounds (120 claims)  — 0 lock denials, worst round  86ms
 *    16-way   x 15 rounds (240 claims)  — 0 lock denials, worst round 136ms
 *    48-way   x  5 rounds (240 claims)  — 0 lock denials, worst round 393ms
 *
 * Denials DO appear at 16- and 48-way (75 and 185 respectively) and are all
 * capacity denials — 11 free slots against 16 and 48 claimants — which is
 * the mechanism working, not the ceiling biting. Lock-contention denials are
 * zero throughout.
 *
 * The margin, stated as the ratio that matters: at the real ceiling of 4, an
 * entire round of contention drains in ~75ms against a ~4000ms floor on this
 * constant's budget — better than 50x. Even at 48-way, twelve times the
 * configured ceiling, the whole queue drains in ~393ms, an order of
 * magnitude inside budget. 800 is not merely adequate for a ceiling of 4; it
 * is generous by roughly two orders of magnitude, and the reviewer's
 * fail-closed-more-often concern does not materialise at any concurrency
 * this host can actually reach.
 *
 * `claim.jest.spec.mjs`'s "4 concurrent --claim invocations at the real
 * configured agent ceiling" test pins the 4-way row so this stays evidence
 * rather than a story about a measurement someone once took.
 */
export const DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS = 800;

/**
 * Recommended polling cadence for an orchestrator's own wall-clock timer
 * driving repeated `--heartbeat` invocations, DISTINCT from a real
 * once-per-sub-agent-task beat. Not enforced by cli.mjs itself — this file
 * remains a one-shot, non-looping process; an orchestrator's own scheduler is
 * responsible for actually firing on this cadence. 20 seconds sits inside
 * `.claude/skills/agent-resource-management/SKILL.md`'s ~15-30s starting
 * hypothesis: frequent enough to catch a fast-moving RED excursion between
 * real beats, not so frequent it meaningfully adds process-spawn overhead.
 */
export const DEFAULT_HEARTBEAT_INTERVAL_MS = 20 * 1000;

/**
 * Recommended polling cadence for an orchestrator's own wall-clock timer
 * driving repeated `--poll-footprint` invocations. Not enforced by cli.mjs itself —
 * this file remains a one-shot, non-looping process; an orchestrator's own
 * scheduler is responsible for actually firing `--poll-footprint` on this
 * cadence. This follows exactly the same pattern as
 * `DEFAULT_HEARTBEAT_INTERVAL_MS` above — its established precedent for a
 * timer-driven, non-self-scheduled cadence constant — just for the
 * `--poll-footprint` beat instead of `--heartbeat`. 20 seconds sits inside
 * the documented 10-30s bound (`MIN_FOOTPRINT_POLL_INTERVAL_MS` /
 * `MAX_FOOTPRINT_POLL_INTERVAL_MS` below): frequent enough to catch a
 * fast-moving footprint change between real beats, not so frequent it
 * meaningfully adds process-spawn overhead.
 */
export const DEFAULT_FOOTPRINT_POLL_INTERVAL_MS = 20 * 1000;

/**
 * Lower bound of the documented, recommended `--poll-footprint` polling
 * cadence. Not enforced by cli.mjs itself — see
 * `DEFAULT_FOOTPRINT_POLL_INTERVAL_MS` immediately above for why this file
 * never self-schedules and who owns firing on this cadence.
 */
export const MIN_FOOTPRINT_POLL_INTERVAL_MS = 10 * 1000;

/**
 * Upper bound of the documented, recommended `--poll-footprint` polling
 * cadence. Not enforced by cli.mjs itself — see
 * `DEFAULT_FOOTPRINT_POLL_INTERVAL_MS` immediately above for why this file
 * never self-schedules and who owns firing on this cadence.
 */
export const MAX_FOOTPRINT_POLL_INTERVAL_MS = 30 * 1000;

/**
 * Margin applied to `MAX_FOOTPRINT_POLL_INTERVAL_MS` when deciding whether a
 * naively-reconstructed `(lastPolledAt - firstPolledAt) / (trajectory.length
 * - 1)` poll interval is itself implausible (see
 * `computeLeakBlocksForBeat`'s own doc comment for the full rationale). Real
 * cadence jitter can occasionally push a genuine interval a little above
 * `MAX_FOOTPRINT_POLL_INTERVAL_MS` without indicating truncation; genuine
 * `TRAJECTORY_CAP` truncation inflates the naive interval far beyond that —
 * by however much history was evicted, typically several multiples of the
 * cadence bound. A margin comfortably wider than ordinary jitter avoids
 * false-flagging a slightly-slow-but-real cadence as "implausible" while
 * still reliably catching genuine truncation.
 */
export const PLAUSIBLE_INTERVAL_MARGIN = 1.5;

// Reserved orchestratorId this module uses to share a cached machine sample
// via the coordination file, debouncing repeated `collect()` shell-outs
// across independent orchestrator processes within DEFAULT_FRESHNESS_WINDOW_MS
//. Follows the general-purpose double-underscore reserved-
// entry convention `isReservedEntry` (./lib/coordination-file.mjs) matches —
// never collides with a real orchestrator id, which callers never spell with
// a leading+trailing `__`. Imported as `SHARED_SAMPLE_RESERVED_ID` from
// ./lib/coordination-file.mjs (the single source of truth for this reserved
// id — see that module's own doc comment) rather than redeclared here, so
// this file and coordination-file.mjs can never silently desync on a future
// rename (DRY follow-up).

/**
 * Recency window for trusting a persisted prior disk sample when computing a
 * genuine `declineRateGbPerHour` trend — see
 * ./disk-trend.jest.spec.mjs's header comment for the full design rationale.
 * Deliberately a NEW, distinct constant from `DEFAULT_FRESHNESS_WINDOW_MS`
 * (90s, governs whether a whole *reading* can be skip-sampled/reused verbatim
 * by a different orchestrator's beat) — a prior disk sample can be too STALE
 * to trust for a rate computation while still being usable, if reused, for
 * the ordinary freshness-window debounce. 15 minutes mirrors
 * `DEFAULT_LIVENESS_THRESHOLD_MS`'s own "long enough to span a normal beat
 * cadence" rationale, but is a genuinely independent constant, not a reuse of
 * that one — the two govern unrelated concerns (dibs-entry liveness vs.
 * trend-sample recency) and must be free to diverge in value later.
 */
export const DEFAULT_DISK_TREND_RECENCY_WINDOW_MS = 15 * 60 * 1000;

/**
 * Noise-tolerance dead zone for `computeDiskTrend` (Phase 6 review,
 * Medium #3; corrected in the Phase 6 disk-decline-trend review, High —
 * flawed noise-epsilon derivation). `freeDiskGb` is reported as a whole-GB
 * integer (`df -g` upstream in `lib/recon.mjs`'s `parseFreeDisk`), so any two
 * real samples can differ by a routine ±1GB of quantization noise (temp
 * files, log rotation, Spotlight indexing) with nothing meaningful actually
 * trending. Without a dead zone, that ±1GB noise alone flips `trend` between
 * `'declining'` and `'recovering'` beat-to-beat even when free disk is
 * genuinely flat.
 *
 * This is deliberately a raw GB delta, NOT a rate (GB/hour) threshold. An
 * earlier version of this constant derived a rate epsilon from
 * `DEFAULT_DISK_TREND_RECENCY_WINDOW_MS` (1GB spread across the full
 * 15-minute recency window = 4 GB/hour) — but `rate = delta / elapsedHours`
 * GROWS without bound as the real elapsed gap between two fresh samples
 * shrinks below that window, and realistic beat-to-beat gaps in this system
 * (as short as `DEFAULT_FRESHNESS_WINDOW_MS`, ~90s, or
 * `DEFAULT_HEARTBEAT_INTERVAL_MS`, ~20s) are almost always far shorter than
 * 15 minutes. A routine ±1GB step at a 90s gap produces `1 / (90 / 3600) ≈ 40
 * GB/hour` — ten times the old 4 GB/hour epsilon — so the old rate-based
 * epsilon let single-GB quantization noise leak through as a reported
 * `'declining'`/`'recovering'` trend for almost every realistic beat pair,
 * reproducing the exact flapping bug Phase 6 was meant to close. A raw GB
 * delta dead zone has no such dependency on elapsed time: a single
 * quantization step is not a trend, no matter how far apart the two samples
 * were taken, so the check is applied to the un-divided `(priorFreeDiskGb -
 * currentFreeDiskGb)` difference BEFORE it is turned into a rate. `computeDiskTrend`
 * compares with `<=`, not strict `<` (Phase 6 final review, Medium — epsilon
 * comparator can't dead-zone real integer-GB noise): because real
 * `freeDiskGb` values are always whole-GB integers, a strict `<` comparison
 * against a 1GB epsilon is mathematically equivalent to `rawDeltaGb === 0` on
 * real data — a genuine ±1GB step (exactly the routine quantization noise
 * this dead zone exists to catch) is never `< 1`, so it would never actually
 * be dead-zoned. Using `<=` instead means a delta that reaches the full 1GB
 * (one whole quantization step) IS treated as noise; only a delta that
 * exceeds a full step is real signal. This does not affect
 * `classifyDiskAxis`'s steep-decline AMBER check, which already uses a
 * median-smoothed rolling window (Phase 5,) — this dead zone
 * exists purely so `diskTrend.trend` itself is a reliable signal.
 *
 * @type {number}
 */
export const DISK_TREND_NOISE_EPSILON_GB = 1;

/**
 * Computes a genuine disk-decline trend from two real, timestamped
 * `freeDiskGb` readings. Never fabricates a rate: returns
 * `{ trend: 'insufficient-data', declineRateGbPerHour: 0 }` (never `NaN`)
 * whenever there's no usable prior sample, the prior sample predates the
 * current one by a non-positive gap (clock anomaly), or the gap exceeds
 * `recencyWindowMs`. `0` here reproduces the exact pre-Phase-6 rate value for
 * a beat with no trend evidence, so `classifyDiskAxis`'s steep-decline AMBER
 * check is never affected by an "unknown" trend — see this constant's own
 * doc comment and disk-trend.jest.spec.mjs's header comment for the full
 * rationale.
 *
 * A raw GB delta at or below `DISK_TREND_NOISE_EPSILON_GB` (see that
 * constant's doc comment for the derivation) is treated as noise, not a real
 * trend, and reported as `'stable'` with `declineRateGbPerHour: 0` — matching
 * the exact-zero `'stable'` shape below, since a below-noise-floor delta is
 * no more meaningful than an exact zero. The comparison is `<=`, not strict
 * `<`: a delta that reaches the FULL epsilon (e.g. a routine 1GB step) is
 * dead-zoned as noise — only a delta that EXCEEDS one whole quantization step
 * is treated as real signal. This check happens BEFORE the delta is divided
 * by elapsed time, so it is immune
 * to elapsed-time variance entirely (see that constant's doc comment).
 * Otherwise computes a signed rate from `(prior - current) / elapsedHours`:
 * positive means free space is shrinking (`'declining'`), negative means
 * it's growing (`'recovering'`).
 *
 * @param {number | null | undefined} currentFreeDiskGb
 * @param {{ freeDiskGb?: number, sampledAt?: number } | null | undefined} priorDiskSample
 * @param {number} now
 * @param {number} recencyWindowMs
 * @returns {{ trend: 'insufficient-data'|'declining'|'stable'|'recovering', declineRateGbPerHour: number }}
 */
function computeDiskTrend(currentFreeDiskGb, priorDiskSample, now, recencyWindowMs) {
  const priorFreeDiskGb = priorDiskSample?.freeDiskGb;
  const priorSampledAt = priorDiskSample?.sampledAt;

  if (
    !Number.isFinite(currentFreeDiskGb) ||
    !Number.isFinite(priorFreeDiskGb) ||
    !Number.isFinite(priorSampledAt)
  ) {
    return { trend: 'insufficient-data', declineRateGbPerHour: 0 };
  }

  const elapsedMs = now - priorSampledAt;
  if (elapsedMs <= 0 || elapsedMs > recencyWindowMs) {
    return { trend: 'insufficient-data', declineRateGbPerHour: 0 };
  }

  const rawDeltaGb = priorFreeDiskGb - currentFreeDiskGb;
  if (Math.abs(rawDeltaGb) <= DISK_TREND_NOISE_EPSILON_GB) {
    return { trend: 'stable', declineRateGbPerHour: 0 };
  }

  const elapsedHours = elapsedMs / (1000 * 60 * 60);
  const declineRateGbPerHour = rawDeltaGb / elapsedHours;

  if (declineRateGbPerHour > 0) return { trend: 'declining', declineRateGbPerHour };
  return { trend: 'recovering', declineRateGbPerHour };
}

// ---------------------------------------------------------------------------
// Argv parsing — supports `--flag=value` (the documented contract) and
// `--flag value`, mirroring scripts/model-routing/cli.mjs's parseArgs.
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const flags = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!arg.startsWith('--')) continue;

    const eqIndex = arg.indexOf('=');
    if (eqIndex !== -1) {
      flags[arg.slice(2, eqIndex)] = arg.slice(eqIndex + 1);
      continue;
    }

    const key = arg.slice(2);
    const next = argv[i + 1];
    if (next === undefined || next.startsWith('--')) {
      flags[key] = true;
    } else {
      flags[key] = next;
      i += 1;
    }
  }
  return flags;
}

// ---------------------------------------------------------------------------
// printDebugDiagnostics — `--debug`/`--verbose`, a
// diagnostic-only escape hatch onto stderr, never onto stdout. Mirrors the
// "record, do not gate" convention already established at the
// `compressionVelocity`/`loadAverage`/`compressedMb` call sites above:
// this output is for a HUMAN reading the terminal, never for an
// orchestrator to parse — stdout stays the single JSON decision contract
// every caller does `JSON.parse(stdout)` on, and nothing here may ever be
// read back by this file or by any caller. Prints whatever raw memory/disk
// fields are actually present on `rawSample` — it must never throw just
// because a given beat type (e.g. `--claim`/`--release`) didn't populate
// every field `classifyMemoryRaw`/`classifyDiskAxis` care about.
//
// @param {{ stale?: boolean, memory?: Record<string, unknown>, disk?: Record<string, unknown> }} rawSample
// ---------------------------------------------------------------------------
function printDebugDiagnostics(rawSample) {
  const memory = rawSample?.memory ?? {};
  const disk = rawSample?.disk ?? {};
  // `stale` is a WHOLE-SAMPLE flag from `takeSample()` (`lib/recon.mjs`) —
  // `rawSample.stale`, never `rawSample.memory.stale`/`rawSample.disk.stale`
  // (those nested paths are never set by the real collection pipeline;
  // `staleMemoryMarker()`/`staleDiskMarker()` degrade to null/NaN fields
  // instead). Reading the nested path here would silently print `false`
  // even on a genuinely stale sample — exactly the Case 2 scenario
  // this flag exists to help diagnose.
  const stale = rawSample?.stale === true;
  console.error(
    '[agent-resource-management debug] memory:',
    JSON.stringify({
      pressureLevel: memory.pressureLevel,
      swapUsedMb: memory.swapUsedMb,
      swapTotalMb: memory.swapTotalMb,
      stale,
    }),
  );
  console.error(
    '[agent-resource-management debug] disk:',
    JSON.stringify({
      freeDiskGb: disk.freeDiskGb,
      stale,
    }),
  );
}

// ---------------------------------------------------------------------------
// resolveOrchestratorIdFlag — the PRODUCER side of 
// decision 2, and the only sanctioned way an orchestrator obtains its
// `--orchestrator-id`.
//
// WHY THIS EXISTS AT ALL. Phase 2 shipped `deriveOrchestratorId` and Phase 3
// wired it into the hook — the CONSUMER side. Nothing was ever built on the
// producer side, so SKILL.md could only say "call `deriveOrchestratorId(...)`
// and pass its return value" in prose, with no mechanism behind it. An LLM
// orchestrator cannot run a Node function by reading a sentence about it, so
// in practice every session kept passing a hand-picked literal, the hook's
// independently-derived key never matched it, `hasEverOptedIn` returned
// `false` forever, and the gate took its tier-1 fail-open path on every gated
// spawn — permanently, and silently. That is precisely the
// "silently never fires" shape this whole feature exists to prevent, and it
// was reachable only by looking at the feature as a composed whole.
//
// `--session-id=<id>` is therefore the single derivation point for every real
// orchestrator caller. Because the derivation happens INSIDE this process, the
// orchestrator and the hook can only ever agree, or be wrong together in the
// same direction — never drift apart independently, which is the property the
// gate's correctness rests on. There is deliberately no second way in: no
// caller is asked to run the helper itself and paste a string.
//
// SESSION-ID ACQUISITION — empirically unverified, best available, and
// deliberately correctable. The documented invocation is
// `--session-id="$CLAUDE_CODE_SESSION_ID"`: the SHELL reads the env var, so
// the var's name is visible in every copy-pasteable example rather than hidden
// inside this file, and a wrong guess is fixed by editing one shell word
// rather than shipping code. On this host (2026-09-06) `CLAUDE_CODE_SESSION_ID`
// holds a bare uuid that matches both the session's own working-directory
// naming under `~/.claude/projects/<project>/` and the `<uuid>.jsonl`
// transcript file Claude Code writes for it — and the PreToolUse payload's
// `transcript_path` points at exactly that file. That is strong corroboration,
// NOT proof that it equals the payload's `session_id` field: confirming that
// would require installing a payload-logging hook on a live session, which was
// not done. Treat this the same way as this feature's `SPAWN_TOOL_NAMES`
// matcher — empirically checked, worth re-checking after any Claude Code
// upgrade. (`CLAUDE_CODE_HOST_SESSION_ID` also exists on this host but carries
// a `local_` prefix and a DIFFERENT uuid; it is a host-level wrapper id, and is
// deliberately not used.)
//
// If the guess is wrong the failure is bounded and non-regressive: the
// orchestrator declares dibs under key A, the hook derives key B, the lookup
// misses, and the gate fails OPEN exactly as it does today for a session that
// never opted in. It cannot mis-gate a DIFFERENT session, because
// `deriveOrchestratorId` is injective over distinct session ids.
//
// The flag is resolved ONCE here, before any dispatch, so every subcommand
// that reads `flags['orchestrator-id']` (~20 of them) inherits it without a
// per-handler change and without any of them growing a second derivation.
//
// @param {Record<string, string|true>} flags mutated in place: on success,
//   `flags['orchestrator-id']` is set to the derived id.
// @returns {{ ok: true } | { ok: false, message: string }} a rejection reason
//   rather than an exit, so `main()` keeps sole ownership of the exit code.
// ---------------------------------------------------------------------------

function resolveOrchestratorIdFlag(flags) {
  const rawSessionId = flags['session-id'];
  if (rawSessionId === undefined) return { ok: true };

  // Both supplied is a HARD conflict, never a silent precedence rule. Picking a
  // winner would let a caller believe it declared dibs under the derived id
  // while it actually declared under the literal (or vice versa) — the same
  // undetectable divergence this flag was added to eliminate.
  if (flags['orchestrator-id'] !== undefined) {
    return {
      ok: false,
      message:
        'agent-resource-management: --session-id and --orchestrator-id cannot be combined — --session-id ' +
        'DERIVES the orchestrator-id (design decision 2), so supplying both would silently discard one of ' +
        'them. Pass --session-id="$CLAUDE_CODE_SESSION_ID" alone.\n',
    };
  }

  // A bare `--session-id` parses to `true`; `--session-id=` with an unset
  // `$CLAUDE_CODE_SESSION_ID` parses to `''`. Both are caught by
  // `deriveOrchestratorId`'s own validation below, but the message it throws
  // speaks about a session id, not about how to obtain one — so name the
  // acquisition path here where the caller can act on it.
  if (typeof rawSessionId !== 'string' || rawSessionId.trim() === '') {
    return {
      ok: false,
      message:
        'agent-resource-management: --session-id=<id> requires a non-empty value. The documented form is ' +
        '--session-id="$CLAUDE_CODE_SESSION_ID"; an empty value usually means that variable is unset in this ' +
        'shell. Do NOT substitute a hand-picked id — the PreToolUse gate derives its lookup key from the ' +
        "session id independently, and a hand-picked one silently disables the gate for this session.\n",
    };
  }

  try {
    flags['orchestrator-id'] = deriveOrchestratorId(rawSessionId);
  } catch (error) {
    return { ok: false, message: `agent-resource-management: --session-id is invalid — ${error.message}\n` };
  }

  return { ok: true };
}

// ---------------------------------------------------------------------------
// validateBareOrTrueFlag (Phase 5 review, third pass, Medium #2) —
// shared boolean-flag validation, generalized from `--heartbeat`'s own
// pre-existing bare-or-`=true`-only precedent (Phase 1, see that flag's
// validation below in `main()`, unchanged in shape). Extracted here so
// `--heartbeat`, `--crashed`, and `--record-outcome` share ONE implementation
// instead of three copies of the same "only bare or literal `true` is valid"
// check — a caller-supplied value like `--crashed=false` or
// `--record-outcome=false` must hard-reject exactly like `--heartbeat=false`
// already does, not silently coerce through `if (flags[key])`'s truthiness.
//
// Returns `{ valid: true, isSet }` when `raw` is `undefined` (flag absent —
// `isSet: false`), the boolean `true` (bare flag, per `parseArgs`), or the
// string `"true"` (`--flag=true`, per `parseArgs`'s `eqIndex` branch) — all
// three are well-formed. Any other value (`"false"`, `"1"`, `"TRUE"`, a
// swallowed positional value) returns `{ valid: false }` with the stderr
// message already written; the caller is responsible for `process.exit(2)`
// and `return`ing immediately after — this function never calls
// `process.exit` itself so its own behavior stays unit-testable in principle
// and so every call site keeps explicit control over exactly when the
// process actually exits (mirroring every other validation block in this
// file).
// ---------------------------------------------------------------------------

function validateBareOrTrueFlag(flags, flagName) {
  const raw = flags[flagName];
  if (raw === undefined) return { valid: true, isSet: false };
  if (raw === true || raw === 'true') return { valid: true, isSet: true };
  process.stderr.write(
    `agent-resource-management: --${flagName} must be bare (--${flagName}) or --${flagName}=true, got "${raw}"\n`,
  );
  return { valid: false, isSet: false };
}

// ---------------------------------------------------------------------------
// parseClaimSpec — parses `--claim=<type>:<count>` into its
// two segments. Returns `null` for anything malformed (no colon, empty
// type, non-integer count, negative count) — the caller is responsible for
// the stderr message + exit 2, mirroring every other parse-then-validate
// block in this file.
// ---------------------------------------------------------------------------

function parseClaimSpec(rawClaim) {
  if (typeof rawClaim !== 'string') return null;

  const colonIndex = rawClaim.indexOf(':');
  if (colonIndex === -1) return null;

  const type = rawClaim.slice(0, colonIndex).trim();
  if (type.length === 0) return null;

  // An empty/whitespace-only count segment (`--claim=foo:`) must reject
  // rather than coerce (pre-PR review, Low) — `Number('')` is `0`, and
  // `Number.isInteger(0)` is `true`, so without this check a truncated
  // `--claim` value silently succeeded as a well-formed "claim zero slots"
  // ask and printed `{"granted":0}` instead of the documented exit 2.
  const countRaw = rawClaim.slice(colonIndex + 1);
  if (countRaw.trim().length === 0) return null;
  const count = Number(countRaw);
  if (!Number.isInteger(count) || count < 0) return null;

  return { type, count };
}

// ---------------------------------------------------------------------------
// parseReleaseSpec — parses `--release=<type>:<count>` into
// its two segments, mirroring `parseClaimSpec`'s validation rules exactly
// (colon-index check, empty-type rejection, empty-count-segment rejection,
// non-integer rejection, negative-count rejection; `count: 0` is valid).
// Returns `null` for anything malformed — the caller is responsible for the
// stderr message + exit 2, same as `parseClaimSpec`.
// ---------------------------------------------------------------------------

function parseReleaseSpec(rawRelease) {
  if (typeof rawRelease !== 'string') return null;

  const colonIndex = rawRelease.indexOf(':');
  if (colonIndex === -1) return null;

  const type = rawRelease.slice(0, colonIndex).trim();
  if (type.length === 0) return null;

  const countRaw = rawRelease.slice(colonIndex + 1);
  if (countRaw.trim().length === 0) return null;
  const count = Number(countRaw);
  if (!Number.isInteger(count) || count < 0) return null;

  return { type, count };
}

// ---------------------------------------------------------------------------
// parseEnqueueSpec — parses
// `--enqueue=<agentClass>:<priority>:<commandRef>` into its three segments,
// modeled on `parseClaimSpec`/`parseReleaseSpec`'s split/validate/`null`-on-
// failure shape. Splits on the FIRST two colons only — `commandRef` (the
// third/last segment) is taken verbatim as everything after the second
// colon, so a command reference containing its own colons round-trips
// intact rather than being truncated. Rejects (`null`) structurally
// malformed input: fewer than two colons, or an empty `agentClass` segment
// (mirrors `parseClaimSpec`'s empty-type rejection). Does NOT itself
// validate `priority`'s enum membership or `commandRef`'s non-emptiness —
// those are `./lib/queue.mjs`'s `enqueueItem`'s job (`QueueInvalidPriorityError`
// / `QueueInvalidCommandRefError`), which the caller must catch and convert
// to the same exit-2 contract, so validation lives in exactly one place
// rather than being duplicated here.
// ---------------------------------------------------------------------------

function parseEnqueueSpec(rawEnqueue) {
  if (typeof rawEnqueue !== 'string') return null;

  const firstColon = rawEnqueue.indexOf(':');
  if (firstColon === -1) return null;

  const agentClass = rawEnqueue.slice(0, firstColon).trim();
  if (agentClass.length === 0) return null;

  const afterAgentClass = rawEnqueue.slice(firstColon + 1);
  const secondColon = afterAgentClass.indexOf(':');
  if (secondColon === -1) return null;

  const priority = afterAgentClass.slice(0, secondColon).trim();
  const commandRef = afterAgentClass.slice(secondColon + 1);

  return { agentClass, priority, commandRef };
}

// ---------------------------------------------------------------------------
// collect() — the ONLY function that ever shells out to the real machine.
// Uses fixed argv arrays with execFileSync (never a shell/string-interpolated
// command) — there is no legitimate reason these commands ever need dynamic
// arguments, per the Phase 1 security review this ticket's Investigation
// carried forward.
//
// Test seam: when ARM_FAKE_COLLECT_JSON is set, returns that parsed value
// instead — this is the ONLY branch ./cli.jest.spec.mjs exercises; the real
// branch below is inherently unmockable end-to-end and macOS-specific (see
// the Investigation's Constraints), so it is not asserted by CI.
//
// The freshness-window debounce (DEFAULT_FRESHNESS_WINDOW_MS above) is wired
// in one level up, in `main()` — not here: `main()` checks the shared cached
// sample BEFORE ever calling this function (via `collect()`), so a fresh-
// enough shared sample means `collect()`/`realCollect()` are never invoked
// at all for that beat. This function itself always samples the live machine
// when it does run. Per-agent RSS/process-tree attribution for pause-
// candidate selection IS wired in (see collectPsOutput()/listAgentProcesses()
// below,).
// ---------------------------------------------------------------------------

// Absolute paths for every shelled-out binary (LOW-10) rather than relying on
// PATH resolution — these are fixed macOS system tools at fixed locations, so
// there's no legitimate reason to resolve them dynamically.
const VM_STAT_BIN = '/usr/bin/vm_stat';
const SYSCTL_BIN = '/usr/sbin/sysctl';
const DF_BIN = '/bin/df';
const PS_BIN = '/bin/ps';

// ---------------------------------------------------------------------------
// Test seam: ARM_FORCE_COLLECT_FAILURES=<n> — see
// ./cli.jest.spec.mjs's bounded-retry-on-transient-collect-failure tests.
//
// Only consulted when ARM_FAKE_COLLECT_JSON is NOT set — that seam already
// short-circuits `collect()` before `realCollect()` is ever reached (see
// `collect()` below), so it always bypasses this one too.
//
// When set to a non-negative integer N, the first N invocations of
// `realCollect()` within THIS process throw a synthetic transient error
// instead of shelling out to vm_stat/sysctl/df, simulating a transient
// collect failure a bounded-retry wrapper is expected to recover from.
//
// Deliberately does NOT fall through to the real OS commands once the
// budget is exhausted: CI runners are not guaranteed to be macOS
// (vm_stat/sysctl/df don't exist on Linux), so realCollect's genuine
// execFileSync branch stays untested end-to-end regardless of this seam
// (see collect()'s doc comment above). Instead, the (N+1)th invocation
// returns a fixed, deterministic canned GREEN-shaped reading — enough to
// prove a retry recovers and produces a normal traffic light, without
// depending on real hardware state or OS-specific binaries.
// ---------------------------------------------------------------------------
const FORCED_COLLECT_FAILURE_BUDGET = process.env.ARM_FORCE_COLLECT_FAILURES
  ? Number(process.env.ARM_FORCE_COLLECT_FAILURES)
  : 0;
let forcedCollectFailuresConsumed = 0;
const FORCED_COLLECT_SUCCESS_READING = {
  memory: {
    pressureLevel: 1,
    swapUsedMb: 1024,
    compressedMb: 512,
    compressions: 1000,
    decompressions: 500,
  },
  disk: { freeDiskGb: 60, declineRateGbPerHour: 0.5 },
};

// ---------------------------------------------------------------------------
// runProbe — every host-resource shell-out goes through here so none
// of them can block indefinitely. Each `execFileSync` below previously
// carried only `{ encoding: 'utf8' }`: no `timeout`, no `killSignal`. A
// wedged child (a hung `df` against a stalled mount, a kernel that will not
// answer `sysctl` on a thrashing host) therefore blocked this process for as
// long as the child took — and, because these calls are SYNCHRONOUS, blocked
// the event loop with it, which is why no promise-based timeout could ever
// have bounded them (see ./lib/probe-bound.mjs's header for the full
// reasoning). `execFileSync`'s own `timeout`/`killSignal` options are a real
// bound: the child is signalled and the call returns.
//
// An overrun is converted to a typed `ProbeTimeoutError` and propagated.
// Callers must fail CLOSED on it — `takeSample()` degrades a thrown collect
// into a `stale` reading, which classifies non-GREEN, which yields zero
// capacity. A probe that did not finish produced no evidence about the host,
// and no evidence must never be rounded up into a grant.
// ---------------------------------------------------------------------------

function runProbe(bin, args, commandLabel) {
  try {
    return execFileSync(bin, args, probeExecOptions());
  } catch (error) {
    throw asProbeFailure(error, commandLabel);
  }
}

async function realCollect() {
  if (FORCED_COLLECT_FAILURE_BUDGET > 0) {
    if (forcedCollectFailuresConsumed < FORCED_COLLECT_FAILURE_BUDGET) {
      forcedCollectFailuresConsumed += 1;
      throw new Error(
        'agent-resource-management: ARM_FORCE_COLLECT_FAILURES test seam: simulated transient collect ' +
          `failure (${forcedCollectFailuresConsumed}/${FORCED_COLLECT_FAILURE_BUDGET})`,
      );
    }
    return FORCED_COLLECT_SUCCESS_READING;
  }

  const vmStatRaw = runProbe(VM_STAT_BIN, [], 'vm_stat');
  const swapRaw = runProbe(SYSCTL_BIN, ['vm.swapusage'], 'sysctl vm.swapusage');
  const dfRaw = runProbe(DF_BIN, ['-g', '/'], 'df -g /');

  // `pressureLevel` comes from the kernel's own production-accurate signal
  // (`kern.memorystatus_vm_pressure_level`: 1=normal, 2=warn, 4=critical) —
  // NOT from parseVmStat's free/total page-ratio heuristic. That ratio is
  // naturally low on macOS even when idle (the kernel deliberately keeps
  // reclaimable file-backed cache populated rather than leaving pages free),
  // so it reads AMBER/RED on a healthy machine. See ./lib/recon.mjs's
  // parseVmStat doc comment for the full rationale. parseVmStat is still the
  // right source for `compressedMb` — that reading is legitimate.
  const pressureRaw = runProbe(SYSCTL_BIN, ['-n', 'kern.memorystatus_vm_pressure_level'], 'sysctl kern.memorystatus_vm_pressure_level');

  const vm = parseVmStat(vmStatRaw);
  const swap = parseSwapUsage(swapRaw);
  const pressureLevel = parsePressureLevel(pressureRaw);

  // A single df snapshot can't derive a decline rate on its own (that needs
  // two timestamped samples) — this CLI is a fresh, short-lived process each
  // beat with nothing cached between invocations yet, so decline-rate
  // computation from a live trend is a documented gap (same shared gap as
  // the freshness-window debounce above: both need a persisted prior sample,
  // which this phase does not yet wire up). Passing the same reading as both
  // "old" and "new" yields `declineRateGbPerHour: 0` (no trend signal) via
  // ./lib/recon.mjs's `parseFreeDisk`, rather than fabricating a rate.
  const now = Date.now();
  const disk = parseFreeDisk({ raw: dfRaw, timestampMs: now }, { raw: dfRaw, timestampMs: now });

  return {
    // `freeRamMb` (Phase 5 Part A wiring) comes from parseVmStat
    // (the same "Pages free" count, reusing its already-derived page-size
    // handling) — it feeds computeHeadroomCap in main() below to derive the
    // memory axis's GREEN cap from live headroom, distinct from
    // `pressureLevel`'s kernel-sourced signal above.
    memory: {
      pressureLevel,
      swapUsedMb: swap.swapUsedMb,
      swapTotalMb: swap.swapTotalMb,
      compressedMb: vm.compressedMb,
      freeRamMb: vm.freeRamMb,
      // `compressions`/`decompressions` (root-cause fix) feed its
      // compressionVelocity — parseVmStat already derives both correctly,
      // but this return object silently dropped them, so coldStart never
      // cleared past the first beat. Forward them straight through.
      compressions: vm.compressions,
      decompressions: vm.decompressions,
    },
    disk,
  };
}

// ---------------------------------------------------------------------------
// Bounded retry: a single transient `realCollect()` failure
// must not degrade a beat all the way to `takeSample()`'s "stale" (AMBER)
// fallback — it's retried once, immediately, before any pipeline stage sees
// it. `takeSample()` itself (./lib/recon.mjs) always swallows a thrown
// `collect()` into a stale marker rather than rethrowing (that's the right
// behavior for lib/recon.mjs's own contract, and its own tests pin it), so a
// genuine TOTAL failure (both the original attempt and the retry throw) is
// captured here in `totalCollectFailure` and rethrown by `main()` right
// after the `takeSample()` call returns — before any dibs are declared —
// reaching the exact same top-level `main().catch()` exit-1 stderr path a
// single un-retried failure would have hit before this feature existed.
//
// Deliberately structured so the ARM_FAKE_COLLECT_JSON test seam short-
// circuits before any of this: that branch returns (or throws a SyntaxError
// from) a single `JSON.parse` call and never reaches the try/catch below, so
// a forced-failure budget alongside ARM_FAKE_COLLECT_JSON has zero effect.
//
// This module-level flag is safe only because cli.mjs is strictly one-shot
// per process (documented invariant, see file header) — if this file ever
// stops being one-shot (e.g. an internal loop calling collect() more than
// once per process), this flag MUST be reset between iterations or it will
// leak a stale failure across them.
// ---------------------------------------------------------------------------
let totalCollectFailure = null;

async function collect() {
  if (process.env.ARM_FAKE_COLLECT_JSON) {
    return JSON.parse(process.env.ARM_FAKE_COLLECT_JSON);
  }
  try {
    return await realCollect();
  } catch {
    try {
      return await realCollect();
    } catch (secondError) {
      // review, Critical — a genuine TOTAL machine-probe failure is,
      // by this function's own contract above, never a per-target eviction
      // outcome: it is captured here for `main()`'s own fail-loud path AND
      // marked `armFailBeat` at this true source so that ANY other consumer
      // reaching it via a different path (today: `handleWatchdogBeat`'s
      // `getPreTakenSample`, cached/shared across every evict target in a
      // beat) is recognized as systemic too, rather than silently isolated
      // per-target by `runWatchdogBeat`'s `isSystemicEvictFailure` check.
      secondError.armFailBeat = true;
      totalCollectFailure = secondError;
      throw secondError;
    }
  }
}

// ---------------------------------------------------------------------------
// Phase 5 — load average: RECORD, do not gate. The issue is explicit
// that a load-42 host was observed as a SYMPTOM of compressor thrash, not a
// cause — agents spend much of their life network-blocked at ~0% CPU while
// holding ~1 GB, so a naive "load > core count => throttle" rule would
// punish exactly the workload this skill exists to keep flowing. This
// function's ONLY job is to produce the reading; nothing downstream may use
// it as an admission input (see the `loadAverage` sibling field added at
// every `process.stdout.write` call site below, all attached AFTER the
// admission decision has already been made, mirroring `diskTrend` above).
//
// Sourced from Node's built-in `os.loadavg()` rather than a new
// `sysctl`/shell-out: it costs nothing extra against this file's "no more
// than one additional sysctl call per beat" sampling-cost budget (already
// spent by Phase 1's `swapTotalMb` plumbing — see `DEFAULT_MEMORY_
// THRESHOLDS`' own history), and it is portable to non-macOS CI runners
// unlike this file's `vm_stat`/`sysctl`/`df` probes.
//
// `ARM_FAKE_LOAD_AVERAGE_JSON` (a JSON 3-element array, `[oneMinute,
// fiveMinute, fifteenMinute]`) mirrors this file's existing `ARM_FAKE_*` test
// seam convention — never set in a real orchestrator invocation. Deliberately
// NOT routed through `collect()`/`ARM_FAKE_COLLECT_JSON`: `os.loadavg()` is
// not a shell-out with the portability/mockability concerns that seam exists
// for, and folding it into the memory/disk sample shape would blur "what
// classifyMemoryAxis/classifyDiskAxis consume" with "what is purely
// observational" — a distinction this phase's whole point is to keep sharp.
function getLoadAverage() {
  if (process.env.ARM_FAKE_LOAD_AVERAGE_JSON) {
    const [oneMinute, fiveMinute, fifteenMinute] = JSON.parse(process.env.ARM_FAKE_LOAD_AVERAGE_JSON);
    return { oneMinute, fiveMinute, fifteenMinute };
  }
  const [oneMinute, fiveMinute, fifteenMinute] = loadavg();
  return { oneMinute, fiveMinute, fifteenMinute };
}

// ---------------------------------------------------------------------------
// collectPsOutput() — mirrors collect()/ARM_FAKE_COLLECT_JSON's seam pattern
// when ARM_FAKE_PS_OUTPUT is set, returns that raw string instead of
// shelling out to the real `ps` binary. review, Low — this no longer
// means "no test run ever samples the real host's process tree": a few
// scenarios in cli-evict.jest.spec.mjs deliberately pass `honorFakeSeam:
// false` (via `ARM_EVICT_TEST_MODE` being unset) specifically to exercise
// the real shell-out path against this host's own process tree. When unset,
// shells out for real via execFileSync with a fixed absolute path and array
// argv (never a shell/string-interpolated
// command), matching the VM_STAT_BIN/SYSCTL_BIN/DF_BIN convention above. If
// the real shell-out throws, degrades to empty ps output rather than
// crashing the CLI — same graceful-degradation posture as the rest of
// main().
// ---------------------------------------------------------------------------

// `honorFakeSeam` (default `true`) lets `handleEvict` force the REAL ps
// shell-out regardless of `ARM_FAKE_PS_OUTPUT` when its own
// `ARM_EVICT_TEST_MODE` master switch (see that function's header comment) is
// not set. Exactly ONE caller omits the argument and so leaves this switch
// defaulted to `true`: `main()`'s memory-RED pause-candidate selection
// (review, Medium — this sentence used to name "the generic
// `--poll-footprint` beat, etc.", and `handlePollFootprint` does not call this
// function, or take a `ps` snapshot, at all). Every other call site —
// including `buildWatchdogCandidates`'s own `collectPsOutput()` call — is on
// the eviction path and passes `evictTestModeEnabled` explicitly
// (bot-review fix: an earlier Phase 3 revision left that call site
// unconditional, which would let a leaked production `ARM_FAKE_PS_OUTPUT`
// corrupt candidate discovery outside test mode).
//
// "Defaulted to `true`" is NOT "honours the seam unconditionally", and the
// difference matters (pre-PR review, Medium — this sentence used to say
// "unconditionally" and this ticket made that false). `honorFakeSeam` is only
// the FIRST of three conditions below: `ARM_COORDINATION_FILE` must also be
// set before `ARM_FAKE_PS_OUTPUT` is read at all. A caller that omits the
// argument still gets the real `ps` when that variable is unset.
function collectPsOutput(honorFakeSeam = true) {
  // — counted here as well as in `collectBeatPsSnapshot`, so the
  // eviction path's own collections are visible to
  // `ARM_FAKE_PS_COLLECTION_LOG` rather than invisible to it. The recorder
  // checks `armTestModeEnabled()` before anything else, so a production
  // invocation — neither master switch set — returns on that check without
  // ever reading the seam variable.
  recordProcessSnapshotCollection();

  // The SAME triple `collectBeatPsSnapshot` tests, and the same one
  // `resolveProcessSnapshotNow` tests for the anchor: this path's master
  // switch (threaded in as `honorFakeSeam`), `ARM_COORDINATION_FILE`, then the
  // seam itself. The anchor derives `startedAt` from the elapsed times in
  // whatever snapshot this returns, so a condition here that differs from the
  // anchor's would hand fixture rows to a real-clock anchor (or the reverse)
  // and derive identities from two different clocks.
  if (honorFakeSeam && process.env.ARM_COORDINATION_FILE && process.env.ARM_FAKE_PS_OUTPUT) {
    return process.env.ARM_FAKE_PS_OUTPUT;
  }
  try {
    return runPsProbe();
  } catch {
    return '';
  }
}

/**
 * The bare `ps` shell-out, THROWING on failure.
 *
 * `collectPsOutput` above swallows that throw and answers `''`, which collapses
 * two observations a liveness sweep must keep apart: a snapshot that saw NO
 * agents (a valid observation — every pid-bearing ledger record is then
 * genuinely absent, hence confirmed dead) and a collection that FAILED (no
 * observation at all — every record must fall back to its TTL). Reaping on the
 * strength of a failed `ps` would turn one flaky shell-out into a fleet-wide
 * reap of live agents, so the beat's own snapshot path
 * (`collectBeatPsSnapshot`) calls this directly and classifies the failure
 * itself. Every `collectPsOutput` caller treats "no rows" and "no snapshot"
 * alike, and keeps that swallow-and-degrade behaviour: pause-candidate
 * selection, the eviction authorization check, and — added by — the
 * eviction beat's own ledger-identity resolution, where `''` reaches
 * `resolveNearestClaudeRootIdentity`, resolves to `null`, and omits both
 * identity fields rather than stamping a sentinel pid. (Enumerated rather than
 * left as "the callers" because a stale enumeration is how this doc-block was
 * wrong before — pre-PR review, Low: it named only the first two after this
 * same ticket had added the third.)
 *
 * @returns {string}
 */
function runPsProbe() {
  // stdio: 'pipe' (not the execFileSync default of inheriting stderr) so a
  // ps error on an unsupported platform/keyword is captured by whichever
  // caller catches it rather than leaking onto this CLI's own stderr — the
  // CLI's documented graceful-degradation contract expects a clean stderr on
  // this path (see ./cli.jest.spec.mjs's `stderr).toBe('')` assertions).
  // `etime` (not the Linux/procps-only `etimes`) is the BSD/macOS-portable
  // elapsed-time keyword this project's actual target host (macOS, per
  // .claude/skills/agent-resource-management/SKILL.md) supports — `etimes`
  // made this shell-out fail unconditionally on macOS, silently degrading
  // every real invocation to empty output (Critical fix).
  // bounded like every other shell-out (see runProbe). `stdio: 'pipe'`
  // is preserved — `collectPsOutput`'s swallow-and-degrade posture already
  // covers a timeout kill, so it needs no separate classification.
  return execFileSync(PS_BIN, ['-Ao', 'pid,ppid,rss,etime,comm'], {
    ...probeExecOptions(),
    stdio: 'pipe',
  });
}

// ---------------------------------------------------------------------------
// Coordination file path — ARM_COORDINATION_FILE (test seam) overrides the
// production default. The resource being coordinated (RAM/disk) belongs to
// the whole HOST, not to any one repo/worktree, so the default resolves to a
// host-global location under the user's home directory rather than
// `process.cwd()` — orchestrators running out of different worktrees (or
// different repos entirely) on the same machine must share ONE coordination
// file, not one per cwd.
// ---------------------------------------------------------------------------

function resolveCoordinationFilePath() {
  if (process.env.ARM_COORDINATION_FILE) {
    return resolve(process.env.ARM_COORDINATION_FILE);
  }
  return join(homedir(), '.claude/agent-state/resource-coordination.json');
}

// ---------------------------------------------------------------------------
// History file path — ARM_HISTORY_FILE (test seam) mirrors
// ARM_COORDINATION_FILE's own precedent exactly: same naming convention, same
// "env override falls back to a real `.claude/agent-state/` production
// default when unset" shape. `lib/history.mjs`'s `recordObservation`/
// `readHistory` operate on this file.
// ---------------------------------------------------------------------------

function resolveHistoryFilePath() {
  if (process.env.ARM_HISTORY_FILE) {
    return resolve(process.env.ARM_HISTORY_FILE);
  }
  return join(homedir(), '.claude/agent-state/resource-history.json');
}

// ---------------------------------------------------------------------------
// ensureHistoryDir — mirrors ensureCoordinationDir below exactly (same
// rationale: on a fresh machine nothing has ever created
// `~/.claude/agent-state/`, so the first --record-outcome beat would
// otherwise fail with ENOENT). A genuinely unwritable path still degrades
// gracefully via handleRecordOutcome's own try/catch around
// `recordObservation`.
// ---------------------------------------------------------------------------

async function ensureHistoryDir(historyFilePath) {
  try {
    await fs.mkdir(dirname(historyFilePath), { recursive: true });
  } catch {
    // Deliberately swallowed — see ensureCoordinationDir's identical comment.
  }
}

// ---------------------------------------------------------------------------
// Footprint-poll state file path — ARM_FOOTPRINT_STATE_FILE
// (test seam) mirrors ARM_HISTORY_FILE's/ARM_COORDINATION_FILE's own
// precedent exactly: same naming convention, same "env override falls back to
// a real `.claude/agent-state/` production default when unset" shape.
// `lib/footprint-state.mjs`'s `recordFootprintPoll`/`consumeFootprintState`
// operate on this file.
// ---------------------------------------------------------------------------

function resolveFootprintStateFilePath() {
  if (process.env.ARM_FOOTPRINT_STATE_FILE) {
    return resolve(process.env.ARM_FOOTPRINT_STATE_FILE);
  }
  return join(homedir(), '.claude/agent-state/footprint-state.json');
}

// ---------------------------------------------------------------------------
// Phase 3 — `leakBlocks` detection echo, additive to every
// `--desired-agents`/`--heartbeat` beat's ordinary JSON body (never gated
// behind `isHeartbeat` — see this ticket's Build Plan handoff: this is pure
// detection/observability, independent of admission-advancement concerns
// like the AIMD ceiling/spawn-rate bucket, which ARE gated on `!isHeartbeat`
// for reasons that don't apply here). Independent of `pressure-block`'s own
// memoryState/diskState precedence chain too — it must still be computed and
// echoed even on a beat that pressure-block halts, mirroring
// `pressure-block`'s own structural independence from admission.
//
// `lib/leak-velocity.mjs`'s `computeLeakVelocity` is a PURE, one-poll-at-a-
// time function: its `consecutiveAboveThreshold` counter is meant to be
// persisted and threaded call-to-call by whoever calls it once per real
// `--poll-footprint` poll (see that module's own header/tests). Since
// `footprint-state.mjs`'s persisted `FootprintStateEntry` carries no such
// counter today, this beat-time detection REPLAYS a pid's already-persisted
// `trajectory` as a sequence of incremental two-sample polls — one call per
// consecutive PAIR of smoothed samples — threading the counter through that
// replay.
//
// SIMPLIFICATION (Medium, Phase 3 review) — this "one call per adjacent
// pair" replay is a deliberate, different statistic from
// `computeGrowthRateMbPerMin`'s natural accumulating/growing-window design
// (first-valid-to-last-valid sample over the WHOLE trajectory). What's
// implemented here is an adjacent-pair INSTANTANEOUS-SLOPE replay: each call
// only ever sees a two-sample window, so `consecutiveAboveThreshold` is
// carrying the sustained-streak signal, not the accumulating-window rate
// `computeLeakVelocity`/`computeGrowthRateMbPerMin` is more naturally suited
// to. That's a defensible simplification for this phase's scope (real
// per-poll wiring, where `computeLeakVelocity` is called once per genuine
// `--poll-footprint` invocation with its own persisted counter, is Phase 4+
// scope) — but it is NOT claimed to be a verified-equivalent replacement for
// that per-poll caller; a future reader should not treat the two as
// interchangeable.
//
// Per-pair timestamps are reconstructed by assuming EVENLY SPACED polls
// across an estimated span ending at the pid's real, always-current
// `lastPolledAt` — see `windowStartAt`/`intervalMs` below. This is an
// APPROXIMATION, not an exact reconstruction of "what a real per-poll caller
// would have seen": real `--poll-footprint` cadence is documented as 10-30s
// but is NOT guaranteed uniform (`footprint-sampler.mjs` does not enforce a
// fixed tick), and a failed poll is dropped entirely rather than recorded as
// a placeholder null, so the true inter-sample gaps can vary. Smearing a
// single estimated interval evenly across every reconstructed pair can
// therefore under- or over-state any one pair's true rate, even though the
// aggregate trend across the streak is still meaningful. Do not read this as
// "exactly as a real per-poll caller would have seen" — it is the best
// reconstruction available without `footprint-state.mjs` persisting a
// timestamp per trajectory sample (tracked as future work; see Critical note
// below).
//
// CRITICAL FIX (Phase 3 review) — `trajectory` is capped at
// `TRAJECTORY_CAP` (120, see `footprint-trajectory.mjs`), oldest entries
// dropped first, while `entry.firstPolledAt` is set once at cold start and
// NEVER adjusted (`footprint-state.mjs`). Once a pid has been polled more
// than `TRAJECTORY_CAP` times, `lastPolledAt - firstPolledAt` spans the
// pid's ENTIRE history since cold start, but `trajectory.length - 1` only
// counts the gaps between the 120 SURVIVING samples — using the raw
// `(lastPolledAt - firstPolledAt) / (trajectory.length - 1)` division in
// that case inflates the reconstructed per-pair interval far beyond the real
// cadence (the numerator includes a truncated-away span the denominator no
// longer represents), which systematically UNDER-estimates the growth rate
// and can silently suppress a trip for exactly the long-running leakers this
// circuit breaker exists to catch. `footprint-state.mjs`'s
// `FootprintStateEntry` has no per-sample timestamp and no total-poll
// counter to derive an exact ratio from (confirmed by inspection — only
// `firstPolledAt`/`lastPolledAt`/`smoothed`/`peakSmoothedMb`/`trajectory`/
// `agentClass?`), so an exact fix is out of this phase's scope (it requires
// changing `footprint-state.mjs`'s persisted shape).
//
// MEDIUM FIX (Phase 3 review, round 2) — the first pass keyed the
// fallback decision purely on `trajectory.length >= TRAJECTORY_CAP`. That is
// NOT a reliable truncation signal: a pid that has been polled EXACTLY
// `TRAJECTORY_CAP` times and never more still has an entirely accurate
// `firstPolledAt` (cold start really was that long ago, nothing was ever
// dropped), yet length-keying discarded it anyway and substituted the
// assumed `DEFAULT_FOOTPRINT_POLL_INTERVAL_MS` cadence in its place. Because
// a trajectory can sit at exactly `TRAJECTORY_CAP` for many beats (whenever
// polling continues at the same rate samples are evicted), this
// misclassification is not a rare one-beat coincidence — it can persist
// across many consecutive beats, using the wrong assumed cadence every time
// and inflating (or deflating) the reconstructed MB/min rate for a pid whose
// real cadence differs from the 20s assumption, producing a false trip (or
// false negative) with no truncation ever having occurred.
//
// The defensible fix taken here: compute the naive
// `(lastPolledAt - firstPolledAt) / (trajectory.length - 1)` interval FIRST,
// unconditionally, and only distrust it — falling back to
// `DEFAULT_FOOTPRINT_POLL_INTERVAL_MS` — when the naive value is ITSELF
// implausible against this file's own documented `MAX_FOOTPRINT_POLL_INTERVAL_MS`
// cadence bound (with a margin above it to allow for normal jitter), and
// symmetrically against `MIN_FOOTPRINT_POLL_INTERVAL_MS` (with the same
// margin dividing it down instead of multiplying it up) below which a naive
// interval is equally distrusted.
//
// pre-PR review — the upper-bound check alone left an
// anomalously SMALL naive interval (clock skew, or a `firstPolledAt`/
// `lastPolledAt` pair compressed tighter than any real poll cadence could
// produce) trusted as-is, understating `intervalMs` and inflating the
// derived MB/min rate enough to false-trip a pid that is not actually
// leaking. The lower-bound check below closes that gap the same way the
// upper-bound one already closes the truncation gap: distrust the naive
// value and fall back to `DEFAULT_FOOTPRINT_POLL_INTERVAL_MS` instead of
// trusting an implausible extreme in either direction.
//
// Genuine
// truncation is exactly the condition that produces an implausibly LARGE naive
// interval — the numerator still spans the pid's entire cold-start history
// while the denominator only counts the 120 surviving gaps, inflating the
// naive interval far past `MAX_FOOTPRINT_POLL_INTERVAL_MS`. A pid that is
// merely sitting at the cap without ever having been truncated keeps a naive
// interval inside the plausible bounds and is left alone, preserving its
// accurate `firstPolledAt`-derived anchor. When the fallback does trigger,
// `windowStartAt` is anchored to the reliable, always-current
// `lastPolledAt`, working BACKWARD across only the `trajectory.length - 1`
// surviving gaps, rather than anchoring forward from the stale
// `firstPolledAt` (which is exactly what smuggles the truncated-away span
// into the reconstruction). This is still an APPROXIMATION —
// `DEFAULT_FOOTPRINT_POLL_INTERVAL_MS` is a recommended cadence, not this
// specific pid's actual one — but it keeps the reconstructed window scoped
// to a value consistent with real production cadence instead of one
// inflated by however much history was truncated away, so a genuinely
// sustained above-threshold streak in the surviving window is no longer
// silently diluted below the trip threshold.
//
// A pid whose growth is genuinely sustained across `consecutivePollsRequired`
// consecutive pairs trips; a pid whose growth is real but INTERRUPTED by even
// one below-threshold (including negative/shrinking) pair never accumulates
// enough consecutive above-threshold pairs to trip, and the streak resets to
// 0 on that pair — exactly `computeLeakVelocity`'s own documented contract.
//
// Phase 4 pre-PR review — extracted so `computeLeakBlocksForBeat`
// (detection-only echo) and `buildLeakVelocityStateForCandidate` (real
// eviction-wiring, see that function's own doc comment below) share ONE
// implementation of the naive-interval/plausibility-fallback timestamp
// reconstruction rather than drifting into two copies. See the MEDIUM FIX
// note originally attached to `computeLeakBlocksForBeat` (now folded into
// this function's own doc comment) for the full rationale.
//
// Key the fallback on whether the naive `firstPolledAt`-derived interval is
// ITSELF implausible against this file's documented cadence bound, not on
// `trajectory.length >= TRAJECTORY_CAP` alone — a pid can sit at exactly
// TRAJECTORY_CAP for many beats without ever having been truncated, in which
// case `firstPolledAt` is still accurate and the naive interval stays inside
// the plausible bound. Genuine truncation smuggles a truncated-away span
// into the numerator while the denominator only counts surviving gaps,
// which inflates the naive interval past the plausible ceiling — that
// inflation, not the array length, is the reliable signal.
//
// @param {{ trajectory?: unknown, firstPolledAt?: unknown, lastPolledAt?: unknown }} entry
// @returns {{ trajectory: number[], firstPolledAt: number, lastPolledAt: number, intervalMs: number, windowStartAt: number } | null}
//   `null` when `entry` does not carry enough real data to reconstruct
//   per-pair timestamps at all.
function reconstructLeakVelocityTiming(entry) {
  const trajectory = Array.isArray(entry?.trajectory) ? entry.trajectory : [];
  const firstPolledAt = entry?.firstPolledAt;
  const lastPolledAt = entry?.lastPolledAt;
  if (
    trajectory.length < 2 ||
    typeof firstPolledAt !== 'number' ||
    typeof lastPolledAt !== 'number' ||
    lastPolledAt <= firstPolledAt
  ) {
    return null;
  }

  const naiveIntervalMs = (lastPolledAt - firstPolledAt) / (trajectory.length - 1);
  const naiveIntervalIsImplausible =
    naiveIntervalMs > MAX_FOOTPRINT_POLL_INTERVAL_MS * PLAUSIBLE_INTERVAL_MARGIN ||
    naiveIntervalMs < MIN_FOOTPRINT_POLL_INTERVAL_MS / PLAUSIBLE_INTERVAL_MARGIN;
  const intervalMs = naiveIntervalIsImplausible ? DEFAULT_FOOTPRINT_POLL_INTERVAL_MS : naiveIntervalMs;
  const windowStartAt = lastPolledAt - intervalMs * (trajectory.length - 1);

  return { trajectory, firstPolledAt, lastPolledAt, intervalMs, windowStartAt };
}

// pre-PR review, Medium 2 — `leakBlocks` is a FLEET-WIDE, advisory
// echo, not scoped to the calling orchestrator's own owned pids, and this is
// a deliberate design choice, not an oversight. This function is called from
// the `--desired-agents`/`--heartbeat` spawn-decision beat (`main()`, see
// this function's call site), which has no `--orchestrator-pid` and no
// ps-tree ancestry proof available at all (unlike `--watchdog-beat`'s
// `buildWatchdogCandidates`, which DOES prove per-pid ownership before ever
// building a candidate) — there is no ownership signal here to filter
// against without adding a new required flag to every spawn-decision beat.
// This mirrors `pressure-block`'s/`pauseCandidate`'s own host-wide posture
// (see SKILL.md's "Unmaskable pressure-WARN admission block" section) rather
// than the real eviction path's `isEvictionAuthorized` proof-of-ownership
// gate. **Any consumer of `leakBlocks` MUST independently verify
// ownership/authorization for a listed pid before acting on it** — this
// function's own output must never be treated as sufficient authorization to
// evict or otherwise act on a listed pid; it is observation only, exactly
// like `pauseCandidate`/`shedSignal` above.
//
// @param {Record<string, { trajectory: number[], firstPolledAt: number, lastPolledAt: number }>} footprintStore
// @param {{ thresholdMbPerMin: number, consecutivePollsRequired: number, minSamples?: number }} config
// @param {number} now
// @returns {Array<{ pid: number, type: 'leak-block', reason: string }>}
function computeLeakBlocksForBeat(footprintStore, config = DEFAULT_LEAK_VELOCITY_THRESHOLDS, now = Date.now()) {
  const leakBlocks = [];

  for (const [key, entry] of Object.entries(footprintStore ?? {})) {
    const pid = Number(key);
    // pre-PR review, Low — a footprint-state store keyed by anything
    // other than a genuine pid string (corrupt/hand-edited JSON) must never
    // reach `Number.isFinite`-assuming arithmetic below as `NaN`.
    if (!Number.isFinite(pid)) continue;

    // pre-PR review, Medium 1 — a footprint-state entry that has not
    // been polled within `DEFAULT_LIVENESS_THRESHOLD_MS` (15 minutes) is
    // exactly the long-dead/pid-reused case `footprint-state.mjs` itself
    // already guards against on its OWN write path, via its own
    // `DEFAULT_STALE_AFTER_MS` — the SAME 15-minute value, passed in
    // explicitly by callers like line ~3140's `readFootprintState` call
    // rather than imported, to avoid inverting the lib/cli.mjs dependency
    // direction (see `footprint-state.mjs`'s own `DEFAULT_STALE_AFTER_MS`
    // doc comment). Reusing `DEFAULT_LIVENESS_THRESHOLD_MS` here rather than
    // inventing a third copy of the same constant. This beat's own read path
    // had no equivalent guard before this fix, so a pid that died (or was
    // reused by an unrelated later process) minutes ago could otherwise
    // still surface a stale `leak-block` echo indefinitely.
    const lastPolledAtForStaleness = entry?.lastPolledAt;
    if (
      typeof lastPolledAtForStaleness === 'number' &&
      now - lastPolledAtForStaleness > DEFAULT_LIVENESS_THRESHOLD_MS
    ) {
      continue;
    }

    const timing = reconstructLeakVelocityTiming(entry);
    if (!timing) continue;
    const { trajectory, intervalMs, windowStartAt } = timing;

    // pre-PR review round 4, Medium — this loop always replays exactly
    // a 2-element `[trajectory[index - 1], trajectory[index]]` sub-trajectory
    // per pair, so `computeLeakVelocity`'s own `config.minSamples` (default 2
    // per `leak-velocity.mjs`) MUST resolve to exactly 2 here regardless of
    // what `config` carries — a caller-supplied `minSamples > 2` would make
    // `computeGrowthRateMbPerMin`'s `validSamples.length < minSamples` check
    // fail on every single pair, silently returning `growthRateMbPerMin:
    // null` forever and permanently disabling this detector with no error.
    // `config` is spread first so `thresholdMbPerMin`/`consecutivePollsRequired`
    // (and any future field) still flow through untouched; only `minSamples`
    // is pinned to the value this pairwise-replay shape actually requires.
    const pairwiseReplayConfig = { ...config, minSamples: 2 };

    let consecutiveAboveThreshold = 0;
    let trippedGrowthRateMbPerMin = null;
    let tripped = false;

    for (let index = 1; index < trajectory.length; index += 1) {
      const pairWindowStartAt = windowStartAt + (index - 1) * intervalMs;
      const pollAt = windowStartAt + index * intervalMs;
      const result = computeLeakVelocity(
        {
          trajectory: [trajectory[index - 1], trajectory[index]],
          firstPolledAt: pairWindowStartAt,
          lastPolledAt: pollAt,
          consecutiveAboveThreshold,
        },
        pairwiseReplayConfig,
      );
      consecutiveAboveThreshold = result.consecutiveAboveThreshold;
      if (result.trip) {
        tripped = true;
        trippedGrowthRateMbPerMin = result.growthRateMbPerMin;
        break;
      }
    }

    // pre-PR review round 2, High note — this loop `break`s on the
    // FIRST trip found anywhere in the replayed trajectory, so a pid that
    // EVER tripped once continues to echo `leak-block` on every subsequent
    // beat even if growth has since genuinely stopped. This is a
    // pre-existing, deliberately-NOT-reconciled divergence from
    // `buildLeakVelocityStateForCandidate` (the real eviction-wiring path),
    // which — after round 3's fix — only treats a trip as live when an
    // unbroken TRAILING run of `consecutivePollsRequired` above-threshold
    // pairs ends at the final pair. Phase 3's `leakBlocks` is
    // detection-only/advisory (see this function's own header comment), so
    // "flagged forever once it happened" is left as-is here; a future reader
    // should not be surprised the two consumers of the same trajectory data
    // can disagree.
    if (tripped) {
      const rateLabel =
        typeof trippedGrowthRateMbPerMin === 'number' ? trippedGrowthRateMbPerMin.toFixed(1) : 'unknown';
      leakBlocks.push({
        pid,
        type: 'leak-block',
        reason: `sustained footprint growth (~${rateLabel} MB/min) across ${config.consecutivePollsRequired} consecutive polls`,
      });
    }
  }

  return leakBlocks;
}

/**
 * Phase 4 pre-PR review round 2, High — replays a pid's FULL persisted
 * footprint trajectory (the SAME `reconstructLeakVelocityTiming` reuse
 * `computeLeakBlocksForBeat` uses for its own detection-only echo) to derive
 * a `leakVelocityState` object that, when fed through `watchdog-beat.mjs`'s
 * own SINGLE `computeLeakVelocity(candidate.leakVelocityState,
 * leakVelocityConfig)` call, correctly reflects whether the pid's sustained
 * growth streak is CURRENTLY LIVE — without requiring `footprint-state.mjs`
 * to persist a cross-beat `consecutiveAboveThreshold` counter.
 * `--watchdog-beat` is a stateless, one-shot CLI invocation each beat (no
 * long-running process to carry a persisted counter in memory), but the full
 * trajectory (up to `TRAJECTORY_CAP`) is available fresh on every beat and
 * this replay is deterministic, so no cross-beat persistence is needed at
 * all.
 *
 * PRIOR BUG (round 2 pre-PR review, High): the previous version of this
 * function replayed every pair EXCEPT the final one, fed the raw accumulated
 * `consecutiveAboveThreshold` into a synthetic final-pair state, and let
 * `watchdog-beat.mjs`'s own single call tick that final pair over. Because
 * `computeLeakVelocity` resets its counter to 0 on every trip (exactly like
 * a real persisted counter would across real beats), a CONTINUOUSLY-leaking
 * pid only reached a fresh trip at trajectory lengths that happened to land
 * on an exact multiple of `consecutivePollsRequired` pairs — e.g. with the
 * default `consecutivePollsRequired: 4`, an unbroken leak tripped at
 * trajectory lengths 5 and 9 but NOT at 6, 7, 8, 10, 11, or 12. A `--watchdog-
 * beat` invocation is a snapshot of an ongoing, real-world trajectory whose
 * length is NOT under this module's control — treating the counter's exact
 * modulo-cycle position as the trip signal meant most real invocations
 * refused to evict an unambiguously runaway pid.
 *
 * CORRECTED SEMANTICS (round 3 pre-PR review, High — supersedes round 2's
 * `everTripped && finalPairAboveThreshold` latch below): "is the trip
 * currently live" is answered by a TRAILING-RUN count — the number of
 * consecutive above-threshold pairs counting backward from the end of the
 * trajectory, walking the WHOLE trajectory (every pair, INCLUDING the final
 * one) with `computeLeakVelocity`'s own real reset semantics. This is a
 * DELIBERATE DIVERGENCE from a literal persisted counter, not an attempt to
 * reproduce one: a real persisted counter resets to 0 on every trip, so its
 * exact value after N unbroken above-threshold pairs is modulo-cyclic in N
 * (see ROUND 2'S BUG below) — which is exactly the defect this round fixes.
 * `trailingAboveThresholdRun` instead answers the operationally-meaningful
 * question "is a sustained run live at this instant", independent of
 * trajectory length. It increments on every pair
 * whose `growthRateMbPerMin` is a number `>= config.thresholdMbPerMin`, and
 * resets to 0 on any pair that is either below threshold OR unmeasurable
 * (`growthRateMbPerMin === null`) — the same reset trigger
 * `computeLeakVelocity` itself uses, applied here as a trailing count rather
 * than an ever-true latch. The trip is CURRENTLY LIVE only when
 * `trailingAboveThresholdRun >= config.consecutivePollsRequired`.
 *
 * ROUND 2'S BUG: the previous `everTripped` was a PERMANENT latch — set true
 * the first time any pair anywhere in the retained trajectory ever reached a
 * full sustained count — combined with `finalPairAboveThreshold` (only the
 * single most recent pair). Once a pid had EVER tripped once anywhere in its
 * retained trajectory (e.g. an early load-ramp unrelated to any current
 * problem), it was evicted after just one more above-threshold poll instead
 * of a fresh full sustained run. The trailing-run count removes the
 * permanent latch entirely: an old trip earns no credit toward a later,
 * unrelated above-threshold run — only an unbroken CURRENT run of
 * `consecutivePollsRequired` pairs counts.
 *
 * When currently live, this function hands `watchdog-beat.mjs` a final-pair
 * state pre-seeded with `consecutiveAboveThreshold: consecutivePollsRequired
 * - 1` (never the raw replayed value) so its own single `computeLeakVelocity`
 * call recomputes the final pair's rate (confirmed already above threshold)
 * and ticks the counter over to `trip: true` on that one call. When NOT
 * currently live, `consecutiveAboveThreshold: 0` is returned instead — the
 * final call may still increment it, but it can never itself produce a
 * trip, so a not-yet-sustained or genuinely-stopped streak is correctly left
 * untripped. This is a wiring-only fix in `cli.mjs`; `watchdog-beat.mjs`'s
 * own already-reviewed `leakVelocityState` contract (and its pinned unit
 * tests) are untouched.
 *
 * @param {{ trajectory?: unknown, firstPolledAt?: unknown, lastPolledAt?: unknown }} entry
 * @param {{ thresholdMbPerMin: number, consecutivePollsRequired: number, minSamples?: number }} config
 * @returns {{ trajectory: number[], firstPolledAt: number, lastPolledAt: number, consecutiveAboveThreshold: number } | undefined}
 *   `undefined` when the entry cannot be reconstructed at all (cold start /
 *   too few samples) — mirrors `leakVelocityState`'s own documented "no
 *   entry yet" contract in `watchdog-beat.mjs`: a candidate with no
 *   `leakVelocityState` is simply skipped there, never trips.
 */
function buildLeakVelocityStateForCandidate(entry, config = DEFAULT_LEAK_VELOCITY_THRESHOLDS) {
  const timing = reconstructLeakVelocityTiming(entry);
  if (!timing) return undefined;

  const { trajectory, intervalMs, windowStartAt } = timing;

  // pre-PR review round 4, Medium — same fix as
  // `computeLeakBlocksForBeat` above: this loop also always replays exactly
  // a 2-element sub-trajectory per pair, so `minSamples` must be pinned to 2
  // regardless of what `config` carries, or a caller-supplied override above
  // 2 would make every pair unmeasurable and silently disable this path's
  // eviction-wiring output forever. See that function's comment for the full
  // failure mechanism.
  const pairwiseReplayConfig = { ...config, minSamples: 2 };

  let consecutiveAboveThreshold = 0;
  let trailingAboveThresholdRun = 0;
  const lastIndex = trajectory.length - 1;

  for (let index = 1; index <= lastIndex; index += 1) {
    const pairWindowStartAt = windowStartAt + (index - 1) * intervalMs;
    const pollAt = windowStartAt + index * intervalMs;
    const result = computeLeakVelocity(
      {
        trajectory: [trajectory[index - 1], trajectory[index]],
        firstPolledAt: pairWindowStartAt,
        lastPolledAt: pollAt,
        consecutiveAboveThreshold,
      },
      pairwiseReplayConfig,
    );
    consecutiveAboveThreshold = result.consecutiveAboveThreshold;
    trailingAboveThresholdRun =
      typeof result.growthRateMbPerMin === 'number' && result.growthRateMbPerMin >= config.thresholdMbPerMin
        ? trailingAboveThresholdRun + 1
        : 0;
  }

  const tripIsCurrentlyLive = trailingAboveThresholdRun >= config.consecutivePollsRequired;

  const finalPairWindowStartAt = windowStartAt + (lastIndex - 1) * intervalMs;
  const finalPollAt = windowStartAt + lastIndex * intervalMs;

  return {
    trajectory: [trajectory[lastIndex - 1], trajectory[lastIndex]],
    firstPolledAt: finalPairWindowStartAt,
    lastPolledAt: finalPollAt,
    consecutiveAboveThreshold: tripIsCurrentlyLive ? config.consecutivePollsRequired - 1 : 0,
  };
}

// Mirrors ensureHistoryDir/ensureCoordinationDir exactly: on a fresh machine
// nothing has ever created `~/.claude/agent-state/`, so the first
// --poll-footprint beat would otherwise fail with ENOENT. A genuinely
// unwritable path still degrades gracefully via handlePollFootprint's own
// try/catch around `recordFootprintPoll`.
async function ensureFootprintStateDir(footprintStateFilePath) {
  try {
    await fs.mkdir(dirname(footprintStateFilePath), { recursive: true });
  } catch {
    // Deliberately swallowed — see ensureCoordinationDir's identical comment.
  }
}

// ---------------------------------------------------------------------------
// Phase 6 — watchdog beat's own persisted-state file paths.
// `ARM_LIVENESS_LOG_FILE` mirrors `ARM_HISTORY_FILE`'s/`ARM_COORDINATION_FILE`'s
// own precedent exactly (env override falls back to a real
// `.claude/agent-state/` production default when unset) — it is the name
// `watchdog-outer-acceptance.jest.spec.mjs` itself pins, and every test in
// this file explicitly overrides it.
//
// `ARM_DEADLOCK_STATE_FILE`/`ARM_WATCHDOG_BACKOFF_FILE` are DIFFERENT in one
// respect (-review-style bug found post-implementation, live host
// pollution): their default is NOT an independent `homedir()`-rooted
// literal. Every real caller and every test in this whole file already
// resolves/overrides `coordinationFilePath` (via `resolveCoordinationFilePath()`,
// itself `ARM_COORDINATION_FILE`-overridable) before either of these is ever
// needed, so defaulting these two to SIBLINGS of that same resolved path —
// rather than inventing a second, independent env-var-with-homedir-fallback
// pair the outer-acceptance test was never asked to set — means every test
// that already isolates `ARM_COORDINATION_FILE` (all of them) gets these two
// state files isolated into the SAME tmp dir for free, with zero test-file
// changes. Without this, a real prior watchdog-backoff/deadlock-state write
// under `~/.claude/agent-state/` (from a genuine local run, or simply a
// second consecutive test run within the same debounce/backoff window)
// silently leaks into the next invocation's "is this due"/"is this a fresh
// window" decision — exactly the failure mode this fix closes. An explicit
// `ARM_DEADLOCK_STATE_FILE`/`ARM_WATCHDOG_BACKOFF_FILE` override still wins
// when set, for a caller that genuinely wants an independent location.
// ---------------------------------------------------------------------------

function resolveDeadlockStateFilePath(coordinationFilePath) {
  if (process.env.ARM_DEADLOCK_STATE_FILE) {
    return resolve(process.env.ARM_DEADLOCK_STATE_FILE);
  }
  return join(dirname(coordinationFilePath), 'deadlock-state.json');
}

function resolveLivenessLogFilePath() {
  if (process.env.ARM_LIVENESS_LOG_FILE) {
    return resolve(process.env.ARM_LIVENESS_LOG_FILE);
  }
  return join(homedir(), '.claude/agent-state/liveness-log.json');
}

// review, Medium — deliberately fleet-shared, not per-orchestrator:
// keyed only off `dirname(coordinationFilePath)`, the same directory every
// concurrent orchestrator's own coordination file lives in, with no
// `orchestratorId` in the filename. This is a documented, accepted
// limitation (see `.claude/skills/agent-resource-management/SKILL.md`'s
// "Known, documented limitations" section, "watchdog backoff-state file is
// fleet-shared") — a genuinely per-orchestrator key would need a safe
// filename-sanitization scheme for an otherwise-free-form `orchestratorId`,
// which does not exist anywhere in this skill today, and new test coverage;
// left as a follow-up rather than an unreviewed late change here.
function resolveWatchdogBackoffFilePath(coordinationFilePath) {
  if (process.env.ARM_WATCHDOG_BACKOFF_FILE) {
    return resolve(process.env.ARM_WATCHDOG_BACKOFF_FILE);
  }
  return join(dirname(coordinationFilePath), 'watchdog-backoff.json');
}

/**
 * Mirrors ensureFootprintStateDir/ensureCoordinationDir exactly — creates the
 * given file's parent directory (best-effort, never throws) before the first
 * write. A genuinely unwritable path still degrades gracefully via each
 * caller's own try/catch.
 *
 * @param {string} filePath
 */
async function ensureStateFileDir(filePath) {
  try {
    await fs.mkdir(dirname(filePath), { recursive: true });
  } catch {
    // Deliberately swallowed — see ensureCoordinationDir's identical comment.
  }
}

// The watchdog beat's own cron-interval assumption for `isBackoffDue`'s
// "turns elapsed" math — this repo has no cron/scheduler wiring for this
// beat at all (see .claude/skills/agent-resource-management/SKILL.md's
// watchdog section), so there is no real external cadence to read; this is
// simply the unit `turnsToWait` counts against. Not exposed as a CLI flag —
// out of scope for this phase (see SKILL.md for the documented limitation).
const DEFAULT_WATCHDOG_CRON_INTERVAL_MS = 5 * 60 * 1000;

// Default per-agent stall bound (Phase 1, `isAgentStalled`'s own `boundMs`)
// when `--watchdog-bound-ms` is omitted — 15 minutes, matching this file's
// own `DEFAULT_LIVENESS_THRESHOLD_MS` scale but kept as a distinct constant
// since the two are different concepts (dibs-entry freshness vs per-agent
// CPU/mtime staleness) that should not silently share one number forever.
const DEFAULT_WATCHDOG_BOUND_MS = 15 * 60 * 1000;

// pre-PR review, Medium — how long a pid stays in the flapping-cooldown
// set (`resolveEvictionTargets`'s own `recentlyEvictedPids` hook, previously
// dead code — see `resolveRecentlyEvictedPids` below for the wiring) after
// this beat has already attempted to evict it once, successfully OR not.
// Matches `DEFAULT_WATCHDOG_CRON_INTERVAL_MS` — one skipped beat's worth of
// cooldown is enough to stop a same-target re-SIGTERM on literally every
// beat (the concrete failure mode a `permission-denied` target hits with no
// cooldown at all) without holding a genuinely-recovered pid in the
// cooldown set for unreasonably long.
const DEFAULT_WATCHDOG_EVICT_COOLDOWN_MS = DEFAULT_WATCHDOG_CRON_INTERVAL_MS;

// ---------------------------------------------------------------------------
// hasExplicitFakeCollectInput — decides whether this invocation carries a
// well-formed `ARM_FAKE_COLLECT_JSON` seam value of its own.
//
// A well-formed explicit reading is honoured unconditionally — the seam's
// existing contract across this file's many pre-existing tests is "this
// beat's collect() must return exactly this value", including when several
// distinct/differently-classified beats share the same orchestratorId and
// coordination file. The freshness-window shared-sample reuse below must
// never silently override that.
//
// An UNSET or malformed (fails to parse, or missing `memory`/`disk`)
// `ARM_FAKE_COLLECT_JSON` is treated the same as "no explicit input of its
// own" — matching `collect()`'s own JSON.parse (which would otherwise throw
// and degrade to a stale/AMBER reading): a beat with no genuine reading of
// its own should prefer a still-fresh shared sample over degrading, exactly
// as a real orchestrator with nothing new to report would.
// ---------------------------------------------------------------------------

function hasExplicitFakeCollectInput() {
  const raw = process.env.ARM_FAKE_COLLECT_JSON;
  if (!raw) return false;
  try {
    const parsed = JSON.parse(raw);
    return Boolean(parsed && typeof parsed === 'object' && parsed.memory && parsed.disk);
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// resolveSpawnBucketConfig — ARM_SPAWN_BUCKET_CONFIG_JSON (test seam) mirrors
// ARM_FAKE_COLLECT_JSON's pattern: a well-formed override replaces
// DEFAULT_SPAWN_BUCKET_CONFIG entirely; anything unset or malformed
// (invalid JSON, or missing/non-numeric capacityTokens/refillTokensPerMs)
// falls back to the production default rather than crashing this beat —
// same graceful-degradation posture as the rest of main().
// ---------------------------------------------------------------------------

function resolveSpawnBucketConfig() {
  const raw = process.env.ARM_SPAWN_BUCKET_CONFIG_JSON;
  if (!raw) return DEFAULT_SPAWN_BUCKET_CONFIG;
  try {
    const parsed = JSON.parse(raw);
    if (
      parsed &&
      typeof parsed.capacityTokens === 'number' &&
      typeof parsed.refillTokensPerMs === 'number'
    ) {
      return parsed;
    }
    return DEFAULT_SPAWN_BUCKET_CONFIG;
  } catch {
    return DEFAULT_SPAWN_BUCKET_CONFIG;
  }
}

// ---------------------------------------------------------------------------
// resolveNow — ARM_FAKE_NOW_MS (test seam, Phase 6) mirrors
// ARM_FAKE_COLLECT_JSON's pattern: it fakes the SAMPLE's clock reference, not
// the sample data itself. `cli.mjs` is a fresh, short-lived process per beat
// with no way to fake elapsed wall-clock time across separate child-process
// invocations otherwise — this lets a black-box test simulate elapsed time
// (e.g. driving GREEN-recovery hysteresis, which now gates on
// `DEFAULT_HYSTERESIS_CONFIG.hysteresisRecoverWindowMs`) by passing
// increasing `ARM_FAKE_NOW_MS` values across sequential invocations, without
// ever waiting in real time.
//
// A well-formed integer value is honoured unconditionally. Anything unset,
// empty/whitespace-only, or malformed (non-numeric, non-integer) falls back
// to `Date.now()` rather than crashing this beat — same graceful-degradation,
// don't-fabricate posture as every other ARM_FAKE_*/ARM_*_JSON seam in this
// file. The empty-string case is deliberately rejected explicitly rather than
// left to `Number('')` (which is `0`, and `Number.isInteger(0)` is `true`) —
// an accidentally-exported-but-blank env var must never silently pin a real
// invocation's `now` to epoch 0.
//
// gate: ARM_FAKE_NOW_MS is honoured only when ARM_COORDINATION_FILE is
// ALSO set. The "is it set" check below is the exact same plain-truthy
// expression `resolveCoordinationFilePath()` uses at its own `if
// (process.env.ARM_COORDINATION_FILE)` (cli.mjs:791) — deliberately NOT this
// function's own `.trim() !== ''` idiom, which belongs only to ARM_FAKE_NOW_MS
// itself. `resolveCoordinationFilePath()` treats a whitespace-only value
// (`'   '`) as SET; the gate must agree with that exact assessment, or the
// two functions diverge on whether a coordination file is configured.
//
// a THIRD leg, `ARM_BEAT_NOW_TEST_MODE === '1'` (checked via the
// dedicated `beatNowTestModeEnabled()` helper, same strict-equality idiom as
// `beatSnapshotTestModeEnabled()`), now gates the other two. All three must
// hold before `ARM_FAKE_NOW_MS` is parsed and honoured; any one of them
// missing falls through to `Date.now()`. This is the master switch the
// residual-gap analysis below calls for: the {ARM_FAKE_NOW_MS,
// ARM_COORDINATION_FILE} pair alone is satisfiable by a set-leak (both are
// exported together by a spec's env object, a dotenv file, or a CI job's
// `env:`), but no real production caller — beat, claim, eviction, or
// otherwise — has any reason to set `ARM_BEAT_NOW_TEST_MODE`.
//
// Why gate THIS seam alone: `cli.mjs` has seven env-var test seams in total —
// the five discussed in this paragraph (ARM_COORDINATION_FILE,
// ARM_FAKE_COLLECT_JSON, ARM_FAKE_PS_OUTPUT, ARM_SPAWN_BUCKET_CONFIG_JSON,
// ARM_FAKE_NOW_MS), plus ARM_HISTORY_FILE (history-file path override, see
// resolveHistoryFilePath() above) and ARM_FORCE_COLLECT_FAILURES
// (simulated transient collect failures) — neither of the latter two is a
// "fake an input" seam in the same sense, so they're set aside here rather
// than folded into this comparison. A leaked ARM_FAKE_NOW_MS — left set in a
// shell profile, a CI env, or inherited by a real orchestrator invocation —
// poisons SHARED, cross-process, cross-orchestrator state written into the
// coordination file itself: dibs `declaredAt`, shared-sample `sampledAt`,
// spawn-bucket `lastRefillAt`, and claim-ledger TTL pruning (`declareDibs`'s
// prune-and-persist, `pruneStale`, and `pruneExpiredClaims` — all in
// ./lib/coordination-file.mjs). It is not the only seam with that shape:
// `ARM_FAKE_COLLECT_JSON` has the exact same cross-process write-back path —
// a faked reading returned by `collect()` gets persisted via
// `writeSharedSample()` and read back by a DIFFERENT process, within
// `DEFAULT_FRESHNESS_WINDOW_MS`, as ground truth. `ARM_SPAWN_BUCKET_CONFIG_JSON`
// has it too — `consumeGlobalSpawnTokens` computes `tokens`/`lastRefillAt`
// from the (possibly faked) config and persists them into the shared
// spawn-bucket entry that every other orchestrator reads. `ARM_HISTORY_FILE`
// is not local either — it names a host-global, cross-process-READ history
// file (`lib/history.mjs`'s `recordObservation`/`readHistory`), the same
// class of shared state as the coordination file itself. `ARM_FAKE_PS_OUTPUT`
// is not single-process-local either, on the path that matters most: the
// per-beat sweep classifies and DELETES coordination-file records against the
// beat's process snapshot, so that seam decides shared, cross-orchestrator
// state too — which is why the beat snapshot has its own master switch
// (`ARM_BEAT_SNAPSHOT_TEST_MODE`, see `collectBeatPsSnapshot`) rather than
// relying on the `ARM_COORDINATION_FILE` gate described here.
//
// WHY THAT GATE IS NOT ENOUGH, STATED ACCURATELY (pre-PR review, Medium
// — this passage used to say "which a real invocation always satisfies", and
// that is false: `resolveCoordinationFilePath` falls back to a real production
// default under `~/.claude/agent-state/`, so a real invocation normally sets
// nothing). The gate does stop a LONE stale `ARM_FAKE_PS_OUTPUT`. What it
// cannot stop is a stale test ENVIRONMENT, which is how these variables are
// expected to leak ON OPERATIONAL GROUNDS — not as a structural certainty:
// they are exported together by a spec's env object, a dotenv file or a CI
// job's `env:`, so `ARM_COORDINATION_FILE` arrives alongside the seam it is
// supposed to gate and satisfies it by the same accident that caused the leak.
// A leak carrying only one of the pair WOULD be caught. The switch exists
// because the common case is the one the gate misses, and a safety-relevant
// gate should not rest on the leak taking its less likely shape. A dedicated
// master switch no production caller has any reason to set is the only gate
// that survives the common case.
//
// AND THAT USED TO APPLY TO THIS FUNCTION'S OWN GATE, BEFORE — stated
// here because the two halves of this comment otherwise contradict each
// other (first bot review, High). THIS WAS THE STATE BEFORE
// LANDED: `resolveNow()` below gated `ARM_FAKE_NOW_MS` on
// `ARM_COORDINATION_FILE`'s presence and nothing else, so the set-leak
// reasoning just given defeated it exactly as it defeated the beat seam's
// would-be gate. The two did not differ in how well the gate held; they
// differed in consequence, which is why added a master switch to the
// seam that DELETES records and did not retrofit one here at that time.
//
// THEN CLOSED THIS: `resolveNow()` now requires a third leg,
// `ARM_BEAT_NOW_TEST_MODE === '1'` (see the block above), before it honours
// `ARM_FAKE_NOW_MS` at all — the exact same two-layer master-switch shape as
// the beat-snapshot gate. No production caller (beat, claim, or eviction) has
// any reason to set that switch, so the set-leak that used to defeat this
// gate by accident (a spec's env object, a dotenv file, or a CI job's `env:`
// exporting `ARM_COORDINATION_FILE` alongside `ARM_FAKE_NOW_MS`) is now inert
// on its own — a leak would additionally have to carry the master switch,
// which nothing in production ever sets.
//
// What did NOT close: the containment fallback still does not fully
// cover this function, even with the new switch in place. It holds for the
// sweep, which touches only the file `ARM_COORDINATION_FILE` names
// (`reconcileLedgers` takes `coordinationFilePath`), but `resolveNow()` also
// feeds `recordObservation(resolveHistoryFilePath(), ...)` in
// `handleRecordOutcome` and `peekQueue`/`dequeueItem` on
// `resolveQueueFilePath()` — both of which resolve through their OWN env vars
// (`ARM_HISTORY_FILE`, `ARM_QUEUE_FILE`; see `lib/queue.mjs`'s "deliberately
// independent of `ARM_COORDINATION_FILE`"), neither of which gained a switch
// of its own, so a leak carrying all three of `ARM_FAKE_NOW_MS`,
// `ARM_COORDINATION_FILE`, and `ARM_BEAT_NOW_TEST_MODE` together would still
// reach the real production history and queue files — narrower than before
// (a fourth, deliberately-never-set variable is now required), but not
// a structural impossibility. Extending the same master-switch pattern to
// `ARM_FAKE_COLLECT_JSON`, `ARM_SPAWN_BUCKET_CONFIG_JSON`, and
// `ARM_HISTORY_FILE` (see "Deliberately out of scope" below) was considered
// and deliberately DEFERRED — not implemented, not rejected — at time;
// see and this design's residual-limitations note for the rationale. The
// direction remains the fail-safe one: `resolveProcessSnapshotNow` already
// gates the snapshot anchor behind the beat master switch, so a leaked clock
// here cannot reap a live agent; it can only stamp a future-dated record
// (documented residual limitation 5, itself updated to record its effect).
//
// The pause-candidate call site in `main()` keeps only the weaker
// `ARM_COORDINATION_FILE` gate and no master switch of its own. That is a
// narrower claim than it once carried here: its output is advisory,
// single-process and unpersisted — it reaches no ledger write and no reap —
// but it is NOT true that it "reaches no signal", which this comment asserted
// before the same review. `selectPauseCandidateWithFootprint`'s result becomes
// `buildTrafficLight`'s `pauseCandidate`, i.e. the `agentId` a memory-RED beat
// prints for the orchestrator to act on. A stale `ARM_FAKE_PS_OUTPUT` can
// therefore still name a fabricated agent in a real RED beat's advice. It
// changes no persisted state and reaps nothing, which is why it is left as it
// is here rather than given a switch inside this ticket's scope — but it is a
// residual, not an absence of one. (It is also NOT "ungated", which is what
// this sentence claimed before pre-PR review — `collectPsOutput` has
// required `ARM_COORDINATION_FILE` since this ticket.)
// So this is not "ARM_FAKE_NOW_MS is uniquely dangerous and the others are
// safe" — it is the smallest safe fix for this ticket's actual scope;
// `ARM_FAKE_COLLECT_JSON`, `ARM_SPAWN_BUCKET_CONFIG_JSON`, and
// `ARM_HISTORY_FILE` carry the same class of risk and are deliberately left
// ungated here (see below).
//
// Residual gap this fix does NOT close (accepted tradeoff, not a bug): if a
// FUTURE test sets ARM_FAKE_NOW_MS without also setting ARM_COORDINATION_FILE,
// it silently gets the REAL clock (`Date.now()`) rather than a loud error
// telling the test author their fake-time seam is being ignored. This
// function deliberately does not throw or warn on that path — doing so would
// contradict this file's established graceful-degradation posture, under
// which every other ARM_FAKE_*/ARM_*_JSON seam already falls back silently on
// a malformed/absent value rather than throwing (see `hasExplicitFakeCollectInput`,
// `resolveSpawnBucketConfig`, and this function's own pre-existing
// empty-string handling above for precedent). The safety net for the
// *existing* suite is `./env-seam-premise.jest.spec.mjs` — a structural
// sentinel that asserts every ARM_FAKE_NOW_MS-setting env object in every
// `*.jest.spec.mjs` file in this directory also sets BOTH
// ARM_COORDINATION_FILE AND ARM_BEAT_NOW_TEST_MODE (updated at to
// require the third leg alongside the original pairing). That guard covers
// only this repo's own test suite under
// scripts/agent-resource-management/*.jest.spec.mjs — it says nothing about,
// and cannot enforce anything on, production usage outside it.
//
// Accepted tradeoff, not a defect: neither `--record-outcome` nor
// `--query-capacity` fakes `now` in any existing spec today, but this gate now
// formally requires any FUTURE test on those beats to also set
// ARM_COORDINATION_FILE AND ARM_BEAT_NOW_TEST_MODE (both otherwise-unrelated
// env vars for those beats) if it wants to fake `now`.
//
// Deliberately out of scope: ARM_HISTORY_FILE, ARM_FAKE_COLLECT_JSON, and
// ARM_SPAWN_BUCKET_CONFIG_JSON are also ungated against this same class of
// cross-process poisoning risk. Extending the same master-switch treatment to
// them was considered and deliberately DEFERRED (not implemented, not
// rejected) when closed the equivalent gap for ARM_FAKE_NOW_MS — see
// for the tracked follow-up and this design's residual-limitations note
// for the rationale (its own scope-creep risk flag).
//
// round-4 review — this generic `resolveNow()` (used by every beat,
// eviction included) still honors `ARM_FAKE_NOW_MS` under the
// `ARM_COORDINATION_FILE`-gate described above; that gate is necessary but
// not sufficient for the EVICTION path specifically, because any real
// `--evict-pid` invocation already sets `ARM_COORDINATION_FILE` (it always
// coordinates through the shared coordination file) — a PREMISE THIS FILE
// DISPROVES ELSEWHERE and which survives here only because its diff does
// not reach this block; tracked for correction, along with the
// identical copy in `resolveEvictNow`'s header below. The conclusion still
// holds on the accurate reason (a set-leak satisfies the gate regardless), but
// do not reason from the premise as written. So the gate above is
// always satisfied for a real eviction call and does nothing to stop a
// leaked `ARM_FAKE_NOW_MS` from reaching `isEvictionAuthorized`'s liveness
// check via `readDibs`'s `pruneStale`. `handleEvict` therefore does NOT call
// this function directly — it calls `resolveEvictNow()` below, which adds a
// second, eviction-specific gate (`ARM_EVICT_TEST_MODE`) in front of this
// one. See `resolveEvictNow`'s and `handleEvict`'s own header comments for
// the full two-layer model.
// ---------------------------------------------------------------------------

function resolveNow() {
  const raw = process.env.ARM_FAKE_NOW_MS;
  if (
    beatNowTestModeEnabled() &&
    raw !== undefined &&
    raw.trim() !== '' &&
    process.env.ARM_COORDINATION_FILE
  ) {
    const parsed = Number(raw);
    if (Number.isInteger(parsed)) return parsed;
  }
  return Date.now();
}

/*
 * — where ledger identity comes from, and where it must never come from.
 *
 * This file has FIVE ledger write CALL SITES, reaching FOUR library functions:
 *
 *   1. `main()`'s `declareDibs`                          — lib/coordination-file.mjs
 *   2. `handleClaim`'s `claimCapacity`                    — lib/coordination-file.mjs
 *   3. `main()`'s `reserveAdmission`                      — lib/coordination-file.mjs
 *   4. `performEviction`'s `declareDibs`                  — lib/coordination-file.mjs
 *      (`performEviction` has two callers, `handleEvict` and
 *      `handleWatchdogBeat`, both dispatched from `main()`)
 *   5. `handleDequeueIfCapacity`'s `claimAndAdmitQueueEntry` — lib/dispatch.mjs
 *
 * Every one of them resolves its `pid`/`pidStartedAt` the SAME WAY —
 * `resolveNearestClaudeRootIdentity` (`lib/recon.mjs`) walking from
 * `process.ppid` up to the nearest claude-rooted ancestor in a `ps` snapshot,
 * with `null` meaning "omit both fields", never a sentinel. They do not all
 * read the same snapshot, and the split is deliberate:
 *
 *   SITES 1, 2, 3 and 5 — the ordinary spawn-decision/advisory beats — take
 *   ONE value per beat: `observeBeatProcesses`' `identity`, memoized so the
 *   first caller pays for the collection and every later caller (all four
 *   sites plus all three ledger sweeps) reads back the same rows, the same
 *   `snapshotAt` and the same identity. Two writes in one of these beats
 *   therefore carry byte-identical values; a site that resolved its own would
 *   read a different `now` and silently break that invariant.
 *
 *   SITE 4 — `performEviction` — is an EXPLICIT, DOCUMENTED EXCEPTION. It
 *   resolves its own identity from its own `collectPsOutput(evictTestModeEnabled)`
 *   snapshot (see the comment at that call site for the local argument). It
 *   is not an oversight, and it must not be "fixed" by threading
 *   `observeBeatProcesses`' identity in.
 *
 *   WHY THE EXCEPTION EXISTS. Each beat type owns its own snapshot because
 *   each owns its own test-mode master switch, and a beat must never be able to
 *   enable the other beat type's fake seams by proxy. Both paths follow the
 *   SAME two-layer model — a leaked or stale `ARM_FAKE_PS_OUTPUT` decides
 *   nothing on either — but through different switches:
 *   `collectBeatPsSnapshot(testModeEnabled)` behind
 *   `ARM_BEAT_SNAPSHOT_TEST_MODE` (see that function's header) and
 *   `collectPsOutput(honorFakeSeam)` behind `ARM_EVICT_TEST_MODE` (see
 *   `handleEvict`'s). Each path's `resolveProcessSnapshotNow(...)` anchor is
 *   passed that same path's switch, so the anchor always belongs to the
 *   snapshot actually used.
 *
 *   WHY THE EXCEPTION CANNOT DIVERGE. Divergence would need one beat to write
 *   through both paths, and no beat can: `main()` dispatches
 *   `handleWatchdogBeat` and `handleEvict` — `performEviction`'s only two
 *   callers — and RETURNS from both, ahead of the `observeBeatProcesses()`
 *   call that begins an ordinary beat. `--evict-pid`/`--watchdog-beat` are
 *   also rejected outright when combined with any other beat-type flag. An
 *   eviction beat therefore performs eviction-path resolutions only and never
 *   touches sites 1/2/3/5; an ordinary beat never reaches site 4. There is no
 *   invocation in which two ledger writes could carry different identities.
 *
 *   Both paths degrade fail-safe: a `ps` failure yields `''`, which resolves
 *   to `null`, which omits both fields and leaves the record TTL-governed.
 *
 *   Both paths are counted by `ARM_FAKE_PS_COLLECTION_LOG` (see
 *   `recordProcessSnapshotCollection`), so "how many snapshots did this beat
 *   collect" is observable on the eviction path too, not only on the ordinary
 *   one. What is memoized to exactly one per ordinary beat is the OBSERVATION
 *   `observeBeatProcesses` returns — its rows, its `snapshotAt` and its
 *   identity — not the invocation's total `ps` spawn count: a RED-memory beat
 *   additionally spawns `ps` for `selectPauseCandidateWithFootprint`'s
 *   candidate list, which reads no ledger and writes nothing.
 *   `ledger-reconciliation-beat-wiring.jest.spec.mjs` pins the GREEN beat's
 *   count at exactly one; `cli-evict.jest.spec.mjs` pins the eviction beat's
 *   own collections and the seam rule above.
 *
 * STANDING RULE: ledger identity must never be derived from this process's own
 * `process.pid` / `process.uptime()`. `cli.mjs` is a one-shot, ~62ms-per-beat
 * CLI, not a daemon, and `listAgentProcesses` only ever emits claude-rooted
 * processes (`AGENT_ROOT_COMM_PATTERN = /\/claude$/`), so a `node cli.mjs` pid
 * can never appear in the snapshot the read-side classifier correlates
 * against. Stamping it would make every record structurally invisible to that
 * classifier, and the sweep would reap every live orchestrator.
 */

/**
 * `handleEvict`'s own clock resolver — see `handleEvict`'s header comment for
 * the full two-layer `ARM_EVICT_TEST_MODE` model this belongs to. When
 * `testModeEnabled` is `false` (the master switch is not set), this ALWAYS
 * returns the real `Date.now()`, full stop — it does not even delegate to
 * `resolveNow()`, because `resolveNow()`'s own two-var {ARM_FAKE_NOW_MS,
 * ARM_COORDINATION_FILE} gate is always satisfiable for a real eviction
 * invocation (every real `--evict-pid` call sets `ARM_COORDINATION_FILE`) and
 * so provides no protection on this path specifically from THAT half of the
 * gate alone (round-4 finding). That parenthetical premise about
 * `ARM_COORDINATION_FILE` being unconditionally set is DISPROVED elsewhere in
 * this file — `resolveCoordinationFilePath` has a real production default —
 * and the two-var gate's weakness survives here only because a set-leak (both
 * vars exported together by a spec's env object, dotenv file, or CI `env:`)
 * satisfies it regardless of that default; tracked at.
 *
 * UPDATE: `resolveNow()` now ALSO requires `ARM_BEAT_NOW_TEST_MODE ===
 * '1'` (via `beatNowTestModeEnabled()`) before it honours `ARM_FAKE_NOW_MS` at
 * all — a third leg no real eviction invocation has any reason to set. So the
 * claim above is no longer complete: the two-var half of the gate is still
 * trivially satisfiable by a real eviction call, exactly as described, but
 * `resolveNow()`'s gate AS A WHOLE now does provide real protection on this
 * path, because the new switch is not something `--evict-pid` sets. This
 * function's own `testModeEnabled`-gated short-circuit above is unaffected —
 * it still returns `Date.now()` before ever reaching `resolveNow()` when
 * `ARM_EVICT_TEST_MODE` is unset — but the fallback path (`testModeEnabled`
 * true, deferring to `resolveNow()`) is now doubly gated rather than resting
 * on a gate that provided none.
 *
 * Only when `testModeEnabled` is
 * `true` does this defer to `resolveNow()`, preserving the exact existing
 * `ARM_FAKE_NOW_MS`/`ARM_COORDINATION_FILE`/`ARM_BEAT_NOW_TEST_MODE` behavior
 * every current eviction test already relies on.
 *
 * @param {boolean} testModeEnabled
 * @returns {number}
 */
function resolveEvictNow(testModeEnabled) {
  if (!testModeEnabled) return Date.now();
  return resolveNow();
}

/**
 * Single stderr voice for every graceful-degradation path in this file.
 *
 * `degradedWhat` and `consequence` are parameters rather than a hardcoded
 * tail because the tail used to read "proceeding without
 * dibs for this beat" at all seven sites — including the claim, release,
 * spawn-bucket and shared-sample sites, where dibs are not what degraded and
 * "proceeding" is the opposite of what happens. A warning that misnames both
 * the subsystem and the outcome is worse than no warning: it sends whoever
 * reads it looking in the wrong place. Defaults preserve the original wording
 * byte-for-byte for the one site it was actually accurate for.
 *
 * @param {{ code?: string, message?: string }} error
 * @param {string} [degradedWhat]
 * @param {string} [consequence]
 */
function warnCoordinationDegraded(
  error,
  degradedWhat = 'coordination file',
  consequence = 'proceeding without dibs for this beat',
) {
  process.stderr.write(
    `agent-resource-management: warning: ${degradedWhat} unavailable (${error.code ?? error.message}); ` +
      `${consequence}\n`,
  );
}

// ---------------------------------------------------------------------------
// — the beat's ONE process observation, and the ledger sweep it feeds.
//
// LOCK-HOLD DISCIPLINE (the bug class). `ps` is an external process
// spawn, and a spawn inside a fencing-lock hold makes every other
// orchestrator's write wait on it. The snapshot is therefore taken here,
// unlocked, before any critical section — the same shape
// `computeAvailableCapacity` already uses for its host probe, where the sample
// is taken before `claimCapacity`'s `withLock` and only the freshness-checked
// cached value is read inside. The sweep then acquires a lock PER LEDGER, for
// the classify-and-write step alone, using the snapshot and identity already
// in hand.
//
// EVERY INVOCATION IS ONE BEAT. There is no long-lived daemon in this design —
// an orchestrator runs `cli.mjs` afresh each beat — so "runs at startup and on
// each beat" is satisfied by a single call site early in `main()`'s
// spawn-decision path, not by two code paths.
// ---------------------------------------------------------------------------

/** The dibs array's label in the sweep's logging and in its test seam's keys. */
const DIBS_LEDGER_KEY = 'dibs';

/**
 * The three ledgers one beat reconciles, in sweep order. Each is its own
 * critical section: a failure on one leaves the others already committed, and
 * leaves the failed one untouched for a later beat to converge.
 */
const RECONCILED_LEDGERS = [
  { key: DIBS_LEDGER_KEY, claimType: null },
  { key: LIVE_AGENT_CLAIM_TYPE, claimType: LIVE_AGENT_CLAIM_TYPE },
  { key: LIVE_AGENT_ADMISSION_CLAIM_TYPE, claimType: LIVE_AGENT_ADMISSION_CLAIM_TYPE },
];

/**
 * Appends one line to `ARM_FAKE_PS_COLLECTION_LOG` per process-snapshot
 * collection this invocation performs, so "how many `ps` snapshots did this
 * beat take" is a COUNTABLE fact rather than one inferred from the values that
 * happen to be written.
 *
 * Inference is not enough here: under a fixed snapshot and a fixed clock every
 * resolution derives byte-identical values, so comparing what two ledger call
 * sites wrote cannot distinguish one resolution threaded to both from two
 * resolutions that agreed. The count can, and a beat that re-collects also pays
 * N `ps` spawns on a loaded host — the cost the ordinary beat's memoization
 * exists to avoid.
 *
 * EVERY collection path calls this, not just the ordinary beat's. Both
 * `collectBeatPsSnapshot` (the memoized one-per-beat snapshot behind
 * `observeBeatProcesses`) and `collectPsOutput` (the eviction path's own
 * `ARM_EVICT_TEST_MODE`-gated collector, plus pause-candidate selection) are
 * counted, so the eviction beat's collections are visible rather than silently
 * uncounted. A counter that covered only one of two collection paths would
 * read "one snapshot this beat" on a beat that took several.
 *
 * A test seam like every other `ARM_FAKE_*` here, and gated the same way they
 * are: a master switch (`armTestModeEnabled`) first, `ARM_COORDINATION_FILE`
 * second. `ARM_COORDINATION_FILE` alone cannot gate it — not because "every
 * real invocation sets it" (this comment said so until pre-PR review;
 * `resolveCoordinationFilePath` has a real production default under
 * `~/.claude/agent-state/`, so a real invocation normally sets nothing), but
 * because these variables are expected — on OPERATIONAL grounds, not as a
 * structural certainty — to leak as a SET: a stale test environment (a spec's
 * env object, a dotenv file, a CI job's `env:`) exports the coordination-file
 * override alongside the very seam it is supposed to gate, satisfying it by the
 * same accident that caused the leak. A leak carrying only one of the pair
 * WOULD be caught by that gate; the switches exist because the common case is
 * the one it misses. That is the whole reason the master switches
 * exist. Without the switch, a
 * leaked `ARM_FAKE_PS_COLLECTION_LOG` would make every production `ps`
 * collection append to an env-named path. `armTestModeEnabled` rather than the
 * beat switch alone because this counter deliberately spans both collection
 * paths, and the eviction path's own switch is `ARM_EVICT_TEST_MODE`.
 *
 * Failures to append are swallowed — an observability seam must never be able
 * to fail a beat.
 */
function recordProcessSnapshotCollection() {
  // Master switch FIRST, so a production invocation — neither switch set —
  // returns after one env read rather than reading the seam path it can never
  // use. `collectPsOutput`'s call-site comment asserts exactly this order.
  if (!armTestModeEnabled() || !process.env.ARM_COORDINATION_FILE) return;

  const logPath = process.env.ARM_FAKE_PS_COLLECTION_LOG;
  if (!logPath) return;

  try {
    appendFileSync(logPath, `${process.pid}\n`);
  } catch {
    // Deliberately swallowed — see above.
  }
}

/**
 * Appends one line to `ARM_FAKE_SWEEP_LOCK_LOG` per ledger the sweep enters
 * under the coordination lock, so "no lock is taken when nothing looks dead"
 * is a COUNTABLE fact rather than an unpinned intention.
 *
 * WHY A COUNT IS THE ONLY WAY TO PIN IT. The unlocked pre-pass in
 * `reconcileLedgers` changes no outcome whatsoever: with it or without it the
 * same records are reaped, the same records are spared, and the same bytes end
 * up in the file — the locked pass re-reads and re-judges everything either
 * way, which is exactly what makes the pre-pass safe. Its entire benefit is
 * the lock acquisitions it AVOIDS on the overwhelmingly common beat that has
 * nothing to reap, and lock-collision surface against other orchestrators on
 * this host is not visible in any value the beat writes. Removing the
 * pre-pass is therefore invisible to every assertion about ledger CONTENT;
 * this counter is what makes it visible.
 *
 * Counted at the point the sweep decides to enter a critical section, one line
 * per ledger, so three lines on a beat that locked all three ledgers and none
 * on a beat that locked no ledger. It is a count of the sweep's OWN critical
 * sections, not of every `withLock` in the process — the beat's ordinary
 * declare/claim writes take their own locks and are outside what this
 * measures.
 *
 * Gated exactly as `recordProcessSnapshotCollection` is: the master switch
 * (`armTestModeEnabled`) first, `ARM_COORDINATION_FILE` second.
 * `ARM_COORDINATION_FILE` alone cannot gate it — these variables are expected on
 * operational grounds to leak as a SET (see `recordProcessSnapshotCollection`
 * for the full argument, for why that is an empirical claim rather than a
 * structural one, and for why the "every real invocation sets it" version of it
 * was wrong), so a leaked
 * `ARM_FAKE_SWEEP_LOCK_LOG` would make every production sweep append to an
 * env-named path.
 *
 * `armTestModeEnabled` rather than `beatSnapshotTestModeEnabled` alone is, to
 * be honest about it, WIDER THAN THIS SEAM STRICTLY NEEDS (pre-PR review,
 * Medium — this doc-block used to justify it as "so the seam stays available to
 * a test driving the sweep from either master switch", and no such test can
 * exist). `reconcileLedgers` has one call site, in `main()`, and both beat
 * types that set `ARM_EVICT_TEST_MODE` — `--evict-pid` and `--watchdog-beat` —
 * return from `main()` before it. The sweep is therefore never reachable under
 * the eviction switch alone. It is kept as `armTestModeEnabled` only so that
 * the two recorders in this section are gated identically and neither can drift
 * from the other; a master switch is required either way, which is the property
 * that matters.
 *
 * Failures to append are swallowed — an observability seam must never be able
 * to fail a beat.
 *
 * @param {string} ledgerKey
 */
function recordSweepLockAcquisition(ledgerKey) {
  // Master switch first, then `ARM_COORDINATION_FILE`, then the seam — the
  // order this function's own doc-block states, and the same order
  // `recordProcessSnapshotCollection` uses. Behaviourally identical to reading
  // the seam first (`&&` short-circuits either way), but the stated symmetry
  // between the two recorders should be real.
  if (!armTestModeEnabled() || !process.env.ARM_COORDINATION_FILE) return;

  const logPath = process.env.ARM_FAKE_SWEEP_LOCK_LOG;
  if (!logPath) return;

  try {
    appendFileSync(logPath, `${ledgerKey}\n`);
  } catch {
    // Deliberately swallowed — see above.
  }
}

/**
 * Layer 1 of the beat snapshot's two-layer seam model — the master switch that
 * must be set before `collectBeatPsSnapshot` honours ANY of its `ARM_FAKE_PS_*`
 * seams. See that function's header for the full argument.
 *
 * @returns {boolean}
 */
function beatSnapshotTestModeEnabled() {
  return process.env.ARM_BEAT_SNAPSHOT_TEST_MODE === '1';
}

/**
 * Layer 1 of `resolveNow()`'s three-way seam model — the master
 * switch that must be set, strict `=== '1'` (not merely truthy, matching
 * `beatSnapshotTestModeEnabled()`'s idiom), before `resolveNow()` honours
 * `ARM_FAKE_NOW_MS` at all. Kept as its own helper, deliberately NOT folded
 * into `armTestModeEnabled()` — that function gates unrelated collection
 * counters (`recordProcessSnapshotCollection`/`recordSweepLockAcquisition`),
 * and folding this switch in would silently widen those functions' own
 * behavior to a switch that has nothing to do with them.
 *
 * @returns {boolean}
 */
function beatNowTestModeEnabled() {
  return process.env.ARM_BEAT_NOW_TEST_MODE === '1';
}

/**
 * True when EITHER of this file's two master switches is set — the beat
 * snapshot's `ARM_BEAT_SNAPSHOT_TEST_MODE` or the eviction path's
 * `ARM_EVICT_TEST_MODE`.
 *
 * For the seams that are not owned by one beat path. `recordProcessSnapshotCollection`
 * counts collections on both paths, so gating it on either switch alone would
 * make it silently inert for tests of the other. Seams that belong to exactly
 * one path keep gating on that path's own switch — this is the exception, not
 * a replacement for them.
 *
 * @returns {boolean}
 */
function armTestModeEnabled() {
  return beatSnapshotTestModeEnabled() || process.env.ARM_EVICT_TEST_MODE === '1';
}

/**
 * Collects this beat's `ps` snapshot, reporting collection FAILURE separately
 * from an empty result — the distinction `collectPsOutput` cannot express.
 *
 * TWO-LAYER SEAM MODEL, the same shape `handleEvict` uses for the eviction
 * beat's `ARM_EVICT_TEST_MODE` (round 4; see that function's header for
 * the original argument). It is applied here because this snapshot is the
 * LIVENESS ORACLE `reconcileLedgers` reaps against: whatever decides what this
 * snapshot contains decides which records the beat deletes from the SHARED
 * coordination file. That is a strictly wider blast radius than the same seam
 * carries on `main()`'s pause-candidate path, where it decides one process's
 * advisory ranking and nothing that is written or acted on.
 *
 *   Layer 1 — `ARM_BEAT_SNAPSHOT_TEST_MODE=1` must be set before this function
 *   honours ANY `ARM_FAKE_PS_*` seam. Unset (the default, and true of every
 *   real orchestrator invocation, which has no reason to ever set it) this
 *   function shells out to the real `ps` and reports real failures, regardless
 *   of what any `ARM_FAKE_PS_*` var happens to hold. A leaked or stale
 *   `ARM_FAKE_PS_OUTPUT` is therefore INERT BY CONSTRUCTION on this path — not
 *   inert because each seam was individually remembered, but because the
 *   real-`ps` branch never reads `process.env.ARM_FAKE_PS_*` at all when the
 *   switch is off. A future seam added here inherits that property as long as
 *   its real-vs-fake branch is written the same way, gated on the
 *   `testModeEnabled` parameter. `observeBeatProcesses` threads the same flag
 *   into `resolveProcessSnapshotNow`, so the `etime` anchor always belongs to
 *   the clock the snapshot actually used.
 *
 *   The `ARM_COORDINATION_FILE` gate cannot serve as Layer 1 here,
 *   because these variables are expected — on OPERATIONAL grounds, not as a
 *   structural certainty — to leak as a SET rather than one at a time: whatever
 *   stale environment carries `ARM_FAKE_PS_OUTPUT` (a spec's env object, a
 *   dotenv file, a CI job's `env:`) carries the coordination-file
 *   override with it and satisfies the gate by the same accident that caused the
 *   leak — exactly the
 *   finding that produced `ARM_EVICT_TEST_MODE`. A leak carrying only one of the
 *   pair WOULD be caught by the gate; the switch exists because the common case
 *   is the one it misses, and a gate this safety-relevant should not rest on the
 *   leak taking its less likely shape. The same reasoning defeats the gate
 *   on `ARM_FAKE_NOW_MS` in `resolveNow()`, which is a known residual not
 *   hardened by this work — see `resolveNow`'s own comment and.
 *   (NOT because "every real
 *   invocation of this CLI sets it", which this passage asserted until
 *   pre-PR review and which is false — `resolveCoordinationFilePath` falls back
 *   to a real production default.) It is kept below as a second condition on all three
 *   seams, not relied on as the switch: it cannot stop a leak, but it does
 *   confine one to an invocation that already names its own coordination file,
 *   so a mistakenly-set switch cannot sweep a faked tree against the real
 *   default ledger at `~/.claude/agent-state/resource-coordination.json`.
 *
 *   Layer 2 — the seams this switch governs, enumerated: `ARM_FAKE_PS_OUTPUT`,
 *   `ARM_FAKE_PS_FAILURE`, `ARM_FAKE_PS_DELAY_MS`. Unlike `handleEvict`'s
 *   `EVICT_FAKE_SEAM_VARS`, this enumeration carries NO all-or-nothing coupling
 *   check, and must not grow one: those three describe mutually exclusive
 *   snapshot conditions (a fixed snapshot, a failed collection, a slow
 *   collection), not several facets of one faked decision, so a test that sets
 *   one deliberately leaves the others unset. The enumeration is here so the
 *   switch's governed surface is stated rather than inferred.
 *
 *   THE ONE RESIDUAL that decision accepts, stated so the reader does not have
 *   to work it out: with the switch ON and `ARM_FAKE_PS_OUTPUT` unset, this
 *   function returns the REAL host process tree and the beat sweeps its
 *   configured ledger against it. That is a confusing test — a fixture's ghost
 *   records are classified against whatever happens to be running — but it is
 *   not an unsafe one: the ledger swept is the one `ARM_COORDINATION_FILE`
 *   names, and a real tree can only ever spare records, never invent liveness.
 *   An all-or-nothing coupling check would trade that confusion for a worse
 *   failure, since the three seams are mutually exclusive by design.
 *
 * The delay seam models a slow `ps` on a loaded host and changes only latency,
 * never the output — it is what makes the lock-hold discipline above
 * observable, since a concurrent orchestrator's own write is unaffected by it.
 * `ARM_FAKE_PS_OUTPUT` deliberately does NOT disable collection, resolution or
 * the sweep: with the master switch set, a beat with a fake snapshot behaves
 * exactly as one with a real snapshot.
 *
 * @param {boolean} testModeEnabled Layer 1 above — `false` forces the real
 *   `ps` shell-out regardless of every `ARM_FAKE_PS_*` var.
 * @returns {Promise<{ ok: boolean, output: string }>}
 */
async function collectBeatPsSnapshot(testModeEnabled) {
  recordProcessSnapshotCollection();

  if (testModeEnabled && process.env.ARM_COORDINATION_FILE) {
    const delayMs = Number(process.env.ARM_FAKE_PS_DELAY_MS);
    if (Number.isInteger(delayMs) && delayMs > 0) {
      await new Promise((resolveDelay) => setTimeout(resolveDelay, delayMs));
    }
    if (process.env.ARM_FAKE_PS_FAILURE === '1') return { ok: false, output: '' };
  }

  if (testModeEnabled && process.env.ARM_COORDINATION_FILE && process.env.ARM_FAKE_PS_OUTPUT) {
    return { ok: true, output: process.env.ARM_FAKE_PS_OUTPUT };
  }

  try {
    return { ok: true, output: runPsProbe() };
  } catch {
    return { ok: false, output: '' };
  }
}

/**
 * The instant a process snapshot's `etime` column is measured against, which is
 * NOT interchangeable with the beat's own `now`.
 *
 * `startedAt`/`pidStartedAt` are both `snapshotNow - etimeSeconds * 1000`, so
 * the anchor has to belong to the same clock the elapsed times do. A REAL `ps`
 * reports elapsed times the kernel measured against the real clock, so a real
 * snapshot is anchored to the real clock even on a beat whose own `now` comes
 * from `ARM_FAKE_NOW_MS` — otherwise two beats stepping that seam forward would
 * derive identities differing by the size of the step, and the second beat
 * would read the first's still-live record as pid reuse and reap it. A FAKE
 * snapshot's elapsed times are fixtures written in the same fictional time
 * `ARM_FAKE_NOW_MS` describes, so that beat's `now` is exactly the right
 * anchor for them.
 *
 * In production neither seam is set and this is `Date.now()` either way.
 *
 * @param {boolean} [honorFakeSeam] Each collection path passes its OWN master
 *   switch's value — `ARM_EVICT_TEST_MODE` from the eviction path,
 *   `ARM_BEAT_SNAPSHOT_TEST_MODE` from `observeBeatProcesses`. `false` means
 *   that path forced the real `ps` shell-out regardless of
 *   `ARM_FAKE_PS_OUTPUT`, and the anchor must follow the snapshot actually
 *   used.
 * @returns {number}
 */
function resolveProcessSnapshotNow(honorFakeSeam = true) {
  // The SAME triple BOTH collectors test before honouring `ARM_FAKE_PS_OUTPUT`
  // — `collectBeatPsSnapshot` and `collectPsOutput` alike: master switch
  // (threaded in as `honorFakeSeam`), `ARM_COORDINATION_FILE`, then the seam
  // itself. The anchor has to agree with the collector about which snapshot was
  // actually taken, and any condition differing from the collectors' own would
  // let the two disagree about provenance whenever that difference is
  // observable. Keep all three conditions identical.
  return honorFakeSeam && process.env.ARM_COORDINATION_FILE && process.env.ARM_FAKE_PS_OUTPUT
    ? resolveNow()
    : Date.now();
}

/**
 * Memoized per invocation, which is what makes "one observation and one
 * identity per ordinary beat" a property of the code rather than a convention
 * call sites are trusted to follow: the first caller pays for the collection and
 * every later caller — ledger write call sites 1, 2, 3 and 5, plus all three
 * ledger sweeps — reads back the same rows, the same `snapshotAt`, and the
 * same identity. Call site 4 (`performEviction`) is reached only from beat
 * types that return before this function is ever called, and resolves from its
 * own `ARM_EVICT_TEST_MODE`-gated snapshot; see the standing-rule block near
 * `resolveNow` for why that exception exists and why it cannot diverge.
 */
let beatProcessObservation = null;

/**
 * This beat's single view of the host's process tree.
 *
 * TWO WAYS A SNAPSHOT CAN BE NO OBSERVATION AT ALL, and both yield
 * `liveProcesses: null` rather than `[]`. `classifyLedgerEntryLiveness` reads
 * `null` as "no snapshot" and defers every record to its ledger's own TTL,
 * whereas `[]` is a valid observation of zero agents and makes every
 * pid-bearing record confirmed dead — so the whole fleet is reaped by anything
 * that reaches `[]` without having actually looked.
 *
 *   COLLECTION FAILED — `ps` threw, or the failure seam fired. Nothing came
 *   back to read.
 *
 *   COLLECTION SUCCEEDED BUT THE TEXT IS NOT A LEGIBLE PROCESS TABLE — `ps`
 *   exited zero and produced text `classifyProcessSnapshotShape` judges
 *   anything other than `'readable'`. Two verdicts land here and BOTH are
 *   degraded (pre-PR review, Medium — only the first was checked):
 *
 *     `'unreadable'` — no MAJORITY of the non-header lines parses under this
 *     project's column grammar INCLUDING their etime field (the etime half of
 *     that test is load-bearing, see that function). A column-order, locale, or
 *     header drift lands here, and already shipped one silent `ps`
 *     incompatibility of exactly this kind, so it is a real failure mode rather
 *     than a defensive hypothetical.
 *
 *     `'empty'` — no non-header line at all. On a host running this very sweep
 *     that is not an observation of an idle machine: the sweeping `node`
 *     process and its own claude-rooted parent are alive, and `ps -A`
 *     enumerates every process on the host, so zero body lines can only mean
 *     the collection came back broken behind an exit code of zero.
 *
 *   `listAgentProcesses` returns `[]` for either, indistinguishably from a host
 *   running no agents — which is exactly why neither may be passed through.
 *
 * `classifyProcessSnapshotShape` still keeps the two verdicts DISTINCT, and
 * should: it judges text and text alone, and cannot know who is running. Which
 * of its verdicts amount to "no observation" is this function's call, and both
 * do.
 *
 * Either degraded case also leaves `identity` `null`, so this beat's own writes
 * record no identity at all — exactly what a standalone invocation writes —
 * rather than an identity derived from evidence that never arrived or could not
 * be read.
 *
 * @returns {Promise<{ liveProcesses: Array<object> | null, identity: { pid: number, pidStartedAt: number } | null, snapshotAt: number, snapshotIssuedAt: number }>}
 */
async function observeBeatProcesses() {
  if (beatProcessObservation === null) {
    beatProcessObservation = (async () => {
      // Layer 1 of this path's two-layer seam model, resolved ONCE and threaded
      // into both the collection and its clock anchor, so the anchor can never
      // belong to a different snapshot than the one collected.
      const testModeEnabled = beatSnapshotTestModeEnabled();
      // TWO INSTANTS, AND THEY ARE NOT INTERCHANGEABLE (pre-PR review, High).
      //
      // `snapshotIssuedAt` is read BEFORE the collection and `snapshotAt`
      // AFTER it, so they straddle the whole `ps` shell-out — up to
      // `DEFAULT_PROBE_TIMEOUT_MS` apart on a loaded host, which is precisely
      // the gap `PID_IDENTITY_TOLERANCE_MS` is derived from.
      //
      //   `snapshotAt` is the ETIME ANCHOR, and stays the post-collection read.
      //   `etime` was measured by the kernel when `ps` was issued, so anchoring
      //   the derived `startedAt` to the later instant pushes it later by the
      //   collection's duration — which is error source (1) that
      //   `PID_IDENTITY_TOLERANCE_MS` is sized to absorb. To be precise about
      //   why it stays rather than overstate it: the tolerance is an UPPER
      //   BOUND, so moving this anchor earlier too would make the derivation
      //   more conservative, not invalid. It is left alone because the tolerance
      //   was measured against this anchor, because the eviction path's own
      //   identity resolution anchors the same way, and because a change that
      //   only tightens an already-safe bound is not this ticket's to make.
      //
      //   `snapshotIssuedAt` is the FRESHNESS BOUNDARY the sweep's
      //   `isWrittenAfterSnapshot` guard measures against, because the
      //   snapshot's CONTENTS were fixed when `ps` was issued, not when it
      //   returned. A claude root born during the collection is carried by no
      //   row in it, yet a record its beat stamped before `snapshotAt` would
      //   pass a guard anchored to `snapshotAt` and be reaped `pid-absent` —
      //   a live-agent reap through the guard that exists to prevent one.
      //   Anchoring to the earlier instant only ever DEFERS more records, which
      //   is the fail-safe direction (a spared record still ages out by TTL).
      const snapshotIssuedAt = resolveProcessSnapshotNow(testModeEnabled);
      const collected = await collectBeatPsSnapshot(testModeEnabled);
      const snapshotAt = resolveProcessSnapshotNow(testModeEnabled);

      if (!collected.ok) {
        warnCoordinationDegraded(
          { code: 'PROCESS_SNAPSHOT_UNAVAILABLE' },
          'process snapshot',
          'reaping nothing this beat and writing no process identity; every ledger keeps its own TTL',
        );
        return { liveProcesses: null, identity: null, snapshotAt, snapshotIssuedAt };
      }

      // BOTH degraded shapes, not just `'unreadable'` (pre-PR review, Medium —
      // this branch checked `'unreadable'` alone). `'empty'` means `ps` exited
      // zero and produced no body line at all, and `listAgentProcesses` answers
      // `[]` for it — a valid observation of zero live agents, which condemns
      // every pid-bearing record in all three ledgers. On a host running this
      // sweep that observation is impossible: the sweeping `node` process and
      // its own claude-rooted parent are both alive and `ps -A` enumerates
      // them, so zero body lines is a broken collection wearing an exit code of
      // zero, never an idle host. (`classifyProcessSnapshotShape` keeps
      // `'empty'` a DISTINCT verdict from `'unreadable'` on purpose — it is a
      // pure judgement about text and asserts nothing about who is running.
      // Which of its verdicts count as "no observation" is this caller's
      // decision, and it is made here.)
      const snapshotShape = classifyProcessSnapshotShape(collected.output);
      if (snapshotShape !== 'readable') {
        warnCoordinationDegraded(
          { code: 'PROCESS_SNAPSHOT_UNREADABLE' },
          'process snapshot',
          `the snapshot was ${snapshotShape} — it carried no parseable process rows, so it is treated as ` +
            'no observation: reaping nothing this beat and writing no process identity; ' +
            'every ledger keeps its own TTL',
        );
        return { liveProcesses: null, identity: null, snapshotAt, snapshotIssuedAt };
      }

      // `liveProcesses` is derived LAZILY, then memoized on first read. The
      // ledger sweep is its only consumer, and the beat types that resolve an
      // identity without sweeping (`--claim`, `--dequeue-if-capacity`) return
      // before the sweep is ever reached — they should not pay to parse a
      // process table nobody reads. The single-collection invariant is
      // untouched: the `ps` spawn already happened above, this defers only the
      // derivation from its text, and the getter's memo keeps repeated reads
      // to one parse.
      let liveProcesses;
      return {
        get liveProcesses() {
          if (liveProcesses === undefined) liveProcesses = listAgentProcesses(collected.output, snapshotAt);
          return liveProcesses;
        },
        // The walk starts at the PARENT: this `node` process can never be
        // claude-rooted, and the orchestrator that spawned it is the process
        // whose life the ledger record actually tracks.
        identity: resolveNearestClaudeRootIdentity(collected.output, process.ppid, snapshotAt),
        snapshotAt,
        snapshotIssuedAt,
      };
    })();
  }
  return beatProcessObservation;
}

/**
 * This beat's resolved ledger identity as the optional fields a write call site
 * spreads into its params. `null` yields no fields at all — absent, never a
 * sentinel, because a placeholder pid matches no snapshot row and so reads as a
 * death certificate for a live agent on the next sweep.
 *
 * @returns {Promise<{ pid?: number, pidStartedAt?: number }>}
 */
async function resolveBeatLedgerIdentity() {
  const { identity } = await observeBeatProcesses();
  return identity === null ? {} : { pid: identity.pid, pidStartedAt: identity.pidStartedAt };
}

/**
 * Builds the `beforeCommit` hook that makes one ledger's reap write fail at its
 * commit point, from `ARM_FAKE_SWEEP_WRITE_FAILURES_JSON` — a `{ ledgerKey:
 * errorCode }` object.
 *
 * Gated on `ARM_BEAT_SNAPSHOT_TEST_MODE` first and `ARM_COORDINATION_FILE`
 * second, the same two-layer shape `collectBeatPsSnapshot` uses, and for the
 * same reason: `ARM_COORDINATION_FILE` leaks in the same breath as the seam it
 * would gate (see `recordProcessSnapshotCollection`), so on its own it gates
 * next to nothing. This seam in particular must not be
 * reachable in production — it does not merely fake a reading, it ABANDONS the
 * sweep for a ledger and reports a fabricated `COORDINATION_LOCK_TIMEOUT`/
 * `COORDINATION_LOCK_LOST` for it, so a leaked value would both disable ghost
 * reaping and send an operator after a lock problem that does not exist.
 * The beat switch is the right one: the sweep is the beat path's own write.
 *
 * Scoped to the SWEEP's own writes: a beat's ordinary `declareDibs`/
 * `claimCapacity`/`reserveAdmission`/`claimAndAdmitQueueEntry` writes are never
 * touched by it, which is what lets a test observe partial-sweep behavior
 * against a beat that otherwise proceeds normally. A malformed value, or a
 * ledger the object does not name, sweeps normally.
 *
 * @param {string} ledgerKey
 * @returns {(() => never) | undefined}
 */
function buildSweepWriteFailure(ledgerKey) {
  if (
    !beatSnapshotTestModeEnabled() ||
    !process.env.ARM_COORDINATION_FILE ||
    !process.env.ARM_FAKE_SWEEP_WRITE_FAILURES_JSON
  ) {
    return undefined;
  }

  let configured;
  try {
    configured = JSON.parse(process.env.ARM_FAKE_SWEEP_WRITE_FAILURES_JSON);
  } catch {
    return undefined;
  }

  const lockPath = `${resolveCoordinationFilePath()}.lock`;
  const code = configured?.[ledgerKey];
  if (code === 'COORDINATION_LOCK_TIMEOUT') {
    return () => {
      throw new LockTimeoutError(lockPath);
    };
  }
  if (code === 'COORDINATION_LOCK_LOST') {
    return () => {
      throw new LockLostError(lockPath);
    };
  }
  return undefined;
}

/**
 * Whether a ledger record was written into the file at or after the instant the
 * sweep's process snapshot was taken — in which case the snapshot predates the
 * record and can say nothing at all about it.
 *
 * WHY A SWEEP NEEDS THIS. The snapshot is fixed before any lock is taken (it
 * must be — spawning `ps` inside a lock hold is the bug class), and the
 * sweep then waits for up to three locks in turn. On a contended host that wait
 * is seconds, not microseconds. An orchestrator whose claude root was born
 * during that wait writes a record naming a pid that no row of the sweeper's
 * older snapshot can carry, which reads as `pid-absent` — a confident
 * `confirmed-dead` verdict on a live agent. The fencing lock does not help:
 * it makes the write atomic, not the verdict fresh, and waiting for it is what
 * opens the window.
 *
 * THE BOUNDARY IS THE INSTANT `ps` WAS ISSUED, NOT THE INSTANT IT RETURNED
 * (pre-PR review, High — this guard was anchored to `snapshotAt`, which is
 * read AFTER the collection). The snapshot's contents were fixed when the
 * kernel served the `ps`; everything born after that is missing from it, and
 * the collection itself can take up to `DEFAULT_PROBE_TIMEOUT_MS` on the loaded
 * host this system exists for. A claude root born inside that window, whose
 * beat stamps its record before the collection returns, is carried by no row
 * AND passes a `snapshotAt`-anchored guard — a live-agent reap straight through
 * the guard written to prevent one. `observeBeatProcesses` therefore reads
 * `snapshotIssuedAt` before the collection and threads it here, while
 * `snapshotAt` stays the etime anchor it was derived to be. Widening the guard
 * only ever defers MORE records, and a deferred record still ages out under its
 * ledger's own TTL.
 *
 * The guard is the record's OWN write stamp, so no extra evidence is needed and
 * no second snapshot is taken. `declaredAt` on a dibs entry, `claimedAt` on a
 * claim record — every record carries one already. Boundary is inclusive: a
 * stamp equal to `snapshotIssuedAt` is a record the snapshot may or may not
 * have seen, and "may not" is the direction that costs a live agent its record.
 *
 * A record with NEITHER stamp usable is not deferred here. Its age is
 * unknowable from the record itself, and deferring every unstamped record would
 * exempt a whole class from reaping on a ground that is not evidence; it falls
 * through to the classifier's verdict, exactly as before.
 *
 * @param {object} record
 * @param {number} snapshotIssuedAt Epoch-ms instant the `ps` was ISSUED — see
 *   the block above for why this is not `snapshotAt`.
 * @returns {boolean}
 */
function isWrittenAfterSnapshot(record, snapshotIssuedAt) {
  const stamps = [record?.declaredAt, record?.claimedAt].filter((stamp) => Number.isFinite(stamp));
  return stamps.some((stamp) => stamp >= snapshotIssuedAt);
}

/**
 * Reaps every ledger record this beat's snapshot proves dead, across the dibs
 * array and both claim ledgers, so the freed capacity is already gone from the
 * file before this same beat reads it for its own allowance/admission math.
 *
 * The reap write goes through `reapConfirmedDeadRecords`, which is
 * `coordination-file.mjs`'s ordinary `withLock`/`writeEntriesAtomic` path — the
 * same choke point every other writer uses, so the fencing re-check cannot be
 * skipped. A lost or timed-out lock therefore abandons that ledger's whole
 * sweep rather than writing part of it: the ledger keeps its own TTL for this
 * beat, says so on stderr, and a later beat converges it.
 *
 * Only `confirmed-dead` reaps. `unknown` — no identity, a corrupt one, or no
 * snapshot — defers to the pre-existing TTL untouched, and TTL-only prunes are
 * not logged here. Reserved `__claim:<type>__` container rows are never
 * candidates. A record the snapshot PREDATES is deferred too, whatever the
 * classifier said about it — see `isWrittenAfterSnapshot`.
 *
 * THE UNLOCKED PRE-PASS, AND THE LINE IT MUST NOT CROSS. Three ledgers means
 * three acquire/release cycles and three whole-file reads per beat, and the
 * overwhelmingly common case has nothing to reap — "nothing to do means no
 * write" already skips the write, but not the lock. So each ledger is first
 * read WITHOUT the lock and classified; only a ledger with at least one
 * apparently-dead record is entered under lock. The pre-pass is a filter and
 * never the authority: everything is re-read and re-judged inside the critical
 * section, and a record that changed in between is judged on what THAT read
 * sees. It deliberately applies the classifier ALONE, without
 * `isWrittenAfterSnapshot`, so its candidate set is a strict SUPERSET of what
 * the locked pass will reap — a filter that can only ever over-lock, never skip
 * a ledger that had work.
 *
 * ITS COST, STATED ACCURATELY (pre-PR review, Low — this said "one unlocked
 * read, which is what the locked pass would have paid anyway", and that is
 * wrong for the ledger that HAS work). `reapConfirmedDeadRecords` re-reads the
 * file inside its own critical section, so a ledger with a candidate now pays
 * TWO whole-file reads instead of one. The trade is deliberate and lopsided in
 * the right direction: the no-work case is the overwhelmingly common one and it
 * saves a whole lock cycle there, while the extra read falls only on the beat
 * that was about to take a lock and rewrite the file anyway.
 *
 * Because the pre-pass changes no outcome, nothing about the ledger's CONTENT
 * can distinguish a beat that took three locks from one that took none. The
 * `ARM_FAKE_SWEEP_LOCK_LOG` seam (see `recordSweepLockAcquisition`) makes the
 * count itself observable, which is what holds the pre-pass in place.
 *
 * — IDENTITY CROSS-CHECK GUARD reuses this same unlocked pre-pass per
 * ledger: when `identity === null` and `liveProcesses` is a literal empty
 * array, a ledger whose pre-pass finds a would-be-confirmed-dead candidate
 * has its reap skipped (no lock, no write) rather than trusted. A ledger with
 * nothing to reap is entirely unaffected. Exactly one warning is emitted for
 * the whole beat, after the per-ledger loop, and only if at least one ledger
 * was actually suppressed — a beat with nothing to reap anywhere is a
 * complete no-op, indistinguishable from before landed.
 *
 * @param {string} coordinationFilePath
 * @param {{ liveProcesses: Array<object> | null, identity: { pid: number, pidStartedAt: number } | null, snapshotAt: number, snapshotIssuedAt: number }} observation
 *   `snapshotAt` is the classifier's reference instant; `snapshotIssuedAt` is
 *   the freshness boundary — see `isWrittenAfterSnapshot` for why they differ.
 *   `identity` is the beat's own resolved ledger identity (`null` if no
 *   claude-rooted ancestor was found) — read by the guard below.
 * @returns {Promise<void>}
 */
async function reconcileLedgers(coordinationFilePath, observation) {
  // — IDENTITY CROSS-CHECK GUARD. `observation.liveProcesses: []` is
  // ambiguous between a genuinely agentless host (no positive identity signal
  // at all — not even this beat's own orchestrator appears in the snapshot)
  // and a host where this beat's own identity resolves but nothing ELSE is
  // running. Only the former is drift-suspect: `identity === null` together
  // with a literal empty array means the snapshot cannot even vouch for the
  // sweeper itself, so a pid-bearing record's absence from it is not
  // trustworthy evidence of death. Deliberately narrower than
  // `liveProcesses === null` (the pre-existing
  // PROCESS_SNAPSHOT_UNREADABLE/UNAVAILABLE path already handles that shape
  // above, before reconcileLedgers is ever called): this guard only fires on
  // a READABLE snapshot that resolves to a valid, empty array.
  //
  // It is evaluated PER-LEDGER, reusing the SAME unlocked pre-pass read
  // (`readLedgerCandidateRecords` + `candidates.some(isConfirmedDead)`) that
  // already exists below — no second read pass. A ledger whose pre-pass finds
  // no would-be-confirmed-dead candidate is entirely unaffected by the guard:
  // it has nothing to reap either way, so there is nothing to protect and
  // nothing to warn about. Only when the pre-pass DOES find a candidate does
  // the guard actually change behaviour, by skipping that ledger's reap
  // (no lock, no write) instead of taking the critical section.
  //
  // Exactly one warning is emitted for the whole beat, after the loop, and
  // only if the guard actually suppressed a reap on at least one ledger — a
  // beat where nothing was ever at risk of being wrongly reaped is a
  // complete no-op, indistinguishable from a beat before landed. Fail-
  // safe direction unchanged: every suppressed record still ages out by its
  // own ledger's TTL.
  const driftGuardActive =
    observation.identity === null && Array.isArray(observation.liveProcesses) && observation.liveProcesses.length === 0;
  let driftGuardSuppressedAReap = false;

  for (const ledger of RECONCILED_LEDGERS) {
    // Verdict reasons, keyed by the record they were reached for, so each
    // reaped record can be logged with the token that condemned it. Populated
    // inside the critical section, read after it commits — nothing is reported
    // as reaped until the write that removed it has landed.
    const reasonByRecord = new Map();
    // Records the classifier condemned and the snapshot-freshness guard spared.
    // Collected rather than written inline, for the same reason reaps are: a
    // critical section is no place to write to a pipe that can block.
    const deferred = [];

    const isConfirmedDead = (record) =>
      classifyLedgerEntryLiveness(record, observation.liveProcesses, observation.snapshotAt).status ===
      LIVENESS_STATUS.CONFIRMED_DEAD;

    try {
      const candidates = await readLedgerCandidateRecords(coordinationFilePath, ledger.claimType);
      if (!candidates.some(isConfirmedDead)) continue;

      // — the pre-pass found a would-be-confirmed-dead candidate on
      // this ledger. If the drift guard is active, this is the one shape
      // where it actually matters: skip the reap for THIS ledger only (no
      // lock, no write) and record that something was suppressed, so the
      // single post-loop warning fires. Every other ledger this beat is
      // untouched by this branch.
      if (driftGuardActive) {
        driftGuardSuppressedAReap = true;
        continue;
      }

      // Counted here rather than inside `reapConfirmedDeadRecords` so the
      // measurement is of the sweep's OWN critical sections. See
      // `recordSweepLockAcquisition` for why the pre-pass above cannot be
      // pinned by any assertion on ledger content.
      recordSweepLockAcquisition(ledger.key);

      const { reaped } = await reapConfirmedDeadRecords(coordinationFilePath, {
        claimType: ledger.claimType,
        isDead: (record) => {
          const liveness = classifyLedgerEntryLiveness(record, observation.liveProcesses, observation.snapshotAt);
          if (liveness.status !== LIVENESS_STATUS.CONFIRMED_DEAD) return false;
          if (isWrittenAfterSnapshot(record, observation.snapshotIssuedAt)) {
            deferred.push(record);
            return false;
          }
          reasonByRecord.set(record, liveness.reason);
          return true;
        },
        beforeCommit: buildSweepWriteFailure(ledger.key),
      });

      // One terse line per reaped record — target and reason, never a dump of
      // the ledger's contents.
      for (const record of reaped) {
        process.stderr.write(
          `agent-resource-management: reaped ${ledger.key} record for ${record?.orchestratorId} ` +
            `(pid ${record?.pid}): ${reasonByRecord.get(record)}\n`,
        );
      }

      // Deferrals are reported in the same vocabulary as reaps, so a record
      // that looked dead and was spared is visible rather than a silent
      // difference between two beats' output.
      for (const record of deferred) {
        process.stderr.write(
          `agent-resource-management: deferred ${ledger.key} record for ${record?.orchestratorId} ` +
            `(pid ${record?.pid}): ${LIVENESS_REASON.WRITTEN_AFTER_SNAPSHOT}\n`,
        );
      }
    } catch (error) {
      warnCoordinationDegraded(
        error,
        `ledger reconciliation for ${ledger.key}`,
        `${ledger.key} was left unswept for this beat and keeps its own TTL until a later beat converges it`,
      );
    }
  }

  // — exactly one warning per beat, only if the guard actually
  // suppressed a reap on at least one ledger above. Nothing seeded anywhere
  // means this line never runs, matching pre-existing behaviour exactly.
  if (driftGuardSuppressedAReap) {
    warnCoordinationDegraded(
      { code: 'AGENT_RECOGNITION_DRIFT_SUSPECTED' },
      'process identity',
      'the snapshot resolved no claude-rooted ancestor for this beat and carried zero live agent processes, so ' +
        'a pid-bearing record\'s absence from it cannot be trusted as evidence of death; at least one ledger had ' +
        'a would-be-confirmed-dead record and the reap was deliberately withheld this beat, every record still ' +
        'ages out by its own TTL',
    );
  }
}

// ---------------------------------------------------------------------------
// ensureCoordinationDir — creates the coordination file's parent directory
// (e.g. `~/.claude/agent-state/`) before the first `declareDibs` call. On a
// fresh machine nothing has ever created this directory, so every
// `declareDibs` would otherwise fail with ENOENT and every real run would
// silently, permanently degrade to "no dibs". A genuinely unwritable path
// (e.g. permissions) still degrades gracefully via the try/catch here —
// `main()`'s own warnCoordinationDegraded path handles the subsequent
// declareDibs failure exactly as before.
// ---------------------------------------------------------------------------

async function ensureCoordinationDir(coordinationFilePath) {
  try {
    await fs.mkdir(dirname(coordinationFilePath), { recursive: true });
  } catch {
    // Deliberately swallowed: an unwritable parent dir is caught here rather
    // than crashing the CLI. The subsequent declareDibs call will fail on
    // its own and be handled by main()'s existing warnCoordinationDegraded
    // path, so this beat simply proceeds without dibs.
  }
}

// ---------------------------------------------------------------------------
// handleRecordOutcome — `--record-outcome` is a distinct,
// standalone beat type: it persists one observation via `lib/history.mjs`'s
// `recordObservation` and prints a minimal confirmation JSON, WITHOUT ever
// running the normal collect -> classify -> dibs -> traffic-light pipeline.
// Never composed with `--desired-agents`/`--heartbeat` in the same
// invocation — see the exact contract at the top of
// ./record-outcome.jest.spec.mjs.
// ---------------------------------------------------------------------------

async function handleRecordOutcome(flags) {
  if (flags['desired-agents'] !== undefined || flags.heartbeat !== undefined) {
    const conflictingFlag = flags['desired-agents'] !== undefined ? '--desired-agents' : '--heartbeat';
    process.stderr.write(
      `agent-resource-management: --record-outcome cannot be combined with ${conflictingFlag} in the same ` +
        'invocation — recording is a standalone beat type, not a spawn-decision beat\n',
    );
    process.exit(2);
    return;
  }

  const operationType = typeof flags['operation-type'] === 'string' ? flags['operation-type'] : undefined;
  if (!operationType) {
    process.stderr.write(
      'agent-resource-management: --operation-type=<type> is required for --record-outcome\n',
    );
    process.exit(2);
    return;
  }

  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write(
      'agent-resource-management: --orchestrator-id=<id> is required for --record-outcome\n',
    );
    process.exit(2);
    return;
  }

  const now = resolveNow();

  // Phase 3 — `--pid=<pid>` (optional, additive) threads the REAL
  // `startedAt` (and, where available, the accumulated smoothed peak /
  // trajectory) from any prior `--poll-footprint` beats for the SAME pid
  // (matched by pid alone — see ./poll-footprint.jest.spec.mjs's header
  // comment for why pid alone is this feature's chosen key), rather than
  // this beat's own historical `startedAt = endedAt = now` default. Absent
  // `--pid`, or a pid with no prior poll beats on record, falls straight
  // through to that unchanged default — see the tests pinning both shapes in
  // ./poll-footprint.jest.spec.mjs.
  //
  // Phase 3 review (Medium) — the read (to thread this pid's prior
  // state into the observation below) and the evict (so the persisted store
  // doesn't grow unboundedly across a long-lived orchestrator's high pid
  // churn — this `--record-outcome` beat is the natural "this pid's run is
  // over" signal) MUST happen as one atomic operation, not two separate
  // lock acquisitions: a concurrent `--poll-footprint` for the SAME pid
  // landing in the gap between an unlocked read and a later clear would
  // write a newer sample that the clear then silently deleted before it was
  // ever recorded. `consumeFootprintState` closes that gap by reading and
  // evicting under one lock. Best-effort on failure — a failed consume
  // (read+evict) leaves any prior state behind (self-healing on this pid's
  // NEXT `--poll-footprint`, which overwrites rather than appends) rather
  // than failing this beat; per Phase 3 review's Low finding, an unexpected
  // read failure here means the evict never happened either (they're one
  // operation now), so there is no separate "read failed but we still tried
  // to clear" case left to handle.
  let priorFootprintState = null;
  const rawPid = flags.pid;
  if (typeof rawPid === 'string') {
    const parsedPid = Number(rawPid);
    if (Number.isInteger(parsedPid)) {
      const footprintStateFilePath = resolveFootprintStateFilePath();
      // whole-branch review, Low finding — on a fresh machine nothing
      // has ever created `~/.claude/agent-state/`, so the very first
      // `--record-outcome --pid=<pid>` call would otherwise hit `consumeFootprintState`'s
      // ENOENT path and emit a spurious warning below. Mirrors
      // `ensureHistoryDir`/`ensureCoordinationDir` already being called
      // before their respective operations elsewhere in this file.
      await ensureFootprintStateDir(footprintStateFilePath);
      try {
        // round-3 review, Medium finding ("asymmetric staleness
        // guard") — this read path used to consume whatever persisted
        // footprint state existed for this pid with NO staleness check, even
        // though `--pid` is optional/additive: a `--record-outcome
        // --pid=<pid>` call that never had a corresponding fresh
        // `--poll-footprint` for THIS run would harvest a previous,
        // unrelated process's state after a macOS pid recycle. Passing the
        // same `DEFAULT_LIVENESS_THRESHOLD_MS` threshold the write path uses
        // (above) makes a stale entry here fall through to `null`, exactly
        // as if `--poll-footprint` had never been called for this pid — the
        // beat then proceeds with its existing now=now default.
        priorFootprintState = await consumeFootprintState(footprintStateFilePath, parsedPid, {
          now,
          staleAfterMs: DEFAULT_LIVENESS_THRESHOLD_MS,
        });
      } catch (error) {
        process.stderr.write(
          `agent-resource-management: warning: failed to consume footprint poll state ` +
            `(${error.code ?? error.message}); proceeding with the default now=now duration\n`,
        );
        priorFootprintState = null;
      }
    } else {
      // whole-branch review, Low finding — `--poll-footprint`'s own
      // `<pid>` argument hard-rejects a non-integer value with exit 2;
      // `--record-outcome --pid=<garbage>` previously degraded silently to
      // the default now=now duration with no signal at all. `--record-outcome`
      // fails open everywhere else in this beat, so a non-integer `--pid` is
      // WARNED about on stderr (visibility) rather than hard-rejected
      // (consistent with this beat's own graceful-degradation posture) —
      // the beat still proceeds and still exits 0.
      process.stderr.write(
        `agent-resource-management: warning: --pid must be an integer pid, got "${rawPid}"; ` +
          'proceeding with the default now=now duration\n',
      );
    }
  }

  const hasRealStartedAt =
    priorFootprintState !== null && Number.isFinite(priorFootprintState.firstPolledAt);

  const observation = {
    operationType,
    orchestratorId,
    startedAt: hasRealStartedAt ? priorFootprintState.firstPolledAt : now,
    endedAt: now,
  };

  if (hasRealStartedAt) {
    // whole-branch review, Medium finding — `peakMemoryMb` and the
    // EWMA-smoothed `phys_footprint` peak are TWO DIFFERENT METRICS with
    // different provenance (the exact metric-disagreement class behind the
    // RSS-inversion incident): legacy `--peak-memory-mb` callers
    // supply a `ps`-RSS-shaped number, while this is a smoothed
    // `phys_footprint` peak. This value is therefore written to a NEW,
    // distinct field (`peakSmoothedFootprintMb`), never into `peakMemoryMb`
    // — see `history.mjs`'s `recordObservation` JSDoc for the full
    // provenance contract. `estimateOperationCost` reads only
    // `peakMemoryMb`/`crashed`, so this is purely additive: an
    // observation with ONLY polling data (no `--peak-memory-mb`) leaves
    // `peakMemoryMb` absent, exactly as before this fix.
    if (Number.isFinite(priorFootprintState.peakSmoothedMb)) {
      observation.peakSmoothedFootprintMb = priorFootprintState.peakSmoothedMb;
      observation.peakMemorySource = 'phys-footprint-ewma';
    }
    if (Array.isArray(priorFootprintState.trajectory) && priorFootprintState.trajectory.length > 0) {
      observation.footprintTrajectory = priorFootprintState.trajectory;
    }
    // whole-branch review, High finding — `agentClass` was parsed by
    // `--poll-footprint` and echoed into that beat's own confirmation JSON,
    // but never persisted into the state entry `--record-outcome` reads
    // back here, so no observation could ever actually carry it despite
    // `history.mjs`'s own JSDoc documenting it as a real field. Now that
    // `lib/footprint-state.mjs` persists it, thread it through.
    if (typeof priorFootprintState.agentClass === 'string') {
      observation.agentClass = priorFootprintState.agentClass;
    }
  }

  if (flags['peak-memory-mb'] !== undefined) {
    const rawPeakMemoryMb = flags['peak-memory-mb'];
    const parsedPeakMemoryMb = Number(rawPeakMemoryMb);
    if (typeof rawPeakMemoryMb !== 'string' || !Number.isFinite(parsedPeakMemoryMb)) {
      process.stderr.write(
        `agent-resource-management: --peak-memory-mb must be a number, got "${rawPeakMemoryMb}"\n`,
      );
      process.exit(2);
      return;
    }
    observation.peakMemoryMb = parsedPeakMemoryMb;
  }

  // `--crashed` boolean-flag validation (Phase 5 review, third pass,
  // Medium #2) — reuses `validateBareOrTrueFlag` (the `--heartbeat`-derived
  // shared helper above) rather than `if (flags.crashed)`'s prior truthiness
  // check, which silently accepted `--crashed=false` as truthy and persisted
  // `crashed: true` — corrupting the exact signal `estimateOperationCost`
  // relies on to exclude crashed observations from its clean-sample median.
  const crashedValidation = validateBareOrTrueFlag(flags, 'crashed');
  if (!crashedValidation.valid) {
    process.exit(2);
    return;
  }
  if (crashedValidation.isSet) {
    observation.crashed = true;
  }

  // Fails open — same posture as every other coordination/history-file I/O
  // path in this file (see warnCoordinationDegraded's precedent above): a
  // beat that can't persist evidence this time is strictly better than one
  // that kills the orchestrator's calling loop. Still exits 0 and still
  // prints a best-effort confirmation JSON.
  //
  // write-side fencing recheck — `LockLostError` (thrown by
  // `history.mjs`'s `recordObservation` when it loses its fencing lock
  // mid-write) is one of the errors this catch is known to cover correctly:
  // it's a documented, deliberately-introduced throw from this exact call
  // site now, not just an incidental one. Its `.code`
  // (`COORDINATION_LOCK_LOST`, set in `LockLostError`'s constructor in
  // `coordination-file.mjs`) is what `error.code ?? error.message` below
  // picks up, so the stderr warning is informative, not `undefined`.
  const historyFilePath = resolveHistoryFilePath();
  await ensureHistoryDir(historyFilePath);
  try {
    await recordObservation(historyFilePath, observation);
  } catch (error) {
    process.stderr.write(
      `agent-resource-management: warning: failed to record outcome history (${error.code ?? error.message}); ` +
        'proceeding without persisting this beat\n',
    );
  }

  process.stdout.write(JSON.stringify({ recorded: true, operationType, orchestratorId }) + '\n');
}

// ---------------------------------------------------------------------------
// handlePollFootprint — `--poll-footprint=<pid>` is a fourth
// distinct, standalone beat type, mirroring `handleRecordOutcome`'s own shape
// exactly: one-shot, no collect -> classify -> dibs -> traffic-light
// pipeline, never composed with `--desired-agents`/`--heartbeat` in the same
// invocation. Samples the given pid's physical footprint ONCE (via Phase 1's
// `sampleFootprint`), folds that sample into a PERSISTED per-pid EWMA-
// smoothing/trajectory state that survives across separate one-shot CLI
// invocations (Phase 2's algorithm, replayed here across processes by
// `lib/footprint-state.mjs` — see that module's own header for why), and
// prints a minimal confirmation JSON. NEVER an internal loop/daemon: an
// external orchestrator re-invokes this beat every 10-30s per live pid —
// see the exact contract at the top of ./poll-footprint.jest.spec.mjs.
// ---------------------------------------------------------------------------

async function handlePollFootprint(flags) {
  // Mutual-exclusion guard, per the Build Plan's explicit AC — scoped to
  // `--desired-agents`/`--heartbeat` only. It is NOT cross-checked against
  // `--claim`/`--query-capacity`/`--record-outcome`/`--dequeue-if-capacity`/
  // `--ack-lease`/`--release-lease`; combining `--poll-footprint` with any of
  // those silently runs the earlier beat in dispatch order instead of
  // rejecting with exit 2. This mirrors a pre-existing gap (those beats don't
  // fully cross-check each other either) — not required to close here,
  // but flagged here so a future reader doesn't mistake it for a
  // completeness bug in THIS guard specifically.
  if (flags['desired-agents'] !== undefined || flags.heartbeat !== undefined) {
    const conflictingFlag = flags['desired-agents'] !== undefined ? '--desired-agents' : '--heartbeat';
    process.stderr.write(
      `agent-resource-management: --poll-footprint cannot be combined with ${conflictingFlag} in the same ` +
        'invocation — polling a footprint is a standalone one-shot beat type, not a spawn-decision beat\n',
    );
    process.exit(2);
    return;
  }

  const operationType = typeof flags['operation-type'] === 'string' ? flags['operation-type'] : undefined;
  if (!operationType) {
    process.stderr.write(
      'agent-resource-management: --operation-type=<type> is required for --poll-footprint\n',
    );
    process.exit(2);
    return;
  }

  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write(
      'agent-resource-management: --orchestrator-id=<id> is required for --poll-footprint\n',
    );
    process.exit(2);
    return;
  }

  const rawPid = flags['poll-footprint'];
  const pid = typeof rawPid === 'string' ? Number(rawPid) : NaN;
  if (typeof rawPid !== 'string' || !Number.isInteger(pid) || pid <= 0) {
    process.stderr.write(
      `agent-resource-management: --poll-footprint=<pid> must be a positive integer pid, got "${rawPid}"\n`,
    );
    process.exit(2);
    return;
  }

  // A missing/invalid --agent-class degrades to undefined (unlabelled) —
  // never crashes this beat (documented contract, ./poll-footprint.jest.spec.mjs).
  const agentClass = typeof flags['agent-class'] === 'string' ? flags['agent-class'] : undefined;

  const now = resolveNow();
  const sampleMb = await sampleFootprint(pid);

  const footprintStateFilePath = resolveFootprintStateFilePath();
  await ensureFootprintStateDir(footprintStateFilePath);
  try {
    // whole-branch review, High finding — `agentClass` is now threaded
    // into the persisted per-pid state, not just echoed into this beat's own
    // stdout.
    //
    // round-3 review, Medium finding ("threshold too tight") —
    // `staleAfterMs` used to be 3x `MAX_FOOTPRINT_POLL_INTERVAL_MS` (90s),
    // which false-fires on this codebase's real beat cadence (orchestrators
    // call this CLI "after each sub-agent's task completes", which can
    // legitimately be minutes apart — a sub-agent task in progress, a `hold`
    // state, a paused agent). `DEFAULT_LIVENESS_THRESHOLD_MS` (15 minutes,
    // above) is this codebase's own settled notion of "how long before we
    // stop trusting a beat-driven entry is still live" for exactly that
    // reason, so this now reuses it directly rather than inventing an
    // independent, much tighter threshold.
    await recordFootprintPoll(footprintStateFilePath, pid, sampleMb, now, {
      agentClass,
      staleAfterMs: DEFAULT_LIVENESS_THRESHOLD_MS,
    });
  } catch (error) {
    process.stderr.write(
      `agent-resource-management: warning: failed to persist footprint poll state ` +
        `(${error.code ?? error.message}); proceeding without persisting this beat\n`,
    );
  }

  // whole-branch review, Medium finding — `sampleMb` (the raw
  // sampleFootprint() result, a finite number or `null` on failure) is now
  // included so a caller can distinguish "sampled fine" from "footprint AND
  // vmmap both failed this poll" (missing binary, permissions, wrong pid) —
  // previously indistinguishable from stdout alone, since `polled: true` was
  // printed unconditionally regardless of whether the sample itself
  // succeeded.
  process.stdout.write(
    JSON.stringify({ polled: true, pid, orchestratorId, operationType, agentClass, sampleMb }) + '\n',
  );
}

// ---------------------------------------------------------------------------
// handleQueryCapacity — `--query-capacity=<types>` is a
// third standalone, non-spawn-decision beat type, structurally the same kind
// of thing as `--record-outcome`: an advisory read that never declares dibs
// and never drives the collect -> classify -> dibs -> traffic-light pipeline
// (that pipeline's dibs step is specific to a spawn decision this beat never
// makes). It still needs a live axis reading (to gate `freeCapacity` to `0`
// on AMBER/RED, exactly as `--desired-agents` gates `spawn-allowed`) — so it
// does call `takeSample(collect)` and the axis classifiers, but with no prior
// hysteresis history threaded in (this beat doesn't participate in the dibs
// coordination file at all, so there is no per-orchestrator history to read),
// which is fine: a first-ever classification with no history is exactly the
// same "worsening always trips immediately" / "solo-GREEN reports GREEN
// immediately" logic `applyHysteresis` already gives a brand-new axis.
//
// Phase 2 (Build Plan Amendment, RC-3): `freeCapacity` used to be
// `computeHeadroomCap` called once PER requested type against the live (or
// `assumedFreeRamMb`-derived) `freeRamMb` sample, with `perAgentRamMb` set to
// that type's own `estimateOperationCost(...).estimatedPeakMemoryMb` — a
// memory-heavier type reported a lower `freeCapacity` than a lighter one.
// RC-3 found `freeRamMb` inverts under macOS memory-compressor pressure, so
// it — and the `assumedFreeRamMb` fallback formula derived from it — are
// fully retired as inputs here, live or absent. Every requested type falls
// back independently to the flat `DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis`
// (3) placeholder cap — still one `freeCapacity` entry per requested type
// (the response shape is unchanged), but no longer differentiated by that
// type's own cost estimate. `confidence` is untouched — it still comes
// straight from `estimateOperationCost` per type.
//
// Phase 3 note: `--query-capacity` was deliberately left OUT of scope
// for the live-agent ceiling (see the Build Plan's judgment call #1 —
// `--query-capacity` is documented as a standalone, non-composable advisory
// read, not the crash-causing beat). It still reports the flat-3 placeholder
// for every type, `live-agent` included — this is a PERMANENT scope
// decision, not a "until a later phase" placeholder; `main()`'s real
// `--desired-agents` admission decision and `--claim=live-agent:N`'s ledger
// are the only two call sites this scope decision is about (Phase 3
// UPDATE: both now resolve the AIMD-adjusted ceiling, not the flat
// `LIVE_AGENT_CEILING` (4) — see `handleDequeueIfCapacity` for a third,
// later-added call site outside this file's `--claim`/`--desired-agents`
// pair, and `LIVE_AGENT_CEILING`'s own doc comment for the full list).
// ---------------------------------------------------------------------------

async function handleQueryCapacity(flags) {
  const typesRaw = flags['query-capacity'];
  const types =
    typeof typesRaw === 'string'
      ? typesRaw
          .split(',')
          .map((type) => type.trim())
          .filter((type) => type.length > 0)
      : [];

  // Missing/empty `--query-capacity` value must hard-reject (Phase 5
  // review, third pass, Medium #1) — mirroring `--operation-type`'s/
  // `--orchestrator-id`'s existing required-parameter precedent in
  // `handleRecordOutcome` above, rather than silently succeeding with `{}`.
  // Unlike `--desired-agents` (which has a documented default of `1`), this
  // flag's value IS the entire payload of this beat type — `--query-capacity`
  // bare (parseArgs yields `true`), `--query-capacity=` (empty string), or a
  // value that's only commas/whitespace all derive an empty `types` array and
  // are equally malformed: there is no reasonable default list of operation
  // types to fall back to.
  if (types.length === 0) {
    const received = typeof typesRaw === 'string' ? typesRaw : String(typesRaw);
    process.stderr.write(
      `agent-resource-management: --query-capacity=<types> requires at least one non-empty comma-separated ` +
        `operation type, got "${received}"\n`,
    );
    process.exit(2);
    return;
  }

  const historyFilePath = resolveHistoryFilePath();
  const now = resolveNow();

  const rawSample = await takeSample(collect);
  const memoryResult = classifyMemoryAxis(
    rawSample.memory,
    DEFAULT_MEMORY_THRESHOLDS,
    undefined,
    DEFAULT_HYSTERESIS_CONFIG,
    undefined,
    now,
  );
  const diskResult = classifyDiskAxis(
    rawSample.disk,
    DEFAULT_DISK_THRESHOLDS,
    undefined,
    DEFAULT_HYSTERESIS_CONFIG,
    undefined,
    now,
  );

  // Both axes GREEN gates whether this beat has ANY headroom to report at
  // all — mirrors `--desired-agents`'s own GREEN-only `spawn-allowed` gate
  // (an AMBER/RED axis zeroes `freeCapacity` for every requested type in the
  // same response, never just one; `confidence` per type stays independently
  // correct regardless — see handleQueryCapacity's header comment above).
  const axisGreen = memoryResult.state === 'GREEN' && diskResult.state === 'GREEN';

  const result = {};
  for (const type of types) {
    // Fail-open per-type (Phase 5 review, Medium/Correctness): a single
    // type's history read failure (e.g. malformed JSON, permission error —
    // `readHistory`/`history.mjs`'s underlying read only tolerates `ENOENT`)
    // must not crash the whole beat for every OTHER requested type. Mirrors
    // `handleRecordOutcome`'s existing degrade-with-stderr-warning posture,
    // scoped to just this type: a cold-start default estimate is reported
    // for this type only, and the loop continues.
    //
    // The `try` is deliberately narrowed to wrap ONLY `readHistory` — the
    // genuine I/O call — and NOT `estimateOperationCost`, which is a pure
    // function over already-validated history data. Wrapping both would let
    // a programming error inside `estimateOperationCost` (e.g. a malformed
    // `history` object reaching a `.filter`/`.map`) get silently caught and
    // misreported as "failed to read history for type", masking a real
    // defect behind a cold-start fallback instead of surfacing it.
    let history;
    try {
      history = await readHistory(historyFilePath, type);
    } catch (error) {
      process.stderr.write(
        `agent-resource-management: warning: failed to read history for type "${type}" ` +
          `(${error.code ?? error.message}); falling back to a default cost estimate for this type\n`,
      );
      history = { raw: [], summary: null };
    }
    const costEstimate = estimateOperationCost(type, history);

    // Phase 2 placeholder cap (see this function's header comment):
    // every type independently gets the flat maxAgentsMemoryAxis cap when
    // both axes are GREEN, never a cost-derived number.
    const freeCapacity = axisGreen ? DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis : 0;

    result[type] = { freeCapacity, confidence: costEstimate.confidence };
  }

  process.stdout.write(JSON.stringify(result) + '\n');
}

// ---------------------------------------------------------------------------
// handleClaim — `--claim=<type>:<count>` is the authoritative
// WRITE half of the query/claim split `--query-capacity` (Phase 4) began: it
// re-validates capacity INSIDE the existing fencing lock
// (`./lib/coordination-file.mjs`'s `claimCapacity`, itself built on the same
// `withLock`/`acquireLock`/`releaseLock` mechanism `declareDibs` uses — no
// second lock primitive) and atomically reserves whatever is genuinely still
// available at the moment the lock is held, closing the race window a
// `--query-capacity` advisory read alone cannot close.
//
// `computeAvailableCapacity` below is the "total genuinely-free capacity for
// this type right now" callback `claimCapacity` invokes once, INSIDE its own
// critical section. Phase 2 (Build Plan Amendment, RC-3): it used to
// re-derive capacity via the same `computeHeadroomCap`/`estimateOperationCost`
// /`readHistory` pipeline `handleQueryCapacity` used, including that
// function's live-vs-`assumedFreeRamMb` fallback (Phase 5 doc-drift
// pass: this named a `hasLiveFreeRamMb` binding here. That binding was real
// in this file — added by `5ed2433f`, retired by an earlier phase of —
// so it is not fictitious, just gone; grepping the current tree finds
// nothing).
// `freeRamMb` (live
// or that fallback) is now fully retired as an admission input here too, for
// the same RC-3 reason (see `handleQueryCapacity`'s header comment above):
// capacity falls back unconditionally to the flat
// `DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis` (3) placeholder cap for
// every claim type EXCEPT `LIVE_AGENT_CLAIM_TYPE` — see
// `probeAvailableCapacity`'s own type-specific branch below, added in Phase 3
// so this ledger's writer agrees with `main()`'s reader on the SAME ceiling
// (Phase 3 UPDATE: the AIMD-adjusted value, read via
// `readAimdCeilingState` — no longer the flat `LIVE_AGENT_CEILING` (4)
// constant), rather than the two silently disagreeing on how large the
// ledger is allowed to grow. The fail-open-per-type history-read posture is
// unchanged.
// ---------------------------------------------------------------------------

async function handleClaim(flags) {
  const claimSpec = parseClaimSpec(flags.claim);
  if (!claimSpec) {
    const received = typeof flags.claim === 'string' ? flags.claim : String(flags.claim);
    process.stderr.write(
      `agent-resource-management: --claim=<type>:<count> requires a non-empty operation type and a ` +
        `non-negative integer count, got "${received}"\n`,
    );
    process.exit(2);
    return;
  }

  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write('agent-resource-management: --orchestrator-id=<id> is required for --claim\n');
    process.exit(2);
    return;
  }
  if (isReservedEntry(orchestratorId)) {
    process.stderr.write(
      `agent-resource-management: --orchestrator-id must not match the reserved double-underscore convention ` +
        `("__...__"), got "${orchestratorId}" — reserved ids collide with internal coordination-file bookkeeping ` +
        '(e.g. the legacy-unattributed-claim sentinel) and are not valid orchestrator ids\n',
    );
    process.exit(2);
    return;
  }

  const { type, count: requestedCount } = claimSpec;

  // review follow-up: `LIVE_AGENT_ADMISSION_CLAIM_TYPE` is an
  // internal-only ledger written exclusively by `reserveAdmission` (the
  // `--desired-agents` beat) — see that constant's own doc comment, which
  // asserts it is "never seeded, claimed, or released via the public
  // `--claim`/`--release` flags." Until this guard, nothing enforced that:
  // a stray `--claim=live-agent-admission:N` silently inflated the ledger
  // `computeAvailableCapacity` (below) subtracts from EVERY other
  // orchestrator's `live-agent` headroom, with host-wide blast radius for a
  // full `DEFAULT_FRESHNESS_WINDOW_MS`. Reject it explicitly rather than
  // let it through read-blind.
  if (type === LIVE_AGENT_ADMISSION_CLAIM_TYPE) {
    process.stderr.write(
      `agent-resource-management: --claim type "${LIVE_AGENT_ADMISSION_CLAIM_TYPE}" is internal-only ` +
        '(written exclusively by the --desired-agents beat via reserveAdmission) and cannot be claimed ' +
        'directly\n',
    );
    process.exit(2);
    return;
  }

  // Same posture, same rationale — see `IDLE_PROBE_CLAIM_TYPE`'s own doc
  // comment: internal-only, written exclusively by the --desired-agents
  // beat's --user-attests-idle idle-probe admission path.
  if (type === IDLE_PROBE_CLAIM_TYPE) {
    process.stderr.write(
      `agent-resource-management: --claim type "${IDLE_PROBE_CLAIM_TYPE}" is internal-only ` +
        '(written exclusively by the --desired-agents beat via --user-attests-idle) and cannot be claimed ' +
        'directly\n',
    );
    process.exit(2);
    return;
  }

  const coordinationFilePath = resolveCoordinationFilePath();
  const now = resolveNow();

  await ensureCoordinationDir(coordinationFilePath);

  // — this beat's ledger identity, resolved here for exactly the reason
  // the host probe below is sampled here: the `ps` shell-out behind it must sit
  // outside `claimCapacity`'s critical section, so a slow `ps` never becomes a
  // lock hold every other orchestrator waits on.
  const beatLedgerIdentity = await resolveBeatLedgerIdentity();

  // -------------------------------------------------------------------------
  // Phase 5 — the host probe runs BEFORE the lock, not inside it.
  //
  // Everything below used to run inside `claimCapacity`'s critical section,
  // which meant up to eight blocking `execFileSync` spawns (see `collect()`)
  // happened while every other orchestrator on the host sat blocked on the
  // same lock file — on precisely the thrashing host this skill exists to
  // manage.
  //
  // Does hoisting weaken the grant? No. `computeAvailableCapacity` was
  // invoked with zero arguments and closed over `handleClaim` locals only: it
  // never received any lock-protected state. `vm_stat`/`df` sample the live
  // machine and `readHistory` reads a DIFFERENT file this lock does not
  // guard, so none of its inputs were ever protected by holding the lock.
  // The only lock-protected input, `alreadyGrantedTotal`, is applied AFTER
  // the callback returns. What makes two concurrent claimers safe is the
  // ledger's read-modify-write under the lock — never the serialisation of
  // the host probe.
  //
  // What hoisting DOES introduce is a sample-to-decision gap the width of the
  // lock wait, whose failure direction is over-admission (a sample taken
  // before pressure rose over-states free RAM). That is the same class of bug
  // this ticket closes, so it is bounded rather than accepted: the sample is
  // stamped and the in-lock closure fails CLOSED when it is older than
  // `DEFAULT_CLAIM_SAMPLE_FRESHNESS_MS`. Phase 4's per-shell-out timeout and
  // this freshness bound are complementary, not redundant — one bounds how
  // long the probe may take, the other how stale its result may be by the
  // time it is used.
  //
  // Probe-storm tradeoff, stated rather than ignored: the lock did serialise
  // concurrent `--claim` probes, and that serialisation is gone. It is
  // accepted because (a) the beat path in `main()` already samples entirely
  // outside any lock, so unserialised concurrent probing is the status quo
  // for this skill, not something introduced here; (b) `--claim` is a
  // one-shot process invoked at most once per orchestrator per beat, and the
  // host runs a small, bounded number of orchestrators; and (c) Phase 4 now
  // caps each shell-out's wall-clock cost, which the in-lock version never
  // did. Reusing the shared-machine-sample entry here the way `main()`
  // already does would remove the storm entirely and is the natural follow-up
  // — it is deliberately out of scope for this ticket.
  //
  // Tradeoff of the non-GREEN short-circuit below: a non-GREEN beat no longer
  // touches the coordination file at all, so it also no longer performs the
  // opportunistic legacy-ledger migration write a zero-grant claim used to do
  // as a side effect. That migration still happens on the next GREEN claim or
  // on any release, so it is delayed, not lost — and not paying lock and
  // write costs on a host already classified unhealthy is the whole point.
  // -------------------------------------------------------------------------

  async function probeAvailableCapacity() {
    const rawSample = await takeSample(collect);
    // Stamped HERE — the instant the reading was actually taken — not at
    // handler entry. Everything after this point (history read, cost
    // estimate, and above all the lock wait) is what the freshness window is
    // reserved for; everything BEFORE it, including `collect()`'s bounded
    // retry of up to eight shell-outs, is time the sample did not yet exist
    // for. Stamping at entry charged the probe's own ~16s worst case against
    // a 20s budget sized for a ~15s lock ceiling, which would deny grants
    // under ordinary contention — the exact functional regression
    // DEFAULT_CLAIM_SAMPLE_FRESHNESS_MS's own rationale forbids.
    //
    // Same `resolveNow()` seam as everything else, so ARM_FAKE_NOW_MS still
    // drives both this stamp and the comparison against it.
    const sampledAt = resolveNow();

    // — a probe that failed is NOT the same as a host that is genuinely
    // unhealthy, and until now `--claim` could not tell them apart from the
    // outside. `takeSample()` swallows every `collect()` throw into a
    // `{stale: true}` marker, which classifies non-GREEN, which prints
    // `{"granted":0}` with EMPTY stderr — byte-identical to a legitimately
    // RED host. That makes the new per-shell-out timeout invisible on the one
    // command path that grants capacity. Say so instead.
    if (rawSample.stale) {
      warnCoordinationDegraded(
        totalCollectFailure ?? new Error('host sample unavailable or malformed'),
        'host-resource probe',
        'granting nothing for this claim — no usable host sample was taken, ' +
          'which is NOT the same as the host being genuinely at capacity',
      );
    }

    const memoryResult = classifyMemoryAxis(
      rawSample.memory,
      DEFAULT_MEMORY_THRESHOLDS,
      undefined,
      DEFAULT_HYSTERESIS_CONFIG,
      undefined,
      now,
    );
    const diskResult = classifyDiskAxis(
      rawSample.disk,
      DEFAULT_DISK_THRESHOLDS,
      undefined,
      DEFAULT_HYSTERESIS_CONFIG,
      undefined,
      now,
    );

    if (!(memoryResult.state === 'GREEN' && diskResult.state === 'GREEN')) {
      return { axisGreen: false, availableCapacity: 0, sampledAt };
    }

    // Phase 2 (Build Plan Amendment, RC-3): `freeRamMb` (live or the
    // retired `assumedFreeRamMb` fallback) no longer feeds capacity here —
    // see this section's header comment above for the full rationale. A
    // per-type cost estimate is no longer needed to derive `availableCapacity`
    // either, since the placeholder cap is flat and type-independent for
    // every OTHER claim type — the flat `DEFAULT_ALLOWANCE_CONFIG.
    // maxAgentsMemoryAxis` (3) stands in for those.
    //
    // Phase 3 (post-review fix): `LIVE_AGENT_CLAIM_TYPE` is the one
    // type this generic placeholder must NOT apply to. `--claim=live-agent:N`
    // is the documented, public way a caller registers a genuinely-spawned
    // live agent against the SAME ledger `main()`'s own live-agent-ceiling
    // pre-check reads (see the `LIVE_AGENT_CLAIM_TYPE` doc comment above) —
    // that reader treats the SAME ceiling this probe reads below as the
    // ceiling (originally the flat `LIVE_AGENT_CEILING` (4); Phase 3
    // UPDATE: the AIMD-adjusted value — see the `readAimdCeilingState` call
    // a few lines below). If this writer capped `availableCapacity` at the
    // unrelated flat-3 placeholder instead, the ledger could never
    // physically hold more than 3 real claims (a 4th `--claim=live-agent:1`
    // would always be denied even though the reader believes more slots
    // exist), and `main()`'s own `liveAgentCeilingRemaining` computation
    // (`<ceiling> - alreadyGrantedTotal`) would then always have at least 1
    // slot of headroom to admit into — silently disabling the one hard
    // ceiling this ticket exists to install, in the conservative-UNSAFE
    // direction. Both
    // sides of this ledger must agree on the SAME ceiling constant.
    //
    // this is still the FLAT, pre-subtraction ceiling only — this
    // probe runs BEFORE the lock (see this function's header comment), so
    // it cannot yet account for outstanding `LIVE_AGENT_ADMISSION_CLAIM_TYPE`
    // holds without reintroducing the same unlocked-read staleness this
    // ticket closes. The cross-ledger subtraction happens later, inside
    // `computeAvailableCapacity` below, which `claimCapacity` invokes from
    // inside its own lock.
    //
    // Phase 3 — the FLAT constant above is now the AIMD-ADJUSTED
    // ceiling, read via `readAimdCeilingState` (Phase 1) rather than the
    // hardcoded `LIVE_AGENT_CEILING`. Only read (never computed/persisted)
    // here — this is a one-shot, pre-lock probe, not the recurring beat that
    // owns advancing the AIMD state machine (see `main()`'s `--desired-agents`
    // beat below, the one call site that both computes AND writes the next
    // state via `computeAimdCeiling`/`writeAimdCeilingState`).
    //
    // Staleness seam: `now: sampledAt` reuses the EXACT stamp this function's
    // own header comment already reserves for `DEFAULT_CLAIM_SAMPLE_
    // FRESHNESS_MS`'s bounded-staleness window — the same `resolveNow()`/
    // `ARM_FAKE_NOW_MS` seam, not a second, independently-invented freshness
    // mechanism for the ceiling specifically (see
    // `./cli-aimd-ceiling.jest.spec.mjs`'s "design decision A" tests). The
    // real safety backstop against a stale ceiling read here remains
    // `reserveAdmission`'s own lock-time re-check inside `main()`'s beat —
    // unchanged by this phase (see the mixed-version-fleet accepted-risk
    // comment on `LIVE_AGENT_CEILING` above).
    const liveAgentCeiling =
      type === LIVE_AGENT_CLAIM_TYPE
        ? (await readAimdCeilingState(coordinationFilePath, { now: sampledAt })).ceiling
        : undefined;

    return {
      axisGreen: true,
      sampledAt,
      availableCapacity:
        type === LIVE_AGENT_CLAIM_TYPE ? liveAgentCeiling : DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis,
    };
  }

  // `sampledAt` comes back from the probe itself (see the stamping comment
  // inside `probeAvailableCapacity`) rather than being captured here, and it
  // is read from the same `resolveNow()`/`ARM_FAKE_NOW_MS` seam as the
  // comparison it will be measured against — never a second, independent
  // `Date.now()`. A fake `sampledAt` sitting next to a real-wall-clock
  // comparison would read as permanently stale under the test seam. See
  // `writeSharedSample`'s identical insistence in coordination-file.mjs.
  let sampledAt = now;
  let capacityAtSample = 0;
  let axisGreen = false;

  // Preserves `--claim`'s fail-CLOSED posture across the hoist: before this
  // change, anything thrown in here surfaced as a rejected `claimCapacity`
  // and was caught below into `granted = 0`. Outside the lock it needs its
  // own catch, or an unexpected failure would escape to `main().catch()` and
  // exit 1 instead of reporting an honest zero grant.
  try {
    const probeResult = await probeAvailableCapacity();
    axisGreen = probeResult.axisGreen;
    capacityAtSample = probeResult.availableCapacity;
    sampledAt = probeResult.sampledAt;
  } catch (error) {
    warnCoordinationDegraded(error, 'host-resource probe', 'granting nothing for this claim');
    axisGreen = false;
  }

  if (!axisGreen) {
    // Never creates `<path>.lock`: an AMBER/RED beat cannot grant anything,
    // so making every other orchestrator queue behind its lock acquisition
    // would be pure contention for a foregone conclusion.
    process.stdout.write(JSON.stringify({ granted: 0 }) + '\n');
    return;
  }

  // The callback `claimCapacity` awaits inside its critical section does one
  // I/O read for `LIVE_AGENT_CLAIM_TYPE` (see below) and is otherwise pure:
  // no subprocess, no clock beyond the shared `resolveNow()` seam. The
  // freshness decision itself lives in `lib/probe-bound.mjs` rather than
  // inline here, because this file is excluded from coverage collection and
  // the hoist's entire safety argument rests on that one branch — see
  // `capacityIfSampleFresh`.
  const computeAvailableCapacity = async () => {
    const freshCapacity = capacityIfSampleFresh({
      availableCapacity: capacityAtSample,
      sampledAt,
      referenceNow: resolveNow(),
      freshnessWindowMs: DEFAULT_CLAIM_SAMPLE_FRESHNESS_MS,
      // The staleness arm used to deny in complete silence — see
      // `describeStaleSampleDenial`. The synthetic code token is what makes
      // the line countable alongside the two real lock codes; this branch
      // throws nothing, so there is no `error.code` to interpolate.
      onStale: (context) => {
        const { degradedWhat, consequence } = describeStaleSampleDenial(context);
        warnCoordinationDegraded({ code: 'CLAIM_SAMPLE_STALE' }, degradedWhat, consequence);
      },
    });

    // (cross-ledger headroom fix): `LIVE_AGENT_CLAIM_TYPE` is the only
    // type this pre-hoisted, flat-ceiling `freshCapacity` is not the whole
    // story for. `main()`'s own `--desired-agents` beat can be holding
    // outstanding `LIVE_AGENT_ADMISSION_CLAIM_TYPE` grants — advisory
    // admissions nobody has converted into a genuine `--claim=live-agent:N`
    // yet — and until this fix, this writer never looked at that ledger at
    // all, so a real claim could land "on top of" an outstanding admission
    // and together exceed `LIVE_AGENT_CEILING`. `reserveAdmission` already
    // subtracts `LIVE_AGENT_CLAIM_TYPE`'s live total from its own admission
    // headroom (the other direction of this same relationship); this closes
    // the reverse direction so neither ledger is blind to the other.
    //
    // This read happens HERE, inside the closure `claimCapacity` invokes
    // from inside its own `withLock` critical section (see that function's
    // `await computeAvailableCapacity()` call site) — not as a separate,
    // unlocked pre-read before the lock is acquired. That is what makes the
    // subtraction race-safe: the admission total read here reflects the
    // instant this specific claim's grant is decided, under the SAME lock
    // cycle, not a snapshot that could go stale between an earlier read and
    // this grant. No second lock cycle is introduced.
    //
    // TTL-pruned against `DEFAULT_FRESHNESS_WINDOW_MS` — the SAME window
    // `reserveAdmission` itself prunes `LIVE_AGENT_ADMISSION_CLAIM_TYPE`
    // against (see that call site's `claimTtlMs` option below `main()`) —
    // never `DEFAULT_CLAIM_TTL_MS` (that TTL belongs to `LIVE_AGENT_CLAIM_TYPE`
    // itself). An admission hold whose TTL has already lapsed must not count
    // against this headroom; using any other TTL here would silently
    // disagree with `reserveAdmission` on which admission holds are "live".
    if (type !== LIVE_AGENT_CLAIM_TYPE) return freshCapacity;

    // Excludes THIS orchestrator's own outstanding admission hold
    // (review follow-up): this is the natural extension of reusing one
    // stable `orchestratorId` across beats, exactly as SKILL.md's own
    // `--desired-agents`/`--heartbeat` worked examples already do — an
    // orchestrator converting its own admission hold into a genuine
    // `--claim` must not have that same hold subtracted from its own
    // headroom — that reintroduces, in the admission-to-claim direction, the
    // exact self-starving bug `reserveAdmission`'s own "own-hold replace, not
    // accumulate" step (this file's `claimsAfterOwnRelease`) exists to avoid
    // in the other direction. Only OTHER orchestrators' outstanding admission
    // holds should count against this claim's headroom.
    const outstandingAdmissionTotal = await getLiveClaimTotal(
      coordinationFilePath,
      LIVE_AGENT_ADMISSION_CLAIM_TYPE,
      now,
      DEFAULT_FRESHNESS_WINDOW_MS,
      orchestratorId,
    );
    return Math.max(0, freshCapacity - outstandingAdmissionTotal);
  };

  // Fails CLOSED (granted: 0). Since that is the rule for every path
  // in this file that GRANTS capacity — `--claim`, `--release`, and the
  // global spawn-rate bucket — while the paths that merely record or share
  // information (the shared-sample write, both `readDibs` calls, the history
  // write) still fail open. `--claim` is the authoritative WRITE half of
  // this skill's atomic-slot-reservation contract — a lock/coordination
  // failure here means this invocation cannot PROVE it safely reserved
  // anything, and silently granting the full ask in that state would defeat
  // the entire race-safety property this flag exists for.
  let granted = 0;
  try {
    const result = await claimCapacity(
      coordinationFilePath,
      {
        type,
        requestedCount,
        orchestratorId,
        computeAvailableCapacity,
        now,
        // — ledger write call site 2 of 5. The beat's single resolved
        // identity, spread in unchanged. Resolved above, OUTSIDE
        // `claimCapacity`'s own `withLock` critical section, so the `ps`
        // shell-out behind it never extends this claim's lock hold. See the
        // ledger-identity note near `resolveNow`.
        ...beatLedgerIdentity,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS, lockMaxAttempts: DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS },
    );
    granted = result.granted;
  } catch (error) {
    // Classified rather than pooled (review round 2): a lock TIMEOUT
    // and a lock LOST indict different constants and send a reader to
    // different places, and a ceiling sized too tight for real concurrency
    // has to announce itself rather than read as "claims stopped working".
    // Anything that is neither returns the original generic pair unchanged.
    const { degradedWhat, consequence } = describeClaimDenial(error, {
      lockMaxAttempts: DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS,
    });
    warnCoordinationDegraded(error, degradedWhat, consequence);
    granted = 0;
  }

  // review follow-up: a genuine `live-agent` grant converts this
  // orchestrator's own outstanding `live-agent-admission` hold into a real
  // claim — that hold must be cleared NOW, not left to age out over
  // `DEFAULT_FRESHNESS_WINDOW_MS` (up to ~90s). Left uncleared, the same
  // agents are double-counted: once here (the real `live-agent` ledger,
  // just written above) and once still sitting in the admission ledger,
  // where `computeAvailableCapacity`'s `othersHeld` subtraction (see this
  // file's `LIVE_AGENT_ADMISSION_CLAIM_TYPE` block above) charges it against
  // every OTHER orchestrator's headroom until it either expires or this
  // orchestrator's next `--desired-agents` beat replaces it. `releaseCapacity`
  // already clamps `releaseCount` to this orchestrator's own held amount
  // (`Math.min(releaseCount, ownHeld)`), so passing `granted` here releases
  // at most what was actually converted — never another orchestrator's hold,
  // and never more than this orchestrator's own admission hold even if it
  // happens to be smaller than `granted`. A failure to acquire the lock here
  // is not fatal to the already-completed `--claim` above — the stale hold
  // still self-heals via TTL or the next beat, so this only warns.
  //
  // review follow-up (round 2): this cycle sits on the caller's
  // critical path — the process does not print `{"granted":...}` until it
  // settles — so, like the claim above it, it must use the fast-fail
  // `DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS` ceiling rather than inheriting
  // `acquireLock`'s much larger DEFAULT (`LOCK_MAX_ATTEMPTS`, ~10-20s of
  // retry sleep — see that constant's own comment in coordination-file.mjs).
  // Releasing this hold is a one-time, inconsequential-on-failure cleanup of
  // this orchestrator's OWN prior state, not a contended cross-orchestrator
  // reservation; there is no reason for it to make a one-shot beat wait as
  // long as a real claim would.
  if (type === LIVE_AGENT_CLAIM_TYPE && granted > 0) {
    try {
      await releaseCapacity(
        coordinationFilePath,
        { type: LIVE_AGENT_ADMISSION_CLAIM_TYPE, releaseCount: granted, orchestratorId, now },
        { claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS, lockMaxAttempts: DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS },
      );
    } catch (error) {
      warnCoordinationDegraded(
        error,
        'coordination file',
        "leaving this orchestrator's outstanding live-agent-admission hold in place; it will self-heal " +
          'via TTL or the next --desired-agents beat',
      );
    }
  }

  process.stdout.write(JSON.stringify({ granted }) + '\n');
}

// ---------------------------------------------------------------------------
// handleRelease (Phase 2, ; orchestrator-scoped since Phase 1) —
// `--release=<type>:<count>` decrements the same per-type claim ledger
// `--claim` writes to, via `releaseCapacity` (./lib/coordination-file.mjs).
// Mostly plumbing: `releaseCapacity` already returns exactly `{ released,
// grantedTotal }`, where `released = min(requestedReleaseCount, ownHeld)` —
// `ownHeld` being only the CALLING `orchestratorId`'s own claim(s), not the
// shared cross-holder pool (see releaseCapacity's own JSDoc) — so this
// handler just validates input, threads `orchestratorId` through, calls it,
// and prints `{"released":<n>}`.
// ---------------------------------------------------------------------------

async function handleRelease(flags) {
  const releaseSpec = parseReleaseSpec(flags.release);
  if (!releaseSpec) {
    const received = typeof flags.release === 'string' ? flags.release : String(flags.release);
    process.stderr.write(
      `agent-resource-management: --release=<type>:<count> requires a non-empty operation type and a ` +
        `non-negative integer count, got "${received}"\n`,
    );
    process.exit(2);
    return;
  }

  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write('agent-resource-management: --orchestrator-id=<id> is required for --release\n');
    process.exit(2);
    return;
  }
  if (isReservedEntry(orchestratorId)) {
    process.stderr.write(
      `agent-resource-management: --orchestrator-id must not match the reserved double-underscore convention ` +
        `("__...__"), got "${orchestratorId}" — reserved ids collide with internal coordination-file bookkeeping ` +
        '(e.g. the legacy-unattributed-claim sentinel) and are not valid orchestrator ids\n',
    );
    process.exit(2);
    return;
  }

  const { type, count: releaseCount } = releaseSpec;

  // review follow-up: mirrors the same guard `handleClaim` applies —
  // see that call site's comment. `LIVE_AGENT_ADMISSION_CLAIM_TYPE` is
  // written and pruned exclusively by `reserveAdmission`; a public
  // `--release` against it would let an orchestrator (or a typo) clear
  // another orchestrator's outstanding admission hold out from under it.
  if (type === LIVE_AGENT_ADMISSION_CLAIM_TYPE) {
    process.stderr.write(
      `agent-resource-management: --release type "${LIVE_AGENT_ADMISSION_CLAIM_TYPE}" is internal-only ` +
        '(written exclusively by the --desired-agents beat via reserveAdmission) and cannot be released ' +
        'directly\n',
    );
    process.exit(2);
    return;
  }

  // Same posture, same rationale — see `IDLE_PROBE_CLAIM_TYPE`'s own doc
  // comment: internal-only, written exclusively by the --desired-agents
  // beat's --user-attests-idle idle-probe admission path.
  if (type === IDLE_PROBE_CLAIM_TYPE) {
    process.stderr.write(
      `agent-resource-management: --release type "${IDLE_PROBE_CLAIM_TYPE}" is internal-only ` +
        '(written exclusively by the --desired-agents beat via --user-attests-idle) and cannot be released ' +
        'directly\n',
    );
    process.exit(2);
    return;
  }

  const coordinationFilePath = resolveCoordinationFilePath();
  const now = resolveNow();

  await ensureCoordinationDir(coordinationFilePath);

  // Fails CLOSED (released: 0), mirroring `--claim`'s own fail-closed posture
  // above: a lock/coordination failure here means this invocation cannot
  // PROVE it safely applied the release, so silently reporting the full ask
  // as released would misrepresent the ledger's true state to the caller.
  // `orchestratorId` (validated above, required, non-empty) IS passed through
  // `releaseCapacity`'s ledger entry now carries per-holder
  // `claims[]` records, so this call releases ONLY the calling orchestrator's
  // own claim(s) for `type` — never another orchestrator's. See
  // releaseCapacity's JSDoc (lib/coordination-file.mjs) and SKILL.md
  // "Releasing capacity" /. `now` threads `resolveNow()`
  // through so TTL expiry judges claim age on the same `ARM_FAKE_NOW_MS`
  // clock `--claim` already uses, rather than diverging onto real wall time.
  let released = 0;
  try {
    const result = await releaseCapacity(
      coordinationFilePath,
      { type, releaseCount, orchestratorId, now },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    );
    released = result.released;
  } catch (error) {
    warnCoordinationDegraded(error, 'coordination file', 'releasing nothing for this beat');
    released = 0;
  }

  process.stdout.write(JSON.stringify({ released }) + '\n');
}

// ---------------------------------------------------------------------------
// ensureQueueDir — mirrors `ensureCoordinationDir`'s mkdir-recursive-and-
// swallow shape, for the durable work queue's own file. `enqueueItem`
// (./lib/queue.mjs) already creates the queue's parent directory itself
// before writing, so `handleEnqueue` never needs this — it exists purely for
// `handleDequeue`'s benefit: `dequeueItem` acquires the queue lock
// unconditionally (even on an empty/nonexistent queue), and on a completely
// fresh host `~/.claude/agent-state/` may not exist yet the first time
// `--dequeue` runs, before any `--enqueue` has ever created it.
// `handlePeek`/`peekQueue` need no such call — peeking never locks or writes
// (see ./lib/queue.mjs's own doc comment on `peekQueue`), so there is no
// directory-must-exist precondition for it to fail on.
// ---------------------------------------------------------------------------

async function ensureQueueDir(queueFilePath) {
  try {
    await fs.mkdir(dirname(queueFilePath), { recursive: true });
  } catch {
    // Deliberately swallowed, mirroring ensureCoordinationDir: an unwritable
    // parent dir surfaces naturally when the subsequent lock acquisition
    // fails on its own.
  }
}

// ---------------------------------------------------------------------------
// handleEnqueue — `--enqueue=<agentClass>:<priority>:
// <commandRef>` appends one durable work item to the queue via
// `./lib/queue.mjs`'s `enqueueItem`, under that module's own lock/atomic-
// write machinery (a second, queue-scoped lock file — never the admission
// ledger's coordination file; see that module's header). All validation
// (structural spec shape here, field-level shape inside `enqueueItem`) runs
// BEFORE any write, so a rejected `--enqueue` never touches disk.
// ---------------------------------------------------------------------------

async function handleEnqueue(flags) {
  const enqueueSpec = parseEnqueueSpec(flags.enqueue);
  if (!enqueueSpec) {
    const received = typeof flags.enqueue === 'string' ? flags.enqueue : String(flags.enqueue);
    process.stderr.write(
      `agent-resource-management: --enqueue=<agentClass>:<priority>:<commandRef> requires a non-empty ` +
        `agentClass and at least two colon-delimited segments, got "${received}"\n`,
    );
    process.exit(2);
    return;
  }

  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write('agent-resource-management: --orchestrator-id=<id> is required for --enqueue\n');
    process.exit(2);
    return;
  }
  if (isReservedEntry(orchestratorId)) {
    process.stderr.write(
      `agent-resource-management: --orchestrator-id must not match the reserved double-underscore convention ` +
        `("__...__"), got "${orchestratorId}" — reserved ids collide with internal coordination-file bookkeeping ` +
        '(e.g. the legacy-unattributed-claim sentinel) and are not valid orchestrator ids\n',
    );
    process.exit(2);
    return;
  }

  const queueFilePath = resolveQueueFilePath();

  let entry;
  try {
    entry = await enqueueItem(queueFilePath, { ...enqueueSpec, orchestratorId });
  } catch (error) {
    // Covers `QueueInvalidPriorityError`/`QueueInvalidCommandRefError`/
    // `QueueInvalidAgentClassError`/`QueueInvalidOrchestratorIdError`
    // (field-level validation `enqueueItem` runs before any write; the
    // orchestratorId itself is already validated present/non-reserved above,
    // so this branch is unreachable for it in practice, but `enqueueItem`
    // still enforces it independently — see that module's own validation)
    // and `QueueCorruptFileError`/lock failures alike
    // — all fail closed (exit 2, no partial write), naming `--enqueue` so the
    // failure is attributable to this beat regardless of which underlying
    // error produced it.
    process.stderr.write(`agent-resource-management: --enqueue failed — ${error.message}\n`);
    process.exit(2);
    return;
  }

  process.stdout.write(JSON.stringify({ enqueued: true, item: entry }) + '\n');
}

// ---------------------------------------------------------------------------
// handlePeek — `--peek` prints the full live queue, ordered
// exactly as `./lib/queue.mjs`'s `peekQueue` returns it (priority descending,
// then enqueue-time ascending), and mutates nothing — `peekQueue` never
// acquires the queue lock or writes (see that function's own doc comment),
// so this handler costs zero additional locked writes.
// ---------------------------------------------------------------------------

async function handlePeek(flags) {
  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write('agent-resource-management: --orchestrator-id=<id> is required for --peek\n');
    process.exit(2);
    return;
  }
  if (isReservedEntry(orchestratorId)) {
    process.stderr.write(
      `agent-resource-management: --orchestrator-id must not match the reserved double-underscore convention ` +
        `("__...__"), got "${orchestratorId}" — reserved ids collide with internal coordination-file bookkeeping ` +
        '(e.g. the legacy-unattributed-claim sentinel) and are not valid orchestrator ids\n',
    );
    process.exit(2);
    return;
  }

  const queueFilePath = resolveQueueFilePath();

  let items;
  try {
    items = await peekQueue(queueFilePath, resolveNow());
  } catch (error) {
    // `QueueCorruptFileError` is the only realistic throw here (a missing
    // file already degrades to `[]` inside `peekQueue`) — surfaced rather
    // than swallowed, since silently reporting an empty queue over a
    // genuinely corrupt file would misrepresent queue state to the caller.
    process.stderr.write(`agent-resource-management: --peek failed — ${error.message}\n`);
    process.exit(2);
    return;
  }

  process.stdout.write(JSON.stringify({ items }) + '\n');
}

// ---------------------------------------------------------------------------
// handleDequeue — `--dequeue` atomically claims and prints
// the top entry via `./lib/queue.mjs`'s `dequeueItem` (one locked critical
// section; two racing `--dequeue` calls never both receive the same entry —
// see that function's own doc comment), or the explicit `{ item: null }`
// empty-queue marker, exit 0 in both cases — an empty queue is not an error
// condition for a caller that is simply asking "is there anything to do".
// Note: since `dequeueItem` became lease-aware, `{ item: null }` no longer
// means only "queue is empty" — it also covers "every remaining entry is
// currently leased by someone else" (nothing eligible to claim right now).
// ---------------------------------------------------------------------------

async function handleDequeue(flags) {
  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write('agent-resource-management: --orchestrator-id=<id> is required for --dequeue\n');
    process.exit(2);
    return;
  }
  if (isReservedEntry(orchestratorId)) {
    process.stderr.write(
      `agent-resource-management: --orchestrator-id must not match the reserved double-underscore convention ` +
        `("__...__"), got "${orchestratorId}" — reserved ids collide with internal coordination-file bookkeeping ` +
        '(e.g. the legacy-unattributed-claim sentinel) and are not valid orchestrator ids\n',
    );
    process.exit(2);
    return;
  }

  const queueFilePath = resolveQueueFilePath();
  await ensureQueueDir(queueFilePath);

  let item;
  try {
    item = await dequeueItem(queueFilePath, { now: resolveNow() });
  } catch (error) {
    process.stderr.write(`agent-resource-management: --dequeue failed — ${error.message}\n`);
    process.exit(2);
    return;
  }

  process.stdout.write(JSON.stringify({ item }) + '\n');
}

// ---------------------------------------------------------------------------
// handleDequeueIfCapacity — `--dequeue-if-capacity`
// composes `claimAndAdmitQueueEntry` (./lib/dispatch.mjs, Phase 2) against
// the SAME `LIVE_AGENT_CLAIM_TYPE`/`LIVE_AGENT_ADMISSION_CLAIM_TYPE`/
// `LIVE_AGENT_CEILING` shared ledger the `--desired-agents` beat's own
// `reserveAdmission` call reserves against (below, in `main()`'s
// `--desired-agents` beat), so a dispatched queue item genuinely competes
// for the same live-agent slots.
// A standalone, decision-only beat type — mirrors `--dequeue`'s own
// "atomic queue-file write, print JSON, exit 0, never spawn" shape; cli.mjs
// itself never spawns a process (see this file's own header comment).
// Remaps `claimAndAdmitQueueEntry`'s internal `'capacity-denied'` reason to
// the caller-facing `'no-capacity'` literal and reports `item: null` on
// denial — the already-released item is no longer actionable by the caller.
//
// MANDATORY caller-side ordering per granted item (documentation-only —
// NOT enforced by this handler or by `claimAndAdmitQueueEntry`; see pre-PR
// review finding #2,): `--dequeue-if-capacity` (this beat; get
// `item`+`leaseId`) → spawn externally → `--claim=live-agent:1` (registers
// the real running agent AND atomically clears this orchestrator's
// admission hold) → … → `--release=live-agent:1` (when the agent finishes)
// → `--ack-lease=<leaseId>` (permanently removes the queue entry). Skipping
// the `--claim=live-agent:N` step before a SUBSEQUENT `--dequeue-if-capacity`
// call is silently wrong, not merely suboptimal: `reserveAdmission`'s
// admission hold uses REPLACE-not-accumulate semantics per `orchestratorId`
// (a single orchestrator's own prior hold is excluded from its own next
// request), so repeated `--dequeue-if-capacity` calls without an
// intervening real `--claim` under-report that orchestrator's true
// outstanding admission against the shared ceiling every time. See
// `claimAndAdmitQueueEntry`'s own JSDoc (`./lib/dispatch.mjs`) for the full
// mechanism.
// ---------------------------------------------------------------------------

async function handleDequeueIfCapacity(flags) {
  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write(
      'agent-resource-management: --orchestrator-id=<id> is required for --dequeue-if-capacity\n',
    );
    process.exit(2);
    return;
  }
  if (isReservedEntry(orchestratorId)) {
    process.stderr.write(
      `agent-resource-management: --orchestrator-id must not match the reserved double-underscore convention ` +
        `("__...__"), got "${orchestratorId}" — reserved ids collide with internal coordination-file bookkeeping ` +
        '(e.g. the legacy-unattributed-claim sentinel) and are not valid orchestrator ids\n',
    );
    process.exit(2);
    return;
  }

  const queueFilePath = resolveQueueFilePath();
  const coordinationFilePath = resolveCoordinationFilePath();
  await ensureQueueDir(queueFilePath);
  await ensureCoordinationDir(coordinationFilePath);

  // Phase 3 — read (never compute/persist) the AIMD-adjusted ceiling,
  // same as `probeAvailableCapacity`'s `--claim` pre-lock probe above: this
  // is a one-shot, non-`main()`-beat invocation, not the recurring beat that
  // owns advancing the AIMD state machine (see `main()`'s `--desired-agents`
  // beat below). `now` is captured once and reused for both the ceiling
  // read and `claimAndAdmitQueueEntry`'s own `now`, so the two never
  // silently disagree about "when" this call happened.
  //
  // Deliberately INSIDE this handler's own try/catch (not read before it):
  // a lock-acquisition failure here is exactly the kind of coordination-file
  // contention this tool exists to arbitrate, so it must fail closed through
  // this handler's own tailored message + exit(2) contract, not escape
  // uncaught to the generic top-level `main().catch()`.
  const now = resolveNow();

  // — resolved before the try block's first lock-taking call, from this
  // beat's one process snapshot, exactly as the other four write call sites
  // resolve theirs. Routing to the ledger through `lib/dispatch.mjs` changes
  // nothing about where the identity comes from.
  const beatLedgerIdentity = await resolveBeatLedgerIdentity();

  let result;
  try {
    const aimdCeilingState = await readAimdCeilingState(coordinationFilePath, { now });
    result = await claimAndAdmitQueueEntry(
      coordinationFilePath,
      queueFilePath,
      {
        orchestratorId,
        snapshotType: LIVE_AGENT_CLAIM_TYPE,
        admissionType: LIVE_AGENT_ADMISSION_CLAIM_TYPE,
        // Mixed-version-fleet accepted risk — see `LIVE_AGENT_CEILING`'s own
        // doc comment above: `claimAndAdmitQueueEntry`/`reserveAdmission`
        // trust this value verbatim, never cross-checking it against
        // whatever a concurrently-running orchestrator may have since
        // persisted.
        ceiling: aimdCeilingState.ceiling,
        now,
        // — ledger write call site 5 of 5, and the only one that reaches
        // the ledger through ./lib/dispatch.mjs rather than
        // ./lib/coordination-file.mjs directly, so it is easy to miss when
        // threading. It carries the same beat-resolved identity as the other
        // four. See the ledger-identity note near `resolveNow`.
        ...beatLedgerIdentity,
      },
      { admission: { claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS, snapshotClaimTtlMs: DEFAULT_CLAIM_TTL_MS } },
    );
  } catch (error) {
    process.stderr.write(`agent-resource-management: --dequeue-if-capacity failed — ${error.message}\n`);
    process.exit(2);
    return;
  }

  if (!result.granted) {
    const reason = result.reason === 'capacity-denied' ? 'no-capacity' : result.reason;
    process.stdout.write(JSON.stringify({ item: null, granted: false, reason }) + '\n');
    return;
  }

  process.stdout.write(JSON.stringify({ item: result.item, granted: true, leaseId: result.leaseId }) + '\n');
}

// ---------------------------------------------------------------------------
// handleAckLease — `--ack-lease` is a thin standalone
// wrapper around `./lib/queue.mjs`'s `ackQueueEntry`, permanently removing
// the leased item once the caller confirms success. A stale/unknown leaseId
// is a safe no-op (exit 0, not an error), mirroring `ackQueueEntry`'s own
// documented contract and `--dequeue`'s "empty queue is not an error"
// precedent.
// ---------------------------------------------------------------------------

async function handleAckLease(flags) {
  // `--orchestrator-id` is required and validated below even though
  // `ackQueueEntry` never actually uses it in the lookup (pre-PR review
  // finding #5,) — the leaseId alone is today's sole capability token.
  // Required anyway for consistency with every other beat-type flag's own
  // logging/attribution convention, and to leave room for a future
  // per-orchestrator lease-ownership check; `ackQueueEntry`/
  // `releaseQueueEntry` do NOT currently enforce ownership by
  // `orchestratorId`, so this is honest documentation of the current state,
  // not a claim that ownership is enforced.
  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write('agent-resource-management: --orchestrator-id=<id> is required for --ack-lease\n');
    process.exit(2);
    return;
  }
  if (isReservedEntry(orchestratorId)) {
    process.stderr.write(
      `agent-resource-management: --orchestrator-id must not match the reserved double-underscore convention ` +
        `("__...__"), got "${orchestratorId}" — reserved ids collide with internal coordination-file bookkeeping ` +
        '(e.g. the legacy-unattributed-claim sentinel) and are not valid orchestrator ids\n',
    );
    process.exit(2);
    return;
  }

  // pre-PR review finding #3: `--ack-lease` (no value; parses to
  // boolean `true`) or `--ack-lease=` (empty string) must be rejected
  // BEFORE calling `ackQueueEntry` — both would otherwise silently produce
  // `{"acked":false}` at exit 0, indistinguishable from a legitimate
  // stale-leaseId no-op. Mirrors `--claim`'s own count-parsing rejection
  // style above.
  const leaseId = flags['ack-lease'];
  if (typeof leaseId !== 'string' || leaseId.length === 0) {
    const received = typeof leaseId === 'string' ? leaseId : String(leaseId);
    process.stderr.write(
      `agent-resource-management: --ack-lease=<leaseId> requires a non-empty leaseId, got "${received}"\n`,
    );
    process.exit(2);
    return;
  }

  const queueFilePath = resolveQueueFilePath();
  await ensureQueueDir(queueFilePath);

  let item;
  try {
    item = await ackQueueEntry(queueFilePath, leaseId);
  } catch (error) {
    process.stderr.write(`agent-resource-management: --ack-lease failed — ${error.message}\n`);
    process.exit(2);
    return;
  }

  if (item === null) {
    process.stdout.write(JSON.stringify({ acked: false }) + '\n');
    return;
  }

  process.stdout.write(JSON.stringify({ acked: true, item }) + '\n');
}

// ---------------------------------------------------------------------------
// handleReleaseLease — `--release-lease` is a thin
// standalone wrapper around `./lib/queue.mjs`'s `releaseQueueEntry`,
// returning the leased item to the claimable queue (lease fields cleared,
// `enqueuedAt` preserved) once the caller reports failure. A stale/unknown
// leaseId is a safe no-op (exit 0, not an error), same rationale as
// `--ack-lease` above.
// ---------------------------------------------------------------------------

async function handleReleaseLease(flags) {
  // `--orchestrator-id` is required and validated below even though
  // `releaseQueueEntry` never actually uses it in the lookup (pre-PR review
  // finding #5,) — the leaseId alone is today's sole capability token.
  // Required anyway for consistency with every other beat-type flag's own
  // logging/attribution convention, and to leave room for a future
  // per-orchestrator lease-ownership check; `ackQueueEntry`/
  // `releaseQueueEntry` do NOT currently enforce ownership by
  // `orchestratorId`, so this is honest documentation of the current state,
  // not a claim that ownership is enforced.
  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write('agent-resource-management: --orchestrator-id=<id> is required for --release-lease\n');
    process.exit(2);
    return;
  }
  if (isReservedEntry(orchestratorId)) {
    process.stderr.write(
      `agent-resource-management: --orchestrator-id must not match the reserved double-underscore convention ` +
        `("__...__"), got "${orchestratorId}" — reserved ids collide with internal coordination-file bookkeeping ` +
        '(e.g. the legacy-unattributed-claim sentinel) and are not valid orchestrator ids\n',
    );
    process.exit(2);
    return;
  }

  // pre-PR review finding #3: `--release-lease` (no value; parses to
  // boolean `true`) or `--release-lease=` (empty string) must be rejected
  // BEFORE calling `releaseQueueEntry` — both would otherwise silently
  // produce `{"released":false}` at exit 0, indistinguishable from a
  // legitimate stale-leaseId no-op. Mirrors `--ack-lease`'s own validation
  // above.
  const leaseId = flags['release-lease'];
  if (typeof leaseId !== 'string' || leaseId.length === 0) {
    const received = typeof leaseId === 'string' ? leaseId : String(leaseId);
    process.stderr.write(
      `agent-resource-management: --release-lease=<leaseId> requires a non-empty leaseId, got "${received}"\n`,
    );
    process.exit(2);
    return;
  }

  const queueFilePath = resolveQueueFilePath();
  await ensureQueueDir(queueFilePath);

  let item;
  try {
    item = await releaseQueueEntry(queueFilePath, leaseId);
  } catch (error) {
    process.stderr.write(`agent-resource-management: --release-lease failed — ${error.message}\n`);
    process.exit(2);
    return;
  }

  if (item === null) {
    process.stdout.write(JSON.stringify({ released: false }) + '\n');
    return;
  }

  process.stdout.write(JSON.stringify({ released: true, item }) + '\n');
}

// ---------------------------------------------------------------------------
// handleBindPid — `--bind-pid=<pid> --lease-id=<leaseId>
// --orchestrator-id=<id>` is a thin standalone wrapper around
// `./lib/queue.mjs`'s `bindPidToLease`, closing the "does this actually get
// called in production" gap the Convergence Analysis raised on its Build
// Plan: Phase 1 shipped `bindPidToLease` and Phase 2 shipped the pid-lease
// verification inside `performEviction`, but nothing in `cli.mjs` ever called
// `bindPidToLease` until this flag — an orchestrator now stamps its
// just-externally-spawned agent's real OS pid onto the leased queue entry
// immediately after spawning, so a LATER `--evict-pid` for that same lease
// can verify the pid it's about to signal actually matches the pid that was
// bound to it. Mirrors `--ack-lease`/`--release-lease`'s own flag-parsing/
// validation/JSON-output convention (including requiring `--orchestrator-id`,
// the caller's own id, passed straight through to `bindPidToLease` as the
// ownership-scoping argument) and `--evict-pid`'s own `PID_PATTERN`
// positive-integer pid validation. A cross-orchestrator or stale/unmatched
// `leaseId` is a safe no-op (exit 0, `{ bound: false }`), mirroring
// `ackQueueEntry`/`releaseQueueEntry`'s existing "no match, return null
// rather than throw" convention that `bindPidToLease` itself reuses.
// ---------------------------------------------------------------------------

async function handleBindPid(flags) {
  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write('agent-resource-management: --orchestrator-id=<id> is required for --bind-pid\n');
    process.exit(2);
    return;
  }
  if (isReservedEntry(orchestratorId)) {
    process.stderr.write(
      `agent-resource-management: --orchestrator-id must not match the reserved double-underscore convention ` +
        `("__...__"), got "${orchestratorId}" — reserved ids collide with internal coordination-file bookkeeping ` +
        '(e.g. the legacy-unattributed-claim sentinel) and are not valid orchestrator ids\n',
    );
    process.exit(2);
    return;
  }

  // Mirrors `--evict-pid`'s own `PID_PATTERN` positive-integer pid
  // validation (see `handleEvict` below) — this pid is ultimately what a
  // later `--evict-pid` call compares its own value against.
  const bindPidRaw = flags['bind-pid'];
  const PID_PATTERN = /^\d+$/;
  if (
    typeof bindPidRaw !== 'string' ||
    bindPidRaw.length === 0 ||
    !PID_PATTERN.test(bindPidRaw) ||
    Number(bindPidRaw) <= 0
  ) {
    const received = typeof bindPidRaw === 'string' ? bindPidRaw : String(bindPidRaw);
    process.stderr.write(
      `agent-resource-management: --bind-pid=<pid> requires a positive integer pid, got "${received}"\n`,
    );
    process.exit(2);
    return;
  }

  const leaseId = flags['lease-id'];
  if (typeof leaseId !== 'string' || leaseId.length === 0) {
    const received = typeof leaseId === 'string' ? leaseId : String(leaseId);
    process.stderr.write(
      `agent-resource-management: --lease-id=<leaseId> is required for --bind-pid, got "${received}"\n`,
    );
    process.exit(2);
    return;
  }

  const queueFilePath = resolveQueueFilePath();
  await ensureQueueDir(queueFilePath);

  let item;
  try {
    item = await bindPidToLease(queueFilePath, leaseId, Number(bindPidRaw), orchestratorId);
  } catch (error) {
    process.stderr.write(`agent-resource-management: --bind-pid failed — ${error.message}\n`);
    process.exit(2);
    return;
  }

  if (item === null) {
    process.stdout.write(JSON.stringify({ bound: false }) + '\n');
    return;
  }

  process.stdout.write(JSON.stringify({ bound: true, item }) + '\n');
}

// ---------------------------------------------------------------------------
// handleEvict — `--evict-pid=<pid> --lease-id=<leaseId>
// --orchestrator-pid=<pid>` composes Phase 1 (phys_footprint victim
// ranking), Phase 2 (`isEvictionAuthorized`, ./lib/recon.mjs), Phase 3
// (`gracefulStop`, ./lib/graceful-stop.mjs), and the durable queue's lease
// primitives (./lib/queue.mjs) into a real, opt-in eviction beat. Standalone
// and decision-only, mirroring `handleDequeue`/`handleDequeueIfCapacity` —
// deliberately NOT routed through `buildTrafficLight` (see
// cli-evict.jest.spec.mjs's design note 3 for the full rationale: keeping
// `'evict'` off buildTrafficLight's reachable-without-explicit-opt-in
// surface is what makes "a bare RED beat can never evict" true by
// construction).
//
// Every non-success path prints `evictionAttempted: true` plus a `reason`
// literal — never a silent, indistinguishable-from-never-attempted pause/hold
// (AC #6, "no silent masking") — but the shape differs depending on WHEN the
// path is taken. Four `reason`s (`'lease-not-found' | 'lease-orchestrator-mismatch' |
// 'not-authorized' | 'candidate-not-found'`) fire before `gracefulStop` is ever called and
// return the SAME shape a bare pause/hold beat would (`{ type: 'pause'|
// 'hold', evictionAttempted: true, reason, ... }`, via `buildFallback`).
// A fifth pre-`gracefulStop` reason, `'orchestrator-pid-mismatch'`,
// fires when the orchestrator is live but the caller-supplied
// `--orchestrator-pid` does not match the pid a PRIOR beat from this same
// `orchestratorId` already recorded on the dibs ledger — see the
// `existingSelf`/`orchestratorIsLive` block below for the full rationale.
// Four more (`'graceful-stop-failed' | 'permission-denied' |
// 'unconfirmed-after-kill' | 'release-failed'`) fire AFTER `gracefulStop` has
// actually run and instead keep `type: 'evict'` — an attempt genuinely
// happened, it just didn't fully confirm the target stopped (or, for
// `'release-failed'`, confirmed the target stopped but then failed to give
// the work item back to the queue) — see the High #1/Low #8 notes further
// down this function for exactly which outcomes route to which of these.
// The one path with NEITHER `evictionAttempted` NOR `reason` is genuine
// success: `{ type: 'evict', pid, outcome, signalsSent, released, item }`.
//
// round-4 review — `ARM_EVICT_TEST_MODE` master switch. This beat's
// several `ARM_FAKE_*` test seams (see the `EVICT_FAKE_SEAM_VARS` comment
// further down for the full list and history) sit upstream of the real
// authorization decision and the real kill/isAlive syscalls. Three
// consecutive review rounds each found a DIFFERENT one of those seams that
// had been left out of a "these must be faked together" enumeration —
// proof that an enumerated list is inherently incomplete-by-construction:
// nothing stops a future seam from being added to this beat without also
// being added to the list. `ARM_EVICT_TEST_MODE=1` closes that class of gap
// architecturally instead of enumerating harder:
//
//   - Unset (the default — true for every real orchestrator invocation):
//     `handleEvict` uses ONLY real adapters — `realEvictKill`,
//     `realEvictIsAlive`, a real `collectPsOutput()` shell-out, a real
//     `Date.now()` via `resolveEvictNow(false)`, and real `footprint`/`vmmap`
//     shell-outs (via `selectPauseCandidateWithFootprint`'s
//     `honorFakeSeam: false`, ./lib/pause-candidate.mjs ->
//     ./lib/footprint-sampler.mjs) for `buildFallback`'s pause-candidate body
//     — full stop, regardless of what any `ARM_FAKE_*` var happens to be set
//     to. A leaked/stale fake var in a real invocation is INERT BY
//     CONSTRUCTION: the real-adapter code paths never read
//     `process.env.ARM_FAKE_*` at all when this switch is off, so there is
//     nothing to leak INTO.
//   - `ARM_EVICT_TEST_MODE=1`: the individual `ARM_FAKE_*` seams work
//     exactly as before (`ARM_FAKE_PS_OUTPUT`, `ARM_FAKE_KILL_LOG`,
//     `ARM_FAKE_KILL_RESULT`, `ARM_FAKE_IS_ALIVE_SEQUENCE`,
//     `ARM_FAKE_NOW_MS` via `resolveEvictNow(true)` -> `resolveNow()`,
//     `ARM_FAKE_FOOTPRINT_OUTPUT` via `honorFakeSeam: true`), still subject
//     to the existing all-or-nothing coupling check (`EVICT_FAKE_SEAM_VARS`)
//     that catches a test author's own mistakes (e.g. faking the ps tree but
//     forgetting to fake the kill log).
//
// Every test in `cli-evict.jest.spec.mjs` and
// `evict-outer-acceptance.jest.spec.mjs` sets `ARM_EVICT_TEST_MODE: '1'` in
// its base env alongside whichever `ARM_FAKE_*` seams it needs.
// `cli-evict.jest.spec.mjs` also carries the test that proves the actual
// security property: setting every `ARM_FAKE_*` seam this beat reads
// WITHOUT `ARM_EVICT_TEST_MODE=1` has NO effect — the beat still shells out
// to a real `ps`, still calls the real kill/isAlive adapters, and still uses
// the real clock.
// ---------------------------------------------------------------------------

/**
 * Whether `psTreeOutput` contains a line whose leading PID column exactly
 * matches `pid` — used only to distinguish the two distinct "not authorized"
 * failure reasons (`'not-authorized'` vs `'candidate-not-found'`):
 * `isEvictionAuthorized` itself collapses both into a single `false`, so this
 * beat needs its own, independent presence check to tell them apart in the
 * fallback `reason` it reports.
 *
 * @param {string} psTreeOutput
 * @param {string|number} pid
 * @returns {boolean}
 */
function psTreeHasPid(psTreeOutput, pid) {
  const target = String(pid);
  const lines = String(psTreeOutput ?? '').split('\n');
  for (const line of lines) {
    // review, Low — explicit header skip, reusing the SAME
    // `PS_HEADER_WITH_ETIME_PATTERN` `isEvictionAuthorized`'s own sibling
    // header check (`PS_HEADER_PATTERN`, ./lib/recon.mjs) is modeled on,
    // rather than relying on the incidental fact that `PID` isn't numeric and
    // so happens not to match the line regex below either. Without this, a
    // future ps-format change that made the header line's first column
    // parse as numeric (however unlikely) would silently break this check.
    if (PS_HEADER_WITH_ETIME_PATTERN.test(line)) continue;
    const match = line.match(/^\s*(\d+)\s+/);
    if (match && match[1] === target) return true;
  }
  return false;
}

// `realEvictKill`/`realEvictIsAlive` live in ./lib/real-evict-adapters.mjs,
// not here — cli.mjs unconditionally calls `main().catch(...)` at module
// load, so any test that `import`ed these adapters directly from this file
// would run the whole CLI beat as a side effect. See that module's header
// comment for the full rationale; real-evict-kill.jest.spec.mjs is the unit
// coverage for both adapters' ESRCH/EPERM/unrecognized-error mapping.

/**
 * Appends one `{ pid, signal }` entry to the JSON-array file at
 * `ARM_FAKE_KILL_LOG` — never a real `process.kill` syscall. Synchronous
 * (mirrors the rest of this file's small, single-shot-process test-seam
 * helpers) since `gracefulStop`'s injected `kill` callback is itself
 * synchronous (see that module's own JSDoc).
 *
 * @param {string} logPath
 * @param {number} pid
 * @param {'SIGTERM'|'SIGKILL'} signal
 */
function appendFakeKillLog(logPath, pid, signal) {
  let entries = [];
  try {
    entries = JSON.parse(readFileSync(logPath, 'utf8'));
    if (!Array.isArray(entries)) entries = [];
  } catch {
    entries = [];
  }
  entries.push({ pid, signal });
  writeFileSync(logPath, JSON.stringify(entries));
}

/**
 * Builds the `kill` callback threaded into `gracefulStop` — the real
 * `process.kill` adapter unless `ARM_FAKE_KILL_LOG` is set, in which case
 * every call is redirected to the fake-kill-log file instead (see
 * `cli-evict.jest.spec.mjs`'s design note 4: this file must never cause a
 * real `kill(2)` syscall against a host PID in any test run).
 * `ARM_FAKE_KILL_RESULT`, when also set, forces every redirected call's
 * return value to that literal (models "already gone by the time we tried
 * to signal it" without a real PID) — otherwise a redirected call reports
 * `'ok'`.
 *
 * Gated behind `testModeEnabled` (`ARM_EVICT_TEST_MODE=1` — see `handleEvict`'s
 * header comment): when `false`, `ARM_FAKE_KILL_LOG` is never even read and
 * the real `realEvictKill` adapter is always returned, regardless of what
 * that env var happens to hold.
 *
 * @param {boolean} testModeEnabled
 * @returns {(pid: number, signal: 'SIGTERM'|'SIGKILL') => 'ok'|'already-gone'|'permission-denied'}
 */
function buildEvictKillFn(testModeEnabled) {
  const fakeLogPath = testModeEnabled ? process.env.ARM_FAKE_KILL_LOG : undefined;
  if (!fakeLogPath) return realEvictKill;

  const forcedResult = process.env.ARM_FAKE_KILL_RESULT;
  return (pid, signal) => {
    appendFakeKillLog(fakeLogPath, pid, signal);
    return forcedResult || 'ok';
  };
}

/**
 * Builds the `isAlive` callback threaded into `gracefulStop` — the real
 * `process.kill(pid, 0)`-based probe (wrapped with `reapAwareIsAlive`'s
 * bounded retry, per : `process.kill(pid, 0)` alone cannot distinguish
 * a running process from an unreaped zombie, so a lone `true` reading is
 * given a brief retry window to observe the OS reap it) unless
 * `ARM_FAKE_IS_ALIVE_SEQUENCE` is set, in which case each successive call
 * consumes one `'true'|'false'` entry from that comma-separated env value,
 * falling back to `'false'` once exhausted (see `cli-evict.jest.spec.mjs`'s
 * design note 4) — this fake path is returned completely unwrapped by
 * `reapAwareIsAlive`, so it keeps consuming exactly one entry per logical
 * `isAlive()` call, unchanged by this behavior.
 *
 * Gated behind `testModeEnabled` (`ARM_EVICT_TEST_MODE=1` — see `handleEvict`'s
 * header comment): when `false`, `ARM_FAKE_IS_ALIVE_SEQUENCE` is never even
 * read and the real `realEvictIsAlive` adapter (wrapped with
 * `reapAwareIsAlive`) is always returned, regardless of what that env var
 * happens to hold.
 *
 * @param {boolean} testModeEnabled
 * @returns {(pid: number) => boolean | Promise<boolean>}
 */
function buildEvictIsAliveFn(testModeEnabled) {
  const raw = testModeEnabled ? process.env.ARM_FAKE_IS_ALIVE_SEQUENCE : undefined;
  if (raw === undefined) {
    return reapAwareIsAlive({
      isAlive: realEvictIsAlive,
      sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    });
  }

  const sequence = raw.split(',').map((entry) => entry.trim());
  let index = 0;
  return () => {
    const value = index < sequence.length ? sequence[index] : 'false';
    index += 1;
    return value === 'true';
  };
}

async function handleEvict(flags) {
  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write('agent-resource-management: --orchestrator-id=<id> is required for --evict-pid\n');
    process.exit(2);
    return;
  }
  if (isReservedEntry(orchestratorId)) {
    process.stderr.write(
      `agent-resource-management: --orchestrator-id must not match the reserved double-underscore convention ` +
        `("__...__"), got "${orchestratorId}" — reserved ids collide with internal coordination-file bookkeeping ` +
        '(e.g. the legacy-unattributed-claim sentinel) and are not valid orchestrator ids\n',
    );
    process.exit(2);
    return;
  }

  const evictPidRaw = flags['evict-pid'];
  // review, Low — require a positive-integer PID string, mirroring
  // `--poll-footprint`'s own `Number.isInteger(pid) && pid > 0` validation,
  // rather than accepting any non-empty string and relying on the ps-tree
  // ancestry match (`isEvictionAuthorized`) to incidentally reject a
  // malformed value later. Defence in depth matters here specifically: this
  // pid ultimately reaches `process.kill(pid, signal)`.
  //
  // round-2 review, Low — `Number.isInteger(Number(x)) && Number(x) > 0`
  // also accepts non-plain-decimal forms `Number()` happily parses, like
  // exponential notation (`"1e2"` -> 100) or hex (`"0x10"` -> 16) — neither is
  // a "positive integer pid string" per this check's own stated intent, even
  // though both are harmless today (a real ps-tree line's leading PID column
  // is always plain decimal, so `"1e2"`/`"0x10"` could never match a real
  // process anyway). An explicit `PID_PATTERN` closes the gap between what
  // this check claims to validate and what it actually accepts, rather than
  // leaving it as an accident of `Number()`'s parsing leniency.
  //
  // final review, Low — what each of the two conditions below actually
  // guards, precisely (the surrounding "positive integer" framing overstated
  // what the trailing `Number(x) <= 0` conjunct contributes beyond the
  // regex): `PID_PATTERN` alone already rejects anything that is not one or
  // more plain decimal digits — no sign, no decimal point, no exponent, no
  // hex prefix, not empty — so every string it accepts parses via `Number()`
  // to a non-negative integer. That leaves exactly ONE value the regex
  // cannot rule out on its own: the literal `"0"` (or an all-zero string
  // like `"00"`), which is a syntactically valid non-negative integer but
  // not a valid pid. `Number(x) <= 0` exists solely to reject that one
  // remaining case; it does no work against anything `PID_PATTERN` doesn't
  // already exclude.
  const PID_PATTERN = /^\d+$/;
  if (
    typeof evictPidRaw !== 'string' ||
    evictPidRaw.length === 0 ||
    !PID_PATTERN.test(evictPidRaw) ||
    // Rejects "0"/"00" — the one value PID_PATTERN's shape check cannot
    // exclude on its own; see the comment above.
    Number(evictPidRaw) <= 0
  ) {
    const received = typeof evictPidRaw === 'string' ? evictPidRaw : String(evictPidRaw);
    process.stderr.write(
      `agent-resource-management: --evict-pid=<pid> requires a positive integer pid, got "${received}"\n`,
    );
    process.exit(2);
    return;
  }

  // whole-branch review, Medium #4 — `--orchestrator-pid` gets the
  // same explicit presence/type validation `--evict-pid` gets above, rather
  // than silently coercing a missing/malformed value to the string
  // `"undefined"` internally. Without this, `isEvictionAuthorized` still
  // fails closed (correctly), but the reported `reason` collapses to the
  // generic `'not-authorized'` — indistinguishable from a genuine
  // cross-orchestrator refusal, violating AC #6's "no silent masking".
  const orchestratorPidRaw = flags['orchestrator-pid'];
  // review, Low — same positive-integer format validation as
  // `--evict-pid` above (see that check's comment, including the round-2
  // `PID_PATTERN` note on rejecting exponential/hex forms): this value feeds
  // the ps-tree ancestry walk in `isEvictionAuthorized`, so a malformed value
  // should fail loudly here rather than merely failing to match any ps line.
  if (
    typeof orchestratorPidRaw !== 'string' ||
    orchestratorPidRaw.length === 0 ||
    !PID_PATTERN.test(orchestratorPidRaw) ||
    Number(orchestratorPidRaw) <= 0
  ) {
    const received = typeof orchestratorPidRaw === 'string' ? orchestratorPidRaw : String(orchestratorPidRaw);
    process.stderr.write(
      `agent-resource-management: --orchestrator-pid=<pid> requires a positive integer pid, got "${received}"\n`,
    );
    process.exit(2);
    return;
  }

  const evictTestModeEnabled = process.env.ARM_EVICT_TEST_MODE === '1';
  let evictionResult;
  try {
    evictionResult = await performEviction({
      orchestratorId,
      orchestratorPidRaw,
      evictPidRaw,
      leaseId: flags['lease-id'],
      evictTestModeEnabled,
    });
  } catch (error) {
    if (error instanceof EvictFakeSeamCouplingError) {
      process.stderr.write(error.message);
      process.exit(2);
      return;
    }
    process.stderr.write(`agent-resource-management: --evict-pid failed — ${error.message}\n`);
    process.exit(2);
    return;
  }

  process.stdout.write(JSON.stringify(evictionResult.body) + '\n');
  if (evictionResult.exitCode) {
    process.exitCode = evictionResult.exitCode;
  }
}

// performEviction — the I/O-and-decision CORE of
// `--evict-pid`, extracted out of `handleEvict` so it can also be reused
// in-process by the watchdog beat's own per-target recycle loop
// (`handleWatchdogBeat` below) — cli.mjs never spawns a child process of its
// own accord (see this file's own header comment / SKILL.md's
// decision-only stance), so the watchdog beat calls this function directly
// rather than shelling back out to itself. `handleEvict` itself keeps every
// flag-format validation (`--orchestrator-id`, `--evict-pid`,
// `--orchestrator-pid` presence/shape) and the final stdout/exitCode
// reporting; this function keeps everything downstream of that — the
// `ARM_EVICT_TEST_MODE` fake-seam coupling check, the lease lookup, the
// liveness/authorization decision, `gracefulStop`, and the queue-release —
// returning `{ body, exitCode }` instead of writing to stdout/exiting
// itself, so BOTH call sites get byte-for-byte the same eviction behavior.
// Every comment on the logic below is preserved verbatim from
// `handleEvict`'s own original, single-call-site version — see 's
// own review history for the "why" of each check. Deliberately positioned
// directly after `handleEvict` with no intervening section divider, so
// `cli-evict.jest.spec.mjs`'s own source-order regression tests (which slice
// from `handleEvict`'s own text up to the NEXT `// ---` section divider)
// keep seeing this logic's literal call sites, in the same order, as part of
// that same slice.
//
// Throws `EvictFakeSeamCouplingError` (a genuine misconfiguration — a
// leftover test-harness env var, a polluted shared env block) and rethrows
// any other unexpected error (e.g. a queue-file read failure) — every call
// site is responsible for catching, formatting a caller-appropriate message,
// and deciding its own exit/propagation behavior; this function never calls
// `process.exit`/writes to `process.stdout`/`process.stderr` itself (with
// the sole pre-existing exception of the cosmetic "succeeded but failed to
// release" warning below, kept verbatim from the original).

// — `armFailBeat` is a plain marker property (not a class check) so
// `watchdog-beat.mjs` can recognize "this is a systemic, whole-process
// misconfiguration, never isolate it to one target" WITHOUT importing this
// class across the DI boundary between the two modules (`watchdog-beat.mjs`
// takes `handleEvict` purely as an injected function — see its own header
// comment — and must never depend on `cli.mjs`'s internals to interpret what
// that function throws). Any future exception this file's `performEviction`
// deliberately throws as a similarly systemic, whole-beat-fatal condition
// should set this same marker rather than growing a parallel `instanceof`
// check on the consuming side.
class EvictFakeSeamCouplingError extends Error {
  constructor(message) {
    super(message);
    this.name = 'EvictFakeSeamCouplingError';
    this.armFailBeat = true;
  }
}

async function performEviction({
  orchestratorId,
  orchestratorPidRaw,
  evictPidRaw,
  leaseId,
  evictTestModeEnabled,
  preTakenSample,
  cachedEvictBeatIdentity,
}) {
  const orchestratorPid = orchestratorPidRaw;

  // round-2 review, Medium, then round-3 review, Medium (recurrence),
  // then round-4 review, Medium (recurrence again) — this beat reads several
  // independent `ARM_FAKE_*` test seams, and every one of them sits upstream
  // of either the authorization decision or the real kill/isAlive syscalls:
  // `ARM_FAKE_PS_OUTPUT` (the ps-tree ancestry data `isEvictionAuthorized`
  // reasons over), `ARM_FAKE_KILL_LOG` (redirects `gracefulStop`'s `kill`
  // callback away from the real `realEvictKill` adapter — see
  // `buildEvictKillFn`), `ARM_FAKE_KILL_RESULT` (forces that redirected
  // call's return value), `ARM_FAKE_IS_ALIVE_SEQUENCE` (redirects
  // `gracefulStop`'s `isAlive` callback away from the real `realEvictIsAlive`
  // adapter — see `buildEvictIsAliveFn`), and `ARM_FAKE_NOW_MS` (redirects
  // `resolveEvictNow()`'s clock, which feeds `readDibs`'s `pruneStale` call
  // below and therefore `isEvictionAuthorized`'s condition 1,
  // `orchestratorIsLive`, DIRECTLY — a stale-but-leaked fake "now" can make a
  // genuinely-dead orchestrator's dibs entry read as live).
  //
  // Three rounds in a row each found a DIFFERENT `ARM_FAKE_*` var this beat
  // reads that had been left out of the "must be faked together" enumeration
  // below (round 2 missed `ARM_FAKE_IS_ALIVE_SEQUENCE`; round 3's fix for
  // that missed `ARM_FAKE_NOW_MS`). Per-variable reasoning about which seams
  // are "provably inert" keeps costing a review round to re-discover the
  // exception that wasn't — and an enumerated list can ALWAYS be incomplete
  // by construction, because nothing stops a future seam from being added to
  // this beat without also being added here. So the fix is architectural,
  // not another list entry:
  //
  // TWO-LAYER MODEL:
  //
  //   Layer 1 — `ARM_EVICT_TEST_MODE=1` is a MASTER switch that must be set
  //   before this beat honors ANY `ARM_FAKE_*` seam at all. When it is unset
  //   (the default — true for every real orchestrator invocation, which has
  //   no reason to ever set it), `resolveEvictNow()`, `buildEvictKillFn()`,
  //   `buildEvictIsAliveFn()`, `collectPsOutput()`'s `honorFakeSeam` flag, and
  //   `selectPauseCandidateWithFootprint()`'s own `honorFakeSeam` flag (which
  //   it threads into `sampleFootprint`, ./lib/pause-candidate.mjs ->
  //   ./lib/footprint-sampler.mjs) are ALL forced onto their real-adapter path
  //   below, regardless of what any `ARM_FAKE_*` var happens to hold. This
  //   makes a leaked/stale fake
  //   var in a real invocation INERT BY CONSTRUCTION — not "inert because
  //   every current seam happens to have been remembered and coupled
  //   correctly" (the property that has now failed three review rounds in a
  //   row), but inert because the real-adapter functions never even look at
  //   `process.env.ARM_FAKE_*` unless this one switch is on. A FUTURE seam
  //   added to this beat inherits this property automatically as long as its
  //   real-vs-fake branch is written the same way (gated on the
  //   `testModeEnabled` parameter threaded through below) — it does not need
  //   a new list entry here to be safe against leakage, only to be caught as
  //   a test-authoring mistake (see Layer 2).
  //
  //   Layer 2 — the enumeration below (now including `ARM_FAKE_NOW_MS`) is
  //   kept, but ONLY as an internal-consistency check among whichever fake
  //   seams a test that HAS opted into `ARM_EVICT_TEST_MODE=1` actually sets:
  //   catching "test author faked the ps tree but forgot to fake the kill
  //   log" is still useful, but it is no longer the mechanism that keeps a
  //   leaked fake var out of a real eviction — Layer 1 does that
  //   unconditionally, before this enumeration is ever consulted.
  const EVICT_FAKE_SEAM_VARS = [
    'ARM_FAKE_PS_OUTPUT',
    'ARM_FAKE_KILL_LOG',
    'ARM_FAKE_IS_ALIVE_SEQUENCE',
    'ARM_FAKE_KILL_RESULT',
    'ARM_FAKE_NOW_MS',
    // review, Medium — joined the coupled seam set alongside the other
    // five: `buildFallback`'s pause-candidate footprint sampling
    // (./lib/pause-candidate.mjs -> ./lib/footprint-sampler.mjs) now also
    // honors `ARM_EVICT_TEST_MODE` via the `honorFakeSeam` flag threaded
    // through both, so this var belongs in the same "faked together or not
    // at all" enumeration as every other seam this beat reads.
    'ARM_FAKE_FOOTPRINT_OUTPUT',
  ];
  // review, High — "set" here must mean the SAME thing each seam's own
  // consumer treats as "fake path selected", not a blanket `!== undefined`.
  // Three of these seams (`ARM_FAKE_PS_OUTPUT` in `collectPsOutput`,
  // `ARM_FAKE_KILL_LOG` in `buildEvictKillFn`, `ARM_FAKE_NOW_MS` in
  // `resolveNow`) are consulted with a truthy/non-blank check, so an EMPTY
  // string silently falls through to the REAL adapter there even though
  // `!== undefined` would have called it "set". Before this fix, setting
  // `ARM_FAKE_KILL_LOG=''` alongside the other five (all non-empty) passed
  // this coupling check as "fully faked" while `buildEvictKillFn` actually
  // returned `realEvictKill` — authorization would be decided from
  // fabricated ps ancestry while the KILL itself hit the real OS `kill(2)`.
  // The other two (`ARM_FAKE_IS_ALIVE_SEQUENCE`, `ARM_FAKE_FOOTPRINT_OUTPUT`)
  // genuinely do treat an empty string as "set" (an intentionally-empty fake
  // sequence/output) in their own consumers, so they keep the `!== undefined`
  // test. `ARM_FAKE_KILL_RESULT` is a modifier of the already-fake kill path
  // (not a real/fake selector on its own) and keeps the loose test too — the
  // documented `''` idiom for "present but inert" in the test files' shared
  // `baseEnv()` relies on this.
  const EVICT_FAKE_SEAM_TRUTHY_VARS = new Set(['ARM_FAKE_PS_OUTPUT', 'ARM_FAKE_KILL_LOG', 'ARM_FAKE_NOW_MS']);
  // pre-PR review, Medium — keeping the rule above TRUE after this
  // ticket. `collectPsOutput` gained a second condition on this branch
  // (`honorFakeSeam && ARM_COORDINATION_FILE && ARM_FAKE_PS_OUTPUT`, see that
  // function), so a truthy `ARM_FAKE_PS_OUTPUT` alone no longer means that
  // consumer takes the fake path. Without mirroring it here, an
  // `ARM_EVICT_TEST_MODE=1` beat with all six seams set but
  // `ARM_COORDINATION_FILE` unset passes this coupling check as "fully faked"
  // while `collectPsOutput` falls through to the REAL `ps` — so
  // `isEvictionAuthorized` would decide authorization from real host ancestry
  // while `buildEvictKillFn` still returned the fake killer. That is exactly
  // the half-faked eviction this gate exists to refuse.
  const EVICT_FAKE_SEAM_EXTRA_CONDITION = { ARM_FAKE_PS_OUTPUT: () => Boolean(process.env.ARM_COORDINATION_FILE) };
  const isEvictFakeSeamSet = (name) => {
    const baseSet = EVICT_FAKE_SEAM_TRUTHY_VARS.has(name)
      ? Boolean(process.env[name])
      : process.env[name] !== undefined;
    // `Object.hasOwn` before the lookup, matching the prototype-gating
    // discipline `lib/reconciliation.mjs` applies to every identity operand: an
    // inherited `Object.prototype` member here would be invoked as a condition.
    if (!Object.hasOwn(EVICT_FAKE_SEAM_EXTRA_CONDITION, name)) return baseSet;
    return baseSet && EVICT_FAKE_SEAM_EXTRA_CONDITION[name]();
  };
  if (evictTestModeEnabled) {
    const evictFakeSeamSet = EVICT_FAKE_SEAM_VARS.filter((name) => isEvictFakeSeamSet(name));
    const evictFakeSeamMissing = EVICT_FAKE_SEAM_VARS.filter((name) => !isEvictFakeSeamSet(name));
    if (evictFakeSeamSet.length > 0 && evictFakeSeamMissing.length > 0) {
      throw new EvictFakeSeamCouplingError(
        `agent-resource-management: --evict-pid refuses to proceed with a partially-faked eviction seam — ` +
          `${evictFakeSeamSet.join(', ')} ${evictFakeSeamSet.length === 1 ? 'is' : 'are'} set but ` +
          `${evictFakeSeamMissing.join(', ')} ${evictFakeSeamMissing.length === 1 ? 'is' : 'are'} not. Every ` +
          `ARM_FAKE_* seam this beat reads (${EVICT_FAKE_SEAM_VARS.join(', ')}) must be faked together or not at ` +
          'all when ARM_EVICT_TEST_MODE=1 — otherwise this beat could authorize a real SIGTERM/SIGKILL against ' +
          'fabricated ancestry data, or judge a real target confirmed-stopped from a fabricated isAlive() ' +
          'sequence. This is refused as a genuine misconfiguration (a leftover test-harness env var, a polluted ' +
          'shared env block), not a valid combination.\n',
      );
    }
  }

  const now = resolveEvictNow(evictTestModeEnabled);
  const queueFilePath = resolveQueueFilePath();
  await ensureQueueDir(queueFilePath);

  // Sample the machine once via the SAME collect()/ARM_FAKE_COLLECT_JSON
  // seam every other beat uses, purely to classify a fallback `type`
  // ('pause' on RED memory, 'hold' otherwise) for every non-`'evict'`
  // outcome below — this beat never routes through `buildTrafficLight` (see
  // this section's header comment), so it derives that classification
  // itself, directly.
  // pre-PR review, Medium — when the watchdog beat has already taken
  // ONE machine sample for this whole beat and threads it in via
  // `preTakenSample` (see `handleWatchdogBeat`'s own hoisted `takeSample`
  // call), reuse it instead of re-sampling `vm_stat`/`ps`/`df` again here —
  // this call site previously ran once PER evict target in the beat's own
  // per-target loop, which is wasted work identical on every iteration
  // within the same beat. The standalone `--evict-pid` call site (no
  // `preTakenSample` supplied) is unaffected — it still samples fresh, once,
  // exactly as before.
  const rawSample = preTakenSample ?? (await takeSample(collect));
  if (!preTakenSample && totalCollectFailure) {
    throw totalCollectFailure;
  }
  const memoryResult = classifyMemoryAxis(
    rawSample.memory,
    DEFAULT_MEMORY_THRESHOLDS,
    undefined,
    DEFAULT_HYSTERESIS_CONFIG,
    undefined,
    now,
  );

  // `buildFallback` samples ps-tree freshly on every call (not once up front)
  // so each of its several call sites below — some of which fire before any
  // queue/dibs I/O, some after — always reflects current process state at
  // the moment it's actually used, rather than a snapshot that may be
  // arbitrarily stale by the time this closure runs.
  // whole-branch review, High #2 — same real phys_footprint-sampling
  // wiring as main()'s own pauseCandidate computation below (see that call
  // site's comment for the full rationale); `buildFallback` is now `async`
  // to accommodate the awaited sampling.
  const buildFallback = async (reason) => {
    const type = memoryResult.state === 'RED' ? 'pause' : 'hold';
    const body = { type, evictionAttempted: true, reason };
    if (type === 'pause') {
      // review, Medium — `honorFakeSeam: evictTestModeEnabled` closes
      // the one eviction-reachable seam (`ARM_FAKE_FOOTPRINT_OUTPUT`) that was
      // previously ungated: without this, a leaked/stale fake footprint var
      // could still shape this beat's emitted `pauseCandidate.physFootprintMb`
      // even with `ARM_EVICT_TEST_MODE` off, contrary to that switch's own
      // "must be set before handleEvict honors ANY of these seams" claim.
      body.pauseCandidate = await selectPauseCandidateWithFootprint(
        listAgentProcesses(collectPsOutput(evictTestModeEnabled), now),
        { honorFakeSeam: evictTestModeEnabled },
      );
    }
    return body;
  };

  // A missing/empty `--lease-id` can never legitimately match an on-disk
  // entry (an UNLEASED entry's own `leaseId` field is also `undefined` — see
  // `releaseQueueEntry`'s doc comment — so comparing against a raw
  // `undefined` would risk a false-positive match against any unleased
  // entry). Fail closed to the same `'lease-not-found'` reason a genuinely
  // unmatched leaseId produces, rather than special-casing this shape.
  // review, Medium — every non-`'evict'`-success fallback path below
  // sets `process.exitCode = 1` (see the CONFIRMED_STOPPED_OUTCOMES release
  // paths further down for the pre-existing precedent this follows): AC #6's
  // "no silent masking" principle already forces `evictionAttempted`/`reason`
  // into the JSON body on every one of these outcomes, but the exit code was
  // left at the default 0 — indistinguishable, to any caller that gates on
  // exit code alone, from a genuinely successful eviction. A single non-zero
  // code is used uniformly rather than distinguishing severities: nothing in
  // this beat's contract promises a stable per-reason exit-code mapping, and
  // the JSON body's `reason`/`outcome` fields are already the documented way
  // to distinguish outcomes programmatically.
  if (typeof leaseId !== 'string' || leaseId.length === 0) {
    return { body: await buildFallback('lease-not-found'), exitCode: 1 };
  }

  const items = await peekQueue(queueFilePath, now);
  const leasedEntry = items.find((item) => item.leaseId === leaseId);

  if (!leasedEntry) {
    return { body: await buildFallback('lease-not-found'), exitCode: 1 };
  }

  // review, High #1 — a leaseId's on-disk queue entry carries the
  // `orchestratorId` of whoever originally ENQUEUED that work item, which is
  // NOT necessarily the caller of this eviction beat: queue peeks are not
  // scoped to a single orchestrator, so any orchestrator that can see
  // another orchestrator's leaseId (e.g. via `--peek`) could otherwise pair
  // it with one of its OWN authorized pids and release a lease it has no
  // legitimate relationship to. Verify the two match BEFORE any
  // authorization/kill work happens — never touching the process or the
  // queue entry when they don't.
  //
  // This closes the cross-orchestrator variant completely (an orchestrator
  // can now only ever release its OWN leases). It does NOT verify that
  // `--evict-pid` and `--lease-id` name the SAME in-flight unit of work when
  // both legitimately belong to the SAME orchestrator managing several
  // concurrent agents (a caller-side pid/lease pairing bug) — closing that
  // required binding a specific pid to a specific lease at claim time (a
  // queue-schema change to the claim/dispatch flow). That binding has now
  // shipped: `bindPidToLease` (Phase 1, ./lib/queue.mjs)
  // populates `leasedEntry.pid`, and the `pid-lease-mismatch` check just
  // below (Phase 2) enforces it here. What remains is Phase 3 — wiring a
  // production `--bind-pid` CLI caller so real dispatch flows actually call
  // `bindPidToLease` — before the gap is closed end-to-end in practice.
  // Tracked separately as follow-up work.
  if (leasedEntry.orchestratorId !== orchestratorId) {
    return { body: await buildFallback('lease-orchestrator-mismatch'), exitCode: 1 };
  }

  // Phase 2 — same-orchestrator pid/lease pairing. The check above
  // proves this orchestrator owns `leaseId`; it says nothing about whether
  // `--evict-pid` is the SAME in-flight unit of work that lease was actually
  // checked out for. An orchestrator managing several concurrent agents could
  // otherwise pair its own genuinely-authorized, ps-tree-descendant pid with
  // ANY of its own leases and release one it has no relationship to (the
  // "same-orchestrator pid/lease pairing" gap named in the
  // `lease-orchestrator-mismatch` block's own comment above, and tracked
  // separately).
  //
  // `leasedEntry.pid` is populated by `bindPidToLease` (./lib/queue.mjs,
  // Phase 1) at claim time. Reject before any authorization/kill work when it
  // is present and doesn't match `--evict-pid` — additive to, never a
  // substitute for, the ps-tree ancestry check further down: a numeric pid
  // match here proves nothing about genuine process ancestry (e.g. OS pid
  // reuse), so `isEvictionAuthorized` still runs, unconditionally, on every
  // pairing that passes this gate.
  //
  // `leasedEntry.pid === undefined` (every pre-existing lease on disk, and any
  // lease claimed by a caller that hasn't yet adopted `--bind-pid`) is
  // deliberately skip-not-fail here: there is nothing bound to compare
  // against, so this new check stays inert and the pre-existing ps-tree-only
  // authorization path is unaffected — the ticket's own backward-compatibility
  // requirement.
  if (leasedEntry.pid !== undefined && leasedEntry.pid !== Number(evictPidRaw)) {
    return { body: await buildFallback('pid-lease-mismatch'), exitCode: 1 };
  }

  // Determine `orchestratorIsLive` for `isEvictionAuthorized`: liveness must
  // reflect a dibs entry that ALREADY EXISTED before this eviction attempt —
  // one made by an earlier, genuine beat from this orchestrator — never this
  // call's own declaration. `readDibs` therefore runs FIRST, before
  // `declareDibs`, and `orchestratorIsLive` is derived from THAT result
  // (whole-branch review, Medium #3). Computing it from a readDibs
  // taken AFTER this beat's own declareDibs (the previous approach) made the
  // check a no-op by construction: the orchestrator's own entry is always
  // present in that later read regardless of whether it had ever made a real
  // beat before, so any orchestrator id could "prove" liveness simply by
  // asking to evict something. `declareDibs` still runs afterward, purely
  // for normal ledger bookkeeping (refreshing this orchestrator's own
  // liveness/priority for FUTURE beats) — its result is never consulted for
  // THIS beat's own authorization decision.
  //
  // review, High #2 — `declareDibs` below is gated on `orchestratorIsLive`
  // being ALREADY true, not called unconditionally. Calling it unconditionally
  // (the original approach) let a never-before-seen `orchestratorId` self-declare
  // as a SIDE EFFECT of a *failed* eviction attempt: a first `--evict-pid`
  // invocation from a brand-new orchestrator id would correctly fail
  // `not-authorized` (its own `existingSelf` was `undefined`), but that same
  // failed attempt's `declareDibs` call persisted a fresh ledger entry for it —
  // so an immediately-following, otherwise-identical second invocation would
  // read that entry back as `existingSelf !== undefined` and pass the liveness
  // gate it had never legitimately earned. Only a genuinely already-live
  // orchestrator (one with a real dibs entry from an earlier, non-eviction beat)
  // gets its ledger entry refreshed here; a never-live one is left exactly as
  // not-live as it started, so repeating the same eviction attempt can never
  // manufacture liveness.
  const coordinationFilePath = resolveCoordinationFilePath();
  await ensureCoordinationDir(coordinationFilePath);

  let existingDibs = [];
  try {
    existingDibs = await readDibs(coordinationFilePath, now, { livenessThresholdMs: DEFAULT_LIVENESS_THRESHOLD_MS });
  } catch (error) {
    warnCoordinationDegraded(error);
  }

  const existingSelf = existingDibs.find((entry) => entry.orchestratorId === orchestratorId);
  const orchestratorIsLive = existingSelf !== undefined;

  // — orchestrator-pid binding. `isEvictionAuthorized` (./lib/recon.mjs)
  // only verifies that `--evict-pid` descends, via ps-tree ancestry, from the
  // caller-supplied `--orchestrator-pid` — it never verifies that
  // `--orchestrator-pid` itself actually belongs to the calling
  // `orchestratorId`. Without this check, a live orchestrator with a
  // genuinely-owned lease could name an arbitrary `--orchestrator-pid` (e.g.
  // a DIFFERENT orchestrator's real root pid) and evict any candidate
  // reachable from THAT root, so long as it happens to descend from it.
  //
  // Bind the two here, before any ps-tree ancestry work or `declareDibs`
  // write: `existingSelf` is this orchestrator's OWN dibs-ledger entry, read
  // above via `readDibs` from a PRIOR beat (never this call's own — see the
  // `orchestratorIsLive` block's rationale just above), and its `pid` field
  // is the root pid that prior beat genuinely resolved for this
  // `orchestratorId`. A caller-supplied `--orchestrator-pid` that doesn't
  // equal it is either a stale/mistaken value or an attempt to borrow another
  // orchestrator's identity — reject before any authorization/kill work,
  // exactly like the `lease-orchestrator-mismatch` check above.
  //
  // Gated on `orchestratorIsLive` — an orchestrator with no prior ledger
  // entry at all has no recorded pid to bind against, and must keep falling
  // through to the existing liveness gate inside `isEvictionAuthorized`
  // (`not-authorized`/`candidate-not-found`), not this new reason.
  //
  // `existingSelf.pid === undefined` (a ledger entry whose own prior beat's
  // ps-tree identity resolution never succeeded — see `withPidIdentity`,
  // ./lib/coordination-file.mjs, which never writes a sentinel, only omits
  // the field entirely) is deliberately treated as a MISMATCH, not as "skip
  // the check" — per this issue's own Build Plan acceptance criterion: there
  // is nothing recorded to prove the caller-supplied `--orchestrator-pid`
  // against, and an unverifiable claim must fail closed exactly like a
  // provably-false one, not silently fall through to the pre-existing
  // ps-tree-ancestry-only check (which would reopen the exact
  // ancestry-alone-authorizes-eviction gap this fix exists to close, for any
  // orchestrator whose most recent beat happened not to resolve a pid). The
  // residual risk this still leaves open — a caller can be wrongly refused
  // (never wrongly authorized) when no PRIOR beat ever resolved a pid, e.g.
  // immediately after a beat whose ps-tree snapshot was flaky — is the
  // accepted, narrowed PID-reuse-adjacent limitation documented in
  // `.claude/skills/agent-resource-management/SKILL.md`.
  if (
    orchestratorIsLive &&
    (existingSelf.pid === undefined || Number(orchestratorPid) !== existingSelf.pid)
  ) {
    return { body: await buildFallback('orchestrator-pid-mismatch'), exitCode: 1 };
  }

  if (orchestratorIsLive) {
    // — the eviction beat's ledger identity, resolved from a snapshot of
    // its own rather than from `observeBeatProcesses`. This is the ONE
    // documented exception to the one-identity-per-beat rule, named as such in
    // that rule's own block ("where ledger identity comes from", near
    // `resolveNow`) — read that block before "fixing" this to share the beat
    // observation. Two snapshots on this one beat is deliberate, because the
    // two uses have contradictory requirements that no single snapshot
    // satisfies:
    //   * this one honours the `ARM_EVICT_TEST_MODE` master switch through
    //     `collectPsOutput(evictTestModeEnabled)`, so a leaked
    //     `ARM_FAKE_PS_OUTPUT` cannot decide what identity a real eviction beat
    //     records (the two-layer model `handleEvict`'s header describes);
    //   * the authorization snapshot below must be taken immediately before
    //     `isEvictionAuthorized` reads it, after all the awaited I/O in this
    //     function, or a pid could exit and be reused inside the gap.
    // Taken here, outside `declareDibs`' critical section, for the same
    // lock-hold reason every other call site resolves before its write.
    const evictBeatIdentity =
      cachedEvictBeatIdentity !== undefined
        ? cachedEvictBeatIdentity
        : resolveNearestClaudeRootIdentity(
            collectPsOutput(evictTestModeEnabled),
            process.ppid,
            resolveProcessSnapshotNow(evictTestModeEnabled),
          );

    try {
      // Review finding (HIGH, verification pass) — `declareDibs`
      // performs a full-entry REPLACE on upsert, not a field-level merge
      // (see its own doc comment: only `firstDeclaredAt` is carried forward
      // automatically). This call site must therefore carry forward every
      // other field this orchestrator's existing entry already holds —
      // `agentClasses` and the memory/disk hysteresis histories — exactly as
      // the `--running-agent-classes` call site above already does, or a
      // liveness-refresh beat (`--evict-pid`) would silently wipe them from
      // the ledger, reproducing the same undercounting failure mode
      // `reconcileMemoryProjectionAdmission` was fixed against elsewhere.
      await declareDibs(
        coordinationFilePath,
        {
          orchestratorId,
          desiredAgents: existingSelf?.desiredAgents ?? 0,
          declaredAt: now,
          firstDeclaredAt: existingSelf?.firstDeclaredAt ?? now,
          agentClasses: readAgentClasses(existingSelf),
          memoryHistory: existingSelf?.memoryHistory,
          diskHistory: existingSelf?.diskHistory,
          // — ledger write call site 4 of 5. `declareDibs` REPLACES the
          // whole entry, so the identity is restated here rather than inherited
          // from `existingSelf`: an eviction beat that omitted it entirely would
          // strip a live orchestrator's identity off its own entry and drop it
          // back to TTL-only governance. Absent when none resolved, never a
          // sentinel.
          //
          // AND THE STRIP IS STILL REACHABLE ON THIS PATH, ACCEPTED (pre-PR
          // review, Low — the sentence above read as though restating the
          // identity closed it). `evictBeatIdentity` is resolved from
          // `collectPsOutput`, which swallows a failed shell-out and answers
          // `''`, and this path runs no `classifyProcessSnapshotShape` check —
          // so a flaky or drifted `ps` resolves `null` here and this write drops
          // the entry's existing `pid`/`pidStartedAt`. Carrying `existingSelf`'s
          // pair forward instead was considered and REJECTED: after a
          // crash-and-respawn under the same `orchestratorId` the surviving
          // entry can name the DEAD incarnation's root, and re-persisting that
          // would hand the next sweep a `pid-absent` death certificate for the
          // live respawn. Stripping costs one beat of TTL-only governance and
          // the next ordinary beat restores the identity; the alternative risks
          // a live reap. Fail-safe direction, deliberately.
          ...(evictBeatIdentity === null
            ? {}
            : { pid: evictBeatIdentity.pid, pidStartedAt: evictBeatIdentity.pidStartedAt }),
        },
        { livenessThresholdMs: DEFAULT_LIVENESS_THRESHOLD_MS },
      );
    } catch (error) {
      warnCoordinationDegraded(
        error,
        'coordination file',
        "this beat's dibs declaration was not persisted; using the last known on-disk view",
      );
    }
  }

  // Fresh ps-tree sample taken immediately before `isEvictionAuthorized` —
  // per that function's own doc comment (./lib/recon.mjs), it must be given
  // a snapshot taken right before use to be safe against PID-reuse races.
  // Everything above this point (queue read, coordination-dir setup,
  // readDibs/declareDibs) is awaited I/O that can take arbitrarily
  // long; sampling any earlier would let the target pid exit and be reused
  // by an unrelated process during that window, defeating the safety
  // contract. The same sample is reused immediately below for the
  // `psTreeHasPid` reason check on the not-authorized path — that reuse is
  // safe since no further I/O happens in between.
  const psTreeOutput = collectPsOutput(evictTestModeEnabled);

  const authorized = isEvictionAuthorized({
    psTreeOutput,
    orchestratorPid,
    orchestratorIsLive,
    candidatePid: evictPidRaw,
  });

  if (!authorized) {
    const reason = psTreeHasPid(psTreeOutput, evictPidRaw) ? 'not-authorized' : 'candidate-not-found';
    return { body: await buildFallback(reason), exitCode: 1 };
  }

  const pid = Number(evictPidRaw);
  const graceMsRaw = process.env.ARM_EVICT_GRACE_MS;
  // `graceMsRaw !== undefined && graceMsRaw !== ''` — an empty-string env var
  // (e.g. an accidentally-blank shell variable passed through) must fall back
  // to the real default, not silently coerce via `Number('') === 0` into an
  // immediate-SIGKILL, zero-grace beat (review, Low).
  //
  // review, Medium — `Number('0')` and `Number('-1')` are both finite,
  // so a literal `'0'`/negative override survived the check above and
  // degraded the advertised graceful SIGTERM -> grace -> SIGKILL stop into
  // an immediate hard kill, silently. Only a genuinely positive override is
  // accepted; anything else (non-numeric, zero, negative) falls back to
  // `gracefulStop`'s own real default the same way `undefined` already does.
  const parsedGraceMs = graceMsRaw !== undefined && graceMsRaw !== '' ? Number(graceMsRaw) : NaN;
  const graceMs = Number.isFinite(parsedGraceMs) && parsedGraceMs > 0 ? parsedGraceMs : undefined;

  // `gracefulStop` deliberately throws (via `assertKnownKillResult`) on an
  // unrecognized `kill()` result — "fail loud, not open" per Phase 3's own
  // design. That must never CRASH this beat to `main().catch()`'s generic
  // `exit(1)` (which would give zero `evictionAttempted`/`reason`
  // observability): the thrown error is caught and reported as an honest JSON
  // result instead.
  //
  // whole-branch review, High #1 — the lease must NEVER be released
  // when `gracefulStop`'s outcome does not CONFIRM the target is no longer
  // running: `'permission-denied'` (SIGTERM was refused — never even
  // delivered, still alive) and `'unconfirmed-after-kill'` (SIGKILL sent but
  // `isAlive` still reports true afterward). Releasing on either of those
  // would let a dispatcher immediately re-dequeue and re-dispatch the SAME
  // work to a second agent while the original may still be executing it —
  // double execution.
  //
  // review, Medium — a thrown `stopError` gets exactly the same
  // treatment, not a separate "always release" carve-out. The original
  // approach released unconditionally on any throw, reasoned as "the
  // situation is genuinely unknown, and holding forever is worse than an
  // honest unknown" — but that reasoning is inconsistent with
  // `permission-denied`/`unconfirmed-after-kill` immediately above, where an
  // equally "unknown-whether-it-will-exit" outcome is deliberately NOT
  // released for exactly the double-execution reason. In practice the only
  // realistic way `gracefulStop` throws is `realEvictKill` propagating an
  // unmapped `process.kill` error (anything other than `ESRCH`/`EPERM`) when
  // sending SIGTERM — meaning SIGTERM was never confirmed delivered, i.e. the
  // target must be presumed still running, exactly like `permission-denied`.
  // Only the three outcomes below CONFIRM the process is no longer running;
  // a throw is treated the same as "not confirmed" and never releases. The
  // lease still expires naturally via the queue's own claim TTL if the
  // candidate is genuinely abandoned, same self-healing story already relied
  // on for `permission-denied`/`unconfirmed-after-kill`.
  const CONFIRMED_STOPPED_OUTCOMES = new Set(['already-gone', 'exited-before-grace', 'killed-after-grace']);

  let stopResult;
  let stopError;
  let released = null;
  let releaseError = null;
  try {
    stopResult = await gracefulStop({
      pid,
      kill: buildEvictKillFn(evictTestModeEnabled),
      isAlive: buildEvictIsAliveFn(evictTestModeEnabled),
      sleep: (ms) => new Promise((resolveSleep) => setTimeout(resolveSleep, ms)),
      graceMs,
      // review, High — re-prove authorization from a FRESH ps
      // snapshot immediately before the SIGKILL escalation, closing the
      // PID-reuse window `graceMs` (default 60s) leaves open between the
      // authorization check above and this point (see `gracefulStop`'s own
      // `verifyStillTarget` doc comment for the full scenario). Reuses the
      // exact same `evictTestModeEnabled`-gated `collectPsOutput` seam and
      // `isEvictionAuthorized` call as the pre-`gracefulStop` check above —
      // a leaked fake var is inert here for the identical reason it's inert
      // there.
      verifyStillTarget: () =>
        isEvictionAuthorized({
          psTreeOutput: collectPsOutput(evictTestModeEnabled),
          orchestratorPid,
          orchestratorIsLive,
          candidatePid: evictPidRaw,
        }),
    });
  } catch (error) {
    stopError = error;
  } finally {
    const shouldRelease = stopError === undefined && CONFIRMED_STOPPED_OUTCOMES.has(stopResult?.outcome);
    if (shouldRelease) {
      // Return the item to the queue via `releaseQueueEntry` (clears the
      // lease, preserves everything else), NOT `enqueueItem` (which would
      // orphan this entry and reset its fairness/aging position — see
      // cli-evict.jest.spec.mjs design note 5).
      try {
        released = await releaseQueueEntry(queueFilePath, leaseId);
      } catch (error) {
        releaseError = error;
        process.stderr.write(
          `agent-resource-management: --evict-pid succeeded but failed to release the queue entry — ${error.message}\n`,
        );
      }
    }
  }

  if (stopError) {
    return {
      body: {
        type: 'evict',
        pid,
        evictionAttempted: true,
        outcome: 'graceful-stop-failed',
        reason: 'graceful-stop-failed',
        error: stopError.message,
        released: released !== null,
        item: released,
      },
      exitCode: 1,
    };
  }

  if (!CONFIRMED_STOPPED_OUTCOMES.has(stopResult.outcome)) {
    // 'permission-denied' or 'unconfirmed-after-kill' — the target is
    // CONFIRMED STILL RUNNING (see the High #1 note above). 'target-changed-
    // before-kill' (review, High — `verifyStillTarget` above) is the
    // same "do not treat as confirmed stopped" bucket for a different
    // reason: the pid this beat started with may no longer BE the
    // authorized target (a PID-reuse race across the grace window), so no
    // SIGKILL was even sent — it is deliberately not distinguished from the
    // other two here for the same reason they aren't distinguished from each
    // other: none of them may release the lease or report success. The
    // lease was deliberately NOT released above; report that honestly
    // rather than printing a body indistinguishable from a genuine
    // successful eviction. The lease still expires naturally via the
    // queue's own claim TTL if this candidate is genuinely abandoned.
    // review, Nit — no separate `reason` field on this path: `outcome`
    // (`'permission-denied'`/`'unconfirmed-after-kill'`/`'target-changed-
    // before-kill'`) already tells the same story verbatim, unlike the
    // PRE-`gracefulStop` fallbacks above (`buildFallback`'s callers), which
    // have no `outcome` field at all and so genuinely need a distinct
    // `reason` literal to explain why no attempt was made.
    // review, Medium — the target is CONFIRMED STILL RUNNING here (see
    // the block comment above): this is the operationally worst outcome this
    // beat can report, so it must never exit 0. Previously this path fell
    // through to the default exit code while a merely-cosmetic release
    // failure below (`releaseError`/`released === null`, where the target IS
    // confirmed stopped) explicitly set exitCode 1 — backwards relative to
    // actual risk. Every non-`'evict'`-success outcome now sets exitCode 1
    // uniformly; see the comment above the `lease-not-found` check earlier in
    // this function for the rationale on using a single non-zero code rather
    // than distinguishing severities.
    return {
      body: {
        type: 'evict',
        pid,
        evictionAttempted: true,
        outcome: stopResult.outcome,
        signalsSent: stopResult.signalsSent,
        released: false,
      },
      exitCode: 1,
    };
  }

  if (releaseError) {
    // whole-branch review, Low #8 — `releaseQueueEntry` itself
    // throwing on this confirmed-stopped path must be OBSERVABLE, not a
    // silent "successful" exit 0: a non-zero exit code plus an explicit
    // `reason: 'release-failed'` in the JSON body, rather than the bare
    // `released: false` this path previously reported with no way to tell
    // it apart from any other reason `released` might be false.
    return {
      body: {
        type: 'evict',
        pid,
        evictionAttempted: true,
        outcome: stopResult.outcome,
        signalsSent: stopResult.signalsSent,
        reason: 'release-failed',
        released: false,
      },
      exitCode: 1,
    };
  }

  if (released === null) {
    // review, Medium — `releaseQueueEntry` returning `null` (no
    // matching on-disk entry, e.g. the lease was reclaimed/expired by
    // something else during the grace window between the `peekQueue` above
    // and this release call) is not a throw, so `releaseError` above is
    // unset and this path was falling through to the block below with
    // `released: false` and NO `reason` field — indistinguishable from a
    // genuinely successful eviction at exit 0. Report it the same
    // observable way as the `releaseError`-thrown case just above: a
    // distinct `reason` literal and a non-zero exit code.
    return {
      body: {
        type: 'evict',
        pid,
        evictionAttempted: true,
        outcome: stopResult.outcome,
        signalsSent: stopResult.signalsSent,
        reason: 'release-not-found',
        released: false,
      },
      exitCode: 1,
    };
  }

  return {
    body: {
      type: 'evict',
      pid,
      outcome: stopResult.outcome,
      signalsSent: stopResult.signalsSent,
      released: true,
      item: released,
    },
  };
}

// ---------------------------------------------------------------------------
// handleWatchdogBeat — `--watchdog-beat --orchestrator-id=<id>
// --orchestrator-pid=<pid> [--watchdog-bound-ms=<n>]
// [--watchdog-deadlock-window-ms=<n>] [--watchdog-aging-bound-ms=<n>]`
// composes `./lib/watchdog-beat.mjs`'s `runWatchdogBeat` (Phases 1-5) with
// `./lib/watchdog-backoff.mjs`'s shared-pacing-pattern-shaped pacing gate into a
// single standalone, opt-in beat — mirroring `--evict-pid`'s own shape
// (one-shot, conflict-checked, prints exactly one JSON body, never composed
// with `--desired-agents`/`--heartbeat`/etc. in the same invocation).
//
// EVICTION WIRING — this beat's own `handleEvict` injection (the seam
// `runWatchdogBeat` requires) calls `performEviction` — the same
// I/O-and-decision core `handleEvict` itself calls (defined immediately
// above, right after `handleEvict`) — directly, IN-PROCESS. cli.mjs never
// spawns a child process of its own accord (this file's own header comment
// / SKILL.md document that as a hard invariant, and
// `cli-aimd-ceiling.jest.spec.mjs`'s own structural regression test pins the
// child-process-spawning call-site count in this file at zero), so this
// beat reuses the real `--evict-pid` eviction behavior by calling the SAME
// extracted function `handleEvict` calls, never by shelling back out to
// itself.
//
// CANDIDATE/LEDGER RESOLUTION — this beat only ever considers pids it can
// prove are the caller's OWN spawned descendants (via the same
// `isEvictionAuthorized` ancestry check `--evict-pid` itself re-verifies
// before any real kill), and only ever pairs them with THIS caller's OWN
// leased queue items (`peekQueue` filtered to `orchestratorId ===
// <the calling orchestrator>`), zipped in ps-tree/queue order. This is a
// deliberate, documented simplification for this phase — it assumes roughly
// one leased work item per spawned candidate pid, the common case; a future
// phase could bind a specific pid to a specific lease at claim time (the
// same gap `--evict-pid`'s own SKILL.md documents under "same-orchestrator
// pid/lease pairing",) to close the general N-candidates/M-leases
// case exactly.
//
// RE-ENTRANCY POLICY — `claimLedger` records only ever get `reentrant: true`
// (the only value `./lib/eviction-targeting.mjs`'s conservative default
// routes to an actual auto-evict) when the leased item's own `commandRef`
// both names `resumable-workflow` AND carries an explicit `--resume=` flag — see
// `isReentrantCommand`'s own doc comment. `resumable-workflow` is the one skill in
// this repo's catalog explicitly documented as "Re-entrant — picks up from
// the last completed phase on restart"; every other/unrecognized
// `commandRef` shape defaults to the safe `alert-only` routing.
// ---------------------------------------------------------------------------

/**
 * Conservative re-entrancy policy for this beat's own claimLedger
 * assembly (never read by `./lib/eviction-targeting.mjs` itself — that
 * module only ever sees the resulting plain boolean). `resumable-workflow` is the
 * one skill in this repo documented as resumable from a persisted phase
 * checkpoint; only an invocation that ALSO carries an explicit `--resume=`
 * flag (i.e. one that has something to resume FROM) is treated as safe to
 * auto-recycle. Every other `commandRef` shape — including a bare, first-run
 * `resumable-workflow` invocation with no `--resume=` yet — defaults to the safe
 * `alert-only` routing `resolveEvictionTargets` already applies to any
 * record with `reentrant !== true`.
 *
 * @param {unknown} commandRef
 * @returns {boolean}
 */
function isReentrantCommand(commandRef) {
  return typeof commandRef === 'string' && commandRef.includes('resumable-workflow') && commandRef.includes('--resume=');
}

/**
 * Parses a JSON object env-var seam (`ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON`/
 * `ARM_FAKE_WORKTREE_MTIME_JSON`) into a plain object, tolerating an
 * unparsable value by degrading to `{}` rather than throwing — matches this
 * whole skill's fail-open-on-read posture for test-seam inputs.
 *
 * @param {string} raw
 * @returns {Record<string, unknown>}
 */
function parseFakeSeamMap(raw) {
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

/**
 * Resolves this beat's own `candidates` array (Phase 1's per-agent stall
 * judgement input): every ps-tree root pid this beat can PROVE is the
 * calling orchestrator's own spawned descendant (reusing
 * `isEvictionAuthorized`'s own ancestry walk, condition 1 — liveness —
 * deliberately bypassed here since this is a candidate-selection heuristic,
 * not the real authorization gate `--evict-pid` re-verifies before any real
 * kill), paired with each pid's footprint trajectory / worktree mtime.
 *
 * Both signals honour the two new fake seams
 * `ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON`/`ARM_FAKE_WORKTREE_MTIME_JSON` when
 * `evictTestModeEnabled` (`ARM_EVICT_TEST_MODE=1`) is set, per the outer
 * acceptance test's own documented seam contract — the SAME master-switch
 * gate every other `ARM_FAKE_*` seam in this file already honours. Outside
 * test mode (or in test mode with the fake mtime seam absent, mirroring the
 * `ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON` fallthrough immediately below):
 * footprint trajectory is read (never mutated/consumed) from the real,
 * already-persisted `--poll-footprint` state (`./lib/footprint-state.mjs`'s
 * `readFootprintState`); worktree-mtime is resolved via the real scanner
 * (`./lib/worktree-mtime-scanner.mjs`'s `getRealWorktreeMtimeMs`,
 * Phase 3) — a git-tracked-file mtime walk of the candidate pid's cwd. Each
 * candidate's real-scanner call is individually try/caught: an unexpected
 * throw from one candidate degrades ONLY that candidate's `worktreeMtimeMs`
 * to `null` ("unknown") rather than aborting the whole beat, even though the
 * scanner's own documented contract says it never throws (defense in
 * depth).
 *
 * Phase 4 pre-PR review, High 2 — each candidate's `leakVelocityState`
 * (the input `watchdog-beat.mjs`'s Step 1b leak-velocity trip loop consumes)
 * is derived here, fresh every beat, via `buildLeakVelocityStateForCandidate`
 * — a replay of the pid's real, already-persisted footprint-state.mjs
 * trajectory, mirroring the same reconstruction `computeLeakBlocksForBeat`
 * (this file's detection-only echo) already uses. This closes the gap where
 * `--watchdog-beat`'s real eviction wiring was previously unreachable from
 * production: prior to this fix, `buildWatchdogCandidates` never set
 * `leakVelocityState` at all, so a real sustained-leak pid could never trip
 * via `watchdog-beat.mjs`'s Step 1b outside a hand-built test. Left
 * `undefined` under `ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON` (see the inline
 * comment at that branch) — that fake seam carries no timestamp data to
 * replay from. Also left `undefined` (pre-PR review round 2, Medium)
 * for an entry whose `lastPolledAt` is stale against
 * `DEFAULT_LIVENESS_THRESHOLD_MS` — see the inline staleness-guard comment at
 * that branch, which mirrors `computeLeakBlocksForBeat`'s own guard so the
 * real eviction path never trusts stale/pid-reused evidence either.
 *
 * @param {{ orchestratorPid: number, now: number, evictTestModeEnabled: boolean, footprintStateFilePath: string, deps?: { getRealWorktreeMtimeMs?: (pid: number) => Promise<number|null> } }} params
 * @returns {Promise<Array<{ pid: number, footprintTrajectory: number[], worktreeMtimeMs: number|null, leakVelocityState?: object }>>}
 */
export async function buildWatchdogCandidates({
  orchestratorPid,
  now,
  evictTestModeEnabled,
  footprintStateFilePath,
  deps = {},
}) {
  const resolveRealWorktreeMtimeMs = deps.getRealWorktreeMtimeMs ?? getRealWorktreeMtimeMs;
  // Candidate discovery's own `ps` fakery is gated behind `evictTestModeEnabled`
  // — the SAME `evictTestModeEnabled`-gated `collectPsOutput` seam every other
  // eviction-path call site in this file uses (bot-review fix: an
  // earlier Phase 3 revision honoured `ARM_FAKE_PS_OUTPUT` unconditionally
  // here, which would let a leaked production env var corrupt candidate
  // discovery outside test mode — production security posture should not be
  // relaxed to satisfy a test). Callers that need a synthetic process tree
  // for the real worktree-mtime scanner (e.g.
  // `cli.watchdog-candidates-real-scanner.jest.spec.mjs`'s direct-import unit
  // tests) must pass `evictTestModeEnabled: true` alongside `ARM_FAKE_PS_OUTPUT`.
  const psTreeOutput = collectPsOutput(evictTestModeEnabled);
  const isOwnedByOrchestrator = (pid) =>
    isEvictionAuthorized({
      psTreeOutput,
      orchestratorPid: String(orchestratorPid),
      orchestratorIsLive: true,
      candidatePid: String(pid),
    });
  const ownedEntries = listAgentProcesses(psTreeOutput, now).filter((entry) => isOwnedByOrchestrator(entry.agentId));

  const fakeTrajectoryRaw = process.env.ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON;
  const fakeTrajectoryMap =
    evictTestModeEnabled && fakeTrajectoryRaw !== undefined ? parseFakeSeamMap(fakeTrajectoryRaw) : null;
  const fakeMtimeRaw = process.env.ARM_FAKE_WORKTREE_MTIME_JSON;
  const fakeMtimeMap = evictTestModeEnabled && fakeMtimeRaw !== undefined ? parseFakeSeamMap(fakeMtimeRaw) : null;

  // pre-PR review, Medium — the footprint-state store is read/parsed
  // ONCE for the whole candidate set here (rather than once PER candidate via
  // `readFootprintState`, which re-reads and re-parses the entire on-disk
  // store file on every call) — a real orchestrator can own many candidate
  // pids per beat, so this collapses N full-file reads into one.
  const footprintStore = fakeTrajectoryMap
    ? null
    : await readFootprintStore(footprintStateFilePath).catch(() => ({}));

  const candidates = [];
  for (const entry of ownedEntries) {
    const pid = Number(entry.agentId);

    let footprintTrajectory;
    // Phase 4 pre-PR review, High 2 — `leakVelocityState` (the real
    // eviction-wiring input `watchdog-beat.mjs`'s Step 1b consumes) is only
    // ever derived from the REAL, already-persisted footprint-state.mjs
    // store below, never from `ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON`. That fake
    // seam carries no timestamp data at all (just a bare number array), so
    // there is nothing to reconstruct per-pair timestamps from — leaving
    // `leakVelocityState` unset for a faked candidate exactly matches
    // `buildLeakVelocityStateForCandidate`'s own documented "cannot
    // reconstruct at all" contract (`undefined`), and preserves every
    // existing `ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON`-based test's behavior
    // unchanged (that seam exists for Phase 1's stall-detection signal, not
    // leak-velocity).
    let leakVelocityState;
    if (fakeTrajectoryMap) {
      footprintTrajectory = Array.isArray(fakeTrajectoryMap[String(pid)]) ? fakeTrajectoryMap[String(pid)] : [];
    } else {
      const state = footprintStore ? (footprintStore[String(pid)] ?? null) : null;
      footprintTrajectory = state && Array.isArray(state.trajectory) ? state.trajectory : [];
      // pre-PR review round 2, Medium — mirrors `computeLeakBlocksForBeat`'s
      // own staleness guard (see that function's inline comment for the full
      // rationale): a footprint-state entry that has not been polled within
      // `DEFAULT_LIVENESS_THRESHOLD_MS` is the long-dead/pid-reused case —
      // without this guard, a live candidate pid whose footprint polling
      // stopped long ago (or a macOS-recycled pid inheriting a stale
      // trajectory from an unrelated earlier process) could still be evicted
      // on stale evidence, even though the advisory `leakBlocks` echo already
      // refuses to surface it. Prior to this fix, ONLY the advisory path had
      // this guard; the real eviction-wiring path had none at all.
      const isStale =
        typeof state?.lastPolledAt === 'number' && now - state.lastPolledAt > DEFAULT_LIVENESS_THRESHOLD_MS;
      if (state && !isStale) {
        leakVelocityState = buildLeakVelocityStateForCandidate(state);
      }
    }

    let worktreeMtimeMs = null;
    if (fakeMtimeMap) {
      const value = fakeMtimeMap[String(pid)];
      worktreeMtimeMs = typeof value === 'number' && Number.isFinite(value) ? value : null;
    } else {
      try {
        worktreeMtimeMs = await resolveRealWorktreeMtimeMs(pid, evictTestModeEnabled);
      } catch {
        // Defense in depth — the scanner's own contract says it never
        // throws, but one candidate's failure must never abort the whole
        // beat. Degrade to "unknown", others are unaffected.
        worktreeMtimeMs = null;
      }
    }

    candidates.push({ pid, footprintTrajectory, worktreeMtimeMs, leakVelocityState });
  }
  return candidates;
}

/**
 * Resolves this beat's own `claimLedger` (Phase 4's ONE trusted identifier
 * source): pairs each `candidates` entry with one of the calling
 * orchestrator's OWN leased queue items, in order — see this function's own
 * "CANDIDATE/LEDGER RESOLUTION" header note above for the documented 1:1
 * simplification this phase accepts.
 *
 * @param {{ orchestratorId: string, orchestratorPid: number, candidates: Array<{ pid: number }>, queueFilePath: string, now: number }} params
 * @returns {Promise<Array<{ pid: number, leaseId: string, orchestratorPid: number, orchestratorId: string, reentrant: boolean }>>}
 */
async function buildWatchdogClaimLedger({ orchestratorId, orchestratorPid, candidates, queueFilePath, now }) {
  const items = await peekQueue(queueFilePath, now).catch(() => []);
  const leasedItems = items.filter(
    (item) => item && item.orchestratorId === orchestratorId && typeof item.leaseId === 'string',
  );

  const pairCount = Math.min(candidates.length, leasedItems.length);
  const claimLedger = [];
  for (let i = 0; i < pairCount; i += 1) {
    const candidate = candidates[i];
    const item = leasedItems[i];
    claimLedger.push({
      pid: candidate.pid,
      leaseId: item.leaseId,
      orchestratorPid,
      orchestratorId,
      reentrant: isReentrantCommand(item.commandRef),
    });
  }
  return claimLedger;
}

/**
 * Derives the flapping-cooldown `recentlyEvictedPids` set (Phase 4's own
 * `resolveEvictionTargets` hook — pre-PR review, Medium: this was
 * previously wired nowhere in the real `--watchdog-beat` path, even though
 * it was unit-tested at Phase 4, so a target whose eviction FAILS — e.g.
 * `permission-denied` — got re-SIGTERM'd on every subsequent beat with no
 * backoff at all) from the liveness log's own recent history, rather than a
 * separate persisted store: any `watchdog-trip`/`deadlock-recycle` entry
 * within the cooldown window whose `outcome` is none of `'alert-only'`,
 * `'skip'`, or `'leak-velocity-throttled'` (pre-PR review round 2, Low —
 * this list previously omitted `'leak-velocity-throttled'`, stale against
 * this function's own `NON_ATTEMPT_OUTCOMES` set below, which already
 * includes it) represents a REAL evict attempt this beat (or a prior one)
 * already made against that pid — successful or not. A failed attempt is
 * therefore held in cooldown the same as a successful one.
 *
 * Fails open (returns an empty Set) on any read failure — a liveness log
 * this beat cannot read degrades to "no eviction history yet", never to a
 * reason to refuse the whole beat.
 *
 * Phase 3: reads via `readLivenessLogTail` (a bounded, backward/tail
 * walk) rather than `readLivenessLog` (an eager, whole-file, oldest-first
 * parse that throws on the FIRST bad line anywhere in the file). The tail
 * walk stops at whichever of its two independent conditions is hit first:
 * the first unparseable line (corruption in old, unrelated history no
 * longer costs this function every genuinely-recent entry), or — since
 * `now`/`cooldownMs` are threaded through below — the first well-formed
 * entry older than `now - cooldownMs` (Phase 2 pre-PR review, Medium:
 * this genuinely bounds per-beat parse cost to the cooldown window rather
 * than to total log size, which merely stopping on corruption alone does
 * not).
 *
 * @param {{ liveLogFilePath: string, now: number, cooldownMs: number }} params
 * @returns {Promise<Set<number>>}
 */
async function resolveRecentlyEvictedPids({ liveLogFilePath, now, cooldownMs }) {
  // Phase 4 pre-PR review, High 1 — `'leak-velocity-throttled'`
  // (watchdog-beat.mjs's Step 5b, DEFAULT_MAX_LEAK_VELOCITY_EVICTIONS_PER_BEAT
  // cap) means, by construction, NO real evict attempt was made this beat —
  // the pid was deliberately excluded from `targets`/`filterEvictable`
  // entirely and only logged for visibility/tuning. Without listing it here
  // alongside `'alert-only'`/`'skip'`, a throttled pid would be wrongly
  // classified as a real evict attempt and enter `recentlyEvictedPids`,
  // suppressing it from eviction for the FULL cooldown window on every
  // subsequent beat even though it was never actually evicted — silently
  // converting a documented "deferred this beat, visible for tuning" outcome
  // into a much longer de-facto immunity than the throttle itself intends.
  const NON_ATTEMPT_OUTCOMES = new Set(['alert-only', 'skip', 'leak-velocity-throttled']);
  let entries;
  try {
    entries = await readLivenessLogTail(liveLogFilePath, { now, cooldownMs });
  } catch {
    return new Set();
  }
  if (!Array.isArray(entries)) return new Set();

  const recentlyEvicted = new Set();
  for (const entry of entries) {
    if (!entry || (entry.detectionType !== 'watchdog-trip' && entry.detectionType !== 'deadlock-recycle')) continue;
    if (NON_ATTEMPT_OUTCOMES.has(entry.outcome)) continue;
    if (typeof entry.timestamp !== 'number' || entry.timestamp > now || now - entry.timestamp > cooldownMs) continue;
    const pid = Number(entry.pid);
    if (Number.isFinite(pid)) recentlyEvicted.add(pid);
  }
  return recentlyEvicted;
}

/**
 * Best-effort `getFootprintTrajectory`/`getWorktreeMtimeMs` resolvers for
 * Phase 3's deadlock-tripwire check (keyed by `orchestratorId`, fleet-wide —
 * NOT the same key space as `candidates`, which is keyed by pid). This beat
 * can only ever resolve signal data for pids it itself proved ownership of
 * above; any OTHER orchestrator's `orchestratorId` degrades to "no
 * evidence" (`[]`/`null`) — the same safe default `detectDeadlock` already
 * applies when no resolver is supplied at all. A future phase wiring a real,
 * fleet-wide signal source would replace this, not this beat's own
 * `candidates`-keyed data.
 *
 * @param {{ claimLedger: Array<{ orchestratorId: string, pid: number }>, candidates: Array<{ pid: number, footprintTrajectory: number[], worktreeMtimeMs: number|null }> }} params
 */
function buildFleetSignalResolvers({ claimLedger, candidates }) {
  const candidateByPid = new Map(candidates.map((candidate) => [candidate.pid, candidate]));
  const pidByOrchestratorId = new Map(claimLedger.map((record) => [record.orchestratorId, record.pid]));

  return {
    getFootprintTrajectory(orchestratorId) {
      const pid = pidByOrchestratorId.get(orchestratorId);
      const candidate = pid !== undefined ? candidateByPid.get(pid) : undefined;
      return candidate ? candidate.footprintTrajectory : [];
    },
    getWorktreeMtimeMs(orchestratorId) {
      const pid = pidByOrchestratorId.get(orchestratorId);
      const candidate = pid !== undefined ? candidateByPid.get(pid) : undefined;
      return candidate ? candidate.worktreeMtimeMs : null;
    },
  };
}

async function handleWatchdogBeat(flags) {
  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write('agent-resource-management: --orchestrator-id=<id> is required for --watchdog-beat\n');
    process.exit(2);
    return;
  }
  if (isReservedEntry(orchestratorId)) {
    process.stderr.write(
      `agent-resource-management: --orchestrator-id must not match the reserved double-underscore convention ` +
        `("__...__"), got "${orchestratorId}" — reserved ids collide with internal coordination-file bookkeeping ` +
        '(e.g. the legacy-unattributed-claim sentinel) and are not valid orchestrator ids\n',
    );
    process.exit(2);
    return;
  }

  // Same positive-integer format validation `--evict-pid`'s own
  // `--orchestrator-pid` gets — this value feeds the SAME ps-tree ancestry
  // walk (via `buildWatchdogCandidates`/`isEvictionAuthorized`), so a
  // malformed value should fail loudly here.
  const PID_PATTERN = /^\d+$/;
  const orchestratorPidRaw = flags['orchestrator-pid'];
  if (
    typeof orchestratorPidRaw !== 'string' ||
    orchestratorPidRaw.length === 0 ||
    !PID_PATTERN.test(orchestratorPidRaw) ||
    Number(orchestratorPidRaw) <= 0
  ) {
    const received = typeof orchestratorPidRaw === 'string' ? orchestratorPidRaw : String(orchestratorPidRaw);
    process.stderr.write(
      `agent-resource-management: --orchestrator-pid=<pid> requires a positive integer pid, got "${received}"\n`,
    );
    process.exit(2);
    return;
  }
  const orchestratorPid = Number(orchestratorPidRaw);

  const parsePositiveMs = (raw, fallback) => {
    if (typeof raw !== 'string' || raw.length === 0) return fallback;
    const parsed = Number(raw);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
  };
  const boundMs = parsePositiveMs(flags['watchdog-bound-ms'], DEFAULT_WATCHDOG_BOUND_MS);
  const deadlockWindowMs = parsePositiveMs(flags['watchdog-deadlock-window-ms'], DEFAULT_DEADLOCK_WINDOW_MS);
  const agingBoundMs = parsePositiveMs(flags['watchdog-aging-bound-ms'], DEFAULT_ESCALATION_BOUND_MS);

  const evictTestModeEnabled = process.env.ARM_EVICT_TEST_MODE === '1';
  const now = resolveEvictNow(evictTestModeEnabled);

  const queueFilePath = resolveQueueFilePath();
  const coordinationFilePath = resolveCoordinationFilePath();
  const deadlockStateFilePath = resolveDeadlockStateFilePath(coordinationFilePath);
  const liveLogFilePath = resolveLivenessLogFilePath();
  const backoffFilePath = resolveWatchdogBackoffFilePath(coordinationFilePath);
  const footprintStateFilePath = resolveFootprintStateFilePath();

  await ensureQueueDir(queueFilePath);
  await ensureCoordinationDir(coordinationFilePath);
  await ensureStateFileDir(deadlockStateFilePath);
  await ensureStateFileDir(liveLogFilePath);
  await ensureStateFileDir(backoffFilePath);

  // Shared pacing gate (Phase 6's own backoff wrapper AROUND
  // `runWatchdogBeat`, per ./lib/watchdog-backoff.jest.spec.mjs's own header
  // note): a beat that isn't due yet is a genuine no-op, distinct from "ran
  // and found nothing" — it never touches the queue/coordination/liveness
  // files at all.
  const previousBackoffState = await readBackoffState(backoffFilePath);
  const due = isBackoffDue({
    lastFullRun: previousBackoffState.lastFullRun,
    turnsToWait: previousBackoffState.turnsToWait,
    now,
    cronIntervalMs: DEFAULT_WATCHDOG_CRON_INTERVAL_MS,
  });
  if (!due) {
    process.stdout.write(JSON.stringify({ type: 'watchdog', skipped: true, reason: 'backoff-not-due' }) + '\n');
    return;
  }

  const candidates = await buildWatchdogCandidates({ orchestratorPid, now, evictTestModeEnabled, footprintStateFilePath });
  const claimLedger = await buildWatchdogClaimLedger({ orchestratorId, orchestratorPid, candidates, queueFilePath, now });
  const { getFootprintTrajectory, getWorktreeMtimeMs } = buildFleetSignalResolvers({ claimLedger, candidates });
  const recentlyEvictedPids = await resolveRecentlyEvictedPids({
    liveLogFilePath,
    now,
    cooldownMs: DEFAULT_WATCHDOG_EVICT_COOLDOWN_MS,
  });

  // No historical capacity-sample source is wired into this beat yet —
  // Phase 2's own `--dequeue-if-capacity` outcomes are the documented
  // interim source (see SKILL.md's watchdog section); wiring that up is out
  // of scope for this phase, so `escalateAgedQueueEntry`'s own
  // capacity-free-throughout check always fails closed (no eligible
  // samples) until a future phase supplies real samples here.
  const capacitySamples = [];

  // Drives each evict target through the real, already-shipped
  // `--evict-pid` eviction CORE in-process — `performEviction` (defined
  // directly above `handleWatchdogBeat`, immediately after `handleEvict`)
  // is the exact same function `handleEvict` itself calls, so this beat's
  // own recycle attempts get byte-for-byte the same eviction behavior
  // without cli.mjs ever spawning a child process of its own accord (this
  // file's own header comment / SKILL.md document that as a hard
  // invariant — see the "no new runProbe/execFileSync/spawnSync call site"
  // structural regression test in `cli-aimd-ceiling.jest.spec.mjs`).
  // review, Medium — `performEviction` never THROWS for an ordinary
  // eviction-outcome-classification result (every one of those — lease not
  // found, not-authorized, graceful-stop-failed, unconfirmed-after-kill,
  // etc. — is returned normally as `{ body, exitCode }`, per its own header
  // comment). Anything it DOES throw (`EvictFakeSeamCouplingError` — a
  // genuine misconfiguration — or an unexpected I/O error, e.g. a
  // `peekQueue`/coordination-file read failure) is therefore, by
  // construction, always a genuinely-unexpected failure, never a "this
  // particular target's eviction didn't succeed" outcome. `--evict-pid`'s
  // own `handleEvict` treats exactly these errors as FATAL
  // (`process.stderr.write` + `process.exit(2)`) rather than downgrading
  // them into a soft body. This beat must not silently swallow the same
  // errors into a fabricated `reason: 'unknown'` outcome as if the target
  // were merely "still running" — that would hide a real misconfiguration
  // or infra failure inside what looks like an ordinary trip result. So this
  // wrapper does not catch at all: it lets the error propagate up to
  // `runWatchdogBeat`'s own caller below, whose surrounding try/catch
  // already fails the whole beat loudly (stderr + exit 2), distinctly from
  // any ordinary per-target outcome.
  // pre-PR review, Medium — `performEviction` used to take its own
  // fresh `takeSample(collect)` machine probe (`vm_stat`/`ps`/`df`) on EVERY
  // call, which was fine for `--evict-pid`'s once-per-invocation use but
  // meant N evict targets in a single beat triggered N full probe rounds.
  // Hoisted here to once PER BEAT: sampled lazily (only if this beat actually
  // has at least one evict target — a no-op beat still does zero probing,
  // same as before) and cached across every subsequent target in the same
  // beat's loop via `runWatchdogBeat`'s own evict-target iteration.
  let cachedSamplePromise = null;
  const getPreTakenSample = () => {
    if (!cachedSamplePromise) {
      cachedSamplePromise = takeSample(collect).then((sample) => {
        if (totalCollectFailure) {
          throw totalCollectFailure;
        }
        return sample;
      });
    }
    return cachedSamplePromise;
  };

  // review, Critical — `performEviction`'s own header comment (see
  // above, immediately before `handleWatchdogBeat`) already documents that
  // it NEVER throws for an ordinary per-target eviction outcome: every one
  // of those (lease not found, not-authorized, graceful-stop-failed,
  // unconfirmed-after-kill, etc.) comes back as a normal `{ body, exitCode }`
  // return value, never a rejection. Anything this function (or
  // `getPreTakenSample` above, which rethrows a genuine total
  // `totalCollectFailure` machine-probe failure — see `collect()`'s own
  // comment) DOES throw is therefore, by construction, always
  // genuinely-unexpected — never a shape `runWatchdogBeat`'s
  // `isSystemicEvictFailure` marker check should isolate per-target. Mark it
  // here, generically, at this call site, so every such rejection (not just
  // the ones `cli.mjs` already tags at their own throw site, e.g.
  // `EvictFakeSeamCouplingError`) propagates and fails the whole beat loudly
  // instead of being silently swallowed into a fabricated `reason: 'unknown'`
  // outcome for every evict target in the beat.
  //
  // — the eviction beat's ledger-identity resolution
  // (`resolveNearestClaudeRootIdentity`), hoisted here to once PER BEAT for
  // the exact same reason `getPreTakenSample` above hoists the machine-
  // resource probe: `performEviction` used to re-resolve this identity on
  // EVERY evict target in a single beat, even though it is the same beat's
  // own identity regardless of which target is being processed. Unlike
  // `takeSample`, `resolveNearestClaudeRootIdentity` is SYNCHRONOUS, so this
  // is a plain lazy value cache rather than a cached promise.
  let cachedEvictBeatIdentity;
  const getEvictBeatIdentity = () => {
    if (cachedEvictBeatIdentity === undefined) {
      cachedEvictBeatIdentity = resolveNearestClaudeRootIdentity(
        collectPsOutput(evictTestModeEnabled),
        process.ppid,
        resolveProcessSnapshotNow(evictTestModeEnabled),
      );
    }
    return cachedEvictBeatIdentity;
  };

  const handleEvictInProcess = async (target) => {
    try {
      const preTakenSample = await getPreTakenSample();
      const result = await performEviction({
        orchestratorId,
        orchestratorPidRaw: String(orchestratorPid),
        evictPidRaw: String(target.pid),
        leaseId: target.leaseId,
        evictTestModeEnabled,
        preTakenSample,
        cachedEvictBeatIdentity: getEvictBeatIdentity(),
      });
      return result.body;
    } catch (error) {
      if (error && error.armFailBeat !== true) {
        error.armFailBeat = true;
      }
      throw error;
    }
  };

  let beatResult;
  try {
    beatResult = await runWatchdogBeat({
      now,
      boundMs,
      deadlockWindowMs,
      agingBoundMs,
      candidates,
      claimLedger,
      // pre-PR review round 2, Medium — this beat's own orchestratorId,
      // threaded through as `runWatchdogBeat`'s fallback for a throttled
      // leak-velocity trip with no `claimLedger` entry (see that function's
      // own inline comment for the reachable configuration this closes).
      orchestratorId,
      recentlyEvictedPids,
      coordinationFilePath,
      queueFilePath,
      deadlockStateFilePath,
      liveLogFilePath,
      capacitySamples,
      getFootprintTrajectory,
      getWorktreeMtimeMs,
      handleEvict: handleEvictInProcess,
    });
  } catch (error) {
    process.stderr.write(`agent-resource-management: --watchdog-beat failed — ${error.message}\n`);
    process.exit(2);
    return;
  }

  // Backoff-state write happens ONLY after `runWatchdogBeat` has resolved
  // successfully — never inside a try/finally that would also run on the
  // throw path above (see ./lib/watchdog-backoff.jest.spec.mjs's own header
  // for why this exact sequencing is the load-bearing half of "a beat that
  // throws mid-run must not corrupt the backoff-state file" that lives in
  // THIS file, not in watchdog-backoff.mjs itself).
  const didWork = beatResult.trips.length > 0 || beatResult.deadlockTripped || beatResult.agingEscalations.length > 0;
  const nextBackoffState = computeNextBackoffState({
    didWork,
    now,
    previousTurnsToWait: previousBackoffState.turnsToWait,
  });
  try {
    await writeBackoffStateAtomic(backoffFilePath, nextBackoffState);
  } catch (error) {
    warnCoordinationDegraded(
      error,
      'watchdog backoff-state file',
      "proceeding without persisting this beat's backoff decision",
    );
  }

  process.stdout.write(JSON.stringify(beatResult) + '\n');
}

// ---------------------------------------------------------------------------
// main()
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Phase 3 — the disk axis's AMBER cleanup response.
//
// Three things gate the only recursively-deleting code path in this skill, and
// each closes a different failure the earlier rounds of this ticket surfaced:
//
//   1. EVIDENCE (`diskEvidenceWarrantsSweep`) — a real, persisted, two-sample
//      decline, or a genuinely low free-space reading. NOT "the disk axis said
//      AMBER": one of `classifyDiskAxis`'s three AMBER routes is "the sample
//      is stale/unknown", and deleting directories because the sampler could
//      not read the disk is action on the absence of evidence.
//   2. OPT-IN (`diskCleanupArmedForBeat`) — an explicit, separate
//      coordination-file row, described below.
//   3. `--advise-only` — checked FIRST at the call site, unconditionally.
//
// Everything about the sweep itself (confinement, budget, candidate cap,
// liveness probing, the never-throws contract) belongs to Phase 2's
// `sweepStaleTempDirsAsync`, not here. This file supplies the trigger, the
// root, and the clock — nothing more.
// ---------------------------------------------------------------------------

/** Local to the trigger; `lib/cleanup.mjs` states the horizon in hours. */
const CLEANUP_HOUR_MS = 60 * 60 * 1000;

/** Test-only ARMING flag (pinned by `cleanup-auto-trigger-outer-acceptance.jest.spec.mjs`). NOT a sweep root — see `diskCleanupArmedForBeat`. */
const TEST_SWEEP_ARM_ENV = 'ARM_TEST_ALLOW_TEMP_SWEEP';

/**
 * PURE. Does this beat's disk reading constitute EVIDENCE that warrants a
 * destructive sweep?
 *
 * Two routes, both evidence-bearing, deliberately expressed against the trend
 * and the raw free-space figure rather than against `classifyDiskAxis`'s
 * `reason` STRING (which would couple this gate to prose):
 *
 *   * `declining` — two real persisted samples, seconds apart, showing free
 *     space actually falling. This fires even while the axis is still GREEN:
 *     the cheapest moment to reclaim a stale `cdk.out-*` is before the axis
 *     degrades, and the sweep is additive, never an admission influence.
 *   * `stable` AND at-or-below the GREEN free-space floor — the low-free-space
 *     AMBER route. `stable` is a real, computed verdict from two samples, so
 *     this is evidence too.
 *
 * Everything else refuses: `insufficient-data` (a first-ever beat has no
 * evidence at all, even when its instantaneous reading sits in the AMBER
 * band), `recovering` (free space is going UP), and any non-finite
 * `freeDiskGb` (the stale/unknown AMBER route — the sampler failed, which is
 * not a fact about the disk).
 *
 * @param {{ trend?: string } | undefined} diskTrend
 * @param {number | null | undefined} freeDiskGb
 * @returns {boolean}
 */
function diskEvidenceWarrantsSweep(diskTrend, freeDiskGb) {
  if (diskTrend?.trend === 'declining') return true;
  if (diskTrend?.trend !== 'stable') return false;
  if (typeof freeDiskGb !== 'number' || !Number.isFinite(freeDiskGb)) return false;
  const greenFloor = DEFAULT_DISK_THRESHOLDS.greenFreeDiskGbAbove;
  return Number.isFinite(greenFloor) && freeDiskGb <= greenFloor;
}

/**
 * Is THIS beat's orchestrator armed for the sweep?
 *
 * PRODUCTION: the dedicated `__disk-cleanup-armed__` coordination-file row,
 * written only by `--enable-disk-cleanup`. It is deliberately NOT
 * `hasEverOptedIn`: `declareDibs` unions the beat's own id into the
 * ever-opted-in set as a side effect of declaring capacity, and it does so
 * earlier in this very function — so gating on that set would mean every beat
 * that declares dibs is armed, including the unattended launchd watchdog beat.
 * "Opt-in, default-inert" would then be "default-armed". See
 * `DISK_CLEANUP_ARMED_RESERVED_ID` in `lib/coordination-file.mjs`.
 *
 * TEST MODE (`ARM_COORDINATION_FILE` set — this directory's own established
 * test-mode marker) reads THE SAME PRODUCTION GATE, and adds ONE EXTRA
 * REFUSAL on top of it. It is a strict AND, never an alternative gate:
 *
 *     ARM_TEST_ALLOW_TEMP_SWEEP === '1'  AND  hasDiskCleanupArmed(...)
 *
 * Phase 3 review (High) — this branch previously read `hasEverOptedIn`
 * INSTEAD of the real gate when both env vars were present. That reintroduced
 * the exact defect the `__disk-cleanup-armed__` row was created to fix
 * (`declareDibs` unions the beat's own id into the ever-opted-in set, so
 * "opt-in" silently meant "default-armed"), and the sweep root is the REAL
 * `os.tmpdir()` — only the coordination-file LOOKUP is test-scoped, never the
 * deletion target. So an env pair leaking into a real environment would have
 * armed every dibs-declaring orchestrator against the host's own `$TMPDIR`.
 * An OR-with-a-broader-set must never be reintroduced here.
 *
 * What survives is only the NARROWING half, which is genuinely test-only work
 * the removed branch also did: the belt-and-braces refusal that keeps
 * `npm run test:skills` from touching the host's real `$TMPDIR` if some future
 * spec forgets the per-child `TMPDIR` idiom
 * (`cleanup-auto-trigger-outer-acceptance.jest.spec.mjs`, condition 3,
 * "suppression 2, the suite-safety net"). Because it can only ever subtract
 * permission, the worst case if that flag leaked into production alongside
 * `ARM_COORDINATION_FILE` is a sweep that refuses — never one that fires.
 *
 * Never throws: the read is lock-free and fails closed to `false`.
 *
 * The beat clock is threaded through because the armed row now carries a
 * 30-day read-time horizon (pre-PR review, High #2). `resolveNow()` is
 * the right clock for it — unlike the sweep's own `now`, which must be the
 * real `Date.now()` because it is compared against real file mtimes, this
 * compares against a `lastSeenAt` the coordination file's own writers stamped
 * with exactly this clock.
 *
 * @param {string} coordinationFilePath
 * @param {string} orchestratorId
 * @param {number} now Beat clock, i.e. `resolveNow()`.
 * @returns {Promise<boolean>}
 */
async function diskCleanupArmedForBeat(coordinationFilePath, orchestratorId, now) {
  try {
    if (process.env.ARM_COORDINATION_FILE && process.env[TEST_SWEEP_ARM_ENV] !== '1') return false;
    return await hasDiskCleanupArmed(coordinationFilePath, orchestratorId, { now });
  } catch {
    // Both readers already fail closed internally; this is the belt-and-braces
    // that keeps a future edit to either of them from turning "cannot tell
    // whether we are armed" into a crashed beat. Unknown means NOT armed.
    return false;
  }
}

/**
 * How long the cooldown write will wait for the coordination lock before
 * giving up (Phase 3 review round 2 — flagged by both reviewers).
 *
 * The same pattern, and the same reasoning, as
 * `DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS` above: a caller on a bounded time budget
 * passes its own tighter ceiling rather than inheriting `acquireLock`'s
 * default (2000 attempts, up to ~10-20s of retry sleep alone). Left at the
 * default, a contended cooldown write could stall the beat well past the
 * PreToolUse hook's `CLI_TIMEOUT_MS` (5s) — and that hook fails CLOSED on
 * timeout, denying EVERY agent spawn on the host.
 *
 * It is sized far tighter than `--claim`'s 800 because the two writes are
 * worth wildly different amounts. A `--claim` that loses its lock denies a
 * grant, so it is worth waiting seconds for. This write is a pure
 * OPTIMISATION — its failure costs one wasted 750ms sweep on a later beat and
 * nothing else, which is why `maybeRecordSweepCooldown` below already swallows
 * the error. A discardable write must not be able to consume more than a
 * token slice of the beat's budget: 40 attempts caps the retry sleep at
 * ~200ms (`LOCK_RETRY_DELAY_MS` is 5ms), i.e. ~4% of `CLI_TIMEOUT_MS`.
 *
 * Failing here is therefore not a degradation to mitigate but the correct
 * outcome: under lock contention there are other beats running, one of which
 * will record its own cooldown shortly.
 */
const DEFAULT_COOLDOWN_LOCK_MAX_ATTEMPTS = 40;

/**
 * Persists the cooldown after a futile-but-complete sweep, and does nothing at
 * all otherwise (Phase 3 review, Medium).
 *
 * NEVER THROWS, and never lets a cooldown failure change what the beat
 * reports. The sweep has already happened by the time this runs; failing to
 * write its cooldown costs one wasted sweep on a later beat, whereas letting
 * the write's error escape would take down a beat whose JSON the PreToolUse
 * hook fails CLOSED on. A degraded write is therefore swallowed on purpose —
 * unlike the sweep's own errors, which are REPORTED, because those may
 * describe an irreversible deletion and this cannot.
 *
 * It is bounded in TIME as well as in consequence: see
 * `DEFAULT_COOLDOWN_LOCK_MAX_ATTEMPTS`. Swallowing a failure is only half the
 * guarantee — a write that cannot fail the beat but CAN stall it for ten
 * seconds is still fatal to a hook that fails closed at five.
 *
 * @param {string} coordinationFilePath
 * @param {number} now Beat clock, matching what `isDiskCleanupSuppressed` reads.
 * @param {object} report
 * @returns {Promise<void>}
 */
async function maybeRecordSweepCooldown(coordinationFilePath, now, report) {
  if (!sweepFoundNothingToDo(report)) return;
  if (!Number.isFinite(now)) return;

  try {
    await recordDiskCleanupCooldown(
      coordinationFilePath,
      { suppressedUntil: now + SWEEP_COOLDOWN_MS },
      { now },
      { lockMaxAttempts: DEFAULT_COOLDOWN_LOCK_MAX_ATTEMPTS },
    );
  } catch {
    // See above: a cooldown that could not be written is a missed
    // optimisation, never a correctness or safety problem.
  }
}

/**
 * Runs the budgeted sweep when — and only when — all three gates pass, and
 * returns the report to attach as the beat's additive `cleanup` field.
 * `undefined` means "the trigger did not fire", which is why the field is
 * spread conditionally at all three emission sites: a PRESENT `cleanup` with
 * `removedCount: 0` means "fired, found nothing", and those two must stay
 * distinguishable to any orchestrator reading the JSON.
 *
 * TOTAL. `sweepStaleTempDirsAsync` contracts never to throw and never to write
 * to stdout, and `diskCleanupArmedForBeat` fails closed — but this is on the beat
 * path, whose single JSON document the PreToolUse hook parses and fails CLOSED
 * on, so a belt-and-braces `catch` is cheap insurance against a future edit
 * making one of them throwable. A sweep that throws degrades to a PRESENT
 * report carrying `kind: 'sweep-threw'` (Phase 3 review, Medium — it
 * used to degrade to "no cleanup key", which claimed the trigger had never
 * fired even when removals had already been issued), never to a crashed beat.
 *
 * `diskCleanupArmed` is passed in as an ALREADY-RESOLVED boolean rather than
 * being read here, because WHEN it is read is part of the gate: it must be
 * snapshotted before `declareDibs` — see that read's own comment.
 *
 * COOLDOWN (Phase 3 review, Medium). The three gates above are all
 * memoryless, so a host parked stably just below the GREEN free-space floor
 * with an already-clean `$TMPDIR` satisfies them on EVERY beat forever, paying
 * the full `SWEEP_BUDGET_MS` each time to reclaim nothing. After a sweep that
 * ran to completion and found nothing worth doing, this function stands the
 * trigger down for `SWEEP_COOLDOWN_MS`. A sweep that removed something, or
 * that hit its budget or candidate cap, writes no cooldown at all — the last
 * two mean "there is more work queued, come back next beat".
 *
 * A suppressed beat returns `undefined`, which is correct rather than a fudge:
 * the trigger genuinely did not fire, and nothing was deleted.
 *
 * @param {{ coordinationFilePath: string, now: number,
 *   diskCleanupArmed: boolean, isAdviseOnly: boolean,
 *   diskTrend: object | undefined,
 *   freeDiskGb: number | null | undefined }} args
 * @returns {Promise<object | undefined>}
 */
async function maybeSweepStaleTempRoot({
  coordinationFilePath,
  now,
  diskCleanupArmed,
  isAdviseOnly,
  diskTrend,
  freeDiskGb,
}) {
  // FIRST, and unconditional. `--advise-only` is documented as a fully
  // non-mutating advisory read, and the PreToolUse hook's whole trust argument
  // rests on that; a recursive removal would be the most mutating action in
  // this file.
  if (isAdviseOnly) return undefined;
  if (!diskCleanupArmed) return undefined;
  if (!diskEvidenceWarrantsSweep(diskTrend, freeDiskGb)) return undefined;
  // LAST of the refusals, deliberately: it is the only one that costs a file
  // read, so it sits behind the three free ones. It also fails OPEN — see
  // `isDiskCleanupSuppressed` — so a corrupt byte can never be the reason a
  // filling host stops reclaiming.
  if (await isDiskCleanupSuppressed(coordinationFilePath, now, SWEEP_COOLDOWN_MS)) return undefined;

  // RESOLVED ONCE, BEFORE the try (Phase 3 review round 2, Low). Both
  // the success path and the `catch` arm below need the sweep root, and the
  // `catch` arm's entire job is to be TOTAL. A second `tmpdir()` call inside
  // it would be the one expression in this function that could throw out of
  // the handler meant to contain throws — producing exactly the raw stack
  // trace on the beat's stdout that every other guard here exists to prevent.
  // `os.tmpdir()` does not throw on any supported platform; that is a reason
  // to hoist it, not a reason to trust it from inside the catch.
  //
  // The sweep root derives ONLY from the platform temp root, never from argv,
  // the coordination file, or an `ARM_*` override. Phase 2's anchor re-checks
  // confinement against the platform allowlist and refuses anything outside it,
  // which is what makes a hostile or mistaken `TMPDIR` (including a
  // repo-relative one) a refusal rather than a deletion.
  const sweepRoot = tmpdir();

  try {
    const report = await sweepStaleTempDirsAsync({
      dir: sweepRoot,
      ageThresholdMs: AUTO_TRIGGER_AGE_HOURS * CLEANUP_HOUR_MS,
      // REAL `Date.now()`, DELIBERATELY NOT `resolveNow()`. Do not "fix" this
      // to use the beat's clock. `resolveNow()` honours `ARM_FAKE_NOW_MS`, so
      // a spec that advances a simulated clock between beats would hand the
      // sweep a `now` minutes away from the host clock — and Phase 2's
      // `SWEEP_NOW_SKEW_TOLERANCE_MS` band would refuse the whole sweep with
      // `now-out-of-band`, silently. More fundamentally: staleness and
      // liveness here are compared against real file mtimes on a real
      // filesystem, so the only correct clock for those comparisons is the
      // real one, whatever the rest of the beat is simulating.
      now: Date.now(),
    });

    await maybeRecordSweepCooldown(coordinationFilePath, now, report);
    return report;
  } catch (error) {
    // Phase 3 review (Medium) — NOT `undefined`. `undefined` is this
    // function's word for "the trigger did not fire", and the very scenario
    // this `catch` exists for is a throw from PARTWAY THROUGH a sweep that has
    // ALREADY ISSUED REMOVALS. Returning `undefined` there would report an
    // irreversible deletion as if nothing had happened at all.
    //
    // A present, shape-complete report with `kind: 'sweep-threw'` on its
    // errors ring keeps the docstring's invariant intact in every arm:
    // absent `cleanup` = never fired; present `cleanup` = fired, and the
    // payload says how it went. `sweepThrewReport` is itself total.
    return sweepThrewReport(sweepRoot, error);
  }
}

/**
 * `--enable-disk-cleanup` — the ONLY writer of the
 * `__disk-cleanup-armed__` row, and a standalone beat that does nothing else.
 *
 * It exists so that arming the skill's only recursively-deleting code path is
 * always a separate, deliberate act. Folding it into an existing beat — even a
 * deliberate one like an opt-in — would make arming a side effect of something
 * else, which is exactly the defect this row was introduced to fix.
 *
 * Emits one JSON document and exits 0, mirroring every other standalone beat.
 * `armed: true` means "newly armed by this call"; `false` means it was already
 * armed (idempotent — the call still refreshes the row's `lastSeenAt`).
 *
 * @param {Record<string, unknown>} flags
 * @returns {Promise<void>}
 */
async function handleEnableDiskCleanup(flags) {
  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write(
      'agent-resource-management: --enable-disk-cleanup requires --orchestrator-id=<id> — the armed set is ' +
        'per-orchestrator, so there is no meaningful id-less arming\n',
    );
    process.exit(2);
    return;
  }
  if (isReservedEntry(orchestratorId)) {
    process.stderr.write(
      'agent-resource-management: --orchestrator-id must not be a reserved id ' +
        `("__...__"), got "${orchestratorId}"\n`,
    );
    process.exit(2);
    return;
  }

  try {
    const armed = await recordDiskCleanupArmed(resolveCoordinationFilePath(), orchestratorId, { now: resolveNow() });
    process.stdout.write(JSON.stringify({ type: 'disk-cleanup-armed', orchestratorId, armed }) + '\n');
  } catch (error) {
    // Fails CLOSED and LOUD: an orchestrator that believes it armed the sweep
    // when it did not would simply see no `cleanup` key later and wonder why.
    // Exit 1 (not 0) so a scripted opt-in cannot silently proceed unarmed.
    process.stderr.write(
      `agent-resource-management: --enable-disk-cleanup could not write the coordination file: ${error.message}\n`,
    );
    process.exit(1);
  }
}

/**
 * `--disable-disk-cleanup` (Phase 3 review, Medium) — the ONLY remover
 * of the `__disk-cleanup-armed__` row, and `--enable-disk-cleanup`'s exact
 * mirror: a standalone beat that does nothing else.
 *
 * It closes a one-way door. Before it, an armed orchestrator stayed armed
 * until `hasDiskCleanupArmed`'s 30-day read-time horizon expired it — an
 * ageing backstop, not a stand-down — refreshed by every later arming call,
 * and the only ways out
 * were hand-editing the shared coordination file (racing every concurrent
 * beat's read) or passing `--advise-only` forever (which suppresses far more
 * than the sweep). Arming the only recursively-deleting path in this skill is
 * a deliberate act; so is standing it back down.
 *
 * Emits one JSON document and exits 0. `disarmed: true` means "this call
 * removed an armed id"; `false` means it was already not armed — idempotent,
 * because an operator reaching for this wants a destructive capability
 * STOPPED, and must never read an error as "still armed".
 *
 * @param {Record<string, unknown>} flags
 * @returns {Promise<void>}
 */
async function handleDisableDiskCleanup(flags) {
  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write(
      'agent-resource-management: --disable-disk-cleanup requires --orchestrator-id=<id> — the armed set is ' +
        'per-orchestrator, so there is no meaningful id-less disarming\n',
    );
    process.exit(2);
    return;
  }
  if (isReservedEntry(orchestratorId)) {
    process.stderr.write(
      'agent-resource-management: --orchestrator-id must not be a reserved id ' +
        `("__...__"), got "${orchestratorId}"\n`,
    );
    process.exit(2);
    return;
  }

  try {
    const disarmed = await clearDiskCleanupArmed(resolveCoordinationFilePath(), orchestratorId, { now: resolveNow() });
    process.stdout.write(JSON.stringify({ type: 'disk-cleanup-disarmed', orchestratorId, disarmed }) + '\n');
  } catch (error) {
    // Fails LOUD for the inverse of `--enable-disk-cleanup`'s reason: an
    // operator who believes they disarmed the sweep, and did not, is left with
    // a recursively-deleting capability still live on the host. Exit 1 so a
    // scripted stand-down cannot silently leave it armed.
    process.stderr.write(
      `agent-resource-management: --disable-disk-cleanup could not write the coordination file: ${error.message}\n`,
    );
    process.exit(1);
  }
}

async function main() {
  const flags = parseArgs(process.argv.slice(2));

  // Resolved before EVERY dispatch below, including `--watchdog-beat`'s
  // "checked absolute first" block: this is not a beat type, it is how the
  // `--orchestrator-id` every beat type already reads is obtained. Doing it
  // here — once, ahead of dispatch — is what keeps `deriveOrchestratorId`
  // called in exactly one place for producers. See
  // `resolveOrchestratorIdFlag`'s own header for why that is load-bearing.
  const resolvedOrchestratorId = resolveOrchestratorIdFlag(flags);
  if (!resolvedOrchestratorId.ok) {
    process.stderr.write(resolvedOrchestratorId.message);
    process.exit(2);
    return;
  }

  // Checked before EVERY beat-type dispatch below — including
  // `--watchdog-beat`'s own "checked absolute first" block — for the same
  // reason that block gives: none of the other beat-type blocks know about
  // `--advise-only`, so a check placed anywhere later is unreachable for
  // them.
  //
  // Phase 6 review (Medium) — `--advise-only`'s WHOLE REASON TO EXIST is
  // a non-mutation guarantee (see its full doc block on the
  // `--desired-agents` path below). Until this block existed, that guarantee
  // was enforced against `--heartbeat` ONLY: the validation and the single
  // heartbeat conflict check both live inside the `--desired-agents` handler,
  // which every OTHER beat-type flag dispatches ahead of. So
  // `--advise-only --record-outcome --operation-type=test --peak-memory-mb=100`
  // was silently accepted and ran the normal, MUTATING record-outcome handler
  // — it wrote to the history file — reported `{"recorded":true}`, and exited
  // 0, with `--advise-only` discarded entirely. Every other mutating beat type
  // had the same shape.
  //
  // The fix mirrors `--watchdog-beat`'s block verbatim: an explicit
  // rejection list naming every other beat-type flag, checked up front, so no
  // reciprocal entry is needed in any of their own (advise-only-unaware)
  // lists. `--heartbeat` stays on the list — this SUBSUMES the older,
  // narrower in-handler check, which is retained below purely as
  // defence-in-depth against a future reordering of this dispatch (it is
  // unreachable while this block runs first, and its doc paragraph is the
  // canonical statement of WHY that particular pair is rejected).
  // `--desired-agents` is deliberately ABSENT: it is the one beat type
  // `--advise-only` modifies rather than conflicts with.
  if (flags['advise-only'] !== undefined) {
    const ADVISE_ONLY_CONFLICTING_BEAT_FLAGS = [
      ['watchdog-beat', '--watchdog-beat'],
      ['evict-pid', '--evict-pid'],
      ['bind-pid', '--bind-pid'],
      ['enqueue', '--enqueue'],
      ['peek', '--peek'],
      ['dequeue', '--dequeue'],
      ['dequeue-if-capacity', '--dequeue-if-capacity'],
      ['ack-lease', '--ack-lease'],
      ['release-lease', '--release-lease'],
      ['release', '--release'],
      ['claim', '--claim'],
      ['query-capacity', '--query-capacity'],
      ['record-outcome', '--record-outcome'],
      ['poll-footprint', '--poll-footprint'],
      ['heartbeat', '--heartbeat'],
      // Phase 3 — reciprocal entries for the standalone arming and
      // disarming beats, per doc-honesty.jest.spec.mjs's
      // *_CONFLICTING_BEAT_FLAGS exhaustiveness gate.
      ['enable-disk-cleanup', '--enable-disk-cleanup'],
      ['disable-disk-cleanup', '--disable-disk-cleanup'],
    ];
    const conflictingEntry = ADVISE_ONLY_CONFLICTING_BEAT_FLAGS.find(([flagKey]) => flags[flagKey] !== undefined);
    const conflictingFlag = conflictingEntry?.[1];

    if (conflictingFlag) {
      process.stderr.write(
        `agent-resource-management: --advise-only cannot be combined with ${conflictingFlag} in the same ` +
          'invocation — --advise-only is a fully non-mutating advisory read of the --desired-agents ' +
          'spawn-decision beat, and every other beat type has its own mutating semantics that would ' +
          'silently run and write despite it\n',
      );
      process.exit(2);
      return;
    }
  }

  // Checked absolute FIRST — ahead of EVERY other beat-type
  // flag's own conflict check, including `--evict-pid`'s own block
  // immediately below: mirrors that block's own "runs first" rationale
  // verbatim (see its comment just below for the full explanation of why
  // this pattern is needed at all). Because this check runs before
  // `--evict-pid`'s own, `--watchdog-beat --evict-pid=...` is caught HERE
  // (via `'evict-pid'` in this block's own conflict list) — no reciprocal
  // entry is needed in `--evict-pid`'s own list for that specific pairing.
  if (flags['watchdog-beat'] !== undefined) {
    const WATCHDOG_CONFLICTING_BEAT_FLAGS = [
      ['evict-pid', '--evict-pid'],
      ['enqueue', '--enqueue'],
      ['peek', '--peek'],
      ['dequeue', '--dequeue'],
      ['claim', '--claim'],
      ['release', '--release'],
      ['query-capacity', '--query-capacity'],
      ['record-outcome', '--record-outcome'],
      ['heartbeat', '--heartbeat'],
      ['desired-agents', '--desired-agents'],
      ['dequeue-if-capacity', '--dequeue-if-capacity'],
      ['ack-lease', '--ack-lease'],
      ['release-lease', '--release-lease'],
      ['poll-footprint', '--poll-footprint'],
      // Phase 3, review (Medium) — `--bind-pid` was missing from this
      // list, so `--watchdog-beat --bind-pid=X --lease-id=Y` silently
      // dispatched into `handleWatchdogBeat` and discarded `--bind-pid`/
      // `--lease-id` entirely, instead of being rejected as a flag conflict —
      // this is the block that runs FIRST in `main()`, so it must know about
      // every other beat-type flag itself; it cannot rely on a reciprocal
      // entry in `--bind-pid`'s own (later-checked) block.
      ['bind-pid', '--bind-pid'],
      // Phase 3 — reciprocal entries for the standalone arming and
      // disarming beats, per doc-honesty.jest.spec.mjs's
      // *_CONFLICTING_BEAT_FLAGS exhaustiveness gate.
      ['enable-disk-cleanup', '--enable-disk-cleanup'],
      ['disable-disk-cleanup', '--disable-disk-cleanup'],
    ];
    const conflictingEntry = WATCHDOG_CONFLICTING_BEAT_FLAGS.find(([flagKey]) => flags[flagKey] !== undefined);
    const conflictingFlag = conflictingEntry?.[1];

    if (conflictingFlag) {
      process.stderr.write(
        `agent-resource-management: --watchdog-beat cannot be combined with ${conflictingFlag} in the same ` +
          'invocation — the watchdog beat is a standalone decision-only beat type, not a spawn-decision/' +
          'advisory-read beat\n',
      );
      process.exit(2);
      return;
    }

    await handleWatchdogBeat(flags);
    return;
  }

  // Phase 3 — `--enable-disk-cleanup`, the standalone arming beat.
  // Placed directly after `--watchdog-beat`'s block (which already lists it)
  // and ahead of every remaining beat-type dispatch, so this block's own list
  // plus the three earlier-running blocks' reciprocal entries give full
  // bidirectional coverage without touching the later nested-ternary blocks.
  if (flags['enable-disk-cleanup'] !== undefined) {
    // pre-PR review (High #1) — VALUE-VALIDATED, not merely present.
    // The dispatch test above is `!== undefined` because "was this flag
    // mentioned at all" is the right question for dispatch and for the
    // conflict arrays below. It is emphatically NOT the right question for
    // whether to ARM: `--enable-disk-cleanup=false` is the exact string an
    // operator types to turn the sweep OFF, and treating any value as "arm
    // it" turned that into an arming beat. Routed through the same
    // `validateBareOrTrueFlag` contract as `--advise-only`/`--heartbeat`/
    // `--crashed`/`--record-outcome`/`--user-attests-idle`, so `=false`,
    // `=off`, `=no`, `=0` and a swallowed positional all hard-reject with
    // exit 2 rather than silently arming a recursive deletion.
    const enableDiskCleanupValidation = validateBareOrTrueFlag(flags, 'enable-disk-cleanup');
    if (!enableDiskCleanupValidation.valid) {
      process.exit(2);
      return;
    }

    const ENABLE_DISK_CLEANUP_CONFLICTING_BEAT_FLAGS = [
      ['evict-pid', '--evict-pid'],
      ['bind-pid', '--bind-pid'],
      ['enqueue', '--enqueue'],
      ['peek', '--peek'],
      ['dequeue', '--dequeue'],
      ['dequeue-if-capacity', '--dequeue-if-capacity'],
      ['ack-lease', '--ack-lease'],
      ['release-lease', '--release-lease'],
      ['release', '--release'],
      ['claim', '--claim'],
      ['query-capacity', '--query-capacity'],
      ['record-outcome', '--record-outcome'],
      ['poll-footprint', '--poll-footprint'],
      ['heartbeat', '--heartbeat'],
      ['desired-agents', '--desired-agents'],
      // Reciprocal only — `--watchdog-beat`'s own block runs first and already
      // catches this pairing. Listed to keep the array exhaustive against every
      // top-level dispatch flag, per doc-honesty.jest.spec.mjs's structural gate.
      ['watchdog-beat', '--watchdog-beat'],
      // Phase 3 review (Medium) — arming and disarming in one invocation
      // is incoherent whichever order argv happens to put them in, and this is
      // the block that runs first of the two, so this is the entry that
      // actually catches the pair.
      ['disable-disk-cleanup', '--disable-disk-cleanup'],
    ];
    // KEPT ON ONE LINE DELIBERATELY, long though it is. Every sibling conflict
    // block above spells this line identically, and doc-honesty.jest.spec.mjs's
    // "entries are actually consulted by the conflict check (not merely
    // declared and ignored)" gate matches that exact single-line form — a
    // wrapped version declares the array and then silently fails the gate that
    // proves it is read. Do not reflow.
    const conflictingEntry = ENABLE_DISK_CLEANUP_CONFLICTING_BEAT_FLAGS.find(([flagKey]) => flags[flagKey] !== undefined);
    const conflictingFlag = conflictingEntry?.[1];

    if (conflictingFlag) {
      process.stderr.write(
        `agent-resource-management: --enable-disk-cleanup cannot be combined with ${conflictingFlag} in the ` +
          'same invocation — arming the disk-axis temp sweep is a standalone, deliberate act by design, and ' +
          'must never ride along on a beat that does something else\n',
      );
      process.exit(2);
      return;
    }

    await handleEnableDiskCleanup(flags);
    return;
  }

  // Phase 3 review (Medium) — `--disable-disk-cleanup`, the standalone
  // DISARMING beat, and `--enable-disk-cleanup`'s exact structural mirror.
  // Placed directly after it (which already lists this flag) for the same
  // reason that block sits where it does: ahead of every remaining beat-type
  // dispatch, so this block's own list plus the four earlier-running blocks'
  // reciprocal entries give full bidirectional coverage.
  if (flags['disable-disk-cleanup'] !== undefined) {
    // Value-validated for the same reason as its arming mirror above, even
    // though this direction is the safe one: `--disable-disk-cleanup=false`
    // is an operator saying something they do not mean, and a flag pair whose
    // two halves disagree about what a value means is worse than either rule
    // on its own.
    const disableDiskCleanupValidation = validateBareOrTrueFlag(flags, 'disable-disk-cleanup');
    if (!disableDiskCleanupValidation.valid) {
      process.exit(2);
      return;
    }

    const DISABLE_DISK_CLEANUP_CONFLICTING_BEAT_FLAGS = [
      ['evict-pid', '--evict-pid'],
      ['bind-pid', '--bind-pid'],
      ['enqueue', '--enqueue'],
      ['peek', '--peek'],
      ['dequeue', '--dequeue'],
      ['dequeue-if-capacity', '--dequeue-if-capacity'],
      ['ack-lease', '--ack-lease'],
      ['release-lease', '--release-lease'],
      ['release', '--release'],
      ['claim', '--claim'],
      ['query-capacity', '--query-capacity'],
      ['record-outcome', '--record-outcome'],
      ['poll-footprint', '--poll-footprint'],
      ['heartbeat', '--heartbeat'],
      ['desired-agents', '--desired-agents'],
      // Reciprocal only — both of these blocks run BEFORE this one and each
      // already lists `disable-disk-cleanup`. Listed to keep this array
      // exhaustive against every top-level dispatch flag, per
      // doc-honesty.jest.spec.mjs's structural gate.
      ['watchdog-beat', '--watchdog-beat'],
      ['enable-disk-cleanup', '--enable-disk-cleanup'],
    ];
    // KEPT ON ONE LINE DELIBERATELY — see the identical note in the arming
    // block above; doc-honesty.jest.spec.mjs's "entries are actually consulted"
    // gate matches this exact single-line form. Do not reflow.
    const conflictingEntry = DISABLE_DISK_CLEANUP_CONFLICTING_BEAT_FLAGS.find(([flagKey]) => flags[flagKey] !== undefined);
    const conflictingFlag = conflictingEntry?.[1];

    if (conflictingFlag) {
      process.stderr.write(
        `agent-resource-management: --disable-disk-cleanup cannot be combined with ${conflictingFlag} in the ` +
          'same invocation — standing the disk-axis temp sweep back down is a standalone, deliberate act by ' +
          'design, and must never ride along on a beat that does something else\n',
      );
      process.exit(2);
      return;
    }

    await handleDisableDiskCleanup(flags);
    return;
  }

  // Checked absolute FIRST, ahead of every other beat-type flag's own
  // conflict check — mirrors `--enqueue`'s own "runs first"
  // rationale immediately below verbatim: none of the other beat-type
  // blocks know about `--evict-pid`, so if this block ran after them, an
  // invocation like `--peek --evict-pid=...` would fall straight into
  // `--peek`'s own (evict-unaware) conflict check, find no conflict, and
  // dispatch into `handlePeek` as if `--evict-pid` weren't present at all.
  // Running first closes that gap unconditionally, and — being the very
  // first check in `main()` — needs no reciprocal entry added to any of the
  // other blocks' own conflict lists to achieve full bidirectional coverage.
  if (flags['evict-pid'] !== undefined) {
    // round-4 review, Low — this is the one beat-type flag introduced
    // by this PR, so it's refactored to an ordered array + `.find()` rather
    // than a nested-ternary chain (the pattern every OTHER beat-type
    // conflict-check block below still uses): "is every beat-type flag
    // covered" is now verifiable at a glance instead of by counting ternary
    // levels, and adding a newly-introduced flag (the exact class of gap
    // `--poll-footprint`'s own belated addition below was, per
    // round-2 review) is a one-line append. The other blocks' own
    // nested-ternary conflict checks are left as-is — refactoring all of
    // them is a larger, separate change out of scope for this fix; this one
    // is refactored because it's the block this PR actually touches.
    const EVICT_CONFLICTING_BEAT_FLAGS = [
      ['enqueue', '--enqueue'],
      ['peek', '--peek'],
      ['dequeue', '--dequeue'],
      ['claim', '--claim'],
      ['release', '--release'],
      ['query-capacity', '--query-capacity'],
      ['record-outcome', '--record-outcome'],
      ['heartbeat', '--heartbeat'],
      ['desired-agents', '--desired-agents'],
      ['dequeue-if-capacity', '--dequeue-if-capacity'],
      ['ack-lease', '--ack-lease'],
      ['release-lease', '--release-lease'],
      // review, Low — `--poll-footprint` was missing from this list,
      // so `--evict-pid=X --poll-footprint=Y` silently dispatched into
      // `handleEvict` and ignored `--poll-footprint` entirely, contradicting
      // this block's own "full bidirectional coverage" claim above.
      ['poll-footprint', '--poll-footprint'],
      // Phase 6 — `--watchdog-beat` is checked FIRST in `main()` (its
      // own conflict block runs ahead of this one and already lists
      // `evict-pid`), so this reciprocal entry is never the one that
      // actually catches `--evict-pid --watchdog-beat` in practice — added
      // purely to keep this array exhaustive against every top-level
      // dispatch flag, per `doc-honesty.jest.spec.mjs`'s own structural
      // completeness gate.
      ['watchdog-beat', '--watchdog-beat'],
      // Phase 3, — `--bind-pid` is checked immediately AFTER this
      // block in `main()` (real order: `--watchdog-beat` → `--evict-pid` →
      // `--bind-pid`), so THIS reciprocal entry is the one that actually
      // catches `--evict-pid --bind-pid` in practice (`--bind-pid`'s own
      // conflict block, checked later, never gets the chance to run) — added
      // for the same "keep this array exhaustive against every top-level
      // dispatch flag" rationale as `watchdog-beat` immediately above, but
      // unlike that entry, this one is NOT merely defensive.
      ['bind-pid', '--bind-pid'],
      // Phase 3 — reciprocal entries for the standalone arming and
      // disarming beats, per doc-honesty.jest.spec.mjs's
      // *_CONFLICTING_BEAT_FLAGS exhaustiveness gate.
      ['enable-disk-cleanup', '--enable-disk-cleanup'],
      ['disable-disk-cleanup', '--disable-disk-cleanup'],
    ];
    const conflictingEntry = EVICT_CONFLICTING_BEAT_FLAGS.find(([flagKey]) => flags[flagKey] !== undefined);
    const conflictingFlag = conflictingEntry?.[1];

    if (conflictingFlag) {
      process.stderr.write(
        `agent-resource-management: --evict-pid cannot be combined with ${conflictingFlag} in the same ` +
          'invocation — evicting a candidate process is a standalone decision-only beat type, not a spawn-' +
          'decision/advisory-read beat\n',
      );
      process.exit(2);
      return;
    }

    await handleEvict(flags);
    return;
  }

  // Checked LAST of the three beat-type-detection blocks — real order in
  // `main()` is `--watchdog-beat` →
  // `--evict-pid` → `--bind-pid` (this block). It still needs its own full
  // conflict list, mirroring `--evict-pid`'s own rationale immediately above:
  // none of the other, EARLIER-checked beat-type blocks
  // (`--peek`, `--claim`, etc.) know about `--bind-pid`, so if this block
  // didn't exist, an invocation like `--peek --bind-pid=...` would fall
  // straight into `--peek`'s own (bind-pid-unaware) conflict check, find no
  // conflict, and dispatch into `handlePeek` as if `--bind-pid` weren't
  // present at all.
  if (flags['bind-pid'] !== undefined) {
    const BIND_PID_CONFLICTING_BEAT_FLAGS = [
      ['enqueue', '--enqueue'],
      ['peek', '--peek'],
      ['dequeue', '--dequeue'],
      ['claim', '--claim'],
      ['release', '--release'],
      ['query-capacity', '--query-capacity'],
      ['record-outcome', '--record-outcome'],
      ['heartbeat', '--heartbeat'],
      ['desired-agents', '--desired-agents'],
      ['dequeue-if-capacity', '--dequeue-if-capacity'],
      ['ack-lease', '--ack-lease'],
      ['release-lease', '--release-lease'],
      ['poll-footprint', '--poll-footprint'],
      ['watchdog-beat', '--watchdog-beat'],
      // `--watchdog-beat` and `--evict-pid` are both checked BEFORE this
      // block in `main()`, and each already lists `bind-pid` in its own
      // conflict array — so neither of these two reciprocal entries is ever
      // the one that actually catches a real `--bind-pid --watchdog-beat` or
      // `--bind-pid --evict-pid` invocation in practice; both are
      // defensive-only, kept purely for this array's own exhaustiveness
      // against every top-level dispatch flag (same rationale
      // `EVICT_CONFLICTING_BEAT_FLAGS`'s own `watchdog-beat` entry documents
      // above).
      ['evict-pid', '--evict-pid'],
      // Phase 3 — reciprocal entries for the standalone arming and
      // disarming beats, per doc-honesty.jest.spec.mjs's
      // *_CONFLICTING_BEAT_FLAGS exhaustiveness gate.
      ['enable-disk-cleanup', '--enable-disk-cleanup'],
      ['disable-disk-cleanup', '--disable-disk-cleanup'],
    ];
    const conflictingEntry = BIND_PID_CONFLICTING_BEAT_FLAGS.find(([flagKey]) => flags[flagKey] !== undefined);
    const conflictingFlag = conflictingEntry?.[1];

    if (conflictingFlag) {
      process.stderr.write(
        `agent-resource-management: --bind-pid cannot be combined with ${conflictingFlag} in the same ` +
          'invocation — binding a pid to a lease is a standalone decision-only beat type, not a spawn-' +
          'decision/advisory-read beat\n',
      );
      process.exit(2);
      return;
    }

    await handleBindPid(flags);
    return;
  }

  // Checked FIRST, ahead of every pre-existing beat-type flag's own conflict
  // check: none of `--release`/`--claim`/`--query-capacity`
  // know about `--enqueue`/`--peek`/`--dequeue`, so if these blocks ran
  // AFTER those, a `--claim` combined with `--enqueue` would fall straight
  // into `--claim`'s own (queue-unaware) conflict check, find no conflict,
  // and dispatch into `handleClaim` as if `--enqueue` weren't present at
  // all. Running first closes that gap unconditionally — whichever of these
  // three flags is present always wins the race to name itself in the
  // rejection, regardless of argv order.
  if (flags.enqueue !== undefined) {
    const conflictingFlag =
      flags.peek !== undefined
        ? '--peek'
        : flags.dequeue !== undefined
          ? '--dequeue'
          : flags.claim !== undefined
            ? '--claim'
            : flags.release !== undefined
              ? '--release'
              : flags['query-capacity'] !== undefined
                ? '--query-capacity'
                : flags['record-outcome'] !== undefined
                  ? '--record-outcome'
                  : flags.heartbeat !== undefined
                    ? '--heartbeat'
                    : flags['desired-agents'] !== undefined
                      ? '--desired-agents'
                      : flags['dequeue-if-capacity'] !== undefined
                        ? '--dequeue-if-capacity'
                        : flags['ack-lease'] !== undefined
                          ? '--ack-lease'
                          : flags['release-lease'] !== undefined
                            ? '--release-lease'
                            : undefined;

    if (conflictingFlag) {
      process.stderr.write(
        `agent-resource-management: --enqueue cannot be combined with ${conflictingFlag} in the same invocation — ` +
          'enqueuing a work item is a standalone atomic-write beat type, not a spawn-decision/advisory-read beat\n',
      );
      process.exit(2);
      return;
    }

    await handleEnqueue(flags);
    return;
  }

  if (flags.peek !== undefined) {
    const conflictingFlag =
      flags.dequeue !== undefined
        ? '--dequeue'
        : flags.claim !== undefined
          ? '--claim'
          : flags.release !== undefined
            ? '--release'
            : flags['query-capacity'] !== undefined
              ? '--query-capacity'
              : flags['record-outcome'] !== undefined
                ? '--record-outcome'
                : flags.heartbeat !== undefined
                  ? '--heartbeat'
                  : flags['desired-agents'] !== undefined
                    ? '--desired-agents'
                    : flags['dequeue-if-capacity'] !== undefined
                      ? '--dequeue-if-capacity'
                      : flags['ack-lease'] !== undefined
                        ? '--ack-lease'
                        : flags['release-lease'] !== undefined
                          ? '--release-lease'
                          : undefined;

    if (conflictingFlag) {
      process.stderr.write(
        `agent-resource-management: --peek cannot be combined with ${conflictingFlag} in the same invocation — ` +
          'peeking the queue is a standalone advisory-read beat type, not a spawn-decision beat\n',
      );
      process.exit(2);
      return;
    }

    await handlePeek(flags);
    return;
  }

  if (flags.dequeue !== undefined) {
    const conflictingFlag =
      flags.claim !== undefined
        ? '--claim'
        : flags.release !== undefined
          ? '--release'
          : flags['query-capacity'] !== undefined
            ? '--query-capacity'
            : flags['record-outcome'] !== undefined
              ? '--record-outcome'
              : flags.heartbeat !== undefined
                ? '--heartbeat'
                : flags['desired-agents'] !== undefined
                  ? '--desired-agents'
                  : flags['dequeue-if-capacity'] !== undefined
                    ? '--dequeue-if-capacity'
                    : flags['ack-lease'] !== undefined
                      ? '--ack-lease'
                      : flags['release-lease'] !== undefined
                        ? '--release-lease'
                        : undefined;

    if (conflictingFlag) {
      process.stderr.write(
        `agent-resource-management: --dequeue cannot be combined with ${conflictingFlag} in the same invocation — ` +
          'dequeuing a work item is a standalone atomic-write beat type, not a spawn-decision/advisory-read beat\n',
      );
      process.exit(2);
      return;
    }

    await handleDequeue(flags);
    return;
  }

  // Phase 4 — `--dequeue-if-capacity`/`--ack-lease`/`--release-lease`
  // are three further standalone, decision-only beat types (mirroring
  // `--dequeue`'s own precedent). Each is checked here, ahead of every
  // pre-existing flag's own conflict check, so a combination like
  // `--dequeue-if-capacity --claim=...` is always caught by ITS OWN
  // conflict-aware block rather than silently falling through to `--claim`'s
  // (new-flag-unaware) check and dispatching into `handleClaim` as if
  // `--dequeue-if-capacity` weren't present at all — the existing blocks
  // above/below have also been extended (bidirectionally) to name these
  // three flags in their own conflict lists.

  if (flags['dequeue-if-capacity'] !== undefined) {
    const conflictingFlag =
      flags.enqueue !== undefined
        ? '--enqueue'
        : flags.peek !== undefined
          ? '--peek'
          : flags.dequeue !== undefined
            ? '--dequeue'
            : flags.claim !== undefined
              ? '--claim'
              : flags.release !== undefined
                ? '--release'
                : flags['query-capacity'] !== undefined
                  ? '--query-capacity'
                  : flags['record-outcome'] !== undefined
                    ? '--record-outcome'
                    : flags.heartbeat !== undefined
                      ? '--heartbeat'
                      : flags['desired-agents'] !== undefined
                        ? '--desired-agents'
                        : flags['ack-lease'] !== undefined
                          ? '--ack-lease'
                          : flags['release-lease'] !== undefined
                            ? '--release-lease'
                            : undefined;

    if (conflictingFlag) {
      process.stderr.write(
        `agent-resource-management: --dequeue-if-capacity cannot be combined with ${conflictingFlag} in the same ` +
          'invocation — dequeuing-with-admission is a standalone decision-only beat type, not a spawn-decision/' +
          'advisory-read beat\n',
      );
      process.exit(2);
      return;
    }

    await handleDequeueIfCapacity(flags);
    return;
  }

  if (flags['ack-lease'] !== undefined) {
    const conflictingFlag =
      flags.enqueue !== undefined
        ? '--enqueue'
        : flags.peek !== undefined
          ? '--peek'
          : flags.dequeue !== undefined
            ? '--dequeue'
            : flags.claim !== undefined
              ? '--claim'
              : flags.release !== undefined
                ? '--release'
                : flags['query-capacity'] !== undefined
                  ? '--query-capacity'
                  : flags['record-outcome'] !== undefined
                    ? '--record-outcome'
                    : flags.heartbeat !== undefined
                      ? '--heartbeat'
                      : flags['desired-agents'] !== undefined
                        ? '--desired-agents'
                        : flags['dequeue-if-capacity'] !== undefined
                          ? '--dequeue-if-capacity'
                          : flags['release-lease'] !== undefined
                            ? '--release-lease'
                            : undefined;

    if (conflictingFlag) {
      process.stderr.write(
        `agent-resource-management: --ack-lease cannot be combined with ${conflictingFlag} in the same invocation — ` +
          'acknowledging a lease is a standalone atomic-write beat type, not a spawn-decision/advisory-read beat\n',
      );
      process.exit(2);
      return;
    }

    await handleAckLease(flags);
    return;
  }

  if (flags['release-lease'] !== undefined) {
    const conflictingFlag =
      flags.enqueue !== undefined
        ? '--enqueue'
        : flags.peek !== undefined
          ? '--peek'
          : flags.dequeue !== undefined
            ? '--dequeue'
            : flags.claim !== undefined
              ? '--claim'
              : flags.release !== undefined
                ? '--release'
                : flags['query-capacity'] !== undefined
                  ? '--query-capacity'
                  : flags['record-outcome'] !== undefined
                    ? '--record-outcome'
                    : flags.heartbeat !== undefined
                      ? '--heartbeat'
                      : flags['desired-agents'] !== undefined
                        ? '--desired-agents'
                        : flags['dequeue-if-capacity'] !== undefined
                          ? '--dequeue-if-capacity'
                          : flags['ack-lease'] !== undefined
                            ? '--ack-lease'
                            : undefined;

    if (conflictingFlag) {
      process.stderr.write(
        `agent-resource-management: --release-lease cannot be combined with ${conflictingFlag} in the same ` +
          'invocation — releasing a lease is a standalone atomic-write beat type, not a spawn-decision/advisory-' +
          'read beat\n',
      );
      process.exit(2);
      return;
    }

    await handleReleaseLease(flags);
    return;
  }

  // Checked FIRST (ahead of `--claim`'s own conflict check below) so an
  // invocation combining `--release` with ANY other beat-type flag —
  // including `--claim` itself — is always caught here, naming `--release`
  // itself in the rejection. Mirrors `--claim`'s own precedent exactly
  //, including the bidirectional requirement: `--claim`'s
  // own conflict list below also names `--release` so the pairing is
  // rejected symmetrically regardless of which flag a future reader adds
  // first.
  if (flags.release !== undefined) {
    const conflictingFlag =
      flags.claim !== undefined
        ? '--claim'
        : flags['desired-agents'] !== undefined
          ? '--desired-agents'
          : flags['query-capacity'] !== undefined
            ? '--query-capacity'
            : flags['record-outcome'] !== undefined
              ? '--record-outcome'
              : flags.heartbeat !== undefined
                ? '--heartbeat'
                : flags['dequeue-if-capacity'] !== undefined
                  ? '--dequeue-if-capacity'
                  : flags['ack-lease'] !== undefined
                    ? '--ack-lease'
                    : flags['release-lease'] !== undefined
                      ? '--release-lease'
                      : undefined;

    if (conflictingFlag) {
      process.stderr.write(
        `agent-resource-management: --release cannot be combined with ${conflictingFlag} in the same invocation — ` +
          'releasing capacity is a standalone atomic-write beat type, not a spawn-decision/advisory-read beat\n',
      );
      process.exit(2);
      return;
    }

    await handleRelease(flags);
    return;
  }

  // Checked FIRST (ahead of `--query-capacity`'s own conflict check below) so
  // an invocation combining `--claim` with ANY other beat-type flag is always
  // caught here, naming `--claim` itself in the rejection — a `--claim`
  // combined with `--query-capacity` must never silently fall through to
  // `--query-capacity`'s own (claim-unaware) conflict check and dispatch into
  // `handleQueryCapacity` as if `--claim` weren't present at all.
  if (flags.claim !== undefined) {
    const conflictingFlag =
      flags.release !== undefined
        ? '--release'
        : flags['desired-agents'] !== undefined
          ? '--desired-agents'
          : flags['query-capacity'] !== undefined
            ? '--query-capacity'
            : flags['record-outcome'] !== undefined
              ? '--record-outcome'
              : flags.heartbeat !== undefined
                ? '--heartbeat'
                : flags['dequeue-if-capacity'] !== undefined
                  ? '--dequeue-if-capacity'
                  : flags['ack-lease'] !== undefined
                    ? '--ack-lease'
                    : flags['release-lease'] !== undefined
                      ? '--release-lease'
                      : undefined;

    if (conflictingFlag) {
      process.stderr.write(
        `agent-resource-management: --claim cannot be combined with ${conflictingFlag} in the same invocation — ` +
          'claiming capacity is a standalone atomic-write beat type, not a spawn-decision/advisory-read beat\n',
      );
      process.exit(2);
      return;
    }

    await handleClaim(flags);
    return;
  }

  if (flags['query-capacity'] !== undefined) {
    const conflictingFlag =
      flags['desired-agents'] !== undefined
        ? '--desired-agents'
        : flags['record-outcome'] !== undefined
          ? '--record-outcome'
          : flags.heartbeat !== undefined
            ? '--heartbeat'
            : flags['dequeue-if-capacity'] !== undefined
              ? '--dequeue-if-capacity'
              : flags['ack-lease'] !== undefined
                ? '--ack-lease'
                : flags['release-lease'] !== undefined
                  ? '--release-lease'
                  : undefined;

    if (conflictingFlag) {
      process.stderr.write(
        `agent-resource-management: --query-capacity cannot be combined with ${conflictingFlag} in the same ` +
          'invocation — capacity querying is a standalone advisory-read beat type, not a spawn-decision beat\n',
      );
      process.exit(2);
      return;
    }

    await handleQueryCapacity(flags);
    return;
  }

  if (flags['record-outcome'] !== undefined) {
    // `--record-outcome` boolean-flag validation (Phase 5 review, third
    // pass, Medium #2) — reuses `validateBareOrTrueFlag` rather than the
    // prior bare `if (flags['record-outcome'])` truthiness check, which
    // silently treated `--record-outcome=false` as truthy and dispatched into
    // `handleRecordOutcome` anyway. Bare `--record-outcome` (Phase 2's own
    // working precedent, `flags['record-outcome'] === true`) and
    // `--record-outcome=true` both remain valid and still dispatch below;
    // only a malformed non-bare/non-true value now hard-rejects.
    const recordOutcomeValidation = validateBareOrTrueFlag(flags, 'record-outcome');
    if (!recordOutcomeValidation.valid) {
      process.exit(2);
      return;
    }
    await handleRecordOutcome(flags);
    return;
  }

  if (flags['poll-footprint'] !== undefined) {
    await handlePollFootprint(flags);
    return;
  }

  const orchestratorId = typeof flags['orchestrator-id'] === 'string' ? flags['orchestrator-id'] : undefined;
  if (!orchestratorId) {
    process.stderr.write('agent-resource-management: --orchestrator-id=<id> is required\n');
    process.exit(2);
    return;
  }
  if (isReservedEntry(orchestratorId)) {
    process.stderr.write(
      `agent-resource-management: --orchestrator-id must not match the reserved double-underscore convention ` +
        `("__...__"), got "${orchestratorId}" — reserved ids collide with internal coordination-file bookkeeping ` +
        '(e.g. the shared-machine-sample cache) and are not valid orchestrator ids\n',
    );
    process.exit(2);
    return;
  }

  let desiredAgents = 1;
  if (flags['desired-agents'] !== undefined) {
    const rawDesiredAgents = flags['desired-agents'];
    const parsedDesiredAgents = Number(rawDesiredAgents);
    if (
      typeof rawDesiredAgents !== 'string' ||
      !Number.isInteger(parsedDesiredAgents) ||
      parsedDesiredAgents < 0
    ) {
      process.stderr.write(
        `agent-resource-management: --desired-agents must be a non-negative integer, got "${rawDesiredAgents}"\n`,
      );
      process.exit(2);
      return;
    }
    desiredAgents = parsedDesiredAgents;
  }

  // Phase 3 review — `--advise-only`: a genuinely NON-MUTATING advisory
  // run of this same spawn-decision beat.
  //
  // WHY IT EXISTS. The ARM PreToolUse gate
  // (./hooks/pretooluse-arm-gate.mjs) needs a verdict — "would a spawn be
  // allowed right now?" — for a session that has already opted into ARM. It
  // used to obtain that by invoking this CLI as `--desired-agents=1`, which is
  // a FULL beat, not a query: it re-`declareDibs`ed (silently overwriting the
  // calling session's own previously-declared `desiredAgents`, e.g. 4 -> 1),
  // consumed a global spawn-rate token a SECOND time for the same spawn,
  // re-sized this orchestrator's live-agent admission hold to the hook's
  // fabricated ask, refreshed the shared machine sample, and advanced the
  // shared AIMD ceiling state — all from a code path with no business steering
  // any of it. Every other orchestrator computing its allowance from the
  // shared ledger then under-counted this session's real load, which is
  // exactly the pacing signal ARM exists to provide.
  //
  // WHAT IT DOES. It runs the same `collect -> classify -> dibs read ->
  // traffic-light` pipeline this beat already runs, and READS the current
  // ledger (so other orchestrators' genuine contention is still accounted
  // for), but performs ZERO writes to the coordination file and takes NO
  // coordination lock. Both halves are unconditional — they hold whether the
  // file is absent, freshly created, fully populated, or corrupted:
  //   - no `ensureCoordinationDir` (never creates the file or its parent),
  //   - no `reconcileLedgers` sweep (no records reaped),
  //   - no `writeSharedSample` (the reserved shared-sample entry is read for
  //     reuse but never refreshed),
  //   - no `declareDibs` (and therefore no `__ever-opted-in__` refresh either
  //     — `declareDibs` owns that upsert),
  //   - no `advanceAimdCeilingState`, AND no cold-start seeding of the
  //     `__aimd-ceiling-state__` row either: the read below passes
  //     `persistColdStart: false`, which is both write-free and LOCK-free.
  //     Before that flag existed, an absent or malformed AIMD row made an
  //     advisory run seed it — the third mutation path found in this file's
  //     review history — and even the well-formed case took the shared lock
  //     with a ~10-20s default budget behind a hook with a 5s timeout,
  //     measured at 18.1s against a held lock,
  //   - no `reconcileMemoryProjectionAdmission` (its projection `decide` is
  //     still evaluated, against the dibs already read, so a genuine
  //     over-budget beat still reports `memory-projection-block` — only the
  //     refusal-retraction WRITE is skipped),
  //   - no `consumeGlobalSpawnTokens` (no rate-bucket token drawn),
  //   - no `reserveAdmission` (no live-agent admission capacity claimed, and
  //     therefore none for a read-only caller to have to release).
  //
  // Every other file this path touches — the footprint-state store
  // (`readFootprintStore`), the history store (`readHistory`), the ledger
  // itself (`readDibs`/`readEntriesRaw`) — is likewise read through a bare,
  // lock-free `fs.readFile` that tolerates absence. Nothing on this path
  // creates a file, a directory, a `.lock`, or a `.tmp-*` sibling.
  //
  // WHAT IT COSTS IN FIDELITY, stated plainly, because this design's
  // "beat 1 consulted, beats 2-N unchecked" gap is NOT fully closed by it:
  // the two host-wide ADMISSION-GRANTING gates — the global spawn-rate bucket
  // and the live-agent ceiling — are CONSUMPTION-based. They can only be
  // evaluated by taking from them, so they are unprobeable without mutating
  // and an advisory run cannot report their verdicts at all. What
  // `--advise-only` therefore actually enforces is the memory axis, the
  // pressure axis, the disk axis, and the dibs-share/memory-projection
  // arithmetic derived from them — NOT spawn-rate and NOT live-agent
  // concurrency. It reports `liveAgentGrant: null` and never applies the
  // bucket's denial clamp — i.e. it can be MORE permissive than a real beat,
  // never less. That is the correct direction for a gate whose whole design
  // is to be cheap and non-authoritative — the real beat the
  // orchestrator itself runs remains the authority — but it does mean the
  // PreToolUse gate built on this is a memory/pressure/disk check, not a full
  // admission check. this design's original Consequences claim ("the narrow but
  // real gap ... is closed") overstated the coverage — see this design's
  // "Amendment (2026-09-06,)" section, which records this residual
  // using this comment as its technical basis.
  //
  // The memory-projection arithmetic named above IS genuinely evaluated for
  // the consulting session's own already-running fleet (Phase 3, third
  // review round, Medium #1 — before that fix it silently was not: an
  // advisory caller supplies no `--running-agent-classes`, and the empty list
  // that produced replaced the self dibs entry's own declared classes, so the
  // session's whole running fleet was priced at zero and a real
  // `memory-projection-block` reported `spawn-allowed`). What it now prices
  // is `projectionOwnRunningAgentClasses` — see its definition below.
  //
  // THREE RESIDUAL PROJECTION GAPS remain — the first is a staleness cliff,
  // not a narrow gap — all stated here so the amendment above can be
  // accurate rather than optimistic:
  //   1. STALENESS CLIFF, not a gradual degradation. The self fleet is read
  //      from the LAST REAL BEAT's persisted `agentClasses`, via a `readDibs`
  //      call that prunes on `DEFAULT_LIVENESS_THRESHOLD_MS` (15 minutes).
  //      Two distinct costs follow, and the second is the severe one:
  //        (a) between beats, agents this session started since its last beat
  //            are not in the persisted set, so a session mid-fan-out is
  //            under-counted; and
  //        (b) ONCE THE SESSION'S OWN DIBS ROW AGES PAST 15 MINUTES it is
  //            pruned from the liveness-filtered read, `selfEntry` is
  //            `undefined`, and this fallback returns NOTHING. That is not a
  //            partial under-count: the memory-projection axis reverts
  //            WHOLESALE to the pre-fix behaviour described above — this
  //            session's entire running fleet priced at zero — for every
  //            advisory consult until its next real beat refreshes the row.
  //            A ledger that reports `memory-projection-block` when the self
  //            row is fresh reports `spawn-allowed` on the same ledger once
  //            that row is 20 minutes old.
  //      No cheaper truth exists for (a): there is no per-agent liveness
  //      signal an advisory run could derive the current set from. (b) is
  //      fixable — read the self row unpruned, ignoring liveness for this
  //      fallback only — but that is a genuine trade-off (conservative on
  //      memory vs. potentially blocking a session whose fleet has actually
  //      finished, until its next beat) and is deliberately NOT decided here.
  //      Recorded as an open, deliberately-undecided trade-off in this design's
  //      "Amendment (2026-09-06,)" section, alongside the
  //      spawn-rate-bucket / live-agent-ceiling limitation above.
  //   2. An advisory caller supplies no `--agent-class` either, so the
  //      candidate is priced as `UNLABELLED_AGENT_CLASS` rather than the
  //      class actually about to spawn. That direction is CONSERVATIVE, not
  //      permissive: a class with no usable history is priced at the greater
  //      of the most-expensive known class in the computation and the
  //      cold-start floor (see `computeProjectedPeakMemoryMb`).
  //   3. A SAME-CLASS FLEET IS PRICED AS ONE AGENT, on this advisory path
  //      only. The persisted `agentClasses` field this fallback reads is
  //      written by `declareDibs` below as
  //      `Array.from(new Set([agentClass, ...ownRunningAgentClasses]))` — it
  //      is DEDUPLICATED by construction, because the dibs entry records the
  //      SET of classes a session has running, not a multiset with counts.
  //      So a session running four live `implementer`-class agents has a
  //      persisted set of ONE entry, and this fallback prices that whole
  //      fleet at one agent's projected peak. The REAL beat is unaffected:
  //      it reads the raw, non-deduplicated `--running-agent-classes` flag
  //      directly and never consults the persisted set for its own fleet.
  //      Direction is PERMISSIVE (an undercount), the same direction as gaps
  //      1 and 2 — the advisory run stays more permissive than a real beat,
  //      never less — so this is a coverage hole on the memory-projection
  //      axis, not a new failure mode. Closing it would mean persisting
  //      per-class COUNTS in the dibs entry, a ledger schema change with its
  //      own back-compat surface, deliberately not undertaken here. Recorded
  //      as accepted residual item 7 in this design's
  //      "Amendment (2026-09-06,)" section.
  //
  // Deliberately rejected in combination with `--heartbeat`: a heartbeat is
  // already a distinct "poll, don't advance" mode with its own mutating
  // liveness/history semantics, and silently layering a second, stronger
  // read-only modifier on top would make which writes actually happen depend
  // on a flag pair no caller has a reason to use.
  const adviseOnlyValidation = validateBareOrTrueFlag(flags, 'advise-only');
  if (!adviseOnlyValidation.valid) {
    process.exit(2);
    return;
  }
  const isAdviseOnly = adviseOnlyValidation.isSet;
  if (isAdviseOnly && flags.heartbeat !== undefined) {
    process.stderr.write(
      'agent-resource-management: --advise-only cannot be combined with --heartbeat — --advise-only is already ' +
        'a fully non-mutating advisory run, and --heartbeat is a distinct mode with its own mutating ' +
        'liveness/history semantics\n',
    );
    process.exit(2);
    return;
  }

  // Phase 2 review, High finding — `--agent-class=<name>` on the
  // `--desired-agents` beat. A missing/non-string value never crashes this
  // beat (mirrors `handlePollFootprint`'s own precedent), but — UNLIKE the
  // pre-fix behaviour — it no longer degrades to a class-less beat exempt
  // from the memory-projection gate. It degrades to `UNLABELLED_AGENT_CLASS`
  // instead: a real, priced, counted catch-all class, so an orchestrator
  // that hasn't adopted the flag still goes through the SAME conservative
  // cold-start-priced gate as any labelled beat, and still contributes to
  // (and is visible in) every other beat's `runningClasses` sum. See
  // `UNLABELLED_AGENT_CLASS`'s own doc comment above for the full rationale.
  const agentClass = typeof flags['agent-class'] === 'string' ? flags['agent-class'] : UNLABELLED_AGENT_CLASS;

  // review, Nit — a caller passing the sentinel string LITERALLY as
  // `--agent-class=__unlabelled__` would otherwise be indistinguishable from
  // an omitted flag once it reaches the gate/dibs entry, silently pooling
  // its history and admission decisions into the shared catch-all bucket
  // instead of getting its own class. Reject explicitly, the same way
  // `isReservedEntry` rejects a colliding orchestrator id, rather than
  // letting it masquerade as "no class supplied".
  if (flags['agent-class'] === UNLABELLED_AGENT_CLASS) {
    process.stderr.write(
      `agent-resource-management: --agent-class must not equal the reserved sentinel "${UNLABELLED_AGENT_CLASS}" ` +
        '— that value is reserved for beats that omit --agent-class entirely\n',
    );
    process.exit(2);
    return;
  }

  // Phase 2 review, Medium finding (dibs schema undercounts a
  // multi-class fleet) — `--running-agent-classes=<a>,<b>,...` is an
  // additive, optional flag: OTHER agent classes this SAME orchestrator
  // already has running concurrently, besides the one named by
  // `--agent-class` (which is always this beat's own admission candidate).
  // Without this, an orchestrator that fans out several concurrently-running
  // sub-agent classes could only ever have its dibs entry reflect the
  // single MOST-RECENTLY-DECLARED class — every earlier class silently
  // disappears from every other orchestrator's projected-peak sum the
  // moment this orchestrator's next beat overwrites its own entry. Passing
  // the full concurrently-live set explicitly, every beat, is this
  // orchestrator's own responsibility (there is no separate per-class
  // liveness signal this CLI could derive that on its own) — `declareDibs`'s
  // existing full-replace-on-upsert semantics need no change to support
  // this: the caller simply supplies the complete current set each time.
  const runningAgentClassesFlag =
    typeof flags['running-agent-classes'] === 'string' ? flags['running-agent-classes'] : '';
  const ownRunningAgentClasses = runningAgentClassesFlag
    .split(',')
    .map((agentClassName) => agentClassName.trim())
    .filter((agentClassName) => agentClassName.length > 0);

  const coordinationFilePath = resolveCoordinationFilePath();
  const now = resolveNow();

  // Phase 3 — `leakBlocks` detection echo. Computed once, early,
  // BEFORE any admission branch (pressure-block, memory-projection-block, or
  // the normal admission path) so it can be attached as additive metadata on
  // every one of this beat's `process.stdout.write` call sites, regardless of
  // which one this beat reaches — mirrors `loadAverage`/`compressedMb`'s own
  // "computed once, attached everywhere" convention. Never gated on
  // `isHeartbeat`: this is pure per-pid detection, not an admission-
  // advancement concern (see `computeLeakBlocksForBeat`'s own doc comment).
  // Degrades to `[]` on a footprint-state read failure (missing/unwritable
  // file, corrupt JSON) rather than aborting the beat — mirrors
  // `buildWatchdogCandidates`'s own `readFootprintStore(...).catch(() => ({}))`
  // precedent exactly: a beat that cannot read the footprint-state store
  // still owes its own admission decision, and this feature is additive/
  // observational, never load-bearing for that decision.
  const footprintStoreForLeakBlocks = await readFootprintStore(resolveFootprintStateFilePath()).catch(() => ({}));
  const leakBlocks = computeLeakBlocksForBeat(footprintStoreForLeakBlocks, DEFAULT_LEAK_VELOCITY_THRESHOLDS, now);

  // Ensure the coordination file's parent directory exists before the first
  // declareDibs call below — see ensureCoordinationDir's doc comment.
  // Skipped under `--advise-only`: there is no `declareDibs` (or any other
  // write) to prepare for, and creating the directory would itself be a
  // side effect on a path documented to have none.
  if (!isAdviseOnly) {
    await ensureCoordinationDir(coordinationFilePath);
  }

  // — this beat's ONE process observation, taken before any lock is
  // acquired, and the ledger reconciliation it feeds. Placed ahead of the
  // `readDibs` below (and therefore ahead of every allowance, claim and
  // admission decision this beat makes) so a record whose process is provably
  // gone cannot count against this same beat's own capacity math. That
  // ordering is what makes "full capacity restored in one beat" true rather
  // than "eventually".
  //
  // This is the single call site that satisfies both halves of "runs at
  // startup and on each beat": every invocation of this CLI is one beat.
  // `--advise-only` skips the whole observe-and-reconcile step: `reconcileLedgers`
  // DELETES ledger records it can prove are dead, which is a write, and
  // `resolveBeatLedgerIdentity` exists only to stamp identity onto writes this
  // run will never make. Skipping it makes an advisory verdict at worst
  // slightly CONSERVATIVE (a dead orchestrator's record may still count
  // against capacity until a real beat sweeps it), never over-permissive — the
  // safe direction, and it also saves the advisory path its `ps` shell-out.
  const beatObservation = isAdviseOnly ? null : await observeBeatProcesses();
  if (!isAdviseOnly) {
    await reconcileLedgers(coordinationFilePath, beatObservation);
  }
  const beatLedgerIdentity = isAdviseOnly ? {} : await resolveBeatLedgerIdentity();

  // Read this orchestrator's own last-declared entry (if any) so the
  // hysteresis history threads across separate CLI invocations, not just
  // within one process's lifetime — each beat is a fresh `node cli.mjs`
  // process, so history can't live in memory between beats.
  let existingEntries = [];
  try {
    existingEntries = await readDibs(coordinationFilePath, now, {
      livenessThresholdMs: DEFAULT_LIVENESS_THRESHOLD_MS,
    });
  } catch (error) {
    warnCoordinationDegraded(error);
  }
  const selfEntry = existingEntries.find((entry) => entry.orchestratorId === orchestratorId);

  // Freshness-window shared-sample reuse: before ever
  // shelling out via `takeSample(collect)`, check whether a DIFFERENT
  // orchestrator already sampled the machine recently enough (within
  // DEFAULT_FRESHNESS_WINDOW_MS) and stored it under the reserved
  // `SHARED_SAMPLE_RESERVED_ID` entry. Reusing it entirely skips
  // this beat's own `collect()` shell-outs. When stale/absent, sample fresh
  // as before and persist the new reading under the reserved entry so the
  // NEXT invocation (from any orchestrator) can reuse it.
  //
  // Deliberately never reused when THIS invocation carries its own
  // well-formed `ARM_FAKE_COLLECT_JSON` — see `hasExplicitFakeCollectInput`'s
  // doc comment for why that seam is honoured unconditionally.
  const sharedSampleEntry = existingEntries.find(
    (entry) => entry.orchestratorId === SHARED_SAMPLE_RESERVED_ID,
  );

  // A reserved shared-sample entry that's present but has no well-formed
  // `reading` (missing/malformed `memory` or `disk`) must degrade the same
  // way a stale/absent entry does — fall through to sampling fresh below —
  // rather than being treated as reusable and crashing downstream classifiers
  // on `undefined` fields. Every other coordination-file read path in this
  // file degrades gracefully; this shape check keeps this one consistent.
  const hasWellFormedSharedReading = Boolean(sharedSampleEntry?.reading?.memory && sharedSampleEntry?.reading?.disk);

  let rawSample;
  // Phase 6 — tracks whether THIS beat took a genuine fresh sample of
  // its own (vs. reusing another orchestrator's still-fresh cached reading
  // verbatim). Only a beat with its own fresh reading has real "current vs.
  // prior" evidence to derive a disk trend from — see the `priorDiskSample`/
  // `diskTrend` computation below.
  let tookFreshSample = false;
  if (
    !hasExplicitFakeCollectInput() &&
    hasWellFormedSharedReading &&
    isSampleFresh(sharedSampleEntry, now, DEFAULT_FRESHNESS_WINDOW_MS)
  ) {
    rawSample = sharedSampleEntry.reading;
  } else {
    rawSample = await takeSample(collect);
    tookFreshSample = true;

    // Bounded-retry total failure: both the original
    // `collect()` attempt and its single immediate retry threw. Propagate to
    // the top-level `main().catch()` exit-1 stderr path NOW — before this
    // beat's single `declareDibs` call below — rather than letting the
    // pipeline continue on `takeSample()`'s swallowed stale/degraded sample.
    if (totalCollectFailure) {
      throw totalCollectFailure;
    }

    // Only a genuinely fresh, non-degraded reading is worth sharing — a
    // `{ stale: true, ... }` marker (from a swallowed collect() failure or
    // malformed ARM_FAKE_COLLECT_JSON) must never be persisted as if it were
    // a usable shared sample for the next invocation to reuse.
    // `--advise-only` never refreshes the shared sample: the reserved
    // `__shared-machine-sample__` entry is READ above (a fresh-enough cached
    // reading is still reused, which is what keeps an advisory run cheap) but
    // never written back. An advisory run that persisted its own sample would
    // silently reset every other orchestrator's freshness window.
    if (!rawSample.stale && !isAdviseOnly) {
      // Phase 6 (review, Medium #2) — deliberately NOT persisting a
      // separate `priorDiskSample` field here. The reserved shared-sample
      // entry's own `reading.disk`/`sampledAt` (captured above, BEFORE this
      // write, as `sharedSampleEntry`) already fully serves as "the prior
      // disk sample" for the NEXT beat that reads this entry back — that's
      // exactly what the `priorDiskSample` local variable below does with
      // it. An earlier revision also wrote those same values out a second
      // time under a distinct persisted `priorDiskSample` field, but nothing
      // ever read that field back: every trend computation derives its
      // "prior" value from `reading`/`sampledAt` directly, one beat removed.
      // A persisted-but-unread field is dead weight that could silently rot
      // (a future change might edit `priorDiskSample` believing it's the
      // "official" home for this and never notice `reading`/`sampledAt`
      // stayed authoritative) — so there is intentionally only ONE
      // persisted home for "prior disk sample" data: this entry's own
      // `reading`/`sampledAt`, one beat back.
      // — persist via `writeSharedSample`'s locked compare-and-swap
      // rather than `declareDibs`'s blind upsert: `declareDibs` always
      // overwrites the reserved entry regardless of which sample is
      // genuinely newer, which is exactly the race that let a stale sample
      // win (see writeSharedSample's doc comment in coordination-file.mjs).
      try {
        await writeSharedSample(coordinationFilePath, {
          sampledAt: now,
          stale: rawSample.stale,
          memory: rawSample.memory,
          disk: rawSample.disk,
        });
      } catch (error) {
        warnCoordinationDegraded(
          error,
          'coordination file',
          "this beat's machine sample was not shared with other orchestrators",
        );
      }
    }
  }

  // Phase 6 — real disk-decline trend. `priorDiskSample` for THIS
  // beat's own trend computation is the shared-sample entry as it existed
  // BEFORE this beat's write above (`sharedSampleEntry`, read earlier in
  // main()) — only meaningful when this beat took a genuine fresh sample of
  // its own (see `tookFreshSample`'s doc comment above); the reuse branch
  // carries no independent "current vs. prior" evidence.
  const priorDiskSample =
    tookFreshSample && sharedSampleEntry?.reading?.disk && Number.isFinite(sharedSampleEntry.sampledAt)
      ? { freeDiskGb: sharedSampleEntry.reading.disk.freeDiskGb, sampledAt: sharedSampleEntry.sampledAt }
      : undefined;

  const diskTrend = computeDiskTrend(
    rawSample.disk?.freeDiskGb,
    priorDiskSample,
    now,
    DEFAULT_DISK_TREND_RECENCY_WINDOW_MS,
  );

  // The real computed trend rate — not the raw sample's own (possibly
  // fixture-decoy) `declineRateGbPerHour` — is what drives
  // `classifyDiskAxis`'s steep-decline AMBER check from here on. Never
  // mutates `rawSample.disk` in place: a fresh object is built so any other
  // consumer of `rawSample` (e.g. the shared-sample persistence above, which
  // already ran) is unaffected.
  const diskSampleForClassification = rawSample.disk
    ? { ...rawSample.disk, declineRateGbPerHour: diskTrend.declineRateGbPerHour }
    : rawSample.disk;

  // `--debug`/`--verbose` — diagnostic-only, never gating,
  // and never to be parsed by an orchestrator (see `printDebugDiagnostics`'s
  // own doc comment). Checked here, after `rawSample` is available but
  // before the `--heartbeat`-vs-`--desired-agents` branch below, so the same
  // raw memory/disk inputs are printed identically regardless of which of
  // those two beat types this invocation turns out to be.
  if (flags.debug !== undefined || flags.verbose !== undefined) {
    printDebugDiagnostics(rawSample);
  }

  // `--heartbeat` marks this invocation as a timer-driven poll between real
  // beats (see DEFAULT_HEARTBEAT_INTERVAL_MS above), not a real
  // once-per-sub-agent-task beat. It still reads the machine and drives the
  // full pipeline below identically — the ONLY difference is the mode passed
  // to the classifiers, which tells ./lib/threshold.mjs's applyHysteresis to
  // classify the current sample without advancing the `consecutiveGreen`
  // recovery counter, so polling on a timer can never fast-forward recovery
  // relative to an orchestrator that only calls this CLI once per real beat.
  // `--heartbeat` boolean-flag validation (Phase 1, now via the shared
  // `validateBareOrTrueFlag` helper above — Phase 5 review, third pass,
  // Medium #2): only bare `--heartbeat` (parseArgs yields `true`) or
  // `--heartbeat=true` (parseArgs yields the string `"true"`, via the
  // `eqIndex` branch) are valid — both coerce to heartbeat mode. Anything
  // else — `--heartbeat=false`, `--heartbeat=1`, `--heartbeat=TRUE`
  // (case-sensitive), or bare `--heartbeat` swallowing a following positional
  // value (`--heartbeat maybe`, which parseArgs assigns as the string
  // `"maybe"` since it doesn't start with `--`) — is a malformed invocation
  // and must be rejected the same way as a malformed
  // `--orchestrator-id`/`--desired-agents`: a clear stderr message and exit
  // 2, checked AFTER --orchestrator-id/--desired-agents validation (unchanged
  // ordering) but before any of this beat's work proceeds.
  const heartbeatValidation = validateBareOrTrueFlag(flags, 'heartbeat');
  if (!heartbeatValidation.valid) {
    process.exit(2);
    return;
  }
  const isHeartbeat = heartbeatValidation.isSet;
  const classificationMode = isHeartbeat ? { countsTowardRecovery: false } : undefined;

  // `--user-attests-idle` boolean-flag validation — same
  // shared `validateBareOrTrueFlag` contract as `--heartbeat`/`--crashed`/
  // `--record-outcome`/`--advise-only` above: only bare `--user-attests-idle`
  // or `--user-attests-idle=true` are valid. SAFETY NOTE: this validation is
  // purely a syntax check on the flag's own value — it never reads
  // `pressureLevel`, never influences the pressure-block short-circuit further
  // below, and is checked here (alongside every other beat-shaping flag) only
  // for consistency with this file's existing validate-all-flags-up-front
  // convention. The flag's actual ADMISSION effect is wired much further
  // down, strictly after the pressure-block short-circuit has already had its
  // chance to return — see the `userAttestsIdle` idle-probe admission block
  // below `buildTrafficLight`.
  const userAttestsIdleValidation = validateBareOrTrueFlag(flags, 'user-attests-idle');
  if (!userAttestsIdleValidation.valid) {
    process.exit(2);
    return;
  }
  const userAttestsIdle = userAttestsIdleValidation.isSet;

  // Phase 5 — load average, purely observational (see getLoadAverage's
  // own doc comment for the full "record, do not gate" rationale). Computed
  // once, early, and attached as a sibling field at every `process.stdout.write`
  // call site below (mirroring `diskTrend`) — never read by anything that
  // feeds an admission decision.
  const loadAverage = getLoadAverage();

  // — compressed-memory reading, purely observational, same
  // "record, do not gate" convention as `loadAverage` immediately above.
  // Computed once, early (from the same `rawSample` already used for the
  // admission decision below), and attached as a sibling field at every
  // `process.stdout.write` call site in this function — not just the final
  // one — so it's present on every beat type, mirroring `loadAverage`'s own
  // universal presence rather than only appearing on beats that reach the
  // end of this function. Never passed into `classifyMemoryRaw`/
  // `classifyMemoryAxis`, never consulted by any admission-verdict
  // function; see its investigation/convergence-analysis for why this
  // field is record-only, not gated — compression pressure alone is a
  // weak, noisy admission signal on this host (same class of reasoning as
  // the `loadAverage` doc comment above). It can legitimately be `NaN`
  // (`parseVmStat`'s degrade-on-garbled-input behavior) — `JSON.stringify`
  // serializes `NaN` to `null`, which is fine here and needs no
  // special-casing, matching how other diagnostic fields in this file
  // already behave.
  const compressedMb = rawSample.memory?.compressedMb;

  // Phase 3 — compression velocity, purely observational, same
  // "record, do not gate" convention as `loadAverage`/`compressedMb`
  // immediately above; never passed into `classifyMemoryRaw`/
  // `classifyMemoryAxis`, never consulted by any admission-verdict function.
  // `previous` is sourced from `sharedSampleEntry` as it existed BEFORE this
  // beat's own `writeSharedSample` call above — mirroring `priorDiskSample`'s
  // `tookFreshSample` gate a few lines up: only a beat that took a genuine
  // fresh sample of its own has real "current vs. prior" evidence to derive a
  // rate from; the cache-reuse branch carries no independent evidence of its
  // own. A prior entry missing `compressions`/`decompressions`/`sampledAt`
  // altogether — either because none is stored yet, or because it's a
  // pre-existing legacy entry that never wrote those fields — degrades to
  // `previous: null` (cold-start-equivalent), exactly like every other
  // coordination-file read path in this file, rather than fabricating a
  // delta against `undefined`.
  const priorCompressionSample =
    tookFreshSample &&
    Number.isFinite(sharedSampleEntry?.reading?.memory?.compressions) &&
    Number.isFinite(sharedSampleEntry?.reading?.memory?.decompressions) &&
    Number.isFinite(sharedSampleEntry?.sampledAt)
      ? {
          compressions: sharedSampleEntry.reading.memory.compressions,
          decompressions: sharedSampleEntry.reading.memory.decompressions,
          timestampMs: sharedSampleEntry.sampledAt,
        }
      : null;
  const compressionVelocity = computeCompressionVelocity({
    previous: priorCompressionSample,
    current: {
      compressions: rawSample.memory?.compressions,
      decompressions: rawSample.memory?.decompressions,
      timestampMs: now,
    },
  });

  const memoryResult = classifyMemoryAxis(
    rawSample.memory,
    DEFAULT_MEMORY_THRESHOLDS,
    selfEntry?.memoryHistory,
    DEFAULT_HYSTERESIS_CONFIG,
    classificationMode,
    now,
  );
  const diskResult = classifyDiskAxis(
    diskSampleForClassification,
    DEFAULT_DISK_THRESHOLDS,
    selfEntry?.diskHistory,
    DEFAULT_HYSTERESIS_CONFIG,
    classificationMode,
    now,
  );

  // Declare this beat's dibs (desired agents + refreshed hysteresis
  // history) and re-read the ledger. On a coordination-file-directory
  // problem (missing/unwritable parent dir, etc.) degrade gracefully: warn
  // to stderr and proceed as if no dibs landed this beat, rather than
  // crashing the orchestrator's calling loop — a traffic light computed
  // without this beat's dibs entry is strictly better than no traffic light
  // at all.
  //
  // A `--heartbeat` beat's OWN dibs entry never derives its desiredAgents
  // from this invocation's `--desired-agents` (SKILL.md documents that flag
  // as informational-only on a heartbeat, never counted) — see the identical
  // rationale on the global spawn-bucket guard below. Without SOME guard,
  // `computePerOrchestratorAllowance` (./lib/allowance.mjs) would treat a
  // heartbeat's own live dibs entry as genuine contention: a second
  // orchestrator polling via `--heartbeat` (no `--desired-agents`, defaulting
  // to `1`) would otherwise silently divide a REAL contending orchestrator's
  // share, purely because it polled, not because it wants to spawn anything.
  //
  // Critically, a heartbeat must PRESERVE this orchestrator's own prior real
  // intent rather than clobbering it to 0: `declareDibs` upserts (fully
  // replaces) this orchestrator's entry, so if a heartbeat always wrote `0`,
  // a busy orchestrator's live "I want N agents" declaration would read as
  // `desiredAgents: 0` for the large majority of wall-clock time (heartbeats
  // fire every ~20s vs. real beats only after each sub-agent completes),
  // nullifying dibs-constrained allowance division almost entirely. Reusing
  // `selfEntry` (already read above, before this beat's own declareDibs)
  // preserves the last REAL beat's declared intent across heartbeat ticks. A
  // purely heartbeat-only orchestrator with no prior real entry still
  // correctly falls back to `0` (preserving the original phantom-contention-
  // prevention property for a poller with no real intent at all).
  const dibsDesiredAgents = isHeartbeat ? (selfEntry?.desiredAgents ?? 0) : desiredAgents;

  // `firstDeclaredAt` (High-priority fix) — the timestamp of this
  // orchestrator's FIRST-EVER declaration, carried forward across every
  // subsequent beat, distinct from `declaredAt` (refreshed every beat for
  // liveness pruning). `declareDibs` itself also preserves an existing
  // entry's `firstDeclaredAt` on upsert (see its doc comment) — this
  // explicit carry-forward here matters for the degraded-fallback branch
  // below, which constructs a dibs entry WITHOUT ever calling `declareDibs`.
  // `selfEntry` was already read above, before this beat's own write, so it
  // reflects this orchestrator's own entry from before this beat.
  const firstDeclaredAt = typeof selfEntry?.firstDeclaredAt === 'number' ? selfEntry.firstDeclaredAt : now;

  // Phase 6 — `declareDibs` and `readDibs` no longer share one `try`.
  // They fail for different reasons and warrant different recoveries, and a
  // shared catch made them indistinguishable both in the warning text and in
  // what the fallback then asserted.
  //
  // The distinction that matters: the fallback below FABRICATES this
  // orchestrator's own dibs entry. That is legitimate after a READ-back
  // failure — the write demonstrably landed, so the entry really is on disk
  // and this is just reconstructing a view of it. After a WRITE failure it
  // is not: it would assert a declaration that never reached the file, which
  // no other orchestrator can see. In that case the honest view is
  // `existingEntries` exactly as it was read at the top of this beat —
  // whatever is genuinely on disk, self included only if a PREVIOUS beat
  // successfully declared.
  // Phase 2 review, Medium finding — every agent class this
  // orchestrator currently considers concurrently live: the candidate
  // (`agentClass`, always a real string post-High-fix — either an explicit
  // `--agent-class` or `UNLABELLED_AGENT_CLASS`) plus whatever OTHER classes
  // it named via `--running-agent-classes`. De-duplicated: a class named
  // twice contributes ONE entry to this orchestrator's own declared set —
  // `computeProjectedPeakMemoryMb` prices each RUNNING AGENT once, and this
  // array's job is only to say WHICH classes this orchestrator has live, not
  // how many of each (an orchestrator running two agents of the identical
  // class still only needs that class named once here for another beat's
  // sum to see it as "at least one live" — undercounting the exact
  // concurrent count of one already-visible class is a smaller residual gap
  // than the one this fix closes, and out of this ticket's scope).
  //
  // review round 2, High finding — a `--heartbeat` beat that omits
  // BOTH `--agent-class` and `--running-agent-classes` (SKILL.md's own
  // worked heartbeat example does exactly this) must NOT collapse this
  // orchestrator's declared classes down to `[UNLABELLED_AGENT_CLASS]`. Left
  // unguarded, `agentClass` already defaults to the sentinel and
  // `ownRunningAgentClasses` defaults to `[]`, so every heartbeat tick would
  // silently overwrite whatever real classes the last REAL beat declared —
  // exactly the same clobber-on-upsert failure mode `dibsDesiredAgents`
  // above is guarded against, but left unfixed here. Because heartbeats fire
  // far more often than real beats, the corrupted `['__unlabelled__']` view
  // would be the steady state every OTHER orchestrator's projected-peak sum
  // reads, undercounting this orchestrator's real footprint. Mirror the same
  // preserve-prior-intent strategy: on a heartbeat with neither flag
  // explicitly supplied, carry the previous beat's own declared classes
  // forward via `readAgentClasses(selfEntry)` instead of recomputing from
  // this (flag-less) invocation. A purely heartbeat-only orchestrator with no
  // prior real entry still correctly falls back to `[UNLABELLED_AGENT_CLASS]`
  // (there is nothing real to preserve).
  const isHeartbeatWithNoExplicitClasses =
    isHeartbeat && typeof flags['agent-class'] !== 'string' && typeof flags['running-agent-classes'] !== 'string';
  const dibsAgentClasses = isHeartbeatWithNoExplicitClasses
    ? (() => {
        const preserved = readAgentClasses(selfEntry);
        return preserved.length > 0 ? preserved : [UNLABELLED_AGENT_CLASS];
      })()
    : Array.from(new Set([agentClass, ...ownRunningAgentClasses]));

  // Phase 3 — the disk-cleanup opt-in is READ HERE, BEFORE `declareDibs`
  // runs a few lines below, and that ordering is load-bearing, not incidental.
  //
  // `declareDibs` unions this beat's own orchestrator-id into the sticky
  // `__ever-opted-in__` set as a SIDE EFFECT of declaring capacity. The
  // gate reads `__disk-cleanup-armed__` — in BOTH production and test mode
  // since Phase 3's High review finding — and no capacity path ever
  // touches that row, so no ordering here can make a beat arm itself today.
  //
  // The read is still snapshotted BEFORE the declaration, deliberately, so
  // that the rule is enforced by control flow rather than by the current
  // contents of one reserved row: arming must PRE-EXIST the beat it arms. No
  // beat may arm itself. Should any future edit make a capacity path touch a
  // set this gate consults, that mistake shows up as a refused sweep rather
  // than as a self-armed recursive deletion.
  const diskCleanupArmed = await diskCleanupArmedForBeat(coordinationFilePath, orchestratorId, now);

  let dibs = existingEntries;
  let dibsDeclared = true;

  // `--advise-only` declares nothing. The ledger view it reasons over is
  // `existingEntries` exactly as read from disk, plus — only when this
  // orchestrator has no live entry of its own — an IN-MEMORY, never-persisted
  // stand-in for the spawn this run is asking about.
  //
  // That stand-in is what stops an advisory run from being systematically
  // wrong in the sticky-but-pruned case exists for: a session whose
  // live dibs entry has been pruned or released still has its sticky
  // `__ever-opted-in__` row, so the gate still consults — and
  // `computePerOrchestratorAllowance` returns 0 for a requester that is
  // absent from the dibs list entirely, which is a verdict about bookkeeping
  // rather than about the host. Supplying the hypothetical entry locally asks
  // the honest question ("if this session wanted N agents right now, what
  // would its share be against everyone genuinely contending?") without
  // asserting the answer to anyone else. `firstDeclaredAt: now` deliberately
  // gives the stand-in the LOWEST priority in the contended walk — a
  // hypothetical must never out-rank a real, older declaration.
  //
  // When a live self entry DOES exist it is left completely untouched, which
  // is the entire point of this mode: an advisory run must not rewrite this
  // session's own declared `desiredAgents` to whatever the advisory caller
  // happened to pass.
  if (isAdviseOnly) {
    dibs = selfEntry
      ? existingEntries
      : [
          ...existingEntries,
          { orchestratorId, desiredAgents, declaredAt: now, firstDeclaredAt: now, agentClasses: dibsAgentClasses },
        ];
    dibsDeclared = false;
  } else {
    try {
      await declareDibs(
        coordinationFilePath,
        {
          orchestratorId,
          desiredAgents: dibsDesiredAgents,
          declaredAt: now,
          firstDeclaredAt,
          // — ledger write call site 1 of 5. The beat's single resolved
          // identity, spread in unchanged; empty when none was resolvable, which
          // leaves both fields off the entry entirely. See the ledger-identity
          // note near `resolveNow`.
          ...beatLedgerIdentity,
          memoryHistory: memoryResult.history,
          diskHistory: diskResult.history,
          // Phase 2 review, Medium finding — an ARRAY of every agent
          // class this orchestrator currently declares concurrently live, not
          // a single most-recently-declared field. A single-field
          // `agentClass` upsert silently lost visibility of any OTHER class
          // this same orchestrator still had running the moment a later beat
          // declared a different one — see `dibsAgentClasses`'s own comment
          // above. Legacy entries written before this fix (a bare
          // `agentClass` string, no `agentClasses` array) are still read
          // correctly by `readAgentClasses()` below — see its doc comment.
          agentClasses: dibsAgentClasses,
        },
        { livenessThresholdMs: DEFAULT_LIVENESS_THRESHOLD_MS },
      );
    } catch (error) {
      dibsDeclared = false;
      warnCoordinationDegraded(
        error,
        'coordination file',
        "this beat's dibs declaration was not persisted; using the last known on-disk view",
      );
    }
  }

  if (dibsDeclared) {
    try {
      dibs = await readDibs(coordinationFilePath, now, { livenessThresholdMs: DEFAULT_LIVENESS_THRESHOLD_MS });
    } catch (error) {
      warnCoordinationDegraded(
        error,
        'coordination file',
        "could not re-read dibs after declaring; synthesising this orchestrator's own entry",
      );
      dibs = existingEntries.some((entry) => entry.orchestratorId === orchestratorId)
        ? existingEntries
        : [
            ...existingEntries,
            // review, Low finding — carries `agentClasses` too, matching
            // the shape `declareDibs` just wrote (and the same field the
            // `--evict-pid` read-back-failure fallback was fixed to include).
            // Without it, this beat's OWN in-memory `dibs` view of itself
            // would silently drop the classes just declared, understating this
            // beat's own contribution to the memory-projection sum for the
            // remainder of THIS beat only (never persisted — the next beat's
            // fresh `readDibs` is unaffected).
            { orchestratorId, desiredAgents: dibsDesiredAgents, declaredAt: now, firstDeclaredAt, agentClasses: dibsAgentClasses },
          ];
    }
  }

  // Phase 4 — the unmaskable pressure-WARN admission pre-check. Reads
  // the RAW `pressureLevel` straight off this beat's sample and decides
  // AFTER axis classification/dibs declaration (so hysteresis history and
  // this orchestrator's dibs liveness/priority stay current across a
  // sustained pressure episode — see the "why AFTER dibs, not before" note
  // below) but strictly BEFORE the spawn-rate bucket, the live-agent
  // ceiling, or `buildTrafficLight` ever run, so that:
  //   - it is genuinely independent of (not derived from, not folded inside)
  //     `buildTrafficLight`'s own memoryState/diskState precedence chain,
  //     and therefore cannot be masked by disk-RED's `alert` short-circuit
  //     the way a memory-RED `pause` verdict already can be (Investigation
  //     R2 / its residual risk — see DEFAULT_PRESSURE_OVERRIDE_THRESHOLDS'
  //     own doc comment above);
  //   - a blocked beat never draws the global spawn-rate bucket and never
  //     attempts the live-agent ceiling's atomic ledger claim — there is
  //     nothing to admit, so none of THAT shared, ADMISSION-GRANTING state
  //     is touched or wasted-work-computed for a beat that was never going
  //     to be granted anything regardless of what it found.
  //
  // Why AFTER dibs declaration, not before (post-review fix — the original
  // Phase 4 draft returned before `declareDibs` ran at all): dibs
  // declaration also persists `memoryHistory`/`diskHistory` (the swap-trend
  // rolling window and consecutive-GREEN/wall-clock hysteresis counters —
  //  Phase 1) and refreshes this orchestrator's own liveness
  // timestamp/`firstDeclaredAt` priority. Returning BEFORE it, as the
  // original draft did, would freeze that history for the ENTIRE duration
  // of a sustained pressure episode — exactly the scenario this ticket
  // targets — so hysteresis recovery would be judged against pre-episode
  // data once pressure finally drops, and this orchestrator's dibs entry
  // would age past `DEFAULT_LIVENESS_THRESHOLD_MS` and be pruned, losing its
  // `firstDeclaredAt` priority the moment other orchestrators' beats are
  // genuinely contending again. `declareDibs`/`readDibs` themselves do not
  // GRANT anything (they only declare intent and persist history/liveness),
  // so running them before this pre-check does not reopen the "wasted
  // admission-granting work" concern above — only the spawn-bucket/
  // live-agent-ceiling claims (both genuinely admission-granting) stay
  // gated behind this check.
  //
  // `pressureLevel` is intentionally read as a raw, possibly-non-finite
  // sample field (mirrors every other raw-sample field elsewhere in this
  // function) — a missing/non-finite reading fails OPEN on this check alone
  // (falls through to the normal pipeline below, which still independently
  // fails closed via its own existing stale-sample/AMBER degradation), since
  // there is no "unknown pressure" literal to report and guessing WARN/
  // CRITICAL from an absent reading would be fabricating a signal, not
  // reporting one.
  const pressureLevel = rawSample.memory?.pressureLevel;

  // Phase 3 — this beat is the ONE place in cli.mjs that both READS
  // the persisted AIMD ceiling state, COMPUTES its next value via the pure
  // `computeAimdCeiling` (Phase 2), and WRITES that result back — atomically,
  // via `advanceAimdCeilingState` (see that function's own doc comment,
  // ./lib/coordination-file.mjs, for why this is ONE locked read-modify-write
  // rather than a separate `readAimdCeilingState` + `writeAimdCeilingState`
  // pair). Every OTHER call site in this file
  // (`probeAvailableCapacity`'s `--claim` probe, `--dequeue-if-capacity`) is
  // a one-shot, non-recurring invocation that only ever READS the current
  // value — advancing the AIMD state machine from more than one place would
  // mean two independent, un-coordinated writers racing to decide "what
  // happened this beat", which no single one of those one-shot call sites
  // has enough context to answer (they don't know whether THIS beat's own
  // pressure sample represents a fresh observation or a stale replay). This
  // `--desired-agents` beat already samples pressure exactly once per beat
  // and drives every other downstream admission decision from that SAME
  // sample (`pressureLevel`, above) — computing and persisting the AIMD
  // ceiling from that identical sample is a natural extension of, not a new
  // beat added alongside, that existing responsibility.
  //
  // Deliberately computed and persisted BEFORE the WARN/CRITICAL
  // pressure-block short-circuit below, not after: a WARN beat must still
  // drive the multiplicative decrease (so a CONCURRENT orchestrator's own
  // beat observes the backed-off ceiling on its very next read), even
  // though THIS beat itself never reaches admission. Persisting the
  // decrease only on beats that go on to attempt admission would mean an
  // isolated WARN beat with nothing else contending never backs the shared
  // ceiling off at all — the opposite of what AIMD's multiplicative-decrease
  // half exists for.
  //
  // Reused (never re-read) below: `allowanceConfig`'s advisory
  // `maxAgentsMemoryAxis`/`maxAgentsDiskAxis` (site 3) and the
  // `reserveAdmission` call further down (site 4) both consume THIS SAME
  // `aimdCeiling` value — not two independent `readAimdCeilingState` calls —
  // so the two can never disagree about what the ceiling was for this beat,
  // even under concurrent writers on other orchestrators' own beats.
  //
  // Fails CLOSED to the flat `LIVE_AGENT_CEILING`, matching every other
  // coordination-file failure mode in this function: a coordination error
  // here cannot prove any AIMD-adjusted value is safe to use, so this beat
  // falls back to the same conservative constant it replaced rather than
  // silently granting an unbounded/undefined ceiling.
  //
  // Gated on `!isHeartbeat` (`isHeartbeat` is resolved earlier in this
  // function, from `heartbeatValidation.isSet`) — mirrors the SAME
  // `!isHeartbeat` guard the global spawn-rate bucket and the live-agent
  // ceiling claim further below already use, and for the same reason: a
  // `--heartbeat` beat carries ZERO real spawn intent (documented in
  // SKILL.md; `--desired-agents` defaults to `1` and is informational-only
  // on a heartbeat), so it must leave every piece of shared, ADMISSION-
  // GOVERNING coordination-file state exactly as it found it — see the
  // dedicated regression test for this in ./cli.jest.spec.mjs ("repeated
  // --heartbeat invocations never drain the shared global spawn-rate
  // bucket"). Advancing the shared AIMD counter/ceiling on every heartbeat
  // tick (heartbeats typically poll far more often than real spawn-decision
  // beats — see `DEFAULT_HEARTBEAT_INTERVAL_MS`) would let a purely idle
  // polling timer alone additively grow the ceiling, or multiplicatively
  // shrink it on a transient WARN blip nothing was ever going to admit
  // against — neither is a beat that "happened" from this mechanism's own
  // point of view. (`!isHeartbeat` alone is not sufficient, though — see the
  // `canAdvanceAimdCeiling` comment further below for the additional
  // finite-`pressureLevel` requirement.)
  // `advanceAimdCeilingState` (./lib/coordination-file.mjs) performs the
  // read, `computeAimdCeiling` invocation, and write-back as ONE atomic
  // `withLock` critical section — not three separately-locked steps — so
  // two beats racing this same coordination file can never interleave a
  // stale read from one with a fresher write from the other (see that
  // function's own doc comment for the lost-update this closes; found via
  // this exact `--desired-agents` beat's own concurrent-race tests).
  // Advancing the AIMD state machine additionally requires a CONFIRMED,
  // finite `pressureLevel` reading on THIS beat — not merely `!isHeartbeat`.
  // `pressureLevel` is deliberately read above (see the long comment on that
  // assignment) as a raw, possibly-`null`/`undefined`/non-finite field, and
  // the WARN/CRITICAL admission-refusal check just below is correct to fail
  // OPEN on that absence (a stale/degraded sample must not fabricate a
  // refusal it can't justify). But `computeAimdCeiling` (./lib/aimd-ceiling.mjs)
  // only branches on `pressureLevel >= 2` to decide "not Normal, decrease" —
  // `null` and `NaN` both fail that comparison exactly the same way a
  // genuinely-healthy `pressureLevel: 1` reading would, so a stale/degraded
  // sample would silently fall into the SAME branch as a CONFIRMED-Normal
  // one and count toward the additive increase's `sustainedNormalCount`.
  // That is precisely the ambiguity `staleMemoryMarker()` in ./lib/recon.mjs
  // exists to prevent — its own doc comment explains `pressureLevel: null`
  // is set specifically so "downstream classifiers can't mistake it for a
  // real sample" — and AIMD's whole asymmetric-safety design
  // depends on ceiling GROWTH requiring confirmed sustained-Normal pressure,
  // not merely "absence of evidence of WARN". Because this ceiling state is
  // persisted and shared across every orchestrator on the host, letting one
  // orchestrator's stale/degraded samples advance it would silently raise
  // the ceiling every OTHER orchestrator admits against. So a non-finite
  // `pressureLevel` on an otherwise-real beat is treated the same as a
  // heartbeat for THIS purpose only: read-only, `sustainedNormalCount`/
  // `ceiling` left exactly as found. (This does NOT change the WARN/CRITICAL
  // admission-refusal check below, which correctly keeps failing open on a
  // non-finite `pressureLevel` for the reasons documented above it — only
  // the AIMD ceiling ADVANCE path changes here.)
  // `--advise-only` is added to the same exclusion for the same reason a
  // heartbeat is: an advisory run is not a beat that "happened" from the AIMD
  // state machine's point of view, and letting a PreToolUse hook's consultation
  // multiplicatively shrink (or additively grow) the ceiling every other
  // orchestrator on the host admits against would be steering shared policy
  // from a read-only path. The `else` branch below already reports the current
  // persisted ceiling via the READ-ONLY `readAimdCeilingState`, so an advisory
  // run still classifies against the real, live ceiling.
  const canAdvanceAimdCeiling = !isHeartbeat && !isAdviseOnly && Number.isFinite(pressureLevel);

  let aimdCeiling = LIVE_AGENT_CEILING;
  if (canAdvanceAimdCeiling) {
    try {
      const nextAimdCeilingState = await advanceAimdCeilingState(
        coordinationFilePath,
        (currentState) => computeAimdCeiling({ pressureLevel, ...currentState }, DEFAULT_AIMD_CEILING_CONFIG),
        { now },
      );
      aimdCeiling = nextAimdCeilingState.ceiling;
    } catch (error) {
      warnCoordinationDegraded(
        error,
        'coordination file',
        `falling back to the flat LIVE_AGENT_CEILING (${LIVE_AGENT_CEILING}) rather than assuming an AIMD-adjusted ceiling was safely read or computed`,
      );
      aimdCeiling = LIVE_AGENT_CEILING;
    }
  } else {
    // Read-only branch — covers both a `--heartbeat` beat (no real spawn
    // intent) and a real beat whose `pressureLevel` came back stale/unknown
    // (see `canAdvanceAimdCeiling` above): neither may ADVANCE the AIMD
    // state machine, so `sustainedNormalCount`/`ceiling` must be left
    // exactly as found. Both must still REPORT the actual current ceiling,
    // though: the `allowance` field written to stdout below is read
    // informationally on every beat, and once any real AIMD adjustment has
    // happened, hardcoding the flat `LIVE_AGENT_CEILING` here would make
    // this beat's reported allowance silently diverge from the real
    // persisted value. `readAimdCeilingState` never ADVANCES the state
    // machine, so calling it here does not reopen the "read-only beats never
    // advance AIMD state" invariant above.
    //
    // It is not, however, unconditionally write-free (Phase 3, second
    // review round). By default it lazily SEEDS `DEFAULT_AIMD_CEILING_STATE`
    // — under the shared coordination lock — when the reserved row is absent
    // (cold start) or malformed. For a `--heartbeat` beat that is fine and
    // wanted: a heartbeat already writes. For `--advise-only` it was a real
    // defect on two counts (a write on a path documented to make none, and a
    // ~10-20s default lock budget behind a hook with a 5s timeout), so the
    // advisory run passes `persistColdStart: false` and takes that function's
    // lock-free, in-memory-default branch instead. The VALUE returned is
    // identical either way; only the byte on disk differs.
    try {
      const currentAimdCeilingState = await readAimdCeilingState(
        coordinationFilePath,
        { now },
        isAdviseOnly ? { persistColdStart: false } : {},
      );
      aimdCeiling = currentAimdCeilingState.ceiling;
    } catch (error) {
      warnCoordinationDegraded(
        error,
        'coordination file',
        `falling back to the flat LIVE_AGENT_CEILING (${LIVE_AGENT_CEILING}) rather than assuming an AIMD-adjusted ceiling was safely read`,
      );
      aimdCeiling = LIVE_AGENT_CEILING;
    }
  }

  // Phase 3 — the disk axis's AMBER cleanup response, evaluated HERE:
  // after this beat's disk evidence exists (`diskTrend` / `rawSample.disk`),
  // and BEFORE the first of the three emission sites that can return. All
  // three attach `cleanup` — the pressure-block and memory-projection-block
  // short-circuits are precisely the degraded states in which reclaiming a
  // stale `cdk.out-*` matters most, and neither is a reason to skip it.
  //
  // Never influences the verdict. `cleanupReport` is attached additively and
  // is read by no gate, no axis, and no admission decision below — a failed or
  // refused sweep changes the beat's `type` not at all.
  const cleanupReport = await maybeSweepStaleTempRoot({
    coordinationFilePath,
    // The BEAT clock, for the cooldown only — never for the sweep's own
    // staleness arithmetic, which compares against real file mtimes. See the
    // `now: Date.now()` comment inside the sweep call.
    now,
    // Read BEFORE `declareDibs` — see that read's comment for why the ordering
    // is the gate, not merely a micro-optimisation.
    diskCleanupArmed,
    isAdviseOnly,
    diskTrend,
    freeDiskGb: rawSample.disk?.freeDiskGb,
  });
  const cleanupField = cleanupReport ? { cleanup: cleanupReport } : {};

  if (
    Number.isFinite(pressureLevel) &&
    pressureLevel >= DEFAULT_PRESSURE_OVERRIDE_THRESHOLDS.warnAtOrAbove
  ) {
    // `shedSignal` is EMISSION ONLY (Investigation R1 / — no executor
    // for `pause`/`pauseCandidate` exists today, so CRITICAL cannot mean
    // "reclaims memory", only "says so"). No code path anywhere in this file
    // sends a signal, suspends/resumes, checkpoints, or execs a process as a
    // result of this field — see doc-honesty.jest.spec.mjs's source-scan
    // assertion against this exact file for the mechanically-enforced
    // version of this claim.
    const shedSignal = pressureLevel >= DEFAULT_PRESSURE_OVERRIDE_THRESHOLDS.criticalAtOrAbove;
    process.stdout.write(
      JSON.stringify({
        type: 'pressure-block',
        pressureLevel,
        shedSignal,
        diskTrend,
        // Phase 3 — additive sibling of `diskTrend`, present only when
        // the trigger actually fired. Never influences `type`.
        ...cleanupField,
        // The atomic live-agent ledger claim is only ever attempted on a
        // beat that would otherwise reach `spawn-allowed` (see the
        // `liveAgentGrant` comment near the end of this function) — a
        // pressure-blocked beat never gets that far, exactly like today's
        // heartbeat/AMBER/RED beats.
        liveAgentGrant: null,
        loadAverage,
        compressedMb,
        leakBlocks,
        memory: { compressionVelocity },
      }) + '\n',
    );
    return;
  }

  // Per-orchestrator dibs-constrained allowance: reserved
  // pseudo-entries (e.g. the shared-machine-sample entry above) are filtered
  // out inside `computePerOrchestratorAllowance` itself — they're not real
  // orchestrators competing for spawn headroom. Uncontended (this
  // orchestrator is the only live real entry), it still receives the full
  // flat per-axis cap as headroom, unchanged from before; once a second
  // orchestrator has genuinely live dibs on the same coordination file, this
  // beat's reported allowance is bounded to this orchestrator's own share of
  // that cap.
  // Phase 2 (Build Plan Amendment, RC-3): this beat used to derive a
  // `memoryHeadroomCap` from THIS beat's own live `rawSample.memory.freeRamMb`
  // sample via `computeHeadroomCap` (Phase 5 Part A). RC-3 found
  // that `freeRamMb`/`ps`-derived RSS INVERTS under macOS memory-compressor
  // pressure — it can read as IMPROVING even as the host approaches freeze —
  // so it is disqualified as an admission input, live or absent (there is no
  // longer an `assumedFreeRamMb`-derived fallback formula to fall back to
  // either; that formula is retired along with the live path).
  //
  // Phase 3: the flat `DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis`/
  // `maxAgentsDiskAxis` (3/3) placeholder Phase 2 fell back to is now REPLACED
  // by `LIVE_AGENT_CEILING` (4) on BOTH axes — the ceiling is a single,
  // axis-independent cap on concurrently LIVE agents, not a per-resource one,
  // so there is no reason for the disk axis to keep enforcing its own,
  // now-redundant flat 3 once the real ceiling is in play (`computeAllowance`'s
  // `min(memoryAmount, diskAmount)` would otherwise silently re-impose the
  // retired flat disk cap as a second, tighter ceiling no one asked for).
  //
  // Phase 3 — `LIVE_AGENT_CEILING` above is now `aimdCeiling`, the
  // AIMD-adjusted value this beat already read/computed/persisted above
  // (before the pressure-block short-circuit) — see that block's own
  // comment for why this beat, and only this beat, owns advancing the AIMD
  // state machine.
  //
  // This is a fixed, dibs-fairness-only cap — it is NOT where the hard
  // ceiling is enforced. Dibs-based division (`computePerOrchestratorAllowance`
  // below) is advisory: it fairly shares a KNOWN, static number across
  // however many orchestrators declare real intent this beat, but it has no
  // way to see the live-agent ledger's actual current occupancy, and (see
  // `computePerOrchestratorAllowance`'s own doc comment) its "uncontended"
  // branch hands the FULL cap to whichever orchestrator's dibs read happens
  // to land while it is momentarily alone — a real race outcome across two
  // genuinely concurrent processes, not a bug specific to this ticket. The
  // ACTUAL hard, ledger-derived ceiling enforcement happens further below,
  // immediately after this beat's own `desiredAgents`/dibs/spawn-bucket
  // shares are known — see the `LIVE_AGENT_ADMISSION_CLAIM_TYPE` block's own
  // comment for why a real atomic claim (not just an advisory number here)
  // is required, and why it targets a SEPARATE claim type rather than
  // `LIVE_AGENT_CLAIM_TYPE` directly.
  const allowanceConfig = {
    maxAgentsMemoryAxis: aimdCeiling,
    maxAgentsDiskAxis: aimdCeiling,
  };

  const perOrchestratorAllowance = computePerOrchestratorAllowance(
    memoryResult.state,
    diskResult.state,
    dibs,
    allowanceConfig,
    orchestratorId,
  );

  // Global spawn-rate token bucket: consulted only on a beat
  // that could actually result in `spawn-allowed` — i.e. both axes GREEN AND
  // this orchestrator's own dibs-constrained `perOrchestratorAllowance` (computed
  // above) is non-zero. The bucket lives in the shared coordination file
  // (reserved `__global-spawn-rate-bucket__` entry), so it caps spawn RATE
  // across every orchestrator on the host, not just this one, but there is
  // nothing to cap on an AMBER/RED beat: `buildTrafficLight` below can only
  // ever produce `hold`/`pause` for either state, and the
  // `Math.min(trafficLight.allowance, spawnBucketGrantedCount)` clamp is
  // applied ONLY on the `spawn-allowed` branch further down — a token draw on
  // an AMBER/RED beat is pure waste, silently starving the shared bucket for
  // zero protective benefit. A sustained AMBER/RED period with a live
  // orchestrator still declaring non-zero `desiredAgents` per beat would
  // otherwise drain tokens that are needed the moment conditions recover to
  // GREEN. The same waste applies when both axes ARE green but dibs-division
  // has already bounded this orchestrator's own share to `0` (genuine
  // contention, pool exhausted by higher-priority orchestrators) — that beat
  // can't spawn anything regardless of what the bucket grants, so drawing
  // tokens for it only starves a genuinely-eligible orchestrator's next beat.
  //
  // A `--heartbeat` beat carries ZERO real spawn intent — `--desired-agents`
  // is documented (SKILL.md) as informational intent the CLI never grants or
  // denies, and the heartbeat example there omits it entirely, so it
  // defaults to `1`. Without this guard, a purely idle polling timer (see
  // DEFAULT_HEARTBEAT_INTERVAL_MS above) would silently draw down the real,
  // shared, host-wide bucket on every tick, starving genuine spawn beats for
  // no real spawning. A heartbeat therefore always requests 0 tokens
  // regardless of whatever `--desired-agents` was passed, and is granted a
  // full no-op count without even calling `consumeGlobalSpawnTokens`.
  //
  // Fails CLOSED since — see the rationale on the `catch` below, which
  // is the single statement of this posture. (This comment previously
  // described the opposite, fail-open behaviour as deliberate; it was in fact
  // a defect, and the two statements contradicted each other across 30 lines
  // of the same function.)
  const isSpawnEligibleBeat =
    memoryResult.state === 'GREEN' && diskResult.state === 'GREEN' && perOrchestratorAllowance > 0;

  // Phase 2 — per-agent-class projected-peak memory admission gate.
  //
  // Runs AFTER the pressure-override pre-check above (which must remain able
  // to block independently of this gate — regression-pinned by
  // ./cli-memory-projection-wiring.jest.spec.mjs test 3: a WARN/CRITICAL
  // pressure reading blocks via `pressure-block` even when the candidate
  // class is comfortably under this gate's own budget) and AFTER this beat's
  // own `declareDibs`/`readDibs` cycle further up (so this beat's own
  // `agentClasses` are already committed) — but BEFORE the global spawn-rate
  // bucket is ever drawn and BEFORE `reserveAdmission`'s atomic
  // live-agent-ledger claim runs. Refusing here, before either of those,
  // means a memory-projection refusal never draws down shared
  // admission-granting state for a beat that was never going to be admitted,
  // and — per the ticket's own Investigation/Convergence Analysis — never
  // has to unwind an already-granted live-agent claim, since none was ever
  // taken.
  //
  // Gated on the SAME `!isHeartbeat && isSpawnEligibleBeat` condition the
  // spawn-rate bucket and the live-agent ceiling below already use, for the
  // identical reason: a `--heartbeat` beat carries no real spawn intent, and
  // an AMBER/RED beat (or one whose own dibs-bounded share is already 0)
  // cannot reach `spawn-allowed` regardless of what this gate would decide,
  // so there is nothing here worth protecting by running it. Post-High-fix,
  // `agentClass` is ALWAYS a real string (an explicit `--agent-class` or
  // `UNLABELLED_AGENT_CLASS`), so there is no longer a `typeof agentClass ===
  // 'string'` escape hatch here — every spawn-eligible, non-heartbeat beat
  // goes through this gate.
  //
  // "Currently running classes" (Phase 0's own assumption #3, carried into
  // Phase 2) is approximated from the `liveDibs` snapshot `decide` below is
  // handed — every OTHER live orchestrator's declared `agentClasses` (this
  // beat's own dibs entry is excluded — the candidate `agentClass` and this
  // orchestrator's own `ownRunningAgentClasses` stand in for it explicitly;
  // reserved pseudo-entries, e.g. the shared-machine-sample cache, are never
  // real orchestrators and are excluded via `isReservedEntry`). Legacy
  // entries carrying only a singular `agentClass` field (declared before
  // Phase 2 review shipped) are read via `readAgentClasses()`'s
  // back-compat fallback — see its own doc comment.
  //
  // Lock design (Phase 2 review, Medium finding) — this gate now owns
  // its OWN `withLock` critical section via
  // `reconcileMemoryProjectionAdmission` (./lib/coordination-file.mjs),
  // rather than deriving correctness from an inductive proof over
  // `declareDibs`'s pre-existing lock ordering (that proof is documented,
  // and preserved as defense-in-depth reasoning, on
  // `reconcileMemoryProjectionAdmission`'s own doc comment, but this gate no
  // longer DEPENDS on it). That lock is acquired and fully released here,
  // strictly BEFORE `reserveAdmission`'s own `withLock` below is ever
  // reached — never nested inside it, never nesting it inside this one — so
  // there is no cross-lock deadlock risk to reason about; the two critical
  // sections simply never overlap in time.
  // ./cli-memory-projection-wiring.jest.spec.mjs test 7 pins exactly this —
  // sequential, bounded-time completion through both.
  // review, High finding — the gate must price what THIS beat can
  // actually cause to spawn, not a flat single candidate. `desiredAgents` can
  // be >1 (`--desired-agents=4`), and even when it is, `perOrchestratorAllowance`
  // (computed above from dibs/AIMD) may further bound how many of those this
  // orchestrator is actually eligible to draw this beat — the same
  // `Math.min(desiredAgents, perOrchestratorAllowance)` shape
  // `spawnBucketRequestAmount` below independently derives. Pricing only ONE
  // candidate while N are about to spawn under-projects the beat's real
  // memory impact by up to (N-1) agent-classes' worth of memory, silently
  // admitting fleets that blow the host budget. Floored at 1 so a
  // `perOrchestratorAllowance` of 0 (which never reaches this gate at all —
  // `isSpawnEligibleBeat` already excludes it) or a defensive 0 never yields
  // zero priced candidates for an otherwise-eligible beat.
  const candidateAgentCount = Math.max(1, Math.min(desiredAgents, perOrchestratorAllowance));

  // Phase 3 review — hoisted out of the `decide:` callback position (its
  // body is unchanged) so BOTH the ordinary locked path and the
  // `--advise-only` read-only path evaluate the IDENTICAL projection.
  // `reconcileMemoryProjectionAdmission` is a read-modify-write only in one
  // narrow case: on a REFUSAL it retracts the refused candidate class from
  // this orchestrator's own persisted entry. An advisory run has no persisted
  // candidacy to retract (it never declared one), so calling this function
  // directly against the dibs already read reproduces the block decision
  // exactly while performing no write at all.
  //
  // Phase 3, third review round, Medium #1 — the advise-only path's
  // OWN already-running fleet. `decideMemoryProjection` deliberately excludes
  // this orchestrator's self entry from `liveDibs` and substitutes
  // `ownRunningAgentClasses` for it, because on a real beat that flag is the
  // authoritative, just-declared set (it is counted even on the very first
  // beat that ever declares it). An ADVISORY caller, though, is not the
  // orchestrator: the PreToolUse gate consults on behalf of a session it is
  // not managing and cannot know that session's `--running-agent-classes`, so
  // it passes neither class flag. The substitution then replaced the self
  // entry with `[]` and priced this session's entire already-running fleet at
  // ZERO — turning a genuine `memory-projection-block` into `spawn-allowed`,
  // i.e. failing OPEN on the one axis this gate exists to enforce.
  //
  // On the advise-only path ONLY, and ONLY when `--running-agent-classes` was
  // not supplied at all, fall back to the self dibs entry's own declared
  // `agentClasses` — the classes this session's last REAL beat persisted, and
  // the best available statement of what it currently has running. Read from
  // `selfEntry`, which the advisory path has already read lock-free from the
  // ledger above: no new I/O, no lock, no write. An explicitly supplied
  // (even empty) `--running-agent-classes` still wins, and the non-advisory
  // path is bit-for-bit unchanged — `ownRunningAgentClasses` remains the sole
  // input there, including for `reconcileMemoryProjectionAdmission`'s
  // `knownRunningClasses` retraction filter, which must keep describing what
  // THIS beat declared rather than what a previous one persisted.
  //
  // STALENESS CLIFF (Phase 3, fourth review round, Medium): `selfEntry`
  // comes from a `readDibs` filtered on `DEFAULT_LIVENESS_THRESHOLD_MS`
  // (15 minutes). Once this session's own dibs row ages past that threshold
  // the row is pruned, `selfEntry` is `undefined`, and `readAgentClasses`
  // returns `[]` — so this fallback yields NOTHING and the memory-projection
  // axis reverts WHOLESALE to the pre-fix zero-cost-self-fleet behaviour for
  // that session until its next real beat refreshes the row. This is a full
  // reversion of the fix above, not a partial under-count: the same ledger
  // that reports `memory-projection-block` with a fresh row reports
  // `spawn-allowed` with a 20-minute-old one. Reading the self row UNPRUNED
  // here would close it, but that is a real trade-off (conservative on memory
  // vs. blocking a session whose fleet has genuinely finished, until its next
  // beat) and is recorded as an open, deliberately-undecided trade-off in
  // this design's "Amendment (2026-09-06,)" section, alongside the
  // spawn-rate/live-agent-ceiling residual.
  const projectionOwnRunningAgentClasses =
    isAdviseOnly && typeof flags['running-agent-classes'] !== 'string'
      ? readAgentClasses(selfEntry)
      : ownRunningAgentClasses;

  const decideMemoryProjection = async (liveDibs) => {
    const otherOrchestratorClasses = liveDibs
      .filter((entry) => !isReservedEntry(entry.orchestratorId) && entry.orchestratorId !== orchestratorId)
      .flatMap((entry) => readAgentClasses(entry));

    // This orchestrator's OWN other concurrently-running classes
    // (Phase 2 review, Medium finding) are read straight from
    // `ownRunningAgentClasses` — the authoritative, just-parsed
    // `--running-agent-classes` value — rather than from `liveDibs`'s
    // own (excluded) self entry, so they are counted even on the
    // very first beat that ever declares them. On the advise-only
    // path with no such flag supplied, `projectionOwnRunningAgentClasses`
    // substitutes the self entry's own persisted `agentClasses`
    // instead of `[]` — see its definition above.
    const runningClasses = [...otherOrchestratorClasses, ...projectionOwnRunningAgentClasses];

    // De-duplicated: `readHistory` is looked up once per DISTINCT class
    // needed, not once per running agent of that class — several running
    // agents of the SAME class share one history lookup, matching
    // `computeProjectedPeakMemoryMb`'s own contract (it separately re-prices
    // each entry in `runningClasses`, duplicates included, from this shared
    // per-class history map). `classesNeeded` never depends on how many
    // candidate copies the decrement search below prices — every candidate
    // count only ever repeats the SAME `agentClass` string, already folded
    // into this set once — so it is safe (and required, per its "no
    // extra `readHistory` calls" constraint) to compute this, and read
    // history for it, exactly ONCE, before the search starts.
    const classesNeeded = Array.from(new Set([...runningClasses, agentClass]));

    const historyFilePath = resolveHistoryFilePath();
    // review, Low finding, security — `Object.create(null)`,
    // not `{}`: `classesNeeded` entries come straight from
    // caller-supplied `--agent-class`/`--running-agent-classes`
    // strings. A plain object literal lets a class literally named
    // `__proto__` be assigned as a property SETTER on the object's
    // prototype rather than an own property, and `constructor`/
    // `toString`/etc. as class names would otherwise resolve to
    // inherited `Object.prototype` members instead of `undefined`.
    // A null-prototype object has no such inherited surface.
    const historyByClass = Object.create(null);
    // Sequential, not `Promise.all` — deliberate, not an oversight.
    // `readHistory`'s own per-process store cache (./lib/history.mjs)
    // means only the FIRST of these calls within this beat ever
    // genuinely reads the history file from disk; every subsequent
    // call in this loop, for a different class, is served from that
    // cache — see ./cli-memory-projection-wiring.jest.spec.mjs test
    // 6, which pins "at most one additional history-file read per
    // beat" even when several distinct running classes are summed
    // alongside the candidate.
    for (const classNeeded of classesNeeded) {
      // eslint-disable-next-line no-await-in-loop -- sequential is deliberate here (see comment above), not a missed optimization.
      historyByClass[classNeeded] = await readHistory(historyFilePath, classNeeded);
    }

    // Phase 1 — decrement search. A `--desired-agents=N` beat whose
    // FULL `candidateAgentCount` projection exceeds budget may still have a
    // smaller, genuinely feasible count (1..candidateAgentCount-1) that
    // fits comfortably — refusing the whole beat in that case under-admits.
    // Search downward from `candidateAgentCount` to `1`, reusing the SAME
    // `runningClasses`/`historyByClass` computed once above for every
    // candidate count tried (no extra history reads — see the comment on
    // `classesNeeded`). Stops at the largest count whose projection is
    // within budget; if even 1 does not fit, returns that count=1 attempt's
    // (real, non-null) totals with `withinBudget: false`.
    let lastAttempt;
    for (let count = candidateAgentCount; count >= 1; count -= 1) {
      // review, High finding — the candidate is repeated `count`
      // times, not always exactly once, so a `--desired-agents=N` beat's
      // search prices all `count` about-to-spawn agents of `agentClass` at
      // each step, not just the first. `computeProjectedPeakMemoryMb`
      // always adds its own `candidateClass` argument on top of
      // `runningClasses`, so only `count - 1` extra copies belong here.
      const candidateRunningClasses = Array(Math.max(0, count - 1)).fill(agentClass);
      const projectedRunningClasses = [...runningClasses, ...candidateRunningClasses];
      const attempt = computeProjectedPeakMemoryMb(projectedRunningClasses, agentClass, historyByClass);
      lastAttempt = attempt;
      if (attempt.withinBudget) {
        // Fully-fitting beat (count === candidateAgentCount): return the
        // projection UNCHANGED — no `memoryProjectionAdmittedCount` key at
        // all, so a fully-admitted beat's JSON output stays byte-for-byte
        // identical to before this search existed (Phase 1 test 3).
        return count === candidateAgentCount ? attempt : { ...attempt, memoryProjectionAdmittedCount: count };
      }
    }
    // Nothing fit, not even 1 — refuse with the count=1 attempt's own real
    // totals (never null/undefined from an aborted search).
    return { ...lastAttempt, memoryProjectionAdmittedCount: 0 };
  };

  let memoryProjection;
  if (!isHeartbeat && isSpawnEligibleBeat) {
    try {
      memoryProjection = isAdviseOnly
        ? await decideMemoryProjection(dibs)
        : await reconcileMemoryProjectionAdmission(
        coordinationFilePath,
        {
          orchestratorId,
          now,
          candidateClass: agentClass,
          // review, High finding — the same class name genuinely
          // running elsewhere under THIS orchestrator (declared via
          // `--running-agent-classes`) must survive a refusal's retraction
          // even though it is also this beat's `candidateClass` — see
          // `reconcileMemoryProjectionAdmission`'s own doc comment for why a
          // bare `!== candidateClass` filter alone cannot distinguish "the
          // refused candidate" from "an already-running agent of the exact
          // same class" once both collapse into one deduplicated class-name
          // entry.
          knownRunningClasses: ownRunningAgentClasses,
          decide: decideMemoryProjection,
        },
        { livenessThresholdMs: DEFAULT_LIVENESS_THRESHOLD_MS },
      );
    } catch (error) {
      // A coordination-file/history-read failure cannot PROVE the projected
      // sum fits the budget — fail CLOSED to "block", matching every other
      // coordination/history failure mode in this function (the spawn-rate
      // bucket and the live-agent ceiling both fail closed to a denial on
      // their own coordination-file errors, immediately below): a gate that
      // cannot evaluate its own input must not silently behave as though it
      // had passed.
      warnCoordinationDegraded(
        error,
        'coordination/history file',
        "blocking this beat's admission rather than assuming the memory-projection budget was satisfied",
      );
      memoryProjection = { withinBudget: false, totalProjectedMemoryMb: null, budgetMemoryMb: null };
    }

    if (!memoryProjection.withinBudget) {
      process.stdout.write(
        JSON.stringify({
          type: 'memory-projection-block',
          totalProjectedMemoryMb: memoryProjection.totalProjectedMemoryMb,
          budgetMemoryMb: memoryProjection.budgetMemoryMb,
          diskTrend,
          // Phase 3 — see the identical note on 'pressure-block' above.
          ...cleanupField,
          // Mirrors 'pressure-block''s own convention: a beat halted before
          // ever reaching `reserveAdmission` never attempted (or reports) a
          // real live-agent ledger claim.
          liveAgentGrant: null,
          loadAverage,
          compressedMb,
          leakBlocks,
          memory: { compressionVelocity },
        }) + '\n',
      );
      return;
    }
  }

  // The global bucket must only ever be asked for what this orchestrator is
  // actually permitted to use THIS beat — `perOrchestratorAllowance`, not the
  // raw, unbounded `desiredAgents` ask. Drawing the full `desiredAgents`
  // regardless of a smaller dibs-bounded share either over-draws the shared
  // bucket (e.g. share=1 but desiredAgents=3 would still draw 3 tokens) or
  // gets denied outright via "deny-the-whole-burst" semantics even when the
  // smaller, actually-usable amount was available (e.g. share=1, only 2
  // tokens left — 1 would succeed, 3 does not).
  //
  // Phase 1 — a THIRD clamp source, composed alongside the two above,
  // not substituted for them: `memoryProjectionAdmittedCount`, when the
  // memory-projection decrement search above admitted fewer than the full
  // `candidateAgentCount` it searched from. A beat that never reached the
  // search (heartbeat/non-spawn-eligible) or that fit fully at
  // `candidateAgentCount` has no such field on `memoryProjection` — the `??
  // candidateAgentCount` fallback makes this `Math.min` a no-op in that case,
  // matching today's two-source clamp exactly.
  const memoryProjectionAdmittedCount = memoryProjection?.memoryProjectionAdmittedCount;
  const spawnBucketRequestAmount = Math.min(
    desiredAgents,
    perOrchestratorAllowance,
    memoryProjectionAdmittedCount ?? candidateAgentCount,
  );

  // A heartbeat, any AMBER/RED beat, or a beat whose own dibs-bounded
  // allowance is already 0, is a full/no-op grant — `Infinity` guarantees the
  // later `Math.min(trafficLight.allowance, spawnBucketGrantedCount)` never
  // clamps anything down on a beat that either carries zero real spawn intent
  // (heartbeat), can't reach `spawn-allowed` in the first place (AMBER/RED),
  // or couldn't spawn anything even if it did (zero allowance); none of those
  // cases ever consult, and therefore never draw down, the real bucket.
  // `--advise-only` joins `isHeartbeat` in the "never draws the bucket"
  // exclusion. A token bucket cannot be probed without taking from it, so an
  // advisory run must not consult it at all rather than consume a token for a
  // spawn it is not itself performing — the double-draw (once by the hook's
  // consultation, once by the orchestrator's real beat) was one of the
  // concrete defects `--advise-only` exists to fix. The `Infinity` sentinel
  // makes the downstream `Math.min(trafficLight.allowance, …)` clamp a no-op,
  // and `spawnBucketCheckPerformed` below stays false so no denial is inferred
  // from a gate that was never consulted.
  const consultsSpawnBucket = !isHeartbeat && !isAdviseOnly && isSpawnEligibleBeat;
  let spawnBucketGrantedCount = consultsSpawnBucket ? spawnBucketRequestAmount : Infinity;
  if (consultsSpawnBucket) {
    try {
      const spawnBucketResult = await consumeGlobalSpawnTokens(
        coordinationFilePath,
        { requestedCount: spawnBucketRequestAmount, config: resolveSpawnBucketConfig(), now },
        { livenessThresholdMs: DEFAULT_LIVENESS_THRESHOLD_MS },
      );
      spawnBucketGrantedCount = spawnBucketResult.grantedCount;
    } catch (error) {
      // Phase 6 — this catch used to leave `spawnBucketGrantedCount` at
      // its pre-seeded value: `spawnBucketRequestAmount`, the FULL ask. Any
      // coordination error therefore made the CLI behave as though the bucket
      // had granted everything requested, so the downstream
      // `Math.min(allowance, spawnBucketGrantedCount)` clamp could never
      // fire. That is silent over-admission of the machine-wide spawn-rate
      // limit, and it is a LIVE defect today — `LockTimeoutError` reaches it
      // right now, without any of this ticket's changes. Fixed here because
      // adding a mid-critical-section throw makes it more reachable, not
      // because this ticket introduced it.
      //
      // Fails CLOSED to 0, matching `--claim`/`--release`: a bucket that
      // cannot prove it debited anything must not be reported as having
      // granted anything. The `Infinity` sentinel on heartbeat and
      // non-spawn-eligible beats is untouched — those never consult the
      // bucket at all, so there is nothing there to fail closed about.
      warnCoordinationDegraded(
        error,
        'coordination file',
        "denying this beat's spawn-rate tokens rather than assuming the full request was granted",
      );
      spawnBucketGrantedCount = 0;
    }
  }

  // Phase 3 — the hard live-agent ceiling. Consulted under the SAME
  // guard as the spawn-rate bucket above (nothing to gate on a heartbeat, an
  // AMBER/RED beat, or a beat whose own dibs-bounded share is already 0), for
  // the same reason: none of those cases can result in `spawn-allowed`
  // admitting anything, so there is nothing to atomically reserve.
  //
  // Phase 3 — this used to be three separate lock cycles (a
  // zero-count `LIVE_AGENT_CLAIM_TYPE` snapshot read, an unconditional
  // `LIVE_AGENT_ADMISSION_CLAIM_TYPE` release, then a
  // `LIVE_AGENT_ADMISSION_CLAIM_TYPE` claim sized off the snapshot), which
  // left a window between cycles where a competing beat could observe and
  // act on intermediate state. `reserveAdmission` folds all three into ONE
  // `withLock` critical section — see its own doc comment in
  // `coordination-file.mjs` for the exact sequence and on-disk semantics
  // (snapshot read, own-hold replace, then grant against the snapshot minus
  // what other orchestrators hold).
  //
  // Deliberately `liveAgentClaimSize` —
  // `Math.min(spawnBucketRequestAmount, spawnBucketGrantedCount)` — NOT the
  // raw `spawnBucketRequestAmount` (the pre-grant ask): the global spawn-rate
  // bucket above is a SEPARATE, independent gate (a host-wide RATE limit vs.
  // this ledger's host-wide COUNT ceiling — see the "SEPARATE clamp" comment
  // below), and if the bucket already denied (or partially denied) this
  // beat's request, reserving the FULL, pre-denial ask against the live-agent
  // ledger anyway would be doubly wrong: it would report `liveAgentGrant` as
  // non-zero on a beat the bucket already refused (an orchestrator reading
  // `liveAgentGrant` per SKILL.md's own "read liveAgentGrant, not allowance"
  // guidance would then spawn agents the rate limiter explicitly said no
  // to), AND it would burn real ledger capacity — for the rest of this
  // freshness window — reserving slots for agents that were never going to
  // spawn, starving a genuinely-eligible concurrent orchestrator for no
  // reason. Sizing the claim to the SMALLER of the two gates' verdicts keeps
  // the ledger's actual occupancy honest to what can really spawn.
  //
  // This is also what makes two (or more) beats racing for the SAME last
  // slot(s) genuinely mutually exclusive: whichever's claim against this
  // ledger lands first (under the SAME `writeEntriesAtomic`/ fencing
  // lock `--claim`/`--release` already rely on) correctly starves the other,
  // regardless of what either beat's own advisory dibs computation reported
  // — dibs' own "uncontended" branch can (and under real concurrency, does)
  // hand the full flat cap to each of several racers independently; this
  // step is what turns that advisory number into a real, ledger-enforced
  // bound.
  const liveAgentClaimSize = Math.min(spawnBucketRequestAmount, spawnBucketGrantedCount);
  // Initial value is never observed as-is: on the `!isHeartbeat &&
  // isSpawnEligibleBeat` path it is unconditionally overwritten by the `if`
  // block immediately below (success at `liveAgentGrantedCount =
  // liveAgentAdmissionResult.granted`, or fail-closed to 0 in the `catch`);
  // on every other path `liveAgentClaimAttempted` below is false, so
  // `liveAgentGrant` reports `null` regardless of this value. `0` is a safe,
  // honest placeholder either way — a prior `Infinity` sentinel here was
  // dead code (review Low finding).
  let liveAgentGrantedCount = 0;
  // `--advise-only` never reserves. `reserveAdmission` GRANTS admission
  // capacity by writing a hold into the shared live-agent ledger — capacity an
  // advisory caller has no way to release, and which (as the hook's own
  // `--desired-agents=1` consultation demonstrably did) also RE-SIZES this
  // orchestrator's existing hold down to the advisory ask. An advisory run
  // therefore reports `liveAgentGrant: null` (via `liveAgentClaimAttempted`
  // below), the same value every other beat that never attempted a claim
  // reports.
  const attemptsLiveAgentClaim = !isHeartbeat && !isAdviseOnly && isSpawnEligibleBeat;
  if (attemptsLiveAgentClaim) {
    try {
      const liveAgentAdmissionResult = await reserveAdmission(
        coordinationFilePath,
        {
          snapshotType: LIVE_AGENT_CLAIM_TYPE,
          admissionType: LIVE_AGENT_ADMISSION_CLAIM_TYPE,
          requestedCount: liveAgentClaimSize,
          orchestratorId,
          // Same just-computed `aimdCeiling` `allowanceConfig` above used —
          // not a second, independent read — see this beat's own
          // compute-and-persist comment above for why.
          ceiling: aimdCeiling,
          now,
          // — ledger write call site 3 of 5. The SAME resolved identity
          // object call site 1 spread in, so both of this beat's ledger writes
          // carry byte-identical values. See the ledger-identity note near
          // `resolveNow`.
          ...beatLedgerIdentity,
        },
        { claimTtlMs: DEFAULT_FRESHNESS_WINDOW_MS, snapshotClaimTtlMs: DEFAULT_CLAIM_TTL_MS },
      );
      liveAgentGrantedCount = liveAgentAdmissionResult.granted;
    } catch (error) {
      // Fails CLOSED to 0, matching `--claim`/`--release`/the spawn-rate
      // bucket above: a coordination failure here cannot PROVE any live-agent
      // slot is genuinely free, so reporting the full ask as admitted would
      // be silent over-admission of the one gate this ticket exists to
      // install.
      warnCoordinationDegraded(
        error,
        'coordination file',
        "denying this beat's live-agent ceiling claim rather than assuming a slot was free",
      );
      liveAgentGrantedCount = 0;
    }
  }

  // Process-tree-based running-agent discovery for pause-candidate
  // selection, gated behind the same condition that gates the pause branch in
  // buildTrafficLight.
  //
  // pre-PR review, Medium — this comment used to justify the gate as
  // "shelling out to `ps` on every beat that can't reach `pause` would be pure
  // overhead". That is no longer a true statement about this file: its
  // `observeBeatProcesses()` takes a `ps` snapshot on EVERY ordinary beat,
  // ahead of the ledger sweep. So the gate no longer saves a beat its only
  // `ps` spawn; on a memory-RED beat it costs a SECOND one, plus a second
  // parse of the same table, on exactly the loaded host where `ps` is slowest.
  //
  // That duplication is accepted deliberately rather than collapsed into
  // `beatObservation.liveProcesses`, and the reason is the two-layer test-seam
  // model, not inertia: this call site reads `ARM_FAKE_PS_OUTPUT` behind
  // `ARM_COORDINATION_FILE` alone, while the beat snapshot reads it only behind
  // the `ARM_BEAT_SNAPSHOT_TEST_MODE` master switch. Sharing one snapshot would
  // let either path's test configuration decide the other's result — for the
  // sweep, that means deciding which records are DELETED. It would also anchor
  // pause-candidate `startedAt` on `snapshotAt` rather than `now`, and hand the
  // selector `null` (no candidate) on a degraded snapshot where it currently
  // still ranks. Both are behaviour changes that belong to a ticket that can
  // test them, not to this one. As of Phase
  // 4, disk-RED alone short-circuits to `alert` inside
  // buildTrafficLight *before* the memory-RED/disk-RED pause check ever
  // runs, so `pause` is only reachable when memoryState is RED (whether or
  // not diskState is also RED) — not "either axis RED" any more.
  //
  // Resolved residual waste (Phase 4 review, Low; threaded through by
  //): on the both-RED tie (memoryState RED AND diskState RED),
  // `buildTrafficLight` no longer discards `pauseCandidate` — it still
  // returns `type: 'alert'` (disk-RED wins the tie, unchanged), but now
  // enriches that shape with `memoryAlsoRed: true` and the REAL
  // `pauseCandidate` computed just above, so the `ps` shell-out's result is
  // surfaced to callers instead of thrown away. `memoryAlsoRed: false` marks
  // the disk-RED-only case, where `pauseCandidate` is genuinely absent from
  // the shape (there was nothing to compute). No change to the `ps`
  // shell-out's own gating — it still only runs on a memory-RED beat.
  // whole-branch review, High #2 — `selectPauseCandidateWithFootprint`
  // (./lib/pause-candidate.mjs) samples each candidate's REAL phys_footprint
  // (via `sampleFootprint`) before ranking, rather than handing
  // `listAgentProcesses`'s bare `{ agentId, rssMb, startedAt }` shape
  // straight to `selectPauseCandidate` — which, lacking a `physFootprintMb`
  // field on every real candidate, always fell back to RSS ranking in
  // practice. Per-candidate sampling failures degrade to
  // `physFootprintMb: null` for that candidate only (see that module's own
  // doc comment); a real `footprint`/`vmmap` shell-out per running agent is
  // real overhead, so this stays behind the exact same RED-memory gate the
  // pre-existing `ps` shell-out above was already gated behind.
  //
  // Phase 3, third review round, Medium #2 — `!isAdviseOnly`. This is
  // the single most expensive step on the whole beat: a SECOND `ps`
  // collection, then one `/usr/bin/footprint` (falling back to
  // `/usr/bin/vmmap`) shell-out PER running agent, each bounded at 2s and —
  // because `sampleFootprint` uses `execFileSync` inside its `Promise.all` —
  // effectively serialized. On a memory-RED host with a real fleet that
  // comfortably exceeds the PreToolUse hook's 5s `CLI_TIMEOUT_MS`, which
  // would manufacture a FAIL-CLOSED deny (and an `arm-gate-fail-closed`
  // liveness entry) out of a host state the advisory path already answers
  // correctly and cheaply. Every other expensive or side-effecting step on
  // this path is already `!isAdviseOnly`-gated; this one was the outlier.
  //
  // Behaviour-preserving for every consumer of an advisory verdict:
  // `buildTrafficLight` already returns `{ type: 'pause', pauseCandidate:
  // null }` verbatim for a null candidate (its documented, honest "nothing
  // to pause" shape — see this file's header), the verdict `type` is
  // unchanged, and the hook reads only `type`/`allowance`/`liveAgentGrant`,
  // treating `pause` as a POLICY deny either way. Naming a process to pause
  // is advice for the orchestrator that owns the fleet, and an advisory
  // consultation is not that caller. A REAL beat is unaffected and still
  // names a real candidate.
  const pauseCandidate =
    memoryResult.state === 'RED' && !isAdviseOnly
      ? await selectPauseCandidateWithFootprint(listAgentProcesses(collectPsOutput(), now))
      : null;

  const trafficLight = buildTrafficLight({
    memoryState: memoryResult.state,
    diskState: diskResult.state,
    diskReason: diskResult.reason,
    allowance: perOrchestratorAllowance,
    pauseCandidate,
  });

  // The bucket is a binary gate on a `spawn-allowed` beat, not a second value
  // that further shrinks an already-correct `allowance` down to whatever
  // (smaller) amount was merely REQUESTED from it. `spawnBucketRequestAmount`
  // is already `min(desiredAgents, perOrchestratorAllowance)` (see above) —
  // when the bucket grants that full request, `trafficLight.allowance` (the
  // dibs-divided headroom-or-share value `buildTrafficLight` already set) is
  // correct as-is and must be left untouched. Only a genuine DENIAL — the
  // bucket actually had less than what was asked and granted 0 — reduces the
  // reported allowance, and only ever down to 0 (deny-the-whole-burst
  // semantics mean there is no partial-grant case to reflect here).
  //
  // `spawnBucketCheckPerformed` mirrors the exact same guard used to decide
  // whether `consumeGlobalSpawnTokens` was even called above: a heartbeat, an
  // AMBER/RED beat, or a beat whose own dibs-bounded allowance is already 0
  // never drew the bucket at all, so there is nothing for it to have denied.
  const spawnBucketCheckPerformed = consultsSpawnBucket;
  const spawnDenied =
    spawnBucketCheckPerformed && spawnBucketGrantedCount === 0 && spawnBucketRequestAmount > 0;
  if (trafficLight.type === 'spawn-allowed' && spawnDenied) {
    trafficLight.allowance = 0;
  }

  // Phase 3 (Option B, owner-decided after review flagged a design
  // conflict — see the PR discussion): `allowance` keeps meaning exactly
  // what it has always meant, on every axis including this one — advisory
  // HEADROOM (dibs-share/cap minus current usage). Specifically, it is never
  // narrowed to reflect what was atomically GRANTED by the live-agent
  // ledger's real reservation (the property this Option B fix is actually
  // about) — this is DISTINCT from the pre-existing spawn-rate-bucket clamp
  // immediately above, which stays exactly as it was before this ticket: a
  // genuine bucket DENIAL is real, host-wide information about headroom
  // itself (the bucket says "you may have advisory dibs headroom, but 0
  // tokens are actually available right now"), not a narrowing to what one
  // beat happened to ask for — see that clamp's own comment for why it is
  // not the same class of narrowing this Option B fix exists to prevent.
  // Clamping `allowance` to the live-agent ledger's grant (the original
  // Phase 3 draft) silently narrowed the CONTRACT for just the live-agent
  // axis, breaking the pinned "headroom, not a target" regression test
  // (cli.jest.spec.mjs — the Medium fix) and SKILL.md's flagship
  // worked example, both of which predate this ticket and are not being
  // redefined by it.
  //
  // `liveAgentGrant` is therefore a SEPARATE, sibling field — same shape as
  // `diskTrend` below (a fact `cli.mjs` attaches to the traffic-light JSON,
  // not a value `buildTrafficLight` itself produces) — reporting what this
  // beat ACTUALLY, atomically reserved against the live-agent ledger: the
  // number of live-agent slots this orchestrator is really granted
  // permission to spawn right now. This is the operational number —
  // orchestrators that want to know how many agents they may actually start
  // this beat must read `liveAgentGrant`, not `allowance`.
  //
  // `null` means "the atomic claim was never attempted this beat" — exactly
  // the beats whose `liveAgentGrantedCount` was left at its initial `0`
  // above without ever entering the claim `if` block (heartbeat, AMBER/RED,
  // or a beat whose own dibs-bounded allowance is already 0): none of those
  // can reach `spawn-allowed` with non-zero headroom in the first place, so
  // there is nothing to report a real reservation for. `isSpawnEligibleBeat`
  // (memoryState/diskState both GREEN and `perOrchestratorAllowance > 0`)
  // guarantees `trafficLight.type` is `'spawn-allowed'` whenever the claim
  // WAS attempted, so `liveAgentGrant` is only ever non-null on that shape.
  //
  // ONE DELIBERATE EXCEPTION — the `--user-attests-idle` idle-probe admission
  // below: an admitted idle-probe beat also reaches
  // `trafficLight.type === 'spawn-allowed'`, but `isSpawnEligibleBeat` is
  // false there by construction (it fires only on an AMBER `hold` beat, and
  // `isSpawnEligibleBeat` requires memoryState/diskState both GREEN), so
  // `attemptsLiveAgentClaim`/`liveAgentClaimAttempted` are both false and
  // `liveAgentGrant` stays `null` on that beat — it is NOT set to `1` to
  // match `trafficLight.allowance`. This is intentional, not an oversight:
  // the idle-probe's single-slot admission is claimed against
  // `IDLE_PROBE_CLAIM_TYPE`, a dedicated ledger entirely separate from
  // `LIVE_AGENT_ADMISSION_CLAIM_TYPE`/`reserveAdmission` and NOT counted
  // against `LIVE_AGENT_CEILING` — the Build Plan's own Phase 3 edge case is
  // explicit that idle-probe admission must not be treated as a normal agent
  // class for AIMD-ceiling purposes. Folding the idle-probe's grant into
  // `liveAgentGrant` would silently conflate the two ledgers and break the
  // load-bearing invariant several existing specs pin — that summing
  // `liveAgentGrant` across any concurrently-racing cohort of beats can never
  // exceed the real, ledger-enforced live-agent ceiling (see
  // outer-acceptance.jest.spec.mjs's "REAL, atomically-reserved
  // `liveAgentGrant`" comment and live-agent-ceiling.jest.spec.mjs's header
  // comment) — an idle-probe grant folded in would inflate that sum with a
  // slot the AIMD-ceiling ledger never actually reserved.
  //
  // Consequently, `liveAgentGrant`/`allowance` are NOT the authoritative pair
  // to read on an idle-probe-tagged beat. A caller must check the sibling
  // `idleProbe` field FIRST: when present, exactly one idle-probe agent has
  // been admitted regardless of what `liveAgentGrant` reports (it will be
  // `null`) — read `trafficLight.type`/`allowance` (both already reported
  // above and unchanged by this comment) for that beat instead. Every other
  // admission path keeps the "read `liveAgentGrant`, not `allowance`"
  // convention unmodified.
  const liveAgentClaimAttempted = attemptsLiveAgentClaim;
  const liveAgentGrant = liveAgentClaimAttempted ? liveAgentGrantedCount : null;

  // `compressedMb`/`compressionVelocity` were already computed early,
  // alongside `loadAverage` above — see those declarations' doc comments for
  // why they're attached on every `process.stdout.write` call site in this
  // function, not just this one.

  // --user-attests-idle idle-probe admission.
  //
  // SAFETY: this block is reachable ONLY by falling all the way through the
  // pressure-block short-circuit above (`return`ed at line ~8035 whenever
  // `pressureLevel >= DEFAULT_PRESSURE_OVERRIDE_THRESHOLDS.warnAtOrAbove`) —
  // a `return` statement unconditionally unwinds `main()`'s call stack for
  // that beat, so no code appearing textually after it (this block included)
  // ever executes once that `return` has fired. At WARN/CRITICAL, execution
  // never reaches this line; `userAttestsIdle` is never read, `claimCapacity`
  // is never called, and the printed JSON is the bare `pressure-block` shape
  // with no `idleProbe` field, by construction, not by a value check here.
  //
  // Deliberately gated on more than just `trafficLight.type === 'hold'`
  // (pre-PR review fix, High — H1/H2/H3/M1). `buildTrafficLight`
  // returns `hold` for THREE distinct reasons, and the Build Plan's "safe to
  // attest idle" case is only the first of them:
  //   1. A genuine, known, below-WARN memory-AMBER reading — the case this
  //      mechanism is documented (SKILL.md, "Safety invariant — additive
  //      only, never a bypass") to act on.
  //   2. A STALE/unknown memory sample. `classifyMemoryRaw` forces AMBER
  //      whenever `rawSample.stale` or `pressureLevel`/`swapUsedMb` are
  //      non-finite (see `threshold.mjs`) — this is "we don't know", not
  //      "we know it's safe". The pressure-block short-circuit above only
  //      fires on a genuine numeric `pressureLevel >= warnAtOrAbove`, so an
  //      UNKNOWN reading (possibly truly at/above WARN) falls all the way
  //      through it untouched — the SAFETY comment's "reachable only by
  //      falling through the pressure-WARN block" claim does not hold for
  //      this case. Attesting idle here would admit a spawn on a host whose
  //      real pressure state was never actually observed.
  //   3. A disk-AMBER reading with memory GREEN. `--user-attests-idle` is
  //      documented as a MEMORY-axis assertion ("this host is genuinely
  //      idle" re: memory); it says nothing about disk headroom, and must
  //      never be read as clearing a disk hold.
  // `isGenuineMemoryAmberHold` narrows to exactly case 1: `hold` is present,
  // it was memory (not disk) that produced it, and the memory sample backing
  // that verdict was a real, finite, non-stale reading — never an "unknown,
  // defaulted-to-AMBER" one. A `spawn-allowed`/`pause`/`alert` verdict, a
  // disk-only hold, or a stale-sample hold are all left completely
  // untouched: this block never mutates any OTHER trafficLight.type, so the
  // flag has zero effect on any beat outside this narrowed case.
  // Also excludes `--heartbeat` (a liveness ping, not a spawn decision —
  // every other admission path in this file is `!isHeartbeat`-gated) and
  // `--advise-only` (a documented fully-non-mutating advisory read;
  // `claimCapacity` below is a real, mutating ledger write, exactly the kind
  // of side effect `--advise-only` promises never to perform — mirrors the
  // `!isAdviseOnly` guards already used by `attemptsLiveAgentClaim`/
  // `consultsSpawnBucket`/the AIMD advance above).
  // pre-PR review fix (Medium — M2): a refused second attestation
  // (a probe is already in flight, or the coordination file degraded) needs
  // to be distinguishable from "the flag was simply absent" — both
  // previously printed the identical bare `{ type: 'hold' }` shape. Set
  // below only on the refusal path; left `undefined` (and therefore omitted
  // from the printed JSON, same convention as `idleProbe`) whenever the flag
  // was never set, or the beat wasn't eligible for idle-probe admission in
  // the first place.
  const isGenuineMemoryAmberHold =
    trafficLight.type === 'hold' &&
    memoryResult.state === 'AMBER' &&
    !rawSample.stale &&
    Number.isFinite(pressureLevel) &&
    // `classifyMemoryRaw` forces the "unknown" AMBER path on EITHER a
    // non-finite `pressureLevel` OR a non-finite `swapUsedMb` (see
    // `threshold.mjs`) — both must be checked here, not just
    // `pressureLevel`, or a sample with a real pressureLevel but a missing/
    // unparseable `swapUsedMb` would slip through as "genuinely known".
    Number.isFinite(rawSample.memory?.swapUsedMb);
  let idleProbeRefused;
  let idleProbe;
  if (userAttestsIdle && !isHeartbeat && !isAdviseOnly && isGenuineMemoryAmberHold) {
    // Reuses the existing, already-tested `claimCapacity` atomic
    // deny-the-remainder ledger primitive (see "Claiming capacity" in
    // SKILL.md) against a fixed, single-slot capacity of 1 for a dedicated
    // `IDLE_PROBE_CLAIM_TYPE` operation type — this is exactly the
    // "single-probe-in-flight" gate the Build Plan calls for: a first
    // `--user-attests-idle` hold beat claims the one available slot and is
    // admitted; a second, concurrent one (from any orchestrator) finds the
    // slot already held and is refused. The claim is intentionally left
    // outstanding — there is no accompanying `--release` call here — so it
    // naturally expires after `DEFAULT_CLAIM_TTL_MS`, bounding how long a
    // probe that never explicitly releases its own slot can block a future
    // probe from being admitted.
    const idleProbeGrant = await claimCapacity(
      coordinationFilePath,
      {
        type: IDLE_PROBE_CLAIM_TYPE,
        requestedCount: 1,
        orchestratorId,
        computeAvailableCapacity: () => 1,
        now,
      },
      { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
    ).catch((error) => {
      warnCoordinationDegraded(
        error,
        'coordination file',
        'refusing to admit an idle-probe agent rather than assuming the single-probe-in-flight gate was safely checked',
      );
      return { granted: 0 };
    });
    if (idleProbeGrant.granted > 0) {
      trafficLight.type = 'spawn-allowed';
      trafficLight.allowance = 1;
      idleProbe = { agentClass: 'idle-probe' };
      // `liveAgentGrant` (computed above, before this block runs) is
      // deliberately left at `null` here — see its own doc comment for why
      // an idle-probe admission does not set it to `1`. A caller must branch
      // on the `idleProbe` field, not `liveAgentGrant`, for this beat.
    }
    // idleProbeGrant.granted === 0 (a probe is already in flight, or the
    // coordination file degraded) leaves `trafficLight` completely
    // untouched — the beat still reports `hold`, but now distinguishably
    // from the flag-absent case via `idleProbeRefused: true` below
    // (review fix, Medium).
    else {
      idleProbeRefused = true;
    }
  }

  process.stdout.write(
    JSON.stringify({
      ...trafficLight,
      diskTrend,
      // Phase 3 — additive sibling of `diskTrend` (the precedent),
      // present only when the trigger fired. Spread AFTER `...trafficLight` so
      // it can never be shadowed by a traffic-light field of the same name.
      ...cleanupField,
      liveAgentGrant,
      loadAverage,
      compressedMb,
      leakBlocks,
      memory: { compressionVelocity },
      ...(idleProbe ? { idleProbe } : {}),
      ...(idleProbeRefused ? { idleProbeRefused } : {}),
      // Phase 1 — present ONLY when the memory-projection decrement
      // search admitted fewer than the originally-requested
      // `candidateAgentCount`, so a fully-fitting beat's JSON output stays
      // byte-for-byte unchanged from before this search existed.
      ...(memoryProjectionAdmittedCount !== undefined ? { memoryProjectionAdmittedCount } : {}),
    }) + '\n',
  );
}

main().catch((error) => {
  process.stderr.write(`agent-resource-management: unexpected error: ${error.stack ?? error.message}\n`);
  process.exit(1);
});
