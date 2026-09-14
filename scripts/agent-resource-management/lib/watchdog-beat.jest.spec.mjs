// Phase 6 — watchdog beat wiring: composes Phase 1 (per-agent
// liveness), Phase 3 (deadlock tripwire), Phase 2 (queue-aging escalation),
// and Phase 4 (recycle wiring: resolveEvictionTargets/filterEvictable) into
// a single beat, logging every detection via Phase 5 (liveness-log).
//
// RED by construction: `./watchdog-beat.mjs` does not exist yet as of this
// commit — every test below fails at import time (`Cannot find module
// './watchdog-beat.mjs'`). Do not add an implementation to make this pass —
// the builder implements it in the next phase to turn this green.
//
// ---------------------------------------------------------------------------
// CONTRACT THIS SUITE SPECIFIES FOR THE IMPLEMENTER (this file IS the spec,
// mirroring ./queue-aging-escalation.jest.spec.mjs's/./eviction-targeting.
// jest.spec.mjs's own "spec file, not just a test file" idiom)
// ---------------------------------------------------------------------------
//
//   runWatchdogBeat({
//     now, boundMs, deadlockWindowMs, agingBoundMs,
//     candidates,               // Array<{ pid: number, footprintTrajectory: number[], worktreeMtimeMs: number|null }>
//     claimLedger,               // Array<{ pid, leaseId, orchestratorPid, orchestratorId, reentrant }>
//                                 // — see ./eviction-targeting.mjs's own header: the ONE trusted
//                                 // source for pid/leaseId/orchestratorPid/orchestratorId. Phase 6's
//                                 // CLI-level I/O wiring (cli.mjs, NOT this module) is responsible for
//                                 // assembling this from the real ps-tree scan + coordination-file dibs
//                                 // + queue lease data. This module never resolves it itself — no fs,
//                                 // no ps, no child_process below this line.
//     recentlyEvictedPids,       // Set<number> | Array<number> | undefined — threaded straight through
//                                 // to resolveEvictionTargets's own cooldown check.
//     coordinationFilePath, queueFilePath, deadlockStateFilePath, liveLogFilePath,
//     capacitySamples,           // threaded straight through to escalateAgedQueueEntry
//     getFootprintTrajectory,    // (orchestratorId) => number[] — threaded through to detectDeadlock;
//                                 // defaults to () => [] when omitted (matches detectDeadlock's own default)
//     getWorktreeMtimeMs,        // (orchestratorId) => number|null — same, defaults to () => null
//     handleEvict,               // REQUIRED: async (target) => handleEvictResult — target is one
//                                 // filterEvictable() entry ({ pid, leaseId, orchestratorPid,
//                                 // orchestratorId, action: 'evict' }). This module never calls the real
//                                 // --evict-pid/process.kill surface itself — that stays cli.mjs's job;
//                                 // this is the injection seam that makes the composition itself
//                                 // unit-testable without spawning a child process. Calling
//                                 // runWatchdogBeat with NO handleEvict and at least one evictable
//                                 // target is a caller error (throws synchronously, before any I/O) —
//                                 // this module refuses to silently skip a real recycle.
//     deps: {                    // ALL optional, each defaulting to the real import of the same name
//                                 // from its own module — present purely so this composition's own
//                                 // wiring/branching is unit-testable with fakes, per this file's own
//                                 // tests below. cli.mjs's production call site passes no `deps` at all.
//       isAgentStalled, detectDeadlock, escalateAgedQueueEntry,
//       resolveEvictionTargets, filterEvictable, classifyEvictionOutcome,
//       logWatchdogTrip, logAgingEscalation, logDeadlockRecycle,
//     },
//   }) -> Promise<{
//     type: 'watchdog',
//     trips: Array<{ pid: number, reason: 'stalled', evicted: boolean, outcome?: string }>,
//     deadlockTripped: boolean,
//     agingEscalations: Array<object>,   // escalateAgedQueueEntry's own `escalated` array, passed through
//     recycled: Array<number>,           // target.pid for every target (watchdog trip OR deadlock trip —
//                                          // both origins always carry a real OS pid; see
//                                          // eviction-targeting.mjs's buildResolvedTarget) that
//                                          // classifyEvictionOutcome resolved as `recycled: true`
//   }>
//
// COMPOSITION ORDER (pinned by this suite's assertions on call sequencing
// where it matters — see the ordering test below):
//   1. Per-candidate stall judgement (Phase 1, `deps.isAgentStalled`) over
//      `candidates` — pure, sync, no I/O.
//   2. Deadlock-window evaluation (Phase 3, `deps.detectDeadlock`) against
//      the real coordination/queue files at `coordinationFilePath`/
//      `queueFilePath`, persisting its own debounce state to
//      `deadlockStateFilePath`.
//   3. Queue-aging escalation (Phase 2, `deps.escalateAgedQueueEntry`)
//      against `queueFilePath`.
//   4. Recycle-target resolution (Phase 4, `deps.resolveEvictionTargets` +
//      `deps.filterEvictable`) over the Step-1 trips + Step-2 deadlock
//      result + `claimLedger`.
//   5. For each `action: 'evict'` target from Step 4, IN ARRAY ORDER: call
//      `handleEvict(target)`, classify its result (`deps.
//      classifyEvictionOutcome`), and log the detection (`deps.
//      logWatchdogTrip` for a target resolved from a Step-1 pid-based trip,
//      `deps.logDeadlockRecycle` for a target resolved from a Step-2
//      orchestratorId-based stalledPid — this module knows which because it
//      is the one that built the `watchdogTrips`/`deadlockTrip` inputs to
//      Step 4 in the first place). `alert-only`/`skip` targets from Step 4
//      are never passed to `handleEvict` at all.
//   6. Every `deps.escalateAgedQueueEntry` `escalated` entry from Step 3 is
//      logged via `deps.logAgingEscalation`, regardless of Steps 4-5's
//      outcome — aging escalation and recycling are independent concerns.
//
// FAILURE POSTURE: this module does NOT swallow a thrown error from any of
// the five composed modules — a throw from Step 1-5 propagates straight out
// of `runWatchdogBeat` uncaught. Per the Convergence Analysis note on this
// ticket, "the beat itself hangs/crashes mid-run must not corrupt the
// backoff-state file" is achieved by cli.mjs's OWN wrapper (Phase 6's CLI
// wiring, not this module) only computing/writing pacing state AFTER
// `runWatchdogBeat` resolves successfully — see
// ./watchdog-backoff.jest.spec.mjs for that half of the contract. This
// module has no backoff-state awareness of its own.
//
// ---------------------------------------------------------------------------
// Unit-level, fully hermetic: real temp-file fs for the coordination/queue/
// deadlock-state/liveness-log paths (mirroring every other lib/*.jest.spec.mjs
// in this directory — no mocked fs), but the five composed *functions*
// themselves are swapped for lightweight fakes via `deps` so this suite can
// assert on the COMPOSITION (call counts, argument shapes, output wiring)
// without needing every sub-module's own full contract satisfied end-to-end
// — each sub-module already has its own dedicated spec file for that.
// ---------------------------------------------------------------------------

import { jest } from '@jest/globals';

import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { runWatchdogBeat } from './watchdog-beat.mjs';
import { logWatchdogTrip as realLogWatchdogTrip } from './liveness-log.mjs';

let workDir;
let coordinationFilePath;
let queueFilePath;
let deadlockStateFilePath;
let liveLogFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-watchdog-beat-'));
  coordinationFilePath = join(workDir, 'coordination.json');
  queueFilePath = join(workDir, 'queue.json');
  deadlockStateFilePath = join(workDir, 'deadlock-state.json');
  liveLogFilePath = join(workDir, 'liveness-log.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

/** A no-op fake for every dep this suite doesn't care about in a given test. */
function baseDeps(overrides = {}) {
  return {
    isAgentStalled: () => false,
    detectDeadlock: async () => ({ trip: false, stalledPids: [], stalledSince: null }),
    escalateAgedQueueEntry: async () => ({ escalated: [], alerted: [], skipped: [] }),
    resolveEvictionTargets: () => [],
    filterEvictable: () => [],
    classifyEvictionOutcome: () => ({ recycled: false, reason: 'unknown' }),
    logWatchdogTrip: async () => ({ written: true }),
    logAgingEscalation: async () => ({ written: true }),
    logDeadlockRecycle: async () => ({ written: true }),
    ...overrides,
  };
}

function baseArgs(overrides = {}) {
  return {
    now: 1_000_000,
    boundMs: 600_000,
    deadlockWindowMs: 900_000,
    agingBoundMs: 1_200_000,
    candidates: [],
    claimLedger: [],
    coordinationFilePath,
    queueFilePath,
    deadlockStateFilePath,
    liveLogFilePath,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// 1. Shape/no-op baseline — no candidates, no aging, no deadlock: the output
// still has the pinned coarse shape with every array empty.
// ---------------------------------------------------------------------------

it('returns the pinned output shape with everything empty when no signal fires', async () => {
  const result = await runWatchdogBeat({ ...baseArgs(), deps: baseDeps() });

  expect(result).toEqual({
    type: 'watchdog',
    trips: [],
    deadlockTripped: false,
    agingEscalations: [],
    recycled: [],
  });
});

// ---------------------------------------------------------------------------
// 2. Per-candidate stall judgement (Phase 1 wiring) — a candidate flagged
// stalled by `deps.isAgentStalled` appears in `trips`, one whose predicate
// returns false does not.
// ---------------------------------------------------------------------------

it('flags exactly the candidates deps.isAgentStalled judges stalled, in candidate order', async () => {
  const isAgentStalled = jest.fn(({ pid }) => pid === 9001);

  const result = await runWatchdogBeat({
    ...baseArgs({
      candidates: [
        { pid: 9001, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 },
        { pid: 9002, footprintTrajectory: [80, 140, 95], worktreeMtimeMs: 999_000 },
      ],
    }),
    deps: baseDeps({ isAgentStalled }),
  });

  expect(isAgentStalled).toHaveBeenCalledTimes(2);
  expect(result.trips).toHaveLength(1);
  expect(result.trips[0]).toMatchObject({ pid: 9001, reason: 'stalled' });
});

// ---------------------------------------------------------------------------
// 3. Recycle wiring (Phase 4 -> real handleEvict) — an `action: 'evict'`
// target from filterEvictable is actually passed to the injected
// `handleEvict`, its result classified, and a successful recycle both
// appears in `recycled` and is logged via `logWatchdogTrip` (pid-based
// origin).
// ---------------------------------------------------------------------------

it('drives an evictable target through the injected handleEvict, classifies it, and records a successful recycle', async () => {
  const handleEvict = jest.fn(async () => ({ type: 'evict', released: true }));
  const logWatchdogTrip = jest.fn(async () => ({ written: true }));

  const result = await runWatchdogBeat({
    ...baseArgs({
      candidates: [{ pid: 9001, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 }],
      claimLedger: [{ pid: 9001, leaseId: 'lease-1', orchestratorPid: 501, orchestratorId: 'orch-a', reentrant: true }],
    }),
    handleEvict,
    deps: baseDeps({
      isAgentStalled: () => true,
      resolveEvictionTargets: ({ watchdogTrips }) =>
        watchdogTrips.map((trip) => ({
          pid: trip.pid,
          leaseId: 'lease-1',
          orchestratorPid: 501,
          orchestratorId: 'orch-a',
          action: 'evict',
        })),
      filterEvictable: (targets) => targets.filter((t) => t.action === 'evict'),
      classifyEvictionOutcome: () => ({ recycled: true }),
      logWatchdogTrip,
    }),
  });

  expect(handleEvict).toHaveBeenCalledTimes(1);
  expect(handleEvict).toHaveBeenCalledWith(
    expect.objectContaining({ pid: 9001, leaseId: 'lease-1', orchestratorPid: 501, orchestratorId: 'orch-a', action: 'evict' }),
  );
  expect(result.recycled).toEqual([9001]);
  expect(result.trips[0]).toMatchObject({ pid: 9001, evicted: true });
  expect(logWatchdogTrip).toHaveBeenCalledTimes(1);
  expect(logWatchdogTrip.mock.calls[0][0]).toBe(liveLogFilePath);
  expect(logWatchdogTrip.mock.calls[0][1]).toMatchObject({ pid: 9001, orchestratorId: 'orch-a', outcome: expect.any(String) });
});

// ---------------------------------------------------------------------------
// 4. `alert-only`/`skip` targets from resolveEvictionTargets are NEVER
// passed to handleEvict at all, and never appear in `recycled`.
// ---------------------------------------------------------------------------

it('never calls handleEvict for an alert-only or skip target, and excludes it from recycled', async () => {
  const handleEvict = jest.fn(async () => ({ type: 'evict', released: true }));

  const result = await runWatchdogBeat({
    ...baseArgs({
      candidates: [{ pid: 9001, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 }],
      claimLedger: [{ pid: 9001, leaseId: 'lease-1', orchestratorPid: 501, orchestratorId: 'orch-a', reentrant: false }],
    }),
    handleEvict,
    deps: baseDeps({
      isAgentStalled: () => true,
      resolveEvictionTargets: () => [
        { pid: 9001, leaseId: 'lease-1', orchestratorPid: 501, orchestratorId: 'orch-a', action: 'alert-only' },
      ],
      filterEvictable: (targets) => targets.filter((t) => t.action === 'evict'),
    }),
  });

  expect(handleEvict).not.toHaveBeenCalled();
  expect(result.recycled).toEqual([]);
});

// ---------------------------------------------------------------------------
// 4b. pre-PR review round 2, Medium — pins the OBSERVABLE half of
// finding 5's per-beat probe-hoisting fix (`getPreTakenSample`/
// `cachedSamplePromise` in cli.mjs): this module's own composition must call
// the injected `handleEvict` exactly once PER evict target, in array order,
// for a beat with >=2 evict targets, and exactly zero times for an all-
// alert-only/skip beat (the latter already covered above; restated here
// alongside the multi-target case for one combined regression anchor).
//
// NOTE — this test proves `runWatchdogBeat`'s own call-count contract with
// `handleEvict`, NOT the probe-hoisting/caching behaviour itself. The actual
// fix (a single shared `takeSample(collect)` call cached across N targets in
// one beat, instead of N separate probe rounds) lives entirely inside
// `cli.mjs`'s `handleWatchdogBeat` — the `cachedSamplePromise` closure is
// constructed OUTSIDE `runWatchdogBeat` and threaded in as the `handleEvict`
// callback's own internals, which this module's injection surface has no
// visibility into (it only ever sees "call this function per target," never
// how many times ITS OWN dependencies were probed). Confirmed by reading
// `runWatchdogBeat`'s composition (./watchdog-beat.mjs) end to end: no
// `collect`/`takeSample`-shaped dependency is threaded through `deps` at
// all. `cli.mjs` also exposes no countable seam for `collect()` invocations
// (unlike e.g. `ARM_FAKE_KILL_LOG`, which counts real kill attempts) — so the
// caching itself is not observable black-box via the `--watchdog-beat` CLI
// either without a production-code seam addition, which is out of scope
// here.
// ---------------------------------------------------------------------------

it('calls the injected handleEvict exactly once per evict target (array order) for a multi-target beat, and zero times for an all-alert-only/skip beat', async () => {
  const handleEvict = jest.fn(async (target) => ({ type: 'evict', pid: target.pid, released: true }));

  const multiTargetResult = await runWatchdogBeat({
    ...baseArgs({
      candidates: [
        { pid: 9001, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 },
        { pid: 9002, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 },
      ],
      claimLedger: [
        { pid: 9001, leaseId: 'lease-1', orchestratorPid: 501, orchestratorId: 'orch-a', reentrant: true },
        { pid: 9002, leaseId: 'lease-2', orchestratorPid: 501, orchestratorId: 'orch-b', reentrant: true },
      ],
    }),
    handleEvict,
    deps: baseDeps({
      isAgentStalled: () => true,
      resolveEvictionTargets: ({ watchdogTrips }) =>
        watchdogTrips.map((trip) => ({
          pid: trip.pid,
          leaseId: trip.pid === 9001 ? 'lease-1' : 'lease-2',
          orchestratorPid: 501,
          orchestratorId: trip.pid === 9001 ? 'orch-a' : 'orch-b',
          action: 'evict',
        })),
      filterEvictable: (targets) => targets.filter((t) => t.action === 'evict'),
      classifyEvictionOutcome: () => ({ recycled: true }),
    }),
  });

  expect(handleEvict).toHaveBeenCalledTimes(2);
  expect(handleEvict.mock.calls[0][0]).toMatchObject({ pid: 9001 });
  expect(handleEvict.mock.calls[1][0]).toMatchObject({ pid: 9002 });
  expect(multiTargetResult.recycled).toEqual([9001, 9002]);

  handleEvict.mockClear();

  const noEvictResult = await runWatchdogBeat({
    ...baseArgs({
      candidates: [
        { pid: 9003, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 },
        { pid: 9004, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 },
      ],
      claimLedger: [
        { pid: 9003, leaseId: 'lease-3', orchestratorPid: 501, orchestratorId: 'orch-c', reentrant: false },
      ],
    }),
    handleEvict,
    deps: baseDeps({
      isAgentStalled: () => true,
      resolveEvictionTargets: ({ watchdogTrips }) =>
        watchdogTrips.map((trip) =>
          trip.pid === 9003
            ? { pid: 9003, leaseId: 'lease-3', orchestratorPid: 501, orchestratorId: 'orch-c', action: 'alert-only' }
            : { pid: trip.pid, action: 'skip', skipReason: 'no matching claimLedger record for watchdog-trip pid' },
        ),
      filterEvictable: (targets) => targets.filter((t) => t.action === 'evict'),
    }),
  });

  expect(handleEvict).not.toHaveBeenCalled();
  expect(noEvictResult.recycled).toEqual([]);
});

// ---------------------------------------------------------------------------
// 5. Deadlock-trip wiring (Phase 3) — a trip surfaces `deadlockTripped:
// true`, its stalledPids (orchestratorId-substituted, per eviction-
// targeting.mjs's documented convention) are threaded into
// resolveEvictionTargets as `deadlockTrip`, and a successfully recycled
// deadlock-trip target is logged via `logDeadlockRecycle` — NOT
// `logWatchdogTrip` — and identified in `recycled` by its real OS pid, just
// like a watchdog-trip target (`buildResolvedTarget` in
// eviction-targeting.mjs always carries the ledger record's own `pid`, even
// though the record was matched BY `orchestratorId` on this path).
// ---------------------------------------------------------------------------

it('wires a deadlock trip through to resolveEvictionTargets and logs a recycled deadlock target via logDeadlockRecycle', async () => {
  const handleEvict = jest.fn(async () => ({ type: 'evict', released: true }));
  const logDeadlockRecycle = jest.fn(async () => ({ written: true }));
  const logWatchdogTrip = jest.fn(async () => ({ written: true }));

  const result = await runWatchdogBeat({
    ...baseArgs({
      claimLedger: [{ pid: 201, leaseId: 'lease-2', orchestratorPid: 502, orchestratorId: 'orch-stuck', reentrant: true }],
    }),
    handleEvict,
    deps: baseDeps({
      detectDeadlock: async () => ({ trip: true, stalledPids: ['orch-stuck'], stalledSince: 0 }),
      resolveEvictionTargets: ({ deadlockTrip }) =>
        deadlockTrip.trip
          ? deadlockTrip.stalledPids.map((orchestratorId) => ({
              pid: 201,
              leaseId: 'lease-2',
              orchestratorPid: 502,
              orchestratorId,
              action: 'evict',
            }))
          : [],
      filterEvictable: (targets) => targets.filter((t) => t.action === 'evict'),
      classifyEvictionOutcome: () => ({ recycled: true }),
      logDeadlockRecycle,
      logWatchdogTrip,
    }),
  });

  expect(result.deadlockTripped).toBe(true);
  expect(handleEvict).toHaveBeenCalledTimes(1);
  expect(logDeadlockRecycle).toHaveBeenCalledTimes(1);
  expect(logDeadlockRecycle.mock.calls[0][1]).toMatchObject({ orchestratorId: 'orch-stuck', outcome: expect.any(String) });
  expect(logWatchdogTrip).not.toHaveBeenCalled();
  expect(result.recycled).toEqual([201]);
});

// ---------------------------------------------------------------------------
// 6. Aging-escalation wiring (Phase 2) — every entry `deps.
// escalateAgedQueueEntry` reports as `escalated` is passed straight through
// into the beat's own `agingEscalations`, and each one is logged via
// `logAgingEscalation` — independent of whether anything was recycled this
// beat.
// ---------------------------------------------------------------------------

it('passes escalateAgedQueueEntry escalations through to agingEscalations and logs each one', async () => {
  const escalated = [{ agentClass: 'typescript-implementer', orchestratorId: 'orch-b', priority: 'high' }];
  const logAgingEscalation = jest.fn(async () => ({ written: true }));

  const result = await runWatchdogBeat({
    ...baseArgs(),
    deps: baseDeps({
      escalateAgedQueueEntry: async () => ({ escalated, alerted: escalated, skipped: [] }),
      logAgingEscalation,
    }),
  });

  expect(result.agingEscalations).toEqual(escalated);
  expect(logAgingEscalation).toHaveBeenCalledTimes(1);
  expect(logAgingEscalation.mock.calls[0][1]).toMatchObject({ agentClass: 'typescript-implementer', orchestratorId: 'orch-b' });
});

// ---------------------------------------------------------------------------
// 7. A trip whose eviction attempt FAILS (classifyEvictionOutcome resolves
// `recycled: false`) is still recorded in `trips` (evicted: false, with the
// failure reason surfaced) and still logged — but never added to
// `recycled`. "Detected, attempted, failed" must stay observable, not
// silently collapse into "nothing happened".
// ---------------------------------------------------------------------------

it('records a failed eviction attempt in trips with evicted:false and a reason, without adding it to recycled', async () => {
  const handleEvict = jest.fn(async () => ({ evictionAttempted: true, reason: 'not-authorized' }));

  const result = await runWatchdogBeat({
    ...baseArgs({
      candidates: [{ pid: 9001, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 }],
      claimLedger: [{ pid: 9001, leaseId: 'lease-1', orchestratorPid: 501, orchestratorId: 'orch-a', reentrant: true }],
    }),
    handleEvict,
    deps: baseDeps({
      isAgentStalled: () => true,
      resolveEvictionTargets: ({ watchdogTrips }) =>
        watchdogTrips.map((trip) => ({
          pid: trip.pid,
          leaseId: 'lease-1',
          orchestratorPid: 501,
          orchestratorId: 'orch-a',
          action: 'evict',
        })),
      filterEvictable: (targets) => targets.filter((t) => t.action === 'evict'),
      classifyEvictionOutcome: () => ({ recycled: false, reason: 'not-authorized' }),
    }),
  });

  expect(result.recycled).toEqual([]);
  expect(result.trips[0]).toMatchObject({ pid: 9001, evicted: false, outcome: 'not-authorized' });
});

// ---------------------------------------------------------------------------
// 8. Calling with at least one evictable target and NO `handleEvict` at all
// is a caller error — throws synchronously, before any real I/O or log
// write is attempted. This is the load-bearing safety property: a future
// caller can never accidentally no-op a real recycle by forgetting to wire
// the injection seam.
// ---------------------------------------------------------------------------

it('throws before any log write when an evictable target exists but no handleEvict was supplied', async () => {
  await expect(
    runWatchdogBeat({
      ...baseArgs({
        candidates: [{ pid: 9001, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 }],
        claimLedger: [{ pid: 9001, leaseId: 'lease-1', orchestratorPid: 501, orchestratorId: 'orch-a', reentrant: true }],
      }),
      // handleEvict deliberately omitted.
      deps: baseDeps({
        isAgentStalled: () => true,
        resolveEvictionTargets: ({ watchdogTrips }) =>
          watchdogTrips.map((trip) => ({
            pid: trip.pid,
            leaseId: 'lease-1',
            orchestratorPid: 501,
            orchestratorId: 'orch-a',
            action: 'evict',
          })),
        filterEvictable: (targets) => targets.filter((t) => t.action === 'evict'),
      }),
    }),
  ).rejects.toThrow(/handleEvict/);

  const rawLog = await readFile(liveLogFilePath, 'utf8').catch((error) => (error.code === 'ENOENT' ? '' : Promise.reject(error)));
  expect(rawLog).toBe('');
});

// ---------------------------------------------------------------------------
// 9. detectDeadlock is invoked with the real coordinationFilePath/
// queueFilePath/deadlockStateFilePath this beat was given — proving the
// composition actually threads the caller's own file paths through, not a
// hardcoded/fixture path.
// ---------------------------------------------------------------------------

it('invokes deps.detectDeadlock with this beat\'s own coordination/queue/state file paths', async () => {
  const detectDeadlock = jest.fn(async () => ({ trip: false, stalledPids: [], stalledSince: null }));

  await runWatchdogBeat({ ...baseArgs(), deps: baseDeps({ detectDeadlock }) });

  expect(detectDeadlock).toHaveBeenCalledWith(
    coordinationFilePath,
    queueFilePath,
    deadlockStateFilePath,
    expect.objectContaining({ now: 1_000_000, windowMs: 900_000 }),
  );
});

// ---------------------------------------------------------------------------
// 10. A pre-existing liveness-log file's prior content is never clobbered by
// this beat's own writes — each `logXxx` dep is invoked via its own natural
// append contract, this beat itself performs no whole-file overwrite of
// `liveLogFilePath`. Sanity-checked here by seeding an unrelated prior file
// and asserting it is passed through untouched to a no-op fake logger.
// ---------------------------------------------------------------------------

it('never touches liveLogFilePath directly — only through the injected log deps', async () => {
  await writeFile(liveLogFilePath, '{"schemaVersion":1,"detectionType":"watchdog-trip"}\n', 'utf8');

  await runWatchdogBeat({ ...baseArgs(), deps: baseDeps() });

  const raw = await readFile(liveLogFilePath, 'utf8');
  expect(raw).toBe('{"schemaVersion":1,"detectionType":"watchdog-trip"}\n');
});

// ---------------------------------------------------------------------------
// 11. Isolation-by-error-type (corrected post-review). The original
// cut of this fix gated isolation on `evictActionCount > 1` — wrong on both
// axes: a multi-target beat where EVERY target hits the identical systemic,
// whole-process misconfiguration (`cli.mjs`'s `performEviction` throwing
// `EvictFakeSeamCouplingError`) would have gotten ALL of them silently
// isolated into fabricated `reason: 'unknown'` outcomes instead of failing
// the beat loudly (exactly the safety regression the outer-acceptance
// guard below exists to catch); and a single-target beat was wrongly
// forbidden from isolating an ORDINARY per-target failure at all. The fix:
// isolation is decided by whether the rejection carries the `armFailBeat`
// marker (set by `EvictFakeSeamCouplingError` — see `cli.mjs`'s own comment
// on that class), never by how many `evict` targets are in the beat.
//
// The three tests below pin the corrected matrix:
//   - ordinary error, single target      -> isolated (NOT propagated)
//   - ordinary error, multiple targets   -> isolated, siblings unaffected
//   - marked systemic error, ANY target count -> propagates, fails loud
// ---------------------------------------------------------------------------

it('isolates an ORDINARY (non-systemic) handleEvict rejection to its own target even with only ONE evict target this beat', async () => {
  const ordinaryFailure = new Error('ECONNRESET: transient queue-file read failure');
  const handleEvict = jest.fn(async () => {
    throw ordinaryFailure;
  });
  const classifyEvictionOutcome = jest.fn(() => ({ recycled: false, reason: 'unknown' }));
  const logWatchdogTrip = jest.fn(async () => ({ written: true }));

  const result = await runWatchdogBeat({
    ...baseArgs({
      candidates: [{ pid: 9001, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 }],
      claimLedger: [{ pid: 9001, leaseId: 'lease-1', orchestratorPid: 501, orchestratorId: 'orch-a', reentrant: true }],
    }),
    handleEvict,
    deps: baseDeps({
      isAgentStalled: () => true,
      resolveEvictionTargets: ({ watchdogTrips }) =>
        watchdogTrips.map((trip) => ({
          pid: trip.pid,
          leaseId: 'lease-1',
          orchestratorPid: 501,
          orchestratorId: 'orch-a',
          action: 'evict',
        })),
      filterEvictable: (targets) => targets.filter((t) => t.action === 'evict'),
      classifyEvictionOutcome,
      logWatchdogTrip,
    }),
  });

  // With the corrected error-type gate, this beat resolves normally — a
  // single ordinary failure with no sibling target is isolated, not
  // propagated, unlike the pre-existing whole-beat-abort behaviour.
  expect(result.type).toBe('watchdog');
  const trip9001 = result.trips.find((trip) => trip.pid === 9001);
  expect(trip9001).toMatchObject({ evicted: false, outcome: 'unknown' });
  expect(result.recycled).not.toContain(9001);
  expect(logWatchdogTrip).toHaveBeenCalled();
});

it('isolates a genuine ORDINARY handleEvict rejection to its own target — the beat returns normally with every other target\'s outcome intact, and never re-fabricates the failure into a resolved reason: unknown outcome for OTHER targets', async () => {
  const ordinaryFailure = new Error('some ordinary per-target eviction failure');
  const handleEvict = jest.fn(async (target) => {
    if (target.pid === 9001) throw ordinaryFailure;
    return { type: 'evict', pid: target.pid, released: true };
  });
  const classifyEvictionOutcome = jest.fn((result) =>
    result?.released ? { recycled: true } : { recycled: false, reason: 'unknown' },
  );
  const logWatchdogTrip = jest.fn(async () => ({ written: true }));

  const result = await runWatchdogBeat({
    ...baseArgs({
      candidates: [
        { pid: 9001, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 },
        { pid: 9002, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 },
      ],
      claimLedger: [
        { pid: 9001, leaseId: 'lease-1', orchestratorPid: 501, orchestratorId: 'orch-a', reentrant: true },
        { pid: 9002, leaseId: 'lease-2', orchestratorPid: 501, orchestratorId: 'orch-b', reentrant: true },
      ],
    }),
    handleEvict,
    deps: baseDeps({
      isAgentStalled: () => true,
      resolveEvictionTargets: ({ watchdogTrips }) =>
        watchdogTrips.map((trip) => ({
          pid: trip.pid,
          leaseId: trip.pid === 9001 ? 'lease-1' : 'lease-2',
          orchestratorPid: 501,
          orchestratorId: trip.pid === 9001 ? 'orch-a' : 'orch-b',
          action: 'evict',
        })),
      filterEvictable: (targets) => targets.filter((t) => t.action === 'evict'),
      classifyEvictionOutcome,
      logWatchdogTrip,
    }),
  });

  // The beat itself must resolve, not reject.
  expect(result.type).toBe('watchdog');

  // The rejecting target (9001) is captured as its OWN failed outcome —
  // never evicted, never crashes the beat.
  const trip9001 = result.trips.find((trip) => trip.pid === 9001);
  expect(trip9001).toMatchObject({ evicted: false });
  expect(result.recycled).not.toContain(9001);

  // The OTHER target (9002) must be entirely unaffected — still correctly
  // evicted and logged, proving error isolation rather than a fabricated
  // partial-success shape.
  const trip9002 = result.trips.find((trip) => trip.pid === 9002);
  expect(trip9002).toMatchObject({ evicted: true });
  expect(result.recycled).toContain(9002);
});

it('propagates a MARKED SYSTEMIC handleEvict rejection out of runWatchdogBeat even with MULTIPLE evict targets — every target hitting the identical whole-process misconfiguration must fail the beat loudly, never be silently isolated into fabricated reason: unknown outcomes', async () => {
  // Mirrors `cli.mjs`'s `EvictFakeSeamCouplingError`: a genuine,
  // whole-process misconfiguration where EVERY evict target this beat
  // attempts throws the SAME error (a leaked/partially-faked ARM_FAKE_* env
  // seam affects every `performEviction` call in this process, not just one
  // target's) — see that class's own comment for why it sets `armFailBeat`.
  const systemicFailure = new Error(
    'agent-resource-management: --evict-pid refuses to proceed with a partially-faked eviction seam',
  );
  systemicFailure.armFailBeat = true;
  const handleEvict = jest.fn(async () => {
    throw systemicFailure;
  });
  const classifyEvictionOutcome = jest.fn(() => ({ recycled: false, reason: 'unknown' }));
  const logWatchdogTrip = jest.fn(async () => ({ written: true }));

  const invocation = runWatchdogBeat({
    ...baseArgs({
      candidates: [
        { pid: 9001, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 },
        { pid: 9002, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 },
      ],
      claimLedger: [
        { pid: 9001, leaseId: 'lease-1', orchestratorPid: 501, orchestratorId: 'orch-a', reentrant: true },
        { pid: 9002, leaseId: 'lease-2', orchestratorPid: 501, orchestratorId: 'orch-b', reentrant: true },
      ],
    }),
    handleEvict,
    deps: baseDeps({
      isAgentStalled: () => true,
      resolveEvictionTargets: ({ watchdogTrips }) =>
        watchdogTrips.map((trip) => ({
          pid: trip.pid,
          leaseId: trip.pid === 9001 ? 'lease-1' : 'lease-2',
          orchestratorPid: 501,
          orchestratorId: trip.pid === 9001 ? 'orch-a' : 'orch-b',
          action: 'evict',
        })),
      filterEvictable: (targets) => targets.filter((t) => t.action === 'evict'),
      classifyEvictionOutcome,
      logWatchdogTrip,
    }),
  });

  // Fails loud, not open — this is exactly the shape the count-based gate
  // got wrong: with two evict targets, the old `evictActionCount > 1` gate
  // would have isolated BOTH into fabricated reason: 'unknown' outcomes
  // instead of rejecting.
  await expect(invocation).rejects.toBe(systemicFailure);
});

it('propagates a MARKED SYSTEMIC handleEvict rejection out of runWatchdogBeat with only ONE evict target, for completeness alongside the multi-target case above', async () => {
  const systemicFailure = new Error('agent-resource-management: --evict-pid refuses to proceed — genuine misconfiguration');
  systemicFailure.armFailBeat = true;
  const handleEvict = jest.fn(async () => {
    throw systemicFailure;
  });
  const classifyEvictionOutcome = jest.fn(() => ({ recycled: false, reason: 'unknown' }));
  const logWatchdogTrip = jest.fn(async () => ({ written: true }));

  const invocation = runWatchdogBeat({
    ...baseArgs({
      candidates: [{ pid: 9001, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 }],
      claimLedger: [{ pid: 9001, leaseId: 'lease-1', orchestratorPid: 501, orchestratorId: 'orch-a', reentrant: true }],
    }),
    handleEvict,
    deps: baseDeps({
      isAgentStalled: () => true,
      resolveEvictionTargets: ({ watchdogTrips }) =>
        watchdogTrips.map((trip) => ({
          pid: trip.pid,
          leaseId: 'lease-1',
          orchestratorPid: 501,
          orchestratorId: 'orch-a',
          action: 'evict',
        })),
      filterEvictable: (targets) => targets.filter((t) => t.action === 'evict'),
      classifyEvictionOutcome,
      logWatchdogTrip,
    }),
  });

  await expect(invocation).rejects.toBe(systemicFailure);
  expect(classifyEvictionOutcome).not.toHaveBeenCalled();
  expect(logWatchdogTrip).not.toHaveBeenCalled();
});

// ---------------------------------------------------------------------------
// — bounded-concurrency eviction pool (Step 6). Today's Step 6 loop is
// `for (const target of targets) { await handleEvict(target); ... }` —
// strictly sequential. Every test below is RED against that implementation
// and pins the REPLACEMENT contract: a concurrency-limited POOL (never a
// "chunk into groups of the cap, await each chunk in turn" batching
// disguise — see the head-of-line-blocking test below, which is specifically
// designed to distinguish the two), capped at
// `DEFAULT_MAX_CONCURRENT_EVICTIONS_PER_BEAT` (3) simultaneous in-flight
// `handleEvict` calls — the constant is a fixed module-level export, not a
// `runWatchdogBeat` parameter, so no test below overrides it; every scenario
// is written against the real cap of 3.
//
// Helper below builds a beat with N synthetic evictable targets (pids
// `10001..10000+N`), each independently controllable via the `handleEvict`
// fake supplied by the caller — `resolveEvictionTargets`/`filterEvictable`
// are wired to pass every trip straight through as an `evict` target, and
// `classifyEvictionOutcome` treats any `{ released: true }` result as a
// successful recycle.
// ---------------------------------------------------------------------------

function delay(ms, value) {
  return new Promise((resolve) => setTimeout(() => resolve(value), ms));
}

function evictBeatArgs({ pids, handleEvict }) {
  return {
    ...baseArgs({
      candidates: pids.map((pid) => ({ pid, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 })),
      claimLedger: pids.map((pid) => ({
        pid,
        leaseId: `lease-${pid}`,
        orchestratorPid: 501,
        orchestratorId: `orch-${pid}`,
        reentrant: true,
      })),
    }),
    handleEvict,
    deps: baseDeps({
      isAgentStalled: () => true,
      resolveEvictionTargets: ({ watchdogTrips }) =>
        watchdogTrips.map((trip) => ({
          pid: trip.pid,
          leaseId: `lease-${trip.pid}`,
          orchestratorPid: 501,
          orchestratorId: `orch-${trip.pid}`,
          action: 'evict',
        })),
      filterEvictable: (targets) => targets.filter((t) => t.action === 'evict'),
      classifyEvictionOutcome: (result) =>
        result?.released ? { recycled: true } : { recycled: false, reason: result?.reason ?? 'unknown' },
    }),
  };
}

it(': runs three evictable targets\' handleEvict calls with genuinely overlapping wall-clock time, not sequential resolution', async () => {
  const timings = [];
  const handleEvict = jest.fn(async (target) => {
    const start = Date.now();
    await delay(40);
    const end = Date.now();
    timings.push({ pid: target.pid, start, end });
    return { released: true };
  });

  await runWatchdogBeat(evictBeatArgs({ pids: [10001, 10002, 10003], handleEvict }));

  expect(timings).toHaveLength(3);
  const byPid = new Map(timings.map((t) => [t.pid, t]));
  const t1 = byPid.get(10001);
  const t2 = byPid.get(10002);
  const t3 = byPid.get(10003);

  // Genuine overlap: each call's START precedes at least one OTHER call's
  // END — impossible under strict sequential `for...await` execution, where
  // call N+1's start can never precede call N's own end.
  expect(t2.start).toBeLessThan(t1.end);
  expect(t3.start).toBeLessThan(t1.end);
  expect(t3.start).toBeLessThan(t2.end);

  // And the whole batch must cost roughly ONE grace period, not three summed
  // — the latest end minus the earliest start is close to a single delay(40),
  // not anywhere near 3x that.
  const earliestStart = Math.min(t1.start, t2.start, t3.start);
  const latestEnd = Math.max(t1.end, t2.end, t3.end);
  expect(latestEnd - earliestStart).toBeLessThan(120); // well under 3 * 40ms
});

it(': is a genuine concurrency-limited POOL, not batching into chunks — a permanently-hung target does not block a later target from starting once an earlier one frees a slot', async () => {
  const calls = [];
  const handleEvict = jest.fn(async (target) => {
    calls.push(target.pid);
    if (target.pid === 10001) {
      // Permanently hung — never resolves, mirroring a wedged gracefulStop.
      return new Promise(() => {});
    }
    await delay(10);
    return { released: true };
  });

  // Deliberately not awaited — target 10001 never resolves, so the whole
  // beat can never resolve either. We only need to observe handleEvict's own
  // call timeline, not the beat's final return value.
  void runWatchdogBeat(evictBeatArgs({ pids: [10001, 10002, 10003, 10004], handleEvict })).catch(() => {});

  // Give the pool a moment to admit its first (cap = 3) batch.
  await delay(5);
  expect(calls).toEqual(expect.arrayContaining([10001, 10002, 10003]));
  expect(calls).not.toContain(10004);

  // Once 10002/10003 resolve (~10ms), a freed slot must IMMEDIATELY admit
  // 10004 — even though 10001 is still hung. A naive "chunk into groups of
  // the cap, await each chunk sequentially" implementation would instead
  // await Promise.all([10001, 10002, 10003]) as one unit, which never
  // resolves because 10001 never does — so 10004 would NEVER be called under
  // that (wrong) implementation. This assertion is the one that fails
  // against chunking.
  await delay(30);
  expect(calls).toContain(10004);
});

it(': stamps per-target trips/recycled by target identity, correct even when handleEvict calls resolve OUT of their original order', async () => {
  const handleEvict = jest.fn(async (target) => {
    // Reverse-order resolution: the FIRST target takes the LONGEST.
    if (target.pid === 10001) await delay(30);
    if (target.pid === 10002) await delay(15);
    if (target.pid === 10003) await delay(0);

    // Mixed outcomes so identity (not position) must drive stamping.
    if (target.pid === 10002) return { released: false, reason: 'not-authorized' };
    return { released: true };
  });

  const result = await runWatchdogBeat(evictBeatArgs({ pids: [10001, 10002, 10003], handleEvict }));

  const trip1 = result.trips.find((t) => t.pid === 10001);
  const trip2 = result.trips.find((t) => t.pid === 10002);
  const trip3 = result.trips.find((t) => t.pid === 10003);

  expect(trip1).toMatchObject({ evicted: true });
  expect(trip2).toMatchObject({ evicted: false, outcome: 'not-authorized' });
  expect(trip3).toMatchObject({ evicted: true });

  expect(result.recycled).toContain(10001);
  expect(result.recycled).not.toContain(10002);
  expect(result.recycled).toContain(10003);
});

it(': recycled preserves targets\' original relative order even when handleEvict promises resolve out of that order', async () => {
  const handleEvict = jest.fn(async (target) => {
    // Resolution order is the REVERSE of targets/candidates order.
    if (target.pid === 10001) await delay(30);
    if (target.pid === 10002) await delay(15);
    if (target.pid === 10003) await delay(0);
    return { released: true };
  });

  const result = await runWatchdogBeat(evictBeatArgs({ pids: [10001, 10002, 10003], handleEvict }));

  // Original target order was [10001, 10002, 10003]; resolution order was
  // [10003, 10002, 10001]. `recycled` must reflect the FORMER.
  expect(result.recycled).toEqual([10001, 10002, 10003]);
});

it('post-review, Critical — two evict targets sharing the SAME orchestratorId run strictly serially (never overlapping), while targets for DIFFERENT orchestratorIds still run genuinely concurrently in the same beat', async () => {
  // `performEviction` (cli.mjs) does a read-then-conditionally-write round
  // trip against its OWN orchestratorId's dibs-ledger entry; two targets for
  // the same orchestrator racing through that independently can corrupt each
  // other's read/write (see watchdog-outer-acceptance.jest.spec.mjs's
  // Scenario 5 regression guard, which this fix restores). This fake
  // `handleEvict` cannot exercise the real ledger race directly (it is a
  // pure unit fake with no coordination-file I/O), so it instead pins the
  // OBSERVABLE property the fix guarantees: overlapping wall-clock execution
  // is impossible for two same-orchestratorId targets, but still happens for
  // two different-orchestratorId targets in the identical beat.
  const timings = [];
  const handleEvict = jest.fn(async (target) => {
    const start = Date.now();
    await delay(30);
    const end = Date.now();
    timings.push({ pid: target.pid, orchestratorId: target.orchestratorId, start, end });
    return { released: true };
  });

  const pids = [10001, 10002, 10003];
  const args = {
    ...baseArgs({
      candidates: pids.map((pid) => ({ pid, footprintTrajectory: [100, 100, 100], worktreeMtimeMs: 0 })),
      claimLedger: pids.map((pid) => ({
        pid,
        leaseId: `lease-${pid}`,
        orchestratorPid: 501,
        // 10001 and 10002 share ONE orchestratorId; 10003 has its own.
        orchestratorId: pid === 10002 ? 'orch-shared' : pid === 10001 ? 'orch-shared' : 'orch-10003',
        reentrant: true,
      })),
    }),
    handleEvict,
    deps: baseDeps({
      isAgentStalled: () => true,
      resolveEvictionTargets: ({ watchdogTrips }) =>
        watchdogTrips.map((trip) => ({
          pid: trip.pid,
          leaseId: `lease-${trip.pid}`,
          orchestratorPid: 501,
          orchestratorId: trip.pid === 10003 ? 'orch-10003' : 'orch-shared',
          action: 'evict',
        })),
      filterEvictable: (targets) => targets.filter((t) => t.action === 'evict'),
      classifyEvictionOutcome: (result) =>
        result?.released ? { recycled: true } : { recycled: false, reason: result?.reason ?? 'unknown' },
    }),
  };

  await runWatchdogBeat(args);

  const byPid = new Map(timings.map((t) => [t.pid, t]));
  const shared1 = byPid.get(10001);
  const shared2 = byPid.get(10002);
  const other = byPid.get(10003);

  // Same orchestratorId ('orch-shared'): strictly serial — one must fully
  // end before the other starts.
  expect(Math.min(shared1.start, shared2.start)).toBeGreaterThanOrEqual(0);
  const [first, second] = shared1.start < shared2.start ? [shared1, shared2] : [shared2, shared1];
  expect(second.start).toBeGreaterThanOrEqual(first.end);

  // Different orchestratorId ('orch-10003'): still genuinely concurrent with
  // whichever 'orch-shared' target is running at the time — its start
  // precedes that target's own end.
  expect(other.start).toBeLessThan(Math.max(shared1.end, shared2.end));
});

it(': zero-target and single-target beats behave identically to today, with no measurable pool-bookkeeping overhead on the single-target path', async () => {
  const zeroTargetResult = await runWatchdogBeat({ ...baseArgs(), deps: baseDeps() });
  expect(zeroTargetResult).toEqual({
    type: 'watchdog',
    trips: [],
    deadlockTripped: false,
    agingEscalations: [],
    recycled: [],
  });

  const handleEvictDurationMs = 20;
  const handleEvict = jest.fn(async () => {
    await delay(handleEvictDurationMs);
    return { released: true };
  });

  const startedAt = Date.now();
  const singleTargetResult = await runWatchdogBeat(evictBeatArgs({ pids: [10001], handleEvict }));
  const elapsedMs = Date.now() - startedAt;

  expect(singleTargetResult.recycled).toEqual([10001]);
  // Generous margin (should be well within a handful of ms of pure
  // bookkeeping/log-write overhead) — a single-target beat must cost
  // approximately ONE handleEvict call's own duration, not meaningfully
  // more just because a concurrency pool now exists under the hood.
  expect(elapsedMs).toBeLessThan(handleEvictDurationMs + 100);
});

it(': three concurrently-evicted targets each append their own watchdog-trip liveness-log entry with no corrupted/interleaved NDJSON and no assertLogWritten failure', async () => {
  const handleEvict = jest.fn(async (target) => {
    await delay(target.pid === 10001 ? 20 : target.pid === 10002 ? 10 : 0);
    return { released: true };
  });

  // Uses the REAL logWatchdogTrip (real file lock contention via
  // coordination-file.mjs's exclusive-create fencing), not a fake — this is
  // the scenario that was IMPOSSIBLE under sequential execution (one writer
  // at a time) and is only reachable now that up to `cap` calls truly race
  // to append to the same liveLogFilePath concurrently.
  const args = evictBeatArgs({ pids: [10001, 10002, 10003], handleEvict });
  args.deps.logWatchdogTrip = realLogWatchdogTrip;

  // Would throw (via assertLogWritten) if any concurrent append lost the
  // lock race outright rather than retrying — the test itself is the
  // assertion that no such failure propagates.
  const result = await runWatchdogBeat(args);
  expect(result.recycled.sort()).toEqual([10001, 10002, 10003]);

  const rawLog = await readFile(liveLogFilePath, 'utf8');
  const lines = rawLog.split('\n').filter((line) => line.length > 0);
  expect(lines).toHaveLength(3);

  const parsedByPid = new Map();
  for (const line of lines) {
    // Throws if any line is corrupted/interleaved JSON — the core assertion.
    const entry = JSON.parse(line);
    parsedByPid.set(entry.pid, entry);
  }
  expect(parsedByPid.size).toBe(3);
  for (const pid of [10001, 10002, 10003]) {
    expect(parsedByPid.get(pid)).toMatchObject({ pid, detectionType: 'watchdog-trip' });
  }
});

it('post-review, Critical — a MARKED SYSTEMIC handleEvict rejection drains still in-flight SIBLING targets before rethrowing, never abandoning a mid-`gracefulStop` sibling to `process.exit`', async () => {
  // Target 10001 rejects systemically almost immediately; target 10002 is
  // still genuinely in-flight (its own `gracefulStop`-equivalent delay
  // hasn't resolved yet) at the moment 10001's rejection would otherwise
  // propagate straight out of `runWatchdogBeat`. If the pool loop rethrows
  // without first draining `inFlight`, 10002's `handleEvict` promise is
  // simply abandoned mid-flight — this test proves it is instead awaited to
  // completion.
  const systemicFailure = new Error(
    'agent-resource-management: --evict-pid refuses to proceed — genuine misconfiguration',
  );
  systemicFailure.armFailBeat = true;

  let sibling10002Settled = false;
  const handleEvict = jest.fn(async (target) => {
    if (target.pid === 10001) {
      await delay(5);
      throw systemicFailure;
    }
    // 10002 (and 10003) take noticeably longer than 10001's rejection, so
    // they are still in-flight at the moment 10001 rejects.
    await delay(40);
    sibling10002Settled = true;
    return { released: true };
  });

  const invocation = runWatchdogBeat(evictBeatArgs({ pids: [10001, 10002, 10003], handleEvict }));

  await expect(invocation).rejects.toBe(systemicFailure);

  // The critical assertion: by the time the beat's promise has actually
  // rejected, the sibling's `handleEvict` call has already been awaited to
  // completion — not merely started, not abandoned mid-flight.
  expect(sibling10002Settled).toBe(true);
  expect(handleEvict).toHaveBeenCalledTimes(3);
});
