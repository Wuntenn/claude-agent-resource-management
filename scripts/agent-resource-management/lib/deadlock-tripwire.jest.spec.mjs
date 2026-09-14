// Phase 3 — deadlock tripwire (all-stalled + non-empty queue, sustained window).
//
// RED unit tests for the not-yet-implemented
// scripts/agent-resource-management/lib/deadlock-tripwire.mjs module.
//
// Direct-import unit tests, mirroring ./queue-aging-escalation.jest.spec.mjs's/
// ./queue.jest.spec.mjs's idiom exactly: real fs, real temp directories
// (mkdtempSync under os.tmpdir()) for the persistence-across-invocation
// coverage, plain synchronous calls (no fs at all) for the pure core —
// never a mocked filesystem.
//
// `lib/deadlock-tripwire.mjs` does NOT exist yet as of this commit — every
// test below is expected to fail at import time (`Cannot find module
// './deadlock-tripwire.mjs'`). Do not add an implementation to make this
// pass — the builder implements it next to turn this green.
//
// ---------------------------------------------------------------------------
// DESIGN DECISION (for the implementer to build against, not to relitigate):
// PURE CORE + THIN PERSISTENCE WRAPPER, following the brief's steer.
//
//   evaluateDeadlockWindow({ agentLivenessResults, queueNonEmpty, now, windowMs, stalledSince })
//     -> { trip: boolean, stalledPids: Array<string|number>, stalledSince: number|null }
//
//   A PLAIN, SYNCHRONOUS, side-effect-free function. Every input it needs —
//   including the "already stalled since" timestamp read from persisted
//   state by the caller — is threaded in explicitly, mirroring
//   `./liveness.mjs`'s `isAgentStalled` convention exactly (no env reads, no
//   `Date.now()`, no fs). `agentLivenessResults` is an array of
//   `{ pid: string|number, stalled: boolean }` — ALREADY-COMPUTED per-agent
//   liveness verdicts (the caller has already run each live agent's
//   `footprintTrajectory`/`worktreeMtimeMs` through `isAgentStalled`;
//   composing that per-agent call is the wrapper's job, not this function's
//   — same "resolve upstream, judge here" split `isAgentStalled` itself
//   documents for `worktreeMtimeMs`).
//
//   This is the ENTIRE trip/no-trip/window-tracking decision, and it is what
//   the bulk of this suite exercises directly — deterministic, no fs, no
//   timers, trivial to hit every edge case (zero agents, single agent,
//   window-not-yet-elapsed, window-exactly-elapsed, an agent joining
//   mid-window, empty queue) with a plain function call.
//
//   detectDeadlock(coordinationFilePath, queueFilePath, stateFilePath, options?)
//     -> Promise<{ trip: boolean, stalledPids: Array<string|number>, stalledSince: number|null }>
//
//   The thin I/O wrapper. Composes:
//     1. `readDibs` (./coordination-file.mjs) to enumerate currently-live
//        agents — per the Build Plan's explicit instruction to enumerate via
//        coordination-file's existing dibs/claim tracking. Read-only,
//        best-effort: a read failure (e.g. a torn/mid-write file caught
//        mid-race) degrades to "treat as zero live agents this beat" rather
//        than throwing — matching `readDibs`'s/`readEntriesRaw`'s own
//        ENOENT-tolerant, ENOENT-is-not-corruption posture, extended here to
//        "any read failure is a soft, one-beat degradation", never a thrown
//        error out of `detectDeadlock` itself.
//
//        NOTE (interpretation, documented rather than hidden): dibs entries
//        in this codebase are keyed by `orchestratorId`, NOT by an OS `pid`
//        — `coordination-file.mjs`'s own `declareDibs`/`readDibs` carry no
//        `pid` field anywhere (confirmed by reading that module directly).
//        This suite therefore treats each live dibs entry's `orchestratorId`
//        as the per-agent identifier threaded through as `isAgentStalled`'s
//        `pid` parameter and returned in `stalledPids` — `isAgentStalled`
//        itself never inspects `pid` beyond passing it through, so this is a
//        safe substitution, but it IS a substitution the implementer/reviewer
//        should confirm against Phase 6's real wiring, not a literal OS pid.
//     2. `peekQueue` (./queue.mjs) for queue-non-empty.
//     3. `options.getFootprintTrajectory(orchestratorId)` /
//        `options.getWorktreeMtimeMs(orchestratorId)` — caller-injected
//        resolvers (defaulting to `() => []` / `() => null`, i.e. "no
//        evidence, never stalled" when omitted) feeding `isAgentStalled`
//        (./liveness.mjs) per live agent. Wiring these to real footprint/
//        worktree data is Phase 6's job; this phase accepts them as
//        dependency-injected functions so this module has no direct
//        knowledge of `./footprint-state.mjs`/git-mtime scanning.
//     4. A persisted debounce-state file at `stateFilePath` (JSON
//        `{ stalledSince: number|null }`), read before evaluating and
//        written back after — via the SAME lock/atomic-write primitives
//        `./coordination-file.mjs` exports (`acquireLock`/`assertStillHeld`/
//        `releaseLock`/`writeEntriesAtomic`-style shape), mirroring
//        `./queue-aging-escalation.mjs`'s house pattern for a persisted-state
//        module — not a bespoke locking mechanism. This is what makes the
//        sustained-window state survive a process restart (Build Plan edge
//        case, Convergence #6): a fresh `detectDeadlock` invocation reads
//        `stalledSince` off disk rather than starting from a zeroed
//        in-process counter.
//
// ---------------------------------------------------------------------------
// COMPOUND-CONDITION ATOMICITY (Convergence #7) — explicitly NOT fully
// atomic this phase, same posture Phase 2 documented for its own
// non-atomicity. `detectDeadlock` reads live agents (step 1) and the queue
// (step 2) as two SEPARATE file reads, not one consistent snapshot — a queue
// transition (drained/refilled) between those two reads can be observed as a
// momentarily-stale combination. This is accepted, not closed, for the same
// reason Phase 2 accepted it: full cross-file atomicity would require a
// single lock spanning two independently-lived files (the coordination file
// and the queue file), which neither of those modules' existing locking
// primitives provide today. What IS closed: `evaluateDeadlockWindow`'s own
// window/debounce math is single-consistent-read within itself (one `now`,
// one `agentLivenessResults` snapshot, one `stalledSince` read) — see
// queue-aging-escalation.mjs's header for the identical "single-consistent-
// read discipline, not cross-file atomicity" framing this suite reuses.
//
// SAMPLING SKEW (Convergence #1) — per-agent liveness checks land at
// slightly different wall-clock moments than the footprint-history samples
// feeding them. This module does not introduce a NEW mitigation for that
// skew beyond what it already inherits: `evaluateDeadlockWindow`'s sustained
// N-minute window IS the mitigation the ticket calls for — a single noisy
// beat (one skewed sample flipping one agent's classification) cannot trip
// the tripwire on its own, because `trip` only fires once the ALL-STALLED +
// NON-EMPTY-QUEUE condition has held continuously (per this module's
// persisted `stalledSince`) for the full `windowMs`, not on any one instant.
// See "does not trip on a single noisy beat before window elapses" below.
// ---------------------------------------------------------------------------

import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { declareDibs } from './coordination-file.mjs';
import { enqueueItem } from './queue.mjs';

import { detectDeadlock, evaluateDeadlockWindow, DEFAULT_DEADLOCK_WINDOW_MS } from './deadlock-tripwire.mjs';

const ONE_MINUTE_MS = 60 * 1000;

function stalledAgent(pid) {
  return { pid, stalled: true };
}
function activeAgent(pid) {
  return { pid, stalled: false };
}

describe('evaluateDeadlockWindow (pure core)', () => {
  const now = 1_000_000;
  const windowMs = 10 * ONE_MINUTE_MS;

  it('does not trip when the fleet is empty (zero live agents), even with a non-empty queue', () => {
    const result = evaluateDeadlockWindow({
      agentLivenessResults: [],
      queueNonEmpty: true,
      now,
      windowMs,
      stalledSince: null,
    });

    expect(result.trip).toBe(false);
    expect(result.stalledPids).toEqual([]);
    // Nothing to be "since" — an empty fleet is never in the stalled
    // condition, so the debounce clock must not start running.
    expect(result.stalledSince).toBeNull();
  });

  it('does not trip when the queue is empty, even with every live agent stalled', () => {
    const result = evaluateDeadlockWindow({
      agentLivenessResults: [stalledAgent('a'), stalledAgent('b')],
      queueNonEmpty: false,
      now,
      windowMs,
      stalledSince: null,
    });

    expect(result.trip).toBe(false);
    expect(result.stalledPids).toEqual([]);
    expect(result.stalledSince).toBeNull();
  });

  it('does not trip when any agent is active, even with a non-empty queue', () => {
    const result = evaluateDeadlockWindow({
      agentLivenessResults: [stalledAgent('a'), activeAgent('b'), stalledAgent('c')],
      queueNonEmpty: true,
      now,
      windowMs,
      stalledSince: null,
    });

    expect(result.trip).toBe(false);
    expect(result.stalledPids).toEqual([]);
    expect(result.stalledSince).toBeNull();
  });

  it('starts the debounce clock (does not trip) on the first beat the condition holds', () => {
    const result = evaluateDeadlockWindow({
      agentLivenessResults: [stalledAgent('a')],
      queueNonEmpty: true,
      now,
      windowMs,
      stalledSince: null, // cold start — condition has never been observed holding before
    });

    expect(result.trip).toBe(false);
    expect(result.stalledPids).toEqual([]);
    // The clock must now be started, at exactly `now` — this is the value a
    // caller persists for the NEXT beat.
    expect(result.stalledSince).toBe(now);
  });

  it('does not trip on a single noisy beat before the window elapses', () => {
    const stalledSince = now - (windowMs - 1); // one ms short of the full window
    const result = evaluateDeadlockWindow({
      agentLivenessResults: [stalledAgent('a')],
      queueNonEmpty: true,
      now,
      windowMs,
      stalledSince,
    });

    expect(result.trip).toBe(false);
    expect(result.stalledPids).toEqual([]);
    // The clock is NOT reset just because it hasn't fired yet — the caller
    // persists this unchanged `stalledSince` and keeps waiting.
    expect(result.stalledSince).toBe(stalledSince);
  });

  it('trips exactly when the condition has held for the full configured window (inclusive boundary)', () => {
    const stalledSince = now - windowMs; // exactly the full window, to the ms
    const result = evaluateDeadlockWindow({
      agentLivenessResults: [stalledAgent('a'), stalledAgent('b')],
      queueNonEmpty: true,
      now,
      windowMs,
      stalledSince,
    });

    expect(result.trip).toBe(true);
    expect(result.stalledPids.sort()).toEqual(['a', 'b']);
    expect(result.stalledSince).toBe(stalledSince);
  });

  it('trips when exactly one live agent is stalled and the queue is non-empty (100% of a fleet of one)', () => {
    const stalledSince = now - windowMs;
    const result = evaluateDeadlockWindow({
      agentLivenessResults: [stalledAgent('solo')],
      queueNonEmpty: true,
      now,
      windowMs,
      stalledSince,
    });

    expect(result.trip).toBe(true);
    expect(result.stalledPids).toEqual(['solo']);
  });

  it('resets the debounce clock when a previously-stalled fleet gains an active (newly-spawned) agent mid-window', () => {
    const staleStalledSince = now - (windowMs - 1); // was mid-window
    const result = evaluateDeadlockWindow({
      agentLivenessResults: [stalledAgent('a'), activeAgent('new-agent')],
      queueNonEmpty: true,
      now,
      windowMs,
      stalledSince: staleStalledSince,
    });

    expect(result.trip).toBe(false);
    expect(result.stalledPids).toEqual([]);
    // Reset to null, not left at the stale timestamp — a LATER beat where
    // the fleet goes back to 100% stalled must start a FRESH window, not
    // resurrect the old one.
    expect(result.stalledSince).toBeNull();
  });

  it('resets the debounce clock when the queue drains mid-window even though every agent is still stalled', () => {
    const staleStalledSince = now - (windowMs - 1);
    const result = evaluateDeadlockWindow({
      agentLivenessResults: [stalledAgent('a')],
      queueNonEmpty: false,
      now,
      windowMs,
      stalledSince: staleStalledSince,
    });

    expect(result.trip).toBe(false);
    expect(result.stalledSince).toBeNull();
  });

  it('does not trip beyond the fully-elapsed window either — remains tripped/sustained, not a one-shot pulse', () => {
    // A window that elapsed well in the past (not just exactly at the
    // boundary) must still report a trip — this is a sustained condition,
    // not an edge-triggered pulse that only fires the instant the boundary
    // is crossed.
    const stalledSince = now - windowMs - 5 * ONE_MINUTE_MS;
    const result = evaluateDeadlockWindow({
      agentLivenessResults: [stalledAgent('a')],
      queueNonEmpty: true,
      now,
      windowMs,
      stalledSince,
    });

    expect(result.trip).toBe(true);
    expect(result.stalledPids).toEqual(['a']);
  });

  it('reconstructs elapsed sustained-time from a persisted stalled-since timestamp, not from a freshly-zeroed counter (Convergence #6)', () => {
    // Simulates a process restart landing exactly at the debounce boundary:
    // the caller re-reads a `stalledSince` that was persisted by an earlier
    // (now-dead) process, well before `now`. This call must derive elapsed
    // time from THAT timestamp, not treat the restart as `stalledSince: null`
    // (which would silently reset and never trip) nor treat it as "now"
    // (which would double-count and trip a whole window early).
    const persistedStalledSince = now - windowMs; // set by a prior, separate invocation
    const result = evaluateDeadlockWindow({
      agentLivenessResults: [stalledAgent('a')],
      queueNonEmpty: true,
      now,
      windowMs,
      stalledSince: persistedStalledSince,
    });

    expect(result.trip).toBe(true);
    expect(result.stalledSince).toBe(persistedStalledSince);
  });

  it('never returns a duplicate pid in stalledPids even if the same identifier appears twice in agentLivenessResults', () => {
    const stalledSince = now - windowMs;
    const result = evaluateDeadlockWindow({
      agentLivenessResults: [stalledAgent('a'), stalledAgent('a')],
      queueNonEmpty: true,
      now,
      windowMs,
      stalledSince,
    });

    expect(result.trip).toBe(true);
    expect(result.stalledPids).toEqual(['a']);
  });
});

describe('DEFAULT_DEADLOCK_WINDOW_MS', () => {
  it('is a finite, positive number of milliseconds', () => {
    expect(typeof DEFAULT_DEADLOCK_WINDOW_MS).toBe('number');
    expect(Number.isFinite(DEFAULT_DEADLOCK_WINDOW_MS)).toBe(true);
    expect(DEFAULT_DEADLOCK_WINDOW_MS).toBeGreaterThan(0);
  });
});

describe('detectDeadlock (I/O wrapper)', () => {
  let workDir;
  let coordinationFilePath;
  let queueFilePath;
  let stateFilePath;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), 'deadlock-tripwire-lib-'));
    coordinationFilePath = join(workDir, 'resource-coordination.json');
    queueFilePath = join(workDir, 'resource-queue.json');
    stateFilePath = join(workDir, 'deadlock-tripwire-state.json');
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  async function seedQueueEntry(now) {
    await enqueueItem(queueFilePath, {
      agentClass: 'typescript-implementer',
      priority: 'normal',
      commandRef: 'some-command',
      orchestratorId: 'queue-owner',
    });
    return now;
  }

  async function seedLiveOrchestrator(orchestratorId, now) {
    await declareDibs(
      coordinationFilePath,
      { orchestratorId, desiredAgents: 1, declaredAt: now, firstDeclaredAt: now },
      { livenessThresholdMs: 15 * 60 * 1000 },
    );
  }

  it('does not trip when there is no coordination file yet (zero live agents) even with a non-empty queue', async () => {
    const now = 2_000_000;
    await seedQueueEntry(now);

    const result = await detectDeadlock(coordinationFilePath, queueFilePath, stateFilePath, {
      now,
      windowMs: 10 * ONE_MINUTE_MS,
    });

    expect(result.trip).toBe(false);
    expect(result.stalledPids).toEqual([]);
  });

  it('does not trip when the queue is empty even with a stalled live agent', async () => {
    const now = 2_000_000;
    await seedLiveOrchestrator('orc-1', now);

    const result = await detectDeadlock(coordinationFilePath, queueFilePath, stateFilePath, {
      now,
      windowMs: 10 * ONE_MINUTE_MS,
      // Flat trajectory + old worktree mtime => stalled, per isAgentStalled.
      getFootprintTrajectory: () => [10, 10, 10],
      getWorktreeMtimeMs: () => now - 60 * ONE_MINUTE_MS,
      boundMs: 5 * ONE_MINUTE_MS,
    });

    expect(result.trip).toBe(false);
    expect(result.stalledPids).toEqual([]);
  });

  it('does not trip with an active agent present, even with a non-empty queue and one stalled agent', async () => {
    const now = 2_000_000;
    await seedLiveOrchestrator('stalled-orc', now);
    await seedLiveOrchestrator('active-orc', now);
    await seedQueueEntry(now);

    const result = await detectDeadlock(coordinationFilePath, queueFilePath, stateFilePath, {
      now,
      windowMs: 10 * ONE_MINUTE_MS,
      boundMs: 5 * ONE_MINUTE_MS,
      getFootprintTrajectory: (orchestratorId) =>
        orchestratorId === 'active-orc' ? [10, 20, 30] : [10, 10, 10],
      getWorktreeMtimeMs: (orchestratorId) =>
        orchestratorId === 'active-orc' ? now : now - 60 * ONE_MINUTE_MS,
    });

    expect(result.trip).toBe(false);
    expect(result.stalledPids).toEqual([]);
  });

  it('does not trip on the first beat (single noisy sample) even when 100% stalled and queue is non-empty', async () => {
    const now = 2_000_000;
    await seedLiveOrchestrator('orc-1', now);
    await seedQueueEntry(now);

    const result = await detectDeadlock(coordinationFilePath, queueFilePath, stateFilePath, {
      now,
      windowMs: 10 * ONE_MINUTE_MS,
      boundMs: 5 * ONE_MINUTE_MS,
      getFootprintTrajectory: () => [10, 10, 10],
      getWorktreeMtimeMs: () => now - 60 * ONE_MINUTE_MS,
    });

    expect(result.trip).toBe(false);
    expect(result.stalledPids).toEqual([]);
  });

  it('trips once the all-stalled + non-empty-queue condition has been observed for the full window across separate invocations (persisted, not in-memory)', async () => {
    const windowMs = 10 * ONE_MINUTE_MS;
    const boundMs = 5 * ONE_MINUTE_MS;
    const firstBeatNow = 2_000_000;
    await seedLiveOrchestrator('orc-1', firstBeatNow);
    await seedQueueEntry(firstBeatNow);

    const stalledDeps = {
      windowMs,
      boundMs,
      getFootprintTrajectory: () => [10, 10, 10],
      getWorktreeMtimeMs: () => firstBeatNow - 60 * ONE_MINUTE_MS,
    };

    // First invocation (first process/beat): starts the window, must not trip.
    const first = await detectDeadlock(coordinationFilePath, queueFilePath, stateFilePath, {
      ...stalledDeps,
      now: firstBeatNow,
    });
    expect(first.trip).toBe(false);

    // Refresh the orchestrator's dibs liveness so it's still "live" at the
    // second beat (mirrors a real beat cadence) without changing its
    // liveness verdict (still flat/stale per the injected resolvers).
    await seedLiveOrchestrator('orc-1', firstBeatNow + windowMs);

    // Second invocation — simulates the watchdog beat PROCESS EXITING and a
    // brand-new `node` process picking the state back up off disk (no
    // shared in-memory state between these two calls at all: this is two
    // fully independent `detectDeadlock` calls against the same files).
    const second = await detectDeadlock(coordinationFilePath, queueFilePath, stateFilePath, {
      ...stalledDeps,
      now: firstBeatNow + windowMs,
    });

    expect(second.trip).toBe(true);
    expect(second.stalledPids).toEqual(['orc-1']);
  });

  it('resets the persisted window when a newly-spawned active agent joins mid-window, rather than falsely tripping on stale state', async () => {
    const windowMs = 10 * ONE_MINUTE_MS;
    const boundMs = 5 * ONE_MINUTE_MS;
    const firstBeatNow = 2_000_000;
    await seedLiveOrchestrator('orc-1', firstBeatNow);
    await seedQueueEntry(firstBeatNow);

    // First beat: 100% stalled, queue non-empty — starts the window.
    const first = await detectDeadlock(coordinationFilePath, queueFilePath, stateFilePath, {
      now: firstBeatNow,
      windowMs,
      boundMs,
      getFootprintTrajectory: () => [10, 10, 10],
      getWorktreeMtimeMs: () => firstBeatNow - 60 * ONE_MINUTE_MS,
    });
    expect(first.trip).toBe(false);

    // A brand-new agent joins mid-window and is genuinely active.
    const midWindowNow = firstBeatNow + windowMs - 1;
    await seedLiveOrchestrator('orc-1', midWindowNow);
    await seedLiveOrchestrator('new-agent', midWindowNow);

    const second = await detectDeadlock(coordinationFilePath, queueFilePath, stateFilePath, {
      now: midWindowNow,
      windowMs,
      boundMs,
      getFootprintTrajectory: (orchestratorId) =>
        orchestratorId === 'new-agent' ? [10, 20, 30] : [10, 10, 10],
      getWorktreeMtimeMs: (orchestratorId) =>
        orchestratorId === 'new-agent' ? midWindowNow : firstBeatNow - 60 * ONE_MINUTE_MS,
    });
    expect(second.trip).toBe(false);

    // Even if `new-agent` later disappears and the fleet returns to 100%
    // stalled at what WOULD have been the original window's boundary, the
    // clock must have been reset by the mid-window join — a beat exactly at
    // the ORIGINAL boundary must NOT trip, because the sustained condition
    // was interrupted.
    const thirdBeatNow = firstBeatNow + windowMs;
    await seedLiveOrchestrator('orc-1', thirdBeatNow);

    const third = await detectDeadlock(coordinationFilePath, queueFilePath, stateFilePath, {
      now: thirdBeatNow,
      windowMs,
      boundMs,
      getFootprintTrajectory: () => [10, 10, 10],
      getWorktreeMtimeMs: () => firstBeatNow - 60 * ONE_MINUTE_MS,
    });
    expect(third.trip).toBe(false);
  });

  it('tolerates a corrupt/unreadable coordination-file snapshot (concurrent claim/release race) by treating it as a no-trip beat, never throwing', async () => {
    const { writeFileSync } = await import('node:fs');
    // A torn/partial write left mid-race — not valid JSON.
    writeFileSync(coordinationFilePath, '{"not":"valid-json-array"', 'utf8');
    await seedQueueEntry(2_000_000);

    await expect(
      detectDeadlock(coordinationFilePath, queueFilePath, stateFilePath, {
        now: 2_000_000,
        windowMs: 10 * ONE_MINUTE_MS,
      }),
    ).resolves.toEqual(expect.objectContaining({ trip: false, stalledPids: [] }));
  });

  it('tolerates a not-yet-created queue file (treated as empty) without throwing', async () => {
    const now = 2_000_000;
    await seedLiveOrchestrator('orc-1', now);

    const result = await detectDeadlock(coordinationFilePath, queueFilePath, stateFilePath, {
      now,
      windowMs: 10 * ONE_MINUTE_MS,
      boundMs: 5 * ONE_MINUTE_MS,
      getFootprintTrajectory: () => [10, 10, 10],
      getWorktreeMtimeMs: () => now - 60 * ONE_MINUTE_MS,
    });

    expect(result.trip).toBe(false);
    expect(result.stalledPids).toEqual([]);
  });

  it('prunes a stale/orphaned dibs entry (declared past the default liveness threshold, never re-declared) so it cannot permanently block the trip for the rest of the fleet (High finding — readDibs was called with no livenessThresholdMs)', async () => {
    const windowMs = 10 * ONE_MINUTE_MS;
    const boundMs = 5 * ONE_MINUTE_MS;
    const defaultLivenessThresholdMs = 15 * ONE_MINUTE_MS;
    const firstBeatNow = 2_000_000;
    const ghostId = 'crashed-orchestrator';

    // Both entries below are seeded via `declareDibs` with NO
    // `livenessThresholdMs` option, so nothing prunes either of them at WRITE
    // time (unlike `seedLiveOrchestrator`, which passes a threshold and would
    // otherwise prune the ghost as a side effect of writing 'orc-1', muddying
    // which code path actually removed it). The only thing that can remove
    // the ghost in this test is `readDibs` pruning it at READ time inside
    // `detectDeadlock` — exactly the code path this regression test targets.

    // A dibs entry from an orchestrator that has since crashed: declared once,
    // already well past the default liveness threshold at `firstBeatNow`, and
    // NEVER re-declared for the rest of this test — exactly the scenario the
    // reviewer flagged as uncovered ("every seeded test entry is re-declared
    // fresh each beat").
    await declareDibs(coordinationFilePath, {
      orchestratorId: ghostId,
      desiredAgents: 1,
      declaredAt: firstBeatNow - defaultLivenessThresholdMs - ONE_MINUTE_MS,
      firstDeclaredAt: firstBeatNow - defaultLivenessThresholdMs - ONE_MINUTE_MS,
    });

    // A genuinely-live orchestrator, re-declared fresh at each beat, whose
    // liveness data says it IS stalled.
    await declareDibs(coordinationFilePath, {
      orchestratorId: 'orc-1',
      desiredAgents: 1,
      declaredAt: firstBeatNow,
      firstDeclaredAt: firstBeatNow,
    });
    await seedQueueEntry(firstBeatNow);

    // If the ghost entry were NOT pruned, its liveness would resolve as
    // genuinely active (fresh trajectory + fresh mtime) — which would keep
    // `evaluateDeadlockWindow`'s ALL-STALLED condition permanently false and
    // the tripwire would never fire, no matter how long the window runs.
    const resolvers = {
      windowMs,
      boundMs,
      getFootprintTrajectory: (orchestratorId) => (orchestratorId === ghostId ? [10, 20, 30] : [10, 10, 10]),
      getWorktreeMtimeMs: (orchestratorId) => (orchestratorId === ghostId ? firstBeatNow : firstBeatNow - 60 * ONE_MINUTE_MS),
    };

    // First beat: starts the window (must not trip yet either way).
    const first = await detectDeadlock(coordinationFilePath, queueFilePath, stateFilePath, {
      ...resolvers,
      now: firstBeatNow,
    });
    expect(first.trip).toBe(false);
    expect(first.stalledPids).toEqual([]);

    // Refresh only the genuinely-live orchestrator at the second beat — the
    // ghost entry is deliberately never re-declared.
    await declareDibs(coordinationFilePath, {
      orchestratorId: 'orc-1',
      desiredAgents: 1,
      declaredAt: firstBeatNow + windowMs,
      firstDeclaredAt: firstBeatNow,
    });

    const second = await detectDeadlock(coordinationFilePath, queueFilePath, stateFilePath, {
      ...resolvers,
      now: firstBeatNow + windowMs,
    });

    // With the ghost correctly pruned by `readDibs`, the live fleet is just
    // `['orc-1']` — 100% stalled, queue non-empty, full window elapsed — so
    // the tripwire fires, and the ghost's id never appears in `stalledPids`.
    expect(second.trip).toBe(true);
    expect(second.stalledPids).toEqual(['orc-1']);
  });
});
