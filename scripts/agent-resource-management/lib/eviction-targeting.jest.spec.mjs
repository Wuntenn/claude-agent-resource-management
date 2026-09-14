// Phase 4 — recycle wiring: resolves watchdog/deadlock trips into
// eviction targets for the real --evict-pid surface.
//
// RED by construction: `./eviction-targeting.mjs` does not exist yet — every
// assertion below fails on the import itself until Phase 4's implementer
// creates it.
//
// ---------------------------------------------------------------------------
// DESIGN DECISIONS PINNED BY THIS FILE (read before extending or objecting)
// ---------------------------------------------------------------------------
//
// 1. TWO EXPORTS, BOTH PURE/SYNCHRONOUS — no fs, no child_process, no
//    `Date.now()`. This module is a COMPOSITION/DECISION layer only: it
//    decides WHO gets targeted and WHY, never itself calls the real
//    `handleEvict`/`--evict-pid` I/O surface. Phase 6's beat wiring is
//    expected to call `resolveEvictionTargets` first, then drive the actual
//    `cli.mjs --evict-pid` invocation per returned `action: 'evict'` target,
//    then optionally run that invocation's JSON result through
//    `classifyEvictionOutcome`. Keeping the risky "who gets targeted"
//    decision independently unit-testable (per this file) without needing to
//    fake real eviction I/O in every test is the whole point of this split —
//    see cli-evict.jest.spec.mjs's own `ARM_EVICT_TEST_MODE` seam for how
//    heavy that fake-I/O harness already is; this module deliberately never
//    needs it.
//
//      resolveEvictionTargets({ watchdogTrips, deadlockTrip, claimLedger, recentlyEvictedPids })
//        -> Array<{ pid, leaseId, orchestratorPid, orchestratorId,
//                    action: 'evict' | 'alert-only' | 'skip', skipReason? }>
//
//      classifyEvictionOutcome(handleEvictResult)
//        -> { recycled: boolean, reason?: string }
//
// 2. INPUT SHAPES.
//
//    `watchdogTrips` — an array of Phase 1 per-agent-watchdog trips, each
//    `{ pid }` (a REAL OS pid — `isAgentStalled`'s own `pid` param, per
//    ./liveness.mjs's doc comment, is threaded from a real process).
//
//    `deadlockTrip` — Phase 3's `evaluateDeadlockWindow`/`detectDeadlock`
//    return shape verbatim: `{ trip: boolean, stalledPids: Array<string> }`.
//    Per ./deadlock-tripwire.mjs's own header/doc comments, dibs entries
//    carry NO os pid at all — `stalledPids` actually holds `orchestratorId`
//    values (the documented `orchestratorId`-as-`pid` substitution). This
//    module therefore looks up a deadlock-trip entry in `claimLedger` BY
//    `orchestratorId`, never by `pid` — there is no real pid to look up by
//    until the ledger resolves one.
//
//    `claimLedger` — the ONE TRUSTED SOURCE this module is allowed to read
//    `pid`/`leaseId`/`orchestratorPid`/`orchestratorId` from. An array of
//    records: `{ pid, leaseId, orchestratorPid, orchestratorId, reentrant? }`.
//    This is expected to be assembled by Phase 6's I/O wiring from the
//    coordination-file dibs ledger + the queue's own lease-claim data (see
//    ./queue.mjs's `claimQueueEntry`/`leaseId` machinery) — i.e. from data an
//    orchestrator declared about itself and its own real spawned pids, NOT
//    from a queue entry's own free-text `commandRef`/`priority` fields. A
//    watchdog trip is matched to a ledger record by `record.pid ===
//    trip.pid`; a deadlock-trip stalled entry is matched by
//    `record.orchestratorId === stalledPid`. CRITICAL: any `leaseId`/
//    `orchestratorPid`/`commandRef`/`priority`-shaped field riding along on
//    the TRIP objects themselves (watchdogTrips/deadlockTrip) is NEVER read
//    for target composition — only the matching `claimLedger` record's own
//    fields are used. See test group 6 below (Convergence #4) for the
//    adversarial-input proof of this.
//
//    `recentlyEvictedPids` (optional) — a Set/array of pids to exclude from
//    targeting entirely (`action: 'skip'`), the hook Convergence #2
//    (flapping) calls for — a real cooldown/backoff policy is out of scope
//    for this phase; this is only the plumbing seam a future one plugs into.
//
// 3. RE-ENTRANCY POLICY (conservative default, per the Build Plan). A
//    resolved ledger record with `reentrant === false`, OR with no
//    `reentrant` field at all (unknown), routes to `action: 'alert-only'` —
//    NEVER auto-evicted. Only `reentrant === true` (explicit opt-in) routes
//    to `action: 'evict'`. This is a policy decision made explicitly in code
//    (this module) and proven by test, not left as an implicit default
//    anywhere else.
//
// 4. UNRESOLVABLE TARGETS FAIL CLOSED. A watchdog/deadlock trip with no
//    matching `claimLedger` record produces `action: 'skip'` with a
//    `skipReason` — this module must never fabricate/guess a `leaseId`/
//    `orchestratorPid` for a trip it cannot resolve.
//
// 5. NO DOUBLE-TARGETING IN ONE BEAT. When the SAME pid is resolvable via
//    both a watchdog trip and a deadlock-trip stalled entry in a single
//    call, the returned list contains exactly one entry for that pid.
//
// ---------------------------------------------------------------------------

import { resolveEvictionTargets, classifyEvictionOutcome, filterEvictable } from './eviction-targeting.mjs';

describe('eviction-targeting', () => {
  describe('resolveEvictionTargets', () => {
    it('returns an empty list when there are no watchdog trips and no deadlock trip', () => {
      const result = resolveEvictionTargets({
        watchdogTrips: [],
        deadlockTrip: { trip: false, stalledPids: [] },
        claimLedger: [],
      });

      expect(result).toEqual([]);
    });

    it('per-agent trip calls evict with correct lease/orchestrator ids', () => {
      const claimLedger = [
        { pid: 123, leaseId: 'lease-abc', orchestratorPid: 999, orchestratorId: 'orch-a', reentrant: true },
      ];

      const result = resolveEvictionTargets({
        watchdogTrips: [{ pid: 123 }],
        deadlockTrip: { trip: false, stalledPids: [] },
        claimLedger,
      });

      expect(result).toEqual([
        {
          pid: 123,
          leaseId: 'lease-abc',
          orchestratorPid: 999,
          orchestratorId: 'orch-a',
          action: 'evict',
        },
      ]);
    });

    it('deadlock trip calls evict for each stalled pid (matched via orchestratorId, per the documented pid substitution)', () => {
      const claimLedger = [
        { pid: 201, leaseId: 'lease-b', orchestratorPid: 501, orchestratorId: 'orch-b', reentrant: true },
        { pid: 202, leaseId: 'lease-c', orchestratorPid: 502, orchestratorId: 'orch-c', reentrant: true },
      ];

      const result = resolveEvictionTargets({
        watchdogTrips: [],
        deadlockTrip: { trip: true, stalledPids: ['orch-b', 'orch-c'] },
        claimLedger,
      });

      expect(result).toEqual([
        { pid: 201, leaseId: 'lease-b', orchestratorPid: 501, orchestratorId: 'orch-b', action: 'evict' },
        { pid: 202, leaseId: 'lease-c', orchestratorPid: 502, orchestratorId: 'orch-c', action: 'evict' },
      ]);
    });

    it('produces no deadlock-derived targets when deadlockTrip.trip is false, even if stalledPids is non-empty', () => {
      const claimLedger = [
        { pid: 201, leaseId: 'lease-b', orchestratorPid: 501, orchestratorId: 'orch-b', reentrant: true },
      ];

      const result = resolveEvictionTargets({
        watchdogTrips: [],
        // A caller passing a stale/malformed deadlockTrip shape (trip false
        // but stalledPids still populated) must not leak targets from it.
        deadlockTrip: { trip: false, stalledPids: ['orch-b'] },
        claimLedger,
      });

      expect(result).toEqual([]);
    });

    it('skips eviction when the claim ledger has no matching entry for a watchdog pid', () => {
      const result = resolveEvictionTargets({
        watchdogTrips: [{ pid: 456 }],
        deadlockTrip: { trip: false, stalledPids: [] },
        claimLedger: [],
      });

      expect(result).toHaveLength(1);
      expect(result[0]).toMatchObject({ pid: 456, action: 'skip' });
      expect(typeof result[0].skipReason).toBe('string');
      expect(result[0].skipReason.length).toBeGreaterThan(0);
      // Must never guess/fabricate identifiers for an unresolvable target.
      expect(result[0]).not.toHaveProperty('leaseId');
      expect(result[0]).not.toHaveProperty('orchestratorPid');
    });

    it('skips eviction when the claim ledger has no matching entry for a deadlock-trip orchestratorId', () => {
      const result = resolveEvictionTargets({
        watchdogTrips: [],
        deadlockTrip: { trip: true, stalledPids: ['orch-unknown'] },
        claimLedger: [],
      });

      expect(result).toHaveLength(1);
      expect(result[0].action).toBe('skip');
      expect(typeof result[0].skipReason).toBe('string');
      // The orchestratorId is known and trusted (it came from detectDeadlock's
      // own stalledPids), so it must be retained even when unresolvable.
      expect(result[0].orchestratorId).toBe('orch-unknown');
      // No real pid was ever resolvable for this stalled orchestratorId.
      expect(result[0]).not.toHaveProperty('leaseId');
      expect(result[0]).not.toHaveProperty('orchestratorPid');
    });

    it('routes to alert-only when the ledger entry explicitly marks the agent class non-re-entrant', () => {
      const claimLedger = [
        { pid: 789, leaseId: 'lease-x', orchestratorPid: 111, orchestratorId: 'orch-x', reentrant: false },
      ];

      const result = resolveEvictionTargets({
        watchdogTrips: [{ pid: 789 }],
        deadlockTrip: { trip: false, stalledPids: [] },
        claimLedger,
      });

      expect(result).toEqual([
        {
          pid: 789,
          leaseId: 'lease-x',
          orchestratorPid: 111,
          orchestratorId: 'orch-x',
          action: 'alert-only',
        },
      ]);
    });

    it('routes to alert-only when the ledger entry carries no reentrant flag at all (unknown treated as non-re-entrant)', () => {
      const claimLedger = [{ pid: 790, leaseId: 'lease-y', orchestratorPid: 112, orchestratorId: 'orch-y' }];

      const result = resolveEvictionTargets({
        watchdogTrips: [{ pid: 790 }],
        deadlockTrip: { trip: false, stalledPids: [] },
        claimLedger,
      });

      expect(result).toEqual([
        {
          pid: 790,
          leaseId: 'lease-y',
          orchestratorPid: 112,
          orchestratorId: 'orch-y',
          action: 'alert-only',
        },
      ]);
    });

    it('does not target a pid present in recentlyEvictedPids (the flapping-cooldown hook)', () => {
      const claimLedger = [
        { pid: 321, leaseId: 'lease-z', orchestratorPid: 222, orchestratorId: 'orch-z', reentrant: true },
      ];

      const result = resolveEvictionTargets({
        watchdogTrips: [{ pid: 321 }],
        deadlockTrip: { trip: false, stalledPids: [] },
        claimLedger,
        recentlyEvictedPids: new Set([321]),
      });

      expect(result).toEqual([{ pid: 321, action: 'skip', skipReason: expect.any(String) }]);
    });

    it('accepts recentlyEvictedPids as a plain array, not only a Set', () => {
      const claimLedger = [
        { pid: 322, leaseId: 'lease-zz', orchestratorPid: 223, orchestratorId: 'orch-zz', reentrant: true },
      ];

      const result = resolveEvictionTargets({
        watchdogTrips: [{ pid: 322 }],
        deadlockTrip: { trip: false, stalledPids: [] },
        claimLedger,
        recentlyEvictedPids: [322],
      });

      expect(result[0].action).toBe('skip');
    });

    it('does not target the same pid twice when both a watchdog trip and a deadlock trip flag it in the same beat', () => {
      const claimLedger = [
        { pid: 555, leaseId: 'lease-dup', orchestratorPid: 333, orchestratorId: 'orch-dup', reentrant: true },
      ];

      const result = resolveEvictionTargets({
        watchdogTrips: [{ pid: 555 }],
        deadlockTrip: { trip: true, stalledPids: ['orch-dup'] },
        claimLedger,
      });

      expect(result).toHaveLength(1);
      expect(result[0]).toMatchObject({ pid: 555, action: 'evict' });
    });

    it('Convergence #4 — derives pid/leaseId/orchestratorPid ONLY from claimLedger, never from an attacker-controlled-looking field riding on the trip/queue-entry object itself', () => {
      const claimLedger = [
        { pid: 42, leaseId: 'trusted-lease', orchestratorPid: 4200, orchestratorId: 'trusted-orch', reentrant: true },
      ];

      // Simulates a Phase 2 queue-aging-escalation entry whose own mutable
      // fields (commandRef/priority) — or an entirely spoofed leaseId/
      // orchestratorPid riding along on the trip object — must NEVER
      // influence which pid gets targeted or with which identifiers.
      const result = resolveEvictionTargets({
        watchdogTrips: [
          {
            pid: 42,
            leaseId: 'ATTACKER-CONTROLLED-LEASE',
            orchestratorPid: 666,
            commandRef: 'rm -rf /',
            priority: 'high',
          },
        ],
        deadlockTrip: { trip: false, stalledPids: [] },
        claimLedger,
      });

      expect(result).toEqual([
        {
          pid: 42,
          leaseId: 'trusted-lease',
          orchestratorPid: 4200,
          orchestratorId: 'trusted-orch',
          action: 'evict',
        },
      ]);
    });

    it('Convergence #4 — a malformed/attacker-shaped stalledPids entry that does not match any claimLedger orchestratorId is skipped, not coerced into targeting an unrelated ledger record', () => {
      const claimLedger = [
        { pid: 42, leaseId: 'trusted-lease', orchestratorPid: 4200, orchestratorId: 'trusted-orch', reentrant: true },
      ];

      const result = resolveEvictionTargets({
        watchdogTrips: [],
        // Neither the empty string nor "__proto__"/"constructor"-shaped
        // values may accidentally resolve via prototype-chain lookup to any
        // ledger record.
        deadlockTrip: { trip: true, stalledPids: ['__proto__', 'constructor', ''] },
        claimLedger,
      });

      expect(result).toHaveLength(3);
      for (const entry of result) {
        expect(entry.action).toBe('skip');
      }
    });

    // pre-PR review round 2, Medium — pins finding 1's fix: the
    // deadlock-trip liveness signal and this module's target resolution can
    // disagree about WHICH of an orchestrator's several candidate pids was
    // actually judged stalled when more than one claimLedger record shares
    // that orchestratorId. `countLedgerRecordsForOrchestrator` gates this —
    // proven here by exercising the actual `resolveEvictionTargets` deadlock
    // branch, not by calling the gate helper directly.
    it('routes a deadlock-trip stalled orchestratorId with MULTIPLE claimLedger candidates to alert-only, never evict, even when reentrant is true', () => {
      const claimLedger = [
        { pid: 601, leaseId: 'lease-multi-1', orchestratorPid: 701, orchestratorId: 'orch-multi', reentrant: true },
        { pid: 602, leaseId: 'lease-multi-2', orchestratorPid: 701, orchestratorId: 'orch-multi', reentrant: true },
      ];

      const result = resolveEvictionTargets({
        watchdogTrips: [],
        deadlockTrip: { trip: true, stalledPids: ['orch-multi'] },
        claimLedger,
      });

      expect(result).toHaveLength(1);
      expect(result[0].action).toBe('alert-only');
      expect(result[0].action).not.toBe('evict');
      // The gate resolves the first-matching ledger record's identifiers —
      // confirm no evict-only fields (e.g. skipReason) leak onto this shape.
      expect(result[0]).toEqual({
        pid: 601,
        leaseId: 'lease-multi-1',
        orchestratorPid: 701,
        orchestratorId: 'orch-multi',
        action: 'alert-only',
      });
    });

    it('a deadlock-trip stalled orchestratorId with exactly ONE claimLedger candidate keeps the normal reentrant-driven evict/alert-only split (the multi-candidate gate does not over-trigger)', () => {
      const reentrantLedger = [
        { pid: 611, leaseId: 'lease-single-evict', orchestratorPid: 711, orchestratorId: 'orch-single-a', reentrant: true },
      ];

      const evictResult = resolveEvictionTargets({
        watchdogTrips: [],
        deadlockTrip: { trip: true, stalledPids: ['orch-single-a'] },
        claimLedger: reentrantLedger,
      });

      expect(evictResult).toEqual([
        {
          pid: 611,
          leaseId: 'lease-single-evict',
          orchestratorPid: 711,
          orchestratorId: 'orch-single-a',
          action: 'evict',
        },
      ]);

      const nonReentrantLedger = [
        { pid: 612, leaseId: 'lease-single-alert', orchestratorPid: 712, orchestratorId: 'orch-single-b', reentrant: false },
      ];

      const alertResult = resolveEvictionTargets({
        watchdogTrips: [],
        deadlockTrip: { trip: true, stalledPids: ['orch-single-b'] },
        claimLedger: nonReentrantLedger,
      });

      expect(alertResult).toEqual([
        {
          pid: 612,
          leaseId: 'lease-single-alert',
          orchestratorPid: 712,
          orchestratorId: 'orch-single-b',
          action: 'alert-only',
        },
      ]);
    });
  });

  describe('filterEvictable', () => {
    it('returns only the evict-action entries from a mixed array of evict/alert-only/skip', () => {
      const mixed = [
        { pid: 1, leaseId: 'lease-1', orchestratorPid: 100, orchestratorId: 'orch-1', action: 'evict' },
        { pid: 2, leaseId: 'lease-2', orchestratorPid: 200, orchestratorId: 'orch-2', action: 'alert-only' },
        { pid: 3, action: 'skip', skipReason: 'recently evicted — cooldown in effect' },
        { pid: 4, leaseId: 'lease-4', orchestratorPid: 400, orchestratorId: 'orch-4', action: 'evict' },
      ];

      const result = filterEvictable(mixed);

      expect(result).toEqual([
        { pid: 1, leaseId: 'lease-1', orchestratorPid: 100, orchestratorId: 'orch-1', action: 'evict' },
        { pid: 4, leaseId: 'lease-4', orchestratorPid: 400, orchestratorId: 'orch-4', action: 'evict' },
      ]);
    });

    it('returns an empty array when none of the entries are evictable', () => {
      const mixed = [
        { pid: 2, leaseId: 'lease-2', orchestratorPid: 200, orchestratorId: 'orch-2', action: 'alert-only' },
        { pid: 3, action: 'skip', skipReason: 'no matching claimLedger record for watchdog-trip pid' },
      ];

      expect(filterEvictable(mixed)).toEqual([]);
    });

    it('returns an empty array (not throwing) on an empty input array', () => {
      expect(filterEvictable([])).toEqual([]);
    });
  });

  describe('classifyEvictionOutcome', () => {
    it('classifies a successful evict result (no evictionAttempted, released: true) as recycled', () => {
      const handleEvictSuccess = {
        type: 'evict',
        pid: 123,
        outcome: 'killed-after-grace',
        signalsSent: ['SIGTERM', 'SIGKILL'],
        released: true,
        item: { orchestratorId: 'orch-a', agentClass: 'implementer' },
      };

      expect(classifyEvictionOutcome(handleEvictSuccess)).toEqual({ recycled: true });
    });

    it('classifies a pre-attempt fallback failure (evictionAttempted: true, reason set) as not recycled, exposing the reason distinctly from success', () => {
      const handleEvictFailure = {
        type: 'pause',
        evictionAttempted: true,
        reason: 'not-authorized',
      };

      expect(classifyEvictionOutcome(handleEvictFailure)).toEqual({
        recycled: false,
        reason: 'not-authorized',
      });
    });

    // pre-PR review, Medium — `performEviction`'s orchestrator-pid-
    // binding refusal is a SECURITY-gate outcome, and it was shipped without
    // being added to `KNOWN_FAILURE_REASONS`, so `runWatchdogBeat` stamped it
    // as `outcome: 'unknown'` / `reason: 'unknown'` on both its JSON body and
    // its liveness NDJSON log — the exact silent masking this module forbids.
    it('classifies an orchestrator-pid-mismatch refusal (evictionAttempted: true, reason set) distinctly, not as unknown', () => {
      const handleEvictFailure = {
        type: 'hold',
        evictionAttempted: true,
        reason: 'orchestrator-pid-mismatch',
      };

      expect(classifyEvictionOutcome(handleEvictFailure)).toEqual({
        recycled: false,
        reason: 'orchestrator-pid-mismatch',
      });
    });

    // Phase 2 review, High — `performEviction`'s pid-lease-mismatch
    // refusal is a SECURITY-gate outcome (a numeric-pid check against the
    // pid `bindPidToLease` bound at claim time), and it was shipped without
    // being added to `KNOWN_FAILURE_REASONS`, so `runWatchdogBeat` stamped it
    // as `outcome: 'unknown'` / `reason: 'unknown'` on both its JSON body and
    // its liveness NDJSON log — the exact silent masking this module forbids.
    it('classifies a pid-lease-mismatch refusal (evictionAttempted: true, reason set) distinctly, not as unknown', () => {
      const handleEvictFailure = {
        type: 'hold',
        evictionAttempted: true,
        reason: 'pid-lease-mismatch',
      };

      expect(classifyEvictionOutcome(handleEvictFailure)).toEqual({
        recycled: false,
        reason: 'pid-lease-mismatch',
      });
    });

    it('classifies a late graceful-stop failure (evictionAttempted: true, reason: graceful-stop-failed) as not recycled', () => {
      const handleEvictFailure = {
        type: 'evict',
        pid: 123,
        evictionAttempted: true,
        outcome: 'graceful-stop-failed',
        reason: 'graceful-stop-failed',
        error: 'EPERM',
        released: false,
        item: null,
      };

      const result = classifyEvictionOutcome(handleEvictFailure);
      expect(result.recycled).toBe(false);
      expect(result.reason).toBe('graceful-stop-failed');
    });

    it('classifies a confirmed-stopped-but-release-failed outcome (evictionAttempted: true, reason: release-failed) as not recycled', () => {
      const handleEvictFailure = {
        type: 'evict',
        pid: 123,
        evictionAttempted: true,
        outcome: 'killed-after-grace',
        signalsSent: ['SIGTERM', 'SIGKILL'],
        reason: 'release-failed',
        released: false,
      };

      expect(classifyEvictionOutcome(handleEvictFailure)).toEqual({
        recycled: false,
        reason: 'release-failed',
      });
    });

    it('never throws on a malformed/unexpected result shape — degrades to not-recycled with an "unknown" reason', () => {
      expect(classifyEvictionOutcome(null)).toEqual({ recycled: false, reason: 'unknown' });
      expect(classifyEvictionOutcome(undefined)).toEqual({ recycled: false, reason: 'unknown' });
      expect(classifyEvictionOutcome({})).toEqual({ recycled: false, reason: 'unknown' });
    });

    // Real `handleEvict` failure shapes (cli.mjs ~3610-3651) that carry only
    // an `outcome` field — no `reason` key at all — on the branch where the
    // target is confirmed still running (or its identity may have changed
    // under the grace window). These must classify distinctly, not collapse
    // to the generic 'unknown' a malformed input would also produce.
    it('classifies a permission-denied outcome (evictionAttempted: true, outcome set, no reason key) distinctly, not as unknown', () => {
      const handleEvictFailure = {
        type: 'evict',
        pid: 123,
        evictionAttempted: true,
        outcome: 'permission-denied',
        signalsSent: ['SIGTERM'],
        released: false,
      };

      expect(classifyEvictionOutcome(handleEvictFailure)).toEqual({
        recycled: false,
        reason: 'permission-denied',
      });
    });

    it('classifies an unconfirmed-after-kill outcome (evictionAttempted: true, outcome set, no reason key) distinctly, not as unknown', () => {
      const handleEvictFailure = {
        type: 'evict',
        pid: 123,
        evictionAttempted: true,
        outcome: 'unconfirmed-after-kill',
        signalsSent: ['SIGTERM', 'SIGKILL'],
        released: false,
      };

      expect(classifyEvictionOutcome(handleEvictFailure)).toEqual({
        recycled: false,
        reason: 'unconfirmed-after-kill',
      });
    });

    it('classifies a target-changed-before-kill outcome (evictionAttempted: true, outcome set, no reason key) distinctly, not as unknown', () => {
      const handleEvictFailure = {
        type: 'evict',
        pid: 123,
        evictionAttempted: true,
        outcome: 'target-changed-before-kill',
        signalsSent: ['SIGTERM'],
        released: false,
      };

      expect(classifyEvictionOutcome(handleEvictFailure)).toEqual({
        recycled: false,
        reason: 'target-changed-before-kill',
      });
    });

    it('classifies a release-not-found outcome (evictionAttempted: true, reason: release-not-found) as not recycled', () => {
      const handleEvictFailure = {
        type: 'evict',
        pid: 123,
        evictionAttempted: true,
        outcome: 'killed-after-grace',
        signalsSent: ['SIGTERM', 'SIGKILL'],
        reason: 'release-not-found',
        released: false,
      };

      expect(classifyEvictionOutcome(handleEvictFailure)).toEqual({
        recycled: false,
        reason: 'release-not-found',
      });
    });
  });
});
