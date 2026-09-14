// Phase 2 — ARM dispatcher: claim-then-admit composition.
//
// Composes two PEER modules — ./queue.mjs's lease primitives
// (claimQueueEntry/releaseQueueEntry) and ./coordination-file.mjs's
// reserveAdmission — into the single cross-file atomic operation a
// dispatcher needs: "claim the top-priority eligible queue item, THEN ask
// the shared coordination-file ledger whether there's capacity to actually
// admit it". Named `dispatch.mjs` (not `queue-dispatch.mjs`) deliberately —
// see dispatch.jest.spec.mjs's header comment for the full naming
// rationale: this module depends on coordination-file.mjs's lock exactly as
// much as it depends on queue.mjs's, so it doesn't belong "under" either.
//
// The two locks (queue file's `<path>.lock` and coordination file's own
// lock) are NEVER held simultaneously — the queue lock is acquired and
// released entirely within `claimQueueEntry`/`releaseQueueEntry`'s own
// critical sections before `reserveAdmission` ever acquires the
// coordination-file lock. This module itself holds no lock of its own; it
// only sequences calls into the two lower modules' own locks.

import { claimQueueEntry, releaseQueueEntry } from './queue.mjs';
import { reserveAdmission, DEFAULT_CLAIM_TTL_MS } from './coordination-file.mjs';

// Mirrors cli.mjs's own `LIVE_AGENT_CLAIM_TYPE` export ('live-agent') as a
// literal, deliberately NOT imported from cli.mjs — cli.mjs runs `main()`
// unconditionally at module-evaluation time with no `import.meta.url` guard,
// so importing anything from it here would run that beat as a side effect
// of loading this module (see dispatch.jest.spec.mjs's header comment for
// the fuller rationale; this mirrors that same file's own
// `DEFAULT_FRESHNESS_WINDOW_MS` literal for the identical reason).
const LIVE_AGENT_CLAIM_TYPE = 'live-agent';

/**
 * Claims the top-priority eligible entry from the durable work queue at
 * `queueFilePath`, then attempts to admit it against the shared
 * coordination-file ledger at `coordinationFilePath` — the composed
 * "claim-then-admit" operation a dispatcher needs before it may safely spawn
 * a real OS process for a queued item.
 *
 * `params.snapshotType`/`params.admissionType` MUST be the SAME
 * `LIVE_AGENT_CLAIM_TYPE`/`LIVE_AGENT_ADMISSION_CLAIM_TYPE` pair cli.mjs's
 * `--desired-agents` beat already passes to `reserveAdmission` — see
 * dispatch.jest.spec.mjs's header comment for the full "why a third claim
 * type would silently escape the shared ceiling" rationale. `requestedCount`
 * is always exactly 1: one queue item is one live-agent-admission unit, not
 * caller-configurable in this phase.
 *
 * Execution order (per the Build Plan — deliberately not
 * reorderable):
 *   1. `claimQueueEntry` — queue lock, released before step 2 begins.
 *   2. Nothing eligible to claim (`null`): return `{ granted: false, reason:
 *      'queue-empty' }` WITHOUT ever calling `reserveAdmission` — this is
 *      the single choke point that keeps an empty-queue dispatch beat from
 *      touching the coordination-file lock at all.
 *   3. `reserveAdmission` — a separate critical section (coordination-file
 *      lock), reading the ledger fresh every call; no caching here.
 *   4. `granted >= 1`: return `{ granted: true, item, leaseId }` WITHOUT
 *      acking — acking (or releasing, on a later failure to actually spawn)
 *      is the caller's job (Phase 4), only after it has genuinely spawned.
 *   5. `granted === 0`: `releaseQueueEntry` immediately (the claimed item's
 *      ORIGINAL `enqueuedAt`/FIFO place survives — `releaseQueueEntry` never
 *      resets it), then return `{ granted: false, reason: 'capacity-denied',
 *      item }`.
 *   6. Any error either primitive throws (queue-lock timeout,
 *      coordination-file lock timeout, a `reserveAdmission` validation
 *      `TypeError`, a `QueueCorruptFileError`, …) propagates UNCAUGHT —
 *      never caught/swallowed here. In particular, an error thrown by
 *      `reserveAdmission` after `claimQueueEntry` already succeeded is NOT
 *      auto-cleaned-up: no automatic `releaseQueueEntry` runs on that path.
 *      The claimed item is left leased and recovers only via the queue
 *      lease's own TTL expiry, exactly like any other crashed consumer.
 *      Fail-closed-by-propagation is the deliberate contract — that
 *      fail-closed DECISION belongs to the caller (Phase 4's CLI-layer
 *      beat), not to this composed primitive.
 *
 * **Mandatory caller-side ordering (not enforced here — documentation-only
 * contract; see pre-PR review finding #2,):** `reserveAdmission`'s
 * admission hold (the `LIVE_AGENT_ADMISSION_CLAIM_TYPE` ledger this function
 * writes to on every call, granted or not) uses REPLACE-not-accumulate
 * semantics PER `orchestratorId` — a single orchestrator's own prior hold is
 * excluded from its own next `reserveAdmission` request every time (see
 * `coordination-file.mjs`'s `reserveAdmission`, "this orchestrator's own
 * prior hold is replaced, not accumulated"). That means an orchestrator that
 * calls this function (via `cli.mjs`'s `--dequeue-if-capacity`) more than
 * once WITHOUT first converting each prior grant into a real
 * `--claim=live-agent:N` will silently UNDER-REPORT its own true outstanding
 * admission — each new call resets, rather than adds to, its own hold, so
 * the shared ceiling never actually sees the accumulating load. The
 * mandatory real-world sequencing per granted item is: `--dequeue-if-capacity`
 * (get `item`+`leaseId`) → spawn externally → `--claim=live-agent:1`
 * (registers the real running agent AND atomically clears this
 * orchestrator's admission hold) → … → `--release=live-agent:1` (when the
 * agent finishes) → `--ack-lease=<leaseId>` (permanently removes the queue
 * entry). No code in this function or in `cli.mjs` enforces this ordering —
 * a caller that violates it gets no error, just quietly wrong ceiling
 * accounting. See `cli-dispatcher-drain.jest.spec.mjs`'s header comment for
 * the outer proof this ordering is load-bearing — that is the durable
 * record of this gap.
 *
 * @param {string} coordinationFilePath
 * @param {string} queueFilePath
 * @param {{
 *   orchestratorId: string,
 *   snapshotType: string,
 *   admissionType: string,
 *   ceiling: number,
 *   now?: number,
 *   pid?: number,
 *   pidStartedAt?: number,
 * }} params `now`, when provided, is the ONE shared wall-clock instant used
 *   for both `claimQueueEntry`'s lease stamp and `reserveAdmission`'s own
 *   `now` — falls back to each primitive's own `Date.now()` default when
 *   omitted. `pid`/`pidStartedAt` are OPTIONAL and purely ADDITIVE:
 *   they arrive ALREADY RESOLVED from the caller and are forwarded verbatim
 *   into the `reserveAdmission` call below, never derived here. STANDING RULE:
 *   this function must not consult `process.pid`/`process.uptime()` — the
 *   identity these fields carry is the beat's nearest claude-rooted ANCESTOR,
 *   not this process's own, because only claude-rooted pids are visible to the
 *   `listAgentProcesses` snapshot the read-side classifier correlates against.
 *   (Resolving that ancestor is the caller's job, via
 *   `resolveNearestClaudeRootIdentity` in `lib/recon.mjs`.) Omitted means
 *   omitted: the admission record simply
 *   carries no `pid`, and is governed by TTL alone. This is the
 *   dispatcher-drain admission path, reached from `cli.mjs`'s
 *   `--dequeue-if-capacity` — a live production write site, so it accepts the
 *   pair alongside the three `coordination-file.mjs` ledger functions
 *   (`declareDibs`, `claimCapacity`, `reserveAdmission`) rather than being
 *   silently exempt. `cli.mjs` reaches those four functions from five call
 *   sites in total; this is the only one that routes through this module.
 * @param {{
 *   queue?: { claimTtlMs?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number },
 *   admission?: { claimTtlMs?: number, snapshotClaimTtlMs?: number, lockStalenessMs?: number, lockMaxAttempts?: number, lockRetryDelayMs?: number },
 * }} [options] `options.queue` is threaded straight through to
 *   `claimQueueEntry`/`releaseQueueEntry` — the QUEUE lease's own TTL,
 *   semantically distinct from the admission ledger's freshness window below,
 *   so kept in its own sub-bag rather than one flat `claimTtlMs` that would
 *   collide the two meanings. `options.admission` is threaded through to
 *   `reserveAdmission` with one default filled in: `snapshotClaimTtlMs`
 *   defaults to `DEFAULT_CLAIM_TTL_MS` when the caller omits it (Phase
 *   2). `params.snapshotType` here is ENFORCED (thrown `TypeError` if
 *   violated — review follow-up,) to be exactly `LIVE_AGENT_CLAIM_TYPE`
 *   — a real live agent's presence, held for hours — never the short-lived
 *   `admissionType` ledger, so defaulting its prune window to the real
 *   multi-hour claim TTL is correct for every caller of this function, not
 *   just `cli.mjs`'s own (now-fixed) call site. Without this default (and
 *   without the invariant assertion), a caller that forgets to pass
 *   `snapshotClaimTtlMs`, or that passes some OTHER `snapshotType`, would
 *   silently get the wrong prune window applied instead of a loud failure —
 *   the exact over-admission bug this ticket exists to close. An explicit
 *   `options.admission.snapshotClaimTtlMs` still overrides this default.
 * @returns {Promise<
 *   | { granted: true, item: object, leaseId: string }
 *   | { granted: false, reason: 'capacity-denied', item: object }
 *   | { granted: false, reason: 'queue-empty' }
 * >}
 */
export async function claimAndAdmitQueueEntry(coordinationFilePath, queueFilePath, params, options = {}) {
  const { orchestratorId, snapshotType, admissionType, ceiling, now, pid, pidStartedAt } = params;
  const queueOptions = options.queue ?? {};
  // Phase 2 (review follow-up) — `snapshotClaimTtlMs` defaults to
  // `DEFAULT_CLAIM_TTL_MS` (not `options.claimTtlMs`) because `snapshotType`
  // here is always the real, hours-lived `LIVE_AGENT_CLAIM_TYPE` ledger — see
  // this function's own doc comment above. That's an assumption about every
  // CALLER's `params.snapshotType`, not something the JSDoc type (a bare
  // `string`) enforces — so assert it loudly, matching this codebase's own
  // idiom of throwing on invariant violation (see `reserveAdmission` in
  // `coordination-file.mjs`), rather than silently defaulting the wrong TTL
  // in for some hypothetical future `snapshotType`. An explicit
  // `snapshotClaimTtlMs` from the caller still wins over the default.
  if (snapshotType !== LIVE_AGENT_CLAIM_TYPE) {
    throw new TypeError(
      `claimAndAdmitQueueEntry: snapshotType must be LIVE_AGENT_CLAIM_TYPE ("${LIVE_AGENT_CLAIM_TYPE}"), ` +
        `received "${snapshotType}" — this function's default snapshotClaimTtlMs ` +
        '(DEFAULT_CLAIM_TTL_MS) is only correct for the real, hours-lived live-agent ledger; a ' +
        'different snapshotType needs its own considered TTL default, not this one silently applied',
    );
  }
  const callerAdmission = options.admission ?? {};
  const admissionOptions = {
    ...callerAdmission,
    snapshotClaimTtlMs: callerAdmission.snapshotClaimTtlMs ?? DEFAULT_CLAIM_TTL_MS,
  };

  const claimed = await claimQueueEntry(queueFilePath, { now, ...queueOptions });
  if (claimed === null) {
    return { granted: false, reason: 'queue-empty' };
  }

  const { granted } = await reserveAdmission(
    coordinationFilePath,
    {
      snapshotType,
      admissionType,
      requestedCount: 1,
      orchestratorId,
      ceiling,
      now,
      // — forwarded UNCHANGED, never derived or defaulted here.
      // `undefined` is safe to pass explicitly: `reserveAdmission`'s
      // `withPidIdentity` omits the own-property entirely rather than writing
      // `pid: undefined`, so absent stays absent on disk.
      pid,
      pidStartedAt,
    },
    admissionOptions,
  );

  if (granted >= 1) {
    return { granted: true, item: claimed, leaseId: claimed.leaseId };
  }

  // `releaseQueueEntry` returns `null` only in the rare race where this
  // exact `leaseId` no longer matches anything on disk — the queue lease
  // TTL expired and a DIFFERENT `claimQueueEntry` call already reclaimed
  // (re-leased) the same entry between our own claim above and this
  // release call. That would leave `item` `null` here, contradicting this
  // function's own documented `{ granted: false, reason: 'capacity-denied',
  // item: object }` return shape (pre-PR review finding #6,). Falling
  // back to the originally-claimed entry (`claimed`) is simpler and more
  // honest than widening the return type to `item: object | null` and
  // pushing a null-check onto every caller for a race this rare: `claimed`
  // is still the correct entry identity/content (`agentClass`, `priority`,
  // etc.) the caller was just denied capacity for — only its `leaseId` is
  // now stale (superseded by whoever reclaimed it), which callers already
  // must not act on directly (denial means "don't spawn"), so returning it
  // costs nothing.
  const released = await releaseQueueEntry(queueFilePath, claimed.leaseId, queueOptions);
  return { granted: false, reason: 'capacity-denied', item: released ?? claimed };
}
