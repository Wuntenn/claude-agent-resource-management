// Phase 4 — priority/allowance algorithm, spawn-rate limiter &
// traffic-light output.
//
// Composes Phase 2's per-axis classification (`{ state, reason, history }`)
// and Phase 3's dibs ledger (`declareDibs`/`readDibs`) into the final
// decision the orchestrator acts on each beat: how many agents may spawn,
// which axis is the bottleneck, which running agent to pause on RED, and a
// discriminated traffic-light signal to hand back to the caller.
//
// Pure, deterministic functions throughout — no I/O, no `Date.now()`,
// `Math.random()`, or unstable sort. Every function is a plain mapping from
// its explicit arguments to its return value.

import { isReservedEntry } from './coordination-file.mjs';

/**
 * @typedef {'GREEN'|'AMBER'|'RED'} AxisState
 */

/**
 * @param {unknown} value
 * @returns {value is number}
 */
function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

/**
 * Maps a single axis's classified state to the number of agents that axis
 * alone would allow, given its configured cap. GREEN grants the cap in full;
 * AMBER and RED both zero out the axis (no new spawns once an axis is
 * non-GREEN) — this is what makes AMBER alone already produce a `hold`
 * traffic light, not only RED.
 *
 * @param {AxisState} state
 * @param {number} cap
 * @returns {number}
 */
function axisAllowance(state, cap) {
  return state === 'GREEN' ? cap : 0;
}

/**
 * Phase 5 (Part A) — derives a numeric agent-count cap for the
 * memory axis from actual live headroom, rather than the flat
 * `DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis` constant. Pure arithmetic,
 * no I/O: `Math.max(0, Math.floor((freeRamMb - osBaselineMb - reserveMb) /
 * perAgentRamMb))`.
 *
 * Defensive clamps (never fabricate a non-finite or negative cap):
 *   - `perAgentRamMb` <= 0, non-finite, or not a number -> degenerate config
 *     -> `0` (never divide by zero/negative, never `Infinity`).
 *   - `freeRamMb` non-finite (NaN/Infinity/missing) -> no live headroom data
 *     -> clamp to `0` before the subtraction ("no data" must never imply
 *     "unlimited headroom").
 *   - `osBaselineMb`/`reserveMb` are NOT independently clamped — both are
 *     expected to be finite host-capacity constants supplied by the caller,
 *     not live-sampled values.
 *
 * @param {{ freeRamMb: number, osBaselineMb: number, reserveMb: number, perAgentRamMb: number }} args
 * @returns {number}
 */
export function computeHeadroomCap({ freeRamMb, osBaselineMb, reserveMb, perAgentRamMb }) {
  if (!isFiniteNumber(perAgentRamMb) || perAgentRamMb <= 0) return 0;

  const safeFreeRamMb = isFiniteNumber(freeRamMb) ? freeRamMb : 0;

  return Math.max(0, Math.floor((safeFreeRamMb - osBaselineMb - reserveMb) / perAgentRamMb));
}

/**
 * Computes the effective spawn allowance as `min(memory-axis, disk-axis)`,
 * names whichever axis was more restrictive (`'memory'` | `'disk'` | `'both'`
 * on a numeric tie — including the everything-GREEN case), and attaches a
 * deterministic dibs `priorityOrder`.
 *
 * Dibs priority ordering (a Build Plan-left-open design choice, documented
 * here): ascending `firstDeclaredAt` (earliest-ARRIVING orchestrator wins
 * priority), tied entries broken by ascending `orchestratorId` for full
 * determinism. This always reflects whatever `dibs` array is passed in for
 * THIS call — there is no memoization keyed on axis state, so a caller
 * re-reading a fresh dibs ledger between two calls with identical
 * (cached/reused) axis-state inputs always gets a fresh `priorityOrder`.
 *
 * Deliberately NOT `declaredAt`: `declaredAt` is refreshed on every single
 * `declareDibs` beat (needed for liveness pruning — see
 * `./coordination-file.mjs`), so it always reflects "when was this entry
 * last refreshed", not "when did this orchestrator first arrive". Sorting by
 * `declaredAt` would make whichever orchestrator's beat is CURRENTLY running
 * always look newest (lowest priority) relative to a contender that simply
 * hasn't beaten as recently — under sustained contention this inverted the
 * intended semantic into a permanent 0/0 deadlock for both orchestrators
 * (reproduced and fixed — see this file's regression test). `firstDeclaredAt`
 * is set once, on an orchestrator's first-ever declaration, and preserved
 * unchanged by `declareDibs` on every subsequent upsert for that
 * `orchestratorId` — a stable arrival timestamp `declaredAt` can't provide.
 * A dibs entry without a `firstDeclaredAt` (e.g. a hand-built test fixture)
 * falls back to its own `declaredAt` for ordering purposes only.
 *
 * @param {AxisState} memoryState
 * @param {AxisState} diskState
 * @param {Array<{ orchestratorId: string, desiredAgents: number, declaredAt: number, firstDeclaredAt?: number }>} dibs
 * @param {{ maxAgentsMemoryAxis: number, maxAgentsDiskAxis: number, memoryHeadroomCap?: number }} config
 *   `memoryHeadroomCap` (Phase 5,  Part A) is OPTIONAL. When
 *   present, it REPLACES `maxAgentsMemoryAxis` as the memory axis's GREEN
 *   cap (typically the output of `computeHeadroomCap` above, fed through by
 *   the caller) — it does not sit alongside the flat constant unused. When
 *   absent, the memory axis cap is `maxAgentsMemoryAxis` exactly as before
 *   this phase — byte-identical, unchanged behavior. The disk axis is
 *   untouched by this phase.
 * @returns {{ amount: number, constrainedBy: 'memory'|'disk'|'both', priorityOrder: string[] }}
 */
export function computeAllowance(memoryState, diskState, dibs, config) {
  const memoryCap = isFiniteNumber(config.memoryHeadroomCap)
    ? config.memoryHeadroomCap
    : config.maxAgentsMemoryAxis;
  const memoryAmount = axisAllowance(memoryState, memoryCap);
  const diskAmount = axisAllowance(diskState, config.maxAgentsDiskAxis);

  const amount = Math.min(memoryAmount, diskAmount);
  const constrainedBy =
    memoryAmount === diskAmount ? 'both' : memoryAmount < diskAmount ? 'memory' : 'disk';

  const priorityKey = (entry) => (isFiniteNumber(entry.firstDeclaredAt) ? entry.firstDeclaredAt : entry.declaredAt);

  // Reserved pseudo-entries (`isReservedEntry`) are excluded, exactly as
  // `computePerOrchestratorAllowance` below already excludes them: they name
  // no orchestrator, so they can never legitimately hold a priority slot, and
  // including one both shifts every real orchestrator's rank and lets a
  // phantom id escape into a caller's ordering. Latent since the first
  // reserved row, made unmissable by its `__ever-opted-in__` row, which
  // `declareDibs` now maintains on every single call.
  const priorityOrder = dibs
    .filter((entry) => !isReservedEntry(entry.orchestratorId))
    .sort((a, b) => priorityKey(a) - priorityKey(b) || a.orchestratorId.localeCompare(b.orchestratorId))
    .map((entry) => entry.orchestratorId);

  return { amount, constrainedBy, priorityOrder };
}

/**
 * Computes ONE orchestrator's bounded share of the flat per-axis cap
 * `computeAllowance` would otherwise hand out unconditionally to every
 * caller (Phase 5 — "dibs constrain allowance"). A thin, pure
 * extension built on top of `computeAllowance`, not a replacement for it:
 * `computeAllowance` itself is unchanged and still used elsewhere (e.g. for
 * `priorityOrder`/`constrainedBy` reporting).
 *
 * Pinned algorithm (see `./allowance.jest.spec.mjs`'s header comment on this
 * function for the full worked rationale):
 *
 *   1. Compute the flat per-axis cap exactly as `computeAllowance` does
 *      today (`min(memory-axis, disk-axis)`), over dibs with reserved
 *      pseudo-entries (`isReservedEntry`) already excluded.
 *   2. Reserved pseudo-entries never count as a contending orchestrator and
 *      never consume any of the cap; requesting a reserved id's own share
 *      always returns `0` (it never has a "real" dibs entry of its own).
 *   3. UNCONTENDED — at most one real orchestrator entry exists: that lone
 *      orchestrator receives the FULL cap as headroom, unclamped by its own
 *      `desiredAgents` (today's existing behavior; must not regress). If the
 *      requester is NOT that lone orchestrator (including "no live dibs
 *      entry at all"), it receives `0`.
 *   4. CONTENDED — two or more real orchestrators have live dibs: walk
 *      `priorityOrder` (ascending `firstDeclaredAt`, ties broken by ascending
 *      `orchestratorId`, exactly as `computeAllowance` already computes it).
 *      Each orchestrator, strictly in that order, claims
 *      `min(its own desiredAgents, whatever axis-cap remains)`; that claim
 *      is subtracted from the remaining pool before moving to the next
 *      orchestrator. Returns the claim computed for
 *      `requestingOrchestratorId` specifically — `0` if it never appears in
 *      the live dibs at all.
 *   5. An orchestrator declaring `desiredAgents: 0` claims `0` and leaves the
 *      remaining pool untouched for whoever comes after it in priority
 *      order.
 *
 * @param {AxisState} memoryState
 * @param {AxisState} diskState
 * @param {Array<{ orchestratorId: string, desiredAgents: number, declaredAt: number, firstDeclaredAt?: number }>} dibs
 * @param {{ maxAgentsMemoryAxis: number, maxAgentsDiskAxis: number }} config
 * @param {string} requestingOrchestratorId
 * @returns {number}
 */
export function computePerOrchestratorAllowance(
  memoryState,
  diskState,
  dibs,
  config,
  requestingOrchestratorId,
) {
  const realDibs = dibs.filter((entry) => !isReservedEntry(entry.orchestratorId));
  const { amount: flatCap, priorityOrder } = computeAllowance(
    memoryState,
    diskState,
    realDibs,
    config,
  );

  // Branch selection (solo vs. contended) must only count entries with real
  // spawn intent (`desiredAgents > 0`) as competitors. A `--heartbeat` beat's
  // own dibs entry preserves this orchestrator's last REAL declared
  // `desiredAgents` (`0` only when there is no prior real declaration at all)
  // specifically so that polling never ADDS contention beyond what's already
  // real (see this skill's SKILL.md) — a co-resident zero-`desiredAgents`
  // entry must never by itself flip an otherwise-solo orchestrator into the
  // contended/clamped branch below. It still participates correctly in the
  // contended-branch walk (claiming `0` and leaving `remaining` untouched)
  // once there genuinely are two or more real competitors.
  const competitors = realDibs.filter((entry) => entry.desiredAgents > 0);

  if (competitors.length <= 1) {
    const [solo] = competitors;
    return solo && solo.orchestratorId === requestingOrchestratorId ? flatCap : 0;
  }

  const dibsById = new Map(realDibs.map((entry) => [entry.orchestratorId, entry]));
  let remaining = flatCap;
  let requestedShare = 0;

  for (const orchestratorId of priorityOrder) {
    const entry = dibsById.get(orchestratorId);
    if (!entry) continue;
    const claim = Math.min(entry.desiredAgents, remaining);
    remaining -= claim;
    if (orchestratorId === requestingOrchestratorId) {
      requestedShare = claim;
    }
  }

  return requestedShare;
}

/**
 * Selects the running agent to pause when an axis has gone RED. Pure and
 * deterministic over the input array: never mutates or relies on input
 * order, never uses `Math.random`, so two independent callers observing the
 * same logical running-agent population (even as separately-built/shuffled
 * array instances) always converge on the identical candidate.
 *
 * Ranking is auto-detecting, NOT policy-selected — the `policy` parameter is
 * accepted purely as caller-facing documentation of which ranking the caller
 * EXPECTS to apply given the data it is passing in, and has NO effect on
 * which candidate is actually returned (review, Low #2 — an earlier
 * revision of this JSDoc implied `policy` chose between two distinct ranking
 * strategies at runtime; it never has, and the one real call site,
 * `./pause-candidate.mjs`'s `selectPauseCandidateWithFootprint`, works today
 * only because this parameter is ignored).
 *
 * If at least one candidate carries a valid `physFootprintMb` — a `number`,
 * already sampled by the caller via `sampleFootprint` (: this function
 * stays pure/synchronous/no-I/O and never samples anything itself, since
 * under memory compression `rssMb` and true physical footprint diverge
 * and `rssMb` alone can pick the wrong victim) — only the candidates
 * with valid `physFootprintMb` compete, ranked by that field descending.
 * Ties are broken deterministically (documented rule, Build Plan left this
 * open): earliest `startedAt` wins (the longer-running agent is judged more
 * disruptive to keep waiting on), then ascending `agentId` as a final
 * tie-break. A candidate with `null`/absent `physFootprintMb` is excluded
 * from that ranking rather than being treated as `0` or silently substituted
 * with `rssMb`. If NO candidate has valid footprint data, this degrades to
 * ranking by `rssMb` descending over all candidates, same tie-break rule
 * (never `null` when candidates exist, never an arbitrary/first-in-array
 * pick).
 *
 * @param {Array<{ agentId: string, rssMb: number, startedAt: number, physFootprintMb?: number | null }>} runningAgents
 * @param {{ policy: 'highest-rss' | 'highest-phys-footprint' }} policy - caller-documentation only; does not change ranking behavior (see above)
 * @returns {{ agentId: string, rssMb: number, startedAt: number, physFootprintMb?: number | null } | null}
 */
export function selectPauseCandidate(runningAgents, policy) {
  // Intentionally unused: ranking auto-detects via `physFootprintMb`
  // presence (see JSDoc above) rather than branching on `policy`. Kept as a
  // parameter, not dropped, since a caller passing the literal that matches
  // its own expectation is useful self-documentation at the call site even
  // though this function itself ignores the value.
  void policy;
  if (runningAgents.length === 0) return null;

  const withFootprint = runningAgents.filter((agent) => isFiniteNumber(agent.physFootprintMb));

  if (withFootprint.length > 0) {
    const [best] = [...withFootprint].sort((a, b) => {
      if (b.physFootprintMb !== a.physFootprintMb) return b.physFootprintMb - a.physFootprintMb;
      if (a.startedAt !== b.startedAt) return a.startedAt - b.startedAt;
      return a.agentId.localeCompare(b.agentId);
    });
    return best;
  }

  const [best] = [...runningAgents].sort((a, b) => {
    if (b.rssMb !== a.rssMb) return b.rssMb - a.rssMb;
    if (a.startedAt !== b.startedAt) return a.startedAt - b.startedAt;
    return a.agentId.localeCompare(b.agentId);
  });

  return best;
}

/**
 * Token-bucket spawn-rate limiter, expressed as a pure function of an
 * explicit, immutable bucket state plus an injected `now` — never a real
 * `setTimeout`/`Date.now()`. Refill is computed purely from elapsed
 * fake-clock time since the bucket's `lastRefillAt`.
 *
 * Deny-the-whole-burst semantics: if the bucket cannot cover the full
 * `requestedCount`, nothing is granted (no partial batch), so a caller never
 * spawns an ambiguous partial burst.
 *
 * This function is deliberately unaware of any OTHER bucket — two
 * independent per-orchestrator buckets are not coordinated by it, so their
 * combined throughput can exceed an intended *global* spawn-rate cap unless
 * bucket state itself is threaded through a shared store (e.g. the Phase 3
 * coordination file). That is an intentional, documented limitation, not an
 * oversight — see this function's test suite.
 *
 * @param {{ bucketState: { tokens: number, lastRefillAt: number } | null, now: number, requestedCount: number, config: { capacityTokens: number, refillTokensPerMs: number } }} args
 * @returns {{ allowed: boolean, grantedCount: number, bucketState: { tokens: number, lastRefillAt: number }, scope: 'per-orchestrator' }}
 */
// This pure function stays unaware of any OTHER bucket by design (see its doc
// comment above) — global threading is done, but at the caller layer, not
// here: `./coordination-file.mjs`'s `consumeGlobalSpawnTokens`
// wraps this function in a lock-guarded read-modify-write against a shared
// coordination-file entry, giving orchestrators a genuinely global cap
// without this function itself needing to know about the coordination file.
export function consumeSpawnTokens({ bucketState, now, requestedCount, config }) {
  const previous = bucketState ?? { tokens: config.capacityTokens, lastRefillAt: now };

  const elapsedMs = Math.max(0, now - previous.lastRefillAt);
  const refilled = Math.min(
    config.capacityTokens,
    previous.tokens + elapsedMs * config.refillTokensPerMs,
  );

  // `scope: 'per-orchestrator'` is not decorative: it is the caller-visible
  // signal (readable from the return shape, not just this doc comment) that
  // this cap is local to whatever bucketState was threaded in — see the
  // limitation documented above and the Phase 5 TODO.
  if (refilled >= requestedCount) {
    return {
      allowed: true,
      grantedCount: requestedCount,
      bucketState: { tokens: refilled - requestedCount, lastRefillAt: now },
      scope: 'per-orchestrator',
    };
  }

  return {
    allowed: false,
    grantedCount: 0,
    bucketState: { tokens: refilled, lastRefillAt: now },
    scope: 'per-orchestrator',
  };
}

/**
 * Builds the discriminated traffic-light signal a caller acts on each beat —
 * never a bare boolean. Exactly one of three shapes:
 *
 *   - `{ type: 'spawn-allowed', allowance }` — both axes GREEN.
 *   - `{ type: 'hold' }` — either axis AMBER (neither RED): freeze new
 *     spawns without disturbing already-running agents.
 *   - `{ type: 'pause', pauseCandidate }` — either axis RED: names the
 *     specific agent to pause, or `null` if there are zero running agents to
 *     pause (a well-defined no-op, never a throw or a fabricated candidate).
 *
 * `allowance` here is `computeAllowance`'s numeric `.amount`, flattened onto
 * this traffic light's own `allowance` field — callers of `buildTrafficLight`
 * read a bare number, not the `{ amount, constrainedBy, priorityOrder }`
 * object `computeAllowance` returns.
 *
 * `pause`/`pauseCandidate` is recommendation-only: this function only *names*
 * a candidate for the calling orchestrator's own judgement to act on (or
 * not) — it does not itself perform any real process-control operation, and
 * it does not itself relieve any memory pressure. Nothing in this module, or
 * anywhere else under `scripts/agent-resource-management/**`, sends the
 * named process a Unix control signal, suspends/resumes it, or checkpoints
 * it — `selectPauseCandidate` (also in this file) only selects which running
 * agent *would* be paused; no code here ever signals or otherwise touches
 * the named process. Treat `pauseCandidate` as advisory data for the
 * orchestrator to decide what (if anything) to do with, never as proof that
 * a process was actually paused or that memory was reclaimed.
 *
 * Forward-contract extension: the `@returns` union below
 * additionally lists `'throttle'`, `'cleanup'`, and `'alert'` so that an
 * exhaustive-union consumer can type-check against the eventual full
 * contract. As of Phase 4, `'alert'` is no longer purely a
 * forward-contract placeholder: `diskState === 'RED'` now genuinely produces
 * it (see the disk-RED bullet below and the dedicated branch in this
 * function's body). `'throttle'` and `'cleanup'` are still never RETURNED by
 * this function — there is no input combination of `memoryState`/`diskState`
 * (or any other argument) that causes it to produce either literal — but they
 * are unreachable for DIFFERENT REASONS, and the difference matters:
 * `'throttle'` has no mechanism behind it at all, whereas `'cleanup'` now has
 * a real one that deliberately reports somewhere else. Each is described
 * honestly below rather than fabricating a trigger condition this function
 * can't back:
 *
 *   - `'throttle'` — documented-only: no CPU/IO throttle mechanism of any
 *     kind exists anywhere in this codebase today. There is no
 *     process-priority or I/O-bandwidth control wired up under
 *     `scripts/agent-resource-management/**`, so this function cannot
 *     honestly decide when to signal it.
 *   - `'cleanup'` — UNREACHABLE HERE, BUT NO LONGER FOR WANT OF A MECHANISM
 *     (updated by Phase 5; this bullet previously read "no cleanup
 *     mechanism exists today", which stopped being true and is exactly the
 *     kind of drift this doc block exists to prevent).
 *
 *     A real, budgeted, recursively-deleting temp sweep now ships in
 *     `lib/cleanup.mjs` (`sweepStaleTempDirsAsync`, +), and
 *     `cli.mjs` genuinely fires it from the disk axis on an armed
 *     orchestrator's beat. The dominant growth contributor it targets —
 *     stale `cdk.out*`/`bundling-temp-*` staging output — was measured and
 *     settled by, so the open question this bullet used to cite is
 *     closed too.
 *
 *     What has NOT changed is this function's own union. The sweep follows
 *     the EVICT arrangement described below: it is produced by `cli.mjs`, not
 *     by `buildTrafficLight`, and it reports through an additive top-level
 *     `cleanup` FIELD on the verdict JSON — a sibling of `diskTrend` and
 *     `liveAgentGrant` — rather than through a traffic-light `type`. That is
 *     a deliberate shape, not an oversight: a `{ type: 'cleanup' }` verdict
 *     would REPLACE the spawn-allowed/hold/pause answer the caller asked for,
 *     turning "I reclaimed 4 GB, and you may still spawn" into "cleanup",
 *     which reads to an orchestrator as "retry later". A sibling field lets
 *     one beat say both things at once, which is what actually happened.
 *
 *     So the literal stays in the union as a forward-contract shape, and this
 *     function still has nothing truthful to trigger on — because the truth
 *     is reported elsewhere, by design.
 *   - `'alert'` — passive-only: no active notification channel exists in
 *     this skill — nothing here dispatches an outbound message to a
 *     human or another system on any channel. The traffic light itself is
 *     the alert: a human tailing logs/output sees the signal directly. As
 *     of Phase 4, `diskState === 'RED'` (regardless of
 *     `memoryState`, including when `memoryState` is also `'RED'` — disk-RED
 *     wins that tie) produces an `alert`, evaluated BEFORE the `pause`
 *     branch below. As of Phase 1, that `alert` shape carries a
 *     `memoryAlsoRed: boolean` flag so a both-RED beat is distinguishable
 *     from a disk-RED-only one: when `memoryState` is also `'RED'` it
 *     returns `{ type: 'alert', reason: diskReason, memoryAlsoRed: true,
 *     pauseCandidate }` (the `pauseCandidate` the caller passed in, echoed
 *     through so downstream consumers can see — and act on — the memory
 *     pressure the disk-RED-wins precedence would otherwise hide); when
 *     `memoryState` is not `'RED'` it returns `{ type: 'alert', reason:
 *     diskReason, memoryAlsoRed: false }` with no `pauseCandidate` key at
 *     all. The mechanism stays passive even though the literal is now
 *     reachable: this function still only returns a value on stdout for the
 *     caller/a human to read — it never itself dispatches anything.
 *
 * Do not read the presence of `'throttle'`/`'cleanup'` in the type union as
 * a promise that calling this function differently, or waiting long enough,
 * will produce them — they are a forward contract shape only. For
 * `'throttle'` that is gated on future work (see the Build Plan for)
 * that has not landed; for `'cleanup'` the work HAS landed and reports
 * through a sibling field instead, so waiting for this function to emit the
 * literal would be waiting for something that is not coming.
 *
 * EVICT — real and shipped, but as a standalone beat, never a
 * literal in THIS function's own `@returns` union. Phase 3 of
 * documented this literal as considered and, at the time, left out of the
 * contract, because no real checkpoint-and-exit (or equivalent
 * memory-reclaiming) capability existed anywhere in this codebase.
 * built that capability for real: `isEvictionAuthorized` (own-spawned-only
 * authorization gate, ./recon.mjs), `gracefulStop` (SIGTERM -> a
 * configurable grace period, default 60s -> SIGKILL if the target is
 * still alive, ./graceful-stop.mjs), and `handleEvict` (cli.mjs) which
 * composes both of those plus the durable-queue lease primitives into a
 * real, opt-in eviction beat, triggered only by the explicit flag
 * combination `--evict-pid=<pid> --lease-id=<leaseId>
 * --orchestrator-pid=<pid>`.
 *
 * This function's own `@returns` union is UNCHANGED by — still
 * exactly the same six literals as before this ticket; no `'evict'`
 * literal was added here. `handleEvict` builds and prints its own
 * evict-typed JSON directly in cli.mjs, entirely without calling this
 * function. That is a deliberate, reviewed design decision (see
 * cli.mjs's `handleEvict` and cli-evict.jest.spec.mjs's header for the
 * full rationale), not an oversight:
 *   1. Keeping the evict outcome off this function's own reachable
 *      surface keeps "a bare RED beat can never evict" true by
 *      construction — this is exactly the function every default (no
 *      extra flags) beat calls, so if it can never signal eviction,
 *      nothing reachable through it can trigger one automatically.
 *   2. This function's signature (`memoryState`, `diskState`,
 *      `diskReason`, `allowance`, `pauseCandidate`) has no room for the
 *      per-invocation targeting data (`evictPid`/`leaseId`/
 *      `orchestratorPid`) an eviction decision needs, without adding
 *      parameters meaningless to every other call site of this pure,
 *      axis-classification-only function.
 *
 * Do not overstate what EVICT does: it stops a targeted, own-spawned
 * process (SIGTERM, escalating to SIGKILL past the grace period) and lets
 * the OS reclaim that process's memory as a byproduct — it does not
 * itself "reclaim memory" the way a checkpoint/restore mechanism would.
 * See its own "superseded premise" framing for the exact distinction.
 *
 * @param {{ memoryState: AxisState, diskState: AxisState, diskReason?: string, allowance: number | { amount: number }, pauseCandidate: object | null }} args
 * @returns {{ type: 'spawn-allowed', allowance: number } | { type: 'hold' } | { type: 'pause', pauseCandidate: object | null } | { type: 'throttle' } | { type: 'cleanup' } | { type: 'alert', reason: string | undefined, memoryAlsoRed: boolean, pauseCandidate?: object | null }} Today,
 *   `'spawn-allowed'` | `'hold'` | `'pause'` | `'alert'` (disk-RED only) are
 *   actually produced by this function; `'throttle'` | `'cleanup'` are
 *   documented forward-contract literals this function never returns —
 *   `'throttle'` for want of any mechanism, `'cleanup'` because its shipped
 *   mechanism reports through the verdict's additive `cleanup` sibling field
 *   instead. See the bullets above.
 *   The `'alert'` variant always carries `memoryAlsoRed`, and additionally
 *   carries `pauseCandidate` only when `memoryAlsoRed` is `true` (both axes
 *   RED); when `memoryAlsoRed` is `false` (disk-RED only) there is no
 *   `pauseCandidate` key on the returned object at all.
 */
export function buildTrafficLight({ memoryState, diskState, diskReason, allowance, pauseCandidate }) {
  const flatAllowance = typeof allowance === 'object' && allowance !== null ? allowance.amount : allowance;

  // Phase 4: disk-RED gets its own distinct `alert` signal, checked
  // BEFORE the memory-RED/disk-RED -> pause branch below, so disk-RED wins
  // the tie when both axes are simultaneously RED. See this function's doc
  // comment above for the full rationale.
  if (diskState === 'RED') {
    // when memory is ALSO RED on this beat, enrich the alert with
    // `memoryAlsoRed: true` plus the `pauseCandidate` already computed by the
    // caller, so downstream consumers can see (and act on) the memory
    // pressure that the disk-RED-wins precedence would otherwise hide.
    if (memoryState === 'RED') {
      return { type: 'alert', reason: diskReason, memoryAlsoRed: true, pauseCandidate: pauseCandidate ?? null };
    }
    return { type: 'alert', reason: diskReason, memoryAlsoRed: false };
  }

  // `diskState === 'RED'` is deliberately NOT part of this condition: the
  // `alert` branch above already returns for every disk-RED case, so this
  // branch is only ever reached with `diskState !== 'RED'` — spelling out
  // `memoryState === 'RED'` alone (rather than an unreachable
  // `|| diskState === 'RED'` disjunct) keeps that invariant visible here too.
  if (memoryState === 'RED') {
    return { type: 'pause', pauseCandidate: pauseCandidate ?? null };
  }

  if (memoryState === 'AMBER' || diskState === 'AMBER') {
    return { type: 'hold' };
  }

  return { type: 'spawn-allowed', allowance: flatAllowance };
}
