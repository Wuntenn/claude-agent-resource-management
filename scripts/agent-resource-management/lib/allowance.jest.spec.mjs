// Phase 4 — priority/allowance algorithm, spawn-rate limiter &
// traffic-light output.
//
// RED by construction: `./lib/allowance.mjs` does not exist yet. This file
// specifies the contract `typescript-implementer` must satisfy,
// consistent with:
//   - the Build Plan's Phase 4 objective/edge-cases/acceptance-criteria/test
//     strategy (issue comments),
//   - the Convergence Analysis's flagged cross-phase edge cases for this
//     phase (simultaneous-RED pause-candidate collision, fresh-dibs-despite-
//     cached-sample, global spawn-rate cap across independent buckets), and
//   - the OUTER test (`../agent-resource-management.jest.spec.mjs`), which is
//     the canonical, already-fixed contract for the three public exports
//     this file also tests: `computeAllowance`, `selectPauseCandidate`,
//     `buildTrafficLight`. Field names below (`type`, `allowance`,
//     `pauseCandidate`) are taken directly from the outer test's assertions
//     (`trafficLight.type`, `trafficLight.allowance`, `trafficLight.pauseCandidate`)
//     — not invented here.
//
// Where the Build Plan left a design choice open (dibs priority ordering
// rule, pause-candidate tie-break, token-bucket function shape), this file
// picks one, documents it in a comment at the point of use, and proves it's
// deterministic. These are proposals for the builder, not restrictions the
// outer test already enforces — flagged as such in the handoff note.
//
// Do not add a stub implementation to make this pass — Phase 4 turns it
// green for real.

import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

import { declareDibs, readDibs, isReservedEntry } from './coordination-file.mjs';
import {
  computeAllowance,
  selectPauseCandidate,
  buildTrafficLight,
  consumeSpawnTokens,
  computePerOrchestratorAllowance,
} from './allowance.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));

// Raw source text of this module, used only by the Phase 2 JSDoc
// completeness test below — a plain text/regex check against the doc
// comment, not a runtime-behavior assertion (matches the existing
// `doc-honesty.jest.spec.mjs` idiom for this same file).
let allowanceSource;

beforeAll(async () => {
  allowanceSource = await readFile(join(__dirname, 'allowance.mjs'), 'utf8');
});

const CONFIG_SYMMETRIC = { maxAgentsMemoryAxis: 6, maxAgentsDiskAxis: 6 };

function makeDibs(orchestratorId, desiredAgents, declaredAt) {
  return { orchestratorId, desiredAgents, declaredAt };
}

// ---------------------------------------------------------------------------
// computeAllowance — effective = min(memory-axis, disk-axis), full 3x3 matrix
// ---------------------------------------------------------------------------

describe('computeAllowance — effective allowance is min(memory-axis, disk-axis)', () => {
  // Axis-allowance mapping used throughout this matrix: GREEN axis grants
  // its configured cap in full; AMBER/RED both zero out that axis's
  // allowance (no new spawns once an axis is non-GREEN) — this matches the
  // outer test's behavioural expectation that AMBER alone already produces
  // a 'hold' traffic light (no new spawns), not merely RED.
  const GREEN = 'GREEN';
  const AMBER = 'AMBER';
  const RED = 'RED';

  // 3x3 matrix: [memoryState, diskState, expectedAmount, expectedConstrainedBy]
  // constrainedBy names whichever axis produced the smaller (more
  // restrictive) numeric allowance; a tie — including the everything-GREEN
  // case and the AMBER/RED-both-zero cases — is reported as 'both', per the
  // Build Plan's explicit "(or 'both' if tied)" instruction. This is a
  // numeric-tie rule, not a severity ranking: AMBER+RED ties at 0 exactly
  // like AMBER+AMBER, since both axes equally contribute zero allowance.
  const matrix = [
    [GREEN, GREEN, 6, 'both'],
    [GREEN, AMBER, 0, 'disk'],
    [GREEN, RED, 0, 'disk'],
    [AMBER, GREEN, 0, 'memory'],
    [AMBER, AMBER, 0, 'both'],
    [AMBER, RED, 0, 'both'],
    [RED, GREEN, 0, 'memory'],
    [RED, AMBER, 0, 'both'],
    [RED, RED, 0, 'both'],
  ];

  it.each(matrix)(
    'memory=%s disk=%s -> amount=%i constrainedBy=%s',
    (memoryState, diskState, expectedAmount, expectedConstrainedBy) => {
      const result = computeAllowance(memoryState, diskState, [], CONFIG_SYMMETRIC);

      expect(result.amount).toBe(expectedAmount);
      expect(result.constrainedBy).toBe(expectedConstrainedBy);
      // The MORE restrictive axis always wins — never one axis silently
      // overriding the other's stricter verdict.
      expect(result.amount).toBeLessThanOrEqual(6);
    },
  );

  it('the more restrictive axis wins even when both axes are GREEN but have different configured caps', () => {
    // Both axes healthy, but disk's cap is lower — proves `min` genuinely
    // compares the two axis allowances, not just "GREEN wins outright".
    const result = computeAllowance(
      'GREEN',
      'GREEN',
      [],
      { maxAgentsMemoryAxis: 6, maxAgentsDiskAxis: 4 },
    );

    expect(result.amount).toBe(4);
    expect(result.constrainedBy).toBe('disk');
  });
});

// ---------------------------------------------------------------------------
// Dibs priority ordering — deterministic regardless of input array order
// ---------------------------------------------------------------------------

describe('computeAllowance — dibs priority ordering is deterministic', () => {
  // ASSUMPTION (Build Plan left this open: "first-declared-wins, or an
  // explicit priority field — pick one, document it"): this suite assumes
  // priority ordering is by ascending `declaredAt` (earliest declaration
  // wins priority), tie-broken by ascending `orchestratorId` for full
  // determinism when two entries declare in the same millisecond. The
  // returned field is assumed to be `priorityOrder: string[]` (an ordered
  // list of `orchestratorId`s) attached to `computeAllowance`'s result,
  // alongside `amount`/`constrainedBy`. If the builder picks a different
  // rule (e.g. an explicit `priority` field on each dibs entry), this test
  // must be updated to match — flagged in the handoff note.

  it('two orchestrators: earlier-declaredAt wins priority regardless of input array order', () => {
    const early = makeDibs('orchestrator-early', 2, 1_000);
    const late = makeDibs('orchestrator-late', 3, 2_000);

    const forward = computeAllowance('GREEN', 'GREEN', [early, late], CONFIG_SYMMETRIC);
    const reversed = computeAllowance('GREEN', 'GREEN', [late, early], CONFIG_SYMMETRIC);

    expect(forward.priorityOrder).toEqual(['orchestrator-early', 'orchestrator-late']);
    expect(reversed.priorityOrder).toEqual(forward.priorityOrder);
  });

  it('three orchestrators: shuffled input orders all converge on the identical priority order', () => {
    const a = makeDibs('orc-a', 1, 500);
    const b = makeDibs('orc-b', 1, 1_500);
    const c = makeDibs('orc-c', 1, 2_500);

    const permutations = [
      [a, b, c],
      [c, b, a],
      [b, c, a],
      [c, a, b],
    ];

    const results = permutations.map(
      (dibs) => computeAllowance('GREEN', 'GREEN', dibs, CONFIG_SYMMETRIC).priorityOrder,
    );

    const expected = ['orc-a', 'orc-b', 'orc-c'];
    for (const priorityOrder of results) {
      expect(priorityOrder).toEqual(expected);
    }
  });

  it('ties on declaredAt are broken deterministically by ascending orchestratorId', () => {
    const sameInstant = 5_000;
    const x = makeDibs('orc-zzz', 1, sameInstant);
    const y = makeDibs('orc-aaa', 1, sameInstant);

    const forward = computeAllowance('GREEN', 'GREEN', [x, y], CONFIG_SYMMETRIC);
    const reversed = computeAllowance('GREEN', 'GREEN', [y, x], CONFIG_SYMMETRIC);

    expect(forward.priorityOrder).toEqual(['orc-aaa', 'orc-zzz']);
    expect(reversed.priorityOrder).toEqual(forward.priorityOrder);
  });
});

// ---------------------------------------------------------------------------
// Fresh-dibs-even-with-cached-sample — dibs and machine-sample freshness are
// independent concerns; a reused sample must never imply reused dibs.
// ---------------------------------------------------------------------------

describe('computeAllowance always reads the CURRENT dibs, independent of machine-sample cache reuse', () => {
  it('two calls with identical axis states (as if reusing a cached sample) but different dibs produce different priorityOrder/amount — no memoization keyed on axis state alone', () => {
    const firstDibs = [makeDibs('orchestrator-a', 2, 1_000)];
    const secondDibs = [
      makeDibs('orchestrator-a', 2, 1_000),
      makeDibs('orchestrator-b', 5, 500),
    ];

    // Same memory/disk states both times — simulating a machine sample that
    // is still within its freshness window and therefore reused verbatim —
    // but dibs genuinely changed (a second orchestrator declared) between
    // the two calls.
    const first = computeAllowance('GREEN', 'GREEN', firstDibs, CONFIG_SYMMETRIC);
    const second = computeAllowance('GREEN', 'GREEN', secondDibs, CONFIG_SYMMETRIC);

    expect(first.priorityOrder).toEqual(['orchestrator-a']);
    expect(second.priorityOrder).toEqual(['orchestrator-b', 'orchestrator-a']);
  });

  it('integration: computeAllowance reflects dibs written to the real coordination file between two calls sharing the same (reused) axis-state inputs', async () => {
    const workDir = await mkdtemp(join(tmpdir(), 'allowance-fresh-dibs-'));
    const coordinationFilePath = join(workDir, 'coordination.json');
    try {
      await declareDibs(coordinationFilePath, {
        orchestratorId: 'orchestrator-a',
        desiredAgents: 2,
        declaredAt: 1_000,
      });
      const dibsBeforeSecondDeclaration = await readDibs(coordinationFilePath);
      const beforeResult = computeAllowance(
        'GREEN',
        'GREEN',
        dibsBeforeSecondDeclaration,
        CONFIG_SYMMETRIC,
      );

      // A second orchestrator declares dibs on the real coordination file —
      // the machine-state axis inputs below are unchanged ("the sample is
      // still fresh"), but dibs must be re-read fresh regardless.
      await declareDibs(coordinationFilePath, {
        orchestratorId: 'orchestrator-b',
        desiredAgents: 5,
        declaredAt: 2_000,
      });
      const dibsAfterSecondDeclaration = await readDibs(coordinationFilePath);
      const afterResult = computeAllowance(
        'GREEN',
        'GREEN',
        dibsAfterSecondDeclaration,
        CONFIG_SYMMETRIC,
      );

      expect(beforeResult.priorityOrder).toEqual(['orchestrator-a']);
      expect(afterResult.priorityOrder).toEqual(['orchestrator-a', 'orchestrator-b']);
    } finally {
      await rm(workDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// computePerOrchestratorAllowance — Phase 5, "dibs constrain
// allowance".
//
// RED by construction: `computePerOrchestratorAllowance` does not exist in
// `./allowance.mjs` yet. `computeAllowance` already computes `priorityOrder`
// but hands every caller the SAME flat per-axis cap regardless of how many
// other orchestrators have declared dibs — this is the gap closes.
//
// PINNED ALGORITHM (a documented starting hypothesis for the builder to
// implement, not a "correct" answer per this ticket's Investigation):
//
//   1. Compute the flat per-axis cap exactly as `computeAllowance` does today
//      (`min(memory-axis, disk-axis)`).
//   2. Filter `dibs` down to REAL orchestrator entries only — reserved
//      pseudo-entries (`isReservedEntry`, e.g. `__shared-machine-sample__`,
//      `__global-spawn-rate-bucket__`) never count as a contending
//      orchestrator and never consume any of the cap.
//   3. UNCONTENDED CASE — at most one real orchestrator entry exists in the
//      (reserved-filtered) dibs array: that lone orchestrator receives the
//      FULL cap as headroom, unclamped by its own `desiredAgents`. This is
//      today's existing behavior and MUST NOT regress — SKILL.md's own
//      worked example is explicit that "the allowance ... is headroom, not a
//      target".
//   4. CONTENDED CASE — two or more real orchestrators have live dibs: walk
//      `priorityOrder` (ascending `declaredAt`, ties broken by ascending
//      `orchestratorId` — the same deterministic rule `computeAllowance`
//      already uses to build `priorityOrder`). Each orchestrator, strictly in
//      that order, claims `min(its own desiredAgents, whatever axis-cap
//      remains)`; that claim is subtracted from the remaining pool before
//      moving to the next orchestrator. The function returns the claim
//      computed for `requestingOrchestratorId` specifically — every
//      orchestrator's share (including the earliest-priority one) is clamped
//      to its own declared `desiredAgents` once real contention (2+ live
//      orchestrators) exists; only the fully-uncontended solo case (step 3)
//      grants headroom beyond the caller's own ask.
//   5. An orchestrator declaring `desiredAgents: 0` claims 0 and leaves the
//      remaining pool untouched for whoever comes after it in priority order.
//   6. If `requestingOrchestratorId` has no live dibs entry at all, the walk
//      never returns a claim for it and the function's result is `0`
//      (untested edge in this suite; documented here for completeness).
//
// Signature: `computePerOrchestratorAllowance(memoryState, diskState, dibs,
// config, requestingOrchestratorId)` — a thin, pure extension built on top of
// `computeAllowance`, not a replacement for it.
// ---------------------------------------------------------------------------

describe('computePerOrchestratorAllowance — dibs constrain the per-orchestrator share', () => {
  it('single orchestrator, no contention (only its own dibs entry live) receives the FULL axis cap unchanged — headroom, not clamped to its own desiredAgents [non-regression]', () => {
    const dibs = [makeDibs('solo-orchestrator', 1, 1_000)];

    const share = computePerOrchestratorAllowance(
      'GREEN',
      'GREEN',
      dibs,
      CONFIG_SYMMETRIC,
      'solo-orchestrator',
    );

    // CONFIG_SYMMETRIC caps both axes at 6 — the lone orchestrator gets the
    // full 6, not merely the 1 it declared it desired.
    expect(share).toBe(6);
  });

  it('two orchestrators whose combined desiredAgents exceeds the cap: each is clamped to min(own desiredAgents, remaining-at-its-turn), summing to at most the cap', () => {
    const early = makeDibs('orchestrator-early', 4, 1_000); // declares first, wants 4
    const late = makeDibs('orchestrator-late', 5, 2_000); // declares second, wants 5
    const dibs = [early, late];

    const earlyShare = computePerOrchestratorAllowance(
      'GREEN',
      'GREEN',
      dibs,
      CONFIG_SYMMETRIC,
      'orchestrator-early',
    );
    const lateShare = computePerOrchestratorAllowance(
      'GREEN',
      'GREEN',
      dibs,
      CONFIG_SYMMETRIC,
      'orchestrator-late',
    );

    // Cap is 6. Earlier-priority orchestrator claims min(4, 6) = 4.
    expect(earlyShare).toBe(4);
    // Later orchestrator claims min(5, 6 - 4) = min(5, 2) = 2.
    expect(lateShare).toBe(2);

    expect(earlyShare + lateShare).toBeLessThanOrEqual(6);
    expect(earlyShare).toBeLessThanOrEqual(6);
    expect(lateShare).toBeLessThanOrEqual(6);
  });

  it('two orchestrators whose combined desiredAgents is LESS than the cap (no real contention): each still gets at least its own desiredAgents worth, not starved by the other\'s live entry', () => {
    const early = makeDibs('orchestrator-a', 1, 1_000);
    const late = makeDibs('orchestrator-b', 2, 2_000);
    const dibs = [early, late];

    // Cap is 6; combined desire is 3 — well under the cap.
    const shareA = computePerOrchestratorAllowance('GREEN', 'GREEN', dibs, CONFIG_SYMMETRIC, 'orchestrator-a');
    const shareB = computePerOrchestratorAllowance('GREEN', 'GREEN', dibs, CONFIG_SYMMETRIC, 'orchestrator-b');

    expect(shareA).toBeGreaterThanOrEqual(1);
    expect(shareB).toBeGreaterThanOrEqual(2);
    // Under the pinned sequential algorithm, un-contended combined demand
    // grants each orchestrator EXACTLY its own ask (no reason to starve OR
    // to inflate either share when nothing is actually being fought over).
    expect(shareA).toBe(1);
    expect(shareB).toBe(2);
  });

  it('an orchestrator declaring desiredAgents: 0 receives 0 and does not by itself create contention for the other live orchestrator [updated for the branch-selection fix, Medium finding]', () => {
    const zeroDesire = makeDibs('orchestrator-idle', 0, 1_000);
    const hungry = makeDibs('orchestrator-hungry', 5, 2_000);
    const dibs = [zeroDesire, hungry];

    const idleShare = computePerOrchestratorAllowance(
      'GREEN',
      'GREEN',
      dibs,
      CONFIG_SYMMETRIC,
      'orchestrator-idle',
    );
    const hungryShare = computePerOrchestratorAllowance(
      'GREEN',
      'GREEN',
      dibs,
      CONFIG_SYMMETRIC,
      'orchestrator-hungry',
    );

    expect(idleShare).toBe(0);
    // With only one entry declaring real spawn intent (`desiredAgents > 0`),
    // this is the UNCONTENDED case, not contended: the zero-desire entry
    // doesn't count as a competitor for branch selection (see
    // `computePerOrchestratorAllowance`'s doc comment), so
    // 'orchestrator-hungry' gets the full flat cap (6) as headroom — NOT
    // clamped down to its own ask of 5. (Previously this test pinned the
    // pre-fix, buggy contended-clamp value of 5; that was the exact Medium
    // finding this fix corrects — a co-resident zero-`desiredAgents` entry
    // must never by itself flip an otherwise-solo orchestrator into the
    // contended branch.)
    expect(hungryShare).toBe(6);
  });

  it('reserved pseudo-entries in the dibs array never receive a share and never consume cap from real orchestrators\' division', () => {
    const sharedMachineSample = {
      orchestratorId: '__shared-machine-sample__',
      sampledAt: 1_000,
      reading: {},
      declaredAt: 1_000,
    };
    const globalSpawnBucket = {
      orchestratorId: '__global-spawn-rate-bucket__',
      tokens: 10,
      lastRefillAt: 1_000,
      declaredAt: 1_000,
    };
    const realOrchestrator = makeDibs('orchestrator-only-real-one', 1, 2_000);
    const dibs = [sharedMachineSample, globalSpawnBucket, realOrchestrator];

    // Sanity: both reserved ids really are reserved per the shared predicate
    // this function is expected to filter by.
    expect(isReservedEntry(sharedMachineSample.orchestratorId)).toBe(true);
    expect(isReservedEntry(globalSpawnBucket.orchestratorId)).toBe(true);

    // A reserved entry is never granted a share even if explicitly queried.
    const reservedShare = computePerOrchestratorAllowance(
      'GREEN',
      'GREEN',
      dibs,
      CONFIG_SYMMETRIC,
      '__shared-machine-sample__',
    );
    expect(reservedShare).toBe(0);

    // With the two reserved pseudo-entries excluded, exactly one REAL
    // orchestrator remains live — the uncontended/solo case — so it still
    // receives the full cap as headroom, proving the reserved entries never
    // ate into its division.
    const realShare = computePerOrchestratorAllowance(
      'GREEN',
      'GREEN',
      dibs,
      CONFIG_SYMMETRIC,
      'orchestrator-only-real-one',
    );
    expect(realShare).toBe(6);
  });

  it('a live zero-desiredAgents entry (e.g. a --heartbeat beat\'s own dibs entry) never flips an otherwise-solo real orchestrator into the contended/clamped branch [regression, Medium finding]', () => {
    // Discriminating regression test for the Medium finding: the entry-COUNT
    // check that used to gate solo-vs-contended (`realDibs.length <= 1`)
    // could not tell "another live entry with real spawn intent" apart from
    // "another live entry that's a zero-intent heartbeat marker" — so ANY
    // second live dibs entry, even one declaring `desiredAgents: 0`, flipped
    // an otherwise-solo real orchestrator from the uncontended/headroom
    // branch into the contended/clamped-to-ask branch.
    //
    // This only surfaces when the real orchestrator's own ask is AT OR BELOW
    // the cap: with `desiredAgents=5` against a cap of 6 both the correct
    // (solo/headroom → 6) and buggy (contended/clamped → min(5, 6) === 5)
    // paths would still coincide if the ask were above the cap, exactly the
    // gap the existing CLI-level regression test (`cli.jest.spec.mjs`)
    // couldn't close. Here `desiredAgents=2` is strictly below the cap (6),
    // so the two paths diverge: 6 (correct) vs. 2 (buggy).
    const realOrchestrator = makeDibs('orchestrator-solo-real', 2, 1_000);
    const heartbeatEntry = makeDibs('orchestrator-heartbeat-other', 0, 2_000);
    const dibs = [realOrchestrator, heartbeatEntry];

    const share = computePerOrchestratorAllowance(
      'GREEN',
      'GREEN',
      dibs,
      CONFIG_SYMMETRIC,
      'orchestrator-solo-real',
    );

    // Full flat cap (6), the headroom bonus — NOT clamped down to the real
    // orchestrator's own ask of 2.
    expect(share).toBe(6);

    // The zero-desiredAgents heartbeat entry itself still correctly claims
    // nothing, whether or not it's ever actually queried.
    const heartbeatShare = computePerOrchestratorAllowance(
      'GREEN',
      'GREEN',
      dibs,
      CONFIG_SYMMETRIC,
      'orchestrator-heartbeat-other',
    );
    expect(heartbeatShare).toBe(0);
  });

  it('AMBER/RED axis states naturally return 0 for the per-orchestrator share, since axisAllowance already zeroes the cap before any division happens', () => {
    const dibs = [makeDibs('orchestrator-solo', 3, 1_000)];

    const shareOnAmber = computePerOrchestratorAllowance(
      'AMBER',
      'GREEN',
      dibs,
      CONFIG_SYMMETRIC,
      'orchestrator-solo',
    );
    const shareOnRed = computePerOrchestratorAllowance(
      'GREEN',
      'RED',
      dibs,
      CONFIG_SYMMETRIC,
      'orchestrator-solo',
    );

    expect(shareOnAmber).toBe(0);
    expect(shareOnRed).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Regression (High-priority fix): priority order must be keyed on
// `firstDeclaredAt` (an orchestrator's FIRST-EVER declaration, preserved
// unchanged across beats — see `./coordination-file.mjs`'s `declareDibs`),
// NOT `declaredAt` (refreshed on every beat for liveness pruning).
//
// The ORIGINAL bug reproduced here: `declareDibs` sets `declaredAt: now` on
// EVERY beat. If priority order were still sorted by `declaredAt` (as it was
// before this fix), the orchestrator whose beat is CURRENTLY running always
// just refreshed its own `declaredAt` to `now` — making itself look NEWEST
// (lowest priority) relative to the other orchestrator's older, unchanged
// `declaredAt` from whenever IT last beat. Under sustained contention where
// both orchestrators want >= the cap, this produces an anti-fair leap-frog:
// whichever orchestrator's turn it currently is always self-demotes to last
// place, and BOTH orchestrators permanently receive `0` — a genuine
// deadlock, not merely unfairness.
//
// This suite proves two things: (1) simulating what the OLD `declaredAt`-only
// ordering would have produced really does deadlock both orchestrators to
// 0/0, and (2) the fixed `firstDeclaredAt`-based ordering does not — the
// orchestrator that arrived first keeps its priority claim, stably, across
// every subsequent round.
// ---------------------------------------------------------------------------

describe('computePerOrchestratorAllowance — firstDeclaredAt priority survives sustained contention (High-priority regression)', () => {
  const CAP = { maxAgentsMemoryAxis: 3, maxAgentsDiskAxis: 3 };

  /**
   * Simulates what `declareDibs` persists for a beat: `declaredAt` is always
   * refreshed to `now`, but `firstDeclaredAt` is set once (on an
   * orchestrator's first-ever declaration) and never changes afterward —
   * exactly the invariant `declareDibs`'s upsert now enforces.
   */
  function beatEntry(orchestratorId, desiredAgents, now, firstDeclaredAt) {
    return { orchestratorId, desiredAgents, declaredAt: now, firstDeclaredAt };
  }

  it('demonstrates the ORIGINAL bug: sorting priority by declaredAt (ignoring firstDeclaredAt) permanently deadlocks both contending orchestrators to 0/0', () => {
    // A "declaredAt-only" ordering is exactly what dropping firstDeclaredAt
    // and reusing declaredAt as the sort key would produce — construct dibs
    // entries the same way a real beat would (each beat refreshes its own
    // declaredAt to "now"), and compute priority by declaredAt alone, mirroring
    // the pre-fix implementation.
    function legacyDeclaredAtOnlyShare(dibs, requestingOrchestratorId) {
      const sorted = [...dibs].sort(
        (a, b) => a.declaredAt - b.declaredAt || a.orchestratorId.localeCompare(b.orchestratorId),
      );
      let remaining = 3; // CAP, both axes GREEN
      let requested = 0;
      for (const entry of sorted) {
        const claim = Math.min(entry.desiredAgents, remaining);
        remaining -= claim;
        if (entry.orchestratorId === requestingOrchestratorId) requested = claim;
      }
      return requested;
    }

    // Round 1: A declares first (t=1000), B declares second (t=1100).
    let a = { orchestratorId: 'orch-a', desiredAgents: 5, declaredAt: 1_000 };
    let b = { orchestratorId: 'orch-b', desiredAgents: 5, declaredAt: 1_100 };

    // Round 2: A beats again — its declaredAt refreshes to 2000, now NEWER
    // than B's still-unchanged 1100 — under declaredAt-only ordering, A now
    // looks lowest-priority and gets 0; B (older declaredAt) claims the cap.
    a = { ...a, declaredAt: 2_000 };
    let shareA = legacyDeclaredAtOnlyShare([a, b], 'orch-a');
    let shareB = legacyDeclaredAtOnlyShare([a, b], 'orch-b');
    expect(shareA).toBe(0);
    expect(shareB).toBe(3);

    // Round 3: B beats — its declaredAt refreshes to 3000, now newer than A's
    // 2000 — B self-demotes; A claims the cap instead.
    b = { ...b, declaredAt: 3_000 };
    shareA = legacyDeclaredAtOnlyShare([a, b], 'orch-a');
    shareB = legacyDeclaredAtOnlyShare([a, b], 'orch-b');
    expect(shareA).toBe(3);
    expect(shareB).toBe(0);

    // Round 4: A beats again — declaredAt refreshes to 4000 — BOTH
    // orchestrators now have declaredAt values that keep leap-frogging each
    // other into last place on their own turn. This does not itself prove
    // deadlock on its own round, but sustained alternation like this is what
    // the CLI reported live: with a fresh beat firing immediately after each
    // grant, the requesting side is *always* the one whose declaredAt was
    // just refreshed — i.e. always self-demoted, always denied. That is the
    // permanent 0/0 the real bug reproduced. The fixed algorithm below never
    // exhibits this alternation at all.
    a = { ...a, declaredAt: 4_000 };
    shareA = legacyDeclaredAtOnlyShare([a, b], 'orch-a');
    shareB = legacyDeclaredAtOnlyShare([a, b], 'orch-b');
    expect(shareA).toBe(0);
    expect(shareB).toBe(3);
  });

  it('fixed behavior: across several rounds of A-then-B-then-A-then-B beats, the SAME first-arriving orchestrator consistently keeps its priority claim — no flapping to 0/0', () => {
    const FIRST_DECLARED_A = 1_000;
    const FIRST_DECLARED_B = 1_100; // B arrives after A

    let now = 1_000;
    const rounds = [];

    // Simulate 4 rounds of alternating beats (A, B, A, B), each refreshing
    // only that orchestrator's own `declaredAt` while `firstDeclaredAt` for
    // both stays pinned to when each first arrived — exactly what the fixed
    // `declareDibs` now guarantees.
    let aEntry = beatEntry('orch-a', 5, FIRST_DECLARED_A, FIRST_DECLARED_A);
    let bEntry = beatEntry('orch-b', 5, FIRST_DECLARED_B, FIRST_DECLARED_B);

    for (let round = 0; round < 4; round += 1) {
      now += 1_000;
      if (round % 2 === 0) {
        aEntry = beatEntry('orch-a', 5, now, FIRST_DECLARED_A);
      } else {
        bEntry = beatEntry('orch-b', 5, now, FIRST_DECLARED_B);
      }

      const dibs = [aEntry, bEntry];
      const shareA = computePerOrchestratorAllowance('GREEN', 'GREEN', dibs, CAP, 'orch-a');
      const shareB = computePerOrchestratorAllowance('GREEN', 'GREEN', dibs, CAP, 'orch-b');
      rounds.push({ shareA, shareB });
    }

    // A arrived first (FIRST_DECLARED_A < FIRST_DECLARED_B) and both want
    // more than the cap (5 each, cap is 3) — A must consistently claim the
    // full cap and B must consistently be starved to 0, in EVERY round,
    // regardless of whose beat most recently refreshed its own `declaredAt`.
    for (const { shareA, shareB } of rounds) {
      expect(shareA).toBe(3);
      expect(shareB).toBe(0);
    }

    // Never a 0/0 deadlock, and never flapping.
    expect(rounds.every(({ shareA, shareB }) => shareA + shareB === 3)).toBe(true);
  });

  it('fixed behavior: priorityOrder itself (computeAllowance) is keyed on firstDeclaredAt, not the more-recently-refreshed declaredAt', () => {
    // B's declaredAt (most recent beat) is numerically LATER than A's, but A
    // arrived first (`firstDeclaredAt` is earlier) — priorityOrder must still
    // list A first.
    const a = beatEntry('orch-a', 5, 9_000, 1_000);
    const b = beatEntry('orch-b', 5, 5_000, 2_000);

    const { priorityOrder } = computeAllowance('GREEN', 'GREEN', [a, b], CAP);

    expect(priorityOrder).toEqual(['orch-a', 'orch-b']);
  });
});

// ---------------------------------------------------------------------------
// selectPauseCandidate — pure function over injected running-agent metadata
// ---------------------------------------------------------------------------

describe('selectPauseCandidate', () => {
  it('picks the highest-RSS agent under the default "highest-rss" policy', () => {
    const runningAgents = [
      { agentId: 'agent-small', rssMb: 220, startedAt: 1_000 },
      { agentId: 'agent-big', rssMb: 900, startedAt: 2_000 },
      { agentId: 'agent-medium', rssMb: 500, startedAt: 3_000 },
    ];

    const candidate = selectPauseCandidate(runningAgents, { policy: 'highest-rss' });

    expect(candidate.agentId).toBe('agent-big');
  });

  it('returns a well-defined no-op (null) — not a throw — when there are zero running agents', () => {
    expect(() => selectPauseCandidate([], { policy: 'highest-rss' })).not.toThrow();
    expect(selectPauseCandidate([], { policy: 'highest-rss' })).toBeNull();
  });

  it('breaks an exact-RSS tie deterministically (documented rule: earliest startedAt, then ascending agentId) rather than by array/object insertion order', () => {
    // ASSUMPTION (Build Plan does not specify a tie-break rule): earliest
    // startedAt wins an RSS tie (the longer-running agent is judged more
    // disruptive to keep waiting on), with ascending agentId as a final
    // deterministic tie-break if startedAt also ties. Flagged in the
    // handoff note as a builder decision to confirm or override.
    const runningAgents = [
      { agentId: 'agent-later', rssMb: 500, startedAt: 5_000 },
      { agentId: 'agent-earlier', rssMb: 500, startedAt: 1_000 },
    ];
    const shuffled = [runningAgents[1], runningAgents[0]];

    const candidateA = selectPauseCandidate(runningAgents, { policy: 'highest-rss' });
    const candidateB = selectPauseCandidate(shuffled, { policy: 'highest-rss' });

    expect(candidateA.agentId).toBe('agent-earlier');
    expect(candidateB.agentId).toBe('agent-earlier');
  });

  it('two simultaneously-RED "orchestrators" computing over the SAME running-agents list deterministically pick the SAME candidate (no accidental randomness)', () => {
    // Simulates the Convergence Analysis's flagged gap: two orchestrators,
    // both observing RED, each independently calling selectPauseCandidate
    // over what is logically the same running-agent population (passed as
    // separate array instances, one of them shuffled, as two independent
    // processes would each build their own process list independently).
    const runningAgentsForOrchestratorX = [
      { agentId: 'agent-1', rssMb: 300, startedAt: 1_000 },
      { agentId: 'agent-2', rssMb: 950, startedAt: 2_000 },
      { agentId: 'agent-3', rssMb: 700, startedAt: 3_000 },
    ];
    const runningAgentsForOrchestratorY = [
      runningAgentsForOrchestratorX[2],
      runningAgentsForOrchestratorX[0],
      runningAgentsForOrchestratorX[1],
    ];

    const candidateX = selectPauseCandidate(runningAgentsForOrchestratorX, { policy: 'highest-rss' });
    const candidateY = selectPauseCandidate(runningAgentsForOrchestratorY, { policy: 'highest-rss' });

    expect(candidateX.agentId).toBe('agent-2');
    expect(candidateY.agentId).toBe(candidateX.agentId);
  });
});

// ---------------------------------------------------------------------------
// selectPauseCandidate — 'highest-phys-footprint' policy
//
// its inversion bug (`ps` RSS diverging from real physical memory
// pressure under macOS memory compression) is the reason `phys_footprint`
// (sampled by `./footprint-sampler.mjs`'s `sampleFootprint`) replaces `ps`
// RSS as the ranking signal for victim selection. `sampleFootprint` does
// real `execFileSync` I/O and is `async` — it cannot be called from inside
// this file's pure, synchronous, no-I/O functions (see this file's own
// header comment on `computeAllowance`/`consumeSpawnTokens`: "Pure,
// deterministic functions throughout — no I/O"). So `selectPauseCandidate`
// itself must stay pure: a real caller (the CLI layer, not `lib/`) is
// responsible for `await`-ing `sampleFootprint(pid)` once per running agent
// BEFORE calling `selectPauseCandidate`, and attaching the result onto each
// candidate as a new `physFootprintMb` field — exactly the same
// injected-data shape `runningAgents` already uses for `rssMb`/`startedAt`.
//
// ASSUMPTION (Build Plan/Convergence Analysis leave the exact shape open,
// flagged here for the implementer to confirm or override): the new policy
// literal is `'highest-phys-footprint'`, and `physFootprintMb` mirrors
// `sampleFootprint`'s own contract exactly — `number` on a successful
// sample, or `null` when the sample failed (never a fabricated `0` or a
// silently-substituted `rssMb`). A candidate for which footprint was never
// sampled at all simply omits the field (`physFootprintMb` absent/
// `undefined`) rather than the caller having to invent a value — this
// function treats "sample failed" (`null`) and "never sampled"
// (`undefined`) identically, per the codebase's existing "don't fabricate"
// convention (see `footprint-sampler.mjs` / `recon.mjs`'s NaN-sentinel
// doc comments).
// ---------------------------------------------------------------------------

describe("selectPauseCandidate — 'highest-phys-footprint' policy", () => {
  it('picks the candidate with the highest phys_footprint over one with higher RSS but lower footprint', () => {
    // The whole point of : `agent-high-rss` would win under the old
    // 'highest-rss' policy (900 > 400), but its physical footprint is
    // actually smaller than `agent-high-footprint`'s — under memory
    // compression, RSS and phys_footprint diverge, and this policy
    // must rank by the real signal, not the misleading one.
    const runningAgents = [
      { agentId: 'agent-high-rss', rssMb: 900, startedAt: 1_000, physFootprintMb: 150 },
      { agentId: 'agent-high-footprint', rssMb: 400, startedAt: 2_000, physFootprintMb: 700 },
    ];

    const candidate = selectPauseCandidate(runningAgents, { policy: 'highest-phys-footprint' });

    expect(candidate.agentId).toBe('agent-high-footprint');
  });

  it('falls back to the existing deterministic tie-break (earliest startedAt, then ascending agentId) on an exact phys_footprint tie', () => {
    const runningAgents = [
      { agentId: 'agent-later', rssMb: 100, startedAt: 5_000, physFootprintMb: 500 },
      { agentId: 'agent-earlier', rssMb: 999, startedAt: 1_000, physFootprintMb: 500 },
    ];
    const shuffled = [runningAgents[1], runningAgents[0]];

    const candidateA = selectPauseCandidate(runningAgents, { policy: 'highest-phys-footprint' });
    const candidateB = selectPauseCandidate(shuffled, { policy: 'highest-phys-footprint' });

    expect(candidateA.agentId).toBe('agent-earlier');
    expect(candidateB.agentId).toBe('agent-earlier');
  });

  it('returns a well-defined no-op (null) — not a throw — for an empty candidate list (must not regress the existing contract)', () => {
    expect(() => selectPauseCandidate([], { policy: 'highest-phys-footprint' })).not.toThrow();
    expect(selectPauseCandidate([], { policy: 'highest-phys-footprint' })).toBeNull();
  });

  it('excludes a candidate whose footprint sample failed (physFootprintMb: null) from ranking, rather than throwing or fabricating a value', () => {
    // `agent-failed-sample` has the numerically highest rssMb, but its
    // footprint sample failed (sampleFootprint resolved null — e.g. both
    // `footprint` and `vmmap --summary` failed for a PID that has already
    // exited). It must never be fabricated a footprint value (e.g. treating
    // null as 0, or silently substituting rssMb) — it is excluded from the
    // footprint ranking entirely, leaving the two candidates with genuine
    // footprint samples to compete normally.
    const runningAgents = [
      { agentId: 'agent-failed-sample', rssMb: 950, startedAt: 1_000, physFootprintMb: null },
      { agentId: 'agent-low-footprint', rssMb: 300, startedAt: 2_000, physFootprintMb: 200 },
      { agentId: 'agent-real-winner', rssMb: 310, startedAt: 3_000, physFootprintMb: 600 },
    ];

    const candidate = selectPauseCandidate(runningAgents, { policy: 'highest-phys-footprint' });

    expect(candidate.agentId).toBe('agent-real-winner');
  });

  it('excludes a candidate whose footprint was never sampled at all (physFootprintMb field absent) the same "don\'t fabricate" way as a failed sample', () => {
    // Convergence Analysis edge case: a candidate that legitimately never
    // had `sampleFootprint` run against it (e.g. it wasn't in the running
    // set at sampling time) must degrade identically to an explicit
    // `null` failure — never treated as footprint 0 or silently
    // rssMb-ranked ahead of a candidate with real footprint data.
    const runningAgents = [
      { agentId: 'agent-never-sampled', rssMb: 950, startedAt: 1_000 }, // no physFootprintMb field at all
      { agentId: 'agent-real-winner', rssMb: 300, startedAt: 2_000, physFootprintMb: 450 },
    ];

    const candidate = selectPauseCandidate(runningAgents, { policy: 'highest-phys-footprint' });

    expect(candidate.agentId).toBe('agent-real-winner');
  });

  it('degrades gracefully — falling back to the rssMb-ranked candidate (never a fabricated tie or a throw) — when NO candidate has valid footprint data', () => {
    // DECISION (Build Plan/Convergence Analysis explicitly leave this open;
    // recorded here for the implementer to confirm or override): rather
    // than returning null and leaving the orchestrator with zero eviction
    // option even though a RED memory axis genuinely needs one, total
    // footprint-sampling failure degrades to the OLD 'highest-rss' ranking
    // (same rssMb/startedAt/agentId tie-break machinery) as the least-bad
    // available signal — never a fabricated phys_footprint value, and never
    // an arbitrary/first-in-array pick.
    const runningAgents = [
      { agentId: 'agent-small-rss', rssMb: 200, startedAt: 1_000, physFootprintMb: null },
      { agentId: 'agent-big-rss', rssMb: 900, startedAt: 2_000 }, // never sampled
    ];

    const candidate = selectPauseCandidate(runningAgents, { policy: 'highest-phys-footprint' });

    expect(candidate.agentId).toBe('agent-big-rss');
  });
});

// ---------------------------------------------------------------------------
// Spawn-rate limiter — pure function of elapsed wall-clock time + bucket
// state, injectable fake clock, never a real setTimeout/sleep.
// ---------------------------------------------------------------------------

describe('consumeSpawnTokens — token-bucket spawn-rate limiter', () => {
  // ASSUMPTION (function/config shape not fixed by the outer test):
  // `consumeSpawnTokens({ bucketState, now, requestedCount, config })` where
  // `config = { capacityTokens, refillTokensPerMs }` and `bucketState` is
  // `null` on first use. Returns `{ allowed, grantedCount, bucketState }`
  // (a fresh, immutable next-state object) — deny-the-whole-burst semantics
  // (no partial grants) when the bucket can't cover `requestedCount`, so a
  // caller never spawns a partial, ambiguous batch.

  it('rejects a burst exceeding the cap even from a stale/reused GREEN sample (no real elapsed time between calls)', () => {
    const config = { capacityTokens: 3, refillTokensPerMs: 0 };
    const fakeNow = 10_000; // frozen — no wall-clock refill between calls

    const first = consumeSpawnTokens({
      bucketState: null,
      now: fakeNow,
      requestedCount: 3,
      config,
    });
    expect(first.allowed).toBe(true);
    expect(first.grantedCount).toBe(3);

    // Immediately after, at the SAME fake `now` (simulating a reused/stale
    // GREEN sample that never triggered a fresh recon), a second burst
    // request must be rejected — the bucket is empty and no time elapsed
    // to refill it, regardless of what the (stale) machine sample implied.
    const second = consumeSpawnTokens({
      bucketState: first.bucketState,
      now: fakeNow,
      requestedCount: 2,
      config,
    });
    expect(second.allowed).toBe(false);
    expect(second.grantedCount).toBe(0);
  });

  it('refills tokens purely as a function of elapsed fake-clock time, not real timers', () => {
    const config = { capacityTokens: 3, refillTokensPerMs: 1 / 1000 }; // 1 token/second
    const start = 0;

    const drained = consumeSpawnTokens({ bucketState: null, now: start, requestedCount: 3, config });
    expect(drained.allowed).toBe(true);

    // 2000ms of fake elapsed time later (no real setTimeout/sleep anywhere
    // in this test) — 2 tokens should have refilled.
    const afterElapsed = consumeSpawnTokens({
      bucketState: drained.bucketState,
      now: start + 2000,
      requestedCount: 2,
      config,
    });
    expect(afterElapsed.allowed).toBe(true);
    expect(afterElapsed.grantedCount).toBe(2);

    // A third token has not yet refilled — requesting one more immediately
    // must fail.
    const stillShort = consumeSpawnTokens({
      bucketState: afterElapsed.bucketState,
      now: start + 2000,
      requestedCount: 1,
      config,
    });
    expect(stillShort.allowed).toBe(false);
  });

  it('documents (and locks in via test) the global-spawn-rate-cap limitation: two independent per-orchestrator buckets are NOT coordinated by this pure function, so their combined throughput can exceed a single-orchestrator cap', () => {
    // Convergence Analysis's flagged gap: "Token-bucket state is naturally
    // per-process — with orchestrators as separate processes, independent
    // buckets could collectively exceed the intended GLOBAL spawn-rate cap
    // unless bucket state itself lives in the shared coordination file."
    //
    // This module's `consumeSpawnTokens` is, by design, a pure function
    // over WHATEVER bucketState the caller threads through it — it has no
    // awareness of any other bucket. That is a legitimate, intentional
    // design (per-orchestrator local backpressure is still useful even
    // without global coordination) but it is NOT sufficient, by itself, to
    // enforce one intended *global* cap across orchestrators. If a true
    // global cap is required, bucket state must be threaded through the
    // Phase 3 coordination file (shared, lock-serialised) rather than kept
    // as two independent in-memory instances, as done here. This test
    // exists to make that limitation explicit and falsifiable, not to
    // silently let it go unexamined.
    const perOrchestratorCap = 3;
    const intendedGlobalCap = 4; // e.g. the host can safely absorb 4 spawns/beat total

    const config = { capacityTokens: perOrchestratorCap, refillTokensPerMs: 0 };
    const fakeNow = 42_000;

    const bucketForOrchestratorA = consumeSpawnTokens({
      bucketState: null,
      now: fakeNow,
      requestedCount: perOrchestratorCap,
      config,
    });
    const bucketForOrchestratorB = consumeSpawnTokens({
      bucketState: null, // independent bucket instance — no shared state
      now: fakeNow,
      requestedCount: perOrchestratorCap,
      config,
    });

    expect(bucketForOrchestratorA.allowed).toBe(true);
    expect(bucketForOrchestratorB.allowed).toBe(true);

    const combinedGranted = bucketForOrchestratorA.grantedCount + bucketForOrchestratorB.grantedCount;

    // Each bucket, in isolation, correctly stayed within ITS OWN cap...
    expect(bucketForOrchestratorA.grantedCount).toBeLessThanOrEqual(perOrchestratorCap);
    expect(bucketForOrchestratorB.grantedCount).toBeLessThanOrEqual(perOrchestratorCap);
    // ...but the combined total is free to exceed the intended GLOBAL cap,
    // because nothing in this pure function coordinates the two buckets.
    // This is the documented, tested limitation: a true global cap requires
    // sharing bucket state via the Phase 3 coordination file, which this
    // function alone does not do.
    expect(combinedGranted).toBeGreaterThan(intendedGlobalCap);
  });
});

// ---------------------------------------------------------------------------
// buildTrafficLight — discriminated output, never a bare boolean
// ---------------------------------------------------------------------------

describe('buildTrafficLight — discriminated three-signal output', () => {
  it('produces { type: "spawn-allowed", allowance } when both axes are GREEN', () => {
    const trafficLight = buildTrafficLight({
      memoryState: 'GREEN',
      diskState: 'GREEN',
      allowance: 6,
      pauseCandidate: null,
    });

    expect(trafficLight.type).toBe('spawn-allowed');
    expect(trafficLight.allowance).toBe(6);
    expect(typeof trafficLight).not.toBe('boolean');
  });

  it('produces { type: "hold" } when either axis is AMBER (and neither is RED)', () => {
    const memoryAmber = buildTrafficLight({
      memoryState: 'AMBER',
      diskState: 'GREEN',
      allowance: 0,
      pauseCandidate: null,
    });
    const diskAmber = buildTrafficLight({
      memoryState: 'GREEN',
      diskState: 'AMBER',
      allowance: 0,
      pauseCandidate: null,
    });

    expect(memoryAmber.type).toBe('hold');
    expect(diskAmber.type).toBe('hold');
  });

  it('produces { type: "pause", pauseCandidate } on memory-RED (disk not RED), naming the specific agent to pause; disk-RED instead produces its own distinct "alert" signal', () => {
    const pauseCandidate = { agentId: 'agent-big', rssMb: 900, startedAt: Date.now() };

    const memoryRed = buildTrafficLight({
      memoryState: 'RED',
      diskState: 'GREEN',
      allowance: 0,
      pauseCandidate,
    });
    // UPDATED: disk-RED no longer shares memory-RED's
    // 'pause' shape — it now produces its own distinct 'alert' signal (see
    // the dedicated "Phase 4 disk-RED gets its own distinct alert
    // signal" describe block below for the full pinned contract).
    const diskRed = buildTrafficLight({
      memoryState: 'GREEN',
      diskState: 'RED',
      allowance: 0,
      pauseCandidate,
    });

    expect(memoryRed.type).toBe('pause');
    expect(memoryRed.pauseCandidate.agentId).toBe('agent-big');
    expect(diskRed.type).toBe('alert');
  });

  it('produces { type: "pause", pauseCandidate: null } on RED with zero running agents, rather than throwing or fabricating a candidate', () => {
    const trafficLight = buildTrafficLight({
      memoryState: 'RED',
      diskState: 'GREEN',
      allowance: 0,
      pauseCandidate: null,
    });

    expect(trafficLight.type).toBe('pause');
    expect(trafficLight.pauseCandidate).toBeNull();
  });

  it('is always one of exactly three documented signal shapes, never a bare boolean', () => {
    const shapes = [
      buildTrafficLight({ memoryState: 'GREEN', diskState: 'GREEN', allowance: 6, pauseCandidate: null }),
      buildTrafficLight({ memoryState: 'AMBER', diskState: 'GREEN', allowance: 0, pauseCandidate: null }),
      buildTrafficLight({ memoryState: 'RED', diskState: 'GREEN', allowance: 0, pauseCandidate: null }),
    ];

    const types = shapes.map((shape) => shape.type);
    expect(new Set(types)).toEqual(new Set(['spawn-allowed', 'hold', 'pause']));
    for (const shape of shapes) {
      expect(typeof shape).toBe('object');
      expect(typeof shape.type).toBe('string');
    }
  });
});

// ---------------------------------------------------------------------------
// Phase 2 — THROTTLE/CLEANUP/ALERT additive-only extension to
// buildTrafficLight's discriminated union.
//
// None of the three has a real backing signal today (no renice/cgroup-
// equivalent throttle mechanism, no confirmed-fastest-growing temp path for
// cleanup, no active notification channel for alert — see the Build Plan and
// SKILL.md's own open §10 question). The exact trigger condition for each is
// therefore a genuinely open implementation decision left to the builder —
// these tests pin the CONSTRAINTS that decision must satisfy, not a specific
// trigger. Do not add a stub/trigger here to make anything pass — that is
// exactly the guessing this suite is designed to avoid.
// ---------------------------------------------------------------------------

describe('buildTrafficLight — Phase 2 non-regression: existing hold/pause/spawn-allowed stay byte-identical', () => {
  // Full memory x disk 3x3 matrix — the single most important test in this
  // suite. Phase 2 is an ADDITIVE-ONLY change to this same function: every
  // input combination that produces spawn-allowed/hold/pause TODAY must
  // produce the exact same shape (same fields, no extras, no renames) once
  // THROTTLE/CLEANUP/ALERT land. If this test starts failing after Phase 2,
  // Phase 2 silently became a breaking rename/reshape.
  //
  // UPDATED: any row with `diskState === 'RED'` is no
  // longer byte-identical to the old pure-pause matrix — its pinned
  // design gives disk-RED a distinct `{ type: 'alert', reason }` signal,
  // evaluated BEFORE the memory-RED/disk-RED -> pause branch, so disk-RED
  // wins even when memoryState is also RED. This is a deliberate, pinned
  // supersession of the -era "disk-RED behaves exactly like memory-RED"
  // assumption baked into this matrix — NOT a silent regression. Only the
  // memory-RED/disk-NOT-RED row (`[RED, GREEN, ...]`) and the memory-RED/
  // disk-AMBER row (`[RED, AMBER, ...]`) still resolve to `pause`, since
  // disk is not RED in either. `reason` is `undefined` here because no
  // `diskReason` is passed — see the omitted-diskReason test below.
  const GREEN = 'GREEN';
  const AMBER = 'AMBER';
  const RED = 'RED';

  // UPDATED: every disk-RED row now also carries `memoryAlsoRed`,
  // pinning whether memoryState was RED on the same beat; the RED/RED row
  // additionally carries the `pauseCandidate` (here `null`, since that's what
  // this matrix passes in) rather than discarding it. See the dedicated
  // " disk-RED alert signal" describe block below for the full pinned
  // shape contract this matrix now matches.
  it.each([
    [GREEN, GREEN, { type: 'spawn-allowed', allowance: 6 }],
    [GREEN, AMBER, { type: 'hold' }],
    [GREEN, RED, { type: 'alert', reason: undefined, memoryAlsoRed: false }],
    [AMBER, GREEN, { type: 'hold' }],
    [AMBER, AMBER, { type: 'hold' }],
    [AMBER, RED, { type: 'alert', reason: undefined, memoryAlsoRed: false }],
    [RED, GREEN, { type: 'pause', pauseCandidate: null }],
    [RED, AMBER, { type: 'pause', pauseCandidate: null }],
    [RED, RED, { type: 'alert', reason: undefined, memoryAlsoRed: true, pauseCandidate: null }],
  ])('memory=%s disk=%s -> byte-identical shape %j', (memoryState, diskState, expectedShape) => {
    const result = buildTrafficLight({
      memoryState,
      diskState,
      allowance: 6,
      pauseCandidate: null,
    });

    expect(result).toEqual(expectedShape);
    // `toEqual` alone would tolerate an implementation that tacks an extra,
    // undocumented field onto 'hold'/'pause' in some matcher combinations —
    // assert the key set explicitly so "byte-identical" also means "no
    // additive junk fields", not just "the fields we bothered to check are
    // right".
    expect(Object.keys(result).sort()).toEqual(Object.keys(expectedShape).sort());
  });

  it('a real (non-null) pauseCandidate on memory-RED/disk-NOT-RED is passed through unchanged; the both-RED case is now alert, not pause, so pauseCandidate is irrelevant to its shape', () => {
    const pauseCandidate = { agentId: 'agent-big', rssMb: 900, startedAt: 1_000 };

    const memoryRedOnly = buildTrafficLight({
      memoryState: 'RED',
      diskState: 'GREEN',
      allowance: 0,
      pauseCandidate,
    });
    // UPDATED: both-RED now resolves to 'alert' (disk-RED
    // wins the tie), not 'pause' — see this describe block's header comment.
    const bothRed = buildTrafficLight({
      memoryState: 'RED',
      diskState: 'RED',
      allowance: 0,
      pauseCandidate,
    });

    expect(memoryRedOnly).toEqual({ type: 'pause', pauseCandidate });
    // UPDATED: the both-RED alert now carries the pauseCandidate it
    // was passed, plus memoryAlsoRed: true, rather than discarding it.
    expect(bothRed).toEqual({ type: 'alert', reason: undefined, memoryAlsoRed: true, pauseCandidate });
  });

  it('spawn-allowed flattens an { amount } object allowance identically to a bare number, exactly as today', () => {
    const withObjectAllowance = buildTrafficLight({
      memoryState: 'GREEN',
      diskState: 'GREEN',
      allowance: { amount: 4 },
      pauseCandidate: null,
    });
    const withBareNumberAllowance = buildTrafficLight({
      memoryState: 'GREEN',
      diskState: 'GREEN',
      allowance: 4,
      pauseCandidate: null,
    });

    expect(withObjectAllowance).toEqual({ type: 'spawn-allowed', allowance: 4 });
    expect(withBareNumberAllowance).toEqual({ type: 'spawn-allowed', allowance: 4 });
  });
});

describe('buildTrafficLight — Phase 2 RED-dominance precedence for the two-axes-RED / mixed-AMBER-RED edge cases (Convergence Analysis)', () => {
  // SUPERSEDED: the -era assertion below was that
  // simultaneous/mixed RED always resolves to 'pause', because 'pause' was
  // the only RED-driven literal that existed at the time. gives
  // disk-RED its own distinct 'alert' signal, evaluated BEFORE the
  // memory-RED-or-disk-RED -> pause branch — so once diskState is RED, the
  // result is 'alert', not 'pause', even when memoryState is also RED (a
  // pinned tie-break: disk-RED wins). The underlying principle these tests
  // were guarding — "doubling/mixing RED signals must never read as WEAKER
  // than a single RED axis" — still holds: 'alert' is not a demotion, it is
  // a MORE specific RED-driven signal than the generic 'pause'. See the
  // dedicated " disk-RED alert signal" describe block below for the
  // full pinned precedence matrix.
  it('both memoryState and diskState RED simultaneously deterministically resolves to "alert" — disk-RED wins the tie over the memory-RED/disk-RED -> pause branch, and this must never read as LESS urgent than today\'s single-axis pause', () => {
    const result = buildTrafficLight({
      memoryState: 'RED',
      diskState: 'RED',
      allowance: 0,
      pauseCandidate: { agentId: 'agent-x', rssMb: 999, startedAt: 1_000 },
    });

    expect(result.type).toBe('alert');
  });

  it('memoryState AMBER + diskState RED: disk-RED still dominates and now resolves to "alert", never regressing to hold (a weaker literal than today\'s pause/alert)', () => {
    const result = buildTrafficLight({
      memoryState: 'AMBER',
      diskState: 'RED',
      allowance: 0,
      pauseCandidate: { agentId: 'agent-y', rssMb: 500, startedAt: 1_000 },
    });

    expect(result.type).toBe('alert');
    expect(result.type).not.toBe('hold');
  });

  it('memoryState RED + diskState AMBER (mirror image): RED still dominates', () => {
    const result = buildTrafficLight({
      memoryState: 'RED',
      diskState: 'AMBER',
      allowance: 0,
      pauseCandidate: { agentId: 'agent-z', rssMb: 500, startedAt: 1_000 },
    });

    expect(result.type).toBe('pause');
    expect(result.type).not.toBe('hold');
  });
});

describe('buildTrafficLight — Phase 2 additive-only union shape: a fixed, named set of type strings', () => {
  // The full expected set once Phase 2 lands — enumerated here, not
  // discovered from production code, so this test cannot be satisfied by
  // production code merely re-exporting whatever it happens to produce.
  const KNOWN_TRAFFIC_LIGHT_TYPES = ['spawn-allowed', 'hold', 'pause', 'throttle', 'cleanup', 'alert'];

  it('every result produced across the full memory/disk GREEN/AMBER/RED battery has a .type drawn only from the fixed known set', () => {
    const states = ['GREEN', 'AMBER', 'RED'];
    const producedTypes = new Set();

    for (const memoryState of states) {
      for (const diskState of states) {
        const result = buildTrafficLight({
          memoryState,
          diskState,
          allowance: 6,
          pauseCandidate: null,
        });
        producedTypes.add(result.type);
        expect(KNOWN_TRAFFIC_LIGHT_TYPES).toContain(result.type);
      }
    }

    // UPDATED: 'alert' IS now reachable from this
    // memory/disk-only battery — any diskState === 'RED' row produces it
    // (disk-RED's own distinct signal). 'throttle'/'cleanup' remain
    // unreachable: neither has a trigger condition wired to
    // memoryState/diskState (still genuinely open, per the Build Plan).
    expect(producedTypes).toEqual(new Set(['spawn-allowed', 'hold', 'pause', 'alert']));
  });

  it("buildTrafficLight's JSDoc @returns documents all six fixed type literals as the one exhaustive union — not just today's three", () => {
    // RED by construction: today's JSDoc union (lib/allowance.mjs) lists
    // only 'spawn-allowed' | 'hold' | 'pause'. Phase 2 must extend this same
    // JSDoc annotation additively to also list 'throttle' | 'cleanup' |
    // 'alert' — the whole point of "additive-only" is that there remains ONE
    // documented, exhaustive return-type union, never an undocumented new
    // literal slipped in without updating the type signature that names it.
    const jsdocMatch = allowanceSource.match(
      /\/\*\*([\s\S]*?)\*\/\s*export function buildTrafficLight/,
    );
    expect(jsdocMatch).not.toBeNull();
    const buildTrafficLightJsDoc = jsdocMatch[1];

    for (const type of KNOWN_TRAFFIC_LIGHT_TYPES) {
      expect(buildTrafficLightJsDoc).toContain(`'${type}'`);
    }
  });
});

// ---------------------------------------------------------------------------
// Phase 3 — EVICT runtime-unreachability characterization guard.
//
// NOT a red test: no 'evict' branch exists anywhere in buildTrafficLight
// today, so this already passes. It exists as a regression guard — if a
// future change ever wires an 'evict' output (contrary to the owner's
// documented decision that no real checkpoint-and-exit capability exists;
// see doc-honesty.jest.spec.mjs and), this test starts failing and
// forces that change to be deliberate, not accidental.
// ---------------------------------------------------------------------------

describe("buildTrafficLight — EVICT is unreachable at runtime (Phase 3 characterization guard, not a red test)", () => {
  it('no combination of memoryState/diskState/allowance-shape/pauseCandidate ever produces { type: "evict" }', () => {
    const states = ['GREEN', 'AMBER', 'RED'];
    const pauseCandidates = [null, { agentId: 'agent-x', rssMb: 999, startedAt: 1_000 }];
    const allowances = [0, 3, { amount: 3 }];

    for (const memoryState of states) {
      for (const diskState of states) {
        for (const pauseCandidate of pauseCandidates) {
          for (const allowance of allowances) {
            const result = buildTrafficLight({ memoryState, diskState, allowance, pauseCandidate });
            expect(result.type).not.toBe('evict');
          }
        }
      }
    }
  });
});

// ---------------------------------------------------------------------------
// Phase 4 — disk-RED gets its own distinct `alert` signal.
//
// RED by construction: `buildTrafficLight` today has no branch that ever
// returns `{ type: 'alert', ... }` — `diskState === 'RED'` currently falls
// into the same `memoryState === 'RED' || diskState === 'RED'` -> `pause`
// branch as memory-RED. This suite pins the new, owner-approved contract
// (Design decision,):
//
//   - `diskState === 'RED'` (REGARDLESS of memoryState) -> distinct signal
//     `{ type: 'alert', reason: <diskReason param> }`, reusing the
//     already-documented-but-never-produced `'alert'` literal from
//     Phase 2's forward contract.
//   - This check runs BEFORE the existing
//     `memoryState === 'RED' || diskState === 'RED'` -> `pause` branch, so
//     disk-RED wins the tie when BOTH axes are RED simultaneously — pinned
//     explicitly below, not left implicit.
//   - `memoryState === 'RED'` with `diskState` NOT RED is unchanged:
//     `{ type: 'pause', pauseCandidate }`.
//   - AMBER/GREEN paths (`hold` / `spawn-allowed`) are unchanged.
//   - New optional `diskReason` param on `buildTrafficLight`'s single args
//     object. Omitting it must not crash — `reason: undefined` is a valid,
//     well-defined result, not an error. This guards a future/other caller
//     that omits `diskReason`, not `cli.mjs` itself — as of this same
//     Phase 4 change, `cli.mjs`'s `main()` always passes
//     `diskReason: diskResult.reason` (see the "omitted diskReason" test
//     below and `cli.jest.spec.mjs`'s own outer CLI-integration test for
//     this same acceptance criterion).
//
// Do not add a stub implementation to make this pass — Phase 4 turns it
// green for real, in `lib/allowance.mjs`.
// ---------------------------------------------------------------------------

describe('buildTrafficLight — Phase 4 disk-RED gets its own distinct alert signal', () => {
  it('diskState: "RED", memoryState: "GREEN" -> { type: "alert", reason: <disk reason> }', () => {
    const result = buildTrafficLight({
      memoryState: 'GREEN',
      diskState: 'RED',
      diskReason: 'disk usage at 97% of capacity',
      allowance: 0,
      pauseCandidate: null,
    });

    // disk-RED-only must report memoryAlsoRed: false, and must NOT
    // manufacture a pauseCandidate field (not even null) — it was never
    // computed for a non-memory-RED beat, so its genuine absence must not
    // be conflated with the "checked and found nothing" null case.
    expect(result).toEqual({ type: 'alert', reason: 'disk usage at 97% of capacity', memoryAlsoRed: false });
  });

  it('tie pin: diskState: "RED", memoryState: "RED" simultaneously -> alert wins over pause (disk-RED is checked before the memory-RED-or-disk-RED -> pause branch)', () => {
    const pauseCandidate = { agentId: 'agent-x', rssMb: 999, startedAt: 1_000 };
    const result = buildTrafficLight({
      memoryState: 'RED',
      diskState: 'RED',
      diskReason: 'disk usage at 99% of capacity',
      allowance: 0,
      pauseCandidate,
    });

    expect(result.type).toBe('alert');
    expect(result.type).not.toBe('pause');
    // the both-RED alert must carry the already-computed
    // pauseCandidate as a sibling field, not silently discard it — `type`
    // (disk-RED-wins,) is unchanged. `toEqual` (not `toMatchObject`)
    // pins the FULL shape, so a regression that drops `pauseCandidate`
    // again fails this assertion rather than being tolerated by it.
    expect(result).toEqual({
      type: 'alert',
      reason: 'disk usage at 99% of capacity',
      memoryAlsoRed: true,
      pauseCandidate,
    });
  });

  it('bounds/carries pauseCandidate on a both-RED alert beat instead of discarding it, while leaving type: "alert" (disk-RED-wins) unchanged', () => {
    const pauseCandidate = { agentId: 'agent-big', rssMb: 900, startedAt: 1_000 };
    const diskReason = 'disk usage at 98% of capacity';

    const result = buildTrafficLight({
      memoryState: 'RED',
      diskState: 'RED',
      diskReason,
      allowance: 0,
      pauseCandidate,
    });

    // RED by construction: today `buildTrafficLight` returns only
    // `{ type: 'alert', reason }` on this branch — the already-computed
    // `pauseCandidate` the caller passed in is silently discarded, so this
    // `toEqual` fails until the production branch is enriched to carry it
    // as a sibling field alongside the unchanged `type: 'alert'`.
    expect(result).toEqual({ type: 'alert', reason: diskReason, memoryAlsoRed: true, pauseCandidate });
  });

  it('both RED, no running agent found -> memoryAlsoRed: true, pauseCandidate: null (memory IS RED even with nothing to pause)', () => {
    const diskReason = 'disk usage at 98.5% of capacity';

    const result = buildTrafficLight({
      memoryState: 'RED',
      diskState: 'RED',
      diskReason,
      allowance: 0,
      pauseCandidate: null,
    });

    expect(result).toEqual({ type: 'alert', reason: diskReason, memoryAlsoRed: true, pauseCandidate: null });
  });

  it('regression pin: diskState: "GREEN", memoryState: "RED" -> unchanged pause (memory-RED-only path is untouched by the disk-RED alert branch)', () => {
    const pauseCandidate = { agentId: 'agent-big', rssMb: 900, startedAt: 1_000 };

    const result = buildTrafficLight({
      memoryState: 'RED',
      diskState: 'GREEN',
      diskReason: 'irrelevant — disk is not RED',
      allowance: 0,
      pauseCandidate,
    });

    expect(result).toEqual({ type: 'pause', pauseCandidate });
  });

  it('regression pin: diskState: "AMBER", memoryState: "GREEN" -> unchanged hold', () => {
    const result = buildTrafficLight({
      memoryState: 'GREEN',
      diskState: 'AMBER',
      diskReason: 'disk usage climbing but not yet RED',
      allowance: 0,
      pauseCandidate: null,
    });

    expect(result).toEqual({ type: 'hold' });
  });

  it('regression pin: both axes GREEN -> unchanged spawn-allowed', () => {
    const result = buildTrafficLight({
      memoryState: 'GREEN',
      diskState: 'GREEN',
      diskReason: 'irrelevant — disk is not RED',
      allowance: 6,
      pauseCandidate: null,
    });

    expect(result).toEqual({ type: 'spawn-allowed', allowance: 6 });
  });

  it('omitting diskReason on a disk-RED beat does not crash and yields reason: undefined — guards a future/other caller that omits diskReason; cli.mjs main() itself always passes it as of this phase', () => {
    expect(() =>
      buildTrafficLight({
        memoryState: 'GREEN',
        diskState: 'RED',
        allowance: 0,
        pauseCandidate: null,
      }),
    ).not.toThrow();

    const result = buildTrafficLight({
      memoryState: 'GREEN',
      diskState: 'RED',
      allowance: 0,
      pauseCandidate: null,
    });

    // UPDATED: memory is GREEN here, so memoryAlsoRed: false and no
    // pauseCandidate key.
    expect(result).toEqual({ type: 'alert', reason: undefined, memoryAlsoRed: false });
  });
});

// ---------------------------------------------------------------------------
// Phase 5 (Part A) — headroom-derived numeric allowance.
//
// RED by construction: `computeHeadroomCap` does not exist in
// `./lib/allowance.mjs` yet. It is imported DYNAMICALLY below (a lazy
// `await import('./allowance.mjs')` inside `beforeAll`, not a static
// top-level `import { computeHeadroomCap } from './allowance.mjs'`)
// specifically so a missing named export does not hard-fail this whole
// file's ESM module-link step — a static import of a name a target module
// doesn't (yet) export is a `SyntaxError` at load time in Node's ESM
// implementation, which would take down every already-passing describe
// block above it in this same file. Dynamic `import()` returns a namespace
// object instead; reading a not-yet-existing property off it is `undefined`
// (no throw) until a test actually calls it, which is where these tests
// legitimately go red.
//
// PINNED CONTRACT (design decision surfaced here for the implementer to
// build against):
//
//   computeHeadroomCap({ freeRamMb, osBaselineMb, reserveMb, perAgentRamMb })
//     -> number (an integer agent count, never negative, never Infinity/NaN)
//
//   amount = Math.floor((freeRamMb - osBaselineMb - reserveMb) / perAgentRamMb)
//   return Math.max(0, amount)
//
//   Safety clamps (never fabricate a non-finite or negative cap):
//     - `perAgentRamMb` <= 0, non-finite (NaN/Infinity), or not a number at
//       all -> the whole config is degenerate -> return 0 (never divide by
//       zero/negative, never Infinity).
//     - `freeRamMb` non-finite (NaN/Infinity/missing) -> no headroom data ->
//       clamp to 0 before the subtraction (err conservative — "no headroom
//       data" must never imply "unlimited headroom").
//     - `osBaselineMb`/`reserveMb` are NOT independently clamped here (the
//       caller is expected to supply finite host-capacity constants) — only
//       `freeRamMb` (a live-sampled, possibly-missing value) and
//       `perAgentRamMb` (a config value that could be misconfigured to 0)
//       get defensive clamps, matching this codebase's existing convention
//       of validating the INPUTS most likely to come from a live/degraded
//       sample rather than every argument uniformly.
//
// Backward-compat seam pinned into `computeAllowance`/
// `computePerOrchestratorAllowance` (both already exist and are exercised
// via the normal, already-passing static import above — no dynamic import
// needed for these two):
//
//   `config` (the existing `{ maxAgentsMemoryAxis, maxAgentsDiskAxis }`
//   shape) gains a new, OPTIONAL field: `memoryHeadroomCap` (number). When
//   present, it REPLACES `config.maxAgentsMemoryAxis` as the memory axis's
//   GREEN cap (fed into the same `axisAllowance(state, cap)` mapping the
//   disk axis already uses unchanged) — it does not merely sit alongside
//   the flat constant unused. When absent (every existing caller/test in
//   this file above), the memory axis cap is `config.maxAgentsMemoryAxis`
//   exactly as today — byte-identical, unchanged behavior. The disk axis is
//   untouched by this phase (no `diskHeadroomCap` — out of scope here).
// ---------------------------------------------------------------------------

describe('computeHeadroomCap — headroom-derived numeric memory allowance (Phase 5 Part A)', () => {
  let computeHeadroomCap;

  beforeAll(async () => {
    const allowanceModule = await import('./allowance.mjs');
    computeHeadroomCap = allowanceModule.computeHeadroomCap;
  });

  it('normal case: computes floor((freeRamMb - osBaselineMb - reserveMb) / perAgentRamMb)', () => {
    // (8192 - 2048 - 1024) / 512 = 5120 / 512 = 10 exactly.
    const cap = computeHeadroomCap({
      freeRamMb: 8192,
      osBaselineMb: 2048,
      reserveMb: 1024,
      perAgentRamMb: 512,
    });

    expect(cap).toBe(10);
  });

  it('negative-after-subtraction case clamps to 0, never a negative agent count', () => {
    // (2000 - 2048 - 1024) / 512 is deeply negative.
    const cap = computeHeadroomCap({
      freeRamMb: 2000,
      osBaselineMb: 2048,
      reserveMb: 1024,
      perAgentRamMb: 512,
    });

    expect(cap).toBe(0);
  });

  it.each([
    ['zero', 0],
    ['negative', -256],
    ['NaN', NaN],
  ])('perAgentRamMb=%s (degenerate config) clamps to 0 rather than producing Infinity/NaN', (_label, perAgentRamMb) => {
    const cap = computeHeadroomCap({
      freeRamMb: 8192,
      osBaselineMb: 2048,
      reserveMb: 1024,
      perAgentRamMb,
    });

    expect(cap).toBe(0);
    expect(Number.isFinite(cap)).toBe(true);
    expect(Number.isNaN(cap)).toBe(false);
  });

  it('non-finite freeRamMb (no live headroom sample) clamps to 0 rather than assuming unlimited headroom', () => {
    for (const badFreeRamMb of [NaN, Infinity, undefined]) {
      const cap = computeHeadroomCap({
        freeRamMb: badFreeRamMb,
        osBaselineMb: 2048,
        reserveMb: 1024,
        perAgentRamMb: 512,
      });

      expect(cap).toBe(0);
    }
  });

  it('proves the formula genuinely replaces the flat DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis=3 constant: a formula-derived cap can be either smaller or larger than 3', () => {
    const FLAT_CONSTANT = 3;

    // Smaller: tight headroom on a nearly-full machine.
    const smallerCap = computeHeadroomCap({
      freeRamMb: 3000,
      osBaselineMb: 2048,
      reserveMb: 512,
      perAgentRamMb: 512,
    });
    expect(smallerCap).toBeLessThan(FLAT_CONSTANT);

    // Larger: generous headroom on a mostly-idle machine.
    const largerCap = computeHeadroomCap({
      freeRamMb: 20000,
      osBaselineMb: 2048,
      reserveMb: 1024,
      perAgentRamMb: 512,
    });
    expect(largerCap).toBeGreaterThan(FLAT_CONSTANT);
  });

  it('never returns Infinity or NaN for any combination of finite/non-finite inputs (fuzz-style sweep)', () => {
    const values = [NaN, Infinity, -Infinity, 0, -100, 100, 8192];
    for (const freeRamMb of values) {
      for (const perAgentRamMb of values) {
        const cap = computeHeadroomCap({ freeRamMb, osBaselineMb: 2048, reserveMb: 1024, perAgentRamMb });
        expect(Number.isFinite(cap)).toBe(true);
        expect(cap).toBeGreaterThanOrEqual(0);
      }
    }
  });
});

describe('computeAllowance — memoryHeadroomCap config override (Phase 5 Part A)', () => {
  it('backward-compat: config WITHOUT memoryHeadroomCap uses config.maxAgentsMemoryAxis exactly as today, unchanged', () => {
    const result = computeAllowance('GREEN', 'GREEN', [], { maxAgentsMemoryAxis: 3, maxAgentsDiskAxis: 3 });

    expect(result.amount).toBe(3);
    expect(result.constrainedBy).toBe('both');
  });

  it('when memoryHeadroomCap is present, it REPLACES maxAgentsMemoryAxis for the memory axis GREEN cap — not merely added alongside it unused', () => {
    // maxAgentsMemoryAxis=3 (the old flat cap) would constrain to 3 if it
    // were still in effect; memoryHeadroomCap=10 must be what actually wins,
    // proving the disk axis (cap 6) becomes the binding constraint instead.
    const result = computeAllowance('GREEN', 'GREEN', [], {
      maxAgentsMemoryAxis: 3,
      maxAgentsDiskAxis: 6,
      memoryHeadroomCap: 10,
    });

    expect(result.amount).toBe(6);
    expect(result.constrainedBy).toBe('disk');
  });

  it('a SMALLER memoryHeadroomCap than the disk cap correctly becomes the binding (memory) constraint', () => {
    const result = computeAllowance('GREEN', 'GREEN', [], {
      maxAgentsMemoryAxis: 3,
      maxAgentsDiskAxis: 6,
      memoryHeadroomCap: 2,
    });

    expect(result.amount).toBe(2);
    expect(result.constrainedBy).toBe('memory');
  });

  it('a non-GREEN memory state still zeroes the memory axis regardless of memoryHeadroomCap (axisAllowance gating is unaffected by this change)', () => {
    const result = computeAllowance('AMBER', 'GREEN', [], {
      maxAgentsMemoryAxis: 3,
      maxAgentsDiskAxis: 6,
      memoryHeadroomCap: 10,
    });

    expect(result.amount).toBe(0);
  });

  it('composition: a headroom cap COMPUTED BY computeHeadroomCap (not a hand-picked literal) flows through computeAllowance as the memory axis cap', async () => {
    const { computeHeadroomCap: dynamicComputeHeadroomCap } = await import('./allowance.mjs');

    const headroomCap = dynamicComputeHeadroomCap({
      freeRamMb: 8192,
      osBaselineMb: 2048,
      reserveMb: 1024,
      perAgentRamMb: 512,
    }); // -> 10, per the computeHeadroomCap suite above

    const result = computeAllowance('GREEN', 'GREEN', [], {
      maxAgentsMemoryAxis: 3, // the old flat constant — must NOT win here
      maxAgentsDiskAxis: 20, // deliberately generous so memory is binding
      memoryHeadroomCap: headroomCap,
    });

    expect(result.amount).toBe(10);
    expect(result.constrainedBy).toBe('memory');
  });

  it('computePerOrchestratorAllowance also threads memoryHeadroomCap through: a solo orchestrator receives the headroom-derived cap, not the flat maxAgentsMemoryAxis', () => {
    const dibs = [makeDibs('solo-headroom-orchestrator', 1, 1_000)];

    const share = computePerOrchestratorAllowance(
      'GREEN',
      'GREEN',
      dibs,
      { maxAgentsMemoryAxis: 3, maxAgentsDiskAxis: 20, memoryHeadroomCap: 10 },
      'solo-headroom-orchestrator',
    );

    expect(share).toBe(10);
  });
});
