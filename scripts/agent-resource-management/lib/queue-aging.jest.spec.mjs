// RED unit tests for the not-yet-implemented AGING policy on top of
// scripts/agent-resource-management/lib/queue.mjs's `sortQueueEntries`
// (Phase 3 — "Dispatcher drain: aging policy default").
//
// Direct-import unit tests, mirroring ./queue-lease.jest.spec.mjs's idiom:
// no real filesystem needed here at all — `sortQueueEntries` is a pure
// function over an in-memory array, so these tests construct plain entry
// objects by hand (never going through `enqueueItem`) purely so each
// entry's `enqueuedAt` can be pinned to an exact, deterministic offset from
// a fixed `now` — no real wall-clock waits, ever.
//
// This is a SEPARATE file from ./queue.jest.spec.mjs and
// ./queue-lease.jest.spec.mjs (one-concern-per-file, matching this
// directory's established pattern) because it specifies a NEW cross-cutting
// concern — aging-based rank promotion — layered on top of (not replacing)
// the existing priority/FIFO contract those two files already pin.
//
// ---------------------------------------------------------------------------
// FUNCTION-SHAPE DECISION (stated explicitly per this phase's instructions):
//
// This suite specifies that the EXISTING `sortQueueEntries` function gains a
// new, OPTIONAL second parameter: `sortQueueEntries(entries, now)` — not a
// parallel `sortQueueEntriesWithAging` function. Two things follow from
// "optional":
//
//   1. `sortQueueEntries` must become an EXPORTED name (it is a private,
//      unexported function in queue.mjs as of this commit) so this file, and
//      any future direct caller, can reach it without going through
//      `peekQueue`/`dequeueItem`/`claimQueueEntry`.
//
//   2. Aging must be considered ONLY when the caller explicitly supplies a
//      finite `now` — NOT "default to `Date.now()` and always consider
//      aging". `peekQueue`/`dequeueItem`/`claimQueueEntry` all currently call
//      `sortQueueEntries(entries)` with zero arguments; if the aging branch
//      activated by default off the real wall clock, every one of those
//      existing call sites would silently start promoting old entries the
//      instant the implementer adds this parameter — a behavior change with
//      no call-site edit and no test coverage protecting against it. Gating
//      aging strictly on "was `now` explicitly passed AND is it a finite
//      number" (mirrors `claimQueueEntry`'s own `isFiniteNumber(options.now)`
//      convention) makes the parameter purely additive: omit it, and
//      `sortQueueEntries` is byte-for-byte the function it is today. Section
//      7 below locks in that non-regression explicitly. The implementer
//      updates call sites to pass `now` only where Phase 4/5's dispatcher
//      wiring actually wants aging to apply — out of scope for this test
//      file, which only pins the function's own contract.
//
// `AGING_THRESHOLD_MS` (10 minutes, `10 * 60 * 1000`) is the new exported
// constant this suite pins as the chosen default aging policy, per the
// Build Plan. `age = now - Date.parse(entry.enqueuedAt)`; `age >=
// AGING_THRESHOLD_MS` counts as "aged" (inclusive boundary).
//
// Promotion is rank-bump-by-one-tier, SORT-TIME only — never mutates
// `entry.priority` on disk, never stored as mutable state, recomputed fresh
// on every `sortQueueEntries` call from `enqueuedAt` + `now`:
//   low    (rank 2) -> promoted rank 1 (competes at "normal"'s rank)
//   normal (rank 1) -> promoted rank 0 (competes at "high"'s rank)
//   high   (rank 0) -> stays rank 0 (no wraparound, no error, no
//                      double-promotion past "high")
//
// A malformed/missing `enqueuedAt` must not throw and must NOT be treated as
// aged — falls back to "age zero, no boost" — mirroring the module's
// existing defensive philosophy for malformed `priority` values (never
// throw, never NaN; see `sortQueueEntries`'s own doc comment for the
// `PRIORITY_RANK`/`Object.create(null)` rationale this preserves).
//
// `sortQueueEntries` does NOT exist as an EXPORT of queue.mjs as of this
// commit (it is an unexported, module-private function today), and
// `AGING_THRESHOLD_MS` does not exist at all — every test below is expected
// to fail at import time (`does not provide an export named 'sortQueueEntries'`
// / `'AGING_THRESHOLD_MS'`) until the implementer makes both changes. Do not
// add an implementation to make this pass — the builder does that in the
// next phase to turn this green.
// ---------------------------------------------------------------------------

import { AGING_THRESHOLD_MS, sortQueueEntries } from './queue.mjs';

// Fixed reference instant — never `Date.now()` — so every test is
// deterministic and requires no real wall-clock wait.
const NOW = 1_700_000_000_000;

/**
 * Builds a minimal, well-formed-shaped queue entry with an exact
 * `enqueuedAt` derived from `NOW - ageOffsetMs`, so a caller can dial in a
 * precise age relative to `NOW` (and thus relative to `AGING_THRESHOLD_MS`)
 * without any wall-clock dependency.
 *
 * @param {{ agentClass: string, priority: string, ageOffsetMs: number }} params
 * @returns {object}
 */
function entryAgedBy({ agentClass, priority, ageOffsetMs }) {
  return {
    agentClass,
    priority,
    commandRef: `echo ${agentClass}`,
    orchestratorId: 'test-orchestrator',
    enqueuedAt: new Date(NOW - ageOffsetMs).toISOString(),
  };
}

function labels(sorted) {
  return sorted.map((entry) => entry.agentClass);
}

// ---------------------------------------------------------------------------
// 0. Pinned aging-threshold constant
// ---------------------------------------------------------------------------

describe('AGING_THRESHOLD_MS', () => {
  it('is exported as 600_000 (10 minutes) — the chosen default aging policy this suite pins', () => {
    expect(AGING_THRESHOLD_MS).toBe(10 * 60 * 1000);
  });
});

// ---------------------------------------------------------------------------
// 1. An aged low-priority entry is promoted to compete at normal's rank
// ---------------------------------------------------------------------------

describe('an entry aged past the threshold at low priority is promoted to compete at normal rank', () => {
  it('sorts an aged-low entry AHEAD of a fresh (unaged) normal-priority entry', () => {
    const agedLow = entryAgedBy({ agentClass: 'aged-low', priority: 'low', ageOffsetMs: AGING_THRESHOLD_MS + 60_000 });
    const freshNormal = entryAgedBy({ agentClass: 'fresh-normal', priority: 'normal', ageOffsetMs: 0 });

    const sorted = sortQueueEntries([freshNormal, agedLow], NOW);

    // Both entries tie at normal's rank once aged-low is promoted; the tie
    // breaks by enqueuedAt, and aged-low is genuinely older, so it still
    // comes first — the promotion doesn't just "match" normal, an older
    // promoted entry legitimately outranks a younger same-tier one.
    expect(labels(sorted)).toEqual(['aged-low', 'fresh-normal']);
  });

  it('a NOT-yet-aged low-priority entry still sorts BEHIND a fresh normal entry — no promotion below threshold', () => {
    const freshLow = entryAgedBy({ agentClass: 'fresh-low', priority: 'low', ageOffsetMs: AGING_THRESHOLD_MS - 1 });
    const freshNormal = entryAgedBy({ agentClass: 'fresh-normal', priority: 'normal', ageOffsetMs: 0 });

    const sorted = sortQueueEntries([freshLow, freshNormal], NOW);

    expect(labels(sorted)).toEqual(['fresh-normal', 'fresh-low']);
  });
});

// ---------------------------------------------------------------------------
// 2. An aged high-priority entry stays at top rank — no wraparound, no error
// ---------------------------------------------------------------------------

describe('an entry aged past the threshold at high priority stays at top rank — no wraparound, no double-promotion', () => {
  it('does not throw, and an aged-high entry ties (rather than illegitimately outranks-by-double-promotion) with a fresh-high entry, breaking by enqueuedAt', () => {
    const agedHigh = entryAgedBy({ agentClass: 'aged-high', priority: 'high', ageOffsetMs: AGING_THRESHOLD_MS + 120_000 });
    const agedNormal = entryAgedBy({ agentClass: 'aged-normal', priority: 'normal', ageOffsetMs: AGING_THRESHOLD_MS + 90_000 });
    const freshHigh = entryAgedBy({ agentClass: 'fresh-high', priority: 'high', ageOffsetMs: 5_000 });

    let sorted;
    expect(() => {
      sorted = sortQueueEntries([freshHigh, agedNormal, agedHigh], NOW);
    }).not.toThrow();

    // All three land at effective rank 0 (agedHigh stays at high's rank;
    // agedNormal is promoted exactly one tier, from normal's rank to
    // high's rank; freshHigh is already at high's rank) — so the only
    // discriminator left is enqueuedAt, oldest first. If aged-high had
    // instead wrapped/double-promoted past high's rank, or agedNormal had
    // been left un-promoted, this exact order would not hold.
    expect(labels(sorted)).toEqual(['aged-high', 'aged-normal', 'fresh-high']);
  });
});

// ---------------------------------------------------------------------------
// 3. Inclusive/exclusive boundary at EXACTLY AGING_THRESHOLD_MS
// ---------------------------------------------------------------------------

describe('the aging boundary is inclusive: age === AGING_THRESHOLD_MS counts as aged, age === AGING_THRESHOLD_MS - 1 does not', () => {
  it('an entry at exactly the threshold is promoted; an entry one millisecond younger is not', () => {
    const atBoundary = entryAgedBy({ agentClass: 'at-boundary', priority: 'low', ageOffsetMs: AGING_THRESHOLD_MS });
    const justBelowBoundary = entryAgedBy({
      agentClass: 'just-below-boundary',
      priority: 'low',
      ageOffsetMs: AGING_THRESHOLD_MS - 1,
    });
    const freshNormal = entryAgedBy({ agentClass: 'fresh-normal', priority: 'normal', ageOffsetMs: 0 });

    const sorted = sortQueueEntries([justBelowBoundary, freshNormal, atBoundary], NOW);

    // at-boundary (aged, promoted to normal's rank) ties with fresh-normal
    // at rank 1 and wins the enqueuedAt tiebreak (it's older); just-below-
    // boundary is NOT promoted, stays at low's rank 2, sorts last.
    expect(labels(sorted)).toEqual(['at-boundary', 'fresh-normal', 'just-below-boundary']);
  });
});

// ---------------------------------------------------------------------------
// 4. Two aged entries promoted to the same tier still tie-break by
// enqueuedAt oldest-first — aging doesn't disturb the existing FIFO order
// within a tier.
// ---------------------------------------------------------------------------

describe('two entries both promoted into the same effective tier still tie-break by enqueuedAt oldest-first', () => {
  it('orders two aged-low entries (both promoted to normal rank) oldest-enqueuedAt-first', () => {
    const olderAgedLow = entryAgedBy({
      agentClass: 'older-aged-low',
      priority: 'low',
      ageOffsetMs: AGING_THRESHOLD_MS + 20_000,
    });
    const youngerAgedLow = entryAgedBy({
      agentClass: 'younger-aged-low',
      priority: 'low',
      ageOffsetMs: AGING_THRESHOLD_MS + 5_000,
    });

    const sorted = sortQueueEntries([youngerAgedLow, olderAgedLow], NOW);

    expect(labels(sorted)).toEqual(['older-aged-low', 'younger-aged-low']);
  });
});

// ---------------------------------------------------------------------------
// 5. Malformed/missing enqueuedAt never throws and is NOT treated as aged
// ---------------------------------------------------------------------------

describe('a malformed or missing enqueuedAt does not throw and is treated as unaged (age zero, no promotion)', () => {
  it('an entry with enqueuedAt entirely missing sorts as an ordinary unaged low-priority entry, behind a fresh normal', () => {
    const malformed = {
      agentClass: 'missing-enqueued-at',
      priority: 'low',
      commandRef: 'echo missing',
      orchestratorId: 'test-orchestrator',
      // enqueuedAt deliberately omitted.
    };
    const freshNormal = entryAgedBy({ agentClass: 'fresh-normal', priority: 'normal', ageOffsetMs: 0 });

    let sorted;
    expect(() => {
      sorted = sortQueueEntries([malformed, freshNormal], NOW);
    }).not.toThrow();

    expect(labels(sorted)).toEqual(['fresh-normal', 'missing-enqueued-at']);
  });

  it('an entry with a non-date, unparsable enqueuedAt string sorts as an ordinary unaged low-priority entry, behind a fresh normal', () => {
    const malformed = {
      agentClass: 'garbage-enqueued-at',
      priority: 'low',
      commandRef: 'echo garbage',
      orchestratorId: 'test-orchestrator',
      enqueuedAt: 'not-a-real-timestamp',
    };
    const freshNormal = entryAgedBy({ agentClass: 'fresh-normal', priority: 'normal', ageOffsetMs: 0 });

    let sorted;
    expect(() => {
      sorted = sortQueueEntries([malformed, freshNormal], NOW);
    }).not.toThrow();

    expect(labels(sorted)).toEqual(['fresh-normal', 'garbage-enqueued-at']);
  });
});

// ---------------------------------------------------------------------------
// 6. A 30-item mixed-age, mixed-priority fixture matches a hand-computed
// expected order — doubles as groundwork for Phase 5's outer 30-item/
// ceiling-4 acceptance test.
// ---------------------------------------------------------------------------

describe('a 30-item mixed-priority, mixed-age fixture sorts into the hand-computed expected order', () => {
  // Six groups of five entries each, spanning all three priorities and both
  // sides of AGING_THRESHOLD_MS (600_000 ms). Every entry's `ageOffsetMs` is
  // a distinct value chosen so the WHOLE 30-item order is unambiguous (no
  // unintended ties), and each group's membership in "aged" vs "fresh" is
  // constructed directly from AGING_THRESHOLD_MS, not merely labelled.
  //
  // Effective rank after aging:
  //   high-aged   (rank 0, no promotion — already top)
  //   high-fresh  (rank 0, no promotion)
  //   normal-aged (rank 0, promoted from rank 1)
  //   normal-fresh(rank 1, no promotion)
  //   low-aged    (rank 1, promoted from rank 2)
  //   low-fresh   (rank 2, no promotion)
  //
  // Within each effective rank, order is enqueuedAt ascending (oldest/
  // largest ageOffsetMs first) — computed by hand below, not by calling the
  // function under test.
  it('produces the expected 30-item order', () => {
    const groupOffsets = (base, step) => Array.from({ length: 5 }, (_, i) => base - i * step);

    const highAgedOffsets = groupOffsets(900_000, 20_000); // 900000..820000 (all >= 600000: aged)
    const highFreshOffsets = groupOffsets(50_000, 2_000); // 50000..42000 (all < 600000: fresh)
    const normalAgedOffsets = groupOffsets(800_000, 20_000); // 800000..720000 (aged)
    const normalFreshOffsets = groupOffsets(40_000, 2_000); // 40000..32000 (fresh)
    const lowAgedOffsets = groupOffsets(700_000, 20_000); // 700000..620000 (aged)
    const lowFreshOffsets = groupOffsets(30_000, 2_000); // 30000..22000 (fresh)

    const buildGroup = (prefix, priority, offsets) =>
      offsets.map((ageOffsetMs, i) => entryAgedBy({ agentClass: `${prefix}${i}`, priority, ageOffsetMs }));

    const highAged = buildGroup('hA', 'high', highAgedOffsets);
    const highFresh = buildGroup('hF', 'high', highFreshOffsets);
    const normalAged = buildGroup('nA', 'normal', normalAgedOffsets);
    const normalFresh = buildGroup('nF', 'normal', normalFreshOffsets);
    const lowAged = buildGroup('lA', 'low', lowAgedOffsets);
    const lowFresh = buildGroup('lF', 'low', lowFreshOffsets);

    // Shuffle the on-disk insertion order deliberately (not already sorted)
    // so a pass-through/no-op implementation can't accidentally satisfy the
    // assertion by coincidence.
    const entries = [
      ...lowFresh,
      ...highFresh,
      ...normalAged,
      ...lowAged,
      ...highAged,
      ...normalFresh,
    ];

    const sorted = sortQueueEntries(entries, NOW);

    // Hand-computed expected order (see the effective-rank table above):
    //   rank 0: hA0..hA4, nA0..nA4, hF0..hF4  (each subgroup internally
    //           oldest-first by construction; hA offsets > nA offsets >
    //           hF offsets, so subgroups don't interleave)
    //   rank 1: lA0..lA4, nF0..nF4            (lA offsets > nF offsets)
    //   rank 2: lF0..lF4
    const expected = [
      'hA0', 'hA1', 'hA2', 'hA3', 'hA4',
      'nA0', 'nA1', 'nA2', 'nA3', 'nA4',
      'hF0', 'hF1', 'hF2', 'hF3', 'hF4',
      'lA0', 'lA1', 'lA2', 'lA3', 'lA4',
      'nF0', 'nF1', 'nF2', 'nF3', 'nF4',
      'lF0', 'lF1', 'lF2', 'lF3', 'lF4',
    ];

    expect(labels(sorted)).toEqual(expected);
    expect(sorted).toHaveLength(30);
  });
});

// ---------------------------------------------------------------------------
// 7. Non-regression: omitting `now` reproduces today's exact priority-then-
// FIFO order, with NO aging boost applied — locks in that
// peekQueue/dequeueItem/claimQueueEntry's existing no-`now` call sites are
// unaffected by this new optional parameter.
// ---------------------------------------------------------------------------

describe('omitting `now` entirely preserves the exact pre-aging priority-then-FIFO order — non-regression for existing no-`now` call sites', () => {
  it('sorts purely by priority desc then enqueuedAt asc, identically with zero, one, or explicit-undefined arguments', () => {
    const high = entryAgedBy({ agentClass: 'high-item', priority: 'high', ageOffsetMs: 1_000 });
    const normalOlder = entryAgedBy({ agentClass: 'normal-older', priority: 'normal', ageOffsetMs: 500_000 });
    const normalNewer = entryAgedBy({ agentClass: 'normal-newer', priority: 'normal', ageOffsetMs: 10_000 });
    const low = entryAgedBy({ agentClass: 'low-item', priority: 'low', ageOffsetMs: 2_000 });
    const entries = [low, normalNewer, high, normalOlder];

    const expected = ['high-item', 'normal-older', 'normal-newer', 'low-item'];

    expect(labels(sortQueueEntries(entries))).toEqual(expected);
    expect(labels(sortQueueEntries(entries, undefined))).toEqual(expected);
  });

  it('a VERY old entry (age far beyond AGING_THRESHOLD_MS) is NOT promoted when `now` is omitted — aging only ever activates when `now` is explicitly supplied', () => {
    const veryOldLow = entryAgedBy({
      agentClass: 'very-old-low',
      priority: 'low',
      // A whole year older than AGING_THRESHOLD_MS — if aging were ever
      // computed off a default `Date.now()` fallback, this would almost
      // certainly be promoted; the contract is that it must not be, because
      // `now` was never supplied at all.
      ageOffsetMs: AGING_THRESHOLD_MS + 365 * 24 * 60 * 60 * 1000,
    });
    const freshNormal = entryAgedBy({ agentClass: 'fresh-normal', priority: 'normal', ageOffsetMs: 0 });

    const sorted = sortQueueEntries([veryOldLow, freshNormal]);

    expect(labels(sorted)).toEqual(['fresh-normal', 'very-old-low']);
  });
});
