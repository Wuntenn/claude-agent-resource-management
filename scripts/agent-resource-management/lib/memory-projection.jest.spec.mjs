// Phase 1 — RED unit tests for the not-yet-implemented
// scripts/agent-resource-management/lib/memory-projection.mjs module.
//
// Per the Build Plan, Phase 1 owns exactly ONE pure, read-only function that
// answers "if this candidate agent class were admitted alongside the classes
// already running, what would the projected combined peak memory be, and
// does it fit inside the host's budget?" It does NOT wire into `cli.mjs`
// (that's Phase 2) and does NOT itself call `readHistory()` (that's I/O —
// the caller reads history per class first and hands this function the
// already-read result, exactly the same posture `estimateOperationCost`
// already takes towards `readHistory()`'s output — see cost-estimate.mjs's
// own header comment).
//
// Designed function signature (documented here for Phase 1's implementer,
// since lib/memory-projection.mjs does not exist yet):
//
//   computeProjectedPeakMemoryMb(
//     runningClasses: string[],          // one entry per CURRENTLY LIVE agent,
//                                         // duplicates allowed (two running
//                                         // agents of the same class are two
//                                         // entries, each priced separately)
//     candidateClass: string,            // the class asking to be admitted
//     historyByClass: Record<string, { raw: object[], summary: object | null } | undefined>,
//                                         // keyed by agent class == operationType
//                                         // (assumption #2 in
//                                         // ./cli-memory-projection.jest.spec.mjs);
//                                         // an absent key means "no history at
//                                         // all for this class" (mirrors
//                                         // readHistory()'s own `{ raw: [],
//                                         // summary: null }` default for an
//                                         // unknown operationType)
//     config?: {
//       totalHostMemoryMb?: number,          // default 16 * 1024 = 16384
//       memoryBudgetMarginMb?: number,       // default 2048 (2 GiB) — see
//                                             // note below; TBD-exact per
//                                             // ./cli-memory-projection.jest.spec.mjs
//                                             // assumption #5, this suite
//                                             // pins ONE concrete default so
//                                             // Phase 1 has something to
//                                             // build to, not a locked value
//       coldStartDefaultMemoryMb?: number,   // default 2560 (~2.5 GiB) — the
//                                             // Build Plan's own cold-start
//                                             // figure; DELIBERATELY NOT
//                                             // cost-estimate.mjs's own
//                                             // DEFAULT_ESTIMATE_CONFIG.defaultPeakMemoryMb
//                                             // (350) — that constant is a
//                                             // generic per-operation
//                                             // engineering default, whereas
//                                             // this gate wants a
//                                             // conservative admission-time
//                                             // default; the new function
//                                             // threads its OWN default in
//                                             // via `estimateOperationCost`'s
//                                             // existing `options.defaultPeakMemoryMb`
//                                             // override rather than
//                                             // duplicating 350 anywhere
//       minCleanRawSamplesForEmpirical?: number, // passed straight through to
//                                                 // estimateOperationCost;
//                                                 // default is that
//                                                 // function's own default (3)
//     },
//   ): {
//     totalProjectedMemoryMb: number,
//     budgetMemoryMb: number,
//     withinBudget: boolean,
//     contributions: Array<{ agentClass: string, estimatedPeakMemoryMb: number }>,
//     // one entry per element of [...runningClasses, candidateClass], IN
//     // THAT ORDER, so `totalProjectedMemoryMb` is always independently
//     // verifiable as the sum of `contributions[].estimatedPeakMemoryMb`
//   }
//
// Cold-start tie-break rule this suite locks in (Build Plan scenario 4 — the
// specific, non-obvious rule, stated precisely so Phase 1 doesn't invent a
// different one):
//
//   A class with ZERO history (no store entry, or a store entry whose clean
//   raw sample count is 0 AND whose summary is null/non-finite) is NOT
//   simply handed the flat `coldStartDefaultMemoryMb` UNCONDITIONALLY.
//   Instead: if ANY OTHER class among `[...runningClasses, candidateClass]`
//   DOES have usable history, the history-less class's estimate is the
//   MAXIMUM of those other classes' own `estimateOperationCost(...)
//   .estimatedPeakMemoryMb` values (i.e. "assume the history-less class costs
//   at least as much as the most expensive class we actually have evidence
//   for" — the conservative, admission-safe reading). Only when NO class in
//   the whole set has any usable history at all does the flat
//   `coldStartDefaultMemoryMb` apply.

import { estimateOperationCost } from './cost-estimate.mjs';
import { computeProjectedPeakMemoryMb } from './memory-projection.mjs';

const MIB_PER_GIB = 1024;
const DEFAULT_TOTAL_HOST_MEMORY_MB = 16 * MIB_PER_GIB; // 16384
const DEFAULT_MEMORY_BUDGET_MARGIN_MB = 2048; // 2 GiB — this suite's pinned default, see header note
const DEFAULT_COLD_START_MEMORY_MB = 2560; // ~2.5 GiB per Build Plan

/** Builds a `readHistory()`-shaped record with N clean raw peaks (no summary). */
function rawHistory(peaksMb) {
  return { raw: peaksMb.map((peakMemoryMb) => ({ peakMemoryMb })), summary: null };
}

/** Builds a `readHistory()`-shaped record backed only by a populated summary. */
function summaryHistory(p90PeakMemoryMb, overrides = {}) {
  return {
    raw: [],
    summary: {
      count: 10,
      meanPeakMemoryMb: p90PeakMemoryMb * 0.8,
      maxPeakMemoryMb: p90PeakMemoryMb * 1.2,
      p90PeakMemoryMb,
      ...overrides,
    },
  };
}

describe('computeProjectedPeakMemoryMb', () => {
  test('1. sums p90/estimated peaks across two running classes plus the candidate class', () => {
    const historyByClass = {
      'class-a': rawHistory([1000, 1000, 1000]), // median 1000, >=3 clean samples -> empirical
      'class-b': rawHistory([2000, 2000, 2000]), // median 2000
      'class-c': rawHistory([500, 500, 500]), // candidate, median 500
    };

    const result = computeProjectedPeakMemoryMb(['class-a', 'class-b'], 'class-c', historyByClass);

    expect(result.totalProjectedMemoryMb).toBe(3500);
    expect(result.contributions).toEqual([
      { agentClass: 'class-a', estimatedPeakMemoryMb: 1000 },
      { agentClass: 'class-b', estimatedPeakMemoryMb: 2000 },
      { agentClass: 'class-c', estimatedPeakMemoryMb: 500 },
    ]);
    // Independently verifiable: total is always the sum of contributions.
    const summed = result.contributions.reduce((sum, entry) => sum + entry.estimatedPeakMemoryMb, 0);
    expect(result.totalProjectedMemoryMb).toBe(summed);
  });

  test('2. falls back to history.summary.p90PeakMemoryMb when the clean raw sample count is below the empirical threshold', () => {
    // Only 2 clean raw samples — below estimateOperationCost's own
    // DEFAULT_MIN_CLEAN_RAW_SAMPLES_FOR_EMPIRICAL (3) — so the populated
    // summary must win, exactly as estimateOperationCost documents.
    const historyByClass = {
      'thin-history-class': {
        raw: [{ peakMemoryMb: 100 }, { peakMemoryMb: 200 }],
        summary: {
          count: 5,
          meanPeakMemoryMb: 120,
          maxPeakMemoryMb: 300,
          p90PeakMemoryMb: 1500,
        },
      },
    };

    const result = computeProjectedPeakMemoryMb([], 'thin-history-class', historyByClass);

    expect(result.totalProjectedMemoryMb).toBe(1500);
    expect(result.contributions).toEqual([{ agentClass: 'thin-history-class', estimatedPeakMemoryMb: 1500 }]);
  });

  test('3. cold start: no class anywhere has any history -> hard-coded ~2.5 GiB default per class', () => {
    const result = computeProjectedPeakMemoryMb(['no-history-a', 'no-history-b'], 'no-history-c', {});

    expect(result.contributions).toEqual([
      { agentClass: 'no-history-a', estimatedPeakMemoryMb: DEFAULT_COLD_START_MEMORY_MB },
      { agentClass: 'no-history-b', estimatedPeakMemoryMb: DEFAULT_COLD_START_MEMORY_MB },
      { agentClass: 'no-history-c', estimatedPeakMemoryMb: DEFAULT_COLD_START_MEMORY_MB },
    ]);
    expect(result.totalProjectedMemoryMb).toBe(DEFAULT_COLD_START_MEMORY_MB * 3);
  });

  test('4a. cold-start tie-break: the CANDIDATE class has no history -> defaults to the max estimated peak among classes that DO have history', () => {
    const historyByClass = {
      'known-a': summaryHistory(4096), // estimated peak 4096, the higher of the two known classes
      'known-b': rawHistory([1000, 2000, 3000]), // median 2000
      // 'history-less-candidate' has no entry at all.
    };

    const result = computeProjectedPeakMemoryMb(['known-a', 'known-b'], 'history-less-candidate', historyByClass);

    expect(result.contributions).toEqual([
      { agentClass: 'known-a', estimatedPeakMemoryMb: 4096 },
      { agentClass: 'known-b', estimatedPeakMemoryMb: 2000 },
      // NOT DEFAULT_COLD_START_MEMORY_MB (2560) — the specific tie-break
      // rule: max(4096, 2000) == 4096, the most-conservative KNOWN class.
      { agentClass: 'history-less-candidate', estimatedPeakMemoryMb: 4096 },
    ]);
    expect(result.totalProjectedMemoryMb).toBe(4096 + 2000 + 4096);
  });

  test('4b. cold-start tie-break: a RUNNING class has no history -> same max-of-known-classes rule applies, not just to the candidate', () => {
    const historyByClass = {
      // 'history-less-runner' has no entry at all.
      'known-candidate': summaryHistory(7000),
    };

    const result = computeProjectedPeakMemoryMb(['history-less-runner'], 'known-candidate', historyByClass);

    expect(result.contributions).toEqual([
      { agentClass: 'history-less-runner', estimatedPeakMemoryMb: 7000 },
      { agentClass: 'known-candidate', estimatedPeakMemoryMb: 7000 },
    ]);
  });

  test('4c. cold-start tie-break: a genuinely new/heavy class among only CHEAP known running classes is priced at the cold-start floor, not the cheap max (review, Medium finding)', () => {
    const historyByClass = {
      // Both known running classes are cheap/well-understood — well below
      // DEFAULT_COLD_START_MEMORY_MB (2560).
      'cheap-helper-a': summaryHistory(512),
      'cheap-helper-b': rawHistory([600, 600, 600]),
      // 'new-heavy-candidate' has no entry at all.
    };

    const result = computeProjectedPeakMemoryMb(
      ['cheap-helper-a', 'cheap-helper-b'],
      'new-heavy-candidate',
      historyByClass,
    );

    expect(result.contributions).toEqual([
      { agentClass: 'cheap-helper-a', estimatedPeakMemoryMb: 512 },
      { agentClass: 'cheap-helper-b', estimatedPeakMemoryMb: 600 },
      // NOT max(512, 600) == 600 — the naive "max of what's running right
      // now" reading would under-price this unknown class far below the
      // conservative floor. Must be at least DEFAULT_COLD_START_MEMORY_MB.
      { agentClass: 'new-heavy-candidate', estimatedPeakMemoryMb: DEFAULT_COLD_START_MEMORY_MB },
    ]);
    expect(result.totalProjectedMemoryMb).toBe(512 + 600 + DEFAULT_COLD_START_MEMORY_MB);
  });

  test('5. excludes crashed/non-finite observations from the sum by delegating to estimateOperationCost, not reimplementing exclusion', () => {
    const rawWithNoise = [
      { peakMemoryMb: 1000 },
      { peakMemoryMb: 2000 },
      { peakMemoryMb: 9999, crashed: true }, // must be excluded
      { peakMemoryMb: 1500 },
      { peakMemoryMb: undefined }, // non-finite, must be excluded
    ];
    const historyByClass = { 'noisy-class': { raw: rawWithNoise, summary: null } };

    const result = computeProjectedPeakMemoryMb([], 'noisy-class', historyByClass);

    // The independently-computed expectation, via the SAME production
    // function this one must delegate to (not a hand-rolled reimplementation
    // of the exclusion rule) — proves the two never diverge.
    const expected = estimateOperationCost('noisy-class', historyByClass['noisy-class'], {
      defaultPeakMemoryMb: DEFAULT_COLD_START_MEMORY_MB,
    });

    expect(result.totalProjectedMemoryMb).toBe(expected.estimatedPeakMemoryMb);
    // Concretely: median([1000, 2000, 1500]) == 1500, NOT a figure inflated
    // by the crashed 9999 sample (e.g. NOT median([1000,1500,2000,9999]) == 1750,
    // and NOT mean-with-crash-included == 3624.75).
    expect(result.totalProjectedMemoryMb).toBe(1500);
  });

  test('6. cross-class isolation: history recorded for one class never leaks into a lexically-similar class\'s estimate', () => {
    // Deliberately similar/overlapping names (prefix collision risk) — a
    // naive lookup (substring match, first-key-found, or an accidental merge
    // of both entries) would blend these; correct behaviour keys strictly by
    // exact agentClass/operationType string.
    const historyByClass = {
      'builder': rawHistory([500, 500, 500]), // median 500
      'typescript-implementer': rawHistory([9000, 9000, 9000]), // median 9000
    };

    const result = computeProjectedPeakMemoryMb(['builder'], 'typescript-implementer', historyByClass);

    expect(result.contributions).toEqual([
      { agentClass: 'builder', estimatedPeakMemoryMb: 500 },
      { agentClass: 'typescript-implementer', estimatedPeakMemoryMb: 9000 },
    ]);
    expect(result.totalProjectedMemoryMb).toBe(9500);
  });

  test('7a. the memory budget defaults to 16 GiB minus this suite\'s pinned default margin (2 GiB)', () => {
    const result = computeProjectedPeakMemoryMb([], 'any-class', {});

    expect(result.budgetMemoryMb).toBe(DEFAULT_TOTAL_HOST_MEMORY_MB - DEFAULT_MEMORY_BUDGET_MARGIN_MB);
  });

  test('7b. the memory budget margin is overridable via config, and the raw 16 GiB host figure is overridable too', () => {
    const overriddenMarginResult = computeProjectedPeakMemoryMb([], 'any-class', {}, { memoryBudgetMarginMb: 4096 });
    expect(overriddenMarginResult.budgetMemoryMb).toBe(DEFAULT_TOTAL_HOST_MEMORY_MB - 4096);

    const overriddenHostResult = computeProjectedPeakMemoryMb(
      [],
      'any-class',
      {},
      { totalHostMemoryMb: 8192, memoryBudgetMarginMb: 1024 },
    );
    expect(overriddenHostResult.budgetMemoryMb).toBe(8192 - 1024);
  });

  test('7c. withinBudget reflects whether the projected total fits inside the (possibly overridden) budget', () => {
    const historyByClass = { 'huge-class': summaryHistory(20 * MIB_PER_GIB) }; // 20 GiB, comfortably over budget

    const overBudget = computeProjectedPeakMemoryMb([], 'huge-class', historyByClass);
    expect(overBudget.withinBudget).toBe(false);

    const underBudget = computeProjectedPeakMemoryMb([], 'huge-class', historyByClass, {
      totalHostMemoryMb: 64 * MIB_PER_GIB,
      memoryBudgetMarginMb: 0,
    });
    expect(underBudget.withinBudget).toBe(true);
  });

  test('8. malformed/missing legacy raw observations (no usable peakMemoryMb at all) do not crash, and fall through to the cold-start tie-break rule', () => {
    const historyByClass = {
      'known-class': rawHistory([3000, 3000, 3000]), // median 3000, the only usable evidence in the set
      'legacy-malformed-class': {
        raw: [{}, { foo: 'bar' }, { peakMemoryMb: undefined }, { peakMemoryMb: Number.NaN }],
        summary: null,
      },
    };

    expect(() =>
      computeProjectedPeakMemoryMb(['known-class'], 'legacy-malformed-class', historyByClass),
    ).not.toThrow();

    const result = computeProjectedPeakMemoryMb(['known-class'], 'legacy-malformed-class', historyByClass);

    expect(result.contributions).toEqual([
      { agentClass: 'known-class', estimatedPeakMemoryMb: 3000 },
      // Zero usable raw samples and no summary == "no history" for this
      // class, per this suite's tie-break rule (test 4a/4b) — NOT
      // DEFAULT_COLD_START_MEMORY_MB, since 'known-class' DOES have usable
      // history to fall back on.
      { agentClass: 'legacy-malformed-class', estimatedPeakMemoryMb: 3000 },
    ]);
  });
});

// Phase 2 — RED tests for a non-agent host-process-footprint term.
//
// ASSUMPTION FOR THE IMPLEMENTER: these tests assume the new signal is threaded
// through as a fifth field on the existing `config` object —
// `config.nonAgentFootprintMb` — alongside `totalHostMemoryMb`,
// `memoryBudgetMarginMb`, and `coldStartDefaultMemoryMb` (all four existing
// config fields already live on one object, so a fifth field is the natural
// fit over a new positional argument). It is expected to surface as its own
// entry in the returned `contributions` array, under the agentClass label
// `NON_AGENT_FOOTPRINT_CONTRIBUTION_LABEL` below (`'non-agent-host-footprint'`),
// appended AFTER the `[...runningClasses, candidateClass]` entries — NOT
// folded into any existing agent-class entry. If the implementer picks a
// different config field name or label, tell this test author so the suite
// can be updated to match rather than silently diverging.
describe('computeProjectedPeakMemoryMb — non-agent host-process footprint', () => {
  const NON_AGENT_FOOTPRINT_CONTRIBUTION_LABEL = 'non-agent-host-footprint';

  test('1. a large non-agent footprint reduces the effective budget and can flip withinBudget from true to false', () => {
    // Verified against the CURRENT (Phase 1) unmodified function: candidate
    // alone has no history anywhere in the set, so it prices at the flat
    // cold-start default (2560), well inside the default budget
    // (16384 - 2048 = 14336) -> withinBudget: true.
    const historyByClass = {};

    const withoutFootprint = computeProjectedPeakMemoryMb([], 'solo-candidate', historyByClass);
    expect(withoutFootprint.totalProjectedMemoryMb).toBe(DEFAULT_COLD_START_MEMORY_MB); // 2560
    expect(withoutFootprint.withinBudget).toBe(true);

    // Adding an 8000MB non-agent footprint pushes the combined total
    // (2560 + 8000 = 10560) still under budget on its own — but the point of
    // this scenario is the flip, so pick a footprint that actually crosses
    // the 14336 budget line: 2560 + 12000 = 14560 > 14336.
    const withFootprint = computeProjectedPeakMemoryMb([], 'solo-candidate', historyByClass, {
      nonAgentFootprintMb: 12000,
    });

    expect(withFootprint.withinBudget).toBe(false);
    expect(withFootprint.totalProjectedMemoryMb).toBe(DEFAULT_COLD_START_MEMORY_MB + 12000);
  });

  test('2a. backward compatibility: omitting nonAgentFootprintMb entirely reproduces existing scenario 1 (two running classes + candidate) exactly', () => {
    const historyByClass = {
      'class-a': rawHistory([1000, 1000, 1000]),
      'class-b': rawHistory([2000, 2000, 2000]),
      'class-c': rawHistory([500, 500, 500]),
    };

    const baseline = computeProjectedPeakMemoryMb(['class-a', 'class-b'], 'class-c', historyByClass);
    const withExplicitZero = computeProjectedPeakMemoryMb(['class-a', 'class-b'], 'class-c', historyByClass, {
      nonAgentFootprintMb: 0,
    });

    // Total/budget outcome must be unaffected by an explicit-zero footprint —
    // deliberately not asserting on `contributions` array length/shape here,
    // since whether a zero-valued footprint entry is included or omitted is
    // an implementation choice this suite doesn't pin (see test 3 for the
    // shape assertion when the footprint is actually non-zero).
    expect(withExplicitZero.totalProjectedMemoryMb).toBe(baseline.totalProjectedMemoryMb);
    expect(withExplicitZero.withinBudget).toBe(baseline.withinBudget);
  });

  test('2b. backward compatibility: omitting nonAgentFootprintMb reproduces existing cold-start scenario 3 exactly', () => {
    const baseline = computeProjectedPeakMemoryMb(['no-history-a', 'no-history-b'], 'no-history-c', {});
    const withoutFootprintConfig = computeProjectedPeakMemoryMb(['no-history-a', 'no-history-b'], 'no-history-c', {}, {});

    expect(withoutFootprintConfig.totalProjectedMemoryMb).toBe(baseline.totalProjectedMemoryMb);
    expect(withoutFootprintConfig.withinBudget).toBe(baseline.withinBudget);
  });

  test('3. the non-agent footprint appears as its own named entry in contributions, not folded into an agent-class entry', () => {
    const historyByClass = { 'class-a': rawHistory([1000, 1000, 1000]) };

    const result = computeProjectedPeakMemoryMb([], 'class-a', historyByClass, { nonAgentFootprintMb: 3000 });

    // The agent-class entry is untouched by the footprint.
    expect(result.contributions).toContainEqual({ agentClass: 'class-a', estimatedPeakMemoryMb: 1000 });
    // A distinct, separately-labelled entry carries the footprint.
    expect(result.contributions).toContainEqual({
      agentClass: NON_AGENT_FOOTPRINT_CONTRIBUTION_LABEL,
      estimatedPeakMemoryMb: 3000,
    });
    expect(result.contributions).toHaveLength(2);
    expect(result.totalProjectedMemoryMb).toBe(1000 + 3000);
  });

  test('4. a negative non-agent footprint (racing-snapshot artefact) is floored at 0 and produces NO contributions entry, not thrown or subtracted', () => {
    const historyByClass = { 'class-a': rawHistory([1000, 1000, 1000]) };

    expect(() =>
      computeProjectedPeakMemoryMb([], 'class-a', historyByClass, { nonAgentFootprintMb: -500 }),
    ).not.toThrow();

    const result = computeProjectedPeakMemoryMb([], 'class-a', historyByClass, { nonAgentFootprintMb: -500 });

    // A floored-to-0 footprint carries no budget impact, so it collapses to
    // "no known footprint" — no separate entry, same as an omitted key.
    expect(result.contributions.some((c) => c.agentClass === NON_AGENT_FOOTPRINT_CONTRIBUTION_LABEL)).toBe(false);
    // Must not subtract from the agent-class contribution either.
    expect(result.contributions).toContainEqual({ agentClass: 'class-a', estimatedPeakMemoryMb: 1000 });
    expect(result.contributions).toHaveLength(1);
    expect(result.totalProjectedMemoryMb).toBe(1000);
  });

  test('5. an unavailable non-agent footprint (undefined or null — stale/unreadable snapshot) degrades to a 0-impact no-entry outcome, not a throw', () => {
    const historyByClass = { 'class-a': rawHistory([1000, 1000, 1000]) };

    const withUndefined = computeProjectedPeakMemoryMb([], 'class-a', historyByClass, {
      nonAgentFootprintMb: undefined,
    });
    const withNull = computeProjectedPeakMemoryMb([], 'class-a', historyByClass, { nonAgentFootprintMb: null });

    // `undefined`/`null` are equivalent to an omitted key: no separate
    // footprint entry, and the total is unaffected.
    expect(withUndefined.contributions.some((c) => c.agentClass === NON_AGENT_FOOTPRINT_CONTRIBUTION_LABEL)).toBe(
      false,
    );
    expect(withNull.contributions.some((c) => c.agentClass === NON_AGENT_FOOTPRINT_CONTRIBUTION_LABEL)).toBe(false);
    expect(withUndefined.totalProjectedMemoryMb).toBe(1000);
    expect(withNull.totalProjectedMemoryMb).toBe(1000);
  });

  test('6. omission, explicit undefined (incl. via spread), explicit null, and explicit zero all produce an identical result with no footprint entry', () => {
    const historyByClass = { 'class-a': rawHistory([1000, 1000, 1000]) };
    const base = { totalHostMemoryMb: 16384, memoryBudgetMarginMb: 2048 };

    const omitted = computeProjectedPeakMemoryMb([], 'class-a', historyByClass, { ...base });
    // Simulates a caller building config via the common spread idiom with a
    // value that happens to resolve to `undefined` at runtime — this must
    // NOT silently append a spurious zero-valued entry that the
    // `omitted` caller above never gets.
    const maybeUndefinedValue = undefined;
    const spreadUndefined = computeProjectedPeakMemoryMb([], 'class-a', historyByClass, {
      ...base,
      nonAgentFootprintMb: maybeUndefinedValue,
    });
    const explicitNull = computeProjectedPeakMemoryMb([], 'class-a', historyByClass, {
      ...base,
      nonAgentFootprintMb: null,
    });
    const explicitZero = computeProjectedPeakMemoryMb([], 'class-a', historyByClass, {
      ...base,
      nonAgentFootprintMb: 0,
    });

    for (const result of [omitted, spreadUndefined, explicitNull, explicitZero]) {
      expect(result.contributions).toEqual([{ agentClass: 'class-a', estimatedPeakMemoryMb: 1000 }]);
      expect(result.totalProjectedMemoryMb).toBe(1000);
    }
  });
});
