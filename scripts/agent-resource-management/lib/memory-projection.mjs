// Phase 1 — per-agent-class projected-peak memory admission gate.
//
// Pure function: given the classes of agents currently running, a candidate
// class asking to be admitted, and per-class history (exactly the shape
// `readHistory()` in `./history.mjs` returns, keyed by agent class), answers
// "what would the projected combined peak memory be if the candidate were
// admitted, and does it fit inside the host's budget?"
//
// Delegates all per-class cost estimation to `estimateOperationCost`
// (`./cost-estimate.mjs`) — this module owns only the summation, the
// cold-start tie-break rule, and the budget arithmetic. No I/O: the caller is
// responsible for calling `readHistory()` per class before calling this
// function (same posture `estimateOperationCost` already takes towards
// `readHistory()`'s output).

import { estimateOperationCost } from './cost-estimate.mjs';

const MIB_PER_GIB = 1024;

// Phase 2 — label for the synthetic contribution entry that carries
// non-agent host-process memory footprint (e.g. the Claude Code harness
// process itself, background OS processes) alongside the per-agent-class
// entries. Only appears when the caller opts in via `config.nonAgentFootprintMb`.
const NON_AGENT_FOOTPRINT_CONTRIBUTION_LABEL = 'non-agent-host-footprint';

const DEFAULT_PROJECTION_CONFIG = {
  totalHostMemoryMb: 16 * MIB_PER_GIB, // 16384 — this host's fixed RAM
  memoryBudgetMarginMb: 2048, // 2 GiB — headroom reserved for the OS/other processes
  coldStartDefaultMemoryMb: 2560, // ~2.5 GiB — conservative admission-time default for a
  // class with zero usable history anywhere in the current computation
};

/**
 * Computes the projected combined peak memory of every currently-running
 * agent plus a candidate class asking to be admitted, and whether that
 * projection fits inside the host's memory budget.
 *
 * Cold-start tie-break: a class with no usable history — `estimateOperationCost`
 * (`./cost-estimate.mjs`) reports it as anything other than `confidence:
 * 'empirical'`, i.e. no store entry, or a store entry whose clean raw sample
 * count is BELOW `minCleanRawSamplesForEmpirical` (default 3, not merely `0`)
 * with no usable summary to fall back on either — is NOT unconditionally
 * handed `coldStartDefaultMemoryMb`,
 * nor is it unconditionally handed the max of whatever OTHER classes happen
 * to have usable history in this computation. It is priced at
 * `Math.max(maxOfKnownClasses, coldStartDefaultMemoryMb)` — i.e. the GREATER
 * of (a) the maximum `estimateOperationCost(...).estimatedPeakMemoryMb` among
 * other classes in `[...runningClasses, candidateClass]` that DO have usable
 * history, and (b) the flat `coldStartDefaultMemoryMb` floor. Only when no
 * class in the whole set has any usable history does (a) drop out and the
 * flat `coldStartDefaultMemoryMb` alone applies.
 *
 * Why the floor is never dropped below `coldStartDefaultMemoryMb`, even when
 * other known classes are cheaper: a genuinely new/heavy class arriving
 * alongside only lightweight, well-understood running classes (e.g. cheap
 * helper agents) must never be priced BELOW the conservative cold-start
 * default just because everything else currently running happens to be
 * cheap — "conservative if we don't know" means at least the floor, not
 * "whatever the current mix of known classes happens to suggest". An
 * earlier version of this rule took `maxKnownPeakMemoryMb ?? coldStartDefaultMemoryMb`
 * (max-of-known-or-default, i.e. an either/or), which let an unlucky mix of
 * cheap running classes under-price a heavy unknown class below the floor.
 *
 * @param {string[]} runningClasses One entry per currently-live agent;
 *   duplicates allowed (two running agents of the same class are two
 *   entries, each priced separately).
 * @param {string} candidateClass The class asking to be admitted.
 * @param {Record<string, { raw: object[], summary: object | null } | undefined>} historyByClass
 *   Keyed by agent class (== operationType). An absent key means "no history
 *   at all for this class".
 * @param {{
 *   totalHostMemoryMb?: number,
 *   memoryBudgetMarginMb?: number,
 *   coldStartDefaultMemoryMb?: number,
 *   minCleanRawSamplesForEmpirical?: number,
 *   nonAgentFootprintMb?: number | null,
 * }} [config] `nonAgentFootprintMb`, when it resolves to a finite value
 *   greater than `0`, adds a distinct `contributions` entry labelled
 *   `'non-agent-host-footprint'` for non-agent host-process memory (e.g. the
 *   harness process itself). Omitting the key, or passing `undefined`,
 *   `null`, `0`, a negative number, or a non-finite value, are all
 *   equivalent — no entry is added and `totalProjectedMemoryMb`/`withinBudget`
 *   are unchanged. This makes the pre-existing call shape (key never passed) and
 *   a spread-built config that happens to carry an empty/zero value
 *   (`{ ...base, nonAgentFootprintMb: maybeUndefinedValue }`) behave
 *   identically — presence-vs-value ambiguity is intentionally collapsed.
 * @returns {{
 *   totalProjectedMemoryMb: number,
 *   budgetMemoryMb: number,
 *   withinBudget: boolean,
 *   contributions: Array<{ agentClass: string, estimatedPeakMemoryMb: number }>,
 * }}
 */
export function computeProjectedPeakMemoryMb(runningClasses, candidateClass, historyByClass, config = {}) {
  const totalHostMemoryMb = config.totalHostMemoryMb ?? DEFAULT_PROJECTION_CONFIG.totalHostMemoryMb;
  const memoryBudgetMarginMb = config.memoryBudgetMarginMb ?? DEFAULT_PROJECTION_CONFIG.memoryBudgetMarginMb;
  const coldStartDefaultMemoryMb = config.coldStartDefaultMemoryMb ?? DEFAULT_PROJECTION_CONFIG.coldStartDefaultMemoryMb;

  const estimateOptions = { defaultPeakMemoryMb: coldStartDefaultMemoryMb };
  if (config.minCleanRawSamplesForEmpirical !== undefined) {
    estimateOptions.minCleanRawSamplesForEmpirical = config.minCleanRawSamplesForEmpirical;
  }

  const agentClasses = [...runningClasses, candidateClass];

  // One estimate per class, in order — `estimateOperationCost` never throws
  // on malformed/legacy history (its own exclusion logic handles non-finite
  // and crashed observations), so this loop is safe even for a class whose
  // `historyByClass` entry is itself malformed (e.g. `{}` or missing
  // `raw`/`summary` keys).
  const estimates = agentClasses.map((agentClass) => ({
    agentClass,
    ...estimateOperationCost(agentClass, historyByClass?.[agentClass], estimateOptions),
  }));

  const knownPeaks = estimates
    .filter((estimate) => estimate.confidence === 'empirical')
    .map((estimate) => estimate.estimatedPeakMemoryMb);
  const maxKnownPeakMemoryMb = knownPeaks.length > 0 ? Math.max(...knownPeaks) : null;

  const contributions = estimates.map(({ agentClass, estimatedPeakMemoryMb, confidence }) => {
    if (confidence === 'empirical') {
      return { agentClass, estimatedPeakMemoryMb };
    }
    // No usable history for this class: price at the GREATER of the
    // most-expensive known class in this computation and the flat
    // cold-start floor — never dragged below the floor just because every
    // currently-known class happens to be cheap (see this function's doc
    // comment).
    const coldStartEstimateMb =
      maxKnownPeakMemoryMb === null ? coldStartDefaultMemoryMb : Math.max(maxKnownPeakMemoryMb, coldStartDefaultMemoryMb);
    return { agentClass, estimatedPeakMemoryMb: coldStartEstimateMb };
  });

  // Non-agent host-process footprint: only participates when
  // it resolves to a genuinely positive, finite value — omitted, `undefined`,
  // `null`, `0`, negative, and non-finite all collapse to the same "no known
  // footprint" outcome (no entry, no budget impact). This keeps every
  // pre-existing call site (which never passes `nonAgentFootprintMb`) a
  // byte-identical no-op, and keeps a spread-built config that happens to
  // carry an empty value from silently appending a spurious zero-valued
  // entry that an omitted-key caller would never get.
  const rawFootprintMb = config.nonAgentFootprintMb;
  const footprintMb = typeof rawFootprintMb === 'number' && Number.isFinite(rawFootprintMb) ? Math.max(rawFootprintMb, 0) : 0;
  const allContributions =
    footprintMb > 0
      ? [...contributions, { agentClass: NON_AGENT_FOOTPRINT_CONTRIBUTION_LABEL, estimatedPeakMemoryMb: footprintMb }]
      : contributions;

  const totalProjectedMemoryMb = allContributions.reduce((sum, { estimatedPeakMemoryMb }) => sum + estimatedPeakMemoryMb, 0);
  const budgetMemoryMb = totalHostMemoryMb - memoryBudgetMarginMb;

  return {
    totalProjectedMemoryMb,
    budgetMemoryMb,
    withinBudget: totalProjectedMemoryMb <= budgetMemoryMb,
    contributions: allContributions,
  };
}
