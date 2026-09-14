// Phase 1 — machine resource recon & sampling.
//
// Pure parse functions for macOS `vm_stat` / `sysctl vm.swapusage` / `df` /
// `ps -Ao pid,ppid,comm`-style output, plus `takeSample(collect)`, the
// composed sampling entrypoint.
//
// IMPORTANT: nothing in this file shells out. Every parse function takes a
// raw string (or a pair of pre-collected samples) and returns structured
// data. `takeSample(collect)` calls the injected `collect` callback exactly
// once and never spawns a process itself — the real `collect()` that shells
// out to vm_stat/sysctl/df/ps belongs to a later, thin CLI layer, not here.

const BYTES_PER_MB = 1024 * 1024;

/**
 * An explicit, distinct "stale/unknown" marker for memory readings. Never
 * shaped like a healthy (GREEN-compatible) reading, so downstream
 * classifiers can't mistake it for a real sample.
 */
const staleMemoryMarker = () => ({
  pressureLevel: null,
  swapUsedMb: null,
  compressedMb: null,
});

/** Explicit "stale/unknown" marker for disk readings, same rationale. */
const staleDiskMarker = () => ({
  freeDiskGb: null,
  declineRateGbPerHour: null,
});

// ---------------------------------------------------------------------------
// parseVmStat
// ---------------------------------------------------------------------------

/**
 * Parses macOS `vm_stat` output into a coarse memory-pressure level (1 =
 * healthy, higher = more pressured) plus the compressor's footprint in MB.
 *
 * IMPORTANT — `pressureLevel` here is a **fallback/test heuristic only**,
 * NOT what the real CLI uses to judge memory pressure in production. The
 * free/total page ratio this derives from is naturally low on macOS even on
 * an idle machine (the kernel deliberately keeps reclaimable file-backed
 * cache populated rather than leaving pages free), so this heuristic reads
 * AMBER/RED on a healthy host. `cli.mjs`'s `realCollect()` instead reads
 * `pressureLevel` from `sysctl -n kern.memorystatus_vm_pressure_level` (see
 * `parsePressureLevel` below) — the kernel's own production-accurate
 * pressure signal. This function's `pressureLevel` output remains useful for
 * fixture-driven unit tests of the classification pipeline, and is why
 * `takeSample`'s composition still treats it as a valid input shape, but it
 * must never be wired into the real collection path again.
 *
 * Each expected `vm_stat` field label is matched independently: a label
 * that isn't found yields `NaN` for that specific input, not a fabricated
 * `0`. That NaN propagates arithmetically — `pressureLevel` (which is
 * derived from all six page counts) and `compressedMb` (derived from the
 * compressor page count) each go `NaN` if any of their own inputs is
 * missing. This naturally covers both a fully-garbled input (every field
 * missing → both outputs NaN) and a partially-truncated one (only the
 * affected output goes NaN) — `takeSample()`'s composition step treats a
 * `NaN` field as unparseable and degrades to `stale: true`.
 *
 * Also extracts `freeRamMb` (Phase 5,  Part A wiring;
 * extended it to also fold in "Pages purgeable") from the "Pages free" and
 * "Pages purgeable" counts combined, reusing the SAME `pageSizeBytes` this
 * function already derives (from the "page size of N bytes" header line,
 * falling back to Apple Silicon's 16384 default) — not a second,
 * independently-hardcoded page-size constant. Purgeable pages are memory the
 * kernel can reclaim instantly at zero cost, so they count as real headroom
 * alongside free pages. Follows this function's existing "don't fabricate"
 * convention: a missing/unparseable "Pages free" OR "Pages purgeable" label
 * yields `NaN` for `freeRamMb`, not a fabricated `0`.
 *
 * Also extracts `compressions`/`decompressions` — the compressor's
 * cumulative "Compressions:"/"Decompressions:" counters — via the SAME
 * `field()` helper as every other label here, so an older-macOS sample
 * missing either line degrades that one field to `NaN` independently,
 * never throws, and never perturbs `pressureLevel`, `compressedMb`, or
 * `freeRamMb`. A reading of exactly `0` is valid and distinct from `NaN`.
 *
 * @param {string} raw
 * @returns {{ pressureLevel: number, compressedMb: number, freeRamMb: number, compressions: number, decompressions: number }}
 */
export function parseVmStat(raw) {
  const text = String(raw ?? '');

  const pageSizeMatch = text.match(/page size of\s+(\d+)\s+bytes/i);
  // No "don't fabricate" placeholder page size is truly safe, but 16384 is
  // Apple Silicon's actual page size and every machine this skill targets
  // (Mac mini M4) is Apple Silicon — this is a materially better fallback
  // than the previous 4096 (Intel-era) default, which silently understated
  // `compressedMb` by 4x on the real target hardware.
  const pageSizeBytes = pageSizeMatch ? Number(pageSizeMatch[1]) : 16384;

  const field = (label) => {
    // Anchored to (optional leading whitespace +) the start of a line —
    // not just "label appears somewhere in the text" — so a label that is
    // itself a substring of another label (e.g. "Compressions" inside
    // "Decompressions") can never match the wrong line.
    const pattern = new RegExp(`^\\s*${label}\\s*:\\s*([\\d,]+)\\.?`, 'im');
    const match = text.match(pattern);
    return match ? Number(match[1].replace(/,/g, '')) : NaN;
  };

  const freePages = field('Pages\\s+free');
  const activePages = field('Pages\\s+active');
  const inactivePages = field('Pages\\s+inactive');
  const speculativePages = field('Pages\\s+speculative');
  const wiredPages = field('Pages\\s+wired\\s+down');
  const occupiedCompressorPages = field('Pages\\s+occupied\\s+by\\s+compressor');
  const purgeablePages = field('Pages\\s+purgeable');
  const compressions = field('Compressions');
  const decompressions = field('Decompressions');

  const totalPages =
    freePages + activePages + inactivePages + speculativePages + wiredPages + occupiedCompressorPages;
  const freeRatio = Number.isNaN(totalPages) ? NaN : totalPages > 0 ? freePages / totalPages : 0;

  let pressureLevel;
  if (Number.isNaN(freeRatio)) {
    pressureLevel = NaN;
  } else if (freeRatio >= 0.25) {
    pressureLevel = 1;
  } else if (freeRatio >= 0.15) {
    pressureLevel = 2;
  } else if (freeRatio >= 0.08) {
    pressureLevel = 3;
  } else {
    pressureLevel = 4;
  }

  const compressedMb = Number.isNaN(occupiedCompressorPages)
    ? NaN
    : (occupiedCompressorPages * pageSizeBytes) / BYTES_PER_MB;

  // Why `freePages + purgeablePages` is not a double-count (see, resolving
  // a High-severity finding raised on PR and merged before it was answered
  // in-thread — recorded here so a future reviewer doesn't re-derive this):
  // "Pages free" and "Pages purgeable" are disjoint kernel counters by
  // definition — a page can't simultaneously be unallocated-free and
  // allocated-but-purgeable — so summing them is safe. Purgeable
  // (VM_PURGABLE_VOLATILE) pages are reclaimable by the kernel at zero
  // write-back cost regardless of which page queue (active/inactive) they're
  // nominally filed under, so crediting them as available headroom is correct.
  // XNU's vm_resident.c confirms purgeable pages ARE also tallied within
  // active_count/inactive_count — but freeRamMb never sums activePages or
  // inactivePages here, only freePages + purgeablePages, and those two are
  // mutually exclusive, so there's no double-count within this formula.
  // (Contrast with speculative_count, which vm_statistics.h documents as
  // already inside free_count, with vm_stat.c subtracting it before
  // printing — no equivalent subtraction applies to purgeable_count here.)
  const freeRamMb =
    Number.isNaN(freePages) || Number.isNaN(purgeablePages)
      ? NaN
      : ((freePages + purgeablePages) * pageSizeBytes) / BYTES_PER_MB;

  return { pressureLevel, compressedMb, freeRamMb, compressions, decompressions };
}

// ---------------------------------------------------------------------------
// parsePressureLevel
// ---------------------------------------------------------------------------

/**
 * Parses the single integer printed by
 * `sysctl -n kern.memorystatus_vm_pressure_level` — the kernel's own
 * production-accurate memory-pressure signal (1 = normal, 2 = warn,
 * 4 = critical). This is what `cli.mjs`'s `realCollect()` uses for
 * `pressureLevel` today; `parseVmStat`'s ratio-derived `pressureLevel` above
 * is a fallback/test heuristic only, not this.
 *
 * Follows this file's existing "don't fabricate" convention: an unparseable
 * (non-numeric, empty, or garbled) input yields `NaN` rather than a
 * fabricated healthy `1`.
 *
 * @param {string} raw
 * @returns {number}
 */
export function parsePressureLevel(raw) {
  const text = String(raw ?? '').trim();
  const match = text.match(/^(\d+)$/);
  return match ? Number(match[1]) : NaN;
}

// ---------------------------------------------------------------------------
// parseSwapUsage
// ---------------------------------------------------------------------------

/**
 * Parses `sysctl vm.swapusage`-style output into used/total swap in MB.
 *
 * `total` and `used` are matched independently: whichever label isn't
 * found yields `NaN` for that specific field rather than a fabricated `0`.
 * A fully-garbled input (neither label found) degrades both fields to
 * `NaN`; a partially-truncated input (only one label present) degrades
 * only the missing field, leaving the matched one intact.
 *
 * @param {string} raw
 * @returns {{ swapUsedMb: number, swapTotalMb: number }}
 */
export function parseSwapUsage(raw) {
  const text = String(raw ?? '');

  const totalMatch = text.match(/total\s*=\s*([\d.]+)\s*M/i);
  const usedMatch = text.match(/used\s*=\s*([\d.]+)\s*M/i);

  return {
    swapUsedMb: usedMatch ? Number(usedMatch[1]) : NaN,
    swapTotalMb: totalMatch ? Number(totalMatch[1]) : NaN,
  };
}

// ---------------------------------------------------------------------------
// parseFreeDisk
// ---------------------------------------------------------------------------

const DF_LINE_PATTERN = /^\S+\s+(\d+)\s+(\d+)\s+(\d+)\s+\d+%/m;

/**
 * Extracts the "Available" (free) GB column from `df -g /`-style output.
 * Returns `NaN` when the line doesn't match the expected `df` shape
 * (garbled or unparseable input) rather than fabricating a `0` reading.
 *
 * @param {string} raw
 * @returns {number}
 */
function parseAvailableGb(raw) {
  const match = String(raw ?? '').match(DF_LINE_PATTERN);
  return match ? Number(match[3]) : NaN;
}

/**
 * Computes current free disk space and its decline rate (GB/hour) from two
 * timestamped `df`-style samples.
 *
 * When NEITHER sample's `df` line matches the expected shape (garbled or
 * unparseable input), returns `NaN` sentinels for both fields rather than
 * fabricating an all-zero (falsely GREEN-looking) reading — `takeSample()`'s
 * composition step treats a `NaN` field as unparseable and degrades to
 * `stale: true`.
 *
 * @param {{ raw: string, timestampMs: number }} oldSample
 * @param {{ raw: string, timestampMs: number }} newSample
 * @returns {{ freeDiskGb: number, declineRateGbPerHour: number }}
 */
export function parseFreeDisk(oldSample, newSample) {
  const oldFreeGb = parseAvailableGb(oldSample.raw);
  const newFreeGb = parseAvailableGb(newSample.raw);

  if (Number.isNaN(oldFreeGb) && Number.isNaN(newFreeGb)) {
    return { freeDiskGb: NaN, declineRateGbPerHour: NaN };
  }

  const elapsedHours = (newSample.timestampMs - oldSample.timestampMs) / (1000 * 60 * 60);
  const declineRateGbPerHour = elapsedHours > 0 ? (oldFreeGb - newFreeGb) / elapsedHours : 0;

  return { freeDiskGb: newFreeGb, declineRateGbPerHour };
}

// ---------------------------------------------------------------------------
// attributeAgentProcesses — parent-process ancestry, not name-substring match
// ---------------------------------------------------------------------------

const PS_HEADER_PATTERN = /^\s*PID\s+PPID\s+COMM\s*$/i;
const PS_LINE_PATTERN = /^\s*(\d+)\s+(\d+)\s+(?:(\d+)\s+)?(.+?)\s*$/;

// A process is considered an "agent root" when its own command is the
// claude binary itself — every descendant of that root (by parent-process
// ancestry, walked transitively) is attributed to the agent tree
// regardless of that descendant's own process name. This is the explicit
// correction the Investigation flagged: matching "node" or "claude" as a
// name substring anywhere in the tree would both over- and under-count.
//
// Deliberately case-SENSITIVE: on macOS the Claude Code CLI's binary path
// ends in lowercase `/claude`, while the Claude DESKTOP app's main process
// binary is `/Applications/Claude.app/Contents/MacOS/Claude` (capital `C`).
// A case-insensitive pattern here would misattribute the desktop app itself
// as an "agent root" — and since its Electron helper/renderer tree usually
// carries far more RSS than a real coding sub-agent, `selectPauseCandidate`
// would then nominate the user's own desktop app for pausing instead of an
// actual sub-agent. Never add the `i` flag back.
// Exported (Phase 2 review, Low) so collect.mjs can reuse this SAME
// pattern for its own footprint-sampling target selection rather than
// maintaining a byte-for-byte local duplicate that can silently drift.
export const AGENT_ROOT_COMM_PATTERN = /\/claude$/;

/**
 * Walks parent-process ancestry transitively from a set of starting PIDs,
 * returning every PID reachable (the starting PIDs themselves plus every
 * descendant, by parent-process ancestry — never by name-substring match).
 * Shared by `attributeAgentProcesses` (walked once, across all roots, so
 * overlapping trees dedupe naturally via the shared `visited` set) and
 * `listAgentProcesses` (walked once per root, to keep each tree's RSS
 * separate — see `stopPids` below for why that walk needs a stop set).
 *
 * @param {string[]} startPids
 * @param {Map<string, string[]>} childrenByPpid
 * @param {Set<string>} [stopPids] PIDs to exclude from this walk entirely —
 *   neither counted nor descended into. Used by `listAgentProcesses` to keep
 *   a nested claude-rooted process (and its own descendants) out of an
 *   ancestor root's walk, so that nested subtree is attributed exclusively
 *   to its own (nearer) root rather than double-counted under both.
 * @returns {Set<string>}
 */
function collectAncestrySet(startPids, childrenByPpid, stopPids = new Set()) {
  const visited = new Set();
  const stack = [...startPids];
  while (stack.length > 0) {
    const pid = stack.pop();
    if (visited.has(pid)) continue;
    if (stopPids.has(pid)) continue;
    visited.add(pid);

    const children = childrenByPpid.get(pid) ?? [];
    for (const childPid of children) {
      if (!visited.has(childPid)) stack.push(childPid);
    }
  }
  return visited;
}

/**
 * Attributes agent-related OS processes by walking parent-process ancestry
 * from `ps -Ao pid,ppid,comm`-style (or `pid,ppid,rss,comm`-style) output —
 * never by substring-matching a process name in isolation.
 *
 * @param {string} psTreeOutput
 * @returns {{ agentCount: number, totalRssMb: number }}
 */
export function attributeAgentProcesses(psTreeOutput) {
  const text = String(psTreeOutput ?? '');
  const lines = text.split('\n').map((line) => line.trim()).filter(Boolean);

  /** @type {Map<string, { ppid: string, comm: string, rssKb: number }>} */
  const processesByPid = new Map();
  /** @type {Map<string, string[]>} */
  const childrenByPpid = new Map();

  for (const line of lines) {
    if (PS_HEADER_PATTERN.test(line)) continue;

    const match = line.match(PS_LINE_PATTERN);
    if (!match) continue;

    const [, pid, ppid, rss, comm] = match;
    processesByPid.set(pid, { ppid, comm, rssKb: rss ? Number(rss) : 0 });

    if (!childrenByPpid.has(ppid)) childrenByPpid.set(ppid, []);
    childrenByPpid.get(ppid).push(pid);
  }

  const rootPids = [...processesByPid.entries()]
    .filter(([, proc]) => AGENT_ROOT_COMM_PATTERN.test(proc.comm))
    .map(([pid]) => pid);

  const attributedPids = collectAncestrySet(rootPids, childrenByPpid);

  let totalRssKb = 0;
  for (const pid of attributedPids) {
    totalRssKb += processesByPid.get(pid)?.rssKb ?? 0;
  }

  return {
    agentCount: attributedPids.size,
    totalRssMb: totalRssKb / 1024,
  };
}

// ---------------------------------------------------------------------------
// isEvictionAuthorized — own-spawned-only eviction authorization
// ---------------------------------------------------------------------------

/**
 * Decides whether `candidatePid` may be evicted (paused/killed) by the
 * orchestrator identified by `orchestratorPid`, using ONLY the ps-tree
 * snapshot passed in this call — a pure, side-effect-free authorization
 * check with no I/O, no shelling out, and no memoization across calls.
 *
 * This is the security-sensitive gate behind eviction: it must never
 * authorize a candidate the orchestrator did not itself spawn (directly or
 * transitively), and it must never default to permissive when any part of
 * the proof is missing or ambiguous. Four independent conditions must ALL
 * hold, or the function returns `false`:
 *
 *   1. `orchestratorIsLive` must be the literal boolean `true`. This value
 *      is caller-supplied, not derived here — the caller is expected to
 *      have already confirmed the orchestrator's own dibs/ledger entry is
 *      live (via `readDibs`) before calling this function. Anything other
 *      than literal `true` (`false`, `undefined`, omitted, or a
 *      truthy-but-non-boolean value) fails closed. This function has no way
 *      to independently verify liveness from a ps-tree snapshot alone, so
 *      it never guesses.
 *
 *   2. `candidatePid` must be reachable from `orchestratorPid` by walking
 *      parent-process ancestry transitively through `psTreeOutput` (reusing
 *      `collectAncestrySet`, the same ancestry-walk helper
 *      `attributeAgentProcesses` and `listAgentProcesses` already use — not
 *      reinvented here). A sibling orchestrator's descendant, a candidate
 *      whose ancestry chain is broken (an intermediate PID absent from the
 *      snapshot, e.g. reparented/orphaned), or a candidate absent from the
 *      snapshot entirely are all unreachable and therefore rejected — never
 *      thrown, just `false`.
 *
 *   3. `candidatePid` itself must match `AGENT_ROOT_COMM_PATTERN` (comm
 *      ends in `/claude`, case-sensitive). Ancestry proves descent, not "is
 *      agent work" — a genuine descendant helper process (e.g. a bare
 *      `node` child) that is not itself a claude-rooted process is
 *      rejected. This function does not trust its caller to have already
 *      filtered the candidate list to agent roots.
 *
 *   4. `candidatePid` must not be identical to `orchestratorPid` — an
 *      orchestrator can never authorize evicting itself, even if its own
 *      `comm` happens to match `AGENT_ROOT_COMM_PATTERN`. This condition is
 *      not implied by condition 2: `collectAncestrySet([orchestratorPid],
 *      ...)` includes the seed PID itself in its returned set by
 *      construction, so ancestry reachability alone would let a live
 *      orchestrator "authorize" evicting itself. This explicit self-pid
 *      check is what closes that hole.
 *
 * Because this reads only the `psTreeOutput` string handed to this specific
 * call, it is inherently immune to PID-reuse races ACROSS calls — a stale
 * snapshot is the caller's problem (sample fresh immediately before use),
 * not something this function can detect or compensate for.
 *
 * NOT a fifth condition here: this function never verifies that
 * `orchestratorPid` itself actually belongs to `orchestratorIsLive`'s
 * orchestrator — it takes `orchestratorPid` purely as an ancestry-walk seed.
 * That binding is a sibling check performed by the caller, in
 * `performEviction` (cli.mjs), which compares the caller-supplied
 * `--orchestrator-pid` against the pid a prior beat already recorded for
 * that `orchestratorId` on the dibs ledger, BEFORE this function is ever
 * called. Kept out of this function deliberately: this function has no
 * access to the dibs ledger, only a ps-tree snapshot.
 *
 * @param {object} params
 * @param {string} params.psTreeOutput A `ps` snapshot matched by
 *   `PS_LINE_PATTERN` (`(pid) (ppid) [rss] (comm...)` — an optional numeric
 *   RSS column, then everything else as `comm`). This function's own test
 *   fixtures use the 3-column `pid,ppid,comm` shape, but its actual
 *   production caller (`collectPsOutput()` in cli.mjs) shells out with 5
 *   columns, `pid,ppid,rss,etime,comm` — `PS_LINE_PATTERN`'s single optional
 *   numeric group only ever captures the `rss` column, so on a real 5-column
 *   line the `etime` column ends up folded into the front of the captured
 *   `comm` string (e.g. `"00:10:00 /path/to/claude"`) rather than parsed out
 *   separately. This is harmless here specifically because every comm check
 *   in this function (`AGENT_ROOT_COMM_PATTERN`, `/claude$/`) is
 *   end-anchored, so a mangled *prefix* on `comm` never changes the
 *   authorization decision — but callers must not rely on `comm` itself
 *   being a clean process name from this function's internal parsing.
 * @param {string} params.orchestratorPid PID of the orchestrator claiming
 *   authorization to evict `candidatePid`.
 * @param {boolean} [params.orchestratorIsLive] Must be literal `true` — the
 *   caller's own already-verified liveness check for `orchestratorPid`.
 * @param {string} params.candidatePid PID of the process being considered
 *   for eviction.
 * @returns {boolean}
 */
export function isEvictionAuthorized({ psTreeOutput, orchestratorPid, orchestratorIsLive, candidatePid }) {
  if (orchestratorIsLive !== true) return false;

  // Coerce to strings defensively: processesByPid/childrenByPpid below are
  // keyed by the string PIDs parsed out of psTreeOutput, so a caller passing
  // a `number` (e.g. straight from `process.pid`) would otherwise silently
  // miss every Map/Set lookup and fail closed with a confusing false
  // negative rather than an error.
  orchestratorPid = String(orchestratorPid);
  candidatePid = String(candidatePid);

  const text = String(psTreeOutput ?? '');
  const lines = text.split('\n').map((line) => line.trim()).filter(Boolean);

  /** @type {Map<string, { ppid: string, comm: string }>} */
  const processesByPid = new Map();
  /** @type {Map<string, string[]>} */
  const childrenByPpid = new Map();

  for (const line of lines) {
    if (PS_HEADER_PATTERN.test(line)) continue;

    const match = line.match(PS_LINE_PATTERN);
    if (!match) continue;

    const [, pid, ppid, , comm] = match;
    processesByPid.set(pid, { ppid, comm });

    if (!childrenByPpid.has(ppid)) childrenByPpid.set(ppid, []);
    childrenByPpid.get(ppid).push(pid);
  }

  const candidate = processesByPid.get(candidatePid);
  if (!candidate) return false;
  if (!AGENT_ROOT_COMM_PATTERN.test(candidate.comm)) return false;

  const descendantPids = collectAncestrySet([orchestratorPid], childrenByPpid);
  return descendantPids.has(candidatePid) && candidatePid !== orchestratorPid;
}

// ---------------------------------------------------------------------------
// listAgentProcesses — per-agent-tree attribution
// ---------------------------------------------------------------------------

// Column order: pid, ppid, rss(KB), etime, comm. `etime` (BSD/macOS `ps`'s
// portable elapsed-time keyword — Linux/procps-only `etimes` is rejected by
// macOS's `ps`, see cli.mjs's collectPsOutput()) is captured with `\S+` (not
// a numeric-only pattern) so an unparseable value like `-` still matches the
// line shape — the "don't fabricate" exclusion check happens after parsing
// (see parseEtimeToSeconds below), not by silently dropping the whole line.
// Exported (Phase 2 review, Medium) so collect.mjs can import this SAME
// pattern as its single source of truth for `ps -Ao pid,ppid,rss,etime,comm`
// row shape, instead of maintaining a byte-for-byte local duplicate that can
// silently drift.
export const PS_LINE_WITH_ETIME_PATTERN = /^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\S+)\s+(.+?)\s*$/;

// Narrower secondary extraction used ONLY when a body line
// fails `PS_LINE_WITH_ETIME_PATTERN` — recovers a leading pid, if the pid
// FIELD ITSELF is readable, from an otherwise-unparseable row. Anchored to
// the start of the (already-trimmed) line so it can never bleed digits from
// a neighbouring line. Deliberately does not attempt to recover any other
// column — a row this signal names is never promoted to a parsed entry.
//
// The trailing boundary is a LOOKAHEAD for whitespace-or-end-of-line, not a
// consumed `\s+` (pre-PR review, Medium — it was `\s+`). A row truncated to
// its own pid and nothing else (`"4242"`) is the simplest garble this signal
// exists to catch, and a required trailing space missed exactly that one: the
// row recovered `pid: null`, the entry fell through to `pid-absent`, and the
// live agent was reaped — the outcome was opened to prevent. The
// lookahead also keeps the boundary a BOUNDARY: `12345abc` still recovers
// nothing rather than silently yielding `12345`, so a non-pid token that
// merely starts with digits cannot lend its prefix to an unrelated record.
const PS_LEADING_PID_PATTERN = /^\s*(\d+)(?=\s|$)/;
// Exported (review, Low) so cli.mjs's own `psTreeHasPid` can reuse the
// SAME explicit header-skip this module's `listAgentProcesses` already
// applies to its own (identically-shaped, `pid,ppid,rss,etime,comm`)
// `collectPsOutput()` input, rather than relying on the incidental fact that
// `PID` isn't numeric and so happens not to match `psTreeHasPid`'s own
// line-matching regex either.
export const PS_HEADER_WITH_ETIME_PATTERN = /^\s*PID\s+PPID\s+RSS\s+ELAPSED\s+COMM\s*$/i;

// BSD/macOS `ps -o etime` format: `[[dd-]hh:]mm:ss` — e.g. `03:15`,
// `01:02:03`, `1-02:03:04`. Each numeric group's width is not fixed (macOS
// does not always zero-pad the leading group), so this is intentionally
// permissive on digit count while still requiring the exact `-`/`:` shape.
const ETIME_PATTERN = /^(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+)$/;

/**
 * The three shapes a `ps` snapshot's TEXT can take, judged on the text alone
 * and before any agent attribution: `'readable'` (a MAJORITY of the non-header
 * lines parse as process rows), `'empty'` (no non-header lines at all), and
 * `'unreadable'` (non-header lines exist and no majority of them parses).
 *
 * @typedef {'readable' | 'empty' | 'unreadable'} ProcessSnapshotShape
 */

/**
 * The share of a snapshot's non-header lines that must parse before the text
 * counts as a legible process table.
 *
 * A MAJORITY rather than "at least one". Real `ps -A` on this project's target
 * host emits hundreds of rows, so a single-parseable-row threshold is
 * disarmable by one surviving row: a drift that mangles all but one line reads
 * as fully readable, the snapshot is passed through, and every agent whose row
 * was mangled classifies `confirmed-dead`. Requiring a majority costs the same
 * single pass and raises the bar from "any one surviving row disarms the guard"
 * to "half the table has to survive".
 *
 * WHAT IT DOES NOT BUY, MEASURED (pre-PR review, Medium — this doc-block used
 * to claim the majority meant "a real drift disarms nothing", and the very next
 * doc-block conceded the opposite). The share is taken over ALL of `ps -A`'s
 * rows, while the only rows whose parseability protects the fleet are the
 * claude-rooted ones. On the development host, `ps -Ao pid,ppid,rss,etime,comm`
 * returned 667 body rows of which `listAgentProcesses` recognised 4 as agent
 * roots — 0.6%. The absolute count drifts run to run, as any process census
 * does; the RATIO is the point. A drift confined to how CLAUDE processes are reported (a `comm`
 * that stops being path-shaped, a wrapper or relaunch, a binary rename) leaves
 * ~99% of rows parseable, classifies `'readable'`, yields `[]`, and reaps every
 * pid-bearing record. This threshold catches a drift that mangles the TABLE; it
 * is blind by construction to one that mangles only the agents. That is the
 * same residual named below, stated with its actual size. It is this design's
 * residual limitation 1, and the cheap bound for it — cross-checking the beat's
 * own resolved `identity` against a `'readable'` snapshot that yielded zero
 * agent roots — is a behavioural change to the reap rule, deliberately NOT made
 * here and tracked at. Do not narrow this threshold in its place: the
 * ratio is the reason the threshold cannot see this case, so no value of it
 * helps.
 *
 * The OTHER residual, the converse one, is a MINORITY-degraded snapshot: text in
 * which most rows parse and a few do not is `'readable'` and is passed through.
 * Phases 1-2 narrowed, but did not close, what happens next: a live
 * agent whose own row was among the unparseable ones, and whose pid was still
 * recoverable from that row, is now correlated via the `unparseableRows`
 * signal and classified `UNKNOWN`/`PID_ROW_UNPARSEABLE` rather than reaped.
 * Only the entry whose recovered pid is itself unrecoverable (`pid: null`)
 * still reads as `confirmed-dead` — an accepted, in-scope-excluded residual
 * (Convergence Analysis edge case #2), not an oversight. It is a documented
 * residual limitation and `reconciliation.mjs`'s "WHAT REMAINS UNCOVERED"
 * block, and is tracked separately — a SEPARATE gap from the total-reap case
 * handled above. The row that gets dropped is the
 * `if (!match) continue` in `listAgentProcesses` below.
 *
 * Strict majority (`> 1/2`), so an exact half-and-half split is `'unreadable'`
 * — the fail-safe side, since `'unreadable'` reaps nothing and defers every
 * record to its TTL.
 */
const READABLE_ROW_MAJORITY = 0.5;

/**
 * Judges whether a `ps` snapshot is legible as a process table at all — the
 * one question `listAgentProcesses` cannot answer for its caller, because its
 * `[]` means "no claude-rooted trees here" whether the text was a perfectly
 * readable table of non-agent processes or a table this file's column grammar
 * could not read a single row of.
 *
 * That distinction is load-bearing for the ledger sweep. `[]` is a valid
 * observation of zero live agents and condemns every pid-bearing ledger record
 * as `confirmed-dead`; an unreadable snapshot produces the same `[]` while
 * being no observation at all, so a sweep that cannot tell them apart reaps the
 * whole fleet the first time `ps` output drifts. Drift is the realistic case
 * rather than a hypothetical one — a column-order, locale, or header change
 * yields text `listAgentProcesses` retains zero rows from without throwing,
 * which is the same class of silent `ps` incompatibility already shipped
 * once.
 *
 * A row counts as parseable only when it matches the column grammar AND its
 * etime field parses — not on the line shape alone. See the body for why: the
 * line shape is deliberately permissive about etime, so judging on it alone let
 * a whole class of column-order drift read as `'readable'`.
 *
 * `'empty'` stays a DISTINCT verdict from `'unreadable'`, because they are
 * different observations about the text: one is a table this grammar could not
 * read, the other is no table at all. What they are NOT is differently
 * trustworthy — and this doc-block used to say they were, justifying `'empty'`
 * as a "legitimate boundary case" that must stay passable so "a genuinely idle
 * host [can] still reap ghosts" (pre-PR review, High). That justification
 * describes a case that cannot occur. `ps -A` enumerates every process on the
 * host, and a host running the sweep is running at minimum `launchd`, the
 * sweeping `node` process and its claude-rooted parent; the development host
 * returned 667 body rows while idle. A genuinely AGENTLESS host produces a large
 * `'readable'` table with no claude roots in it and reaps ghosts through that
 * branch, which is the case the old rationale was reaching for. Zero body lines
 * behind an exit code of zero is a broken collection, and `listAgentProcesses`
 * answers `[]` for it exactly as it does for a drifted table.
 *
 * This function still returns the two verdicts separately: it judges TEXT, and
 * cannot know who is running. Which verdicts amount to "no observation" is the
 * caller's decision, and `cli.mjs`'s `observeBeatProcesses` treats BOTH as one
 * — it threads `null` for anything that is not `'readable'`.
 *
 * The readable verdict is a MAJORITY of parseable body lines, not a single one
 * — see `READABLE_ROW_MAJORITY`. This narrows, and does not close, the
 * partially-degraded-snapshot residual named in `./reconciliation.mjs`: a drift
 * that leaves most rows parseable while mangling a few still passes through.
 *
 * Total over every input this project can hand it, and pure: any input is
 * coerced through the same `String(value ?? '')` funnel the parsers here
 * already use, and nothing is mutated. NOT unconditionally throw-free, which
 * this line claimed until a pre-PR review checked it — `String(value)` runs the
 * value's own `toString`/`valueOf`, so an object with a hostile one propagates
 * that throw. Production input is always the string `execFileSync` returned, so
 * the reachable contract is the one stated; the unreachable absolute is not
 * worth a guard here, only an honest sentence.
 *
 * @param {string} psTreeOutput `ps -Ao pid,ppid,rss,etime,comm`-style text.
 * @returns {ProcessSnapshotShape}
 */
export function classifyProcessSnapshotShape(psTreeOutput) {
  const bodyLines = String(psTreeOutput ?? '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .filter((line) => !PS_HEADER_WITH_ETIME_PATTERN.test(line));

  if (bodyLines.length === 0) return 'empty';

  // The line shape ALONE is not enough, and this is the whole point of the
  // guard (pre-PR review, High). `PS_LINE_WITH_ETIME_PATTERN` captures the
  // etime column as `\S+` on purpose — `listAgentProcesses` needs a row with an
  // unparseable etime (`-`) to still match the line shape so it can exclude
  // that ONE row rather than mistake the whole table for garbage. But a
  // column-order drift that keeps three leading integer columns, e.g.
  // `pid,ppid,rss,comm,etime` instead of `pid,ppid,rss,etime,comm`, matches the
  // line shape on EVERY row while `listAgentProcesses` retains none of them:
  // the comm lands in the etime group (`NaN` seconds -> row excluded) and the
  // etime lands in the comm group (no `/claude` suffix -> not a root). Judging
  // on the line shape alone therefore called that text `'readable'` and handed
  // the sweep an empty array — a "valid observation of zero live agents" — so
  // every pid-bearing record in all three ledgers classified `pid-absent` ->
  // `confirmed-dead` and the whole fleet's ledger state was reaped on one beat.
  // That is precisely the outcome this function exists to prevent.
  //
  // So a body line counts as parseable only when the field the attribution walk
  // actually consumes parses too. This is still a MAJORITY test, so the
  // legitimate single `-` etime row does not tip a real table to `'unreadable'`,
  // and the failure direction is unchanged: `'unreadable'` reaps nothing.
  const parseableCount = bodyLines.filter((line) => {
    const match = line.match(PS_LINE_WITH_ETIME_PATTERN);
    return match !== null && ETIME_PATTERN.test(match[4]);
  }).length;
  return parseableCount > bodyLines.length * READABLE_ROW_MAJORITY ? 'readable' : 'unreadable';
}

/**
 * Parses a BSD/macOS `ps -o etime` value (`[[dd-]hh:]mm:ss`) into total
 * elapsed seconds. Returns `NaN` for any value that doesn't match the exact
 * grammar — callers must treat `NaN` as "exclude this process/tree", never
 * fabricate a fallback duration (this file's existing "don't fabricate"
 * convention, previously applied to the `etimes`(seconds) column before this
 * skill's Phase 2 fix switched to macOS-portable `etime`).
 *
 * @param {string} etimeRaw
 * @returns {number}
 */
export function parseEtimeToSeconds(etimeRaw) {
  const match = String(etimeRaw ?? '').match(ETIME_PATTERN);
  if (!match) return NaN;

  const [, days, hours, minutes, seconds] = match;
  return (
    (days ? Number(days) * 86400 : 0) + (hours ? Number(hours) * 3600 : 0) + Number(minutes) * 60 + Number(seconds)
  );
}

/**
 * Lists one entry per independent claude-rooted process tree (same
 * ancestry-based attribution rule as `attributeAgentProcesses` —
 * `AGENT_ROOT_COMM_PATTERN` for the root, transitive descendants via
 * parent-process ancestry), rather than a single aggregate across all trees.
 *
 * A root whose own `etime` field is unparseable is excluded — the whole
 * tree is dropped rather than fabricating a `startedAt` (this file's
 * existing "don't fabricate" convention).
 *
 * Nested/multi-contributor orchestration is plausible in this project's
 * architecture: one claude-rooted process (a sub-orchestrator) can itself be
 * a descendant of another claude-rooted process. Each root's ancestry walk
 * excludes every *other* root PID (and, transitively, that other root's own
 * subtree) — so a nested root's tree is attributed exclusively to its own
 * (nearest) root, never double-counted under an ancestor root as well. Every
 * process therefore ends up in exactly one entry: the nearest claude-rooted
 * ancestor, not every claude-rooted ancestor above it.
 *
 * @param {string} psTreeOutput ps -Ao pid,ppid,rss,etime,comm-style output.
 * @param {number} now Epoch-ms "now", used to derive `startedAt` from etime.
 * @returns {Array<{ agentId: string, rssMb: number, startedAt: number }> & { unparseableRows: Array<{ pid: number|null, rawLine: string }> }}
 *   The returned array carries a non-enumerable `unparseableRows` property
 *   — rows that failed to parse and whose pid, if recovered,
 *   does not collide with a successfully-parsed row elsewhere in the same
 *   snapshot. Non-enumerable so `toEqual`/`JSON.stringify`/`for...in` and
 *   other own-enumerable-property walks see a plain array; read it via
 *   direct property access or destructuring instead.
 */
export function listAgentProcesses(psTreeOutput, now) {
  const text = String(psTreeOutput ?? '');
  const lines = text.split('\n').map((line) => line.trim()).filter(Boolean);

  /** @type {Map<string, { ppid: string, comm: string, rssKb: number, etimeRaw: string }>} */
  const processesByPid = new Map();
  /** @type {Map<string, string[]>} */
  const childrenByPpid = new Map();
  /** @type {Array<{ pid: number|null, rawLine: string }>} */
  const unparseableRows = [];

  for (const line of lines) {
    if (PS_HEADER_WITH_ETIME_PATTERN.test(line)) continue;

    const match = line.match(PS_LINE_WITH_ETIME_PATTERN);
    // THIS `continue` IS WHERE THE MINORITY-GARBLED RESIDUAL ( 
    // residual limitation 2) ORIGINATES. Dropping an unparseable row is
    // correct — this file's "don't fabricate" convention — but a LIVE agent
    // whose own row was the unparseable one is then absent from the returned
    // array itself.
    //
    // Phase 1 stopped that drop from being silent: the row is still
    // never fabricated into a parsed entry, but its pid, if recoverable from
    // the line, plus the raw line itself are additively surfaced via the
    // `unparseableRows` property (see the pid-recovery attempt +
    // valid-row-wins filter below). Phase 2 (`reconciliation.mjs`
    // rule 3.5) consumes that signal: a ledger entry whose pid matches a
    // `unparseableRows` entry is classified `UNKNOWN`/`PID_ROW_UNPARSEABLE`
    // instead of falling through to `pid-absent` -> `confirmed-dead`, so a
    // live agent recovered this way is no longer reaped.
    //
    // What remains open is the entry whose recovered pid is `null` — the
    // row's pid field itself failed to parse, so there is nothing to
    // correlate it against and it still falls through to `pid-absent` ->
    // `confirmed-dead` as before. That narrower residual is accepted,
    // in-scope-excluded behaviour (Convergence Analysis edge case #2), not a
    // gap Phase 1/2 closed. `classifyProcessSnapshotShape`'s majority
    // threshold narrows the overall exposure (a drift mangling half the table
    // degrades to `'unreadable'`, which reaps nothing) independently of this.
    // Distinct from the table-parses-but-no-agent-roots case.
    if (!match) {
      const pidMatch = line.match(PS_LEADING_PID_PATTERN);
      unparseableRows.push({ pid: pidMatch ? Number(pidMatch[1]) : null, rawLine: line });
      continue;
    }

    const [, pid, ppid, rss, etimeRaw, comm] = match;
    processesByPid.set(pid, { ppid, comm, rssKb: Number(rss), etimeRaw });

    if (!childrenByPpid.has(ppid)) childrenByPpid.set(ppid, []);
    childrenByPpid.get(ppid).push(pid);
  }

  // Valid-row-wins (Convergence Analysis edge case #5): a recovered pid whose
  // digits collide with a row that parsed cleanly elsewhere in the same
  // table must not also be reported as unparseable — the successfully-parsed
  // row is authoritative for that pid.
  const recoveredUnparseableRows = unparseableRows.filter(
    (row) => row.pid === null || !processesByPid.has(String(row.pid)),
  );

  const rootPids = [...processesByPid.entries()]
    .filter(([, proc]) => AGENT_ROOT_COMM_PATTERN.test(proc.comm))
    .map(([pid]) => pid);

  // Up-front pass (fix): determine which root PIDs will actually
  // produce their own entry (a parseable etime) BEFORE walking any root's
  // ancestry. A root with an unparseable etime is excluded from every OTHER
  // root's stop-set — it produces no entry of its own, so it must not wall
  // off its subtree from whichever ancestor root's walk would otherwise
  // reach it. This is computed independently of `entries` (which only grows
  // incrementally inside the loop below) and independently of `rootPids`'
  // order, so the result is the same regardless of which order roots appear
  // in the raw `ps` text.
  const validRootPids = new Set(
    rootPids.filter((pid) => !Number.isNaN(parseEtimeToSeconds(processesByPid.get(pid).etimeRaw))),
  );

  const entries = [];
  for (const rootPid of rootPids) {
    const rootEtimeSeconds = parseEtimeToSeconds(processesByPid.get(rootPid).etimeRaw);
    if (Number.isNaN(rootEtimeSeconds)) continue;

    // Every other VALIDLY-COUNTED claude-rooted PID is a stop point for this
    // root's walk — if this root's descendant tree contains another root
    // that itself produced an entry, that nested root (and everything under
    // it) is exclusively that root's own entry, not double-counted here too.
    // A root that was skipped for an unparseable etime is deliberately left
    // OUT of this stop-set: it has no entry of its own to attribute its
    // subtree to, so its subtree must fold into the nearest ancestor root's
    // walk instead of vanishing.
    const otherRootPids = new Set([...validRootPids].filter((otherPid) => otherPid !== rootPid));
    const treePids = collectAncestrySet([rootPid], childrenByPpid, otherRootPids);

    let totalRssKb = 0;
    for (const pid of treePids) {
      totalRssKb += processesByPid.get(pid)?.rssKb ?? 0;
    }

    entries.push({
      agentId: rootPid,
      rssMb: totalRssKb / 1024,
      startedAt: now - rootEtimeSeconds * 1000,
    });
  }

  entries.sort((a, b) => Number(a.agentId) - Number(b.agentId));

  // Additive-only: a property on the same array, not a
  // reshaped envelope, so existing callers that only iterate/map/filter the
  // array's own elements observe no change. Non-enumerable so Jest's
  // `toEqual`/`Object.keys`/`for...in`/`JSON.stringify` — all of which walk
  // own-enumerable properties, not just indices, for an array — continue to
  // see a plain array; direct access (`entries.unparseableRows`, or
  // destructuring `{ unparseableRows }`) still works.
  Object.defineProperty(entries, 'unparseableRows', {
    value: recoveredUnparseableRows,
    enumerable: false,
  });

  return entries;
}

// ---------------------------------------------------------------------------
// resolveNearestClaudeRootIdentity — the ledger write path's identity
// ---------------------------------------------------------------------------

/**
 * Resolves the ledger identity a `cli.mjs` beat records on every record it
 * writes: the NEAREST claude-rooted process at or above `startPid` in
 * parent-process ancestry, named by its pid and by the start time
 * `listAgentProcesses` derives for that same pid from that same snapshot.
 *
 * WHY AN ANCESTOR AND NOT THE WRITER ITSELF. `cli.mjs` is a one-shot `node`
 * process per beat, and `listAgentProcesses` — the snapshot the read-side
 * classifier (`./reconciliation.mjs`) correlates against — only ever emits
 * claude-rooted processes (`AGENT_ROOT_COMM_PATTERN`). A `node` pid can never
 * appear there, so a record stamped with the writer's own pid is structurally
 * invisible to the classifier and reads as `confirmed-dead` on the next sweep.
 * The orchestrator that spawned the beat is the thing whose life the record
 * actually tracks, and that is what this resolves.
 *
 * NEAREST, NOT TOPMOST. `listAgentProcesses` attributes every process to its
 * nearest claude-rooted ancestor — a nested root owns its own subtree
 * exclusively. Resolving to a topmost root would name a pid whose snapshot row
 * exists but whose tree does not contain the writer, so the write side and the
 * read side would disagree about which agent a record belongs to.
 *
 * ONE FORMULA, SHARED. `pidStartedAt` is `now - parseEtimeToSeconds(etime) *
 * 1000`, the same derivation `listAgentProcesses` applies to its own roots'
 * `startedAt`, reusing this module's own parser rather than repeating it. The
 * correlation the sweep performs is only sound because both sides compute the
 * instant identically; a second derivation would drift a live agent out of
 * `PID_IDENTITY_TOLERANCE_MS` and reap it as pid reuse.
 *
 * Pure and side-effect-free like everything else here: it shells out for
 * nothing, reads no clock (the caller's `now` is the only anchor, which is what
 * lets one beat resolve one identity for all of its writes), mutates neither
 * argument, and never throws. Every unresolvable case — an unparseable or
 * non-string snapshot, a `startPid` the snapshot does not carry, a chain
 * running off the top of a partially-captured tree, a ppid cycle, no
 * claude-rooted ancestor at all, or a nearest root whose own `etime` cannot be
 * parsed — returns `null`, which callers must read as "omit `pid`/
 * `pidStartedAt` from this write" and never as an error or a sentinel pid. An
 * unparseable-etime root is `null` rather than a promotion to the root above
 * it, following this module's "don't fabricate" convention: a fabricated or
 * foreign `pidStartedAt` reads as pid reuse on the next sweep, which is a reap
 * of a live agent.
 *
 * The walk is ITERATIVE and guarded by a visited set, so a self-parenting
 * process or a multi-process ppid cycle terminates, and an arbitrarily deep
 * chain costs no stack.
 *
 * NEVER THROWS, for any argument: each of the three is rejected by type before
 * it reaches an operation that could raise on a hostile value. Every
 * unresolvable case is `null`.
 *
 * @param {string} psTreeOutput `ps -Ao pid,ppid,rss,etime,comm`-style text —
 *   the same shape `listAgentProcesses` parses.
 * @param {number | string} startPid The pid the upward walk starts AT, and
 *   itself in scope for the match (an orchestrator spawning `cli.mjs` directly
 *   makes the claude root the child's own `process.ppid`, so excluding the
 *   starting point would break the ordinary case). Accepted as a number or a
 *   numeric string: `process.ppid` is a number while `ps` pids are strings, and
 *   no caller should have to know which side coerces. Any OTHER type is
 *   `null` — rejected by type BEFORE coercion, because `String(value)` runs
 *   the value's own `toString`/`valueOf`, and a hostile or broken one that
 *   throws would break the "never throws" contract above.
 *
 *   NOT because `String()` throws on a `Symbol` — it does not, and this
 *   comment said so until a pre-PR review checked it. `String(Symbol('x'))`
 *   is specified to return `'Symbol(x)'`; only IMPLICIT ToString (a template
 *   literal, `+`) throws on one. A symbol is rejected here simply for being
 *   the wrong type, and would have been harmless either way.
 * @param {number} now Epoch-ms instant the snapshot was taken. Held to the
 *   SAME type-before-use discipline as `startPid`, and for the same reason
 *   (pre-PR review, Medium — it was not, and the "never throws" contract was
 *   therefore false: `now` reaches arithmetic below, and a `Symbol`, a
 *   `BigInt`, or an object with a throwing `valueOf` each raised out of this
 *   function). Anything that is not a finite number is `null`.
 * @returns {{ pid: number, pidStartedAt: number } | null}
 */
export function resolveNearestClaudeRootIdentity(psTreeOutput, startPid, now) {
  if (typeof psTreeOutput !== 'string') return null;
  if (typeof startPid !== 'number' && typeof startPid !== 'string') return null;
  // `now` is the OTHER operand the "never throws" contract depends on: it is
  // subtracted from below, and `Symbol`/`BigInt`/a hostile `valueOf` all throw
  // out of that arithmetic rather than yielding `NaN`. Gated by type up front,
  // exactly as `startPid` is.
  if (typeof now !== 'number' || !Number.isFinite(now)) return null;

  /** @type {Map<string, { ppid: string, comm: string, etimeRaw: string }>} */
  const processesByPid = new Map();
  for (const line of psTreeOutput.split('\n').map((rawLine) => rawLine.trim()).filter(Boolean)) {
    if (PS_HEADER_WITH_ETIME_PATTERN.test(line)) continue;

    const match = line.match(PS_LINE_WITH_ETIME_PATTERN);
    if (!match) continue;

    const [, pid, ppid, , etimeRaw, comm] = match;
    processesByPid.set(pid, { ppid, comm, etimeRaw });
  }

  const visited = new Set();
  let cursor = String(startPid);
  while (processesByPid.has(cursor) && !visited.has(cursor)) {
    visited.add(cursor);
    const currentProcess = processesByPid.get(cursor);

    if (AGENT_ROOT_COMM_PATTERN.test(currentProcess.comm)) {
      const etimeSeconds = parseEtimeToSeconds(currentProcess.etimeRaw);
      if (Number.isNaN(etimeSeconds)) return null;

      // Both halves must survive the read side's own shape gates or the pair is
      // worthless, so reject it here rather than persist it (pre-PR review,
      // Low). `now` is not this function's to trust: a non-integer or `NaN`
      // anchor yields a `pidStartedAt` that `JSON.stringify` writes as `null`
      // (for `NaN`) or that `isEpochMsLike` rejects for being fractional, and
      // the record then carries a `pid` with an unusable partner — permanently
      // `no-pid`/`unknown`, dead weight no sweep can ever act on. `null` is the
      // documented "omit both fields" answer for exactly this class of
      // unresolvable case, and it keeps `withPidIdentity`'s "never `null`,
      // never `NaN`" promise true of the persisted record. Production cannot
      // reach it — the beat's anchor is `cli.mjs`'s `resolveProcessSnapshotNow`
      // (pre-PR review, Low — this named `resolveNow`, which is only the branch
      // that anchor takes under `ARM_FAKE_PS_OUTPUT`), and both of its branches
      // return an integer — so it is a library-contract guard, not a live-path
      // fix.
      const pidStartedAt = now - etimeSeconds * 1000;
      if (!Number.isInteger(pidStartedAt) || pidStartedAt <= 0) return null;

      return { pid: Number(cursor), pidStartedAt };
    }

    cursor = currentProcess.ppid;
  }

  return null;
}

// ---------------------------------------------------------------------------
// takeSample(collect) — composed sampling entrypoint
// ---------------------------------------------------------------------------

/**
 * @param {unknown} sample
 * @returns {sample is { memory: object, disk: object }}
 */
function isWellFormedSample(sample) {
  return (
    sample !== null &&
    typeof sample === 'object' &&
    typeof sample.memory === 'object' &&
    sample.memory !== null &&
    typeof sample.disk === 'object' &&
    sample.disk !== null
  );
}

/**
 * Detects the `NaN` sentinel the parsers above emit when the underlying
 * command text was garbled/unparseable (none of their expected labels were
 * found). A single `NaN` field anywhere in the reading is enough to treat
 * the whole reading as unparseable — a partially-fabricated reading is just
 * as dangerous as a fully-fabricated one.
 *
 * @param {object} reading
 * @returns {boolean}
 */
function hasUnparseableField(reading) {
  return Object.values(reading).some((value) => typeof value === 'number' && Number.isNaN(value));
}

/**
 * Composes a full resource sample from an injected `collect()` callback.
 * Calls `collect()` exactly once. Never shells out itself, and never
 * throws — a failing collect(), a malformed collect() result, or a
 * well-shaped-but-unparseable (`NaN`-sentinel) reading all degrade to the
 * same explicit `{ stale: true, ... }` marker instead.
 *
 * @param {() => Promise<{ memory: object, disk: object }>} collect
 * @returns {Promise<{ stale: boolean, memory: object, disk: object }>}
 */
export async function takeSample(collect) {
  let raw;
  try {
    raw = await collect();
  } catch {
    return { stale: true, memory: staleMemoryMarker(), disk: staleDiskMarker() };
  }

  if (!isWellFormedSample(raw)) {
    return { stale: true, memory: staleMemoryMarker(), disk: staleDiskMarker() };
  }

  if (hasUnparseableField(raw.memory) || hasUnparseableField(raw.disk)) {
    return { stale: true, memory: staleMemoryMarker(), disk: staleDiskMarker() };
  }

  return { stale: false, memory: raw.memory, disk: raw.disk };
}
