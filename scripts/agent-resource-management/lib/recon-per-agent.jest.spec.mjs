// Phase 1 RED unit tests for `listAgentProcesses` (— "Per-agent process
// attribution primitive"). `recon.mjs` does not yet export this function —
// this file is red by construction (either a module-not-found/undefined-
// export error, or explicit assertion failures) until Phase 1's implementer
// adds it. Do not add a stub implementation to force green.
//
// This spec is ADDITIVE to `recon.jest.spec.mjs` — it does NOT modify that
// file or `attributeAgentProcesses`'s existing aggregate contract, per the
// ticket's Investigation.
//
// Contract under test:
//   listAgentProcesses(psTreeOutput, now)
//     -> Array<{ agentId: string, rssMb: number, startedAt: number }>
//
// One entry per claude-rooted process tree (same ancestry-based attribution
// rule as `attributeAgentProcesses`'s `AGENT_ROOT_COMM_PATTERN` — a root is a
// process whose own `comm` matches `/\/claude$/` (case-sensitive — the
// capitalized Claude desktop app's `.../MacOS/Claude` binary must NOT match,
// see the "does not attribute a capitalized .../Claude desktop-app process"
// test below); every transitive descendant by parent-process ancestry
// belongs to that root's tree, never a name-substring match):
//   - agentId   = the root process's PID, stringified.
//   - rssMb     = SUM of RSS (MB, converted from the KB `ps` reports) across
//                 the root and every descendant in its tree — mirroring
//                 `attributeAgentProcesses`'s existing `totalRssMb`
//                 computation, but per-root instead of aggregated across all
//                 roots.
//   - startedAt = epoch-ms timestamp for the ROOT process, derived from a NEW
//                 `ps` output field carrying process start time.
//
// INPUT SHAPE DECISION (documented here — Phase 2 Critical fix):
// this file's fixtures originally used `ps -Ao pid,ppid,rss,etimes,comm`-style
// text (`etimes`, a plain integer seconds-elapsed-since-start). `etimes` is a
// Linux/procps-only keyword — BSD/macOS `ps` (this project's actual target
// host, per SKILL.md) rejects it outright, so the real (non-test) shell-out
// silently failed on every production run. Fixtures now use the
// macOS/BSD-portable `etime` keyword instead, whose format is
// `[[dd-]hh:]mm:ss` (verified against this machine's real `ps -Ao
// pid,ppid,rss,etime,comm` output): seconds-only durations render as `mm:ss`,
// durations of an hour or more (but under a day) render as `hh:mm:ss`, and
// durations of a day or more render as `dd-hh:mm:ss`. `startedAt` is computed
// as `now - (parseEtimeToSeconds(etime) * 1000)`.
//
// Column order for every fixture line below: pid, ppid, rss(KB), etime, comm

import { listAgentProcesses } from './recon.mjs';

// A fixed `now` so `startedAt` expectations are deterministic.
const NOW_MS = Date.parse('2026-08-04T12:00:00Z');

// ---------------------------------------------------------------------------
// Fixtures — ps -Ao pid,ppid,rss,etime,comm style output.
// ---------------------------------------------------------------------------

// Two independent claude-rooted trees on one host, each with its own
// descendant(s). Root A (9001) has a node child (9002); root B (9101) has a
// node child (9102). Ordered non-ascending by PID here to prove sort order
// isn't just "preserve input order".
const PS_TREE_TWO_ROOTS = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9101   501 204800    02:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9102  9101  20480    01:58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001  51200       58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

// A claude-rooted tree (9001) plus an unrelated `node` process (701) whose
// parent (700, mDNSResponder) is not descended from any claude root — proves
// ancestry-based, not name-substring, attribution for this new function too.
const PS_TREE_WITH_UNRELATED_NODE = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001  51200       58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
  700     1    768 1-03:38:20 /usr/sbin/mDNSResponder
  701   700  40960    02:30:00 node /Users/dev/unrelated-server/index.js
`;

const PS_TREE_IDLE = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
  700     1    768 1-03:38:20 /usr/sbin/mDNSResponder
`;

const PS_TREE_EMPTY = '';

const PS_TREE_GARBLED = 'this is not ps output at all\n\tsome garbage\t\t123';

// Two claude roots: 9001 has a well-formed etime; 9201's own etime column is
// unparseable ("-", the shape `ps` prints for a field it can't report). Root
// 9201 (and its whole tree) must be excluded entirely from the breakdown —
// not included with a fabricated startedAt.
const PS_TREE_UNPARSEABLE_ETIMES_ROOT = `  PID  PPID    RSS     ELAPSED COMM
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001  51200       58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
 9201   501  51200           - /Applications/Claude.app/Contents/MacOS/claude
 9202  9201  25600       16:40 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

// Two claude roots with identical per-tree rssMb and identical etime (so
// identical startedAt), listed in descending PID order in the raw input, to
// prove the output order is a stable, deterministic tie-break (ascending
// agentId) rather than input-preserving or unstable.
const PS_TREE_TIED_ROOTS_DESCENDING_INPUT = `  PID  PPID    RSS     ELAPSED COMM
 9301   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9101   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
`;

// Nested/multi-contributor orchestration: root A (9001) has a node child
// (9002), which itself spawns a SECOND claude-rooted process, root B (9003)
// — a sub-orchestrator — which in turn has its own node child (9004). B sits
// inside A's descendant tree. Every process must be attributed to exactly
// one entry: the NEAREST claude-rooted ancestor. So 9002 belongs to A (its
// nearest root is A itself); 9003 and 9004 belong exclusively to B, and must
// NOT also appear (or have their RSS counted) under A.
const PS_TREE_NESTED_ROOTS = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001  51200       58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
 9003  9002 204800       33:20 /Applications/Claude.app/Contents/MacOS/claude
 9004  9003  20480       31:40 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

// A single-root fixture used to exercise every etime shape in isolation:
// seconds-only (mm:ss), hours (hh:mm:ss), and days (dd-hh:mm:ss).
const PS_TREE_ETIME_SECONDS_ONLY = `  PID  PPID    RSS     ELAPSED COMM
 9001   501 102400       01:05 /Applications/Claude.app/Contents/MacOS/claude
`;
const PS_TREE_ETIME_HOURS = `  PID  PPID    RSS     ELAPSED COMM
 9001   501 102400    02:03:04 /Applications/Claude.app/Contents/MacOS/claude
`;
const PS_TREE_ETIME_DAYS = `  PID  PPID    RSS     ELAPSED COMM
 9001   501 102400 3-04:05:06 /Applications/Claude.app/Contents/MacOS/claude
`;
const PS_TREE_ETIME_MALFORMED = `  PID  PPID    RSS     ELAPSED COMM
 9001   501 102400    not-a-time /Applications/Claude.app/Contents/MacOS/claude
`;

// A capitalized `.../Claude` root — the Claude DESKTOP app's own main
// process binary path (e.g. `/Applications/Claude.app/Contents/MacOS/Claude`)
// — alongside a genuine lowercase `.../claude` root (the CLI). Proves
// `AGENT_ROOT_COMM_PATTERN` is case-SENSITIVE: the capitalized desktop-app
// process must never be attributed as an agent root, while the lowercase
// CLI-rooted tree still is.
const PS_TREE_DESKTOP_APP_AND_CLI_ROOT = `  PID  PPID    RSS     ELAPSED COMM
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001  51200       58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
47960   501 819200 3-00:00:00 /Applications/Claude.app/Contents/MacOS/Claude
47961 47960 409600 3-00:00:00 /Applications/Claude.app/Contents/Frameworks/Claude Helper (Renderer).app/Contents/MacOS/Claude Helper (Renderer)
`;

// RED: a nested claude-rooted process (B, 9401) whose own `etime` is
// unparseable ("-") correctly produces no entry of its own — but its PID is
// still added to `otherRootPids`, the stop-set used when walking OTHER
// roots' ancestry (see recon.mjs's `listAgentProcesses`, the
// `otherRootPids`/`collectAncestrySet(..., stopPids)` call). That means B's
// whole subtree (B itself, RSS 51200 KB, plus its child C, RSS 20480 KB) is
// walled off from root A's (9301) walk too, even though B has no entry of
// its own to attribute that subtree to. Root A's tree must fold B's subtree
// back in: A (9301, RSS 102400 KB) + B (9401, RSS 51200 KB) + C (9402, RSS
// 20480 KB) = 174080 KB = 170 MB. Today's buggy code stops at A's own RSS
// (102400 KB = 100 MB) because the `otherRootPids` stop set excludes B (and
// therefore C) from A's walk.
const PS_TREE_NESTED_ROOT_UNPARSEABLE_ETIME = `  PID  PPID    RSS     ELAPSED COMM
 9301   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9401  9301  51200           - /Applications/Claude.app/Contents/MacOS/claude
 9402  9401  20480       16:40 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

// The SAME fixture as PS_TREE_NESTED_ROOT_UNPARSEABLE_ETIME above, but with
// the invalid-etime root's line (and its child's line) appearing BEFORE the
// valid ancestor root's line in the raw `ps` text — proves the fix cannot
// depend on processing roots in `ps` output order (or in the order entries
// happen to accumulate).
const PS_TREE_NESTED_ROOT_UNPARSEABLE_ETIME_REORDERED = `  PID  PPID    RSS     ELAPSED COMM
 9402  9401  20480       16:40 node /Applications/Claude.app/Contents/Resources/app/cli.js
 9401  9301  51200           - /Applications/Claude.app/Contents/MacOS/claude
 9301   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
`;

// Sibling nested roots: root A (9001) has a node child (9002), which itself
// has TWO claude-rooted children — B1 (9003, unparseable etime) and B2
// (9101, valid etime). B1's subtree (B1 + its own node child 9004) must fold
// into A's rssMb; B2 remains its own separate entry AND must still be
// excluded from A's walk (no double-counting regression against the
// existing PS_TREE_NESTED_ROOTS behaviour).
const PS_TREE_SIBLING_NESTED_ROOTS = `  PID  PPID    RSS     ELAPSED COMM
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001  51200       58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
 9003  9002  30720           - /Applications/Claude.app/Contents/MacOS/claude
 9004  9003  10240       16:40 node /Applications/Claude.app/Contents/Resources/app/cli.js
 9101  9002  40960    00:30:00 /Applications/Claude.app/Contents/MacOS/claude
 9102  9101  20480    00:29:00 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

// Doubly-unparseable chain: root A (9501, valid) -> child B (9502, claude-
// rooted, unparseable etime) -> grandchild C (9503, claude-rooted, ALSO
// unparseable etime) -> great-grandchild D (9504, plain node). Neither B nor
// C produces an entry of its own; A's rssMb must include A + B + C + D.
const PS_TREE_DOUBLY_UNPARSEABLE_CHAIN = `  PID  PPID    RSS     ELAPSED COMM
 9501   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9502  9501  51200           - /Applications/Claude.app/Contents/MacOS/claude
 9503  9502  25600           - /Applications/Claude.app/Contents/MacOS/claude
 9504  9503  10240    00:10:00 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

// Reverse-nested chain: valid root A (9601) -> invalid root B (9602) ->
// valid nested root C (9603). A's walk must include B's own row but STOP at
// C's boundary (C already has its own separate entry) — A's rssMb = A + B
// only, never C's subtree.
const PS_TREE_REVERSE_NESTED_CHAIN = `  PID  PPID    RSS     ELAPSED COMM
 9601   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9602  9601  51200           - /Applications/Claude.app/Contents/MacOS/claude
 9603  9602  40960    00:20:00 /Applications/Claude.app/Contents/MacOS/claude
 9604  9603  10240    00:19:00 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

// An invalid root (9801) as a SIBLING (not ancestor/descendant) of an
// unrelated valid root (9701) elsewhere in the tree — confirms stop-set
// filtering is root-identity-based, not tree-position-based. 9701's entry
// must be unaffected by 9801's presence anywhere else in the snapshot.
const PS_TREE_INVALID_ROOT_AS_UNRELATED_SIBLING = `  PID  PPID    RSS     ELAPSED COMM
 9701   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9801   501  30720           - /Applications/Claude.app/Contents/MacOS/claude
 9802  9801  10240    00:05:00 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

// Chain of two CONSECUTIVE invalid roots between two valid roots: A (9901,
// valid) -> B (9902, invalid) -> C (9903, invalid) -> D (9904, valid). A
// absorbs B's and C's own rows; D remains its own entry, and A's walk must
// stop at D's boundary.
const PS_TREE_TWO_CONSECUTIVE_INVALID_ROOTS = `  PID  PPID    RSS     ELAPSED COMM
 9901   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9902  9901  51200           - /Applications/Claude.app/Contents/MacOS/claude
 9903  9902  25600           - /Applications/Claude.app/Contents/MacOS/claude
 9904  9903  40960    00:15:00 /Applications/Claude.app/Contents/MacOS/claude
 9905  9904  10240    00:14:00 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

// Every claude-rooted PID in this fixture has an unparseable etime —
// `listAgentProcesses` must return `[]` without throwing.
const PS_TREE_ALL_ROOTS_INVALID = `  PID  PPID    RSS     ELAPSED COMM
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 2001   501  51200           - /Applications/Claude.app/Contents/MacOS/claude
 2002   501  30720           - /Applications/Claude.app/Contents/MacOS/claude
 2003  2001  10240    00:05:00 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

// ---------------------------------------------------------------------------
// listAgentProcesses
// ---------------------------------------------------------------------------

describe('listAgentProcesses', () => {
  it('returns one entry per independent claude-rooted process tree, with rssMb summing only that tree\'s own processes', () => {
    const result = listAgentProcesses(PS_TREE_TWO_ROOTS, NOW_MS);

    expect(result).toHaveLength(2);

    const rootA = result.find((entry) => entry.agentId === '9001');
    const rootB = result.find((entry) => entry.agentId === '9101');

    expect(rootA).toBeDefined();
    expect(rootB).toBeDefined();

    // Root A: 102400 + 51200 KB = 153600 KB = 150 MB.
    expect(rootA.rssMb).toBeCloseTo(150, 5);
    expect(rootA.startedAt).toBe(NOW_MS - 3600 * 1000);

    // Root B: 204800 + 20480 KB = 225280 KB = 220 MB.
    expect(rootB.rssMb).toBeCloseTo(220, 5);
    expect(rootB.startedAt).toBe(NOW_MS - 7200 * 1000);
  });

  it('excludes an unrelated node process outside any claude-rooted ancestry, proving ancestry-based (not name-substring) attribution', () => {
    const result = listAgentProcesses(PS_TREE_WITH_UNRELATED_NODE, NOW_MS);

    expect(result).toHaveLength(1);
    expect(result[0].agentId).toBe('9001');
    // Only the root (102400 KB) + its own child (51200 KB) — never the
    // unrelated node's 40960 KB.
    expect(result[0].rssMb).toBeCloseTo(150, 5);
  });

  it('returns an empty array — never throws — for an idle host with zero agent-related processes', () => {
    expect(() => listAgentProcesses(PS_TREE_IDLE, NOW_MS)).not.toThrow();
    expect(listAgentProcesses(PS_TREE_IDLE, NOW_MS)).toEqual([]);
  });

  it('returns an empty array — never throws or divides by zero — for completely empty ps output', () => {
    expect(() => listAgentProcesses(PS_TREE_EMPTY, NOW_MS)).not.toThrow();
    expect(listAgentProcesses(PS_TREE_EMPTY, NOW_MS)).toEqual([]);
  });

  it('returns an empty array — never throws — for completely garbled ps output', () => {
    expect(() => listAgentProcesses(PS_TREE_GARBLED, NOW_MS)).not.toThrow();
    expect(listAgentProcesses(PS_TREE_GARBLED, NOW_MS)).toEqual([]);
  });

  it('excludes a root whose etime field is unparseable/missing entirely, rather than fabricating a startedAt', () => {
    const result = listAgentProcesses(PS_TREE_UNPARSEABLE_ETIMES_ROOT, NOW_MS);

    expect(result).toHaveLength(1);
    expect(result[0].agentId).toBe('9001');
    expect(result.some((entry) => entry.agentId === '9201')).toBe(false);
    // No entry in the array may carry a NaN/fabricated startedAt.
    expect(result.every((entry) => Number.isFinite(entry.startedAt))).toBe(true);
  });

  it('produces a stable, deterministic order (ascending agentId) when two roots tie on rssMb and startedAt', () => {
    const result = listAgentProcesses(PS_TREE_TIED_ROOTS_DESCENDING_INPUT, NOW_MS);

    expect(result).toHaveLength(2);
    expect(result.map((entry) => entry.agentId)).toEqual(['9101', '9301']);
  });

  it('attributes a nested claude-rooted process (a sub-orchestrator inside another root\'s descendant tree) exclusively to its own nearest root, never double-counting its RSS under the ancestor root too', () => {
    const result = listAgentProcesses(PS_TREE_NESTED_ROOTS, NOW_MS);

    expect(result).toHaveLength(2);

    const rootA = result.find((entry) => entry.agentId === '9001');
    const rootB = result.find((entry) => entry.agentId === '9003');

    expect(rootA).toBeDefined();
    expect(rootB).toBeDefined();

    // Root A's tree stops at 9002 (its own node child) — it must NOT
    // descend into 9003's subtree just because 9003 happens to be a
    // descendant of A by ancestry. 102400 + 51200 KB = 153600 KB = 150 MB.
    expect(rootA.rssMb).toBeCloseTo(150, 5);

    // Root B's tree is exclusively its own: 204800 + 20480 KB = 225280 KB =
    // 220 MB — the same RSS must not also appear inside rootA.rssMb above.
    expect(rootB.rssMb).toBeCloseTo(220, 5);
    expect(rootB.startedAt).toBe(NOW_MS - 2000 * 1000);

    // No process appears in both entries' underlying totals: the sum of the
    // two entries' rssMb must equal the sum of every process's own RSS
    // exactly once (102400 + 51200 + 204800 + 20480 KB = 370 MB total) —
    // proving no double count anywhere across the two entries.
    const totalRssMb = result.reduce((sum, entry) => sum + entry.rssMb, 0);
    expect(totalRssMb).toBeCloseTo(370, 5);
  });

  // Phase 2 Critical fix: `etime`'s three width shapes ([[dd-]hh:]mm:ss)
  // each parse to the correct total-elapsed-seconds independently.
  it('parses a seconds-only (mm:ss) etime value correctly', () => {
    const result = listAgentProcesses(PS_TREE_ETIME_SECONDS_ONLY, NOW_MS);

    expect(result).toHaveLength(1);
    expect(result[0].startedAt).toBe(NOW_MS - (1 * 60 + 5) * 1000);
  });

  it('parses an hours (hh:mm:ss) etime value correctly', () => {
    const result = listAgentProcesses(PS_TREE_ETIME_HOURS, NOW_MS);

    expect(result).toHaveLength(1);
    expect(result[0].startedAt).toBe(NOW_MS - (2 * 3600 + 3 * 60 + 4) * 1000);
  });

  it('parses a days (dd-hh:mm:ss) etime value correctly', () => {
    const result = listAgentProcesses(PS_TREE_ETIME_DAYS, NOW_MS);

    expect(result).toHaveLength(1);
    expect(result[0].startedAt).toBe(NOW_MS - (3 * 86400 + 4 * 3600 + 5 * 60 + 6) * 1000);
  });

  it('excludes a root whose etime value is malformed (not the [[dd-]hh:]mm:ss grammar), rather than fabricating a startedAt', () => {
    const result = listAgentProcesses(PS_TREE_ETIME_MALFORMED, NOW_MS);

    expect(result).toEqual([]);
  });

  // Verified-Critical regression: on a real macOS host, the Claude DESKTOP
  // app's main process binary path ends in capitalized `/Claude`, distinct
  // from the Claude Code CLI's lowercase `/claude`. A case-insensitive root
  // pattern previously matched both, so the desktop app's (much larger)
  // Electron process tree could be nominated as an "agent" to pause instead
  // of a real sub-agent. `AGENT_ROOT_COMM_PATTERN` must stay case-sensitive.
  it('does not attribute a capitalized `.../Claude` desktop-app process as an agent root, while a genuine lowercase `.../claude` root is still attributed', () => {
    const result = listAgentProcesses(PS_TREE_DESKTOP_APP_AND_CLI_ROOT, NOW_MS);

    expect(result).toHaveLength(1);
    expect(result[0].agentId).toBe('9001');
    expect(result.some((entry) => entry.agentId === '47960')).toBe(false);

    // The desktop app's huge RSS (819200 + 409600 KB) must not leak into the
    // CLI root's tree either — only 9001's own 102400 + 51200 KB = 150 MB.
    expect(result[0].rssMb).toBeCloseTo(150, 5);
  });

  // RED: a nested root whose own `etime` is unparseable must still
  // have its subtree folded into its ancestor root's rssMb, not dropped.
  it('attributes a nested claude-rooted subtree to its ancestor root when the nested root\'s own etime is unparseable', () => {
    const result = listAgentProcesses(PS_TREE_NESTED_ROOT_UNPARSEABLE_ETIME, NOW_MS);

    // B (9401) produces no entry of its own — the existing "don't fabricate
    // a startedAt" behaviour for an unparseable-etime root is preserved.
    expect(result.some((entry) => entry.agentId === '9401')).toBe(false);

    // A's entry must fold B's whole subtree (B + C) into its own rssMb —
    // not just A's own RSS. (102400 + 51200 + 20480) KB = 174080 KB = 170 MB.
    const rootA = result.find((entry) => entry.agentId === '9301');
    expect(rootA).toBeDefined();
    expect(rootA.rssMb).toBeCloseTo(170, 5);
  });

  // RED: order-independence — the invalid root's `ps` line (and its
  // child's) appear BEFORE the valid ancestor root's line in the raw input.
  // The fix must not depend on processing roots in `ps` output order or in
  // `entries`-accumulated-so-far order. Same assertion shape as the test
  // directly above, just with the lines reordered.
  it('attributes a nested claude-rooted subtree to its ancestor root when the nested root\'s own etime is unparseable, regardless of ps line order', () => {
    const result = listAgentProcesses(PS_TREE_NESTED_ROOT_UNPARSEABLE_ETIME_REORDERED, NOW_MS);

    expect(result.some((entry) => entry.agentId === '9401')).toBe(false);

    const rootA = result.find((entry) => entry.agentId === '9301');
    expect(rootA).toBeDefined();
    expect(rootA.rssMb).toBeCloseTo(170, 5);
  });

  // RED: sibling nested roots under the same parent — one invalid
  // (folds into the ancestor), one valid (stays its own entry, and is still
  // excluded from the ancestor's walk).
  it('folds an invalid sibling nested root into its ancestor while a valid sibling nested root remains its own separate, non-double-counted entry', () => {
    const result = listAgentProcesses(PS_TREE_SIBLING_NESTED_ROOTS, NOW_MS);

    expect(result.some((entry) => entry.agentId === '9003')).toBe(false);

    const rootA = result.find((entry) => entry.agentId === '9001');
    const rootB2 = result.find((entry) => entry.agentId === '9101');

    expect(rootA).toBeDefined();
    expect(rootB2).toBeDefined();

    // A: 102400 (A) + 51200 (node child) + 30720 (B1) + 10240 (B1's child)
    // KB = 194560 KB = 190 MB. Must NOT include B2's subtree.
    expect(rootA.rssMb).toBeCloseTo(190, 5);

    // B2: 40960 + 20480 KB = 61440 KB = 60 MB — its own entry, untouched.
    expect(rootB2.rssMb).toBeCloseTo(60, 5);

    // No double count: every process's RSS is attributed exactly once.
    // 102400+51200+30720+10240+40960+20480 KB = 256000 KB = 250 MB.
    const totalRssMb = result.reduce((sum, entry) => sum + entry.rssMb, 0);
    expect(totalRssMb).toBeCloseTo(250, 5);
  });

  // RED: doubly-unparseable chain — two consecutive claude-rooted
  // processes with unparseable etimes, neither producing its own entry; the
  // valid ancestor root's rssMb must absorb both plus their descendant.
  it('folds a chain of two consecutive unparseable-etime nested roots into the valid ancestor root', () => {
    const result = listAgentProcesses(PS_TREE_DOUBLY_UNPARSEABLE_CHAIN, NOW_MS);

    expect(result.some((entry) => entry.agentId === '9502')).toBe(false);
    expect(result.some((entry) => entry.agentId === '9503')).toBe(false);

    const rootA = result.find((entry) => entry.agentId === '9501');
    expect(rootA).toBeDefined();
    // 102400 + 51200 + 25600 + 10240 KB = 189440 KB = 185 MB.
    expect(rootA.rssMb).toBeCloseTo(185, 5);
    expect(result).toHaveLength(1);
  });

  // RED: reverse-nested chain — valid root -> invalid root -> valid
  // nested root. The outer root's walk must include the invalid root's own
  // row but STOP at the inner valid root's boundary, not swallow its subtree.
  it('stops an ancestor root\'s walk at a nested valid root\'s boundary, even when an invalid root sits between them', () => {
    const result = listAgentProcesses(PS_TREE_REVERSE_NESTED_CHAIN, NOW_MS);

    expect(result.some((entry) => entry.agentId === '9602')).toBe(false);

    const rootA = result.find((entry) => entry.agentId === '9601');
    const rootC = result.find((entry) => entry.agentId === '9603');

    expect(rootA).toBeDefined();
    expect(rootC).toBeDefined();

    // A: 102400 (A) + 51200 (B, invalid, folded in) KB = 153600 KB = 150 MB.
    // Must NOT include C's subtree.
    expect(rootA.rssMb).toBeCloseTo(150, 5);

    // C: 40960 + 10240 KB = 51200 KB = 50 MB — its own entry, untouched.
    expect(rootC.rssMb).toBeCloseTo(50, 5);
  });

  // RED: an invalid root as a SIBLING (not ancestor/descendant) of an
  // unrelated valid root elsewhere in the tree. Confirms stop-set filtering
  // is root-identity-based, not tree-position-based.
  it('leaves an unrelated valid root\'s entry unaffected by an invalid root elsewhere in the tree', () => {
    const result = listAgentProcesses(PS_TREE_INVALID_ROOT_AS_UNRELATED_SIBLING, NOW_MS);

    expect(result.some((entry) => entry.agentId === '9801')).toBe(false);

    const rootA = result.find((entry) => entry.agentId === '9701');
    expect(rootA).toBeDefined();
    // 9701 has no children of its own — just its own 102400 KB = 100 MB.
    expect(rootA.rssMb).toBeCloseTo(100, 5);
  });

  // RED: a chain of TWO consecutive invalid roots between two valid
  // roots — the outer valid root absorbs both invalid roots' own rows, but
  // still stops at the inner valid root's boundary.
  it('folds a chain of two consecutive invalid roots into the outer valid root while the inner valid root remains its own entry', () => {
    const result = listAgentProcesses(PS_TREE_TWO_CONSECUTIVE_INVALID_ROOTS, NOW_MS);

    expect(result.some((entry) => entry.agentId === '9902')).toBe(false);
    expect(result.some((entry) => entry.agentId === '9903')).toBe(false);

    const rootA = result.find((entry) => entry.agentId === '9901');
    const rootD = result.find((entry) => entry.agentId === '9904');

    expect(rootA).toBeDefined();
    expect(rootD).toBeDefined();

    // A: 102400 (A) + 51200 (B) + 25600 (C) KB = 179200 KB = 175 MB.
    expect(rootA.rssMb).toBeCloseTo(175, 5);

    // D: 40960 + 10240 KB = 51200 KB = 50 MB — its own entry, untouched.
    expect(rootD.rssMb).toBeCloseTo(50, 5);
  });

  // RED: every claude-rooted PID in the fixture has an unparseable
  // etime — `listAgentProcesses` must return `[]` without throwing.
  it('returns an empty array — never throws — when every claude-rooted PID has an unparseable etime', () => {
    expect(() => listAgentProcesses(PS_TREE_ALL_ROOTS_INVALID, NOW_MS)).not.toThrow();
    expect(listAgentProcesses(PS_TREE_ALL_ROOTS_INVALID, NOW_MS)).toEqual([]);
  });
});
