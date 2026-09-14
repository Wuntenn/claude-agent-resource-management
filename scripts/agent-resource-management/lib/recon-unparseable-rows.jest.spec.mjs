// Phase 1 RED unit tests — per-row provenance signal in
// `listAgentProcesses` (`./recon.mjs`).
//
// THE GAP THIS PINS
//   `listAgentProcesses`'s row loop drops any `ps` body line that fails
//   `PS_LINE_WITH_ETIME_PATTERN` via a bare `if (!match) continue`
//   (`recon.mjs` ~line 714). Today that drop carries zero provenance: the
//   caller cannot tell "no row for this pid" (genuinely dead) apart from "a
//   row existed and failed to parse" (possibly a live agent whose row got
//   mangled by the same minority-garble drift the outer acceptance
//   test at `./reconciliation-minority-garble.jest.spec.mjs` pins).
//
// THE SIGNAL UNDER TEST (per the Build Plan's Phase 1 acceptance criteria)
//   `listAgentProcesses` must ADDITIVELY surface which rows failed to parse,
//   and — where a pid is recoverable from the row even though the full line
//   didn't match — which pid it carried. The Build Plan's suggested shape is
//   an `unparseableRows: Array<{ pid: number|null, rawLine: string }>` field
//   attached to the SAME array `listAgentProcesses` already returns, so none
//   of its 4 existing `cli.mjs` callers — which only ever iterate/map/filter
//   the array's own elements — observe any change to what they already
//   consume. This file therefore reads the signal as
//   `listAgentProcesses(psText, now).unparseableRows`, a property on the
//   returned array itself, not a second return value or a reshaped envelope.
//
// `recon.mjs` does not export any such signal today — every test below is
// red by construction (either `unparseableRows` is `undefined` or the
// property doesn't exist at all) until Phase 1's implementer adds it. Do not
// add a stub implementation to force green.
//
// FIXTURE DISCIPLINE: literal pids/timestamps, `ps -Ao pid,ppid,rss,etime,comm`
// column order, matching `./recon-per-agent.jest.spec.mjs`'s existing idiom.

import { listAgentProcesses } from './recon.mjs';

// A fixed `now` so any startedAt/derived values stay deterministic.
const NOW_MS = Date.parse('2026-08-04T12:00:00Z');

// ---------------------------------------------------------------------------
// Fixtures — ps -Ao pid,ppid,rss,etime,comm style output.
// ---------------------------------------------------------------------------

// A fully clean, majority-and-entirely-readable table: every body line
// parses. Baseline for the "additive, not a breaking reshape" assertion.
const PS_TREE_FULLY_CLEAN = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001  51200       58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

// One row with a garbled RSS column (`N/A` instead of digits) — this breaks
// `PS_LINE_WITH_ETIME_PATTERN`'s third `(\d+)` group, so the WHOLE line fails
// to match and hits the `if (!match) continue` drop site, exactly like every
// other row-drop in this file. Unlike the outer acceptance test's fixture
// (which garbles the pid itself), this row's leading `pid`/`ppid` fields
// are perfectly readable — the pid is recoverable even though the full row
// is not.
const PS_TREE_RECOVERABLE_PID_GARBLED_RSS = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9001   501    N/A    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
`;

// One row whose PID FIELD ITSELF is unparseable (`XXXX`, not `\d+`) — the
// same garble shape as the outer acceptance test's fixture. No pid can be
// recovered from this row at all; it must surface as an "unattributed"
// unparseable-row signal (`pid: null`), never thrown away silently and never
// fabricated as some other number.
const PS_TREE_UNRECOVERABLE_PID = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 XXXX     1 102400    03:00:00 /Applications/Claude.app/Contents/MacOS/claude
`;

// Multiple garbled rows in one table: one pid-recoverable (garbled RSS,
// pid 9001), one pid-unrecoverable (garbled pid field), interleaved among
// otherwise-clean, fully-parseable rows (including two valid claude roots,
// 8001 and 8002) — proves the new signal doesn't disturb the parsed rows'
// contents or ordering.
const PS_TREE_MULTIPLE_GARBLED_ROWS = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
 8002   501 102400    02:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9001   501    N/A    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 XXXX     1  51200    03:00:00 /Applications/Claude.app/Contents/MacOS/claude
 8001   501  30720    00:30:00 /Applications/Claude.app/Contents/MacOS/claude
`;

// Edge case #5 (Convergence Analysis): the SAME pid (9001) appears both in a
// row that parses perfectly fine AND in a separately-garbled row whose
// recoverable-pid prefix happens to collide with it. The valid row must win
// — 9001 must never also appear in `unparseableRows`, even though a second,
// garbled line elsewhere in the table carries the same leading digits.
const PS_TREE_DUPLICATE_PID_VALID_ROW_WINS = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9001   501    N/A    00:45:00 /Applications/Claude.app/Contents/MacOS/claude
`;

// Edge case #9 (Convergence Analysis): two ADJACENT garbled rows, each with
// its own distinct recoverable pid (9101, then 9102 immediately below it).
// A naive/greedy pid-recovery approach operating on raw text could bleed
// digits from one malformed line into its neighbour; this fixture proves
// each row's recovered pid is attributed independently and never leaks into
// the other row's entry.
const PS_TREE_ADJACENT_GARBLED_ROWS = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9101   501    N/A    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9102   501    N/A    00:45:00 /Applications/Claude.app/Contents/MacOS/claude
`;

// ---------------------------------------------------------------------------
// listAgentProcesses(...).unparseableRows
// ---------------------------------------------------------------------------

describe('listAgentProcesses — additive unparseableRows provenance signal', () => {
  it('surfaces a recoverable pid for a row that fails full parsing but whose pid field is readable', () => {
    const result = listAgentProcesses(PS_TREE_RECOVERABLE_PID_GARBLED_RSS, NOW_MS);

    // The row is still dropped from the parsed-agent list — 9001 never
    // produces a normal entry (its RSS was unparseable, so no tree can be
    // computed for it).
    expect(result.some((entry) => entry.agentId === '9001')).toBe(false);

    expect(result.unparseableRows).toBeDefined();
    expect(result.unparseableRows).toHaveLength(1);
    expect(result.unparseableRows[0].pid).toBe(9001);
    expect(result.unparseableRows[0].rawLine).toContain('9001');
    expect(result.unparseableRows[0].rawLine).toContain('N/A');
  });

  it('surfaces an unattributed (pid: null) signal when the pid field itself is unparseable', () => {
    const result = listAgentProcesses(PS_TREE_UNRECOVERABLE_PID, NOW_MS);

    expect(result.unparseableRows).toBeDefined();
    expect(result.unparseableRows).toHaveLength(1);
    expect(result.unparseableRows[0].pid).toBeNull();
    expect(result.unparseableRows[0].rawLine).toContain('XXXX');
  });

  it('returns an empty (or absent) unparseableRows for a fully clean, entirely-readable table — proving the change is additive, not a breaking reshape', () => {
    const result = listAgentProcesses(PS_TREE_FULLY_CLEAN, NOW_MS);

    // The existing element shape is completely undisturbed.
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({
      agentId: '9001',
      rssMb: 150,
      startedAt: NOW_MS - 3600 * 1000,
    });

    expect(result.unparseableRows ?? []).toEqual([]);
  });

  it('handles multiple garbled rows (recoverable and unrecoverable) without affecting the parsed rows\' contents or order', () => {
    const result = listAgentProcesses(PS_TREE_MULTIPLE_GARBLED_ROWS, NOW_MS);

    // The two valid claude roots still parse, in the function's existing
    // stable ascending-agentId order — completely unaffected by the two
    // garbled rows interleaved between them in the raw text.
    expect(result.map((entry) => entry.agentId)).toEqual(['8001', '8002']);

    expect(result.unparseableRows).toHaveLength(2);

    const recoverable = result.unparseableRows.find((row) => row.pid === 9001);
    const unattributed = result.unparseableRows.find((row) => row.pid === null);

    expect(recoverable).toBeDefined();
    expect(recoverable.rawLine).toContain('N/A');

    expect(unattributed).toBeDefined();
    expect(unattributed.rawLine).toContain('XXXX');
  });

  // Convergence Analysis edge case #5 — invariant: valid-row-wins.
  it('never surfaces a pid in unparseableRows when that same pid also has a successfully-parsed row (valid-row-wins invariant)', () => {
    const result = listAgentProcesses(PS_TREE_DUPLICATE_PID_VALID_ROW_WINS, NOW_MS);

    // The valid row produces a normal, correctly-attributed entry.
    expect(result.some((entry) => entry.agentId === '9001')).toBe(true);

    // The garbled row (whose recoverable prefix is also 9001) must NOT
    // additionally list 9001 in unparseableRows — no pid may appear in
    // both places at once.
    const parsedPids = new Set(result.map((entry) => Number(entry.agentId)));
    const unparseablePids = (result.unparseableRows ?? [])
      .map((row) => row.pid)
      .filter((pid) => pid !== null);

    for (const pid of unparseablePids) {
      expect(parsedPids.has(pid)).toBe(false);
    }
  });

  // Convergence Analysis edge case #9 — adjacent-row non-cross-contamination.
  it('attributes each of two adjacent garbled rows its own recovered pid, without either leaking into the other\'s entry', () => {
    const result = listAgentProcesses(PS_TREE_ADJACENT_GARBLED_ROWS, NOW_MS);

    expect(result.unparseableRows).toHaveLength(2);

    const first = result.unparseableRows.find((row) => row.rawLine.includes('01:00:00'));
    const second = result.unparseableRows.find((row) => row.rawLine.includes('00:45:00'));

    expect(first).toBeDefined();
    expect(second).toBeDefined();

    // Each row's recovered pid matches ONLY its own line — 9101 must never
    // be attributed to the 00:45:00 row, and 9102 must never be attributed
    // to the 01:00:00 row.
    expect(first.pid).toBe(9101);
    expect(second.pid).toBe(9102);

    // Neither garbled row produced a normal parsed entry.
    expect(result.some((entry) => entry.agentId === '9101')).toBe(false);
    expect(result.some((entry) => entry.agentId === '9102')).toBe(false);
  });

  // Pre-PR review (Medium): the pid-recovery pattern originally required a
  // trailing `\s+`, so a row TRUNCATED to its own pid and nothing else
  // recovered `pid: null` and the entry it named was still reaped
  // `pid-absent`. That is the simplest garble the whole signal exists to
  // catch — a write cut short mid-row — so it is pinned here rather than left
  // to the pattern's shape.
  it('recovers the pid from a row truncated to the pid alone, with no trailing column at all', () => {
    const result = listAgentProcesses(
      `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
 9301`,
      NOW_MS,
    );

    expect(result.unparseableRows).toHaveLength(1);
    expect(result.unparseableRows[0].pid).toBe(9301);
    expect(result.unparseableRows[0].rawLine).toBe('9301');
  });

  // The converse, and the reason the boundary is a LOOKAHEAD rather than a
  // dropped anchor: a leading token that merely BEGINS with digits is not a
  // pid, and must recover nothing rather than donate its numeric prefix to
  // whichever ledger record happens to carry that pid.
  it('recovers nothing from a leading token that only begins with digits', () => {
    const result = listAgentProcesses(
      `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
9301abc   501   1024 00:10:00 /some/thing`,
      NOW_MS,
    );

    expect(result.unparseableRows).toHaveLength(1);
    expect(result.unparseableRows[0].pid).toBeNull();
  });
});
