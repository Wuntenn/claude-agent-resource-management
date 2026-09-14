// Phase 2 unit tests for — "Own-spawned-only eviction authorization".
// `recon.mjs` exports `isEvictionAuthorized` and this Phase implements the
// full authorization decision (all conditions below); every test in this
// file is green. What Phase 3 still defers is the actual SIGTERM/SIGKILL
// execution against an authorized candidate — this file only covers the
// pure authorization decision, not eviction execution.
//
// WHY THIS LIVES IN `recon.mjs` RATHER THAN A NEW MODULE: the Build Plan
// left the module choice to the implementer, preferring `recon.mjs` if it
// avoids exporting `collectAncestrySet` (currently a private helper in that
// file, shared today by `attributeAgentProcesses` and `listAgentProcesses`).
// Keeping the new function inside `recon.mjs` lets it call that private
// helper directly rather than growing recon.mjs's exported surface just for
// this. This spec file itself is new (matching the Convergence Analysis's
// suggested file name) rather than appended to `recon.jest.spec.mjs`, to
// keep this ticket's red tests isolated from Phase 1's existing suite — but
// it imports from the SAME `./recon.mjs` module.
//
// Contract under test:
//   isEvictionAuthorized({ psTreeOutput, orchestratorPid, orchestratorIsLive, candidatePid })
//     -> boolean
//
// Design decisions this contract encodes (see the Convergence Analysis edge
// cases in the issue):
//
//   - PURE FUNCTION, no I/O. `psTreeOutput` is a plain `ps -Ao
//     pid,ppid,comm`-style string — the SAME shape `attributeAgentProcesses`
//     already parses (this file's `PS_LINE_PATTERN` + `AGENT_ROOT_COMM_PATTERN`
//     — reused directly, not reinvented). The function never shells out and
//     never reads the coordination file itself.
//
//   - `orchestratorIsLive` is a caller-supplied boolean, not something this
//     function derives — the caller is expected to have already checked the
//     orchestrator's own dibs/ledger entry (via `readDibs`) for liveness
//     before calling this function. Passing anything other than the literal
//     `true` (missing, `false`, `undefined`, a truthy-but-non-boolean value)
//     must fail closed — this function must NEVER default to permissive when
//     it cannot itself prove the caller is a live, declared orchestrator.
//
//   - Ancestry is proven ONLY from the `psTreeOutput` snapshot passed in this
//     call. The function does not cache, memoize, or compare across calls —
//     given a fresh snapshot where a PID has been reused by an unrelated
//     process (or reparented), it must authorize (or refuse) based on THAT
//     snapshot alone. Avoiding stale snapshots is the caller's job (sample
//     fresh immediately before use); this function just has to be correct
//     given whatever snapshot it receives.
//
//   - Ancestry proves descent, not "is agent work". The candidate PID itself
//     must ALSO match `AGENT_ROOT_COMM_PATTERN` (i.e. only real agent-root
//     processes — comm ending in `/claude`, case-sensitive — are
//     authorizable). An arbitrary descendant helper process (e.g. a bare
//     `node` child that is not itself a claude-rooted process) must be
//     REJECTED even though it is a genuine descendant of the orchestrator.
//     This is the safer default: it does not rely on every caller of
//     `isEvictionAuthorized` to already have filtered its candidate list to
//     agent roots (today `listAgentProcesses` does, but this function must
//     not silently depend on that always being true upstream).
//
//   - An unreachable/broken ancestry chain (a candidate whose `ppid` is
//     never actually reachable by walking children from the orchestrator's
//     own PID — e.g. a reparented or orphaned intermediate process) must
//     fail closed. There is no fallback path that treats "can't prove it"
//     as "allow it".

import { isEvictionAuthorized } from './recon.mjs';

// ---------------------------------------------------------------------------
// Fixtures — `ps -Ao pid,ppid,comm`-style output (same shape as
// `attributeAgentProcesses`'s `PS_TREE_CLAUDE_LAUNCHED_TREE` fixture in
// recon.jest.spec.mjs).
// ---------------------------------------------------------------------------

// Orchestrator 501 with:
//   - a direct agent-root child (502)
//   - a transitive agent-root descendant several levels deep, through two
//     non-agent-root intermediates (503 -> 504 -> 505)
//   - a non-agent-root descendant (506, a bare `node` helper) that must NOT
//     be authorizable even though it is a genuine descendant
// Sibling orchestrator 601 (also under launchd, NOT under 501) with its own
// agent-root child (602) — proves ancestry, not "anywhere on the host",
// gates authorization.
const PS_TREE_WITH_ORCHESTRATOR_AND_SIBLING = `  PID  PPID COMM
    1     0 /sbin/launchd
  501     1 node /Users/dev/orchestrators/orchestrator-a.js
  502   501 /Applications/Claude.app/Contents/MacOS/claude
  503   501 /bin/bash
  504   503 node /Applications/Claude.app/Contents/Resources/app/spawn-wrapper.js
  505   504 /Applications/Claude.app/Contents/MacOS/claude
  506   501 node /Applications/Claude.app/Contents/Resources/app/cli-helper.js
  601     1 node /Users/dev/orchestrators/orchestrator-b.js
  602   601 /Applications/Claude.app/Contents/MacOS/claude
`;

// A candidate (505) whose `ppid` (999) is never itself reachable from the
// orchestrator (501) — 999 does not appear as a `pid` anywhere in this
// output, so no chain of edges connects 501 down to 505. Simulates a
// reparented/orphaned intermediate that breaks the ancestry proof.
const PS_TREE_WITH_BROKEN_CHAIN = `  PID  PPID COMM
    1     0 /sbin/launchd
  501     1 node /Users/dev/orchestrators/orchestrator-a.js
  502   501 /bin/bash
  505   999 /Applications/Claude.app/Contents/MacOS/claude
`;

// Two snapshots of the SAME PID (506) taken at different times, simulating
// PID reuse: in the OLD snapshot 506 is a genuine descendant agent root of
// orchestrator 501; in the NEW snapshot the OS has reassigned PID 506 to an
// unrelated, unrelated-ancestor process entirely.
const PS_TREE_PID_REUSE_OLD = `  PID  PPID COMM
    1     0 /sbin/launchd
  501     1 node /Users/dev/orchestrators/orchestrator-a.js
  506   501 /Applications/Claude.app/Contents/MacOS/claude
`;
const PS_TREE_PID_REUSE_NEW = `  PID  PPID COMM
    1     0 /sbin/launchd
  501     1 node /Users/dev/orchestrators/orchestrator-a.js
  700     1 /usr/sbin/some-daemon
  506   700 /usr/bin/unrelated-service
`;

// The orchestrator's OWN process (501) has a `comm` that matches
// AGENT_ROOT_COMM_PATTERN (ends in `/claude`) — i.e. the orchestrator itself
// looks like a valid agent-root process. `collectAncestrySet([orchestratorPid],
// ...)` includes the seed PID (501) in its returned set by construction, so
// if the self-pid guard (`candidatePid !== orchestratorPid`) were ever
// dropped, condition 2 (ancestry reachable) AND condition 3 (candidate's own
// comm matches AGENT_ROOT_COMM_PATTERN) would BOTH be satisfied for
// candidatePid === '501' — proving this guard is load-bearing, not
// redundant with the other conditions.
const PS_TREE_ORCHESTRATOR_LOOKS_LIKE_AGENT_ROOT = `  PID  PPID COMM
    1     0 /sbin/launchd
  501     1 /Applications/Claude.app/Contents/MacOS/claude
  502   501 /Applications/Claude.app/Contents/MacOS/claude
`;

// ---------------------------------------------------------------------------
// isEvictionAuthorized
// ---------------------------------------------------------------------------

describe('isEvictionAuthorized', () => {
  // Scenario 1 — direct child.
  it('authorizes a direct agent-root child of the orchestrator\'s own PID', () => {
    const result = isEvictionAuthorized({
      psTreeOutput: PS_TREE_WITH_ORCHESTRATOR_AND_SIBLING,
      orchestratorPid: '501',
      orchestratorIsLive: true,
      candidatePid: '502',
    });

    expect(result).toBe(true);
  });

  // Scenario 2 — transitive descendant several levels deep.
  it('authorizes a transitive agent-root descendant several levels deep', () => {
    const result = isEvictionAuthorized({
      psTreeOutput: PS_TREE_WITH_ORCHESTRATOR_AND_SIBLING,
      orchestratorPid: '501',
      orchestratorIsLive: true,
      candidatePid: '505',
    });

    expect(result).toBe(true);
  });

  // Scenario 3 — sibling orchestrator's tree.
  it("rejects a candidate belonging to a sibling orchestrator's tree", () => {
    const result = isEvictionAuthorized({
      psTreeOutput: PS_TREE_WITH_ORCHESTRATOR_AND_SIBLING,
      orchestratorPid: '501',
      orchestratorIsLive: true,
      candidatePid: '602',
    });

    expect(result).toBe(false);
  });

  // Scenario 4 — fails closed when the orchestrator's own dibs/ledger entry
  // is stale/absent. Never defaults to permissive.
  describe('when the orchestrator itself cannot be proven live', () => {
    it('rejects an otherwise-valid direct child when orchestratorIsLive is false (stale entry)', () => {
      const result = isEvictionAuthorized({
        psTreeOutput: PS_TREE_WITH_ORCHESTRATOR_AND_SIBLING,
        orchestratorPid: '501',
        orchestratorIsLive: false,
        candidatePid: '502',
      });

      expect(result).toBe(false);
    });

    it('rejects an otherwise-valid direct child when orchestratorIsLive is omitted (absent entry)', () => {
      const result = isEvictionAuthorized({
        psTreeOutput: PS_TREE_WITH_ORCHESTRATOR_AND_SIBLING,
        orchestratorPid: '501',
        candidatePid: '502',
      });

      expect(result).toBe(false);
    });
  });

  // Scenario 5 — broken/reparented ancestry chain: can't be proven a
  // descendant, must never fall back to "allow".
  it('rejects a candidate whose ancestry chain is broken (an intermediate PID is unreachable from the orchestrator)', () => {
    const result = isEvictionAuthorized({
      psTreeOutput: PS_TREE_WITH_BROKEN_CHAIN,
      orchestratorPid: '501',
      orchestratorIsLive: true,
      candidatePid: '505',
    });

    expect(result).toBe(false);
  });

  // Scenario 6 — PID reuse: authorization must reflect only the snapshot
  // passed to THIS call, never a previous call's result for the same PID.
  describe('PID reuse across two snapshots of the same PID', () => {
    it('authorizes PID 506 against the OLD snapshot, where it is a genuine descendant', () => {
      const result = isEvictionAuthorized({
        psTreeOutput: PS_TREE_PID_REUSE_OLD,
        orchestratorPid: '501',
        orchestratorIsLive: true,
        candidatePid: '506',
      });

      expect(result).toBe(true);
    });

    it('rejects PID 506 against the NEW snapshot, where the OS has reassigned it to an unrelated process', () => {
      const result = isEvictionAuthorized({
        psTreeOutput: PS_TREE_PID_REUSE_NEW,
        orchestratorPid: '501',
        orchestratorIsLive: true,
        candidatePid: '506',
      });

      expect(result).toBe(false);
    });
  });

  // Scenario 7 — ancestry proves descent, not "is agent work". The
  // candidate PID itself must also match AGENT_ROOT_COMM_PATTERN; a
  // descendant helper process that is not itself a claude-rooted process is
  // rejected, even though it IS a genuine descendant.
  it('rejects a genuine descendant PID whose own comm is not an agent-root process (e.g. a bare node helper)', () => {
    const result = isEvictionAuthorized({
      psTreeOutput: PS_TREE_WITH_ORCHESTRATOR_AND_SIBLING,
      orchestratorPid: '501',
      orchestratorIsLive: true,
      candidatePid: '506',
    });

    expect(result).toBe(false);
  });

  // Bonus — a candidate PID absent from the snapshot entirely can't be
  // proven anything; must fail closed rather than throw or default true.
  it('rejects a candidatePid that does not appear anywhere in the ps-tree snapshot', () => {
    const result = isEvictionAuthorized({
      psTreeOutput: PS_TREE_WITH_ORCHESTRATOR_AND_SIBLING,
      orchestratorPid: '501',
      orchestratorIsLive: true,
      candidatePid: '999999',
    });

    expect(result).toBe(false);
  });

  // Scenario 8 — self-eviction must never be authorized, even when the
  // orchestrator's own process would otherwise satisfy every other
  // condition: it is trivially "reachable from itself" via
  // collectAncestrySet's seed-inclusive construction, AND its own `comm`
  // matches AGENT_ROOT_COMM_PATTERN. Only the explicit
  // `candidatePid !== orchestratorPid` check closes this hole.
  it('rejects self-eviction even when the orchestrator\'s own comm matches AGENT_ROOT_COMM_PATTERN', () => {
    const result = isEvictionAuthorized({
      psTreeOutput: PS_TREE_ORCHESTRATOR_LOOKS_LIKE_AGENT_ROOT,
      orchestratorPid: '501',
      orchestratorIsLive: true,
      candidatePid: '501',
    });

    expect(result).toBe(false);
  });

  it('still authorizes a genuine agent-root child of an agent-root-looking orchestrator (control case)', () => {
    const result = isEvictionAuthorized({
      psTreeOutput: PS_TREE_ORCHESTRATOR_LOOKS_LIKE_AGENT_ROOT,
      orchestratorPid: '501',
      orchestratorIsLive: true,
      candidatePid: '502',
    });

    expect(result).toBe(true);
  });

  // Scenario 9 — defense-in-depth type coercion: PIDs must be compared as
  // strings internally (Map/Set keys parsed from psTreeOutput are always
  // strings), so a caller passing a `number` (e.g. straight from
  // `process.pid`) must still resolve correctly rather than silently
  // missing every lookup and failing closed for the wrong reason.
  it('authorizes correctly when orchestratorPid and candidatePid are passed as numbers', () => {
    const result = isEvictionAuthorized({
      psTreeOutput: PS_TREE_WITH_ORCHESTRATOR_AND_SIBLING,
      orchestratorPid: 501,
      orchestratorIsLive: true,
      candidatePid: 502,
    });

    expect(result).toBe(true);
  });

  it('still rejects numeric self-eviction (orchestratorPid === candidatePid as numbers)', () => {
    const result = isEvictionAuthorized({
      psTreeOutput: PS_TREE_ORCHESTRATOR_LOOKS_LIKE_AGENT_ROOT,
      orchestratorPid: 501,
      orchestratorIsLive: true,
      candidatePid: 501,
    });

    expect(result).toBe(false);
  });

  // Scenario 10 (whole-branch review, Low #9) — this function's own
  // test fixtures above all use the 3-column `pid,ppid,comm` shape, but its
  // ACTUAL production caller (`collectPsOutput()` in cli.mjs) shells out
  // with 5 columns: `pid,ppid,rss,etime,comm`. This must correctly extract
  // `comm` (and therefore authorize/reject exactly as it would against the
  // 3-column shape) from real 5-column input too — not by accident of an
  // end-anchored regex, but as an asserted, pinned behavior.
  describe('against real 5-column `pid,ppid,rss,etime,comm` production-shaped input (not just this file\'s 3-column fixtures)', () => {
    const PS_TREE_FIVE_COLUMN = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 node /Users/dev/orchestrators/orchestrator-a.js
  502   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
  601     1   1024 1-03:30:00 node /Users/dev/orchestrators/orchestrator-b.js
  602   601 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
`;

    it('authorizes a direct agent-root child parsed from 5-column input', () => {
      const result = isEvictionAuthorized({
        psTreeOutput: PS_TREE_FIVE_COLUMN,
        orchestratorPid: '501',
        orchestratorIsLive: true,
        candidatePid: '502',
      });

      expect(result).toBe(true);
    });

    it('still rejects a sibling orchestrator\'s child parsed from 5-column input', () => {
      const result = isEvictionAuthorized({
        psTreeOutput: PS_TREE_FIVE_COLUMN,
        orchestratorPid: '501',
        orchestratorIsLive: true,
        candidatePid: '602',
      });

      expect(result).toBe(false);
    });
  });
});
