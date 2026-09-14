// Phase 0 — OUTER ACCEPTANCE TEST: a minority-garbled `ps`
// snapshot must not reap an individual live agent.
//
// THE BUG THIS PINS (currently RED)
//   `classifyProcessSnapshotShape` (`./recon.mjs`) judges a `ps` snapshot
//   `'readable'` once a MAJORITY of its non-header lines parse
//   (`READABLE_ROW_MAJORITY = 0.5`). That threshold protects the fleet from a
//   drift that mangles the whole table (its case) — it says nothing about
//   a snapshot where nearly every row parses and exactly one does not,
//   because that snapshot is still `'readable'` by a wide margin.
//
//   `listAgentProcesses` drops an unparseable row silently (`if (!match)
//   continue`, `./recon.mjs`) with no per-row provenance signal. If the
//   dropped row happens to belong to a genuinely live agent's own claude-
//   rooted process, that agent's pid is simply absent from the returned
//   `liveProcesses` array — indistinguishable, from
//   `classifyLedgerEntryLiveness`'s point of view, from an agent that
//   actually exited. Rule 4 in `./reconciliation.mjs` then returns
//   `CONFIRMED_DEAD` / `PID_ABSENT`, and `cli.mjs`'s `reconcileLedgers` reaps
//   the ledger entry for a process that is still running.
//
// THE INTEGRATION SEAM THIS TEST USES, AND WHY
//   The real end-to-end reap entry point is `cli.mjs`'s `reconcileLedgers`,
//   but it is a private, unexported async function reachable only by
//   spawning the whole CLI under lock/file-IO side effects (see
//   `./ledger-reconciliation-outer-acceptance.jest.spec.mjs` for that much
//   heavier idiom, reserved for the ticket that owns the beat-wiring layer,
//). This ticket's fix (per its Build Plan) lands entirely inside
//   `recon.mjs` and `reconciliation.mjs`, so this test composes the same
//   three functions `reconcileLedgers` itself calls, in the same order, and
//   ends with the identical reap predicate `reconcileLedgers` uses
//   (`classifyLedgerEntryLiveness(...).status === LIVENESS_STATUS.CONFIRMED_DEAD`,
//   compare `cli.mjs`'s `isConfirmedDead` inside `reconcileLedgers`):
//
//     classifyProcessSnapshotShape(psText)  -- must be 'readable' (precondition)
//               |
//     listAgentProcesses(psText, now)       -- the row-drop site, its target
//               |
//     classifyLedgerEntryLiveness(entry, liveProcesses, snapshotAt)
//
//   This is the same composition `./reconciliation.jest.spec.mjs`'s own
//   "consumes listAgentProcesses output verbatim" test already uses, and is
//   the integration seam this file's task brief names as the acceptable
//   fallback when `reconcileLedgers` itself is too deeply coupled to
//   CLI/filesystem side effects to call directly.
//
// FIXTURE DISCIPLINE: every pid and timestamp is a LITERAL, never derived
// from this test process's own `process.pid` / `Date.now()` — matching
// `./reconciliation.jest.spec.mjs`'s stated convention.
//
// TARGET (POST-FIX) BEHAVIOUR ASSERTED HERE, NOT TODAY'S BUGGY BEHAVIOUR:
// this test asserts the entry classifies `UNKNOWN` and survives. Today's
// code classifies it `CONFIRMED_DEAD` / `PID_ABSENT` instead, so this test is
// RED until Phases 1-3 of the Build Plan land.

import { classifyProcessSnapshotShape, listAgentProcesses } from './recon.mjs';
import { LIVENESS_REASON, LIVENESS_STATUS, classifyLedgerEntryLiveness } from './reconciliation.mjs';

/** The instant the beat's `ps` snapshot was taken. */
const SNAPSHOT_AT = Date.parse('2026-08-29T12:00:00Z');

/**
 * A claude-rooted orchestrator process that is genuinely running as of
 * `SNAPSHOT_AT`. Its ledger identity (`pid` + `pidStartedAt`) was recorded by
 * an earlier, successful beat, before this beat's `ps` collection degraded.
 */
const LIVE_PID = 9001;
const LIVE_STARTED_AT = Date.parse('2026-08-29T09:00:00Z');

/** A dibs entry carrying an already-resolved identity pair. */
function dibsEntry(pid, pidStartedAt) {
  return {
    orchestratorId: 'orchestrator-a',
    declaredAt: SNAPSHOT_AT - 60 * 1000,
    firstDeclaredAt: LIVE_STARTED_AT + 5 * 1000,
    desiredAgents: 2,
    pid,
    pidStartedAt,
  };
}

/**
 * A majority-readable `ps -Ao pid,ppid,rss,etime,comm` snapshot in which the
 * LIVE agent's own root row is the one garbled line. Of the four body lines,
 * three parse cleanly (`launchd`, an unrelated daemon, an unrelated user
 * process) and one does not: the row that, uncorrupted, would have read
 * ` 9001     1 102400    03:00:00 /Applications/Claude.app/Contents/MacOS/claude`
 * — matching `LIVE_PID`'s recorded identity exactly — instead has its `rss`
 * field garbled (`XXXXXX`, not `\d+`), so it fails `PS_LINE_WITH_ETIME_PATTERN`
 * entirely and is dropped by `listAgentProcesses`'s `if (!match) continue`.
 *
 * PID COLUMN DELIBERATELY LEFT READABLE. Garbling the pid field itself (as an
 * earlier draft of this fixture did) makes the row's pid unrecoverable, which
 * `PS_LEADING_PID_PATTERN` (`./recon.mjs`) cannot repair either — that is the
 * `pid: null` residual Convergence Analysis edge case #2 explicitly scoped
 * OUT of this ticket's fix (this design's documented residual: an unrecoverable
 * pid stays unrecoverable). This fixture instead matches the Investigation's
 * confirmed in-scope example — "a row where only [a non-pid column] fails but
 * pid/ppid/comm are readable — pid should be recoverable" — by corrupting
 * `rss` (a column, like `ppid`, whose `\d+` capture group must fail to match
 * or the whole line still parses; unlike `etime`'s permissive `\S+` capture,
 * digit-only fields can't absorb garbage without breaking the full-line
 * match). Rule 3.5 in `./reconciliation.mjs` recovers `LIVE_PID` from
 * `listAgentProcesses`'s additive `unparseableRows` signal and this test
 * exercises exactly that path.
 *
 * 3 of 4 body lines parse: 75% clears `READABLE_ROW_MAJORITY` (50%) by a wide
 * margin, so this snapshot classifies `'readable'` and is passed through to
 * the reap sweep today — it is not caught by the total-reap guard.
 */
function minorityGarbledPsText() {
  return [
    '  PID  PPID    RSS     ELAPSED COMM',
    '    1     0    512 1-03:46:39 /sbin/launchd',
    ' 8000     1  10240    00:05:00 /usr/sbin/some-daemon',
    ' 9001     1 XXXXXX    03:00:00 /Applications/Claude.app/Contents/MacOS/claude',
    ' 8500     1   2048    00:10:00 /usr/bin/some-other-proc',
    '',
  ].join('\n');
}

describe('outer acceptance — minority-garbled snapshot must not reap a live agent', () => {
  it('does not reap a ledger entry whose ps row failed to parse, when the surrounding table is majority-readable', () => {
    const psText = minorityGarbledPsText();

    // Precondition: the snapshot passes the total-reap guard. If this
    // assertion fails, the fixture is not exercising the minority-garble gap
    // at all — it is exercising the already-closed total-reap case instead.
    expect(classifyProcessSnapshotShape(psText)).toBe('readable');

    const liveProcesses = listAgentProcesses(psText, SNAPSHOT_AT);

    // The garbled row hid the live agent from discovery entirely — this is
    // the row-drop site itself, proven independently of the classifier.
    expect(liveProcesses.find((candidate) => candidate.agentId === String(LIVE_PID))).toBeUndefined();

    const entry = dibsEntry(LIVE_PID, LIVE_STARTED_AT);
    const verdict = classifyLedgerEntryLiveness(entry, liveProcesses, SNAPSHOT_AT);

    // THE TARGET BEHAVIOUR (post-fix): not decidable from this evidence —
    // defer to the ledger's own TTL rather than issue a death certificate to
    // a process that is still running.
    expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(verdict.reason).not.toBe(LIVENESS_REASON.PID_ABSENT);

    // The exact predicate `cli.mjs`'s `reconcileLedgers` uses to decide
    // whether a record is reaped (`isConfirmedDead`, cli.mjs ~line 2493).
    // Composing it here proves the end-to-end reap decision, not merely the
    // classifier's status field in isolation.
    const wouldBeReapedByReconcileLedgers = verdict.status === LIVENESS_STATUS.CONFIRMED_DEAD;
    expect(wouldBeReapedByReconcileLedgers).toBe(false);
  });
});
