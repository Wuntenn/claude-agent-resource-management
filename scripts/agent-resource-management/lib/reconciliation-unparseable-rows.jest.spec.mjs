// Phase 2 RED unit tests — `classifyLedgerEntryLiveness` consuming
// Phase 1's `unparseableRows` provenance signal.
//
// THE GAP THIS PINS
//   Phase 1 (`./recon-unparseable-rows.jest.spec.mjs`, landed) taught
//   `listAgentProcesses` to attach an additive `unparseableRows` array to the
//   SAME array it already returns — `listAgentProcesses(psText, now)
//   .unparseableRows`, never a second return value or a reshaped envelope.
//   Phase 2's job is to give `classifyLedgerEntryLiveness` a NEW,
//   fail-conservative lookup against that signal: when a ledger entry's `pid`
//   matches an `unparseableRows` row (recoverable pid), the verdict must be
//   `UNKNOWN` under a reason token distinct from every existing
//   `LIVENESS_REASON` value — never `CONFIRMED_DEAD`/`PID_ABSENT` (today's
//   buggy behaviour) and never `MALFORMED_PROCESS_ROW` (a different, existing
//   reason for a different situation: a row WAS found and matched, but its
//   own `startedAt` was unreadable — not "no row was found because it failed
//   to parse at all").
//
//   `reconciliation.mjs` has no such lookup today, so every "new reason"
//   assertion below is red by construction — it currently falls through to
//   rule 4 (`PID_ABSENT`/`CONFIRMED_DEAD`) because `unparseableRows` is never
//   consulted. Because the new reason token does not exist yet, this file
//   deliberately does NOT import it by name (importing a nonexistent named
//   export from an ES module throws at import time, which would make every
//   test in the file fail with a parse/import error rather than a clean
//   assertion failure). Instead it captures whatever token production
//   returns and asserts on ITS SHAPE and DISTINCTNESS — a literal string,
//   different from `LIVENESS_REASON.PID_ABSENT` and
//   `LIVENESS_REASON.MALFORMED_PROCESS_ROW` (and every other existing
//   reason). The implementer is free to name the constant
//   `PID_ROW_UNPARSEABLE` (`'pid-row-unparseable'`) per the Build Plan's own
//   suggestion, or `UNPARSEABLE_SNAPSHOT_ROW` — whichever they choose, these
//   tests validate the STRING VALUE's distinctness rather than the export
//   name, so they stay green either way once implemented.
//
// THE SIGNAL SHAPE UNDER TEST, restated from Phase 1's contract
//   `liveProcesses.unparseableRows` is `Array<{ pid: number|null, rawLine:
//   string }>`, attached to the very array `listAgentProcesses` returns (and
//   which the caller then threads into `classifyLedgerEntryLiveness` as its
//   `liveProcesses` argument, UNCHANGED — see the outer acceptance test at
//   `./reconciliation-minority-garble.jest.spec.mjs`, which composes exactly
//   that path with no reshaping in between). This file therefore builds
//   fixtures the same way: a plain array `liveProcesses` with an
//   `unparseableRows` property attached directly, mirroring what
//   `listAgentProcesses` itself produces — sometimes built by hand for
//   precise control over the fixture, sometimes via the real
//   `listAgentProcesses` for end-to-end fidelity, matching this directory's
//   existing idiom (`./reconciliation-minority-garble.jest.spec.mjs` uses the
//   real function throughout).
//
// FIXTURE DISCIPLINE: every pid and timestamp is a LITERAL, never derived
// from this test process's own `process.pid` / `Date.now()`, matching
// `./reconciliation.jest.spec.mjs`'s stated convention.

import { listAgentProcesses } from './recon.mjs';
import { LIVENESS_REASON, LIVENESS_STATUS, classifyLedgerEntryLiveness } from './reconciliation.mjs';

/** The instant the beat's `ps` snapshot was taken. */
const SNAPSHOT_AT = Date.parse('2026-08-29T12:00:00Z');

/** A claude-rooted orchestrator process recorded as running by an earlier beat. */
const LIVE_PID = 9001;
const LIVE_STARTED_AT = Date.parse('2026-08-29T09:00:00Z');

/** A second, independent claude-rooted orchestrator, also genuinely running. */
const OTHER_LIVE_PID = 8001;
const OTHER_LIVE_STARTED_AT = Date.parse('2026-08-29T08:00:00Z');

/** A pid recorded before a host death — nothing runs at this number now, and no row (garbled or otherwise) names it. */
const GHOST_PID = 4242;
const GHOST_STARTED_AT = Date.parse('2026-08-29T06:00:00Z');

/** A dibs entry carrying an already-resolved identity pair. */
function dibsEntry(pid, pidStartedAt, orchestratorId = 'orchestrator-a') {
  return {
    orchestratorId,
    declaredAt: SNAPSHOT_AT - 60 * 1000,
    firstDeclaredAt: pidStartedAt !== undefined ? pidStartedAt + 5 * 1000 : SNAPSHOT_AT - 60 * 1000,
    desiredAgents: 2,
    pid,
    pidStartedAt,
  };
}

/** A clean snapshot naming exactly one live claude-rooted process. */
function liveSnapshot() {
  return [{ agentId: String(LIVE_PID), rssMb: 512, startedAt: LIVE_STARTED_AT }];
}

/**
 * Builds a `liveProcesses`-shaped array (plain rows plus an additive
 * `unparseableRows` property), the exact shape `listAgentProcesses` produces
 * per Phase 1's contract.
 */
function snapshotWithUnparseableRows(rows, unparseableRows) {
  const snapshot = [...rows];
  snapshot.unparseableRows = unparseableRows;
  return snapshot;
}

/**
 * Every reason token `LIVENESS_REASON` documented BEFORE Phase 2 folded
 * `PID_ROW_UNPARSEABLE` into it, hardcoded as literal strings rather than
 * derived via `Object.values(LIVENESS_REASON)`. Deriving from the enum would
 * be self-referential once the new token becomes a member of that same
 * object — the distinctness loop below would compare the new reason against
 * itself and trivially "pass" without proving anything. This snapshot proves
 * distinctness against the actual pre-existing vocabulary instead.
 */
const ALL_EXISTING_REASONS = [
  LIVENESS_REASON.PID_MATCHED,
  LIVENESS_REASON.PID_ABSENT,
  LIVENESS_REASON.PID_REUSE,
  LIVENESS_REASON.NO_PID,
  LIVENESS_REASON.NO_SNAPSHOT,
  LIVENESS_REASON.MALFORMED_PROCESS_ROW,
  LIVENESS_REASON.RESERVED_ENTRY,
  LIVENESS_REASON.WRITTEN_AFTER_SNAPSHOT,
];

// ---------------------------------------------------------------------------
// 1. The core new path: a ledger entry's pid matches an unparseableRows row.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — pid matches an unparseableRows row', () => {
  it('classifies UNKNOWN, never CONFIRMED_DEAD, when the pid only appears in unparseableRows', () => {
    const liveProcesses = snapshotWithUnparseableRows(
      [], // no matching row in the main parsed list at all
      [{ pid: LIVE_PID, rawLine: ' XXXX     1 102400    03:00:00 /Applications/Claude.app/Contents/MacOS/claude' }],
    );

    const verdict = classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, LIVE_STARTED_AT), liveProcesses, SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(verdict.status).not.toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
  });

  it('assigns a reason distinct from PID_ABSENT and MALFORMED_PROCESS_ROW, and from every other existing reason token', () => {
    // Convergence Analysis edge case #8 — the exact-string assertion this
    // block exists for. A reason that merely "looks different" by eye is not
    // good enough: this pins the STRING VALUE against every documented token.
    const liveProcesses = snapshotWithUnparseableRows(
      [],
      [{ pid: LIVE_PID, rawLine: ' XXXX     1 102400    03:00:00 /Applications/Claude.app/Contents/MacOS/claude' }],
    );

    const verdict = classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, LIVE_STARTED_AT), liveProcesses, SNAPSHOT_AT);

    expect(verdict.reason).not.toBe(LIVENESS_REASON.PID_ABSENT);
    expect(verdict.reason).not.toBe(LIVENESS_REASON.MALFORMED_PROCESS_ROW);
    for (const existingReason of ALL_EXISTING_REASONS) {
      expect(verdict.reason).not.toBe(existingReason);
    }
    // A real reason token was produced — not `undefined`, not the empty
    // string, and not a value shared with any other verdict path.
    expect(typeof verdict.reason).toBe('string');
    expect(verdict.reason.length).toBeGreaterThan(0);
  });

  it('reaches the same UNKNOWN verdict end-to-end through the real listAgentProcesses output, not only a hand-built fixture', () => {
    // Mirrors the outer acceptance test's composition
    // (classifyProcessSnapshotShape -> listAgentProcesses ->
    // classifyLedgerEntryLiveness) so this file proves the SAME new-reason
    // contract against the real Phase 1 signal, not only a shape approximated
    // by hand above.
    const psText = [
      '  PID  PPID    RSS     ELAPSED COMM',
      '    1     0    512 1-03:46:39 /sbin/launchd',
      ' 8000     1  10240    00:05:00 /usr/sbin/some-daemon',
      ' XXXX     1 102400    03:00:00 /Applications/Claude.app/Contents/MacOS/claude',
      '',
    ].join('\n');

    const liveProcesses = listAgentProcesses(psText, SNAPSHOT_AT);
    // Precondition: Phase 1's signal is present and unattributed for this
    // fixture (garbled pid field, not merely a garbled column), matching
    // `./recon-unparseable-rows.jest.spec.mjs`'s own fixture shape.
    expect(liveProcesses.unparseableRows?.some((row) => row.pid === null)).toBe(true);

    // A ledger entry naming the SAME pid this text's uncorrupted row would
    // have carried cannot be attributed via `pid: null` alone — this case
    // is deliberately the recoverable-pid one, exercised via a hand-built
    // `unparseableRows` override on top of the real function's other output,
    // since `listAgentProcesses` itself cannot recover a pid from `XXXX`.
    const withRecoveredPid = snapshotWithUnparseableRows(liveProcesses, [
      ...liveProcesses.unparseableRows,
      { pid: LIVE_PID, rawLine: ' XXXX     1 102400    03:00:00 claude' },
    ]);

    const verdict = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, LIVE_STARTED_AT),
      withRecoveredPid,
      SNAPSHOT_AT,
    );

    expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(verdict.reason).not.toBe(LIVENESS_REASON.PID_ABSENT);
  });
});

// ---------------------------------------------------------------------------
// 2. Regression — genuinely absent pid (no row anywhere, garbled or clean).
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — regression: genuinely absent pid is unaffected', () => {
  it('still classifies CONFIRMED_DEAD/PID_ABSENT when the pid has no match in liveProcesses and no match in unparseableRows', () => {
    const liveProcesses = snapshotWithUnparseableRows(
      liveSnapshot(),
      [{ pid: 7777, rawLine: ' XXXX     1  51200    00:10:00 some-other-mangled-row' }],
    );

    const verdict = classifyLedgerEntryLiveness(dibsEntry(GHOST_PID, GHOST_STARTED_AT), liveProcesses, SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_ABSENT);
  });

  it('still classifies CONFIRMED_DEAD/PID_ABSENT when unparseableRows is entirely absent from the snapshot (pre-Phase-1-signal shape)', () => {
    // A caller that has not yet been updated to thread the additive signal
    // (or a snapshot built by hand with no such property at all) must not
    // change today's behaviour for a genuinely-absent pid.
    const verdict = classifyLedgerEntryLiveness(dibsEntry(GHOST_PID, GHOST_STARTED_AT), liveSnapshot(), SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_ABSENT);
  });

  it('still classifies CONFIRMED_DEAD/PID_ABSENT when unparseableRows is present but empty', () => {
    const liveProcesses = snapshotWithUnparseableRows(liveSnapshot(), []);

    const verdict = classifyLedgerEntryLiveness(dibsEntry(GHOST_PID, GHOST_STARTED_AT), liveProcesses, SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_ABSENT);
  });

  // Pre-PR review (Medium): rule 3.5 originally read `liveProcesses
  // .unparseableRows` without an `Object.hasOwn` gate, unlike every other
  // caller-supplied property this classifier reads (rules 3, 4 and 5 all gate
  // on presence for exactly this reason). A polluted
  // `Object.prototype.unparseableRows` therefore lent a garbled-row alibi to
  // a snapshot that carried none, and every record whose pid it named went
  // UNKNOWN on every beat — un-reapable by the sweep, reachable only by its
  // ledger's own TTL. Direction is fail-safe, so this is defence in depth
  // rather than a live-path fix, and it is pinned for the same reason the
  // module's other pollution guards are.
  it('ignores an inherited unparseableRows and still reaps a genuinely absent pid (prototype-pollution defence in depth)', () => {
    const liveProcesses = liveSnapshot();
    Object.defineProperty(Object.prototype, 'unparseableRows', {
      value: [{ pid: GHOST_PID, rawLine: ' polluted' }],
      configurable: true,
      enumerable: false,
      writable: true,
    });

    try {
      const verdict = classifyLedgerEntryLiveness(
        dibsEntry(GHOST_PID, GHOST_STARTED_AT),
        liveProcesses,
        SNAPSHOT_AT,
      );

      expect(verdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
      expect(verdict.reason).toBe(LIVENESS_REASON.PID_ABSENT);
    } finally {
      delete Object.prototype.unparseableRows;
    }
  });
});

// ---------------------------------------------------------------------------
// 3. Regression — genuinely live pid is unaffected.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — regression: genuinely live pid is unaffected', () => {
  it('still classifies ALIVE when the pid matches a row in the main parsed liveProcesses list', () => {
    const liveProcesses = snapshotWithUnparseableRows(
      liveSnapshot(),
      [{ pid: 7777, rawLine: ' XXXX     1  51200    00:10:00 unrelated-mangled-row' }],
    );

    const verdict = classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, LIVE_STARTED_AT), liveProcesses, SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.ALIVE);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_MATCHED);
  });

  it('still classifies ALIVE when liveProcesses carries no unparseableRows property at all', () => {
    const verdict = classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, LIVE_STARTED_AT), liveSnapshot(), SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.ALIVE);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_MATCHED);
  });
});

// ---------------------------------------------------------------------------
// 4. Convergence edge case #4 — the new lookup must be a SEPARATE, INDEPENDENT
//    lookup against unparseableRows, never a synthetic candidate injected into
//    the main liveProcesses array where it could be picked up by the existing
//    rule 4/5 `.find` and mis-verdict via MALFORMED_PROCESS_ROW instead of the
//    new reason.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — edge case #4: independent lookup, not a synthetic liveProcesses candidate', () => {
  it('does not evaluate an unparseableRows-matched pid against the existing malformed-process-row rule', () => {
    // If an implementation wrongly folded `unparseableRows` entries into the
    // main array as synthetic rows (e.g. `{ agentId: String(pid), startedAt:
    // NaN }`) to reuse rule 4/5's existing `.find`, that row's unreadable
    // `startedAt` would trip rule 5 and produce `MALFORMED_PROCESS_ROW`
    // instead of the new, distinct reason this ticket adds. This test fails
    // under exactly that wrong shape and passes only when the new reason is
    // reached via its own, separate lookup.
    const liveProcesses = snapshotWithUnparseableRows(
      [],
      [{ pid: LIVE_PID, rawLine: ' XXXX     1 102400    03:00:00 claude' }],
    );

    const verdict = classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, LIVE_STARTED_AT), liveProcesses, SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(verdict.reason).not.toBe(LIVENESS_REASON.MALFORMED_PROCESS_ROW);
    expect(verdict.reason).not.toBe(LIVENESS_REASON.PID_ABSENT);
  });

  it('an unrelated, currently-alive ledger entry is unaffected by a numerically-colliding unparseableRows pid belonging to a different entry', () => {
    // Two different orchestrators: one genuinely alive at OTHER_LIVE_PID (a
    // real row in the main array), one whose pid is recoverable only from
    // unparseableRows. Classifying the ALIVE one must not be perturbed by the
    // other pid's presence in unparseableRows.
    const liveProcesses = snapshotWithUnparseableRows(
      [{ agentId: String(OTHER_LIVE_PID), rssMb: 256, startedAt: OTHER_LIVE_STARTED_AT }],
      [{ pid: LIVE_PID, rawLine: ' XXXX     1 102400    03:00:00 claude' }],
    );

    const aliveVerdict = classifyLedgerEntryLiveness(
      dibsEntry(OTHER_LIVE_PID, OTHER_LIVE_STARTED_AT, 'orchestrator-b'),
      liveProcesses,
      SNAPSHOT_AT,
    );
    const unparseableVerdict = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, LIVE_STARTED_AT, 'orchestrator-a'),
      liveProcesses,
      SNAPSHOT_AT,
    );

    expect(aliveVerdict.status).toBe(LIVENESS_STATUS.ALIVE);
    expect(aliveVerdict.reason).toBe(LIVENESS_REASON.PID_MATCHED);
    expect(unparseableVerdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(unparseableVerdict.reason).not.toBe(LIVENESS_REASON.PID_ABSENT);
  });
});

// ---------------------------------------------------------------------------
// 5. Convergence edge case #2 — the null-pid (unattributed) case is a
//    documented, accepted residual, not a bug. Pinned here as explicit,
//    intentional CURRENT (and target) behaviour, per the ticket's own scope.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — edge case #2: unattributed (pid: null) unparseableRows entries protect nothing (documented residual)', () => {
  it('still classifies CONFIRMED_DEAD/PID_ABSENT for an entry whose pid has no recoverable match anywhere — only an unattributed null-pid row exists', () => {
    // No pid in this snapshot can be attributed to GHOST_PID: the main array
    // has no row for it, and the only unparseableRows entry present carries
    // `pid: null` (i.e. the garbled row's own pid field could not be
    // recovered at all — the worst-case garble). Per the ticket's own
    // documented scope (Convergence Analysis edge case #2), an unattributed
    // row protects NOTHING: there is no way to correlate it to any specific
    // ledger entry, so the pre-existing, unchanged CONFIRMED_DEAD/PID_ABSENT
    // verdict is the correct, accepted behaviour — not a bug this phase
    // leaves open.
    const liveProcesses = snapshotWithUnparseableRows(
      liveSnapshot(),
      [{ pid: null, rawLine: ' XXXX     1 102400    03:00:00 claude' }],
    );

    const verdict = classifyLedgerEntryLiveness(dibsEntry(GHOST_PID, GHOST_STARTED_AT), liveProcesses, SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_ABSENT);
  });
});

// ---------------------------------------------------------------------------
// 6. Convergence edge case #9 — two ledger entries in one snapshot, each with
//    its own correct, independent verdict: one genuinely dead, one garbled.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — edge case #9: independent verdicts for a genuinely-dead entry and a garbled-row entry sharing one snapshot', () => {
  it('classifies the genuinely-absent entry CONFIRMED_DEAD and the unparseableRows-matched entry UNKNOWN, independently', () => {
    const liveProcesses = snapshotWithUnparseableRows(
      [],
      [{ pid: LIVE_PID, rawLine: ' XXXX     1 102400    03:00:00 claude' }],
    );

    const deadEntryVerdict = classifyLedgerEntryLiveness(
      dibsEntry(GHOST_PID, GHOST_STARTED_AT, 'orchestrator-ghost'),
      liveProcesses,
      SNAPSHOT_AT,
    );
    const garbledEntryVerdict = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, LIVE_STARTED_AT, 'orchestrator-a'),
      liveProcesses,
      SNAPSHOT_AT,
    );

    expect(deadEntryVerdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
    expect(deadEntryVerdict.reason).toBe(LIVENESS_REASON.PID_ABSENT);

    expect(garbledEntryVerdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(garbledEntryVerdict.reason).not.toBe(LIVENESS_REASON.PID_ABSENT);
    expect(garbledEntryVerdict.reason).not.toBe(deadEntryVerdict.reason);
  });
});

// ---------------------------------------------------------------------------
// 7. Freshness-guard interaction regression (Convergence edge case #7). The
//    `written-after-snapshot` guard lives in `cli.mjs`'s `reconcileLedgers`,
//    OUTSIDE this function, and only ever runs inside the `isDead` callback
//    it applies to a record already classified CONFIRMED_DEAD (see
//    `cli.mjs`'s `reapConfirmedDeadRecords` call: `if (liveness.status !==
//    LIVENESS_STATUS.CONFIRMED_DEAD) return false;` gates entry to the guard
//    check entirely). This function itself never reads that guard and never
//    produces WRITTEN_AFTER_SNAPSHOT — proving the new UNKNOWN reason behaves
//    exactly like every pre-existing UNKNOWN reason in that respect: a
//    record classified UNKNOWN by this function can never reach the
//    freshness guard at all, regardless of how recently it was written,
//    because the guard is gated on CONFIRMED_DEAD status upstream of it.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — regression: written-after-snapshot guard interaction unchanged for the new reason', () => {
  it('never returns WRITTEN_AFTER_SNAPSHOT itself, for the new unparseableRows path or any other', () => {
    const liveProcesses = snapshotWithUnparseableRows(
      [],
      [{ pid: LIVE_PID, rawLine: ' XXXX     1 102400    03:00:00 claude' }],
    );

    const verdict = classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, LIVE_STARTED_AT), liveProcesses, SNAPSHOT_AT);

    expect(verdict.reason).not.toBe(LIVENESS_REASON.WRITTEN_AFTER_SNAPSHOT);
  });

  it('classifies UNKNOWN for the new reason regardless of how recently the entry was declared — the guard has nothing to gate, exactly as for the other UNKNOWN reasons', () => {
    // A record declared an instant before the snapshot (the case
    // `isWrittenAfterSnapshot` exists to catch for a CONFIRMED_DEAD verdict)
    // must classify identically to one declared long before it: this
    // function's verdict for the unparseableRows path does not vary with
    // `declaredAt` at all, matching every other UNKNOWN reason (`no-pid`,
    // `no-snapshot`, `malformed-process-row`, `reserved-entry`), none of
    // which read a write stamp either.
    const liveProcesses = snapshotWithUnparseableRows(
      [],
      [{ pid: LIVE_PID, rawLine: ' XXXX     1 102400    03:00:00 claude' }],
    );

    const declaredJustBeforeSnapshot = {
      ...dibsEntry(LIVE_PID, LIVE_STARTED_AT),
      declaredAt: SNAPSHOT_AT - 1,
    };
    const declaredLongBefore = {
      ...dibsEntry(LIVE_PID, LIVE_STARTED_AT),
      declaredAt: SNAPSHOT_AT - 6 * 60 * 60 * 1000,
    };

    const recentVerdict = classifyLedgerEntryLiveness(declaredJustBeforeSnapshot, liveProcesses, SNAPSHOT_AT);
    const oldVerdict = classifyLedgerEntryLiveness(declaredLongBefore, liveProcesses, SNAPSHOT_AT);

    expect(recentVerdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(oldVerdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(recentVerdict.reason).toBe(oldVerdict.reason);
  });
});
