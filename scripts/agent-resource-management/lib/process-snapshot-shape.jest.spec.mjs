// — `classifyProcessSnapshotShape` (./recon.mjs): is a `ps` snapshot's
// TEXT legible as a process table at all?
//
// Owned by its own file rather than folded into ./recon.jest.spec.mjs, whose
// header declares a different (Phase 1, sampling) contract.
//
// WHY THIS GUARD EXISTS. `listAgentProcesses` answers `[]` both for a host
// genuinely running no agents and for text its column grammar cannot read. The
// first is a valid observation and condemns every pid-bearing ledger record as
// `confirmed-dead`; the second is no observation at all. A sweep that cannot
// tell them apart reaps the whole live fleet the first time `ps` output drifts
// — the failure this classifier exists to prevent, and the inverse of the
// ticket's hard AC ("never reap a live agent").
//
// WHY A MAJORITY AND NOT "AT LEAST ONE". Real `ps -A` on this project's target
// host emits hundreds of rows. Under an at-least-one rule a drift that mangles
// every row but one classifies the whole snapshot `'readable'`, the snapshot is
// passed through, and every agent whose row was mangled reads `confirmed-dead`
// — one surviving row disarms the guard entirely. The boundaries below pin the
// stronger predicate, including which way the exact-half tie breaks.

import { classifyProcessSnapshotShape } from './recon.mjs';

const HEADER = '  PID  PPID    RSS     ELAPSED COMM';

/** A well-formed `ps -Ao pid,ppid,rss,etime,comm` row. */
const parseableRow = (pid) => `${pid}     1 102400    01:00:00 /usr/local/bin/node`;

/**
 * A row of the same processes emitted with comma separators instead of
 * whitespace columns — the realistic drift shape (column order, locale, header
 * change) that leaves `ps` exiting zero while not one row matches the grammar.
 */
const unparseableRow = (pid) => `${pid},1,102400,01:00:00,/usr/local/bin/node`;

/** Builds a snapshot with exactly `good` parseable and `bad` unparseable rows. */
function snapshotOf(good, bad) {
  return [
    HEADER,
    ...Array.from({ length: good }, (_, index) => parseableRow(9000 + index)),
    ...Array.from({ length: bad }, (_, index) => unparseableRow(8000 + index)),
    '',
  ].join('\n');
}

describe('the empty boundary — untouched by the majority rule', () => {
  // `'empty'` is a THIRD verdict, not a synonym for either other one: text
  // carrying nothing but a header (or nothing at all) is a different
  // observation about the text from a table this grammar could not read, and
  // this function's whole job is to describe text. The majority rule does not
  // apply to it — 0 of 0 is not a question about a majority.
  //
  // IT IS NOT A LICENCE TO CLASSIFY AGAINST (pre-PR review, High — this block
  // used to justify the verdict as "a genuinely idle host must still be able
  // to reap ghosts" and as "a valid observation of zero agents", and both are
  // false). `ps -A` enumerates every process on the host, and a host running
  // the sweep is running at minimum `launchd`, the sweeping `node` process and
  // its claude-rooted parent — the development host answered 667 body rows
  // while idle. Zero body lines is a collection that came back broken behind
  // an exit code of zero. An AGENTLESS host produces a large `'readable'`
  // table with no claude roots in it and reaps ghosts through that branch,
  // which is the case the old rationale was reaching for; it is pinned in
  // ../ledger-reconciliation-beat-wiring.jest.spec.mjs. `cli.mjs`'s
  // `observeBeatProcesses` accordingly threads `null` for ANY verdict that is
  // not `'readable'`. Keeping the verdicts separate here is about naming what
  // the text is, not about trusting one of them more than the other.
  it.each([
    ['a header and no rows', `${HEADER}\n`],
    ['a header with no trailing newline', HEADER],
    ['the empty string', ''],
    ['whitespace only', '   \n\n\t\n'],
    ['null', null],
    ['undefined', undefined],
  ])('classifies %s as empty', (_label, input) => {
    expect(classifyProcessSnapshotShape(input)).toBe('empty');
  });
});

describe('the majority threshold', () => {
  it('classifies an all-parseable table as readable', () => {
    expect(classifyProcessSnapshotShape(snapshotOf(5, 0))).toBe('readable');
  });

  it('classifies a table with no parseable row at all as unreadable', () => {
    expect(classifyProcessSnapshotShape(snapshotOf(0, 5))).toBe('unreadable');
  });

  it('classifies a bare majority of parseable rows as readable', () => {
    // 3 of 5 — one row over half.
    expect(classifyProcessSnapshotShape(snapshotOf(3, 2))).toBe('readable');
  });

  it('classifies an exact half-and-half split as unreadable, breaking the tie fail-safe', () => {
    // 2 of 4 is not a MAJORITY. The tie breaks toward `'unreadable'` because
    // that verdict reaps nothing and defers every record to its TTL, whereas
    // `'readable'` would let a half-mangled table condemn live agents.
    expect(classifyProcessSnapshotShape(snapshotOf(2, 2))).toBe('unreadable');
  });

  it('classifies a bare minority of parseable rows as unreadable', () => {
    // 2 of 5 — one row under half.
    expect(classifyProcessSnapshotShape(snapshotOf(2, 3))).toBe('unreadable');
  });

  it('does not let one surviving row disarm the guard on a large drifted table', () => {
    // The mutation this threshold exists to kill: under an `.some()`/
    // at-least-one rule this is `'readable'`, the snapshot is passed to the
    // classifier, and all 199 agents whose rows drifted read `confirmed-dead`.
    expect(classifyProcessSnapshotShape(snapshotOf(1, 199))).toBe('unreadable');
  });

  it('still reads a single-row table as readable — one row is a majority of one', () => {
    // The `'empty'`/`'readable'` boundary is about row COUNT, not about the
    // threshold: a host with exactly one process visible is a real observation.
    expect(classifyProcessSnapshotShape(snapshotOf(1, 0))).toBe('readable');
  });

  it('ignores blank lines and the header when counting either side of the threshold', () => {
    // 2 parseable, 1 unparseable, plus a header and blank lines that must not
    // be counted as unparseable body rows — if they were, this would be a
    // minority and flip to `'unreadable'`.
    const withNoise = [HEADER, '', parseableRow(9001), '   ', unparseableRow(8001), parseableRow(9002), '', ''].join(
      '\n',
    );
    expect(classifyProcessSnapshotShape(withNoise)).toBe('readable');
  });
});

// ---------------------------------------------------------------------------
// The drift class the LINE SHAPE alone cannot see (pre-PR review, High).
//
// `PS_LINE_WITH_ETIME_PATTERN` captures the etime column as `\S+` so that a row
// carrying an unparseable etime (`-`) still matches the line shape and can be
// excluded individually by `listAgentProcesses` rather than condemning the
// whole table. The cost of that permissiveness is that a column-order drift
// which merely SWAPS etime and comm — both non-empty, non-whitespace fields,
// with the three leading integer columns intact — matches the line shape on
// every single row.
//
// Judged on the line shape alone that text was `'readable'`, while
// `listAgentProcesses` retained nothing from it (the comm parses as `NaN`
// seconds of etime, and the etime carries no `/claude` suffix to make a root).
// `[]` from a `'readable'` snapshot is a valid observation of zero live agents,
// so the sweep condemned every pid-bearing record in all three ledgers as
// `pid-absent` -> `confirmed-dead` and reaped the entire live fleet's ledger
// state on a single beat — the exact inverse of the ticket's hard AC, reached
// by the guard that exists to prevent it.
// ---------------------------------------------------------------------------

describe('drift that matches the line shape but not the etime grammar', () => {
  /**
   * `pid ppid rss comm etime` — etime and comm transposed. Every field is
   * present and correctly typed for the LINE grammar; only the etime GRAMMAR
   * rejects it.
   */
  const etimeCommSwappedRow = (pid) => `${pid}     1 102400 /usr/local/bin/claude 01:00:00`;

  const swappedSnapshotOf = (good, swapped) =>
    [
      HEADER,
      ...Array.from({ length: good }, (_, index) => parseableRow(9000 + index)),
      ...Array.from({ length: swapped }, (_, index) => etimeCommSwappedRow(8000 + index)),
      '',
    ].join('\n');

  it('reads a wholly etime/comm-transposed table as unreadable, not readable', () => {
    // The regression. Before the etime half of the parseability test existed,
    // this was `'readable'` on 200/200 rows while the attribution walk retained
    // none of them.
    expect(classifyProcessSnapshotShape(swappedSnapshotOf(0, 200))).toBe('unreadable');
  });

  it('does not let one correctly-ordered row disarm the guard on a transposed table', () => {
    expect(classifyProcessSnapshotShape(swappedSnapshotOf(1, 199))).toBe('unreadable');
  });

  it('still reads a real table carrying a minority of unparseable etimes as readable', () => {
    // The permissiveness this guard must NOT destroy: `ps` can legitimately
    // report `-` for a row's elapsed time. `listAgentProcesses` excludes that
    // one row; the table as a whole is still a real observation, so a minority
    // of them must not tip the snapshot to `'unreadable'` and disable the sweep.
    const dashEtimeRow = (pid) => `${pid}     1 102400           - /usr/local/bin/node`;
    const mostlyGood = [
      HEADER,
      ...Array.from({ length: 8 }, (_, index) => parseableRow(9000 + index)),
      ...Array.from({ length: 2 }, (_, index) => dashEtimeRow(8000 + index)),
      '',
    ].join('\n');

    expect(classifyProcessSnapshotShape(mostlyGood)).toBe('readable');
  });
});
