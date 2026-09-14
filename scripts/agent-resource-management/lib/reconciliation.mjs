// — ghost-entry liveness classification for coordination-file ledgers.
//
// WHAT THIS MODULE IS
//   One pure function, `classifyLedgerEntryLiveness`, that decides whether a
//   single ledger record names a process that is alive, confirmed dead, or
//   undecidable. A `confirmed-dead` verdict lets a sweep reap the record
//   immediately instead of waiting out that ledger's TTL; an `unknown` verdict
//   defers to that TTL unchanged.
//
//   The function shells out for nothing and reads no clock. Its whole view of
//   the world is the arguments it is handed: one ledger record, one
//   already-taken process snapshot (`listAgentProcesses`' own output, threaded
//   in by the caller), the epoch-ms instant that snapshot was taken, and an
//   options bag whose only member, `toleranceMs`, is a test seam production
//   never passes. That keeps it unit-testable without a process tree, and it
//   lets one `ps` snapshot serve every record in every ledger within a single
//   beat.
//
// WHY IT LIVES HERE AND NOT IN `./coordination-file.mjs`
//   The classifier correlates ledger identity against process discovery.
//   Housing it in the ledger-write module would hand that module a dependency
//   on process discovery it has no other reason to carry. This module is the
//   seam between the two; it imports `isReservedEntry` from the ledger module
//   so the reserved-row exemption cannot drift from `pruneStale`'s.
//
// ONE RULE FOR EVERY LEDGER
//   Dibs entries, `live-agent` claim records, and `live-agent-admission` claim
//   records all pass through this single function. There is no ledger-type
//   parameter, because the rule does not vary by ledger: each record carries a
//   `pid`/`pidStartedAt` pair or it does not. Each ledger keeps its own
//   pre-existing TTL for the `unknown` path. This module owns no TTL, defines
//   no TTL, and changes none of them.
//
// THE CLASSIFICATION RULE, IN PRECEDENCE ORDER
//   1. A reserved `__claim:<type>__` row is a container, not a process. It
//      names no pid and is never classified — only the per-claim records held
//      inside it are.
//   2. No snapshot at all (the caller's discovery attempt failed, so it passes
//      `null`) → `unknown`. An EMPTY ARRAY is not this case: it is a
//      successful observation of zero live agents, and records are classified
//      against it normally.
//   3. An incomplete or corrupt identity → `unknown`. Both halves of the
//      `pid`/`pidStartedAt` pair must be present AND shaped like the thing they
//      claim to be: a pid is an integer in the range a POSIX `pid_t` can hold,
//      a `pidStartedAt` is a positive integer epoch-ms instant within the range
//      a `Date` can represent that does not postdate the snapshot observing it.
//      Missing, non-numeric, `NaN`, infinite, fractional, zero, negative, past
//      the `pid_t` ceiling, past the maximum representable time value, or
//      claiming a start beyond the snapshot's own instant PLUS the tolerance
//      (see `postdatesSnapshot`, which grants the same grace the identity
//      comparison does) all fail here. The ledger write path persists
//      caller-supplied values verbatim, so a hand-edited
//      file can carry anything; half an identity is no identity, and a number
//      that could never be a pid — or an instant that could never be a start
//      time — is not an identity either. A value that cannot name a real
//      process must never reach rule 4 or rule 5, where its inevitable lookup
//      miss or tolerance miss would read as a death certificate.
//   4. No process in the snapshot at that pid → `confirmed-dead`.
//   5. A process at that pid whose start time is within the tolerance →
//      `alive`; beyond it, the pid has been reused by an unrelated process and
//      the record's own process is `confirmed-dead`. A matched snapshot row
//      whose own `startedAt` is unreadable — or impossible, which includes an
//      instant postdating the snapshot that observed the row — decides nothing
//      either way and takes `unknown` under its own distinct reason token. The
//      shape demanded of a snapshot row's `startedAt` is exactly the shape
//      demanded of a record's `pidStartedAt`: an asymmetry between the two
//      sides of this comparison is a hole, because whichever side is laxer is
//      the side a corrupt value enters through.
//
//   An `alive` verdict is AGE-BLIND: this classifier never reads a record's
//   write stamp, so a record naming a running process is spared BY THE SWEEP
//   however old it is. That is what makes recovery immediate in one direction
//   without making it destructive in the other.
//
//   IT IS NOT A TTL EXEMPTION, and must never be written up as one. This
//   module only ever decides what the sweep REMOVES; it grants nothing the
//   power to SURVIVE. The pre-existing age-based prunes are untouched by it
//   and still apply to an `alive` record exactly as before — `pruneStale`
//   (`./coordination-file.mjs`) drops any dibs entry whose `declaredAt` is
//   older than `livenessThresholdMs` (15 min in `cli.mjs`) with no reference
//   to `pid`, and each claim ledger's readers still prune by `claimedAt`
//   against their own TTL. A live orchestrator that stops beating for longer
//   than its ledger's window still ages out; what `alive` buys it is that the
//   sweep will not reap it EARLY, on the strength of a pid correlation.
//
// FAIL-CONSERVATIVE POSTURE
//   Every case where the classifier cannot see enough to decide — no identity,
//   a corrupt identity, an impossibly-shaped identity, no snapshot, a matched
//   snapshot row with an unreadable or impossible start time, a reserved row —
//   resolves to `unknown` and defers to the TTL. No ambiguous input ever
//   produces a death certificate. A start-time mismatch between two POSSIBLE
//   instants is deliberately not an ambiguous case: it is a decided one, and it
//   takes `confirmed-dead`.
//
//   A CONSEQUENCE WORTH STATING: an identity stored in the wrong TYPE — most
//   plausibly `pid: "4242"` as a string, since `listAgentProcesses` reports
//   `agentId` as a string and copying that form across is the natural slip —
//   classifies `unknown` forever and can only age out under its ledger's TTL.
//   That is the deliberate choice, in the safe direction: a record whose
//   identity cannot be trusted is never reaped early, at the cost of losing
//   immediate reclamation for it. Coercing such a value into a number here
//   would make the classifier's evidence weaker than the shape it demands.
//
// RESIDUAL LIMITATION ONE — DISCOVERY-WALK OMISSION
//   A live agent on this host, reachable by the very `ps` the sweep runs, that
//   the discovery walk in `listAgentProcesses` fails to enumerate (the class
//   of bug hardened it against) is indistinguishable to this classifier
//   from a process that genuinely exited: both read as "absent from the
//   snapshot", and both take `confirmed-dead`. This classifier inherits
//   whatever correctness the walk has and cannot improve on it, because the
//   walk's output is its only evidence. A record carrying NO recorded identity
//   is unaffected — with nothing to correlate, an omission changes nothing and
//   the record ages out under its ledger's own TTL. This limitation is named
//   and accepted, not closed here, so that the guarantee is never overstated
//   elsewhere as "a live agent is never reaped, unconditionally".
//
//   THIS IS NOT THE PID-NAMESPACE BOUNDARY described as limitation two below,
//   and the two must never be merged into one statement. This one is a defect
//   in a walk that CAN see the process — the host and the PID namespace are
//   the sweep's own, so hardening the walk closes it. Limitation two is a
//   boundary the sweep's `ps` cannot cross at all, which no walk fix can
//   reach. Reading either as an instance of the other would make one of them
//   look already handled when it is not.
//
// RESIDUAL LIMITATION TWO — PID-NAMESPACE BOUNDARY
//   Structurally different from the walk-omission limitation above, and kept
//   separate from it deliberately: there the walk is imperfect on a host it
//   can see, whereas here the process sits behind a boundary the sweep's own
//   `ps` cannot see across at all, so no fix to the walk could ever surface
//   it. The standing assumption this classifier rests on is that a recorded
//   `pid` is only trustworthy when the process it names is visible to the same
//   PID namespace the sweep itself runs in. A pid recorded inside a container
//   whose namespace is distinct from the sweep's will never appear in the host
//   snapshot, and rule 4 maps that to `confirmed-dead` — a CONFIDENT wrong
//   answer rather than an ambiguous one, which is the part worth naming. This
//   assumption covers only that boundary case; it must not be stretched to
//   excuse the narrower walk-omission limitation, which is a separate thing.
//
// RESIDUAL LIMITATION THREE — COINCIDENTAL START-TIME COLLISION
//   Two different real processes, one reusing the other's pid, each starting
//   within `PID_IDENTITY_TOLERANCE_MS` of the other, are identical under this
//   rule and the reused pid reads as `alive`. Pid reuse requires the number
//   space to wrap, and the successor must additionally land inside a window
//   reaching `PID_IDENTITY_TOLERANCE_MS` on EITHER side of its predecessor's
//   start — so the window is twice `PID_IDENTITY_TOLERANCE_MS` wide in total,
//   not once — which together makes this vanishingly unlikely. It is an
//   accepted residual risk, named rather than left silent, and tightening
//   `PID_IDENTITY_TOLERANCE_MS` narrows it.
//
// RESPONSIBILITY BOUNDARY — VERDICT TIME vs COMMIT TIME
//   A verdict is a statement about SNAPSHOT TIME and nothing else, which is
//   why `snapshotAt` is threaded in by the caller and echoed back on every
//   verdict rather than read from a clock here. A record can enter or change
//   in the file after the snapshot was taken and before the sweep's write
//   commits, and this function cannot see that gap from a single record and a
//   single snapshot. The gap runs in BOTH directions, and both have to be
//   named because they have opposite likelihoods and opposite consequences:
//
//     `pid-reuse` DIRECTION — a pid confirmed dead at snapshot time is reused
//     by an unrelated process before the write commits. Rare: it needs the pid
//     space to wrap inside the gap.
//
//     `pid-absent` DIRECTION — a record is WRITTEN into the file at or after
//     the snapshot, by a process that was born after the `ps` ran and is
//     therefore carried by no row in it. Rule 4 maps that to `confirmed-dead`
//     under `pid-absent`, so the sweep would reap a live agent's record. This
//     is the strictly more likely of the two — it needs only an ordinary
//     concurrent orchestrator and a lock wait, both of which are this host's
//     normal operating mode — and it is the one whose consequence is a live
//     reap rather than a delayed one.
//
//   Neither is resolvable from inside this function, and the fencing lock does
//   not resolve them either: the lock makes the write atomic, it does not make
//   the verdict fresh, and waiting for it is what widens the gap in the first
//   place. The obligation is discharged in `cli.mjs`'s `reconcileLedgers`,
//   which compares each record's own write stamp (`declaredAt` on a dibs
//   entry, `claimedAt` on a claim record) against the instant the sweeper's
//   `ps` was ISSUED and refuses to act on any record the snapshot could not
//   have seen, reporting `WRITTEN_AFTER_SNAPSHOT` for it. This function's
//   responsibility ends at the verdict.
//
//   THE ISSUE INSTANT, NOT `snapshotAt` — the distinction is the whole guard
//   (pre-PR review, High; the guard was anchored to `snapshotAt` and this
//   paragraph claimed the gap was closed). `snapshotAt` is read AFTER the
//   collection returns, but the snapshot's CONTENTS were fixed when the kernel
//   served the `ps`, up to `DEFAULT_PROBE_TIMEOUT_MS` earlier. A claude root
//   born inside that window is in no row of the snapshot, yet a record its beat
//   stamped before `snapshotAt` cleared a `snapshotAt`-anchored guard and was
//   reaped `pid-absent` — the live reap this paragraph says is prevented,
//   arriving through the prevention. `cli.mjs` reads `snapshotIssuedAt` before
//   the collection and threads that here instead.
//
// CALLER OBLIGATION — A STRUCTURALLY DEGRADED SNAPSHOT
//   This function sees ONE record per call and therefore cannot count anything
//   about the snapshot as a whole. A snapshot that is an array of rows none of
//   which carries a usable `agentId` is, from inside a single call, identical to
//   a valid observation of zero live agents: every pid-bearing record misses the
//   lookup and takes `confirmed-dead` under `pid-absent`. That is the right
//   answer for a genuinely empty host and the wrong one for a discovery result
//   that came back shaped wrongly, and no per-record rule can tell the two
//   apart.
//
//   The obligation therefore sits with the caller, which is the only party that
//   sees the whole snapshot: it must not pass a snapshot it can already tell is
//   structurally degraded, and should thread `null` — the `no-snapshot` path,
//   which defers every record to its TTL — instead.
//
//   `cli.mjs`'s beat sweep discharges that obligation on the grounds it can
//   detect from the collection itself. It threads `null` when its own `ps`
//   collection FAILED, and it threads `null` when the collection succeeded but
//   `classifyProcessSnapshotShape` (`./recon.mjs`) judges the text anything
//   other than `'readable'` — which covers BOTH of that function's degraded
//   verdicts (pre-PR review, Medium — this list named only the first, and the
//   `'empty'` verdict was passed straight through as a valid observation of
//   zero agents):
//
//     `'unreadable'` — no MAJORITY of the non-header lines parses as a process
//     row, including its etime field. The column/locale/header-drift case,
//     which otherwise parses to zero or a handful of rows without throwing.
//
//     `'empty'` — no non-header line at all. Not an idle host: `ps -A`
//     enumerates every process, and a host running the sweep is running at
//     least the sweeping `node` process and its claude-rooted parent, so zero
//     rows means a collection that came back broken behind an exit code of
//     zero. A genuinely AGENTLESS host still produces a large readable table
//     and still reaps normally — the guard turns on legibility, never on row
//     count.
//
//   WHAT REMAINS UNCOVERED, stated rather than implied — NARROWED by
//   Phases 1-2, not closed. A MINORITY-degraded snapshot (most rows parse, a
//   few do not, so `classifyProcessSnapshotShape` still reports `'readable'`
//   and the snapshot is passed through) no longer defaults every unparseable
//   row's ledger entry to `confirmed-dead`: `listAgentProcesses` now attaches
//   each dropped row's recovered pid (when it has one) to the additive
//   `unparseableRows` signal (`lib/recon.mjs`), and rule 3.5 below correlates
//   a ledger entry's own `pid` against that signal ahead of the main lookup,
//   classifying it `UNKNOWN`/`PID_ROW_UNPARSEABLE` instead of reaping it.
//
//   What remains uncovered is narrower and specific: a row whose OWN pid
//   field is itself unrecoverable (`unparseableRows` records it as
//   `pid: null`) cannot be attributed to any particular ledger entry — there
//   is nothing to correlate against — so a live agent whose row garbled that
//   badly still falls through rule 3.5 to the main lookup, finds no match,
//   and takes `PID_ABSENT` / `CONFIRMED_DEAD` exactly as before. This is
//   Convergence Analysis edge case #2, and it is accepted, in-scope-excluded
//   residual — not an oversight; a documented residual limitation, split
//   into a closed sub-case and an open one. It is a SEPARATE gap from the
//   table-parses-but-no-agent-roots total reap, and from the reap path's
//   duplicate-row and normalization divergences; do not treat either of
//   those as covering it. Closing this
//   narrower residual would require recovering a pid from a row where the pid
//   field itself failed to parse — a different, harder problem than
//   Phases 1-2 solved.
//
// CROSS-BEAT WALL-CLOCK STEP — THE DELIBERATE CHOICE
//   `pidStartedAt` (written by one beat) and a snapshot's `startedAt` (read by
//   a later beat) both derive from the same `now - parseEtimeToSeconds(etime)
//   * 1000` formula against a single `ps` snapshot, so within ONE beat the two
//   cannot drift apart by construction. Across beats they can move apart for
//   TWO unrelated reasons, and the ordinary one needs no clock event at all:
//   each beat's `ps` collection latency (bounded only by
//   `DEFAULT_PROBE_TIMEOUT_MS`) plus `etime`'s whole-second flooring push that
//   beat's derived start later by up to their sum, so two beats at opposite
//   ends of that range disagree by nearly its whole width on a live process.
//   `PID_IDENTITY_TOLERANCE_MS` is DERIVED from those two bounds precisely so
//   that this ordinary variance can never reach it. The second reason is a
//   wall-clock step — an NTP correction or a manual clock change — which is
//   unbounded and can. The design's choice: a step within the
//   tolerance is absorbed and the record stays `alive`; a step beyond it is
//   indistinguishable from pid reuse from a single snapshot and takes the same
//   `confirmed-dead` verdict. Inventing a third signal to tell the two apart
//   would trade a rare wrong reap for a permanent inability to detect reuse.
//   An owner who prefers the other trade widens `PID_IDENTITY_TOLERANCE_MS`,
//   which is exported as a single named constant for exactly that purpose.
//
// AGNOSTIC TO HOW A PID WAS DERIVED
//   This function has no opinion on how a record's `pid` was obtained — only
//   that it is the kind of pid `listAgentProcesses` can report, i.e. a
//   claude-rooted root pid. Ledger writers derive that identity by walking
//   ancestry to the nearest claude-rooted process; if a writer instead
//   recorded its own `node` pid, that pid could never appear in a
//   claude-rooted-only snapshot and every such record would read as
//   `confirmed-dead` on the next beat. The correctness of the recorded pid is
//   the writer's responsibility, and this contract is deliberately unchanged
//   by it.

import { isReservedEntry } from './coordination-file.mjs';
import { DEFAULT_PROBE_TIMEOUT_MS } from './probe-bound.mjs';

/**
 * `etime`'s granularity, in milliseconds. `ps` reports elapsed time in WHOLE
 * SECONDS, so `now - parseEtimeToSeconds(etime) * 1000` floors away up to one
 * second of the process's real age, pushing every derived `startedAt` up to
 * this much LATER than the true start.
 */
const ETIME_TRUNCATION_MS = 1000;

/**
 * Headroom above the two error sources below, so the tolerance is not sized to
 * the exact worst case with nothing to spare. A successful `execFileSync`
 * returns strictly INSIDE its timeout, but the anchor is read a few
 * instructions later still (`cli.mjs`'s `snapshotAt`), and neither bound is
 * measured to the millisecond.
 */
const PID_IDENTITY_TOLERANCE_MARGIN_MS = 500;

/**
 * Maximum difference, in milliseconds, between a record's `pidStartedAt` and a
 * live process's `startedAt` for the two to be considered the same process.
 *
 * DERIVED FROM THE BOUNDS THIS DESIGN ALREADY SETS, not written as a fresh
 * literal, so the relationship cannot silently break if either bound moves.
 * A beat's derived `startedAt` for a process is LATER than that process's true
 * start by two independent, additive amounts:
 *
 *   1. The `ps` collection's own duration. `cli.mjs` resolves `snapshotAt`
 *      AFTER `collectBeatPsSnapshot` returns, while `etime` was measured by
 *      the kernel when `ps` was issued, so the whole shell-out lands in the
 *      derived start. Its only ceiling is `DEFAULT_PROBE_TIMEOUT_MS`.
 *   2. `ETIME_TRUNCATION_MS`, `etime`'s whole-second flooring.
 *
 * Per-beat error therefore ranges over `DEFAULT_PROBE_TIMEOUT_MS +
 * ETIME_TRUNCATION_MS`, and two beats landing at opposite ends of that range
 * disagree by very nearly the whole width — WITH NO CLOCK CHANGE INVOLVED. A
 * tolerance narrower than that width condemns a GENUINELY LIVE agent as
 * `pid-reuse` on precisely the contended host this system exists for (a slow
 * `ps` on a thrashing host is the common case, not the exotic one). It must
 * therefore exceed the width, not merely absorb `etime` alone.
 *
 * WIDENING IS THE FAIL-SAFE DIRECTION, by design residual limitation 4 and
 * this design's posture: a wider window makes a reused pid marginally more likely
 * to read as its predecessor, i.e. a ghost goes UNREAPED — which costs
 * throughput and still converges via each ledger's own pre-existing TTL. A
 * narrower window costs a live agent its life, which is the one outcome this
 * work exists to prevent.
 *
 * A single named, exported constant so an owner can retune it in one place
 * rather than chase a literal across call sites.
 *
 * THE ONE PLACE. `options.toleranceMs` exists as a TEST SEAM, not as a
 * production knob: production wiring must pass this constant (i.e. pass no
 * override at all) so that tightening the tolerance is a one-line change here
 * rather than a hunt through call sites. A tolerance is a widening of what
 * counts as `alive`, so an override in the narrowing direction is the only one
 * that can cost a live agent its life; a negative override is rejected outright
 * rather than trusted, because no caller can have a legitimate use for one.
 */
export const PID_IDENTITY_TOLERANCE_MS =
  DEFAULT_PROBE_TIMEOUT_MS + ETIME_TRUNCATION_MS + PID_IDENTITY_TOLERANCE_MARGIN_MS;

/** The three verdicts this classifier can reach. */
export const LIVENESS_STATUS = Object.freeze({
  /** A process matching the record's identity is running as of the snapshot. */
  ALIVE: 'alive',
  /** The record's process is provably gone; reap it without waiting for a TTL. */
  CONFIRMED_DEAD: 'confirmed-dead',
  /** Not decidable from this evidence; defer to the ledger's own TTL. */
  UNKNOWN: 'unknown',
});

/** Stable machine tokens explaining a verdict, safe to render into a log line. */
export const LIVENESS_REASON = Object.freeze({
  /** A live process carries the record's pid and a start time within tolerance. */
  PID_MATCHED: 'pid-matched',
  /** No process in the snapshot carries the record's pid. */
  PID_ABSENT: 'pid-absent',
  /** A process carries the record's pid, but started outside the tolerance. */
  PID_REUSE: 'pid-reuse',
  /** The record carries no usable `pid`/`pidStartedAt` pair. */
  NO_PID: 'no-pid',
  /** No process snapshot was available to correlate against. */
  NO_SNAPSHOT: 'no-snapshot',
  /**
   * A snapshot WAS supplied and DID carry a row at the record's pid, but that
   * row's own `startedAt` is unreadable or impossible, so the row proves
   * nothing either way.
   * Distinct from `no-snapshot` on purpose, so a reader of a verdict can tell
   * that the snapshot arrived and the row is the broken part rather than going
   * looking for a failed discovery call that never happened. NOTE that no
   * caller renders it today: `cli.mjs`'s sweep logs reasons only for records it
   * REAPS, and every `unknown` reason — this one, `no-pid`, `no-snapshot`,
   * `reserved-entry` — is therefore observable from this module's own spec and
   * nowhere else. The distinction is worth keeping for the day a caller does
   * surface it; it is not one an operator can act on now.
   */
  MALFORMED_PROCESS_ROW: 'malformed-process-row',
  /** The record is a reserved `__claim:<type>__` container row, not a process. */
  RESERVED_ENTRY: 'reserved-entry',
  /**
   * The record's own write stamp (`declaredAt` on a dibs entry, `claimedAt` on
   * a claim record) is at or after the snapshot the sweep is judging against,
   * so the snapshot predates the record and cannot speak to it at all.
   *
   * NOT REACHED BY `classifyLedgerEntryLiveness`, and that is deliberate: this
   * classifier sees one record and one snapshot and has no notion of a write
   * stamp, so it would have to grow a ledger-shape dependency to reach it. The
   * token lives here, beside the reasons it belongs with, so a sweep that
   * defers on this ground reports it in the same vocabulary as every other
   * verdict rather than inventing a private string. `cli.mjs`'s
   * `reconcileLedgers` is the one producer — see the RESPONSIBILITY BOUNDARY
   * block above.
   */
  WRITTEN_AFTER_SNAPSHOT: 'written-after-snapshot',
  /**
   * Phase 2's new fail-conservative path: the record's `pid` matches no
   * row in the main parsed snapshot, but DOES match a `{ pid, rawLine }`
   * entry in `liveProcesses.unparseableRows` — the additive per-row
   * provenance signal `listAgentProcesses` (`./recon.mjs`) attaches to the
   * same array it returns. That row's own pid field WAS
   * recoverable, but the rest of the row failed to parse (the
   * minority-garble residual named in this module's WHAT REMAINS UNCOVERED
   * block above), so this classifier has no trustworthy `startedAt` to
   * compare against and cannot decide `alive` or `confirmed-dead`. Distinct
   * from `PID_ABSENT` (no row anywhere named this pid) and from
   * `MALFORMED_PROCESS_ROW` (a row WAS matched via the main lookup, but its
   * own `startedAt` was unreadable) — this reason means a row was found only
   * via the SEPARATE `unparseableRows` lookup, never via the main snapshot
   * array.
   *
   * A proper member of this enum, not a standalone export: this is a
   * `LIVENESS_REASON` token like any other, and keeping it off the frozen
   * object would silently omit it from any future `Object.values` /
   * `Object.entries` walk of "every reason this classifier can return" (a
   * reason→message map, a JSON-schema enum, a logging categoriser).
   */
  PID_ROW_UNPARSEABLE: 'pid-row-unparseable',
});

/**
 * A real number: not a string that looks like one, not `NaN`, not infinite.
 *
 * @param {unknown} value
 * @returns {value is number}
 */
function isUsableNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

/**
 * Largest value a POSIX `pid_t` can hold. `pid_t` is a signed 32-bit integer on
 * every platform this runs on, so no kernel can issue a pid above this and no
 * `ps` row can carry one. Not a tuning knob: it is a property of the type, and
 * real per-host pid ceilings (macOS's ~99999, Linux's `pid_max`) sit far below
 * it. Deliberately the loosest bound that is still a FACT rather than a guess,
 * because this predicate's job is to exclude the impossible, not the unlikely.
 */
const MAX_PID_VALUE = 2 ** 31 - 1;

/**
 * A value shaped like a pid a kernel could actually have handed out: an integer
 * in `1 .. MAX_PID_VALUE`. Finiteness alone is not enough — `0`, `-1`, `4242.5`,
 * `1e21` and `Number.MAX_SAFE_INTEGER` are all finite, and none of them can
 * ever match a row in a process snapshot, so admitting any of them to the pid
 * lookup would convert a corrupt ledger field into a death certificate.
 *
 * @param {unknown} value
 * @returns {value is number}
 */
function isPidLike(value) {
  return typeof value === 'number' && Number.isInteger(value) && value > 0 && value <= MAX_PID_VALUE;
}

/**
 * Maximum time value the ECMAScript language can represent as a `Date`
 * (ECMA-262 §21.4.1.1): ±8.64e15 ms, roughly ±273,790 years around the epoch.
 * No `Date` anywhere in this process can exceed it, so no epoch-ms instant that
 * ever named a real process start can either. Not a tuning knob and not a
 * plausibility judgement: real instants sit nine orders of magnitude below it.
 * Deliberately the loosest bound that is still a FACT rather than a guess,
 * matching `MAX_PID_VALUE`'s posture — exclude the impossible, not the unlikely.
 *
 * A SEPARATE CONSTANT FROM `MAX_PID_VALUE`, AND IT MUST STAY THAT WAY. The two
 * describe different quantities and sit five orders of magnitude apart: a real
 * epoch-ms instant (~1.7e12 in the 2020s) is far above the 32-bit `pid_t`
 * ceiling, so reusing `MAX_PID_VALUE` here would reject every real timestamp and
 * push every well-formed record to `unknown`.
 */
const MAX_EPOCH_MS_VALUE = 8.64e15;

/**
 * A value shaped like an epoch-ms instant: a positive integer no greater than
 * `MAX_EPOCH_MS_VALUE`. NOT bounded by `MAX_PID_VALUE` — a timestamp and a pid
 * share a type and nothing else, and a 32-bit ceiling would reject every real
 * millisecond instant since 1970.
 *
 * The ceiling is ABSOLUTE: it holds for both operands of the start-time
 * comparison — an entry's `pidStartedAt` and a matched snapshot row's
 * `startedAt` — and it does not depend on `snapshotAt` being usable. That is
 * what distinguishes it from the relative bound in `postdatesSnapshot`, which a
 * broken `snapshotAt` disables; `1e21` is impossible whatever the caller's clock
 * says, so nothing about the caller's clock should be able to admit it.
 *
 * @param {unknown} value
 * @returns {value is number}
 */
function isEpochMsLike(value) {
  return (
    typeof value === 'number' &&
    Number.isInteger(value) &&
    value > 0 &&
    value <= MAX_EPOCH_MS_VALUE
  );
}

/**
 * Whether an epoch-ms instant claims to postdate the snapshot that observed it,
 * beyond the tolerance the comparison already grants. A process cannot start
 * after the `ps` run that saw it, so such an instant is PROVABLY impossible
 * rather than merely surprising — and, being impossible, it can never land
 * inside the tolerance window, so admitting it to rule 5 converts a corrupt
 * field into a `pid-reuse` death certificate for a process sitting alive in the
 * snapshot. This is the RELATIVE half of that exclusion: it catches instants
 * that are impossible only in relation to `snapshotAt` — `8.64e15`, or a value
 * one day past the snapshot — which `isEpochMsLike`'s absolute ceiling cannot
 * reach. Instants above that ceiling never arrive here at all; `isEpochMsLike`
 * has already rejected them.
 *
 * Deliberately ONE-SIDED. A far-PAST instant is not impossible and stays
 * decisive: an entry naming a start time long before the matched row's is the
 * pid-reuse signal itself, and the tradeoff documented under CROSS-BEAT
 * WALL-CLOCK STEP depends on it. This closes the impossible half only.
 *
 * Skipped entirely unless `snapshotAt` is itself shaped like an epoch-ms
 * instant. A caller threading a broken clock supplies no trustworthy reference
 * point, so there is nothing to measure "the future" against; measuring anyway
 * would turn one bad argument into a blanket `unknown` for every record. A
 * `snapshotAt` of `-5` treated as an enforced upper bound would do exactly that:
 * one bad caller argument costing every ledger its immediate reclamation.
 *
 * WHAT THE SKIP COSTS, NAMED RATHER THAN LEFT IMPLICIT. Under a bad clock this
 * bound stops running, so instants it would otherwise have excluded reach rules
 * 4 and 5 again and can still read as `pid-absent` or `pid-reuse`. The only
 * thing still bounding them is the absolute ceiling in `isEpochMsLike`, which
 * runs regardless of `snapshotAt`. So what survives the skip is EVERY instant up
 * to and including `MAX_EPOCH_MS_VALUE` — not merely a start time in the
 * snapshot's near future, but also instants tens of thousands of years out that
 * only a working reference point could have shown to be absurd. Solely instants
 * ABOVE that ceiling, such as `1e21`, never arrive here at all. The residual is
 * therefore wider than "plausible-looking values", and it is the price of not
 * letting one bad caller argument blanket-`unknown` every record.
 *
 * @param {number} instantMs
 * @param {unknown} snapshotAt
 * @param {number} toleranceMs
 * @returns {boolean}
 */
function postdatesSnapshot(instantMs, snapshotAt, toleranceMs) {
  return isEpochMsLike(snapshotAt) && instantMs > snapshotAt + toleranceMs;
}

/**
 * @param {string} status
 * @param {string} reason
 * @param {number} snapshotAt
 * @returns {{ status: string, reason: string, snapshotAt: number }}
 */
function verdict(status, reason, snapshotAt) {
  return { status, reason, snapshotAt };
}

/**
 * Classifies one ledger record's liveness as of a given process snapshot.
 *
 * Reads only its arguments: no `ps`, no `Date.now()`, no file access. Mutates
 * neither the record nor the snapshot — a match is never "consumed", so any
 * number of records sharing one live pid each classify independently. That
 * sharing is the common path rather than a corner case, since every ledger
 * write from one orchestrator beat carries the same claude-rooted ancestor
 * pid.
 *
 * @param {Record<string, unknown>} entry One ledger record: a dibs entry, a
 *   `live-agent` claim record, or a `live-agent-admission` claim record.
 * @param {Array<{ agentId: string, rssMb: number, startedAt: number }> | null | undefined} liveProcesses
 *   `listAgentProcesses`' own output, or `null`/`undefined` when the snapshot
 *   could not be taken. An empty array is a valid snapshot of zero agents.
 * @param {number} snapshotAt Epoch-ms instant the snapshot was taken; echoed
 *   back on the verdict so a caller can reason about how stale it is, and used
 *   as the upper bound no start time on either side may exceed. When it is not
 *   itself shaped like an epoch-ms instant that RELATIVE bound is skipped rather
 *   than enforced against an untrustworthy reference point; the absolute
 *   maximum-representable-time ceiling applies to both sides either way.
 * @param {{ toleranceMs?: number }} [options] `toleranceMs` overrides
 *   `PID_IDENTITY_TOLERANCE_MS` for this call. It is a TEST SEAM: production
 *   wiring passes no override, so `PID_IDENTITY_TOLERANCE_MS` stays the single
 *   place an owner tightens the rule. A negative value is ignored in favour of
 *   the constant — it could only turn a matching live identity into a
 *   `pid-reuse` death certificate.
 * @returns {{ status: string, reason: string, snapshotAt: number }} A
 *   `LIVENESS_STATUS` value, a `LIVENESS_REASON` token, and `snapshotAt`.
 */
export function classifyLedgerEntryLiveness(entry, liveProcesses, snapshotAt, options = {}) {
  // Only a plain object can carry a ledger identity. A caller iterating a
  // hand-edited file can hand this function whatever `JSON.parse` produced for
  // a row, including a bare primitive or `null`; substituting an empty record
  // makes every such input absent identity rather than a thrown error or an
  // identity read off something that is not a record.
  const record = entry !== null && typeof entry === 'object' ? entry : {};

  // Rule 1 — reserved rows, checked ahead of the snapshot so a `__claim:<type>__`
  // container is exempt whether or not discovery succeeded.
  //
  // DEFENCE IN DEPTH, not a live path (pre-PR review, Medium — this comment
  // previously implied otherwise). Neither sweep entry point can hand a
  // container row to this function: `readLedgerCandidateRecords` filters
  // reserved ids out of the dibs pre-pass and returns `claims[]` directly for a
  // claim ledger, and `reapConfirmedDeadRecords` returns `true` for a reserved
  // entry before calling `isDead` at all (both `./coordination-file.mjs`). The
  // check stays because this is an exported, independently-callable classifier
  // and the failure it prevents is reaping a container row.
  //
  // What it DOES fire on is a claim record INSIDE `claims[]` whose own
  // `orchestratorId` is reserved-shaped: it is a claim, not a container, but it
  // reads as a container here and can only ever age out by TTL.
  //
  // THE REACHABLE CASE IS THE LEGACY SENTINEL, NOT AN ARBITRARY `'__foo__'`
  // (pre-PR review, Low — this comment said `'__foo__'` "is an ordinary
  // orchestrator id" in `coordination-file.mjs`, and it is not: all three claim
  // write paths there — `claimCapacity`, `releaseCapacity`, `reserveAdmission`
  // — reject a reserved-shaped `orchestratorId` outright with a `TypeError`, so
  // no supported write can produce one). What DOES reach this branch is
  // `coordination-file.mjs`'s own synthesised holder for a migrated pre-existing
  // scalar record, `LEGACY_UNATTRIBUTED_CLAIM_HOLDER_ID`
  // (`'__legacy-unattributed-claim__'`), plus anything a hand-edit put there.
  // Fail-safe in direction either way — a record is spared, never reaped — and
  // left as-is rather than given a ledger-shape parameter this module
  // deliberately does not take.
  if (isReservedEntry(record.orchestratorId)) {
    return verdict(LIVENESS_STATUS.UNKNOWN, LIVENESS_REASON.RESERVED_ENTRY, snapshotAt);
  }

  // Rule 2 — no snapshot to correlate against. `Array.isArray` is the test, so
  // an empty array falls through as the valid observation it is.
  if (!Array.isArray(liveProcesses)) {
    return verdict(LIVENESS_STATUS.UNKNOWN, LIVENESS_REASON.NO_SNAPSHOT, snapshotAt);
  }

  // Resolved before rule 3, because rule 3's own future-instant bound is stated
  // in terms of it. A negative override is not honoured: it can only ever narrow
  // the match to nothing and convert a live identity into a `pid-reuse` death
  // certificate.
  const overrideMs = options?.toleranceMs;
  const toleranceMs =
    isUsableNumber(overrideMs) && overrideMs >= 0 ? overrideMs : PID_IDENTITY_TOLERANCE_MS;

  // Rule 3 — a complete identity pair, both halves shaped like the thing they
  // claim to be. Only a value that COULD name a real process is allowed to
  // reach rules 4 and 5, where an unmatched lookup or an out-of-tolerance start
  // time means `confirmed-dead`; anything finite-but-impossible would miss
  // there by construction and be reaped on the strength of its own corruption.
  //
  // `Object.hasOwn` on both halves before reading either: a polluted
  // `Object.prototype.pid` / `Object.prototype.pidStartedAt` would otherwise
  // lend a well-shaped identity to a record that carries none, and an inherited
  // identity that matches nothing in the snapshot reads as `pid-absent` —
  // `confirmed-dead`. No pollution primitive reaches this module today
  // (`JSON.parse` materialises `__proto__` as an own data property), so this is
  // defence in depth on a boundary where the failure mode is killing a live
  // agent, not a fix for a live path.
  //
  // Each half is gated SEPARATELY and both gates are load-bearing on their own:
  // a record with an own `pid` and no own `pidStartedAt` carries half an
  // identity and must stay `no-pid`, and a record with the halves the other way
  // round must too. The two matching gates on the snapshot side — rule 4's
  // `agentId` and rule 5's `startedAt` — complete the set: all four operands of
  // the identity comparison are read the same way, because whichever one is
  // read laxer is the one a corrupt or inherited value enters through.
  const pid = Object.hasOwn(record, 'pid') ? record.pid : undefined;
  const pidStartedAt = Object.hasOwn(record, 'pidStartedAt') ? record.pidStartedAt : undefined;
  if (
    !isPidLike(pid) ||
    !isEpochMsLike(pidStartedAt) ||
    postdatesSnapshot(pidStartedAt, snapshotAt, toleranceMs)
  ) {
    return verdict(LIVENESS_STATUS.UNKNOWN, LIVENESS_REASON.NO_PID, snapshotAt);
  }

  // Rule 3.5 — Phase 2: a SEPARATE, INDEPENDENT lookup against
  // `liveProcesses.unparseableRows`, the additive per-row provenance signal
  // `listAgentProcesses` attaches to the array it returns (Phase 1). Checked
  // ahead of rule 4's main lookup so a pid recoverable only from a garbled row
  // takes precedence over falling through to `PID_ABSENT` — the entire point
  // of this rule.
  //
  // DELIBERATELY NOT FOLDED INTO THE MAIN LOOKUP (Convergence Analysis edge
  // case #4). Synthesising a row for each `unparseableRows` entry and pushing
  // it into the array rule 4 searches would let it fall through into rule 5's
  // `startedAt` check and misclassify as `MALFORMED_PROCESS_ROW` instead of
  // this rule's own distinct reason. Keeping the lookup entirely separate —
  // its own `Array.isArray` guard, its own `.find`, no mutation of
  // `liveProcesses` or of the main lookup's inputs — means this rule can never
  // be reached via, and never interferes with, rule 4/5's classification of
  // any other entry.
  //
  // `unparseableRows` is OPTIONAL on `liveProcesses`: a pre-Phase-1 caller, or
  // a hand-built fixture with no such property, must classify exactly as
  // before — `Array.isArray` on the property (not merely truthiness) is one
  // half of the guard.
  //
  // The other half is `Object.hasOwn`, and it is read that way for exactly the
  // reason rules 3, 4 and 5 read THEIR operands that way: every property this
  // classifier reads off a caller-supplied object is gated by presence on the
  // object itself, never by prototype lookup. A polluted
  // `Object.prototype.unparseableRows` holding so much as one `{ pid }` object
  // would otherwise lend a garbled-row alibi to a snapshot that carries none,
  // and every ledger record whose pid it names would classify UNKNOWN on every
  // beat — un-reapable by the sweep, and reachable only by its ledger's own
  // TTL. Its direction is the fail-safe one (a record is spared, never reaped),
  // which is why this is defence in depth rather than a fix for a live path —
  // the same standing this module gives rule 3's and rule 5's `hasOwn` gates.
  // `Object.defineProperty` in `listAgentProcesses` creates an OWN property, so
  // a genuine Phase-1 snapshot passes this gate unchanged.
  //
  // Note that rule 2's `Array.isArray(liveProcesses)` is NOT the precedent
  // here: that guards an ARGUMENT, where there is no prototype chain to walk.
  // The precedent for a property read is rule 3/4/5's `Object.hasOwn`.
  //
  // Only entries carrying a non-null NUMERIC pid are attributable (edge case
  // #2): `pid: null` means the garbled row's own pid field could not be
  // recovered at all, so there is nothing to correlate to any specific ledger
  // entry and it must protect nothing — the pre-existing `PID_ABSENT`
  // verdict for such an entry is correct, accepted behaviour, not a gap this
  // rule closes. `String(...)` coercion mirrors rule 4's own comparison
  // discipline so the two lookups agree on what "the same pid" means.
  if (Object.hasOwn(liveProcesses, 'unparseableRows') && Array.isArray(liveProcesses.unparseableRows)) {
    const unparseableMatch = liveProcesses.unparseableRows.find(
      (row) =>
        row !== null &&
        typeof row === 'object' &&
        Object.hasOwn(row, 'pid') &&
        typeof row.pid === 'number' &&
        String(row.pid) === String(pid),
    );
    if (unparseableMatch !== undefined) {
      return verdict(LIVENESS_STATUS.UNKNOWN, LIVENESS_REASON.PID_ROW_UNPARSEABLE, snapshotAt);
    }
  }

  // Rule 4 — `listAgentProcesses` reports root pids as strings; the ledger
  // stores them as numbers. Both sides are coerced to strings so the comparison
  // is symmetric, rather than trusting only the snapshot to have the right
  // type. Coercion is `String(...)` on both operands and comparison is `===`:
  // a loose `==` would additionally match ` 4242` and `0x1092` against the pid
  // `4242`, which widens what counts as the SAME process on the strength of
  // JavaScript's coercion table rather than any property of the identity.
  // `[4242]` is NOT one of those additions — it matches under either
  // comparison, because `String()` and `==` agree on a single-element array,
  // and the spec pins that as a reap (`snapshot row agentId :: a one-element
  // array holding the live pid` in EXPECTED_REAPS). `true` matches under
  // neither: `true == 4242` is false. `find` reads the array without touching
  // it. The callback parameter is NOT named `process`: shadowing the Node
  // global inside a module family whose central invariant is "derive no
  // identity from `process.pid`" would let a later edit read a snapshot row
  // while looking like it read the global.
  //
  // `Object.hasOwn` on the row's `agentId`, exactly as rule 3 demands it of the
  // record's `pid`. This is the snapshot half of that guard and it is not
  // optional: a polluted `Object.prototype.agentId` would otherwise lend a
  // matching identity to a row that carries none, so a row for some other
  // process — or a row that names no process at all — would answer the lookup
  // for a pid the snapshot never held, and that row's start time would then
  // decide `alive` or `pid-reuse` on its behalf.
  //
  // `agentId` is deliberately gated by presence and coercion ALONE — it is the
  // one operand with no `isPidLike`-style shape mirror, and that asymmetry is
  // the fail-conservative posture rather than a gap in it. The other three
  // operands fail SAFE when their shape gate rejects: a rejected `pid` or
  // `pidStartedAt` is `no-pid`, a rejected row `startedAt` is
  // `malformed-process-row`, both `unknown`. A rejected `agentId` fails the
  // other way — the row stops answering the lookup, the lookup misses, and the
  // record is reaped `pid-absent`. So `typeof candidate.agentId === 'string'`
  // would convert every non-string-but-coercible row naming a LIVE process into
  // a death certificate for it; `9001 === '9001'` is false, and a numeric
  // `agentId` row is exactly the case pinned as ALIVE in this module's spec.
  // The residual it leaves — a structurally-degraded snapshot carrying a row
  // whose `agentId` is `[4242]`, `new String('4242')` or `{ toString }`,
  // ordered ahead of the genuine row, winning `find` and lending it a foreign
  // start time — is unreachable from `listAgentProcesses`, which emits a parsed
  // `ps` string and cannot emit two rows for one pid. It belongs to the CALLER
  // OBLIGATION stated at the top of this file, which owns the snapshot-wide
  // structural checks this one-record-at-a-time classifier cannot make.
  const pidKey = String(pid);
  const match = liveProcesses.find(
    (candidate) =>
      candidate !== null &&
      typeof candidate === 'object' &&
      Object.hasOwn(candidate, 'agentId') &&
      String(candidate.agentId) === pidKey,
  );
  if (match === undefined) {
    return verdict(LIVENESS_STATUS.CONFIRMED_DEAD, LIVENESS_REASON.PID_ABSENT, snapshotAt);
  }

  // A snapshot row whose start time is not a possible start time carries no
  // evidence of identity either way, so it is treated as no observation of this
  // pid rather than as grounds to reap. Held to the SAME bar as the record's
  // own `pidStartedAt` — a positive integer epoch-ms instant that does not
  // postdate the snapshot — because the laxer of the two sides is the one a
  // corrupt value enters through, and an out-of-tolerance comparison against a
  // corrupt row hands back `pid-reuse` for a process that is alive.
  //
  // `listAgentProcesses` filters unparseable `etime` rows out, so this guards a
  // shape it does not produce — but `startedAt` is computed as
  // `now - etimeSeconds * 1000`, and a caller threading a wrong `now` reaches
  // every rejected shape here: a non-finite `now` yields `NaN`, and a finite
  // but wrong one (`0`, or an instant supplied in seconds) yields zero or a
  // negative. Its reason is `malformed-process-row`, not `no-snapshot`: a
  // snapshot was supplied and a row for this pid was found.
  //
  // Read through `Object.hasOwn` for the same reason rule 3 reads the record's
  // `pidStartedAt` that way. Both operands of the start-time comparison are now
  // gated identically: a polluted `Object.prototype.startedAt` lends a
  // well-shaped instant to a row that carries none, and an inherited far-past
  // instant reads as a start-time disagreement — `pid-reuse`, a death
  // certificate issued to a process sitting alive in the snapshot.
  const rowStartedAt = Object.hasOwn(match, 'startedAt') ? match.startedAt : undefined;
  if (!isEpochMsLike(rowStartedAt) || postdatesSnapshot(rowStartedAt, snapshotAt, toleranceMs)) {
    return verdict(LIVENESS_STATUS.UNKNOWN, LIVENESS_REASON.MALFORMED_PROCESS_ROW, snapshotAt);
  }

  // Rule 5 — same pid, same start time within tolerance, same process. Both
  // operands are now known to be possible instants, so the only way past this
  // point is a genuine disagreement about when the process started.
  if (Math.abs(rowStartedAt - pidStartedAt) <= toleranceMs) {
    return verdict(LIVENESS_STATUS.ALIVE, LIVENESS_REASON.PID_MATCHED, snapshotAt);
  }

  return verdict(LIVENESS_STATUS.CONFIRMED_DEAD, LIVENESS_REASON.PID_REUSE, snapshotAt);
}
