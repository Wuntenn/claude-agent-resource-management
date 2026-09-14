// Phase 2 — ghost-entry liveness classification, pinned at the pure
// function boundary.
//
// UNIT UNDER TEST
//   `classifyLedgerEntryLiveness(entry, liveProcesses, snapshotAt, options)`
//   from `./reconciliation.mjs` — the sole export this file exercises, plus the
//   `LIVENESS_STATUS`, `LIVENESS_REASON` and `PID_IDENTITY_TOLERANCE_MS`
//   constants it imports rather than restates.
//
// WHY A SEPARATE MODULE: the classifier correlates ledger identity against
// process discovery. Putting it in `./coordination-file.mjs` would give the
// ledger-write module a dependency on process discovery it has no other reason
// to carry; `./reconciliation.mjs` keeps that seam.
//
// THE CONTRACT THIS FILE PINS
//   - `entry` is ONE ledger record: a dibs entry, a `live-agent` claim record,
//     or a `live-agent-admission` claim record. The same function serves all
//     three; the type is not a parameter, because the classification rule does
//     not vary by ledger.
//   - `liveProcesses` is `listAgentProcesses`' own output — an array of
//     `{agentId, rssMb, startedAt}`, where `agentId` is a claude-rooted ROOT
//     PID as a string — or `null`/`undefined` when the snapshot could not be
//     taken at all. The classifier never shells out for it itself, which is
//     what keeps it a pure function and lets one beat's snapshot serve every
//     entry in every ledger.
//   - `snapshotAt` is the epoch-ms instant that snapshot was taken. The
//     classifier reads no clock of its own: its verdict is a statement about
//     snapshot time, and the caller — not the classifier — owns how stale that
//     verdict is by the time a write commits.
//   - The verdict is `{status, reason, snapshotAt}`, with `status` one of
//     `alive` / `confirmed-dead` / `unknown` and `reason` a stable machine
//     token `cli.mjs`'s `reconcileLedgers` renders into its stderr reap log,
//     one line per reaped record.
//
// FIXTURE DISCIPLINE: every pid and timestamp below is a LITERAL. None is
// derived from this test process's own `process.pid` / `process.uptime()` /
// `Date.now()`. A fixture derived from the test process's identity would let
// an implementation that reads its own identity internally pass by
// coincidence — and a self-derived ledger identity is precisely the defect
// this ticket's design exists to rule out (a `node cli.mjs` pid can never
// appear in a claude-rooted-only snapshot, so a self-recorded pid would read
// as confirmed-dead on the very next beat and reap its own live orchestrator).
//
// TWO RESIDUAL LIMITATIONS, KEPT DISTINCT: this file names both, separately,
// in two separate describe blocks, because they are structurally different
// failure modes and collapsing them hides one behind the other:
//   (1) DISCOVERY-WALK OMISSION — a live agent on this host, in this PID
//       namespace, that `listAgentProcesses`' ancestry walk fails to
//       enumerate. A bug in the walk (hardened it), on the same host.
//   (2) PID-NAMESPACE BOUNDARY — a pid recorded by a process the sweep's own
//       `ps` cannot see across at all (a container boundary). Not a bug in the
//       walk: a boundary the walk is structurally unable to cross.
// Both read to this classifier as "pid absent from snapshot", so both produce
// confirmed-dead. That is asserted below AS THE DOCUMENTED BEHAVIOUR, not as a
// defect for this ticket to close.

import { readFileSync } from 'node:fs';

import { jest } from '@jest/globals';

import { isReservedEntry } from './coordination-file.mjs';
import { DEFAULT_PROBE_TIMEOUT_MS } from './probe-bound.mjs';
import { listAgentProcesses } from './recon.mjs';
import {
  LIVENESS_REASON,
  LIVENESS_STATUS,
  PID_IDENTITY_TOLERANCE_MS,
  classifyLedgerEntryLiveness,
} from './reconciliation.mjs';

// Mirrors `cli.mjs`'s real exports. `cli.mjs` self-executes on import, so unit
// specs in this directory restate its constants rather than importing it.
const LIVE_AGENT_CLAIM_TYPE = 'live-agent';
const LIVE_AGENT_ADMISSION_CLAIM_TYPE = 'live-agent-admission';

// ---------------------------------------------------------------------------
// Literal fixtures.
// ---------------------------------------------------------------------------

/**
 * `etime`'s reporting granularity. `ps` prints elapsed time in whole seconds,
 * so a derived `startedAt` floors away up to this much of the process's real
 * age. Restated here rather than imported because `reconciliation.mjs` keeps
 * it module-private — the exported surface is the tolerance it feeds, and a
 * test that imported the input could not fail if the two stopped agreeing.
 */
const ETIME_GRANULARITY_MS = 1000;

/** The instant the beat's `ps` snapshot was taken. */
const SNAPSHOT_AT = Date.parse('2026-08-28T12:00:00Z');

/** A claude-rooted orchestrator process that is genuinely running. */
const LIVE_PID = 9001;
const LIVE_STARTED_AT = Date.parse('2026-08-28T09:00:00Z');

/** A pid recorded before a host death — nothing runs at this number now. */
const GHOST_PID = 4242;
const GHOST_STARTED_AT = Date.parse('2026-08-28T06:00:00Z');

/** A snapshot holding exactly one live claude-rooted process. */
function liveSnapshot() {
  return [{ agentId: String(LIVE_PID), rssMb: 512, startedAt: LIVE_STARTED_AT }];
}

/** A dibs entry carrying an already-resolved identity pair. */
function dibsEntry(pid, pidStartedAt) {
  return {
    orchestratorId: 'orchestrator-a',
    declaredAt: Date.parse('2026-08-28T11:59:00Z'),
    firstDeclaredAt: Date.parse('2026-08-28T09:00:05Z'),
    desiredAgents: 2,
    ...(pid === undefined ? {} : { pid }),
    ...(pidStartedAt === undefined ? {} : { pidStartedAt }),
  };
}

/** A per-claim record as `claimCapacity` / `reserveAdmission` append it. */
function claimRecord(pid, pidStartedAt) {
  return {
    orchestratorId: 'orchestrator-a',
    count: 2,
    claimedAt: Date.parse('2026-08-28T11:59:00Z'),
    ...(pid === undefined ? {} : { pid }),
    ...(pidStartedAt === undefined ? {} : { pidStartedAt }),
  };
}

/** Recursively freezes an object graph so any write attempt throws. */
function deepFreeze(value) {
  if (value !== null && typeof value === 'object' && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const inner of Object.values(value)) deepFreeze(inner);
  }
  return value;
}

const RECONCILIATION_SOURCE_PATH = new URL('./reconciliation.mjs', import.meta.url);

/**
 * Flattens module source into one whitespace-normalized line, dropping the
 * leading `*` or `//` of comment lines, so prose assertions survive any line
 * wrapping or comment style the author chooses.
 */
function flattenComments(source) {
  return source.replace(/^\s*(?:\*|\/\/)/gm, ' ').replace(/\s+/g, ' ');
}

/** Headings that delimit the module's residual-limitation blocks. */
const WALK_OMISSION_HEADING = 'RESIDUAL LIMITATION ONE — DISCOVERY-WALK OMISSION';
const NAMESPACE_BOUNDARY_HEADING = 'RESIDUAL LIMITATION TWO — PID-NAMESPACE BOUNDARY';
const COINCIDENTAL_HEADING = 'RESIDUAL LIMITATION THREE';
/** The heading that follows limitation three, bounding its block from below. */
const RESPONSIBILITY_BOUNDARY_HEADING = 'RESPONSIBILITY BOUNDARY';

/**
 * Slices the module doc comment into the three residual-limitation blocks by
 * their headings, so each can be asserted on its OWN content. Returns `''` for
 * a block whose heading is missing — which is what a merged-into-one rewrite,
 * or an outright deletion, produces, and what makes either fail rather than
 * pass.
 */
function residualLimitationBlocks() {
  const prose = flattenComments(readFileSync(RECONCILIATION_SOURCE_PATH, 'utf8'));
  const one = prose.indexOf(WALK_OMISSION_HEADING);
  const two = prose.indexOf(NAMESPACE_BOUNDARY_HEADING);
  const three = prose.indexOf(COINCIDENTAL_HEADING);
  const afterThree = prose.indexOf(RESPONSIBILITY_BOUNDARY_HEADING, three === -1 ? 0 : three);

  const empty = { walkOmission: '', namespaceBoundary: '', coincidental: '' };
  if (one === -1 || two === -1 || two < one) return empty;

  return {
    walkOmission: prose.slice(one, two),
    namespaceBoundary: prose.slice(two, three === -1 ? undefined : three),
    coincidental:
      three === -1 ? '' : prose.slice(three, afterThree === -1 ? undefined : afterThree),
  };
}

// ---------------------------------------------------------------------------
// 1. The classification rule, branch by branch.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — classification rule', () => {
  it('classifies an entry with no pid as unknown, leaving it to the existing TTL', () => {
    const verdict = classifyLedgerEntryLiveness(dibsEntry(), liveSnapshot(), SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(verdict.reason).toBe(LIVENESS_REASON.NO_PID);
  });

  it('classifies an entry whose pid is absent from the live snapshot as confirmed-dead', () => {
    const verdict = classifyLedgerEntryLiveness(
      dibsEntry(GHOST_PID, GHOST_STARTED_AT),
      liveSnapshot(),
      SNAPSHOT_AT,
    );

    expect(verdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_ABSENT);
  });

  it('treats an empty snapshot as a successful observation of zero live agents, not as an absent snapshot', () => {
    const verdict = classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, LIVE_STARTED_AT), [], SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_ABSENT);
  });

  it('classifies an entry whose pid is present with a matching startedAt as alive', () => {
    const verdict = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, LIVE_STARTED_AT),
      liveSnapshot(),
      SNAPSHOT_AT,
    );

    expect(verdict.status).toBe(LIVENESS_STATUS.ALIVE);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_MATCHED);
  });

  it('classifies a matching entry as alive however old it is — the verdict is age-blind', () => {
    // Declared six hours before the snapshot: past `DEFAULT_CLAIM_TTL_MS` (6h)
    // and far past the 90s admission freshness window. The process is running,
    // so the classifier reaches `alive` regardless of the record's age — it
    // never reads a write stamp. This is what makes recovery immediate in one
    // direction without making it destructive in the other.
    //
    // NOT a TTL exemption, and this test does not claim to be one: it pins the
    // classifier's VERDICT, which is all this module produces. Whether the
    // entry then survives in the file is decided by the ledger's own
    // pre-existing age-based prunes (`pruneStale`, the claim TTLs), which this
    // module neither reads nor overrides — see the header block.
    const ancient = {
      ...dibsEntry(LIVE_PID, LIVE_STARTED_AT),
      declaredAt: SNAPSHOT_AT - 6 * 60 * 60 * 1000,
      firstDeclaredAt: SNAPSHOT_AT - 6 * 60 * 60 * 1000,
    };

    const verdict = classifyLedgerEntryLiveness(ancient, liveSnapshot(), SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.ALIVE);
  });

  it('classifies an entry whose pid now hosts a different process as confirmed-dead (pid reuse)', () => {
    // Something IS running at this pid — but it started three hours after the
    // process the entry names. Sparing the entry because *a* process exists at
    // that number is the bug this branch rules out.
    const reused = [{ agentId: String(LIVE_PID), rssMb: 128, startedAt: SNAPSHOT_AT - 60 * 1000 }];

    const verdict = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, LIVE_STARTED_AT),
      reused,
      SNAPSHOT_AT,
    );

    expect(verdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_REUSE);
  });

  it('classifies every entry as unknown when the snapshot itself is null, leaving the TTL as the sole safety net', () => {
    const entries = [
      dibsEntry(),
      dibsEntry(GHOST_PID, GHOST_STARTED_AT),
      claimRecord(LIVE_PID, LIVE_STARTED_AT),
    ];

    for (const entry of entries) {
      const verdict = classifyLedgerEntryLiveness(entry, null, SNAPSHOT_AT);

      expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
      expect(verdict.reason).toBe(LIVENESS_REASON.NO_SNAPSHOT);
    }
  });

  it('classifies every entry as unknown when the snapshot argument is omitted entirely', () => {
    const verdict = classifyLedgerEntryLiveness(dibsEntry(GHOST_PID, GHOST_STARTED_AT), undefined, SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(verdict.reason).toBe(LIVENESS_REASON.NO_SNAPSHOT);
  });

  it('matches a snapshot row whose agentId is a NUMBER, coercing both sides rather than only the ledger', () => {
    // `listAgentProcesses` reports `agentId` as a string today, and every other
    // fixture in this file builds it with `String(...)` — which is exactly why
    // this case needs its own test: dropping the `String()` on the SNAPSHOT
    // side of the comparison would pass all of them. The cost of that drop is
    // in the safety-critical direction: `9001 === '9001'` is false, so a
    // numeric-`agentId` row stops matching, the lookup misses, and a live agent
    // is issued a `pid-absent` death certificate.
    const numericAgentIdSnapshot = [{ agentId: LIVE_PID, rssMb: 512, startedAt: LIVE_STARTED_AT }];

    expect(typeof numericAgentIdSnapshot[0].agentId).toBe('number');

    const verdict = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, LIVE_STARTED_AT),
      numericAgentIdSnapshot,
      SNAPSHOT_AT,
    );

    expect(verdict.status).toBe(LIVENESS_STATUS.ALIVE);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_MATCHED);
  });

  it('consumes listAgentProcesses output verbatim, with no reshaping by the caller', () => {
    // Built by the real discovery function from real-shaped `ps` text, so the
    // classifier is pinned against the producer's actual output rather than a
    // hand-written approximation of it. `9001` is the claude-rooted root; the
    // `node` child beneath it is not a root and gets no entry of its own.
    const psText = [
      '  PID  PPID    RSS     ELAPSED COMM',
      '    1     0    512 1-03:46:39 /sbin/launchd',
      ' 9001     1 102400    03:00:00 /Applications/Claude.app/Contents/MacOS/claude',
      ' 9002  9001  51200    02:59:00 node /Applications/Claude.app/Contents/Resources/app/cli.js',
      '',
    ].join('\n');
    const snapshot = listAgentProcesses(psText, SNAPSHOT_AT);
    const [root] = snapshot;

    expect(root.agentId).toBe(String(LIVE_PID));

    expect(classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, root.startedAt), snapshot, SNAPSHOT_AT).status).toBe(
      LIVENESS_STATUS.ALIVE,
    );
    // The `node` child's pid is not a root, so it is invisible to the
    // classifier — which is why ledger identity is the claude-rooted ancestor.
    expect(classifyLedgerEntryLiveness(dibsEntry(9002, root.startedAt), snapshot, SNAPSHOT_AT).status).toBe(
      LIVENESS_STATUS.CONFIRMED_DEAD,
    );
  });
});

// ---------------------------------------------------------------------------
// 2. The tolerance boundary. `PID_IDENTITY_TOLERANCE_MS` is imported, never
//    restated as a literal — one named constant an owner can tighten later.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — PID_IDENTITY_TOLERANCE_MS boundary', () => {
  it('exports a positive, finite tolerance generous enough to absorb etime second-granularity', () => {
    expect(Number.isFinite(PID_IDENTITY_TOLERANCE_MS)).toBe(true);
    expect(PID_IDENTITY_TOLERANCE_MS).toBeGreaterThanOrEqual(1000);
  });

  it('exceeds the per-beat error this design already permits, so ordinary ps variance cannot condemn a live agent', () => {
    // THE RELATIONSHIP, PINNED — not the number. A beat's derived `startedAt`
    // lands LATER than the true start by its `ps` collection duration (ceiling
    // `DEFAULT_PROBE_TIMEOUT_MS`, because `cli.mjs` resolves `snapshotAt` after
    // the collection returns) plus `etime`'s whole-second flooring (< 1000 ms).
    // Two beats at opposite ends of that range disagree by nearly its width
    // with NO clock change involved, so a tolerance at or below the width
    // reaps live agents on a contended host. Fails if anyone sets the constant
    // back below the bound, or moves `DEFAULT_PROBE_TIMEOUT_MS` out from under
    // it.
    expect(PID_IDENTITY_TOLERANCE_MS).toBeGreaterThan(DEFAULT_PROBE_TIMEOUT_MS + ETIME_GRANULARITY_MS);
  });

  it('classifies a startedAt difference of exactly PID_IDENTITY_TOLERANCE_MS as alive', () => {
    const entry = dibsEntry(LIVE_PID, LIVE_STARTED_AT - PID_IDENTITY_TOLERANCE_MS);

    expect(classifyLedgerEntryLiveness(entry, liveSnapshot(), SNAPSHOT_AT).status).toBe(LIVENESS_STATUS.ALIVE);
  });

  it('classifies a startedAt difference of exactly PID_IDENTITY_TOLERANCE_MS in the other direction as alive', () => {
    const entry = dibsEntry(LIVE_PID, LIVE_STARTED_AT + PID_IDENTITY_TOLERANCE_MS);

    expect(classifyLedgerEntryLiveness(entry, liveSnapshot(), SNAPSHOT_AT).status).toBe(LIVENESS_STATUS.ALIVE);
  });

  it('classifies a startedAt difference one millisecond past PID_IDENTITY_TOLERANCE_MS as confirmed-dead', () => {
    const entry = dibsEntry(LIVE_PID, LIVE_STARTED_AT - PID_IDENTITY_TOLERANCE_MS - 1);

    const verdict = classifyLedgerEntryLiveness(entry, liveSnapshot(), SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_REUSE);
  });

  it('accepts an options.toleranceMs override in place of the exported default', () => {
    const entry = dibsEntry(LIVE_PID, LIVE_STARTED_AT - PID_IDENTITY_TOLERANCE_MS - 1);

    const widened = classifyLedgerEntryLiveness(entry, liveSnapshot(), SNAPSHOT_AT, {
      toleranceMs: PID_IDENTITY_TOLERANCE_MS + 1,
    });
    const narrowed = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, LIVE_STARTED_AT - 500),
      liveSnapshot(),
      SNAPSHOT_AT,
      { toleranceMs: 100 },
    );

    expect(widened.status).toBe(LIVENESS_STATUS.ALIVE);
    expect(narrowed.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
  });

  it('ignores a negative toleranceMs rather than letting it kill an exactly-matching live identity', () => {
    // The override is a test seam, and the only direction it can do harm in is
    // narrowing: a negative tolerance makes `Math.abs(delta) <= tolerance`
    // unsatisfiable, so a record whose startedAt matches EXACTLY would be
    // issued a `pid-reuse` death certificate for a process that is running.
    // The constant wins instead.
    const exactMatch = dibsEntry(LIVE_PID, LIVE_STARTED_AT);

    for (const toleranceMs of [-1, -PID_IDENTITY_TOLERANCE_MS, Number.NEGATIVE_INFINITY]) {
      const verdict = classifyLedgerEntryLiveness(exactMatch, liveSnapshot(), SNAPSHOT_AT, { toleranceMs });

      expect(verdict.status).toBe(LIVENESS_STATUS.ALIVE);
      expect(verdict.reason).toBe(LIVENESS_REASON.PID_MATCHED);
    }
  });

  it('honours a zero toleranceMs, which is narrow but not incoherent', () => {
    // Zero is the tightest COHERENT override: exact equality still matches, so
    // it never contradicts itself the way a negative value does.
    const exact = classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, LIVE_STARTED_AT), liveSnapshot(), SNAPSHOT_AT, {
      toleranceMs: 0,
    });
    const offByOne = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, LIVE_STARTED_AT + 1),
      liveSnapshot(),
      SNAPSHOT_AT,
      { toleranceMs: 0 },
    );

    expect(exact.status).toBe(LIVENESS_STATUS.ALIVE);
    expect(offByOne.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
  });

  it('documents options.toleranceMs as a test seam, keeping the exported constant the single place to tighten', () => {
    const prose = flattenComments(readFileSync(RECONCILIATION_SOURCE_PATH, 'utf8'));

    expect(prose).toMatch(/TEST SEAM/);
    expect(prose).toMatch(/production wiring (?:must pass|passes) this constant|production wiring passes no override/i);
  });
});

// ---------------------------------------------------------------------------
// 3. Corrupt identity on disk. The write path persists caller-supplied values
//    verbatim and validates nothing, so a hand-edited ledger file can carry
//    anything. Half an identity is no identity.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — corrupt or partial identity', () => {
  // A pid is a positive integer. Every FINITE-BUT-IMPOSSIBLE value below is the
  // dangerous class: it survives a bare `Number.isFinite` check, then misses the
  // snapshot lookup by construction (no `ps` row can carry `0`, `-1`, `4242.5`
  // or `1e21` as a pid), so a rule that only tested finiteness would hand back
  // `confirmed-dead` on the strength of the corruption itself — even with a
  // matching live process sitting in the snapshot.
  const corruptEntries = [
    ['a non-numeric pid', dibsEntry('nine-thousand-and-one', LIVE_STARTED_AT)],
    ['a null pid', dibsEntry(null, LIVE_STARTED_AT)],
    ['a NaN pid', dibsEntry(Number.NaN, LIVE_STARTED_AT)],
    ['an infinite pid', dibsEntry(Number.POSITIVE_INFINITY, LIVE_STARTED_AT)],
    ['a zero pid', dibsEntry(0, LIVE_STARTED_AT)],
    ['a negative pid', dibsEntry(-1, LIVE_STARTED_AT)],
    ['a fractional pid', dibsEntry(4242.5, LIVE_STARTED_AT)],
    ['a pid beyond the safe integer range', dibsEntry(1e21, LIVE_STARTED_AT)],
    ['a pid at Number.MAX_SAFE_INTEGER', dibsEntry(Number.MAX_SAFE_INTEGER, LIVE_STARTED_AT)],
    ['a non-numeric pidStartedAt', dibsEntry(LIVE_PID, 'this morning')],
    ['a NaN pidStartedAt', dibsEntry(LIVE_PID, Number.NaN)],
    ['an infinite pidStartedAt', dibsEntry(LIVE_PID, Number.POSITIVE_INFINITY)],
    ['a zero pidStartedAt', dibsEntry(LIVE_PID, 0)],
    ['a negative pidStartedAt', dibsEntry(LIVE_PID, -1)],
    ['a fractional pidStartedAt', dibsEntry(LIVE_PID, LIVE_STARTED_AT + 0.5)],
    // The mirror of the impossible-MAGNITUDE pid rows above, on the timestamp
    // side. Each of these is a positive integer, so a bare epoch-ms shape check
    // admits it; each then misses the tolerance window by construction and is
    // issued `pid-reuse` — a death certificate for the live process sitting in
    // `liveSnapshot()` — unless the snapshot instant bounds it from above.
    ['a pidStartedAt one day after the snapshot', dibsEntry(LIVE_PID, SNAPSHOT_AT + 24 * 60 * 60 * 1000)],
    ['a pidStartedAt beyond the safe integer range', dibsEntry(LIVE_PID, 1e21)],
    ['a pidStartedAt at Number.MAX_SAFE_INTEGER', dibsEntry(LIVE_PID, Number.MAX_SAFE_INTEGER)],
    ['a pidStartedAt at the maximum representable Date', dibsEntry(LIVE_PID, 8.64e15)],
    ['a pid with no pidStartedAt beside it', dibsEntry(LIVE_PID, undefined)],
    ['a pidStartedAt with no pid beside it', dibsEntry(undefined, LIVE_STARTED_AT)],
  ];

  it.each(corruptEntries)('treats %s as absent identity and classifies unknown', (_label, entry) => {
    const verdict = classifyLedgerEntryLiveness(entry, liveSnapshot(), SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(verdict.reason).toBe(LIVENESS_REASON.NO_PID);
  });

  it.each(corruptEntries)('never reaps on %s — a corrupt field is never a death certificate', (_label, entry) => {
    expect(classifyLedgerEntryLiveness(entry, liveSnapshot(), SNAPSHOT_AT).status).not.toBe(
      LIVENESS_STATUS.CONFIRMED_DEAD,
    );
  });

  it('never reaps an impossible pid even when a snapshot row literally carries its string form', () => {
    // The sharpest form of the rule: the lookup would SUCCEED for these values
    // if they were let through to it, so the guard cannot be dismissed as "the
    // lookup would have missed anyway". `0`, `-1` and `4242.5` are not pids, so
    // a row claiming one is itself untrustworthy and decides nothing.
    const impossiblePids = [0, -1, 4242.5];
    const snapshot = impossiblePids.map((pid) => ({
      agentId: String(pid),
      rssMb: 128,
      startedAt: LIVE_STARTED_AT,
    }));

    for (const pid of impossiblePids) {
      const verdict = classifyLedgerEntryLiveness(dibsEntry(pid, LIVE_STARTED_AT), snapshot, SNAPSHOT_AT);

      expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
      expect(verdict.reason).toBe(LIVENESS_REASON.NO_PID);
    }
  });

  it('leaves a string-form pid permanently un-reapable, by deliberate choice', () => {
    // `listAgentProcesses` reports `agentId` as a STRING, so `pid: "4242"` is
    // the natural copy-paste slip on the write side. The classifier does not
    // coerce it: an identity stored in the wrong type is not an identity it
    // acts on, so such a record can only age out under its ledger's own TTL and
    // is never reaped early. That is the SAFE direction and it is intentional —
    // pinned here so the behaviour is a decision on the record rather than a
    // silent side effect of a type check.
    const verdict = classifyLedgerEntryLiveness(
      dibsEntry(String(GHOST_PID), GHOST_STARTED_AT),
      liveSnapshot(),
      SNAPSHOT_AT,
    );

    expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(verdict.reason).toBe(LIVENESS_REASON.NO_PID);
  });

  it.each([
    ['null', null],
    ['undefined', undefined],
    ['a number', 4242],
    ['a string', 'orchestrator-a'],
    ['a boolean', true],
  ])('treats a non-object entry (%s) as carrying no identity at all', (_label, entry) => {
    // A caller iterating a hand-edited ledger can hand this function whatever
    // `JSON.parse` produced for a row — including a bare primitive where an
    // object was expected. Reading `.pid` off a primitive yields `undefined`
    // rather than throwing, so without the object guard the function would
    // still answer; the guard is what makes the answer a deliberate one. Every
    // such input is absent identity, never a death certificate.
    const verdict = classifyLedgerEntryLiveness(entry, liveSnapshot(), SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(verdict.reason).toBe(LIVENESS_REASON.NO_PID);
  });

  it('takes identity only from a plain object, not from anything merely carrying the right own properties', () => {
    // The entry guard is a TYPE check, not just a null check, and only a
    // non-`'object'` value that can hold own properties can tell the two apart
    // — a function is the only such value. Loosening the guard to `entry ?? {}`
    // reads identity off it, misses the snapshot lookup, and hands back
    // `confirmed-dead`. `null`, `undefined` and the primitives above pass under
    // either form, so this case is what makes the check testable at all.
    const notALedgerRecord = Object.assign(() => {}, {
      orchestratorId: 'orchestrator-a',
      pid: GHOST_PID,
      pidStartedAt: GHOST_STARTED_AT,
    });

    expect(typeof notALedgerRecord).not.toBe('object');
    expect(Object.hasOwn(notALedgerRecord, 'pid')).toBe(true);

    const verdict = classifyLedgerEntryLiveness(notALedgerRecord, liveSnapshot(), SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(verdict.reason).toBe(LIVENESS_REASON.NO_PID);
  });

  it('classifies a matched snapshot row with an unreadable startedAt as unknown, under its own reason', () => {
    // A snapshot WAS supplied and a row for this pid WAS found — only the row's
    // start time is unreadable, so it proves nothing either way. Reporting
    // `no-snapshot` here would send an operator triaging a deferred record
    // hunting for a discovery failure that never happened.
    const malformed = [{ agentId: String(LIVE_PID), rssMb: 512, startedAt: Number.NaN }];

    const verdict = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, LIVE_STARTED_AT),
      malformed,
      SNAPSHOT_AT,
    );

    expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(verdict.reason).toBe(LIVENESS_REASON.MALFORMED_PROCESS_ROW);
    expect(verdict.reason).not.toBe(LIVENESS_REASON.NO_SNAPSHOT);
  });

  it('gives a matched-but-malformed row a reason distinct from every other unknown path', () => {
    const missingRowStartedAt = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, LIVE_STARTED_AT),
      [{ agentId: String(LIVE_PID), rssMb: 512, startedAt: 'three o clock' }],
      SNAPSHOT_AT,
    );
    const absentSnapshot = classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, LIVE_STARTED_AT), null, SNAPSHOT_AT);
    const absentIdentity = classifyLedgerEntryLiveness(dibsEntry(), liveSnapshot(), SNAPSHOT_AT);

    const reasons = [missingRowStartedAt.reason, absentSnapshot.reason, absentIdentity.reason];

    expect(missingRowStartedAt.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(new Set(reasons).size).toBe(reasons.length);
  });
});

// ---------------------------------------------------------------------------
// 4. Fail-conservative posture, stated as its own invariant.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — fail-conservative posture', () => {
  it('routes every ambiguous input to unknown and never to confirmed-dead', () => {
    // Ambiguity means "the classifier cannot see enough to decide": no
    // identity, a corrupt identity, no snapshot at all, or a reserved row that
    // names no process. A startedAt mismatch is deliberately NOT in this list
    // — that is a decided case, not an ambiguous one (see the pid-reuse and
    // clock-step blocks).
    const ambiguous = [
      [dibsEntry(), liveSnapshot()],
      [dibsEntry(LIVE_PID, undefined), liveSnapshot()],
      [dibsEntry('bad', 'worse'), liveSnapshot()],
      [dibsEntry(GHOST_PID, GHOST_STARTED_AT), null],
      [claimRecord(GHOST_PID, GHOST_STARTED_AT), null],
      [{ orchestratorId: `__claim:${LIVE_AGENT_CLAIM_TYPE}__`, claims: [] }, liveSnapshot()],
    ];

    for (const [entry, snapshot] of ambiguous) {
      expect(classifyLedgerEntryLiveness(entry, snapshot, SNAPSHOT_AT).status).toBe(LIVENESS_STATUS.UNKNOWN);
    }
  });

  it('returns only the three documented statuses', () => {
    const statuses = [LIVENESS_STATUS.ALIVE, LIVENESS_STATUS.CONFIRMED_DEAD, LIVENESS_STATUS.UNKNOWN];
    const cases = [
      [dibsEntry(LIVE_PID, LIVE_STARTED_AT), liveSnapshot()],
      [dibsEntry(GHOST_PID, GHOST_STARTED_AT), liveSnapshot()],
      [dibsEntry(), null],
    ];

    for (const [entry, snapshot] of cases) {
      expect(statuses).toContain(classifyLedgerEntryLiveness(entry, snapshot, SNAPSHOT_AT).status);
    }
  });
});

// ---------------------------------------------------------------------------
// 5. Reserved ledger rows. `__claim:<type>__` is a container, not a process —
//    it names no pid and must never be reaped, mirroring `pruneStale`'s own
//    unconditional exemption. Only the per-claim records inside it are
//    classified.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — reserved ledger rows', () => {
  const reservedIds = [
    `__claim:${LIVE_AGENT_CLAIM_TYPE}__`,
    `__claim:${LIVE_AGENT_ADMISSION_CLAIM_TYPE}__`,
  ];

  it.each(reservedIds)('leaves the reserved row %s unclassified', (orchestratorId) => {
    // Guard: these ids really are the ones `coordination-file.mjs` exempts.
    expect(isReservedEntry(orchestratorId)).toBe(true);

    const reservedRow = {
      orchestratorId,
      declaredAt: SNAPSHOT_AT - 10 * 60 * 60 * 1000,
      grantedTotal: 2,
      claims: [claimRecord(GHOST_PID, GHOST_STARTED_AT)],
    };

    const verdict = classifyLedgerEntryLiveness(reservedRow, liveSnapshot(), SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(verdict.reason).toBe(LIVENESS_REASON.RESERVED_ENTRY);
  });

  it('still reaps a ghost claim record held inside a reserved row', () => {
    // The row is exempt; its contents are not. A sweep classifies the records.
    const reservedRow = {
      orchestratorId: `__claim:${LIVE_AGENT_CLAIM_TYPE}__`,
      claims: [claimRecord(GHOST_PID, GHOST_STARTED_AT), claimRecord(LIVE_PID, LIVE_STARTED_AT)],
    };
    const [ghost, live] = reservedRow.claims;

    expect(classifyLedgerEntryLiveness(ghost, liveSnapshot(), SNAPSHOT_AT).status).toBe(
      LIVENESS_STATUS.CONFIRMED_DEAD,
    );
    expect(classifyLedgerEntryLiveness(live, liveSnapshot(), SNAPSHOT_AT).status).toBe(LIVENESS_STATUS.ALIVE);
  });

  it('ignores a reserved row even when the snapshot is unavailable', () => {
    const reservedRow = { orchestratorId: `__claim:${LIVE_AGENT_CLAIM_TYPE}__`, claims: [] };

    expect(classifyLedgerEntryLiveness(reservedRow, null, SNAPSHOT_AT).status).toBe(LIVENESS_STATUS.UNKNOWN);
  });

  it('reports RESERVED_ENTRY, not NO_SNAPSHOT, when a reserved row meets an absent snapshot', () => {
    // Pins the PRECEDENCE, which `.status` alone cannot: both orderings of the
    // reserved-row check and the snapshot check yield `unknown`, so only the
    // reason distinguishes them. The reserved-row check must come FIRST — a
    // `__claim:<type>__` container names no process, so "no snapshot to
    // correlate against" is not the truth about it under any circumstances, and
    // Phase 3 will render this token into an operator-facing log line.
    const reservedRow = { orchestratorId: `__claim:${LIVE_AGENT_CLAIM_TYPE}__`, claims: [] };

    for (const absentSnapshot of [null, undefined]) {
      const verdict = classifyLedgerEntryLiveness(reservedRow, absentSnapshot, SNAPSHOT_AT);

      expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
      expect(verdict.reason).toBe(LIVENESS_REASON.RESERVED_ENTRY);
    }
  });
});

// ---------------------------------------------------------------------------
// 6. Independence. Every ledger write from one orchestrator beat carries the
//    SAME claude-rooted ancestor pid, so several entries sharing one live pid
//    is the common path, not a corner case. A lookup that "consumes" a match
//    would reap the second entry of every pair.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — entries sharing one live pid', () => {
  it('classifies a dibs entry and a claim record sharing one live pid as alive, independently', () => {
    const snapshot = liveSnapshot();
    const dibs = dibsEntry(LIVE_PID, LIVE_STARTED_AT);
    const claim = claimRecord(LIVE_PID, LIVE_STARTED_AT);

    expect(classifyLedgerEntryLiveness(dibs, snapshot, SNAPSHOT_AT).status).toBe(LIVENESS_STATUS.ALIVE);
    expect(classifyLedgerEntryLiveness(claim, snapshot, SNAPSHOT_AT).status).toBe(LIVENESS_STATUS.ALIVE);
  });

  it('keeps classifying the same live pid as alive across many consecutive calls', () => {
    const snapshot = liveSnapshot();

    for (let call = 0; call < 5; call += 1) {
      expect(
        classifyLedgerEntryLiveness(claimRecord(LIVE_PID, LIVE_STARTED_AT), snapshot, SNAPSHOT_AT).status,
      ).toBe(LIVENESS_STATUS.ALIVE);
    }
  });

  it('reaps only the ghost record when two records under one orchestratorId carry different pids', () => {
    // Crash-and-respawn under one id: `claimCapacity` appends rather than
    // upserting, so both records coexist. Classification is per record.
    const ghost = { ...claimRecord(GHOST_PID, GHOST_STARTED_AT), orchestratorId: 'orchestrator-a' };
    const live = { ...claimRecord(LIVE_PID, LIVE_STARTED_AT), orchestratorId: 'orchestrator-a' };

    expect(classifyLedgerEntryLiveness(ghost, liveSnapshot(), SNAPSHOT_AT).status).toBe(
      LIVENESS_STATUS.CONFIRMED_DEAD,
    );
    expect(classifyLedgerEntryLiveness(live, liveSnapshot(), SNAPSHOT_AT).status).toBe(LIVENESS_STATUS.ALIVE);
  });

  it('mutates neither the snapshot array nor the process objects inside it', () => {
    // Asserted directly, rather than inferred from two calls both returning
    // alive: a stateful "consumed" marker is easy to add and easy to miss.
    const snapshot = deepFreeze(liveSnapshot());
    const before = JSON.stringify(snapshot);

    classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, LIVE_STARTED_AT), snapshot, SNAPSHOT_AT);
    classifyLedgerEntryLiveness(claimRecord(LIVE_PID, LIVE_STARTED_AT), snapshot, SNAPSHOT_AT);
    classifyLedgerEntryLiveness(dibsEntry(GHOST_PID, GHOST_STARTED_AT), snapshot, SNAPSHOT_AT);

    expect(JSON.stringify(snapshot)).toBe(before);
    expect(snapshot).toHaveLength(1);
  });

  it('does not mutate the ledger entry it is given', () => {
    const entry = deepFreeze(dibsEntry(LIVE_PID, LIVE_STARTED_AT));
    const before = JSON.stringify(entry);

    classifyLedgerEntryLiveness(entry, liveSnapshot(), SNAPSHOT_AT);

    expect(JSON.stringify(entry)).toBe(before);
  });
});

// ---------------------------------------------------------------------------
// 7. Snapshot time is the caller's, not the classifier's. The verdict is true
//    as of the snapshot; how stale it is by the time a write commits is the
//    caller's problem to reason about, which it can only do if the classifier
//    hands the timestamp back rather than reading a clock of its own.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — snapshot timestamp threading', () => {
  it('reports the caller-supplied snapshot timestamp on the verdict', () => {
    const verdict = classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, LIVE_STARTED_AT), liveSnapshot(), SNAPSHOT_AT);

    expect(verdict.snapshotAt).toBe(SNAPSHOT_AT);
  });

  it('reads no clock of its own', () => {
    const dateNow = jest.spyOn(Date, 'now');

    try {
      classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, LIVE_STARTED_AT), liveSnapshot(), SNAPSHOT_AT);
      classifyLedgerEntryLiveness(dibsEntry(GHOST_PID, GHOST_STARTED_AT), liveSnapshot(), SNAPSHOT_AT);
      classifyLedgerEntryLiveness(dibsEntry(), null, SNAPSHOT_AT);

      expect(dateNow).not.toHaveBeenCalled();
    } finally {
      dateNow.mockRestore();
    }
  });

  it('echoes back an ABSENT snapshot timestamp rather than defaulting it to the current time', () => {
    // A well-formed `snapshotAt` cannot detect the plausible future
    // convenience `snapshotAt ?? Date.now()`, because the fallback never fires.
    // Only an absent one can. Defaulting would be a clock read smuggled in as
    // ergonomics, and would make the verdict a statement about NOW rather than
    // about the instant the snapshot was actually taken — which is the whole
    // reason the caller threads it in.
    const dateNow = jest.spyOn(Date, 'now');

    try {
      for (const absent of [undefined, null]) {
        const verdict = classifyLedgerEntryLiveness(
          dibsEntry(LIVE_PID, LIVE_STARTED_AT),
          liveSnapshot(),
          absent,
        );

        expect(verdict.snapshotAt).toBe(absent);
        // Still classifies rather than throwing: a missing snapshot instant
        // costs the future-instant bound, not the verdict.
        expect(verdict.status).toBe(LIVENESS_STATUS.ALIVE);
      }

      expect(dateNow).not.toHaveBeenCalled();
    } finally {
      dateNow.mockRestore();
    }
  });

  it('echoes back a zero snapshot timestamp unchanged, which a nullish default would also survive but a falsy one would not', () => {
    const verdict = classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, LIVE_STARTED_AT), liveSnapshot(), 0);

    expect(verdict.snapshotAt).toBe(0);
  });

  it('gives the same verdict for the same inputs whatever the snapshot timestamp is', () => {
    // The verdict turns on pid identity, not on how long ago the entry was
    // written — that is the TTL's job, and the TTL is not this function.
    const entry = dibsEntry(LIVE_PID, LIVE_STARTED_AT);

    const early = classifyLedgerEntryLiveness(entry, liveSnapshot(), SNAPSHOT_AT);
    const late = classifyLedgerEntryLiveness(entry, liveSnapshot(), SNAPSHOT_AT + 48 * 60 * 60 * 1000);

    expect(late.status).toBe(early.status);
    expect(late.reason).toBe(early.reason);
    expect(late.snapshotAt).toBe(SNAPSHOT_AT + 48 * 60 * 60 * 1000);
  });
});

// ---------------------------------------------------------------------------
// 8. One rule for every ledger. Two claim ledgers exist, each with its own
//    distinct TTL fallback; neither TTL is this function's business, and the
//    classification rule does not branch on which ledger a record came from.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — one rule across every ledger', () => {
  const identities = [
    ['a live identity', LIVE_PID, LIVE_STARTED_AT, LIVENESS_STATUS.ALIVE],
    ['a ghost identity', GHOST_PID, GHOST_STARTED_AT, LIVENESS_STATUS.CONFIRMED_DEAD],
    ['no identity', undefined, undefined, LIVENESS_STATUS.UNKNOWN],
  ];

  it.each(identities)(
    'gives live-agent and live-agent-admission records the same verdict for %s',
    (_label, pid, pidStartedAt, expected) => {
      // The two ledgers differ only in the reserved row that holds them; the
      // record shape `claimCapacity` and `reserveAdmission` append is identical.
      const claimLedgerRecord = claimRecord(pid, pidStartedAt);
      const admissionLedgerRecord = claimRecord(pid, pidStartedAt);

      const claimVerdict = classifyLedgerEntryLiveness(claimLedgerRecord, liveSnapshot(), SNAPSHOT_AT);
      const admissionVerdict = classifyLedgerEntryLiveness(admissionLedgerRecord, liveSnapshot(), SNAPSHOT_AT);

      expect(claimVerdict.status).toBe(expected);
      expect(admissionVerdict).toEqual(claimVerdict);
    },
  );

  it.each(identities)('gives a dibs entry the same verdict as a claim record for %s', (_label, pid, pidStartedAt) => {
    const dibsVerdict = classifyLedgerEntryLiveness(dibsEntry(pid, pidStartedAt), liveSnapshot(), SNAPSHOT_AT);
    const claimVerdict = classifyLedgerEntryLiveness(claimRecord(pid, pidStartedAt), liveSnapshot(), SNAPSHOT_AT);

    expect(dibsVerdict.status).toBe(claimVerdict.status);
    expect(dibsVerdict.reason).toBe(claimVerdict.reason);
  });
});

// ---------------------------------------------------------------------------
// 9. RESIDUAL LIMITATION ONE — DISCOVERY-WALK OMISSION.
//    A live agent on this host, in this PID namespace, that
//    `listAgentProcesses`' ancestry walk fails to enumerate. hardened
//    the walk; this classifier inherits whatever correctness the walk has.
//    Simulated by a snapshot that omits a process which is in fact running —
//    the only way to model it, since the classifier is built strictly on the
//    walk's output.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — residual limitation: discovery-walk omission', () => {
  it('leaves a pid-less entry the walk omits to the TTL, its ledger being the only thing that reaps it', () => {
    // The provable half of the guarantee. With no recorded identity there is
    // nothing to correlate, so a walk omission cannot change the outcome at
    // all: the entry ages out under its ledger's own TTL and this classifier
    // adds nothing to its fate in either direction.
    const verdict = classifyLedgerEntryLiveness(dibsEntry(), [], SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
    expect(verdict.reason).toBe(LIVENESS_REASON.NO_PID);
  });

  it('reads a pid-bearing entry the walk omits as confirmed-dead — the accepted limitation, asserted as-is', () => {
    // This is an accepted limitation, not a defect: an omitted-but-running
    // process is indistinguishable from a dead one to any consumer of the
    // walk's output, so no classifier built on it can do better. Closing it
    // means hardening the walk. Stated here so the guarantee is not
    // overstated elsewhere as "a live agent is never reaped, unconditionally".
    //
    // THE SNAPSHOT CARRIES OTHER AGENTS (pre-PR review, Low — it used to be
    // `[]`, which made this test byte-identical to the empty-snapshot case
    // seven hundred lines above and modelled "no agents are running" rather
    // than "one running agent's row was dropped"). A walk omission is
    // specifically a snapshot that saw the host, enumerated other trees, and
    // lost this one; asserting it against a populated snapshot is what makes
    // the case distinct from the one already covered.
    const OTHER_LIVE_PID = LIVE_PID + 1;
    const snapshotOmittingALiveAgent = [
      { agentId: String(OTHER_LIVE_PID), rssMb: 512, startedAt: LIVE_STARTED_AT },
      { agentId: String(OTHER_LIVE_PID + 1), rssMb: 256, startedAt: LIVE_STARTED_AT },
    ];
    // The snapshot is a real observation of a populated host, and the entry's
    // own pid is the one thing missing from it.
    expect(snapshotOmittingALiveAgent.map((row) => row.agentId)).not.toContain(String(LIVE_PID));

    const verdict = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, LIVE_STARTED_AT),
      snapshotOmittingALiveAgent,
      SNAPSHOT_AT,
    );

    expect(verdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_ABSENT);
  });
});

// ---------------------------------------------------------------------------
// 10. RESIDUAL LIMITATION TWO — PID-NAMESPACE BOUNDARY.
//     Structurally distinct from limitation one above, and deliberately in its
//     own block so the two never collapse into each other. There the walk is
//     imperfect on a host it can see; here the process is behind a boundary
//     the sweep's own `ps` cannot see across at all, so NO walk fix could ever
//     surface it. The verdict is a CONFIDENT wrong answer rather than an
//     ambiguous one, which is the part worth naming.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — residual limitation: PID-namespace boundary', () => {
  it('reads a pid from outside the sweep PID namespace as confirmed-dead, matching the documented assumption', () => {
    // Pid 17 is namespace-local to a container: alive there, structurally
    // unresolvable from the host snapshot the sweep runs against. The
    // assertion proves the documented behaviour, not that the limitation is
    // fixed — fixing it is out of scope for this ticket.
    const NAMESPACE_LOCAL_PID = 17;
    const NAMESPACE_LOCAL_STARTED_AT = Date.parse('2026-08-28T11:00:00Z');
    const hostSnapshot = liveSnapshot();

    const verdict = classifyLedgerEntryLiveness(
      dibsEntry(NAMESPACE_LOCAL_PID, NAMESPACE_LOCAL_STARTED_AT),
      hostSnapshot,
      SNAPSHOT_AT,
    );

    expect(verdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_ABSENT);
  });

  it('documents the PID-namespace-boundary assumption in the module doc comment', () => {
    // The assumption is load-bearing: everything this classifier concludes
    // from an absent pid rests on it. Matched against comment prose with line
    // wrapping and leading `*` markers flattened, so reflowing a doc comment
    // does not break the assertion.
    const prose = flattenComments(readFileSync(RECONCILIATION_SOURCE_PATH, 'utf8'));

    expect(prose).toMatch(
      /only trustworthy when the process it names is visible to the same PID namespace the sweep itself runs in/i,
    );
  });

  it('documents the walk-omission limitation as its own headed block, not merged into the namespace-boundary one', () => {
    // NOT "two regexes match at different offsets" — a single merged paragraph
    // naming and the namespace assumption together satisfies that while
    // being exactly the collapse this assertion exists to forbid. Two SEPARATE
    // HEADED BLOCKS must exist, in order, each carrying its own subject matter.
    const { walkOmission, namespaceBoundary } = residualLimitationBlocks();

    expect(walkOmission).not.toBe('');
    expect(namespaceBoundary).not.toBe('');

    // Block one is about a walk that fails on a host it can see.
    expect(walkOmission).toMatch(/ /);
    expect(walkOmission).toMatch(/fails to enumerate/i);
    // Block two is about a boundary no walk fix can cross, and carries the
    // load-bearing namespace assumption.
    expect(namespaceBoundary).toMatch(/PID namespace/i);
    expect(namespaceBoundary).toMatch(/only trustworthy when the process it names is visible/i);
    // The assumption belongs to block two alone; finding it in block one would
    // mean the two subjects have bled together.
    expect(walkOmission).not.toMatch(/only trustworthy when the process it names is visible/i);
  });

  it('disambiguates the walk-omission block from the namespace boundary inside the walk-omission block itself', () => {
    // A reader who lands on limitation one must be told, there and then, that
    // it is not limitation two — otherwise the namespace assumption documented
    // further down reads as though it already covers the walk omission, and one
    // of the two limitations looks handled when it is not.
    const { walkOmission } = residualLimitationBlocks();

    expect(walkOmission).toMatch(/not the pid-namespace boundary/i);
  });
});

// ---------------------------------------------------------------------------
// 10b. RESIDUAL LIMITATION THREE — COINCIDENTAL START-TIME COLLISION.
//      The third of the three, and the only one that errs in the OPPOSITE
//      direction from the other two: limitations one and two reap a live
//      process, this one spares a dead record. Named in the module as an
//      accepted residual risk at a security co-reviewer's request, and
//      pinned here so deleting the block fails rather than passes.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — residual limitation: coincidental start-time collision', () => {
  it('reads a reused pid whose successor started within the tolerance as alive — the accepted collision', () => {
    // The behaviour the limitation describes, asserted as-is: the snapshot row
    // is a DIFFERENT process that happened to inherit the pid and start just
    // inside the window. Nothing in one snapshot can tell it from the original,
    // so it reads as alive and the dead record is spared.
    const successor = [
      {
        agentId: String(LIVE_PID),
        rssMb: 64,
        startedAt: LIVE_STARTED_AT + PID_IDENTITY_TOLERANCE_MS,
      },
    ];

    const verdict = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, LIVE_STARTED_AT),
      successor,
      SNAPSHOT_AT,
    );

    expect(verdict.status).toBe(LIVENESS_STATUS.ALIVE);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_MATCHED);
  });

  it('documents the coincidental collision as its own headed residual-limitation block', () => {
    // Limitations one and two each have a prose test; without this one the
    // whole block can be deleted with every test still green, and an accepted
    // risk that nobody can find is not an accepted risk.
    const { coincidental } = residualLimitationBlocks();

    expect(coincidental).not.toBe('');
    expect(coincidental).toMatch(/COINCIDENTAL START-TIME COLLISION/);
    expect(coincidental).toMatch(/reads as .?alive/i);
    expect(coincidental).toMatch(/accepted residual risk/i);
  });

  it('states the collision window relative to the constant rather than restating its value', () => {
    // The one place in the module that could restate `PID_IDENTITY_TOLERANCE_MS`
    // as a literal. An owner tightening the constant must not have to remember
    // to correct a sentence, so the width is phrased as a multiple of the name.
    const { coincidental } = residualLimitationBlocks();

    expect(coincidental).toMatch(/twice .?PID_IDENTITY_TOLERANCE_MS/i);
    expect(coincidental).not.toMatch(new RegExp(`\\b${PID_IDENTITY_TOLERANCE_MS}\\b`));
    expect(coincidental).not.toMatch(/four seconds/i);
  });
});

// ---------------------------------------------------------------------------
// 11. Wall-clock step between the beat that wrote an identity and the beat
//     that classifies it. Both timestamps come from the same
//     `now - parseEtimeToSeconds(etime) * 1000` formula, so within ONE beat
//     there is no drift by construction; across beats an NTP correction or a
//     manual clock change can move them apart.
//
//     THE DELIBERATE CHOICE, asserted here rather than left to fall out of the
//     tolerance constant: a step within the tolerance is absorbed and the
//     entry stays alive; a step beyond it is indistinguishable from pid reuse
//     and takes the same confirmed-dead verdict. The classifier cannot tell
//     the two apart from a single snapshot, and inventing a third signal to
//     try would trade a rare wrong reap for a permanent inability to detect
//     reuse. An owner who wants the other trade widens
//     PID_IDENTITY_TOLERANCE_MS, which is exported for exactly that reason.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — cross-beat wall-clock step', () => {
  it('absorbs a clock step smaller than the tolerance, keeping a live entry alive', () => {
    const stepMs = Math.floor(PID_IDENTITY_TOLERANCE_MS / 2);
    const entry = dibsEntry(LIVE_PID, LIVE_STARTED_AT + stepMs);

    expect(classifyLedgerEntryLiveness(entry, liveSnapshot(), SNAPSHOT_AT).status).toBe(LIVENESS_STATUS.ALIVE);
  });

  it('classifies a clock step beyond the tolerance as confirmed-dead, indistinguishable from pid reuse', () => {
    // A 30s NTP correction between the writing beat and this one. The entry
    // names a process that IS still running, and this classifies it dead —
    // the accepted cost of using start time as the identity discriminator.
    const NTP_STEP_MS = 30 * 1000;
    const entry = dibsEntry(LIVE_PID, LIVE_STARTED_AT + NTP_STEP_MS);

    const verdict = classifyLedgerEntryLiveness(entry, liveSnapshot(), SNAPSHOT_AT);

    expect(NTP_STEP_MS).toBeGreaterThan(PID_IDENTITY_TOLERANCE_MS);
    expect(verdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_REUSE);
  });

  it('is symmetric about the direction of the step', () => {
    const backwards = dibsEntry(LIVE_PID, LIVE_STARTED_AT - 30 * 1000);
    const forwards = dibsEntry(LIVE_PID, LIVE_STARTED_AT + 30 * 1000);

    expect(classifyLedgerEntryLiveness(backwards, liveSnapshot(), SNAPSHOT_AT).status).toBe(
      classifyLedgerEntryLiveness(forwards, liveSnapshot(), SNAPSHOT_AT).status,
    );
  });

  it('keeps a live agent alive across two beats whose ps collections sit at opposite ends of the permitted latency range', () => {
    // NO CLOCK EVENT ANYWHERE IN THIS TEST — the drift is entirely ordinary,
    // and that is the point. Both beats' identities are derived by the REAL
    // `listAgentProcesses`, from real `ps`-shaped tables, exactly as
    // `observeBeatProcesses` derives them; only the two latencies differ.
    const TRUE_START = Date.parse('2026-08-28T12:00:00Z');
    const PID = 4242;
    const psTable = (etime) =>
      [
        '  PID  PPID   RSS     ELAPSED COMM',
        '    1     0  1000    10:00:00 /sbin/launchd',
        `  ${PID}     1 20000    ${etime} /tmp/arm/claude`,
      ].join('\n');

    // WRITER BEAT — a quiet host. `ps` issued 1.0 s after the true start and
    // returned in 20 ms; `cli.mjs` reads `snapshotAt` after that return.
    const writerSnapshotAt = TRUE_START + 1000 + 20;
    const [writerRow] = listAgentProcesses(psTable('0:01'), writerSnapshotAt);
    const record = dibsEntry(PID, writerRow.startedAt);

    // SWEEPER BEAT, five minutes later — a THRASHING host, the one ARM exists
    // for. `ps` issued at +300.9 s (so `etime` floors 900 ms away) and took
    // 1900 ms, comfortably INSIDE its own `DEFAULT_PROBE_TIMEOUT_MS`, so it
    // succeeded and produced a snapshot the sweep is entitled to trust.
    const psDurationMs = DEFAULT_PROBE_TIMEOUT_MS - 100;
    const sweeperSnapshotAt = TRUE_START + 300_900 + psDurationMs;
    const liveProcesses = listAgentProcesses(psTable('5:00'), sweeperSnapshotAt);

    // The two beats disagree by more than the old flat 2000 ms tolerance…
    const skewMs = Math.abs(liveProcesses[0].startedAt - record.pidStartedAt);
    expect(skewMs).toBeGreaterThan(2000);
    // …but strictly less than the bound the derived tolerance is sized to,
    // which is why the derivation — not a bigger guessed literal — is what
    // makes this safe.
    expect(skewMs).toBeLessThan(PID_IDENTITY_TOLERANCE_MS);

    // THE HARD AC: the process named by this record is genuinely running.
    const verdict = classifyLedgerEntryLiveness(record, liveProcesses, sweeperSnapshotAt);
    expect(verdict.status).toBe(LIVENESS_STATUS.ALIVE);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_MATCHED);
  });
});

// ---------------------------------------------------------------------------
// 12. IMPOSSIBLE INSTANTS, ON BOTH SIDES OF THE COMPARISON.
//
//     The same class the pid guard closes, on the timestamp inputs. A pid that
//     could never exist misses the snapshot LOOKUP by construction; an instant
//     that could never be a start time misses the TOLERANCE WINDOW by
//     construction. Both misses read as evidence of death, and both are really
//     evidence of nothing but corruption.
//
//     The comparison has two operands and they must be held to the same bar:
//     whichever side is laxer is the side a corrupt value enters through. The
//     entry side is `pidStartedAt`, written by a past beat into a file anyone
//     can edit; the snapshot side is a row's `startedAt`, computed by
//     `listAgentProcesses` as `now - etimeSeconds * 1000` from a caller-supplied
//     `now`. Each block below is mirrored by the other.
//
//     THE BOUND IS ONE-SIDED ON PURPOSE. Only a FUTURE instant is provably
//     impossible — a process cannot start after the `ps` run that observed it.
//     A far-PAST instant is a legitimate disagreement about identity and keeps
//     its confirmed-dead verdict; that is the documented pid-reuse rule, not a
//     gap, and the last test here pins it so a future tightening cannot quietly
//     take it away.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — impossible instants on either side', () => {
  /** Instants no process on this host could have started at, given SNAPSHOT_AT. */
  const impossibleInstants = [
    ['one millisecond after the snapshot, past the tolerance', SNAPSHOT_AT + PID_IDENTITY_TOLERANCE_MS + 1],
    ['one day after the snapshot', SNAPSHOT_AT + 24 * 60 * 60 * 1000],
    ['1e21', 1e21],
    ['Number.MAX_SAFE_INTEGER', Number.MAX_SAFE_INTEGER],
    ['the maximum representable Date', 8.64e15],
  ];

  describe('on the entry side — pidStartedAt', () => {
    it.each(impossibleInstants)(
      'never reaps a live agent for a pidStartedAt %s',
      (_label, pidStartedAt) => {
        // The live process IS in the snapshot at this very pid. Before the
        // bound, the impossible instant missed the tolerance window and the
        // entry was handed a `pid-reuse` death certificate for a running agent.
        const verdict = classifyLedgerEntryLiveness(
          dibsEntry(LIVE_PID, pidStartedAt),
          liveSnapshot(),
          SNAPSHOT_AT,
        );

        expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
        expect(verdict.reason).toBe(LIVENESS_REASON.NO_PID);
      },
    );

    it('admits a pidStartedAt exactly at the snapshot instant plus the tolerance', () => {
      // The boundary sits at `snapshotAt + PID_IDENTITY_TOLERANCE_MS`, not at
      // `snapshotAt`: the tolerance already exists to absorb etime's ~1s
      // granularity on both sides, so a value inside it is not yet impossible.
      const row = [{ agentId: String(LIVE_PID), rssMb: 512, startedAt: SNAPSHOT_AT }];
      const entry = dibsEntry(LIVE_PID, SNAPSHOT_AT + PID_IDENTITY_TOLERANCE_MS);

      const verdict = classifyLedgerEntryLiveness(entry, row, SNAPSHOT_AT);

      expect(verdict.status).toBe(LIVENESS_STATUS.ALIVE);
      expect(verdict.reason).toBe(LIVENESS_REASON.PID_MATCHED);
    });
  });

  describe('on the snapshot side — a matched row startedAt', () => {
    const impossibleRowInstants = [
      ...impossibleInstants,
      ['zero, as a wrong `now` of 0 produces', 0],
      ['negative, as a seconds-valued `now` produces', -5],
      ['far negative', -1e12],
      ['fractional', Number.MIN_VALUE],
      ['NaN, as a non-finite `now` produces', Number.NaN],
    ];

    it.each(impossibleRowInstants)(
      'never reaps a live agent for a matched row whose startedAt is %s',
      (_label, startedAt) => {
        // Mirrors the entry-side block. The row carries the record's own pid,
        // so the lookup SUCCEEDS; only the row's instant is unusable, and an
        // unusable instant is not grounds to reap. `malformed-process-row`
        // rather than `no-pid`: the record's identity is fine, the row is not.
        const row = [{ agentId: String(LIVE_PID), rssMb: 512, startedAt }];

        const verdict = classifyLedgerEntryLiveness(
          dibsEntry(LIVE_PID, LIVE_STARTED_AT),
          row,
          SNAPSHOT_AT,
        );

        expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
        expect(verdict.reason).toBe(LIVENESS_REASON.MALFORMED_PROCESS_ROW);
      },
    );

    it('admits a matched row startedAt exactly at the snapshot instant plus the tolerance', () => {
      const row = [
        { agentId: String(LIVE_PID), rssMb: 512, startedAt: SNAPSHOT_AT + PID_IDENTITY_TOLERANCE_MS },
      ];

      const verdict = classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, SNAPSHOT_AT), row, SNAPSHOT_AT);

      expect(verdict.status).toBe(LIVENESS_STATUS.ALIVE);
    });
  });

  it('holds both sides of the comparison to the same bar', () => {
    // The asymmetry that let this class survive one fix round: the entry side
    // was checked with an epoch-ms shape test while the snapshot side was
    // checked only for finiteness. Asserted as a property over the same input
    // set applied to each side in turn, so tightening one side alone fails.
    for (const [, instant] of impossibleInstants) {
      const entrySide = classifyLedgerEntryLiveness(
        dibsEntry(LIVE_PID, instant),
        liveSnapshot(),
        SNAPSHOT_AT,
      );
      const snapshotSide = classifyLedgerEntryLiveness(
        dibsEntry(LIVE_PID, LIVE_STARTED_AT),
        [{ agentId: String(LIVE_PID), rssMb: 512, startedAt: instant }],
        SNAPSHOT_AT,
      );

      expect(entrySide.status).toBe(LIVENESS_STATUS.UNKNOWN);
      expect(snapshotSide.status).toBe(LIVENESS_STATUS.UNKNOWN);
    }
  });

  it('skips the future-instant bound when the snapshot instant is itself unusable, changing nothing for a bad clock', () => {
    // The bound needs a trustworthy reference point. A caller threading a
    // broken `snapshotAt` has none, so the bound is skipped rather than
    // enforced against nonsense — enforcing it would turn one bad argument into
    // a blanket `unknown` for every record in every ledger, which is a
    // behaviour change dressed as a safety fix. Classification still happens on
    // the evidence that IS trustworthy: the pid lookup.
    for (const badClock of [Number.NaN, undefined, null, 0, -1, 1.5, Number.POSITIVE_INFINITY]) {
      const alive = classifyLedgerEntryLiveness(
        dibsEntry(LIVE_PID, LIVE_STARTED_AT),
        liveSnapshot(),
        badClock,
      );
      const ghost = classifyLedgerEntryLiveness(
        dibsEntry(GHOST_PID, GHOST_STARTED_AT),
        liveSnapshot(),
        badClock,
      );

      expect(alive.status).toBe(LIVENESS_STATUS.ALIVE);
      expect(ghost.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
    }
  });

  it('still excludes an absolutely-impossible instant on either side when the snapshot instant is unusable', () => {
    // The cross-product the two tests above each miss on their own. The skip
    // test uses only POSSIBLE instants, so it pins the design decision without
    // exercising its consequence; the impossible-instant tests all use a good
    // clock. Crossed, they describe the case that matters: a bad `snapshotAt`
    // disables the RELATIVE bound, and without an absolute ceiling an instant
    // like `1e21` becomes decisive again and hands a `pid-reuse` death
    // certificate to a process sitting alive in the very snapshot supplied.
    //
    // The instants below are impossible against no reference point at all: each
    // exceeds the maximum time value a `Date` can represent, so no clock —
    // working or broken — could have produced one.
    const badClocks = [Number.NaN, undefined, null, 0, -1, 1.5, Number.POSITIVE_INFINITY, '1756382400000'];
    const absolutelyImpossible = [1e21, Number.MAX_SAFE_INTEGER, 8.64e15 + 1, 1e16];

    for (const badClock of badClocks) {
      for (const instant of absolutelyImpossible) {
        const entrySide = classifyLedgerEntryLiveness(
          dibsEntry(LIVE_PID, instant),
          liveSnapshot(),
          badClock,
        );
        const snapshotSide = classifyLedgerEntryLiveness(
          dibsEntry(LIVE_PID, LIVE_STARTED_AT),
          [{ agentId: String(LIVE_PID), rssMb: 512, startedAt: instant }],
          badClock,
        );

        expect(entrySide.status).toBe(LIVENESS_STATUS.UNKNOWN);
        expect(entrySide.reason).toBe(LIVENESS_REASON.NO_PID);
        expect(snapshotSide.status).toBe(LIVENESS_STATUS.UNKNOWN);
        expect(snapshotSide.reason).toBe(LIVENESS_REASON.MALFORMED_PROCESS_ROW);
      }
    }
  });

  it('still reaps a far-PAST pidStartedAt, which is a real disagreement rather than an impossibility', () => {
    // The scope line of the new bound, pinned from the other side. Only the
    // future is provably impossible; a past instant is exactly what pid reuse
    // looks like, and widening the guard to cover it would delete the
    // module's ability to detect reuse at all.
    for (const pidStartedAt of [1, 1000, LIVE_STARTED_AT - 365 * 24 * 60 * 60 * 1000]) {
      const verdict = classifyLedgerEntryLiveness(
        dibsEntry(LIVE_PID, pidStartedAt),
        liveSnapshot(),
        SNAPSHOT_AT,
      );

      expect(verdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
      expect(verdict.reason).toBe(LIVENESS_REASON.PID_REUSE);
    }
  });

  it('reads the identity pair as own properties, so a polluted prototype lends no identity', () => {
    // Defence in depth on the one boundary where being wrong kills a live
    // agent. No pollution primitive reaches this module today — `JSON.parse`
    // materialises `__proto__` as an OWN data property, so a hand-edited ledger
    // cannot pollute through the read path — but an inherited identity is the
    // worst possible kind: a record that carries NO identity of its own would
    // borrow one, miss the snapshot lookup, and be reaped as `pid-absent`.
    const pid = { value: GHOST_PID, configurable: true, writable: true };
    const pidStartedAt = { value: GHOST_STARTED_AT, configurable: true, writable: true };
    Object.defineProperty(Object.prototype, 'pid', pid);
    Object.defineProperty(Object.prototype, 'pidStartedAt', pidStartedAt);

    try {
      const noIdentity = dibsEntry();

      // Guard: the pollution really is visible through normal property access,
      // so this test would fail without the own-property reads.
      expect(noIdentity.pid).toBe(GHOST_PID);

      const verdict = classifyLedgerEntryLiveness(noIdentity, liveSnapshot(), SNAPSHOT_AT);

      expect(verdict.status).toBe(LIVENESS_STATUS.UNKNOWN);
      expect(verdict.reason).toBe(LIVENESS_REASON.NO_PID);
    } finally {
      delete Object.prototype.pid;
      delete Object.prototype.pidStartedAt;
    }
  });

  it('still reads an own identity that shadows a polluted prototype', () => {
    // The other half: the guard must reject INHERITED identity without
    // rejecting the ordinary own-property case that every real record uses.
    Object.defineProperty(Object.prototype, 'pid', {
      value: GHOST_PID,
      configurable: true,
      writable: true,
    });

    try {
      const verdict = classifyLedgerEntryLiveness(
        dibsEntry(LIVE_PID, LIVE_STARTED_AT),
        liveSnapshot(),
        SNAPSHOT_AT,
      );

      expect(verdict.status).toBe(LIVENESS_STATUS.ALIVE);
    } finally {
      delete Object.prototype.pid;
    }
  });
});

// ---------------------------------------------------------------------------
// 13. THE HOSTILE-INPUT CROSS PRODUCT — the operand-wise structural guard.
//
//     WHY THIS BLOCK EXISTS. The classifier's core is a symmetric comparison
//     whose operands arrive by four different paths: a record's `pid` and
//     `pidStartedAt`, read off a file anyone can edit, and a snapshot row's
//     `agentId` and `startedAt`, computed by `listAgentProcesses`. Every guard
//     in the module applies to all four or it applies to none, because
//     whichever operand is read laxer is the one a corrupt or inherited value
//     enters through. Hand-written tests enumerate one operand at a time, so a
//     guard added to one path leaves its mirror both unguarded and unpinned —
//     and line coverage cannot see the gap, since both paths are covered
//     either way.
//
//     WHAT IT DOES INSTEAD. One corpus of hostile values, one list of operand
//     positions, one list of clocks, crossed by generation rather than by
//     hand. The invariant is the fail-conservative one: no hostile value in
//     any operand position may produce `confirmed-dead`. Adding a guard to one
//     operand without its mirror, or deleting one, changes an outcome in this
//     table and fails here.
//
//     EXCEPTIONS ARE DATA, NOT OMISSIONS. A handful of position/value pairs
//     legitimately DO reap — a far-past instant is the pid-reuse signal
//     itself, and a value inside the `pid_t` range is a possible pid whose
//     absence from the snapshot is real evidence. Each is listed below with
//     its reason and its exact expected verdict, so it stays a decision on the
//     record rather than a hole in the corpus. Deleting a value from the
//     corpus to make the table pass would be the defect this block exists to
//     catch.
//
//     ADDITIVE. The hand-written blocks above pin specific reason codes,
//     precedence and prose; this one pins the shape of the guard set.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — hostile inputs across every operand position', () => {
  /** How a hostile value is placed into an operand position. */
  const OWN_PROPERTY = 'own';
  const NO_OWN_PROPERTY = 'absent';
  const INHERITED_MATCHING = 'inherited-matching';
  const INHERITED_FOREIGN = 'inherited-foreign';

  /**
   * Every value probed against this module, in one place. The union of the
   * hand-written corpora above plus the placements that only a prototype can
   * produce. A position/value pair that reaps is recorded in EXPECTED_REAPS
   * below; nothing is dropped from here to make the table pass.
   */
  const HOSTILE_VALUES = [
    ['zero', OWN_PROPERTY, 0],
    ['minus one', OWN_PROPERTY, -1],
    ['negative zero', OWN_PROPERTY, -0],
    ['a fractional number', OWN_PROPERTY, 4242.5],
    ['NaN', OWN_PROPERTY, Number.NaN],
    ['positive infinity', OWN_PROPERTY, Number.POSITIVE_INFINITY],
    ['negative infinity', OWN_PROPERTY, Number.NEGATIVE_INFINITY],
    ['1e21', OWN_PROPERTY, 1e21],
    ['Number.MAX_SAFE_INTEGER', OWN_PROPERTY, Number.MAX_SAFE_INTEGER],
    ['the maximum representable Date', OWN_PROPERTY, 8.64e15],
    ['one past the maximum representable Date', OWN_PROPERTY, 8.64e15 + 1],
    ['one past the pid_t ceiling', OWN_PROPERTY, 2 ** 31],
    ['the pid_t ceiling itself', OWN_PROPERTY, 2 ** 31 - 1],
    // In the three positions that do not already hold it, this is a hostile
    // substitution — a string where the module expects a number. In `snapshot
    // row agentId` it equals `decoyRow()`'s own baseline, so that one cell
    // asserts the decoy's unsubstituted behaviour rather than a substitution;
    // its EXPECTED_REAPS entry is the decoy doing what a decoy does.
    ['a decimal string form of the live pid', OWN_PROPERTY, String(LIVE_PID)],
    ['a space-padded string form of the live pid', OWN_PROPERTY, ` ${LIVE_PID}`],
    ['a hexadecimal string form of the live pid', OWN_PROPERTY, '0x2329'],
    ['a decimal string form of the live start time', OWN_PROPERTY, String(LIVE_STARTED_AT)],
    ['a non-numeric string', OWN_PROPERTY, 'this morning'],
    ['true', OWN_PROPERTY, true],
    ['a one-element array holding the live pid', OWN_PROPERTY, [LIVE_PID]],
    ['null', OWN_PROPERTY, null],
    ['undefined', OWN_PROPERTY, undefined],
    ['an object whose valueOf returns the live pid', OWN_PROPERTY, { valueOf: () => LIVE_PID }],
    ['absent, with an unpolluted prototype', NO_OWN_PROPERTY, undefined],
    ['absent, with a matching value on the prototype', INHERITED_MATCHING, undefined],
    ['absent, with a foreign far-past value on the prototype', INHERITED_FOREIGN, undefined],
  ];

  /**
   * The four operands of the identity comparison. `matching` is the value that
   * would make this position agree with the live process; `foreign` is one
   * belonging to an unrelated, long-dead process. Both are used only as
   * prototype-pollution payloads.
   *
   * This list is hand-maintained and the table cannot grow itself: the module
   * reads a fifth record field, `record.orchestratorId`, which no position
   * covers. Pollution there is safe-direction only — an inherited
   * `orchestratorId` can reach `unknown`/`reserved-entry` and never a reap — so
   * its absence is a decision, not a hole. A NEW operand position added to the
   * module must be hand-added here; nothing detects one that is missing.
   */
  const OPERAND_POSITIONS = [
    ['entry pid', 'entry', 'pid', LIVE_PID, GHOST_PID],
    ['entry pidStartedAt', 'entry', 'pidStartedAt', LIVE_STARTED_AT, GHOST_STARTED_AT],
    ['snapshot row agentId', 'row', 'agentId', String(LIVE_PID), String(GHOST_PID)],
    ['snapshot row startedAt', 'row', 'startedAt', LIVE_STARTED_AT, GHOST_STARTED_AT],
  ];

  /**
   * Both clocks, because `postdatesSnapshot` deliberately stops running when
   * `snapshotAt` is unusable. Half the guard set is only reachable under the
   * good clock and half the residual only visible under the bad one, so a
   * corpus crossed against one clock alone would miss whichever half it omits.
   */
  const CLOCKS = [
    ['a usable snapshot instant', SNAPSHOT_AT],
    ['an unusable snapshot instant', Number.NaN],
  ];

  /** A record naming the live process, complete and well-shaped. */
  function healthyEntry() {
    return {
      orchestratorId: 'orchestrator-a',
      declaredAt: Date.parse('2026-08-28T11:59:00Z'),
      desiredAgents: 2,
      pid: LIVE_PID,
      pidStartedAt: LIVE_STARTED_AT,
    };
  }

  /**
   * The row the hostile value is placed into, sitting FIRST in the snapshot so
   * it wins `find` whenever it answers the lookup. It carries the live pid and
   * a long-dead start time: that pairing is what makes a corrupted read
   * observable, because a row that wrongly answers the lookup then hands the
   * record a `pid-reuse` death certificate rather than a harmless `alive`.
   */
  function decoyRow() {
    return { agentId: String(LIVE_PID), rssMb: 64, startedAt: GHOST_STARTED_AT };
  }

  /**
   * Builds one case and classifies it, restoring any prototype pollution
   * afterwards. Everything except the operand under test is well-shaped and
   * agrees, so the unpolluted baseline of every row-side case is `alive`.
   */
  function classifyHostileCase(position, hostile, snapshotAt) {
    const [, side, key, matching, foreign] = position;
    const [, placement, value] = hostile;

    const target = side === 'entry' ? healthyEntry() : decoyRow();
    if (placement === OWN_PROPERTY) target[key] = value;
    else delete target[key];

    const entry = side === 'entry' ? target : healthyEntry();
    const snapshot = side === 'entry' ? liveSnapshot() : [target, ...liveSnapshot()];

    const polluting = placement === INHERITED_MATCHING || placement === INHERITED_FOREIGN;
    if (polluting) {
      Object.defineProperty(Object.prototype, key, {
        value: placement === INHERITED_MATCHING ? matching : foreign,
        configurable: true,
        writable: true,
      });
    }

    try {
      return classifyLedgerEntryLiveness(entry, snapshot, snapshotAt);
    } finally {
      if (polluting) delete Object.prototype[key];
    }
  }

  /**
   * The position/value pairs that DO reap, each with the reason it is correct
   * to. Keyed `clock :: position :: value`. Every one of these is a decided
   * case rather than an ambiguous one, which is the line the fail-conservative
   * posture is drawn on: a corrupt operand never reaps, a possible-but-
   * disagreeing one does.
   */
  const EXPECTED_REAPS = new Map(
    [
      // A value inside the `pid_t` range is a POSSIBLE pid. The ceiling excludes
      // the impossible, not the unlikely, so its absence from the snapshot is
      // real evidence of death rather than evidence of corruption.
      ['entry pid :: the pid_t ceiling itself', LIVENESS_REASON.PID_ABSENT],
      // Read as instants, both sit in January 1970 — far-PAST, which is the
      // pid-reuse signal itself. Widening the guard to cover the past would
      // delete this module's ability to detect reuse at all.
      ['entry pidStartedAt :: one past the pid_t ceiling', LIVENESS_REASON.PID_REUSE],
      ['entry pidStartedAt :: the pid_t ceiling itself', LIVENESS_REASON.PID_REUSE],
      ['snapshot row startedAt :: one past the pid_t ceiling', LIVENESS_REASON.PID_REUSE],
      ['snapshot row startedAt :: the pid_t ceiling itself', LIVENESS_REASON.PID_REUSE],
      // `String()` on both operands is the documented symmetric coercion, and
      // both of these coerce to the pid key exactly. The decoy row therefore
      // genuinely answers the lookup, and its long-dead start time is a real
      // disagreement about identity.
      ['snapshot row agentId :: a decimal string form of the live pid', LIVENESS_REASON.PID_REUSE],
      ['snapshot row agentId :: a one-element array holding the live pid', LIVENESS_REASON.PID_REUSE],
    ].flatMap(([suffix, reason]) => CLOCKS.map(([clockLabel]) => [`${clockLabel} :: ${suffix}`, reason])),
  );

  // Under a bad clock the RELATIVE future-instant bound stops running, so an
  // instant it would otherwise have excluded reaches rule 5 and its
  // disagreement becomes decisive. The absolute ceiling still holds — anything
  // ABOVE it is rejected on either clock — so this is exactly the residual the
  // module's `postdatesSnapshot` doc names, pinned rather than left implicit.
  for (const position of ['entry pidStartedAt', 'snapshot row startedAt']) {
    EXPECTED_REAPS.set(
      `an unusable snapshot instant :: ${position} :: the maximum representable Date`,
      LIVENESS_REASON.PID_REUSE,
    );
  }

  const hostileCases = CLOCKS.flatMap(([clockLabel, snapshotAt]) =>
    OPERAND_POSITIONS.flatMap((position) =>
      HOSTILE_VALUES.map((hostile) => [
        `${clockLabel} :: ${position[0]} :: ${hostile[0]}`,
        position,
        hostile,
        snapshotAt,
      ]),
    ),
  );

  it('crosses every hostile value against every operand position on both clocks', () => {
    // Guards the generator itself: a refactor that silently dropped a value, a
    // position or a clock would shrink the table and quietly stop testing the
    // mirror this block exists to hold. Every side of the arity is a LITERAL,
    // never a `.length` read back off the array being measured — a
    // self-referential count shrinks in step with the corpus and lets a value
    // be deleted with the suite still green, which is the precise laundering
    // this block exists to catch. Deleting a value, a position or a clock now
    // fails here, whether or not it appears in EXPECTED_REAPS.
    expect(hostileCases).toHaveLength(26 * 4 * 2);
    expect(HOSTILE_VALUES).toHaveLength(26);
    expect(HOSTILE_VALUES.map(([label]) => label)).toEqual([
      'zero',
      'minus one',
      'negative zero',
      'a fractional number',
      'NaN',
      'positive infinity',
      'negative infinity',
      '1e21',
      'Number.MAX_SAFE_INTEGER',
      'the maximum representable Date',
      'one past the maximum representable Date',
      'one past the pid_t ceiling',
      'the pid_t ceiling itself',
      'a decimal string form of the live pid',
      'a space-padded string form of the live pid',
      'a hexadecimal string form of the live pid',
      'a decimal string form of the live start time',
      'a non-numeric string',
      'true',
      'a one-element array holding the live pid',
      'null',
      'undefined',
      'an object whose valueOf returns the live pid',
      'absent, with an unpolluted prototype',
      'absent, with a matching value on the prototype',
      'absent, with a foreign far-past value on the prototype',
    ]);
    expect(OPERAND_POSITIONS).toHaveLength(4);
    expect(OPERAND_POSITIONS.map((position) => position[0])).toEqual([
      'entry pid',
      'entry pidStartedAt',
      'snapshot row agentId',
      'snapshot row startedAt',
    ]);
    expect(CLOCKS).toHaveLength(2);
    expect(CLOCKS.map(([label]) => label)).toEqual([
      'a usable snapshot instant',
      'an unusable snapshot instant',
    ]);
  });

  it.each(hostileCases)('%s', (title, position, hostile, snapshotAt) => {
    const verdict = classifyHostileCase(position, hostile, snapshotAt);
    const expectedReapReason = EXPECTED_REAPS.get(title);

    if (expectedReapReason === undefined) {
      expect(verdict.status).not.toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
      return;
    }

    // The named exceptions are pinned to their exact verdict, not merely
    // allowed to reap: each is a decided case, and a change of reason means the
    // rule that decided it moved.
    expect(verdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
    expect(verdict.reason).toBe(expectedReapReason);
  });

  it('reaps nothing at all when no operand is hostile', () => {
    // The baseline the table is measured against. Without it, a change that
    // made every case `unknown` would satisfy the invariant above while having
    // destroyed the classifier.
    const verdict = classifyLedgerEntryLiveness(healthyEntry(), liveSnapshot(), SNAPSHOT_AT);

    expect(verdict.status).toBe(LIVENESS_STATUS.ALIVE);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_MATCHED);
  });

  it('lists every reaping pair as named data, so none can be hidden by dropping a corpus value', () => {
    // An exception that no longer fires is an exception whose corpus value has
    // gone missing — the exact way this table could be quietly weakened.
    const firing = new Set(
      hostileCases
        .filter(([, position, hostile, snapshotAt]) =>
          classifyHostileCase(position, hostile, snapshotAt).status === LIVENESS_STATUS.CONFIRMED_DEAD)
        .map(([title]) => title),
    );

    expect([...firing].sort()).toEqual([...EXPECTED_REAPS.keys()].sort());
  });
});

// ---------------------------------------------------------------------------
// 14. The remaining single-operand rules, each pinned by the one input that
//     tells it apart from its plausible weakening. Kept out of the cross
//     product above because each concerns a rule with only ONE operand, so
//     there is no mirror for the table to hold.
// ---------------------------------------------------------------------------

describe('classifyLedgerEntryLiveness — boundaries with no mirror', () => {
  it('admits an instant exactly at the maximum representable Date on both sides', () => {
    // The epoch ceiling is INCLUSIVE, exactly as the pid ceiling is, and only
    // this instant tells `<=` from `<`. It needs an unusable snapshot instant
    // to be observable at all: under a good clock the relative bound rejects it
    // first, so both forms of the ceiling would agree. Asserted as ALIVE — with
    // both operands at the ceiling there is no disagreement to decide, and a
    // narrowed ceiling would answer `no-pid` instead.
    const bothAtTheCeiling = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, 8.64e15),
      [{ agentId: String(LIVE_PID), rssMb: 512, startedAt: 8.64e15 }],
      Number.NaN,
    );

    expect(bothAtTheCeiling.status).toBe(LIVENESS_STATUS.ALIVE);
    expect(bothAtTheCeiling.reason).toBe(LIVENESS_REASON.PID_MATCHED);
  });

  it('rejects an instant one millisecond past the maximum representable Date on both sides', () => {
    // The other half of the same boundary: no `Date` can hold this value, so no
    // start time ever had it, whatever the caller's clock says.
    const entrySide = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, 8.64e15 + 1),
      liveSnapshot(),
      Number.NaN,
    );
    const snapshotSide = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, LIVE_STARTED_AT),
      [{ agentId: String(LIVE_PID), rssMb: 512, startedAt: 8.64e15 + 1 }],
      Number.NaN,
    );

    expect(entrySide.reason).toBe(LIVENESS_REASON.NO_PID);
    expect(snapshotSide.reason).toBe(LIVENESS_REASON.MALFORMED_PROCESS_ROW);
  });

  it.each([
    ['a space-padded decimal', ` ${LIVE_PID}`],
    ['a hexadecimal literal', '0x2329'],
    ['a one-element array wrapper', [LIVE_PID]],
    ['a leading-plus decimal', `+${LIVE_PID}`],
  ])('matches a row agentId by String coercion and strict equality, not by loose coercion (%s)', (_label, agentId) => {
    // `==` against the numeric pid would match every one of these, widening
    // what counts as the SAME process on the strength of JavaScript's coercion
    // table. `String(...) === ...` matches only the array wrapper, whose
    // coercion is the same one the module applies to the pid itself.
    const row = [{ agentId, rssMb: 512, startedAt: LIVE_STARTED_AT }];

    const verdict = classifyLedgerEntryLiveness(dibsEntry(LIVE_PID, LIVE_STARTED_AT), row, SNAPSHOT_AT);

    const matchesUnderStringCoercion = String(agentId) === String(LIVE_PID);
    expect(verdict.status).toBe(
      matchesUnderStringCoercion ? LIVENESS_STATUS.ALIVE : LIVENESS_STATUS.CONFIRMED_DEAD,
    );
  });

  it.each([
    ['a numeric string', '0'],
    ['a numeric string below the default', '100'],
    ['null', null],
    ['an empty array', []],
    ['true', true],
    ['a Date', new Date(0)],
  ])('ignores a non-number toleranceMs (%s) rather than coercing it into a narrower window', (_label, toleranceMs) => {
    // `Number(options?.toleranceMs)` would turn each of these into a number —
    // `'0'`, `null` and `[]` all become `0` — narrowing the window and issuing
    // a `pid-reuse` death certificate to a process that is running. The
    // override is a number or it is not an override.
    const withinTheDefault = dibsEntry(LIVE_PID, LIVE_STARTED_AT - 500);

    const verdict = classifyLedgerEntryLiveness(withinTheDefault, liveSnapshot(), SNAPSHOT_AT, {
      toleranceMs,
    });

    expect(verdict.status).toBe(LIVENESS_STATUS.ALIVE);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_MATCHED);
  });

  it('names the structurally-degraded-snapshot obligation as the caller obligation it is', () => {
    // The classifier sees one record per call and cannot count, so a snapshot
    // of rows that carry no usable identity is indistinguishable from a valid
    // empty one and reaps every record. Defensible here, but only if the layer
    // that CAN see the whole snapshot is told it owns the check.
    const prose = flattenComments(readFileSync(RECONCILIATION_SOURCE_PATH, 'utf8'));

    expect(prose).toMatch(/CALLER OBLIGATION/);
    expect(prose).toMatch(/structurally degraded/i);
  });

  it('reaps every record against a snapshot whose rows carry no usable agentId, as the obligation describes', () => {
    // The behaviour the obligation exists because of, asserted as-is so the
    // prose above is a statement about this module rather than an aspiration.
    const degraded = [{ rssMb: 512, startedAt: LIVE_STARTED_AT }, { agentId: null, rssMb: 8, startedAt: 1 }];

    const verdict = classifyLedgerEntryLiveness(
      dibsEntry(LIVE_PID, LIVE_STARTED_AT),
      degraded,
      SNAPSHOT_AT,
    );

    expect(verdict.status).toBe(LIVENESS_STATUS.CONFIRMED_DEAD);
    expect(verdict.reason).toBe(LIVENESS_REASON.PID_ABSENT);
  });
});
