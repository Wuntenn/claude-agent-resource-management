// Phase 3 — unit tests for `resolveNearestClaudeRootIdentity`, the
// ancestry-walk that produces the ledger identity every one of `cli.mjs`'s
// five ledger write call sites records.
//
// `lib/recon.mjs` exports no such function, so this file is red by
// construction (an undefined-export TypeError on every test) until Phase 3's
// implementer adds it. Do not add a stub to force green.
//
// ADDITIVE to `./recon.jest.spec.mjs` and `./recon-per-agent.jest.spec.mjs` —
// it changes neither `attributeAgentProcesses`' nor `listAgentProcesses`'
// existing contract.
//
// =========================================================================
// CONTRACT UNDER TEST
// =========================================================================
//
//   resolveNearestClaudeRootIdentity(psTreeOutput, startPid, now)
//     -> { pid: number, pidStartedAt: number } | null
//
//   psTreeOutput  `ps -Ao pid,ppid,rss,etime,comm`-style plaintext — the SAME
//                 shape `listAgentProcesses` parses, matched by
//                 `PS_LINE_WITH_ETIME_PATTERN`, header line skipped via
//                 `PS_HEADER_WITH_ETIME_PATTERN`.
//   startPid      The pid the upward walk starts AT. Production passes
//                 `process.ppid`. Accepted as a number or a numeric string:
//                 `process.ppid` is a number while the pids parsed out of
//                 `ps` text are strings, and a caller must not have to know
//                 which side does the coercion.
//   now           Epoch-ms instant the snapshot was taken.
//
//   Returns the NEAREST process, at or above `startPid` in parent-process
//   ancestry, whose `comm` matches `AGENT_ROOT_COMM_PATTERN` (`/\/claude$/`,
//   case-SENSITIVE), as:
//     pid           that ancestor's pid, as a NUMBER. Number, not string,
//                   because it is written straight onto a ledger record and
//                   `classifyLedgerEntryLiveness`' `isPidLike` gate
//                   (./reconciliation.mjs) admits only an integer — a
//                   stringified pid there reads as absent identity, and an
//                   entry with absent identity that is genuinely alive is the
//                   failure mode this whole ticket exists to prevent.
//     pidStartedAt  `now - parseEtimeToSeconds(etime) * 1000` for that same
//                   ancestor — the identical derivation `listAgentProcesses`
//                   applies to its own roots' `startedAt`, REUSED from this
//                   module rather than reimplemented. That shared formula is
//                   the entire reason a recorded identity correlates with a
//                   later snapshot's row for the same pid.
//
//   Returns `null` — never throws — whenever no such ancestor is resolvable.
//   `null` means "omit `pid`/`pidStartedAt` from this beat's writes", which
//   degrades to the pre-existing TTL-only governance. It is a safe outcome,
//   not an error, and callers must never substitute a sentinel pid for it.
//
// `startPid` ITSELF IS IN SCOPE FOR THE MATCH. The walk starts at `startPid`
// and climbs; if `startPid`'s own `comm` is claude-rooted it is the answer.
// That is the ordinary production shape — an orchestrator spawning `cli.mjs`
// directly makes the claude root the child's immediate `process.ppid` — so
// excluding it would break the common case, not a corner one.
//
// WHY "NEAREST", NOT "TOPMOST", IS LOAD-BEARING. Nested claude-rooted
// processes (a claude process spawning another claude process) are normal in
// this domain, and `listAgentProcesses` already attributes every process to
// its NEAREST claude-rooted ancestor rather than to every claude-rooted
// ancestor above it (see its doc comment). An identity resolved to the
// TOPMOST root would therefore name a pid whose snapshot row exists but whose
// tree does not contain the writer — the two sides would disagree about which
// agent the record belongs to. The nested-roots test below is the one that
// pins this; it is called out because an earlier review recorded this
// distinction as explicitly untested.
//
// WHY A DECOY MATTERS. An implementation that scans the snapshot for "any
// claude-rooted pid" instead of walking ancestry passes every single-root
// fixture. The unrelated-branch test below is what separates the two.

import {
  listAgentProcesses,
  parseEtimeToSeconds,
  resolveNearestClaudeRootIdentity,
} from './recon.mjs';

/** Fixed `now`, so every `pidStartedAt` expectation below is exact. */
const NOW_MS = Date.parse('2026-08-28T12:00:00Z');

const CLAUDE_COMM = '/Users/someone/.local/bin/claude';
const NODE_COMM = '/usr/local/bin/node';
/** The Claude DESKTOP app's binary — capital `C`, deliberately NOT a match. */
const DESKTOP_APP_COMM = '/Applications/Claude.app/Contents/MacOS/Claude';

const HEADER = '  PID  PPID    RSS     ELAPSED COMM';

/** One `ps -Ao pid,ppid,rss,etime,comm` row. */
function row(pid, ppid, { rssKb = 65_536, etime = '00:10:00', comm = NODE_COMM } = {}) {
  return `${pid} ${ppid} ${rssKb} ${etime} ${comm}`;
}

/** Assembles rows into a snapshot, header included (it must be skipped). */
function snapshot(...rows) {
  return [HEADER, row(1, 0, { rssKb: 512, etime: '1-03:46:39', comm: '/sbin/launchd' }), ...rows, ''].join('\n');
}

/** The derivation both sides of the correlation must agree on. */
function startedAtFor(etime, now = NOW_MS) {
  return now - parseEtimeToSeconds(etime) * 1000;
}

describe('resolveNearestClaudeRootIdentity — finding the ancestor', () => {
  it('climbs past multiple non-claude hops to the claude-rooted ancestor above them', () => {
    // 1 -> 5000 (claude) -> 5001 (node) -> 5002 (node) -> 5003 (node, the
    // walk's starting point). Three intervening hops, so an implementation
    // that inspects only the immediate parent cannot pass.
    const ps = snapshot(
      row(5000, 1, { etime: '01:00:00', comm: CLAUDE_COMM }),
      row(5001, 5000),
      row(5002, 5001),
      row(5003, 5002),
    );

    expect(resolveNearestClaudeRootIdentity(ps, 5003, NOW_MS)).toEqual({
      pid: 5000,
      pidStartedAt: startedAtFor('01:00:00'),
    });
  });

  it('resolves `startPid` itself when `startPid` is the claude-rooted process (the direct-spawn shape)', () => {
    // An orchestrator spawning cli.mjs directly makes the claude root the
    // child's own `process.ppid`. If the walk skipped its starting point,
    // this — the ordinary production shape — would resolve to `null` and
    // every real beat would write a pid-less, TTL-only record.
    const ps = snapshot(row(6000, 1, { etime: '00:45:00', comm: CLAUDE_COMM }));

    expect(resolveNearestClaudeRootIdentity(ps, 6000, NOW_MS)).toEqual({
      pid: 6000,
      pidStartedAt: startedAtFor('00:45:00'),
    });
  });

  it('returns the NEAREST claude-rooted ancestor, not the topmost, when claude processes are nested', () => {
    // 1 -> 7000 (claude, outer) -> 7001 (node) -> 7002 (claude, INNER)
    //   -> 7003 (node) -> 7004 (node, the walk's start).
    // Both 7000 and 7002 match the root pattern. `listAgentProcesses`
    // attributes 7003/7004 to 7002's tree (a nested root owns its own
    // subtree exclusively), so the identity must name 7002 for the two sides
    // to agree about which agent the record belongs to.
    const ps = snapshot(
      row(7000, 1, { etime: '04:00:00', comm: CLAUDE_COMM }),
      row(7001, 7000),
      row(7002, 7001, { etime: '00:20:00', comm: CLAUDE_COMM }),
      row(7003, 7002),
      row(7004, 7003),
    );

    expect(resolveNearestClaudeRootIdentity(ps, 7004, NOW_MS)).toEqual({
      pid: 7002,
      pidStartedAt: startedAtFor('00:20:00'),
    });
  });

  it('ignores a claude-rooted process sitting on an unrelated branch', () => {
    // 8000 (claude) is the answer; 8900 (claude) is a decoy hanging off pid 1
    // on a branch the walk never touches. An implementation that scans for
    // "any claude-rooted pid" resolves 8900 (or 8000, by luck of iteration
    // order) rather than walking, and this test is what catches it.
    const ps = snapshot(
      row(8000, 1, { etime: '02:00:00', comm: CLAUDE_COMM }),
      row(8001, 8000),
      row(8900, 1, { etime: '03:00:00', comm: CLAUDE_COMM }),
      row(8901, 8900),
    );

    expect(resolveNearestClaudeRootIdentity(ps, 8001, NOW_MS)).toEqual({
      pid: 8000,
      pidStartedAt: startedAtFor('02:00:00'),
    });
  });

  it('does not match a capitalized `.../MacOS/Claude` desktop-app ancestor', () => {
    // `AGENT_ROOT_COMM_PATTERN` is case-SENSITIVE by deliberate design (see
    // its comment in ./recon.mjs). The desktop app is not an agent root, so
    // an ancestry chain whose only "claude-ish" ancestor is the desktop app
    // resolves to no identity at all.
    const ps = snapshot(row(9000, 1, { etime: '05:00:00', comm: DESKTOP_APP_COMM }), row(9001, 9000));

    expect(resolveNearestClaudeRootIdentity(ps, 9001, NOW_MS)).toBeNull();
  });

  it('accepts a numeric-string `startPid` as readily as a number', () => {
    // `process.ppid` is a number; the pids parsed out of `ps` text are
    // strings. Whichever side a caller passes, the same ancestor resolves.
    const ps = snapshot(row(9100, 1, { etime: '00:30:00', comm: CLAUDE_COMM }), row(9101, 9100));

    expect(resolveNearestClaudeRootIdentity(ps, '9101', NOW_MS)).toEqual(
      resolveNearestClaudeRootIdentity(ps, 9101, NOW_MS),
    );
    expect(resolveNearestClaudeRootIdentity(ps, '9101', NOW_MS)).toEqual({
      pid: 9100,
      pidStartedAt: startedAtFor('00:30:00'),
    });
  });

  it('returns a NUMBER pid, the shape `classifyLedgerEntryLiveness` can read as an identity', () => {
    const ps = snapshot(row(9200, 1, { etime: '00:15:00', comm: CLAUDE_COMM }), row(9201, 9200));

    const identity = resolveNearestClaudeRootIdentity(ps, 9201, NOW_MS);

    expect(typeof identity.pid).toBe('number');
    expect(typeof identity.pidStartedAt).toBe('number');
  });
});

describe('resolveNearestClaudeRootIdentity — every unresolvable case is `null`, never a throw', () => {
  it('returns null when the ancestry chain contains no claude-rooted process at all', () => {
    const ps = snapshot(row(4000, 1), row(4001, 4000), row(4002, 4001));

    expect(resolveNearestClaudeRootIdentity(ps, 4002, NOW_MS)).toBeNull();
  });

  it('returns null on ps output nothing in it can be parsed out of', () => {
    for (const unparseable of ['', '   ', 'ps: illegal option -- Z', 'not\nremotely\ntabular', ' ']) {
      expect(resolveNearestClaudeRootIdentity(unparseable, 4002, NOW_MS)).toBeNull();
    }
  });

  it('returns null on a nullish or non-string snapshot', () => {
    // `collectPsOutput()` degrades to `''`, but the parameter is public and
    // a caller threading a failed read must get `null` rather than a throw.
    for (const value of [null, undefined, 42, {}]) {
      expect(resolveNearestClaudeRootIdentity(value, 4002, NOW_MS)).toBeNull();
    }
  });

  it('returns null on a `startPid` that is neither a number nor a string', () => {
    // The contract says "never throws", and the walk begins by coercing
    // `startPid` to a string. A caller threading a garbled value must get the
    // same `null` every other unresolvable case gets, so the type is rejected
    // before any coercion happens.
    const ps = snapshot(row(4800, 1, { etime: '01:00:00', comm: CLAUDE_COMM }), row(4801, 4800));

    for (const value of [Symbol('4801'), null, undefined, {}, [], true, () => 4801]) {
      expect(resolveNearestClaudeRootIdentity(ps, value, NOW_MS)).toBeNull();
    }
  });

  it('returns null on a `startPid` whose own coercion throws, rather than propagating the throw', () => {
    // NON-VACUITY (pre-PR review, Medium). The corpus above does not actually
    // pin the type guard: delete `typeof startPid !== 'number' && !== 'string'`
    // outright and all of those values still return `null`, because `String(x)`
    // on each yields a string matching no pid key. That includes `Symbol` —
    // `String(Symbol('4801'))` returns `'Symbol(4801)'` and does NOT throw
    // (only IMPLICIT ToString, e.g. a template literal, throws on a symbol),
    // which is why the rationale that used to be written here was wrong.
    //
    // The guard's removal is observable on exactly one class of input: a value
    // whose own `toString`/`valueOf` throws. Without the guard that throw
    // escapes `resolveNearestClaudeRootIdentity` and breaks the "never throws"
    // contract the whole fail-conservative posture rests on — a beat would die
    // rather than write a record with no identity.
    const ps = snapshot(row(4800, 1, { etime: '01:00:00', comm: CLAUDE_COMM }), row(4801, 4800));
    const hostile = [
      {
        toString() {
          throw new Error('toString is hostile');
        },
      },
      {
        valueOf() {
          throw new Error('valueOf is hostile');
        },
        toString: undefined,
      },
    ];

    for (const value of hostile) {
      expect(() => resolveNearestClaudeRootIdentity(ps, value, NOW_MS)).not.toThrow();
      expect(resolveNearestClaudeRootIdentity(ps, value, NOW_MS)).toBeNull();
    }
  });

  it('returns null on a `now` that is not a finite number, rather than propagating the throw', () => {
    // THE OTHER OPERAND OF THE SAME CONTRACT (pre-PR review, Medium). `startPid`
    // was gated by type against a hostile coercion; `now` was not, and it
    // reaches arithmetic — `now - etimeSeconds * 1000`. `Symbol` and `BigInt`
    // both THROW there (unlike `String()`, `-` uses ToNumeric, which raises on a
    // symbol and refuses to mix a BigInt), and so does a hostile `valueOf`. The
    // snapshot here resolves a real claude root, so the walk genuinely reaches
    // the arithmetic and each of these values would escape the "never throws"
    // contract the fail-conservative posture rests on.
    const ps = snapshot(row(4802, 1, { etime: '01:00:00', comm: CLAUDE_COMM }), row(4803, 4802));
    expect(resolveNearestClaudeRootIdentity(ps, 4803, NOW_MS)).not.toBeNull();

    const unusable = [
      Symbol('now'),
      1n,
      {
        valueOf() {
          throw new Error('valueOf is hostile');
        },
      },
      NaN,
      Infinity,
      '1700000000000',
      null,
      undefined,
    ];

    for (const value of unusable) {
      expect(() => resolveNearestClaudeRootIdentity(ps, 4803, value)).not.toThrow();
      expect(resolveNearestClaudeRootIdentity(ps, 4803, value)).toBeNull();
    }
  });

  it('returns null when the derived `pidStartedAt` is not a usable epoch-ms instant', () => {
    // NON-VACUITY (pre-PR review, Low). The `Number.isInteger(pidStartedAt) ||
    // pidStartedAt <= 0` guard could be deleted outright with the whole suite
    // still green — nothing exercised a `now` that is finite but yields an
    // unusable instant. Both halves are reachable from a caller threading a
    // wrong clock: a FRACTIONAL `now` yields a fractional instant, which the
    // read side's `isEpochMsLike` rejects, and a `now` supplied in SECONDS (or
    // simply too small) yields zero or a negative one. Either way the pair is
    // worthless on disk — a `pid` whose partner can never match — so the whole
    // identity is withheld rather than persisted as dead weight.
    const ps = snapshot(row(4804, 1, { etime: '01:00:00', comm: CLAUDE_COMM }), row(4805, 4804));
    const ONE_HOUR_MS = 3_600_000;

    // Fractional anchor -> fractional instant.
    expect(resolveNearestClaudeRootIdentity(ps, 4805, NOW_MS + 0.5)).toBeNull();
    // Anchor at exactly the process's own start -> zero, not a positive instant.
    expect(resolveNearestClaudeRootIdentity(ps, 4805, ONE_HOUR_MS)).toBeNull();
    // Anchor earlier than the process's own start -> negative.
    expect(resolveNearestClaudeRootIdentity(ps, 4805, ONE_HOUR_MS - 1)).toBeNull();
    // One millisecond later is the smallest anchor that DOES resolve, which is
    // what makes the three rejections above about the boundary and not about
    // the walk failing for some unrelated reason.
    expect(resolveNearestClaudeRootIdentity(ps, 4805, ONE_HOUR_MS + 1)).toEqual({
      pid: 4804,
      pidStartedAt: 1,
    });
  });

  it('returns null when `startPid` is absent from the snapshot', () => {
    // The walk has no anchor: a process that exited between the `ps` run and
    // this call, or a pid the snapshot simply never enumerated.
    const ps = snapshot(row(4100, 1, { etime: '01:00:00', comm: CLAUDE_COMM }), row(4101, 4100));

    expect(resolveNearestClaudeRootIdentity(ps, 4999, NOW_MS)).toBeNull();
  });

  it('returns null when the chain runs off the top of the snapshot before reaching a claude root', () => {
    // 4201's ppid names 4200, which no row describes — a reparented or
    // partially-captured tree. Broken ancestry proves nothing, so: null.
    const ps = snapshot(row(4201, 4200), row(4202, 4201));

    expect(resolveNearestClaudeRootIdentity(ps, 4202, NOW_MS)).toBeNull();
  });

  it('terminates and returns null on a self-parenting process', () => {
    // 4300's ppid is itself. A walk that follows ppid unconditionally spins
    // here forever; this test fails by TIMING OUT rather than by assertion
    // if termination is not guaranteed.
    const ps = snapshot(row(4300, 4300));

    expect(resolveNearestClaudeRootIdentity(ps, 4300, NOW_MS)).toBeNull();
  }, 5_000);

  it('terminates and returns null on a multi-process ppid cycle', () => {
    // 4400 -> 4401 -> 4402 -> 4400. Same failure mode as the self-parent
    // case, but not caught by a naive `pid !== ppid` guard.
    const ps = snapshot(row(4400, 4402), row(4401, 4400), row(4402, 4401));

    expect(resolveNearestClaudeRootIdentity(ps, 4401, NOW_MS)).toBeNull();
  }, 5_000);

  it('walks a 50,000-deep chain without exhausting the call stack', () => {
    // Iterative, not recursive. A recursive walk overflows here and the
    // failure surfaces as a RangeError rather than a `null` — which is
    // precisely the difference this pins.
    const rows = [];
    for (let pid = 4500; pid < 54_500; pid += 1) rows.push(row(pid, pid === 4500 ? 1 : pid - 1));

    expect(resolveNearestClaudeRootIdentity(snapshot(...rows), 54_499, NOW_MS)).toBeNull();
  }, 20_000);

  it('returns null when the resolved claude root carries an unparseable etime', () => {
    // `parseEtimeToSeconds` yields NaN, and this module's standing "don't
    // fabricate" convention forbids inventing a `startedAt` in its place —
    // `listAgentProcesses` drops such a root entirely for the same reason.
    // Fabricating one here would be worse than omitting the identity: a
    // wrong `pidStartedAt` reads as PID REUSE on the next sweep, which is a
    // reap of a live agent.
    const ps = snapshot(row(4600, 1, { etime: '-', comm: CLAUDE_COMM }), row(4601, 4600));

    expect(resolveNearestClaudeRootIdentity(ps, 4601, NOW_MS)).toBeNull();
  });

  it('does not skip past an unparseable-etime root to a claude root further up', () => {
    // 4700 (claude, good etime) -> 4701 (claude, BROKEN etime) -> 4702.
    // The nearest root is 4701 and it cannot be described, so the answer is
    // "no identity" — not 4700, whose tree does not own 4702 (a nested root
    // owns its own subtree exclusively, per `listAgentProcesses`). Silently
    // promoting the grandparent would record an identity the read side
    // attributes to a different agent.
    const ps = snapshot(
      row(4700, 1, { etime: '06:00:00', comm: CLAUDE_COMM }),
      row(4701, 4700, { etime: 'not-an-etime', comm: CLAUDE_COMM }),
      row(4702, 4701),
    );

    expect(resolveNearestClaudeRootIdentity(ps, 4702, NOW_MS)).toBeNull();
  });
});

describe('resolveNearestClaudeRootIdentity — one formula shared with listAgentProcesses', () => {
  it('derives a `pidStartedAt` byte-identical to the `startedAt` listAgentProcesses reports for the same pid', () => {
    // THE PROPERTY THE WHOLE CORRELATION RESTS ON. The write side records
    // `pidStartedAt` from this helper; a later beat's read side compares it
    // against `listAgentProcesses`' `startedAt` within
    // `PID_IDENTITY_TOLERANCE_MS`. If the two derivations ever diverge — a
    // rounding difference, a second `Date.now()` read, a reimplemented
    // parser — a genuinely live agent drifts out of tolerance and is reaped
    // as PID REUSE. Cross-checking against `listAgentProcesses`' own output
    // on ONE snapshot is what pins them to a single formula.
    const ps = snapshot(
      row(3000, 1, { etime: '1-02:03:04', comm: CLAUDE_COMM }),
      row(3001, 3000),
      row(3002, 3001),
    );

    const identity = resolveNearestClaudeRootIdentity(ps, 3002, NOW_MS);
    const agentEntry = listAgentProcesses(ps, NOW_MS).find((entry) => entry.agentId === String(identity.pid));

    expect(agentEntry).toBeDefined();
    expect(identity.pidStartedAt).toBe(agentEntry.startedAt);
    // Exactly zero drift — not "within tolerance". The tolerance exists to
    // absorb `etime`'s ~1s granularity ACROSS beats, not to paper over two
    // disagreeing derivations within one snapshot.
    expect(identity.pidStartedAt - agentEntry.startedAt).toBe(0);
  });

  it('agrees with listAgentProcesses for every claude root in a nested-tree snapshot', () => {
    // Two roots, two walks, both cross-checked — so the agreement above is
    // a property of the derivation rather than a coincidence of one fixture.
    const ps = snapshot(
      row(3100, 1, { etime: '03:00:00', comm: CLAUDE_COMM }),
      row(3101, 3100),
      row(3102, 3101, { etime: '00:00:45', comm: CLAUDE_COMM }),
      row(3103, 3102),
    );

    const byAgentId = new Map(listAgentProcesses(ps, NOW_MS).map((entry) => [entry.agentId, entry]));

    for (const [startPid, expectedRoot] of [
      [3101, 3100],
      [3103, 3102],
    ]) {
      const identity = resolveNearestClaudeRootIdentity(ps, startPid, NOW_MS);
      expect(identity.pid).toBe(expectedRoot);
      expect(identity.pidStartedAt).toBe(byAgentId.get(String(expectedRoot)).startedAt);
    }
  });

  it('shifts `pidStartedAt` exactly with `now`, since `now` is the only clock it reads', () => {
    // No internal `Date.now()`: the caller's `now` is the sole anchor, which
    // is what lets one beat resolve one identity for all five of its writes.
    const ps = snapshot(row(3200, 1, { etime: '00:05:00', comm: CLAUDE_COMM }), row(3201, 3200));

    const first = resolveNearestClaudeRootIdentity(ps, 3201, NOW_MS);
    const later = resolveNearestClaudeRootIdentity(ps, 3201, NOW_MS + 60_000);

    expect(first.pidStartedAt).toBe(startedAtFor('00:05:00'));
    expect(later.pidStartedAt - first.pidStartedAt).toBe(60_000);
  });

  it('reads only its arguments — the same snapshot resolves identically however many times it is called', () => {
    // Pure and side-effect-free, like every other function in this module:
    // no shell-out of its own, no memoization, no mutation of the input.
    const ps = snapshot(row(3300, 1, { etime: '02:30:00', comm: CLAUDE_COMM }), row(3301, 3300));
    const before = ps;

    const results = [0, 1, 2].map(() => resolveNearestClaudeRootIdentity(ps, 3301, NOW_MS));

    expect(results[1]).toEqual(results[0]);
    expect(results[2]).toEqual(results[0]);
    expect(ps).toBe(before);
  });
});
