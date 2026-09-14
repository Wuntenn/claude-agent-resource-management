// Phase 0 — OUTER ACCEPTANCE TEST: post-crash ledger
// reconciliation ("reap ghost entries, never live agents").
//
// This is the outer test for the whole ticket: it passes only when all
// three of the ticket's parts compose, and fails if any one of them regresses.
// What each part contributes:
//   Phase 1 — additive, PURELY CALLER-SUPPLIED `pid` / `pidStartedAt` fields
//             on the dibs entry, on `claimCapacity`'s per-claim record, on
//             `reserveAdmission`'s admission-ledger claim record, and on
//             `lib/dispatch.mjs`'s `claimAndAdmitQueueEntry` pass-through.
//             Phase 1 computes NOTHING — it only persists/forwards.
//   Phase 2 — `classifyLedgerEntryLiveness(...)`: pid + start-time identity
//             correlation against `listAgentProcesses`' output
//             (alive / confirmed-dead / unknown).
//   Phase 3 — ancestry-derived identity resolution + the beat wiring: ONE
//             process snapshot per beat, taken OUTSIDE any `withLock` hold,
//             used BOTH to resolve this beat's write-side identity AND to
//             classify+reap, run EARLY enough in `main()` that the freed
//             capacity is visible to that SAME invocation's own
//             allowance/admission math.
//
// =========================================================================
// WHY THE FIXTURE SEEDS A PROCESS TREE RATHER THAN A KNOWN PID
// =========================================================================
// STANDING RULE FOR ANY OUTER TEST IN THIS AREA: seed a process tree and
// assert on the identity THE CODE DERIVED FROM IT. Never hand a "correct" pid
// to the fixture and then assert the ledger contains it.
//
// The reason is that a fixture supplying the right pid externally passes
// IDENTICALLY whether the write path records `process.pid` or an
// ancestry-walk-derived pid. It never asks the write path *which pid it
// actually chose to record* — and that choice is precisely the property this
// ticket's Secure AC turns on.
//
// Why the choice matters: `cli.mjs` is a one-shot, ~62ms-per-invocation CLI,
// not a daemon. `listAgentProcesses` (`lib/recon.mjs`) only ever emits entries
// for CLAUDE-ROOTED processes (`comm` matching `AGENT_ROOT_COMM_PATTERN =
// /\/claude$/`). A `node cli.mjs` process's `comm` can never match that
// pattern, so a recorded `process.pid` is structurally invisible to this
// ticket's own read-side classifier — and Phase 2's "pid absent from snapshot
// → confirmed-dead" rule would reap every LIVE orchestrator on the very next
// sweep. That is the exact over-admission inversion of the Secure AC.
//
// So this file:
//   * seeds a REALISTIC MULTI-PROCESS TREE into `ARM_FAKE_PS_OUTPUT` — a fake
//     claude-rooted parent, two non-claude `node` hops beneath it, and the
//     real `cli.mjs` invocation hanging off the bottom of that chain — plus a
//     DECOY claude-rooted process on an unrelated branch;
//   * drives the REAL write path (a real `--claim=live-agent:1` beat) under
//     that tree to obtain the "genuinely live" record, and
//   * asserts the ledger carries THE PID THE WRITE PATH ACTUALLY PRODUCED,
//     proving it is the ancestry-resolved PARENT's pid and emphatically not
//     any pid belonging to the `cli.mjs` invocation itself.
//
// -----------------------------------------------------------------------
// HOW THE ANCESTRY FIXTURE IS CONSTRUCTED (and why it exercises the walk)
// -----------------------------------------------------------------------
// The ancestry resolver walks UP from `process.ppid` — the `cli.mjs` child's
// parent — until it finds a process whose `comm` matches
// `AGENT_ROOT_COMM_PATTERN`. The chain it must climb is therefore anchored at
// a pid this spec genuinely knows AT FIXTURE-CONSTRUCTION TIME: because we
// spawn `cli.mjs` directly, the child's `process.ppid` IS this test process's
// own `process.pid`. So the snapshot is generated, not hardcoded:
//
//     1 (launchd)
//     ├── DECOY_CLAUDE_ROOT_PID   comm .../MacOS/claude   (unrelated branch,
//     │                                                    EMITTED FIRST)
//     └── FAKE_CLAUDE_ROOT_PID    comm .../MacOS/claude   <-- the answer
//         └── TEST_PPID           comm .../node           (non-claude hop 1)
//             └── TEST_PID        comm .../node           (non-claude hop 2,
//                                                          = cli.mjs's real
//                                                          `process.ppid`)
//                 └── (the real `cli.mjs` child — see the note below)
//
// This is load-bearing in three independent ways:
//   * TWO non-claude hops separate the walk's starting point from the answer,
//     so an implementation that only looks at the immediate parent fails.
//   * The DECOY claude-rooted process means an implementation that simply
//     scans the snapshot for "any claude-rooted pid" (rather than walking
//     ancestry) resolves the WRONG pid and fails. The decoy's ROW ORDER is
//     part of that claim, not incidental: it is emitted BEFORE the real root,
//     so the commonest shortcut of all — take the first claude-rooted row —
//     lands on the decoy rather than accidentally on the right answer.
//   * The starting point is the REAL `process.ppid` of the real spawned
//     child, so the walk is exercised against the actual runtime tree — not
//     against a wholly synthetic start pid the code never sees.
//
// ON THE `cli.mjs` CHILD'S OWN ROW: `ARM_FAKE_PS_OUTPUT` is an ENV VAR, and
// a child's pid is only knowable AFTER `spawn` returns — by which time its
// environment is already fixed. There is therefore no way to inject the real
// child's own row through this seam, and deliberately no fabricated row is
// added for it: a fabricated `cli.mjs` row would either be a `node` row (the
// walk skips it anyway, so it proves nothing) or a claude-rooted row (a lie
// about the real world, and one that would make a self-referential
// implementation pass). The walk STARTS at `process.ppid`, so the child's own
// row is never consulted — omitting it costs this test nothing, and inventing
// one would cost it its whole point.
//
// -----------------------------------------------------------------------
// WHAT THIS PROVES (the ticket's Functional + Secure ACs, end to end,
// black-box, through the real `cli.mjs` as a child process — never imported
// directly, matching ./cli-two-orchestrators.jest.spec.mjs's idiom):
//   1. The write path derives its recorded identity by ANCESTRY WALK: the
//      pid a real beat records is the claude-rooted PARENT's, never the
//      `cli.mjs` invocation's own.
//   2. A simulated host death (dibs + `__claim:live-agent__` +
//      `__claim:live-agent-admission__` records whose recorded pids belong to
//      processes confirmed NOT running) is FULLY reconciled after exactly ONE
//      `--desired-agents` beat — with NO TTL wait. Every seeded fixture below
//      is deliberately well INSIDE its own ledger's TTL window
//      (`DEFAULT_CLAIM_TTL_MS` = 6h for claims, `DEFAULT_LIVENESS_THRESHOLD_MS`
//      = 15min for dibs), so nothing here can be reaped by the pre-existing
//      time-based aging path. If these entries disappear, it is because the
//      new liveness sweep removed them, not because they aged out.
//   3. The freed capacity is reflected in THAT SAME invocation's own output
//      (`allowance` and `liveAgentGrant`) — not merely in the on-disk file
//      afterwards. This is the actual functional promise ("restores full
//      capacity within one beat"); a test that only inspected the file could
//      pass while the capacity math was still wrong.
//   4. A genuinely LIVE agent is NEVER reaped (the Secure AC) — and its
//      liveness is expressed through the identity THE CODE PRODUCED in an
//      earlier real beat, not through a pid this file invented.
//   5. The ` `-adjacent discovery-walk-omission case: a ledger entry that
//      the process snapshot does not enumerate AT ALL and that carries NO
//      recorded pid classifies as *unknown*, defers to the pre-existing TTL
//      safety net, and is therefore left untouched by this beat. This proves
//      the immediate-reap path and the TTL path compose rather than one
//      silently overriding the other. Note what this is and is not: the
//      pid-less entry is the sub-case `classifyLedgerEntryLiveness`' own header
//      calls UNAFFECTED by the walk-omission limitation — with no recorded
//      identity there is nothing an omission could change. It is the CONTROL,
//      not the limitation. (Limitation one proper — an entry that HAS a pid
//      which the walk omits — is a documented, accepted residual limitation of
//      `classifyLedgerEntryLiveness` and is reaped `confirmed-dead`. It is NOT
//      covered here; it is named in that function's own header comment in
//      ./lib/reconciliation.mjs under "RESIDUAL LIMITATION ONE", and pinned as
//      a test in ./lib/reconciliation.jest.spec.mjs ("reads a pid-bearing entry the walk omits as confirmed-dead") — "reads a pid-bearing
//      entry the walk omits as confirmed-dead". It is not composable end-to-end
//      from here, because no fixture can make the walk omit a row it was handed.
//      An ADR recording the residual is planned and does not exist yet.)
//
// -----------------------------------------------------------------------
// SEAM CONTRACT — READ THIS BEFORE CHANGING THE WIRING.
// -----------------------------------------------------------------------
// This test injects the beat's process snapshot through the EXISTING seam:
//
//     ARM_FAKE_PS_OUTPUT
//
// opted into with the beat snapshot's Layer 1 master switch:
//
//     ARM_BEAT_SNAPSHOT_TEST_MODE=1
//
// The switch is not a second snapshot seam and injects nothing — it is the
// gate that decides whether `collectBeatPsSnapshot` reads `ARM_FAKE_PS_*` at
// all. Unset (the default, and true of every real orchestrator invocation) the
// beat shells out to a real `ps`, so a leaked or stale `ARM_FAKE_PS_OUTPUT`
// cannot decide which records this beat's sweep DELETES from the shared
// coordination file. `ARM_COORDINATION_FILE` cannot serve as that gate: every
// real invocation sets it. Same two-layer shape `ARM_EVICT_TEST_MODE` gives the
// eviction beat.
//
// The snapshot seam itself is deliberately NOT a new env var. `ARM_FAKE_PS_OUTPUT` already exists in
// `cli.mjs` (see ./cli-two-orchestrators.jest.spec.mjs test 1 and
// ./cli.jest.spec.mjs's `PS_ONE_AGENT` fixtures): plaintext in
// `ps -Ao pid,ppid,rss,etime,comm` column order, fed through
// `./lib/recon.mjs`'s `listAgentProcesses(psTreeOutput, now)`.
//
// The contract this file asserts is therefore:
//   * The per-beat process snapshot MUST be sourced from the same
//     `collectPsOutput()`-style path that already honours `ARM_FAKE_PS_OUTPUT`,
//     so a test can substitute a synthetic host process table. Do NOT add a
//     second, parallel fake-process seam (e.g. a JSON one) — one snapshot,
//     one seam, reused for BOTH identity resolution and all three ledgers'
//     classification within a beat.
//   * The snapshot MUST be interpreted via `listAgentProcesses(psTreeOutput,
//     now)` / the same `parseEtimeToSeconds` + `now - seconds * 1000`
//     derivation, so a resolved `pidStartedAt` and a classified `startedAt`
//     are computed by one formula. `now` is the value `resolveNow()` returns,
//     so `ARM_FAKE_NOW_MS` (the existing Phase 6 seam) fixes it — that
//     is what lets this file express an EXACT expected `pidStartedAt`.
//   * `ARM_FAKE_PS_OUTPUT` being set MUST NOT, by itself, disable the sweep or
//     identity resolution. A beat with a fake snapshot must behave exactly as
//     a beat with a real `ps` snapshot would.
//
// If `ARM_FAKE_PS_OUTPUT` is present but the sweep still shells out to the
// real `ps`, this test fails loudly rather than quietly: the real tree
// contains no claude-rooted ancestor with the etime this file encodes, so the
// recorded identity assertion below cannot accidentally pass.

import { spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { AIMD_CEILING_RESERVED_ID } from './lib/coordination-file.mjs';
import { LIVENESS_REASON } from './lib/reconciliation.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

// Mirrors of `cli.mjs`'s own exported constants, restated as literals rather
// than imported: `cli.mjs` invokes `main()` at module scope, so importing it
// from a spec runs the CLI (and `process.exit(2)`s on the missing
// `--orchestrator-id`). Every other black-box spec in this directory restates
// these the same way — see ./cli-two-orchestrators.jest.spec.mjs's
// `FULL_UNCAPPED_PER_AXIS_MAXIMUM` / `CAP`.
const LIVE_AGENT_CEILING = 4; // cli.mjs:254
const LIVE_AGENT_CLAIM_TYPE = 'live-agent'; // cli.mjs:334
const LIVE_AGENT_ADMISSION_CLAIM_TYPE = 'live-agent-admission'; // cli.mjs:393

/** Ledger entry id convention for a per-type claim ledger (`__claim:<type>__`). */
const claimLedgerId = (type) => `__claim:${type}__`;

/** A GREEN, low-pressure fixture — keeps every axis classification out of the way. */
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 40_000 };
const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

/** Fixed fake "now" (via `ARM_FAKE_NOW_MS`), so `startedAt` arithmetic is exact. */
const T0 = 1_700_000_000_000;
/** Every hand-seeded ghost record is 60s old — far inside EVERY ledger's own TTL. */
const SEEDED_AT = T0 - 60_000;

/**
 * The claude-rooted ancestor's elapsed time in the fake `ps` snapshot, and
 * therefore the exact `pidStartedAt` the ancestry resolver derives for it
 * (`now - parseEtimeToSeconds(etime) * 1000`).
 */
const ANCESTOR_ETIME = '01:00:00';
const ANCESTOR_ETIME_MS = 60 * 60 * 1000;
const EXPECTED_PID_STARTED_AT = T0 - ANCESTOR_ETIME_MS;

/**
 * The real anchors of the fixture tree. `TEST_PID` is, by construction, the
 * `process.ppid` every `cli.mjs` child spawned by `runCli` below will see —
 * which is exactly where the ancestry walk starts.
 */
const TEST_PID = process.pid;
const TEST_PPID = process.ppid;

/**
 * Synthetic pids for the two claude-rooted processes. Chosen well clear of the
 * real anchors and asserted distinct below, so the fixture can never
 * accidentally resolve to a real pid.
 */
const FAKE_CLAUDE_ROOT_PID = 910_001;
const DECOY_CLAUDE_ROOT_PID = 910_002;

/**
 * Builds the fake `ps -Ao pid,ppid,rss,etime,comm` table described in this
 * file's header. Generated (not hardcoded) precisely because the walk's
 * starting point is the real `process.ppid` of the real spawned child.
 */
function buildAncestryPsOutput() {
  return [
    '  PID  PPID    RSS     ELAPSED COMM',
    '    1     0    512 1-03:46:39 /sbin/launchd',
    // A DECOY claude root on an unrelated branch, deliberately placed BEFORE
    // the real one. Order is load-bearing: an implementation that scans for
    // "any claude-rooted process" instead of walking ancestry reaches this row
    // FIRST and resolves the wrong pid. With the real root listed first, the
    // commonest such shortcut — first match wins — would land on the right
    // answer by accident and this fixture would prove nothing.
    `${DECOY_CLAUDE_ROOT_PID}     1  98304    02:00:00 /Applications/Claude.app/Contents/MacOS/claude`,
    // The answer: the nearest claude-rooted ancestor of the cli.mjs child.
    `${FAKE_CLAUDE_ROOT_PID}     1 102400    ${ANCESTOR_ETIME} /Applications/Claude.app/Contents/MacOS/claude`,
    // Non-claude hop 1 (the jest parent), child of the claude root.
    `${TEST_PPID} ${FAKE_CLAUDE_ROOT_PID}  81920    00:30:00 /usr/local/bin/node`,
    // Non-claude hop 2 (this very test process) — the REAL parent of every
    // cli.mjs child `runCli` spawns, i.e. the walk's real starting pid.
    `${TEST_PID} ${TEST_PPID}  65536    00:10:00 /usr/local/bin/node`,
    // (No row for the cli.mjs child itself — see this file's header for why
    // that is deliberate and costs the test nothing.)
    '',
  ].join('\n');
}

/** Runs the real cli.mjs as a child process — never imports its `main()`. */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

/**
 * Returns a pid that is genuinely NOT running on this host right now, proven
 * by `process.kill(pid, 0)` raising `ESRCH`. Probed rather than hardcoded so
 * this file is honest on both macOS (default `pid_max` 99999) and Linux CI
 * (default 4194304).
 *
 * WHAT THIS PROBE DOES AND DOES NOT BUY (pre-PR review, Low — this doc-block
 * used to claim the fixture's whole premise was "verified, not assumed", which
 * overstated it). The sweep under test never consults the host: on these beats
 * its only liveness oracle is the `ARM_FAKE_PS_OUTPUT` fixture, so
 * `process.kill(pid, 0)` proves nothing about the verdict. The premise that IS
 * load-bearing — the pid appears nowhere in that fixture — is asserted at each
 * call site by `expectAbsentFromSnapshot`. What the probe buys is narrower and
 * still worth having: it keeps the chosen pid from colliding with a REAL
 * process, which matters for the host-facing assertions elsewhere in this file
 * and would make the fixture quietly dishonest about what it depicts.
 */
function pickConfirmedDeadPid(startAt) {
  for (let candidate = startAt; candidate > startAt - 500; candidate -= 1) {
    try {
      process.kill(candidate, 0);
    } catch (error) {
      if (error.code === 'ESRCH') return candidate;
    }
  }
  throw new Error(`could not find a confirmed-dead pid near ${startAt}`);
}

/**
 * Asserts the one premise these fixtures actually rest on: the pid appears
 * nowhere in the snapshot the sweep judges against. `pickConfirmedDeadPid`'s
 * host probe does not establish this — see its doc-block — so every ghost pid
 * that is supposed to read `pid-absent` states it here explicitly, the way the
 * namespace-boundary case and the beat-wiring spec already do.
 */
function expectAbsentFromSnapshot(pid) {
  expect(fakePsOutput).not.toMatch(new RegExp(`(^|\\s)${pid}(\\s|$)`, 'm'));
}

let workDir;
let coordinationFilePath;
let fakePsOutput;
let beatEnv;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-ledger-recon-'));
  coordinationFilePath = join(workDir, 'coordination.json');
  fakePsOutput = buildAncestryPsOutput();
  beatEnv = {
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_BEAT_NOW_TEST_MODE: '1',
    ARM_FAKE_NOW_MS: String(T0),
    // Layer 1 of the beat snapshot's two-layer seam model — see this file's
    // SEAM CONTRACT header. Without it `ARM_FAKE_PS_OUTPUT` is inert here.
    ARM_BEAT_SNAPSHOT_TEST_MODE: '1',
    ARM_FAKE_PS_OUTPUT: fakePsOutput,
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
  };
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

it(
  'reaps every ghost dibs/claim/admission entry on the first beat after a simulated host death, restoring full ' +
    'ceiling capacity, while never reaping a live agent, and sparing the pid-less control for \'s ' +
    'discovery-walk omission — using the same ancestry-walk-derived identity the write path actually produces',
  async () => {
    // The fixture's premise: the two synthetic claude roots must not collide
    // with the real anchors, or "the walk resolved the parent" would be
    // indistinguishable from "the walk resolved itself".
    expect(new Set([FAKE_CLAUDE_ROOT_PID, DECOY_CLAUDE_ROOT_PID, TEST_PID, TEST_PPID]).size).toBe(4);

    // === STAGE 1 — the LIVE agent's ledger record, written by the REAL =====
    // === write path under the realistic ancestry tree. ====================
    // Nothing about this record's identity is asserted into the fixture; it
    // is whatever `cli.mjs` chose to record. That is the entire point.
    await writeFile(
      coordinationFilePath,
      JSON.stringify(
        [
          {
            orchestratorId: AIMD_CEILING_RESERVED_ID,
            ceiling: LIVE_AGENT_CEILING,
            sustainedNormalCount: 0,
            declaredAt: SEEDED_AT,
          },
        ],
        null,
        2,
      ),
      'utf8',
    );

    const liveClaimBeat = runCli(
      ['--orchestrator-id=live-orch', `--claim=${LIVE_AGENT_CLAIM_TYPE}:1`],
      beatEnv,
    );
    expect(liveClaimBeat.status).toBe(0);

    const afterClaim = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
    const producedRecord = (afterClaim.find(
      (entry) => entry.orchestratorId === claimLedgerId(LIVE_AGENT_CLAIM_TYPE),
    )?.claims ?? []).find((claim) => claim.orchestratorId === 'live-orch');

    expect(producedRecord).toMatchObject({ orchestratorId: 'live-orch', count: 1 });

    // --- THE ASSERTION ONLY A TREE FIXTURE CAN MAKE ----------------------
    // The invariant: the identity a ledger write records is the one the
    // ancestry walk derived from this beat's snapshot — the nearest
    // claude-rooted ancestor of the writing process, never the writer's own
    // pid and never the first claude-rooted row in the table.
    expect(producedRecord.pid).toBe(FAKE_CLAUDE_ROOT_PID);
    expect(producedRecord.pidStartedAt).toBe(EXPECTED_PID_STARTED_AT);
    // …and, said the other way round, it is NOT self-referential and NOT a
    // "first claude-rooted process I could find" shortcut.
    expect(producedRecord.pid).not.toBe(TEST_PID);
    expect(producedRecord.pid).not.toBe(TEST_PPID);
    expect(producedRecord.pid).not.toBe(DECOY_CLAUDE_ROOT_PID);

    const producedIdentity = { pid: producedRecord.pid, pidStartedAt: producedRecord.pidStartedAt };

    // === STAGE 2 — simulate the host death. ==============================
    // `ghost-orch` died with the host: its pid is confirmed absent from this
    // machine AND from the beat's process snapshot.
    const GHOST_PID = pickConfirmedDeadPid(4_194_301);
    expectAbsentFromSnapshot(GHOST_PID);
    const GHOST_PID_STARTED_AT = T0 - 7_200_000;
    // `walk-omitted-orch` is the pid-less CONTROL for the -adjacent
    // walk-omission limitation — the sub-case that limitation does not affect:
    // pre-existing ledger shape (NO pid recorded at all) AND absent from the
    // snapshot. (A record BEARING a pid the walk omits is reaped instead; see
    // the compositions section below.) Must survive —
    // "unknown", deferred to the (not-yet-elapsed) TTL, never immediately
    // reaped. `desiredAgents: 0` so it contributes nothing to allowance
    // contention and cannot muddy the capacity assertions below.
    //
    // The whole ledger is rebuilt explicitly (rather than patched in place) so
    // the fixture is deterministic regardless of what else stage 1's beat
    // happened to write — with ONE exception: `producedRecord` is carried
    // through verbatim, because it is the one piece of state that must come
    // from the code and not from this file.
    const seededEntries = [
      {
        orchestratorId: AIMD_CEILING_RESERVED_ID,
        ceiling: LIVE_AGENT_CEILING,
        sustainedNormalCount: 0,
        declaredAt: SEEDED_AT,
      },
      // Ghost dibs entry: claims the WHOLE ceiling as intended usage, so for
      // as long as it survives, this beat's own `allowance` is squeezed to 0.
      {
        orchestratorId: 'ghost-orch',
        desiredAgents: LIVE_AGENT_CEILING,
        declaredAt: SEEDED_AT,
        firstDeclaredAt: SEEDED_AT,
        pid: GHOST_PID,
        pidStartedAt: GHOST_PID_STARTED_AT,
      },
      // The walk-omission control: no pid, not in the snapshot. Only
      // this pid-less shape survives; a pid-bearing omission is reaped.
      {
        orchestratorId: 'walk-omitted-orch',
        desiredAgents: 0,
        declaredAt: SEEDED_AT,
        firstDeclaredAt: SEEDED_AT,
      },
      // Live-agent claim ledger: one ghost record (3 slots) + the genuinely
      // live record STAGE 1's real write path produced (1 slot). Both sit
      // under the SAME ledger entry, so the sweep has to operate at record
      // granularity, not entry granularity.
      {
        orchestratorId: claimLedgerId(LIVE_AGENT_CLAIM_TYPE),
        claims: [
          {
            orchestratorId: 'ghost-orch',
            count: 3,
            claimedAt: SEEDED_AT,
            pid: GHOST_PID,
            pidStartedAt: GHOST_PID_STARTED_AT,
          },
          producedRecord,
        ],
        grantedTotal: 3 + producedRecord.count,
        declaredAt: SEEDED_AT,
      },
      // Admission ledger: a ghost hold that must also be reaped — this is the
      // second, distinct TTL surface (`DEFAULT_FRESHNESS_WINDOW_MS`) the
      //  audit insists must not be forgotten.
      {
        orchestratorId: claimLedgerId(LIVE_AGENT_ADMISSION_CLAIM_TYPE),
        claims: [
          {
            orchestratorId: 'ghost-orch',
            count: 2,
            claimedAt: SEEDED_AT,
            pid: GHOST_PID,
            pidStartedAt: GHOST_PID_STARTED_AT,
          },
        ],
        grantedTotal: 2,
        declaredAt: SEEDED_AT,
      },
    ];
    await writeFile(coordinationFilePath, JSON.stringify(seededEntries, null, 2), 'utf8');

    // === STAGE 3 — ONE beat. Not two. Not a beat plus a TTL wait. =========
    const beat = runCli(['--orchestrator-id=sweeper', '--desired-agents=3'], beatEnv);

    expect(beat.status).toBe(0);
    const parsed = JSON.parse(beat.stdout);

    const persisted = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
    const byId = (id) => persisted.find((entry) => entry.orchestratorId === id);
    const claimsOf = (type) => byId(claimLedgerId(type))?.claims ?? [];

    // --- 1. Every ghost entry is gone, after ONE beat, with no TTL wait. --
    // The liveness sweep — not age — is what removes these. `ghost-orch`'s
    // dibs entry is 60s young and so still "live" to `pruneStale`; if it is
    // gone, the sweep took it.
    expect(byId('ghost-orch')).toBeUndefined();

    // Same invariant on the live-agent ledger: the claim record is 60s old,
    // far inside `DEFAULT_CLAIM_TTL_MS` (6h), so `pruneExpiredClaims` would
    // keep it. Its absence is the sweep's doing.
    expect(claimsOf(LIVE_AGENT_CLAIM_TYPE).map((claim) => claim.orchestratorId)).not.toContain('ghost-orch');

    // And the same again for the admission ledger's ghost hold.
    expect(claimsOf(LIVE_AGENT_ADMISSION_CLAIM_TYPE).map((claim) => claim.orchestratorId)).not.toContain(
      'ghost-orch',
    );

    // --- 2. The live agent is untouched — the Secure AC. ----------------
    // Asserted against the identity THE CODE produced in stage 1, not against
    // one this file invented.
    const survivingLiveClaim = claimsOf(LIVE_AGENT_CLAIM_TYPE).find(
      (claim) => claim.orchestratorId === 'live-orch',
    );
    expect(survivingLiveClaim).toMatchObject({
      orchestratorId: 'live-orch',
      count: producedRecord.count,
      pid: producedIdentity.pid,
      pidStartedAt: producedIdentity.pidStartedAt,
    });

    // --- 3. walk-omission CONTROL: pid-less + un-enumerated => -----
    // unknown (the sub-case the limitation does not affect),
    // deferred to TTL, NOT reaped by the immediate path. Its TTL has not
    // elapsed (60s of a 15min dibs liveness window), so it must still be here.
    expect(byId('walk-omitted-orch')).toMatchObject({
      orchestratorId: 'walk-omitted-orch',
      declaredAt: SEEDED_AT,
    });

    // --- 4. The freed capacity is visible in THIS SAME invocation. -------
    // Not "the file looks clean afterwards" — the beat that did the reaping
    // must itself already see the headroom. Once `ghost-orch`'s dibs entry is
    // reaped, `sweeper` is the only contending orchestrator, so it gets the
    // full flat per-axis cap as headroom. A beat that swept only after its own
    // allowance math would report 0 here instead: the ghost dibs entry
    // (desiredAgents = 4) claims the entire cap by first-arrival priority.
    expect(parsed.type).toBe('spawn-allowed');
    expect(parsed.allowance).toBe(LIVE_AGENT_CEILING);

    // And the live-agent admission math on this same beat: ceiling 4, minus
    // the ONE surviving genuinely-live claim, minus zero surviving admission
    // holds => the full ask of 3 is granted. Without the sweep landing first,
    // the ghosts consume the whole ceiling, no admission is reserved, and this
    // is `null`.
    expect(parsed.liveAgentGrant).toBe(3);

    // --- 5. The reap is reported, not silent (Functional AC 4). ----------
    // Loose on wording, strict on substance: the reaped target must be named
    // so it shows up in an end-of-run report.
    expect(beat.stderr).toMatch(/ghost-orch/);
  },
  30_000,
);

// ===========================================================================
// COMPOSITIONS — what only an end-to-end beat can show.
// ===========================================================================
// The three tests below are compositions, not restatements. Each mechanism
// they touch is already proven in isolation elsewhere: the classifier's four
// verdicts in ./lib/reconciliation.jest.spec.mjs, and the beat's wiring of
// them (ordering, locking, logging, degradation) in
// ./ledger-reconciliation-beat-wiring.jest.spec.mjs. What no unit or wiring
// test can show is whether two mechanisms that reach the same record COMPOSE
// — whether the immediate liveness sweep and the pre-existing TTL path each
// still govern the records they are meant to, or whether one silently
// overrides the other on a real beat. That is what these assert, and it is
// why they are phrased as whole-beat runs rather than as classifier calls.
//
// THE TWO RESIDUAL LIMITATIONS ARE KEPT APART HERE ON PURPOSE.
// `classifyLedgerEntryLiveness`' own header (./lib/reconciliation.mjs) names
// them as limitation ONE and limitation TWO and forbids merging them. They
// are two different failure modes and they must not collapse into one in the
// test suite either:
//
//   ONE — DISCOVERY-WALK OMISSION. A live agent on THIS host, inside the
//   sweep's OWN PID namespace, that `listAgentProcesses`' walk fails to
//   enumerate (the bug class hardened against). The sweep's `ps` can
//   see the process; the walk simply misses it. Hardening the walk closes
//   this — it is a defect with a fix, and the fix lives in the walk.
//
//   TWO — PID-NAMESPACE BOUNDARY. A pid recorded inside a namespace distinct
//   from the sweep's (a container, say) that the sweep's `ps` cannot see
//   across AT ALL. No amount of walk hardening reaches it, because there is
//   nothing on the sweep's side of the boundary to enumerate. It is
//   structurally unclosable from here, and the verdict it produces is a
//   CONFIDENT wrong answer rather than an ambiguous one — which is the part
//   worth naming.
//
// WHAT THE TWO TESTS BELOW ACTUALLY CONTRAST. They are separated by name and
// by fixture, and each asserts an OUTCOME the other does not: the pid-less
// CONTROL for limitation one — the sub-case `reconciliation.mjs`' header calls
// UNAFFECTED by a walk omission, since there is no recorded identity to
// correlate — SURVIVES the beat and is left to its TTL, whereas the namespace
// case's record is REAPED by the beat.
//
// That divergence is between the CONTROL and limitation two, not between the
// two limitations. Limitation one and limitation two share an OUTCOME
// (`confirmed-dead`, reason `pid-absent`); what separates them is closability,
// not fate — one is a walk defect on the sweep's own host that hardening the
// walk closes, the other a boundary `ps` cannot cross at all. Limitation one
// proper is therefore a LIVE-AGENT REAP, not a benign deferral, and reading
// the control as if it were limitation one would make that reap path look
// already handled. Limitation one is pinned at unit level in
// ./lib/reconciliation.jest.spec.mjs ("reads a pid-bearing entry the walk omits as confirmed-dead") and is not composable in a whole-beat
// test, because no fixture can make the walk omit a row it was handed. An ADR
// recording both is planned and does not exist yet.

/** The AIMD ceiling row, so `LIVE_AGENT_CEILING` is deterministic per fixture. */
function ceilingEntry() {
  return {
    orchestratorId: AIMD_CEILING_RESERVED_ID,
    ceiling: LIVE_AGENT_CEILING,
    sustainedNormalCount: 0,
    declaredAt: SEEDED_AT,
  };
}

function claimRecord(orchestratorId, count, identity) {
  return { orchestratorId, count, claimedAt: SEEDED_AT, ...identity };
}

function claimLedger(type, claims) {
  return {
    orchestratorId: claimLedgerId(type),
    claims,
    grantedTotal: claims.reduce((sum, claim) => sum + claim.count, 0),
    declaredAt: SEEDED_AT,
  };
}

async function seedLedger(entries) {
  await writeFile(coordinationFilePath, JSON.stringify(entries, null, 2), 'utf8');
}

async function readLedger() {
  const persisted = JSON.parse(await readFile(coordinationFilePath, 'utf8'));
  return {
    byId: (id) => persisted.find((entry) => entry.orchestratorId === id),
    claimsOf: (type) =>
      persisted.find((entry) => entry.orchestratorId === claimLedgerId(type))?.claims ?? [],
  };
}

/**
 * Runs one real beat under the ancestry fixture, driving the whole write path.
 * Every seam comes from `beatEnv`, so a caller overriding one (the clock, say)
 * never has to restate the rest.
 */
function beat(args, extraEnv = {}) {
  return runCli(args, { ...beatEnv, ...extraEnv });
}

/**
 * `DEFAULT_LIVENESS_THRESHOLD_MS` (cli.mjs) — the dibs ledger's own,
 * pre-existing age-out window. Restated as a literal for the same reason every
 * other constant in this file is: `cli.mjs` runs `main()` at module scope and
 * cannot be imported from a spec.
 */
const DIBS_LIVENESS_THRESHOLD_MS = 15 * 60 * 1000;

it(
  'never-reap-live regression (the Secure AC): a genuinely live record survives the sweep AND keeps ' +
    'consuming its slot in that same beat\'s own capacity math',
  async () => {
    // THE SECURE AC'S REGRESSION TEST, findable by name. The ticket's hard
    // invariant is "never reap a live agent", and this is the case a reader
    // looking for it should land on.
    //
    // What it adds over the wiring spec's own survival test is the SECOND
    // half of the invariant. Survival on disk is not the whole promise: a
    // beat that leaves the record in the file but drops it from its own
    // capacity arithmetic has still, functionally, reaped it — it hands out
    // the live agent's slot to somebody else in the very same breath, which
    // is the over-admission this ticket exists to prevent. So the ledger and
    // the beat's report are asserted TOGETHER, and the report is asserted to
    // be SMALLER than the ceiling, which no "sweep everything" mutant can
    // satisfy.
    await seedLedger([ceilingEntry()]);

    // The live record's identity is whatever a real earlier beat chose to
    // write — never a pid this file invented. (Standing rule, see header.)
    const liveClaimBeat = beat(['--orchestrator-id=live-orch', `--claim=${LIVE_AGENT_CLAIM_TYPE}:2`]);
    expect(liveClaimBeat.status).toBe(0);

    const produced = (await readLedger())
      .claimsOf(LIVE_AGENT_CLAIM_TYPE)
      .find((claim) => claim.orchestratorId === 'live-orch');
    expect(produced).toMatchObject({ orchestratorId: 'live-orch', count: 2 });

    const GHOST_PID = pickConfirmedDeadPid(4_194_299);
    expectAbsentFromSnapshot(GHOST_PID);
    await seedLedger([
      ceilingEntry(),
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [
        claimRecord('ghost-orch', 2, { pid: GHOST_PID, pidStartedAt: T0 - 7_200_000 }),
        produced,
      ]),
    ]);

    const sweepBeat = beat(['--orchestrator-id=sweeper', '--desired-agents=3']);
    expect(sweepBeat.status).toBe(0);

    const afterSweep = await readLedger();
    const survivors = afterSweep
      .claimsOf(LIVE_AGENT_CLAIM_TYPE)
      .filter((claim) => claim.orchestratorId === 'live-orch');

    // 1. On disk: intact and byte-identical, not rewritten or re-identified.
    expect(survivors).toHaveLength(1);
    expect(survivors[0]).toEqual(produced);
    // The ghost beside it went, so this beat's sweep demonstrably ran at all
    // — survival alone would also be satisfied by a beat that swept nothing.
    expect(afterSweep.claimsOf(LIVE_AGENT_CLAIM_TYPE).map((claim) => claim.orchestratorId)).not.toContain(
      'ghost-orch',
    );

    // 2. In the same beat's own arithmetic: ceiling 4 minus the two slots the
    // live record still legitimately holds leaves 2, so an ask of 3 is
    // granted 2. A beat that freed the live agent's slots along with the
    // ghost's would report 3 here while the file above still looked correct.
    expect(JSON.parse(sweepBeat.stdout).liveAgentGrant).toBe(2);

    // 3. Nothing claimed it was reaped, either.
    expect(sweepBeat.stderr).not.toMatch(/live-orch/);

    // 4. And it is not a one-beat reprieve: a second beat, reading the file
    // the first one wrote, must reach the same verdict. A sweep that decayed
    // liveness across beats would reap on the second pass instead of the
    // first, turning this ticket's guarantee into a delay.
    const secondBeat = beat(['--orchestrator-id=second-sweeper', '--desired-agents=1']);
    expect(secondBeat.status).toBe(0);
    expect(
      (await readLedger()).claimsOf(LIVE_AGENT_CLAIM_TYPE).find((claim) => claim.orchestratorId === 'live-orch'),
    ).toEqual(produced);
  },
  30_000,
);

it(
  'walk-omission CONTROL (the pid-less sub-case residual limitation one does not affect), composed ' +
    'end-to-end: a pid-less record the process snapshot also omits is left to its TTL by the sweep, ' +
    'and is then reaped by that TTL alone',
  async () => {
    // Composition, not restatement. The unit spec proves the VERDICT for this
    // record is `unknown`/`no-pid`; what only a beat can show is that the
    // verdict then hands the record over to the pre-existing TTL path INTACT
    // — that the immediate sweep and the age-out path govern disjoint sets of
    // records rather than one quietly swallowing the other's work.
    //
    // Both halves are asserted, because either alone is weak:
    //   * "the sweep leaves it" alone is satisfied by a build where nothing
    //     ever reaps this record — a leak, not a safety net;
    //   * "the TTL takes it" alone is satisfied by a build where the sweep
    //     had already taken it immediately.
    // Together they pin the record to the TTL path and nowhere else.
    //
    // WHAT THIS RECORD IS: the pid-less CONTROL for residual limitation one —
    // the sub-case `classifyLedgerEntryLiveness`' own header calls UNAFFECTED
    // by a discovery-walk omission. With no recorded identity there is nothing
    // to correlate, so an omission changes nothing and the record ages out
    // under its ledger's own TTL. That is the behaviour asserted below.
    //
    // WHAT IT IS NOT: limitation one proper. That is a record BEARING a pid
    // which the walk omits, and it is reaped `confirmed-dead` — the same fate
    // limitation two's record takes in the test below, and a live-agent reap
    // rather than a benign deferral. It is pinned at unit level in
    // ./lib/reconciliation.jest.spec.mjs ("reads a pid-bearing entry the walk omits as confirmed-dead") and is deliberately not composed
    // end-to-end here: no fixture can make the walk omit a row it was handed,
    // so a whole-beat test could only restate the classifier's verdict.
    // Nor is this the namespace boundary; see the test below.
    const GHOST_PID = pickConfirmedDeadPid(4_194_297);
    expectAbsentFromSnapshot(GHOST_PID);

    await seedLedger([
      ceilingEntry(),
      // The walk-omission CONTROL record: PRE- SHAPE, no `pid` at all, and
      // absent from the snapshot. `desiredAgents: 0` so it cannot contend for
      // allowance and muddy anything.
      {
        orchestratorId: 'walk-omitted-orch',
        desiredAgents: 0,
        declaredAt: SEEDED_AT,
        firstDeclaredAt: SEEDED_AT,
      },
      // A pid-bearing ghost beside it, so the immediate path is demonstrably
      // ARMED on this same beat. Without it, "walk-omitted-orch survived"
      // would also be true of a beat whose sweep never ran.
      {
        orchestratorId: 'ghost-orch',
        desiredAgents: 1,
        declaredAt: SEEDED_AT,
        firstDeclaredAt: SEEDED_AT,
        pid: GHOST_PID,
        pidStartedAt: T0 - 7_200_000,
      },
    ]);

    // --- The sweep's beat: 60s of a 15min dibs window has elapsed, so the
    // TTL cannot yet be what decides anything here.
    const sweepBeat = beat(['--orchestrator-id=sweeper', '--desired-agents=1']);
    expect(sweepBeat.status).toBe(0);

    const afterSweep = await readLedger();
    expect(afterSweep.byId('ghost-orch')).toBeUndefined();
    expect(afterSweep.byId('walk-omitted-orch')).toMatchObject({
      orchestratorId: 'walk-omitted-orch',
      declaredAt: SEEDED_AT,
    });
    // The sweep did not merely spare it — it never claimed it at all. A reap
    // line naming this record would mean the immediate path had an opinion
    // about a record it has no evidence for.
    expect(sweepBeat.stderr).not.toMatch(/walk-omitted-orch/);
    expect(sweepBeat.stderr).toMatch(/ghost-orch/);

    // --- The same record, one TTL later. Only the clock moves: the same
    // ledger, the same snapshot, the same beat shape. It ages out under the
    // dibs ledger's own pre-existing liveness threshold, exactly as it would
    // have before this ticket existed.
    const agedBeat = beat(['--orchestrator-id=sweeper', '--desired-agents=1'], {
      // `ARM_COORDINATION_FILE` is restated beside the clock override rather
      // than left to `beat`'s spread of `beatEnv`, which already carries it.
      // The pairing invariant (./env-seam-premise.jest.spec.mjs) is that
      // a fake clock never travels without the temp coordination file that
      // scopes it — a fake `now` reaching a REAL shared ledger is what makes
      // one stray env var reap another orchestrator's live entries.
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_NOW_MS: String(SEEDED_AT + DIBS_LIVENESS_THRESHOLD_MS + 1_000),
      ARM_BEAT_NOW_TEST_MODE: '1',
    });
    expect(agedBeat.status).toBe(0);
    expect((await readLedger()).byId('walk-omitted-orch')).toBeUndefined();
  },
  30_000,
);

it(
  'RESIDUAL LIMITATION TWO (PID-namespace boundary), composed end-to-end: a record whose pid the ' +
    'sweep\'s snapshot cannot resolve at all is reaped immediately — the documented, accepted behaviour',
  async () => {
    // DISTINCT FROM THE TEST ABOVE, and deliberately opposite in outcome.
    // There, the pid-LESS control record survives the sweep and waits out its
    // TTL. Here a pid-BEARING record is reaped on the spot, with a death
    // certificate — `pid-absent`, the same reason token a genuinely exited
    // process earns. The divergence is between the control and this case: a
    // record the sweep declines to judge versus a record it judges
    // CONFIDENTLY and, for a namespace-local process, WRONGLY.
    //
    // Limitation one proper lands on the same side as this one: a pid-bearing
    // record the walk omits is likewise reaped `pid-absent`. The two
    // limitations differ in CLOSABILITY, not in fate — hardening the walk
    // closes limitation one, and nothing closes this one from here.
    //
    // This test asserts the documented behaviour; it does not assert the
    // limitation is fixed. Fixing it is out of scope for this ticket and is
    // not reachable from here at all — the sweep's `ps` cannot see across a
    // PID-namespace boundary, so no hardening of the discovery walk (which is
    // what would close limitation one) touches this case.
    //
    // Pid 17 stands for a container-local pid: a real, running process inside
    // its own namespace, and structurally unresolvable from the host snapshot
    // the sweep runs against. The fixture's premise is asserted rather than
    // assumed — if the snapshot did carry a row for it, this would silently
    // become an ordinary "alive" case.
    const NAMESPACE_LOCAL_PID = 17;
    expect(fakePsOutput).not.toMatch(new RegExp(`(^|\\s)${NAMESPACE_LOCAL_PID}(\\s|$)`, 'm'));

    await seedLedger([
      ceilingEntry(),
      {
        orchestratorId: 'container-orch',
        desiredAgents: 1,
        declaredAt: SEEDED_AT,
        firstDeclaredAt: SEEDED_AT,
        pid: NAMESPACE_LOCAL_PID,
        pidStartedAt: T0 - 3_600_000,
      },
      claimLedger(LIVE_AGENT_CLAIM_TYPE, [
        claimRecord('container-orch', 2, { pid: NAMESPACE_LOCAL_PID, pidStartedAt: T0 - 3_600_000 }),
      ]),
      // The pid-less control record again, in the SAME run — this is what
      // makes the distinction assertable rather than merely asserted in prose.
      // Two records, both invisible to the snapshot, taking two different
      // fates because the evidence about them differs: one carries a pid to
      // judge, the other carries nothing.
      {
        orchestratorId: 'walk-omitted-orch',
        desiredAgents: 0,
        declaredAt: SEEDED_AT,
        firstDeclaredAt: SEEDED_AT,
      },
    ]);

    const sweepBeat = beat(['--orchestrator-id=sweeper', '--desired-agents=1']);
    expect(sweepBeat.status).toBe(0);

    const afterSweep = await readLedger();

    // Reaped on the first beat, across BOTH ledgers it appears in, and while
    // 60s of a 15min/6h TTL had elapsed — so age is not what removed it.
    expect(afterSweep.byId('container-orch')).toBeUndefined();
    expect(afterSweep.claimsOf(LIVE_AGENT_CLAIM_TYPE).map((claim) => claim.orchestratorId)).not.toContain(
      'container-orch',
    );

    // With the reason the classifier's own doc comment says this case earns:
    // `pid-absent`, indistinguishable from a genuinely exited process. The
    // machine token is asserted rather than prose, so the two verdicts stay
    // distinguishable to whoever triages the log.
    expect(sweepBeat.stderr).toMatch(/container-orch/);
    expect(sweepBeat.stderr).toContain(LIVENESS_REASON.PID_ABSENT);

    // And the contrast that keeps evidence-bearing records apart from
    // evidence-free ones: the pid-less control in the same file, equally
    // absent from the snapshot, is untouched. Same beat, same snapshot,
    // different fate — because only one of them offered a pid to judge.
    expect(afterSweep.byId('walk-omitted-orch')).toMatchObject({ orchestratorId: 'walk-omitted-orch' });
  },
  30_000,
);
