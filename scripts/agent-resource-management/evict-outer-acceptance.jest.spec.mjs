// Phase 0 outer acceptance test ("EVICT with no lost work —
// checkpoint-and-exit + re-enqueue + resume via re-entrant skills").
//
// Outer acceptance test (per the ## Build Plan comment posted on):
//
//   it('evicts the highest-phys_footprint own-spawned agent, re-enqueues its
//   work item with class/command intact, and only escalates to SIGKILL if
//   the process is still alive after the grace period', ...)
//   Layer: integration — black-box via child_process spawning of cli.mjs
//   (matching every sibling cli.*.jest.spec.mjs in this skill), with
//   injected fake kill/process-tree fixtures rather than real OS process
//   signalling.
//   Green when: Phase 1 (phys_footprint victim selection) + Phase 2
//   (own-spawned-only authorization) + Phase 3 (graceful-stop mechanism) +
//   Phase 4 (EVICT wiring + re-enqueue) all pass their gates.
//
// RED by construction, right now: none of Phases 1-4 have landed.
// `buildTrafficLight` (./lib/allowance.mjs) has NO `'evict'` literal in its
// discriminated union today — see that function's own extensive "EVICT —
// considered and deliberately excluded" JSDoc, which names
// THIS ticket as the one that must add it once a real mechanism
// exists. `selectPauseCandidate` still ranks purely by `rssMb`, not
// `phys_footprint`. `cli.mjs` has no eviction wiring at all: a RED beat with
// a running agent always returns `{ type: 'pause', ... }`, never
// `{ type: 'evict', ... }`, and never touches the durable queue
// (./lib/queue.mjs) in any way.
//
// Do not add a stub implementation to make this pass — that defeats the
// point of the outer/phase red-green loop. Phases 1-4's implementer turns
// this file green by actually building the mechanism.
//
// -----------------------------------------------------------------------
// Scope notes / seam-shape decisions (read before extending this file):
//
// 1. SAFETY — no real SIGTERM/SIGKILL against an arbitrary host PID. This
//    file spawns fake `ps`-tree text (via the ALREADY-EXISTING
//    `ARM_FAKE_PS_OUTPUT` seam cli.mjs already honours for pause-candidate
//    discovery — see cli.jest.spec.mjs's "Phase 2" describe block) so
//    every "running agent" PID in this test's fixtures (e.g. `9001`) is
//    SYNTHETIC — it does not correspond to any real process on the machine
//    running this test. Sending it a real OS signal would be unsafe (that
//    PID number could coincidentally belong to an unrelated real process on
//    the host), so this file also sets a documented, NOT-YET-HONOURED fake
//    seam, `ARM_FAKE_KILL_LOG` (a file path), on every relevant call — the
//    same idiom as `ARM_FAKE_PS_OUTPUT`/`ARM_FAKE_FOOTPRINT_OUTPUT`/
//    `ARM_FAKE_COLLECT_JSON`: when set, Phase 3's real `process.kill` calls
//    should be redirected to append `{ pid, signal }` entries to this file
//    instead of touching a real process. This is THIS TEST's own suggestion
//    for Phase 3/4's seam name/shape, not a binding contract pinned by the
//    Build Plan — if Phase 3's implementer lands a differently-shaped seam,
//    update this file's env plumbing to match it (the important invariant to
//    preserve is "this file never causes a real kill(2) syscall against a
//    host PID", not the exact env var name). Because that seam does not
//    exist yet, this file does NOT assert on `ARM_FAKE_KILL_LOG`'s contents
//    anywhere below — see note 3.
//
// 2. CLI SURFACE FOR TRIGGERING EVICTION — unresolved by the Investigation
//    and Build Plan (neither commits to an exact flag shape). This file
//    guesses `--evict-pid=<pid> --lease-id=<leaseId>` (mirroring the
//    existing `--ack-lease=<leaseId>`/`--release-lease=<leaseId>` naming
//    convention) as a plausible way for the calling orchestrator to tell
//    cli.mjs "the item under this lease is running as this pid; evaluate it
//    against the RED/pause path and evict if authorized." This is ALSO not a
//    binding contract — per `cli-dequeue-if-capacity.jest.spec.mjs`'s own
//    precedent ("none of these three flags exist in cli.mjs yet... falls all
//    the way through to the bottom of main() and runs the default beat
//    instead"), an unrecognized flag is silently absorbed by `parseArgs` and
//    ignored (confirmed by reading `parseArgs`/`main()` in cli.mjs directly:
//    there is no unknown-flag rejection anywhere in this file), so passing
//    these two guessed flags today is a safe no-op, not a crash — the
//    default RED/pause beat runs exactly as it does today, which is
//    precisely the RED behaviour this file needs to observe and then flip to
//    GREEN once Phase 4 lands. If Phase 4's implementer picks a different
//    CLI shape, update the `EVICT_FLAGS` helper below to match it — the
//    assertions that matter are the OBSERVABLE OUTCOMEs (traffic-light
//    `type`, and the re-enqueued item's shape), not these exact flag names.
//
// 3. SIGTERM -> grace period -> SIGKILL sequencing (Phase 3's core
//    mechanism) is deliberately NOT asserted here with a precise seam
//    contract. Per this ticket's own build-out instructions: "If you are not
//    confident enough in the exact seam shape to assert on it precisely, ...
//    leave the SIGTERM/SIGKILL-sequencing assertion as a test.todo(...)."
//    Phase 3 doesn't exist yet, so its exact injected-clock/injected-kill
//    signature is unknown — inventing one here risks pinning Phase 3's
//    implementer to the wrong shape. See `test.todo(...)` at the bottom of
//    this file.
//
// 4. PID -> QUEUE-ITEM ASSOCIATION is itself an open design question: no
//    existing data structure in this skill maps "a live agent-root PID" back
//    to "the queue item/lease it is running." This file resolves it, for
//    test-construction purposes only, by dequeuing the item first (getting a
//    real `leaseId` via the ALREADY-EXISTING `--dequeue-if-capacity` beat)
//    and threading that `leaseId` through the guessed `--lease-id=` flag
//    above (see note 2) — Phase 4's implementer is free to resolve this
//    association differently as long as the observable outcome (the item
//    reappears in the queue with its class/command/orchestratorId intact)
//    still holds.
// -----------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

import { selectPauseCandidate } from './lib/allowance.mjs';
// — this file's own real-CLI-beat `declareDibs` idiom never resolves a
// `/claude`-rooted ancestor inside Jest, so the dibs entry it seeds carries
// no `pid`. `performEviction`'s fail-closed orchestrator-pid-binding check
// (see cli-evict.jest.spec.mjs's own `declareDibsWithPid` for the identical
// idiom) requires a genuinely-recorded, matching pid, so this file seeds one
// directly via this lib-level primitive rather than through a spawned beat.
import { declareDibs } from './lib/coordination-file.mjs';

const CLI = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };
// Mirrors cli.jest.spec.mjs's own MEMORY_RED fixture exactly.
const MEMORY_RED = { pressureLevel: 1, swapUsedMb: 12288, compressedMb: 4096 };
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512, freeRamMb: 8192 };

// Single claude-rooted agent tree, PID 9001 — mirrors cli.jest.spec.mjs's
// `PS_ONE_AGENT` fixture exactly (see that file's "Phase 2" describe
// block for the ps -Ao pid,ppid,rss,etime,comm grammar this represents).
// PID 9001 here is SYNTHETIC (see note 1 above) — never a real process on
// the host running this test.
const PS_ONE_AGENT = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
 9001   501 102400    01:00:00 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001  51200       58:20 node /Applications/Claude.app/Contents/Resources/app/cli.js
`;

/** Runs the real cli.mjs as a real child process — never imports it directly (matches every sibling cli.*.jest.spec.mjs). */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

let workDir;
let queueFilePath;
let coordinationFilePath;
let fakeKillLogPath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-evict-outer-'));
  queueFilePath = join(workDir, 'queue.json');
  coordinationFilePath = join(workDir, 'coordination.json');
  fakeKillLogPath = join(workDir, 'fake-kill-log.json'); // see note 1 above
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

function baseEnv(extra = {}) {
  return {
    ARM_QUEUE_FILE: queueFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    // round-4 review — the master `ARM_EVICT_TEST_MODE=1` switch
    // (cli.mjs) must be set before `handleEvict` honors ANY `ARM_FAKE_*`
    // seam at all; see cli-evict.jest.spec.mjs's own baseEnv for the full
    // rationale. This file's --evict-pid call relies on the fake seams
    // below, so this must be present.
    ARM_EVICT_TEST_MODE: '1',
    ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
    ARM_FAKE_PS_OUTPUT: PS_ONE_AGENT,
    // Not honoured by any real code yet (see note 1) — set defensively so
    // this file stays a safe no-op-on-real-processes invocation even after
    // Phase 3 lands, IF Phase 3's implementer adopts this suggested name.
    ARM_FAKE_KILL_LOG: fakeKillLogPath,
    // round-3 review, Medium — handleEvict's fake-seam coupling check
    // (cli.mjs) now requires every ARM_FAKE_* seam it reads to be set
    // together whenever any one of them is set. This file's --evict-pid
    // call always carries ARM_FAKE_PS_OUTPUT/ARM_FAKE_KILL_LOG above, so
    // these must accompany them too — see cli-evict.jest.spec.mjs's own
    // baseEnv for the identical addition and rationale.
    ARM_FAKE_IS_ALIVE_SEQUENCE: 'false',
    ARM_FAKE_KILL_RESULT: '',
    // round-4 review — ARM_FAKE_NOW_MS joined the coupled seam set
    // (cli.mjs's EVICT_FAKE_SEAM_VARS); computed fresh per call so it
    // tracks the real clock closely, same rationale as
    // cli-evict.jest.spec.mjs's own baseEnv.
    ARM_FAKE_NOW_MS: String(Date.now()),
    // Phase 2 — resolveNow() now also requires ARM_BEAT_NOW_TEST_MODE
    // before honouring ARM_FAKE_NOW_MS; without it that fake-time intent
    // silently does nothing.
    ARM_BEAT_NOW_TEST_MODE: '1',
    // review, Medium — ARM_FAKE_FOOTPRINT_OUTPUT joined the coupled
    // seam set (cli.mjs's EVICT_FAKE_SEAM_VARS); see cli-evict.jest.spec.mjs's
    // own baseEnv for the identical addition and rationale.
    ARM_FAKE_FOOTPRINT_OUTPUT: 'phys_footprint: 100 MB',
    ...extra,
  };
}

// ---------------------------------------------------------------------------
// Test 1 — Phase 1 (AC #5): victim selection must rank by phys_footprint,
// not ps RSS. Pure, safe, unit-level (no process spawning at all) — directly
// exercises the real, currently-shipped `selectPauseCandidate`.
//
// Phase 1 landed and settled the field name as `physFootprintMb` (see
// lib/allowance.jest.spec.mjs's `selectPauseCandidate — 'highest-phys-footprint'
// policy` describe block) — updated from this test's original
// placeholder guess (`footprintMb`) now that the real contract exists. The
// OBSERVABLE requirement this test pins is unchanged: given a candidate with
// lower RSS but higher phys_footprint than another, the higher-footprint one
// must win.
// ---------------------------------------------------------------------------

it('Phase 1: selectPauseCandidate ranks by phys_footprint, not RSS, when footprint data is available', () => {
  const candidates = [
    {
      agentId: 'agent-high-rss-low-footprint',
      rssMb: 220,
      physFootprintMb: 90, // lower real footprint despite higher RSS
      startedAt: 1_700_000_000_000,
    },
    {
      agentId: 'agent-low-rss-high-footprint',
      rssMb: 150,
      physFootprintMb: 260, // the genuinely bigger memory consumer
      startedAt: 1_700_000_100_000,
    },
  ];

  const candidate = selectPauseCandidate(candidates, { policy: 'highest-rss' });

  // GREEN since Phase 1: selectPauseCandidate ranks by physFootprintMb when
  // present, so the genuinely-bigger consumer wins despite lower RSS.
  expect(candidate.agentId).toBe('agent-low-rss-high-footprint');
});

// ---------------------------------------------------------------------------
// Test 2 — the outer acceptance test itself (Phases 1+2+4 composed):
// end-to-end via the real cli.mjs, asserting only the OBSERVABLE OUTCOME
// (per this ticket's own guidance, since Phase 3/4's exact CLI/kill seam
// shape isn't fixed yet — see notes 1, 2, 4 above): a RED beat with a single
// running, presumed-own-spawned claude-rooted agent must (a) report
// `type: 'evict'` instead of today's `type: 'pause'`, and (b) cause the
// in-flight work item to reappear in the durable queue with its
// agentClass/commandRef/orchestratorId intact and no leaseId (i.e. released
// back to the pool, not still "checked out").
// ---------------------------------------------------------------------------

it(
  'evicts the highest-phys_footprint own-spawned agent and re-enqueues its work item with ' +
    'class/command/orchestratorId intact (outer acceptance test for)',
  async () => {
    const ORCHESTRATOR_ID = 'orch-evict-outer';
    const AGENT_CLASS = 'typescript-implementer';
    const COMMAND_REF = 'resumable-workflow --resume=1071-phase-3';

    // Declare dibs for this orchestrator via a normal, real beat FIRST —
    // `handleEvict`'s `orchestratorIsLive` check (whole-branch review,
    // Medium #3 fix) requires a dibs entry that already existed before the
    // eviction attempt's own internal declaration, mirroring how a genuine
    // orchestrator establishes liveness in production over several real
    // beats before ever attempting an eviction.
    const declareResult = runCli(
      [`--orchestrator-id=${ORCHESTRATOR_ID}`, '--desired-agents=0'],
      baseEnv({ ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }), ARM_FAKE_PS_OUTPUT: undefined }),
    );
    expect(declareResult.status).toBe(0);

    // — a genuine PRIOR beat must have recorded this orchestrator's own
    // pid on the ledger, matching the --orchestrator-pid supplied below, or
    // the fail-closed binding check would now reject before this scenario
    // ever reaches its own authorization/kill machinery.
    const now = Date.now();
    await declareDibs(coordinationFilePath, {
      orchestratorId: ORCHESTRATOR_ID,
      desiredAgents: 0,
      declaredAt: now,
      firstDeclaredAt: now,
      pid: 501,
    });

    // Seed one real queue item and dequeue it (real, already-shipped
    // ./lib/queue.mjs + --dequeue-if-capacity machinery) to model "this item
    // is currently running" — see note 4 above for why this is the least
    // presumptuous way to construct the pid<->item association this test
    // needs.
    const enqueueResult = runCli(
      [`--enqueue=${AGENT_CLASS}:normal:${COMMAND_REF}`, `--orchestrator-id=${ORCHESTRATOR_ID}`],
      baseEnv({ ARM_FAKE_PS_OUTPUT: undefined, ARM_FAKE_COLLECT_JSON: undefined }),
    );
    expect(enqueueResult.status).toBe(0);

    const dequeueResult = runCli(
      ['--dequeue-if-capacity', `--orchestrator-id=${ORCHESTRATOR_ID}`],
      baseEnv({ ARM_FAKE_PS_OUTPUT: undefined, ARM_FAKE_COLLECT_JSON: undefined }),
    );
    expect(dequeueResult.status).toBe(0);
    const dequeued = JSON.parse(dequeueResult.stdout);
    expect(dequeued.granted).toBe(true);
    const { leaseId } = dequeued;

    // The genuine RED beat: memory RED, one running claude-rooted agent
    // (PID 9001, synthetic — see note 1), plus `--evict-pid`/`--lease-id`
    // (note 2) associating that PID with the leased item above, and
    // `--orchestrator-pid=501` — the third flag Phase 4's implementer
    // resolved as necessary (isEvictionAuthorized needs the calling
    // orchestrator's own PID; the dibs ledger has no pid field, so it must
    // be stated explicitly — see cli-evict.jest.spec.mjs's own design note
    // and PS_ONE_AGENT's fixture, where PID 501 is 9001's parent).
    const evictResult = runCli(
      [
        `--orchestrator-id=${ORCHESTRATOR_ID}`,
        '--orchestrator-pid=501',
        '--evict-pid=9001',
        `--lease-id=${leaseId}`,
      ],
      // ARM_EVICT_GRACE_MS keeps this test from genuinely waiting out
      // gracefulStop's real 60s default grace period (see
      // cli-evict.jest.spec.mjs's own design note 3 for the same override).
      // This test doesn't assert on stopResult.outcome/signalsSent, only on
      // `type` and the re-enqueued item's shape, so the exact
      // ARM_FAKE_IS_ALIVE_SEQUENCE: 'false' value baseEnv() sets above (the
      // isAlive probe reports "not alive" after SIGTERM, so gracefulStop
      // resolves 'exited-before-grace' without ever escalating to SIGKILL)
      // is immaterial to what this test asserts on.
      baseEnv({ ARM_EVICT_GRACE_MS: '5' }),
    );
    expect(evictResult.status).toBe(0);
    const evictBody = JSON.parse(evictResult.stdout);

    // EVICT is wired end-to-end: a genuine RED beat with an
    // authorized, confirmed-stopped candidate reports `{ type: 'evict', ... }`.
    expect(evictBody.type).toBe('evict');

    // Observable outcome: the item is back in the durable queue, released
    // (no leaseId — matches ./cli-dispatcher-drain.jest.spec.mjs's own
    // "entry carrying no leaseId" idiom for an unclaimed entry), with its
    // agentClass/commandRef/orchestratorId untouched. `handleEvict` DOES
    // touch the queue file on a confirmed-stopped eviction outcome — via
    // `releaseQueueEntry`, which clears the lease in place rather than
    // re-enqueueing a fresh entry (see cli-evict.jest.spec.mjs's design
    // note 5) — so the previously-dequeued item above is given back here.
    const peekResult = runCli(
      ['--peek', `--orchestrator-id=${ORCHESTRATOR_ID}`],
      baseEnv({ ARM_FAKE_PS_OUTPUT: undefined, ARM_FAKE_COLLECT_JSON: undefined }),
    );
    expect(peekResult.status).toBe(0);
    const remaining = JSON.parse(peekResult.stdout).items;

    expect(remaining).toHaveLength(1);
    expect(remaining[0]).toMatchObject({
      agentClass: AGENT_CLASS,
      commandRef: COMMAND_REF,
      orchestratorId: ORCHESTRATOR_ID,
    });
    expect(remaining[0].leaseId).toBeUndefined();
  },
);

// ---------------------------------------------------------------------------
// Test 3 (documentation-as-scope, not a red assertion) — Phase 4's own
// documented edge case: eviction must fall through to today's pause/hold
// behaviour, never fabricate an eviction, when no authorized candidate
// exists. This assertion is ALREADY true of today's real code (a RED beat
// with zero running agents already reports `pause`/null, never a
// fabricated anything) — kept here as a regression guard for Phase 4, not
// as this file's RED reason.
// ---------------------------------------------------------------------------

it('falls through to the existing pause/hold behaviour (never fabricates an eviction) when no running agent exists', () => {
  const PS_IDLE_NO_AGENTS = `  PID  PPID    RSS     ELAPSED COMM
    1     0    512 1-03:46:39 /sbin/launchd
  501     1   1024 1-03:30:00 /usr/libexec/loginwindow
`;

  const result = runCli(
    ['--orchestrator-id=orch-evict-outer-idle'],
    baseEnv({ ARM_FAKE_PS_OUTPUT: PS_IDLE_NO_AGENTS }),
  );
  expect(result.status).toBe(0);
  const body = JSON.parse(result.stdout);

  expect(body.type).not.toBe('evict');
  expect(['pause', 'hold']).toContain(body.type);
});

// ---------------------------------------------------------------------------
// Phase 3's own core mechanism — SIGTERM immediately, SIGKILL only after the
// configured grace period AND only if the process is confirmed still alive.
// Deliberately left as test.todo (see note 3 above): Phase 3 doesn't exist
// yet, so this file does not invent its injected-clock/injected-kill
// function signature. Phase 3's implementer should replace this with real
// assertions against whatever pure, injectable module they land (mirroring
// `lib/allowance.mjs`'s existing token-bucket pattern: injected `now`, no
// real `setTimeout`/`Date.now()` in the tested unit).
// ---------------------------------------------------------------------------

test.todo(
  'sends SIGTERM immediately, waits the full configured grace period (default 60_000ms), and sends ' +
    'SIGKILL only if the process is still alive after it — never before, and never at all if the ' +
    'process exited cleanly during the grace period',
);

test.todo(
  'ESRCH from the kill syscall (process already gone) is treated as "already evicted", not a failure ' +
    '— eviction still proceeds to re-enqueue',
);

// ---------------------------------------------------------------------------
// Phase 0 (outer acceptance) — same-orchestrator pid/lease
// pairing is not verified today, per this ticket's own Build Plan
// (superseding "## Build Plan" revision 2). This test exercises the FULL,
// real-world production sequence end-to-end via real `cli.mjs` invocations
// (never importing it directly, matching every test in this file):
// `--claim` (via `--dequeue-if-capacity`, the already-shipped claim
// machinery this file's own outer acceptance test above already uses) ->
// an externally-simulated spawn (this file never spawns a real agent
// process; PID 9001 is synthetic, exactly like every other fixture in this
// file — see note 1 above) -> the NEW `--bind-pid=<pid> --lease-id=<leaseId>`
// CLI flag -> `--evict-pid`.
//
// RED BY CONSTRUCTION, right now: `cli.mjs` has no `--bind-pid` flag at all
// today (confirmed: `grep -n "bind-pid" cli.mjs` matches nothing). Per this
// file's own established "unrecognized flag is silently absorbed by
// parseArgs" precedent (see note 2 above and
// ./cli-dequeue-if-capacity.jest.spec.mjs's identical RED-by-construction
// framing), a `--bind-pid=...` invocation today either falls through to the
// nearest recognized beat shape or is rejected by whatever standalone-flag
// conflict-checking `--evict-pid`'s own wiring (cli-evict.jest.spec.mjs
// Scenario 6/6b) already applies to unrecognized combinations — in neither
// case does it actually bind a pid to a lease, so the MISMATCH half of this
// test (which relies on that binding having genuinely happened) is red for
// the same underlying reason cli-evict.jest.spec.mjs's own mismatch case is:
// nothing today reads or enforces a `pid` on a leased queue entry, so a
// mismatched evict-pid is never rejected. This assertion is written against
// the flag's DOCUMENTED FUTURE shape (Build Plan Phase 3): if
// `--bind-pid`/`--lease-id` land under different flag names, update the
// `bindPidToLease` helper below to match — the assertion that matters is the
// OBSERVABLE OUTCOME (a mismatched pid is refused, a matching one succeeds),
// not these exact flag names.
// ---------------------------------------------------------------------------

// Restored from git history (Phase 2's commit `28869fbb` converted this to
// `test.todo` because `--bind-pid` did not exist yet in that worktree —
// this is Phase 3, the phase that lands it). RED by construction right
// now: `cli.mjs` still has no `--bind-pid` flag (confirmed:
// `grep -n "bind-pid" cli.mjs` matches nothing outside comments), so the
// `--bind-pid` invocation below is silently absorbed by `parseArgs` and
// falls through to the default beat (see note 2 above), which never binds
// any pid to any lease — the MISMATCH half of this test therefore fails
// (nothing rejects the mismatched `--evict-pid` below) until Phase 3's
// production step lands the flag.
it(
  'a full claim → external spawn → bind-pid → evict-pid sequence, run end-to-end via real CLI invocations, ' +
    'closes the gap for both a matching and a mismatched pid',
  async () => {
    const ORCHESTRATOR_ID = 'orch-evict-pid-lease-binding-outer';
    const AGENT_CLASS = 'typescript-implementer';
    const COMMAND_REF = 'resumable-workflow --resume=1316-phase-3';

    // A genuine PRIOR beat must have recorded this orchestrator's own pid on
    // the ledger (the already-shipped orchestrator-pid binding), or
    // performEviction's existing fail-closed check would reject before this
    // scenario ever reaches the pid/lease-binding machinery under
    // test.
    const now = Date.now();
    await declareDibs(coordinationFilePath, {
      orchestratorId: ORCHESTRATOR_ID,
      desiredAgents: 0,
      declaredAt: now,
      firstDeclaredAt: now,
      pid: 501,
    });

    // --claim: enqueue the work item and dequeue it under a lease — the real,
    // already-shipped claim machinery every other test in this file uses.
    const enqueueResult = runCli(
      [`--enqueue=${AGENT_CLASS}:normal:${COMMAND_REF}`, `--orchestrator-id=${ORCHESTRATOR_ID}`],
      baseEnv({ ARM_FAKE_PS_OUTPUT: undefined, ARM_FAKE_COLLECT_JSON: undefined }),
    );
    expect(enqueueResult.status).toBe(0);

    const dequeueResult = runCli(
      ['--dequeue-if-capacity', `--orchestrator-id=${ORCHESTRATOR_ID}`],
      baseEnv({
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: undefined,
      }),
    );
    expect(dequeueResult.status).toBe(0);
    const dequeued = JSON.parse(dequeueResult.stdout);
    expect(dequeued.granted).toBe(true);
    const { leaseId } = dequeued;

    // External spawn is simulated (never a real process — see note 1): the
    // orchestrator now "knows" its just-spawned agent's real OS pid is 9001
    // (a genuine claude-rooted descendant of 501 per PS_ONE_AGENT).
    const SPAWNED_PID = 9001;

    // --bind-pid: the NEW Phase 3 CLI flag, calling Phase 1's
    // bindPidToLease with the calling --orchestrator-id.
    //
    // Deliberately uses the SAME fully-faked `baseEnv()` seam set every
    // other beat in this file uses (rather than disabling
    // ARM_FAKE_PS_OUTPUT/ARM_FAKE_COLLECT_JSON as the surrounding
    // --enqueue/--dequeue-if-capacity/--peek calls do) so this call's
    // behaviour is deterministic, not host-dependent: today, with no
    // `--bind-pid` flag recognized, this invocation is silently absorbed by
    // `parseArgs` and falls through to the DEFAULT beat instead (the same
    // "unrecognized flag" precedent ./cli-dequeue-if-capacity.jest.spec.mjs
    // and this file's own note 2 document) — which samples memory/ps-tree
    // and unconditionally re-declares this orchestrator's dibs entry WITHOUT
    // a resolved `pid` (this file's synthetic ancestry fixtures do not
    // include the real Jest process's own ps ancestry, so the default beat's
    // own identity resolution never matches). That default-beat side effect
    // is itself part of what makes this scenario red for the right reason:
    // it both fails to bind any pid to the lease, AND clobbers the earlier
    // `declareDibs(..., { pid: 501 })` seed this test relies on for the
    // downstream --evict-pid calls' orchestrator-pid-binding check
    // (already shipped) to keep passing.
    const bindResult = runCli(
      [
        `--orchestrator-id=${ORCHESTRATOR_ID}`,
        `--bind-pid=${SPAWNED_PID}`,
        `--lease-id=${leaseId}`,
      ],
      baseEnv(),
    );
    // Not itself the RED assertion (an unrecognized flag falling through to
    // a default beat still exits 0) — the actual gap surfaces below, at
    // --evict-pid.
    expect(bindResult.status).toBe(0);

    // --evict-pid with the SAME pid just bound: must succeed.
    const evictMatchResult = runCli(
      [
        `--orchestrator-id=${ORCHESTRATOR_ID}`,
        '--orchestrator-pid=501',
        `--evict-pid=${SPAWNED_PID}`,
        `--lease-id=${leaseId}`,
      ],
      baseEnv({ ARM_EVICT_GRACE_MS: '5' }),
    );
    expect(evictMatchResult.status).toBe(0);
    const evictMatchBody = JSON.parse(evictMatchResult.stdout);
    expect(evictMatchBody.type).toBe('evict');

    // Second lease, second bind, MISMATCHED evict-pid: must be refused, and
    // the second lease must be left untouched (never released, never
    // touched by any kill signal).
    const enqueueResult2 = runCli(
      [`--enqueue=${AGENT_CLASS}:normal:${COMMAND_REF}-2`, `--orchestrator-id=${ORCHESTRATOR_ID}`],
      baseEnv({ ARM_FAKE_PS_OUTPUT: undefined, ARM_FAKE_COLLECT_JSON: undefined }),
    );
    expect(enqueueResult2.status).toBe(0);

    const dequeueResult2 = runCli(
      ['--dequeue-if-capacity', `--orchestrator-id=${ORCHESTRATOR_ID}`],
      baseEnv({
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: undefined,
      }),
    );
    expect(dequeueResult2.status).toBe(0);
    const dequeued2 = JSON.parse(dequeueResult2.stdout);
    expect(dequeued2.granted).toBe(true);
    const leaseId2 = dequeued2.leaseId;

    const bindResult2 = runCli(
      [
        `--orchestrator-id=${ORCHESTRATOR_ID}`,
        `--bind-pid=${SPAWNED_PID}`,
        `--lease-id=${leaseId2}`,
      ],
      baseEnv(),
    );
    expect(bindResult2.status).toBe(0);

    // A DIFFERENT pid than the one bound above — the same-orchestrator
    // pid/lease pairing bug exists to catch.
    const MISMATCHED_PID = 12345;
    const evictMismatchResult = runCli(
      [
        `--orchestrator-id=${ORCHESTRATOR_ID}`,
        '--orchestrator-pid=501',
        `--evict-pid=${MISMATCHED_PID}`,
        `--lease-id=${leaseId2}`,
      ],
      baseEnv({ ARM_EVICT_GRACE_MS: '5' }),
    );
    // RED today, and for a real production reason once --bind-pid lands:
    // nothing in performEviction reads a bound pid, so a mismatched
    // --evict-pid is not refused.
    expect(evictMismatchResult.status).not.toBe(0);
    const evictMismatchBody = JSON.parse(evictMismatchResult.stdout);
    expect(evictMismatchBody.type).not.toBe('evict');
    expect(evictMismatchBody.evictionAttempted).toBe(true);

    const peekResult = runCli(
      ['--peek', `--orchestrator-id=${ORCHESTRATOR_ID}`],
      baseEnv({ ARM_FAKE_PS_OUTPUT: undefined, ARM_FAKE_COLLECT_JSON: undefined }),
    );
    expect(peekResult.status).toBe(0);
    const remaining = JSON.parse(peekResult.stdout).items;
    const secondItem = remaining.find((item) => item.leaseId === leaseId2);
    expect(secondItem).toBeDefined();
  },
);
