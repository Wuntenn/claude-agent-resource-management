---
name: agent-resource-management
description: >-
  Coordinates concurrent orchestrator agents spawning Claude Code / Node
  sub-agents on a resource-constrained host (fixed RAM, single SSD) so they
  collectively maximize completed work per hour without ever crossing the
  memory or disk ceiling. Orchestrators run `cli.mjs` at each beat (after
  each sub-agent's task completes) to declare intended usage ("dibs") and
  read back a traffic-light signal — spawn-allowed, hold, or pause a named
  agent. Does not tell orchestrators to hold back pre-emptively; it paces
  them, not blocks them. Used by orchestrator agents — not invoked directly
  by a human via `/`.
user-invocable: false
---

# Agent resource management — pace, don't block

A resource-constrained host (fixed RAM, single SSD) running concurrent
orchestrator agents can crash unpredictably from two independent failure
modes: **disk exhaustion** (agent temp files accumulate over a session until
the volume hits 0 GB free) and **memory-pressure collapse** (enough
concurrent Node/Claude-Code agents push memory pressure to critical, swap
spikes, and the OS panics). The RAM ceiling is fixed hardware — the only
levers are workload management: concurrency, scheduling, cleanup.

This skill turns that into a shared decision loop: a deterministic script
reads live machine state plus every orchestrator's declared intent from one
coordination file, and returns a **traffic light** each orchestrator consults
at each beat.

## The objective — maximize work per hour, not "avoid crashing at all costs"

The goal is **not** the most conservative concurrency that never risks a
crash. Beyond some point *more* concurrent agents make the machine slower
(paging, IO contention), not faster — there is an optimal concurrency, often
below the crash ceiling, that finishes the most real work per hour. This
skill's job is to find and hold that operating point: **maximize completed
work per hour without crossing either resource ceiling** — spawn as much as
the machine can currently sustain, not as little as possible.

## Behavioral contract — pace, don't block

**Orchestrators keep attempting their normal workload.** This skill never
tells an orchestrator "don't spawn" as a standing posture — it only answers,
at each beat, whether *this specific beat* may spawn more, should hold at
current concurrency, or should pause one specific named agent. An
orchestrator that would otherwise spawn 5 sub-agents this beat still *tries*
to spawn 5; this skill's traffic light is what paces that attempt down to
what the machine can currently sustain, not a pre-emptive cap the
orchestrator applies to itself out of caution.

**Worked example.** Orchestrator A finishes a sub-agent's task and would
normally spawn its next 2 queued sub-agents immediately. Instead it runs:

```bash
node scripts/agent-resource-management/cli.mjs \
  --session-id="$CLAUDE_CODE_SESSION_ID" --desired-agents=2
```

- **`{"type":"spawn-allowed","allowance":3}`** — both axes GREEN. Orchestrator
  A spawns its 2 queued sub-agents (it wanted 2; the allowance of 3 is
  headroom, not a target) and keeps going exactly as it would have without
  this skill.
- **`{"type":"spawn-allowed","allowance":0}`** — both axes GREEN, but the
  host-wide global spawn-rate bucket (see "Global spawn-rate limiting" below)
  denied this beat's request outright. This is a genuinely reachable,
  intentional shape, not a bug: the bucket is a binary gate — a genuine
  denial (the bucket granted `0` against a non-zero request) forces
  `cli.mjs` to zero out `allowance`; otherwise the dibs-computed `allowance`
  passes through unchanged, even when it exceeds what the bucket actually
  granted. **Orchestrators MUST check `allowance > 0` before spawning** —
  `type === 'spawn-allowed'` alone does not mean there is room to spawn even
  one agent; it only means neither the memory nor the disk axis is the
  current constraint. Treat `allowance: 0` the same as `hold` for the
  no-spawn-this-beat posture: don't spawn this beat, keep already-running
  agents undisturbed, and re-check next beat once the bucket has had time to
  refill. Unlike `hold` below, this is not itself a reason to enqueue
  anything — the bucket's own passive refill resolves it without a durable
  entry point.
- **`{"type":"hold"}`** — either axis AMBER (never RED — a RED axis always
  reports `pause`, even with zero running agents to name as a candidate; see
  the `pause`/`pauseCandidate: null` bullet below). Orchestrator A does
  **not** spawn new sub-agents this beat, but every already-running agent
  keeps running undisturbed. Rather than busy-waiting on this beat and
  re-polling next tick, A durably enqueues the deferred spawn intent — one
  `--enqueue=<agentClass>:<priority>:<commandRef>` call per deferred
  sub-agent (`lib/queue.mjs`'s `enqueueItem`, wired into `cli.mjs` as
  `--enqueue`; also requires `--orchestrator-id=<id>`, persisted onto the
  entry as its `orchestratorId` field — not merely validated and discarded —
  so a future dispatcher can attribute/filter queued work by owning
  orchestrator, and rejects (exit 2) a structurally malformed spec or an
  invalid `priority`/empty `commandRef`/invalid `orchestratorId` before any
  write) — then exits this beat, or continues whatever other work it already
  has in flight. This gives the deferred work a durable, re-discoverable
  entry point instead of a decision that only ever lived in this beat's
  stack frame. A has not been told to scale back its ambitions, only that
  THIS beat is not the one that spawns them: whoever next processes the
  queue — via `--dequeue` (same lib, atomically claims the top entry or the
  explicit `{"item":null}` empty-queue marker — which, once dequeueing is
  lease-aware, can also mean every remaining entry is currently leased by
  someone else) — picks the deferred work back up once capacity frees,
  rather than A itself looping or re-polling to wait for that moment. (A
  non-mutating `--peek` prints the live queue, ordered priority-descending
  then enqueue-time-ascending WITH aging always applied — an entry waiting
  longer than the 10-minute `AGING_THRESHOLD_MS` is promoted one rank tier
  toward `high`, never skipping a tier — for any orchestrator that wants to
  inspect the queue without claiming anything.)

  The dispatcher that watches the queue and drives it back into a spawn is
  three `cli.mjs` flags composing `lib/dispatch.mjs`'s
  `claimAndAdmitQueueEntry`: `--dequeue-if-capacity` (atomic claim-then-admit
  against the same shared live-agent ceiling `--desired-agents` reserves
  against; decision-only, never spawns) → the orchestrator spawns externally
  → `--bind-pid=<pid> --lease-id=<leaseId> --orchestrator-id=<id>` (stamps
  the just-spawned agent's real OS pid onto the leased queue entry via
  `bindPidToLease`, `lib/queue.mjs`, so a LATER `--evict-pid` for that same
  lease can verify the pid it's about to signal actually matches the pid
  that was bound to it; `--orchestrator-id` is required, exactly like every
  other beat type) → `--claim=live-agent:N` (registers the real running
  agent and atomically clears this orchestrator's admission hold) → … →
  `--release=live-agent:N` (when the agent finishes) → `--ack-lease=<leaseId>`
  (permanently removes the queue entry; `--release-lease=<leaseId>` instead,
  if the spawn failed, to requeue it). This exact ordering is mandatory but
  documentation-only — `reserveAdmission`'s admission hold replaces rather
  than accumulates per orchestrator, so repeated `--dequeue-if-capacity`
  calls without an intervening real `--claim` silently under-report that
  orchestrator's true outstanding admission. This remains a cooperative,
  in-beat drain, not a standalone daemon. **Callers must inspect
  `--bind-pid`'s JSON output, not just its exit code.** `handleBindPid`
  exits 0 with `{ "bound": false }` on a silent no-op — a stale, expired, or
  wrong-orchestrator lease — which is correct behaviour for the primitive
  (it never throws for an ordinary, already-gone lease) but means a caller
  that only checks the process exit code can silently fail to bind and only
  discover it later, when a subsequent `--evict-pid` skips the
  pid-lease-mismatch check entirely (degrading back to unverified pairing)
  with no visible signal at the point of failure. Treat `bound: false` as
  "the pid/lease pairing verification did not take effect for this lease"
  and handle it explicitly in the dispatch sequence above.

The forward/library-level contract also defines a third output, `pause`,
naming a specific already-running agent to shed on a RED beat. **`pause` is
recommendation-only**: `selectPauseCandidate` only *names* a candidate for
the orchestrator's own judgement to act on — it does not itself perform any
real process-control operation. Nothing in the default pause path sends the
named process a signal, suspends/resumes it, or checkpoints it: no code
reachable from a bare RED beat (no extra flags) calls
`kill`/`SIGSTOP`/`SIGCONT`/`exec` against a named agent process. **The one
exception is the opt-in eviction beat** (`--evict-pid`, see "Eviction"
below): `lib/real-evict-adapters.mjs` sends real `SIGTERM`/`SIGKILL` via
`gracefulStop` when a caller explicitly opts in with all four required
flags. The claim above is scoped to the `pause`/`pauseCandidate` output
specifically, which stays observation-only regardless:

- **`{"type":"pause","pauseCandidate":{"agentId":"agent-7", ...}}`** — either
  axis RED and a real running agent is available to name. Orchestrator A
  would *treat* `agent-7` as the recommended candidate to pause (the
  policy-selected candidate — today, highest-RSS) and decide, in its own
  code, what if anything to actually do about it — this skill never pauses
  `agent-7` itself, sends it any signal, or reclaims its memory; every other
  running agent stays undisturbed regardless, and A resumes normal spawning
  as soon as a later beat reports `spawn-allowed` again.
- **`{"type":"pause","pauseCandidate":null}`** — either axis RED but there is
  no real running agent to name (an honest "nothing to pause" signal, not a
  placeholder or an error). This is still `pause`, never downgraded to
  `hold`: a RED axis always reports `pause` regardless of running-agent
  availability. Orchestrator A treats this identically to the named-candidate
  case for spawn purposes — it does **not** spawn new sub-agents this beat —
  it simply has nothing of its own to act on for the pause recommendation.

The failure mode this contract exists to prevent: an orchestrator reading
"RED" and deciding to stop spawning *forever*, or an implementer turning the
traffic light into a hard gate the orchestrator can't safely retry past. If
you find yourself writing "if AMBER/RED, don't ever check again" — that's
the anti-pattern this skill exists to avoid.

**Worked example, continued — a heartbeat tick between beats.** Suppose
Orchestrator A's 2 sub-agents from above are now running and it isn't due for
its next real beat for a while (no sub-agent has completed yet). Instead of
waiting in silence, its own wall-clock timer — firing every
`DEFAULT_HEARTBEAT_INTERVAL_MS` (~20s; see "Check points" below) — runs:

```bash
node scripts/agent-resource-management/cli.mjs \
  --session-id="$CLAUDE_CODE_SESSION_ID" --heartbeat
```

This reads the machine and drives the identical pipeline a real beat does
(same `collect()` → classify → dibs → traffic-light steps), so it still
catches a fast-moving RED excursion between real beats. The one difference:
this call is passed a non-counting classification mode, so a GREEN sample
here does **not** advance `consecutiveGreen` toward hysteresis recovery —
only a real beat's GREEN sample does that. Concretely: if axis history was
already `AMBER`/tripped, a heartbeat tick that samples GREEN still reports
`hold` (the last tripped level), exactly like a normal beat would with the
same history — a heartbeat tick never yields a different top-level literal
(`spawn-allowed` / `hold` / `pause`) than a normal beat would for the same
underlying sample and history; it only differs in whether GREEN samples are
*counted* toward eventually recovering. Orchestrator A treats the
heartbeat's output the same way it treats a normal beat's for axis-state
purposes: `hold` means don't spawn new sub-agents this tick;
already-running agents are undisturbed either way. A heartbeat's `hold`
does **not** itself trigger a fresh `--enqueue` call — a heartbeat carries
zero real spawn intent (see the caveat below), so it has no deferred work of
its own to enqueue; the enqueue-on-`hold` behavior above belongs to a REAL
beat (check points 1/2 below), which enqueues once per deferred sub-agent,
not once per observational heartbeat tick.

**Caveat — a heartbeat's `allowance` does NOT reflect the global spawn-rate
bucket.** A `--heartbeat` beat never calls `consumeGlobalSpawnTokens` at
all — it represents zero real spawn intent, so it always short-circuits to a
full/no-op grant regardless of the bucket's actual state. This means a
heartbeat can report a non-zero `allowance` (up to the full dibs-computed
headroom/share) even when the shared bucket is completely drained; the very
next REAL beat, run with the identical inputs, can correctly report
`allowance: 0` because it actually draws from the bucket. **An orchestrator
must NOT spawn agents directly off a heartbeat's `allowance` value.** Use a
heartbeat only to observe axis-level state (GREEN/AMBER/RED, and any
`pause` candidate) between real beats — never as the basis for an actual
spawn decision. Before spawning, always run a REAL (non-heartbeat) beat
immediately first; that beat's `allowance` is the one that correctly
reflects the bucket's current state.

## Check points — the moments a resource check belongs

Six moments in an orchestrator's lifecycle where a resource check belongs:

1. **Before spawn** — the `--desired-agents=N` beat described in "Invoking
   the CLI" below. An orchestrator about to spawn sub-agents checks first and
   paces its spawn count to the returned `allowance`.
2. **After sub-agent completion** — the "Worked example"'s beat, run the
   moment a sub-agent's task finishes and before deciding what to spawn next.
   This is the *normal* beat cadence this skill is built around.
3. **Periodically, via `--heartbeat`** — a timer-driven poll an orchestrator
   runs on its own wall-clock schedule, independent of sub-agent completions,
   so a fast-moving RED excursion is caught even during a long stretch with
   no natural beat. `cli.mjs` does not loop or self-schedule this — it stays
   a one-shot process exactly like every other invocation; the
   orchestrator's own timer is what actually fires `--heartbeat` repeatedly.
   `DEFAULT_HEARTBEAT_INTERVAL_MS` (`cli.mjs`, currently `20 * 1000` — 20
   seconds) is the **recommended** cadence for that timer, not a value
   `cli.mjs` enforces on itself. A `--heartbeat` call drives the identical
   pipeline as a normal beat and can still detect an immediate worsening to
   AMBER/RED, but it never advances the recovery counter
   (`consecutiveGreen`) on a GREEN sample. It surfaces the same literals a
   normal beat can — `--heartbeat` changes nothing about which literals are
   reachable, only whether a GREEN sample counts toward hysteresis recovery.
4. **Periodically, via `--poll-footprint`** — the same timer-driven-poll
   shape as `--heartbeat` above, but for an orchestrator's own declared
   per-agent memory footprint rather than the machine-wide GREEN/AMBER/RED
   axes. `cli.mjs` does not loop or self-schedule this either — it stays a
   one-shot process; the orchestrator's own timer is what actually fires
   `--poll-footprint` repeatedly. `DEFAULT_FOOTPRINT_POLL_INTERVAL_MS`
   (`cli.mjs`, currently `20 * 1000` — 20 seconds) is the **recommended**
   cadence, not a value `cli.mjs` enforces on itself, and must stay within
   the documented `MIN_FOOTPRINT_POLL_INTERVAL_MS`/
   `MAX_FOOTPRINT_POLL_INTERVAL_MS` bound (10–30 seconds).
5. **Before resuming a paused agent** — the forward contract's `pause`
   output names a specific agent to shed on a RED beat; the mirror-image
   check, at the moment an orchestrator would *resume* that paused agent, is
   the intended point to re-check the traffic light before bringing it back.
   **Pause/resume execution itself is not implemented** — `cli.mjs`
   genuinely names a real pause candidate today, but there is no code path
   that actually pauses or resumes an agent process — acting on that name is
   documented as the intended future integration point, not a shipped
   behavior.
6. **Immediately after a stale/failed sample** — `collect()` retries a
   single *transient* `realCollect()` failure once, immediately, before any
   pipeline stage (classification, dibs) ever sees it: if the first attempt
   throws, a second attempt is made in the same beat; only if both throw
   does `main()` propagate the failure to the top-level exit-1 stderr path,
   exactly as an un-retried failure always has. This is a narrow, bounded
   retry around a single `collect()` call — **it is not** the
   freshness-window reuse mechanism (`isSampleFresh` in
   `lib/coordination-file.mjs` — a beat reuses another orchestrator's still-
   fresh shared sample from the coordination file instead of always
   re-sampling). A sample that `takeSample()` (`lib/recon.mjs`) itself marks
   stale after collection succeeds — as opposed to `collect()` throwing
   outright — is unaffected by this retry and still degrades to
   `takeSample()`'s existing AMBER fallback, per that module's own contract
   and tests.

## Invoking the CLI

Run `scripts/agent-resource-management/cli.mjs` at each beat — the moment
right after each sub-agent's task completes (not on a fixed timer; this is a
"check before your next decision" call, not a background poll):

```bash
node scripts/agent-resource-management/cli.mjs \
  --session-id="$CLAUDE_CODE_SESSION_ID" \
  --desired-agents=<how-many-this-orchestrator-would-spawn-this-beat>
```

### `cli.mjs` is the sole admission authority

This exact invocation is the **only** admission-decision gate for this
skill. A session must never construct its own alternative gate — for
example, a raw shell one-liner that combines a memory-pressure sysctl, a
`ps` count of agent processes, and a swap-usage reading into a home-grown
spawn/hold verdict. Any such ad-hoc check reimplements, incompletely and
without the trend-aware classification, dibs accounting, and hysteresis this
file documents below — and it can disagree with `cli.mjs`'s own verdict for
the exact same window, which makes it actively misleading rather than
merely redundant. If a beat needs a decision, call `cli.mjs`; there is no
sanctioned shell-level substitute or supplement to it.

This does not restrict the CLI's own advisory reads — `--query-capacity`
(documented below) is a legitimate, sanctioned way to inspect `cli.mjs`'s
own state without taking a spawn decision. The prohibition is specifically
on building a gate *outside* `cli.mjs`, not on using the flags `cli.mjs`
itself provides.

- **`--session-id="$CLAUDE_CODE_SESSION_ID"`** (the required, and only,
  sanctioned way to identify this orchestrator) — `cli.mjs` calls
  `deriveOrchestratorId()` on the value itself and uses the result as this
  beat's `--orchestrator-id`. Copy the flag verbatim, including the quotes:
  the shell expands `$CLAUDE_CODE_SESSION_ID` from the Claude Code process
  environment, so the env var's name stays visible in the command rather
  than hidden inside `cli.mjs`, and a wrong value is a one-word fix.

  **Never hand-pick a free-form id** (`orchestrator-a`, `orch-a`,
  `my-orchestrator`, or similar), and never run `deriveOrchestratorId`
  yourself and paste its output. The ARM PreToolUse gate (see below) derives
  its own lookup key from the *same* session id independently of this call.
  Unless both sides run the identical derivation on the identical input, the
  gate's sticky-opt-in lookup never matches and the gate silently never
  activates for that session. `--session-id` exists precisely so that
  derivation happens in ONE place for callers: the orchestrator and the hook
  can then only agree, or be wrong together, never drift apart.

  `deriveOrchestratorId` (`lib/orchestrator-id.mjs`) is pure — no clock, no
  randomness, no per-process salt — and throws on a missing, blank, or
  whitespace-padded session id rather than substituting a shared fallback.
  There is deliberately no fallback id, because one would let unrelated
  sessions inherit each other's sticky opt-in and fail-closed gate state. An
  empty `--session-id=` and combining `--session-id` with
  `--orchestrator-id` are both hard rejections with **exit code 2** and an
  actionable stderr message; the latter is a conflict rather than a
  precedence rule precisely because silently picking a winner would recreate
  the drift this flag eliminates.
- **`--orchestrator-id=<id>`** (required, but supplied for you via
  `--session-id`) — the stable identifier this orchestrator is keyed by
  across beats, so its dibs entry and hysteresis history persist between
  separate CLI invocations (each beat is a fresh, short-lived process, not a
  long-running daemon). Pass `--orchestrator-id` directly only from
  non-session tooling that genuinely has no Claude Code session (for example
  a `--record-outcome` call seeding history in a test fixture).
- **`--desired-agents=`** (optional, default `1`) — how many sub-agents this
  orchestrator would like to spawn this beat. `allowance` still carries the
  final, authoritative answer — `desired-agents` is not itself the CLI's
  grant/deny decision — but it materially shapes that answer in two places.
  First, under dibs contention (2+ live orchestrators),
  `computePerOrchestratorAllowance` caps this orchestrator's own claim to
  `min(desiredAgents, remaining pool)` in priority order (see "How the
  pieces compose" below), so a smaller `desired-agents` yields a smaller
  contended share. Second, it is the exact amount requested from (and
  potentially denied by) the global spawn-rate bucket, via
  `spawnBucketRequestAmount = min(desiredAgents, perOrchestratorAllowance)`.
  An orchestrator that under-declares its real intent will both get a
  smaller contended share and draw fewer bucket tokens than it should,
  potentially under-provisioning itself for later beats.
- **`--agent-class=<label>`** (optional) — names the agent class THIS beat is
  requesting admission for, priced against the per-agent-class
  projected-peak memory admission gate (see "Per-agent-class projected-peak
  memory admission gate" below). Omitting it does not exempt the beat from
  the gate WHEN THE GATE ITSELF RUNS: it degrades to the shared
  `UNLABELLED_AGENT_CLASS` catch-all class, priced exactly like any other
  class with no history. The gate as a whole is only ever consulted on the
  same `!isHeartbeat && isSpawnEligibleBeat` condition the spawn-rate bucket
  and live-agent ceiling share — a `--heartbeat` beat, an AMBER/RED beat, or
  a beat whose own dibs-bounded `perOrchestratorAllowance` is already 0
  never reaches this gate at all. This is the same label
  `--poll-footprint`'s own `--agent-class` passthrough uses — a consistent
  per-agent-class vocabulary across both beats.
- **`--running-agent-classes=<a>,<b>,...`** (optional) — a comma-separated
  list of OTHER agent classes this SAME orchestrator already has running
  concurrently, distinct from `--agent-class` (which always names the ONE
  class this beat is requesting admission for). Needed because the gate's
  own live-dibs snapshot excludes this orchestrator's own entry (it can only
  see other ORCHESTRATORS' declared classes) — an orchestrator running a
  multi-class fleet (e.g. one implementation agent already running plus a
  reviewer agent about to be requested) must declare the implementation
  agent's class here so its memory is counted in the sum, or it silently
  under-projects its own footprint. Empty/omitted means "no other classes
  currently running under this orchestrator".
- **`--heartbeat`** (optional flag, no value) — marks this invocation as a
  timer-driven periodic poll (check point 3 above) rather than a real
  once-per-sub-agent-task beat. Still requires `--orchestrator-id=` and runs
  the full pipeline identically to a normal beat; the only behavioral
  difference is that a GREEN sample under `--heartbeat` does not advance the
  hysteresis recovery counter. Takes no interval argument — the interval
  lives in the orchestrator's own timer, not in this flag; repeated
  `--heartbeat` invocations at any polling frequency are safe. A
  `--heartbeat` beat also NEVER draws from the global spawn-rate token
  bucket, regardless of whatever `--desired-agents` was passed or defaulted
  to — it represents zero real spawn intent, so it is treated as a full/no-op
  grant rather than consuming shared, host-wide spawn tokens.
  **Boolean-flag validation:** only two forms are valid — bare `--heartbeat`
  (Node's `parseArgs` yields `true`) or `--heartbeat=true` (yields the
  string `"true"`). Every other form is rejected: `--heartbeat=false`,
  `--heartbeat=1`, `--heartbeat=TRUE` (case-sensitive match, so this is also
  invalid), and a bare `--heartbeat` followed by a positional value
  (`--heartbeat maybe`) all hard-reject with a stderr message and **exit
  code 2**, checked after `--orchestrator-id`/`--desired-agents` validation
  but before any of the beat's work proceeds. A shared
  `validateBareOrTrueFlag(flags, flagName)` helper in `cli.mjs` implements
  this — `--crashed`, `--record-outcome`, `--user-attests-idle`,
  `--enable-disk-cleanup`/`--disable-disk-cleanup` and `--advise-only` all
  reuse the same helper and the same bare-or-`=true` contract, so every one
  of them rejects malformed values identically.
- **`--debug`/`--verbose`** (optional flags, no value; either name works
  identically) — a diagnostic-only escape hatch that prints the raw
  `memory`/`disk` fields this beat actually read
  (`pressureLevel`/`swapUsedMb`/`swapTotalMb`/`stale` and
  `freeDiskGb`/`stale`) via `printDebugDiagnostics`, `cli.mjs`. **Output goes
  to stderr only, never stdout** — stdout stays the single JSON decision
  contract every caller does `JSON.parse(stdout)` on. This flag is
  diagnostic/non-gating: it does not gate or narrow any admission verdict.
  This output must never be parsed by an orchestrator — it is meant for a
  HUMAN reading the terminal, and nothing in this codebase reads its own
  stderr back. Safe to pass on any beat type; it never throws for a beat
  whose raw sample is missing a given field (e.g. `--claim`/`--release`
  beats that don't populate every field the classifiers care about).

### Output shapes

`buildTrafficLight` in `lib/allowance.mjs` can produce any of four shapes,
plus two more literals `cli.mjs` produces before `buildTrafficLight` is ever
called — six in total, and orchestrators should branch on `.type` and handle
all six:

```jsonc
{ "type": "spawn-allowed", "allowance": 3 }
{ "type": "hold" }
{ "type": "pause", "pauseCandidate": { "agentId": "agent-7", "rssMb": 640, "startedAt": 1735900000000 } | null }
{ "type": "alert", "reason": "disk freeDiskGb=5 below red threshold 10" }
{ "type": "pressure-block", "pressureLevel": 2, "shedSignal": false, "diskTrend": { "trend": "stable", "declineRateGbPerHour": 0 }, "liveAgentGrant": null, "loadAverage": { "oneMinute": 4.2, "fiveMinute": 3.8, "fifteenMinute": 3.1 }, "compressedMb": 512 }
{ "type": "memory-projection-block", "totalProjectedMemoryMb": 15200, "budgetMemoryMb": 14336, "diskTrend": { "trend": "stable", "declineRateGbPerHour": 0 }, "liveAgentGrant": null, "loadAverage": { "oneMinute": 2.1, "fiveMinute": 1.9, "fifteenMinute": 1.7 }, "compressedMb": 340 }
```

`pressure-block` (see "Unmaskable pressure-WARN admission block" below) is
not part of `buildTrafficLight`'s own precedence chain at all — it is an
independent pre-check `cli.mjs` runs before `buildTrafficLight` is ever
invoked. `memory-projection-block` (see "Per-agent-class projected-peak
memory admission gate" below) is likewise outside that precedence chain.

**On a memory-RED beat**, `cli.mjs` calls `listAgentProcesses` on real `ps`
output to get the real running-agent list and feeds it into
`selectPauseCandidate`. If a real agent is found,
`{"type":"pause","pauseCandidate":{...}}` is printed as-is. If none is
found, `selectPauseCandidate` resolves `pauseCandidate: null`, and
`buildTrafficLight` returns `{"type":"pause","pauseCandidate":null}`
unmodified — an honest "nothing to pause" signal. On a GREEN/AMBER beat,
`ps` is never even shelled out — only ever on `memoryState === 'RED'`.

**`alert` is distinct from `pause` — specifically for disk-RED.**
`diskState === 'RED'` (regardless of `memoryState`, including when
`memoryState` is also `'RED'` — disk-RED wins that tie) makes
`buildTrafficLight` return `{"type":"alert","reason":<diskResult.reason>}`
instead of `pause`, checked BEFORE the memory-RED/disk-RED → `pause` branch.
The mechanism stays passive: nothing dispatches an outbound notification on
any channel (no push/email/Slack/webhook) — the traffic-light JSON on stdout
IS the alert.

**The `alert` shape always carries a `memoryAlsoRed: boolean` field, and
`pauseCandidate` is present on it ONLY when `memoryAlsoRed` is `true`.**
Disk-RED wins the `type` tie against memory-RED unchanged, but when
`memoryState === 'RED'` at the same time as `diskState === 'RED'`, the `ps`
shell-out and `selectPauseCandidate` selection already ran, so `alert` is
enriched with `memoryAlsoRed: true` and that already-computed
`pauseCandidate` — surfacing the memory signal instead of discarding it:

```jsonc
{ "type": "alert", "reason": "disk usage at 97% of capacity", "memoryAlsoRed": true, "pauseCandidate": { "agentId": "agent-7", "rssMb": 640, "startedAt": 1735900000000 } }
```

On the disk-RED-only case, the `ps` shell-out never ran, so there is nothing
to surface: `memoryAlsoRed: false` and `pauseCandidate` is genuinely absent
from the shape entirely, not present-and-`null`:

```jsonc
{ "type": "alert", "reason": "disk freeDiskGb=5 below red threshold 10", "memoryAlsoRed": false }
```

This does not add a seventh literal — `alert` stays one of
`buildTrafficLight`'s own four, just with two additional fields on that one
shape.

**`spawn-allowed` does not guarantee `allowance > 0`.** When the global
spawn-rate bucket is fully drained on an otherwise-GREEN beat, `cli.mjs`
reports `{"type":"spawn-allowed", "allowance":0}` rather than downgrading to
`hold` — deliberate: `spawn-allowed` communicates "neither resource axis is
constraining you," and `allowance` communicates "here is how much headroom
exists right now," and those are two independent facts.

`throttle`/`cleanup` are forward-contract literals `buildTrafficLight`'s own
JSDoc documents but never actually produces for any input today — they are
never observed on stdout.

**Test seams** (used only in this repo's own spec files, never in
production): `ARM_COORDINATION_FILE` overrides the coordination file path;
`ARM_FAKE_COLLECT_JSON` substitutes a fixed `{ memory, disk }` reading
instead of shelling out to the real machine; `ARM_FAKE_PS_OUTPUT`
substitutes fixed `ps`-style text instead of shelling out to the real `ps`
binary for running-agent discovery; `ARM_SPAWN_BUCKET_CONFIG_JSON`
overrides the global spawn-rate token bucket's
`{ capacityTokens, refillTokensPerMs }` config (see "Global spawn-rate
limiting" below); `ARM_FAKE_NOW_MS` substitutes a fixed integer for
`Date.now()` as this beat's `now`, letting a black-box test simulate
elapsed wall-clock time across sequential `cli.mjs` invocations without ever
waiting in real time. `ARM_FAKE_NOW_MS` additionally requires the master
switch `ARM_BEAT_NOW_TEST_MODE === '1'` before `resolveNow()` honours it at
all, because a leaked value here would poison SHARED, cross-process
coordination-file state rather than staying local to one process. None of
these should ever be set in a real orchestrator invocation.

### Live-agent ceiling and the `liveAgentGrant` field

A hard, ledger-derived ceiling on concurrently LIVE agents across every
orchestrator on the host (`LIVE_AGENT_CEILING`, default 4 — illustrative,
tune from real data) backs `--desired-agents`'s per-axis cap. The ceiling is
enforced by an atomic `claimCapacity`/`releaseCapacity` reservation against
the coordination file (the same fencing-safe primitive `--claim`/`--release`
use, TTL-pruned so a crashed agent's slot ages out within one freshness
window with no explicit `--release` required — see "Claiming capacity"
below), not by the dibs-fairness division alone.

**`allowance` keeps its existing meaning unchanged, on every axis including
this one.** It is still advisory headroom (cap minus current usage) and is
never narrowed to reflect what the live-agent ledger actually, atomically
granted. What the live-agent reservation actually decided is reported
instead as a separate, sibling field `cli.mjs` attaches to the traffic-light
JSON — same shape as `diskTrend` below:

```jsonc
{ "type": "spawn-allowed", "allowance": 3, "liveAgentGrant": 2, "diskTrend": { "trend": "stable", "declineRateGbPerHour": 0 } }
```

- **`liveAgentGrant`** is the OPERATIONAL number: how many live-agent slots
  this beat was actually, atomically granted permission to spawn right now.
  **Orchestrators that want to know how many agents they may really start
  this beat must read `liveAgentGrant`, not `allowance`** — `allowance` can
  legitimately report more headroom than the ledger will actually grant once
  the reservation is attempted (e.g. under a genuine race for the last
  slot).
- `liveAgentGrant` is `null` whenever the atomic claim was never attempted
  this beat — a heartbeat, an AMBER/RED beat, or a beat whose own
  dibs-bounded `allowance` was already `0`. It is a finite number (which can
  legitimately be `0`, a genuine refusal) whenever the claim WAS attempted —
  always exactly the beats where `trafficLight.type === 'spawn-allowed'`
  with a real dibs-bounded chance to spawn.

The live-agent admission decision is computed inside one locked critical
section, `reserveAdmission` (`lib/coordination-file.mjs`), so the preview it
computes cannot diverge from the ledger between read and write. The internal
admission ledger (`LIVE_AGENT_ADMISSION_CLAIM_TYPE`) and the public ledger a
real `--claim=live-agent:N` writes to (`LIVE_AGENT_CLAIM_TYPE`) are not
mutually blind — a real `--claim=live-agent:N`'s own headroom computation
subtracts outstanding admission holds too, so an outstanding advisory
admission and a genuine concurrent claim FROM A DIFFERENT ORCHESTRATOR can
never together exceed `LIVE_AGENT_CEILING` within a single lock cycle's
decision. That subtraction excludes the claimant's own outstanding
admission hold — **this requires the orchestrator to pass the identical
`--orchestrator-id` value to both the `--desired-agents` beat and a
subsequent `--claim=live-agent:N`**; a different id on the `--claim` is
charged the full admission hold and can be denied indefinitely until it
ages out. Symmetrically, a successful `--claim=live-agent:N` (`granted > 0`)
also releases up to `granted` of that same orchestrator's own admission
hold immediately afterward, converting the advisory hold into the real
claim rather than double-counting it. `LIVE_AGENT_ADMISSION_CLAIM_TYPE` can
never be named in a public `--claim`/`--release` — both handlers reject it
with exit code 2.

The two readers of the live-agent claim ledger use different, deliberate
TTLs: the "snapshot" read of the public real-claim ledger is pruned against
`DEFAULT_CLAIM_TTL_MS` (6h, matching what `--claim=live-agent:N` itself
uses), while the internal admission ledger is pruned against the much
shorter `DEFAULT_FRESHNESS_WINDOW_MS` (90s) — because it records only a
single beat's advisory admission hold, which should never outlive one
freshness window. Net effect: a real `live-agent` claim older than 90s but
younger than 6h is visible to the next beat's headroom check, so
`liveAgentGrant` cannot authorise more agents than `LIVE_AGENT_CEILING`
actually allows once claims persist past 90s. One consequence: a crashed or
never-released agent's ceiling slot no longer frees after the old 90s
freshness window on paths outside a `--desired-agents` beat — recovery
comes only from the per-beat liveness sweep (which runs solely on
`--desired-agents` beats; `--claim`/`--dequeue-if-capacity` return before
reaching it) or from the 6h claim TTL itself expiring.

### Unmaskable pressure-WARN admission block + `shedSignal`

`main()` reads the raw `pressureLevel` off this beat's sample and, if it is
`>= DEFAULT_PRESSURE_OVERRIDE_THRESHOLDS.warnAtOrAbove` (2, i.e. WARN or
worse), blocks admission via `{"type":"pressure-block","pressureLevel":<n>,
"shedSignal":<boolean>}` AFTER memory/disk axis classification and dibs
declaration have run, but strictly BEFORE the spawn-rate bucket or the
live-agent ceiling ever run — an independent PRE-CHECK, not a branch folded
inside `buildTrafficLight`'s own memoryState/diskState precedence chain.
Running it after axis classification/dibs declaration (rather than before)
is deliberate: those calls persist the hysteresis history and refresh this
orchestrator's own dibs liveness/priority, and returning before them would
freeze both for the entire duration of a sustained pressure episode. This is
deliberately UNMASKABLE: without it, `pressureLevel` only ever reached an
admission decision indirectly, blended into `memoryState`, and a
simultaneous disk-RED reading could silently out-vote it, since
`buildTrafficLight` checks `diskState === 'RED'` first. A `pressure-block`
verdict wins that tie instead. `pressureLevel >=
DEFAULT_PRESSURE_OVERRIDE_THRESHOLDS.criticalAtOrAbove` (4, CRITICAL)
additionally sets `shedSignal: true`.

**`shedSignal` is EMISSION-ONLY — the same disclaimer as `pause` above.**
`shedSignal: true` does not itself execute any real process-control action
and does not itself relieve memory pressure — it is observable JSON only,
for an orchestrator's own judgement to act on (or not); no code on the
`pressure-block`/`pause` beat path sends the named beat's host any
`kill`/`SIGSTOP`/`SIGCONT`/`exec` as a RESULT of this field. As with `pause`
above, the one exception is eviction (`--evict-pid`), a genuinely separate,
opt-in beat that DOES send real signals when a caller explicitly asks for
it — `shedSignal`/`pauseCandidate` never trigger it on their own.

A `pressure-block` beat DOES still classify both axes and declare dibs — but
it never draws the global spawn-rate bucket and never attempts the
live-agent ceiling's atomic ledger claim, since the pre-check returns before
either runs. `liveAgentGrant` is always `null` on this literal.

### The AIMD concurrency ceiling

`LIVE_AGENT_CEILING` above is a fixed, hand-picked constant. Layered on top
of it is an Additive-Increase/Multiplicative-Decrease (AIMD) adjustment that
lets the ceiling actually used at admission time drift up cautiously when
the host has been running comfortably, and drop immediately once it hasn't —
the same conservative response curve TCP congestion avoidance uses:
grow slowly, back off fast.

The pure arithmetic lives in `computeAimdCeiling` (`lib/aimd-ceiling.mjs`), a
side-effect-free function taking `{ pressureLevel, ceiling,
sustainedNormalCount }` plus a config and returning the next `{ ceiling,
sustainedNormalCount }`:

- **Additive increase.** On a Normal beat (`pressureLevel === 1`),
  `sustainedNormalCount` increments. Only once it reaches
  `config.sustainedNormalBeatsRequired` CONSECUTIVE Normal beats does the
  ceiling actually grow, by `config.increaseStep`, clamped at
  `config.ceilingMax` — then the counter resets to 0, so the next increase
  needs a fresh run of consecutive Normal beats, not merely staying above
  threshold.
- **Multiplicative decrease.** On any non-Normal beat (`pressureLevel >= 2`,
  WARN or CRITICAL — both use the same factor, no steeper cut for CRITICAL),
  the ceiling drops IMMEDIATELY that same beat to `Math.max(config.floor,
  Math.floor(ceiling * config.decreaseFactor))`, and `sustainedNormalCount`
  resets to 0 — a single WARN/CRITICAL beat discards an entire in-progress
  Normal streak, not just its most recent contribution.
- Both directions always yield integers (`Math.floor`'d on every decrease,
  not only at the clamp boundary) — a ceiling is a count of agents.

**Config shape**, `DEFAULT_AIMD_CEILING_CONFIG` (`cli.mjs`):

```jsonc
{
  "floor": 1,
  "ceilingMax": 64,
  "increaseStep": 1,
  "decreaseFactor": 0.5,
  "sustainedNormalBeatsRequired": 5
}
```

Illustrative starting point — tune once real data is in.

**Persistence and cold start.** The ceiling and `sustainedNormalCount` are
carried as a reserved entry in the same coordination file `declareDibs`/
`readDibs` use, read/written exclusively through `readAimdCeilingState`/
`writeAimdCeilingState`/`advanceAimdCeilingState` (`lib/coordination-file.mjs`).
On the very first read against a coordination file with no prior AIMD
entry, `readAimdCeilingState` both returns AND persists the cold-start
default `{ ceiling: 4, sustainedNormalCount: 0 }` under a single lock
acquisition, so two first-ever readers racing the same lock converge on one
persisted entry rather than a duplicate write. On any coordination-file
error, `cli.mjs` degrades to the flat, un-adjusted `LIVE_AGENT_CEILING`
rather than assuming an AIMD-adjusted value was safely read or computed.

The AIMD-adjusted value is what actually backs the live-agent ceiling
`reserveAdmission` enforces — AIMD only decides WHAT the ceiling number is
this beat; it has no admission logic of its own.

**Accepted risk — mixed-version fleet.** An orchestrator still running an
older `cli.mjs` binary that predates the AIMD gate never calls
`advanceAimdCeilingState` at all, so it neither contributes to nor benefits
from the shared ceiling's drift — it keeps using whatever fixed ceiling
logic its own binary shipped with.

### Per-agent-class projected-peak memory admission gate

Runs strictly AFTER the pressure-WARN pre-check above, and AFTER this
beat's own `declareDibs`/`readDibs` cycle, but BEFORE the global spawn-rate
bucket and BEFORE `reserveAdmission`'s atomic live-agent-ceiling claim. It
sums the projected peak memory of every OTHER live orchestrator's declared
agent classes (read from the coordination file's live dibs snapshot), plus
this orchestrator's OWN other concurrently running classes (declared via
`--running-agent-classes`), plus the candidate class named by `--agent-class`
(or `UNLABELLED_AGENT_CLASS` if omitted) — repeated `min(desiredAgents,
perOrchestratorAllowance)` times (floored at 1), not always exactly once: a
`--desired-agents=N` beat that could actually draw N slots this beat prices
all N about-to-spawn candidates, not just the first. Every OTHER
orchestrator's declared classes are still summed once per distinct class
regardless of ITS `--desired-agents` — a lower bound on every other
orchestrator's real concurrent demand.

`computeProjectedPeakMemoryMb` (`lib/memory-projection.mjs`) prices each
class from its own per-class history (`lib/history.mjs`'s `readHistory`,
falling back to a conservative cold-start estimate for a class with no
usable history), and returns `withinBudget: false` once the sum crosses
`totalHostMemoryMb - memoryBudgetMarginMb` (16 GiB minus a 2 GiB margin by
default — tune these to your actual host). On a refusal, `cli.mjs` prints
`{"type":"memory-projection-block", totalProjectedMemoryMb, budgetMemoryMb,
diskTrend, liveAgentGrant: null, loadAverage, compressedMb}` and returns —
mirroring `pressure-block`'s own shape and its `liveAgentGrant: null`
convention. On a coordination-file/history-file error (rather than a
genuine budget refusal), `totalProjectedMemoryMb` and `budgetMemoryMb` are
printed as `null` — an orchestrator parsing this literal must handle both
the numeric-refusal shape and this degraded `null` shape.

**`--agent-class` values MUST match the `--operation-type` used to record
this same work's history, or the gate never sees real data for that class.**
`readHistory` (`lib/history.mjs`) keys the on-disk store strictly by
`operationType` — the value passed to `--record-outcome
--operation-type=<type>` — NOT by `agentClass`, which is a separate,
orthogonal field on an individual observation. This gate looks up history
using the STRING passed to `--agent-class`/`--running-agent-classes` as if
it were an `operationType`. If an orchestrator's `--agent-class` labels
never literally match the `--operation-type` labels it uses for
`--record-outcome`, every lookup misses, every class is priced at
cold-start forever, and the gate never becomes evidence-based no matter how
much history accumulates. To get real, empirical pricing: use the IDENTICAL
string for both `--agent-class` (on `--desired-agents`/
`--running-agent-classes`/`--poll-footprint`) and `--operation-type` (on
`--record-outcome`) for a given kind of agent.

**Precedence relative to `pressure-block`.** The two are independent
pre-checks, not alternates of one shared branch: a WARN/CRITICAL pressure
reading blocks via `pressure-block` even when the candidate class is
comfortably under the memory-projection budget (`pressure-block` runs first
and returns before the memory-projection gate is ever reached), and a
GREEN/GREEN pressure reading still allows `memory-projection-block` to
refuse independently once the projected class sum crosses budget. The two
verdicts are always distinguishable on stdout.

**Locking.** The gate owns its own atomic critical section via
`reconcileMemoryProjectionAdmission` (`lib/coordination-file.mjs`) — acquired
and fully released here, strictly before `reserveAdmission`'s own lock is
ever reached, never nested inside it. On refusal, this same reconcile call
retracts this orchestrator's own just-declared `agentClasses`/`agentClass`
dibs field in the same write, so a refused (never-admitted) beat does not
keep over-counting its demand against every other orchestrator's sum until
its dibs entry's liveness TTL expires. This gate only ever retracts on ITS
OWN refusal — a candidate class that clears this gate but is then denied
further down (the global spawn-rate bucket or the live-agent ceiling) is not
separately retracted here, but self-corrects on this orchestrator's very
next beat because `declareDibs` fully REPLACES (not merges) the declaring
orchestrator's `agentClasses` on every beat, and a live orchestrator
re-declares at least once per `DEFAULT_HEARTBEAT_INTERVAL_MS` (~20s) even
when idle.

**Accepted risk — one dibs entry per class, not per running agent.**
`readAgentClasses()`/`--running-agent-classes` report WHICH classes an
orchestrator has live, not how many concurrent agents of each — a class
named once and a class with three concurrently-running agents look
identical to every other orchestrator's sum ("at least one live X"). The
sum this gate computes is therefore a LOWER bound on real memory demand
whenever any orchestrator runs more than one agent of the same class at
once. THIS beat's own candidate is the one exception (see
`candidateAgentCount` above) — a `--desired-agents=N` beat prices its own N
about-to-spawn candidates exactly, closing the gap on the self side only.

**Accepted risk — cold-start pricing can amplify without bound.** An
unlabelled/never-recorded class is priced at `Math.max(maxKnownPeak,
coldStartDefaultMemoryMb)` — deliberately conservative, but with no ceiling
of its own: if any OTHER class in the same computation has a large
empirical peak, every cold-start class in that same beat is priced at that
same large number. In the worst case this can make an
`UNLABELLED_AGENT_CLASS` beat refuse indefinitely whenever a single heavy
labelled class is running alongside it. Always passing a real
`--agent-class` is the mitigation — it lets the class accumulate its OWN
empirical history instead of permanently riding the cold-start floor.

**Accepted risk — mixed-version fleet.** An orchestrator still running an
older `cli.mjs` binary never calls this gate at all — it has no
`--agent-class`/`--running-agent-classes` flags to pass, and its dibs entry
carries neither `agentClasses` nor `agentClass`, so its real memory
footprint stays invisible to every OTHER beat's projected sum until it
upgrades.

### Load average, compressed memory, and compression velocity — observability only

Every beat's printed JSON also carries `loadAverage: { oneMinute, fiveMinute,
fifteenMinute }` (sourced from Node's built-in `os.loadavg()`),
`compressedMb` (sourced from the same `vm_stat` reading already parsed for
`freeRamMb` — can legitimately be `null` when the reading is garbled), and
`memory: { compressionVelocity }` where `compressionVelocity: {
compressionsPerSec, compressionRatio, coldStart }` (computed by
`computeCompressionVelocity`, `lib/compression-velocity.mjs`). All three are
present on every literal and every beat type. **None of them ever
influences, gates, or narrows any admission verdict** —
`classifyMemoryRaw`/`classifyMemoryAxis` never read any of them, only
`pressureLevel`/`swapUsedMb`/`swapTotalMb`. This is deliberate: a
high-load-but-network-blocked host is a symptom of memory-compressor
thrash, not a cause, so treating high load (or compression pressure) as a
reason to refuse admission would punish exactly the workload this skill
exists to keep flowing. They are surfaced purely for visibility/future
analysis.

## The coordination file

One shared JSON file, read/written by every concurrent orchestrator **on the
host** — not scoped to any one repo or checkout:

```
~/.claude/agent-state/resource-coordination.json
```

The resource being coordinated (RAM/disk) belongs to the whole machine, not
to any one checkout, so the default path is resolved under the user's home
directory (`os.homedir()`), never `process.cwd()` — an orchestrator running
out of one checkout and another running out of a different one on the same
host still share this one file. `cli.mjs` creates this file's parent
directory on first use if it doesn't already exist, so a fresh machine
doesn't silently degrade to "no dibs" forever; a genuinely unwritable path
(e.g. permissions) still degrades gracefully (see below).
`ARM_COORDINATION_FILE` overrides this default for tests.

This is **one shared file all orchestrators read and write**, because the
whole point is cross-orchestrator coordination, not per-agent pacing.

Format: a JSON array of per-orchestrator entries, upserted by
`orchestratorId`:

```jsonc
[
  {
    "orchestratorId": "orchestrator-a",
    "desiredAgents": 2,
    "declaredAt": 1735900000000,
    "memoryHistory": { "consecutiveGreen": 1, "tripped": true, "level": "AMBER" },
    "diskHistory": { "consecutiveGreen": 3, "tripped": false, "level": "GREEN" }
  }
]
```

- **Atomic writes** — every write goes to a `<path>.tmp-<random>` sibling
  then `rename()`s onto the real path, so a concurrent reader only ever
  observes the whole old file or the whole new one, never a torn write.
- **Concurrent-write safety** — a `<path>.lock` exclusive-create lock file
  (with a fencing token) serialises the read-modify-write cycle across
  independent orchestrator processes; a lock older than the configured
  staleness window is reclaimed from a crashed writer rather than blocking
  forever. See `lib/coordination-file.mjs` for the full mechanism and its
  extensive doc comments on the fencing invariant.
- **Write-side ownership re-check** — every write re-reads the lock file and
  re-compares its fencing token immediately before the final `rename`, and
  throws `LockLostError` instead of writing when it no longer matches, so a
  holder whose critical section outlived the staleness window is refused
  rather than silently clobbering the reclaiming holder. This narrows the
  race window to the single event-loop hop between the check and the
  `rename` — it does not close it structurally the way the
  removal/restore fencing invariant does. Under heavy contention a beat can
  end with every writer refused (`LockLostError`); nothing retries
  internally — recovery is the orchestrator's next beat. A burst of
  `LockLostError` warnings means "the host is too loaded for this many
  concurrent orchestrators," the same signal the traffic light is already
  giving.
- **Liveness pruning** — entries older than the configured liveness
  threshold are excluded (pruned on write, excluded on read) so a crashed
  orchestrator doesn't hold a permanent slot.
- **`memoryHistory`/`diskHistory`** — each orchestrator's own hysteresis
  state, persisted here so recovery-gating survives across the separate
  `cli.mjs` process invocations that make up its beats.

Implementation: `lib/coordination-file.mjs` (`declareDibs`, `readDibs`,
`isSampleFresh`).

## Configurable thresholds — one place to look

Almost all numeric defaults live as documented, named constants at the top
of `cli.mjs`. The exceptions are the `lib/` modules that carry their own
constants beside the code they bound (`lib/cleanup.mjs`'s sweep bounds,
`lib/coordination-file.mjs`'s lock-retry knobs, `lib/history.mjs`'s
retention cap, `lib/cost-estimate.mjs`'s confidence boundary — read each
module for its current set).

The `cli.mjs` set includes `DEFAULT_MEMORY_THRESHOLDS`,
`DEFAULT_DISK_THRESHOLDS`, `DEFAULT_HYSTERESIS_CONFIG`,
`DEFAULT_ALLOWANCE_CONFIG`, `DEFAULT_HEADROOM_CONFIG`,
`DEFAULT_SPAWN_BUCKET_CONFIG`, `DEFAULT_LIVENESS_THRESHOLD_MS`,
`DEFAULT_FRESHNESS_WINDOW_MS`, `DEFAULT_HEARTBEAT_INTERVAL_MS`,
`DEFAULT_FOOTPRINT_POLL_INTERVAL_MS`, `MIN_FOOTPRINT_POLL_INTERVAL_MS`,
`MAX_FOOTPRINT_POLL_INTERVAL_MS`, `DEFAULT_DISK_TREND_RECENCY_WINDOW_MS`,
`DISK_TREND_NOISE_EPSILON_GB`, `LIVE_AGENT_CEILING` (default 4), and
`DEFAULT_PRESSURE_OVERRIDE_THRESHOLDS` (`{ warnAtOrAbove: 2,
criticalAtOrAbove: 4 }`). **All of these are starting hypotheses to tune
against real data on your own host, not settled constants.** A human
re-tuning any threshold in `cli.mjs` edits that file only; nothing else in
this skill hardcodes a threshold value.

- `DEFAULT_HEARTBEAT_INTERVAL_MS` is `20 * 1000` (20 seconds) — the
  recommended polling cadence for an orchestrator's own wall-clock timer
  driving repeated `--heartbeat` invocations; `cli.mjs` does not
  self-enforce it.
- `DEFAULT_FOOTPRINT_POLL_INTERVAL_MS` is `20 * 1000` (20 seconds), bounded
  by `MIN_FOOTPRINT_POLL_INTERVAL_MS` (`10 * 1000`) and
  `MAX_FOOTPRINT_POLL_INTERVAL_MS` (`30 * 1000`) — the recommended cadence,
  and its documented bound, for an orchestrator's own wall-clock timer
  driving repeated `--poll-footprint` invocations; `cli.mjs` does not
  self-enforce either.

Several related defaults live beside the logic they configure instead of
being duplicated here:
- lock staleness (30s) and lock retry/backoff — top of
  `lib/coordination-file.mjs`.
- `DEFAULT_CLAIM_TTL_MS` (6 hours, `lib/coordination-file.mjs`) — the claim
  ledger's TTL window: how long a `--claim`-granted slot is honoured in
  `alreadyGrantedTotal`/`grantedTotal` aggregate math with no explicit
  `--release` ever made against it.
- history retention cap (`DEFAULT_RETENTION_CAP`, 50 raw observations per
  operation type) and the reservoir-sampling capacity (`RESERVOIR_CAPACITY`,
  500) — top of `lib/history.mjs`.
- the empirical-vs-default confidence boundary
  (`DEFAULT_MIN_CLEAN_RAW_SAMPLES_FOR_EMPIRICAL`, 3 clean samples) and the
  flat cold-start cost estimate (`DEFAULT_ESTIMATE_CONFIG.defaultPeakMemoryMb`,
  350MB) — top of `lib/cost-estimate.mjs`.

Every threshold is passed as an explicit argument into the pure classifier
functions (`classifyMemoryAxis`/`classifyDiskAxis` in `lib/threshold.mjs`) —
never inlined as a magic number in the classification logic itself.

## Memory-axis headroom, swap-trend recovery, and disk-decline smoothing

Three refinements sit on top of the flat-cap/simple-hysteresis behavior
described above. Each is an illustrative starting point, not a settled
constant — tune once real data is in.

**1. Memory-axis headroom formula — retired from the live spawn-decision
path.** An earlier design computed the memory-axis cap from live
`freeRamMb` via `computeHeadroomCap` (`lib/allowance.mjs`):
`memoryHeadroomCap = floor((freeRamMb - osBaselineMb - reserveMb) /
perAgentRamMb)`, clamped to `>= 0`, using `DEFAULT_HEADROOM_CONFIG` (`{
perAgentRamMb: 350, osBaselineMb: 2048, reserveMb: 2048 }`). **This formula
is retired from the live spawn-decision path, permanently, not as an
interim placeholder** — `freeRamMb` (and the `ps`-derived RSS it's built
from) INVERTS under macOS memory-compressor pressure — it can read as
IMPROVING even as the host approaches freeze — so it is disqualified as an
admission input, live or absent. `main()`'s spawn-decision beat now uses the
real, ledger-derived `LIVE_AGENT_CEILING` on both axes (see "Live-agent
ceiling" above) instead. `computeHeadroomCap`/`DEFAULT_HEADROOM_CONFIG`
still exist in `lib/allowance.mjs`/`cli.mjs` and back
`--query-capacity`/`--claim` for every OTHER operation type — a deliberate,
permanent scope decision, not something expected to be revisited.

**Known gap — `freeRamMb` can be over-conservative on macOS.** `parseVmStat`
(`lib/recon.mjs`) derives `freeRamMb` from `vm_stat`'s strict "Pages free"
count only, ignoring purgeable/inactive pages macOS treats as
reclaimable-on-demand. This can make `computeHeadroomCap` (and, downstream,
`--query-capacity`/`--claim`'s per-type `freeCapacity`) report zero
available capacity even while `swapUsedMb` stays at `0` and `pressureLevel`
stays at `1` (normal) — the exact signals `classifyMemoryAxis` already
trusts to call the beat GREEN.

**2. Swap-trend GREEN-recovery gate.** Recovering the memory axis to GREEN
already requires `hysteresisRecoverConsecutiveGreen` consecutive GREEN
samples (count gate) AND `hysteresisRecoverWindowMs` elapsed since the last
non-GREEN sample (wall-clock gate). A THIRD, independent gate sits on top of
both: `classifyMemoryAxis` (`lib/threshold.mjs`) also tracks a rolling
3-sample window of `swapUsedMb` readings, and recovery is additionally
blocked unless that window's trend is non-increasing (`window[last] <=
window[0]`). **This means recovery can be refused even after the count and
wall-clock gates have both already passed, purely because swap is still
trending upward** — worsening (AMBER/RED) is completely unaffected and still
takes effect immediately. The gate is skipped (treated satisfied) whenever
fewer than 3 swap readings exist yet, so recovery never hangs waiting on
trend data that was never collected.

**3. Disk-decline median smoothing.** The disk axis's steep-decline AMBER
check (`hoursToFull` vs. `amberHoursToFullBelow`) uses the MEDIAN of a
rolling 3-sample window (`classifyDiskAxis`, `lib/threshold.mjs`) instead of
only this beat's raw, possibly-noisy `declineRateGbPerHour` sample — robust
to a single outlier/reversing sample. This substitution applies ONLY to the
steep-decline `hoursToFull` check; the RED `freeDiskGb` floor and the GREEN
`freeDiskGb` ceiling are both still driven by the instantaneous
`freeDiskGb` reading.

## How the pieces compose

The spawn-decision beat (`--desired-agents`, including `--heartbeat`)
composes:

```
collect()                       -- the ONLY thing that shells out
  -> takeSample()                lib/recon.mjs
  -> getLoadAverage()             os.loadavg() — no shell-out, purely
                                  observational; computed early, attached to
                                  every print below regardless of verdict
  -> computeDiskTrend()           see "Disk decline-rate trend" below —
                                  computed unconditionally here, before axis
                                  classification, so it is already available
                                  for BOTH the pressure-WARN pre-check's
                                  print below and classifyDiskAxis()'s
                                  steep-decline AMBER check
  -> classifyMemoryAxis()         lib/threshold.mjs   (+ hysteresis history)
     classifyDiskAxis()
  -> declareDibs() / readDibs()   lib/coordination-file.mjs
  -> pressure-WARN pre-check      reads raw pressureLevel; if >=
                                  warnAtOrAbove, prints "pressure-block"
                                  HERE (already carrying diskTrend, computed
                                  above) and returns — everything below this
                                  line never runs for that beat. Runs AFTER
                                  axis classification/dibs declaration (not
                                  before) so hysteresis history and this
                                  orchestrator's dibs liveness/priority stay
                                  current across a sustained pressure episode
  -> computePerOrchestratorAllowance()   lib/allowance.mjs
       (flat cap = min(memory, disk); a lone live orchestrator gets that
       full cap as headroom; 2+ live orchestrators each claim
       min(own desiredAgents, remaining pool) in priorityOrder)
  -> claimCapacity()/releaseCapacity()  lib/coordination-file.mjs
       (the live-agent ceiling's atomic reservation, computed BEFORE
       buildTrafficLight but reported as the separate `liveAgentGrant`
       field only after it (alongside `diskTrend`), never folded back into
       `allowance`)
     selectPauseCandidate()
     buildTrafficLight()
  -> print traffic-light JSON (+ diskTrend, liveAgentGrant, loadAverage, compressedMb), exit 0
```

SEVERAL OTHER, standalone beat types bypass most of this pipeline entirely —
none of them declares dibs or drives
`computePerOrchestratorAllowance`/`buildTrafficLight`; see "Recording
outcomes and querying capacity", "`--poll-footprint`", and "Claiming
capacity" for the full per-flag contract. (`--advise-only` is a MODIFIER of
the `--desired-agents` beat, not a standalone beat that bypasses the
pipeline — it runs the full allowance/traffic-light pipeline and emits a
real verdict, which is precisely what the PreToolUse hook parses.)

```
--record-outcome    -> recordObservation()    lib/history.mjs
                     -> print {"recorded":true,...}, exit 0

--poll-footprint=<pid> -> sampleFootprint()    lib/footprint-sampler.mjs
                     -> recordFootprintPoll()  lib/footprint-state.mjs
                     -> print {"polled":true,...,"sampleMb":<number|null>}, exit 0

--query-capacity=... -> takeSample() -> classifyMemoryAxis/classifyDiskAxis
                     -> readHistory()          lib/history.mjs
                     -> estimateOperationCost() lib/cost-estimate.mjs
                     -> print { "<type>": { freeCapacity, confidence }, ... }, exit 0

--claim=<type>:<n>   -> takeSample() + classify + readHistory +
                          estimateOperationCost
                          (BEFORE the lock)
                     -> claimCapacity()        lib/coordination-file.mjs
                          (INSIDE the fencing lock: read the claim ledger,
                          apply alreadyGrantedTotal, write a
                          deny-the-remainder grant atomically)
                     -> print {"granted":<n>}, exit 0
```

Declared dibs (`desiredAgents`) genuinely constrain the reported allowance
once 2+ orchestrators are live on the same coordination file
(`computePerOrchestratorAllowance`): each contending orchestrator receives a
**priority-ordered share** of the flat per-axis cap — the earliest-declared
orchestrator's claim is satisfied first, up to its own `desiredAgents`, then
the next-earliest claims from whatever remains, and so on — rather than
every live orchestrator being told the same full cap simultaneously. A lone
orchestrator (no other live dibs entry) is unaffected — it still gets the
full flat cap as headroom.

This is strict priority order, not an equal or proportional split: it is a
known, documented tradeoff (not a bug) that a small, late-declaring
orchestrator can be fully starved (`0` share) even when it asked for far
less than the remaining cap, if earlier-priority orchestrators' combined
`desiredAgents` already exhausts the cap first. Priority order is by
ascending `firstDeclaredAt` — the timestamp of an orchestrator's FIRST-EVER
declaration, preserved unchanged across every subsequent beat — not by
`declaredAt` (which is refreshed on every beat for liveness pruning). A
`--heartbeat` beat's own dibs entry preserves this orchestrator's last REAL
declared `desiredAgents` (`0` only when there is no prior real declaration
at all) specifically so that polling never adds contention beyond what's
already real.

`cli.mjs` is the thin, largely-untested entrypoint that wires this together
and performs all I/O (shelling out, reading/writing the coordination file).
The `lib/*.mjs` modules (`recon`, `threshold`, `coordination-file`,
`allowance`, `history`, `cost-estimate`, and the rest) are pure/injectable
and carry the unit + integration test coverage; `cli.mjs` itself is
black-box smoke-tested via real process spawning in `cli.jest.spec.mjs`
(and its beat-type-specific siblings `record-outcome.jest.spec.mjs`/
`query-capacity.jest.spec.mjs`/`claim.jest.spec.mjs`/
`disk-trend.jest.spec.mjs`).

## Global spawn-rate limiting

A host-wide token bucket caps the RATE of new agent spawns, independent of
the per-axis memory/disk headroom caps above — a burst of spawns can starve
the machine even while memory/disk both read GREEN, because spawn cost
(process startup, initial model calls, disk churn) front-loads before
steady-state resource use shows up in a sample.

- **Config**: `DEFAULT_SPAWN_BUCKET_CONFIG` at the top of `cli.mjs` —
  `{ capacityTokens: 10, refillTokensPerMs: 1 / 60_000 }` (10 tokens,
  refilling at 1 token/minute). Illustrative starting hypothesis.
- **Test seam / operator override**: `ARM_SPAWN_BUCKET_CONFIG_JSON` (env
  var, JSON `{ capacityTokens, refillTokensPerMs }`) replaces the default
  entirely when well-formed; unset or malformed falls back to the
  production default rather than crashing the beat.
- **Storage**: the bucket lives in the shared coordination file (reserved
  `__global-spawn-rate-bucket__` entry), so it caps spawn rate across every
  orchestrator on the host sharing that file, not just one.
- **When it's consulted**: only on a beat whose axis classification is
  GREEN/GREEN AND whose own dibs-constrained per-orchestrator allowance is
  greater than zero — i.e. only when the beat could actually reach
  `spawn-allowed` AND actually spawn something. An AMBER or RED beat can
  never spawn regardless (`hold`/`pause`), so it never draws a token;
  neither does a GREEN/GREEN beat whose share has already been bounded to
  `0` by dibs division.
- **Heartbeat exclusion**: a `--heartbeat` beat carries ZERO real spawn
  intent, so it is always treated as a full/no-op grant and never calls
  `consumeGlobalSpawnTokens` at all.
- **The bucket is a binary gate, not a second clamp on `allowance`**: the
  bucket is only ever asked for `min(desiredAgents, perOrchestratorAllowance)`
  — this orchestrator's own bounded share, never its full dibs-computed
  headroom. If the bucket grants that (smaller) request in full, the
  dibs-computed `allowance` is left completely untouched. Only a genuine
  denial — the bucket had less than the request and granted `0` — forces
  `allowance` down to `0` on the `spawn-allowed` shape; there is no
  partial-grant case, since the bucket uses deny-the-whole-burst semantics.
- **Fails CLOSED**: a coordination error at this site denies this beat's
  spawn-rate tokens (`grantedCount: 0`) rather than assuming the full
  request was granted — a rate limiter that stops limiting whenever the
  file it coordinates through is under stress is inverted: the file is
  under stress precisely when the host is, which is exactly when the limit
  matters. The `Infinity` sentinel on heartbeat and non-spawn-eligible beats
  is unchanged — those never consult the bucket.

Implementation: `consumeSpawnTokens` (`lib/allowance.mjs`) is the pure,
per-bucket-state token-bucket math; `consumeGlobalSpawnTokens`
(`lib/coordination-file.mjs`) threads that state through the shared
coordination file to make the cap genuinely global.

## Eviction — `--evict-pid`

EVICT is a genuine, opt-in capability — but it ships as a **standalone
`cli.mjs` beat**, not as a seventh literal on `buildTrafficLight`'s own
return union; `buildTrafficLight` still returns exactly the same four
literals it always has. This keeps "a bare RED beat can never evict" true by
construction.

**What it actually does — graceful-stop-and-requeue, not memory
reclamation:**

- **Explicit, per-target opt-in only.** A bare beat (no extra flags),
  however RED memory gets, never evicts anything. Eviction only fires when
  the caller names all four of `--evict-pid=<pid> --lease-id=<leaseId>
  --orchestrator-pid=<pid> --orchestrator-id=<id>` together;
  `handleEvict` builds and prints its own `evict`-typed JSON directly — it
  does not route through `buildTrafficLight`.
- **Ancestry-scoped authorization, bound to the caller's own recorded
  pid.** `isEvictionAuthorized` (`lib/recon.mjs`) walks `ps`-tree ancestry
  from the CALLER-SUPPLIED `--orchestrator-pid`, proving only "descends
  from *some* pid" — it has no dibs-ledger access, so it can't prove "*this*
  orchestrator's" pid. `performEviction` (`cli.mjs`) closes that: before any
  ancestry work, it reads this orchestrator's own dibs-ledger entry and
  rejects with `'orchestrator-pid-mismatch'` if `--orchestrator-pid`
  doesn't equal the `pid` a PRIOR beat recorded — INCLUDING when no `pid`
  was ever recorded (fails closed there too, rather than falling through to
  the ancestry-only check). **Residual:** bare equality can't distinguish
  "still alive" from "OS reused the pid" — a narrow race remains; a missing
  recorded pid means a genuinely live orchestrator is *refused*, never
  wrongly *authorized*. Both accepted for this tool's single-user trust
  model.
- **Graceful stop, not a hard kill.** `gracefulStop` (`lib/graceful-stop.mjs`)
  sends `SIGTERM`, waits a configurable grace period (default 60s,
  overridable for tests), and escalates to `SIGKILL` only if the target is
  still alive after that window. Stopping the process lets the OS reclaim
  its memory as a byproduct — EVICT does not itself "reclaim memory" the way
  a checkpoint/restore mechanism would.
- **Work is never lost, including when the target is already dead.** On
  success, `handleEvict` calls `releaseQueueEntry(queueFilePath, leaseId)`
  (`lib/queue.mjs`) — not `enqueueItem` — so the existing queue entry has
  its `leaseId`/`leasedAt` cleared in place, preserving its original
  `agentClass`/`commandRef`/`orchestratorId`/`enqueuedAt` and
  queue-fairness position untouched.
- **Accepted limitation: non-re-entrant agent classes restart from
  scratch.** Resume-after-re-enqueue relies on the target skill's own
  re-entrancy. Ad-hoc/exploratory agent classes checkpoint no external
  state, so a re-enqueued instance of one simply restarts from scratch.
- **Bounded retry for the `isAlive` zombie-reap race.** `process.kill(pid, 0)`
  succeeds against a zombie (exited but not yet reaped) — it only fails once
  fully reaped. `gracefulStop`'s two `isAlive` checkpoints (post-grace,
  post-SIGKILL) are composed with `lib/reap-aware-is-alive.mjs`'s
  `reapAwareIsAlive`: a single `true` reading is retried with a short
  bounded delay/jitter to give the OS a brief window to reap before the
  process is reported "still alive". This retry is still bounded, not a
  guarantee — a reap that takes longer than the full retry budget is still
  reported as "still alive", which can still cause an unneeded SIGKILL
  escalation.
- **Lease-ownership check.** `handleEvict` verifies that `--lease-id`'s
  on-disk queue entry has an `orchestratorId` matching the calling
  `--orchestrator-id` BEFORE any authorization/kill work happens — an
  orchestrator can only ever release its own leases.
- **`pid-lease-mismatch` — the same-orchestrator pairing check.**
  `bindPidToLease` (`lib/queue.mjs`, wired to the `--bind-pid=<pid>
  --lease-id=<leaseId> --orchestrator-id=<id>` CLI flag) stamps a real OS
  pid onto a leased queue entry at claim time. `performEviction` (`cli.mjs`)
  reads that stamped `leasedEntry.pid` and, when present, rejects with
  `'pid-lease-mismatch'` if it disagrees with the `--evict-pid` value the
  caller supplied — before any authorization/kill work happens, additive to
  (never a substitute for) the ps-tree ancestry check further down. This
  check fires for BOTH callers of the shared `performEviction` core — a
  human/orchestrator-invoked `--evict-pid` and the unattended
  `--watchdog-beat` auto-recycle path. `leasedEntry.pid === undefined` —
  every lease claimed by a caller that hasn't yet adopted `--bind-pid` — is
  deliberately skip-not-fail. **`--bind-pid` is the prerequisite for this
  check to do anything at all.** **Residual, asymmetric trust (accepted,
  consistent with this tool's single-user model):** unlike `--evict-pid`,
  `--bind-pid` takes no `--orchestrator-pid` and requires no dibs-ledger
  liveness check at all — ownership is the self-asserted `--orchestrator-id`
  alone, and `bindPidToLease` is last-write-wins. This can only ever DENY a
  legitimate future eviction — it cannot HIJACK one, since
  `isEvictionAuthorized`'s ps-tree ancestry check still gates the actual
  kill regardless of what pid was bound.
- **`ARM_EVICT_TEST_MODE` — the master test-seam switch.** `handleEvict`
  reads several independent `ARM_FAKE_*` test seams (`ARM_FAKE_PS_OUTPUT`,
  `ARM_FAKE_KILL_LOG`/`ARM_FAKE_KILL_RESULT`, `ARM_FAKE_IS_ALIVE_SEQUENCE`,
  `ARM_FAKE_NOW_MS`, `ARM_FAKE_FOOTPRINT_OUTPUT`).
  **`ARM_EVICT_TEST_MODE=1` must be set before `handleEvict` honors ANY of
  these seams at all.** When unset (the default — true for every real
  orchestrator invocation), `handleEvict` uses ONLY real adapters — a real
  `ps` shell-out, real kill/isAlive adapters, the real `Date.now()`, and
  real `footprint`/`vmmap` shell-outs — full stop, regardless of what any
  `ARM_FAKE_*` var happens to hold. This makes a leaked/stale fake var in a
  real invocation inert by construction.

## Watchdog beat — unattended liveness, queue aging, deadlock tripwire

`--evict-pid` (above) is opt-in per target — a human or an orchestrator's
own logic decides WHICH pid to recycle. `--watchdog-beat` is the detection
layer sitting in front of it: a single standalone, opt-in beat that judges
whether any of the CALLING orchestrator's own spawned agents look stalled,
escalates queue items that have aged past a bound while capacity was free,
checks for a sustained fleet-wide deadlock, and recycles whatever it finds
through the same real `--evict-pid` mechanism documented above.

```bash
node scripts/agent-resource-management/cli.mjs \
  --watchdog-beat \
  --session-id="$CLAUDE_CODE_SESSION_ID" \
  --orchestrator-pid=<pid> \
  [--watchdog-bound-ms=<n>] \
  [--watchdog-deadlock-window-ms=<n>] \
  [--watchdog-aging-bound-ms=<n>]
```

- **Required**: `--orchestrator-id=<id>` and `--orchestrator-pid=<pid>` —
  every candidate this beat considers must be provable, via `ps`-tree
  ancestry, as a descendant of `--orchestrator-pid`. Supply the first by
  passing `--session-id="$CLAUDE_CODE_SESSION_ID"`.
- **Optional bound overrides** — `--watchdog-bound-ms` (per-agent stall
  bound, default 15 minutes), `--watchdog-deadlock-window-ms` (sustained-
  window bound, default `DEFAULT_DEADLOCK_WINDOW_MS`, 15 minutes),
  `--watchdog-aging-bound-ms` (escalation bound, default
  `DEFAULT_ESCALATION_BOUND_MS`, 20 minutes).
- **Standalone, decision-only, and conflict-checked** exactly like every
  other beat-type flag: `--watchdog-beat` is never composed with
  `--desired-agents`/`--heartbeat`/`--evict-pid`/`--claim`/etc. in the same
  invocation — combining it with any of them hard-rejects with exit code 2.

**What it actually does, in order:**

1. **Per-agent stall judgement** over every `ps`-tree root pid this beat can
   prove is the caller's own descendant — flags a pid stalled only when
   BOTH a flat footprint trajectory (the EWMA-smoothed `phys_footprint` MB
   series harvested from prior `--poll-footprint` beats — NOT a CPU-time
   signal) AND no git-tracked worktree mtime movement hold over the bound
   (AND-semantics — either signal alone showing activity is enough to NOT
   flag).
2. **Deadlock-window evaluation** against the real coordination/queue
   files — trips only when 100% of the fleet-wide live dibs entries are
   stalled AND the queue is non-empty, sustained for the full configured
   window.
3. **Queue-aging escalation** — bumps a queue entry's persisted priority
   once it has waited past the bound while capacity was free, alerting once
   per entry.
4. **Recycle-target resolution** — resolves each trip against this beat's
   own `claimLedger`; a target only ever reaches a real recycle attempt when
   its resolved `reentrant` flag is `true` (see "Re-entrancy policy"
   below) — everything else routes to `alert-only`, never a silent
   auto-evict. A flapping-cooldown mechanism excludes any pid whose
   eviction recently either succeeded or failed within
   `DEFAULT_WATCHDOG_EVICT_COOLDOWN_MS`, so a target is not re-SIGTERM'd on
   every subsequent beat with no backoff at all.
5. **Real recycle, in-process** — for every `action: 'evict'` target, this
   beat calls `performEviction(...)` directly, in-process — the same
   I/O-and-decision core `--evict-pid`'s own `handleEvict` calls.
   `cli.mjs` never spawns a child process of its own accord anywhere in
   this recycle path.
6. **Detection logging** — every watchdog trip, aging escalation, and
   deadlock recycle is logged (see "Log shape and how to query it" below),
   independent of whether the recycle attempt itself succeeded.

**Re-entrancy policy.** `lib/eviction-targeting.mjs`'s own conservative
default only ever auto-evicts a `claimLedger` record with `reentrant ===
true` — everything else routes to `alert-only`. `cli.mjs`'s own
`claimLedger` assembly sets `reentrant: true` only when a leased item's
`commandRef` both names a genuinely re-entrant skill (one that resumes from
a checkpoint) AND carries an explicit `--resume=` flag. Every other
`commandRef` shape defaults to the safe `alert-only` routing.

**Known, documented limitations:**

- **This beat runs only inside a live orchestrator's own beat loop, plus an
  optional launchd backstop.** `--watchdog-beat` follows a self-throttling
  backoff pattern (`lib/watchdog-backoff.mjs`, doubling on an idle turn and
  capped at 8x). There is no cron/scheduler that invokes `--watchdog-beat`
  on its own by default — it only ever runs as one more step inside an
  already-running orchestrator's own beat loop. See "Launchd watchdog
  backstop" below for the optional OS-level scheduler path.
- **This does NOT address "zero orchestrators alive, nothing drains the
  queue."** If EVERY orchestrator dies while the queue is non-empty and a
  stalled agent holds a lease, there is no live beat loop left for
  `--watchdog-beat` to run inside — nothing drives detection or recycling
  until the stale lease's own claim TTL naturally expires.
- **The mtime scan only sees git-TRACKED files.** `git ls-files -z`
  enumerates the index, not the working tree, so an actively-working agent
  whose only changes in a stall window are new untracked files produces
  zero mtime movement on the scanned set — a known false-positive risk if
  `phys_footprint` also happens to read flat over that same window.
- **No historical capacity-sample source is wired into aging escalation
  yet.** The capacity-free-throughout check always fails closed (no
  eligible samples) in production today, so aging escalation cannot fire.
- **The deadlock tripwire is single-orchestrator-scoped.** It can only
  resolve footprint/mtime evidence for pids THIS orchestrator's own
  candidates prove ownership of; any OTHER live dibs entry degrades to "no
  evidence", which reads as NOT stalled. Since the tripwire requires 100% of
  the fleet-wide live dibs entries to be stalled, it is effectively inert
  once a second orchestrator has any live dibs entry of its own — a
  deliberate fail-closed posture (better to never trip than to trip on
  incomplete data), not a bug.
- **The watchdog backoff-state file is fleet-shared, not
  per-orchestrator**, unless the caller sets `ARM_WATCHDOG_BACKOFF_FILE`
  explicitly — one orchestrator's idle turn can pace off an unrelated
  orchestrator's genuinely-due watchdog work. The launchd wrapper (see
  below) is a deliberate exemption: it points at its own isolated file by
  default.

**Fake test seams** (all gated behind `ARM_EVICT_TEST_MODE=1`):
`ARM_FAKE_FOOTPRINT_TRAJECTORY_JSON` (`{ "<pid>": [<sampleMb>, ...] }`),
`ARM_FAKE_WORKTREE_MTIME_JSON` (`{ "<pid>": <epochMs> }`), plus every
`--evict-pid` seam already listed above (threaded through unchanged since
this beat calls `performEviction` in-process).

**Log shape and how to query it.** Every detection this beat makes is
appended to the report-consumable liveness log (`ARM_LIVENESS_LOG_FILE`,
default `~/.claude/agent-state/liveness-log.json`) via `lib/liveness-log.mjs`
— **NDJSON, one self-contained JSON object per line, not a single JSON
array** — so the log stays `grep`/`jq -c` friendly. Each entry carries
`schemaVersion`, `detectionType` (`'watchdog-trip' | 'aging-escalation' |
'deadlock-recycle'`), `timestamp`, the affected
`pid`/`orchestratorId`/`queueItemId`, `outcome`, and an optional `detail`
object.

The live file is capped at `LIVENESS_LOG_MAX_ENTRIES` (2000) entries. Once
an append would push it past the cap, the oldest overflowed entries roll
into a sibling archive file at `<path>.1`, itself bounded to the same cap.
Query examples:

```bash
# Every detection across both the live file and its archive.
cat ~/.claude/agent-state/liveness-log.json{.1,} 2>/dev/null

# Every real recycle this host has ever attempted (live + archive).
grep -h '"detectionType":"watchdog-trip"' \
  ~/.claude/agent-state/liveness-log.json{.1,} 2>/dev/null | jq -c .

# Every deadlock-tripwire recycle (live + archive).
grep -h '"detectionType":"deadlock-recycle"' \
  ~/.claude/agent-state/liveness-log.json{.1,} 2>/dev/null | jq -c .

# Count aging escalations by orchestrator (live + archive).
grep -h '"detectionType":"aging-escalation"' \
  ~/.claude/agent-state/liveness-log.json{.1,} 2>/dev/null \
  | jq -r '.orchestratorId' | sort | uniq -c
```

Programmatic access: `readLivenessLog(logFilePath)` (`lib/liveness-log.mjs`)
reads and parses every line of the **live file only** back into an array of
entries, tolerating a not-yet-created file (`[]`) — it never reads the `.1`
archive.

## Leak-velocity circuit breaker — `leak-block` detection + throttled eviction

A second, independent signal sits alongside the per-agent stall judgement
inside `--watchdog-beat`: **leak-velocity**, a sustained MB/min growth
reading derived from the same `phys_footprint` trajectory `--poll-footprint`
already harvests.

**Two starting-hypothesis defaults:**
- `DEFAULT_LEAK_VELOCITY_THRESHOLDS` (`lib/leak-velocity.mjs`) —
  `{ thresholdMbPerMin: 10, consecutivePollsRequired: 4 }`. A pid trips only
  after 4 consecutive above-threshold polls; a single below-threshold (or
  unmeasurable) poll resets the streak to 0.
- `DEFAULT_MAX_LEAK_VELOCITY_EVICTIONS_PER_BEAT` (`lib/watchdog-beat.mjs`) —
  `3`. A per-orchestrator-per-beat cap on how many pids may be EVICTED via a
  `leak-velocity`-only trip in a single `--watchdog-beat` invocation — NOT a
  fleet-wide cap. Excess trips beyond the cap are excluded from eviction
  (sorted growth-rate descending, pid ascending tie-break) and logged with
  `outcome: 'leak-velocity-throttled'` instead of acted on.

**Detection and eviction are different code paths:**
- **Detection-only, advisory:** every `--desired-agents`/`--heartbeat`
  beat's JSON body carries an additive `leakBlocks` array
  (`computeLeakBlocksForBeat` in `cli.mjs`), mirroring
  `pressure-block`/`pauseCandidate`'s observation-only posture. It does
  **not** gate new-agent admission. `leakBlocks` is fleet-wide, not scoped
  to the calling orchestrator's own owned pids — a listed pid may belong to
  ANOTHER orchestrator entirely. Any consumer must independently verify
  ownership/authorization for a listed pid before acting on it.
- **Real eviction:** happens exclusively through `--watchdog-beat` →
  `watchdogTrips` → `resolveEvictionTargets` → `handleEvict`.

**Precedence, corroboration, and dedup:**
- A pid tripping BOTH `isAgentStalled` and leak-velocity in the same beat
  produces exactly one trip entry — `stalled` wins. The leak-velocity
  evidence is not discarded: the winning trip carries
  `corroboratingSignals: [{ reason: 'leak-velocity', growthRateMbPerMin }]`.
- An uncorroborated leak-velocity trip is still an accepted, on-its-own
  basis for real eviction — leak-velocity's own `consecutivePollsRequired`
  sustained-window rule already provides corroboration-over-time.
- `growthRateMbPerMin` is durably logged for every leak-velocity-attributed
  trip — evicted, throttled, or corroborating — so the thresholds can
  eventually be tuned from real production data.
- The per-beat throttle is closed against a deadlock-tripwire side door: a
  throttled pid is filtered out of the MERGED output of both resolution
  paths before the eviction loop runs.

## Swap-fraction gate — a small demand-grown `swapTotalMb` no longer pins the memory axis at AMBER

`classifyMemoryRaw` (`lib/threshold.mjs`) computes `swapUsedFractionOfTotal
= swapUsedMb / swapTotalMb` and feeds it into the fractional GREEN/RED gates
alongside the absolute-MB thresholds. Because macOS grows its swapfile on
demand, a perfectly healthy, idle host can briefly report a tiny
`swapTotalMb` (a few hundred MB) — any nonzero `swapUsedMb` against that
small a denominator produces a wildly inflated fraction, which can trip the
fractional AMBER/RED gates despite negligible absolute swap usage.

Two named constants (`lib/threshold.mjs`), illustrative starting points:
- **`MIN_CONFIDENT_SWAP_TOTAL_MB`** (`512`) — a floor on `swapTotalMb`'s own
  magnitude. Below this floor, `classifyMemoryRaw` skips the fractional
  gates entirely and falls back to the absolute-MB/`pressureLevel` checks
  only. **This deliberately widens a false negative below the floor**: a
  tiny `swapTotalMb` can never exceed the default absolute RED/GREEN lines
  (10240/4096 MB) even at 90%+ saturation, classifying GREEN on the
  absolute checks alone. This is the accepted trade-off the floor exists
  for; without it, the transient sub-512MB demand-grown swapfile window
  reopens the same false-positive it was meant to fix.
- **`GREEN_FRACTION_NOISE_TOLERANCE_MB`** (`512`) — a separate, deliberately
  distinct constant: an absolute-MB noise-tolerance band added on top of
  the fraction-derived GREEN ceiling, clamped to the midpoint between the
  green and red fraction lines so it can never grow large enough to reach
  or exceed the red line.

A genuinely pressured host with a *large* `swapTotalMb` and high
absolute+fractional swap usage still classifies AMBER/RED unchanged — this
narrows a false positive (idle host, noise-scale swap-fraction inflation)
without widening a false negative for a real, sustained swap-pressure
episode (with the one documented exception above, below the 512MB floor).

## Opt-in idle-calibration probe — `--user-attests-idle`

A mechanism for reducing false-conservative AMBER holds: an explicit,
opt-in way for a user to assert "this host is genuinely idle right now" and
let the machine safely re-test that assertion, rather than staying pinned
at `hold` until the next organic GREEN sample arrives on its own.

**This is an explicit, one-shot user assertion — never inferred
automatically.** `--user-attests-idle` (bare-or-`=true` boolean-flag
convention, off by default) must be re-passed on every invocation that
wants its effect; nothing remembers a prior attestation across beats or
silently carries it forward.

**This flag must only ever be passed after the user has explicitly
confirmed, in the current conversation, that they observe the host is
idle — an orchestrator or agent must never self-attest on its own
judgment.** The entire safety argument rests on `--user-attests-idle` being
a HUMAN assertion, not an inference an orchestrator draws from metrics it
can already see and then builds into the command line itself.

**Safety invariant — additive only, never a bypass.** This mechanism is
reachable ONLY by falling all the way through the unmaskable pressure-WARN
admission block described above. At `pressureLevel >= 2` (WARN or CRITICAL)
the flag has **zero effect** — the pressure-block `return` has already
unwound the beat before this code is ever reached. This mechanism never
weakens, removes, or bypasses any existing automatic gate — it only ever
acts on a beat that was already going to report the ordinary `hold`
(AMBER, below-WARN) verdict.

**`hold` is not by itself the safe-to-attest case.** `buildTrafficLight`
returns `hold` for three different underlying reasons, and only the first is
the genuine "known, below-WARN memory pressure" situation this section
describes: (1) a real, finite, non-stale memory reading that classified
AMBER — the documented case; (2) a **stale/unknown** memory sample — "the
real pressure state was never observed", not "observed and safe"; (3) a
**disk**-AMBER reading with memory GREEN — `--user-attests-idle` is a
memory-idleness assertion and says nothing about disk headroom. `cli.mjs`'s
admission block gates on the narrower `isGenuineMemoryAmberHold` condition,
not merely `trafficLight.type === 'hold'` — a flag set on a stale-sample
hold or a disk-only hold has zero effect.

**Admission.** When the flag is set and this beat is a genuine, known,
below-WARN memory-AMBER `hold`, `cli.mjs` claims a single fixed slot
against a dedicated `IDLE_PROBE_CLAIM_TYPE` operation type (`'idle-probe'`)
via the existing `claimCapacity` atomic deny-the-remainder primitive,
scoped to capacity `1`. A first eligible beat that finds the slot free is
admitted — `trafficLight` is rewritten to `{ type: 'spawn-allowed',
allowance: 1 }` plus a sibling `idleProbe: { agentClass: 'idle-probe' }`
field. A second, concurrent attestation while that slot is still held finds
it already claimed and is refused: `{ type: 'hold', idleProbeRefused: true
}` — the same ordinary `hold` shape plus this one distinguishing field. An
attestation on an ineligible hold is silently a no-op — the beat reports the
ordinary verdict with no `idleProbe`/`idleProbeRefused` field.

**The slot is released ONLY via its six-hour TTL — there is no early-release
path wired into the sampling loop today.** `applyIdleProbeOutcome`
(`lib/idle-probe-trajectory.mjs`) is the only code that releases
`IDLE_PROBE_CLAIM_TYPE` early, and nothing in `cli.mjs`'s `main()` calls
it — the module is a pure, fully-unit-tested classification/consequence
pair with no caller wired into the per-beat sampling loop yet. A refused
second attestation keeps being refused for up to six hours after the first
slot was claimed, regardless of whether the in-flight probe has actually
finished.

`IDLE_PROBE_CLAIM_TYPE` is internal-only, mechanically enforced: both a
public `--claim=idle-probe:N` and `--release=idle-probe:N` are rejected
with exit code 2.

**Trajectory classification** (`classifyIdleProbeTrajectory`,
`lib/idle-probe-trajectory.mjs`) — a pure function deciding which outcome an
admitted idle-probe's bounded observation window landed in, using the same
`computeLeakVelocity` circuit-breaker mechanism the leak-velocity section
above uses. Five outcomes, four terminal:
- **`degrading`** — leak-velocity trips; releases the claim, no further
  consequence.
- **`inconclusive`** — the window elapses with fewer than the configured
  minimum valid polls gathered; releases the claim, no further consequence.
- **`flat`** — the window completes with no leak-velocity trip and no
  measurable footprint delta; feeds `computeAimdCeiling` using the SAME
  additive-increase path an organic sustained-normal beat already uses.
- **`new-plateau`** — the window completes with a measurable footprint
  delta at or above the configured plateau threshold; records the
  empirical delta via `recordObservation`, tagged `operationType:
  'idle-probe'` / `agentClass: 'idle-probe'` so it can never contaminate
  the probed agent class's own history.
- **`observing`** — non-terminal: no leak-velocity trip yet, and the
  window hasn't elapsed; signals "keep polling, no verdict yet".

**This closes the loop only at the pure-classification-function level.** An
admitted idle-probe agent is genuinely spawned, but nothing today
automatically observes it, classifies its trajectory, or applies the
consequence described above beat-by-beat — that observation loop is a
follow-up, not implied to be end-to-end operational.

## Recording outcomes and querying capacity

Alongside the spawn-decision beat above, `cli.mjs` supports two more
standalone, non-spawn-decision beat types that together move this skill
from flat per-axis caps toward evidence-based, per-operation-type capacity:
`--record-outcome` (the WRITE half — persist what actually happened) and
`--query-capacity` (the READ half — ask how much headroom a given operation
type has right now, given its own recorded history). `--claim` (below) is
the third, authoritative-write half of the same evidence loop, closing the
race window `--query-capacity` alone cannot.

### `--record-outcome` — persisting a real observation

```bash
node scripts/agent-resource-management/cli.mjs \
  --record-outcome \
  --operation-type=<type> \
  --session-id="$CLAUDE_CODE_SESSION_ID" \
  [--peak-memory-mb=<n>] \
  [--crashed] \
  [--pid=<pid>]
```

- **Required**: `--operation-type=<type>` and `--orchestrator-id=<id>` —
  missing either hard-rejects with a stderr message and **exit code 2**,
  before any file I/O is attempted.
- **`--peak-memory-mb=<n>`** (optional) — a numeric peak-memory observation
  for this run; a non-numeric value hard-rejects (exit 2) rather than being
  silently coerced or dropped.
- **`--crashed`** (optional, boolean-flag) — validated via the same
  `validateBareOrTrueFlag` helper `--heartbeat` uses: only bare `--crashed`
  or `--crashed=true` are valid; every other form hard-rejects (exit 2)
  rather than being silently coerced through truthiness.
- **`--pid=<pid>`** (optional) — the **poll-harvest linkage** to any prior
  `--poll-footprint` beats (below) for the same pid. When one or more such
  beats exist, this call atomically reads-and-evicts
  (`consumeFootprintState`, `lib/footprint-state.mjs`) that pid's persisted
  EWMA-smoothed footprint state and threads it into this observation: the
  real `startedAt` (the timestamp of the FIRST poll for that pid, replacing
  this beat's own degenerate `startedAt = endedAt = now` default), a
  `peakSmoothedFootprintMb` field, a `footprintTrajectory` array, and
  `agentClass` if any poll for that pid supplied `--agent-class`.
  **Without `--pid`, none of this happens** — polling alone accomplishes
  nothing durable; `--record-outcome --pid=<pid>` is what harvests it into
  history.
- **Never composable** with `--desired-agents` or `--heartbeat` in the same
  invocation.
- **Storage**: persists via `lib/history.mjs`'s `recordObservation`,
  appending to a per-`operationType` bounded raw log in a SEPARATE JSON file
  from the coordination file — `~/.claude/agent-state/resource-history.json`
  by default (`ARM_HISTORY_FILE` overrides for tests).
- **`peakMemoryMb` vs. `peakSmoothedFootprintMb`** — two different metrics,
  never conflated. `peakMemoryMb` is populated ONLY from the
  `--peak-memory-mb` flag above — `estimateOperationCost`'s median/
  percentile logic reads exclusively this field. `peakSmoothedFootprintMb`
  is populated ONLY from a linked `--poll-footprint` series's accumulated
  EWMA-smoothed `phys_footprint` peak, via `--pid` above.
- **Fails open** — a history-file write failure degrades to a stderr
  warning, still exits 0, still prints a best-effort confirmation.
- **Output**: `{"recorded":true,"operationType":"<type>","orchestratorId":"<id>"}`
  on stdout.

### `--poll-footprint` — per-pid physical footprint polling

```bash
node scripts/agent-resource-management/cli.mjs \
  --poll-footprint=<pid> \
  --session-id="$CLAUDE_CODE_SESSION_ID" \
  --operation-type=<type> \
  [--agent-class=<label>]
```

A **one-shot** beat — like every other beat, `cli.mjs` never loops or
self-schedules. An external orchestrator re-invokes this beat every 10-30s
per live pid on its own wall-clock timer; each invocation samples the given
pid's physical footprint exactly ONCE and folds it into a per-pid
EWMA-smoothed/trajectory state that persists across those separate
invocations (`lib/footprint-state.mjs`, `recordFootprintPoll`).

- **`--poll-footprint=<pid>`** — required; `<pid>` must be a positive
  integer, or this hard-rejects (exit 2) before any sampling or file I/O.
- **Required**: `--orchestrator-id=<id>` and `--operation-type=<type>`.
- **`--agent-class=<label>`** (optional) — the spawning agent's label (e.g.
  `implementer`). Persisted into the per-pid state entry, last-writer-wins
  across repeated polls of the same pid. Threaded through to the eventual
  observation's `agentClass` field by a linked `--record-outcome
  --pid=<pid>` call.
- **Storage**: `ARM_FOOTPRINT_STATE_FILE` (env override) —
  `~/.claude/agent-state/footprint-state.json` by default. Concurrent polls
  are serialised via the same exclusive-create fencing lock the
  coordination file uses.
- **PID-reuse staleness guard** — a pid whose `lastPolledAt` predates this
  poll by more than `DEFAULT_LIVENESS_THRESHOLD_MS` (15 minutes) is treated
  as a FRESH run: its prior smoothed/peak/trajectory/`agentClass` are
  discarded rather than folded into what is very likely a different,
  earlier process that happened to reuse the same pid (macOS recycles
  pids). Also opportunistically prunes any OTHER similarly-stale entry in
  the store on the same write.
- **Fails open** — a state-file write failure degrades to a stderr
  warning, still exits 0.
- **Output**:
  `{"polled":true,"pid":<pid>,"orchestratorId":"<id>","operationType":"<type>","agentClass":<label|undefined>,"sampleMb":<number|null>}`
  on stdout. `sampleMb` is `null` when both `footprint` and its `vmmap
  --summary` fallback failed (missing binary, permissions, a pid that no
  longer exists).

### Operation-type history retention

`lib/history.mjs` bounds each operation type's stored history forever: raw
observations are kept up to a `retentionCap` (50 by default); once that cap
is exceeded, the OLDEST raw entry folds into a rolling `{ count, sum, max,
reservoir }` accumulator instead of being dropped or growing the file
unboundedly. Only CLEAN observations fold: an evicted entry that is
`crashed: true`, or that carries no finite `peakMemoryMb`, is evicted from
`raw` but contributes nothing to the summary. `reservoir` is a
fixed-capacity (500) uniform random sample (reservoir sampling, Algorithm
R) of every folded peak ever seen, used only to *estimate* p90 at read
time — exact whenever fewer than 500 observations have ever folded.
`readHistory` always returns an explicit `{ raw: [], summary: null }` for a
cold-start type — never `undefined`. Concurrent writes for the same type
reuse the coordination file's existing exclusive-create fencing lock
mechanism.

### `--query-capacity` — the advisory read

```bash
node scripts/agent-resource-management/cli.mjs \
  --query-capacity=<comma-separated-types>
```

- **Syntax**: one or more non-empty, comma-separated operation-type
  strings — `--query-capacity=implementer,reviewer`. Bare
  `--query-capacity`, `--query-capacity=` (empty), or a value that is only
  commas/whitespace all hard-reject (exit 2).
- **No `--orchestrator-id` required** — a pure advisory read that never
  declares dibs.
- **Never composable** with `--desired-agents`, `--record-outcome`, or
  `--heartbeat` in the same invocation.
- **Axis-gated**: both axes GREEN gates whether ANY type gets non-zero
  `freeCapacity` at all — an AMBER/RED axis zeroes `freeCapacity` for EVERY
  requested type in the same response; `confidence` per type stays
  independently correct regardless.
- **A never-seen operation type** is treated as a fresh cold-start type
  (`confidence: 'default'`) — never rejected as invalid input.
- **Per-type fail-open on a corrupted history read** — one type's
  history-read failure degrades that type only.
- **Output**: `{"<type>":{"freeCapacity":<n>,"confidence":"empirical"|"default"},...}`
  on stdout, one entry per requested type. Every requested type currently
  falls back to the flat `DEFAULT_ALLOWANCE_CONFIG.maxAgentsMemoryAxis` (3)
  placeholder cap when both axes are GREEN (`0` otherwise) — the same
  `freeRamMb`-inversion issue documented under "Memory-axis headroom"
  above applies here too, so this is a placeholder pending a real
  live-agent-ceiling-backed formula, not a permanent design.
- **Does not affect `diskTrend`** — `diskTrend` is only ever computed and
  attached on a normal spawn-decision beat.

### `--advise-only` — a non-mutating spawn verdict

```bash
node scripts/agent-resource-management/cli.mjs \
  --session-id="$CLAUDE_CODE_SESSION_ID" --advise-only
```

- **What it is for**: answering "would a spawn be allowed right now?" from a
  code path that is not itself performing the spawn. Its one production
  caller is the ARM PreToolUse gate (see below). Orchestrators must NOT use
  it in place of their normal beat — a real beat is still how intent gets
  declared.
- **Runs the same pipeline** as the ordinary `--desired-agents` beat:
  `collect` → `classifyMemoryAxis`/`classifyDiskAxis` → dibs READ → memory-
  projection check → `buildTrafficLight`. Same verdict vocabulary.
- **Writes nothing, and locks nothing.** No `declareDibs`, no
  shared-machine-sample write, no AIMD ceiling advance and no cold-start
  seeding of the AIMD state row, no global spawn-rate token drawn, no
  live-agent admission claim, no ledger reconciliation sweep, and no
  coordination file, parent directory, `.lock` or `.tmp-*` sibling created.
  These properties are **unconditional** — they hold with the coordination
  file absent, populated, or corrupted.
- **Known fidelity limit**: the two host-wide admission-GRANTING gates (the
  spawn-rate bucket and the live-agent ceiling) are consumption-based and
  cannot be probed without taking from them, so an advisory run skips both.
  It reports `liveAgentGrant: null` and never applies the bucket's denial
  clamp — i.e. it can be MORE permissive than a real beat, never less. The
  orchestrator's own beat remains the authority for those two gates.
- **The memory-projection arithmetic covers the consulting session's own
  running fleet, with a staleness cliff.** An advisory caller supplies no
  `--running-agent-classes`, so this path falls back to the session's own
  dibs entry's declared `agentClasses`. That dibs entry is read through a
  15-minute liveness-filtered read: agents started since the session's last
  real beat are uncounted between beats, and once the row itself ages past
  15 minutes it is pruned and the memory-projection axis reverts wholesale
  to pricing this session's entire running fleet at zero — until its next
  real beat. The candidate class itself is priced as the unlabelled
  catch-all when no `--agent-class` is supplied.
- **A memory-RED advisory verdict reports `pauseCandidate: null`.** The
  `type` is still `pause`, but the candidate-selection step (a second `ps`
  collection plus a per-running-agent footprint shell-out chain) is
  skipped, so an advisory consultation on a memory-RED host cannot blow a
  caller's timeout budget.
- **`spawn-allowed` is not on its own a permit.** Read the grant:
  `allowance > 0`. `{"type":"spawn-allowed","allowance":0}` is an explicit
  "you may spawn zero agents".
- **Not composable with any other beat type** (exit 2).
- **`--desired-agents=N` is still honoured** as the hypothetical ask when
  supplied (defaulting to 1), and is used only to size the in-memory
  projection — never written anywhere.

## The PreToolUse gate — opt-in-but-sticky

Every ARM mechanism documented above — admission control, the durable
queue, the dispatcher, the watchdog, the leak-velocity circuit breaker —
only runs if a session *chooses* to invoke `cli.mjs`. Nothing above
mechanically compels that consultation.
`scripts/agent-resource-management/hooks/pretooluse-arm-gate.mjs`, wired
into `.claude/settings.json` as a `PreToolUse` hook matching `Agent`/`Task`,
closes that gap for any session that has ever consulted ARM at least once —
without imposing a standing cost on sessions that never touch this skill at
all. Read the hook file's own header comment for the authoritative,
review-hardened account; this section is a pointer into it.

- **Opt-in, not mandatory.** A session that never runs a `cli.mjs` beat is
  never gated.
- **Both sides derive the key; neither side hand-picks it.** The
  orchestrator passes `--session-id="$CLAUDE_CODE_SESSION_ID"` and
  `cli.mjs` derives; the hook reads `session_id` off its own PreToolUse
  payload and derives; both call the same pure `deriveOrchestratorId`. An
  orchestrator that instead passes a hand-picked `--orchestrator-id` opts
  *itself* out — its dibs land under one key while the hook looks up
  another, `hasEverOptedIn` returns `false` forever, and every gated spawn
  takes the fail-open path with no error anywhere.
- **Sticky once opted in.** The first `--desired-agents` (or `--heartbeat`)
  beat a session ever runs durably records that session's derived
  orchestrator-id in the coordination file's `EVER_OPTED_IN_RESERVED_ID`
  reserved entry. A later `pruneStale` sweep or a stray `--release` on that
  session's live dibs row does **not** un-stick it — the gate's question is
  "has this session *ever* declared dibs", not "does it have a live entry
  right now". This reserved entry ages out ids untouched for more than
  `EVER_OPTED_IN_MAX_AGE_MS` (30 days) and enforces an
  `EVER_OPTED_IN_MAX_RECORDS` (256) count-cap backstop, oldest-`lastSeenAt`-
  first.
- **Three tiers:**
  - **tier 0 — not a gated spawn event** (unparseable stdin, a different
    hook, a non-spawn tool) — allow, having read and spawned nothing. This
    is the hot path for every tool call on the host and must stay
    genuinely free.
  - **tier 1 — a gated spawn from a session that never opted in** — allow,
    via the lock-free, fail-open `hasEverOptedIn` read. An underivable
    session identity lands here too.
  - **tier 2 — a gated spawn from an opted-in session** — consult `cli.mjs
    --orchestrator-id=<derived> --advise-only` (5s timeout, read-only) and
    honour its verdict: `spawn-allowed` with a non-zero grant allows; any
    other well-formed verdict is a policy deny.
- **Fail-open when not opted in; fail CLOSED once opted in.** For an
  opted-in session, a `cli.mjs` crash, non-zero exit, empty/unparseable
  output, missing binary, or timeout is a deny, not an allow — a deliberate
  reversal of the fail-open default everywhere else in this skill, scoped
  specifically to the opted-in case. Every such fail-closed trip appends
  one `arm-gate-fail-closed` entry to the liveness log, so the path's real
  firing rate is auditable rather than a transient stderr line. Policy
  denials are deliberately **not** logged into that bucket.
- **What it does not enforce.** `--advise-only` cannot probe the two
  consumption-based, host-wide admission-granting gates — the global
  spawn-rate bucket and the live-agent concurrency ceiling — without
  mutating shared state, so it does not try. This gate therefore closes the
  "beat 1 consulted, beats 2-N unchecked" gap only on the memory, pressure,
  and disk axes (plus the dibs-share/memory-projection arithmetic derived
  from them); spawn-rate and live-agent concurrency remain enforced only by
  the orchestrator's own real beat.
- **Opting out.** Removing the `PreToolUse` entry for
  `pretooluse-arm-gate.mjs` from `.claude/settings.json` is a complete,
  clean off-ramp — the only residue is the inert hook entry itself; no
  marker file or ledger state needs cleaning up afterward.
- **`.claude/settings.json` changes deserve real review scrutiny.** Every
  `command` field in that file executes as a shell command on every
  matching tool call in every session against your project — treat edits
  to it with the same care you'd give a CI workflow file.

### Empirical cost estimation — `estimateOperationCost`

`lib/cost-estimate.mjs`'s pure `estimateOperationCost(operationType,
history)` turns recorded history into the per-type `perAgentRamMb` figure
`--query-capacity`/`--claim` both divide headroom by:

- **Cold start** (no `summary` and fewer than 3 clean raw samples) returns
  the flat `defaultPeakMemoryMb` constant (350MB, matching
  `DEFAULT_HEADROOM_CONFIG.perAgentRamMb`) with `confidence: 'default'`.
- **Warmed up** (`>= 3` clean raw samples, or a populated rolling
  `summary`) returns `confidence: 'empirical'`. Evidence precedence is
  most-recent-first: (1) `>= 3` clean raw samples → the MEDIAN of those
  peaks; (2) otherwise a populated `summary` → `summary.p90PeakMemoryMb`.
  The ordering is deliberate — `summary` holds ONLY observations already
  aged out of `raw` past the retention cap, so it is strictly the OLDER
  half of the record; consulting it first would freeze the estimate at
  pre-cap history forever past the 51st observation.
- **`crashed: true` observations are excluded from the central estimate
  entirely** — never silently averaged in. Their count is still surfaced
  via a distinct `crashedObservationCount` field.
- **Observations with no `peakMemoryMb`** carry no cost signal and are
  excluded from the clean-sample set entirely.

## Claiming capacity

Alongside dibs-pacing and the global spawn-rate bucket above, `cli.mjs`
supports a third, structurally similar shared-state beat type — a
per-operation-`type` claim ledger, `claimCapacity`
(`lib/coordination-file.mjs`), invoked via a standalone `--claim` flag:

```bash
node scripts/agent-resource-management/cli.mjs \
  --session-id="$CLAUDE_CODE_SESSION_ID" \
  --claim=<type>:<count>
```

- **Syntax**: `--claim=<type>:<count>` — `<type>` is a non-empty operation
  type string, `<count>` is a non-negative integer. Malformed input
  hard-rejects (exit 2). `--claim` also requires `--orchestrator-id=<id>`,
  and cannot be combined with `--release`, `--desired-agents`,
  `--query-capacity`, `--record-outcome`, or `--heartbeat` in the same
  invocation.
- **`orchestratorId` is caller-asserted, not authenticated.** Neither
  `--claim` nor any other beat verifies the supplied `--orchestrator-id`
  against a signature, token, or process identity — any caller can claim
  (or release) under any non-reserved id it types. This is safe only
  because every caller is a cooperating local process sharing the same
  host/user, not an untrusted network principal; it is not a security
  boundary and must never be treated as one.
- **Deny-the-remainder grant semantics.** `--claim` derives the currently-
  available flat cap for `<type>` (see "Query-capacity" above for the
  current formula), then grants `granted = min(requestedCount, remaining)`,
  where `remaining = max(0, availableCapacity - alreadyGrantedTotal)` and
  `alreadyGrantedTotal` accumulates across every prior successful claim for
  this `type`, from any orchestrator, for as long as the ledger entry
  persists. This closes the race window a `--query-capacity` advisory read
  alone cannot: two concurrent `--claim` calls for the last free slot of
  one `type` can never both read the same pre-mutation ledger state and
  independently grant a combined amount that exceeds `availableCapacity`.
  Output on stdout is `{"granted":<n>}`.
- **The ledger's read-modify-write under the lock is what makes this
  safe — not the host probe running under it.** The host sample, axis
  classification, history read and cost estimate all run BEFORE
  `claimCapacity` is called; only `alreadyGrantedTotal`'s read/apply/write
  happens inside one critical section. The sample is stamped when TAKEN and
  the in-lock check fails closed (`granted: 0`) past
  `DEFAULT_CLAIM_SAMPLE_FRESHNESS_MS` (20s) — `--claim` uses a tighter lock
  retry ceiling (`DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS`, ~8s of retry sleep) so
  the freshness window clears the lock wait with real margin. A non-GREEN
  beat short-circuits before the lock and never creates `<path>.lock` at
  all.
- **Fails CLOSED — `granted: 0` on any coordination-file error.** `--claim`
  is the authoritative WRITE half of this skill's atomic-slot-reservation
  contract; silently granting the full ask on a lock/coordination failure
  would defeat the race-safety property the deny-the-remainder semantics
  exist for. The dividing rule across this whole skill: **a path that
  GRANTS capacity fails closed; a path that merely records or shares
  information fails open.** Fail closed: `--claim`, `--release`, and the
  global spawn-rate bucket. Fail open: the shared-machine-sample write, the
  pre-beat `readDibs`, the post-declare `readDibs` read-back, and the
  history write.
- **Grants are not one-way / cumulative-only.** `--release=<type>:<count>`
  returns previously-claimed slots to the same per-`type` ledger `--claim`
  writes to (see "Releasing capacity" below).
- **TTL-based automatic expiry.** A claim record whose age exceeds
  `DEFAULT_CLAIM_TTL_MS` (6 hours) is excluded from
  `alreadyGrantedTotal`/`grantedTotal`'s aggregate on the next read, with no
  explicit `--release` required, and is physically pruned from the
  persisted `claims[]` array on the next write that touches that ledger
  entry.
- **Known gap — TTL is a fixed window stamped once at claim time, not a
  liveness signal.** `claimedAt` is never refreshed — there is no
  heartbeat/renewal call that extends a still-in-use claim's TTL. A single
  sub-agent task that genuinely runs longer than 6 hours will have its
  claim silently excluded and pruned even though it is still actually using
  that capacity, letting a different orchestrator claim into the same
  capacity — a real double-grant, triggered by elapsed time. Accepted as a
  residual risk for this skill's actual deployment (local dev tooling
  pacing cooperating processes on one host, not a production capacity
  system).
- **Known gap — a crash between the ledger write and the grant
  acknowledgement can strand a slot with no confirmation either way.** If
  the process running `--claim` is killed after the atomic `rename()`
  completes but before the `{"granted":<n>}` stdout write executes, the
  slot is already consumed from the ledger, but the calling orchestrator
  never receives confirmation. It can neither safely retry nor safely
  assume denial. An orchestrator that suspects it hit this window can issue
  an explicit `--release` once it has reconciled its own state — that
  release is scoped to the caller's own `orchestratorId` (see below), so
  its blast radius is bounded.

## Releasing capacity

`--release=<type>:<count>` (backed by `releaseCapacity` in
`lib/coordination-file.mjs`) is the counterpart to `--claim` above: it
decrements the same per-`type` claim ledger entry, returning
previously-granted slots to the pool. The decrement is scoped to the
calling orchestrator's OWN previously-granted slots.

```bash
node scripts/agent-resource-management/cli.mjs \
  --session-id="$CLAUDE_CODE_SESSION_ID" \
  --release=<type>:<count>
```

- **Syntax**: same shape as `--claim`. Malformed input hard-rejects (exit
  2). `--release` also requires `--orchestrator-id=<id>`, and cannot be
  combined with `--claim`, `--desired-agents`, `--query-capacity`,
  `--record-outcome`, or `--heartbeat` in the same invocation.
- **`orchestratorId` is caller-asserted, not authenticated** — same posture
  as `--claim`.
- **Orchestrator-scoped floor-at-0 decrement semantics.** The ledger entry
  persists an array of per-claim records, `claims: [{ orchestratorId,
  count, claimedAt }, ...]` — one record per successful `--claim` grant,
  not a single cumulative scalar. `releaseCapacity` sums only the records
  whose `orchestratorId` matches the CALLING orchestrator (`ownHeld`), then
  computes `released = min(releaseCount, ownHeld)` and writes back the
  caller's own record(s) shrunk (or dropped, if fully consumed) by that
  amount — every other holder's records are left untouched. A caller
  releasing more than its own share, or a stale double-release, simply
  releases everything it itself holds and reports the true (floored)
  amount on stdout as `{"released":<n>}`. Releasing against a `type` with
  no ledger entry at all, or one where the caller holds nothing, is
  "nothing to release" (`released: 0`), never an error and never a
  decrement to anyone else's claims — orchestrator B can no longer release
  (and then re-claim) capacity orchestrator A holds. `grantedTotal` on the
  response remains the cross-holder aggregate.
- **Fails CLOSED — `released: 0` on any coordination-file error**, matching
  `--claim`'s own posture for the identical reason.
- **Known gap** — same fixed-TTL-not-a-liveness-check caveat as "Claiming
  capacity" above.

## Disk decline-rate trend

Every `cli.mjs` beat's normal spawn-decision JSON carries a top-level
`diskTrend` sibling field alongside the traffic-light shape:

```json
{ "type": "spawn-allowed", "allowance": 3, "diskTrend": { "trend": "declining", "declineRateGbPerHour": 4.5 } }
```

- **Shape**: `diskTrend: { trend, declineRateGbPerHour }`. `trend` is one of
  four literals — `"insufficient-data"` | `"declining"` | `"stable"` |
  `"recovering"`. `declineRateGbPerHour` is always a plain, finite number,
  never `NaN`/`null` — `0` whenever `trend` is `"insufficient-data"` or
  `"stable"`.
- **How it's computed** (`computeDiskTrend`, `cli.mjs`): from two real,
  timestamped `freeDiskGb` readings — this beat's own current reading, and
  a prior reading persisted in the reserved
  `__shared-machine-sample__` coordination-file entry (the SAME fields the
  freshness-window debounce already uses). `declineRateGbPerHour =
  (priorFreeDiskGb - currentFreeDiskGb) / elapsedHours`: positive means
  free space is shrinking (`"declining"`), negative means it's growing
  (`"recovering"`).
- **Recency window**: `DEFAULT_DISK_TREND_RECENCY_WINDOW_MS` (15 minutes) —
  a genuinely distinct constant from `DEFAULT_FRESHNESS_WINDOW_MS` (90s,
  governs whether a whole *reading* can be skip-sampled/reused verbatim by
  a different orchestrator's beat). No usable prior sample, a prior sample
  older than this window, or a non-positive elapsed gap all yield
  `"insufficient-data"`. **Rapid polling (every 15-30s) will show mostly
  `"insufficient-data"` by design** — this is not a bug to re-diagnose.
- **Noise-tolerance dead zone**: `freeDiskGb` is a whole-GB integer (`df
  -g`), so any two real samples can differ by a routine ±1GB of
  quantization noise with nothing meaningful actually trending.
  `computeDiskTrend` treats any raw `(priorFreeDiskGb - currentFreeDiskGb)`
  delta at or below `DISK_TREND_NOISE_EPSILON_GB` (1GB) as noise, reporting
  `"stable"` with `declineRateGbPerHour: 0` instead of flapping on rounding
  alone. This check is applied to the raw GB delta BEFORE it is divided by
  elapsed time, deliberately NOT as a rate threshold — a rate-based epsilon
  would let single-GB quantization noise leak through as a reported trend
  for almost every realistic short beat-to-beat gap.
- **Scope**: this substitution applies only to `diskTrend` on normal
  spawn-decision beats — `--query-capacity`, `--claim`, and `--release` say
  nothing about it.
- **Locked compare-and-swap on the shared-sample entry.** The persist for
  the reserved `__shared-machine-sample__` entry goes through
  `writeSharedSample` (`lib/coordination-file.mjs`), whose entire
  read-decide-write happens inside a single lock: it re-reads whatever is
  currently persisted, and only overwrites it when that entry is
  absent/degraded or the incoming reading's `sampledAt` is strictly newer
  than the persisted one's. Two orchestrators racing a fresh sample in the
  same narrow window can no longer clobber one another.

## Cleanup guidance — purging a finished/dead orchestrator's temp output

Disk exhaustion is driven by agent temp output that accumulates over a
session and is only reclaimed on reboot. This is the skill's **only
recursively-deleting code path**, which is why it is fenced by more gates
than anything else here. Below is what's actually built (the mtime-based
sweep of stale staging output); a purely ancestry-based route for purging a
*named dead orchestrator's* own attributable temp output was considered but
never built — the shipped sweep uses mtime instead, for the reason
explained below.

**mtime, not ancestry, is the applicable rule here.** An ancestry-based
route (walking `ps`-tree ownership to attribute temp paths to a specific
dead orchestrator, the way RSS accounting does) works when the creating
process's tree is still discoverable. It does not work for build-tool
staging output (e.g. bundler/synth scratch directories): those are created
by short-lived child processes that have already exited by the time
cleanup runs, so there is no live process tree left to attribute them to.
The substitute signal is mtime — nothing ever reopens or resumes reading an
old staging directory; a resumed or paused task triggers brand-new output
rather than a re-read of stale output, so an old mtime reliably means the
directory is dead weight. Age alone is not a safe rule for every artifact
type, though — a genuinely resumable artifact (a worktree, saved session
state) can sit untouched for hours while legitimately paused; a future
cleanup mechanism targeting that category should use a sentinel/marker
placed on creation and removed on completion, not pure mtime.

### The disk-axis sweep — what actually runs on a beat

The sweep is wired into the disk axis: an ordinary beat on a host with a
declining disk trend can reclaim stale temp output and report what it
reclaimed in the same verdict.

- **It is OFF until you arm it, per orchestrator.** Arming is a separate,
  deliberate act — never a side effect of any other beat:

```bash
node scripts/agent-resource-management/cli.mjs \
  --enable-disk-cleanup \
  --session-id="$CLAUDE_CODE_SESSION_ID"
```

```bash
node scripts/agent-resource-management/cli.mjs \
  --disable-disk-cleanup \
  --session-id="$CLAUDE_CODE_SESSION_ID"
```

- **`--enable-disk-cleanup`** writes the `__disk-cleanup-armed__` row and
  does nothing else, emitting one JSON document and exiting 0.
  `armed: true` means "newly armed by this call"; `false` means it was
  already armed (idempotent — still refreshes `lastSeenAt`).
  **`--disable-disk-cleanup`** stands the sweep back down. The armed state
  is a coordination-file row deliberately DECOUPLED from `declareDibs`, so
  opting an orchestrator into pacing never implicitly opts it into
  deletion.
- **Both flags are BARE-ONLY.** `--enable-disk-cleanup=false` — the string
  an operator reaches for when they mean "turn it off" — is a hard **exit
  2**, not a silent arming. Use `--disable-disk-cleanup` to stand the sweep
  down.
- **Arming ages out after `EVER_OPTED_IN_MAX_AGE_MS` (30 days).** An
  orchestrator armed and then left alone for 30 days stops sweeping,
  silently — the beat simply emits no `cleanup` field. Re-arm with one more
  `--enable-disk-cleanup` beat.
- **Three gates must all pass before a single directory is listed**, in
  order, cheapest first:
  1. **`--advise-only` suppresses the sweep unconditionally**, first and
     before anything else — the PreToolUse hook consults the CLI with
     `--advise-only`, so it can never trigger a sweep at all.
  2. **The `diskCleanupArmed` opt-in gate.** Not armed, no sweep.
  3. **Disk evidence must warrant it** — fires on exactly two routes: a
     `declining` trend (which fires **even while the disk axis is still
     GREEN** — reclaiming early is the cheap moment), or a `stable` trend
     at or below the GREEN free-space floor. `insufficient-data`,
     `recovering`, and a non-finite free-space reading all refuse. There is
     deliberately no "AMBER trend" route — deleting directories because the
     sampler could not read the disk would be action on the ABSENCE of
     evidence.
  4. Then, last, a **futile-sweep cooldown** (`SWEEP_COOLDOWN_MS`, 30 min) —
     the only gate costing a file read, and the only one that fails *open*:
     it stops a clean host paying the sweep budget forever after finding
     nothing to reclaim, but a corrupt byte can never be the reason a
     filling host stops reclaiming. It applies only to a sweep that ran to
     completion and found nothing to do — a budget-cut, candidate-capped,
     or live-writer-skipped sweep re-fires next beat rather than standing
     down.
- **The output is an additive `cleanup` field on the verdict JSON**, a
  sibling of `diskTrend` — never influences the verdict. An ABSENT
  `cleanup` field means the sweep never fired; a PRESENT one means it
  fired, with `removedCount`, `reclaimedBytes`, `completed`,
  `budgetExhausted`, `candidatesCapped`, `truncatedCount`,
  `sizeUnmeasuredCount`, `liveWriterSkippedCount`, `probeTruncatedCount`,
  `removalIssuedUnconfirmedCount`, and a bounded `errors` ring. A sweep
  that threw partway through still reports a present, shape-complete field.

**Constants** (all in `lib/cleanup.mjs`):
- **`SWEEP_BUDGET_MS` = 750** — a hard deadline on the sweep's own wall
  clock, enforced as a RACE rather than a loop-top check, sized against the
  PreToolUse hook's own timeout so a runaway sweep can never deny every
  agent spawn on the host by overrunning it.
- **`SWEEP_MAX_CANDIDATES` = 24** — per-beat work cap; a real backlog is
  drained across many bounded beats, not in one multi-gigabyte walk on the
  admission path.
- **`AUTO_TRIGGER_AGE_HOURS` = 6** — the staleness threshold for the
  unattended auto-trigger, deliberately MORE conservative than the 2h
  default of an operator-typed `--cleanup` command: nobody typed anything
  to start the auto sweep, so it must never be the more aggressive of the
  two.
- **`LIVE_WRITE_MARGIN_MS` = 15 min** — a candidate with a *descendant*
  touched within this margin is treated as still being written into and
  skipped. Root mtime alone is not a liveness signal: on POSIX a
  directory's mtime moves only when its own entry list changes.
- **`SWEEP_COOLDOWN_MS` = 30 min** is deliberately **twice**
  `LIVE_WRITE_MARGIN_MS` — a live-writer-skipped sweep is excluded from
  cooling down precisely so it doesn't out-wait the very margin that caused
  the skip.

**Two caveats:**
- **The beat's `diskTrend` predates its own sweep** — the trend reported
  alongside `cleanup` was sampled BEFORE any bytes were reclaimed, so a
  beat can correctly report both a declining trend and a large
  `reclaimedBytes`. The relief shows up on the next beat's trend.
- **Concurrent beats can double-count `reclaimedBytes`.** There is no
  cross-beat lock on the sweep — two orchestrators sweeping the same temp
  root in the same window may each report bytes the other also counted.
  Treat the figure as an indication that reclamation happened, not as an
  accounting total.

## Launchd watchdog backstop (optional)

`scripts/agent-resource-management/launchd/` ships an optional, opt-in
macOS `launchd` LaunchAgent template
(`com.example.arm-watchdog.plist.template` — rename/relabel it for your own
project before installing; see that directory's own README for the exact
steps) that invokes `lib/watchdog-beat-launchd.mjs` on a `StartInterval`
timer, independent of any orchestrator session being alive.

This is a **detection-only backstop with a scope narrower than "watchdog"
suggests**: because a launchd-spawned process has no `ps`-tree descendants,
`isEvictionAuthorized` can never authorize it to evict anything, so the
per-agent stall/leak-velocity EVICT step structurally cannot fire through
this path. Queue-aging escalation also does not fire through this path (the
same pre-existing "no historical capacity-sample source" gap documented
under "Watchdog beat" above). **Only the deadlock tripwire is confirmed to
provide genuine unattended coverage via this path** — it evaluates shared
coordination/queue files independent of `ps`-tree ancestry, so a
launchd-spawned orphan can still trip it.

The wrapper resolves its own pid as a synthetic
`launchd-watchdog-backstop` orchestrator identity, and — unlike the
fleet-shared watchdog backoff-state file described above — points
`ARM_WATCHDOG_BACKOFF_FILE` at its own isolated file by default, resetting
it before every invocation.

## Manual verification (not CI-checkable)

Whether the traffic light "responds sensibly" to real AMBER/RED conditions
can only be confirmed by running the CLI against synthetic conditions
(either on the real target machine, or locally via `ARM_FAKE_COLLECT_JSON`)
and eyeballing the output — no Jest assertion can stand in for that. Report
the observed output when shipping a change to this skill's thresholds or
decision logic.

## Diagnostics collection — installing forensics tooling on a new host

`scripts/agent-resource-management/diagnostics/collect.mjs` exists so a
host-freeze investigation can be run mechanically on any host, live or
after the fact, instead of by hand.

Two modes, both invoked directly with `node` (no shebang, no install step):

```bash
# Current-instant snapshot: memory/pressure/swap/process-tree + per-agent
# physical-footprint sample.
node scripts/agent-resource-management/diagnostics/collect.mjs --live

# Forensic recovery: scans /Library/Logs/DiagnosticReports/ for JetsamEvent
# reports and probes the unified log, both bounded to a lookback window.
# Default lookback is 30 days back from now if --since is omitted.
node scripts/agent-resource-management/diagnostics/collect.mjs --retroactive

# --since accepts any Date.parse()-able string, to narrow the window:
node scripts/agent-resource-management/diagnostics/collect.mjs --retroactive --since 2026-08-20
```

**Never escalates privilege.** The collector shells out only to fixed-path
system binaries — `vm_stat`, `sysctl`, `ps`, `log show` directly, plus
`footprint` and `vmmap` (on `--live`'s per-agent physical-footprint
sampling step) — and never invokes `sudo` or any other privilege-elevation
path, in either mode. Every probe is wrapped so a missing binary or a
permission-denied result degrades to a reported `status: 'failed'` (or
`'skipped'` where there was nothing to probe) inside the JSON output — it is
never a crash, and one probe's failure never prevents the others from
running or being reported.

**Output location.** Each run writes one timestamped JSON file to
`scripts/agent-resource-management/diagnostics/output/` (mode `0o600`) and
also prints the same JSON to stdout. That `output/` directory is gitignored
— it must never be committed; it exists purely as the on-host artifact for
whoever is triaging the freeze.

**Security note.** Don't paste raw collector output — especially
`--retroactive` unified-log or diagnostic-report content — into a shared
issue, PR, or commit. It can contain other users' or processes' data;
summarize categories of findings instead.
