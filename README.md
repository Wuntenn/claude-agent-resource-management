# claude-agent-resource-management

A traffic-light coordination CLI that paces concurrent Claude Code / Node
orchestrator agents on a resource-constrained macOS host (fixed RAM, single
SSD) so they collectively maximize completed work per hour without ever
crossing the memory or disk ceiling.

If you run multiple orchestrator agents (each spawning its own sub-agents)
on the same machine, you've probably hit this: enough concurrent agents
push memory pressure to critical, swap spikes, and the machine slows to a
crawl or the OS kills something — or agent temp output quietly fills the
disk over a long session. The naive fix is a fixed concurrency cap, but
that leaves throughput on the table on a quiet host and still isn't safe on
a busy one. This tool instead has every orchestrator declare its intent at
each "beat" (after each sub-agent's task finishes) and reads back a
deterministic verdict — **spawn-allowed**, **hold**, or **pause** a named
agent — computed from live machine state plus every other orchestrator's
declared intent, shared through one coordination file. The design
philosophy is **pace, don't block**: it never tells an orchestrator to stop
trying, only how much headroom exists *this beat*.

## Requirements

**macOS only.** The resource probes shell out to macOS-specific system
binaries — `vm_stat`, `sysctl vm.swapusage` / `kern.memorystatus_vm_pressure_level`,
`df`, `ps`, `footprint`, `vmmap`, and (for one optional feature) `lsof` and
`log show`. None of these exist in the same form on Linux, and this project
has no Linux code path or testing — treat it as untested/unsupported there.
Node 20+ (developed against Node 22).

## Installation

This is a skill + script pair meant to be copied into your own project, not
installed as an npm dependency.

1. Copy `.claude/skills/agent-resource-management/` (the `SKILL.md`
   behavioral contract) and `scripts/agent-resource-management/` (the
   actual CLI and its library) into your project, preserving both paths
   exactly — the skill doc's own examples assume
   `scripts/agent-resource-management/cli.mjs` from your project root.

2. Install the one runtime dependency (`micromatch`) plus Jest if you want
   to keep running the test suite in your own project:

   ```bash
   npm install micromatch
   npm install --save-dev jest @jest/globals
   ```

3. (Optional but recommended) Wire the PreToolUse gate into your own
   `.claude/settings.json` so agent spawns are automatically checked once a
   session has opted in at least once:

   ```json
   {
     "hooks": {
       "PreToolUse": [
         {
           "matcher": "Agent|Task",
           "hooks": [
             { "type": "command", "command": "node scripts/agent-resource-management/hooks/pretooluse-arm-gate.mjs" }
           ]
         }
       ]
     }
   }
   ```

   Every `command` field in `.claude/settings.json` runs as a shell command
   on matching tool calls — review this file with the same scrutiny you'd
   give a CI workflow change. See the SKILL.md's "PreToolUse gate" section
   for the full opt-in/fail-open/fail-closed contract before wiring this in.

4. (Optional) Coverage: if your project's Jest config uses
   `collectCoverageFrom`, add
   `scripts/agent-resource-management/lib/**/*.mjs` (excluding its own
   `*.jest.spec.mjs` files and `cli.mjs`, which is black-box tested via
   child-process spawns) to keep the library code counted.

5. (Optional) The unattended `launchd` watchdog backstop — a detection-only
   safety net that keeps working even if every orchestrator session has
   died — is documented separately in
   [`scripts/agent-resource-management/launchd/README.md`](scripts/agent-resource-management/launchd/README.md).
   It is genuinely opt-in and installs a real background daemon; read that
   README's opening section before installing it.

## Usage

Run `cli.mjs` at each **beat** — right after each sub-agent's task
completes:

```bash
node scripts/agent-resource-management/cli.mjs \
  --session-id="$CLAUDE_CODE_SESSION_ID" \
  --desired-agents=<how-many-you-would-spawn-this-beat>
```

`--session-id="$CLAUDE_CODE_SESSION_ID"` is required and must be passed
verbatim like this — `cli.mjs` derives a stable orchestrator identity from
it, and that same derivation is what lets the PreToolUse gate (if wired up)
recognize this session. Never hand-pick your own `--orchestrator-id`.

The response is one JSON object on stdout. Handle all six possible `type`
values — `spawn-allowed`, `hold`, `pause`, `alert`, `pressure-block`,
`memory-projection-block` — and always check `allowance > 0` before
spawning even when `type === 'spawn-allowed'`:

```jsonc
{ "type": "spawn-allowed", "allowance": 3, "liveAgentGrant": 2 }
```

```jsonc
{ "type": "hold" }
```

```jsonc
{ "type": "pause", "pauseCandidate": { "agentId": "agent-7", "rssMb": 640, "startedAt": 1735900000000 } }
```

A non-zero exit code or unparseable stdout means the CLI hit something it
couldn't recover from — treat that as "assume hold," never as a crash of
your own orchestrator loop.

Full behavioral contract, worked examples, and every edge case are in
[`.claude/skills/agent-resource-management/SKILL.md`](.claude/skills/agent-resource-management/SKILL.md) —
read it before building against this tool. The summary below is enough to
get moving, not the full reference.

### The six check points

1. **Before spawn** — the `--desired-agents=N` beat above.
2. **After sub-agent completion** — the normal beat cadence this tool is
   built around.
3. **Periodically, via `--heartbeat`** — a timer-driven poll (recommended
   ~20s) that catches a fast-moving RED excursion between real beats. A
   heartbeat's `allowance` does **not** reflect the global spawn-rate
   bucket — never spawn directly off a heartbeat's output.
4. **Periodically, via `--poll-footprint=<pid>`** — polls one agent's
   physical memory footprint (recommended every 10–30s per live pid),
   feeding the stall detector and leak-velocity circuit breaker.
5. **Before resuming a paused agent** — re-check the traffic light before
   bringing a previously-paused agent back. (Pause/resume execution itself
   is not implemented — `pause` only *names* a candidate; see below.)
6. **Immediately after a stale/failed sample** — handled internally by a
   single bounded retry; nothing you need to do.

### CLI flag reference

| Flag | Purpose |
|---|---|
| `--session-id="$CLAUDE_CODE_SESSION_ID"` | Required on every beat. Derives this orchestrator's stable identity. |
| `--desired-agents=<n>` | How many sub-agents you'd spawn this beat (default 1). Shapes your contended share and spawn-rate bucket draw. |
| `--agent-class=<label>` | Names the agent class this beat requests admission for, priced against per-class memory history. |
| `--running-agent-classes=<a>,<b>` | Other classes this same orchestrator already has running, for accurate memory projection. |
| `--heartbeat` | Timer-driven poll between real beats. Never spawn off its `allowance`. |
| `--poll-footprint=<pid>` | One-shot physical-footprint sample for a live pid, folded into EWMA state. |
| `--debug` / `--verbose` | Print raw memory/disk fields to stderr only. Diagnostic, never gating. |
| `--record-outcome --operation-type=<type>` | Persist a real observation (`--peak-memory-mb`, `--crashed`, `--pid=<pid>` to harvest linked footprint polls) into per-type history. |
| `--query-capacity=<types>` | Advisory read: how much headroom does each named operation type have right now? |
| `--claim=<type>:<n>` / `--release=<type>:<n>` | Atomically reserve/return slots in a per-operation-type ledger — the authoritative write half of the capacity model. |
| `--advise-only` | Non-mutating spawn verdict — the PreToolUse gate's own consultation mode. Don't use it in place of a real beat. |
| `--enqueue=<class>:<priority>:<ref>` / `--dequeue` / `--peek` | Durable work queue for spawns deferred by a `hold`. |
| `--dequeue-if-capacity` / `--bind-pid` / `--ack-lease` / `--release-lease` | The dispatcher sequence that drains the queue back into real spawns. |
| `--evict-pid=<pid> --lease-id=<id> --orchestrator-pid=<pid> --orchestrator-id=<id>` | Opt-in graceful-stop-and-requeue of one named process (SIGTERM, then SIGKILL after a grace period). |
| `--watchdog-beat --orchestrator-pid=<pid>` | Opt-in unattended stall/deadlock/queue-aging detection over your own spawned agents. |
| `--user-attests-idle` | One-shot human assertion that the host is genuinely idle right now, admitting one calibration probe. Never self-attest programmatically. |
| `--enable-disk-cleanup` / `--disable-disk-cleanup` | Arms/disarms the only recursively-deleting code path in this tool — a gated sweep of stale temp output. |
| `--watchdog-beat`, `--claim=live-agent:N`, etc. | See SKILL.md for the full live-agent-ceiling and dispatcher sequence. |

This table is a map, not the territory — flag validation rules, exact JSON
field shapes, and every documented accepted risk live in SKILL.md.

### Design philosophy, briefly

- **Pace, don't block.** A `hold`/`pause` verdict is never a standing
  instruction to stop trying — it's an answer about *this beat only*.
- **Admission-granting paths fail closed; everything else fails open.**
  `--claim`, `--release`, and the global spawn-rate bucket deny on a
  coordination error rather than risk over-granting; reads and
  observability fields degrade gracefully instead.
- **Recommendation, not remote control.** `pause`/`shedSignal` only *name*
  a candidate — nothing on that path sends a real signal to any process.
  The one deliberate exception is the opt-in `--evict-pid` beat, which does
  send real `SIGTERM`/`SIGKILL`, and only when a caller supplies all four
  required flags.
- **Every numeric threshold is a starting hypothesis**, not a settled
  constant — they're sized against a 16GB Mac mini and documented as such.
  Tune them from your own host's real data.

## Testing

```bash
npm install
npm test
```

Runs the full Jest suite (native ESM via `--experimental-vm-modules`) —
unit/integration tests for every `lib/` module, plus black-box
child-process tests for `cli.mjs` and the PreToolUse hook.

## License

MIT — see [`LICENSE`](LICENSE).
