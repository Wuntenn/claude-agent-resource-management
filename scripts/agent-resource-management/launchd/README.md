# launchd LaunchAgent — ARM watchdog beat backstop

> **Note on naming.** The plist template's `Label` and log-file names use
> the generic placeholder `com.example.arm-watchdog`. Rename it to your own
> reverse-DNS identifier (e.g. `com.<yourorg>.arm-watchdog`) before
> installing if you want it to look distinctly yours — every `launchctl`
> command below, the log paths, and the template filename must then be
> updated consistently to match.

## What this is, plainly, before you install anything

This installs a **real, unattended, `SIGKILL`-capable daemon** on your Mac —
a `launchd` LaunchAgent that runs every ~15 minutes regardless of whether any
orchestrator session is open, and whose invoked beat *can* call `kill` on a
process it judges eligible. That makes this genuinely security/safety-sensitive
local machine configuration, not a cosmetic convenience script.

- **It is OPT-IN.** Nothing in `npm install`, `npm run build`, or any other
  repo tooling installs, loads, or references this LaunchAgent. You must
  deliberately follow the install steps below on a machine you control.
- **It is a DETECTION-ONLY backstop, and its scope is narrower than
  "watchdog" suggests.** `runWatchdogBeat` (`lib/watchdog-beat.mjs`) has
  several steps; when invoked from `launchd` via this plist, only ONE of
  them is confirmed to genuinely fire unattended:
  - **The deadlock tripwire (Step 2) is the real protection this backstop
    provides.** It operates on shared coordination/queue files independent
    of process-tree ancestry, so it is unaffected by launchd spawning an
    orphan process with no descendants of its own.
  - **Per-agent stall/leak-velocity EVICT does NOT fire through this path.**
    `isEvictionAuthorized` (`lib/recon.mjs:458-496`) only authorizes eviction
    of `ps`-tree descendants of the calling orchestrator process. A
    launchd-spawned process has no such descendants — there is nothing for
    it to ever be authorized to evict. This is a deliberate, accepted
    limitation, not a bug in this feature: see
    the project's own architecture decision records (cross-orchestrator eviction authorization is intentionally out of scope; document your own decision if you carry this pattern into a larger fleet)
    for the full root-cause writeup (cross-orchestrator eviction is
    out of scope repo-wide, not just for this backstop).

    <!-- doc-honesty-anchor: leak-velocity-launchd-detection-gap -->
    **This is a narrower claim than the one that follows, and the two must
    not be conflated.** `isEvictionAuthorized` says eviction of an already-
    computed leak-velocity trip cannot be *authorized* for a launchd-spawned
    process. But leak-velocity's trip computation itself never even runs for
    such a beat, for a separate, upstream reason: `buildWatchdogCandidates`
    (`cli.mjs`) only ever populates its `candidates` list from pids that
    `isOwnedByOrchestrator` proves are ps-tree descendants of the *calling*
    orchestrator's own pid (`orchestratorPid`) — and only a pid that survives
    that ownership filter is even eligible to carry a `leakVelocityState`
    read from `footprint-state.mjs`. A `launchd`-spawned process is an
    orphan with no ps-tree descendants of its own, so `ownedEntries` is
    empty and `candidates` never contains anything. With no candidates
    carrying a `leakVelocityState`, `watchdog-beat.mjs`'s Step 1b (the
    per-candidate `computeLeakVelocity` loop) never runs at all for a
    launchd-invoked beat — not "runs and can't authorize eviction", but
    never runs, never computes a trip, in the first place. Detection itself
    is the gap here, not just the downstream action.

    **Separately, no disk-write-rate or swap-size detection exists in this
    codebase today, for any invocation path.** Leak-velocity
    (`lib/leak-velocity.mjs`, fed by `lib/footprint-sampler.mjs`) tracks
    macOS `phys_footprint` — a process's physical memory footprint — which
    is a different observable from disk-write bytes or swap-file growth.
    Nothing in this tree samples either of those metrics directly; physical-
    footprint growth is correlated with swap growth under memory pressure
    but is not the same signal, and no such disk/swap sampler exists at all
    today.

    **This is distinct from the project's own architecture decision records (cross-orchestrator eviction authorization is intentionally out of scope; document your own decision if you carry this pattern into a larger fleet).**
    That note does not cover this gap — it is scoped to the cross-orchestrator
    eviction-authorization decision described two paragraphs up (a pid this
    orchestrator doesn't own can't be evicted by it), which is a different
    decision from leak-velocity detection never running at all for an
    orphaned, launchd-spawned beat.

    **The 2026-09-08 host-crash incident's two PIDs (2181 and 73329) could
    not be retroactively identified as ARM-managed or otherwise.** The
    `.diag` files macOS produced for that incident are disk-writes-triggered
    microstackshot reports, and that diagnostic format carries no
    JS-source, module, or command-line identifying data for the flagged
    pids — a structural limit of the `.diag` format itself, not an
    unfinished piece of this investigation.

  - **Queue-aging escalation (Step 3) does NOT fire through this path
    either — and this is a separate, PRE-EXISTING gap unrelated to this
    feature.** `handleWatchdogBeat` in `cli.mjs` always calls
    `escalateAgedQueueEntry` with an empty `capacitySamples: []` array,
    which makes the capacity-free-throughout check fail closed by
    construction on every invocation of `--watchdog-beat`, launchd-driven or
    not. Installing this LaunchAgent does not close that gap; only the
    deadlock tripwire is confirmed to provide real unattended coverage
    today.

If what you actually need is genuine unattended stall/leak-velocity
recycling, this backstop does not provide it — it strengthens the deadlock
tripwire specifically for the "every orchestrator died, queue non-empty"
scenario the beat can still detect from shared files, and nothing more.

**The wrapper bounds every beat and force-kills a hung one.** Because
`launchd` won't start a new `StartInterval` instance of this `Label` while
the current one is still running, a beat that hangs (e.g. stuck on a file
lock) would otherwise silently and permanently stop this backstop forever —
no restart, no alert. `watchdog-beat-launchd.mjs` therefore force-terminates
the invoked `cli.mjs --watchdog-beat` child after 2 minutes
(`DEFAULT_WATCHDOG_BEAT_TIMEOUT_MS`), comfortably under the 900s
`StartInterval` below, and logs a `watchdog-beat-launchd: ... timed out`
marker to stderr when it does. The wrapper also forwards `SIGTERM`/`SIGINT`
to that same child, so a `launchctl bootout` (see the routine
reinstall/uninstall step further down) cleanly tears down both processes
instead of leaving the child orphaned.

<!-- doc-honesty-anchor: sigkill-wrapper-orphan-risk -->
**The "bounds every beat" claim above only holds while the wrapper process itself survives.**
`watchdog-beat-launchd.mjs`'s 2-minute force-kill and
`SIGTERM`/`SIGINT` forwarding are both actions the wrapper takes against its
child — neither exists once the wrapper is gone. If something `SIGKILL`s the
wrapper itself (there is no `SIGKILL` handler to intercept this; none is
possible), the `cli.mjs --watchdog-beat` grandchild it spawned is left
unbounded: nothing enforces the 2-minute timeout, nothing forwards
`SIGTERM`/`SIGINT` to it, and it keeps running. Because `launchd` only
tracks the wrapper's own pid — not its child's — `launchd` sees the wrapper
gone and starts a fresh instance on the next `StartInterval` fire, so the
orphaned grandchild can end up running concurrently with that fresh
wrapper-and-child pair.

**Accepted-the-risk decision: this concurrency cannot escalate into kill
authority.** This is a deliberate, accepted limitation, like the leak-velocity
gap above — not a bug. The orphaned `cli.mjs --watchdog-beat` grandchild is still
subject to the same structural constraint as any other launchd-spawned
beat: `isEvictionAuthorized` (`lib/recon.mjs:458-496`) only authorizes
eviction of `ps`-tree descendants of the *calling* orchestrator process
(a ps-tree descendant, that is), and
an orphaned grandchild has no descendants of its own to evict. It cannot
gain kill authority over the fresh instance, over any orchestrator session,
or over any other process through this path. This must NOT be read as a
claim that the underlying orphan risk goes away — the orphan keeps running,
consuming resources, and racing the fresh instance against shared
coordination/queue files; only the escalation to kill authority is
prevented.

**This is distinct from the leak-velocity detection gap
(`leak-velocity-launchd-detection-gap`) documented earlier in this
section.** That gap is about a launchd-spawned beat never computing a
leak-velocity trip in the first place, for pids it is legitimately
invoked against. This is a different, third limitation: a SIGKILL of the
*wrapper itself* mid-beat, which leaves an already-running grandchild
outside any enforcement at all, orphaned and duplicated rather than
merely undetected.

## Install

### 0. Prerequisites

- macOS with `launchd` (all supported dev machines).
- A local clone of this repo with `node` on your `PATH` in an interactive
  shell (you'll capture its absolute path below).

### 1. Resolve the two absolute paths you need

```bash
command -v node
# e.g. /Users/you/.nvm/versions/node/v22.23.0/bin/node

cd /path/to/your/clone
node -e "console.log(require('fs').realpathSync('scripts/agent-resource-management/lib/watchdog-beat-launchd.mjs'))"
```

Use `fs.realpathSync`, not `path.resolve` — `realpathSync` also resolves any
symlinks in the path (a symlinked clone directory, an external-volume mount,
or `/tmp` itself, which is a symlink to `/private/tmp` on macOS), so the
plist ends up with a real, symlink-free path. The wrapper's own entry-point
guard compares against Node's symlink-resolved `import.meta.url`; feeding it
a path that still traverses a symlink would make the guard fail silently and
the whole backstop would never run.

Keep both outputs handy — you'll substitute them in step 3.

### 2. Copy the template out of the repo

Do not load the repo copy in place. Copy it to the standard per-user
LaunchAgents directory first:

```bash
mkdir -p ~/Library/LaunchAgents
cp scripts/agent-resource-management/launchd/com.example.arm-watchdog.plist.template \
   ~/Library/LaunchAgents/com.example.arm-watchdog.plist
```

### 3. Substitute the placeholders in the copy

The copy has two placeholders, `__NODE_BIN__` and `__WRAPPER_PATH__`. Replace
them with the two absolute paths from step 1 — for example:

```bash
sed -i '' \
  -e "s#__NODE_BIN__#$(command -v node)#" \
  -e "s#__WRAPPER_PATH__#$(cd /path/to/your/clone && node -e \"console.log(require('fs').realpathSync('scripts/agent-resource-management/lib/watchdog-beat-launchd.mjs'))\")#" \
  ~/Library/LaunchAgents/com.example.arm-watchdog.plist
```

(`sed -i ''` is the BSD/macOS in-place syntax — note the empty string
argument; GNU `sed -i` on Linux would omit it, but this is a macOS-only
LaunchAgent so BSD `sed` is what you have. The substitution uses `#` as the
`s#…#…#` delimiter because `/` appears in the paths being substituted — if
your clone or node binary path itself happens to contain a literal `#`,
pick a different delimiter character that appears in neither path.)

Verify no placeholder survived the substitution:

```bash
grep -n '__NODE_BIN__\|__WRAPPER_PATH__' ~/Library/LaunchAgents/com.example.arm-watchdog.plist && \
  echo "STOP: a placeholder was not substituted" || echo "OK: no placeholders remain"
```

### 4. Strip Gatekeeper quarantine

A fresh clone or a file that ever traveled through Finder/Safari/AirDrop can
carry the `com.apple.quarantine` extended attribute, which can silently
block execution. Strip it from both the plist and the wrapper script it
invokes:

```bash
xattr -d com.apple.quarantine ~/Library/LaunchAgents/com.example.arm-watchdog.plist 2>/dev/null
xattr -d com.apple.quarantine /path/to/your/clone/scripts/agent-resource-management/lib/watchdog-beat-launchd.mjs 2>/dev/null
```

(The `2>/dev/null` is safe — `xattr -d` exits non-zero when the attribute
isn't present at all, which is the common case for a `git clone`.)

### 5. Check before you install — idempotency

Installing a second time (or on a second clone on the same host) without
checking first can register a duplicate/label-colliding LaunchAgent. Check
whether it's already loaded:

```bash
launchctl print gui/$(id -u)/com.example.arm-watchdog >/dev/null 2>&1 && echo "already loaded" || echo "not loaded"
```

If it prints "already loaded", unload the existing one first (see Uninstall
step 1) before re-loading — do not `load` on top of an already-loaded label.

### 6. Load it

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.example.arm-watchdog.plist
```

(`launchctl load` also works on current macOS but `bootstrap`/`bootout` is
the modern, more explicit pair and is used consistently in this doc.)

## launchd caches the loaded plist — reinstall, don't just overwrite

Once loaded, `launchd` holds the plist's contents **in memory**. Editing the
on-disk file at `~/Library/LaunchAgents/com.example.arm-watchdog.plist`
afterwards has **zero effect** on the running job until you explicitly
unload and reload it:

```bash
launchctl bootout gui/$(id -u)/com.example.arm-watchdog
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.example.arm-watchdog.plist
```

Any time you change the plist (new paths, a different `StartInterval`,
etc.), you must repeat this reinstall cycle — there is no "reload" shortcut
that reads the file fresh without it.

## Verify it's actually running

```bash
launchctl list | grep com.example.arm-watchdog
```

A row with a PID (or `-` between fires) and exit-status column confirms the
job is registered. To confirm a beat has actually executed and something
downstream is happening:

1. Note the current tail of the app-level liveness log
   (`$ARM_LIVENESS_LOG_FILE`, default `~/.claude/agent-state/liveness-log.json`).
2. Wait at least one `StartInterval` (900s / 15 minutes) with the machine
   **awake and unlocked** — see the cadence caveat below.
3. Check `/tmp/com.example.arm-watchdog.out.log` and
   `/tmp/com.example.arm-watchdog.err.log` (the plist's
   `StandardOutPath`/`StandardErrorPath`). These are **not** silent on a
   healthy run: because this wrapper resets its own isolated backoff file
   before every invocation (see "What this wrapper isolates" above), the
   beat is always due, so the underlying `cli.mjs --watchdog-beat` process
   writes one `{"type":"watchdog",...}` JSON line to stdout on every single
   fire — expect a new line roughly every 15 minutes, forever, for as long
   as the job is loaded (`stdio: 'inherit'` passes it straight through, and
   nothing rotates or caps this file, unlike the app-level liveness log —
   truncate it yourself occasionally if that matters to you). The wrapper's
   own `watchdog-beat-launchd: ...` diagnostic markers on stderr are the
   ones that appear only on failure; stderr silence is expected on a clean
   run, but stdout silence is not — its absence of *new* content is what
   would actually indicate the job stopped firing.
4. Re-check the liveness log for a new entry (a deadlock trip, if one
   genuinely occurred) — absence of a new entry on a healthy fleet is
   expected and not itself a sign of failure; the beat can run and find
   nothing to log.

### Cadence caveat: this is not a reliable 15-minute clock

`StartInterval` does **not** replay missed fires across sleep. If the
machine sleeps (lid closed, display sleep with the machine suspended, etc.)
for hours, the beat does not "catch up" with a burst of missed runs when it
wakes — it simply resumes firing every 900s from whenever the machine next
wakes. On a laptop that sleeps most of the day, treat this as "detection
happens at some point during periods the machine is awake, roughly every 15
minutes while awake" rather than a guaranteed wall-clock cadence.

## Uninstall

**Unload before deleting the file — never the reverse.** Deleting the plist
file first while the job is still loaded leaves a phantom loaded job that
`launchctl` cannot cleanly reference by path anymore (a known macOS
footgun).

```bash
# 1. Unload first
launchctl bootout gui/$(id -u)/com.example.arm-watchdog

# 2. Only then delete the file
rm ~/Library/LaunchAgents/com.example.arm-watchdog.plist
```

Confirm it's gone:

```bash
launchctl list | grep com.example.arm-watchdog
# (no output expected)
```
