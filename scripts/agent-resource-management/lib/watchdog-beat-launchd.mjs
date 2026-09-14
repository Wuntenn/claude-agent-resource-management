// Phase 1 — launchd LaunchAgent OS-level backstop for the
// watchdog/EVICT beat.
//
// A `launchd` LaunchAgent's `ProgramArguments` array is static: there is no
// shell-style self-pid substitution (no `$$`) a plist can use to satisfy
// `--watchdog-beat`'s existing required `--orchestrator-pid=<pid>` contract
// (cli.mjs, `handleWatchdogBeat`). This wrapper exists purely to bridge that
// gap — `launchd` invokes THIS script directly (bare, no argv — see
// ./watchdog-beat-launchd-outer.jest.spec.mjs and
// ./watchdog-beat-launchd.jest.spec.mjs, both of which pin the contract this
// file implements), and it:
//
//   (1) resolves its OWN process pid (`process.pid` — the pid `launchd`
//       actually assigned this process, the only pid a bare plist invocation
//       can ever know),
//   (2) shells out to the existing `cli.mjs --watchdog-beat` with a fixed
//       synthetic `--orchestrator-id` (matching /launchd/i, per the spec's
//       own vocabulary) and `--orchestrator-pid=<its own pid>`,
//   (3) NEVER lets an internal error — a thrown JS error, a spawn failure,
//       the invoked beat exiting non-zero, OR the invoked beat simply
//       HANGING — propagate as a wrapper crash or a silent, permanent
//       stop. `launchd` won't start a new `StartInterval` instance of the
//       same `Label` while the current one is still running (see
//       ../launchd/README.md), so a hung child — e.g. stuck on a file lock
//       — would otherwise silently and permanently stop this backstop from
//       ever firing again: no restart, no alert, no log entry, because none
//       of `spawnSync`'s (or `spawn`'s) error/signal/non-zero-status
//       branches ever fire while the child is merely hanging. This wrapper
//       therefore bounds every invocation with a hard timeout (well under
//       the 900s `StartInterval`) and force-terminates a hung child so the
//       next `StartInterval` fire is never blocked. It always exits 0 and
//       instead logs a `watchdog-beat-launchd`-tagged marker to stderr on
//       any failure (spawn error, signal, non-zero exit, or timeout).
//
// This wrapper is intentionally read-only against the coordination file: it
// never calls `declareDibs` (or anything else) for its own synthetic
// identity — it only shells out to `--watchdog-beat`, which itself never
// writes a dibs/ledger entry either. There is nothing for a real fleet
// orchestrator to ever see or be confused by.
//
// The env it receives (`ARM_LIVENESS_LOG_FILE`, `ARM_COORDINATION_FILE`,
// `ARM_QUEUE_FILE`, and every other `ARM_*` test/config seam) is passed
// through to the child unmodified, with one deliberate exception:
// `ARM_WATCHDOG_BACKOFF_FILE`, which this wrapper isolates from the
// fleet-shared default unless the caller has already set it — see
// `resolveDefaultIsolatedBackoffFilePath` below for why.

import { spawn } from 'node:child_process';
import { promises as fs, realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

/**
 * Fixed synthetic identity for every launchd-driven beat — deliberately not
 * randomised per run: there is exactly one LaunchAgent, and a stable id lets
 * anyone inspecting the coordination/liveness state recognise this wrapper's
 * own beats at a glance. Must match /launchd/i per the Phase 0/1 test
 * contract.
 */
export const SYNTHETIC_ORCHESTRATOR_ID = 'launchd-watchdog-backstop';

/**
 * Bounds a single `--watchdog-beat` invocation. 120000ms (2 minutes) is
 * comfortably under the plist's 900s (15 minute) `StartInterval` — chosen so
 * that even a fully-hung child is force-terminated with ample margin before
 * the next scheduled fire, while still being generous relative to how long a
 * healthy beat (a handful of file reads/writes and a `ps` invocation) should
 * ever actually take. Overridable via `ARM_WATCHDOG_LAUNCHD_TIMEOUT_MS`
 * purely as a test seam (mirrors `ARM_WATCHDOG_LAUNCHD_CLI_PATH` below) — no
 * real launchd invocation is expected to ever need a non-default value.
 *
 * @type {number}
 */
export const DEFAULT_WATCHDOG_BEAT_TIMEOUT_MS = 120_000;

/**
 * pre-PR review, High — `--watchdog-beat`'s own backoff-state file
 * (`resolveWatchdogBackoffFilePath`, cli.mjs) defaults to a path derived from
 * `dirname(coordinationFilePath)` — fleet-shared across every concurrent
 * orchestrator, by design (see cli.mjs's own comment directly above that
 * function). `isBackoffDue` (lib/watchdog-backoff.mjs) doubles
 * `turnsToWait` up to `MAX_TURNS_TO_WAIT` (8) against a 5-minute unit
 * whenever a beat trips nothing — an exponential-backoff model designed to
 * avoid busy-checking inside a TIGHT IN-SESSION polling loop. Before this
 * ticket, nothing called `--watchdog-beat` at all while no orchestrator
 * session was running, so that shared state only ever advanced from a real,
 * live orchestrator's own beats. This wrapper now calls `--watchdog-beat`
 * unconditionally every ~15 minutes regardless of whether any orchestrator
 * is alive — if it shared the default backoff file, it would silently back
 * the SHARED state off to an 8-turn (40-minute) ceiling, which could then
 * gate a live orchestrator's own beat (the one caller with real ps-tree
 * eviction authority via `isEvictionAuthorized`) as `skipped: true, reason:
 * 'backoff-not-due'` — exactly backwards for a backstop whose entire point
 * is to never suppress the one path that can actually evict a stalled agent.
 *
 * The fix has two parts, both required:
 *
 *   1. This wrapper gets its OWN isolated backoff-state file, entirely
 *      separate from the shared default — via the exact same
 *      `ARM_WATCHDOG_BACKOFF_FILE` override seam `cli.mjs` already honours
 *      (`resolveWatchdogBackoffFilePath`), so no change to cli.mjs itself is
 *      needed. When the caller (a real launchd invocation, or a test) has
 *      not already set `ARM_WATCHDOG_BACKOFF_FILE`, this wrapper sets it
 *      itself before spawning the child, pointed at a launchd-specific
 *      sibling of the real liveness-log default
 *      (`resolveLivenessLogFilePath()`'s own `~/.claude/agent-state/`
 *      convention). An explicit caller-set override still always wins —
 *      this wrapper never clobbers it.
 *
 *   2. The doubling/backoff model itself doesn't even apply to this
 *      wrapper's own call: `isBackoffDue` exists to avoid busy-checking
 *      inside a tight in-session loop, but this call is ALREADY paced by an
 *      external OS timer (`launchd`'s `StartInterval`, 900s) — no amount of
 *      internal backoff can make it fire more often than that, and letting
 *      it fire LESS often than that (by accumulating its own backoff state
 *      turn over turn) would silently widen this backstop's real-world
 *      cadence past what the plist's own `StartInterval` comment and the
 *      README's verification steps already claim (~15 minutes). So this
 *      wrapper resets its OWN isolated backoff file before every single
 *      invocation (best-effort — see `resetIsolatedBackoffFile` below),
 *      guaranteeing every wrapper-driven beat is always due. This reset is
 *      skipped when the caller supplied `ARM_WATCHDOG_BACKOFF_FILE`
 *      explicitly (e.g. a test asserting the isolated file's own
 *      state-persistence behaviour) — resetting a file the caller
 *      deliberately pointed at somewhere specific would defeat the point of
 *      that override.
 *
 * @returns {string}
 */
function resolveDefaultIsolatedBackoffFilePath() {
  return join(homedir(), '.claude', 'agent-state', 'launchd-watchdog-backoff.json');
}

/**
 * Best-effort delete of this wrapper's own isolated backoff-state file —
 * never throws (mirrors every other fail-open state-file helper in this
 * skill, e.g. cli.mjs's `ensureStateFileDir`). A file that doesn't exist yet
 * (the common case, first run) is not an error.
 *
 * @param {string} filePath
 * @returns {Promise<void>}
 */
async function resetIsolatedBackoffFile(filePath) {
  try {
    await fs.rm(filePath, { force: true });
  } catch (err) {
    // Deliberately non-fatal — a failure here just means the next beat
    // falls back to reading whatever stale state is on disk, which
    // `readBackoffState`/`isBackoffDue` already degrade gracefully from.
    // `{ force: true }` already makes ENOENT a non-error, so anything that
    // reaches this catch is a genuine (if rare) filesystem fault — e.g. a
    // permissions problem on `~/.claude/agent-state/` — and a *persistent*
    // instance of it would silently reintroduce the accumulating-backoff
    // behaviour this isolated file exists to avoid. One stderr line keeps
    // that failure mode from being completely invisible.
    process.stderr.write(
      `watchdog-beat-launchd: could not reset isolated backoff file at ${filePath} — ` +
        `falling back to stale on-disk state: ${err && err.message ? err.message : err}\n`,
    );
  }
}

/**
 * Resolved relative to THIS module's own location, not `process.cwd()` — a
 * real launchd invocation has no predictable working directory, so any
 * cwd-relative resolution would be silently broken in production while
 * still passing every test run from the repo root.
 *
 * @returns {string}
 */
function resolveDefaultCliPath() {
  return join(dirname(fileURLToPath(import.meta.url)), '..', 'cli.mjs');
}

/**
 * Parses an `ARM_*_MS`-style env override, rejecting anything that isn't a
 * finite positive number (`NaN`, `0`, negative values, empty/garbage
 * strings) rather than silently passing it through — mirrors cli.mjs's own
 * `parsePositiveMs` idiom. An unset env var (`undefined`) also falls back to
 * `fallback`.
 *
 * @param {string | undefined} rawValue
 * @param {number} fallback
 * @returns {number}
 */
function parsePositiveMs(rawValue, fallback) {
  const parsed = Number(rawValue);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

/**
 * @param {string} cliPath
 * @returns {string[]}
 */
function buildWatchdogBeatArgv(cliPath) {
  return [
    cliPath,
    '--watchdog-beat',
    `--orchestrator-id=${SYNTHETIC_ORCHESTRATOR_ID}`,
    `--orchestrator-pid=${process.pid}`,
  ];
}

/**
 * The one child process this wrapper ever has in flight at a time, tracked
 * purely so a signal arriving at this wrapper (SIGTERM/SIGINT — see
 * `installSignalForwarding` below) has something to forward to. `null`
 * whenever no beat is currently running (before spawn, or after it has
 * already exited) — the forwarding handler treats that as "nothing to
 * forward to".
 *
 * @type {import('node:child_process').ChildProcess | null}
 */
let activeChild = null;

/**
 * Spawns `node <cliPath> --watchdog-beat ...` asynchronously and resolves —
 * NEVER rejects — with a `spawnSync`-shaped result (`{ error, signal,
 * status, timedOut }`) once the child exits, is killed for timing out, or
 * fails to spawn at all. Async `spawn` (unlike `spawnSync`) has no built-in
 * `timeout` option, so the bounded-hang guarantee is implemented directly
 * here with `setTimeout` + `child.kill('SIGKILL')`.
 *
 * `SIGKILL` (not `SIGTERM`) is used on timeout deliberately: the whole
 * premise of this timeout is a child that is stuck (e.g. blocked on a file
 * lock) and therefore may not be in a position to honour a graceful
 * `SIGTERM` at all — a guaranteed kill is what actually preserves the next
 * `StartInterval` fire.
 *
 * `stdio: 'inherit'` deliberately does NOT swallow the child's stdout/stderr
 * — a real launchd LaunchAgent normally redirects this process's own
 * stdout/stderr to log files (via the plist's `StandardOutPath`/
 * `StandardErrorPath`), so passing the child's streams straight through is
 * what makes the underlying beat's own diagnostics show up there at all.
 *
 * @param {string[]} argv `[cliPath, ...cliArgs]` — `argv[0]` is passed as
 *   the script to run under `process.execPath`.
 * @param {number} timeoutMs
 * @param {NodeJS.ProcessEnv} env env to pass through to the child — always
 *   `process.env` plus this wrapper's own `ARM_WATCHDOG_BACKOFF_FILE`
 *   isolation (see `resolveDefaultIsolatedBackoffFilePath` above), never
 *   `process.env` directly, so the child never resolves the fleet-shared
 *   default backoff file.
 * @returns {Promise<{error: Error | null, signal: NodeJS.Signals | null, status: number | null, timedOut: boolean}>}
 */
function spawnWatchdogBeatChild(argv, timeoutMs, env) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, argv, {
      env,
      stdio: 'inherit',
    });
    activeChild = child;

    let timedOut = false;
    let settled = false;

    const timer = setTimeout(() => {
      timedOut = true;
      child.kill('SIGKILL');
    }, timeoutMs);
    // Never keeps the event loop alive on its own — the `exit`/`error`
    // listeners below always clear it once the child settles, but `unref`
    // is cheap insurance against this timer being the sole reason the
    // process lingers in some future refactor.
    timer.unref?.();

    const settle = (result) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      activeChild = null;
      resolve({ ...result, timedOut });
    };

    child.on('error', (error) => {
      settle({ error, signal: null, status: null });
    });

    child.on('exit', (code, signal) => {
      settle({ error: null, signal, status: code });
    });
  });
}

/**
 * pre-PR review, Medium — bounds how long this wrapper waits for a
 * forwarded signal to actually terminate `activeChild` before escalating to
 * `SIGKILL` and exiting anyway. This wrapper's own motivating scenario (see
 * this file's header) is a child stuck on a file lock — a hang that, by
 * definition, may not respond to a graceful `SIGTERM`/`SIGINT` either.
 * Forwarding alone, with no escalation, would leave this wrapper blocked
 * indefinitely on that same uncooperative child (bounded only by
 * `spawnWatchdogBeatChild`'s own multi-minute `DEFAULT_WATCHDOG_BEAT_TIMEOUT_MS`
 * timer) — far past what `launchctl bootout` tolerates before the OS itself
 * intervenes. Overridable via `ARM_WATCHDOG_LAUNCHD_SIGTERM_ESCALATION_MS`
 * purely as a test seam, mirroring `ARM_WATCHDOG_LAUNCHD_TIMEOUT_MS` above.
 *
 * @type {number}
 */
export const DEFAULT_SIGTERM_ESCALATION_MS = 3_000;

/**
 * Registers SIGTERM/SIGINT handlers that forward the signal to whatever
 * child is currently running (see `activeChild` above) before letting this
 * wrapper's own process continue. This closes the "orphaned grandchild"
 * gap on `launchctl bootout` (the documented, routine reinstall/uninstall
 * path — see ../launchd/README.md): launchd signals THIS wrapper process,
 * but the spawned `cli.mjs --watchdog-beat` child is a separate process
 * launchd has no direct knowledge of and will not itself signal.
 *
 * Forwarding is followed by an unref'd escalation timer
 * (`DEFAULT_SIGTERM_ESCALATION_MS`, ~3s): if the child has not exited by
 * then, it is `SIGKILL`ed and this wrapper exits immediately rather than
 * continuing to wait on the normal `await spawnWatchdogBeatChild(...)` flow
 * (inside `runWatchdogBeatLaunchd`) to observe the child's `exit` event —
 * see this function's own doc comment above for why forwarding alone is not
 * sufficient. A child that DOES honour the forwarded signal within the
 * escalation window still exits through the normal flow well before the
 * timer fires — the escalation path is a backstop, not the common case.
 * The one edge case handled explicitly here is a signal arriving when there
 * is no child to forward to (nothing spawned yet, or the child already
 * exited) — in that case this wrapper has nothing left to do, so it exits
 * immediately.
 *
 * @returns {void}
 */
function installSignalForwarding() {
  const escalationMs = parsePositiveMs(
    process.env.ARM_WATCHDOG_LAUNCHD_SIGTERM_ESCALATION_MS,
    DEFAULT_SIGTERM_ESCALATION_MS,
  );

  /** @param {NodeJS.Signals} signal */
  const forward = (signal) => {
    const child = activeChild;
    if (child && child.exitCode === null && child.signalCode === null) {
      child.kill(signal);
      const escalationTimer = setTimeout(() => {
        if (child.exitCode === null && child.signalCode === null) {
          child.kill('SIGKILL');
        }
        process.exit(0);
      }, escalationMs);
      escalationTimer.unref?.();
    } else {
      process.exit(0);
    }
  };

  process.on('SIGTERM', () => forward('SIGTERM'));
  process.on('SIGINT', () => forward('SIGINT'));
}

/**
 * Runs one launchd-driven watchdog beat. Always resolves — never rejects —
 * so a caller (including this module's own direct-execution entry point
 * below) never needs its own try/catch to keep the process from crashing.
 *
 * `ARM_WATCHDOG_LAUNCHD_CLI_PATH`, when set, overrides which script is
 * invoked in place of the real `cli.mjs` — mirrors every other `ARM_*_FILE`
 * test seam already documented in cli.mjs (`ARM_DEADLOCK_STATE_FILE`,
 * `ARM_WATCHDOG_BACKOFF_FILE`, etc.), letting tests substitute a tiny
 * stand-in rather than spawning the real, much heavier `cli.mjs`.
 *
 * `ARM_WATCHDOG_LAUNCHD_TIMEOUT_MS`, when set, overrides
 * `DEFAULT_WATCHDOG_BEAT_TIMEOUT_MS` — purely a test seam so specs can pin a
 * short bound instead of waiting out the real 2-minute default.
 *
 * `ARM_WATCHDOG_BACKOFF_FILE` — when the CALLER has not already set this,
 * this function sets it itself (see `resolveDefaultIsolatedBackoffFilePath`
 * above) before spawning the child, and resets that isolated file first —
 * both steps are what keep this wrapper's beats off the fleet-shared default
 * backoff file and off any internal throttling of its own. A caller-set
 * value always wins untouched.
 *
 * @returns {Promise<void>}
 */
export async function runWatchdogBeatLaunchd() {
  try {
    const cliPath = process.env.ARM_WATCHDOG_LAUNCHD_CLI_PATH || resolveDefaultCliPath();
    const timeoutMs = parsePositiveMs(
      process.env.ARM_WATCHDOG_LAUNCHD_TIMEOUT_MS,
      DEFAULT_WATCHDOG_BEAT_TIMEOUT_MS,
    );

    // post-merge review, Medium — deliberately a TRUTHINESS check, not
    // `typeof ... === 'string'`. cli.mjs's own `resolveWatchdogBackoffFilePath`
    // gates on truthiness (`if (process.env.ARM_WATCHDOG_BACKOFF_FILE)`), so an
    // explicit empty string (`ARM_WATCHDOG_BACKOFF_FILE=`) is falsy there and
    // falls straight through to the fleet-shared default file. A `typeof`
    // check alone would treat that empty string as "the caller already set
    // it", skip this wrapper's isolation/reset logic, and pass `''` through to
    // the child — silently reinstating the exact fleet-shared-backoff failure
    // mode this file exists to prevent. Aligning on the same truthiness
    // semantics as cli.mjs closes that gap.
    const callerSetBackoffFile = Boolean(process.env.ARM_WATCHDOG_BACKOFF_FILE);
    const backoffFilePath = callerSetBackoffFile
      ? process.env.ARM_WATCHDOG_BACKOFF_FILE
      : resolveDefaultIsolatedBackoffFilePath();
    if (!callerSetBackoffFile) {
      await resetIsolatedBackoffFile(backoffFilePath);
    }
    const childEnv = { ...process.env, ARM_WATCHDOG_BACKOFF_FILE: backoffFilePath };

    const result = await spawnWatchdogBeatChild(buildWatchdogBeatArgv(cliPath), timeoutMs, childEnv);

    if (result.timedOut) {
      process.stderr.write(
        `watchdog-beat-launchd: the watchdog beat timed out after ${timeoutMs}ms (cli=${cliPath}) and was force-terminated\n`,
      );
    } else if (result.error) {
      process.stderr.write(
        `watchdog-beat-launchd: failed to spawn the watchdog beat (cli=${cliPath}) — ${result.error.message}\n`,
      );
    } else if (result.signal) {
      process.stderr.write(
        `watchdog-beat-launchd: the watchdog beat was terminated by signal ${result.signal}\n`,
      );
    } else if (typeof result.status === 'number' && result.status !== 0) {
      process.stderr.write(
        `watchdog-beat-launchd: the watchdog beat exited non-zero (status=${result.status})\n`,
      );
    }
  } catch (error) {
    // Belt-and-braces — `spawnWatchdogBeatChild` itself resolves rather than
    // throws for every failure mode it knows about, but nothing above is
    // guaranteed never to throw (e.g. a future refactor), and per this
    // wrapper's own contract NO internal error may ever propagate as a
    // crash.
    process.stderr.write(`watchdog-beat-launchd: unexpected error — ${error?.stack ?? error}\n`);
  }
}

// pre-PR review, Medium — entry-point guard. Without this, the block
// below ran unconditionally at MODULE-EVALUATION time — so a future `import`
// of this module for either of its exports (`SYNTHETIC_ORCHESTRATOR_ID`,
// `DEFAULT_WATCHDOG_BEAT_TIMEOUT_MS`, `DEFAULT_SIGTERM_ESCALATION_MS`) would
// trigger a REAL watchdog beat and then kill the importing process — nothing
// imports this module today, but that's an accident of history, not a
// guarantee. The standard Node ESM guard below (`argv[1]` resolved to a
// `file://` URL, compared against `import.meta.url`) makes plain `import`
// safe: it only runs the direct-execution block below when this file is
// itself the script Node was launched with — which is exactly how
// `launchd`'s `ProgramArguments` invokes it (`node
// lib/watchdog-beat-launchd.mjs`, no args), and how both Phase 0 and Phase
// 1's own specs invoke it too (spawnSync, never imported).
//
// `import.meta.url` is always symlink-resolved by Node (the real path), but
// `process.argv[1]` is the literal string the invoker (launchd, a shell,
// etc.) passed — if the wrapper is reached through a symlink (a symlinked
// clone directory, an external-volume mount, or `/tmp` itself, which is a
// symlink to `/private/tmp` on macOS) those two diverge and a naive
// string-equality guard evaluates false, silently no-opping the entire
// backstop with no stderr output. Resolving `argv[1]` through
// `realpathSync` before comparing closes that gap; an unreadable/missing
// `argv[1]` is treated as "not a direct execution we can confirm" rather
// than throwing.
const isDirectExecution = (() => {
  if (typeof process.argv[1] !== 'string') return false;
  try {
    return pathToFileURL(realpathSync(process.argv[1])).href === import.meta.url;
  } catch (err) {
    // A missing/unreadable argv[1] genuinely means "not a direct execution
    // we can confirm", so this still resolves to false either way — but a
    // silent catch here would mean this backstop can stop firing entirely
    // (skip the whole `if (isDirectExecution)` block below, including
    // `installSignalForwarding`/`runWatchdogBeatLaunchd`) with zero trace
    // anywhere, which is exactly the failure mode a detection-only
    // backstop must not have. Write one line to stderr so a persistent
    // realpathSync failure (as opposed to the expected "imported, not
    // executed" case, which never reaches this catch) at least shows up in
    // the launchd StandardErrorPath log.
    process.stderr.write(
      `watchdog-beat-launchd: entry-point guard could not resolve process.argv[1] (${String(
        process.argv[1],
      )}) — treating as non-direct execution: ${err && err.message ? err.message : err}\n`,
    );
    return false;
  }
})();

if (isDirectExecution) {
  // `process.exit(0)` is unconditional and outside any try/catch:
  // `runWatchdogBeatLaunchd` above already guarantees it never rejects, and
  // this line is the wrapper's own outermost enforcement of "always exit 0"
  // regardless of what happened inside.
  installSignalForwarding();
  await runWatchdogBeatLaunchd();
  process.exit(0);
}
