// Phase 0 outer acceptance test ("agent-resource-management:
// launchd LaunchAgent as OS-level backstop for the watchdog/EVICT beat").
//
// Outer acceptance test:
//
//   A `launchd` LaunchAgent's `ProgramArguments` has no shell-style
//   self-pid substitution (no `$$`), so `--watchdog-beat`'s existing
//   required `--orchestrator-pid=<pid>` contract (cli.mjs, `handleWatchdogBeat`)
//   cannot be satisfied directly from a plist. Phase 1's implementer will add
//   a small Node wrapper script, `scripts/agent-resource-management/lib/
//   watchdog-beat-launchd.mjs`, that `launchd` invokes directly. It must:
//     (1) resolve its OWN process pid,
//     (2) invoke the existing `cli.mjs --watchdog-beat` with a fixed
//         synthetic `--orchestrator-id` and `--orchestrator-pid=<its own pid>`,
//     (3) NEVER let an internal error (a thrown JS error, OR the invoked
//         beat exiting non-zero) propagate as a wrapper crash — it must
//         catch/log and still exit 0, so `launchd` never sees this as a
//         process crash worth restart-looping.
//
// Layer: outer acceptance, INTEGRATION-level. Spawns the real wrapper script
// as a real child process via `child_process` (`spawnSync`) — never imports
// it — so this test genuinely observes the wrapper's own pid resolution and
// process exit behavior, not a unit-level stand-in. Mirrors
// ./watchdog-outer-acceptance.jest.spec.mjs's own `runCli`-via-`spawnSync`
// idiom.
//
// RED by construction, right now: `scripts/agent-resource-management/lib/
// watchdog-beat-launchd.mjs` does not exist yet (confirmed: `ls
// scripts/agent-resource-management/lib/` has no such file as of this
// commit). Both scenarios below fail for that reason (`spawnSync` reports
// `ENOENT`/a non-zero status from Node failing to find the module) until
// Phase 1 lands the wrapper.
//
// Do not add a stub implementation to make this pass — that defeats the
// point of the outer/phase red-green loop. Phase 1's implementer turns this
// file green by actually building the wrapper.
//
// ---------------------------------------------------------------------------
// Hermeticity — every env var this test sets is a genuine, already-documented
// `cli.mjs` test seam (grepped from cli.mjs directly, not guessed):
//   ARM_LIVENESS_LOG_FILE  — resolveLivenessLogFilePath()
//   ARM_COORDINATION_FILE  — resolveCoordinationFilePath()
//   ARM_QUEUE_FILE         — resolveQueueFilePath()
// all pointed at a fresh `fs.mkdtempSync` scratch directory per test, so this
// suite never touches the real `~/.claude/agent-state/` liveness/coordination
// files a genuine launchd-driven beat would use in production. `HOME` is
// likewise overridden fleet-wide in `baseEnv()` (post-merge review,
// Medium) so the wrapper's own `homedir()`-rooted isolated backoff-file
// default (`resolveDefaultIsolatedBackoffFilePath`,
// lib/watchdog-beat-launchd.mjs) resolves inside this same scratch directory
// too, never against a real developer machine's
// `~/.claude/agent-state/launchd-watchdog-backoff.json`.
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

// Pinned per the ticket's suggested (implementer may rename) wrapper path —
// Phase 1's interface contract this file exercises end-to-end.
const WRAPPER = join(
  dirname(fileURLToPath(import.meta.url)),
  'lib',
  'watchdog-beat-launchd.mjs',
);

let workDir;
let livenessLogFilePath;
let coordinationFilePath;
let queueFilePath;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'arm-watchdog-launchd-outer-'));
  livenessLogFilePath = join(workDir, 'liveness-log.json');
  coordinationFilePath = join(workDir, 'coordination.json');
  queueFilePath = join(workDir, 'queue.json');
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
});

function baseEnv(extra = {}) {
  return {
    ...process.env,
    // post-merge review, Medium — rooted here, in the shared helper,
    // not just a specific scenario, so `homedir()`-derived defaults inside
    // the wrapper (`resolveDefaultIsolatedBackoffFilePath`) never resolve
    // against a real developer machine's `~/.claude/agent-state/`.
    HOME: join(workDir, 'fake-home'),
    ARM_LIVENESS_LOG_FILE: livenessLogFilePath,
    ARM_COORDINATION_FILE: coordinationFilePath,
    ARM_QUEUE_FILE: queueFilePath,
    ...extra,
  };
}

/** Runs the real wrapper script as a real child process (spawnSync — never imported). */
function runWrapper(extraEnv) {
  const result = spawnSync('node', [WRAPPER], {
    encoding: 'utf8',
    env: baseEnv(extraEnv),
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

it(
  'runs standalone with no special stubbing and exits 0 — a normal beat, whether or not it trips ' +
    'deadlock/aging, completes normally',
  () => {
    const result = runWrapper();

    expect(result.status).toBe(0);
  },
);

it(
  'still exits 0, with a caught-and-logged marker on stderr, when the invoked --watchdog-beat fails ' +
    '(a genuinely-unwritable coordination-file directory forces cli.mjs itself to exit non-zero)',
  () => {
    // Forces a hermetic, real failure inside the invoked `cli.mjs
    // --watchdog-beat` call: `ARM_COORDINATION_FILE` is pointed at a path
    // whose PARENT segment is a plain file, not a directory, so cli.mjs's
    // own `ensureCoordinationDir` (called unconditionally and NOT wrapped in
    // its own try/catch inside `handleWatchdogBeat` — see cli.mjs around
    // `await ensureCoordinationDir(coordinationFilePath);` just above the
    // `runWatchdogBeat` call) throws ENOTDIR attempting `fs.mkdir` under a
    // file. That error is caught nowhere inside `handleWatchdogBeat` itself,
    // so it propagates up to cli.mjs's own top-level `main().catch(...)`,
    // which prints `agent-resource-management: unexpected error: ...` to
    // stderr and calls `process.exit(1)` — a genuine non-zero exit from the
    // invoked beat, the exact "invoked beat exiting non-zero" case this
    // wrapper must swallow.
    const blockerFilePath = join(workDir, 'blocker-not-a-directory');
    writeFileSync(blockerFilePath, 'this is a file, not a directory');
    const brokenCoordinationFilePath = join(blockerFilePath, 'coordination.json');

    const result = runWrapper({ ARM_COORDINATION_FILE: brokenCoordinationFilePath });

    expect(result.status).toBe(0);
    // A reasonable "caught and logged" marker — not over-specified on exact
    // wording, per this ticket's own guidance.
    expect(result.stderr).toMatch(/watchdog-beat-launchd/i);
  },
);

it('the wrapper script file actually exists at the pinned Phase 1 path (sanity — a missing file is the RED condition this suite exists to prove)', () => {
  // This assertion is deliberately the LAST thing this file checks, not a
  // `beforeAll` gate — the two scenarios above are the real acceptance
  // criteria; this one exists only so a failure reads unambiguously as
  // "the wrapper file is missing" rather than leaving that inference to
  // `spawnSync`'s own opaque ENOENT/exit-code behavior.
  expect(existsSync(WRAPPER)).toBe(true);
});
