#!/usr/bin/env node
// Phase 3 — the ARM PreToolUse gate.
//
// Claude Code invokes this with the hook event as JSON on stdin. Exit 0 allows
// the tool call; exit 2 blocks it and surfaces stderr as the shown reason. See
// https://code.claude.com/docs/en/hooks.md.
//
// The gate is designed OPT-IN BUT STICKY, with a tiered failure policy:
//
//   tier 0  not a gated spawn event at all (unparseable stdin, a different
//           hook, a non-spawn tool) -> allow, having read nothing and spawned
//           nothing. This is the hot path for every tool call on the host, so
//           it must stay genuinely free: no ledger read, no `cli.mjs` child
//           process, and no coordination file created as a side effect.
//
//   tier 1  a gated spawn from a session that never declared dibs -> allow.
//           `hasEverOptedIn` is a plain, lock-free, fail-open read; a missing
//           or corrupt ledger reads as "never opted in", because no rule has
//           been established for this session and a read must never be the
//           thing that blocks a tool call. An UNDERIVABLE session identity
//           (missing/blank/padded/non-string `session_id`) lands here too: if
//           we cannot name the session we cannot have established anything for
//           it, and `orchestrator-id.mjs` deliberately provides no shared
//           fallback id to inherit another session's gate state from.
//
//   tier 2  a gated spawn from an OPTED-IN session -> consult `cli.mjs`
//           read-only (`--orchestrator-id=<derived> --advise-only`, 5s
//           timeout, never `--release`/`--record-outcome`) and honour its
//           verdict. `spawn-allowed` WITH A NON-ZERO GRANT allows (see
//           `grantsNothing` below — `spawn-allowed` alone is not enough); any
//           other well-formed verdict is a POLICY deny. No trustworthy answer
//           at all (crash, non-zero exit, unparseable/empty stdout, timeout,
//           missing target) is a FAIL-CLOSED deny, which additionally appends
//           one `arm-gate-fail-closed` entry to the liveness log so the path's
//           real firing rate stays auditable. Policy denials are deliberately
//           NOT logged into that bucket — mixing them in would destroy
//           exactly that measurement.
//
// WHAT THIS GATE DOES NOT ENFORCE. `--advise-only` is a memory/pressure/disk
// check, not a full admission check. The two host-wide admission-GRANTING
// gates — the global spawn-rate bucket and the live-agent concurrency ceiling
// — are consumption-based: they can only be evaluated by taking from them, so
// an advisory run cannot probe either without mutating shared state, and does
// not try. This gate therefore closes the "beat 1 consulted, beats 2-N
// unchecked" gap only on the memory, pressure and disk axes (plus the dibs-
// share/memory-projection arithmetic derived from them). Spawn-rate and
// live-agent concurrency remain enforced ONLY by the orchestrator's own real
// beat — an earlier design note overstated this; see `--advise-only`'s doc
// comment in `../cli.mjs` for the corrected statement at the source.
//
// The memory-projection half of that claim IS real, including for the gated
// session's OWN running fleet: this hook passes neither `--agent-class` nor
// `--running-agent-classes` (it consults on behalf of a session it does not
// manage and cannot know either value), and `cli.mjs` therefore falls back to
// that session's own dibs entry for the running set rather than pricing it at
// zero (Phase 3, third review round, Medium #1). Three residuals survive
// and are documented in full at that fallback's definition:
//
//   1. A STALENESS CLIFF, not a partial gap. The fallback reads the session's
//      dibs row through a liveness-filtered read whose threshold is
//      `DEFAULT_LIVENESS_THRESHOLD_MS` — 15 minutes. Between beats, agents
//      started since the last real beat are uncounted; and once the row
//      itself ages past 15 minutes it is pruned, the fallback returns
//      NOTHING, and the memory-projection axis reverts WHOLESALE to the
//      pre-fix behaviour — this session's entire running fleet priced at
//      zero — for every consult until its next real beat. The same ledger
//      that yields `memory-projection-block` with a fresh row yields
//      `spawn-allowed` with a 20-minute-old one. Reading the self row
//      unpruned would close this, but it is a genuine trade-off
//      (conservative on memory vs. blocking a session whose fleet has
//      actually finished, until its next beat) and is left open,
//      deliberately undecided, alongside the spawn-rate / live-agent-ceiling
//      residual above.
//   2. The candidate is priced as the unlabelled catch-all class rather than
//      the class actually about to spawn — a conservative direction, not a
//      permissive one.
//   3. A SAME-CLASS FLEET IS PRICED AS ONE AGENT. The persisted `agentClasses`
//      field the fallback reads is written by `declareDibs` as
//      `Array.from(new Set([agentClass, ...ownRunningAgentClasses]))` — i.e.
//      DEDUPLICATED. So N live agents of the SAME class are priced as 1 on
//      this advisory path: four live `implementer` agents cost what one does.
//      The REAL beat is unaffected, because it reads the raw, non-deduplicated
//      `--running-agent-classes` flag directly rather than the persisted set;
//      this is an advisory-fallback-only undercount. Direction is PERMISSIVE,
//      the same direction as residuals 1 and 2 — the gate can be more
//      permissive than a real beat, never less. Recorded as an accepted
//      residual, deliberately not closed.
//
// PURE-READER CLAIM, stated precisely — because an imprecise version of it was
// wrong. This is a claim about NET EFFECT on the coordination file across the
// whole hook process AND every child it spawns, not merely about this file's
// own `fs` calls:
//
//   - the hook itself performs exactly one coordination-file operation, the
//     `hasEverOptedIn` read at tier 1b — a bare `fs.readFile` that tolerates
//     absence. It never creates the file, its parent directory, its `.lock`
//     or a `.tmp-*` sibling, never writes it, and never acquires the
//     coordination lock.
//   - the tier-2 `cli.mjs` child is invoked in `--advise-only` mode, which is
//     that CLI's genuinely non-mutating advisory pipeline (collect ->
//     classify -> dibs READ -> traffic light). It declares no dibs, refreshes
//     no shared machine sample, advances OR COLD-START-SEEDS no AIMD ceiling
//     state, draws no global spawn-rate token, and claims no live-agent
//     admission capacity — and, since the second review round, takes no
//     coordination lock on ANY sub-path, so a contended lock elsewhere on the
//     host can no longer push the consultation past `CLI_TIMEOUT_MS` and
//     manufacture a fail-closed deny on a healthy host. These properties are
//     unconditional: they hold with the coordination file absent, populated,
//     or corrupted. See `--advise-only`'s own doc comment in `../cli.mjs` for
//     the exact list, and the ledger-immutability assertions in this hook's
//     two spec files (parameterised over AIMD row present / absent /
//     malformed) for the mechanical proof.
//
//   This distinction is load-bearing history, not pedantry: the hook
//   originally consulted `cli.mjs` as `--desired-agents=1`, which is a FULL
//   beat. The hook's own `fs` calls were read-only then too, so the naive
//   "pure reader" claim looked true — while the child process was silently
//   overwriting the calling session's declared `desiredAgents` (e.g. 4 -> 1),
//   double-drawing the shared spawn-rate bucket for one spawn, and re-sizing
//   the session's live-agent admission hold. Scope the claim to net effect, or
//   it will drift back into being false.
//
// The liveness-log append (a DIFFERENT file) is the hook's only write, and it
// is best-effort and BOUNDED — a contended log lock must not stack a
// multi-second wait on top of the 5s `cli.mjs` timeout, and a failed write
// must never flip the decision.
//
// TOOL-NAME MATCHER — empirically checked, still worth re-checking after any
// Claude Code upgrade. In a census of this host's local Claude Code
// transcripts under one project's `~/.claude/projects/<project-slug>/`
// directory, taken on 2026-09-05, every `tool_use` block naming a subagent-spawn tool
// named it `Agent` (a four-figure occurrence count at the time of the
// snapshot); `Task` appeared zero times and `Workflow` zero times. Treat those
// figures as a dated observation of one project's transcripts, not a standing
// fact — the point that survives is the RANKING (`Agent` is the live name
// here), not the tally. `Task` is nonetheless retained because it is the name
// older Claude Code versions used — matching both costs nothing and a matcher
// that named only one would silently never fire (the
// `worktreeMtimeMs`-always-null failure shape). Both names are pinned
// symmetrically by `describe.each`/`test.each` in both spec files, so dropping
// either one turns a suite red. `Workflow` is deliberately NOT matched: there
// is no evidence it exists here, and it is not obviously a subagent spawn.
// This set, and the `.claude/settings.json` matcher that routes events here,
// must always name the same tools — re-verify against a real hook payload
// before trusting the gate in production.
//
// Live-dispatch confirmation of this matcher, `hook_event_name`, and the
// `session_id` shape is tracked as a follow-up rather than blocking, not a
// merge gate.

import { spawnSync } from 'node:child_process';
import { writeSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join, resolve as resolvePath } from 'node:path';
import { fileURLToPath } from 'node:url';

/** Tool names treated as a subagent spawn — see the matcher note in the header. */
const SPAWN_TOOL_NAMES = new Set(['Task', 'Agent']);

/** this design's load-bearing mitigation: a hang must not stall Claude Code. */
const CLI_TIMEOUT_MS = 5_000;

/** Guards against a stdin that never closes (interactive misuse); normal reads end far sooner. */
const STDIN_TIMEOUT_MS = 2_000;

/**
 * Bound on the best-effort liveness-log append (~20 x 5ms + jitter). The log's
 * own default ceiling is 2000 x 5ms ~= 10s, which would otherwise stack on top
 * of `CLI_TIMEOUT_MS` and blow the deny's latency budget.
 */
const LOG_LOCK_MAX_ATTEMPTS = 20;
const LOG_LOCK_RETRY_DELAY_MS = 5;

/** Keeps a child's diagnostics readable on one line, and short. */
const DETAIL_MAX_LENGTH = 300;

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolvePath(HERE, '..', '..', '..');

/** `ARM_CLI_PATH` mirrors `ARM_COORDINATION_FILE`'s precedence convention exactly. */
function cliPath() {
  return process.env.ARM_CLI_PATH ? resolvePath(process.env.ARM_CLI_PATH) : join(HERE, '..', 'cli.mjs');
}

/** Same resolution as `cli.mjs`'s own, so both sides read one file. */
function coordinationFilePath() {
  return process.env.ARM_COORDINATION_FILE
    ? resolvePath(process.env.ARM_COORDINATION_FILE)
    : join(homedir(), '.claude/agent-state/resource-coordination.json');
}

/** Same resolution as `cli.mjs`'s own. */
function livenessLogPath() {
  return process.env.ARM_LIVENESS_LOG_FILE
    ? resolvePath(process.env.ARM_LIVENESS_LOG_FILE)
    : join(homedir(), '.claude/agent-state/liveness-log.json');
}

/**
 * `writeSync` rather than `process.stderr.write`: stderr is a pipe here, and a
 * buffered async write can be truncated by the `process.exit` that follows it.
 *
 * @param {string} message
 */
function emit(message) {
  try {
    writeSync(2, message);
  } catch {
    // A closed stderr must never turn a decision into a crash.
  }
}

function allow() {
  process.exit(0);
}

/**
 * @param {string} message
 * @returns {never}
 */
function deny(message) {
  emit(message);
  process.exit(2);
}

/**
 * Collapses whitespace and truncates, so a child's multi-line stack trace
 * cannot be replayed verbatim into the terminal reason.
 *
 * @param {unknown} value
 * @returns {string}
 */
function summarise(value) {
  const text = String(value ?? '').replace(/\s+/g, ' ').trim();
  return text.length > DETAIL_MAX_LENGTH ? `${text.slice(0, DETAIL_MAX_LENGTH)}…` : text;
}

/**
 * Reads the whole hook event from stdin, resolving `''` on any failure — every
 * unusable payload is a tier-0 allow, so there is nothing to distinguish.
 *
 * @returns {Promise<string>}
 */
function readStdin() {
  return new Promise((resolveStdin) => {
    let data = '';
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolveStdin(data);
    };
    const timer = setTimeout(finish, STDIN_TIMEOUT_MS);
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (chunk) => {
      data += chunk;
    });
    process.stdin.on('end', finish);
    process.stdin.on('error', finish);
  });
}

/**
 * @param {string} raw
 * @returns {Record<string, unknown> | null} the event object, or `null` for
 *   anything that is not a JSON object (including `null` and arrays).
 */
function parseEvent(raw) {
  try {
    const parsed = JSON.parse(raw);
    return parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

/**
 * The read-only consultation. Returns either a verdict or the reason no
 * trustworthy answer could be obtained.
 *
 * `--advise-only` — NOT `--desired-agents=1` — is what makes this genuinely a
 * query rather than a beat. See the pure-reader note in this file's header for
 * what the earlier `--desired-agents=1` form silently mutated.
 *
 * @param {string} orchestratorId
 * @returns {{ verdict: Record<string, unknown> } | { failure: string }}
 */
function consultCli(orchestratorId) {
  const result = spawnSync(
    process.execPath,
    [cliPath(), `--orchestrator-id=${orchestratorId}`, '--advise-only'],
    { cwd: REPO_ROOT, encoding: 'utf8', timeout: CLI_TIMEOUT_MS, env: process.env },
  );

  if (result.error) {
    return result.error.code === 'ETIMEDOUT'
      ? { failure: `it timed out after ${CLI_TIMEOUT_MS}ms` }
      : { failure: `it could not be started (${summarise(result.error.message)})` };
  }

  // J3 — a process that failed has not given a trustworthy answer, whatever
  // its stdout happens to say.
  if (result.status !== 0) {
    const stderr = summarise(result.stderr);
    return { failure: `it exited with status ${result.status}${stderr ? ` (${stderr})` : ''}` };
  }

  const stdout = String(result.stdout ?? '').trim();
  if (!stdout) return { failure: 'it produced no output' };

  let parsed;
  try {
    parsed = JSON.parse(stdout);
  } catch {
    return { failure: `its output was not JSON (${summarise(stdout)})` };
  }

  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed) || typeof parsed.type !== 'string') {
    return { failure: `its output was not a verdict object (${summarise(stdout)})` };
  }

  return { verdict: parsed };
}

/**
 * Best-effort, bounded, and never throwing: the decision is already made by
 * the time this runs.
 *
 * @param {string} orchestratorId
 * @param {string} reason
 * @param {string} toolName
 */
async function recordFailClosed(orchestratorId, reason, toolName) {
  try {
    const { logArmGateFailClosed } = await import('../lib/liveness-log.mjs');
    await logArmGateFailClosed(
      livenessLogPath(),
      { orchestratorId, outcome: 'denied', reason, toolName },
      { lockMaxAttempts: LOG_LOCK_MAX_ATTEMPTS, lockRetryDelayMs: LOG_LOCK_RETRY_DELAY_MS },
    );
  } catch {
    // `logArmGateFailClosed` already fails open internally; this catch covers
    // the import itself. Either way the deny below still stands.
  }
}

/**
 * @param {string} orchestratorId
 * @param {string} reason
 * @param {string} toolName
 * @returns {Promise<never>}
 */
async function denyFailClosed(orchestratorId, reason, toolName) {
  await recordFailClosed(orchestratorId, reason, toolName);
  return deny(
    'agent-resource-management: spawn blocked — failing closed. The ARM PreToolUse gate asked ' +
      `cli.mjs for a verdict and ${reason}. This session has opted into ARM, so the gate denies ` +
      'rather than guesses. Fix or unblock cli.mjs ' +
      `(${cliPath()}), or remove the PreToolUse entry from .claude/settings.json to opt out.\n`,
  );
}

async function main() {
  const event = parseEvent(await readStdin());

  // ---- tier 0: not a gated spawn event. Nothing read, nothing spawned. ----
  if (!event) allow();
  if (event.hook_event_name !== 'PreToolUse') allow();
  const toolName = event.tool_name;
  if (typeof toolName !== 'string' || !SPAWN_TOOL_NAMES.has(toolName)) allow();

  // ---- tier 1a: an underivable session identity fails open ( d.2). ----
  const { deriveOrchestratorId } = await import('../lib/orchestrator-id.mjs');
  let orchestratorId;
  try {
    orchestratorId = deriveOrchestratorId(event.session_id);
  } catch {
    allow();
  }

  // ---- tier 1b: never opted in. A lock-free, fail-open, non-creating read. ----
  const { hasEverOptedIn } = await import('../lib/coordination-file.mjs');
  if (!(await hasEverOptedIn(coordinationFilePath(), orchestratorId))) allow();

  // ---- tier 2: opted in — consult cli.mjs and honour its verdict. ----
  let outcome;
  try {
    outcome = consultCli(orchestratorId);
  } catch (error) {
    outcome = { failure: `the consultation itself failed (${summarise(error?.message)})` };
  }

  if ('failure' in outcome) {
    await denyFailClosed(orchestratorId, outcome.failure, toolName);
    return;
  }

  if (outcome.verdict.type === 'spawn-allowed') {
    // `spawn-allowed` is NOT on its own a permit. SKILL.md's own contract
    // (see "Live-agent ceiling and the `liveAgentGrant` field") says both
    // halves of this out loud: "An orchestrator that spawns on `type ===
    // 'spawn-allowed'` without checking `allowance > 0` first will attempt to
    // spawn with zero granted headroom", and "orchestrators that want to know
    // how many agents they may really start this beat must read
    // `liveAgentGrant`, not `allowance`". `{"type":"spawn-allowed",
    // "allowance":0}` is an explicit "you may spawn zero agents" and must
    // deny.
    //
    // WHICH FIELD ACTUALLY DECIDES, so this cannot silently rot into dead
    // logic. Today there is exactly one caller shape:
    //
    //   - `--advise-only` (the only mode `consultCli` uses) ALWAYS reports
    //     `liveAgentGrant: null`, because the live-agent ledger cannot be
    //     probed without claiming from it. `null` is not a finite number, so
    //     the `liveAgentGrant` clause below never fires today: `allowance` is
    //     the live check, and it is the one this gate rests on.
    //   - the `liveAgentGrant === 0` clause is therefore forward-looking, for
    //     a hypothetical future non-advisory consultation (one that really
    //     did reserve). It is kept because on such a beat `liveAgentGrant` is
    //     the AUTHORITATIVE number and `allowance` can legitimately report
    //     more headroom than the ledger granted — so a gate that checked only
    //     `allowance` would allow a spawn the ledger had already refused.
    //
    // Both clauses require a FINITE number before denying: a missing,
    // `null`, or non-numeric field is "not reported", never "zero". Treating
    // absence as a refusal would deny every advisory verdict outright.
    const { allowance, liveAgentGrant } = outcome.verdict;
    const grantsNothing =
      (typeof allowance === 'number' && Number.isFinite(allowance) && allowance <= 0) ||
      (typeof liveAgentGrant === 'number' && Number.isFinite(liveAgentGrant) && liveAgentGrant === 0);

    if (!grantsNothing) allow();

    // A POLICY deny, exactly like the non-`spawn-allowed` case below: ARM
    // answered clearly and the answer was "zero". No `arm-gate-fail-closed`
    // entry — nothing failed.
    deny(
      'agent-resource-management: spawn blocked — the ARM verdict for this session was "spawn-allowed" but ' +
        `granted nothing (allowance=${JSON.stringify(allowance)}, liveAgentGrant=${JSON.stringify(liveAgentGrant)}). ` +
        'That is an explicit "you may spawn zero agents", not a permit. Wait for headroom, or run the ' +
        "agent-resource-management skill's beat to see the current host state. If this keeps denying and " +
        'you want out, remove the PreToolUse entry from .claude/settings.json to opt out.\n',
    );
  }

  // J2 — a real, well-formed non-`spawn-allowed` verdict is a POLICY deny, not
  // a fail-closed trip, and is deliberately not written to the fail-closed
  // liveness bucket.
  //
  // `pause` reaches here like any other. Only `type` is read: an advisory
  // memory-RED verdict deliberately carries `pauseCandidate: null` rather than
  // paying for a second `ps` collection plus a per-agent
  // `footprint`/`vmmap` chain that could exceed `CLI_TIMEOUT_MS` and
  // manufacture a fail-closed deny (Phase 3, third review round,
  // Medium #2). Naming a process to pause is advice for the orchestrator that
  // owns the fleet; this gate is not that caller.
  deny(
    `agent-resource-management: spawn blocked — the ARM verdict for this session was "${outcome.verdict.type}", ` +
      'not "spawn-allowed". Wait for the signal to clear, or run the agent-resource-management skill\'s ' +
      'beat to see the current host state. If this keeps denying and you want out, remove the PreToolUse ' +
      'entry from .claude/settings.json to opt out.\n',
  );
}

main().catch(() => {
  // Nothing above is expected to reject — every known failure mode resolves to
  // an explicit allow/deny. An unexpected one must still not crash the hook,
  // and must not leave the tool call blocked on a bug in the gate itself.
  allow();
});
