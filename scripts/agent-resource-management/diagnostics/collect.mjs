// Phase 2 — diagnostics collector CLI orchestrator.
//
// Composes the Phase 1 probes (lib/recon.mjs, lib/footprint-sampler.mjs,
// lib/jetsam-parser.mjs, lib/diagnostic-reports-scanner.mjs,
// lib/log-show-probe.mjs, lib/since-window-parser.mjs) into one
// self-describing JSON report, in two modes:
//
//   `--live`        — current-instant memory/pressure/swap/process-tree
//                      snapshot, plus a physical-footprint sample per
//                      agent-rooted process tree.
//   `--retroactive` — forensic recovery of what already happened: scans
//                      `/Library/Logs/DiagnosticReports/` for JetsamEvent
//                      reports since a `--since` window, and probes the
//                      unified log for the same window.
//
// Every top-level probe result in the returned object carries an explicit
// `status: 'ok' | 'skipped' | 'failed'` — a probe that could not run (missing
// binary, permission denied, malformed data) is REPORTED, never silently
// dropped. `runCollection` itself never throws or rejects for a probe-level
// failure; only `parseCliArgs` (a pure, synchronous, separate function)
// rejects malformed CLI input, and it does so before any probe ever runs.
//
// NEVER shells out to `sudo` anywhere in this file.

import { execFileSync } from 'node:child_process';
// Namespace import (not named), deliberately: this module is composed
// together with lib/diagnostic-reports-scanner.mjs (imports `readdir`/`lstat`
// from this same specifier) under Jest's `jest.unstable_mockModule` ESM
// mocking, and each of this file's two spec files mocks a DIFFERENT, disjoint
// subset of `node:fs/promises`'s named exports (collect.jest.spec.mjs mocks
// `readdir`/`readFile`/`lstat`; collect-cli.jest.spec.mjs mocks
// `mkdir`/`writeFile`/`chmod`). A named import (`import { mkdir } from ...`)
// is a live binding checked at ESM link time — it throws a `SyntaxError`
// immediately if the mock factory in effect for that test file doesn't
// provide it, even for a function this file never calls in that test. A
// namespace import defers property access to call time, so each spec's own
// (correctly narrower) mock links cleanly.
import * as fsPromises from 'node:fs/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { dirname, join } from 'node:path';

import {
  parseVmStat,
  parsePressureLevel,
  parseSwapUsage,
  PS_HEADER_WITH_ETIME_PATTERN,
  PS_LINE_WITH_ETIME_PATTERN,
  AGENT_ROOT_COMM_PATTERN,
} from '../lib/recon.mjs';
import { sampleFootprint } from '../lib/footprint-sampler.mjs';
import { parseJetsamEventReport } from '../lib/jetsam-parser.mjs';
import { runLogShow } from '../lib/log-show-probe.mjs';
import { parseSinceWindow } from '../lib/since-window-parser.mjs';
import { probeExecOptions } from '../lib/probe-bound.mjs';

// lib/diagnostic-reports-scanner.mjs is loaded lazily (dynamic `import()`,
// inside collectRetroactiveFindings below) for the same reason `fs/promises`
// is namespace-imported above: it is the one Phase 1 module that itself
// statically named-imports `readdir`/`lstat` from `node:fs/promises`, so a
// static top-level import of it here would link-fail under
// collect-cli.jest.spec.mjs's mock (which mocks a disjoint subset and never
// exercises this module at all). Deferring to a dynamic import means it is
// only ever linked by a test that actually calls into the retroactive path
// and has mocked `node:fs/promises` accordingly.

// ---------------------------------------------------------------------------
// Absolute paths for every shelled-out binary — fixed macOS system tools at
// fixed locations, matching cli.mjs's own VM_STAT_BIN/SYSCTL_BIN/PS_BIN
// convention rather than relying on PATH resolution.
// ---------------------------------------------------------------------------

const VM_STAT_BIN = '/usr/bin/vm_stat';
const SYSCTL_BIN = '/usr/sbin/sysctl';
const PS_BIN = '/bin/ps';

/**
 * Shells out for a probe, capturing stdout as utf8 text and never throwing —
 * the caller wraps the result in a `{ status, ... }` envelope, so a thrown
 * error (missing binary, non-zero exit) is this function's ONLY failure
 * signal; it deliberately does not distinguish ENOENT from any other
 * failure, since every caller treats them identically (`status: 'failed'`).
 *
 * Bounded via `probeExecOptions()` from `lib/probe-bound.mjs` — the same
 * wall-clock ceiling `footprint-sampler.mjs` and `log-show-probe.mjs` already
 * apply to their own shell-outs — so a hung `vm_stat`/`sysctl`/`ps` on a
 * wedged host cannot stall the whole collection run indefinitely. A timeout
 * kill is reported like any other probe failure (`ok: false`); this function
 * deliberately does not distinguish it from other error causes, matching the
 * existing "every caller treats them identically" contract below.
 *
 * @param {string} bin
 * @param {string[]} args
 * @returns {{ ok: true, stdout: string } | { ok: false, error: string }}
 */
function runShellProbe(bin, args) {
  try {
    const stdout = execFileSync(bin, args, { ...probeExecOptions(), stdio: 'pipe' });
    return { ok: true, stdout };
  } catch (error) {
    return { ok: false, error: `${bin}: ${error.message}` };
  }
}

// ---------------------------------------------------------------------------
// Raw ps-row parsing — collect.mjs's own reporting needs the RAW pid/ppid/comm
// rows (so `excludeSelfFromProcessTree` can drop this process's own subtree),
// which is a different shape from anything `lib/recon.mjs` exports today (its
// own row grammar is internal to `listAgentProcesses`/`attributeAgentProcesses`).
// This mirrors that internal grammar (`pid,ppid,rss,etime,comm`) by importing
// recon.mjs's own exported `PS_HEADER_WITH_ETIME_PATTERN` (header skip),
// `PS_LINE_WITH_ETIME_PATTERN` (row shape), and `AGENT_ROOT_COMM_PATTERN`
// (agent-root detection) as the single source of truth, rather than
// maintaining byte-for-byte local copies that can silently drift — without
// duplicating any of recon.mjs's own attribution/authorization logic.
// ---------------------------------------------------------------------------

/**
 * Rows that fail to match `PS_LINE_WITH_ETIME_PATTERN` are silently dropped
 * here — UNLIKE `lib/recon.mjs`'s `listAgentProcesses`, which surfaces a
 * non-enumerable `unparseableRows` signal for exactly this case (added after
 * real incidents of garbled `ps` rows hiding live agents from
 * eviction bookkeeping). That tracking is deliberately NOT replicated here:
 * this function's only consumer is footprint-sampling target selection for a
 * diagnostics/reporting snapshot, not an authorization-security decision —
 * a dropped row here just means one less footprint sample in a report, not a
 * live agent silently surviving past its eviction window. If a future
 * consumer of this parser needs eviction-grade guarantees, adopt recon.mjs's
 * `unparseableRows` mechanism rather than reimplementing a weaker one here.
 *
 * @param {string} raw `ps -Ao pid,ppid,rss,etime,comm`-style text.
 * @returns {Array<{ pid: string, ppid: string, rssKb: number, etime: string, comm: string }>}
 */
function parseProcessRows(raw) {
  const text = String(raw ?? '');
  const rows = [];

  for (const line of text.split('\n').map((entry) => entry.trim()).filter(Boolean)) {
    if (PS_HEADER_WITH_ETIME_PATTERN.test(line)) continue;

    const match = line.match(PS_LINE_WITH_ETIME_PATTERN);
    if (!match) continue;

    const [, pid, ppid, rss, etime, comm] = match;
    rows.push({ pid, ppid, rssKb: Number(rss), etime, comm });
  }

  return rows;
}

// ---------------------------------------------------------------------------
// excludeSelfFromProcessTree — pure, synchronous self + transitive-descendant
// exclusion from a raw `{ pid, ppid, ... }` process array.
// ---------------------------------------------------------------------------

/**
 * @param {Array<{ pid: string | number, ppid: string | number }>} processes
 * @param {string | number} selfPid
 * @returns {Array<object>}
 */
export function excludeSelfFromProcessTree(processes, selfPid) {
  const list = Array.isArray(processes) ? processes : [];
  const selfPidStr = String(selfPid);

  /** @type {Map<string, string[]>} */
  const childrenByPpid = new Map();
  for (const proc of list) {
    const ppidStr = String(proc.ppid);
    if (!childrenByPpid.has(ppidStr)) childrenByPpid.set(ppidStr, []);
    childrenByPpid.get(ppidStr).push(String(proc.pid));
  }

  const excluded = new Set();
  const stack = [selfPidStr];
  while (stack.length > 0) {
    const pid = stack.pop();
    if (excluded.has(pid)) continue;
    excluded.add(pid);

    for (const childPid of childrenByPpid.get(pid) ?? []) {
      if (!excluded.has(childPid)) stack.push(childPid);
    }
  }

  return list.filter((proc) => !excluded.has(String(proc.pid)));
}

// ---------------------------------------------------------------------------
// Live-mode probes
// ---------------------------------------------------------------------------

/**
 * `parseVmStat`'s own `pressureLevel` is a ratio-derived fallback/test
 * heuristic ONLY (see its doc comment in `lib/recon.mjs`) — the authoritative
 * pressure reading for a report is the sysctl-derived one already captured
 * separately by `collectPressureReading()` at `result.pressure.pressureLevel`.
 * Spreading `parseVmStat`'s output wholesale would put a second,
 * differently-derived `pressureLevel` inside this `memory` envelope, so it is
 * destructured out here and re-added under an unambiguous name rather than
 * left to collide with the authoritative field.
 *
 * @returns {object} self-describing `status: 'ok' | 'failed'` envelope.
 */
function collectMemoryReading() {
  const result = runShellProbe(VM_STAT_BIN, []);
  if (!result.ok) return { status: 'failed', error: result.error };

  const parsed = parseVmStat(result.stdout);
  // Fail on ANY unparsed field, not only when every field fails together —
  // a single field failing (e.g. `vm_stat` output-format drift affecting one
  // line) must be REPORTED, never silently serialized as a `null` inside an
  // otherwise `status: 'ok'` envelope. See this file's stated invariant: a
  // probe that could not fully run is reported, never silently dropped.
  if (Number.isNaN(parsed.compressedMb) || Number.isNaN(parsed.freeRamMb)) {
    return { status: 'failed', error: 'collect: vm_stat output did not match the expected format' };
  }
  // `vmStatHeuristicPressureLevel` (`parseVmStat`'s `pressureLevel`) is
  // explicitly documented in `lib/recon.mjs` as a SECONDARY, fallback/test
  // heuristic — never the authoritative pressure signal (that's
  // `collectPressureReading()`'s sysctl-derived reading). It is derived from
  // a wider set of `vm_stat` fields (active/inactive/speculative/wired page
  // counts) than `compressedMb`/`freeRamMb` above, so it can independently go
  // `NaN` even when those two parse cleanly (e.g. a single unrelated
  // `vm_stat` line fails to match on a future OS version). Unlike
  // `compressedMb`/`freeRamMb`, which are load-bearing enough that any NaN
  // fails the whole probe, this field is allowed to degrade on its own — but
  // per this file's "never silently null" invariant, a NaN heuristic is
  // omitted from the envelope entirely rather than serialized as `null`,
  // so its absence is an explicit, visible partial degradation rather than
  // an indistinguishable-from-healthy null value.
  const { pressureLevel: vmStatHeuristicPressureLevel, ...rest } = parsed;
  return {
    status: 'ok',
    ...rest,
    ...(Number.isNaN(vmStatHeuristicPressureLevel) ? {} : { vmStatHeuristicPressureLevel }),
  };
}

/**
 * @returns {object}
 */
function collectPressureReading() {
  const result = runShellProbe(SYSCTL_BIN, ['-n', 'kern.memorystatus_vm_pressure_level']);
  if (!result.ok) return { status: 'failed', error: result.error };

  const pressureLevel = parsePressureLevel(result.stdout);
  if (Number.isNaN(pressureLevel)) {
    return { status: 'failed', error: 'collect: sysctl pressure output did not match the expected format' };
  }
  return { status: 'ok', pressureLevel };
}

/**
 * @returns {object}
 */
function collectSwapReading() {
  const result = runShellProbe(SYSCTL_BIN, ['vm.swapusage']);
  if (!result.ok) return { status: 'failed', error: result.error };

  const parsed = parseSwapUsage(result.stdout);
  // Same OR-not-AND fix as `collectMemoryReading` above: any single field
  // failing to parse must fail the probe, not just both together.
  if (Number.isNaN(parsed.swapUsedMb) || Number.isNaN(parsed.swapTotalMb)) {
    return { status: 'failed', error: 'collect: sysctl vm.swapusage output did not match the expected format' };
  }
  return { status: 'ok', ...parsed };
}

/**
 * @returns {{ status: 'ok' | 'failed', list: Array<object>, error?: string }}
 */
function collectProcessTree() {
  const result = runShellProbe(PS_BIN, ['-Ao', 'pid,ppid,rss,etime,comm']);
  if (!result.ok) return { status: 'failed', list: [], error: result.error };

  const rows = parseProcessRows(result.stdout);
  const filtered = excludeSelfFromProcessTree(rows, process.pid);
  return { status: 'ok', list: filtered };
}

// Concurrency cap for `collectFootprintSamples` below. `sampleFootprint`
// shells out to `footprint`/`vmmap` — non-trivially expensive tools — and
// this collector exists specifically to observe a host that may already be
// under severe memory/CPU pressure; firing an unbounded burst of them (one
// per agent-rooted pid, however many that is) risks adding load during the
// very incident this tool is meant to diagnose. A small fixed batch size
// keeps `--live` a light touch regardless of how many agent processes are
// present. `sampleFootprint`'s current implementation happens to be
// internally synchronous (`execFileSync`, see its own header comment), so
// this cap does not change today's actual wall-clock behaviour — but it is
// the correct, forward-looking guard: it caps real concurrency the moment
// `sampleFootprint` (or a future caller of `runInBatches`) becomes
// genuinely non-blocking, without anyone having to remember to add a cap
// later.
const FOOTPRINT_SAMPLE_BATCH_SIZE = 4;

/**
 * Runs `worker` over `items`, N at a time (`batchSize`), awaiting each batch
 * fully (via `Promise.all`) before starting the next. Preserves input order
 * in the returned array. A minimal, dependency-free alternative to pulling
 * in a concurrency-limiting library for this one call site.
 *
 * @template T, R
 * @param {T[]} items
 * @param {number} batchSize
 * @param {(item: T) => Promise<R>} worker
 * @returns {Promise<R[]>}
 */
async function runInBatches(items, batchSize, worker) {
  const results = [];
  for (let start = 0; start < items.length; start += batchSize) {
    const batch = items.slice(start, start + batchSize);
    const batchResults = await Promise.all(batch.map(worker));
    results.push(...batchResults);
  }
  return results;
}

/**
 * Samples physical footprint for every agent-rooted process this collection
 * observed (after self-exclusion) via `lib/footprint-sampler.mjs`. `status`
 * is `'skipped'` when there was nothing to sample (no agent-rooted process in
 * the tree), `'ok'` when at least one sample succeeded, and `'failed'` when
 * every attempted sample came back `null` (missing `footprint`/`vmmap`
 * binaries, or the pid no longer exists).
 *
 * Sampled in fixed-size batches (`FOOTPRINT_SAMPLE_BATCH_SIZE`), never all at
 * once — see that constant's own comment for the rationale.
 *
 * @param {Array<{ pid: string, comm: string }>} processRows
 * @returns {Promise<{ status: 'ok' | 'skipped' | 'failed', samples: Array<{ pid: string, footprintMb: number | null }> }>}
 */
async function collectFootprintSamples(processRows) {
  const agentPids = processRows.filter((proc) => AGENT_ROOT_COMM_PATTERN.test(proc.comm)).map((proc) => proc.pid);

  if (agentPids.length === 0) {
    return { status: 'skipped', samples: [] };
  }

  // `honorFakeSeam: false` — production diagnostics collection must never
  // honor the `ARM_FAKE_FOOTPRINT_OUTPUT` test seam. If that env var were set
  // in the ambient environment for any reason, honoring it here would
  // silently replace every footprint sample in a `--live` report with
  // fabricated data, indistinguishable from a real reading — worse than a
  // reported failure in a forensics tool.
  const samples = await runInBatches(agentPids, FOOTPRINT_SAMPLE_BATCH_SIZE, async (pid) => ({
    pid,
    footprintMb: await sampleFootprint(Number(pid), false),
  }));

  const status = samples.some((sample) => sample.footprintMb !== null) ? 'ok' : 'failed';
  return { status, samples };
}

// ---------------------------------------------------------------------------
// Retroactive-mode probes
// ---------------------------------------------------------------------------

/**
 * Runs a single ASYNC probe step and degrades ANY exception it throws (or
 * rejects with) to a `{ ...fallback, status: 'failed', error }` envelope —
 * the async counterpart of `runProbeIsolated` above, for the retroactive
 * branch's own sub-steps. Isolating `collectDiagnosticReportsFindings` and
 * `collectLogShowFindings` behind this (rather than one shared try/catch
 * around both, as `collectRetroactiveFindings` used to have) means an
 * unexpected throw from either sub-step — e.g. the dynamically-imported
 * `scanDiagnosticReports` misbehaving — degrades ONLY that sub-step, never
 * discarding the other sub-step's already-successful result. This is the
 * same "one probe's failure never prevents others from running or being
 * reported" guarantee the `live` branch already gets structurally, applied
 * to `retroactive`'s two sub-steps.
 *
 * @param {() => Promise<object>} probeFn
 * @param {object} fallback shape merged under the failure envelope so callers
 *   can still destructure the fields they expect (e.g. `entries: []`).
 * @returns {Promise<object>}
 */
async function runAsyncProbeIsolated(probeFn, fallback) {
  try {
    return await probeFn();
  } catch (error) {
    return { ...fallback, status: 'failed', error: `collect: ${error.message}` };
  }
}

/**
 * Scans `/Library/Logs/DiagnosticReports/` for JetsamEvent reports since
 * `since` and reads+parses each match. Isolated into its own function so it
 * can be wrapped by `runAsyncProbeIsolated` independently of the `log show`
 * sub-step in `collectRetroactiveFindings`.
 *
 * @param {Date} since
 * @returns {Promise<{ status: 'ok' | 'failed', files: Array<object>, jetsamFindings: Array<object>, unparseable: Array<object>, error?: string }>}
 */
async function collectDiagnosticReportsFindings(since) {
  const { scanDiagnosticReports } = await import('../lib/diagnostic-reports-scanner.mjs');
  const scan = await scanDiagnosticReports({ since });

  // `jetsamFindings` holds only SUCCESSFULLY parsed reports (fed into
  // `findings` below); `jetsamUnparseable` holds files that were found by the
  // scan but could not be turned into a finding — either the read itself
  // failed (permissions, vanished mid-scan) or `parseJetsamEventReport`
  // rejected the contents (truncated/garbled `.ips`). Keeping these two
  // buckets distinct is deliberate: collapsing them back into one filtered
  // list (as an earlier version of this function did) throws away the
  // difference between "we looked and found nothing" and "we found files but
  // couldn't read them" — exactly the ambiguity this tool exists to remove.
  const jetsamFindings = [];
  const jetsamUnparseable = [];
  for (const file of scan.files) {
    if (!/jetsam/i.test(file.name)) continue;

    // TOCTOU guard: `scan.files` was symlink-checked by the scanner's own
    // `lstat`, but that check happened earlier in this same async function
    // call — a window wide enough for another local process to swap the
    // entry for a symlink to an arbitrary file (e.g. an SSH key) before we
    // read it. Re-`lstat` immediately before the read and refuse to follow a
    // symlink discovered here, exactly like the scanner itself does, rather
    // than trusting a stat result that is no longer current.
    try {
      const freshStats = await fsPromises.lstat(file.path);
      if (freshStats.isSymbolicLink()) {
        jetsamUnparseable.push({
          file: file.path,
          error: 'collect: entry became a symlink between scan and read — refusing to follow',
        });
        continue;
      }
    } catch (error) {
      jetsamUnparseable.push({ file: file.path, error: `collect: ${error.message}` });
      continue;
    }

    let fileContents;
    try {
      fileContents = await fsPromises.readFile(file.path, 'utf8');
    } catch (error) {
      jetsamUnparseable.push({ file: file.path, error: `collect: ${error.message}` });
      continue;
    }

    const parsed = parseJetsamEventReport(fileContents);
    if (parsed.available) {
      jetsamFindings.push({ file: file.path, ...parsed });
    } else {
      jetsamUnparseable.push({ file: file.path, error: parsed.error });
    }
  }

  // A scan that ran to completion and legitimately found zero matching files
  // is NOT "skipped" (this file's own header defines 'skipped' as "there was
  // nothing to probe") — it is the canonical "we looked and found nothing"
  // case, which this whole tool exists to distinguish from "we didn't look".
  // `status: 'skipped'` is therefore never reached here; it is reserved for a
  // probe that genuinely did not run, which for this scanner is indicated by
  // `scan.reason` and already reported as `'failed'` above.
  const diagnosticReportsStatus = scan.reason ? 'failed' : 'ok';

  return {
    status: diagnosticReportsStatus,
    files: scan.files,
    jetsamFindings,
    // Files the scan found but this function could not turn into a
    // finding — surfaced alongside `findings` rather than folded into it,
    // so a consumer can tell "nothing was there" apart from "something was
    // there but unreadable/corrupt". See the buckets comment above.
    unparseable: jetsamUnparseable,
    ...(scan.reason ? { error: scan.reason } : {}),
  };
}

/**
 * Probes the unified log for Jetsam/memorystatus messages since `since`.
 * Isolated into its own function so it can be wrapped by
 * `runAsyncProbeIsolated` independently of the diagnostic-reports sub-step in
 * `collectRetroactiveFindings`.
 *
 * @param {Date} since
 * @returns {Promise<{ status: 'ok' | 'failed', entries: Array<object>, error?: string }>}
 */
async function collectLogShowFindings(since) {
  return runLogShow({
    // `contains[c]` (case-insensitive), not plain `contains` — real
    // kernel/memorystatus messages vary in case by OS version/subsystem, and
    // NSPredicate's `contains` is case-sensitive by default.
    predicate: 'eventMessage contains[c] "Jetsam" or eventMessage contains[c] "memorystatus"',
    since: since.toISOString(),
  });
}

/**
 * @param {Date} since
 * @returns {Promise<object>}
 */
async function collectRetroactiveFindings(since) {
  const diagnosticReportsResult = await runAsyncProbeIsolated(
    () => collectDiagnosticReportsFindings(since),
    { files: [], jetsamFindings: [], unparseable: [] },
  );
  const logResult = await runAsyncProbeIsolated(() => collectLogShowFindings(since), { entries: [] });

  const findings = [
    ...diagnosticReportsResult.jetsamFindings,
    ...logResult.entries.map((entry) => ({ source: 'log-show', entry })),
  ];

  return {
    diagnosticReports: {
      status: diagnosticReportsResult.status,
      files: diagnosticReportsResult.files,
      unparseable: diagnosticReportsResult.unparseable,
      ...(diagnosticReportsResult.error ? { error: diagnosticReportsResult.error } : {}),
    },
    logShow: logResult,
    // "Genuinely nothing was found" — false if EITHER a finding was recovered,
    // a file was found but could not be parsed (a known-unknown, not a clean
    // "nothing here"), OR either underlying probe failed outright (permission
    // denied on the reports directory, `log show` erroring/timing out, or an
    // unexpected throw isolated by `runAsyncProbeIsolated`). A failed probe
    // means we never actually looked — collapsing that into the same `true`
    // as "we looked thoroughly and found nothing" is exactly the ambiguity
    // this tool exists to remove, so a failed probe must never report a
    // clean null result.
    nothingRecoverable:
      findings.length === 0 &&
      diagnosticReportsResult.unparseable.length === 0 &&
      diagnosticReportsResult.status !== 'failed' &&
      logResult.status !== 'failed',
    findings,
  };
}

// ---------------------------------------------------------------------------
// runCollection — the composed entrypoint. Never throws/rejects.
// ---------------------------------------------------------------------------

/**
 * Runs a single synchronous probe function and degrades ANY exception it
 * throws (not just the shell-exec failures each probe already catches
 * internally) to the same `{ status: 'failed', error }` envelope every other
 * probe failure produces. Isolating each probe like this — rather than
 * letting one throw abort every probe after it inside a single shared
 * try/catch — is what makes the "one probe's failure never prevents the
 * others from running or being reported" guarantee documented at the top of
 * this file structurally true, instead of true only for the shell-exec
 * sub-case the probes themselves already handle.
 *
 * @param {() => object} probeFn
 * @returns {object}
 */
function runProbeIsolated(probeFn) {
  try {
    return probeFn();
  } catch (error) {
    return { status: 'failed', error: `collect: ${error.message}` };
  }
}

/**
 * @param {{ mode: 'live' | 'retroactive', since?: string | Date }} params
 * @returns {Promise<object>}
 */
export async function runCollection({ mode, since } = {}) {
  const generatedAt = new Date().toISOString();
  const result = { mode, generatedAt };

  try {
    if (mode === 'live') {
      result.memory = runProbeIsolated(collectMemoryReading);
      result.pressure = runProbeIsolated(collectPressureReading);
      result.swap = runProbeIsolated(collectSwapReading);

      const processTree = runProbeIsolated(collectProcessTree);
      result.processes = processTree;
      // A failed process-tree probe has no `list` to sample footprints from —
      // treat that as nothing-to-sample (`skipped`) rather than crashing the
      // one probe (footprints) that depends on another probe's output.
      try {
        result.footprints = await collectFootprintSamples(processTree.list ?? []);
      } catch (error) {
        result.footprints = { status: 'failed', samples: [], error: `collect: ${error.message}` };
      }
    } else if (mode === 'retroactive') {
      // `since` is already a `Date` on the shipped CLI path (parseCliArgs
      // validates and converts it via parseSinceWindow before runCollection
      // is ever called). A direct caller of this exported function may pass
      // a raw string (or omit `since` entirely) instead — route that through
      // the SAME `parseSinceWindow` validator used everywhere else, rather
      // than constructing a `Date` by hand, so this is never a second,
      // divergent source of truth for what counts as a valid `since` value
      // (see this file's header comment). An invalid/unparseable value here
      // throws and is caught by the belt-and-braces handler below, degrading
      // to `result.unexpectedError` exactly like any other unexpected
      // failure in this function — never a silently-wrong `Invalid Date`.
      let sinceDate;
      if (since instanceof Date) {
        sinceDate = since;
      } else {
        const sinceResult = parseSinceWindow(since);
        if (!sinceResult.valid) {
          throw new Error(sinceResult.error);
        }
        sinceDate = sinceResult.since;
      }
      result.retroactive = await collectRetroactiveFindings(sinceDate);
    } else {
      // Aligned with every other branch's convention: attach the outcome
      // under a probe-named key holding a `{ status, ... }` envelope
      // (`result.memory`, `result.retroactive`, etc.), rather than a bespoke
      // `result.error` key no consumer of this function's other branches
      // expects. `result.mode` itself already holds the raw `mode` string
      // (set at the top of this function, for every branch) — reusing that
      // key here would silently clobber it, so the branch-specific envelope
      // is reported under its own descriptive key instead.
      result.unknownMode = { status: 'failed', error: `collect: unknown mode "${mode}"` };
    }
  } catch (error) {
    // Belt-and-braces: every probe above already catches its own failures,
    // but `runCollection` itself must NEVER throw/reject regardless — a bug
    // in a probe's own error handling must still degrade to a reported
    // failure, not an uncaught rejection the outer acceptance test forbids.
    result.unexpectedError = { status: 'failed', error: `collect: ${error.message}` };
  }

  return result;
}

// ---------------------------------------------------------------------------
// parseCliArgs — pure, synchronous CLI flag parsing. Never throws.
// ---------------------------------------------------------------------------

const RECOGNIZED_FLAGS = new Set(['live', 'retroactive', 'since']);
// `--live` / `--retroactive` are bare boolean switches, not value-taking
// flags — unlike `--since`. Without rejecting an attached value here,
// `--live=false` (a plausible typo/copy-paste from another tool's flag
// convention, intending to DISABLE live mode) would silently set `hasLive`
// to `true` anyway (the check downstream is presence, not truthiness),
// doing the opposite of what was almost certainly intended with no error at
// all — undermining the "rejects garbage input" contract this parser
// otherwise holds `--since` to.
const BOOLEAN_FLAGS = new Set(['live', 'retroactive']);

/**
 * @param {string[]} argv
 * @returns {{ valid: true, mode: 'live' | 'retroactive', since?: Date } | { valid: false, error: string }}
 */
export function parseCliArgs(argv) {
  const args = Array.isArray(argv) ? argv : [];
  const flags = {};

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (typeof arg !== 'string') continue;
    // A token that isn't itself a `--flag` AND wasn't consumed above as the
    // preceding flag's value (that branch advances `i` past it) is a stray
    // positional argument — e.g. `node collect.mjs extra --live`. Silently
    // `continue`-ing past it here would undermine this parser's own
    // documented "reject garbage input" stance (already enforced for
    // `--live=false` and a value trailing a bare boolean flag below) for the
    // one shape neither of those checks catches: a positional token that
    // never sits immediately after a recognized flag.
    if (!arg.startsWith('--')) {
      return { valid: false, error: `collect: unexpected positional argument "${arg}"` };
    }

    const eqIndex = arg.indexOf('=');
    let key;
    let value;
    if (eqIndex !== -1) {
      key = arg.slice(2, eqIndex);
      value = arg.slice(eqIndex + 1);
    } else {
      key = arg.slice(2);
      const next = args[i + 1];
      if (next === undefined || (typeof next === 'string' && next.startsWith('--'))) {
        value = true;
      } else {
        value = next;
        i += 1;
      }
    }

    if (!RECOGNIZED_FLAGS.has(key)) {
      return { valid: false, error: `collect: unrecognized flag "--${key}"` };
    }
    if (BOOLEAN_FLAGS.has(key) && value !== true) {
      return { valid: false, error: `collect: "--${key}" does not take a value (got "${value}")` };
    }
    flags[key] = value;
  }

  const hasLive = flags.live !== undefined;
  const hasRetroactive = flags.retroactive !== undefined;

  if (hasLive && hasRetroactive) {
    return { valid: false, error: 'collect: --live and --retroactive are mutually exclusive' };
  }
  if (!hasLive && !hasRetroactive) {
    return { valid: false, error: 'collect: one of --live or --retroactive is required' };
  }

  if (hasLive) {
    if (flags.since !== undefined) {
      return { valid: false, error: 'collect: --since is only valid alongside --retroactive' };
    }
    return { valid: true, mode: 'live' };
  }

  // `flags.since === true` means `--since` was typed with no value attached
  // (bare trailing flag, or immediately followed by another `--flag`) — a
  // forgotten value, not "no --since given". Silently falling back to the
  // default window here would make `--retroactive --since` behave
  // identically to `--retroactive` alone with no error at all, undermining
  // this parser's otherwise strict "reject garbage input" stance (it already
  // rejects `--live=false` and stray positional tokens for the same reason).
  if (flags.since === true) {
    return { valid: false, error: 'collect: "--since" requires a value' };
  }

  const sinceResult = parseSinceWindow(flags.since);
  if (!sinceResult.valid) {
    return { valid: false, error: sinceResult.error };
  }

  return { valid: true, mode: 'retroactive', since: sinceResult.since };
}

// ---------------------------------------------------------------------------
// writeCollectionOutput — writes the collection result to a timestamped,
// collision-resistant JSON file under `outputDir`.
// ---------------------------------------------------------------------------

const DEFAULT_OUTPUT_DIR = join(dirname(fileURLToPath(import.meta.url)), 'output');

/**
 * @param {object} result
 * @param {{ outputDir?: string }} [options]
 * @returns {Promise<{ filePath: string }>}
 */
export async function writeCollectionOutput(result, { outputDir = DEFAULT_OUTPUT_DIR } = {}) {
  try {
    // Restrictive mode: the directory would otherwise inherit the process
    // umask (commonly 0755), making it world-readable — filenames embed
    // timestamps/PIDs even though individual files are already `0600`.
    await fsPromises.mkdir(outputDir, { recursive: true, mode: 0o700 });
  } catch (error) {
    if (error.code !== 'EEXIST') throw error;
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const randomSuffix = Math.random().toString(36).slice(2, 8);
  const fileName = `collect-${timestamp}-${process.pid}-${randomSuffix}.json`;
  const filePath = join(outputDir, fileName);

  await fsPromises.writeFile(filePath, JSON.stringify(result, null, 2), { mode: 0o600 });

  return { filePath };
}

// ---------------------------------------------------------------------------
// CLI entry point — only runs when this file is executed directly
// (`node collect.mjs --live` / `node collect.mjs --retroactive [--since ...]`),
// not when imported by tests. Mirrors cli.mjs's own no-shebang, node-invoked
// convention (no execute bit needed).
// ---------------------------------------------------------------------------

// `pathToFileURL(...).href` comparison (rather than the naive
// `file://${process.argv[1]}` template) is required for correctness on any
// invocation path containing a space or a non-ASCII character — the naive
// form does not percent-encode those, so it silently fails to match and this
// entry-point guard would never fire.
if (process.argv[1] !== undefined && pathToFileURL(process.argv[1]).href === import.meta.url) {
  const parsedArgs = parseCliArgs(process.argv.slice(2));

  if (!parsedArgs.valid) {
    process.stderr.write(`${parsedArgs.error}\n`);
    process.stderr.write('Usage: node collect.mjs --live | --retroactive [--since <ISO date>]\n');
    process.exit(2);
  } else {
    const result = await runCollection({ mode: parsedArgs.mode, since: parsedArgs.since });

    // Print the collected JSON FIRST, before ever attempting the write. A
    // disk-full/read-only-fs/permission-denied failure below must never cost
    // the caller the diagnostics that were already successfully gathered —
    // that would defeat the entire point of a forensics tool. Only the write
    // itself is guarded by the try/catch that follows; a failure there
    // degrades to a stderr warning, and the process still exits 0, because
    // the actual diagnostics work already succeeded.
    process.stdout.write(JSON.stringify(result, null, 2) + '\n');

    try {
      const { filePath } = await writeCollectionOutput(result);
      process.stderr.write(`collect: wrote diagnostics report to ${filePath}\n`);
    } catch (error) {
      process.stderr.write(
        `warning: failed to write output file: ${error.message}; JSON above is still valid\n`,
      );
    }

    // No explicit `process.exit(0)` here: `process.stdout.write` above is
    // asynchronous, and calling `process.exit` immediately after it does not
    // wait for that write to flush — piping this tool's output (`| jq`,
    // `| tee`, `| pbcopy`) could silently truncate the JSON, exactly the
    // failure mode the "print first" ordering above was meant to prevent.
    // Node exits naturally, with the correct code 0, once the event loop is
    // empty — nothing else here keeps it alive.
  }
}
