// Phase 0 — OUTER acceptance test for
// `scripts/agent-resource-management/diagnostics/collect.mjs`, the
// diagnostics collector.
//
// `collect.mjs` now exists and this suite passes against it. The RED-phase
// framing below (this test predates the implementation) is kept for context
// on the contract it locked in, not as a live instruction.
//
// Outer acceptance criterion under test (verbatim from the ticket): "collect.mjs
// --live and --retroactive always exit 0 with self-describing JSON, even when
// every privileged probe fails or is missing."
//
// SHAPE THIS TEST ASSUMES (the contract the builder must satisfy — chosen by
// this test's author, not yet implemented):
//   export async function runCollection({ mode: 'live' | 'retroactive', since? })
//     -> Promise<object>  (never throws/rejects for probe-level failures —
//        only argument-validation failures, e.g. a malformed --since, may
//        reject; that path is out of scope for this outer test and is
//        exercised by a narrower unit spec in a later phase.)
// A thin CLI wrapper (`if (import.meta.url === ...) { ... process.exit(0) }`)
// is expected to sit around this function so `node collect.mjs --live` exits
// 0 — we assert the necessary condition for that (the promise resolves
// rather than throws/rejects) without spawning a subprocess, so the
// child_process mocks below actually take effect (a spawned subprocess would
// hit the REAL macOS binaries, defeating the point of mocking).
//
// MOCKING IDIOM: `jest.unstable_mockModule` against `node:child_process` and
// `node:fs/promises`, matching ./lib/footprint-sampler.jest.spec.mjs's
// established idiom for this skill (see that file's header comment for the
// rationale: macOS-only, non-deterministic real output, no existing
// convention to shell out for real in `lib/`-adjacent specs).

import { jest } from '@jest/globals';

const execFileSyncMock = jest.fn();
const readdirMock = jest.fn();
const readFileMock = jest.fn();
const lstatMock = jest.fn();

jest.unstable_mockModule('node:child_process', () => ({
  execFileSync: execFileSyncMock,
}));

jest.unstable_mockModule('node:fs/promises', () => ({
  readdir: readdirMock,
  readFile: readFileMock,
  lstat: lstatMock,
}));

const { runCollection } = await import('./collect.mjs');

// ---------------------------------------------------------------------------
// Fixtures — plausible macOS command output (copied in shape from
// ../lib/recon.jest.spec.mjs's own fixtures, not invented from nothing).
// ---------------------------------------------------------------------------

const VM_STAT_HEALTHY = `Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                             612345.
Pages active:                           834567.
Pages inactive:                         245678.
Pages speculative:                        4123.
Pages throttled:                             0.
Pages wired down:                       456789.
Pages purgeable:                          4567.
"Translation faults":                123456789.
Pages copy-on-write:                    1234567.
Pages zero filled:                     45678901.
Pages reactivated:                        98765.
Pages purged:                             43210.
File-backed pages:                      234567.
Anonymous pages:                         45678.
Pages stored in compressor:               8192.
Pages occupied by compressor:             6144.
Decompressions:                          987654.
Compressions:                           1234567.
Pageins:                                2345678.
Pageouts:                                 12345.
Swapins:                                     12.
Swapouts:                                    34.
`;

const PRESSURE_LEVEL_NORMAL_STDOUT = '1\n';

const SWAP_USAGE_STDOUT = 'vm.swapusage: total = 3072.00M  used = 512.00M  free = 2560.00M  (encrypted)\n';

const PS_TREE_STDOUT = `  PID  PPID    RSS ELAPSED COMM
 39459     1   4832 01:02:03 /bin/zsh
 39461 39459   2048 00:15:20 /opt/homebrew/bin/node /path/to/claude
`;

/** Node ENOENT-shaped error, matching what `execFileSync` throws for a missing binary. */
function enoentError(binary) {
  const error = new Error(`spawnSync ${binary} ENOENT`);
  error.code = 'ENOENT';
  error.errno = -2;
  error.syscall = `spawnSync ${binary}`;
  error.path = binary;
  return error;
}

/** Node EACCES-shaped error, matching what `fs/promises` throws on permission-denied. */
function eaccesError(targetPath) {
  const error = new Error(`EACCES: permission denied, scandir '${targetPath}'`);
  error.code = 'EACCES';
  error.errno = -13;
  error.syscall = 'scandir';
  error.path = targetPath;
  return error;
}

/**
 * Routes the mocked `execFileSync` by binary/first-arg, so probes that are
 * meant to succeed (vm_stat, sysctl pressure/swap, ps) return plausible
 * fixture text, while `footprint`, `vmmap`, `spindump`, and `log` are
 * simulated as MISSING binaries (ENOENT) — per the ticket's scenario (b).
 */
function installAllProbesSucceedExceptPrivilegedOnes() {
  execFileSyncMock.mockImplementation((command, args = []) => {
    const joined = `${command} ${args.join(' ')}`;

    if (command.endsWith('vm_stat')) return VM_STAT_HEALTHY;
    if (command.endsWith('sysctl') && joined.includes('kern.memorystatus_vm_pressure_level')) {
      return PRESSURE_LEVEL_NORMAL_STDOUT;
    }
    if (command.endsWith('sysctl') && joined.includes('vm.swapusage')) return SWAP_USAGE_STDOUT;
    if (command.endsWith('ps')) return PS_TREE_STDOUT;

    // Privileged/optional binaries: simulate "not installed on this host".
    if (
      command.endsWith('footprint') ||
      command.endsWith('vmmap') ||
      command.endsWith('spindump') ||
      command.endsWith('log')
    ) {
      throw enoentError(command);
    }

    throw enoentError(command);
  });
}

/** Simulates `/Library/Logs/DiagnosticReports/` being unreadable (EACCES). */
function installDiagnosticReportsPermissionDenied() {
  const target = '/Library/Logs/DiagnosticReports';
  readdirMock.mockImplementation((dirPath) => {
    if (String(dirPath).startsWith(target)) return Promise.reject(eaccesError(target));
    return Promise.reject(eaccesError(String(dirPath)));
  });
  readFileMock.mockRejectedValue(eaccesError(target));
  lstatMock.mockRejectedValue(eaccesError(target));
}

/** Recursively asserts every leaf "probe result" object is self-describing. */
function collectProbeStatusStrings(node, out = []) {
  if (!node || typeof node !== 'object') return out;
  if (typeof node.status === 'string') out.push(node.status);
  for (const value of Object.values(node)) {
    if (value && typeof value === 'object') collectProbeStatusStrings(value, out);
  }
  return out;
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('runCollection({ mode: "live" }) — every privileged probe missing', () => {
  beforeEach(() => {
    installAllProbesSucceedExceptPrivilegedOnes();
  });

  it('resolves (does not throw/reject) even though footprint/vmmap are missing binaries', async () => {
    await expect(runCollection({ mode: 'live' })).resolves.toBeDefined();
  });

  it('returns a self-describing JSON-serialisable object naming the mode and a timestamp', async () => {
    const result = await runCollection({ mode: 'live' });

    expect(result.mode).toBe('live');
    expect(typeof result.generatedAt).toBe('string');
    // Must be a valid, parseable instant — not a placeholder/undefined string.
    expect(Number.isNaN(Date.parse(result.generatedAt))).toBe(false);

    // The whole thing must be JSON-serialisable without throwing (it will be
    // written to a file and printed to stdout per the ticket's constraint 3).
    expect(() => JSON.stringify(result)).not.toThrow();
  });

  it('documents every probe outcome (ok or failed-with-reason) rather than omitting failed ones', async () => {
    const result = await runCollection({ mode: 'live' });

    const statuses = collectProbeStatusStrings(result);
    expect(statuses.length).toBeGreaterThan(0);
    // Every reported probe status must be one of the three self-describing
    // outcomes the ticket requires (ran / skipped / failed) — never silently
    // absent, and never an uncaught exception.
    for (const status of statuses) {
      expect(['ok', 'skipped', 'failed']).toContain(status);
    }
    // At least one status must be 'failed' in this scenario (footprint/vmmap
    // are simulated as missing binaries) — a run that reports everything as
    // 'ok' here would be silently swallowing the missing-binary condition
    // rather than surfacing it, which is exactly what the ticket forbids.
    expect(statuses).toContain('failed');
  });

  it('never invokes execFileSync with sudo, regardless of which probes failed', async () => {
    await runCollection({ mode: 'live' });

    for (const call of execFileSyncMock.mock.calls) {
      const [command, args = []] = call;
      expect(command).not.toBe('sudo');
      expect(args).not.toContain('sudo');
    }
  });
});

/** `vm_stat` output missing the "Pages occupied by compressor" line only —
 * every other field parses fine, so `compressedMb` alone comes back `NaN`
 * while `freeRamMb` parses successfully (plausible `vm_stat` output-format
 * drift affecting a single counter, not the whole command). */
const VM_STAT_MISSING_COMPRESSOR_LINE = VM_STAT_HEALTHY.split('\n')
  .filter((line) => !line.includes('Pages occupied by compressor'))
  .join('\n');

/** `sysctl vm.swapusage` output missing the `total` field only. */
const SWAP_USAGE_MISSING_TOTAL = 'vm.swapusage: used = 512.00M  free = 2560.00M  (encrypted)\n';

describe('collectMemoryReading / collectSwapReading — partial field-parse failure', () => {
  it('reports memory probe as failed when only ONE field (compressedMb) fails to parse, not silently ok', async () => {
    execFileSyncMock.mockImplementation((command, args = []) => {
      const joined = `${command} ${args.join(' ')}`;
      if (command.endsWith('vm_stat')) return VM_STAT_MISSING_COMPRESSOR_LINE;
      if (command.endsWith('sysctl') && joined.includes('kern.memorystatus_vm_pressure_level')) {
        return PRESSURE_LEVEL_NORMAL_STDOUT;
      }
      if (command.endsWith('sysctl') && joined.includes('vm.swapusage')) return SWAP_USAGE_STDOUT;
      if (command.endsWith('ps')) return PS_TREE_STDOUT;
      throw enoentError(command);
    });

    const result = await runCollection({ mode: 'live' });

    // Must be reported as failed — NOT `status: 'ok'` with a silently-`null`
    // `compressedMb` field, which is exactly what the Medium finding
    // identified as the bug (AND instead of OR across the two NaN checks).
    expect(result.memory.status).toBe('failed');
    expect(result.memory.compressedMb).toBeUndefined();
  });

  it('reports swap probe as failed when only ONE field (swapTotalMb) fails to parse, not silently ok', async () => {
    execFileSyncMock.mockImplementation((command, args = []) => {
      const joined = `${command} ${args.join(' ')}`;
      if (command.endsWith('vm_stat')) return VM_STAT_HEALTHY;
      if (command.endsWith('sysctl') && joined.includes('kern.memorystatus_vm_pressure_level')) {
        return PRESSURE_LEVEL_NORMAL_STDOUT;
      }
      if (command.endsWith('sysctl') && joined.includes('vm.swapusage')) return SWAP_USAGE_MISSING_TOTAL;
      if (command.endsWith('ps')) return PS_TREE_STDOUT;
      throw enoentError(command);
    });

    const result = await runCollection({ mode: 'live' });

    expect(result.swap.status).toBe('failed');
    expect(result.swap.swapTotalMb).toBeUndefined();
  });
});

/**
 * Well-formed JetsamEvent `.ips` content: header line + newline + body
 * document, matching `jetsam-parser.mjs`'s documented two-document format.
 */
const WELL_FORMED_JETSAM_CONTENTS = `${JSON.stringify({ bug_type: '298', os_version: 'macOS 15.0' })}
${JSON.stringify({
  timestamp: '2026-08-01 12:00:00.000 +0000',
  largestProcess: { name: 'node', pid: 4242, rsizeMB: 512 },
})}`;

/** Malformed content: header parses, body does not (truncated/corrupt). */
const MALFORMED_JETSAM_CONTENTS = `${JSON.stringify({ bug_type: '298' })}\n{not valid json`;

/**
 * Simulates `/Library/Logs/DiagnosticReports/` being readable and containing
 * one well-formed and one malformed JetsamEvent report — exercising the
 * `fsPromises.readFile` + `parseJetsamEventReport` loop inside
 * `collectRetroactiveFindings` end to end (Medium 2 fix: this loop previously
 * had no test coverage with a non-empty `scan.files`).
 */
function installDiagnosticReportsWithJetsamFiles() {
  const dir = '/Library/Logs/DiagnosticReports';
  const fakeDirStat = { isSymbolicLink: () => false, mtimeMs: Date.now() };

  lstatMock.mockImplementation(() => Promise.resolve(fakeDirStat));
  readdirMock.mockImplementation((dirPath) => {
    if (String(dirPath) === dir) {
      return Promise.resolve(['JetsamEvent-good.ips', 'JetsamEvent-bad.ips', 'crashreport-unrelated.ips']);
    }
    return Promise.resolve([]);
  });
  readFileMock.mockImplementation((filePath) => {
    const path = String(filePath);
    if (path.endsWith('JetsamEvent-good.ips')) return Promise.resolve(WELL_FORMED_JETSAM_CONTENTS);
    if (path.endsWith('JetsamEvent-bad.ips')) return Promise.resolve(MALFORMED_JETSAM_CONTENTS);
    return Promise.reject(new Error(`unexpected readFile call: ${path}`));
  });
}

describe('runCollection({ mode: "retroactive" }) — read+parse loop over found jetsam files', () => {
  beforeEach(() => {
    installAllProbesSucceedExceptPrivilegedOnes();
    installDiagnosticReportsWithJetsamFiles();
  });

  it('surfaces the successfully-parsed file in findings and the unparseable one separately, never dropping either', async () => {
    const result = await runCollection({ mode: 'retroactive', since: '2020-01-01T00:00:00.000Z' });

    // The well-formed report is a recovered finding.
    expect(result.retroactive.findings).toHaveLength(1);
    expect(result.retroactive.findings[0]).toMatchObject({
      file: expect.stringContaining('JetsamEvent-good.ips'),
      available: true,
      largestProcess: { name: 'node', pid: 4242, rsizeMB: 512 },
    });

    // The malformed report is NOT silently dropped — it shows up as
    // unparseable, distinct from "nothing was found".
    expect(result.retroactive.diagnosticReports.unparseable).toHaveLength(1);
    expect(result.retroactive.diagnosticReports.unparseable[0]).toMatchObject({
      file: expect.stringContaining('JetsamEvent-bad.ips'),
      error: expect.any(String),
    });

    // A finding WAS recovered, so this is unambiguously not "nothing
    // recoverable".
    expect(result.retroactive.nothingRecoverable).toBe(false);
  });

  it('reports nothingRecoverable: false when every found file is unparseable and no other findings exist', async () => {
    readFileMock.mockImplementation((filePath) => {
      const path = String(filePath);
      if (path.endsWith('JetsamEvent-good.ips')) return Promise.resolve(MALFORMED_JETSAM_CONTENTS);
      if (path.endsWith('JetsamEvent-bad.ips')) return Promise.resolve(MALFORMED_JETSAM_CONTENTS);
      return Promise.reject(new Error(`unexpected readFile call: ${path}`));
    });

    const result = await runCollection({ mode: 'retroactive', since: '2020-01-01T00:00:00.000Z' });

    expect(result.retroactive.findings).toHaveLength(0);
    expect(result.retroactive.diagnosticReports.unparseable).toHaveLength(2);
    // Files WERE found but none could be read as a finding — this must not
    // be reported as "genuinely nothing was found".
    expect(result.retroactive.nothingRecoverable).toBe(false);
  });
});

describe('runCollection({ mode: "retroactive" }) — probes failed outright (never actually looked)', () => {
  beforeEach(() => {
    installAllProbesSucceedExceptPrivilegedOnes();
    installDiagnosticReportsPermissionDenied();
  });

  it('resolves successfully even when DiagnosticReports is unreadable and `log`/`spindump` are missing', async () => {
    await expect(
      runCollection({ mode: 'retroactive', since: '2026-08-01T00:00:00.000Z' }),
    ).resolves.toBeDefined();
  });

  it('reports nothingRecoverable: false when a probe failed outright — a failed probe means we never actually looked, which must never be reported the same as a clean null result', async () => {
    const result = await runCollection({ mode: 'retroactive', since: '2026-08-01T00:00:00.000Z' });

    expect(result.mode).toBe('retroactive');
    expect(result).toHaveProperty('retroactive');
    // "nothing recoverable" must be reserved for "we looked thoroughly and
    // found nothing" — NOT "we couldn't look" (permission denied here). A
    // consumer reading this file must be able to tell "we looked and found
    // nothing" apart from "we forgot to look" or "we were blocked from
    // looking"; collapsing the latter into `true` would silently misreport a
    // tooling failure as a clean result.
    expect(result.retroactive.nothingRecoverable).toBe(false);
    expect(result.retroactive.diagnosticReports.status).toBe('failed');
    expect(Array.isArray(result.retroactive.findings)).toBe(true);
    expect(result.retroactive.findings).toHaveLength(0);

    // The permission-denied read must still be reported, not swallowed.
    const statuses = collectProbeStatusStrings(result);
    expect(statuses).toContain('failed');
  });

  it('is JSON-serialisable and stamps a generatedAt timestamp, same as --live', async () => {
    const result = await runCollection({ mode: 'retroactive', since: '2026-08-01T00:00:00.000Z' });

    expect(typeof result.generatedAt).toBe('string');
    expect(Number.isNaN(Date.parse(result.generatedAt))).toBe(false);
    expect(() => JSON.stringify(result)).not.toThrow();
  });
});

describe('runCollection({ mode: "retroactive" }) — log show predicate is case-insensitive', () => {
  it('passes `contains[c]` (not bare `contains`) for both eventMessage clauses to `log show --predicate`', async () => {
    installAllProbesSucceedExceptPrivilegedOnes();
    // Override the shared helper's "log is a missing binary" simulation so we
    // can capture the exact args `log show` was invoked with.
    execFileSyncMock.mockImplementation((command, args = []) => {
      const joined = `${command} ${args.join(' ')}`;
      if (command.endsWith('vm_stat')) return VM_STAT_HEALTHY;
      if (command.endsWith('sysctl') && joined.includes('kern.memorystatus_vm_pressure_level')) {
        return PRESSURE_LEVEL_NORMAL_STDOUT;
      }
      if (command.endsWith('sysctl') && joined.includes('vm.swapusage')) return SWAP_USAGE_STDOUT;
      if (command.endsWith('ps')) return PS_TREE_STDOUT;
      if (command.endsWith('log')) return '';
      throw enoentError(command);
    });
    readdirMock.mockResolvedValue([]);
    lstatMock.mockResolvedValue({ isSymbolicLink: () => false, mtimeMs: Date.now() });

    await runCollection({ mode: 'retroactive', since: '2026-08-01T00:00:00.000Z' });

    const logShowCall = execFileSyncMock.mock.calls.find(([command]) => command.endsWith('log'));
    expect(logShowCall).toBeDefined();
    const [, args] = logShowCall;
    const predicateIndex = args.indexOf('--predicate');
    expect(predicateIndex).toBeGreaterThan(-1);
    const predicate = args[predicateIndex + 1];

    // Real kernel/memorystatus messages vary in case by OS version/subsystem;
    // NSPredicate's `contains` is case-sensitive by default, so both clauses
    // must use the case-insensitive `contains[c]` variant, not bare `contains`.
    const jetsamClause = predicate.match(/eventMessage\s+(contains(?:\[c\])?)\s+"Jetsam"/i);
    const memorystatusClause = predicate.match(/eventMessage\s+(contains(?:\[c\])?)\s+"memorystatus"/i);
    expect(jetsamClause).not.toBeNull();
    expect(jetsamClause[1]).toBe('contains[c]');
    expect(memorystatusClause).not.toBeNull();
    expect(memorystatusClause[1]).toBe('contains[c]');
  });
});

/** `vm_stat` output missing the "Pages active" line only — `totalPages`
 * (which sums free/active/inactive/speculative/wired/compressor) goes `NaN`,
 * so the ratio-derived `pressureLevel` heuristic goes `NaN` too, while
 * `compressedMb` (derived only from the compressor count) and `freeRamMb`
 * (derived only from free + purgeable counts) remain valid — a plausible
 * partial `vm_stat` output-format drift affecting one unrelated counter. */
const VM_STAT_MISSING_ACTIVE_LINE = VM_STAT_HEALTHY.split('\n')
  .filter((line) => !line.trim().startsWith('Pages active'))
  .join('\n');

describe('collectMemoryReading — regression: NaN heuristic pressureLevel is omitted, not nulled', () => {
  it('omits vmStatHeuristicPressureLevel from the envelope while keeping status ok and other fields intact', async () => {
    execFileSyncMock.mockImplementation((command, args = []) => {
      const joined = `${command} ${args.join(' ')}`;
      if (command.endsWith('vm_stat')) return VM_STAT_MISSING_ACTIVE_LINE;
      if (command.endsWith('sysctl') && joined.includes('kern.memorystatus_vm_pressure_level')) {
        return PRESSURE_LEVEL_NORMAL_STDOUT;
      }
      if (command.endsWith('sysctl') && joined.includes('vm.swapusage')) return SWAP_USAGE_STDOUT;
      if (command.endsWith('ps')) return PS_TREE_STDOUT;
      throw enoentError(command);
    });

    const result = await runCollection({ mode: 'live' });

    // Partial degradation: the probe as a whole still succeeded (compressedMb
    // and freeRamMb both parsed cleanly), so status remains 'ok' — only the
    // secondary heuristic field is affected.
    expect(result.memory.status).toBe('ok');
    expect(Number.isNaN(result.memory.compressedMb)).toBe(false);
    expect(Number.isNaN(result.memory.freeRamMb)).toBe(false);

    // The key must be ABSENT, not present as `null` or `NaN` — a NaN
    // heuristic must be an explicit, visible omission rather than a value
    // indistinguishable from a healthy reading of `null`/`NaN`.
    expect(Object.prototype.hasOwnProperty.call(result.memory, 'vmStatHeuristicPressureLevel')).toBe(false);
  });
});

describe('runCollection({ mode: "retroactive" }) — genuinely nothing recoverable (probes ran cleanly, found nothing)', () => {
  it('reports nothingRecoverable: true only when every probe ran (status !== "failed") and found nothing', async () => {
    const dir = '/Library/Logs/DiagnosticReports';
    const fakeDirStat = { isSymbolicLink: () => false, mtimeMs: Date.now() };

    installAllProbesSucceedExceptPrivilegedOnes();
    // Override the shared helper's "log show is a missing binary" simulation
    // — this test's whole point is the case where `log show` genuinely RAN
    // and found nothing, which is a distinct status from "failed to run".
    execFileSyncMock.mockImplementation((command, args = []) => {
      if (command.endsWith('vm_stat')) return VM_STAT_HEALTHY;
      const joined = `${command} ${args.join(' ')}`;
      if (command.endsWith('sysctl') && joined.includes('kern.memorystatus_vm_pressure_level')) {
        return PRESSURE_LEVEL_NORMAL_STDOUT;
      }
      if (command.endsWith('sysctl') && joined.includes('vm.swapusage')) return SWAP_USAGE_STDOUT;
      if (command.endsWith('ps')) return PS_TREE_STDOUT;
      if (command.endsWith('log')) return '';
      throw enoentError(command);
    });
    lstatMock.mockImplementation(() => Promise.resolve(fakeDirStat));
    readdirMock.mockImplementation((dirPath) => (String(dirPath) === dir ? Promise.resolve([]) : Promise.resolve([])));

    const result = await runCollection({ mode: 'retroactive', since: '2026-08-01T00:00:00.000Z' });

    // The scan ran to completion and legitimately found zero files — that is
    // the canonical "we looked and found nothing" case, reported as 'ok'
    // (with an empty `files` array), not 'skipped' (reserved for a probe that
    // genuinely didn't run).
    expect(result.retroactive.diagnosticReports.status).toBe('ok');
    expect(result.retroactive.diagnosticReports.files).toEqual([]);
    expect(result.retroactive.logShow.status).not.toBe('failed');
    expect(result.retroactive.nothingRecoverable).toBe(true);
  });
});

describe('runCollection({ mode: "retroactive" }) — since-fallback routes through parseSinceWindow (regression)', () => {
  beforeEach(() => {
    installAllProbesSucceedExceptPrivilegedOnes();
    lstatMock.mockImplementation(() => Promise.resolve({ isSymbolicLink: () => false, mtimeMs: Date.now() }));
    readdirMock.mockImplementation(() => Promise.resolve([]));
  });

  it('omitting `since` entirely still resolves (defaults via parseSinceWindow, not a hand-built Date)', async () => {
    // The shipped CLI always passes an already-validated `Date` (parseCliArgs
    // runs first), so this shape — a direct call to the exported function
    // with no `since` at all — is only reachable by a future direct caller.
    // Before the fix this still worked (`new Date(undefined ?? default)`),
    // so this test's real purpose is guarding the refactor: the single
    // source of truth is now `parseSinceWindow`, not a parallel `new Date`
    // construction, and the promise must still resolve rather than reject.
    await expect(runCollection({ mode: 'retroactive' })).resolves.toBeDefined();

    const result = await runCollection({ mode: 'retroactive' });
    expect(result.retroactive).toBeDefined();
    expect(result.unexpectedError).toBeUndefined();
  });

  it('a raw (non-Date) string `since` is still accepted, parsed the same way parseCliArgs would', async () => {
    const result = await runCollection({ mode: 'retroactive', since: '2020-01-01T00:00:00.000Z' });

    expect(result.unexpectedError).toBeUndefined();
    expect(result.retroactive).toBeDefined();
  });

  it('a malformed non-Date `since` degrades to result.unexpectedError instead of silently producing an Invalid Date', async () => {
    const result = await runCollection({ mode: 'retroactive', since: 'not-a-real-date' });

    // Before the fix, `new Date('not-a-real-date')` silently produced an
    // `Invalid Date` that was then handed to `collectRetroactiveFindings`
    // with no error ever surfaced. Routing through `parseSinceWindow` (the
    // same validator `parseCliArgs` uses) makes this a REPORTED failure
    // instead — this is the newly-testable path the fix introduces.
    expect(result.retroactive).toBeUndefined();
    expect(result.unexpectedError).toBeDefined();
    expect(result.unexpectedError.status).toBe('failed');
    expect(result.unexpectedError.error).toMatch(/could not parse "not-a-real-date" as a date/);
  });

  it('a `since` value in the future degrades to result.unexpectedError, matching parseSinceWindow\'s own future-date rejection', async () => {
    const farFuture = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000).toISOString();

    const result = await runCollection({ mode: 'retroactive', since: farFuture });

    expect(result.retroactive).toBeUndefined();
    expect(result.unexpectedError).toBeDefined();
    expect(result.unexpectedError.error).toMatch(/is in the future/);
  });
});

describe('runCollection({ mode: <unrecognized> }) — consistent result shape (regression)', () => {
  it('reports an unrecognized mode under its own status-envelope key, not a bespoke `result.error`', async () => {
    const result = await runCollection({ mode: 'bogus' });

    // `result.mode` keeps holding the raw input value (set once, up front,
    // for every branch) — the failure envelope must NOT clobber it.
    expect(result.mode).toBe('bogus');
    expect(result.error).toBeUndefined();

    expect(result.unknownMode).toBeDefined();
    expect(result.unknownMode.status).toBe('failed');
    expect(result.unknownMode.error).toMatch(/collect: unknown mode "bogus"/);

    // No probe ever ran for an unrecognized mode — the live/retroactive
    // branch keys must be entirely absent.
    expect(result.memory).toBeUndefined();
    expect(result.retroactive).toBeUndefined();
  });

  it('an omitted `mode` is treated the same as any other unrecognized mode', async () => {
    const result = await runCollection({});

    expect(result.unknownMode).toBeDefined();
    expect(result.unknownMode.status).toBe('failed');
  });
});
