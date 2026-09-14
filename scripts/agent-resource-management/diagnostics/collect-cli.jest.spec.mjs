// Phase 2 — unit tests for scenarios `collect.jest.spec.mjs`
// (Phase 0's outer acceptance test) does NOT cover: CLI flag parsing, output
// file writing, and self-exclusion from the process tree.
//
// `collect.mjs` now exists and this suite passes against it. The RED-phase
// framing below (this spec predates the implementation) is kept for context
// on the contract it locked in, not as a live instruction.
//
// This file does NOT modify `collect.jest.spec.mjs` — that Phase 0 outer
// test stays exactly as it is. This is a separate, narrower spec covering
// the contract pieces Phase 0 explicitly deferred:
//   "only argument-validation failures, e.g. a malformed --since, may
//    reject; that path is out of scope for this outer test and is exercised
//    by a narrower unit spec in a later phase." — this is that spec.
//
// ---------------------------------------------------------------------------
// CONTRACT this spec assumes `collect.mjs` will export (chosen by this
// spec's author — not yet implemented; the implementer should treat these
// three exports, their names, and their shapes as the agreed surface):
//
//   export function parseCliArgs(argv)
//     -> { valid: true, mode: 'live' | 'retroactive', since?: Date }
//      | { valid: false, error: string }
//     Pure, synchronous, never throws. Mirrors the `{ valid, error }` result
//     shape `lib/since-window-parser.mjs`'s `parseSinceWindow` already
//     established, and reuses `parseSinceWindow` internally for `--since`
//     validation so the "malformed/future date" rule lives in exactly one
//     place. Recognises exactly three flags: `--live` (bare), `--retroactive`
//     (bare), `--since <value>` (only meaningful alongside `--retroactive`,
//     mirroring `parseArgs`'s `--flag value` / `--flag=value` support in
//     `cli.mjs`). Rejects: no mode flag at all; both `--live` and
//     `--retroactive` together; any unrecognized `--foo` flag; `--since`
//     supplied without `--retroactive`; a `--since` value that
//     `parseSinceWindow` itself rejects (malformed date, future date).
//
//   export async function writeCollectionOutput(result, { outputDir } = {})
//     -> Promise<{ filePath: string }>
//     `outputDir` defaults to
//     `scripts/agent-resource-management/diagnostics/output/`. Creates the
//     directory (`mkdir(outputDir, { recursive: true })`) if it doesn't
//     exist, then writes `result` as pretty-printed JSON to a timestamped,
//     collision-resistant filename inside it (e.g.
//     `collect-<ISO-timestamp>-<pid>-<random>.json`), and ensures the file
//     is created with mode `0o600` (either by passing `{ mode: 0o600 }` to
//     `writeFile`, or by a follow-up `chmod`). Two calls issued back to back
//     — even if a fake clock makes their timestamps identical — must produce
//     two DIFFERENT file paths.
//
//   export function excludeSelfFromProcessTree(processes, selfPid)
//     -> Array<object>
//     Pure, synchronous. `processes` is an array of process-record objects
//     as `collect.mjs` assembles them for its own output (each carries at
//     least `pid` and `ppid` fields, matching the shape
//     `lib/recon.mjs`-adjacent parsing already produces elsewhere in this
//     skill). Filters out `selfPid` (normally `process.pid`, injected here so
//     the function is testable without depending on the real process's own
//     pid) AND every transitive descendant of `selfPid` within the given
//     array (an exec-spawned child of `collect.mjs` itself — e.g. the `ps`/
//     `vm_stat` child processes it shells out to — must not appear in the
//     final process list it reports on the host). `collect.mjs` is expected
//     to call this on whatever `recon.mjs`-derived process list it captures
//     before embedding it in the result it returns from `runCollection`.
//
// MOCKING IDIOM: `jest.unstable_mockModule` against `node:fs/promises`,
// matching `collect.jest.spec.mjs` (Phase 0) and
// `../lib/footprint-sampler.jest.spec.mjs`'s established idiom for this
// skill. `parseCliArgs` and `excludeSelfFromProcessTree` are pure functions
// and need no mocking at all.

import { jest } from '@jest/globals';

const mkdirMock = jest.fn();
const writeFileMock = jest.fn();
const chmodMock = jest.fn();

jest.unstable_mockModule('node:fs/promises', () => ({
  mkdir: mkdirMock,
  writeFile: writeFileMock,
  chmod: chmodMock,
}));

const { parseCliArgs, writeCollectionOutput, excludeSelfFromProcessTree } = await import('./collect.mjs');

beforeEach(() => {
  jest.clearAllMocks();
  mkdirMock.mockResolvedValue(undefined);
  writeFileMock.mockResolvedValue(undefined);
  chmodMock.mockResolvedValue(undefined);
});

// ---------------------------------------------------------------------------
// 1 & 2. parseCliArgs — flag parsing / usage errors
// ---------------------------------------------------------------------------

describe('parseCliArgs', () => {
  it('accepts bare --live with no --since', () => {
    const result = parseCliArgs(['--live']);
    expect(result).toEqual({ valid: true, mode: 'live' });
  });

  it('accepts bare --retroactive with no --since (falls back to parseSinceWindow default)', () => {
    const result = parseCliArgs(['--retroactive']);
    expect(result.valid).toBe(true);
    expect(result.mode).toBe('retroactive');
    expect(result.since).toBeInstanceOf(Date);
  });

  it('accepts --retroactive --since <ISO date> and parses it to a Date', () => {
    const result = parseCliArgs(['--retroactive', '--since', '2026-08-01T00:00:00.000Z']);
    expect(result.valid).toBe(true);
    expect(result.mode).toBe('retroactive');
    expect(result.since).toBeInstanceOf(Date);
    expect(result.since.toISOString()).toBe('2026-08-01T00:00:00.000Z');
  });

  it('accepts --retroactive --since=<ISO date> (equals form)', () => {
    const result = parseCliArgs(['--retroactive', '--since=2026-08-01T00:00:00.000Z']);
    expect(result.valid).toBe(true);
    expect(result.since.toISOString()).toBe('2026-08-01T00:00:00.000Z');
  });

  it('rejects with a clear { valid: false, error } shape when no mode flag is given at all', () => {
    const result = parseCliArgs([]);
    expect(result.valid).toBe(false);
    expect(typeof result.error).toBe('string');
    expect(result.error.length).toBeGreaterThan(0);
  });

  it('rejects when both --live and --retroactive are given together', () => {
    const result = parseCliArgs(['--live', '--retroactive']);
    expect(result.valid).toBe(false);
    expect(typeof result.error).toBe('string');
  });

  it('rejects an unrecognized flag rather than silently ignoring it', () => {
    const result = parseCliArgs(['--live', '--frobnicate']);
    expect(result.valid).toBe(false);
    expect(typeof result.error).toBe('string');
  });

  it('rejects --since supplied without --retroactive', () => {
    const result = parseCliArgs(['--live', '--since', '2026-08-01T00:00:00.000Z']);
    expect(result.valid).toBe(false);
    expect(typeof result.error).toBe('string');
  });

  it('rejects a bare trailing --since (value forgotten, nothing follows) rather than silently falling back to the default window', () => {
    const result = parseCliArgs(['--retroactive', '--since']);
    expect(result.valid).toBe(false);
    expect(typeof result.error).toBe('string');
  });

  it('rejects --since immediately followed by another --flag (value forgotten) rather than silently falling back to the default window', () => {
    const result = parseCliArgs(['--retroactive', '--since', '--live']);
    expect(result.valid).toBe(false);
    expect(typeof result.error).toBe('string');
  });

  it('never throws for garbage input — always returns a result object', () => {
    expect(() => parseCliArgs(['--since'])).not.toThrow();
    expect(() => parseCliArgs(['not-a-flag-at-all'])).not.toThrow();
    expect(() => parseCliArgs(undefined)).not.toThrow();
  });

  it('rejects an explicit value attached to the bare --live/--retroactive switches rather than silently absorbing it', () => {
    // `--live=false` must NOT be treated as "live mode enabled" just because
    // `flags.live` is present — a typo/copy-paste like this is a plausible
    // real mistake (other CLIs use `=false` to disable a switch) and must
    // surface as a usage error, not silently do the opposite of what was
    // intended.
    const equalsForm = parseCliArgs(['--live=false']);
    expect(equalsForm.valid).toBe(false);
    expect(typeof equalsForm.error).toBe('string');

    // `--retroactive somepath` must not silently swallow `somepath` as
    // --retroactive's "value" either.
    const trailingTokenForm = parseCliArgs(['--retroactive', 'somepath']);
    expect(trailingTokenForm.valid).toBe(false);
    expect(typeof trailingTokenForm.error).toBe('string');
  });

  it('rejects a stray positional argument that never sits immediately after a recognized flag', () => {
    // Pre-existing gap (found in adversarial pre-PR review): the loop used to
    // `continue` past any token not starting with `--`, so a leading
    // positional was silently dropped instead of rejected — unlike
    // `--live=false` and a trailing value on a bare boolean flag, which were
    // already correctly rejected above. `node collect.mjs garbage --live`
    // must surface as a usage error, not silently run in `--live` mode.
    const leadingPositional = parseCliArgs(['garbage', '--live']);
    expect(leadingPositional.valid).toBe(false);
    expect(typeof leadingPositional.error).toBe('string');

    const positionalBetweenFlags = parseCliArgs(['--retroactive', '--since', '2026-08-01T00:00:00.000Z', 'garbage']);
    expect(positionalBetweenFlags.valid).toBe(false);
    expect(typeof positionalBetweenFlags.error).toBe('string');
  });

  // ---------------------------------------------------------------------
  // 2. --retroactive with a malformed/future --since is rejected at the
  // flag-parsing layer, BEFORE any probing starts — i.e. this must surface
  // as a usage error from parseCliArgs itself, not a runtime exception
  // thrown mid-collection by runCollection.
  // ---------------------------------------------------------------------

  it('rejects --retroactive --since <malformed date> as a usage error, not a crash', () => {
    const result = parseCliArgs(['--retroactive', '--since', 'not-a-real-date']);
    expect(result.valid).toBe(false);
    expect(typeof result.error).toBe('string');
    // Surfaces the same validation `since-window-parser.mjs` already
    // performs — the CLI layer must not reimplement its own, divergent
    // date-parsing rule.
    expect(result.error).toMatch(/since-window-parser/);
  });

  it('rejects --retroactive --since <a date in the future> as a usage error, not a crash', () => {
    const farFuture = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000).toISOString();
    const result = parseCliArgs(['--retroactive', '--since', farFuture]);
    expect(result.valid).toBe(false);
    expect(typeof result.error).toBe('string');
    expect(result.error).toMatch(/since-window-parser/);
  });

  it('never calls into collection machinery when validation fails (parseCliArgs is pure and standalone)', () => {
    // parseCliArgs must not itself import/invoke runCollection — a malformed
    // --since must be rejected before any probe fires. We assert this
    // behaviourally: calling parseCliArgs with bad input synchronously
    // returns an invalid result and touches none of the mocked fs/promises
    // functions runCollection/writeCollectionOutput would use.
    const result = parseCliArgs(['--retroactive', '--since', 'garbage']);
    expect(result.valid).toBe(false);
    expect(mkdirMock).not.toHaveBeenCalled();
    expect(writeFileMock).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// 3. writeCollectionOutput — output file writing
// ---------------------------------------------------------------------------

describe('writeCollectionOutput', () => {
  const sampleResult = { mode: 'live', generatedAt: '2026-09-02T00:00:00.000Z', probes: {} };

  it('creates the output directory (recursive) before writing', async () => {
    await writeCollectionOutput(sampleResult, { outputDir: '/tmp/arm-diagnostics-output' });

    expect(mkdirMock).toHaveBeenCalledWith('/tmp/arm-diagnostics-output', expect.objectContaining({ recursive: true }));
  });

  it('creates the output directory with a restrictive 0o700 mode, not the inherited umask default', async () => {
    await writeCollectionOutput(sampleResult, { outputDir: '/tmp/arm-diagnostics-output' });

    expect(mkdirMock).toHaveBeenCalledWith('/tmp/arm-diagnostics-output', expect.objectContaining({ mode: 0o700 }));
  });

  it('defaults outputDir to scripts/agent-resource-management/diagnostics/output/ when not given', async () => {
    await writeCollectionOutput(sampleResult);

    const [dirArg] = mkdirMock.mock.calls[0];
    expect(String(dirArg)).toMatch(/scripts[/\\]agent-resource-management[/\\]diagnostics[/\\]output[/\\]?$/);
  });

  it('writes the result as JSON inside the output directory', async () => {
    await writeCollectionOutput(sampleResult, { outputDir: '/tmp/arm-diagnostics-output' });

    expect(writeFileMock).toHaveBeenCalledTimes(1);
    const [filePath, contents] = writeFileMock.mock.calls[0];
    expect(String(filePath)).toContain('/tmp/arm-diagnostics-output');
    expect(String(filePath)).toMatch(/\.json$/);
    expect(JSON.parse(contents)).toEqual(sampleResult);
  });

  it('creates the file with restrictive permissions (mode 0o600)', async () => {
    await writeCollectionOutput(sampleResult, { outputDir: '/tmp/arm-diagnostics-output' });

    const writeFileOptions = writeFileMock.mock.calls[0][2];
    const passedModeToWriteFile = writeFileOptions && writeFileOptions.mode === 0o600;
    const passedModeToChmod =
      chmodMock.mock.calls.length > 0 && chmodMock.mock.calls[0][1] === 0o600;

    expect(passedModeToWriteFile || passedModeToChmod).toBe(true);
  });

  it('returns the file path it wrote to', async () => {
    const { filePath } = await writeCollectionOutput(sampleResult, { outputDir: '/tmp/arm-diagnostics-output' });

    expect(typeof filePath).toBe('string');
    expect(writeFileMock.mock.calls[0][0]).toBe(filePath);
  });

  it('produces two DIFFERENT filenames for two rapid consecutive calls, even under a frozen clock', async () => {
    // `writeCollectionOutput`'s timestamp segment comes from
    // `new Date().toISOString()`, not `Date.now()` — in V8/Node, the no-arg
    // `Date` constructor reads the engine's internal clock directly, it is
    // NOT routed through the overridable static `Date.now`. Overriding only
    // `Date.now` (as an earlier version of this test did) would leave
    // `new Date()` reading the REAL clock, so that version's "frozen clock"
    // premise didn't actually hold — it passed only because of this
    // function's random filename suffix, not because the clock was frozen.
    // `jest.useFakeTimers().setSystemTime(...)` genuinely freezes both
    // `Date.now()` and `new Date()`, so this test now exercises what it
    // claims to: two calls under a truly identical timestamp still produce
    // different filenames, via the random suffix.
    jest.useFakeTimers().setSystemTime(1_756_800_000_000);
    try {
      const first = await writeCollectionOutput(sampleResult, { outputDir: '/tmp/arm-diagnostics-output' });
      const second = await writeCollectionOutput(sampleResult, { outputDir: '/tmp/arm-diagnostics-output' });

      expect(first.filePath).not.toBe(second.filePath);
    } finally {
      jest.useRealTimers();
    }
  });

  it('does not throw when mkdir rejects because the directory already exists', async () => {
    const eexistError = new Error('EEXIST: file already exists, mkdir');
    eexistError.code = 'EEXIST';
    mkdirMock.mockRejectedValueOnce(eexistError);

    await expect(
      writeCollectionOutput(sampleResult, { outputDir: '/tmp/arm-diagnostics-output' }),
    ).resolves.toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// 4. excludeSelfFromProcessTree — self-exclusion from the reported ps-tree
// ---------------------------------------------------------------------------

describe('excludeSelfFromProcessTree', () => {
  it('removes the exact self pid from the process list', () => {
    const processes = [
      { pid: '100', ppid: '1', comm: '/bin/zsh' },
      { pid: '200', ppid: '100', comm: 'node collect.mjs' },
      { pid: '300', ppid: '1', comm: '/usr/bin/some-other-agent' },
    ];

    const result = excludeSelfFromProcessTree(processes, '200');

    expect(result.map((p) => p.pid)).not.toContain('200');
    expect(result.map((p) => p.pid)).toEqual(expect.arrayContaining(['100', '300']));
  });

  it('removes transitive descendants of the self pid (exec-spawned children of collect.mjs)', () => {
    const processes = [
      { pid: '100', ppid: '1', comm: '/bin/zsh' },
      { pid: '200', ppid: '100', comm: 'node collect.mjs' },
      { pid: '201', ppid: '200', comm: '/usr/bin/ps' },
      { pid: '202', ppid: '201', comm: '/bin/sh' },
      { pid: '300', ppid: '1', comm: '/usr/bin/some-other-agent' },
    ];

    const result = excludeSelfFromProcessTree(processes, '200');

    const remainingPids = result.map((p) => p.pid);
    expect(remainingPids).not.toContain('200');
    expect(remainingPids).not.toContain('201');
    expect(remainingPids).not.toContain('202');
    expect(remainingPids).toEqual(expect.arrayContaining(['100', '300']));
  });

  it('accepts a numeric selfPid (process.pid is a number) and matches string pid fields', () => {
    const processes = [
      { pid: '200', ppid: '100', comm: 'node collect.mjs' },
      { pid: '300', ppid: '1', comm: '/usr/bin/some-other-agent' },
    ];

    const result = excludeSelfFromProcessTree(processes, 200);

    expect(result.map((p) => p.pid)).toEqual(['300']);
  });

  it('is a no-op (returns an equivalent list) when selfPid is not present in the given processes', () => {
    const processes = [
      { pid: '300', ppid: '1', comm: '/usr/bin/some-other-agent' },
      { pid: '400', ppid: '1', comm: '/usr/bin/another-agent' },
    ];

    const result = excludeSelfFromProcessTree(processes, '999');

    expect(result).toEqual(processes);
  });

  it('does not mutate the input array', () => {
    const processes = [
      { pid: '200', ppid: '100', comm: 'node collect.mjs' },
      { pid: '300', ppid: '1', comm: '/usr/bin/some-other-agent' },
    ];
    const snapshot = JSON.parse(JSON.stringify(processes));

    excludeSelfFromProcessTree(processes, '200');

    expect(processes).toEqual(snapshot);
  });

  it('handles an empty process list without throwing', () => {
    expect(excludeSelfFromProcessTree([], '200')).toEqual([]);
  });
});
