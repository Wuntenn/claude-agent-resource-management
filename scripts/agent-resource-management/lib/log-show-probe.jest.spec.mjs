// RED unit tests for the not-yet-implemented
// scripts/agent-resource-management/lib/log-show-probe.mjs module
// (Phase 1 — "diagnostics-parity tooling").
//
// Do NOT add a stub implementation to make this pass — that is the builder's
// job in a later phase. Every test below is expected to fail at IMPORT TIME
// (`Cannot find module './log-show-probe.mjs'`) because the module does not
// exist yet.
//
// SHAPE THIS TEST ASSUMES (chosen by this test's author, not yet
// implemented):
//
//   export async function runLogShow({ predicate, since }) -> Promise<{
//     status: 'ok' | 'failed',
//     entries: Array<object>,
//     error?: string,
//   }>
//
// `runLogShow` shells out to `log show --predicate <predicate> --start
// <since> --style ndjson` (or equivalent), reusing
// `./probe-bound.mjs`'s `probeExecOptions()` for the same time-boxing
// rationale `footprint-sampler.mjs` uses (`execFileSync` cannot be bounded
// after the fact — see probe-bound.mjs's own header comment). It NEVER
// throws: a missing `log` binary or zero matching entries are both
// self-describing successful/degraded results, matching the outer contract
// exercised by collect.jest.spec.mjs ('ok' | 'skipped' | 'failed' per
// probe — this module folds its own two failure modes into 'ok' with an
// empty array for "nothing found" and 'failed' for "could not run",
// mirroring collect.jest.spec.mjs's own convention that a MISSING BINARY is
// reported as 'failed', not silently downgraded to 'skipped').
//
// MOCKING IDIOM: `jest.unstable_mockModule` against `node:child_process`,
// matching footprint-sampler.jest.spec.mjs's established idiom.

import { jest } from '@jest/globals';

const execFileSyncMock = jest.fn();

jest.unstable_mockModule('node:child_process', () => ({
  execFileSync: execFileSyncMock,
}));

const { runLogShow } = await import('./log-show-probe.mjs');
const { probeExecOptions } = await import('./probe-bound.mjs');

/** Node ENOENT-shaped error, matching what `execFileSync` throws for a missing binary. */
function enoentError(binary) {
  const error = new Error(`spawnSync ${binary} ENOENT`);
  error.code = 'ENOENT';
  error.errno = -2;
  error.syscall = `spawnSync ${binary}`;
  error.path = binary;
  return error;
}

const NDJSON_TWO_ENTRIES =
  '{"timestamp":"2026-08-25 09:12:33.00","eventMessage":"memorystatus: killing process"}\n' +
  '{"timestamp":"2026-08-25 09:12:34.00","eventMessage":"jetsam: high water mark"}\n';

beforeEach(() => {
  execFileSyncMock.mockReset();
});

describe('runLogShow — successful invocation with matching entries', () => {
  it('parses NDJSON stdout into an array of log entries', async () => {
    execFileSyncMock.mockReturnValue(NDJSON_TWO_ENTRIES);

    const result = await runLogShow({ predicate: 'eventMessage contains "jetsam"', since: '2026-08-01T00:00:00.000Z' });

    expect(result.status).toBe('ok');
    expect(result.entries).toHaveLength(2);
    expect(result.entries[0].eventMessage).toBe('memorystatus: killing process');
  });
});

describe('runLogShow — zero matching entries', () => {
  it('returns a graceful ok result with an empty array rather than treating "no matches" as an error', async () => {
    execFileSyncMock.mockReturnValue('');

    const result = await runLogShow({ predicate: 'eventMessage contains "nothing-matches-this"', since: '2026-08-01T00:00:00.000Z' });

    expect(result.status).toBe('ok');
    expect(result.entries).toEqual([]);
  });
});

describe('runLogShow — `log` binary missing', () => {
  it('returns a graceful degraded result (never throws) when execFileSync throws ENOENT', async () => {
    execFileSyncMock.mockImplementation(() => {
      throw enoentError('log');
    });

    let result;
    await expect(
      (async () => {
        result = await runLogShow({ predicate: 'eventMessage contains "jetsam"', since: '2026-08-01T00:00:00.000Z' });
      })(),
    ).resolves.toBeUndefined();

    expect(result.status).toBe('failed');
    expect(result.entries).toEqual([]);
    expect(typeof result.error).toBe('string');
  });
});

describe('runLogShow — exec call is bounded via probeExecOptions', () => {
  it('invokes execFileSync with options matching probeExecOptions() (a real timeout is applied)', async () => {
    execFileSyncMock.mockReturnValue(NDJSON_TWO_ENTRIES);

    await runLogShow({ predicate: 'eventMessage contains "jetsam"', since: '2026-08-01T00:00:00.000Z' });

    expect(execFileSyncMock).toHaveBeenCalledTimes(1);
    const [command, , options] = execFileSyncMock.mock.calls[0];
    expect(command).toMatch(/log$/);
    expect(options).toEqual(expect.objectContaining(probeExecOptions()));
  });
});

describe('runLogShow — regression: maxBuffer is raised above Node\'s 1 MB execFileSync default', () => {
  it('invokes execFileSync with maxBuffer: 50 * 1024 * 1024, layered on top of probeExecOptions()', async () => {
    execFileSyncMock.mockReturnValue(NDJSON_TWO_ENTRIES);

    await runLogShow({ predicate: 'eventMessage contains "jetsam"', since: '2026-08-01T00:00:00.000Z' });

    expect(execFileSyncMock).toHaveBeenCalledTimes(1);
    const [, , options] = execFileSyncMock.mock.calls[0];

    // Asserts the EXACT value, not merely presence — `objectContaining` in the
    // sibling test above would pass even if `maxBuffer` were absent entirely
    // or set to Node's default. A `log show --style ndjson` scan over even a
    // moderate window routinely exceeds Node's default 1 MB `execFileSync`
    // buffer ceiling, producing an ENOBUFS masquerading as "binary missing" —
    // see this module's own header comment.
    expect(options.maxBuffer).toBe(50 * 1024 * 1024);
    // Still layered on top of, not replacing, probeExecOptions()'s own
    // timeout/killSignal/encoding settings.
    expect(options).toEqual(expect.objectContaining(probeExecOptions()));
  });
});

describe('runLogShow — regression: `--start` argument format', () => {
  it('formats an ISO-8601 `since` as local-time "YYYY-MM-DD HH:MM:SS" for `log show --start`, not the raw ISO-8601 string', async () => {
    execFileSyncMock.mockReturnValue('');

    const since = new Date('2026-08-01T00:00:00.000Z');
    await runLogShow({ predicate: 'eventMessage contains "jetsam"', since: since.toISOString() });

    expect(execFileSyncMock).toHaveBeenCalledTimes(1);
    const [, args] = execFileSyncMock.mock.calls[0];
    const startIndex = args.indexOf('--start');
    expect(startIndex).toBeGreaterThan(-1);

    const startArg = args[startIndex + 1];

    // Must NOT be the ISO-8601 passthrough that macOS `log show` rejects
    // with "Failed conversion of '<timestamp>' using format
    // '%Y-%m-%d %H:%M:%S'".
    expect(startArg).not.toContain('T');
    expect(startArg).not.toContain('Z');
    expect(startArg).not.toContain('.');

    // Must match macOS `log show --start`'s expected local-time format.
    expect(startArg).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

    const expectedLocal = since; // formatForLogShow() is local-time based
    const pad = (value) => String(value).padStart(2, '0');
    const expectedFormatted =
      `${expectedLocal.getFullYear()}-${pad(expectedLocal.getMonth() + 1)}-${pad(expectedLocal.getDate())} ` +
      `${pad(expectedLocal.getHours())}:${pad(expectedLocal.getMinutes())}:${pad(expectedLocal.getSeconds())}`;
    expect(startArg).toBe(expectedFormatted);
  });
});
