// Regression coverage for `collect.mjs`'s `retroactive` branch: the
// diagnostic-reports scan and the `log show` probe must each be isolated
// from the other's unexpected throw, mirroring the `runProbeIsolated`
// guarantee the `live` branch already gets structurally (see collect.mjs's
// own header comment: "one probe's failure never prevents others from
// running or being reported").
//
// Before this fix, `collectRetroactiveFindings` wrapped BOTH sub-steps in
// one shared implicit control flow with no per-step isolation — an
// unexpected throw from the dynamically-imported `scanDiagnosticReports`
// (or from `runLogShow`) would propagate all the way out to
// `runCollection`'s outer belt-and-braces try/catch, collapsing the WHOLE
// retroactive section (including the other sub-step's already-successful
// result) into a single opaque `result.unexpectedError`.
//
// MOCKING IDIOM: `jest.unstable_mockModule` against
// `../lib/diagnostic-reports-scanner.mjs` and `../lib/log-show-probe.mjs`
// directly, so each sub-step can be independently forced to throw —
// matching `collect-footprint-seam.jest.spec.mjs`'s established idiom of
// mocking a single Phase-1 lib module rather than its transitive
// `execFileSync`/`fs` dependencies.

import { jest } from '@jest/globals';

const scanDiagnosticReportsMock = jest.fn();
const runLogShowMock = jest.fn();
const execFileSyncMock = jest.fn();

jest.unstable_mockModule('node:child_process', () => ({
  execFileSync: execFileSyncMock,
}));

jest.unstable_mockModule('../lib/diagnostic-reports-scanner.mjs', () => ({
  scanDiagnosticReports: scanDiagnosticReportsMock,
}));

jest.unstable_mockModule('../lib/log-show-probe.mjs', () => ({
  runLogShow: runLogShowMock,
}));

const { runCollection } = await import('./collect.mjs');

const SINCE = '2026-08-01T00:00:00.000Z';

beforeEach(() => {
  jest.clearAllMocks();
});

describe('runCollection({ mode: "retroactive" }) — sub-step isolation (regression)', () => {
  it('keeps a successful log-show result when the diagnostic-reports scan throws unexpectedly', async () => {
    scanDiagnosticReportsMock.mockRejectedValue(new TypeError('scanDiagnosticReports: unexpected internal error'));
    runLogShowMock.mockResolvedValue({
      status: 'ok',
      entries: [{ eventMessage: 'Jetsam: killed process 4242' }],
    });

    const result = await runCollection({ mode: 'retroactive', since: SINCE });

    // The whole retroactive section must NOT collapse into
    // `result.unexpectedError` — the throwing sub-step degrades on its own.
    expect(result.unexpectedError).toBeUndefined();
    expect(result.retroactive).toBeDefined();

    // The diagnostic-reports sub-step reports its own isolated failure.
    expect(result.retroactive.diagnosticReports.status).toBe('failed');
    expect(result.retroactive.diagnosticReports.error).toMatch(/unexpected internal error/);
    expect(result.retroactive.diagnosticReports.files).toEqual([]);

    // The log-show sub-step's already-successful result is NOT discarded.
    expect(result.retroactive.logShow.status).toBe('ok');
    expect(result.retroactive.findings).toHaveLength(1);
    expect(result.retroactive.findings[0]).toMatchObject({
      source: 'log-show',
      entry: { eventMessage: 'Jetsam: killed process 4242' },
    });

    // A failed sub-step means we never fully looked — never a clean null.
    expect(result.retroactive.nothingRecoverable).toBe(false);
  });

  it('keeps a successful diagnostic-reports result when log-show throws unexpectedly', async () => {
    scanDiagnosticReportsMock.mockResolvedValue({ files: [] });
    runLogShowMock.mockRejectedValue(new Error('runLogShow: unexpected internal error'));

    const result = await runCollection({ mode: 'retroactive', since: SINCE });

    expect(result.unexpectedError).toBeUndefined();
    expect(result.retroactive).toBeDefined();

    // The diagnostic-reports sub-step ran cleanly and is reported as such.
    expect(result.retroactive.diagnosticReports.status).toBe('ok');
    expect(result.retroactive.diagnosticReports.files).toEqual([]);

    // The log-show sub-step's throw degrades to its own isolated failure,
    // rather than discarding the diagnostic-reports result above.
    expect(result.retroactive.logShow.status).toBe('failed');
    expect(result.retroactive.logShow.error).toMatch(/unexpected internal error/);
    expect(result.retroactive.logShow.entries).toEqual([]);

    expect(result.retroactive.nothingRecoverable).toBe(false);
  });
});
