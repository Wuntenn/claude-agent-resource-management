// RED unit tests for the not-yet-implemented
// scripts/agent-resource-management/lib/since-window-parser.mjs module
// (Phase 1 — "diagnostics-parity tooling").
//
// Do NOT add a stub implementation to make this pass — that is the builder's
// job in a later phase. Every test below is expected to fail at IMPORT TIME
// (`Cannot find module './since-window-parser.mjs'`) because the module does
// not exist yet.
//
// SHAPE THIS TEST ASSUMES (chosen by this test's author, not yet
// implemented):
//
//   export function parseSinceWindow(rawValue, referenceNow = new Date())
//     -> { valid: true, since: Date } | { valid: false, error: string }
//
// The `{ valid: boolean }` result shape (rather than throwing) mirrors the
// established convention already in this skill's `cli.mjs`
// (`validateBareOrTrueFlag` returns `{ valid: true/false }` rather than
// throwing, so a CLI call site keeps explicit control over exactly when the
// process exits — see that function's own header comment). Following the
// same convention here lets Phase 2's CLI layer reject a malformed --since
// with a clear non-zero exit before any probing starts, without a try/catch
// at the call site.
//
// DEFAULT WINDOW CHOICE: 30 days back. Diagnostic reports and jetsam/kill
// logs on macOS are not retained indefinitely (the OS itself rotates/purges
// `/Library/Logs/DiagnosticReports/` over time), and an unbounded
// since-epoch default would make a bare `--retroactive` scan (no `--since`
// given) silently attempt to read arbitrarily far back, contradicting the
// ticket's "self-describing, bounded" posture that the rest of this ticket's
// probes (`probe-bound.mjs`'s `DEFAULT_PROBE_TIMEOUT_MS`,
// `diagnostic-reports-scanner.mjs`'s injectable window) already establish.
// 30 days is a reasonable, clearly-stated default for a diagnostics tool
// used interactively/ad hoc, not a value derived from any measured
// retention policy — the implementer or a later ticket may retune it, but it
// must remain BOUNDED, not epoch-since-1970.

import { parseSinceWindow } from './since-window-parser.mjs';

const REFERENCE_NOW = new Date('2026-09-02T12:00:00.000Z');
const THIRTY_DAYS_MS = 30 * 24 * 60 * 60 * 1000;

describe('parseSinceWindow — no value provided', () => {
  it('returns a valid, bounded default of 30 days back from the reference time (not since-epoch)', () => {
    const result = parseSinceWindow(undefined, REFERENCE_NOW);

    expect(result.valid).toBe(true);
    expect(result.since).toBeInstanceOf(Date);

    const expectedMs = REFERENCE_NOW.getTime() - THIRTY_DAYS_MS;
    expect(result.since.getTime()).toBe(expectedMs);

    // Explicitly rule out an unbounded/since-epoch default.
    expect(result.since.getTime()).toBeGreaterThan(new Date('2000-01-01T00:00:00.000Z').getTime());
  });
});

describe('parseSinceWindow — valid ISO8601 date string', () => {
  it('parses a well-formed ISO8601 string into the matching Date', () => {
    const result = parseSinceWindow('2026-08-01T00:00:00.000Z', REFERENCE_NOW);

    expect(result.valid).toBe(true);
    expect(result.since).toBeInstanceOf(Date);
    expect(result.since.toISOString()).toBe('2026-08-01T00:00:00.000Z');
  });
});

describe('parseSinceWindow — malformed input', () => {
  it('returns { valid: false, error } (does not throw) for an unparseable string', () => {
    let result;
    expect(() => {
      result = parseSinceWindow('notadate', REFERENCE_NOW);
    }).not.toThrow();

    expect(result.valid).toBe(false);
    expect(typeof result.error).toBe('string');
    expect(result.error.length).toBeGreaterThan(0);
  });
});

describe('parseSinceWindow — future-dated value', () => {
  it('rejects a value in the future the same way as malformed input', () => {
    const oneDayInFuture = new Date(REFERENCE_NOW.getTime() + 24 * 60 * 60 * 1000).toISOString();

    let result;
    expect(() => {
      result = parseSinceWindow(oneDayInFuture, REFERENCE_NOW);
    }).not.toThrow();

    expect(result.valid).toBe(false);
    expect(typeof result.error).toBe('string');
    expect(result.error.length).toBeGreaterThan(0);
  });
});
