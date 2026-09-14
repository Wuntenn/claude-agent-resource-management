// Unit tests for scripts/agent-resource-management/lib/jetsam-parser.mjs
// (Phase 1 — "diagnostics-parity tooling", Phase 3 real-host fix).
//
// SHAPE:
//   export function parseJetsamEventReport(fileContents: string) -> {
//     available: true,
//     timestamp: string,
//     largestProcess: { name: string, pid: number, rsizeMB: number },
//   } | { available: false, error: string }
//
// The parser must NEVER throw — a truncated or unreadable report is exactly
// the kind of input a real forensic scan will encounter (files caught
// mid-write, rotated logs, partial reads), and the ticket's outer contract
// requires every probe to degrade gracefully rather than propagate an
// exception.
//
// FIXTURE NOTE: a Phase 3 real-host validation run of this ticket's
// diagnostics collector discovered that a genuine macOS JetsamEvent `.ips`
// file is NOT a single JSON document — it is TWO newline-separated JSON
// documents concatenated in one file: a short header line (`bug_type`,
// `timestamp`, `os_version`), then a newline, then the much larger body
// document carrying `largestProcess`. This is documented (in redacted/
// synthetic form) in design/okf/agent-fleet-host-freeze-2026-08-20.md. The
// fixtures below are clean synthetic constructions mirroring that
// structure — NOT captured content from any real host.

import { parseJetsamEventReport } from './jetsam-parser.mjs';

const SYNTHETIC_HEADER = JSON.stringify({
  bug_type: '298',
  timestamp: '2026-08-25 09:12:33.00 +0100',
  os_version: 'macOS 26.5.2 (25F84)',
});

const SYNTHETIC_BODY = JSON.stringify({
  reason: 'vm-pageout-jetsam',
  largestProcess: {
    name: 'Google Chrome Helper (Renderer)',
    pid: 87033,
    rsizeMB: 612,
  },
  memoryStatus: {
    available_pages: 12345,
    compressor_size: 5678,
  },
});

const WELL_FORMED_JETSAM_REPORT = `${SYNTHETIC_HEADER}\n${SYNTHETIC_BODY}`;

describe('parseJetsamEventReport — well-formed input', () => {
  it('parses largestProcess name, pid, and RSS/footprint into structured fields', () => {
    const result = parseJetsamEventReport(WELL_FORMED_JETSAM_REPORT);

    expect(result.available).toBe(true);
    expect(result.largestProcess).toEqual({
      name: 'Google Chrome Helper (Renderer)',
      pid: 87033,
      rsizeMB: 612,
    });
  });

  it('parses the event timestamp as a non-empty string', () => {
    const result = parseJetsamEventReport(WELL_FORMED_JETSAM_REPORT);

    expect(typeof result.timestamp).toBe('string');
    expect(result.timestamp.length).toBeGreaterThan(0);
  });
});

describe('parseJetsamEventReport — regression: real two-document .ips structure', () => {
  it('parses a header line + newline + body document into the expected fields (does not throw "Extra data")', () => {
    const result = parseJetsamEventReport(WELL_FORMED_JETSAM_REPORT);

    expect(result).toEqual({
      available: true,
      timestamp: '2026-08-25 09:12:33.00 +0100',
      largestProcess: {
        name: 'Google Chrome Helper (Renderer)',
        pid: 87033,
        rsizeMB: 612,
      },
    });
  });

  it('degrades gracefully on a single-JSON-document input with no header/body newline (the historical bug shape)', () => {
    const singleDocument = SYNTHETIC_BODY;

    let result;
    expect(() => {
      result = parseJetsamEventReport(singleDocument);
    }).not.toThrow();

    expect(result.available).toBe(false);
    expect(typeof result.error).toBe('string');
  });

  it('degrades gracefully when the body document (after the header newline) fails to parse', () => {
    const badBody = `${SYNTHETIC_HEADER}\n{ not valid json`;

    let result;
    expect(() => {
      result = parseJetsamEventReport(badBody);
    }).not.toThrow();

    expect(result.available).toBe(false);
    expect(typeof result.error).toBe('string');
  });
});

describe('parseJetsamEventReport — malformed/truncated JSON', () => {
  it('returns an { available: false } result rather than throwing on truncated JSON', () => {
    const truncated = '{"timestamp": "2026-08-25 09:12:33.00 +0100", "largestProcess": {"name": "Sa';

    let result;
    expect(() => {
      result = parseJetsamEventReport(truncated);
    }).not.toThrow();

    expect(result.available).toBe(false);
    expect(typeof result.error).toBe('string');
    expect(result.error.length).toBeGreaterThan(0);
  });

  it('returns an { available: false } result rather than throwing on non-JSON garbage input', () => {
    let result;
    expect(() => {
      result = parseJetsamEventReport('not even close to json {{{');
    }).not.toThrow();

    expect(result.available).toBe(false);
    expect(typeof result.error).toBe('string');
  });
});

describe('parseJetsamEventReport — empty input', () => {
  it('returns an { available: false } result rather than throwing on an empty string', () => {
    let result;
    expect(() => {
      result = parseJetsamEventReport('');
    }).not.toThrow();

    expect(result.available).toBe(false);
    expect(typeof result.error).toBe('string');
  });
});
