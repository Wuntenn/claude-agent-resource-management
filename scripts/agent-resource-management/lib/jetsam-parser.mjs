// Phase 1 — parses a macOS JetsamEvent diagnostic report into the
// structured fields the diagnostics-parity tooling cares about.
//
// This is a PURE string-in/object-out parser: no file I/O lives here (that is
// `diagnostic-reports-scanner.mjs`'s job), which is what makes it unit
// testable without touching a real macOS DiagnosticReports directory.
//
// Never throws — a truncated, unreadable, or empty report is exactly the
// kind of input a real forensic scan will encounter (files caught mid-write,
// rotated logs, partial reads), and the ticket's outer contract requires
// every probe to degrade gracefully rather than propagate an exception.
//
// REAL .ips FORMAT NOTE (confirmed against real hosts in its retroactive
// validation): a genuine JetsamEvent `.ips` file is NOT one JSON document —
// it is TWO newline-separated JSON documents concatenated in one file: a
// short header line (`bug_type`, `timestamp`, `os_version`, ...) followed by
// a newline, followed by the much larger body document (the jetsam event
// payload, including `largestProcess`). `JSON.parse` on the whole string
// throws ("Unexpected non-whitespace character after JSON") because it stops
// after the header and finds the body's `{` as trailing garbage. We split on
// the first newline and parse each half independently; `timestamp` may live
// in either document depending on the report vintage, so we check the body
// first (jetsam payloads carry their own event timestamp) and fall back to
// the header.

/**
 * @param {string} fileContents
 * @returns {{ available: true, timestamp: string, largestProcess: { name: string, pid: number, rsizeMB: number } } | { available: false, error: string }}
 */
export function parseJetsamEventReport(fileContents) {
  if (typeof fileContents !== 'string' || fileContents.length === 0) {
    return { available: false, error: 'jetsam-parser: empty or non-string input' };
  }

  const newlineIndex = fileContents.indexOf('\n');
  if (newlineIndex === -1) {
    return { available: false, error: 'jetsam-parser: expected a header line and a body document separated by a newline' };
  }

  const headerLine = fileContents.slice(0, newlineIndex);
  const bodyText = fileContents.slice(newlineIndex + 1);

  let header;
  try {
    header = JSON.parse(headerLine);
  } catch (error) {
    return { available: false, error: `jetsam-parser: failed to parse header JSON — ${error.message}` };
  }

  let body;
  try {
    body = JSON.parse(bodyText);
  } catch (error) {
    return { available: false, error: `jetsam-parser: failed to parse body JSON — ${error.message}` };
  }

  if (!header || typeof header !== 'object') {
    return { available: false, error: 'jetsam-parser: parsed header is not an object' };
  }

  if (!body || typeof body !== 'object') {
    return { available: false, error: 'jetsam-parser: parsed body is not an object' };
  }

  const timestamp = body.timestamp ?? header.timestamp;
  const largestProcess = body.largestProcess ?? header.largestProcess;

  if (typeof timestamp !== 'string' || timestamp.length === 0) {
    return { available: false, error: 'jetsam-parser: missing or empty "timestamp" field' };
  }

  if (!largestProcess || typeof largestProcess !== 'object') {
    return { available: false, error: 'jetsam-parser: missing "largestProcess" field' };
  }

  const { name, pid, rsizeMB } = largestProcess;

  if (typeof name !== 'string' || name.length === 0) {
    return { available: false, error: 'jetsam-parser: missing or empty "largestProcess.name" field' };
  }

  if (typeof pid !== 'number' || !Number.isFinite(pid)) {
    return { available: false, error: 'jetsam-parser: missing or non-numeric "largestProcess.pid" field' };
  }

  if (typeof rsizeMB !== 'number' || !Number.isFinite(rsizeMB)) {
    return { available: false, error: 'jetsam-parser: missing or non-numeric "largestProcess.rsizeMB" field' };
  }

  return {
    available: true,
    timestamp,
    largestProcess: { name, pid, rsizeMB },
  };
}
