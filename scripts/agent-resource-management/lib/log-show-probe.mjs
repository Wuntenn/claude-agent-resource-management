// Phase 1 — shells out to `log show` to retrieve NDJSON-formatted
// system log entries matching a predicate since a given timestamp.
//
// Reuses `probeExecOptions()` from `probe-bound.mjs` for the same
// time-boxing rationale `footprint-sampler.mjs` uses: `execFileSync` cannot
// be bounded after the fact, so the timeout/killSignal options must be
// passed up front.
//
// Never throws: a missing `log` binary or zero matching entries are both
// self-describing results. "Nothing found" folds into `status: 'ok'` with an
// empty array; "could not run at all" (e.g. missing binary) is `status:
// 'failed'`, mirroring `collect.jest.spec.mjs`'s convention that a missing
// binary is reported as failed, not silently downgraded to skipped.
//
// DATE FORMAT NOTE (confirmed by a real failure in Phase 3 real-host
// validation): macOS `log show --start` does NOT accept an ISO-8601
// timestamp — it expects local-time `"YYYY-MM-DD HH:MM:SS"` (space
// separator, no `T`, no timezone suffix, no milliseconds). Passing an
// ISO-8601 string produced `Failed conversion of '<timestamp>' using format
// '%Y-%m-%d %H:%M:%S'`. `formatForLogShow()` below converts a `Date` (or a
// parseable date/ISO string) into that exact format, in local time — `log
// show` uses local time by default and we don't introduce a timezone flag.

import { execFileSync } from 'node:child_process';
import { probeExecOptions } from './probe-bound.mjs';

/**
 * Formats a Date (or parseable date string) as the local-time
 * "YYYY-MM-DD HH:MM:SS" string `log show --start` requires.
 *
 * @param {Date | string} since
 * @returns {string}
 */
function formatForLogShow(since) {
  const date = since instanceof Date ? since : new Date(since);

  const pad = (value) => String(value).padStart(2, '0');

  const year = date.getFullYear();
  const month = pad(date.getMonth() + 1);
  const day = pad(date.getDate());
  const hours = pad(date.getHours());
  const minutes = pad(date.getMinutes());
  const seconds = pad(date.getSeconds());

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

const LOG_BIN = '/usr/bin/log';

// `probeExecOptions()` sets encoding/timeout/killSignal only — it does not set
// `maxBuffer`, so `execFileSync` would otherwise fall back to Node's default
// 1 MB ceiling. A `log show --style ndjson` scan over even a moderate window
// routinely exceeds that, producing an `ENOBUFS` that this function's catch
// block would report identically to "binary is missing" — a generic
// `status: 'failed'` that hides the real cause. Layered on top of (not
// replacing) `probeExecOptions()`'s other settings.
const LOG_SHOW_MAX_BUFFER_BYTES = 50 * 1024 * 1024; // 50 MB

/**
 * Parses NDJSON stdout (one JSON object per line) into an array of entries.
 * Lines that fail to parse are skipped rather than aborting the whole parse.
 *
 * @param {string} stdout
 * @returns {Array<object>}
 */
function parseNdjson(stdout) {
  return stdout
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter((entry) => entry !== null);
}

/**
 * @param {{ predicate: string, since: string }} params
 * @returns {Promise<{ status: 'ok' | 'failed', entries: Array<object>, error?: string }>}
 */
export async function runLogShow({ predicate, since }) {
  try {
    const stdout = execFileSync(
      LOG_BIN,
      ['show', '--predicate', predicate, '--start', formatForLogShow(since), '--style', 'ndjson'],
      { ...probeExecOptions(), maxBuffer: LOG_SHOW_MAX_BUFFER_BYTES },
    );
    return { status: 'ok', entries: parseNdjson(stdout) };
  } catch (error) {
    return { status: 'failed', entries: [], error: `log-show-probe: ${error.message}` };
  }
}
