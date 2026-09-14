// Phase 1 — parses a `--since` CLI value into a bounded `Date` window.
//
// Follows the `{ valid: boolean }` result convention already established by
// `cli.mjs`'s `validateBareOrTrueFlag` (returns `{ valid: true/false }`
// rather than throwing, so a CLI call site keeps explicit control over
// exactly when the process exits). Never throws.
//
// DEFAULT WINDOW: 30 days back from `referenceNow` when no value is given.
// Diagnostic reports and jetsam/kill logs on macOS are not retained
// indefinitely, so an unbounded since-epoch default would make a bare
// `--retroactive` scan silently attempt to read arbitrarily far back. 30 days
// is a reasonable, clearly-stated default for interactive/ad-hoc use, not a
// value derived from any measured retention policy.

const DEFAULT_WINDOW_MS = 30 * 24 * 60 * 60 * 1000;

/**
 * @param {string | undefined} rawValue
 * @param {Date} [referenceNow]
 * @returns {{ valid: true, since: Date } | { valid: false, error: string }}
 */
export function parseSinceWindow(rawValue, referenceNow = new Date()) {
  if (rawValue === undefined) {
    return { valid: true, since: new Date(referenceNow.getTime() - DEFAULT_WINDOW_MS) };
  }

  if (typeof rawValue !== 'string' || rawValue.length === 0) {
    return { valid: false, error: 'since-window-parser: --since must be a non-empty string' };
  }

  const parsedMs = Date.parse(rawValue);
  if (!Number.isFinite(parsedMs)) {
    return { valid: false, error: `since-window-parser: could not parse "${rawValue}" as a date` };
  }

  if (parsedMs > referenceNow.getTime()) {
    return { valid: false, error: `since-window-parser: "${rawValue}" is in the future` };
  }

  return { valid: true, since: new Date(parsedMs) };
}
