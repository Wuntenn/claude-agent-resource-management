// Phase 4 — bounded polling-cadence constant for `--poll-footprint`.
//
// RED by construction: `DEFAULT_FOOTPRINT_POLL_INTERVAL_MS` (and its
// documented min/max bound) does not exist in cli.mjs yet — only Phase 3's
// `--poll-footprint` beat has landed. This mirrors the Build Plan's Phase 4
// objective: "Make the 10-30s polling cadence explicitly configurable and
// bounded (a `DEFAULT_FOOTPRINT_POLL_INTERVAL_MS`-style constant plus a
// documented min/max clamp, following `DEFAULT_HEARTBEAT_INTERVAL_MS`'s
// existing pattern and its 'not enforced by cli.mjs itself' doc-comment
// convention)".
//
// Like `DEFAULT_HEARTBEAT_INTERVAL_MS`, this constant documents a cadence
// for an ORCHESTRATOR's own wall-clock timer driving repeated
// `--poll-footprint` invocations — cli.mjs remains a one-shot,
// non-looping process and never itself loops on this value. Per
// `doc-honesty.jest.spec.mjs`'s existing precedent for asserting the exact
// literal value of a `DEFAULT_*` constant (`DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS`),
// this test reads cli.mjs as source text and asserts against the exported
// declarations directly, WITHOUT importing cli.mjs as an ES module — cli.mjs
// has no `import.meta.url` main-guard, so importing it would execute its
// top-level `main().catch(...)` call as a side effect (see cli.mjs's final
// lines), which is neither hermetic nor relevant to what this test pins:
// the literal, statically-declared bound.
//
// This file deliberately does NOT test a `--poll-interval-hint` flag. The
// Build Plan's Phase 4 AC frames that flag as explicitly optional ("if a
// `--poll-interval-hint` flag is added"/"if such a flag is added") — purely
// advisory, never read back by cli.mjs to change its own behavior, since
// cli.mjs does not self-schedule. Adding a flag (and rejection/clamping
// logic for it) purely to give this ticket something to assert on would be
// scope cli.mjs's one-shot, non-looping contract does not need: the "named,
// exported constant documents the default/min/max poll interval, tested
// directly" AC is already unambiguously satisfied by the constant-value
// assertions below, independent of whether a hint flag ever exists.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const cliSource = readFileSync(join(__dirname, 'cli.mjs'), 'utf8');

const TEN_SECONDS_MS = 10 * 1000;
const THIRTY_SECONDS_MS = 30 * 1000;

/**
 * Extracts the numeric literal assigned to `export const <name> = <value>;`
 * from cli.mjs's source text, tolerating either a raw integer literal (e.g.
 * `20000`) or a `<n> * 1000` seconds-expressed literal (the style
 * `DEFAULT_HEARTBEAT_INTERVAL_MS` itself uses: `20 * 1000`).
 */
function extractExportedMsConstant(source, name) {
  const pattern = new RegExp(`export const ${name}\\s*=\\s*([0-9_]+)\\s*(?:\\*\\s*([0-9_]+))?\\s*;`);
  const match = source.match(pattern);
  if (!match) {
    return undefined;
  }
  const first = Number(match[1].replace(/_/g, ''));
  const second = match[2] === undefined ? 1 : Number(match[2].replace(/_/g, ''));
  return first * second;
}

describe('DEFAULT_FOOTPRINT_POLL_INTERVAL_MS', () => {
  it('a named, exported default poll-interval constant exists and falls within the documented 10-30s range', () => {
    const value = extractExportedMsConstant(cliSource, 'DEFAULT_FOOTPRINT_POLL_INTERVAL_MS');

    expect(value).toBeDefined();
    expect(Number.isFinite(value)).toBe(true);
    expect(value).toBeGreaterThanOrEqual(TEN_SECONDS_MS);
    expect(value).toBeLessThanOrEqual(THIRTY_SECONDS_MS);
  });

  it('named, exported min/max poll-interval bound constants exist and match the documented 10-30s range', () => {
    const min = extractExportedMsConstant(cliSource, 'MIN_FOOTPRINT_POLL_INTERVAL_MS');
    const max = extractExportedMsConstant(cliSource, 'MAX_FOOTPRINT_POLL_INTERVAL_MS');

    expect(min).toBeDefined();
    expect(max).toBeDefined();
    expect(min).toBe(TEN_SECONDS_MS);
    expect(max).toBe(THIRTY_SECONDS_MS);
    expect(min).toBeLessThan(max);
  });

  it('the default poll interval sits within its own documented min/max bound', () => {
    const defaultValue = extractExportedMsConstant(cliSource, 'DEFAULT_FOOTPRINT_POLL_INTERVAL_MS');
    const min = extractExportedMsConstant(cliSource, 'MIN_FOOTPRINT_POLL_INTERVAL_MS');
    const max = extractExportedMsConstant(cliSource, 'MAX_FOOTPRINT_POLL_INTERVAL_MS');

    expect(defaultValue).toBeGreaterThanOrEqual(min);
    expect(defaultValue).toBeLessThanOrEqual(max);
  });

  it("documents the same 'not enforced by cli.mjs itself' orchestrator-driven-cadence convention as DEFAULT_HEARTBEAT_INTERVAL_MS", () => {
    const constantIndex = cliSource.indexOf('export const DEFAULT_FOOTPRINT_POLL_INTERVAL_MS');
    expect(constantIndex).toBeGreaterThan(-1);

    // Look at the doc comment immediately preceding the declaration (the
    // nearest block comment above it), mirroring how doc-honesty.jest.spec.mjs
    // locates a declaration's own preceding prose for DEFAULT_CLAIM_LOCK_MAX_ATTEMPTS.
    const precedingText = cliSource.slice(0, constantIndex);
    const lastCommentStart = precedingText.lastIndexOf('/**');
    expect(lastCommentStart).toBeGreaterThan(-1);
    const docComment = precedingText.slice(lastCommentStart);

    expect(docComment).toMatch(/not enforced by cli\.mjs itself/i);
    expect(docComment).toMatch(/orchestrator/i);
  });
});
