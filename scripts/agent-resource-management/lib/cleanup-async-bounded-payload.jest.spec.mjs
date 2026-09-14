// Phase 2 — BOUNDED-PAYLOAD exhaustive-state-coverage matrix over
// `sweepStaleTempDirsAsync`'s `errors` ring and `truncatedCount`.
//
// ---------------------------------------------------------------------------
// THE PROPERTY THIS FILE PROTECTS
// ---------------------------------------------------------------------------
//
// The sweep's report is not returned to a log. It is EMBEDDED IN THE VERDICT
// JSON that `cli.mjs` writes to stdout, and the PreToolUse hook `JSON.parse`s
// that whole stream and FAILS CLOSED on anything it cannot parse or afford to
// read. So the size of `errors` is not an aesthetic concern — it is a
// denial-of-service surface on the admission gate for every agent spawn on this
// host, reachable without any privilege at all:
//
//   * `/private/tmp` is shared between users on macOS, and under launchd's
//     minimal environment (TMPDIR unset) it IS the watchdog's temp root. Another
//     user's 3,000 root-owned `cdk.out-*` dirs yield 3,000 EPERM failures on a
//     single beat.
//   * A read-only or full volume fails EVERY removal, not a few.
//   * The host genuinely carries a multi-thousand-entry `cdk.out` backlog today.
//
// An unbounded `errors` array turns any of those into a multi-megabyte JSON
// document on a hot path. The contract is therefore a RING BOUNDED AT
// `SWEEP_MAX_ERRORS`, with everything beyond it COUNTED in `truncatedCount` —
// counted, not discarded silently, because "8 errors" and "8 errors and 4,992
// more" are different operational situations and the operator must be able to
// tell them apart.
//
// WHY A NAIVE IMPLEMENTATION PASSES A WEAKER TEST. A spot-check — "induce a few
// failures, assert some errors came back" — is satisfied by an unbounded
// `errors.push(...)` with no ring at all. The two states that actually matter
// are the BOUNDARY (exactly `SWEEP_MAX_ERRORS`: the off-by-one that makes the
// ring hold one too many or one too few) and the PATHOLOGICAL (thousands: the
// only state that distinguishes a real bound from a large one). Both are
// invisible to a spot-check, so this file asserts `errors.length` AND
// `truncatedCount` in EVERY cell of the matrix — including the empty cell,
// where `truncatedCount` must be `0` and not `undefined`, since the hook reads
// the field unconditionally.
//
// THE `JSON.stringify(result).length` BOUND is asserted alongside the counts
// because the counts alone do not bound the payload: eight errors carrying a
// megabyte-long `message` each (an `EPERM` from a deep path, a native binding
// that threw a stringified struct) satisfies every count assertion and still
// produces the document the hook cannot afford. Bounding the serialised length
// is the only assertion that closes that, and the final cell drives it directly.
//
// ---------------------------------------------------------------------------
// RED BY CONSTRUCTION
// ---------------------------------------------------------------------------
//
// `./cleanup.mjs` exports neither `sweepStaleTempDirsAsync` nor
// `SWEEP_MAX_ERRORS`; the named import below fails ESM link-time resolution.
// `SWEEP_MAX_ERRORS` is IMPORTED rather than restated as `8` deliberately — a
// matrix built from a duplicated literal would keep passing while the real
// constant moved, which is the failure mode this file is least able to afford.
//
// SAFETY: `node:fs/promises` is mocked, so no removal is ever real. The sweep
// root is an `fs.mkdtempSync` scratch directory nominated explicitly through
// `allowedPrefixes`; the process's real `$TMPDIR` is never read or written, and
// the sentinel cell at the end pins that.

import { jest } from '@jest/globals';
import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const readdirMock = jest.fn();
const statMock = jest.fn();
const lstatMock = jest.fn();
const rmMock = jest.fn();

const fsPromisesMock = {
  readdir: readdirMock,
  stat: statMock,
  lstat: lstatMock,
  rm: rmMock,
  rmdir: jest.fn(),
  unlink: jest.fn(),
  realpath: jest.fn(),
  opendir: jest.fn(),
  access: jest.fn(),
  readFile: jest.fn(),
  writeFile: jest.fn(),
  mkdir: jest.fn(),
  mkdtemp: jest.fn(),
  utimes: jest.fn(),
  copyFile: jest.fn(),
  cp: jest.fn(),
  chmod: jest.fn(),
};

jest.unstable_mockModule('node:fs/promises', () => ({ ...fsPromisesMock, default: fsPromisesMock }));

const { SWEEP_MAX_CANDIDATES, SWEEP_MAX_ERRORS, sweepStaleTempDirsAsync } = await import('./cleanup.mjs');

const HOUR_MS = 60 * 60 * 1000;

/**
 * The serialised-payload ceiling. Sized as "comfortably more than
 * `SWEEP_MAX_ERRORS` ordinary error objects, and far less than anything that
 * could trouble a `JSON.parse` on a hot beat". It is an ORDER-OF-MAGNITUDE
 * bound, not a tight one — a tight bound would fail on an innocent path-length
 * change and teach the next author to loosen it rather than to look.
 */
const MAX_SERIALISED_REPORT_BYTES = 8 * 1024;

function direntDir(name) {
  return { name, isDirectory: () => true, isFile: () => false, isSymbolicLink: () => false };
}

let scratchRoot;
let root;
let stdoutSpy;

beforeEach(() => {
  scratchRoot = mkdtempSync(join(tmpdir(), 'arm-bounded-payload-'));
  root = realpathSync(scratchRoot);
  readdirMock.mockReset();
  statMock.mockReset();
  lstatMock.mockReset();
  rmMock.mockReset();
  stdoutSpy = jest.spyOn(process.stdout, 'write').mockImplementation(() => true);
});

afterEach(() => {
  stdoutSpy.mockRestore();
  rmSync(scratchRoot, { recursive: true, force: true });
});

/**
 * Seeds `failingCount` candidates whose `rm` rejects plus `SUCCEEDING_COUNT`
 * that succeed, and returns the sweep's report.
 *
 * The succeeding candidates are load-bearing, not filler: they are what proves
 * the ring TRUNCATES rather than ABORTS. An implementation that stopped at the
 * error ceiling would satisfy every `errors.length` assertion here while
 * silently abandoning the rest of the backlog.
 */
const SUCCEEDING_COUNT = 2;

async function sweepWithInducedFailures(failingCount) {
  const now = Date.now();
  const failing = Array.from({ length: failingCount }, (_, index) => `cdk.out-fail-${index}`);
  const succeeding = Array.from({ length: SUCCEEDING_COUNT }, (_, index) => `cdk.out-ok-${index}`);

  readdirMock.mockImplementation((dirPath) => {
    if (dirPath === root) return Promise.resolve([...failing, ...succeeding].map(direntDir));
    return Promise.resolve([]); // every candidate is an empty directory — 0 bytes, fast to measure
  });
  statMock.mockResolvedValue({
    size: 0,
    mtimeMs: now - 20 * HOUR_MS,
    isDirectory: () => true,
    isFile: () => false,
  });
  rmMock.mockImplementation((targetPath) => {
    if (targetPath.includes('cdk.out-fail-')) {
      const error = new Error('EPERM: operation not permitted');
      error.code = 'EPERM';
      return Promise.reject(error);
    }
    return Promise.resolve(undefined);
  });

  return sweepStaleTempDirsAsync({
    dir: root,
    ageThresholdMs: 2 * HOUR_MS,
    now,
    // Both bounds lifted well clear, so this file measures the ERROR bound and
    // nothing else. The candidate cap and the deadline have their own specs.
    budgetMs: 20_000,
    maxCandidates: failingCount + SUCCEEDING_COUNT + 10,
    allowedPrefixes: [root],
  });
}

describe('RED-state premise — the exports under test exist', () => {
  test('`sweepStaleTempDirsAsync`, `SWEEP_MAX_ERRORS` and `SWEEP_MAX_CANDIDATES` are exported', () => {
    // A dynamic `await import` destructures a missing export to `undefined`
    // rather than failing at link time, so this cell is what makes the red
    // state legible.
    expect(typeof sweepStaleTempDirsAsync).toBe('function');
    expect(typeof SWEEP_MAX_ERRORS).toBe('number');
    expect(typeof SWEEP_MAX_CANDIDATES).toBe('number');
  });
});

describe('the errors ring is bounded and the overflow is COUNTED, across every state of the matrix', () => {
  // Exhaustive state coverage over the boundary rather than a sample: the
  // empty case, the single case, exactly at the bound, one past it, and the
  // pathological multi-thousand case that only a real ring survives.
  test.each([
    ['0 induced failures — the clean sweep', () => 0],
    ['1 induced failure', () => 1],
    ['exactly SWEEP_MAX_ERRORS failures — the ring is full but nothing is truncated', () => SWEEP_MAX_ERRORS],
    ['SWEEP_MAX_ERRORS + 1 failures — the off-by-one cell', () => SWEEP_MAX_ERRORS + 1],
    ['5000 failures — the shared-/private/tmp, root-owned-backlog case', () => 5_000],
  ])('%s', async (_label, failingCountOf) => {
    const failingCount = failingCountOf();

    const result = await sweepWithInducedFailures(failingCount);

    const expectedErrorCount = Math.min(failingCount, SWEEP_MAX_ERRORS);
    const expectedTruncated = Math.max(0, failingCount - SWEEP_MAX_ERRORS);

    // BOTH fields asserted in EVERY cell — including the empty one, where
    // `truncatedCount` must be a real `0` and not `undefined`, because the hook
    // reads the field unconditionally.
    expect(result.errors).toHaveLength(expectedErrorCount);
    expect(result.truncatedCount).toBe(expectedTruncated);
    expect(typeof result.truncatedCount).toBe('number');

    // Every recorded error is a removal failure, correctly attributed — the
    // ring must not degrade into anonymous counters under load.
    for (const error of result.errors) {
      expect(error.kind).toBe('removal-failed');
      expect(typeof error.path).toBe('string');
      expect(typeof error.message).toBe('string');
    }

    // TRUNCATED, NOT ABORTED: the sweep kept going past the error ceiling and
    // still removed the healthy candidates. This is what a naive
    // "stop when errors are full" implementation fails.
    expect(result.removedCount).toBe(SUCCEEDING_COUNT);
    expect(result.budgetExhausted).toBe(false);

    // The payload bound, asserted in every cell rather than once: 5,000
    // failures and 0 failures must produce documents of the same order.
    expect(JSON.stringify(result).length).toBeLessThan(MAX_SERIALISED_REPORT_BYTES);

    expect(stdoutSpy).not.toHaveBeenCalled();
  });

  test('the report shape carries all seven contract fields, whatever state it is in', async () => {
    const result = await sweepWithInducedFailures(SWEEP_MAX_ERRORS + 3);

    for (const field of [
      'removedCount',
      'reclaimedBytes',
      'errors',
      'truncatedCount',
      'budgetExhausted',
      'candidatesCapped',
      'sizeUnmeasuredCount',
    ]) {
      expect(result).toHaveProperty(field);
    }
    expect(Array.isArray(result.errors)).toBe(true);
    expect(typeof result.budgetExhausted).toBe('boolean');
    expect(typeof result.candidatesCapped).toBe('boolean');
    expect(typeof result.sizeUnmeasuredCount).toBe('number');
  });

  test('`SWEEP_MAX_ERRORS` is a small positive integer — the bound is a bound, not a formality', () => {
    expect(Number.isInteger(SWEEP_MAX_ERRORS)).toBe(true);
    expect(SWEEP_MAX_ERRORS).toBeGreaterThan(0);
    // A "bound" of thousands would satisfy every cell above while re-opening
    // the payload problem entirely.
    expect(SWEEP_MAX_ERRORS).toBeLessThanOrEqual(64);
  });
});

describe('the payload bound survives a pathological individual error, not just pathological COUNTS', () => {
  test('a single 1 MB rejection message cannot blow the serialised report past the bound', async () => {
    // The state every count-based assertion misses. `errors.length === 1`,
    // `truncatedCount === 0`, and the document is a megabyte — on the stream
    // the PreToolUse hook parses to decide whether any agent may spawn.
    // A real EPERM from a deeply-nested path, or a native binding that threw a
    // stringified struct, reaches this shape without any adversary at all.
    const now = Date.now();
    const HUGE_MESSAGE = 'x'.repeat(1024 * 1024);

    readdirMock.mockImplementation((dirPath) => {
      if (dirPath === root) return Promise.resolve([direntDir('cdk.out-huge-error')]);
      return Promise.resolve([]);
    });
    statMock.mockResolvedValue({ size: 0, mtimeMs: now - 20 * HOUR_MS, isDirectory: () => true, isFile: () => false });
    rmMock.mockRejectedValue(new Error(HUGE_MESSAGE));

    const result = await sweepStaleTempDirsAsync({
      dir: root,
      ageThresholdMs: 2 * HOUR_MS,
      now,
      budgetMs: 20_000,
      allowedPrefixes: [root],
    });

    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].kind).toBe('removal-failed');
    // The message must be TRUNCATED rather than dropped: an operator still
    // needs to see what kind of failure it was.
    expect(result.errors[0].message.length).toBeLessThan(MAX_SERIALISED_REPORT_BYTES);
    expect(result.errors[0].message.length).toBeGreaterThan(0);
    expect(JSON.stringify(result).length).toBeLessThan(MAX_SERIALISED_REPORT_BYTES);
    expect(stdoutSpy).not.toHaveBeenCalled();
  });
});

// ===========================================================================
// The OTHER bound on the same beat: how many candidates one sweep may touch.
// Kept here rather than in its own file because it is the same argument — a
// destructive primitive on a hot path must have a ceiling on the WORK it does,
// not only on the REPORT it returns.
// ===========================================================================

describe('the candidate cap bounds the work per beat, oldest-first, leaving the remainder for the next beat', () => {
  /**
   * Seeds `count` stale candidates whose ages DESCEND with their index
   * (index 0 is the oldest), so "oldest-first" is observable rather than
   * assumed. A cap that took an arbitrary slice would pass a same-age fixture.
   */
  async function sweepBacklog(count, maxCandidates) {
    const now = Date.now();
    const names = Array.from({ length: count }, (_, index) => `cdk.out-backlog-${index}`);
    const ageOf = (name) => {
      const index = Number(name.slice('cdk.out-backlog-'.length));
      return now - (100 - index) * HOUR_MS;
    };

    readdirMock.mockImplementation((dirPath) => {
      if (dirPath === root) return Promise.resolve(names.map(direntDir));
      return Promise.resolve([]);
    });
    statMock.mockImplementation((entryPath) => {
      const name = entryPath.slice(root.length + 1);
      return Promise.resolve({
        size: 0,
        mtimeMs: names.includes(name) ? ageOf(name) : now - 200 * HOUR_MS,
        isDirectory: () => true,
        isFile: () => false,
      });
    });
    rmMock.mockResolvedValue(undefined);

    const result = await sweepStaleTempDirsAsync({
      dir: root,
      ageThresholdMs: 2 * HOUR_MS,
      now,
      budgetMs: 20_000,
      maxCandidates,
      allowedPrefixes: [root],
    });
    return { result, names };
  }

  test('a backlog larger than the cap removes exactly `maxCandidates`, flags `candidatesCapped`, and leaves the rest untouched', async () => {
    const BACKLOG = 40;
    const CAP = 6;

    const { result, names } = await sweepBacklog(BACKLOG, CAP);

    expect(result.removedCount).toBe(CAP);
    expect(result.candidatesCapped).toBe(true);
    expect(rmMock).toHaveBeenCalledTimes(CAP);
    // The remainder is genuinely UNTOUCHED — not merely unremoved. An
    // implementation that measured all 40 and removed 6 would have done 40
    // directories' worth of walking on the beat, which is the cost the cap
    // exists to avoid.
    for (const name of names.slice(CAP)) {
      expect(rmMock).not.toHaveBeenCalledWith(join(root, name), expect.anything());
    }
    // OLDEST FIRST: the cap drains the backlog in a defined order, so
    // successive beats make progress instead of re-picking the same slice.
    for (const name of names.slice(0, CAP)) {
      expect(rmMock).toHaveBeenCalledWith(join(root, name), expect.objectContaining({ recursive: true, force: true }));
    }
    expect(JSON.stringify(result).length).toBeLessThan(MAX_SERIALISED_REPORT_BYTES);
  });

  test('a backlog exactly at the cap is NOT flagged as capped — the boundary cell', async () => {
    const CAP = 6;
    const { result } = await sweepBacklog(CAP, CAP);

    expect(result.removedCount).toBe(CAP);
    // `candidatesCapped` means "there was more to do"; at exactly the cap there
    // was not. Conflating the two makes every full beat look like a backlog.
    expect(result.candidatesCapped).toBe(false);
  });

  test('a backlog below the cap is not capped and drains completely', async () => {
    const { result } = await sweepBacklog(3, 6);

    expect(result.removedCount).toBe(3);
    expect(result.candidatesCapped).toBe(false);
    expect(result.truncatedCount).toBe(0);
  });

  test('omitting `maxCandidates` applies `SWEEP_MAX_CANDIDATES` — asserted against the real constant, never a literal', async () => {
    // A default of "unbounded" would make the cap opt-in, and the one caller
    // that matters (the beat path) is the one least likely to pass it.
    expect(Number.isInteger(SWEEP_MAX_CANDIDATES)).toBe(true);
    expect(SWEEP_MAX_CANDIDATES).toBeGreaterThan(0);

    const { result } = await sweepBacklog(SWEEP_MAX_CANDIDATES + 5, undefined);

    expect(result.removedCount).toBe(SWEEP_MAX_CANDIDATES);
    expect(result.candidatesCapped).toBe(true);
  });
});

describe('SENTINEL — this file can never be pointed at the real host temp root', () => {
  test('the process`s real os.tmpdir() is refused, and the refusal itself is a bounded payload', async () => {
    const result = await sweepStaleTempDirsAsync({
      dir: tmpdir(),
      ageThresholdMs: 2 * HOUR_MS,
      now: Date.now(),
      budgetMs: 20_000,
      allowedPrefixes: [root],
    });

    expect(result.removedCount).toBe(0);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].kind).toBe('confinement-refused');
    expect(result.truncatedCount).toBe(0);
    expect(JSON.stringify(result).length).toBeLessThan(MAX_SERIALISED_REPORT_BYTES);
    expect(rmMock).not.toHaveBeenCalled();
    expect(readdirMock).not.toHaveBeenCalled();
  });
});
