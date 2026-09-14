// Phase 2 — DEADLINE/BUDGET cells for `sweepStaleTempDirsAsync`.
//
// ---------------------------------------------------------------------------
// THE PROPERTY THIS FILE PROTECTS
// ---------------------------------------------------------------------------
//
// The sweep runs INSIDE a PreToolUse hook beat. The hook `JSON.parse`s the
// whole of `cli.mjs`'s stdout and fails closed on a parse error, and every
// agent spawn on this host waits on that beat. So an unbounded sweep is not
// "slow" — it is a denial of the orchestration fabric, triggered by nothing
// more exotic than an NFS mount going quiet, a `/private/tmp` shared with
// another user, or a backlog of 3,000 real `cdk.out` dirs (which this host
// genuinely has). The contract is therefore a HARD DEADLINE, not a courtesy:
// the returned promise SETTLES within `budgetMs` regardless of what the
// filesystem does, reporting whatever work it managed, with
// `budgetExhausted: true`.
//
// WHY A NAIVE IMPLEMENTATION PASSES A WEAKER TEST. The obvious implementation
// checks `performance.now()` at the top of the candidate loop. That passes any
// test built from FAST fs mocks — the loop always gets to its next check — and
// then hangs forever in production the first time a single `readdir` never
// settles, because the check is only reached between calls. It also passes a
// test that merely asserts "budgetExhausted is true after a long run", since
// that says nothing about WHEN the promise settled. These cells are built to
// fail both:
//
//   * a NEVER-SETTLING `readdir` (cell 1) — only a deadline RACE survives it,
//     a loop-top check cannot;
//   * a size walk slow enough that the deadline lands MID-DIRECTORY-LEVEL
//     (cells 2 and 3) — only a per-level check inside the recursive walk
//     survives it;
//   * and, most importantly, cell 3's REMOVAL assertion: a candidate whose
//     size measurement was ABANDONED must NOT be removed. An implementation
//     that removes it anyway is not merely inaccurate — it reports a
//     `reclaimedBytes` figure the disk never saw, which is the number the AMBER
//     response loop uses to decide whether the pressure is relieved. Wrong
//     accounting on a recursive-removal primitive is worse than no removal.
//
// STDOUT PURITY (contract property 7) is asserted in every cell here, not in a
// spec of its own, because it is only meaningful when something is actually
// going wrong — a hang, a cut-off walk, a rejected budget. That is exactly when
// a well-meaning `console.log` gets added.
//
// ---------------------------------------------------------------------------
// RED BY CONSTRUCTION
// ---------------------------------------------------------------------------
//
// `./cleanup.mjs` exists but exports NONE of `sweepStaleTempDirsAsync`,
// `SWEEP_BUDGET_MS`, `SWEEP_MAX_ERRORS`. The named import below therefore fails
// ESM link-time resolution with
// `SyntaxError: The requested module './cleanup.mjs' does not provide an export
// named 'sweepStaleTempDirsAsync'`. That is this file's documented red state.
// Do NOT add a stub export to turn it green.
//
// ---------------------------------------------------------------------------
// MOCKING IDIOM AND ITS ONE DELIBERATE ASYMMETRY
// ---------------------------------------------------------------------------
//
// `jest.unstable_mockModule('node:fs/promises', ...)` — `cleanup-adapter
// .jest.spec.mjs`'s house idiom, moved to the promises surface. `node:fs` is
// left REAL on purpose: the Phase 1 anchor (`classifySweepRoot`) resolves the
// sweep root with the synchronous `realpathSync`, and that resolution must be a
// genuine one or the confinement gate under test would be simulated rather than
// exercised. So the root is a REAL `fs.mkdtempSync` scratch directory, and the
// fake tree beneath it is keyed on that root's REALPATH — because
// `resolvedDir`, not the written `dir`, is what the sweep must walk.
//
// SAFETY: no cell reads or writes the process's real `$TMPDIR`. The scratch
// root is created with `fs.mkdtempSync`, nominated explicitly through
// `allowedPrefixes`, and removed in `afterEach`. Nothing under it is ever
// really deleted either — `rm` is a mock. The final cell in this file is an
// explicit SENTINEL: the real `os.tmpdir()` is refused, so a future edit that
// pointed these cells at the host temp root would fail loudly rather than
// quietly sweeping 3,000+ real `cdk.out` dirs (see `./cleanup.jest.spec.mjs`).

import { jest } from '@jest/globals';
import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const readdirMock = jest.fn();
const statMock = jest.fn();
const lstatMock = jest.fn();
const rmMock = jest.fn();

/**
 * A deliberately broad promise-surface: a mock factory REPLACES the module, so
 * any named import the implementation reaches for that is absent here fails at
 * link time rather than at the assertion. Breadth here costs nothing; absence
 * costs a misleading red.
 */
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

const { SWEEP_BUDGET_MS, sweepStaleTempDirsAsync } = await import('./cleanup.mjs');

const HOUR_MS = 60 * 60 * 1000;
/** Ample real-clock headroom over each cell's `budgetMs`, so the settle assertion is not a flake generator. */
const SETTLE_SLACK_MS = 2_500;

/** Dirent-shaped, matching the three predicates `listCandidateEntries` already consults. */
function direntDir(name) {
  return { name, isDirectory: () => true, isFile: () => false, isSymbolicLink: () => false };
}

function direntFile(name) {
  return { name, isDirectory: () => false, isFile: () => true, isSymbolicLink: () => false };
}

/** A promise that never settles — the only honest way to express "the filesystem went quiet". */
function hangsForever() {
  return new Promise(() => {});
}

function settlesAfter(ms, value) {
  return new Promise((resolve) => {
    setTimeout(() => resolve(value), ms);
  });
}

let scratchRoot;
/** The REALPATH of `scratchRoot` — what `classifySweepRoot` returns, and therefore what the sweep must walk. */
let root;
let stdoutSpy;
let stderrSpy;

beforeEach(() => {
  scratchRoot = mkdtempSync(join(tmpdir(), 'arm-async-budget-'));
  root = realpathSync(scratchRoot);

  readdirMock.mockReset();
  statMock.mockReset();
  lstatMock.mockReset();
  rmMock.mockReset();
  rmMock.mockResolvedValue(undefined);

  // Contract property 7. `mockImplementation(() => true)` rather than a
  // pass-through: a genuine write would corrupt Jest's own reporter stream, so
  // a violation must be silenced AND recorded, never echoed.
  stdoutSpy = jest.spyOn(process.stdout, 'write').mockImplementation(() => true);
  stderrSpy = jest.spyOn(process.stderr, 'write').mockImplementation(() => true);
});

afterEach(() => {
  stdoutSpy.mockRestore();
  stderrSpy.mockRestore();
  rmSync(scratchRoot, { recursive: true, force: true });
});

/** Contract property 7 — the sweep writes NOTHING to stdout, ever. */
function expectStdoutUntouched() {
  expect(stdoutSpy).not.toHaveBeenCalled();
}

function sweep(overrides = {}) {
  return sweepStaleTempDirsAsync({
    dir: root,
    ageThresholdMs: 2 * HOUR_MS,
    now: Date.now(),
    allowedPrefixes: [root],
    ...overrides,
  });
}

describe('RED-state premise — the export under test exists', () => {
  test('`sweepStaleTempDirsAsync` is exported as a function', () => {
    // A dynamic `await import` destructures a missing export to `undefined`
    // rather than failing at link time, so this cell is what makes the red
    // state legible: it names the missing export instead of leaving every cell
    // below to fail with "not a function".
    expect(typeof sweepStaleTempDirsAsync).toBe('function');
  });
});

describe('the deadline is a race, not a loop-top check — a hung filesystem still settles', () => {
  test('cell 1: a `readdir` that NEVER settles still resolves on the deadline, with zero removals', async () => {
    // The single cell a loop-top budget check cannot pass. There is no "next
    // iteration" at which to notice the budget: control never returns.
    readdirMock.mockImplementation(() => hangsForever());

    const budgetMs = 150;
    const startedAt = Date.now();
    const result = await sweep({ budgetMs });
    const elapsedMs = Date.now() - startedAt;

    expect(elapsedMs).toBeLessThan(budgetMs + SETTLE_SLACK_MS);
    expect(result.budgetExhausted).toBe(true);
    expect(result.removedCount).toBe(0);
    expect(result.reclaimedBytes).toBe(0);
    expect(result.errors.some((error) => error.kind === 'budget-exhausted')).toBe(true);
    // Exactly ONE budget error, not one per abandoned candidate — the payload
    // bound is a separate spec, but the shape is pinned at its source.
    expect(result.errors.filter((error) => error.kind === 'budget-exhausted')).toHaveLength(1);
    // Nothing was removed on a timeout. A deadline is not a licence to guess.
    expect(rmMock).not.toHaveBeenCalled();
    expectStdoutUntouched();
  });

  test('cell 2: a slow size walk resolves PARTIAL work with budgetExhausted, rather than running to completion', async () => {
    // Six candidates; each candidate's own directory level costs ~60 ms to
    // list. The budget cannot cover all six, so a correct implementation stops
    // partway and REPORTS that it did. An implementation that only checks the
    // budget between candidates still passes this cell — cell 3 is what fails
    // that one, which is why both exist.
    const CANDIDATE_COUNT = 6;
    const LEVEL_COST_MS = 60;
    const FILE_BYTES = 1_000;

    const candidates = Array.from({ length: CANDIDATE_COUNT }, (_, index) => `cdk.out-slow-${index}`);

    readdirMock.mockImplementation((dirPath) => {
      if (dirPath === root) return Promise.resolve(candidates.map(direntDir));
      return settlesAfter(LEVEL_COST_MS, [direntFile('payload.bin')]);
    });
    statMock.mockImplementation((entryPath) => {
      if (entryPath.endsWith('payload.bin')) {
        return Promise.resolve({ size: FILE_BYTES, mtimeMs: 0, isDirectory: () => false, isFile: () => true });
      }
      return Promise.resolve({ size: 0, mtimeMs: 0, isDirectory: () => true, isFile: () => false });
    });

    const budgetMs = 200;
    const startedAt = Date.now();
    const result = await sweep({ budgetMs });
    const elapsedMs = Date.now() - startedAt;

    expect(elapsedMs).toBeLessThan(budgetMs + SETTLE_SLACK_MS);
    expect(result.budgetExhausted).toBe(true);
    // Partial: some real work landed, but not the whole backlog. Both bounds
    // matter — zero would mean the budget is so tight nothing is asserted, and
    // CANDIDATE_COUNT would mean the deadline never bit.
    expect(result.removedCount).toBeGreaterThan(0);
    expect(result.removedCount).toBeLessThan(CANDIDATE_COUNT);
    // BYTES ARE A LOWER BOUND, NEVER A FICTION. A candidate whose size walk the
    // budget cut short is still removed — leaving it in place livelocks the
    // queue, since the sweep is stateless across beats and would re-select the
    // same too-big candidate forever (see `SWEEP_REMOVAL_RESERVE_MS`). What is
    // preserved is the invariant that actually protects the AMBER loop:
    // `reclaimedBytes` counts only bytes this sweep genuinely measured, so it
    // can UNDER-report and can never over-report. `sizeUnmeasuredCount` names
    // exactly how many removals the shortfall can come from.
    const fullyMeasured = result.removedCount - result.sizeUnmeasuredCount;
    expect(result.sizeUnmeasuredCount).toBeLessThanOrEqual(result.removedCount);
    expect(result.reclaimedBytes).toBeGreaterThanOrEqual(fullyMeasured * FILE_BYTES);
    expect(result.reclaimedBytes).toBeLessThanOrEqual(result.removedCount * FILE_BYTES);
    expect(rmMock).toHaveBeenCalledTimes(result.removedCount);
    expectStdoutUntouched();
  });

  test('cell 3: a candidate on a HUNG filesystem is never removed — a deadline is not a licence to guess', async () => {
    // THE CELL THIS WHOLE FILE EXISTS FOR.
    //
    // `cdk.out-fast` measures instantly. Every read of `cdk.out-slow` HANGS —
    // the filesystem has gone quiet. The deadline lands with nothing at all
    // known about it, and it must be left exactly where it is.
    //
    // NOTE THE DISTINCTION this cell now draws, because the two were once
    // conflated into a single over-broad rule ("never remove what you could not
    // fully measure") that livelocked the queue: a candidate merely too LARGE
    // to finish measuring within the budget IS removed, with `reclaimedBytes`
    // degraded to a lower bound and `sizeUnmeasuredCount` saying so (cell 2). A
    // candidate on an UNRESPONSIVE path is a different situation — no reply
    // came back at all, no removal can even be issued — and it is still
    // untouched. `reclaimedBytes` is protected in both: it is only ever bytes
    // this sweep really measured.
    //
    // `cdk.out-fast` is made the OLDER of the two so oldest-first ordering puts
    // it first; the cell is about the abandoned one, not about ordering.
    const now = Date.now();
    const fastPath = join(root, 'cdk.out-fast');
    const slowPath = join(root, 'cdk.out-slow');
    const FAST_BYTES = 500;

    readdirMock.mockImplementation((dirPath) => {
      if (dirPath === root) return Promise.resolve([direntDir('cdk.out-fast'), direntDir('cdk.out-slow')]);
      if (dirPath === fastPath) return Promise.resolve([direntFile('fast.bin')]);
      if (dirPath === slowPath) return hangsForever();
      return Promise.resolve([]);
    });
    statMock.mockImplementation((entryPath) => {
      if (entryPath === fastPath) {
        return Promise.resolve({ size: 0, mtimeMs: now - 20 * HOUR_MS, isDirectory: () => true, isFile: () => false });
      }
      if (entryPath === slowPath) {
        return Promise.resolve({ size: 0, mtimeMs: now - 10 * HOUR_MS, isDirectory: () => true, isFile: () => false });
      }
      if (entryPath.endsWith('fast.bin')) {
        return Promise.resolve({ size: FAST_BYTES, mtimeMs: now - 20 * HOUR_MS, isDirectory: () => false, isFile: () => true });
      }
      return Promise.resolve({ size: 0, mtimeMs: now, isDirectory: () => false, isFile: () => true });
    });

    const budgetMs = 200;
    const result = await sweep({ budgetMs, now });

    expect(result.budgetExhausted).toBe(true);
    expect(rmMock).toHaveBeenCalledWith(fastPath, expect.objectContaining({ recursive: true, force: true }));
    // The assertion that a naive implementation fails.
    expect(rmMock).not.toHaveBeenCalledWith(slowPath, expect.anything());
    expect(result.removedCount).toBe(1);
    expect(result.reclaimedBytes).toBe(FAST_BYTES);
    expectStdoutUntouched();
  });

  test('cell 3b: a candidate too LARGE to finish measuring IS removed, with the shortfall named', async () => {
    // THE CELL THAT PINS FORWARD PROGRESS, deterministically and on its own.
    //
    // Cell 2 asserts the same property across a six-candidate backlog, but only
    // as a RANGE (`removedCount` somewhere between 1 and 5) — which a naive
    // implementation that abandons every budget-cut candidate still satisfies,
    // because some other candidate measured in time. This cell removes that
    // escape: ONE candidate, on a RESPONSIVE filesystem, whose size walk the
    // budget certainly cuts short. The old rule ("never remove what you could
    // not fully measure") produces `removedCount: 0` here and re-selects the
    // identical candidate on every subsequent beat, forever — a livelock on
    // exactly the large `bundling-temp-*`/`node_modules` trees exists to
    // reclaim.
    //
    // The contrast with cell 3 is the whole point and is load-bearing: there,
    // the filesystem never answered and nothing may be assumed; here, it
    // answers every time and is merely slow, so the candidate is removed and
    // `reclaimedBytes` degrades to a truthful LOWER BOUND with
    // `sizeUnmeasuredCount` stating how many removals the shortfall can come
    // from.
    //
    // TIMING, chosen so this is a fact rather than a race. The payload level
    // holds 24 files at ~25 ms per `stat`, i.e. ~600 ms per full traversal. The
    // liveness probe performs one such traversal (~600 ms); the size walk then
    // starts with ~300 ms before it must stop, so it measures roughly half the
    // files and cannot possibly finish. Its per-ENTRY budget check means it
    // stops within one `stat` (~25 ms) of the reserve line, leaving the bulk of
    // `SWEEP_REMOVAL_RESERVE_MS` for the `rm`.
    const now = Date.now();
    const candidatePath = join(root, 'bundling-temp-oversized');
    const payloadPath = join(candidatePath, 'payload');
    const FILE_COUNT = 24;
    const STAT_COST_MS = 25;
    const FILE_BYTES = 1_000;
    const payloadFiles = Array.from({ length: FILE_COUNT }, (_, index) => `chunk-${index}.js`);

    readdirMock.mockImplementation((dirPath) => {
      if (dirPath === root) return Promise.resolve([direntDir('bundling-temp-oversized')]);
      if (dirPath === candidatePath) return Promise.resolve([direntDir('payload')]);
      if (dirPath === payloadPath) return Promise.resolve(payloadFiles.map(direntFile));
      return Promise.resolve([]);
    });
    statMock.mockImplementation((entryPath) => {
      // Only the leaf payload files are slow. The candidate and the one
      // intermediate directory answer instantly, so the probe's cost is the
      // payload level and nothing else — which is what makes the arithmetic
      // above hold.
      if (entryPath.startsWith(payloadPath + '/')) {
        return settlesAfter(STAT_COST_MS, {
          size: FILE_BYTES,
          mtimeMs: now - 20 * HOUR_MS,
          isDirectory: () => false,
          isFile: () => true,
        });
      }
      return Promise.resolve({
        size: 0,
        mtimeMs: now - 20 * HOUR_MS,
        isDirectory: () => true,
        isFile: () => false,
      });
    });

    const budgetMs = 1_000;
    const startedAt = Date.now();
    const result = await sweep({ budgetMs, now });
    const elapsedMs = Date.now() - startedAt;

    expect(elapsedMs).toBeLessThan(budgetMs + SETTLE_SLACK_MS);
    // FORWARD PROGRESS: the candidate is gone, not left to be re-abandoned.
    expect(result.removedCount).toBe(1);
    expect(rmMock).toHaveBeenCalledWith(candidatePath, expect.objectContaining({ recursive: true, force: true }));
    // AND THE REPORT SAYS SO, rather than passing the partial figure off as
    // exact — the removal is counted, and counted as unmeasured.
    expect(result.sizeUnmeasuredCount).toBe(1);
    // A LOWER BOUND, NEVER A FICTION: some bytes really were measured, and
    // strictly fewer than the tree holds, because the walk never finished.
    expect(result.reclaimedBytes).toBeGreaterThan(0);
    expect(result.reclaimedBytes).toBeLessThan(FILE_COUNT * FILE_BYTES);
    expectStdoutUntouched();
  });

  test('cell 3c: an `rm` still in flight when the budget expires is reported as ISSUED-UNCONFIRMED, not as zero', async () => {
    // THE CELL THAT PINS `removalIssuedUnconfirmedCount`, the axis added once
    // the comment at the `rm` race was found to be factually false. The `rm` is
    // CALLED first and raced second; `fsPromises.rm` takes no `AbortSignal`, so
    // the race bounds how long the sweep WAITS, never how long the kernel takes
    // to finish the delete. A candidate in that state has very probably been
    // removed — which is neither a removal this function may claim nor a bare
    // `0` it may report, because a `0` sends the AMBER loop re-sweeping for
    // pressure that is already draining away.
    //
    // Everything except the `rm` answers INSTANTLY, so each neighbouring axis
    // is pinned for its own reason rather than by accident: the size walk ran
    // to completion (`sizeUnmeasuredCount === 0` — that axis describes
    // CONFIRMED removals whose walk was cut short, a different set), no removal
    // was ever confirmed (`removedCount === 0`), and nothing was counted
    // reclaimed (`reclaimedBytes === 0` — only a completed removal contributes).
    const now = Date.now();
    const candidatePath = join(root, 'bundling-temp-unconfirmed');

    readdirMock.mockImplementation((dirPath) => {
      if (dirPath === root) return Promise.resolve([direntDir('bundling-temp-unconfirmed')]);
      return Promise.resolve([]);
    });
    statMock.mockImplementation(() =>
      Promise.resolve({
        size: 0,
        mtimeMs: now - 20 * HOUR_MS,
        isDirectory: () => true,
        isFile: () => false,
      }),
    );
    // The only slow step in the cell, and it never answers at all — the honest
    // expression of "the unlink is still walking a huge tree".
    rmMock.mockImplementation(() => hangsForever());

    const budgetMs = 400;
    const startedAt = Date.now();
    const result = await sweep({ budgetMs, now });
    const elapsedMs = Date.now() - startedAt;

    // The sweep settles on its own deadline despite the un-abortable `rm`.
    expect(elapsedMs).toBeLessThan(budgetMs + SETTLE_SLACK_MS);
    expect(rmMock).toHaveBeenCalledWith(candidatePath, expect.objectContaining({ recursive: true, force: true }));
    expect(result.removalIssuedUnconfirmedCount).toBe(1);
    expect(result.removedCount).toBe(0);
    expect(result.reclaimedBytes).toBe(0);
    expect(result.sizeUnmeasuredCount).toBe(0);
    expect(result.budgetExhausted).toBe(true);
    expectStdoutUntouched();
  });

  test('cell 4: `budgetMs <= 0` resolves immediately with zero removals and never touches `rm`', async () => {
    // A zero/negative budget is not a degenerate input to be normalised away —
    // it is the caller saying "there is no time". A sweep that helpfully swept
    // "just one" would be doing unbudgeted destructive work on a hot beat path.
    readdirMock.mockResolvedValue([direntDir('cdk.out-eligible')]);
    statMock.mockResolvedValue({ size: 0, mtimeMs: 0, isDirectory: () => true, isFile: () => false });

    for (const budgetMs of [0, -1, -SWEEP_BUDGET_MS]) {
      rmMock.mockClear();
      const startedAt = Date.now();
      const result = await sweep({ budgetMs });

      expect(Date.now() - startedAt).toBeLessThan(SETTLE_SLACK_MS);
      expect(result.removedCount).toBe(0);
      expect(result.reclaimedBytes).toBe(0);
      expect(result.budgetExhausted).toBe(true);
      expect(rmMock).not.toHaveBeenCalled();
    }
    expectStdoutUntouched();
  });

  test('cell 5: the default `budgetMs` is `SWEEP_BUDGET_MS` — asserted against the real constant, never a literal', async () => {
    // Omitting `budgetMs` must not mean "unbounded". Pinned against the
    // exported constant so this cell cannot drift from the value the hook path
    // actually runs under.
    expect(Number.isFinite(SWEEP_BUDGET_MS)).toBe(true);
    expect(SWEEP_BUDGET_MS).toBeGreaterThan(0);

    readdirMock.mockImplementation(() => hangsForever());

    const startedAt = Date.now();
    const result = await sweep();
    const elapsedMs = Date.now() - startedAt;

    expect(result.budgetExhausted).toBe(true);
    expect(elapsedMs).toBeLessThan(SWEEP_BUDGET_MS + SETTLE_SLACK_MS);
    expectStdoutUntouched();
  });
});

describe('SENTINEL — these cells can never be pointed at the real host temp root', () => {
  test('the process`s real os.tmpdir() is refused by this file`s own allowlist, with zero removals', async () => {
    // If a future edit widened `allowedPrefixes`, or dropped it and let the
    // platform list govern, this cell is what fails first. The host `$TMPDIR`
    // holds 3,000+ real `cdk.out` dirs (see `./cleanup.jest.spec.mjs`); a mocked
    // `rm` protects this file today, but nothing guarantees the next edit keeps
    // the mock.
    readdirMock.mockResolvedValue([direntDir('cdk.out-would-be-swept')]);

    const result = await sweepStaleTempDirsAsync({
      dir: tmpdir(),
      ageThresholdMs: 2 * HOUR_MS,
      now: Date.now(),
      allowedPrefixes: [root],
    });

    expect(result.removedCount).toBe(0);
    expect(result.reclaimedBytes).toBe(0);
    expect(result.errors[0].kind).toBe('confinement-refused');
    expect(rmMock).not.toHaveBeenCalled();
    expect(readdirMock).not.toHaveBeenCalled();
    expectStdoutUntouched();
  });
});
