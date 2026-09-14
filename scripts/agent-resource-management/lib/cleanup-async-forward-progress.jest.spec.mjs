// pre-PR review (High #4 and #5) — FORWARD PROGRESS. Two independent
// livelocks, each of which made the sweep unable to reclaim anything on ANY
// beat, forever.
//
// ---------------------------------------------------------------------------
// WHY "IT STAYED INSIDE ITS BUDGET" IS NOT THE SAME CLAIM AS "IT MAKES PROGRESS"
// ---------------------------------------------------------------------------
//
// `cleanup-async-deadline-budget.jest.spec.mjs` pins the ceiling: whatever the
// filesystem does, the sweep settles inside `budgetMs`. Every cell below is
// inside that ceiling too — and every one of them reclaimed NOTHING, on every
// beat, for a reason the ceiling tests cannot see. The sweep is STATELESS
// across beats and sorts oldest-first, so an expensive step that always runs
// first, and always aborts everything behind it, is not a slow sweep: it is a
// sweep that can never reach the work.
//
//   * LIVELOCK A (High #4) — `probeLiveWriter` had no removal reserve and, on
//     expiry, the caller `break`-ed the WHOLE candidate loop. One big tree at
//     the front of the queue therefore consumed the budget to the last
//     millisecond and cancelled every candidate behind it, including the cheap
//     ones that need no probe at all.
//
//   * LIVELOCK B (High #5) — the listing stage `stat`-ed EVERY dirent in the
//     sweep root before `selectStaleTempDirs` ever applied the name pattern,
//     and threw the whole gathered list away on expiry. `readdir` order is
//     stable, so a root whose unrelated entries cost more than the budget could
//     never reach a real candidate — not on this beat, not on any later one.
//
// ---------------------------------------------------------------------------
// WHY THE `fs` SURFACE IS MOCKED HERE
// ---------------------------------------------------------------------------
//
// Both properties are about WHERE TIME GOES, and a test that tried to make a
// real filesystem slow enough to expire a budget would be a flake generator on
// a fast SSD and a false green on a slow one. The mock makes each syscall's
// cost an explicit, stated number, so the cells assert the control-flow
// property rather than the host's disk. The confinement anchor still runs
// against the REAL synchronous `node:fs` — it is not mocked — so the scratch
// root is a genuinely resolved, genuinely allowed path.

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

const { sweepStaleTempDirsAsync } = await import('./cleanup.mjs');

const HOUR_MS = 60 * 60 * 1000;
/** Comfortably older than both the staleness threshold and `LIVE_WRITE_MARGIN_MS`. */
const OLD_MS = 10 * HOUR_MS;
/** The stated per-syscall cost. Large enough that a few hundred of them cannot fit in a cell's budget. */
const SLOW_STAT_MS = 5;

function direntDir(name) {
  return { name, isDirectory: () => true, isFile: () => false, isSymbolicLink: () => false };
}

function direntFile(name) {
  return { name, isDirectory: () => false, isFile: () => true, isSymbolicLink: () => false };
}

function settlesAfter(ms, value) {
  return new Promise((resolveSettled) => {
    setTimeout(() => resolveSettled(value), ms);
  });
}

let scratchRoot;
let root;
let now;
let stdoutSpy;
let stderrSpy;

beforeEach(() => {
  scratchRoot = mkdtempSync(join(tmpdir(), 'arm-forward-progress-'));
  root = realpathSync(scratchRoot);
  now = Date.now();

  readdirMock.mockReset();
  statMock.mockReset();
  lstatMock.mockReset();
  rmMock.mockReset();
  rmMock.mockResolvedValue(undefined);

  stdoutSpy = jest.spyOn(process.stdout, 'write').mockImplementation(() => true);
  stderrSpy = jest.spyOn(process.stderr, 'write').mockImplementation(() => true);
});

afterEach(() => {
  stdoutSpy.mockRestore();
  stderrSpy.mockRestore();
  rmSync(scratchRoot, { recursive: true, force: true });
});

function sweep(overrides = {}) {
  return sweepStaleTempDirsAsync({
    dir: root,
    ageThresholdMs: 2 * HOUR_MS,
    now,
    allowedPrefixes: [root],
    ...overrides,
  });
}

/** Every path the mocked `rm` was asked to remove. */
function removedPaths() {
  return rmMock.mock.calls.map(([path]) => path);
}

// ===========================================================================
// LIVELOCK A — an expired liveness probe is a verdict about ONE candidate.
// ===========================================================================

describe('High #4 — an expired probe skips its own candidate, not the whole queue', () => {
  /**
   * Two candidates, oldest first: a DIRECTORY whose probe cannot finish inside
   * the budget, and behind it a stale plain FILE — which needs no probe at all
   * and costs one `stat` plus one `rm`.
   */
  function seedBigDirThenCheapFile() {
    const bigDir = join(root, 'cdk.out-big');
    const cheapFile = join(root, 'bundling-temp-leftover');
    // 400 children x 5ms = 2s of probe work; no cell here has that much budget.
    const bigDirChildren = Array.from({ length: 400 }, (_unused, index) => direntFile(`child-${index}`));

    readdirMock.mockImplementation((dirPath) => {
      if (dirPath === root) return Promise.resolve([direntDir('cdk.out-big'), direntFile('bundling-temp-leftover')]);
      if (dirPath === bigDir) return Promise.resolve(bigDirChildren);
      return Promise.resolve([]);
    });

    statMock.mockImplementation((entryPath) => {
      // The two top-level candidates are cheap to list; the big directory's
      // CHILDREN are what the probe cannot get through.
      if (entryPath === bigDir) return Promise.resolve({ mtimeMs: now - OLD_MS - 1000, size: 0 });
      if (entryPath === cheapFile) return Promise.resolve({ mtimeMs: now - OLD_MS, size: 4096 });
      return settlesAfter(SLOW_STAT_MS, { mtimeMs: now - OLD_MS, size: 1 });
    });

    return { bigDir, cheapFile };
  }

  test('the cheap candidate BEHIND the un-probeable one is still reclaimed', async () => {
    const { bigDir, cheapFile } = seedBigDirThenCheapFile();

    const report = await sweep({ budgetMs: 400 });

    // THE PROPERTY. `break` left this at 0 — the probe expired on the first
    // candidate and cancelled the second, which needed no probe and ~no time.
    expect(report.removedCount).toBe(1);
    expect(removedPaths()).toEqual([cheapFile]);

    // The un-probeable tree is DEFERRED, not deleted. "I could not establish
    // that nothing is writing into it" must never resolve to a removal.
    expect(removedPaths()).not.toContain(bigDir);

    // And the beat still reports honestly that it stopped short, so the
    // cooldown will not stand the sweep down.
    expect(report.budgetExhausted).toBe(true);
    expect(report.errors.map((error) => error.kind)).toContain('budget-exhausted');
  });

  test('the exhaustion is recorded ONCE, not once per skipped candidate', async () => {
    seedBigDirThenCheapFile();

    const report = await sweep({ budgetMs: 400 });

    expect(report.errors.filter((error) => error.kind === 'budget-exhausted')).toHaveLength(1);
  });

  test('the probe leaves removal headroom on the clock rather than spending the budget whole', async () => {
    // The reserve is what makes the `continue` above buy anything: without it
    // the probe runs to the deadline and the candidate behind it has no clock
    // left either. Observable as the `rm` that still happened.
    seedBigDirThenCheapFile();

    const startedAt = Date.now();
    const budgetMs = 400;
    const report = await sweep({ budgetMs });

    expect(rmMock).toHaveBeenCalledTimes(1);
    expect(report.removedCount).toBe(1);
    // Still inside the ceiling the budget spec pins — progress was bought from
    // the reserve, not by overrunning.
    expect(Date.now() - startedAt).toBeLessThan(budgetMs + 2_500);
  });
});

// ===========================================================================
// LIVELOCK B — the listing stage must not pay for entries it will discard.
// ===========================================================================

describe('High #5 — the name pattern is applied BEFORE the stat that would pay for it', () => {
  /**
   * A sweep root whose `readdir` order puts many unrelated entries ahead of the
   * only real candidate — the ordinary shape of a busy `$TMPDIR`, and the one
   * `readdir`'s stable ordering makes reproducible on every beat.
   */
  function seedNoisyRootThenCandidate(noiseCount = 400) {
    const candidate = join(root, 'cdk.out-real');
    const noise = Array.from({ length: noiseCount }, (_unused, index) => direntFile(`unrelated-${index}`));

    readdirMock.mockImplementation((dirPath) => {
      if (dirPath === root) return Promise.resolve([...noise, direntDir('cdk.out-real')]);
      return Promise.resolve([]);
    });

    statMock.mockImplementation((entryPath) => {
      if (entryPath === candidate) return Promise.resolve({ mtimeMs: now - OLD_MS, size: 0 });
      // Every unrelated entry costs a real syscall — IF the listing stats it.
      return settlesAfter(SLOW_STAT_MS, { mtimeMs: now - OLD_MS, size: 1 });
    });

    return { candidate };
  }

  test('a root whose unrelated entries out-cost the budget still reaches its candidate', async () => {
    const { candidate } = seedNoisyRootThenCandidate();

    // 400 unrelated entries x 5ms = 2s of stat work the listing used to pay
    // for before the name filter ever ran. The budget is a tenth of that.
    const report = await sweep({ budgetMs: 200 });

    expect(report.removedCount).toBe(1);
    expect(removedPaths()).toEqual([candidate]);
    expect(report.budgetExhausted).toBe(false);
    expect(report.completed).toBe(true);
  });

  test('no syscall is spent on an entry the name pattern would discard', async () => {
    seedNoisyRootThenCandidate();

    await sweep({ budgetMs: 200 });

    const statted = statMock.mock.calls.map(([path]) => path);
    expect(statted.filter((path) => path.includes('unrelated-'))).toEqual([]);
  });

  test('a listing that DOES expire hands back what it found instead of discarding it', async () => {
    // Enough matching candidates that the listing itself cannot finish, each
    // costing a stat. The partial list must still be worked: throwing it away
    // reported `removedCount: 0` for a beat that had already paid to find real
    // candidates, and the next beat paid exactly the same cost to reach exactly
    // the same point.
    //
    // PLAIN FILES, deliberately. They are the candidate kind that needs no
    // liveness probe, so they are what the listing's own removal reserve is
    // there to leave room for. A DIRECTORY candidate reached in that reserve is
    // correctly deferred instead — see the disclosed residual on
    // `probeLiveWriter`'s expiry branch — so using directories here would be
    // asserting the opposite property from the one this cell names.
    const candidates = Array.from({ length: 400 }, (_unused, index) => direntFile(`bundling-temp-${index}`));

    readdirMock.mockImplementation((dirPath) => {
      if (dirPath === root) return Promise.resolve(candidates);
      return Promise.resolve([]);
    });
    statMock.mockImplementation(() => settlesAfter(SLOW_STAT_MS, { mtimeMs: now - OLD_MS, size: 1 }));

    const report = await sweep({ budgetMs: 300 });

    expect(report.budgetExhausted).toBe(true);
    // THE PROPERTY: a partial listing is a lower bound the beat can act on,
    // not a reason to report nothing.
    expect(report.removedCount).toBeGreaterThan(0);
  });
});
