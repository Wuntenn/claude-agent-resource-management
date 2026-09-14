// Phase 2 (review round) — REFUSALS, ATTRIBUTION, and TOTALITY.
//
// ---------------------------------------------------------------------------
// THE PROPERTY THIS FILE PROTECTS
// ---------------------------------------------------------------------------
//
// `sweepStaleTempDirsAsync` sits on the PreToolUse hook's beat path. The hook
// `JSON.parse`s the whole of `cli.mjs`'s stdout and fails CLOSED on anything it
// cannot parse, so the two ways this function can hurt the host are
// not "wrong answer" and "slow":
//
//   1. IT REJECTS. An unhandled rejection prints a raw stack trace onto that
//      stdout stream and denies EVERY agent spawn on the host. The function's
//      doc comment promises it "NEVER REJECTS, for any input" — a promise the
//      cells below actually try to break, rather than restate.
//   2. IT ANSWERS "all clear" WHEN IT DID NOTHING. `{ removedCount: 0,
//      errors: [] }` is what a genuinely healthy, already-clean temp root
//      returns. Any refusal that produces the SAME document is a silent
//      control-loop failure: Phase 3's AMBER response reads it as "nothing to
//      reclaim" and stops asking, with no operator-visible symptom at all.
//      Every refusal here must therefore be DISTINGUISHABLE.
//
// The third property is attribution: `budgetExhausted` and `candidatesCapped`
// are the flags Phase 3 uses to decide whether to re-arm the next beat, so
// "why did this beat stop" must be answered correctly and not merely
// plausibly.
//
// ---------------------------------------------------------------------------
// WHY A NAIVE IMPLEMENTATION PASSES A WEAKER TEST
// ---------------------------------------------------------------------------
//
// Every cell here is built against an implementation that looks right:
//
//   * `maxCandidates: 0` — a `Number.isFinite(x) && x > 0 ? x : DEFAULT`
//     normalisation reads as defensive and turns "do nothing" into "do up to
//     24" on a RECURSIVE-REMOVAL primitive. A test that only checks the happy
//     cap never sees it.
//   * an invalid `now` — it is passed straight into the age comparison, where
//     `NaN > threshold` is false for every entry. The sweep reports a perfect
//     clean bill of health. A test asserting "no crash" passes.
//   * a hostile error value — `String(value)` is total for every value a test
//     author naturally writes, and throws on the ones a native binding or a
//     mocked surface actually produces.
//   * `candidatesCapped` set eagerly from `selected.length > cap` BEFORE the
//     loop — indistinguishable from correct on every cell that does not also
//     exhaust the budget.
//
// ---------------------------------------------------------------------------
// MOCKING IDIOM
// ---------------------------------------------------------------------------
//
// `jest.unstable_mockModule('node:fs/promises', ...)` — this directory's house
// idiom (see `./cleanup-async-deadline-budget.jest.spec.mjs`). `node:fs` stays
// REAL, because `classifySweepRoot` resolves the sweep root with the real
// `realpathSync` and the root must genuinely exist for confinement to pass.
//
// SAFETY: `rm` is mocked in every cell, so nothing here can delete anything.
// The scratch root is a real `fs.mkdtempSync` directory, nominated explicitly
// through `allowedPrefixes` and torn down in `afterEach`; the sentinel cell at
// the end pins that the process's real `$TMPDIR` is refused.

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

function direntDir(name) {
  return { name, isDirectory: () => true, isFile: () => false, isSymbolicLink: () => false };
}

let scratchRoot;
let root;
let stdoutSpy;
let stderrSpy;

beforeEach(() => {
  scratchRoot = mkdtempSync(join(tmpdir(), 'arm-refusals-'));
  root = realpathSync(scratchRoot);

  readdirMock.mockReset();
  statMock.mockReset();
  lstatMock.mockReset();
  rmMock.mockReset();
  rmMock.mockResolvedValue(undefined);

  // A genuine write would corrupt Jest's own reporter stream, so a violation is
  // silenced AND recorded, never echoed.
  stdoutSpy = jest.spyOn(process.stdout, 'write').mockImplementation(() => true);
  stderrSpy = jest.spyOn(process.stderr, 'write').mockImplementation(() => true);
});

afterEach(() => {
  stdoutSpy.mockRestore();
  stderrSpy.mockRestore();
  rmSync(scratchRoot, { recursive: true, force: true });
});

/** Seeds `count` stale candidate directories at the sweep root, all eligible. */
function seedBacklog(count, now) {
  const names = Array.from({ length: count }, (_, index) => `cdk.out-${index}`);
  readdirMock.mockImplementation((dirPath) => {
    if (dirPath === root) return Promise.resolve(names.map(direntDir));
    return Promise.resolve([]);
  });
  statMock.mockImplementation(() =>
    Promise.resolve({ size: 0, mtimeMs: now - 20 * HOUR_MS, isDirectory: () => true, isFile: () => false }),
  );
  return names;
}

function sweep(overrides = {}) {
  return sweepStaleTempDirsAsync({
    dir: root,
    ageThresholdMs: 2 * HOUR_MS,
    now: Date.now(),
    budgetMs: 5_000,
    allowedPrefixes: [root],
    ...overrides,
  });
}

describe('an explicit "do no work" is honoured, never upgraded to the default', () => {
  test('`maxCandidates: 0` removes nothing and never touches `rm`', async () => {
    // The finding this cell exists for: `0` fell through a
    // `Number.isFinite(x) && x > 0 ? x : SWEEP_MAX_CANDIDATES` normalisation
    // and became the DEFAULT cap. On a recursive-removal primitive, silently
    // turning "remove nothing" into "remove up to 24" is the worst possible
    // direction for a misread argument to fail in.
    const now = Date.now();
    seedBacklog(SWEEP_MAX_CANDIDATES + 5, now);

    const result = await sweep({ now, maxCandidates: 0 });

    expect(result.removedCount).toBe(0);
    expect(rmMock).not.toHaveBeenCalled();
    // Nothing FAILED, so nothing is recorded as an error — but the stop reason
    // is still stated, so this beat is not confusable with a clean sweep of an
    // empty root.
    expect(result.errors).toEqual([]);
    expect(result.candidatesCapped).toBe(true);
    expect(stdoutSpy).not.toHaveBeenCalled();
  });

  test('a negative `maxCandidates` is refused on the same footing, not treated as an absolute value', async () => {
    const now = Date.now();
    seedBacklog(4, now);

    const result = await sweep({ now, maxCandidates: -3 });

    expect(result.removedCount).toBe(0);
    expect(rmMock).not.toHaveBeenCalled();
  });

  test('a non-finite `maxCandidates` is refused rather than silently becoming the default', async () => {
    const now = Date.now();
    seedBacklog(4, now);

    for (const value of [Number.NaN, Number.POSITIVE_INFINITY, 'six']) {
      rmMock.mockClear();
      const result = await sweep({ now, maxCandidates: value });
      expect(result.removedCount).toBe(0);
      expect(rmMock).not.toHaveBeenCalled();
    }
  });

  test.each([
    ['NaN', Number.NaN],
    ['Infinity', Number.POSITIVE_INFINITY],
    ['a numeric string', 'six'],
    ['null', null],
    // A FRACTION IN `(0, 1)` is the case that slipped past the first version of
    // this gate: finite, greater than zero, and then `Math.floor(0.5) === 0`,
    // so the cap silently became zero and the report was bit-identical to a
    // healthy fully-capped sweep — the exact ambiguity the split was built to
    // eliminate. A cap counts directories; half a directory is a bug.
    ['a fraction below one', 0.5],
  ])(
    'an INVALID `maxCandidates` (%s) is distinguishable from a healthy capped sweep, under `invalid-max-candidates`',
    async (_label, value) => {
      // The distinction this cell draws, and why it is not pedantry. A numeric
      // `maxCandidates <= 0` is the caller SAYING "do no work" — nothing went
      // wrong, so nothing is recorded and `candidatesCapped` carries the whole
      // story. A non-number / `NaN` / `Infinity` is a caller BUG, and if it
      // produces the identical document (`candidatesCapped: true, errors: []`)
      // then it is also indistinguishable from a genuinely healthy sweep that
      // drained its 24 slots and has more to do — precisely the
      // silent-control-loop-failure shape the `invalid-now` gate exists to
      // prevent. Same principle, same treatment.
      const now = Date.now();
      seedBacklog(4, now);

      const result = await sweep({ now, maxCandidates: value });

      expect(result.removedCount).toBe(0);
      expect(rmMock).not.toHaveBeenCalled();
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0].kind).toBe('invalid-max-candidates');
      expect(stdoutSpy).not.toHaveBeenCalled();
    },
  );

  test.each([
    ['zero', 0],
    ['a negative', -3],
  ])('a NUMERIC non-positive `maxCandidates` (%s) stays a silent, error-free refusal', async (_label, value) => {
    // The other half of the split: this one really is an explicit caller
    // statement, not a bug, and recording an error for it would page an
    // operator about a beat that did exactly what it was told.
    const now = Date.now();
    seedBacklog(4, now);

    const result = await sweep({ now, maxCandidates: value });

    expect(result.removedCount).toBe(0);
    expect(result.errors).toEqual([]);
    expect(result.candidatesCapped).toBe(true);
  });

  test('omitting `maxCandidates` still applies the default — the refusal has not eaten the happy path', async () => {
    const now = Date.now();
    seedBacklog(3, now);

    const result = await sweep({ now });

    expect(result.removedCount).toBe(3);
  });
});

describe('`now` is validated, because an invalid one is INVISIBLE in the report', () => {
  // `now` is half of every staleness comparison. An `undefined`/`NaN`/string
  // value makes `now - mtimeMs > ageThresholdMs` false for every entry, so the
  // sweep selects nothing and returns a report bit-identical to a healthy,
  // already-clean root. The AMBER loop reads that as "nothing to reclaim" and
  // stops retrying — a caller bug that presents as good news.
  test.each([
    ['undefined', undefined],
    ['NaN', Number.NaN],
    ['a numeric string', '1700000000000'],
    ['Infinity', Number.POSITIVE_INFINITY],
    ['null', null],
  ])('%s is refused under a distinct `invalid-now` kind, not silently swept', async (_label, value) => {
    seedBacklog(3, Date.now());

    const result = await sweep({ now: value });

    expect(result.removedCount).toBe(0);
    expect(rmMock).not.toHaveBeenCalled();
    // DISTINGUISHABLE — the whole point. A bare `{removedCount: 0, errors: []}`
    // would be the failure this cell exists to prevent.
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].kind).toBe('invalid-now');
    expect(stdoutSpy).not.toHaveBeenCalled();
  });

  test('a valid `now` is not caught by the guard — the happy path still sweeps', async () => {
    const now = Date.now();
    seedBacklog(2, now);

    const result = await sweep({ now });

    expect(result.removedCount).toBe(2);
    expect(result.errors).toEqual([]);
  });
});

describe('`now` must also be PLAUSIBLE — finite is not the same as sane', () => {
  // ---------------------------------------------------------------------------
  // THE HAZARD, stated once, because it is the worst one in this module.
  // ---------------------------------------------------------------------------
  //
  // `now` is not merely an input to a comparison: it is the sole reference
  // point for BOTH safety gates on a recursive-removal primitive, and a single
  // skewed value defeats them TOGETHER, in the same direction.
  //
  //   * `selectStaleTempDirs` asks `now - mtimeMs > ageThresholdMs`. Skew `now`
  //     forward and EVERY entry is stale — including one created a second ago.
  //   * `probeLiveWriter` asks `now - mtimeMs < LIVE_WRITE_MARGIN_MS`. Skew
  //     `now` forward by more than that margin and a descendant written
  //     MOMENTS AGO reads as ancient, so the probe reports "no live writer".
  //
  // The combined result is `rm -rf` across the whole temp root — actively
  // written `bundling-temp-*` build trees included — reported back to the AMBER
  // loop as a clean, successful, entirely unremarkable sweep. The inputs that
  // produce it are ordinary CALLER-SIDE bugs: a seconds-vs-milliseconds unit
  // slip, `Date.now() * 1000`, a stale monotonic reading, a wrong variable or a
  // long-cached timestamp handed in as `now`.
  //
  // A finiteness check cannot see any of this. Only a comparison against the
  // real clock can, which is why the band is measured against `Date.now()`
  // inside the sweep rather than trusted from the argument.
  //
  // What the band does NOT catch, and these cells therefore do not claim: a
  // HOST-WIDE clock change (an NTP step, a manual `date` set, a VM resume)
  // moves the caller's `now` and the sweep's own `Date.now()` together, so the
  // skew stays ~0 and the gate passes. That residual is accepted and disclosed
  // on `SWEEP_NOW_SKEW_TOLERANCE_MS`, not closed here.

  test.each([
    ['seconds mistaken for milliseconds (× 1000)', () => Date.now() * 1000],
    ['a caller-side unit slip of a full day', () => Date.now() + 24 * HOUR_MS],
    ['a backward skew of a full day', () => Date.now() - 24 * HOUR_MS],
  ])('a `now` skewed by %s is refused under `now-out-of-band`, with nothing removed', async (_label, makeNow) => {
    const skewed = makeNow();
    // Seeded against the REAL clock, so every candidate is only ~20 h old:
    // genuinely stale, but the removal must be refused on the skew, not
    // performed on the strength of a clock nobody can vouch for.
    seedBacklog(4, Date.now());

    const result = await sweep({ now: skewed });

    expect(result.removedCount).toBe(0);
    expect(rmMock).not.toHaveBeenCalled();
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].kind).toBe('now-out-of-band');
    expect(stdoutSpy).not.toHaveBeenCalled();
  });

  test.each([
    ['exactly the real clock', 0],
    ['a second of ordinary drift', 1_000],
    ['a second behind', -1_000],
  ])('ORDINARY drift (%s) is NOT refused — the band is a safety wall, not a tripwire', async (_label, offsetMs) => {
    // The other half of the gate, and the half that keeps it honest. A band
    // tight enough to reject the caller's own `Date.now()` plus a scheduling
    // delay would turn every busy beat into a refusal, and the AMBER loop would
    // stop reclaiming anything at all. Small offsets must sweep normally.
    const now = Date.now() + offsetMs;
    seedBacklog(2, now);

    const result = await sweep({ now });

    expect(result.removedCount).toBe(2);
    expect(result.errors).toEqual([]);
  });
});

describe('the "never rejects" contract, actually attacked', () => {
  test('a synchronously THROWING fs surface yields a `sweep-aborted` report, not a rejection', async () => {
    // A mocked or native surface that throws instead of rejecting bypasses
    // every per-step `settleOutcome` guard, because the throw happens before
    // the guard is entered. Without the function-level `catch` this is an
    // unhandled rejection on the hook's stdout — i.e. a host-wide spawn denial.
    readdirMock.mockImplementation(() => {
      throw new Error('EIO: synchronous explosion from the fs surface');
    });

    const result = await sweep();

    expect(result.removedCount).toBe(0);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].kind).toBe('sweep-aborted');
    expect(typeof result.errors[0].message).toBe('string');
    expect(stdoutSpy).not.toHaveBeenCalled();
  });

  test('a rejection carrying an UNSTRINGIFIABLE value is recorded, not re-thrown', async () => {
    // `String(value)` — the obvious implementation of a "bound this field"
    // helper — throws on an object with a throwing `toString`, on a
    // null-prototype object, and on a bare Symbol. All three are reachable: the
    // recorded value comes from `error.message ?? error` of a REJECTED fs
    // promise, and a native binding can reject with anything.
    const now = Date.now();
    seedBacklog(1, now);

    const hostile = Object.create(null);
    Object.defineProperty(hostile, 'message', {
      get() {
        throw new Error('nice try');
      },
    });
    rmMock.mockRejectedValue(hostile);

    const result = await sweep({ now });

    expect(result.removedCount).toBe(0);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].kind).toBe('removal-failed');
    expect(typeof result.errors[0].message).toBe('string');
    expect(stdoutSpy).not.toHaveBeenCalled();
  });

  test('a `dir` that cannot be stringified does not throw on the way to its own refusal', async () => {
    const unstringifiable = Object.create(null);

    const result = await sweepStaleTempDirsAsync({
      dir: unstringifiable,
      ageThresholdMs: Number.NaN,
      now: Date.now(),
      allowedPrefixes: [root],
    });

    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].kind).toBe('invalid-age-threshold');
    expect(typeof result.errors[0].path).toBe('string');
  });
});

describe('the stop reason is attributed to whatever actually ended the beat', () => {
  test('a backlog larger than the cap that drains its slots reports `candidatesCapped`', async () => {
    const now = Date.now();
    seedBacklog(10, now);

    const result = await sweep({ now, maxCandidates: 4 });

    expect(result.removedCount).toBe(4);
    expect(result.candidatesCapped).toBe(true);
    expect(result.budgetExhausted).toBe(false);
  });

  test('a beat that dies on the BUDGET does not claim it was capped, even with a capped-size backlog', async () => {
    // The finding this cell exists for. `candidatesCapped` was set eagerly from
    // `selected.length > cap` BEFORE the loop ran, so a sweep that selected 24
    // and then expired at candidate 1 reported `candidatesCapped: true` —
    // indistinguishable from one that genuinely drained all 24 slots. Phase 3
    // re-arms the next beat off these flags, so a wrong stop reason is handed
    // straight to the component whose only job is reacting to it.
    const now = Date.now();
    const names = Array.from({ length: 30 }, (_, index) => `cdk.out-${index}`);
    readdirMock.mockImplementation((dirPath) => {
      if (dirPath === root) return Promise.resolve(names.map(direntDir));
      // Every candidate's own probe hangs: the budget, not the cap, is what
      // ends this beat.
      return new Promise(() => {});
    });
    statMock.mockImplementation(() =>
      Promise.resolve({ size: 0, mtimeMs: now - 20 * HOUR_MS, isDirectory: () => true, isFile: () => false }),
    );

    const result = await sweep({ now, budgetMs: 150, maxCandidates: 4 });

    expect(result.budgetExhausted).toBe(true);
    expect(result.candidatesCapped).toBe(false);
    expect(result.removedCount).toBe(0);
  });
});

describe('liveness SKIPS do not crowd genuine failures out of the bounded error ring', () => {
  test('many live candidates produce ONE ring entry and an exact count', async () => {
    // The finding: `live-writer-suspected` shared the 8-slot `SWEEP_MAX_ERRORS`
    // ring with real failures, even though the sweep's own doc calls a skip
    // "not an alarm". A busy host with several live candidates per beat filled
    // the ring with non-events and pushed genuine `removal-failed` entries into
    // the anonymous `truncatedCount` — the bound meant to protect the
    // operator's signal destroying it instead.
    const now = Date.now();
    const liveCount = SWEEP_MAX_ERRORS + 4;
    const names = Array.from({ length: liveCount }, (_, index) => `cdk.out-live-${index}`);

    readdirMock.mockImplementation((dirPath) => {
      if (dirPath === root) return Promise.resolve(names.map(direntDir));
      return Promise.resolve([direntDir('nested')]);
    });
    statMock.mockImplementation((entryPath) => {
      // The candidate roots are stale (so they are selected); every child is
      // fresh (so every one of them is skipped as live).
      const isCandidateRoot = names.some((name) => entryPath === join(root, name));
      return Promise.resolve({
        size: 0,
        mtimeMs: isCandidateRoot ? now - 20 * HOUR_MS : now,
        isDirectory: () => true,
        isFile: () => false,
      });
    });

    const result = await sweep({ now, maxCandidates: liveCount });

    expect(result.removedCount).toBe(0);
    expect(rmMock).not.toHaveBeenCalled();
    // The magnitude lives on its own counter...
    expect(result.liveWriterSkippedCount).toBe(liveCount);
    // ...and costs the ring exactly one slot, so seven remain for real
    // failures rather than none.
    expect(result.errors.filter((error) => error.kind === 'live-writer-suspected')).toHaveLength(1);
    expect(result.truncatedCount).toBe(0);
  });
});

describe('SENTINEL — these cells can never be pointed at the real host temp root', () => {
  test('the process`s real os.tmpdir() is refused by this file`s own allowlist, with zero removals', async () => {
    const result = await sweep({ dir: tmpdir() });

    expect(result.removedCount).toBe(0);
    expect(result.errors[0].kind).toBe('confinement-refused');
    expect(rmMock).not.toHaveBeenCalled();
  });
});
