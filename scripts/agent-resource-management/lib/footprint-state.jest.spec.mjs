// Unit tests for `lib/footprint-state.mjs`'s `consumeFootprintState`
// (Phase 3 review, Medium finding).
//
// Before `consumeFootprintState` existed, `cli.mjs`'s `handleRecordOutcome`
// called `readFootprintState` then, separately, `clearFootprintState` — two
// independent lock acquisitions. A concurrent `--poll-footprint` for the
// SAME pid landing in the unlocked gap between those two calls would write a
// newer sample that the subsequent clear then silently deleted before it was
// ever recorded — a real data-loss race, since this feature polls every
// 10-30s per pid while `--record-outcome` can land at roughly the same time.
//
// This suite proves `consumeFootprintState` closes that gap two ways:
//   1. Directly — an externally-held lock blocks the ENTIRE read+delete, not
//      just one half of it, proving there is no unlocked window inside the
//      function for a concurrent write to land in.
//   2. Behaviourally — real concurrent `recordFootprintPoll`/
//      `consumeFootprintState` calls for the same pid, run many times to
//      exercise both possible lock-acquisition orders, never lose data:
//      whichever call wins the lock race completes its ENTIRE operation
//      before the other one starts, so a poll's sample is always either
//      folded into what `consumeFootprintState` returns, or still sitting in
//      the store for a future consume — never neither.
//
// This codebase has no established ESM `jest.unstable_mockModule`/`spyOn`
// idiom for spying on another native-ESM module's named exports (attempting
// `jest.spyOn` on an imported module namespace throws "Cannot assign to read
// only property" under `--experimental-vm-modules`) — so this suite proves
// the single-lock-acquisition property against the REAL fencing lock in
// `coordination-file.mjs`, the same idiom `coordination-file.jest.spec.mjs`
// and `dispatch.jest.spec.mjs` already use for lock/concurrency testing.

import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { promises as fsPromises, existsSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { jest } from '@jest/globals';

import { acquireLock, releaseLock } from './coordination-file.mjs';
import { LockLostError } from './coordination-file.mjs';
import { recordFootprintPoll, readFootprintState, consumeFootprintState, clearFootprintState } from './footprint-state.mjs';
// Namespace import (not a named `{ writeStoreAtomic }` import) — deliberate,
// mirroring `history.jest.spec.mjs`'s own comment on this exact idiom.
// `writeStoreAtomic` is not yet an exported symbol of `footprint-state.mjs`
// (Phase 1's whole point is that it needs to become one, taking a
// mandatory `lockContext`, mirroring `history.mjs`'s post- export). A
// named import of a not-yet-existing export is a hard ESM SyntaxError at
// module-link time, which would take down every test in this file — not the
// "genuinely red because the feature is missing" failure this suite wants.
import * as footprintStateModule from './footprint-state.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const footprintStateSource = readFileSync(join(__dirname, 'footprint-state.mjs'), 'utf8');

const LOCK_STALENESS_MS = 30_000;

let workDir;
let stateFilePath;
let lockPath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-footprint-state-'));
  stateFilePath = join(workDir, 'footprint-state.json');
  lockPath = `${stateFilePath}.lock`;
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
  jest.restoreAllMocks();
});

/**
 * Polls for `path` to exist rather than a fixed sleep — mirrors
 * `history.jest.spec.mjs`'s own `waitForFile` helper exactly.
 *
 * @param {string} path
 * @param {number} [timeoutMs]
 * @returns {Promise<boolean>}
 */
async function waitForFile(path, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (existsSync(path)) return true;
    // eslint-disable-next-line no-await-in-loop -- deliberate poll loop.
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  return false;
}

async function tmpSiblingsOf(dir) {
  return (await fsPromises.readdir(dir)).filter((name) => name.includes('.tmp-'));
}

/**
 * Pre-seeds `targetPath` with a bulk of unrelated filler pid entries, exactly
 * mirroring `history.jest.spec.mjs`'s own `seedFillerStore` — a single
 * `readStore`/`pruneStaleEntries`/`JSON.stringify` cycle over a store this
 * size takes long enough that an external test can reliably observe (and
 * act on) the `.lock` sibling while a real write is still in flight, without
 * a fixed sleep.
 *
 * @param {string} targetPath
 */
async function seedFootprintFillerStore(targetPath) {
  const count = 20_000;
  const store = {};
  for (let index = 0; index < count; index += 1) {
    store[`filler-${index}`] = {
      firstPolledAt: 0,
      lastPolledAt: 0,
      smoothed: 100,
      peakSmoothedMb: 100,
      trajectory: [100, 100, 100],
      padding: 'x'.repeat(40),
    };
  }
  await fsPromises.writeFile(targetPath, JSON.stringify(store), 'utf8');
}

/**
 * Same bulk-filler seeding as `seedFootprintFillerStore`, but preserves
 * whatever real entry already exists on disk for `preservePid` (written by a
 * prior genuine `recordFootprintPoll` call) — needed by
 * `consumeFootprintState`/`clearFootprintState` call-site tests, which need
 * BOTH a real entry to operate on AND a store large enough to slow their
 * read-modify-write cycle down to an externally-observable window.
 *
 * @param {string} targetPath
 * @param {number} preservePid
 */
async function seedFootprintFillerStoreKeepingExisting(targetPath, preservePid) {
  const existing = JSON.parse(await fsPromises.readFile(targetPath, 'utf8'));
  const preserved = existing[String(preservePid)];
  await seedFootprintFillerStore(targetPath);
  const withPreserved = JSON.parse(await fsPromises.readFile(targetPath, 'utf8'));
  withPreserved[String(preservePid)] = preserved;
  await fsPromises.writeFile(targetPath, JSON.stringify(withPreserved), 'utf8');
}

// ---------------------------------------------------------------------------
// Phase 1 (RED) — footprint-state.mjs's writeStoreAtomic has the
// identical unfenced-write gap closed for history.mjs's sibling
// function of the same name: no lockContext parameter, no
// assertStillHeld() recheck before rename(). Every test in this block is
// RED by construction against TODAY's footprint-state.mjs — none of it adds
// a stub implementation; the builder's job is to turn these green by
// mirroring history.mjs's writeStoreAtomic contract exactly (see that
// module's own extensive doc comment on `writeStoreAtomic` for the shape to
// copy: mandatory `{ assertStillHeld }`-shaped lockContext, stage-then-
// assert-then-rename ordering, `.tmp-*` cleanup on any failure, and a
// `fnThrew` flag in the calling function's `finally` so a releaseLock
// failure never masks what the try block actually threw).
// ---------------------------------------------------------------------------

describe('Phase 1 — writeStoreAtomic requires a lockContext, checked before any rename', () => {
  it('rejects with a greppable message and creates nothing when called without a lockContext', async () => {
    await expect(
      footprintStateModule.writeStoreAtomic(stateFilePath, { '4242': { firstPolledAt: 1 } }),
    ).rejects.toThrow(/lockContext[\s\S]*assertStillHeld/i);

    expect(existsSync(stateFilePath)).toBe(false);
    expect(await tmpSiblingsOf(workDir)).toEqual([]);
  });

  it('rejects a lockContext whose assertStillHeld is not callable, rather than trusting the shape', async () => {
    await expect(
      footprintStateModule.writeStoreAtomic(stateFilePath, { '4242': { firstPolledAt: 1 } }, { token: 'x' }),
    ).rejects.toThrow(/lockContext[\s\S]*assertStillHeld/i);

    expect(existsSync(stateFilePath)).toBe(false);
    expect(await tmpSiblingsOf(workDir)).toEqual([]);
  });

  it('writes and renames exactly as before when a real lockContext still holds ownership', async () => {
    const { acquireLock: acquire, releaseLock: release, assertStillHeld } = await import('./coordination-file.mjs');
    const token = await acquire(lockPath, LOCK_STALENESS_MS);

    await expect(
      footprintStateModule.writeStoreAtomic(stateFilePath, { '4242': { firstPolledAt: 1 } }, {
        lockPath,
        token,
        assertStillHeld: () => assertStillHeld(lockPath, token),
      }),
    ).resolves.toBeUndefined();

    expect(JSON.parse(readFileSync(stateFilePath, 'utf8'))).toEqual({ '4242': { firstPolledAt: 1 } });
    expect(await tmpSiblingsOf(workDir)).toEqual([]);

    await release(lockPath, token);
  });

  it('calls assertStillHeld() immediately before rename(), never after', async () => {
    const calls = [];
    const lockContext = {
      assertStillHeld: async () => {
        calls.push('assert');
      },
    };
    const originalRename = fsPromises.rename.bind(fsPromises);
    jest.spyOn(fsPromises, 'rename').mockImplementationOnce(async (src, dest) => {
      calls.push('rename');
      return originalRename(src, dest);
    });

    await footprintStateModule.writeStoreAtomic(stateFilePath, { '1': { firstPolledAt: 1 } }, lockContext);

    expect(calls).toEqual(['assert', 'rename']);
  });
});

describe('Phase 1 — .tmp-* cleanup on assertStillHeld throw and on rename failure', () => {
  it('leaves no orphaned .tmp-* sibling when assertStillHeld rejects (ownership lost)', async () => {
    const lostError = new LockLostError(lockPath);

    await expect(
      footprintStateModule.writeStoreAtomic(stateFilePath, { '1': { firstPolledAt: 1 } }, {
        assertStillHeld: async () => {
          throw lostError;
        },
      }),
    ).rejects.toBe(lostError);

    expect(existsSync(stateFilePath)).toBe(false);
    expect(await tmpSiblingsOf(workDir)).toEqual([]);
  });

  it('leaves no orphaned .tmp-* sibling when the rename itself fails (ownership was fine)', async () => {
    const renameError = new Error('simulated ENOSPC on rename');
    const originalRename = fsPromises.rename.bind(fsPromises);
    jest.spyOn(fsPromises, 'rename').mockImplementationOnce(async (src, dest) => {
      if (dest === stateFilePath) throw renameError;
      return originalRename(src, dest);
    });

    await expect(
      footprintStateModule.writeStoreAtomic(stateFilePath, { '1': { firstPolledAt: 1 } }, {
        assertStillHeld: async () => {},
      }),
    ).rejects.toBe(renameError);

    expect(existsSync(stateFilePath)).toBe(false);
    expect(await tmpSiblingsOf(workDir)).toEqual([]);
  });
});

describe('Phase 1 — finally-block masking: the original assertStillHeld failure must surface, not releaseLock\'s', () => {
  // Mirrors history.jest.spec.mjs's two finally-masking tests for
  // recordObservation exactly (lock-absence, then a genuine token-mismatch
  // reclaim) — the two real ways releaseLock's own call in a write-site
  // caller's `finally` could otherwise replace the ORIGINAL
  // assertStillHeld/LockLostError failure with cleanup noise. Both drive
  // `recordFootprintPoll` (the production caller), not writeStoreAtomic
  // directly, because the `fnThrew`-flag contract this proves belongs to the
  // CALLER's try/finally around releaseLock — exactly where `recordObservation`
  // carries it today and where `recordFootprintPoll` must gain it
  //.
  //
  // RED by construction: `recordFootprintPoll` does not build a lockContext
  // or call assertStillHeld at all today, so deleting/reclaiming the lock
  // file out from under it does not currently produce any rejection —
  // the poll simply succeeds, and the assertions below fail.

  it('propagates LockLostError from the write-side recheck, not masked by releaseLock, when the lock file is absent', async () => {
    await seedFootprintFillerStore(stateFilePath);
    const pid = 5001;
    const recordPromise = recordFootprintPoll(stateFilePath, pid, 200, 1_000, {
      lockStalenessMs: 60_000,
    });

    await expect(waitForFile(lockPath)).resolves.toBe(true);
    await fsPromises.unlink(lockPath);

    await expect(recordPromise).rejects.toBeInstanceOf(LockLostError);
    await expect(recordPromise).rejects.toMatchObject({ code: 'COORDINATION_LOCK_LOST' });
  }, 15_000);

  it('propagates LockLostError from the write-side recheck, not masked by releaseLock\'s mismatch branch, when a contender has genuinely reclaimed the lock', async () => {
    const { acquireLock: acquire, releaseLock: release, reclaimStaleLock } = await import('./coordination-file.mjs');
    await seedFootprintFillerStore(stateFilePath);
    const pid = 5002;

    const recordPromise = recordFootprintPoll(stateFilePath, pid, 200, 1_000, {
      lockStalenessMs: 60_000,
    });

    await expect(waitForFile(lockPath)).resolves.toBe(true);
    expect(await reclaimStaleLock(lockPath, -1)).toBe(true);
    const contenderToken = await acquire(lockPath, 60_000);

    let caught;
    try {
      await recordPromise;
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(LockLostError);
    expect(caught).toMatchObject({ code: 'COORDINATION_LOCK_LOST' });

    await release(lockPath, contenderToken);
  }, 15_000);
});

describe('Phase 1 — early-return paths must not attempt the fencing recheck at all', () => {
  // `acquireLock`/`releaseLock` themselves use `fs.rename` internally (the
  // lock file's own atomic-create/release mechanics) — so the regression
  // guard below only cares whether `fs.rename` was ever called with the
  // STORE path as its destination, not merely whether `rename` was called at
  // all.
  function renamesToStateFile(renameSpy) {
    return renameSpy.mock.calls.filter(([, dest]) => dest === stateFilePath);
  }

  it('consumeFootprintState never writes (and therefore never rechecks ownership) when the entry is undefined', async () => {
    const renameSpy = jest.spyOn(fsPromises, 'rename');

    const result = await consumeFootprintState(stateFilePath, 9_999_999);

    expect(result).toBeNull();
    expect(renamesToStateFile(renameSpy)).toEqual([]);
    // No store file at all was ever created — the early return happens
    // before writeStoreAtomic (and thus before any rename/assert) runs.
    expect(existsSync(stateFilePath)).toBe(false);
  });

  it('clearFootprintState never writes (and therefore never rechecks ownership) when the key is not present', async () => {
    await recordFootprintPoll(stateFilePath, 111, 100, 1_000);
    const renameSpy = jest.spyOn(fsPromises, 'rename');

    await clearFootprintState(stateFilePath, 222); // never polled — key absent

    expect(renamesToStateFile(renameSpy)).toEqual([]);
    // The genuinely-present entry (pid 111) is untouched.
    const entry = await readFootprintState(stateFilePath, 111);
    expect(entry).not.toBeNull();
  });
});

describe('Phase 1 — ownership-recheck ordering: stage tmp, then assert, then rename', () => {
  it('runs assertStillHeld after the tmp file is staged and before the rename — matching history.mjs, not the other way round', () => {
    const fnIndex = footprintStateSource.indexOf('function writeStoreAtomic(');
    expect(fnIndex).toBeGreaterThan(-1);

    const stagedWriteIndex = footprintStateSource.indexOf('fs.writeFile(tmpPath,', fnIndex);
    const assertIndex = footprintStateSource.indexOf('lockContext.assertStillHeld()', fnIndex);
    const renameIndex = footprintStateSource.indexOf('fs.rename(tmpPath, stateFilePath)', fnIndex);

    expect(stagedWriteIndex).toBeGreaterThan(fnIndex);
    expect(assertIndex).toBeGreaterThan(stagedWriteIndex);
    expect(renameIndex).toBeGreaterThan(assertIndex);
  });
});

describe('Phase 1 — all three call sites independently thread a lockContext through and are refused on lock loss', () => {
  it('recordFootprintPoll is refused (LockLostError), not silently written, when its lock is lost mid-write', async () => {
    await seedFootprintFillerStore(stateFilePath);
    const pid = 6001;
    const recordPromise = recordFootprintPoll(stateFilePath, pid, 200, 1_000, { lockStalenessMs: 60_000 });

    await expect(waitForFile(lockPath)).resolves.toBe(true);
    await fsPromises.unlink(lockPath);

    await expect(recordPromise).rejects.toBeInstanceOf(LockLostError);
  }, 15_000);

  it('consumeFootprintState is refused (LockLostError), not silently written, when its lock is lost mid-write', async () => {
    const pid = 6002;
    await recordFootprintPoll(stateFilePath, pid, 200, 1_000);
    await seedFootprintFillerStoreKeepingExisting(stateFilePath, pid);

    const consumePromise = consumeFootprintState(stateFilePath, pid, { lockStalenessMs: 60_000 });

    await expect(waitForFile(lockPath)).resolves.toBe(true);
    await fsPromises.unlink(lockPath);

    await expect(consumePromise).rejects.toBeInstanceOf(LockLostError);
  }, 15_000);

  it('clearFootprintState is refused (LockLostError), not silently written, when its lock is lost mid-write', async () => {
    const pid = 6003;
    await recordFootprintPoll(stateFilePath, pid, 200, 1_000);
    await seedFootprintFillerStoreKeepingExisting(stateFilePath, pid);

    const clearPromise = clearFootprintState(stateFilePath, pid, { lockStalenessMs: 60_000 });

    await expect(waitForFile(lockPath)).resolves.toBe(true);
    await fsPromises.unlink(lockPath);

    await expect(clearPromise).rejects.toBeInstanceOf(LockLostError);
  }, 15_000);
});

// ---------------------------------------------------------------------------
// 1. consumeFootprintState's read+delete happens under ONE lock acquisition
// — an externally-held lock blocks the whole operation, not just its first
// half.
// ---------------------------------------------------------------------------

test('an externally-held lock blocks the entire read+delete, proving there is no unlocked window inside consumeFootprintState', async () => {
  const pid = 4242;
  await recordFootprintPoll(stateFilePath, pid, 200, 1_000);

  // Simulate "some other process currently holds the fencing lock" —
  // exactly the state consumeFootprintState's own acquireLock call would be
  // waiting behind.
  const externalToken = await acquireLock(lockPath, LOCK_STALENESS_MS);

  let settled = false;
  const consumePromise = consumeFootprintState(stateFilePath, pid, {
    lockMaxAttempts: 50,
    lockRetryDelayMs: 20,
  }).then((entry) => {
    settled = true;
    return entry;
  });

  // Give consumeFootprintState plenty of chances to (incorrectly) proceed if
  // it were not actually blocked on the lock.
  await new Promise((resolvePromise) => setTimeout(resolvePromise, 150));

  expect(settled).toBe(false);
  // The store must be COMPLETELY untouched while the lock is held elsewhere
  // — neither the read nor the delete half of consumeFootprintState may have
  // run yet.
  const rawWhileBlocked = await readFile(stateFilePath, 'utf8');
  expect(JSON.parse(rawWhileBlocked)[String(pid)]).toBeDefined();

  await releaseLock(lockPath, externalToken);

  const consumed = await consumePromise;
  expect(settled).toBe(true);
  expect(consumed).toMatchObject({ firstPolledAt: 1_000 });

  // Both the read AND the delete completed as a single unit once the lock
  // was free — the entry is gone.
  const afterConsume = await readFootprintState(stateFilePath, pid);
  expect(afterConsume).toBeNull();
});

// ---------------------------------------------------------------------------
// 2. Real concurrent recordFootprintPoll / consumeFootprintState for the
// SAME pid never lose data, regardless of which wins the lock race.
// ---------------------------------------------------------------------------

test('concurrent recordFootprintPoll and consumeFootprintState for the same pid never silently lose the poll sample', async () => {
  const pid = 9999;
  const TRIALS = 20;

  for (let trial = 0; trial < TRIALS; trial += 1) {
    await rm(stateFilePath, { force: true });
    await rm(lockPath, { force: true });

    // Seed one prior poll so consumeFootprintState has something to
    // (potentially) read before the race even starts.
    await recordFootprintPoll(stateFilePath, pid, 200, 1_000 + trial);

    const racedSample = 900 + trial;
    const [consumed] = await Promise.all([
      consumeFootprintState(stateFilePath, pid),
      recordFootprintPoll(stateFilePath, pid, racedSample, 2_000 + trial),
    ]);

    const afterRace = await readFootprintState(stateFilePath, pid);

    // The raced poll's sample folds into an EWMA-smoothed value, so its
    // exact numeric contribution isn't independently identifiable — but its
    // effect (the trajectory growing by one entry) IS, and that is the
    // signal this test needs: it proves the poll's write was actually
    // applied to the store SOMEWHERE (either observed by consumeFootprintState's
    // own read, or landed after eviction and is still sitting in the store),
    // never applied-then-silently-deleted-unread as the pre-fix two-lock
    // implementation allowed.
    const consumedSawTheRacedPoll = consumed !== null && consumed.trajectory.length === 2;
    const storeStillHasTheRacedPoll = afterRace !== null && afterRace.trajectory.length === 1;

    expect(consumedSawTheRacedPoll || storeStillHasTheRacedPoll).toBe(true);

    // The failure mode this test guards against: the raced poll's write
    // happened, but neither the consumed entry nor the store reflects it —
    // i.e. it vanished between an unlocked read and a later delete.
    expect(consumedSawTheRacedPoll && storeStillHasTheRacedPoll).toBe(false);
  }
});

// ---------------------------------------------------------------------------
// 3. Staleness guard, pruning, agentClass omission, and trajectory-cap
// coverage on `recordFootprintPoll`'s actual production fold path
// (round-3 review, Medium finding — none of this was directly unit tested;
// only lock/race behaviour was).
// ---------------------------------------------------------------------------

describe('recordFootprintPoll staleness guard', () => {
  const STALE_AFTER_MS = 90_000;

  test('a stale entry (older than the threshold) resets firstPolledAt/peak/trajectory/agentClass on the next poll', async () => {
    const pid = 111;

    const first = await recordFootprintPoll(stateFilePath, pid, 100, 1_000, {
      staleAfterMs: STALE_AFTER_MS,
      agentClass: 'sub-agent',
    });
    expect(first.firstPolledAt).toBe(1_000);
    expect(first.agentClass).toBe('sub-agent');

    // Second poll lands well past STALE_AFTER_MS after the first's
    // lastPolledAt — this pid's entry must be treated as a fresh run.
    const staleLaterNow = 1_000 + STALE_AFTER_MS + 1;
    const second = await recordFootprintPoll(stateFilePath, pid, 500, staleLaterNow, {
      staleAfterMs: STALE_AFTER_MS,
    });

    expect(second.firstPolledAt).toBe(staleLaterNow);
    expect(second.trajectory).toEqual([500]);
    expect(second.peakSmoothedMb).toBe(500);
    // No --agent-class on this poll, and the prior entry was discarded as
    // stale, so agentClass must NOT survive from the stale entry.
    expect(second.agentClass).toBeUndefined();
  });

  test('a fresh (not-yet-stale) entry does not reset — folds normally', async () => {
    const pid = 112;

    const first = await recordFootprintPoll(stateFilePath, pid, 100, 1_000, {
      staleAfterMs: STALE_AFTER_MS,
      agentClass: 'orchestrator',
    });

    const freshLaterNow = 1_000 + STALE_AFTER_MS - 1;
    const second = await recordFootprintPoll(stateFilePath, pid, 200, freshLaterNow, {
      staleAfterMs: STALE_AFTER_MS,
    });

    expect(second.firstPolledAt).toBe(first.firstPolledAt);
    expect(second.trajectory.length).toBe(2);
    expect(second.agentClass).toBe('orchestrator');
  });

  test('a second poll that omits --agent-class preserves the previously-stored value', async () => {
    const pid = 113;

    await recordFootprintPoll(stateFilePath, pid, 100, 1_000, {
      staleAfterMs: STALE_AFTER_MS,
      agentClass: 'sub-agent',
    });

    const second = await recordFootprintPoll(stateFilePath, pid, 150, 1_500, {
      staleAfterMs: STALE_AFTER_MS,
      // agentClass intentionally omitted.
    });

    expect(second.agentClass).toBe('sub-agent');
  });

  test('pruning: an unrelated stale pid entry is pruned while polling a different pid, leaving the polled pid untouched', async () => {
    const pidA = 201;
    const pidB = 202;

    await recordFootprintPoll(stateFilePath, pidB, 300, 1_000, { staleAfterMs: STALE_AFTER_MS });
    await recordFootprintPoll(stateFilePath, pidA, 400, 1_200, { staleAfterMs: STALE_AFTER_MS });

    // Poll pid A again, long after pid B's last poll but well within
    // pid A's own freshness window.
    const now = 1_200 + STALE_AFTER_MS - 1;
    await recordFootprintPoll(stateFilePath, pidA, 410, now, { staleAfterMs: STALE_AFTER_MS });

    const entryA = await readFootprintState(stateFilePath, pidA);
    const entryB = await readFootprintState(stateFilePath, pidB);

    expect(entryA).not.toBeNull();
    expect(entryA.trajectory.length).toBe(2);
    // pid B's entry was stale relative to `now` (1_000 vs now, which is
    // > STALE_AFTER_MS later) and must have been pruned away as a
    // defense-in-depth side effect of polling pid A.
    expect(entryB).toBeNull();
  });

  test('trajectory stays capped at TRAJECTORY_CAP across many polls through recordFootprintPoll', async () => {
    const pid = 301;
    const TRAJECTORY_CAP = 120;
    let now = 1_000;

    for (let i = 0; i < TRAJECTORY_CAP + 10; i += 1) {
      // eslint-disable-next-line no-await-in-loop -- sequential polls of the same pid must fold in order.
      await recordFootprintPoll(stateFilePath, pid, 100 + i, now, { staleAfterMs: STALE_AFTER_MS });
      now += 1;
    }

    const finalEntry = await readFootprintState(stateFilePath, pid);
    expect(finalEntry.trajectory.length).toBe(TRAJECTORY_CAP);
  });
});

// ---------------------------------------------------------------------------
// 4. consumeFootprintState's own read-side staleness guard (round-3
// review, Medium finding — "asymmetric staleness guard": the write path
// guarded against pid reuse, but this read path did not).
// ---------------------------------------------------------------------------

describe('consumeFootprintState staleness guard', () => {
  test('returns null (and still evicts) for a stale entry when staleAfterMs/now are provided', async () => {
    const pid = 401;
    const staleAfterMs = 90_000;

    await recordFootprintPoll(stateFilePath, pid, 200, 1_000);

    const consumedNow = 1_000 + staleAfterMs + 1;
    const consumed = await consumeFootprintState(stateFilePath, pid, {
      now: consumedNow,
      staleAfterMs,
    });

    expect(consumed).toBeNull();

    // Still evicted, exactly like a fresh consume of an existing entry would
    // be — a stale entry must not be left behind to be misread again later.
    const afterConsume = await readFootprintState(stateFilePath, pid);
    expect(afterConsume).toBeNull();
  });

  test('returns the entry as usual when it is not stale', async () => {
    const pid = 402;
    const staleAfterMs = 90_000;

    await recordFootprintPoll(stateFilePath, pid, 200, 1_000);

    const consumedNow = 1_000 + staleAfterMs - 1;
    const consumed = await consumeFootprintState(stateFilePath, pid, {
      now: consumedNow,
      staleAfterMs,
    });

    expect(consumed).toMatchObject({ firstPolledAt: 1_000 });
  });

  test('without staleAfterMs/now, behaves exactly as before (no staleness check)', async () => {
    const pid = 403;

    await recordFootprintPoll(stateFilePath, pid, 200, 1_000);

    const consumed = await consumeFootprintState(stateFilePath, pid);
    expect(consumed).toMatchObject({ firstPolledAt: 1_000 });
  });
});
