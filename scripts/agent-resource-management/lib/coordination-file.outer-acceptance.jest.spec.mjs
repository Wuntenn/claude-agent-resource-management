// Phase 0 (outer acceptance test loop) — 5 deferred nit-fixes to
// scripts/agent-resource-management/lib/coordination-file.mjs. Each `it`
// below corresponds 1:1 to one deferred item from review:
//
//   1. releaseCapacity's no-op return path should return the literal
//      `released` variable it computed, not just a value that happens to
//      equal 0 — a pure code-honesty refactor with NO behavioral
//      difference. Written here as an explicit REGRESSION GUARD (it
//      already passes today) rather than a red test — see its own comment.
//   2. normalizeClaims's legacy-scalar branch should honour an injected
//      clock (the same `now`/`referenceNow` claimCapacity/releaseCapacity
//      already thread through everywhere else) instead of falling back to
//      a real, un-injectable Date.now() call when `declaredAt` is absent on
//      a legacy-shaped entry. MUST be red today.
//   3. A duplicated `__claim:<type>__` ledger entry (two entries sharing the
//      same reserved id — e.g. left behind by a hand-edited file, a crashed
//      write, or a pre-existing writer racing a migrated one) should have BOTH
//      entries' claims reflected in claimCapacity's `alreadyGrantedTotal`.
//      MUST be red today — current code only reads the FIRST `.find()`
//      match.
//   4. A ledger entry whose `claims[]` becomes empty (via a releaseCapacity
//      call that releases everything) should not be present in the
//      coordination file at all afterward. MUST be red today.
//   5. Calling claimCapacity/releaseCapacity with `options.claimTtlMs`
//      unset against a coordination file already holding a
//      legacy-migrated claim record should throw a clear, catchable error.
//      MUST be red today — current code silently proceeds with no TTL
//      enforcement at all.
//
// These are OUTER acceptance tests only — they exercise claimCapacity/
// releaseCapacity's public contract exactly as ./coordination-file.jest.
// spec.mjs's existing "Phase 1"/"Phase 2" describe blocks do
// (TestBed-free, real fs, mkdtemp work dir, jest.spyOn(Date, 'now') for
// clock control — no mocks of the module's own internals). They do not
// drive an implementation; the builder makes each red one green without
// changing these tests' assertions.

import { mkdtempSync, rmSync } from 'node:fs';
import { promises as fsPromises } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { jest } from '@jest/globals';

import {
  claimCapacity,
  releaseCapacity,
  acquireLock,
  releaseLock,
  reclaimStaleLock,
  reapConfirmedDeadRecords,
  DEFAULT_CLAIM_TTL_MS,
} from './coordination-file.mjs';

let workDir;
let filePath;

const ample = async () => 100;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'coordination-file-outer-acceptance-'));
  filePath = join(workDir, 'resource-coordination.json');
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
  jest.restoreAllMocks();
});

// ---------------------------------------------------------------------------
// 1. releaseCapacity's no-op return path (regression guard — already green)
// ---------------------------------------------------------------------------

it('nit 1 — releaseCapacity\'s no-op return path returns the literal released value it computed, and grantedTotal reflects the correct pre-existing total (REGRESSION GUARD: this is a pure code-honesty refactor with no behavioral difference — it already passes against today\'s code, pinning existing-correct behavior rather than driving new work)', async () => {
  // Releasing against a type with no ledger entry at all hits the no-op
  // branch (`released > 0 || needsLegacyMigrationWrite` is false).
  const neverClaimed = await releaseCapacity(filePath, {
    type: 'never-claimed-type',
    releaseCount: 5,
    orchestratorId: 'orch-solo',
  });
  expect(neverClaimed.released).toBe(0);
  expect(neverClaimed.grantedTotal).toBe(0);

  // A ledger entry exists for the type, but this orchestratorId holds none
  // of it — still the no-op branch, and grantedTotal must reflect the
  // correct pre-existing total (not silently reset to 0 by the no-op path).
  await claimCapacity(filePath, {
    type: 'preexisting-type',
    requestedCount: 4,
    orchestratorId: 'orch-holder',
    computeAvailableCapacity: ample,
  });

  const releaseByNonHolder = await releaseCapacity(filePath, {
    type: 'preexisting-type',
    releaseCount: 2,
    orchestratorId: 'orch-not-a-holder',
  });
  expect(releaseByNonHolder.released).toBe(0);
  expect(releaseByNonHolder.grantedTotal).toBe(4);
});

// ---------------------------------------------------------------------------
// 2. normalizeClaims's legacy-scalar branch and the real Date.now() fallback
// ---------------------------------------------------------------------------

it('nit 2 — normalizeClaims\'s legacy-scalar branch honours the injected clock instead of calling real Date.now() when declaredAt is absent (MUST be red today)', async () => {
  const injectedNow = 1_700_000_000_000;

  // Scenario A: legacy entry HAS declaredAt — normalizeClaims's legacy
  // branch takes claimedAt straight off the entry; no Date.now() call is
  // needed for this branch at all.
  const fileA = join(workDir, 'a.json');
  await fsPromises.writeFile(
    fileA,
    JSON.stringify([{ orchestratorId: '__claim:type-a__', grantedTotal: 5, declaredAt: injectedNow }]),
    'utf8',
  );

  // `claimTtlMs: DEFAULT_CLAIM_TTL_MS` is passed throughout this test (as
  // every real caller — cli.mjs — always does) so it exercises the
  // Date.now()-injection behavior under test, not the separate
  // claimTtlMs-unset+legacy-record guard covered by its own describe block
  // in coordination-file.jest.spec.mjs (Convergence Analysis
  // follow-up).
  const dateNowSpy = jest.spyOn(Date, 'now');
  dateNowSpy.mockClear();
  await claimCapacity(
    fileA,
    {
      type: 'type-a',
      requestedCount: 1,
      orchestratorId: 'orch-a',
      computeAvailableCapacity: ample,
      now: injectedNow,
    },
    { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
  );
  const callsWithDeclaredAt = dateNowSpy.mock.calls.length;

  // Scenario B: legacy entry has NO declaredAt at all — normalizeClaims's
  // legacy branch must synthesize claimedAt from *something*. Desired: the
  // SAME injected clock (`now`/referenceNow) claimCapacity already threads
  // through the rest of this critical section — an explicit `now` was
  // supplied, so no real, un-injectable Date.now() call should ever be
  // necessary here.
  const fileB = join(workDir, 'b.json');
  await fsPromises.writeFile(
    fileB,
    JSON.stringify([{ orchestratorId: '__claim:type-b__', grantedTotal: 5 }]),
    'utf8',
  );

  dateNowSpy.mockClear();
  await claimCapacity(
    fileB,
    {
      type: 'type-b',
      requestedCount: 1,
      orchestratorId: 'orch-b',
      computeAvailableCapacity: ample,
      now: injectedNow,
    },
    { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
  );
  const callsWithoutDeclaredAt = dateNowSpy.mock.calls.length;

  // Both scenarios drive the identical lock/read/write codepath against a
  // freshly-created, uncontended lock file — the ONLY structural difference
  // between A and B is whether normalizeClaims's legacy branch has to
  // synthesize claimedAt itself. Today it does so via a real Date.now()
  // call, one MORE real-clock read than scenario A — this assertion fails
  // until normalizeClaims is threaded the injected clock instead.
  expect(callsWithoutDeclaredAt).toBe(callsWithDeclaredAt);
});

// ---------------------------------------------------------------------------
// 3. Duplicated __claim:<type>__ ledger entries
// ---------------------------------------------------------------------------

it('nit 3 — a duplicated __claim:<type>__ ledger entry (two entries sharing the same reserved id, one legacy-scalar-shaped and one already claims-array-shaped) has BOTH entries\' claims reflected in claimCapacity\'s alreadyGrantedTotal (MUST be red today)', async () => {
  const ledgerId = '__claim:duplicated-type__';
  const now = Date.now();

  await fsPromises.writeFile(
    filePath,
    JSON.stringify([
      // Legacy-scalar-shaped duplicate.
      { orchestratorId: ledgerId, grantedTotal: 4, declaredAt: now },
      // Already-migrated, claims-array-shaped duplicate — e.g. left behind
      // by a mixed-version writer racing the legacy one onto the same file.
      {
        orchestratorId: ledgerId,
        claims: [{ orchestratorId: 'orch-b', count: 6, claimedAt: now }],
        grantedTotal: 6,
        declaredAt: now,
      },
    ]),
    'utf8',
  );

  // `claimTtlMs: DEFAULT_CLAIM_TTL_MS` (as every real caller — cli.mjs —
  // always passes) so this test exercises the duplicate-merge behavior under
  // test, not the separate claimTtlMs-unset+legacy-record guard covered by
  // its own describe block in coordination-file.jest.spec.mjs
  // (a Convergence Analysis follow-up).
  const result = await claimCapacity(
    filePath,
    {
      type: 'duplicated-type',
      requestedCount: 1,
      orchestratorId: 'orch-new',
      computeAvailableCapacity: ample,
    },
    { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
  );

  // Current code's `existing.find(entry => entry.orchestratorId === ledgerId)`
  // only ever reads the FIRST match (the legacy-scalar entry, total 4) —
  // the second duplicate's 6 claimed slots are silently invisible to this
  // aggregate.
  expect(result.alreadyGrantedTotal).toBe(10);
});

// ---------------------------------------------------------------------------
// 4. Fully-drained ledger entries must not linger with an empty claims[]
// ---------------------------------------------------------------------------

it('nit 4 — a ledger entry whose claims[] becomes empty after releaseCapacity releases everything is not present in the coordination file at all afterward (MUST be red today)', async () => {
  await claimCapacity(filePath, {
    type: 'drain-type',
    requestedCount: 3,
    orchestratorId: 'orch-solo',
    computeAvailableCapacity: ample,
  });

  await releaseCapacity(filePath, {
    type: 'drain-type',
    releaseCount: 3,
    orchestratorId: 'orch-solo',
  });

  const raw = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
  const ledgerEntry = raw.find((entry) => entry.orchestratorId === '__claim:drain-type__');

  // Today: the write path unconditionally persists `nextLedgerEntry`, even
  // when `nextClaims`/`stampedClaims` ended up empty — the entry survives
  // on disk as `{ orchestratorId: '__claim:drain-type__', claims: [],
  // grantedTotal: 0, declaredAt: ... }` instead of disappearing.
  expect(ledgerEntry).toBeUndefined();
});

// ---------------------------------------------------------------------------
// 5. claimTtlMs unset against an already legacy-migrated claim record
// ---------------------------------------------------------------------------

it('nit 5 — claimCapacity/releaseCapacity throw a clear, catchable error when options.claimTtlMs is unset against a coordination file already holding a legacy-migrated claim record (MUST be red today)', async () => {
  await fsPromises.writeFile(
    filePath,
    JSON.stringify([{ orchestratorId: '__claim:no-ttl-type__', grantedTotal: 3, declaredAt: Date.now() }]),
    'utf8',
  );

  // Today: `options.claimTtlMs` unset -> `pruneExpiredClaims` short-circuits
  // via `isFiniteNumber(claimTtlMs)` being false and returns `claims`
  // untouched, no TTL enforcement of any kind, and no error of any kind —
  // both calls below currently resolve rather than reject.
  await expect(
    claimCapacity(filePath, {
      type: 'no-ttl-type',
      requestedCount: 1,
      orchestratorId: 'orch-a',
      computeAvailableCapacity: ample,
    }),
  ).rejects.toThrow();

  await expect(
    releaseCapacity(filePath, { type: 'no-ttl-type', releaseCount: 1, orchestratorId: 'orch-a' }),
  ).rejects.toThrow();
});

// ---------------------------------------------------------------------------
// 6. Corrupt count:0-only claim records must not linger in the persisted
//    ledger — (follow-up to the "Known residual gap" comment left in
//    releaseCapacity's write path, and its claimCapacity mirror)
// ---------------------------------------------------------------------------

it('releaseCapacity omits a corrupt count:0-only claim record from the persisted ledger entry', async () => {
  const now = Date.now();

  // -------------------------------------------------------------------------
  // releaseCapacity: a genuine full release of the caller's own live claim
  // leaves behind a hand-edited/corrupt `count: 0` record (for the SAME
  // orchestratorId) riding along in the same ledger entry. `isValidClaimRecord`
  // admits `count: 0` (via `isNonNegativeInteger`), so this record survives
  // `normalizeClaims` untouched — nothing upstream of the write strips it.
  // Today, releaseCapacity's `stampedClaims.length > 0` omission guard sees
  // this lone zero-count record and keeps the entry on disk as
  // `{ claims: [{ count: 0, ... }], grantedTotal: 0 }` instead of omitting it
  // entirely — the exact "entry that only ever grows" leak the guard exists
  // to close, just for this one degenerate shape (see the "Known residual
  // gap" comment on releaseCapacity's write path).
  // -------------------------------------------------------------------------
  const releaseLedgerId = '__claim:release-zero-type__';
  await fsPromises.writeFile(
    filePath,
    JSON.stringify([
      {
        orchestratorId: releaseLedgerId,
        claims: [
          { orchestratorId: 'orch-a', count: 5, claimedAt: now },
          // Corrupt: a hand-edited/leftover count:0 record for the SAME
          // orchestratorId, constructed directly rather than via a normal
          // claim/release call (which never write a bare count:0 record).
          { orchestratorId: 'orch-a', count: 0, claimedAt: now },
        ],
        grantedTotal: 5,
        declaredAt: now,
      },
    ]),
    'utf8',
  );

  const releaseResult = await releaseCapacity(filePath, {
    type: 'release-zero-type',
    releaseCount: 5,
    orchestratorId: 'orch-a',
    now,
  });
  expect(releaseResult.released).toBe(5);

  const rawAfterRelease = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
  const releaseLedgerEntry = rawAfterRelease.find((entry) => entry.orchestratorId === releaseLedgerId);

  // Phase 1: releaseCapacity's write path now guards against this
  // degenerate zero-count-only shape — this assertion is green.
  expect(releaseLedgerEntry).toBeUndefined();
});

it('claimCapacity omits a corrupt count:0-only claim record from the persisted ledger entry', async () => {
  const now = Date.now();

  // -------------------------------------------------------------------------
  // claimCapacity's legacy-migration path: an existing legacy-scalar
  // record (`legacyMigrated: true`, no `claims[]`) with a fractional
  // `grantedTotal` in (0, 1) so `Math.floor(grantedTotal)` inside
  // `normalizeClaims` produces a synthesized `count: 0` record. A call that
  // grants 0 (computeAvailableCapacity returns 0) still enters the write
  // branch via `needsLegacyMigrationWrite`, without ever appending a new
  // nonzero claim — at the time this test was written, claimCapacity's write
  // path had NO equivalent omission guard at all (its doc comment used to
  // say: "There is therefore no 'omit the dead entry' case to guard for on
  // this write path"), so it unconditionally persisted the emptied/
  // zero-count entry.
  //
  // Phase 2 closed this gap: claimCapacity's write path now runs
  // `filterOutZeroCountClaims` after `stampMigratedClaims` and, mirroring
  // `releaseCapacity`, omits the ledger entry entirely when nothing live
  // remains (see `filterOutZeroCountClaims`'s doc comment and the
  // `liveNextClaims` inline comment in `coordination-file.mjs` for the
  // current behaviour). This assertion is green.
  // -------------------------------------------------------------------------
  const claimLedgerId = '__claim:claim-zero-type__';
  const claimFilePath = join(workDir, 'claim-zero.json');
  await fsPromises.writeFile(
    claimFilePath,
    JSON.stringify([
      {
        orchestratorId: claimLedgerId,
        // Legacy scalar shape (no claims[]) — normalizeClaims folds this into
        // a synthetic legacyMigrated record with count: Math.floor(0.5) = 0.
        grantedTotal: 0.5,
        declaredAt: now,
      },
    ]),
    'utf8',
  );

  const claimResult = await claimCapacity(
    claimFilePath,
    {
      type: 'claim-zero-type',
      requestedCount: 1,
      orchestratorId: 'orch-b',
      computeAvailableCapacity: async () => 0,
      now,
    },
    { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
  );
  expect(claimResult.granted).toBe(0);

  const rawAfterClaim = JSON.parse(await fsPromises.readFile(claimFilePath, 'utf8'));
  const claimLedgerEntry = rawAfterClaim.find((entry) => entry.orchestratorId === claimLedgerId);

  // Phase 2 landed the omission guard on claimCapacity's write path
  // (mirroring releaseCapacity's) — this assertion is now green.
  expect(claimLedgerEntry).toBeUndefined();
});

// ---------------------------------------------------------------------------
// Outer acceptance test for — "lock has no in-critical-section fencing
// re-check before write".
//
// RED BY CONSTRUCTION until the write-side re-check lands. Today
// `writeEntriesAtomic` takes `(filePath, entries)` only: nothing re-proves,
// at write time, that the lock this caller acquired is still the lock on
// disk. A holder that stalls past `lockStalenessMs` therefore has its lock
// legitimately reclaimed by `reclaimStaleLock`, and then writes anyway —
// clobbering whatever the reclaiming holder wrote in the meantime, with no
// thrown error and no signal of any kind. That is the silent-drop failure
// this ticket exists to close.
//
// The driver is deliberately NOT a fake timer and NOT an fs mock (this
// module's correctness is only meaningful against the real filesystem — see
// ./coordination-file.jest.spec.mjs's header). `claimCapacity` awaits its
// caller-supplied `computeAvailableCapacity` callback INSIDE the critical
// section, which makes that callback a genuine, deterministic seam: while it
// is awaited, the test performs a real `reclaimStaleLock` against the
// production path (not a hand-unlink) and lets a second holder acquire the
// lock and write. When the callback returns, holder A is provably no longer
// the owner — so what happens next is exactly the production failure mode.
//
// GREEN once `writeEntriesAtomic` re-verifies ownership immediately before
// its `rename`: holder A throws `LockLostError`, writes nothing, and holder
// B's entry survives untouched.
//
// Which guard fires here, honestly: since its post-probe re-check landed,
// the FIRST guard this particular path trips is the assert immediately after
// `computeAvailableCapacity` returns, not the one inside
// `writeEntriesAtomic` — both throw the same `LockLostError`, so this test
// cannot distinguish them and does not claim to. It proves the end-to-end
// contract (refuse, throw, leave the other holder's entry and lock intact).
// The write choke point itself is proven directly, in isolation, by the
// `writeEntriesAtomic ownership choke point` block in
// ./coordination-file.jest.spec.mjs.
//
// Honesty note carried from the Build Plan: this re-check NARROWS the
// assert->rename window; it does not close it. A reclaim landing inside that
// one-syscall gap can still clobber. See coordination-file.mjs's top-of-file
// comment for the distinction against the removal/restore paths' structural
// close.
// ---------------------------------------------------------------------------

describe('outer acceptance: write-side fencing re-check', () => {
  it("a holder whose lock was reclaimed mid-critical-section refuses to write and throws LockLostError, leaving the reclaiming holder's entry intact", async () => {
    const now = Date.now();
    const lockPath = `${filePath}.lock`;
    const holderBEntry = { orchestratorId: 'holder-b', desiredAgents: 1, declaredAt: now };

    let holderBToken;

    // Runs while holder A holds the lock. Everything in here is a real fs
    // operation through the module's own exported fencing surface.
    const reclaimTheLockAndWriteAsHolderB = async () => {
      // A negative staleness threshold makes holder A's freshly-minted lock
      // read as stale to THIS call without any wall-clock waiting — the
      // reclaim itself still runs the full production claim-then-verify
      // path, so what is being exercised is `reclaimStaleLock`'s real
      // behaviour, not a hand-unlink standing in for it.
      const reclaimed = await reclaimStaleLock(lockPath, -1);
      expect(reclaimed).toBe(true);

      // Holder B now legitimately owns the lock and performs its own write.
      holderBToken = await acquireLock(lockPath, 60_000);
      await fsPromises.writeFile(filePath, JSON.stringify([holderBEntry]), 'utf8');

      // Deliberately still held when holder A resumes, so what fires is a
      // token MISMATCH (someone else's live lock at this path) rather than
      // the weaker "lock file is simply absent" case.
      return 100;
    };

    await expect(
      claimCapacity(
        filePath,
        {
          type: 'fencing-recheck-type',
          requestedCount: 4,
          orchestratorId: 'holder-a',
          computeAvailableCapacity: reclaimTheLockAndWriteAsHolderB,
          now,
        },
        { claimTtlMs: DEFAULT_CLAIM_TTL_MS },
      ),
    ).rejects.toMatchObject({ name: 'LockLostError', code: 'COORDINATION_LOCK_LOST' });

    // Holder B's entry survives byte-for-byte — holder A's ledger write was
    // refused, not merely reordered behind it.
    const persisted = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    expect(persisted).toEqual([holderBEntry]);

    // On THIS path no `.tmp-*` is ever staged, because the post-probe assert
    // fires before `writeEntriesAtomic` is entered (see the block header). So
    // this is an ORDERING guard, not evidence of cleanup: if a future change
    // moved the ownership re-check later, a staged sibling would appear here
    // and this would catch it. The cleanup itself is proven directly by the
    // `writeEntriesAtomic ownership choke point` block in
    // ./coordination-file.jest.spec.mjs.
    const siblings = await fsPromises.readdir(workDir);
    expect(siblings.filter((name) => name.includes('.tmp-'))).toEqual([]);

    // `withLock`'s `finally` still ran, and its fencing-aware `releaseLock`
    // left holder B's lock exactly where it was rather than dropping someone
    // else's lock on the way out.
    const lockPayloadAfter = JSON.parse(await fsPromises.readFile(lockPath, 'utf8'));
    expect(lockPayloadAfter.token).toBe(holderBToken);

    await releaseLock(lockPath, holderBToken);
  });
});

// ---------------------------------------------------------------------------
// — the reap write is subject to the SAME fencing re-check as every
// other write to this file.
//
// `reapConfirmedDeadRecords` is the only writer added by the ledger sweep, and
// the property it must carry is not "it aborts when something throws at its
// commit point" — an injected throw proves only that the caller's own hook was
// awaited. The property is that the write itself goes through
// `writeEntriesAtomic`, whose `assertStillHeld()` re-proves ownership
// immediately before the rename. A bespoke `fs.writeFile` in that spot would
// satisfy an injected-throw test unchanged while silently clobbering whichever
// holder legitimately reclaimed the lock in the meantime — which, for a reap,
// means deleting live records another writer had just added.
//
// So this exercises the GENUINE path: the lock is really reclaimed, and a
// second holder really writes, while the sweep is inside its critical section.
// Mirrors the `claimCapacity` fencing test above; the seam here is
// `beforeCommit`, which the module documents as awaited inside the critical
// section immediately before the write, and which is used purely to time the
// reclaim rather than to inject the failure.
// ---------------------------------------------------------------------------

describe('outer acceptance: the reap write reuses the fencing choke point', () => {
  it("a sweep whose lock is reclaimed mid-critical-section refuses to write and throws LockLostError, leaving the reclaiming holder's entries intact", async () => {
    const now = Date.now();
    const lockPath = `${filePath}.lock`;
    const ghostEntry = { orchestratorId: 'ghost-orch', desiredAgents: 1, declaredAt: now, pid: 4242 };
    const holderBEntry = { orchestratorId: 'holder-b', desiredAgents: 1, declaredAt: now };

    await fsPromises.writeFile(filePath, JSON.stringify([ghostEntry]), 'utf8');

    let holderBToken;

    await expect(
      reapConfirmedDeadRecords(filePath, {
        isDead: (record) => record?.orchestratorId === 'ghost-orch',
        beforeCommit: async () => {
          // Real `reclaimStaleLock` against the production path, exactly as
          // the `claimCapacity` test above — a negative staleness threshold
          // makes the sweep's freshly-minted lock read as stale to THIS call
          // without any wall-clock waiting.
          const reclaimed = await reclaimStaleLock(lockPath, -1);
          expect(reclaimed).toBe(true);

          // Holder B now legitimately owns the lock and writes an entry the
          // sweep's own survivor list knows nothing about. Deliberately still
          // held when the sweep resumes, so what fires is a token MISMATCH
          // rather than the weaker "lock file is simply absent" case.
          holderBToken = await acquireLock(lockPath, 60_000);
          await fsPromises.writeFile(filePath, JSON.stringify([holderBEntry]), 'utf8');
        },
      }),
    ).rejects.toMatchObject({ name: 'LockLostError', code: 'COORDINATION_LOCK_LOST' });

    // Holder B's entry survives byte-for-byte: the reap was refused, not
    // merely reordered behind it. An unfenced write here would have replaced
    // the file with the sweep's own survivor list and taken holder B with it.
    const persisted = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    expect(persisted).toEqual([holderBEntry]);

    // No staged sibling left behind by the refused write.
    const siblings = await fsPromises.readdir(workDir);
    expect(siblings.filter((name) => name.includes('.tmp-'))).toEqual([]);

    // `withLock`'s fencing-aware release left holder B's lock where it was.
    const lockPayloadAfter = JSON.parse(await fsPromises.readFile(lockPath, 'utf8'));
    expect(lockPayloadAfter.token).toBe(holderBToken);

    await releaseLock(lockPath, holderBToken);
  });
});
