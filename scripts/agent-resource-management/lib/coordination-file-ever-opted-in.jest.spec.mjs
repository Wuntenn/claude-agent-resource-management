// Phase 1 (RED) — the sticky "ever opted-in" ledger entry.
//
// design decision 1: the PreToolUse gate's question is "has this session
// EVER declared dibs?", not "does it have a live entry right now". The current
// ledger cannot answer that — `declareDibs`/`readDibs` are liveness-pruned by
// construction, so "no live entry" is indistinguishable from "never opted in",
// and a mid-session `pruneStale` expiry or a stray `--release` would silently
// disarm the gate.
//
// The decision is to add ONE more reserved entry to the SAME coordination file,
// following the existing `GLOBAL_SPAWN_BUCKET_RESERVED_ID` /
// `SHARED_SAMPLE_RESERVED_ID` / `AIMD_CEILING_RESERVED_ID` precedent: leading +
// trailing `__` so `isReservedEntry` recognises it and `pruneStale` exempts it
// unconditionally, mutated only inside `withLock`/`writeEntriesAtomic`, holding
// the de-duplicated SET of session-derived orchestrator-ids that have declared
// dibs at least once.
//
// ---------------------------------------------------------------------------
// THE CONTRACT THIS FILE PINS (unambiguous, for the Phase 1 implementer)
//
//   EVER_OPTED_IN_RESERVED_ID  — exported const, value `'__ever-opted-in__'`.
//
//   recordEverOptedIn(filePath, orchestratorId, { now } = {}, options = {})
//     -> Promise<boolean>   `true` when THIS call newly added the id,
//                           `false` when it was already recorded (idempotent
//                           no-op). Performs the whole read-union-write inside
//                           ONE `withLock` critical section — never a
//                           read-then-write against a snapshot taken outside
//                           the lock. `options` carries the same
//                           `lockStalenessMs`/`lockMaxAttempts`/
//                           `lockRetryDelayMs` knobs every other write in this
//                           module takes.
//     THROWS `TypeError` for a non-string/empty id, and for an id matching
//     `isReservedEntry`'s `__…__` convention (see the collision section below).
//
//   hasEverOptedIn(filePath, orchestratorId, { now } = {}, options = {})
//     -> Promise<boolean>   A cheap, LOCK-FREE, plain read (the hook calls this
//                           on every gated tool call and must never shell out
//                           or contend for the coordination lock). FAILS OPEN:
//                           a missing file, unreadable file, corrupt JSON, or a
//                           non-array top-level shape all resolve `false` and
//                           NEVER throw — design decision 3's not-opted-in
//                           tier. Never creates or writes the file.
//
//   declareDibs(...)        additionally records `entry.orchestratorId` into
//                           the set on EVERY call, inside its own existing
//                           critical section, idempotently.
//
//   PERSISTED SHAPE (revised by the Phase 1 review — the original bare
//   `orchestratorIds: string[]` had no per-id recency, so the never-pruned set
//   could only ever grow, and nothing could distinguish a live id from one
//   last seen months ago)
//     { orchestratorId: '__ever-opted-in__',
//       everOptedIn: [{ id: string, lastSeenAt: number }, ...],  // SET, order
//                                                                // unspecified
//       declaredAt: number }   // row-level "last rewritten", read by nothing
//     Exactly ONE such row ever exists. Nothing below asserts array ORDER.
//
//   LEGACY SHAPE — a row persisted as `orchestratorIds: string[]` (the first
//     cut, shipped earlier on this same branch) is still READ correctly by
//     `hasEverOptedIn`, and MIGRATED into the per-id-record shape on the next
//     write rather than erroring or being discarded.
//
//   BOUNDED GROWTH
//     `EVER_OPTED_IN_MAX_AGE_MS` (30 days) sweeps ids untouched for that long;
//     `EVER_OPTED_IN_MAX_RECORDS` is the count-cap backstop. Both evict
//     OLDEST-`lastSeenAt`-first, and the id being touched by the write is
//     exempt from both — a live session is never the victim.
//     Every touch (`declareDibs` or `recordEverOptedIn`), INCLUDING a repeat
//     of an already-recorded id, refreshes that id's `lastSeenAt`. A repeat
//     still returns `false` from `recordEverOptedIn` (membership unchanged)
//     but is no longer a no-op on disk.
//
//   RESERVED-ID COLLISION — REJECT, not normalize (the Build Plan left this
//     "rejected/normalized"; this file picks one and asserts it).
//       * `recordEverOptedIn` THROWS `TypeError` for a `__…__`-shaped id.
//       * `declareDibs` must NOT start throwing for one — this module's own
//         existing tests seed reserved rows through `declareDibs` (see the
//         AIMD prune-exemption test in `coordination-file.jest.spec.mjs`), so
//         that would be a breaking change. It instead SKIPS the ever-opted-in
//         recording for a reserved-shaped id, and `hasEverOptedIn` therefore
//         answers `false` for it forever.
//       Normalization was rejected because stripping/escaping `__` maps
//       distinct real ids onto one another — the same cross-session state leak
//       Convergence Analysis flagged for the sanitizer in Phase 2.
//
// RED BY CONSTRUCTION: none of `EVER_OPTED_IN_RESERVED_ID`,
// `recordEverOptedIn`, or `hasEverOptedIn` exist yet, and `declareDibs` does
// not record anything. Everything not-yet-implemented is reached through the
// NAMESPACE import (`coordinationFile.*`), never a named import — a named
// import of a genuinely missing ESM export is a hard module-evaluation
// SyntaxError that would take down every test in this file instead of failing
// them one by one with a useful message.
// ---------------------------------------------------------------------------

import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { promises as fsPromises } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { declareDibs, readDibs, isReservedEntry } from './coordination-file.mjs';
import * as coordinationFile from './coordination-file.mjs';

// The literal this file assumes. Asserted against the module's own exported
// constant below, so a rename by the implementer surfaces as one failing
// assertion rather than silently divergent test/production ids.
const EXPECTED_EVER_OPTED_IN_RESERVED_ID = '__ever-opted-in__';

// Session-derived orchestrator ids, in the `session-<sessionId>` shape 
// decision 2 / Phase 2 mandate. (This phase does not depend on Phase 2's
// `deriveOrchestratorId` — the primitive under test takes an opaque string.)
const ORCH_A = 'session-7c9a1f42-1b7e-4d2c-9f31-0a6b5c8e4d10';
const ORCH_B = 'session-de3f0b21-55aa-4c98-8e17-9d4c2a7f6b03';

const LIVENESS_THRESHOLD_MS = 15 * 60 * 1000;

let workDir;
let filePath;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'coordination-ever-opted-in-'));
  filePath = join(workDir, 'resource-coordination.json');
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
});

/** The persisted reserved row, or `undefined`. Read straight off disk. */
async function readEverOptedInEntry() {
  if (!existsSync(filePath)) return undefined;
  const parsed = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
  return parsed.find((entry) => entry?.orchestratorId === EXPECTED_EVER_OPTED_IN_RESERVED_ID);
}

/** Every row carrying the reserved id — used to prove it is never duplicated. */
async function readEverOptedInRows() {
  if (!existsSync(filePath)) return [];
  const parsed = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
  return parsed.filter((entry) => entry?.orchestratorId === EXPECTED_EVER_OPTED_IN_RESERVED_ID);
}

/**
 * The recorded per-id RECORDS, however the implementer shapes the row's array
 * field. Deliberately tolerant of the field NAME (`everOptedIn` is the pinned
 * choice) — what is NOT tolerated is more than one array field on the row,
 * which would mean the legacy `orchestratorIds` array survived a write
 * alongside the canonical one instead of being migrated away.
 */
async function readRecordedRecords() {
  const entry = await readEverOptedInEntry();
  if (!entry) return [];
  const arrays = Object.values(entry).filter((value) => Array.isArray(value));
  expect(arrays).toHaveLength(1);
  return arrays[0];
}

/** Just the ids from `readRecordedRecords`, for membership assertions. */
async function readRecordedIds() {
  const records = await readRecordedRecords();
  return records.map((record) => (typeof record === 'string' ? record : record?.id));
}

/** `lastSeenAt` for one recorded id, or `undefined` if it is not recorded. */
async function readLastSeenAt(orchestratorId) {
  const records = await readRecordedRecords();
  return records.find((record) => record?.id === orchestratorId)?.lastSeenAt;
}

/** Writes `rows` to the coordination file verbatim — seeds on-disk shapes. */
async function seedRows(rows) {
  await fsPromises.writeFile(filePath, JSON.stringify(rows), 'utf8');
}

/**
 * Drops every non-reserved row — the union of the two ways says a
 * session can lose its LIVE entry mid-session (`pruneStale` expiry and an
 * explicit `--release`). Mirrors the outer acceptance test's own
 * `dropAllLiveEntries` helper exactly.
 */
async function dropAllLiveEntries() {
  const parsed = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
  const survivors = parsed.filter((entry) => isReservedEntry(entry?.orchestratorId));
  await fsPromises.writeFile(filePath, JSON.stringify(survivors), 'utf8');
}

describe('Phase 1 — sticky ever-opted-in ledger entry: the reserved id', () => {
  it('exports EVER_OPTED_IN_RESERVED_ID following the existing __…__ reserved-entry convention', () => {
    expect(coordinationFile.EVER_OPTED_IN_RESERVED_ID).toBe(EXPECTED_EVER_OPTED_IN_RESERVED_ID);
    expect(isReservedEntry(coordinationFile.EVER_OPTED_IN_RESERVED_ID)).toBe(true);
  });

  it('does not collide with the three existing reserved ids', () => {
    expect(EXPECTED_EVER_OPTED_IN_RESERVED_ID).not.toBe(coordinationFile.SHARED_SAMPLE_RESERVED_ID);
    expect(EXPECTED_EVER_OPTED_IN_RESERVED_ID).not.toBe(coordinationFile.AIMD_CEILING_RESERVED_ID);
    expect(EXPECTED_EVER_OPTED_IN_RESERVED_ID).not.toBe('__global-spawn-rate-bucket__');
  });

  it('exports recordEverOptedIn and hasEverOptedIn as functions', () => {
    expect(typeof coordinationFile.recordEverOptedIn).toBe('function');
    expect(typeof coordinationFile.hasEverOptedIn).toBe('function');
  });
});

describe('Phase 1 — declareDibs records the id into the sticky set', () => {
  it('hasEverOptedIn is true after a single declareDibs call for that id', async () => {
    const now = Date.now();

    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now })).toBe(false);

    await declareDibs(
      filePath,
      { orchestratorId: ORCH_A, desiredAgents: 1, declaredAt: now },
      { livenessThresholdMs: LIVENESS_THRESHOLD_MS },
    );

    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now })).toBe(true);
  });

  it('persists the set under the reserved row, as a de-duplicated array', async () => {
    const now = Date.now();
    await declareDibs(filePath, { orchestratorId: ORCH_A, declaredAt: now });
    await declareDibs(filePath, { orchestratorId: ORCH_B, declaredAt: now });

    const recorded = await readRecordedIds();
    expect(new Set(recorded)).toEqual(new Set([ORCH_A, ORCH_B]));
    expect(recorded).toHaveLength(2);
    expect(await readEverOptedInRows()).toHaveLength(1);
  });

  it("one session's opt-in never marks a DIFFERENT session as opted in", async () => {
    const now = Date.now();
    await declareDibs(filePath, { orchestratorId: ORCH_A, declaredAt: now });

    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now })).toBe(true);
    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_B, { now })).toBe(false);
  });
});

describe('Phase 1 — stickiness: the record outlives the live dibs entry', () => {
  // The single most important property in this file. Without it a mid-session
  // pruneStale expiry or a stray --release silently disarms the gate, which is
  // the exact gap exists to close, moved one layer down.
  it('stays true after the live entry is pruned by pruneStale (age-based expiry)', async () => {
    const declaredAt = 1_000_000;
    const wellPastThreshold = declaredAt + 20 * 60 * 1000;

    await declareDibs(
      filePath,
      { orchestratorId: ORCH_A, declaredAt },
      { livenessThresholdMs: LIVENESS_THRESHOLD_MS },
    );

    // A later, unrelated write physically prunes A's now-stale live row.
    await declareDibs(
      filePath,
      { orchestratorId: ORCH_B, declaredAt: wellPastThreshold },
      { livenessThresholdMs: LIVENESS_THRESHOLD_MS },
    );

    // Precondition: A's LIVE row really is gone, so a "does it have an entry
    // right now" implementation would read A as never-opted-in…
    const live = (await readDibs(filePath, wellPastThreshold, { livenessThresholdMs: LIVENESS_THRESHOLD_MS })).filter(
      (entry) => !isReservedEntry(entry.orchestratorId),
    );
    expect(live.map((entry) => entry.orchestratorId)).not.toContain(ORCH_A);

    // …while the sticky record survives.
    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now: wellPastThreshold })).toBe(true);
  });

  it('stays true after the live entry is explicitly released (every non-reserved row dropped)', async () => {
    const now = Date.now();
    await declareDibs(filePath, { orchestratorId: ORCH_A, declaredAt: now });

    await dropAllLiveEntries();

    const live = (await readDibs(filePath, now)).filter((entry) => !isReservedEntry(entry.orchestratorId));
    expect(live).toEqual([]);

    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now })).toBe(true);
  });

  it('the reserved row is exempt from pruneStale exactly like the three existing reserved ids', async () => {
    const declaredAt = 1_000_000;
    const wellPastThreshold = declaredAt + 20 * 60 * 1000;

    // Seeded through `declareDibs` directly — the same shape
    // coordination-file.jest.spec.mjs uses to prove the AIMD reserved row's
    // exemption, so this asserts the GENERIC isReservedEntry/pruneStale
    // exemption applies to this specific id, independent of the new API.
    await declareDibs(
      filePath,
      { orchestratorId: EXPECTED_EVER_OPTED_IN_RESERVED_ID, orchestratorIds: [ORCH_A], declaredAt },
      { livenessThresholdMs: LIVENESS_THRESHOLD_MS },
    );

    const survivors = await readDibs(filePath, wellPastThreshold, { livenessThresholdMs: LIVENESS_THRESHOLD_MS });

    expect(survivors.map((entry) => entry.orchestratorId)).toContain(EXPECTED_EVER_OPTED_IN_RESERVED_ID);
    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now: wellPastThreshold })).toBe(true);
  });
});

describe('Phase 1 — hasEverOptedIn fails OPEN and stays cheap (design decision 3, not-opted-in tier)', () => {
  it('returns false for an id that never declared dibs, against a populated file', async () => {
    const now = Date.now();
    await declareDibs(filePath, { orchestratorId: ORCH_A, declaredAt: now });

    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_B, { now })).toBe(false);
  });

  it('cold start: returns false without throwing when the coordination file does not exist', async () => {
    expect(existsSync(filePath)).toBe(false);

    await expect(coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now: Date.now() })).resolves.toBe(false);

    // Zero cost: a read must never create the file (nor its .lock sibling) —
    // a session that never touches ARM leaves no residue at all.
    expect(existsSync(filePath)).toBe(false);
    expect(existsSync(`${filePath}.lock`)).toBe(false);
  });

  it('corrupt/unparseable JSON: returns false and does NOT throw', async () => {
    writeFileSync(filePath, '{ this is not valid json', 'utf8');

    await expect(coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now: Date.now() })).resolves.toBe(false);
  });

  it('truncated mid-array JSON (a torn write): returns false and does NOT throw', async () => {
    writeFileSync(filePath, '[{"orchestratorId":"__ever-opted-in__","orchestratorIds":["ses', 'utf8');

    await expect(coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now: Date.now() })).resolves.toBe(false);
  });

  it('valid JSON of the wrong top-level shape: returns false and does NOT throw', async () => {
    writeFileSync(filePath, '{"orchestratorIds":["session-x"]}', 'utf8');

    await expect(coordinationFile.hasEverOptedIn(filePath, 'session-x', { now: Date.now() })).resolves.toBe(false);
  });

  it('reserved row present but its set field is a non-array: returns false and does NOT throw', async () => {
    writeFileSync(
      filePath,
      JSON.stringify([
        { orchestratorId: EXPECTED_EVER_OPTED_IN_RESERVED_ID, orchestratorIds: 'session-x', declaredAt: 1 },
      ]),
      'utf8',
    );

    await expect(coordinationFile.hasEverOptedIn(filePath, 'session-x', { now: Date.now() })).resolves.toBe(false);
  });

  it('is lock-free: answers while another writer holds the coordination lock', async () => {
    const now = Date.now();
    await declareDibs(filePath, { orchestratorId: ORCH_A, declaredAt: now });

    // The hook calls this on every gated tool call; contending for the lock
    // would stack the module's 10-20s retry ceiling onto every spawn.
    await fsPromises.writeFile(
      `${filePath}.lock`,
      JSON.stringify({ pid: process.pid, acquiredAt: Date.now(), token: 'someone-elses-lock' }),
      'utf8',
    );

    await expect(coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now })).resolves.toBe(true);
  }, 10_000);
});

describe('Phase 1 — recordEverOptedIn is idempotent and does not grow per call', () => {
  it('returns true on the first record and false on a repeat for the same id', async () => {
    const now = Date.now();

    await expect(coordinationFile.recordEverOptedIn(filePath, ORCH_A, { now })).resolves.toBe(true);
    await expect(coordinationFile.recordEverOptedIn(filePath, ORCH_A, { now })).resolves.toBe(false);

    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now })).toBe(true);
  });

  // Convergence Analysis flagged unbounded growth of this never-pruned set as a
  // real risk (it directly threatens the "zero cost when not opted in" claim,
  // since even the read path's cost grows with the serialized size). This test
  // proves NO growth for repeat opt-ins of the SAME id — membership, not an
  // append-per-call log. Bounding growth across many DISTINCT ids is the
  // separate question the review forced; it is pinned by the "the set is
  // BOUNDED" describe block at the bottom of this file.
  it('20 repeat opt-ins of the same id leave a set of exactly one, in exactly one row', async () => {
    const now = Date.now();

    for (let beat = 0; beat < 20; beat += 1) {
      await declareDibs(filePath, { orchestratorId: ORCH_A, declaredAt: now + beat });
      await coordinationFile.recordEverOptedIn(filePath, ORCH_A, { now: now + beat });
    }

    expect(await readRecordedIds()).toEqual([ORCH_A]);
    expect(await readEverOptedInRows()).toHaveLength(1);
  }, 20_000);

  it('records an id that has never called declareDibs at all', async () => {
    const now = Date.now();

    await coordinationFile.recordEverOptedIn(filePath, ORCH_B, { now });

    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_B, { now })).toBe(true);
    expect((await readDibs(filePath, now)).filter((entry) => !isReservedEntry(entry.orchestratorId))).toEqual([]);
  });
});

describe('Phase 1 — concurrent first-time opt-ins union rather than clobber', () => {
  // Convergence Analysis edge case 3: the union must happen inside the SAME
  // lock acquisition that writes it. An implementation that reads the file,
  // computes the union, and only THEN takes the lock (or takes two separate
  // locks to read and to write) loses whichever writer read first — a classic
  // lost update. Both assertions below fail against that naive shape and pass
  // against a proper read-union-write critical section.
  it('two near-simultaneous first-time recordEverOptedIn calls both end up recorded', async () => {
    const now = Date.now();

    await Promise.all([
      coordinationFile.recordEverOptedIn(filePath, ORCH_A, { now }),
      coordinationFile.recordEverOptedIn(filePath, ORCH_B, { now }),
    ]);

    expect(new Set(await readRecordedIds())).toEqual(new Set([ORCH_A, ORCH_B]));
    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now })).toBe(true);
    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_B, { now })).toBe(true);
    expect(await readEverOptedInRows()).toHaveLength(1);
  }, 15_000);

  it('twelve concurrent first-time recordEverOptedIn calls all survive — none silently dropped', async () => {
    const now = Date.now();
    const ids = Array.from({ length: 12 }, (_unused, index) => `session-concurrent-${index}`);

    await Promise.all(ids.map((id) => coordinationFile.recordEverOptedIn(filePath, id, { now })));

    const recorded = await readRecordedIds();
    expect(new Set(recorded)).toEqual(new Set(ids));
    expect(recorded).toHaveLength(ids.length);
    expect(await readEverOptedInRows()).toHaveLength(1);
  }, 30_000);

  it('twelve concurrent first-time declareDibs calls record all twelve ids AND keep all twelve live rows', async () => {
    const now = Date.now();
    const ids = Array.from({ length: 12 }, (_unused, index) => `session-dibs-race-${index}`);

    await Promise.all(
      ids.map((id) =>
        declareDibs(filePath, { orchestratorId: id, declaredAt: now }, { livenessThresholdMs: LIVENESS_THRESHOLD_MS }),
      ),
    );

    // Both effects of the SAME critical section must survive the race: the
    // pre-existing live-entry upsert, and the new set-union.
    const live = (await readDibs(filePath, now, { livenessThresholdMs: LIVENESS_THRESHOLD_MS })).filter(
      (entry) => !isReservedEntry(entry.orchestratorId),
    );
    expect(new Set(live.map((entry) => entry.orchestratorId))).toEqual(new Set(ids));
    expect(new Set(await readRecordedIds())).toEqual(new Set(ids));
    expect(await readEverOptedInRows()).toHaveLength(1);
  }, 30_000);

  it('a concurrent declareDibs and recordEverOptedIn for different ids both land', async () => {
    const now = Date.now();

    await Promise.all([
      declareDibs(filePath, { orchestratorId: ORCH_A, declaredAt: now }),
      coordinationFile.recordEverOptedIn(filePath, ORCH_B, { now }),
    ]);

    expect(new Set(await readRecordedIds())).toEqual(new Set([ORCH_A, ORCH_B]));
    expect(await readEverOptedInRows()).toHaveLength(1);
  }, 15_000);
});

describe('Phase 1 — reserved-naming collision is REJECTED, never normalized', () => {
  const COLLIDING_ID = '__my-test-run__';

  it('the colliding id really does look reserved (so the guard has something to catch)', () => {
    expect(isReservedEntry(COLLIDING_ID)).toBe(true);
  });

  it('recordEverOptedIn throws a TypeError for a __…__-shaped orchestrator id', async () => {
    await expect(coordinationFile.recordEverOptedIn(filePath, COLLIDING_ID, { now: Date.now() })).rejects.toThrow(
      TypeError,
    );
  });

  it('the rejected id is never recorded, and nothing is written for it', async () => {
    const now = Date.now();

    await expect(coordinationFile.recordEverOptedIn(filePath, COLLIDING_ID, { now })).rejects.toThrow();

    expect(await coordinationFile.hasEverOptedIn(filePath, COLLIDING_ID, { now })).toBe(false);
    expect(await readRecordedIds()).toEqual([]);
  });

  it('declareDibs with a reserved-shaped id still works (back-compat) but records nothing', async () => {
    const now = Date.now();

    // Reserved rows are legitimately seeded through declareDibs elsewhere in
    // this module's own suite (the AIMD prune-exemption test), so this must not
    // become a throwing path.
    await expect(
      declareDibs(filePath, { orchestratorId: COLLIDING_ID, declaredAt: now }),
    ).resolves.toBeUndefined();

    expect(await coordinationFile.hasEverOptedIn(filePath, COLLIDING_ID, { now })).toBe(false);
    expect(await readEverOptedInEntry()).toBeUndefined();
  });

  it('does not normalize: a colliding id is never silently rewritten onto some other recorded id', async () => {
    const now = Date.now();
    // A plausible normalization (strip the underscores) would map
    // `__my-test-run__` onto `my-test-run` — one session's opted-in state
    // leaking onto an unrelated one. Rejecting is what makes that impossible.
    await coordinationFile.recordEverOptedIn(filePath, 'my-test-run', { now });

    await expect(coordinationFile.recordEverOptedIn(filePath, COLLIDING_ID, { now })).rejects.toThrow();

    expect(await readRecordedIds()).toEqual(['my-test-run']);
    expect(await coordinationFile.hasEverOptedIn(filePath, COLLIDING_ID, { now })).toBe(false);
  });
});

describe('Phase 1 — orchestrator-id parameter validation', () => {
  it.each([
    ['an empty string', ''],
    ['a non-string number', 42],
    ['null', null],
    ['undefined', undefined],
  ])('recordEverOptedIn rejects %s with a TypeError', async (_label, badId) => {
    await expect(coordinationFile.recordEverOptedIn(filePath, badId, { now: Date.now() })).rejects.toThrow(TypeError);
  });

  it.each([
    ['an empty string', ''],
    ['a non-string number', 42],
    ['null', null],
    ['undefined', undefined],
  ])('hasEverOptedIn answers false for %s rather than throwing (read path fails open)', async (_label, badId) => {
    await expect(coordinationFile.hasEverOptedIn(filePath, badId, { now: Date.now() })).resolves.toBe(false);
  });
});

describe('Phase 1 — every touch refreshes that id’s lastSeenAt', () => {
  // The property the whole bounded-growth design rests on. If a repeat opt-in
  // did NOT refresh, a session that beats for hours would still carry its
  // FIRST beat's timestamp and would be handed straight to the age sweep — the
  // gate silently disarming mid-session, which is precisely the failure
  // exists to prevent, reintroduced one layer down.
  it('a repeat recordEverOptedIn for an already-recorded id moves its lastSeenAt forward', async () => {
    const firstBeat = 1_700_000_000_000;
    const laterBeat = firstBeat + 5 * 60 * 1000;

    await expect(coordinationFile.recordEverOptedIn(filePath, ORCH_A, { now: firstBeat })).resolves.toBe(true);
    expect(await readLastSeenAt(ORCH_A)).toBe(firstBeat);

    // Still `false` — membership is unchanged, which is what the flag reports.
    await expect(coordinationFile.recordEverOptedIn(filePath, ORCH_A, { now: laterBeat })).resolves.toBe(false);

    expect(await readLastSeenAt(ORCH_A)).toBe(laterBeat);
    expect(await readRecordedIds()).toEqual([ORCH_A]);
    expect(await readEverOptedInRows()).toHaveLength(1);
  });

  it('a repeat declareDibs for an already-recorded id moves its lastSeenAt forward too', async () => {
    const firstBeat = 1_700_000_000_000;
    const laterBeat = firstBeat + 60 * 1000;

    await declareDibs(filePath, { orchestratorId: ORCH_A, declaredAt: firstBeat });
    expect(await readLastSeenAt(ORCH_A)).toBe(firstBeat);

    await declareDibs(filePath, { orchestratorId: ORCH_A, declaredAt: laterBeat });

    expect(await readLastSeenAt(ORCH_A)).toBe(laterBeat);
    expect(await readRecordedIds()).toEqual([ORCH_A]);
  });

  it("touching one id leaves every OTHER id's lastSeenAt untouched", async () => {
    const firstBeat = 1_700_000_000_000;
    const laterBeat = firstBeat + 10 * 60 * 1000;

    await coordinationFile.recordEverOptedIn(filePath, ORCH_A, { now: firstBeat });
    await coordinationFile.recordEverOptedIn(filePath, ORCH_B, { now: laterBeat });

    expect(await readLastSeenAt(ORCH_A)).toBe(firstBeat);
    expect(await readLastSeenAt(ORCH_B)).toBe(laterBeat);
  });
});

describe('Phase 1 — the set is BOUNDED: aged out, count-capped, live ids exempt', () => {
  const MAX_RECORDS = coordinationFile.EVER_OPTED_IN_MAX_RECORDS;
  const MAX_AGE_MS = coordinationFile.EVER_OPTED_IN_MAX_AGE_MS;

  it('exports both bounds as finite, positive, named constants', () => {
    expect(Number.isFinite(MAX_RECORDS)).toBe(true);
    expect(MAX_RECORDS).toBeGreaterThan(0);
    expect(Number.isFinite(MAX_AGE_MS)).toBe(true);
    expect(MAX_AGE_MS).toBeGreaterThan(0);
  });

  // THE headline property: N distinct ids beyond the bound leaves a BOUNDED
  // row, not an ever-growing one. Driven through the real API — every id here
  // is recorded by an actual locked read-union-write, not seeded on disk — so
  // this fails against any implementation that only bounds on a seeded path.
  it('recording MAX_RECORDS + 8 distinct ids leaves at most MAX_RECORDS records in exactly one row', async () => {
    const now = 1_700_000_000_000;
    const ids = Array.from({ length: MAX_RECORDS + 8 }, (_unused, index) => `session-bounded-${index}`);

    for (const [index, id] of ids.entries()) {
      // Each id one millisecond newer than the last, so "oldest first" has an
      // unambiguous meaning for the eviction assertions below.
      await coordinationFile.recordEverOptedIn(filePath, id, { now: now + index });
    }

    const recorded = await readRecordedIds();
    expect(recorded.length).toBeLessThanOrEqual(MAX_RECORDS);
    expect(recorded).toHaveLength(MAX_RECORDS);
    expect(new Set(recorded).size).toBe(recorded.length);
    expect(await readEverOptedInRows()).toHaveLength(1);

    // Oldest-first: the 8 earliest ids are the ones gone, the newest survive.
    const evicted = ids.slice(0, 8);
    const survivors = ids.slice(8);
    expect(new Set(recorded)).toEqual(new Set(survivors));
    for (const id of evicted) {
      expect(await coordinationFile.hasEverOptedIn(filePath, id, { now })).toBe(false);
    }
    expect(await coordinationFile.hasEverOptedIn(filePath, ids.at(-1), { now })).toBe(true);
  }, 120_000);

  it('a recently-touched id is NOT the victim even though it was the first ever recorded', async () => {
    const now = 1_700_000_000_000;

    // ORCH_A is the oldest MEMBER (recorded first) but the newest TOUCH: a
    // long-running session that has been beating since before everything else
    // in the set. An "evict in insertion order" or "evict by firstSeen"
    // implementation drops it here; an oldest-`lastSeenAt`-first one cannot.
    const others = Array.from({ length: MAX_RECORDS - 1 }, (_unused, index) => ({
      id: `session-filler-${index}`,
      lastSeenAt: now - (MAX_RECORDS - index) * 1000,
    }));
    await seedRows([
      {
        orchestratorId: EXPECTED_EVER_OPTED_IN_RESERVED_ID,
        everOptedIn: [{ id: ORCH_A, lastSeenAt: now - 10_000 }, ...others],
        declaredAt: now,
      },
    ]);

    // ORCH_A beats again (newest lastSeenAt), then a brand-new id arrives and
    // pushes the set over the cap, forcing exactly one eviction.
    await coordinationFile.recordEverOptedIn(filePath, ORCH_A, { now: now + 1 });
    await coordinationFile.recordEverOptedIn(filePath, ORCH_B, { now: now + 2 });

    const recorded = await readRecordedIds();
    expect(recorded).toHaveLength(MAX_RECORDS);
    expect(recorded).toContain(ORCH_A);
    expect(recorded).toContain(ORCH_B);
    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now })).toBe(true);

    // The genuine oldest went instead.
    expect(recorded).not.toContain('session-filler-0');
  }, 20_000);

  it('the id being touched is never evicted even when the set is already at the cap', async () => {
    const now = 1_700_000_000_000;
    const filler = Array.from({ length: MAX_RECORDS }, (_unused, index) => ({
      id: `session-filler-${index}`,
      lastSeenAt: now - 1000,
    }));
    await seedRows([
      { orchestratorId: EXPECTED_EVER_OPTED_IN_RESERVED_ID, everOptedIn: filler, declaredAt: now },
    ]);

    await expect(coordinationFile.recordEverOptedIn(filePath, ORCH_A, { now })).resolves.toBe(true);

    const recorded = await readRecordedIds();
    expect(recorded).toHaveLength(MAX_RECORDS);
    expect(recorded).toContain(ORCH_A);
    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now })).toBe(true);
  });

  it('ages out an id untouched for longer than MAX_AGE_MS, keeping one just inside the horizon', async () => {
    const now = 1_700_000_000_000;
    const staleId = 'session-stale';
    const freshId = 'session-just-inside';

    await seedRows([
      {
        orchestratorId: EXPECTED_EVER_OPTED_IN_RESERVED_ID,
        everOptedIn: [
          { id: staleId, lastSeenAt: now - MAX_AGE_MS - 1 },
          { id: freshId, lastSeenAt: now - MAX_AGE_MS + 1 },
        ],
        declaredAt: now - MAX_AGE_MS,
      },
    ]);

    // Both are readable before any write — ageing is a write-time sweep.
    expect(await coordinationFile.hasEverOptedIn(filePath, staleId, { now })).toBe(true);

    await coordinationFile.recordEverOptedIn(filePath, ORCH_A, { now });

    expect(new Set(await readRecordedIds())).toEqual(new Set([freshId, ORCH_A]));
    expect(await coordinationFile.hasEverOptedIn(filePath, staleId, { now })).toBe(false);
    expect(await coordinationFile.hasEverOptedIn(filePath, freshId, { now })).toBe(true);
  });

  it('a lastSeenAt in the future (clock skew between orchestrators) is kept, not aged out', async () => {
    const now = 1_700_000_000_000;
    const skewedId = 'session-clock-skewed';

    await seedRows([
      {
        orchestratorId: EXPECTED_EVER_OPTED_IN_RESERVED_ID,
        everOptedIn: [{ id: skewedId, lastSeenAt: now + 60 * 60 * 1000 }],
        declaredAt: now,
      },
    ]);

    await coordinationFile.recordEverOptedIn(filePath, ORCH_A, { now });

    expect(await coordinationFile.hasEverOptedIn(filePath, skewedId, { now })).toBe(true);
  });
});

describe('Phase 1 — back-compat: the legacy orchestratorIds row is read, then migrated', () => {
  // The legacy shape shipped earlier on THIS branch, so it can plausibly be
  // sitting on disk from a partial run. Reading it must keep working, and the
  // next write must migrate it — never error, never silently discard it.
  const LEGACY_DECLARED_AT = 1_700_000_000_000;

  async function seedLegacyRow(ids) {
    await seedRows([
      { orchestratorId: EXPECTED_EVER_OPTED_IN_RESERVED_ID, orchestratorIds: ids, declaredAt: LEGACY_DECLARED_AT },
    ]);
  }

  it('hasEverOptedIn reads a legacy row correctly for every member, and only its members', async () => {
    await seedLegacyRow([ORCH_A, ORCH_B]);

    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now: Date.now() })).toBe(true);
    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_B, { now: Date.now() })).toBe(true);
    expect(await coordinationFile.hasEverOptedIn(filePath, 'session-never-seen', { now: Date.now() })).toBe(false);
  });

  it('the next write migrates legacy members into per-id records and drops the legacy field', async () => {
    const now = LEGACY_DECLARED_AT + 60 * 1000;
    await seedLegacyRow([ORCH_A, ORCH_B]);

    const newId = 'session-post-migration';
    await coordinationFile.recordEverOptedIn(filePath, newId, { now });

    const entry = await readEverOptedInEntry();
    expect(entry.orchestratorIds).toBeUndefined();
    expect(Array.isArray(entry.everOptedIn)).toBe(true);
    expect(new Set(await readRecordedIds())).toEqual(new Set([ORCH_A, ORCH_B, newId]));

    // Migrated members are dated as freshly seen, so the very write that
    // migrates them can never also age them out.
    for (const record of entry.everOptedIn) {
      expect(Number.isFinite(record.lastSeenAt)).toBe(true);
    }
    expect(await readLastSeenAt(ORCH_A)).toBe(now);
    expect(await readLastSeenAt(ORCH_B)).toBe(now);

    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now })).toBe(true);
    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_B, { now })).toBe(true);
    expect(await readEverOptedInRows()).toHaveLength(1);
  });

  it('declareDibs also migrates a legacy row, preserving its members alongside the new id', async () => {
    const now = LEGACY_DECLARED_AT + 60 * 1000;
    await seedLegacyRow([ORCH_A]);

    await declareDibs(filePath, { orchestratorId: ORCH_B, declaredAt: now });

    expect(new Set(await readRecordedIds())).toEqual(new Set([ORCH_A, ORCH_B]));
    expect((await readEverOptedInEntry()).orchestratorIds).toBeUndefined();
    expect(await coordinationFile.hasEverOptedIn(filePath, ORCH_A, { now })).toBe(true);
  });

  it('a repeat opt-in of a LEGACY member returns false (already recorded) and dates it now', async () => {
    const now = LEGACY_DECLARED_AT + 60 * 1000;
    await seedLegacyRow([ORCH_A]);

    await expect(coordinationFile.recordEverOptedIn(filePath, ORCH_A, { now })).resolves.toBe(false);

    expect(await readRecordedIds()).toEqual([ORCH_A]);
    expect(await readLastSeenAt(ORCH_A)).toBe(now);
  });

  it('legacy non-string members are dropped by the migration rather than persisted', async () => {
    const now = LEGACY_DECLARED_AT + 60 * 1000;
    await seedLegacyRow([ORCH_A, '', null, 42]);

    await coordinationFile.recordEverOptedIn(filePath, ORCH_B, { now });

    expect(new Set(await readRecordedIds())).toEqual(new Set([ORCH_A, ORCH_B]));
  });
});
