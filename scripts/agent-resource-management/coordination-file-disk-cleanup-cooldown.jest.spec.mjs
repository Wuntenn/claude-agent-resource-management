// Phase 3 review (Medium) — the futile-sweep COOLDOWN: the trigger's
// memory of its own last outcome.
//
// ---------------------------------------------------------------------------
// THE CASE THIS EXISTS FOR
// ---------------------------------------------------------------------------
//
// All three of the sweep's real gates (`--advise-only`, armed,
// disk-evidence) are memoryless. A host sitting stably just below the GREEN
// free-space floor, with a `$TMPDIR` that is already clean — nothing left, or
// only entries younger than `AUTO_TRIGGER_AGE_HOURS`, or all
// live-writer-skipped — satisfies all three on EVERY beat, forever, paying the
// full `SWEEP_BUDGET_MS` listing-and-probing cost each time to reclaim zero
// bytes. That is a long-running steady state, not an edge case.
//
// ---------------------------------------------------------------------------
// THE THREE PROPERTIES PINNED HERE
// ---------------------------------------------------------------------------
//
// 1. `sweepFoundNothingToDo` fires for exactly ONE outcome, and every
//    uncertainty resolves toward sweeping again. `budgetExhausted` and
//    `candidatesCapped` both mean "there is more work queued", so a sweep that
//    hits either must NOT be suppressed — that is the half of the finding a
//    naive `removedCount === 0` check would get wrong.
// 2. The cooldown row round-trips through the real coordination file.
// 3. Every degraded read FAILS OPEN to "not suppressed". This value can only
//    ever DELAY a sweep the three real gates already allowed, never authorise
//    one — so failing the other way would let one corrupt byte silently stop a
//    filling host from reclaiming anything, which is much the worse failure.
//
// The end-to-end "a clean sweep suppresses the NEXT beat" behaviour belongs to
// `cleanup-auto-trigger-outer-acceptance.jest.spec.mjs`, which drives real
// child-process beats; it is deliberately not restated here.

import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  LIVE_WRITE_MARGIN_MS,
  SWEEP_COOLDOWN_MS,
  sweepFoundNothingToDo,
  sweepStaleTempDirsAsync,
  sweepThrewReport,
} from './lib/cleanup.mjs';
import {
  DISK_CLEANUP_COOLDOWN_RESERVED_ID,
  declareDibs,
  isDiskCleanupSuppressed,
  recordDiskCleanupCooldown,
} from './lib/coordination-file.mjs';

let workDir;
let coordinationFilePath;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'arm-disk-cleanup-cooldown-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
});

/** A report that ran to completion and found nothing — the one suppressible outcome. */
function cleanEmptyReport(overrides = {}) {
  return {
    removedCount: 0,
    reclaimedBytes: 0,
    errors: [],
    truncatedCount: 0,
    budgetExhausted: false,
    candidatesCapped: false,
    liveWriterSkippedCount: 0,
    // The STRUCTURAL fact that separates "swept everything, found nothing"
    // from "refused before enumerating anything". Only the sweep's one
    // terminal return path sets it.
    completed: true,
    ...overrides,
  };
}

describe('sweepFoundNothingToDo — which outcomes may stand the trigger down', () => {
  test('a clean, complete, empty-handed sweep is the one suppressible outcome', () => {
    expect(sweepFoundNothingToDo(cleanEmptyReport())).toBe(true);
  });

  test('a sweep that REMOVED something is never suppressed — there was work, and there may be more', () => {
    expect(sweepFoundNothingToDo(cleanEmptyReport({ removedCount: 1 }))).toBe(false);
  });

  test('a BUDGET-EXHAUSTED sweep re-fires immediately: candidates were left unexamined', () => {
    expect(sweepFoundNothingToDo(cleanEmptyReport({ budgetExhausted: true }))).toBe(false);
  });

  test('a CANDIDATE-CAPPED sweep re-fires immediately: a backlog is being drained across beats by design', () => {
    expect(sweepFoundNothingToDo(cleanEmptyReport({ candidatesCapped: true }))).toBe(false);
  });

  test('both "more work queued" flags together still refuse to suppress', () => {
    expect(sweepFoundNothingToDo(cleanEmptyReport({ budgetExhausted: true, candidatesCapped: true }))).toBe(false);
  });

  test('a report that errored but still ran to completion with nothing removed IS suppressible', () => {
    // An unremovable candidate (EPERM, say) will still be unremovable next
    // beat. Re-listing it every few seconds reclaims nothing either.
    const report = cleanEmptyReport({ errors: [{ path: '/tmp/x', kind: 'removal-failed', message: 'EPERM' }] });
    expect(sweepFoundNothingToDo(report)).toBe(true);
  });

  test('a missing or shapeless report never suppresses — uncertainty resolves toward sweeping again', () => {
    expect(sweepFoundNothingToDo(undefined)).toBe(false);
    expect(sweepFoundNothingToDo(null)).toBe(false);
    expect(sweepFoundNothingToDo({})).toBe(false);
    // A report whose `removedCount` is absent or not a number is not a clean
    // empty sweep — it is an unrecognised shape, and must not stand the
    // trigger down.
    expect(sweepFoundNothingToDo({ removedCount: undefined })).toBe(false);
    expect(sweepFoundNothingToDo({ removedCount: '0' })).toBe(false);
  });

  // -------------------------------------------------------------------------
  // Phase 3 review round 2 (BOTH reviewers, top finding) — the predicate
  // must not conflate "refused before enumerating anything" with "completed
  // and found nothing".
  // -------------------------------------------------------------------------

  test('a sweep that never ran to completion is NOT suppressible, whatever its counters say', () => {
    // `completed` is the positive fact, so its ABSENCE — an older report
    // shape, a future refusal kind nobody remembered to enumerate — resolves
    // to "sweep again", not "stand down for half an hour".
    expect(sweepFoundNothingToDo(cleanEmptyReport({ completed: undefined }))).toBe(false);
    expect(sweepFoundNothingToDo(cleanEmptyReport({ completed: false }))).toBe(false);
    // Not merely truthy — `=== true`, on the same defensive footing as the
    // two "more work queued" flags.
    expect(sweepFoundNothingToDo(cleanEmptyReport({ completed: 'yes' }))).toBe(false);
    expect(sweepFoundNothingToDo(cleanEmptyReport({ completed: 1 }))).toBe(false);
  });

  test('a LIVE-WRITER-SKIPPED sweep re-fires: those trees become eligible again inside the cooldown', () => {
    // LIVE_WRITE_MARGIN_MS (15 min) is HALF SWEEP_COOLDOWN_MS (30 min), so
    // suppressing here would throttle precisely the case where retrying soon
    // is most likely to succeed.
    expect(LIVE_WRITE_MARGIN_MS).toBeLessThan(SWEEP_COOLDOWN_MS);
    expect(sweepFoundNothingToDo(cleanEmptyReport({ liveWriterSkippedCount: 1 }))).toBe(false);
    expect(sweepFoundNothingToDo(cleanEmptyReport({ liveWriterSkippedCount: 7 }))).toBe(false);
  });
});

describe('the sweep marks completion STRUCTURALLY, so refusals cannot be read as "nothing to do"', () => {
  const stale = 60_000;

  test('a real sweep of a clean root reports completed: true, and IS suppressible', async () => {
    const sweepDir = mkdtempSync(join(tmpdir(), 'arm-cooldown-clean-root-'));
    try {
      const report = await sweepStaleTempDirsAsync({ dir: sweepDir, ageThresholdMs: stale, now: Date.now() });
      expect(report.completed).toBe(true);
      expect(report.removedCount).toBe(0);
      expect(sweepFoundNothingToDo(report)).toBe(true);
    } finally {
      rmSync(sweepDir, { recursive: true, force: true });
    }
  });

  test.each([
    // The security-relevant pair first: both currently emit a visible refusal
    // on every beat, and a cooldown here would silence that signal for 30
    // minutes after the first sighting.
    [
      'confinement-refused',
      // A REAL, resolvable directory outside the platform temp allowlist —
      // the hostile/misconfigured-`TMPDIR` shape Phase 1's anchor exists to
      // catch. (It must exist, or the anchor refuses it one gate earlier,
      // under `unresolved-root-refused`.)
      () => ({ dir: process.cwd(), ageThresholdMs: stale, now: Date.now() }),
    ],
    [
      'unresolved-root-refused',
      // A sweep root that cannot be fully symlink-resolved — here because it
      // does not exist at all.
      () => ({ dir: join(workDir, 'not-a-real-root'), ageThresholdMs: stale, now: Date.now() }),
    ],
    // NOTE the sweep roots below: `workDir`, this file's own per-test
    // `mkdtempSync` scratch directory, NOT `tmpdir()` itself (pre-PR
    // review, High #8). These three cells assert a refusal that fires BEFORE
    // any enumeration — but a test whose only protection against deleting the
    // developer's (or the CI runner's) real temp directory is "the gate under
    // test still works" has no protection at all: if that gate ever regressed,
    // these cells would not merely go red, they would sweep `$TMPDIR` for real.
    // `workDir` sits under the same temp-root prefix, so it satisfies the
    // confinement allowlist identically and the refusal under test is
    // unchanged. The one root that must stay outside the allowlist is
    // `confinement-refused`'s above, which is the whole point of that case.
    [
      'now-out-of-band',
      // The clock-skew safety gate.
      () => ({ dir: workDir, ageThresholdMs: stale, now: Date.now() + 10 * 60_000 }),
    ],
    ['invalid-age-threshold', () => ({ dir: workDir, ageThresholdMs: Number.NaN, now: Date.now() })],
    ['invalid-max-candidates', () => ({ dir: workDir, ageThresholdMs: stale, now: Date.now(), maxCandidates: 1.5 })],
  ])('a %s refusal never enumerated anything, so it must NOT stand the trigger down', async (kind, buildArgs) => {
    const report = await sweepStaleTempDirsAsync(buildArgs());

    expect(report.errors.map((error) => error.kind)).toContain(kind);
    // The shape that used to fool the predicate: zero removals, neither
    // "more work queued" flag set.
    expect(report.removedCount).toBe(0);
    expect(report.budgetExhausted).toBe(false);
    expect(report.candidatesCapped).toBe(false);
    // ...and the structural fact that now distinguishes it.
    expect(report.completed).not.toBe(true);
    expect(sweepFoundNothingToDo(report)).toBe(false);
  });

  test('a thrown sweep is not a completed one either', () => {
    expect(sweepThrewReport('/tmp/x', new Error('boom')).completed).not.toBe(true);
    expect(sweepFoundNothingToDo(sweepThrewReport('/tmp/x', new Error('boom')))).toBe(false);
  });
});

describe('the cooldown row round-trips through the coordination file', () => {
  test('a recorded cooldown suppresses until its instant, and not one moment past it', async () => {
    const now = 1_000_000;
    await recordDiskCleanupCooldown(coordinationFilePath, { suppressedUntil: now + SWEEP_COOLDOWN_MS }, { now });

    expect(await isDiskCleanupSuppressed(coordinationFilePath, now, SWEEP_COOLDOWN_MS)).toBe(true);
    expect(await isDiskCleanupSuppressed(coordinationFilePath, now + SWEEP_COOLDOWN_MS - 1, SWEEP_COOLDOWN_MS)).toBe(
      true,
    );
    // Exclusive at the horizon: the beat AT `suppressedUntil` sweeps again.
    expect(await isDiskCleanupSuppressed(coordinationFilePath, now + SWEEP_COOLDOWN_MS, SWEEP_COOLDOWN_MS)).toBe(false);
    expect(await isDiskCleanupSuppressed(coordinationFilePath, now + SWEEP_COOLDOWN_MS + 1, SWEEP_COOLDOWN_MS)).toBe(
      false,
    );
  });

  test('a later cooldown REPLACES the earlier one rather than accumulating rows', async () => {
    await recordDiskCleanupCooldown(coordinationFilePath, { suppressedUntil: 5_000 }, { now: 1_000 });
    await recordDiskCleanupCooldown(coordinationFilePath, { suppressedUntil: 9_000 }, { now: 2_000 });

    const entries = JSON.parse(readFileSync(coordinationFilePath, 'utf8'));
    const cooldownRows = entries.filter((entry) => entry?.orchestratorId === DISK_CLEANUP_COOLDOWN_RESERVED_ID);
    expect(cooldownRows).toHaveLength(1);
    expect(cooldownRows[0].suppressedUntil).toBe(9_000);

    // The earlier, shorter cooldown is gone, not merely shadowed.
    expect(await isDiskCleanupSuppressed(coordinationFilePath, 8_000, SWEEP_COOLDOWN_MS)).toBe(true);
  });

  test('it does not disturb, and is not disturbed by, the ordinary dibs ledger', async () => {
    await declareDibs(coordinationFilePath, {
      orchestratorId: 'orch-a',
      desiredAgents: 2,
      declaredAt: 1_000,
      firstDeclaredAt: 1_000,
    });
    await recordDiskCleanupCooldown(coordinationFilePath, { suppressedUntil: 9_000 }, { now: 1_000 });

    expect(await isDiskCleanupSuppressed(coordinationFilePath, 2_000, SWEEP_COOLDOWN_MS)).toBe(true);

    // A subsequent real beat must not clobber the cooldown.
    await declareDibs(coordinationFilePath, {
      orchestratorId: 'orch-b',
      desiredAgents: 1,
      declaredAt: 2_000,
      firstDeclaredAt: 2_000,
    });
    expect(await isDiskCleanupSuppressed(coordinationFilePath, 2_500, SWEEP_COOLDOWN_MS)).toBe(true);
  });

  test('a non-finite suppressedUntil is REJECTED at the write, not persisted as garbage', async () => {
    await expect(recordDiskCleanupCooldown(coordinationFilePath, { suppressedUntil: NaN })).rejects.toThrow(TypeError);
    await expect(recordDiskCleanupCooldown(coordinationFilePath, { suppressedUntil: 'soon' })).rejects.toThrow(
      TypeError,
    );
  });
});

describe('every degraded state FAILS OPEN to "not suppressed"', () => {
  test('a missing file, and a file with no cooldown row', async () => {
    expect(await isDiskCleanupSuppressed(coordinationFilePath, 1_000, SWEEP_COOLDOWN_MS)).toBe(false);

    await declareDibs(coordinationFilePath, {
      orchestratorId: 'orch-a',
      desiredAgents: 1,
      declaredAt: 1_000,
      firstDeclaredAt: 1_000,
    });
    expect(await isDiskCleanupSuppressed(coordinationFilePath, 1_000, SWEEP_COOLDOWN_MS)).toBe(false);
  });

  test('corrupt bytes', async () => {
    writeFileSync(coordinationFilePath, 'not json at all{{{');

    expect(await isDiskCleanupSuppressed(coordinationFilePath, 1_000, SWEEP_COOLDOWN_MS)).toBe(false);
  });

  test('a hand-edited row with a hostile or missing suppressedUntil', async () => {
    for (const suppressedUntil of [undefined, null, 'forever', NaN, Infinity, {}]) {
      writeFileSync(
        coordinationFilePath,
        JSON.stringify([{ orchestratorId: DISK_CLEANUP_COOLDOWN_RESERVED_ID, suppressedUntil, declaredAt: 1 }]),
      );

      expect(await isDiskCleanupSuppressed(coordinationFilePath, 1_000, SWEEP_COOLDOWN_MS)).toBe(false);
    }
  });

  test('a far-future suppressedUntil is CLAMPED, so no single bad write can stand the sweep down forever', async () => {
    writeFileSync(
      coordinationFilePath,
      JSON.stringify([
        { orchestratorId: DISK_CLEANUP_COOLDOWN_RESERVED_ID, suppressedUntil: 1_000 + 365 * 24 * 3_600_000 },
      ]),
    );

    expect(await isDiskCleanupSuppressed(coordinationFilePath, 1_000, SWEEP_COOLDOWN_MS)).toBe(false);
  });

  test('a non-finite `now` or horizon reads false rather than throwing', async () => {
    await recordDiskCleanupCooldown(coordinationFilePath, { suppressedUntil: 9_000 }, { now: 1_000 });

    expect(await isDiskCleanupSuppressed(coordinationFilePath, NaN, SWEEP_COOLDOWN_MS)).toBe(false);
    expect(await isDiskCleanupSuppressed(coordinationFilePath, 1_000, NaN)).toBe(false);
    expect(await isDiskCleanupSuppressed(coordinationFilePath, undefined, undefined)).toBe(false);
  });
});
