// Phase 2 — the AUTO-TRIGGER AGE THRESHOLD and its relation to the
// operator-facing CLI default.
//
// ---------------------------------------------------------------------------
// THE PROPERTY THIS FILE PROTECTS
// ---------------------------------------------------------------------------
//
// Two different actors can now recursively remove directories out of a temp
// root, and they carry different levels of consent:
//
//   * `DEFAULT_AGE_HOURS` (2h) governs the EXISTING CLI. A human typed
//     `node cleanup.mjs`, is watching the output, and can undo their mistake.
//   * `AUTO_TRIGGER_AGE_HOURS` governs the NEW AUTO-TRIGGER. Nobody typed
//     anything. It fires unattended, inside a PreToolUse beat, while an agent
//     is mid-task — quite possibly the very agent whose `cdk synth` produced
//     the `cdk.out` in question.
//
// The unattended actor must therefore be at least as CONSERVATIVE as the
// attended one: `AUTO_TRIGGER_AGE_HOURS >= DEFAULT_AGE_HOURS`. If it were ever
// the other way round, a beat would delete build output that the operator's own
// explicit command would have spared — destructive divergence, taken without
// consent, in the direction nobody would choose.
//
// WHY A NAIVE IMPLEMENTATION PASSES A WEAKER TEST. The obvious spec writes
// `expect(AUTO_TRIGGER_AGE_HOURS).toBe(6)` and `expect(DEFAULT_AGE_HOURS).toBe(2)`.
// That pins two NUMBERS and says nothing about the RELATION between them, so
// the day somebody tunes the CLI default up to 8 hours — a perfectly reasonable
// change on its own terms — the relation inverts silently and both assertions
// still pass. Worse, a spec that restates the literals rather than importing
// them keeps passing even when the module's own values move. Hence: the
// relation, asserted against the REAL exported constants, with no literal
// anywhere in the comparison. `DEFAULT_AGE_HOURS` must be PROMOTED from
// module-private to exported for that to be possible, which is itself part of
// the Phase 2 contract.
//
// The second half of the file is the behavioural consequence on a REAL
// filesystem: a candidate one hour younger than the threshold SURVIVES, one
// hour older is REMOVED. The boundary is inherited from `selectStaleTempDirs`
// (strictly-older-than, never at-or-older-than) and is deliberately re-asserted
// here at the ASYNC entry point, because a re-implementation of the comparison
// on the async path — rather than a delegation to the unmodified Phase 2
// function — is exactly how that boundary would silently flip to `<=`.
//
// ---------------------------------------------------------------------------
// RED BY CONSTRUCTION
// ---------------------------------------------------------------------------
//
// `./cleanup.mjs` exports neither `AUTO_TRIGGER_AGE_HOURS`, nor
// `DEFAULT_AGE_HOURS` (still module-private), nor `sweepStaleTempDirsAsync`.
// The named import below fails ESM link-time resolution, naming the missing
// member. Do NOT add stubs to turn any of it green.
//
// SAFETY: every directory this file creates lives under its own
// `fs.mkdtempSync` scratch root, nominated explicitly through `allowedPrefixes`
// and torn down unconditionally in `afterEach`. The process's real `$TMPDIR` is
// never read, written, or swept — the sentinel cell at the end of this file
// would fail loudly if a future edit made it so. Real removals DO happen here
// (that is the point of a real-fs cell), which is precisely why the sentinel
// matters more in this file than in its mocked siblings.

import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  realpathSync,
  rmSync,
  statSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  AUTO_TRIGGER_AGE_HOURS,
  DEFAULT_AGE_HOURS,
  sweepStaleTempDirsAsync,
} from './cleanup.mjs';

const HOUR_MS = 60 * 60 * 1000;

let scratchRoot;
let root;

beforeEach(() => {
  scratchRoot = mkdtempSync(join(tmpdir(), 'arm-age-threshold-'));
  root = realpathSync(scratchRoot);
});

afterEach(() => {
  rmSync(scratchRoot, { recursive: true, force: true });
});

/**
 * A real candidate directory with real content, backdated `ageHours` into the
 * past — BOTH the payload file and the directory itself, because the one-level
 * liveness probe (contract property 6) reads the immediate children's mtimes
 * and an un-backdated child would make every cell here a false negative.
 */
function seedCandidate(name, ageHours, fileSizeBytes = 2048) {
  const dirPath = join(root, name);
  mkdirSync(dirPath, { recursive: true });
  const filePath = join(dirPath, 'payload.bin');
  writeFileSync(filePath, Buffer.alloc(fileSizeBytes, 1));
  const backdatedSeconds = (Date.now() - ageHours * HOUR_MS) / 1000;
  utimesSync(filePath, backdatedSeconds, backdatedSeconds);
  utimesSync(dirPath, backdatedSeconds, backdatedSeconds);
  return dirPath;
}

function sweepAtAutoThreshold() {
  return sweepStaleTempDirsAsync({
    dir: root,
    ageThresholdMs: AUTO_TRIGGER_AGE_HOURS * HOUR_MS,
    now: Date.now(),
    budgetMs: 5_000,
    allowedPrefixes: [root],
  });
}

describe('the unattended threshold is never more aggressive than the attended one', () => {
  test('both constants are exported as finite positive numbers', () => {
    // `DEFAULT_AGE_HOURS` being EXPORTED at all is part of the contract: while
    // it stays module-private the relation below cannot be asserted without
    // duplicating its value, and a duplicated value is a spec that passes while
    // the code moves.
    expect(Number.isFinite(DEFAULT_AGE_HOURS)).toBe(true);
    expect(DEFAULT_AGE_HOURS).toBeGreaterThan(0);
    expect(Number.isFinite(AUTO_TRIGGER_AGE_HOURS)).toBe(true);
    expect(AUTO_TRIGGER_AGE_HOURS).toBeGreaterThan(0);
  });

  test('AUTO_TRIGGER_AGE_HOURS >= DEFAULT_AGE_HOURS — the relation, against the real constants', () => {
    // No literal appears in this comparison, by design. Tuning either constant
    // is allowed; inverting the relation between them is not.
    expect(AUTO_TRIGGER_AGE_HOURS).toBeGreaterThanOrEqual(DEFAULT_AGE_HOURS);
  });

  test('the CLI default is unchanged in behaviour by the promotion to an export (contract property 8)', () => {
    // Promoting a module-private binding to an export must not change what it
    // is. This is the one place the old private value is pinned, and it is
    // pinned as "the value the existing CLI has always used" — stated as a
    // literal here deliberately, because this cell exists precisely to catch an
    // accidental change of the CLI's own behaviour during the promotion.
    expect(DEFAULT_AGE_HOURS).toBe(2);
  });
});

describe('the async sweep honours the threshold on a REAL filesystem', () => {
  test('a candidate younger than the auto threshold survives', async () => {
    const young = seedCandidate('cdk.out-young', AUTO_TRIGGER_AGE_HOURS - 1);

    const result = await sweepAtAutoThreshold();

    expect(existsSync(young)).toBe(true);
    expect(result.removedCount).toBe(0);
    expect(result.reclaimedBytes).toBe(0);
    expect(result.errors).toEqual([]);
  });

  test('a candidate older than the auto threshold is removed, and only it', async () => {
    const old = seedCandidate('cdk.out-old', AUTO_TRIGGER_AGE_HOURS + 1);
    const young = seedCandidate('cdk.out-young', AUTO_TRIGGER_AGE_HOURS - 1);
    // A correctly-aged but WRONGLY-NAMED directory. The name filter is
    // security-relevant, not merely selective — see the anchor module's header:
    // it is what keeps an allowed-but-CONTAINING root (a CI checkout under
    // `/private/tmp`) from walking into a working tree.
    const unrelated = seedCandidate('some-real-work', AUTO_TRIGGER_AGE_HOURS + 10);

    const result = await sweepAtAutoThreshold();

    expect(existsSync(old)).toBe(false);
    expect(existsSync(young)).toBe(true);
    expect(existsSync(unrelated)).toBe(true);
    expect(result.removedCount).toBe(1);
    expect(result.reclaimedBytes).toBe(2048);
    expect(result.candidatesCapped).toBe(false);
    expect(result.budgetExhausted).toBe(false);
  });

  test('a candidate at exactly the threshold survives — strictly-older-than, inherited from selectStaleTempDirs', async () => {
    // The boundary `selectStaleTempDirs` already documents ("a directory is
    // never deleted before it has been unambiguously past the threshold for at
    // least an instant"). Re-asserted at the ASYNC entry point because a
    // re-implemented comparison on that path is exactly how `<` becomes `<=`.
    const dirPath = join(root, 'cdk.out-exact');
    mkdirSync(dirPath, { recursive: true });
    writeFileSync(join(dirPath, 'payload.bin'), Buffer.alloc(16, 1));

    const ageThresholdMs = AUTO_TRIGGER_AGE_HOURS * HOUR_MS;

    // `now` IS DERIVED FROM THE MTIME WE READ BACK, never from `Date.now()`,
    // and that inversion is what makes this cell deterministic. `utimesSync`
    // takes float SECONDS and the kernel stores nanoseconds, so the obvious
    // spelling — set the mtime to `(Date.now() - ageThresholdMs) / 1000`, then
    // pass the original `Date.now()` as `now` — round-trips through binary
    // floating point and lands a fraction of a millisecond BELOW the intended
    // value about half the time (measured: 94 below / 106 exact over 200 fresh
    // dirs on this host). When it lands below, the candidate genuinely IS older
    // than the threshold, `selectStaleTempDirs` correctly selects it, and the
    // cell fails — a flake in the fixture, not a defect in the sweep. Reading
    // the stored value back and defining `now` as `mtimeMs + ageThresholdMs`
    // puts the candidate EXACTLY on the boundary by construction, whatever the
    // filesystem's timestamp granularity, so what is under test is the
    // comparison operator itself.
    const exactSeconds = (Date.now() - ageThresholdMs) / 1000;
    utimesSync(join(dirPath, 'payload.bin'), exactSeconds, exactSeconds);
    utimesSync(dirPath, exactSeconds, exactSeconds);

    const storedMtimeMs = statSync(dirPath).mtimeMs;
    const now = storedMtimeMs + ageThresholdMs;

    const result = await sweepStaleTempDirsAsync({
      dir: root,
      ageThresholdMs,
      now,
      budgetMs: 5_000,
      allowedPrefixes: [root],
    });

    expect(existsSync(dirPath)).toBe(true);
    expect(result.removedCount).toBe(0);
  });

  test('an empty root is a clean, fully-zeroed report — "fired and found nothing" must be distinguishable from a failure', async () => {
    const result = await sweepAtAutoThreshold();

    expect(result).toEqual({
      removedCount: 0,
      reclaimedBytes: 0,
      errors: [],
      truncatedCount: 0,
      budgetExhausted: false,
      candidatesCapped: false,
      sizeUnmeasuredCount: 0,
      // Added in the Phase 2 review round; both are part of "fully zeroed" for
      // the same reason every other counter is — the caller reads them
      // unconditionally, so `undefined` and `0` must not be confusable.
      liveWriterSkippedCount: 0,
      probeTruncatedCount: 0,
      removalIssuedUnconfirmedCount: 0,
      // The one NON-zero field of a "fully zeroed" report, and deliberately so
      // (Phase 3 review round 2). This sweep LISTED the root and found
      // it empty — the single outcome that is genuinely "nothing to do", as
      // distinct from the refusals that produce an identical set of counters
      // without ever having looked at anything.
      completed: true,
    });
  });
});

describe('SENTINEL — this file, which performs REAL removals, can never reach the host temp root', () => {
  test('the process`s real os.tmpdir() is refused with zero removals and nothing under it is listed', async () => {
    // The host `$TMPDIR` holds 3,000+ real `cdk.out` dirs (see
    // `./cleanup.jest.spec.mjs`). Unlike this file's mocked siblings there is
    // no `rm` mock standing between a mistake here and the disk, so this cell
    // is the guard rail — assert it fails LOUDLY if the allowlist ever widens.
    const seededInScratch = seedCandidate('cdk.out-must-survive', AUTO_TRIGGER_AGE_HOURS + 10);

    const result = await sweepStaleTempDirsAsync({
      dir: tmpdir(),
      ageThresholdMs: AUTO_TRIGGER_AGE_HOURS * HOUR_MS,
      now: Date.now(),
      budgetMs: 5_000,
      allowedPrefixes: [root],
    });

    expect(result.removedCount).toBe(0);
    expect(result.reclaimedBytes).toBe(0);
    expect(result.errors[0].kind).toBe('confinement-refused');
    // And the refusal really was a refusal, not a no-op that happened to find
    // nothing: an eligible candidate exists on disk right now, inside the
    // scratch root the sweep was NOT pointed at.
    expect(existsSync(seededInScratch)).toBe(true);
  });
});
