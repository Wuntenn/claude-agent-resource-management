// Phase 3 — the `__disk-cleanup-armed__` opt-in row, and the two
// standalone beats that are its only writers: `--enable-disk-cleanup`, which
// adds an id, and `--disable-disk-cleanup`, which takes one back out.
//
// ---------------------------------------------------------------------------
// WHY THIS FILE EXISTS (it is named by two production doc comments)
// ---------------------------------------------------------------------------
//
// `DISK_CLEANUP_ARMED_RESERVED_ID`'s doc block in `lib/coordination-file.mjs`
// makes a load-bearing claim — "`coordination-file-disk-cleanup-armed.jest
// .spec.mjs` asserts that `declareDibs` leaves this row absent" — and
// `cli.mjs`'s `diskCleanupArmedForBeat` makes another: "the production row is
// exercised directly". This file is what makes both claims true rather than
// aspirational. Test 4 is the first; the `--enable-disk-cleanup` describe
// block is the second.
//
// ---------------------------------------------------------------------------
// THE PROPERTY UNDER TEST, AND WHY IT IS WORTH A FILE
// ---------------------------------------------------------------------------
//
// The sweep this row gates is the ONLY code path in this skill that
// recursively deletes directories. `everOptedIn` — the obvious set to reuse —
// is unusable for that job, and not by a small margin: `declareDibs` unions
// the beat's own orchestrator-id into it as a SIDE EFFECT of declaring
// capacity, so every orchestrator that has ever asked for capacity is in it.
// Gating a recursive removal on that set would render "opt-in, default-inert"
// as "default-ARMED", including for the unattended launchd watchdog beat.
//
// So the invariant is a separation invariant, and separation invariants rot
// silently: nothing about the code LOOKS wrong on the day someone adds a
// second writer. Test 4 is the guard, and it asserts the negative directly
// against the real `declareDibs`, not against a description of it.
//
// The `cli.mjs` half of the same gate — its trigger, its evidence routes,
// `--advise-only`, the three emission sites — belongs to
// `cleanup-auto-trigger-outer-acceptance.jest.spec.mjs` and is deliberately
// not restated here. Since Phase 3's review round-1 High finding, that
// file drives THIS SAME production row: `diskCleanupArmedForBeat` reads
// `hasDiskCleanupArmed` in test mode too, with the test-only env flag ANDed on
// as an extra refusal rather than substituted as an alternative gate.
//
// ---------------------------------------------------------------------------
// Phase 3 review (Medium) — the DISARM half
// ---------------------------------------------------------------------------
//
// `clearDiskCleanupArmed` / `--disable-disk-cleanup` exist because arming the
// only recursively-deleting path in this skill was otherwise a one-way door
// for up to 30 days. Their describe blocks sit at the foot of this file,
// mirroring the arming ones test for test.

import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  DISK_CLEANUP_ARMED_RESERVED_ID,
  EVER_OPTED_IN_MAX_AGE_MS,
  EVER_OPTED_IN_RESERVED_ID,
  declareDibs,
  hasDiskCleanupArmed,
  hasEverOptedIn,
  recordDiskCleanupArmed,
  clearDiskCleanupArmed,
  recordEverOptedIn,
} from './lib/coordination-file.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const CLI = join(__dirname, 'cli.mjs');

let workDir;
let coordinationFilePath;

beforeEach(() => {
  workDir = mkdtempSync(join(tmpdir(), 'arm-disk-cleanup-armed-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(() => {
  rmSync(workDir, { recursive: true, force: true });
});

/** The raw on-disk entries, read independently of the module's own readers. */
function readRawEntries() {
  return JSON.parse(readFileSync(coordinationFilePath, 'utf8'));
}

/** The reserved row for `reservedId`, or `undefined` when absent. */
function findReservedRow(reservedId) {
  const parsed = readRawEntries();
  const entries = Array.isArray(parsed) ? parsed : (parsed.entries ?? []);
  return entries.find((entry) => entry?.orchestratorId === reservedId);
}

function runCli(args, extraEnv = {}) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ARM_COORDINATION_FILE: coordinationFilePath, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

// ===========================================================================
// The primitive pair.
// ===========================================================================

describe('recordDiskCleanupArmed / hasDiskCleanupArmed', () => {
  test('arming records the id, and the read answers true for it afterwards', async () => {
    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-a')).toBe(false);

    const added = await recordDiskCleanupArmed(coordinationFilePath, 'orch-a');

    expect(added).toBe(true);
    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-a')).toBe(true);
  });

  test('arming is idempotent in the SET while still refreshing the row timestamp', async () => {
    await recordDiskCleanupArmed(coordinationFilePath, 'orch-a', { now: 1_000 });
    const firstRow = findReservedRow(DISK_CLEANUP_ARMED_RESERVED_ID);

    // `false` means "was already armed", not "failed" — the distinction the
    // `--enable-disk-cleanup` beat reports back as its `armed` field.
    expect(await recordDiskCleanupArmed(coordinationFilePath, 'orch-a', { now: 9_000 })).toBe(false);

    const secondRow = findReservedRow(DISK_CLEANUP_ARMED_RESERVED_ID);
    expect(secondRow.diskCleanupArmed).toHaveLength(1);
    expect(secondRow.diskCleanupArmed[0].lastSeenAt).toBeGreaterThan(firstRow.diskCleanupArmed[0].lastSeenAt);
    // Read on the SAME simulated clock the row was written with — the read now
    // applies the 30-day horizon, so a `Date.now()` read of a row dated
    // `9_000` would (correctly) answer "expired".
    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-a', { now: 9_000 })).toBe(true);
  });

  test('arming one id never arms another', async () => {
    await recordDiskCleanupArmed(coordinationFilePath, 'orch-a');

    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-b')).toBe(false);
  });

  test('a reserved-shaped or empty orchestratorId is REJECTED, never normalized', async () => {
    // Normalizing would map distinct sessions onto one shared id — and here
    // that means arming one session's recursive sweep off another's opt-in.
    await expect(recordDiskCleanupArmed(coordinationFilePath, DISK_CLEANUP_ARMED_RESERVED_ID)).rejects.toThrow(
      TypeError,
    );
    await expect(recordDiskCleanupArmed(coordinationFilePath, '__anything__')).rejects.toThrow(TypeError);
    await expect(recordDiskCleanupArmed(coordinationFilePath, '')).rejects.toThrow(TypeError);
    await expect(recordDiskCleanupArmed(coordinationFilePath, undefined)).rejects.toThrow(TypeError);
  });

  test('every degraded read resolves false — "cannot tell" must mean "delete nothing"', async () => {
    // Missing file.
    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-a')).toBe(false);

    // Corrupt bytes. Note this is the READ path only: the write path is
    // deliberately left free to surface a corrupt file loudly (it holds the
    // lock and can repair), which is why this is the last thing asserted
    // against this file.
    writeFileSync(coordinationFilePath, 'not json at all{{{');
    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-a')).toBe(false);
  });

  test('a bad orchestratorId reads false even against a perfectly good file', async () => {
    await recordDiskCleanupArmed(coordinationFilePath, 'orch-a');

    expect(await hasDiskCleanupArmed(coordinationFilePath, '')).toBe(false);
    expect(await hasDiskCleanupArmed(coordinationFilePath, undefined)).toBe(false);
    expect(await hasDiskCleanupArmed(coordinationFilePath, DISK_CLEANUP_ARMED_RESERVED_ID)).toBe(false);
  });

  // pre-PR review (High #7) — THE GUARD, ASSERTED NON-VACUOUSLY.
  //
  // The cell above arms only `orch-a`, so `hasDiskCleanupArmed(…, '<reserved>')`
  // would answer `false` whether or not any reserved-id guard existed: the id
  // is simply not in the row. This cell hand-writes a row in which the reserved
  // id and the empty string ARE present — the corrupt/hand-edited file the
  // guard exists for — so `false` can only come from the guard itself.
  test('a reserved or empty id PRESENT in the armed row is still refused by the read guard', async () => {
    const now = 1_000_000;
    writeFileSync(
      coordinationFilePath,
      JSON.stringify([
        {
          orchestratorId: DISK_CLEANUP_ARMED_RESERVED_ID,
          diskCleanupArmed: [
            { id: DISK_CLEANUP_ARMED_RESERVED_ID, lastSeenAt: now },
            { id: '__anything__', lastSeenAt: now },
            { id: 'orch-real', lastSeenAt: now },
          ],
          declaredAt: now,
        },
      ]),
    );

    // NON-VACUITY FIRST: this hand-built row really is readable, and a normal
    // id in it really does read as armed. Without this line the three
    // refusals below could all be passing for the wrong reason (an unparseable
    // file, a mis-spelled set field) and nobody would know.
    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-real', { now })).toBe(true);

    expect(await hasDiskCleanupArmed(coordinationFilePath, DISK_CLEANUP_ARMED_RESERVED_ID, { now })).toBe(false);
    expect(await hasDiskCleanupArmed(coordinationFilePath, '__anything__', { now })).toBe(false);
    expect(await hasDiskCleanupArmed(coordinationFilePath, '', { now })).toBe(false);
  });

  // pre-PR review (High #2) — the 30-day horizon two docstrings already
  // claimed, now actually applied. Nothing but the arm/disarm handlers ever
  // rewrites this row (unlike the ever-opted-in row, which `declareDibs`
  // re-bounds on every beat), so without a read-time horizon an armed id
  // stayed armed forever.
  describe('the armed row carries the documented 30-day horizon', () => {
    test('an id armed within the horizon still reads as armed', async () => {
      const armedAt = 5_000_000_000;
      await recordDiskCleanupArmed(coordinationFilePath, 'orch-a', { now: armedAt });

      expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-a', { now: armedAt })).toBe(true);
      expect(
        await hasDiskCleanupArmed(coordinationFilePath, 'orch-a', {
          now: armedAt + EVER_OPTED_IN_MAX_AGE_MS - 1,
        }),
      ).toBe(true);
    });

    test('an id armed longer ago than the horizon reads as NOT armed', async () => {
      const armedAt = 5_000_000_000;
      await recordDiskCleanupArmed(coordinationFilePath, 'orch-a', { now: armedAt });

      // Exclusive at the horizon, matching the write-time sweep's own boundary.
      expect(
        await hasDiskCleanupArmed(coordinationFilePath, 'orch-a', {
          now: armedAt + EVER_OPTED_IN_MAX_AGE_MS,
        }),
      ).toBe(false);
      expect(
        await hasDiskCleanupArmed(coordinationFilePath, 'orch-a', {
          now: armedAt + EVER_OPTED_IN_MAX_AGE_MS * 600,
        }),
      ).toBe(false);
    });

    test('a row dated 1970 — the reproduction — does not read as armed half a century later', async () => {
      writeFileSync(
        coordinationFilePath,
        JSON.stringify([
          {
            orchestratorId: DISK_CLEANUP_ARMED_RESERVED_ID,
            diskCleanupArmed: [{ id: 'orch-ancient', lastSeenAt: 0 }],
            declaredAt: 0,
          },
        ]),
      );

      expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-ancient', { now: Date.now() })).toBe(false);
    });

    test('a FUTURE-dated arming (clock skew) stays armed rather than reading as expired', async () => {
      const now = 5_000_000_000;
      await recordDiskCleanupArmed(coordinationFilePath, 'orch-a', { now: now + 60_000 });

      expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-a', { now })).toBe(true);
    });

    test('a record with no usable timestamp reads as NOT armed — "cannot date this" means "delete nothing"', async () => {
      writeFileSync(
        coordinationFilePath,
        JSON.stringify([
          {
            orchestratorId: DISK_CLEANUP_ARMED_RESERVED_ID,
            // The legacy flat-array shape, which carries no per-id timestamp.
            orchestratorIds: ['orch-legacy'],
            declaredAt: Date.now(),
          },
        ]),
      );

      expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-legacy', { now: Date.now() })).toBe(false);
    });
  });
});

// ===========================================================================
// Test 4 — THE separation invariant. Named by `DISK_CLEANUP_ARMED_RESERVED_ID`'s
// own doc block.
// ===========================================================================

describe('the armed set is separate from the ever-opted-in set', () => {
  test('declareDibs opts the orchestrator in but leaves the disk-cleanup-armed row ABSENT', async () => {
    await declareDibs(coordinationFilePath, {
      orchestratorId: 'orch-dibs',
      desiredAgents: 2,
      declaredAt: Date.now(),
      firstDeclaredAt: Date.now(),
    });

    // The side effect that makes `everOptedIn` unusable as a destructive gate:
    // declaring capacity opted this id in, without it ever asking to.
    expect(await hasEverOptedIn(coordinationFilePath, 'orch-dibs')).toBe(true);
    expect(findReservedRow(EVER_OPTED_IN_RESERVED_ID)).toBeDefined();

    // And the whole point: it did NOT arm the sweep. The row does not exist at
    // all — not merely "exists but without this id".
    expect(findReservedRow(DISK_CLEANUP_ARMED_RESERVED_ID)).toBeUndefined();
    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-dibs')).toBe(false);
  });

  test('the two sets do not leak into one another in either direction', async () => {
    await recordEverOptedIn(coordinationFilePath, 'orch-opted');
    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-opted')).toBe(false);

    await recordDiskCleanupArmed(coordinationFilePath, 'orch-armed');
    expect(await hasEverOptedIn(coordinationFilePath, 'orch-armed')).toBe(false);

    // Both rows coexist, each holding only its own id.
    expect(await hasEverOptedIn(coordinationFilePath, 'orch-opted')).toBe(true);
    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-armed')).toBe(true);
  });
});

// ===========================================================================
// The standalone arming beat — the row's only writer.
// ===========================================================================

describe('--enable-disk-cleanup', () => {
  test('arms the id, reports armed:true once and armed:false thereafter, and exits 0', () => {
    const first = runCli(['--enable-disk-cleanup', '--orchestrator-id=orch-cli']);

    expect(first.status).toBe(0);
    expect(JSON.parse(first.stdout)).toEqual({
      type: 'disk-cleanup-armed',
      orchestratorId: 'orch-cli',
      armed: true,
    });

    const second = runCli(['--enable-disk-cleanup', '--orchestrator-id=orch-cli']);
    expect(second.status).toBe(0);
    expect(JSON.parse(second.stdout).armed).toBe(false);
  });

  test('the row it writes is the one the production reader reads', async () => {
    expect(runCli(['--enable-disk-cleanup', '--orchestrator-id=orch-cli']).status).toBe(0);

    // The seam that matters: the beat and `cli.mjs`'s own gate must agree on
    // the row shape, which a hand-written fixture could never prove.
    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-cli')).toBe(true);
  });

  test('refuses an id-less invocation — the armed set is per-orchestrator', () => {
    const result = runCli(['--enable-disk-cleanup']);

    expect(result.status).toBe(2);
    expect(result.stderr).toContain('--orchestrator-id');
  });

  test('refuses a reserved-shaped orchestrator id', () => {
    const result = runCli(['--enable-disk-cleanup', '--orchestrator-id=__ever-opted-in__']);

    expect(result.status).toBe(2);
    expect(result.stderr).toContain('reserved');
  });

  test('refuses to ride along on any other beat — arming is standalone by design', () => {
    // One from each of the earlier-running conflict blocks and one from its
    // own, so the check is not passing by accident of ordering.
    for (const other of ['--heartbeat', '--advise-only', '--watchdog-beat', '--query-capacity']) {
      const result = runCli(['--enable-disk-cleanup', '--orchestrator-id=orch-cli', other]);

      expect(result.status).toBe(2);
      expect(result.stdout).toBe('');
    }

    // And nothing was armed along the way.
    expect(runCli(['--enable-disk-cleanup', '--orchestrator-id=orch-cli']).stdout).toContain('"armed":true');
  });

  // pre-PR review (High #1) — THE WORST POSSIBLE DEFAULT, now refused.
  //
  // Dispatch tested `flags['enable-disk-cleanup'] !== undefined` and never
  // looked at the VALUE, so `--enable-disk-cleanup=false` — the exact string an
  // operator types to turn the sweep OFF — printed `{"armed":true}` and armed
  // the only recursively-deleting path in this skill. Routed through the same
  // `validateBareOrTrueFlag` contract as `--advise-only`/`--heartbeat`/
  // `--crashed`/`--record-outcome`/`--user-attests-idle`.
  test.each(['false', 'off', 'no', '0', 'TRUE', 'yes', '1'])(
    'refuses --enable-disk-cleanup=%s outright — it must never be read as "arm it"',
    async (value) => {
      const result = runCli([`--enable-disk-cleanup=${value}`, '--orchestrator-id=orch-cli']);

      expect(result.status).toBe(2);
      expect(result.stdout).toBe('');
      expect(result.stderr).toContain('--enable-disk-cleanup');
      // The property that actually matters: nothing was armed.
      expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-cli')).toBe(false);
    },
  );

  test('a bare flag and an explicit =true are BOTH still accepted', () => {
    expect(runCli(['--enable-disk-cleanup', '--orchestrator-id=orch-bare']).status).toBe(0);
    expect(runCli(['--enable-disk-cleanup=true', '--orchestrator-id=orch-eq']).status).toBe(0);
  });
});

// ===========================================================================
// Phase 3 review (Medium) — the DISARM half. `--enable-disk-cleanup`
// had no counterpart, so an armed id stayed armed for up to 30 days
// (`EVER_OPTED_IN_MAX_AGE_MS`), refreshed by every later arming call. The only
// ways out were hand-editing the shared coordination file — racing every
// concurrent beat's read — or passing `--advise-only` on every subsequent
// beat, which suppresses far more than the sweep.
// ===========================================================================

describe('clearDiskCleanupArmed', () => {
  test('disarming removes the id, and the production reader answers false immediately afterwards', async () => {
    await recordDiskCleanupArmed(coordinationFilePath, 'orch-a');
    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-a')).toBe(true);

    const removed = await clearDiskCleanupArmed(coordinationFilePath, 'orch-a');

    expect(removed).toBe(true);
    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-a')).toBe(false);
  });

  test('disarming the LAST armed id removes the whole row, leaving no residue', async () => {
    await recordDiskCleanupArmed(coordinationFilePath, 'orch-a');
    await clearDiskCleanupArmed(coordinationFilePath, 'orch-a');

    // "Row absent" is exactly the cold, never-armed state — so there is one
    // such state to reason about rather than two.
    expect(findReservedRow(DISK_CLEANUP_ARMED_RESERVED_ID)).toBeUndefined();
  });

  test('disarming one id leaves every other armed id alone', async () => {
    await recordDiskCleanupArmed(coordinationFilePath, 'orch-a');
    await recordDiskCleanupArmed(coordinationFilePath, 'orch-b');

    expect(await clearDiskCleanupArmed(coordinationFilePath, 'orch-a')).toBe(true);

    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-a')).toBe(false);
    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-b')).toBe(true);
    expect(findReservedRow(DISK_CLEANUP_ARMED_RESERVED_ID).diskCleanupArmed).toHaveLength(1);
  });

  test('it is idempotent and safe to over-call — an unarmed id, and a file with no row at all', async () => {
    // An operator reaching for this wants a destructive capability STOPPED,
    // and must never read an error as "still armed".
    expect(await clearDiskCleanupArmed(coordinationFilePath, 'orch-never-armed')).toBe(false);

    await recordDiskCleanupArmed(coordinationFilePath, 'orch-a');
    expect(await clearDiskCleanupArmed(coordinationFilePath, 'orch-b')).toBe(false);
    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-a')).toBe(true);

    expect(await clearDiskCleanupArmed(coordinationFilePath, 'orch-a')).toBe(true);
    expect(await clearDiskCleanupArmed(coordinationFilePath, 'orch-a')).toBe(false);
  });

  test('it never touches the ever-opted-in set — the separation holds in the remove direction too', async () => {
    await recordEverOptedIn(coordinationFilePath, 'orch-a');
    await recordDiskCleanupArmed(coordinationFilePath, 'orch-a');

    await clearDiskCleanupArmed(coordinationFilePath, 'orch-a');

    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-a')).toBe(false);
    // Disarming a recursive sweep must not quietly revoke an unrelated
    // capability gate.
    expect(await hasEverOptedIn(coordinationFilePath, 'orch-a')).toBe(true);
  });

  test('a reserved-shaped or empty orchestratorId is REJECTED, never normalized', async () => {
    // A normalizing disarm would let one session silently disarm another.
    await expect(clearDiskCleanupArmed(coordinationFilePath, DISK_CLEANUP_ARMED_RESERVED_ID)).rejects.toThrow(
      TypeError,
    );
    await expect(clearDiskCleanupArmed(coordinationFilePath, '__anything__')).rejects.toThrow(TypeError);
    await expect(clearDiskCleanupArmed(coordinationFilePath, '')).rejects.toThrow(TypeError);
    await expect(clearDiskCleanupArmed(coordinationFilePath, undefined)).rejects.toThrow(TypeError);
  });
});

// ===========================================================================
// The standalone disarming beat — the row's only remover.
// ===========================================================================

describe('--disable-disk-cleanup', () => {
  test('disarms the id, reports disarmed:true once and disarmed:false thereafter, and exits 0', () => {
    expect(runCli(['--enable-disk-cleanup', '--orchestrator-id=orch-cli']).status).toBe(0);

    const first = runCli(['--disable-disk-cleanup', '--orchestrator-id=orch-cli']);

    expect(first.status).toBe(0);
    expect(JSON.parse(first.stdout)).toEqual({
      type: 'disk-cleanup-disarmed',
      orchestratorId: 'orch-cli',
      disarmed: true,
    });

    const second = runCli(['--disable-disk-cleanup', '--orchestrator-id=orch-cli']);
    expect(second.status).toBe(0);
    expect(JSON.parse(second.stdout).disarmed).toBe(false);
  });

  test('the row it clears is the one the production reader reads — the full arm/disarm round trip', async () => {
    expect(runCli(['--enable-disk-cleanup', '--orchestrator-id=orch-cli']).status).toBe(0);
    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-cli')).toBe(true);

    expect(runCli(['--disable-disk-cleanup', '--orchestrator-id=orch-cli']).status).toBe(0);

    expect(await hasDiskCleanupArmed(coordinationFilePath, 'orch-cli')).toBe(false);
  });

  test('disarming against a coordination file that never existed still succeeds', () => {
    // The stand-down path must work on a host where ARM has never run, rather
    // than leaving an operator unsure whether anything is armed.
    const result = runCli(['--disable-disk-cleanup', '--orchestrator-id=orch-cli']);

    expect(result.status).toBe(0);
    expect(JSON.parse(result.stdout).disarmed).toBe(false);
  });

  test('refuses an id-less invocation — the armed set is per-orchestrator', () => {
    const result = runCli(['--disable-disk-cleanup']);

    expect(result.status).toBe(2);
    expect(result.stderr).toContain('--orchestrator-id');
  });

  test('refuses a reserved-shaped orchestrator id', () => {
    const result = runCli(['--disable-disk-cleanup', '--orchestrator-id=__ever-opted-in__']);

    expect(result.status).toBe(2);
    expect(result.stderr).toContain('reserved');
  });

  test('refuses to ride along on any other beat, including its own mirror', () => {
    for (const other of ['--heartbeat', '--advise-only', '--watchdog-beat', '--query-capacity']) {
      const result = runCli(['--disable-disk-cleanup', '--orchestrator-id=orch-cli', other]);

      expect(result.status).toBe(2);
      expect(result.stdout).toBe('');
    }

    // Arming and disarming in one invocation is incoherent whichever order
    // argv puts them in.
    const both = runCli(['--enable-disk-cleanup', '--disable-disk-cleanup', '--orchestrator-id=orch-cli']);
    expect(both.status).toBe(2);
    expect(both.stdout).toBe('');
  });

  // Value-validated for the same reason as its arming mirror, even though this
  // direction is the safe one: a flag pair whose two halves disagree about
  // what `=false` means is worse than either rule on its own.
  test.each(['false', 'off', 'no', '0'])('refuses --disable-disk-cleanup=%s outright', (value) => {
    const result = runCli([`--disable-disk-cleanup=${value}`, '--orchestrator-id=orch-cli']);

    expect(result.status).toBe(2);
    expect(result.stdout).toBe('');
    expect(result.stderr).toContain('--disable-disk-cleanup');
  });
});
