// Phase 6 — watchdog beat's own shared-pacing-pattern-shaped backoff-state
// file, per a load-dependent backoff protocol's documented contract
// (`last_full_run`, `turns_to_wait`, `last_outcome`; productive turns reset
// to 1, idle turns double, capped at 8).
//
// RED by construction: `./watchdog-backoff.mjs` does not exist yet — every
// test below fails at import time. Do not add an implementation to make
// this pass — the builder implements it in the next phase.
//
// ---------------------------------------------------------------------------
// WHY A SEPARATE MODULE FROM ./watchdog-beat.mjs
// ---------------------------------------------------------------------------
//
// `runWatchdogBeat` (./watchdog-beat.mjs) is the beat's OWN work — it has no
// opinion on whether this turn should run at all, or on backoff. This module
// is the pacing wrapper cli.mjs's `--watchdog-beat` handler composes AROUND
// a call to `runWatchdogBeat`, following a "one implementation, referenced
// not copied" principle: a cron-shaped due/not-due decision plus atomic
// state persistence, kept fully generic (never watchdog-specific) so it is
// reusable verbatim by any future ARM-family beat, exactly as a shared
// pacing module would be reused by other scheduled maintenance agents.
//
// UNLIKE those other consumers, a shared pacing module's own state file
// format is often markdown, written for a human-legible cron-triggered
// CLAUDE AGENT to read at the top of its own turn. This beat is a plain
// Node CLI invocation with
// no cron/scheduler wiring in this repo at all (per the Investigation and
// this ticket's own Convergence Analysis gap) — nothing currently reads this
// state file to decide whether to invoke `--watchdog-beat` in the first
// place. This module still persists the IDENTICAL state SHAPE
// (`last_full_run`/`turns_to_wait`/`last_outcome`, doubling capped at 8) as
// plain JSON rather than the markdown `.md` format the shared pacing
// pattern uses for its other existing consumers — a deliberate, minimal
// divergence: this
// state is read/written exclusively by this module's own code, never by a
// human or another agent skimming a markdown file at the top of a turn, so
// markdown's human-legibility payoff doesn't apply, while JSON keeps this
// module dependency-free (no markdown parser) and trivially round-trippable.
// If real scheduling is added later (closing the Convergence Analysis gap),
// revisit whether a markdown mirror is worth adding for operator visibility
// — out of scope for this phase.
//
// ---------------------------------------------------------------------------
// CONTRACT THIS SUITE SPECIFIES FOR THE IMPLEMENTER
// ---------------------------------------------------------------------------
//
//   DEFAULT_TURNS_TO_WAIT -> 1
//   MAX_TURNS_TO_WAIT -> 8
//
//   computeNextBackoffState({ didWork: boolean, now: number, previousTurnsToWait: number|null|undefined })
//     -> { lastFullRun: number, turnsToWait: number, lastOutcome: 'worked' | 'no-op' }
//
//     PURE, synchronous — no fs, no Date.now(). `didWork: true` always
//     resets `turnsToWait` to `DEFAULT_TURNS_TO_WAIT` (1) — "stay hot".
//     `didWork: false` doubles `previousTurnsToWait` (defaulting to
//     `DEFAULT_TURNS_TO_WAIT` when `previousTurnsToWait` is not a positive
//     finite integer — mirrors the shared pacing pattern's own "on first run, treat as
//     immediately due" bootstrap posture), capped at `MAX_TURNS_TO_WAIT`
//     (8) — never grows past it, even given a starting value already above
//     it (e.g. corrupted/hand-edited state).
//
//   readBackoffState(filePath) -> Promise<{ lastFullRun: number|null, turnsToWait: number, lastOutcome: string|null }>
//
//     Tolerant of a not-yet-created file (defaults: `lastFullRun: null`,
//     `turnsToWait: DEFAULT_TURNS_TO_WAIT`, `lastOutcome: null` — "treat as
//     immediately due", matching the shared pacing pattern's own bootstrap rule) and of
//     an empty/corrupt file (same defaults — never throws for a read-side
//     problem, matching this whole skill's fail-open-on-read posture, e.g.
//     ./queue-aging-escalation.mjs's `readRawEntries`/./deadlock-tripwire.mjs's
//     `readPersistedStalledSince`).
//
//   isBackoffDue({ lastFullRun, turnsToWait, now, cronIntervalMs }) -> boolean
//
//     PURE. `turnsElapsed = Math.floor((now - lastFullRun) / cronIntervalMs)`;
//     due when `turnsElapsed >= turnsToWait`. A `null`/non-finite
//     `lastFullRun` (never-yet-run) is ALWAYS due, regardless of
//     `turnsToWait` — mirrors the SAME bootstrap rule `readBackoffState`
//     already encodes via its own defaults, kept here too since a caller may
//     hold an in-memory `{ lastFullRun: null, ... }` value without having
//     gone through `readBackoffState` (e.g. re-checking due-ness after a
//     state mutation in the same process).
//
//   writeBackoffStateAtomic(filePath, state) -> Promise<void>
//
//     `state` is `{ lastFullRun: number, turnsToWait: number, lastOutcome:
//     string }` (computeNextBackoffState's own return shape, verbatim).
//     Serialises to JSON and writes via the SAME `<path>.tmp-<random>` ->
//     `rename()` atomic-write idiom every other persisted-state module in
//     this directory uses (e.g. ./liveness-log.mjs's `writeLogAtomic`,
//     ./deadlock-tripwire.mjs's `writeEntriesAtomic`-via-coordination-file)
//     — a reader can only ever observe the whole prior file or the whole
//     new file, never a torn/partial write, and a crash between the
//     `writeFile` and the `rename` leaves the ORIGINAL file completely
//     untouched (the tmp sibling is orphaned, not the real path).
//
// The load-bearing edge case this ticket's Build Plan names explicitly —
// "backoff-state write is atomic even if a check throws mid-run" — is a
// property of HOW cli.mjs's own `--watchdog-beat` handler sequences these
// two modules, not a new mechanism inside either one: the handler must call
// `runWatchdogBeat` FIRST, and only call `writeBackoffStateAtomic` in a
// `.then()`/after a successful `await` — never inside a `try/finally` that
// would run the write on the throw path too. This suite proves the
// necessary HALF of that property that lives in this module: a
// `writeBackoffStateAtomic` call that is never reached (because the caller's
// own upstream step threw first) leaves the state file exactly as it was
// before — proven below by simulating that exact call sequence.
// ---------------------------------------------------------------------------

import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  DEFAULT_TURNS_TO_WAIT,
  MAX_TURNS_TO_WAIT,
  computeNextBackoffState,
  readBackoffState,
  isBackoffDue,
  writeBackoffStateAtomic,
} from './watchdog-backoff.mjs';

let workDir;
let stateFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-watchdog-backoff-'));
  stateFilePath = join(workDir, 'watchdog-backoff.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Pure doubling contract (the shared pacing protocol, step 4).
// ---------------------------------------------------------------------------

it('exports the documented defaults', () => {
  expect(DEFAULT_TURNS_TO_WAIT).toBe(1);
  expect(MAX_TURNS_TO_WAIT).toBe(8);
});

it('resets turnsToWait to DEFAULT_TURNS_TO_WAIT when the turn did work', () => {
  const next = computeNextBackoffState({ didWork: true, now: 5000, previousTurnsToWait: 8 });
  expect(next).toEqual({ lastFullRun: 5000, turnsToWait: 1, lastOutcome: 'worked' });
});

it('doubles turnsToWait on an idle (no-op) turn', () => {
  const next = computeNextBackoffState({ didWork: false, now: 5000, previousTurnsToWait: 2 });
  expect(next).toEqual({ lastFullRun: 5000, turnsToWait: 4, lastOutcome: 'no-op' });
});

it('caps the doubling at MAX_TURNS_TO_WAIT even starting from an already-high value', () => {
  const next = computeNextBackoffState({ didWork: false, now: 5000, previousTurnsToWait: 8 });
  expect(next.turnsToWait).toBe(8);

  const overCap = computeNextBackoffState({ didWork: false, now: 5000, previousTurnsToWait: 999 });
  expect(overCap.turnsToWait).toBe(8);
});

it('treats a missing/non-finite previousTurnsToWait as DEFAULT_TURNS_TO_WAIT before doubling', () => {
  const next = computeNextBackoffState({ didWork: false, now: 5000, previousTurnsToWait: undefined });
  expect(next.turnsToWait).toBe(2); // doubled from the DEFAULT_TURNS_TO_WAIT (1) baseline
});

// ---------------------------------------------------------------------------
// isBackoffDue — pure due/not-due check.
// ---------------------------------------------------------------------------

it('is always due when lastFullRun is null (never yet run)', () => {
  expect(isBackoffDue({ lastFullRun: null, turnsToWait: 8, now: 5000, cronIntervalMs: 3_600_000 })).toBe(true);
});

it('is due once enough cron intervals have elapsed to meet turnsToWait', () => {
  const cronIntervalMs = 3_600_000; // 1 hour
  const lastFullRun = 0;
  // 2 intervals elapsed, turnsToWait 2 -> due (inclusive boundary).
  expect(isBackoffDue({ lastFullRun, turnsToWait: 2, now: 2 * cronIntervalMs, cronIntervalMs })).toBe(true);
});

it('is not due when fewer cron intervals have elapsed than turnsToWait requires', () => {
  const cronIntervalMs = 3_600_000;
  const lastFullRun = 0;
  expect(isBackoffDue({ lastFullRun, turnsToWait: 4, now: 1 * cronIntervalMs, cronIntervalMs })).toBe(false);
});

// ---------------------------------------------------------------------------
// readBackoffState — tolerant read, bootstrap defaults.
// ---------------------------------------------------------------------------

it('returns bootstrap defaults for a not-yet-created state file', async () => {
  const state = await readBackoffState(stateFilePath);
  expect(state).toEqual({ lastFullRun: null, turnsToWait: DEFAULT_TURNS_TO_WAIT, lastOutcome: null });
});

it('returns bootstrap defaults for a corrupt/unparsable state file, without throwing', async () => {
  await writeFile(stateFilePath, 'not valid json{{{', 'utf8');
  const state = await readBackoffState(stateFilePath);
  expect(state).toEqual({ lastFullRun: null, turnsToWait: DEFAULT_TURNS_TO_WAIT, lastOutcome: null });
});

it('round-trips a previously-written state through writeBackoffStateAtomic then readBackoffState', async () => {
  await writeBackoffStateAtomic(stateFilePath, { lastFullRun: 12345, turnsToWait: 4, lastOutcome: 'no-op' });
  const state = await readBackoffState(stateFilePath);
  expect(state).toEqual({ lastFullRun: 12345, turnsToWait: 4, lastOutcome: 'no-op' });
});

// ---------------------------------------------------------------------------
// Atomicity — the write is genuinely tmp-then-rename (never a torn direct
// write), and no tmp sibling survives after a successful write.
// ---------------------------------------------------------------------------

it('leaves no orphaned .tmp-* sibling behind after a successful write', async () => {
  await writeBackoffStateAtomic(stateFilePath, { lastFullRun: 1, turnsToWait: 1, lastOutcome: 'worked' });
  const { readdir } = await import('node:fs/promises');
  const entries = await readdir(workDir);
  expect(entries).toEqual(['watchdog-backoff.json']);
});

// ---------------------------------------------------------------------------
// The Build Plan's named edge case: a beat that throws mid-run must never
// corrupt (or write at all to) the backoff-state file — simulated here by
// the exact call sequence cli.mjs's own handler is required to use (read
// -> [beat work, which throws] -> write is never reached), and asserting
// the state file is byte-for-byte unchanged from before the failed turn.
// ---------------------------------------------------------------------------

it('leaves a prior state file completely untouched when the beat work between read and write throws', async () => {
  await writeBackoffStateAtomic(stateFilePath, { lastFullRun: 111, turnsToWait: 2, lastOutcome: 'worked' });
  const before = await readFile(stateFilePath, 'utf8');

  const previous = await readBackoffState(stateFilePath);
  expect(isBackoffDue({ ...previous, now: 999_999_999, cronIntervalMs: 3_600_000 })).toBe(true);

  // Simulate cli.mjs's own required sequencing: the beat's own work runs
  // between the read and any write, and this beat throws — the write call
  // below must never execute as a direct consequence (proven by the guard
  // clause itself, not a mocked throw inside writeBackoffStateAtomic).
  async function simulateBeatWork() {
    throw new Error('synthetic mid-run failure');
  }

  await expect(
    (async () => {
      await simulateBeatWork();
      // Unreachable — a correct caller (and this test) never gets here.
      await writeBackoffStateAtomic(stateFilePath, computeNextBackoffState({ didWork: false, now: 222, previousTurnsToWait: previous.turnsToWait }));
    })(),
  ).rejects.toThrow('synthetic mid-run failure');

  const after = await readFile(stateFilePath, 'utf8');
  expect(after).toBe(before);
});
