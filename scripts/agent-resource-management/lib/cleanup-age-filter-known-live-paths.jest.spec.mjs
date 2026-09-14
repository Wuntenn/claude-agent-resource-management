// Phase 2 — KNOWN-LIVE PATHS the name filter must never reach.
//
// ---------------------------------------------------------------------------
// THE PROPERTY THIS FILE PROTECTS
// ---------------------------------------------------------------------------
//
// The `cdk.out*` / `bundling-temp-*` name filter is the LAST line of defence
// on this subsystem, and the anchor module's header says so in as many words:
// confinement refuses the repository as a ROOT, not as a DESCENDANT of one. In
// the ordinary arrangement — a CI checkout under `/private/tmp`, or simply the
// host's temp root containing anything at all — `classifySweepRoot` returns
// `allowed: true` for the CONTAINING directory, and the only thing then
// standing between a recursive removal and everything else living in that root
// is which NAMES the filter is willing to match.
//
// Two categories of directory live in exactly that space on this host, right
// now, and are actively in use while the sweep runs:
//
//   1. THE AGENT SCRATCHPAD — `/private/tmp/claude-501/<project>/<session>/…`,
//      holding `scratchpad/` and `tasks/` (this very session's background-task
//      output files are there as this is written). Removing it destroys
//      in-flight agent state, including the output of the very run that
//      triggered the sweep.
//   2. JEST WORKER TEMP DIRS — `<tmp>/jest_dx/…`, `<tmp>/jest_rs/…`, and the
//      per-worker `mkdtemp` roots every spec in this repository (this one
//      included) creates. The sweep runs on a beat that fires DURING test runs.
//      Removing these corrupts unrelated suites in ways that look like flakes.
//
// WHY A NAIVE IMPLEMENTATION PASSES A WEAKER TEST. Nothing here is a bug
// today — the filter does not match these names, and a test that merely
// re-asserted today's matching rules would be a tautology. This file exists for
// the CHANGE that is easy to argue for and hard to take back: widening the
// filter. "Also sweep `tmp-*`", "also sweep anything with `temp` in it", "also
// sweep `*-build`", "match `cdk.out` anywhere in the name, not just as a
// prefix" — each is a reasonable-sounding reclaim-more-space proposal, and each
// one reaches into this list. `/private/tmp/claude-501/…` contains `tmp`.
// `jest_dx` is a temp directory by every definition except the filter's.
//
// So these cells are a RATCHET, not a characterisation: they name the real
// paths, feed them ages far past any plausible threshold (so age can never be
// what saves them), and demand a NON-match. The day somebody widens the filter,
// the failure arrives with the names of the things that were about to be
// deleted.
//
// ---------------------------------------------------------------------------
// RED BY CONSTRUCTION
// ---------------------------------------------------------------------------
//
// The `selectStaleTempDirs` cells characterise an existing, unmodified module
// and pass TODAY — deliberately, because they are the PREMISE the real-fs
// cells rest on: "the decision function refuses these names" is what makes
// "the sweep leaves them alone" meaningful rather than coincidental. The
// real-fs cells are RED, because `./cleanup.mjs` exports no
// `sweepStaleTempDirsAsync`.
//
// `./cleanup.mjs` is imported as a NAMESPACE rather than by name, precisely so
// that the red is per-cell (`cleanup.sweepStaleTempDirsAsync is not a
// function`) instead of one ESM link-time `SyntaxError` that would take the
// green premise cells down with it and report nothing about which member is
// missing. Both halves matter: the pure half pins the DECISION, the real-fs
// half pins that the decision is actually the one the async sweep acts on.
//
// SAFETY: the real-fs block builds LOOK-ALIKES of these paths inside its own
// `fs.mkdtempSync` scratch root — it never points the sweep at the genuine
// `/private/tmp/claude-501` or at a live Jest temp directory. The process's
// real `$TMPDIR` is never swept; the sentinel cell at the end pins that.

import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  realpathSync,
  rmSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, join } from 'node:path';

import { selectStaleTempDirs } from './cleanup-age-filter.mjs';
import * as cleanup from './cleanup.mjs';

const HOUR_MS = 60 * 60 * 1000;

/**
 * Real, verbatim shapes taken from this host — not invented examples. The
 * numeric UID segment (`claude-501`) and the project-slug segment are the forms
 * the agent harness actually writes, and the `jest_` prefixes are Jest's own.
 */
const KNOWN_LIVE_PATHS = [
  // The agent scratchpad, at every level a sweep could plausibly aim at.
  '/private/tmp/claude-501',
  '/private/tmp/claude-501/-Users-someuser-Sites-example-project-com',
  '/private/tmp/claude-501/-Users-someuser-Sites-example-project-com/43aacbb4-5da1-4d9d-9b5c-59c248b9bdfa',
  '/private/tmp/claude-501/-Users-someuser-Sites-example-project-com/43aacbb4-5da1-4d9d-9b5c-59c248b9bdfa/scratchpad',
  '/private/tmp/claude-501/-Users-someuser-Sites-example-project-com/43aacbb4-5da1-4d9d-9b5c-59c248b9bdfa/tasks',
  // Jest's own worker/cache temp trees.
  '/private/var/folders/ab/xyz/T/jest_dx',
  '/private/var/folders/ab/xyz/T/jest_rs',
  '/private/var/folders/ab/xyz/T/jest_dx/worker-3',
  // This repository's own spec scratch roots, which exist while the sweep runs.
  '/private/var/folders/ab/xyz/T/arm-live-writer-AbC123',
  '/private/var/folders/ab/xyz/T/arm-cleanup-autotrigger-sweep-XyZ789',
  // Other things that legitimately live in a temp root and are not build output.
  '/private/tmp/com.apple.launchd.abcdef',
  '/private/tmp/tmp-not-ours',
  '/private/tmp/temporary-work',
  '/private/var/folders/ab/xyz/T/TemporaryItems',
];

describe('the name filter never reaches the agent scratchpad or Jest worker temp dirs', () => {
  test.each(KNOWN_LIVE_PATHS)('%s is NOT selected, however old it is', (path) => {
    const now = Date.now();
    const selected = selectStaleTempDirs({
      // A YEAR old. Age can never be what saves these paths — only the name
      // filter can, which is precisely the assertion.
      entries: [{ path, mtimeMs: now - 365 * 24 * HOUR_MS }],
      now,
      ageThresholdMs: HOUR_MS,
    });

    expect(selected).toEqual([]);
  });

  test('PREMISE: the fixtures are genuinely eligible on every axis EXCEPT the name', () => {
    // Without this the block above would pass for a filter that selected
    // nothing at all, or for an age threshold that excluded everything. The
    // control below is identically aged and identically rooted, and IS selected.
    const now = Date.now();
    const control = '/private/tmp/claude-501/cdk.out-control';

    const selected = selectStaleTempDirs({
      entries: [{ path: control, mtimeMs: now - 365 * 24 * HOUR_MS }],
      now,
      ageThresholdMs: HOUR_MS,
    });

    expect(selected.map((entry) => entry.path)).toEqual([control]);
  });

  test('a widening that merely made the match a SUBSTRING would reach these paths — the ratchet, stated', () => {
    // Not an assertion about the implementation: an assertion about WHY these
    // cells exist. `cdk.out` and `bundling-temp-` are matched as ANCHORED
    // prefixes (with a separator boundary), never as substrings. This cell
    // documents what the anchoring is buying, using names that are not in the
    // list above precisely because today they do not match.
    const now = Date.now();
    const substringLookalikes = [
      '/private/tmp/claude-501/session-bundling-temp-cache',
      '/private/tmp/claude-501/backup-of-cdk.out',
      '/private/tmp/claude-501/cdk.outline-backup',
      '/private/tmp/claude-501/cdk.output-notes',
    ];

    const selected = selectStaleTempDirs({
      entries: substringLookalikes.map((path) => ({ path, mtimeMs: now - 365 * 24 * HOUR_MS })),
      now,
      ageThresholdMs: HOUR_MS,
    });

    expect(selected).toEqual([]);
  });
});

describe('the async sweep acts on that decision — look-alike live paths survive a real sweep', () => {
  let scratchRoot;
  let root;

  beforeEach(() => {
    scratchRoot = mkdtempSync(join(tmpdir(), 'arm-known-live-'));
    root = realpathSync(scratchRoot);
  });

  afterEach(() => {
    rmSync(scratchRoot, { recursive: true, force: true });
  });

  /** Backdates a whole seeded tree past any plausible threshold, deepest first. */
  function seedAncient(name) {
    const dirPath = join(root, name);
    mkdirSync(dirPath, { recursive: true });
    const payload = join(dirPath, 'payload.bin');
    writeFileSync(payload, Buffer.alloc(256, 1));
    const seconds = (Date.now() - 365 * 24 * HOUR_MS) / 1000;
    utimesSync(payload, seconds, seconds);
    utimesSync(dirPath, seconds, seconds);
    return dirPath;
  }

  test('scratchpad-, tasks- and jest-shaped directories survive while a real cdk.out-* candidate is removed', async () => {
    // The look-alikes are built INSIDE this file's own scratch root — the real
    // `/private/tmp/claude-501` tree is never pointed at, only imitated by name.
    const survivors = [
      'claude-501',
      'scratchpad',
      'tasks',
      'jest_dx',
      'jest_rs',
      'arm-live-writer-AbC123',
      'com.apple.launchd.abcdef',
      'tmp-not-ours',
      'temporary-work',
      // NOT literally `TemporaryItems`: macOS special-cases any directory of
      // that exact name under a temp root and denies even its owner
      // `readdir`/`rm` on it (EPERM, despite `drwxr-xr-x`) — verified
      // standalone, with no cleanup code in the picture. Seeding one makes this
      // file's own `afterEach` teardown throw and leaves scratch dirs behind.
      // The pure-decision block above still covers the real spelling, where it
      // costs nothing because no directory is created; the name-filter
      // behaviour under test is identical for both spellings.
      'TemporaryItems-lookalike',
    ].map(seedAncient);
    const doomed = seedAncient('cdk.out-genuine-build-output');

    const result = await cleanup.sweepStaleTempDirsAsync({
      dir: root,
      ageThresholdMs: cleanup.AUTO_TRIGGER_AGE_HOURS * HOUR_MS,
      now: Date.now(),
      budgetMs: 5_000,
      allowedPrefixes: [root],
    });

    for (const survivor of survivors) {
      expect({ name: basename(survivor), exists: existsSync(survivor) }).toEqual({
        name: basename(survivor),
        exists: true,
      });
    }
    // PREMISE: the sweep really did run and really was capable of removing
    // something in this root — otherwise every survival above is vacuous.
    expect(existsSync(doomed)).toBe(false);
    expect(result.removedCount).toBe(1);
    expect(result.reclaimedBytes).toBe(256);
    expect(result.errors).toEqual([]);
  });

  test('SENTINEL: the real host temp root — which really does contain the scratchpad — is refused outright', async () => {
    // The strongest form of this file's property. The genuine
    // `/private/tmp/claude-501` tree and this process's own Jest temp dirs live
    // under the real temp root; confinement against this file's own allowlist
    // is what keeps the sweep from ever being aimed there.
    const survivor = seedAncient('cdk.out-must-survive');

    const result = await cleanup.sweepStaleTempDirsAsync({
      dir: tmpdir(),
      ageThresholdMs: cleanup.AUTO_TRIGGER_AGE_HOURS * HOUR_MS,
      now: Date.now(),
      budgetMs: 5_000,
      allowedPrefixes: [root],
    });

    expect(result.removedCount).toBe(0);
    expect(result.reclaimedBytes).toBe(0);
    expect(result.errors[0].kind).toBe('confinement-refused');
    expect(existsSync(survivor)).toBe(true);
  });
});
