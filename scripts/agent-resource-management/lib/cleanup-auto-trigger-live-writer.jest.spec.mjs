// Phase 2 — the LIVENESS MARGIN: a bounded one-level recency probe that
// spares a candidate somebody is still writing into.
//
// ---------------------------------------------------------------------------
// THE PROPERTY THIS FILE PROTECTS
// ---------------------------------------------------------------------------
//
// Staleness is inferred from ONE number: the candidate directory's own mtime.
// On POSIX that number is wrong for the case that matters most. A directory's
// mtime changes only when its OWN entry list changes — creating, renaming or
// removing a direct child. It does NOT change when a file inside it is written
// to, nor when anything happens further down the tree. So:
//
//     mkdir cdk.out/asset      <- cdk.out's mtime set, once
//     ... 7 hours of a long `cdk synth` writing into cdk.out/asset/ ...
//     cdk.out's mtime: still 7 hours old.
//
// The sweep looks at that 7-hour-old mtime, calls it stale, and recursively
// removes the output of a build that is STILL RUNNING — destroying work, and
// doing it on an unattended beat, quite possibly for the very agent that
// triggered the beat. its attribution rule (mtime, not process ancestry)
// is the right rule and this is its known blind spot.
//
// The liveness margin closes it: before removing a candidate, probe its
// DESCENDANTS' mtimes breadth-first; if any is within `LIVE_WRITE_MARGIN_MS`,
// skip the candidate and record `kind: 'live-writer-suspected'`.
//
// DEPTH IS THE WHOLE QUESTION, and this file got it wrong once. The first
// implementation probed ONE level and argued that was sufficient: writing
// `cdk.out/asset/x.js` updates `cdk.out/asset`'s mtime, and `asset` IS an
// immediate child. True — at depth 2. At depth 3 the mutated parent is a
// GRANDCHILD (`bundling-temp-X/node_modules/pkg/chunk.js` moves `pkg`), and
// directory mtimes do not propagate upward, so a one-level probe sees a
// perfectly stale `node_modules` and the sweep deletes a live build tree. That
// shape is not hypothetical: it is esbuild / `cdk synth` bundling output, the
// exact workload targets, and it was reproduced in security review.
//
// So the probe walks, BREADTH-FIRST, bounded by `SWEEP_PROBE_MAX_ENTRIES`
// rather than by depth — because an unbounded recency probe would cost exactly
// the full-tree walk the deadline exists to avoid, on every candidate, before
// deciding whether to touch it at all. Hitting that cap degrades PRECISION, not
// progress: the candidate is still removed, and the imprecision is reported on
// `probeTruncatedCount`. Treating truncation as "assume live" would spare every
// large tree forever, which is the opposite of the point.
//
// IT IS A SKIP, NOT AN ALARM. `live-writer-suspected` means "declined, on
// purpose, and the candidate is still there" — the same category as the
// existing `size-read-failed` (see `sweepStaleTempDirs`'s doc comment on how
// `errors` CONFLATES kinds). It must not fail the beat, must not change the
// verdict type, and must not count toward `removedCount`.
//
// WHY A NAIVE IMPLEMENTATION PASSES A WEAKER TEST. The natural test seeds a
// candidate whose OWN mtime is recent and checks it survives — but such a
// candidate is never selected in the first place, so that test passes with no
// probe implemented at all. It is vacuous. The cells below are built the only
// way that is not vacuous: the candidate's own mtime is BACKDATED (so
// `selectStaleTempDirs` really does select it) while a child is fresh (so only
// the probe can save it). The premise is asserted explicitly in each cell, so
// the fixture cannot rot into vacuity unnoticed.
//
// ---------------------------------------------------------------------------
// RED BY CONSTRUCTION
// ---------------------------------------------------------------------------
//
// `./cleanup.mjs` exports neither `sweepStaleTempDirsAsync` nor
// `LIVE_WRITE_MARGIN_MS` nor `AUTO_TRIGGER_AGE_HOURS`; the named import below
// fails ESM link-time resolution, naming the missing member. Do NOT add stubs.
//
// SAFETY: REAL removals happen in this file. Every directory it creates lives
// under its own `fs.mkdtempSync` scratch root, nominated explicitly through
// `allowedPrefixes` and torn down in `afterEach`. The process's real `$TMPDIR`
// is never swept; the sentinel cell at the end pins that, and in a file that
// really deletes things that cell is the difference between a bug and an
// incident.

import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
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
  LIVE_WRITE_MARGIN_MS,
  SWEEP_PROBE_MAX_ENTRIES,
  sweepStaleTempDirsAsync,
} from './cleanup.mjs';

const HOUR_MS = 60 * 60 * 1000;

let scratchRoot;
let root;

beforeEach(() => {
  scratchRoot = mkdtempSync(join(tmpdir(), 'arm-live-writer-'));
  root = realpathSync(scratchRoot);
});

afterEach(() => {
  rmSync(scratchRoot, { recursive: true, force: true });
});

function backdate(path, ageMs) {
  const seconds = (Date.now() - ageMs) / 1000;
  utimesSync(path, seconds, seconds);
}

/**
 * A stale two-level candidate: `<name>/nested/leaf.bin`, every level backdated
 * well past the auto threshold. The caller then makes exactly one thing fresh,
 * so each cell isolates a single reason to skip or to remove.
 */
function seedStaleTree(name, { leafBytes = 4096 } = {}) {
  const candidate = join(root, name);
  const nested = join(candidate, 'nested');
  mkdirSync(nested, { recursive: true });
  const leaf = join(nested, 'leaf.bin');
  writeFileSync(leaf, Buffer.alloc(leafBytes, 1));
  const topFile = join(candidate, 'top.bin');
  writeFileSync(topFile, Buffer.alloc(leafBytes, 2));

  const staleMs = (AUTO_TRIGGER_AGE_HOURS + 4) * HOUR_MS;
  // Deepest first: backdating a parent before its children would be undone by
  // the child write.
  for (const path of [leaf, nested, topFile, candidate]) backdate(path, staleMs);

  return { candidate, nested, leaf, topFile };
}

/** Byte total computed independently of whatever the implementation sums, so this file never echoes its own arithmetic back at itself. */
function realDirSizeBytes(dirPath) {
  let total = 0;
  for (const entry of readdirSync(dirPath, { withFileTypes: true })) {
    const entryPath = join(dirPath, entry.name);
    total += entry.isDirectory() ? realDirSizeBytes(entryPath) : statSync(entryPath).size;
  }
  return total;
}

function sweep(overrides = {}) {
  return sweepStaleTempDirsAsync({
    dir: root,
    ageThresholdMs: AUTO_TRIGGER_AGE_HOURS * HOUR_MS,
    now: Date.now(),
    budgetMs: 5_000,
    allowedPrefixes: [root],
    ...overrides,
  });
}

describe('the liveness margin spares a candidate somebody is still writing into', () => {
  test('the margin constant is a positive, non-trivial duration', () => {
    expect(Number.isFinite(LIVE_WRITE_MARGIN_MS)).toBe(true);
    // A margin of a few milliseconds would satisfy "a margin exists" while
    // sparing nothing: a `cdk synth` can easily go minutes between touching a
    // given subdirectory. Pinned as a lower bound rather than an exact value so
    // the figure may be tuned upward without a spec edit.
    expect(LIVE_WRITE_MARGIN_MS).toBeGreaterThanOrEqual(60_000);
  });

  test('a stale root whose DEEP child was just written is skipped as live-writer-suspected', async () => {
    // THE CELL THIS FILE EXISTS FOR, and the exact shape of the real hazard.
    const { candidate, nested } = seedStaleTree('cdk.out-live-build');

    // A file appears inside `nested` — the ordinary act of a running build.
    // POSIX updates `nested`'s mtime (its entry list changed) and leaves the
    // CANDIDATE's own mtime untouched.
    writeFileSync(join(nested, 'just-written.js'), 'console.log(1)\n');

    // PREMISE, asserted rather than assumed — without this the cell would be
    // vacuous, passing against an implementation with no probe at all:
    //   (a) the candidate's own mtime is still stale, so it IS selected;
    //   (b) its immediate child `nested` is fresh, so only the probe can save it.
    const nowMs = Date.now();
    expect(nowMs - statSync(candidate).mtimeMs).toBeGreaterThan(AUTO_TRIGGER_AGE_HOURS * HOUR_MS);
    expect(nowMs - statSync(nested).mtimeMs).toBeLessThan(LIVE_WRITE_MARGIN_MS);

    const result = await sweep();

    expect(existsSync(candidate)).toBe(true);
    expect(result.removedCount).toBe(0);
    expect(result.reclaimedBytes).toBe(0);

    // A SKIP, NOT AN ALARM: recorded, attributed to this path, and distinct
    // from a genuine removal failure so an operator is not paged for a
    // deliberate decline.
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0]).toEqual(
      expect.objectContaining({ path: candidate, kind: 'live-writer-suspected' }),
    );
    expect(result.errors.some((error) => error.kind === 'removal-failed')).toBe(false);
    expect(result.budgetExhausted).toBe(false);
  });

  test('a stale root whose IMMEDIATE FILE child was just written is skipped too', async () => {
    const { candidate, topFile } = seedStaleTree('cdk.out-live-file');
    writeFileSync(topFile, Buffer.alloc(64, 9));

    expect(Date.now() - statSync(candidate).mtimeMs).toBeGreaterThan(AUTO_TRIGGER_AGE_HOURS * HOUR_MS);

    const result = await sweep();

    expect(existsSync(candidate)).toBe(true);
    expect(result.removedCount).toBe(0);
    expect(result.errors[0].kind).toBe('live-writer-suspected');
  });

  test('a truly stale tree is removed and its bytes reported', async () => {
    // The other half of the property: the probe must not be so cautious that
    // nothing is ever reclaimed. An implementation that skipped everything
    // would pass every cell above.
    const { candidate } = seedStaleTree('cdk.out-truly-stale');
    const expectedBytes = realDirSizeBytes(candidate);
    expect(expectedBytes).toBeGreaterThan(0);

    const result = await sweep();

    expect(existsSync(candidate)).toBe(false);
    expect(result.removedCount).toBe(1);
    expect(result.reclaimedBytes).toBe(expectedBytes);
    expect(result.errors).toEqual([]);
    expect(result.sizeUnmeasuredCount).toBe(0);
  });

  test('a skipped candidate does not stop the sweep — its stale neighbour is still removed', async () => {
    const { candidate: live, nested } = seedStaleTree('cdk.out-live-neighbour');
    writeFileSync(join(nested, 'just-written.js'), 'x\n');
    const { candidate: dead } = seedStaleTree('cdk.out-dead-neighbour');
    const deadBytes = realDirSizeBytes(dead);

    const result = await sweep();

    expect(existsSync(live)).toBe(true);
    expect(existsSync(dead)).toBe(false);
    expect(result.removedCount).toBe(1);
    expect(result.reclaimedBytes).toBe(deadBytes);
    expect(result.errors.map((error) => error.kind)).toEqual(['live-writer-suspected']);
  });

  test('a fresh GRANDCHILD spares the candidate — depth 2 is NOT the bound (the review-round regression cell)', async () => {
    // THE CELL THAT INVERTS ITS PREDECESSOR, and the reason it was inverted.
    //
    // This file previously carried a cell asserting the OPPOSITE: that a fresh
    // grandchild does NOT spare the candidate, justified as "the bound,
    // asserted as a bound". The justification was wrong, and the cell pinned a
    // live-data hazard in place.
    //
    // The original probe read one level deep and argued that was sufficient
    // because "a write moves ITS OWN parent's mtime, and that parent is an
    // immediate child of the candidate". That holds at depth 2 and nowhere
    // deeper. Writing `cand/node_modules/pkg/chunk.js` moves `cand/
    // node_modules/pkg` — a GRANDCHILD — and directory mtimes do not propagate
    // upward, so `cand/node_modules` stays exactly as old as the moment `pkg`
    // was created. A one-level probe sees nothing and the sweep recursively
    // deletes a tree being written into right now.
    //
    // That is the literal shape of esbuild / `cdk synth` bundling output, which
    // is what exists to reclaim, and it was reproduced deleting a live
    // directory in security review. The probe is now a bounded breadth-first
    // walk; the bound it still has is an ENTRY CAP, pinned by its own cell
    // below, not a depth of 1.
    const { candidate, nested } = seedStaleTree('cdk.out-deep-live');
    const deep = join(nested, 'deep');
    mkdirSync(deep, { recursive: true });
    const grandchild = join(deep, 'fresh.bin');
    writeFileSync(grandchild, Buffer.alloc(32, 3));

    // The intermediate levels are backdated AFTER the write, reproducing
    // exactly what POSIX leaves behind: only `deep` (the written file's own
    // parent) carries a fresh mtime, and nothing above it does.
    const staleMs = (AUTO_TRIGGER_AGE_HOURS + 4) * HOUR_MS;
    backdate(nested, staleMs);
    backdate(candidate, staleMs);

    // PREMISE, asserted so the cell cannot rot into vacuity:
    //   (a) the candidate IS selected (its own mtime is stale);
    //   (b) its IMMEDIATE child is stale too, so a one-level probe sees nothing;
    //   (c) the grandchild's parent is fresh, so only a deeper probe can save it.
    const nowMs = Date.now();
    expect(nowMs - statSync(candidate).mtimeMs).toBeGreaterThan(AUTO_TRIGGER_AGE_HOURS * HOUR_MS);
    expect(nowMs - statSync(nested).mtimeMs).toBeGreaterThan(LIVE_WRITE_MARGIN_MS);
    expect(nowMs - statSync(deep).mtimeMs).toBeLessThan(LIVE_WRITE_MARGIN_MS);

    const result = await sweep();

    expect(existsSync(candidate)).toBe(true);
    expect(result.removedCount).toBe(0);
    expect(result.liveWriterSkippedCount).toBe(1);
    expect(result.errors.map((error) => error.kind)).toEqual(['live-writer-suspected']);
  });

  test('the real bundling shape — a fresh file at DEPTH 3 under node_modules/<pkg>/ — spares the candidate', async () => {
    // The same property stated in the vocabulary of the actual workload, so a
    // future reader sees the scenario and not only the abstraction. Only
    // `<pkg>`'s own mtime is fresh; `node_modules` and the candidate are both
    // as stale as the backlog they sit in.
    const candidate = join(root, 'bundling-temp-live');
    const pkg = join(candidate, 'node_modules', 'some-pkg');
    mkdirSync(pkg, { recursive: true });
    writeFileSync(join(pkg, 'chunk.js'), 'export default 1\n');

    const staleMs = (AUTO_TRIGGER_AGE_HOURS + 4) * HOUR_MS;
    backdate(join(candidate, 'node_modules'), staleMs);
    backdate(candidate, staleMs);

    const nowMs = Date.now();
    expect(nowMs - statSync(candidate).mtimeMs).toBeGreaterThan(AUTO_TRIGGER_AGE_HOURS * HOUR_MS);
    expect(nowMs - statSync(join(candidate, 'node_modules')).mtimeMs).toBeGreaterThan(LIVE_WRITE_MARGIN_MS);
    expect(nowMs - statSync(pkg).mtimeMs).toBeLessThan(LIVE_WRITE_MARGIN_MS);

    const result = await sweep();

    expect(existsSync(candidate)).toBe(true);
    expect(result.removedCount).toBe(0);
    expect(result.liveWriterSkippedCount).toBe(1);
  });

  test('the probe`s bound is an ENTRY CAP, and hitting it still REMOVES — precision degrades, progress does not', async () => {
    // `SWEEP_PROBE_MAX_ENTRIES` is what keeps the probe from becoming the
    // full-tree walk the deadline exists to avoid. Truncation is deliberately
    // NOT a veto: every large `node_modules` tree exceeds the cap, so treating
    // "I ran out of entries" as "assume live" would mean the biggest
    // candidates — the ones this feature exists to reclaim — are never removed
    // at all. The candidate is removed and the imprecision is REPORTED.
    expect(Number.isInteger(SWEEP_PROBE_MAX_ENTRIES)).toBe(true);
    expect(SWEEP_PROBE_MAX_ENTRIES).toBeGreaterThan(0);

    const candidate = join(root, 'cdk.out-over-cap');
    mkdirSync(candidate, { recursive: true });
    const staleMs = (AUTO_TRIGGER_AGE_HOURS + 4) * HOUR_MS;
    for (let index = 0; index < SWEEP_PROBE_MAX_ENTRIES + 20; index += 1) {
      const leaf = join(candidate, `f${index}.bin`);
      writeFileSync(leaf, '');
      backdate(leaf, staleMs);
    }
    backdate(candidate, staleMs);

    const result = await sweep({ budgetMs: 20_000 });

    expect(existsSync(candidate)).toBe(false);
    expect(result.removedCount).toBe(1);
    expect(result.probeTruncatedCount).toBe(1);
    expect(result.liveWriterSkippedCount).toBe(0);
  });

  test('an EMPTY stale candidate has no children to probe and is removed, not skipped', async () => {
    // The degenerate cell. An implementation that treated "no child mtimes" as
    // "cannot rule out a writer" would never drain the empty directories that
    // make up much of a real backlog.
    const empty = join(root, 'cdk.out-empty');
    mkdirSync(empty, { recursive: true });
    backdate(empty, (AUTO_TRIGGER_AGE_HOURS + 4) * HOUR_MS);

    const result = await sweep();

    expect(existsSync(empty)).toBe(false);
    expect(result.removedCount).toBe(1);
    expect(result.reclaimedBytes).toBe(0);
    expect(result.errors).toEqual([]);
  });
});

describe('SENTINEL — this file, which performs REAL removals, can never reach the host temp root', () => {
  test('the process`s real os.tmpdir() is refused, while an eligible candidate sits untouched in this file`s own scratch root', async () => {
    const { candidate } = seedStaleTree('cdk.out-must-survive');

    const result = await sweepStaleTempDirsAsync({
      dir: tmpdir(),
      ageThresholdMs: AUTO_TRIGGER_AGE_HOURS * HOUR_MS,
      now: Date.now(),
      budgetMs: 5_000,
      allowedPrefixes: [root],
    });

    expect(result.removedCount).toBe(0);
    expect(result.errors[0].kind).toBe('confinement-refused');
    expect(existsSync(candidate)).toBe(true);
  });
});
