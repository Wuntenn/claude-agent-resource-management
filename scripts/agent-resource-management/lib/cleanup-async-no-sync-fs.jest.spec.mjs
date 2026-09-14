// Phase 2 — "GENUINELY ASYNC" FITNESS FUNCTION over
// `sweepStaleTempDirsAsync` (Ford/Parsons, *Building Evolutionary
// Architectures*).
//
// ---------------------------------------------------------------------------
// THE PROPERTY THIS FILE PROTECTS
// ---------------------------------------------------------------------------
//
// The sweep's deadline is only real if every step of it can be ABANDONED. A
// synchronous filesystem call cannot be: `readdirSync` on a quiet NFS mount, a
// spun-down external volume, or a `/private/tmp` shared with another user
// blocks the single JavaScript thread outright. While it blocks, no timer
// fires, no deadline race resolves, and `budgetExhausted` is a promise the code
// structurally cannot keep. The budget spec next door
// (`./cleanup-async-deadline-budget.jest.spec.mjs`) asserts the deadline
// BEHAVIOURALLY; this file asserts the STRUCTURAL precondition without which
// that behaviour is unattainable.
//
// WHY A NAIVE IMPLEMENTATION PASSES A WEAKER TEST. The tempting implementation
// of the async sweep is a thin `async` wrapper that reuses the two sync
// helpers already in `cleanup.mjs` — `listCandidateEntries` and, above all,
// `directorySizeBytes`, whose recursive walk is by far the most expensive and
// most blocking part of the whole operation:
//
//     export async function sweepStaleTempDirsAsync(args) {
//       return sweepStaleTempDirs(args);            // <- passes every
//     }                                             //    fast-mock test
//
// Against fast mocks that is indistinguishable from a correct implementation:
// same report shape, same removals, same counts, and `budgetExhausted` is
// simply never reached. It is only distinguishable when the filesystem is SLOW
// — which is precisely the condition no unit test can reproduce and the
// production host reproduces routinely. Hence a fitness function rather than
// another behavioural cell.
//
// TWO HALVES, BECAUSE EITHER ALONE IS ESCAPABLE:
//
//   1. RUNTIME (the transitive half). `node:fs` is mocked so that EVERY
//      synchronous entry point throws on contact, and the sweep is then driven
//      through a fake asynchronous tree and required to do real work anyway.
//      This catches sync usage however deeply it is buried — including via
//      `directorySizeBytes`/`listCandidateEntries`, or via a helper added
//      later that a source scan was never taught to look at. A source scan
//      alone could not.
//
//   2. SOURCE (the direct half). The async export's own body is extracted and
//      scanned for `*Sync` identifiers and for calls to the two sync helpers.
//      This catches a delegation that the runtime half might miss because the
//      sync path happened to be short-circuited by the particular fixture — and
//      it fails at the exact line a reviewer needs to read.
//
// THE ONE PERMITTED SYNCHRONOUS FILESYSTEM CALL is the Phase 1 anchor's single
// `realpathSync` (see `./cleanup-temp-root-anchor.mjs`'s "FILESYSTEM BUDGET:
// ONE READ, NOTHING ELSE"). It is bounded, it happens once per sweep BEFORE any
// walking, and it is the call that makes the confinement verdict about the real
// filesystem rather than about a string. The runtime half therefore passes
// `realpathSync` through to the genuine implementation and forbids everything
// else — an asymmetry that is deliberate and is stated here so it cannot be
// mistaken for an oversight and quietly widened.
//
// ---------------------------------------------------------------------------
// RED BY CONSTRUCTION
// ---------------------------------------------------------------------------
//
// `./cleanup.mjs` does not export `sweepStaleTempDirsAsync`. The dynamic import
// below therefore rejects at link time, and the source-scan half additionally
// finds no matching signature. Do NOT add a stub export.
//
// SAFETY: the runtime half never touches the real filesystem except through
// the anchor's `realpathSync` against an `fs.mkdtempSync` scratch root of this
// file's own making, nominated explicitly via `allowedPrefixes`. Every
// destructive primitive is a throwing mock. The real `$TMPDIR` is never read
// or written; the sentinel cell at the end of this file pins that.

import { jest } from '@jest/globals';
import * as actualFs from 'node:fs';
import { readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const CLEANUP_PATH = join(HERE, 'cleanup.mjs');

// ===========================================================================
// HALF 1 — RUNTIME: every synchronous fs entry point throws on contact.
// ===========================================================================

/** Records which forbidden sync entry points were reached, so a failure names the culprit rather than merely failing. */
const syncCalls = [];

/**
 * The full synchronous surface `cleanup.mjs` could plausibly reach for. Each
 * one BOTH records and throws: recording alone would let a `try/catch` inside
 * the implementation swallow the evidence and continue, which is exactly what
 * `directorySizeBytes` already does with its own guarded `readdirSync`.
 */
const FORBIDDEN_SYNC_ENTRY_POINTS = [
  'readdirSync',
  'statSync',
  'lstatSync',
  'rmSync',
  'rmdirSync',
  'unlinkSync',
  'opendirSync',
  'readFileSync',
  'writeFileSync',
  'appendFileSync',
  'existsSync',
  'mkdirSync',
  'mkdtempSync',
  'accessSync',
  'copyFileSync',
  'cpSync',
  'renameSync',
  'chmodSync',
  'truncateSync',
  'utimesSync',
  'readlinkSync',
];

const forbiddenSyncMocks = Object.fromEntries(
  FORBIDDEN_SYNC_ENTRY_POINTS.map((name) => [
    name,
    jest.fn(() => {
      syncCalls.push(name);
      throw new Error(`SYNC_FS_ON_ASYNC_PATH: ${name}`);
    }),
  ]),
);

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

const fsMock = {
  ...forbiddenSyncMocks,
  // THE ONE PERMITTED SYNCHRONOUS CALL — see the header. Passed through to the
  // genuine implementation so the anchor's confinement verdict is about the
  // real filesystem, not about a string.
  realpathSync: actualFs.realpathSync,
  promises: fsPromisesMock,
  constants: actualFs.constants,
};

jest.unstable_mockModule('node:fs', () => ({ ...fsMock, default: fsMock }));
jest.unstable_mockModule('node:fs/promises', () => ({ ...fsPromisesMock, default: fsPromisesMock }));

const { sweepStaleTempDirsAsync } = await import('./cleanup.mjs');

const HOUR_MS = 60 * 60 * 1000;

function direntDir(name) {
  return { name, isDirectory: () => true, isFile: () => false, isSymbolicLink: () => false };
}

function direntFile(name) {
  return { name, isDirectory: () => false, isFile: () => true, isSymbolicLink: () => false };
}

let scratchRoot;
let root;

beforeEach(() => {
  // `mkdtempSync` is taken from the REAL binding captured before the mock was
  // registered — the mocked one throws by design.
  scratchRoot = actualFs.mkdtempSync(join(tmpdir(), 'arm-no-sync-fs-'));
  root = actualFs.realpathSync(scratchRoot);
  syncCalls.length = 0;
  readdirMock.mockReset();
  statMock.mockReset();
  lstatMock.mockReset();
  rmMock.mockReset();
  rmMock.mockResolvedValue(undefined);
});

afterEach(() => {
  actualFs.rmSync(scratchRoot, { recursive: true, force: true });
});

describe('HALF 1 (runtime, transitive) — the async path reaches no synchronous fs entry point', () => {
  /** A two-level tree, so the RECURSIVE size walk — the single most likely place for a sync helper to be reused — is genuinely exercised. */
  function seedFakeTree(now) {
    const candidate = join(root, 'cdk.out-nested');
    const nested = join(candidate, 'deep');

    readdirMock.mockImplementation((dirPath) => {
      if (dirPath === root) return Promise.resolve([direntDir('cdk.out-nested')]);
      if (dirPath === candidate) return Promise.resolve([direntFile('top.bin'), direntDir('deep')]);
      if (dirPath === nested) return Promise.resolve([direntFile('deep.bin')]);
      return Promise.resolve([]);
    });
    statMock.mockImplementation((entryPath) => {
      if (entryPath.endsWith('.bin')) {
        return Promise.resolve({ size: 100, mtimeMs: now - 20 * HOUR_MS, isDirectory: () => false, isFile: () => true });
      }
      return Promise.resolve({ size: 0, mtimeMs: now - 20 * HOUR_MS, isDirectory: () => true, isFile: () => false });
    });
    return { candidate };
  }

  test('a full sweep over a nested tree completes without touching one synchronous fs call', async () => {
    const now = Date.now();
    const { candidate } = seedFakeTree(now);

    const result = await sweepStaleTempDirsAsync({
      dir: root,
      ageThresholdMs: 2 * HOUR_MS,
      now,
      budgetMs: 5_000,
      allowedPrefixes: [root],
    });

    // The assertion that fails a `return sweepStaleTempDirs(args)` delegation.
    // Named explicitly so the failure message says WHICH primitive leaked.
    expect(syncCalls).toEqual([]);
    for (const name of FORBIDDEN_SYNC_ENTRY_POINTS) {
      expect(forbiddenSyncMocks[name]).not.toHaveBeenCalled();
    }

    // PREMISE, not decoration: without this the cell would also pass for an
    // implementation that did nothing at all. Real work happened, and the
    // recursive walk really did descend a level.
    expect(result.removedCount).toBe(1);
    expect(result.reclaimedBytes).toBe(200);
    expect(result.errors).toEqual([]);
    expect(rmMock).toHaveBeenCalledWith(candidate, expect.objectContaining({ recursive: true, force: true }));
    expect(readdirMock).toHaveBeenCalledWith(join(candidate, 'deep'), expect.anything());
  });

  test('the async path uses the promise surface for listing, sizing AND removal — not just for listing', async () => {
    // A half-migrated implementation (async `readdir`, sync `statSync`/`rmSync`)
    // is the realistic partial failure, and it is strictly worse than no
    // migration: it looks async, so nobody looks again.
    const now = Date.now();
    seedFakeTree(now);

    await sweepStaleTempDirsAsync({
      dir: root,
      ageThresholdMs: 2 * HOUR_MS,
      now,
      budgetMs: 5_000,
      allowedPrefixes: [root],
    });

    expect(readdirMock).toHaveBeenCalled();
    expect(statMock).toHaveBeenCalled();
    expect(rmMock).toHaveBeenCalled();
    expect(syncCalls).toEqual([]);
  });

  test('a symlink candidate is `lstat`-ed through the PROMISE surface, never the sync one (carried forward)', async () => {
    // its rule survives the migration: a symlink's own mtime gates its own
    // removal, because `rm` on a link never follows it. The async path must
    // reach that rule through `fsPromises.lstat`, not `fs.lstatSync`.
    const now = Date.now();
    const linkPath = join(root, 'cdk.out-link');

    readdirMock.mockImplementation((dirPath) => {
      if (dirPath === root) {
        return Promise.resolve([
          { name: 'cdk.out-link', isDirectory: () => false, isFile: () => false, isSymbolicLink: () => true },
        ]);
      }
      return Promise.resolve([]);
    });
    lstatMock.mockResolvedValue({ size: 4, mtimeMs: now - 20 * HOUR_MS, isDirectory: () => false, isFile: () => false });
    statMock.mockRejectedValue(new Error('stat must not be used for a symlink candidate'));

    const result = await sweepStaleTempDirsAsync({
      dir: root,
      ageThresholdMs: 2 * HOUR_MS,
      now,
      budgetMs: 5_000,
      allowedPrefixes: [root],
    });

    expect(lstatMock).toHaveBeenCalledWith(linkPath);
    expect(syncCalls).toEqual([]);
    expect(result.removedCount).toBe(1);
    // Sized 0: removing the link frees none of the target's content, so any
    // other figure is bytes the disk never got back.
    expect(result.reclaimedBytes).toBe(0);
  });
});

// ===========================================================================
// HALF 2 — SOURCE: the async export's own body, scanned directly.
// ===========================================================================

/**
 * Blanks comments while preserving length and line structure, so tokens can
 * never be accidentally merged. Same idiom (and the same "regex, not a parser"
 * caveat) as `./cleanup-temp-root-anchor-source-scan.jest.spec.mjs` and
 * `../dead-export-fitness.jest.spec.mjs`. Load-bearing here: the async
 * function's doc comment is REQUIRED to explain why it does NOT delegate to
 * `directorySizeBytes`, so those identifiers must be legal in prose and illegal
 * in code.
 */
function stripComments(source) {
  return source.replace(/\/\*[\s\S]*?\*\/|\/\/.*$/gm, (match) => match.replace(/[^\n]/g, ' '));
}

/**
 * Extracts one function's body by brace balancing from its signature. Crude by
 * design — it runs over comment-stripped source and the target is a single
 * top-level `export async function`, which is the case this handles correctly.
 */
function extractFunctionBody(source, signaturePattern) {
  const match = source.match(signaturePattern);
  if (!match) return null;
  const openIndex = source.indexOf('{', match.index + match[0].length - 1);
  if (openIndex === -1) return null;

  let depth = 0;
  for (let index = openIndex; index < source.length; index += 1) {
    if (source[index] === '{') depth += 1;
    else if (source[index] === '}') {
      depth -= 1;
      if (depth === 0) return source.slice(openIndex, index + 1);
    }
  }
  return null;
}

describe('HALF 2 (source, direct) — the async export does not delegate to the synchronous helpers', () => {
  let codeSource;
  let asyncBody;

  beforeAll(async () => {
    codeSource = stripComments(await readFile(CLEANUP_PATH, 'utf8'));
    asyncBody = extractFunctionBody(codeSource, /export\s+async\s+function\s+sweepStaleTempDirsAsync\s*\(/);
  });

  test('the async export exists and its body can be located — the premise every cell below rests on', () => {
    // RED state (b): on `origin/master` there is no such signature, so this
    // cell fails FIRST and names the missing export rather than leaving the
    // scans below to pass vacuously against `null`.
    expect(codeSource).toMatch(/export\s+async\s+function\s+sweepStaleTempDirsAsync\s*\(/);
    expect(asyncBody).not.toBeNull();
    expect(asyncBody.length).toBeGreaterThan(100);
  });

  test('its body names no `*Sync` identifier at all', () => {
    const syncIdentifiers = [...asyncBody.matchAll(/\b([A-Za-z_$][\w$]*Sync)\b/g)].map((match) => match[1]);
    expect([...new Set(syncIdentifiers)]).toEqual([]);
  });

  test('its body calls neither `directorySizeBytes` nor `listCandidateEntries` — the two sync helpers already next door', () => {
    // The exact delegation the header describes. These helpers remain exported
    // and unchanged for the SYNC sweep (contract property 8); what is forbidden
    // is reaching them from the async path.
    expect(asyncBody).not.toMatch(/\bdirectorySizeBytes\s*\(/);
    expect(asyncBody).not.toMatch(/\blistCandidateEntries\s*\(/);
    expect(asyncBody).not.toMatch(/\bsweepStaleTempDirs\s*\(/);
  });

  test('it derives its deadline from `performance.now`, not from `Date.now` (contract property 3)', () => {
    // `Date.now()` is wall-clock: it moves under NTP correction, manual clock
    // changes and DST, so a budget built on it can expire instantly or never.
    // The monotonic clock is the only one a deadline may be built from. The
    // `now` PARAMETER remains wall-clock deliberately — it is compared against
    // file mtimes, which are wall-clock too.
    expect(asyncBody).toMatch(/performance\s*\.\s*now\s*\(/);
  });

  test('the synchronous `sweepStaleTempDirs` export is still present and untouched in shape (contract property 8)', () => {
    // The async sweep is ADDITIVE. The existing CLI path keeps its synchronous
    // implementation; nothing here is a migration of it.
    expect(codeSource).toMatch(/export\s+function\s+sweepStaleTempDirs\s*\(/);
    expect(codeSource).toMatch(/export\s+function\s+directorySizeBytes\s*\(/);
    expect(codeSource).toMatch(/export\s+function\s+listCandidateEntries\s*\(/);
  });

  test('`selectStaleTempDirs` is still imported from the unmodified Phase 2 module and not reimplemented', () => {
    // Contract property 8: the age/name decision stays in one place. A private
    // copy inside `cleanup.mjs` would let the async path drift from the sync
    // one silently — and the name filter is SECURITY-RELEVANT (see the anchor's
    // header: it is what bounds an allowed-but-containing root).
    expect(codeSource).toMatch(/from\s+['"]\.\/cleanup-age-filter\.mjs['"]/);
    expect(codeSource).toMatch(/\bselectStaleTempDirs\b/);
    expect(codeSource).not.toMatch(/function\s+selectStaleTempDirs\s*\(/);
  });

  test('the async path consults the Phase 1 anchor rather than the module-private `isConfinedToTmpRoot`', () => {
    // The whole point of Phase 1: `isConfinedToTmpRoot` measures `dir` against
    // the very TMPDIR variable `dir` derives from on the auto-trigger path, so
    // it is vacuous there. See `./cleanup-temp-root-anchor.mjs`'s header.
    //
    // EITHER IMPORT FORM SATISFIES THIS, and the DYNAMIC one is what
    // `cleanup.mjs` actually uses — deliberately, not as a stylistic
    // preference. `cleanup-temp-root-anchor.mjs` does
    // `import { realpathSync } from 'node:fs'`, while the pre-existing
    // `./cleanup-adapter.jest.spec.mjs` registers
    // `jest.unstable_mockModule('node:fs', …)` with a factory providing only
    // `readdirSync`/`statSync`/`rmSync`. A STATIC import of the anchor drags
    // that named import into `cleanup.mjs`'s module graph and kills that whole
    // suite at link time with `SyntaxError: The requested module 'node:fs'
    // does not provide an export named 'realpathSync'` — verified empirically
    // against this worktree, not theorised. It is exactly the hazard
    // `cleanup.mjs`'s own header documents for `existsSync`, one level further
    // out: the named import now sits in a DEPENDENCY, so that header's
    // namespace-import remedy cannot reach it. A memoised lazy `await import()`
    // is this repo's documented remedy for precisely this shape.
    //
    // What this assertion is for is unchanged: the anchor must be the thing
    // consulted. HOW the module reference is obtained was never the security
    // property — that `classifySweepRoot` governs the sweep is, and the two
    // assertions below are what pin it.
    expect(codeSource).toMatch(
      /(?:from|import\s*\()\s*['"]\.\/cleanup-temp-root-anchor\.mjs['"]/,
    );
    expect(asyncBody).toMatch(/\bclassifySweepRoot\s*\(/);
    expect(asyncBody).not.toMatch(/\bisConfinedToTmpRoot\s*\(/);
  });

  test('nothing on the async path writes to stdout (contract property 7)', () => {
    expect(asyncBody).not.toMatch(/process\s*\.\s*stdout/);
    expect(asyncBody).not.toMatch(/\bconsole\s*\.\s*(log|info|debug|warn|error)\b/);
  });
});

describe('SENTINEL — the async sweep is never pointed at the real host temp root', () => {
  test('the process`s real os.tmpdir() is refused against this file`s own allowlist', async () => {
    const result = await sweepStaleTempDirsAsync({
      dir: tmpdir(),
      ageThresholdMs: 2 * HOUR_MS,
      now: Date.now(),
      budgetMs: 5_000,
      allowedPrefixes: [root],
    });

    expect(result.removedCount).toBe(0);
    expect(result.errors[0].kind).toBe('confinement-refused');
    expect(rmMock).not.toHaveBeenCalled();
    expect(syncCalls).toEqual([]);
  });
});
