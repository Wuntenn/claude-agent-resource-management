// Phase 2 (RED) — the deterministic, session-derived orchestrator-id.
//
// design decision 2: the PreToolUse hook derives its lookup key
// INDEPENDENTLY of the orchestrator, so unless both sides derive the SAME key
// from the SAME session id, the lookup never matches and the gate silently
// never activates for anyone — the exact failure shape as the
// `worktreeMtimeMs`-always-null gap. One exported helper, called verbatim by
// both sides, is the only thing that makes that impossible.
//
// This file is the unit-level contract for that helper. The Phase 0 outer
// acceptance test (`../hooks/pretooluse-arm-gate.outer-acceptance.jest.spec.mjs`)
// already imports `deriveOrchestratorId` from `./orchestrator-id.mjs` and calls
// it as `deriveOrchestratorId(SESSION_ID)` with a uuid-shaped string; the
// module path and call signature pinned below match that import exactly.
//
// ---------------------------------------------------------------------------
// THE CONTRACT THIS FILE PINS (unambiguous, for the Phase 2 implementer)
//
//   MODULE  `scripts/agent-resource-management/lib/orchestrator-id.mjs`
//
//   deriveOrchestratorId(sessionId) -> string
//     PURE and DETERMINISTIC: no clock, no randomness, no per-process salt,
//     no I/O. The same session id yields the same derived id in this process,
//     in the hook's child process, and on the next host boot.
//
//     THROWS `TypeError` for anything that is not a non-empty, non-blank
//     string. It must NOT fall back to a shared placeholder (the superseded
//     draft's `String(x || 'unknown-session')`): a shared fallback lets every
//     malformed-session-id caller inherit one another's opted-in / denied gate
//     state, which is a cross-session correctness bug, not a nicety. See the
//     "no shared fallback" describe block below.
//
//     RETURNS an id that is:
//       - prefixed `session-` (design decision 2's mandated convention, and
//         what SKILL.md's Phase 5 text and every `--orchestrator-id=` example
//         must be greppably consistent with);
//       - drawn only from `[A-Za-z0-9_-]` after that prefix, so it is safe
//         verbatim as a JSON string value, as a shell argv token, and as a
//         single path component (cli.mjs:1857 records that no safe
//         orchestrator-id filename-sanitization scheme exists in this skill
//         today — this helper is that scheme);
//       - length-bounded regardless of how long the input is;
//       - NEVER `isReservedEntry`-shaped (`__…__`), for ANY input, including
//         inputs made entirely of underscores — a reserved-shaped id is
//         rejected outright by `recordEverOptedIn`, `claimCapacity`,
//         `releaseCapacity`, `reserveAdmission` and cli.mjs's `--claim`, so a
//         helper that could ever emit one would hard-fail the opt-in path;
//       - INJECTIVE for realistic session-id shapes. This is the load-bearing
//         one. A naive `.replace(/[^a-zA-Z0-9_-]/g, '')` strip (the superseded
//         draft's approach) maps `sess:2026-09-05T10:00:00.000Z` and
//         `sess2026-09-05T100000000Z` — and a colon-separated uuid and its
//         bare-hex twin — onto the SAME derived id. That is not low-probability
//         noise: it means one session's sticky opted-in state, and its
//         fail-closed denials, leak onto an unrelated session. The
//         "distinctness" block below asserts concrete near-miss pairs, so a
//         lossy strip cannot pass. Deriving from a hash of the RAW id (with or
//         without a readable sanitized prefix alongside it) satisfies this;
//         a lossy strip alone does not.
//
// Nothing here asserts an exact literal output string on purpose: the
// implementer is free to choose the encoding (hash, reversible escape, or a
// readable-prefix + hash hybrid) as long as every property below holds.
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  isReservedEntry,
  recordEverOptedIn,
  hasEverOptedIn,
} from './coordination-file.mjs';
// Phase 2 — does not exist yet. This import is the expected RED failure.
import { deriveOrchestratorId } from './orchestrator-id.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const MODULE_PATH = join(HERE, 'orchestrator-id.mjs');

/** A realistic Claude Code session id — uuid-shaped, hyphenated. */
const SESSION_ID = '7c9a1f42-1b7e-4d2c-9f31-0a6b5c8e4d10';

/** The `[A-Za-z0-9_-]`-only shape every derived id must satisfy. */
const SAFE_ID = /^session-[A-Za-z0-9_-]+$/;

/**
 * Realistic session-id shapes seen or plausible in the wild: uuids (hyphenated
 * and bare), ISO timestamps with `:`/`.` separators, path- and url-ish ids,
 * and ids differing only in case or in a single removable character.
 */
const REALISTIC_SESSION_IDS = [
  '7c9a1f42-1b7e-4d2c-9f31-0a6b5c8e4d10',
  'de3f0b21-55aa-4c98-8e17-9d4c2a7f6b03',
  '7c9a1f421b7e4d2c9f310a6b5c8e4d10',
  '7c9a1f42:1b7e:4d2c:9f31:0a6b5c8e4d10',
  '7C9A1F42-1B7E-4D2C-9F31-0A6B5C8E4D10',
  'sess:2026-09-05T10:00:00.000Z',
  'sess-2026-09-05T10-00-00-000Z',
  'sess2026-09-05T100000000Z',
  'sess.2026-09-05T10.00.00.000Z',
  'session/abc/def',
  'sessionabcdef',
  'session abc def',
  '../../etc/passwd',
  'etcpasswd',
  '__ever-opted-in__',
  '_______',
  '______',
  'café-session',
  'cafe-session',
];

describe('Phase 2 — deriveOrchestratorId', () => {
  describe('determinism and purity', () => {
    test('the same session id always derives the same orchestrator id', () => {
      const first = deriveOrchestratorId(SESSION_ID);

      for (let i = 0; i < 50; i += 1) {
        expect(deriveOrchestratorId(SESSION_ID)).toBe(first);
      }
    });

    test('interleaving other inputs does not perturb a previously derived id', () => {
      const before = deriveOrchestratorId(SESSION_ID);
      REALISTIC_SESSION_IDS.forEach((id) => deriveOrchestratorId(id));

      expect(deriveOrchestratorId(SESSION_ID)).toBe(before);
    });

    test('a FRESH node process derives the identical id (no per-process salt, clock or randomness)', () => {
      // The hook runs in its own child process, the orchestrator's cli.mjs beat
      // in another. If the derivation carried any per-process entropy the two
      // would key different ledger rows and the gate would silently never fire.
      const inProcess = deriveOrchestratorId(SESSION_ID);

      const script =
        `import { deriveOrchestratorId } from ${JSON.stringify(MODULE_PATH)};\n` +
        `process.stdout.write(deriveOrchestratorId(${JSON.stringify(SESSION_ID)}));\n`;
      const child = spawnSync(process.execPath, ['--input-type=module', '-e', script], {
        encoding: 'utf8',
      });

      expect(child.status).toBe(0);
      expect(child.stdout).toBe(inProcess);
    });
  });

  describe('shape — safe as a JSON value, an argv token and a path component', () => {
    test.each(REALISTIC_SESSION_IDS)('derives a `session-`-prefixed, [A-Za-z0-9_-] id for %p', (sessionId) => {
      const derived = deriveOrchestratorId(sessionId);

      expect(typeof derived).toBe('string');
      expect(derived.startsWith('session-')).toBe(true);
      expect(derived).toMatch(SAFE_ID);
    });

    test.each(REALISTIC_SESSION_IDS)('round-trips through JSON unchanged for %p', (sessionId) => {
      const derived = deriveOrchestratorId(sessionId);

      expect(JSON.parse(JSON.stringify({ orchestratorId: derived })).orchestratorId).toBe(derived);
      // No escaping needed at all — the serialized form contains the raw id.
      expect(JSON.stringify(derived)).toBe(`"${derived}"`);
    });

    test.each(REALISTIC_SESSION_IDS)('is usable as a single path component for %p', (sessionId) => {
      const derived = deriveOrchestratorId(sessionId);

      expect(derived).not.toContain('/');
      expect(derived).not.toContain('\\');
      expect(derived).not.toContain('\0');
      expect(derived).not.toContain('.');
      expect(derived).not.toMatch(/\s/);
      expect(derived).not.toBe('.');
      expect(derived).not.toBe('..');
      // A path-component round-trip: joining it adds exactly one segment.
      expect(join('/tmp/arm', derived).split('/')).toHaveLength(4);
    });

    test('is length-bounded even for a pathologically long session id', () => {
      const derived = deriveOrchestratorId('x'.repeat(100_000));

      expect(derived).toMatch(SAFE_ID);
      expect(derived.length).toBeLessThanOrEqual(128);
    });
  });

  describe('never collides with the reserved `__…__` convention', () => {
    // `recordEverOptedIn`, `claimCapacity`, `releaseCapacity`, `reserveAdmission`
    // and cli.mjs `--claim` all THROW/exit for a reserved-shaped id. A derived id
    // that could ever be reserved-shaped would hard-fail the opt-in path.
    const ADVERSARIAL = [
      '__',
      '____',
      '__x__',
      '_'.repeat(64),
      '__ever-opted-in__',
      '__shared-machine-sample__',
      '__aimd-ceiling-state__',
      '__global-spawn-rate-bucket__',
      ':__:',
      'session-__',
      'x__',
      '__x',
    ];

    test.each(ADVERSARIAL)('isReservedEntry is false for the id derived from %p', (sessionId) => {
      const derived = deriveOrchestratorId(sessionId);

      expect(isReservedEntry(derived)).toBe(false);
      expect(derived).toMatch(SAFE_ID);
    });

    test.each([...ADVERSARIAL, ...REALISTIC_SESSION_IDS])(
      'the derived id never both starts and ends with `__` for %p',
      (sessionId) => {
        const derived = deriveOrchestratorId(sessionId);

        expect(derived.startsWith('__')).toBe(false);
        expect(isReservedEntry(derived)).toBe(false);
      },
    );
  });

  describe('distinctness — sanitization collision is a cross-session state leak', () => {
    // Each pair is a genuine collider under the superseded draft's
    // `.replace(/[^a-zA-Z0-9_-]/g, '')` strip: both members reduce to the SAME
    // string once the non-`[A-Za-z0-9_-]` characters are dropped. If these
    // derive equal ids, one session inherits the other's sticky opt-in and its
    // fail-closed denials.
    const COLLIDING_UNDER_A_NAIVE_STRIP = [
      ['sess:2026-09-05T10:00:00.000Z', 'sess2026-09-05T100000000Z'],
      ['sess:2026-09-05T10:00:00.000Z', 'sess.2026-09-05T10.00.00.000Z'],
      ['7c9a1f42:1b7e:4d2c:9f31:0a6b5c8e4d10', '7c9a1f421b7e4d2c9f310a6b5c8e4d10'],
      ['session/abc/def', 'sessionabcdef'],
      ['session abc def', 'sessionabcdef'],
      ['../../etc/passwd', 'etcpasswd'],
      ['café-session', 'caf-session'],
      ['sess#1', 'sess1'],
      ['a.b', 'ab'],
    ];

    test.each(COLLIDING_UNDER_A_NAIVE_STRIP)('%p and %p derive DIFFERENT orchestrator ids', (left, right) => {
      // Guard the fixture itself: these really are colliders under the naive
      // strip, so this test can only pass by NOT using a lossy strip.
      const strip = (value) => value.replace(/[^a-zA-Z0-9_-]/g, '');
      expect(strip(left)).toBe(strip(right));
      expect(left).not.toBe(right);

      expect(deriveOrchestratorId(left)).not.toBe(deriveOrchestratorId(right));
    });

    test('near-miss uuid variants (hyphenated / bare / colon-separated / cased) are all distinct', () => {
      const variants = [
        '7c9a1f42-1b7e-4d2c-9f31-0a6b5c8e4d10',
        '7c9a1f421b7e4d2c9f310a6b5c8e4d10',
        '7c9a1f42:1b7e:4d2c:9f31:0a6b5c8e4d10',
        '7C9A1F42-1B7E-4D2C-9F31-0A6B5C8E4D10',
      ];

      const derived = variants.map(deriveOrchestratorId);

      expect(new Set(derived).size).toBe(variants.length);
    });

    test('the whole realistic corpus derives pairwise-distinct ids', () => {
      const unique = new Set(REALISTIC_SESSION_IDS);
      const derived = new Set([...unique].map((id) => deriveOrchestratorId(id)));

      expect(derived.size).toBe(unique.size);
    });

    test('ids differing only in a trailing separator stay distinct', () => {
      const pairs = [
        ['sess-1', 'sess-1.'],
        ['sess-1', 'sess-1:'],
        ['sess-1', 'sess-1/'],
      ];

      pairs.forEach(([left, right]) => {
        expect(deriveOrchestratorId(left)).not.toBe(deriveOrchestratorId(right));
      });
    });

    test('a whitespace-padded session id is REJECTED rather than silently deriving a different id', () => {
      // The digest is taken over the RAW id, so `'sess-1 '` would otherwise
      // derive a different key from `'sess-1'` — and a hook reading
      // `session_id` from hook-event JSON while another caller captures it via
      // shell/file output with incidental padding would key different ledger
      // rows, so the lookup silently never matches (its failure shape).
      // Trimming and proceeding is NOT the fix either: that would silently fold
      // a caller meaning the padded literal onto the unpadded session. Reject
      // loudly, in the module's existing no-shared-fallback style.
      const PADDED = ['sess-1 ', ' sess-1', 'sess-1\n', '\tsess-1', ' sess-1 ', 'sess-1\r\n'];

      PADDED.forEach((sessionId) => {
        expect(() => deriveOrchestratorId(sessionId)).toThrow(TypeError);
        expect(() => deriveOrchestratorId(sessionId)).toThrow(/sessionId/i);
      });

      // The unpadded core still derives normally — only the padded form is
      // refused, so this is a rejection, not a widened blanket ban.
      expect(deriveOrchestratorId('sess-1')).toMatch(SAFE_ID);
    });
  });

  describe('no shared fallback — a malformed session id must not inherit another session’s gate state', () => {
    // The superseded draft's `String(sessionId || 'unknown-session')` collapses
    // EVERY malformed caller onto one derived id, so an opted-in malformed
    // session would gate — and fail closed on — every other malformed one.
    // The contract is to THROW instead: loud, per-caller, unlinkable.
    const MALFORMED = [
      ['undefined', undefined],
      ['null', null],
      ['empty string', ''],
      ['blank string', '   '],
      ['tab/newline only', '\t\n'],
      ['number', 42],
      ['zero', 0],
      ['boolean', true],
      ['object', {}],
      ['array', []],
      ['function', () => {}],
      ['NaN', Number.NaN],
    ];

    test.each(MALFORMED)('throws a TypeError for a %s session id', (_label, sessionId) => {
      expect(() => deriveOrchestratorId(sessionId)).toThrow(TypeError);
    });

    test('the thrown message names the offending parameter so a hook author can act on it', () => {
      expect(() => deriveOrchestratorId(undefined)).toThrow(/sessionId/i);
    });

    test('no malformed input silently yields a shared placeholder id', () => {
      // Belt and braces on the block above: whatever the implementation does,
      // it must never RETURN the same string for two different malformed
      // inputs (that is the leak). Throwing satisfies this; a
      // `'unknown-session'` fallback does not.
      const returned = [];
      MALFORMED.forEach(([, sessionId]) => {
        try {
          returned.push(deriveOrchestratorId(sessionId));
        } catch {
          /* throwing is the contract — nothing to record */
        }
      });

      expect(new Set(returned).size).toBe(returned.length);
      expect(returned).toEqual([]);
    });
  });

  describe('interop — a derived id is a valid key for the Phase 1 sticky ledger', () => {
    let workDir;
    let coordinationFilePath;

    beforeEach(async () => {
      workDir = await mkdtemp(join(tmpdir(), 'arm-orchestrator-id-'));
      coordinationFilePath = join(workDir, 'coordination.json');
    });

    afterEach(async () => {
      await rm(workDir, { recursive: true, force: true });
    });

    test('recordEverOptedIn accepts a derived id and hasEverOptedIn reads it back', async () => {
      const derived = deriveOrchestratorId(SESSION_ID);

      await expect(recordEverOptedIn(coordinationFilePath, derived)).resolves.toBe(true);
      await expect(hasEverOptedIn(coordinationFilePath, derived)).resolves.toBe(true);
    });

    test('an adversarial, underscore-only session id still produces a ledger-acceptable id', async () => {
      const derived = deriveOrchestratorId('_'.repeat(32));

      await expect(recordEverOptedIn(coordinationFilePath, derived)).resolves.toBe(true);
      await expect(hasEverOptedIn(coordinationFilePath, derived)).resolves.toBe(true);
    });

    test("one session's recorded opt-in does not answer true for a near-miss sibling session id", async () => {
      await recordEverOptedIn(coordinationFilePath, deriveOrchestratorId('sess:2026-09-05T10:00:00.000Z'));

      await expect(
        hasEverOptedIn(coordinationFilePath, deriveOrchestratorId('sess2026-09-05T100000000Z')),
      ).resolves.toBe(false);
    });
  });
});
