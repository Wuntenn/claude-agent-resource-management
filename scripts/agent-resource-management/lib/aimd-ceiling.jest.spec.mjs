// Phase 2 — failing tests for the PURE AIMD ceiling-adjustment
// function, `computeAimdCeiling` in `./aimd-ceiling.mjs` (does not exist
// yet — this whole file is RED by construction: the `import` below is
// expected to fail module resolution, exactly the same acceptable "red" form
// documented in `./aimd-ceiling-outer-acceptance.jest.spec.mjs`).
//
// ---------------------------------------------------------------------------
// PINNED CONTRACT (this is the spec the Phase 2 implementer must build to):
//
//   computeAimdCeiling(state, config) -> nextState
//
//   state:  { pressureLevel: number, ceiling: number, sustainedNormalCount: number }
//     - `pressureLevel` follows the existing codebase convention seen
//       throughout cli.mjs/lib/allowance.mjs: 1 = Normal, 2 = WARN,
//       3 = CRITICAL (>= 2 is "not Normal").
//     - `ceiling`/`sustainedNormalCount` are the PERSISTED state's current
//       values (Phase 1's `DEFAULT_AIMD_CEILING_STATE` cold-start shape:
//       `{ ceiling: 4, sustainedNormalCount: 0 }` — this function's `state`
//       argument is that same shape, plus the one-beat `pressureLevel`
//       signal layered on top).
//
//   config: { floor: number, ceilingMax: number, increaseStep: number,
//             decreaseFactor: number, sustainedNormalBeatsRequired: number }
//     - All caller-supplied, no defaults baked into the pure function itself
//       (Phase 3 supplies operator-configurable defaults; this module is
//       arithmetic only).
//
//   returns: { ceiling: number, sustainedNormalCount: number }
//     - A NEW pure-computation pair — deliberately NOT the same object shape
//       contract as Phase 1's I/O-bound `readAimdCeilingState`/
//       `writeAimdCeilingState` pair, even though the fields look the same:
//       this function never touches a file, a lock, or any per-agent-class
//       or admission-history data. Phase 3 is responsible for reading
//       persisted state in, calling this function, and writing the result
//       back out via Phase 1's persisted-state functions.
//
// DESIGN DECISION — CRITICAL vs WARN decrease magnitude (explicitly pinned,
// not left ambiguous): this contract uses the SAME `decreaseFactor` for both
// WARN (pressureLevel: 2) and CRITICAL (pressureLevel: 3). Rationale: the
// existing codebase's WARN/CRITICAL admission-block logic (cli.mjs's
// pressure-block, `pressureLevel >= 2`) already treats "not Normal" as one
// undifferentiated refusal band for admission purposes — mirroring that
// same >=2 threshold here, rather than inventing a second, steeper cut with
// no existing precedent to calibrate it against, keeps the AIMD ceiling's
// behavior consistent with the rest of the pressure-handling contract it
// backs. Test 6 below pins this explicitly.
// ---------------------------------------------------------------------------

import { computeAimdCeiling } from './aimd-ceiling.mjs';

const BASE_CONFIG = {
  floor: 1,
  ceilingMax: 20,
  increaseStep: 1,
  decreaseFactor: 0.7,
  sustainedNormalBeatsRequired: 3,
};

const NORMAL = 1;
const WARN = 2;
const CRITICAL = 3;

describe('computeAimdCeiling — pure AIMD concurrency-ceiling adjustment', () => {
  it('does not increase before N consecutive Normal beats are observed (beats 1 and 2 of 3 required leave the ceiling unchanged)', () => {
    let state = { pressureLevel: NORMAL, ceiling: 4, sustainedNormalCount: 0 };

    const afterBeat1 = computeAimdCeiling(state, BASE_CONFIG);
    expect(afterBeat1.ceiling).toBe(4);
    expect(afterBeat1.sustainedNormalCount).toBe(1);

    const afterBeat2 = computeAimdCeiling(
      { pressureLevel: NORMAL, ...afterBeat1 },
      BASE_CONFIG,
    );
    expect(afterBeat2.ceiling).toBe(4);
    expect(afterBeat2.sustainedNormalCount).toBe(2);
  });

  it('additive increase fires after exactly N (3) consecutive Normal beats', () => {
    let state = { ceiling: 4, sustainedNormalCount: 0 };
    for (let beat = 0; beat < BASE_CONFIG.sustainedNormalBeatsRequired; beat += 1) {
      state = computeAimdCeiling({ pressureLevel: NORMAL, ...state }, BASE_CONFIG);
    }

    expect(state.ceiling).toBe(4 + BASE_CONFIG.increaseStep);
  });

  it(
    'resets the sustained-Normal counter FULLY to 0 on a single non-Normal beat interrupting an ' +
      'otherwise-long streak — it does not decrement by 1 or partially forgive (2 Normal, 1 WARN, ' +
      '2 more Normal must NOT read as count 4)',
    () => {
      let state = { ceiling: 4, sustainedNormalCount: 0 };

      state = computeAimdCeiling({ pressureLevel: NORMAL, ...state }, BASE_CONFIG);
      state = computeAimdCeiling({ pressureLevel: NORMAL, ...state }, BASE_CONFIG);
      expect(state.sustainedNormalCount).toBe(2);

      state = computeAimdCeiling({ pressureLevel: WARN, ...state }, BASE_CONFIG);
      expect(state.sustainedNormalCount).toBe(0);

      state = computeAimdCeiling({ pressureLevel: NORMAL, ...state }, BASE_CONFIG);
      state = computeAimdCeiling({ pressureLevel: NORMAL, ...state }, BASE_CONFIG);

      // Restarted counting from the WARN beat — must be 2 (not 4, and not
      // "2 + partial credit" of any kind), and the increase must not have
      // fired yet since only 2 of the required 3 consecutive beats have
      // elapsed since the reset.
      expect(state.sustainedNormalCount).toBe(2);
      // The WARN beat above already applied its own multiplicative decrease
      // (Math.floor(4 * 0.7) = 2, per BASE_CONFIG and the same arithmetic
      // pinned by the "multiplicative decrease fires immediately" test below)
      // — the 2 subsequent Normal beats don't reach the 3-beat threshold, so
      // no additive increase fires and the ceiling stays at that
      // post-decrease value. (Corrected from an inconsistent `toBe(4)` that
      // ignored the WARN beat's own decrease — see PR discussion for
      // Phase 2.)
      expect(state.ceiling).toBe(2);
    },
  );

  it('multiplicative decrease fires immediately on the very beat WARN is first observed, no delay', () => {
    const state = { ceiling: 4, sustainedNormalCount: 0 };
    const afterWarn = computeAimdCeiling({ pressureLevel: WARN, ...state }, BASE_CONFIG);

    expect(afterWarn.ceiling).toBeLessThan(4);
  });

  it('the multiplicative-decrease result is always an integer, floored — not rounded — even from a fractional multiply (5 * 0.7 = 3.5 -> 3)', () => {
    const config = { ...BASE_CONFIG, decreaseFactor: 0.7 };
    const state = { ceiling: 5, sustainedNormalCount: 0 };

    const afterWarn = computeAimdCeiling({ pressureLevel: WARN, ...state }, config);

    expect(afterWarn.ceiling).toBe(3);
    expect(Number.isInteger(afterWarn.ceiling)).toBe(true);
  });

  it(
    'CRITICAL (pressureLevel: 3) produces the SAME decreased ceiling as WARN (pressureLevel: 2) from the ' +
      'same starting ceiling — pins the explicit "same decrease factor for both" design decision documented ' +
      'at the top of this file',
    () => {
      const state = { ceiling: 10, sustainedNormalCount: 0 };

      const afterWarn = computeAimdCeiling({ pressureLevel: WARN, ...state }, BASE_CONFIG);
      const afterCritical = computeAimdCeiling({ pressureLevel: CRITICAL, ...state }, BASE_CONFIG);

      expect(afterCritical.ceiling).toBe(afterWarn.ceiling);
      expect(afterWarn.ceiling).toBe(Math.floor(10 * BASE_CONFIG.decreaseFactor));
    },
  );

  it('additive increase clamps at the configured upper bound (ceilingMax), does not overshoot', () => {
    const config = { ...BASE_CONFIG, ceilingMax: 5, increaseStep: 1, sustainedNormalBeatsRequired: 1 };
    let state = { ceiling: 5, sustainedNormalCount: 0 };

    state = computeAimdCeiling({ pressureLevel: NORMAL, ...state }, config);

    expect(state.ceiling).toBe(5);
    expect(state.ceiling).toBeLessThanOrEqual(config.ceilingMax);
  });

  it('multiplicative decrease clamps at the configured floor, never reaches 0 or negative (floor=1, current=1, WARN pressure stays at 1)', () => {
    const config = { ...BASE_CONFIG, floor: 1 };
    const state = { ceiling: 1, sustainedNormalCount: 0 };

    const afterWarn = computeAimdCeiling({ pressureLevel: WARN, ...state }, config);

    expect(afterWarn.ceiling).toBe(1);
    expect(afterWarn.ceiling).toBeGreaterThanOrEqual(config.floor);
  });

  it('is idempotent when already at the upper bound and pressure keeps favoring increase — no drift, same output every call', () => {
    const config = { ...BASE_CONFIG, ceilingMax: 5, increaseStep: 1, sustainedNormalBeatsRequired: 1 };
    let state = { ceiling: 5, sustainedNormalCount: 0 };

    const first = computeAimdCeiling({ pressureLevel: NORMAL, ...state }, config);
    const second = computeAimdCeiling({ pressureLevel: NORMAL, ...first }, config);
    const third = computeAimdCeiling({ pressureLevel: NORMAL, ...second }, config);

    expect(first.ceiling).toBe(5);
    expect(second.ceiling).toBe(5);
    expect(third.ceiling).toBe(5);
  });

  it('is idempotent when already at the floor and pressure keeps favoring decrease — no drift, same output every call', () => {
    const config = { ...BASE_CONFIG, floor: 1 };
    let state = { ceiling: 1, sustainedNormalCount: 0 };

    const first = computeAimdCeiling({ pressureLevel: WARN, ...state }, config);
    const second = computeAimdCeiling({ pressureLevel: WARN, ...first }, config);
    const third = computeAimdCeiling({ pressureLevel: CRITICAL, ...second }, config);

    expect(first.ceiling).toBe(1);
    expect(second.ceiling).toBe(1);
    expect(third.ceiling).toBe(1);
  });
});

describe('source-scan boundary: computeAimdCeiling carries zero per-agent-class or admission-history awareness', () => {
  // Mirrors the `shedSignal`-is-emission-only source-scan idiom in
  // `../doc-honesty.jest.spec.mjs` (see e.g. its "confirms the underlying
  // fact being documented" assertions, which grep raw source text rather
  // than probing runtime behavior alone) — reads this module's OWN source
  // text and regex-scans it for identifiers that would betray awareness of
  // concepts this module must never depend on. This is a hard boundary
  // Phase 3's wiring must not violate later; this test protects it now, and
  // is expected to fail today purely because the file it reads does not
  // exist yet.
  let source;

  beforeAll(async () => {
    const { readFileSync } = await import('node:fs');
    const { fileURLToPath } = await import('node:url');
    const { dirname, join } = await import('node:path');
    const __dirname = dirname(fileURLToPath(import.meta.url));
    source = readFileSync(join(__dirname, 'aimd-ceiling.mjs'), 'utf8');
  });

  // Forbidden identifiers: anything shaped like a per-agent-class concept
  // (agent type/class/name, per-orchestrator identity) or an
  // admission-history concept (past decisions, historical records, queues,
  // claims/dibs ledgers). None of these may appear anywhere in this pure
  // module's signature or body.
  const FORBIDDEN_IDENTIFIER_PATTERNS = [
    /agentClass/i,
    /agentType/i,
    /agentName/i,
    /orchestratorId/i,
    /admissionHistory/i,
    /historyEntr/i,
    /\bhistory\b/i,
    /\bqueue\b/i,
    /\bclaims?\b/i,
    /\bdibs\b/i,
    /\bledger\b/i,
    /perAgentClass/i,
    /runningAgents/i,
  ];

  it('has no forbidden per-agent-class or admission-history identifier anywhere in its source text', () => {
    expect(source).toBeDefined();
    for (const pattern of FORBIDDEN_IDENTIFIER_PATTERNS) {
      expect(source).not.toMatch(pattern);
    }
  });

  it('never imports node:fs, node:child_process, or any coordination-file module — zero I/O, zero locking', () => {
    expect(source).not.toMatch(/from ['"]node:fs['"]/);
    expect(source).not.toMatch(/from ['"]node:child_process['"]/);
    expect(source).not.toMatch(/coordination-file\.mjs/);
    expect(source).not.toMatch(/withLock|acquireLock|writeEntriesAtomic/);
  });

  it("the exported function's own parameter list carries only the pinned pure-arithmetic fields (pressureLevel/ceiling/sustainedNormalCount + config), not a per-agent-class or history-shaped parameter", () => {
    const exportMatch = source.match(/export function computeAimdCeiling\(([^)]*)\)/);
    expect(exportMatch).not.toBeNull();
    const paramList = exportMatch[1];

    expect(paramList).not.toMatch(/agentClass|agentType|history|queue|claims|ledger/i);
  });
});
