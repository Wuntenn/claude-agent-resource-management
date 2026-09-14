// Phase 3 — `--user-attests-idle` CLI flag + admission wiring.
//
// RED BY CONSTRUCTION: `--user-attests-idle` is not a recognized flag
// anywhere in `cli.mjs` today. `parseArgs` (cli.mjs:827-849) is a purely
// generic `--flag`/`--flag=value` tokenizer with no allow-list, so passing
// `--user-attests-idle` today is silently accepted into the `flags` object
// and then never read by anything — a `hold` beat stays a bare
// `{ type: 'hold' }` whether or not the flag is present, and there is no
// idle-probe admission path at all. Every test in this file that asserts an
// OBSERVABLE difference caused by the flag (i.e. every test except the
// explicit "flag absent" / "verdict not hold" control pins) fails against
// today's `cli.mjs` for exactly that reason.
//
// ---------------------------------------------------------------------------
// OUTPUT-SHAPE DECISION (documented per this repo's convention of writing
// down judgment calls the Build Plan itself left open — see
// pressure-override.jest.spec.mjs's own header comment for the precedent).
// The Build Plan pins the *behavioural* contract precisely
// ("admits exactly one agent tagged agentClass: 'idle-probe'") but not the
// exact JSON shape `cli.mjs` should print for that admission. This file
// assumes:
//
//   Admitted (flag set, verdict would be `hold`, no probe in flight):
//     { type: 'spawn-allowed', allowance: 1, idleProbe: { agentClass: 'idle-probe' } }
//
//   Why reuse `spawn-allowed` rather than inventing a new top-level literal
//   (unlike pressure-override's `pressure-block`): admitting the probe IS
//   granting permission to spawn exactly one real agent — an orchestrator
//   that only branches on `type === 'spawn-allowed'` to decide whether to
//   proceed should still work correctly for a probe beat. The `idleProbe`
//   sibling field (mirroring the existing `diskTrend`/`liveAgentGrant`
//   convention) is what distinguishes "an ordinary spawn-allowed beat" from
//   "a probe admission" for a caller that cares, without requiring every
//   caller to learn a new literal for what is, mechanically, still a
//   spawn-allowed decision.
//
//   Refused (flag set, verdict would be `hold`, a probe already in flight):
//     { type: 'hold', idleProbeRefused: true } — the ordinary hold shape
//     plus one distinguishing field (pre-PR review fix, Medium — M2).
//     Without it, a refused-because-in-flight beat is byte-for-byte
//     indistinguishable from a beat where `--user-attests-idle` was never
//     passed at all — impossible for a caller (or a human debugging a
//     stuck orchestrator) to tell "the flag worked but a probe beat it to
//     the slot" apart from "the flag was never set". This file therefore
//     DOES pin `idleProbeRefused: true` on the refusal case, and pins its
//     ABSENCE on both the admitted case and the flag-absent control case.
//
// If the implementer's handoff diverges from this shape, that is fine —
// this is a starting contract, not an unchangeable one — but the divergence
// must be explained and this file's assertions updated to match, per the
// general stance of testing behaviour, not dictating implementation.
// ---------------------------------------------------------------------------

import { spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const __dirname = dirname(fileURLToPath(import.meta.url));
const CLI = join(__dirname, 'cli.mjs');

/** Runs the real cli.mjs as a child process — never imports it directly (it
 * runs main() unconditionally at module scope on import). */
function runCli(args, extraEnv) {
  const result = spawnSync('node', [CLI, ...args], {
    encoding: 'utf8',
    env: { ...process.env, ...extraEnv },
  });
  return { status: result.status, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

const DISK_GREEN = { freeDiskGb: 60, declineRateGbPerHour: 0.5 };

// Mirrors cli.jest.spec.mjs's own MEMORY_GREEN/MEMORY_AMBER/MEMORY_RED
// fixtures exactly, so this file's fixtures classify identically to the
// canonical smoke test's — pressureLevel stays 1 (below WARN) throughout
// except where a test explicitly names WARN/CRITICAL.
const MEMORY_GREEN = { pressureLevel: 1, swapUsedMb: 1024, compressedMb: 512 };
const MEMORY_HOLD = { pressureLevel: 1, swapUsedMb: 6144, compressedMb: 2048 }; // AMBER -> hold
const MEMORY_RED = { pressureLevel: 1, swapUsedMb: 12288, compressedMb: 4096 }; // RED -> pause
const MEMORY_HOLD_WARN = { pressureLevel: 2, swapUsedMb: 6144, compressedMb: 2048 }; // hold-shaped, but WARN fires first
const MEMORY_HOLD_CRITICAL = { pressureLevel: 4, swapUsedMb: 6144, compressedMb: 2048 }; // hold-shaped, but CRITICAL fires first

let workDir;
let coordinationFilePath;

beforeEach(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'arm-user-attests-idle-'));
  coordinationFilePath = join(workDir, 'coordination.json');
});

afterEach(async () => {
  await rm(workDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Item 1 — parsed via the house parseArgs boolean-flag convention: a bare
// trailing `--user-attests-idle`, or `--user-attests-idle` immediately
// followed by another `--flag`, both parse to boolean `true` (cli.mjs:
// 827-849's `next === undefined || next.startsWith('--')` branch). Asserted
// behaviourally (the only way to observe parseArgs's output, since it is
// not exported and cli.mjs runs main() unconditionally on import — mirrors
// how cli.jest.spec.mjs's own `--heartbeat` boolean-flag tests are written).
// ---------------------------------------------------------------------------

describe('cli.mjs --user-attests-idle parses via the house boolean-flag convention', () => {
  test('bare trailing --user-attests-idle (nothing after it) is recognized as boolean true: admits an idle-probe on a hold beat', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-uai-bare-trailing', '--desired-agents=1', '--user-attests-idle'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_HOLD, disk: DISK_GREEN }),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('spawn-allowed');
    expect(parsed.idleProbe).toEqual({ agentClass: 'idle-probe' });
  });

  test('--user-attests-idle followed by another --flag (not a value) is recognized as boolean true: admits an idle-probe on a hold beat', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-uai-followed-by-flag', '--user-attests-idle', '--desired-agents=1'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_HOLD, disk: DISK_GREEN }),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('spawn-allowed');
    expect(parsed.idleProbe).toEqual({ agentClass: 'idle-probe' });
  });
});

// ---------------------------------------------------------------------------
// Items 2 & 6 — flag absent (default) / verdict not hold: no change at all.
// These are regression pins as much as red tests — they may already hold
// true today (an unimplemented flag is, trivially, always a no-op), but they
// pin the exact byte-identical contract the implementer's Phase 3 landing
// must preserve, and they double as the "control" half of the WARN/CRITICAL
// independence tests below (proving the flag is NOT simply always inert).
// ---------------------------------------------------------------------------

describe('cli.mjs --user-attests-idle default-off / non-hold no-op paths', () => {
  test('flag absent entirely: a hold beat is unchanged — bare { type: "hold" }, no idleProbe field', () => {
    const { stdout } = runCli(
      ['--orchestrator-id=orch-uai-absent-hold'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_HOLD, disk: DISK_GREEN }),
      },
    );

    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('hold');
    expect(parsed.idleProbe).toBeUndefined();
  });

  test('flag set but verdict would be spawn-allowed (GREEN): flag has no effect, output byte-identical to the flag-absent run', async () => {
    // Deliberately TWO SEPARATE, freshly-created coordination files (rather
    // than sharing this describe block's own `coordinationFilePath`): the
    // real AIMD ceiling (a global, coordination-file-persisted reserved-id
    // ledger entry, ./lib/aimd-ceiling.mjs) grows across consecutive Normal
    // beats regardless of orchestratorId, so two sequential beats sharing
    // one coordination file would legitimately see `allowance` climb between
    // them — a fact about the pre-existing AIMD mechanism, unrelated to
    // whether --user-attests-idle does anything. Giving each beat its own
    // cold-start coordination file isolates the ONE variable this test cares
    // about (the flag) from that orthogonal, already-covered-elsewhere
    // growth behaviour (see aimd-ceiling-incident-replay.jest.spec.mjs).
    const workDirA = await mkdtemp(join(tmpdir(), 'arm-user-attests-idle-green-a-'));
    const workDirB = await mkdtemp(join(tmpdir(), 'arm-user-attests-idle-green-b-'));
    try {
      const withoutFlag = runCli(
        ['--orchestrator-id=orch-uai-green-noflag', '--desired-agents=2'],
        {
          ARM_COORDINATION_FILE: join(workDirA, 'coordination.json'),
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        },
      );
      expect(withoutFlag.status).toBe(0);
      const withoutFlagParsed = JSON.parse(withoutFlag.stdout);
      expect(withoutFlagParsed.type).toBe('spawn-allowed');
      expect(withoutFlagParsed.idleProbe).toBeUndefined();

      const withFlag = runCli(
        ['--orchestrator-id=orch-uai-green-flag', '--desired-agents=2', '--user-attests-idle'],
        {
          ARM_COORDINATION_FILE: join(workDirB, 'coordination.json'),
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_GREEN }),
        },
      );
      expect(withFlag.status).toBe(0);
      const withFlagParsed = JSON.parse(withFlag.stdout);

      // Same verdict shape (modulo orchestrator-independent fields only —
      // both beats request the same --desired-agents against an identical
      // GREEN sample from an equally cold-start coordination file, so
      // `allowance` must also match).
      expect(withFlagParsed.type).toBe('spawn-allowed');
      expect(withFlagParsed.allowance).toBe(withoutFlagParsed.allowance);
      expect(withFlagParsed.idleProbe).toBeUndefined();
    } finally {
      await rm(workDirA, { recursive: true, force: true });
      await rm(workDirB, { recursive: true, force: true });
    }
  });

  test('flag set but verdict would be pause (RED): flag has no effect, type stays pause, no idleProbe field', () => {
    const { status, stdout } = runCli(
      ['--orchestrator-id=orch-uai-red-flag', '--desired-agents=1', '--user-attests-idle'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_RED, disk: DISK_GREEN }),
        ARM_FAKE_PS_OUTPUT: '', // idle host: no claude-rooted trees running -> pauseCandidate: null
      },
    );

    expect(status).toBe(0);
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('pause');
    expect(parsed.idleProbe).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Item 2 (THE core safety invariant) — pressureLevel >= 2 (WARN or CRITICAL)
// short-circuits at the pressure-block pre-check (cli.mjs:~8004-8035) BEFORE
// buildTrafficLight ever runs, so the flag must have ZERO effect: identical
// pressure-block/shedSignal output whether or not --user-attests-idle is
// set. Tested at both pressureLevel=2 (WARN) and pressureLevel=4 (CRITICAL)
// per the Investigation's explicit note that the real short-circuit fires at
// WARN, not just CRITICAL.
//
// Each test also runs a CONTROL beat first: the identical swap/compressed
// figures at pressureLevel=1 (below WARN) DO admit an idle-probe when the
// flag is set — proving the flag is genuinely "live" (not simply
// unimplemented-and-therefore-inert) and that WARN/CRITICAL specifically
// squash it, not that the flag never does anything at all.
// ---------------------------------------------------------------------------

describe('cli.mjs --user-attests-idle has zero effect at pressureLevel >= WARN (safety invariant)', () => {
  test('control: the same hold-shaped sample at pressureLevel=1 (below WARN) DOES admit an idle-probe when the flag is set', () => {
    const { stdout } = runCli(
      ['--orchestrator-id=orch-uai-control-below-warn', '--desired-agents=1', '--user-attests-idle'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_HOLD, disk: DISK_GREEN }),
      },
    );
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('spawn-allowed');
    expect(parsed.idleProbe).toEqual({ agentClass: 'idle-probe' });
  });

  test('WARN (pressureLevel=2): pressure-block output is byte-identical whether or not --user-attests-idle is set', async () => {
    // Two separate, freshly-created coordination files (see the GREEN test
    // above for the full rationale): `diskTrend` is itself derived from
    // `diskHistory` persisted on the coordination file, so two sequential
    // beats sharing ONE file would see a different diskTrend on the second
    // beat (a prior sample now exists to compute a trend from) purely from
    // beat ordering — orthogonal to whether the flag does anything.
    const workDirA = await mkdtemp(join(tmpdir(), 'arm-user-attests-idle-warn-a-'));
    const workDirB = await mkdtemp(join(tmpdir(), 'arm-user-attests-idle-warn-b-'));
    try {
      // Phase 3 review follow-up (Medium): pin loadAverage via
      // ARM_FAKE_LOAD_AVERAGE_JSON in BOTH beats so the `toEqual` comparison
      // below is fully deterministic. Without it, `loadAverage` is sourced
      // from the real os.loadavg() independently in each of the two
      // spawnSync calls — sampled milliseconds apart, occasionally straddling
      // a kernel sampling boundary and disagreeing, flaking the assertion.
      const FAKE_LOAD_AVERAGE = JSON.stringify([1.0, 1.0, 1.0]);

      const withoutFlag = runCli(
        ['--orchestrator-id=orch-uai-warn-noflag', '--desired-agents=1'],
        {
          ARM_COORDINATION_FILE: join(workDirA, 'coordination.json'),
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_HOLD_WARN, disk: DISK_GREEN }),
          ARM_FAKE_LOAD_AVERAGE_JSON: FAKE_LOAD_AVERAGE,
        },
      );
      expect(withoutFlag.status).toBe(0);
      const withoutFlagParsed = JSON.parse(withoutFlag.stdout);
      expect(withoutFlagParsed.type).toBe('pressure-block');
      expect(withoutFlagParsed.idleProbe).toBeUndefined();

      const withFlag = runCli(
        ['--orchestrator-id=orch-uai-warn-flag', '--desired-agents=1', '--user-attests-idle'],
        {
          ARM_COORDINATION_FILE: join(workDirB, 'coordination.json'),
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_HOLD_WARN, disk: DISK_GREEN }),
          ARM_FAKE_LOAD_AVERAGE_JSON: FAKE_LOAD_AVERAGE,
        },
      );
      expect(withFlag.status).toBe(0);
      const withFlagParsed = JSON.parse(withFlag.stdout);

      // Byte-identical block shape (pressureLevel/shedSignal/liveAgentGrant),
      // and critically: never spawn-allowed, never an idleProbe field, no
      // matter how the flag was passed.
      expect(withFlagParsed).toEqual(withoutFlagParsed);
      expect(withFlagParsed.type).toBe('pressure-block');
      expect(withFlagParsed.pressureLevel).toBe(2);
      expect(withFlagParsed.idleProbe).toBeUndefined();
    } finally {
      await rm(workDirA, { recursive: true, force: true });
      await rm(workDirB, { recursive: true, force: true });
    }
  });

  test('CRITICAL (pressureLevel=4): pressure-block output is byte-identical whether or not --user-attests-idle is set', async () => {
    const workDirA = await mkdtemp(join(tmpdir(), 'arm-user-attests-idle-critical-a-'));
    const workDirB = await mkdtemp(join(tmpdir(), 'arm-user-attests-idle-critical-b-'));
    try {
      // Phase 3 review follow-up (Medium): see the WARN test above for
      // the full rationale — pin loadAverage via ARM_FAKE_LOAD_AVERAGE_JSON
      // in BOTH beats so the `toEqual` comparison below is deterministic.
      const FAKE_LOAD_AVERAGE = JSON.stringify([1.0, 1.0, 1.0]);

      const withoutFlag = runCli(
        ['--orchestrator-id=orch-uai-critical-noflag', '--desired-agents=1'],
        {
          ARM_COORDINATION_FILE: join(workDirA, 'coordination.json'),
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_HOLD_CRITICAL, disk: DISK_GREEN }),
          ARM_FAKE_LOAD_AVERAGE_JSON: FAKE_LOAD_AVERAGE,
        },
      );
      expect(withoutFlag.status).toBe(0);
      const withoutFlagParsed = JSON.parse(withoutFlag.stdout);
      expect(withoutFlagParsed.type).toBe('pressure-block');
      expect(withoutFlagParsed.idleProbe).toBeUndefined();

      const withFlag = runCli(
        ['--orchestrator-id=orch-uai-critical-flag', '--desired-agents=1', '--user-attests-idle'],
        {
          ARM_COORDINATION_FILE: join(workDirB, 'coordination.json'),
          ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_HOLD_CRITICAL, disk: DISK_GREEN }),
          ARM_FAKE_LOAD_AVERAGE_JSON: FAKE_LOAD_AVERAGE,
        },
      );
      expect(withFlag.status).toBe(0);
      const withFlagParsed = JSON.parse(withFlag.stdout);

      expect(withFlagParsed).toEqual(withoutFlagParsed);
      expect(withFlagParsed.type).toBe('pressure-block');
      expect(withFlagParsed.pressureLevel).toBe(4);
      expect(withFlagParsed.shedSignal).toBe(true); // CRITICAL alone still sheds — unrelated to the flag.
      expect(withFlagParsed.idleProbe).toBeUndefined();
    } finally {
      await rm(workDirA, { recursive: true, force: true });
      await rm(workDirB, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Items 3 & 4 — flag set + verdict hold (pressure below WARN):
//   - no probe in flight -> admits exactly one idle-probe-tagged agent.
//   - a probe already in flight -> refused/held, no second probe admitted.
//
// The "already in flight" state is established purely through the CLI's own
// observable behaviour (sequential real spawnSync beats sharing one
// coordination file, no time advance needed) — deliberately not asserting
// on any specific coordination-file field name for the in-flight marker, to
// avoid over-constraining an implementation the Build Plan left open on that
// point (Convergence Analysis note: "enforced against actual persisted
// process/coordination-file state, not in-memory beat state").
// ---------------------------------------------------------------------------

describe('cli.mjs --user-attests-idle admission (hold + flag, single-probe-in-flight gate)', () => {
  test('flag set + hold + no probe in flight: admits exactly one idle-probe-tagged agent', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-uai-admit-1', '--desired-agents=1', '--user-attests-idle'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_HOLD, disk: DISK_GREEN }),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('spawn-allowed');
    expect(parsed.allowance).toBe(1); // exactly one probe agent, regardless of --desired-agents.
    expect(parsed.idleProbe).toEqual({ agentClass: 'idle-probe' });
    //  Phase 3 review follow-up (Medium): `liveAgentGrant` stays
    // `null` on an admitted idle-probe beat — it is deliberately NOT set to
    // `1` to mirror `allowance`. The idle-probe's single slot is claimed
    // against a dedicated `IDLE_PROBE_CLAIM_TYPE` ledger, entirely separate
    // from `LIVE_AGENT_ADMISSION_CLAIM_TYPE`/`reserveAdmission` and not
    // counted against `LIVE_AGENT_CEILING` (Build Plan Phase 3 edge case:
    // idle-probe must not be treated as a normal agent class for AIMD-ceiling
    // purposes). Folding it into `liveAgentGrant` would break the invariant
    // several existing specs pin — that summing `liveAgentGrant` across a
    // racing cohort never exceeds the real, ledger-enforced live-agent
    // ceiling (see outer-acceptance.jest.spec.mjs and
    // live-agent-ceiling.jest.spec.mjs) — by inflating that sum with a slot
    // the AIMD-ceiling ledger never reserved. A caller must branch on the
    // `idleProbe` field (present here), not `liveAgentGrant`, to learn that
    // one agent was admitted on this beat.
    expect(parsed.liveAgentGrant).toBeNull();
  });

  test('flag set + hold + no probe in flight: liveAgentGrant is null (not 1) on the admitted idle-probe beat — the idleProbe field, not liveAgentGrant, is authoritative here', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-uai-admit-live-agent-grant-exception', '--desired-agents=1', '--user-attests-idle'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_HOLD, disk: DISK_GREEN }),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = JSON.parse(stdout);
    // Pin the full admitted-idle-probe shape in one place: type is
    // spawn-allowed, allowance is the single probe slot, idleProbe names the
    // admitted agent class, and liveAgentGrant is the one field that stays
    // null despite type === 'spawn-allowed' — see cli.mjs's `liveAgentGrant`
    // doc comment for the "ONE DELIBERATE EXCEPTION" this pins.
    expect(parsed).toMatchObject({
      type: 'spawn-allowed',
      allowance: 1,
      idleProbe: { agentClass: 'idle-probe' },
      liveAgentGrant: null,
    });
  });

  test('flag set + hold + a probe already in flight (from a prior beat, different orchestrator): refused/held, no second probe admitted', () => {
    // Beat 1: orchestrator A attests idle and is admitted a probe.
    const beat1 = runCli(
      ['--orchestrator-id=orch-uai-inflight-a', '--desired-agents=1', '--user-attests-idle'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_HOLD, disk: DISK_GREEN }),
      },
    );
    expect(beat1.status).toBe(0);
    const beat1Parsed = JSON.parse(beat1.stdout);
    expect(beat1Parsed.type).toBe('spawn-allowed');
    expect(beat1Parsed.idleProbe).toEqual({ agentClass: 'idle-probe' });

    // Beat 2: a DIFFERENT orchestrator (B) also attests idle on the very
    // next beat, before beat 1's probe has any chance to conclude. The
    // single-probe-in-flight gate is global (not per-orchestrator) per the
    // Build Plan edge case — beat 2 must be refused/held, not granted a
    // second concurrent probe.
    const beat2 = runCli(
      ['--orchestrator-id=orch-uai-inflight-b', '--desired-agents=1', '--user-attests-idle'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_HOLD, disk: DISK_GREEN }),
      },
    );
    expect(beat2.status).toBe(0);
    const beat2Parsed = JSON.parse(beat2.stdout);
    expect(beat2Parsed.type).toBe('hold');
    expect(beat2Parsed.idleProbe).toBeUndefined();
    // pre-PR review fix (Medium — M2): this refusal must be
    // distinguishable from the flag simply being absent.
    expect(beat2Parsed.idleProbeRefused).toBe(true);
  });

  test('flag ABSENT + hold: idleProbeRefused is never present — the field is exclusively a refusal signal, not a generic hold marker', () => {
    // Same MEMORY_HOLD/DISK_GREEN inputs as the refusal case above, but
    // WITHOUT --user-attests-idle at all. Pins that idleProbeRefused only
    // ever appears on a genuinely refused attestation, never on an ordinary
    // hold beat that never tried to attest in the first place — otherwise
    // the field would fail to distinguish anything.
    const beat = runCli(['--orchestrator-id=orch-uai-flag-absent', '--desired-agents=1'], {
      ARM_COORDINATION_FILE: coordinationFilePath,
      ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_HOLD, disk: DISK_GREEN }),
    });
    expect(beat.status).toBe(0);
    const beatParsed = JSON.parse(beat.stdout);
    expect(beatParsed.type).toBe('hold');
    expect(beatParsed.idleProbeRefused).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// pre-PR review fix (High — H1/H2/H3, Medium — M1): `hold` is not, by
// itself, the "safe to attest idle" case — see `isGenuineMemoryAmberHold`'s
// own doc comment in cli.mjs. These pin the three narrowed exclusions plus
// the heartbeat/advise-only exclusion, each of which previously admitted a
// probe (or, for --advise-only, performed a real ledger write) that the
// Build Plan's safety invariant never intended to cover.
// ---------------------------------------------------------------------------

describe('cli.mjs --user-attests-idle: hold-but-not-eligible cases are left untouched (pre-PR review fix)', () => {
  test('stale/unknown memory sample forced to AMBER: flag has zero effect, no idle-probe admitted', () => {
    // `memory: {}` is a well-formed object (passes isWellFormedSample) with
    // no NaN fields (passes hasUnparseableField), so takeSample()'s own
    // `stale` flag stays false — but pressureLevel/swapUsedMb are both
    // `undefined`, which classifyMemoryRaw treats as "unknown", forcing
    // AMBER regardless. This is exactly the case the pressure-block
    // short-circuit CANNOT catch (it only fires on a genuine numeric
    // pressureLevel >= warnAtOrAbove) — the true pressure state here was
    // never actually observed and could, in reality, be at or above WARN.
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-uai-stale-hold', '--desired-agents=1', '--user-attests-idle'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: {}, disk: DISK_GREEN }),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('hold');
    expect(parsed.idleProbe).toBeUndefined();
    expect(parsed.idleProbeRefused).toBeUndefined();
  });

  test('a real pressureLevel but a missing/unparseable swapUsedMb: flag has zero effect (classifyMemoryRaw treats EITHER non-finite field as unknown)', () => {
    // Regression pin for a narrower miss in the H1 fix's first draft, which
    // checked `Number.isFinite(pressureLevel)` alone: `classifyMemoryRaw`
    // forces the "unknown" AMBER path on a non-finite `pressureLevel` OR a
    // non-finite `swapUsedMb` (`threshold.mjs`) — a sample with a genuine
    // pressureLevel but a missing swapUsedMb is just as "never actually
    // observed" as the fully-empty case above and must be excluded too.
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-uai-partial-stale-hold', '--desired-agents=1', '--user-attests-idle'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: { pressureLevel: 1 }, disk: DISK_GREEN }),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('hold');
    expect(parsed.idleProbe).toBeUndefined();
    expect(parsed.idleProbeRefused).toBeUndefined();
  });

  test('disk-AMBER hold with memory GREEN: flag has zero effect — --user-attests-idle asserts memory idleness, not disk headroom', () => {
    const DISK_AMBER = { freeDiskGb: 20, declineRateGbPerHour: 0.5 };
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-uai-disk-amber-hold', '--desired-agents=1', '--user-attests-idle'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_GREEN, disk: DISK_AMBER }),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('hold');
    expect(parsed.idleProbe).toBeUndefined();
    expect(parsed.idleProbeRefused).toBeUndefined();
  });

  test('--heartbeat + --user-attests-idle on a memory-AMBER hold: flag has zero effect — a heartbeat is a liveness ping, not a spawn decision', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-uai-heartbeat', '--heartbeat', '--user-attests-idle'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_HOLD, disk: DISK_GREEN }),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('hold');
    expect(parsed.idleProbe).toBeUndefined();
    expect(parsed.idleProbeRefused).toBeUndefined();
  });

  test('--advise-only + --user-attests-idle on a memory-AMBER hold: flag has zero effect — --advise-only is a documented non-mutating read, and claiming the idle-probe slot is a real ledger write', () => {
    const { status, stdout, stderr } = runCli(
      ['--orchestrator-id=orch-uai-advise-only', '--advise-only', '--user-attests-idle'],
      {
        ARM_COORDINATION_FILE: coordinationFilePath,
        ARM_FAKE_COLLECT_JSON: JSON.stringify({ memory: MEMORY_HOLD, disk: DISK_GREEN }),
      },
    );

    expect(status).toBe(0);
    expect(stderr).toBe('');
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe('hold');
    expect(parsed.idleProbe).toBeUndefined();
    expect(parsed.idleProbeRefused).toBeUndefined();
  });
});
