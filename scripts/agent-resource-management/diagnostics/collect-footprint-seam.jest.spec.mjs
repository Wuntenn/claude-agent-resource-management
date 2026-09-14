// Regression coverage for `collect.mjs`'s `collectFootprintSamples` — the
// production `--live` collection path must NEVER honor the
// `ARM_FAKE_FOOTPRINT_OUTPUT` test seam that `lib/footprint-sampler.mjs`'s
// `sampleFootprint` exposes. See collect.mjs's own header comment on
// `collectFootprintSamples` for the rationale: honoring the seam here would
// silently replace every footprint sample in a live diagnostics report with
// fabricated data, indistinguishable from a real reading.
//
// Lives in its own file (rather than folded into collect.jest.spec.mjs)
// because it needs to mock `../lib/footprint-sampler.mjs` itself — a mock
// `collect.jest.spec.mjs` deliberately does not install, since its own
// suite exercises the real `sampleFootprint` parsing against mocked
// `execFileSync` output instead.
//
// MOCKING IDIOM: `jest.unstable_mockModule`, matching collect.jest.spec.mjs
// and footprint-sampler.jest.spec.mjs's established idiom for this skill.

import { jest } from '@jest/globals';

const execFileSyncMock = jest.fn();
const readdirMock = jest.fn();
const readFileMock = jest.fn();
const lstatMock = jest.fn();
const sampleFootprintMock = jest.fn();

jest.unstable_mockModule('node:child_process', () => ({
  execFileSync: execFileSyncMock,
}));

jest.unstable_mockModule('node:fs/promises', () => ({
  readdir: readdirMock,
  readFile: readFileMock,
  lstat: lstatMock,
}));

jest.unstable_mockModule('../lib/footprint-sampler.mjs', () => ({
  sampleFootprint: sampleFootprintMock,
}));

const { runCollection } = await import('./collect.mjs');

const VM_STAT_HEALTHY = `Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                             612345.
Pages active:                           834567.
Pages inactive:                         245678.
Pages speculative:                        4123.
Pages throttled:                             0.
Pages wired down:                       456789.
Pages purgeable:                          4567.
"Translation faults":                123456789.
Pages copy-on-write:                    1234567.
Pages zero filled:                     45678901.
Pages reactivated:                        98765.
Pages purged:                             43210.
File-backed pages:                      234567.
Anonymous pages:                         45678.
Pages stored in compressor:               8192.
Pages occupied by compressor:             6144.
Decompressions:                          987654.
Compressions:                           1234567.
Pageins:                                2345678.
Pageouts:                                 12345.
Swapins:                                     12.
Swapouts:                                    34.
`;

const PRESSURE_LEVEL_NORMAL_STDOUT = '1\n';
const SWAP_USAGE_STDOUT = 'vm.swapusage: total = 3072.00M  used = 512.00M  free = 2560.00M  (encrypted)\n';

// Two agent-rooted process rows (comm matches AGENT_ROOT_COMM_PATTERN) plus
// one unrelated row, so the assertion below proves the seam-disabling call
// happens for EVERY sampled agent pid, not just a coincidental single one.
const PS_TREE_WITH_TWO_AGENT_PIDS = `  PID  PPID    RSS ELAPSED COMM
 39459     1   4832 01:02:03 /bin/zsh
 39461 39459   2048 00:15:20 /opt/homebrew/bin/node /path/to/claude
 39462 39459   2048 00:15:21 /opt/homebrew/bin/node /path/to/claude
`;

function enoentError(binary) {
  const error = new Error(`spawnSync ${binary} ENOENT`);
  error.code = 'ENOENT';
  error.errno = -2;
  error.syscall = `spawnSync ${binary}`;
  error.path = binary;
  return error;
}

beforeEach(() => {
  jest.clearAllMocks();
  execFileSyncMock.mockImplementation((command, args = []) => {
    const joined = `${command} ${args.join(' ')}`;
    if (command.endsWith('vm_stat')) return VM_STAT_HEALTHY;
    if (command.endsWith('sysctl') && joined.includes('kern.memorystatus_vm_pressure_level')) {
      return PRESSURE_LEVEL_NORMAL_STDOUT;
    }
    if (command.endsWith('sysctl') && joined.includes('vm.swapusage')) return SWAP_USAGE_STDOUT;
    if (command.endsWith('ps')) return PS_TREE_WITH_TWO_AGENT_PIDS;
    throw enoentError(command);
  });
  sampleFootprintMock.mockResolvedValue(42);
});

describe('runCollection({ mode: "live" }) — regression: fake-footprint-seam is always disabled', () => {
  it('calls sampleFootprint with honorFakeSeam=false for every agent-rooted pid', async () => {
    const result = await runCollection({ mode: 'live' });

    expect(sampleFootprintMock).toHaveBeenCalledTimes(2);
    for (const call of sampleFootprintMock.mock.calls) {
      const [, honorFakeSeam] = call;
      expect(honorFakeSeam).toBe(false);
    }
    expect(result.footprints.status).toBe('ok');
    expect(result.footprints.samples).toHaveLength(2);
  });
});

// ---------------------------------------------------------------------------
// Regression: footprint sampling during `--live` is concurrency-capped, not
// fired as one unbounded `Promise.all` burst across every agent-rooted pid.
// Uses a `ps` fixture with MORE agent pids than the batch size, and a
// controllable `sampleFootprint` mock that tracks how many calls are
// in-flight at once (incrementing on entry, decrementing only once its
// caller-controlled promise resolves), so the assertion genuinely exercises
// the batching rather than merely trusting the implementation's shape.
// ---------------------------------------------------------------------------

// Six agent-rooted rows plus one unrelated row — comfortably more than the
// production batch size (4), so a real concurrency cap is observable.
const PS_TREE_WITH_SIX_AGENT_PIDS = `  PID  PPID    RSS ELAPSED COMM
 39459     1   4832 01:02:03 /bin/zsh
 39461 39459   2048 00:15:20 /opt/homebrew/bin/node /path/to/claude
 39462 39459   2048 00:15:21 /opt/homebrew/bin/node /path/to/claude
 39463 39459   2048 00:15:22 /opt/homebrew/bin/node /path/to/claude
 39464 39459   2048 00:15:23 /opt/homebrew/bin/node /path/to/claude
 39465 39459   2048 00:15:24 /opt/homebrew/bin/node /path/to/claude
 39466 39459   2048 00:15:25 /opt/homebrew/bin/node /path/to/claude
`;

describe('runCollection({ mode: "live" }) — regression: footprint sampling is concurrency-capped', () => {
  it('never has more than 4 sampleFootprint calls in-flight at once, even with 6 agent-rooted pids', async () => {
    execFileSyncMock.mockImplementation((command, args = []) => {
      const joined = `${command} ${args.join(' ')}`;
      if (command.endsWith('vm_stat')) return VM_STAT_HEALTHY;
      if (command.endsWith('sysctl') && joined.includes('kern.memorystatus_vm_pressure_level')) {
        return PRESSURE_LEVEL_NORMAL_STDOUT;
      }
      if (command.endsWith('sysctl') && joined.includes('vm.swapusage')) return SWAP_USAGE_STDOUT;
      if (command.endsWith('ps')) return PS_TREE_WITH_SIX_AGENT_PIDS;
      throw enoentError(command);
    });

    // Real macrotask (`setImmediate`) resolution, not a manually-driven
    // microtask queue: each call increments `inFlight` synchronously on
    // entry and only decrements it once its own `setImmediate` callback
    // fires. If the implementation regressed to a single unbounded
    // `Promise.all` over all 6 pids, every call would fire (and increment
    // `inFlight`) synchronously up front, before any of them have had a
    // chance to resolve — `maxInFlight` would reach 6. A genuinely
    // batched implementation instead only ever has the current batch's
    // calls outstanding, so `maxInFlight` stays capped at the batch size.
    let inFlight = 0;
    let maxInFlight = 0;
    sampleFootprintMock.mockImplementation(
      () =>
        new Promise((resolve) => {
          inFlight += 1;
          maxInFlight = Math.max(maxInFlight, inFlight);
          setImmediate(() => {
            inFlight -= 1;
            resolve(42);
          });
        }),
    );

    const result = await runCollection({ mode: 'live' });

    expect(sampleFootprintMock).toHaveBeenCalledTimes(6);
    expect(maxInFlight).toBeLessThanOrEqual(4);
    expect(maxInFlight).toBeGreaterThan(0);
    expect(result.footprints.samples).toHaveLength(6);
  });
});
