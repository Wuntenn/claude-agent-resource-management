// RED unit tests for the not-yet-implemented
// scripts/agent-resource-management/lib/footprint-sampler.mjs module
// (Phase 1 — "footprint-sampler: sample per-agent memory footprint").
//
// Do NOT add a stub implementation to make this pass — that is the builder's
// job in the next phase. Every test below is expected to fail at IMPORT TIME
// (`Cannot find module './footprint-sampler.mjs'`) because the module does
// not exist yet.
//
// MOCKING IDIOM: this suite mocks `node:child_process` directly via
// `jest.unstable_mockModule`, rather than shelling out to the real
// `footprint`/`vmmap` binaries the way `probe-bound.jest.spec.mjs` shells out
// to real `sleep`. Two reasons this is the right call for THIS module and not
// a departure from house style:
//
//   1. `footprint`/`vmmap` are macOS-only tools with non-deterministic,
//      host-dependent output (`phys_footprint` for a live PID varies run to
//      run and does not exist at all on non-macOS CI runners). The exact
//      literal fixture text this suite needs to prove unit-parsing edge
//      cases (KB vs MB vs no-space `848K`) can only be produced by mocking —
//      real execution cannot hold the output byte-for-byte stable.
//   2. `dispatch.jest.spec.mjs` audited this codebase (grep, zero hits) and
//      confirmed there is no established ESM `jest.unstable_mockModule` idiom
//      to reuse OR avoid for `lib/` specs — this is a green field, and
//      `jest.unstable_mockModule` is Jest 30's own documented mechanism for
//      mocking a `node:` builtin under native ESM (this project already runs
//      `NODE_OPTIONS=--experimental-vm-modules` for `test:skills`).
//
// Fixture text below is copied LITERALLY from
// `.footprint-format-spike-1266.md` (Phase 0) — not invented, not
// reformatted. See that file for the full annotated raw captures.
//
// Unit-conversion contract this suite locks in for the implementer: MB
// values are computed using BINARY (1024-based) units, consistent with
// `footprint`'s own "(16384 bytes per page)" framing and macOS's other
// memory-reporting tools. 848 KB -> 848 / 1024 = 0.828125 MB.

import { jest } from '@jest/globals';

const execFileSyncMock = jest.fn();

jest.unstable_mockModule('node:child_process', () => ({
  execFileSync: execFileSyncMock,
}));

const { sampleFootprint } = await import('./footprint-sampler.mjs');
const { probeExecOptions } = await import('./probe-bound.mjs');

const FOOTPRINT_BIN = '/usr/bin/footprint';
const VMMAP_BIN = '/usr/bin/vmmap';

// ---------------------------------------------------------------------------
// Fixtures — literal copies from the Phase 0 spike.
// ---------------------------------------------------------------------------

const FOOTPRINT_LIGHT_SUCCESS_STDOUT = `======================================================================
sleep [39461]: 64-bit    Footprint: 832 KB (16384 bytes per page)
======================================================================

  Dirty      Clean  Reclaimable    Regions    Category
    ---        ---          ---        ---    ---
 240 KB        0 B          0 B          8    MALLOC metadata
 144 KB        0 B          0 B          1    page table
 132 KB        0 B          0 B         35    __DATA_DIRTY
  74 KB        0 B          0 B         34    __DATA
  50 KB        0 B          0 B        115    unused dyld shared cache area
  32 KB        0 B          0 B          1    MALLOC_TINY
  32 KB        0 B          0 B          5    untagged (VM_ALLOCATE)
  32 KB        0 B          0 B          2    stack
  32 KB        0 B          0 B          2    MALLOC_SMALL
  32 KB        0 B          0 B          2    __TPRO_CONST
  16 KB        0 B          0 B          1    os_alloc_once
  16 KB        0 B          0 B          1    tag 22
    0 B      16 KB          0 B         46    __TEXT
    0 B      16 KB          0 B         43    __DATA_CONST
    0 B      16 KB          0 B          3    __LINKEDIT
    0 B        0 B          0 B          1    dyld private memory
    0 B        0 B          0 B         45    __AUTH_CONST
    0 B        0 B          0 B         12    __AUTH
    0 B        0 B          0 B          1    mapped file
    ---        ---          ---        ---    ---
 832 KB      48 KB          0 B        360    TOTAL

Auxiliary data:
    phys_footprint: 848 KB
    phys_footprint_peak: 864 KB
`;

const FOOTPRINT_HEAVY_SUCCESS_STDOUT = `======================================================================
Google Chrome Helper (Renderer) [87033]: 64-bit    Footprint: 182 MB (16384 bytes per page)
======================================================================

  Dirty      Clean  Reclaimable    Regions    Category
    ---        ---          ---        ---    ---
  89 MB        0 B        57 MB       3776    app-specific tag 14
  68 MB        0 B        37 MB      14404    app-specific tag 16
  11 MB      80 KB          0 B       1015    __DATA_CONST
5184 KB        0 B          0 B        220    untagged (VM_ALLOCATE)
2906 KB        0 B          0 B        959    __DATA
1490 KB        0 B          0 B          1    page table
1392 KB        0 B          0 B         42    stack
 765 KB        0 B          0 B        864    __DATA_DIRTY
 752 KB        0 B          0 B          4    MALLOC_SMALL
 448 KB        0 B          0 B         15    Mach message
 416 KB        0 B          0 B          9    MALLOC metadata
 128 KB        0 B          0 B          1    libdispatch
 112 KB        0 B          0 B          2    __TPRO_CONST
  89 KB        0 B          0 B       2501    unused dyld shared cache area
  64 KB        0 B          0 B          1    MALLOC_TINY
  32 KB        0 B          0 B        618    __AUTH
  32 KB        0 B          0 B          1    Activity Tracing
  16 KB        0 B          0 B          1    tag 22
  16 KB        0 B          0 B          1    PROTECTED_MEMORY
  16 KB        0 B          0 B          1    ColorSync
  16 KB        0 B          0 B          1    os_alloc_once
    0 B        0 B        48 KB          6    ImageIO
    0 B      92 MB          0 B       1036    __TEXT
    0 B      14 MB          0 B         20    mapped file
    0 B     112 KB          0 B          4    __LINKEDIT
    0 B        0 B          0 B          1    __CTF
    0 B        0 B          0 B       1007    __AUTH_CONST
    0 B        0 B          0 B          1    __FONT_DATA
    ---        ---          ---        ---    ---
 182 MB     107 MB        94 MB      26514    TOTAL

Auxiliary data:
    phys_footprint: 182 MB
    phys_footprint_peak: 617 MB
`;

// Identical text/exit-code for both the never-allocated PID (999999) and a
// PID that just died (39461, per the spike) — `footprint` does not
// distinguish the two failure classes.
const FOOTPRINT_FAILURE_TEXT = `footprint: Unable to find pid for process matching '999999'
footprint: Unable to find any processes matching the supplied process names or pids (try as root?)
`;

const FOOTPRINT_FAILURE_TEXT_DEAD_PID = `footprint: Unable to find pid for process matching '39461'
footprint: Unable to find any processes matching the supplied process names or pids (try as root?)
`;

const VMMAP_SUCCESS_SUMMARY_STDOUT = `Process:         sleep [39461]
Path:            /bin/sleep
Load Address:    0x104ee0000
Identifier:      sleep
Version:         329
Code Type:       ARM64E
Platform:        macOS
Parent Process:  zsh [39459]
Target Type:     live task

Date/Time:       2026-08-25 00:07:27.490 +0100
Launch Time:     2026-08-25 00:06:50.297 +0100
OS Version:      macOS 26.5.2 (25F84)
Report Version:  7
Analysis Tool:   /usr/bin/vmmap

Physical footprint:         848K
Physical footprint (peak):  864K
Idle exit:                  untracked
----
`;

// Both the never-allocated PID (999999) and a PID that just died produce this
// exact two-line message shape and exit code 255 — per the spike.
const VMMAP_FAILURE_TEXT = `vmmap[40148]: vmmap cannot examine process 999999 (with name like '999999') because it no longer appears to be running.
vmmap[40148]: [fatal] mach port for process 0 not valid
`;

const VMMAP_FAILURE_TEXT_DEAD_PID = `vmmap[40228]: vmmap cannot examine process 39461 (with name like '39461') because it no longer appears to be running.
vmmap[40228]: [fatal] mach port for process 0 not valid
`;

/**
 * Builds an Error shaped like the one Node's `execFileSync` throws when the
 * child exits non-zero: `.status` is the exit code, `.stdout`/`.stderr` hold
 * the captured streams. The spike could not confirm which stream carries the
 * error text for either tool, so both fixtures populate BOTH streams —
 * whichever one the implementer reads, the text is there.
 */
function execFileSyncFailure(status, text) {
  const error = new Error(`Command failed with exit code ${status}.`);
  error.status = status;
  error.stdout = text;
  error.stderr = text;
  return error;
}

beforeEach(() => {
  execFileSyncMock.mockReset();
});

describe('samples via footprint when available', () => {
  it('parses a light-process footprint success fixture (848 KB) into an MB number', async () => {
    execFileSyncMock.mockReturnValue(FOOTPRINT_LIGHT_SUCCESS_STDOUT);

    const result = await sampleFootprint(39461);

    expect(result).not.toBeNull();
    expect(result).toBeCloseTo(848 / 1024, 6);
    expect(execFileSyncMock).toHaveBeenCalledTimes(1);
  });

  it('parses the heavier-process footprint fixture (182 MB phys_footprint), confirming unit parsing is not hardcoded to KB', async () => {
    execFileSyncMock.mockReturnValue(FOOTPRINT_HEAVY_SUCCESS_STDOUT);

    const result = await sampleFootprint(87033);

    expect(result).not.toBeNull();
    expect(result).toBeCloseTo(182, 6);
    expect(execFileSyncMock).toHaveBeenCalledTimes(1);
  });
});

describe('falls back to vmmap --summary when footprint shell-out fails', () => {
  it('returns the correct MB value from a no-space vmmap `848K` reading', async () => {
    execFileSyncMock.mockImplementation((bin) => {
      if (bin === FOOTPRINT_BIN) {
        throw execFileSyncFailure(66, FOOTPRINT_FAILURE_TEXT);
      }
      if (bin === VMMAP_BIN) {
        return VMMAP_SUCCESS_SUMMARY_STDOUT;
      }
      throw new Error(`unexpected binary invoked in test: ${bin}`);
    });

    const result = await sampleFootprint(39461);

    expect(result).not.toBeNull();
    expect(result).toBeCloseTo(848 / 1024, 6);
    expect(execFileSyncMock).toHaveBeenCalledTimes(2);
  });
});

describe('returns null when both shell-outs fail', () => {
  it('returns null without throwing when footprint exits 66 and vmmap exits 255', async () => {
    execFileSyncMock.mockImplementation((bin) => {
      if (bin === FOOTPRINT_BIN) {
        throw execFileSyncFailure(66, FOOTPRINT_FAILURE_TEXT);
      }
      if (bin === VMMAP_BIN) {
        throw execFileSyncFailure(255, VMMAP_FAILURE_TEXT);
      }
      throw new Error(`unexpected binary invoked in test: ${bin}`);
    });

    const result = await sampleFootprint(999999);

    expect(result).toBeNull();
    expect(execFileSyncMock).toHaveBeenCalledTimes(2);
  });
});

describe("returns null for a dead PID's fixture output (from Phase 0)", () => {
  it('returns null for the exact killed-process fixture text/exit codes captured in the spike', async () => {
    execFileSyncMock.mockImplementation((bin) => {
      if (bin === FOOTPRINT_BIN) {
        throw execFileSyncFailure(66, FOOTPRINT_FAILURE_TEXT_DEAD_PID);
      }
      if (bin === VMMAP_BIN) {
        throw execFileSyncFailure(255, VMMAP_FAILURE_TEXT_DEAD_PID);
      }
      throw new Error(`unexpected binary invoked in test: ${bin}`);
    });

    const result = await sampleFootprint(39461);

    expect(result).toBeNull();
  });
});

describe('never shells out to ps', () => {
  it('does not invoke the ps binary on either the success or failure path', async () => {
    execFileSyncMock.mockImplementation((bin) => {
      if (bin === FOOTPRINT_BIN) {
        throw execFileSyncFailure(66, FOOTPRINT_FAILURE_TEXT);
      }
      if (bin === VMMAP_BIN) {
        return VMMAP_SUCCESS_SUMMARY_STDOUT;
      }
      throw new Error(`unexpected binary invoked in test: ${bin}`);
    });

    await sampleFootprint(39461);

    for (const call of execFileSyncMock.mock.calls) {
      expect(call[0]).not.toMatch(/\/ps$/);
      expect(call[0]).not.toBe('ps');
    }
  });
});

describe('argv contains only the PID — no string interpolation', () => {
  it('passes an argv array containing exactly [String(pid)] to footprint', async () => {
    execFileSyncMock.mockReturnValue(FOOTPRINT_LIGHT_SUCCESS_STDOUT);

    await sampleFootprint(39461);

    expect(execFileSyncMock).toHaveBeenCalledTimes(1);
    const [bin, argv] = execFileSyncMock.mock.calls[0];
    expect(typeof bin).toBe('string');
    expect(bin.startsWith('/')).toBe(true);
    expect(argv).toEqual(['39461']);
  });

  it('passes an argv array containing exactly [String(pid)] to vmmap on fallback, with a fixed absolute bin path', async () => {
    execFileSyncMock.mockImplementation((bin) => {
      if (bin === FOOTPRINT_BIN) {
        throw execFileSyncFailure(66, FOOTPRINT_FAILURE_TEXT);
      }
      if (bin === VMMAP_BIN) {
        return VMMAP_SUCCESS_SUMMARY_STDOUT;
      }
      throw new Error(`unexpected binary invoked in test: ${bin}`);
    });

    await sampleFootprint(39461);

    expect(execFileSyncMock).toHaveBeenCalledTimes(2);
    const [footprintBin, footprintArgv] = execFileSyncMock.mock.calls[0];
    const [vmmapBin, vmmapArgv] = execFileSyncMock.mock.calls[1];

    expect(footprintBin).toBe(FOOTPRINT_BIN);
    expect(footprintArgv).toEqual(['39461']);
    expect(vmmapBin).toBe(VMMAP_BIN);
    expect(vmmapArgv).toEqual(['--summary', '39461']);
  });
});

describe('shell-out is time-bounded via probeExecOptions', () => {
  it('passes execFileSync options matching probeExecOptions() on the footprint call', async () => {
    execFileSyncMock.mockReturnValue(FOOTPRINT_LIGHT_SUCCESS_STDOUT);

    await sampleFootprint(39461);

    const [, , options] = execFileSyncMock.mock.calls[0];
    expect(options).toEqual(expect.objectContaining(probeExecOptions()));
  });

  it('passes execFileSync options matching probeExecOptions() on the vmmap fallback call', async () => {
    execFileSyncMock.mockImplementation((bin) => {
      if (bin === FOOTPRINT_BIN) {
        throw execFileSyncFailure(66, FOOTPRINT_FAILURE_TEXT);
      }
      if (bin === VMMAP_BIN) {
        return VMMAP_SUCCESS_SUMMARY_STDOUT;
      }
      throw new Error(`unexpected binary invoked in test: ${bin}`);
    });

    await sampleFootprint(39461);

    const [, , options] = execFileSyncMock.mock.calls[1];
    expect(options).toEqual(expect.objectContaining(probeExecOptions()));
  });
});

describe('honorFakeSeam gating (review, Medium)', () => {
  it('ignores ARM_FAKE_FOOTPRINT_OUTPUT and shells out for real when honorFakeSeam is false', async () => {
    process.env.ARM_FAKE_FOOTPRINT_OUTPUT = 'phys_footprint: 9999 MB';
    try {
      execFileSyncMock.mockReturnValue(FOOTPRINT_LIGHT_SUCCESS_STDOUT);

      const result = await sampleFootprint(39461, false);

      expect(result).not.toBe(9999);
      expect(result).toBeCloseTo(848 / 1024, 6);
      expect(execFileSyncMock).toHaveBeenCalledTimes(1);
    } finally {
      delete process.env.ARM_FAKE_FOOTPRINT_OUTPUT;
    }
  });

  it('still honors ARM_FAKE_FOOTPRINT_OUTPUT when honorFakeSeam is true (explicit, matches the default)', async () => {
    process.env.ARM_FAKE_FOOTPRINT_OUTPUT = 'phys_footprint: 9999 MB';
    try {
      const result = await sampleFootprint(39461, true);

      expect(result).toBe(9999);
      expect(execFileSyncMock).not.toHaveBeenCalled();
    } finally {
      delete process.env.ARM_FAKE_FOOTPRINT_OUTPUT;
    }
  });
});

describe('malformed footprint output', () => {
  it('returns null (not NaN, not a throw) when footprint succeeds but stdout has no phys_footprint line', async () => {
    execFileSyncMock.mockReturnValue('some unexpected garbled output with no auxiliary data section\n');

    const result = await sampleFootprint(39461);

    expect(result).toBeNull();
    expect(result).not.toBeNaN();
  });

  it('returns null (not NaN, not a throw) when footprint succeeds but the unit token on phys_footprint is garbled', async () => {
    execFileSyncMock.mockReturnValue('Auxiliary data:\n    phys_footprint: 848 ZB\n');

    const result = await sampleFootprint(39461);

    expect(result).toBeNull();
    expect(result).not.toBeNaN();
  });
});
