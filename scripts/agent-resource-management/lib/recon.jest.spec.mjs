// Phase 1 RED unit tests for the machine resource recon & sampling module
//. `scripts/agent-resource-management/lib/recon.mjs` does not exist
// yet — this file is red by construction (module-not-found) until Phase 1's
// implementer creates it. Do not add a stub implementation to force green.
//
// Contract under test (per the Build Plan's Phase 1 acceptance criteria):
//   - parseVmStat(raw)             -> { pressureLevel, compressedMb }
//   - parseSwapUsage(raw)          -> { swapUsedMb, swapTotalMb }
//   - parseFreeDisk(oldSample, newSample) -> { freeDiskGb, declineRateGbPerHour }
//   - attributeAgentProcesses(psTreeOutput) -> { agentCount, totalRssMb }
//   - takeSample(collect)          -> composed sampling entrypoint; degrades
//                                      to an explicit stale/unknown marker
//                                      rather than throwing when `collect()`
//                                      fails or returns malformed output.
//
// All parse functions are pure — no shell-out in this file. `collect()` is
// always an injected stub here, never a real OS call, per the outer test's
// hermeticity requirement.

import {
  parseVmStat,
  parsePressureLevel,
  parseSwapUsage,
  parseFreeDisk,
  attributeAgentProcesses,
  takeSample,
} from './recon.mjs';

// ---------------------------------------------------------------------------
// Fixtures — plausible macOS command output, not toy placeholder text.
// ---------------------------------------------------------------------------

// A healthy host: high free-page ratio, small compressor footprint.
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

// A pressured host: low free-page ratio, large compressor footprint.
const VM_STAT_PRESSURED = `Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                              18234.
Pages active:                          1834567.
Pages inactive:                         945678.
Pages speculative:                        1023.
Pages throttled:                             0.
Pages wired down:                       956789.
Pages purgeable:                          2567.
"Translation faults":                223456789.
Pages copy-on-write:                    2234567.
Pages zero filled:                     55678901.
Pages reactivated:                       198765.
Pages purged:                            143210.
File-backed pages:                      334567.
Anonymous pages:                         85678.
Pages stored in compressor:             612288.
Pages occupied by compressor:           487552.
Decompressions:                         1987654.
Compressions:                           3234567.
Pageins:                                4345678.
Pageouts:                                712345.
Swapins:                                   4512.
Swapouts:                                  8934.
`;

// dogfooding evidence from : a real host where "Pages free" is
// low but "Pages purgeable" (reclaimable-on-demand memory) is high, and
// swap/pressure otherwise read healthy. `freeRamMb` derived from "Pages
// free" alone under-reports available capacity on hosts shaped like this.
const VM_STAT_LOW_FREE_HIGH_PURGEABLE = `Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                               12000.
Pages active:                            234567.
Pages inactive:                          145678.
Pages speculative:                         2123.
Pages throttled:                              0.
Pages wired down:                        256789.
Pages purgeable:                         450000.
"Translation faults":                 123456789.
Pages copy-on-write:                    1234567.
Pages zero filled:                     45678901.
Pages reactivated:                        98765.
Pages purged:                             43210.
File-backed pages:                      234567.
Anonymous pages:                         45678.
Pages stored in compressor:                8192.
Pages occupied by compressor:              6144.
Decompressions:                          987654.
Compressions:                           1234567.
Pageins:                                2345678.
Pageouts:                                 12345.
Swapins:                                      0.
Swapouts:                                     0.
`;

// Non-regression fixture: both "Pages free" and "Pages purgeable" are low
// (purgeable scarce/zero) — the fix must not report more capacity than
// today's free-only formula when there is nothing extra to reclaim.
const VM_STAT_LOW_FREE_LOW_PURGEABLE = `Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                               15000.
Pages active:                            734567.
Pages inactive:                          345678.
Pages speculative:                         1023.
Pages throttled:                              0.
Pages wired down:                        556789.
Pages purgeable:                              0.
"Translation faults":                 223456789.
Pages copy-on-write:                    2234567.
Pages zero filled:                     55678901.
Pages reactivated:                       198765.
Pages purged:                            143210.
File-backed pages:                      334567.
Anonymous pages:                         85678.
Pages stored in compressor:              212288.
Pages occupied by compressor:            187552.
Decompressions:                         1987654.
Compressions:                           3234567.
Pageins:                                4345678.
Pageouts:                                712345.
Swapins:                                      0.
Swapouts:                                      0.
`;

// sysctl-style swap usage, as reported by `sysctl vm.swapusage`.
const SWAP_USAGE_LIGHT = 'vm.swapusage: total = 2048.00M  used = 256.00M  free = 1792.00M  (encrypted)';
const SWAP_USAGE_HEAVY = 'vm.swapusage: total = 4096.00M  used = 3584.00M  free = 512.00M  (encrypted)';

// df-style output (macOS `df -g /`), two samples taken an hour apart.
const DF_SAMPLE_OLD = {
  raw: `Filesystem     1G-blocks  Used Available Capacity  Mounted on
/dev/disk3s1s1       460   410       45    91%   /
`,
  timestampMs: Date.parse('2026-08-02T09:00:00Z'),
};
const DF_SAMPLE_NEW = {
  raw: `Filesystem     1G-blocks  Used Available Capacity  Mounted on
/dev/disk3s1s1       460   414       41    92%   /
`,
  timestampMs: Date.parse('2026-08-02T10:00:00Z'),
};

// `ps -Ao pid,ppid,comm` style output forming a process tree where a `node`
// process and an Electron helper are descendants of a `claude`-launched
// root — this is the exact shape the Investigation's caveat calls out: a
// name-substring match on "claude" alone would undercount the `node` child.
const PS_TREE_CLAUDE_LAUNCHED_TREE = `  PID  PPID COMM
    1     0 /sbin/launchd
  501     1 /usr/libexec/loginwindow
 9001   501 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001 node /Applications/Claude.app/Contents/Resources/app/cli.js
 9003  9002 /Applications/Claude.app/Contents/Frameworks/Claude Helper.app/Contents/MacOS/Claude Helper
 9004  9002 /Applications/Claude.app/Contents/Frameworks/Claude Helper (Renderer).app/Contents/MacOS/Claude Helper (Renderer)
  700     1 /usr/sbin/mDNSResponder
  701   700 node /Users/dev/unrelated-server/index.js
`;

const PS_TREE_IDLE = `  PID  PPID COMM
    1     0 /sbin/launchd
  501     1 /usr/libexec/loginwindow
  700     1 /usr/sbin/mDNSResponder
`;

const PS_TREE_EMPTY = '';

// A capitalized `.../Claude` root — the Claude DESKTOP app's own main
// process binary path — alongside a genuine lowercase `.../claude` root (the
// CLI). Proves `AGENT_ROOT_COMM_PATTERN` is case-SENSITIVE: the capitalized
// desktop-app process (and its Electron helper tree) must never be
// attributed as an agent root, while the lowercase CLI-rooted tree still is.
const PS_TREE_DESKTOP_APP_AND_CLI_ROOT = `  PID  PPID COMM
    1     0 /sbin/launchd
  501     1 /usr/libexec/loginwindow
 9001   501 /Applications/Claude.app/Contents/MacOS/claude
 9002  9001 node /Applications/Claude.app/Contents/Resources/app/cli.js
47960   501 /Applications/Claude.app/Contents/MacOS/Claude
47961 47960 /Applications/Claude.app/Contents/Frameworks/Claude Helper (Renderer).app/Contents/MacOS/Claude Helper (Renderer)
`;

// ---------------------------------------------------------------------------
// parseVmStat
// ---------------------------------------------------------------------------

describe('parseVmStat', () => {
  it('parses a healthy vm_stat sample into a low pressure level with a small compressed footprint', () => {
    const result = parseVmStat(VM_STAT_HEALTHY);

    expect(result.pressureLevel).toBeGreaterThanOrEqual(1);
    expect(result.pressureLevel).toBeLessThanOrEqual(2);
    // 6144 compressor pages * 16384 bytes/page / (1024*1024) = 96 MB.
    expect(result.compressedMb).toBeCloseTo(96, 0);
    // freeRamMb must incorporate "Pages purgeable" (4567 pages), not
    // just "Pages free" (612345 pages) alone.
    // (612345 + 4567) * 16384 / (1024*1024) MB.
    const expectedFreeRamMb = ((612345 + 4567) * 16384) / (1024 * 1024);
    expect(result.freeRamMb).toBeCloseTo(expectedFreeRamMb, 0);
    // pressureLevel is derived solely from the six page counts it always
    // was — proving the purgeable-aware freeRamMb change didn't leak into
    // this separate calculation. freeRatio = 612345 / 2159646 ≈ 0.2836,
    // which is >= 0.25, so pressureLevel is exactly 1 (byte-identical to
    // the pre-fix expected value), not merely "in range".
    expect(result.pressureLevel).toBe(1);
  });

  it('parses a pressured vm_stat sample into a high pressure level with a large compressed footprint', () => {
    const result = parseVmStat(VM_STAT_PRESSURED);

    expect(result.pressureLevel).toBeGreaterThanOrEqual(3);
    // 487552 compressor pages * 16384 bytes/page / (1024*1024) = 7618 MB.
    expect(result.compressedMb).toBeCloseTo(7618, 0);
    // freeRamMb must incorporate "Pages purgeable" (2567 pages), not
    // just "Pages free" (18234 pages) alone.
    const expectedFreeRamMb = ((18234 + 2567) * 16384) / (1024 * 1024);
    expect(result.freeRamMb).toBeCloseTo(expectedFreeRamMb, 0);
    // freeRatio = 18234 / 4243843 ≈ 0.0043, which is < 0.08, so
    // pressureLevel is exactly 4 (byte-identical to the pre-fix expected
    // value) — proving the purgeable-aware freeRamMb change didn't leak
    // into this separate calculation.
    expect(result.pressureLevel).toBe(4);
  });

  it(': incorporates "Pages purgeable" into freeRamMb, materially increasing it over the free-pages-only value when purgeable pages are plentiful and swap/pressure otherwise read healthy', () => {
    const result = parseVmStat(VM_STAT_LOW_FREE_HIGH_PURGEABLE);

    const freeOnlyMb = (12000 * 16384) / (1024 * 1024);
    // "Pages free" alone (12000 pages) reports ~187 MB; "Pages purgeable"
    // (450000 pages) is reclaimable on demand and must be reflected too —
    // the fixed formula must report materially more headroom than the
    // free-pages-only formula, not merely a rounding-level difference.
    expect(result.freeRamMb).toBeGreaterThan(freeOnlyMb * 2);
  });

  it(': does NOT regress the conservative free-pages-only result when "Pages purgeable" is scarce (must not just always report more capacity)', () => {
    const result = parseVmStat(VM_STAT_LOW_FREE_LOW_PURGEABLE);

    // Today's free-only formula: "Pages free" (15000) * pageSizeBytes
    // (16384) / (1024*1024).
    const freeOnlyMb = (15000 * 16384) / (1024 * 1024);
    expect(result.freeRamMb).toBeCloseTo(freeOnlyMb, 5);
  });

  it(': "Pages purgeable: 0." boundary case collapses freeRamMb exactly to the free-only value', () => {
    // Reuses VM_STAT_HEALTHY's free-page profile but zeroes out purgeable,
    // proving the boundary holds independent of the free-page level.
    const zeroedPurgeable = VM_STAT_HEALTHY.split('\n')
      .map((line) => (/^Pages\s+purgeable/i.test(line) ? line.replace(/\d+\.\s*$/, '0.') : line))
      .join('\n');
    const result = parseVmStat(zeroedPurgeable);

    const freeOnlyMb = (612345 * 16384) / (1024 * 1024);
    expect(result.freeRamMb).toBeCloseTo(freeOnlyMb, 5);
  });

  it(': missing/truncated "Pages purgeable" label degrades freeRamMb to NaN (per this file\'s "don\'t fabricate" convention), leaving other fields whose inputs are unaffected intact', () => {
    // Mirrors the existing missing-"Pages free" truncation test below:
    // simulates a truncated vm_stat capture that cuts off before "Pages
    // purgeable" was written, while "Pages free" and every other label
    // (including the compressor line) are intact.
    const truncated = VM_STAT_HEALTHY.split('\n')
      .filter((line) => !/^Pages\s+purgeable/i.test(line))
      .join('\n');
    const result = parseVmStat(truncated);

    expect(Number.isNaN(result.freeRamMb)).toBe(true);
    // compressedMb depends only on the (present) compressor line and must
    // still parse correctly.
    expect(result.compressedMb).toBeCloseTo(96, 0);
    // pressureLevel is derived from the six page counts, none of which is
    // "Pages purgeable" — must remain unaffected, not collapse to NaN too.
    expect(result.pressureLevel).toBe(1);
  });

  it('is tolerant of extra leading/trailing whitespace and differing column spacing across macOS point releases', () => {
    const wobblyFormatting = VM_STAT_HEALTHY.split('\n')
      .map((line) => `   ${line.replace(/\s+/g, '   ')}   `)
      .join('\n');

    expect(() => parseVmStat(wobblyFormatting)).not.toThrow();
    const result = parseVmStat(wobblyFormatting);
    expect(result.pressureLevel).toBeGreaterThanOrEqual(1);
    expect(Number.isFinite(result.compressedMb)).toBe(true);
  });

  it('reports NaN sentinels — not a fabricated worst-case pressure level — when the input is garbled/unparseable', () => {
    const result = parseVmStat('garbage');

    expect(Number.isNaN(result.pressureLevel)).toBe(true);
    expect(Number.isNaN(result.compressedMb)).toBe(true);
    // freeRamMb must degrade to NaN on garbled input too, not a
    // fabricated 0 that a headroom calculation could misread as "no free
    // RAM" (falsely conservative) or silently pass through as 0.
    expect(Number.isNaN(result.freeRamMb)).toBe(true);
    // The old behaviour fabricated the worst-case RED reading; guard against
    // regressing back to it.
    expect(result).not.toEqual({ pressureLevel: 4, compressedMb: 0 });
  });

  it('reports NaN sentinels for empty input rather than a fabricated reading', () => {
    const result = parseVmStat('');

    expect(Number.isNaN(result.pressureLevel)).toBe(true);
    expect(Number.isNaN(result.compressedMb)).toBe(true);
    // freeRamMb must degrade to NaN on empty input too.
    expect(Number.isNaN(result.freeRamMb)).toBe(true);
  });

  it('degrades only the field(s) whose own label is missing on a partially-truncated sample — not the whole reading', () => {
    // Simulates a truncated vm_stat capture that cuts off before "Pages
    // free" was written, while every other label (including the compressor
    // line that compressedMb depends on) is intact.
    const truncated = VM_STAT_HEALTHY.split('\n')
      .filter((line) => !/^Pages\s+free/i.test(line))
      .join('\n');
    const result = parseVmStat(truncated);

    // pressureLevel is derived from all six page counts, so a missing
    // "Pages free" must degrade it to NaN...
    expect(Number.isNaN(result.pressureLevel)).toBe(true);
    // ...but compressedMb depends only on the (present) compressor line and
    // must still parse correctly — not silently default to 0, and not
    // collapse the whole reading to NaN.
    expect(result.compressedMb).toBeCloseTo(96, 0);
    // freeRamMb is derived from "Pages free" (now combined with
    // "Pages purgeable") — a missing "Pages free" label must degrade it to
    // NaN too, even though "Pages purgeable" is present in this fixture.
    expect(Number.isNaN(result.freeRamMb)).toBe(true);
  });

  // -------------------------------------------------------------------------
  // compressor VELOCITY counters — cumulative `Compressions:` /
  // `Decompressions:` lines, parsed via the SAME `field()` helper/regex
  // convention as every other label in this function (not a bespoke parser).
  // RED by construction: `parseVmStat`'s return shape does not yet include
  // `compressions`/`decompressions`.
  // -------------------------------------------------------------------------
  describe('compressor velocity counters (compressions / decompressions)', () => {
    it('parses a realistic captured sample\'s cumulative Compressions/Decompressions counters into non-NaN values', () => {
      // VM_STAT_HEALTHY already carries genuine-shaped counter lines:
      //   Decompressions:                          987654.
      //   Compressions:                           1234567.
      const result = parseVmStat(VM_STAT_HEALTHY);

      expect(result.compressions).toBe(1234567);
      expect(result.decompressions).toBe(987654);
    });

    it('yields NaN for compressions/decompressions on an older-macOS sample missing those lines entirely, without throwing and without perturbing the other existing fields', () => {
      const withoutCounters = VM_STAT_HEALTHY.split('\n')
        .filter((line) => !/^(Compressions|Decompressions)\s*:/i.test(line))
        .join('\n');

      expect(() => parseVmStat(withoutCounters)).not.toThrow();
      const result = parseVmStat(withoutCounters);

      expect(Number.isNaN(result.compressions)).toBe(true);
      expect(Number.isNaN(result.decompressions)).toBe(true);
      // Sibling fields, derived from wholly different labels, must be
      // completely unaffected by the missing counter lines.
      expect(result.pressureLevel).toBe(1);
      expect(result.compressedMb).toBeCloseTo(96, 0);
      const expectedFreeRamMb = ((612345 + 4567) * 16384) / (1024 * 1024);
      expect(result.freeRamMb).toBeCloseTo(expectedFreeRamMb, 0);
    });

    it('treats a counter value of exactly 0 as a valid, non-NaN reading — not conflated with "missing"', () => {
      const zeroedCounters = VM_STAT_HEALTHY.replace(/Decompressions:\s*\d+\./, 'Decompressions:                               0.').replace(
        /Compressions:\s*\d+\./,
        'Compressions:                                 0.',
      );
      const result = parseVmStat(zeroedCounters);

      expect(result.compressions).toBe(0);
      expect(result.decompressions).toBe(0);
      expect(Number.isNaN(result.compressions)).toBe(false);
      expect(Number.isNaN(result.decompressions)).toBe(false);
    });

    it('parses a very large, comma-separated counter near Number.MAX_SAFE_INTEGER correctly', () => {
      const hugeCounters = VM_STAT_HEALTHY.replace(
        /Decompressions:\s*\d+\./,
        'Decompressions:              9,007,199,254,740,991.',
      ).replace(/Compressions:\s*\d+\./, 'Compressions:                9,007,199,254,740,991.');
      const result = parseVmStat(hugeCounters);

      expect(result.compressions).toBe(Number.MAX_SAFE_INTEGER);
      expect(result.decompressions).toBe(Number.MAX_SAFE_INTEGER);
    });
  });
});

// ---------------------------------------------------------------------------
// parseSwapUsage
// ---------------------------------------------------------------------------

describe('parseSwapUsage', () => {
  it('parses light swap usage from sysctl-style output', () => {
    expect(parseSwapUsage(SWAP_USAGE_LIGHT)).toEqual({
      swapUsedMb: 256,
      swapTotalMb: 2048,
    });
  });

  it('parses heavy swap usage from sysctl-style output', () => {
    expect(parseSwapUsage(SWAP_USAGE_HEAVY)).toEqual({
      swapUsedMb: 3584,
      swapTotalMb: 4096,
    });
  });

  it('is tolerant of extra whitespace between fields', () => {
    const wobbly = 'vm.swapusage:   total   =   1024.00M    used   =   0.00M    free   =   1024.00M';
    expect(parseSwapUsage(wobbly)).toEqual({ swapUsedMb: 0, swapTotalMb: 1024 });
  });

  it('reports NaN sentinels — not a fabricated healthy-looking all-zero reading — for empty/unparseable input', () => {
    const result = parseSwapUsage('');

    expect(Number.isNaN(result.swapUsedMb)).toBe(true);
    expect(Number.isNaN(result.swapTotalMb)).toBe(true);
    // The old behaviour fabricated an all-zero reading that reads as
    // healthy/GREEN downstream; guard against regressing back to it.
    expect(result).not.toEqual({ swapUsedMb: 0, swapTotalMb: 0 });
  });

  it('reports NaN sentinels for garbled/unparseable input', () => {
    const result = parseSwapUsage('garbage');

    expect(Number.isNaN(result.swapUsedMb)).toBe(true);
    expect(Number.isNaN(result.swapTotalMb)).toBe(true);
  });

  it('degrades only the missing field on a partially-truncated sample — not the whole reading', () => {
    // Simulates a truncated sysctl capture that cuts off before "used" was
    // written, while "total" is intact.
    const truncated = 'vm.swapusage: total = 2048.00M';
    const result = parseSwapUsage(truncated);

    expect(Number.isNaN(result.swapUsedMb)).toBe(true);
    // "total" is present and must still parse correctly, not silently
    // default to 0.
    expect(result.swapTotalMb).toBe(2048);
  });
});

// ---------------------------------------------------------------------------
// parsePressureLevel
// ---------------------------------------------------------------------------

describe('parsePressureLevel', () => {
  it('parses "1" (normal) from sysctl -n kern.memorystatus_vm_pressure_level output', () => {
    expect(parsePressureLevel('1\n')).toBe(1);
  });

  it('parses "2" (warn)', () => {
    expect(parsePressureLevel('2\n')).toBe(2);
  });

  it('parses "4" (critical)', () => {
    expect(parsePressureLevel('4\n')).toBe(4);
  });

  it('is tolerant of surrounding whitespace with no trailing newline', () => {
    expect(parsePressureLevel('  1  ')).toBe(1);
  });

  it('reports NaN — not a fabricated healthy "1" — for empty/unparseable input', () => {
    expect(Number.isNaN(parsePressureLevel(''))).toBe(true);
  });

  it('reports NaN for garbled/unparseable input', () => {
    expect(Number.isNaN(parsePressureLevel('garbage'))).toBe(true);
  });

  it('reports NaN for null/undefined input', () => {
    expect(Number.isNaN(parsePressureLevel(null))).toBe(true);
    expect(Number.isNaN(parsePressureLevel(undefined))).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// parseFreeDisk
// ---------------------------------------------------------------------------

describe('parseFreeDisk', () => {
  it('computes free_disk_gb from the newer sample and a decline rate from the trend between two samples', () => {
    const result = parseFreeDisk(DF_SAMPLE_OLD, DF_SAMPLE_NEW);

    expect(result.freeDiskGb).toBe(41);
    // 45 -> 41 GB over 1 hour = 4 GB/hour decline.
    expect(result.declineRateGbPerHour).toBeCloseTo(4, 5);
  });

  it('reports a zero (not negative-signed-as-growth-confused) decline rate when free disk is flat', () => {
    const flatNew = { ...DF_SAMPLE_NEW, raw: DF_SAMPLE_OLD.raw };
    const result = parseFreeDisk(DF_SAMPLE_OLD, flatNew);

    expect(result.declineRateGbPerHour).toBeCloseTo(0, 5);
  });

  it('reports a negative decline rate (i.e. disk is recovering) when free space increases between samples', () => {
    const recovering = {
      raw: `Filesystem     1G-blocks  Used Available Capacity  Mounted on
/dev/disk3s1s1       460   400       55    88%   /
`,
      timestampMs: DF_SAMPLE_NEW.timestampMs,
    };
    const result = parseFreeDisk(DF_SAMPLE_OLD, recovering);

    expect(result.declineRateGbPerHour).toBeLessThan(0);
  });

  it('reports NaN sentinels — not a fabricated healthy-looking 0/0 reading — when neither sample is parseable', () => {
    const garbledSample = { raw: 'garbage', timestampMs: DF_SAMPLE_OLD.timestampMs };
    const result = parseFreeDisk(garbledSample, { ...garbledSample, timestampMs: DF_SAMPLE_NEW.timestampMs });

    expect(Number.isNaN(result.freeDiskGb)).toBe(true);
    expect(Number.isNaN(result.declineRateGbPerHour)).toBe(true);
    // The old behaviour fabricated an all-zero reading that reads as
    // healthy/GREEN downstream; guard against regressing back to it.
    expect(result).not.toEqual({ freeDiskGb: 0, declineRateGbPerHour: 0 });
  });

  it('reports NaN sentinels for empty input on both samples', () => {
    const emptySample = { raw: '', timestampMs: DF_SAMPLE_OLD.timestampMs };
    const result = parseFreeDisk(emptySample, { ...emptySample, timestampMs: DF_SAMPLE_NEW.timestampMs });

    expect(Number.isNaN(result.freeDiskGb)).toBe(true);
    expect(Number.isNaN(result.declineRateGbPerHour)).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// attributeAgentProcesses — parent-process ancestry, not name-substring match
// ---------------------------------------------------------------------------

describe('attributeAgentProcesses', () => {
  it('counts a node process by ancestry when it is a child of a claude-launched process tree, not by name match alone', () => {
    const result = attributeAgentProcesses(PS_TREE_CLAUDE_LAUNCHED_TREE);

    // Root claude process (9001), its node child (9002), and both Electron
    // helper grandchildren (9003, 9004) all descend from the same claude
    // root and must all be attributed to the agent tree.
    expect(result.agentCount).toBe(4);
  });

  it('does not attribute an unrelated node process outside any claude-rooted ancestry, proving this is ancestry-based, not a "node"/"claude" name-substring match', () => {
    const result = attributeAgentProcesses(PS_TREE_CLAUDE_LAUNCHED_TREE);

    // PID 701 is a `node` process, but its parent (700, mDNSResponder) is
    // not descended from any claude-launched root. A name-substring
    // matcher on "node" would wrongly include it; ancestry-based
    // attribution must not.
    expect(result.agentCount).toBeLessThan(5);
  });

  it('handles an idle host with zero agent-related processes without throwing or dividing by zero', () => {
    expect(() => attributeAgentProcesses(PS_TREE_IDLE)).not.toThrow();
    const result = attributeAgentProcesses(PS_TREE_IDLE);

    expect(result.agentCount).toBe(0);
    expect(result.totalRssMb).toBe(0);
    expect(Number.isFinite(result.totalRssMb)).toBe(true);
  });

  it('handles completely empty process-tree output without throwing', () => {
    expect(() => attributeAgentProcesses(PS_TREE_EMPTY)).not.toThrow();
    const result = attributeAgentProcesses(PS_TREE_EMPTY);

    expect(result.agentCount).toBe(0);
  });

  // Verified-Critical regression: on a real macOS host, the Claude DESKTOP
  // app's main process binary path ends in capitalized `/Claude`, distinct
  // from the Claude Code CLI's lowercase `/claude`. A case-insensitive root
  // pattern previously matched both, so the desktop app's own process tree
  // could be misattributed as an agent tree. `AGENT_ROOT_COMM_PATTERN` must
  // stay case-sensitive.
  it('does not attribute a capitalized `.../Claude` desktop-app process (or its descendants) as an agent tree, while a genuine lowercase `.../claude` root still is', () => {
    const result = attributeAgentProcesses(PS_TREE_DESKTOP_APP_AND_CLI_ROOT);

    // Only the lowercase root (9001) and its node child (9002) are
    // attributed — never the capitalized root (47960) or its Electron
    // helper child (47961).
    expect(result.agentCount).toBe(2);
  });

  // `AGENT_ROOT_COMM_PATTERN` is `/\/claude$/` — the `comm` value must END IN
  // `/claude`, i.e. it must be a PATH. A bare basename `claude` is NOT an
  // agent root, and that is a deliberate property rather than an accident of
  // the regex:
  //
  //   - macOS/BSD `ps -o comm` prints the RESOLVED EXECUTABLE PATH, which is
  //     the host ARM targets (this design's admission signals are macOS-only).
  //     Requiring the path means neither `argv[0]` nor a process merely
  //     NAMED `claude` can pass — the executable file itself has to be.
  //   - Widening to `/(^|\/)claude$/` to accommodate Linux/procps `ps`, whose
  //     `comm` is a 15-char basename that can never contain a `/`, would
  //     silently make any bare-`claude`-named process an agent tree root on
  //     the real host too.
  //
  // Nothing else in this suite pinned that boundary: the widened pattern
  // passed every other test in this file and in ./recon-per-agent.jest.spec.mjs
  // unchanged. Pinned here, next to the pattern it protects, so a future
  // widening is a red test rather than a silent semantic change.
  it('does not attribute a bare-basename `claude` comm as an agent root — the pattern requires a resolved path, not a name', () => {
    const basenameOnlyTree = `  PID  PPID COMM
    1     0 systemd
  900     1 claude
  901   900 node
`;

    expect(attributeAgentProcesses(basenameOnlyTree).agentCount).toBe(0);

    // …and the path-shaped equivalent of the same tree still is one, so the
    // assertion above is about the `comm` FORMAT and not about the fixture
    // being unparseable.
    const pathShapedTree = `  PID  PPID COMM
    1     0 /sbin/launchd
  900     1 /usr/local/bin/claude
  901   900 /usr/local/bin/node
`;

    expect(attributeAgentProcesses(pathShapedTree).agentCount).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// takeSample(collect) — composed sampling entrypoint; stale/unknown must be
// an explicit, distinct, testable marker — never a silent GREEN-compatible
// default and never `undefined`. This is the exact gap the Convergence
// Analysis flagged: "does a stale/unknown sample correctly propagate — must
// not be silently read as GREEN downstream".
// ---------------------------------------------------------------------------

describe('takeSample(collect)', () => {
  it('returns the structured sample produced by a well-formed collect()', async () => {
    const wellFormedSample = {
      memory: { pressureLevel: 1, swapUsedMb: 256, compressedMb: 96 },
      disk: { freeDiskGb: 120, declineRateGbPerHour: 0.2 },
    };
    const collect = async () => wellFormedSample;

    const result = await takeSample(collect);

    expect(result.stale).toBe(false);
    expect(result.memory).toEqual(wellFormedSample.memory);
    expect(result.disk).toEqual(wellFormedSample.disk);
  });

  it('degrades to an explicit stale/unknown marker when collect() throws, rather than throwing itself', async () => {
    const collect = async () => {
      throw new Error('vm_stat timed out under extreme host load');
    };

    await expect(takeSample(collect)).resolves.toBeDefined();
    const result = await takeSample(collect);

    expect(result.stale).toBe(true);
  });

  it('degrades to an explicit stale/unknown marker when collect() resolves with malformed/empty output', async () => {
    const collect = async () => ({});

    const result = await takeSample(collect);

    expect(result.stale).toBe(true);
  });

  it('degrades to an explicit stale/unknown marker when collect() resolves with null', async () => {
    const collect = async () => null;

    const result = await takeSample(collect);

    expect(result.stale).toBe(true);
  });

  it('produces a stale marker that is a distinct, testable value — not merely `undefined` and not a silently-passing default that downstream code could misread as GREEN', async () => {
    const collect = async () => undefined;

    const result = await takeSample(collect);

    // The marker itself must exist and be explicitly truthy/typed, not an
    // accidental `undefined` that a careless `if (sample.stale)` check
    // would still catch today but a refactor could silently break.
    expect(result).not.toBeUndefined();
    expect(typeof result.stale).toBe('boolean');
    expect(result.stale).toBe(true);
    // The stale sample must not carry a shape that a downstream classifier
    // could mistake for a real, healthy (GREEN-compatible) reading.
    expect(result.memory).not.toEqual({ pressureLevel: 1, swapUsedMb: 0, compressedMb: 0 });
  });

  it('degrades to an explicit stale/unknown marker when collect() resolves with a well-formed-shaped sample whose readings are the NaN sentinel from garbled/unparseable command output', async () => {
    // Simulates parseVmStat('garbage') feeding through into the composed
    // sample: the shape check alone would pass, but the underlying command
    // text was unparseable.
    const garbledSample = {
      memory: { pressureLevel: NaN, swapUsedMb: 0, compressedMb: NaN },
      disk: { freeDiskGb: 120, declineRateGbPerHour: 0.2 },
    };
    const collect = async () => garbledSample;

    const result = await takeSample(collect);

    expect(result.stale).toBe(true);
    // Must not leak the NaN-bearing reading through as if it were real data.
    expect(result.memory).not.toEqual(garbledSample.memory);
  });

  it('never calls collect() more than once per invocation (no retry storm under a failing collector)', async () => {
    let calls = 0;
    const collect = async () => {
      calls += 1;
      throw new Error('boom');
    };

    await takeSample(collect);

    expect(calls).toBe(1);
  });
});
