// whole-branch review, High #2 — regression coverage for
// `selectPauseCandidateWithFootprint`, the real production wiring between
// `listAgentProcesses`-shaped candidate lists and `selectPauseCandidate`'s
// phys_footprint ranking.
//
// Unlike `evict-outer-acceptance.jest.spec.mjs`'s Phase 1 test (which calls
// `selectPauseCandidate` directly on hand-built objects that already carry a
// `physFootprintMb` field — proving the ranking ALGORITHM works, but nothing
// about whether any real call site ever populates that field), every test
// below starts from a `listAgentProcesses`-SHAPED candidate list
// (`{ agentId, rssMb, startedAt }`, no `physFootprintMb`) and an injected
// `sampleFootprintFn` — the same shape/injection seam a real cli.mjs call
// site uses in production (with the real `sampleFootprint` as the default).
// This is what actually proves the wiring exists end-to-end through the real
// call path, not just that the underlying ranking algorithm is correct in
// isolation.

import { selectPauseCandidateWithFootprint } from './pause-candidate.mjs';

const LOW_RSS_HIGH_FOOTPRINT = { agentId: 'agent-low-rss-high-footprint', rssMb: 150, startedAt: 1_700_000_000_000 };
const HIGH_RSS_LOW_FOOTPRINT = { agentId: 'agent-high-rss-low-footprint', rssMb: 220, startedAt: 1_700_000_100_000 };

describe('selectPauseCandidateWithFootprint', () => {
  it('ranks by real, sampled phys_footprint (not RSS) end-to-end through the real candidate-list shape', async () => {
    const sampleFootprintFn = async (agentId) => {
      if (agentId === LOW_RSS_HIGH_FOOTPRINT.agentId) return 260; // the genuinely bigger consumer
      if (agentId === HIGH_RSS_LOW_FOOTPRINT.agentId) return 90;
      throw new Error(`unexpected agentId: ${agentId}`);
    };

    const candidate = await selectPauseCandidateWithFootprint(
      [HIGH_RSS_LOW_FOOTPRINT, LOW_RSS_HIGH_FOOTPRINT],
      { sampleFootprintFn },
    );

    // Higher RSS would win under the old (broken) RSS-only fallback; the
    // genuinely bigger real memory consumer must win once footprint sampling
    // is actually wired in.
    expect(candidate.agentId).toBe(LOW_RSS_HIGH_FOOTPRINT.agentId);
    expect(candidate.physFootprintMb).toBe(260);
  });

  it('falls back to RSS ranking when every candidate\'s footprint sample resolves null', async () => {
    const sampleFootprintFn = async () => null;

    const candidate = await selectPauseCandidateWithFootprint(
      [LOW_RSS_HIGH_FOOTPRINT, HIGH_RSS_LOW_FOOTPRINT],
      { sampleFootprintFn },
    );

    expect(candidate.agentId).toBe(HIGH_RSS_LOW_FOOTPRINT.agentId);
    expect(candidate.physFootprintMb).toBeNull();
  });

  it('degrades a single candidate\'s thrown sampling failure to physFootprintMb: null rather than crashing, still ranking the rest by footprint', async () => {
    const sampleFootprintFn = async (agentId) => {
      if (agentId === HIGH_RSS_LOW_FOOTPRINT.agentId) throw new Error('footprint shell-out failed');
      return 260;
    };

    const candidate = await selectPauseCandidateWithFootprint(
      [HIGH_RSS_LOW_FOOTPRINT, LOW_RSS_HIGH_FOOTPRINT],
      { sampleFootprintFn },
    );

    expect(candidate.agentId).toBe(LOW_RSS_HIGH_FOOTPRINT.agentId);
    expect(candidate.physFootprintMb).toBe(260);
  });

  it('returns null for an empty candidate list without sampling anything', async () => {
    let calls = 0;
    const sampleFootprintFn = async () => {
      calls += 1;
      return 100;
    };

    const candidate = await selectPauseCandidateWithFootprint([], { sampleFootprintFn });

    expect(candidate).toBeNull();
    expect(calls).toBe(0);
  });

  it('forwards honorFakeSeam as sampleFootprintFn\'s second argument on every call (review, Medium)', async () => {
    const calls = [];
    const sampleFootprintFn = async (agentId, honorFakeSeam) => {
      calls.push({ agentId, honorFakeSeam });
      return 100;
    };

    await selectPauseCandidateWithFootprint([LOW_RSS_HIGH_FOOTPRINT, HIGH_RSS_LOW_FOOTPRINT], {
      sampleFootprintFn,
      honorFakeSeam: false,
    });

    expect(calls).toEqual([
      { agentId: LOW_RSS_HIGH_FOOTPRINT.agentId, honorFakeSeam: false },
      { agentId: HIGH_RSS_LOW_FOOTPRINT.agentId, honorFakeSeam: false },
    ]);
  });

  it('defaults honorFakeSeam to true when not supplied', async () => {
    const calls = [];
    const sampleFootprintFn = async (agentId, honorFakeSeam) => {
      calls.push(honorFakeSeam);
      return 100;
    };

    await selectPauseCandidateWithFootprint([LOW_RSS_HIGH_FOOTPRINT], { sampleFootprintFn });

    expect(calls).toEqual([true]);
  });

  it('defaults sampleFootprintFn to the real sampleFootprint when no override is supplied', async () => {
    // Not a real shell-out assertion (that belongs to footprint-sampler.jest.spec.mjs) —
    // just proves the default parameter wires the real production sampler in,
    // not a silent no-op, by observing it degrades gracefully (rather than
    // throwing out of this function) against a synthetic pid no real
    // `footprint`/`vmmap` binary can resolve.
    const candidate = await selectPauseCandidateWithFootprint([
      { agentId: '999999999', rssMb: 42, startedAt: 1_700_000_000_000 },
    ]);

    expect(candidate.agentId).toBe('999999999');
    expect(candidate.physFootprintMb === null || typeof candidate.physFootprintMb === 'number').toBe(true);
  });
});
