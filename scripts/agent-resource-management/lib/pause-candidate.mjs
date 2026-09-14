// whole-branch review, High #2 — wires Phase 1's phys_footprint
// victim-selection ranking (`selectPauseCandidate`, ./allowance.mjs) into
// cli.mjs's real production call sites.
//
// Before this fix, nothing under `scripts/` ever populated a `physFootprintMb`
// field on a real candidate: `listAgentProcesses` (./recon.mjs) only emits
// `{ agentId, rssMb, startedAt }`, so every real call site of
// `selectPauseCandidate` in cli.mjs always fell back to RSS ranking in
// practice — Phase 1's AC #5 ("victim selection ranks by phys_footprint, not
// RSS") was only true in hand-built unit tests that construct candidate
// objects with a `physFootprintMb` field directly, never in the real running
// system.
//
// Pure orchestration over an injected `sampleFootprintFn` — mirrors
// `graceful-stop.mjs`'s "pure decision + injected side effects" posture and
// this file's own `sampleFootprint`'s already-established contract (never
// throws, resolves `null` on any failure). This module doesn't decide HOW to
// sample a footprint, only how to fold whatever `sampleFootprintFn` resolves
// into the shape `selectPauseCandidate` already knows how to rank.
//
// Extracted into its own lib module (rather than living inline in cli.mjs)
// specifically so it can be unit-tested directly with an injected fake
// sampler — cli.mjs itself is black-box-only (it unconditionally runs
// `main()` at module load, and is excluded from coverage collection; see
// ./probe-bound.mjs's header for the same rationale applied to
// `collectPsOutput()`/`probeExecOptions()`).
//
// review, Medium — `honorFakeSeam` threads through to `sampleFootprint`
// (./footprint-sampler.mjs) exactly the way `cli.mjs`'s `buildEvictKillFn`/
// `buildEvictIsAliveFn`/`collectPsOutput` already thread `testModeEnabled`/
// `honorFakeSeam` into their own real-vs-fake adapter choice, so
// `ARM_FAKE_FOOTPRINT_OUTPUT` is gated behind `handleEvict`'s
// `ARM_EVICT_TEST_MODE` master switch like every other eviction-reachable
// seam, rather than being a standing exception to it.

import { selectPauseCandidate } from './allowance.mjs';
import { sampleFootprint } from './footprint-sampler.mjs';

/**
 * Samples each candidate's real physical footprint and folds the result into
 * `physFootprintMb` before ranking via `selectPauseCandidate` — the
 * end-to-end wiring `selectPauseCandidate` alone cannot provide on its own,
 * since it only ever ranks whatever shape it is handed.
 *
 * Per-candidate sampling failures are non-fatal and never crash this beat: a
 * candidate whose `sampleFootprintFn` call throws, or resolves `null`, is
 * left with `physFootprintMb: null` — exactly the shape `selectPauseCandidate`
 * already treats as "no footprint data for this candidate". If EVERY
 * candidate ends up without valid footprint data, `selectPauseCandidate`
 * itself already falls back to ranking by RSS, so a total sampling failure
 * degrades gracefully to the pre-existing behavior rather than crashing the
 * beat.
 *
 * @param {Array<{ agentId: string, rssMb: number, startedAt: number }>} candidates
 * @param {object} [options]
 * @param {(agentId: string, honorFakeSeam: boolean) => Promise<number | null>} [options.sampleFootprintFn]
 *   Defaults to the real `sampleFootprint` (./footprint-sampler.mjs).
 *   Injectable so this function is unit-testable without shelling out to the
 *   real `footprint`/`vmmap` binaries.
 * @param {boolean} [options.honorFakeSeam]
 *   Forwarded as `sampleFootprintFn`'s second argument on every call (default
 *   `true`) — mirrors `sampleFootprint`'s own `honorFakeSeam` parameter
 *   (./footprint-sampler.mjs) and `cli.mjs`'s `collectPsOutput(honorFakeSeam)`
 *   precedent. `handleEvict`'s fallback path (cli.mjs's `buildFallback`)
 *   passes its own `evictTestModeEnabled` here so `ARM_FAKE_FOOTPRINT_OUTPUT`
 *   is inert unless `ARM_EVICT_TEST_MODE=1` is also set — review,
 *   Medium: this was previously the one eviction-reachable seam NOT gated
 *   behind that master switch. A caller supplying its own `sampleFootprintFn`
 *   override is responsible for honoring (or ignoring) this flag itself; the
 *   default only affects the real `sampleFootprint`.
 * @returns {Promise<{ agentId: string, rssMb: number, startedAt: number, physFootprintMb: number | null } | null>}
 */
export async function selectPauseCandidateWithFootprint(
  candidates,
  { sampleFootprintFn = sampleFootprint, honorFakeSeam = true } = {},
) {
  const withFootprint = await Promise.all(
    candidates.map(async (candidate) => {
      let physFootprintMb = null;
      try {
        physFootprintMb = await sampleFootprintFn(candidate.agentId, honorFakeSeam);
      } catch {
        physFootprintMb = null;
      }
      return { ...candidate, physFootprintMb };
    }),
  );

  // `policy` is caller-documentation only (see `selectPauseCandidate`'s own
  // JSDoc, ./allowance.mjs — review, Low #2): it has no effect on
  // ranking, which auto-detects via `physFootprintMb` presence. Pass the
  // literal that actually matches what this call site is doing — every
  // candidate here has already been through the real footprint-sampling
  // wiring above — rather than the mismatched `'highest-rss'` this used to
  // pass, which only "worked" because the parameter was ignored.
  return selectPauseCandidate(withFootprint, { policy: 'highest-phys-footprint' });
}
