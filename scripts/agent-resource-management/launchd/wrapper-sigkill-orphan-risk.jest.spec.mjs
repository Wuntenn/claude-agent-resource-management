// outer acceptance test.
//
// Pins the acceptance criterion that `launchd/README.md` must document, behind
// a dedicated doc-honesty anchor, a THIRD distinct limitation of this
// backstop: the wrapper's "bounds every beat" claim (the paragraph starting
// "The wrapper bounds every beat and force-kills a hung one.") only holds
// while the wrapper process (`watchdog-beat-launchd.mjs`) itself survives. If
// something SIGKILLs the wrapper, the grandchild it spawned
// (`cli.mjs --watchdog-beat`) is left unbounded — it keeps running with
// nothing left to enforce the 2-minute timeout or forward SIGTERM/SIGINT to
// it — and it can end up running concurrently with a fresh instance `launchd`
// starts on the next `StartInterval` fire, since `launchd` only tracks the
// wrapper's own pid, not its child's.
//
// The accepted-risk decision: the orphaned grandchild cannot escalate that
// concurrency into kill authority, because `isEvictionAuthorized`
// (`lib/recon.mjs:458-496`) only authorizes eviction of `ps`-tree descendants
// of the *calling* orchestrator process — an orphaned `cli.mjs --watchdog-beat`
// has no such descendants of its own to evict, same structural reason already
// documented for the launchd-spawned-beat case. This must NOT be read as
// "the orphan risk is eliminated" — only that it cannot gain kill authority.
//
// This is a third, distinct limitation from the two already documented above
// it in this same README section: the per-agent EVICT-authorization gap
// and the leak-velocity DETECTION gap (the
// `leak-velocity-launchd-detection-gap` anchor). The new section must
// disambiguate itself from at least one of those, mirroring how the
// leak-velocity section disambiguates itself from the EVICT-authorization gap.
//
// This is a documentation-honesty test, not a runtime-behavior test — a
// grep/regex assertion against real doc prose is exactly the tool for the job
// (mirrors this directory's `watchdog-detection-gap.jest.spec.mjs` convention:
// `readFileSync` + anchor-scoped substring/regex extraction, not a bare
// file-wide substring match, to avoid the vacuous-grep failure mode where a
// stray pre-existing mention elsewhere in the file could satisfy a
// loosely-scoped assertion (see [[feedback_absent_key_vuln_tests]]).
//
// RED today: the README has no `sigkill-wrapper-orphan-risk` anchor yet.
// Expected to turn GREEN once the README doc fix lands. Do not add a
// stub/rewording to make this pass here — the real production doc fix turns
// it green.
//
// Anchor string the implementer MUST use verbatim, placed in
// scripts/agent-resource-management/launchd/README.md:
//   <!-- doc-honesty-anchor: sigkill-wrapper-orphan-risk -->

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));

const READY_ANCHOR = '<!-- doc-honesty-anchor: sigkill-wrapper-orphan-risk -->';

const readmePath = join(__dirname, 'README.md');
const readmeSource = readFileSync(readmePath, 'utf8');

/**
 * Extracts the anchor-scoped section from a doc-honesty anchor comment to the
 * next blank line followed by a heading (`#`), or to the next blank line
 * followed by another anchor comment, or to end-of-file — whichever comes
 * first. Mirrors `watchdog-detection-gap.jest.spec.mjs`'s established
 * convention of addressing sections by an explicit anchor rather than by
 * brittle substring position, so a stray earlier/later mention of a term like
 * "isEvictionAuthorized" elsewhere in the file cannot vacuously satisfy an
 * assertion scoped to this section.
 */
function extractAnchorSection(source, anchor) {
  const anchorIndex = source.indexOf(anchor);
  if (anchorIndex === -1) {
    return null;
  }
  const afterAnchor = source.slice(anchorIndex + anchor.length);
  const nextHeadingMatch = afterAnchor.match(/\n#{1,6}\s/);
  const nextAnchorMatch = afterAnchor.match(/<!-- doc-honesty-anchor:/);
  const candidates = [nextHeadingMatch, nextAnchorMatch]
    .filter((m) => m !== null)
    .map((m) => m.index);
  const cutoff = candidates.length > 0 ? Math.min(...candidates) : afterAnchor.length;
  return afterAnchor.slice(0, cutoff);
}

describe('outer acceptance: README documents the SIGKILL-of-wrapper orphan risk as a third, distinct limitation', () => {
  it('the dedicated doc-honesty anchor exists in launchd/README.md', () => {
    // Non-vacuity: a missing anchor must fail loudly here rather than hand
    // the assertions below a null/empty window.
    expect(readmeSource).toContain(READY_ANCHOR);
  });

  it('the anchor section states the "bounds every beat" claim only holds while the wrapper process itself survives', () => {
    const section = extractAnchorSection(readmeSource, READY_ANCHOR);
    expect(section).not.toBeNull();

    expect(section).toMatch(/bounds every beat/i);
    expect(section).toMatch(/only holds|only while|as long as|while the wrapper/i);
    expect(section).toMatch(/wrapper (process )?(itself )?survives|wrapper.*(is alive|stays alive|remains running)/i);
  });

  it('the anchor section names the specific consequence: SIGKILL of the wrapper leaves the grandchild unbounded and running concurrently with a fresh launchd-started instance', () => {
    const section = extractAnchorSection(readmeSource, READY_ANCHOR);
    expect(section).not.toBeNull();

    expect(section).toMatch(/SIGKILL/);
    expect(section).toMatch(/cli\.mjs --watchdog-beat|grandchild/i);
    expect(section).toMatch(/unbounded/i);
    expect(section).toMatch(/concurrent(ly)?/i);
    expect(section).toMatch(/launchd/i);
  });

  it('the anchor section records the accept-the-risk decision, naming isEvictionAuthorized and its ps-tree-descendant requirement', () => {
    const section = extractAnchorSection(readmeSource, READY_ANCHOR);
    expect(section).not.toBeNull();

    expect(section).toMatch(/isEvictionAuthorized/);
    expect(section).toMatch(/ps[- ]tree/i);
    expect(section).toMatch(/descendant/i);
    expect(section).toMatch(/accept(ed|s)?[- ]the[- ]risk|accepted risk|deliberate|accepted limitation/i);
  });

  it('the anchor section does NOT claim the orphan risk is eliminated — only that escalation to kill authority is prevented', () => {
    const section = extractAnchorSection(readmeSource, READY_ANCHOR);
    expect(section).not.toBeNull();

    expect(section).toMatch(/kill authority/i);
    expect(section).toMatch(/cannot|can't|prevents?|no path to/i);

    // The section must not assert the risk itself is gone.
    expect(section).not.toMatch(/risk is eliminated|eliminates the (orphan )?risk|no (longer|orphan) risk/i);
  });

  it('the anchor section cross-references at least one of the two prior limitations in this section as an explicit disambiguation', () => {
    const section = extractAnchorSection(readmeSource, READY_ANCHOR);
    expect(section).not.toBeNull();

    const mentionsLeakVelocityGap =
      /leak-velocity/i.test(section) ||
      /leak-velocity-launchd-detection-gap/.test(section);
    const mentionsEvictionGap = /EVICT-authorization/i.test(section);
    expect(mentionsLeakVelocityGap || mentionsEvictionGap).toBe(true);

    // It must read as an explicit disambiguation, not just a bare mention.
    expect(section).toMatch(
      /distinct from|different (from|decision|gap|limitation)|separate (from|limitation|gap)|not to be confused/i,
    );
  });

  it('regression pin: the existing "wrapper bounds every beat" paragraph is still present and unduplicated by the new anchor section', () => {
    // The new prose must extend/follow the existing wrapper paragraph, not
    // replace or duplicate its heading text.
    expect(readmeSource).toMatch(
      /\*\*The wrapper bounds every beat and force-kills a hung one\.\*\*/,
    );

    const occurrenceCount = (
      readmeSource.match(/\*\*The wrapper bounds every beat and force-kills a hung one\.\*\*/g) ?? []
    ).length;
    expect(occurrenceCount).toBe(1);
  });
});
