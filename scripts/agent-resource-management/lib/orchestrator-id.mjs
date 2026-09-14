// Phase 2 — the deterministic, session-derived orchestrator-id.
//
// design decision 2: the PreToolUse hook derives its coordination-file
// lookup key INDEPENDENTLY of the orchestrator that wrote it. Unless both
// sides derive the SAME key from the SAME Claude Code session id, the lookup
// never matches and the gate silently never activates for anyone — the same
// failure shape as the `worktreeMtimeMs`-always-null gap. This module is
// the single derivation both sides call verbatim; nothing else may invent a
// key.
//
// Derivation = readable prefix + cryptographic digest of the RAW session id:
//
//   session-<sanitized-prefix>-<base64url(sha256(sessionId))[0..31]>
//
// The two halves do different jobs and neither is optional:
//
//   * the sanitized prefix is for HUMANS — a `--orchestrator-id=` value, a
//     coordination-file row, or a fail-closed terminal message stays greppable
//     back to the session it came from. It is lossy by construction and is
//     NEVER load-bearing for identity.
//   * the sha256 digest carries the identity. A lossy sanitize alone (the
//     superseded draft's `.replace(/[^a-zA-Z0-9_-]/g, '')`) maps
//     `sess:2026-09-05T10:00:00.000Z` and `sess2026-09-05T100000000Z`, and a
//     colon-separated uuid and its bare-hex twin, onto ONE derived id. That is
//     not low-probability noise: it leaks one session's sticky opted-in state
//     — and its fail-closed denials — onto an unrelated session.
//
// `base64url` is chosen because its alphabet is exactly `[A-Za-z0-9_-]`, so
// the whole id is simultaneously safe as a JSON string value (no escaping), a
// shell argv token, and a single path component — which cli.mjs:1857 records
// as not existing in this skill until now.
//
// PURE: no clock, no randomness, no per-process salt, no I/O. The hook's child
// process, the orchestrator's `cli.mjs` beat, and the next host boot must all
// agree.
//
// A malformed session id THROWS rather than falling back to a shared
// placeholder: a shared fallback would let every malformed-session caller
// inherit one another's gate state, which is a cross-session correctness bug.

import { createHash } from 'node:crypto';

/** Mandated by design decision 2; SKILL.md's examples must match this. */
const ID_PREFIX = 'session-';

/** Characters kept verbatim in the human-readable prefix. */
const UNSAFE_CHARACTERS = /[^A-Za-z0-9_-]/g;

/** Readable-prefix budget — enough to recognise, short enough to stay bounded. */
const READABLE_PREFIX_MAX_LENGTH = 24;

/** Digest budget in base64url characters (32 chars = 192 bits of sha256). */
const DIGEST_LENGTH = 32;

/**
 * The human-readable, deliberately lossy half of the derived id.
 *
 * @param {string} sessionId
 * @returns {string} possibly empty; never contains anything outside
 *   `[A-Za-z0-9_-]`.
 */
function readablePrefixOf(sessionId) {
  return sessionId.replace(UNSAFE_CHARACTERS, '').slice(0, READABLE_PREFIX_MAX_LENGTH);
}

/**
 * The identity-bearing half: a truncated base64url sha256 of the RAW session
 * id — pre-sanitization, so no two distinct session ids share a digest.
 *
 * @param {string} sessionId
 * @returns {string} exactly `DIGEST_LENGTH` `[A-Za-z0-9_-]` characters.
 */
function digestOf(sessionId) {
  return createHash('sha256').update(sessionId, 'utf8').digest('base64url').slice(0, DIGEST_LENGTH);
}

/**
 * Derive the deterministic orchestrator-id for a Claude Code session id.
 *
 * The returned id is `session-`-prefixed, drawn only from `[A-Za-z0-9_-]`,
 * length-bounded regardless of input length, never `isReservedEntry`-shaped
 * (it starts with `session-`, so it can never both start and end with `__`),
 * and injective over distinct session ids up to a 192-bit sha256 truncation.
 *
 * @param {string} sessionId a non-empty, non-blank Claude Code session id.
 * @returns {string} the orchestrator-id to pass as `--orchestrator-id=` and to
 *   look up in the coordination file's sticky opted-in ledger.
 * @throws {TypeError} if `sessionId` is not a non-empty, non-blank string, or
 *   if it carries leading/trailing whitespace. Deliberately loud and
 *   per-caller: there is no shared fallback id to inherit another session's
 *   gate state from.
 */
export function deriveOrchestratorId(sessionId) {
  if (typeof sessionId !== 'string' || sessionId.trim() === '') {
    throw new TypeError(
      `deriveOrchestratorId: sessionId must be a non-empty, non-blank string, received ${typeof sessionId}. ` +
        'There is no fallback id by design — a shared placeholder would let unrelated sessions inherit ' +
        "one another's sticky opt-in and fail-closed gate state (design decision 2).",
    );
  }

  // Whitespace-padded input is REJECTED, not normalized. The digest is taken
  // over the raw id, so `'x'`, `' x '` and `'x\n'` would otherwise derive three
  // DIFFERENT ids — and one side reading `session_id` from hook-event JSON
  // while another captures it via shell/file output with incidental padding
  // would then key different ledger rows and the gate would silently never
  // match. That is the failure shape this module exists to prevent.
  // Trimming and proceeding would be the mirror-image risk: it would silently
  // fold a caller that means the padded literal onto the unpadded session.
  // Reject loudly instead, per-caller, with no shared fallback.
  if (sessionId !== sessionId.trim()) {
    throw new TypeError(
      'deriveOrchestratorId: sessionId must not have leading or trailing whitespace, received ' +
        `${JSON.stringify(sessionId)}. It is rejected rather than trimmed by design — silently ` +
        'normalizing would make a padded and an unpadded capture of the same session indistinguishable, ' +
        'and silently accepting it would make them derive different lookup keys, so the coordination-file ' +
        'lookup would never match (design decision 2).',
    );
  }

  return `${ID_PREFIX}${readablePrefixOf(sessionId)}-${digestOf(sessionId)}`;
}
