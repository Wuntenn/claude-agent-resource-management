// Phase 4 — real `process.kill`-based adapters for `gracefulStop`.
//
// Pulled out of cli.mjs (rather than defined + exported there) because
// cli.mjs unconditionally calls `main().catch(...)` at module load — any
// direct `import` of cli.mjs from a test runs the whole CLI beat as a side
// effect. Living here instead lets these two adapters be imported and unit
// tested directly, mirroring every other lib/ module's "pure function,
// normal named export" convention. cli.mjs imports both and threads them
// into `gracefulStop` unchanged via `buildEvictKillFn`/`buildEvictIsAliveFn`.

/**
 * Real `kill()` adapter for `gracefulStop` — never called when
 * `ARM_FAKE_KILL_LOG` is set (see cli.mjs's `buildEvictKillFn`). Maps the
 * two documented, non-fatal `process.kill` failure modes (`ESRCH`/`EPERM`)
 * onto `gracefulStop`'s own literal-result contract; any other thrown error
 * propagates uncaught rather than being silently reinterpreted.
 *
 * @param {number} pid
 * @param {'SIGTERM'|'SIGKILL'} signal
 * @returns {'ok'|'already-gone'|'permission-denied'}
 */
export function realEvictKill(pid, signal) {
  try {
    process.kill(pid, signal);
    return 'ok';
  } catch (error) {
    if (error.code === 'ESRCH') return 'already-gone';
    if (error.code === 'EPERM') return 'permission-denied';
    throw error;
  }
}

/**
 * Real `isAlive()` adapter for `gracefulStop` — signal 0 probes existence
 * without actually signalling the process. Never called when
 * `ARM_FAKE_IS_ALIVE_SEQUENCE` is set.
 *
 * round-4 review, Medium, accepted limitation (tracked separately) —
 * `kill(pid, 0)`
 * succeeds against a zombie (exited but not yet reaped by its real parent),
 * only failing once fully reaped. `gracefulStop` calls this immediately
 * after the grace period and again immediately after SIGKILL, with no delay
 * for reaping, so OS timing alone can cause an unneeded SIGKILL escalation
 * against an already-exited-but-unreaped process, or a successful SIGKILL
 * being reported `unconfirmed-after-kill` (which never releases the lease)
 * purely because reaping hadn't happened yet. See SKILL.md's "Accepted
 * limitation: the isAlive probe cannot tell a running process from an
 * unreaped zombie" for the full writeup and why this isn't fixed here.
 *
 * @param {number} pid
 * @returns {boolean}
 */
export function realEvictIsAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error.code === 'ESRCH') return false;
    // EPERM means the process exists but this process may not signal it —
    // that is still "alive", not "gone".
    return true;
  }
}
