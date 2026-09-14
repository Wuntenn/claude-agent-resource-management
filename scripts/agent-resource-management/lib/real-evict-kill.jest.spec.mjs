// Phase 4 security-review follow-up — unit coverage for
// `realEvictKill`/`realEvictIsAlive` (./real-evict-adapters.mjs), the real,
// non-test-seamed `process.kill` adapters `gracefulStop` is wired to by
// default (see cli.mjs's `buildEvictKillFn`/`buildEvictIsAliveFn`, which
// only redirect to a fake when `ARM_FAKE_KILL_LOG`/
// `ARM_FAKE_IS_ALIVE_SEQUENCE` are set).
//
// Every other eviction test deliberately routes AROUND these two functions
// via those env-var seams, precisely so no test suite ever risks a real
// `kill(2)` syscall against a host PID. That left the ESRCH/EPERM/other ->
// outcome-literal mapping itself — the one piece of logic this phase
// introduces — with no regression protection: a future edit that swapped
// the ESRCH/EPERM branches, or checked `error.errno` instead of
// `error.code`, would go undetected.
//
// This file closes that gap the same way: `jest.spyOn(process, 'kill')` is
// mocked to throw SYNTHETIC error objects shaped like real Node `SystemError`
// instances (`{ code: 'ESRCH', ... }` etc.) — never a real syscall against a
// real PID.

import { jest } from '@jest/globals';

import { realEvictKill, realEvictIsAlive } from './real-evict-adapters.mjs';

/**
 * Build a synthetic error shaped like Node's real `process.kill` throw —
 * carries `.code`, matching what `realEvictKill`/`realEvictIsAlive` branch
 * on (never `.errno`, which is a distinct, easy-to-confuse-with-code field
 * on the same real Node `SystemError` objects).
 */
function syntheticKillError(code) {
  const error = new Error(`kill ${code}`);
  error.code = code;
  return error;
}

describe('realEvictKill', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('returns "ok" when process.kill succeeds', () => {
    jest.spyOn(process, 'kill').mockImplementation(() => true);

    expect(realEvictKill(4242, 'SIGTERM')).toBe('ok');
    expect(process.kill).toHaveBeenCalledWith(4242, 'SIGTERM');
  });

  it('maps ESRCH ("no such process") to "already-gone"', () => {
    jest.spyOn(process, 'kill').mockImplementation(() => {
      throw syntheticKillError('ESRCH');
    });

    expect(realEvictKill(4242, 'SIGTERM')).toBe('already-gone');
  });

  it('maps EPERM ("operation not permitted") to "permission-denied"', () => {
    jest.spyOn(process, 'kill').mockImplementation(() => {
      throw syntheticKillError('EPERM');
    });

    expect(realEvictKill(4242, 'SIGKILL')).toBe('permission-denied');
  });

  it('rethrows any other error code rather than silently mapping it', () => {
    const unrecognized = syntheticKillError('EINVAL');
    jest.spyOn(process, 'kill').mockImplementation(() => {
      throw unrecognized;
    });

    expect(() => realEvictKill(4242, 'SIGTERM')).toThrow(unrecognized);
  });

  it('does not swap the ESRCH/EPERM branches', () => {
    jest.spyOn(process, 'kill').mockImplementationOnce(() => {
      throw syntheticKillError('ESRCH');
    });
    expect(realEvictKill(1, 'SIGTERM')).not.toBe('permission-denied');

    jest.spyOn(process, 'kill').mockImplementationOnce(() => {
      throw syntheticKillError('EPERM');
    });
    expect(realEvictKill(1, 'SIGTERM')).not.toBe('already-gone');
  });
});

describe('realEvictIsAlive', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('returns true when signal-0 probe succeeds (process exists)', () => {
    jest.spyOn(process, 'kill').mockImplementation(() => true);

    expect(realEvictIsAlive(4242)).toBe(true);
    expect(process.kill).toHaveBeenCalledWith(4242, 0);
  });

  it('maps ESRCH to false ("no such process" — genuinely gone)', () => {
    jest.spyOn(process, 'kill').mockImplementation(() => {
      throw syntheticKillError('ESRCH');
    });

    expect(realEvictIsAlive(4242)).toBe(false);
  });

  it('maps EPERM to true (process exists, just unsignalable by us)', () => {
    jest.spyOn(process, 'kill').mockImplementation(() => {
      throw syntheticKillError('EPERM');
    });

    expect(realEvictIsAlive(4242)).toBe(true);
  });

  it('treats any other unrecognized error code as "still alive" (fail closed)', () => {
    jest.spyOn(process, 'kill').mockImplementation(() => {
      throw syntheticKillError('EINVAL');
    });

    expect(realEvictIsAlive(4242)).toBe(true);
  });
});
