/**
 * Suite-wide test identity (rfc-server-v4, 0.15 migration row 10).
 *
 * The runtime is FAIL-CLOSED since core 0.15: with no server app stamped in
 * the process, every operation that does not declare `allowAnonymous: true`
 * answers 401 before it dispatches. That is the correct production posture
 * and the whole point of the revision — but almost none of these suites are
 * about authentication. They test the wire envelope, cluster placement,
 * streaming, reminders, topics. Left unconfigured they would all fail 401
 * and say nothing about what they exist to check.
 *
 * So the suite does what a real app does: configure ONE app, once. Every
 * caller is a signed-in `test-principal`, which is the uninteresting case
 * these suites want as their background.
 *
 * Two rules for anything auth-shaped:
 *
 *  - a test about DENIAL stamps its own app (`stubServerApp` returns the
 *    restore, so do it in the test and restore in a `finally`), or declares
 *    a policy that refuses — the app default here only ever ADMITS, so it
 *    cannot mask a deny a test is asserting;
 *  - a test about ANONYMOUS access must stamp an app whose `authenticate`
 *    returns `null`, because this one always produces a principal.
 *
 * The `codec` is here so `ctx.principal` propagates across hops and
 * host-to-host in the cluster suites — without it identity stops at the
 * entry point and the propagation tests would pass vacuously.
 */
import { beforeAll } from 'vitest';
import { stubServerApp } from '@sigx/server/testing';

export interface TestPrincipal {
    readonly id: string;
}

export const TEST_PRINCIPAL: TestPrincipal = { id: 'test-principal' };

/**
 * Run `fn` with NO server app stamped, then restore.
 *
 * The one thing the suite-wide stamp makes untestable is the unconfigured
 * process itself — and that state has observable behaviour worth pinning:
 * the fail-closed deny, and the registration warning that explains it. This
 * removes the seam entry rather than stamping an empty config, because an
 * empty config IS configured and behaves differently.
 */
export async function withoutServerApp<T>(fn: () => T | Promise<T>): Promise<T> {
    const seam = globalThis as { __SIGX_SERVER_APP__?: unknown };
    // Restore PRESENCE, not just value: if the process had no app and `fn`
    // stamps one (any `stubServerApp` call inside it does), restoring only a
    // non-undefined previous would leave that stamp behind and silently
    // configure every later test in the file.
    const had = '__SIGX_SERVER_APP__' in seam;
    const previous = seam.__SIGX_SERVER_APP__;
    delete seam.__SIGX_SERVER_APP__;
    try {
        return await fn();
    } finally {
        if (had) seam.__SIGX_SERVER_APP__ = previous;
        else delete seam.__SIGX_SERVER_APP__;
    }
}

beforeAll(() => {
    // Deliberately NOT restored between files: the stamp is process-wide and
    // last-wins, and a test that replaces it restores its own.
    stubServerApp({
        authenticate: () => TEST_PRINCIPAL,
        codec: {
            encode: (principal) => (principal as TestPrincipal).id,
            decode: (encoded) => (encoded === '' ? null : { id: encoded })
        }
    });
});
