/**
 * The server app — the rig's identity, and ONLY that (#172).
 *
 * This rig ran fully anonymous until the socket work needed a second arm:
 * cross-host live subscriptions coalesce on a key that INCLUDES the
 * principal (`cluster/placement.ts`), so "50k anonymous subscribers" and
 * "50k signed-in subscribers" are different measurements, and the second one
 * cannot be made without real principals. `#138` tracks sharing across
 * distinct principals; until it lands, this is the arm that shows what it
 * costs.
 *
 * Deliberately minimal. It configures `authenticate` and `codec` and
 * NOTHING else — no policy, no posture — so every existing scenario keeps
 * the behaviour it was baselined with: `authenticate` returns `null` for a
 * request with no cookie, that is anonymous, and every actor in this rig
 * says `allowAnonymous: true`. Adding a posture here would silently re-shape
 * the HTTP numbers in `BASELINES.md`.
 *
 * `ENABLE_SESSIONS` is the switch. Off (the default) authentication is a
 * constant `null` and the secret is never read, so an existing deployment
 * that sets no new env behaves exactly as before. On, the secret is
 * REQUIRED in production — a forgeable session would make the per-principal
 * arm measure nothing at all, which is worse than not running it.
 */
import { createHmac, timingSafeEqual } from 'node:crypto';
import { createServerApp } from '@sigx/server/server';
import type { ServerFnContext } from '@sigx/server';

/** This rig's principal. A name is all the arm needs. */
export interface RigUser {
    readonly name: string;
}

const ENABLED = process.env.ENABLE_SESSIONS === '1';

if (ENABLED && !process.env.SESSION_SECRET && process.env.NODE_ENV === 'production') {
    throw new Error(
        '[perf-aks] SESSION_SECRET is required when ENABLE_SESSIONS=1 — sessions are signed with it'
    );
}
const SECRET = process.env.SESSION_SECRET ?? 'perf-aks-dev-secret';

const sign = (name: string): string => createHmac('sha256', SECRET).update(name).digest('hex');

/** The signed cookie VALUE for a name — the load generators mint this. */
export const cookieFor = (name: string): string => encodeURIComponent(`${name}.${sign(name)}`);

/** Verify the session cookie; anything short of a valid signature is null. */
export function currentUser(rq: ServerFnContext): string | null {
    if (!ENABLED) return null;
    const cookie = rq.request.headers.get('cookie') ?? '';
    const match = /(?:^|;\s*)user=([^;]+)/.exec(cookie);
    if (!match) return null;
    let value: string;
    try {
        value = decodeURIComponent(match[1]!);
    } catch {
        // URIError on half-encoded input — an unreadable session is NO
        // session; letting it escape turns a 401 into a masked 500.
        return null;
    }
    const dot = value.lastIndexOf('.');
    if (dot <= 0) return null;
    const name = value.slice(0, dot);
    const got = Buffer.from(value.slice(dot + 1), 'hex');
    const want = Buffer.from(sign(name), 'hex');
    // timingSafeEqual throws on unequal lengths, so check length first.
    if (got.length !== want.length || !timingSafeEqual(got, want)) return null;
    return name;
}

/**
 * What lets `ctx.principal` ride the envelope between hosts — and therefore
 * what the cross-host coalescing key is built from.
 *
 * Exported because the suite-wide stamp in `vitest.setup.ts` replaces this
 * app with its own in a `beforeAll` (last-wins, by design), so a test about
 * THIS rig's identity has to stamp the rig's codec back. Sharing the value
 * is what stops the two from drifting into a test that passes against a
 * principal shape the deployment never produces.
 */
export const principalCodec = {
    encode: (user: RigUser) => user.name,
    decode: (encoded: string) => (encoded === '' ? null : { name: encoded })
};

export const serverApp = createServerApp<RigUser>({
    authenticate: (rq) => {
        const name = currentUser(rq);
        return name === null ? null : { name };
    },
    codec: principalCodec
});
