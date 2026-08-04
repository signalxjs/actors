/**
 * Session verification — what makes this example a real test rather than a
 * demo, and, since rfc-server-v4, ONLY that.
 *
 * `currentUser` is now the body of the app's `authenticate` (see
 * `server-app.ts`). It used to be one third of a `requireUser` guard that
 * also threw the 401 and stamped the call bag; the 401 is the app's default
 * policy and the propagation is automatic, so both of those are gone and
 * this file does one job.
 *
 * It reads `rq.request` — which works over the wire but THROWS on a
 * detached context. During SSR the context comes from the request scope the
 * document handler opens, and that scope only exists because `@sigx/server`
 * was imported (it stamps `__SIGX_SERVERFN_SCOPE__` on import). If the
 * server entry ever stops importing it, authentication is what fails.
 *
 * Sessions are REAL here: the cookie is `user=<name>.<hmac>`, minted by the
 * `signIn` serverFn with a server secret and verified with a timing-safe
 * compare. A missing, malformed, or forged signature is NO session — this
 * returns `null`, never throws. That distinction is load-bearing under v4:
 * `null` is anonymous (a valid outcome the pre-v4 guard model could not
 * express), while a throw would be an infrastructure failure and a masked
 * 500. (The cookie is also HttpOnly, so the old
 * `document.cookie = "user=ada"` party trick is over.)
 */
import { createHmac, timingSafeEqual } from 'node:crypto';
import type { ServerFnContext } from '@sigx/server';

// The chart injects a generated secret; the dev fallback keeps the local
// no-env experience working (and is worthless outside your machine). A
// production build without a real secret would make every session
// forgeable — that must be a boot failure, not a silent default.
if (!process.env.AUTH_SECRET && process.env.NODE_ENV === 'production') {
    throw new Error('[chat] AUTH_SECRET is required in production — sessions are signed with it');
}
const SECRET = process.env.AUTH_SECRET ?? 'chat-dev-secret';

const sign = (name: string): string =>
    createHmac('sha256', SECRET).update(name).digest('hex');

/** The signed cookie VALUE for a name — used by `signIn` when minting. */
export const cookieFor = (name: string): string =>
    encodeURIComponent(`${name}.${sign(name)}`);

/** Verify the session cookie; anything short of a valid signature is null. */
export function currentUser(rq: ServerFnContext): string | null {
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
    // Length differs on any truncated/padded forgery; timingSafeEqual
    // throws on unequal lengths, so check first.
    if (got.length !== want.length || !timingSafeEqual(got, want)) return null;
    return name;
}

// `requireUser` is deliberately gone. It was the conflated guard
// rfc-server-v4 exists to split, and each of its three jobs now has a home:
//
//   the verify      → `authenticate` in `server-app.ts`  (this file's body)
//   the 401         → the app's default policy, i.e. core's built-in
//                     `requireAuthenticated` — nothing to write, and the
//                     two public surfaces say `allowAnonymous: true`
//   the propagation → automatic: identity rides its own envelope slot, so
//                     `ctx.principal` works in every actor and across every
//                     hop with nobody remembering to stamp it
//
// The `stampCallBag` call went with it. That channel still exists and is
// still the right one for app DATA — it is just no longer how identity
// travels, which is what stops a forgotten stamp from silently dropping the
// caller somewhere down a call chain.
