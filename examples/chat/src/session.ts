/**
 * Session verification — and only that.
 *
 * `currentUser` is the body of the app's `authenticate` (see
 * `server-app.ts`). It reads `rq.request`, which works over the wire but
 * THROWS on a detached context. During SSR the context comes from the
 * request scope the document handler opens, and that scope exists only
 * because `@sigx/server` was imported — see the note in `entry-server.tsx`.
 *
 * The session is real: the cookie is `user=<name>.<hmac>`, minted by the
 * `signIn` serverFn with a server secret and verified with a timing-safe
 * compare. A missing, malformed or forged signature is NO session — this
 * returns `null`, never throws. That distinction is load-bearing: `null` is
 * anonymous, a valid outcome, while a throw would be an infrastructure
 * failure and a masked 500.
 */
import { createHmac, timingSafeEqual } from 'node:crypto';
import type { ServerFnContext } from '@sigx/server';

// A production build without a real secret would make every session
// forgeable. That must be a boot failure, not a silent default.
if (!process.env.AUTH_SECRET && process.env.NODE_ENV === 'production') {
    throw new Error('[chat] AUTH_SECRET is required in production — sessions are signed with it');
}
const SECRET = process.env.AUTH_SECRET ?? 'chat-dev-secret';

const sign = (name: string): string => createHmac('sha256', SECRET).update(name).digest('hex');

/** The signed cookie VALUE for a name — used by `signIn` when minting. */
export const cookieFor = (name: string): string => encodeURIComponent(`${name}.${sign(name)}`);

/** Verify the session cookie; anything short of a valid signature is null. */
export function currentUser(rq: ServerFnContext): string | null {
    const cookie = rq.request.headers.get('cookie') ?? '';
    const match = /(?:^|;\s*)user=([^;]+)/.exec(cookie);
    if (!match) return null;

    let value: string;
    try {
        value = decodeURIComponent(match[1]!);
    } catch {
        // URIError on half-encoded input. An unreadable session is NO
        // session; letting it escape turns a 401 into a masked 500.
        return null;
    }

    const dot = value.lastIndexOf('.');
    if (dot <= 0) return null;
    const name = value.slice(0, dot);
    const got = Buffer.from(value.slice(dot + 1), 'hex');
    const want = Buffer.from(sign(name), 'hex');
    // `timingSafeEqual` throws on unequal lengths, and any truncated or
    // padded forgery differs in length — so check that first.
    if (got.length !== want.length || !timingSafeEqual(got, want)) return null;
    return name;
}
