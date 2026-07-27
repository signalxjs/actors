/**
 * The guard that makes this example a real test rather than a demo.
 *
 * It reads `rq.request` — which works over the wire but THROWS on a
 * detached context. During SSR the context comes from the request scope the
 * document handler opens, and that scope only exists because `@sigx/server`
 * was imported (it stamps `__SIGX_SERVERFN_SCOPE__` on import). If the
 * server entry ever stops importing it, this guard is what fails.
 */
import { ServerFnError, type ServerFnContext } from '@sigx/server';

/** Toy session: a `user` cookie. Real apps verify a signed token here. */
export function currentUser(rq: ServerFnContext): string | null {
    const cookie = rq.request.headers.get('cookie') ?? '';
    const match = /(?:^|;\s*)user=([^;]+)/.exec(cookie);
    if (!match) return null;
    try {
        return decodeURIComponent(match[1]!);
    } catch {
        // `decodeURIComponent` throws a URIError on a half-encoded value
        // (`user=%E0%A4%A`). An unreadable session is NO session — letting
        // it escape turned an unauthenticated call into a masked 500
        // instead of the guard's 401.
        return null;
    }
}

/** Refuses the call with a 401 when there is no session. */
export function requireUser(rq: ServerFnContext): void {
    const user = currentUser(rq);
    if (!user) throw new ServerFnError(401, 'sign in to use the chat');
    // Guards run before the mailbox; `locals` carries the result onward.
    rq.locals.user = user;
}
