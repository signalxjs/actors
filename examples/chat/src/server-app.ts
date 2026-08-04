/**
 * The server app — the ONE place policy lives (rfc-server-v4 §3).
 *
 * This file is cut 1 and cut 2 of the guard split. Before v4 this example
 * had a single `requireUser` guard doing three jobs at once — verify a
 * cookie (authentication), throw 401 (authorization), and stash the result
 * for downstream hops (propagation) — repeated as `use: [requireUser]` on
 * every actor and every serverFn that needed it. The three jobs now live
 * where each belongs:
 *
 *  1. **the verify becomes `authenticate`** — it was already the right
 *     shape: return the principal, or `null` for a bad/absent cookie. It
 *     never throws for a bad cookie; a broken session store would be an
 *     infrastructure error, and that distinction is the whole point;
 *  2. **the 401 becomes the default policy** — which is core's built-in
 *     `requireAuthenticated`, so there is nothing to write here. Every
 *     actor and serverFn inherits it, and the two deliberately public
 *     surfaces say `allowAnonymous: true` (grep for it — that IS the
 *     example's whole anonymous-reachable surface);
 *  3. **the propagation is deleted** — `stampCallBag({ user })` is gone.
 *     Identity rides its own envelope slot now, so `ctx.principal` works
 *     inside every actor, across `ctx.actor` hops and between hosts, with
 *     nobody remembering to stamp anything. The bag stays for app DATA.
 *
 * The `codec` is what makes cut 3 work across hops: a principal has to
 * round-trip as a string to ride the envelope. Ours is a user name, so the
 * codec is nearly the identity function — but it is still the app's, not
 * the runtime's, because only the app knows what a principal IS. Omit it
 * and `@sigx/actors` propagates nothing and dev-warns once, which is
 * fail-closed at the reader: a hop reads `null`, never a wrong identity.
 */
import { createServerApp } from '@sigx/server/server';
import { currentUser } from './guards';

/** This app's principal. A real app would carry roles, an org id, … */
export interface ChatUser {
    readonly name: string;
}

export const app = createServerApp<ChatUser>({
    // Cut 1. Memoized once per request store by the pipeline, so one SSR
    // render of a page with five cells verifies the cookie ONCE — the
    // repeated-work problem that motivated the per-request store.
    authenticate: (rq) => {
        const name = currentUser(rq);
        return name === null ? null : { name };
    },
    // Cut 3's enabler. Not integrity-protected — it rides outside the
    // cluster HMAC exactly like the context bag, so its trust is the
    // deployment perimeter (mTLS/VPC between hosts), never a proof.
    codec: {
        encode: (user) => user.name,
        decode: (encoded) => (encoded === '' ? null : { name: encoded })
    }
});
