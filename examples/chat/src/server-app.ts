/**
 * The server app — the ONE place policy lives, and the lesson this example
 * is built around.
 *
 * Three jobs that look like one, kept apart:
 *
 *  1. **`authenticate`** answers *who is this?* It returns the principal, or
 *     `null` for an absent or bad cookie. It does not throw for a bad
 *     cookie — a broken session store would be an infrastructure error, and
 *     conflating the two is how a 401 becomes a 500.
 *  2. **the default policy** answers *may they?* Here it is core's built-in
 *     `requireAuthenticated`, so there is nothing to write. Every actor and
 *     serverFn inherits it, and the deliberately public surfaces opt out
 *     with `allowAnonymous: true` — grep for it, and you have this example's
 *     entire anonymous-reachable surface.
 *  3. **propagation** is not your problem. Identity rides its own envelope
 *     slot, so `ctx.principal` works inside every actor, across every
 *     `ctx.actor` and `ctx.publish` hop, and between hosts, with nobody
 *     remembering to stamp anything.
 *
 * The `codec` is what makes (3) work: a principal has to round-trip as a
 * string to ride the envelope. Ours is a name, so it is nearly the identity
 * function — but it is still the app's, because only the app knows what a
 * principal IS. Omit it and nothing propagates: a hop reads `null`, never a
 * wrong identity.
 */
import { createServerApp } from '@sigx/server/server';
import { currentUser } from './session';

/** This app's principal. A real one would carry roles, an org id, … */
export interface ChatUser {
    readonly name: string;
}

export const app = createServerApp<ChatUser>({
    // Memoized once per request store, so one SSR render of a page with
    // several actor reads verifies the cookie ONCE.
    authenticate: (rq) => {
        const name = currentUser(rq);
        return name === null ? null : { name };
    },
    codec: {
        encode: (user) => user.name,
        decode: (encoded) => (encoded === '' ? null : { name: encoded })
    }
});
