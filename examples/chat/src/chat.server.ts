/**
 * serverFns living beside actors on the same server — and calling into
 * them.
 *
 * `postMessage` is the trust boundary for an attributed write: `requireUser`
 * verifies the session and stashes it in `rq.locals.user`, and the handler
 * hands the actor a `from` the browser never supplied. An actor guard cannot
 * do this itself, because guards receive `(rq, { symbol, name })` — no key
 * and no arguments.
 *
 * The `actor()` call here dispatches IN-PROCESS through the host seam: same
 * expression as the browser's, no HTTP hop, and the actor's own
 * `requireUser` chain still runs against this request.
 */
import { serverFn, ServerFnError } from '@sigx/server';
import { actor } from '@sigx/actors';
import { RoomActor } from './room.actor';
import { cookieFor, currentUser, requireUser } from './guards';

/**
 * Who am I? Drives the header; also proves serverFns work beside actors.
 * `unguarded` — it REPORTS the session (null when absent), so gating it on
 * one would be circular.
 */
export const me = serverFn({
    unguarded: true,
    handler: async (rq): Promise<string | null> => currentUser(rq),
});

/**
 * Mint a session: an HMAC-signed, HttpOnly cookie. HttpOnly is the point —
 * the browser can render the session but never read or forge it, and the
 * signature means nobody else can either.
 */
export const signIn = serverFn({
    unguarded: true, // deliberate: this IS the sign-in
    handler: async (rq, input: { name: string }): Promise<string> => {
        const name = input.name.trim();
        if (!/^[\w-]{1,32}$/.test(name)) {
            throw new ServerFnError(400, 'name must be 1-32 letters, digits, _ or -');
        }
        const secure = new URL(rq.request.url).protocol === 'https:' ? '; Secure' : '';
        rq.responseHeaders.append(
            'set-cookie',
            `user=${cookieFor(name)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=604800${secure}`
        );
        return name;
    },
});

export const signOut = serverFn({
    unguarded: true,
    handler: async (rq): Promise<null> => {
        rq.responseHeaders.append('set-cookie', 'user=; Path=/; HttpOnly; Max-Age=0');
        return null;
    },
});

export const postMessage = serverFn({
    use: [requireUser],
    handler: async (rq, input: { room: string; text: string }): Promise<number> => {
        const from = rq.locals.user as string;
        const text = input.text.trim();
        if (!text) throw new ServerFnError(400, 'message is empty');
        return actor(RoomActor, input.room).post(from, text);
    },
});
