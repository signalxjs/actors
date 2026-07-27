/**
 * serverFns living beside actors on the same server — and calling into
 * them.
 *
 * `postMessage` is the trust boundary for an attributed write: it reads the
 * session off the request, then hands the actor a `from` the browser never
 * supplied. An actor guard cannot do this itself, because guards receive
 * `(rq, { symbol, name })` — no key and no arguments.
 *
 * The `actor()` call here dispatches IN-PROCESS through the silo seam: same
 * expression as the browser's, no HTTP hop, and the actor's own
 * `requireUser` chain still runs against this request.
 */
import { serverFn, ServerFnError } from '@sigx/server';
import { actor } from '@sigx/actors';
import { RoomActor } from './room.actor';
import { currentUser } from './guards';

/** Who am I? Drives the header; also proves serverFns work beside actors. */
export const me = serverFn(async (rq): Promise<string | null> => currentUser(rq));

export const postMessage = serverFn(
    async (rq, input: { room: string; text: string }): Promise<number> => {
        const from = currentUser(rq);
        if (!from) throw new ServerFnError(401, 'sign in to post');
        const text = input.text.trim();
        if (!text) throw new ServerFnError(400, 'message is empty');
        return actor(RoomActor, input.room).post(from, text);
    }
);
