/**
 * A chat room — one actor per room name, single-threaded, persistent.
 *
 * `use: [requireUser]` runs on EVERY transport: the wire call from the
 * browser and the in-process dispatch during an SSR render. Proving those
 * two agree is why this example exists, because they reach the request very
 * differently.
 *
 * NOTE ON AUTHORIZATION. A guard receives `(rq, { symbol, name })` — not the
 * actor key and not the arguments. So "is this signed in?" is expressible
 * here, but "does this user own THIS room?" and "is `from` really the
 * caller?" are not. Attributed writes therefore go through a serverFn (see
 * `chat.server.ts`), which is the trust boundary that does know the session:
 * it verifies, then calls the actor with a value the browser cannot forge.
 * `setTopic` needs no attribution, so it is called directly.
 */
import { defineActor } from './actors.app';
import { roomActivity } from './activity.actor';
import { requireUser } from './guards';

export interface Message {
    readonly from: string;
    readonly text: string;
    readonly at: Date;
}

export const RoomActor = defineActor({
    type: 'Room',
    use: [requireUser],
    state: () => ({ topic: 'general chatter', messages: [] as Message[] }),
    methods: (ctx) => ({
        /** The read `useActorState` seeds from during SSR. */
        async recent(limit: number): Promise<Message[]> {
            return ctx.state.messages.slice(-limit);
        },
        async topic(): Promise<string> {
            return ctx.state.topic;
        },
        /**
         * Attributed write — only ever called from `chat.server.ts`, which
         * supplies a `from` it took from the session rather than the body.
         */
        async post(from: string, text: string): Promise<number> {
            ctx.state.messages.push({ from, text, at: new Date() });
            await ctx.save();
            // Announce to whoever declared interest (see activity.actor.ts).
            // Awaited: publish settles when every subscriber's turn has, and
            // a subscriber failure lands in the report, never here.
            await ctx.publish(roomActivity(ctx.key), { what: 'message' });
            return ctx.state.messages.length;
        },
        /** Unattributed write — safe to call straight from the browser. */
        async setTopic(text: string): Promise<string> {
            ctx.state.topic = text;
            await ctx.save();
            await ctx.publish(roomActivity(ctx.key), { what: 'topic' });
            return ctx.state.topic;
        }
    })
    // NO `streams:` here, deliberately.
    //
    // The push half of this example used to be a per-actor `watch()` stream
    // over `ctx.changes()`, with the page holding one NDJSON response per
    // room and a reconnect loop of its own. `useActorState(…, { live: true })`
    // replaced all of it: the runtime re-runs the READS this page actually
    // declares (`recent(20)`, `topic`) after every mutating turn and pushes
    // their results on one multiplexed connection for the whole page — so
    // there is nothing left for a hand-written stream to add here.
    //
    // `streams:` remains the right tool for a feed that is not a read of
    // current state — a log tail, a progress sequence, an event history. The
    // counter example shows that shape.
});
