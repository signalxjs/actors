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
            return ctx.state.messages.length;
        },
        /** Unattributed write — safe to call straight from the browser. */
        async setTopic(text: string): Promise<string> {
            ctx.state.topic = text;
            await ctx.save();
            return ctx.state.topic;
        }
    }),
    streams: (ctx) => ({
        /**
         * The push half. Without it a tab only ever sees its OWN writes:
         * `useActorAction` and `cells.invalidate()` refresh the cells in the
         * tab that called them, and nothing tells the others.
         *
         * `ctx.changes()` yields after every turn that mutated state — one
         * value per `post` or `setTopic`, from ANY tab, because they all
         * address the same single-threaded actor.
         *
         * `{ initial: true }` closes the gap this page cannot otherwise
         * see. SSR reads the room during the render; the browser only
         * subscribes once the client bundle has loaded and hydrated, and a
         * message posted in between belongs to neither — the page would sit
         * on it until the NEXT write happened to come along, which in a
         * quiet room is never. The initial value is that catch-up, and it
         * is queued synchronously with the subscription, so nothing can
         * slip through the middle of it either.
         *
         * The guard chain covers this exactly as it covers the methods —
         * `use: [requireUser]` runs before the first pull.
         */
        async *watch(): AsyncGenerator<{ topic: string; messages: Message[] }> {
            yield* ctx.changes({ initial: true });
        }
    })
});
