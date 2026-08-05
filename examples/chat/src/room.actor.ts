/**
 * A chat room — one actor per room name, single-threaded, persistent.
 *
 * **It declares no authorization at all, and that is the point.** The app's
 * default policy (`server-app.ts`) decides, on every transport: the wire
 * call from the browser and the in-process dispatch during an SSR render.
 * Proving those two agree is most of why this example exists, because they
 * reach the request very differently.
 *
 * If you need *"may they call it on THIS room?"* — the question a guard
 * cannot answer, because it never sees the key — a policy can, because it
 * receives the resolved principal and the instance:
 *
 * ```ts
 * authorize: (user, _rq, op) => op.resource!.key.startsWith(`${user.name}:`)
 * ```
 */
import { defineActor } from './actors.app';
import { roomActivity } from './activity.actor';

export interface Message {
    readonly from: string;
    readonly text: string;
    readonly at: Date;
}

/** The shape version carried IN the record. */
export const ROOM_STATE_VERSION = 2;

interface RoomState {
    /** Absent on every record written before this field existed. */
    v: number;
    topic: string;
    messages: Message[];
}

export const RoomActor = defineActor({
    type: 'Room',
    state: (): RoomState => ({
        v: ROOM_STATE_VERSION,
        topic: 'general chatter',
        messages: []
    }),
    /**
     * Evolve a stored room between the storage read and activation, so a
     * record written by an older build activates as the current shape.
     *
     * Returning `stored` unchanged is the fast path, and identity is how the
     * runtime detects it — so the migrating branch must build a NEW object.
     *
     * Lazy: the migrated shape rides the next save the room would have made
     * anyway, so a deploy adds no write amplification. The consequence is
     * that a room only ever READ never persists its migration, which is
     * fine — it migrates again, from the same stored bytes, next time.
     */
    migrateState: {
        persist: 'lazy',
        migrate: (stored): RoomState => {
            const record = stored as Partial<RoomState>;
            if (record.v === ROOM_STATE_VERSION) return record as RoomState;
            return {
                v: ROOM_STATE_VERSION,
                topic: typeof record.topic === 'string' ? record.topic : 'general chatter',
                messages: Array.isArray(record.messages) ? record.messages : []
            };
        }
    },
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
         * supplies a `from` taken from the session rather than the body.
         */
        async post(from: string, text: string): Promise<number> {
            ctx.state.messages.push({ from, text, at: new Date() });
            await ctx.save(); // persistence is explicit — state saves when asked
            // Announce to whoever declared interest (activity.actor.ts).
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
    // NO `streams:` here, deliberately. `useActorState(…, { live: true })`
    // re-runs the READS this page declares after any turn that mutated the
    // room, over one connection for the whole page — so a hand-written
    // stream would add nothing.
    //
    // `streams:` is still the right tool for a feed that is not a read of
    // current state: a log tail, a progress sequence, an event history.
    // `examples/counter` shows that shape.
});
