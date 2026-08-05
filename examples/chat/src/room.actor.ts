/**
 * A chat room — one actor per room name, single-threaded, persistent.
 *
 * Authorization runs on every transport — the wire call from the browser
 * and the in-process dispatch during an SSR render. Proving those two agree
 * is most of why this example exists, because they reach the request very
 * differently.
 *
 * Two layers do it. The app's default policy (`server-app.ts`) answers *"is
 * this caller signed in?"* for everything, with nothing declared here; the
 * `authorize` below answers the one question that default cannot reach,
 * *"may they call it on THIS room?"*, because only a policy sees the
 * instance.
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

/** A room only its owner may open: `dm-<user>-<whatever>`. */
const PRIVATE = /^dm-([\w-]+?)-/;

export const RoomActor = defineActor({
    type: 'Room',
    /**
     * The keyed question, which is the one a guard could not answer.
     *
     * A guard is called `(rq, { symbol, name })` — no key, no arguments —
     * because it runs before the endpoint has decoded which actor you meant.
     * So "is this caller signed in?" was expressible and "does this caller
     * own THIS instance?" was not. A policy receives the resolved principal
     * AND `op.resource`, so it is ordinary.
     *
     * Strict-`true`: anything other than `true` denies (403, or 401 when the
     * principal is null). Returning a falsy value by accident fails closed.
     *
     * It runs on EVERY transport — the wire endpoint, `$live`, and the
     * in-process dispatch during SSR — and always outside any turn, so a
     * slow check never occupies the actor.
     *
     * Try it: signed in as `ada`, open `/r/dm-ada-notes`. Then open
     * `/r/dm-bob-notes` and watch the page render the refusal, because an
     * actor read reports a policy rejection as state rather than throwing.
     */
    authorize: (user, _rq, op) => {
        // `op.resource` is optional on the shared policy type — a serverFn
        // op has none. For an actor policy it is always the instance, so
        // this branch is unreachable here; deny rather than assert, because
        // an unidentified resource is exactly when you want to fail closed.
        if (!op.resource) return false;
        const match = PRIVATE.exec(op.resource.key);
        if (match === null) return true; // an ordinary, shared room
        // `user` is non-null here in practice — this actor does not declare
        // `allowAnonymous`, so core's pre-decode prelude has already 401'd
        // an anonymous caller before any policy runs. The type still admits
        // null, because a policy on an `allowAnonymous: true` actor DOES
        // see it. Handle it, rather than leave a latent null dereference
        // for whoever copies this into that case.
        return user !== null && match[1] === user.name;
    },
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
