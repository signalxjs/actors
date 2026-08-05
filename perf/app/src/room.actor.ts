/**
 * A chat room — one actor per room name, single-threaded, persistent.
 *
 * **This actor declares no authorization at all, and that is the point.**
 * The app's default policy (`server-app.ts`) decides, on EVERY transport:
 * the wire call from the browser and the in-process dispatch during an SSR
 * render. Proving those two agree is why this example exists, because they
 * reach the request very differently. Before rfc-server-v4 this line was
 * `use: [requireUser]`, repeated on every actor in the example.
 *
 * NOTE ON AUTHORIZATION. This is where the split earns itself. A pre-v4
 * guard received `(rq, { symbol, name })` — not the actor key, not the
 * arguments — so "is this signed in?" was expressible and "does this user
 * own THIS room?" was not. A policy receives the resolved principal AND the
 * instance, so the second question is now ordinary:
 *
 * ```ts
 * authorize: (user, _rq, op) => op.resource!.key.startsWith(`${user.name}:`)
 * ```
 *
 * For "who is the caller?", `ctx.principal` is the answer everywhere — no
 * stamping, no bag key, and it survives every `ctx.actor`/`ctx.publish` hop
 * and every host-to-host forward, so the ActivityFeed subscriber attributes
 * its entries from exactly the identity the edge authenticated.
 *
 * `post` still takes `from` as an explicit serverFn-supplied argument: the
 * wire signature is pinned, and the serverFn remains a fine trust boundary.
 */
import { defineActor } from './actors.app';
import { roomActivity } from './activity.actor';

export interface Message {
    readonly from: string;
    readonly text: string;
    readonly at: Date;
}

/**
 * The shape version carried IN the record. Bumping this plus teaching
 * `migrateState` below about the old shape is the whole upgrade ritual —
 * see the hook for why the deployment cares.
 */
export const ROOM_STATE_VERSION = 2;

interface RoomState {
    /** Absent on every record written before this field existed — which is
     *  exactly what makes the migration testable against a real rolling
     *  deploy rather than a fixture. */
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
     * Evolve a stored room between the storage read and activation.
     *
     * The v1 shape is `{ topic, messages }` with no `v` — literally what the
     * previous image wrote — so a rolling deploy of THIS image migrates real
     * records under real load, which is the only place the interesting
     * questions live (how often the CAS conflicts, what the write
     * amplification is, whether activation latency moves). The infra suite
     * asserts it through `version()` below.
     *
     * Returning `stored` unchanged is the fast path and identity is how the
     * runtime detects it, so the migrating branch builds a NEW object.
     *
     * Lazy by default: the migrated shape rides the next save the room would
     * have made anyway, so a rolling deploy adds no write amplification. The
     * consequence is that a room only ever READ after the deploy never
     * persists its migration — `MIGRATE_PERSIST=eager` is the opt-in to one
     * CAS write-back at activation, and the knob exists so the deployment can
     * measure the difference.
     */
    migrateState: {
        persist: process.env.MIGRATE_PERSIST === 'eager' ? 'eager' : 'lazy',
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
         * The migration, observable from outside. A room written by the
         * previous image answers `2` here only because `migrateState` ran.
         */
        async version(): Promise<number> {
            return ctx.state.v;
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
