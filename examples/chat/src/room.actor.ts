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
    use: [requireUser],
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
