/**
 * The fan-out workload (#172) — the actor a WebSocket scale test subscribes
 * to. `Counter` deliberately stays as it is: every one of its calls saves,
 * which measures state churn, and mixing a Redis CAS into a delivery
 * measurement is how you end up quoting storage latency as push latency.
 *
 * ## What a live subscription actually costs, and why this shape
 *
 * A client subscribes to a METHOD, not to a stream: the runtime re-invokes
 * the read after every mutating turn (`host/watch.ts`) — which is what lets
 * a subscriber ask for `current()` rather than for state it would have to
 * re-derive. Two consequences shape everything here:
 *
 *  - **The read is shared, the send is not.** N subscribers to the same
 *    `(method, throttleMs, args)` cost ONE re-invocation per turn, fanned
 *    out. But each subscriber has its own correlation id, so the session
 *    encodes and `JSON.stringify`s the frame once PER SUBSCRIBER
 *    (`server/socket-session.ts`). Fan-out is O(1) reads and O(N)
 *    serializations — which is precisely what `PAYLOAD_BYTES` exists to
 *    put a price on.
 *  - **The throttle is not negotiable.** `dispatchWatch`'s `throttleMs`
 *    option is never passed by the socket session or by `$live`, so every
 *    client subscription runs at `DEFAULT_WATCH_THROTTLE_MS` (50 ms). A
 *    subscriber therefore receives at most ~20 pushes/s no matter how fast
 *    this actor is published to, and `deliveries_per_publish` is a
 *    COALESCING RATIO rather than a constant.
 *
 * ## The two reads are the experiment
 *
 * `current()` ignores identity, so every subscriber shares one watch loop.
 * `mine()` does exactly the same work but consults `ctx.principal`, which
 * the runtime observes and answers by qualifying the watch key per encoded
 * principal (#121) — one loop per identity. Same payload, same turn cost,
 * one variable. Cross-host is harsher still: the coalesced stream keys on
 * the principal UNCONDITIONALLY (#138), so per-user subscribers get one
 * remote stream each whether or not the read touches identity.
 */
import { defineActor } from './actors.app.ts';

/** Bounded so a mistyped env cannot ask for a gigabyte of payload. */
const MAX_PAYLOAD_BYTES = 1 << 20;

export const Fanout = defineActor({
    type: 'Fanout',
    // Public on purpose: the load generators subscribe bare, and the
    // anonymous arm is half the experiment.
    allowAnonymous: true,
    state: () => ({ seq: 0, at: 0, payload: '' }),
    methods: (ctx) => ({
        /**
         * One publish. `persist` is the whole storage axis: false mutates
         * state and returns, true adds one Redis CAS to the same turn.
         *
         * A mutation with no `ctx.save()` still notifies subscribers — the
         * dirty mark is set by state tracking at write time and folded into
         * the version at the turn boundary, entirely independently of
         * saving (`host/activation.ts`). That is load-bearing for the
         * no-persist arm, so `__tests__/fanout.test.ts` pins it.
         */
        async publish(bytes: number, persist: boolean) {
            const size = Math.min(Math.max(0, Math.trunc(bytes)), MAX_PAYLOAD_BYTES);
            ctx.state.seq += 1;
            // The publisher's clock. The load generator only compares it
            // against its own when the publisher and the subscriber are the
            // SAME process (its probe connections) — across two pods this
            // would be measuring clock skew.
            ctx.state.at = Date.now();
            ctx.state.payload = 'x'.repeat(size);
            if (persist) await ctx.save();
            return ctx.state.seq;
        },

        /** The identity-blind read. Every subscriber shares one watch loop. */
        async current() {
            return { seq: ctx.state.seq, at: ctx.state.at, payload: ctx.state.payload };
        },

        /**
         * The identity-aware twin. Byte-for-byte the same result shape plus
         * the caller's name, but consulting `ctx.principal` is what makes
         * the runtime split the shared loop per principal.
         */
        async mine() {
            const who = (ctx.principal as { name: string } | null)?.name ?? null;
            return { seq: ctx.state.seq, at: ctx.state.at, payload: ctx.state.payload, who };
        },

        /** Cheap unary call for the `calls` mode — no state, no save. */
        async ping() {
            return ctx.state.seq;
        }
    })
});
