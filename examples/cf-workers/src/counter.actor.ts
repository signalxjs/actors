/**
 * State churn: every call is a real Durable Object storage write, so a run
 * measures the persistence path rather than in-memory arithmetic.
 */
import { defineActor } from './actors.app';

export const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by = 1) {
            ctx.state.count += by;
            // One `ctx.save()` per call = one storage write per call. That is
            // deliberate: without it the object would batch and the numbers
            // would describe turn serialization, not the platform.
            await ctx.save();
            return ctx.state.count;
        },
        async read() {
            return ctx.state.count;
        },
        /**
         * Calls ANOTHER actor. On Workers this leaves the object and comes
         * back in through the callee's own — which is the thing that would
         * silently corrupt state if the placement were wrong.
         */
        async bumpPeer(key: string) {
            return ctx.actor(Counter, key).increment(1);
        }
    }),
    streams: (ctx) => ({
        async *watch() {
            yield* ctx.changes({ initial: true });
        }
    })
});
