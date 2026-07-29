// The state-churn workload: every increment is a ctx.save(), which on the
// AKS deployment is one Redis CAS per call — storage is on the hot path by
// design, so the perf sweep measures the whole stack, not just dispatch.
import { defineActor } from './actors.app.ts';

export const Counter = defineActor({
    type: 'Counter',
    // Public on purpose: the load generator and smoke tests call it bare.
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        },
        async current() {
            return ctx.state.count;
        }
    }),
    streams: (ctx) => ({
        async *watch() {
            yield* ctx.changes({ initial: true });
        }
    })
});
