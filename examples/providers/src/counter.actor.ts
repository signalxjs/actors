import { defineActor } from '@sigx/actors';

/**
 * The one actor every demo here runs. It is `examples/counter`'s Counter,
 * unchanged, because the point of this example is what is around the
 * actor — the store, the membership, the wire, the exporter — and an actor
 * that did anything interesting would only get in the way of that.
 *
 * `ctx.save()` on every increment is what makes the storage seam visible:
 * with `pgStorage` or `surrealStorage` underneath, each call is one
 * etag-CAS write to the database, and the failover step reads it back.
 */
export const Counter = defineActor({
    type: 'Counter',
    allowAnonymous: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        },
        async current() {
            return { count: ctx.state.count };
        }
    })
});
