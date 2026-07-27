// The app-bound defineActor: `ctx` carries whatever the app's plugins add.
import { defineActor } from './actors.app';

/**
 * A virtual actor: addressable by key, activated lazily on first call,
 * single-threaded (one turn at a time — no races on `ctx.state`), and
 * persistent across restarts via the silo's storage.
 */
export const Counter = defineActor({
    type: 'Counter',
    // This demo counter is deliberately public. Real actors declare a
    // `use: [...]` guard chain — the build gate insists you pick one.
    unguarded: true,
    state: () => ({ count: 0, lastVisit: null as Date | null }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            ctx.state.lastVisit = new Date();
            await ctx.save(); // Orleans WriteStateAsync — explicit by default
            return ctx.state.count;
        },
        async current() {
            return { count: ctx.state.count, lastVisit: ctx.state.lastVisit };
        }
    }),
    streams: (ctx) => ({
        /** Push the current snapshot, then one after every change. */
        async *watch() {
            yield* ctx.changes({ initial: true });
        }
    })
});
