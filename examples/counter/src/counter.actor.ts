// The app-bound defineActor: `ctx` carries whatever this app's plugins
// add. Identical at runtime — the TYPE is what differs.
import { defineActor } from './actors.app.ts';

/**
 * A virtual actor: addressable by key, activated lazily on first call,
 * single-threaded (one turn at a time — no races on `ctx.state`), and
 * persistent across restarts via the host's storage.
 */
export const Counter = defineActor({
    type: 'Counter',
    // Deliberately public. A real actor either declares an `authorize`
    // policy or inherits the app's default — the build gate insists you
    // pick one. See examples/chat for both.
    allowAnonymous: true,
    state: () => ({ count: 0, lastVisit: null as Date | null }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            ctx.state.lastVisit = new Date();
            await ctx.save(); // persistence is explicit — state saves only when asked
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
