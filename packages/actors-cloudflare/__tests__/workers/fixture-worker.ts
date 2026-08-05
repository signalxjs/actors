/**
 * The Worker and Durable Object under test, assembled the way a user would.
 *
 * Everything here is shipped code — `createHostDurableObject`,
 * `createWorkerHandler`, the placement, the host — running on real workerd.
 */
import { defineActor } from '@sigx/actors';
import { createHostDurableObject, createWorkerHandler } from '@sigx/actors-cloudflare';

export interface Env {
    ACTORS: DurableObjectNamespace;
}

export const Counter = defineActor({
    type: 'Counter',
    allowAnonymous: true,
    state: () => ({ count: 0, woke: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        },
        async read() {
            return ctx.state.count;
        },
        /** Cross-actor: must reach the OTHER object, not activate a copy here. */
        async bumpPeer(key: string) {
            return ctx.actor(Counter, key).increment(1);
        },
        /** A payload big enough to probe the platform's per-value limit. */
        async fill(bytes: number) {
            ctx.state.blob = 'x'.repeat(bytes);
            await ctx.save();
            return ctx.state.blob.length;
        },
        async armIn(ms: number) {
            await ctx.reminders.set('wake', { due: ms });
            return true;
        },
        async woke() {
            return ctx.state.woke;
        },
        /** Reschedules from inside the handler — the #140 re-entrancy shape. */
        async armRescheduling() {
            ctx.state.reschedule = true;
            await ctx.save();
            await ctx.reminders.set('wake', { due: 0 });
            return true;
        }
    }),
    async onReminder(ctx) {
        ctx.state.woke++;
        if (ctx.state.reschedule) {
            ctx.state.reschedule = false;
            // Takes the gate from inside delivery. Against the pre-#140 code
            // this deadlocks the object on the real platform.
            await ctx.reminders.set('again', { due: 0 });
        }
        await ctx.save();
    },
    streams: (ctx) => ({
        async *watch() {
            yield* ctx.changes({ initial: true });
        }
    })
});

export class TestHost extends createHostDurableObject<Env>({
    actors: [Counter],
    namespace: (env) => env.ACTORS
}) {}

export default createWorkerHandler<Env>({
    actors: [Counter],
    namespace: (env) => env.ACTORS,
    // A Worker's callers are not browsers posting a form.
    fetch: { origin: false }
});
