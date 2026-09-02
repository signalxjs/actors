/**
 * The Worker and Durable Object under test, assembled the way a user would.
 *
 * Everything here is shipped code — `createHostDurableObject`,
 * `createWorkerHandler`, the placement, the host — running on real workerd.
 */
import { defineActor } from '@sigx/actors';
import { defineActorApp } from '@sigx/actors/host';
import {
    createHostDurableObject,
    createWorkerHandler,
    workerSocket
} from '@sigx/actors-cloudflare';

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
        },
        /** Arms a reminder whose FIRST delivery throws — the #326 retry shape. */
        async armFlaky() {
            ctx.state.failNext = true;
            await ctx.save();
            await ctx.reminders.set('wake', { due: 0 });
            return true;
        }
    }),
    async onReminder(ctx) {
        if (ctx.state.failNext) {
            // A rejected dispatch is not a firing: the alarm must re-arm
            // this reminder `reminderTickMs` out rather than drop it.
            ctx.state.failNext = false;
            await ctx.save();
            throw new Error('flaky: the first delivery fails');
        }
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
    namespace: (env) => env.ACTORS,
    // A short retry cadence so `reminders.test.ts` can watch a failed
    // dispatch come back on the real alarm (#326) — 30s is the default.
    app: (base) => defineActorApp({ ...base, defaults: { ...base.defaults, reminderTickMs: 500 } }),
    // The object-terminated socket (#158). Session options live HERE — the
    // session runs inside the object; the Worker only forwards the upgrade.
    socket: { origin: false }
}) {}

export default createWorkerHandler<Env>({
    actors: [Counter],
    namespace: (env) => env.ACTORS,
    // Both termination modes on one deployment — the two paths differ by
    // arity, so they compose. The Worker-terminated mounts ride the app
    // factory (they need no env); the object-terminated forwarding needs
    // the namespace binding, so it takes the `socket` sugar. The second
    // worker-terminated mount KEEPS the default 'same-origin' posture — the
    // refusal test dials it with no Origin header and must get an HTTP
    // status back, not a dead socket.
    app: (base) =>
        defineActorApp(base)
            .use(workerSocket({ origin: false }))
            .use(workerSocket({ path: '/_sigx/socket-strict' })),
    // A Worker's callers are not browsers posting a form.
    fetch: { origin: false },
    socket: { terminate: 'object' }
});
