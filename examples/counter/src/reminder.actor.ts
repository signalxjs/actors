import { defineActor } from './actors.app.ts';

/**
 * A **durable reminder**, and the property that makes it different from a
 * timer: it re-activates an actor that is no longer there.
 *
 * `ctx.timer()` is volatile — it ticks as an ordinary turn and dies with the
 * activation. `ctx.reminders.set()` is stored through `ActorStorage` and
 * fired by the HOST's scheduler, so it survives the activation being swept,
 * the process exiting, and the actor being placed on a different host
 * entirely. When it fires, the runtime activates the actor to deliver it.
 *
 * That is the whole reason to reach for one: work that must happen even
 * though nobody is going to call you.
 */
export const Reminder = defineActor({
    type: 'Reminder',
    // Demo-public. A real actor decides with `authorize` or inherits the
    // app's default policy — see examples/chat.
    allowAnonymous: true,
    state: () => ({ fired: 0, armedAt: null as Date | null, lastFiredAt: null as Date | null }),
    methods: (ctx) => ({
        /**
         * Arm a ONE-SHOT reminder. `period` would make it repeat, and that
         * is the argument with a floor — 60s, because a durable reminder is
         * a storage write per tick. A one-shot `due` has no minimum.
         */
        async arm(dueMs: number) {
            ctx.state.armedAt = new Date();
            await ctx.save();
            await ctx.reminders.set('wake', { due: dueMs });
            return ctx.state.armedAt;
        },
        async status() {
            return {
                fired: ctx.state.fired,
                armedAt: ctx.state.armedAt,
                lastFiredAt: ctx.state.lastFiredAt,
                pending: await ctx.reminders.list()
            };
        }
    }),
    /**
     * Delivery. This runs as an ordinary turn on a freshly activated actor,
     * so `ctx.state` is whatever storage held — note that `fired` keeps
     * counting up across a process restart, which is the point.
     */
    async onReminder(ctx, name) {
        if (name !== 'wake') return;
        ctx.state.fired += 1;
        ctx.state.lastFiredAt = new Date();
        await ctx.save();
    }
});
