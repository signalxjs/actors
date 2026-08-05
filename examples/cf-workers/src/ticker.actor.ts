/**
 * Reminder-driven, so the deployed Worker exercises the alarm path — the one
 * thing that cannot be checked by making a request.
 *
 * A reminder here is a real Durable Object alarm: the platform wakes the
 * object at the due time with nothing else running. That is why the runbook
 * can tell you to close the terminal and come back.
 */
import { defineActor } from './actors.app';

export const Ticker = defineActor({
    type: 'Ticker',
    allowAnonymous: true,
    state: () => ({ ticks: 0, repeating: false, everyMs: 1_000 }),
    methods: (ctx) => ({
        /** Fire once, `afterMs` from now. */
        async once(afterMs = 0) {
            await ctx.reminders.set('tick', { due: afterMs });
            return true;
        },
        /**
         * Fire repeatedly by rescheduling from inside the handler.
         *
         * Not `period` — the runtime floors a periodic reminder at 60s, and
         * this is the shape worth demonstrating anyway: rescheduling from
         * `onReminder` takes the object's concurrency gate from inside
         * delivery, which is exactly the interaction the runtime has to keep
         * out of its own way.
         */
        async repeat(everyMs = 1_000) {
            ctx.state.repeating = true;
            ctx.state.everyMs = everyMs;
            await ctx.save();
            await ctx.reminders.set('tick', { due: everyMs });
            return true;
        },
        async stop() {
            ctx.state.repeating = false;
            await ctx.save();
            await ctx.reminders.clear('tick');
            return true;
        },
        async ticks() {
            return ctx.state.ticks;
        }
    }),
    streams: (ctx) => ({
        /**
         * The tick count, pushed.
         *
         * `ctx.changes()` emits after any turn that mutated state — and a
         * reminder delivery IS such a turn. So a reminder firing notifies
         * every watcher by itself, with nothing polling for it. That is the
         * whole point worth demonstrating, and it is why this page needs no
         * timer.
         */
        async *watch() {
            yield* ctx.changes({ initial: true });
        }
    }),
    async onReminder(ctx) {
        ctx.state.ticks++;
        if (ctx.state.repeating) {
            await ctx.reminders.set('tick', { due: ctx.state.everyMs });
        }
        await ctx.save();
    }
});
