import { defineActor } from './actors.app.ts';

/**
 * `ctx.tasks` — the primitive `defineJob` is built on, on its own.
 *
 * A method call **holds the activation until it settles**. That is right for
 * milliseconds and wrong for an import, a sync or a model run: while it
 * runs, every other call to this actor queues behind it, including the
 * `status()` the caller wants in order to find out how it is going.
 *
 * A task body runs **detached, outside any turn**, so ordinary reads keep
 * answering while it works. The trade is that it gets no `ctx.state` and no
 * `ctx.save()` — every mutation goes through `ctx.turn(fn)`, which enqueues
 * `fn` as one ordinary serialized turn. Nothing about the actor model is
 * bent: the writes are still single-threaded, and change feeds and watches
 * downstream of them work unmodified.
 *
 * `defineJob` adds the durable ledger, attempts and `watch()` on top of
 * exactly this. Reach for the job when you want that; reach for a task when
 * you want detached work and nothing more.
 */
export const Importer = defineActor({
    type: 'Importer',
    allowAnonymous: true,
    state: () => ({ done: 0, total: 0, phase: 'idle' as 'idle' | 'running' | 'done' | 'stopped' }),
    methods: (ctx) => ({
        /**
         * Returns as soon as the body is LAUNCHED, not finished — and it is
         * single-flight per name, so calling it twice does not start two.
         */
        async begin(total: number) {
            await ctx.tasks.start('run', total);
            return { started: true, total };
        },
        /** Answers WHILE the task runs. That is the whole demonstration. */
        async status() {
            return { ...ctx.state, running: ctx.tasks.list().map((t) => t.name) };
        },
        /**
         * A request, not a join: it aborts and returns. Awaiting settlement
         * from inside a turn would deadlock against the task's own
         * wind-down `turn()`.
         */
        async stop() {
            await ctx.tasks.cancel('run');
            return true;
        }
    }),
    tasks: (ctx) => ({
        async run(total: number) {
            await ctx.turn((c) => {
                c.state.phase = 'running';
                c.state.total = total;
                c.state.done = 0;
            });
            try {
                for (let i = 0; i < total; i++) {
                    // In a task, this is the RUN's signal: it fires on
                    // `cancel`, and on deactivation for any reason.
                    ctx.abortSignal.throwIfAborted();
                    await new Promise((r) => setTimeout(r, 120)); // the "work"
                    await ctx.turn((c) => {
                        c.state.done = i + 1;
                    });
                }
            } catch {
                // Cancelled. Deactivation grants a bounded grace with turns
                // still open, so a final checkpoint like this one lands.
                await ctx.turn(async (c) => {
                    c.state.phase = 'stopped';
                    await c.save();
                });
                return;
            }
            await ctx.turn(async (c) => {
                c.state.phase = 'done';
                await c.save();
            });
        }
    })
});
