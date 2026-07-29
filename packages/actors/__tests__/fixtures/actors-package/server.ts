/**
 * The SERVER half of a published actor package: the real definitions.
 * Resolved by the `import`/`node` conditions in package.json.
 */
import { defineActor } from '@sigx/actors';

/** A guard chain is the package author's responsibility — the consuming
 *  app's build gate only ever sees its own source. */
const requireCaller = (): void => {};

export const Greeter = defineActor({
    type: 'acme/greeter',
    use: [requireCaller],
    state: () => ({ greeted: 0 }),
    methods: (ctx) => ({
        async greet(name: string) {
            ctx.state.greeted++;
            await ctx.save();
            return `hello ${name}`;
        },
        async count() {
            return ctx.state.greeted;
        }
    }),
    streams: (ctx) => ({
        async *watch() {
            // `{ initial: true }` seeds in the same call that subscribes; a
            // `yield ctx.snapshot()` prologue would lose every mutation
            // until the consumer resumes.
            yield* ctx.changes({ initial: true });
        }
    })
});

export const Presence = defineActor({
    type: 'acme/presence',
    unguarded: true,
    state: () => ({ online: false }),
    methods: (ctx) => ({
        async setOnline(value: boolean) {
            ctx.state.online = value;
            return ctx.state.online;
        }
    })
});
