import { topic } from '@sigx/actors';
import { defineActor } from './actors.app.ts';

/**
 * The topic, declared ONCE and imported by both sides. The NAME is the
 * namespace a `subscriptions:` entry binds to; the KEY is the publishing
 * gate's own key, so a subscriber can tell gates apart without a field in
 * the payload. The type parameter types the payload on the publish side.
 */
export const gatePassed = (gate: string) => topic<{ count: number }>('gate-passed', gate);

/**
 * The publishing half of actor pub/sub. Nothing here knows who listens:
 * `ctx.publish` fans out to every TYPE in the deploy that declared a
 * `subscriptions:` entry for the topic name — no registration, no broker,
 * nothing stored. Framework-free on purpose: `examples/chat` runs the same
 * pattern under a sigx app, and none of it depends on the app.
 */
export const Gate = defineActor({
    type: 'Gate',
    allowAnonymous: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async pass() {
            ctx.state.count += 1;
            await ctx.save();
            // Awaited: it settles when every subscriber's turn has, and it
            // resolves to a REPORT. A subscriber that throws fails only its
            // own delivery and lands in `failures` — the publisher never
            // throws for it.
            const report = await ctx.publish(gatePassed(ctx.key), { count: ctx.state.count });
            return { count: ctx.state.count, ...report };
        }
    })
});
