import { defineActor } from './actors.app.ts';

/**
 * The subscribing half. `subscriptions:` is a declaration on the TYPE:
 * every publish to `gate-passed` reaches a Tally whether or not one is
 * active — a publish ACTIVATES an idle subscriber, exactly as a reminder
 * does — and the subscriber set is a pure function of which types the host
 * was started with. Start a host without Tally and the same publish reaches
 * nobody, which is a report saying `subscribers: 0`, not an error.
 *
 * The handler is an ordinary turn: it mutates state and calls `ctx.save()`,
 * it is serialized with the actor's other turns, and it is NOT
 * wire-callable — no client can fake a delivery.
 */
export const Tally = defineActor({
    type: 'Tally',
    allowAnonymous: true,
    state: () => ({
        deliveries: 0,
        byGate: {} as Record<string, number>,
        lastFrom: null as string | null
    }),
    methods: (ctx) => ({
        async totals() {
            return ctx.snapshot();
        }
    }),
    subscriptions: {
        'gate-passed': {
            // The topic key is the GATE's key. Left alone it would also pick
            // the subscriber — one Tally per gate. Mapping every key to one
            // constant makes a single instance the aggregate.
            key: () => 'all',
            handle: async (ctx, event) => {
                const { count } = event.payload as { count: number };
                ctx.state.deliveries += 1;
                ctx.state.byGate[event.topic.key] = count;
                // `publisher` is the actor whose turn published — set for
                // `ctx.publish`, absent for `host.publish()` from plain
                // server code outside any actor.
                ctx.state.lastFrom = event.publisher
                    ? `${event.publisher.type}/${event.publisher.key}`
                    : 'outside any actor';
                await ctx.save();
            }
        }
    }
});
