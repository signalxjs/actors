/**
 * Traceparent propagation across a cluster — issue #245, end to end and
 * with no tracing library anywhere: a caller-side `useDispatch` middleware
 * stamps the context, the envelope's `tp` field carries it host-to-host,
 * `ctx.actor` relays it through an intermediate actor, and the owner host's
 * turn observer reads it back out. That is the whole seam a tracing
 * exporter rides.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import type { ActorPlugin } from '@sigx/actors/host';
import { createCluster, selfPolicy, type ClusterHarness } from './harness';

const TP = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        }
    })
});

const Relay = defineActor({
    type: 'Relay',
    unguarded: true,
    state: () => ({}),
    methods: (ctx) => ({
        async relay(key: string) {
            return (ctx.actor(Counter, key) as { increment(by: number): Promise<number> })
                .increment(1);
        }
    })
});

/** Stamp `TP` onto dispatches of `type` only — everything else untouched,
 *  so a relayed hop proves inheritance rather than re-injection. */
function injectTraceparent(type: string): ActorPlugin {
    return {
        name: 'inject-tp',
        setup(registry) {
            registry.useDispatch((next) => ({
                dispatch: (ref, method, args, call) =>
                    next.dispatch(
                        ref,
                        method,
                        args,
                        ref.type === type ? { ...call, traceparent: TP } : call
                    ),
                ...(next.dispatchStream && {
                    dispatchStream: (ref, method, args, call) =>
                        next.dispatchStream!(ref, method, args, call)
                })
            }));
        }
    };
}

let running: ClusterHarness | null = null;
afterEach(async () => {
    await running?.stop();
    running = null;
});

describe('traceparent across the cluster', () => {
    it('crosses a host-to-host hop and reaches the owner turn observer', async () => {
        const cluster = await createCluster(2, {
            actors: [Counter],
            policy: selfPolicy,
            plugins: (i) => (i === 0 ? [injectTraceparent('Counter')] : [])
        });
        running = cluster;

        // selfPolicy: host 1's first call makes it the owner.
        await cluster.hosts[1]!.actor(Counter, 'k').increment(1);
        const seen: Array<{ method: string; traceparent: string | undefined }> = [];
        cluster.hosts[1]!.observeTurns((_ref, method, _q, _e, _f, call) => {
            seen.push({ method, traceparent: call?.traceparent });
        });

        // host 0 dispatches → its middleware stamps TP → envelope → host 1.
        await cluster.hosts[0]!.actor(Counter, 'k').increment(1);
        expect(seen).toEqual([{ method: 'increment', traceparent: TP }]);
    });

    it('is relayed by ctx.actor through an uninstrumented hop', async () => {
        const cluster = await createCluster(2, {
            actors: [Counter, Relay],
            policy: selfPolicy,
            // The middleware stamps only the RELAY dispatch. The Counter hop
            // it makes must inherit the context via ctx.actor, not get it
            // re-stamped.
            plugins: (i) => (i === 0 ? [injectTraceparent('Relay')] : [])
        });
        running = cluster;

        // Counter#far is owned by host 1; Relay activates on host 0.
        await cluster.hosts[1]!.actor(Counter, 'far').increment(1);
        const seen: Array<{ type: string; traceparent: string | undefined }> = [];
        cluster.hosts[1]!.observeTurns((ref, _m, _q, _e, _f, call) => {
            seen.push({ type: ref.type, traceparent: call?.traceparent });
        });

        await cluster.hosts[0]!.actor(Relay, 'r').relay('far');
        // Middleware stamped Relay on host 0 → Relay's turn context carried
        // TP → ctx.actor inherited it → envelope crossed to host 1.
        expect(seen).toEqual([{ type: 'Counter', traceparent: TP }]);
    });
});
