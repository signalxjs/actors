/**
 * Watching in a cluster.
 *
 * A LOCALLY placed actor watches normally — placement hands back the local
 * host, which implements `dispatchWatch`.
 *
 * A REMOTELY placed one does not yet: the silo-to-silo endpoint decides
 * stream-vs-unary from `def.streamNames`, and a watch is neither — it is an
 * ordinary read dispatched in watch mode, so forwarding it needs a flag on
 * the call envelope. That is deliberately a separate change (#69).
 *
 * What matters until then is that the limit is LOUD: `dispatchWatch` is
 * optional on `ActorDispatcher`, so a remote dispatcher simply lacks it and
 * the silo throws a message naming the placement — rather than hanging, or
 * silently watching a second activation on the wrong host.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, type Silo } from '@sigx/actors';
import { defineActorApp, memoryStorage, type ActorApp } from '@sigx/actors/silo';
import { cluster, memoryClusterHub, preferLocalPolicy } from '@sigx/actors/cluster';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async read() {
            return ctx.state.count;
        },
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        }
    })
});

const apps: ActorApp[] = [];

async function twoSilos(): Promise<Silo[]> {
    const hub = memoryClusterHub();
    const storage = memoryStorage();
    const silos: Silo[] = [];
    for (let i = 0; i < 2; i++) {
        const app = defineActorApp({ actors: [Counter], storage, defaults: quiet }).use(
            cluster({
                providers: hub.providers(),
                advertise: `http://127.0.0.1:${9700 + i}`,
                secret: 'watch-cluster-secret',
                policy: preferLocalPolicy()
            })
        );
        apps.push(app);
        silos.push(await app.start());
    }
    return silos;
}

afterEach(async () => {
    for (const app of apps.splice(0)) await app.stop({ timeoutMs: 1000 });
});

describe('dispatchWatch in a cluster', () => {
    it('watches an actor placed on the LOCAL silo', async () => {
        const [a] = await twoSilos();
        // prefer-local places it here on first touch.
        await a!.actor(Counter, 'local').increment(1);

        const iterator = a!
            .dispatchWatch!({ type: 'Counter', key: 'local' }, 'read', [], {
                callChain: [],
                callId: 'c'
            })
            [Symbol.asyncIterator]();
        expect((await iterator.next()).value).toBe(1);

        await a!.actor(Counter, 'local').increment(4);
        expect((await iterator.next()).value).toBe(5);
        await iterator.return?.();
    });

    it('refuses a REMOTELY placed actor with a message naming the limit', async () => {
        const [a, b] = await twoSilos();
        // Place it on b, then watch from a — a cross-host watch.
        await b!.actor(Counter, 'remote').increment(1);

        const watching = a!.dispatchWatch!({ type: 'Counter', key: 'remote' }, 'read', [], {
            callChain: [],
            callId: 'c'
        });
        await expect(watching[Symbol.asyncIterator]().next()).rejects.toThrow(/cannot watch/);
    });
});
