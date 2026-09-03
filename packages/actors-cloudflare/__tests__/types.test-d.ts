/**
 * Typing contracts for the Durable Objects plugin (#351): `durableObjects()`
 * carries `never` as its `Placement`, so the app-bound `defineActor` of a
 * DO app refuses `placement` outright — which matches what the backend does
 * with it (nothing: the platform IS the directory, and a ref maps to its
 * object by name). A cluster policy declared on a DO-hosted actor used to
 * compile and be a silent no-op at runtime.
 */
import { describe, expectTypeOf, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import { defineActorApp, type ActorApp, type ActorPlugin } from '@sigx/actors/host';
import { consistentHashPolicy, type ClusterPlugin } from '@sigx/actors/cluster';
import { durableObjects, type DurableObjectNamespaceLike } from '@sigx/actors-cloudflare';

declare const namespace: DurableObjectNamespaceLike;
declare const clusterPlugin: ClusterPlugin;

interface Logger {
    info(message: string): void;
}
declare const loggerPlugin: ActorPlugin<{ log: Logger }>;

const placedBase = {
    allowAnonymous: true as const,
    state: () => ({ n: 0 }),
    methods: () => ({
        get() {
            return 0;
        }
    })
};

describe('durableObjects() narrows placement to never', () => {
    it('rejects every strategy on a durableObjects() app', () => {
        const app = defineActorApp({ actors: [] }).use(durableObjects({ namespace }));
        expectTypeOf(app).toMatchTypeOf<ActorApp<Record<never, never>, never>>();
        // `Placement` is `never`, so the option collapses to "absent".
        expectTypeOf<Parameters<typeof app.defineActor>[0]['placement']>().toEqualTypeOf<
            undefined
        >();
        void app.defineActor({
            ...placedBase,
            type: 'D1',
            // @ts-expect-error a cluster policy has no meaning on Durable Objects
            placement: consistentHashPolicy()
        });
        void app.defineActor({
            ...placedBase,
            type: 'D2',
            // @ts-expect-error nor does an untagged strategy — the DO placement never reads it
            placement: { name: 'mine' }
        });
        void app.defineActor({
            ...placedBase,
            type: 'D3',
            // @ts-expect-error nor one tagged for the DO backend itself
            placement: { name: 'mine', backend: 'durable-objects' }
        });
        // no placement at all is the only spelling
        void app.defineActor({ ...placedBase, type: 'D4' });
        void app.defineActor({ ...placedBase, type: 'D5', placement: undefined });
    });

    it('narrowing survives destructuring and composes with Ext', () => {
        const { defineActor: bound } = defineActorApp({ actors: [] })
            .use(loggerPlugin)
            .use(durableObjects({ namespace }));
        void bound({
            ...placedBase,
            type: 'D6',
            methods: (ctx) => ({
                get() {
                    expectTypeOf(ctx.log).toEqualTypeOf<Logger>();
                    return 0;
                }
            })
        });
        void bound({
            ...placedBase,
            type: 'D7',
            // @ts-expect-error a cluster policy has no meaning on Durable Objects
            placement: consistentHashPolicy()
        });
    });

    it('a cluster() app, an app without a placement plugin, and the unbound defineActor are unchanged', () => {
        const clustered = defineActorApp({ actors: [] }).use(clusterPlugin);
        void clustered.defineActor({ ...placedBase, type: 'D8', placement: consistentHashPolicy() });
        const plain = defineActorApp({ actors: [] }).use(loggerPlugin);
        void plain.defineActor({ ...placedBase, type: 'D9', placement: consistentHashPolicy() });
        void plain.defineActor({ ...placedBase, type: 'D10', placement: { name: 'mine' } });
        void defineActor({ ...placedBase, type: 'D11', placement: consistentHashPolicy() });
    });
});
