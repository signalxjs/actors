/**
 * `useActorState` / `useActorAction` against a real in-process host.
 *
 * The load-bearing assertion here is DEDUPE: two components reading the same
 * actor must produce ONE dispatch. That falls out of building on `useData`'s
 * canonical-key in-flight dedupe rather than on a bespoke cache, and it is
 * the whole answer to "ten components reading one actor = ten round trips".
 */
import { afterEach, describe, expect, it } from 'vitest';
import { component, defineApp, signal, type App } from 'sigx';
import { defineActor } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';
import { useActorAction, useActorState } from '@sigx/actors/app';
import type { AsyncState } from 'sigx';
import type { Host } from '@sigx/actors';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

let dispatches = 0;

const CartActor = defineActor({
    type: 'Cart',
    unguarded: true,
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async total() {
            dispatches++;
            return ctx.state.items.length;
        },
        async add(item: string) {
            ctx.state.items.push(item);
            await ctx.save();
            return ctx.state.items.length;
        }
    })
});

const mounted: App<unknown>[] = [];
const hosts: Host[] = [];

/**
 * `actor()` resolves the RUNNING host through the `__SIGX_ACTOR_HOST__`
 * seam, which `start()` installs — creating one is not enough.
 */
async function startHost(): Promise<Host> {
    const host = createHost({ actors: [CartActor], defaults: quiet });
    await host.start();
    hosts.push(host);
    return host;
}

/** Mount a setup function as an app; returns whatever it exposes. */
function mount<T>(setup: () => T): T {
    let captured!: T;
    const Root = component(() => {
        captured = setup();
        return () => null;
    });
    const host = document.createElement('div');
    document.body.appendChild(host);
    const app = defineApp(Root({})) as App<unknown>;
    mounted.push(app);
    app.mount(host as never);
    return captured;
}

/** Let pending microtasks and the async read settle. */
const settle = (): Promise<void> => new Promise((r) => setTimeout(r, 10));

afterEach(async () => {
    for (const app of mounted.splice(0)) app.unmount();
    for (const host of hosts.splice(0)) await host.stop();
    document.body.innerHTML = '';
    dispatches = 0;
});

describe('useActorState', () => {
    it('resolves an actor read into AsyncState', async () => {
        const host = await startHost();
        await host.actor(CartActor, 'c1').add('apple');

        const state = mount(() => useActorState(CartActor, 'c1', 'total'));
        await settle();

        expect(state.state).toBe('ready');
        expect(state.value).toBe(1);
        expect(state.error).toBeNull();
    });

    it('two components reading the same actor dispatch ONCE', async () => {
        const host = await startHost();
        await host.actor(CartActor, 'c2').add('pear');
        dispatches = 0;

        const a = mount(() => useActorState(CartActor, 'c2', 'total'));
        const b = mount(() => useActorState(CartActor, 'c2', 'total'));
        await settle();

        expect(a.value).toBe(1);
        expect(b.value).toBe(1);
        expect(dispatches).toBe(1);
    });

    it('different actor keys are different reads', async () => {
        const host = await startHost();
        await host.actor(CartActor, 'c3').add('plum');
        dispatches = 0;

        const a = mount(() => useActorState(CartActor, 'c3', 'total'));
        const b = mount(() => useActorState(CartActor, 'c4', 'total'));
        await settle();

        expect(a.value).toBe(1);
        expect(b.value).toBe(0);
        expect(dispatches).toBe(2);
    });

    it('a falsy reactive key parks the read in idle without dispatching', async () => {
        await startHost();

        const state = mount(() =>
            useActorState(CartActor, () => null as unknown as ['c5', 'total'])
        );
        await settle();

        expect(state.state).toBe('idle');
        expect(dispatches).toBe(0);
    });

    it('follows a reactive key, reading arguments out of it rather than a closure', async () => {
        const host = await startHost();
        await host.actor(CartActor, 'c6').add('fig');

        const selected = signal({ id: 'c6' });
        const state: AsyncState<number> = mount(() =>
            useActorState(CartActor, () => [selected.id, 'total'] as const)
        );
        await settle();
        expect(state.value).toBe(1);

        selected.id = 'c7'; // an actor that was never written to
        await settle();
        expect(state.value).toBe(0);
    });
});

describe('useActorAction', () => {
    it('runs a mutation and reports it as AsyncAction', async () => {
        await startHost();

        const action = mount(() => useActorAction(CartActor, 'c8', 'add'));
        expect(action.loading).toBe(false);

        const result = await action.run(['kiwi']);
        expect(result).toEqual({ ok: true, value: 1 });
        expect(action.value).toBe(1);
        expect(action.loading).toBe(false);
    });

    it('resolves a getter key at run time', async () => {
        await startHost();

        let key = 'c9';
        const action = mount(() => useActorAction(CartActor, () => key, 'add'));
        await action.run(['a']);
        key = 'c10';
        await action.run(['b']);

        const read = mount(() => useActorState(CartActor, 'c10', 'total'));
        await settle();
        expect(read.value).toBe(1); // the second run went to c10, not c9
    });
});
