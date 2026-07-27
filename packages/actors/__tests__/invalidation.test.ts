/**
 * A write refreshes the reads it staled — and only those.
 *
 * The whole point is that `await post.run(text)` no longer has to be
 * followed by a hand-written `await messages.refresh()`, so these assert on
 * observed dispatch counts and rendered values rather than on the registry's
 * internals.
 */
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { component, defineApp, signal, type App } from 'sigx';
import { defineActor, type Silo } from '@sigx/actors';
import { createSilo } from '@sigx/actors/silo';
import { actorsPlugin, useActorAction, useActorState } from '@sigx/actors/app';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

let reads = 0;

const CartActor = defineActor({
    type: 'Cart',
    unguarded: true,
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async total() {
            reads++;
            return ctx.state.items.length;
        },
        async first() {
            reads++;
            return ctx.state.items[0] ?? null;
        },
        async add(item: string) {
            ctx.state.items.push(item);
            await ctx.save();
            return ctx.state.items.length;
        }
    })
});

const OrderActor = defineActor({
    type: 'Order',
    unguarded: true,
    state: () => ({ n: 7 }),
    methods: (ctx) => ({
        async count() {
            reads++;
            return ctx.state.n;
        }
    })
});

const mounted: App<unknown>[] = [];
const silos: Silo[] = [];

async function startSilo(): Promise<Silo> {
    const silo = createSilo({ actors: [CartActor, OrderActor], defaults: quiet });
    await silo.start();
    silos.push(silo);
    return silo;
}

/** Mount a setup fn inside an app that has the plugin installed. */
function mount<T>(setup: () => T): T {
    return mountAll({ only: setup }).only;
}

/**
 * Mount several components into ONE app.
 *
 * The registry is per-app by design, so a reader and a writer in separate
 * apps genuinely should not see each other — which makes separate `mount()`
 * calls useless for testing invalidation, and is why this exists.
 */
function mountAll<T extends Record<string, () => unknown>>(
    setups: T
): { [K in keyof T]: ReturnType<T[K]> } {
    const captured = {} as { [K in keyof T]: ReturnType<T[K]> };
    const children = Object.entries(setups).map(([name, setup]) =>
        component(() => {
            captured[name as keyof T] = setup() as ReturnType<T[keyof T]>;
            return () => null;
        })
    );
    const Root = component(() => () => children.map((Child) => Child({})));
    const host = document.createElement('div');
    document.body.appendChild(host);
    const app = defineApp(Root({})).use(actorsPlugin()) as App<unknown>;
    mounted.push(app);
    app.mount(host as never);
    return captured;
}

const settle = (): Promise<void> => new Promise((r) => setTimeout(r, 15));

beforeEach(() => {
    reads = 0;
});

afterEach(async () => {
    for (const app of mounted.splice(0)) app.unmount();
    for (const silo of silos.splice(0)) await silo.stop();
    document.body.innerHTML = '';
    delete (globalThis as { __SIGX_ASYNC__?: unknown }).__SIGX_ASYNC__;
});

describe('invalidation on write', () => {
    it('refreshes a read of the same actor, with no manual refresh()', async () => {
        await startSilo();

        const view = mount(() => ({
            total: useActorState(CartActor, 'c1', 'total'),
            add: useActorAction(CartActor, 'c1', 'add')
        }));
        await settle();
        expect(view.total.value).toBe(0);

        await view.add.run(['apple']);
        await settle();

        expect(view.total.value).toBe(1); // no refresh() call anywhere
    });

    it('refreshes EVERY read of that actor, across components', async () => {
        await startSilo();

        const { a, b, write } = mountAll({
            a: () => useActorState(CartActor, 'c2', 'total'),
            b: () => useActorState(CartActor, 'c2', 'first'),
            write: () => useActorAction(CartActor, 'c2', 'add')
        });
        await settle();

        await write.run(['pear']);
        await settle();

        expect(a.value).toBe(1);
        expect(b.value).toBe('pear');
    });

    it('leaves a different actor alone', async () => {
        await startSilo();

        const { cart, order, write } = mountAll({
            cart: () => useActorState(CartActor, 'c3', 'total'),
            order: () => useActorState(OrderActor, 'o1', 'count'),
            write: () => useActorAction(CartActor, 'c3', 'add')
        });
        await settle();
        const before = reads;

        await write.run(['plum']);
        await settle();

        expect(cart.value).toBe(1);
        expect(order.value).toBe(7);
        // Exactly one re-read: the cart's. The order was never touched.
        expect(reads).toBe(before + 1);
    });

    it('leaves a different KEY of the same type alone', async () => {
        await startSilo();

        const { c4, c40, write } = mountAll({
            c4: () => useActorState(CartActor, 'c4', 'total'),
            c40: () => useActorState(CartActor, 'c40', 'total'),
            write: () => useActorAction(CartActor, 'c4', 'add')
        });
        await settle();
        const before = reads;

        await write.run(['fig']);
        await settle();

        expect(c4.value).toBe(1);
        expect(c40.value).toBe(0);
        expect(reads).toBe(before + 1); // 'c4' is not a prefix of 'c40'
    });

    it('invalidates: false disables it', async () => {
        await startSilo();

        const view = mount(() => ({
            total: useActorState(CartActor, 'c5', 'total'),
            add: useActorAction(CartActor, 'c5', 'add', { invalidates: false })
        }));
        await settle();

        await view.add.run(['kiwi']);
        await settle();

        expect(view.total.value).toBe(0); // stale on purpose
        await view.total.refresh();
        await settle();
        expect(view.total.value).toBe(1);
    });

    it('honours an explicit pattern list', async () => {
        await startSilo();

        // A cart write declared as staling every Order instead.
        const { cart, order, write } = mountAll({
            cart: () => useActorState(CartActor, 'c6', 'total'),
            order: () => useActorState(OrderActor, 'o2', 'count'),
            write: () =>
                useActorAction(CartActor, 'c6', 'add', {
                    invalidates: [['@actor', 'Order']]
                })
        });
        await settle();
        const before = reads;

        await write.run(['date']);
        await settle();

        expect(order.value).toBe(7);
        expect(reads).toBe(before + 1); // the Order re-read…
        expect(cart.value).toBe(0); // …and the cart was NOT, as declared
    });

    it('accepts a function form, given the result and the key', async () => {
        await startSilo();

        const seen: unknown[] = [];
        const view = mount(() => ({
            total: useActorState(CartActor, 'c7', 'total'),
            add: useActorAction(CartActor, 'c7', 'add', {
                invalidates: (result, key) => {
                    seen.push([result, key]);
                    return [['@actor', 'Cart', key]];
                }
            })
        }));
        await settle();

        await view.add.run(['lime']);
        await settle();

        expect(seen).toEqual([[1, 'c7']]); // the write's return value
        expect(view.total.value).toBe(1);
    });

    it('does not invalidate when the write FAILS', async () => {
        const silo = await startSilo();

        const view = mount(() => ({
            total: useActorState(CartActor, 'c8', 'total'),
            // A method that does not exist — the dispatch rejects.
            bad: useActorAction(CartActor, 'c8', 'nope' as 'add')
        }));
        await settle();
        await silo.actor(CartActor, 'c8').add('cherry'); // change it behind the read
        const before = reads;

        const result = await view.bad.run(['x']);
        await settle();

        expect(result.ok).toBe(false);
        expect(reads).toBe(before); // nothing re-read
        expect(view.total.value).toBe(0); // still the value it had
    });

    it('matches a REACTIVE key at its current value, not the one it mounted with', async () => {
        await startSilo();

        const selected = signal({ id: 'c9' });
        const view = mount(() => ({
            total: useActorState(CartActor, () => [selected.id, 'total'] as const),
            add: useActorAction(CartActor, () => selected.id, 'add')
        }));
        await settle();

        // Move the selection: the read is now of c10, but it registered
        // while pointing at c9.
        selected.id = 'c10';
        await settle();
        expect(view.total.value).toBe(0);

        await view.add.run(['melon']);
        await settle();

        expect(view.total.value).toBe(1); // refreshed as c10, not c9
    });

    it('drops the page-payload entry so a remount refetches', async () => {
        await startSilo();

        const blob = Object.assign(Object.create(null) as Record<string, unknown>, {
            '["@actor","Cart","c11","total"]': 99
        });
        (globalThis as { __SIGX_ASYNC__?: Record<string, unknown> }).__SIGX_ASYNC__ = blob;

        const view = mount(() => ({
            total: useActorState(CartActor, 'c11', 'total'),
            add: useActorAction(CartActor, 'c11', 'add')
        }));
        await settle();
        expect(view.total.value).toBe(99); // restored from the payload

        await view.add.run(['nut']);
        await settle();

        expect(view.total.value).toBe(1); // the real value, not the seed
        // The stale seed is gone. `useData` writes the FRESH value back into
        // the blob after the refetch, so a remount restores 1 rather than
        // resurrecting 99 — which is the property that actually matters.
        expect(blob['["@actor","Cart","c11","total"]']).toBe(1);
    });

    it('unregisters on unmount — no refresh after the component is gone', async () => {
        await startSilo();

        const { gone, write } = mountAll({
            gone: () => useActorState(CartActor, 'c12', 'total'),
            write: () => useActorAction(CartActor, 'c12', 'add')
        });
        await settle();

        // Tear the whole app down: every read unregisters, so the write has
        // nothing left to refresh.
        mounted.pop()!.unmount();
        const before = reads;

        await write.run(['olive']);
        await settle();

        expect(reads).toBe(before);
        expect(gone.value).toBe(0);
    });
});
