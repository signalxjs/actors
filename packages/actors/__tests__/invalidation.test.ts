/**
 * A write refreshes the reads it staled — and only those.
 *
 * The whole point is that `await post.run(text)` no longer has to be
 * followed by a hand-written `await messages.refresh()`, so these assert on
 * observed dispatch counts and rendered values rather than on the registry's
 * internals.
 */
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { component, defineApp, onUnmounted, signal, type App } from 'sigx';
import { actor, actorKey, defineActor, type Host } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';
import {
    actorsPlugin,
    useActorAction,
    useActorsContext,
    useActorState
} from '@sigx/actors/app';

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
    }),
    streams: (ctx) => ({
        /** The push side — see the last describe in this file. */
        async *watch() {
            yield* ctx.changes({ initial: true });
        },
        /** The same feed WITHOUT the seed, to show what that seed buys. */
        async *watchFromNow() {
            yield* ctx.changes();
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
const hosts: Host[] = [];

async function startHost(): Promise<Host> {
    const host = createHost({ actors: [CartActor, OrderActor], defaults: quiet });
    await host.start();
    hosts.push(host);
    return host;
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
    for (const host of hosts.splice(0)) await host.stop();
    document.body.innerHTML = '';
    delete (globalThis as { __SIGX_ASYNC__?: unknown }).__SIGX_ASYNC__;
});

describe('invalidation on write', () => {
    it('refreshes a read of the same actor, with no manual refresh()', async () => {
        await startHost();

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
        await startHost();

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
        await startHost();

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
        await startHost();

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
        await startHost();

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
        await startHost();

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
        await startHost();

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
        const host = await startHost();

        const view = mount(() => ({
            total: useActorState(CartActor, 'c8', 'total'),
            // A method that does not exist — the dispatch rejects.
            bad: useActorAction(CartActor, 'c8', 'nope' as 'add')
        }));
        await settle();
        await host.actor(CartActor, 'c8').add('cherry'); // change it behind the read
        const before = reads;

        const result = await view.bad.run(['x']);
        await settle();

        expect(result.ok).toBe(false);
        expect(reads).toBe(before); // nothing re-read
        expect(view.total.value).toBe(0); // still the value it had
    });

    it('matches a REACTIVE key at its current value, not the one it mounted with', async () => {
        await startHost();

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
        await startHost();

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
        await startHost();

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

/**
 * The registry is per-app, so everything above only ever refreshes the page
 * that wrote. A SECOND page (another browser tab) writing the same actor is
 * invisible to it — there is no push in a request/response call.
 *
 * The bridge is a `streams:` change feed: subscribe to it and hand each
 * value to the same `cells.invalidate()` a local write would have called.
 * `examples/chat` wires exactly this, which is what makes messages typed in
 * one tab appear in the others.
 */
describe('a write from another client', () => {
    /** Mount a read, plus optionally a change feed driving invalidation. */
    function mountReader(key: string, options: { live: false | 'watch' | 'watchFromNow' }) {
        return mount(() => {
            const total = useActorState(CartActor, key, 'total');
            if (!options.live) return { total };
            const { cells } = useActorsContext();
            const feed = actor(CartActor, key)[options.live]()[Symbol.asyncIterator]();
            // Closed on unmount, exactly as the example's AbortController is
            // — otherwise the activation's keep-alive ref outlives the test.
            onUnmounted(() => void feed.return?.(undefined));
            void (async () => {
                for (;;) {
                    if ((await feed.next()).done) return;
                    cells.invalidate([actorKey(CartActor, key)]);
                }
            })();
            return { total };
        });
    }

    it('does not reach a read that only listens to its own app', async () => {
        const host = await startHost();
        const view = mountReader('c13', { live: false });
        await settle();
        expect(view.total.value).toBe(0);

        // The "other tab": a write this app had no part in.
        await host.actor(CartActor, 'c13').add('apple');
        await settle();

        expect(view.total.value).toBe(0); // stale, and nothing said so
    });

    it('reaches it once a ctx.changes() feed drives the invalidation', async () => {
        const host = await startHost();
        const view = mountReader('c14', { live: 'watch' });
        await settle();
        expect(view.total.value).toBe(0);

        await host.actor(CartActor, 'c14').add('apple');
        await settle();

        expect(view.total.value).toBe(1);

        // And it keeps up — the feed is not a one-shot.
        await host.actor(CartActor, 'c14').add('pear');
        await settle();
        expect(view.total.value).toBe(2);
    });

    /**
     * The window a live page cannot see from inside: SSR reads the actor
     * during the render, the browser subscribes only after the bundle has
     * loaded and hydrated, and a write landing in between belongs to
     * neither. `{ initial: true }` is what covers it — without the seed the
     * page waits for the NEXT write, which in a quiet room never comes.
     *
     * Staged with the page payload, because that is what makes the mounted
     * read hold the SSR value rather than fetch the current one.
     */
    function seedPage(key: string, value: number): void {
        (globalThis as { __SIGX_ASYNC__?: Record<string, unknown> }).__SIGX_ASYNC__ =
            Object.assign(Object.create(null) as Record<string, unknown>, {
                [`["@actor","Cart","${key}","total"]`]: value
            });
    }

    it('catches up on a write that landed between the SSR read and the subscription', async () => {
        const host = await startHost();
        seedPage('c15', 0); // what the server rendered
        await host.actor(CartActor, 'c15').add('apple'); // …then someone else posted

        const view = mountReader('c15', { live: 'watch' });
        await settle();

        expect(view.total.value).toBe(1); // the seed's first value closed it
    });

    it('…and without the seed the page sits on the stale value', async () => {
        const host = await startHost();
        seedPage('c16', 0);
        await host.actor(CartActor, 'c16').add('apple');

        const view = mountReader('c16', { live: 'watchFromNow' });
        await settle();

        expect(view.total.value).toBe(0); // subscribed too late, told nothing

        // Only an unrelated later write shakes it loose.
        await host.actor(CartActor, 'c16').add('pear');
        await settle();
        expect(view.total.value).toBe(2);
    });
});
