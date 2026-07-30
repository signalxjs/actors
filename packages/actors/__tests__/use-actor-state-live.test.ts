/**
 * `useActorState(…, { live: true })` — the read that updates when SOMEONE ELSE
 * writes.
 *
 * Everything else in the read path only refreshes the tab that wrote:
 * `useActorAction`'s invalidation is local bookkeeping, and no
 * request/response call tells a second browser that anything happened. So the
 * assertions here are about exactly that gap, and about its cost: a push must
 * update the component WITHOUT a new unary read, or the live layer is just a
 * poll with extra steps.
 *
 * Mounted against the BUILD-SWAPPED CLIENT REF, so `actor()` takes the
 * transport path into a real endpoint over a real `$live` mount. Against the
 * definition it would dispatch in-process and the fetch assertions would pass
 * vacuously.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { component, defineApp, signal, type App } from 'sigx';
import { createSSR } from '@sigx/server-renderer';
import { defineActor, type Silo } from '@sigx/actors';
import { createSilo } from '@sigx/actors/silo';
import { handleActorRequest } from '@sigx/actors/server';
import { actorsPlugin, useActorState, useActorsContext, type LiveChannel } from '@sigx/actors/app';
import { __actorRef, configureActors, fetchTransport } from '@sigx/actors/client';
import type { AsyncState } from 'sigx';

const ENDPOINT = 'http://actors.test/_sigx/actor';
const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };
/** Small enough to test, large enough to still coalesce a mount burst. */
const fast = { debounceMs: 2, retryMs: 5, maxRetryMs: 20 };

const RoomActor = defineActor({
    type: 'Room',
    unguarded: true,
    state: () => ({ messages: [] as string[] }),
    methods: (ctx) => ({
        async count() {
            return ctx.state.messages.length;
        },
        /** A read whose legitimate answer is `null`. */
        async nothing(): Promise<null> {
            void ctx;
            return null;
        },
        async post(text: string) {
            ctx.state.messages.push(text);
            await ctx.save();
            return ctx.state.messages.length;
        }
    })
});

const RoomRef = __actorRef('Room', ENDPOINT) as unknown as typeof RoomActor;

const silos: Silo[] = [];
const mounted: App<unknown>[] = [];
/** Every wire symbol the client asked for, in order. */
let wire: string[] = [];

async function startSilo(): Promise<Silo> {
    const silo = createSilo({ actors: [RoomActor], defaults: quiet });
    await silo.start();
    silos.push(silo);
    configureActors(null);
    return silo;
}

/**
 * A transport whose fetch goes straight into the real endpoint, recording what
 * crossed it. The abort link matters: without it, closing a live connection
 * would leave the server-side watch (and its keep-alive) open.
 */
function transportFor(silo: Silo): Parameters<typeof actorsPlugin>[0] {
    return {
        transport: {
            endpoint: ENDPOINT,
            fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
                const request = new Request(input, init);
                wire.push(decodeURIComponent(new URL(request.url).pathname.split('/').pop()!));
                const response = await handleActorRequest(request, {
                    silo,
                    origin: false,
                    // A ping would be invisible to the consumer anyway; off so
                    // the frames in this file are only the ones under test.
                    streamPingMs: 0
                });
                if (!response.body || !init?.signal) return response;
                const reader = response.body.getReader();
                init.signal.addEventListener('abort', () => void reader.cancel().catch(() => {}));
                return new Response(
                    new ReadableStream<Uint8Array>({
                        async pull(controller) {
                            const { value, done } = await reader.read();
                            if (done) controller.close();
                            else controller.enqueue(value);
                        },
                        cancel: (reason) => reader.cancel(reason)
                    }),
                    { status: response.status, headers: response.headers }
                );
            }
        },
        live: fast
    };
}

/** Mount a setup function as an app with the actors plugin installed. */
function mount<T>(silo: Silo, setup: () => T, options?: Parameters<typeof actorsPlugin>[0]): T {
    let captured!: T;
    const Root = component(() => {
        captured = setup();
        return () => null;
    });
    const host = document.createElement('div');
    document.body.appendChild(host);
    const app = defineApp(Root({})).use(
        actorsPlugin(options ?? transportFor(silo))
    ) as App<unknown>;
    mounted.push(app);
    app.mount(host as never);
    return captured;
}

const reads = (): number => wire.filter((symbol) => symbol === 'Room#count').length;

afterEach(async () => {
    for (const app of mounted.splice(0)) app.unmount();
    for (const silo of silos.splice(0)) await silo.stop({ timeoutMs: 1000 });
    document.body.innerHTML = '';
    configureActors(null);
    wire = [];
});

describe('useActorState({ live: true })', () => {
    it("shows another client's write, with no second read", async () => {
        const silo = await startSilo();
        const state = mount(silo, () =>
            useActorState(RoomRef, 'r1', 'count', { live: true })
        ) as AsyncState<number>;

        await vi.waitFor(() => expect(state.value).toBe(0), { timeout: 2000 });
        expect(reads()).toBe(1); // the ordinary read that seeds the cell
        expect(wire).toContain('$live#subscribe');

        // Somebody else entirely — no action, no invalidation, no refresh.
        await silo.actor(RoomActor, 'r1').post('hello');

        await vi.waitFor(() => expect(state.value).toBe(1), { timeout: 2000 });
        expect(state.state).toBe('ready');
        // THE point: the value arrived on the open connection. A live layer
        // that re-read on every push would be a poll with extra steps.
        expect(reads()).toBe(1);
    });

    it('leaves the read working when the live feed cannot be established', async () => {
        // A silo whose placement cannot watch answers 501 for the whole
        // subscribe. The read itself is fine, and must stay fine: "not live"
        // is a degradation, not a failure.
        const silo = await startSilo();
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const original = silo.dispatchWatch;
        Reflect.deleteProperty(silo as unknown as Record<string, unknown>, 'dispatchWatch');
        (silo as unknown as Record<string, unknown>).dispatchWatch = undefined;

        const state = mount(silo, () =>
            useActorState(RoomRef, 'r2', 'count', { live: true })
        ) as AsyncState<number>;

        await vi.waitFor(() => expect(state.value).toBe(0), { timeout: 2000 });
        // The subscribe attempt failed (a connection-level 501, not a frame),
        // and the read is still ready with its value.
        await new Promise((r) => setTimeout(r, 50));
        expect(state.state).toBe('ready');
        expect(state.error).toBeNull();

        (silo as unknown as Record<string, unknown>).dispatchWatch = original;
        warn.mockRestore();
    });

    it('a read whose value IS null stays ready when its subscription fails', async () => {
        // `null` is a legitimate result, so it cannot double as "nothing to
        // show": treating it that way would let a failed subscription blank out
        // a read that is perfectly ready. The state machine answers that
        // question, never the value.
        //
        // Driven through a transport's OWN `live()` — the same seam a WebSocket
        // transport would use — because a per-subscription failure has to be
        // delivered on demand, which no real feed will do to order.
        const silo = await startSilo();
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        let fail: ((error: Error) => void) | undefined;
        const base = transportFor(silo)!.transport as Parameters<typeof fetchTransport>[0];

        const state = mount(
            silo,
            () => useActorState(RoomRef, 'r6', 'nothing', { live: true }) as AsyncState<null>,
            {
                transport: {
                    ...fetchTransport(base),
                    name: 'fake-live',
                    live: () => ({
                        subscribe: (_sub, _onValue, onError) => {
                            fail = onError;
                            return () => {};
                        }
                    })
                },
                live: fast
            }
        );

        await vi.waitFor(() => expect(state.state).toBe('ready'), { timeout: 2000 });
        expect(state.value).toBeNull();

        await vi.waitFor(() => expect(fail).toBeTypeOf('function'), { timeout: 2000 });
        fail!(new Error('watch rejected'));

        // Still ready, still null: "not live" is not "broken".
        expect(state.state).toBe('ready');
        expect(state.error).toBeNull();
        expect(warn.mock.calls.flat().join(' ')).toMatch(/live subscription for Room\/r6#nothing/);
        warn.mockRestore();
    });

    it('follows a reactive key: the old subscription goes, the new one seeds', async () => {
        const silo = await startSilo();
        await silo.actor(RoomActor, 'b').post('x');

        const selected = signal({ id: 'a' });
        let channel!: LiveChannel;
        const state = mount(silo, () => {
            channel = useActorsContext().live;
            return useActorState(RoomRef, () => [selected.id, 'count'] as const, { live: true });
        }) as AsyncState<number>;

        await vi.waitFor(() => expect(state.value).toBe(0), { timeout: 2000 });
        await vi.waitFor(() => expect(channel.stats().subscriptions).toBe(1), { timeout: 2000 });

        selected.id = 'b';
        await vi.waitFor(() => expect(state.value).toBe(1), { timeout: 2000 });
        // Exactly one: the subscription moved with the key rather than
        // accumulating one per key the component has ever shown.
        expect(channel.stats().subscriptions).toBe(1);

        // …and it is watching 'b' now, so 'b' pushes reach the component.
        await silo.actor(RoomActor, 'b').post('y');
        await vi.waitFor(() => expect(state.value).toBe(2), { timeout: 2000 });
    });

    it('unsubscribes on unmount, releasing the watch on the server', async () => {
        const silo = await startSilo();
        let channel!: LiveChannel;
        mount(silo, () => {
            channel = useActorsContext().live;
            return useActorState(RoomRef, 'r3', 'count', { live: true });
        });

        await vi.waitFor(() => expect(channel.stats().subscriptions).toBe(1), { timeout: 2000 });
        for (const app of mounted.splice(0)) app.unmount();

        expect(channel.stats().subscriptions).toBe(0);
        // The keep-alive an open watch holds is released, so the grain becomes
        // collectable again — a page of closed tabs must not pin a silo.
        await vi.waitFor(
            () => expect(silo.activations().every((a) => !a.keptAlive)).toBe(true),
            { timeout: 2000 }
        );
    });

    it('a refreshed cell wins over an older push', async () => {
        // The ordering that matters for the writer's own tab: `useActorAction`
        // invalidates on success, the cell refetches the post-write truth, and
        // the push for that same mutation is still behind the watch throttle.
        // Preferring the push there would show the writer their OLD value.
        const silo = await startSilo();
        const state = mount(silo, () =>
            useActorState(RoomRef, 'r4', 'count', { live: true })
        ) as AsyncState<number>;

        await vi.waitFor(() => expect(state.value).toBe(0), { timeout: 2000 });
        await vi.waitFor(() => expect(wire).toContain('$live#subscribe'), { timeout: 2000 });

        await silo.actor(RoomActor, 'r4').post('one');
        await vi.waitFor(() => expect(state.value).toBe(1), { timeout: 2000 });

        // A fresh read lands a newer value than the push we are holding.
        await silo.actor(RoomActor, 'r4').post('two');
        await state.refresh();
        expect(state.value).toBe(2);
    });

    it('a remount restores the pushed value, not the last fetched one', async () => {
        const silo = await startSilo();
        const state = mount(silo, () =>
            useActorState(RoomRef, 'r5', 'count', { live: true })
        ) as AsyncState<number>;
        await vi.waitFor(() => expect(state.value).toBe(0), { timeout: 2000 });
        await vi.waitFor(() => expect(wire).toContain('$live#subscribe'), { timeout: 2000 });

        await silo.actor(RoomActor, 'r5').post('one');
        await vi.waitFor(() => expect(state.value).toBe(1), { timeout: 2000 });
        const before = reads();

        // A route change and back: the cell restores from the page blob
        // without refetching, so the blob had better hold the pushed value —
        // otherwise the remount paints the pre-push number.
        for (const app of mounted.splice(0)) app.unmount();
        const again = mount(silo, () =>
            useActorState(RoomRef, 'r5', 'count', { live: true })
        ) as AsyncState<number>;

        expect(again.value).toBe(1);
        expect(reads()).toBe(before);
    });

    it('does not mistake a stray object argument for the option bag', async () => {
        // The trailing-options position is only unambiguous for TYPED callers.
        // A JavaScript caller passing a Date (or any non-plain object, or a
        // plain one with unknown keys) must still reach `actorKey`'s argument
        // check and be told, rather than have it swallowed as options and
        // silently address a different key.
        const silo = await startSilo();
        const bad = (value: unknown): void => {
            mount(silo, () =>
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                useActorState(RoomRef, 'r7', 'count', value as any)
            );
        };

        expect(() => bad(new Date())).toThrow(/must be JSON primitives/);
        expect(() => bad({ nope: 1 })).toThrow(/must be JSON primitives/);
        expect(() => bad(new Map())).toThrow(/must be JSON primitives/);
        // …while the real thing, including the empty bag that type-checks, is
        // taken as options.
        expect(() => bad({})).not.toThrow();
        expect(() => bad({ live: true, server: false })).not.toThrow();
    });

    it('never subscribes during a server render', async () => {
        // Rendered through the SAME plugin and transport a client uses, so the
        // only thing keeping the connection shut is the real mechanism: the
        // subscription lives in `onMounted`, which a server render never runs.
        // Move it into setup and this test fails.
        const silo = await startSilo();
        let channel!: LiveChannel;
        const Live = component(() => {
            channel = useActorsContext().live;
            const state = useActorState(RoomActor, 'ssr', 'count', { live: true });
            return () => state.match({ ready: (n) => `count: ${n}`, pending: () => 'loading' });
        });

        const app = defineApp(Live({})).use(actorsPlugin(transportFor(silo))) as App<unknown>;
        const html = await createSSR().renderDocument(app, {
            template: '<html><body><!--ssr-outlet--></body></html>'
        });

        // The read still resolves during the render — `live` is additive.
        expect(html).toContain('count: 0');
        expect(channel.stats().subscriptions).toBe(0);
        expect(channel.stats().opens).toBe(0);
        expect(wire).not.toContain('$live#subscribe');
    });
});
