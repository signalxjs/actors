/**
 * The payoff of building on `useData`: an actor read resolves DURING the
 * server render, serializes into the page under its canonical actor key, and
 * the browser restores it without a single request.
 *
 * This is the test that proves the claim, and it is deliberately end-to-end:
 * a real in-process host, a real document render, then a client mount
 * against the emitted payload with `fetch` spied to catch a refetch.
 *
 * The client half mounts the BUILD-SWAPPED CLIENT REF, not the definition.
 * Against the definition `actor()` takes the server path and dispatches
 * in-process, so `fetch` would never be called whether or not the payload
 * was restored — the assertion would pass vacuously. A negative control
 * pins that too: the same mount, unseeded, must really call `fetch`.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { component, defineApp, type App } from 'sigx';
import { createSSR } from '@sigx/server-renderer';
import { actorKey, defineActor, type Host } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';
import { __actorRef, configureActors } from '@sigx/actors/client';
import { useActorState } from '@sigx/actors/app';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

let dispatches = 0;

const CartActor = defineActor({
    type: 'Cart',
    allowAnonymous: true,
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async total() {
            dispatches++;
            return ctx.state.items.length;
        }
    })
});

const CANONICAL = JSON.stringify(actorKey(CartActor, 'seeded', 'total'));

/**
 * What the build ships to the browser for `CartActor`. Typed as the
 * definition because the swap replaces values, never types — and it keys
 * identically, which is what makes the seeded payload findable from the
 * client side.
 */
const CartRef = __actorRef(
    'Cart',
    'http://actors.test/_sigx/actor'
) as unknown as typeof CartActor;

const hosts: Host[] = [];
const mounted: App<unknown>[] = [];

afterEach(async () => {
    for (const app of mounted.splice(0)) app.unmount();
    for (const host of hosts.splice(0)) await host.stop();
    delete (globalThis as { __SIGX_ASYNC__?: unknown }).__SIGX_ASYNC__;
    document.body.innerHTML = '';
    dispatches = 0;
    configureActors(null);
    vi.unstubAllGlobals();
});

const Cart = component(() => {
    const state = useActorState(CartActor, 'seeded', 'total');
    return () => state.match({ ready: (n) => `items: ${n}`, pending: () => 'loading' });
});

describe('SSR seeding', () => {
    it('resolves the read during the render and serializes it under the actor key', async () => {
        const host = createHost({ actors: [CartActor], defaults: quiet });
        await host.start();
        hosts.push(host);
        await host.actor(CartActor, 'seeded').total(); // activate
        await host.actor(CartActor, 'seeded').total();
        dispatches = 0;

        const html = await createSSR().renderDocument(Cart({}), {
            template: '<html><body><!--ssr-outlet--></body></html>'
        });

        // The read really ran on the server — the rendered markup has the value.
        expect(html).toContain('items: 0');
        // …and the page payload is keyed by the canonical actor key, byte for
        // byte. The emitted script reads:
        //   window.__SIGX_ASYNC__=Object.assign(…,{"[\"@actor\",…,\"total\"]":0})
        // so the key appears JSON-escaped inside a JSON string — which is
        // exactly `JSON.stringify(CANONICAL)`.
        expect(html).toContain('window.__SIGX_ASYNC__=');
        expect(html).toContain(`${JSON.stringify(CANONICAL)}:0`);
        expect(dispatches).toBe(1);
    });

    it('explains a missing request scope rather than blaming the hook', async () => {
        // A guard that reads the request, rendered with no scope open — the
        // failure a developer is most likely to misdiagnose.
        const Guarded = defineActor({
            type: 'Guarded',
            // Reads the request, then allows — the point is that an
            // in-process SSR call reaches a live context at all.
            authorize: [(_p, rq) => (rq.request ? true : true)],
            state: () => ({}),
            methods: () => ({
                async read() {
                    return 1;
                }
            })
        });
        const host = createHost({ actors: [Guarded], defaults: quiet });
        await host.start();
        hosts.push(host);

        const errors: unknown[] = [];
        const Boom = component(() => {
            const state = useActorState(Guarded, 'g1', 'read');
            return () => state.match({ ready: () => 'ok', error: (e) => void errors.push(e) });
        });
        await createSSR().renderDocument(Boom({}), {
            template: '<html><body><!--ssr-outlet--></body></html>'
        });

        const message = String((errors[0] as Error | undefined)?.message ?? '');
        expect(message).toMatch(/nothing supplied a request/); // core's own text…
        expect(message).toMatch(/DURING A SERVER RENDER/); // …plus the remedy
        expect(message).toMatch(/@sigx\/server/);
    });

    /**
     * Mount as a REAL browser would: against the build-swapped client ref,
     * so `actor()` takes the transport path. Mounting the definition instead
     * would make the `fetch` assertion vacuous — the server path dispatches
     * in-process and never calls `fetch` whether or not the payload was
     * restored.
     */
    function mountBrowser(seed: Record<string, unknown> | null): {
        state: ReturnType<typeof useActorState>;
        fetchSpy: ReturnType<typeof vi.fn>;
    } {
        if (seed) {
            (globalThis as { __SIGX_ASYNC__?: Record<string, unknown> }).__SIGX_ASYNC__ =
                Object.assign(Object.create(null) as Record<string, unknown>, seed);
        }
        const fetchSpy = vi.fn(
            async (_u: RequestInfo | URL, _i?: RequestInit) =>
                new Response(JSON.stringify({ data: 99 }))
        );
        vi.stubGlobal('fetch', fetchSpy);

        let state!: ReturnType<typeof useActorState>;
        const Root = component(() => {
            state = useActorState(CartRef, 'seeded', 'total');
            return () => null;
        });
        const host = document.createElement('div');
        document.body.appendChild(host);
        const app = defineApp(Root({})) as App<unknown>;
        mounted.push(app);
        app.mount(host as never);
        return { state, fetchSpy };
    }

    it('a client mount over the emitted payload does NOT refetch', async () => {
        dispatches = 0;
        const { state, fetchSpy } = mountBrowser({ [CANONICAL]: 7 });
        await new Promise((r) => setTimeout(r, 10));

        expect(state.value).toBe(7); // the SERVER's value, restored
        expect(state.state).toBe('ready');
        expect(fetchSpy).not.toHaveBeenCalled(); // no request…
        expect(dispatches).toBe(0); // …and no in-process dispatch either
    });

    it('…but WOULD fetch without the payload — the negative control', async () => {
        // Without this the assertion above proves nothing: it must be shown
        // that this mount really does reach the transport when unseeded.
        const { state, fetchSpy } = mountBrowser(null);
        await new Promise((r) => setTimeout(r, 10));

        expect(fetchSpy).toHaveBeenCalledTimes(1);
        expect(String(fetchSpy.mock.calls[0][0])).toContain('Cart/total');
        expect(state.value).toBe(99); // the transport's answer, not the server's
    });
});
