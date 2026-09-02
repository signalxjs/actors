/**
 * The transport seam: the client Proxy must never speak HTTP itself, so a
 * substitute transport can batch, authenticate differently, or use a
 * protocol other than fetch without any call site changing.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
    ACTOR_ROUTE_HEADER,
    __actorRef,
    configureActors,
    fetchTransport,
    hashRouteToken
} from '@sigx/actors/client';
import type { ActorCallInit, ActorTransport } from '@sigx/actors/client';

const ENDPOINT = 'http://actors.test/_sigx/actor';

interface Recorded {
    symbol: string;
    args: unknown[];
    init: ActorCallInit | undefined;
}

function recordingTransport(): ActorTransport & { calls: Recorded[]; streams: Recorded[] } {
    const calls: Recorded[] = [];
    const streams: Recorded[] = [];
    return {
        name: 'recording',
        calls,
        streams,
        async call(symbol, args, init) {
            calls.push({ symbol, args, init });
            return { ok: symbol };
        },
        stream(symbol, args, init) {
            streams.push({ symbol, args, init });
            return (async function* () {
                yield 1;
                yield 2;
            })();
        }
    };
}

/** A ref shaped like the build transform's output. */
function ref(): {
    __sigxActorProxy: (
        key: string,
        options?: ActorCallInit
    ) => {
        add(item: string): Promise<unknown>;
        watch(): AsyncIterable<unknown>;
        with(o?: ActorCallInit): { add(item: string): Promise<unknown> };
    };
} {
    return __actorRef('Cart', ENDPOINT, ['watch']) as never;
}

afterEach(() => {
    configureActors(null);
    vi.unstubAllGlobals();
});

describe('the transport seam', () => {
    it('routes unary calls through a substitute transport, never fetch', async () => {
        const fetchSpy = vi.fn(() => Promise.reject(new Error('fetch must not be called')));
        vi.stubGlobal('fetch', fetchSpy);
        const transport = recordingTransport();
        configureActors(transport);

        const cart = ref().__sigxActorProxy('c1');
        await expect(cart.add('apple')).resolves.toEqual({ ok: 'Cart#add' });

        expect(fetchSpy).not.toHaveBeenCalled();
        expect(transport.calls).toHaveLength(1);
        expect(transport.calls[0].symbol).toBe('Cart#add');
        // The actor key is still spliced in as the first wire argument.
        expect(transport.calls[0].args).toEqual(['c1', 'apple']);
    });

    it('routes stream methods through the transport too', async () => {
        const fetchSpy = vi.fn(() => Promise.reject(new Error('fetch must not be called')));
        vi.stubGlobal('fetch', fetchSpy);
        const transport = recordingTransport();
        configureActors(transport);

        const out: unknown[] = [];
        for await (const chunk of ref().__sigxActorProxy('c2').watch()) out.push(chunk);

        expect(out).toEqual([1, 2]);
        expect(fetchSpy).not.toHaveBeenCalled();
        expect(transport.streams).toHaveLength(1);
        expect(transport.streams[0].symbol).toBe('Cart#watch');
        expect(transport.streams[0].args).toEqual(['c2']);
    });

    it('hands the transport the build-baked endpoint, and per-call headers/signal', async () => {
        const transport = recordingTransport();
        configureActors(transport);
        const controller = new AbortController();

        await ref()
            .__sigxActorProxy('c3')
            .with({ headers: { authorization: 'Bearer t' }, signal: controller.signal })
            .add('pear');

        const init = transport.calls[0].init;
        expect(init?.endpoint).toBe(ENDPOINT);
        expect(init?.headers).toEqual({ authorization: 'Bearer t' });
        expect(init?.signal).toBe(controller.signal);
    });

    it('threads oneWay to the transport, and streams refuse it at the proxy', async () => {
        const transport = recordingTransport();
        configureActors(transport);

        const proxy = ref().__sigxActorProxy('c9', { oneWay: true } as ActorCallInit);
        await proxy.add('kiwi');
        expect(transport.calls[0].init?.oneWay).toBe(true);

        expect(() => proxy.watch()).toThrow(/streams cannot be one-way/);
        expect(transport.streams).toHaveLength(0);
    });

    it('an options object is still sugar for the default fetch transport', async () => {
        const fetchSpy = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => new Response(JSON.stringify({ data: 7 })));
        configureActors({ endpoint: 'http://other.test/actors', fetch: fetchSpy });

        await expect(ref().__sigxActorProxy('c4').add('plum')).resolves.toBe(7);

        expect(fetchSpy).toHaveBeenCalledTimes(1);
        // The configured endpoint wins over the one baked into the ref; the
        // routing token rides as a MIDDLE segment, leaving the symbol last.
        expect(String(fetchSpy.mock.calls[0][0])).toBe(
            `http://other.test/actors/r/${hashRouteToken('Cart', 'c4')}/Cart/add`
        );
    });

    it('a config without an endpoint keeps using the ref\'s baked one', async () => {
        const fetchSpy = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => new Response(JSON.stringify({ data: 0 })));
        configureActors({ headers: { 'x-tenant': 'acme' }, fetch: fetchSpy });

        await ref().__sigxActorProxy('c5').add('fig');

        expect(String(fetchSpy.mock.calls[0][0])).toBe(
            `${ENDPOINT}/r/${hashRouteToken('Cart', 'c5')}/Cart/add`
        );
        const init = fetchSpy.mock.calls[0][1] as RequestInit;
        expect((init.headers as Record<string, string>)['x-tenant']).toBe('acme');
    });

    it('route:\'none\' restores the bare URL, and emits no header', async () => {
        const fetchSpy = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => new Response(JSON.stringify({ data: 0 })));
        configureActors({ route: 'none', fetch: fetchSpy });

        await ref().__sigxActorProxy('c6').add('fig');

        expect(String(fetchSpy.mock.calls[0][0])).toBe(`${ENDPOINT}/Cart/add`);
        const init = fetchSpy.mock.calls[0][1] as RequestInit;
        expect((init.headers as Record<string, string>)[ACTOR_ROUTE_HEADER]).toBeUndefined();
    });

    it('a defineWorker ref mints no routing token, so the edge keeps spreading it', async () => {
        // A worker has no owner to route to: it is always placed on whichever
        // host the call lands on. A token would pin every call for one worker
        // key to one pod and hand back the fleet-wide spread that is the
        // pool's whole point (#148). An actor ref beside it still gets one.
        const fetchSpy = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => new Response(JSON.stringify({ data: 0 })));
        configureActors({ fetch: fetchSpy });
        const Worker = __actorRef('Resize', ENDPOINT, ['chunks'], [], true) as never as ReturnType<typeof ref>;

        await Worker.__sigxActorProxy('w1').add('fig');
        await ref().__sigxActorProxy('w1').add('fig');

        expect(String(fetchSpy.mock.calls[0][0])).toBe(`${ENDPOINT}/Resize/add`);
        const init = fetchSpy.mock.calls[0][1] as RequestInit;
        expect((init.headers as Record<string, string>)[ACTOR_ROUTE_HEADER]).toBeUndefined();
        expect(String(fetchSpy.mock.calls[1][0])).toBe(
            `${ENDPOINT}/r/${hashRouteToken('Cart', 'w1')}/Cart/add`
        );
    });

    it('a worker stream carries no token either, and a substitute transport sees the flag', async () => {
        const fetchSpy = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) =>
            new Response('{"done":true}\n', { headers: { 'content-type': 'application/x-ndjson' } })
        );
        configureActors({ route: 'key', fetch: fetchSpy });
        const Worker = __actorRef('Resize', ENDPOINT, ['watch'], [], true) as never as ReturnType<typeof ref>;
        for await (const _ of Worker.__sigxActorProxy('w2').watch()) {
            // drain
        }
        // Even an explicit `route: 'key'` does not tokenize a worker — the
        // mode chooses the token's SHAPE; a worker has nothing to shape.
        expect(String(fetchSpy.mock.calls[0][0])).toBe(`${ENDPOINT}/Resize/watch`);

        const transport = recordingTransport();
        configureActors(transport);
        await Worker.__sigxActorProxy('w3').add('x');
        await ref().__sigxActorProxy('w3').add('x');
        expect(transport.calls[0]!.init?.worker).toBe(true);
        expect(transport.calls[1]!.init?.worker).toBeUndefined();
    });

    it('a caller cannot flag a stateful actor as a worker through .with()', async () => {
        // The worker flag is the build's fact about the TYPE, not a per-call
        // option: `.with({ worker: true })` on a `defineActor` ref must not
        // silently drop its routing token, exactly as `.with({ ref })` cannot
        // re-route a proxy and `.with({ get: true })` cannot reach a write.
        const fetchSpy = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => new Response(JSON.stringify({ data: 0 })));
        configureActors({ fetch: fetchSpy });

        await ref().__sigxActorProxy('c9').with({ worker: true }).add('fig');

        expect(String(fetchSpy.mock.calls[0][0])).toBe(
            `${ENDPOINT}/r/${hashRouteToken('Cart', 'c9')}/Cart/add`
        );
        const init = fetchSpy.mock.calls[0][1] as RequestInit;
        expect((init.headers as Record<string, string>)[ACTOR_ROUTE_HEADER]).toBe(hashRouteToken('Cart', 'c9'));

        const transport = recordingTransport();
        configureActors(transport);
        await ref().__sigxActorProxy('c9').with({ worker: true }).add('fig');
        expect(transport.calls[0]!.init?.worker).toBeUndefined();
    });

    it('clearing the transport falls back to the default', async () => {
        configureActors(recordingTransport());
        configureActors(null);
        const fetchSpy = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => new Response(JSON.stringify({ data: 'default' })));
        vi.stubGlobal('fetch', fetchSpy);

        await expect(ref().__sigxActorProxy('c6').add('date')).resolves.toBe('default');
        expect(fetchSpy).toHaveBeenCalledTimes(1);
    });

    it('fetchTransport is usable directly as a transport value', async () => {
        const fetchSpy = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => new Response(JSON.stringify({ data: 1 })));
        configureActors(fetchTransport({ endpoint: ENDPOINT, fetch: fetchSpy }));

        await expect(ref().__sigxActorProxy('c7').add('kiwi')).resolves.toBe(1);
        expect(fetchSpy).toHaveBeenCalledTimes(1);
    });
});
