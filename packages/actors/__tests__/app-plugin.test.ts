/**
 * `actorsPlugin()` — the SSR-safety posture above all. A server app installs
 * per REQUEST, and `configureActors()` is page-global, so installing a
 * transport there would let one request's config bleed into another's
 * concurrent render.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { actorsPlugin, ACTORS_TOKEN, useActorsContext } from '@sigx/actors/app';
import { __actorRef, configureActors } from '@sigx/actors/client';
import type { ActorTransport } from '@sigx/actors/client';
import { getProvided } from 'sigx/internals';

function stubTransport(name = 'stub'): ActorTransport {
    return {
        name,
        async call() {
            return name;
        },
        stream() {
            return (async function* () {})();
        }
    };
}

/** The minimum of `App` the plugin touches. */
function fakeApp(): { _context: { provides: Map<symbol, unknown>; disposables: Set<() => void> } } {
    return { _context: { provides: new Map(), disposables: new Set() } };
}

function install(
    app: ReturnType<typeof fakeApp>,
    options?: Parameters<typeof actorsPlugin>[0]
): void {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    actorsPlugin(options).install(app as any);
}

function cart(): { add(): Promise<unknown> } {
    const ref = __actorRef('Cart', 'http://actors.test/_sigx/actor') as {
        __sigxActorProxy: (key: string) => { add(): Promise<unknown> };
    };
    return ref.__sigxActorProxy('k');
}

const globals = globalThis as { __SIGX_LIVE_CLIENT__?: boolean };

beforeEach(() => {
    globals.__SIGX_LIVE_CLIENT__ = true; // pretend a live client by default
});

afterEach(() => {
    delete globals.__SIGX_LIVE_CLIENT__;
    configureActors(null);
    vi.restoreAllMocks();
});

describe('actorsPlugin', () => {
    it('installs the transport on a live client, and unmount clears it', async () => {
        const app = fakeApp();
        install(app, { transport: stubTransport('installed') });

        await expect(cart().add()).resolves.toBe('installed');

        for (const dispose of app._context.disposables) dispose();
        // Back to the default fetch transport.
        const fetchSpy = vi.fn(
            async (_u: RequestInfo | URL, _i?: RequestInit) =>
                new Response(JSON.stringify({ data: 'default' }))
        );
        vi.stubGlobal('fetch', fetchSpy);
        await expect(cart().add()).resolves.toBe('default');
        vi.unstubAllGlobals();
    });

    it('does NOT install a transport on a server app — the per-request bleed regression', async () => {
        // The suite runs under happy-dom, so `document` exists; a server
        // render has neither it nor core's live-client declaration.
        delete globals.__SIGX_LIVE_CLIENT__;
        vi.stubGlobal('document', undefined);
        const app = fakeApp();
        install(app, { transport: stubTransport('server') });

        const fetchSpy = vi.fn(
            async (_u: RequestInfo | URL, _i?: RequestInit) =>
                new Response(JSON.stringify({ data: 'untouched' }))
        );
        vi.stubGlobal('fetch', fetchSpy);
        // The page-global seam was never written to.
        await expect(cart().add()).resolves.toBe('untouched');
        expect(app._context.disposables.size).toBe(0);
        vi.unstubAllGlobals();

        // …but the context is still provided, so the hooks resolve during SSR.
        const context = getProvided(app._context.provides, ACTORS_TOKEN);
        expect(context).toBeDefined();
        expect(context?.transport).toBeNull();
    });

    it('warns when a second app overwrites a live transport, and last install wins', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const first = fakeApp();
        const second = fakeApp();
        install(first, { transport: stubTransport('first') });
        install(second, { transport: stubTransport('second') });

        await expect(cart().add()).resolves.toBe('second');
        expect(warn.mock.calls.flat().join(' ')).toMatch(/last install wins/i);
    });

    it('the first app\'s disposal does not strip the second app\'s transport', async () => {
        vi.spyOn(console, 'warn').mockImplementation(() => {});
        const first = fakeApp();
        const second = fakeApp();
        install(first, { transport: stubTransport('first') });
        install(second, { transport: stubTransport('second') });

        for (const dispose of first._context.disposables) dispose();

        // Still the second app's — disposal is a no-op when it isn't ours.
        await expect(cart().add()).resolves.toBe('second');
    });

    it('provides a context carrying the installed transport', () => {
        const app = fakeApp();
        const transport = stubTransport('provided');
        install(app, { transport });
        expect(getProvided(app._context.provides, ACTORS_TOKEN)?.transport).toBe(transport);
    });

    it('installs nothing when no transport is given, but still provides a context', () => {
        const app = fakeApp();
        install(app);
        const context = getProvided(app._context.provides, ACTORS_TOKEN);
        expect(context).toEqual({ transport: null });
        expect(app._context.disposables.size).toBe(0);
    });

    it('useActorsContext() outside any app returns a working default and warns once', () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const first = useActorsContext();
        const second = useActorsContext();
        expect(first.transport).not.toBeNull(); // the page-global transport
        expect(second).toBeDefined();
        const warnings = warn.mock.calls.filter((c) =>
            String(c[0]).includes('no actorsPlugin()')
        );
        expect(warnings).toHaveLength(1);
    });
});
