/**
 * The branches of `workerSocket` that real workerd cannot exercise: route
 * matching in isolation, and the module refusing gracefully outside the
 * Workers runtime — the reason its Cloudflare types are structural.
 *
 * Everything that needs a real socket runs in `workers/socket.test.ts`.
 */
import { describe, expect, it } from 'vitest';
import type { ActorRoute, PluginRegistry } from '@sigx/actors/host';
import type { Host } from '@sigx/actors';
import { DEFAULT_SOCKET_PATH, workerSocket } from '../src/socket';

function routeOf(options?: Parameters<typeof workerSocket>[0]): ActorRoute {
    let captured: ActorRoute | undefined;
    workerSocket(options).setup({
        route(route: ActorRoute) {
            captured = route;
        }
    } as unknown as PluginRegistry);
    if (!captured) throw new Error('workerSocket registered no route');
    return captured;
}

/**
 * `Upgrade` is a forbidden header name, so Node's `Request` constructor
 * silently STRIPS it (workerd special-cases it for `fetch` upgrades). A bare
 * `Headers` has no guard, so a structural request carries it fine — and the
 * route reads nothing but `url` and `headers`.
 */
function upgrade(url: string, value = 'websocket'): Request {
    return { url, headers: new Headers({ upgrade: value }) } as unknown as Request;
}

describe('workerSocket route matching', () => {
    it('registers a route via the plugin registry', () => {
        expect(routeOf().name).toBe('cloudflare:worker-socket');
    });

    it('matches only a websocket upgrade on the EXACT path', () => {
        const route = routeOf();
        expect(route.match(upgrade(`https://x.test${DEFAULT_SOCKET_PATH}`))).toBe(true);
        // Query is not part of the pathname.
        expect(route.match(upgrade(`https://x.test${DEFAULT_SOCKET_PATH}?a=1`))).toBe(true);
        // No Upgrade header → someone else's request.
        expect(route.match(new Request(`https://x.test${DEFAULT_SOCKET_PATH}`))).toBe(false);
        // Prefix must never adopt a neighbouring endpoint's upgrades.
        expect(route.match(upgrade(`https://x.test${DEFAULT_SOCKET_PATH}extra`))).toBe(false);
        expect(route.match(upgrade(`https://x.test${DEFAULT_SOCKET_PATH}/sub`))).toBe(false);
        expect(route.match(upgrade('https://x.test/other'))).toBe(false);
    });

    it('honours a custom path', () => {
        const route = routeOf({ path: '/ws' });
        expect(route.match(upgrade('https://x.test/ws'))).toBe(true);
        expect(route.match(upgrade(`https://x.test${DEFAULT_SOCKET_PATH}`))).toBe(false);
    });

    it('is case-insensitive on the Upgrade value', () => {
        const route = routeOf();
        expect(route.match(upgrade(`https://x.test${DEFAULT_SOCKET_PATH}`, 'WebSocket'))).toBe(
            true
        );
    });
});

describe('workerSocket outside workerd', () => {
    it('answers 500 with a named reason when WebSocketPair is absent', async () => {
        // Loadable-anywhere is the contract; failing with a clear pointer to
        // the Node adapter is the graceful half of it. The host is never
        // touched on this path.
        const route = routeOf();
        const res = await route.handle(
            upgrade(`https://x.test${DEFAULT_SOCKET_PATH}`),
            null as unknown as Host
        );
        expect(res.status).toBe(500);
        const body = (await res.json()) as { error: { message: string } };
        expect(body.error.message).toContain('WebSocketPair');
        expect(body.error.message).toContain('attachActorSocket');
    });
});
