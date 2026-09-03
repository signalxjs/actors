/**
 * The branches of `workerSocket` that real workerd cannot exercise: route
 * matching in isolation, and the module refusing gracefully outside the
 * Workers runtime — the reason its Cloudflare types are structural.
 *
 * Everything that needs a real socket runs in `workers/socket.test.ts`.
 */
import { describe, expect, it } from 'vitest';
import type { ActorRoute, PluginRegistry } from '@sigx/actors/host';
import { defineActor, defineWorker, type Host } from '@sigx/actors';
import {
    DEFAULT_SOCKET_PATH,
    objectSocketRoute,
    parseSocketActorPath,
    workerSocket
} from '../src/socket';
import type { DurableObjectStubResolver } from '../src/placement';

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

describe('parseSocketActorPath', () => {
    it('parses exactly two non-empty URI-encoded segments after the prefix', () => {
        expect(parseSocketActorPath('/_sigx/socket/Room/room-1')).toEqual({
            type: 'Room',
            key: 'room-1'
        });
        expect(parseSocketActorPath('/_sigx/socket/acme%2Fgreeter/k%2Fx')).toEqual({
            type: 'acme/greeter',
            key: 'k/x'
        });
    });

    it('answers null for every other shape', () => {
        // The bare path is the WORKER-terminated route's — arity is the
        // disambiguator that lets both modes share the prefix.
        expect(parseSocketActorPath('/_sigx/socket')).toBeNull();
        expect(parseSocketActorPath('/_sigx/socket/')).toBeNull();
        expect(parseSocketActorPath('/_sigx/socket/Room')).toBeNull();
        expect(parseSocketActorPath('/_sigx/socket/Room/')).toBeNull();
        expect(parseSocketActorPath('/_sigx/socket//k')).toBeNull();
        expect(parseSocketActorPath('/_sigx/socket/Room/a/b')).toBeNull();
        expect(parseSocketActorPath('/other/Room/k')).toBeNull();
        // Malformed percent-encoding must not throw out of a route matcher.
        expect(parseSocketActorPath('/_sigx/socket/%zz/k')).toBeNull();
    });

    it('honours a custom prefix', () => {
        expect(parseSocketActorPath('/ws/Room/k', '/ws')).toEqual({ type: 'Room', key: 'k' });
        expect(parseSocketActorPath('/_sigx/socket/Room/k', '/ws')).toBeNull();
    });

    it('normalizes a trailing slash on the configured prefix', () => {
        // `/ws/` must mean the same mount as `/ws` — silently matching
        // nothing is a footgun, not a feature.
        expect(parseSocketActorPath('/ws/Room/k', '/ws/')).toEqual({ type: 'Room', key: 'k' });
        expect(parseSocketActorPath('/ws/Room/k', '/ws//')).toEqual({ type: 'Room', key: 'k' });
    });
});

describe('objectSocketRoute', () => {
    const Stateful = defineActor({
        type: 'Stateful',
        allowAnonymous: true,
        state: () => ({}),
        methods: () => ({})
    });
    const Pool = defineWorker({
        type: 'Pool',
        methods: () => ({})
    });
    /** Off the public wire (#74): the upgrade must answer as if unregistered. */
    const Ledger = defineActor({
        type: 'Ledger',
        internal: true,
        allowAnonymous: true,
        state: () => ({}),
        methods: () => ({})
    });

    const fakeHost = {
        definition: async (type: string) =>
            type === 'Stateful'
                ? Stateful
                : type === 'Pool'
                  ? Pool
                  : type === 'Ledger'
                    ? Ledger
                    : undefined
    } as unknown as Host;

    function resolverTo(fetched: { url: string }[]): DurableObjectStubResolver {
        return {
            name: (ref) => `${ref.type}/${ref.key}`,
            stub: () => ({
                fetch: async (input: string | Request) => {
                    fetched.push({ url: typeof input === 'string' ? input : input.url });
                    return new Response(null, { status: 200 });
                }
            })
        };
    }

    it('matches only an upgrade whose path parses as an actor', () => {
        const route = objectSocketRoute({ resolver: resolverTo([]) });
        expect(route.match(upgrade('https://x.test/_sigx/socket/Stateful/k'))).toBe(true);
        expect(route.match(upgrade('https://x.test/_sigx/socket'))).toBe(false);
        expect(route.match(new Request('https://x.test/_sigx/socket/Stateful/k'))).toBe(false);
    });

    it('forwards the ORIGINAL request to the resolved stub', async () => {
        const fetched: { url: string }[] = [];
        const route = objectSocketRoute({ resolver: resolverTo(fetched) });
        const request = upgrade('https://x.test/_sigx/socket/Stateful/k');
        await route.handle(request, fakeHost);
        // Verbatim — cookies and Origin must ride through to the object.
        expect(fetched).toEqual([{ url: 'https://x.test/_sigx/socket/Stateful/k' }]);
    });

    it('404s an unknown type without touching a stub', async () => {
        const fetched: { url: string }[] = [];
        const route = objectSocketRoute({ resolver: resolverTo(fetched) });
        const res = await route.handle(upgrade('https://x.test/_sigx/socket/Nope/k'), fakeHost);
        expect(res.status).toBe(404);
        expect(fetched).toEqual([]);
    });

    it('404s an internal type exactly like an unknown one, without minting a stub', async () => {
        // The lookup SUCCEEDS for an `internal: true` type, so this is the
        // one public entry point where a naive "!def" check would answer
        // 101 and wake a Durable Object for a type the wire must not serve —
        // and an upgrade that 101s where an unknown type 404s is a probe's
        // answer. Same status, same body, and the resolver is never touched.
        const fetched: { url: string }[] = [];
        const route = objectSocketRoute({ resolver: resolverTo(fetched) });
        const internal = await route.handle(
            upgrade('https://x.test/_sigx/socket/Ledger/k'),
            fakeHost
        );
        const missing = await route.handle(upgrade('https://x.test/_sigx/socket/Nope/k'), fakeHost);
        expect(internal.status).toBe(404);
        expect(internal.status).toBe(missing.status);
        const internalBody = (await internal.json()) as { error: { message: string } };
        const missingBody = (await missing.json()) as { error: { message: string } };
        expect(internalBody.error.message.replaceAll('Ledger', 'Nope')).toBe(
            missingBody.error.message
        );
        expect(fetched).toEqual([]);
    });

    it('400s a stateless worker pool with a named reason', async () => {
        const fetched: { url: string }[] = [];
        const route = objectSocketRoute({ resolver: resolverTo(fetched) });
        const res = await route.handle(upgrade('https://x.test/_sigx/socket/Pool/k'), fakeHost);
        expect(res.status).toBe(400);
        const body = (await res.json()) as { error: { message: string } };
        expect(body.error.message).toContain('stateless');
        expect(fetched).toEqual([]);
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
