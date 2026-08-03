/**
 * The `ActorRouter` seam: who picks the endpoint, and what happens when the
 * pick is wrong.
 *
 * The load-bearing property is negative — **a router can never fail a
 * call.** Routing is an optimization; a router that throws, answers
 * staleness, or points at a dead host costs a hop, never an answer. Several
 * tests here exist only to hold that line, because the natural
 * implementation of each would propagate.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
    __actorRef,
    actorRedirect,
    chainRouters,
    configureActors,
    learningRouter,
    routedTransport,
    staticRouter,
    type ActorCallInit,
    type ActorRouter,
    type ActorTransport
} from '@sigx/actors/client';
import { actor, defineActor } from '@sigx/actors';

const Cart = defineActor({
    type: 'Cart',
    state: () => ({ n: 0 }),
    methods: () => ({
        async add(_item: string): Promise<number> {
            return 1;
        }
    }),
    streams: () => ({
        async *ticks(_to: number): AsyncGenerator<number> {
            yield 1;
        }
    })
});

const ENDPOINT = 'http://default.test/_sigx/actor';
const Ref = __actorRef('Cart', ENDPOINT, ['ticks']) as typeof Cart;

afterEach(() => configureActors(null));

/** A 421 shaped exactly as the public endpoint sends it. */
function wrongHost(endpoint: string, hostId = 's1'): Error {
    return Object.assign(new Error(`[sigx actors] owned by ${hostId}`), {
        __sigxServerFnError: true,
        status: 421,
        data: { kind: 'wrong-host', owner: { hostId, endpoint } }
    });
}

interface Recorded {
    symbol: string;
    init: ActorCallInit | undefined;
}

/** A transport that records what it was asked to do, and can be scripted. */
function stub(
    script?: (call: Recorded, n: number) => unknown
): ActorTransport & { calls: Recorded[] } {
    const calls: Recorded[] = [];
    return {
        name: 'stub',
        calls,
        async call(symbol, _args, init) {
            calls.push({ symbol, init });
            const out = script?.({ symbol, init }, calls.length - 1);
            if (out instanceof Error) throw out;
            return out ?? 'ok';
        },
        async *stream(symbol, _args, init) {
            calls.push({ symbol, init });
            const out = script?.({ symbol, init }, calls.length - 1);
            if (out instanceof Error) throw out;
            yield out ?? 'ok';
        }
    };
}

describe('the ref reaches the router', () => {
    it('is threaded from the proxy, not re-derived from the symbol', async () => {
        const seen: unknown[] = [];
        const router: ActorRouter = {
            name: 'spy',
            resolve: (ref) => {
                seen.push(ref);
                return null;
            }
        };
        configureActors(routedTransport(stub(), router));
        await actor(Ref, 'c1').add('x');
        expect(seen).toEqual([{ type: 'Cart', key: 'c1' }]);
    });

    it('cannot be spoofed through .with()', async () => {
        const seen: unknown[] = [];
        const router: ActorRouter = {
            name: 'spy',
            resolve: (ref) => {
                seen.push(ref.key);
                return null;
            }
        };
        configureActors(routedTransport(stub(), router));
        // `.with()` sets call options; it must not be able to make this
        // proxy route as a different actor.
        await actor(Ref, 'real').with({ headers: { x: '1' } }).add('x');
        expect(seen).toEqual(['real']);
    });

    it('is absent for $live, so the router is bypassed entirely', async () => {
        // $live multiplexes many actors onto one request — there is no single
        // owner to route to, so it must not be routed at all. The live layer
        // calls the transport directly, with no ref, which is what this does.
        let asked = 0;
        const inner = stub();
        const transport = routedTransport(inner, {
            name: 'spy',
            resolve: () => {
                asked++;
                return 'http://never.test';
            }
        });

        await expect(transport.call('$live#subscribe', [[]], undefined)).resolves.toBe('ok');

        expect(asked).toBe(0);
        expect(inner.calls[0]!.init?.route).toBeUndefined();
    });
});

describe('endpoint precedence', () => {
    it("a router's answer beats configureActors({ endpoint })", async () => {
        // The regression this exists for: `endpointOf` used to prefer the
        // CONFIGURED endpoint, so any app that set one silently defeated its
        // own router.
        const inner = stub();
        configureActors({ endpoint: 'http://configured.test/_sigx/actor' });
        configureActors(routedTransport(inner, staticRouter('http://router.test/_sigx/actor')));
        await actor(Ref, 'c1').add('x');
        expect(inner.calls[0]!.init?.route).toBe('http://router.test/_sigx/actor');
    });

    it('null means "no opinion" and leaves the default alone', async () => {
        const inner = stub();
        configureActors(routedTransport(inner, staticRouter()));
        await actor(Ref, 'c1').add('x');
        expect(inner.calls[0]!.init?.route).toBeUndefined();
        expect(inner.calls[0]!.init?.endpoint).toBe(ENDPOINT);
    });
});

describe('a router can never fail a call', () => {
    it('survives resolve() throwing, and warns once', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const inner = stub();
        const transport = routedTransport(inner, {
            name: 'broken',
            resolve: () => {
                throw new Error('router blew up');
            }
        });
        configureActors(transport);

        await expect(actor(Ref, 'c1').add('x')).resolves.toBe('ok');
        await expect(actor(Ref, 'c2').add('x')).resolves.toBe('ok');

        expect(transport.stats().routerErrors).toBe(2);
        expect(warn).toHaveBeenCalledTimes(1); // noisy once, not per call
        warn.mockRestore();
    });

    it('survives learn() throwing on a redirect', async () => {
        const inner = stub((_c, n) => (n === 0 ? wrongHost('http://owner.test/_sigx/actor') : 'ok'));
        configureActors(
            routedTransport(inner, {
                name: 'broken-learn',
                resolve: () => null,
                learn: () => {
                    throw new Error('cache blew up');
                }
            })
        );
        // The redirect is still followed: a cache that throws while
        // REMEMBERING an answer must not destroy the answer itself.
        await expect(actor(Ref, 'c1').add('x')).resolves.toBe('ok');
        expect(inner.calls).toHaveLength(2);
    });
});

describe('following a redirect', () => {
    it('learns the owner and goes straight there next time', async () => {
        const owner = 'http://owner.test/_sigx/actor';
        const inner = stub((c, n) => (n === 0 && c.init?.route === undefined ? wrongHost(owner) : 'ok'));
        const router = learningRouter();
        const transport = routedTransport(inner, router);
        configureActors(transport);

        await expect(actor(Ref, 'c1').add('x')).resolves.toBe('ok');
        expect(inner.calls).toHaveLength(2);
        expect(inner.calls[1]!.init?.route).toBe(owner);

        await expect(actor(Ref, 'c1').add('x')).resolves.toBe('ok');
        expect(inner.calls).toHaveLength(3);
        expect(inner.calls[2]!.init?.route).toBe(owner); // no second redirect
        expect(transport.stats().redirects).toBe(1);
    });

    it('advertises that it can follow, and counts hops for the server', async () => {
        const inner = stub((_c, n) => (n === 0 ? wrongHost('http://owner.test/_sigx/actor') : 'ok'));
        configureActors(routedTransport(inner, learningRouter()));
        await actor(Ref, 'c1').add('x');

        expect(inner.calls[0]!.init?.headers?.['x-sigx-actor-follow']).toBe('1');
        expect(inner.calls[0]!.init?.headers?.['x-sigx-actor-hops']).toBeUndefined();
        // The server bounds the chain too, so it has to be told the count.
        expect(inner.calls[1]!.init?.headers?.['x-sigx-actor-hops']).toBe('1');
    });

    it('is bounded, and surfaces the last 421 rather than looping', async () => {
        // A cluster whose hosts disagree about ownership must not ping-pong.
        const inner = stub(() => wrongHost('http://elsewhere.test/_sigx/actor'));
        configureActors(routedTransport(inner, learningRouter(), { maxRedirects: 2 }));
        await expect(actor(Ref, 'c1').add('x')).rejects.toThrow(/owned by/);
        expect(inner.calls).toHaveLength(3); // initial + 2 redirects
    });

    it('re-routes a stream on the FIRST pull, not halfway through', async () => {
        const owner = 'http://owner.test/_sigx/actor';
        const inner = stub((c) => (c.init?.route === undefined ? wrongHost(owner) : 'chunk'));
        configureActors(routedTransport(inner, learningRouter()));

        const got: unknown[] = [];
        for await (const v of actor(Ref, 'c1').ticks(1)) got.push(v);
        expect(got).toEqual(['chunk']);
        expect(inner.calls[1]!.init?.route).toBe(owner);
    });
});

describe('a redirect only implicates ONE actor', () => {
    it('does not evict other actors cached at the same endpoint', async () => {
        // Regression: the redirect path used to `invalidate(oldRoute)`, and
        // `learningRouter.invalidate(endpoint)` prunes EVERY actor mapped
        // there. So one 421 threw away every good route sharing that host,
        // re-costing a round trip each. A 421 proves this actor moved — it
        // says nothing about its neighbours.
        const shared = 'http://host-a.test/_sigx/actor';
        const moved = 'http://host-b.test/_sigx/actor';
        const router = learningRouter();
        router.learn!({ type: 'Cart', key: 'stays' }, shared);
        router.learn!({ type: 'Cart', key: 'moves' }, shared);

        const inner = stub((c) => (c.init?.route === shared ? wrongHost(moved) : 'ok'));
        configureActors(routedTransport(inner, router));

        await expect(actor(Ref, 'moves').add('x')).resolves.toBe('ok');

        expect(router.resolve({ type: 'Cart', key: 'moves' }, ctx())).toBe(moved);
        // The neighbour is untouched.
        expect(router.resolve({ type: 'Cart', key: 'stays' }, ctx())).toBe(shared);
    });

    it('DOES drop the whole endpoint when it is unreachable', async () => {
        // The other half of the rule: a dial that never landed implicates
        // the host, not one actor.
        const dead = 'http://host-dead.test/_sigx/actor';
        const router = learningRouter();
        router.learn!({ type: 'Cart', key: 'a' }, dead);
        router.learn!({ type: 'Cart', key: 'b' }, dead);

        const inner = stub((c) =>
            c.init?.route === dead ? new TypeError('fetch failed') : 'ok'
        );
        configureActors(routedTransport(inner, router));

        await expect(actor(Ref, 'a').add('x')).resolves.toBe('ok');
        expect(router.stats!().size).toBe(0);
    });
});

describe('dropping a route that proved wrong', () => {
    it('forgets a learned endpoint that stops answering, then retries the default', async () => {
        const owner = 'http://owner.test/_sigx/actor';
        const router = learningRouter();
        const inner = stub((c, n) => {
            if (n === 0) return wrongHost(owner);
            if (c.init?.route === owner && n === 2) return new TypeError('fetch failed');
            return 'ok';
        });
        const transport = routedTransport(inner, router);
        configureActors(transport);

        await actor(Ref, 'c1').add('x'); // learns owner
        await expect(actor(Ref, 'c1').add('x')).resolves.toBe('ok'); // owner dies, falls back

        expect(transport.stats().invalidations).toBeGreaterThan(0);
        expect(router.stats!().size).toBe(0);
    });

    it('does NOT re-route a 4xx — that is the application answering', async () => {
        const owner = 'http://owner.test/_sigx/actor';
        const router = learningRouter();
        router.learn!({ type: 'Cart', key: 'c1' }, owner);
        const inner = stub(() =>
            Object.assign(new Error('forbidden'), { __sigxServerFnError: true, status: 403 })
        );
        configureActors(routedTransport(inner, router));

        await expect(actor(Ref, 'c1').add('x')).rejects.toThrow('forbidden');
        expect(inner.calls).toHaveLength(1); // no retry
        expect(router.stats!().size).toBe(1); // and nothing forgotten
    });
});

describe('learningRouter', () => {
    const ref = (key: string) => ({ type: 'Cart', key });

    it('evicts the oldest at maxEntries', () => {
        const router = learningRouter({ maxEntries: 2 });
        router.learn!(ref('a'), 'http://1');
        router.learn!(ref('b'), 'http://2');
        router.learn!(ref('c'), 'http://3');
        expect(router.stats!().size).toBe(2);
        expect(router.resolve(ref('a'), ctx())).toBeNull(); // oldest gone
        expect(router.resolve(ref('c'), ctx())).toBe('http://3');
    });

    it('remembers nothing at maxEntries: 0', () => {
        const router = learningRouter({ maxEntries: 0 });
        router.learn!(ref('a'), 'http://1');
        expect(router.resolve(ref('a'), ctx())).toBeNull();
    });

    it('invalidate(endpoint) drops only matching entries', () => {
        const router = learningRouter();
        router.learn!(ref('a'), 'http://1');
        router.learn!(ref('b'), 'http://2');
        router.invalidate!('http://1');
        expect(router.resolve(ref('a'), ctx())).toBeNull();
        expect(router.resolve(ref('b'), ctx())).toBe('http://2');
    });

    it('invalidate() with no argument drops everything', () => {
        const router = learningRouter();
        router.learn!(ref('a'), 'http://1');
        router.learn!(ref('b'), 'http://2');
        router.invalidate!();
        expect(router.stats!().size).toBe(0);
    });
});

describe('chainRouters', () => {
    const ref = { type: 'Cart', key: 'c1' };

    it('takes the first non-null answer', () => {
        const chain = chainRouters(staticRouter(), staticRouter('http://second'));
        expect(chain.resolve(ref, ctx())).toBe('http://second');
    });

    it('broadcasts learn and invalidate to every member', () => {
        const a = learningRouter();
        const b = learningRouter();
        const chain = chainRouters(a, b);
        chain.learn!(ref, 'http://x');
        expect(a.resolve(ref, ctx())).toBe('http://x');
        expect(b.resolve(ref, ctx())).toBe('http://x');
        chain.invalidate!('http://x');
        expect(a.stats!().size + b.stats!().size).toBe(0);
    });
});

describe('actorRedirect', () => {
    it('reads a wrong-host 421', () => {
        expect(actorRedirect(wrongHost('http://o/_sigx/actor', 's9'))).toEqual({
            endpoint: 'http://o/_sigx/actor',
            hostId: 's9'
        });
    });

    it('is null for anything else', () => {
        expect(actorRedirect(new Error('boom'))).toBeNull();
        expect(actorRedirect(undefined)).toBeNull();
        expect(
            actorRedirect(Object.assign(new Error('x'), { data: { kind: 'state-conflict' } }))
        ).toBeNull();
        // wrong-host without a usable endpoint is not actionable.
        expect(
            actorRedirect(Object.assign(new Error('x'), { data: { kind: 'wrong-host' } }))
        ).toBeNull();
    });
});

function ctx() {
    return {
        symbol: 'Cart#add',
        method: 'add',
        mode: 'call' as const,
        endpoint: ENDPOINT,
        attempt: 0
    };
}

describe('a router cannot send a call to the wrong origin', () => {
    it.each([
        ['an empty string', ''],
        ['a number', 42],
        ['an object', { url: 'http://x' }]
    ])('ignores %s from resolve(), and uses the default', async (_label, answer) => {
        // A router is user code and may be JavaScript, where these sail past
        // the types. An empty base is the dangerous one: the URL becomes
        // `/{symbol}`, a RELATIVE path resolved against whatever origin the
        // page is on — silently calling the wrong host, which is far worse
        // than not routing at all.
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const inner = stub();
        const transport = routedTransport(inner, {
            name: 'sloppy',
            resolve: () => answer as unknown as string
        });
        configureActors(transport);

        await expect(actor(Ref, 'c1').add('x')).resolves.toBe('ok');
        expect(inner.calls[0]!.init?.route).toBeUndefined();
        expect(inner.calls[0]!.init?.endpoint).toBe(ENDPOINT);
        expect(transport.stats().routerErrors).toBe(1);
        warn.mockRestore();
    });
});
