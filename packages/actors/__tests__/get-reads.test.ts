/**
 * GET-cacheable actor reads (#11) — the `reads:` declaration, end to end.
 *
 * The point is traffic that never reaches an actor: a declared read answers
 * from a browser, CDN or reverse-proxy cache, which no amount of turn
 * throughput can compete with. The price is explicit and permanent — for
 * `maxAge` seconds the response may be older than the actor's state, and
 * nothing on the server can invalidate it — so the tests here are mostly
 * about the promises made to those caches being exactly the declared ones.
 *
 * Pinned against the REAL endpoint, because every piece of the mechanism is
 * core's: GET admission, `?args=` decoding, the header emission, and the
 * `Vary` that keeps a private read out of a shared cache.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { ServerFnError } from '@sigx/server';
import { actor, defineActor, type Host } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';
import { handleActorRequest } from '@sigx/actors/server';
import { preferLocalPolicy } from '@sigx/actors/cluster';
import {
    ACTOR_ROUTE_HEADER,
    __actorRef,
    configureActors,
    hashRouteToken
} from '@sigx/actors/client';
import { createCluster } from './harness';

const ENDPOINT = 'http://actors.test/_sigx/actor';
const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

/** Public reads: no guards at all, so a shared cache is honest. */
const Product = defineActor({
    type: 'Product',
    allowAnonymous: true,
    reads: {
        summary: { maxAge: 5 },
        price: { maxAge: 60, public: true, staleWhileRevalidate: 30 }
    },
    state: () => ({ views: 0, cents: 999 }),
    methods: (ctx) => ({
        async summary(currency: string, at: Date) {
            return { currency, at, cents: ctx.state.cents };
        },
        async price() {
            return ctx.state.cents;
        },
        /** Undeclared: POST-only, like every other actor method. */
        async views() {
            return ctx.state.views;
        },
        async setPrice(cents: number) {
            ctx.state.cents = cents;
            await ctx.save();
            return cents;
        }
    }),
    streams: (ctx) => ({
        async *feed() {
            yield* ctx.changes({ initial: true });
        }
    })
});

/** A guarded actor whose read is still cacheable — per client, not shared. */
const Cart = defineActor({
    type: 'Cart',
    authorize: [
        (_p, rq) => {
            // `authorization`, not `cookie`: this suite runs under happy-dom,
            // whose `Request` drops a cookie header as forbidden — the policy
            // would then reject its own fixture rather than test anything.
            if (rq.request.headers.get('authorization') === null) {
                throw new ServerFnError(401, 'no session');
            }
            return true;
        }
    ],
    reads: { total: { maxAge: 10 } },
    state: () => ({ items: 2 }),
    methods: (ctx) => ({
        async total() {
            return ctx.state.items;
        }
    })
});

const ProductRef = __actorRef('Product', ENDPOINT, ['feed'], ['summary', 'price']) as unknown as
    typeof Product;

const running: Host[] = [];

async function startHost(): Promise<Host> {
    const host = createHost({ actors: [Product, Cart], defaults: quiet });
    await host.start();
    running.push(host);
    return host;
}

/** A GET straight into the endpoint, args in the query like the proxy sends. */
function get(
    host: Host,
    symbol: string,
    args: readonly unknown[],
    headers?: Record<string, string>
): Promise<Response> {
    const query = encodeURIComponent(JSON.stringify(args));
    return handleActorRequest(
        new Request(`${ENDPOINT}/${encodeURIComponent(symbol)}?args=${query}`, {
            method: 'GET',
            ...(headers ? { headers } : {})
        }),
        { host, origin: false }
    );
}

afterEach(async () => {
    configureActors(null);
    for (const host of running.splice(0)) await host.stop({ timeoutMs: 1000 });
});

describe('the reads: declaration', () => {
    it('refuses to cache a stream', () => {
        expect(() =>
            defineActor({
                type: 'Bad',
                allowAnonymous: true,
                reads: { feed: { maxAge: 5 } },
                state: () => ({}),
                methods: () => ({}),
                streams: () => ({
                    // eslint-disable-next-line require-yield
                    async *feed() {}
                })
            })
        ).toThrow(/a stream cannot be cached/);
    });

    it('refuses a maxAge that is not whole seconds', () => {
        const bad = (maxAge: unknown): void =>
            void defineActor({
                type: 'Bad',
                allowAnonymous: true,
                reads: { read: { maxAge: maxAge as number } },
                state: () => ({}),
                methods: () => ({ async read() {} })
            });

        expect(() => bad(-1)).toThrow(/whole, non-negative seconds/);
        expect(() => bad(Number.NaN)).toThrow(/whole, non-negative seconds/);
        expect(() => bad(Number.POSITIVE_INFINITY)).toThrow(/whole, non-negative seconds/);
        expect(() => bad('5')).toThrow(/whole, non-negative seconds/);
        // `Cache-Control`'s delta-seconds are integers (RFC 9111 §1.2.2):
        // `max-age=0.5` is not half a second to an intermediary, it is a
        // malformed directive, and the caching quietly does not happen.
        expect(() => bad(0.5)).toThrow(/whole, non-negative seconds/);
        expect(() => bad(0)).not.toThrow(); // 0 is a real answer: revalidate always

        const badWindow = (field: 'staleWhileRevalidate' | 'sMaxAge'): void =>
            void defineActor({
                type: 'Bad',
                allowAnonymous: true,
                reads: { read: { maxAge: 5, [field]: 1.5 } },
                state: () => ({}),
                methods: () => ({ async read() {} })
            });
        expect(() => badWindow('staleWhileRevalidate')).toThrow(/whole, non-negative seconds/);
        expect(() => badWindow('sMaxAge')).toThrow(/whole, non-negative seconds/);
    });

    it('refuses `public` on a guarded read — a shared cache would leak it', () => {
        // The whole reason `public` is gated: a shared cache serves one
        // caller's copy to the next, so the response must depend on its
        // arguments alone. A guard is the one thing here that provably reads
        // the request, and nothing can inspect WHAT it reads.
        const definition = {
            type: 'Leaky',
            authorize: [() => true],
            reads: { mine: { maxAge: 5, public: true } },
            state: () => ({}),
            methods: () => ({ async mine() {} })
        };
        expect(() => defineActor(definition)).toThrow(/public read goes in SHARED caches/);

        // Per-method guards are caught the same way…
        expect(() =>
            defineActor({
                type: 'Leaky',
                allowAnonymous: true,
                methodAuthorize: { mine: [() => true] },
                reads: { mine: { maxAge: 5, public: true } },
                state: () => ({}),
                methods: () => ({ async mine() {} })
            })
        ).toThrow(/public read goes in SHARED caches/);

        // …while the same read without `public` is fine: still cached, per
        // client, and the endpoint adds `Vary: Cookie`.
        expect(() => defineActor({ ...definition, reads: { mine: { maxAge: 5 } } })).not.toThrow();
    });

    it('refuses a declared key with no cache object', () => {
        // The one shape that could ship a client and server that disagree: the
        // build stamps GET-capable names from the KEYS (it cannot evaluate
        // values), while the wrapper is marked cacheable from the VALUE — so
        // the proxy would GET a method the endpoint answers 405 for, and both
        // sides would look declared.
        expect(() =>
            defineActor({
                type: 'Skewed',
                allowAnonymous: true,
                reads: { price: undefined },
                state: () => ({}),
                methods: () => ({ async price() {} })
            })
        ).toThrow(/has no cache declaration/);
    });

    it('accepts a declaration with no reads at all', () => {
        // The common case must stay free: no `reads:`, no validation, no
        // metadata, nothing stamped on the wrapper.
        expect(() =>
            defineActor({
                type: 'Plain',
                allowAnonymous: true,
                state: () => ({}),
                methods: () => ({ async read() {} })
            })
        ).not.toThrow();
    });
});

describe('a declared read over GET', () => {
    it('answers, and emits exactly the declared Cache-Control', async () => {
        const host = await startHost();

        const response = await get(host, 'Product#price', ['p1']);
        expect(response.status).toBe(200);
        expect(await response.json()).toEqual({ data: 999 });
        expect(response.headers.get('cache-control')).toBe(
            'public, max-age=60, s-maxage=60, stale-while-revalidate=30'
        );
        // `public` means a shared cache may keep ONE copy for everybody, so
        // varying on the cookie would defeat the point.
        expect(response.headers.get('vary')).toBeNull();
    });

    it('keeps a private read out of shared caches', async () => {
        const host = await startHost();

        const response = await get(host, 'Cart#total', ['c1'], { authorization: 'Bearer x' });
        expect(response.status).toBe(200);
        expect(response.headers.get('cache-control')).toBe('private, max-age=10');
        // The header that makes a per-client cache safe: without it a shared
        // cache could hand this response to the next visitor.
        expect(response.headers.get('vary')).toContain('Cookie');
    });

    it('runs the guard chain — a cacheable read is not an unguarded one', async () => {
        const host = await startHost();

        const refused = await get(host, 'Cart#total', ['c1']); // no session
        expect(refused.status).toBe(401);
        // A failed read must never be cached, whatever the declaration says.
        expect(refused.headers.get('cache-control')).toBe('no-store');
    });

    it('decodes arguments from the query through the same codec as a body', async () => {
        const host = await startHost();
        const at = new Date(1700000000000);

        const response = await get(host, 'Product#summary', [
            'p1',
            'EUR',
            { $date: at.getTime() }
        ]);
        expect(response.status).toBe(200);
        const body = (await response.json()) as { data: { currency: string; at: unknown } };
        expect(body.data.currency).toBe('EUR');
        // Rich types survive the query exactly as they survive a body — the
        // endpoint applies the codec to `?args=` too.
        expect(body.data.at).toEqual({ $date: at.getTime() });
    });

    it('405s a method with no declaration, and a stream', async () => {
        const host = await startHost();

        const undeclared = await get(host, 'Product#views', ['p1']);
        expect(undeclared.status).toBe(405);
        expect(undeclared.headers.get('allow')).toBe('POST');

        // A stream cannot be GET even though `reads` would refuse to name one:
        // the endpoint checks `__sigxStream` independently.
        const stream = await get(host, 'Product#feed', ['p1']);
        expect(stream.status).toBe(405);
    });

    it('does not inherit a declaration from Object.prototype', async () => {
        // `reads?.[method]` follows the prototype chain, so an actor method
        // named `toString` (or `constructor`, `valueOf`, …) found a truthy
        // "declaration" nobody wrote: the wrapper was stamped GET-capable with
        // `max-age=undefined`, and the endpoint then ADMITTED a GET for it.
        const host = await startHost();

        const response = await get(host, 'Product#toString', ['p1']);
        expect(response.status).toBe(405);
        expect(response.headers.get('cache-control')).not.toContain('undefined');
    });

    it('still accepts POST for a declared read', async () => {
        // The declaration lives on the definition, not on the wire: an old
        // client, a hand-built host with no build transform, or a service
        // calling by hand all keep POSTing, and must keep working.
        const host = await startHost();

        const response = await handleActorRequest(
            new Request(`${ENDPOINT}/${encodeURIComponent('Product#price')}`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ args: ['p1'] })
            }),
            { host, origin: false }
        );
        expect(response.status).toBe(200);
        expect(await response.json()).toEqual({ data: 999 });
        // No cache headers on the POST: caching is a property of the GET
        // response, and a POST reply is not one an intermediary may keep.
        expect(response.headers.get('cache-control')).toBeNull();
    });
});

describe('a cacheable read in a cluster', () => {
    it('proxies to the owner and still answers with the cache headers', async () => {
        // A GET is decoded at whichever host the edge picked, then dispatched
        // like any other call — so an actor another host owns is reached over
        // the internal transport, and the caching promise is made by the host
        // that answered. Nothing about the hop is cache-visible.
        const cluster = await createCluster(2, { actors: [Product], policy: preferLocalPolicy() });
        try {
            await cluster.hosts[1]!.actor(Product, 'remote').setPrice(1234);

            const query = encodeURIComponent(JSON.stringify(['remote']));
            const response = await handleActorRequest(
                new Request(
                    `http://host0.test/_sigx/actor/${encodeURIComponent('Product#price')}?args=${query}`,
                    { method: 'GET' }
                ),
                { host: cluster.hosts[0]!, origin: false }
            );

            expect(response.status).toBe(200);
            expect(await response.json()).toEqual({ data: 1234 });
            expect(response.headers.get('cache-control')).toBe(
                'public, max-age=60, s-maxage=60, stale-while-revalidate=30'
            );
        } finally {
            await cluster.stop();
        }
    });
});

describe('the client proxy', () => {
    it('GETs a declared read and POSTs everything else, token intact', async () => {
        const host = await startHost();
        const seen: { method: string; url: string; route: string | null }[] = [];
        configureActors({
            endpoint: ENDPOINT,
            fetch: async (input, init) => {
                const request = new Request(input, init);
                seen.push({
                    method: request.method,
                    url: request.url,
                    route: request.headers.get(ACTOR_ROUTE_HEADER)
                });
                return handleActorRequest(request, { host, origin: false });
            }
        });

        await expect(actor(ProductRef, 'p1').price()).resolves.toBe(999);
        await expect(actor(ProductRef, 'p1').views()).resolves.toBe(0);

        const [read, write] = seen;
        expect(read!.method).toBe('GET');
        // All-scalar arguments ride as named params — arg 0 is the actor key.
        // The `%5B%22`-noise of `?args=` was the read path's share of the ugly
        // URL, and a cacheable GET is the one an operator actually reads.
        expect(read!.url).toContain('?a0=p1');
        expect(read!.url).not.toContain('args=');
        // The routing token still travels, in BOTH carriers: an edge that
        // hashes it must route a cacheable read exactly like any other call.
        expect(read!.url).toContain(`/r/${hashRouteToken('Product', 'p1')}/`);
        expect(read!.route).toBe(hashRouteToken('Product', 'p1'));
        expect(write!.method).toBe('POST');
    });

    it('round-trips rich arguments and results through the query', async () => {
        // The named-param form is all-or-nothing: one non-scalar argument and
        // the WHOLE call falls back to the `args=` blob, so a call never mixes
        // the two encodings and the cache key stays a pure function of the
        // arguments. `at` is a Date, so this is the fallback under test.
        const host = await startHost();
        const urls: string[] = [];
        configureActors({
            endpoint: ENDPOINT,
            fetch: async (input, init) => {
                const request = new Request(input, init);
                urls.push(request.url);
                return handleActorRequest(request, { host, origin: false });
            }
        });

        const at = new Date(1700000000000);
        const summary = (await actor(ProductRef, 'p1').summary('EUR', at)) as {
            currency: string;
            at: Date;
            cents: number;
        };
        expect(summary.currency).toBe('EUR');
        expect(summary.at).toBeInstanceOf(Date);
        expect(summary.at.getTime()).toBe(at.getTime());
        expect(summary.cents).toBe(999);
        expect(urls[0]).toContain('?args=');
        expect(urls[0]).not.toContain('a0=');
    });

    it('honours an explicit `.with({ get: false })` on a declared read', async () => {
        // The escape hatch, and it has two real uses: arguments too large for a
        // URL (intermediaries cap the query), and arguments — the actor key
        // among them — that should stay out of access logs and referrers. The
        // endpoint accepts POST for a declared read either way.
        const host = await startHost();
        const methods: string[] = [];
        configureActors({
            endpoint: ENDPOINT,
            fetch: async (input, init) => {
                const request = new Request(input, init);
                methods.push(request.method);
                return handleActorRequest(request, { host, origin: false });
            }
        });

        await expect(actor(ProductRef, 'p1').with({ get: false }).price()).resolves.toBe(999);
        await expect(actor(ProductRef, 'p1').price()).resolves.toBe(999);

        expect(methods).toEqual(['POST', 'GET']);
    });

    it('a one-way call never rides a cacheable GET — oneWay wins over the declaration', async () => {
        // A one-way answers at ACCEPTANCE with no data; serving that from an
        // HTTP cache would replay an ack as if the call had happened.
        const host = await startHost();
        const methods: string[] = [];
        configureActors({
            endpoint: ENDPOINT,
            fetch: async (input, init) => {
                const request = new Request(input, init);
                methods.push(`${request.method}${new URL(request.url).search}`);
                return handleActorRequest(request, { host, origin: false });
            }
        });

        await expect(
            actor(ProductRef, 'p1').with({ oneWay: true }).price()
        ).resolves.toBeUndefined();
        // Even an explicit get: true loses to oneWay.
        await expect(
            actor(ProductRef, 'p1').with({ oneWay: true, get: true }).price()
        ).resolves.toBeUndefined();

        expect(methods).toEqual(['POST', 'POST']);
    });

    it('ignores `get: true` where it cannot apply — writes and streams', async () => {
        // `get` is a carrier choice for a DECLARED read. Forwarded anywhere
        // else it would turn a working write (or a stream) into a 405, so the
        // proxy drops it rather than passing a caller's mistake to the wire.
        const host = await startHost();
        const methods: string[] = [];
        configureActors({
            endpoint: ENDPOINT,
            fetch: async (input, init) => {
                const request = new Request(input, init);
                // The symbol is the last TWO segments now (`Product/views`).
                const symbol = new URL(request.url).pathname.split('/').slice(-2).join('/');
                methods.push(`${request.method} ${symbol}`);
                return handleActorRequest(request, { host, origin: false });
            }
        });

        const withGet = actor(ProductRef, 'p1').with({ get: true });
        await expect(withGet.setPrice(1)).resolves.toBe(1); // an undeclared WRITE
        await expect(withGet.views()).resolves.toBe(0); // an undeclared read
        const stream = withGet.feed();
        const first = await stream[Symbol.asyncIterator]().next();
        expect(first.done).toBe(false);

        expect(methods).toEqual([
            'POST Product/setPrice',
            'POST Product/views',
            'POST Product/feed'
        ]);
    });

    it('sends no content-type on a GET — it would describe a body there is none of', async () => {
        // Also one fewer non-safelisted header, so one fewer reason for a
        // cross-origin request to preflight. Only one fewer: the routing token
        // header ships by default and preflights on its own.
        const host = await startHost();
        let contentType: string | null = 'unset';
        configureActors({
            endpoint: ENDPOINT,
            fetch: async (input, init) => {
                const request = new Request(input, init);
                contentType = request.headers.get('content-type');
                return handleActorRequest(request, { host, origin: false });
            }
        });

        await actor(ProductRef, 'p1').price();
        expect(contentType).toBeNull();
    });
});
