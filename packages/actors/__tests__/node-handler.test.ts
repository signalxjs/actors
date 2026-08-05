/**
 * @vitest-environment node
 *
 * Real sockets need real fetch: happy-dom's enforces the browser
 * same-origin policy and blocks every request to a test server.
 *
 * `createAppHandler` over REAL sockets — the bridge that replaces the
 * ~40 lines every clustered Node deployment used to hand-write.
 *
 * The in-process harness pipes `Request`s straight into handlers, so it
 * cannot catch anything this bridge actually does: header copying, body
 * buffering, streamed responses, and disconnect→abort. Two hosts on real
 * ports do.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { createServer, type Server } from 'node:http';
import { connect } from 'node:net';
import { defineActor, type ActorStorage } from '@sigx/actors';
import {
    defineActorApp,
    memoryStorage,
    type ActorApp,
    type ActorPlugin
} from '@sigx/actors/host';
import { cluster, memoryClusterHub, preferLocalPolicy } from '@sigx/actors/cluster';
import { createAppHandler } from '@sigx/actors/node';
import { createFetchHandler } from '@sigx/actors/server';
import { hashRouteToken } from '@sigx/actors/client';
import { withoutServerApp } from '../../../vitest.setup';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const Counter = defineActor({
    type: 'Counter',
    allowAnonymous: true,
    // A declared read is the only call that carries a QUERY STRING, so it is
    // what proves the routed rewrite keeps one.
    reads: { total: { maxAge: 5 } },
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        },
        async total() {
            return ctx.state.count;
        }
    }),
    streams: (ctx) => ({
        async *watch() {
            yield ctx.snapshot();
            for await (const state of ctx.changes()) yield state;
        }
    })
});

interface Node {
    app: ActorApp;
    server: Server;
    port: number;
}

let running: Node[] = [];

afterEach(async () => {
    await Promise.allSettled(running.map((n) => n.app.stop({ timeoutMs: 1000 })));
    await Promise.allSettled(
        running.map((n) => new Promise<void>((resolve) => n.server.close(() => resolve())))
    );
    running = [];
});

interface ServeOptions {
    maxBodyBytes?: number;
    extraPlugin?: ActorPlugin;
}

/** N hosts on real localhost sockets, mounted with createAppHandler. */
async function serveCluster(
    n: number,
    storage: ActorStorage,
    serveOptions: ServeOptions = {}
): Promise<Node[]> {
    const hub = memoryClusterHub();
    const nodes: Node[] = [];
    for (let i = 0; i < n; i++) {
        // Port 0 = let the OS pick, so the suite never collides.
        const server = createServer();
        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        const port = typeof address === 'object' && address ? address.port : 0;

        let app = defineActorApp({ actors: [Counter], storage, defaults: quiet }).use(
            cluster({
                providers: hub.providers(),
                advertise: `http://127.0.0.1:${port}`,
                secret: 'node-test-secret',
                policy: preferLocalPolicy()
            })
        );
        if (serveOptions.extraPlugin) app = app.use(serveOptions.extraPlugin);
        // One handler for BOTH mounts — the public endpoint and the
        // cluster's internal route, which used to need a bespoke bridge.
        const handler = createAppHandler(app, {
            origin: false,
            ...(serveOptions.maxBodyBytes !== undefined
                ? { maxBodyBytes: serveOptions.maxBodyBytes }
                : {})
        });
        server.on('request', (req, res) => {
            void handler(req, res, () => res.writeHead(404).end());
        });
        await app.start();
        nodes.push({ app, server, port });
    }
    return nodes;
}

describe('createAppHandler over real sockets', () => {
    it('routes a cross-host call through the plugin internal mount', async () => {
        const storage = memoryStorage();
        const nodes = await serveCluster(2, storage);
        running = nodes;

        // prefer-local puts 'a' on host 1...
        await expect(nodes[1]!.app.host!.actor(Counter, 'a').increment(2)).resolves.toBe(2);
        expect(nodes[1]!.app.host!.stats().perType['Counter']).toBe(1);

        // ...so calling through host 0 has to cross a real socket, over the
        // internal mount the cluster plugin contributed.
        await expect(nodes[0]!.app.host!.actor(Counter, 'a').increment(3)).resolves.toBe(5);
        // Still exactly one activation, on the owner.
        expect(nodes[0]!.app.host!.stats().activations).toBe(0);
        expect(nodes[1]!.app.host!.stats().perType['Counter']).toBe(1);
    });

    it('falls through on an unparseable request target instead of 500ing', async () => {
        const nodes = await serveCluster(1, memoryStorage());
        running = nodes;

        // `req.url` is the RAW request target and Node neither normalizes
        // nor validates it, so a client can send one that `new URL()`
        // refuses. It cannot address an actor, so it belongs to whatever is
        // mounted after us — a 500 here would also mean a document handler
        // never got the chance to answer.
        const raw = (target: string): Promise<string> =>
            new Promise((resolve) => {
                const socket = connect(nodes[0]!.port, '127.0.0.1', () => {
                    socket.write(
                        `GET ${target} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n`
                    );
                });
                let buffer = '';
                socket.on('data', (chunk) => (buffer += chunk));
                socket.on('close', () => resolve(buffer));
            });

        for (const target of ['//', 'http://[']) {
            const response = await raw(target);
            // 404 is the harness's own fallthrough, i.e. `next()` ran.
            expect(response.split('\r\n')[0], target).toContain('404');
        }

        // …and the server is still alive afterwards.
        const ok = await raw('/_sigx/actor');
        expect(ok).toBeTruthy();
    });

    it('falls through on an ABSOLUTE-form target, tokenized or not', async () => {
        // The other raw target Node hands through verbatim: a proxy-style
        // request line (`GET http://host/path HTTP/1.1`). Core's adapter
        // gates on `req.url.startsWith('/_sigx/actor/')`, which an absolute
        // form never satisfies — so the actor mount does not serve one at
        // all, and the routing token cannot change that either way. Pinned
        // because the Node strip works on the raw target: this is what says
        // the two forms are not silently decided differently.
        const nodes = await serveCluster(1, memoryStorage());
        running = nodes;
        const origin = `http://127.0.0.1:${nodes[0]!.port}`;

        const raw = (target: string): Promise<string> =>
            new Promise((resolve) => {
                const socket = connect(nodes[0]!.port, '127.0.0.1', () => {
                    socket.write(
                        `POST ${target} HTTP/1.1\r\nHost: 127.0.0.1\r\n` +
                            `content-type: application/json\r\ncontent-length: 2\r\n` +
                            `Connection: close\r\n\r\n{}`
                    );
                });
                let buffer = '';
                socket.on('data', (chunk) => (buffer += chunk));
                socket.on('close', () => resolve(buffer));
            });

        for (const path of [
            '/_sigx/actor/Counter%23increment',
            `/_sigx/actor/r/${hashRouteToken('Counter', 'abs')}/Counter%23increment`
        ]) {
            const response = await raw(`${origin}${path}`);
            // 404 is the harness's own fallthrough, i.e. `next()` ran — the
            // SAME answer with the token as without it.
            expect(response.split('\r\n')[0], path).toContain('404');
        }
    });

    it('streams a cross-host watch() back through the bridge', async () => {
        const storage = memoryStorage();
        const nodes = await serveCluster(2, storage);
        running = nodes;

        await nodes[1]!.app.host!.actor(Counter, 'streamer').increment(1);

        // Consumed from the NON-owner: every chunk crosses the socket and
        // comes back through sendResponse's streaming path.
        const seen: number[] = [];
        // Wait for the subscription to actually attach, signalled by its
        // initial snapshot — a fixed sleep would flake on a loaded CI box.
        let attached!: () => void;
        const subscribed = new Promise<void>((resolve) => {
            attached = resolve;
        });
        const watching = (async () => {
            for await (const state of nodes[0]!.app.host!.actor(Counter, 'streamer').watch()) {
                seen.push((state as { count: number }).count);
                if (seen.length === 1) attached();
                if (seen.length >= 3) break;
            }
        })();

        await subscribed;
        await nodes[1]!.app.host!.actor(Counter, 'streamer').increment(10);
        await nodes[1]!.app.host!.actor(Counter, 'streamer').increment(100);
        await watching;

        expect(seen).toEqual([1, 11, 111]);
    });

    it('serves the public actor endpoint on the same handler', async () => {
        const storage = memoryStorage();
        const nodes = await serveCluster(1, storage);
        running = nodes;

        const response = await fetch(
            `http://127.0.0.1:${nodes[0]!.port}/_sigx/actor/Counter%23increment`,
            {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ args: ['wire', 7] })
            }
        );
        expect(response.status).toBe(200);
        await expect(response.json()).resolves.toEqual({ data: 7 });
    });

    it('serves a ROUTED url — the client default — identically to a bare one', async () => {
        // #93: `route: 'hash'` is the client default, so every call through
        // `@sigx/actors/client` arrives as `{base}/r/{token}/{symbol}`. The
        // WinterCG mount strips those segments before core decodes the path;
        // the Node mount did not, so core read `r/{token}/Counter#increment`
        // as the SYMBOL — an unknown actor, and (below) a 401 before that.
        const nodes = await serveCluster(1, memoryStorage());
        running = nodes;
        const base = `http://127.0.0.1:${nodes[0]!.port}/_sigx/actor`;
        // Byte-for-byte what the default `route: 'hash'` client emits.
        const token = hashRouteToken('Counter', 'routed');

        const routed = await fetch(`${base}/r/${token}/Counter%23increment`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args: ['routed', 7] })
        });
        expect(routed.status).toBe(200);
        await expect(routed.json()).resolves.toEqual({ data: 7 });

        // The token routes; it never identifies. The bare URL reaches the
        // same activation, and its count continues from the routed call's.
        const bare = await fetch(`${base}/Counter%23increment`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args: ['routed', 1] })
        });
        await expect(bare.json()).resolves.toEqual({ data: 8 });

        // A declared read is a GET with `?args=`, so this is also the case
        // that pins the QUERY surviving the path rewrite.
        const args = encodeURIComponent(JSON.stringify(['routed']));
        const read = await fetch(`${base}/r/${token}/Counter%23total?args=${args}`);
        expect(read.status).toBe(200);
        await expect(read.json()).resolves.toEqual({ data: 8 });
        expect(read.headers.get('cache-control')).toContain('max-age=5');
    });

    it('keeps allowAnonymous on the routed path with no server app configured', async () => {
        // The reported symptom (#93), exactly: 200 direct, 401 routed. The
        // suite stamps a signed-in app for every test, which would hide it —
        // an anonymous process is where fail-closed actually bites, and where
        // `allowAnonymous` has to reach core's PRE-DECODE identity gate. It
        // only can if the wrapper core resolves is the actor's, which only
        // happens if the routing segments came off the path first.
        const nodes = await serveCluster(1, memoryStorage());
        running = nodes;
        const base = `http://127.0.0.1:${nodes[0]!.port}/_sigx/actor`;
        const token = hashRouteToken('Counter', 'anon');

        await withoutServerApp(async () => {
            for (const url of [
                `${base}/Counter%23increment`,
                `${base}/r/${token}/Counter%23increment`
            ]) {
                const response = await fetch(url, {
                    method: 'POST',
                    headers: { 'content-type': 'application/json' },
                    body: JSON.stringify({ args: ['anon', 1] })
                });
                expect(response.status, url).toBe(200);
            }
        });
    });

    it('reports 404 skew against the SYMBOL, not the routing token', async () => {
        const nodes = await serveCluster(1, memoryStorage());
        running = nodes;

        const response = await fetch(
            `http://127.0.0.1:${nodes[0]!.port}/_sigx/actor/r/abc123/Ghost%23increment`,
            {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ args: ['g1', 1] })
            }
        );
        expect(response.status).toBe(404);
        const body = (await response.json()) as { error: { message: string } };
        expect(body.error.message).toContain('Ghost#increment');
        expect(body.error.message).not.toContain('abc123');
    });

    it('rejects an oversized body with 413 before buffering it', async () => {
        const storage = memoryStorage();
        const nodes = await serveCluster(1, storage, { maxBodyBytes: 1024 });
        running = nodes;

        // Declared length is over the cap — rejected without reading.
        const declared = await fetch(
            `http://127.0.0.1:${nodes[0]!.port}/_sigx/host/Counter%23increment`,
            {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: 'x'.repeat(4096)
            }
        );
        expect(declared.status).toBe(413);

        // ...and a chunked body with NO content-length is capped while
        // reading — the case that could otherwise OOM the process, since
        // there is no declared length to reject up front. No catch here on
        // purpose: swallowing a fetch rejection would let this pass without
        // the request ever reaching the server.
        const streamed = await fetch(
            `http://127.0.0.1:${nodes[0]!.port}/_sigx/host/Counter%23increment`,
            {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                duplex: 'half',
                body: new ReadableStream<Uint8Array>({
                    start(controller) {
                        for (let i = 0; i < 8; i++) {
                            controller.enqueue(new TextEncoder().encode('y'.repeat(512)));
                        }
                        controller.close();
                    }
                })
            } as RequestInit
        );
        expect(streamed.status).toBe(413);
    });

    it('mounts on createServer() directly, with no next()', async () => {
        // Exactly the README's shape: Node calls a listener with (req, res).
        const hub = memoryClusterHub();
        const server = createServer();
        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        const port = typeof address === 'object' && address ? address.port : 0;
        const app = defineActorApp({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: quiet
        }).use(
            cluster({
                providers: hub.providers(),
                advertise: `http://127.0.0.1:${port}`,
                secret: 's'
            })
        );
        server.on('request', createAppHandler(app, { origin: false }));
        await app.start();
        try {
            // A matching call still works...
            const ok = await fetch(`http://127.0.0.1:${port}/_sigx/actor/Counter%23increment`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ args: ['direct', 4] })
            });
            await expect(ok.json()).resolves.toEqual({ data: 4 });
            // ...and an unmatched one ends in a 404 instead of hanging.
            const missing = await fetch(`http://127.0.0.1:${port}/nope`);
            expect(missing.status).toBe(404);
        } finally {
            await app.stop({ timeoutMs: 1000 });
            await new Promise<void>((resolve) => server.close(() => resolve()));
        }
    });

    it('answers 503 while the app is not running', async () => {
        const hub = memoryClusterHub();
        const server = createServer();
        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        const port = typeof address === 'object' && address ? address.port : 0;
        const app = defineActorApp({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: quiet
        }).use(
            cluster({
                providers: hub.providers(),
                advertise: `http://127.0.0.1:${port}`,
                secret: 's'
            })
        );
        // Mounted but never started — the ordinary ordering for a cluster
        // host, whose listener must exist before it can advertise.
        const handler = createAppHandler(app, { origin: false });
        server.on('request', (req, res) => {
            void handler(req, res, () => res.writeHead(404).end());
        });
        try {
            const response = await fetch(
                `http://127.0.0.1:${port}/_sigx/actor/Counter%23increment`,
                {
                    method: 'POST',
                    headers: { 'content-type': 'application/json' },
                    body: JSON.stringify({ args: ['k', 1] })
                }
            );
            expect(response.status).toBe(503);
            await expect(response.json()).resolves.toMatchObject({
                error: { status: 503 }
            });
        } finally {
            await new Promise<void>((resolve) => server.close(() => resolve()));
        }
    });

    it('does not seal the app when the handler is built', async () => {
        const app = defineActorApp({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: quiet
        });
        // Building the handler must not freeze plugin registration: reading
        // `app.routes` seals the app, and mounting before `.use()` is a
        // legitimate order (a listener often has to exist first).
        createAppHandler(app);
        expect(() =>
            app.use({
                name: 'late-but-legal',
                setup: () => {}
            })
        ).not.toThrow();
        await app.stop();
    });

    it('gives route matching the request headers', async () => {
        const storage = memoryStorage();
        const seen: string[] = [];
        const nodes = await serveCluster(1, storage, {
            extraPlugin: {
                name: 'header-route',
                setup(registry) {
                    registry.route({
                        name: 'header-route',
                        // A route keyed on a HEADER, not a path.
                        match: (request) => {
                            seen.push(request.headers.get('x-probe') ?? '(none)');
                            return request.headers.get('x-probe') === 'yes';
                        },
                        handle: async () => new Response('matched')
                    });
                }
            }
        });
        running = nodes;

        const hit = await fetch(`http://127.0.0.1:${nodes[0]!.port}/anything`, {
            headers: { 'x-probe': 'yes' }
        });
        expect(await hit.text()).toBe('matched');
        expect(seen).toContain('yes');
    });

    it('falls through to next() for an unrelated path', async () => {
        const storage = memoryStorage();
        const nodes = await serveCluster(1, storage);
        running = nodes;

        const response = await fetch(`http://127.0.0.1:${nodes[0]!.port}/nothing/here`);
        expect(response.status).toBe(404);
    });
});

describe('createFetchHandler (WinterCG)', () => {
    it('serves the actor endpoint, plugin routes, and a 404 fallback', async () => {
        const hub = memoryClusterHub();
        const app = defineActorApp({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: quiet
        })
            .use(
                cluster({
                    providers: hub.providers(),
                    advertise: 'http://fetch.test',
                    secret: 's'
                })
            )
            .use({
                name: 'health',
                setup: (r) =>
                    r.route({
                        name: 'health',
                        match: (request) => new URL(request.url).pathname === '/healthz',
                        handle: async () => new Response('ok')
                    })
            });
        await app.start();
        const handler = createFetchHandler(app, { origin: false });
        try {
            // public endpoint
            const call = await handler(
                new Request('http://fetch.test/_sigx/actor/Counter%23increment', {
                    method: 'POST',
                    headers: { 'content-type': 'application/json' },
                    body: JSON.stringify({ args: ['k', 3] })
                })
            );
            await expect(call.json()).resolves.toEqual({ data: 3 });

            // a plugin route
            const health = await handler(new Request('http://fetch.test/healthz'));
            expect(await health.text()).toBe('ok');

            // the cluster's internal mount is a plugin route too — it exists,
            // and refuses an unauthenticated call rather than 404ing.
            const internal = await handler(
                new Request('http://fetch.test/_sigx/host/Counter%23increment', {
                    method: 'POST',
                    headers: { 'content-type': 'application/json' },
                    body: JSON.stringify({ args: ['k'] })
                })
            );
            expect(internal.status).toBe(403);

            // anything else
            const missing = await handler(new Request('http://fetch.test/nope'));
            expect(missing.status).toBe(404);
        } finally {
            await app.stop({ timeoutMs: 1000 });
        }
    });

    it('answers 503 before the app is started', async () => {
        const app = defineActorApp({ actors: [Counter], storage: memoryStorage(), defaults: quiet });
        const response = await createFetchHandler(app)(new Request('http://fetch.test/nope'));
        expect(response.status).toBe(503);
    });

    it('honours maxLiveSubscriptions — the Node path must not drop a security bound', async () => {
        // `ActorHandlerOptions extends ActorResolverOptions`, so this option
        // TYPE-CHECKS on createAppHandler/createActorHandler. It was landing in
        // the `...rest` spread and going to the serverFn handler, which ignores
        // it — so the cap silently did not apply on the plain Node deployment
        // path. A bound that type-checks and does nothing is worse than one
        // that is missing.
        const app = defineActorApp({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: quiet
        });
        // The NODE handler specifically — `createFetchHandler` (WinterCG)
        // already threaded this, which is exactly what made the gap easy to
        // miss: the option worked on one mount and silently did not on the
        // other.
        const server = createServer();
        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        const port = typeof address === 'object' && address ? address.port : 0;
        server.on('request', createAppHandler(app, { origin: false, maxLiveSubscriptions: 2 }));
        await app.start();
        try {
            const over = await fetch(
                `http://127.0.0.1:${port}/_sigx/actor/${encodeURIComponent('$live#subscribe')}`,
                {
                    method: 'POST',
                    headers: { 'content-type': 'application/json' },
                    body: JSON.stringify({
                        args: [
                            [
                                { t: 'Counter', k: 'a', m: 'increment' },
                                { t: 'Counter', k: 'b', m: 'increment' },
                                { t: 'Counter', k: 'c', m: 'increment' }
                            ]
                        ]
                    })
                }
            );
            expect(over.status).toBe(400);
            expect(await over.text()).toMatch(/too many subscriptions/i);
        } finally {
            await app.stop({ timeoutMs: 1000 });
            await new Promise<void>((resolve) => server.close(() => resolve()));
        }
    });
});
