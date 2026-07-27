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
 * buffering, streamed responses, and disconnect→abort. Two silos on real
 * ports do.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { createServer, type Server } from 'node:http';
import { defineActor, type ActorStorage } from '@sigx/actors';
import {
    defineActorApp,
    memoryStorage,
    type ActorApp,
    type ActorPlugin
} from '@sigx/actors/silo';
import { cluster, memoryClusterHub, preferLocalPolicy } from '@sigx/actors/cluster';
import { createAppHandler } from '@sigx/actors/node';
import { createFetchHandler } from '@sigx/actors/server';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
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

/** N silos on real localhost sockets, mounted with createAppHandler. */
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
    it('routes a cross-silo call through the plugin internal mount', async () => {
        const storage = memoryStorage();
        const nodes = await serveCluster(2, storage);
        running = nodes;

        // prefer-local puts 'a' on silo 1...
        await expect(nodes[1]!.app.silo!.actor(Counter, 'a').increment(2)).resolves.toBe(2);
        expect(nodes[1]!.app.silo!.stats().perType['Counter']).toBe(1);

        // ...so calling through silo 0 has to cross a real socket, over the
        // internal mount the cluster plugin contributed.
        await expect(nodes[0]!.app.silo!.actor(Counter, 'a').increment(3)).resolves.toBe(5);
        // Still exactly one activation, on the owner.
        expect(nodes[0]!.app.silo!.stats().activations).toBe(0);
        expect(nodes[1]!.app.silo!.stats().perType['Counter']).toBe(1);
    });

    it('streams a cross-host watch() back through the bridge', async () => {
        const storage = memoryStorage();
        const nodes = await serveCluster(2, storage);
        running = nodes;

        await nodes[1]!.app.silo!.actor(Counter, 'streamer').increment(1);

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
            for await (const state of nodes[0]!.app.silo!.actor(Counter, 'streamer').watch()) {
                seen.push((state as { count: number }).count);
                if (seen.length === 1) attached();
                if (seen.length >= 3) break;
            }
        })();

        await subscribed;
        await nodes[1]!.app.silo!.actor(Counter, 'streamer').increment(10);
        await nodes[1]!.app.silo!.actor(Counter, 'streamer').increment(100);
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

    it('rejects an oversized body with 413 before buffering it', async () => {
        const storage = memoryStorage();
        const nodes = await serveCluster(1, storage, { maxBodyBytes: 1024 });
        running = nodes;

        // Declared length is over the cap — rejected without reading.
        const declared = await fetch(
            `http://127.0.0.1:${nodes[0]!.port}/_sigx/silo/Counter%23increment`,
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
            `http://127.0.0.1:${nodes[0]!.port}/_sigx/silo/Counter%23increment`,
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
        // silo, whose listener must exist before it can advertise.
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
                new Request('http://fetch.test/_sigx/silo/Counter%23increment', {
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
});
