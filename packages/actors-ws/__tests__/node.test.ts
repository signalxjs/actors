/**
 * `attachActorSocket()` end to end: a REAL `node:http` server, a REAL `ws`
 * upgrade, a REAL loopback socket — the one place the whole chain
 * (`socketTransport` → WebSocket → upgrade → session → host) runs with no
 * fakes anywhere.
 *
 * The node environment, NOT the suite-default happy-dom: this adapter runs
 * on servers, and happy-dom's `Request` enforces the browser's
 * forbidden-header rules — it silently drops `Origin`, which would refuse
 * every same-origin upgrade here and test the mock instead of the adapter.
 *
 * @vitest-environment node
 */
import { afterEach, describe, expect, it } from 'vitest';
import { createServer, type Server } from 'node:http';
import { WebSocket as WsClient } from 'ws';
import { defineActor } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import { attachActorSocket, DEFAULT_SOCKET_PATH } from '@sigx/actors-ws/node';
import { socketTransport, type SocketLink } from '@sigx/actors-ws/client';
import type { ActorTransport } from '@sigx/actors/client';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const Cart = defineActor({
    type: 'Cart',
    allowAnonymous: true,
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async add(item: string) {
            ctx.state.items.push(item);
            await ctx.save();
            return ctx.state.items.length;
        },
        async total() {
            return ctx.state.items.length;
        }
    }),
    streams: () => ({
        async *countTo(to: number) {
            for (let i = 1; i <= to; i++) yield i;
        }
    })
});

let host: Host | null = null;
let server: Server | null = null;
const transports: ActorTransport[] = [];

afterEach(async () => {
    for (const t of transports) void t.close?.();
    transports.length = 0;
    await new Promise<void>((resolve) => (server ? server.close(() => resolve()) : resolve()));
    server = null;
    await host?.stop();
    host = null;
});

async function listen(origin: Parameters<typeof attachActorSocket>[1]['origin']): Promise<number> {
    host = createHost({ actors: [Cart], defaults: quiet });
    await host.start();
    server = createServer((_req, res) => res.end('no'));
    attachActorSocket(server, { host, origin, pingMs: 0 });
    await new Promise<void>((resolve) => server!.listen(0, '127.0.0.1', resolve));
    return (server.address() as { port: number }).port;
}

/** Dial with `ws` (the runner's global WebSocket is happy-dom's browser
 *  mock), optionally carrying an Origin header like a browser would. */
function dial(port: number, origin?: string): ActorTransport {
    const transport = socketTransport({
        connect: (handlers) => {
            const ws = new WsClient(
                `ws://127.0.0.1:${port}${DEFAULT_SOCKET_PATH}`,
                origin ? { headers: { origin } } : {}
            );
            ws.on('open', () => handlers.onOpen());
            ws.on('message', (data) => handlers.onMessage(String(data)));
            ws.on('close', () => handlers.onClose());
            ws.on('error', () => {});
            const link: SocketLink = {
                send: (message) => ws.send(message),
                close: () => ws.close()
            };
            return link;
        }
    });
    transports.push(transport);
    return transport;
}

describe('attachActorSocket over real sockets', () => {
    it('carries unary calls, streams and live end to end', async () => {
        const port = await listen(false);
        const transport = dial(port);
        expect(await transport.call('Cart#add', ['k1', 'apple'])).toBe(1);
        const seen: unknown[] = [];
        for await (const value of transport.stream('Cart#countTo', ['k1', 3])) {
            seen.push(value);
        }
        expect(seen).toEqual([1, 2, 3]);
        const live: unknown[] = [];
        const off = transport.live!().subscribe(
            { type: 'Cart', key: 'k1', method: 'total' },
            (value) => live.push(value)
        );
        await expect.poll(() => live.length).toBe(1);
        await transport.call('Cart#add', ['k1', 'pear']);
        await expect.poll(() => live.includes(2)).toBe(true);
        off();
    });

    it("enforces the origin posture at upgrade — the browser's cookies ride it", async () => {
        const port = await listen('same-origin');
        // A matching Origin upgrades…
        const good = dial(port, `http://127.0.0.1:${port}`);
        expect(await good.call('Cart#total', ['gk'])).toBe(0);
        // …a cross-site one is refused before a single message is read.
        const bad = dial(port, 'http://evil.test');
        await expect(bad.call('Cart#total', ['k'])).rejects.toMatchObject({ status: 0 });
    });

    it('leaves other upgrade paths alone when another listener exists', async () => {
        const port = await listen(false);
        let othersSaw = 0;
        server!.on('upgrade', (req, socket) => {
            if (req.url === '/other') {
                othersSaw += 1;
                socket.destroy();
            }
        });
        const ws = new WsClient(`ws://127.0.0.1:${port}/other`);
        ws.on('error', () => {});
        await expect.poll(() => othersSaw).toBe(1);
    });
});
