/**
 * The Worker-terminated client socket (#157) on real workerd: the
 * `workerSocket` route upgrading in the Worker, speaking the socket-wire
 * vocabulary, with every call fanning out through placement to the actor's
 * Durable Object.
 *
 * What is deliberately NOT asserted here: that a departed consumer releases
 * `keptAlive` in the objects it watched. It does not — the cancellation dies
 * at the `stub.fetch` boundary (#47) — and the test that pins the fix is the
 * DO-terminated socket's (#158).
 */
import { SELF } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { encodeSymbolPath } from '../../../actors/src/wire-url';
import type { Env } from './fixture-worker';

declare module 'cloudflare:test' {
    interface ProvidedEnv extends Env {}
}

type Reply = { i?: number; v?: unknown; d?: 1; e?: { message: string; status: number }; p?: 1 };

/** Parsed inbound frames plus a promise-shaped reader over them. */
function frames(ws: WebSocket): {
    next(matches: (reply: Reply) => boolean): Promise<Reply>;
    closed: Promise<{ code: number; reason: string }>;
} {
    const queue: Reply[] = [];
    const waiters: { matches: (reply: Reply) => boolean; resolve: (reply: Reply) => void }[] = [];
    ws.addEventListener('message', (event) => {
        const reply = JSON.parse(event.data as string) as Reply;
        const index = waiters.findIndex((w) => w.matches(reply));
        if (index >= 0) {
            const [waiter] = waiters.splice(index, 1);
            waiter.resolve(reply);
        } else {
            queue.push(reply);
        }
    });
    let closeResolve: (value: { code: number; reason: string }) => void;
    const closed = new Promise<{ code: number; reason: string }>((resolve) => {
        closeResolve = resolve;
    });
    ws.addEventListener('close', (event) => {
        closeResolve({ code: event.code, reason: event.reason });
    });
    return {
        next(matches) {
            const index = queue.findIndex((reply) => matches(reply));
            if (index >= 0) return Promise.resolve(queue.splice(index, 1)[0]);
            return new Promise((resolve) => {
                waiters.push({ matches, resolve });
            });
        },
        closed
    };
}

async function dial(path = '/_sigx/socket'): Promise<Response> {
    return SELF.fetch(`https://edge.test${path}`, { headers: { Upgrade: 'websocket' } });
}

async function open(path = '/_sigx/socket'): Promise<WebSocket> {
    const res = await dial(path);
    expect(res.status).toBe(101);
    const ws = res.webSocket;
    if (!ws) throw new Error('no webSocket on the 101 response');
    ws.accept();
    return ws;
}

async function invoke(symbol: string, args: readonly unknown[]): Promise<unknown> {
    const res = await SELF.fetch(`https://edge.test/_sigx/actor/${encodeSymbolPath(symbol)}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ args })
    });
    const body = (await res.json()) as { data?: unknown; error?: { message?: string } };
    if (!res.ok || body.error) throw new Error(body.error?.message ?? `HTTP ${res.status}`);
    return body.data;
}

describe('worker-terminated socket (#157)', () => {
    it('upgrades at the socket path', async () => {
        const ws = await open();
        ws.close();
    });

    it('round-trips a unary call into the Durable Object', async () => {
        const ws = await open();
        const inbound = frames(ws);
        ws.send(JSON.stringify({ i: 1, s: 'Counter#increment', a: ['sock-unary', 2] }));
        expect(await inbound.next((r) => r.i === 1 && 'v' in r)).toEqual({ i: 1, v: 2 });
        expect(await inbound.next((r) => r.i === 1 && r.d === 1)).toEqual({ i: 1, d: 1 });
        // The same state over the HTTP wire — one actor, two transports.
        await expect(invoke('Counter#read', ['sock-unary'])).resolves.toBe(2);
        ws.close();
    });

    it('multiplexes calls to different actors over one socket', async () => {
        // The socket's whole point on this backend: one client connection,
        // N objects, placement fanning each call out per dispatch.
        const ws = await open();
        const inbound = frames(ws);
        ws.send(JSON.stringify({ i: 1, s: 'Counter#increment', a: ['sock-a', 1] }));
        ws.send(JSON.stringify({ i: 2, s: 'Counter#increment', a: ['sock-b', 5] }));
        expect(await inbound.next((r) => r.i === 1 && 'v' in r)).toEqual({ i: 1, v: 1 });
        expect(await inbound.next((r) => r.i === 2 && 'v' in r)).toEqual({ i: 2, v: 5 });
        ws.close();
    });

    it('delivers a live subscription seed and a pushed update', async () => {
        const ws = await open();
        const inbound = frames(ws);
        await invoke('Counter#increment', ['sock-live', 1]);
        ws.send(JSON.stringify({ i: 3, sub: { t: 'Counter', k: 'sock-live', m: 'read' } }));
        const seed = (await inbound.next((r) => r.i === 3 && 'v' in r)) as { v: number };
        expect(seed.v).toBe(1);
        // A write from a DIFFERENT transport reaches the held watch: HTTP
        // increment → object pushes → NDJSON back to the Worker → socket.
        await invoke('Counter#increment', ['sock-live', 1]);
        const pushed = (await inbound.next((r) => r.i === 3 && 'v' in r)) as { v: number };
        expect(pushed.v).toBe(2);
        ws.send(JSON.stringify({ i: 3, uns: 1 }));
        ws.close();
    });

    it('closes 1003 on a binary frame', async () => {
        const ws = await open();
        const inbound = frames(ws);
        ws.send(new Uint8Array([1, 2, 3]));
        const closed = await inbound.closed;
        expect(closed.code).toBe(1003);
    });

    it('answers an error frame for an unknown actor, and stays open', async () => {
        const ws = await open();
        const inbound = frames(ws);
        ws.send(JSON.stringify({ i: 4, s: 'Nope#read', a: ['k'] }));
        const failed = await inbound.next((r) => r.i === 4 && 'e' in r);
        expect(failed.e?.status).toBe(404);
        // The failed call must not have cost the connection.
        ws.send(JSON.stringify({ i: 5, s: 'Counter#read', a: ['sock-unary'] }));
        await inbound.next((r) => r.i === 5 && 'v' in r);
        ws.close();
    });

    it('leaves a non-upgrade request on the path to the app', async () => {
        const res = await SELF.fetch('https://edge.test/_sigx/socket');
        // No Upgrade header → the route does not match → the app's 404, not
        // a broken 101.
        expect(res.status).toBe(404);
    });

    it('refuses a bad origin with an HTTP status, not a dead socket', async () => {
        // The strict mount runs the default 'same-origin' posture; workerd's
        // client fetch sends no Origin header, which same-origin refuses.
        const res = await SELF.fetch('https://edge.test/_sigx/socket-strict', {
            headers: { Upgrade: 'websocket' }
        });
        expect(res.status).toBe(403);
        const body = (await res.json()) as { error: { message: string } };
        expect(body.error.message).toContain('origin');
    });
});
