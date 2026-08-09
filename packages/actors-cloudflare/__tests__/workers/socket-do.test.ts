/**
 * The OBJECT-terminated client socket (#158) on real workerd: the Worker
 * forwards `/_sigx/socket/{type}/{key}` to that actor's Durable Object,
 * which accepts it with the hibernation API — so the session lives where
 * the actor lives, and a disconnect releases the activation locally
 * instead of dying at the `stub.fetch` boundary (#47).
 *
 * The release assertions themselves live in `disconnect.test.ts`, which
 * was written as the acceptance criteria for exactly this feature and is
 * unskipped by the same PR. This file covers the plumbing around them.
 */
import { env, runInDurableObject, SELF } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import type { Env } from './fixture-worker';

declare module 'cloudflare:test' {
    interface ProvidedEnv extends Env {}
}

const SEP = '\u0000';

type Reply = { i?: number; v?: unknown; d?: 1; e?: { message: string; status: number }; p?: 1 };

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

async function dial(type: string, key: string): Promise<Response> {
    return SELF.fetch(
        `https://edge.test/_sigx/socket/${encodeURIComponent(type)}/${encodeURIComponent(key)}`,
        { headers: { Upgrade: 'websocket' } }
    );
}

async function open(type: string, key: string): Promise<WebSocket> {
    const res = await dial(type, key);
    expect(res.status).toBe(101);
    const ws = res.webSocket;
    if (!ws) throw new Error('no webSocket on the 101 response');
    ws.accept();
    return ws;
}

/** `keptAlive` for the actor this object hosts, read from inside it. */
async function keptAlive(key: string): Promise<boolean | undefined> {
    const stub = env.ACTORS.get(env.ACTORS.idFromName(`Counter${SEP}${key}`));
    return runInDurableObject(stub, async (instance) => {
        const host = await (instance as { host(): Promise<import('@sigx/actors').Host> }).host();
        return host.activations().find((a) => a.key === key)?.keptAlive;
    });
}

describe('object-terminated socket (#158)', () => {
    it('upgrades through the Worker into the object and round-trips a call', async () => {
        const ws = await open('Counter', 'do-unary');
        const inbound = frames(ws);
        ws.send(JSON.stringify({ i: 1, s: 'Counter#increment', a: ['do-unary', 3] }));
        expect(await inbound.next((r) => r.i === 1 && 'v' in r)).toEqual({ i: 1, v: 3 });
        expect(await inbound.next((r) => r.i === 1 && r.d === 1)).toEqual({ i: 1, d: 1 });
        ws.close();
    });

    it('holds keptAlive for a live subscription, and releases it on close', async () => {
        // The headline. Worker-terminated cannot pass this (#47); the whole
        // point of terminating in the object is that it can.
        const ws = await open('Counter', 'do-room');
        const inbound = frames(ws);
        ws.send(JSON.stringify({ i: 2, sub: { t: 'Counter', k: 'do-room', m: 'read' } }));
        await inbound.next((r) => r.i === 2 && 'v' in r);
        expect(await keptAlive('do-room')).toBe(true);

        ws.close();
        let seen: boolean | undefined;
        for (let i = 0; i < 40; i++) {
            seen = await keptAlive('do-room');
            if (seen === false) break;
            await scheduler.wait(50);
        }
        expect(seen).toBe(false);
    });

    it('pushes updates driven from the OTHER transport', async () => {
        const ws = await open('Counter', 'do-live');
        const inbound = frames(ws);
        ws.send(JSON.stringify({ i: 3, sub: { t: 'Counter', k: 'do-live', m: 'read' } }));
        const seed = (await inbound.next((r) => r.i === 3 && 'v' in r)) as { v: number };
        const res = await SELF.fetch('https://edge.test/_sigx/actor/Counter/increment', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args: ['do-live', 1] })
        });
        expect(res.ok).toBe(true);
        const pushed = (await inbound.next((r) => r.i === 3 && 'v' in r)) as { v: number };
        expect(pushed.v).toBe(seed.v + 1);
        ws.close();
    });

    it('answers the {"p":1} keepalive without involving the session', async () => {
        // setWebSocketAutoResponse — which is what lets a hibernated object
        // stay asleep under client pings.
        const ws = await open('Counter', 'do-ping');
        const inbound = frames(ws);
        ws.send('{"p":1}');
        expect(await inbound.next((r) => r.p === 1)).toEqual({ p: 1 });
        ws.close();
    });

    it('closes 1003 on a binary frame', async () => {
        const ws = await open('Counter', 'do-binary');
        const inbound = frames(ws);
        ws.send(new Uint8Array([1, 2, 3]));
        const closed = await inbound.closed;
        expect(closed.code).toBe(1003);
    });

    it('404s an unknown actor type without minting an object', async () => {
        const res = await dial('Nope', 'k');
        expect(res.status).toBe(404);
    });

    it('404s a malformed actor path', async () => {
        const res = await SELF.fetch('https://edge.test/_sigx/socket/Counter/a/b', {
            headers: { Upgrade: 'websocket' }
        });
        // Three segments parse as neither termination mode; the app answers.
        expect(res.status).toBe(404);
    });

    it('keeps the Worker-terminated mount working beside it', async () => {
        // The two modes share a path prefix and are disambiguated by arity —
        // prove the coexistence rather than assert it in prose.
        const res = await SELF.fetch('https://edge.test/_sigx/socket', {
            headers: { Upgrade: 'websocket' }
        });
        expect(res.status).toBe(101);
        res.webSocket?.accept();
        res.webSocket?.close();
    });
});
