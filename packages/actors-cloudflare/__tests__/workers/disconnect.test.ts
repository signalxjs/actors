/**
 * Does a client going away release the Durable Object?
 *
 * This is the load-bearing question for the come-and-go shape — "player
 * rooms", where clients join a room, stream its state, and leave. Creating a
 * room is free and fan-out is shared, so the whole economic story rests on an
 * EMPTY room costing nothing. That only holds if a disconnect actually
 * propagates:
 *
 *   client goes → socket close → webSocketClose IN THE OBJECT → session
 *   teardown → generator return() → subscriber removed → count hits zero →
 *   keptAlive released → object goes idle → evicted
 *
 * These four tests were written as the acceptance criteria for #47 while the
 * only path was HTTP streams through the Worker — where the chain is correct
 * right up to the `stub.fetch` boundary, which swallows the abort (measured
 * in `stubsignal.test.ts`, still skipped as evidence). The OBJECT-terminated
 * socket (#158) is what unskipped them: the session lives in the object, so
 * the teardown never has to cross that boundary at all. The HTTP-stream leak
 * remains real, which is why #47 stays open.
 *
 * `ActivationInfo.keptAlive` is the observable: "Held open — by a stream, a
 * watch, or a running detached task — so idle sweeping skips it."
 */
import { env, runInDurableObject, SELF } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import type { Env } from './fixture-worker';

declare module 'cloudflare:test' {
    interface ProvidedEnv extends Env {}
}

const SEP = '\u0000';

function stubFor(key: string) {
    return env.ACTORS.get(env.ACTORS.idFromName(`Counter${SEP}${key}`));
}

/** `keptAlive` for the actor this object hosts, read from inside it. */
async function keptAlive(key: string): Promise<boolean | undefined> {
    return runInDurableObject(stubFor(key), async (instance) => {
        const host = await (instance as { host(): Promise<import('@sigx/actors').Host> }).host();
        return host.activations().find((a) => a.key === key)?.keptAlive;
    });
}

/** A live subscription over the object-terminated socket, proven live by its
 *  seed frame — genuinely established on the object, not merely requested. */
interface Player {
    read(): Promise<unknown>;
    leave(): void;
}

async function join(key: string): Promise<Player> {
    const res = await SELF.fetch(`https://edge.test/_sigx/socket/Counter/${key}`, {
        headers: { Upgrade: 'websocket' }
    });
    expect(res.status).toBe(101);
    const ws = res.webSocket;
    if (!ws) throw new Error('no webSocket on the 101 response');
    ws.accept();

    const values: unknown[] = [];
    const waiters: ((value: unknown) => void)[] = [];
    ws.addEventListener('message', (event) => {
        const frame = JSON.parse(event.data as string) as { i?: number; v?: unknown };
        if (frame.i !== 1 || !('v' in frame)) return;
        const waiter = waiters.shift();
        if (waiter) waiter(frame.v);
        else values.push(frame.v);
    });
    const read = (): Promise<unknown> => {
        if (values.length > 0) return Promise.resolve(values.shift());
        return new Promise((resolve) => waiters.push(resolve));
    };

    ws.send(JSON.stringify({ i: 1, sub: { t: 'Counter', k: key, m: 'read' } }));
    await read(); // the seed
    return { read, leave: () => ws.close() };
}

/** `keptAlive` is cleared asynchronously, so give the teardown a moment. */
async function keptAliveSettles(key: string, want: boolean): Promise<boolean | undefined> {
    let seen: boolean | undefined;
    for (let i = 0; i < 40; i++) {
        seen = await keptAlive(key);
        if (seen === want) return seen;
        await scheduler.wait(50);
    }
    return seen;
}

describe('a disconnected watch releases the object', () => {
    it('holds the activation open while a watch is live', async () => {
        const player = await join('room-a');
        try {
            expect(await keptAlive('room-a')).toBe(true);
        } finally {
            player.leave();
        }
    });

    it('RELEASES it when the client goes away', async () => {
        // The question this file exists for: an empty room must not bill
        // indefinitely.
        const player = await join('room-b');
        expect(await keptAlive('room-b')).toBe(true);

        player.leave();

        expect(await keptAliveSettles('room-b', false)).toBe(false);
    });

    it('keeps the shared loop for the players who stayed', async () => {
        // Subscribers to one read share a single loop, so removing one must
        // not tear it down for the rest — the room survives a player leaving.
        const first = await join('room-c');
        const second = await join('room-c');
        expect(await keptAlive('room-c')).toBe(true);

        first.leave();
        // Still held: one player remains.
        expect(await keptAlive('room-c')).toBe(true);

        // And the survivor still receives changes.
        const res = await SELF.fetch(`https://edge.test/_sigx/actor/Counter/increment`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args: ['room-c', 1] })
        });
        expect(res.ok).toBe(true);
        await second.read();

        second.leave();
        // Empty room: released.
        expect(await keptAliveSettles('room-c', false)).toBe(false);
    });

    it('lets a room be rejoined after it emptied', async () => {
        const first = await join('room-d');
        first.leave();
        expect(await keptAliveSettles('room-d', false)).toBe(false);

        // Coming back must work — the teardown must not have poisoned the
        // shared-watch entry for that (method, args).
        const again = await join('room-d');
        try {
            expect(await keptAlive('room-d')).toBe(true);
        } finally {
            again.leave();
        }
    });
});
