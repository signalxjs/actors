/**
 * Does a client going away release the Durable Object?
 *
 * This is the load-bearing question for the come-and-go shape — "player
 * rooms", where clients join a room, stream its state, and leave. Creating a
 * room is free and fan-out is shared, so the whole economic story rests on an
 * EMPTY room costing nothing. That only holds if a disconnect actually
 * propagates:
 *
 *   client goes → Worker request aborts → stub fetch signal → object's
 *   request aborts → generator return() → subscriber removed → count hits
 *   zero → keptAlive released → object goes idle → evicted
 *
 * Core has the machinery (#120). What is unproven is whether the signal
 * crosses the Worker→stub boundary at all. If it does not, a closed tab
 * leaves a subscriber attached forever, `keptAlive` never clears, and you pay
 * for empty rooms indefinitely — invisible until the bill.
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
        const silo = await (instance as { silo(): Promise<import('@sigx/actors').Silo> }).silo();
        return silo.activations().find((a) => a.key === key)?.keptAlive;
    });
}

/** Open a watch through the Worker and read its first frame, proving it live. */
async function openWatch(key: string): Promise<ReadableStreamDefaultReader<Uint8Array>> {
    const res = await SELF.fetch(
        `https://edge.test/_sigx/actor/${encodeURIComponent('Counter#watch')}`,
        {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args: [key] })
        }
    );
    expect(res.ok).toBe(true);
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (!buffer.includes('\n')) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
    }
    // A frame arrived, so the subscription is genuinely established on the
    // object rather than merely requested.
    expect(buffer).toContain('"chunk"');
    return reader;
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

// SKIPPED, and not because the tests are wrong: `stub.fetch(url, { signal })`
// does not propagate cancellation into a Durable Object — measured directly in
// `stubsignal.test.ts`. #184 fixed the three queued-`return()` layers above the
// boundary, so the chain is correct right up to it and the boundary swallows
// the signal. These are the acceptance criteria for #187: unskip them when it
// lands.
describe.skip('a disconnected watch releases the object', () => {
    it('holds the activation open while a watch is live', async () => {
        const reader = await openWatch('room-a');
        try {
            expect(await keptAlive('room-a')).toBe(true);
        } finally {
            await reader.cancel();
        }
    });

    it('RELEASES it when the client goes away', async () => {
        // The question this file exists for. If the abort does not cross the
        // Worker→stub hop, this stays true forever and an empty room bills
        // indefinitely.
        const reader = await openWatch('room-b');
        expect(await keptAlive('room-b')).toBe(true);

        await reader.cancel();

        expect(await keptAliveSettles('room-b', false)).toBe(false);
    });

    it('keeps the shared loop for the players who stayed', async () => {
        // Fifty subscribers to one read share a single loop, so removing one
        // must not tear it down for the rest — the room survives a player
        // leaving.
        const first = await openWatch('room-c');
        const second = await openWatch('room-c');
        expect(await keptAlive('room-c')).toBe(true);

        await first.cancel();
        // Still held: one player remains.
        expect(await keptAlive('room-c')).toBe(true);

        // And the survivor still receives changes.
        const res = await SELF.fetch(
            `https://edge.test/_sigx/actor/${encodeURIComponent('Counter#increment')}`,
            {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ args: ['room-c', 1] })
            }
        );
        expect(res.ok).toBe(true);
        const decoder = new TextDecoder();
        let buffer = '';
        while (!buffer.includes('\n')) {
            const { value, done } = await second.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
        }
        expect(buffer).toContain('"chunk"');

        await second.cancel();
        // Empty room: released.
        expect(await keptAliveSettles('room-c', false)).toBe(false);
    });

    it('lets a room be rejoined after it emptied', async () => {
        const first = await openWatch('room-d');
        await first.cancel();
        expect(await keptAliveSettles('room-d', false)).toBe(false);

        // Coming back must work — the teardown must not have poisoned the
        // shared-watch entry for that (method, args).
        const again = await openWatch('room-d');
        try {
            expect(await keptAlive('room-d')).toBe(true);
        } finally {
            await again.cancel();
        }
    });
});

