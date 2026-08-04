/**
 * Does cancelling a watch response release the activation, with NO Cloudflare
 * anywhere in the picture?
 *
 * This exists to localise a failure found on Workers: after a client cancels a
 * watch, `keptAlive` stayed true and the activation was never released. That
 * chain crosses several links, so this asks the simplest possible version of
 * the question — a plain host behind the public endpoint — to tell a core
 * problem apart from one introduced by the Worker→Durable-Object hop.
 */
import { describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import { createHost, manualScheduler, memoryStorage } from '@sigx/actors/host';
import { handleActorRequest } from '@sigx/actors/server';
import { relayStream } from '../src/stream-relay';

const Counter = defineActor({
    type: 'Counter',
    allowAnonymous: true,
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
            yield* ctx.changes({ initial: true });
        }
    })
});

function host() {
    return createHost({
        actors: [Counter],
        storage: memoryStorage(),
        scheduler: manualScheduler(),
        defaults: { sweepIntervalMs: 0, callTimeoutMs: 0 }
    });
}

const call = (symbol: string, args: readonly unknown[], signal?: AbortSignal): Request =>
    new Request(`https://host.invalid/_sigx/actor/${encodeURIComponent(symbol)}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ args }),
        ...(signal ? { signal } : {})
    });

/** Poll `keptAlive` until it settles, or give up. */
async function settles(
    read: () => boolean | undefined,
    want: boolean
): Promise<boolean | undefined> {
    let seen = read();
    for (let i = 0; i < 40 && seen !== want; i++) {
        await new Promise((r) => setTimeout(r, 25));
        seen = read();
    }
    return seen;
}

describe('relayStream', () => {
    it('resolves a parked next() as done when return() closes underneath it', async () => {
        // The common disconnect shape: a consumer is parked on `next()` when
        // the host closes the stream, and `onClose` wakes it by aborting.
        // That rejection is the teardown WORKING — surfacing it would turn a
        // deliberate close into an unhandled rejection.
        const ac = new AbortController();
        const inner: AsyncIterableIterator<unknown> = {
            [Symbol.asyncIterator]() {
                return this;
            },
            next: () =>
                new Promise<IteratorResult<unknown>>((_resolve, reject) => {
                    ac.signal.addEventListener('abort', () => reject(new Error('aborted')), {
                        once: true
                    });
                }),
            return: async () => ({ done: true, value: undefined })
        };

        const relay = relayStream(inner, {
            mapError: (e) => e,
            onClose: () => ac.abort()
        });

        const parked = relay.next();
        await new Promise((r) => setTimeout(r, 0));
        await relay.return!();

        // Resolves, rather than rejecting with the abort we caused.
        await expect(parked).resolves.toEqual({ done: true, value: undefined });
    });

    it('still reports a spontaneous inner failure', async () => {
        // The contrast: an error the consumer did NOT cause must reach them.
        const inner: AsyncIterableIterator<unknown> = {
            [Symbol.asyncIterator]() {
                return this;
            },
            next: async () => {
                throw new Error('actor exploded');
            },
            return: async () => ({ done: true, value: undefined })
        };
        const relay = relayStream(inner, { mapError: (e) => e });
        await expect(relay.next()).rejects.toThrow(/actor exploded/);
    });
});

describe('cancelling a watch, over the public endpoint, no Cloudflare', () => {
    it('releases the activation when the REQUEST is aborted', async () => {
        // What a real client disconnect looks like: the inbound request is
        // aborted. This is the trigger that matters in production.
        const s = host();
        await s.start();
        try {
            const ac = new AbortController();
            const res = await handleActorRequest(call('Counter#watch', ['r'], ac.signal), {
                host: s,
                origin: false
            });
            expect(res.status).toBe(200);
            const reader = res.body!.getReader();
            const decoder = new TextDecoder();
            let buffer = '';
            while (!buffer.includes('\n')) {
                const { value, done } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
            }
            const held = () => s.activations().find((a) => a.key === 'r')?.keptAlive;
            expect(held()).toBe(true);

            ac.abort();

            expect(await settles(held, false)).toBe(false);
        } finally {
            await s.stop({ timeoutMs: 1_000 });
        }
    });

    it('releases the activation when the consumer cancels', async () => {
        const s = host();
        await s.start();
        try {
            const res = await handleActorRequest(call('Counter#watch', ['a']), {
                host: s,
                origin: false
            });
            expect(res.status).toBe(200);

            const reader = res.body!.getReader();
            const decoder = new TextDecoder();
            let buffer = '';
            while (!buffer.includes('\n')) {
                const { value, done } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
            }
            expect(buffer).toContain('"chunk"');

            const held = () => s.activations().find((a) => a.key === 'a')?.keptAlive;
            expect(held()).toBe(true);

            await reader.cancel();

            // Give the teardown a chance; it is asynchronous.
            let seen = held();
            for (let i = 0; i < 40 && seen !== false; i++) {
                await new Promise((r) => setTimeout(r, 25));
                seen = held();
            }
            expect(seen).toBe(false);
        } finally {
            await s.stop({ timeoutMs: 1_000 });
        }
    });
});
