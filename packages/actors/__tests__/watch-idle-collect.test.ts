/**
 * Does the idle timer collect an actor whose watch consumer has gone?
 *
 * The virtual-actor model is that an actor outlives its last caller and deactivates
 * on an idle timer — it does NOT die the moment a client disconnects. This
 * runtime copies that (`idleAfterMs`).
 *
 * So the question is not "was the actor collected immediately", it is whether
 * a departed consumer leaves the activation ELIGIBLE for that timer at all.
 * `keptAlive` does not delay collection, it exempts from it:
 *
 *     if (a.idle && !a.keptAlive && now - a.lastActivityMs >= idleAfter)
 *
 * A control case with no stream at all pins the timer itself, so a failure
 * here cannot be blamed on the sweeper being misconfigured.
 */
import { describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import { createHost, manualScheduler, memoryStorage } from '@sigx/actors/host';
import { handleActorRequest } from '@sigx/actors/server';

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
            yield* ctx.changes({ initial: true });
        }
    })
});

/** A host whose idle collection can be driven exactly. */
function host() {
    const scheduler = manualScheduler();
    return {
        scheduler,
        s: createHost({
            actors: [Counter],
            storage: memoryStorage(),
            scheduler,
            // Collect anything idle, as soon as the sweeper runs.
            defaults: { idleAfterMs: 0, sweepIntervalMs: 1_000, callTimeoutMs: 0 }
        })
    };
}

const call = (symbol: string, args: readonly unknown[]): Request =>
    new Request(`https://host.invalid/_sigx/actor/${encodeURIComponent(symbol)}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ args })
    });

/** Let queued mailbox turns settle — timer callbacks run as turns. */
async function drain(): Promise<void> {
    for (let i = 0; i < 10; i++) await new Promise((r) => setTimeout(r, 0));
}

describe('idle collection after a watch consumer leaves', () => {
    it('CONTROL: collects a plain idle activation', async () => {
        // Proves the sweeper and the timer work at all, so the case below
        // cannot be dismissed as a misconfigured test.
        const { scheduler, s } = host();
        await s.start();
        try {
            await handleActorRequest(call('Counter#increment', ['plain', 1]), {
                host: s,
                origin: false
            });
            expect(s.stats().activations).toBe(1);

            scheduler.advance(1_000);
            await drain();
            expect(s.stats().activations).toBe(0);
        } finally {
            await s.stop({ timeoutMs: 1_000 });
        }
    });

    it('collects the actor after its watch consumer has gone', async () => {
        const { scheduler, s } = host();
        await s.start();
        try {
            const res = await handleActorRequest(call('Counter#watch', ['watched']), {
                host: s,
                origin: false
            });
            const reader = res.body!.getReader();
            const decoder = new TextDecoder();
            let buffer = '';
            while (!buffer.includes('\n')) {
                const { value, done } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
            }
            expect(s.stats().activations).toBe(1);

            // The consumer goes away.
            await reader.cancel();
            await drain();

            // Now let the idle timer run — generously, many periods.
            for (let i = 0; i < 5; i++) {
                scheduler.advance(1_000);
                await drain();
            }

            // Idle collection: not instant, but the timer must eventually get it.
            expect(s.stats().activations).toBe(0);
        } finally {
            await s.stop({ timeoutMs: 1_000 });
        }
    });
});
