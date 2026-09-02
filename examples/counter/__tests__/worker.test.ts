/**
 * The property `worker-demo.mjs` narrates, pinned structurally rather than
 * by wall clock: two concurrent calls to one key overlap under
 * `defineWorker` and serialize under `defineActor`; the pool grows only
 * under pressure and stops at `maxLocal`.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActorApp, memoryStorage, type Host } from '@sigx/actors/host';
import { Resolver, SerialResolver } from '../src/resolver.actor.ts';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };
const LATENCY = 30;

const running: Host[] = [];
async function start() {
    const host = await defineActorApp({
        actors: [Resolver, SerialResolver],
        storage: memoryStorage(),
        defaults: quiet
    }).start();
    running.push(host);
    return host;
}
afterEach(async () => {
    for (const host of running.splice(0)) await host.stop({ timeoutMs: 1000 });
});

describe('defineWorker beside defineActor', () => {
    it('an actor serializes two concurrent calls to one key', async () => {
        const host = await start();
        const client = host.actor(SerialResolver, 'k');
        const [a, b] = await Promise.all([client.lookup(LATENCY), client.lookup(LATENCY)]);
        expect([a.overlapping, b.overlapping]).toEqual([0, 0]);
        expect(host.stats().perType['SerialResolver']).toBe(1);
    });

    it('a worker pool overlaps them, on two members of the same key', async () => {
        const host = await start();
        const client = host.actor(Resolver, 'k');
        const [a, b] = await Promise.all([client.lookup(LATENCY), client.lookup(LATENCY)]);
        // Exactly one of them found the other already inside.
        expect(a.overlapping + b.overlapping).toBe(1);
        expect(a.member).not.toBe(b.member);
        expect(host.stats().perType['Resolver']).toBe(2);
    });

    it('grows under pressure to maxLocal and no further; the excess queues', async () => {
        const host = await start();
        const results = await Promise.all(
            Array.from({ length: 8 }, () => host.actor(Resolver, 'k').lookup(LATENCY))
        );
        expect(new Set(results.map((r) => r.member)).size).toBe(4);
        expect(Math.max(...results.map((r) => r.overlapping))).toBe(3); // peak 4 in flight
        expect(host.stats().perType['Resolver']).toBe(4);
    });

    it('sequential calls never grow the pool', async () => {
        const host = await start();
        const seen = new Set<number>();
        for (let i = 0; i < 3; i++) {
            seen.add((await host.actor(Resolver, 'quiet').lookup(LATENCY)).member);
        }
        expect(seen.size).toBe(1);
        expect(host.stats().perType['Resolver']).toBe(1);
    });
});
