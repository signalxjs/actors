/**
 * The contract `topics-demo.mjs` narrates, pinned: a turn's `ctx.publish`
 * reaches a `subscriptions:` handler on another type with no framework in
 * the way, the delivery activates an idle subscriber, a publish from
 * outside any actor rides the same fan-out, and a deploy without the
 * subscriber reports zero rather than failing.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActorApp, memoryStorage, type Host } from '@sigx/actors/host';
import { Gate, gatePassed } from '../src/gate.actor.ts';
import { Tally } from '../src/tally.actor.ts';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const running: Host[] = [];
async function start(actors: Parameters<typeof defineActorApp>[0]['actors']) {
    const host = await defineActorApp({ actors, storage: memoryStorage(), defaults: quiet }).start();
    running.push(host);
    return host;
}
afterEach(async () => {
    for (const host of running.splice(0)) await host.stop({ timeoutMs: 1000 });
});

describe('topics without a framework', () => {
    it('a publish from a turn is delivered to the aggregate subscriber', async () => {
        const host = await start([Gate, Tally]);
        const report = await host.actor(Gate, 'north').pass();
        expect(report).toMatchObject({ count: 1, subscribers: 1, delivered: 1, failures: [] });
        await host.actor(Gate, 'north').pass();
        await host.actor(Gate, 'south').pass();
        // Every gate's key was mapped to the one instance.
        const tally = await host.actor(Tally, 'all').totals();
        expect(tally).toEqual({
            deliveries: 3,
            byGate: { north: 2, south: 1 },
            lastFrom: 'Gate/south'
        });
    });

    it('the delivery activates the subscriber — nothing else called it', async () => {
        const host = await start([Gate, Tally]);
        await host.actor(Gate, 'north').pass();
        // Read the stats BEFORE touching Tally, so the read cannot be what
        // activated it.
        expect(host.stats().perType).toEqual({ Gate: 1, Tally: 1 });
    });

    it('host.publish() from outside any actor reaches the same handler, unattributed', async () => {
        const host = await start([Gate, Tally]);
        const report = await host.publish(gatePassed('side-door'), { count: 7 });
        expect(report).toEqual({ subscribers: 1, delivered: 1, failures: [] });
        const tally = await host.actor(Tally, 'all').totals();
        expect(tally).toEqual({
            deliveries: 1,
            byGate: { 'side-door': 7 },
            lastFrom: 'outside any actor'
        });
    });

    it('a deploy without the subscriber delivers to nobody and does not throw', async () => {
        const host = await start([Gate]);
        const report = await host.actor(Gate, 'north').pass();
        expect(report).toMatchObject({ count: 1, subscribers: 0, delivered: 0, failures: [] });
        expect(host.stats().perType).toEqual({ Gate: 1 });
    });
});
