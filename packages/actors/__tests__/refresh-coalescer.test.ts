import { describe, expect, it } from 'vitest';
import { refreshCoalescer } from '@sigx/actors/cluster';

/**
 * Deterministic harness: refresh resolution is manual, so every interleaving
 * below is exact — no timers, no polling. `version` reflects the last
 * COMPLETED refresh, like a provider's cached view does.
 */
function harness(quietMs = 0) {
    let version = 0;
    let refreshes = 0;
    const pending: Array<{ resolve: (v: number) => void; reject: (e: unknown) => void }> = [];
    const timers: Array<{ fn: () => void; ms: number; cleared: boolean }> = [];
    const coalescer = refreshCoalescer<number>({
        refresh: () =>
            new Promise<number>((resolve, reject) => {
                refreshes++;
                pending.push({
                    resolve: (v: number) => {
                        version = v;
                        resolve(v);
                    },
                    reject
                });
            }),
        version: () => version,
        quietMs,
        setTimeoutImpl: ((fn: () => void, ms: number) => {
            const t = { fn, ms, cleared: false };
            timers.push(t);
            return t;
        }) as unknown as typeof setTimeout,
        clearTimeoutImpl: ((t: { cleared: boolean }) => {
            t.cleared = true;
        }) as unknown as typeof clearTimeout
    });
    const settle = () => new Promise<void>((r) => setTimeout(r, 0));
    return {
        coalescer,
        stats: () => ({ refreshes, inFlight: pending.length }),
        complete: (v: number) => {
            pending.shift()!.resolve(v);
            return settle();
        },
        fail: (e: unknown) => {
            pending.shift()!.reject(e);
            return settle();
        },
        timers
    };
}

describe('refreshCoalescer', () => {
    it('a burst of notes during one flight costs exactly 2 refreshes', async () => {
        const h = harness();
        h.coalescer.note(1); // leading edge — starts immediately
        expect(h.stats().refreshes).toBe(1);
        for (let i = 2; i <= 10; i++) h.coalescer.note(i); // burst mid-flight
        expect(h.stats().refreshes).toBe(1);
        await h.complete(10); // catches all the way up
        // trailing run skipped: version 10 >= maxVersion 10
        expect(h.stats().refreshes).toBe(1);
        await h.coalescer.settled();
    });

    it('a trailing refresh runs when the flight did not catch up', async () => {
        const h = harness();
        h.coalescer.note(1);
        h.coalescer.note(5);
        await h.complete(3); // flight lands BEHIND the noted version
        expect(h.stats().refreshes).toBe(2);
        await h.complete(5);
        await h.coalescer.settled();
        expect(h.stats().refreshes).toBe(2);
    });

    it('a stale-version note costs zero refreshes', async () => {
        const h = harness();
        h.coalescer.note(1);
        await h.complete(4);
        h.coalescer.note(3); // already seen
        h.coalescer.note(4); // boundary: <= version, also seen
        expect(h.stats().refreshes).toBe(1);
    });

    it('a versionless note is never skipped', async () => {
        const h = harness();
        h.coalescer.note(1);
        h.coalescer.note(); // versionless, mid-flight
        await h.complete(99);
        expect(h.stats().refreshes).toBe(2); // trailing ran despite the high version
        await h.complete(99);
    });

    it('demand() mid-flight resolves with the trailing result, not the stale flight', async () => {
        const h = harness();
        h.coalescer.note(1);
        const demanded = h.coalescer.demand(); // joins the trailing run
        await h.complete(1); // the stale flight settles → trailing starts
        expect(h.stats().refreshes).toBe(2);
        await h.complete(2);
        await expect(demanded).resolves.toBe(2);
    });

    it('demand() while idle starts a refresh immediately', async () => {
        const h = harness();
        const demanded = h.coalescer.demand();
        expect(h.stats().refreshes).toBe(1);
        await h.complete(7);
        await expect(demanded).resolves.toBe(7);
    });

    it('a rejected refresh does not wedge the coalescer, and demand() sees it', async () => {
        const h = harness();
        const demanded = h.coalescer.demand();
        await h.fail(new Error('store down'));
        await expect(demanded).rejects.toThrow('store down');
        // Fully recovered: the next hint refreshes normally.
        h.coalescer.note();
        expect(h.stats().refreshes).toBe(2);
        await h.complete(1);
        await h.coalescer.settled();
    });

    it('a hint-triggered rejection surfaces nowhere (no unhandled rejection)', async () => {
        const unhandled: unknown[] = [];
        const onUnhandled = (reason: unknown): void => {
            unhandled.push(reason);
        };
        process.on('unhandledRejection', onUnhandled);
        try {
            const h = harness();
            h.coalescer.note();
            await h.fail(new Error('nobody is listening'));
            await new Promise((r) => setTimeout(r, 10));
            expect(unhandled).toEqual([]);
        } finally {
            process.off('unhandledRejection', onUnhandled);
        }
    });

    it('quietMs delays the trailing run, and demand() cuts the delay short', async () => {
        const h = harness(50);
        h.coalescer.note(1);
        h.coalescer.note(); // pending trailing
        await h.complete(1);
        // Trailing is parked behind the quiet timer, not started.
        expect(h.stats().refreshes).toBe(1);
        expect(h.timers.length).toBe(1);
        const demanded = h.coalescer.demand(); // cuts the delay
        expect(h.timers[0]!.cleared).toBe(true);
        expect(h.stats().refreshes).toBe(2);
        await h.complete(2);
        await expect(demanded).resolves.toBe(2);
    });

    it('settled() resolves only once nothing is in flight or pending', async () => {
        const h = harness();
        h.coalescer.note(1);
        h.coalescer.note(); // versionless → trailing will run
        let done = false;
        void h.coalescer.settled().then(() => {
            done = true;
        });
        await h.complete(1);
        expect(done).toBe(false); // trailing flight is up
        await h.complete(2);
        expect(done).toBe(true);
    });
});
