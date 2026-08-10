/**
 * `createWatchReadPump` against fake deps — the module contract (#180).
 *
 * The activation-level behaviour (fairness, seed priority, per-read
 * isolation) is pinned end-to-end in `watch-batch.test.ts`; this file pins
 * the pump's own mechanics: single flight, seeds-before-reads, slicing
 * with a trailing drain, per-job failure isolation, and the two shutdown
 * paths.
 */
import { describe, expect, it } from 'vitest';
import { createWatchReadPump } from '../src/host/watch-pump';
import { HostShutdownError } from '../src/errors';

interface Harness {
    /** Bodies handed to enqueueTurn, NOT yet run. */
    pending: Array<() => Promise<void>>;
    enqueues: number;
    /** Run the next queued drain turn to completion. */
    step(): Promise<void>;
    closed: boolean;
}

function harness(sliceSize?: number): {
    pump: ReturnType<typeof createWatchReadPump>;
    h: Harness;
} {
    const h: Harness = {
        pending: [],
        enqueues: 0,
        closed: false,
        async step() {
            const body = h.pending.shift();
            if (!body) throw new Error('no drain queued');
            await body();
        }
    };
    const pump = createWatchReadPump({
        enqueueTurn: (body) => {
            if (h.closed) return Promise.reject(new HostShutdownError());
            h.enqueues += 1;
            h.pending.push(body);
            return Promise.resolve();
        },
        closed: () => h.closed,
        ...(sliceSize !== undefined ? { sliceSize } : {})
    });
    return { pump, h };
}

const job =
    (log: string[], name: string, result: unknown = name) =>
    async () => {
        log.push(name);
        return result;
    };

describe('createWatchReadPump', () => {
    it('holds at most one queued turn however many reads are scheduled', async () => {
        const { pump, h } = harness();
        const log: string[] = [];
        const settled = [
            pump.schedule(job(log, 'a'), false),
            pump.schedule(job(log, 'b'), false),
            pump.schedule(job(log, 'c'), false)
        ];
        expect(h.enqueues).toBe(1);
        await h.step();
        expect(await Promise.all(settled)).toEqual(['a', 'b', 'c']);
        expect(h.pending).toHaveLength(0);
    });

    it('drains seeds before already-pending re-reads', async () => {
        const { pump, h } = harness();
        const log: string[] = [];
        const reads = [
            pump.schedule(job(log, 'read1'), false),
            pump.schedule(job(log, 'read2'), false)
        ];
        const seed = pump.schedule(job(log, 'seed'), true);
        await h.step();
        await Promise.all([...reads, seed]);
        expect(log).toEqual(['seed', 'read1', 'read2']);
    });

    it('runs at most sliceSize jobs per turn and re-enqueues the remainder', async () => {
        const { pump, h } = harness(2);
        const log: string[] = [];
        const settled = ['a', 'b', 'c', 'd', 'e'].map((n) => pump.schedule(job(log, n), false));
        await h.step();
        expect(log).toEqual(['a', 'b']);
        // The remainder went to a NEW turn at the tail, not the same one.
        expect(h.pending).toHaveLength(1);
        await h.step();
        expect(log).toEqual(['a', 'b', 'c', 'd']);
        await h.step();
        await Promise.all(settled);
        expect(log).toEqual(['a', 'b', 'c', 'd', 'e']);
    });

    it('work scheduled mid-drain lands in the trailing turn, never reentrantly', async () => {
        const { pump, h } = harness();
        const log: string[] = [];
        let lateSettled: Promise<unknown> | null = null;
        const first = pump.schedule(async () => {
            log.push('first');
            lateSettled = pump.schedule(job(log, 'late'), false);
            return 'first';
        }, false);
        await h.step();
        // `late` was requested INSIDE the drain: it must not have run yet.
        expect(log).toEqual(['first']);
        expect(h.pending).toHaveLength(1);
        await h.step();
        expect(log).toEqual(['first', 'late']);
        await first;
        await lateSettled!;
    });

    it('one failing read rejects only its own caller and the drain turn never rejects', async () => {
        const { pump, h } = harness();
        const log: string[] = [];
        const boom = new Error('boom');
        const failing = pump.schedule(async () => {
            throw boom;
        }, false);
        const fine = pump.schedule(job(log, 'fine'), false);
        // step() awaits the drain body directly: a rejection here would
        // reject the turn — the thing the Turns tail must never carry.
        await h.step();
        await expect(failing).rejects.toBe(boom);
        expect(await fine).toBe('fine');
    });

    it('a queue closing mid-drain rejects the remainder like queued reads', async () => {
        const { pump, h } = harness();
        const first = pump.schedule(async () => {
            h.closed = true;
            return 'ran';
        }, false);
        const second = pump.schedule(async () => 'never', false);
        await h.step();
        expect(await first).toBe('ran');
        await expect(second).rejects.toBeInstanceOf(HostShutdownError);
    });

    it('an enqueue rejection rejects everything pending', async () => {
        const { pump, h } = harness();
        h.closed = true;
        const settled = pump.schedule(async () => 'never', false);
        await expect(settled).rejects.toBeInstanceOf(HostShutdownError);
        expect(h.pending).toHaveLength(0);
    });

    it('recovers after an enqueue rejection once the queue reopens', async () => {
        const { pump, h } = harness();
        h.closed = true;
        await expect(pump.schedule(async () => 'never', false)).rejects.toBeInstanceOf(
            HostShutdownError
        );
        h.closed = false;
        const log: string[] = [];
        const settled = pump.schedule(job(log, 'again'), false);
        await h.step();
        expect(await settled).toBe('again');
    });
});
