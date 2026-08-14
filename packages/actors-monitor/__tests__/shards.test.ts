/**
 * Reminder-shard states, and the percentile triple.
 *
 * Both are verdicts rather than data: "no claimant" is an incident and "two
 * claimants" is a divergence; an empty histogram is *no reading* and not
 * three zeroes. They live here rather than in a renderer precisely so a
 * second renderer inherits the verdict instead of re-deriving it — and these
 * assertions are what that inheritance is worth.
 */
import { describe, expect, it } from 'vitest';
import {
    percentileCeiling,
    percentilePoints,
    shardStates,
    splitShards,
    unclaimedShards
} from '@sigx/actors-monitor';

const shards = {
    p0: ['host-a'],
    p1: [],
    p2: ['host-a', 'host-b'],
    p10: ['host-c']
};

describe('shardStates', () => {
    it('distinguishes the three states that mean different things', () => {
        expect(shardStates(shards).map((s) => s.state)).toEqual([
            'claimed',
            'unclaimed',
            'split',
            'claimed'
        ]);
    });

    it('orders shards numerically, so p10 does not sit between p1 and p2', () => {
        expect(shardStates(shards).map((s) => s.label)).toEqual(['p0', 'p1', 'p2', 'p10']);
    });

    it('names the claimants, so a divergence says WHICH hosts', () => {
        expect(shardStates(shards)[2]!.claimants).toEqual(['host-a', 'host-b']);
    });

    it('keeps an unparseable shard id rather than dropping it', () => {
        // A host on a newer build could name shards differently. Losing the
        // row entirely would report "nothing wrong" about a shard nobody is
        // ticking, which is the exact failure this panel exists to catch.
        const odd = shardStates({ weird: [], p1: ['a'] });
        expect(odd.map((s) => s.label)).toEqual(['p1', 'weird']);
        expect(odd[1]!.state).toBe('unclaimed');
    });

    it('picks out the two findings worth acting on', () => {
        // Unclaimed means those reminders are simply not firing, and nothing
        // else in the system surfaces it.
        expect(unclaimedShards(shards)).toEqual(['p1']);
        expect(splitShards(shards)).toEqual(['p2']);
    });

    it('handles an empty map', () => {
        expect(shardStates({})).toEqual([]);
        expect(unclaimedShards({})).toEqual([]);
        expect(splitShards({})).toEqual([]);
    });
});

const hist = (p50: number, p90: number, p99: number, count = 10) => ({
    count,
    minMs: 0,
    maxMs: p99,
    meanMs: p50,
    p50Ms: p50,
    p90Ms: p90,
    p99Ms: p99
});

describe('percentilePoints', () => {
    it('reports an absent or empty histogram as no reading, not as zero', () => {
        // A row of zeroes asserts "we measured, and it was fast".
        expect(percentilePoints(null).map((p) => p.value)).toEqual([null, null, null]);
        expect(percentilePoints(undefined).map((p) => p.value)).toEqual([null, null, null]);
        expect(percentilePoints(hist(0, 0, 0, 0)).map((p) => p.value)).toEqual([null, null, null]);
    });

    it('labels the triple', () => {
        expect(percentilePoints(hist(1, 2, 3))).toEqual([
            { label: 'p50', value: 1 },
            { label: 'p90', value: 2 },
            { label: 'p99', value: 3 }
        ]);
    });
});

describe('percentileCeiling', () => {
    it('covers the tallest point across every histogram', () => {
        // The whole value of stacking latency, queue and turn is the
        // comparison; a per-group ceiling would draw a 12µs queue wait and a
        // 47ms turn identically.
        expect(percentileCeiling([hist(1, 2, 100), hist(0.01, 0.02, 0.05)])).toBe(100);
        expect(percentileCeiling([hist(0.01, 0.02, 0.05)])).toBe(0.05);
    });

    it('ignores a histogram that recorded nothing', () => {
        expect(percentileCeiling([null, undefined, hist(0, 0, 0, 0)])).toBe(0);
    });

    it('is driven by p50 too, not only by p99', () => {
        // A histogram whose p99 is absent but whose p50 is not would
        // otherwise scale to zero and draw every bar full-height.
        expect(percentileCeiling([{ ...hist(7, 7, 7), p99Ms: Number.NaN }])).toBe(7);
    });
});
