import { describe, expect, it } from 'vitest';
import { parseExposition, sampleValue } from '../src/prometheus.ts';

/**
 * A scrape in the shape `renderPrometheus` produces: metadata lines, a
 * counter with labels, a histogram with `+Inf`, an unlabelled gauge — plus
 * the two things a naive split on `"` gets wrong: an escaped quote and an
 * escaped newline inside a label value, and a trailing timestamp.
 */
const SCRAPE = `
# HELP sigx_actors_calls_total Calls dispatched, by actor type.
# TYPE sigx_actors_calls_total counter
sigx_actors_calls_total{type="Counter"} 13
sigx_actors_calls_total{type="Odd \\"quoted\\"\\nname"} 2
# TYPE sigx_actors_call_duration_seconds histogram
sigx_actors_call_duration_seconds_bucket{le="0.001024"} 8
sigx_actors_call_duration_seconds_bucket{le="+Inf"} 13
sigx_actors_call_duration_seconds_sum 0.026326
sigx_actors_call_duration_seconds_count 13
sigx_actors_up 1 1700000000000
this line is not a sample
sigx_actors_broken{type="Counter"} not-a-number
`;

describe('parseExposition', () => {
    const samples = parseExposition(SCRAPE);

    it('reads every sample line and skips comments, blanks, junk and non-numeric values', () => {
        expect(samples.map((s) => s.name)).toEqual([
            'sigx_actors_calls_total',
            'sigx_actors_calls_total',
            'sigx_actors_call_duration_seconds_bucket',
            'sigx_actors_call_duration_seconds_bucket',
            'sigx_actors_call_duration_seconds_sum',
            'sigx_actors_call_duration_seconds_count',
            'sigx_actors_up'
        ]);
    });

    it('parses labels, unescaping the way the format defines', () => {
        expect(samples[0]!.labels).toEqual({ type: 'Counter' });
        expect(samples[1]!.labels).toEqual({ type: 'Odd "quoted"\nname' });
        expect(samples[4]!.labels).toEqual({});
    });

    it('reads +Inf buckets and ignores a trailing timestamp', () => {
        expect(samples[3]!.labels.le).toBe('+Inf');
        expect(samples[3]!.value).toBe(13);
        expect(samples[6]!.value).toBe(1);
    });

    it('never yields a NaN sample', () => {
        expect(samples.some((s) => Number.isNaN(s.value))).toBe(false);
        expect(sampleValue(samples, 'sigx_actors_broken')).toBeNull();
    });
});

describe('sampleValue', () => {
    const samples = parseExposition(SCRAPE);

    it('matches on name and every named label, leaving the rest free', () => {
        expect(sampleValue(samples, 'sigx_actors_calls_total', { type: 'Counter' })).toBe(13);
        expect(sampleValue(samples, 'sigx_actors_call_duration_seconds_bucket', { le: '+Inf' })).toBe(13);
        // No labels named: the FIRST sample of that name.
        expect(sampleValue(samples, 'sigx_actors_calls_total')).toBe(13);
        expect(sampleValue(samples, 'sigx_actors_call_duration_seconds_count')).toBe(13);
    });

    it('is null, not zero, for a sample that is not there', () => {
        expect(sampleValue(samples, 'sigx_actors_calls_total', { type: 'Nope' })).toBeNull();
        expect(sampleValue(samples, 'sigx_actors_missing')).toBeNull();
    });
});
