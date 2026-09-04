// @vitest-environment node
/**
 * `wf-local/*` (#381): the shape string that keeps a laptop fleet from
 * ever being compared against a Tier-3 `wf` baseline, and the Tier-2 rule
 * that timings on a shared box are context, not evidence.
 */
import { describe, expect, it } from 'vitest';
import { shapeMismatch } from '../src/shape.mjs';
import { asTier2, parseLadder, positiveNumber, wfLocalShape } from '../src/scenarios/wf-local.ts';

const shape = (over: Partial<Parameters<typeof wfLocalShape>[0]> = {}) =>
    wfLocalShape({
        hosts: [1, 2, 4, 8],
        cores: 10,
        cpu: 'Apple M4 Pro',
        nodeMajor: 24,
        image: 'abc1234',
        env: { WF_REMINDER_TICK_MS: '1000', TRANSPORT: 'http', HOME: '/x', WF_EMPTY: '' },
        ...over
    });

describe('wfLocalShape', () => {
    it('carries the ladder, the box and the sorted host knobs, and nothing else from env', () => {
        expect(shape()).toBe(
            'wf-local hosts=1,2,4,8 cores=10 cpu=Apple_M4_Pro node=24 image=abc1234 knobs=TRANSPORT=http,WF_REMINDER_TICK_MS=1000'
        );
    });

    it('names an unknobbed run as such', () => {
        expect(shape({ env: {} })).toMatch(/knobs=\(default\)$/);
    });

    it('is refused against a Tier-3 wf baseline by the shared contract', () => {
        const tier3 = 'wf replicas=3 nodes=3 image=abc1234 knobs=FETCH_CONNECTIONS=64,TRANSPORT=http';
        expect(shapeMismatch(shape(), tier3)).not.toBeNull();
        // And a different box, or a different ladder, is a different shape.
        expect(shapeMismatch(shape(), shape({ cores: 8 }))).not.toBeNull();
        expect(shapeMismatch(shape(), shape({ hosts: [1, 8] }))).not.toBeNull();
        expect(shapeMismatch(shape(), shape())).toBeNull();
    });
});

describe('asTier2', () => {
    it('demotes timings to informational and leaves counts, ratios and multipliers gating', () => {
        const out = asTier2([
            { name: 'a/runs_completed_per_sec', value: 1, unit: 'runs/s', direction: 'higher' },
            { name: 'a/start_p50_ms', value: 1, unit: 'ms', direction: 'lower' },
            { name: 'a/stuck_ratio', value: 0, unit: 'ratio', direction: 'lower', noiseFloor: 0.001 },
            { name: 'a/wakes_lost', value: 0, unit: 'count', direction: 'lower', noiseFloor: 1 },
            { name: 'scale_8_over_1', value: 5.9, unit: 'x', direction: 'higher', noiseFloor: 1 }
        ]);
        expect(out.map((m) => m.informational ?? false)).toEqual([true, true, false, false, false]);
    });
});

describe('parseLadder', () => {
    it('reads the env value in order, or the fallback when unset', () => {
        expect(parseLadder('X', '8,1, 4', '1,2')).toEqual([8, 1, 4]);
        expect(parseLadder('X', undefined, '1,2')).toEqual([1, 2]);
    });

    it('refuses a value that parses to no rungs rather than no-op the scenario', () => {
        expect(() => parseLadder('WF_LOCAL_HOSTS', '', '1,2')).toThrow(/WF_LOCAL_HOSTS/);
        expect(() => parseLadder('WF_LOCAL_HOSTS', 'a,b', '1,2')).toThrow(/positive numbers/);
    });
});

describe('positiveNumber', () => {
    it('takes the env value when set and the fallback when not', () => {
        expect(positiveNumber('X', '45', 20)).toBe(45);
        expect(positiveNumber('X', undefined, 20)).toBe(20);
    });

    it('refuses empty, non-numeric and non-positive values by name', () => {
        expect(() => positiveNumber('WF_LOCAL_RATE', '', 200)).toThrow(/WF_LOCAL_RATE/);
        expect(() => positiveNumber('WF_LOCAL_RATE', 'fast', 200)).toThrow(/positive number/);
        expect(() => positiveNumber('WF_LOCAL_DURATION_S', '0', 20)).toThrow(/positive number/);
    });
});
