// @vitest-environment node
/**
 * The pure parts of `wf-fleet.mjs` (#381): the CLI parser that decides
 * which words are rig options and which are knobs passed to the hosts, and
 * the CPU summary the rig reports. The driving itself forks real hosts and
 * needs a Redis, so it is exercised by `pnpm bench:wf-local`, not here.
 */
import { describe, expect, it } from 'vitest';
import { cpuSummary, parseFleetArgs, validateBasePort } from '../wf-fleet.mjs';

describe('parseFleetArgs', () => {
    it('splits rig options from host knobs', () => {
        const args = parseFleetArgs([
            'hosts=4',
            'sweep=50,100',
            'durationS=20',
            'WF_TASK_MS=2',
            'WF_MAX_INFLIGHT=50000',
            'TRANSPORT=tcp',
            'FETCH_CONNECTIONS=128',
            'REMINDERS=redis',
            '--json'
        ]);
        expect(args.hosts).toBe(4);
        expect(args.sweep).toEqual([50, 100]);
        expect(args.durationS).toBe(20);
        expect(args.json).toBe(true);
        expect(args.env).toEqual({
            WF_TASK_MS: '2',
            WF_MAX_INFLIGHT: '50000',
            TRANSPORT: 'tcp',
            FETCH_CONNECTIONS: '128',
            REMINDERS: 'redis'
        });
    });

    it('takes a single rate as well as a sweep', () => {
        expect(parseFleetArgs(['hosts=1', 'rate=25']).rate).toBe(25);
    });

    it('refuses what it does not understand rather than passing it on', () => {
        expect(() => parseFleetArgs(['hosts=1', 'bogus=1'])).toThrow(/unknown option/);
        expect(() => parseFleetArgs(['hosts=zero'])).toThrow(/positive number/);
        expect(() => parseFleetArgs(['hosts=1', 'sweep=a,b'])).toThrow(/positive rates/);
        expect(() => parseFleetArgs(['nonsense'])).toThrow(/key=value/);
    });
});

describe('cpuSummary', () => {
    it('reports peak and mean over the numeric samples only', () => {
        expect(cpuSummary([10, null, 30.04, 20])).toEqual({ peak: 30, avg: 20, samples: 3 });
    });

    it('says so when nothing was sampled', () => {
        expect(cpuSummary([null, null])).toEqual({ peak: null, avg: null, samples: 0 });
        expect(cpuSummary([])).toEqual({ peak: null, avg: null, samples: 0 });
    });
});

describe('validateBasePort', () => {
    it('accepts a port with room for the http and tcp ranges', () => {
        expect(validateBasePort('7411', 8)).toBe(7411);
        expect(validateBasePort(65435, 1)).toBe(65435);
    });

    it('refuses a non-port, and a base that pushes either range past 65535', () => {
        expect(() => validateBasePort('seven', 1)).toThrow(/integer in 1..65535/);
        expect(() => validateBasePort(0, 1)).toThrow(/integer in 1..65535/);
        expect(() => validateBasePort(65500, 1)).toThrow(/no room/);
        expect(() => validateBasePort(65400, 40)).toThrow(/no room/);
        expect(() => validateBasePort(7411, 101)).toThrow(/at most 100 hosts/);
    });
});
