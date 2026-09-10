// @vitest-environment node
/**
 * The pure halves of the `wf-load` timeline sampler (#380): what a
 * `kubectl top pods` line and a `redis-cli INFO` body parse to, and what
 * a sampled timeline reduces to. The sampling itself needs a cluster and
 * a metrics-server; these are the parts that would otherwise be wrong
 * silently — a CPU figure in the wrong unit reads as a fleet at 1% or at
 * 1000%, and both look like a finding.
 */
import { describe, expect, it } from 'vitest';
import {
    parseCpuMillis,
    parseRedisInfo,
    parseTopPods,
    restartDelta,
    runBudgetMs,
    timelinePeaks
} from '../deploy/wf-load.mjs';

describe('parseTopPods', () => {
    it('reads millicores and bytes off `kubectl top pods --no-headers`', () => {
        const text = 'sigx-host-abc   812m   301Mi\nsigx-host-def   1200m  1Gi\nsigx-redis-x    45m    12Mi\n';
        expect(parseTopPods(text)).toEqual([
            { pod: 'sigx-host-abc', cpuM: 812, memBytes: 301 * 1024 * 1024 },
            { pod: 'sigx-host-def', cpuM: 1200, memBytes: 1024 * 1024 * 1024 },
            { pod: 'sigx-redis-x', cpuM: 45, memBytes: 12 * 1024 * 1024 }
        ]);
    });

    it('tolerates whole cores, blank lines and an empty body', () => {
        expect(parseTopPods('p 2 100Ki\n\n')).toEqual([{ pod: 'p', cpuM: 2000, memBytes: 100 * 1024 }]);
        expect(parseTopPods('')).toEqual([]);
        expect(parseTopPods(null)).toEqual([]);
    });
});

describe('parseCpuMillis', () => {
    it('handles the resource-quantity forms a limit takes', () => {
        expect(parseCpuMillis('1800m')).toBe(1800);
        expect(parseCpuMillis('1')).toBe(1000);
        expect(parseCpuMillis('0.5')).toBe(500);
        expect(parseCpuMillis('')).toBeNull();
        expect(parseCpuMillis(undefined)).toBeNull();
        expect(parseCpuMillis('lots')).toBeNull();
    });
});

describe('parseRedisInfo', () => {
    it('picks the CPU pair and the three gauges off an INFO body and ignores the rest', () => {
        const body =
            '# Server\r\nredis_version:7.2.4\r\n# Clients\r\nconnected_clients:12\r\n# Memory\r\n' +
            'used_memory:1048576\r\n# Stats\r\ninstantaneous_ops_per_sec:340\r\n# CPU\r\n' +
            'used_cpu_sys:12.5\r\nused_cpu_user:30.25\r\n';
        expect(parseRedisInfo(body)).toEqual({
            cpuS: 42.75,
            opsPerSec: 340,
            memBytes: 1048576,
            clients: 12
        });
    });

    it('returns null for a body that is not INFO', () => {
        expect(parseRedisInfo('')).toBeNull();
        expect(parseRedisInfo(null)).toBeNull();
        expect(parseRedisInfo('error: NOAUTH')).toBeNull();
    });

    it('never yields NaN: a gauge that is absent or not a number is null', () => {
        const parsed = parseRedisInfo('used_cpu_user:1\r\nused_cpu_sys:2\r\nused_memory:lots\r\nconnected_clients:\r\n');
        expect(parsed).toEqual({ cpuS: 3, opsPerSec: null, memBytes: null, clients: null });
        // And a CPU field that is not a number is not INFO at all.
        expect(parseRedisInfo('used_cpu_user:x\r\nused_cpu_sys:2\r\n')).toBeNull();
    });
});

describe('timelinePeaks', () => {
    const sample = (t: number, hosts: Array<[number, number]>, redis: [number, number, number] | null) => ({
        t,
        hosts: hosts.map(([cpuM, memBytes], i) => ({ pod: `h${i}`, cpuM, memBytes })),
        redis: redis ? { cpuS: redis[0], opsPerSec: redis[1], memBytes: redis[2], clients: 1 } : null,
        activations: null,
        queued: 0
    });

    it('takes the hottest host against its limit, and Redis CPU as a rate between samples', () => {
        const timeline = [
            sample(0, [[100, 1e6], [200, 2e6]], [10, 50, 1e6]),
            sample(10_000, [[900, 3e6], [400, 2e6]], [15, 800, 2e6]),
            sample(20_000, [[600, 3e6], [300, 2e6]], [17, 300, 3e6])
        ];
        const peaks = timelinePeaks(timeline, { hostCpuLimitM: 1000 });
        expect(peaks.hostCpuPeakM).toBe(900);
        expect(peaks.hostCpuPeakRatio).toBeCloseTo(0.9, 6);
        expect(peaks.hostMemPeakBytes).toBe(3e6);
        // 5 CPU-seconds over the 10 s between the first two samples.
        expect(peaks.redisCpuPeakRatio).toBeCloseTo(0.5, 6);
        expect(peaks.redisOpsPerSecPeak).toBe(800);
        expect(peaks.redisMemEndBytes).toBe(3e6);
    });

    it('says nothing it cannot know', () => {
        expect(timelinePeaks([], { hostCpuLimitM: 1000 })).toEqual({});
        // No limit known: the millicore peak stands, the ratio does not.
        const one = timelinePeaks([sample(0, [[500, 1]], null)], { hostCpuLimitM: null });
        expect(one.hostCpuPeakM).toBe(500);
        expect(one.hostCpuPeakRatio).toBeUndefined();
        expect(one.redisCpuPeakRatio).toBeUndefined();
        // One Redis sample is a level, not a rate.
        const single = timelinePeaks([sample(0, [], [10, 5, 7])], { hostCpuLimitM: 1000 });
        expect(single.redisCpuPeakRatio).toBeUndefined();
        expect(single.redisOpsPerSecPeak).toBe(5);
        expect(single.redisMemEndBytes).toBe(7);
        // Null gauges are skipped, not treated as 0 or NaN.
        const gapped = timelinePeaks(
            [
                { t: 0, hosts: [], redis: { cpuS: 1, opsPerSec: null, memBytes: null, clients: null }, activations: null, queued: 0 },
                { t: 1000, hosts: [], redis: { cpuS: 1.5, opsPerSec: 9, memBytes: 4, clients: 1 }, activations: null, queued: 0 },
                { t: 2000, hosts: [], redis: { cpuS: 1.6, opsPerSec: null, memBytes: null, clients: null }, activations: null, queued: 0 }
            ],
            { hostCpuLimitM: 1000 }
        );
        expect(gapped.redisOpsPerSecPeak).toBe(9);
        // "At the end" is the FINAL sample; when it has no memory figure,
        // an earlier one does not stand in for it.
        expect(gapped.redisMemEndBytes).toBeUndefined();
        expect(gapped.redisCpuPeakRatio).toBeCloseTo(0.5, 6);
    });
});

describe('restartDelta', () => {
    it('counts a restart on a pod present at both ends', () => {
        expect(restartDelta({ a: 0, b: 2 }, { a: 1, b: 2 })).toEqual({ restartsDuringRun: 1, podsReplaced: 0 });
    });

    // A rolling update replaces every pod, and the replacements start at 0.
    // Six hosts were then liveness-killed during the same rung on
    // 2026-09-08 (#391 G2) and the rig reported 0, because only pods seen
    // BEFORE the run were counted. The replacement itself is not a restart;
    // the replacement's own restarts are.
    it('counts the restarts of a pod that replaced one mid-run', () => {
        expect(restartDelta({ a: 0, b: 0 }, { a: 0, c: 2, d: 0 })).toEqual({
            restartsDuringRun: 2,
            podsReplaced: 2
        });
    });

    it('is null when either end could not be observed', () => {
        expect(restartDelta(null, { a: 1 })).toEqual({ restartsDuringRun: null, podsReplaced: null });
        expect(restartDelta({ a: 0 }, null)).toEqual({ restartsDuringRun: null, podsReplaced: null });
    });
});

// A soak is longer than the hour the run budget used to allow, and the
// verb could not raise it: the 2026-09-10 soak was cancelled at 60 min
// with the generator still running and its row never written. The budget
// now follows the rungs.
describe('runBudgetMs', () => {
    it('is an hour for the default 60 s rung', () => {
        expect(runBudgetMs({})).toBe(3_600_000);
    });

    it('follows a long rung: its length, the drain, and margin — never under an hour', () => {
        // 5 400 s of arrivals + 240 s drain + 15 min margin.
        expect(runBudgetMs({ durationS: '5400', WF_DRAIN_S: '240' })).toBe((5400 + 240 + 900) * 1000);
    });

    it('reads a knob only from the keys the run path consumes', () => {
        // `loadgen.WF_DRAIN_S` is not a key runWfLoad maps, so it must not shape the budget.
        expect(runBudgetMs({ durationS: '5400', 'loadgen.WF_DRAIN_S': '60' })).toBe((5400 + 120 + 900) * 1000);
    });

    it('sums a sweep', () => {
        // three rungs of 60 s, the default 120 s drain each, plus margin — still under an hour.
        expect(runBudgetMs({ sweep: '100,200,400', durationS: '60' })).toBe(3_600_000);
        expect(runBudgetMs({ sweep: '100,200', durationS: '1800', 'loadgen.wf.WF_DRAIN_S': '60' })).toBe((2 * (1800 + 60) + 900) * 1000);
    });
});
