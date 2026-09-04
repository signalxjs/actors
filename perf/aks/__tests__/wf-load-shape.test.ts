// @vitest-environment node
/**
 * `mergeWfRows` — the one pure function between a generator pod's JSON
 * line and the number in a baseline (#297). Counts must sum across pods,
 * percentiles must come from ONE pod and say so, and rates must be
 * recomputed from summed counts rather than added — each of those is a
 * silent error when got wrong, which is why the merge is tested and not
 * the driving.
 */
import { describe, expect, it } from 'vitest';
import { mergeWfRows } from '../deploy/wf-load.mjs';

const row = (over: Record<string, unknown> = {}) => ({
    runId: 'pod',
    mode: 'workflow',
    rate: 50,
    arrival: 'poisson',
    mix: 'order:100',
    knobs: { taskMs: 20 },
    durationMs: 60_000,
    drainMs: 10_000,
    started: 100,
    startFailures: 0,
    startsDeferred: 0,
    completed: 90,
    failed: 5,
    compensated: 3,
    cancelled: 0,
    childRuns: 0,
    completedUnreported: 0,
    droppedEvents: 0,
    unknownEvents: 0,
    sweepUnpollable: 0,
    signalsSent: 0,
    signalsSkipped: 0,
    signalFailures: 0,
    stuck: { sleeping: 1, waiting: 0, blocked: 1, running: 0, other: 0, total: 2 },
    byTemplate: { order: { completed: 90, failed: 5, compensated: 3 } },
    latencyMs: { order: { count: 98, p50: 100, p90: 200, p99: 300, max: 400 } },
    observedMs: { order: { count: 98, p50: 110, p90: 210, p99: 310, max: 410 } },
    startMs: { count: 100, p50: 5, p90: 9, p99: 20, max: 30 },
    deferredMs: null,
    transitions: 700,
    remindersFired: 12,
    nodeMs: { task: { count: 400, p50: 20, p90: 25, p99: 40, max: 60 } },
    errors: { total: 1, byKind: { 'start:503': 1 } },
    ...over
});

describe('mergeWfRows', () => {
    it('sums counts across pods and recomputes rates over the longest window', () => {
        const [merged] = mergeWfRows([row(), row({ durationMs: 62_000, drainMs: 8_000, started: 50, completed: 45, failed: 5, compensated: 0, byTemplate: { order: { completed: 45, failed: 5 } } })]);
        expect(merged).toBeDefined();
        expect(merged!.pods).toBe(2);
        expect(merged!.started).toBe(150);
        expect(merged!.completed).toBe(135);
        expect(merged!.failed).toBe(10);
        expect(merged!.stuck.total).toBe(4);
        expect(merged!.byTemplate.order).toEqual({ completed: 135, failed: 10, compensated: 3 });
        expect(merged!.durationMs).toBe(62_000);
        // 150 over the longest arrival window, not the sum of two rates.
        expect(merged!.runsStartedPerSec).toBeCloseTo(150 / 62, 2);
        expect(merged!.runsCompletedPerSec).toBeCloseTo(135 / 72, 2);
        expect(merged!.errors).toEqual({ total: 2, byKind: { 'start:503': 2 } });
    });

    it('takes percentiles and engine sums from ONE pod and labels it', () => {
        const [merged] = mergeWfRows([
            row({ latencyMs: { order: { count: 1, p50: 1, p90: 1, p99: 1, max: 1 } }, transitions: 700 }),
            row({ latencyMs: { order: { count: 1, p50: 999, p90: 999, p99: 999, max: 999 } }, transitions: 700 })
        ]);
        expect(merged!.latencyMs?.order?.p50).toBe(1);
        expect(merged!.latencyFromPods).toBe(2);
        // The aggregator's sums are cluster-wide already; two pods reading
        // them must not double them.
        expect(merged!.transitions).toBe(700);
        expect(merged!.transitionsPerSec).toBeCloseTo(700 / 70, 2);
    });

    it('reports the OFFERED rate — the rung times the pods — beside the per-pod one (#380)', () => {
        // Four pods each offering 250/s offered 1,000/s to the fleet; a row
        // labelled r=250 would understate the load fourfold.
        const [merged] = mergeWfRows([row({ rate: 250 }), row({ rate: 250 }), row({ rate: 250 }), row({ rate: 250 })]);
        expect(merged!.rate).toBe(250);
        expect(merged!.pods).toBe(4);
        expect(merged!.offeredRate).toBe(1000);
        expect(mergeWfRows([row()])[0]!.offeredRate).toBe(50);
    });

    it('sums the generator CPU across pods, and reports it only when every pod carried it', () => {
        const [merged] = mergeWfRows([row({ generatorCpuMs: 1200 }), row({ generatorCpuMs: 800 })]);
        expect(merged!.generatorCpuMs).toBe(2000);
        expect(mergeWfRows([row()])[0]!.generatorCpuMs).toBeNull();
        // A mixed rung — one pod from a generator that predates the field —
        // must not read as the newer pod's total.
        expect(mergeWfRows([row({ generatorCpuMs: 1200 }), row()])[0]!.generatorCpuMs).toBeNull();
        expect(mergeWfRows([row({ generatorCpuMs: 1200 }), row()])[0]!).not.toHaveProperty('generatorCpuPods');
    });

    it('keeps rungs apart and in order, and stamps partial only when told', () => {
        const merged = mergeWfRows([row({ rate: 100 }), row({ rate: 25 })]);
        expect(merged.map((r) => r.rate)).toEqual([25, 100]);
        expect(merged[0]!.partial).toBeUndefined();
        expect(mergeWfRows([row()], { partial: true })[0]!.partial).toBe(true);
    });
});

describe('runWfLoad argument guards', () => {
    it('refuses chaos=owner-kill with a multi-rung sweep before touching a cluster', async () => {
        const { runWfLoad } = await import('../deploy/wf-load.mjs');
        // No context, no kubectl: the guard fires on the values alone.
        await expect(
            runWfLoad({
                context: 'none', namespace: 'x', chartDir: 'x', imageRepository: 'r', imageTag: 't', workload: 'w',
                values: { chaos: 'owner-kill', sweep: '10,25' }
            })
        ).rejects.toThrow(/single rung/);
        await expect(
            runWfLoad({
                context: 'none', namespace: 'x', chartDir: 'x', imageRepository: 'r', imageTag: 't', workload: 'w',
                values: { chaos: 'sideways' }
            })
        ).rejects.toThrow(/unknown chaos/);
    });
});
