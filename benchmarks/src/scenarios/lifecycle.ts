/**
 * Activation cost and the two background jobs that scale with the number
 * of activations rather than the number of calls.
 *
 * Activation is where the expensive per-actor setup lives: an
 * `effectScope`, a deep reactive `signal()` proxy over initial state, the
 * method table built by calling the `methods` factory, and a storage load.
 * A workload with poor locality pays it constantly, so its cost bounds how
 * aggressive idle collection can afford to be.
 *
 * `sweeper/scan` and `reminders/tick` are the O(n) jobs. Both are invoked
 * directly through the manual clock rather than waited for, so what gets
 * measured is the scan itself and not the interval.
 */
import { Reminded, Tiny } from '../actors.ts';
import { closedLoop, LATENCY_NOISE_FLOOR_MS } from '../loop.ts';
import { benchCall, createBenchSilo, refsFor, warmActivations } from '../silo-fixture.ts';
import type { Metric, RunContext, Scenario } from '../types.ts';

/**
 * Activate a never-seen key, then deactivate it. Measures the pair, since
 * measuring activation alone would leave the silo holding every activation
 * it created and turn this into a memory scenario by accident.
 */
const coldActivation: Scenario = {
    name: 'activation/cold-cycle',
    description: 'activate a fresh key + deactivate it — Activation.create, storage load, teardown',
    async run(ctx: RunContext): Promise<Metric[]> {
        const fixture = await createBenchSilo({ actors: [Tiny] });
        try {
            const call = benchCall();
            const outcome = await closedLoop({
                call: async (i) => {
                    const ref = { type: Tiny.type, key: `cold-${i}` };
                    await fixture.silo.dispatch(ref, 'noop', [], call);
                    await fixture.silo.deactivate(ref);
                },
                // Serial: concurrent cold activations would measure the
                // activation storm, and the storm is a different question.
                concurrency: 1,
                durationMs: ctx.durationMs,
                latency: true
            });
            const p = outcome.percentiles!;
            return [
                { name: 'cycles_per_sec', value: outcome.opsPerSec, unit: 'ops/s', direction: 'higher' },
                {
                    name: 'p50_ms',
                    value: p.p50,
                    unit: 'ms',
                    direction: 'lower',
                    noiseFloor: LATENCY_NOISE_FLOOR_MS
                },
                {
                    name: 'p99_ms',
                    value: p.p99,
                    unit: 'ms',
                    direction: 'lower',
                    noiseFloor: LATENCY_NOISE_FLOOR_MS
                }
            ];
        } finally {
            await fixture.stop();
        }
    }
};

/**
 * The idle sweeper walks the whole activation directory on every tick. At
 * 60s intervals that is invisible until the directory is large — this is
 * where we find out how large "large" has to be.
 */
const sweeperScan: Scenario = {
    name: 'sweeper/scan',
    description: 'one idle-collection sweep over 10k / 50k live activations',
    async run(ctx: RunContext): Promise<Metric[]> {
        const sizes = ctx.quick ? [1000] : [10_000, 50_000];
        const metrics: Metric[] = [];
        for (const size of sizes) {
            const fixture = await createBenchSilo({ actors: [Tiny] });
            try {
                await warmActivations(fixture.silo, refsFor(Tiny.type, size));
                // idleAfterMs is an hour, so the sweep finds nothing to
                // collect: we are timing the SCAN, not the deactivations.
                const started = performance.now();
                const passes = 20;
                for (let i = 0; i < passes; i++) fixture.clock.advance(3_600_000);
                const elapsed = performance.now() - started;
                metrics.push({
                    name: `n=${size}/sweep_ms`,
                    value: elapsed / passes,
                    unit: 'ms',
                    direction: 'lower'
                });
            } finally {
                await fixture.stop();
            }
        }
        return metrics;
    }
};

/**
 * The reminder tick reads and rewrites every shard record it owns, and all
 * mutations serialize through a single promise chain across all 16 shards.
 * With M reminders registered that chain is the contention point.
 */
const reminderTick: Scenario = {
    name: 'reminders/tick',
    description: 'one reminder tick with 2k reminders spread over the 16 shards',
    async run(ctx: RunContext): Promise<Metric[]> {
        const count = ctx.quick ? 200 : 2000;
        const fixture = await createBenchSilo({ actors: [Reminded] });
        try {
            // onActivate registers one durable reminder per activation.
            await warmActivations(fixture.silo, refsFor(Reminded.type, count), 'current');
            const started = performance.now();
            const passes = 10;
            for (let i = 0; i < passes; i++) {
                fixture.clock.advance(3_600_000);
                // Let the tick's async storage work settle before the next.
                await new Promise((resolve) => setTimeout(resolve, 0));
            }
            const elapsed = performance.now() - started;
            return [
                {
                    name: `n=${count}/tick_ms`,
                    value: elapsed / passes,
                    unit: 'ms',
                    direction: 'lower'
                }
            ];
        } finally {
            await fixture.stop();
        }
    }
};

export const lifecycleScenarios: Scenario[] = [coldActivation, sweeperScan, reminderTick];
