/**
 * Stateless workers: the two invariants issue #243 promised, gated exactly.
 *
 * 1. `directory_ops == 0` — a stateless dispatch never touches the cluster
 *    directory: no claim, no lookup, no release, at any N. The stateful
 *    baseline is 2 per cold activation (`cluster/directory-ops-per-
 *    activation`); the worker's zero is structural (the placement answers
 *    local before `#resolveTarget` runs), so this is an invariant of the
 *    routing design, not a measurement of this machine.
 *
 * 2. `pool_members == maxLocal` under saturation — pressure-driven growth
 *    stops exactly at the cap. Saturation is BY CONSTRUCTION: every call
 *    parks on a latch, and all of them are dispatched before the latch
 *    opens, so the pool must grow to the cap and not one member past it.
 *
 * Both metrics are deterministic-by-construction (counters over fixed
 * loops; `maxLocal` is always explicit — the CPU-derived default would be
 * machine-dependent), which is the `Metric.exact` contract.
 */
import { defineWorker } from '@sigx/actors';
import { createCluster, selfPolicy } from '../cluster-harness.ts';
import { benchCall, createBenchHost } from '../host-fixture.ts';
import type { Metric, RunContext, Scenario } from '../types.ts';

function sizesFor(ctx: RunContext): number[] {
    return ctx.quick ? [1, 10] : [1, 10, 100];
}

const POOL_CAP = 4;

const Work = defineWorker({
    type: 'StatelessBench',
    unguarded: true,
    maxLocal: POOL_CAP,
    methods: () => ({
        async noop() {}
    })
});

const statelessDirectoryOps: Scenario = {
    name: 'cluster/stateless-directory-ops',
    description: 'directory calls per stateless dispatch vs N — must be zero',
    async run(ctx: RunContext): Promise<Metric[]> {
        const calls = ctx.quick ? 50 : 200;
        const metrics: Metric[] = [];
        for (const n of sizesFor(ctx)) {
            const harness = await createCluster(n, { actors: [Work], policy: selfPolicy });
            try {
                const host = harness.hosts[0] as (typeof harness.hosts)[number];
                const call = benchCall();
                harness.counter.reset();
                // Cold keys and a repeated hot key — neither may touch the
                // directory.
                for (let i = 0; i < calls; i++) {
                    const key = i % 2 === 0 ? `g${i}` : 'hot';
                    await host.dispatch({ type: Work.type, key }, 'noop', [], call);
                }
                metrics.push({
                    name: `n=${n}/directory_ops`,
                    value: harness.counter.sum('directory.') / calls,
                    unit: 'count',
                    direction: 'lower',
                    // The issue's headline invariant: a stateless dispatch
                    // performs ZERO directory operations. One round-trip
                    // appearing here means the always-local short-circuit
                    // regressed, and that cannot be seen by a timing.
                    exact: true,
                    noiseFloor: 0.1
                });
            } finally {
                await harness.stop();
            }
        }
        return metrics;
    }
};

const statelessPoolCap: Scenario = {
    name: 'dispatch/stateless-pool-cap',
    description: `pool growth under saturation — exactly maxLocal (${POOL_CAP}) members`,
    async run(ctx: RunContext): Promise<Metric[]> {
        const calls = ctx.quick ? 32 : 64;
        // Local to the run: the entered-counter must close over THIS run.
        let entered = 0;
        let saturate!: () => void;
        const saturatedTurns = new Promise<void>((r) => (saturate = r));
        const Parked = defineWorker({
            type: 'StatelessPool',
            unguarded: true,
            maxLocal: POOL_CAP,
            methods: () => ({
                async park(gate: Promise<void>) {
                    if (++entered === POOL_CAP) saturate();
                    await gate;
                }
            })
        });
        const fixture = await createBenchHost({ actors: [Parked] });
        try {
            let release!: () => void;
            const gate = new Promise<void>((r) => (release = r));
            // Every call is dispatched before the latch opens: all members
            // are busy from the first turn on, so growth is pressure-driven
            // on every dispatch and MUST stop at the cap.
            const inFlight = Array.from({ length: calls }, () =>
                fixture.host.dispatch(
                    { type: Parked.type, key: 'saturated' },
                    'park',
                    [gate],
                    benchCall()
                )
            );
            // Wait until `maxLocal` turns are IN their bodies — the pool is
            // saturated and every member is active, deterministically.
            await saturatedTurns;
            const saturated = fixture.host.stats().perType[Parked.type] ?? 0;
            release();
            await Promise.all(inFlight);
            const drained = fixture.host.stats().perType[Parked.type] ?? 0;
            return [
                {
                    name: 'pool_members',
                    value: saturated,
                    unit: 'count',
                    direction: 'lower',
                    // `== maxLocal` exactly: one MORE is a cap violation, one
                    // FEWER means the spin-up rule stopped growing under
                    // pressure — both are correctness bugs, not perf drift.
                    exact: true,
                    noiseFloor: 0.1
                },
                {
                    name: 'pool_members_after_drain',
                    value: drained,
                    unit: 'count',
                    direction: 'lower',
                    // Draining must not have grown the pool either.
                    exact: true,
                    noiseFloor: 0.1
                }
            ];
        } finally {
            await fixture.stop();
        }
    }
};

export const statelessScenarios: Scenario[] = [statelessDirectoryOps, statelessPoolCap];
