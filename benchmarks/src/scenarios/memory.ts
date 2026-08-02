/**
 * Footprint and leaks.
 *
 * Two different questions, and they need different methods:
 *
 *  - FOOTPRINT: what does one live actor cost? Settle the heap, activate N,
 *    settle again, divide the delta. The answer bounds how many actors a
 *    host can hold, which is the number people actually plan capacity on.
 *
 *  - LEAK: does a completed cycle give everything back? Activate N,
 *    deactivate all, settle, compare to the starting heap — repeated, so a
 *    single noisy cycle cannot pass or fail it alone. `retained_per_actor`
 *    is the gate: it should be ~0, and a steady positive number is a leak
 *    regardless of how small it looks per actor.
 *
 * The leak metrics straddle zero (a settled heap drifts both ways), so they
 * all carry a `noiseFloor` and are compared on ABSOLUTE difference. A
 * percentage against a baseline of ~0 is meaningless, and against a
 * negative one it inverts — which would report a new leak as an
 * improvement.
 *
 * Every reading is taken after `settleGc()`. An unsettled heap is mostly
 * garbage, and comparing garbage to garbage produces confident nonsense.
 */
import { Timered, Tiny, Large } from '../actors.ts';
import { heapSlopeBytesPerSample, heapUsed, maybeHeapSnapshot, settleGc } from '../memory.ts';
import {
    benchCall,
    createBenchHost,
    refsFor,
    requireStreamDispatch,
    warmActivations
} from '../host-fixture.ts';
import type { Metric, RunContext, Scenario } from '../types.ts';

const perGrainFootprint: Scenario = {
    name: 'mem/per-actor-footprint',
    description: 'heapUsed delta per live activation, tiny vs ~200-row state',
    async run(ctx: RunContext): Promise<Metric[]> {
        const count = ctx.quick ? 1000 : 20_000;
        const metrics: Metric[] = [];
        for (const [label, def] of [
            ['tiny', Tiny],
            ['large', Large]
        ] as const) {
            const fixture = await createBenchHost({ actors: [def] });
            try {
                await settleGc();
                const before = heapUsed();
                await warmActivations(fixture.host, refsFor(def.type, count));
                await settleGc();
                const after = heapUsed();
                metrics.push({
                    name: `${label}/bytes_per_actor`,
                    value: (after - before) / count,
                    unit: 'bytes',
                    direction: 'lower'
                });
                // Guard against measuring an empty host by accident.
                const live = fixture.host.stats().activations;
                if (live !== count) {
                    throw new Error(`expected ${count} activations, found ${live}`);
                }
            } finally {
                await fixture.stop();
            }
        }
        return metrics;
    }
};

/**
 * The core leak check. Three cycles, because one cycle cannot distinguish
 * "leaked" from "the heap happened to grow"; a leak shows up as a
 * consistent per-cycle retention.
 */
const activateDeactivateLeak: Scenario = {
    name: 'mem/leak-activate-deactivate',
    description: 'activate N → deactivate all → settle, ×3 — bytes retained per completed cycle',
    async run(ctx: RunContext): Promise<Metric[]> {
        const count = ctx.quick ? 500 : 5000;
        const cycles = 3;
        const fixture = await createBenchHost({ actors: [Tiny] });
        try {
            const refs = refsFor(Tiny.type, count);
            // One untimed cycle first: lazy module init and the storage
            // map's own growth are one-off costs, not per-cycle retention.
            await warmActivations(fixture.host, refs);
            for (const ref of refs) await fixture.host.deactivate(ref);
            await settleGc();

            const readings: number[] = [heapUsed()];
            for (let c = 0; c < cycles; c++) {
                await warmActivations(fixture.host, refs);
                for (const ref of refs) await fixture.host.deactivate(ref);
                await settleGc();
                readings.push(heapUsed());
            }

            const live = fixture.host.stats().activations;
            if (live !== 0) throw new Error(`expected 0 activations after teardown, found ${live}`);
            maybeHeapSnapshot('leak-activate-deactivate');

            const retainedPerCycle =
                ((readings[readings.length - 1] as number) - (readings[0] as number)) / cycles;
            return [
                {
                    name: 'retained_per_cycle',
                    value: retainedPerCycle,
                    unit: 'bytes',
                    direction: 'lower',
                    informational: true
                },
                // The gate. A clean cycle lands within a few bytes per
                // actor; a planted leak measured 626 B/actor, so 64 B is
                // comfortably between "GC scheduling jitter" and "leak".
                {
                    name: 'retained_per_actor',
                    value: retainedPerCycle / count,
                    unit: 'bytes',
                    direction: 'lower',
                    noiseFloor: 64
                }
            ];
        } finally {
            await fixture.stop();
        }
    }
};

/**
 * Streams take a keep-alive reference and register a subscription; both
 * must be released when the consumer goes away, or an actor that was
 * watched once never becomes idle again.
 */
const streamLeak: Scenario = {
    name: 'mem/leak-streams',
    description: 'open K change-feed streams, consume, abort — retention after release',
    async run(ctx: RunContext): Promise<Metric[]> {
        const streams = ctx.quick ? 50 : 500;
        const cycles = 3;
        const fixture = await createBenchHost({ actors: [Tiny] });
        try {
            const ref = { type: Tiny.type, key: 'watched' };
            const call = benchCall();
            await fixture.host.dispatch(ref, 'increment', [1], call);

            const openStream = requireStreamDispatch(fixture.host);
            const cycle = async (): Promise<void> => {
                const controller = new AbortController();
                const drains: Promise<void>[] = [];
                for (let i = 0; i < streams; i++) {
                    const stream = openStream(
                        ref,
                        'watch',
                        [],
                        benchCall({ abortSignal: controller.signal })
                    );
                    drains.push(
                        (async () => {
                            try {
                                for await (const _ of stream) break;
                            } catch {
                                // Aborted — expected.
                            }
                        })()
                    );
                }
                await Promise.allSettled(drains);
                controller.abort();
                // Give the abort a turn to unwind the generators.
                await new Promise((resolve) => setTimeout(resolve, 10));
            };

            await cycle();
            await settleGc();
            const before = heapUsed();
            for (let c = 0; c < cycles; c++) await cycle();
            await settleGc();
            const after = heapUsed();

            return [
                {
                    name: 'retained_per_stream',
                    value: (after - before) / (cycles * streams),
                    unit: 'bytes',
                    direction: 'lower',
                    noiseFloor: 256
                }
            ];
        } finally {
            await fixture.stop();
        }
    }
};

/** A per-activation volatile timer must die with its activation. */
const timerLeak: Scenario = {
    name: 'mem/leak-timers',
    description: 'activations carrying a volatile ctx.timer(), activated and deactivated ×3',
    async run(ctx: RunContext): Promise<Metric[]> {
        const count = ctx.quick ? 200 : 2000;
        const cycles = 3;
        const fixture = await createBenchHost({ actors: [Timered] });
        try {
            const refs = refsFor(Timered.type, count);
            const cycle = async (): Promise<void> => {
                await warmActivations(fixture.host, refs, 'current');
                for (const ref of refs) await fixture.host.deactivate(ref);
            };
            await cycle();
            await settleGc();
            const before = heapUsed();
            for (let c = 0; c < cycles; c++) await cycle();
            await settleGc();
            const after = heapUsed();
            return [
                {
                    name: 'retained_per_timer',
                    value: (after - before) / (cycles * count),
                    unit: 'bytes',
                    direction: 'lower',
                    noiseFloor: 256
                }
            ];
        } finally {
            await fixture.stop();
        }
    }
};

/**
 * The catch-all. A mixed workload against a fixed working set should reach
 * a steady heap; a positive slope that survives GC is growth the targeted
 * scenarios did not name. This is the one that catches the leak nobody
 * predicted, which is why it samples rather than measuring endpoints.
 */
const soak: Scenario = {
    name: 'mem/soak-steady-state',
    description: 'mixed workload over a fixed actor set — heap slope per sample (want ~0)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const actors = ctx.quick ? 50 : 500;
        const samples = ctx.quick ? 4 : 10;
        const perSample = ctx.quick ? 2000 : 20_000;
        const fixture = await createBenchHost({ actors: [Tiny] });
        try {
            const refs = refsFor(Tiny.type, actors);
            const call = benchCall();
            await warmActivations(fixture.host, refs);

            const readings: number[] = [];
            let i = 0;
            for (let s = 0; s < samples; s++) {
                for (let op = 0; op < perSample; op++) {
                    const ref = refs[i++ % actors] as (typeof refs)[number];
                    // Mix the paths that allocate differently: a bare turn,
                    // a mutating turn, and a full codec walk.
                    const method = op % 3 === 0 ? 'noop' : op % 3 === 1 ? 'increment' : 'snapshot';
                    await fixture.host.dispatch(ref, method, method === 'increment' ? [1] : [], call);
                }
                await settleGc();
                readings.push(heapUsed());
            }

            maybeHeapSnapshot('soak-steady-state');
            const slope = heapSlopeBytesPerSample(readings);
            return [
                // Straddles zero by design (a settled heap drifts both
                // ways), so this MUST compare on absolute difference.
                {
                    name: 'heap_slope_bytes',
                    value: slope,
                    unit: 'bytes',
                    direction: 'lower',
                    noiseFloor: 65_536
                },
                {
                    name: 'heap_final',
                    value: readings[readings.length - 1] as number,
                    unit: 'bytes',
                    direction: 'lower',
                    informational: true
                }
            ];
        } finally {
            await fixture.stop();
        }
    }
};

export const memoryScenarios: Scenario[] = [
    perGrainFootprint,
    activateDeactivateLeak,
    streamLeak,
    timerLeak,
    soak
];
