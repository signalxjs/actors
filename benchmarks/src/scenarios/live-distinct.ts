/**
 * Distinct deliveries on a live read (#442) — as COUNTS, so it gates.
 *
 * A `$live` watch re-invokes its read after every mutating turn and pushes
 * the result to every subscriber; on the socket path a push is one `writev`
 * per subscriber, the term the 2026-08-14 profile put at 77–83% of a fan-out
 * host's busy time. When the re-read returns what the last delivery carried
 * — the turn touched state the read never looks at — the loop now pushes
 * nothing. Three invariants, each deterministic by construction:
 *
 *  - `emissions_per_unrelated_mutation`: 0 under the default, 1 under a
 *    `distinct: false` declaration (the control arm — proves the read WAS
 *    re-run and the dedupe is what suppressed the delivery);
 *  - `emissions_per_relevant_mutation`: 1 under both;
 *  - `reads_per_mutation`: 1 under both — the dedupe saves deliveries, never
 *    a read, so a mutating turn still costs its re-read.
 *
 * `throttleMs: 0` and one mutation at a time, so every mutation is its own
 * boundary and the counts are per mutation rather than per window.
 */
import { defineActor } from '@sigx/actors';
import { benchCall, createBenchHost } from '../host-fixture.ts';
import type { Metric, Scenario } from '../types.ts';

let reads = 0;

function watched(distinct: boolean) {
    return defineActor({
        type: distinct ? 'DistinctCart' : 'ChattyCart',
        allowAnonymous: true,
        state: () => ({ items: [] as string[], views: 0 }),
        ...(distinct ? {} : { watches: { total: { distinct: false as const } } }),
        methods: (ctx) => ({
            async total() {
                reads++;
                return ctx.state.items.length;
            },
            async add(item: string) {
                ctx.state.items.push(item);
                await ctx.save();
            },
            /** A mutation `total()` cannot see. */
            async view() {
                ctx.state.views++;
                await ctx.save();
            }
        })
    });
}

const MUTATIONS = 50;

/** One macrotask boundary — the smallest step that lets queued microtasks and a `throttleMs: 0` window run. */
const drain = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

/**
 * Wait for the loop's RE-READ, not for time to pass: after a mutating turn
 * the read counter moves exactly once, and only after it has can an
 * emission (or its deliberate absence) be counted. Bounded so a broken loop
 * fails the scenario instead of hanging it; the bound is generous because
 * it is never reached on a working one. Then one more boundary, so a push
 * that follows the read has landed in the consumer before the count is read.
 */
async function afterReRead(expected: number): Promise<void> {
    for (let i = 0; i < 1_000 && reads < expected; i++) await drain();
    if (reads < expected) throw new Error(`live/distinct: the watch never re-read (reads=${reads}, expected ${expected})`);
    await drain();
}

const distinctDeliveries: Scenario = {
    name: 'live/distinct',
    description:
        'deliveries and reads per mutating turn under one $live watch — unrelated vs relevant mutations, distinct (default) vs distinct: false (exact)',
    async run(): Promise<Metric[]> {
        const metrics: Metric[] = [];
        for (const [label, distinct] of [
            ['distinct', true],
            ['chatty', false]
        ] as const) {
            const def = watched(distinct);
            const fixture = await createBenchHost({ actors: [def] });
            const controller = new AbortController();
            try {
                const ref = { type: def.type, key: 'w' };
                const call = benchCall();
                await fixture.host.dispatch(ref, 'view', [], call);
                const watch = fixture.host.dispatchWatch?.(
                    ref,
                    'total',
                    [],
                    benchCall({ abortSignal: controller.signal }),
                    { throttleMs: 0 }
                );
                if (!watch) throw new Error('host.dispatchWatch is missing — no $live path to measure.');
                let emissions = 0;
                let first: Promise<void> | undefined;
                let firstSeen!: () => void;
                first = new Promise<void>((resolve) => {
                    firstSeen = resolve;
                });
                const consumer = (async () => {
                    try {
                        for await (const _ of watch) {
                            emissions++;
                            if (emissions === 1) firstSeen();
                            if (controller.signal.aborted) break;
                        }
                    } catch {
                        // Aborted at teardown — expected.
                    }
                })();
                await first;

                emissions = 0;
                reads = 0;
                for (let i = 0; i < MUTATIONS; i++) {
                    await fixture.host.dispatch(ref, 'view', [], call);
                    await afterReRead(i + 1);
                }
                const unrelatedEmissions = emissions;
                const unrelatedReads = reads;

                emissions = 0;
                reads = 0;
                for (let i = 0; i < MUTATIONS; i++) {
                    await fixture.host.dispatch(ref, 'add', [`i${i}`], call);
                    await afterReRead(i + 1);
                }
                const relevantEmissions = emissions;

                metrics.push(
                    {
                        name: `${label}/emissions_per_unrelated_mutation`,
                        value: unrelatedEmissions / MUTATIONS,
                        unit: 'count',
                        direction: 'lower',
                        exact: true
                    },
                    {
                        name: `${label}/emissions_per_relevant_mutation`,
                        value: relevantEmissions / MUTATIONS,
                        unit: 'count',
                        direction: 'lower',
                        exact: true
                    },
                    {
                        // The dedupe saves deliveries, never reads.
                        name: `${label}/reads_per_mutation`,
                        value: unrelatedReads / MUTATIONS,
                        unit: 'count',
                        direction: 'lower',
                        exact: true
                    }
                );
                controller.abort();
                await fixture.host.dispatch(ref, 'add', ['wake'], call);
                await Promise.race([consumer, new Promise((r) => setTimeout(r, 2_000))]);
            } finally {
                controller.abort();
                await fixture.stop();
            }
        }
        return metrics;
    }
};

export const liveDistinctScenarios: Scenario[] = [distinctDeliveries];
