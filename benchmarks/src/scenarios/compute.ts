/**
 * Does moving a turn's compute to another thread pay? (#119)
 *
 * `defineWorker` multiplies MAILBOXES, which is concurrency at `await`
 * points. A host is one process with one JS thread, so a pool of eight over
 * a synchronous hash loop shares one core and buys nothing — Tier 3 measured
 * 1.07x against a single activation, with seven cores idle.
 *
 * #119 asks for "a benchmark first, not an implementation", and this is it.
 * Nothing here changes the runtime: the threaded arm drives a
 * `worker_threads` pool from inside an ordinary actor method
 * (`compute-actors.ts`), which is enough to price the CROSSING. If the
 * crossover sits above realistic payloads, the right outcome is to close
 * #119 with the number attached rather than build a seam for it.
 *
 * **Opt-in, like Tier 2**, via `BENCH_THREADS=1`. Two reasons, and the
 * second is the important one: it spawns 8 OS threads, which is a bigger
 * footprint than the rest of the suite takes; and the CI runner has ~2
 * usable cores, where a thread-parallelism measurement is not merely noisy
 * but *structurally* incapable of showing the effect. A number that cannot
 * be right on the machine that records it should not be recorded there.
 * That is the same reason the Tier-3 arm reads 1.07x: its node has ~1.1
 * usable cores.
 *
 * Read `thread_speedup` and ignore the absolutes. It is the ratio of two
 * arms measured back to back in one process, which is the only part a
 * contended machine cannot distort much — and it is the number the decision
 * turns on.
 */
import { CpuActor, CpuPool, makeThreadedWorker } from '../compute-actors.ts';
import { createThreadPool, type ThreadPool } from '../thread-pool.ts';
import { closedLoop } from '../loop.ts';
import { benchCall, createBenchHost } from '../host-fixture.ts';
import type { Metric, RunContext, Scenario } from '../types.ts';

/** Matches the Tier-3 arm's shape: 8 members, and 8 threads to match. */
const POOL_SIZE = 8;

/** Enough in flight to occupy every member; fewer would measure queueing. */
const CONCURRENCY = 8;

/**
 * `150_000` is the Tier-3 figure, so the rows relate. The smaller two exist
 * to find where the fixed cost of a crossing stops being worth paying.
 */
const ITERATION_LADDER = [1_000, 10_000, 150_000] as const;

/** Bytes of argument shipped per call — structured-cloned in both directions. */
const PAYLOAD_LADDER = [64, 16 * 1024, 1024 * 1024] as const;

const payloadOf = (bytes: number): string => 'x'.repeat(bytes);

/** One arm: N concurrent calls of the same body against one warm actor. */
async function armOpsPerSec(
    def: Parameters<typeof createBenchHost>[0]['actors'][number],
    payload: string,
    iterations: number,
    durationMs: number
): Promise<number> {
    const fixture = await createBenchHost({ actors: [def] });
    try {
        const ref = { type: def.type, key: 'cpu' };
        const call = benchCall();
        const run = (): Promise<unknown> =>
            fixture.host.dispatch(ref, 'compute', [payload, iterations], call);
        await run();
        const outcome = await closedLoop({
            call: run,
            concurrency: CONCURRENCY,
            durationMs,
            latency: false
        });
        return outcome.opsPerSec;
    } finally {
        await fixture.stop();
    }
}

/** Terminate the pool whatever happens — an orphan worker outlives the run. */
async function withPool<T>(fn: (pool: ThreadPool) => Promise<T>): Promise<T> {
    const pool = createThreadPool(POOL_SIZE);
    try {
        return await fn(pool);
    } finally {
        await pool.close();
    }
}

/**
 * Compute size vs the fixed cost of a crossing, at a payload small enough
 * that cloning is not the story (64 B).
 */
const threadCrossover: Scenario = {
    name: 'compute/thread-crossover',
    description: 'CPU-bound turns: one activation vs a defineWorker pool vs worker_threads',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        const payload = payloadOf(64);
        const ladder = ctx.quick ? ITERATION_LADDER.slice(0, 2) : ITERATION_LADDER;
        for (const iterations of ladder) {
            const actor = await armOpsPerSec(CpuActor, payload, iterations, ctx.durationMs);
            const pool = await armOpsPerSec(CpuPool, payload, iterations, ctx.durationMs);
            const threaded = await withPool((p) =>
                armOpsPerSec(makeThreadedWorker(p), payload, iterations, ctx.durationMs)
            );
            metrics.push(
                {
                    name: `iters=${iterations}/actor_ops_per_sec`,
                    value: actor,
                    unit: 'ops/s',
                    direction: 'higher'
                },
                {
                    name: `iters=${iterations}/pool_ops_per_sec`,
                    value: pool,
                    unit: 'ops/s',
                    direction: 'higher'
                },
                {
                    name: `iters=${iterations}/threaded_ops_per_sec`,
                    value: threaded,
                    unit: 'ops/s',
                    direction: 'higher'
                },
                {
                    // The decision number: what threads buy over the pool we
                    // already ship, on the same body at the same size.
                    name: `iters=${iterations}/thread_speedup`,
                    value: pool === 0 ? 0 : threaded / pool,
                    unit: 'x',
                    direction: 'higher',
                    noiseFloor: 0.05
                }
            );
        }
        return metrics;
    }
};

/**
 * Payload size at a fixed, generous compute (the Tier-3 150k iterations).
 *
 * The single-activation control is dropped here: it is flat in payload and
 * would only lengthen the run. What this asks is narrower — given compute
 * worth moving, how much argument can you ship before structured clone in
 * BOTH directions eats the win?
 */
const threadPayload: Scenario = {
    name: 'compute/thread-payload',
    description: 'worker_threads vs a defineWorker pool by payload size, at 150k iterations',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        const iterations = 150_000;
        const ladder = ctx.quick ? PAYLOAD_LADDER.slice(0, 2) : PAYLOAD_LADDER;
        for (const bytes of ladder) {
            const payload = payloadOf(bytes);
            const pool = await armOpsPerSec(CpuPool, payload, iterations, ctx.durationMs);
            const threaded = await withPool((p) =>
                armOpsPerSec(makeThreadedWorker(p), payload, iterations, ctx.durationMs)
            );
            const label = bytes >= 1024 ? `${bytes / 1024}k` : `${bytes}b`;
            metrics.push(
                {
                    name: `payload=${label}/pool_ops_per_sec`,
                    value: pool,
                    unit: 'ops/s',
                    direction: 'higher'
                },
                {
                    name: `payload=${label}/threaded_ops_per_sec`,
                    value: threaded,
                    unit: 'ops/s',
                    direction: 'higher'
                },
                {
                    name: `payload=${label}/thread_speedup`,
                    value: pool === 0 ? 0 : threaded / pool,
                    unit: 'x',
                    direction: 'higher',
                    noiseFloor: 0.05
                }
            );
        }
        return metrics;
    }
};

export const computeScenarios: Scenario[] = [threadCrossover, threadPayload];
