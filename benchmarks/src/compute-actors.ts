/**
 * CPU-bound fixtures for #119 — the three ways a host can run a turn's
 * compute, held identical in everything but that.
 *
 * In their own file rather than `actors.ts` because they are the only
 * fixtures here whose BODY is the variable under test: everywhere else the
 * body is kept as close to zero as possible so the runtime is what gets
 * measured, and mixing the two sets under that file's "kept deliberately
 * trivial" header would be misleading.
 */
import { defineActor, defineWorker } from '@sigx/actors';
import type { ThreadPool } from './thread-pool.ts';

/**
 * The work. Must stay byte-identical to the copy in `compute-worker.mjs` —
 * the whole comparison is "the same work, here or there".
 *
 * A hash loop rather than something with a library behind it: it is pure
 * CPU, it cannot be optimised away (the result is returned), and it is what
 * the Tier-3 `infra/worker-pool` arm already uses, so the numbers relate.
 */
export function hashIterations(payload: string, iterations: number): number {
    let h = 0;
    for (let i = 0; i < iterations; i++) {
        h = (h << 5) - h + (payload.charCodeAt(i % payload.length) | 0);
        h |= 0;
    }
    return h;
}

/** One activation, compute on the event loop. The control. */
export const CpuActor = defineActor({
    type: 'BenchCpuActor',
    allowAnonymous: true,
    state: () => ({ n: 0 }),
    methods: () => ({
        compute(payload: string, iterations: number) {
            return hashIterations(payload, iterations);
        }
    })
});

/**
 * The same body behind a `defineWorker` pool. This is the arm #119 reports at
 * 1.07× against the control: `maxLocal` multiplies MAILBOXES, which is
 * concurrency at `await` points, and a synchronous hash loop never yields —
 * so eight members share one thread and buy nothing.
 */
export const CpuPool = defineWorker({
    type: 'BenchCpuPool',
    allowAnonymous: true,
    maxLocal: 8,
    methods: () => ({
        compute(payload: string, iterations: number) {
            return hashIterations(payload, iterations);
        }
    })
});

/**
 * The modelled arm: the same body, shipped to a `worker_threads` pool.
 *
 * The pool is injected rather than owned, so the scenario controls its
 * lifetime — a worker left running past a scenario is work nobody is
 * accounting for in every later measurement.
 */
export function makeThreadedWorker(pool: ThreadPool): ReturnType<typeof defineWorker> {
    return defineWorker({
        type: 'BenchCpuThreaded',
        allowAnonymous: true,
        maxLocal: 8,
        methods: () => ({
            compute(payload: string, iterations: number) {
                return pool.run(payload, iterations);
            }
        })
    });
}
