/**
 * A `worker_threads` pool, shared by every activation on this host.
 *
 * A host is ONE Node process with ONE JS thread, so every actor turn on it
 * runs on the same loop. That has a consequence bigger than throughput: a
 * turn that computes for 500ms does not merely slow its own actor, it
 * stalls the loop — every other actor on the host, the health probe, the
 * membership heartbeat and every in-flight wire call wait behind it. On a
 * single-threaded host one greedy actor is a host-wide problem.
 *
 * `defineWorker` does not fix that. It gives a key many ACTIVATIONS, not many
 * threads, so members interleave at `await` points on the same loop and
 * contend for the same core — measured on a 1-core host at 289 ops/s for
 * the pool and 289 for a single activation, i.e. exactly nothing.
 *
 * Threads are the direct answer: the actor keeps identity, state and turn
 * ordering; the thread does pure arithmetic and hands a value back. Pair it
 * with `defineWorker` and a single key gets several turns in flight, each
 * on its own core — which is the only combination here that is genuinely
 * parallel.
 *
 * MODULE scope on purpose. `methods:` runs once per activation (per pool
 * member!), so a pool built there would spawn threads per actor and be
 * catastrophic. One pool per process, lazily, is the shape.
 */
import { Worker } from 'node:worker_threads';
import { availableParallelism } from 'node:os';

/**
 * Evaluated rather than loaded from a file: the app is bundled, so a
 * sibling `.js` would need to survive the build and be resolvable at
 * runtime under a hashed name. `eval` wraps the source as CommonJS, hence
 * `require` rather than `import`.
 */
const WORKER_SOURCE = `
const { parentPort } = require('node:worker_threads');
const { createHash } = require('node:crypto');
parentPort.on('message', ({ id, text, iterations }) => {
    let hash = createHash('sha256').update(text).digest();
    for (let i = 0; i < iterations; i++) hash = createHash('sha256').update(hash).digest();
    parentPort.postMessage({ id, digest: hash.toString('hex') });
});
`;

interface Job {
    readonly text: string;
    readonly iterations: number;
    readonly resolve: (digest: string) => void;
    readonly reject: (error: Error) => void;
}

class ThreadPool {
    readonly #idle: Worker[] = [];
    readonly #queue: Job[] = [];
    /** In flight per worker, so a dead thread fails its caller rather than
     *  leaving a promise that never settles. */
    readonly #busy = new Map<Worker, Job>();
    #seq = 0;

    constructor(readonly size: number) {
        for (let i = 0; i < size; i++) this.#idle.push(this.#spawn());
    }

    #spawn(): Worker {
        const worker = new Worker(WORKER_SOURCE, { eval: true });
        worker.on('message', ({ id, digest }: { id: number; digest: string }) => {
            void id;
            const job = this.#busy.get(worker);
            if (!job) return;
            this.#busy.delete(worker);
            job.resolve(digest);
            this.#release(worker);
        });
        worker.on('error', (error) => {
            // A wedged or crashed thread must fail its caller AND be
            // replaced, or the pool silently loses capacity one thread at a
            // time until everything queues forever.
            const job = this.#busy.get(worker);
            this.#busy.delete(worker);
            job?.reject(error instanceof Error ? error : new Error(String(error)));
            void worker.terminate();
            this.#release(this.#spawn());
        });
        // Never hold the process open: a pool is a helper, not a reason to
        // stay alive through shutdown.
        worker.unref();
        return worker;
    }

    #release(worker: Worker): void {
        const next = this.#queue.shift();
        if (!next) {
            this.#idle.push(worker);
            return;
        }
        this.#dispatch(worker, next);
    }

    #dispatch(worker: Worker, job: Job): void {
        this.#busy.set(worker, job);
        worker.postMessage({ id: this.#seq++, text: job.text, iterations: job.iterations });
    }

    run(text: string, iterations: number): Promise<string> {
        return new Promise<string>((resolve, reject) => {
            const job: Job = { text, iterations, resolve, reject };
            const worker = this.#idle.pop();
            if (worker) this.#dispatch(worker, job);
            else this.#queue.push(job);
        });
    }
}

let pool: ThreadPool | null = null;

/**
 * Threads in the pool. `availableParallelism()` reports the NODE's cores,
 * not the container's CPU limit — so a host capped at 1000m gets threads it
 * cannot run, and the cap, not this number, is what decides throughput.
 * `DIGEST_THREADS` exists to set it to the cgroup's real allowance.
 */
const SIZE = Math.max(1, Number(process.env.DIGEST_THREADS) || availableParallelism());

/** Built on first use, so a deployment that never calls it spawns nothing. */
export function digestPool(): ThreadPool {
    pool ??= new ThreadPool(SIZE);
    return pool;
}

export const poolSize = (): number => SIZE;
