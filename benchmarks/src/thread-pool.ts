/**
 * A minimal `worker_threads` pool, for pricing the crossing (#119).
 *
 * Deliberately in `benchmarks/`, not in the runtime. #119 asks for "a
 * benchmark first, not an implementation", and the question it has to answer
 * comes BEFORE any runtime seam: at what payload does shipping a turn's
 * compute to another thread beat running it on the loop? If the crossover
 * sits above realistic payloads, the right outcome is to close #119 with the
 * number rather than build anything.
 *
 * So this is as thin as it can be while still being honest — one round trip
 * per call, `postMessage` both ways, structured clone in both directions.
 * A real implementation would want backpressure, worker restart on throw,
 * and a transfer-list fast path for binary payloads; every one of those makes
 * the crossing CHEAPER or safer, never more expensive, so a floor measured
 * here is a floor the runtime could also reach.
 */
import { Worker } from 'node:worker_threads';

export interface ThreadPool {
    run(payload: string, iterations: number): Promise<number>;
    /** Terminate every worker. Must be awaited, or the process will not exit. */
    close(): Promise<void>;
}

interface Pending {
    resolve(value: number): void;
    reject(error: unknown): void;
}

/**
 * `size` workers, dispatched round-robin.
 *
 * Round-robin rather than least-busy on purpose: the scenarios below issue
 * one call at a time per arm, so a queueing policy would never be exercised
 * and its cost would just be noise on the measurement.
 */
export function createThreadPool(size: number): ThreadPool {
    const url = new URL('./compute-worker.mjs', import.meta.url);
    const workers = Array.from({ length: size }, () => new Worker(url));
    const pending = new Map<number, Pending>();
    let nextId = 0;
    let cursor = 0;

    for (const worker of workers) {
        worker.on('message', (msg: { id: number; value: number }) => {
            const p = pending.get(msg.id);
            if (!p) return;
            pending.delete(msg.id);
            p.resolve(msg.value);
        });
        // A worker that dies takes every in-flight call with it; failing loudly
        // beats a scenario that hangs and reports as a timed-out run.
        worker.on('error', (error) => {
            for (const [id, p] of pending) {
                pending.delete(id);
                p.reject(error);
            }
        });
    }
    // Deliberately NOT `unref()`ed. An unref'd worker does not hold the event
    // loop open, so a `postMessage` awaiting its reply lets Node decide it has
    // nothing left to do and exit mid-run — which it does, with "unsettled
    // top-level await" and exit code 13. `close()` in the scenario's `finally`
    // is what stops these from outliving the run.

    return {
        run(payload, iterations) {
            const id = nextId++;
            const worker = workers[cursor++ % workers.length]!;
            return new Promise<number>((resolve, reject) => {
                pending.set(id, { resolve, reject });
                worker.postMessage({ id, payload, iterations });
            });
        },
        async close() {
            await Promise.all(workers.map((w) => w.terminate()));
        }
    };
}
