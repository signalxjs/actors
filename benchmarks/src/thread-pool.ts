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
    let closed = false;

    /**
     * Settle everything outstanding. The map is shared across workers, so one
     * worker's death fails every in-flight call rather than only its own —
     * coarse, but this is a benchmark harness, and the alternative is a
     * scenario that parks forever and reports as a timed-out RUN instead of
     * as the error that actually happened.
     */
    const failAll = (error: unknown): void => {
        for (const [id, p] of pending) {
            pending.delete(id);
            p.reject(error);
        }
    };

    for (const worker of workers) {
        worker.on('message', (msg: { id: number; value: number }) => {
            const p = pending.get(msg.id);
            if (!p) return;
            pending.delete(msg.id);
            p.resolve(msg.value);
        });
        worker.on('error', (error) => failAll(error));
        // A worker can also go away WITHOUT an error — `exit` is the only
        // event for that, and without it a call in flight when one stops
        // never settles at all.
        worker.on('exit', (code) => {
            if (closed) return; // Expected: `close()` has already settled them.
            failAll(new Error(`[bench] compute worker exited unexpectedly (code ${code})`));
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
                try {
                    worker.postMessage({ id, payload, iterations });
                } catch (error) {
                    // `postMessage` throws synchronously on a worker that has
                    // already gone. The promise would reject on its own here,
                    // but the map entry would survive as an id nothing can
                    // ever settle.
                    pending.delete(id);
                    reject(error);
                }
            });
        },
        async close() {
            // Settle BEFORE terminating. A caller still awaiting a reply when
            // the pool closes — an arm that threw mid-loop, say — would
            // otherwise wait on a worker that no longer exists, and the
            // scenario would hang rather than surface whatever went wrong.
            closed = true;
            failAll(new Error('[bench] compute thread pool closed with calls in flight'));
            await Promise.all(workers.map((w) => w.terminate()));
        }
    };
}
