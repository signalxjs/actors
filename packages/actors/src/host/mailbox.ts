/**
 * The per-activation mailbox — turn-based concurrency as a promise chain.
 *
 * Guarantees (the virtual-actor core promise, restated for JS), for the
 * DEFAULT serial lane:
 *  - One turn at a time per activation. A turn is one method invocation from
 *    dequeue to promise settlement; no two serial turns of one activation
 *    ever interleave, so plain field mutation on actor state is race-free.
 *  - `await` inside a turn does NOT release the mailbox — an awaited fetch
 *    holds every queued message until it settles (non-reentrant default).
 *  - A failed turn never poisons the queue: the next turn runs regardless.
 *
 * `run(turn, true)` is the INTERLEAVED lane (`reentrant: 'always'` /
 * `methodReentrancy`): the turn launches immediately — it neither waits for
 * the serial tail nor holds it — so interleaved turns overlap each other AND
 * serial turns; only serial turns stay mutually exclusive with each other.
 * Both lanes share `depth` (unsettled turns: queued + running), `close()`
 * (refuses both kinds) and `drain()` (all in-flight turns of both lanes
 * settled).
 */
import { HostShutdownError } from '../errors';

export class Mailbox {
    #tail: Promise<unknown> = Promise.resolve();
    /** Unsettled interleaved turns (never-rejecting guards). */
    #inflight = new Set<Promise<void>>();
    #depth = 0;
    #closed = false;

    /** Queued + running turns, across both lanes. */
    get depth(): number {
        return this.#depth;
    }

    get closed(): boolean {
        return this.#closed;
    }

    /** Enqueue one turn; returns that turn's own settlement. */
    run<T>(turn: () => T | Promise<T>, interleave = false): Promise<T> {
        if (this.#closed) return Promise.reject(new HostShutdownError());
        this.#depth++;
        if (interleave) {
            // Launched on its own microtask — the same never-synchronous-
            // from-enqueue property the serial lane has via `#tail.then()` —
            // and NOT chained on the tail in either direction.
            const settled = Promise.resolve().then(turn);
            const guard: Promise<void> = settled.then(
                () => {
                    this.#depth--;
                    this.#inflight.delete(guard);
                },
                // The turn's failure belongs to ITS caller only.
                () => {
                    this.#depth--;
                    this.#inflight.delete(guard);
                }
            );
            this.#inflight.add(guard);
            return settled;
        }
        const result = this.#tail.then(
            () => turn(),
            // The previous turn's failure belongs to ITS caller only.
            () => turn()
        );
        const settled = result.finally(() => {
            this.#depth--;
        });
        // The tail must never carry a rejection forward, and must include the
        // depth decrement so `drain()` observes a fully settled queue.
        this.#tail = settled.catch(() => {});
        return settled;
    }

    /** Refuse new turns; queued and in-flight turns still run. */
    close(): void {
        this.#closed = true;
    }

    /**
     * Resolves when every turn enqueued SO FAR — serial and interleaved —
     * has settled. Call `close()` first for a final drain — otherwise later
     * `run()`s are not covered.
     */
    async drain(): Promise<void> {
        // New turns may land while awaiting; loop until BOTH lanes are
        // stable: the tail unchanged and no interleaved turn in flight.
        for (;;) {
            const tail = this.#tail;
            const inflight = [...this.#inflight];
            await tail;
            await Promise.all(inflight);
            if (tail === this.#tail && this.#inflight.size === 0) return;
        }
    }
}
