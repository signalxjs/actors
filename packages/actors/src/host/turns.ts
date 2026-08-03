/**
 * One activation's turns — what makes actor state race-free without a lock.
 *
 * A **turn** is one method invocation, from the moment it starts running to
 * the moment its promise settles.
 *
 * By default turns run one at a time, in arrival order:
 *  - Nothing else on that activation runs while a turn is running, so plain
 *    mutation of actor state is safe. There is nothing to lock.
 *  - `await` does NOT end the turn. An awaited fetch holds every later turn
 *    until it settles — slow I/O inside a turn is a queue for that actor.
 *  - A failed turn never poisons what follows; the next one runs regardless.
 *
 * `run(turn, true)` opts out (`reentrant: 'always'` / `methodReentrancy`):
 * the turn launches immediately, neither waiting for the tail nor holding it,
 * so it overlaps both other interleaved turns and serial ones. Only serial
 * turns stay mutually exclusive with each other. `depth`, `close()` and
 * `drain()` cover both kinds.
 *
 * Ordering comes from promise resolution, not a queue: `#tail` is a promise
 * that turns chain onto, and `depth` is a counter of unsettled turns.
 */
import { HostShutdownError } from '../errors';

export class Turns {
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

    /** Schedule one turn; returns that turn's own settlement. */
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
        // depth decrement so `drain()` observes every turn settled.
        this.#tail = settled.catch(() => {});
        return settled;
    }

    /** Refuse new turns; already-scheduled and running turns still finish. */
    close(): void {
        this.#closed = true;
    }

    /**
     * Resolves when every turn scheduled SO FAR — serial and interleaved —
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
