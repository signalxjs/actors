/**
 * The per-activation mailbox — turn-based concurrency as a promise chain.
 *
 * Guarantees (the virtual-actor core promise, restated for JS):
 *  - One turn at a time per activation. A turn is one method invocation from
 *    dequeue to promise settlement; no two turns of one activation ever
 *    interleave, so plain field mutation on actor state is race-free.
 *  - `await` inside a turn does NOT release the mailbox — an awaited fetch
 *    holds every queued message until it settles (non-reentrant default).
 *  - A failed turn never poisons the queue: the next turn runs regardless.
 */
import { HostShutdownError } from '../errors';

export class Mailbox {
    #tail: Promise<unknown> = Promise.resolve();
    #depth = 0;
    #closed = false;

    /** Queued + running turns. */
    get depth(): number {
        return this.#depth;
    }

    get closed(): boolean {
        return this.#closed;
    }

    /** Enqueue one turn; returns that turn's own settlement. */
    run<T>(turn: () => T | Promise<T>): Promise<T> {
        if (this.#closed) return Promise.reject(new HostShutdownError());
        this.#depth++;
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

    /** Refuse new turns; queued turns still run. */
    close(): void {
        this.#closed = true;
    }

    /**
     * Resolves when every turn enqueued SO FAR has settled. Call `close()`
     * first for a final drain — otherwise later `run()`s are not covered.
     */
    async drain(): Promise<void> {
        // New turns may land while awaiting; loop until the tail is stable.
        let tail;
        do {
            tail = this.#tail;
            await tail;
        } while (tail !== this.#tail);
    }
}
