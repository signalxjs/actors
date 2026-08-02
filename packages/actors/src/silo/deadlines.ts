/**
 * Call-deadline enforcement without a host timer per call.
 *
 * `raceDeadline` used to cost every dispatch an `async` wrapper, a
 * `Promise.race`, an extra promise, and a `setTimeout`/`clearTimeout` pair —
 * measured at 38% of `dispatch/warm-grain` throughput, and paid by every
 * production call because `callTimeoutMs` defaults to 30s. A 30s deadline
 * does not need millisecond firing, so far deadlines share ONE recurring
 * coarse timer and a bucket registry; per call that leaves a promise, a
 * small entry object, and an O(1) push/swap-pop.
 *
 * Near deadlines (short explicit budgets, wire hops arriving nearly spent)
 * keep an exact per-call timer — there, precision is the contract.
 *
 * Firing is never early; a far deadline may fire up to ~2 ticks late. The
 * `deadline` field itself is what crosses hops, so remaining-ms budgets on
 * the wire are unaffected by when the local timer happens to fire.
 *
 * Host timers on purpose, not the `ActorScheduler` — see the note on
 * `SiloDefaults.callTimeoutMs`. The recurring timer is armed only while
 * calls are pending and is disarmed lazily (re-arm-on-tick only when
 * non-empty): clearing it eagerly on empty would reintroduce a
 * `setTimeout`/`clearTimeout` pair per call under sequential load, and the
 * cost of laziness is at most one final unref'd quiet tick.
 */
import { ActorCallTimeoutError } from '../errors';
import { actorLabel, type ActorRef } from '../types';

/** Far-path granularity. Never fires early; may fire ≤ ~2 ticks late. */
const COARSE_TICK_MS = 1_000;
/** Below this remaining budget, precision beats allocation: exact timer. */
const EXACT_BELOW_MS = 10_000;

interface Pending {
    /** Budget at registration — keeps the timeout message's ms accurate. */
    remaining: number;
    ref: ActorRef;
    method: string;
    reject: (reason: unknown) => void;
    /** Bucket key, so removal can drop an emptied bucket from the map. */
    key: number;
    /** `null` once settled or fired — makes remove/fire idempotent. */
    bucket: Pending[] | null;
    index: number;
}

export class CallDeadlines {
    /** deadline bucket (ceil'd tick index) → pending entries. */
    #buckets = new Map<number, Pending[]>();
    #size = 0;
    #timer: ReturnType<typeof setTimeout> | undefined;

    /**
     * Settle like `work`, unless the deadline passes first — then reject
     * with the timeout while the turn keeps running. The turn is never
     * killed, so `work`'s own late settlement must never surface as
     * unhandled: every branch attaches handlers to it.
     */
    race(
        work: Promise<unknown>,
        deadline: number,
        ref: ActorRef,
        method: string
    ): Promise<unknown> {
        const remaining = deadline - Date.now();
        if (remaining <= 0) {
            work.catch(() => {});
            return Promise.reject(new ActorCallTimeoutError(actorLabel(ref), method, 0));
        }
        if (remaining < EXACT_BELOW_MS) {
            return new Promise((resolve, reject) => {
                const timer = setTimeout(
                    () => reject(new ActorCallTimeoutError(actorLabel(ref), method, remaining)),
                    remaining
                );
                (timer as { unref?: () => void }).unref?.();
                work.then(
                    (value) => {
                        clearTimeout(timer);
                        resolve(value);
                    },
                    (error) => {
                        clearTimeout(timer);
                        reject(error);
                    }
                );
            });
        }
        return new Promise((resolve, reject) => {
            const entry = this.#add(deadline, remaining, ref, method, reject);
            work.then(
                (value) => {
                    this.#remove(entry);
                    resolve(value);
                },
                (error) => {
                    this.#remove(entry);
                    reject(error);
                }
            );
        });
    }

    dispose(): void {
        if (this.#timer !== undefined) {
            clearTimeout(this.#timer);
            this.#timer = undefined;
        }
    }

    #add(
        deadline: number,
        remaining: number,
        ref: ActorRef,
        method: string,
        reject: (reason: unknown) => void
    ): Pending {
        // Ceil: a bucket's boundary is ≥ every deadline inside it, so a
        // fired bucket can never reject a call whose deadline is still ahead.
        const key = Math.ceil(deadline / COARSE_TICK_MS);
        let bucket = this.#buckets.get(key);
        if (bucket === undefined) {
            bucket = [];
            this.#buckets.set(key, bucket);
        }
        const entry: Pending = { remaining, ref, method, reject, key, bucket, index: bucket.length };
        bucket.push(entry);
        this.#size++;
        if (this.#timer === undefined) this.#arm();
        return entry;
    }

    #arm(): void {
        this.#timer = setTimeout(() => this.#tick(), COARSE_TICK_MS);
        (this.#timer as { unref?: () => void }).unref?.();
    }

    #tick(): void {
        this.#timer = undefined;
        const now = Date.now();
        for (const [key, bucket] of this.#buckets) {
            if (key * COARSE_TICK_MS > now) continue;
            this.#buckets.delete(key);
            for (const entry of bucket) {
                if (entry.bucket === null) continue;
                entry.bucket = null;
                this.#size--;
                entry.reject(
                    new ActorCallTimeoutError(actorLabel(entry.ref), entry.method, entry.remaining)
                );
            }
        }
        if (this.#size > 0) this.#arm();
    }

    #remove(entry: Pending): void {
        const bucket = entry.bucket;
        if (bucket === null) return;
        entry.bucket = null;
        this.#size--;
        // Swap-pop: 1M calls/s against a 30s horizon would otherwise retain
        // tens of millions of settled entries until their bucket fires.
        const last = bucket.pop();
        if (last !== undefined && last !== entry) {
            bucket[entry.index] = last;
            last.index = entry.index;
        }
        if (bucket.length === 0) this.#buckets.delete(entry.key);
    }
}
