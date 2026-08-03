/**
 * The change-driven read — what makes a live subscription possible.
 *
 * `ctx.changes()` yields STATE, but a subscriber asked for a METHOD RESULT
 * (`total()`, `recent(20)`). Only the actor can derive one from the other,
 * so a watch re-invokes the read after every mutating turn rather than
 * pushing the state and hoping the client can.
 *
 * Two properties are the whole point, and neither is an optimisation:
 *
 *  - **One loop per `(method, args)`.** Fifty subscribers to the same read
 *    must cost one re-invocation per turn, not fifty. Without this a live
 *    feed turns a popular actor into a self-inflicted load problem — the
 *    turns are serialized, so those queue behind each other.
 *  - **Trailing throttle.** A burst of mutations coalesces into one read.
 *    Trailing rather than leading because the last value is the true one;
 *    emitting the first and dropping the rest would leave subscribers on
 *    a stale value until the next unrelated turn.
 *
 * Kept out of `activation.ts` because it is self-contained: it is handed
 * the three things it needs (run the read, observe changes, hold the
 * activation alive) and knows nothing else about an activation.
 */
import type { ActorScheduler } from '../types';

export interface WatchDeps {
    /** Invoke the read as a normal turn. */
    invoke(): Promise<unknown>;
    /** The activation's change feed — one subscription per shared watch. */
    changes(): AsyncIterable<unknown>;
    /** Acquire a keep-alive ref; the returned fn releases it. */
    keepAlive(): () => void;
    scheduler: ActorScheduler;
    throttleMs: number;
}

interface Subscriber {
    queue: unknown[];
    error: unknown;
    failed: boolean;
    wake: (() => void) | null;
    done: boolean;
    /** Detach this subscriber's abort listener; null once detached. */
    off: (() => void) | null;
}

/** Bounded per subscriber: a consumer slower than the actor drops oldest. */
const WATCH_BUFFER = 16;

/**
 * The shared-watch identity, as an INJECTIVE string.
 *
 * This is correctness, not tidiness. Two subscriptions with equal keys share
 * one loop and one re-invocation, so a collision serves one subscriber the
 * result of the OTHER's arguments — a wrong answer, silently.
 *
 * `JSON.stringify` cannot carry that guarantee: it THROWS on a `bigint`,
 * folds `NaN` onto `null` and `-0` onto `0`, drops `undefined` inside
 * objects, and orders keys by insertion so equal args split into two loops.
 * So the args arrive already through the codec (which tags `bigint`, `Date`,
 * `Map` and `undefined` into distinct JSON shapes) and are written here in a
 * grammar where every node carries its own length — nothing can be parsed
 * two ways, and no separator can occur inside a string.
 *
 * `encodedArgs` must be the codec-encoded form (`ActivationHost.encodeArgs`),
 * never the raw values.
 */
export function watchKey(method: string, throttleMs: number, encodedArgs: unknown): string {
    const out: string[] = [];
    writeCanonical(method, out);
    writeCanonical(throttleMs, out);
    writeCanonical(encodedArgs, out);
    return out.join('');
}

function writeCanonical(value: unknown, out: string[]): void {
    if (value === null) {
        out.push('z');
        return;
    }
    switch (typeof value) {
        case 'undefined':
            out.push('u');
            return;
        case 'boolean':
            out.push(value ? 't' : 'f');
            return;
        case 'number': {
            // `String` keeps NaN and Infinity as themselves; `-0` needs
            // saying explicitly, since `String(-0)` is `'0'`.
            const text = Object.is(value, -0) ? '-0' : String(value);
            out.push(`n${text.length}:${text}`);
            return;
        }
        case 'string':
            out.push(`s${value.length}:${value}`);
            return;
        case 'object': {
            if (Array.isArray(value)) {
                out.push(`a${value.length}:`);
                for (const item of value) writeCanonical(item, out);
                return;
            }
            const record = value as Record<string, unknown>;
            const keys = Object.keys(record).sort();
            out.push(`o${keys.length}:`);
            for (const key of keys) {
                out.push(`k${key.length}:${key}`);
                writeCanonical(record[key], out);
            }
            return;
        }
    }
    throw new Error(
        `[sigx actors] a watch argument of type "${typeof value}" survived the codec, so two ` +
            `subscriptions cannot be told apart and would share one loop. Register a type ` +
            `handler for it, or pass a value the wire can carry.`
    );
}

export interface SharedWatch {
    /**
     * `signal` is the SUBSCRIBER's, not the watch's — one aborting drops
     * only that subscriber, and the shared loop survives for the rest.
     *
     * It has to be honoured here rather than left to the consumer calling
     * `return()`, because the consumer is often in no position to. A cross-
     * host watch is served by a generator parked at `await next()`, and an
     * async generator suspended at an `await` cannot observe `return()` —
     * the spec queues it until the generator next yields, which on a quiet
     * actor is never. The abort is then the ONLY signal that reaches the
     * owner, and without it a dropped connection pins the activation on a
     * host that has no idea the subscriber has gone.
     */
    subscribe(signal?: AbortSignal): AsyncIterable<unknown>;
    /** Live subscriber count — the map owner drops the entry at zero. */
    readonly size: number;
}

export function createSharedWatch(deps: WatchDeps, onEmpty: () => void): SharedWatch {
    const subscribers = new Set<Subscriber>();
    let stopped = false;
    let release: (() => void) | null = null;
    /**
     * The most recent result, replayed to whoever subscribes next.
     *
     * Sharing the loop means the initial read happens ONCE, so without this
     * a second subscriber would hang until the actor next mutated — which
     * for a quiet actor is never. The whole point of a live read is that it
     * starts with a value.
     */
    let last: { value: unknown } | null = null;

    const push = (value: unknown): void => {
        last = { value };
        for (const sub of subscribers) {
            sub.queue.push(value);
            if (sub.queue.length > WATCH_BUFFER) sub.queue.shift();
            sub.wake?.();
        }
    };

    const fail = (error: unknown): void => {
        for (const sub of subscribers) {
            sub.error = error;
            sub.failed = true;
            sub.wake?.();
        }
    };

    const finish = (): void => {
        for (const sub of subscribers) {
            sub.done = true;
            sub.wake?.();
        }
    };

    /** Wait out the throttle window. Resolves immediately at 0. */
    const settle = (): Promise<void> =>
        deps.throttleMs <= 0
            ? Promise.resolve()
            : new Promise<void>((resolve) => void deps.scheduler.after(deps.throttleMs, resolve));

    /**
     * Changes are collapsed to a DIRTY FLAG rather than consumed one-to-one.
     *
     * Iterating the feed directly would run one read per change: three
     * mutations in a burst became three settle-then-read cycles, each
     * reporting state the next was about to supersede. The flag makes the
     * burst what it actually is — "something changed, at least once" — so a
     * window absorbs the whole thing and exactly one read follows it.
     */
    let dirty = false;
    let wakeLoop: (() => void) | null = null;

    /**
     * Held so `drop` can close the feed. `for await` would close it too, but
     * only on its way out of the loop — and the pump spends its life parked
     * inside `next()`, which nothing but a mutation resumes.
     */
    let changeIterator: AsyncIterator<unknown> | null = null;

    const pump = async (): Promise<void> => {
        const iterator = deps.changes()[Symbol.asyncIterator]();
        changeIterator = iterator;
        try {
            for (;;) {
                const { done } = await iterator.next();
                if (done || stopped) return;
                dirty = true;
                wakeLoop?.();
            }
        } catch (error) {
            if (!stopped) fail(error);
        } finally {
            changeIterator = null;
            await iterator.return?.(undefined);
        }
    };

    const waitForChange = async (): Promise<void> => {
        if (dirty) return;
        await new Promise<void>((resolve) => {
            wakeLoop = resolve;
        });
        wakeLoop = null;
    };

    const loop = async (): Promise<void> => {
        try {
            // The initial value, so a subscriber never waits for a mutation
            // that may never come.
            push(await deps.invoke());
            while (!stopped) {
                await waitForChange();
                if (stopped) return;
                // Trailing edge: wait the window out, THEN clear the flag, so
                // everything that landed inside it is absorbed by the single
                // read that follows.
                await settle();
                if (stopped) return;
                dirty = false;
                push(await deps.invoke());
            }
        } catch (error) {
            if (!stopped) fail(error);
        } finally {
            finish();
        }
    };

    release = deps.keepAlive();
    void pump();
    void loop();

    const drop = (sub: Subscriber): void => {
        // Wake a parked next() before dropping. An external return() — the
        // `$live` teardown does exactly this — otherwise leaves that
        // awaited promise hanging for the life of the process.
        sub.done = true;
        sub.wake?.();
        sub.off?.();
        sub.off = null;
        // IDEMPOTENT. An external `return()` and the parked `next()` it wakes
        // both land here for the same subscriber, so the cleanup below would
        // run twice. That is not merely redundant: `onEmpty()` removes this
        // watch from the activation's map BY KEY, and a second pass can evict
        // a NEWER shared watch that has since taken the same key — after
        // which every later subscriber silently builds its own loop.
        if (!subscribers.delete(sub)) return;
        if (subscribers.size > 0) return;
        // Last one out stops the loop and releases the activation.
        stopped = true;
        wakeLoop?.();
        // Unsubscribe from the change feed NOW, rather than leaving it to
        // the pump's own unwind: the pump is parked inside the feed's
        // `next()`, and only a mutation resumes it — on a quiet actor,
        // never. The activation would go on queueing snapshots for a watch
        // that has already gone, for as long as it stays activated.
        void changeIterator?.return?.(undefined);
        release?.();
        release = null;
        onEmpty();
    };

    return {
        get size() {
            return subscribers.size;
        },
        subscribe(signal?: AbortSignal): AsyncIterable<unknown> {
            const sub: Subscriber = {
                // Seeded with the latest result if the loop has produced
                // one; empty means the initial read is still in flight and
                // this subscriber will receive it with everyone else.
                queue: last ? [last.value] : [],
                error: null,
                failed: false,
                wake: null,
                done: false,
                off: null
            };
            subscribers.add(sub);
            if (signal) {
                if (signal.aborted) drop(sub);
                else {
                    const onAbort = (): void => drop(sub);
                    signal.addEventListener('abort', onAbort, { once: true });
                    sub.off = () => signal.removeEventListener('abort', onAbort);
                }
            }
            return {
                [Symbol.asyncIterator]: () => ({
                    async next(): Promise<IteratorResult<unknown>> {
                        for (;;) {
                            if (sub.queue.length > 0) {
                                return { value: sub.queue.shift()!, done: false };
                            }
                            if (sub.failed) {
                                sub.failed = false;
                                drop(sub);
                                throw sub.error;
                            }
                            if (sub.done) {
                                drop(sub);
                                return { value: undefined, done: true };
                            }
                            await new Promise<void>((resolve) => {
                                sub.wake = resolve;
                            });
                            sub.wake = null;
                        }
                    },
                    async return(): Promise<IteratorResult<unknown>> {
                        drop(sub);
                        return { value: undefined, done: true };
                    }
                })
            };
        }
    };
}
