/**
 * The subscriber fan-out and the canonical key grammar — shared by every
 * layer that multiplexes ONE value stream to many consumers: the
 * activation's shared watch loop (`host/watch.ts`) and the cluster's
 * coalesced cross-host watch (`cluster/placement.ts`, #111).
 *
 * Top-level rather than under `host/` so the cluster bundle can import it
 * without dragging the activation loop, pump and keep-alive machinery in —
 * the `stream-relay.ts` precedent for host/cluster-shared code.
 */

interface Subscriber {
    queue: unknown[];
    error: unknown;
    failed: boolean;
    wake: (() => void) | null;
    done: boolean;
    /** Detach this subscriber's abort listener; null once detached. */
    off: (() => void) | null;
}

/** Bounded per subscriber: a consumer slower than the feed drops oldest. */
export const WATCH_BUFFER = 16;

/** Trailing-throttle window for a change-driven read (`openWatch`). */
export const DEFAULT_WATCH_THROTTLE_MS = 50;

export interface FanOut {
    /** Deliver to every subscriber; recorded as `last` for replay. */
    push(value: unknown): void;
    /** Mark every subscriber failed; each throws from its next `next()`. */
    fail(error: unknown): void;
    /** Mark every subscriber done; each completes on its next `next()`. */
    finish(): void;
    /**
     * `signal` is the SUBSCRIBER's, not the fan-out's — one aborting drops
     * only that subscriber, and the shared source survives for the rest.
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

/**
 * A bounded multicast over one value stream. `onEmpty` fires exactly ONCE
 * per empty transition — the idempotent `drop` below is what guarantees it,
 * and both layers lean on that guarantee to delete their map entry by key
 * without ever evicting a newer entry that has since taken the same key.
 */
export function createFanOut(onEmpty: () => void): FanOut {
    const subscribers = new Set<Subscriber>();
    /**
     * The most recent value, replayed to whoever subscribes next.
     *
     * Sharing the source means the initial read happens ONCE, so without
     * this a second subscriber would hang until the source next emitted —
     * which for a quiet actor is never. The whole point of a live read is
     * that it starts with a value.
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

    const drop = (sub: Subscriber): void => {
        // Wake a parked next() before dropping. An external return() — the
        // `$live` teardown does exactly this — otherwise leaves that
        // awaited promise hanging for the life of the process.
        sub.done = true;
        sub.wake?.();
        sub.off?.();
        sub.off = null;
        // IDEMPOTENT. An external `return()` and the parked `next()` it
        // wakes both land here for the same subscriber, so the cleanup
        // below would run twice. That is not merely redundant: the owner
        // removes this fan-out from its map BY KEY, and a second pass can
        // evict a NEWER entry that has since taken the same key — after
        // which every later subscriber silently builds its own source.
        if (!subscribers.delete(sub)) return;
        if (subscribers.size > 0) return;
        onEmpty();
    };

    return {
        push,
        fail,
        finish,
        get size() {
            return subscribers.size;
        },
        subscribe(signal?: AbortSignal): AsyncIterable<unknown> {
            const sub: Subscriber = {
                // Seeded with the latest value if the source has produced
                // one; empty means the initial emission is still in flight
                // and this subscriber will receive it with everyone else.
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

/**
 * An INJECTIVE string over a sequence of codec-encoded values.
 *
 * This is correctness, not tidiness. Two subscriptions with equal keys
 * share one source, so a collision serves one subscriber the result of the
 * OTHER's arguments — a wrong answer, silently.
 *
 * `JSON.stringify` cannot carry that guarantee: it THROWS on a `bigint`,
 * folds `NaN` onto `null` and `-0` onto `0`, drops `undefined` inside
 * objects, and orders keys by insertion so equal args split into two
 * sources. So the values arrive already through a codec (which tags
 * `bigint`, `Date`, `Map` and `undefined` into distinct JSON shapes) and
 * are written here in a grammar where every node carries its own length —
 * nothing can be parsed two ways, and no separator can occur inside a
 * string. Each part's encoding is self-delimiting, so a sequence of parts
 * is as injective as one.
 */
export function canonicalKey(parts: readonly unknown[]): string {
    const out: string[] = [];
    for (const part of parts) writeCanonical(part, out);
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
            // PLAIN records only. A class instance, `Date` or `Map` that
            // reached this point escaped the codec (a missing type
            // handler), and writing its enumerable keys would collapse it —
            // a `Date` has none and becomes `o0:` — silently colliding
            // with a distinct subscription. That is the exact failure this
            // grammar exists to prevent, so it throws instead.
            const proto: unknown = Object.getPrototypeOf(value);
            if (proto !== Object.prototype && proto !== null) {
                throw new Error(
                    `[sigx actors] a watch argument of type ` +
                        `"${(value as object).constructor?.name ?? 'object'}" survived the ` +
                        `codec, so two subscriptions cannot be told apart and would share ` +
                        `one loop. Register a type handler for it, or pass a value the ` +
                        `wire can carry.`
                );
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

/**
 * The structural subset of `ActorOptions` the watch-sharing declaration
 * needs. Structural rather than an import of the definition type, following
 * `ReentrancyOptions`, so neither the cluster bundle nor this module has to
 * depend on the full `ActorOptions` generic to read one flag.
 */
export interface WatchDeclarationOptions {
    readonly watches?: Readonly<Record<string, { principalIndependent?: true } | undefined>>;
}

/**
 * Did this method declare its watched read principal-independent (#138)?
 *
 * Read INDEPENDENTLY by both sides — the relay, to drop the principal from
 * its coalescing key, and the owner, to police the promise. That is what
 * makes enforcement relay-independent: the owner fails a lying read whether
 * or not anything coalesced.
 *
 * Own keys only, the `methodAuthorize` / `methodReentrancy` rule — an actor
 * must not inherit a sharing promise from `Object.prototype`.
 *
 * Shape-guarded rather than trusting the type, because the RELAY reads this
 * before anything validates the definition — it may never activate the actor
 * at all, and `validateWatchDeclarations` runs at the owner's first
 * activation. A malformed `watches` (`null`, an array, a string) must read as
 * "not declared" and leave the owner to fail loudly, not crash coalescing on
 * a host that is only passing the subscription through.
 */
export function declaresPrincipalIndependent(
    opts: WatchDeclarationOptions,
    method: string
): boolean {
    const map = opts.watches as unknown;
    return (
        typeof map === 'object' &&
        map !== null &&
        !Array.isArray(map) &&
        Object.hasOwn(map, method) &&
        (map as Record<string, { principalIndependent?: true } | undefined>)[method]
            ?.principalIndependent === true
    );
}
