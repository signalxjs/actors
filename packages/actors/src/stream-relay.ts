/**
 * Relay an actor stream to a wire endpoint WITHOUT an async generator in the
 * middle.
 *
 * The obvious wrapper is a generator (`async function* pump() { yield*
 * iterable }`), and it silently breaks disconnect teardown. A generator parked
 * at `yield*` is suspended at an INTERNAL await, and the spec QUEUES
 * `return()` there rather than running it: the generator is never resumed, so
 * it never forwards `return()` inward, and the activation's own teardown — the
 * thing that releases its keep-alive ref — is never reached.
 *
 * That is the same trap #120 fixed inside the activation, reappearing one
 * level up. The cost is identical and just as quiet: a departed consumer
 * leaves `keptAlive` set, which does not merely delay idle collection, it
 * EXEMPTS the activation from it (`local-host.ts`: `if (a.idle &&
 * !a.keptAlive && …)`). The actor never becomes eligible — and on Workers,
 * where the platform's eviction is the only collector, the object stays
 * resident and billable for as long as the isolate lives.
 *
 * So this is a hand-written iterator whose `return()` forwards INWARD FIRST,
 * before awaiting anything that could park it.
 */

/** Map a relayed failure to the shape this endpoint puts on the wire. */
export type RelayErrorMap = (error: unknown) => unknown;

export interface RelayOptions {
    /** Failure → the shape this endpoint puts on the wire. */
    mapError: RelayErrorMap;
    /**
     * The request's abort signal, when the endpoint has one.
     *
     * Belt and braces, and not redundant: a consumer going away reaches us as
     * a body CANCEL on some hosts and as a request ABORT on others, and only
     * the first drives `return()` by itself. Wiring both makes the teardown
     * guarantee independent of which one the host happens to raise.
     */
    signal?: AbortSignal;
    /**
     * Run SYNCHRONOUSLY the moment the relay closes, before anything is
     * awaited — the hook for whatever wakes a parked inner iterator.
     *
     * A sending transport aborts its fetch here: `readNdjson` is suspended on
     * `reader.read()`, and cancelling the response is the only thing that
     * makes it return. Doing it in a generator's `finally` does not work,
     * because that `finally` is exactly what never runs.
     */
    onClose?: () => void;
}

/**
 * Wrap `iterable` for a serverFn stream reply.
 *
 * Returned as a plain async iterator rather than a generator, because
 * `@sigx/server` drives `next()`/`return()` on it directly and a generator
 * would re-introduce the queued-return trap above.
 */
export function relayStream(
    iterable: AsyncIterable<unknown>,
    options: RelayOptions
): AsyncIterableIterator<unknown> {
    const { mapError, signal, onClose } = options;
    let inner: AsyncIterator<unknown> | null = null;
    /** Lazy, so nothing is pulled from the actor until the first `next()`. */
    const iterator = (): AsyncIterator<unknown> =>
        (inner ??= iterable[Symbol.asyncIterator]());
    let finished = false;
    let detach: (() => void) | null = null;

    const settle = (): void => {
        finished = true;
        detach?.();
        detach = null;
        // Before any await, so a parked inner iterator is woken rather than
        // waited on.
        onClose?.();
    };

    /** Tear the inner iterator down, swallowing a throwing cleanup. */
    const closeInner = async (value?: unknown): Promise<void> => {
        const started = inner;
        if (!started) return;
        try {
            await started.return?.(value);
        } catch {
            // A throwing cleanup must not mask the disconnect that caused it
            // — the consumer has already gone.
        }
    };

    const relay: AsyncIterableIterator<unknown> = {
        [Symbol.asyncIterator](): AsyncIterableIterator<unknown> {
            return this;
        },

        async next(): Promise<IteratorResult<unknown>> {
            if (finished) return { done: true, value: undefined };
            try {
                const result = await iterator().next();
                // Closed underneath us while this was parked: the value is
                // moot and the consumer is gone.
                if (finished) return { done: true, value: undefined };
                if (result.done) settle();
                return result;
            } catch (error) {
                // The COMMON disconnect shape, not an exception: a `next()`
                // parked on `inner.next()` is woken by our own `onClose`
                // aborting the reader, so the rejection is the teardown
                // working. Surfacing it would turn a deliberate close into an
                // unhandled rejection.
                if (finished) return { done: true, value: undefined };
                settle();
                // A spontaneous failure — tear the inner iterator down too,
                // rather than leaving it to be collected.
                await closeInner(undefined);
                throw mapError(error);
            }
        },

        async return(value?: unknown): Promise<IteratorResult<unknown>> {
            if (finished) return { done: true, value };
            // Mark finished and detach BEFORE awaiting: the activation's
            // `return()` closes its change feed synchronously to wake a body
            // parked inside `ctx.changes()`, and suspending here first would
            // strand that wake-up.
            settle();
            await closeInner(value);
            return { done: true, value };
        },

        async throw(error?: unknown): Promise<IteratorResult<unknown>> {
            if (finished) throw mapError(error);
            settle();
            await closeInner(undefined);
            throw mapError(error);
        }
    };

    if (signal) {
        if (signal.aborted) {
            void relay.return?.(undefined);
        } else {
            const onAbort = (): void => void relay.return?.(undefined);
            signal.addEventListener('abort', onAbort, { once: true });
            detach = (): void => signal.removeEventListener('abort', onAbort);
        }
    }

    return relay;
}
