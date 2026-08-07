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
 *    turns are serialized, so those queue behind each other. The one
 *    refinement: a read OBSERVED consulting `ctx.principal` gets one loop
 *    per encoded principal instead — identity is then an input to the read,
 *    and sharing across it serves one subscriber another's view (#121). See
 *    `qualifyWatchKey` and `Activation.openWatch`.
 *  - **Trailing throttle.** A burst of mutations coalesces into one read.
 *    Trailing rather than leading because the last value is the true one;
 *    emitting the first and dropping the rest would leave subscribers on
 *    a stale value until the next unrelated turn.
 *
 * Kept out of `activation.ts` because it is self-contained: it is handed
 * the three things it needs (run the read, observe changes, hold the
 * activation alive) and knows nothing else about an activation. The
 * subscriber fan-out itself lives in `../watch-core`, shared with the
 * cluster's cross-host watch coalescing (#111).
 */
import type { ActorScheduler } from '../types';
import { canonicalKey, createFanOut } from '../watch-core';

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

/**
 * The shared-watch identity, as an INJECTIVE string — see `canonicalKey`
 * for why injectivity is correctness, not tidiness.
 *
 * `encodedArgs` must be the codec-encoded form (`ActivationHost.encodeArgs`),
 * never the raw values: the codec is what tags `bigint`, `Date`, `Map` and
 * `undefined` into shapes the grammar can carry.
 */
export function watchKey(method: string, throttleMs: number, encodedArgs: unknown): string {
    return canonicalKey([method, throttleMs, encodedArgs]);
}

/**
 * Qualify a base watch key by an encoded principal.
 *
 * Used once a read is observed consulting `ctx.principal` (#121): identity
 * is then an input to the read, so subscribers may only share a loop when
 * they share it. Injective against plain `watchKey` output — a base key
 * parses as exactly three self-delimiting canonical values with nothing
 * left over, and `'P'` is not the lead byte of any canonical value — and
 * two qualified keys are equal iff base AND principal are. The ENCODED
 * principal, never the decoded one: equal encodings decode identically,
 * which is the only equality the wire can guarantee.
 */
export function qualifyWatchKey(base: string, encodedPrincipal: string | undefined): string {
    return `${base}P${canonicalKey([encodedPrincipal])}`;
}

export interface SharedWatch {
    /**
     * `signal` is the SUBSCRIBER's, not the watch's — one aborting drops
     * only that subscriber, and the shared loop survives for the rest.
     * See `FanOut.subscribe` in `../watch-core` for why the signal must be
     * honoured here rather than left to the consumer calling `return()`.
     */
    subscribe(signal?: AbortSignal): AsyncIterable<unknown>;
    /** Live subscriber count — the map owner drops the entry at zero. */
    readonly size: number;
}

export function createSharedWatch(deps: WatchDeps, onEmpty: () => void): SharedWatch {
    let stopped = false;
    let release: (() => void) | null = null;

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
     * Held so the teardown can close the feed. `for await` would close it
     * too, but only on its way out of the loop — and the pump spends its
     * life parked inside `next()`, which nothing but a mutation resumes.
     */
    let changeIterator: AsyncIterator<unknown> | null = null;

    const fanOut = createFanOut(() => {
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
    });

    /** Wait out the throttle window. Resolves immediately at 0. */
    const settle = (): Promise<void> =>
        deps.throttleMs <= 0
            ? Promise.resolve()
            : new Promise<void>((resolve) => void deps.scheduler.after(deps.throttleMs, resolve));

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
            if (!stopped) fanOut.fail(error);
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
            fanOut.push(await deps.invoke());
            while (!stopped) {
                await waitForChange();
                if (stopped) return;
                // Trailing edge: wait the window out, THEN clear the flag, so
                // everything that landed inside it is absorbed by the single
                // read that follows.
                await settle();
                if (stopped) return;
                dirty = false;
                fanOut.push(await deps.invoke());
            }
        } catch (error) {
            if (!stopped) fanOut.fail(error);
        } finally {
            fanOut.finish();
        }
    };

    release = deps.keepAlive();
    void pump();
    void loop();

    return {
        get size() {
            return fanOut.size;
        },
        subscribe: (signal?: AbortSignal) => fanOut.subscribe(signal)
    };
}
