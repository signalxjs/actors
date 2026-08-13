/**
 * Change-feed subscriber machinery, shared by every scenario that measures
 * with a live `ctx.changes()` consumer attached (`state/*`, `jobs/*`).
 *
 * Unwinding the consumers is the fiddly half, which is why this is shared
 * rather than copied per scenario file — the discipline below was worked
 * out once, for `state/dirty-size`, and a second hand-rolled copy would
 * drift on exactly the parts that matter.
 */
import { benchCall, requireStreamDispatch } from './host-fixture.ts';
import type { ActorRef, Host } from '@sigx/actors/host';

/** How long teardown may take before we call the stream path broken. */
export const TEARDOWN_TIMEOUT_MS = 2000;

/**
 * `true` if every promise settled in time, `false` on timeout. The timer is
 * always cleared — an un-cleared `setTimeout` would hold the event loop open
 * past the end of the run.
 */
export async function settledWithin(
    promises: readonly Promise<unknown>[],
    ms: number
): Promise<boolean> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<false>((resolve) => {
        timer = setTimeout(() => resolve(false), ms);
    });
    try {
        return await Promise.race([Promise.allSettled(promises).then(() => true), timeout]);
    } finally {
        clearTimeout(timer);
    }
}

export interface Subscribers {
    /** Teardown-safe abort for a `finally`; never throws, never waits. */
    abort(): void;
    /** Abort, wake the parked consumers, and prove they unwound. */
    unwind(): Promise<void>;
}

/**
 * `unwind()` belongs on the SUCCESS path only, and a scenario's `finally`
 * deliberately does not call it.
 *
 * It is an assertion, not cleanup: releasing the consumers is
 * `fixture.stop()`'s job either way — `host.stop()` deactivates, and both
 * `deactivate()` and `forceStop()` call `#closeSubs()`, which marks every sub
 * done and wakes a parked `next()` (`activation.ts:773`, `:786`, `:1850`).
 * What `unwind()` adds is the proof that the runtime releases them from the
 * CONSUMER side, on abort plus a wake-up mutation, without needing the host
 * torn down underneath it.
 *
 * Putting it in `finally` would run that assertion against a host whose
 * measurement has just thrown — and its failure, or a dispatch failure from
 * its own wake-up call, would replace the exception that actually broke the
 * round. Losing the real error to a teardown check is a bad trade.
 */

/**
 * Open `count` change feeds on `ref` and drain each in the background: an
 * unconsumed feed fills its 16-slot buffer and starts dropping, which would
 * silently stop measuring the fan-out.
 *
 * A consumer parked in `ctx.changes()` is waiting for the NEXT change, and
 * abort alone does not wake it — so `unwind()` aborts, then calls `wake` to
 * push one more mutation, which lets each consumer see the aborted signal,
 * break, and have `for await` call `return()` on the generator.
 *
 * If that does not happen we FAIL rather than carry on: the scenario runs
 * again every round, and iterators left parked on a host we are about to
 * drop would contaminate every later measurement with work nobody is
 * accounting for.
 */
export function openSubscribers(
    host: Host,
    ref: ActorRef,
    count: number,
    wake: () => Promise<unknown>,
    /** Arguments for the `watch` stream itself (e.g. a throttle option). */
    args: readonly unknown[] = []
): Subscribers {
    const controller = new AbortController();
    const drains: Promise<void>[] = [];
    if (count > 0) {
        const openStream = requireStreamDispatch(host);
        for (let i = 0; i < count; i++) {
            const stream = openStream(ref, 'watch', [...args], benchCall({ abortSignal: controller.signal }));
            drains.push(
                (async () => {
                    try {
                        for await (const _ of stream) {
                            if (controller.signal.aborted) break;
                        }
                    } catch {
                        // Aborted at teardown — expected.
                    }
                })()
            );
        }
    }
    return {
        abort: () => controller.abort(),
        async unwind(): Promise<void> {
            // Nothing to prove with no consumers — and the wake-up mutation
            // would be an extra (possibly durable) turn outside the timed
            // window in every 0-subscriber arm.
            if (drains.length === 0) return;
            controller.abort();
            await wake();
            if (!(await settledWithin(drains, TEARDOWN_TIMEOUT_MS))) {
                throw new Error(
                    `${count} change-feed consumer(s) did not unwind within ` +
                        `${TEARDOWN_TIMEOUT_MS}ms after abort + a wake-up mutation — ` +
                        `ctx.changes() is not releasing parked consumers.`
                );
            }
        }
    };
}
