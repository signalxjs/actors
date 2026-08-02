/**
 * `$live#subscribe` — many actor subscriptions on ONE held-open response.
 *
 * It is a synthesized serverFn like every other actor method, so it inherits
 * the whole stack for free: origin policy, the content-type gate, body caps,
 * the wire codec, `ServerFnError` masking, `onError`, the request scope —
 * and, decisively, CLUSTER ROUTING, because each subscription dispatches
 * through placement like any other call. A bespoke WebSocket mount would
 * have had to re-earn all of it.
 *
 * The multiplex is why it exists: a page with twelve live components should
 * hold one connection, not twelve. Frames carry the subscription index so
 * the client can fan them back out.
 *
 * Failure is PER SUBSCRIPTION. A guard rejecting one widget must not cost
 * the page its other eleven, so a rejection becomes an `e` frame at that
 * index and the rest keep streaming.
 */
import { ServerFnError, type ServerFnContext } from '@sigx/server';
import { mintCallId } from '../call-id';
import { runGuards } from '../guards';
import { toClientError } from './client-error';
import { LIVE_SYMBOL, type LiveFrame, type LiveSubscription } from '../wire-shared';
import type { AnyActorDefinition, Host } from '../types';

// The wire contract itself (the symbol, the subscription record, the frame
// shapes) lives in `../wire-shared`: the client half builds what this half
// parses, and it must not import a server module to learn the shape.
export { LIVE_SYMBOL, type LiveFrame, type LiveSubscription };

/**
 * How often an otherwise-silent connection emits `{p:1}`.
 *
 * A live page is mostly quiet — that is the normal case, not the edge — and
 * proxies, load balancers and mobile NATs reap an idle response well before
 * the next mutation arrives. Without a ping the connection dies silently and
 * the page stops updating with nothing to show for it. 30 s sits under the
 * common 60 s idle timeouts with room for one lost frame.
 */
export const DEFAULT_LIVE_PING_MS = 30_000;

function badRequest(message: string): never {
    throw new ServerFnError(400, `[sigx actors] $live: ${message}`);
}

function parseSubscriptions(raw: unknown): LiveSubscription[] {
    if (!Array.isArray(raw)) badRequest('expected an array of subscriptions');
    return raw.map((entry, index) => {
        const sub = entry as Partial<LiveSubscription>;
        if (typeof sub?.t !== 'string' || !sub.t) badRequest(`subscription ${index} has no type`);
        if (typeof sub.k !== 'string' || !sub.k) badRequest(`subscription ${index} has no key`);
        if (typeof sub.m !== 'string' || !sub.m) badRequest(`subscription ${index} has no method`);
        if (sub.a !== undefined && !Array.isArray(sub.a)) {
            badRequest(`subscription ${index} has non-array args`);
        }
        return { t: sub.t, k: sub.k, m: sub.m, a: sub.a ?? [] };
    });
}

/**
 * Mask like the unary endpoint does — and, first, CLASSIFY like it does.
 *
 * A branded actor error carries no `status` of its own, so without
 * `toClientError` an unknown method reached the client as a masked 500 here
 * while the very same call over the unary path answered 404. A subscription
 * and a poll of the same read must not disagree about whose fault it is.
 */
function toFrameError(error: unknown): { message: string; status: number } {
    const classified = toClientError(error);
    const wire = classified as { __sigxServerFnError?: boolean; status?: number; message?: string };
    if (wire?.__sigxServerFnError === true && typeof wire.status === 'number') {
        return { message: String(wire.message ?? 'error'), status: wire.status };
    }
    if (__DEV__) {
        return { message: error instanceof Error ? error.message : String(error), status: 500 };
    }
    return { message: 'internal error', status: 500 };
}

/**
 * Drive every subscription concurrently into one frame stream.
 *
 * Deliberately an async generator over a shared queue rather than a merge of
 * iterators: subscriptions produce independently and at their own rate, and
 * a round-robin merge would let one quiet actor stall the others.
 */
export function subscribeAll(
    host: Host,
    rq: ServerFnContext,
    raw: unknown,
    options: { pingMs?: number } = {}
): AsyncGenerator<LiveFrame> {
    const subs = parseSubscriptions(raw);
    if (!host.dispatchWatch) {
        throw new ServerFnError(
            501,
            '[sigx actors] this host cannot watch (no dispatchWatch on its placement).'
        );
    }

    /**
     * Pending frames, BOUNDED by the number of subscriptions.
     *
     * A plain queue grows without limit when the HTTP consumer is slower
     * than the actors are busy — a slow client could walk the server out of
     * memory. Values are therefore coalesced per subscription index: an
     * undelivered value is worthless once a newer one for the same
     * subscription exists, which is exactly the semantics of a live read.
     *
     * Errors are NOT coalesced away — each is terminal for its subscription
     * and there is at most one, so they ride a separate list.
     */
    const latest = new Map<number, LiveFrame>();
    const terminal: LiveFrame[] = [];
    let wake: (() => void) | null = null;
    let live = subs.length;
    let closed = false;
    const stops: Array<() => Promise<void>> = [];

    const pending = (): boolean => latest.size > 0 || terminal.length > 0;

    const take = (): LiveFrame | undefined => {
        if (terminal.length > 0) return terminal.shift();
        const next = latest.entries().next();
        if (next.done) return undefined;
        latest.delete(next.value[0]);
        return next.value[1];
    };

    const emit = (frame: LiveFrame): void => {
        if ('v' in frame) latest.set(frame.i, frame);
        else terminal.push(frame);
        wake?.();
    };

    const start = async (sub: LiveSubscription, index: number): Promise<void> => {
        try {
            const def = await host.definition(sub.t);
            if (!def) throw new ServerFnError(404, `unknown actor type "${sub.t}"`);
            // The SAME guard chain a unary call runs, against this request.
            // A watch therefore exposes nothing a poller could not already
            // read — which is why there is no per-actor opt-in.
            await runGuards(def as AnyActorDefinition, sub.m, rq);

            const iterable = host.dispatchWatch!({ type: sub.t, key: sub.k }, sub.m, sub.a ?? [], {
                callChain: [],
                callId: mintCallId(),
                abortSignal: rq.abortSignal
            });
            const iterator = iterable[Symbol.asyncIterator]();
            stops.push(async () => void (await iterator.return?.(undefined)));

            for (;;) {
                const { value, done } = await iterator.next();
                if (done || closed) break;
                emit({ i: index, v: value });
            }
        } catch (error) {
            if (!closed) emit({ i: index, e: toFrameError(error) });
        } finally {
            live--;
            wake?.();
        }
    };

    for (const [index, sub] of subs.entries()) void start(sub, index);

    async function* frames(): AsyncGenerator<LiveFrame> {
        // `0` disables it explicitly; only an ABSENT option takes the default,
        // so a test can ask for a ping-free stream.
        const ping = options.pingMs ?? DEFAULT_LIVE_PING_MS;
        try {
            for (;;) {
                for (let frame = take(); frame; frame = take()) yield frame;
                // Every subscription has ended (all rejected, or the actors
                // all stopped) — nothing further can arrive.
                if (live === 0 && !pending()) return;
                let timer: ReturnType<typeof setTimeout> | undefined;
                await new Promise<void>((resolve) => {
                    wake = resolve;
                    if (ping > 0) timer = setTimeout(resolve, ping);
                });
                wake = null;
                if (timer) clearTimeout(timer);
                // The consumer disconnected while we were parked. Checked
                // here rather than left to the queued `return()`, which
                // cannot land until this generator reaches a `yield` —
                // see the wrapper below.
                if (closed) return;
                // A ping only when the wait really did time out, so an idle
                // connection stays warm through proxies without adding
                // noise to a busy one.
                if (!pending() && live > 0 && ping > 0) yield { p: 1 };
            }
        } finally {
            closed = true;
            await Promise.allSettled(stops.map((stop) => stop()));
        }
    }

    /**
     * `frames()` behind a `return()` that can actually reach it.
     *
     * A live connection spends nearly all its time parked on the wait above,
     * and an async generator suspended at an `await` — not a `yield` — cannot
     * observe `return()`: the spec queues it until the generator next yields.
     * The endpoint calls `return()` when the client disconnects, so on a
     * QUIET actor, where nothing ever arrives to resume the loop, the
     * `finally` never ran: the watch stayed open and its keep-alive pinned
     * the activation for the life of the process. Tabs close far more often
     * than actors mutate, so that is the common path, not the edge.
     *
     * Setting `closed` and waking the loop first lets it unwind on its own;
     * the queued `return()` then lands normally. (The same shape as #71's
     * `ctx.changes()` deadlock, which is not fixable this way because the
     * parked generator there is the user's.)
     */
    const generator = frames();
    return {
        [Symbol.asyncIterator]() {
            return this;
        },
        next: (...args) => generator.next(...args),
        throw: (error: unknown) => generator.throw(error),
        return: (value) => {
            closed = true;
            wake?.();
            return generator.return(value as undefined);
        }
    } as AsyncGenerator<LiveFrame>;
}
