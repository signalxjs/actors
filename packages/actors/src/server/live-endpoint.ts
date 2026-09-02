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
import { takeCallBag } from '../call-context-bag';
import { actorPosture, authorizeActorCall, encodePrincipal } from '../guards';
import { isTraceparent } from '../traceparent';
import { toClientError } from './client-error';
import { LIVE_SYMBOL, type LiveFrame, type LiveSubscription } from '../wire-shared';
import { DEFAULT_WATCH_THROTTLE_MS } from '../watch-core';
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

/**
 * Most subscriptions one `$live` connection may open.
 *
 * The fan-out below is one `dispatchWatch` per entry, started at once, and
 * each one can force a distinct activation that then sits pinned for
 * `idleAfterMs`. A minimal entry is ~25 bytes on the wire, so the 1 MiB
 * default body cap would otherwise buy tens of thousands of activations from
 * a single unauthenticated request — the largest amplification this mount
 * has. The per-subscription guards do not bound it: they run inside the
 * fan-out, after it has already started.
 *
 * 256 is deliberately generous rather than tight. The multiplex exists so a
 * page with twelve live components holds one connection; a dense dashboard
 * might reach fifty. The cap's job is to stop forty thousand, not three
 * hundred, and a default that breaks a real page is a default that gets
 * raised to Infinity by the first person it bites.
 */
export const DEFAULT_MAX_LIVE_SUBSCRIPTIONS = 256;

function badRequest(message: string): never {
    throw new ServerFnError(400, `[sigx actors] $live: ${message}`);
}

/**
 * A security bound must not be disabled by a typo. `0` is the documented
 * escape hatch; `-1`, `1.5` and `NaN` are misconfiguration, and under a plain
 * `max > 0` test every one of them silently turned the cap OFF — the failure
 * mode this option exists to prevent, reached by getting the option wrong.
 *
 * Exported (module-level, not on the `./server` barrel) for the socket
 * session (#99), whose per-connection subscription cap is the same bound.
 */
export function resolveMaxSubscriptions(max: number): number {
    if (!Number.isInteger(max) || max < 0) {
        throw new Error(
            `[sigx actors] maxLiveSubscriptions must be a non-negative integer — got ` +
                `${String(max)}. Use 0 to disable the cap deliberately.`
        );
    }
    return max;
}

/**
 * How much say a client gets over its own delivery rate (#247).
 *
 * `min` is the fastest window any client may ask for; `buckets` is the ladder
 * a request is rounded UP to. The ladder exists because `throttleMs` is part
 * of the watch identity (`Activation.openWatch`) AND of the cross-host
 * coalescing key (`ClusterPlacement.#coalescedWatch`): honouring an arbitrary
 * number would give every distinct value its own watch loop and its own
 * remote stream, which is precisely what #121/#138/#139 exist to collapse. A
 * socket may hold 256 subscriptions, so "one loop per requested value" is a
 * cheap way for one client to make an actor do 256x the work.
 */
export interface LiveThrottlePolicy {
    /** Fastest window a client may request, in ms. */
    min: number;
    /** Ascending ladder of windows a request is rounded up to, in ms. */
    buckets: readonly number[];
}

/**
 * The default ladder. `min` is the runtime's own watch throttle, so **the
 * default policy can only ever make a subscription SLOWER than it is today** —
 * going faster is an explicit operator decision, the same posture as
 * `maxSubscriptions`.
 */
export const DEFAULT_THROTTLE_POLICY: LiveThrottlePolicy = {
    min: DEFAULT_WATCH_THROTTLE_MS,
    buckets: [DEFAULT_WATCH_THROTTLE_MS, 250, 1000, 5000]
};

/**
 * Validated at CONSTRUCTION, like `resolveMaxSubscriptions` — a policy whose
 * ladder is empty, unsorted, or entirely below its own floor is a
 * misconfiguration that would otherwise surface as a subscription rate nobody
 * asked for, per subscription, at runtime.
 */
export function resolveThrottlePolicy(policy: LiveThrottlePolicy): LiveThrottlePolicy {
    const { min, buckets } = policy;
    if (!Number.isInteger(min) || min < 0) {
        throw new Error(
            `[sigx actors] throttlePolicy.min must be a non-negative integer of ` +
                `milliseconds — got ${String(min)}.`
        );
    }
    if (!Array.isArray(buckets) || buckets.length === 0) {
        throw new Error('[sigx actors] throttlePolicy.buckets must be a non-empty array.');
    }
    let previous = -1;
    for (const bucket of buckets) {
        if (!Number.isInteger(bucket) || bucket < 0) {
            throw new Error(
                `[sigx actors] throttlePolicy.buckets must be non-negative integers of ` +
                    `milliseconds — got ${String(bucket)}.`
            );
        }
        if (bucket <= previous) {
            throw new Error(
                `[sigx actors] throttlePolicy.buckets must be strictly ascending — got ` +
                    `${buckets.join(', ')}.`
            );
        }
        previous = bucket;
    }
    if (buckets[buckets.length - 1]! < min) {
        throw new Error(
            `[sigx actors] throttlePolicy has no bucket at or above its own min (${min}ms) — ` +
                `every request would be served faster than it asked for.`
        );
    }
    return policy;
}

/** A `w` a client may not have. Thrown/refused, never defaulted. */
export class BadThrottleRequest extends Error {}

/**
 * Resolve a client's requested window against the policy.
 *
 * - absent → `undefined`, and the caller passes NO `throttleMs`, so the watch
 *   key is byte-for-byte the one a client that never heard of `w` produces.
 * - a finite, non-negative number → floored at `min`, then rounded **up** to
 *   the nearest bucket. Up, never down: a client must never be served faster
 *   than it asked for.
 * - above the top bucket → the top bucket. A bounded overshoot the operator
 *   chose by picking the ladder, and it errs toward more data, never less.
 * - anything else → `BadThrottleRequest`. Reading `w: '1000'` as "the default"
 *   would silently bill the client 20x what it asked for, which is the whole
 *   cost this option exists to let it avoid.
 */
export function resolveClientThrottle(
    requested: unknown,
    policy: LiveThrottlePolicy
): number | undefined {
    if (requested === undefined) return undefined;
    if (typeof requested !== 'number' || !Number.isFinite(requested) || requested < 0) {
        throw new BadThrottleRequest(
            `throttle window must be a non-negative finite number of milliseconds — got ` +
                `${String(requested)}`
        );
    }
    const wanted = Math.max(requested, policy.min);
    for (const bucket of policy.buckets) {
        if (bucket >= wanted) return bucket;
    }
    return policy.buckets[policy.buckets.length - 1];
}

function parseSubscriptions(
    raw: unknown,
    max: number,
    policy: LiveThrottlePolicy
): LiveSubscription[] {
    if (!Array.isArray(raw)) badRequest('expected an array of subscriptions');
    // Length first, before the per-entry walk: refusing after validating
    // 40 000 entries still pays for validating 40 000 entries.
    if (max > 0 && raw.length > max) {
        badRequest(`too many subscriptions — ${raw.length} requested, limit is ${max}`);
    }
    return raw.map((entry, index) => {
        const sub = entry as Partial<LiveSubscription>;
        if (typeof sub?.t !== 'string' || !sub.t) badRequest(`subscription ${index} has no type`);
        if (typeof sub.k !== 'string' || !sub.k) badRequest(`subscription ${index} has no key`);
        if (typeof sub.m !== 'string' || !sub.m) badRequest(`subscription ${index} has no method`);
        if (sub.a !== undefined && !Array.isArray(sub.a)) {
            badRequest(`subscription ${index} has non-array args`);
        }
        let w: number | undefined;
        try {
            w = resolveClientThrottle(sub.w, policy);
        } catch (error) {
            badRequest(
                `subscription ${index}: ${error instanceof Error ? error.message : String(error)}`
            );
        }
        // Absent stays ABSENT rather than becoming the resolved default: the
        // watch key is built from what is passed, so writing a value here
        // would rebase every existing subscription's identity.
        return { t: sub.t, k: sub.k, m: sub.m, a: sub.a ?? [], ...(w !== undefined ? { w } : {}) };
    });
}

/**
 * Mask like the unary endpoint does — and, first, CLASSIFY like it does.
 *
 * A branded actor error carries no `status` of its own, so without
 * `toClientError` an unknown method reached the client as a masked 500 here
 * while the very same call over the unary path answered 404. A subscription
 * and a poll of the same read must not disagree about whose fault it is.
 *
 * Exported (module-level, not on the `./server` barrel) for the socket
 * session (#99) — the third caller of "classify, then mask", and the reason
 * it must be one function rather than three copies.
 */
export function toFrameError(error: unknown): { message: string; status: number } {
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
    options: {
        pingMs?: number;
        maxSubscriptions?: number;
        throttlePolicy?: LiveThrottlePolicy;
    } = {}
): AsyncGenerator<LiveFrame> {
    const subs = parseSubscriptions(
        raw,
        resolveMaxSubscriptions(options.maxSubscriptions ?? DEFAULT_MAX_LIVE_SUBSCRIPTIONS),
        resolveThrottlePolicy(options.throttlePolicy ?? DEFAULT_THROTTLE_POLICY)
    );
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

    // The app's posture, read once per connection — the same bound the
    // socket session arms (#180), repaid here per subscription below.
    const timeoutMs = actorPosture().timeoutMs;

    const start = async (sub: LiveSubscription, index: number): Promise<void> => {
        // The deadline covers ESTABLISHMENT only: pipeline + authorization +
        // dispatch + the FIRST value. A watch read is a normal turn on a
        // single-threaded actor, so on a busy one the seed can queue
        // arbitrarily long — and the watch path deliberately has no runtime
        // deadline (the loop lives forever), so before this timer a starved
        // seed held its subscription, watch loop and keep-alive open
        // SILENTLY, forever (#192; the socket half was #180). A seeded
        // subscription is never timed out — pushes are the loop's cadence.
        //
        // Coverage nuance: core's endpoint already arms this same
        // `timeoutMs` over the request up to the stream's FIRST chunk, so on
        // a connection where NO subscription seeds in time the request-level
        // 504 answers first. The per-subscription frame is what bites the
        // multiplex case, where a sibling seeded and kept the response alive.
        //
        // The timer aborts a PER-SUBSCRIPTION controller, never the shared
        // request signal: the starved seed is this subscription's failure,
        // and its siblings on the same connection keep streaming. The signal
        // detaches THIS subscriber from the fan-out; the shared watch loop
        // itself runs under its own signal (`#createWatchEntry` in
        // `host/activation.ts`, #137), so a creator timing out here never
        // fails a re-read another connection is still waiting on.
        let deadline: ReturnType<typeof setTimeout> | undefined;
        let timedOut: ServerFnError | null = null;
        const disarmDeadline = (): void => {
            if (deadline !== undefined) clearTimeout(deadline);
            deadline = undefined;
            // A fired deadline is STALE once a value lands. The abort can
            // race the seed — the fan-out replays its cached `last` to a
            // subscriber whose signal is already aborted — and re-raising
            // `timedOut` after that value would 504 a subscription that DID
            // seed, killing a healthy widget.
            timedOut = null;
        };
        // The per-subscription context view (the socket session's
        // `callContext` shape): the prelude and authorization run under the
        // COMPOSED signal, so app middleware that honors `rq.abortSignal` —
        // a rate limiter against an overloaded store — observes the deadline
        // instead of holding the establishment open past it.
        let rqSub = rq;
        try {
            if (timeoutMs !== undefined && timeoutMs > 0) {
                const ctrl = new AbortController();
                rqSub = { ...rq, abortSignal: AbortSignal.any([rq.abortSignal, ctrl.signal]) };
                deadline = setTimeout(() => {
                    timedOut = new ServerFnError(
                        504,
                        `[sigx actors] subscription "${sub.t}#${sub.m}" timed out before its first value`
                    );
                    // The abort releases everything the starved seed held —
                    // fan-out subscriber, shared loop when last-out, keep-alive.
                    ctrl.abort(timedOut);
                }, timeoutMs);
            }
            const def = await host.definition(sub.t);
            if (!def) throw new ServerFnError(404, `unknown actor type "${sub.t}"`);
            // The SAME pipeline a unary call runs, against this request and
            // this instance. A watch therefore exposes nothing a poller
            // could not already read — which is why there is no per-actor
            // opt-in. This IS an entry point (one `$live` connection carries
            // many subscriptions, each authorized on its own), so unlike the
            // unary wire path it runs the prelude itself: core's endpoint
            // ran it once for `$live#subscribe`, not for each subscription.
            await authorizeActorCall(def as AnyActorDefinition, sub.m, sub.k, rqSub, 'wire');

            const traceparent = rq.request.headers.get('traceparent');
            // Same edge rule as the unary endpoint: the bag comes only from
            // what this request stamped, never from a header — and identity
            // rides its own slot. `rqSub` shares the connection's `locals`,
            // so the take also clears a policy's stamp before a sibling
            // subscription runs its own guards. The watched METHOD never
            // sees it, though: a watch turn is shared, and `ctx.bag` inside
            // one is the empty bag whatever the edge stamped (#137) — the
            // stamp is a unary-call channel.
            const bag = takeCallBag(rqSub.locals);
            const principal = await encodePrincipal(rqSub);
            const iterable = host.dispatchWatch!(
                { type: sub.t, key: sub.k },
                sub.m,
                sub.a ?? [],
                {
                    callChain: [],
                    callId: mintCallId(),
                    ...(isTraceparent(traceparent) ? { traceparent } : {}),
                    ...(bag !== undefined ? { bag } : {}),
                    ...(principal !== undefined ? { principal } : {}),
                    abortSignal: rqSub.abortSignal
                },
                // Omitted when the client asked for nothing, so the watch key
                // is the one it has always been (#247).
                sub.w !== undefined ? { throttleMs: sub.w } : undefined
            );
            const iterator = iterable[Symbol.asyncIterator]();
            stops.push(async () => void (await iterator.return?.(undefined)));

            for (;;) {
                const { value, done } = await iterator.next();
                if (done || closed) break;
                // Established: the deadline covered exactly the first value.
                disarmDeadline();
                emit({ i: index, v: value });
            }
            // A fired deadline ends the aborted subscriber CLEANLY (`done`),
            // so the starvation must be re-raised to become the frame the
            // client is owed — silence is the failure mode this fixes.
            if (timedOut !== null) throw timedOut;
        } catch (error) {
            if (!closed) emit({ i: index, e: toFrameError(error) });
        } finally {
            disarmDeadline();
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
