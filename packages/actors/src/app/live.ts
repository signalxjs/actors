/**
 * The client half of the live layer — one connection for a page's whole
 * subscription set.
 *
 * `@sigx/actors/app`, not `./client`, and that placement is load-bearing:
 * `./client`'s bytes ride every bundle that touches an actor, and its
 * tree-shaking guard exists to keep policy out of it. A live page has already
 * paid for the reactive framework, so this belongs next to `useActorState`,
 * where it is reachable only by importing `./app`.
 *
 * It implements the `ActorLiveChannel` interface declared in `./client`, and a
 * transport that brings its OWN `live()` (a WebSocket transport, say) wins
 * over it — same seam, one resolution rule.
 *
 * ## The two decisions worth knowing
 *
 * **One connection carries the whole set, and a set change reopens it.** A
 * `fetch` POST body is not duplex outside Chromium, so a late subscription
 * cannot be pushed onto an open stream. The obvious alternative — a control
 * POST against a session — is rejected on purpose: on Workers, at an edge, or
 * in a multi-host cluster that POST cannot be routed to the instance holding
 * the open stream, and any session-affinity scheme becomes a new sticky-
 * routing requirement on every deployment target. So a set change debounces,
 * aborts, and reopens with the new set in the body.
 *
 * **Correctness therefore needs no resume token and no server-side
 * cross-request state**, because every subscription re-seeds on open. A newly
 * mounted component needed an initial value anyway; the existing subscribers
 * get one redundant identical frame, which structural equality suppresses.
 */
import {
    LIVE_SYMBOL,
    wireFail,
    type LiveFrame,
    type LiveSubscription
} from '../wire-shared';
import type { ActorLiveChannel, ActorSubscription, ActorTransport } from '../client';

export interface LiveChannelOptions {
    /**
     * How long a subscription-set change waits for more changes before the
     * connection reopens. Default 20 ms.
     *
     * A page mounts its components in one burst, so without coalescing a
     * twelve-widget page would open (and abort) twelve connections on the way
     * to the one it wants.
     */
    debounceMs?: number;
    /** First reconnect delay, doubling to {@link maxRetryMs}. Default 1 s. */
    retryMs?: number;
    /** Ceiling for the reconnect backoff. Default 30 s. */
    maxRetryMs?: number;
    /**
     * Notified when the CONNECTION fails (and will be retried). A single
     * subscription's failure is not this — it goes to that subscriber's own
     * error callback, because the page's other widgets are unaffected.
     */
    onError?: (error: Error) => void;
}

/** What the channel has done, for tests and devtools. */
export interface LiveChannelStats {
    /** Connections opened, including reopens and reconnects. */
    opens: number;
    /** Connection-level failures (each followed by a backoff retry). */
    failures: number;
    /** Values delivered to subscribers, after re-seed suppression. */
    values: number;
    /** Per-subscription error frames delivered. */
    errors: number;
    /** Distinct subscriptions currently held. */
    subscriptions: number;
}

export interface LiveChannel extends ActorLiveChannel {
    stats(): LiveChannelStats;
    /** Abort the connection and drop every subscription. */
    close(): void;
}

interface Listener {
    onValue: (value: unknown) => void;
    onError?: (error: Error) => void;
}

interface Entry {
    wire: LiveSubscription;
    listeners: Set<Listener>;
    /** Fingerprint of the last delivered value — the re-seed suppressor. */
    print: string | null;
    /** The last delivered value, replayed to a late subscriber. */
    value: unknown;
    seeded: boolean;
}

const DEFAULT_DEBOUNCE_MS = 20;
const DEFAULT_RETRY_MS = 1_000;
const DEFAULT_MAX_RETRY_MS = 30_000;

/** Subscription identity: the tuple that decides who shares a watch. */
function canonical(sub: ActorSubscription): string {
    return JSON.stringify([sub.type, sub.key, sub.method, sub.args ?? []]);
}

/**
 * A comparable rendering of a pushed value, or `null` when there is none.
 *
 * Used ONLY to suppress the identical re-seed a reopen produces, so being
 * conservative is free: `null` (a circular structure, a bigint) means "cannot
 * compare", and an unsuppressed duplicate frame is a redundant render rather
 * than a wrong one. Map and Set are unfolded because `JSON.stringify` renders
 * every one of them as `{}`, which would make two different maps compare
 * equal — the one failure mode that could suppress a REAL update.
 */
function fingerprint(value: unknown): string | null {
    try {
        return (
            JSON.stringify(value, (_key, inner: unknown) =>
                inner instanceof Map
                    ? ['@map', [...inner]]
                    : inner instanceof Set
                      ? ['@set', [...inner]]
                      : typeof inner === 'bigint'
                        ? `@bigint:${inner}`
                        : inner
            ) ?? 'undefined'
        );
    } catch {
        return null;
    }
}

/** A listener that throws must not cost the connection its other subscribers. */
function quietly(run: () => void): void {
    try {
        run();
    } catch (error) {
        if (__DEV__) console.error('[sigx actors] a live subscriber threw:', error);
    }
}

/**
 * Hand a frame to every listener, over a SNAPSHOT of the set.
 *
 * A subscriber may unsubscribe itself — or a sibling — from inside its own
 * callback (a component unmounting on the value it just received is the
 * ordinary case), and mutating the set mid-iteration would let that decide by
 * accident who else hears this frame.
 */
function notify(listeners: Set<Listener>, run: (listener: Listener) => void): void {
    // eslint-disable-next-line unicorn/no-useless-spread
    for (const listener of [...listeners]) quietly(() => run(listener));
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
        if (signal.aborted) return resolve();
        const done = (): void => {
            clearTimeout(timer);
            signal.removeEventListener('abort', done);
            resolve();
        };
        const timer = setTimeout(done, ms);
        signal.addEventListener('abort', done);
    });
}

/**
 * Build the default live channel over a transport's `stream()`.
 *
 * The transport is resolved per connection rather than captured, so a page
 * that reconfigures it (or a plugin that installs one after the app is built)
 * is picked up on the next open instead of pinning the first one forever.
 */
export function createLiveChannel(
    getTransport: () => ActorTransport | null,
    options: LiveChannelOptions = {}
): LiveChannel {
    const debounceMs = options.debounceMs ?? DEFAULT_DEBOUNCE_MS;
    const retryMs = options.retryMs ?? DEFAULT_RETRY_MS;
    const maxRetryMs = options.maxRetryMs ?? DEFAULT_MAX_RETRY_MS;

    const entries = new Map<string, Entry>();
    const stats: LiveChannelStats = {
        opens: 0,
        failures: 0,
        values: 0,
        errors: 0,
        subscriptions: 0
    };

    /**
     * A transport that brings its own push channel, resolved once per
     * transport. Memoized on the transport itself: `live()` may build a real
     * connection, so calling it per subscribe would open one per widget.
     */
    let delegateFor: ActorTransport | null = null;
    let delegate: ActorLiveChannel | null = null;
    const delegateChannel = (transport: ActorTransport): ActorLiveChannel | null => {
        if (!transport.live) return null;
        if (delegateFor !== transport) {
            delegateFor = transport;
            delegate = transport.live();
        }
        return delegate;
    };

    let timer: ReturnType<typeof setTimeout> | undefined;
    let connection: AbortController | null = null;
    /** Bumped per (re)start; a run whose generation is stale stops quietly. */
    let generation = 0;
    let closed = false;
    let warnedNoTransport = false;

    const schedule = (): void => {
        if (closed || timer !== undefined) return;
        timer = setTimeout(() => {
            timer = undefined;
            restart();
        }, debounceMs);
    };

    const restart = (): void => {
        generation++;
        // Not a failure: the run sees a stale generation and returns without
        // reporting or retrying.
        connection?.abort();
        connection = null;
        if (closed || entries.size === 0) return;
        const controller = new AbortController();
        connection = controller;
        void run(generation, controller);
    };

    /**
     * Fan one frame out to its subscription.
     *
     * `order` is the set AS SENT — indices are positions in that array, so it
     * has to be the snapshot this connection opened with, never the live map.
     *
     * An UNCHANGED value is dropped rather than delivered, which matters twice
     * and neither is an edge case:
     *
     *  - **Every reopen re-seeds every subscription.** That is what makes a
     *    reconnect need no resume token, and it means one widget mounting
     *    would otherwise look like the whole page updating.
     *  - **A mutating turn re-runs EVERY subscription on that actor.** Change
     *    a room's topic and its `recent(20)` watch re-runs too, returning an
     *    identical list. Passing that on is a re-render (and a page-cache
     *    write) with nothing behind it.
     *
     * Safe because these are views of current state, not an event log: a
     * subscriber cannot need to know that the value it already holds was
     * recomputed. And a value that cannot be fingerprinted is never
     * suppressed — see `fingerprint`.
     */
    const deliver = (frame: LiveFrame, order: readonly Entry[]): void => {
        if ('p' in frame) return; // keepalive: moves bytes, carries nothing
        const entry = order[frame.i];
        if (!entry) return; // an index outside the set we sent
        if ('e' in frame) {
            stats.errors++;
            const error = wireFail(
                frame.e.status,
                frame.e,
                `[sigx actors] live subscription ${entry.wire.t}/${entry.wire.k}#${entry.wire.m} failed`
            );
            notify(entry.listeners, (listener) => listener.onError?.(error));
            return;
        }
        const print = fingerprint(frame.v);
        if (entry.seeded && print !== null && print === entry.print) return;
        entry.print = print;
        entry.value = frame.v;
        entry.seeded = true;
        stats.values++;
        notify(entry.listeners, (listener) => listener.onValue(frame.v));
    };

    const run = async (mine: number, controller: AbortController): Promise<void> => {
        let delay = retryMs;
        while (!closed && mine === generation) {
            const transport = getTransport();
            if (!transport) return;
            const order = [...entries.values()];
            const subs = order.map((entry) => entry.wire);
            try {
                stats.opens++;
                const frames = transport.stream(LIVE_SYMBOL, [subs], {
                    signal: controller.signal
                });
                for await (const frame of frames) {
                    if (closed || mine !== generation) return;
                    delay = retryMs; // a healthy frame earns a fresh budget
                    deliver(frame as LiveFrame, order);
                }
                // A clean `done` means every subscription ended server-side
                // (all of them rejected, or their actors stopped). Reopening
                // would spin, so wait for the set to change instead.
                return;
            } catch (error) {
                if (closed || mine !== generation || controller.signal.aborted) return;
                stats.failures++;
                const reported =
                    error instanceof Error ? error : new Error(String(error));
                if (options.onError) quietly(() => options.onError!(reported));
                else if (__DEV__) {
                    console.warn('[sigx actors] live connection dropped, retrying:', reported);
                }
                // Jitter so a host restart does not bring every tab back at
                // the same millisecond.
                await sleep(delay + Math.random() * 250, controller.signal);
                delay = Math.min(delay * 2, maxRetryMs);
            }
        }
    };

    return {
        subscribe(sub, onValue, onError) {
            if (closed) return () => {};
            const transport = getTransport();
            if (!transport) {
                // The server render path, and a client with no transport at
                // all. Silent by design on the server: `useActorState` reads
                // the same code path during SSR and there is nothing to push
                // to.
                return () => {};
            }
            const own = delegateChannel(transport);
            if (own) return own.subscribe(sub, onValue, onError);
            if (typeof transport.stream !== 'function') {
                // Nothing to open. Registering anyway would schedule a reopen
                // that throws inside the run loop, which the retry logic reads
                // as a dropped connection — an endless reconnect against a
                // transport that can never serve one. `stream` is required by
                // the interface, so this is the JS / `any` path, and the honest
                // answer there is to degrade to "not live".
                if (__DEV__ && !warnedNoTransport) {
                    warnedNoTransport = true;
                    console.warn(
                        `[sigx actors] transport "${transport.name}" has no stream() and no ` +
                            'live(), so live reads cannot update. They still work as ordinary ' +
                            'reads; give the transport one or the other to make them live.'
                    );
                }
                return () => {};
            }

            const id = canonical(sub);
            let entry = entries.get(id);
            const listener: Listener = onError ? { onValue, onError } : { onValue };
            if (!entry) {
                entry = {
                    wire: {
                        t: sub.type,
                        k: sub.key,
                        m: sub.method,
                        ...(sub.args && sub.args.length > 0 ? { a: sub.args } : {})
                    },
                    listeners: new Set(),
                    print: null,
                    value: undefined,
                    seeded: false
                };
                entries.set(id, entry);
                stats.subscriptions = entries.size;
                // A NEW subscription changes the set, so the connection has
                // to carry it: reopen. An additional listener on an existing
                // subscription does not — it is served from the cache below.
                schedule();
            }
            const mine = entry;
            mine.listeners.add(listener);
            if (mine.seeded) {
                // Freshest known value for a late subscriber, without a
                // reopen. Asynchronous so `subscribe()` never calls back
                // before it has returned its disposer.
                queueMicrotask(() => {
                    if (mine.listeners.has(listener)) quietly(() => onValue(mine.value));
                });
            }

            return () => {
                if (!mine.listeners.delete(listener)) return;
                if (mine.listeners.size > 0) return;
                entries.delete(id);
                stats.subscriptions = entries.size;
                schedule();
            };
        },

        stats: () => ({ ...stats }),

        close() {
            closed = true;
            if (timer !== undefined) clearTimeout(timer);
            timer = undefined;
            generation++;
            connection?.abort();
            connection = null;
            entries.clear();
            stats.subscriptions = 0;
            delegateFor = null;
            delegate = null;
        }
    };
}
