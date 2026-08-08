/**
 * The live machinery BOTH client channels share (#99): the `$live`
 * NDJSON channel (`./live`) and the socket transport's incremental channel
 * (`createSocketLiveChannel`, re-exported through `@sigx/actors/socket-wire`
 * so an out-of-repo transport reaches it too).
 *
 * Two properties live here precisely so they cannot fork:
 *
 * - **Identity coalescing** — `canonical()`, N listeners per wire
 *   subscription, late-subscriber replay. Without it a socket channel
 *   re-earns the fan-out cost `$live`'s multiplex exists to avoid: twelve
 *   widgets watching one value must cost ONE subscription.
 * - **Re-seed suppression** — `fingerprint()`. Sockets still drop, and a
 *   re-established subscription re-delivers its current value; comparing
 *   prints is what keeps a reconnect from re-rendering a page that did not
 *   change. Forking this function is exactly the kind of subtlety that
 *   survives one copy and not two (Map/Set unfold, or two different maps
 *   compare equal and a REAL update is suppressed).
 */
import type { ActorLiveChannel, ActorSubscription } from '../client/index';
import { wireFail } from '../wire-shared';
import type { LiveSubscription, SocketReply, SocketRequest } from '../socket-wire';

/** Subscription identity: the tuple that decides who shares a watch. */
export function canonical(sub: ActorSubscription): string {
    return JSON.stringify([sub.type, sub.key, sub.method, sub.args ?? []]);
}

/**
 * A comparable rendering of a pushed value, or `null` when there is none.
 *
 * Used ONLY to suppress the identical re-delivery a re-seed produces, so
 * being conservative is free: `null` (a circular structure) means "cannot
 * compare", and an unsuppressed duplicate frame is a redundant render rather
 * than a wrong one. Map and Set are unfolded because `JSON.stringify`
 * renders every one of them as `{}`, which would make two different maps
 * compare equal — the one failure mode that could suppress a REAL update.
 */
export function fingerprint(value: unknown): string | null {
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

export interface LiveListener {
    onValue(value: unknown): void;
    onError?(error: Error): void;
}

/** A listener that throws must not cost the channel its other subscribers. */
export function quietly(run: () => void): void {
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
 * ordinary case), and mutating the set mid-iteration would let that decide
 * by accident who else hears this frame.
 */
export function notify(listeners: Set<LiveListener>, run: (listener: LiveListener) => void): void {
    // eslint-disable-next-line unicorn/no-useless-spread
    for (const listener of [...listeners]) quietly(() => run(listener));
}

/** One coalesced subscription: the wire record plus its delivery cache. */
export interface SubscriptionEntry {
    /** Canonical identity — the coalescing key. */
    readonly id: string;
    /** The record that rides the wire. */
    readonly wire: LiveSubscription;
    readonly listeners: Set<LiveListener>;
    /** Fingerprint of the last delivered value (re-seed suppression). */
    print: string | null;
    /** The last delivered value, replayed to a late subscriber. */
    value: unknown;
    seeded: boolean;
}

export interface SubscriptionSet {
    /**
     * Register a listener. A NEW canonical identity fires `added`; an
     * additional listener on an existing one is served from the cache (the
     * last value replays on a microtask, so `subscribe()` never calls back
     * before returning its disposer). The disposer fires `removed` when the
     * LAST listener leaves.
     */
    subscribe(
        sub: ActorSubscription,
        onValue: (value: unknown) => void,
        onError?: (error: Error) => void
    ): () => void;
    entries(): SubscriptionEntry[];
    /** Deliver a value with re-seed suppression. True when it was delivered. */
    deliverValue(entry: SubscriptionEntry, value: unknown): boolean;
    deliverError(entry: SubscriptionEntry, error: Error): void;
    size(): number;
    clear(): void;
}

export function createSubscriptionSet(hooks: {
    added(entry: SubscriptionEntry): void;
    removed(entry: SubscriptionEntry): void;
}): SubscriptionSet {
    const entries = new Map<string, SubscriptionEntry>();
    return {
        subscribe(sub, onValue, onError) {
            const id = canonical(sub);
            let entry = entries.get(id);
            const listener: LiveListener = onError ? { onValue, onError } : { onValue };
            if (!entry) {
                entry = {
                    id,
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
                hooks.added(entry);
            }
            const mine = entry;
            mine.listeners.add(listener);
            if (mine.seeded) {
                queueMicrotask(() => {
                    if (mine.listeners.has(listener)) quietly(() => onValue(mine.value));
                });
            }
            return () => {
                if (!mine.listeners.delete(listener)) return;
                if (mine.listeners.size > 0) return;
                entries.delete(id);
                hooks.removed(mine);
            };
        },
        entries: () => [...entries.values()],
        deliverValue(entry, value) {
            const print = fingerprint(value);
            if (entry.seeded && print !== null && print === entry.print) return false;
            entry.print = print;
            entry.value = value;
            entry.seeded = true;
            notify(entry.listeners, (listener) => listener.onValue(value));
            return true;
        },
        deliverError(entry, error) {
            notify(entry.listeners, (listener) => listener.onError?.(error));
        },
        size: () => entries.size,
        clear: () => entries.clear()
    };
}

// ---------------------------------------------------------------------------
// The socket live channel — the client half of `{i,sub}` / `{i,uns}`

/** What the channel needs from its transport. Ids come from the TRANSPORT'S
 *  allocator: replies carry only `i`, so calls and subscriptions must share
 *  one namespace or `{i,v}` becomes ambiguous. */
export interface SocketLiveHost {
    allocateId(): number;
    /** Deliver one request; the transport owns connection readiness. */
    send(request: SocketRequest): void;
    /** Revive one pushed value through the wire codec. */
    revive(value: unknown): unknown;
}

export interface SocketLiveChannel {
    /** The seam `delegateChannel()` consumes. */
    channel: ActorLiveChannel;
    /** Route one reply frame; true when it belonged to a subscription. */
    handle(reply: SocketReply & { i: number }): boolean;
    /**
     * The connection reopened: re-establish every subscription under fresh
     * ids — a re-seed, exactly what a `$live` reopen does, with
     * `fingerprint()` suppressing the values that did not change.
     */
    reseed(): void;
    size(): number;
    close(): void;
}

/**
 * Incremental live over a socket: adding a subscription is one ~40-byte
 * `{i,sub}` message and removing one is `{i,uns}` — no debounce, no
 * restart, no generation counter, because nothing reopens. Re-seed exists
 * only where it is real: on reconnect.
 */
export function createSocketLiveChannel(host: SocketLiveHost): SocketLiveChannel {
    /** Wire id → entry, for frames; entry id → wire id, for uns/reseed. */
    const byWireId = new Map<number, SubscriptionEntry>();
    const wireIds = new Map<string, number>();
    let closed = false;

    const open = (entry: SubscriptionEntry): void => {
        const id = host.allocateId();
        byWireId.set(id, entry);
        wireIds.set(entry.id, id);
        host.send({ i: id, sub: entry.wire });
    };

    const set = createSubscriptionSet({
        added(entry) {
            if (!closed) open(entry);
        },
        removed(entry) {
            const id = wireIds.get(entry.id);
            if (id === undefined) return;
            wireIds.delete(entry.id);
            byWireId.delete(id);
            if (!closed) host.send({ i: id, uns: 1 });
        }
    });

    return {
        channel: {
            subscribe: (sub, onValue, onError) => {
                if (closed) return () => {};
                return set.subscribe(sub, onValue, onError);
            }
        },
        handle(reply) {
            const entry = byWireId.get(reply.i);
            if (!entry) return false;
            if ('v' in reply) {
                set.deliverValue(entry, host.revive(reply.v));
            } else if ('e' in reply) {
                // Terminal for the subscription server-side; the entry stays
                // so a reconnect re-seeds it, mirroring `$live`'s retry.
                set.deliverError(
                    entry,
                    wireFail(
                        reply.e.status ?? 500,
                        reply.e,
                        `[sigx actors] live subscription ${entry.wire.t}/${entry.wire.k}#${entry.wire.m} failed`
                    )
                );
            }
            return true;
        },
        reseed() {
            if (closed) return;
            byWireId.clear();
            wireIds.clear();
            for (const entry of set.entries()) open(entry);
        },
        size: () => set.size(),
        close() {
            closed = true;
            byWireId.clear();
            wireIds.clear();
            set.clear();
        }
    };
}
