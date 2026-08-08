/**
 * `@sigx/actors-ws/client` — `socketTransport()`: browsers (and any WinterCG
 * runtime) calling actors over ONE multiplexed socket, speaking the
 * `@sigx/actors/socket-wire` vocabulary to `createActorSocketSession` (#99).
 *
 * WinterCG-clean by construction: no `node:` imports, the global `WebSocket`
 * as the default carrier, and the connection seam is a LINK, not a URL — so
 * socket.io, a test harness or any message channel can carry the traffic by
 * supplying `connect` (the same idea `FrameLink` proved one layer down).
 *
 * ## What this transport is honest about
 *
 * - **It is host-affine.** One peer, many actors: every call re-dispatches
 *   through placement on the session side (`onMiss: 'proxy'` semantics).
 *   Per-call `endpoint` overrides — the router seam's mechanism — are
 *   ignored, with one `__DEV__` warning: honouring them would mean a socket
 *   per host and destroy the connection collapse that is the point.
 * - **In-flight calls fail when the socket drops, and are NEVER retried
 *   here.** Same policy as the cluster connection: silently re-sending a
 *   non-idempotent actor method is a correctness bug, not a retry. The
 *   next call dials a fresh connection (with `retryMs`/`maxRetryMs`
 *   backoff between failed attempts).
 * - **Cancelling one call never closes the socket** — a cancel is a
 *   message (`{i,c:1}`), because everything is multiplexed.
 */
import type { ActorCallInit, ActorTransport } from '@sigx/actors/client';
import {
    encodeWire,
    parseWire,
    reviveWire,
    wireFail,
    type SocketReply,
    type SocketRequest,
    type WireError
} from '@sigx/actors/socket-wire';

/** One live connection, as the transport sees it. */
export interface SocketLink {
    send(message: string): void;
    close(): void;
}

/** What a connection must deliver back. `onOpen` gates the first send. */
export interface SocketHandlers {
    onOpen(): void;
    onMessage(message: string): void;
    onClose(): void;
}

export interface SocketTransportOptions {
    /**
     * Build one connection attempt. PRIMARY seam; `url` is sugar over it.
     * Called again after a drop — each call is one fresh attempt.
     */
    connect?(handlers: SocketHandlers): SocketLink;
    /** Sugar: dial `new WebSocket(url)` on the global constructor. */
    url?: string;
    /** Backoff floor between failed connection attempts. Default 300 ms. */
    retryMs?: number;
    /** Backoff ceiling. Default 10 s. */
    maxRetryMs?: number;
}

interface InFlight {
    frame(reply: SocketReply & { i: number }): void;
    fail(error: Error): void;
}

const label = (symbol: string): string => `[sigx actors] socket call "${symbol}"`;

function abortError(reason: unknown): Error {
    if (reason instanceof Error) return reason;
    const error = new Error('[sigx actors] call aborted');
    error.name = 'AbortError';
    return error;
}

function webSocketConnect(url: string): (handlers: SocketHandlers) => SocketLink {
    return (handlers) => {
        const WS = (globalThis as { WebSocket?: new (url: string) => WebSocket }).WebSocket;
        if (!WS) {
            throw new Error(
                `[sigx actors] socketTransport({ url }) needs a global WebSocket ` +
                    `constructor — this runtime has none, so pass connect() instead.`
            );
        }
        const ws = new WS(url);
        ws.addEventListener('open', () => handlers.onOpen());
        ws.addEventListener('message', (event) => {
            // The protocol is text JSON; a binary frame is not ours to guess at.
            if (typeof (event as MessageEvent).data === 'string') {
                handlers.onMessage((event as MessageEvent).data as string);
            }
        });
        ws.addEventListener('close', () => handlers.onClose());
        // A failed dial fires 'error' then 'close'; close is the one signal.
        ws.addEventListener('error', () => {});
        return {
            send: (message) => ws.send(message),
            close: () => ws.close()
        };
    };
}

/**
 * Build the client-facing socket transport. Lazy: nothing is dialled until
 * the first call, so a transport configured at module scope costs nothing
 * on pages that never touch an actor.
 */
export function socketTransport(options: SocketTransportOptions = {}): ActorTransport {
    const connectFn =
        options.connect ??
        (options.url !== undefined ? webSocketConnect(options.url) : undefined);
    if (!connectFn) {
        throw new Error('[sigx actors] socketTransport needs either `connect` or `url`.');
    }
    const retryMs = options.retryMs ?? 300;
    const maxRetryMs = options.maxRetryMs ?? 10_000;

    let closed = false;
    let link: SocketLink | null = null;
    let opening: Promise<void> | null = null;
    let failures = 0;
    let nextId = 1;
    const inFlight = new Map<number, InFlight>();
    let warnedEndpoint = false;
    let warnedHeaders = false;

    const closedError = (): Error =>
        Object.assign(new Error('[sigx actors] socket transport is closed'), {
            __sigxServerFnError: true,
            status: 0
        });

    const lostError = (): Error =>
        Object.assign(
            new Error(
                '[sigx actors] socket closed with this call in flight — it was NOT ' +
                    'retried (re-sending a non-idempotent actor method is a ' +
                    'correctness bug, not a retry)'
            ),
            { __sigxServerFnError: true, status: 0 }
        );

    const failAll = (error: Error): void => {
        const entries = [...inFlight.values()];
        inFlight.clear();
        for (const entry of entries) entry.fail(error);
    };

    const handleMessage = (message: string): void => {
        let reply: SocketReply;
        try {
            reply = parseWire<SocketReply>(message);
        } catch {
            // Not our vocabulary. The session never sends this; drop it
            // rather than take the connection down over a stray frame.
            return;
        }
        if (typeof reply !== 'object' || reply === null || !('i' in reply)) return;
        // Unknown ids are fine — a reply legitimately races its cancel.
        inFlight.get(reply.i)?.frame(reply as SocketReply & { i: number });
    };

    const dropLink = (wasOpen: boolean): void => {
        link = null;
        opening = null;
        if (wasOpen) failAll(lostError());
    };

    const openOnce = async (): Promise<void> => {
        if (failures > 0) {
            const wait = Math.min(retryMs * 2 ** (failures - 1), maxRetryMs);
            await new Promise((resolve) => setTimeout(resolve, wait));
        }
        if (closed) throw closedError();
        await new Promise<void>((resolve, reject) => {
            let settled = false;
            let attempt: SocketLink;
            try {
                attempt = connectFn({
                    onOpen: () => {
                        if (settled) return;
                        settled = true;
                        link = attempt;
                        failures = 0;
                        opening = null;
                        resolve();
                    },
                    onMessage: handleMessage,
                    onClose: () => {
                        if (!settled) {
                            settled = true;
                            failures += 1;
                            opening = null;
                            reject(
                                Object.assign(
                                    new Error('[sigx actors] socket connection failed'),
                                    { __sigxServerFnError: true, status: 0 }
                                )
                            );
                            return;
                        }
                        if (link === attempt) dropLink(true);
                    }
                });
            } catch (error) {
                settled = true;
                failures += 1;
                opening = null;
                reject(error);
                return;
            }
            if (closed) attempt.close();
        });
    };

    const ready = async (): Promise<void> => {
        if (closed) throw closedError();
        if (link) return;
        opening ??= openOnce();
        return opening;
    };

    const post = (request: SocketRequest): void => {
        link?.send(JSON.stringify(request));
    };

    const checkInit = (init: ActorCallInit | undefined): void => {
        if (!__DEV__ || !init) return;
        if (init.endpoint !== undefined && !warnedEndpoint) {
            warnedEndpoint = true;
            console.warn(
                '[sigx actors] socketTransport ignores per-call endpoints — the socket ' +
                    'is host-affine and every call re-dispatches through placement. A ' +
                    'router (routedTransport) buys nothing on this transport.'
            );
        }
        if (init.headers !== undefined && !warnedHeaders) {
            warnedHeaders = true;
            console.warn(
                '[sigx actors] socketTransport cannot send per-call headers — the ' +
                    'connection was authenticated once at upgrade.'
            );
        }
    };

    return {
        name: 'socket',

        async call(symbol: string, args: unknown[], init?: ActorCallInit): Promise<unknown> {
            checkInit(init);
            if (init?.signal?.aborted) throw abortError(init.signal.reason);
            await ready();
            const id = nextId++;
            const a = encodeWire(args) as unknown[];
            if (init?.oneWay === true) {
                // No reply frames exist for a one-way call; the send is all.
                post({ i: id, s: symbol, a, w: 1 });
                return undefined;
            }
            return new Promise<unknown>((resolve, reject) => {
                let value: unknown;
                const signal = init?.signal;
                const onAbort = (): void => {
                    if (inFlight.delete(id)) post({ i: id, c: 1 });
                    reject(abortError(signal?.reason));
                };
                const done = (settle: () => void): void => {
                    signal?.removeEventListener('abort', onAbort);
                    settle();
                };
                signal?.addEventListener('abort', onAbort, { once: true });
                inFlight.set(id, {
                    frame(reply) {
                        if ('v' in reply) {
                            value = reviveWire(reply.v);
                        } else if ('d' in reply) {
                            inFlight.delete(id);
                            done(() => resolve(value));
                        } else if ('e' in reply) {
                            inFlight.delete(id);
                            const wire = reply.e as WireError;
                            done(() =>
                                reject(wireFail(wire.status ?? 500, wire, label(symbol)))
                            );
                        }
                    },
                    fail(error) {
                        done(() => reject(error));
                    }
                });
                post({ i: id, s: symbol, a });
            });
        },

        stream(symbol: string, args: unknown[], init?: ActorCallInit): AsyncIterable<unknown> {
            checkInit(init);
            const run = async function* (): AsyncGenerator<unknown> {
                if (init?.signal?.aborted) throw abortError(init.signal.reason);
                await ready();
                const id = nextId++;
                const queue: unknown[] = [];
                let ended = false;
                let failure: Error | null = null;
                let wake: (() => void) | null = null;
                const entry: InFlight = {
                    frame(reply) {
                        if ('v' in reply) queue.push(reviveWire(reply.v));
                        else if ('d' in reply) {
                            ended = true;
                            inFlight.delete(id);
                        } else if ('e' in reply) {
                            const wire = reply.e as WireError;
                            failure = wireFail(wire.status ?? 500, wire, label(symbol));
                            inFlight.delete(id);
                        }
                        wake?.();
                    },
                    fail(error) {
                        failure = error;
                        wake?.();
                    }
                };
                const onAbort = (): void => entry.fail(abortError(init?.signal?.reason));
                init?.signal?.addEventListener('abort', onAbort, { once: true });
                inFlight.set(id, entry);
                try {
                    post({ i: id, s: symbol, a: encodeWire(args) as unknown[], k: 1 });
                    for (;;) {
                        while (queue.length > 0) yield queue.shift();
                        if (failure) throw failure;
                        if (ended) return;
                        await new Promise<void>((resolve) => {
                            wake = resolve;
                        });
                        wake = null;
                    }
                } finally {
                    init?.signal?.removeEventListener('abort', onAbort);
                    // Consumer break, abort, or failure: one cancel message,
                    // never a socket close.
                    if (inFlight.delete(id) && !ended) post({ i: id, c: 1 });
                }
            };
            return { [Symbol.asyncIterator]: () => run() };
        },

        /** Idempotent by `ActorTransport.close()` contract (#102). */
        close(): void {
            if (closed) return;
            closed = true;
            const current = link;
            link = null;
            opening = null;
            failAll(closedError());
            current?.close();
        }
    };
}
