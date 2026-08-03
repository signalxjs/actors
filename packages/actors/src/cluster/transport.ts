/**
 * `httpTransport()` — the default host-to-host transport, and the only one
 * that is WinterCG-clean: outbound is a fetch to the owner's internal
 * mount, inbound is a route contributed to the app's own listener. Body and
 * NDJSON framing are byte-identical to the public wire (same codec, caps,
 * and cancellation chain); call metadata rides the envelope header. Errors
 * come back re-created with their ACTOR brand so a caller can't tell a
 * remote hop from a local dispatch.
 *
 * Riding a route rather than its own socket is exactly why this one works
 * on Cloudflare Workers, and why it stays the default.
 */
import { ActorUnreachableError, isActorError } from '../errors';
import type { ActorRoute } from '../host/app';
import type { ActorCallContext, ActorDispatcher } from '../types';
import { relayStream } from '../stream-relay';
import { parseWireWith, readNdjson, type WireError } from '../wire-shared';
import { encodeEnvelope, signAuth, HOST_AUTH_HEADER, HOST_CALL_HEADER } from './envelope';
import type {
    HostTransport,
    HostTransportConfig,
    HostTransportFactory,
    HostTransportRuntime
} from './seam';
import {
    handleHostRequestForRuntime,
    matchesHostRequest,
    type HostEndpointOptions
} from './host-endpoint';
import type { HostDescriptor } from './types';
import { watchSymbol } from './watch-symbol';

export interface HttpTransportOptions {
    /**
     * Fetch implementation. Tests and benchmarks pipe this straight into
     * peers' handlers; it is also where a tuned dispatcher goes —
     * `(url, init) => undiciFetch(url, { ...init, dispatcher: agent })`.
     */
    fetch?: typeof globalThis.fetch;
    /** Forwarded to the internal mount (body caps, `onError`, `timeoutMs`). */
    endpoint?: HostEndpointOptions;
}

/** This transport's key in `HostDescriptor.addresses`. */
export const HTTP_TRANSPORT_NAME = 'http';

/**
 * The peer's origin for THIS transport. Falls back to `address` — what a
 * host from a build predating `addresses` publishes, and what every host
 * publishes for HTTP anyway, since the internal mount rides the same
 * listener as the public one.
 */
function originFor(target: HostDescriptor): string {
    const raw = target.addresses?.[HTTP_TRANSPORT_NAME] ?? target.address;
    return raw.endsWith('/') ? raw.slice(0, -1) : raw;
}

export function httpTransport(options: HttpTransportOptions = {}): HostTransportFactory {
    return (config: HostTransportConfig): HostTransport => {
        const doFetch = options.fetch ?? globalThis.fetch;
        const base = config.internalBase.endsWith('/')
            ? config.internalBase.slice(0, -1)
            : config.internalBase;

        // Captured at `start()`. The route closure is built now — plugin
        // setup reads `routes` before the host exists — but can only serve
        // once there is a host behind it.
        let runtime: HostTransportRuntime | null = null;

        const send = async (
            target: HostDescriptor,
            symbol: string,
            payload: readonly unknown[],
            call: ActorCallContext,
            signal: AbortSignal | undefined
        ): Promise<Response> => {
            const url = `${originFor(target)}${base}/${encodeURIComponent(symbol)}`;
            const headers: Record<string, string> = {
                'content-type': 'application/json',
                [HOST_CALL_HEADER]: encodeEnvelope(call, config.hostId)
            };
            if (config.secret !== undefined) {
                // Per-request HMAC bound to this symbol + callId (see envelope.ts).
                headers[HOST_AUTH_HEADER] = await signAuth(config.secret, symbol, call.callId);
            }
            try {
                return await doFetch(url, {
                    method: 'POST',
                    headers,
                    body: JSON.stringify({ args: config.codec.encode(payload) }),
                    ...(signal ? { signal } : {})
                });
            } catch (error) {
                if (signal?.aborted) throw error;
                throw new ActorUnreachableError(`${target.hostId} (${originFor(target)})`, {
                    cause: error
                });
            }
        };

        /**
         * The streamed reply path, shared by `dispatchStream` and
         * `dispatchWatch`. A watch answers with the same NDJSON a stream
         * does — the only difference is the symbol asking for it and the
         * options riding the payload — so the abort linking, the terminator
         * handling and the actor-kind re-lift are had once rather than
         * kept in step by hand.
         */
        const streamOver = (
            target: HostDescriptor,
            symbol: string,
            payload: readonly unknown[],
            call: ActorCallContext
        ): AsyncIterable<unknown> => {
            const controller = new AbortController();
            const signal = call.abortSignal
                ? AbortSignal.any([call.abortSignal, controller.signal])
                : controller.signal;
            async function* stream(): AsyncGenerator<unknown> {
                try {
                    const res = await send(target, symbol, payload, call, signal);
                    if (!res.ok || !res.body) {
                        let wire: WireError | undefined;
                        try {
                            wire = parseWireWith<{ error?: WireError }>(
                                await res.text(),
                                config.codec.reviver
                            )?.error;
                        } catch {
                            wire = undefined;
                        }
                        throw config.fromWireError(
                            res.status,
                            wire,
                            `[sigx actors] host stream ${symbol} to ${target.hostId} ` +
                                `failed with HTTP ${res.status}`
                        );
                    }
                    try {
                        yield* readNdjson(res, symbol);
                    } catch (error) {
                        // In-band error lines are serverFn-branded by
                        // readNdjson; lift actor kinds back to their brand.
                        const status = (error as { status?: number }).status;
                        const data = (error as { data?: unknown }).data;
                        if (!isActorError(error) && data !== undefined) {
                            throw config.fromWireError(
                                status ?? 500,
                                { message: (error as Error).message, status, data },
                                (error as Error).message
                            );
                        }
                        throw error;
                    }
                } finally {
                    controller.abort(); // normal end, or an error unwinding
                }
            }
            // Relayed, not returned raw. A generator parked at `yield*
            // readNdjson(...)` has `return()` QUEUED rather than run, so the
            // `finally` above never fires on a consumer hanging up — the fetch
            // is never aborted, the peer never learns, and its activation's
            // keep-alive ref leaks. `onClose` aborts synchronously, which is
            // what wakes the parked `reader.read()`.
            return relayStream(stream(), {
                mapError: (error) => error,
                onClose: () => controller.abort()
            });
        };

        const dispatcherFor = (target: HostDescriptor): ActorDispatcher => ({
            async dispatch(ref, method, args, call) {
                const res = await send(
                    target,
                    `${ref.type}#${method}`,
                    [ref.key, ...args],
                    call,
                    call.abortSignal
                );
                let parsed: { data?: unknown; error?: WireError } | undefined;
                try {
                    parsed = parseWireWith<{ data?: unknown; error?: WireError }>(
                        await res.text(),
                        config.codec.reviver
                    );
                } catch {
                    parsed = undefined;
                }
                if (!res.ok || !parsed || parsed.error) {
                    throw config.fromWireError(
                        res.status,
                        parsed?.error,
                        `[sigx actors] host call ${ref.type}#${method} to ${target.hostId} ` +
                            `failed with HTTP ${res.status}`
                    );
                }
                return config.codec.decode(parsed.data);
            },

            dispatchStream(ref, method, args, call) {
                return streamOver(target, `${ref.type}#${method}`, [ref.key, ...args], call);
            },

            dispatchWatch(ref, method, args, call, options) {
                // `options` leads the args, after the key — the same
                // convention the key itself follows. It is NOT in the
                // symbol: that would let a peer mint unbounded distinct
                // cache keys on the receiving mount.
                return streamOver(
                    target,
                    watchSymbol(ref.type, method),
                    [ref.key, options ?? null, ...args],
                    call
                );
            }
        });

        const route: ActorRoute = {
            name: 'cluster:host',
            match: (request) => matchesHostRequest(request, base),
            handle: (request) => {
                if (!runtime) {
                    // Routes only run on a started host, so this is the narrow
                    // window between mounting and `start()`. It answers with
                    // the `host-shutdown` KIND, not a bare 503: the caller
                    // classifies on the kind, and an unclassifiable 503 is a
                    // hard failure where a retry would have converged.
                    return Promise.resolve(
                        new Response(
                            JSON.stringify({
                                error: {
                                    message: '[sigx actors] the host is not started',
                                    status: 503,
                                    data: { kind: 'host-shutdown' }
                                }
                            }),
                            { status: 503, headers: { 'content-type': 'application/json' } }
                        )
                    );
                }
                return handleHostRequestForRuntime(request, {
                    ...options.endpoint,
                    runtime,
                    ...(config.secret !== undefined ? { secret: config.secret } : {})
                });
            }
        };

        return {
            name: HTTP_TRANSPORT_NAME,
            routes: [route],
            start(rt) {
                runtime = rt;
                // No address of its own: the internal mount rides the app's
                // listener, which is what `advertise` already names.
            },
            // Never null — every descriptor carries `address`, so HTTP reaches
            // every peer. That is also why it is only ever valid as the LAST
            // entry in a fallback chain.
            dispatcherFor
            // No `stop()`: the mount rides the app's listener, which the host
            // owns and tears down. Keeping the runtime bound past `stop()` is
            // deliberate — a peer still calling in during a rolling deploy
            // must reach the HOST and get its `host-shutdown` (which the
            // caller retries and re-routes on), not a mount that has gone
            // silent underneath it.
        };
    };
}
