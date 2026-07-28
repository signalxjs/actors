/**
 * `httpTransport()` — the default silo-to-silo transport, and the only one
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
import type { ActorRoute } from '../silo/app';
import type { ActorCallContext, ActorDispatcher, ActorRef } from '../types';
import { readNdjson, type WireError } from '../wire-shared';
import { encodeEnvelope, signAuth, SILO_AUTH_HEADER, SILO_CALL_HEADER } from './envelope';
import type {
    SiloTransport,
    SiloTransportConfig,
    SiloTransportFactory,
    SiloTransportRuntime
} from './seam';
import {
    handleSiloRequestForRuntime,
    matchesSiloRequest,
    type SiloEndpointOptions
} from './silo-endpoint';
import type { SiloDescriptor } from './types';

export interface HttpTransportOptions {
    /**
     * Fetch implementation. Tests and benchmarks pipe this straight into
     * peers' handlers; it is also where a tuned dispatcher goes —
     * `(url, init) => undiciFetch(url, { ...init, dispatcher: agent })`.
     */
    fetch?: typeof globalThis.fetch;
    /** Forwarded to the internal mount (body caps, `onError`, `timeoutMs`). */
    endpoint?: SiloEndpointOptions;
}

/** This transport's key in `SiloDescriptor.addresses`. */
export const HTTP_TRANSPORT_NAME = 'http';

/**
 * The peer's origin for THIS transport. Falls back to `address` — what a
 * silo from a build predating `addresses` publishes, and what every silo
 * publishes for HTTP anyway, since the internal mount rides the same
 * listener as the public one.
 */
function originFor(target: SiloDescriptor): string {
    const raw = target.addresses?.[HTTP_TRANSPORT_NAME] ?? target.address;
    return raw.endsWith('/') ? raw.slice(0, -1) : raw;
}

export function httpTransport(options: HttpTransportOptions = {}): SiloTransportFactory {
    return (config: SiloTransportConfig): SiloTransport => {
        const doFetch = options.fetch ?? globalThis.fetch;
        const base = config.internalBase.endsWith('/')
            ? config.internalBase.slice(0, -1)
            : config.internalBase;

        // Captured at `start()`. The route closure is built now — plugin
        // setup reads `routes` before the silo exists — but can only serve
        // once there is a silo behind it.
        let runtime: SiloTransportRuntime | null = null;

        const send = async (
            target: SiloDescriptor,
            ref: ActorRef,
            method: string,
            args: readonly unknown[],
            call: ActorCallContext,
            signal: AbortSignal | undefined
        ): Promise<Response> => {
            const symbol = `${ref.type}#${method}`;
            const url = `${originFor(target)}${base}/${encodeURIComponent(symbol)}`;
            const headers: Record<string, string> = {
                'content-type': 'application/json',
                [SILO_CALL_HEADER]: encodeEnvelope(call, config.siloId)
            };
            if (config.secret !== undefined) {
                // Per-request HMAC bound to this symbol + callId (see envelope.ts).
                headers[SILO_AUTH_HEADER] = await signAuth(config.secret, symbol, call.callId);
            }
            try {
                return await doFetch(url, {
                    method: 'POST',
                    headers,
                    body: JSON.stringify({ args: config.codec.encode([ref.key, ...args]) }),
                    ...(signal ? { signal } : {})
                });
            } catch (error) {
                if (signal?.aborted) throw error;
                throw new ActorUnreachableError(`${target.siloId} (${originFor(target)})`, {
                    cause: error
                });
            }
        };

        const dispatcherFor = (target: SiloDescriptor): ActorDispatcher => ({
            async dispatch(ref, method, args, call) {
                const res = await send(target, ref, method, args, call, call.abortSignal);
                let parsed: { data?: unknown; error?: WireError } | undefined;
                try {
                    parsed = JSON.parse(await res.text(), config.codec.reviver) as {
                        data?: unknown;
                        error?: WireError;
                    };
                } catch {
                    parsed = undefined;
                }
                if (!res.ok || !parsed || parsed.error) {
                    throw config.fromWireError(
                        res.status,
                        parsed?.error,
                        `[sigx actors] silo call ${ref.type}#${method} to ${target.siloId} ` +
                            `failed with HTTP ${res.status}`
                    );
                }
                return config.codec.decode(parsed.data);
            },

            dispatchStream(ref, method, args, call) {
                const controller = new AbortController();
                const signal = call.abortSignal
                    ? AbortSignal.any([call.abortSignal, controller.signal])
                    : controller.signal;
                const symbol = `${ref.type}#${method}`;
                async function* stream(): AsyncGenerator<unknown> {
                    try {
                        const res = await send(target, ref, method, args, call, signal);
                        if (!res.ok || !res.body) {
                            let wire: WireError | undefined;
                            try {
                                wire = (
                                    JSON.parse(await res.text(), config.codec.reviver) as {
                                        error?: WireError;
                                    }
                                )?.error;
                            } catch {
                                wire = undefined;
                            }
                            throw config.fromWireError(
                                res.status,
                                wire,
                                `[sigx actors] silo stream ${symbol} to ${target.siloId} ` +
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
                        controller.abort(); // consumer break/return or end
                    }
                }
                return stream();
            }
        });

        const route: ActorRoute = {
            name: 'cluster:silo',
            match: (request) => matchesSiloRequest(request, base),
            handle: (request) => {
                if (!runtime) {
                    // Routes only run on a started silo, so this is the narrow
                    // window between mounting and `start()`. It answers with
                    // the `silo-shutdown` KIND, not a bare 503: the caller
                    // classifies on the kind, and an unclassifiable 503 is a
                    // hard failure where a retry would have converged.
                    return Promise.resolve(
                        new Response(
                            JSON.stringify({
                                error: {
                                    message: '[sigx actors] the silo is not started',
                                    status: 503,
                                    data: { kind: 'silo-shutdown' }
                                }
                            }),
                            { status: 503, headers: { 'content-type': 'application/json' } }
                        )
                    );
                }
                return handleSiloRequestForRuntime(request, {
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
            // must reach the SILO and get its `silo-shutdown` (which the
            // caller retries and re-routes on), not a mount that has gone
            // silent underneath it.
        };
    };
}
