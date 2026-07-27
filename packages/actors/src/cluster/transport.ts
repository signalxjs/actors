/**
 * The silo-to-silo HTTP transport: a remote `ActorDispatcher` whose
 * dispatch is a fetch to the owner's internal mount. Body and NDJSON
 * framing are byte-identical to the public wire (same codec, caps, and
 * cancellation chain); call metadata rides the envelope header. Errors come
 * back re-created with their ACTOR brand so a caller can't tell a remote
 * hop from a local dispatch.
 */
import {
    ActorUnreachableError,
    isActorError,
    type ActorErrorKind,
    type ActorOwnerHint
} from '../errors';
import type { ActorCallContext, ActorDispatcher, ActorRef } from '../types';
import { encodeWire, readNdjson, reviveWire, reviver, wireFail, type WireError } from '../wire-shared';
import { encodeEnvelope, signAuth, SILO_AUTH_HEADER, SILO_CALL_HEADER } from './envelope';
import type { SiloDescriptor } from './types';

export interface SiloTransportOptions {
    /** This silo's id — stamped into every envelope as `from`. */
    siloId: string;
    /** Path prefix of the peers' internal mount. */
    internalBase: string;
    secret?: string;
    fetch?: typeof globalThis.fetch;
}

export interface SiloTransport {
    /** A dispatcher whose calls cross the wire to `target`. */
    dispatcherFor(target: SiloDescriptor): ActorDispatcher;
}

const ACTOR_ERROR_KINDS = new Set<string>([
    'deadlock',
    'activation',
    'state-conflict',
    'method-not-found',
    'silo-shutdown',
    'call-timeout',
    'wrong-host',
    'unreachable'
]);

/**
 * Re-create a peer's error. Actor-branded kinds come back as actor errors
 * (a remote state-conflict must satisfy `isActorError` exactly like a local
 * one); anything else keeps the serverFn wire brand.
 */
function recreate(status: number, wire: WireError | undefined, fallback: string): Error {
    const data = wire?.data as { kind?: string; owner?: ActorOwnerHint } | undefined;
    const kind = data?.kind;
    if (typeof kind === 'string' && ACTOR_ERROR_KINDS.has(kind)) {
        // Brand-assign rather than re-run constructors: the peer's message
        // already carries the context, and brands (not instanceof) are the
        // contract across module graphs.
        return Object.assign(new Error(wire?.message ?? fallback), {
            __sigxActorError: true as const,
            kind: kind as ActorErrorKind,
            ...(kind === 'wrong-host' && data?.owner ? { owner: data.owner } : {})
        });
    }
    return wireFail(status, wire, fallback);
}

export function createSiloTransport(options: SiloTransportOptions): SiloTransport {
    const doFetch = options.fetch ?? globalThis.fetch;
    const base = options.internalBase.endsWith('/')
        ? options.internalBase.slice(0, -1)
        : options.internalBase;

    const send = async (
        target: SiloDescriptor,
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext,
        signal: AbortSignal | undefined
    ): Promise<Response> => {
        const symbol = `${ref.type}#${method}`;
        const address = target.address.endsWith('/')
            ? target.address.slice(0, -1)
            : target.address;
        const url = `${address}${base}/${encodeURIComponent(symbol)}`;
        const headers: Record<string, string> = {
            'content-type': 'application/json',
            [SILO_CALL_HEADER]: encodeEnvelope(call, options.siloId)
        };
        if (options.secret !== undefined) {
            // Per-request HMAC bound to this symbol + callId (see envelope.ts).
            headers[SILO_AUTH_HEADER] = await signAuth(options.secret, symbol, call.callId);
        }
        try {
            return await doFetch(url, {
                method: 'POST',
                headers,
                body: JSON.stringify({ args: encodeWire([ref.key, ...args]) }),
                ...(signal ? { signal } : {})
            });
        } catch (error) {
            if (signal?.aborted) throw error;
            throw new ActorUnreachableError(`${target.siloId} (${target.address})`, {
                cause: error
            });
        }
    };

    return {
        dispatcherFor(target: SiloDescriptor): ActorDispatcher {
            return {
                async dispatch(ref, method, args, call) {
                    const res = await send(target, ref, method, args, call, call.abortSignal);
                    let parsed: { data?: unknown; error?: WireError } | undefined;
                    try {
                        parsed = JSON.parse(await res.text(), reviver) as {
                            data?: unknown;
                            error?: WireError;
                        };
                    } catch {
                        parsed = undefined;
                    }
                    if (!res.ok || !parsed || parsed.error) {
                        throw recreate(
                            res.status,
                            parsed?.error,
                            `[sigx actors] silo call ${ref.type}#${method} to ${target.siloId} ` +
                                `failed with HTTP ${res.status}`
                        );
                    }
                    return reviveWire(parsed.data);
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
                                        JSON.parse(await res.text(), reviver) as {
                                            error?: WireError;
                                        }
                                    )?.error;
                                } catch {
                                    wire = undefined;
                                }
                                throw recreate(
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
                                    throw recreate(
                                        status ?? 500,
                                        {
                                            message: (error as Error).message,
                                            status,
                                            data
                                        },
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
            };
        }
    };
}
