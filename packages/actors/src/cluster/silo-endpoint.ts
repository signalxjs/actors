/**
 * The INTERNAL silo-to-silo endpoint — a sibling of `@sigx/actors/server`
 * riding the same `handleServerFnRequest` machinery (codec, body caps,
 * NDJSON, cancellation) with three deliberate differences:
 *
 *  1. no guards — the public edge already ran them; trust here comes from
 *     the route plus the shared secret, never a spoofable payload marker;
 *  2. the call context comes from the envelope header (chain, callId,
 *     remaining-ms deadline) instead of being minted fresh;
 *  3. dispatch is LOCAL-ONLY (`placement.dispatchInbound`) — a misdirected
 *     call answers 421 wrong-host with the owner hint (redirect-not-proxy),
 *     it is never forwarded onward.
 *
 * Mount beside the public endpoint on the same listener:
 * `matchesSiloRequest(req) ? handleSiloRequest(req, {...}) : ...`
 */
import { ServerFnError, type ServerFnContext, type ServerFnInfo } from '@sigx/server';
import {
    handleServerFnRequest,
    type ServerFnRequestOptions
} from '@sigx/server/server';
import { isActorError, type ActorWrongHostError } from '../errors';
import type { ActorCallContext, AnyActorDefinition, Silo } from '../types';
import { decodeEnvelope, verifyAuth, SILO_AUTH_HEADER, SILO_CALL_HEADER } from './envelope';
import type { ClusterPlacement } from './placement';

export interface SiloRequestOptions
    extends Omit<ServerFnRequestOptions, 'resolve' | 'renderBoundaries' | 'origin' | 'guard'> {
    silo: Silo;
    placement: ClusterPlacement;
    /** Shared cluster secret; when set, requests without it are 403'd. */
    secret?: string;
}

export function matchesSiloRequest(request: Request, base = '/_sigx/silo'): boolean {
    const prefix = base.endsWith('/') ? base : `${base}/`;
    return new URL(request.url).pathname.startsWith(prefix);
}

export function handleSiloRequest(
    request: Request,
    options: SiloRequestOptions
): Promise<Response> {
    const { silo, placement, secret, ...rest } = options;
    // Malformed percent-encoding would throw inside the core endpoint's
    // own decode (before any guard) — reject it here so an invalid path
    // can never crash the mount.
    try {
        decodeURIComponent(new URL(request.url).pathname);
    } catch {
        return Promise.resolve(
            new Response(
                JSON.stringify({
                    error: { message: '[sigx actors] cluster authentication failed', status: 403 }
                }),
                { status: 403, headers: { 'content-type': 'application/json' } }
            )
        );
    }
    return handleServerFnRequest(request, {
        ...rest,
        // Server-to-server traffic sends no Origin header; the per-request
        // HMAC (bound to symbol + callId, freshness-windowed) is the
        // authentication, checked before anything dispatches.
        origin: false,
        guard: async (rq) => {
            if (secret === undefined) return;
            let symbol: string;
            try {
                symbol = decodeURIComponent(
                    rq.url.pathname.slice(rq.url.pathname.lastIndexOf('/') + 1)
                );
            } catch {
                // Malformed percent-encoding can't crash the endpoint —
                // an undecodable path is an unauthenticated request.
                throw new ServerFnError(403, '[sigx actors] cluster authentication failed');
            }
            const callHeader = rq.request.headers.get(SILO_CALL_HEADER);
            let callId = '';
            try {
                callId = callHeader ? decodeEnvelope(callHeader).call.callId : '';
            } catch {
                callId = '';
            }
            const ok =
                callId !== '' &&
                (await verifyAuth(
                    secret,
                    rq.request.headers.get(SILO_AUTH_HEADER),
                    symbol,
                    callId
                ));
            if (!ok) {
                throw new ServerFnError(403, '[sigx actors] cluster authentication failed');
            }
        },
        resolve: resolverFor(silo, placement)
    });
}

const resolvers = new WeakMap<Silo, (symbol: string) => unknown | Promise<unknown>>();

function resolverFor(
    silo: Silo,
    placement: ClusterPlacement
): (symbol: string) => unknown | Promise<unknown> {
    let resolver = resolvers.get(silo);
    if (!resolver) {
        resolver = createSiloResolver(silo, placement);
        resolvers.set(silo, resolver);
    }
    return resolver;
}

/** symbol (`Cart#addItem`) → synthesized wrapper, memoized per method. */
export function createSiloResolver(
    silo: Silo,
    placement: ClusterPlacement
): (symbol: string) => unknown | Promise<unknown> {
    const cache = new Map<string, unknown>();
    return (symbol: string) => {
        const hit = cache.get(symbol);
        if (hit !== undefined) return hit;
        const hash = symbol.lastIndexOf('#');
        if (hash <= 0 || hash === symbol.length - 1) return null;
        const type = symbol.slice(0, hash);
        const method = symbol.slice(hash + 1);
        const def = silo.definition(type);
        if (!def) return null;
        if (isPromise(def)) {
            return def.then((resolved) => {
                if (!resolved) return null;
                const wrapped = synthesize(placement, resolved, symbol, method);
                cache.set(symbol, wrapped);
                return wrapped;
            });
        }
        const wrapped = synthesize(placement, def, symbol, method);
        cache.set(symbol, wrapped);
        return wrapped;
    };
}

function isPromise<T>(value: T | Promise<T>): value is Promise<T> {
    return typeof (value as { then?: unknown })?.then === 'function';
}

function synthesize(
    placement: ClusterPlacement,
    def: AnyActorDefinition,
    symbol: string,
    method: string
): unknown {
    const isStream = def.streamNames.includes(method);

    const prepare = (
        rq: ServerFnContext,
        args: unknown[]
    ): { key: string; rest: unknown[]; call: ActorCallContext } => {
        const [key, ...rest] = args;
        if (typeof key !== 'string' || key.length === 0) {
            throw new ServerFnError(
                400,
                `silo call "${symbol}" needs a non-empty string key as its first argument`
            );
        }
        const header = rq.request.headers.get(SILO_CALL_HEADER);
        if (!header) {
            throw new ServerFnError(
                400,
                `silo call "${symbol}" is missing the ${SILO_CALL_HEADER} envelope header`
            );
        }
        let decoded;
        try {
            decoded = decodeEnvelope(header);
        } catch (error) {
            throw new ServerFnError(400, (error as Error).message);
        }
        return {
            key,
            rest,
            call: { ...decoded.call, abortSignal: rq.abortSignal }
        };
    };

    if (isStream) {
        return {
            __sigxName: method,
            __sigxStream: true,
            __sigxFn: async (rq: ServerFnContext, _info: ServerFnInfo, args: unknown[]) => {
                const { key, rest, call } = prepare(rq, args);
                const iterable = placement.dispatchInboundStream(
                    { type: def.type, key },
                    method,
                    rest,
                    call
                );
                return (async function* pump(): AsyncGenerator<unknown> {
                    try {
                        yield* iterable;
                    } catch (error) {
                        throw toInternalWireError(error);
                    }
                })();
            }
        };
    }

    return {
        __sigxName: method,
        __sigxFn: async (rq: ServerFnContext, _info: ServerFnInfo, args: unknown[]) => {
            const { key, rest, call } = prepare(rq, args);
            try {
                return await placement.dispatchInbound({ type: def.type, key }, method, rest, call);
            } catch (error) {
                throw toInternalWireError(error);
            }
        }
    };
}

/**
 * Map actor errors for a TRUSTED peer: every kind travels with its `kind`
 * (and `owner` for wrong-host → 421) so the calling silo re-creates the
 * exact branded error — no prod masking between silos. The public endpoint
 * still masks whatever ultimately reaches a browser.
 */
function toInternalWireError(error: unknown): unknown {
    if (!isActorError(error)) return error;
    const status =
        error.kind === 'wrong-host'
            ? 421
            : error.kind === 'method-not-found'
              ? 404
              : error.kind === 'state-conflict'
                ? 409
                : error.kind === 'silo-shutdown' || error.kind === 'unreachable'
                  ? 503
                  : error.kind === 'call-timeout'
                    ? 504
                    : 500;
    const owner = (error as ActorWrongHostError).owner;
    return new ServerFnError(status, error.message, {
        kind: error.kind,
        ...(owner ? { owner } : {})
    });
}
