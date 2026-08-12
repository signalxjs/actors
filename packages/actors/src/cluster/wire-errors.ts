/**
 * The error contract across a host hop, in one place because "the error is
 * the same error" is a conformance requirement rather than a nicety: a
 * caller must not be able to tell a remote hop from a local dispatch, so a
 * remote `state-conflict` has to satisfy `isActorError` and carry its
 * `kind` exactly like a local one.
 *
 * Both directions live here so a transport in another package gets them
 * from `HostTransportConfig` instead of re-deriving them — the kind list
 * and the status mapping are versioned, and a transport that invents its
 * own drifts silently.
 */
import {
    isActorError,
    type ActorErrorKind,
    type ActorOwnerHint,
    type ActorWrongHostError
} from '../errors';
import { encodeWire, reviveWire, reviver, wireFail } from '../wire-shared';
import type { HostWireCodec, HostWireError } from './seam';

/**
 * Actor kinds that travel with their brand. A kind NOT in this set comes
 * back as an ordinary serverFn error — the safe direction, since inventing
 * an actor brand for something the runtime never threw would let a peer
 * fake a `wrong-host` redirect.
 */
const ACTOR_ERROR_KINDS = new Set<string>([
    'deadlock',
    'activation',
    'state-conflict',
    'method-not-found',
    'host-shutdown',
    'call-timeout',
    'wrong-host',
    'unreachable',
    'unplaceable',
    'watch-declaration'
]);

/**
 * Map an actor error for a TRUSTED peer: every kind travels with its `kind`
 * (and `owner` for wrong-host) so the calling host re-creates the exact
 * branded error — no prod masking between hosts. The public endpoint still
 * masks whatever ultimately reaches a browser.
 *
 * The status is HTTP-shaped even off HTTP, because `clusterStats`
 * classifies peer failures by it (403 → `unauthorized`, 404 →
 * `unsupported`); a transport that drops the number degrades every auth
 * failure to a bare `'error'` in the ops surface.
 */
export function toHostWireError(error: unknown): HostWireError {
    if (!isActorError(error)) {
        const status = (error as { status?: number }).status;
        return {
            message: (error as Error)?.message ?? String(error),
            ...(typeof status === 'number' ? { status } : {})
        };
    }
    const status =
        error.kind === 'wrong-host'
            ? 421
            : error.kind === 'method-not-found'
              ? 404
              : error.kind === 'state-conflict'
                ? 409
                : error.kind === 'host-shutdown' ||
                    error.kind === 'unreachable' ||
                    error.kind === 'unplaceable'
                  ? 503
                  : error.kind === 'call-timeout'
                    ? 504
                    : 500;
    const owner = (error as ActorWrongHostError).owner;
    return {
        message: error.message,
        status,
        data: { kind: error.kind, ...(owner ? { owner } : {}) }
    };
}

/**
 * Re-create a peer's error. Actor-branded kinds come back as actor errors
 * (a remote state-conflict must satisfy `isActorError` exactly like a local
 * one); anything else keeps the serverFn wire brand.
 */
export function fromHostWireError(
    status: number,
    wire: HostWireError | undefined,
    fallback: string
): Error {
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

/**
 * THE codec the public wire uses — never a bare `JSON.stringify`. Exported
 * alongside the two mappers above because that trio is exactly what a
 * transport package needs to build a `HostTransportConfig` by hand in its
 * own tests.
 */
export const hostWireCodec: HostWireCodec = {
    encode: encodeWire,
    decode: reviveWire,
    reviver
};
