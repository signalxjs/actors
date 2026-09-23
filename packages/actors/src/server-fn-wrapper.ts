/**
 * The one shape core's server-fn endpoint dispatches to (rfc-server-v5,
 * signalxjs/core#692): an object carrying a frozen `__sigx` descriptor.
 *
 * Both actor mounts — the public one (`./server`) and the HMAC-authenticated
 * host-to-host one (`./cluster`) — answer `handleServerFnRequest`'s
 * `resolve(key)` with a wrapper SYNTHESIZED per method, never with a
 * `serverFn`. Core ships no builder for the descriptor (its own `serverFn`
 * and `serverStream` freeze theirs inline), so this is that builder, in one
 * place, so the two mounts cannot drift on what the endpoint reads:
 *
 *  - `kind` — `'stream'` pumps the returned async iterator as NDJSON;
 *  - `anon` — the identity gate, which runs BEFORE the args are decoded, so
 *    it lives here rather than in `invoke`;
 *  - `read` — the GET opt-in and its `Cache-Control`, for `reads:` methods.
 *
 * `form` is always false (an actor call is never a form post) and
 * `invalidates` is never set (actor reads are staled by `useActorAction`,
 * not by the serverFn invalidation channel).
 *
 * `invoke` takes the args ARRAY as decoded off the wire. Core's own
 * `serverFn` enforces one input; the endpoint does not, which is what lets
 * the actor wire keep its `[key, ...args]` layout.
 */
import type { ServerFnContext, ServerFnInfo, WrappedServerFn } from '@sigx/server';

export interface ServerFnWrapperOptions {
    readonly kind: 'fn' | 'stream';
    readonly invoke: (rq: ServerFnContext, info: ServerFnInfo, args: unknown[]) => Promise<unknown>;
    readonly anon?: boolean;
    /** Present only for a `reads:` method — makes the wrapper a GET target. */
    readonly cacheControl?: string;
}

export function serverFnWrapper(options: ServerFnWrapperOptions): WrappedServerFn {
    return {
        __sigx: Object.freeze({
            kind: options.kind,
            invoke: options.invoke,
            anon: options.anon === true,
            form: false,
            ...(options.cacheControl !== undefined
                ? { read: Object.freeze({ cacheControl: options.cacheControl }) }
                : {})
        })
    };
}
