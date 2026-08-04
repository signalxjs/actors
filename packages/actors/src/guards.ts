/**
 * The actor half of core's server pipeline (rfc-server-v4 §7) — shared by
 * the wire resolver (`./server`), the live endpoint, and the in-process
 * `actor()` entry, so every transport runs the SAME middleware,
 * authentication and authorization.
 *
 * The split core made (#607/#610) lands here as three facts:
 *
 *  - **Middleware and authentication are the app's**, configured once on
 *    `createServerApp` and reached through core's fail-closed seam. Actors
 *    configures nothing and duplicates nothing; a miss denies.
 *  - **Authorization is per ENTRY POINT**, not per call. The entry points
 *    are the wire endpoint, the live endpoint, an in-process `actor()`
 *    call, and a job enqueue. A `ctx.actor` hop is NOT an entry point:
 *    authentication already resolved the caller once at the system
 *    boundary, and re-deciding mid-chain would authorize the ACTOR rather
 *    than the request. Actor-to-actor calls stay trusted, and now say why.
 *  - **The policy sees the instance.** `op.resource` carries
 *    `{ kind, type, key, method }`, so "may this user read cart `u_123`?"
 *    is expressible — the thing the pre-v4 guard chain structurally could
 *    not do, since the key was peeled off only after the guard had run.
 *
 * Everything here runs OUTSIDE any turn: a slow policy never occupies the
 * actor's turn.
 */
import {
    principal,
    serverFeature,
    type ServerFnContext,
    type ServerFnInfo,
    type ServerPolicy
} from '@sigx/server';
import type { AnyActorDefinition } from './types';

/** One pipeline object, reused: every member resolves the app per call. */
const feature = serverFeature();

/** What kind of thing a policy is deciding about (`op.resource.kind`). */
export type ActorResourceKind = 'actor' | 'worker' | 'job';

/**
 * Identity of one actor operation. `symbol` stays `Type#method` on BOTH
 * transports — honest now that core's `symbol` is pure identity and no
 * longer doubles as the transport discriminator (rfc-server-v4 §1.1). The
 * discriminator is `transport`, so actor middleware (a rate limiter) can
 * finally tell a browser call from an SSR render without losing per-method
 * identity.
 */
export function actorInfo(
    def: AnyActorDefinition,
    method: string,
    transport: 'wire' | 'in-process'
): ServerFnInfo {
    return { symbol: `${def.type}#${method}`, name: method, transport };
}

/** The declared chain for one method: actor-level, then per-method. */
function policiesFor(
    def: AnyActorDefinition,
    method: string
): readonly ServerPolicy[] | undefined {
    const opts = def.__sigxActor;
    const actorChain = opts.authorize;
    // OWN keys only: `methodAuthorize?.[method]` would resolve
    // `Object.prototype` members, so a prototype-named method 500'd before
    // dispatch could answer its honest 404.
    const methodChain =
        opts.methodAuthorize && Object.hasOwn(opts.methodAuthorize, method)
            ? opts.methodAuthorize[method]
            : undefined;
    const flat = [
        ...(actorChain ? (Array.isArray(actorChain) ? actorChain : [actorChain]) : []),
        ...(methodChain ? (Array.isArray(methodChain) ? methodChain : [methodChain]) : [])
    ] as ServerPolicy[];
    // `undefined` (not `[]`) where nothing is declared: core reads absence
    // as "use the app default", where an empty array would vacuously ALLOW.
    return flat.length > 0 ? flat : undefined;
}

/** `defineWorker`/`defineJob` stamp `kind`; everything else is an actor. */
function kindOf(def: AnyActorDefinition): ActorResourceKind {
    return def.__sigxActor.kind ?? 'actor';
}

/** Does this definition decide its own access, either way? */
export function hasAuthorization(def: AnyActorDefinition): boolean {
    const opts = def.__sigxActor;
    if (opts.allowAnonymous === true) return true;
    const declared = opts.authorize;
    // An empty array is NOT a declaration — it would vacuously allow, which
    // is the stricter reading actors has always taken (`extract.ts`).
    return declared ? (Array.isArray(declared) ? declared.length > 0 : true) : false;
}

/**
 * Is a server app stamped in this process? A read-only peek at core's
 * documented seam (`docs/seams.md` → `__SIGX_SERVER_APP__`).
 *
 * Used ONLY to decide whether the registration warning would tell the
 * developer anything — never for an access decision, which is core's job
 * and stays fail-closed regardless of what this returns.
 */
export function serverAppConfigured(): boolean {
    return (
        (globalThis as { __SIGX_SERVER_APP__?: unknown }).__SIGX_SERVER_APP__ !== undefined
    );
}

/**
 * The full pipeline for one entry point: app middleware → authenticate →
 * identity gate → authorization, with the instance as the resource.
 *
 * `transport` is the caller's own: `'wire'` at the endpoints, `'in-process'`
 * for `actor()`. On the wire the prelude has ALREADY run inside core's
 * `handleServerFnRequest` (it owns the pre-decode slot, so anonymous bytes
 * never reach the codec), which is why `skipPrelude` exists — running it
 * twice would run app middleware twice for one call.
 */
export async function authorizeActorCall(
    def: AnyActorDefinition,
    method: string,
    key: string,
    rq: ServerFnContext,
    transport: 'wire' | 'in-process',
    options?: { skipPrelude?: boolean }
): Promise<void> {
    const info = actorInfo(def, method, transport);
    const allowAnonymous = def.__sigxActor.allowAnonymous === true;
    if (options?.skipPrelude !== true) {
        await feature.prelude(rq, info, { allowAnonymous });
    }
    await feature.authorize(rq, {
        fn: info,
        policies: policiesFor(def, method),
        allowAnonymous,
        resource: { kind: kindOf(def), type: def.type, key, method }
    });
}

/**
 * Encode the request's principal for the call envelope, so identity
 * survives `ctx.actor` hops and host-to-host forwarding.
 *
 * `undefined` when the caller is anonymous or the app configured no
 * `codec` — the reader treats absence as anonymous, never as trusted, so
 * an unconfigured codec propagates nothing rather than propagating
 * something unverifiable.
 */
export async function encodePrincipal(rq: ServerFnContext): Promise<string | undefined> {
    const codec = feature.principalCodec;
    if (!codec) {
        warnMissingCodecOnce();
        return undefined;
    }
    const p = await principal(rq);
    if (p === null || p === undefined) return undefined;
    try {
        return codec.encode(p);
    } catch {
        // An un-encodable principal propagates nothing rather than failing
        // the call: the hop degrades to anonymous, and the callee's own
        // policies refuse it if they need an identity.
        return undefined;
    }
}

/**
 * Decode a principal off the envelope. Never throws: a malformed slot is
 * anonymous, the same posture the bag takes on a malformed entry.
 */
export function decodePrincipal(encoded: string | undefined): unknown {
    if (encoded === undefined) return null;
    const codec = feature.principalCodec;
    if (!codec) return null;
    try {
        return codec.decode(encoded) ?? null;
    } catch {
        return null;
    }
}

let warnedMissingCodec = false;
function warnMissingCodecOnce(): void {
    if (!__DEV__ || warnedMissingCodec) return;
    warnedMissingCodec = true;
    console.warn(
        '[sigx actors] no principal `codec` is configured on the server app, so the ' +
            'authenticated identity does not propagate across actor hops or between hosts — ' +
            '`ctx.principal` reads null inside every actor. Add `codec` to ' +
            'createServerApp({ … }) (rfc-server-v4 §3.1). Fires once per process.'
    );
}
