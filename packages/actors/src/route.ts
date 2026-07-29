/**
 * The routing token — grain identity where an intermediary can see it.
 *
 * The wire puts the actor KEY in the JSON body:
 *
 *     POST /_sigx/actor/Cart%23addItem     {"args": ["cart-123", …]}
 *
 * No load balancer parses a body to route, so the edge cannot tell which
 * grain a request is for, and locality is a coin flip that degrades as 1/N.
 * This module exposes a stable per-grain token in the request line so an
 * off-the-shelf LB can hash it:
 *
 *     POST /_sigx/actor/r/{token}/Cart%23addItem
 *     x-sigx-actor-route: {token}
 *
 * Composed with `preferLocalPolicy()`, the LB BECOMES the placement: the
 * first request for a key lands on silo X and activates there, and every
 * later request for that key hashes to X again. The LB and the cluster then
 * need to agree on nothing but stability — not on an algorithm, not on a
 * membership view.
 *
 * BOTH carriers, always from this one function so they cannot disagree.
 * Neither dominates: a path segment is loud (a mangled path 404s, whereas a
 * stripped header degrades to zero locality in silence) and serves HAProxy
 * `balance uri depth 4` and nginx with no custom filter — but Envoy's
 * `route.hash_policy` has no path-substring option at all, so a path-only
 * token taxes every Envoy/Istio deployment with a Lua filter.
 *
 *     nginx    map $uri $r { ~^/_sigx/actor/r/([^/]+)/ $1; }  hash $r consistent;
 *              (or simply: hash $http_x_sigx_actor_route consistent;)
 *     HAProxy  balance uri depth 4
 *     Envoy    hash_policy: { header: { header_name: x-sigx-actor-route } }
 *
 * The token is a MIDDLE segment, and must be: `@sigx/server` decodes the
 * symbol as the LAST path segment, so a token appended after it would BE the
 * symbol. Conversely `matchesActorRequest` is a prefix match, so a middle
 * segment is already transparent — the server needs no parsing change and
 * deliberately IGNORES the token entirely (see `actorRouteToken`).
 *
 * Routing is an optimization and never load-bearing for correctness. A
 * misrouted request still produces the right answer — the receiving silo
 * forwards, and the directory remains the sole arbiter of single-activation.
 * Everything here is a hint that is allowed to be stale, wrong, or absent.
 */
import { fnv1a } from './hash';
import { actorId } from './types';

/** The marker segment introducing a routing token: `…/r/{token}/{symbol}`.
 *  A fixed literal so every LB regex has something to anchor on, and so a
 *  second prefix can be added later without another wire change. */
export const ACTOR_ROUTE_SEGMENT = 'r';

/** The routing token, mirrored out of the path for hash policies that can
 *  only read headers (Envoy, Istio). Always equal to the path segment. */
export const ACTOR_ROUTE_HEADER = 'x-sigx-actor-route';

/**
 * How the client derives a grain's routing token.
 *
 * - `'hash'` (default) — an opaque hash of the actor id.
 * - `'key'` — the raw actor key, for human-readable routing and debugging.
 * - `'none'` — emit no token; the URL stays as it was.
 * - a function — bring your own scheme (tenant affinity, an existing
 *   sharding key). Returning `null` or `''` emits no token for that grain.
 *
 * `'hash'` is the default because actor keys are frequently user ids or
 * emails, and a raw key in the path lands in every access log, proxy trace
 * and referrer header. Note this is LOG HYGIENE, not privacy: an unkeyed
 * hash of an email is one dictionary lookup from plaintext at any width.
 * Real unlinkability needs a keyed HMAC with a secret the client holds,
 * which a browser cannot hold.
 *
 * Because the composition only needs STABILITY and not agreement, a hash
 * routes exactly as well as the key.
 */
export type ActorRouteMode = 'hash' | 'key' | 'none';

export type ActorRouteToken =
    | ActorRouteMode
    | ((ref: { type: string; key: string }) => string | null);

/**
 * The default token: an opaque, fixed-width hash of the actor id.
 *
 * 32 bits is deliberate. Collisions do not hurt correctness — two colliding
 * grains merely co-locate — and the only thing more bits could buy is
 * pre-image resistance, which an unkeyed hash does not provide at any width.
 *
 * Hashes `actorId()` itself rather than re-deriving the NUL-separated form,
 * so the two cannot drift even in principle. Changing this format costs a
 * one-deploy locality dip as tokens re-hash and grains re-home, and nothing
 * else — unlike `reminderShardOf`, which shares the hash but not the pin.
 */
export function hashRouteToken(type: string, key: string): string {
    return fnv1a(actorId({ type, key })).toString(36).padStart(7, '0');
}

/**
 * The token for one grain under `route`, or `null` for "emit none".
 *
 * `$`-prefixed types never get a token. `$live#subscribe` is one held-open
 * response fanning out to MANY grains, so there is no single token and no
 * single owner for it; sharding that connection would defeat the reason it
 * exists. `defineActor` refuses `$`-prefixed types, so this can never
 * collide with a real actor and it covers every future reserved mount.
 */
export function routeTokenFor(
    route: ActorRouteToken,
    type: string,
    key: string
): string | null {
    if (route === 'none' || type.startsWith('$')) return null;
    if (route === 'hash') return hashRouteToken(type, key);
    if (route === 'key') return key === '' ? null : key;
    return route({ type, key }) || null;
}

/**
 * The token's WIRE form — what both carriers transmit, byte for byte.
 *
 * Percent-encoded unconditionally, including a custom function's result, for
 * two independent reasons:
 *
 * 1. In the path, a raw `/` would smuggle in an extra segment and shift
 *    which one the server reads as the symbol.
 * 2. In the header, a raw non-ASCII byte is not a valid field value and a
 *    raw newline would be header injection — `encodeURIComponent` output is
 *    always safe ASCII.
 *
 * Both carriers MUST use this one function. An LB hashes the bytes it sees,
 * so a path carrying `tenant%2Fa` beside a header carrying `tenant/a` would
 * hash to two different silos — the carriers would disagree exactly when the
 * key is interesting.
 */
export function encodeRouteToken(token: string): string {
    return encodeURIComponent(token);
}

/**
 * `${base}/r/${token}/${symbol}` — or `${base}/${symbol}` when there is no
 * token, which is exactly the URL that shipped before this existed.
 */
export function routePath(base: string, token: string | null, symbol: string): string {
    const trimmed = base.endsWith('/') ? base.slice(0, -1) : base;
    const encoded = encodeURIComponent(symbol);
    return token === null
        ? `${trimmed}/${encoded}`
        : `${trimmed}/${ACTOR_ROUTE_SEGMENT}/${encodeRouteToken(token)}/${encoded}`;
}

/**
 * The routing token an inbound request carries, or `null`.
 *
 * READ-ONLY, and the endpoint does not call it: the server ignores the token
 * completely. Validating it would make a hint load-bearing, contradicting
 * the invariant this whole mechanism rests on; it is also impossible in
 * general, since `'key'` mode and custom functions produce tokens the server
 * cannot recompute. Exposed for adapters that want to log or shard on it,
 * and for tests.
 *
 * Prefers the path segment over the header — the path is the carrier a proxy
 * cannot silently strip — and falls back to the header for edges that route
 * on it and rewrite the path.
 *
 * BOTH carriers are decoded, because both are encoded on the way out. An
 * adapter logging or sharding on this must see one form regardless of which
 * carrier survived the trip.
 */
export function actorRouteToken(request: Request, base = '/_sigx/actor'): string | null {
    const prefix = base.endsWith('/') ? base : `${base}/`;
    const { pathname } = new URL(request.url);
    if (pathname.startsWith(`${prefix}${ACTOR_ROUTE_SEGMENT}/`)) {
        const rest = pathname.slice(prefix.length + ACTOR_ROUTE_SEGMENT.length + 1);
        const slash = rest.indexOf('/');
        // No trailing segment means no symbol follows — not a tokenized URL.
        if (slash > 0) return decodeToken(rest.slice(0, slash));
    }
    const header = request.headers.get(ACTOR_ROUTE_HEADER);
    return header === null ? null : decodeToken(header);
}

/** A malformed token is ABSENT, never fatal — it is only ever a hint. */
function decodeToken(raw: string): string | null {
    try {
        return decodeURIComponent(raw);
    } catch {
        return null;
    }
}
