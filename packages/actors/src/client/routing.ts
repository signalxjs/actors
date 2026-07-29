/**
 * `ActorRouter` — where a CLIENT decides which endpoint to call.
 *
 * Sits BESIDE `ActorTransport` rather than inside it, so routing and
 * transport compose instead of multiplying: a WebSocket transport and a
 * learning router are one wrapper apart, not a new class each.
 *
 * The invariant everything here rests on: **routing is an optimization and
 * is never load-bearing for correctness.** A router that answers wrongly,
 * stalely, or not at all costs a network hop — never a wrong answer,
 * because the silo that receives a misrouted call still forwards or
 * redirects it, and the directory remains the sole arbiter of who owns a
 * grain. That is what makes it safe to let arbitrary user code choose an
 * endpoint on the hot path, and it is why `resolve()` throwing is caught
 * and ignored rather than propagated.
 */
import { ACTOR_FOLLOW_HEADER, ACTOR_HOPS_HEADER } from '../route';
import type { ActorRef } from '../types';
import { actorId } from '../types';
import { fetchTransport } from './index';
import type { ActorCallInit, ActorTransport, ActorTransportConfig } from './index';

/** What a router is told about the call it is routing, beyond the grain. */
export interface ActorRouteContext {
    /** `Cart#addItem`. */
    readonly symbol: string;
    readonly method: string;
    /** Long-lived streams may deserve a different endpoint than unary calls. */
    readonly mode: 'call' | 'stream';
    /**
     * Best-effort hint at the endpoint that will be used if `resolve`
     * answers `null`.
     *
     * Derived from the call's own init, so it is not authoritative: a
     * transport with its own configured endpoint (`fetchTransport({ endpoint })`)
     * outranks the build-baked one, and this cannot see that. Treat it as
     * "roughly where this would go otherwise", not as a promise.
     */
    readonly endpoint: string;
}

export interface ActorRouterStats {
    /** Grains currently mapped to an endpoint. */
    size: number;
    /** `learn()` calls since construction — events, NOT distinct endpoints;
     *  re-learning one grain counts again. */
    learned: number;
}

export interface ActorRouter {
    /** Diagnostic label, e.g. `'learning'`. */
    readonly name: string;
    /**
     * The endpoint for this grain, or `null` for "no opinion — use the
     * default". Sync: this runs on every call, and a router that needs to
     * await something should answer from cache and fill in the background
     * rather than put a round trip in front of every dispatch.
     */
    resolve(ref: ActorRef, ctx: ActorRouteContext): string | null;
    /** The cluster said this grain lives there — a 421 taught us. */
    learn?(ref: ActorRef, endpoint: string): void;
    /**
     * Drop what is now wrong: entries pointing at `endpoint`, or everything
     * when it is omitted.
     *
     * The argument matters. Blanket invalidation throws away a whole cache
     * because one silo changed; the cluster's own route cache only prunes
     * entries naming the silo that left, and this is that idea.
     */
    invalidate?(endpoint?: string): void;
    stats?(): ActorRouterStats;
    /** Release anything held — a membership subscription, a timer. */
    close?(): void;
}

export interface ActorRoutingStats {
    /** Calls where the router had an opinion. */
    routed: number;
    /** Redirects followed. Should trend to ~0 as a learning router warms. */
    redirects: number;
    /** Times a learned endpoint was dropped for proving wrong. */
    invalidations: number;
    /** Router callbacks that threw — swallowed, but not hidden. */
    routerErrors: number;
}

export interface RoutedTransport extends ActorTransport {
    readonly router: ActorRouter;
    stats(): ActorRoutingStats;
}

// ---------------------------------------------------------------------------
// Reading a redirect

/** Where a `421` said the grain actually lives. */
export interface ActorRouteRedirect {
    /** A CALLABLE actor endpoint base, e.g. `https://silo-7.example.com/_sigx/actor`. */
    readonly endpoint: string;
    readonly siloId?: string;
}

/**
 * The redirect an error carries, or `null` for every other failure.
 *
 * Reads the shape the public endpoint sends under `onMiss: 'redirect'`;
 * `wireFail` has already revived `error.data` by the time this sees it.
 */
export function actorRedirect(error: unknown): ActorRouteRedirect | null {
    const data = (error as { data?: unknown })?.data as
        | { kind?: string; owner?: { endpoint?: unknown; siloId?: unknown } }
        | undefined;
    if (data?.kind !== 'wrong-host') return null;
    const endpoint = data.owner?.endpoint;
    if (typeof endpoint !== 'string' || endpoint === '') return null;
    const siloId = data.owner?.siloId;
    return { endpoint, ...(typeof siloId === 'string' ? { siloId } : {}) };
}

/**
 * Is this failure a reason to STOP believing a learned endpoint?
 *
 * A transport error with no status is a dial that never landed; a 5xx is a
 * silo that answered but is unwell. Both mean "the place we were told about
 * may be gone", and both are worth one retry against the default. A 4xx is
 * not: that is the application answering, and re-sending it elsewhere would
 * turn a clean error into a confusing one.
 */
function isStaleRouteFailure(error: unknown): boolean {
    const status = (error as { status?: unknown })?.status;
    if (status === undefined) return true;
    return typeof status === 'number' && status >= 500;
}

// ---------------------------------------------------------------------------
// The shared cache

/** Matches the cluster's own ROUTE_CACHE_MAX — same job, same bound. */
const DEFAULT_MAX_ENTRIES = 10_000;

interface RouteCache {
    get(id: string): string | undefined;
    set(id: string, endpoint: string): void;
    prune(endpoint?: string): void;
    readonly size: number;
    readonly learned: number;
}

/**
 * A bounded, insertion-ordered `Map`: the first key is the oldest WRITE, and
 * re-learning a grain moves it to the back. Reads do NOT refresh position,
 * so this is FIFO-by-write rather than a true LRU — the same behaviour as
 * the cluster's own `#routeCache`, and deliberately so: two implementations
 * of one idea would drift, and eviction here costs a redirect, not an error.
 */
function routeCache(max = DEFAULT_MAX_ENTRIES): RouteCache {
    const cap = Number.isFinite(max) ? Math.floor(max) : DEFAULT_MAX_ENTRIES;
    const routes = new Map<string, string>();
    let learned = 0;
    return {
        get: (id) => routes.get(id),
        set(id, endpoint) {
            if (cap <= 0) return; // `maxEntries: 0` means remember nothing
            learned++;
            if (routes.delete(id) === false && routes.size >= cap) {
                const oldest = routes.keys().next().value;
                if (oldest !== undefined) routes.delete(oldest);
            }
            routes.set(id, endpoint);
        },
        prune(endpoint) {
            if (endpoint === undefined) {
                routes.clear();
                return;
            }
            for (const [id, at] of routes) if (at === endpoint) routes.delete(id);
        },
        get size() {
            return routes.size;
        },
        get learned() {
            return learned;
        }
    };
}

// ---------------------------------------------------------------------------
// The shipped routers

/**
 * Always the same endpoint, or — with no argument — never an opinion.
 *
 * Note this is NOT "the default router": no router at all is the default,
 * because installing one costs bytes and an indirection for the majority of
 * apps that talk to a single origin. This exists to pin every actor at one
 * endpoint over the build-time one, and as the null object in tests.
 */
export function staticRouter(endpoint?: string): ActorRouter {
    return {
        name: 'static',
        resolve: () => endpoint ?? null
    };
}

export interface LearningRouterOptions {
    /** Grain→endpoint pairs to remember. Default 10 000; 0 disables. */
    maxEntries?: number;
}

/**
 * Starts with no opinion and gets better as it runs: every `421` teaches it
 * where one grain lives, and it goes straight there next time.
 *
 * The shipped non-trivial default, because it needs nothing — no membership
 * feed, no configuration, no ability to reach silos directly, no agreement
 * with the cluster's placement. It only needs the server to be answering
 * redirects (`onMiss: 'redirect'` or `'auto'`).
 *
 * No TTL and no negative caching, deliberately: staleness is self-correcting
 * — a wrong entry produces one redirect, which replaces it. And no
 * persistence, because a route that survives a deploy that moved every silo
 * is worse than no route, while the win it would buy is one round trip per
 * grain per session.
 */
export function learningRouter(options?: LearningRouterOptions): ActorRouter {
    const cache = routeCache(options?.maxEntries);
    return {
        name: 'learning',
        resolve: (ref) => cache.get(actorId(ref)) ?? null,
        learn: (ref, endpoint) => cache.set(actorId(ref), endpoint),
        invalidate: (endpoint) => cache.prune(endpoint),
        stats: () => ({ size: cache.size, learned: cache.learned })
    };
}

/**
 * First non-null answer wins; `learn`, `invalidate` and `close` reach all of
 * them.
 *
 * The composition that makes a precise-but-cold router usable: put a
 * directory-backed one in front of a learning one and the pair covers both
 * "someone else already activated this" and "I was told where it went".
 */
export function chainRouters(...routers: readonly ActorRouter[]): ActorRouter {
    return {
        name: `chain(${routers.map((r) => r.name).join(',')})`,
        resolve(ref, ctx) {
            for (const router of routers) {
                const answer = router.resolve(ref, ctx);
                if (answer !== null) return answer;
            }
            return null;
        },
        learn(ref, endpoint) {
            for (const router of routers) router.learn?.(ref, endpoint);
        },
        invalidate(endpoint) {
            for (const router of routers) router.invalidate?.(endpoint);
        },
        stats: () =>
            routers.reduce<ActorRouterStats>(
                (total, r) => {
                    const s = r.stats?.();
                    return s
                        ? { size: total.size + s.size, learned: total.learned + s.learned }
                        : total;
                },
                { size: 0, learned: 0 }
            ),
        close() {
            for (const router of routers) router.close?.();
        }
    };
}

// ---------------------------------------------------------------------------
// The wrapper

const DEFAULT_MAX_REDIRECTS = 2;

/**
 * Wrap a transport so a router chooses each call's endpoint, and a `421`
 * teaches that router where the grain really lives.
 *
 * The retry loop lives here rather than in `fetchTransport` so a custom
 * transport gets the same behaviour for free — which is the point of having
 * two seams instead of one.
 */
export function routedTransport(
    inner: ActorTransport,
    router: ActorRouter,
    options?: { maxRedirects?: number }
): RoutedTransport {
    const maxRedirects = options?.maxRedirects ?? DEFAULT_MAX_REDIRECTS;
    const stats: ActorRoutingStats = {
        routed: 0,
        redirects: 0,
        invalidations: 0,
        routerErrors: 0
    };
    let warned = false;

    /**
     * Every router callback is fire-and-forget. `resolve` is the obvious one,
     * but `learn` and `invalidate` matter just as much: a cache that throws
     * while REMEMBERING an answer must not destroy the answer, which the
     * caller is about to receive successfully.
     */
    const quietly = (what: string, run: () => void): void => {
        try {
            run();
        } catch (error) {
            stats.routerErrors++;
            if (__DEV__ && !warned) {
                warned = true;
                console.warn(
                    `[sigx actors] router "${router.name}" threw from ${what}(); ` +
                        `ignoring it. Routing is an optimization and never ` +
                        `load-bearing, so the call continues.`,
                    error
                );
            }
        }
    };

    /** A router must never be able to fail a call. */
    const safeResolve = (ref: ActorRef, ctx: ActorRouteContext): string | null => {
        try {
            const answer = router.resolve(ref, ctx);
            if (answer === null || answer === undefined) return null;
            // VALIDATE, don't trust. A router is user code and may be
            // JavaScript, where `''` or a non-string sails past the types —
            // and an empty base would turn the URL into `/{symbol}`, a
            // relative path resolved against whatever origin the page
            // happens to be on. Silently calling the wrong host is a far
            // worse failure than not routing, and "a router cannot fail a
            // call" has to mean this too.
            if (typeof answer !== 'string' || answer === '') {
                stats.routerErrors++;
                if (__DEV__ && !warned) {
                    warned = true;
                    console.warn(
                        `[sigx actors] router "${router.name}" resolved to ` +
                            `${JSON.stringify(answer)}, which is not a usable endpoint; ` +
                            `using the default instead.`
                    );
                }
                return null;
            }
            stats.routed++;
            return answer;
        } catch (error) {
            stats.routerErrors++;
            if (__DEV__ && !warned) {
                warned = true;
                console.warn(
                    `[sigx actors] router "${router.name}" threw from resolve(); ` +
                        `falling back to the default endpoint. Routing is an ` +
                        `optimization and never load-bearing, so the call continues.`,
                    error
                );
            }
            return null;
        }
    };

    /**
     * The init for one attempt: the chosen route, plus the two headers that
     * make the SERVER a participant.
     *
     * `x-sigx-actor-follow` is what an `onMiss: 'auto'` mount keys off, so
     * it redirects exactly the callers that can act on it. `x-sigx-actor-hops`
     * lets the server bound the chain too — a redirect loop has to be
     * impossible, not merely unlikely, and a client that ignores its own cap
     * (or is not ours) must not be able to bounce between two silos that
     * disagree about ownership.
     */
    const initFor = (
        init: ActorCallInit | undefined,
        route: string | null,
        attempt: number
    ): ActorCallInit => ({
        ...init,
        ...(route === null ? {} : { route }),
        headers: {
            ...init?.headers,
            [ACTOR_FOLLOW_HEADER]: '1',
            ...(attempt > 0 ? { [ACTOR_HOPS_HEADER]: String(attempt) } : {})
        }
    });

    const context = (
        symbol: string,
        mode: 'call' | 'stream',
        init: ActorCallInit | undefined
    ): ActorRouteContext => {
        const hash = symbol.lastIndexOf('#');
        return {
            symbol,
            method: hash > 0 ? symbol.slice(hash + 1) : symbol,
            mode,
            // A caller-supplied `route` survives when `resolve` answers
            // null, so it is the more accurate fallback of the two.
            endpoint: init?.route ?? init?.endpoint ?? ''
        };
    };

    /**
     * Decide what to do with a failure: follow a redirect, drop a route that
     * proved wrong, or give up. Returns the next endpoint to try, or throws.
     */
    const nextRoute = (
        error: unknown,
        ref: ActorRef,
        route: string | null,
        attempt: number
    ): string | null => {
        const hint = actorRedirect(error);
        if (hint !== null) {
            if (attempt >= maxRedirects) throw error;
            stats.redirects++;
            // `learn` ONLY — deliberately no invalidate(route) here. A 421
            // proves that THIS grain moved, not that the endpoint we tried
            // is gone, and `invalidate(endpoint)` prunes every grain mapped
            // there. Evicting them would throw away good routes on every
            // redirect and re-cost a round trip each. Endpoint-wide
            // invalidation is for the unreachable/5xx path below, which is
            // the case that really does implicate the whole silo.
            quietly('learn', () => router.learn?.(ref, hint.endpoint));
            return hint.endpoint;
        }
        // A route WE chose that is unreachable or unwell: forget it and fall
        // back to the default once. Only when we chose one — an error from
        // the default endpoint is the caller's to see.
        if (route !== null && attempt < maxRedirects && isStaleRouteFailure(error)) {
            stats.invalidations++;
            quietly('invalidate', () => router.invalidate?.(route));
            return null;
        }
        throw error;
    };

    return {
        name: `routed(${inner.name})`,
        router,
        stats: () => ({ ...stats }),

        async call(symbol, args, init) {
            const ref = init?.ref;
            // No single grain — `$live` multiplexes many onto one request,
            // and a hand-built call may carry no ref at all.
            if (!ref) return inner.call(symbol, args, init);

            let route = safeResolve(ref, context(symbol, 'call', init));
            for (let attempt = 0; ; attempt++) {
                try {
                    return await inner.call(symbol, args, initFor(init, route, attempt));
                } catch (error) {
                    route = nextRoute(error, ref, route, attempt);
                }
            }
        },

        stream(symbol, args, init) {
            const ref = init?.ref;
            if (!ref) return inner.stream(symbol, args, init);

            // The first value is pulled INSIDE the retry loop, so a redirect
            // surfaces as a re-route rather than as a failure halfway
            // through a stream the caller has already started reading. The
            // cluster's own routed streaming does the same for the same
            // reason.
            async function* routed(): AsyncGenerator<unknown> {
                let route = safeResolve(ref!, context(symbol, 'stream', init));
                for (let attempt = 0; ; attempt++) {
                    const iterator = inner
                        .stream(symbol, args, initFor(init, route, attempt))
                        [Symbol.asyncIterator]();
                    let first: IteratorResult<unknown>;
                    try {
                        first = await iterator.next();
                    } catch (error) {
                        // Release the generator we are abandoning before
                        // deciding whether to try elsewhere.
                        await iterator.return?.(undefined).catch(() => {});
                        route = nextRoute(error, ref!, route, attempt);
                        continue;
                    }
                    if (first.done === true) return;
                    yield first.value;
                    // Past the first value the endpoint is settled; a later
                    // failure is a stream failure, not a routing question.
                    for (;;) {
                        const step = await iterator.next();
                        if (step.done === true) return;
                        yield step.value;
                    }
                }
            }
            return routed();
        },

        ...(inner.live ? { live: () => inner.live!() } : {}),

        close() {
            quietly('close', () => router.close?.());
            return inner.close?.();
        }
    };
}

// ---------------------------------------------------------------------------
// Sugar

export interface RoutedFetchConfig extends ActorTransportConfig {
    /**
     * Follow `421` wrong-host redirects, remembering where each grain lives
     * so only the FIRST call to it pays the extra round trip.
     *
     * Shorthand for `router: learningRouter()`. Without the memo a redirect
     * costs TWO client round trips versus one plus an internal hop — worse
     * than letting the silo proxy — so following and remembering ship
     * together or not at all.
     *
     * Note the server must be mounted with `onMiss: 'redirect'` or `'auto'`
     * for anything to redirect in the first place. And note that a browser
     * behind one origin should NOT use this: the retry would be
     * cross-origin, so it preflights and is refused by the endpoint's
     * same-origin policy.
     */
    follow?: boolean | { maxHops?: number; cache?: number };
    /**
     * Choose each call's endpoint per grain.
     *
     * Replaces the router `follow` would have installed — but NOT the rest
     * of it: `follow.maxHops` still bounds the redirect chain, because that
     * is a property of the following, not of the router doing the
     * resolving. `follow.cache` is ignored, since it only configures the
     * `learningRouter` this displaces.
     */
    router?: ActorRouter;
}

/**
 * `fetchTransport` with a router in front — the one-liner for the common
 * case.
 *
 * Lives here rather than on `configureActors` so that routing stays
 * opt-in BY IMPORT: an app that never mentions it ships none of these bytes,
 * which matters because the client entry rides every bundle that touches an
 * actor, and a single-origin browser app cannot use redirects at all.
 *
 * ```ts
 * configureActors(routedFetchTransport({ endpoint, follow: true }));
 * ```
 */
export function routedFetchTransport(config: RoutedFetchConfig = {}): ActorTransport {
    const { router, follow, ...rest } = config;
    const inner = fetchTransport(rest);
    const chosen =
        router ??
        (follow === undefined || follow === false
            ? undefined
            : learningRouter(
                  typeof follow === 'object' && follow.cache !== undefined
                      ? { maxEntries: follow.cache }
                      : undefined
              ));
    if (chosen === undefined) return inner;
    const maxRedirects = typeof follow === 'object' ? follow.maxHops : undefined;
    return routedTransport(
        inner,
        chosen,
        maxRedirects === undefined ? undefined : { maxRedirects }
    );
}
