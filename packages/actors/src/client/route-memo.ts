/**
 * What a client remembers from a wrong-host redirect.
 *
 * Without this, `onMiss: 'redirect'` is a REGRESSION: two client round trips
 * per call versus one client trip plus one internal hop. The redirect only
 * pays for itself if the second call to a grain goes straight to its owner,
 * so the memo ships with the redirect rather than after it.
 *
 * Deliberately small and internal. It is the same idea as the cluster's own
 * `#routeCache` (insertion-ordered Map as a cheap LRU, bounded, dropped on
 * anything that proves it wrong), and `learningRouter()` lifts it out into
 * the public `ActorRouter` seam later.
 */

/** Matches the cluster's own ROUTE_CACHE_MAX — same job, same bound. */
const DEFAULT_MAX = 10_000;

export interface RouteMemo {
    get(id: string): string | undefined;
    learn(id: string, endpoint: string): void;
    forget(id: string): void;
    /** Drop every grain believed to live at `endpoint` — a silo went away,
     *  not just one grain. */
    forgetEndpoint(endpoint: string): void;
    readonly size: number;
}

export function routeMemo(max = DEFAULT_MAX): RouteMemo {
    // A garbage cap falls back to the default; a cap of 0 or less means
    // "remember nothing", which is the only reading of `cache: 0` that
    // matches what the option says it does.
    const cap = Number.isFinite(max) ? Math.floor(max) : DEFAULT_MAX;
    // Insertion-ordered, so the first key is the oldest — delete-then-set on
    // a hit refreshes its position and makes this an LRU for free.
    const routes = new Map<string, string>();
    return {
        get: (id) => routes.get(id),
        learn(id, endpoint) {
            if (cap <= 0) return;
            if (routes.delete(id) === false && routes.size >= cap) {
                const oldest = routes.keys().next().value;
                if (oldest !== undefined) routes.delete(oldest);
            }
            routes.set(id, endpoint);
        },
        forget: (id) => void routes.delete(id),
        forgetEndpoint(endpoint) {
            for (const [id, at] of routes) if (at === endpoint) routes.delete(id);
        },
        get size() {
            return routes.size;
        }
    };
}
