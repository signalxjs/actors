/**
 * `cluster()` — clustering as an app plugin.
 *
 * Hand-wired, a clustered silo repeats itself: `secret` goes to both
 * `clusterPlacement` and `handleSiloRequest`, `internalBase` has to agree
 * with `matchesSiloRequest`'s default, and the internal mount is left to the
 * entry to route. Nothing type-checks any of that agreement.
 *
 * As a plugin each value is declared ONCE and the internal mount travels
 * with it as a contributed route, so an adapter that mounts `app.routes`
 * gets silo-to-silo traffic for free. This is composition only — the
 * placement, endpoint and providers underneath are unchanged.
 */
import type { ActorPlugin, PluginRegistry } from '../silo/app';
import { clusterPlacement, type ClusterPlacement } from './placement';
import { handleSiloRequest, matchesSiloRequest, type SiloRequestOptions } from './silo-endpoint';
import type { ClusterProviders, PlacementPolicy } from './types';

export interface ClusterPluginOptions {
    /**
     * Membership + directory for THIS silo, from a provider package
     * (`redisCluster({ url })`, `memoryClusterHub().providers()`). A named
     * field rather than a spread, so the provider/config boundary stays
     * visible.
     */
    providers: ClusterProviders;
    /** Peer-reachable origin of this silo's HTTP listener. */
    advertise: string;
    /**
     * Shared cluster secret. Declared once here and used by BOTH the
     * outbound transport and the internal mount's HMAC verification.
     */
    secret?: string;
    /** Path prefix of the internal mount. Default `/_sigx/silo`. */
    internalBase?: string;
    /** Fetch implementation (tests pipe it straight into peers' handlers). */
    fetch?: typeof globalThis.fetch;
    /** Placement policy for NEW activations. Default: uniform random.
     *  A `defineActor({ placement })` declaration beats this. */
    policy?: PlacementPolicy;
    /** Per-actor-type policy overrides. */
    typePolicies?: Record<string, PlacementPolicy>;
    /** Wrong-host / unreachable re-resolve attempts. Default 3. */
    retries?: number;
    /** Linear backoff between UNREACHABLE retries, ms. Default 100. */
    retryBackoffMs?: number;
    /** Free-form placement hints published in the membership descriptor. */
    meta?: Record<string, string>;
    /** Forwarded to the internal mount (body caps, `onError`, `timeoutMs`). */
    endpoint?: Omit<SiloRequestOptions, 'silo' | 'placement' | 'secret'>;
}

export interface ClusterPlugin extends ActorPlugin {
    /**
     * This silo's placement — for the operational primitives that have no
     * plugin equivalent: `identity`, `descriptor()`, `migrate(ref)`.
     * Available immediately; the placement does not need the silo until
     * `bind()`.
     */
    readonly placement: ClusterPlacement;
}

export function cluster(options: ClusterPluginOptions): ClusterPlugin {
    const internalBase = options.internalBase ?? '/_sigx/silo';
    if (!internalBase.startsWith('/')) {
        // Silent otherwise, and doubly so: `matchesSiloRequest` compares
        // against a pathname (always leading-slash) so the route would never
        // match, AND peer dispatch URLs would be malformed.
        throw new Error(
            `[sigx actors] cluster({ internalBase }) must start with "/" — got ` +
                `"${internalBase}".`
        );
    }
    // Built eagerly: a placement defers everything that needs the silo to
    // `bind()`, so there is nothing to wait for — and exposing it right away
    // keeps `migrate()`/`identity` reachable without starting the app.
    const placement = clusterPlacement({
        ...options.providers,
        advertise: options.advertise,
        internalBase,
        ...(options.secret !== undefined ? { secret: options.secret } : {}),
        ...(options.fetch ? { fetch: options.fetch } : {}),
        ...(options.policy ? { policy: options.policy } : {}),
        ...(options.typePolicies ? { typePolicies: options.typePolicies } : {}),
        ...(options.retries !== undefined ? { retries: options.retries } : {}),
        ...(options.retryBackoffMs !== undefined
            ? { retryBackoffMs: options.retryBackoffMs }
            : {}),
        ...(options.meta ? { meta: options.meta } : {})
    });

    return {
        name: 'cluster',
        placement,
        setup(registry: PluginRegistry): void {
            registry.setPlacement(() => placement);
            // Readiness, so a `health()` endpoint drains this silo without
            // knowing clustering exists. `'leaving'` is the M4 handoff
            // window (announced BEFORE the drain, which is the whole point:
            // the load balancer stops sending while activations hand off).
            // `'fenced'` is the one that would otherwise be invisible —
            // `#fence()` leaves the PUBLISHED status at 'active' while
            // every activation is refused, so a fenced silo is a black hole
            // the balancer would happily keep feeding.
            registry.reportHealth('cluster', () => {
                const { status } = placement.counters();
                return {
                    ready: status === 'active',
                    detail:
                        status === 'fenced'
                            ? 'fenced — membership lost, activations refused'
                            : status === 'leaving'
                              ? 'leaving — draining, take out of rotation'
                              : status
                };
            });
            registry.route({
                name: 'cluster:silo',
                match: (request) => matchesSiloRequest(request, internalBase),
                handle: (request, silo) =>
                    handleSiloRequest(request, {
                        ...options.endpoint,
                        silo,
                        placement,
                        ...(options.secret !== undefined ? { secret: options.secret } : {})
                    })
            });
        }
    };
}
