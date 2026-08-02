/**
 * `cluster()` — clustering as an app plugin.
 *
 * Hand-wired, a clustered host repeats itself: `secret` goes to both
 * `clusterPlacement` and `handleHostRequest`, `internalBase` has to agree
 * with `matchesHostRequest`'s default, and the internal mount is left to the
 * entry to route. Nothing type-checks any of that agreement.
 *
 * As a plugin each value is declared ONCE and the internal mount travels
 * with it as a contributed route, so an adapter that mounts `app.routes`
 * gets host-to-host traffic for free. This is composition only — the
 * placement, endpoint and providers underneath are unchanged.
 */
import type { ActorPlugin, HealthReport, PluginRegistry } from '../host/app';
import type { MetricsDigest } from '../host/digest';
import {
    clusterPlacement,
    type ClusterPlacement,
    type RebalanceOptions
} from './placement';
import type { HostTransportFactory } from './seam';
import type { HostEndpointOptions } from './host-endpoint';
import { httpTransport } from './transport';
import type { ClusterProviders, PlacementPolicy } from './types';

export interface ClusterPluginOptions {
    /**
     * Membership + directory for THIS host, from a provider package
     * (`redisCluster({ url })`, `memoryClusterHub().providers()`). A named
     * field rather than a spread, so the provider/config boundary stays
     * visible.
     */
    providers: ClusterProviders;
    /** Peer-reachable origin of this host's HTTP listener. */
    advertise: string;
    /**
     * Origin a CLIENT can reach this host's PUBLIC actor mount on, e.g.
     * `https://host-3.example.com` — published so peers can redirect a
     * caller here under `onMiss: 'redirect'`.
     *
     * Deliberately NOT defaulted from `advertise`, which is the internal
     * origin: a pod IP is unreachable from outside and disclosing it hands
     * out internal topology. Unset means peers proxy for this host instead
     * of redirecting to it.
     */
    publicAddress?: string;
    /**
     * Shared cluster secret. Declared once here and used by BOTH the
     * outbound transport and the internal mount's HMAC verification.
     */
    secret?: string;
    /** Path prefix of the internal mount. Default `/_sigx/host`. */
    internalBase?: string;
    /**
     * How this host reaches its peers. Default `httpTransport()`.
     *
     * A LIST is a fallback chain, tried in order — the rolling-deploy
     * story: `[tcpTransport(), httpTransport()]` upgrades link by link as
     * peers gain a `tcp` address, with no window where half the cluster is
     * unreachable. A SINGLE transport is strict: a peer publishing no
     * address for it is unreachable, loudly. `httpTransport()` reaches
     * every peer, so it is only ever valid as the LAST entry.
     *
     * Configuring only socket transports means there is NO internal HTTP
     * mount — a smaller attack surface, and nothing to `curl`.
     */
    transport?: HostTransportFactory | readonly HostTransportFactory[];
    /**
     * Fetch implementation (tests pipe it straight into peers' handlers).
     * Sugar for `transport: httpTransport({ fetch })`; passing both throws.
     */
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
    /**
     * Forwarded to the internal HTTP mount (body caps, `onError`,
     * `timeoutMs`). Sugar for `transport: httpTransport({ endpoint })`;
     * passing both throws, since it would otherwise silently apply to
     * nothing when the chain contains no HTTP transport.
     */
    endpoint?: HostEndpointOptions;
    /**
     * Run `placement.rebalance()` on a cadence — OFF unless configured.
     * `intervalMs` default 60 000; the rest are `RebalanceOptions`. Each
     * host sheds only its own excess (down to the cluster mean, bounded by
     * `maxMoves` per round), so the correction is slow, decentralized, and
     * composes with whatever placement policy re-places the shed actors —
     * pair it with `activationCountPolicy()` and they land on cold hosts.
     */
    rebalance?: RebalanceOptions & { intervalMs?: number };
}

export interface ClusterPlugin extends ActorPlugin {
    /**
     * This host's placement — for the operational primitives that have no
     * plugin equivalent: `identity`, `descriptor()`, `migrate(ref)`.
     * Available immediately; the placement does not need the host until
     * `bind()`.
     */
    readonly placement: ClusterPlacement;
}

export function cluster(options: ClusterPluginOptions): ClusterPlugin {
    const internalBase = options.internalBase ?? '/_sigx/host';
    if (!internalBase.startsWith('/')) {
        // Silent otherwise, and doubly so: `matchesHostRequest` compares
        // against a pathname (always leading-slash) so the route would never
        // match, AND peer dispatch URLs would be malformed.
        throw new Error(
            `[sigx actors] cluster({ internalBase }) must start with "/" — got ` +
                `"${internalBase}".`
        );
    }
    if (options.transport && (options.fetch || options.endpoint)) {
        // Both only ever configure the HTTP transport, which an explicit
        // chain may not even contain — silently applying to nothing is worse
        // than a throw.
        const names = [
            ...(options.fetch ? ['fetch'] : []),
            ...(options.endpoint ? ['endpoint'] : [])
        ];
        // Two renderings: one that is valid object-literal syntax to paste,
        // and one that reads as prose. Joining with "and" would suggest
        // `httpTransport({ fetch and endpoint })`, which is neither.
        const literal = names.join(', ');
        const prose = names.join(' and ');
        throw new Error(
            `[sigx actors] cluster({ transport }) and cluster({ ${literal} }) are mutually ` +
                `exclusive — ${prose} only ever reach the HTTP transport, which an explicit ` +
                `chain need not contain. Pass them to it instead: ` +
                `transport: httpTransport({ ${literal} }).`
        );
    }
    // `fetch`/`endpoint` are sugar for the default transport's two options,
    // so the chain is built once, here, and the placement is only ever handed
    // a transport.
    const transport =
        options.transport ??
        httpTransport({
            ...(options.fetch ? { fetch: options.fetch } : {}),
            ...(options.endpoint ? { endpoint: options.endpoint } : {})
        });
    // Built eagerly: a placement defers everything that needs the host to
    // `bind()`, so there is nothing to wait for — and exposing it right away
    // keeps `migrate()`/`identity` reachable without starting the app. It is
    // also why the transport is an option here rather than its own plugin:
    // a later `.use()` could not reach this constructor.
    // Late-bound, exactly as `ops()` late-binds its health/ops readers: the
    // registry does not exist yet here, and reading these per REPORT rather
    // than capturing them at setup is what makes `.use()` order irrelevant.
    let readHealth: (() => HealthReport) | null = null;
    let readDigest: ((options?: unknown) => unknown) | null = null;

    const placement = clusterPlacement({
        ...options.providers,
        health: () => readHealth?.(),
        // `registry.digest()` walks digest providers ONLY. Reading the ops
        // section instead would re-enter this very report — `cluster()`
        // publishes `placement.report()` as the `'cluster'` ops section —
        // and would hand back percentiles, which is precisely the shape
        // that cannot be merged across hosts.
        metrics: (digestOptions) => readDigest?.(digestOptions) as MetricsDigest | undefined,
        advertise: options.advertise,
        ...(options.publicAddress !== undefined
            ? { publicAddress: options.publicAddress }
            : {}),
        internalBase,
        transport,
        ...(options.secret !== undefined ? { secret: options.secret } : {}),
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
            readHealth = () => registry.health();
            readDigest = (digestOptions) => registry.digest('metrics', digestOptions);
            // Readiness, so a `health()` endpoint drains this host without
            // knowing clustering exists. `'leaving'` is the M4 handoff
            // window (announced BEFORE the drain, which is the whole point:
            // the load balancer stops sending while activations hand off).
            // `'fenced'` is the one that would otherwise be invisible —
            // `#fence()` leaves the PUBLISHED status at 'active' while
            // every activation is refused, so a fenced host is a black hole
            // the balancer would happily keep feeding.
            registry.reportHealth('cluster', () => {
                const { status } = placement.counters();
                return {
                    ready: status === 'active',
                    // Fenced is TERMINAL for this host identity (identities
                    // are minted per start, there is no rejoin path), so it
                    // must fail liveness — not just readiness — or the pod
                    // sits live-200/ready-503 forever after a membership
                    // store outage and only a human can revive the cluster.
                    // `leaving` stays non-fatal: draining is alive.
                    fatal: status === 'fenced',
                    detail:
                        status === 'fenced'
                            ? 'fenced — membership lost, activations refused'
                            : status === 'leaving'
                              ? 'leaving — draining, take out of rotation'
                              : status
                };
            });
            // This host's own report, for an `ops()` endpoint. Deliberately
            // the LOCAL report and not a `clusterStats()` fan-out: a section
            // provider is sync and must stay cheap, and the fan-out is an
            // explicit second route precisely because it costs N peer
            // round-trips.
            // Deliberately the BARE report: no digest, no actor list. The
            // metrics section sits right beside this one in the same
            // snapshot, so including the digest here would ship the same
            // numbers twice — once as percentiles and once as buckets.
            registry.reportOps('cluster', () => placement.report());
            // The internal mount is no longer special-cased: it is whatever
            // the configured transports declare. A chain of socket
            // transports declares nothing, and this host then has no
            // internal HTTP surface at all.
            for (const route of placement.routes()) registry.route(route);

            if (options.rebalance) {
                const { intervalMs = 60_000, ...round } = options.rebalance;
                let timer: ReturnType<typeof setInterval> | null = null;
                // Single-flight: a round that outlives the interval (slow
                // probes, many drains) must not stack the next one behind
                // it — churn is exactly what a slow correction must not do.
                let running = false;
                registry.onStart(() => {
                    // `rebalance()` is total — it reports instead of
                    // throwing — so the loop needs no error plumbing.
                    timer = setInterval(() => {
                        if (running) return;
                        running = true;
                        void placement.rebalance(round).finally(() => {
                            running = false;
                        });
                    }, intervalMs);
                    // A background correction must never hold a process open.
                    (timer as { unref?: () => void }).unref?.();
                });
                registry.onStop(() => {
                    if (timer !== null) clearInterval(timer);
                    timer = null;
                });
            }
        }
    };
}
