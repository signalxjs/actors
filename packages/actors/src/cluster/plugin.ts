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
     *
     * REQUIRED outside development. The internal mount runs no guards — the
     * public edge is supposed to have run them — and it rides the same
     * listener as the public actor endpoint, so omitting this serves every
     * type, key and method unauthenticated. `ops()` has thrown on the same
     * omission since it shipped; this exposes more.
     *
     * `null` is the deliberate opt-out, for a mount that is genuinely not
     * reachable by an untrusted caller (mTLS-terminated mesh, a private
     * network, a single host). It is spelled explicitly so the choice appears
     * in the diff rather than in the absence of a line.
     */
    secret?: string | null;
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
    /**
     * Free-form placement hints published in the membership descriptor and
     * on the ops channel (`HostReport.meta`). One key has an agreed meaning:
     * `node`, the machine this host runs on — fill it from the downward
     * API's `spec.nodeName` and the monitor counts distinct values into
     * `3 host(s) / 1 node(s)`, the packed fleet every replica readout
     * reports as `3/3` (#51).
     */
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
    // Same posture as `ops()`, for a mount that exposes strictly more: the
    // internal endpoint runs NO guards, so a missing secret is not "clustering
    // without auth", it is every actor reachable by anyone who can address the
    // listener. Throw on omission outside dev; `null` says it was meant.
    const { secret } = options;
    if (typeof secret === 'string' && secret.trim() === '') {
        // Never deliberate — this is `process.env.CLUSTER_SECRET ?? ''`
        // reading as configured while authenticating with an empty key. It
        // throws in development too, because there is no dev story for it:
        // `undefined` already covers "I have not set this up yet".
        throw new Error(
            '[sigx actors] cluster({ secret }) must not be empty — an empty secret ' +
                'signs and verifies every peer call with an empty key, which is not ' +
                'authentication. Pass a real secret, or `secret: null` to opt out ' +
                'deliberately on a trusted network.'
        );
    }
    if (secret === undefined && !__DEV__) {
        throw new Error(
            '[sigx actors] cluster() requires a secret outside development — the internal ' +
                'host-to-host mount runs no guards and rides the same listener as your ' +
                'public actor endpoint, so without one every actor type, key and method is ' +
                'reachable unauthenticated. Pass cluster({ secret }), e.g. from an ' +
                'environment variable, or cluster({ secret: null }) if this mount is ' +
                'genuinely unreachable by an untrusted caller.'
        );
    }
    if (__DEV__ && secret === undefined) {
        console.warn(
            '[sigx actors] cluster() is serving its internal mount UNAUTHENTICATED because ' +
                'no secret was given. This is allowed in development only — a production ' +
                'build throws instead. Pass cluster({ secret: null }) to make it deliberate.'
        );
    }

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
        // `null` is the opt-out, and reaches the placement as absence — the
        // layers below only ever see "a secret" or "no secret", so the opt-out
        // stays a construction-time concept rather than a third state to
        // thread through signing and verification.
        ...(typeof secret === 'string' ? { secret } : {}),
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
                    // Fenced is TERMINAL for this host identity: it holds
                    // (or could hold) claims a survivor has taken over, so
                    // it may never re-register under an identity peers
                    // remember. It must therefore fail liveness — not just
                    // readiness — or the pod sits live-200/ready-503 forever
                    // after a membership store outage and only a human can
                    // revive the cluster. `leaving` stays non-fatal:
                    // draining is alive.
                    //
                    // A host registering ONLY workers never reaches this
                    // status at all: it claims nothing, so it rejoins on its
                    // own instead of fencing (#272) and stays ready while it
                    // retries.
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
