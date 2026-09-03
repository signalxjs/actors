/**
 * Prometheus text exposition for `@sigx/actors` — a renderer from the
 * runtime's mergeable metrics digest to the exposition format, and a plugin
 * mounting it beside the JSON `ops()` endpoint with the same bearer-token
 * posture.
 *
 * This module imports NOTHING from OpenTelemetry — it is its own entry
 * (`@sigx/actors-otel/prometheus`) precisely so a Prometheus-only consumer
 * never touches the optional `@opentelemetry/api` peer.
 *
 * Cardinality rule (issue #245): labels are actor `type` and `method` only —
 * both already bounded by the digest and folded into `'(other)'`. Actor KEYS
 * can be personal data and never appear here, the same rule `clusterStats`
 * detail clamps follow.
 */
import {
    bucketUpperBoundsUs,
    timingSafeEquals,
    type ActorPlugin,
    type HostStats,
    type MetricsDigest,
    type PluginRegistry
} from '@sigx/actors/host';
import type { Host } from '@sigx/actors';
import type { ClusterCounterTotals } from '@sigx/actors/cluster';
import type { SocketStats } from '@sigx/actors/server';

/**
 * What `registry.digest('sockets')` answers when the app publishes
 * `socketStats().digest()` under that name (#166): the flat monotonic
 * totals plus the connection-lifetime histogram in the runtime's own
 * layout. Named here rather than in `@sigx/actors/server` because the
 * runtime only ever hands it out as the factory's return type.
 */
export type SocketStatsDigest = ReturnType<SocketStats['digest']>;

export interface RenderPrometheusOptions {
    /** Metric-name prefix. Default `'sigx_actors_'`. */
    prefix?: string;
    /**
     * Requested histogram bucket bounds, in seconds. Each is snapped DOWN to
     * the nearest native bound of the digest's log-linear layout — cumulative
     * counts at native bounds are exact, so snapping trades the bound you
     * asked for, never correctness. Bounds below the smallest native bound
     * are dropped. Default: a per-octave grid, 1µs → ~134s (~28 bounds).
     */
    bucketsSeconds?: readonly number[];
    /**
     * The socket-session digest (#166), when the app publishes one
     * (`registry.reportDigest('sockets', () => stats.digest())`). Absent or
     * null renders no socket family at all — a host that serves no sockets
     * is not a host with zero connections, and a scraper that sees the
     * family appear knows the transport is mounted.
     */
    sockets?: SocketStatsDigest | null;
    /**
     * This host's cluster counters (`placement.counters()`), when the host
     * is clustered (#346). Renders the `{prefix}cluster_dispatches_total`
     * pair, labelled `locality="local"` / `"remote"` — the per-request
     * locality fraction is a PromQL ratio over the two, never a precomputed
     * gauge, so it aggregates across a fleet. Absent or null renders no
     * cluster family at all: a single host is not a cluster with zero
     * dispatches.
     */
    cluster?: ClusterCounterTotals | null;
}

/**
 * `prometheusOps` reads the socket digest off the registry itself, and the
 * cluster counters through a thunk read on every scrape.
 */
export interface PrometheusOpsOptions extends Omit<RenderPrometheusOptions, 'sockets' | 'cluster'> {
    /** Route path. Default `'/_sigx/metrics'`. */
    path?: string;
    /**
     * How to read this host's cluster counters on each scrape — typically
     * `() => plugin.placement.counters()` from the `cluster()` plugin
     * (#346). A thunk rather than the placement itself so the endpoint
     * holds no cluster handle and `@sigx/actors` stays size-neutral. Answer
     * `undefined` for "not clustered (yet)": no cluster family is emitted.
     */
    cluster?: () => ClusterCounterTotals | undefined;
    /**
     * Bearer token. MANDATORY outside development — this endpoint reports
     * your actor types and traffic, so it must not be servable by omission
     * (the `ops()` posture, verbatim).
     */
    secret?: string;
}

const DEFAULT_PREFIX = 'sigx_actors_';
const BEARER = /^Bearer (.+)$/;

/** `\` `"` and newline are the three characters the exposition format
 *  escapes in label values. */
function escapeLabel(value: string): string {
    return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n');
}

/**
 * The default grid: every power of two from 1µs to 2^27µs (~134s), i.e. one
 * bound per octave of the layout plus the sub-16µs linear region. Native
 * bounds by construction, so cumulative counts are exact. 28 bounds per
 * histogram rather than the layout's 384 — full native resolution would be
 * ~1,540 series per host, which no scrape budget wants.
 */
function defaultGridUs(): number[] {
    const grid: number[] = [];
    for (let us = 1; us <= 2 ** 27; us *= 2) grid.push(us);
    return grid;
}

/** Snap each requested bound (seconds) DOWN to a native bound (µs). */
function snapGridUs(bucketsSeconds: readonly number[], bounds: readonly number[]): number[] {
    const snapped = new Set<number>();
    for (const seconds of bucketsSeconds) {
        const us = seconds * 1e6;
        // Largest finite native bound <= us; below the smallest, drop.
        let best = -1;
        for (const bound of bounds) {
            if (bound === Infinity || bound > us) break;
            best = bound;
        }
        if (best > 0) snapped.add(best);
    }
    return [...snapped].sort((a, b) => a - b);
}

interface HistogramSource {
    readonly count: number;
    readonly sumUs: number;
    readonly idx: readonly number[];
    readonly n: readonly number[];
}

class Lines {
    #out: string[] = [];
    push(line: string): void {
        this.#out.push(line);
    }
    family(name: string, type: 'counter' | 'gauge' | 'histogram', help: string): void {
        this.#out.push(`# HELP ${name} ${help}`);
        this.#out.push(`# TYPE ${name} ${type}`);
    }
    text(): string {
        return this.#out.join('\n') + '\n';
    }
}

function renderHistogram(
    out: Lines,
    name: string,
    help: string,
    digest: HistogramSource,
    gridUs: readonly number[],
    boundsUs: readonly number[]
): void {
    out.family(name, 'histogram', help);
    // One pass over the sparse buckets, cumulating as the grid advances.
    // A native bound `g` covers exactly the buckets whose upper bound is
    // <= g, so `cumulative` at each grid line is exact (`le` is inclusive
    // and values are integer µs — worst-case skew is the single value
    // sitting exactly on the bound, <= 1µs, far inside the layout's ~6%
    // bucket error).
    let cumulative = 0;
    let i = 0;
    for (const g of gridUs) {
        while (i < digest.idx.length && boundsUs[digest.idx[i]!]! <= g) {
            cumulative += digest.n[i]!;
            i++;
        }
        out.push(`${name}_bucket{le="${g / 1e6}"} ${cumulative}`);
    }
    out.push(`${name}_bucket{le="+Inf"} ${digest.count}`);
    out.push(`${name}_sum ${digest.sumUs / 1e6}`);
    out.push(`${name}_count ${digest.count}`);
}

/**
 * Render one host's metrics digest (plus, optionally, its live `HostStats`
 * gauges) to the Prometheus text exposition format.
 *
 * Pure — exported separately from the plugin so it is testable against
 * golden output and reusable for a merged cluster-wide view later.
 *
 * A digest whose `layout` this build does not know renders counters only —
 * the same counts-only degradation `createMetricsAccumulator` applies, and
 * for the same reason: bucket indices from an unknown axis must not be
 * drawn on this one.
 *
 * Caveat, worth repeating from the README: `metrics().reset()` zeroes the
 * counters. Prometheus `rate()`/`increase()` recover from a counter reset
 * but lose the increments between the reset and the next scrape — do not
 * combine `reset()`-based polling with scraping. The `_metrics_window_seconds`
 * gauge is how an operator notices a reset happened.
 *
 * `options.sockets` is the socket-session digest (#166); see
 * `RenderPrometheusOptions`.
 */
export function renderPrometheus(
    digest: MetricsDigest,
    stats: HostStats | null,
    options: RenderPrometheusOptions = {}
): string {
    const prefix = options.prefix ?? DEFAULT_PREFIX;
    const sockets = options.sockets ?? null;
    const cluster = options.cluster ?? null;
    const out = new Lines();

    // --- counters -----------------------------------------------------
    out.family(`${prefix}calls_total`, 'counter', 'Actor method calls, by actor type.');
    for (const [type, row] of Object.entries(digest.byType)) {
        out.push(`${prefix}calls_total{type="${escapeLabel(type)}"} ${row.calls}`);
    }
    out.family(`${prefix}calls_failed_total`, 'counter', 'Failed actor method calls, by actor type.');
    for (const [type, row] of Object.entries(digest.byType)) {
        out.push(`${prefix}calls_failed_total{type="${escapeLabel(type)}"} ${row.failed}`);
    }

    out.family(
        `${prefix}method_calls_total`,
        'counter',
        'Actor method calls, by type and method.'
    );
    const methodRows: Array<[string, string, { calls: number; failed: number }]> = [];
    for (const [key, row] of Object.entries(digest.byMethod)) {
        // Keys are `Type#method`; the fold row `(other)` has no `#`.
        const hash = key.indexOf('#');
        const type = hash === -1 ? key : key.slice(0, hash);
        const method = hash === -1 ? key : key.slice(hash + 1);
        methodRows.push([type, method, row]);
        out.push(
            `${prefix}method_calls_total{type="${escapeLabel(type)}",method="${escapeLabel(method)}"} ${row.calls}`
        );
    }
    out.family(
        `${prefix}method_calls_failed_total`,
        'counter',
        'Failed actor method calls, by type and method.'
    );
    for (const [type, method, row] of methodRows) {
        out.push(
            `${prefix}method_calls_failed_total{type="${escapeLabel(type)}",method="${escapeLabel(method)}"} ${row.failed}`
        );
    }

    out.family(`${prefix}streams_total`, 'counter', 'Actor stream openings.');
    out.push(`${prefix}streams_total ${digest.calls.streams}`);

    out.family(
        `${prefix}one_way_failures_total`,
        'counter',
        'One-way calls that failed after acceptance — no caller saw the error.'
    );
    out.push(`${prefix}one_way_failures_total ${digest.calls.oneWayFailures ?? 0}`);

    out.family(`${prefix}errors_total`, 'counter', 'Actor call failures, by error kind.');
    for (const [kind, count] of Object.entries(digest.errors.byKind)) {
        out.push(`${prefix}errors_total{kind="${escapeLabel(kind)}"} ${count}`);
    }

    out.family(`${prefix}activations_created_total`, 'counter', 'Activations created.');
    out.push(`${prefix}activations_created_total ${digest.activations.created}`);
    out.family(`${prefix}activations_destroyed_total`, 'counter', 'Activations destroyed.');
    out.push(`${prefix}activations_destroyed_total ${digest.activations.destroyed}`);
    out.family(
        `${prefix}deactivations_total`,
        'counter',
        'Activations destroyed, by deactivation reason.'
    );
    for (const [reason, count] of Object.entries(digest.activations.byReason)) {
        out.push(`${prefix}deactivations_total{reason="${escapeLabel(reason)}"} ${count}`);
    }

    out.family(
        `${prefix}storage_operations_total`,
        'counter',
        'Actor storage operations, by operation.'
    );
    out.push(`${prefix}storage_operations_total{op="load"} ${digest.storage.loads}`);
    out.push(`${prefix}storage_operations_total{op="save"} ${digest.storage.saves}`);
    out.push(`${prefix}storage_operations_total{op="clear"} ${digest.storage.clears}`);
    out.family(
        `${prefix}storage_conflicts_total`,
        'counter',
        'Optimistic-concurrency conflicts reported by storage.'
    );
    out.push(`${prefix}storage_conflicts_total ${digest.storage.conflicts}`);

    // --- histograms ----------------------------------------------------
    const boundsUs = bucketUpperBoundsUs(digest.layout);
    if (boundsUs !== null) {
        const gridUs = options.bucketsSeconds
            ? snapGridUs(options.bucketsSeconds, boundsUs)
            : defaultGridUs();
        const histograms: Array<[string, string, HistogramSource | null]> = [
            [`${prefix}call_duration_seconds`, 'Dispatch latency (queue + turn).', digest.latency],
            [
                `${prefix}turn_queue_duration_seconds`,
                'Time a call waited for its turn.',
                digest.queue
            ],
            [`${prefix}turn_duration_seconds`, 'Time a turn held the activation.', digest.turn],
            [
                `${prefix}storage_operation_duration_seconds`,
                'Actor storage operation latency.',
                digest.storageLatency
            ]
        ];
        for (const [name, help, source] of histograms) {
            if (source) renderHistogram(out, name, help, source, gridUs, boundsUs);
        }
    }

    // --- gauges ---------------------------------------------------------
    if (stats) {
        out.family(`${prefix}activations`, 'gauge', 'Live activations, by actor type.');
        for (const [type, count] of Object.entries(stats.perType)) {
            out.push(`${prefix}activations{type="${escapeLabel(type)}"} ${count}`);
        }
        out.family(`${prefix}queued_messages`, 'gauge', 'Turns waiting to run.');
        out.push(`${prefix}queued_messages ${stats.queued}`);
    }

    // --- socket sessions -----------------------------------------------
    if (sockets) renderSockets(out, prefix, sockets, options.bucketsSeconds);

    // --- cluster locality ----------------------------------------------
    if (cluster) renderCluster(out, prefix, cluster);
    out.family(
        `${prefix}metrics_window_seconds`,
        'gauge',
        'Collection window of this snapshot — a drop means metrics().reset() ran.'
    );
    out.push(`${prefix}metrics_window_seconds ${digest.windowMs / 1000}`);

    return out.text();
}

/**
 * The socket-session families (#166), from the `'sockets'` digest.
 *
 * Counters are the digest's totals one-to-one. The two gauges are DERIVED:
 * `opened − closed` is exactly the number of sessions open right now (the
 * recorder counts a close only for a session it counted open, so the
 * difference cannot drift), and the same holds for subscriptions. They are
 * emitted rather than left to a recording rule because "how many sockets
 * are open" is the first thing anyone alerts on.
 *
 * `delivery_bytes_total` is UTF-16 code units, not encoded bytes — exact
 * for ASCII, an under-count otherwise. The runtime measures it that way on
 * purpose (a UTF-8 pass per frame on the hot path, to sharpen a number
 * nobody bills on), and the HELP text says so rather than the metric name
 * pretending otherwise.
 *
 * The lifetime histogram has its own `layout`, checked separately from the
 * metrics digest's: the two digests come from different providers and a
 * mismatch in one must not cost the other its distribution.
 */
function renderSockets(
    out: Lines,
    prefix: string,
    sockets: SocketStatsDigest,
    bucketsSeconds: readonly number[] | undefined
): void {
    const counters: Array<[string, string, number]> = [
        ['socket_connections_opened_total', 'Socket sessions that completed the upgrade.', sockets.connectionsOpened],
        ['socket_connections_closed_total', 'Socket sessions torn down.', sockets.connectionsClosed],
        [
            'socket_connections_refused_total',
            'Socket upgrades refused before serving a byte (origin, auth).',
            sockets.connectionsRefused
        ],
        ['socket_calls_total', 'Calls dispatched over sockets (unary, stream and one-way).', sockets.callsStarted],
        ['socket_calls_failed_total', 'Socket calls that answered an error frame.', sockets.callsFailed],
        ['socket_subscriptions_opened_total', 'Live subscriptions opened over sockets.', sockets.subscriptionsOpened],
        ['socket_subscriptions_closed_total', 'Live subscriptions closed over sockets.', sockets.subscriptionsClosed],
        [
            'socket_protocol_breaches_total',
            'Socket sessions closed for speaking outside the vocabulary (1003/1009).',
            sockets.protocolBreaches
        ],
        [
            'socket_lifetime_closes_total',
            'Socket sessions closed by revalidateMs/maxConnectionMs (1008).',
            sockets.lifetimeCloses
        ],
        ['socket_deliveries_total', 'Subscription frames pushed to socket clients.', sockets.deliveries],
        [
            'socket_delivery_bytes_total',
            'Outbound subscription-frame size, in UTF-16 code units — exact for ASCII, under-reports otherwise.',
            sockets.deliveryBytes
        ],
        [
            'socket_throttle_quantized_total',
            'Subscriptions whose requested delivery window the throttle policy moved.',
            sockets.throttleQuantized
        ]
    ];
    for (const [name, help, value] of counters) {
        out.family(`${prefix}${name}`, 'counter', help);
        out.push(`${prefix}${name} ${value}`);
    }

    out.family(`${prefix}socket_sessions`, 'gauge', 'Socket sessions open right now.');
    out.push(`${prefix}socket_sessions ${sockets.connectionsOpened - sockets.connectionsClosed}`);
    out.family(`${prefix}socket_subscriptions`, 'gauge', 'Live subscriptions held over sockets right now.');
    out.push(
        `${prefix}socket_subscriptions ${sockets.subscriptionsOpened - sockets.subscriptionsClosed}`
    );

    if (!sockets.lifetime) return;
    const boundsUs = bucketUpperBoundsUs(sockets.layout);
    if (boundsUs === null) return;
    const gridUs = bucketsSeconds ? snapGridUs(bucketsSeconds, boundsUs) : defaultGridUs();
    renderHistogram(
        out,
        `${prefix}socket_connection_duration_seconds`,
        'Lifetime of completed socket sessions.',
        sockets.lifetime,
        gridUs,
        boundsUs
    );
}

/**
 * The cluster dispatch pair (#52, #346): calls THIS host initiated, split
 * by whether this host or a peer served them. Two counter series under one
 * family, so `local / (local + remote)` — the per-request locality fraction
 * the CLI and dashboard show as `locality` — is a `rate()` ratio Prometheus
 * derives, and `sum by (...)` across hosts stays meaningful. A precomputed
 * ratio would be neither.
 *
 * `?? 0`: a host still on a build predating the pair reports totals without
 * it, and a missing series parses worse than a zero one.
 */
function renderCluster(out: Lines, prefix: string, cluster: ClusterCounterTotals): void {
    const totals: Partial<ClusterCounterTotals> = cluster;
    out.family(
        `${prefix}cluster_dispatches_total`,
        'counter',
        'Calls this host initiated, by whether this host (local) or a peer (remote) served them — ' +
            'once per call, stream or watch subscriber.'
    );
    out.push(`${prefix}cluster_dispatches_total{locality="local"} ${totals.dispatchesLocal ?? 0}`);
    out.push(`${prefix}cluster_dispatches_total{locality="remote"} ${totals.dispatchesRemote ?? 0}`);
}

/**
 * Mount the exposition beside `ops()`: `GET /_sigx/metrics` with the same
 * bearer posture — secret mandatory outside development, auth before any
 * path logic, one 401 for missing and wrong alike.
 *
 * Reads the digest through the registry (`registry.digest('metrics')`), so
 * it composes with `metrics()` by registration and holds no direct handle.
 * The `'sockets'` digest is read the same way, and its absence is normal:
 * a host that serves no sockets simply emits no socket family. The cluster
 * counters come through `options.cluster`, a thunk read on every scrape;
 * absent, or answering `undefined`, emits no cluster family.
 */
export function prometheusOps(options: PrometheusOpsOptions = {}): ActorPlugin {
    const raw = options.path ?? '/_sigx/metrics';
    if (!raw.startsWith('/')) {
        // The route matches against a pathname, which always has a leading
        // slash — without one it would simply never match, silently.
        throw new Error(`[sigx actors] prometheusOps({ path }) must start with "/" — got "${raw}".`);
    }
    const path = raw.endsWith('/') ? raw.slice(0, -1) : raw;
    const secret = options.secret;
    const readCluster = options.cluster;
    if (!secret && !__DEV__) {
        throw new Error(
            '[sigx actors] prometheusOps() requires a secret outside development — this ' +
                'endpoint reports your actor types and traffic, so it must not be servable ' +
                'by omission. Pass prometheusOps({ secret }), e.g. from an environment variable.'
        );
    }
    if (__DEV__ && !secret) {
        console.warn(
            '[sigx actors] prometheusOps() is serving UNAUTHENTICATED because no secret was ' +
                'given. This is allowed in development only — a production build throws instead.'
        );
    }
    const render: RenderPrometheusOptions = {
        ...(options.prefix !== undefined ? { prefix: options.prefix } : {}),
        ...(options.bucketsSeconds !== undefined ? { bucketsSeconds: options.bucketsSeconds } : {})
    };

    const fail = (status: number, message: string, extra?: HeadersInit): Response =>
        new Response(JSON.stringify({ error: { message, status } }), {
            status,
            headers: { 'content-type': 'application/json', 'cache-control': 'no-store', ...extra }
        });

    let host: Host | null = null;

    return {
        name: 'prometheus-ops',
        setup(registry: PluginRegistry): void {
            registry.onStart((started) => {
                host = started;
            });
            registry.onStop(() => {
                host = null;
            });
            registry.route({
                name: 'prometheus-ops',
                match(request: Request): boolean {
                    return new URL(request.url).pathname === path;
                },
                async handle(request: Request): Promise<Response> {
                    const method = request.method;
                    if (method !== 'GET' && method !== 'HEAD') {
                        return fail(405, '[sigx actors] the metrics endpoint is GET-only', {
                            allow: 'GET, HEAD'
                        });
                    }
                    // Auth BEFORE anything else — same posture as ops(): an
                    // unauthenticated caller learns nothing, and one message
                    // covers missing and wrong alike.
                    if (secret) {
                        const match = BEARER.exec(request.headers.get('authorization') ?? '');
                        if (!match || !timingSafeEquals(match[1]!, secret)) {
                            return fail(
                                401,
                                '[sigx actors] metrics endpoint requires a valid bearer token',
                                { 'www-authenticate': 'Bearer' }
                            );
                        }
                    }
                    const digest = registry.digest('metrics', { errors: 0 }) as
                        | MetricsDigest
                        | undefined;
                    if (!digest) {
                        return fail(
                            503,
                            '[sigx actors] no metrics digest — register metrics() before ' +
                                'prometheusOps()'
                        );
                    }
                    // `undefined` is "no provider"; `null` is a provider with
                    // nothing to report. Both render no socket family.
                    const sockets = registry.digest('sockets') as SocketStatsDigest | null | undefined;
                    const body = renderPrometheus(digest, host?.stats() ?? null, {
                        ...render,
                        sockets: sockets ?? null,
                        cluster: readCluster?.() ?? null
                    });
                    return new Response(method === 'HEAD' ? null : body, {
                        status: 200,
                        headers: {
                            'content-type': 'text/plain; version=0.0.4; charset=utf-8',
                            // A cached scrape is a stale one.
                            'cache-control': 'no-store'
                        }
                    });
                }
            });
        }
    };
}
