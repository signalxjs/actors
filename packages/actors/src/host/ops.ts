/**
 * `ops()` — the authenticated ops endpoint: everything a monitoring tool
 * needs to see, over HTTP, from outside the process.
 *
 * It exists because none of what M7 and `metrics()` already collect was
 * reachable from anywhere else. `metrics().snapshot()` contributes no route,
 * `clusterStats()` takes a placement OBJECT rather than a URL, and the one
 * remote surface that did exist — the `$sigx:host#stats` symbol — is
 * cluster-only, carries no latency distributions, and disappears entirely in
 * a cluster of socket-only transports. A single-node host was completely
 * unobservable from outside.
 *
 * Why this is a NEW route rather than more of `/_sigx/health`:
 *
 *   health   is probed by a load balancer that HAS no cluster secret, so it
 *            cannot be authenticated, so it must not name your actor types.
 *   ops      is read by an operator who does, so it can carry `perType`,
 *            per-method latencies, and the whole cluster topology.
 *
 * That asymmetry is the entire distinction. Extending health would have
 * meant either withholding the useful half or publishing your deployment to
 * anyone who can reach the port.
 *
 * The secret is therefore MANDATORY: `ops()` refuses to construct without
 * one outside `__DEV__`, so there is no configuration in which this ships
 * open by accident.
 */
import { timingSafeEquals } from '../timing-safe';
import {
    emptyHostStats,
    resolveLimit,
    type ActivationInfo,
    type Host,
    type HostStats
} from '../types';
import type { ActorPlugin, OpsReport, PluginRegistry } from './app';
import type { HealthStatus } from './health';

export interface OpsOptions {
    /**
     * Path prefix. The snapshot is served at `{base}`, the cluster fan-out
     * at `{base}/cluster`. Default `/_sigx/ops`.
     */
    base?: string;
    /**
     * Bearer token every request must present.
     *
     * REQUIRED outside `__DEV__` — constructing without one throws, rather
     * than defaulting to open. An ops endpoint that is unauthenticated by
     * omission is worse than no ops endpoint: it publishes your topology,
     * your actor type names and your traffic shape to whoever can reach the
     * port, and nothing about the response tells you it is happening.
     *
     * In dev, omitting it serves unauthenticated and dev-warns once.
     */
    secret?: string;
    /**
     * Enables `GET {base}/cluster`. Supply the fan-out as a thunk — almost
     * always `() => clusterStats(cluster.placement)`.
     *
     * It is wired by the CALLER rather than discovered, because `ops()`
     * lives in `@sigx/actors/host` and `clusterStats` in
     * `@sigx/actors/cluster`: a single-node host must not pay for the
     * cluster bundle to have an ops endpoint. Passing a thunk also leaves
     * `timeoutMs` / `concurrency` where they already are, on
     * `ClusterStatsOptions`, rather than mirroring them here.
     *
     * Without it, `{base}/cluster` answers 404 — the honest code for "this
     * host has no cluster to report on".
     */
    cluster?: (signal: AbortSignal, query?: OpsClusterQuery) => Promise<unknown>;
    /**
     * How many live activations to include, sorted by queue depth. Default
     * 20; 0 omits the list.
     *
     * Small by default for two reasons. The walk is O(activations), and
     * this endpoint gets polled. And the list carries actor KEYS — which on
     * this endpoint is the point, but is also the one field here that can
     * be personal data, so it is worth being able to turn off without
     * giving up the rest of the snapshot.
     */
    activations?: number;
}

/**
 * What `GET {base}/cluster` was asked for, parsed from the query string.
 *
 * Declared structurally rather than imported from `@sigx/actors/cluster`:
 * `ops()` lives in `/host` and must not drag the cluster bundle in to
 * describe a query. It is assignable to `ClusterStatsOptions['detail']`, so
 * the usual thunk — `(signal, query) => clusterStats(placement, { signal,
 * ...query })` — type-checks without either module importing the other.
 */
export interface OpsClusterQuery {
    detail?: boolean | { activations?: number; errors?: number; hosts?: string[] };
}

/** What `GET {base}` answers. */
export interface OpsSnapshot {
    /** Payload version. A rolling deploy IS a mixed-version fleet, and the
     *  tool reading this has to keep working during the deploy that broke
     *  things — same discipline as `HostReport.v`. */
    v: 1;
    /** Epoch-ms the snapshot was taken. */
    at: number;
    /** Since `start()`, ms. */
    uptimeMs: number;
    /** Live gauges, INCLUDING `perType` — this endpoint is authenticated. */
    stats: HostStats;
    /**
     * The hottest live activations, most queued turns first. Bounded by
     * `activations`, empty when it is 0 or the host is not running.
     */
    activations: readonly ActivationInfo[];
    /** The same aggregate `/_sigx/health/ready` reports on, in full. */
    health: HealthStatus;
    /** Every `registry.reportOps()` section: `'metrics'`, `'cluster'`, … */
    ops: OpsReport;
}

export interface OpsPlugin extends ActorPlugin {
    /**
     * The same answer the endpoint gives, in process — for a test, or for a
     * tool embedded in the host rather than polling it.
     */
    snapshot(): OpsSnapshot;
}

/**
 * `?detail=1&activations=5&host=a&host=b` → what the fan-out should ask for.
 *
 * Detail is opted INTO explicitly: absent, off, or anything unrecognised
 * means the cheap report. An allowlist rather than "not one of the falsy
 * spellings", because the expensive direction must be the one you have to
 * ask for precisely — `?detail=flase` should cost nothing, not make every
 * host in the fleet walk its activation table.
 */
const DETAIL_ON = new Set(['', '1', 'true', 'yes', 'on']);

function clusterQuery(url: URL): OpsClusterQuery {
    const detail = url.searchParams.get('detail');
    if (detail === null || !DETAIL_ON.has(detail.toLowerCase())) return {};
    const hosts = url.searchParams.getAll('host').filter(Boolean);
    const number = (name: string): number | undefined => {
        const raw = url.searchParams.get(name);
        if (raw === null) return undefined;
        const value = Number(raw);
        return Number.isFinite(value) && value >= 0 ? Math.floor(value) : undefined;
    };
    const activations = number('activations');
    const errors = number('errors');
    if (hosts.length === 0 && activations === undefined && errors === undefined) {
        return { detail: true };
    }
    return {
        detail: {
            ...(activations === undefined ? {} : { activations }),
            ...(errors === undefined ? {} : { errors }),
            ...(hosts.length > 0 ? { hosts } : {})
        }
    };
}

const BEARER = /^Bearer (.+)$/;
/** Enough to see the hot actors; small enough to poll. */
const DEFAULT_OPS_ACTIVATIONS = 20;

export function ops(options: OpsOptions = {}): OpsPlugin {
    const raw = options.base ?? '/_sigx/ops';
    if (!raw.startsWith('/')) {
        // The route matches against a pathname, which always has a leading
        // slash — without one it would simply never match, silently.
        throw new Error(`[sigx actors] ops({ base }) must start with "/" — got "${raw}".`);
    }
    const base = raw.endsWith('/') ? raw.slice(0, -1) : raw;
    const clusterPath = `${base}/cluster`;
    const secret = options.secret;
    if (!secret && !__DEV__) {
        throw new Error(
            '[sigx actors] ops() requires a secret outside development — this endpoint ' +
                'reports your actor types, traffic and cluster topology, so it must not be ' +
                'servable by omission. Pass ops({ secret }), e.g. from an environment ' +
                'variable. Liveness and readiness stay unauthenticated on /_sigx/health.'
        );
    }
    if (__DEV__ && !secret) {
        console.warn(
            '[sigx actors] ops() is serving UNAUTHENTICATED because no secret was given. ' +
                'This is allowed in development only — a production build throws instead.'
        );
    }
    const collectCluster = options.cluster;
    const activationLimit = resolveLimit(options.activations, DEFAULT_OPS_ACTIVATIONS);

    let host: Host | null = null;
    let startedAt = 0;
    // Captured in setup() and called per REQUEST, so sections and checks
    // contributed by plugins registered AFTER this one are still included.
    let readHealth: (() => HealthStatus) | null = null;
    let readOps: (() => OpsReport) | null = null;

    const snapshot = (): OpsSnapshot => ({
        v: 1,
        at: Date.now(),
        uptimeMs: startedAt === 0 ? 0 : Math.round(performance.now() - startedAt),
        stats: host?.stats() ?? emptyHostStats(),
        activations:
            activationLimit === 0 || !host
                ? []
                : host.activations({ limit: activationLimit, sortBy: 'queued' }),
        health: readHealth?.() ?? {
            live: host !== null,
            ready: host !== null,
            fatal: false,
            uptimeMs: 0,
            checks: {},
            host: null
        },
        ops: readOps?.() ?? {}
    });

    const respond = (status: number, body: unknown, head: boolean): Response =>
        new Response(head ? null : JSON.stringify(body), {
            status,
            headers: {
                'content-type': 'application/json',
                // A cached ops snapshot is a stale one, and every number in
                // it is read precisely because it changes.
                'cache-control': 'no-store'
            }
        });

    const fail = (status: number, message: string, extra?: HeadersInit): Response =>
        new Response(JSON.stringify({ error: { message, status } }), {
            status,
            headers: { 'content-type': 'application/json', 'cache-control': 'no-store', ...extra }
        });

    /** Bearer check. Returns null when the request may proceed. */
    const reject = (request: Request): Response | null => {
        if (!secret) return null;
        const match = BEARER.exec(request.headers.get('authorization') ?? '');
        // One message for missing AND wrong: distinguishing them tells an
        // unauthenticated caller which half they got right.
        if (!match || !timingSafeEquals(match[1]!, secret)) {
            return fail(401, '[sigx actors] ops endpoint requires a valid bearer token', {
                'www-authenticate': 'Bearer'
            });
        }
        return null;
    };

    return {
        name: 'ops',
        snapshot,

        setup(registry: PluginRegistry): void {
            readHealth = () => {
                const report = registry.health();
                const stats = host?.stats();
                return {
                    live: host !== null,
                    ready: host !== null && report.ready,
                    fatal: report.fatal,
                    uptimeMs: startedAt === 0 ? 0 : Math.round(performance.now() - startedAt),
                    checks: report.checks,
                    host: stats ? { activations: stats.activations, queued: stats.queued } : null
                };
            };
            readOps = () => registry.ops();
            registry.onStart((started) => {
                host = started;
                startedAt = performance.now();
            });
            registry.onStop(() => {
                host = null;
                startedAt = 0;
            });
            registry.route({
                name: 'ops',
                match(request: Request): boolean {
                    const path = new URL(request.url).pathname;
                    return path === base || path === clusterPath;
                },
                async handle(request: Request): Promise<Response> {
                    const method = request.method;
                    if (method !== 'GET' && method !== 'HEAD') {
                        return fail(405, '[sigx actors] ops endpoints are GET-only', {
                            allow: 'GET, HEAD'
                        });
                    }
                    // Auth BEFORE the path split, so an unauthenticated
                    // caller cannot probe which paths exist.
                    const denied = reject(request);
                    if (denied) return denied;

                    const head = method === 'HEAD';
                    const url = new URL(request.url);
                    if (url.pathname === base) {
                        return respond(200, snapshot(), head);
                    }
                    if (!collectCluster) {
                        return fail(
                            404,
                            '[sigx actors] this host reports no cluster — pass ' +
                                'ops({ cluster: () => clusterStats(placement) }) to enable it'
                        );
                    }
                    // The fan-out is bounded by `clusterStats` itself, but a
                    // caller that hangs up must not leave a wave of peer
                    // requests running behind it.
                    const controller = new AbortController();
                    const abort = (): void => controller.abort();
                    request.signal?.addEventListener('abort', abort);
                    try {
                        return respond(
                            200,
                            await collectCluster(controller.signal, clusterQuery(url)),
                            head
                        );
                    } catch (error) {
                        // `clusterStats` is documented never to throw because
                        // a PEER is sick — it classifies those into
                        // `unreachable` and sets `partial`. So reaching here
                        // means the collector itself failed, which is worth
                        // saying plainly rather than as an empty report that
                        // reads like a healthy cluster of zero hosts.
                        const message = (error as Error)?.message ?? String(error);
                        return fail(503, `[sigx actors] cluster stats failed: ${message}`);
                    } finally {
                        request.signal?.removeEventListener('abort', abort);
                    }
                }
            });
        }
    };
}
