/**
 * `httpSource` — poll a running host's `ops()` endpoint.
 *
 * The mode that works against production: it loads no user code, holds no
 * host, and needs nothing but a URL and the bearer secret. Everything it
 * knows comes from two GETs.
 */
import type { ActivationInfo, ActorMetricsSnapshot, HealthStatus } from '@sigx/actors/host';
import type { ClusterStatsReport, HostReport } from '@sigx/actors/cluster';
import {
    hostViewFromReport,
    type ClusterView,
    type MonitorSnapshot,
    type MonitorSource,
    type HostView,
    type SnapshotOptions
} from './types';

export interface HttpSourceOptions {
    /** Origin of the host's mount, e.g. `http://localhost:3000`. */
    url: string;
    /** The `ops({ secret })` bearer token. */
    secret?: string;
    /** Path prefix, matching `ops({ base })`. Default `/_sigx/ops`. */
    base?: string;
    /** Per-request budget, ms. Default 5000. */
    timeoutMs?: number;
    /** Injectable for tests. */
    fetch?: typeof globalThis.fetch;
}

/** The subset of `OpsSnapshot` we depend on, restated so a version skew
 *  between the CLI and the host it is watching is a missing field rather
 *  than a type error at build time. */
interface OpsBody {
    v: number;
    at: number;
    uptimeMs: number;
    stats: HostView['stats'];
    // Optional because the whole point of restating this shape is that a
    // host on an older build may not send them. Typing a field as required
    // and then defaulting it is the unsound half of both worlds.
    activations?: readonly ActivationInfo[];
    health?: HealthStatus;
    ops?: Record<string, unknown>;
}

const DEFAULT_TIMEOUT_MS = 5000;

export class OpsRequestError extends Error {
    readonly status: number | null;
    constructor(message: string, status: number | null) {
        super(message);
        this.name = 'OpsRequestError';
        this.status = status;
    }
}

export function httpSource(options: HttpSourceOptions): MonitorSource {
    const origin = options.url.replace(/\/+$/, '');
    const base = (options.base ?? '/_sigx/ops').replace(/\/+$/, '');
    const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    const doFetch = options.fetch ?? globalThis.fetch;
    const headers: Record<string, string> = options.secret
        ? { authorization: `Bearer ${options.secret}` }
        : {};

    /**
     * `null` means "this host does not serve that route", which is a real
     * answer rather than a failure — a single-node host genuinely has no
     * cluster. Any other non-2xx is an error worth showing, because a
     * dashboard that silently renders zeroes for a 401 is how you spend ten
     * minutes debugging a healthy cluster.
     */
    async function get<T>(path: string, signal?: AbortSignal): Promise<T | null> {
        // Already cancelled: `addEventListener('abort')` never fires on a
        // signal that has already aborted, so the request would run to its
        // full timeout and then be reported as a sick host rather than as
        // the cancellation it was.
        signal?.throwIfAborted();
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);
        const onAbort = (): void => controller.abort();
        signal?.addEventListener('abort', onAbort);
        try {
            const response = await doFetch(`${origin}${path}`, {
                headers,
                signal: controller.signal
            });
            if (response.status === 404) return null;
            if (!response.ok) {
                throw new OpsRequestError(
                    response.status === 401
                        ? `${origin} rejected the ops secret (401) — check --secret / SIGX_OPS_SECRET`
                        : `${origin}${path} answered ${response.status}`,
                    response.status
                );
            }
            return (await response.json()) as T;
        } catch (error) {
            if (error instanceof OpsRequestError) throw error;
            if ((error as Error)?.name === 'AbortError') {
                // Distinguish OUR deadline from the caller hanging up: only
                // the former is worth reporting as a sick host.
                if (signal?.aborted) throw error;
                throw new OpsRequestError(`${origin}${path} timed out after ${timeoutMs}ms`, null);
            }
            throw new OpsRequestError(
                `${origin}${path} is unreachable: ${(error as Error)?.message ?? String(error)}`,
                null
            );
        } finally {
            clearTimeout(timer);
            signal?.removeEventListener('abort', onAbort);
        }
    }

    return {
        kind: 'http',
        label: origin,

        async snapshot(signal?: AbortSignal, ask?: SnapshotOptions): Promise<MonitorSnapshot> {
            const body = await get<OpsBody>(base, signal);
            if (!body) {
                throw new OpsRequestError(
                    `${origin}${base} is not serving an ops endpoint — is ops() wired into the app?`,
                    404
                );
            }
            // The cluster fan-out is a second, more expensive route, and its
            // absence is normal. Requested after the snapshot so a
            // single-node host still renders instantly.
            // `?detail` is only sent while something is actually looking at
            // a per-host panel — otherwise every poll would make the whole
            // cluster walk its activation table for nothing.
            const query = ask?.detail
                ? `?detail=1${ask.hostId ? `&host=${encodeURIComponent(ask.hostId)}` : ''}`
                : '';
            const cluster = await get<ClusterStatsReport>(`${base}/cluster${query}`, signal);

            const local = body.ops?.cluster as HostReport | undefined;
            const hosts: HostView[] = cluster
                ? cluster.hosts.map(hostViewFromReport)
                : [selfView(body, local, origin)];

            return {
                at: body.at,
                hosts,
                cluster: cluster ? clusterView(cluster) : null,
                metrics: (body.ops?.metrics as ActorMetricsSnapshot | undefined) ?? null,
                activations: body.activations ?? null,
                health: body.health ?? null,
                partial: cluster?.partial ?? false
            };
        },

        close(): Promise<void> {
            return Promise.resolve();
        }
    };
}

/** The polled host alone, for a single-node deployment. */
function selfView(body: OpsBody, local: HostReport | undefined, origin: string): HostView {
    if (local) return hostViewFromReport(local);
    return {
        // A host with no cluster() has no host id at all — it is not
        // anonymous by accident, it genuinely has no membership identity.
        hostId: origin,
        address: origin,
        status: 'unknown',
        uptimeMs: body.uptimeMs,
        stats: body.stats,
        counters: null,
        reminderShards: [],
        membershipVersion: null,
        transports: null,
        metrics: null,
        health: body.health ? { ready: body.health.ready, fatal: body.health.fatal, checks: body.health.checks } : null,
        activations: body.activations ?? null
    };
}

function clusterView(report: ClusterStatsReport): ClusterView {
    return {
        from: report.from,
        view: report.view,
        totals: report.totals,
        reminderShards: report.reminderShards,
        unreachable: report.unreachable
    };
}
