/**
 * `health()` — liveness and readiness endpoints, built from the readiness
 * seam rather than from any knowledge of what a silo is attached to.
 *
 * The plugin itself knows nothing about clustering. It serves the aggregate
 * of every `registry.reportHealth()` contribution, and `cluster()` registers
 * its own — so `.use(cluster(...)).use(health())` reports membership state
 * with no wiring, and a single-node app gets the same two endpoints with an
 * empty check set.
 *
 * The distinction that matters operationally:
 *
 *   liveness   is this process worth keeping?      -> restart if it fails
 *   readiness  should it be receiving traffic?     -> drain if it fails
 *
 * Conflating them is how a rolling deploy turns into an outage: a draining
 * silo (M4's `beginStop` announces `leaving` BEFORE the drain) is perfectly
 * alive and must not be restarted — it must stop receiving new calls while
 * it hands its activations off.
 */
import type { Silo } from '../types';
import type { ActorPlugin, HealthReport, PluginRegistry } from './app';

export interface HealthOptions {
    /**
     * Path prefix. Liveness is served at `{base}`, readiness at
     * `{base}/ready`. Default `/_sigx/health`.
     */
    base?: string;
    /**
     * Include the per-check `checks` map and the silo gauges in the response
     * body. Default `__DEV__`. The status code and a minimal
     * `{ status, uptimeMs }` body are always returned; this only controls
     * the diagnostic extras.
     *
     * These endpoints are UNAUTHENTICATED — they have to be, since the
     * prober is a load balancer with no cluster secret — so in production
     * the status code carries the contract and the extras would describe
     * your deployment to whoever can reach the port.
     */
    detail?: boolean;
}

/** Deliberately NOT `SiloStats`: `perType` names your actor types, and this
 *  endpoint cannot be authenticated. Totals only. */
export interface HealthGauges {
    activations: number;
    queued: number;
}

export interface HealthStatus {
    /** The silo is running. */
    live: boolean;
    /** Every contributed check passed. */
    ready: boolean;
    /** Since `start()`, ms. */
    uptimeMs: number;
    checks: HealthReport['checks'];
    /** Activation totals — null before start. Never a per-type breakdown. */
    silo: HealthGauges | null;
}

export interface HealthPlugin extends ActorPlugin {
    /**
     * The same answer the endpoint gives, in process — for a test, a
     * shutdown gate, or a hand-rolled mount.
     */
    status(): HealthStatus;
}

export function health(options: HealthOptions = {}): HealthPlugin {
    const base = options.base ?? '/_sigx/health';
    if (!base.startsWith('/')) {
        // The route matches against a pathname, which always has a leading
        // slash — without one it would simply never match, silently.
        throw new Error(
            `[sigx actors] health({ base }) must start with "/" — got "${base}".`
        );
    }
    const readyPath = `${base.endsWith('/') ? base.slice(0, -1) : base}/ready`;
    const livePath = base.endsWith('/') ? base.slice(0, -1) : base;
    const detail = options.detail ?? __DEV__;

    let silo: Silo | null = null;
    let startedAt = 0;
    // Captured in setup() and called per REQUEST: the aggregate must include
    // plugins registered after this one.
    let report: (() => HealthReport) | null = null;

    const status = (): HealthStatus => {
        const health = report?.() ?? { ready: true, checks: {} };
        const stats = silo?.stats();
        return {
            live: silo !== null,
            ready: silo !== null && health.ready,
            uptimeMs: startedAt === 0 ? 0 : Math.round(performance.now() - startedAt),
            checks: health.checks,
            // Totals only — `perType` would publish your actor type names on
            // an endpoint that cannot be authenticated.
            silo: stats ? { activations: stats.activations, queued: stats.queued } : null
        };
    };

    const respond = (status: number, body: unknown, head: boolean): Response =>
        new Response(head ? null : JSON.stringify(body), {
            status,
            headers: {
                'content-type': 'application/json',
                // A cached readiness answer is a drained silo still taking
                // traffic, or a healthy one still out of rotation.
                'cache-control': 'no-store'
            }
        });

    return {
        name: 'health',
        status,

        setup(registry: PluginRegistry): void {
            report = () => registry.health();
            registry.onStart((started) => {
                silo = started;
                startedAt = performance.now();
            });
            registry.onStop(() => {
                silo = null;
                startedAt = 0;
            });
            registry.route({
                name: 'health',
                match(request: Request): boolean {
                    const path = new URL(request.url).pathname;
                    return path === livePath || path === readyPath;
                },
                handle(request: Request): Promise<Response> {
                    const method = request.method;
                    if (method !== 'GET' && method !== 'HEAD') {
                        return Promise.resolve(
                            new Response(
                                JSON.stringify({
                                    error: {
                                        message: '[sigx actors] health endpoints are GET-only',
                                        status: 405
                                    }
                                }),
                                {
                                    status: 405,
                                    headers: {
                                        'content-type': 'application/json',
                                        allow: 'GET, HEAD',
                                        'cache-control': 'no-store'
                                    }
                                }
                            )
                        );
                    }
                    const head = method === 'HEAD';
                    const current = status();
                    // Liveness deliberately asks nothing of the checks: a
                    // draining or fenced silo is NOT ready, but it is very
                    // much alive and restarting it would abort the handoff.
                    if (new URL(request.url).pathname === livePath) {
                        return Promise.resolve(
                            respond(200, { status: 'live', uptimeMs: current.uptimeMs }, head)
                        );
                    }
                    const body = current.ready
                        ? { status: 'ready', uptimeMs: current.uptimeMs }
                        : { status: 'not-ready', uptimeMs: current.uptimeMs };
                    return Promise.resolve(
                        respond(
                            current.ready ? 200 : 503,
                            detail
                                ? { ...body, checks: current.checks, silo: current.silo }
                                : body,
                            head
                        )
                    );
                }
            });
        }
    };
}
