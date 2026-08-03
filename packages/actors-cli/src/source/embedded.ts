/**
 * `embeddedSource` — load the project's own actor app and read it directly.
 *
 * The zero-config development mode. It sees strictly more than the HTTP one
 * (full histograms, the unbounded activation list, the placement itself)
 * because nothing has to cross a wire, and it needs no secret because there
 * is no boundary to authenticate across.
 *
 * The trade is that it STARTS A HOST. It cannot attach to one already
 * running, and the host it starts is a real one — it joins membership,
 * claims actors, and ticks reminders. That is fine pointed at a dev app and
 * wrong pointed at production, which is why the HTTP source exists and why
 * this one says loudly what it is doing.
 */
import type {
    ActorApp,
    HealthPlugin,
    MetricsPlugin,
    OpsPlugin,
    Host
} from '@sigx/actors/host';
import type { ClusterPlugin } from '@sigx/actors/cluster';
import {
    hostViewFromReport,
    type ClusterView,
    type MonitorSnapshot,
    type MonitorSource,
    type HostView,
    type SnapshotOptions
} from './types';

export interface EmbeddedSourceOptions {
    /** Absolute path or file: URL of the module exporting the actor app. */
    module: string;
    /** How many activations to list. Default 200. */
    activations?: number;
    /** Injectable for tests — defaults to a dynamic `import()`. */
    load?: (specifier: string) => Promise<Record<string, unknown>>;
}

/**
 * What we hope to find on the app module.
 *
 * Everything but the app is OPTIONAL, and an app that exports none of it
 * still monitors — it just shows fewer panels. That is the right failure
 * mode for a convention: a plugin handle is only reachable if the author
 * kept a reference to it, and `defineActorApp` deliberately does not expose
 * its plugins (they are contributions, not members). So the CLI asks for
 * them by name rather than inventing a registry accessor in core for the
 * benefit of one consumer.
 *
 * ```ts
 * // src/actors.app.ts
 * export const metrics = actorMetrics();
 * export const health = actorHealth();
 * export const app = defineActorApp({ … }).use(metrics).use(health);
 * ```
 *
 * The HTTP source needs none of this: `ops()` already assembles the same
 * thing server-side, which is why that is the mode for anything you did not
 * write.
 */
interface AppModule {
    app?: ActorApp;
    default?: ActorApp;
    metrics?: MetricsPlugin;
    /** Either handle answers for health — `ops()` carries it too. */
    ops?: OpsPlugin;
    health?: HealthPlugin;
    cluster?: ClusterPlugin;
}

const DEFAULT_ACTIVATIONS = 200;

export class EmbeddedSourceError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'EmbeddedSourceError';
    }
}

export async function embeddedSource(options: EmbeddedSourceOptions): Promise<MonitorSource> {
    const load = options.load ?? ((specifier) => import(specifier) as Promise<Record<string, unknown>>);
    // Non-finite falls back to the default rather than through: NaN would
    // defeat the `=== 0` check below and reach `host.activations()` as a
    // bound that is not one. (Core's own `resolveLimit` is internal, so the
    // check is restated rather than reached into.)
    const limit = Number.isFinite(options.activations)
        ? Math.max(0, Math.floor(options.activations!))
        : DEFAULT_ACTIVATIONS;

    let loaded: AppModule;
    try {
        loaded = (await load(options.module)) as AppModule;
    } catch (error) {
        throw new EmbeddedSourceError(
            `could not load the actor app module "${options.module}": ` +
                `${(error as Error)?.message ?? String(error)}`
        );
    }

    const app = loaded.app ?? loaded.default;
    if (!app || typeof app.start !== 'function') {
        throw new EmbeddedSourceError(
            `"${options.module}" exports no actor app — expected an \`app\` (or default) ` +
                'export from defineActorApp(). Point --app at your actors.app.ts, or use ' +
                '--url to watch a running host over its ops endpoint instead.'
        );
    }

    // An already-started app is reused rather than restarted: `start()` is
    // idempotent while running, and a monitor must never be the thing that
    // cycles the host it is monitoring.
    //
    // Read BEFORE starting, because that is the only moment the answer is
    // knowable — and it decides whether `close()` may stop the app. Shutting
    // down a host we merely attached to would drain someone else's
    // activations on Ctrl+C.
    const alreadyRunning = app.host !== null;
    const host: Host = app.host ?? (await app.start());
    const metrics = loaded.metrics ?? null;
    const cluster = loaded.cluster ?? null;
    const ops = loaded.ops ?? null;
    const healthPlugin = loaded.health ?? null;

    return {
        kind: 'embedded',
        label: options.module,

        async snapshot(signal?: AbortSignal, ask?: SnapshotOptions): Promise<MonitorSnapshot> {
            const stats = host.stats();
            const at = Date.now();
            // `Host` itself carries no start time — uptime lives on the ops
            // and health plugins, both of which stamp it in `onStart`. With
            // neither, a single-node view honestly reports 0 rather than
            // inventing one from when the CLI happened to attach.
            const health = ops?.snapshot().health ?? healthPlugin?.status() ?? null;
            const uptimeMs = health?.uptimeMs ?? 0;

            let hosts: readonly HostView[];
            let clusterView: ClusterView | null = null;
            let partial = false;

            if (cluster) {
                // Imported lazily so a single-node app never pulls the
                // cluster bundle in just to be watched.
                const { clusterStats } = await import('@sigx/actors/cluster');
                const report = await clusterStats(cluster.placement, {
                    signal,
                    ...(ask?.detail
                        ? { detail: ask.hostId ? { hosts: [ask.hostId] } : true }
                        : {})
                });
                hosts = report.hosts.map(hostViewFromReport);
                partial = report.partial;
                clusterView = {
                    from: report.from,
                    view: report.view,
                    totals: report.totals,
                    reminderShards: report.reminderShards,
                    unreachable: report.unreachable
                };
            } else {
                hosts = [
                    {
                        hostId: options.module,
                        address: 'in-process',
                        status: 'unknown',
                        uptimeMs,
                        stats,
                        counters: null,
                        reminderShards: [],
                        membershipVersion: null,
                        transports: null,
                        metrics: null,
                        health: null,
                        activations: null
                    }
                ];
            }

            return {
                at,
                hosts,
                cluster: clusterView,
                metrics: metrics?.snapshot() ?? null,
                activations: limit === 0 ? null : host.activations({ limit, sortBy: 'queued' }),
                health,
                partial
            };
        },

        async close(): Promise<void> {
            if (!alreadyRunning) await app.stop();
        }
    };
}
