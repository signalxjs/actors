/**
 * OpenTelemetry metrics bridge for `@sigx/actors` — observable instruments
 * over `metrics().digest()`.
 *
 * No timer of its own: OTel's pull model already owns the period (a
 * `PeriodicExportingMetricReader` drives collection), so the bridge
 * registers observables and one batch callback that reads the digest ONCE
 * per collection — every instrument observes the same consistent snapshot.
 *
 * Distributions: the OTel metrics API has no instrument that ingests
 * pre-bucketed histograms, and re-recording per-dispatch through a
 * synchronous Histogram would duplicate the runtime's own recorder and
 * double the cost. So the digest's percentiles export as gauges (off by
 * `percentileGauges: false`), plainly labeled non-aggregatable across
 * hosts — real distribution export belongs to the Prometheus endpoint,
 * where the raw buckets survive.
 *
 * Register the `MeterProvider` (globally or via the option) BEFORE
 * `app.start()`: unlike traces, the api has no upgrading proxy for meters —
 * a meter obtained from the no-op provider stays no-op.
 *
 * The socket-session digest (#166) rides the same callback: a second
 * `registry.digest('sockets')` read beside the first, observed only when
 * the app publishes one. The instruments exist either way — an instrument
 * with no observations exports nothing, and creating them lazily on the
 * first digest would race the reader.
 */
import { metrics as otelMetrics, type BatchObservableResult, type MeterProvider, type Observable } from '@opentelemetry/api';
import type { Host } from '@sigx/actors';
import type { SocketStatsTotals } from '@sigx/actors/server';
import {
    digestSnapshot,
    type ActorPlugin,
    type MetricsDigest,
    type PluginRegistry
} from '@sigx/actors/host';
import type { SocketStatsDigest } from './prometheus';

export interface OtelMetricsBridgeOptions {
    /** The provider to report against. Default: the global registration. */
    meterProvider?: MeterProvider;
    /**
     * Export p50/p90/p99 of the four runtime distributions as gauges
     * (seconds). Default true. Per-host percentiles do NOT aggregate —
     * averaging two hosts' p99s describes no call that ever happened — so
     * dashboards over more than one host should read the Prometheus
     * endpoint's histograms instead.
     */
    percentileGauges?: boolean;
}

const METER_NAME = '@sigx/actors-otel';

export function otelMetricsBridge(options: OtelMetricsBridgeOptions = {}): ActorPlugin {
    const percentileGauges = options.percentileGauges !== false;

    return {
        name: 'otel-metrics-bridge',
        setup(registry: PluginRegistry): void {
            let host: Host | null = null;
            let detach: (() => void) | null = null;

            registry.onStart((started) => {
                host = started;
                const meter = (options.meterProvider ?? otelMetrics.getMeterProvider()).getMeter(
                    METER_NAME
                );

                const calls = meter.createObservableCounter('sigx.actors.calls', {
                    description: 'Actor method calls, by actor type.'
                });
                const callsFailed = meter.createObservableCounter('sigx.actors.calls.failed', {
                    description: 'Failed actor method calls, by actor type.'
                });
                const streams = meter.createObservableCounter('sigx.actors.streams', {
                    description: 'Actor stream openings.'
                });
                const oneWayFailures = meter.createObservableCounter(
                    'sigx.actors.calls.one_way_failures',
                    {
                        description:
                            'One-way calls that failed after acceptance — no caller saw the error.'
                    }
                );
                const errors = meter.createObservableCounter('sigx.actors.errors', {
                    description: 'Actor call failures, by error kind.'
                });
                const created = meter.createObservableCounter('sigx.actors.activations.created', {
                    description: 'Activations created.'
                });
                const destroyed = meter.createObservableCounter(
                    'sigx.actors.activations.destroyed',
                    { description: 'Activations destroyed.' }
                );
                const deactivations = meter.createObservableCounter('sigx.actors.deactivations', {
                    description: 'Activations destroyed, by deactivation reason.'
                });
                const storageOps = meter.createObservableCounter('sigx.actors.storage.operations', {
                    description: 'Actor storage operations, by operation.'
                });
                const conflicts = meter.createObservableCounter('sigx.actors.storage.conflicts', {
                    description: 'Optimistic-concurrency conflicts reported by storage.'
                });
                const activations = meter.createObservableGauge('sigx.actors.activations', {
                    description: 'Live activations, by actor type.'
                });
                const queued = meter.createObservableGauge('sigx.actors.queued_messages', {
                    description: 'Turns waiting to run.'
                });

                // Socket sessions (#166). Counters map one-to-one onto the
                // digest's totals; the two gauges are `opened − closed`,
                // which is exactly the live count (the recorder closes only
                // what it opened) — the same derivation the Prometheus
                // renderer makes, so the two exporters cannot disagree.
                const socketCounters: Array<[Observable, keyof SocketStatsTotals]> = (
                    [
                        ['connections.opened', 'Socket sessions that completed the upgrade.', 'connectionsOpened'],
                        ['connections.closed', 'Socket sessions torn down.', 'connectionsClosed'],
                        [
                            'connections.refused',
                            'Socket upgrades refused before serving a byte (origin, auth).',
                            'connectionsRefused'
                        ],
                        ['calls', 'Calls dispatched over sockets (unary, stream and one-way).', 'callsStarted'],
                        ['calls.failed', 'Socket calls that answered an error frame.', 'callsFailed'],
                        ['subscriptions.opened', 'Live subscriptions opened over sockets.', 'subscriptionsOpened'],
                        ['subscriptions.closed', 'Live subscriptions closed over sockets.', 'subscriptionsClosed'],
                        [
                            'protocol_breaches',
                            'Socket sessions closed for speaking outside the vocabulary (1003/1009).',
                            'protocolBreaches'
                        ],
                        [
                            'lifetime_closes',
                            'Socket sessions closed by revalidateMs/maxConnectionMs (1008).',
                            'lifetimeCloses'
                        ],
                        ['deliveries', 'Subscription frames pushed to socket clients.', 'deliveries'],
                        [
                            'delivery_bytes',
                            'Outbound subscription-frame size, in UTF-16 code units — exact for ' +
                                'ASCII, under-reports otherwise.',
                            'deliveryBytes'
                        ],
                        [
                            'throttle_quantized',
                            'Subscriptions whose requested delivery window the throttle policy moved.',
                            'throttleQuantized'
                        ]
                    ] as const
                ).map(([name, description, field]) => [
                    meter.createObservableCounter(`sigx.actors.socket.${name}`, { description }),
                    field
                ]);
                const socketSessions = meter.createObservableGauge('sigx.actors.socket.sessions', {
                    description: 'Socket sessions open right now.'
                });
                const socketSubscriptions = meter.createObservableGauge(
                    'sigx.actors.socket.subscriptions',
                    { description: 'Live subscriptions held over sockets right now.' }
                );

                const instruments: Observable[] = [
                    calls,
                    callsFailed,
                    streams,
                    oneWayFailures,
                    errors,
                    created,
                    destroyed,
                    deactivations,
                    storageOps,
                    conflicts,
                    activations,
                    queued,
                    ...socketCounters.map(([instrument]) => instrument),
                    socketSessions,
                    socketSubscriptions
                ];

                type DistributionKey = 'latency' | 'queue' | 'turn' | 'storageLatency';
                const percentiles: Array<
                    [Observable, Observable, Observable, DistributionKey]
                > = [];
                if (percentileGauges) {
                    for (const [name, source] of [
                        ['call_duration', 'latency'],
                        ['turn_queue_duration', 'queue'],
                        ['turn_duration', 'turn'],
                        ['storage_operation_duration', 'storageLatency']
                    ] as const) {
                        const gauge = (quantile: string): Observable =>
                            meter.createObservableGauge(`sigx.actors.${name}.${quantile}`, {
                                description:
                                    `${quantile} of ${name} on THIS host, seconds. ` +
                                    'Not aggregatable across hosts.',
                                unit: 's'
                            });
                        const trio: [Observable, Observable, Observable, DistributionKey] = [
                            gauge('p50'),
                            gauge('p90'),
                            gauge('p99'),
                            source
                        ];
                        percentiles.push(trio);
                        instruments.push(trio[0], trio[1], trio[2]);
                    }
                }
                // The socket lifetime distribution, under the same switch and
                // the same caveat: THIS host's percentiles, not aggregatable.
                let socketLifetime: [Observable, Observable, Observable] | null = null;
                if (percentileGauges) {
                    const gauge = (quantile: string): Observable =>
                        meter.createObservableGauge(
                            `sigx.actors.socket.connection_duration.${quantile}`,
                            {
                                description:
                                    `${quantile} of socket session lifetime on THIS host, seconds. ` +
                                    'Not aggregatable across hosts.',
                                unit: 's'
                            }
                        );
                    socketLifetime = [gauge('p50'), gauge('p90'), gauge('p99')];
                    instruments.push(...socketLifetime);
                }

                const callback = (result: BatchObservableResult): void => {
                    const digest = registry.digest('metrics') as MetricsDigest | undefined;
                    if (digest === undefined) return;
                    for (const [type, row] of Object.entries(digest.byType)) {
                        result.observe(calls, row.calls, { type });
                        result.observe(callsFailed, row.failed, { type });
                    }
                    result.observe(streams, digest.calls.streams);
                    result.observe(oneWayFailures, digest.calls.oneWayFailures ?? 0);
                    for (const [kind, count] of Object.entries(digest.errors.byKind)) {
                        result.observe(errors, count as number, { kind });
                    }
                    result.observe(created, digest.activations.created);
                    result.observe(destroyed, digest.activations.destroyed);
                    for (const [reason, count] of Object.entries(digest.activations.byReason)) {
                        result.observe(deactivations, count, { reason });
                    }
                    result.observe(storageOps, digest.storage.loads, { op: 'load' });
                    result.observe(storageOps, digest.storage.saves, { op: 'save' });
                    result.observe(storageOps, digest.storage.clears, { op: 'clear' });
                    result.observe(conflicts, digest.storage.conflicts);
                    const stats = host?.stats();
                    if (stats) {
                        for (const [type, count] of Object.entries(stats.perType)) {
                            result.observe(activations, count, { type });
                        }
                        result.observe(queued, stats.queued);
                    }
                    for (const [p50, p90, p99, source] of percentiles) {
                        const snapshot = digestSnapshot(digest[source]);
                        if (snapshot.count === 0) continue;
                        result.observe(p50, snapshot.p50Ms / 1000);
                        result.observe(p90, snapshot.p90Ms / 1000);
                        result.observe(p99, snapshot.p99Ms / 1000);
                    }

                    // Absent is normal — a host with no socket mount — and
                    // observes nothing, so the families never appear for it.
                    // `undefined` is "no provider"; `null` is a provider with
                    // nothing to report. Both are absent.
                    const sockets = registry.digest('sockets') as SocketStatsDigest | null | undefined;
                    if (!sockets) return;
                    for (const [instrument, field] of socketCounters) {
                        result.observe(instrument, sockets[field]);
                    }
                    result.observe(
                        socketSessions,
                        sockets.connectionsOpened - sockets.connectionsClosed
                    );
                    result.observe(
                        socketSubscriptions,
                        sockets.subscriptionsOpened - sockets.subscriptionsClosed
                    );
                    if (socketLifetime && sockets.lifetime) {
                        const lifetime = digestSnapshot(sockets.lifetime);
                        if (lifetime.count > 0) {
                            result.observe(socketLifetime[0], lifetime.p50Ms / 1000);
                            result.observe(socketLifetime[1], lifetime.p90Ms / 1000);
                            result.observe(socketLifetime[2], lifetime.p99Ms / 1000);
                        }
                    }
                };

                meter.addBatchObservableCallback(callback, instruments);
                detach = () => meter.removeBatchObservableCallback(callback, instruments);
            });

            registry.onStop(() => {
                detach?.();
                detach = null;
                host = null;
            });
        }
    };
}
