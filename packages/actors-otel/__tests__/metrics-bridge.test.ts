/**
 * `otelMetricsBridge()` — observables over the runtime digest, one digest
 * read per collection, values matching what `metrics().digest()` itself
 * answers, and the callback detached on stop.
 */
import { afterEach, describe, expect, it } from 'vitest';
import {
    AggregationTemporality,
    InMemoryMetricExporter,
    MeterProvider,
    PeriodicExportingMetricReader,
    type ResourceMetrics
} from '@opentelemetry/sdk-metrics';
import { defineActor } from '@sigx/actors';
import type { ClusterCounterTotals } from '@sigx/actors/cluster';
import {
    defineActorApp,
    metrics,
    type ActorApp,
    type ActorPlugin,
    type MetricsDigest
} from '@sigx/actors/host';
import { otelMetricsBridge, type SocketStatsDigest } from '@sigx/actors-otel';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const Counter = defineActor({
    type: 'Counter',
    allowAnonymous: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        }
    })
});

/** A `socketStats().digest()` as an app with a socket mount publishes it (#166). */
const socketDigest: SocketStatsDigest = {
    connectionsOpened: 7,
    connectionsClosed: 4,
    connectionsRefused: 2,
    callsStarted: 40,
    callsFailed: 3,
    subscriptionsOpened: 9,
    subscriptionsClosed: 5,
    protocolBreaches: 1,
    lifetimeCloses: 2,
    deliveries: 1200,
    deliveryBytes: 48_000,
    throttleQuantized: 6,
    subscriptionsShed: 3,
    layout: 'll-4-26',
    lifetime: { count: 4, sumUs: 46, minUs: 2, maxUs: 21, idx: [1, 20], n: [2, 2] }
};

/** A placement's `counters()` (#52, #346): 90 served locally, 10 by a peer. */
const clusterCounters: Partial<ClusterCounterTotals> = {
    dispatchesLocal: 90,
    dispatchesRemote: 10,
    routedLocal: 4,
    remoteDispatches: 12
};

const socketsPlugin = (digest: SocketStatsDigest): ActorPlugin => ({
    name: 'fake-sockets',
    setup(registry) {
        registry.reportDigest('sockets', () => digest);
    }
});

let running: ActorApp | null = null;
afterEach(async () => {
    await running?.stop();
    running = null;
});

function dataPoints(resourceMetrics: ResourceMetrics, name: string) {
    for (const scope of resourceMetrics.scopeMetrics) {
        for (const metric of scope.metrics) {
            if (metric.descriptor.name === name) return metric.dataPoints;
        }
    }
    return [];
}

describe('otelMetricsBridge', () => {
    it('observes the digest counters and gauges, matching metrics() itself', async () => {
        const reader = new PeriodicExportingMetricReader({
            exporter: new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE),
            exportIntervalMillis: 3_600_000
        });
        const provider = new MeterProvider({ readers: [reader] });
        const collector = metrics();
        const app = defineActorApp({ actors: [Counter], defaults: quiet })
            .use(collector)
            .use(otelMetricsBridge({ meterProvider: provider }));
        running = app;
        const host = await app.start();

        await host.actor(Counter, 'k1').increment(1);
        await host.actor(Counter, 'k2').increment(1);

        const { resourceMetrics } = await reader.collect();
        const digest = collector.digest() as MetricsDigest;

        const calls = dataPoints(resourceMetrics, 'sigx.actors.calls');
        const counter = calls.find((p) => p.attributes['type'] === 'Counter')!;
        expect(counter.value).toBe(digest.byType['Counter']!.calls);
        expect(counter.value).toBe(2);

        const activations = dataPoints(resourceMetrics, 'sigx.actors.activations');
        expect(activations.find((p) => p.attributes['type'] === 'Counter')?.value).toBe(2);

        // No `cluster` option → no cluster family at all (#346).
        expect(dataPoints(resourceMetrics, 'sigx.actors.cluster.dispatches')).toHaveLength(0);

        const created = dataPoints(resourceMetrics, 'sigx.actors.activations.created');
        expect(created[0]?.value).toBe(digest.activations.created);

        // One-way failures observe 0 (not absent) when none happened, so a
        // dashboard can alert on the counter existing and rising.
        const oneWay = dataPoints(resourceMetrics, 'sigx.actors.calls.one_way_failures');
        expect(oneWay[0]?.value).toBe(digest.calls.oneWayFailures ?? 0);

        // Percentile gauges: present, in seconds, on THIS host only.
        const p99 = dataPoints(resourceMetrics, 'sigx.actors.call_duration.p99');
        expect(p99).toHaveLength(1);
        expect(p99[0]!.value as number).toBeGreaterThanOrEqual(0);
    });

    it('observes the sockets digest beside the metrics one (#166)', async () => {
        const reader = new PeriodicExportingMetricReader({
            exporter: new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE),
            exportIntervalMillis: 3_600_000
        });
        const provider = new MeterProvider({ readers: [reader] });
        const app = defineActorApp({ actors: [Counter], defaults: quiet })
            .use(metrics())
            .use(socketsPlugin(socketDigest))
            .use(otelMetricsBridge({ meterProvider: provider }));
        running = app;
        await app.start();

        const { resourceMetrics } = await reader.collect();
        expect(dataPoints(resourceMetrics, 'sigx.actors.socket.connections.opened')[0]?.value).toBe(7);
        expect(dataPoints(resourceMetrics, 'sigx.actors.socket.deliveries')[0]?.value).toBe(1200);
        expect(dataPoints(resourceMetrics, 'sigx.actors.socket.delivery_bytes')[0]?.value).toBe(48_000);
        // Derived gauges: opened − closed is the live count.
        expect(dataPoints(resourceMetrics, 'sigx.actors.socket.sessions')[0]?.value).toBe(3);
        expect(dataPoints(resourceMetrics, 'sigx.actors.socket.subscriptions')[0]?.value).toBe(4);
        // Lifetime percentiles, seconds, this host only.
        const p99 = dataPoints(resourceMetrics, 'sigx.actors.socket.connection_duration.p99');
        expect(p99).toHaveLength(1);
        expect(p99[0]!.value as number).toBeGreaterThan(0);
        // The metrics families are still observed — a second read, not a
        // replacement.
        expect(dataPoints(resourceMetrics, 'sigx.actors.activations.created')).toHaveLength(1);
    });

    it('observes no socket family when no sockets digest is published', async () => {
        const reader = new PeriodicExportingMetricReader({
            exporter: new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE),
            exportIntervalMillis: 3_600_000
        });
        const provider = new MeterProvider({ readers: [reader] });
        const app = defineActorApp({ actors: [Counter], defaults: quiet })
            .use(metrics())
            .use(otelMetricsBridge({ meterProvider: provider }));
        running = app;
        const host = await app.start();
        await host.actor(Counter, 'k1').increment(1);

        const { resourceMetrics } = await reader.collect();
        expect(dataPoints(resourceMetrics, 'sigx.actors.calls')).not.toHaveLength(0);
        // A host with no socket mount is not a host with zero sockets.
        expect(dataPoints(resourceMetrics, 'sigx.actors.socket.sessions')).toHaveLength(0);
        expect(dataPoints(resourceMetrics, 'sigx.actors.socket.connections.opened')).toHaveLength(0);
    });

    it('treats a null sockets digest as absent, not as a digest to read', async () => {
        const reader = new PeriodicExportingMetricReader({
            exporter: new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE),
            exportIntervalMillis: 3_600_000
        });
        const provider = new MeterProvider({ readers: [reader] });
        // `registry.digest()` answers whatever the provider returned — a
        // provider that says `null` is "nothing to report", and must not
        // become a field read off `null` inside the observable callback.
        const nullSockets: ActorPlugin = {
            name: 'null-sockets',
            setup(registry) {
                registry.reportDigest('sockets', () => null);
            }
        };
        const app = defineActorApp({ actors: [Counter], defaults: quiet })
            .use(metrics())
            .use(nullSockets)
            .use(otelMetricsBridge({ meterProvider: provider }));
        running = app;
        const host = await app.start();
        await host.actor(Counter, 'k1').increment(1);

        const { resourceMetrics, errors } = await reader.collect();
        expect(errors).toEqual([]);
        expect(dataPoints(resourceMetrics, 'sigx.actors.calls')).not.toHaveLength(0);
        expect(dataPoints(resourceMetrics, 'sigx.actors.socket.sessions')).toHaveLength(0);
    });

    it('observes the cluster dispatch pair by locality through the thunk (#346)', async () => {
        const reader = new PeriodicExportingMetricReader({
            exporter: new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE),
            exportIntervalMillis: 3_600_000
        });
        const provider = new MeterProvider({ readers: [reader] });
        let totals: Partial<ClusterCounterTotals> | undefined = clusterCounters;
        const placement = { counters: () => totals };
        const app = defineActorApp({ actors: [Counter], defaults: quiet })
            .use(metrics())
            .use(otelMetricsBridge({ meterProvider: provider, cluster: () => placement.counters() }));
        running = app;
        await app.start();

        let { resourceMetrics } = await reader.collect();
        let points = dataPoints(resourceMetrics, 'sigx.actors.cluster.dispatches');
        expect(points.find((p) => p.attributes['locality'] === 'local')?.value).toBe(90);
        expect(points.find((p) => p.attributes['locality'] === 'remote')?.value).toBe(10);
        expect(points).toHaveLength(2);
        // The metrics families are still observed — an addition, not a
        // replacement.
        expect(dataPoints(resourceMetrics, 'sigx.actors.activations.created')).toHaveLength(1);

        // Read on every collection: the counters move.
        totals = { ...clusterCounters, dispatchesLocal: 91 };
        ({ resourceMetrics } = await reader.collect());
        points = dataPoints(resourceMetrics, 'sigx.actors.cluster.dispatches');
        expect(points.find((p) => p.attributes['locality'] === 'local')?.value).toBe(91);
    });

    it('observes 0, not NaN, for cluster totals predating the pair', async () => {
        const reader = new PeriodicExportingMetricReader({
            exporter: new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE),
            exportIntervalMillis: 3_600_000
        });
        const provider = new MeterProvider({ readers: [reader] });
        const legacy: Partial<ClusterCounterTotals> = { routedLocal: 4, remoteDispatches: 12 };
        const app = defineActorApp({ actors: [Counter], defaults: quiet })
            .use(metrics())
            .use(otelMetricsBridge({ meterProvider: provider, cluster: () => legacy }));
        running = app;
        await app.start();

        const { resourceMetrics, errors } = await reader.collect();
        expect(errors).toEqual([]);
        const points = dataPoints(resourceMetrics, 'sigx.actors.cluster.dispatches');
        expect(points.find((p) => p.attributes['locality'] === 'local')?.value).toBe(0);
        expect(points.find((p) => p.attributes['locality'] === 'remote')?.value).toBe(0);
    });

    it('observes no cluster family without a cluster thunk, or when it answers undefined', async () => {
        const reader = new PeriodicExportingMetricReader({
            exporter: new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE),
            exportIntervalMillis: 3_600_000
        });
        const provider = new MeterProvider({ readers: [reader] });
        const app = defineActorApp({ actors: [Counter], defaults: quiet })
            .use(metrics())
            .use(otelMetricsBridge({ meterProvider: provider, cluster: () => undefined }));
        running = app;
        const host = await app.start();
        await host.actor(Counter, 'k1').increment(1);

        const { resourceMetrics, errors } = await reader.collect();
        expect(errors).toEqual([]);
        expect(dataPoints(resourceMetrics, 'sigx.actors.calls')).not.toHaveLength(0);
        // A single host is not a cluster with zero dispatches.
        expect(dataPoints(resourceMetrics, 'sigx.actors.cluster.dispatches')).toHaveLength(0);
    });

    it('detaches the callback on stop', async () => {
        const reader = new PeriodicExportingMetricReader({
            exporter: new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE),
            exportIntervalMillis: 3_600_000
        });
        const provider = new MeterProvider({ readers: [reader] });
        const app = defineActorApp({ actors: [Counter], defaults: quiet })
            .use(metrics())
            .use(otelMetricsBridge({ meterProvider: provider }));
        running = app;
        const host = await app.start();
        await host.actor(Counter, 'k1').increment(1);

        await app.stop();
        running = null;

        const { resourceMetrics } = await reader.collect();
        // No callback → no observations for this collection.
        expect(dataPoints(resourceMetrics, 'sigx.actors.calls')).toHaveLength(0);
    });

    it('respects percentileGauges: false', async () => {
        const reader = new PeriodicExportingMetricReader({
            exporter: new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE),
            exportIntervalMillis: 3_600_000
        });
        const provider = new MeterProvider({ readers: [reader] });
        const app = defineActorApp({ actors: [Counter], defaults: quiet })
            .use(metrics())
            .use(otelMetricsBridge({ meterProvider: provider, percentileGauges: false }));
        running = app;
        const host = await app.start();
        await host.actor(Counter, 'k1').increment(1);

        const { resourceMetrics } = await reader.collect();
        expect(dataPoints(resourceMetrics, 'sigx.actors.calls')).not.toHaveLength(0);
        expect(dataPoints(resourceMetrics, 'sigx.actors.call_duration.p99')).toHaveLength(0);
    });
});
