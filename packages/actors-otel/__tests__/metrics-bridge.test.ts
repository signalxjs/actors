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
import { defineActorApp, metrics, type ActorApp, type MetricsDigest } from '@sigx/actors/host';
import { otelMetricsBridge } from '@sigx/actors-otel';

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

        const created = dataPoints(resourceMetrics, 'sigx.actors.activations.created');
        expect(created[0]?.value).toBe(digest.activations.created);

        // Percentile gauges: present, in seconds, on THIS host only.
        const p99 = dataPoints(resourceMetrics, 'sigx.actors.call_duration.p99');
        expect(p99).toHaveLength(1);
        expect(p99[0]!.value as number).toBeGreaterThanOrEqual(0);
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
