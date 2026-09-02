/**
 * `@sigx/actors-otel` — the two metrics exporters mounted on the same
 * three-host cluster, then read back: one Prometheus scrape per host, and
 * one OpenTelemetry collection per host, both over the digest `metrics()`
 * already keeps.
 *
 *     pnpm --filter providers-example otel           # after `pnpm build`
 *
 * `prometheusOps()` is a route beside `ops()` with the same bearer posture
 * — no secret outside development is a construction error, not a warning.
 * `otelMetricsBridge()` registers observables against a `MeterProvider`
 * and reads the digest ONCE per collection; the SDK's reader owns the
 * period. Here that provider is in-memory so the demo can read the
 * collection back; a deployment hands it an OTLP exporter instead and
 * changes nothing else.
 */
import {
    AggregationTemporality,
    InMemoryMetricExporter,
    MeterProvider,
    PeriodicExportingMetricReader
} from '@opentelemetry/sdk-metrics';
import { memoryStorage } from '@sigx/actors/host';
import { memoryClusterHub } from '@sigx/actors/cluster';
import { otelMetricsBridge } from '@sigx/actors-otel';
import { prometheusOps } from '@sigx/actors-otel/prometheus';
import { startCluster } from './src/cluster.ts';
import { parseExposition, sampleValue } from './src/prometheus.ts';

const opsSecret = process.env.PROVIDERS_DEMO_OPS_SECRET ?? 'demo-ops-secret';
const hub = memoryClusterHub();
/** One provider + reader PER HOST: a meter's instruments are keyed by
 *  name, and three hosts observing `sigx.actors.calls` on one meter would
 *  be three callbacks fighting over one time series. */
const readers = [];
const providers = [];

const demo = await startCluster({
    label: 'memoryStorage()',
    storage: memoryStorage(),
    providers: () => hub.providers(),
    opsSecret,
    plugins: (index) => {
        const reader = new PeriodicExportingMetricReader({
            exporter: new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE),
            // Effectively manual: the demo calls `collect()` itself.
            exportIntervalMillis: 3_600_000
        });
        const provider = new MeterProvider({ readers: [reader] });
        readers[index] = reader;
        providers[index] = provider;
        return [
            // Same secret as `ops()`, mounted at /_sigx/metrics beside it.
            prometheusOps({ secret: opsSecret }),
            // Register the provider BEFORE app.start(): unlike traces, the
            // OTel api has no upgrading proxy for meters.
            otelMetricsBridge({ meterProvider: provider })
        ];
    }
});

await demo.spread();
const { owner } = await demo.singleActivation();
await demo.crossHost(owner);

demo.step('Prometheus — one scrape per host');
const scrape = async (m, bearer = true) =>
    fetch(`http://127.0.0.1:${m.port}/_sigx/metrics`, {
        headers: bearer ? { authorization: `Bearer ${opsSecret}` } : {}
    });
const unauthenticated = await scrape(demo.members[0], false);
console.log(`GET /_sigx/metrics without a bearer → ${unauthenticated.status}`);
if (unauthenticated.status !== 401) throw new Error('the scrape served without a token!');

const perHost = new Map(); // hostId → calls_total{type="Counter"} on that host
let shownLines = false;
for (const m of demo.members) {
    const response = await scrape(m);
    const text = await response.text();
    const samples = parseExposition(text);
    const calls = sampleValue(samples, 'sigx_actors_calls_total', { type: 'Counter' });
    const increments = sampleValue(samples, 'sigx_actors_method_calls_total', {
        type: 'Counter',
        method: 'increment'
    });
    const observed = sampleValue(samples, 'sigx_actors_call_duration_seconds_count');
    const activations = sampleValue(samples, 'sigx_actors_activations', { type: 'Counter' });
    if (calls === null || increments === null) throw new Error(`no Counter samples on ${demo.hostId(m)}`);
    perHost.set(demo.hostId(m), calls);
    console.log(
        `${demo.hostId(m)} (:${m.port}) → ${response.status} ${response.headers.get('content-type')}: ` +
            `${samples.length} samples; calls_total{type="Counter"}=${calls} ` +
            `method_calls_total{method="increment"}=${increments} ` +
            `call_duration_seconds_count=${observed} activations{type="Counter"}=${activations}`
    );
    if (!shownLines) {
        // The raw text, once — the histogram is the part worth seeing: real
        // buckets from the digest's log-linear layout, not a summary.
        shownLines = true;
        const interesting = text
            .split('\n')
            .filter(
                (line) =>
                    line.startsWith('# TYPE sigx_actors_call_duration_seconds') ||
                    line.startsWith('sigx_actors_calls_total') ||
                    line.startsWith('sigx_actors_call_duration_seconds_bucket{le="0.001')  ||
                    line.startsWith('sigx_actors_call_duration_seconds_bucket{le="+Inf') ||
                    line.startsWith('sigx_actors_call_duration_seconds_sum') ||
                    line.startsWith('sigx_actors_call_duration_seconds_count')
            );
        console.log(`  the exposition, excerpted:\n    ${interesting.join('\n    ')}`);
        console.log(
            `  curl -H 'Authorization: Bearer ${opsSecret}' http://127.0.0.1:${m.port}/_sigx/metrics`
        );
    }
}
const total = [...perHost.values()].reduce((a, b) => a + b, 0);
console.log(
    `sum over hosts: ${total} calls — a cross-host call is metered where it ENTERED and ` +
        `where it RAN, so this is the out+in rule of clusterStats, not double counting`
);

demo.step('OpenTelemetry — one collection per host, same digest');
for (const m of demo.members) {
    const { resourceMetrics, errors } = await readers[m.index].collect();
    if (errors.length) throw errors[0];
    const metric = resourceMetrics.scopeMetrics
        .flatMap((scope) => scope.metrics)
        .find((metric) => metric.descriptor.name === 'sigx.actors.calls');
    const point = metric?.dataPoints.find((p) => p.attributes.type === 'Counter');
    const otelCalls = point?.value ?? null;
    const scraped = perHost.get(demo.hostId(m));
    console.log(
        `${demo.hostId(m)}: sigx.actors.calls{type="Counter"}=${otelCalls}  ` +
            `(Prometheus said ${scraped}; scope "${resourceMetrics.scopeMetrics[0]?.scope.name}", ` +
            `${resourceMetrics.scopeMetrics[0]?.metrics.length} instruments)`
    );
    if (otelCalls !== scraped) throw new Error('the two exporters disagree about one digest!');
}
console.log('(distributions stay with Prometheus: the OTel metrics API cannot ingest pre-bucketed histograms)');

const { survivor } = await demo.failover(owner);
await demo.report(survivor);
await demo.stop();
await Promise.all(providers.map((p) => p.shutdown()));
console.log('\nOTEL DEMO COMPLETE — one digest, two exporters, three hosts.');
