/**
 * The Prometheus renderer and its endpoint.
 *
 * The renderer is asserted against golden text from a hand-built digest —
 * known sparse buckets in, known cumulative `le` lines out — because the
 * exposition format is the contract a scraper parses. The endpoint tests
 * pin the `ops()` auth posture it copies.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor } from '@sigx/actors';
import {
    defineActorApp,
    metrics,
    type ActorApp,
    type ActorPlugin,
    type MetricsDigest
} from '@sigx/actors/host';
import {
    prometheusOps,
    renderPrometheus,
    type SocketStatsDigest
} from '@sigx/actors-otel/prometheus';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

/**
 * Hand-built: bucket 1 (bound 2µs) holds 5 samples, bucket 20 (values
 * [20µs, 21µs), so bound 21µs) holds 3. Against the default power-of-two
 * grid the cumulative counts are 0 at 1µs, 5 from 2µs through 16µs, and 8
 * from 32µs on.
 */
const digest: MetricsDigest = {
    v: 1,
    layout: 'll-4-26',
    windowMs: 60_000,
    calls: { total: 12, failed: 2, streams: 3, oneWayFailures: 2 },
    latency: { count: 8, sumUs: 1234, minUs: 1, maxUs: 20, idx: [1, 20], n: [5, 3] },
    queue: null,
    turn: null,
    storageLatency: null,
    errors: { byKind: { 'call-timeout': 1, '(unknown)': 1 } },
    activations: { created: 4, destroyed: 1, byReason: { idle: 1 } },
    storage: { loads: 4, saves: 3, clears: 0, conflicts: 1 },
    byType: { Counter: { calls: 10, failed: 1 }, '(other)': { calls: 2, failed: 1 } },
    byMethod: { 'Counter#increment': { calls: 10, failed: 1 }, '(other)': { calls: 2, failed: 1 } }
};

/**
 * A `socketStats().digest()` as the app would publish it (#166): 7 sessions
 * opened, 4 closed, so 3 open now; two lifetimes in bucket 1 (2µs) and two
 * in bucket 20 (21µs), the same shape as the metrics histogram above.
 */
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
    layout: 'll-4-26',
    lifetime: { count: 4, sumUs: 46, minUs: 2, maxUs: 21, idx: [1, 20], n: [2, 2] }
};

/** A plugin publishing a fixed socket digest, the way an app with a socket mount does. */
const socketsPlugin = (digest: SocketStatsDigest): ActorPlugin => ({
    name: 'fake-sockets',
    setup(registry) {
        registry.reportDigest('sockets', () => digest);
    }
});

describe('renderPrometheus', () => {
    it('renders counters, histograms and gauges from a known digest', () => {
        const text = renderPrometheus(
            digest,
            {
                activations: 3,
                queued: 2,
                perType: { Counter: 3 },
                transitional: { activating: 0, deactivating: 0 }
            },
            {}
        );
        const lines = text.split('\n');

        expect(lines).toContain('sigx_actors_calls_total{type="Counter"} 10');
        expect(lines).toContain('sigx_actors_calls_total{type="(other)"} 2');
        expect(lines).toContain('sigx_actors_calls_failed_total{type="Counter"} 1');
        expect(lines).toContain(
            'sigx_actors_method_calls_total{type="Counter",method="increment"} 10'
        );
        // The fold row has no `#`; both labels carry the fold marker.
        expect(lines).toContain(
            'sigx_actors_method_calls_total{type="(other)",method="(other)"} 2'
        );
        expect(lines).toContain('sigx_actors_streams_total 3');
        expect(lines).toContain('sigx_actors_one_way_failures_total 2');
        // A legacy digest without the field renders 0, not NaN.
        expect(
            renderPrometheus(
                { ...digest, calls: { total: 12, failed: 2, streams: 3 } },
                null,
                {}
            ).split('\n')
        ).toContain('sigx_actors_one_way_failures_total 0');
        expect(lines).toContain('sigx_actors_errors_total{kind="call-timeout"} 1');
        expect(lines).toContain('sigx_actors_activations_created_total 4');
        expect(lines).toContain('sigx_actors_deactivations_total{reason="idle"} 1');
        expect(lines).toContain('sigx_actors_storage_operations_total{op="load"} 4');
        expect(lines).toContain('sigx_actors_storage_conflicts_total 1');

        // The histogram: exact cumulative counts at native bounds.
        expect(lines).toContain('sigx_actors_call_duration_seconds_bucket{le="0.000001"} 0');
        expect(lines).toContain('sigx_actors_call_duration_seconds_bucket{le="0.000002"} 5');
        expect(lines).toContain('sigx_actors_call_duration_seconds_bucket{le="0.000016"} 5');
        expect(lines).toContain('sigx_actors_call_duration_seconds_bucket{le="0.000032"} 8');
        expect(lines).toContain('sigx_actors_call_duration_seconds_bucket{le="+Inf"} 8');
        expect(lines).toContain('sigx_actors_call_duration_seconds_sum 0.001234');
        expect(lines).toContain('sigx_actors_call_duration_seconds_count 8');
        // Null histograms render nothing.
        expect(text).not.toContain('turn_queue_duration');

        expect(lines).toContain('sigx_actors_activations{type="Counter"} 3');
        expect(lines).toContain('sigx_actors_queued_messages 2');
        expect(lines).toContain('sigx_actors_metrics_window_seconds 60');

        // HELP/TYPE exactly once per family.
        const helps = lines.filter((l) => l.startsWith('# HELP sigx_actors_calls_total '));
        expect(helps).toHaveLength(1);
        expect(lines).toContain('# TYPE sigx_actors_call_duration_seconds histogram');
    });

    it('renders the socket families from the sockets digest (#166)', () => {
        const text = renderPrometheus(digest, null, { sockets: socketDigest });
        const lines = text.split('\n');

        // Counters, one-to-one with the totals.
        expect(lines).toContain('sigx_actors_socket_connections_opened_total 7');
        expect(lines).toContain('sigx_actors_socket_connections_closed_total 4');
        expect(lines).toContain('sigx_actors_socket_connections_refused_total 2');
        expect(lines).toContain('sigx_actors_socket_calls_total 40');
        expect(lines).toContain('sigx_actors_socket_calls_failed_total 3');
        expect(lines).toContain('sigx_actors_socket_subscriptions_opened_total 9');
        expect(lines).toContain('sigx_actors_socket_subscriptions_closed_total 5');
        expect(lines).toContain('sigx_actors_socket_protocol_breaches_total 1');
        expect(lines).toContain('sigx_actors_socket_lifetime_closes_total 2');
        expect(lines).toContain('sigx_actors_socket_deliveries_total 1200');
        expect(lines).toContain('sigx_actors_socket_delivery_bytes_total 48000');
        expect(lines).toContain('sigx_actors_socket_throttle_quantized_total 6');
        expect(lines).toContain('# TYPE sigx_actors_socket_deliveries_total counter');
        // The unit caveat is on the family, not hidden in a README.
        expect(text).toContain('UTF-16 code units');

        // Gauges: opened − closed IS the live count.
        expect(lines).toContain('# TYPE sigx_actors_socket_sessions gauge');
        expect(lines).toContain('sigx_actors_socket_sessions 3');
        expect(lines).toContain('sigx_actors_socket_subscriptions 4');

        // The lifetime histogram, on the same exact-at-native-bounds grid.
        expect(lines).toContain('# TYPE sigx_actors_socket_connection_duration_seconds histogram');
        expect(lines).toContain('sigx_actors_socket_connection_duration_seconds_bucket{le="0.000001"} 0');
        expect(lines).toContain('sigx_actors_socket_connection_duration_seconds_bucket{le="0.000002"} 2');
        expect(lines).toContain('sigx_actors_socket_connection_duration_seconds_bucket{le="0.000032"} 4');
        expect(lines).toContain('sigx_actors_socket_connection_duration_seconds_bucket{le="+Inf"} 4');
        expect(lines).toContain('sigx_actors_socket_connection_duration_seconds_count 4');
    });

    it('emits no socket family at all without a sockets digest', () => {
        // A host with no socket mount is not a host with zero connections.
        expect(renderPrometheus(digest, null, {})).not.toContain('socket');
    });

    it('renders socket counters but no lifetime histogram before any session closed', () => {
        const text = renderPrometheus(digest, null, { sockets: { ...socketDigest, lifetime: null } });
        expect(text).toContain('sigx_actors_socket_connections_opened_total 7');
        expect(text).not.toContain('socket_connection_duration');
    });

    it('checks the sockets digest layout on its own', () => {
        // The two digests come from different providers; a foreign layout
        // on one must not cost the other its distribution, and vice versa.
        const text = renderPrometheus(digest, null, { sockets: { ...socketDigest, layout: 'll-5-30' } });
        expect(text).toContain('sigx_actors_call_duration_seconds_count 8');
        expect(text).toContain('sigx_actors_socket_sessions 3');
        expect(text).not.toContain('socket_connection_duration');
    });

    it('degrades an unknown layout to counters only', () => {
        const foreign = { ...digest, layout: 'll-5-30' };
        const text = renderPrometheus(foreign, null, {});
        expect(text).toContain('sigx_actors_calls_total{type="Counter"} 10');
        // The same counts-only answer the accumulator gives: no distribution
        // drawn on an axis this build cannot name.
        expect(text).not.toContain('call_duration_seconds');
    });

    it('escapes label values', () => {
        const hostile = {
            ...digest,
            byType: { 'we"ird\\type\nx': { calls: 1, failed: 0 } },
            byMethod: {}
        };
        const text = renderPrometheus(hostile, null, {});
        expect(text).toContain('sigx_actors_calls_total{type="we\\"ird\\\\type\\nx"} 1');
    });

    it('snaps a requested grid down to native bounds', () => {
        // 0.00002s = 20µs is NOT a native bound (octave 4 has 1µs buckets up
        // to 32µs... bounds are integers; 20µs IS one). Use 0.0000205 —
        // between the 20µs and 21µs native bounds — and expect 20µs.
        const text = renderPrometheus(digest, null, { bucketsSeconds: [0.0000205] });
        expect(text).toContain('sigx_actors_call_duration_seconds_bucket{le="0.00002"} 5');
        expect(text).toContain('sigx_actors_call_duration_seconds_bucket{le="+Inf"} 8');
    });
});

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

async function startApp(secret?: string, withMetrics = true, sockets?: SocketStatsDigest) {
    let app = defineActorApp({ actors: [Counter], defaults: quiet });
    if (withMetrics) app = app.use(metrics());
    if (sockets) app = app.use(socketsPlugin(sockets));
    app = app.use(prometheusOps(secret !== undefined ? { secret } : {}));
    running = app;
    const host = await app.start();
    const route = app.routes.find((r) => r.name === 'prometheus-ops')!;
    return { app, host, route };
}

const get = (route: { handle(rq: Request, host: never): Promise<Response> }, headers = {}) =>
    route.handle(new Request('http://host.test/_sigx/metrics', { headers }), null as never);

describe('prometheusOps endpoint', () => {
    it('serves the exposition with a valid bearer', async () => {
        const { host, route } = await startApp('s3cret');
        await host.actor(Counter, 'k1').increment(1);
        const response = await get(route, { authorization: 'Bearer s3cret' });
        expect(response.status).toBe(200);
        expect(response.headers.get('content-type')).toBe(
            'text/plain; version=0.0.4; charset=utf-8'
        );
        expect(response.headers.get('cache-control')).toBe('no-store');
        const text = await response.text();
        expect(text).toContain('sigx_actors_calls_total{type="Counter"} 1');
        expect(text).toContain('sigx_actors_activations{type="Counter"} 1');
    });

    it('reads the sockets digest through the registry, beside metrics (#166)', async () => {
        const { route } = await startApp('s3cret', true, socketDigest);
        const response = await get(route, { authorization: 'Bearer s3cret' });
        expect(response.status).toBe(200);
        const text = await response.text();
        expect(text).toContain('sigx_actors_socket_sessions 3');
        expect(text).toContain('sigx_actors_socket_deliveries_total 1200');
        // And the metrics families are still there — a second read, not a
        // replacement.
        expect(text).toContain('sigx_actors_metrics_window_seconds');
    });

    it('answers one 401 for missing and wrong bearer alike', async () => {
        const { route } = await startApp('s3cret');
        for (const headers of [{}, { authorization: 'Bearer wrong' }]) {
            const response = await get(route, headers);
            expect(response.status).toBe(401);
            expect(response.headers.get('www-authenticate')).toBe('Bearer');
        }
    });

    it('is GET/HEAD only', async () => {
        const { route } = await startApp('s3cret');
        const response = await route.handle(
            new Request('http://host.test/_sigx/metrics', { method: 'POST' }),
            null as never
        );
        expect(response.status).toBe(405);
        expect(response.headers.get('allow')).toBe('GET, HEAD');
    });

    it('answers 503, naming the fix, when metrics() is absent', async () => {
        const { route } = await startApp('s3cret', false);
        const response = await get(route, { authorization: 'Bearer s3cret' });
        expect(response.status).toBe(503);
        expect(await response.text()).toContain('metrics()');
    });

    it('serves unauthenticated in dev, with a warning', async () => {
        // Outside __DEV__ the constructor throws instead — that branch is a
        // string comparison this test suite runs with __DEV__ true.
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            const { route } = await startApp();
            expect(warn).toHaveBeenCalledWith(expect.stringContaining('UNAUTHENTICATED'));
            const response = await get(route);
            expect(response.status).toBe(200);
        } finally {
            warn.mockRestore();
        }
    });
});
