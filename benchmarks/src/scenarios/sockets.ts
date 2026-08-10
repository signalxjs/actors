/**
 * Tier 3 — WebSocket connection scale (#172/#184).
 *
 * A different axis from `infra/*`, and the difference is not cosmetic.
 * `infra/*` measures ops/s over the PUBLIC endpoint from a same-region VM:
 * TLS, an ingress, an edge hash. These measure CONNECTIONS HELD and
 * MESSAGES DELIVERED, driven from Jobs INSIDE the cluster straight at the
 * Service. No ingress, no TLS, no load VM — because the question is what
 * the runtime can hold and push, not what the edge can carry. **The two
 * are not comparable and must never be quoted as one tier.**
 *
 * Opt-in twice over, because each run is a real Job on a real cluster:
 *
 *   BENCH_WS=1 INFRA_WS_CONTEXT=… INFRA_WS_IMAGE=… pnpm bench:ws
 *
 * `testenv.mjs ws-bench` assembles that env from the live release, which is
 * also where `INFRA_SHAPE` comes from — and the shape here has to describe
 * the ACTORS release including its socket caps, or a run under a different
 * `SOCKET_MAX_SUBSCRIPTIONS` compares as if it were the same deployment.
 *
 * **Give it `--runs=1`.** The harness discards a warmup run before the
 * measured ones, so `--runs=3` is four full ladders on a paid cluster. The
 * same is already true of `infra/*`; it matters more here because a rung
 * holds tens of thousands of sockets for its whole duration.
 *
 * No metric is `exact`, and none can be — the same contract the rest of
 * Tier 3 lives under. Connection counts look deterministic and are not: a
 * generator pod caps at its own ephemeral port range, and the pods are not
 * barrier-synchronised, so what is achieved depends on the machine as much
 * as on the code.
 */
import { fileURLToPath } from 'node:url';
import { runWsLoad } from '../../../perf/aks/deploy/ws-load.mjs';
import type { Metric, RunContext, Scenario } from '../types.ts';

const CONTEXT = process.env.INFRA_WS_CONTEXT ?? process.env.INFRA_CONTEXT ?? '';
const NAMESPACE = process.env.INFRA_WS_NS ?? 'sigx-actors-test';
const RELEASE = process.env.INFRA_WS_RELEASE ?? 'sigx';
const IMAGE = process.env.INFRA_WS_IMAGE ?? '';
const IMAGE_TAG = process.env.INFRA_WS_IMAGE_TAG ?? '';
const WORKLOAD = process.env.INFRA_WS_WORKLOAD ?? 'sigx-actors-test';

const CHART = fileURLToPath(new URL('../../../perf/aks/deploy/chart', import.meta.url));

/**
 * Gated on the image as well as the cluster: without a tag the Job renders
 * against whatever `gitSha()` says, which after any merge is an image that
 * does not exist in the registry — an ImagePullBackOff that looks exactly
 * like a cluster problem.
 */
export const WS_ENABLED =
    process.env.BENCH_WS === '1' && CONTEXT !== '' && IMAGE !== '' && IMAGE_TAG !== '';

export function wsHint(): string | null {
    if (process.env.BENCH_WS !== '1') return 'set BENCH_WS=1 (and see `testenv.mjs ws-bench`)';
    const missing = [
        CONTEXT === '' ? 'INFRA_WS_CONTEXT' : null,
        IMAGE === '' ? 'INFRA_WS_IMAGE' : null,
        IMAGE_TAG === '' ? 'INFRA_WS_IMAGE_TAG' : null
    ].filter(Boolean);
    return missing.length > 0 ? `missing ${missing.join(', ')}` : null;
}

/** Parse a comma ladder, keeping the caller's order. */
const ladder = (name: string, fallback: string): number[] =>
    (process.env[name] ?? fallback)
        .split(',')
        .map((s) => Number(s.trim()))
        .filter((n) => Number.isFinite(n) && n > 0);

interface Row {
    n: number;
    connected: number;
    connectFailures: number;
    deliveries: number;
    deliveriesPerSec: number;
    deliveriesPerPublish: number;
    publishes: number;
    publishFailures: number;
    subscriptionErrors: number;
    maxBufferedBytes: number;
    latencyMs: { p50: number; p90: number; p99: number; max: number } | null;
    partial?: boolean;
}

interface RunResult {
    merged: Row[];
    peakOpen: number;
    peakSubscriptions: number;
    hosts: number;
    delta: Record<string, number>;
    /** The cluster stats fan-out missed a host — watch counts are a lower bound. */
    watchesPartial?: boolean;
    partial: boolean;
}

async function drive(values: Record<string, unknown>): Promise<RunResult> {
    return (await runWsLoad({
        context: CONTEXT,
        namespace: NAMESPACE,
        release: RELEASE,
        chartDir: CHART,
        imageRepository: IMAGE,
        imageTag: IMAGE_TAG,
        workload: WORKLOAD,
        values
    })) as RunResult;
}

/**
 * Per-rung metrics. `error_rate` carries the 1% noise floor for the same
 * reason it does in `infra/*`: a broken run answers faster than a working
 * one, so without a floor a failure reads as an improvement.
 */
function rowMetrics(row: Row, prefix = ''): Metric[] {
    const at = prefix || `n=${row.n}/`;
    const attempted = row.connected + row.connectFailures;
    const metrics: Metric[] = [
        { name: `${at}connected`, value: row.connected, unit: 'count', direction: 'higher' },
        {
            name: `${at}connect_failure_rate`,
            value: attempted > 0 ? row.connectFailures / attempted : 0,
            unit: 'ratio',
            direction: 'lower',
            noiseFloor: 0.01
        },
        {
            name: `${at}deliveries_per_sec`,
            value: row.deliveriesPerSec,
            unit: 'msg/s',
            direction: 'higher'
        },
        // A COALESCING RATIO, not a constant: client subscriptions run at
        // the runtime's fixed 50 ms watch throttle, so above ~20 publishes/s
        // per key this falls below the subscriber count BY DESIGN. Reading
        // it as loss is a misreading, hence informational.
        {
            name: `${at}deliveries_per_publish`,
            value: row.deliveriesPerPublish,
            unit: 'count',
            direction: 'higher',
            informational: true
        },
        {
            name: `${at}max_buffered_bytes`,
            value: row.maxBufferedBytes,
            unit: 'bytes',
            direction: 'lower',
            informational: true,
            noiseFloor: 1
        }
    ];
    if (row.latencyMs) {
        metrics.push(
            {
                name: `${at}delivery_p50_ms`,
                value: row.latencyMs.p50,
                unit: 'ms',
                direction: 'lower'
            },
            {
                name: `${at}delivery_p99_ms`,
                value: row.latencyMs.p99,
                unit: 'ms',
                direction: 'lower',
                informational: true
            }
        );
    }
    return metrics;
}

/** Run-level counters every scenario reports the same way. */
function runMetrics(result: RunResult): Metric[] {
    return [
        // The number to quote for concurrency: the `open` GAUGE sampled off
        // the HOSTS, not the generator's claim. Sampled, so it under-reports
        // a peak that fell between polls — and the generator's pods are not
        // barrier-synchronised, so its own sum over-reports simultaneity.
        { name: 'peak_open', value: result.peakOpen, unit: 'count', direction: 'higher' },
        {
            name: 'peak_subscriptions',
            value: result.peakSubscriptions,
            unit: 'count',
            direction: 'higher',
            informational: true
        },
        {
            name: 'protocol_breaches',
            value: result.delta.protocolBreaches ?? 0,
            unit: 'count',
            direction: 'lower',
            noiseFloor: 1
        },
        ...watchMetrics(result)
    ];
}

/**
 * The MECHANISM counters (#202), when the run carried them.
 *
 * `remote_watch_streams` is the number this whole axis turns on: with a
 * per-principal key it tracks the identity population and each one pins a
 * pooled host-to-host connection, which is what put the ceiling at the
 * fetch pool's arithmetic (#194). On a `principalIndependent` read (#138)
 * it should stay flat as identities grow.
 *
 * Informational, never `exact` and never gating: placement is `random` on
 * this deployment, sockets are host-affine, and the count depends on how
 * subscribers landed across pods. It is here to ATTRIBUTE a cliff, not to
 * pin an invariant — `cluster/live-fanout` does that at Tier 1.
 *
 * Absent rather than zero when the run did not collect them: a zero here
 * would read as "no cross-host streams", which is a claim about the
 * runtime, not about the harness.
 */
function watchMetrics(result: RunResult): Metric[] {
    const opened = result.delta['cluster/remoteWatches'];
    if (opened === undefined) return [];
    // A missed member makes every count a lower bound. Renaming rather than
    // footnoting it: the metric name is all a comparison carries forward, so
    // a lower bound must not sit in the same row as a complete count.
    const at = result.watchesPartial ? 'lower_bound/' : '';
    return [
        {
            name: `${at}remote_watch_streams`,
            value: opened,
            unit: 'count',
            direction: 'lower',
            informational: true
        },
        {
            name: `${at}coalesced_watch_joins`,
            value: result.delta['cluster/coalescedWatches'] ?? 0,
            unit: 'count',
            direction: 'higher',
            informational: true
        },
        {
            name: `${at}inbound_watch_streams`,
            value: result.delta['cluster/inboundWatches'] ?? 0,
            unit: 'count',
            direction: 'lower',
            informational: true
        }
    ];
}

/** A partial run is not a slow run — it is a different measurement. */
function refusePartial(result: RunResult, scenario: string): void {
    if (result.partial) {
        throw new Error(
            `[${scenario}] the load job did not complete cleanly — rows are summed across ` +
                'pods, so a missing pod shrinks every total. Refusing to report it as a result.'
        );
    }
}

const idleCapacity: Scenario = {
    name: 'sockets/idle-capacity',
    description: 'held connections with no traffic — what a socket costs before it carries anything',
    async run(ctx: RunContext): Promise<Metric[]> {
        const rungs = ladder('INFRA_WS_IDLE_LADDER', ctx.quick ? '1000,5000' : '1000,5000,10000');
        const result = await drive({
            mode: 'idle',
            ladder: rungs.join(','),
            parallelism: process.env.INFRA_WS_PODS ?? 1,
            durationS: Math.max(15, Math.round(ctx.durationMs / 1000))
        });
        refusePartial(result, 'sockets/idle-capacity');
        return [...result.merged.flatMap((row) => rowMetrics(row)), ...runMetrics(result)];
    }
};

const hotFanout: Scenario = {
    name: 'sockets/hot-fanout',
    description: 'N subscribers on ONE actor — deliveries/s and delivery latency vs subscriber count',
    async run(ctx: RunContext): Promise<Metric[]> {
        const rungs = ladder('INFRA_WS_HOT_LADDER', ctx.quick ? '1000,5000' : '1000,5000,10000');
        const result = await drive({
            mode: 'hot',
            actors: 1,
            ladder: rungs.join(','),
            parallelism: process.env.INFRA_WS_PODS ?? 1,
            publishRate: process.env.INFRA_WS_PUBLISH_RATE ?? 10,
            durationS: Math.max(30, Math.round(ctx.durationMs / 1000)),
            probes: 20
        });
        refusePartial(result, 'sockets/hot-fanout');
        return [...result.merged.flatMap((row) => rowMetrics(row)), ...runMetrics(result)];
    }
};

/**
 * The cliff, as one number.
 *
 * A live read that consults `ctx.principal` gets one watch loop per encoded
 * principal (#121), each loop's first read is a turn on a single-threaded
 * actor (batched since #193), and each identity's cross-host stream PINS
 * one pooled host-to-host connection for the subscription's lifetime
 * (#138). The measured 100–250 wall on AKS was that LAST cost under the
 * default `FETCH_CONNECTIONS=64` (#194): with the pool sized to the
 * identity population, 1000 identities dialled clean where ANONYMOUS
 * subscribers to the same actor reach 20 000 either way.
 *
 * `max_healthy_identities` is the highest rung that dialled cleanly. That
 * is the number #180 existed to move and #138 still would (fewer held
 * connections, not just fewer loops), and tracking it is the whole point
 * of this scenario: a ratio at a safe rung would read ~1.0 and say
 * nothing.
 *
 * The generator stops its ladder at the first rung with connect failures,
 * so the cost is bounded — and `connectTimeoutS` is deliberately short here
 * so the failing rung fails fast instead of hanging for its full window.
 */
const principalCliff: Scenario = {
    name: 'sockets/principal-cliff',
    description: 'how many DISTINCT identities one actor can serve live before dialling fails (#180)',
    async run(ctx: RunContext): Promise<Metric[]> {
        // 1000 was measured clean once the fetch pool stopped binding
        // (#194), so the default ladder reaches past the old 100–250 shelf
        // — a recorded run should find the cliff, not its own ceiling.
        const rungs = ladder(
            'INFRA_WS_IDENTITY_LADDER',
            ctx.quick ? '50,100,250' : '50,100,250,500,1000'
        );
        const result = await drive({
            mode: 'hot',
            actors: 1,
            read: 'mine',
            principal: 'per-user',
            ladder: rungs.join(','),
            parallelism: 1,
            publishRate: process.env.INFRA_WS_PUBLISH_RATE ?? 10,
            durationS: Math.max(30, Math.round(ctx.durationMs / 1000)),
            probes: 20,
            connectBatch: 50,
            connectTimeoutS: 10
        });
        // NOT refused on partial: a rung failing to dial is the measurement
        // here, and the generator stops the ladder rather than the pod.

        const clean = result.merged.filter((row) => row.connectFailures === 0);
        const highest = clean.length > 0 ? Math.max(...clean.map((row) => row.n)) : 0;
        const healthy = clean.find((row) => row.n === highest);

        return [
            {
                name: 'max_healthy_identities',
                value: highest,
                unit: 'count',
                direction: 'higher',
                // Not exact: where the cliff lands depends on the host's CPU
                // share and on how fast the generator dials, not only on the
                // runtime. It is a tracked figure, not an invariant.
                noiseFloor: 1
            },
            ...(healthy ? rowMetrics(healthy, 'healthy/') : []),
            ...runMetrics(result)
        ];
    }
};

/**
 * The #138 arm: the SAME identity population, on a read that declared
 * itself principal-independent.
 *
 * `Fanout.shared()` is byte-for-byte `Fanout.current()` plus
 * `watches: { shared: { principalIndependent: true } }`, so the relay
 * coalesces it across identities instead of holding one cross-host stream —
 * and one pooled connection — per principal. Everything else here is
 * `sockets/principal-cliff` verbatim, including the ladder, so the two
 * scenarios differ in exactly one thing and their `max_healthy_identities`
 * are directly comparable.
 *
 * **It runs at the CHART DEFAULT `FETCH_CONNECTIONS=64` on purpose.** That
 * is what makes this a different claim from #194's: that run escaped the
 * ceiling by sizing the pool to the identity population, which is a
 * mitigation. If #138 works, the pool never binds in the first place and
 * the arm walks the whole ladder on the untouched default. A run under a
 * resized pool proves nothing here and `INFRA_SHAPE` refuses it anyway.
 *
 * The `undeclared` twin is `sockets/principal-cliff` itself — it uses
 * `mine()`, which genuinely consults identity. `current()` (identity-blind,
 * undeclared) is the tighter control and is what
 * `INFRA_WS_DECLARED_CONTROL=1` switches this scenario to, for a run that
 * wants the pair with the read body held constant.
 */
const declaredFanout: Scenario = {
    name: 'sockets/declared-fanout',
    description:
        'distinct identities on a principalIndependent read — does #138 remove the ceiling at the default pool?',
    async run(ctx: RunContext): Promise<Metric[]> {
        const rungs = ladder(
            'INFRA_WS_IDENTITY_LADDER',
            ctx.quick ? '50,100,250' : '50,100,250,500,1000'
        );
        const result = await drive({
            mode: 'hot',
            actors: 1,
            read: process.env.INFRA_WS_DECLARED_CONTROL === '1' ? 'current' : 'shared',
            principal: 'per-user',
            ladder: rungs.join(','),
            parallelism: 1,
            publishRate: process.env.INFRA_WS_PUBLISH_RATE ?? 10,
            durationS: Math.max(30, Math.round(ctx.durationMs / 1000)),
            probes: 20,
            connectBatch: 50,
            connectTimeoutS: 10
        });
        // Same posture as principal-cliff: a rung failing to dial IS the
        // measurement, so a partial ladder is not refused.

        const clean = result.merged.filter((row) => row.connectFailures === 0);
        const highest = clean.length > 0 ? Math.max(...clean.map((row) => row.n)) : 0;
        const healthy = clean.find((row) => row.n === highest);

        return [
            {
                name: 'max_healthy_identities',
                value: highest,
                unit: 'count',
                direction: 'higher',
                // Not exact, for the reason `principal-cliff` documents: the
                // cliff's location depends on CPU share and dial rate, not
                // only on the runtime.
                noiseFloor: 1
            },
            ...(healthy ? rowMetrics(healthy, 'healthy/') : []),
            ...runMetrics(result)
        ];
    }
};

export const socketScenarios: Scenario[] = [
    idleCapacity,
    hotFanout,
    principalCliff,
    declaredFanout
];
