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
    /** Connections closed under us — see `sockets/slow-consumer` (#182). */
    drops: number;
    /** Subscribers deliberately made to stop reading (#182); 0 unless asked. */
    slowConnections: number;
    maxBufferedBytes: number;
    latencyMs: { p50: number; p90: number; p99: number; max: number } | null;
    partial?: boolean;
    /** A pod re-ran this rung after a transient over-threshold first attempt (#222). */
    retried?: boolean;
    /** The generator's reason for ending the ladder here; absent ⇒ it completed. */
    ladderStopped?: string;
}

export interface RunResult {
    merged: Row[];
    peakOpen: number;
    peakSubscriptions: number;
    /** Deepest HOST-side send buffer seen; `null` ⇒ no host could report one. */
    peakBufferedBytes?: number | null;
    hosts: number;
    /** Only the counters the hosts reported — a key can be absent (#223). */
    delta: Record<string, number | undefined>;
    /** Both cluster-stats snapshots saw the whole fleet; false ⇒ no `cluster/*`. */
    watchesTrustworthy?: boolean;
    /** Hosts reporting `tcp` in their transport chain (#203). */
    tcpHosts?: number;
    partial: boolean;
}

// No cast: `ws-load.d.mts` declares the shape, and a cast here would hide
// exactly the drift it exists to catch — the metric mapping below is the
// part most likely to be silently wrong.
async function drive(values: Record<string, unknown>): Promise<RunResult> {
    return await runWsLoad({
        context: CONTEXT,
        namespace: NAMESPACE,
        release: RELEASE,
        chartDir: CHART,
        imageRepository: IMAGE,
        imageTag: IMAGE_TAG,
        workload: WORKLOAD,
        values
    });
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
            // Which host-to-host transport the fleet ACTUALLY ran, recorded
            // on every socket run. `tcpTransport` falls through to HTTP per
            // link for any peer advertising no tcp address, so a fleet
            // mid-rollout yields a clean HTTP measurement wearing the tcp
            // label (#203) — and `INFRA_SHAPE` would compare it as tcp.
            name: 'hosts_with_tcp_transport',
            value: result.tcpHosts ?? 0,
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
        ...deliveryMetrics(result),
        ...watchMetrics(result)
    ];
}

/**
 * What the HOSTS say they sent, and how deep their own buffers got (#252).
 *
 * Every delivery figure recorded on this tier so far is the GENERATOR's
 * count. That is the right number for "did the client receive it" and the
 * wrong one for "what did this cost the host" — and after #245 the second
 * question is the interesting one, since a delivery is 77% socket write and
 * `host_deliveries` is therefore the closest thing to a syscall count.
 * Reported alongside the generator's, never instead of it: a gap between the
 * two is a real finding (frames written but not received) rather than an
 * error to reconcile away.
 *
 * `peak_host_buffered_bytes` is the number #182 argued about without and
 * #208 filed for. It is **omitted, not zeroed**, when no host could report
 * one — an adapter with no `bufferedBytes` seam yields `null`, and emitting
 * a 0 would assert "the hosts are not buffering" from an absence of data,
 * which is precisely the misreading that left #182 open.
 *
 * All informational. They describe a deployment under a load generator; the
 * invariants live at Tier 1.
 */
export function deliveryMetrics(result: RunResult): Metric[] {
    const metrics: Metric[] = [];
    const deliveries = result.delta.deliveries;
    if (deliveries !== undefined) {
        metrics.push(
            {
                name: 'host_deliveries',
                value: deliveries,
                unit: 'count',
                direction: 'higher',
                informational: true
            },
            {
                name: 'host_delivery_bytes',
                value: result.delta.deliveryBytes ?? 0,
                unit: 'count',
                direction: 'higher',
                informational: true
            }
        );
    }
    if (result.delta.throttleQuantized !== undefined) {
        metrics.push({
            name: 'throttle_quantized',
            value: result.delta.throttleQuantized,
            unit: 'count',
            direction: 'lower',
            informational: true
        });
    }
    if (typeof result.peakBufferedBytes === 'number') {
        metrics.push({
            name: 'peak_host_buffered_bytes',
            value: result.peakBufferedBytes,
            unit: 'count',
            direction: 'lower',
            informational: true
        });
    }
    return metrics;
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
 * Absent rather than zero when the run did not collect them — a zero here
 * would read as "no cross-host streams", which is a claim about the runtime
 * rather than about the harness. The harness drops them whenever either
 * cluster-stats snapshot missed a host, because the two failures point
 * opposite ways (a partial baseline OVERSTATES the delta, a partial end
 * snapshot understates it), so there is no honest label for the result.
 */
function watchMetrics(result: RunResult): Metric[] {
    const opened = result.delta['cluster/remoteWatches'];
    if (opened === undefined) return [];
    return [
        {
            name: 'remote_watch_streams',
            value: opened,
            unit: 'count',
            direction: 'lower',
            informational: true
        },
        {
            name: 'coalesced_watch_joins',
            value: result.delta['cluster/coalescedWatches'] ?? 0,
            unit: 'count',
            direction: 'higher',
            informational: true
        },
        {
            name: 'inbound_watch_streams',
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
 * The generator stops its ladder at the first rung whose connect-failure
 * RATE survives a retry over its threshold (#222 — one transient no longer
 * ends the climb, but a real ceiling still stops it after at most one
 * repeat), so the cost is bounded — and `connectTimeoutS` is deliberately
 * short here so a failing rung fails fast instead of hanging for its full
 * window.
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
        // The toggle is resolved ONCE and reported as a metric below. A
        // scenario that can silently run its own control is the same trap
        // the generator's `READ` validation closes one layer down: the
        // recorded artifact is named `sockets/declared-fanout` either way,
        // and nothing downstream would show which arm produced the number
        // now sitting in BASELINES.md.
        const control = process.env.INFRA_WS_DECLARED_CONTROL === '1';
        const result = await drive({
            mode: 'hot',
            actors: 1,
            read: control ? 'current' : 'shared',
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
                // 1 = the DECLARED read (`shared`), 0 = the undeclared
                // control (`current`). Present in every saved artifact so a
                // number can never be quoted under the wrong arm.
                name: 'declared_read',
                value: control ? 0 : 1,
                unit: 'count',
                direction: 'higher',
                informational: true
            },
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

/**
 * The slow-consumer arm (#182): what happens to a host when subscribers stop
 * keeping up.
 *
 * The socket send path is fire-and-forget — nothing in the runtime reads
 * `bufferedAmount` — so #182 says a slow client shows up as MEMORY on the
 * host rather than as backpressure. `SLOW_FRACTION` produces such a client
 * by pausing the TCP socket underneath a fraction of subscribers (see
 * `stall()` in `ws-loadgen.mjs` for why it must be the socket and not the
 * message handler). Nothing evicts them: the session's `{ p: 1 }` is an
 * application frame with no pong requirement, and `@sigx/actors-ws`
 * installs no WebSocket keepalive and no idle timeout.
 *
 * **What this scenario deliberately does NOT report is the buffer itself.**
 * The rig's `maxBufferedBytes` is sampled from the CLIENT's
 * `WebSocket#bufferedAmount` — data queued to send — and a subscriber sends
 * almost nothing, so it sits at ~0 however much the host is holding. The
 * host has no `bufferedAmount` instrumentation anywhere (`socketStats()`
 * has none, nor does `@sigx/actors-ws`), which is #182's own point and also
 * why #182 cannot be settled from this side of the socket. Reporting the
 * client number here would produce a confident `0` that means nothing, and
 * that is how "the hosts never outran the clients" gets written down.
 *
 * What it reports instead is the CONSEQUENCE, which is observable — and
 * this is a question rather than a gate, so every metric is informational:
 *
 * - `top/deliveries_per_sec` falling against a run at the same rung with
 *   `slow_connections: 0` is the finding that changes the sizing rule: a
 *   slow client degrading service for everyone on its host, not only itself.
 * - `drops` climbing means something DID react (the kernel, the ingress,
 *   `ws`), and where is then worth knowing — the runtime is not the only
 *   thing in the path.
 * - both flat means the host absorbed it silently, which is #182's
 *   prediction and the case that needs host-side instrumentation to confirm
 *   rather than merely fail to refute.
 */
const slowConsumer: Scenario = {
    name: 'sockets/slow-consumer',
    description: 'a fraction of subscribers stop reading — is there any send-path backpressure? (#182)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const rungs = ladder('INFRA_WS_SLOW_LADDER', ctx.quick ? '500' : '1000,5000');
        // Validated HERE as well as in the generator, and deliberately not
        // defaulted past a bad value: the generator's own guard fires inside
        // a Job on a live cluster, minutes and one deploy after the mistake.
        // A typo should cost a shell prompt, not a rung.
        const raw = process.env.INFRA_WS_SLOW_FRACTION ?? '0.1';
        const fraction = Number(raw);
        if (!Number.isFinite(fraction) || fraction <= 0 || fraction > 1) {
            throw new Error(
                `[sockets/slow-consumer] INFRA_WS_SLOW_FRACTION must be a number in (0, 1], ` +
                    `got '${raw}'. Zero would run the scenario with no slow consumer at all, ` +
                    `which is a different measurement wearing this one's name.`
            );
        }
        const result = await drive({
            mode: 'hot',
            actors: 1,
            read: 'current',
            ladder: rungs.join(','),
            parallelism: 1,
            publishRate: process.env.INFRA_WS_PUBLISH_RATE ?? 10,
            // Payload matters here in a way it does not elsewhere: buffered
            // BYTES is the observable, so a zero-length payload would show
            // the effect only in frame overhead.
            payloadBytes: process.env.INFRA_WS_SLOW_PAYLOAD ?? 4096,
            durationS: Math.max(60, Math.round(ctx.durationMs / 1000)),
            probes: 20,
            connectBatch: 250,
            slowFraction: fraction,
            slowAfterS: 5
        });
        refusePartial(result, 'sockets/slow-consumer');

        const top = result.merged[result.merged.length - 1];
        // The generator reports what ACTUALLY stalled, and zero means the
        // arm did not run — `stall()` depends on a private `ws` field, and a
        // silent no-op would leave `max_buffered_bytes: 0` looking like #182
        // disproven rather than never tested. Refuse rather than record it.
        if ((top?.slowConnections ?? 0) === 0) {
            throw new Error(
                `[sockets/slow-consumer] asked for ${fraction} of subscribers to stop ` +
                    `reading and none did. Without a slow consumer this rung measures ` +
                    `ordinary fan-out, and its buffered-bytes figure means nothing.`
            );
        }
        return [
            {
                name: 'slow_fraction',
                value: fraction,
                unit: 'ratio',
                direction: 'higher',
                informational: true
            },
            {
                name: 'slow_connections',
                value: top?.slowConnections ?? 0,
                unit: 'count',
                direction: 'higher',
                informational: true
            },
            {
                // The CLIENT's send buffer, recorded only so a reader can
                // see it stayed ~0 and know that is expected — a subscriber
                // sends nothing. It is NOT the host's buffer and must never
                // be quoted as evidence about #182 either way.
                name: 'client_max_buffered_bytes',
                value: top?.maxBufferedBytes ?? 0,
                unit: 'bytes',
                direction: 'higher',
                informational: true
            },
            {
                name: 'drops',
                value: top?.drops ?? 0,
                unit: 'count',
                direction: 'lower',
                informational: true
            },
            ...(top ? rowMetrics(top, 'top/') : []),
            ...runMetrics(result)
        ];
    }
};

export const socketScenarios: Scenario[] = [
    idleCapacity,
    hotFanout,
    principalCliff,
    declaredFanout,
    slowConsumer
];
