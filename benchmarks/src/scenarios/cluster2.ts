/**
 * TIER 2 — N silos as real OS processes over real loopback sockets.
 *
 * Everything else in this suite is Tier 1: one process, `pipeFetch`, zero
 * sockets. That is the right way to measure algorithmic shape, and it makes
 * the entire socket story invisible. These scenarios are the other half.
 *
 * ## Which numbers here are evidence
 *
 * N processes contend for the same cores, so this rig CANNOT honestly report
 * absolute throughput — the same objection `cluster-harness.ts` raises. The
 * split is enforced mechanically rather than by a note a reader may skip:
 *
 *   - **Counts gate.** Sockets accepted, concurrent peak, requests per
 *     connection, bytes per call, store ops per call. These are counts of
 *     events, invariant under CPU scheduling — a busy laptop does not change
 *     how many connections a transport opens.
 *   - **Everything else is `informational: true`**, so the comparer
 *     STRUCTURALLY cannot fail a run on it. Throughput, percentiles and RSS
 *     are recorded because they are useful context, not because a delta in
 *     them means anything on a contended box.
 *
 * ## What it still cannot measure
 *
 * Loopback RTT is ~30-60µs against a LAN's 200-1000µs, so this rig
 * systematically OVERSTATES software's share of latency and understates
 * connection setup. It has no congestion, no loss and no head-of-line
 * blocking — which is precisely the main risk of h2 multiplexing. Absolute
 * cluster-wide numbers at N >= 10 need N machines, not N processes. See
 * BASELINES.md.
 */
import { startRig, BENCH_TYPE, type Rig } from '../tier2/rig.ts';
import type { Metric, RunContext, Scenario } from '../types.ts';

/** Actor keys the driver round-robins over. More than one so the mailbox is
 *  not the bottleneck — this measures the wire, not queueing. */
const KEYS = ['k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7'];
const targets = KEYS.map((key) => ({ type: BENCH_TYPE, key }));

/**
 * Ownership is deterministic: every silo activates its own new actors, so
 * warming on silo `owner` guarantees every call from any other silo crosses
 * the wire. Without that, locality would silently vary run to run and the
 * socket counts would be meaningless.
 */
async function warmOn(rig: Rig, owner: number): Promise<void> {
    await rig.warm(owner, targets);
}

interface Measured {
    opsPerSec: number;
    ops: number;
    /** Summed over every silo that received traffic. */
    accepted: number;
    peak: number;
    inbound: number;
    bytes: number;
    storeOps: number;
    rssBytes: number;
    p50?: number;
    p99?: number;
}

async function measure(
    rig: Rig,
    driver: number,
    receivers: readonly number[],
    concurrency: number,
    durationMs: number,
    latency: boolean
): Promise<Measured> {
    // Settle before measuring. `warmOn` activates the actors on their owner,
    // but it is the DRIVER's route cache that decides whether the measured
    // window touches the directory — and that only fills once this silo has
    // actually dispatched to each target. Without this the fixed startup cost
    // (one resolution per concurrent worker) lands inside the window and the
    // guard below reads it as store traffic, which it is not.
    await rig.drive(driver, { targets, concurrency, durationMs: 50, latency: false });
    await rig.resetStats();
    const outcome = await rig.drive(driver, { targets, concurrency, durationMs, latency });
    const stats = await Promise.all(receivers.map((i) => rig.stats(i)));
    const driverStats = await rig.stats(driver);
    const sum = (pick: (s: (typeof stats)[number]) => number): number =>
        stats.reduce((total, s) => total + pick(s), 0);
    const percentiles = outcome.percentiles as { p50?: number; p99?: number } | undefined;
    return {
        opsPerSec: outcome.opsPerSec,
        ops: outcome.ops,
        accepted: sum((s) => s.accepted),
        peak: sum((s) => s.concurrentPeak),
        inbound: sum((s) => s.inboundRequests),
        bytes: sum((s) => s.bytesIn + s.bytesOut),
        // The guard: store RPCs the DRIVER made while measuring. After
        // warmup the route cache should make this zero; anything else means
        // the run measured the cluster store, not the transport.
        storeOps: driverStats.storeOps,
        rssBytes: sum((s) => s.rssBytes),
        ...(percentiles?.p50 !== undefined ? { p50: percentiles.p50 } : {}),
        ...(percentiles?.p99 !== undefined ? { p99: percentiles.p99 } : {})
    };
}

function guardStoreOps(label: string, m: Measured): void {
    const perCall = m.ops === 0 ? 0 : m.storeOps / m.ops;
    if (perCall > 0.05) {
        throw new Error(
            `[tier2] ${label}: ${perCall.toFixed(3)} store ops per call — the route cache is ` +
                `not holding, so this measured the cluster store rather than the transport. ` +
                `The number would be meaningless; failing instead of reporting it.`
        );
    }
}

/**
 * The #89 test.
 *
 * The claim under test is that HTTP/1.1 under undici's default
 * `pipelining: 1` needs one connection per IN-FLIGHT request, so the pool
 * sizes to `concurrency x peers` rather than to the peer count. If that is
 * right, sockets grow linearly with concurrency and flat-line with nothing.
 *
 * Sockets are counted by ACCEPTS on the receiving side — if a silo accepted
 * K connections from a peer, that peer opened K. No hook into undici, and
 * cross-checked against libuv's own TCP handle table.
 */
const socketScaling: Scenario = {
    name: 'cluster2/socket-scaling',
    description:
        'TIER 2 (real sockets, N processes) — connections per in-flight request. Counts gate; nothing here is timing',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        const concurrencies = ctx.quick ? [1, 8] : [1, 8, 64];
        const rig = await startRig({ silos: 2, secret: null });
        try {
            await warmOn(rig, 1);
            for (const concurrency of concurrencies) {
                const m = await measure(rig, 0, [1], concurrency, ctx.durationMs, false);
                guardStoreOps(`c=${concurrency}`, m);
                const prefix = `c=${concurrency}`;
                metrics.push(
                    {
                        name: `${prefix}/sockets_accepted`,
                        value: m.accepted,
                        unit: 'count',
                        direction: 'lower'
                    },
                    {
                        name: `${prefix}/sockets_peak`,
                        value: m.peak,
                        unit: 'count',
                        direction: 'lower'
                    },
                    {
                        // THE headline ratio, and the whole point of the
                        // scenario. 1.0 means one connection per in-flight
                        // request — #89's projection; higher means the pool
                        // is larger still.
                        //
                        // Computed from the PEAK, not from accepts: a
                        // connection already open when the window started is
                        // part of the pool but was never re-accepted inside
                        // it, so accepts undercount the pool by exactly the
                        // number of connections keep-alive did its job on.
                        name: `${prefix}/sockets_per_concurrency`,
                        value: m.peak / concurrency,
                        unit: 'ratio',
                        direction: 'lower'
                    },
                    {
                        // Keep-alive working looks like a big number here.
                        name: `${prefix}/requests_per_connection`,
                        value: m.accepted === 0 ? 0 : m.inbound / m.accepted,
                        unit: 'count',
                        direction: 'higher'
                    },
                    {
                        name: `${prefix}/ops_per_sec`,
                        value: m.opsPerSec,
                        unit: 'ops/sec',
                        direction: 'higher',
                        // Contended box: real, but never a gate.
                        informational: true
                    }
                );
            }
        } finally {
            await rig.stop();
        }
        return metrics;
    }
};

/**
 * Per-call cost over a REAL socket, with HMAC as an explicit axis.
 *
 * #84 put per-call HMAC at ~35µs — but that was measured through
 * `pipeFetch`, with no socket in the picture at all. This is the same
 * comparison with the wire present, which is the only version that says
 * anything about production.
 */
const transportMatrix: Scenario = {
    name: 'cluster2/transport-matrix',
    description:
        'TIER 2 (real sockets, N processes) — per-call cost with HMAC on vs off. Bytes and sockets gate; timings are informational',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        const concurrencies = ctx.quick ? [1] : [1, 64];
        for (const [label, secret] of [
            ['hmac', 'tier2-secret'],
            ['no-hmac', null]
        ] as const) {
            const rig = await startRig({ silos: 2, secret });
            try {
                await warmOn(rig, 1);
                for (const concurrency of concurrencies) {
                    // Throughput and latency in SEPARATE passes: timestamping
                    // every op costs a `performance.now()` per call, which is
                    // real distortion at this scale.
                    const thr = await measure(rig, 0, [1], concurrency, ctx.durationMs, false);
                    guardStoreOps(`${label} c=${concurrency}`, thr);
                    const lat = await measure(rig, 0, [1], concurrency, ctx.durationMs, true);

                    const prefix = `${label}/c=${concurrency}`;
                    metrics.push(
                        {
                            name: `${prefix}/bytes_per_call`,
                            value: thr.ops === 0 ? 0 : thr.bytes / thr.ops,
                            unit: 'bytes',
                            direction: 'lower'
                        },
                        // No socket metric here on purpose: `socket-scaling`
                        // owns that question and answers it exactly (2.00 at
                        // every concurrency), whereas at c=1 this scenario's
                        // pool is 1 or 2 depending on what survived warmup —
                        // a ±100% gating metric that would eventually fail a
                        // run for no reason.
                        {
                            name: `${prefix}/ops_per_sec`,
                            value: thr.opsPerSec,
                            unit: 'ops/sec',
                            direction: 'higher',
                            informational: true
                        },
                        {
                            name: `${prefix}/p50_ms`,
                            value: lat.p50 ?? 0,
                            unit: 'ms',
                            direction: 'lower',
                            informational: true
                        },
                        {
                            name: `${prefix}/p99_ms`,
                            value: lat.p99 ?? 0,
                            unit: 'ms',
                            direction: 'lower',
                            informational: true
                        },
                        {
                            name: `${prefix}/rss_mb`,
                            value: thr.rssBytes / (1024 * 1024),
                            unit: 'MB',
                            direction: 'lower',
                            informational: true
                        }
                    );
                }
            } finally {
                await rig.stop();
            }
        }
        return metrics;
    }
};

/**
 * #89's candidates, settled on the rig rather than argued.
 *
 * `default` is the global `fetch` — what ships today, whose undici agent has
 * `connections: null` and `pipelining: 1`. `bounded` caps the pool per origin.
 *
 * The `h2` arm sets `allowH2` on the client, and **measures identical to the
 * matching `bounded` arm**: the rig's silos serve over `createAppHandler` →
 * `node:http`, which is HTTP/1.1 only, so nothing negotiates and undici falls
 * back. It is kept as a control that documents that fact rather than as a
 * multiplexing test — real h2 would need a `node:http2` server first.
 *
 * All arms go through the EXISTING `fetch` seam, so nothing in the runtime
 * changes to run this — which is itself the finding: the escape hatch is
 * sufficient and only needed documenting.
 *
 * NOTE: results are undici-major-specific. `undici` is pinned to the major
 * Node bundles for the global `fetch`, so the tuned arms differ from
 * `default` only in Agent configuration and not in library version.
 */
const dispatcherMatrix: Scenario = {
    name: 'cluster2/dispatcher-matrix',
    description:
        'TIER 2 (real sockets) — default vs bounded vs h2 undici dispatcher. Sockets gate; timings informational',
    async run(ctx: RunContext): Promise<Metric[]> {
        const metrics: Metric[] = [];
        const concurrency = 64;
        const arms = ctx.quick
            ? ([['default', 0], ['h2', 8]] as const)
            : ([
                  ['default', 0],
                  ['bounded', 1],
                  ['bounded', 8],
                  ['bounded', 64],
                  // h2 at ONE connection is the real question: if it
                  // multiplexes, 64 concurrent calls ride a single socket.
                  ['h2', 1],
                  ['h2', 8]
              ] as const);
        for (const [kind, connections] of arms) {
            const rig = await startRig({
                silos: 2,
                secret: null,
                dispatcher: kind,
                connections
            });
            try {
                await warmOn(rig, 1);
                const label = kind === 'default' ? 'default' : `${kind}-${connections}`;
                const m = await measure(rig, 0, [1], concurrency, ctx.durationMs, false);
                guardStoreOps(label, m);
                metrics.push(
                    {
                        name: `${label}/sockets_peak`,
                        value: m.peak,
                        unit: 'count',
                        direction: 'lower'
                    },
                    {
                        name: `${label}/sockets_per_concurrency`,
                        value: m.peak / concurrency,
                        unit: 'ratio',
                        direction: 'lower'
                    },
                    {
                        name: `${label}/bytes_per_call`,
                        value: m.ops === 0 ? 0 : m.bytes / m.ops,
                        unit: 'bytes',
                        direction: 'lower'
                    },
                    {
                        name: `${label}/ops_per_sec`,
                        value: m.opsPerSec,
                        unit: 'ops/sec',
                        direction: 'higher',
                        informational: true
                    }
                );
            } finally {
                await rig.stop();
            }
        }
        return metrics;
    }
};

export const tier2Scenarios: Scenario[] = [socketScaling, transportMatrix, dispatcherMatrix];
