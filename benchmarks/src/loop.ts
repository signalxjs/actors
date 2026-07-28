/**
 * The closed-loop load generator — the thing `vitest bench` / tinybench
 * cannot do.
 *
 * tinybench awaits one operation at a time, which answers "how long does a
 * call take when nothing else is happening". For an actor runtime the
 * interesting question is the other one: with C calls in flight against a
 * mailbox that serializes turns, where does throughput saturate and what
 * happens to the tail? Against ONE grain throughput should flatten past
 * C=1 while p99 grows with queue depth; against N grains it should scale.
 * Those two curves are the runtime's core performance contract.
 *
 * Closed-loop (each worker re-issues on completion) rather than open-loop
 * (fixed arrival rate) because we are measuring capacity, not behaviour
 * under a known load — and closed-loop cannot suffer coordinated omission,
 * since a stalled call simply stops issuing new ones.
 */
import { Samples, type Percentiles } from './histogram.ts';
import type { Metric } from './types.ts';

/** Adaptive batching targets roughly this much work between clock reads. */
const TARGET_BATCH_MS = 1;
const MAX_BATCH = 1024;

/**
 * Below half a microsecond, a latency percentile is measuring
 * `performance.now()` and the loop scaffolding as much as the operation:
 * an uncontended turn lands around 0.2µs, where a 0.04µs wobble reads as
 * "20% slower" and fails a comparison on identical code. Differences under
 * this floor are not evidence. It is small enough to be irrelevant once
 * queueing pushes latency into the tens of microseconds, which is where the
 * relative gate is meaningful — and at c=1 the ops/sec metric measures the
 * same thing far more robustly anyway.
 */
export const LATENCY_NOISE_FLOOR_MS = 0.0005;

export interface LoopOptions {
    /** Issue operation `i`. Must resolve when the operation is complete. */
    call(i: number): Promise<unknown>;
    concurrency: number;
    durationMs: number;
    /**
     * Timestamp every operation. Costs an extra `performance.now()` per op,
     * which is real distortion at ~1µs/op — so throughput and latency are
     * measured in separate passes, never the same one.
     */
    latency: boolean;
}

export interface LoopOutcome {
    ops: number;
    elapsedMs: number;
    opsPerSec: number;
    percentiles?: Percentiles;
}

export async function closedLoop(options: LoopOptions): Promise<LoopOutcome> {
    const { call, concurrency, durationMs, latency } = options;
    const samples = latency ? new Samples() : null;
    let issued = 0;
    let ops = 0;

    const started = performance.now();
    const deadline = started + durationMs;

    /**
     * The deadline check is amortized, because at ~1µs/op a clock read per
     * iteration is several percent of the measurement. But a FIXED batch
     * would overrun badly on slow operations (32 × 100ms = 3.2s past the
     * deadline), so the batch size adapts to keep each batch ~1ms: fast
     * ops get a big batch and cheap clock reads, slow ops get a batch of 1
     * and stop on time.
     */
    const worker = async (): Promise<void> => {
        let batch = 1;
        let sinceCheck = 0;
        let lastCheck = started;
        for (;;) {
            let now: number;
            if (latency) {
                const t0 = performance.now();
                await call(issued++);
                now = performance.now();
                samples!.record(now - t0);
            } else {
                await call(issued++);
                now = 0;
            }
            ops++;
            if (++sinceCheck < batch) continue;

            // In latency mode we already paid for a clock read; reuse it.
            if (!latency) now = performance.now();
            const batchMs = now - lastCheck;
            lastCheck = now;
            sinceCheck = 0;
            if (batchMs < TARGET_BATCH_MS / 2) batch = Math.min(MAX_BATCH, batch * 2);
            else if (batchMs > TARGET_BATCH_MS * 2) batch = Math.max(1, batch >> 1);
            if (now >= deadline) return;
        }
    };

    await Promise.all(Array.from({ length: concurrency }, worker));
    const elapsedMs = performance.now() - started;

    const outcome: LoopOutcome = {
        ops,
        elapsedMs,
        opsPerSec: (ops / elapsedMs) * 1000
    };
    if (samples) outcome.percentiles = samples.percentiles();
    return outcome;
}

/**
 * Run one target at several concurrency levels, measuring throughput and
 * latency in separate passes, and flatten to metrics.
 */
export async function sweepConcurrency(options: {
    call(i: number): Promise<unknown>;
    concurrencies: readonly number[];
    durationMs: number;
    /** Skip the latency pass where the throughput number is all we want. */
    latency?: boolean;
}): Promise<Metric[]> {
    const { call, concurrencies, durationMs, latency = true } = options;
    const metrics: Metric[] = [];

    for (const concurrency of concurrencies) {
        const throughput = await closedLoop({ call, concurrency, durationMs, latency: false });
        metrics.push({
            name: `c=${concurrency}/ops_per_sec`,
            value: throughput.opsPerSec,
            unit: 'ops/s',
            direction: 'higher'
        });

        if (!latency) continue;
        const timed = await closedLoop({ call, concurrency, durationMs, latency: true });
        const p = timed.percentiles as Percentiles;
        metrics.push(
            {
                name: `c=${concurrency}/p50_ms`,
                value: p.p50,
                unit: 'ms',
                direction: 'lower',
                noiseFloor: LATENCY_NOISE_FLOOR_MS
            },
            {
                name: `c=${concurrency}/p99_ms`,
                value: p.p99,
                unit: 'ms',
                direction: 'lower',
                noiseFloor: LATENCY_NOISE_FLOOR_MS
            },
            {
                name: `c=${concurrency}/p999_ms`,
                value: p.p999,
                unit: 'ms',
                direction: 'lower',
                informational: true
            },
            {
                name: `c=${concurrency}/max_ms`,
                value: p.max,
                unit: 'ms',
                direction: 'lower',
                informational: true
            }
        );
    }
    return metrics;
}
