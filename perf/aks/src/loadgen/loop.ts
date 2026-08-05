/**
 * Copied from benchmarks/src/loop.ts (closedLoop only; the seam is
 * `call(i)`) — keep in sync by hand. One deliberate divergence: at
 * HTTP-scale latencies (ms, not µs) the extra clock read per op is noise,
 * so this copy is normally run with `latency: true` in a single pass
 * instead of the benchmark's separate throughput/latency passes.
 *
 * Closed-loop (each worker re-issues on completion) rather than open-loop
 * because we measure capacity, and it cannot suffer coordinated omission —
 * a stalled call simply stops issuing new ones.
 */
import { Samples, type Percentiles } from './histogram.ts';

/** Adaptive batching targets roughly this much work between clock reads. */
const TARGET_BATCH_MS = 1;
const MAX_BATCH = 1024;

export interface LoopOptions {
    /** Issue operation `i`. Must resolve when the operation is complete. */
    call(i: number): Promise<unknown>;
    concurrency: number;
    durationMs: number;
    /** Timestamp every operation (percentiles in the outcome). */
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
     * The deadline check is amortized; a FIXED batch would overrun badly on
     * slow operations, so the batch size adapts to keep each batch ~1ms:
     * fast ops get a big batch and cheap clock reads, slow ops get a batch
     * of 1 and stop on time.
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
