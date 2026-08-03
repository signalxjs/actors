/**
 * Heap and GC accounting.
 *
 * Two independent signals, both cheap:
 *  - `settleGc()` + `heapUsed()` for absolute footprint questions ("what
 *    does one actor cost?", "did that leak?"). Forcing GC is the only way
 *    to make heapUsed mean anything — an unsettled heap is mostly garbage.
 *  - `observeGc()` for allocation PRESSURE. It rides every scenario because
 *    a change that doubles GC pause time is a regression even when mean
 *    latency looks flat, and it is the earliest warning we get.
 */
import { GCProfiler, getHeapStatistics, writeHeapSnapshot } from 'node:v8';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

export interface GcTotals {
    collections: number;
    pauseMs: number;
    /** Major (mark-compact) collections — the expensive kind. */
    majorCollections: number;
}

export interface HeapReading {
    heapUsedBytes: number;
    rssBytes: number;
    externalBytes: number;
    /** V8's own accounting — catches growth heapUsed can hide. */
    heapSizeLimitBytes: number;
}

function requireGc(): () => void {
    const gc = (globalThis as { gc?: () => void }).gc;
    if (!gc) {
        throw new Error(
            'benchmarks need --expose-gc (use `pnpm bench:run`, not a bare `node src/main.ts`)'
        );
    }
    return gc;
}

/**
 * Drive the heap to a quiet state so `heapUsed()` reflects live objects.
 *
 * Two collections back to back, a macrotask turn so finalizers and any
 * pending I/O callbacks drain, then a third. One `gc()` is not enough:
 * objects that only became unreachable during the first pass survive it.
 */
export async function settleGc(): Promise<void> {
    const gc = requireGc();
    gc();
    gc();
    await delay(50);
    gc();
}

export function heapUsed(): number {
    return process.memoryUsage().heapUsed;
}

export function readHeap(): HeapReading {
    const mem = process.memoryUsage();
    return {
        heapUsedBytes: mem.heapUsed,
        rssBytes: mem.rss,
        externalBytes: mem.external,
        heapSizeLimitBytes: getHeapStatistics().heap_size_limit
    };
}

/**
 * Count GC events and total pause time until `stop()`.
 *
 * Uses `v8.GCProfiler`, NOT `PerformanceObserver({entryTypes:['gc']})`.
 * The observer is delivered through the event loop, and an in-process actor
 * benchmark is a pure-microtask workload that never turns the loop — so the
 * observer silently reports ZERO while `--trace-gc` shows hundreds of
 * scavenges. GCProfiler hooks V8's GC prologue/epilogue directly and sees
 * all of them.
 *
 * `statistics[].cost` is MICROSECONDS (verified against `--trace-gc`),
 * despite the surrounding heap figures being bytes.
 *
 * Costs ~3% throughput, which is why the runner gives it a dedicated
 * repetition instead of wrapping the measured ones.
 */
export function observeGc(): { stop(): GcTotals } {
    const profiler = new GCProfiler();
    profiler.start();
    return {
        stop(): GcTotals {
            const result = profiler.stop();
            let pauseUs = 0;
            let majorCollections = 0;
            for (const entry of result.statistics) {
                pauseUs += entry.cost;
                if (entry.gcType !== 'Scavenge') majorCollections++;
            }
            return {
                collections: result.statistics.length,
                pauseMs: pauseUs / 1000,
                majorCollections
            };
        }
    };
}

/**
 * Least-squares slope of a heap sample series, in bytes per sample.
 * A steady-state workload should sit at ~0; a positive slope that survives
 * `settleGc()` is the leak signal.
 */
export function heapSlopeBytesPerSample(samples: readonly number[]): number {
    const n = samples.length;
    if (n < 2) return 0;
    const meanX = (n - 1) / 2;
    const meanY = samples.reduce((a, b) => a + b, 0) / n;
    let num = 0;
    let den = 0;
    for (let i = 0; i < n; i++) {
        const dx = i - meanX;
        num += dx * ((samples[i] as number) - meanY);
        den += dx * dx;
    }
    return den === 0 ? 0 : num / den;
}

/**
 * Write a .heapsnapshot for manual retainer-path inspection in Chrome
 * DevTools. Opt-in via BENCH_HEAP_SNAPSHOT=1 — the files are hundreds of MB.
 */
export function maybeHeapSnapshot(label: string): string | null {
    if (process.env.BENCH_HEAP_SNAPSHOT !== '1') return null;
    // fileURLToPath, not URL.pathname: on Windows the latter yields
    // `/C:/...` with percent-escapes intact, which is not a usable path.
    const dir = fileURLToPath(new URL('../profiles/', import.meta.url));
    mkdirSync(dir, { recursive: true });
    const safe = label.replace(/[^a-zA-Z0-9_-]+/g, '-');
    return writeHeapSnapshot(join(dir, `${safe}.heapsnapshot`));
}
