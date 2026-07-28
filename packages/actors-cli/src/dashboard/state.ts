/**
 * The dashboard's live state: one poll loop, one reactive object, and the
 * derived series the sparklines read.
 *
 * Deliberately separate from the rendering. The screens are pure functions
 * of this object, so the awkward parts — polling, back-pressure, rate
 * derivation across resets, remembering an error without discarding the
 * last good snapshot — are testable without a terminal.
 */
import { signal } from '@sigx/terminal';
import { RateTracker, Series } from '../model/rates';
import type { MonitorSnapshot, MonitorSource } from '../source/types';

export interface DashboardOptions {
    source: MonitorSource;
    /** Poll interval, ms. Default 1000. */
    intervalMs?: number;
    /** Sparkline history length, in samples. Default 60. */
    history?: number;
}

export interface DashboardView {
    /**
     * The last GOOD snapshot. Kept across a failed poll on purpose: a
     * dashboard that blanks the moment a silo hiccups destroys exactly the
     * context you need to understand the hiccup.
     */
    snapshot: MonitorSnapshot | null;
    /** The most recent poll failure, cleared by the next success. */
    error: string | null;
    paused: boolean;
    intervalMs: number;
    /** Epoch-ms of the last successful poll — how stale the numbers are. */
    lastOk: number;
    polls: number;
}

export const DEFAULT_INTERVAL_MS = 1000;
export const MIN_INTERVAL_MS = 200;
export const MAX_INTERVAL_MS = 60_000;
const DEFAULT_HISTORY = 60;

export class DashboardState {
    readonly source: MonitorSource;
    /** One deep signal — the screens read it, the poll loop mutates it. */
    readonly view: DashboardView;

    readonly calls: Series;
    readonly failures: Series;
    readonly queued: Series;
    readonly activations: Series;

    #rates = new RateTracker();
    #timer: ReturnType<typeof setTimeout> | null = null;
    #inFlight: AbortController | null = null;
    #stopped = false;

    constructor(options: DashboardOptions) {
        this.source = options.source;
        const history = Number.isFinite(options.history)
            ? Math.max(1, Math.floor(options.history!))
            : DEFAULT_HISTORY;
        this.calls = new Series(history);
        this.failures = new Series(history);
        this.queued = new Series(history);
        this.activations = new Series(history);
        this.view = signal<DashboardView>({
            snapshot: null,
            error: null,
            paused: false,
            intervalMs: clampInterval(options.intervalMs ?? DEFAULT_INTERVAL_MS),
            lastOk: 0,
            polls: 0
        });
    }

    /** One poll. Safe to call directly — that is what the refresh key does. */
    async poll(): Promise<void> {
        if (this.#stopped) return;
        // A slow silo must not queue polls behind each other: the previous
        // request is abandoned rather than left to pile up, or a 5s silo on
        // a 1s interval accumulates requests until something gives.
        this.#inFlight?.abort();
        const controller = new AbortController();
        this.#inFlight = controller;
        try {
            const next = await this.source.snapshot(controller.signal);
            if (this.#stopped || controller.signal.aborted) return;
            this.#record(next);
            this.view.snapshot = next;
            this.view.error = null;
            this.view.lastOk = next.at;
        } catch (error) {
            if (controller.signal.aborted || this.#stopped) return;
            // The last good snapshot stays on screen; the caller greys it.
            this.view.error = (error as Error)?.message ?? String(error);
        } finally {
            if (this.#inFlight === controller) this.#inFlight = null;
            this.view.polls++;
        }
    }

    /** Start polling. Idempotent. */
    start(): void {
        if (this.#timer || this.#stopped) return;
        const tick = async (): Promise<void> => {
            if (!this.view.paused) await this.poll();
            if (this.#stopped) return;
            this.#timer = setTimeout(tick, this.view.intervalMs);
            // Never hold the process open for a poll: Ctrl+C should exit
            // now, not after the next interval elapses.
            this.#timer.unref?.();
        };
        void tick();
    }

    async stop(): Promise<void> {
        this.#stopped = true;
        if (this.#timer) clearTimeout(this.#timer);
        this.#timer = null;
        this.#inFlight?.abort();
        await this.source.close();
    }

    togglePause(): void {
        this.view.paused = !this.view.paused;
    }

    /** Step the interval, clamped. */
    nudgeInterval(factor: number): void {
        this.view.intervalMs = clampInterval(Math.round(this.view.intervalMs * factor));
    }

    /** Fold one snapshot into the derived series. */
    #record(next: MonitorSnapshot): void {
        const totals = next.cluster?.totals;
        const activations =
            totals?.activations ?? next.silos.reduce((sum, s) => sum + s.stats.activations, 0);
        const queued = totals?.queued ?? next.silos.reduce((sum, s) => sum + s.stats.queued, 0);

        // Gauges, not counters: pushed as-is rather than differenced.
        this.activations.push(activations);
        this.queued.push(queued);

        const metrics = next.metrics;
        if (metrics) {
            this.calls.push(this.#rates.observe('calls', next.at, metrics.calls.total));
            this.failures.push(this.#rates.observe('failed', next.at, metrics.calls.failed));
        } else {
            // A gap, not a zero — "no metrics plugin" is not "no traffic".
            this.calls.push(null);
            this.failures.push(null);
        }

        // Silos that left stop being tracked, so a long-lived dashboard over
        // a churning cluster does not grow a series per departed peer.
        this.#rates.retain(['calls', 'failed', ...next.silos.map((s) => s.siloId)]);
    }
}

export function clampInterval(ms: number): number {
    if (!Number.isFinite(ms)) return DEFAULT_INTERVAL_MS;
    return Math.max(MIN_INTERVAL_MS, Math.min(MAX_INTERVAL_MS, Math.round(ms)));
}
