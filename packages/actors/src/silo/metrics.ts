/**
 * `metrics()` — pull-based observability, built almost entirely from seams
 * that already existed.
 *
 * The design follows issue #38: counters you READ, not a push pipeline and
 * no metrics-library dependency. `snapshot()` is the whole consumer API, so
 * a `/metrics` route, a stats grain, or a test assertion all use the same
 * thing.
 *
 * Where each number comes from:
 *
 *   useDispatch          calls, failures, end-to-end latency, per type
 *   observeTurns         queue wait vs turn time (the one new seam)
 *   onBeforeActivate     activations created
 *   onAfterDeactivate    activations destroyed, by reason
 *   decorateStorage      load/save/clear counts, latency, etag conflicts
 *   onStart              a silo handle, for live gauges
 *
 * Only `observeTurns` needed adding. A dispatch middleware measures
 * enqueue-to-settle and cannot separate "this grain is slow" from "this
 * grain is hot" — opposite problems with opposite fixes — and only the
 * activation knows both halves.
 */
import { isStorageConflict } from '../errors';
import type {
    ActorDispatcher,
    ActorRef,
    ActorStorage,
    DeactivationReason,
    Silo,
    SiloStats
} from '../types';
import { Histogram, type HistogramSnapshot } from './histogram';
import type { ActorPlugin, PluginRegistry } from './app';

export interface MetricsOptions {
    /**
     * Record latency distributions as well as counts. Default true. Turn it
     * off for counters only — a handful of integer adds per call and no
     * per-type histogram allocation.
     */
    histograms?: boolean;
    /**
     * Cap on distinct actor types tracked separately. Types beyond it fold
     * into `'(other)'`. Guards against unbounded growth when types are
     * generated rather than declared; 0 disables the per-type breakdown.
     */
    maxTypes?: number;
    /**
     * Start collecting immediately. Default true. Set false to attach the
     * plugin but pay nothing until something calls `enable()` — the shape
     * for "leave it wired in production, switch it on to investigate".
     */
    enabled?: boolean;
}

export interface TypeMetrics {
    calls: number;
    failed: number;
    /** Null when `histograms: false`. */
    latencyMs: HistogramSnapshot | null;
    queueMs: HistogramSnapshot | null;
    turnMs: HistogramSnapshot | null;
}

export interface ActorMetricsSnapshot {
    /** Wall-clock ms this window covers (since start or the last `reset()`). */
    windowMs: number;
    calls: {
        total: number;
        failed: number;
        /** Stream opens. Long-lived, so they carry no latency. */
        streams: number;
    };
    /** End-to-end: enqueue to settle. Null when `histograms: false`. */
    latencyMs: HistogramSnapshot | null;
    /** Time waiting for the mailbox — high means the grain is HOT. */
    queueMs: HistogramSnapshot | null;
    /** Time holding the mailbox — high means the turn itself is SLOW. */
    turnMs: HistogramSnapshot | null;
    byType: Record<string, TypeMetrics>;
    activations: {
        created: number;
        destroyed: number;
        byReason: Record<string, number>;
    };
    storage: {
        loads: number;
        saves: number;
        clears: number;
        /** Etag mismatches — each one discarded an activation. */
        conflicts: number;
        latencyMs: HistogramSnapshot | null;
    };
    /** Live gauges from `silo.stats()`; null before `start()`. */
    gauges: SiloStats | null;
}

export interface MetricsPlugin extends ActorPlugin {
    /** Read the counters. Cheap enough to poll; allocates a fresh object. */
    snapshot(): ActorMetricsSnapshot;
    /** Zero every counter and restart the window. */
    reset(): void;
    /** Is collection currently running? */
    readonly enabled: boolean;
    /**
     * Start collecting. Before `start()` this only records the intent; the
     * turn subscription attaches when the silo comes up.
     */
    enable(): void;
    /**
     * Stop collecting, and stop PAYING for it: the turn subscription is
     * dropped, so the runtime goes back to not timing turns at all, and the
     * dispatch wrapper degrades to a single branch and a pass-through.
     *
     * Counters keep their accumulated values — disabling freezes the numbers
     * rather than discarding them. `reset()` clears them.
     */
    disable(): void;
}

const DEFAULT_MAX_TYPES = 64;
const OTHER = '(other)';

interface TypeBucket {
    calls: number;
    failed: number;
    latency: Histogram | null;
    queue: Histogram | null;
    turn: Histogram | null;
}

export function metrics(options: MetricsOptions = {}): MetricsPlugin {
    const useHistograms = options.histograms ?? true;
    // Clamp rather than trust: a negative would make `types.size >= maxTypes`
    // true from the first call and fold EVERY type into '(other)', which
    // looks like a bug in the breakdown rather than a bad option. A
    // fractional value would make the cap arbitrary.
    const maxTypes = Math.max(0, Math.floor(options.maxTypes ?? DEFAULT_MAX_TYPES));

    let silo: Silo | null = null;
    // Monotonic: `windowMs` is a duration, and the wall clock can step.
    let windowStart = performance.now();
    let enabled = options.enabled ?? true;
    /** Live turn subscription; null whenever collection is off. */
    let detachTurns: (() => void) | null = null;
    let subscribeTurns: (() => void) | null = null;

    let calls = 0;
    let failed = 0;
    let streams = 0;
    let created = 0;
    let destroyed = 0;
    const byReason: Record<string, number> = {};
    let loads = 0;
    let saves = 0;
    let clears = 0;
    let conflicts = 0;

    const newHistogram = (): Histogram | null => (useHistograms ? new Histogram() : null);
    const latency = newHistogram();
    const queue = newHistogram();
    const turn = newHistogram();
    const storageLatency = newHistogram();
    const types = new Map<string, TypeBucket>();

    /** Per-type bucket, capped so a generated type namespace cannot leak. */
    function bucketFor(type: string): TypeBucket | null {
        if (maxTypes === 0) return null;
        const existing = types.get(type);
        if (existing) return existing;
        // Past the cap every new type folds into one overflow bucket, which
        // is itself allowed to exceed the cap — otherwise the calls it
        // represents would vanish from the totals' breakdown entirely.
        const key = types.size >= maxTypes ? OTHER : type;
        const overflow = types.get(key);
        if (overflow) return overflow;
        const bucket: TypeBucket = {
            calls: 0,
            failed: 0,
            latency: newHistogram(),
            queue: newHistogram(),
            turn: newHistogram()
        };
        types.set(key, bucket);
        return bucket;
    }

    const plugin: MetricsPlugin = {
        name: 'metrics',

        setup(registry: PluginRegistry): void {
            registry.useDispatch((next: ActorDispatcher): ActorDispatcher => {
                const measured = async (
                    ref: Parameters<ActorDispatcher['dispatch']>[0],
                    method: string,
                    args: readonly unknown[],
                    call: Parameters<ActorDispatcher['dispatch']>[3]
                ): Promise<unknown> => {
                    // Only read the clock if something will read the number.
                    // `histograms: false` is the cheap mode; a clock read per
                    // dispatch for a value that is then discarded is exactly
                    // the overhead it exists to avoid.
                    const started = latency ? performance.now() : 0;
                    const bucket = bucketFor(ref.type);
                    calls++;
                    if (bucket) bucket.calls++;
                    try {
                        return await next.dispatch(ref, method, args, call);
                    } catch (error) {
                        failed++;
                        if (bucket) bucket.failed++;
                        throw error;
                    } finally {
                        if (latency) {
                            // Monotonic, for the same reason the turn
                            // observer is: a wall clock can step backwards.
                            const elapsed = performance.now() - started;
                            latency.record(elapsed);
                            bucket?.latency?.record(elapsed);
                        }
                    }
                };
                const wrapped: ActorDispatcher = {
                    // Deliberately NOT async. The middleware cannot be
                    // removed from the chain once composed, so the disabled
                    // path has to be as close to free as a wrapper can get:
                    // returning `next.dispatch(...)` directly costs a branch
                    // and a call, whereas an `async` wrapper would allocate
                    // an extra promise and an async frame on every dispatch
                    // even while switched off.
                    dispatch(ref, method, args, call) {
                        if (!enabled) return next.dispatch(ref, method, args, call);
                        return measured(ref, method, args, call);
                    }
                };
                // MUST forward dispatchStream. It is optional on
                // ActorDispatcher, so returning a bare { dispatch } silently
                // kills every `streams:` method and the failure looks like a
                // transport bug, not a middleware one. Streams are
                // long-lived, so they get a counter and no latency.
                if (next.dispatchStream) {
                    wrapped.dispatchStream = (ref, method, args, call) => {
                        if (enabled) streams++;
                        return next.dispatchStream!(ref, method, args, call);
                    };
                }
                return wrapped;
            });

            // Subscribed ONLY when there is somewhere to put the numbers
            // AND collection is on. A no-op observer would still switch on
            // the activation's per-turn timestamps, so both `histograms:
            // false` and `disable()` would quietly keep paying for the
            // larger half of the cost.
            if (queue && turn) {
                subscribeTurns = () => {
                    if (detachTurns || !silo) return;
                    detachTurns = silo.observeTurns((ref, _method, queuedMs, elapsedMs) => {
                        queue.record(queuedMs);
                        turn.record(elapsedMs);
                        const bucket = bucketFor(ref.type);
                        bucket?.queue?.record(queuedMs);
                        bucket?.turn?.record(elapsedMs);
                    });
                };
            }

            registry.onBeforeActivate((_ref: ActorRef) => {
                created++;
            });
            registry.onAfterDeactivate((_ref: ActorRef, reason: DeactivationReason) => {
                destroyed++;
                byReason[reason] = (byReason[reason] ?? 0) + 1;
            });

            registry.decorateStorage(
                (inner: ActorStorage): ActorStorage => ({
                    async load(type, key) {
                        const started = storageLatency ? performance.now() : 0;
                        try {
                            return await inner.load(type, key);
                        } finally {
                            loads++;
                            storageLatency?.record(performance.now() - started);
                        }
                    },
                    async save(type, key, state, expectedEtag) {
                        const started = storageLatency ? performance.now() : 0;
                        try {
                            return await inner.save(type, key, state, expectedEtag);
                        } catch (error) {
                            // An etag mismatch is not a generic failure: it
                            // means an activation was discarded as stale, and
                            // a rising count is a real correctness signal.
                            if (isStorageConflict(error)) conflicts++;
                            throw error;
                        } finally {
                            saves++;
                            storageLatency?.record(performance.now() - started);
                        }
                    },
                    async clear(type, key, expectedEtag) {
                        const started = storageLatency ? performance.now() : 0;
                        try {
                            return await inner.clear(type, key, expectedEtag);
                        } catch (error) {
                            if (isStorageConflict(error)) conflicts++;
                            throw error;
                        } finally {
                            clears++;
                            storageLatency?.record(performance.now() - started);
                        }
                    }
                })
            );

            // Publish to the ops endpoint if one is mounted. `reportOps` is
            // just a registration — an app with no `ops()` never calls the
            // provider, so this costs nothing but the closure.
            registry.reportOps('metrics', () => plugin.snapshot());

            registry.onStart((live: Silo) => {
                silo = live;
                if (enabled) subscribeTurns?.();
            });
            registry.onStop(() => {
                // Drop the handle: gauges from a stopped silo are stale, and
                // holding it would pin the whole silo graph in memory.
                detachTurns?.();
                detachTurns = null;
                silo = null;
            });
        },

        snapshot(): ActorMetricsSnapshot {
            const byTypeOut: Record<string, TypeMetrics> = {};
            for (const [type, bucket] of types) {
                byTypeOut[type] = {
                    calls: bucket.calls,
                    failed: bucket.failed,
                    latencyMs: bucket.latency?.snapshot() ?? null,
                    queueMs: bucket.queue?.snapshot() ?? null,
                    turnMs: bucket.turn?.snapshot() ?? null
                };
            }
            return {
                windowMs: performance.now() - windowStart,
                calls: { total: calls, failed, streams },
                latencyMs: latency?.snapshot() ?? null,
                queueMs: queue?.snapshot() ?? null,
                turnMs: turn?.snapshot() ?? null,
                byType: byTypeOut,
                activations: { created, destroyed, byReason: { ...byReason } },
                storage: {
                    loads,
                    saves,
                    clears,
                    conflicts,
                    latencyMs: storageLatency?.snapshot() ?? null
                },
                gauges: silo ? silo.stats() : null
            };
        },

        get enabled(): boolean {
            return enabled;
        },

        enable(): void {
            if (enabled) return;
            enabled = true;
            subscribeTurns?.();
        },

        disable(): void {
            if (!enabled) return;
            enabled = false;
            // Unsubscribing is the point. Leaving the observer attached and
            // returning early inside it would keep the runtime timing every
            // turn for numbers nobody reads.
            detachTurns?.();
            detachTurns = null;
        },

        reset(): void {
            windowStart = performance.now();
            calls = 0;
            failed = 0;
            streams = 0;
            created = 0;
            destroyed = 0;
            for (const key of Object.keys(byReason)) delete byReason[key];
            loads = 0;
            saves = 0;
            clears = 0;
            conflicts = 0;
            latency?.reset();
            queue?.reset();
            turn?.reset();
            storageLatency?.reset();
            // Drop the type map entirely rather than resetting each bucket:
            // a type that stopped being called should leave the report, not
            // linger as a row of zeroes forever.
            types.clear();
        }
    };

    return plugin;
}
