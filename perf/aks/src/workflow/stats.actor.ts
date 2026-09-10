/**
 * `WorkflowStats` — the engine's visibility store, and deliberately a
 * SINGLETON subscriber: every run's completion event lands on key `all`
 * (the aggregator shape of the topic key mapping, as `ActivityFeed` in
 * `perf/app`). It is on the completion path by construction — `ctx.publish`
 * settles only when this handler's turn has — so its save cadence is a
 * knob (`WF_STATS_SAVE_EVERY`) and its turn queue is the first place a
 * cluster-wide completion rate is expected to cap.
 *
 * What it keeps durably is small: counts and a bounded ring of events the
 * load generator drains by cursor. Percentiles live in memory only
 * (`Samples` from the loadgen's histogram) — a histogram is not state, and
 * losing it to a host death costs a run its percentiles, never its counts.
 */
import { defineActor } from '../actors.app.ts';
import { Samples, type Percentiles } from '../loadgen/histogram.ts';
import { config } from './config.ts';
import { workflowCounters } from './counters.ts';
import type { CompletionEvent, RunStatus } from './types.ts';

/**
 * FNV-1a 32-bit over the run id — the shard a completion lands on when
 * `WF_STATS_SHARDS` > 1 (#432). Local on purpose: the runtime's hash is
 * not public API, and this one only has to be stable for the life of one
 * deployment (a run publishes once).
 */
function fnv1a(input: string): number {
    let h = 0x811c9dc5;
    for (let i = 0; i < input.length; i++) {
        h ^= input.charCodeAt(i);
        h = Math.imul(h, 0x01000193) >>> 0;
    }
    return h >>> 0;
}

/** The `WorkflowStats` key a run's completion is published to. */
export const statsShardKey = (runId: string): string =>
    config.statsShards === 1 ? 'all' : `s${fnv1a(runId) % config.statsShards}`;

/** Every shard key — what the generator resets and drains. */
export const statsShardKeys = (shards: number = config.statsShards): string[] =>
    shards === 1 ? ['all'] : Array.from({ length: shards }, (_v, i) => `s${i}`);

/**
 * One appended entry (#432): the event as the ring stores it (seq is
 * assigned by the reducer, so a replay numbers it identically) and the
 * per-run sums it contributes. `applyEntry` folds it; the handler folds
 * the same object through `ctx.append`, or through the reducer directly
 * in save mode.
 */
interface StatsEntry {
    e: Omit<StatsEvent, 'seq'>;
    sums: number[];
}
const SUM_KEYS: (keyof StatsSnapshot['sums'])[] = [
    'attempts', 'failures', 'compensations', 'children', 'transitions', 'timers',
    'reminders', 'fallback', 'lost', 'stale', 'signalsDelivered', 'signalsBuffered',
    'signalsLate', 'signalTimeouts'
];

export interface StatsEvent {
    seq: number;
    runId: string;
    template: string;
    tag: string | null;
    status: RunStatus;
    startedAt: number;
    endedAt: number;
    parent: boolean;
    /** The run's failure reason, truncated — absent on success. */
    error?: string;
}

export interface StatsSnapshot {
    /** How many shards share the stream — the generator drains them all. */
    shards: number;
    total: number;
    seq: number;
    byTemplate: Record<string, Record<string, number>>;
    /** End-to-end run latency per template (host clocks, both stamps). */
    latencyMs: Record<string, Percentiles>;
    /** Per node TYPE, from every run's raw samples. */
    nodeMs: Record<string, Percentiles>;
    wakeLagMs: Percentiles;
    sums: {
        attempts: number;
        failures: number;
        compensations: number;
        children: number;
        transitions: number;
        timers: number;
        reminders: number;
        fallback: number;
        lost: number;
        stale: number;
        signalsDelivered: number;
        signalsBuffered: number;
        signalsLate: number;
        signalTimeouts: number;
    };
}

const emptySums = (): StatsSnapshot['sums'] => ({
    attempts: 0,
    failures: 0,
    compensations: 0,
    children: 0,
    transitions: 0,
    timers: 0,
    reminders: 0,
    fallback: 0,
    lost: 0,
    stale: 0,
    signalsDelivered: 0,
    signalsBuffered: 0,
    signalsLate: 0,
    signalTimeouts: 0
});

/** Retained per activation. Sized modestly: the aggregator sees every
 *  run, and a reservoir keeps the percentiles honest past capacity. */
const SAMPLE_CAPACITY = 1 << 16;

export const WorkflowStats = defineActor({
    type: 'WorkflowStats',
    // Public on purpose: the load generator drains it bare.
    allowAnonymous: true,
    state: initialState,
    // The reducer behind `ctx.append` (#432): pure in (state, entry), run
    // once at append and once per activation replaying the log. Save mode
    // folds through the same function, so both modes agree byte for byte.
    applyEntry: (state, entry) => fold(state, entry as StatsEntry),
    methods: (ctx) => {
        const side = sideTable(ctx);
        const percentiles = (map: Map<string, Samples>): Record<string, Percentiles> =>
            Object.fromEntries([...map].map(([k, s]) => [k, s.percentiles()]));

        return {
            /**
             * Events after `since` (a seq), for one load run. The ring is
             * bounded, so a generator that polls slower than the ring turns
             * over loses events — `dropped` says so rather than the counts
             * quietly coming up short.
             */
            async drain(
                tag: string,
                since: number,
                limit = 5_000
            ): Promise<{ cursor: number; events: StatsEvent[]; dropped: number }> {
                const all = ctx.state.events;
                const oldest = all.length > 0 ? (all[0] as StatsEvent).seq : ctx.state.seq + 1;
                // Anything between `since` and the oldest retained event is
                // gone; only a positive gap counts.
                const dropped = since >= oldest - 1 ? 0 : oldest - 1 - since;
                const out: StatsEvent[] = [];
                for (const event of all) {
                    if (event.seq <= since) continue;
                    if (event.tag === tag) out.push(event);
                    if (out.length >= limit) break;
                }
                const cursor = out.length > 0 ? (out[out.length - 1] as StatsEvent).seq : ctx.state.seq;
                return { cursor, events: ctx.snapshot(out), dropped };
            },
            async snapshot(): Promise<StatsSnapshot> {
                return {
                    shards: config.statsShards,
                    total: ctx.state.total,
                    seq: ctx.state.seq,
                    byTemplate: ctx.snapshot(ctx.state.byTemplate),
                    latencyMs: percentiles(side.latency),
                    nodeMs: percentiles(side.nodes),
                    wakeLagMs: side.wakeLag.percentiles(),
                    sums: ctx.snapshot(ctx.state.sums)
                };
            },
            /** Forget everything — one load run at a time. */
            async reset(): Promise<void> {
                ctx.state.events = [];
                ctx.state.byTemplate = {};
                ctx.state.sums = emptySums();
                ctx.state.total = 0;
                ctx.state.unsaved = 0;
                side.latency.clear();
                side.nodes.clear();
                side.wakeLag = new Samples(SAMPLE_CAPACITY);
                // A reset is a full save, so it is a compaction point too.
                side.sinceCompaction = 0;
                await ctx.save();
            }
        };
    },
    subscriptions: {
        'workflow-events': {
            // The topic key is the run id; with one shard everything lands
            // on `all`, with N the id is hashed onto `s0`..`s{N-1}`.
            key: statsShardKey,
            handle: async (ctx, event) => {
                const e = event.payload as CompletionEvent;
                workflowCounters.statsEvents++;
                const st = e.stats;
                const entry: StatsEntry = {
                    e: {
                        runId: e.runId,
                        template: e.template,
                        tag: e.tag,
                        status: e.status,
                        startedAt: e.startedAt,
                        endedAt: e.endedAt,
                        parent: e.parentRunId !== null,
                        ...(e.error ? { error: e.error.slice(0, 120) } : {})
                    },
                    sums: [
                        st.attempts, st.failures, st.compensations, st.children, st.transitions,
                        st.wakes.timers, st.wakes.reminders, st.wakes.fallback, st.wakes.lost,
                        st.wakes.stale, st.signals.delivered, st.signals.buffered,
                        st.signals.late, st.signals.timedOut
                    ]
                };
                // The in-memory half rides the SAME activation the methods
                // closure holds — a subscription handler runs as a turn of
                // it — but it has no access to that closure, so the samples
                // hang off a per-activation side table instead.
                const side = sideTable(ctx);
                side.sample(side.latency, e.template).record(e.endedAt - e.startedAt);
                for (const [type, values] of Object.entries(st.nodeMs)) {
                    const target = side.sample(side.nodes, type);
                    for (const v of values) target.record(v);
                }
                for (const v of st.wakeLagMs) side.wakeLag.record(v);
                if (config.statsAppend) {
                    // O(entry): the reducer folds it into ctx.state and the
                    // storage appends it under the record's etag. The full
                    // save every WF_STATS_COMPACT_EVERY is the compaction —
                    // the only time the whole ring is written.
                    await ctx.append(entry);
                    if (++side.sinceCompaction >= config.statsCompactEvery) {
                        side.sinceCompaction = 0;
                        await ctx.save();
                    }
                } else {
                    fold(ctx.state, entry);
                    if (++ctx.state.unsaved >= config.statsSaveEvery) {
                        ctx.state.unsaved = 0;
                        await ctx.save();
                    }
                }
            }
        }
    }
});

function initialState() {
    return {
        seq: 0,
        total: 0,
        events: [] as StatsEvent[],
        byTemplate: {} as Record<string, Record<string, number>>,
        sums: emptySums(),
        /** Events since the last save — the cadence knob's counter (save mode). */
        unsaved: 0
    };
}
type State = ReturnType<typeof initialState>;

/** Fold one entry into the state, in place — the one reducer both modes use. */
function fold(s: State, entry: StatsEntry): void {
    s.seq++;
    s.total++;
    s.events.push({ seq: s.seq, ...entry.e });
    if (s.events.length > config.statsRing) {
        s.events.splice(0, s.events.length - config.statsRing);
    }
    const byStatus = (s.byTemplate[entry.e.template] ??= {});
    byStatus[entry.e.status] = (byStatus[entry.e.status] ?? 0) + 1;
    for (let i = 0; i < SUM_KEYS.length; i++) {
        s.sums[SUM_KEYS[i]!] += entry.sums[i] ?? 0;
    }
}

// The methods factory and the subscription handler receive the same ctx
// object, so a WeakMap keyed on it is the bridge. The factory above keeps
// its own maps for `snapshot()`; both read through here so they agree.
interface Side {
    latency: Map<string, Samples>;
    nodes: Map<string, Samples>;
    wakeLag: Samples;
    /** Appends since the last full save — append mode's compaction counter. */
    sinceCompaction: number;
    sample(map: Map<string, Samples>, key: string): Samples;
}
const sides = new WeakMap<object, Side>();
function sideTable(ctx: object): Side {
    let side = sides.get(ctx);
    if (!side) {
        side = {
            latency: new Map(),
            nodes: new Map(),
            wakeLag: new Samples(SAMPLE_CAPACITY),
            sinceCompaction: 0,
            sample(map, key) {
                let s = map.get(key);
                if (!s) map.set(key, (s = new Samples(SAMPLE_CAPACITY)));
                return s;
            }
        };
        sides.set(ctx, side);
    }
    return side;
}
