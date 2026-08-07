/**
 * Benchmark fixtures.
 *
 * Kept deliberately trivial: the point is to measure the RUNTIME, so the
 * actor body must contribute as close to zero as possible. `noop()` in
 * particular touches nothing — the number it produces is the floor cost of
 * getting a call through the dispatcher, the reentrancy check and one turn.
 *
 * `largeState` is the counterweight: the same call shape over a payload big
 * enough that the codec walks (`save()`, `snapshot()`) dominate, which is
 * where the write-behind and change-feed costs actually show up.
 */
import { defineActor } from '@sigx/actors';
import type { AnyActorDefinition } from '@sigx/actors';
import type { ServerPolicy } from '@sigx/server';

export interface TinyState {
    count: number;
}

/** The baseline actor: explicit persistence, so a call touches no storage. */
export const Tiny = defineActor({
    type: 'BenchTiny',
    allowAnonymous: true,
    state: (): TinyState => ({ count: 0 }),
    methods: (ctx) => ({
        /** Touches nothing — the pure dispatch-path floor. */
        noop() {
            return 0;
        },
        /** Mutates state without persisting. */
        increment(by: number) {
            ctx.state.count += by;
            return ctx.state.count;
        },
        /** Mutate + persist: one codec walk + one storage round trip. */
        async incrementAndSave(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        },
        /** A detached deep copy — the same double walk the change feed pays. */
        snapshot() {
            return ctx.snapshot();
        }
    }),
    streams: (ctx) => ({
        async *watch() {
            yield* ctx.changes({ initial: true });
        }
    })
});

/**
 * `Tiny`'s interleaving twin: `reentrant: 'always'`, same trivial body.
 * What it prices is the interleaved dispatch path — the per-turn call
 * store (AsyncLocalStorage) and the concurrent turn lane — against
 * `Tiny`'s serial promise chain.
 */
export const AlwaysTiny = defineActor({
    type: 'BenchAlwaysTiny',
    allowAnonymous: true,
    reentrant: 'always',
    state: (): TinyState => ({ count: 0 }),
    methods: (ctx) => ({
        noop() {
            return 0;
        },
        increment(by: number) {
            ctx.state.count += by;
            return ctx.state.count;
        }
    })
});

/** Same shape, but every mutating turn schedules a debounced background save. */
export const WriteBehind = defineActor({
    type: 'BenchWriteBehind',
    allowAnonymous: true,
    persistence: { mode: 'write-behind', debounceMs: 50 },
    state: (): TinyState => ({ count: 0 }),
    methods: (ctx) => ({
        increment(by: number) {
            ctx.state.count += by;
            return ctx.state.count;
        }
    })
});

export interface LargeState {
    count: number;
    rows: { id: number; name: string; tags: string[]; at: Date }[];
}

/** ~200 rows of mixed types, including a Date so the codec has real work. */
function makeRows(n: number): LargeState['rows'] {
    return Array.from({ length: n }, (_, i) => ({
        id: i,
        name: `row-${i}`,
        tags: ['alpha', 'beta', 'gamma'],
        at: new Date(1_700_000_000_000 + i)
    }));
}

export const Large = defineActor({
    type: 'BenchLarge',
    allowAnonymous: true,
    state: (): LargeState => ({ count: 0, rows: makeRows(200) }),
    methods: (ctx) => ({
        noop() {
            return 0;
        },
        increment(by: number) {
            ctx.state.count += by;
            return ctx.state.count;
        },
        async incrementAndSave(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        }
    }),
    streams: (ctx) => ({
        async *watch() {
            yield* ctx.changes({ initial: true });
        }
    })
});

/**
 * `Large`'s parameterised family, for sweeping state SIZE against the
 * change-tracking path (#124). Identical in every respect except the row
 * count — the variable under test — per the warning below.
 *
 * Write-behind with an hour-long debounce, because write-behind is the one
 * way to install change tracking with NO subscriber attached
 * (`#ensureChangeTracking` runs for write-behind persistence or for an open
 * change feed, and nothing else). The debounce arms once and, under
 * `manualScheduler()`, never fires — so what a turn pays here is the
 * tracking walk, not a storage write. Holding persistence identical across
 * the subs=0 and subs=1 arms is what makes their delta the snapshot clone
 * and nothing else.
 */
export function makeTrackedActor(rows: number): AnyActorDefinition {
    return defineActor({
        type: `BenchTracked${rows}`,
        allowAnonymous: true,
        persistence: { mode: 'write-behind', debounceMs: 3_600_000 },
        state: (): LargeState => ({ count: 0, rows: makeRows(rows) }),
        methods: (ctx) => ({
            increment(by: number) {
                ctx.state.count += by;
                return ctx.state.count;
            }
        }),
        streams: (ctx) => ({
            async *watch() {
                yield* ctx.changes({ initial: true });
            }
        })
    });
}

/**
 * `makeTrackedActor`'s twin with a READ method, for the `$live` path
 * (`host.dispatchWatch`) rather than a raw `ctx.changes()` feed (#129).
 *
 * Separate rather than a method added to `makeTrackedActor`, because that
 * fixture is one half of a recorded A/B and this file's own rule is that the
 * definitions under comparison differ in nothing but the variable under test.
 */
export function makeWatchableActor(rows: number): AnyActorDefinition {
    return defineActor({
        type: `BenchWatchable${rows}`,
        allowAnonymous: true,
        state: (): LargeState => ({ count: 0, rows: makeRows(rows) }),
        methods: (ctx) => ({
            increment(by: number) {
                ctx.state.count += by;
                return ctx.state.count;
            },
            /** The watched read: derived, tiny, and independent of `rows`. */
            total() {
                return ctx.state.count;
            }
        })
    });
}

export interface GrowingState {
    steps: { id: number; label: string; output: string; at: Date }[];
}

/**
 * The shape #124 was reported against: a job actor whose state accumulates
 * one step's output per turn, so the graph grows monotonically through a
 * run and every boundary re-walks (and re-clones) a larger tree. Explicit
 * persistence, exactly like `defineJob` — so nothing is tracked until a
 * `watch()` subscriber attaches, which is precisely how the reporter's
 * `WorkflowRun` ends up paying for it.
 *
 * The date is derived from the step index rather than read from a clock:
 * state a benchmark measures must not vary run to run.
 */
export const Growing = defineActor({
    type: 'BenchGrowing',
    allowAnonymous: true,
    state: (): GrowingState => ({ steps: [] }),
    methods: (ctx) => ({
        appendStep() {
            const id = ctx.state.steps.length;
            ctx.state.steps.push({
                id,
                label: `step-${id}`,
                output: `output of step ${id}`,
                at: new Date(1_700_000_000_000 + id)
            });
            return id;
        }
    }),
    streams: (ctx) => ({
        async *watch() {
            yield* ctx.changes({ initial: true });
        }
    })
});

/**
 * Guarded vs unguarded, for the wire path only — guards run OUTSIDE the
 * turns and are not on `host.dispatch`, so `host.actor()` never sees
 * them. Comparing these two over `handleActorRequest` is what prices the
 * guard chain, and it needs no new runtime seam.
 *
 * The two definitions below are deliberately IDENTICAL except for the
 * guards — same state, same single method, no stream table. An earlier
 * version used `Tiny` as the control and measured the GUARDED actor as
 * faster, because `Tiny` also carries a stream table and three extra
 * methods: the actor's shape swamped the variable under test. Holding
 * everything else fixed is the whole point of the comparison.
 */
const passPolicy: ServerPolicy = () => true;

const pricedState = (): TinyState => ({ count: 0 });
const pricedMethods = () => ({
    noop() {
        return 0;
    }
});

export const Guarded = defineActor({
    type: 'BenchGuarded',
    authorize: [passPolicy, passPolicy],
    state: pricedState,
    methods: pricedMethods
});

/** The control for `Guarded`: same shape, no policy chain. */
export const Unguarded = defineActor({
    type: 'BenchUnguarded',
    allowAnonymous: true,
    state: pricedState,
    methods: pricedMethods
});

/** Registers a durable reminder on activation, so the table has real entries. */
export const Reminded = defineActor({
    type: 'BenchReminded',
    allowAnonymous: true,
    state: (): TinyState => ({ count: 0 }),
    async onActivate(ctx) {
        await ctx.reminders.set('tick', { due: 60_000, period: 60_000 });
    },
    onReminder(ctx) {
        ctx.state.count++;
    },
    methods: (ctx) => ({
        current() {
            return ctx.state.count;
        }
    })
});

/** Holds a volatile timer per activation — for the timer-leak scenario. */
export const Timered = defineActor({
    type: 'BenchTimered',
    allowAnonymous: true,
    state: (): TinyState => ({ count: 0 }),
    onActivate(ctx) {
        ctx.timer(
            'tick',
            () => {
                ctx.state.count++;
            },
            { due: 3_600_000, period: 3_600_000 }
        );
    },
    methods: (ctx) => ({
        current() {
            return ctx.state.count;
        }
    })
});

export const ALL_BENCH_ACTORS = [
    Tiny,
    WriteBehind,
    Large,
    Guarded,
    Unguarded,
    Reminded,
    Timered
] as const;
