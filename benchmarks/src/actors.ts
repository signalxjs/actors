/**
 * Benchmark fixtures.
 *
 * Kept deliberately trivial: the point is to measure the RUNTIME, so the
 * actor body must contribute as close to zero as possible. `noop()` in
 * particular touches nothing — the number it produces is the floor cost of
 * getting a call through the dispatcher, the reentrancy check, the mailbox
 * and one turn.
 *
 * `largeState` is the counterweight: the same call shape over a payload big
 * enough that the codec walks (`save()`, `snapshot()`) dominate, which is
 * where the write-behind and change-feed costs actually show up.
 */
import { defineActor } from '@sigx/actors';
import type { ServerFnGuard } from '@sigx/server';

export interface TinyState {
    count: number;
}

/** The baseline actor: explicit persistence, so a call touches no storage. */
export const Tiny = defineActor({
    type: 'BenchTiny',
    unguarded: true,
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

/** Same shape, but every mutating turn schedules a debounced background save. */
export const WriteBehind = defineActor({
    type: 'BenchWriteBehind',
    unguarded: true,
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
    unguarded: true,
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
 * Guarded vs unguarded, for the wire path only — guards run OUTSIDE the
 * mailbox and are not on `host.dispatch`, so `host.actor()` never sees
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
const passGuard: ServerFnGuard = () => {};

const pricedState = (): TinyState => ({ count: 0 });
const pricedMethods = () => ({
    noop() {
        return 0;
    }
});

export const Guarded = defineActor({
    type: 'BenchGuarded',
    use: [passGuard, passGuard],
    state: pricedState,
    methods: pricedMethods
});

/** The control for `Guarded`: same shape, no guard chain. */
export const Unguarded = defineActor({
    type: 'BenchUnguarded',
    unguarded: true,
    state: pricedState,
    methods: pricedMethods
});

/** Registers a durable reminder on activation, so the table has real entries. */
export const Reminded = defineActor({
    type: 'BenchReminded',
    unguarded: true,
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
    unguarded: true,
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
