/**
 * The dispatch ladder.
 *
 * Each scenario adds exactly one layer to the one above it, so the cost of
 * a layer is a SUBTRACTION rather than a guess:
 *
 *   turns-raw            promise-chain turn serialization, no actor at all
 *   warm-actor           + placement, reentrancy check, activation lookup, turn
 *   warm-actor-deadline  + the call-deadline machinery (the production default)
 *   via-proxy            + the client proxy and a freshly minted call context
 *   fan-out-actors       warm-actor across N activations (directory + parallelism)
 *
 * No single number here means much on its own; the gaps between them are
 * the finding.
 */
import { defineActor, isActorError } from '@sigx/actors';
import { Turns } from '@sigx/actors/host';
import { AlwaysTiny, Tiny } from '../actors.ts';
import { sweepConcurrency } from '../loop.ts';
import {
    benchCall,
    createBenchHost,
    PRODUCTION_CALL_TIMEOUT_MS,
    refsFor,
    warmActivations
} from '../host-fixture.ts';
import type { Metric, RunContext, Scenario } from '../types.ts';

/**
 * 1 = uncontended, 64 = deep queue, 512 = saturated. c=8 was dropped: it
 * told us nothing between 1 and 64, and every extra pass is more sustained
 * load, which is the dominant source of measurement drift on a laptop.
 */
const FULL_SWEEP = [1, 64, 512] as const;
const SHORT_SWEEP = [1, 64] as const;

const turnsRaw: Scenario = {
    name: 'dispatch/turns-raw',
    description: 'Turns.run() alone — the turn-serialization floor, no actor',
    async run(ctx: RunContext): Promise<Metric[]> {
        const turns = new Turns();
        return sweepConcurrency({
            call: () => turns.run(() => 0),
            concurrencies: ctx.quick ? SHORT_SWEEP : FULL_SWEEP,
            durationMs: ctx.durationMs
        });
    }
};

const warmGrain: Scenario = {
    name: 'dispatch/warm-actor',
    description: 'host.dispatch() to one warm activation, no deadline (callTimeoutMs: 0)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const fixture = await createBenchHost({ actors: [Tiny] });
        try {
            const ref = { type: Tiny.type, key: 'warm' };
            const call = benchCall();
            await fixture.host.dispatch(ref, 'noop', [], call);
            return await sweepConcurrency({
                call: () => fixture.host.dispatch(ref, 'noop', [], call),
                concurrencies: ctx.quick ? SHORT_SWEEP : FULL_SWEEP,
                durationMs: ctx.durationMs
            });
        } finally {
            await fixture.stop();
        }
    }
};

const warmGrainDeadline: Scenario = {
    name: 'dispatch/warm-actor-deadline',
    description: `same call with the PRODUCTION callTimeoutMs (${PRODUCTION_CALL_TIMEOUT_MS}ms) — the call-deadline tax`,
    async run(ctx: RunContext): Promise<Metric[]> {
        const fixture = await createBenchHost({
            actors: [Tiny],
            callTimeoutMs: PRODUCTION_CALL_TIMEOUT_MS
        });
        try {
            const ref = { type: Tiny.type, key: 'warm' };
            const call = benchCall();
            await fixture.host.dispatch(ref, 'noop', [], call);
            return await sweepConcurrency({
                call: () => fixture.host.dispatch(ref, 'noop', [], call),
                concurrencies: ctx.quick ? SHORT_SWEEP : FULL_SWEEP,
                durationMs: ctx.durationMs
            });
        } finally {
            await fixture.stop();
        }
    }
};

const viaProxy: Scenario = {
    name: 'dispatch/via-proxy',
    description: 'host.actor(def, key).noop() — adds the client proxy and a minted call id',
    async run(ctx: RunContext): Promise<Metric[]> {
        const fixture = await createBenchHost({ actors: [Tiny] });
        try {
            await fixture.host.actor(Tiny, 'warm').noop();
            return await sweepConcurrency({
                call: () => fixture.host.actor(Tiny, 'warm').noop(),
                concurrencies: ctx.quick ? SHORT_SWEEP : FULL_SWEEP,
                durationMs: ctx.durationMs
            });
        } finally {
            await fixture.stop();
        }
    }
};

/**
 * The scaling half of the contract. One actor serializes; N actors should
 * not — if throughput here tracks `warm-actor` instead of rising with
 * concurrency, something is serializing that should not be.
 */
const fanOut: Scenario = {
    name: 'dispatch/fan-out-actors',
    description: 'host.dispatch() round-robin across 1 000 warm activations',
    async run(ctx: RunContext): Promise<Metric[]> {
        const fixture = await createBenchHost({ actors: [Tiny] });
        try {
            const count = ctx.quick ? 100 : 1000;
            const refs = refsFor(Tiny.type, count);
            await warmActivations(fixture.host, refs);
            const call = benchCall();
            return await sweepConcurrency({
                call: (i) =>
                    fixture.host.dispatch(refs[i % count] as (typeof refs)[number], 'noop', [], call),
                concurrencies: ctx.quick ? SHORT_SWEEP : FULL_SWEEP,
                durationMs: ctx.durationMs
            });
        } finally {
            await fixture.stop();
        }
    }
};

/**
 * The dispatch ladder in COUNTS rather than seconds.
 *
 * Every rung above is a timing, and on a contended laptop a timing cannot
 * settle an argument — the comparer says so itself, refusing a verdict when
 * the probe drifts. A microtask turn is not a duration though, it is an
 * event, and the count of them between calling `dispatch()` and its promise
 * settling is fully deterministic: repeated samples return the same integer
 * on a busy machine and an idle one alike.
 *
 * That makes it the one dispatch metric that can gate. It measures exactly
 * what the timings cannot isolate — whether the path allocates promises it
 * does not need — so an `async` reintroduced on a synchronous path shows up
 * here as +1 even when throughput is buried in noise.
 */
const warmTurns: Scenario = {
    name: 'dispatch/warm-turns',
    description: 'microtask turns for ONE warm dispatch — a count, so it gates where timings cannot',
    async run(): Promise<Metric[]> {
        const fixture = await createBenchHost({ actors: [Tiny] });
        try {
            const ref = { type: Tiny.type, key: 'warm' };
            const call = benchCall();
            // Warm the slot to `active` and let the tiered compiler settle;
            // a cold or deactivating slot takes the slow path by design.
            for (let i = 0; i < 2_000; i++) {
                await fixture.host.dispatch(ref, 'noop', [], call);
            }

            // Chain a self-rescheduling microtask and count how many turns
            // drain before the dispatch settles. `done` stops the chain so
            // the loop cannot outlive the measurement.
            const turnsForOneDispatch = async (): Promise<number> => {
                let turns = 0;
                let done = false;
                const tick = (): void => {
                    if (done) return;
                    turns++;
                    queueMicrotask(tick);
                };
                queueMicrotask(tick);
                await fixture.host.dispatch(ref, 'noop', [], call);
                done = true;
                return turns;
            };

            const samples: number[] = [];
            for (let i = 0; i < 15; i++) samples.push(await turnsForOneDispatch());
            samples.sort((a, b) => a - b);

            return [
                {
                    name: 'microtask_turns',
                    value: samples[Math.floor(samples.length / 2)] as number,
                    unit: 'turns',
                    direction: 'lower',
                    // A count of turns through a fixed code path — the same
                    // on any machine. An `await` added to the hot dispatch
                    // path moves it by exactly one, which no timing metric on
                    // a shared runner could resolve.
                    exact: true
                },
                {
                    // Guards the claim that this metric is deterministic: if
                    // the spread is ever non-zero the median above stops
                    // being safe to gate on, and we want to see that.
                    name: 'microtask_turns_spread',
                    value: (samples[samples.length - 1] as number) - (samples[0] as number),
                    unit: 'turns',
                    direction: 'lower',
                    // Exact for the same reason, and it is the guard on the
                    // claim above: the day this stops being 0, the median
                    // stops being safe to gate on and we must hear about it.
                    exact: true,
                    noiseFloor: 0.5
                }
            ];
        } finally {
            await fixture.stop();
        }
    }
};

/**
 * The same count with the PRODUCTION deadline enabled — the rung that pays
 * the deadline machinery. Gates for the same reason `warm-turns` does: a
 * promise added to (or removed from) the deadline path moves the integer,
 * which no timing on a shared runner could resolve.
 *
 * The timer count rides along as informational rather than exact: whether a
 * shared registry's idle tick lands inside the counted window depends on
 * wall time, so the value is not deterministic by construction — but the
 * order of magnitude (per-call timers vs amortized-zero) is the finding.
 */
const warmTurnsDeadline: Scenario = {
    name: 'dispatch/warm-turns-deadline',
    description: 'microtask turns for ONE warm dispatch with the production deadline — the deadline path, as a count',
    async run(): Promise<Metric[]> {
        const fixture = await createBenchHost({
            actors: [Tiny],
            callTimeoutMs: PRODUCTION_CALL_TIMEOUT_MS
        });
        try {
            const ref = { type: Tiny.type, key: 'warm' };
            const call = benchCall();
            for (let i = 0; i < 2_000; i++) {
                await fixture.host.dispatch(ref, 'noop', [], call);
            }

            const turnsForOneDispatch = async (): Promise<number> => {
                let turns = 0;
                let done = false;
                const tick = (): void => {
                    if (done) return;
                    turns++;
                    queueMicrotask(tick);
                };
                queueMicrotask(tick);
                await fixture.host.dispatch(ref, 'noop', [], call);
                done = true;
                return turns;
            };

            const samples: number[] = [];
            for (let i = 0; i < 15; i++) samples.push(await turnsForOneDispatch());
            samples.sort((a, b) => a - b);

            // Count host timers created by 1000 sequential dispatches.
            const realSetTimeout = globalThis.setTimeout;
            let timers = 0;
            globalThis.setTimeout = ((...args: Parameters<typeof setTimeout>) => {
                timers++;
                return realSetTimeout(...args);
            }) as typeof setTimeout;
            try {
                for (let i = 0; i < 1_000; i++) {
                    await fixture.host.dispatch(ref, 'noop', [], call);
                }
            } finally {
                globalThis.setTimeout = realSetTimeout;
            }

            return [
                {
                    name: 'microtask_turns',
                    value: samples[Math.floor(samples.length / 2)] as number,
                    unit: 'turns',
                    direction: 'lower',
                    exact: true
                },
                {
                    name: 'microtask_turns_spread',
                    value: (samples[samples.length - 1] as number) - (samples[0] as number),
                    unit: 'turns',
                    direction: 'lower',
                    exact: true,
                    noiseFloor: 0.5
                },
                {
                    name: 'timers_per_1000_dispatches',
                    value: timers,
                    unit: 'timers',
                    direction: 'lower',
                    informational: true,
                    noiseFloor: 2
                }
            ];
        } finally {
            await fixture.stop();
        }
    }
};

/**
 * `warm-actor`'s interleaving twin — the same call over `AlwaysTiny`
 * (`reentrant: 'always'`). The delta against `warm-actor` prices what an
 * interleaved turn pays that a serial one does not: the AsyncLocalStorage
 * establishment per turn and the concurrent-lane bookkeeping. A timing, so
 * it informs and never gates.
 */
const alwaysWarmGrain: Scenario = {
    name: 'dispatch/always-warm-actor',
    description: "host.dispatch() to one warm reentrant: 'always' activation — the interleaved-lane tax vs warm-actor",
    async run(ctx: RunContext): Promise<Metric[]> {
        const fixture = await createBenchHost({ actors: [AlwaysTiny] });
        try {
            const ref = { type: AlwaysTiny.type, key: 'warm' };
            const call = benchCall();
            await fixture.host.dispatch(ref, 'noop', [], call);
            return await sweepConcurrency({
                call: () => fixture.host.dispatch(ref, 'noop', [], call),
                concurrencies: ctx.quick ? SHORT_SWEEP : FULL_SWEEP,
                durationMs: ctx.durationMs
            });
        } finally {
            await fixture.stop();
        }
    }
};

/**
 * `warm-turns` over the interleaved lane: the microtask count for one warm
 * dispatch to a `reentrant: 'always'` actor. Deterministic by construction
 * (a fixed code path, no clock, no randomness), so it gates exactly like
 * `warm-turns` — an accidental promise hop added to the interleaved path
 * moves the integer where no timing could resolve it.
 */
const alwaysWarmTurns: Scenario = {
    name: 'dispatch/always-warm-turns',
    description: "microtask turns for ONE warm dispatch to a reentrant: 'always' actor — the interleaved lane, as a count",
    async run(): Promise<Metric[]> {
        const fixture = await createBenchHost({ actors: [AlwaysTiny] });
        try {
            const ref = { type: AlwaysTiny.type, key: 'warm' };
            const call = benchCall();
            for (let i = 0; i < 2_000; i++) {
                await fixture.host.dispatch(ref, 'noop', [], call);
            }

            const turnsForOneDispatch = async (): Promise<number> => {
                let turns = 0;
                let done = false;
                const tick = (): void => {
                    if (done) return;
                    turns++;
                    queueMicrotask(tick);
                };
                queueMicrotask(tick);
                await fixture.host.dispatch(ref, 'noop', [], call);
                done = true;
                return turns;
            };

            const samples: number[] = [];
            for (let i = 0; i < 15; i++) samples.push(await turnsForOneDispatch());
            samples.sort((a, b) => a - b);

            return [
                {
                    name: 'microtask_turns',
                    value: samples[Math.floor(samples.length / 2)] as number,
                    unit: 'turns',
                    direction: 'lower',
                    exact: true
                },
                {
                    name: 'microtask_turns_spread',
                    value: (samples[samples.length - 1] as number) - (samples[0] as number),
                    unit: 'turns',
                    direction: 'lower',
                    exact: true,
                    noiseFloor: 0.5
                }
            ];
        } finally {
            await fixture.stop();
        }
    }
};

/**
 * Admission control (#384), counted.
 *
 * `overload-shed` offers one activation many times its capacity in a single
 * synchronous burst — the shape a drowning host sees — and counts what the
 * runtime did with each call. With `maxQueued` set, exactly `maxQueued`
 * calls are admitted (one running, the rest queued) and every later one is
 * refused before it is queued; the admitted ones all complete, none times
 * out. Every one of those is a count of synchronous decisions on a fixed
 * code path, so they are `exact`. The control arm has no cap and a short
 * deadline: it admits everything and most of it times out — that arm's
 * counts depend on wall time and stay informational, but they are the
 * "drown" the cap exists to replace.
 */
const OFFERED = 400;
const CAP = 8;

function slowActor(type: string, maxQueued: number) {
    return defineActor({
        type,
        allowAnonymous: true,
        maxQueued,
        state: () => ({ ran: 0 }),
        methods: (ctx) => ({
            async ms1() {
                ctx.state.ran++;
                await new Promise((r) => setTimeout(r, 1));
                return ctx.state.ran;
            },
            ran() {
                return ctx.state.ran;
            }
        })
    });
}

async function burst(
    type: string,
    maxQueued: number,
    callTimeoutMs: number
): Promise<{ admitted: number; refused: number; completed: number; timeouts: number; other: number; ran: number }> {
    const def = slowActor(type, maxQueued);
    const fixture = await createBenchHost({ actors: [def], callTimeoutMs });
    try {
        const ref = { type, key: 'hot' };
        // Warm the slot: an activation-in-progress parks callers rather
        // than queueing turns, and the cap is a property of the queue.
        await fixture.host.dispatch(ref, 'ran', [], benchCall());
        const outcomes = await Promise.allSettled(
            Array.from({ length: OFFERED }, () => fixture.host.dispatch(ref, 'ms1', [], benchCall()))
        );
        let refused = 0;
        let completed = 0;
        let timeouts = 0;
        let other = 0;
        for (const o of outcomes) {
            if (o.status === 'fulfilled') completed++;
            else if (isActorError(o.reason) && o.reason.kind === 'overloaded') refused++;
            else if (isActorError(o.reason) && o.reason.kind === 'call-timeout') timeouts++;
            else other++;
        }
        // Let skipped turns drain before reading how many bodies ran.
        await new Promise((r) => setTimeout(r, 20));
        const ran = (await fixture.host.dispatch(ref, 'ran', [], benchCall())) as number;
        return { admitted: OFFERED - refused, refused, completed, timeouts, other, ran };
    } finally {
        await fixture.stop();
    }
}

const overloadShed: Scenario = {
    name: 'dispatch/overload-shed',
    description: `one activation offered ${OFFERED} calls at once: admitted / refused / timed out, with and without maxQueued`,
    async run(): Promise<Metric[]> {
        const capped = await burst('BenchShedCapped', CAP, PRODUCTION_CALL_TIMEOUT_MS);
        const control = await burst('BenchShedOpen', 0, 100);
        const count = (name: string, value: number, exact: boolean, direction: 'lower' | 'higher' = 'lower'): Metric => ({
            name,
            value,
            unit: 'calls',
            direction,
            ...(exact ? { exact: true } : { informational: true })
        });
        return [
            // The cap arm: every value is a synchronous decision on a fixed
            // path, identical on any machine.
            count('cap/admitted', capped.admitted, true, 'higher'),
            count('cap/refused', capped.refused, true),
            count('cap/completed', capped.completed, true, 'higher'),
            count('cap/timeouts', capped.timeouts, true),
            count('cap/other_errors', capped.other, true),
            count('cap/bodies_run', capped.ran, true, 'higher'),
            // The control arm: no cap, a 100 ms deadline, 400 × ~1 ms of
            // turns — the drown. How many time out depends on wall time.
            count('none/refused', control.refused, true),
            count('none/timeouts', control.timeouts, false),
            count('none/completed', control.completed, false, 'higher'),
            count('none/bodies_run', control.ran, false)
        ];
    }
};

/**
 * Drop-on-dequeue (#384): a queued turn whose caller's deadline has
 * already passed is skipped, never run. Constructed rather than raced —
 * the deadlines are minted in the past — so the count is exact.
 */
const expiredSkipped: Scenario = {
    name: 'dispatch/expired-skipped',
    description: 'bodies run for calls whose deadline expired while queued — must be 0',
    async run(): Promise<Metric[]> {
        let release!: () => void;
        const gate = new Promise<void>((r) => (release = r));
        const def = defineActor({
            type: 'BenchExpired',
            allowAnonymous: true,
            state: () => ({ ran: 0 }),
            methods: (ctx) => ({
                async hold() {
                    ctx.state.ran++;
                    await gate;
                },
                bump() {
                    ctx.state.ran++;
                    return ctx.state.ran;
                },
                ran() {
                    return ctx.state.ran;
                }
            })
        });
        const fixture = await createBenchHost({ actors: [def] });
        try {
            const ref = { type: def.type, key: 'e' };
            const holding = fixture.host.dispatch(ref, 'hold', [], benchCall());
            await new Promise((r) => setTimeout(r, 5));
            const expired = Date.now() - 1;
            const queued = Array.from({ length: 200 }, () =>
                fixture.host.dispatch(ref, 'bump', [], benchCall({ deadline: expired }))
            );
            release();
            await holding;
            const outcomes = await Promise.allSettled(queued);
            const rejected = outcomes.filter(
                (o) => o.status === 'rejected' && isActorError(o.reason) && o.reason.kind === 'call-timeout'
            ).length;
            const ran = (await fixture.host.dispatch(ref, 'ran', [], benchCall())) as number;
            return [
                {
                    name: 'expired_bodies_run',
                    value: ran - 1,
                    unit: 'turns',
                    direction: 'lower',
                    exact: true
                },
                {
                    name: 'expired_rejected',
                    value: rejected,
                    unit: 'calls',
                    direction: 'higher',
                    exact: true
                }
            ];
        } finally {
            await fixture.stop();
        }
    }
};

export const dispatchScenarios: Scenario[] = [
    turnsRaw,
    warmGrain,
    warmGrainDeadline,
    viaProxy,
    fanOut,
    warmTurns,
    warmTurnsDeadline,
    alwaysWarmGrain,
    alwaysWarmTurns,
    overloadShed,
    expiredSkipped
];
