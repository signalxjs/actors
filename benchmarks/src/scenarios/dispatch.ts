/**
 * The dispatch ladder.
 *
 * Each scenario adds exactly one layer to the one above it, so the cost of
 * a layer is a SUBTRACTION rather than a guess:
 *
 *   mailbox-raw          promise-chain turn serialization, no actor at all
 *   warm-actor           + placement, reentrancy check, activation lookup, turn
 *   warm-actor-deadline  + the call-deadline machinery (the production default)
 *   via-proxy            + the client proxy and a freshly minted call context
 *   fan-out-actors       warm-actor across N activations (directory + parallelism)
 *
 * No single number here means much on its own; the gaps between them are
 * the finding.
 */
import { Mailbox } from '@sigx/actors/host';
import { Tiny } from '../actors.ts';
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

const mailboxRaw: Scenario = {
    name: 'dispatch/mailbox-raw',
    description: 'Mailbox.run() alone — the turn-serialization floor, no actor',
    async run(ctx: RunContext): Promise<Metric[]> {
        const mailbox = new Mailbox();
        return sweepConcurrency({
            call: () => mailbox.run(() => 0),
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

export const dispatchScenarios: Scenario[] = [
    mailboxRaw,
    warmGrain,
    warmGrainDeadline,
    viaProxy,
    fanOut,
    warmTurns,
    warmTurnsDeadline
];
