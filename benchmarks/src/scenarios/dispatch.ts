/**
 * The dispatch ladder.
 *
 * Each scenario adds exactly one layer to the one above it, so the cost of
 * a layer is a SUBTRACTION rather than a guess:
 *
 *   mailbox-raw          promise-chain turn serialization, no actor at all
 *   warm-grain           + placement, reentrancy check, activation lookup, turn
 *   warm-grain-deadline  + raceDeadline (the production default)
 *   via-proxy            + the client proxy and a freshly minted call context
 *   fan-out-grains       warm-grain across N activations (directory + parallelism)
 *
 * No single number here means much on its own; the gaps between them are
 * the finding.
 */
import { Mailbox } from '@sigx/actors/silo';
import { Tiny } from '../actors.ts';
import { sweepConcurrency } from '../loop.ts';
import {
    benchCall,
    createBenchSilo,
    PRODUCTION_CALL_TIMEOUT_MS,
    refsFor,
    warmActivations
} from '../silo-fixture.ts';
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
    name: 'dispatch/warm-grain',
    description: 'silo.dispatch() to one warm activation, no deadline (callTimeoutMs: 0)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const fixture = await createBenchSilo({ actors: [Tiny] });
        try {
            const ref = { type: Tiny.type, key: 'warm' };
            const call = benchCall();
            await fixture.silo.dispatch(ref, 'noop', [], call);
            return await sweepConcurrency({
                call: () => fixture.silo.dispatch(ref, 'noop', [], call),
                concurrencies: ctx.quick ? SHORT_SWEEP : FULL_SWEEP,
                durationMs: ctx.durationMs
            });
        } finally {
            await fixture.stop();
        }
    }
};

const warmGrainDeadline: Scenario = {
    name: 'dispatch/warm-grain-deadline',
    description: `same call with the PRODUCTION callTimeoutMs (${PRODUCTION_CALL_TIMEOUT_MS}ms) — the raceDeadline tax`,
    async run(ctx: RunContext): Promise<Metric[]> {
        const fixture = await createBenchSilo({
            actors: [Tiny],
            callTimeoutMs: PRODUCTION_CALL_TIMEOUT_MS
        });
        try {
            const ref = { type: Tiny.type, key: 'warm' };
            const call = benchCall();
            await fixture.silo.dispatch(ref, 'noop', [], call);
            return await sweepConcurrency({
                call: () => fixture.silo.dispatch(ref, 'noop', [], call),
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
    description: 'silo.actor(def, key).noop() — adds the client proxy and a minted call id',
    async run(ctx: RunContext): Promise<Metric[]> {
        const fixture = await createBenchSilo({ actors: [Tiny] });
        try {
            await fixture.silo.actor(Tiny, 'warm').noop();
            return await sweepConcurrency({
                call: () => fixture.silo.actor(Tiny, 'warm').noop(),
                concurrencies: ctx.quick ? SHORT_SWEEP : FULL_SWEEP,
                durationMs: ctx.durationMs
            });
        } finally {
            await fixture.stop();
        }
    }
};

/**
 * The scaling half of the contract. One grain serializes; N grains should
 * not — if throughput here tracks `warm-grain` instead of rising with
 * concurrency, something is serializing that should not be.
 */
const fanOut: Scenario = {
    name: 'dispatch/fan-out-grains',
    description: 'silo.dispatch() round-robin across 1 000 warm activations',
    async run(ctx: RunContext): Promise<Metric[]> {
        const fixture = await createBenchSilo({ actors: [Tiny] });
        try {
            const count = ctx.quick ? 100 : 1000;
            const refs = refsFor(Tiny.type, count);
            await warmActivations(fixture.silo, refs);
            const call = benchCall();
            return await sweepConcurrency({
                call: (i) =>
                    fixture.silo.dispatch(refs[i % count] as (typeof refs)[number], 'noop', [], call),
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
        const fixture = await createBenchSilo({ actors: [Tiny] });
        try {
            const ref = { type: Tiny.type, key: 'warm' };
            const call = benchCall();
            // Warm the slot to `active` and let the tiered compiler settle;
            // a cold or deactivating slot takes the slow path by design.
            for (let i = 0; i < 2_000; i++) {
                await fixture.silo.dispatch(ref, 'noop', [], call);
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
                await fixture.silo.dispatch(ref, 'noop', [], call);
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
                    direction: 'lower'
                },
                {
                    // Guards the claim that this metric is deterministic: if
                    // the spread is ever non-zero the median above stops
                    // being safe to gate on, and we want to see that.
                    name: 'microtask_turns_spread',
                    value: (samples[samples.length - 1] as number) - (samples[0] as number),
                    unit: 'turns',
                    direction: 'lower',
                    noiseFloor: 0.5
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
    warmTurns
];
