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

export const dispatchScenarios: Scenario[] = [
    mailboxRaw,
    warmGrain,
    warmGrainDeadline,
    viaProxy,
    fanOut
];
