/**
 * The wire path, without a socket.
 *
 * `handleActorRequest` takes a `Request` and returns a `Response`, so the
 * whole endpoint — origin policy, content-type gate, body read, wire codec,
 * guard chain, error masking — runs against an in-memory Request. What is
 * left out is loopback TCP, and that is deliberate: kernel networking adds
 * tens of microseconds of variance that would swamp the thing we can
 * actually act on, which is our own serialization cost. Subtracting
 * `dispatch/via-proxy` AND `wire/request-construction` from this gives the
 * per-request wire overhead — the roundtrip rung builds its `Request` inside
 * the timed closure (a body is single-use, so it cannot be hoisted), and the
 * construction rung prices that setup so the subtraction is explicit rather
 * than assumed.
 *
 * Guards are also priced here rather than in the dispatch ladder, because
 * they run OUTSIDE any turn and are not on `host.dispatch` at all —
 * `host.actor()` never invokes them. Comparing a guarded actor against an
 * unguarded one over this same path is the only way to see them, and it
 * needs no new runtime seam.
 */
import { handleActorRequest } from '@sigx/actors/server';
import { Guarded, Tiny, Unguarded } from '../actors.ts';
import { closedLoop, sweepConcurrency } from '../loop.ts';
import { createBenchHost } from '../host-fixture.ts';
import type { Metric, RunContext, Scenario } from '../types.ts';

const CONCURRENCIES = [1, 64] as const;

/** POST {base}/{Type}%23{method} with `{"args": [key, ...args]}`. */
function actorRequest(type: string, method: string, args: readonly unknown[]): Request {
    return new Request(`http://bench.test/_sigx/actor/${encodeURIComponent(`${type}#${method}`)}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ args })
    });
}

async function assertOk(response: Response, label: string): Promise<void> {
    if (!response.ok) {
        throw new Error(`${label}: wire call failed ${response.status} ${await response.text()}`);
    }
}

/**
 * The construction-only control: what `actorRequest()` alone costs. undici's
 * `Request` allocates a `_Request`, a `_HeadersList` and an `extractBody`
 * pass, plus our `JSON.stringify` — work `endpoint-roundtrip` charges to the
 * endpoint because it cannot pre-build consumable bodies. Measured at c=1:
 * construction is synchronous, so a concurrency sweep would measure nothing
 * (the `async` wrapper's promise is the same scaffolding every rung's closed
 * loop carries). First in the ladder — floor-first, like the rest.
 */
const requestConstruction: Scenario = {
    name: 'wire/request-construction',
    description: 'actorRequest() alone — URL building, JSON.stringify, new Request(...) — the harness setup endpoint-roundtrip pays inside its timed closure, priced so it can be subtracted',
    async run(ctx: RunContext): Promise<Metric[]> {
        const call = async (): Promise<Request> => actorRequest(Tiny.type, 'noop', ['warm']);
        const outcome = await closedLoop({
            call,
            concurrency: 1,
            durationMs: ctx.durationMs,
            latency: false
        });
        return [
            {
                name: 'ops_per_sec',
                value: outcome.opsPerSec,
                unit: 'ops/s',
                direction: 'higher'
            }
        ];
    }
};

const endpointRoundtrip: Scenario = {
    name: 'wire/endpoint-roundtrip',
    description: 'handleActorRequest() with an in-memory Request — encode, JSON, revive, dispatch (includes Request construction; subtract wire/request-construction)',
    async run(ctx: RunContext): Promise<Metric[]> {
        const fixture = await createBenchHost({ actors: [Tiny] });
        try {
            const call = (): Promise<Response> =>
                handleActorRequest(actorRequest(Tiny.type, 'noop', ['warm']), {
                    host: fixture.host,
                    origin: false
                });
            await assertOk(await call(), 'wire/endpoint-roundtrip');
            return await sweepConcurrency({
                call,
                concurrencies: CONCURRENCIES,
                durationMs: ctx.durationMs
            });
        } finally {
            await fixture.stop();
        }
    }
};

const guardCost: Scenario = {
    name: 'wire/guard-chain',
    description: 'same wire call, unguarded vs a two-guard chain — what guards cost per request',
    async run(ctx: RunContext): Promise<Metric[]> {
        const fixture = await createBenchHost({ actors: [Unguarded, Guarded] });
        try {
            const metrics: Metric[] = [];
            for (const [label, def] of [
                ['unguarded', Unguarded],
                ['guarded', Guarded]
            ] as const) {
                const call = (): Promise<Response> =>
                    handleActorRequest(actorRequest(def.type, 'noop', ['warm']), {
                        host: fixture.host,
                        origin: false
                    });
                await assertOk(await call(), `wire/guard-chain:${label}`);
                // Per-variant warmup. Whichever variant runs first would
                // otherwise pay for tiering up the shared endpoint code and
                // hand the second one a free head start — which is exactly
                // how this scenario first reported guards as making things
                // FASTER.
                await closedLoop({ call, concurrency: 1, durationMs: 100, latency: false });
                const outcome = await closedLoop({
                    call,
                    concurrency: 1,
                    durationMs: ctx.durationMs,
                    latency: false
                });
                metrics.push({
                    name: `${label}/ops_per_sec`,
                    value: outcome.opsPerSec,
                    unit: 'ops/s',
                    direction: 'higher'
                });
            }
            return metrics;
        } finally {
            await fixture.stop();
        }
    }
};

export const wireScenarios: Scenario[] = [requestConstruction, endpointRoundtrip, guardCost];
