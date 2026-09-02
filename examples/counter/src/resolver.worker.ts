import { defineActor, defineWorker } from './actors.app.ts';

/**
 * `defineWorker` beside the `defineActor` it differs from — the same method
 * on both, so the ONLY variable is what happens when two calls reach the
 * same key at once.
 *
 * An actor is one activation per key, one turn at a time: the second call
 * queues behind the first. That serialization is what makes `ctx.state`
 * safe, and it is the wrong shape for stateless work — a lookup against a
 * slow upstream has nothing to protect and no reason to wait in line.
 *
 * A worker has no state, so it has no identity to protect. The runtime keeps
 * a POOL of up to `maxLocal` interchangeable activations per key on each
 * host, grows it under pressure, and hands the second concurrent call to a
 * second member. Two calls to the same key overlap. That is the contract,
 * and it is why everything identity-bound — `state`, `save`, `reminders`,
 * `tasks`, `subscriptions` — is structurally absent from its options.
 *
 * What a pool is NOT: threads. A host is one Node process with one JS
 * thread, so members interleave at `await` points on the same loop. The
 * pool buys concurrency for work that WAITS — I/O, an upstream, a model
 * API — and exactly nothing for work that computes: measured on a 1-core
 * host at 289 ops/s for a pool and 289 for a single activation. For CPU
 * work, pair it with `worker_threads` (see `perf/app/src/digest-pool.ts`);
 * the pool is then what lets several turns be in flight on several cores.
 */

/** A stand-in for the slow upstream — the wait is the point, not the answer. */
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * In flight per definition, at MODULE scope so it counts across pool
 * members: `methods:` runs once per activation, and for a worker that is
 * once per MEMBER, so a counter in that closure would only ever see itself.
 */
const inFlight = { Resolver: 0, SerialResolver: 0 };
let minted = 0;

async function lookup(lane: keyof typeof inFlight, member: number, latencyMs: number) {
    // How many calls were already inside when this one entered. Under a
    // single activation this is 0, always.
    const overlapping = inFlight[lane]++;
    try {
        await sleep(latencyMs);
        return { member, overlapping };
    } finally {
        inFlight[lane]--;
    }
}

export const Resolver = defineWorker({
    type: 'Resolver',
    allowAnonymous: true,
    // Pool cap per (type, key) per host. The default is the core count
    // clamped to [4, 16]; stated here so the demo's numbers do not depend
    // on the machine running it.
    maxLocal: 4,
    // Once per pool MEMBER — hence the minted id. Distinct ids inside one
    // burst are distinct activations serving one key.
    methods: () => {
        const member = ++minted;
        return {
            async lookup(latencyMs: number) {
                return lookup('Resolver', member, latencyMs);
            }
        };
    }
});

/** The same method under `defineActor`: one activation, and calls take turns. */
export const SerialResolver = defineActor({
    type: 'SerialResolver',
    allowAnonymous: true,
    state: () => ({}),
    methods: () => ({
        async lookup(latencyMs: number) {
            return lookup('SerialResolver', 1, latencyMs);
        }
    })
});
