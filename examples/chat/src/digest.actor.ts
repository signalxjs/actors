/**
 * The stateless-worker surface, and its single-activation control arm.
 *
 * `Digest` is a `defineWorker` pool: pure compute, no identity-bound state,
 * so the host runs up to `maxLocal` interchangeable members per key and two
 * calls to the SAME key overlap. `DigestActor` is the identical body on
 * `defineActor` — one activation per key, so the same two calls queue.
 *
 * Running both against identical work is what turns "workers are concurrent"
 * into a number on real hardware (`infra/worker-pool`), and the returned
 * `member` id is what lets the assertion suite prove the overlap from
 * OUTSIDE the cluster: `methods:` is called once per pool member, so the
 * closure below mints one id per member and every stateful arm reports the
 * same id forever.
 *
 * Deliberately pure: the payload arrives as an argument rather than being
 * read back off a Room. A worker has no `state`, `save`, `reminders` or
 * `tasks` — the type says so — and keeping storage off this path is what
 * makes the A/B measure activation concurrency instead of Redis.
 */
import { createHash, randomUUID } from 'node:crypto';
import { defineActor, defineWorker } from './actors.app';
import { digestPool } from './digest-pool';
import { requireUser } from './guards';

/**
 * Members per (type, key) on ONE host. Left undefined by default so the
 * runtime's own `navigator.hardwareConcurrency` (clamped to 16) applies —
 * every unit test pins this explicitly, so the default has never actually
 * run on real hardware until now.
 */
const MAX_LOCAL = Number(process.env.DIGEST_MAX_LOCAL) || undefined;

/** How much CPU one call may ask for. A guarded endpoint is still an
 *  endpoint: without a ceiling `iterations` is a CPU-burn primitive. */
const MAX_ITERATIONS = Number(process.env.DIGEST_MAX_ITERS ?? 200_000);
const DEFAULT_ITERATIONS = Number(process.env.DIGEST_ITERS ?? 2_000);

export interface DigestResult {
    /** Identifies the pool MEMBER that ran this call. Distinct values for
     *  the same key are the whole point of a worker. */
    readonly member: string;
    readonly startedAt: number;
    readonly endedAt: number;
    readonly digest: string;
}

/** Hashes per uninterrupted slice — see the yield below for why there is one. */
const CHUNK = 500;

/**
 * A sha256 hash-chain — controllable, un-optimizable CPU — run in slices
 * with a yield between them.
 *
 * The yield is the load-bearing part. A pool gives a key many ACTIVATIONS,
 * not many threads: one `for` loop of synchronous hashing blocks the event
 * loop, and nothing — worker or not — overlaps with it. Chunking is also
 * simply how you write CPU work in a server that has to stay responsive,
 * so this is the realistic shape as well as the measurable one.
 */
async function burn(text: string, iterations: number): Promise<string> {
    let hash = createHash('sha256').update(text).digest();
    for (let done = 0; done < iterations; done += CHUNK) {
        const slice = Math.min(CHUNK, iterations - done);
        for (let i = 0; i < slice; i++) hash = createHash('sha256').update(hash).digest();
        // Explicit callback rather than `new Promise(setImmediate)`: that
        // spelling passes the executor straight to setImmediate, which then
        // calls `resolve(reject)` — it happens to work, on an argument quirk
        // nobody should have to reason about.
        await new Promise((resolve) => setImmediate(resolve));
    }
    return hash.toString('hex');
}

async function run(
    member: string,
    text: string,
    iterations: number | undefined
): Promise<DigestResult> {
    const startedAt = Date.now();
    const digest = await burn(
        text,
        Math.min(MAX_ITERATIONS, Math.max(1, Math.trunc(iterations ?? DEFAULT_ITERATIONS)))
    );
    return { member, startedAt, endedAt: Date.now(), digest };
}

export const Digest = defineWorker({
    type: 'Digest',
    use: [requireUser],
    ...(MAX_LOCAL === undefined ? {} : { maxLocal: MAX_LOCAL }),
    methods: () => {
        // Once per POOL MEMBER — the id is what makes concurrency visible.
        const member = randomUUID().slice(0, 8);
        return {
            // The pool key is the wire call's FIRST argument and the runtime
            // consumes it, so it is deliberately absent here.
            async summarize(text: string, iterations?: number): Promise<DigestResult> {
                return await run(member, text, iterations);
            }
        };
    }
});

/**
 * The control arm. Same work, one activation per key — so `member` is
 * constant and calls to one key serialize through its turns.
 */
export const DigestActor = defineActor({
    type: 'DigestActor',
    use: [requireUser],
    state: () => ({ calls: 0 }),
    methods: (ctx) => {
        const member = randomUUID().slice(0, 8);
        return {
            async summarize(text: string, iterations?: number): Promise<DigestResult> {
                ctx.state.calls += 1;
                return run(member, text, iterations);
            }
        };
    }
});

// --- the same two arms, with the compute moved OFF the event loop ---------
//
// Four arms in total, which is the whole point: activations and cores are
// independent axes, and only measuring both separates them.
//
//                      compute on the loop   compute on a thread
//   one activation   DigestActor           DigestActorThreaded
//   many activations  Digest                DigestThreaded
//
// The left column can never exceed one core no matter how many activations it
// has. The right column can — but only where something supplies more than
// one turn at a time, so `DigestActorThreaded` is expected to stay near the
// single-activation figure while `DigestThreaded` scales. That contrast is
// the measurement: it shows concurrency and parallelism are not the same
// knob, which is exactly the thing the first three arms could not show.

async function runThreaded(
    member: string,
    text: string,
    iterations: number | undefined
): Promise<DigestResult> {
    const startedAt = Date.now();
    const digest = await digestPool().run(
        text,
        Math.min(MAX_ITERATIONS, Math.max(1, Math.trunc(iterations ?? DEFAULT_ITERATIONS)))
    );
    return { member, startedAt, endedAt: Date.now(), digest };
}

/** Many activations AND many threads — the only genuinely parallel arm. */
export const DigestThreaded = defineWorker({
    type: 'DigestThreaded',
    use: [requireUser],
    ...(MAX_LOCAL === undefined ? {} : { maxLocal: MAX_LOCAL }),
    methods: () => {
        const member = randomUUID().slice(0, 8);
        return {
            async summarize(text: string, iterations?: number): Promise<DigestResult> {
                return await runThreaded(member, text, iterations);
            }
        };
    }
});

/**
 * One activation, threaded compute. Throughput should track the plain single
 * activation — one turn at a time is one thread busy — but the host's loop
 * stays free, which is the property `slowTurnMs` exists to complain about
 * and which no throughput number reports.
 */
export const DigestActorThreaded = defineActor({
    type: 'DigestActorThreaded',
    use: [requireUser],
    state: () => ({ calls: 0 }),
    methods: (ctx) => {
        const member = randomUUID().slice(0, 8);
        return {
            async summarize(text: string, iterations?: number): Promise<DigestResult> {
                ctx.state.calls += 1;
                return await runThreaded(member, text, iterations);
            }
        };
    }
});
