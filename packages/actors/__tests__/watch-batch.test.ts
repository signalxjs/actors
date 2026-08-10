/**
 * Watch reads must not monopolize the turn queue (#180).
 *
 * P distinct principals on one principal-consulting read are P watch loops
 * (#121), and before the watch read pump each loop enqueued its own turn:
 * a publish put O(P) read turns on the single serial queue, and anything
 * arriving behind them — an external call, or a NEW subscriber's seed —
 * waited for all of them. On the measured cluster that FIFO is the
 * establishment collapse: seeds starve behind re-reads, dialling clients
 * time out, and the retries feed back (#180's cliff between 100 and 250
 * identities).
 *
 * The contract pinned here: the whole watch population contributes O(1)
 * queue slots (a batch turn draining up to a slice of reads), an external
 * call runs after at most one slice, and a new subscriber's seed runs
 * before already-requested re-reads.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, type ActorCallContext } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import { stubServerApp } from '@sigx/server/testing';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

/** More loops than one pump slice, so fairness is observable. */
const P = 50;

/** `Feed#hold` parks on one of these; afterEach releases stragglers so a
 *  parked turn never outlives its test. */
const gates: Array<() => void> = [];

interface User {
    readonly id: string;
}

let host: Host | null = null;
let restore: (() => void) | undefined;

afterEach(async () => {
    const drain = setInterval(() => {
        for (const release of gates.splice(0)) release();
    }, 5);
    try {
        await host?.stop();
    } finally {
        clearInterval(drain);
    }
    host = null;
    restore?.();
    restore = undefined;
});

const Feed = defineActor({
    type: 'Feed',
    allowAnonymous: true,
    // The publish rides the interleaved lane so it can run WHILE `hold`
    // parks the serial lane — that is what makes the contended-queue state
    // constructible instead of raced-for: the re-read requests it triggers
    // land behind the parked turn and provably cannot run early.
    methodReentrancy: { increment: 'always' },
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        mine() {
            const me = (ctx.principal as User | null)?.id ?? 'nobody';
            return `${me}:${ctx.state.count}`;
        },
        probe() {
            return ctx.state.count;
        },
        async hold() {
            await new Promise<void>((resolve) => gates.push(resolve));
        },
        async increment() {
            ctx.state.count += 1;
            await ctx.save();
        }
    })
});

function callAs(id: string, signal: AbortSignal): ActorCallContext {
    return { callChain: [], callId: `call-${id}`, principal: id, abortSignal: signal };
}

function stub(decodes?: { count: number }): void {
    restore = stubServerApp({
        authenticate: () => ({ id: 'alice' }) satisfies User,
        codec: {
            encode: (principal) => (principal as User).id,
            decode: (encoded) => {
                if (decodes) decodes.count += 1;
                return encoded === '' ? null : ({ id: encoded } satisfies User);
            }
        }
    });
}

async function until(predicate: () => boolean, ms = 2000): Promise<void> {
    const started = Date.now();
    while (!predicate()) {
        if (Date.now() - started > ms) throw new Error('timed out waiting');
        await new Promise((resolve) => setTimeout(resolve, 5));
    }
}

/**
 * P distinct principals watching `mine`, sequential and awaited; then the
 * serial lane is parked (`hold`), and only THEN does the publish run —
 * on the interleaved lane, which the parked turn cannot block. Every
 * loop's re-read request therefore lands behind the parked turn and
 * provably cannot run before the caller releases the gate: the
 * contended-queue state a new arrival faces on a busy actor, constructed
 * instead of raced-for.
 *
 * Returns the parked turn's settlement WRAPPED in an object — `await`
 * would flatten a bare returned promise and wait for the parked turn
 * itself, which is exactly the deadlock the wrapper avoids.
 */
async function establishAndPublishWedged(
    h: Host,
    abort: AbortController,
    iterators: AsyncIterator<unknown>[]
): Promise<{ held: Promise<unknown> }> {
    const ref = { type: 'Feed', key: 'f' };
    for (let i = 0; i < P; i++) {
        const iterator = h
            .dispatchWatch!(ref, 'mine', [], callAs(`u${i}`, abort.signal), { throttleMs: 0 })
            [Symbol.asyncIterator]();
        iterators.push(iterator);
        await iterator.next();
    }
    const held = h.dispatch(ref, 'hold', [], {
        callChain: [],
        callId: 'hold',
        abortSignal: abort.signal
    });
    await until(() => gates.length === 1);
    await h.dispatch(ref, 'increment', [], {
        callChain: [],
        callId: 'writer',
        abortSignal: abort.signal
    });
    // Generous quiescence: the lane is parked, so nothing can RUN — this
    // only has to outlast the microtask hops between each loop's tick and
    // its read request being placed.
    await new Promise((resolve) => setTimeout(resolve, 20));
    return { held };
}

describe('the watch read pump', () => {
    it('an external call runs after at most one slice of re-reads, not all of them', async () => {
        stub();
        host = createHost({ actors: [Feed], defaults: quiet });
        await host.start();
        const abort = new AbortController();
        const order: string[] = [];
        const unobserve = host.observeTurns((_ref, method) => order.push(method));

        const iterators: AsyncIterator<unknown>[] = [];
        const { held } = await establishAndPublishWedged(host, abort, iterators);
        // The queue is parked, so nothing runs between here and the release:
        // the log now records only the contended-queue drain.
        order.length = 0;
        const probe = host.dispatch({ type: 'Feed', key: 'f' }, 'probe', [], {
            callChain: [],
            callId: 'probe',
            abortSignal: abort.signal
        });
        gates.splice(0).forEach((release) => release());
        await probe;
        await held;

        const readsBeforeProbe = order
            .slice(0, order.indexOf('probe'))
            .filter((m) => m === 'mine').length;
        // FIFO ran the probe after all P re-reads; the pump bounds the wait
        // to one slice.
        expect(readsBeforeProbe).toBeLessThan(P);

        unobserve();
        abort.abort();
        void iterators;
    });

    it("a new principal's seed runs before already-requested re-reads", async () => {
        stub();
        host = createHost({ actors: [Feed], defaults: quiet });
        await host.start();
        const abort = new AbortController();
        const order: Array<{ method: string; principal: string | undefined }> = [];
        const unobserve = host.observeTurns((_ref, method, _q, _e, _f, call) =>
            order.push({ method, principal: call?.principal })
        );

        const iterators: AsyncIterator<unknown>[] = [];
        const { held } = await establishAndPublishWedged(host, abort, iterators);
        // Parked queue — the log restarts at the contended-queue drain.
        order.length = 0;
        const late = host
            .dispatchWatch!({ type: 'Feed', key: 'f' }, 'mine', [], callAs('late', abort.signal), {
                throttleMs: 0
            })
            [Symbol.asyncIterator]();
        gates.splice(0).forEach((release) => release());
        expect((await late.next()).value).toBe('late:1');
        await held;

        const seedAt = order.findIndex((t) => t.principal === 'late');
        expect(seedAt).toBeGreaterThanOrEqual(0);
        const rereadsBeforeSeed = order
            .slice(0, seedAt)
            .filter((t) => t.method === 'mine').length;
        // FIFO seeded at position P+1 — the establishment starvation of
        // #180. The pump drains seeds ahead of re-reads.
        expect(rereadsBeforeSeed).toBeLessThan(P);

        unobserve();
        abort.abort();
        void iterators;
        void late;
    });

    it('re-reads across P principals pay one decode per identity, not one per turn', async () => {
        const decodes = { count: 0 };
        stub(decodes);
        host = createHost({ actors: [Feed], defaults: quiet });
        await host.start();
        const abort = new AbortController();
        const ref = { type: 'Feed', key: 'f' };

        const iterators: AsyncIterator<unknown>[] = [];
        for (const id of ['a', 'b', 'c']) {
            const iterator = host
                .dispatchWatch!(ref, 'mine', [], callAs(id, abort.signal), { throttleMs: 0 })
                [Symbol.asyncIterator]();
            iterators.push(iterator);
            await iterator.next();
        }
        for (let round = 0; round < 2; round++) {
            await host.dispatch(ref, 'increment', [], {
                callChain: [],
                callId: `w${round}`,
                abortSignal: abort.signal
            });
            for (const iterator of iterators) await iterator.next();
        }

        // Three identities → three decodes, memoized across every later
        // read turn. The single-slot memo thrashed: one decode per read
        // turn once loops for distinct principals interleave.
        expect(decodes.count).toBe(3);

        abort.abort();
    });
});
