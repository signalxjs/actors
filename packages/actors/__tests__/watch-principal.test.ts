/**
 * A shared watch must not serve one subscriber another's view.
 *
 * `openWatch` keys on `(method, throttleMs, args)` and captures the FIRST
 * subscriber's `ActorCallContext` into the loop's `invoke`. A read that
 * derives its result from `ctx.principal` therefore computes ONE answer —
 * the first subscriber's — and pushes it to everyone on that key.
 *
 * Nothing is bypassed: both subscribers are authorized for the call they
 * made. The defect is in what they SEE.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, type ActorCallContext } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import { stubServerApp } from '@sigx/server/testing';
import { createCluster, selfPolicy } from './harness';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

interface User {
    readonly id: string;
}

let host: Host | null = null;
let restore: (() => void) | undefined;

afterEach(async () => {
    await host?.stop();
    host = null;
    restore?.();
    restore = undefined;
});

/**
 * A read whose result depends on the CALLER, not on its arguments — the
 * per-caller filtering `ctx.principal` exists to make possible.
 */
const Mailbox = defineActor({
    type: 'Mailbox',
    allowAnonymous: true,
    state: () => ({
        messages: [
            { to: 'alice', body: 'a1' },
            { to: 'bob', body: 'b1' }
        ]
    }),
    methods: (ctx) => ({
        async inbox(limit: number): Promise<string[]> {
            const me = (ctx.principal as User | null)?.id ?? null;
            return ctx.state.messages
                .filter((m) => m.to === me)
                .map((m) => m.body)
                .slice(-limit);
        }
    })
});

/** A wire-shaped call context carrying `id` as the encoded principal. */
function callAs(id: string): { call: ActorCallContext; abort: AbortController } {
    const abort = new AbortController();
    return {
        abort,
        call: {
            callChain: [],
            callId: `call-${id}`,
            principal: id,
            abortSignal: abort.signal
        }
    };
}

/**
 * Open a watch and take its initial read, leaving the subscription OPEN.
 *
 * Closing it would matter: the last subscriber out runs `onEmpty`, which
 * deletes the entry from the activation's map, so a later subscriber builds
 * a fresh loop under its own context and sees the right answer. Real
 * subscribers overlap — that is what a live page is — so the test has to.
 */
function openWatch(iterable: AsyncIterable<unknown>): {
    first: Promise<unknown>;
    close: () => Promise<void>;
} {
    const iterator = iterable[Symbol.asyncIterator]();
    return {
        first: iterator.next().then((r) => r.value),
        close: async () => void (await iterator.return?.(undefined))
    };
}

describe('a shared watch and ctx.principal', () => {
    it("serves each subscriber its OWN view, not the first subscriber's", async () => {
        // The codec is what makes `ctx.principal` decode inside the turn;
        // without it identity stops at the entry point and this would pass
        // vacuously with both sides reading null.
        restore = stubServerApp({
            authenticate: () => ({ id: 'alice' }) satisfies User,
            codec: {
                encode: (principal) => (principal as User).id,
                decode: (encoded) => (encoded === '' ? null : ({ id: encoded } satisfies User))
            }
        });
        host = createHost({ actors: [Mailbox], defaults: quiet });
        await host.start();

        const ref = { type: 'Mailbox', key: 'shared' };
        const alice = callAs('alice');
        const bob = callAs('bob');

        // Alice subscribes first, so hers is the context the shared loop
        // captures. Same actor, same method, same args as Bob's — which is
        // exactly the case a per-caller read produces, because the caller
        // is NOT an argument.
        const aliceWatch = openWatch(host.dispatchWatch!(ref, 'inbox', [10], alice.call));
        const aliceValue = await aliceWatch.first;

        // Bob joins while Alice is still watching — the overlap is the case.
        const bobWatch = openWatch(host.dispatchWatch!(ref, 'inbox', [10], bob.call));
        const bobValue = await bobWatch.first;

        await aliceWatch.close();
        await bobWatch.close();
        alice.abort.abort();
        bob.abort.abort();

        expect(aliceValue).toEqual(['a1']);
        // Bob is authorized for this call and made it under his own
        // identity. He must not be handed Alice's inbox.
        expect(bobValue).toEqual(['b1']);
    });

    it('does not leak across hosts either — the owner shares one loop for the cluster', async () => {
        restore = stubServerApp({
            authenticate: () => ({ id: 'alice' }) satisfies User,
            codec: {
                encode: (principal) => (principal as User).id,
                decode: (encoded) => (encoded === '' ? null : ({ id: encoded } satisfies User))
            }
        });
        // `selfPolicy` makes placement deterministic: whoever dispatches
        // first activates locally. Host 0 claims the actor, so both watches
        // opened on host 1 cross to it.
        const harness = await createCluster(2, { actors: [Mailbox], policy: selfPolicy });
        try {
            const ref = { type: 'Mailbox', key: 'shared' };
            const alice = callAs('alice');
            const bob = callAs('bob');

            // Claim on host 0.
            await harness.hosts[0]!.dispatch(ref, 'inbox', [10], callAs('alice').call);

            const aliceWatch = openWatch(
                harness.hosts[1]!.dispatchWatch!(ref, 'inbox', [10], alice.call)
            );
            const aliceValue = await aliceWatch.first;
            const bobWatch = openWatch(
                harness.hosts[1]!.dispatchWatch!(ref, 'inbox', [10], bob.call)
            );
            const bobValue = await bobWatch.first;

            await aliceWatch.close();
            await bobWatch.close();
            alice.abort.abort();
            bob.abort.abort();

            expect(aliceValue).toEqual(['a1']);
            expect(bobValue).toEqual(['b1']);
        } finally {
            await harness.stop();
        }
    });
});
