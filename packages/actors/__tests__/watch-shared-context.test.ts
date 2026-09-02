/**
 * A shared watch turn carries NO subscriber's per-request context (#137).
 *
 * A coalesced watch loop serves many subscribers with one read, so the
 * read cannot be allowed to observe the CREATING subscriber's `ctx.bag` or
 * to hang a cross-host hop on the creating subscriber's abort signal: the
 * first would hand later subscribers a value computed for someone else's
 * request, the second would fail everyone's loop the moment the first
 * subscriber left. Inside a shared watch turn `ctx.bag` is the empty bag
 * and the call's abort signal is the watch's own — it fires only when the
 * shared loop itself is torn down.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, type ActorCallContext } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import { createCluster, quiet, selfPolicy, type ClusterHarness } from './harness';

let host: Host | null = null;
let running: ClusterHarness | null = null;

afterEach(async () => {
    await host?.stop();
    host = null;
    await running?.stop();
    running = null;
});

/** A wire-shaped call context stamped with `user` in its bag. */
function callAs(user: string): { call: ActorCallContext; abort: AbortController } {
    const abort = new AbortController();
    return {
        abort,
        call: {
            callChain: [],
            callId: `call-${user}`,
            bag: Object.freeze({ user }),
            abortSignal: abort.signal
        }
    };
}

/** A pull-on-demand subscriber over an open watch. */
function subscriber(iterable: AsyncIterable<unknown>): {
    next: () => Promise<unknown>;
    close: () => Promise<void>;
} {
    const iterator = iterable[Symbol.asyncIterator]();
    return {
        next: async () => (await iterator.next()).value,
        close: async () => void (await iterator.return?.(undefined))
    };
}

describe('shared watch call context (#137)', () => {
    it('a shared read never observes the creating subscriber’s bag — nor relays it over a hop', async () => {
        const seen: (string | undefined)[] = [];
        const hopped: Record<string, string>[] = [];
        const Echo = defineActor({
            type: 'BagEcho',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({
                async bagOf(): Promise<Record<string, string>> {
                    return { ...ctx.bag };
                }
            })
        });
        const Board = defineActor({
            type: 'Board',
            allowAnonymous: true,
            state: () => ({ posts: ['hello'] }),
            methods: (ctx) => ({
                async posts(): Promise<string[]> {
                    seen.push(ctx.bag.user);
                    hopped.push(
                        await (
                            ctx.actor(Echo, 'e') as { bagOf(): Promise<Record<string, string>> }
                        ).bagOf()
                    );
                    return [...ctx.state.posts];
                },
                async post(text: string): Promise<void> {
                    ctx.state.posts.push(text);
                },
                async whoami(): Promise<string | undefined> {
                    return ctx.bag.user;
                }
            })
        });
        host = createHost({ actors: [Board, Echo], defaults: quiet });
        await host.start();
        const ref = { type: 'Board', key: 'b' };
        const alice = callAs('alice');
        const bob = callAs('bob');

        const a = subscriber(host.dispatchWatch!(ref, 'posts', [], alice.call, { throttleMs: 0 }));
        expect(await a.next()).toEqual(['hello']);
        const b = subscriber(host.dispatchWatch!(ref, 'posts', [], bob.call, { throttleMs: 0 }));
        expect(await b.next()).toEqual(['hello']);

        await host.dispatch(ref, 'post', ['x'], callAs('carol').call);
        expect(await a.next()).toEqual(['hello', 'x']);
        expect(await b.next()).toEqual(['hello', 'x']);

        // Two reads served both subscribers; neither ran under Alice's bag,
        // and neither relayed it into the hop.
        expect(seen).toEqual([undefined, undefined]);
        expect(hopped).toEqual([{}, {}]);

        // The plain call path is untouched: a turn a caller dispatched
        // still reads that caller's bag.
        expect(await host.dispatch(ref, 'whoami', [], alice.call)).toBe('alice');

        await a.close();
        await b.close();
    });

    it('the first subscriber leaving does not abort a hop the shared read has in flight', async () => {
        let calls = 0;
        let gate: Promise<void> | null = null;
        let parked: (() => void) | null = null;
        const Other = defineActor({
            type: 'Other',
            allowAnonymous: true,
            state: () => ({}),
            methods: () => ({
                async value(): Promise<string> {
                    calls++;
                    parked?.();
                    if (gate) await gate;
                    return 'v';
                }
            })
        });
        const Watched = defineActor({
            type: 'Watched',
            allowAnonymous: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                async view(): Promise<string> {
                    const v = await (ctx.actor(Other, 'o') as { value(): Promise<string> }).value();
                    return `${ctx.state.n}:${v}`;
                },
                async bump(): Promise<void> {
                    ctx.state.n++;
                }
            })
        });
        const harness = await createCluster(2, {
            actors: [Other, Watched],
            policy: selfPolicy,
            // The in-memory pipe answers whatever the signal does; real fetch
            // REJECTS an in-flight request the moment its signal aborts —
            // which is exactly what makes the hop's signal matter.
            wrapFetch: (inner) => (input, init) => {
                const signal = init?.signal ?? (input instanceof Request ? input.signal : null);
                if (!signal) return inner(input, init);
                return new Promise<Response>((resolve, reject) => {
                    const onAbort = (): void =>
                        reject(signal.reason ?? new DOMException('aborted', 'AbortError'));
                    if (signal.aborted) return onAbort();
                    signal.addEventListener('abort', onAbort, { once: true });
                    inner(input, init)
                        .then(resolve, reject)
                        .finally(() => signal.removeEventListener('abort', onAbort));
                });
            }
        });
        running = harness;
        const otherRef = { type: 'Other', key: 'o' };
        const ref = { type: 'Watched', key: 'w' };
        // selfPolicy: the first toucher owns. `Other` on host 1, so every
        // hop from `Watched` (owned by host 0 below) crosses the wire —
        // where the call's abort signal is honoured.
        expect(await harness.hosts[1]!.dispatch(otherRef, 'value', [], callAs('seed').call)).toBe(
            'v'
        );

        const alice = callAs('alice');
        const bob = callAs('bob');
        const a = subscriber(
            harness.hosts[0]!.dispatchWatch!(ref, 'view', [], alice.call, { throttleMs: 0 })
        );
        expect(await a.next()).toBe('0:v');
        const b = subscriber(
            harness.hosts[0]!.dispatchWatch!(ref, 'view', [], bob.call, { throttleMs: 0 })
        );
        expect(await b.next()).toBe('0:v');
        expect(calls).toBe(2);

        // Park the NEXT hop, trigger the re-read, and let Alice — the
        // entry's creator — leave while the shared read is awaiting it.
        let release!: () => void;
        gate = new Promise<void>((resolve) => {
            release = resolve;
        });
        const parkedAt = new Promise<void>((resolve) => {
            parked = resolve;
        });
        await harness.hosts[0]!.dispatch(ref, 'bump', [], callAs('mutator').call);
        await parkedAt;
        alice.abort.abort();
        release();

        // Bob's loop survives Alice's departure and delivers the re-read.
        expect(await b.next()).toBe('1:v');
        expect(calls).toBe(3);

        await a.close();
        await b.close();
    });
});
