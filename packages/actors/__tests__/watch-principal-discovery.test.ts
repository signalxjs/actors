/**
 * Discovery-based principal qualification of shared watches (#121).
 *
 * A read that never consults `ctx.principal` keeps ONE loop for every
 * subscriber — the property watch.ts exists for. The moment a read is
 * OBSERVED consulting it, that key's loop splits per encoded principal:
 * subscribers who presented a different one are evicted before the
 * discovering invocation's value is pushed, and transparently re-attach
 * under their own qualified key.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, type ActorCallContext } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import { stubServerApp } from '@sigx/server/testing';
import { qualifyWatchKey, watchKey } from '../src/host/watch';
import { createCluster, quiet, selfPolicy } from './harness';

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

/** The codec that makes `ctx.principal` decode inside a turn. */
function stub(): void {
    restore = stubServerApp({
        authenticate: () => ({ id: 'alice' }) satisfies User,
        codec: {
            encode: (principal) => (principal as User).id,
            decode: (encoded) => (encoded === '' ? null : ({ id: encoded } satisfies User))
        }
    });
}

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

describe('watch principal discovery', () => {
    it('a read that never consults the principal keeps one loop across principals', async () => {
        stub();
        let invocations = 0;
        const Board = defineActor({
            type: 'Board',
            allowAnonymous: true,
            state: () => ({ posts: ['hello'] }),
            methods: (ctx) => ({
                async posts(): Promise<string[]> {
                    invocations++;
                    return [...ctx.state.posts];
                },
                async post(text: string): Promise<void> {
                    ctx.state.posts.push(text);
                }
            })
        });
        host = createHost({ actors: [Board], defaults: quiet });
        await host.start();
        const ref = { type: 'Board', key: 'b' };
        const alice = callAs('alice');
        const bob = callAs('bob');

        const a = subscriber(host.dispatchWatch!(ref, 'posts', [], alice.call, { throttleMs: 0 }));
        expect(await a.next()).toEqual(['hello']);
        const b = subscriber(host.dispatchWatch!(ref, 'posts', [], bob.call, { throttleMs: 0 }));
        expect(await b.next()).toEqual(['hello']);
        // Bob was seeded from the shared loop's replay — no second read.
        expect(invocations).toBe(1);

        await host.dispatch(ref, 'post', ['x'], callAs('carol').call);
        expect(await a.next()).toEqual(['hello', 'x']);
        expect(await b.next()).toEqual(['hello', 'x']);
        // ONE re-invocation served both subscribers.
        expect(invocations).toBe(2);

        await a.close();
        await b.close();
    });

    it('same-principal subscribers share a loop even after discovery', async () => {
        stub();
        let invocations = 0;
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
                async inbox(): Promise<string[]> {
                    invocations++;
                    const me = (ctx.principal as User | null)?.id ?? null;
                    return ctx.state.messages.filter((m) => m.to === me).map((m) => m.body);
                }
            })
        });
        host = createHost({ actors: [Mailbox], defaults: quiet });
        await host.start();
        const ref = { type: 'Mailbox', key: 'm' };

        // First subscriber discovers the dependence during the initial read.
        const alice1 = subscriber(
            host.dispatchWatch!(ref, 'inbox', [], callAs('alice').call, { throttleMs: 0 })
        );
        expect(await alice1.next()).toEqual(['a1']);
        expect(invocations).toBe(1);

        // A NEW same-principal subscriber lands on the qualified key — a
        // second loop while the pre-discovery entry drains (the accepted
        // trade-off of immutable entry keys)...
        const alice2 = subscriber(
            host.dispatchWatch!(ref, 'inbox', [], callAs('alice').call, { throttleMs: 0 })
        );
        expect(await alice2.next()).toEqual(['a1']);
        expect(invocations).toBe(2);

        // ...and every subsequent same-principal subscriber SHARES it.
        const alice3 = subscriber(
            host.dispatchWatch!(ref, 'inbox', [], callAs('alice').call, { throttleMs: 0 })
        );
        expect(await alice3.next()).toEqual(['a1']);
        expect(invocations).toBe(2);

        // A different principal gets a loop of its own, and its own view.
        const bob = subscriber(
            host.dispatchWatch!(ref, 'inbox', [], callAs('bob').call, { throttleMs: 0 })
        );
        expect(await bob.next()).toEqual(['b1']);
        expect(invocations).toBe(3);

        await alice1.close();
        await alice2.close();
        await alice3.close();
        await bob.close();
    });

    it('a subscriber who joined BEFORE discovery is evicted before the first push and re-reads under its own identity', async () => {
        stub();
        let parked!: () => void;
        const parkedAt = new Promise<void>((resolve) => {
            parked = resolve;
        });
        let release!: () => void;
        const gate = new Promise<void>((resolve) => {
            release = resolve;
        });
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
                async inbox(): Promise<string[]> {
                    // Park BEFORE touching the principal, so a second
                    // subscriber can join the still-unqualified entry while
                    // the discovering read is in flight.
                    parked();
                    await gate;
                    const me = (ctx.principal as User | null)?.id ?? null;
                    return ctx.state.messages.filter((m) => m.to === me).map((m) => m.body);
                }
            })
        });
        host = createHost({ actors: [Mailbox], defaults: quiet });
        await host.start();
        const ref = { type: 'Mailbox', key: 'm' };

        const alice = subscriber(
            host.dispatchWatch!(ref, 'inbox', [], callAs('alice').call, { throttleMs: 0 })
        );
        const aliceFirst = alice.next();
        await parkedAt;
        // Bob joins the shared entry while Alice's read is parked — the
        // entry has no idea yet that the read consults the principal.
        const bob = subscriber(
            host.dispatchWatch!(ref, 'inbox', [], callAs('bob').call, { throttleMs: 0 })
        );
        const bobFirst = bob.next();
        release();

        expect(await aliceFirst).toEqual(['a1']);
        // Bob's FIRST value is his own: the sweep evicted him before the
        // push, and his re-attached loop read under his identity.
        expect(await bobFirst).toEqual(['b1']);

        await alice.close();
        await bob.close();
    });

    it('discovery can happen LATE, and splits the loop from that moment on', async () => {
        stub();
        const Feed = defineActor({
            type: 'Feed',
            allowAnonymous: true,
            state: () => ({ personalized: false }),
            methods: (ctx) => ({
                async view(): Promise<string> {
                    if (!ctx.state.personalized) return 'public';
                    const me = (ctx.principal as User | null)?.id ?? 'anon';
                    return `for:${me}`;
                },
                async personalize(): Promise<void> {
                    ctx.state.personalized = true;
                }
            })
        });
        host = createHost({ actors: [Feed], defaults: quiet });
        await host.start();
        const ref = { type: 'Feed', key: 'f' };

        const alice = subscriber(
            host.dispatchWatch!(ref, 'view', [], callAs('alice').call, { throttleMs: 0 })
        );
        const bob = subscriber(
            host.dispatchWatch!(ref, 'view', [], callAs('bob').call, { throttleMs: 0 })
        );
        // Shared while the read ignores identity.
        expect(await alice.next()).toBe('public');
        expect(await bob.next()).toBe('public');

        await host.dispatch(ref, 'personalize', [], callAs('mutator').call);

        // The re-invocation (under Alice, the entry's creator) consults the
        // principal for the first time: Bob is evicted before its value is
        // pushed and re-reads under his own identity. Neither ever sees the
        // other's view.
        expect(await alice.next()).toBe('for:alice');
        expect(await bob.next()).toBe('for:bob');

        // Dependence is sticky: a later subscriber goes straight to its
        // qualified key and its own view — even after the others leave.
        await alice.close();
        await bob.close();
        const carol = subscriber(
            host.dispatchWatch!(ref, 'view', [], callAs('carol').call, { throttleMs: 0 })
        );
        expect(await carol.next()).toBe('for:carol');
        await carol.close();
    });

    it('does not share across hosts either — and same-principal watchers still share the owner loop', async () => {
        stub();
        let invocations = 0;
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
                async inbox(): Promise<string[]> {
                    invocations++;
                    const me = (ctx.principal as User | null)?.id ?? null;
                    return ctx.state.messages.filter((m) => m.to === me).map((m) => m.body);
                }
            })
        });
        const harness = await createCluster(2, { actors: [Mailbox], policy: selfPolicy });
        try {
            const ref = { type: 'Mailbox', key: 'shared' };
            // Claim on host 0, so every watch from host 1 crosses to it.
            await harness.hosts[0]!.dispatch(ref, 'inbox', [], callAs('seed').call);
            expect(invocations).toBe(1);

            const alice = subscriber(
                harness.hosts[1]!.dispatchWatch!(ref, 'inbox', [], callAs('alice').call)
            );
            expect(await alice.next()).toEqual(['a1']);
            expect(invocations).toBe(2);

            const bob1 = subscriber(
                harness.hosts[1]!.dispatchWatch!(ref, 'inbox', [], callAs('bob').call)
            );
            expect(await bob1.next()).toEqual(['b1']);
            expect(invocations).toBe(3);

            // A second watcher with the SAME principal shares the owner's
            // qualified loop: replayed, not re-read.
            const bob2 = subscriber(
                harness.hosts[1]!.dispatchWatch!(ref, 'inbox', [], callAs('bob').call)
            );
            expect(await bob2.next()).toEqual(['b1']);
            expect(invocations).toBe(3);

            await alice.close();
            await bob1.close();
            await bob2.close();
        } finally {
            await harness.stop();
        }
    });

    it('qualified keys collide with nothing', () => {
        const base = watchKey('m', 0, ['x']);
        // Never a base key — neither this one nor one whose ARG spells out
        // the qualification suffix.
        expect(qualifyWatchKey(base, 'abc')).not.toBe(base);
        expect(qualifyWatchKey(base, 'abc')).not.toBe(watchKey('m', 0, ['xPs3:abc']));
        // Anonymous is distinct from a principal that LOOKS like the
        // anonymous marker.
        expect(qualifyWatchKey(base, undefined)).not.toBe(qualifyWatchKey(base, 'u'));
        // Equal iff base and encoded principal are both equal.
        expect(qualifyWatchKey(base, 'a')).not.toBe(qualifyWatchKey(base, 'b'));
        expect(qualifyWatchKey(watchKey('m', 0, ['y']), 'a')).not.toBe(qualifyWatchKey(base, 'a'));
        expect(qualifyWatchKey(base, 'a')).toBe(qualifyWatchKey(base, 'a'));
    });
});
