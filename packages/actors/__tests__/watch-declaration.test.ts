/**
 * `watches: { m: { principalIndependent: true } }` — the declaration that
 * restores cross-principal sharing for identity-blind reads (#138), and the
 * enforcement that makes it safe to trust.
 *
 * The relay drops the principal from its coalescing key for a declared
 * method, so distinct identities share one cross-host stream. That is only
 * sound because the OWNER polices the promise where the read runs: a
 * declared read observed consulting `ctx.principal` fails the watch, in
 * every build, whether or not anything coalesced. By the time it fires a
 * relay may already have merged identities onto one stream, and the owner —
 * which sees a single subscriber per stream — cannot repair that, so the
 * failure is closed and permanent rather than a quiet split.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor, type ActorCallContext, type AnyActorDefinition } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import { isActorError, isActorErrorKind } from '../src/errors';
import {
    declaresPrincipalIndependent,
    type WatchDeclarationOptions
} from '../src/watch-core';
import { stubServerApp } from '@sigx/server/testing';
import { preferLocalPolicy } from '@sigx/actors/cluster';
import { createCluster, quiet, type ClusterHarness } from './harness';

interface User {
    readonly id: string;
}

let host: Host | null = null;
let harness: ClusterHarness | null = null;
let restore: (() => void) | undefined;

afterEach(async () => {
    await host?.stop();
    host = null;
    await harness?.stop();
    harness = null;
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

function callAs(id: string): ActorCallContext {
    return {
        callChain: [],
        callId: `call-${id}-${Math.random().toString(36).slice(2)}`,
        principal: id,
        abortSignal: new AbortController().signal
    };
}

describe('watch declaration: principalIndependent (#138)', () => {
    describe('the promise is policed', () => {
        it('a declared read that consults ctx.principal fails the watch, branded', async () => {
            stub();
            const Liar = defineActor({
                type: 'Liar',
                allowAnonymous: true,
                watches: { feed: { principalIndependent: true } },
                state: () => ({ n: 1 }),
                methods: (ctx) => ({
                    async feed(): Promise<string> {
                        // The lie: identity IS an input to this read.
                        return `${(ctx.principal as User | null)?.id ?? 'anon'}:${ctx.state.n}`;
                    }
                })
            });
            host = createHost({ actors: [Liar], defaults: quiet });
            await host.start();

            const it = host
                .dispatchWatch!({ type: 'Liar', key: 'x' }, 'feed', [], callAs('alice'), {
                    throttleMs: 0
                })
                [Symbol.asyncIterator]();

            await expect(it.next()).rejects.toThrow(/principalIndependent/);
            await expect(it.next().catch((e: unknown) => e)).resolves.toBeDefined();
        });

        it('names the actor and method, and points at methodAuthorize', async () => {
            stub();
            const Liar = defineActor({
                type: 'Ledger',
                allowAnonymous: true,
                watches: { balance: { principalIndependent: true } },
                state: () => ({ n: 1 }),
                methods: (ctx) => ({
                    async balance(): Promise<number> {
                        void ctx.principal;
                        return ctx.state.n;
                    }
                })
            });
            host = createHost({ actors: [Liar], defaults: quiet });
            await host.start();

            const error = await host
                .dispatchWatch!({ type: 'Ledger', key: 'x' }, 'balance', [], callAs('alice'), {
                    throttleMs: 0
                })
                [Symbol.asyncIterator]()
                .next()
                .then(
                    () => null,
                    (e: unknown) => e
                );

            expect(isActorErrorKind(error, 'watch-declaration')).toBe(true);
            expect((error as Error).message).toContain('"Ledger.balance"');
            expect((error as Error).message).toContain('methodAuthorize');
        });

        it('fails even when the read body SWALLOWS the throw', async () => {
            // The case a getter-only implementation gets wrong: a broad
            // try/catch around the principal access would otherwise hand a
            // value to a population the relay already merged.
            stub();
            const Sneaky = defineActor({
                type: 'Sneaky',
                allowAnonymous: true,
                watches: { feed: { principalIndependent: true } },
                state: () => ({ n: 7 }),
                methods: (ctx) => ({
                    async feed(): Promise<number> {
                        try {
                            void ctx.principal;
                        } catch {
                            // deliberately swallowed
                        }
                        return ctx.state.n;
                    }
                })
            });
            host = createHost({ actors: [Sneaky], defaults: quiet });
            await host.start();

            const it = host
                .dispatchWatch!({ type: 'Sneaky', key: 'x' }, 'feed', [], callAs('alice'), {
                    throttleMs: 0
                })
                [Symbol.asyncIterator]();

            await expect(it.next()).rejects.toThrow(/principalIndependent/);
        });

        it('does not heal on re-subscribe, nor on a fresh activation', async () => {
            // The declaration is source code; #121's discovery is per
            // activation. So unlike a split, this cannot be escaped by
            // failing over — which is the point.
            stub();
            const Liar = defineActor({
                type: 'Persistent',
                allowAnonymous: true,
                watches: { feed: { principalIndependent: true } },
                state: () => ({ n: 1 }),
                methods: (ctx) => ({
                    async feed(): Promise<number> {
                        void ctx.principal;
                        return ctx.state.n;
                    }
                })
            });
            host = createHost({ actors: [Liar], defaults: quiet });
            await host.start();
            const ref = { type: 'Persistent', key: 'x' };

            const open = (): Promise<unknown> =>
                host!
                    .dispatchWatch!(ref, 'feed', [], callAs('alice'), { throttleMs: 0 })
                    [Symbol.asyncIterator]()
                    .next();

            await expect(open()).rejects.toThrow(/principalIndependent/);
            await expect(open()).rejects.toThrow(/principalIndependent/);

            await host.deactivate(ref, 'idle');
            await expect(open()).rejects.toThrow(/principalIndependent/);
        });

        it('a PLAIN call of the same method reads ctx.principal normally', async () => {
            // The marker rides watch invokes only. A declaration says
            // nothing about unary dispatch, and must not break it.
            stub();
            const Both = defineActor({
                type: 'Both',
                allowAnonymous: true,
                watches: { whoami: { principalIndependent: true } },
                state: () => ({ n: 1 }),
                methods: (ctx) => ({
                    async whoami(): Promise<string> {
                        return (ctx.principal as User | null)?.id ?? 'anon';
                    }
                })
            });
            host = createHost({ actors: [Both], defaults: quiet });
            await host.start();

            await expect(
                host.dispatch({ type: 'Both', key: 'x' }, 'whoami', [], callAs('alice'))
            ).resolves.toBe('alice');
        });

        it('an honest declared read is untouched by any of this', async () => {
            stub();
            let invocations = 0;
            const Honest = defineActor({
                type: 'Honest',
                allowAnonymous: true,
                watches: { feed: { principalIndependent: true } },
                state: () => ({ n: 1 }),
                methods: (ctx) => ({
                    async feed(): Promise<number> {
                        invocations++;
                        return ctx.state.n;
                    },
                    async bump(): Promise<void> {
                        ctx.state.n += 1;
                    }
                })
            });
            host = createHost({ actors: [Honest], defaults: quiet });
            await host.start();
            const ref = { type: 'Honest', key: 'x' };

            const a = host
                .dispatchWatch!(ref, 'feed', [], callAs('alice'), { throttleMs: 0 })
                [Symbol.asyncIterator]();
            const b = host
                .dispatchWatch!(ref, 'feed', [], callAs('bob'), { throttleMs: 0 })
                [Symbol.asyncIterator]();
            expect((await a.next()).value).toBe(1);
            expect((await b.next()).value).toBe(1);
            expect(invocations).toBe(1);

            await host.dispatch(ref, 'bump', [], callAs('carol'));
            expect((await a.next()).value).toBe(2);
            expect((await b.next()).value).toBe(2);
            expect(invocations).toBe(2);

            await a.return?.();
            await b.return?.();
        });
    });

    describe('the failure reaches a coalesced population', () => {
        it('every subscriber on a shared cross-host stream fails together', async () => {
            stub();
            const Liar = defineActor({
                type: 'ClusterLiar',
                allowAnonymous: true,
                watches: { feed: { principalIndependent: true } },
                state: () => ({ n: 1 }),
                methods: (ctx) => ({
                    async feed(): Promise<number> {
                        void ctx.principal;
                        return ctx.state.n;
                    },
                    async increment(): Promise<void> {
                        ctx.state.n += 1;
                        await ctx.save();
                    }
                })
            });
            // `preferLocalPolicy` PLUS activating from host 1 is what pins
            // the actor there — without a policy the default may claim it on
            // host 0, making the watches below local and the stream counts 0.
            harness = await createCluster(2, {
                actors: [Liar],
                policy: preferLocalPolicy()
            });
            const ref = { type: 'ClusterLiar', key: 'shared' } as const;
            // Own it on host 1 so the watches below cross the hop.
            await harness.hosts[1]!.actor(Liar, 'shared').increment();

            const alice = harness.hosts[0]!
                .dispatchWatch!(ref, 'feed', [], callAs('alice'))
                [Symbol.asyncIterator]();
            const bob = harness.hosts[0]!
                .dispatchWatch!(ref, 'feed', [], callAs('bob'))
                [Symbol.asyncIterator]();

            // BOTH pulls in flight before either settles. A coalesced watch
            // opens lazily on the first `next()`, and the owner's refusal
            // drops the entry — so awaiting them in sequence would have bob
            // open a SECOND stream after alice's failed, and prove nothing.
            const alicePull = alice.next();
            const bobPull = bob.next();
            await expect(alicePull).rejects.toThrow();
            await expect(bobPull).rejects.toThrow();

            // The assertion that makes this case able to fail: they really
            // were one stream, so what the owner refused is a MERGED
            // population — not two subscriptions that would each have
            // failed independently anyway.
            const counters = harness.placements[0]!.counters();
            expect(counters.remoteWatches).toBe(1);
            expect(counters.coalescedWatches).toBe(1);
        });
    });

    describe('validation, at the first activation', () => {
        const base = {
            allowAnonymous: true as const,
            state: () => ({ n: 0 }),
            methods: () => ({
                async get(): Promise<number> {
                    return 1;
                }
            })
        };

        async function firstCallError(def: AnyActorDefinition): Promise<string> {
            const h = createHost({ actors: [def], defaults: quiet });
            const client = h.actor(def, 'k') as unknown as { get(): Promise<number> };
            try {
                await client.get();
                return 'resolved';
            } catch (error) {
                expect(isActorError(error) && error.kind === 'activation').toBe(true);
                return String((error as Error).cause);
            } finally {
                await h.stop();
            }
        }

        it('a value other than { principalIndependent: true } fails', async () => {
            const bad = defineActor({
                ...base,
                type: 'BadWatch1',
                watches: { get: true as unknown as { principalIndependent: true } }
            });
            await expect(firstCallError(bad)).resolves.toMatch(/principalIndependent: true/);
        });

        it('a non-object `watches` fails', async () => {
            const bad = defineActor({
                ...base,
                type: 'BadWatch2',
                watches: [] as unknown as Record<string, { principalIndependent: true }>
            });
            await expect(firstCallError(bad)).resolves.toMatch(/`watches` must be an object/);
        });

        it('a `streams:` name in the map fails', async () => {
            const bad = defineActor({
                ...base,
                type: 'BadWatch3',
                watches: {
                    tick: { principalIndependent: true }
                } as unknown as Record<string, { principalIndependent: true }>,
                streams: () => ({
                    async *tick(): AsyncGenerator<number> {
                        yield 1;
                    }
                })
            });
            await expect(firstCallError(bad)).resolves.toMatch(/never shared/);
        });

        it('a reserved $-name in the map fails', async () => {
            const bad = defineActor({
                ...base,
                type: 'BadWatch4',
                watches: {
                    '$sigx:reminder': { principalIndependent: true }
                } as unknown as Record<string, { principalIndependent: true }>
            });
            await expect(firstCallError(bad)).resolves.toMatch(/reserved name/);
        });

        it('a MALFORMED watches map reads as "not declared" on the relay', async () => {
            // The relay reads the declaration before anything validates it —
            // it may never activate the actor at all. `Object.hasOwn(null, m)`
            // throws, so an unguarded read would crash coalescing on a host
            // that is only passing the subscription through, instead of
            // leaving the owner to fail loudly.
            for (const malformed of [null, [], 'nope', 42]) {
                expect(
                    declaresPrincipalIndependent(
                        { watches: malformed } as unknown as WatchDeclarationOptions,
                        'feed'
                    )
                ).toBe(false);
            }
            // And a well-formed map still answers.
            expect(
                declaresPrincipalIndependent(
                    { watches: { feed: { principalIndependent: true } } },
                    'feed'
                )
            ).toBe(true);
            // Inherited keys never count.
            const inherited = Object.create({ feed: { principalIndependent: true } }) as Record<
                string,
                { principalIndependent: true }
            >;
            expect(declaresPrincipalIndependent({ watches: inherited }, 'feed')).toBe(false);
        });

        it('a name matching no method warns rather than throwing', async () => {
            // Definition time cannot check it (the methods factory needs a
            // live ctx), and a typo'd key is inert rather than dangerous.
            const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
            try {
                const odd = defineActor({
                    ...base,
                    type: 'OddWatch',
                    watches: {
                        nope: { principalIndependent: true }
                    } as unknown as Record<string, { principalIndependent: true }>
                });
                host = createHost({ actors: [odd], defaults: quiet });
                await host.start();
                const client = host.actor(odd, 'k') as unknown as { get(): Promise<number> };
                await expect(client.get()).resolves.toBe(1);
                expect(warn).toHaveBeenCalledWith(expect.stringContaining('names no method'));
            } finally {
                warn.mockRestore();
            }
        });
    });
});
