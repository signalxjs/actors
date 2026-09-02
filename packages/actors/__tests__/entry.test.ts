/**
 * The isomorphic `actor()` entry: host seam resolution, guard chains on the
 * in-process transport, and per-call options.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { ServerFnError, type ServerFnContext, type ServerFnInfo } from '@sigx/server';
import { actor, defineActor, useActor } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

let running: Host | null = null;
afterEach(async () => {
    await running?.stop({ timeoutMs: 1000 });
    running = null;
});

async function startHost(actors: Parameters<typeof createHost>[0]['actors']): Promise<Host> {
    const host = createHost({ actors, defaults: quiet });
    await host.start();
    running = host;
    return host;
}

describe('actor() entry', () => {
    it('throws a descriptive boot-order error when no host is running', async () => {
        const def = defineActor({
            type: 'NoHost',
            allowAnonymous: true,
            state: () => ({}),
            methods: () => ({
                async ping() {
                    return 'pong';
                }
            })
        });
        await expect(async () => actor(def, 'k').ping()).rejects.toThrow(/no host is running/);
    });

    it('dispatches in-process through the seam once a host is started', async () => {
        const def = defineActor({
            type: 'Seamed',
            allowAnonymous: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                async bump() {
                    ctx.state.n++;
                    return ctx.state.n;
                }
            })
        });
        await startHost([def]);
        await expect(actor(def, 'k').bump()).resolves.toBe(1);
        await expect(useActor(def, 'k').bump()).resolves.toBe(2);
    });

    it('runs actor-level then method-level guards, with the actor#method info', async () => {
        const seen: string[] = [];
        const def = defineActor({
            type: 'Guarded',
            authorize: [
                (_p, _rq, op) => {
                    seen.push(`actor:${op.fn.symbol}:${op.fn.name}`);
                    return true;
                }
            ],
            methodAuthorize: {
                secret: [
                    () => {
                        seen.push('method:secret');
                        throw new ServerFnError(403, 'forbidden');
                    }
                ]
            },
            state: () => ({}),
            methods: () => ({
                async open() {
                    return 'ok';
                },
                async secret() {
                    return 'leaked';
                }
            })
        });
        await startHost([def]);
        await expect(actor(def, 'g').open()).resolves.toBe('ok');
        await expect(actor(def, 'g').secret()).rejects.toMatchObject({
            __sigxServerFnError: true,
            status: 403
        });
        expect(seen).toEqual([
            'actor:Guarded#open:open',
            'actor:Guarded#secret:secret',
            'method:secret'
        ]);
    });

    it('a one-way call still runs guards first — a veto is pre-acceptance and rejects', async () => {
        const events: string[] = [];
        const def = defineActor({
            type: 'OneWayGuarded',
            authorize: [
                (_p, _rq, op) => {
                    events.push(`guard:${op.fn.name}`);
                    return true;
                }
            ],
            methodAuthorize: {
                sealed: [
                    () => {
                        throw new ServerFnError(403, 'forbidden');
                    }
                ]
            },
            state: () => ({}),
            methods: () => ({
                async open() {
                    events.push('open:ran');
                    return 'ok';
                },
                async sealed() {
                    events.push('sealed:ran');
                }
            })
        });
        await startHost([def]);
        // Success resolves early with undefined, guards having run.
        await expect(actor(def, 'g').with({ oneWay: true }).open()).resolves.toBeUndefined();
        // A guard veto REJECTS the one-way caller — the call was never accepted.
        await expect(actor(def, 'g').with({ oneWay: true }).sealed()).rejects.toMatchObject({
            __sigxServerFnError: true,
            status: 403
        });
        await actor(def, 'g').open(); // settle the queue
        expect(events).toContain('guard:open');
        expect(events).toContain('guard:sealed');
        expect(events).not.toContain('sealed:ran');
    });

    it('guards see the explicit context from .with({ context })', async () => {
        const request = new Request('https://example.test/page');
        let sawUrl: string | null = null;
        const def = defineActor({
            type: 'CtxSee',
            authorize: [
                (_p, rq) => {
                    sawUrl = rq.request.url;
                    return true;
                }
            ],
            state: () => ({}),
            methods: () => ({
                async ping() {
                    return 'pong';
                }
            })
        });
        await startHost([def]);
        await expect(actor(def, 'c').with({ context: request }).ping()).resolves.toBe('pong');
        expect(sawUrl).toBe('https://example.test/page');
    });

    it('the ambient seam returning { request, locals } shares the locals store with guards', async () => {
        // Core 0.14 (signalxjs/core#494): a request scope resolves to a
        // Partial<ServerFnContext> whose `locals` IS the per-request store.
        // Pin that an actor guard both reads it and that its writes land
        // back in the same store.
        const request = new Request('https://example.test/scoped');
        const locals: Record<string, unknown> = { user: 'ada' };
        let sawUser: unknown = null;
        const g = globalThis as { __SIGX_SERVERFN_CONTEXT__?: () => unknown };
        g.__SIGX_SERVERFN_CONTEXT__ = () => ({ request, locals });
        try {
            const def = defineActor({
                type: 'AmbientLocals',
                authorize: [
                    (_p, rq) => {
                        sawUser = rq.locals.user;
                        rq.locals.stamped = true;
                        return true;
                    }
                ],
                state: () => ({}),
                methods: () => ({
                    async ping() {
                        return 'pong';
                    }
                })
            });
            await startHost([def]);
            await expect(actor(def, 'a').ping()).resolves.toBe('pong');
            expect(sawUser).toBe('ada');
            expect(locals.stamped).toBe(true);
        } finally {
            delete g.__SIGX_SERVERFN_CONTEXT__;
        }
    });

    it('a guard reading rq.request with no context gets the descriptive detached throw', async () => {
        const def = defineActor({
            type: 'Detached',
            authorize: [
                (_p, rq) => {
                    void rq.request; // no ambient scope in this test → throws
                    return true;
                }
            ],
            state: () => ({}),
            methods: () => ({
                async ping() {
                    return 'pong';
                }
            })
        });
        await startHost([def]);
        await expect(actor(def, 'd').ping()).rejects.toThrow(/not available on an in-process/);
    });

    it('guards run before the stream opens (wire parity for streams)', async () => {
        const def = defineActor({
            type: 'GStream',
            authorize: [
                () => {
                    throw new ServerFnError(401, 'nope');
                }
            ],
            state: () => ({}),
            methods: () => ({}),
            streams: () => ({
                async *feed() {
                    yield 1;
                }
            })
        });
        await startHost([def]);
        const iterate = async () => {
            for await (const chunk of actor(def, 's').feed()) void chunk;
        };
        await expect(iterate()).rejects.toMatchObject({ status: 401 });
    });

    it('allows allowAnonymous alongside authorize — no contradiction since v4', () => {
        expect(() =>
            defineActor({
                type: 'Contradiction',
                allowAnonymous: true,
                authorize: [() => true],
                state: () => ({}),
                methods: () => ({})
            })
        ).not.toThrow();
    });
});

// ---------------------------------------------------------------------------
// The context bag on the in-process transport (#246): a guard's stamp is
// lifted from the request's locals after runGuards, `.with({ bag })` merges
// over it (explicit wins), and the detached case degrades to an empty bag.

import { stampCallBag } from '@sigx/actors';

describe('actor() entry: context bag', () => {
    const bagged = defineActor({
        type: 'EntryBagged',
        authorize: [
            (_p, rq) => {
                stampCallBag(rq, { user: 'ada', from: 'guard' });
                return true;
            }
        ],
        state: () => ({}),
        methods: (ctx) => ({
            async bagOf() {
                return { ...ctx.bag };
            }
        })
    });

    it('a guard stamp reaches ctx.bag in-process', async () => {
        await startHost([bagged]);
        await expect(actor(bagged, 'b1').bagOf()).resolves.toEqual({
            user: 'ada',
            from: 'guard'
        });
    });

    it('.with({ bag }) merges over the guard stamp, explicit wins', async () => {
        await startHost([bagged]);
        await expect(
            actor(bagged, 'b2').with({ bag: { user: 'grace', extra: 'y' } }).bagOf()
        ).resolves.toEqual({ user: 'grace', from: 'guard', extra: 'y' });
    });

    it('an invalid .with({ bag }) throws at the caller, pre-dispatch', async () => {
        await startHost([bagged]);
        await expect(
            actor(bagged, 'b3').with({ bag: { '': 'x' } }).bagOf()
        ).rejects.toThrow(/\[sigx actors\]/);
    });

    it('an anonymous detached call degrades to an empty bag, never a throw', async () => {
        const plain = defineActor({
            type: 'EntryPlainBag',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({
                async bagOf() {
                    return { ...ctx.bag };
                }
            })
        });
        await startHost([plain]);
        await expect(actor(plain, 'b4').bagOf()).resolves.toEqual({});
    });

    it('the ambient request scope carries a serverFn guard stamp into the actor call', async () => {
        // The chat.server.ts shape: a serverFn's guard stamps the bag on the
        // shared locals store; the in-process actor() call inside that scope
        // lifts it.
        const request = new Request('https://example.test/scoped');
        const locals: Record<string, unknown> = {};
        stampCallBag({ locals }, { user: 'ada' });
        const g = globalThis as { __SIGX_SERVERFN_CONTEXT__?: () => unknown };
        g.__SIGX_SERVERFN_CONTEXT__ = () => ({ request, locals });
        try {
            const plain = defineActor({
                type: 'EntryAmbientBag',
                allowAnonymous: true,
                state: () => ({}),
                methods: (ctx) => ({
                    async bagOf() {
                        return { ...ctx.bag };
                    }
                })
            });
            await startHost([plain]);
            await expect(actor(plain, 'b5').bagOf()).resolves.toEqual({ user: 'ada' });
        } finally {
            delete g.__SIGX_SERVERFN_CONTEXT__;
        }
    });
});
