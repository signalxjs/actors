/**
 * The isomorphic `actor()` entry: silo seam resolution, guard chains on the
 * in-process transport, and per-call options.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { ServerFnError, type ServerFnContext, type ServerFnInfo } from '@sigx/server';
import { actor, defineActor, useActor } from '@sigx/actors';
import { createSilo, type Silo } from '@sigx/actors/silo';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

let running: Silo | null = null;
afterEach(async () => {
    await running?.stop({ timeoutMs: 1000 });
    running = null;
});

async function startSilo(actors: Parameters<typeof createSilo>[0]['actors']): Promise<Silo> {
    const silo = createSilo({ actors, defaults: quiet });
    await silo.start();
    running = silo;
    return silo;
}

describe('actor() entry', () => {
    it('throws a descriptive boot-order error when no silo is running', async () => {
        const def = defineActor({
            type: 'NoSilo',
            unguarded: true,
            state: () => ({}),
            methods: () => ({
                async ping() {
                    return 'pong';
                }
            })
        });
        await expect(async () => actor(def, 'k').ping()).rejects.toThrow(/no silo is running/);
    });

    it('dispatches in-process through the seam once a silo is started', async () => {
        const def = defineActor({
            type: 'Seamed',
            unguarded: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                async bump() {
                    ctx.state.n++;
                    return ctx.state.n;
                }
            })
        });
        await startSilo([def]);
        await expect(actor(def, 'k').bump()).resolves.toBe(1);
        await expect(useActor(def, 'k').bump()).resolves.toBe(2);
    });

    it('runs actor-level then method-level guards, with the actor#method info', async () => {
        const seen: string[] = [];
        const def = defineActor({
            type: 'Guarded',
            use: [
                (_rq: ServerFnContext, info: ServerFnInfo) => {
                    seen.push(`actor:${info.symbol}:${info.name}`);
                }
            ],
            methodUse: {
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
        await startSilo([def]);
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

    it('guards see the explicit context from .with({ context })', async () => {
        const request = new Request('https://example.test/page');
        let sawUrl: string | null = null;
        const def = defineActor({
            type: 'CtxSee',
            use: [
                (rq: ServerFnContext) => {
                    sawUrl = rq.request.url;
                }
            ],
            state: () => ({}),
            methods: () => ({
                async ping() {
                    return 'pong';
                }
            })
        });
        await startSilo([def]);
        await expect(actor(def, 'c').with({ context: request }).ping()).resolves.toBe('pong');
        expect(sawUrl).toBe('https://example.test/page');
    });

    it('a guard reading rq.request with no context gets the descriptive detached throw', async () => {
        const def = defineActor({
            type: 'Detached',
            use: [
                (rq: ServerFnContext) => {
                    void rq.request; // no ambient scope in this test → throws
                }
            ],
            state: () => ({}),
            methods: () => ({
                async ping() {
                    return 'pong';
                }
            })
        });
        await startSilo([def]);
        await expect(actor(def, 'd').ping()).rejects.toThrow(/not available on an in-process/);
    });

    it('guards run before the stream opens (wire parity for streams)', async () => {
        const def = defineActor({
            type: 'GStream',
            use: [
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
        await startSilo([def]);
        const iterate = async () => {
            for await (const chunk of actor(def, 's').feed()) void chunk;
        };
        await expect(iterate()).rejects.toMatchObject({ status: 401 });
    });

    it('defineActor rejects the unguarded+use contradiction at definition time', () => {
        expect(() =>
            defineActor({
                type: 'Contradiction',
                unguarded: true,
                use: [() => {}],
                state: () => ({}),
                methods: () => ({})
            })
        ).toThrow(/unguarded: true.*use/s);
    });
});
