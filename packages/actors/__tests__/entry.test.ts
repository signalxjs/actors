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
            unguarded: true,
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
            unguarded: true,
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
        await startHost([def]);
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
        await startHost([def]);
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
        await startHost([def]);
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
