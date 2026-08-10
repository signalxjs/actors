/**
 * The per-principal watch split (#121), made visible from ops (#180).
 *
 * A popular read costs ONE loop however many subscribers share it; a read
 * that consults `ctx.principal` splits per distinct identity. The collapse
 * measured in #180 is that split growing with the signed-in audience — and
 * before these gauges it was observable only from a collapsed deployment.
 * `watchLoops` against `watchSubscribers` is the signature: many
 * subscribers per loop is healthy sharing, a ratio near 1 on a hot actor
 * is the cliff building.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, type ActorCallContext } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import { stubServerApp } from '@sigx/server/testing';
import { emptyHostStats } from '../src/types';

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

const Feed = defineActor({
    type: 'Feed',
    allowAnonymous: true,
    state: () => ({ posts: ['hello'] }),
    methods: (ctx) => ({
        /** Identity-blind: every subscriber shares one loop. */
        async all(): Promise<string[]> {
            return [...ctx.state.posts];
        },
        /** Consults the principal: one loop per distinct identity (#121). */
        async mine(): Promise<string> {
            const me = (ctx.principal as User | null)?.id ?? 'nobody';
            return `${me}:${ctx.state.posts.length}`;
        }
    })
});

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

function stub(): void {
    restore = stubServerApp({
        authenticate: () => ({ id: 'alice' }) satisfies User,
        codec: {
            encode: (principal) => (principal as User).id,
            decode: (encoded) => (encoded === '' ? null : ({ id: encoded } satisfies User))
        }
    });
}

/** Open a watch, await its first value, keep it open. */
async function watch(
    h: Host,
    method: string,
    call: ActorCallContext
): Promise<() => Promise<void>> {
    const iterator = h
        .dispatchWatch!({ type: 'Feed', key: 'f' }, method, [], call, { throttleMs: 0 })
        [Symbol.asyncIterator]();
    await iterator.next();
    return async () => void (await iterator.return?.(undefined));
}

describe('watch-loop observability', () => {
    it('an identity-blind read reports one loop however many identities subscribe', async () => {
        stub();
        host = createHost({ actors: [Feed], defaults: quiet });
        await host.start();

        const closes = [];
        for (const id of ['alice', 'bob', 'carol']) {
            closes.push(await watch(host, 'all', callAs(id).call));
        }

        expect(host.stats().watchLoops).toBe(1);
        const info = host.activations().find((a) => a.type === 'Feed')!;
        expect(info.watchLoops).toBe(1);
        expect(info.watchSubscribers).toBe(3);

        for (const close of closes) await close();
    });

    it('a principal-consulting read reports one loop per distinct identity', async () => {
        stub();
        host = createHost({ actors: [Feed], defaults: quiet });
        await host.start();

        // Sequential and awaited, so discovery settles on the first seed
        // and every later identity resolves straight to a qualified key.
        const closes = [];
        for (const id of ['alice', 'bob', 'carol', 'dave']) {
            closes.push(await watch(host, 'mine', callAs(id).call));
        }

        // 4 identities → 4 loops (1 draining base entry + 3 qualified),
        // 4 subscribers: the ratio-of-one signature of #180.
        expect(host.stats().watchLoops).toBe(4);
        const info = host.activations().find((a) => a.type === 'Feed')!;
        expect(info.watchLoops).toBe(4);
        expect(info.watchSubscribers).toBe(4);

        for (const close of closes) await close();
    });

    it('drops to zero when the last subscriber leaves', async () => {
        stub();
        host = createHost({ actors: [Feed], defaults: quiet });
        await host.start();

        const closeAll = await watch(host, 'all', callAs('alice').call);
        const closeMine = await watch(host, 'mine', callAs('alice').call);
        expect(host.stats().watchLoops).toBe(2);

        await closeAll();
        await closeMine();

        expect(host.stats().watchLoops).toBe(0);
        const info = host.activations().find((a) => a.type === 'Feed')!;
        expect(info.watchLoops).toBe(0);
        expect(info.watchSubscribers).toBe(0);
    });

    it('a stopped host reports the gauge as zero, not absent', () => {
        expect(emptyHostStats().watchLoops).toBe(0);
    });
});
