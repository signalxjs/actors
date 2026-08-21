/**
 * #221 — the authoring hint for identity-blind, undeclared watch reads.
 *
 * An identity-blind read that is not declared `principalIndependent` costs
 * exactly what a genuinely identity-dependent read costs (one cross-host
 * stream per identity), and nothing tells the author until the wall. The
 * runtime already observes whether a shared watch read consulted
 * `ctx.principal` (#121) and whether the method was declared (#138), so
 * after N completed reads that never consulted identity it emits ONE
 * `__DEV__` warning per (type, method) with the exact declaration to add.
 *
 * Never for a declared method, never once any read consults the principal,
 * and the tally lives with the HOST — a re-activation must not reset it.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor, type ActorCallContext } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import { stubServerApp } from '@sigx/server/testing';
import { WATCH_DECLARATION_HINT_READS } from '../src/host/activation';
import { quiet } from './harness';

const N = WATCH_DECLARATION_HINT_READS;

interface User {
    readonly id: string;
}

let host: Host | null = null;
let restore: (() => void) | undefined;
let warnSpy: ReturnType<typeof vi.spyOn> | undefined;
const aborts: AbortController[] = [];

afterEach(async () => {
    await host?.stop();
    host = null;
    restore?.();
    restore = undefined;
    for (const a of aborts.splice(0)) a.abort();
    warnSpy?.mockRestore();
    warnSpy = undefined;
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

const Board = defineActor({
    type: 'Board',
    allowAnonymous: true,
    watches: { shared: { principalIndependent: true } },
    state: () => ({ n: 0 }),
    methods: (ctx) => ({
        /** Identity-blind and UNDECLARED — the case the hint exists for. */
        async current(): Promise<number> {
            return ctx.state.n;
        },
        /** The same read, declared — must never hint. */
        async shared(): Promise<number> {
            return ctx.state.n;
        },
        /** Genuinely identity-dependent — must never hint. */
        async mine(): Promise<string> {
            return `${(ctx.principal as User | null)?.id ?? 'anon'}:${ctx.state.n}`;
        },
        /** Conditionally identity-dependent: blind until `n` reaches 3. */
        async sometimes(): Promise<number> {
            if (ctx.state.n >= 3) void ctx.principal;
            return ctx.state.n;
        },
        async bump(): Promise<number> {
            return ++ctx.state.n;
        },
        async bye(): Promise<string> {
            ctx.deactivate();
            return 'bye';
        }
    })
});

function callAs(id: string): ActorCallContext {
    const abort = new AbortController();
    aborts.push(abort);
    return {
        callChain: [],
        callId: `call-${id}-${Math.random().toString(36).slice(2)}`,
        principal: id,
        abortSignal: abort.signal
    };
}

/** Open a zero-throttle watch and hand back its iterator. */
function open(method: string, key = 'k'): AsyncIterator<unknown> {
    return host!
        .dispatchWatch!({ type: 'Board', key }, method, [], callAs('alice'), { throttleMs: 0 })
        [Symbol.asyncIterator]();
}

/** The hint lines the spy has seen — other dev warnings are not counted. */
function hints(): string[] {
    return (warnSpy?.mock.calls ?? [])
        .map((c: unknown[]) => String(c[0]))
        .filter((m: string) => m.includes('without consulting ctx.principal'));
}

async function setup(): Promise<ReturnType<Host['actor']>> {
    stub();
    warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    host = createHost({ actors: [Board], defaults: quiet });
    await host.start();
    return host.actor(Board, 'k');
}

/** Consume `reads - 1` change-driven re-reads after the seed already read. */
async function drive(
    client: { bump(): Promise<number> },
    it: AsyncIterator<unknown>,
    reads: number
): Promise<void> {
    for (let i = 0; i < reads; i++) {
        await client.bump();
        await it.next();
    }
}

describe('watch declaration hint (#221)', () => {
    it(`warns once per (type, method) after ${N} identity-blind reads of an undeclared method`, async () => {
        const client = await setup();
        const it = open('current');
        await it.next(); // the seed read — completed read 1
        await drive(client, it, N - 2); // reads 2..N-1
        expect(hints()).toHaveLength(0);

        await drive(client, it, 1); // read N — the threshold
        expect(hints()).toHaveLength(1);
        expect(hints()[0]).toContain('"Board.current"');
        expect(hints()[0]).toContain('watches: { current: { principalIndependent: true } }');

        await drive(client, it, 5); // well past the threshold — still once
        expect(hints()).toHaveLength(1);
        await it.return?.(undefined);
    });

    it('never warns for a method declared principalIndependent', async () => {
        const client = await setup();
        const it = open('shared');
        await it.next();
        await drive(client, it, N + 4);
        expect(hints()).toHaveLength(0);
        await it.return?.(undefined);
    });

    it('never warns for a read that consults ctx.principal', async () => {
        const client = await setup();
        const it = open('mine');
        await it.next();
        await drive(client, it, N + 4);
        expect(hints()).toHaveLength(0);
        await it.return?.(undefined);
    });

    it('one consultation suppresses the hint for good — conditional reads stay quiet', async () => {
        const client = await setup();
        const it = open('sometimes');
        await it.next(); // blind reads tally while n < 3…
        await drive(client, it, N + 4); // …then one read consults, and the tally is dead
        expect(hints()).toHaveLength(0);
        await it.return?.(undefined);
    });

    it('the tally survives re-activation — it lives with the host, not the activation', async () => {
        const client = await setup();
        const first = open('current');
        await first.next();
        await drive(client, first, 9); // 10 completed reads, then let the actor go
        await first.return?.(undefined);
        await client.bye();
        await vi.waitFor(() => expect(host!.stats().activations).toBe(0));

        const second = open('current');
        await second.next(); // read 11 — a fresh activation, same tally
        await drive(client, second, N - 12);
        expect(hints()).toHaveLength(0); // read N-1: not yet
        await drive(client, second, 1); // read N
        expect(hints()).toHaveLength(1);
        await second.return?.(undefined);
    });
});
