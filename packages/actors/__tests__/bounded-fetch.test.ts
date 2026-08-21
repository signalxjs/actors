/**
 * @vitest-environment node
 *
 * `boundedFetch` (#118) — the bounded-pool fetch helper on `@sigx/actors/node`.
 *
 * The +6%/half-the-sockets recipe was prose; this is the code. Two things
 * matter and both are asserted here: the returned fetch threads ONE
 * `Agent({ connections })` as the `dispatcher` of every forwarded call (the
 * whole point — without it undici opens two sockets per in-flight request),
 * and a host without the optional `undici` peer fails with guidance rather
 * than a bare MODULE_NOT_FOUND (Node bundles undici for its global fetch,
 * but does not expose the Dispatcher API — the package must be installed).
 *
 * The threading assertions inject a fake undici through the internal
 * factory; the real package (a devDependency here, an optional peer for
 * consumers) is exercised once, over REAL sockets, to prove the cap is a
 * cap — that end-to-end case needs the node environment: happy-dom's fetch
 * enforces the browser same-origin policy and blocks every request to a
 * test server.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createServer, type Server } from 'node:http';
import type { Socket } from 'node:net';
import { boundedFetch } from '@sigx/actors/node';
import { createBoundedFetch, type UndiciLike } from '../src/node/bounded-fetch';

function fakeUndici() {
    const agents: Array<{ connections: number }> = [];
    class Agent {
        constructor(options: { connections: number }) {
            agents.push(options);
        }
    }
    const calls: Array<{ input: unknown; init: Record<string, unknown> | undefined }> = [];
    const undici: UndiciLike = {
        Agent: Agent as UndiciLike['Agent'],
        fetch: (input, init) => {
            calls.push({ input, init: init as Record<string, unknown> | undefined });
            return Promise.resolve(new Response('ok'));
        }
    };
    return { undici, agents, calls };
}

describe('boundedFetch', () => {
    it('threads one Agent({ connections }) as the dispatcher of every call', async () => {
        const { undici, agents, calls } = fakeUndici();
        const load = vi.fn(() => Promise.resolve(undici));
        const fetch = createBoundedFetch({ connections: 7 }, load);

        // Lazy: nothing loads until the first call.
        expect(load).not.toHaveBeenCalled();
        expect(agents).toHaveLength(0);

        const res = await fetch('http://peer.internal/x', { method: 'POST', body: 'b' });
        expect(await res.text()).toBe('ok');
        await fetch('http://peer.internal/y');

        // One agent, built with the caller's cap, shared by every call.
        expect(load).toHaveBeenCalledTimes(1);
        expect(agents).toEqual([{ connections: 7 }]);
        expect(calls).toHaveLength(2);
        for (const call of calls) {
            expect(call.init?.dispatcher).toBeInstanceOf(undici.Agent);
        }
        expect(calls[0]!.init?.dispatcher).toBe(calls[1]!.init?.dispatcher);

        // The caller's init survives the forwarding.
        expect(calls[0]!.input).toBe('http://peer.internal/x');
        expect(calls[0]!.init?.method).toBe('POST');
        expect(calls[0]!.init?.body).toBe('b');
    });

    it('loads undici once even under concurrent first calls', async () => {
        const { undici, agents } = fakeUndici();
        let release!: (mod: UndiciLike) => void;
        const gate = new Promise<UndiciLike>((resolve) => {
            release = resolve;
        });
        const load = vi.fn(() => gate);
        const fetch = createBoundedFetch({ connections: 3 }, load);

        const first = fetch('http://peer.internal/a');
        const second = fetch('http://peer.internal/b');
        release(undici);
        await Promise.all([first, second]);

        expect(load).toHaveBeenCalledTimes(1);
        expect(agents).toEqual([{ connections: 3 }]);
    });

    it('rejects a non-positive or fractional connections cap eagerly', () => {
        for (const connections of [0, -1, 1.5, Number.NaN]) {
            expect(() => boundedFetch({ connections })).toThrow(/connections/);
        }
    });

    it('fails with install guidance when undici is absent', async () => {
        // What `import('undici')` rejects with on a host without the package.
        const missing = Object.assign(new Error("Cannot find package 'undici'"), {
            code: 'ERR_MODULE_NOT_FOUND'
        });
        const fetch = createBoundedFetch({ connections: 64 }, () => Promise.reject(missing));
        await expect(fetch('http://peer.internal/x')).rejects.toThrow(
            /optional peer dependency "undici"/
        );
        // Not a one-shot: the guidance survives a second call.
        await expect(fetch('http://peer.internal/x')).rejects.toThrow(/pnpm add undici/);
    });

    it('does not cache a failure from the Agent constructor either', async () => {
        // The rejection can come from AFTER the load — undici resolving but
        // its Agent throwing — and must not pin `ready` to a permanently
        // rejected promise: the next call retries from the loader.
        let attempts = 0;
        class FlakyAgent {
            constructor() {
                attempts += 1;
                if (attempts === 1) throw new Error('agent init failed');
            }
        }
        const undici: UndiciLike = {
            Agent: FlakyAgent as unknown as UndiciLike['Agent'],
            fetch: () => Promise.resolve(new Response('ok'))
        };
        const load = vi.fn(() => Promise.resolve(undici));
        const fetch = createBoundedFetch({ connections: 2 }, load);

        await expect(fetch('http://peer.internal/x')).rejects.toThrow('agent init failed');
        const res = await fetch('http://peer.internal/x');
        expect(await res.text()).toBe('ok');
        expect(load).toHaveBeenCalledTimes(2);
        expect(attempts).toBe(2);
    });

    it('lets a non-resolution load failure through untranslated', async () => {
        const broken = Object.assign(new Error('undici blew up at init'), { code: 'EWHATEVER' });
        const fetch = createBoundedFetch({ connections: 1 }, () => Promise.reject(broken));
        await expect(fetch('http://peer.internal/x')).rejects.toBe(broken);
    });

    describe('against real undici and real sockets', () => {
        let server: Server | undefined;
        afterEach(async () => {
            if (server) {
                // The agent keeps its pooled sockets alive after the
                // responses land, and `server.close()` waits for them —
                // drop them first so teardown never rides a keep-alive
                // timeout.
                server.closeAllConnections();
                await new Promise((resolve) => server!.close(resolve));
            }
            server = undefined;
        });

        it('caps the pool at `connections` under real concurrency', async () => {
            const sockets = new Set<Socket>();
            server = createServer((_req, res) => {
                // Hold each response briefly so the 16 requests actually
                // overlap — an unbounded pool would open a socket per
                // in-flight request here.
                setTimeout(() => res.end('hi'), 20);
            });
            server.on('connection', (socket) => sockets.add(socket));
            await new Promise<void>((resolve) => server!.listen(0, resolve));
            const address = server.address() as { port: number };
            const url = `http://127.0.0.1:${address.port}/`;

            const fetch = boundedFetch({ connections: 2 });
            const bodies = await Promise.all(
                Array.from({ length: 16 }, () => fetch(url).then((res) => res.text()))
            );

            expect(bodies).toEqual(Array.from({ length: 16 }, () => 'hi'));
            expect(sockets.size).toBeGreaterThan(0);
            expect(sockets.size).toBeLessThanOrEqual(2);
        });
    });
});
