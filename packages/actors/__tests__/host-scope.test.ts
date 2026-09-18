/**
 * `runWithHost` — the per-request host scope behind the ambient seam (#456).
 *
 * Several hosts in one process is the normal state of a Durable Object
 * isolate: one host per object. The global seam is last-wins, so without a
 * scope an ambient `actor()` inside one object resolves through whichever
 * object booted LAST. These tests pin the scope on plain in-memory hosts —
 * the placement-level consequence is pinned by actors-cloudflare's suites.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { actor, currentHost, defineActor, peekHost, type Host } from '@sigx/actors';
import { createHost, memoryStorage, runWithHost } from '@sigx/actors/host';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const Counter = defineActor({
    type: 'Counter',
    allowAnonymous: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        }
    })
});

const started: Host[] = [];

async function startHost(): Promise<{ host: Host; storage: ReturnType<typeof memoryStorage> }> {
    const storage = memoryStorage();
    const host = createHost({ actors: [Counter], storage, defaults: quiet });
    await host.start();
    started.push(host);
    return { host, storage };
}

afterEach(async () => {
    while (started.length) await started.pop()!.stop();
    vi.restoreAllMocks();
});

describe('runWithHost', () => {
    it('resolves the scoped host, not the last one started', async () => {
        const first = await startHost();
        const second = await startHost();
        // Outside any scope the seam is still last-wins.
        expect(currentHost()).toBe(second.host);

        await runWithHost(first.host, async () => {
            expect(currentHost()).toBe(first.host);
            expect(peekHost()).toBe(first.host);
        });
        // And back to the global once the scope exits.
        expect(currentHost()).toBe(second.host);
    });

    it('holds across awaits and timers started inside the scope', async () => {
        const first = await startHost();
        const second = await startHost();

        const seen = await runWithHost(first.host, async () => {
            await Promise.resolve();
            const later = await new Promise<Host>((resolve) =>
                setTimeout(() => resolve(currentHost()), 0)
            );
            return [currentHost(), later];
        });
        expect(seen).toEqual([first.host, first.host]);
        expect(currentHost()).toBe(second.host);
    });

    it('routes an ambient actor() call through the scoped host', async () => {
        const first = await startHost();
        const second = await startHost();

        await runWithHost(first.host, () => actor(Counter, 'a').increment(1));

        // The activation — and therefore the save — happened on the scoped
        // host's storage, not on the last-started host's.
        expect(await first.storage.load('Counter', 'a')).not.toBeNull();
        expect(await second.storage.load('Counter', 'a')).toBeNull();
    });

    it('keeps concurrent scopes apart', async () => {
        const first = await startHost();
        const second = await startHost();

        const tick = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 1));
        const [a, b] = await Promise.all([
            runWithHost(first.host, async () => {
                await tick();
                return currentHost();
            }),
            runWithHost(second.host, async () => {
                await tick();
                return currentHost();
            })
        ]);
        expect(a).toBe(first.host);
        expect(b).toBe(second.host);
    });

    it('lets the innermost scope win', async () => {
        const first = await startHost();
        const second = await startHost();

        await runWithHost(first.host, async () => {
            await runWithHost(second.host, async () => {
                expect(currentHost()).toBe(second.host);
            });
            expect(currentHost()).toBe(first.host);
        });
    });

    it('falls back to the stamped host while a thunk answers undefined', async () => {
        // The lazily booted shape: a request enters the scope before its
        // host exists, and a read in that window must not throw.
        const first = await startHost();
        let late: Host | undefined;

        await runWithHost(
            () => late,
            async () => {
                expect(currentHost()).toBe(first.host);
                late = (await startHost()).host;
                expect(currentHost()).toBe(late);
            }
        );
    });

    it('does not warn about a second host started inside a scope', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
        await startHost();

        await runWithHost(
            () => undefined,
            () => startHost()
        );
        expect(warn).not.toHaveBeenCalledWith(
            expect.stringContaining('a second host was started')
        );

        // Outside a scope, last-wins is still worth saying out loud.
        await startHost();
        expect(warn).toHaveBeenCalledWith(expect.stringContaining('a second host was started'));
    });
});
