/**
 * `ctx.bag` semantics on the activation (#246): frozen, per-read (turn-
 * correct), and empty in the contexts that deliberately do NOT inherit it —
 * detached task bodies and volatile timer ticks, the same rule as
 * `traceparent`.
 */
import { describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

describe('ctx.bag', () => {
    it('reads the current turn\'s bag, frozen, and empty when the call has none', async () => {
        const Probe = defineActor({
            type: 'BagProbe',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({
                async read() {
                    return { bag: { ...ctx.bag }, frozen: Object.isFrozen(ctx.bag) };
                }
            })
        });
        const host = createHost({ actors: [Probe], defaults: quiet });
        await expect(
            host.actor(Probe, 'a').with({ bag: { user: 'ada' } }).read()
        ).resolves.toEqual({ bag: { user: 'ada' }, frozen: true });
        // The next turn carries no bag — nothing leaks across turns.
        await expect(host.actor(Probe, 'a').read()).resolves.toEqual({ bag: {}, frozen: true });
        await host.stop({ timeoutMs: 1000 });
    });

    it('reads empty in a volatile timer tick — ticks have no caller', async () => {
        const ticks: Record<string, string>[] = [];
        const Timed = defineActor({
            type: 'BagTimed',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({
                async arm() {
                    ctx.timer('t', () => void ticks.push({ ...ctx.bag }), { due: 1 });
                }
            })
        });
        const host = createHost({ actors: [Timed], defaults: quiet });
        await host.actor(Timed, 'a').with({ bag: { user: 'ada' } }).arm();
        await new Promise((r) => setTimeout(r, 30));
        expect(ticks).toEqual([{}]);
        await host.stop({ timeoutMs: 1000 });
    });

    it('reads empty inside a detached task body — a task outlives its starter', async () => {
        const seen: Record<string, string>[] = [];
        const Tasked = defineActor({
            type: 'BagTasked',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({
                async kick() {
                    ctx.tasks.start('job');
                }
            }),
            tasks: (task) => ({
                async job() {
                    await task.turn((ctx) => {
                        seen.push({ ...ctx.bag });
                    });
                }
            })
        });
        const host = createHost({ actors: [Tasked], defaults: quiet });
        await host.actor(Tasked, 'a').with({ bag: { user: 'ada' } }).kick();
        await new Promise((r) => setTimeout(r, 50));
        expect(seen).toEqual([{}]);
        await host.stop({ timeoutMs: 1000 });
    });
});
