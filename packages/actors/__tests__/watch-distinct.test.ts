/**
 * Distinct deliveries on a live read (#442).
 *
 * A watch re-invokes its read after every mutating turn. When the result is
 * what the last delivery carried — the turn touched something the read does
 * not look at — nothing is pushed, so N subscribers pay N fewer writes. The
 * first value always goes out; `watches: { method: { distinct: false } }`
 * restores a delivery per mutating turn; a fingerprint that cannot be taken
 * delivers rather than drops.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, type Host } from '@sigx/actors';
import { createHost, manualScheduler } from '@sigx/actors/host';

let running: Host[] = [];
afterEach(async () => {
    for (const host of running) await host.stop({ timeoutMs: 1000 });
    running = [];
});

let reads = 0;

function cart(distinct: boolean) {
    return defineActor({
        type: distinct ? 'Cart' : 'ChattyCart',
        allowAnonymous: true,
        state: () => ({ items: [] as string[], views: 0 }),
        ...(distinct ? {} : { watches: { total: { distinct: false as const } } }),
        methods: (ctx) => ({
            async total() {
                reads++;
                return ctx.state.items.length;
            },
            async add(item: string) {
                ctx.state.items.push(item);
                await ctx.save();
            },
            /** Mutates state the read never looks at. */
            async view() {
                ctx.state.views++;
                await ctx.save();
            }
        })
    });
}

/** Pull the next value, or 'nothing' if none arrives within a drained tick. */
async function next(iterator: AsyncIterator<unknown>): Promise<unknown> {
    const value = iterator.next().then((r) => r.value);
    const nothing = new Promise((r) => setTimeout(() => r('nothing'), 30));
    return Promise.race([value, nothing]);
}

for (const distinct of [true, false]) {
    describe(`dispatchWatch with distinct ${distinct ? 'on (default)' : 'off (declared)'}`, () => {
        it(
            distinct
                ? 'does not deliver a re-read whose result did not change; a real change still arrives'
                : 'delivers every re-read, changed or not',
            async () => {
                const clock = manualScheduler();
                const def = cart(distinct);
                const host = createHost({
                    actors: [def],
                    scheduler: clock,
                    defaults: { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 }
                });
                running.push(host);
                await host.start();
                reads = 0;
                const ref = { type: def.type, key: 'w' };
                const iterator = host
                    .dispatchWatch!(ref, 'total', [], { callChain: [], callId: 'c' }, { throttleMs: 0 })
                    [Symbol.asyncIterator]();
                // The first value always arrives.
                expect(await next(iterator)).toBe(0);
                expect(reads).toBe(1);

                // A mutation the read does not see: re-read, but delivered
                // only when the declaration asks for every re-read.
                await host.actor(def, 'w').view();
                await new Promise((r) => setTimeout(r, 0));
                expect(reads).toBe(2);
                expect(await next(iterator)).toBe(distinct ? 'nothing' : 0);

                // A mutation the read does see arrives either way.
                await host.actor(def, 'w').add('a');
                expect(await next(iterator)).toBe(1);

                // A value that returns to a previously seen one is still a
                // change from the LAST delivery, so it is delivered.
                await host.actor(def, 'w').view();
                await new Promise((r) => setTimeout(r, 0));
                expect(await next(iterator)).toBe(distinct ? 'nothing' : 1);
                await iterator.return?.();
            }
        );
    });
}

describe('the declaration is validated', () => {
    it('accepts distinct: false alone, principalIndependent: true alone, and both; refuses the defaults spelled out', async () => {
        const good = [
            { distinct: false as const },
            { principalIndependent: true as const },
            { principalIndependent: true as const, distinct: false as const }
        ];
        for (const declaration of good) {
            const def = defineActor({
                type: 'Declared',
                allowAnonymous: true,
                state: () => ({ n: 0 }),
                watches: { read: declaration },
                methods: (ctx) => ({ async read() { return ctx.state.n; } })
            });
            const host = createHost({ actors: [def], defaults: { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 } });
            running.push(host);
            await expect(host.actor(def, 'k').read()).resolves.toBe(0);
        }
        for (const declaration of [{ distinct: true }, { principalIndependent: false }, {}, { other: 1 }]) {
            const def = defineActor({
                type: 'Misdeclared',
                allowAnonymous: true,
                state: () => ({ n: 0 }),
                watches: { read: declaration as never },
                methods: (ctx) => ({ async read() { return ctx.state.n; } })
            });
            const host = createHost({ actors: [def], defaults: { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 } });
            running.push(host);
            // The declaration is checked at the type's first activation, so
            // the caller sees the activation failure wrapping it.
            await expect(host.actor(def, 'k').read()).rejects.toThrow(/activation of Misdeclared/);
        }
    });
});
