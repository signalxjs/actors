import { describe, expect, it, vi } from 'vitest';
import { defineActor } from '@sigx/actors';
import { createHost, memoryStorage } from '@sigx/actors/host';
import type { ActorStorage } from '@sigx/actors';

/**
 * Pins for state-change detection (#28). Written against the per-mutation
 * deep watch and kept green across its replacement with turn-boundary
 * dirty tracking — every case here is a way the replacement could silently
 * lose a change, and under write-behind a lost change is lost data.
 */

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

function trackedStorage(): { storage: ActorStorage; saves: () => number } {
    const inner = memoryStorage();
    let saves = 0;
    return {
        storage: {
            load: (t, k) => inner.load(t, k),
            save: (t, k, s, e) => {
                saves++;
                return inner.save(t, k, s, e);
            },
            clear: (t, k, e) => inner.clear(t, k, e)
        },
        saves: () => saves
    };
}

const listFeed = defineActor({
    type: 'ListFeed',
    unguarded: true,
    state: () => ({ list: [] as { n: number }[] }),
    methods: (ctx) => ({
        async add() {
            ctx.state.list.push({ n: 0 });
        },
        async bumpFirst() {
            ctx.state.list[0]!.n = 5;
        },
        async read() {
            return ctx.state.list.length;
        }
    }),
    streams: (ctx) => ({
        async *watch() {
            for await (const s of ctx.changes()) yield s.list;
        }
    })
});

describe('turn-boundary change detection', () => {
    it('an object ADDED in one turn is tracked — mutating it next turn emits', async () => {
        const host = createHost({ actors: [listFeed], defaults: quiet });
        const client = host.actor(listFeed, 'a');
        await client.read(); // activate

        const iterator = client.watch()[Symbol.asyncIterator]();
        const emitted: { n: number }[][] = [];
        const pulls = (async () => {
            for (let i = 0; i < 2; i++) {
                const { value, done } = await iterator.next();
                if (done) break;
                emitted.push(value);
            }
        })();
        await new Promise((r) => setTimeout(r, 20));
        await client.add(); // turn N: adds an object the tracker has never walked
        await client.bumpFirst(); // turn N+1: mutates ONLY inside that object
        await pulls;
        expect(emitted).toEqual([[{ n: 0 }], [{ n: 5 }]]);
        await iterator.return?.();
        await host.stop();
    });

    it('an onActivate mutation with no subsequent mutating turn survives write-behind deactivation', async () => {
        const { storage, saves } = trackedStorage();
        const wb = defineActor({
            type: 'WB',
            unguarded: true,
            state: () => ({ stamped: 0 }),
            persistence: { mode: 'write-behind', debounceMs: 60_000 },
            onActivate(ctx) {
                ctx.state.stamped = 42;
            },
            methods: (ctx) => ({
                async read() {
                    return ctx.state.stamped;
                }
            })
        });
        const host = createHost({ actors: [wb], storage, defaults: quiet });
        await host.actor(wb, 'w').read(); // activate; read-only turn
        await host.stop(); // deactivation flush is the only save opportunity
        expect(saves()).toBeGreaterThan(0);
        const record = await storage.load('WB', 'w');
        expect(record!.state).toMatchObject({ stamped: 42 });
    });

    it('a read-only turn emits nothing and saves nothing', async () => {
        const { storage, saves } = trackedStorage();
        const wb = defineActor({
            type: 'WB2',
            unguarded: true,
            state: () => ({ n: 0 }),
            persistence: { mode: 'write-behind', debounceMs: 5 },
            methods: (ctx) => ({
                async bump() {
                    ctx.state.n++;
                },
                async read() {
                    return ctx.state.n;
                }
            })
        });
        const host = createHost({ actors: [wb], storage, defaults: quiet });
        const client = host.actor(wb, 'w');
        await client.bump();
        await vi.waitFor(() => {
            expect(saves()).toBe(1);
        });
        for (let i = 0; i < 10; i++) await client.read();
        await new Promise((r) => setTimeout(r, 30));
        expect(saves()).toBe(1); // reads scheduled no further saves
        await host.stop();
        expect(saves()).toBe(1); // and the flush had nothing to add
    });

    it('Map and Set mutations are detected', async () => {
        const collections = defineActor({
            type: 'Coll',
            unguarded: true,
            state: () => ({ m: new Map<string, number>(), s: new Set<string>() }),
            methods: (ctx) => ({
                async setKey() {
                    ctx.state.m.set('k', 1);
                },
                async addMember() {
                    ctx.state.s.add('x');
                },
                async size() {
                    return ctx.state.m.size + ctx.state.s.size;
                }
            }),
            streams: (ctx) => ({
                async *watch() {
                    for await (const s of ctx.changes()) yield [s.m.size, s.s.size];
                }
            })
        });
        const host = createHost({ actors: [collections], defaults: quiet });
        const client = host.actor(collections, 'c');
        await client.size(); // activate

        const iterator = client.watch()[Symbol.asyncIterator]();
        const emitted: number[][] = [];
        const pulls = (async () => {
            for (let i = 0; i < 2; i++) {
                const { value, done } = await iterator.next();
                if (done) break;
                emitted.push(value);
            }
        })();
        await new Promise((r) => setTimeout(r, 20));
        await client.setKey();
        await client.addMember();
        await pulls;
        expect(emitted).toEqual([
            [1, 0],
            [1, 1]
        ]);
        await iterator.return?.();
        await host.stop();
    });

    it('explicit ctx.save() interleaved with tracked mutations stays consistent', async () => {
        const { storage } = trackedStorage();
        const ex = defineActor({
            type: 'Ex',
            unguarded: true,
            state: () => ({ a: 0, b: 0 }),
            methods: (ctx) => ({
                async mixed() {
                    ctx.state.a++; // tracked mutation
                    await ctx.save(); // explicit save mid-turn
                    ctx.state.b++; // tracked mutation after the save
                },
                async flush() {
                    await ctx.save();
                },
                async read() {
                    return { ...ctx.state };
                }
            }),
            streams: (ctx) => ({
                async *watch() {
                    for await (const s of ctx.changes()) yield { a: s.a, b: s.b };
                }
            })
        });
        const host = createHost({ actors: [ex], storage, defaults: quiet });
        const client = host.actor(ex, 'e');
        await client.read(); // activate

        const iterator = client.watch()[Symbol.asyncIterator]();
        const emitted: { a: number; b: number }[] = [];
        const pulls = (async () => {
            const { value } = await iterator.next();
            emitted.push(value);
        })();
        await new Promise((r) => setTimeout(r, 20));
        await client.mixed();
        await pulls;
        // One snapshot per mutating turn, carrying BOTH mutations.
        expect(emitted).toEqual([{ a: 1, b: 1 }]);
        // The save gate is not stuck: a later explicit save persists b.
        await client.flush();
        const record = await storage.load('Ex', 'e');
        expect(record!.state).toMatchObject({ a: 1, b: 1 });
        await iterator.return?.();
        await host.stop();
    });
});
