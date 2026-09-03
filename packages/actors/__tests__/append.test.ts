/**
 * `ctx.append(entry)` — an O(entry) durable write on the `appendText` seam
 * (#312), and the replay that makes a record "snapshot + log" read as one
 * state again.
 *
 * The contract under test, in the order a record lives through it:
 *
 *  - an append FOLDS the entry into live state through the definition's
 *    `applyEntry` reducer and appends it to the record's log — the stored
 *    snapshot does not move;
 *  - a load replays the log through the same reducer, after `migrateState`;
 *  - a full save is the compaction: it stores the folded state and the log
 *    is empty afterwards;
 *  - without a record yet, or on a storage without the seam, an append is
 *    the full save the turn would have made anyway — same result, today's
 *    cost;
 *  - an append is a write under the record's etag: a stale peer's later
 *    save conflicts, and a conflict on the append itself takes the save's
 *    own conflict path (fault, or the #368 reload).
 */
import { describe, expect, it } from 'vitest';
import {
    defineActor,
    isActorError,
    type ActorStorage,
    type MigrateState
} from '@sigx/actors';
import { createHost, manualScheduler, memoryStorage } from '@sigx/actors/host';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

interface Ledger {
    total: number;
    rows: number[];
}
type Entry = { n: number };

function ledgerActor(extra: {
    retryQueuedOnConflict?: true;
    reentrant?: 'always';
    persistence?: { mode: 'write-behind'; debounceMs?: number };
    applyEntry?: false;
    events?: string[];
} = {}) {
    const events = extra.events ?? [];
    return defineActor({
        type: 'Ledger',
        allowAnonymous: true,
        ...(extra.retryQueuedOnConflict ? { retryQueuedOnConflict: true as const } : {}),
        ...(extra.reentrant ? { reentrant: extra.reentrant } : {}),
        ...(extra.persistence ? { persistence: extra.persistence } : {}),
        state: (): Ledger => ({ total: 0, rows: [] }),
        ...(extra.applyEntry === false
            ? {}
            : {
                  applyEntry: (s: Ledger, entry: unknown) => {
                      const { n } = entry as Entry;
                      s.total += n;
                      s.rows.push(n);
                  }
              }),
        onActivate(ctx) {
            events.push(`activate:${ctx.key}`);
        },
        onDeactivate(ctx, reason) {
            events.push(`deactivate:${ctx.key}:${reason}`);
        },
        methods: (ctx) => ({
            async add(n: number) {
                await ctx.append({ n } satisfies Entry);
                return ctx.state.total;
            },
            async read() {
                return { total: ctx.state.total, rows: [...ctx.state.rows] };
            },
            async save() {
                await ctx.save();
            },
            async saveEventually() {
                await ctx.save({ durability: 'eventual' });
            },
            /** A direct write the log does not carry, then an append. */
            async bumpThenAdd(n: number) {
                ctx.state.total += 1000;
                await ctx.append({ n } satisfies Entry);
                return ctx.state.total;
            },
            async touch() {
                ctx.state.total += 1000;
            }
        })
    });
}

/** The record as storage holds it: the snapshot and the log, separately. */
async function stored(storage: ActorStorage, key = 'k') {
    const record = await storage.load('Ledger', key);
    return record && { state: record.state as Ledger, log: record.log as Entry[] | undefined };
}

/** `memoryStorage` minus the seam — what a decorator that forgets to forward it looks like. */
function withoutAppend(inner: ActorStorage): ActorStorage {
    return {
        load: (t, k) => inner.load(t, k),
        save: (t, k, s, e) => inner.save(t, k, s, e),
        clear: (t, k, e) => inner.clear(t, k, e)
    };
}

describe('ctx.append: fold + append (#312)', () => {
    it('folds the entry into live state and appends it to the log; the snapshot does not move', async () => {
        const storage = memoryStorage();
        const host = createHost({ actors: [ledgerActor()], storage, defaults: quiet });
        const client = host.actor(ledgerActor(), 'k');
        // No record yet: the first append is a full save (there is nothing
        // to append to), which creates the record with the folded state.
        await expect(client.add(1)).resolves.toBe(1);
        expect(await stored(storage)).toEqual({ state: { total: 1, rows: [1] }, log: [] });
        // From here on, O(entry): the entry lands in the log, the snapshot
        // is untouched, and live state is the fold.
        await expect(client.add(2)).resolves.toBe(3);
        await expect(client.add(3)).resolves.toBe(6);
        expect(await stored(storage)).toEqual({
            state: { total: 1, rows: [1] },
            log: [{ n: 2 }, { n: 3 }]
        });
        await expect(client.read()).resolves.toEqual({ total: 6, rows: [1, 2, 3] });
        await host.stop();
    });

    it('a fresh activation replays the log — same host after deactivation, and a second host', async () => {
        const storage = memoryStorage();
        const host = createHost({ actors: [ledgerActor()], storage, defaults: quiet });
        const client = host.actor(ledgerActor(), 'k');
        await client.add(1);
        await client.add(2);
        await client.add(3);
        await host.deactivateType('Ledger');
        await expect(client.read()).resolves.toEqual({ total: 6, rows: [1, 2, 3] });
        // Replay wrote nothing: the record is what it was.
        expect(await stored(storage)).toEqual({
            state: { total: 1, rows: [1] },
            log: [{ n: 2 }, { n: 3 }]
        });
        const other = createHost({ actors: [ledgerActor()], storage, defaults: quiet });
        await expect(other.actor(ledgerActor(), 'k').read()).resolves.toEqual({
            total: 6,
            rows: [1, 2, 3]
        });
        await host.stop();
        await other.stop();
    });

    it('a full save is the compaction: the folded state becomes the snapshot and the log is empty', async () => {
        const storage = memoryStorage();
        const host = createHost({ actors: [ledgerActor()], storage, defaults: quiet });
        const client = host.actor(ledgerActor(), 'k');
        await client.add(1);
        await client.add(2);
        await client.save();
        expect(await stored(storage)).toEqual({ state: { total: 3, rows: [1, 2] }, log: [] });
        // And the chain continues from the compacted record.
        await client.add(4);
        expect(await stored(storage)).toEqual({
            state: { total: 3, rows: [1, 2] },
            log: [{ n: 4 }]
        });
        await host.deactivateType('Ledger');
        await expect(client.read()).resolves.toEqual({ total: 7, rows: [1, 2, 4] });
        await host.stop();
    });

    it('falls back to a full save on a storage without the seam — same result, no log', async () => {
        const inner = memoryStorage();
        const storage = withoutAppend(inner);
        const host = createHost({ actors: [ledgerActor()], storage, defaults: quiet });
        const client = host.actor(ledgerActor(), 'k');
        await client.add(1);
        await client.add(2);
        // Every append was a full save: the snapshot carries the fold.
        expect(await stored(inner)).toEqual({ state: { total: 3, rows: [1, 2] }, log: [] });
        await host.deactivateType('Ledger');
        await expect(client.read()).resolves.toEqual({ total: 3, rows: [1, 2] });
        await host.stop();
    });

    it('rejects when the definition has no applyEntry — nothing is written', async () => {
        const storage = memoryStorage();
        const def = ledgerActor({ applyEntry: false });
        const host = createHost({ actors: [def], storage, defaults: quiet });
        const client = host.actor(def, 'k');
        await expect(client.add(1)).rejects.toThrow(/applyEntry/);
        expect(await stored(storage)).toBeNull();
        // The activation is fine — a programming error, not a fault.
        await expect(client.read()).resolves.toEqual({ total: 0, rows: [] });
        await host.stop();
    });

    it('a record with a log fails activation under a definition without applyEntry, naming the type', async () => {
        const storage = memoryStorage();
        const writer = createHost({ actors: [ledgerActor()], storage, defaults: quiet });
        const w = writer.actor(ledgerActor(), 'k');
        await w.add(1);
        await w.add(2);
        await writer.stop();

        const def = ledgerActor({ applyEntry: false });
        const host = createHost({ actors: [def], storage, defaults: quiet });
        await expect(host.actor(def, 'k').read()).rejects.toSatisfy(
            (e: unknown) =>
                isActorError(e) &&
                e.kind === 'activation' &&
                /applyEntry/.test(String((e as { cause?: Error }).cause?.message)) &&
                /"Ledger"/.test(String((e as { cause?: Error }).cause?.message))
        );
        // Loud, never silently reset: the record is untouched.
        expect(await stored(storage)).toEqual({
            state: { total: 1, rows: [1] },
            log: [{ n: 2 }]
        });
        await host.stop();
    });

    it('is not available from a tasks: body — persistence goes through turn()', async () => {
        const def = defineActor({
            type: 'Tasked',
            allowAnonymous: true,
            state: () => ({ n: 0 }),
            applyEntry: (s, e) => {
                s.n += e as number;
            },
            methods: (ctx) => ({
                async go() {
                    await ctx.tasks.start('t', 1);
                    return ctx.state.n;
                }
            }),
            tasks: (tctx) => ({
                async t(by: unknown) {
                    // Untyped cast: the member is typed away on the task context.
                    await (tctx as unknown as { append(e: unknown): Promise<void> }).append(by);
                }
            })
        });
        const host = createHost({ actors: [def], defaults: quiet });
        const client = host.actor(def, 'k');
        await client.go();
        // The detached body's rejection is the task's own; the state never moved.
        await expect(client.go()).resolves.toBe(0);
        await host.stop();
    });
});

describe('ctx.append: the etag chain', () => {
    it("a stale peer's full save after an append conflicts — two hosts on one storage", async () => {
        const storage = memoryStorage();
        const hostA = createHost({ actors: [ledgerActor()], storage, defaults: quiet });
        const hostB = createHost({ actors: [ledgerActor()], storage, defaults: quiet });
        const a = hostA.actor(ledgerActor(), 'k');
        const b = hostB.actor(ledgerActor(), 'k');
        await a.add(1); // creates the record; A holds its etag
        await b.read(); // B activates on that record and holds the same etag
        await a.add(2); // O(entry) — but it moves the etag
        // B's full save presents the etag the append superseded.
        await expect(b.save()).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'state-conflict'
        );
        // Nothing of B's landed; B's next call re-activates on the fold.
        expect(await stored(storage)).toEqual({ state: { total: 1, rows: [1] }, log: [{ n: 2 }] });
        await expect(b.read()).resolves.toEqual({ total: 3, rows: [1, 2] });
        await hostA.stop();
        await hostB.stop();
    });

    it('a conflict on the append faults the activation exactly like a save', async () => {
        const events: string[] = [];
        const storage = memoryStorage();
        const def = ledgerActor({ events });
        const host = createHost({ actors: [def], storage, defaults: quiet });
        const client = host.actor(def, 'k');
        await client.add(1);
        // A second writer moves the record behind the activation's back.
        const record = await storage.load('Ledger', 'k');
        await storage.save('Ledger', 'k', { total: 99, rows: [99] }, record!.etag);

        const first = client.add(2);
        const queued = client.add(3);
        await expect(first).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'state-conflict'
        );
        await expect(queued).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'state-conflict'
        );
        expect(events.filter((e) => e.startsWith('deactivate:'))).toEqual(['deactivate:k:conflict']);
        // The winner stands, the losing entries are gone, and the next
        // call activates on the winner.
        expect(await stored(storage)).toEqual({ state: { total: 99, rows: [99] }, log: [] });
        await expect(client.read()).resolves.toEqual({ total: 99, rows: [99] });
        await host.stop();
    });

    it('under retryQueuedOnConflict a conflict on the append parks a reload; queued turns re-run on the winner', async () => {
        const events: string[] = [];
        const storage = memoryStorage();
        const def = ledgerActor({ retryQueuedOnConflict: true, events });
        const host = createHost({ actors: [def], storage, defaults: quiet });
        const client = host.actor(def, 'k');
        await client.add(1);
        // The winner is a snapshot PLUS a log: the reload must replay it.
        const record = await storage.load('Ledger', 'k');
        const etag = await storage.save('Ledger', 'k', { total: 10, rows: [10] }, record!.etag);
        await storage.appendText!('Ledger', 'k', '{"n":20}', etag);

        const first = client.add(2);
        const queued = client.add(3);
        await expect(first).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'state-conflict'
        );
        // 10 + 20 (replayed) + 3, in place — no deactivation.
        await expect(queued).resolves.toBe(33);
        expect(events.filter((e) => e.startsWith('deactivate:'))).toEqual([]);
        expect(await stored(storage)).toEqual({
            state: { total: 10, rows: [10] },
            log: [{ n: 20 }, { n: 3 }]
        });
        await host.stop();
    });

    it('on an interleaving actor an append waits for the in-flight full save and chains on its etag', async () => {
        const inner = memoryStorage();
        let release!: () => void;
        const held = new Promise<void>((r) => (release = r));
        let holdNext = false;
        const storage: ActorStorage = {
            ...inner,
            async save(t, k, s, e) {
                if (holdNext) {
                    holdNext = false;
                    await held;
                }
                return inner.save(t, k, s, e);
            },
            appendText: (t, k, j, e) => inner.appendText!(t, k, j, e)
        };
        const def = ledgerActor({ reentrant: 'always' });
        const host = createHost({ actors: [def], storage, defaults: quiet });
        const client = host.actor(def, 'k');
        await client.add(1);
        holdNext = true;
        const saving = client.save(); // parked inside storage, holding the slot
        await new Promise((r) => setTimeout(r, 10));
        const appending = client.add(2); // must wait for the slot, not race the CAS
        await new Promise((r) => setTimeout(r, 10));
        expect(await stored(inner)).toEqual({ state: { total: 1, rows: [1] }, log: [] });
        release();
        await Promise.all([saving, appending]);
        // The save's snapshot, then the append on the etag the save minted:
        // one chain, no conflict, and a fresh activation reads the fold.
        expect(await stored(inner)).toEqual({ state: { total: 1, rows: [1] }, log: [{ n: 2 }] });
        await host.deactivateType('Ledger');
        await expect(client.read()).resolves.toEqual({ total: 3, rows: [1, 2] });
        await host.stop();
    });
});

describe('ctx.append: what the append makes durable', () => {
    /** Count the JOB's full saves — appends are the point, and are not saves. */
    function counting() {
        const inner = memoryStorage();
        const counts = { saves: 0, appends: 0 };
        const storage: ActorStorage = {
            load: (t, k) => inner.load(t, k),
            save: (t, k, s, e) => {
                counts.saves++;
                return inner.save(t, k, s, e);
            },
            appendText: (t, k, j, e) => {
                counts.appends++;
                return inner.appendText!(t, k, j, e);
            },
            clear: (t, k, e) => inner.clear(t, k, e)
        };
        return { storage, inner, counts };
    }

    it('on a write-behind actor a pure append leaves nothing for the debounce to flush', async () => {
        const { storage, inner, counts } = counting();
        const clock = manualScheduler();
        const def = ledgerActor({ persistence: { mode: 'write-behind', debounceMs: 10 } });
        const host = createHost({ actors: [def], storage, scheduler: clock, defaults: quiet });
        const client = host.actor(def, 'k');
        await client.add(1); // the creating full save
        await client.add(2);
        await client.add(3);
        expect(counts).toEqual({ saves: 1, appends: 2 });
        clock.advance(50);
        await new Promise((r) => setTimeout(r, 0));
        // The entries ARE durable: the debounce found nothing dirty.
        expect(counts).toEqual({ saves: 1, appends: 2 });
        expect(await stored(inner)).toEqual({
            state: { total: 1, rows: [1] },
            log: [{ n: 2 }, { n: 3 }]
        });
        await host.stop();
        expect(counts.saves).toBe(1); // nor did the final flush
    });

    it('a direct write ahead of the append is NOT in the log — the flush still carries it', async () => {
        const { storage, inner, counts } = counting();
        const clock = manualScheduler();
        const def = ledgerActor({ persistence: { mode: 'write-behind', debounceMs: 10 } });
        const host = createHost({ actors: [def], storage, scheduler: clock, defaults: quiet });
        const client = host.actor(def, 'k');
        await client.add(1);
        await expect(client.bumpThenAdd(2)).resolves.toBe(1003);
        expect(counts).toEqual({ saves: 1, appends: 1 });
        clock.advance(50);
        await new Promise((r) => setTimeout(r, 0));
        // The debounce owed the +1000: one full save, which also compacts.
        expect(counts).toEqual({ saves: 2, appends: 1 });
        expect(await stored(inner)).toEqual({ state: { total: 1003, rows: [1, 2] }, log: [] });
        await host.stop();
    });

    it('an eventual save asked for before the append is still delivered', async () => {
        const { storage, inner, counts } = counting();
        const clock = manualScheduler();
        const def = ledgerActor();
        const host = createHost({ actors: [def], storage, scheduler: clock, defaults: quiet });
        const client = host.actor(def, 'k');
        await client.add(1);
        await client.touch(); // +1000, unsaved
        await client.saveEventually();
        await client.add(2);
        expect(counts).toEqual({ saves: 1, appends: 1 });
        clock.advance(100);
        await new Promise((r) => setTimeout(r, 0));
        expect(counts).toEqual({ saves: 2, appends: 1 });
        expect(await stored(inner)).toEqual({ state: { total: 1003, rows: [1, 2] }, log: [] });
        await host.stop();
    });
});

describe('ctx.append: replay and migrateState', () => {
    interface CartV2 {
        v: 2;
        items: string[];
        coupons: string[];
    }
    const migrateV1 = (stored: unknown): CartV2 => {
        const s = stored as { v?: 2; items: string[]; coupons?: string[] };
        if (s.v === 2) return s as CartV2;
        return { v: 2, items: s.items, coupons: [] };
    };
    function cartActor(migrateState: MigrateState<CartV2>) {
        return defineActor({
            type: 'Cart',
            allowAnonymous: true,
            state: (): CartV2 => ({ v: 2, items: [], coupons: [] }),
            migrateState,
            // The reducer touches a v2-only field: it can only work on a
            // record that was migrated FIRST.
            applyEntry: (s, entry) => {
                s.coupons.push((entry as { code: string }).code);
            },
            methods: (ctx) => ({
                async read() {
                    return ctx.snapshot();
                }
            })
        });
    }
    /** A v1 record with two entries appended after it. */
    async function seedV1(storage: ActorStorage) {
        let etag = await storage.save('Cart', 'k', { items: ['a'] }, null);
        etag = await storage.appendText!('Cart', 'k', '{"code":"TEN"}', etag);
        return storage.appendText!('Cart', 'k', '{"code":"FREE"}', etag);
    }

    it('migrateState runs BEFORE the replay; a lazy migration writes nothing', async () => {
        const storage = memoryStorage();
        await seedV1(storage);
        const def = cartActor(migrateV1);
        const host = createHost({ actors: [def], storage, defaults: quiet });
        await expect(host.actor(def, 'k').read()).resolves.toEqual({
            v: 2,
            items: ['a'],
            coupons: ['TEN', 'FREE']
        });
        const record = await storage.load('Cart', 'k');
        expect(record!.state).toEqual({ items: ['a'] });
        expect(record!.log).toEqual([{ code: 'TEN' }, { code: 'FREE' }]);
        await host.stop();
    });

    it("persist: 'eager' writes the migrated AND folded state back — the write-back is a compaction", async () => {
        const storage = memoryStorage();
        await seedV1(storage);
        const def = cartActor({ migrate: migrateV1, persist: 'eager' });
        const host = createHost({ actors: [def], storage, defaults: quiet });
        await expect(host.actor(def, 'k').read()).resolves.toEqual({
            v: 2,
            items: ['a'],
            coupons: ['TEN', 'FREE']
        });
        // A full save truncates the log — so it MUST carry the fold, or the
        // two entries would be lost.
        expect(await storage.load('Cart', 'k')).toMatchObject({
            state: { v: 2, items: ['a'], coupons: ['TEN', 'FREE'] },
            log: []
        });
        await host.stop();
    });

    it('a replay that throws fails activation, and the record stays as it was', async () => {
        const storage = memoryStorage();
        const etag = await storage.save('Ledger', 'k', { total: 1, rows: [1] }, null);
        await storage.appendText!('Ledger', 'k', '"not an entry"', etag);
        const def = defineActor({
            type: 'Ledger',
            allowAnonymous: true,
            state: (): Ledger => ({ total: 0, rows: [] }),
            applyEntry: (s, entry) => {
                if (typeof entry !== 'object') throw new Error('bad entry');
                s.total += (entry as Entry).n;
            },
            methods: (ctx) => ({ async read() {
                return ctx.state.total;
            } })
        });
        const host = createHost({ actors: [def], storage, defaults: quiet });
        await expect(host.actor(def, 'k').read()).rejects.toSatisfy(
            (e: unknown) =>
                isActorError(e) &&
                e.kind === 'activation' &&
                /bad entry/.test(String((e as { cause?: Error }).cause?.message))
        );
        expect(await stored(storage)).toEqual({
            state: { total: 1, rows: [1] },
            log: ['not an entry' as unknown as Entry]
        });
        await host.stop();
    });
});
