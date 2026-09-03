/**
 * surrealStorage tests — env-gated on `SURREAL_URL` like the other provider
 * suites (CI starts a SurrealDB 3 container on one Linux job; the rest of the
 * matrix skips cleanly). Covers the ActorStorage contract matrix (mirroring
 * memoryStorage/pgStorage semantics), CAS races, the composite-record-id NUL
 * property, and an end-to-end host-restart state recovery — the property a
 * clustered deployment leans on.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { defineActor, isStorageConflict } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';
import { ensureSurrealSchema, surrealStorage } from '@sigx/actors-surreal';
import { connect, dropNamespace, testNamespace, type TestDb } from './helpers';

const SURREAL_URL = process.env.SURREAL_URL;

describe.skipIf(!SURREAL_URL)('surrealStorage', () => {
    let db: TestDb;
    let namespace: string;
    let seq = 0;

    beforeAll(async () => {
        namespace = testNamespace();
        db = await connect(namespace);
        await ensureSurrealSchema(db);
    });
    afterAll(async () => {
        await dropNamespace(db, namespace);
    });

    const storage = () => surrealStorage({ db });
    /** A fresh type per test — no cross-test record collisions in one ns. */
    const type = () => `T${++seq}`;

    describe('contract', () => {
        it('load of a missing record is null', async () => {
            await expect(storage().load(type(), 'missing')).resolves.toBeNull();
        });

        it('create requires expectedEtag null, then round-trips state and etag', async () => {
            const s = storage();
            const t = type();
            const state = { n: 1, deep: { list: [1, 2] }, tag: 'x' };
            const etag = await s.save(t, 'k', state, null);
            expect(etag).not.toBe('');
            await expect(s.load(t, 'k')).resolves.toEqual({ state, etag, log: [] });
        });

        it('a top-level ARRAY state survives', async () => {
            // A native document field could not hold this at all — one of the
            // reasons state is stored as a JSON string.
            const s = storage();
            const t = type();
            const state = [1, 'two', { three: 3 }, [4]];
            const etag = await s.save(t, 'k', state, null);
            await expect(s.load(t, 'k')).resolves.toEqual({ state, etag, log: [] });
        });

        it('null and scalar states stay distinguishable from absent', async () => {
            // SurrealDB distinguishes `none` from `null`; the JSON-string
            // encoding sidesteps that entirely, which this pins.
            const s = storage();
            const t = type();
            const nullEtag = await s.save(t, 'nul', null, null);
            await expect(s.load(t, 'nul')).resolves.toEqual({ state: null, etag: nullEtag, log: [] });
            const numEtag = await s.save(t, 'num', 0, null);
            await expect(s.load(t, 'num')).resolves.toEqual({ state: 0, etag: numEtag, log: [] });
            await expect(s.load(t, 'absent')).resolves.toBeNull();
        });

        it('create against an existing record conflicts', async () => {
            const s = storage();
            const t = type();
            await s.save(t, 'k', { n: 1 }, null);
            await expect(s.save(t, 'k', { n: 2 }, null)).rejects.toSatisfy(isStorageConflict);
        });

        it('update needs the current etag; a stale one conflicts', async () => {
            const s = storage();
            const t = type();
            const first = await s.save(t, 'k', { n: 1 }, null);
            const second = await s.save(t, 'k', { n: 2 }, first);
            expect(second).not.toBe(first);
            await expect(s.save(t, 'k', { n: 3 }, first)).rejects.toSatisfy(isStorageConflict);
            await expect(s.load(t, 'k')).resolves.toEqual({ state: { n: 2 }, etag: second, log: [] });
        });

        it('update of a missing record conflicts', async () => {
            // UPDATE never creates — which is precisely why this package does
            // NOT use `UPSERT … WHERE`, whose create arm ignores the
            // condition and would resurrect a deleted record here.
            await expect(storage().save(type(), 'k', { n: 1 }, 'not-an-etag')).rejects.toSatisfy(
                isStorageConflict
            );
        });

        it('a stale writer cannot resurrect a deleted record', async () => {
            const s = storage();
            const t = type();
            const etag = await s.save(t, 'k', { n: 1 }, null);
            await s.clear(t, 'k', etag);
            await expect(s.save(t, 'k', { n: 2 }, etag)).rejects.toSatisfy(isStorageConflict);
            await expect(s.load(t, 'k')).resolves.toBeNull();
        });

        it('clear is compare-and-delete; null expected asserts absence', async () => {
            const s = storage();
            const t = type();
            const etag = await s.save(t, 'k', { n: 1 }, null);
            await expect(s.clear(t, 'k', 'stale')).rejects.toSatisfy(isStorageConflict);
            // Present + expected null = conflict (the record exists).
            await expect(s.clear(t, 'k', null)).rejects.toSatisfy(isStorageConflict);
            await s.clear(t, 'k', etag);
            await expect(s.load(t, 'k')).resolves.toBeNull();
            // Missing + expected null = success no-op.
            await expect(s.clear(t, 'k', null)).resolves.toBeUndefined();
            // Missing + a concrete expected = conflict.
            await expect(s.clear(t, 'k', etag)).rejects.toSatisfy(isStorageConflict);
        });

        it('NUL survives in keys and state — the reminder/task-ledger shape', async () => {
            // The runtime feeds this storage exactly that: a task-ledger row's
            // KEY is an actor id (`type<NUL>key`), and a reminder shard
            // record's STATE keys its entries by actor id. Unlike Postgres
            // `text`, SurrealDB carries NUL verbatim inside a composite record
            // id — this pins that there is no escaping layer to get wrong.
            const NUL = String.fromCharCode(0);
            const s = storage();
            const t = type();
            const ledgerKey = `Cart${NUL}user-42`;
            const shardState = {
                [`Room${NUL}general`]: { cleanup: { nextDue: 123, period: 60_000 } },
                note: `a user string may hold ${NUL} too`,
                path: 'C:\\backslashes\\stay\\distinct'
            };
            const etag = await s.save(t, ledgerKey, shardState, null);
            await expect(s.load(t, ledgerKey)).resolves.toEqual({ state: shardState, etag, log: [] });
            // Injective: neither an escaped-looking key nor a different split
            // of the same characters is the same record.
            await expect(s.load(t, 'Cart\\0user-42')).resolves.toBeNull();
            await expect(s.load(`Cart${NUL}user-42`, t)).resolves.toBeNull();
            await s.clear(t, ledgerKey, etag);
        });

        it('two racing creates produce one winner', async () => {
            // The write–write conflict at commit is what makes this safe, and
            // the connection's retry is what turns the loser's abort into a
            // truthful conflict instead of a raw storage error.
            const s = storage();
            const t = type();
            const results = await Promise.allSettled([
                s.save(t, 'k', { winner: 'a' }, null),
                s.save(t, 'k', { winner: 'b' }, null)
            ]);
            expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
            const rejected = results.filter((r) => r.status === 'rejected');
            expect(rejected).toHaveLength(1);
            expect(isStorageConflict((rejected[0] as PromiseRejectedResult).reason)).toBe(true);
        });
    });

    /**
     * The single-walk save path (#238). `saveText` is not a second storage
     * FORMAT — it is the same write reached one walk earlier, so the only
     * thing worth asserting is that nothing downstream can tell which one
     * ran. State is already held as a JSON STRING here (see the provider
     * header), so the string lands in the field directly.
     */
    describe('saveText (#238)', () => {
        it('is implemented — this adapter stores the serialized form', () => {
            expect(storage().saveText).toBeTypeOf('function');
        });

        it('stores exactly what save stores, given the same value', async () => {
            const s = storage();
            const t = type();
            const state = { n: 1, deep: { list: [1, 2, null] }, tag: 'x' };
            await s.saveText!(t, 'text', JSON.stringify(state), null);
            await s.save(t, 'tree', state, null);
            expect((await s.load(t, 'text'))?.state).toEqual((await s.load(t, 'tree'))?.state);
        });

        it('honours CAS and throws the conflict brand', async () => {
            const s = storage();
            const t = type();
            const etag = await s.saveText!(t, 'k', '{"n":1}', null);
            await expect(s.saveText!(t, 'k', '{"n":2}', 'stale')).rejects.toSatisfy(
                isStorageConflict
            );
            await expect(s.saveText!(t, 'k', '{"n":2}', null)).rejects.toSatisfy(isStorageConflict);
            expect((await s.load(t, 'k'))?.state).toEqual({ n: 1 });
            expect(await s.saveText!(t, 'k', '{"n":2}', etag)).toBeTypeOf('string');
            expect((await s.load(t, 'k'))?.state).toEqual({ n: 2 });
        });

        it('interleaves with save on one record', async () => {
            // The host picks a path per boundary, so a record's etag chain
            // runs through both.
            const s = storage();
            const t = type();
            const e1 = await s.save(t, 'k', { n: 1 }, null);
            const e2 = await s.saveText!(t, 'k', '{"n":2}', e1);
            await expect(s.save(t, 'k', { n: 3 }, e2)).resolves.toBeTypeOf('string');
            expect((await s.load(t, 'k'))?.state).toEqual({ n: 3 });
        });
    });

    describe('end to end', () => {
        const Cart = defineActor({
            type: 'SurrealCart',
            allowAnonymous: true,
            state: () => ({ items: [] as string[] }),
            methods: (ctx) => ({
                async add(item: string) {
                    ctx.state.items.push(item);
                    await ctx.save();
                    return ctx.state.items.length;
                },
                async items() {
                    return [...ctx.state.items];
                }
            })
        });
        const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

        it('state survives a host restart', async () => {
            const first = createHost({ actors: [Cart], storage: storage(), defaults: quiet });
            await first.start();
            await first.actor(Cart, 'c1').add('apples');
            await first.actor(Cart, 'c1').add('pears');
            await first.stop({ timeoutMs: 1000 });

            const second = createHost({
                actors: [Cart],
                // The same namespace — a NEW storage instance, like a restart.
                storage: surrealStorage({ db }),
                defaults: quiet
            });
            await second.start();
            await expect(second.actor(Cart, 'c1').items()).resolves.toEqual(['apples', 'pears']);
            await second.stop({ timeoutMs: 1000 });
        });
    });
});

describe.runIf(!SURREAL_URL)('surrealStorage (no SURREAL_URL)', () => {
    it('skips the SurrealDB suite when SURREAL_URL is not set', () => {
        expect(SURREAL_URL).toBeUndefined();
    });
});
