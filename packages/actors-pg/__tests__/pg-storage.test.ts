/**
 * pgStorage tests — env-gated on `PG_URL` like the provider suite (CI
 * provides a postgres service container on one Linux job; the rest of the
 * matrix skips cleanly). Covers the ActorStorage contract matrix (mirroring
 * memoryStorage/redisStorage semantics), CAS races, the jsonb array trap,
 * and an end-to-end host-restart state recovery — the property a clustered
 * deployment leans on.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import pg from 'pg';
import { defineActor, isStorageConflict } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';
import { ensurePgSchema, pgStorage } from '@sigx/actors-pg';

const PG_URL = process.env.PG_URL;

describe.skipIf(!PG_URL)('pgStorage', () => {
    let pool: pg.Pool;
    let schema: string;
    let seq = 0;

    beforeAll(async () => {
        pool = new pg.Pool({ connectionString: PG_URL });
        schema = `sigx_test_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
        await ensurePgSchema(pool, { schema });
    });
    afterAll(async () => {
        await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
        await pool.end();
    });

    const storage = () => pgStorage({ pool, schema });
    /** A fresh type per test — no cross-test row collisions in one schema. */
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
            const record = await s.load(t, 'k');
            expect(record).toEqual({ state, etag });
        });

        it('a top-level ARRAY state survives (the node-postgres array trap)', async () => {
            // Unparameterized jsonb would serialize a JS array as a Postgres
            // ARRAY literal; the explicit stringify + ::jsonb cast is what
            // this pins.
            const s = storage();
            const t = type();
            const state = [1, 'two', { three: 3 }, [4]];
            const etag = await s.save(t, 'k', state, null);
            await expect(s.load(t, 'k')).resolves.toEqual({ state, etag });
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
            await expect(s.load(t, 'k')).resolves.toEqual({ state: { n: 2 }, etag: second });
        });

        it('update of a missing record conflicts', async () => {
            await expect(
                storage().save(type(), 'k', { n: 1 }, 'not-an-etag')
            ).rejects.toSatisfy(isStorageConflict);
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
            // Postgres text/jsonb reject raw NUL, and the runtime feeds this
            // storage exactly that: a task-ledger row's KEY is an actor id
            // (`type<NUL>key`), and a reminder shard record's STATE keys its
            // entries by actor id. This pins the pgText escape and the
            // text-not-jsonb column against the real shapes.
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
            await expect(s.load(t, ledgerKey)).resolves.toEqual({ state: shardState, etag });
            // The escape is injective: a key that LOOKS like the escaped
            // form is a different record.
            await expect(s.load(t, 'Cart\\0user-42')).resolves.toBeNull();
            await s.clear(t, ledgerKey, etag);
        });

        it('two racing creates produce one winner', async () => {
            const s = storage();
            const t = type();
            const results = await Promise.allSettled([
                s.save(t, 'k', { winner: 'a' }, null),
                s.save(t, 'k', { winner: 'b' }, null)
            ]);
            const fulfilled = results.filter((r) => r.status === 'fulfilled');
            const rejected = results.filter((r) => r.status === 'rejected');
            expect(fulfilled).toHaveLength(1);
            expect(rejected).toHaveLength(1);
        });
    });

    /**
     * The single-walk save path (#238). `saveText` is not a second storage
     * FORMAT — it is the same write reached one walk earlier, so the only
     * thing worth asserting is that nothing downstream can tell which one
     * ran. `state` is a `text` column here, so the string lands in it
     * directly.
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
            type: 'PgCart',
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
                // The same schema — a NEW storage instance, like a restart.
                storage: pgStorage({ pool, schema }),
                defaults: quiet
            });
            await second.start();
            await expect(second.actor(Cart, 'c1').items()).resolves.toEqual([
                'apples',
                'pears'
            ]);
            await second.stop({ timeoutMs: 1000 });
        });
    });
});

describe.runIf(!PG_URL)('pgStorage (no PG_URL)', () => {
    it('skips the Postgres suite when PG_URL is not set', () => {
        expect(PG_URL).toBeUndefined();
    });
});
