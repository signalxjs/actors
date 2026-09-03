/**
 * @vitest-environment node
 *
 * sqliteStorage tests — what is SQLite-specific, beyond the shared
 * conformance suite: the NUL escape on identifiers (SQLite stores a
 * NUL-bearing string whole and truncates it on every read), the CAS race
 * (the provider pins its own atomicity — the suite deliberately does not),
 * persistence across a close and reopen of the same file, the option
 * checks, and an end-to-end host restart.
 *
 * Gated on `node:sqlite` being importable (Node >= 22.13; the CI matrix has
 * a Node 20 leg) — see storage-conformance.test.ts for why the package is
 * imported dynamically, and why this file runs in the `node` environment.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { defineActor, isStorageConflict } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';

const nodeSqlite = await import('node:sqlite').then(
    (m) => m,
    () => null
);
const hasSqlite = nodeSqlite !== null;
const sqlite = hasSqlite ? await import('@sigx/actors-sqlite') : null;

describe.skipIf(!hasSqlite)('sqliteStorage', () => {
    const dirs: string[] = [];
    afterEach(async () => {
        for (const dir of dirs.splice(0)) await rm(dir, { recursive: true, force: true });
    });
    /** A fresh database file in a fresh temp dir, removed after the test. */
    const freshPath = async (): Promise<string> => {
        const dir = await mkdtemp(join(tmpdir(), 'sigx-sqlite-test-'));
        dirs.push(dir);
        return join(dir, 'actors.db');
    };
    const NUL = String.fromCharCode(0);

    describe('options', () => {
        it('requires either path or database', () => {
            expect(() => sqlite!.sqliteStorage({})).toThrow(/pass either `path` or `database`/);
        });

        it('refuses both path and database — one would be silently ignored', () => {
            const db = new nodeSqlite!.DatabaseSync(':memory:');
            expect(() => sqlite!.sqliteStorage({ path: ':memory:', database: db })).toThrow(
                /pass either `path` or `database`/
            );
            db.close();
        });

        it('refuses a table name that is not a plain identifier', () => {
            expect(() => sqlite!.sqliteStorage({ path: ':memory:', table: 'state; DROP' })).toThrow(
                /must match/
            );
        });

        it('every call rejects after close()', async () => {
            const s = sqlite!.sqliteStorage({ path: ':memory:' });
            await s.save('T', 'k', { n: 1 }, null);
            s.close();
            await expect(s.load('T', 'k')).rejects.toThrow();
            await expect(s.save('T', 'k2', { n: 1 }, null)).rejects.toThrow();
            await expect(s.saveText('T', 'k3', '{"n":1}', null)).rejects.toThrow();
            await expect(s.clear('T', 'k', null)).rejects.toThrow();
        });

        it('close() is idempotent — a finally plus an explicit stop path must not throw', () => {
            const s = sqlite!.sqliteStorage({ path: ':memory:' });
            s.close();
            expect(() => s.close()).not.toThrow();
        });

        it('a caller-supplied database is used as-is and shares its rows', async () => {
            const db = new nodeSqlite!.DatabaseSync(':memory:');
            const a = sqlite!.sqliteStorage({ database: db });
            const b = sqlite!.sqliteStorage({ database: db });
            const etag = await a.save('T', 'k', { n: 1 }, null);
            await expect(b.load('T', 'k')).resolves.toEqual({ state: { n: 1 }, etag });
            // Two tables in one database are two namespaces.
            const other = sqlite!.sqliteStorage({ database: db, table: 'other_state' });
            await expect(other.load('T', 'k')).resolves.toBeNull();
            db.close();
        });
    });

    describe('identifiers', () => {
        it('NUL-bearing keys are distinct records and are stored escaped', async () => {
            const db = new nodeSqlite!.DatabaseSync(':memory:');
            const s = sqlite!.sqliteStorage({ database: db });
            // Two keys that differ only AFTER the NUL: a store that truncated
            // at NUL — or compared truncated forms — would collide them.
            const e1 = await s.save('T', `Cart${NUL}user-42`, { who: 42 }, null);
            const e2 = await s.save('T', `Cart${NUL}user-43`, { who: 43 }, null);
            await expect(s.load('T', `Cart${NUL}user-42`)).resolves.toEqual({ state: { who: 42 }, etag: e1 });
            await expect(s.load('T', `Cart${NUL}user-43`)).resolves.toEqual({ state: { who: 43 }, etag: e2 });
            // The escape is injective: a key that LOOKS like the escaped form
            // is a different record, and so is the bare prefix.
            await expect(s.load('T', 'Cart\\0user-42')).resolves.toBeNull();
            await expect(s.load('T', 'Cart')).resolves.toBeNull();
            // And what sits in the column is readable text: SQLite's own text
            // functions see the whole key, not everything before the NUL.
            const rows = db
                .prepare('SELECT key, length(key) AS len FROM sigx_state ORDER BY key')
                .all() as { key: string; len: number }[];
            expect(rows).toEqual([
                { key: 'Cart\\0user-42', len: 13 },
                { key: 'Cart\\0user-43', len: 13 }
            ]);
            db.close();
        });

        it('the type goes through the same escape', async () => {
            const s = sqlite!.sqliteStorage({ path: ':memory:' });
            const etag = await s.save(`A${NUL}B`, 'k', { nul: true }, null);
            await s.save('A', 'k', { nul: false }, null);
            await expect(s.load(`A${NUL}B`, 'k')).resolves.toEqual({ state: { nul: true }, etag });
            await expect(s.load('A\\0B', 'k')).resolves.toBeNull();
            s.close();
        });
    });

    describe('concurrency', () => {
        it('racing creates produce exactly one winner', async () => {
            const s = sqlite!.sqliteStorage({ path: ':memory:' });
            const results = await Promise.allSettled(
                Array.from({ length: 8 }, (_, i) => s.save('T', 'k', { winner: i }, null))
            );
            expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
            const losers = results.filter((r) => r.status === 'rejected');
            expect(losers).toHaveLength(7);
            for (const loser of losers) expect((loser as PromiseRejectedResult).reason).toSatisfy(isStorageConflict);
            s.close();
        });

        it('racing updates on one etag produce exactly one winner', async () => {
            const s = sqlite!.sqliteStorage({ path: ':memory:' });
            const etag = await s.save('T', 'k', { n: 0 }, null);
            const results = await Promise.allSettled(
                Array.from({ length: 8 }, (_, i) => s.save('T', 'k', { n: i + 1 }, etag))
            );
            const winners = results.filter((r) => r.status === 'fulfilled');
            expect(winners).toHaveLength(1);
            expect(results.filter((r) => r.status === 'rejected')).toHaveLength(7);
            const record = await s.load('T', 'k');
            expect(record?.etag).toBe((winners[0] as PromiseFulfilledResult<string>).value);
            s.close();
        });
    });

    describe('persistence', () => {
        it('state survives close() and a reopen of the same file', async () => {
            const path = await freshPath();
            const first = sqlite!.sqliteStorage({ path });
            const etag = await first.save('T', 'k', { items: ['a', 'b'] }, null);
            const text = await first.saveText('T', 'text', '{"via":"text"}', null);
            first.close();

            const second = sqlite!.sqliteStorage({ path });
            await expect(second.load('T', 'k')).resolves.toEqual({ state: { items: ['a', 'b'] }, etag });
            await expect(second.load('T', 'text')).resolves.toEqual({ state: { via: 'text' }, etag: text });
            // The etag chain continues across the reopen.
            const next = await second.save('T', 'k', { items: [] }, etag);
            expect(next).not.toBe(etag);
            await expect(second.save('T', 'k', { items: ['stale'] }, etag)).rejects.toSatisfy(isStorageConflict);
            second.close();
        });
    });

    describe('end to end', () => {
        const Cart = defineActor({
            type: 'SqliteCart',
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
            const path = await freshPath();
            const storage = sqlite!.sqliteStorage({ path });
            const first = createHost({ actors: [Cart], storage, defaults: quiet });
            await first.start();
            await first.actor(Cart, 'c1').add('apples');
            await first.actor(Cart, 'c1').add('pears');
            await first.stop({ timeoutMs: 1000 });
            storage.close();

            // The same file — a NEW storage instance, like a restart.
            const reopened = sqlite!.sqliteStorage({ path });
            const second = createHost({ actors: [Cart], storage: reopened, defaults: quiet });
            await second.start();
            await expect(second.actor(Cart, 'c1').items()).resolves.toEqual(['apples', 'pears']);
            await second.stop({ timeoutMs: 1000 });
            reopened.close();
        });
    });
});

describe.runIf(!hasSqlite)('sqliteStorage (no node:sqlite)', () => {
    it('skips the SQLite suite when node:sqlite is unavailable', () => {
        expect(hasSqlite).toBe(false);
    });
});
