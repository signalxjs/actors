/**
 * The shared `ActorStorage` conformance suite (#65), run against
 * `durableObjectStorage` over a fake `DurableStorage` — the same cases
 * `memoryStorage`, `fileStorage`, Postgres, Redis and SurrealDB run.
 *
 * A Map is all the platform's storage is from this adapter's point of view
 * (`get`/`put`/`delete`/`list`, structured values), so the suite runs in the
 * plain Node lane rather than the workers pool; what a real Durable Object
 * adds — per-object serialisation, `blockConcurrencyWhile` — is the CAS
 * mechanism, which the suite deliberately does not assert.
 * `durable-objects.test.ts` keeps those.
 */
import { describe, expect, it } from 'vitest';
import {
    storageConformance,
    type StorageConformanceFactory,
    type StorageConformanceHarness
} from '@sigx/actors/testing';
import { durableObjectStorage, type DurableStorage } from '@sigx/actors-cloudflare';

type ListingStorage = DurableStorage & { list: NonNullable<DurableStorage['list']> };

/**
 * A DO's storage as a Map. `structuredClone` on both sides, because the real
 * storage serializes: a stored value never aliases what the caller holds.
 * `list` returns the keys in order, as the platform's does — the log's
 * append order (#375).
 */
function fakeStorage(map = new Map<string, unknown>()): ListingStorage {
    return {
        get: async <T,>(key: string) =>
            map.has(key) ? (structuredClone(map.get(key)) as T) : undefined,
        put: async <T,>(key: string, value: T) => void map.set(key, structuredClone(value)),
        delete: async (key: string) => map.delete(key),
        list: async <T,>({ prefix }: { prefix: string }) =>
            new Map(
                [...map.keys()]
                    .filter((k) => k.startsWith(prefix))
                    .sort()
                    .map((k) => [k, structuredClone(map.get(k)) as T] as const)
            )
    };
}

/** The same fake without `list` — a storage the append path cannot read back. */
function unlistedStorage(map = new Map<string, unknown>()): DurableStorage {
    const { get, put, delete: remove } = fakeStorage(map);
    return { get, put, delete: remove };
}

const createDurableObjectStorage: StorageConformanceFactory =
    async (): Promise<StorageConformanceHarness> => ({
        // A fresh Map per harness IS a fresh object's storage; there is
        // nothing to bootstrap and nothing to drop.
        storage: () => durableObjectStorage(fakeStorage()),
        stop: async () => {},
        // Per-entry keys under the record's prefix (#375).
        appendText: true
    });

describe('storage conformance: durableObjectStorage()', () => {
    const skipped: string[] = [];
    for (const testCase of storageConformance) {
        it(testCase.name, async (ctx) => {
            const outcome = await testCase.run(createDurableObjectStorage);
            if (outcome && 'skipped' in outcome) {
                // The adapter hands the platform a structured value and has
                // no saveText by design (#238): only those cases may skip.
                expect(testCase.name).toMatch(/saveText/);
                skipped.push(testCase.name);
                ctx.skip(outcome.skipped);
            }
            expect(outcome).toBeUndefined();
        });
    }
    it('skips exactly the three saveText cases', () => {
        // Pinned as a count so the list cannot grow a skip silently: a
        // required case that started skipping would fail the regex above,
        // and an optional path this adapter gains would shrink this number.
        expect(skipped).toHaveLength(3);
    });
});

describe('durableObjectStorage(): the append layout (#375)', () => {
    const T = 'job';
    const isRecord = (k: string) => k.endsWith(`\u0000${T}\u0000k`);
    const isLog = (k: string) => k.includes('\u0000log\u0000');

    it('appends in O(entry): the snapshot is never rewritten — one key per entry, plus the head', async () => {
        const map = new Map<string, unknown>();
        const storage = durableObjectStorage(fakeStorage(map));
        const big = { big: 'x'.repeat(10_000) };
        const etag = await storage.save(T, 'k', big, null);
        const snapshot = [...map.entries()].find(([k]) => isRecord(k))![1];
        const e2 = await storage.appendText!(T, 'k', '{"step":1}', etag);
        const e3 = await storage.appendText!(T, 'k', '{"step":2}', e2);
        // The very same stored object: no append put the snapshot again.
        expect([...map.entries()].find(([k]) => isRecord(k))![1]).toBe(snapshot);
        expect([...map.keys()].filter(isLog)).toHaveLength(2);
        expect(await storage.load(T, 'k')).toEqual({ state: big, etag: e3, log: [{ step: 1 }, { step: 2 }] });
    });

    it('a full save compacts: the log and the head go, the etag keeps counting', async () => {
        const map = new Map<string, unknown>();
        const storage = durableObjectStorage(fakeStorage(map));
        const e1 = await storage.save(T, 'k', { n: 1 }, null);
        const e2 = await storage.appendText!(T, 'k', '{"step":1}', e1);
        const e3 = await storage.save(T, 'k', { n: 2 }, e2);
        expect(Number(e3)).toBeGreaterThan(Number(e2));
        expect([...map.keys()]).toHaveLength(1);
        expect(await storage.load(T, 'k')).toEqual({ state: { n: 2 }, etag: e3, log: [] });
    });

    it('a clear removes the snapshot, the head and the log', async () => {
        const map = new Map<string, unknown>();
        const storage = durableObjectStorage(fakeStorage(map));
        const e1 = await storage.save(T, 'k', { n: 1 }, null);
        const e2 = await storage.appendText!(T, 'k', '{"step":1}', e1);
        await storage.clear(T, 'k', e2);
        expect(map.size).toBe(0);
    });

    it('orders the log numerically past ten entries — the ordinal is zero-padded', async () => {
        const storage = durableObjectStorage(fakeStorage());
        let etag = await storage.save(T, 'k', {}, null);
        for (let i = 0; i < 12; i++) etag = await storage.appendText!(T, 'k', JSON.stringify({ i }), etag);
        expect((await storage.load(T, 'k'))!.log).toEqual(Array.from({ length: 12 }, (_, i) => ({ i })));
    });

    it('loads a record an older version wrote — no head, no log — with its own etag and an empty log', async () => {
        const map = new Map<string, unknown>();
        await durableObjectStorage(unlistedStorage(map)).save(T, 'k', { n: 1 }, null);
        const storage = durableObjectStorage(fakeStorage(map));
        expect(await storage.load(T, 'k')).toEqual({ state: { n: 1 }, etag: '1', log: [] });
        expect(await storage.appendText!(T, 'k', '{"step":1}', '1')).toBe('2');
    });

    it('keeps a full save per append on a storage that cannot list: no appendText, no log on load', async () => {
        const storage = durableObjectStorage(unlistedStorage());
        expect(storage.appendText).toBeUndefined();
        await storage.save(T, 'k', { n: 1 }, null);
        expect(await storage.load(T, 'k')).toEqual({ state: { n: 1 }, etag: '1' });
    });

    it('never reads another record\'s log under a key that extends this one', async () => {
        const storage = durableObjectStorage(fakeStorage());
        const a = await storage.save(T, 'k', {}, null);
        const b = await storage.save(T, 'k2', {}, null);
        await storage.appendText!(T, 'k2', '{"other":true}', b);
        expect(await storage.load(T, 'k')).toEqual({ state: {}, etag: a, log: [] });
    });
});
