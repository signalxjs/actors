/**
 * The shared `ActorStorage` conformance suite (#65), run against
 * `durableObjectStorage` over a fake `DurableStorage` — the same cases
 * `memoryStorage`, `fileStorage`, Postgres, Redis and SurrealDB run.
 *
 * A Map is all the platform's storage is from this adapter's point of view
 * (`get`/`put`/`delete`, structured values), so the suite runs in the plain
 * Node lane rather than the workers pool; what a real Durable Object adds —
 * per-object serialisation, `blockConcurrencyWhile` — is the CAS mechanism,
 * which the suite deliberately does not assert. `durable-objects.test.ts`
 * keeps those.
 */
import { describe, expect, it } from 'vitest';
import {
    storageConformance,
    type StorageConformanceFactory,
    type StorageConformanceHarness
} from '@sigx/actors/testing';
import { durableObjectStorage, type DurableStorage } from '@sigx/actors-cloudflare';

/**
 * A DO's storage as a Map. `structuredClone` on both sides, because the real
 * storage serializes: a stored value never aliases what the caller holds.
 */
function fakeStorage(): DurableStorage {
    const map = new Map<string, unknown>();
    return {
        get: async <T,>(key: string) =>
            map.has(key) ? (structuredClone(map.get(key)) as T) : undefined,
        put: async <T,>(key: string, value: T) => void map.set(key, structuredClone(value)),
        delete: async (key: string) => map.delete(key)
    };
}

const createDurableObjectStorage: StorageConformanceFactory =
    async (): Promise<StorageConformanceHarness> => ({
        // A fresh Map per harness IS a fresh object's storage; there is
        // nothing to bootstrap and nothing to drop.
        storage: () => durableObjectStorage(fakeStorage()),
        stop: async () => {}
    });

describe('storage conformance: durableObjectStorage()', () => {
    const skipped: string[] = [];
    for (const testCase of storageConformance) {
        it(testCase.name, async (ctx) => {
            const outcome = await testCase.run(createDurableObjectStorage);
            if (outcome && 'skipped' in outcome) {
                // The adapter hands the platform a structured value and has
                // neither saveText (#238) nor appendText (#312) by design:
                // only those cases may skip.
                expect(testCase.name).toMatch(/saveText|append/);
                skipped.push(testCase.name);
                ctx.skip(outcome.skipped);
            }
            expect(outcome).toBeUndefined();
        });
    }
    it('skips exactly the text and append cases — three plus six', () => {
        // Pinned as a count so the list cannot grow a skip silently: a
        // required case that started skipping would fail the regex above,
        // and an optional path this adapter gains would shrink this number.
        expect(skipped.filter((n) => /saveText/.test(n))).toHaveLength(3);
        expect(skipped.filter((n) => /append/.test(n))).toHaveLength(6);
        expect(skipped).toHaveLength(9);
    });
});
