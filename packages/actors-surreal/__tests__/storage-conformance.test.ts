/**
 * The shared `ActorStorage` conformance suite (#65), run against a REAL
 * SurrealDB — the same cases `memoryStorage`, `fileStorage`, Postgres, Redis
 * and the Durable Object adapter run, so "surrealStorage is a storage" is
 * one list of outcomes rather than this package's own reading of the
 * contract.
 *
 * SurrealDB-specific mechanics — the composite record id carrying a NUL
 * verbatim, `none` versus `null`, the commit-time CAS race — stay in
 * `surreal-storage.test.ts`.
 *
 * Every case gets a throwaway namespace, bootstrapped through the same
 * `ensureSurrealSchema` a booting host runs, and drops it afterwards.
 */
import { describe, expect, it } from 'vitest';
import {
    storageConformance,
    type StorageConformanceFactory,
    type StorageConformanceHarness
} from '@sigx/actors/testing';
import { ensureSurrealSchema, surrealStorage } from '@sigx/actors-surreal';
import { connect, dropNamespace, testNamespace } from './helpers';

const SURREAL_URL = process.env.SURREAL_URL;

const createSurrealStorage: StorageConformanceFactory =
    async (): Promise<StorageConformanceHarness> => {
        const namespace = testNamespace();
        const db = await connect(namespace);
        return {
            bootstrap: () => ensureSurrealSchema(db),
            storage: () => surrealStorage({ db }),
            // surrealStorage stores the serialized form and implements the
            // text path (#238): the saveText cases must run here, not skip.
            saveText: true,
            // ... and keeps a per-record log (#312): the append cases too.
            appendText: true,
            stop: () => dropNamespace(db, namespace)
        };
    };

describe.skipIf(!SURREAL_URL)('storage conformance: surrealStorage()', () => {
    for (const testCase of storageConformance) {
        it(testCase.name, async (ctx) => {
            const outcome = await testCase.run(createSurrealStorage);
            if (outcome && 'skipped' in outcome) {
                ctx.skip(outcome.skipped);
            }
            expect(outcome).toBeUndefined();
        });
    }
});

describe.runIf(!SURREAL_URL)('storage conformance (surreal)', () => {
    it('skips the SurrealDB suite when SURREAL_URL is not set', () => {
        expect(SURREAL_URL).toBeUndefined();
    });
});
