/**
 * @vitest-environment node
 *
 * The shared `ActorStorage` conformance suite (#65), run against a REAL
 * SQLite file — the same cases `memoryStorage`, `fileStorage`, Postgres,
 * Redis, SurrealDB and the Durable Object adapter run, so "sqliteStorage is
 * a storage" is one list of outcomes rather than this package's own reading
 * of the contract.
 *
 * SQLite-specific mechanics — the NUL escape, the CAS race, persistence
 * across a reopen — stay in `sqlite-storage.test.ts`.
 *
 * Gated on `node:sqlite` being importable rather than on an env var: the
 * package needs Node >= 22.13 (its `engines` says so) and the CI matrix has
 * a Node 20 leg, which must skip cleanly. The package itself imports
 * `node:sqlite` statically, so it is loaded dynamically AFTER the probe —
 * a static import would throw at module load, before any `skipIf` ran.
 *
 * `@vitest-environment node`, not the root config's `happy-dom`: vite's
 * client environment externalizes only the builtins the RUNNING Node lists
 * in `module.builtinModules`, and `node:sqlite` (a `node:`-scheme-only
 * module) appears there from Node 24 — on 20 and 22 the resolver refuses
 * it with "Cannot bundle Node.js built-in" before the probe ever runs. The
 * server environment treats every `node:` id as builtin on any Node.
 *
 * Every case gets a fresh database FILE in its own temp directory (not
 * `:memory:` — the on-disk mode with its WAL sidecars is what a host runs
 * on), and `stop()` closes and removes it.
 */
import { describe, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
    storageConformance,
    type StorageConformanceFactory,
    type StorageConformanceHarness
} from '@sigx/actors/testing';

const hasSqlite = await import('node:sqlite').then(
    () => true,
    () => false
);
const sqlite = hasSqlite ? await import('@sigx/actors-sqlite') : null;

const createSqliteStorage: StorageConformanceFactory = async (): Promise<StorageConformanceHarness> => {
    const dir = await mkdtemp(join(tmpdir(), 'sigx-sqlite-conformance-'));
    const storage = sqlite!.sqliteStorage({ path: join(dir, 'actors.db') });
    return {
        storage: () => storage,
        // sqliteStorage stores the serialized form and implements the text
        // path (#238): the saveText cases must run here, not skip.
        saveText: true,
        // ... and keeps a per-record log (#312): the append cases too.
        appendText: true,
        async stop() {
            storage.close();
            await rm(dir, { recursive: true, force: true });
        }
    };
};

describe.skipIf(!hasSqlite)('storage conformance: sqliteStorage()', () => {
    for (const testCase of storageConformance) {
        it(testCase.name, async (ctx) => {
            const outcome = await testCase.run(createSqliteStorage);
            if (outcome && 'skipped' in outcome) {
                ctx.skip(outcome.skipped);
            }
            expect(outcome).toBeUndefined();
        });
    }
});

describe.runIf(!hasSqlite)('storage conformance (sqlite)', () => {
    it('skips the SQLite suite when node:sqlite is unavailable', () => {
        expect(hasSqlite).toBe(false);
    });
});
