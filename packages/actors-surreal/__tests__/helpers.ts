/**
 * Shared test plumbing for the SurrealDB provider suites.
 *
 * Isolation is per FILE and per namespace: `use()` as root creates a
 * namespace/database implicitly, so each suite gets a throwaway one and drops
 * it afterwards. (In 3.2 `REMOVE NAMESPACE` deletes the catalog entry and
 * enqueues the data for a background reaper, so teardown is fast but
 * reclamation is not immediate — never assert on storage right after.)
 */
import { Surreal } from 'surrealdb';
import { surrealRetryable } from '@sigx/actors-surreal';

export type TestDb = Surreal;

const url = (): string => process.env.SURREAL_URL ?? 'ws://127.0.0.1:8000';

/** A namespace unique to one worker AND one run. */
export function testNamespace(): string {
    const worker = process.env.VITEST_WORKER_ID ?? '0';
    const salt = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
    return `sigx_test_${worker}_${salt}`;
}

export async function connect(namespace: string, database = 'test'): Promise<TestDb> {
    const db = new Surreal();
    await db.connect(url(), {
        namespace,
        database,
        authentication: {
            username: process.env.SURREAL_USER ?? 'root',
            password: process.env.SURREAL_PASS ?? 'root'
        },
        // The same posture the package installs on a connection it owns —
        // without it a lost CAS race raises instead of reporting a conflict.
        retry: { enabled: true, attempts: 5, retryable: surrealRetryable }
    });
    // `connect()` SELECTS a namespace/database, it does not create them.
    await db.query(
        `DEFINE NAMESPACE IF NOT EXISTS ${namespace}; ` +
            `USE NS ${namespace}; DEFINE DATABASE IF NOT EXISTS ${database};`
    );
    return db;
}

export async function dropNamespace(db: TestDb, namespace: string): Promise<void> {
    try {
        await db.query(`REMOVE NAMESPACE IF EXISTS ${namespace}`);
    } finally {
        await db.close();
    }
}
