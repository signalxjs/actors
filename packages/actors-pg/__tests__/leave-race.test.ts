/**
 * #209 — `leave()` must not race its own heartbeat. The beat fires
 * `void writeSelf()` every `heartbeatMs` without tracking the promise;
 * `clearInterval` stops FUTURE ticks, but an upsert already handed to the
 * pool is neither awaited nor ordered against the `DELETE` that follows —
 * on another pool connection it commits AFTER it, resurrecting the row
 * until its TTL lapses. Seen as a stale host leaking into the next
 * pg-cluster case on CI.
 *
 * Not env-gated: the pool is a fake whose upsert commits when the test
 * says so, which the structural `PgPoolLike` seam exists to allow.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { pgMembership, type PgPoolLike } from '../src';

const empty = { rows: [], rowCount: 0 };

interface RacingPool extends PgPoolLike {
    /** Host ids whose row exists — an upsert lands when it COMMITS. */
    rows: Set<string>;
    /** Every write, in commit order. */
    log: string[];
    /** Hold the next upsert on the wire until `commitHeld()`. */
    holdNextUpsert(): void;
    held(): boolean;
    commitHeld(): void;
}

/** `hosts` as a set of ids, with one upsert the test can keep in flight. */
function racingPool(): RacingPool {
    const rows = new Set<string>();
    const log: string[] = [];
    let holdNext = false;
    let held: (() => void) | null = null;
    return {
        rows,
        log,
        holdNextUpsert: () => {
            holdNext = true;
        },
        held: () => held !== null,
        commitHeld: () => {
            const commit = held;
            held = null;
            commit?.();
        },
        query(text: string, params?: unknown[]) {
            if (text.includes('INSERT INTO')) {
                const id = String(params?.[0]);
                const commit = (): void => {
                    rows.add(id);
                    log.push(`upsert ${id}`);
                };
                if (holdNext) {
                    holdNext = false;
                    return new Promise((resolve) => {
                        held = () => {
                            commit();
                            resolve(empty);
                        };
                    });
                }
                commit();
                return Promise.resolve(empty);
            }
            if (text.includes('DELETE FROM') && text.includes('host_id')) {
                const id = String(params?.[0]);
                rows.delete(id);
                log.push(`delete ${id}`);
                return Promise.resolve(empty);
            }
            if (text.includes('SELECT version') || text.includes('UPDATE')) {
                return Promise.resolve({ rows: [{ version: 1 }], rowCount: 1 });
            }
            return Promise.resolve(empty);
        }
    };
}

/** Enough microtask turns for `leave()` to reach its DELETE unimpeded. */
const flush = async (): Promise<void> => {
    for (let i = 0; i < 20; i++) await Promise.resolve();
};

afterEach(() => {
    vi.useRealTimers();
});

describe('pgMembership leave vs in-flight heartbeat (#209)', () => {
    it('leave() waits for a heartbeat already on the wire before its DELETE', async () => {
        vi.useFakeTimers();
        const pool = racingPool();
        const m = pgMembership(pool, { heartbeatMs: 50, ttlMs: 200, pollMs: 60_000 });
        await m.join({ hostId: 'h1', epoch: 1, address: 'http://h1', status: 'active' });
        expect(pool.rows.has('h1')).toBe(true);

        // The next beat's upsert is handed to the pool but has not committed
        // yet — a slow connection, a busy pool.
        pool.holdNextUpsert();
        await vi.advanceTimersByTimeAsync(50);
        expect(pool.held()).toBe(true);

        const leaving = m.leave();
        try {
            await flush();
            // The DELETE must wait for that upsert: issued now, it would be
            // overtaken by the commit and the row would come back.
            expect(pool.log).not.toContain('delete h1');
        } finally {
            pool.commitHeld();
        }
        await leaving;
        expect(pool.log).toEqual(['upsert h1', 'upsert h1', 'delete h1']);
        expect(pool.rows.has('h1')).toBe(false);
    });
});
