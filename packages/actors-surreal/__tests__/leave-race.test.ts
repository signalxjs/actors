/**
 * #209 — `leave()` must not race its own heartbeat. The beat fires
 * `void writeSelf()` every `heartbeatMs` without tracking the promise;
 * `clearInterval` stops FUTURE ticks, but an `UPSERT` already on the wire
 * is neither awaited nor ordered against the `DELETE` that follows — it
 * can commit AFTER it, resurrecting the row until its TTL lapses. (UPSERT
 * has no gate that would notice the delete — rule 1 in the package notes.)
 *
 * Not env-gated: the db is a fake whose upsert commits when the test says
 * so, which the structural `SurrealQueryable` seam exists to allow.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { surrealMembership } from '../src';
import type { SurrealQueryable } from '../src/connection';

interface RacingDb extends SurrealQueryable {
    /** Host record ids whose row exists — an upsert lands when it COMMITS. */
    rows: Set<string>;
    /** Every write, in commit order. */
    log: string[];
    /** Hold the next upsert on the wire until `commitHeld()`. */
    holdNextUpsert(): void;
    held(): boolean;
    commitHeld(): void;
}

/** The host table as a set of record ids, with one upsert the test can keep in flight. */
function racingDb(): RacingDb {
    const rows = new Set<string>();
    const log: string[] = [];
    let version = 0;
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
        query<R extends unknown[]>(query: string, vars?: Record<string, unknown>): Promise<R> {
            const id = String(vars?.['id']);
            if (query.startsWith('UPSERT $id CONTENT')) {
                const commit = (): void => {
                    rows.add(id);
                    log.push(`upsert ${id}`);
                };
                if (holdNext) {
                    holdNext = false;
                    return new Promise((resolve) => {
                        held = () => {
                            commit();
                            resolve([] as unknown as R);
                        };
                    });
                }
                commit();
                return Promise.resolve([] as unknown as R);
            }
            if (query.startsWith('DELETE $id')) {
                rows.delete(id);
                log.push(`delete ${id}`);
                return Promise.resolve([] as unknown as R);
            }
            if (query.startsWith('RETURN (UPSERT $id SET v += 1')) {
                return Promise.resolve([++version] as unknown as R);
            }
            if (query.includes('SELECT VALUE v FROM ONLY')) {
                return Promise.resolve([version, []] as unknown as R);
            }
            return Promise.resolve([] as unknown as R);
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

describe('surrealMembership leave vs in-flight heartbeat (#209)', () => {
    it('leave() waits for a heartbeat already on the wire before its DELETE', async () => {
        vi.useFakeTimers();
        const db = racingDb();
        const m = surrealMembership({ db, heartbeatMs: 50, ttlMs: 200, pollMs: 60_000 });
        await m.join({ hostId: 'h1', epoch: 1, address: 'http://h1', status: 'active' });
        expect(db.rows.has('sigx_host:h1')).toBe(true);

        // The next beat's upsert is on the wire but has not committed yet.
        db.holdNextUpsert();
        await vi.advanceTimersByTimeAsync(50);
        expect(db.held()).toBe(true);

        const leaving = m.leave();
        try {
            await flush();
            // The DELETE must wait for that upsert: issued now, it would be
            // overtaken by the commit and the row would come back.
            expect(db.log).not.toContain('delete sigx_host:h1');
        } finally {
            db.commitHeld();
        }
        await leaving;
        expect(db.log).toEqual(['upsert sigx_host:h1', 'upsert sigx_host:h1', 'delete sigx_host:h1']);
        expect(db.rows.has('sigx_host:h1')).toBe(false);
    });
});
