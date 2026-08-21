/**
 * #268 — a permanently failing membership prune must not be silent. Same
 * hole as actors-surreal: the lazy `DELETE FROM {s}.hosts` is best-effort
 * (the TTL predicate already excludes expired rows), but `.catch(noop)`
 * hid a permissions error or schema drift forever while the table
 * accumulated dead hosts. Under `__DEV__` the failure now warns — once per
 * membership, not once per refresh, since the prune rides every poll tick.
 *
 * Not env-gated: the failure is injected through a fake pool, which the
 * structural `PgPoolLike` seam exists to allow.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { pgMembership, type PgPoolLike } from '../src';

const empty = { rows: [], rowCount: 0 };

/** A pool whose prune DELETE always rejects; everything else answers. */
function failingPrunePool(): PgPoolLike & { pruneAttempts: () => number } {
    let attempts = 0;
    return {
        pruneAttempts: () => attempts,
        query(text: string) {
            if (text.includes('DELETE FROM')) {
                attempts += 1;
                return Promise.reject(new Error('permission denied for table hosts'));
            }
            if (text.includes('SELECT version')) {
                return Promise.resolve({ rows: [{ version: 1 }], rowCount: 1 });
            }
            return Promise.resolve(empty);
        }
    };
}

afterEach(() => {
    vi.restoreAllMocks();
});

describe('membership prune failure (#268)', () => {
    it('warns once under __DEV__, and keeps refreshing', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const pool = failingPrunePool();
        const membership = pgMembership(pool);

        const view = await membership.refresh();
        // The refresh itself must survive the failing prune — it is
        // best-effort, not load-bearing.
        expect(view.version).toBe(1);
        await vi.waitFor(() => expect(warn).toHaveBeenCalledTimes(1));
        expect(String(warn.mock.calls[0])).toContain('[sigx actors-pg] membership prune failed');

        // A second failing prune does not warn again — once per membership.
        await membership.refresh();
        await vi.waitFor(() => expect(pool.pruneAttempts()).toBe(2));
        expect(warn).toHaveBeenCalledTimes(1);
    });
});
