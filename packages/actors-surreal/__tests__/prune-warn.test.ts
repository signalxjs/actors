/**
 * #268 — a permanently failing membership PRUNE must not be silent. The
 * prune is best-effort by design (expired rows are already excluded by the
 * TTL predicate), but `.catch(noop)` hid a permissions error or schema
 * drift forever while `sigx_host` accumulated dead rows. Under `__DEV__`
 * the failure now warns — once per membership, not once per refresh, since
 * the prune rides every poll tick.
 *
 * Not env-gated: the failure is injected through a fake `db`, which the
 * structural `SurrealQueryable` seam exists to allow.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { surrealMembership } from '../src';
import type { SurrealQueryable } from '../src/connection';

const isPrune = (query: string): boolean => query.startsWith('DELETE (SELECT VALUE id FROM');
const isRefresh = (query: string): boolean => query.includes('SELECT VALUE v FROM ONLY');

/** A db whose PRUNE statement always rejects; everything else answers. */
function failingPruneDb(): SurrealQueryable & { pruneAttempts: () => number } {
    let attempts = 0;
    return {
        pruneAttempts: () => attempts,
        query<R extends unknown[]>(query: string): Promise<R> {
            if (isPrune(query)) {
                attempts += 1;
                return Promise.reject(new Error('IAM error: Not enough permissions'));
            }
            if (isRefresh(query)) return Promise.resolve([1, []] as unknown as R);
            return Promise.resolve([] as unknown as R);
        }
    };
}

afterEach(() => {
    vi.restoreAllMocks();
});

describe('membership prune failure (#268)', () => {
    it('warns once under __DEV__, and keeps refreshing', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const db = failingPruneDb();
        const membership = surrealMembership({ db });

        const view = await membership.refresh();
        // The refresh itself must survive the failing prune — it is
        // best-effort, not load-bearing.
        expect(view.version).toBe(1);
        await vi.waitFor(() => expect(warn).toHaveBeenCalledTimes(1));
        expect(String(warn.mock.calls[0])).toContain('[sigx actors-surreal] membership prune failed');

        // A second failing prune does not warn again — once per membership.
        await membership.refresh();
        await vi.waitFor(() => expect(db.pruneAttempts()).toBe(2));
        expect(warn).toHaveBeenCalledTimes(1);
    });
});
