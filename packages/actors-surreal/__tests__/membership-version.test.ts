/**
 * #267 — `MembershipView.version` must move on a TTL expiry. `sigx_mver` is
 * bumped only by a host that WRITES (join / setStatus / leave); a peer dying
 * silently has no writer, so its departure changed `hosts` while `version`
 * stood still — and a consumer memoizing on `version` latched the stale
 * member count (the production finding this issue came from). The provider
 * now advances the exposed version locally (`max(stored, cached + 1)`)
 * whenever the host signature changes without a counter bump, and
 * re-aligns with the counter only once written bumps carry it past the
 * advanced value.
 *
 * Not env-gated: the expiry is scripted through a fake `db` on the
 * structural `SurrealQueryable` seam — the database-clock version of this
 * case lives in `surreal-cluster.test.ts`.
 */
import { describe, expect, it } from 'vitest';
import type { HostDescriptor } from '@sigx/actors/cluster';
import { surrealMembership } from '../src';
import type { SurrealQueryable } from '../src/connection';

const isRefresh = (query: string): boolean => query.includes('SELECT VALUE v FROM ONLY');

const host = (hostId: string): HostDescriptor => ({
    hostId,
    epoch: 1,
    address: `http://${hostId}`,
    status: 'active'
});

/** A db whose live-host list and counter the test drives by hand. */
function scriptedDb(): SurrealQueryable & { hosts: HostDescriptor[]; stored: number } {
    const db = {
        hosts: [] as HostDescriptor[],
        stored: 0,
        query<R extends unknown[]>(query: string): Promise<R> {
            if (isRefresh(query)) {
                const rows = db.hosts.map((h) => JSON.stringify(h));
                return Promise.resolve([db.stored, rows] as unknown as R);
            }
            return Promise.resolve([] as unknown as R);
        }
    };
    return db;
}

describe('MembershipView.version on expiry (#267)', () => {
    it('advances when a host expires without a counter bump, and re-aligns once the counter overtakes it', async () => {
        const db = scriptedDb();
        db.stored = 3;
        db.hosts = [host('s.w1'), host('s.ghost')];
        const membership = surrealMembership({ db });
        const changes: number[] = [];
        membership.onChange((view) => changes.push(view.version));

        const before = await membership.refresh();
        expect(before.version).toBe(3);
        expect(before.hosts.map((h) => h.hostId)).toEqual(['s.w1', 's.ghost']);

        // The ghost's TTL lapses: the predicate excludes its record, nobody
        // writes the counter. The exposed version must still move.
        db.hosts = [host('s.w1')];
        const expired = await membership.refresh();
        expect(expired.hosts.map((h) => h.hostId)).toEqual(['s.w1']);
        expect(expired.version).toBeGreaterThan(before.version);
        expect(membership.view().version).toBe(expired.version);
        expect(changes).toEqual([3, expired.version]);

        // Nothing changed: the version holds (it is a change token, not a
        // refresh counter), onChange stays quiet, and it is the SAME object —
        // identity-keyed memos (`membersMemo()`) must not churn per poll.
        const same = await membership.refresh();
        expect(same).toBe(expired);
        expect(membership.view()).toBe(expired);
        expect(changes).toHaveLength(2);

        // A WRITTEN bump whose counter merely catches up to the locally
        // advanced value is still a change and must read as one: the
        // version is strictly monotonic per process view.
        db.stored = expired.version;
        db.hosts = [host('s.w1'), host('s.w2')];
        const caughtUp = await membership.refresh();
        expect(caughtUp.version).toBeGreaterThan(expired.version);
        expect(changes).toHaveLength(3);

        // Once the counter runs past the local view, the view follows it.
        db.stored = 42;
        const converged = await membership.refresh();
        expect(converged.version).toBe(42);
        expect(changes).toHaveLength(4);
    });
});
