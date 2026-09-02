/**
 * #267 — `MembershipView.version` must move on a TTL expiry. The store's
 * `membership_version` counter is bumped only by a host that WRITES
 * (join / setStatus / leave); a peer dying silently has no writer, so its
 * departure changed `hosts` while `version` stood still — and a consumer
 * memoizing on `version` latched the stale member count. The provider now
 * advances the exposed version locally (`max(stored, cached + 1)`) whenever
 * the host signature changes without a counter bump, and re-converges onto
 * the counter at the next written bump.
 *
 * Not env-gated: the expiry is scripted through a fake pool on the
 * structural `PgPoolLike` seam — the database-clock version of this case
 * lives in `pg-cluster.test.ts`.
 */
import { describe, expect, it } from 'vitest';
import type { HostDescriptor } from '@sigx/actors/cluster';
import { pgMembership, type PgPoolLike } from '../src';

const empty = { rows: [], rowCount: 0 };

const host = (hostId: string): HostDescriptor => ({
    hostId,
    epoch: 1,
    address: `http://${hostId}`,
    status: 'active'
});

/** A pool whose live-host list and counter the test drives by hand. */
function scriptedPool(): PgPoolLike & { hosts: HostDescriptor[]; stored: number } {
    const pool = {
        hosts: [] as HostDescriptor[],
        stored: 0,
        query(text: string) {
            if (text.includes('SELECT version')) {
                return Promise.resolve({ rows: [{ version: pool.stored }], rowCount: 1 });
            }
            if (text.includes('SELECT descriptor')) {
                const rows = pool.hosts.map((h) => ({ descriptor: JSON.stringify(h) }));
                return Promise.resolve({ rows, rowCount: rows.length });
            }
            return Promise.resolve(empty);
        }
    };
    return pool;
}

describe('MembershipView.version on expiry (#267)', () => {
    it('advances when a host expires without a counter bump, and re-converges on the next one', async () => {
        const pool = scriptedPool();
        pool.stored = 3;
        pool.hosts = [host('s.w1'), host('s.ghost')];
        const membership = pgMembership(pool);
        const changes: number[] = [];
        membership.onChange((view) => changes.push(view.version));

        const before = await membership.refresh();
        expect(before.version).toBe(3);
        expect(before.hosts.map((h) => h.hostId)).toEqual(['s.w1', 's.ghost']);

        // The ghost's TTL lapses: the predicate excludes its row, nobody
        // writes the counter. The exposed version must still move.
        pool.hosts = [host('s.w1')];
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
        pool.stored = expired.version;
        pool.hosts = [host('s.w1'), host('s.w2')];
        const caughtUp = await membership.refresh();
        expect(caughtUp.version).toBeGreaterThan(expired.version);
        expect(changes).toHaveLength(3);

        // Once the counter runs past the local view, the view follows it.
        pool.stored = 42;
        const converged = await membership.refresh();
        expect(converged.version).toBe(42);
        expect(changes).toHaveLength(4);
    });
});
