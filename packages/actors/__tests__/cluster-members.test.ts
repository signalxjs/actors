/**
 * `membersMemo()` and `ClusterPlacement.onChange` (#269): the two public
 * answers to "members, cheaply, on a hot path". A consumer that memoized
 * on `view().version` latched a stale member count, because a provider's
 * version need not move on TTL expiry (#267) — the memo here keys on the
 * view OBJECT, the same key placement's own `activeHosts` memo uses.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor } from '@sigx/actors';
import { membersMemo, type HostDescriptor, type MembershipView } from '@sigx/actors/cluster';
import { createCluster, quiet, selfPolicy, type ClusterHarness } from './harness';

const host = (
    id: string,
    status: HostDescriptor['status'] = 'active',
    types?: string[]
): HostDescriptor => ({
    hostId: id,
    epoch: 1,
    address: `http://${id}.test`,
    status,
    ...(types ? { types } : {})
});

const worker = defineActor({
    type: 'Probe',
    allowAnonymous: true,
    state: () => ({}),
    methods: () => ({ async ping() {} })
});

let running: ClusterHarness | null = null;
afterEach(async () => {
    await running?.stop();
    running = null;
});

describe('membersMemo', () => {
    it('hits while the view object is stable and recomputes on a new object — even at the same version', () => {
        let view: MembershipView = { version: 3, hosts: [host('a'), host('b', 'leaving')] };
        const placement = { view: () => view };
        const members = membersMemo(placement);

        const first = members();
        expect(first.map((h) => h.hostId)).toEqual(['a']); // active by default
        expect(members()).toBe(first); // same view object → same array, no work

        // A provider that expires a host on the store's TTL clock may hand
        // back a NEW view carrying the SAME version (#267). A memo keyed on
        // `version` would still say ['a']; one keyed on the object does not.
        view = { version: 3, hosts: [host('b', 'leaving'), host('c')] };
        const second = members();
        expect(second).not.toBe(first);
        expect(second.map((h) => h.hostId)).toEqual(['c']);
        expect(members()).toBe(second);
    });

    it('applies the members() filter: status and registers', () => {
        const view: MembershipView = {
            version: 1,
            hosts: [
                host('a', 'active', ['Counter']),
                host('b', 'leaving', ['Counter', 'Probe']),
                host('c', 'active', ['Probe']),
                host('legacy') // no `types` → registers everything (#212)
            ]
        };
        const placement = { view: () => view };
        expect(membersMemo(placement)().map((h) => h.hostId)).toEqual(['a', 'c', 'legacy']);
        expect(membersMemo(placement, { status: 'any' })().map((h) => h.hostId)).toEqual([
            'a',
            'b',
            'c',
            'legacy'
        ]);
        expect(membersMemo(placement, { registers: 'Probe' })().map((h) => h.hostId)).toEqual([
            'c',
            'legacy'
        ]);
        expect(
            membersMemo(placement, { registers: 'Probe', status: 'any' })().map((h) => h.hostId)
        ).toEqual(['b', 'c', 'legacy']);
    });

    it('agrees with placement.members() and follows a real membership change', async () => {
        const cluster = await createCluster(2, { actors: [worker], policy: selfPolicy, defaults: quiet });
        running = cluster;
        const placement = cluster.placements[0]!;
        const members = membersMemo(placement, { registers: 'Probe' });

        const before = members();
        expect(before).toEqual(placement.members!({ registers: 'Probe' }));
        expect(before).toHaveLength(2);
        expect(members()).toBe(before); // the memory hub keeps a stable view object

        await cluster.add([worker]);
        const after = members();
        expect(after).not.toBe(before);
        expect(after).toHaveLength(3);
        expect(after).toEqual(placement.members!({ registers: 'Probe' }));
    });
});

describe('ClusterPlacement.onChange', () => {
    it('passes the membership change stream through, and the unsubscribe stops it', async () => {
        const cluster = await createCluster(2, { actors: [worker], policy: selfPolicy, defaults: quiet });
        running = cluster;
        const placement = cluster.placements[0]!;
        expect(placement.onChange).toBeTypeOf('function');

        const seen = vi.fn<(view: MembershipView) => void>();
        const unsubscribe = placement.onChange!(seen);
        const index = await cluster.add([worker]);
        expect(seen).toHaveBeenCalled();
        const latest = seen.mock.lastCall![0];
        expect(latest).toBe(placement.view()); // the SAME object view() now answers
        expect(latest.hosts).toHaveLength(3);

        unsubscribe();
        seen.mockClear();
        await cluster.hosts[index]!.stop({ timeoutMs: 1000 });
        expect(placement.view().hosts).toHaveLength(2); // the change happened…
        expect(seen).not.toHaveBeenCalled(); // …and an unsubscribed listener missed it
    });
});
