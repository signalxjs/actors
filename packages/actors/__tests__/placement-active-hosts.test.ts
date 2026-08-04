import { describe, expect, it } from 'vitest';
import { consistentHashPolicy, memoryClusterHub } from '@sigx/actors/cluster';
import type { HostDescriptor, MembershipView } from '@sigx/actors/cluster';

const host = (id: string, status: HostDescriptor['status'] = 'active'): HostDescriptor => ({
    hostId: id,
    epoch: 1,
    address: `http://${id}.test`,
    status
});

/**
 * Storage-compat tripwire for #27: `rendezvous()` decides reminder-shard
 * ownership, and shard assignment is pinned storage identity — the winner
 * for a given key over a given host set must NEVER change. The expectations
 * below were captured from the implementation BEFORE the active-host
 * memoization landed; if this table ever needs editing, that is a storage
 * migration, not a test update.
 */
describe('consistent-hash placement is pinned', () => {
    const view: MembershipView = {
        version: 7,
        hosts: [
            host('alpha'),
            host('bravo', 'leaving'),
            host('charlie'),
            host('delta'),
            host('echo')
        ]
    };
    const self = host('self');
    const policy = consistentHashPolicy();

    it.each([
        ['Counter', 'a', 'alpha'],
        ['Counter', 'b', 'delta'],
        ['Room', '1', 'charlie'],
        ['Room', '2', 'alpha'],
        ['Job', 'x', 'delta'],
        ['Job', 'y', 'echo'],
        ['T', 'k1', 'charlie'],
        ['T', 'k2', 'echo'],
        ['T', 'k3', 'charlie'],
        ['T', 'k4', 'echo']
    ])('%s/%s -> %s', (type, key, expected) => {
        expect(policy.choose({ type, key }, view, self).hostId).toBe(expected);
    });

    it('never picks a non-active host', () => {
        for (let i = 0; i < 200; i++) {
            expect(policy.choose({ type: 'T', key: `k${i}` }, view, self).hostId).not.toBe('bravo');
        }
    });

    it('falls back to self when no host is active', () => {
        const empty: MembershipView = { version: 1, hosts: [host('x', 'leaving')] };
        expect(policy.choose({ type: 'T', key: 'k' }, empty, self).hostId).toBe('self');
    });
});

describe('active-host memoization', () => {
    it('the filtered list is computed once per view object', () => {
        // Observable proxy for the memo: a view is a snapshot, so placement
        // may derive the active-host list once per view OBJECT. Mutating
        // `hosts` in place (unsupported — see ClusterMembership.view) must
        // therefore be invisible: after a first choose() on this view, a
        // host spliced into the raw array is never picked, however often we
        // sample. If this test ever fails, the memo was removed or bypassed
        // and every choose() is paying the filter again.
        const hosts = [host('alpha')];
        const view: MembershipView = { version: 1, hosts };
        const self = host('self');
        const policy = consistentHashPolicy();
        expect(policy.choose({ type: 'T', key: 'k' }, view, self).hostId).toBe('alpha');
        hosts.push(host('zulu'));
        for (let i = 0; i < 100; i++) {
            expect(policy.choose({ type: 'T', key: `k${i}` }, view, self).hostId).toBe('alpha');
        }
        // A NEW view object with the same hosts derives afresh and can see zulu.
        const fresh: MembershipView = { version: 2, hosts };
        const winners = new Set<string>();
        for (let i = 0; i < 100; i++) {
            winners.add(policy.choose({ type: 'T', key: `k${i}` }, fresh, self).hostId);
        }
        expect(winners).toContain('zulu');
    });
});

describe('memory hub view caching', () => {
    it('returns a stable view object between membership changes', async () => {
        const hub = memoryClusterHub();
        const m = hub.providers().membership;
        await m.join(host('h1'));
        const a = m.view();
        const b = m.view();
        // Identity, not just equality — the active-host memo in placement
        // keys a WeakMap on the view object, so a fresh object per call
        // would silently disable it.
        expect(b).toBe(a);
        expect(await m.refresh()).toBe(a);
    });

    it('invalidates on join, leave, kill and setStatus', async () => {
        const hub = memoryClusterHub();
        const m1 = hub.providers().membership;
        const m2 = hub.providers().membership;
        await m1.join(host('h1'));
        const before = m1.view();
        await m2.join(host('h2'));
        const after = m1.view();
        expect(after).not.toBe(before);
        expect(after.hosts.map((h) => h.hostId).sort()).toEqual(['h1', 'h2']);

        await m2.setStatus('leaving');
        expect(m1.view().hosts.find((h) => h.hostId === 'h2')?.status).toBe('leaving');

        hub.kill('h2');
        expect(m1.view().hosts.map((h) => h.hostId)).toEqual(['h1']);

        await m1.leave();
        expect(m2.view().hosts).toEqual([]);
    });

    it('delivers exactly one onChange per membership change', async () => {
        const hub = memoryClusterHub();
        const m1 = hub.providers().membership;
        const m2 = hub.providers().membership;
        let delivered = 0;
        m1.onChange(() => delivered++);
        await m1.join(host('h1'));
        await m2.join(host('h2'));
        hub.kill('h2');
        expect(delivered).toBe(3);
    });

    it('a captured view is a snapshot — later changes do not mutate it', async () => {
        const hub = memoryClusterHub();
        const m = hub.providers().membership;
        await m.join(host('h1'));
        const snapshot = m.view();
        const m2 = hub.providers().membership;
        await m2.join(host('h2'));
        expect(snapshot.hosts.map((h) => h.hostId)).toEqual(['h1']);
    });
});
