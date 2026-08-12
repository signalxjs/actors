/**
 * The `ClusterMembership` contract on the fake API server: Lease shape,
 * view convergence between instances, the renewal/version separation,
 * liveness expiry with no event, drain visibility, self-fencing, token
 * rotation, the relist safety net — and a 2-host smoke proving the
 * membership/directory seam split end-to-end (k8s membership, in-memory
 * directory) with real forwarded actor calls.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, type Host } from '@sigx/actors';
import { createHost, memoryStorage } from '@sigx/actors/host';
import { handleActorRequest } from '@sigx/actors/server';
import {
    clusterPlacement,
    handleHostRequest,
    matchesHostRequest,
    memoryClusterHub,
    type ClusterMembership,
    type ClusterPlacement,
    type HostDescriptor,
    type HostStatus
} from '@sigx/actors/cluster';
import { k8sMembership, kubeClient } from '@sigx/actors-k8s';
import { buildLease, parseLease, renewPatch, DESCRIPTOR_ANNOTATION, microTime } from '../src/lease';
import { fakeKube, type FakeKube } from './fake-kube';

const LABELS = { 'sigx.dev/cluster': 'default' };

function descriptor(hostId: string, status: HostStatus = 'active'): HostDescriptor {
    return { hostId, epoch: 1, address: `http://${hostId}:7311`, status };
}

async function until(cond: () => boolean, ms = 2000): Promise<void> {
    const start = Date.now();
    while (!cond()) {
        if (Date.now() - start > ms) throw new Error('condition not reached in time');
        await new Promise((resolve) => setTimeout(resolve, 5));
    }
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

describe('lease descriptor round-trip', () => {
    const leaseOptions = {
        name: 'sigx-t',
        namespace: 'ns',
        labels: LABELS,
        ttlMs: 15_000,
        nowMs: 1_000
    };

    it('HostDescriptor.types survives buildLease → parseLease and the renew patch (#214)', () => {
        const typed = {
            hostId: 's.typed',
            epoch: 3,
            address: 'http://typed:7311',
            status: 'active' as const,
            types: ['Counter', 'Relay']
        };
        // Deep-equal on the WHOLE descriptor: `parseLease` validates the
        // required fields and must pass everything else through untouched —
        // a stricter validator that projects known fields would silently
        // make a heterogeneous cluster eligible-for-everything.
        expect(parseLease(buildLease(typed, leaseOptions))).toEqual(typed);
        const patched = renewPatch(2_000, { ...typed, status: 'leaving' as const });
        expect(
            JSON.parse(patched.metadata!.annotations[DESCRIPTOR_ANNOTATION]!).types
        ).toEqual(['Counter', 'Relay']);
    });

    it('a legacy lease without types still parses, and absent stays absent', () => {
        const legacy = {
            hostId: 's.legacy',
            epoch: 1,
            address: 'http://legacy:7311',
            status: 'active' as const
        };
        const parsed = parseLease(buildLease(legacy, leaseOptions));
        expect(parsed).toEqual(legacy);
        expect(parsed!.types).toBeUndefined();
    });
});

describe('k8s membership provider', () => {
    const open: ClusterMembership[] = [];

    function membershipFor(
        fake: FakeKube,
        overrides: Parameters<typeof k8sMembership>[0] = {}
    ): ClusterMembership {
        const membership = k8sMembership({
            apiServer: 'http://fake',
            namespace: 'test',
            fetch: fake.fetch,
            heartbeatMs: 50,
            ttlMs: 1000,
            clockSkewMs: 0,
            relistMs: 0,
            watchBackoff: { minMs: 5, maxMs: 20 },
            ...overrides
        });
        open.push(membership);
        return membership;
    }

    afterEach(async () => {
        await Promise.allSettled(open.map((membership) => membership.leave()));
        open.length = 0;
    });

    it('join writes a labeled Lease carrying the descriptor; view() contains self', async () => {
        const fake = fakeKube();
        const membership = membershipFor(fake);
        await membership.join(descriptor('s.a'));

        const lease = fake.leases.get('sigx-s.a')!;
        expect(lease).toBeDefined();
        expect(lease.metadata.labels).toEqual(LABELS);
        expect(lease.spec.holderIdentity).toBe('s.a');
        expect(lease.spec.leaseDurationSeconds).toBe(1);
        const stored = JSON.parse(lease.metadata.annotations![DESCRIPTOR_ANNOTATION]!) as HostDescriptor;
        expect(stored).toEqual(descriptor('s.a'));

        expect(membership.view().hosts.map((host) => host.hostId)).toEqual(['s.a']);
        expect(membership.view().version).toBeGreaterThan(0);
    });

    it('two instances converge through the watch; onChange fires with a bumped version', async () => {
        const fake = fakeKube();
        const a = membershipFor(fake);
        await a.join(descriptor('s.a'));
        const seen: number[] = [];
        a.onChange((view) => seen.push(view.version));

        const b = membershipFor(fake);
        await b.join(descriptor('s.b'));

        await until(() => a.view().hosts.length === 2);
        await until(() => b.view().hosts.length === 2);
        expect(seen.length).toBeGreaterThan(0);
        expect(seen.at(-1)).toBe(a.view().version);
        expect(b.view().hosts.map((host) => host.hostId).sort()).toEqual(['s.a', 's.b']);
    });

    it('renewals never bump the version or fire onChange', async () => {
        const fake = fakeKube();
        const a = membershipFor(fake);
        const b = membershipFor(fake);
        await a.join(descriptor('s.a'));
        await b.join(descriptor('s.b'));
        await until(() => a.view().hosts.length === 2);

        const version = a.view().version;
        let changes = 0;
        a.onChange(() => changes++);
        await sleep(300); // several heartbeats from both hosts
        expect(a.view().version).toBe(version);
        expect(changes).toBe(0);
    });

    it('a peer that stops renewing drops out via the sweep, with no event', async () => {
        const fake = fakeKube();
        const a = membershipFor(fake);
        await a.join(descriptor('s.a'));

        // A lease with nobody behind it: fresh on arrival, then it just ages.
        const client = kubeClient({ apiServer: 'http://fake', namespace: 'test', fetch: fake.fetch });
        await client.create(
            buildLease(descriptor('s.dead'), {
                name: 'sigx-s.dead',
                namespace: 'test',
                labels: LABELS,
                ttlMs: 1000,
                nowMs: Date.now()
            })
        );
        await until(() => a.view().hosts.length === 2);

        await until(() => a.view().hosts.length === 1, 3000);
        expect(a.view().hosts[0]!.hostId).toBe('s.a');
        // The lease object is still there — liveness expired, not deleted.
        expect(fake.leases.has('sigx-s.dead')).toBe(true);
    });

    it("setStatus('leaving') is immediately visible to peers", async () => {
        const fake = fakeKube();
        const a = membershipFor(fake);
        const b = membershipFor(fake);
        await a.join(descriptor('s.a'));
        await b.join(descriptor('s.b'));
        await until(() => a.view().hosts.length === 2);

        await b.setStatus('leaving');
        await until(
            () => a.view().hosts.find((host) => host.hostId === 's.b')?.status === 'leaving'
        );
    });

    it('leave() deletes the Lease; peers converge; isAlive goes false', async () => {
        const fake = fakeKube();
        const a = membershipFor(fake);
        const b = membershipFor(fake);
        await a.join(descriptor('s.a'));
        await b.join(descriptor('s.b'));
        await until(() => a.view().hosts.length === 2);

        await b.leave();
        expect(fake.leases.has('sigx-s.b')).toBe(false);
        await until(() => a.view().hosts.length === 1);
        await expect(a.isAlive('s.b')).resolves.toBe(false);
    });

    it('isAlive: fresh true, stale false, missing false', async () => {
        const fake = fakeKube();
        const a = membershipFor(fake);
        await a.join(descriptor('s.a'));
        await expect(a.isAlive('s.a')).resolves.toBe(true);

        const client = kubeClient({ apiServer: 'http://fake', namespace: 'test', fetch: fake.fetch });
        const stale = buildLease(descriptor('s.stale'), {
            name: 'sigx-s.stale',
            namespace: 'test',
            labels: LABELS,
            ttlMs: 1000,
            nowMs: Date.now()
        });
        stale.spec.renewTime = microTime(Date.now() - 60_000);
        await client.create(stale);

        await expect(a.isAlive('s.stale')).resolves.toBe(false);
        await expect(a.isAlive('s.ghost')).resolves.toBe(false);
    });

    it('renewal failures past ttlMs fire onSelfSuspect exactly once; recovery un-latches', async () => {
        const fake = fakeKube();
        const membership = membershipFor(fake, { heartbeatMs: 50, ttlMs: 150 });
        await membership.join(descriptor('s.a'));
        let suspects = 0;
        membership.onSelfSuspect(() => suspects++);

        fake.failRenewals(1000);
        await until(() => suspects === 1, 3000);
        await sleep(200);
        expect(suspects).toBe(1); // latched — one fence, not one per beat

        fake.failRenewals(0);
        await sleep(150); // a successful renewal clears the latch
        fake.failRenewals(1000);
        await until(() => suspects === 2, 3000);
    });

    it('a renewal that lands past ttlMs fires onSelfSuspect even though it SUCCEEDS (#45)', async () => {
        // #45: self-suspicion used to fire only from a renewal REJECTION, so
        // a host whose event loop stalled past the TTL resumed, renewed
        // late, succeeded, and kept serving actors a survivor had already
        // taken over. `heartbeatMs > ttlMs` reproduces that deterministically
        // — every beat is late by construction and every renewal succeeds.
        const fake = fakeKube();
        const membership = membershipFor(fake, { heartbeatMs: 300, ttlMs: 100 });
        await membership.join(descriptor('s.a'));
        let suspects = 0;
        membership.onSelfSuspect(() => suspects++);

        await until(() => suspects >= 1, 3000);
        // Nothing failed: the Lease is being renewed happily throughout.
        expect(fake.leases.get('sigx-s.a')).toBeDefined();
    });

    it('a DELETED Lease fires onSelfSuspect and is not recreated (#69)', async () => {
        // The Lease IS this host's membership token. If it is gone —
        // operator cleanup, namespace churn — peers have already aged us out
        // of their views and released every directory claim we held, so a
        // survivor may be serving our actors. Recreating it (what this used
        // to do) silently re-advertises a host whose claims are forfeit:
        // #45's violation reached by a different route.
        const fake = fakeKube();
        const membership = membershipFor(fake, { heartbeatMs: 50, ttlMs: 5_000 });
        await membership.join(descriptor('s.a'));
        let suspects = 0;
        membership.onSelfSuspect(() => suspects++);

        fake.leases.delete('sigx-s.a');
        const before = fake.log.length;
        await until(() => suspects === 1, 3000);

        // Terminal, not self-healing: nothing re-creates the Lease.
        expect(fake.leases.has('sigx-s.a')).toBe(false);
        expect(fake.log.slice(before).filter((line) => line.startsWith('POST'))).toEqual([]);

        // Latched — one fence, not one per beat.
        await sleep(200);
        expect(suspects).toBe(1);
    });

    it('a 404 from our OWN leave() racing a renewal does not suspect (#69)', async () => {
        // leave() clears the beat and nulls `self` before deleting the
        // Lease, so a renewal already on the wire comes back 404 during a
        // perfectly graceful shutdown. That is our own doing, not lost
        // membership, and must not fence.
        const fake = fakeKube();
        const membership = membershipFor(fake, { heartbeatMs: 30, ttlMs: 5_000 });
        await membership.join(descriptor('s.a'));
        let suspects = 0;
        membership.onSelfSuspect(() => suspects++);

        // Hold the next renewal on the wire, then leave underneath it.
        const release = fake.gatePatches();
        await until(() => fake.log.some((line) => line.startsWith('PATCH')), 2000);
        await membership.leave();
        release();

        await sleep(150);
        expect(suspects).toBe(0);
    });

    it('a rotated token causes one 401, a re-read, and no suspect', async () => {
        const fake = fakeKube();
        let current = 'token-a';
        fake.validToken = 'token-a';
        const membership = membershipFor(fake, { token: () => current });
        await membership.join(descriptor('s.a'));
        let suspects = 0;
        membership.onSelfSuspect(() => suspects++);

        current = 'token-b';
        fake.validToken = 'token-b';
        const before = fake.leases.get('sigx-s.a')!.spec.renewTime;
        await until(() => fake.leases.get('sigx-s.a')!.spec.renewTime !== before, 2000);
        expect(suspects).toBe(0);
    });

    it('the relist safety net reconciles a deletion the watch never delivered', async () => {
        const fake = fakeKube();
        const a = membershipFor(fake, { relistMs: 100 });
        await a.join(descriptor('s.a'));

        const client = kubeClient({ apiServer: 'http://fake', namespace: 'test', fetch: fake.fetch });
        await client.create(
            buildLease(descriptor('s.b'), {
                name: 'sigx-s.b',
                namespace: 'test',
                labels: LABELS,
                ttlMs: 60_000, // too fresh for the sweep to ever drop it
                nowMs: Date.now()
            })
        );
        await until(() => a.view().hosts.length === 2);

        // Vanishes with no DELETED event — only a LIST can notice.
        fake.leases.delete('sigx-s.b');
        await until(() => a.view().hosts.length === 1, 1000);
    });

    it('a malformed descriptor on a selector-matching Lease never enters the view', async () => {
        const fake = fakeKube();
        const a = membershipFor(fake);
        await a.join(descriptor('s.a'));

        const client = kubeClient({ apiServer: 'http://fake', namespace: 'test', fetch: fake.fetch });
        const halfFormed = buildLease(descriptor('s.bad'), {
            name: 'sigx-s.bad',
            namespace: 'test',
            labels: LABELS,
            ttlMs: 60_000,
            nowMs: Date.now()
        });
        // Right labels, fresh renewTime — but the descriptor is missing
        // epoch and status, which placement assumes on every view entry.
        halfFormed.metadata.annotations![DESCRIPTOR_ANNOTATION] = JSON.stringify({
            hostId: 's.bad',
            address: 'http://bad:7311'
        });
        await client.create(halfFormed);

        const garbage = buildLease(descriptor('s.garbage'), {
            name: 'sigx-s.garbage',
            namespace: 'test',
            labels: LABELS,
            ttlMs: 60_000,
            nowMs: Date.now()
        });
        garbage.metadata.annotations![DESCRIPTOR_ANNOTATION] = 'not json';
        await client.create(garbage);

        await sleep(150); // plenty of watch deliveries
        expect(a.view().hosts.map((host) => host.hostId)).toEqual(['s.a']);
    });

    it('2-host smoke: k8s membership + in-memory directory under clusterPlacement', async () => {
        const fake = fakeKube();
        const hub = memoryClusterHub();
        const secret = 'smoke-secret';
        const counter = defineActor({
            type: 'Counter',
            allowAnonymous: true,
            state: () => ({ count: 0 }),
            methods: (ctx) => ({
                async increment(by: number) {
                    ctx.state.count += by;
                    await ctx.save();
                    return ctx.state.count;
                }
            })
        });

        const registry = new Map<string, { host: Host; placement: ClusterPlacement }>();
        const pipeFetch: typeof globalThis.fetch = async (input, init) => {
            const request = new Request(input, init);
            const member = registry.get(new URL(request.url).host);
            if (!member) throw new TypeError('connection refused');
            return matchesHostRequest(request)
                ? handleHostRequest(request, { ...member, secret })
                : handleActorRequest(request, { host: member.host, origin: false });
        };

        const storage = memoryStorage();
        const hosts: Host[] = [];
        for (let i = 0; i < 2; i++) {
            const placement = clusterPlacement({
                membership: membershipFor(fake),
                directory: hub.directory,
                advertise: `http://smoke${i}.test`,
                secret,
                fetch: pipeFetch
            });
            const host = createHost({
                actors: [counter],
                storage,
                placement,
                defaults: { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 }
            });
            registry.set(`smoke${i}.test`, { host, placement });
            hosts.push(host);
            await host.start();
        }
        try {
            const [a, b] = await Promise.all([
                hosts[0]!.actor(counter, 'shared').increment(1),
                hosts[1]!.actor(counter, 'shared').increment(1)
            ]);
            expect([a, b].sort()).toEqual([1, 2]);
            await expect(hosts[0]!.actor(counter, 'shared').increment(1)).resolves.toBe(3);
            await expect(hosts[1]!.actor(counter, 'shared').increment(1)).resolves.toBe(4);
        } finally {
            await Promise.allSettled(hosts.map((host) => host.stop({ timeoutMs: 1000 })));
        }
    });
});
