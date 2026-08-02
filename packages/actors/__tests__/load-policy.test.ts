/**
 * activationCountPolicy (#241) — load-aware placement.
 *
 * The contract: `choose()` is sync, reads only a cached load view refreshed
 * out of band, degrades to random when un-attached or data-less, and is
 * NEVER load-bearing for correctness — a stale or missing number costs
 * balance, nothing else. The deterministic backbone of these tests is the
 * two-host case: power-of-two-choices always samples both hosts, so strict
 * less-than steers every placement to the colder one until the pending
 * delta reaches parity.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor } from '@sigx/actors';
import {
    activationCountPolicy,
    preferLocalPolicy,
    type HostDescriptor,
    type PlacementPolicy,
    type PolicyRuntime
} from '@sigx/actors/cluster';
import { createCluster, type ClusterHarness } from './harness';

const host = (hostId: string, status: HostDescriptor['status'] = 'active'): HostDescriptor => ({
    hostId,
    epoch: 1,
    address: `http://${hostId}.test`,
    status
});

const ref = { type: 'T', key: 'k' };

function fakeRuntime(
    hostId: string,
    hosts: HostDescriptor[],
    loads: Record<string, number>,
    overrides?: Partial<PolicyRuntime>
): PolicyRuntime {
    return {
        hostId,
        view: () => ({ version: 1, hosts }),
        selfLoad: () => loads[hostId] ?? 0,
        peerLoad: (target) => {
            const value = loads[target.hostId];
            return value === undefined
                ? Promise.reject(new Error(`no load for ${target.hostId}`))
                : Promise.resolve(value);
        },
        ...overrides
    };
}

let cluster: ClusterHarness | null = null;

afterEach(async () => {
    await cluster?.stop();
    cluster = null;
    vi.useRealTimers();
});

describe('choose(), un-attached', () => {
    it('falls back to self with no active hosts and picks only active ones', () => {
        const policy = activationCountPolicy();
        const self = host('s.self');
        expect(policy.choose(ref, { version: 1, hosts: [] }, self)).toBe(self);
        const joining = host('s.j', 'joining');
        const active = host('s.a');
        expect(policy.choose(ref, { version: 1, hosts: [joining, active] }, self)).toBe(active);
        for (let i = 0; i < 50; i++) {
            const chosen = policy.choose(
                ref,
                { version: 1, hosts: [host('s.a'), host('s.b'), joining] },
                self
            );
            expect(chosen.status).toBe('active');
        }
    });

    it('with no load data behaves like random over the active set', () => {
        const policy = activationCountPolicy();
        const hosts = [host('s.a'), host('s.b'), host('s.c')];
        const counts = new Map<string, number>();
        for (let i = 0; i < 300; i++) {
            const chosen = policy.choose(ref, { version: 1, hosts }, hosts[0]!);
            counts.set(chosen.hostId, (counts.get(chosen.hostId) ?? 0) + 1);
        }
        // Every host gets a meaningful share — no data means no favourite.
        // Un-attached, choose() takes its first uniform sample and does no
        // bookkeeping at all, so this IS a uniform draw; loose bound anyway
        // (each ≥ 10%) because it is still random.
        for (const h of hosts) expect(counts.get(h.hostId) ?? 0).toBeGreaterThan(30);
    });
});

describe('choose(), attached', () => {
    it('steers every placement to the colder of two hosts until parity', async () => {
        const policy = activationCountPolicy({ refreshMs: 60_000 });
        const hosts = [host('s.hot'), host('s.cold')];
        const detach = policy.attach!(fakeRuntime('s.hot', hosts, { 's.hot': 10, 's.cold': 0 }));
        // Let the attach-time refresh settle.
        await vi.waitFor(() => {
            expect(policy.choose(ref, { version: 1, hosts }, hosts[0]!).hostId).toBe('s.cold');
        });
        // Already 1 pending on s.cold from the probe above; 9 more before
        // parity. Strict less-than: every one lands cold, deterministically.
        for (let i = 0; i < 9; i++) {
            expect(policy.choose(ref, { version: 1, hosts }, hosts[0]!).hostId).toBe('s.cold');
        }
        // At parity the tie goes to the first random sample — both possible.
        const later = new Set<string>();
        for (let i = 0; i < 50; i++) {
            later.add(policy.choose(ref, { version: 1, hosts }, hosts[0]!).hostId);
        }
        expect(later.has('s.hot')).toBe(true);
        (detach as () => void)();
    });

    it('keeps the stale value when a peer probe fails', async () => {
        const policy = activationCountPolicy({ refreshMs: 30 });
        const hosts = [host('s.self'), host('s.flaky')];
        let calls = 0;
        const runtime = fakeRuntime('s.self', hosts, { 's.self': 5 }, {
            peerLoad: () => {
                calls += 1;
                return calls === 1
                    ? Promise.resolve(0)
                    : Promise.reject(new Error('probe timeout'));
            }
        });
        const detach = policy.attach!(runtime);
        await vi.waitFor(() => expect(calls).toBeGreaterThanOrEqual(2));
        // The failed probes did not erase the first answer: flaky (0) still
        // reads colder than self (5).
        expect(policy.choose(ref, { version: 1, hosts }, hosts[0]!).hostId).toBe('s.flaky');
        (detach as () => void)();
    });

    it('refreshes on the configured cadence and stops at teardown', async () => {
        vi.useFakeTimers();
        const policy = activationCountPolicy({ refreshMs: 100 });
        const hosts = [host('s.only')];
        let reads = 0;
        const runtime = fakeRuntime('s.only', hosts, {}, {
            selfLoad: () => {
                reads += 1;
                return 0;
            }
        });
        const detach = policy.attach!(runtime) as () => void;
        expect(reads).toBe(1); // the attach-time prime
        await vi.advanceTimersByTimeAsync(250);
        expect(reads).toBe(3); // two ticks
        detach();
        await vi.advanceTimersByTimeAsync(500);
        expect(reads).toBe(3); // torn down — no further reads
    });
});

describe('the attach seam on the placement', () => {
    const Seed = defineActor({
        type: 'Seed',
        unguarded: true,
        state: () => ({}),
        methods: () => ({ async touch() {} })
    });

    it('attaches once per policy object and tears down at stop', async () => {
        const attached: PolicyRuntime[] = [];
        let torndown = 0;
        const stateful: PlacementPolicy = {
            name: 'spy',
            backend: 'cluster',
            choose: (_ref, _view, self) => self,
            attach: (runtime) => {
                attached.push(runtime);
                return () => void (torndown += 1);
            }
        };
        cluster = await createCluster(1, {
            actors: [Seed],
            policy: stateful,
            // The SAME object serving a type — must not attach twice.
            typePolicies: { Seed: stateful }
        });
        expect(attached).toHaveLength(1);
        expect(attached[0]!.hostId).toBe(cluster.placements[0]!.identity.hostId);
        expect(attached[0]!.selfLoad()).toBe(0);
        await cluster.hosts[0]!.actor(Seed, 'a').touch();
        expect(attached[0]!.selfLoad()).toBe(1);
        await cluster.stop();
        expect(torndown).toBe(1);
        cluster = null;
    });

    it('attaches a definition-declared policy on first resolution', async () => {
        let attachedCount = 0;
        const declared: PlacementPolicy = {
            name: 'declared-spy',
            backend: 'cluster',
            choose: (_ref, _view, self) => self,
            attach: () => void (attachedCount += 1)
        };
        const Pinned = defineActor({
            type: 'Pinned',
            unguarded: true,
            placement: declared,
            state: () => ({}),
            methods: () => ({ async touch() {} })
        });
        cluster = await createCluster(1, { actors: [Pinned] });
        expect(attachedCount).toBe(0); // lazy — nothing resolved yet
        await cluster.hosts[0]!.actor(Pinned, 'a').touch();
        expect(attachedCount).toBe(1);
        await cluster.hosts[0]!.actor(Pinned, 'b').touch();
        expect(attachedCount).toBe(1); // memoized, not re-attached
    });

    it('a throwing attach is contained — the placement keeps working', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const broken: PlacementPolicy = {
            name: 'broken',
            backend: 'cluster',
            choose: (_ref, _view, self) => self,
            attach: () => {
                throw new Error('attach exploded');
            }
        };
        cluster = await createCluster(1, { actors: [Seed], policy: broken });
        await cluster.hosts[0]!.actor(Seed, 'a').touch();
        expect(cluster.hosts[0]!.stats().activations).toBe(1);
        expect(warn).toHaveBeenCalledWith(
            expect.stringContaining('threw in attach()'),
            expect.any(Error)
        );
        warn.mockRestore();
    });
});

describe('end to end', () => {
    const Seed = defineActor({
        type: 'Seed',
        unguarded: true,
        state: () => ({}),
        methods: () => ({ async touch() {} })
    });
    const Balanced = defineActor({
        type: 'Balanced',
        unguarded: true,
        state: () => ({}),
        methods: () => ({ async touch() {} })
    });

    it('new activations of a balanced type land on the colder host', async () => {
        cluster = await createCluster(2, {
            actors: [Seed, Balanced],
            policy: activationCountPolicy({ refreshMs: 50, timeoutMs: 500 }),
            // The imbalance: seed actors pin to whichever host is called.
            typePolicies: { Seed: preferLocalPolicy() }
        });
        // 30 seed activations on host 0 — the hot host.
        for (let i = 0; i < 30; i++) await cluster.hosts[0]!.actor(Seed, `s${i}`).touch();
        // Let a refresh observe the imbalance.
        await new Promise((r) => setTimeout(r, 200));
        // 10 new balanced actors, all created via the HOT host: with two
        // hosts, power-of-two compares both every time, and 10 < the load
        // gap, so every one must land on the cold host.
        for (let i = 0; i < 10; i++) await cluster.hosts[0]!.actor(Balanced, `b${i}`).touch();
        expect(cluster.hosts[1]!.stats().perType['Balanced']).toBe(10);
        expect(cluster.hosts[0]!.stats().perType['Balanced']).toBeUndefined();
    });
});
