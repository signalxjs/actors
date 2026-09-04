/**
 * An N-host cluster in one process, with counted providers.
 *
 * Adapted from `packages/actors/__tests__/harness.ts` (minus its vitest
 * assertion) and extended with op counting, because the question here is not
 * "how fast" but "how much work does the runtime ASK of the cluster
 * providers, as a function of N".
 *
 * That distinction matters: every host shares one CPU here, so absolute
 * throughput is meaningless — a 100-host run is 100 hosts contending for one
 * core. What IS exact is the algorithmic shape: directory calls per
 * activation, membership notifications per change, placement cost per
 * decision. Those are the numbers that decide whether 100 hosts on 100 VMs
 * works, and they can be had for free on a laptop.
 *
 * What this deliberately CANNOT tell you: what those provider calls cost
 * against real Redis. `memoryClusterHub` answers `refresh()` from a local
 * map; the Redis provider answers it with one `SMEMBERS`, then one `HGET`
 * per id that came back — so a refresh against a cluster of M hosts is
 * `M + 1` round trips, and 100 refreshes are `100 × (M + 1)`. The multiplier
 * is read from `packages/actors-redis/src/index.ts`, not measured, and
 * measuring it for real needs a Redis instance (Tier 2).
 */
import { defineActorApp, memoryStorage } from '@sigx/actors/host';
import type { ActorApp, ActorStorage, Host, HostDefaults } from '@sigx/actors/host';
import { handleActorRequest, matchesActorRequest } from '@sigx/actors/server';
import { cluster, memoryClusterHub } from '@sigx/actors/cluster';
import type {
    ActorDirectory,
    ClusterMembership,
    ClusterPlacement,
    ClusterProviders,
    MemoryClusterHub,
    PlacementPolicy
} from '@sigx/actors/cluster';
import type { AnyActorDefinition } from '@sigx/actors';

/** Deterministic host defaults — no background churn, no call deadlines. */
export const quiet = { sweepIntervalMs: 3_600_000, reminderTickMs: 3_600_000, callTimeoutMs: 0 };

/** Every host activates its own new actors — pins locality for A/B tests. */
export const selfPolicy: PlacementPolicy = {
    name: 'self',
    choose: (_ref, _view, self) => self
};

/**
 * One counter per provider method the runtime can call. Membership
 * `view()` is separated from `refresh()` on purpose: `view()` is a cached
 * local read on every provider, while `refresh()` is the one that goes to
 * the store — conflating them would hide the entire cost.
 */
export interface ProviderCounts {
    'membership.view': number;
    'membership.refresh': number;
    'membership.join': number;
    'membership.setStatus': number;
    'membership.leave': number;
    'membership.isAlive': number;
    /** Change callbacks actually delivered to a host. */
    'membership.notify': number;
    'directory.lookup': number;
    'directory.claim': number;
    'directory.release': number;
    'directory.evict': number;
    'directory.evictHost': number;
}

function emptyCounts(): ProviderCounts {
    return {
        'membership.view': 0,
        'membership.refresh': 0,
        'membership.join': 0,
        'membership.setStatus': 0,
        'membership.leave': 0,
        'membership.isAlive': 0,
        'membership.notify': 0,
        'directory.lookup': 0,
        'directory.claim': 0,
        'directory.release': 0,
        'directory.evict': 0,
        'directory.evictHost': 0
    };
}

export interface Counter {
    readonly counts: ProviderCounts;
    reset(): void;
    total(): number;
    /** Sum of the keys whose names start with `prefix`. */
    sum(prefix: string): number;
}

function createCounter(): Counter {
    const counts = emptyCounts();
    return {
        counts,
        reset() {
            for (const key of Object.keys(counts) as (keyof ProviderCounts)[]) counts[key] = 0;
        },
        total() {
            return Object.values(counts).reduce((a, b) => a + b, 0);
        },
        sum(prefix) {
            let total = 0;
            for (const [key, value] of Object.entries(counts)) {
                if (key.startsWith(prefix)) total += value;
            }
            return total;
        }
    };
}

/** Wrap one host's providers so every call is counted. */
function countProviders(inner: ClusterProviders, counter: Counter): ClusterProviders {
    const c = counter.counts;
    const membership: ClusterMembership = {
        join: (self) => {
            c['membership.join']++;
            return inner.membership.join(self);
        },
        setStatus: (status) => {
            c['membership.setStatus']++;
            return inner.membership.setStatus(status);
        },
        leave: () => {
            c['membership.leave']++;
            return inner.membership.leave();
        },
        view: () => {
            c['membership.view']++;
            return inner.membership.view();
        },
        refresh: () => {
            c['membership.refresh']++;
            return inner.membership.refresh();
        },
        isAlive: (hostId) => {
            c['membership.isAlive']++;
            return inner.membership.isAlive(hostId);
        },
        onChange: (cb) =>
            inner.membership.onChange((view) => {
                // The delivery, not the registration: this is the per-host
                // work a single membership change fans out into, and with a
                // remote provider each delivery is a store round trip.
                c['membership.notify']++;
                cb(view);
            }),
        onSelfSuspect: (cb) => inner.membership.onSelfSuspect(cb)
    };
    const directory: ActorDirectory = {
        lookup: (id) => {
            c['directory.lookup']++;
            return inner.directory.lookup(id);
        },
        claim: (id, mine) => {
            c['directory.claim']++;
            return inner.directory.claim(id, mine);
        },
        release: (id, expected) => {
            c['directory.release']++;
            return inner.directory.release(id, expected);
        },
        evict: (id, expected) => {
            c['directory.evict']++;
            return inner.directory.evict(id, expected);
        },
        ...(inner.directory.evictHost
            ? {
                  evictHost: (hostId: string): Promise<number> => {
                      c['directory.evictHost']++;
                      return inner.directory.evictHost!(hostId);
                  }
              }
            : {})
    };
    return { membership, directory };
}

export interface ClusterOptions {
    actors: readonly AnyActorDefinition[];
    storage?: ActorStorage;
    policy?: PlacementPolicy;
    /**
     * Envelope signing key. Omit for the default (**HMAC on**); pass `null`
     * to disable signing entirely — that is the A/B for what it costs.
     */
    secret?: string | null;
    retries?: number;
    /**
     * Host defaults layered over `quiet`. A scenario that MEASURES background
     * work (a reminder tick, a sweep) names the cadence it wants here; the
     * rest keep `quiet`'s hour-long intervals so nothing fires mid-measurement.
     */
    defaults?: HostDefaults;
}

export interface ClusterHarness {
    hosts: Host[];
    placements: ClusterPlacement[];
    hub: MemoryClusterHub;
    /** Provider calls across every host, since the last `reset()`. */
    counter: Counter;
    fetch: typeof globalThis.fetch;
    /** Add one host to a running cluster — the membership-change probe. */
    addHost(): Promise<void>;
    stop(): Promise<void>;
}

export async function createCluster(n: number, options: ClusterOptions): Promise<ClusterHarness> {
    const hub = memoryClusterHub();
    const storage = options.storage ?? memoryStorage();
    const counter = createCounter();
    const registry = new Map<string, { app: ActorApp; host: Host }>();
    const hosts: Host[] = [];
    const placements: ClusterPlacement[] = [];
    const apps: ActorApp[] = [];

    const pipeFetch: typeof globalThis.fetch = async (input, init) => {
        const request = new Request(input, init);
        const url = new URL(request.url);
        const member = registry.get(url.host);
        // No listener at that address — a connection refusal, which the
        // transport classifies as unreachable.
        if (!member) throw new TypeError(`fetch failed: connection refused to ${url.host}`);
        const route = member.app.routes.find((candidate) => candidate.match(request));
        if (route) return route.handle(request, member.host);
        if (matchesActorRequest(request)) {
            return handleActorRequest(request, { host: member.host, origin: false });
        }
        throw new Error(`unroutable request ${request.url}`);
    };

    const spawn = async (index: number): Promise<void> => {
        const plugin = cluster({
            providers: countProviders(hub.providers(), counter),
            advertise: `http://host${index}.test`,
            // `secret: null` is PASSED, not omitted. Benchmarks run against
            // the prod dist (`--conditions=production`), where `cluster()`
            // now throws on an omitted secret — so the no-HMAC arm has to say
            // it means it. Omitting would also change what is measured: the
            // point of this arm is dispatch cost WITHOUT signing.
            secret: options.secret === null ? null : (options.secret ?? 'bench-secret'),
            fetch: pipeFetch,
            ...(options.policy ? { policy: options.policy } : {}),
            ...(options.retries !== undefined ? { retries: options.retries } : {})
        });
        const app = defineActorApp({
            actors: options.actors,
            storage,
            defaults: { ...quiet, ...options.defaults }
        }).use(plugin);
        const host = await app.start();
        registry.set(`host${index}.test`, { app, host });
        hosts.push(host);
        placements.push(plugin.placement);
        apps.push(app);
    };

    for (let i = 0; i < n; i++) await spawn(i);

    return {
        hosts,
        placements,
        hub,
        counter,
        fetch: pipeFetch,
        addHost: () => spawn(hosts.length),
        stop: async () => {
            // Settle, not throw: a benchmark tearing down 100 hosts should
            // report its numbers even if one drain times out.
            await Promise.allSettled(apps.map((a) => a.stop({ timeoutMs: 2000 })));
        }
    };
}
