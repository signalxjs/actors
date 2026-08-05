/**
 * The clustering integration suite: N real hosts in one process, wired
 * through the in-memory pipe (real public + internal endpoints, zero
 * sockets). Pins the M1 invariants: single activation across hosts,
 * callChain/deadline propagation, wrong-host redirect convergence, guards
 * running exactly once, cross-host streams with keep-alive release, and
 * exactly-once reminders under the leader lease.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { actor, defineActor, isActorError, type ActorContext } from '@sigx/actors';
import {
    clusterPlacement,
    consistentHashPolicy,
    decodeEnvelope,
    encodeEnvelope,
    memoryClusterHub,
    preferLocalPolicy,
    signAuth,
    verifyAuth,
    HOST_CALL_HEADER,
    type ClusterMembership,
    type HostDescriptor
} from '@sigx/actors/cluster';
import { createHost } from '@sigx/actors/host';
import { __actorRef, configureActors } from '@sigx/actors/client';
import { encodeSymbolPath } from '../src/wire-url';
import { createCluster, quiet, selfPolicy, type ClusterHarness } from './harness';

let running: ClusterHarness | null = null;
afterEach(async () => {
    configureActors(null);
    await running?.stop();
    running = null;
});

function counterActor(events: string[] = []) {
    return defineActor({
        type: 'Counter',
        allowAnonymous: true,
        state: () => ({ count: 0 }),
        onActivate(ctx) {
            events.push(`activate:${ctx.key}`);
        },
        onDeactivate(ctx, reason) {
            events.push(`deactivate:${ctx.key}:${reason}`);
        },
        methods: (ctx) => ({
            async increment(by: number) {
                ctx.state.count += by;
                await ctx.save();
                return ctx.state.count;
            },
            async get() {
                return ctx.state.count;
            },
            async slow(ms: number) {
                await new Promise((r) => setTimeout(r, ms));
                return 'done';
            }
        })
    });
}

describe('envelope', () => {
    it('round-trips the call context; the deadline crosses as its remaining budget', async () => {
        const deadline = Date.now() + 1000;
        // Time the caller spends before sending IS charged against the
        // budget (remaining-ms is computed at send time); transit slack is
        // deliberately not (a few ms per hop, bounded by hop count).
        await new Promise((r) => setTimeout(r, 25));
        const header = encodeEnvelope(
            { callChain: ['Counter\u0000a'], callId: 'c1.x', deadline },
            's.origin'
        );
        expect(header).toMatch(/^[\x20-\x7e]+$/); // header-safe ASCII, NUL escaped
        const decoded = decodeEnvelope(header);
        expect(decoded.from).toBe('s.origin');
        expect(decoded.call.callChain).toEqual(['Counter\u0000a']);
        expect(decoded.call.callId).toBe('c1.x');
        // The receiver re-anchors the remaining budget on its OWN clock,
        // so skew between hosts never inflates or deflates it.
        const remaining = decoded.call.deadline! - Date.now();
        expect(remaining).toBeGreaterThan(0);
        expect(remaining).toBeLessThanOrEqual(1000 - 20);
    });

    it('rejects a version-skewed envelope loudly', () => {
        const header = encodeEnvelope({ callChain: [], callId: 'c' }, 's.a').replace(
            '"v":1',
            '"v":2'
        );
        expect(() => decodeEnvelope(header)).toThrow(/version skew/);
    });

    it('rejects non-numeric remainingMs and non-integer hops (no NaN deadlines)', () => {
        const good = encodeEnvelope({ callChain: [], callId: 'c', deadline: Date.now() + 100 }, 's.a');
        expect(() =>
            decodeEnvelope(good.replace(/"remainingMs":\d+/, '"remainingMs":"soon"'))
        ).toThrow(/malformed/);
        expect(() => decodeEnvelope(good.replace('"hops":1', '"hops":null'))).toThrow(/malformed/);
        expect(() => decodeEnvelope(good.replace('"hops":1', '"hops":1.5'))).toThrow(/malformed/);
    });
});

describe('cluster: activation & routing', () => {
    it('one activation per key across hosts — racing front doors join one owner', async () => {
        const events: string[] = [];
        const cluster = await createCluster(2, { actors: [counterActor(events)] });
        running = cluster;
        const def = counterActor();
        const [a, b] = await Promise.all([
            cluster.hosts[0]!.actor(def, 'shared').increment(1),
            cluster.hosts[1]!.actor(def, 'shared').increment(1)
        ]);
        expect([a, b].sort()).toEqual([1, 2]);
        expect(events.filter((e) => e === 'activate:shared')).toHaveLength(1);
        // Both hosts read the same state wherever it lives.
        await expect(cluster.hosts[0]!.actor(def, 'shared').get()).resolves.toBe(2);
        await expect(cluster.hosts[1]!.actor(def, 'shared').get()).resolves.toBe(2);
    });

    it('a misdirected internal call answers 421 wrong-host and the caller converges', async () => {
        const events: string[] = [];
        const cluster = await createCluster(3, {
            actors: [counterActor(events)],
            policy: selfPolicy
        });
        running = cluster;
        const def = counterActor();
        // Owner: host 2 (selfPolicy pins new activations locally).
        await cluster.hosts[2]!.actor(def, 'k').increment(5);

        // Misdirect: hit host 1's INTERNAL endpoint for host 2's actor,
        // with a correctly signed per-request HMAC.
        const misdirected = await cluster.fetch('http://host1.test/_sigx/host/Counter%23get', {
            method: 'POST',
            headers: {
                'content-type': 'application/json',
                'x-sigx-cluster-auth': await signAuth('test-secret', 'Counter#get', 'c.t'),
                [HOST_CALL_HEADER]: encodeEnvelope({ callChain: [], callId: 'c.t' }, 's.test')
            },
            body: JSON.stringify({ args: ['k'] })
        });
        expect(misdirected.status).toBe(421);
        const parsed = (await misdirected.json()) as {
            error: { data: { kind: string; owner: { hostId: string } } };
        };
        expect(parsed.error.data.kind).toBe('wrong-host');
        expect(parsed.error.data.owner.hostId).toBe(cluster.placements[2]!.identity.hostId);

        // Routing-level convergence: host 0 has no claim and no cache; the
        // directory sends it straight to the owner.
        await expect(cluster.hosts[0]!.actor(def, 'k').get()).resolves.toBe(5);
        expect(events.filter((e) => e === 'activate:k')).toHaveLength(1);
    });

    it('the internal mount verifies the HMAC over a slash-containing actor type', async () => {
        // The pre-check used to read the symbol as the LAST path segment,
        // which is only the whole symbol when the type has no slash. For a
        // packaged type (`acme/greeter`) it recovered `greeter#greet` while
        // the sender had signed `acme/greeter#greet`, so every secured
        // host-to-host call to such an actor 403'd — a mount that works in
        // every test fixture and fails on the one naming convention the
        // README recommends.
        const greeter = defineActor({
            type: 'acme/greeter',
            allowAnonymous: true,
            state: () => ({}),
            methods: () => ({
                async greet() {
                    return 'hi';
                }
            })
        });
        const cluster = await createCluster(1, { actors: [greeter] });
        running = cluster;
        const symbol = 'acme/greeter#greet';
        const res = await cluster.fetch(
            `http://host0.test/_sigx/host/${encodeSymbolPath(symbol)}`,
            {
                method: 'POST',
                headers: {
                    'content-type': 'application/json',
                    'x-sigx-cluster-auth': await signAuth('test-secret', symbol, 'c.t'),
                    [HOST_CALL_HEADER]: encodeEnvelope({ callChain: [], callId: 'c.t' }, 's.test')
                },
                body: JSON.stringify({ args: ['k'] })
            }
        );
        expect(res.status).toBe(200);
        expect(await res.json()).toMatchObject({ data: 'hi' });
    });

    it('the internal mount rejects a bad cluster secret', async () => {
        const cluster = await createCluster(1, { actors: [counterActor()] });
        running = cluster;
        const res = await cluster.fetch('http://host0.test/_sigx/host/Counter%23get', {
            method: 'POST',
            headers: {
                'content-type': 'application/json',
                'x-sigx-cluster-auth': 'wrong',
                [HOST_CALL_HEADER]: encodeEnvelope({ callChain: [], callId: 'c.t' }, 's.test')
            },
            body: JSON.stringify({ args: ['k'] })
        });
        expect(res.status).toBe(403);
    });
});

describe('cluster: call-chain & deadline propagation', () => {
    function pingPong(reentrantAlpha: boolean) {
        type AlphaMethods = { poke(): Promise<string>; back(): Promise<string>; warm(): Promise<string> };
        const alpha = defineActor({
            type: 'Alpha',
            allowAnonymous: true,
            reentrant: reentrantAlpha,
            state: () => ({}),
            methods: (ctx: ActorContext<object>) => ({
                async poke() {
                    return (await (ctx.actor(beta, 'b') as { poke(): Promise<string> }).poke()) as string;
                },
                async back() {
                    return 'alpha-back';
                },
                async warm() {
                    return 'warm';
                }
            })
        });
        const beta = defineActor({
            type: 'Beta',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx: ActorContext<object>) => ({
                async poke() {
                    return (await (ctx.actor(alpha, 'a') as unknown as AlphaMethods).back()) as string;
                },
                async warm() {
                    return 'warm';
                }
            })
        });
        return { alpha, beta };
    }

    it('a cross-host cycle into a non-reentrant actor throws deadlock with the full chain', async () => {
        const { alpha, beta } = pingPong(false);
        const cluster = await createCluster(2, { actors: [alpha, beta], policy: selfPolicy });
        running = cluster;
        // Alpha/a lives on host 0, Beta/b on host 1.
        await cluster.hosts[0]!.actor(alpha, 'a').warm();
        await cluster.hosts[1]!.actor(beta, 'b').warm();

        await expect(cluster.hosts[0]!.actor(alpha, 'a').poke()).rejects.toSatisfy(
            (e: unknown) =>
                isActorError(e) &&
                e.kind === 'deadlock' &&
                /Alpha/.test((e as Error).message) &&
                /Beta/.test((e as Error).message)
        );
    });

    it('a cross-host cycle into a REENTRANT actor runs inline and succeeds', async () => {
        const { alpha, beta } = pingPong(true);
        const cluster = await createCluster(2, { actors: [alpha, beta], policy: selfPolicy });
        running = cluster;
        await cluster.hosts[0]!.actor(alpha, 'a').warm();
        await cluster.hosts[1]!.actor(beta, 'b').warm();
        await expect(cluster.hosts[0]!.actor(alpha, 'a').poke()).resolves.toBe('alpha-back');
    });

    it('the caller deadline crosses the hop as remaining-ms and times out remotely', async () => {
        const cluster = await createCluster(2, {
            actors: [counterActor()],
            policy: selfPolicy,
            defaults: { callTimeoutMs: 60 }
        });
        running = cluster;
        const def = counterActor();
        // Owner: host 1. Call from host 0 → one hop.
        await cluster.hosts[1]!.actor(def, 'slowpoke').get();
        await expect(cluster.hosts[0]!.actor(def, 'slowpoke').slow(500)).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'call-timeout'
        );
    });
});

describe('cluster: guards, streams, reminders, failover', () => {
    it('guards run exactly once: at the public edge, not on the internal hop', async () => {
        const guardLog: string[] = [];
        const guarded = defineActor({
            type: 'Guarded',
            authorize: [
                (_p, _rq, op) => {
                    guardLog.push(`use:${op.fn.symbol}`);
                    return true;
                }
            ],
            state: () => ({}),
            methods: () => ({
                async hello() {
                    return 'hi';
                }
            })
        });
        const cluster = await createCluster(2, { actors: [guarded], policy: selfPolicy });
        running = cluster;
        // Owner: host 1. Public wire call lands on host 0 → internal hop.
        await cluster.hosts[1]!.actor(guarded, 'g').hello();
        guardLog.length = 0;

        configureActors({ endpoint: cluster.endpointOf(0), fetch: cluster.fetch });
        const ref = __actorRef('Guarded', cluster.endpointOf(0)) as unknown as typeof guarded;
        await expect(actor(ref, 'g').hello()).resolves.toBe('hi');
        expect(guardLog).toEqual(['use:Guarded#hello']);
    });

    it('streams cross the hop; consumer break runs the remote finally and frees the actor', async () => {
        const cleanupLog: string[] = [];
        const streamer = defineActor({
            type: 'Streamer',
            allowAnonymous: true,
            state: () => ({}),
            methods: () => ({
                async warm() {
                    return 'warm';
                }
            }),
            streams: () => ({
                async *forever() {
                    try {
                        for (let i = 0; ; i++) {
                            yield i;
                            await new Promise((r) => setTimeout(r, 5));
                        }
                    } finally {
                        cleanupLog.push('forever:finally');
                    }
                },
                async *countTo(to: number) {
                    for (let i = 1; i <= to; i++) yield i;
                }
            })
        });
        const cluster = await createCluster(2, { actors: [streamer], policy: selfPolicy });
        running = cluster;
        // Owner: host 1; consume from host 0 across the internal hop.
        await cluster.hosts[1]!.actor(streamer, 's').warm();

        const finite: number[] = [];
        for await (const chunk of cluster.hosts[0]!.actor(streamer, 's').countTo(3)) {
            finite.push(chunk as number);
        }
        expect(finite).toEqual([1, 2, 3]);

        for await (const chunk of cluster.hosts[0]!.actor(streamer, 's').forever()) {
            if ((chunk as number) >= 2) break;
        }
        await vi.waitFor(() => expect(cleanupLog).toContain('forever:finally'));
        // Keep-alive released on the owner: the activation can deactivate.
        await cluster.hosts[1]!.deactivateType('Streamer');
        expect(cluster.hosts[1]!.stats().activations).toBe(0);
    });

    it('reminders fire exactly once across hosts (rendezvous shard ownership)', async () => {
        const events: string[] = [];
        const waking = defineActor({
            type: 'Waking',
            allowAnonymous: true,
            state: () => ({}),
            onReminder(_ctx, name) {
                events.push(`reminder:${name}`);
            },
            methods: (ctx) => ({
                async wakeMeIn(ms: number) {
                    await ctx.reminders.set('wake', { due: ms });
                }
            })
        });
        const cluster = await createCluster(2, {
            actors: [waking],
            defaults: { reminderTickMs: 25 }
        });
        running = cluster;
        // Register through host 1 (a non-leader mutation is fine).
        await cluster.hosts[1]!.actor(waking, 'r1').wakeMeIn(0);
        await vi.waitFor(() => expect(events).toContain('reminder:wake'), { timeout: 3000 });
        // Both hosts tick fast; several windows must not double-fire.
        await new Promise((r) => setTimeout(r, 150));
        expect(events.filter((e) => e === 'reminder:wake')).toHaveLength(1);
    });

    it('every reminder shard has exactly one owner across the cluster (rendezvous)', async () => {
        const cluster = await createCluster(3, { actors: [counterActor()] });
        running = cluster;
        for (let i = 0; i < 16; i++) {
            const owners = cluster.placements.filter((pl) => pl.ownsReminderShard(`p${i}`));
            expect(owners).toHaveLength(1);
        }
        // Both survivors re-cover the dead host's shards deterministically.
        cluster.crash(0);
        for (let i = 0; i < 16; i++) {
            const owners = cluster.placements
                .slice(1)
                .filter((pl) => pl.ownsReminderShard(`p${i}`));
            expect(owners).toHaveLength(1);
        }
    });

    it('crash failover: the survivor evicts the dead claim and resumes from storage', async () => {
        const events: string[] = [];
        const cluster = await createCluster(2, {
            actors: [counterActor(events)],
            policy: selfPolicy
        });
        running = cluster;
        const def = counterActor();
        // Owner: host 0, with persisted state.
        await cluster.hosts[0]!.actor(def, 'phoenix').increment(41);

        cluster.crash(0);
        // Self-fence: the crashed host dropped its activations.
        await vi.waitFor(() => expect(cluster.hosts[0]!.stats().activations).toBe(0));

        // The survivor takes over: dead-owner entry evicted, fresh
        // activation from shared storage — state survives the crash.
        await expect(cluster.hosts[1]!.actor(def, 'phoenix').increment(1)).resolves.toBe(42);
        expect(events.filter((e) => e === 'activate:phoenix')).toHaveLength(2);
    });
});

describe('cluster: milestone 2 — failover & directory hygiene', () => {
    it('survivors proactively evict a dead host’s directory entries (no call needed)', async () => {
        const cluster = await createCluster(2, { actors: [counterActor()] });
        running = cluster;
        // A phantom third host joins, claims two actors, then crashes
        // WITHOUT any self-cleanup (a real dead process runs nothing).
        const phantom = cluster.hub.providers();
        await phantom.membership.join({
            hostId: 's.phantom',
            epoch: 1,
            address: 'http://phantom.test',
            status: 'active'
        });
        const key = (k: string) => ['Counter', k].join(String.fromCharCode(0));
        await cluster.hub.directory.claim(key('lost1'), {
            hostId: 's.phantom',
            activationId: 's.phantom/1/1'
        });
        await cluster.hub.directory.claim(key('lost2'), {
            hostId: 's.phantom',
            activationId: 's.phantom/1/2'
        });

        cluster.hub.kill('s.phantom');
        // Survivors observe the departure and sweep — entries disappear
        // without any dispatch touching those actors.
        await vi.waitFor(async () => {
            await expect(cluster.hub.directory.lookup(key('lost1'))).resolves.toBeNull();
            await expect(cluster.hub.directory.lookup(key('lost2'))).resolves.toBeNull();
        });
    });

    it('a host expired from the view without being told still self-fences (#45)', async () => {
        // The paused-loop double activation. Peers expire host 0 on the TTL
        // and sweep its claims; a survivor re-activates the same actor. Host
        // 0 itself never saw a failure — its heartbeat would simply resume
        // and succeed — so before #45 it kept its activations live and kept
        // accepting writes: two live activations of one actor.
        const events: string[] = [];
        const cluster = await createCluster(2, {
            actors: [counterActor(events)],
            policy: selfPolicy
        });
        running = cluster;
        const def = counterActor();
        await cluster.hosts[0]!.actor(def, 'ghost').increment(7);
        expect(cluster.hosts[0]!.stats().activations).toBe(1);

        // Expired, not killed: no onSelfSuspect, nothing failed anywhere.
        cluster.hub.expire(cluster.placements[0]!.identity.hostId);

        await vi.waitFor(() =>
            expect(cluster.placements[0]!.counters().status).toBe('fenced')
        );
        // It dropped what it held rather than serving a second copy…
        await vi.waitFor(() => expect(cluster.hosts[0]!.stats().activations).toBe(0));
        // …and refuses to activate again.
        await expect(cluster.hosts[0]!.actor(def, 'ghost').get()).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'activation'
        );
        // The survivor owns it now, resuming the persisted state.
        await expect(cluster.hosts[1]!.actor(def, 'ghost').increment(1)).resolves.toBe(8);
    });

    it('an empty view is solo, not lost membership — no fence', async () => {
        // `hosts.length === 0` already means "solo / not started" elsewhere
        // in placement. Fencing on it would turn a membership store failing
        // over to a cold replica into every host fencing at once — every pod
        // failing liveness, the whole cluster gone (#141).
        const hub = memoryClusterHub();
        const providers = hub.providers();
        const placement = clusterPlacement({
            membership: providers.membership,
            directory: providers.directory,
            advertise: 'http://self.test'
        });
        const host = createHost({ actors: [counterActor()], placement, defaults: quiet });
        await host.start();
        try {
            await host.actor(counterActor(), 'solo').increment(1);
            // Everyone vanishes, including us — a wiped store, not a fence.
            hub.expire(placement.identity.hostId);
            await new Promise((r) => setTimeout(r, 20));
            expect(placement.view().hosts).toHaveLength(0);
            expect(placement.counters().status).not.toBe('fenced');
            await expect(host.actor(counterActor(), 'solo').get()).resolves.toBe(1);
        } finally {
            await host.stop({ timeoutMs: 1000 });
        }
    });

    it('a stale view alone does not fence — the fresh refresh decides', async () => {
        // The absence check costs a pod its life, so a cached view that
        // merely lags must never be enough on its own.
        const hub = memoryClusterHub();
        const providers = hub.providers();
        let hideSelf = true;
        const membership: ClusterMembership = {
            ...providers.membership,
            // A view that lies once, and a store that tells the truth.
            view: () => {
                const real = providers.membership.view();
                if (!hideSelf) return real;
                return { ...real, hosts: real.hosts.filter((h) => h.hostId !== selfId) };
            },
            refresh: async () => {
                hideSelf = false;
                return providers.membership.refresh();
            }
        };
        const placement = clusterPlacement({
            membership,
            directory: providers.directory,
            advertise: 'http://self.test'
        });
        const selfId = placement.identity.hostId;
        const host = createHost({ actors: [counterActor()], placement, defaults: quiet });
        await host.start();
        try {
            await host.actor(counterActor(), 'stale').increment(3);
            // Any membership change re-runs the check against the lying view.
            const peer = hub.providers();
            await peer.membership.join({
                hostId: 's.peer',
                epoch: 1,
                address: 'http://peer.test',
                status: 'active'
            });
            await new Promise((r) => setTimeout(r, 20));
            expect(placement.counters().status).not.toBe('fenced');
            await expect(host.actor(counterActor(), 'stale').get()).resolves.toBe(3);
        } finally {
            await host.stop({ timeoutMs: 1000 });
        }
    });

    it('a transient view drop does not forget a host — the sweep retries on the next change', async () => {
        const hub = memoryClusterHub();
        const providers = hub.providers();
        // A membership whose store answer diverges from the view once:
        // the phantom is absent from the view but isAlive still says true
        // (e.g. a stale poll racing the heartbeat store).
        let lieOnce = true;
        const membership: ClusterMembership = {
            ...providers.membership,
            isAlive: async (id) => {
                if (id === 's.phantom' && lieOnce) {
                    lieOnce = false;
                    return true;
                }
                return providers.membership.isAlive(id);
            }
        };
        const placement = clusterPlacement({
            membership,
            directory: providers.directory,
            advertise: 'http://self.test'
        });
        const host = createHost({ actors: [counterActor()], placement, defaults: quiet });
        await host.start();
        try {
            const phantom = hub.providers();
            await phantom.membership.join({
                hostId: 's.phantom',
                epoch: 1,
                address: 'http://phantom.test',
                status: 'active'
            });
            const key = ['Counter', 'ghost'].join(String.fromCharCode(0));
            await hub.directory.claim(key, {
                hostId: 's.phantom',
                activationId: 's.phantom/1/1'
            });

            hub.kill('s.phantom');
            await new Promise((r) => setTimeout(r, 20));
            // First departure saw the transient "alive" answer: no sweep —
            // and crucially the phantom must NOT be forgotten.
            await expect(hub.directory.lookup(key)).resolves.not.toBeNull();

            // Any later membership change re-runs the diff; the store now
            // tells the truth and the sweep completes.
            const late = hub.providers();
            await late.membership.join({
                hostId: 's.late',
                epoch: 1,
                address: 'http://late.test',
                status: 'active'
            });
            await vi.waitFor(async () => {
                await expect(hub.directory.lookup(key)).resolves.toBeNull();
            });
        } finally {
            await host.stop({ timeoutMs: 1000 });
        }
    });

    it('unreachable-but-alive peers get backed-off bounded retries, then recovery after death', async () => {
        const urls: string[] = [];
        const cluster = await createCluster(2, {
            actors: [counterActor()],
            policy: selfPolicy,
            retries: 2,
            retryBackoffMs: 15,
            onRequest: (url) => urls.push(url)
        });
        running = cluster;
        const def = counterActor();
        // Owner: host 1, with saved state.
        await cluster.hosts[1]!.actor(def, 'flaky').increment(7);

        // Kill only the ADDRESS (network partition) — membership stays alive.
        cluster.unbind(1);
        urls.length = 0;
        const before = Date.now();
        await expect(cluster.hosts[0]!.actor(def, 'flaky').get()).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'activation'
        );
        // Bounded: retries+1 attempts at the dead address, spaced by backoff.
        expect(urls.filter((u) => u.includes('host1.test'))).toHaveLength(3);
        expect(Date.now() - before).toBeGreaterThanOrEqual(15 + 30);

        // The host then actually dies: survivors evict and re-place.
        cluster.hub.kill(cluster.placements[1]!.identity.hostId);
        await expect(cluster.hosts[0]!.actor(def, 'flaky').get()).resolves.toBe(7);
    });

    it('a mid-call owner crash makes chain re-entry a loud retryable error (migrated guard, for real)', async () => {
        let betaEntered!: () => void;
        const entered = new Promise<void>((r) => (betaEntered = r));
        let proceed!: () => void;
        const gate = new Promise<void>((r) => (proceed = r));

        type BetaClient = { poke(): Promise<unknown> };
        const alpha = defineActor({
            type: 'Alpha',
            allowAnonymous: true,
            reentrant: true,
            state: () => ({}),
            methods: (ctx: ActorContext<object>) => ({
                async poke() {
                    return (ctx.actor(beta, 'b') as BetaClient).poke();
                },
                async back() {
                    return 'alpha-back';
                },
                async warm() {
                    return 'warm';
                }
            })
        });
        const beta = defineActor({
            type: 'Beta',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx: ActorContext<object>) => ({
                async poke() {
                    betaEntered();
                    await gate; // the test crashes alpha's host here
                    try {
                        return await (
                            ctx.actor(alpha, 'a') as { back(): Promise<string> }
                        ).back();
                    } catch (error) {
                        return {
                            kind: isActorError(error) ? error.kind : 'other',
                            message: (error as Error).message
                        };
                    }
                },
                async warm() {
                    return 'warm';
                }
            })
        });

        const cluster = await createCluster(2, { actors: [alpha, beta], policy: selfPolicy });
        running = cluster;
        await cluster.hosts[0]!.actor(alpha, 'a').warm(); // Alpha/a on host 0
        await cluster.hosts[1]!.actor(beta, 'b').warm(); // Beta/b on host 1

        const call = cluster.hosts[0]!.actor(alpha, 'a').poke();
        await entered;
        // Alpha's host dies while its turn is up-stack awaiting beta. The
        // chain re-entering "alpha" now finds no activation anywhere it can
        // legally run inline — a second copy would break single-activation.
        cluster.crash(0);
        proceed();

        const outcome = (await call) as { kind: string; message: string };
        expect(outcome.kind).toBe('deadlock');
        expect(outcome.message).toMatch(/mid-turn|moved mid-call/);
    });
});

describe('cluster: milestone 4 — rebalancing & graceful handoff', () => {
    it('a rolling deploy hands actors off: zero failed calls, migrated reasons, state intact', async () => {
        const events: string[] = [];
        const cluster = await createCluster(2, {
            actors: [counterActor(events)],
            policy: selfPolicy,
            retryBackoffMs: 10
        });
        running = cluster;
        const def = counterActor();
        const keys = ['h1', 'h2', 'h3'];
        for (const key of keys) await cluster.hosts[0]!.actor(def, key).increment(1);

        // Stop host 0 while traffic keeps arriving through host 1.
        const stopping = cluster.hosts[0]!.stop({ timeoutMs: 2000 });
        const results = await Promise.all(
            keys.map((key) => cluster.hosts[1]!.actor(def, key).increment(1))
        );
        await stopping;

        expect(results.sort()).toEqual([2, 2, 2]); // every call succeeded, state intact
        for (const key of keys) {
            expect(events).toContain(`deactivate:${key}:migrated`);
            expect(events.filter((e) => e === `activate:${key}`)).toHaveLength(2);
        }
        expect(cluster.hosts[1]!.stats().activations).toBe(3);
    });

    it('the leaver announces `leaving` BEFORE the drain, so peers stop placing there', async () => {
        const slow = defineActor({
            type: 'Slow',
            allowAnonymous: true,
            state: () => ({}),
            methods: () => ({
                async nap(ms: number) {
                    await new Promise((r) => setTimeout(r, ms));
                    return 'rested';
                }
            })
        });
        const cluster = await createCluster(2, { actors: [slow], policy: selfPolicy });
        running = cluster;
        const viewer = cluster.hub.providers().membership; // shared hub view
        const leaverId = cluster.placements[0]!.identity.hostId;

        // A turn in flight holds the drain open…
        const napping = cluster.hosts[0]!.actor(slow, 'z').nap(150);
        await new Promise((r) => setTimeout(r, 20));
        const stopping = cluster.hosts[0]!.stop({ timeoutMs: 2000 });

        // …and while it drains, the view already says leaving — peers'
        // placement policies (which filter on 'active') skip it.
        await vi.waitFor(() => {
            const member = viewer.view().hosts.find((m) => m.hostId === leaverId);
            expect(member?.status).toBe('leaving');
        });
        await expect(napping).resolves.toBe('rested'); // in-flight turn completed
        await stopping;
        // Fully left after the drain.
        expect(viewer.view().hosts.find((m) => m.hostId === leaverId)).toBeUndefined();
    });

    it('consistentHashPolicy: all hosts agree on the target; keys spread across hosts', () => {
        const hosts: HostDescriptor[] = ['s.aaa', 's.bbb', 's.ccc'].map((hostId, i) => ({
            hostId,
            epoch: 1,
            address: `http://${i}.test`,
            status: 'active' as const
        }));
        const policy = consistentHashPolicy();
        const perHost = new Map<string, number>();
        for (let k = 0; k < 100; k++) {
            const ref = { type: 'Counter', key: `k${k}` };
            const targets = hosts.map(
                (self) => policy.choose(ref, { version: 1, hosts }, self).hostId
            );
            // Every host picks the SAME owner, whatever its own identity.
            expect(new Set(targets).size).toBe(1);
            perHost.set(targets[0]!, (perHost.get(targets[0]!) ?? 0) + 1);
        }
        // …and the keys spread over all three.
        expect(perHost.size).toBe(3);
        for (const count of perHost.values()) expect(count).toBeGreaterThan(10);
    });

    it('typePolicies pin selected types local while the default policy handles the rest', async () => {
        const sticky = defineActor({
            type: 'Sticky',
            allowAnonymous: true,
            state: () => ({}),
            methods: () => ({
                async ping() {
                    return 'pong';
                }
            })
        });
        const cluster = await createCluster(2, {
            actors: [sticky],
            typePolicies: { Sticky: preferLocalPolicy() }
        });
        running = cluster;
        await cluster.hosts[1]!.actor(sticky, 'a').ping();
        await cluster.hosts[1]!.actor(sticky, 'b').ping();
        expect(cluster.hosts[1]!.stats().perType['Sticky']).toBe(2);
        expect(cluster.hosts[0]!.stats().activations).toBe(0);
    });

    it('defineActor({ placement }) routes the type and beats typePolicies', async () => {
        // The placement attribute form: the strategy rides the actor.
        const pinned = defineActor({
            type: 'Pinned',
            allowAnonymous: true,
            placement: preferLocalPolicy(),
            state: () => ({}),
            methods: () => ({
                async ping() {
                    return 'pong';
                }
            })
        });
        const cluster = await createCluster(2, {
            actors: [pinned],
            // Both of these would send it to host 0; the declaration wins.
            policy: { name: 'pin-zero', choose: (_r, view) => view.hosts[0]! },
            typePolicies: { Pinned: { name: 'pin-zero', choose: (_r, view) => view.hosts[0]! } }
        });
        running = cluster;

        // Called through host 1 — prefer-local keeps it there.
        await cluster.hosts[1]!.actor(pinned, 'a').ping();
        expect(cluster.hosts[1]!.stats().perType['Pinned']).toBe(1);
        expect(cluster.hosts[0]!.stats().activations).toBe(0);
    });

    it('ignores a strategy TAGGED for another backend, silently', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        // A legitimately foreign strategy: it names its backend, so the
        // cluster placement knows it is not being asked to use it and falls
        // back to the configured policy. This is the case the opacity of
        // `ActorPlacementStrategy` exists to allow, so it must be SILENT —
        // warning about a correct declaration is noise the author cannot act
        // on.
        const odd = defineActor({
            type: 'Odd',
            allowAnonymous: true,
            placement: { name: 'durable-object', backend: 'durable-objects' },
            state: () => ({}),
            methods: () => ({
                async ping() {
                    return 'pong';
                }
            })
        });
        const cluster = await createCluster(2, { actors: [odd], policy: selfPolicy });
        running = cluster;
        try {
            await expect(cluster.hosts[1]!.actor(odd, 'a').ping()).resolves.toBe('pong');
            // Narrowed to placement warnings: the multi-host harness emits
            // its own unrelated "second host started" notice.
            const placementWarnings = warn.mock.calls
                .map((c) => String(c[0]))
                .filter((line) => line.includes('placement') || line.includes('backend'));
            expect(placementWarnings).toEqual([]);
        } finally {
            warn.mockRestore();
        }
    });

    it('will not run a foreign strategy even if it HAS a choose()', async () => {
        // The tag decides, not the shape. A strategy written against another
        // backend's view of the world must not be executed here just because
        // it happens to expose the right method name — that would be worse
        // than ignoring it.
        let called = false;
        const foreign = defineActor({
            type: 'Foreign',
            allowAnonymous: true,
            placement: {
                name: 'do-pin',
                backend: 'durable-objects',
                choose: (_r: unknown, _v: unknown, self: unknown) => {
                    called = true;
                    return self;
                }
            } as never,
            state: () => ({}),
            methods: () => ({
                async ping() {
                    return 'pong';
                }
            })
        });
        const cluster = await createCluster(2, { actors: [foreign], policy: selfPolicy });
        running = cluster;
        await expect(cluster.hosts[1]!.actor(foreign, 'a').ping()).resolves.toBe('pong');
        expect(called).toBe(false);
    });

    it('REFUSES an untagged strategy it cannot use, rather than misplacing the actor', async () => {
        // The bug this replaces: with no backend tag and no choose(), the
        // runtime could not tell "someone else's" from "broken", so it
        // ignored both with a dev-only warning — and in production the actor
        // was quietly placed somewhere other than where the author declared,
        // with nothing pointing at the cause.
        const broken = defineActor({
            type: 'Broken',
            allowAnonymous: true,
            placement: { name: 'my-strategy' }, // no choose(), no backend
            state: () => ({}),
            methods: () => ({
                async ping() {
                    return 'pong';
                }
            })
        });
        const cluster = await createCluster(2, { actors: [broken], policy: selfPolicy });
        running = cluster;
        await expect(cluster.hosts[1]!.actor(broken, 'a').ping()).rejects.toThrow(
            /not a usable cluster PlacementPolicy/
        );
    });

    it('migrate() releases the claim and the next call re-places with state intact', async () => {
        const events: string[] = [];
        const cluster = await createCluster(2, {
            actors: [counterActor(events)],
            policy: selfPolicy
        });
        running = cluster;
        const def = counterActor();
        const ref = { type: 'Counter', key: 'mover' };
        await cluster.hosts[0]!.actor(def, 'mover').increment(5);

        await cluster.placements[0]!.migrate(ref);
        expect(events).toContain('deactivate:mover:migrated');
        expect(cluster.hosts[0]!.stats().activations).toBe(0);
        const key = ['Counter', 'mover'].join(String.fromCharCode(0));
        await expect(cluster.hub.directory.lookup(key)).resolves.toBeNull();

        // Re-placed where the next call lands (selfPolicy → host 1).
        await expect(cluster.hosts[1]!.actor(def, 'mover').increment(1)).resolves.toBe(6);
        expect(cluster.hosts[1]!.stats().perType['Counter']).toBe(1);

        // migrate() on a non-owner is a harmless no-op.
        await cluster.placements[0]!.migrate(ref);
        await expect(cluster.hosts[1]!.actor(def, 'mover').get()).resolves.toBe(6);
    });
});

describe('cluster: milestone 5 — per-request HMAC auth', () => {
    it('accepts a correctly signed request; rejects tampered and wrong-call signatures', async () => {
        const cluster = await createCluster(1, { actors: [counterActor()] });
        running = cluster;
        const send = (auth: string) =>
            cluster.fetch('http://host0.test/_sigx/host/Counter%23get', {
                method: 'POST',
                headers: {
                    'content-type': 'application/json',
                    'x-sigx-cluster-auth': auth,
                    [HOST_CALL_HEADER]: encodeEnvelope({ callChain: [], callId: 'c.t' }, 's.x')
                },
                body: JSON.stringify({ args: ['k'] })
            });

        const good = await signAuth('test-secret', 'Counter#get', 'c.t');
        expect((await send(good)).status).toBe(200);

        const flipped = good.endsWith('0') ? `${good.slice(0, -1)}1` : `${good.slice(0, -1)}0`;
        expect((await send(flipped)).status).toBe(403);

        // A signature captured from a DIFFERENT call must not authorize this one.
        const otherSymbol = await signAuth('test-secret', 'Counter#increment', 'c.t');
        expect((await send(otherSymbol)).status).toBe(403);
        const otherCallId = await signAuth('test-secret', 'Counter#get', 'c.other');
        expect((await send(otherCallId)).status).toBe(403);
        expect((await send('test-secret')).status).toBe(403); // old bearer format is dead
    });

    it('rejects malformed header shapes and undecodable request paths without crashing', async () => {
        const good = await signAuth('s3cret', 'Cart#add', 'c.1');
        // Trailing junk, non-hex signatures, and wrong part counts all fail.
        await expect(verifyAuth('s3cret', `${good}.junk`, 'Cart#add', 'c.1')).resolves.toBe(false);
        await expect(
            verifyAuth('s3cret', 'v1.123.not-hex-at-all', 'Cart#add', 'c.1')
        ).resolves.toBe(false);
        await expect(verifyAuth('s3cret', 'v1.123', 'Cart#add', 'c.1')).resolves.toBe(false);

        // Malformed percent-encoding in the path: 403, never a crash.
        const cluster = await createCluster(1, { actors: [counterActor()] });
        running = cluster;
        const res = await cluster.fetch('http://host0.test/_sigx/host/%E0%A4%A', {
            method: 'POST',
            headers: {
                'content-type': 'application/json',
                'x-sigx-cluster-auth': good,
                [HOST_CALL_HEADER]: encodeEnvelope({ callChain: [], callId: 'c.1' }, 's.x')
            },
            body: JSON.stringify({ args: ['k'] })
        });
        expect(res.status).toBe(403);
    });

    it('signatures expire: outside the freshness window verification fails', async () => {
        const header = await signAuth('s3cret', 'Cart#add', 'c.1');
        await expect(verifyAuth('s3cret', header, 'Cart#add', 'c.1')).resolves.toBe(true);
        vi.useFakeTimers();
        try {
            vi.setSystemTime(Date.now() + 6 * 60_000); // past the 5-minute window
            await expect(verifyAuth('s3cret', header, 'Cart#add', 'c.1')).resolves.toBe(false);
        } finally {
            vi.useRealTimers();
        }
    });
});
