/**
 * The clustering integration suite: N real silos in one process, wired
 * through the in-memory pipe (real public + internal endpoints, zero
 * sockets). Pins the M1 invariants: single activation across silos,
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
    SILO_CALL_HEADER,
    type ClusterMembership,
    type SiloDescriptor
} from '@sigx/actors/cluster';
import { createSilo } from '@sigx/actors/silo';
import { __actorRef, configureActors } from '@sigx/actors/client';
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
        unguarded: true,
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
    it('one activation per key across silos — racing front doors join one owner', async () => {
        const events: string[] = [];
        const cluster = await createCluster(2, { actors: [counterActor(events)] });
        running = cluster;
        const def = counterActor();
        const [a, b] = await Promise.all([
            cluster.silos[0]!.actor(def, 'shared').increment(1),
            cluster.silos[1]!.actor(def, 'shared').increment(1)
        ]);
        expect([a, b].sort()).toEqual([1, 2]);
        expect(events.filter((e) => e === 'activate:shared')).toHaveLength(1);
        // Both silos read the same state wherever it lives.
        await expect(cluster.silos[0]!.actor(def, 'shared').get()).resolves.toBe(2);
        await expect(cluster.silos[1]!.actor(def, 'shared').get()).resolves.toBe(2);
    });

    it('a misdirected internal call answers 421 wrong-host and the caller converges', async () => {
        const events: string[] = [];
        const cluster = await createCluster(3, {
            actors: [counterActor(events)],
            policy: selfPolicy
        });
        running = cluster;
        const def = counterActor();
        // Owner: silo 2 (selfPolicy pins new activations locally).
        await cluster.silos[2]!.actor(def, 'k').increment(5);

        // Misdirect: hit silo 1's INTERNAL endpoint for silo 2's actor.
        const misdirected = await cluster.fetch('http://silo1.test/_sigx/silo/Counter%23get', {
            method: 'POST',
            headers: {
                'content-type': 'application/json',
                'x-sigx-cluster-auth': 'test-secret',
                [SILO_CALL_HEADER]: encodeEnvelope({ callChain: [], callId: 'c.t' }, 's.test')
            },
            body: JSON.stringify({ args: ['k'] })
        });
        expect(misdirected.status).toBe(421);
        const parsed = (await misdirected.json()) as {
            error: { data: { kind: string; owner: { siloId: string } } };
        };
        expect(parsed.error.data.kind).toBe('wrong-host');
        expect(parsed.error.data.owner.siloId).toBe(cluster.placements[2]!.identity.siloId);

        // Routing-level convergence: silo 0 has no claim and no cache; the
        // directory sends it straight to the owner.
        await expect(cluster.silos[0]!.actor(def, 'k').get()).resolves.toBe(5);
        expect(events.filter((e) => e === 'activate:k')).toHaveLength(1);
    });

    it('the internal mount rejects a bad cluster secret', async () => {
        const cluster = await createCluster(1, { actors: [counterActor()] });
        running = cluster;
        const res = await cluster.fetch('http://silo0.test/_sigx/silo/Counter%23get', {
            method: 'POST',
            headers: {
                'content-type': 'application/json',
                'x-sigx-cluster-auth': 'wrong',
                [SILO_CALL_HEADER]: encodeEnvelope({ callChain: [], callId: 'c.t' }, 's.test')
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
            unguarded: true,
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
            unguarded: true,
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

    it('a cross-silo cycle into a non-reentrant actor throws deadlock with the full chain', async () => {
        const { alpha, beta } = pingPong(false);
        const cluster = await createCluster(2, { actors: [alpha, beta], policy: selfPolicy });
        running = cluster;
        // Alpha/a lives on silo 0, Beta/b on silo 1.
        await cluster.silos[0]!.actor(alpha, 'a').warm();
        await cluster.silos[1]!.actor(beta, 'b').warm();

        await expect(cluster.silos[0]!.actor(alpha, 'a').poke()).rejects.toSatisfy(
            (e: unknown) =>
                isActorError(e) &&
                e.kind === 'deadlock' &&
                /Alpha/.test((e as Error).message) &&
                /Beta/.test((e as Error).message)
        );
    });

    it('a cross-silo cycle into a REENTRANT actor runs inline and succeeds', async () => {
        const { alpha, beta } = pingPong(true);
        const cluster = await createCluster(2, { actors: [alpha, beta], policy: selfPolicy });
        running = cluster;
        await cluster.silos[0]!.actor(alpha, 'a').warm();
        await cluster.silos[1]!.actor(beta, 'b').warm();
        await expect(cluster.silos[0]!.actor(alpha, 'a').poke()).resolves.toBe('alpha-back');
    });

    it('the caller deadline crosses the hop as remaining-ms and times out remotely', async () => {
        const cluster = await createCluster(2, {
            actors: [counterActor()],
            policy: selfPolicy,
            defaults: { callTimeoutMs: 60 }
        });
        running = cluster;
        const def = counterActor();
        // Owner: silo 1. Call from silo 0 → one hop.
        await cluster.silos[1]!.actor(def, 'slowpoke').get();
        await expect(cluster.silos[0]!.actor(def, 'slowpoke').slow(500)).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'call-timeout'
        );
    });
});

describe('cluster: guards, streams, reminders, failover', () => {
    it('guards run exactly once: at the public edge, not on the internal hop', async () => {
        const guardLog: string[] = [];
        const guarded = defineActor({
            type: 'Guarded',
            use: [
                (_rq, info) => {
                    guardLog.push(`use:${info.symbol}`);
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
        // Owner: silo 1. Public wire call lands on silo 0 → internal hop.
        await cluster.silos[1]!.actor(guarded, 'g').hello();
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
            unguarded: true,
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
        // Owner: silo 1; consume from silo 0 across the internal hop.
        await cluster.silos[1]!.actor(streamer, 's').warm();

        const finite: number[] = [];
        for await (const chunk of cluster.silos[0]!.actor(streamer, 's').countTo(3)) {
            finite.push(chunk as number);
        }
        expect(finite).toEqual([1, 2, 3]);

        for await (const chunk of cluster.silos[0]!.actor(streamer, 's').forever()) {
            if ((chunk as number) >= 2) break;
        }
        await vi.waitFor(() => expect(cleanupLog).toContain('forever:finally'));
        // Keep-alive released on the owner: the activation can deactivate.
        await cluster.silos[1]!.deactivateType('Streamer');
        expect(cluster.silos[1]!.stats().activations).toBe(0);
    });

    it('reminders fire exactly once across silos (rendezvous shard ownership)', async () => {
        const events: string[] = [];
        const waking = defineActor({
            type: 'Waking',
            unguarded: true,
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
        // Register through silo 1 (a non-leader mutation is fine).
        await cluster.silos[1]!.actor(waking, 'r1').wakeMeIn(0);
        await vi.waitFor(() => expect(events).toContain('reminder:wake'), { timeout: 3000 });
        // Both silos tick fast; several windows must not double-fire.
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
        // Both survivors re-cover the dead silo's shards deterministically.
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
        // Owner: silo 0, with persisted state.
        await cluster.silos[0]!.actor(def, 'phoenix').increment(41);

        cluster.crash(0);
        // Self-fence: the crashed silo dropped its activations.
        await vi.waitFor(() => expect(cluster.silos[0]!.stats().activations).toBe(0));

        // The survivor takes over: dead-owner entry evicted, fresh
        // activation from shared storage — state survives the crash.
        await expect(cluster.silos[1]!.actor(def, 'phoenix').increment(1)).resolves.toBe(42);
        expect(events.filter((e) => e === 'activate:phoenix')).toHaveLength(2);
    });
});

describe('cluster: milestone 2 — failover & directory hygiene', () => {
    it('survivors proactively evict a dead silo’s directory entries (no call needed)', async () => {
        const cluster = await createCluster(2, { actors: [counterActor()] });
        running = cluster;
        // A phantom third silo joins, claims two actors, then crashes
        // WITHOUT any self-cleanup (a real dead process runs nothing).
        const phantom = cluster.hub.providers();
        await phantom.membership.join({
            siloId: 's.phantom',
            epoch: 1,
            address: 'http://phantom.test',
            status: 'active'
        });
        const key = (k: string) => ['Counter', k].join(String.fromCharCode(0));
        await cluster.hub.directory.claim(key('lost1'), {
            siloId: 's.phantom',
            activationId: 's.phantom/1/1'
        });
        await cluster.hub.directory.claim(key('lost2'), {
            siloId: 's.phantom',
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

    it('a transient view drop does not forget a silo — the sweep retries on the next change', async () => {
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
        const silo = createSilo({ actors: [counterActor()], placement, defaults: quiet });
        await silo.start();
        try {
            const phantom = hub.providers();
            await phantom.membership.join({
                siloId: 's.phantom',
                epoch: 1,
                address: 'http://phantom.test',
                status: 'active'
            });
            const key = ['Counter', 'ghost'].join(String.fromCharCode(0));
            await hub.directory.claim(key, {
                siloId: 's.phantom',
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
                siloId: 's.late',
                epoch: 1,
                address: 'http://late.test',
                status: 'active'
            });
            await vi.waitFor(async () => {
                await expect(hub.directory.lookup(key)).resolves.toBeNull();
            });
        } finally {
            await silo.stop({ timeoutMs: 1000 });
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
        // Owner: silo 1, with saved state.
        await cluster.silos[1]!.actor(def, 'flaky').increment(7);

        // Kill only the ADDRESS (network partition) — membership stays alive.
        cluster.unbind(1);
        urls.length = 0;
        const before = Date.now();
        await expect(cluster.silos[0]!.actor(def, 'flaky').get()).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'activation'
        );
        // Bounded: retries+1 attempts at the dead address, spaced by backoff.
        expect(urls.filter((u) => u.includes('silo1.test'))).toHaveLength(3);
        expect(Date.now() - before).toBeGreaterThanOrEqual(15 + 30);

        // The silo then actually dies: survivors evict and re-place.
        cluster.hub.kill(cluster.placements[1]!.identity.siloId);
        await expect(cluster.silos[0]!.actor(def, 'flaky').get()).resolves.toBe(7);
    });

    it('a mid-call owner crash makes chain re-entry a loud retryable error (migrated guard, for real)', async () => {
        let betaEntered!: () => void;
        const entered = new Promise<void>((r) => (betaEntered = r));
        let proceed!: () => void;
        const gate = new Promise<void>((r) => (proceed = r));

        type BetaClient = { poke(): Promise<unknown> };
        const alpha = defineActor({
            type: 'Alpha',
            unguarded: true,
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
            unguarded: true,
            state: () => ({}),
            methods: (ctx: ActorContext<object>) => ({
                async poke() {
                    betaEntered();
                    await gate; // the test crashes alpha's silo here
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
        await cluster.silos[0]!.actor(alpha, 'a').warm(); // Alpha/a on silo 0
        await cluster.silos[1]!.actor(beta, 'b').warm(); // Beta/b on silo 1

        const call = cluster.silos[0]!.actor(alpha, 'a').poke();
        await entered;
        // Alpha's silo dies while its turn is up-stack awaiting beta. The
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
        for (const key of keys) await cluster.silos[0]!.actor(def, key).increment(1);

        // Stop silo 0 while traffic keeps arriving through silo 1.
        const stopping = cluster.silos[0]!.stop({ timeoutMs: 2000 });
        const results = await Promise.all(
            keys.map((key) => cluster.silos[1]!.actor(def, key).increment(1))
        );
        await stopping;

        expect(results.sort()).toEqual([2, 2, 2]); // every call succeeded, state intact
        for (const key of keys) {
            expect(events).toContain(`deactivate:${key}:migrated`);
            expect(events.filter((e) => e === `activate:${key}`)).toHaveLength(2);
        }
        expect(cluster.silos[1]!.stats().activations).toBe(3);
    });

    it('the leaver announces `leaving` BEFORE the drain, so peers stop placing there', async () => {
        const slow = defineActor({
            type: 'Slow',
            unguarded: true,
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
        const leaverId = cluster.placements[0]!.identity.siloId;

        // A turn in flight holds the drain open…
        const napping = cluster.silos[0]!.actor(slow, 'z').nap(150);
        await new Promise((r) => setTimeout(r, 20));
        const stopping = cluster.silos[0]!.stop({ timeoutMs: 2000 });

        // …and while it drains, the view already says leaving — peers'
        // placement policies (which filter on 'active') skip it.
        await vi.waitFor(() => {
            const member = viewer.view().silos.find((m) => m.siloId === leaverId);
            expect(member?.status).toBe('leaving');
        });
        await expect(napping).resolves.toBe('rested'); // in-flight turn completed
        await stopping;
        // Fully left after the drain.
        expect(viewer.view().silos.find((m) => m.siloId === leaverId)).toBeUndefined();
    });

    it('consistentHashPolicy: all silos agree on the target; keys spread across silos', () => {
        const silos: SiloDescriptor[] = ['s.aaa', 's.bbb', 's.ccc'].map((siloId, i) => ({
            siloId,
            epoch: 1,
            address: `http://${i}.test`,
            status: 'active' as const
        }));
        const policy = consistentHashPolicy();
        const perSilo = new Map<string, number>();
        for (let k = 0; k < 100; k++) {
            const ref = { type: 'Counter', key: `k${k}` };
            const targets = silos.map(
                (self) => policy.choose(ref, { version: 1, silos }, self).siloId
            );
            // Every silo picks the SAME owner, whatever its own identity.
            expect(new Set(targets).size).toBe(1);
            perSilo.set(targets[0]!, (perSilo.get(targets[0]!) ?? 0) + 1);
        }
        // …and the keys spread over all three.
        expect(perSilo.size).toBe(3);
        for (const count of perSilo.values()) expect(count).toBeGreaterThan(10);
    });

    it('typePolicies pin selected types local while the default policy handles the rest', async () => {
        const sticky = defineActor({
            type: 'Sticky',
            unguarded: true,
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
        await cluster.silos[1]!.actor(sticky, 'a').ping();
        await cluster.silos[1]!.actor(sticky, 'b').ping();
        expect(cluster.silos[1]!.stats().perType['Sticky']).toBe(2);
        expect(cluster.silos[0]!.stats().activations).toBe(0);
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
        await cluster.silos[0]!.actor(def, 'mover').increment(5);

        await cluster.placements[0]!.migrate(ref);
        expect(events).toContain('deactivate:mover:migrated');
        expect(cluster.silos[0]!.stats().activations).toBe(0);
        const key = ['Counter', 'mover'].join(String.fromCharCode(0));
        await expect(cluster.hub.directory.lookup(key)).resolves.toBeNull();

        // Re-placed where the next call lands (selfPolicy → silo 1).
        await expect(cluster.silos[1]!.actor(def, 'mover').increment(1)).resolves.toBe(6);
        expect(cluster.silos[1]!.stats().perType['Counter']).toBe(1);

        // migrate() on a non-owner is a harmless no-op.
        await cluster.placements[0]!.migrate(ref);
        await expect(cluster.silos[1]!.actor(def, 'mover').get()).resolves.toBe(6);
    });
});
