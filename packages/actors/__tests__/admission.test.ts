/**
 * Admission control (#384): a turn queue can refuse, and an expired call
 * is skipped rather than run.
 *
 * Before this the only bound on a queue was the deadline — a host under
 * offered load past its capacity held every call for `callTimeoutMs` and
 * then failed all of them after doing the work ("drowned", #302 and the
 * `wf-local/drown-vs-shed` row of BASELINES.md). Under test:
 *
 *  - `defineActor({ maxQueued })` and `HostDefaults.maxQueuedPerActor`: a
 *    call that would push an activation's queue past the cap is refused
 *    in microseconds with `ActorOverloadedError` (`scope: 'actor'`).
 *  - `HostDefaults.maxInflightTurns`: the same at host level (`'host'`).
 *  - The refusal is PRE-acceptance: a one-way call rejects instead of
 *    resolving `undefined`; a reminder delivery counts as undelivered and
 *    is re-armed; a topic delivery lands in `failures[]`.
 *  - System turns — watch reads, the write-behind flush, a conflict
 *    reload — are never capped.
 *  - Drop-on-dequeue: a queued call whose deadline passed does not run its
 *    body; its caller already holds the timeout.
 *  - Across hosts the kind survives the wire, is never re-placed by the
 *    routing loop, and is counted (`overloadedReplies`).
 *  - `HostStats.overloadRefusals` counts what the host refused, and
 *    `reminderShardEntriesMax` says how large the sharded reminder table
 *    has grown (the "outgrown" gauge, #396).
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ActorCallTimeoutError, defineActor, isActorError, topic, type ActorContext } from '@sigx/actors';
import { fromHostWireError, toHostWireError } from '../src/cluster/wire-errors';
import { toClientError } from '../src/server/client-error';
import type { ActorOverloadedError } from '@sigx/actors';
import { createHost, memoryStorage, type Host } from '@sigx/actors/host';
import { emptyHostStats } from '../src/types';
import type { PlacementPolicy } from '@sigx/actors/cluster';
import { handleActorRequest } from '@sigx/actors/server';
import { __actorRef, configureActors } from '@sigx/actors/client';
import { createCluster, quiet, type ClusterHarness } from './harness';

const tick = () => new Promise<void>((r) => setTimeout(r, 0));

/** An actor whose turns block until released, so a queue can be built. */
function gated(type: string, extra: { maxQueued?: number; retryQueuedOnConflict?: true } = {}) {
    const gates: Array<() => void> = [];
    const ran: string[] = [];
    let opened = false;
    const def = defineActor({
        type,
        allowAnonymous: true,
        ...extra,
        state: () => ({ n: 0 }),
        methods: (ctx: ActorContext<{ n: number }>) => ({
            async hold(label: string) {
                ran.push(label);
                if (!opened) await new Promise<void>((r) => gates.push(r));
                ctx.state.n++;
                return label;
            },
            peek() {
                return ctx.state.n;
            }
        })
    });
    return {
        def,
        ran,
        release: () => {
            const g = gates.shift();
            g?.();
        },
        /** Release every held turn, and let every later one through. */
        releaseAll: () => {
            opened = true;
            while (gates.length > 0) gates.shift()!();
        }
    };
}

const overloaded = (error: unknown): error is ActorOverloadedError =>
    isActorError(error) && error.kind === 'overloaded';

let running: Host | null = null;
let cluster: ClusterHarness | null = null;
afterEach(async () => {
    await running?.stop();
    running = null;
    await cluster?.stop();
    cluster = null;
});

describe('per-actor queue cap', () => {
    it('refuses the call that would exceed maxQueued, in microseconds, with the depth and the limit', async () => {
        const { def, release, releaseAll } = gated('Capped', { maxQueued: 2 });
        const host = createHost({ actors: [def], defaults: quiet });
        running = host;
        await host.start();
        const a = host.actor(def, 'k');
        const first = a.hold('a'); // running
        await tick();
        const second = a.hold('b'); // queued — depth 2
        const third = a.hold('c'); // refused
        const settled = await Promise.allSettled([third]);
        expect(settled[0]!.status).toBe('rejected');
        const error = (settled[0] as PromiseRejectedResult).reason;
        expect(overloaded(error)).toBe(true);
        expect(error).toMatchObject({ scope: 'actor', depth: 2, limit: 2 });
        expect(error.message).toMatch(/maxQueued/);
        expect(host.stats().overloadRefusals).toBe(1);
        expect(emptyHostStats().overloadRefusals).toBe(0);
        // The accepted calls are untouched by the refusal.
        release();
        await expect(first).resolves.toBe('a');
        await tick();
        release();
        await expect(second).resolves.toBe('b');
        releaseAll();
        expect(host.stats().queued).toBe(0);
    });

    it('takes the host default when the definition names none, and the definition wins (0 = unlimited)', async () => {
        const capped = gated('Inherits');
        const open = gated('Unlimited', { maxQueued: 0 });
        const host = createHost({
            actors: [capped.def, open.def],
            defaults: { ...quiet, maxQueuedPerActor: 1 }
        });
        running = host;
        await host.start();
        const c = host.actor(capped.def, 'k');
        const first = c.hold('a');
        await tick();
        await expect(c.hold('b')).rejects.toSatisfy(overloaded);
        const o = host.actor(open.def, 'k');
        const calls = [o.hold('1'), o.hold('2'), o.hold('3'), o.hold('4')];
        await tick();
        capped.releaseAll();
        open.releaseAll();
        await first;
        await expect(Promise.all(calls)).resolves.toEqual(['1', '2', '3', '4']);
        expect(host.stats().overloadRefusals).toBe(1);
    });

    it('refuses a one-way call BEFORE acceptance: the caller rejects instead of resolving undefined', async () => {
        const { def, releaseAll } = gated('OneWayCapped', { maxQueued: 1 });
        const host = createHost({ actors: [def], defaults: quiet });
        running = host;
        await host.start();
        const a = host.actor(def, 'k');
        const first = a.hold('a');
        await tick();
        await expect(a.with({ oneWay: true }).hold('b')).rejects.toSatisfy(overloaded);
        releaseAll();
        await first;
        // Nothing was queued behind the refusal.
        expect(host.stats().queued).toBe(0);
    });

    it('does not cap watch reads (a system turn) and does not disturb retryQueuedOnConflict', async () => {
        const { def, release, releaseAll } = gated('Watched', {
            maxQueued: 1,
            retryQueuedOnConflict: true
        });
        const host = createHost({ actors: [def], defaults: quiet });
        running = host;
        await host.start();
        const a = host.actor(def, 'k');
        const first = a.hold('a');
        await tick();
        // The queue is at its cap; a watch's first read must still open.
        const abort = new AbortController();
        const iterator = host
            .dispatchWatch!(
                { type: def.type, key: 'k' },
                'peek',
                [],
                { callChain: [], callId: 'w1', abortSignal: abort.signal },
                { throttleMs: 0 }
            )
            [Symbol.asyncIterator]();
        const firstRead = iterator.next();
        await tick();
        release();
        await first;
        expect((await firstRead).value).toBe(1);
        abort.abort();
        await iterator.return?.(undefined);
        releaseAll();
        expect(host.stats().overloadRefusals).toBe(0);
    });
});

describe('host in-flight cap', () => {
    it('refuses with scope "host" once the host has maxInflightTurns turns queued or running, across actors', async () => {
        const one = gated('HostCapA');
        const two = gated('HostCapB');
        const host = createHost({
            actors: [one.def, two.def],
            defaults: { ...quiet, maxInflightTurns: 2 }
        });
        running = host;
        await host.start();
        const a = host.actor(one.def, 'k').hold('a');
        const b = host.actor(two.def, 'k').hold('b');
        await tick();
        const error = await host
            .actor(one.def, 'other')
            .hold('c')
            .then(
                () => null,
                (e: unknown) => e
            );
        expect(overloaded(error)).toBe(true);
        expect(error).toMatchObject({ scope: 'host', depth: 2, limit: 2 });
        expect(host.stats().overloadRefusals).toBe(1);
        one.releaseAll();
        two.releaseAll();
        await Promise.all([a, b]);
        // Settled turns give their slot back.
        await expect(host.actor(one.def, 'other').peek()).resolves.toBe(0);
    });

    it('counts the host’s own turns — a timer tick holding the loop refuses new calls, and is never refused itself', async () => {
        const gates: Array<() => void> = [];
        const ticks: number[] = [];
        const Ticker = defineActor({
            type: 'TickerHost',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({
                arm() {
                    ctx.timer('t', async () => {
                        ticks.push(Date.now());
                        await new Promise<void>((r) => gates.push(r));
                    }, { due: 1 });
                }
            })
        });
        const other = gated('BesideTicker');
        const host = createHost({
            actors: [Ticker, other.def],
            defaults: { ...quiet, maxInflightTurns: 1 }
        });
        running = host;
        await host.start();
        await host.actor(Ticker, 'k').arm();
        await vi.waitFor(() => expect(ticks).toHaveLength(1));
        // The tick is the one turn in flight; it was never subject to the
        // cap, and it fills the host for everyone else.
        const error = await host
            .actor(other.def, 'k')
            .hold('a')
            .then(
                () => null,
                (e: unknown) => e
            );
        expect(error).toMatchObject({ kind: 'overloaded', scope: 'host', depth: 1, limit: 1 });
        while (gates.length > 0) gates.shift()!();
        await vi.waitFor(() => expect(host.stats().queued).toBe(0));
        other.releaseAll();
        await expect(host.actor(other.def, 'k').hold('b')).resolves.toBe('b');
    });
});

describe('admission is at the entrance (#408)', () => {
    const selfPolicy: PlacementPolicy = { choose: (_ref, _view, self) => self };

    /**
     * The rule the cluster taught: a cap sheds what ARRIVES, never what is
     * already in flight. On a real fleet a host-wide cap that refused a
     * run's own worker-pool calls halved throughput at the knee and
     * stranded runs the uncapped fleet finished — refusing a chained call
     * is not shedding, it is destroying part-done work and failing the
     * turn that owns it.
     */
    it('never refuses a call made from inside a turn, however full the host is', async () => {
        const pool = gated('PoolBehindWork');
        const Caller = defineActor({
            type: 'CallerAtEntrance',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({
                // A turn that reaches for another actor, exactly as a
                // workflow run reaches for its compute pool.
                async work() {
                    return ctx.actor(pool.def, 'p').hold('inner');
                }
            })
        });
        const host = createHost({
            actors: [Caller, pool.def],
            defaults: { ...quiet, maxInflightTurns: 1 }
        });
        running = host;
        await host.start();

        // One turn in flight fills the host: the Caller's own turn.
        const outer = host.actor(Caller, 'k').work();
        await vi.waitFor(() => expect(host.stats().queued).toBeGreaterThanOrEqual(1));

        // A NEW arrival is refused — the cap still works.
        const refused = await host.actor(Caller, 'other').work().then(
            () => null,
            (e: unknown) => e
        );
        expect(refused).toMatchObject({ kind: 'overloaded', scope: 'host' });

        // …while the inner call, which the host already accepted work for,
        // goes through and the outer turn completes. Before #408 this
        // rejected and took its caller down with it.
        pool.releaseAll();
        await expect(outer).resolves.toBe('inner');
    });

    it('exempts a chained call that crossed a wire — the envelope carries the chain, so a hop is not an arrival', async () => {
        // The failure this guards: if a cross-host hop arrived with an
        // empty chain it would read as new work and be refused, which is
        // exactly what halved throughput on the cluster — most of a
        // workflow run's calls cross hosts.
        const pool = gated('PoolOnPeer');
        const Caller = defineActor({
            type: 'CallerNearby',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({
                async work() {
                    return ctx.actor(pool.def, 'p').hold('inner');
                }
            })
        });
        cluster = await createCluster(2, {
            actors: [pool.def, Caller],
            defaults: { ...quiet, maxInflightTurns: 1 },
            // Both self-placing: the pool is claimed by whichever host
            // touches it first, which below is deliberately the peer. A
            // peer-seeking policy would place it differently depending on
            // who asked, and the directory winner would be a race.
            typePolicies: { PoolOnPeer: selfPolicy, CallerNearby: selfPolicy }
        });
        const [near, peer] = [cluster.hosts[0]!, cluster.hosts[1]!];

        // Fill the PEER — the pool's owner — with one arrival of its own.
        const first = peer.dispatch({ type: pool.def.type, key: 'p' }, 'hold', ['first'], {
            callChain: [],
            callId: 'first'
        });
        await vi.waitFor(() => expect(peer.stats().queued).toBeGreaterThanOrEqual(1));

        // A fresh arrival at the full peer is refused…
        const arrival = await peer
            .dispatch({ type: pool.def.type, key: 'p' }, 'hold', ['arrival'], {
                callChain: [],
                callId: 'arrival'
            })
            .then(
                () => null,
                (e: unknown) => e
            );
        expect(arrival).toMatchObject({ kind: 'overloaded' });

        // …while a turn on the near host reaching the SAME full peer is
        // admitted, because its chain survived the hop. Waiting for the
        // peer to actually hold BOTH turns is what makes this a test: if
        // the hop were judged an arrival it would be refused there and the
        // peer would stay at one, so the wait is the assertion.
        const outer = near.actor(Caller, 'k').work();
        await vi.waitFor(() => expect(peer.stats().queued).toBe(2));
        pool.releaseAll();
        await expect(outer).resolves.toBe('inner');
        await expect(first).resolves.toBe('first');
    });
});

describe('drop-on-dequeue', () => {
    it('never runs the body of a queued call whose deadline already passed', async () => {
        const { def, ran, release, releaseAll } = gated('Expiring');
        const host = createHost({
            actors: [def],
            defaults: { ...quiet, callTimeoutMs: 40 }
        });
        running = host;
        await host.start();
        const a = host.actor(def, 'k');
        // Both callers give up at 40 ms — the existing contract; the
        // RUNNING turn 'a' is never killed and completes when released.
        const first = a.hold('a').catch((e: unknown) => e);
        await tick();
        const second = a.hold('b');
        await expect(second).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'call-timeout'
        );
        // …and now the QUEUED turn 'b' is skipped rather than run for nobody.
        await new Promise((r) => setTimeout(r, 20));
        release();
        expect(isActorError(await first)).toBe(true);
        await tick();
        expect(ran).toEqual(['a']);
        releaseAll();
        await expect(a.peek()).resolves.toBe(1);
    });
});

describe('refusals on the runtime’s own delivery paths', () => {
    it('a refused reminder delivery is counted as undelivered and re-armed for the next tick', async () => {
        const gates: Array<() => void> = [];
        const fired: number[] = [];
        const def = defineActor({
            type: 'RemindMeCapped',
            allowAnonymous: true,
            maxQueued: 1,
            state: () => ({}),
            onReminder() {
                fired.push(Date.now());
            },
            methods: (ctx) => ({
                async arm() {
                    await ctx.reminders.set('wake', { due: 0 });
                },
                async hold() {
                    await new Promise<void>((r) => gates.push(r));
                }
            })
        });
        const host = createHost({
            actors: [def],
            storage: memoryStorage(),
            defaults: { reminderTickMs: 25, sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        running = host;
        await host.start();
        const a = host.actor(def, 'k');
        await a.arm();
        const holding = a.hold();
        await tick();
        await vi.waitFor(() => expect(host.stats().remindersUndelivered).toBeGreaterThan(0), {
            timeout: 3000
        });
        expect(fired).toEqual([]);
        expect(host.stats().overloadRefusals).toBeGreaterThan(0);
        while (gates.length > 0) gates.shift()!();
        await holding;
        // The re-arm lands one tick later and now gets through.
        await vi.waitFor(() => expect(fired.length).toBe(1), { timeout: 3000 });
    });

    it('a refused topic delivery is a failures[] entry, never a publisher exception', async () => {
        const gates: Array<() => void> = [];
        const chat = topic<string>('news', 'all');
        const Sub = defineActor({
            type: 'SubCapped',
            allowAnonymous: true,
            maxQueued: 1,
            state: () => ({ seen: [] as string[] }),
            methods: (ctx) => ({
                async hold() {
                    await new Promise<void>((r) => gates.push(r));
                }
            }),
            subscriptions: {
                news: {
                    key: () => 'all',
                    handle: (ctx, event) => {
                        ctx.state.seen.push(String(event.payload));
                    }
                }
            }
        });
        const host = createHost({ actors: [Sub], defaults: quiet });
        running = host;
        await host.start();
        const holding = host.actor(Sub, 'all').hold();
        await tick();
        const report = await host.publish(chat, 'dropped');
        expect(report.subscribers).toBe(1);
        expect(report.delivered).toBe(0);
        expect(report.failures).toHaveLength(1);
        expect(report.failures[0]!.kind).toBe('overloaded');
        while (gates.length > 0) gates.shift()!();
        await holding;
    });
});

describe('across hosts', () => {
    const peerPolicy: PlacementPolicy = {
        choose: (_ref, view, self) =>
            view.hosts.find((h) => h.hostId !== self.hostId && h.status === 'active') ?? self
    };

    it('surfaces the kind unchanged through the internal mount, is not re-placed, and is counted', async () => {
        const { def, releaseAll } = gated('RemoteCapped', { maxQueued: 1 });
        cluster = await createCluster(2, {
            actors: [def],
            typePolicies: { RemoteCapped: peerPolicy }
        });
        const a = cluster.hosts[0]!.actor(def, 'k');
        const first = a.hold('a');
        // The activation lives on the peer; a second call from host 0 is
        // refused THERE and comes back branded.
        await vi.waitFor(() =>
            expect(cluster!.hosts[1]!.stats().perType['RemoteCapped']).toBe(1)
        );
        await vi.waitFor(() => expect(cluster!.hosts[1]!.stats().queued).toBe(1));
        const counters = () => cluster!.placements[0]!.counters();
        const before = counters();
        const error = await a.hold('b').then(
            () => null,
            (e: unknown) => e
        );
        expect(overloaded(error)).toBe(true);
        expect(error).toMatchObject({ scope: 'actor', limit: 1 });
        const after = counters();
        expect(after.retries - before.retries).toBe(0);
        expect(after.overloadedReplies - before.overloadedReplies).toBe(1);
        expect(cluster.hosts[1]!.stats().overloadRefusals).toBe(1);
        releaseAll();
        await first;
    });

    it('tracks outbound hops in flight per host as a gauge with a peak', async () => {
        const { def, releaseAll } = gated('Gauged');
        cluster = await createCluster(2, {
            actors: [def],
            typePolicies: { Gauged: peerPolicy }
        });
        const a = cluster.hosts[0]!.actor(def, 'k');
        const counters = () => cluster!.placements[0]!.counters();
        const calls = [a.hold('a'), a.hold('b'), a.hold('c')];
        await vi.waitFor(() => expect(counters().remoteInflight).toBe(3));
        expect(counters().remoteInflightPeak).toBe(3);
        releaseAll();
        await Promise.all(calls);
        expect(counters().remoteInflight).toBe(0);
        expect(counters().remoteInflightPeak).toBe(3);
    });
});

describe('the public wire', () => {
    it('a browser client sees a branded overloaded error with a 429', async () => {
        const { def, releaseAll } = gated('PublicCapped', { maxQueued: 1 });
        const host = createHost({ actors: [def], defaults: quiet });
        running = host;
        await host.start();
        const first = host.actor(def, 'k').hold('a');
        await tick();
        const ENDPOINT = 'http://actors.test/_sigx/actor';
        const statuses: number[] = [];
        configureActors({
            endpoint: ENDPOINT,
            fetch: async (input, init) => {
                const request = new Request(input, init);
                const response = await handleActorRequest(request, { host, origin: false });
                statuses.push(response.status);
                return response;
            }
        });
        const client = (
            __actorRef(def.type, ENDPOINT, []) as {
                __sigxActorProxy: (key: string) => { hold(label: string): Promise<unknown> };
            }
        ).__sigxActorProxy('k');
        const error = (await client.hold('b').then(
            () => null,
            (e: unknown) => e
        )) as { status?: number; data?: { kind?: string; scope?: string; limit?: number } };
        expect(statuses).toEqual([429]);
        expect(error.status).toBe(429);
        expect(error.data).toMatchObject({ kind: 'overloaded', scope: 'actor', limit: 1 });
        configureActors(null);
        releaseAll();
        await first;
    });
});


describe('the skipped flag is a field, not a sentence', () => {
    it('is readable on the error, crosses the host wire, and reaches a client', () => {
        const ran = new ActorCallTimeoutError('T/k', 'm', 30, {});
        const skipped = new ActorCallTimeoutError('T/k', 'm', 30, { skipped: true });
        // A caller distinguishes "the turn is still running without me"
        // from "the turn never ran" without parsing English.
        expect(ran.skipped).toBe(false);
        expect(skipped.skipped).toBe(true);

        const wire = toHostWireError(skipped);
        expect(wire.status).toBe(504);
        expect((wire.data as { skipped?: boolean }).skipped).toBe(true);
        const back = fromHostWireError(wire.status ?? 504, wire, 'fallback');
        expect((back as { skipped?: boolean }).skipped).toBe(true);

        // The older outcome stays absent on the wire, so a peer that never
        // learned the field reads exactly what it always did.
        const ranWire = toHostWireError(ran);
        expect('skipped' in (ranWire.data as object)).toBe(false);
        expect((fromHostWireError(ranWire.status ?? 504, ranWire, 'f') as { skipped?: boolean }).skipped).toBe(
            false
        );

        const client = toClientError(skipped) as { data?: { skipped?: boolean } };
        expect(client.data?.skipped).toBe(true);
    });
});

describe('the reminder-table size gauge', () => {
    it('reports the largest sharded reminder record the host has ticked, in entries', async () => {
        const def = defineActor({
            type: 'ManyReminders',
            allowAnonymous: true,
            state: () => ({}),
            onReminder() {},
            methods: (ctx) => ({
                async arm(n: number) {
                    for (let i = 0; i < n; i++) {
                        await ctx.reminders.set(`r${i}`, { due: 60_000 * 60 });
                    }
                }
            })
        });
        const host = createHost({
            actors: [def],
            storage: memoryStorage(),
            defaults: { reminderTickMs: 25, sweepIntervalMs: 60_000, callTimeoutMs: 0 }
        });
        running = host;
        await host.start();
        expect(host.stats().reminderShardEntriesMax).toBe(0);
        // One actor → one shard record holding every entry.
        await host.actor(def, 'k').arm(7);
        await vi.waitFor(() => expect(host.stats().reminderShardEntriesMax).toBe(7), {
            timeout: 3000
        });
    });
});
