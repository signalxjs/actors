/**
 * One-way calls (`.with({ oneWay: true })`) — issue #246.
 *
 * The contract under test: the dispatch resolves at ACCEPTANCE (enqueue into
 * the target activation; remotely, the transport reply after the receiving
 * host's enqueue), never at turn settlement. Failures BEFORE acceptance
 * (unknown type, activation failure, shutdown, a failing guard) still
 * reject; failures AFTER acceptance are dropped-with-counter
 * (`oneWayFailures`), never delivered.
 */
import { describe, expect, it } from 'vitest';
import { defineActor, isActorError, type ActorContext } from '@sigx/actors';
import {
    createHost,
    defineActorApp,
    memoryStorage,
    metrics,
    type Host,
    type MetricsPlugin
} from '@sigx/actors/host';
import { ACTOR_ONEWAY_HEADER, handleActorRequest } from '@sigx/actors/server';
import { __actorRef, configureActors } from '@sigx/actors/client';
import { createCluster, quiet, selfPolicy } from './harness';

function slowActor(events: string[] = []) {
    return defineActor({
        type: 'OneWaySubject',
        unguarded: true,
        state: () => ({ count: 0 }),
        methods: (ctx) => ({
            async increment(by: number) {
                ctx.state.count += by;
                return ctx.state.count;
            },
            get() {
                return ctx.state.count;
            },
            async slowMark(ms: number) {
                await new Promise((r) => setTimeout(r, ms));
                events.push('slow:done');
                return 'done';
            },
            boom() {
                events.push('boom:ran');
                throw new Error('post-acceptance failure');
            }
        }),
        streams: (ctx) => ({
            async *ticks() {
                yield ctx.state.count;
            }
        })
    });
}

describe('one-way calls: local semantics', () => {
    it('resolves undefined at acceptance, before the turn completes', async () => {
        const events: string[] = [];
        const def = slowActor(events);
        const host = createHost({ actors: [def], defaults: quiet });
        const started = Date.now();
        const result = await host.actor(def, 'a').with({ oneWay: true }).slowMark(150);
        expect(Date.now() - started).toBeLessThan(100);
        expect(result).toBeUndefined();
        expect(events).not.toContain('slow:done');
        // The turn still runs to completion — fire-and-forget, not fire-and-drop.
        await host.actor(def, 'a').get();
        expect(events).toContain('slow:done');
    });

    it('keeps turn FIFO: an awaited call issued after a one-way observes its effect', async () => {
        const def = slowActor();
        const host = createHost({ actors: [def], defaults: quiet });
        await host.actor(def, 'fifo').with({ oneWay: true }).increment(5);
        // The one-way resolved AFTER enqueue, so this read queues behind it.
        await expect(host.actor(def, 'fifo').get()).resolves.toBe(5);
    });

    it('a post-acceptance throw is dropped: the caller resolved, the queue is not poisoned', async () => {
        const events: string[] = [];
        const def = slowActor(events);
        const host = createHost({ actors: [def], defaults: quiet });
        await expect(host.actor(def, 'drop').with({ oneWay: true }).boom()).resolves.toBeUndefined();
        // The failing turn ran, and the next turn runs regardless.
        await expect(host.actor(def, 'drop').increment(1)).resolves.toBe(1);
        expect(events).toContain('boom:ran');
    });

    it('pre-acceptance failures still reject: unknown type', async () => {
        const def = slowActor();
        const host = createHost({ actors: [def], defaults: quiet });
        const stranger = defineActor({
            type: 'Unregistered',
            unguarded: true,
            state: () => ({}),
            methods: () => ({ async poke() {} })
        });
        await expect(host.actor(stranger, 'x').with({ oneWay: true }).poke()).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'activation'
        );
    });

    it('pre-acceptance failures still reject: activation failure', async () => {
        const exploding = defineActor({
            type: 'ExplodingState',
            unguarded: true,
            state: (): { n: number } => {
                throw new Error('state() failed');
            },
            methods: () => ({ async poke() {} })
        });
        const host = createHost({ actors: [exploding], defaults: quiet });
        await expect(host.actor(exploding, 'x').with({ oneWay: true }).poke()).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'activation'
        );
    });

    it('pre-acceptance failures still reject: host shutting down', async () => {
        const def = slowActor();
        const host = createHost({ actors: [def], defaults: quiet });
        await host.actor(def, 'x').increment(1);
        await host.stop({ timeoutMs: 1000 });
        await expect(host.actor(def, 'x').with({ oneWay: true }).increment(1)).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'host-shutdown'
        );
    });

    it('skips the caller deadline race: a tiny callTimeoutMs cannot reject a one-way', async () => {
        const events: string[] = [];
        const def = slowActor(events);
        const host = createHost({
            actors: [def],
            defaults: { ...quiet, callTimeoutMs: 20 }
        });
        await expect(
            host.actor(def, 'deadline').with({ oneWay: true }).slowMark(100)
        ).resolves.toBeUndefined();
        // And the turn was not killed either — it outlives its own deadline
        // because nothing is racing the caller anymore.
        await new Promise((r) => setTimeout(r, 150));
        expect(events).toContain('slow:done');
        await host.stop({ timeoutMs: 1000 });
    });

    it('a one-way self-call on a non-reentrant actor queues instead of deadlocking', async () => {
        const events: string[] = [];
        const selfCaller = defineActor({
            type: 'SelfKicker',
            unguarded: true,
            state: () => ({}),
            methods: (ctx: ActorContext<object>) => ({
                async kick() {
                    // Awaited, this exact call is an ActorDeadlockError. One-way
                    // it is "schedule more work for myself".
                    await (
                        ctx.actor(selfCaller, ctx.key).with({ oneWay: true }) as unknown as {
                            record(): Promise<void>;
                        }
                    ).record();
                    return 'kicked';
                },
                async record() {
                    events.push('record:ran');
                }
            })
        });
        const host = createHost({ actors: [selfCaller], defaults: quiet });
        const client = host.actor(selfCaller, 's') as unknown as { kick(): Promise<string> };
        await expect(client.kick()).resolves.toBe('kicked');
        // The queued self-call runs after the kicking turn releases the activation.
        await new Promise((r) => setTimeout(r, 20));
        expect(events).toContain('record:ran');
    });

    it('streams refuse one-way with a descriptive error', async () => {
        const def = slowActor();
        const host = createHost({ actors: [def], defaults: quiet });
        const bound = host.actor(def, 'st').with({ oneWay: true }) as unknown as {
            ticks(): AsyncIterable<number>;
        };
        expect(() => bound.ticks()).toThrow(/streams cannot be one-way/);
    });
});

describe('one-way calls: metrics', () => {
    async function metricsHost(m: MetricsPlugin, def: ReturnType<typeof slowActor>) {
        const app = defineActorApp({
            actors: [def],
            storage: memoryStorage(),
            defaults: quiet
        }).use(m);
        return app.start();
    }

    it('counts a post-acceptance failure as oneWayFailures, not failed', async () => {
        const m = metrics();
        const events: string[] = [];
        const def = slowActor(events);
        const host = await metricsHost(m, def);
        await host.actor(def, 'm').with({ oneWay: true }).boom();
        await host.actor(def, 'm').get(); // settle the queue
        const snap = m.snapshot();
        expect(snap.calls.oneWayFailures).toBe(1);
        // The one-way dispatch itself succeeded (acceptance), so `failed`
        // counts nothing.
        expect(snap.calls.failed).toBe(0);
        expect(m.digest().calls.oneWayFailures).toBe(1);
        m.reset();
        expect(m.snapshot().calls.oneWayFailures).toBe(0);
    });

    it('counts even with histograms: false (the minimal observer)', async () => {
        const m = metrics({ histograms: false });
        const def = slowActor();
        const host = await metricsHost(m, def);
        await host.actor(def, 'm2').with({ oneWay: true }).boom();
        await host.actor(def, 'm2').get();
        expect(m.snapshot().calls.oneWayFailures).toBe(1);
        expect(m.snapshot().latencyMs).toBeNull();
    });

    it('a pre-acceptance one-way failure counts under failed, not oneWayFailures', async () => {
        const m = metrics();
        const def = slowActor();
        const host = await metricsHost(m, def);
        const stranger = defineActor({
            type: 'NopeOneWay',
            unguarded: true,
            state: () => ({}),
            methods: () => ({ async poke() {} })
        });
        await expect(host.actor(stranger, 'x').with({ oneWay: true }).poke()).rejects.toThrow();
        const snap = m.snapshot();
        expect(snap.calls.failed).toBe(1);
        expect(snap.calls.oneWayFailures).toBe(0);
    });

    it('the turn observer receives call.oneWay on a one-way-delivered turn', async () => {
        const def = slowActor();
        const host = createHost({ actors: [def], defaults: quiet });
        const seen: Array<true | undefined> = [];
        const detach = host.observeTurns((_ref, _method, _q, _e, _failed, call) => {
            seen.push(call?.oneWay);
        });
        await host.actor(def, 'obs').with({ oneWay: true }).increment(1);
        await host.actor(def, 'obs').get();
        detach();
        expect(seen).toEqual([true, undefined]);
    });
});

describe('one-way calls: public wire', () => {
    const ENDPOINT = 'http://actors.test/_sigx/actor';

    function wireHost(events: string[]): { host: Host; headers: string[] } {
        const def = slowActor(events);
        const host = createHost({ actors: [def], defaults: quiet });
        const headers: string[] = [];
        configureActors({
            endpoint: ENDPOINT,
            fetch: async (input, init) => {
                const request = new Request(input, init);
                headers.push(request.headers.get(ACTOR_ONEWAY_HEADER) ?? '');
                return handleActorRequest(request, { host, origin: false });
            }
        });
        return { host, headers };
    }

    it('sends the one-way header and the response arrives at acceptance', async () => {
        const events: string[] = [];
        const { host, headers } = wireHost(events);
        try {
            const SubjectRef = __actorRef('OneWaySubject', ENDPOINT, ['ticks']);
            const proxy = (
                SubjectRef as {
                    __sigxActorProxy: (key: string, o?: { oneWay?: boolean }) => {
                        slowMark(ms: number): Promise<unknown>;
                        get(): Promise<number>;
                        with(o: { oneWay: true }): { slowMark(ms: number): Promise<void> };
                    };
                }
            ).__sigxActorProxy('w1');

            const started = Date.now();
            await expect(proxy.with({ oneWay: true }).slowMark(150)).resolves.toBeUndefined();
            expect(Date.now() - started).toBeLessThan(100);
            expect(headers).toEqual(['1']);
            expect(events).not.toContain('slow:done');

            // Without the option the header does not ride.
            await proxy.get();
            expect(headers).toEqual(['1', '']);
            expect(events).toContain('slow:done');
        } finally {
            configureActors(null);
            await host.stop({ timeoutMs: 1000 });
        }
    });
});

describe('one-way calls: across a 2-host cluster', () => {
    it('acks at the owner\'s acceptance, delivers exactly once, counts the dropped failure owner-side', async () => {
        const events: string[] = [];
        const meters = [metrics(), metrics()];
        const def = slowActor(events);
        const h = await createCluster(2, {
            actors: [def],
            policy: selfPolicy,
            plugins: (i) => [meters[i]!]
        });
        try {
            // selfPolicy: host 1 owns the key; calls from host 0 cross the wire.
            await h.hosts[1]!.actor(def, 'remote').increment(0);

            const started = Date.now();
            await expect(
                h.hosts[0]!.actor(def, 'remote').with({ oneWay: true }).slowMark(200)
            ).resolves.toBeUndefined();
            expect(Date.now() - started).toBeLessThan(150);
            expect(events).not.toContain('slow:done');

            // Exactly once: the next awaited read queues behind both turns.
            await h.hosts[0]!.actor(def, 'remote').with({ oneWay: true }).increment(5);
            await expect(h.hosts[0]!.actor(def, 'remote').get()).resolves.toBe(5);
            expect(events).toContain('slow:done');

            // A dropped failure counts on the host that RAN the turn.
            await h.hosts[0]!.actor(def, 'remote').with({ oneWay: true }).boom();
            await h.hosts[0]!.actor(def, 'remote').get();
            expect(meters[1]!.snapshot().calls.oneWayFailures).toBe(1);
            expect(meters[0]!.snapshot().calls.oneWayFailures).toBe(0);
        } finally {
            await h.stop();
        }
    });
});
