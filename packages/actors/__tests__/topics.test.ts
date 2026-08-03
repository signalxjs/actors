/**
 * Topics (#239) — actor-to-actor pub/sub, single host.
 *
 * The contract under test: implicit `subscriptions:` on the definition,
 * best-effort at-most-once delivery as ordinary dispatches of the reserved
 * `$sigx:topic` method, a settlement report that absorbs subscriber
 * failures, and the call-chain rule that turns a subscription cycle into a
 * detected deadlock instead of a hang.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
    defineActor,
    publishTopic,
    topic,
    type Host,
    type TopicEvent
} from '@sigx/actors';
import { createHost } from '@sigx/actors/host';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const running: Host[] = [];

async function startHost(actors: Parameters<typeof createHost>[0]['actors']): Promise<Host> {
    const host = createHost({ actors, defaults: quiet });
    await host.start();
    running.push(host);
    return host;
}

afterEach(async () => {
    for (const host of running.splice(0)) await host.stop({ timeoutMs: 1000 });
    vi.restoreAllMocks();
});

// ---------------------------------------------------------------------------
// Validation

describe('topic()', () => {
    it('refuses invalid names and keys in every build', () => {
        expect(() => topic('')).toThrow(/non-empty string name/);
        expect(() => topic('a#b')).toThrow(/must not contain/);
        expect(() => topic('$mine')).toThrow(/reserved/);
        expect(() => topic('@mine')).toThrow(/reserved/);
        expect(() => topic('ok', '')).toThrow(/non-empty string key/);
    });

    it('defaults the key so a singleton topic is one argument', () => {
        expect(topic('config-changed')).toEqual({ name: 'config-changed', key: 'default' });
    });
});

describe('the subscriptions: declaration', () => {
    const base = {
        unguarded: true,
        state: () => ({}),
        methods: () => ({})
    };

    it('refuses a reserved or malformed topic name', () => {
        expect(() =>
            defineActor({ ...base, type: 'Bad', subscriptions: { '$sigx:x': () => {} } })
        ).toThrow(/reserved/);
        expect(() =>
            defineActor({ ...base, type: 'Bad', subscriptions: { 'a#b': () => {} } })
        ).toThrow(/must not contain/);
    });

    it('refuses a handler that is not callable', () => {
        expect(() =>
            defineActor({
                ...base,
                type: 'Bad',
                subscriptions: { t: 42 as unknown as () => void }
            })
        ).toThrow(/needs a handler/);
        expect(() =>
            defineActor({
                ...base,
                type: 'Bad',
                subscriptions: { t: { handle: 42 } as unknown as () => void }
            })
        ).toThrow(/needs a handler/);
        expect(() =>
            defineActor({
                ...base,
                type: 'Bad',
                subscriptions: {
                    t: { key: 'nope', handle: () => {} } as unknown as () => void
                }
            })
        ).toThrow(/`key` that is not a function/);
    });
});

// ---------------------------------------------------------------------------
// Delivery

const chat = topic<string>('chat-messages', 'room-1');

function makeFeed() {
    return defineActor({
        type: 'Feed',
        unguarded: true,
        state: () => ({ log: [] as string[], events: [] as TopicEvent[] }),
        methods: (ctx) => ({
            async log() {
                return [...ctx.state.log];
            },
            async events() {
                return ctx.snapshot().events;
            }
        }),
        subscriptions: {
            'chat-messages': async (ctx, event) => {
                ctx.state.log.push(String(event.payload));
                ctx.state.events.push(event);
                await ctx.save();
            }
        }
    });
}

describe('publish', () => {
    it('activates an idle subscriber, runs the handler as a turn, and reports it', async () => {
        const Feed = makeFeed();
        const host = await startHost([Feed]);
        const report = await host.publish(chat, 'hello');
        expect(report).toEqual({ subscribers: 1, delivered: 1, failures: [] });
        await expect(host.actor(Feed, 'room-1').log()).resolves.toEqual(['hello']);
    });

    it('stamps the event with topic, at, and no publisher for a host publish', async () => {
        const Feed = makeFeed();
        const host = await startHost([Feed]);
        await host.publish(chat, 'x');
        const [event] = await host.actor(Feed, 'room-1').events();
        expect(event!.topic).toEqual({ name: 'chat-messages', key: 'room-1' });
        expect(typeof event!.at).toBe('number');
        expect(event!.publisher).toBeUndefined();
    });

    it('publishes through the ambient seam via publishTopic()', async () => {
        const Feed = makeFeed();
        const host = await startHost([Feed]);
        const report = await publishTopic(chat, 'ambient');
        expect(report.delivered).toBe(1);
        await expect(host.actor(Feed, 'room-1').log()).resolves.toEqual(['ambient']);
    });

    it('refuses a malformed hand-built topic at the caller', async () => {
        const host = await startHost([makeFeed()]);
        await expect(
            host.publish({ name: '$sigx:evil', key: 'k' }, 1)
        ).rejects.toThrow(/build it with topic\(\)/);
    });

    it('names the publisher when published from a turn', async () => {
        const Feed = makeFeed();
        const Pub = defineActor({
            type: 'Pub',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                async post(text: string) {
                    return ctx.publish(chat, text);
                }
            })
        });
        const host = await startHost([Feed, Pub]);
        const report = await host.actor(Pub, 'p1').post('from-actor');
        expect(report.delivered).toBe(1);
        const [event] = await host.actor(Feed, 'room-1').events();
        expect(event!.publisher).toEqual({ type: 'Pub', key: 'p1' });
    });

    it('maps the subscriber key: a singleton aggregator sees every topic key', async () => {
        const seen: string[] = [];
        const Agg = defineActor({
            type: 'Agg',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                async key() {
                    return ctx.key;
                }
            }),
            subscriptions: {
                presence: {
                    key: () => 'aggregate',
                    handle: (_ctx, event) => {
                        seen.push(event.topic.key);
                    }
                }
            }
        });
        const host = await startHost([Agg]);
        await host.publish(topic('presence', 'user-1'), 1);
        await host.publish(topic('presence', 'user-2'), 1);
        expect(seen).toEqual(['user-1', 'user-2']);
        // Exactly one activation received both: the mapped key.
        expect(host.stats().perType['Agg']).toBe(1);
    });

    it('delivers in publish order when the publisher awaits sequentially', async () => {
        const Feed = makeFeed();
        const host = await startHost([Feed]);
        await host.publish(chat, 'one');
        await host.publish(chat, 'two');
        await host.publish(chat, 'three');
        await expect(host.actor(Feed, 'room-1').log()).resolves.toEqual([
            'one',
            'two',
            'three'
        ]);
    });
});

// ---------------------------------------------------------------------------
// Failure isolation

describe('failure isolation', () => {
    it('one throwing handler is one failures entry — others deliver', async () => {
        const t = topic('fanout', 'k');
        const ok = (type: string) =>
            defineActor({
                type,
                unguarded: true,
                state: () => ({ got: 0 }),
                methods: (ctx) => ({
                    async got() {
                        return ctx.state.got;
                    }
                }),
                subscriptions: { fanout: (ctx) => void (ctx.state.got += 1) }
            });
        const A = ok('A');
        const B = defineActor({
            type: 'B',
            unguarded: true,
            state: () => ({}),
            methods: () => ({}),
            subscriptions: {
                fanout: () => {
                    throw new Error('B is broken');
                }
            }
        });
        const C = ok('C');
        const host = await startHost([A, B, C]);
        const report = await host.publish(t, null);
        expect(report.subscribers).toBe(3);
        expect(report.delivered).toBe(2);
        expect(report.failures).toEqual([
            { type: 'B', key: 'k', message: expect.stringContaining('B is broken') }
        ]);
        await expect(host.actor(A, 'k').got()).resolves.toBe(1);
        await expect(host.actor(C, 'k').got()).resolves.toBe(1);
    });

    it('a throwing key() mapper costs only its own delivery', async () => {
        const t = topic('mapped', 'k');
        const Bad = defineActor({
            type: 'BadMap',
            unguarded: true,
            state: () => ({}),
            methods: () => ({}),
            subscriptions: {
                mapped: {
                    key: () => {
                        throw new Error('mapper died');
                    },
                    handle: () => {}
                }
            }
        });
        const host = await startHost([Bad]);
        const report = await host.publish(t, 1);
        expect(report.delivered).toBe(0);
        expect(report.failures[0]).toEqual({
            type: 'BadMap',
            key: '(key() failed)',
            message: expect.stringContaining('mapper died')
        });
    });

    it('a handler that throws does not fault the activation', async () => {
        const t = topic('flaky', 'k');
        let attempts = 0;
        const Flaky = defineActor({
            type: 'Flaky',
            unguarded: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                async n() {
                    return ctx.state.n;
                }
            }),
            subscriptions: {
                flaky: (ctx) => {
                    attempts += 1;
                    if (attempts === 1) throw new Error('first delivery fails');
                    ctx.state.n += 1;
                }
            }
        });
        const host = await startHost([Flaky]);
        const first = await host.publish(t, 1);
        expect(first.failures).toHaveLength(1);
        const second = await host.publish(t, 1);
        expect(second.delivered).toBe(1);
        await expect(host.actor(Flaky, 'k').n()).resolves.toBe(1);
    });
});

// ---------------------------------------------------------------------------
// Cycles

describe('subscription cycles', () => {
    it('a subscription cycling back into a non-reentrant publisher is a deadlock entry', async () => {
        const loop = topic('loop', 'self');
        const Loop = defineActor({
            type: 'Loop',
            unguarded: true,
            state: () => ({}),
            methods: (ctx) => ({
                async trigger() {
                    return ctx.publish(loop, 1);
                }
            }),
            subscriptions: { loop: () => {} }
        });
        const host = await startHost([Loop]);
        const report = await host.actor(Loop, 'self').trigger();
        expect(report.delivered).toBe(0);
        expect(report.failures[0]!.kind).toBe('deadlock');
    });

    it('reentrant: true lets a self-subscription deliver inline', async () => {
        const loop = topic('loop-ok', 'self');
        let handled = 0;
        const Loop = defineActor({
            type: 'LoopOk',
            unguarded: true,
            reentrant: true,
            state: () => ({}),
            methods: (ctx) => ({
                async trigger() {
                    return ctx.publish(loop, 1);
                }
            }),
            subscriptions: { 'loop-ok': () => void (handled += 1) }
        });
        const host = await startHost([Loop]);
        const report = await host.actor(Loop, 'self').trigger();
        expect(report).toEqual({ subscribers: 1, delivered: 1, failures: [] });
        expect(handled).toBe(1);
    });

    it("reentrant: 'always' delivers a self-subscription as a concurrent turn", async () => {
        const loop = topic('loop-always', 'self');
        let handled = 0;
        const Loop = defineActor({
            type: 'LoopAlways',
            unguarded: true,
            reentrant: 'always',
            state: () => ({}),
            methods: (ctx) => ({
                async trigger() {
                    return ctx.publish(loop, 1);
                }
            }),
            subscriptions: { 'loop-always': () => void (handled += 1) }
        });
        const host = await startHost([Loop]);
        // The publishing turn awaits the fan-out; the delivery lands as an
        // interleaved turn of the same activation instead of deadlocking.
        const report = await host.actor(Loop, 'self').trigger();
        expect(report).toEqual({ subscribers: 1, delivered: 1, failures: [] });
        expect(handled).toBe(1);
    });
});

// ---------------------------------------------------------------------------
// Edges

describe('edges', () => {
    it('publishing to a topic nobody subscribes to reports zero subscribers', async () => {
        const host = await startHost([makeFeed()]);
        const report = await host.publish(topic('nobody-listens'), 1);
        expect(report).toEqual({ subscribers: 0, delivered: 0, failures: [] });
    });

    it('a delivery for a subscription this build removed is a warned no-op', async () => {
        const Feed = makeFeed();
        const host = await startHost([Feed]);
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        // A peer on an older deploy still publishing: simulate by dispatching
        // the reserved method directly with an unsubscribed topic name.
        const result = await host.dispatch(
            { type: 'Feed', key: 'room-1' },
            '$sigx:topic',
            [{ topic: { name: 'gone', key: 'room-1' }, payload: 1, at: Date.now() }],
            { callChain: [], callId: 'test' }
        );
        expect(result).toBeUndefined();
        expect(warn).toHaveBeenCalledWith(expect.stringContaining('declares no subscription'));
        await expect(host.actor(Feed, 'room-1').log()).resolves.toEqual([]);
    });

    it('resolves lazy-registered subscribers through the registry', async () => {
        const Feed = makeFeed();
        const host = createHost({
            actors: { Feed: async () => ({ Feed }) },
            defaults: quiet
        });
        await host.start();
        running.push(host);
        const report = await host.publish(chat, 'lazy');
        expect(report.delivered).toBe(1);
    });

    it('a failing lazy loader fails the publish with the loader error', async () => {
        const host = createHost({
            actors: {
                Broken: async () => {
                    throw new Error('loader exploded');
                }
            },
            defaults: quiet
        });
        await host.start();
        running.push(host);
        await expect(host.publish(topic('anything'), 1)).rejects.toThrow(/loader exploded/);
    });
});
